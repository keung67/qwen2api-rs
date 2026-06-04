//! 請求統計：SQLite 落盤 + 背景批次寫入 + 聚合查詢。
//!
//! 設計重點（對齊 dev/LATENCY.md 的 hot-path 原則）：
//! - 請求熱路徑只做**非阻塞** `try_send`；通道滿則丟棄並計數，絕不阻塞回應延遲。
//! - 實際寫入在**獨立 OS 執行緒**批次進行（WAL 模式），單一寫者避免鎖競爭。
//! - 面板查詢（admin，低頻）在 `spawn_blocking` 內各自開唯讀連線（WAL 容許並發讀）。
//!
//! 資料庫檔預設 `data/stats.db`（落在既有持久化卷，重部署不丟）。

use rusqlite::{params, Connection};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TrySendError};
use std::sync::Arc;
use std::time::Duration;

/// 單筆請求的統計記錄。
#[derive(Debug, Clone)]
pub struct RequestRecord {
    /// 請求開始的 unix 毫秒。
    pub ts_ms: i64,
    /// 協議入口：openai/anthropic/gemini/responses/images/videos。
    pub surface: String,
    /// 對外模型名（客戶端請求的）。
    pub model: String,
    /// 上游實際模型。
    pub resolved_model: String,
    /// t2t/t2i/t2v/deep_research…
    pub chat_type: String,
    pub stream: bool,
    pub success: bool,
    pub error: Option<String>,
    pub prompt_tokens: i64,
    pub completion_tokens: i64,
    pub reasoning_tokens: i64,
    pub total_tokens: i64,
    /// 首字延遲（毫秒）；無 token 或失敗時為 None。
    pub ttft_ms: Option<i64>,
    /// 完成總耗時（毫秒）。
    pub duration_ms: i64,
    /// 呼叫者（API key 截斷前綴，僅供分組；非完整密鑰）。
    pub caller: Option<String>,
}

/// 通道容量：滿了就丟棄統計（保護請求延遲），4096 足以吸收突發。
const CHANNEL_CAP: usize = 4096;
/// 單批最多寫入筆數。
const BATCH_MAX: usize = 512;

pub struct Stats {
    tx: SyncSender<RequestRecord>,
    db_path: PathBuf,
    dropped: Arc<AtomicU64>,
}

impl Stats {
    /// 建立統計子系統：初始化 schema + 啟動背景寫入執行緒。
    pub fn new(db_path: impl AsRef<Path>) -> Arc<Self> {
        let db_path = db_path.as_ref().to_path_buf();
        if let Some(parent) = db_path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        if let Err(e) = init_schema(&db_path) {
            tracing::error!("[stats] 初始化資料庫失敗 {:?}: {e}", db_path);
        }

        let (tx, rx) = sync_channel::<RequestRecord>(CHANNEL_CAP);
        let dropped = Arc::new(AtomicU64::new(0));

        let writer_path = db_path.clone();
        if let Err(e) = std::thread::Builder::new()
            .name("stats-writer".into())
            .spawn(move || writer_loop(writer_path, rx))
        {
            tracing::error!("[stats] 啟動寫入執行緒失敗: {e}");
        }

        Arc::new(Stats { tx, db_path, dropped })
    }

    /// hot-path：非阻塞記錄一筆；通道滿則丟棄並計數。
    pub fn record(&self, rec: RequestRecord) {
        match self.tx.try_send(rec) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                let n = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
                if n % 100 == 1 {
                    tracing::warn!("[stats] 寫入通道已滿，累計丟棄 {n} 筆統計（不影響請求）");
                }
            }
            Err(TrySendError::Disconnected(_)) => {}
        }
    }

    pub fn db_path(&self) -> PathBuf {
        self.db_path.clone()
    }

    pub fn dropped_count(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

/// 開啟連線並套用 WAL / NORMAL / busy_timeout。
fn open_conn(path: &Path) -> rusqlite::Result<Connection> {
    let conn = Connection::open(path)?;
    // WAL：容許單寫者與多讀者並發；NORMAL：在 WAL 下安全且快。
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    conn.busy_timeout(Duration::from_secs(5))?;
    Ok(conn)
}

fn init_schema(path: &Path) -> rusqlite::Result<()> {
    let conn = open_conn(path)?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS request_logs (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_ms             INTEGER NOT NULL,
            surface           TEXT    NOT NULL DEFAULT '',
            model             TEXT    NOT NULL DEFAULT '',
            resolved_model    TEXT    NOT NULL DEFAULT '',
            chat_type         TEXT    NOT NULL DEFAULT '',
            stream            INTEGER NOT NULL DEFAULT 0,
            success           INTEGER NOT NULL DEFAULT 1,
            error             TEXT,
            prompt_tokens     INTEGER NOT NULL DEFAULT 0,
            completion_tokens INTEGER NOT NULL DEFAULT 0,
            reasoning_tokens  INTEGER NOT NULL DEFAULT 0,
            total_tokens      INTEGER NOT NULL DEFAULT 0,
            ttft_ms           INTEGER,
            duration_ms       INTEGER NOT NULL DEFAULT 0,
            caller            TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_logs_ts ON request_logs(ts_ms);
        CREATE INDEX IF NOT EXISTS idx_logs_model ON request_logs(model);",
    )?;
    Ok(())
}

/// 背景寫入迴圈：阻塞收第一筆 → 盡量 drain 同批 → 一次交易寫入。
fn writer_loop(path: PathBuf, rx: Receiver<RequestRecord>) {
    let mut conn = match open_conn(&path) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("[stats] 寫入執行緒開連線失敗: {e}");
            return;
        }
    };
    loop {
        let first = match rx.recv() {
            Ok(r) => r,
            Err(_) => break, // 所有 sender 已釋放 → 結束
        };
        let mut batch = Vec::with_capacity(64);
        batch.push(first);
        while batch.len() < BATCH_MAX {
            match rx.try_recv() {
                Ok(r) => batch.push(r),
                Err(_) => break,
            }
        }
        if let Err(e) = insert_batch(&mut conn, &batch) {
            tracing::error!("[stats] 批次寫入失敗（{} 筆）: {e}", batch.len());
        }
    }
}

fn insert_batch(conn: &mut Connection, batch: &[RequestRecord]) -> rusqlite::Result<()> {
    let tx = conn.transaction()?;
    {
        let mut stmt = tx.prepare_cached(
            "INSERT INTO request_logs
             (ts_ms,surface,model,resolved_model,chat_type,stream,success,error,
              prompt_tokens,completion_tokens,reasoning_tokens,total_tokens,ttft_ms,duration_ms,caller)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)",
        )?;
        for r in batch {
            stmt.execute(params![
                r.ts_ms,
                r.surface,
                r.model,
                r.resolved_model,
                r.chat_type,
                r.stream as i64,
                r.success as i64,
                r.error,
                r.prompt_tokens,
                r.completion_tokens,
                r.reasoning_tokens,
                r.total_tokens,
                r.ttft_ms,
                r.duration_ms,
                r.caller,
            ])?;
        }
    }
    tx.commit()?;
    Ok(())
}

// ===================== 查詢（spawn_blocking 內呼叫）=====================

/// 面板總覽：summary + by_model + by_surface + 時序。`since_ms`=0 表示全部。
pub fn query_dashboard(path: &Path, since_ms: i64, bucket_ms: i64) -> rusqlite::Result<Value> {
    let conn = open_conn(path)?;
    let now = crate::util::now_millis();

    // --- summary ---
    let summary = conn.query_row(
        "SELECT COUNT(*),
                COALESCE(SUM(success),0),
                COALESCE(SUM(prompt_tokens),0),
                COALESCE(SUM(completion_tokens),0),
                COALESCE(SUM(reasoning_tokens),0),
                COALESCE(SUM(total_tokens),0),
                AVG(ttft_ms),
                AVG(duration_ms)
         FROM request_logs WHERE ts_ms >= ?1",
        params![since_ms],
        |row| {
            let requests: i64 = row.get(0)?;
            let success: i64 = row.get(1)?;
            let prompt: i64 = row.get(2)?;
            let completion: i64 = row.get(3)?;
            let reasoning: i64 = row.get(4)?;
            let total: i64 = row.get(5)?;
            let avg_ttft: Option<f64> = row.get(6)?;
            let avg_dur: Option<f64> = row.get(7)?;
            Ok(json!({
                "requests": requests,
                "success": success,
                "failed": requests - success,
                "success_rate": if requests > 0 { success as f64 / requests as f64 } else { 0.0 },
                "prompt_tokens": prompt,
                "completion_tokens": completion,
                "reasoning_tokens": reasoning,
                "total_tokens": total,
                "avg_ttft_ms": avg_ttft.map(|v| v.round() as i64),
                "avg_duration_ms": avg_dur.map(|v| v.round() as i64),
            }))
        },
    )?;

    // 即時 RPM：最近 60s 的請求數。
    let rpm_now: i64 = conn.query_row(
        "SELECT COUNT(*) FROM request_logs WHERE ts_ms >= ?1",
        params![now - 60_000],
        |r| r.get(0),
    )?;

    // --- by_model（基本聚合 + TTFT 分位數）---
    // 平均值單一數字資訊量低，補上 min/p50/p95/max 才能看出延遲分佈（如尾部退化、冷啟動）。
    // SQLite 沒原生 percentile_cont，但 bundled 版本（>=3.25）支援 window function，
    // 用 ROW_NUMBER + COUNT 在 partition 內做整數百分位（rn = (cnt*P + 50)/100，cnt≥1 永遠命中）。
    let by_model = {
        // 基本聚合（已含平均，欄位順序勿動，避免錯位）
        let mut stmt = conn.prepare(
            "SELECT model, COUNT(*), COALESCE(SUM(total_tokens),0), AVG(ttft_ms), AVG(duration_ms)
             FROM request_logs WHERE ts_ms >= ?1
             GROUP BY model ORDER BY COUNT(*) DESC LIMIT 50",
        )?;
        let rows = stmt.query_map(params![since_ms], |r| {
            let avg_ttft: Option<f64> = r.get(3)?;
            let avg_dur: Option<f64> = r.get(4)?;
            Ok(json!({
                "model": r.get::<_, String>(0)?,
                "requests": r.get::<_, i64>(1)?,
                "total_tokens": r.get::<_, i64>(2)?,
                "avg_ttft_ms": avg_ttft.map(|v| v.round() as i64),
                "avg_duration_ms": avg_dur.map(|v| v.round() as i64),
            }))
        })?;
        let mut base: Vec<Value> = rows.collect::<rusqlite::Result<Vec<Value>>>()?;

        // TTFT 分位數（只計 ttft_ms IS NOT NULL 的紀錄）
        let mut ts_stmt = conn.prepare(
            "WITH t AS (
                 SELECT model, ttft_ms,
                        ROW_NUMBER() OVER (PARTITION BY model ORDER BY ttft_ms) AS rn,
                        COUNT(*)     OVER (PARTITION BY model)                  AS cnt
                 FROM request_logs
                 WHERE ts_ms >= ?1 AND ttft_ms IS NOT NULL
             )
             SELECT model,
                    MIN(ttft_ms) AS min_ttft,
                    MAX(CASE WHEN rn = (cnt * 50 + 50) / 100 THEN ttft_ms END) AS p50_ttft,
                    MAX(CASE WHEN rn = (cnt * 95 + 50) / 100 THEN ttft_ms END) AS p95_ttft,
                    MAX(ttft_ms) AS max_ttft
             FROM t
             GROUP BY model",
        )?;
        let ttft_map: HashMap<String, (i64, i64, i64, i64)> = ts_stmt
            .query_map(params![since_ms], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    (
                        r.get::<_, i64>(1)?,
                        r.get::<_, Option<i64>>(2)?.unwrap_or(0),
                        r.get::<_, Option<i64>>(3)?.unwrap_or(0),
                        r.get::<_, i64>(4)?,
                    ),
                ))
            })?
            .collect::<rusqlite::Result<HashMap<_, _>>>()?;

        for entry in base.iter_mut() {
            let model = entry.get("model").and_then(|v| v.as_str()).unwrap_or("").to_string();
            if let Some((min_t, p50, p95, max_t)) = ttft_map.get(&model) {
                entry["min_ttft_ms"] = json!(min_t);
                entry["p50_ttft_ms"] = json!(p50);
                entry["p95_ttft_ms"] = json!(p95);
                entry["max_ttft_ms"] = json!(max_t);
            }
        }
        base
    };

    // --- by_surface ---
    let by_surface = {
        let mut stmt = conn.prepare(
            "SELECT surface, COUNT(*), COALESCE(SUM(total_tokens),0)
             FROM request_logs WHERE ts_ms >= ?1
             GROUP BY surface ORDER BY COUNT(*) DESC LIMIT 50",
        )?;
        let rows = stmt.query_map(params![since_ms], |r| {
            Ok(json!({
                "surface": r.get::<_, String>(0)?,
                "requests": r.get::<_, i64>(1)?,
                "total_tokens": r.get::<_, i64>(2)?,
            }))
        })?;
        rows.collect::<rusqlite::Result<Vec<Value>>>()?
    };

    // --- 時序（依 bucket_ms 分桶）---
    let points = {
        let mut stmt = conn.prepare(
            "SELECT (ts_ms / ?2) * ?2 AS bucket,
                    COUNT(*),
                    COALESCE(SUM(total_tokens),0),
                    AVG(ttft_ms),
                    AVG(duration_ms)
             FROM request_logs WHERE ts_ms >= ?1
             GROUP BY bucket ORDER BY bucket",
        )?;
        let rows = stmt.query_map(params![since_ms, bucket_ms.max(1)], |r| {
            let avg_ttft: Option<f64> = r.get(3)?;
            let avg_dur: Option<f64> = r.get(4)?;
            Ok(json!({
                "t": r.get::<_, i64>(0)?,
                "requests": r.get::<_, i64>(1)?,
                "tokens": r.get::<_, i64>(2)?,
                "avg_ttft_ms": avg_ttft.map(|v| v.round() as i64),
                "avg_duration_ms": avg_dur.map(|v| v.round() as i64),
            }))
        })?;
        rows.collect::<rusqlite::Result<Vec<Value>>>()?
    };

    Ok(json!({
        "now_ms": now,
        "since_ms": since_ms,
        "summary": summary,
        "rpm_now": rpm_now,
        "by_model": by_model,
        "by_surface": by_surface,
        "timeseries": { "bucket_ms": bucket_ms, "points": points },
    }))
}

/// 最近 N 筆明細（id 由大到小）。
pub fn query_recent(path: &Path, limit: i64) -> rusqlite::Result<Value> {
    let conn = open_conn(path)?;
    let mut stmt = conn.prepare(
        "SELECT ts_ms,surface,model,resolved_model,chat_type,stream,success,error,
                prompt_tokens,completion_tokens,reasoning_tokens,total_tokens,ttft_ms,duration_ms,caller
         FROM request_logs ORDER BY id DESC LIMIT ?1",
    )?;
    let rows = stmt.query_map(params![limit.clamp(1, 1000)], |r| {
        Ok(json!({
            "ts_ms": r.get::<_, i64>(0)?,
            "surface": r.get::<_, String>(1)?,
            "model": r.get::<_, String>(2)?,
            "resolved_model": r.get::<_, String>(3)?,
            "chat_type": r.get::<_, String>(4)?,
            "stream": r.get::<_, i64>(5)? != 0,
            "success": r.get::<_, i64>(6)? != 0,
            "error": r.get::<_, Option<String>>(7)?,
            "prompt_tokens": r.get::<_, i64>(8)?,
            "completion_tokens": r.get::<_, i64>(9)?,
            "reasoning_tokens": r.get::<_, i64>(10)?,
            "total_tokens": r.get::<_, i64>(11)?,
            "ttft_ms": r.get::<_, Option<i64>>(12)?,
            "duration_ms": r.get::<_, i64>(13)?,
            "caller": r.get::<_, Option<String>>(14)?,
        }))
    })?;
    let items = rows.collect::<rusqlite::Result<Vec<Value>>>()?;
    Ok(json!({ "items": items }))
}
