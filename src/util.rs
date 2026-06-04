//! 共用工具函式。

use base64::Engine;
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};

/// 解 chat.qwen.ai web JWT 的 exp 欄位（unix 秒），失敗回 None。
/// 不驗簽（驗簽密鑰只有 Qwen 後端有）——僅用於 client 端時序判定（refresh 排程、過期分桶等）。
pub fn jwt_exp(token: &str) -> Option<i64> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return None;
    }
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(parts[1]).ok()?;
    let v: Value = serde_json::from_slice(&payload).ok()?;
    v.get("exp").and_then(|e| e.as_i64())
}

/// Unix epoch 秒（浮點，對齊 Python time.time()）。
pub fn now_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

/// Unix epoch 秒（整數）。
pub fn now_unix() -> i64 {
    now_secs() as i64
}

/// Unix epoch 毫秒。
pub fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// 產生短 id（hex 前 n 碼），對齊 Python uuid4().hex[:n]。
pub fn short_id(n: usize) -> String {
    let s = uuid::Uuid::new_v4().simple().to_string();
    s[..n.min(s.len())].to_string()
}

pub fn uuid4() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Python len()：計算 Unicode 標量數（非 bytes）。
pub fn char_len(s: &str) -> usize {
    s.chars().count()
}

/// UTC 時間格式化為 (ISO8601 basic "YYYYMMDDTHHMMSSZ", date "YYYYMMDD")。
/// 自行做曆法換算以避免引入 chrono（Howard Hinnant civil_from_days 演算法）。
pub fn utc_iso8601_basic() -> (String, String) {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0) as i64;
    let days = secs.div_euclid(86400);
    let rem = secs.rem_euclid(86400);
    let (hh, mm, ss) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    // civil_from_days
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };
    let date = format!("{:04}{:02}{:02}", year, m, d);
    let iso = format!("{}T{:02}{:02}{:02}Z", date, hh, mm, ss);
    (iso, date)
}

/// 抖動毫秒（用於帳號最小間隔/限流冷卻）。
pub fn jitter_ms(min_ms: u64, max_ms: u64) -> u64 {
    if max_ms <= min_ms {
        return min_ms;
    }
    use rand::Rng;
    rand::thread_rng().gen_range(min_ms..=max_ms)
}
