//! 管理台 API（prefix /api/admin），對應 Python `api/admin.py`。
//! 注：帳號自動註冊/激活已移除（見 dev/UPSTREAM.md），相關端點回 501。

use crate::account::Account;
use crate::auth::{verify_admin, User};
use crate::error::AppError;
use crate::media::MediaKind;
use crate::state::AppState;
use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::{json, Value};
use std::collections::HashMap;

macro_rules! admin_guard {
    ($state:expr, $headers:expr) => {
        if let Err(e) = verify_admin(&$state, &$headers).await {
            return e.into_response();
        }
    };
}

pub async fn status(State(state): State<AppState>, headers: HeaderMap) -> Response {
    admin_guard!(state, headers);
    let accounts = state.pool.status().await;
    let per_account = state.pool.per_account_status().await;
    let in_use = accounts.get("in_use").and_then(|v| v.as_i64()).unwrap_or(0);

    let chat_id_pool = json!({
        "total_cached": state.chat_id_pool.total_size().await,
        "target_per_account": state.chat_id_pool.target(),
        "ttl_seconds": state.chat_id_pool.ttl(),
        "per_account": state.chat_id_pool.per_account_sizes().await,
    });

    let no_t2v_count = state.no_t2v.get().await.len();
    Json(json!({
        "accounts": accounts,
        "per_account": per_account,
        "chat_id_pool": chat_id_pool,
        "no_t2v_skipped": no_t2v_count,
        "runtime": { "asyncio_running_tasks": in_use },
        "request_runtime": {
            "mode": "direct_http",
            "browser_required_for_requests": false,
            "description": "普通请求直连 HTTP，不经过浏览器",
        },
        "browser_automation": {
            "mode": "disabled",
            "description": "Rust 版不支持浏览器注册，仅手动注入 token",
        }
    }))
    .into_response()
}

/// 帳號列表（分頁 + 搜尋 + 狀態過濾）。
/// query: page(預設1) page_size(預設50,上限200) q(email/username 子串) status(status_code)。
/// 回應另帶 counts（全量各狀態計數，分頁後統計卡仍正確）與 total（過濾後總數）。
pub async fn list_accounts(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    admin_guard!(state, headers);
    let all = state.pool.list().await;

    // 全量各狀態計數（不受過濾/分頁影響）
    let mut counts: HashMap<String, i64> = HashMap::new();
    for a in &all {
        *counts.entry(a.get_status_code()).or_insert(0) += 1;
    }

    // 過濾條件
    let q = query.get("q").map(|s| s.trim().to_lowercase()).filter(|s| !s.is_empty());
    let status = query.get("status").map(|s| s.trim().to_string()).filter(|s| !s.is_empty());
    let filtered: Vec<&Account> = all
        .iter()
        .filter(|a| {
            let hit_q = match &q {
                Some(kw) => a.email.to_lowercase().contains(kw) || a.username.to_lowercase().contains(kw),
                None => true,
            };
            let hit_status = match &status {
                Some(s) => a.get_status_code().as_str() == s.as_str(),
                None => true,
            };
            hit_q && hit_status
        })
        .collect();

    let total = filtered.len();
    // 分頁（page 從 1 起算）
    let page = query.get("page").and_then(|v| v.parse::<usize>().ok()).unwrap_or(1).max(1);
    let page_size = query.get("page_size").and_then(|v| v.parse::<usize>().ok()).unwrap_or(50).clamp(1, 200);
    let start = (page - 1) * page_size;
    let accs: Vec<Value> = filtered.iter().skip(start).take(page_size).map(|a| a.to_admin_json()).collect();

    Json(json!({
        "accounts": accs,
        "total": total,
        "page": page,
        "page_size": page_size,
        "counts": counts,
    }))
    .into_response()
}

pub async fn add_account(State(state): State<AppState>, headers: HeaderMap, body: axum::body::Bytes) -> Response {
    admin_guard!(state, headers);
    let data: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => return AppError::BadRequest("Invalid JSON body".into()).into_response(),
    };
    let token = data.get("token").and_then(|v| v.as_str()).unwrap_or("").trim().to_string();
    if token.is_empty() {
        return AppError::BadRequest("token is required".into()).into_response();
    }
    let email = data
        .get("email")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("manual_{}@qwen", crate::util::now_millis()));
    let password = data.get("password").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let username = data.get("username").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let cookies = data.get("cookies").and_then(|v| v.as_str()).unwrap_or("").to_string();

    // 注入前驗證 token
    if !state.client.verify_token(&token).await {
        return Json(json!({ "ok": false, "error": "Invalid token (验证失败，请确认Token有效)" })).into_response();
    }
    let acc = Account::new(email.clone(), password, token, cookies, username);
    state.pool.add(acc).await;
    Json(json!({ "ok": true, "email": email })).into_response()
}

pub async fn delete_account(State(state): State<AppState>, headers: HeaderMap, Path(email): Path<String>) -> Response {
    admin_guard!(state, headers);
    state.pool.remove(&email).await;
    Json(json!({ "ok": true })).into_response()
}

/// 單獨驗證帳號（live 探測 token；已移除瀏覽器刷新）。
pub async fn verify_account(State(state): State<AppState>, headers: HeaderMap, Path(email): Path<String>) -> Response {
    admin_guard!(state, headers);
    let token = match state.pool.token_of(&email).await {
        Some(t) => t,
        None => return AppError::NotFound("Account not found".into()).into_response(),
    };
    let valid = state.client.verify_token(&token).await;
    let (sc, st_text, err) = if valid {
        ("valid", "正常", "")
    } else {
        ("auth_error", "鉴权失效", "Token 失效（Rust 版不支持自动刷新，请重新手动注入）")
    };
    state.pool.apply_verify(&email, valid, sc, err).await;
    Json(json!({
        "email": email, "valid": valid, "status_code": sc, "status_text": st_text, "error": err
    }))
    .into_response()
}

/// 單帳號 refresh：跑 chat.qwen.ai signin 拿新 JWT 覆寫。
///
/// 流程：取帳號 password → client.signin(email, password) → pool.replace_token。
/// 失敗時把上游錯誤訊息（"incorrect"/"not registered"/etc.）寫入 last_error 留證。
/// 細節見 memory `reference-qwen-signin-protocol`。
pub async fn resign_account(State(state): State<AppState>, headers: HeaderMap, Path(email): Path<String>) -> Response {
    admin_guard!(state, headers);
    let (_, password) = match state.pool.token_and_password_of(&email).await {
        Some(p) => p,
        None => return AppError::NotFound("Account not found".into()).into_response(),
    };
    if password.is_empty() {
        let msg = "帳號無 password 欄位，無法重登";
        state.pool.apply_verify(&email, false, "auth_error", msg).await;
        return Json(json!({"email": email, "ok": false, "error": msg})).into_response();
    }
    match state.client.signin(&email, &password).await {
        Ok(new_token) => {
            let updated = state.pool.replace_token(&email, new_token.clone()).await;
            Json(json!({
                "email": email,
                "ok": true,
                "updated": updated,
                "token_len": new_token.len(),
            }))
            .into_response()
        }
        Err(e) => {
            let msg = e.to_string();
            state.pool.apply_verify(&email, false, "auth_error", &msg).await;
            Json(json!({"email": email, "ok": false, "error": msg})).into_response()
        }
    }
}

/// 批次 refresh（單次上限 200，避免 HTTP timeout + 風控壓力）。
/// 全表 refresh 由背景 worker 按 exp 過濾後分批跑；此 endpoint 給管理者「手動觸發一輪」。
pub async fn resign_all(State(state): State<AppState>, headers: HeaderMap) -> Response {
    admin_guard!(state, headers);
    let accounts = state.pool.list().await;
    let total = accounts.len();
    let cap = 200usize;
    let mut ok_n = 0usize;
    let mut failed = 0usize;
    let mut skipped_no_pw = 0usize;
    let mut results = Vec::new();
    for a in accounts.into_iter().take(cap) {
        if a.password.is_empty() {
            skipped_no_pw += 1;
            continue;
        }
        match state.client.signin(&a.email, &a.password).await {
            Ok(new_token) => {
                let _ = state.pool.replace_token(&a.email, new_token).await;
                ok_n += 1;
                results.push(json!({"email": a.email, "ok": true}));
            }
            Err(e) => {
                failed += 1;
                let msg = e.to_string();
                state.pool.apply_verify(&a.email, false, "auth_error", &msg).await;
                results.push(json!({"email": a.email, "ok": false, "error": msg}));
            }
        }
        // 風控 jitter：每帳號間 100ms 停頓（保守，背景 worker 應更慢）
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    Json(json!({
        "ok": true,
        "summary": {
            "total": total,
            "attempted": cap.min(total),
            "refreshed": ok_n,
            "failed": failed,
            "skipped_no_password": skipped_no_pw,
        },
        "results": results,
        "note": if total > cap { format!("帳號過多，本次僅 refresh 前 {cap} 個；其餘由背景 worker 按 exp 過濾分批跑") } else { String::new() },
    }))
    .into_response()
}

/// JWT exp 分桶摘要：給前端 dashboard 顯示「N 天內過期 X 個」警示。
/// JWT payload 是 base64(json{id, exp, last_password_change})；解 exp 後分桶。
pub async fn accounts_exp_summary(State(state): State<AppState>, headers: HeaderMap) -> Response {
    admin_guard!(state, headers);
    use crate::util::jwt_exp;
    let accounts = state.pool.list().await;
    let now = crate::util::now_unix();
    let mut total = 0;
    let mut no_token = 0;
    let mut no_exp = 0;
    let mut expired = 0;
    let mut in_7d = 0;
    let mut in_30d = 0;
    let mut after_30d = 0;
    let mut earliest_exp: Option<i64> = None;
    let mut soon_emails: Vec<String> = Vec::new(); // 7 天內到期，up to 30 個
    for a in &accounts {
        total += 1;
        if a.token.is_empty() {
            no_token += 1;
            continue;
        }
        let Some(exp) = jwt_exp(&a.token) else {
            no_exp += 1;
            continue;
        };
        if earliest_exp.map_or(true, |e| exp < e) {
            earliest_exp = Some(exp);
        }
        let days = (exp - now) as f64 / 86400.0;
        if days < 0.0 {
            expired += 1;
        } else if days < 7.0 {
            in_7d += 1;
            if soon_emails.len() < 30 {
                soon_emails.push(a.email.clone());
            }
        } else if days < 30.0 {
            in_30d += 1;
        } else {
            after_30d += 1;
        }
    }
    Json(json!({
        "now": now,
        "total": total,
        "no_token": no_token,
        "no_exp": no_exp,
        "expired": expired,
        "expiring_within_7d": in_7d,
        "expiring_within_30d": in_30d,
        "after_30d": after_30d,
        "earliest_exp_unix": earliest_exp,
        "earliest_exp_days_from_now": earliest_exp.map(|e| (e - now) as f64 / 86400.0),
        "soon_sample": soon_emails,
    }))
    .into_response()
}

/// 全量巡检。
pub async fn verify_all(State(state): State<AppState>, headers: HeaderMap) -> Response {
    admin_guard!(state, headers);
    let accounts = state.pool.list().await;
    let mut valid_n = 0;
    let mut failed = 0;
    let total = accounts.len();
    // 為避免上萬帳號全打爆上游，限制單次巡检數量
    let cap = 200usize;
    let mut results = Vec::new();
    for a in accounts.into_iter().take(cap) {
        let valid = state.client.verify_token(&a.token).await;
        if valid {
            valid_n += 1;
            state.pool.apply_verify(&a.email, true, "valid", "").await;
        } else {
            failed += 1;
            state.pool.apply_verify(&a.email, false, "auth_error", "Token 失效").await;
        }
        results.push(json!({ "email": a.email, "valid": valid }));
    }
    Json(json!({
        "ok": true,
        "results": results,
        "summary": { "total": total, "checked": results.len(), "valid": valid_n, "refreshed": 0, "banned": 0, "failed": failed },
        "concurrency": 1,
        "note": if total > cap { format!("帐号过多，本次仅巡检前 {cap} 个") } else { String::new() },
    }))
    .into_response()
}

/// 帳號註冊（已移除）。
pub async fn register_account(State(state): State<AppState>, headers: HeaderMap) -> Response {
    admin_guard!(state, headers);
    (axum::http::StatusCode::NOT_IMPLEMENTED,
     Json(json!({ "ok": false, "error": "自动注册功能在 Rust 版已移除，请手动注入 token" }))).into_response()
}

/// 帳號激活（已移除）。
pub async fn activate_account(State(state): State<AppState>, headers: HeaderMap, Path(_email): Path<String>) -> Response {
    admin_guard!(state, headers);
    (axum::http::StatusCode::NOT_IMPLEMENTED,
     Json(json!({ "ok": false, "error": "激活功能需浏览器自动化，Rust 版不支持；请手动注入有效 token" }))).into_response()
}

// ---- API Keys ----

pub async fn get_keys(State(state): State<AppState>, headers: HeaderMap) -> Response {
    admin_guard!(state, headers);
    let keys: Vec<String> = state.api_keys.read().await.iter().cloned().collect();
    Json(json!({ "keys": keys })).into_response()
}

pub async fn create_key(State(state): State<AppState>, headers: HeaderMap) -> Response {
    admin_guard!(state, headers);
    let new_key = format!("sk-{}", crate::util::short_id(48));
    {
        let mut keys = state.api_keys.write().await;
        keys.insert(new_key.clone());
    }
    state.save_api_keys().await;
    Json(json!({ "ok": true, "key": new_key })).into_response()
}

pub async fn delete_key(State(state): State<AppState>, headers: HeaderMap, Path(key): Path<String>) -> Response {
    admin_guard!(state, headers);
    {
        let mut keys = state.api_keys.write().await;
        keys.remove(&key);
    }
    state.save_api_keys().await;
    Json(json!({ "ok": true })).into_response()
}

// ---- Settings ----

pub async fn get_settings(State(state): State<AppState>, headers: HeaderMap) -> Response {
    admin_guard!(state, headers);
    let (max_inflight, global_max, _queue) = state.pool.concurrency_config().await;
    let model_aliases: Value = {
        let map = state.model_map.read().await;
        serde_json::to_value(&*map).unwrap_or(json!({}))
    };
    Json(json!({
        "version": "2.0.0",
        "max_inflight_per_account": max_inflight,
        "global_max_inflight": global_max,
        "account_min_interval_ms": state.pool.min_interval_ms(),
        "upstream_proxy": state.client.proxy(),
        "chat_id_pool_target": state.chat_id_pool.target(),
        "chat_id_pool_ttl_seconds": state.chat_id_pool.ttl(),
        "model_aliases": model_aliases,
    }))
    .into_response()
}

pub async fn update_settings(State(state): State<AppState>, headers: HeaderMap, body: axum::body::Bytes) -> Response {
    admin_guard!(state, headers);
    let data: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => return AppError::BadRequest("Invalid JSON".into()).into_response(),
    };
    if let Some(v) = data.get("max_inflight_per_account").and_then(|v| v.as_i64()) {
        state.pool.set_max_inflight(v).await;
    }
    if let Some(v) = data.get("global_max_inflight").and_then(|v| v.as_i64()) {
        state.pool.set_global_max_inflight(v).await;
    }
    if let Some(v) = data.get("account_min_interval_ms").and_then(|v| v.as_u64()) {
        state.pool.set_min_interval(v);
    }
    // 出口全局代理：key 存在即套用（字串=設定，null/空=清除回退環境變數）
    if let Some(v) = data.get("upstream_proxy") {
        let proxy = v.as_str().map(|s| s.to_string());
        state.set_upstream_proxy(proxy).await;
    }
    let target = data.get("chat_id_pool_target").and_then(|v| v.as_u64()).map(|v| v as usize);
    let ttl = data.get("chat_id_pool_ttl_seconds").and_then(|v| v.as_u64());
    if target.is_some() || ttl.is_some() {
        state.chat_id_pool.apply_config(target, ttl).await;
    }
    if let Some(aliases) = data.get("model_aliases").and_then(|v| v.as_object()) {
        let mut map = state.model_map.write().await;
        map.clear();
        for (k, v) in aliases {
            if let Some(s) = v.as_str() {
                map.insert(k.clone(), s.to_string());
            }
        }
    }
    Json(json!({ "ok": true })).into_response()
}

// ---- 請求統計（數據面板）----

/// GET /api/admin/stats?range=1h|6h|24h|7d|all
/// 回傳總覽 summary + 即時 RPM + 按模型/接口分組 + 時序，供數據面板渲染。
pub async fn stats(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    admin_guard!(state, headers);
    let range = query.get("range").map(|s| s.as_str()).unwrap_or("24h");
    let now = crate::util::now_millis();
    const H: i64 = 3_600_000; // 1 小時毫秒
    // (起始時間, 時序分桶大小)：桶數控制在 ~100 以內
    let (since_ms, bucket_ms) = match range {
        "1h" => (now - H, 60_000),            // 1 分鐘桶 → 60 點
        "6h" => (now - 6 * H, 5 * 60_000),    // 5 分鐘桶 → 72 點
        "24h" => (now - 24 * H, 15 * 60_000), // 15 分鐘桶 → 96 點
        "7d" => (now - 7 * 24 * H, 2 * H),    // 2 小時桶 → 84 點
        _ => (0, 24 * H),                     // all：1 天桶
    };
    let path = state.stats.db_path();
    let result = tokio::task::spawn_blocking(move || crate::stats::query_dashboard(&path, since_ms, bucket_ms)).await;
    match result {
        Ok(Ok(mut v)) => {
            if let Some(obj) = v.as_object_mut() {
                obj.insert("range".into(), json!(range));
                obj.insert("dropped".into(), json!(state.stats.dropped_count()));
            }
            Json(v).into_response()
        }
        Ok(Err(e)) => AppError::Internal(format!("統計查詢失敗: {e}")).into_response(),
        Err(e) => AppError::Internal(format!("統計任務失敗: {e}")).into_response(),
    }
}

/// GET /api/admin/stats/recent?limit=50 — 最近 N 筆請求明細。
pub async fn stats_recent(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    admin_guard!(state, headers);
    let limit = query.get("limit").and_then(|v| v.parse::<i64>().ok()).unwrap_or(50);
    let path = state.stats.db_path();
    let result = tokio::task::spawn_blocking(move || crate::stats::query_recent(&path, limit)).await;
    match result {
        Ok(Ok(v)) => Json(v).into_response(),
        Ok(Err(e)) => AppError::Internal(format!("統計查詢失敗: {e}")).into_response(),
        Err(e) => AppError::Internal(format!("統計任務失敗: {e}")).into_response(),
    }
}

// ---- 媒體任務佇列（圖片/影片背景生成 + 本地保存）----

/// POST /api/admin/media/tasks — 提交生成任務（支援單 prompt 或多 prompts 批次）。
/// body: { kind:"image"|"video", prompt|prompts, ratio, size, n, width, height }
pub async fn media_submit(State(state): State<AppState>, headers: HeaderMap, body: axum::body::Bytes) -> Response {
    admin_guard!(state, headers);
    let data: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => return AppError::BadRequest("Invalid JSON".into()).into_response(),
    };
    let kind = MediaKind::parse(data.get("kind").and_then(|v| v.as_str()).unwrap_or("image"));
    // 收集 prompt(s)
    let mut prompts: Vec<String> = Vec::new();
    if let Some(arr) = data.get("prompts").and_then(|v| v.as_array()) {
        for v in arr {
            if let Some(s) = v.as_str() {
                let t = s.trim();
                if !t.is_empty() {
                    prompts.push(t.to_string());
                }
            }
        }
    } else if let Some(s) = data.get("prompt").and_then(|v| v.as_str()) {
        let t = s.trim();
        if !t.is_empty() {
            prompts.push(t.to_string());
        }
    }
    if prompts.is_empty() {
        return AppError::BadRequest("prompt is required".into()).into_response();
    }
    // 共用生成參數
    let mut prm = serde_json::Map::new();
    for k in ["ratio", "aspect_ratio", "size", "width", "height", "n"] {
        if let Some(v) = data.get(k) {
            prm.insert(k.to_string(), v.clone());
        }
    }
    let params = Value::Object(prm);
    let caller = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim().trim_start_matches("Bearer ").trim().to_string());

    let mut ids = Vec::new();
    for p in &prompts {
        if let Some(id) = state.media_queue.submit(kind, p, params.clone(), caller.clone()).await {
            ids.push(id);
        }
    }
    Json(json!({ "ok": true, "ids": ids, "count": ids.len(), "kind": kind.as_str() })).into_response()
}

/// GET /api/admin/media/tasks?kind=image|video&limit=100 — 任務列表（含結果，供畫廊）。
pub async fn media_tasks(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    admin_guard!(state, headers);
    let kind = query.get("kind").map(|s| MediaKind::parse(s));
    let limit = query.get("limit").and_then(|v| v.parse::<i64>().ok()).unwrap_or(100);
    let v = state.media_queue.store.list(kind, limit).await;
    Json(v).into_response()
}

// ---- Users（可選）----

pub async fn list_users(State(state): State<AppState>, headers: HeaderMap) -> Response {
    admin_guard!(state, headers);
    Json(json!({ "users": state.users_db.get().await })).into_response()
}

pub async fn create_user(State(state): State<AppState>, headers: HeaderMap, body: axum::body::Bytes) -> Response {
    admin_guard!(state, headers);
    let data: Value = serde_json::from_slice(&body).unwrap_or(json!({}));
    let name = data.get("name").and_then(|v| v.as_str()).unwrap_or("user").to_string();
    let quota = data.get("quota").and_then(|v| v.as_i64()).unwrap_or(1_000_000);
    let user = User {
        id: format!("sk-{}", crate::util::short_id(32)),
        name,
        quota,
        used_tokens: 0,
    };
    state.users_db.update(|users| users.push(user.clone())).await;
    Json(serde_json::to_value(&user).unwrap()).into_response()
}
