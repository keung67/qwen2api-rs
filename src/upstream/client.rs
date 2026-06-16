//! Qwen 上游 HTTP 客戶端，對應 Python `services/qwen_client.py`。
//! 用 reqwest（rustls + http2 + 連線池）；headers 與 Python 對齊。

use crate::error::{AppError, AppResult};
use crate::util::{now_unix, qwen_request_id};
use arc_swap::ArcSwap;
use serde_json::Value;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub const BASE_URL: &str =
    "https://qwen.keungliang.workers.dev";
pub const UA: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

/// signin 的密碼校驗格式（chat.qwen.ai 後端比對的是 sha256 hex，不是明文）。
fn sha256_hex(s: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(s.as_bytes());
    hex::encode(h.finalize())
}

/// 依出口代理建立 reqwest client。proxy 為 None/空 → 不顯式設代理（reqwest 仍會讀 HTTP(S)_PROXY env）。
fn build_http(proxy: Option<&str>) -> reqwest::Client {
    let mut b = reqwest::Client::builder()
        .pool_max_idle_per_host(20)
        .pool_idle_timeout(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(30))
        // 讀取超時放大以支援長任務（工具調用/出圖），靠每次請求個別 timeout 控制
        .timeout(Duration::from_secs(300))
        .gzip(true);
    if let Some(p) = proxy {
        let p = p.trim();
        if !p.is_empty() {
            match reqwest::Proxy::all(p) {
                Ok(px) => {
                    b = b.proxy(px);
                    tracing::info!("[QwenClient] 出口代理已啟用");
                }
                Err(e) => tracing::warn!("[QwenClient] 無效出口代理 {p}: {e}（忽略）"),
            }
        }
    }
    b.build().expect("建立 reqwest client 失敗")
}

pub struct QwenClient {
    /// 可熱抽換的 HTTP client（切換出口代理時整個重建並 swap）。
    http: ArcSwap<reqwest::Client>,
    /// 目前顯式設定的出口代理（None = 用環境變數 / 不走代理）。
    proxy: Mutex<Option<String>>,
}

impl QwenClient {
    pub fn new(proxy: Option<String>) -> Self {
        let http = build_http(proxy.as_deref());
        QwenClient {
            http: ArcSwap::from_pointee(http),
            proxy: Mutex::new(proxy),
        }
    }

    /// 即時切換出口代理（重建 client 並原子抽換）。None/空字串 = 清除。
    pub fn set_proxy(&self, proxy: Option<String>) {
        let normalized = proxy.and_then(|p| {
            let t = p.trim().to_string();
            if t.is_empty() { None } else { Some(t) }
        });
        let http = build_http(normalized.as_deref());
        self.http.store(Arc::new(http));
        *self.proxy.lock().unwrap() = normalized;
    }

    /// 目前出口代理設定（供管理台顯示）。
    pub fn proxy(&self) -> Option<String> {
        self.proxy.lock().unwrap().clone()
    }

    /// 取得目前 client 的複本（reqwest::Client 內部為 Arc，clone 便宜）。
    pub fn client(&self) -> reqwest::Client {
        (**self.http.load()).clone()
    }

    fn req(&self, method: reqwest::Method, url: String, token: &str) -> reqwest::RequestBuilder {
        self.http
            .load()
            .request(method, url)
            .header("Authorization", format!("Bearer {token}"))
            .header("User-Agent", UA)
            .header("Accept", "application/json, text/plain, */*")
            .header("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")
            .header("Referer", "https://chat.qwen.ai/")
            .header("Origin", "https://chat.qwen.ai")
            .header("x-request-id", qwen_request_id())
    }

    /// chat.qwen.ai 純 HTTP 重登（取代過期 / 即將過期的 token）。
    ///
    /// 端點：POST /api/v1/auths/signin，body `{email, password: sha256_hex(plain)}`。
    /// 校驗格式取自 OneDragon 註冊工具 `one_dragon.py:423`，**送明文會回 400 incorrect**。
    /// 成功回 200 + JSON 含 `token`（209 字元 HS256 JWT，TTL ~30 天）。
    /// 細節見 memory `reference-qwen-signin-protocol`。
    pub async fn signin(&self, email: &str, plain_password: &str) -> AppResult<String> {
        if plain_password.is_empty() {
            return Err(AppError::Unauthorized("帳號無 password 欄位，無法重登".into()));
        }
        let url = format!("{BASE_URL}/api/v1/auths/signin");
        let body = serde_json::json!({
            "email": email,
            "password": sha256_hex(plain_password),
        });
        let resp = self
            .http
            .load()
            .post(url)
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .header("User-Agent", UA)
            .header("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")
            .header("Referer", "https://chat.qwen.ai/auth")
            .header("Origin", "https://chat.qwen.ai")
            .header("x-request-id", qwen_request_id())
            .timeout(Duration::from_secs(20))
            .json(&body)
            .send()
            .await
            .map_err(|e| AppError::Upstream(format!("signin 連線失敗: {e}")))?;
        let status = resp.status().as_u16();
        let text = resp.text().await.unwrap_or_default();
        if status == 200 {
            let v: Value = serde_json::from_str(&text)
                .map_err(|e| AppError::Upstream(format!("signin 回應解析失敗: {e}")))?;
            let token = v.get("token").and_then(|t| t.as_str()).unwrap_or("");
            if token.is_empty() {
                return Err(AppError::Upstream("signin 200 但 token 欄位空".into()));
            }
            return Ok(token.to_string());
        }
        // 失敗：把上游 detail 字串（"incorrect" / "not registered" / "..."）抓出
        let detail = serde_json::from_str::<Value>(&text)
            .ok()
            .and_then(|v| v.get("detail").and_then(|d| d.as_str()).map(String::from))
            .unwrap_or_else(|| text.chars().take(160).collect());
        if matches!(status, 400 | 401 | 403) {
            Err(AppError::Unauthorized(format!("signin HTTP {status}: {detail}")))
        } else {
            Err(AppError::Upstream(format!("signin HTTP {status}: {detail}")))
        }
    }

    /// 驗證 token：GET /api/v1/auths/ → 200 且 role==user。
    pub async fn verify_token(&self, token: &str) -> bool {
        if token.is_empty() {
            return false;
        }
        let url = format!("{BASE_URL}/api/v1/auths/");
        let resp = self
            .req(reqwest::Method::GET, url, token)
            .timeout(Duration::from_secs(15))
            .send()
            .await;
        match resp {
            Ok(r) => {
                let status = r.status();
                let text = r.text().await.unwrap_or_default();
                if status.as_u16() == 200 {
                    if let Ok(v) = serde_json::from_str::<Value>(&text) {
                        return v.get("role").and_then(|x| x.as_str()) == Some("user");
                    }
                    // WAF/HTML 頁面：保守不判死
                    let lower = text.to_lowercase();
                    return lower.contains("aliyun_waf") || lower.contains("<!doctype");
                }
                false
            }
            Err(_) => false,
        }
    }

    /// 列出上游模型 GET /api/models → data 陣列。
    pub async fn list_models(&self, token: &str) -> Vec<Value> {
        let url = format!("{BASE_URL}/api/models");
        match self
            .req(reqwest::Method::GET, url, token)
            .timeout(Duration::from_secs(10))
            .send()
            .await
        {
            Ok(r) if r.status().as_u16() == 200 => {
                let v: Value = r.json().await.unwrap_or(Value::Null);
                v.get("data").and_then(|d| d.as_array()).cloned().unwrap_or_default()
            }
            _ => Vec::new(),
        }
    }

    /// 建立會話 POST /api/v2/chats/new → chat_id。
    pub async fn create_chat(&self, token: &str, model: &str, chat_type: &str) -> AppResult<String> {
        let ts = now_unix();
        let body = serde_json::json!({
            "title": format!("api_{ts}"),
            "models": [model],
            "chat_mode": "normal",
            "chat_type": chat_type,
            "timestamp": ts,
        });
        let url = format!("{BASE_URL}/api/v2/chats/new");
        let resp = self
            .req(reqwest::Method::POST, url, token)
            .header("Content-Type", "application/json")
            .timeout(Duration::from_secs(30))
            .json(&body)
            .send()
            .await
            .map_err(|e| AppError::Upstream(format!("create_chat 連線失敗: {e}")))?;
        let status = resp.status().as_u16();
        let text = resp.text().await.unwrap_or_default();
        if status != 200 {
            let lower = text.to_lowercase();
            if status == 401
                || status == 403
                || lower.contains("unauthorized")
                || lower.contains("forbidden")
                || lower.contains("login")
            {
                return Err(AppError::Unauthorized(format!(
                    "create_chat HTTP {status}: {}",
                    &text.chars().take(100).collect::<String>()
                )));
            }
            if status == 429 {
                return Err(AppError::Upstream("429 Too Many Requests".into()));
            }
            return Err(AppError::Upstream(format!(
                "create_chat HTTP {status}: {}",
                &text.chars().take(120).collect::<String>()
            )));
        }
        let data: Value = serde_json::from_str(&text)
            .map_err(|e| AppError::Upstream(format!("create_chat parse error: {e}")))?;
        if data.get("success") != Some(&Value::Bool(true)) {
            let lower = text.to_lowercase();
            if ["html", "login", "unauthorized", "token", "expired", "invalid", "pending", "activation"]
                .iter()
                .any(|k| lower.contains(k))
            {
                return Err(AppError::Unauthorized(format!(
                    "account issue: {}",
                    &text.chars().take(160).collect::<String>()
                )));
            }
            return Err(AppError::Upstream("Qwen API returned success=false".into()));
        }
        data.get("data")
            .and_then(|d| d.get("id"))
            .and_then(|i| i.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| AppError::Upstream("create_chat 缺少 data.id".into()))
    }

    /// 刪除會話 DELETE /api/v2/chats/{chat_id}（200/204/404 視為成功）。
    pub async fn delete_chat(&self, token: &str, chat_id: &str) -> bool {
        if token.is_empty() || chat_id.is_empty() {
            return true;
        }
        let url = format!("{BASE_URL}/api/v2/chats/{chat_id}");
        match self
            .req(reqwest::Method::DELETE, url, token)
            .timeout(Duration::from_secs(20))
            .send()
            .await
        {
            Ok(r) => matches!(r.status().as_u16(), 200 | 204 | 404),
            Err(_) => false,
        }
    }

    /// 帶有限重試的刪除。
    pub async fn delete_chat_reliable(&self, token: &str, chat_id: &str, attempts: u32, delay_ms: u64) {
        let max = attempts.max(1);
        for attempt in 1..=max {
            if self.delete_chat(token, chat_id).await {
                return;
            }
            if attempt < max {
                tokio::time::sleep(Duration::from_millis(delay_ms * attempt as u64)).await;
            }
        }
        tracing::warn!("[DeleteChat] 多次刪除失敗 chat_id={chat_id}");
    }

    /// 發起串流補全，回傳 reqwest Response（呼叫端做 SSE framing）。
    pub async fn start_stream(&self, token: &str, chat_id: &str, payload: &Value) -> AppResult<reqwest::Response> {
        let url = format!("{BASE_URL}/api/v2/chat/completions?chat_id={chat_id}");
        let resp = self
            .req(reqwest::Method::POST, url, token)
            .header("Content-Type", "application/json")
            .header("Accept", "text/event-stream")
            .json(payload)
            .send()
            .await
            .map_err(|e| AppError::Upstream(format!("stream 連線失敗: {e}")))?;
        let status = resp.status().as_u16();
        if status != 200 {
            let body = resp.text().await.unwrap_or_default();
            let body = body.chars().take(200).collect::<String>();
            if status == 429 {
                return Err(AppError::Upstream(format!("429: {body}")));
            }
            if status == 401 || status == 403 {
                return Err(AppError::Unauthorized(format!("HTTP {status}: {body}")));
            }
            return Err(AppError::Upstream(format!("HTTP {status}: {body}")));
        }
        Ok(resp)
    }
}

impl Default for QwenClient {
    fn default() -> Self {
        Self::new(None)
    }
}
