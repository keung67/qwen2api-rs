//! 全域設定，對應 Python `core/config.py`。
//! 從環境變數 / `.env` 讀取，並提供 MODEL_MAP 模型別名映射。

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;

fn env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn env_str(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

/// 執行期設定。部分欄位（並發/預熱池）可在管理台運行時調整，故用內部可變的 AppState 持有副本。
#[derive(Debug, Clone)]
pub struct Settings {
    pub port: u16,
    pub admin_key: String,

    // 並發 / 容災
    pub max_inflight_per_account: i64,
    pub max_retries: u32,
    /// auth_error 連續失敗門檻：到達此值才永久標 valid=false（避免單次 401 誤殺）。
    /// 預設 3；中間幾次仍保留 valid、繼續嘗試，由 mark_success 重置計數。
    pub auth_error_fail_threshold: u32,
    pub account_min_interval_ms: u64,
    pub request_jitter_min_ms: u64,
    pub request_jitter_max_ms: u64,
    pub rate_limit_base_cooldown: u64,
    pub rate_limit_max_cooldown: u64,

    // 上游 chat 生命週期
    pub chat_delete_retry_attempts: u32,
    pub chat_delete_retry_delay_ms: u64,
    pub chat_id_prewarm_target_per_account: usize,
    pub chat_id_prewarm_ttl_seconds: u64,
    /// 預熱池最多覆蓋幾個帳號（優化：帳號數可能上萬，避免對全部帳號建會話打爆上游）。
    pub chat_id_prewarm_max_accounts: usize,

    pub log_level: String,

    // 資料檔案路徑
    pub data_dir: PathBuf,
    pub accounts_file: PathBuf,
    pub users_file: PathBuf,
    pub api_keys_file: PathBuf,
    pub config_file: PathBuf,
    /// 請求統計 SQLite 檔（data/stats.db）。
    pub stats_file: PathBuf,
    /// 媒體任務 SQLite 檔（data/media.db）。
    pub media_db_file: PathBuf,
    /// 生成媒體本地保存目錄（data/generated_media）。
    pub media_dir: PathBuf,
    /// 媒體任務 worker 並發數。
    pub media_concurrency: usize,
    /// 媒體生成的應用層重試次數（輪換帳號找有額度者）。
    pub media_max_attempts: u32,
    /// t2v 已知無權限的帳號集（持久化）。
    pub no_t2v_file: PathBuf,

    // 上下文 / 附件
    pub context_inline_max_chars: usize,
    pub context_force_file_max_chars: usize,
    pub context_attachment_ttl_seconds: u64,
    pub context_upload_parse_timeout_seconds: u64,
    pub context_generated_dir: PathBuf,
    pub context_cache_file: PathBuf,
    pub uploaded_files_file: PathBuf,
    pub context_affinity_file: PathBuf,
    pub context_allowed_user_exts: String,

    /// chat_id 預熱池預設模型（動態：啟動後若抓到上游模型列表會覆蓋）
    pub default_model: String,

    /// 出口全局代理初始值（優先 UPSTREAM_PROXY，否則沿用 HTTP(S)_PROXY env）。
    /// 之後可在管理台即時覆蓋並持久化。
    pub upstream_proxy: Option<String>,

    /// Pillar 2：就緒帳號索引（Ready-Set）。false 回退舊 O(n) 掃描（kill-switch）。預設開。
    pub pool_ready_index: bool,
    /// Pillar 3：連線保活。每 N 秒對上游送一次輕量請求保溫一條連線；0=關閉（預設，風控敏感）。
    pub conn_keepalive_seconds: u64,

    /// Token refresh worker（解決「JWT 30 天 TTL → 16k 帳號集體過期」）。
    /// 每 INTERVAL_HOURS 跑一輪：解所有帳號 JWT exp，篩出 exp < now+AHEAD_DAYS 的，分批跑
    /// chat.qwen.ai signin 拿新 token；每帳號間 JITTER_MS 隨機停頓避風控。0=關閉 worker。
    pub token_refresh_interval_hours: u64,
    pub token_refresh_ahead_days: i64,
    pub token_refresh_batch_per_cycle: usize,
    pub token_refresh_jitter_min_ms: u64,
    pub token_refresh_jitter_max_ms: u64,
}

/// 依序讀取代理環境變數（含大小寫變體）。
fn read_proxy_env() -> Option<String> {
    for key in ["UPSTREAM_PROXY", "HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy", "ALL_PROXY", "all_proxy"] {
        if let Ok(v) = env::var(key) {
            let v = v.trim().to_string();
            if !v.is_empty() {
                return Some(v);
            }
        }
    }
    None
}

impl Settings {
    pub fn from_env() -> Self {
        let data_dir = PathBuf::from(env_str("DATA_DIR", "./data"));
        let p = |key: &str, default_name: &str| -> PathBuf {
            match env::var(key) {
                Ok(v) => PathBuf::from(v),
                Err(_) => data_dir.join(default_name),
            }
        };
        Settings {
            port: env_or("PORT", 7860u16),
            admin_key: env_str("ADMIN_KEY", "admin"),
            max_inflight_per_account: env::var("MAX_INFLIGHT_PER_ACCOUNT")
                .or_else(|_| env::var("MAX_INFLIGHT"))
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(2),
            max_retries: env_or("MAX_RETRIES", 3u32),
            auth_error_fail_threshold: env_or("AUTH_ERROR_FAIL_THRESHOLD", 3u32),
            // 風控：同帳號兩次請求之間的強制休息（毫秒）。預設 3s，避免單帳號被打太快而封號。
            account_min_interval_ms: env_or("ACCOUNT_MIN_INTERVAL_MS", 3000u64),
            request_jitter_min_ms: env_or("REQUEST_JITTER_MIN_MS", 0u64),
            request_jitter_max_ms: env_or("REQUEST_JITTER_MAX_MS", 0u64),
            rate_limit_base_cooldown: env_or("RATE_LIMIT_BASE_COOLDOWN", 600u64),
            rate_limit_max_cooldown: env_or("RATE_LIMIT_MAX_COOLDOWN", 3600u64),
            chat_delete_retry_attempts: env_or("CHAT_DELETE_RETRY_ATTEMPTS", 3u32),
            chat_delete_retry_delay_ms: (env_or::<f64>("CHAT_DELETE_RETRY_DELAY_SECONDS", 0.5) * 1000.0) as u64,
            chat_id_prewarm_target_per_account: env_or("CHAT_ID_PREWARM_TARGET_PER_ACCOUNT", 5usize),
            chat_id_prewarm_ttl_seconds: env_or("CHAT_ID_PREWARM_TTL_SECONDS", 120u64),
            chat_id_prewarm_max_accounts: env_or("CHAT_ID_PREWARM_MAX_ACCOUNTS", 8usize),
            log_level: env_str("LOG_LEVEL", "info"),
            accounts_file: p("ACCOUNTS_FILE", "accounts.json"),
            users_file: p("USERS_FILE", "users.json"),
            api_keys_file: p("API_KEYS_FILE", "api_keys.json"),
            config_file: p("CONFIG_FILE", "config.json"),
            stats_file: p("STATS_FILE", "stats.db"),
            media_db_file: p("MEDIA_DB_FILE", "media.db"),
            media_dir: p("MEDIA_DIR", "generated_media"),
            media_concurrency: env_or("MEDIA_CONCURRENCY", 3usize),
            media_max_attempts: env_or("MEDIA_MAX_ATTEMPTS", 10u32),
            no_t2v_file: p("NO_T2V_FILE", "no_t2v_accounts.json"),
            context_inline_max_chars: env_or("CONTEXT_INLINE_MAX_CHARS", 4000usize),
            context_force_file_max_chars: env_or("CONTEXT_FORCE_FILE_MAX_CHARS", 10000usize),
            context_attachment_ttl_seconds: env_or("CONTEXT_ATTACHMENT_TTL_SECONDS", 1800u64),
            context_upload_parse_timeout_seconds: env_or("CONTEXT_UPLOAD_PARSE_TIMEOUT_SECONDS", 60u64),
            context_generated_dir: p("CONTEXT_GENERATED_DIR", "context_files"),
            context_cache_file: p("CONTEXT_CACHE_FILE", "context_cache.json"),
            uploaded_files_file: p("UPLOADED_FILES_FILE", "uploaded_files.json"),
            context_affinity_file: p("CONTEXT_AFFINITY_FILE", "session_affinity.json"),
            context_allowed_user_exts: env_str(
                "CONTEXT_ALLOWED_USER_EXTS",
                "txt,md,json,log,xml,yaml,yml,csv,html,css,py,js,ts,java,c,cpp,cs,php,go,rb,sh,zsh,ps1,bat,cmd,pdf,doc,docx,ppt,pptx,xls,xlsx,png,jpg,jpeg,webp,gif,tiff,bmp,svg",
            ),
            data_dir,
            default_model: env_str("DEFAULT_MODEL", "qwen3.7-plus"),
            upstream_proxy: read_proxy_env(),
            // 預設開；設 POOL_READY_INDEX=0/false/off/no 可回退舊掃描。
            pool_ready_index: env::var("POOL_READY_INDEX")
                .ok()
                .map(|v| !matches!(v.trim().to_lowercase().as_str(), "0" | "false" | "off" | "no"))
                .unwrap_or(true),
            conn_keepalive_seconds: env_or("CONN_KEEPALIVE_SECONDS", 0u64),
            // Token refresh worker（預設開啟，每 6h 跑、提前 7 天刷、每輪上限 500、每帳號 2-5 秒 jitter）
            token_refresh_interval_hours: env_or("TOKEN_REFRESH_INTERVAL_HOURS", 6u64),
            token_refresh_ahead_days: env_or("TOKEN_REFRESH_AHEAD_DAYS", 7i64),
            token_refresh_batch_per_cycle: env_or("TOKEN_REFRESH_BATCH_PER_CYCLE", 500usize),
            token_refresh_jitter_min_ms: env_or("TOKEN_REFRESH_JITTER_MIN_MS", 2000u64),
            token_refresh_jitter_max_ms: env_or("TOKEN_REFRESH_JITTER_MAX_MS", 5000u64),
        }
    }
}

/// 預設模型別名映射（對應 Python MODEL_MAP）。下游傳入的模型名 → Qwen 實際 base model。
/// 注意：上游現役旗艦已是 qwen3.7-plus（見 dev/UPSTREAM.md），故預設指向之。
pub fn default_model_map() -> HashMap<String, String> {
    let plus = "qwen3.7-plus";
    let flash = "qwen3.5-flash";
    let pairs: &[(&str, &str)] = &[
        // OpenAI
        ("gpt-4o", plus), ("gpt-4o-mini", flash), ("gpt-4-turbo", plus), ("gpt-4", plus),
        ("gpt-4.1", plus), ("gpt-4.1-mini", flash), ("gpt-3.5-turbo", flash),
        ("gpt-5", plus), ("o1", plus), ("o1-mini", flash), ("o3", plus), ("o3-mini", flash),
        // Anthropic
        ("claude-opus-4-6", plus), ("claude-opus-4-8", plus), ("claude-sonnet-4-5", plus),
        ("claude-sonnet-4-6", plus), ("claude-3-opus", plus), ("claude-3.5-sonnet", plus),
        ("claude-3-sonnet", plus), ("claude-3-haiku", flash),
        // Gemini
        ("gemini-2.5-pro", plus), ("gemini-2.5-flash", flash),
        // Qwen aliases
        ("qwen", plus), ("qwen-max", plus), ("qwen-plus", plus), ("qwen-turbo", flash),
        ("qwen3.6-plus", plus), // 舊預設 → 導向現役
        // DeepSeek
        ("deepseek-chat", plus), ("deepseek-reasoner", plus),
    ];
    pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
}

/// 從 model_aliases 解析實際 base model；命中則回映射值，否則原樣返回。
pub fn resolve_model(map: &HashMap<String, String>, name: &str) -> String {
    map.get(name).cloned().unwrap_or_else(|| name.to_string())
}

/// api_keys.json 的結構 `{"keys": [...]}`
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ApiKeysFile {
    #[serde(default)]
    pub keys: Vec<String>,
}
