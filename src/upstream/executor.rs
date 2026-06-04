//! 請求執行器：帳號層重試、預熱池快路徑、SSE framing、資源清理。
//! 對應 Python `upstream/qwen_executor.py` 的 chat_stream_events_with_retry。

use super::chat_id_pool::ChatIdPool;
use super::client::QwenClient;
use super::payload::{build_chat_payload, BuildPayloadArgs, ImageOptions};
use super::sse::{delta_has_answer_content, extract_upstream_error, parse_sse_chunk, QwenDelta};
use crate::account::AccountPool;
use crate::config::Settings;
use async_stream::stream;
use futures_util::Stream;
use futures_util::StreamExt;
use std::collections::HashSet;
use std::sync::Arc;

/// 執行器輸出的事件。
#[derive(Debug, Clone)]
pub enum UpstreamEvent {
    Meta { chat_id: String, email: String },
    Delta(QwenDelta),
    Done,
    Error(String),
    /// 即將跨帳號重試（通常因上游 quota/rate-limit 中斷）。
    /// 下游（mod.rs）收到後應重置 `answer_buf` / `ReasoningTracker` / `streamed_content`
    /// 等本輪累積狀態，避免上一輪殘片混入下一輪解析或重複 yield。
    Retrying,
}

#[derive(Clone)]
pub struct StreamParams {
    pub model: String,
    pub content: String,
    pub has_custom_tools: bool,
    pub files: Vec<serde_json::Value>,
    pub chat_type: String,
    pub image_options: Option<ImageOptions>,
    pub thinking_enabled: Option<bool>,
    pub enable_search: bool,
    pub fixed_account: Option<String>,
    pub existing_chat_id: Option<String>,
    pub delete_on_close: bool,
    pub use_prewarmed: bool,
    /// 本次請求的帳號層重試上限（None＝用 executor 預設）。影像/影片把重試交給應用層精準控制，故設 1。
    pub max_retries: Option<u32>,
    /// 取帳號時要繞過的 email 集合（如 t2v 已知無權限的帳號）。
    pub exclude: HashSet<String>,
}

impl Default for StreamParams {
    fn default() -> Self {
        StreamParams {
            model: String::new(),
            content: String::new(),
            has_custom_tools: false,
            files: Vec::new(),
            chat_type: "t2t".into(),
            image_options: None,
            thinking_enabled: None,
            enable_search: false,
            fixed_account: None,
            existing_chat_id: None,
            delete_on_close: true,
            use_prewarmed: true,
            max_retries: None,
            exclude: HashSet::new(),
        }
    }
}

#[derive(Clone)]
pub struct Executor {
    pub pool: Arc<AccountPool>,
    pub client: Arc<QwenClient>,
    pub chat_id_pool: Arc<ChatIdPool>,
    pub max_retries: u32,
    pub delete_attempts: u32,
    pub delete_delay_ms: u64,
}

/// 取消安全的資源 guard：drop 時（含 client 中途斷線導致 stream future 被丟棄）
/// 一定釋放帳號並刪除本次建立的上游會話，各一次。對應修復「斷線洩漏」。
struct StreamGuard {
    pool: Arc<AccountPool>,
    client: Arc<QwenClient>,
    /// Some = 由帳號池取得，需 release；fixed_account 則為 None。
    email: Option<String>,
    token: String,
    /// Some = 本次建立、需刪除的會話。
    chat_id: Option<String>,
    delete_attempts: u32,
    delete_delay_ms: u64,
    armed: bool,
}

impl Drop for StreamGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let pool = self.pool.clone();
        let client = self.client.clone();
        let email = self.email.clone();
        let token = self.token.clone();
        let chat_id = self.chat_id.clone();
        let (attempts, delay) = (self.delete_attempts, self.delete_delay_ms);
        // Drop 不能 await → spawn detached 清理任務
        tokio::spawn(async move {
            if let Some(cid) = chat_id {
                client.delete_chat_reliable(&token, &cid, attempts, delay).await;
            }
            if let Some(em) = email {
                pool.release(&em).await;
            }
        });
    }
}

impl Executor {
    pub fn new(pool: Arc<AccountPool>, client: Arc<QwenClient>, chat_id_pool: Arc<ChatIdPool>, settings: &Settings) -> Self {
        Executor {
            pool,
            client,
            chat_id_pool,
            max_retries: settings.max_retries,
            delete_attempts: settings.chat_delete_retry_attempts,
            delete_delay_ms: settings.chat_delete_retry_delay_ms,
        }
    }

    /// 取得會話 id：優先用既有；否則嘗試預熱池；再否則建新會話。
    /// 回傳 (chat_id, owns)（owns=true 表示是本次建立、結束時可刪）。
    async fn obtain_chat_id(
        &self,
        token: &str,
        email: &str,
        model: &str,
        chat_type: &str,
        existing: &Option<String>,
        use_prewarmed: bool,
    ) -> Result<(String, bool), crate::error::AppError> {
        if let Some(cid) = existing {
            return Ok((cid.clone(), false));
        }
        if use_prewarmed && chat_type == "t2t" {
            // 傳入 token：回補與過期刪除都用它，避免在熱路徑查 pool（O(n) 鎖競爭）。
            if let Some(cid) = self.chat_id_pool.acquire(email, token, model).await {
                tracing::debug!("[執行器] 預熱池命中 email={email} chat={cid}");
                return Ok((cid, true));
            }
        }
        let cid = self.client.create_chat(token, model, chat_type).await?;
        Ok((cid, true))
    }

    /// 主串流：產生 UpstreamEvent 流。內含帳號層重試。
    pub fn run_stream(self: Arc<Self>, params: StreamParams) -> impl Stream<Item = UpstreamEvent> {
        stream! {
            // 初始 exclude：呼叫端傳入的「已知不可用」集合（如 t2v 無權限帳號）
            let mut exclude: HashSet<String> = params.exclude.clone();
            let mut last_error: Option<String> = None;
            let attempts = if params.fixed_account.is_some() {
                1
            } else {
                params.max_retries.unwrap_or(self.max_retries)
            };

            for attempt in 0..attempts {
                // 1) 取得帳號
                let handle = if let Some(email) = &params.fixed_account {
                    match self.pool.token_of(email).await {
                        Some(token) => crate::account::AccountHandle { email: email.clone(), token },
                        None => { yield UpstreamEvent::Error(format!("指定帳號不存在: {email}")); return; }
                    }
                } else {
                    match self.pool.acquire_wait(None, &exclude, 60.0).await {
                        Some(h) => h,
                        None => {
                            // 若先前嘗試已有真實錯誤，浮現它而非掩蓋成「無可用帳號」
                            let msg = last_error.clone().unwrap_or_else(|| {
                                "帳號池無可用帳號（全忙或限流）".into()
                            });
                            yield UpstreamEvent::Error(msg);
                            return;
                        }
                    }
                };
                let email = handle.email.clone();
                let token = handle.token.clone();
                let is_pool_acquired = params.fixed_account.is_none();

                // 取消安全 guard：取得帳號後立刻建立，所有離開路徑（成功/重試/錯誤/斷線）皆由它清理。
                let mut guard = StreamGuard {
                    pool: self.pool.clone(),
                    client: self.client.clone(),
                    email: if is_pool_acquired { Some(email.clone()) } else { None },
                    token: token.clone(),
                    chat_id: None,
                    delete_attempts: self.delete_attempts,
                    delete_delay_ms: self.delete_delay_ms,
                    armed: true,
                };

                // 2) 取得會話
                let (chat_id, owns) = match self
                    .obtain_chat_id(&token, &email, &params.model, &params.chat_type, &params.existing_chat_id, params.use_prewarmed)
                    .await
                {
                    Ok(v) => v,
                    Err(e) => {
                        // guard 在 continue 時 drop → 釋放帳號
                        let msg = e.to_string();
                        last_error = Some(msg.clone());
                        self.classify_and_mark(&email, &msg).await;
                        exclude.insert(email.clone());
                        tracing::warn!("[執行器] 建會話失敗 第{}次 email={email} err={msg}", attempt + 1);
                        continue;
                    }
                };
                // 本次建立的會話交由 guard 負責刪除
                if owns && params.delete_on_close {
                    guard.chat_id = Some(chat_id.clone());
                }

                yield UpstreamEvent::Meta { chat_id: chat_id.clone(), email: email.clone() };

                // 3) 串流
                let payload = build_chat_payload(&BuildPayloadArgs {
                    chat_id: &chat_id,
                    model: &params.model,
                    content: &params.content,
                    has_custom_tools: params.has_custom_tools,
                    files: params.files.clone(),
                    chat_type: &params.chat_type,
                    image_options: params.image_options.clone(),
                    thinking_enabled: params.thinking_enabled,
                    enable_search: params.enable_search,
                });

                let resp = match self.client.start_stream(&token, &chat_id, &payload).await {
                    Ok(r) => r,
                    Err(e) => {
                        // 串流尚未開始 → 可重試；guard 在 continue 時 drop → 刪會話 + 釋放帳號
                        let msg = e.to_string();
                        last_error = Some(msg.clone());
                        self.classify_and_mark(&email, &msg).await;
                        exclude.insert(email.clone());
                        tracing::warn!("[執行器] 串流啟動失敗 第{}次 email={email} err={msg}", attempt + 1);
                        continue;
                    }
                };

                // 4) 消費 SSE bytes
                let mut byte_stream = resp.bytes_stream();
                let mut buffer: Vec<u8> = Vec::with_capacity(8192);
                let mut stream_error: Option<String> = None;

                // 追蹤是否至少有一筆「真正回覆」delta（answer-phase content）。
                // 對 thinking 模型，上游可能跑完 thinking 就斷線（quota 中斷、上游降級），
                // 此時 client（Claude/OpenAI/Gemini）只看到「Thought for Xs」然後莫名 stop，
                // 沒有任何 text。要把這個情境視為瞬時錯誤跨帳號重試。
                // had_answer_content：客戶端真的收到了任何 answer-phase content。
                // 在「還沒收到 answer content」前就出錯/結束 → 重新取另一個帳號重試。
                // media（t2i/t2v）content 本來就可能在 phase 變化中為空，仍走 had_answer_content
                // 判定但 mod.rs 對影片 URL 是經 phase 出來的特殊 content（非空），不會誤判。
                let mut had_answer_content = false;

                'consume: loop {
                    match byte_stream.next().await {
                        Some(Ok(chunk)) => {
                            buffer.extend_from_slice(&chunk);
                            // 以 b"\n\n" 切分完整訊息（\n 不會出現在 UTF-8 多位元組序列中）
                            loop {
                                let pos = find_subslice(&buffer, b"\n\n");
                                let Some(p) = pos else { break };
                                let msg_bytes: Vec<u8> = buffer.drain(..p + 2).collect();
                                let msg = String::from_utf8_lossy(&msg_bytes[..p]);
                                if let Some(err) = extract_upstream_error(&msg) {
                                    stream_error = Some(err);
                                    break 'consume;
                                }
                                for d in parse_sse_chunk(&msg) {
                                    if delta_has_answer_content(&d) {
                                        had_answer_content = true;
                                    }
                                    yield UpstreamEvent::Delta(d);
                                }
                            }
                        }
                        Some(Err(e)) => {
                            stream_error = Some(format!("串流讀取錯誤: {e}"));
                            break 'consume;
                        }
                        None => break 'consume,
                    }
                }
                // 處理殘餘 buffer
                if stream_error.is_none() && !buffer.is_empty() {
                    let msg = String::from_utf8_lossy(&buffer);
                    if let Some(err) = extract_upstream_error(&msg) {
                        stream_error = Some(err);
                    } else {
                        for d in parse_sse_chunk(&msg) {
                            if delta_has_answer_content(&d) {
                                had_answer_content = true;
                            }
                            yield UpstreamEvent::Delta(d);
                        }
                    }
                }

                // 5) 結束（清理由 guard 在 drop 時統一處理：釋放帳號 + 刪會話，恰好一次）
                match stream_error {
                    None => {
                        // 空回覆判定：t2t 跑完整輪但沒收到 answer-phase content → 視為瞬時失敗、跨帳號重試。
                        // 對 thinking 模型，上游可能只送 reasoning 就 [DONE]（中斷或上游異常），
                        // client 等於看到「Thought for Xs」然後停下；對 client 是嚴重 UX 問題。
                        // 已 yield 的 reasoning/partial delta 在客戶端「最後再來一輪」覆寫即可
                        // （Anthropic translator 是用 content_block index 各自獨立，重來會開新 thinking/text block；
                        // OpenAI translator 是純增量，重來會接續再吐；雖非完美，比直接斷掉好得多）。
                        if params.chat_type == "t2t" && !had_answer_content {
                            let err = "上游回應無 answer content（可能只有 reasoning 或全空），視為瞬時錯誤重試".to_string();
                            last_error = Some(err.clone());
                            // 不 classify_and_mark：避免把這帳號標 invalid（401/429 pattern 都不 match）
                            exclude.insert(email.clone());
                            tracing::warn!("[執行器] 無 answer 內容視為失敗 第{}次 email={email}", attempt + 1);
                            continue;
                        }
                        if is_pool_acquired {
                            self.pool.mark_success(&email).await;
                        }
                        yield UpstreamEvent::Done;
                        return; // guard drop → 刪會話 + 釋放帳號
                    }
                    Some(err) => {
                        // 重試判定優先序：
                        // 1) 還沒看到 answer-phase content → 跨帳號重試（vehicle 客戶端尚未收到答覆）
                        // 2) 已看到 answer content **但** 屬於「換帳號即可解」類錯誤（quota/rate-limit/
                        //    internal_error/503）→ 也重試，並先發 Retrying 給下游重置 buffer。
                        //    對 thinking 模型尤為關鍵：上游常先送 1-2 字 content delta 才拋 quota，
                        //    舊版判 had_answer_content=true 不重試 → 把 raw error 吐給 client。
                        // 3) 其他（permanent / client-side）→ 直接吐 Error 給 client
                        let swap_retryable = is_account_swap_retryable(&err);
                        if had_answer_content && !swap_retryable {
                            yield UpstreamEvent::Error(err);
                            return; // guard drop → 清理
                        }
                        if had_answer_content {
                            // 通知 mod.rs 清空累積的 answer_buf / reasoning tracker，
                            // 避免兩輪殘片相黏導致 tool_call 解析錯亂
                            yield UpstreamEvent::Retrying;
                        }
                        last_error = Some(err.clone());
                        self.classify_and_mark(&email, &err).await;
                        exclude.insert(email.clone());
                        let stage = if had_answer_content { "已部分輸出但屬於換帳號可解類" } else { "無 answer 內容前" };
                        tracing::warn!("[執行器] 串流錯誤（{stage}）可重試 第{}次 email={email} err={err}", attempt + 1);
                        continue; // guard drop → 清理本次帳號/會話，下輪重新取得
                    }
                }
            }

            yield UpstreamEvent::Error(format!(
                "全部 {} 次嘗試失敗。最後錯誤: {}",
                attempts,
                last_error.unwrap_or_else(|| "未知".into())
            ));
        }
    }

    /// 依錯誤訊息分類並標記帳號狀態。
    async fn classify_and_mark(&self, email: &str, err: &str) {
        // 注意：此函式只「標記帳號」，是否走重試另由呼叫端用 is_account_swap_retryable 判斷。
        let lower = err.to_lowercase();
        // 含影片/影像每日額度上限（code=RateLimited / "upper limit for today's usage"）：
        // 視為限流並冷卻，使帳號池輪換到其他帳號（重試找有額度者）。
        if lower.contains("429")
            || lower.contains("rate limit")
            || lower.contains("ratelimited")
            || lower.contains("too many")
            || lower.contains("upper limit")
            || lower.contains("today's usage")
        {
            self.pool.mark_rate_limited(email).await;
        } else if lower.contains("unauthorized") || lower.contains("401") || lower.contains("403") {
            let reason = if lower.contains("activation") || lower.contains("pending") {
                "pending_activation"
            } else {
                "auth_error"
            };
            // 把原始上游錯誤訊息一併寫進 last_error；mark_invalid 對 auth_error 帶門檻，
            // 連續失敗 N 次（預設 3，AUTH_ERROR_FAIL_THRESHOLD）才永久 valid=false。
            self.pool.mark_invalid(email, reason, err).await;
        }
        // timeout / 其他：僅 exclude，不改帳號狀態
    }
}

/// 上游錯誤是否屬於「換帳號就可解」類型（quota 配額、rate-limit、暫時性 internal_error / 503）。
/// 用於 SSE 中途已 yield 過 answer-phase content delta 後仍應跨帳號重試的判定 ——
/// 這類錯誤跟「auth_error / 內容安全 / 客戶端格式錯」不一樣，跟使用者的 prompt 無關，
/// 純粹是被分配到的那個上游帳號用滿了配額／被限流，下一個帳號通常會成功。
fn is_account_swap_retryable(err: &str) -> bool {
    let lower = err.to_lowercase();
    // quota / 額度：阿里云 model studio 的 "Allocated quota exceeded" 是典型；
    // 也含 "quota exceeded"、"upper limit for today" 等變體
    if lower.contains("quota exceeded")
        || lower.contains("allocated quota")
        || lower.contains("upper limit")
        || lower.contains("today's usage")
        || lower.contains("token-limit")
    {
        return true;
    }
    // rate limit / 429：明確指示「換一個帳號」
    if lower.contains("429")
        || lower.contains("rate limit")
        || lower.contains("ratelimited")
        || lower.contains("too many")
    {
        return true;
    }
    // 上游短暫不可用 / 內部錯誤（保守處理：再給一次機會、最多受 attempts 上限）
    if lower.contains("internal_error")
        || lower.contains("internal error")
        || lower.contains("service unavailable")
        || lower.contains("503")
        || lower.contains("502")
        || lower.contains("504")
    {
        return true;
    }
    false
}

/// 在 byte slice 中尋找子序列位置。
fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    haystack
        .windows(needle.len())
        .position(|w| w == needle)
}

#[cfg(test)]
mod tests {
    use super::is_account_swap_retryable;

    /// 阿里云 model studio 的真實 quota 錯誤訊息（命中 `allocated quota`／`quota exceeded`／`token-limit`）
    #[test]
    fn aliyun_quota_exceeded_is_swap_retryable() {
        let real = "Qwen upstream error code=internal_error request_id=a79b... details=Allocated quota exceeded, please increase your quota limit. For details, see: https://help.aliyun.com/zh/model-studio/error-code#token-limit";
        assert!(is_account_swap_retryable(real), "Allocated quota exceeded 應走跨帳號重試");
    }

    /// 429 / rate limit 系列
    #[test]
    fn rate_limit_variants_are_swap_retryable() {
        for s in ["HTTP 429 Too Many Requests", "rate limit exceeded", "RateLimited", "too many requests"] {
            assert!(is_account_swap_retryable(s), "應為 swap-retryable: {s}");
        }
    }

    /// 上游維護性暫時錯誤
    #[test]
    fn transient_upstream_errors_are_swap_retryable() {
        for s in [
            "code=internal_error something happened",
            "service unavailable, try again",
            "HTTP 503 Service Unavailable",
            "502 Bad Gateway",
            "504 Gateway Timeout",
        ] {
            assert!(is_account_swap_retryable(s), "應為 swap-retryable: {s}");
        }
    }

    /// 內容安全 / 認證 / 客戶端問題 → 換帳號沒用，不該重試
    #[test]
    fn permanent_errors_are_not_swap_retryable() {
        for s in [
            "code=data_inspection_failed 内容安全警告",
            "401 Unauthorized invalid token",
            "code=auth_error account banned",
            "JSON 解析錯誤",
            "expected string at line 1 column 2",
        ] {
            assert!(!is_account_swap_retryable(s), "不該 swap-retry: {s}");
        }
    }
}
