//! 執行編排：驅動上游執行器串流，正規化成 OutEvent，處理思考增量、工具解析、usage。
//! 對應 Python `runtime/execution.py` + `services/completion_bridge.py`（取核心快樂路徑）。

pub mod formatters;
pub mod presenter;
pub mod translator;

use crate::request::StandardRequest;
use crate::state::AppState;
use crate::stats::{RequestRecord, Stats};
use crate::toolcall::{parse_tool_calls, strip_tool_calls_with, ParsedToolCall};
use crate::upstream::{ImageOptions, StreamParams, UpstreamEvent};
use crate::util::{char_len, now_millis};
use async_stream::stream;
use futures_util::{Stream, StreamExt};
use once_cell::sync::Lazy;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tiktoken_rs::CoreBPE;

static BPE: Lazy<CoreBPE> = Lazy::new(|| tiktoken_rs::cl100k_base().expect("cl100k_base"));

pub fn count_tokens(text: &str) -> usize {
    BPE.encode_ordinary(text).len()
}

#[derive(Debug, Clone, Default)]
pub struct Usage {
    pub prompt_tokens: i64,
    pub completion_tokens: i64,
    pub total_tokens: i64,
    pub reasoning_tokens: i64,
}

/// 正規化輸出事件。
#[derive(Debug, Clone)]
pub enum OutEvent {
    ReasoningDelta(String),
    ContentDelta(String),
    ToolCalls(Vec<ParsedToolCall>),
    Done { usage: Usage, finish_reason: String, email: Option<String> },
    Error(String),
}

/// 計算 reasoning 增量（處理 qwen3.7 累積陣列）。
struct ReasoningTracker {
    emitted: String,
}
impl ReasoningTracker {
    fn new() -> Self {
        ReasoningTracker { emitted: String::new() }
    }
    fn delta(&mut self, cumulative: &Option<String>, incremental: &str) -> String {
        if let Some(full) = cumulative {
            let inc = if full.starts_with(&self.emitted) {
                full[self.emitted.len()..].to_string()
            } else {
                full.clone()
            };
            self.emitted = full.clone();
            inc
        } else if !incremental.is_empty() {
            self.emitted.push_str(incremental);
            incremental.to_string()
        } else {
            String::new()
        }
    }
}

fn build_stream_params(std: &StandardRequest, image_options: Option<ImageOptions>) -> StreamParams {
    // 有附件綁定帳號時不能用預熱池（須用上傳檔案的同一帳號）
    let bound = std.bound_account.clone();
    let use_prewarmed = std.chat_type == "t2t" && bound.is_none();
    // 影像/影片：executor 內層只試 1 個帳號，重試交由應用層（generate_media_with_retry）精準控制輪換次數。
    let is_media = std.chat_type == "t2i" || std.chat_type == "t2v";
    StreamParams {
        model: std.resolved_model.clone(),
        content: std.prompt.clone(),
        has_custom_tools: std.has_tools(),
        files: std.files.clone(),
        chat_type: std.chat_type.clone(),
        image_options,
        thinking_enabled: std.thinking_enabled,
        enable_search: std.enable_search,
        fixed_account: bound,
        existing_chat_id: None,
        delete_on_close: true,
        use_prewarmed,
        max_retries: if is_media { Some(1) } else { None },
        exclude: std.exclude_accounts.clone(),
    }
}

/// 從上游 usage Value 萃取數值。
fn parse_upstream_usage(v: &Value) -> (i64, i64) {
    let output = v.get("output_tokens").and_then(|x| x.as_i64()).unwrap_or(0);
    let reasoning = v
        .get("output_tokens_details")
        .and_then(|d| d.get("reasoning_tokens"))
        .and_then(|x| x.as_i64())
        .unwrap_or(0);
    (output, reasoning)
}

/// 統計埋點所需的請求中介資料（由 StandardRequest 萃取）。
struct ProbeMeta {
    surface: String,
    model: String,
    resolved_model: String,
    chat_type: String,
    stream: bool,
    caller: Option<String>,
}

/// 取消安全的統計探針：在串流結束（Done/Error/客戶端斷線）時於 Drop 一次性記錄。
/// 對齊 executor 的 StreamGuard 模式，確保斷線也會留下記錄（記為未完成）。
struct StatsProbe {
    stats: Arc<Stats>,
    meta: ProbeMeta,
    start: Instant,
    start_ms: i64,
    ttft_ms: Option<i64>,
    usage: Usage,
    success: bool,
    error: Option<String>,
}

impl StatsProbe {
    fn new(stats: Arc<Stats>, meta: ProbeMeta) -> Self {
        StatsProbe {
            stats,
            meta,
            start: Instant::now(),
            start_ms: now_millis(),
            ttft_ms: None,
            usage: Usage::default(),
            success: false,
            error: None,
        }
    }
    /// 標記首字到達（僅記錄第一次）。
    fn mark_first_token(&mut self) {
        if self.ttft_ms.is_none() {
            self.ttft_ms = Some(self.start.elapsed().as_millis() as i64);
        }
    }
}

impl Drop for StatsProbe {
    fn drop(&mut self) {
        let duration_ms = self.start.elapsed().as_millis() as i64;
        self.stats.record(RequestRecord {
            ts_ms: self.start_ms,
            surface: std::mem::take(&mut self.meta.surface),
            model: std::mem::take(&mut self.meta.model),
            resolved_model: std::mem::take(&mut self.meta.resolved_model),
            chat_type: std::mem::take(&mut self.meta.chat_type),
            stream: self.meta.stream,
            success: self.success,
            error: self.error.take(),
            prompt_tokens: self.usage.prompt_tokens,
            completion_tokens: self.usage.completion_tokens,
            reasoning_tokens: self.usage.reasoning_tokens,
            total_tokens: self.usage.total_tokens,
            ttft_ms: self.ttft_ms,
            duration_ms,
            caller: self.meta.caller.take(),
        });
    }
}

/// 截斷呼叫者識別，避免在統計庫存放完整密鑰（僅留前綴供分組）。
fn truncate_caller(c: &str) -> String {
    let n = 12;
    if c.chars().count() <= n {
        c.to_string()
    } else {
        format!("{}…", c.chars().take(n).collect::<String>())
    }
}

/// 主入口：回傳 OutEvent 串流。stream 與 non-stream 處理皆消費此串流。
pub fn run_completion(
    state: AppState,
    std: StandardRequest,
    registry: HashMap<String, String>,
) -> impl Stream<Item = OutEvent> {
    let has_tools = std.has_tools();
    let prompt = std.prompt.clone();
    let image_options = std.image_options.clone();
    let params = build_stream_params(&std, image_options);
    let executor = state.executor.clone();
    let stats = state.stats.clone();
    let probe_meta = ProbeMeta {
        surface: std.surface.clone(),
        model: std.response_model.clone(),
        resolved_model: std.resolved_model.clone(),
        chat_type: std.chat_type.clone(),
        stream: std.stream,
        caller: std.caller.as_deref().map(truncate_caller),
    };

    stream! {
        // 取消安全統計探針：Drop 時記錄一筆（成功/失敗/斷線皆涵蓋）。
        let mut probe = StatsProbe::new(stats, probe_meta);
        let mut upstream = Box::pin(executor.clone().run_stream(params));
        let mut tracker = ReasoningTracker::new();
        let mut answer_buf = String::new();
        let mut streamed_content = false; // 無工具時逐步串流
        let mut last_out_tokens = 0i64;
        let mut last_reasoning_tokens = 0i64;
        let mut errored = false;
        // 最後一次使用的帳號（executor 內層重試會多次發 Meta，取最後一個）
        let mut last_email: Option<String> = None;
        // 上游每個 phase 出現次數（診斷用：偶發「思考完啥都沒看到」時 dump 出來）
        let mut phase_counts: std::collections::BTreeMap<String, usize> =
            std::collections::BTreeMap::new();
        // 跳過的 content 累積（phase 屬於 think / thinking_summary 時）—
        // 若最終空回覆，這份能告訴我們上游把答案送到了哪個 phase。
        let mut skipped_phase_content_chars: usize = 0;

        while let Some(ev) = upstream.next().await {
            match ev {
                UpstreamEvent::Meta { email, .. } => { last_email = Some(email); }
                UpstreamEvent::Delta(d) => {
                    *phase_counts.entry(d.phase.clone()).or_insert(0) += 1;
                    // usage
                    if let Some(u) = &d.usage {
                        let (o, r) = parse_upstream_usage(u);
                        if o > 0 { last_out_tokens = o; }
                        if r > 0 { last_reasoning_tokens = r; }
                    }
                    // reasoning（增量）
                    let rinc = tracker.delta(&d.reasoning_cumulative, &d.reasoning_incremental);
                    if !rinc.is_empty() {
                        probe.mark_first_token();
                        yield OutEvent::ReasoningDelta(rinc);
                    }
                    // content：收集所有非思考階段（answer / image_gen / t2v 影片 URL 等）
                    if !d.content.is_empty() {
                        if d.phase != "think" && d.phase != "thinking_summary" {
                            probe.mark_first_token();
                            answer_buf.push_str(&d.content);
                            if !has_tools {
                                yield OutEvent::ContentDelta(d.content.clone());
                                streamed_content = true;
                            }
                        } else {
                            skipped_phase_content_chars += d.content.chars().count();
                        }
                    }
                }
                UpstreamEvent::Done => break,
                UpstreamEvent::Error(e) => {
                    probe.error = Some(e.clone());
                    probe.success = false;
                    yield OutEvent::Error(e);
                    errored = true;
                    break;
                }
                UpstreamEvent::Retrying => {
                    // executor 即將跨帳號重試本輪請求 → 清掉本輪累積，
                    // 避免上一輪殘片污染下一輪 tool_call 解析 / 重複統計。
                    // 已 yield 給 client 的部分文字救不回，但 has_tools=true 緩衝路徑
                    // （Claude Code 主場景）content 沒 yield 出去，重試對 client 等於透明。
                    tracing::warn!(
                        "[執行編排] 收到 Retrying，清空本輪 answer_buf({}) / reasoning state",
                        answer_buf.chars().count()
                    );
                    answer_buf.clear();
                    streamed_content = false;
                    tracker = ReasoningTracker::new();
                    last_out_tokens = 0;
                    last_reasoning_tokens = 0;
                    phase_counts.clear();
                    skipped_phase_content_chars = 0;
                }
            }
        }

        if errored {
            return;
        }

        // 最終化：工具解析。只要有工具就嘗試解析（parse 內的 regex 對非工具文字會 no-op），
        // 避免漏接 ```json 圍欄形式（looks_like_tool_call 不覆蓋該形式）。
        let mut tool_calls: Vec<ParsedToolCall> = Vec::new();
        if has_tools && !answer_buf.is_empty() {
            tool_calls = parse_tool_calls(&answer_buf, &registry);
        }

        // 有工具時，緩衝的可見文字在此一次性送出（剝除工具標記＋裸 JSON tool_call）
        let mut yielded_content_at_end = false;
        if has_tools && !streamed_content {
            let cleaned = strip_tool_calls_with(&answer_buf, &registry);
            let cleaned = cleaned.trim();
            if !cleaned.is_empty() {
                yield OutEvent::ContentDelta(cleaned.to_string());
                yielded_content_at_end = true;
            }
        }
        if !tool_calls.is_empty() {
            yield OutEvent::ToolCalls(tool_calls.clone());
        }

        // 保險網（fail-open）：has_tools 終局時，若 buffer 非空卻被 strip 清光、又沒解析出 tool_calls
        // → 把原 buffer 當作 content yield 出去，至少 client 看得到上游回了什麼東西，
        // 而不是收到「思考完突然 stop、零內容」的詭異體驗。
        // 此分支理論上不應觸發（parser 已用 brace-balanced 處理多 JSON / 巢狀 / 未閉合），
        // 留著是為了未來上游再出新格式時仍有最後一道防線。同時搭配下方診斷 log 留證。
        if has_tools && !streamed_content && !yielded_content_at_end && tool_calls.is_empty()
            && !answer_buf.trim().is_empty()
        {
            yield OutEvent::ContentDelta(answer_buf.clone());
            yielded_content_at_end = true;
        }

        // 診斷：思考模型偶發「思考完客戶端啥都沒看到」。發生條件是：
        // (a) 整輪結束無錯誤；(b) 沒串流出 content；(c) 終局沒一次性送 content；
        // (d) 沒任何 tool_calls。warn log 把 phase 統計 + answer_buf 全文 + 解析後 cleaned 印出來，
        // 用來判斷上游把答案送到了哪個 phase / parser 為什麼漏抓 tool_call。
        let client_saw_nothing = !streamed_content && !yielded_content_at_end && tool_calls.is_empty();
        if client_saw_nothing && has_tools {
            let cleaned_full = crate::toolcall::strip_tool_calls_with(&answer_buf, &registry);
            let cleaned_trim = cleaned_full.trim();
            tracing::warn!(
                "[執行編排] 客戶端零輸出 phases={phase_counts:?} skipped_phase_chars={skipped_phase_content_chars} answer_buf_chars={} cleaned_chars={} cleaned_is_empty={} last_email={:?} out_tokens={} reasoning_tokens={} answer_buf_full={:?}",
                answer_buf.chars().count(),
                cleaned_full.chars().count(),
                cleaned_trim.is_empty(),
                last_email,
                last_out_tokens,
                last_reasoning_tokens,
                answer_buf,
            );
        }

        // usage：completion 用上游 output_tokens（最準），prompt 用本地 tiktoken
        let visible = if has_tools { strip_tool_calls_with(&answer_buf, &registry) } else { answer_buf.clone() };
        let prompt_tokens = count_tokens(&prompt) as i64;
        let completion_tokens = if last_out_tokens > 0 { last_out_tokens } else { char_len(&visible) as i64 };
        let usage = Usage {
            prompt_tokens,
            completion_tokens,
            total_tokens: prompt_tokens + completion_tokens,
            reasoning_tokens: last_reasoning_tokens,
        };
        let finish_reason = if !tool_calls.is_empty() { "tool_calls" } else { "stop" };
        // 統計：成功完成，回填 usage（probe 於 Drop 時落盤）。
        probe.usage = usage.clone();
        probe.success = true;
        yield OutEvent::Done { usage, finish_reason: finish_reason.to_string(), email: last_email };
    }
}

/// 非串流：消費整個串流，回傳聚合結果。
pub struct CollectedResult {
    pub content: String,
    pub reasoning: String,
    pub tool_calls: Vec<ParsedToolCall>,
    pub usage: Usage,
    pub finish_reason: String,
    pub error: Option<String>,
    /// 實際使用的帳號 email（若 executor 取到了帳號）；供 t2v 智慧跳過邏輯標記無權限帳號。
    pub email: Option<String>,
}

pub async fn collect_completion(
    state: AppState,
    std: StandardRequest,
    registry: HashMap<String, String>,
) -> CollectedResult {
    let mut s = Box::pin(run_completion(state, std, registry));
    let mut content = String::new();
    let mut reasoning = String::new();
    let mut tool_calls = Vec::new();
    let mut usage = Usage::default();
    let mut finish_reason = "stop".to_string();
    let mut error = None;
    let mut email = None;
    while let Some(ev) = s.next().await {
        match ev {
            OutEvent::ReasoningDelta(r) => reasoning.push_str(&r),
            OutEvent::ContentDelta(c) => content.push_str(&c),
            OutEvent::ToolCalls(tc) => tool_calls = tc,
            OutEvent::Done { usage: u, finish_reason: fr, email: em } => {
                usage = u;
                finish_reason = fr;
                email = em;
            }
            OutEvent::Error(e) => error = Some(e),
        }
    }
    CollectedResult { content, reasoning, tool_calls, usage, finish_reason, error, email }
}

/// 共用：取得某請求的工具註冊表。
pub fn registry_for(std: &StandardRequest) -> HashMap<String, String> {
    let normalized = crate::toolcall::normalize_tools(&std.tools);
    crate::toolcall::build_registry(&normalized)
}

// 讓 Arc<Executor> 可被 clone 進 stream
type _AssertSend = Arc<crate::upstream::Executor>;
