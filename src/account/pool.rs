//! 帳號池：4 層並發控制、acquire/release、限流冷卻、最少負載選號、黏滯(affinity)。
//! 對應 Python `core/account_pool/{pool_core,pool_acquire}.py`。
//!
//! Pillar 2（dev/LATENCY.md）：就緒帳號索引 Ready-Set。
//! 舊版 acquire 每次對全部帳號做 O(n) filter + O(n log n) sort 且全程持鎖；帳號上萬時
//! 在 acquire_wait 的 500ms 輪詢迴圈中會嚴重序列化。Ready-Set 以「ready 隊列 + cooldown 最小堆 +
//! loc 成員表 + email→index」維護，acquire 降為攤銷 ~O(1)。可用 POOL_READY_INDEX=0 回退舊掃描。

use super::account::Account;
use crate::config::Settings;
use crate::db::write_json_atomic;
use crate::util::{jitter_ms, now_secs};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};

/// 排隊計數的 RAII guard：drop 時遞減（取消安全，client 斷線丟棄 future 也會還原）。
struct WaitGuard<'a>(&'a AtomicUsize);
impl Drop for WaitGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

/// 取得帳號後交給呼叫端的輕量句柄。
#[derive(Debug, Clone)]
pub struct AccountHandle {
    pub email: String,
    pub token: String,
}

/// f64 的全序包裝，供 BinaryHeap 當 key（NaN 不會出現於時間戳）。
#[derive(Clone, Copy, PartialEq)]
struct OrdF64(f64);
impl Eq for OrdF64 {}
impl PartialOrd for OrdF64 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for OrdF64 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

/// 帳號在索引中的位置。loc 為「真相來源」；ready/cooldown 容許過期殘留，取用時以 loc 驗證。
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Loc {
    /// 可立即取用（valid && !rate_limited && next_available_at<=now && inflight<cap）。
    Ready,
    /// 等待中（風控最小間隔或限流冷卻），key 為 next_available_at 快照（僅排序提示，取用時即時重算）。
    Cooldown,
    /// 不在任何隊列（已失效，或 inflight 已達上限）。經 release/mark_* 事件重新入列。
    Idle,
}

/// 就緒帳號索引（Pillar 2）。email-keyed，免受 accounts Vec 索引位移影響（add/remove 時整建）。
struct ReadyIndex {
    /// email → accounts Vec 索引（O(1) 取帳號；僅在整建時更新）。
    pos: HashMap<String, usize>,
    /// 就緒最小堆，依 (inflight, last_used) 排序（least-loaded + LRU，對齊舊 acquire_scan 主鍵與 LATENCY.md 規格）。
    /// lazy 刪除：取出時以 loc 驗證是否仍為 Ready；舊鍵殘留條目於 pop 時丟棄（loc 不符）或照常重用（仍 Ready）。
    ready: BinaryHeap<Reverse<(i64, OrdF64, String)>>,
    /// 冷卻最小堆，key=next_available_at 快照（lazy 刪除 + 取出時即時重算）。
    cooldown: BinaryHeap<Reverse<(OrdF64, String)>>,
    /// 成員表（真相來源）。
    loc: HashMap<String, Loc>,
}

impl ReadyIndex {
    fn new() -> Self {
        ReadyIndex {
            pos: HashMap::new(),
            ready: BinaryHeap::new(),
            cooldown: BinaryHeap::new(),
            loc: HashMap::new(),
        }
    }

    /// 依即時狀態把 email 放到對的位置。O(log n)（push；舊殘留 lazy 丟棄）。
    /// 呼叫前帳號欄位必須已是最新值（與欄位寫入同臨界區，避免 TOCTOU）。
    fn place(&mut self, email: &str, valid: bool, available: bool, inflight: i64, cap: i64, next_avail: f64, last_used: f64) {
        let l = if !valid || inflight >= cap {
            Loc::Idle
        } else if available {
            // 最小堆鍵：inflight 升序（least-loaded）、再 last_used 升序（LRU）。
            self.ready.push(Reverse((inflight, OrdF64(last_used), email.to_string())));
            Loc::Ready
        } else {
            self.cooldown.push(Reverse((OrdF64(next_avail), email.to_string())));
            Loc::Cooldown
        };
        self.loc.insert(email.to_string(), l);
    }
}

/// 整建索引（O(n)，僅 load / add / remove / set_max_inflight / min_interval 變更時觸發）。
fn rebuild_index(st: &mut PoolState, min_ms: u64) {
    let cap = st.max_inflight_per_account;
    let PoolState { accounts, idx, .. } = st;
    let Some(idx) = idx.as_mut() else { return };
    idx.pos.clear();
    idx.ready.clear();
    idx.cooldown.clear();
    idx.loc.clear();
    for (i, acc) in accounts.iter().enumerate() {
        idx.pos.insert(acc.email.clone(), i);
        let avail = acc.is_available(min_ms);
        let na = acc.next_available_at(min_ms);
        idx.place(&acc.email, acc.valid, avail, acc.inflight, cap, na, acc.last_used);
    }
}

/// 某帳號欄位變動後，於同臨界區內重新放置（index 關閉時為 no-op）。
fn reindex_email(st: &mut PoolState, email: &str, min_ms: u64) {
    if !st.use_index {
        return;
    }
    let cap = st.max_inflight_per_account;
    let PoolState { accounts, idx, .. } = st;
    let Some(idx) = idx.as_mut() else { return };
    let Some(&pos) = idx.pos.get(email) else { return };
    let acc = &accounts[pos];
    let avail = acc.is_available(min_ms);
    let na = acc.next_available_at(min_ms);
    idx.place(email, acc.valid, avail, acc.inflight, cap, na, acc.last_used);
}

/// 取帳號在 Vec 的位置：index 開啟用 pos 表 O(1)；否則線性掃描（舊行為）。
fn account_pos(st: &PoolState, email: &str) -> Option<usize> {
    match st.idx.as_ref() {
        Some(idx) => idx.pos.get(email).copied(),
        None => st.accounts.iter().position(|a| a.email == email),
    }
}

struct PoolState {
    accounts: Vec<Account>,
    max_inflight_per_account: i64,
    /// 全域同時在途上限；0 = 不限。
    global_max_inflight: i64,
    /// 佇列上限；0 = 不可排隊。
    max_queue_size: i64,
    global_in_use: i64,
    /// 管理員手動設定的全域上限；None = 自動（= 可用帳號數 × 每帳號上限）。
    admin_global: Option<i64>,
    sticky_email: Option<String>,
    /// 是否啟用 Ready-Set 索引（Pillar 2）。
    use_index: bool,
    /// 索引本體（use_index 時為 Some）。
    idx: Option<ReadyIndex>,
    /// 已建索引對應的 min_interval 世代；與 AccountPool.interval_gen 不符時惰性整建。
    built_gen: u64,
}

pub struct AccountPool {
    state: Mutex<PoolState>,
    /// 目前排隊等待的請求數（原子計數，配合 WaitGuard 取消安全）。
    waiting: AtomicUsize,
    notify: Notify,
    accounts_file: PathBuf,
    /// 風控：同帳號最小間隔（毫秒），可在管理台即時調整。
    min_interval_ms: AtomicU64,
    /// min_interval 變更世代（set_min_interval 為 sync/lock-free，靠此觸發 acquire 時惰性整建索引）。
    interval_gen: AtomicU64,
    jitter_min_ms: u64,
    jitter_max_ms: u64,
    base_cooldown: u64,
    max_cooldown: u64,
    /// auth_error 連續失敗門檻：到達此值才永久標 valid=false（避免單次 401/CF 攔截誤殺）。
    auth_error_threshold: u32,
}

impl AccountPool {
    pub async fn load(settings: &Settings) -> Arc<Self> {
        let mut accounts: Vec<Account> =
            crate::db::read_json_or(&settings.accounts_file, Vec::new()).await;
        for a in accounts.iter_mut() {
            a.init_runtime();
        }
        let use_index = settings.pool_ready_index;
        let pool = AccountPool {
            state: Mutex::new(PoolState {
                accounts,
                max_inflight_per_account: settings.max_inflight_per_account,
                global_max_inflight: 0,
                max_queue_size: 0,
                global_in_use: 0,
                admin_global: None,
                sticky_email: None,
                use_index,
                idx: if use_index { Some(ReadyIndex::new()) } else { None },
                built_gen: 0,
            }),
            waiting: AtomicUsize::new(0),
            notify: Notify::new(),
            accounts_file: settings.accounts_file.clone(),
            min_interval_ms: AtomicU64::new(settings.account_min_interval_ms),
            interval_gen: AtomicU64::new(0),
            jitter_min_ms: settings.request_jitter_min_ms,
            jitter_max_ms: settings.request_jitter_max_ms,
            base_cooldown: settings.rate_limit_base_cooldown,
            max_cooldown: settings.rate_limit_max_cooldown,
            auth_error_threshold: settings.auth_error_fail_threshold,
        };
        {
            let mut st = pool.state.lock().await;
            Self::reset_concurrency_limits(&mut st);
            if st.use_index {
                rebuild_index(&mut st, settings.account_min_interval_ms);
            }
        }
        let arc = Arc::new(pool);
        tracing::info!(
            "帳號池已載入 {} 個帳號（Ready-Set 索引: {}）",
            arc.count().await,
            if use_index { "開" } else { "關" }
        );
        arc
    }

    pub async fn count(&self) -> usize {
        self.state.lock().await.accounts.len()
    }

    /// recommended = available_count * max_inflight_per_account；同步設給 queue/global。
    fn reset_concurrency_limits(st: &mut PoolState) {
        let now = now_secs();
        let available = st
            .accounts
            .iter()
            .filter(|a| a.valid && a.rate_limited_until <= now)
            .count() as i64;
        let recommended = available * st.max_inflight_per_account.max(1);
        // 有管理員覆蓋則用之，否則自動 = recommended；queue 與 global 同步。
        let effective = st.admin_global.unwrap_or(recommended);
        st.global_max_inflight = effective;
        st.max_queue_size = effective;
    }

    pub async fn set_max_inflight(&self, v: i64) {
        let min_ms = self.min_interval_ms.load(Ordering::Relaxed);
        let mut st = self.state.lock().await;
        st.max_inflight_per_account = v.max(1);
        Self::reset_concurrency_limits(&mut st);
        // cap 改變影響 inflight>=cap 的就緒判定 → 整建索引。
        if st.use_index {
            rebuild_index(&mut st, min_ms);
        }
    }

    pub async fn set_global_max_inflight(&self, v: i64) {
        let mut st = self.state.lock().await;
        st.admin_global = if v > 0 { Some(v) } else { None };
        Self::reset_concurrency_limits(&mut st);
        // 僅改全域閘門，不影響任何帳號的就緒狀態 → 不需整建索引。
    }

    /// 風控：設定同帳號最小間隔（毫秒），即時生效。bump 世代以觸發 acquire 時整建索引。
    pub fn set_min_interval(&self, ms: u64) {
        self.min_interval_ms.store(ms, Ordering::Relaxed);
        self.interval_gen.fetch_add(1, Ordering::Relaxed);
    }

    pub fn min_interval_ms(&self) -> u64 {
        self.min_interval_ms.load(Ordering::Relaxed)
    }

    async fn persist(&self, accounts: &[Account]) {
        write_json_atomic(&self.accounts_file, &accounts.to_vec()).await;
    }

    pub async fn save(&self) {
        let snapshot = { self.state.lock().await.accounts.clone() };
        self.persist(&snapshot).await;
    }

    /// 新增 / 覆蓋帳號（依 email 去重），觸發保存。
    pub async fn add(&self, mut acc: Account) {
        acc.init_runtime();
        let min_ms = self.min_interval_ms.load(Ordering::Relaxed);
        let snapshot = {
            let mut st = self.state.lock().await;
            if let Some(existing) = st.accounts.iter_mut().find(|a| a.email == acc.email) {
                *existing = acc;
            } else {
                st.accounts.push(acc);
            }
            Self::reset_concurrency_limits(&mut st);
            // accounts Vec 結構改變 → pos 失效 → 整建索引。
            if st.use_index {
                rebuild_index(&mut st, min_ms);
            }
            st.accounts.clone()
        };
        self.persist(&snapshot).await;
        self.notify.notify_waiters();
    }

    pub async fn remove(&self, email: &str) {
        let min_ms = self.min_interval_ms.load(Ordering::Relaxed);
        let snapshot = {
            let mut st = self.state.lock().await;
            st.accounts.retain(|a| a.email != email);
            Self::reset_concurrency_limits(&mut st);
            // retain 會位移索引 → 整建。
            if st.use_index {
                rebuild_index(&mut st, min_ms);
            }
            st.accounts.clone()
        };
        self.persist(&snapshot).await;
    }

    /// 列出所有帳號（管理台用，clone 快照）。
    pub async fn list(&self) -> Vec<Account> {
        self.state.lock().await.accounts.clone()
    }

    /// 取得某帳號的 token（依 email）。
    pub async fn token_of(&self, email: &str) -> Option<String> {
        let st = self.state.lock().await;
        account_pos(&st, email).map(|i| st.accounts[i].token.clone())
    }

    /// 取得某帳號的 (token, password)（依 email）——refresh 流程用。
    /// password 是註冊時的明文（accounts.json 直接存）；signin 自己 sha256 後送上游。
    pub async fn token_and_password_of(&self, email: &str) -> Option<(String, String)> {
        let st = self.state.lock().await;
        account_pos(&st, email).map(|i| {
            let a = &st.accounts[i];
            (a.token.clone(), a.password.clone())
        })
    }

    /// 取得任一可用帳號 (email, token)（用於檔案上傳並綁定後續對話）。
    pub async fn any_valid_account(&self) -> Option<(String, String)> {
        let now = now_secs();
        self.state
            .lock()
            .await
            .accounts
            .iter()
            .find(|a| a.valid && a.rate_limited_until <= now && !a.token.is_empty())
            .map(|a| (a.email.clone(), a.token.clone()))
    }

    /// 取得任一可用帳號的 token（用於抓模型列表等）。
    pub async fn any_valid_token(&self) -> Option<String> {
        let now = now_secs();
        self.state
            .lock()
            .await
            .accounts
            .iter()
            .find(|a| a.valid && a.rate_limited_until <= now && !a.token.is_empty())
            .map(|a| a.token.clone())
    }

    /// 非阻塞取得帳號。preferred 命中時優先；exclude 內的 email 跳過。
    pub async fn acquire(&self, preferred: Option<&str>, exclude: &HashSet<String>) -> Option<AccountHandle> {
        let mut st = self.state.lock().await;
        self.acquire_locked(&mut st, preferred, exclude)
    }

    /// 內部：依設定派發到索引版或掃描版；並在 min_interval 世代變更時惰性整建索引。
    fn acquire_locked(
        &self,
        st: &mut PoolState,
        preferred: Option<&str>,
        exclude: &HashSet<String>,
    ) -> Option<AccountHandle> {
        let min_ms = self.min_interval_ms.load(Ordering::Relaxed);
        if st.use_index {
            let gen = self.interval_gen.load(Ordering::Relaxed);
            if st.built_gen != gen {
                rebuild_index(st, min_ms);
                st.built_gen = gen;
            }
            self.acquire_indexed(st, preferred, exclude, min_ms)
        } else {
            self.acquire_scan(st, preferred, exclude, min_ms)
        }
    }

    /// 舊版 O(n) 掃描選號（POOL_READY_INDEX=0 時使用；亦作為測試 oracle）。
    fn acquire_scan(
        &self,
        st: &mut PoolState,
        preferred: Option<&str>,
        exclude: &HashSet<String>,
        min_ms: u64,
    ) -> Option<AccountHandle> {
        if st.global_max_inflight > 0 && st.global_in_use >= st.global_max_inflight {
            return None;
        }
        let now = now_secs();
        let cap = st.max_inflight_per_account;

        let mut candidates: Vec<usize> = st
            .accounts
            .iter()
            .enumerate()
            .filter(|(_, a)| a.is_available(min_ms) && a.inflight < cap && !exclude.contains(&a.email))
            .map(|(i, _)| i)
            .collect();
        if candidates.is_empty() {
            return None;
        }
        candidates.sort_by(|&a, &b| {
            let aa = &st.accounts[a];
            let bb = &st.accounts[b];
            aa.inflight
                .cmp(&bb.inflight)
                .then(aa.last_request_started.partial_cmp(&bb.last_request_started).unwrap_or(std::cmp::Ordering::Equal))
                .then(aa.last_used.partial_cmp(&bb.last_used).unwrap_or(std::cmp::Ordering::Equal))
        });

        let chosen = preferred
            .and_then(|pe| candidates.iter().copied().find(|&i| st.accounts[i].email == pe))
            .unwrap_or(candidates[0]);

        let only_one = candidates.len() == 1;
        let jitter = jitter_ms(self.jitter_min_ms, self.jitter_max_ms) as f64 / 1000.0;
        let acc = &mut st.accounts[chosen];
        acc.inflight += 1;
        acc.last_used = now;
        acc.last_request_started = now + jitter;
        let handle = AccountHandle { email: acc.email.clone(), token: acc.token.clone() };
        st.global_in_use += 1;
        if only_one {
            st.sticky_email = Some(handle.email.clone());
        }
        Some(handle)
    }

    /// Ready-Set 選號（攤銷 ~O(1)）。
    fn acquire_indexed(
        &self,
        st: &mut PoolState,
        preferred: Option<&str>,
        exclude: &HashSet<String>,
        min_ms: u64,
    ) -> Option<AccountHandle> {
        if st.global_max_inflight > 0 && st.global_in_use >= st.global_max_inflight {
            return None;
        }
        let cap = st.max_inflight_per_account;
        let now = now_secs();
        let jitter = jitter_ms(self.jitter_min_ms, self.jitter_max_ms) as f64 / 1000.0;

        let PoolState { accounts, idx, global_in_use, .. } = st;
        let idx = idx.as_mut().expect("use_index 時 idx 必為 Some");

        // 1) 將到期的 cooldown 帳號移回（lazy：以 loc 為準，並依即時狀態重新放置）。
        while let Some(Reverse((key, _))) = idx.cooldown.peek() {
            if key.0 > now {
                break;
            }
            let Reverse((_, email)) = idx.cooldown.pop().unwrap();
            if idx.loc.get(&email) != Some(&Loc::Cooldown) {
                continue; // 過期殘留
            }
            let Some(&pos) = idx.pos.get(&email) else {
                idx.loc.remove(&email);
                continue;
            };
            let acc = &accounts[pos];
            let avail = acc.is_available(min_ms);
            let na = acc.next_available_at(min_ms);
            idx.place(&email, acc.valid, avail, acc.inflight, cap, na, acc.last_used);
        }

        // 2) 選號：preferred 優先；否則取就緒堆頂（(inflight,last_used) 最小者）。
        //    跳過 exclude（per-request）的條目暫存後原樣放回堆（保留鍵、不飢餓）。
        let mut chosen: Option<String> = None;
        if let Some(pe) = preferred {
            if idx.loc.get(pe) == Some(&Loc::Ready) && !exclude.contains(pe) {
                chosen = Some(pe.to_string());
            }
        }
        if chosen.is_none() {
            let mut held: Vec<Reverse<(i64, OrdF64, String)>> = Vec::new();
            while let Some(Reverse((inflight, lu, email))) = idx.ready.pop() {
                if idx.loc.get(&email) != Some(&Loc::Ready) {
                    continue; // 過期殘留（loc 不符）→ 丟棄
                }
                if exclude.contains(&email) {
                    held.push(Reverse((inflight, lu, email)));
                    continue;
                }
                chosen = Some(email);
                break;
            }
            for h in held {
                idx.ready.push(h);
            }
        }
        let email = chosen?;

        // 3) 佔用（欄位寫入 + 索引重置同臨界區，無 TOCTOU）。
        let &pos = idx.pos.get(&email).expect("chosen email 必有 pos");
        let token = {
            let acc = &mut accounts[pos];
            acc.inflight += 1;
            acc.last_used = now;
            acc.last_request_started = now + jitter;
            acc.token.clone()
        };
        *global_in_use += 1;
        // interval>0 時 next_available_at>now → 落入 cooldown；interval==0 且 inflight<cap → 仍 ready。
        let acc = &accounts[pos];
        let avail = acc.is_available(min_ms);
        let na = acc.next_available_at(min_ms);
        idx.place(&email, acc.valid, avail, acc.inflight, cap, na, acc.last_used);
        // 註：sticky_email 為 preferred 親和用，目前熱路徑 preferred 恆為 None（未啟用），索引版不維護之。

        Some(AccountHandle { email, token })
    }

    /// 阻塞取得帳號（有截止時間）。
    pub async fn acquire_wait(
        &self,
        preferred: Option<&str>,
        exclude: &HashSet<String>,
        timeout_secs: f64,
    ) -> Option<AccountHandle> {
        // 先試即時取得
        {
            let mut st = self.state.lock().await;
            if let Some(h) = self.acquire_locked(&mut st, preferred, exclude) {
                return Some(h);
            }
            // 佇列關閉(<=0=不可排隊) 或已滿 → 不等待，快速失敗
            let max_q = st.max_queue_size;
            if max_q <= 0 || (self.waiting.load(Ordering::Relaxed) as i64) >= max_q {
                return None;
            }
        }
        // 進入排隊：原子計數 + RAII guard（取消安全，斷線丟棄 future 也會還原計數）
        self.waiting.fetch_add(1, Ordering::Relaxed);
        let _guard = WaitGuard(&self.waiting);

        let deadline = now_secs() + timeout_secs;
        loop {
            {
                let mut st = self.state.lock().await;
                if let Some(h) = self.acquire_locked(&mut st, preferred, exclude) {
                    return Some(h);
                }
            }
            let remaining = deadline - now_secs();
            if remaining <= 0.0 {
                return None;
            }
            let wait = remaining.min(0.5);
            let _ = tokio::time::timeout(
                std::time::Duration::from_secs_f64(wait),
                self.notify.notified(),
            )
            .await;
        }
    }

    /// 釋放帳號。
    pub async fn release(&self, email: &str) {
        {
            let mut st = self.state.lock().await;
            let min_ms = self.min_interval_ms.load(Ordering::Relaxed);
            if let Some(pos) = account_pos(&st, email) {
                {
                    let a = &mut st.accounts[pos];
                    a.inflight = (a.inflight - 1).max(0);
                    a.last_request_finished = now_secs();
                }
                reindex_email(&mut st, email, min_ms);
            }
            st.global_in_use = (st.global_in_use - 1).max(0);
        }
        self.notify.notify_one();
    }

    /// 標記限流（指數退避冷卻）。
    pub async fn mark_rate_limited(&self, email: &str) {
        let min_ms = self.min_interval_ms.load(Ordering::Relaxed);
        let snapshot = {
            let mut st = self.state.lock().await;
            let (base, maxc) = (self.base_cooldown, self.max_cooldown);
            let jit = jitter_ms(0, 5000) / 1000;
            if let Some(pos) = account_pos(&st, email) {
                let a = &mut st.accounts[pos];
                a.rate_limit_strikes += 1;
                let factor = 2f64.powi((a.rate_limit_strikes - 1).max(0) as i32);
                let cooldown = ((base as f64 * factor) as u64).min(maxc) + jit;
                a.rate_limited_until = now_secs() + cooldown as f64;
                a.status_code = "rate_limited".to_string();
            }
            if st.sticky_email.as_deref() == Some(email) {
                st.sticky_email = None;
            }
            reindex_email(&mut st, email, min_ms);
            st.accounts.clone()
        };
        self.persist(&snapshot).await;
    }

    /// 標記失效。
    ///
    /// 設計（修正 13 個 auth_error 帳號的根因）：
    /// - `err` 寫入 `last_error`（先前只設 status_code，導致管理台看不到具體原因）。
    /// - `reason == "auth_error"` 加連續失敗門檻：未達 threshold 時保留 `valid=true`，僅累積
    ///   `consecutive_failures`；達門檻才永久 `valid=false`。可由 `AUTH_ERROR_FAIL_THRESHOLD`
    ///   調整（預設 3）。避免上游瞬時 401 / CF 攔截把帳號一次打死。
    /// - 其他 reason（`pending_activation` / `banned` / 空字串等）維持立即失效語意。
    /// - `mark_success` / `apply_verify(valid=true)` 會重置 `consecutive_failures`，正常請求
    ///   不會累積；只有連續失敗才會達門檻。
    pub async fn mark_invalid(&self, email: &str, reason: &str, err: &str) {
        let min_ms = self.min_interval_ms.load(Ordering::Relaxed);
        let threshold = self.auth_error_threshold.max(1) as i64;
        let snapshot = {
            let mut st = self.state.lock().await;
            if let Some(pos) = account_pos(&st, email) {
                let a = &mut st.accounts[pos];
                a.status_code = if reason.is_empty() { "auth_error" } else { reason }.to_string();
                a.consecutive_failures += 1;
                if !err.is_empty() {
                    a.last_error = err.to_string();
                }
                let immediate_kill = reason != "auth_error";
                if immediate_kill || a.consecutive_failures >= threshold {
                    a.valid = false;
                }
                if reason == "pending_activation" {
                    a.activation_pending = true;
                }
            }
            if st.sticky_email.as_deref() == Some(email) {
                st.sticky_email = None;
            }
            reindex_email(&mut st, email, min_ms);
            st.accounts.clone()
        };
        self.persist(&snapshot).await;
    }

    /// 標記成功（清失敗計數、限流→可用）。
    pub async fn mark_success(&self, email: &str) {
        let min_ms = self.min_interval_ms.load(Ordering::Relaxed);
        let mut st = self.state.lock().await;
        if let Some(pos) = account_pos(&st, email) {
            {
                let a = &mut st.accounts[pos];
                a.consecutive_failures = 0;
                a.rate_limit_strikes = 0;
                if a.status_code == "rate_limited" {
                    a.rate_limited_until = 0.0;
                }
                if a.valid {
                    a.status_code = "valid".to_string();
                }
            }
            // 重新放置以即時 next_available_at 計算：min_interval 冷卻仍會讓它留在 cooldown（不違風控）。
            reindex_email(&mut st, email, min_ms);
        }
    }

    /// 儀表板狀態快照。
    pub async fn status(&self) -> serde_json::Value {
        let st = self.state.lock().await;
        let now = now_secs();
        let total = st.accounts.len();
        let mut valid = 0;
        let mut rate_limited = 0;
        let mut invalid = 0;
        let mut in_use = 0i64;
        for a in &st.accounts {
            if a.rate_limited_until > now {
                rate_limited += 1;
            } else if a.valid {
                valid += 1;
            } else {
                invalid += 1;
            }
            in_use += a.inflight;
        }
        serde_json::json!({
            "total": total,
            "valid": valid,
            "rate_limited": rate_limited,
            "invalid": invalid,
            "in_use": in_use,
            "global_in_use": st.global_in_use,
            "waiting": self.waiting.load(Ordering::Relaxed),
            "max_inflight_per_account": st.max_inflight_per_account,
            "max_queue_size": st.max_queue_size,
            "global_max_inflight": st.global_max_inflight,
        })
    }

    /// 各帳號明細（儀表板表格）。
    /// 帳號數可達數萬，且儀表板每 3 秒輪詢一次；只回「有負載或異常」的帳號
    /// （inflight>0 / 限流中 / 非 valid），正常閒置帳號不列，最多 500 筆，避免全量序列化拖垮前端。
    pub async fn per_account_status(&self) -> Vec<serde_json::Value> {
        let st = self.state.lock().await;
        let cap = st.max_inflight_per_account;
        let now = now_secs();
        st.accounts
            .iter()
            .filter(|a| a.inflight > 0 || a.rate_limited_until > now || !a.valid)
            .take(500)
            .map(|a| {
                serde_json::json!({
                    "email": a.email,
                    "status": a.get_status_code(),
                    "inflight": a.inflight,
                    "max_inflight": cap,
                    "consecutive_failures": a.consecutive_failures,
                    "rate_limit_strikes": a.rate_limit_strikes,
                    "last_request_finished": a.last_request_finished,
                })
            })
            .collect()
    }

    /// 用新 token 覆寫帳號並 reset 為健康狀態。
    /// 用於 refresh 流程：admin endpoint / 背景 worker 跑完 signin 後寫回新 JWT。
    /// 一律清 last_error、failures=0、status="valid"——既有 hot-path 的「3 次門檻」失敗計數不該跨 refresh 累積。
    pub async fn replace_token(&self, email: &str, new_token: String) -> bool {
        let min_ms = self.min_interval_ms.load(Ordering::Relaxed);
        let mut ok = false;
        let snapshot = {
            let mut st = self.state.lock().await;
            if let Some(pos) = account_pos(&st, email) {
                let a = &mut st.accounts[pos];
                a.token = new_token;
                a.valid = true;
                a.activation_pending = false;
                a.status_code = "valid".to_string();
                a.last_error = String::new();
                a.consecutive_failures = 0;
                a.rate_limit_strikes = 0;
                ok = true;
            }
            reindex_email(&mut st, email, min_ms);
            st.accounts.clone()
        };
        if ok {
            self.persist(&snapshot).await;
        }
        ok
    }

    /// 套用驗證結果（管理台單獨/全量驗證用）。
    pub async fn apply_verify(&self, email: &str, valid: bool, status_code: &str, error: &str) {
        let min_ms = self.min_interval_ms.load(Ordering::Relaxed);
        let snapshot = {
            let mut st = self.state.lock().await;
            if let Some(pos) = account_pos(&st, email) {
                let a = &mut st.accounts[pos];
                a.valid = valid;
                a.status_code = status_code.to_string();
                a.last_error = error.to_string();
                if valid {
                    a.activation_pending = false;
                    a.consecutive_failures = 0;
                    a.rate_limit_strikes = 0;
                } else {
                    a.consecutive_failures += 1;
                }
            }
            reindex_email(&mut st, email, min_ms);
            st.accounts.clone()
        };
        self.persist(&snapshot).await;
    }

    /// 目前並發設定 (max_inflight_per_account, global_max_inflight, max_queue_size)。
    pub async fn concurrency_config(&self) -> (i64, i64, i64) {
        let st = self.state.lock().await;
        (st.max_inflight_per_account, st.global_max_inflight, st.max_queue_size)
    }

    /// 所有可用帳號 (email, token)（給預熱池 bootstrap 用）。
    pub async fn all_emails_tokens(&self) -> Vec<(String, String)> {
        let now = now_secs();
        self.state
            .lock()
            .await
            .accounts
            .iter()
            .filter(|a| a.valid && a.rate_limited_until <= now && !a.token.is_empty())
            .map(|a| (a.email.clone(), a.token.clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk(email: &str, valid: bool, inflight: i64, rl_until: f64, started: f64, finished: f64) -> Account {
        let mut a = Account::new(email.into(), String::new(), format!("tok-{email}"), String::new(), String::new());
        a.valid = valid;
        a.inflight = inflight;
        a.rate_limited_until = rl_until;
        a.last_request_started = started;
        a.last_request_finished = finished;
        a
    }

    fn test_pool(accounts: Vec<Account>, use_index: bool, min_ms: u64, cap: i64) -> AccountPool {
        let pool = AccountPool {
            state: Mutex::new(PoolState {
                accounts,
                max_inflight_per_account: cap,
                global_max_inflight: 0,
                max_queue_size: 0,
                global_in_use: 0,
                admin_global: None,
                sticky_email: None,
                use_index,
                idx: if use_index { Some(ReadyIndex::new()) } else { None },
                built_gen: 0,
            }),
            waiting: AtomicUsize::new(0),
            notify: Notify::new(),
            accounts_file: std::env::temp_dir().join("qwen2api_test_accounts.json"),
            min_interval_ms: AtomicU64::new(min_ms),
            interval_gen: AtomicU64::new(0),
            jitter_min_ms: 0,
            jitter_max_ms: 0,
            base_cooldown: 600,
            max_cooldown: 3600,
            // 測試預設用 1，等同既有「一次失敗即標 invalid」行為，
            // 隨機 ops 測試（random_ops_keep_index_consistent）便維持原本不變式。
            auth_error_threshold: 1,
        };
        {
            let mut st = pool.state.try_lock().unwrap();
            AccountPool::reset_concurrency_limits(&mut st);
            if use_index {
                rebuild_index(&mut st, min_ms);
            }
        }
        pool
    }

    /// 同一批帳號狀態下，索引版與掃描版「能取出的帳號數」必須一致（min_interval=0，避免冷卻時間干擾）。
    /// 並驗證每次取出的帳號都合法（valid && inflight<cap），且絕不重複超過 cap。
    #[test]
    fn parity_and_validity_no_interval() {
        // 構造多樣狀態：valid/invalid、不同 inflight、限流中/已過期。
        let now = now_secs();
        let accounts = vec![
            mk("a", true, 0, 0.0, 0.0, 0.0),
            mk("b", true, 0, 0.0, 0.0, 0.0),
            mk("c", false, 0, 0.0, 0.0, 0.0),                 // 失效
            mk("d", true, 0, now + 9999.0, 0.0, 0.0),         // 限流中
            mk("e", true, 1, 0.0, 0.0, 0.0),                  // inflight=1 (cap=2 仍可)
            mk("f", true, 2, 0.0, 0.0, 0.0),                  // 已達 cap=2
            mk("g", true, 0, 0.0, 0.0, 0.0),
        ];
        let cap = 2;
        let exclude = HashSet::new();

        let drain = |use_index: bool| -> usize {
            let pool = test_pool(accounts.clone(), use_index, 0, cap);
            let mut st = pool.state.try_lock().unwrap();
            let mut n = 0;
            while let Some(h) = pool.acquire_locked(&mut st, None, &exclude) {
                // 取出者必須合法
                let pos = st.accounts.iter().position(|a| a.email == h.email).unwrap();
                let a = &st.accounts[pos];
                assert!(a.valid, "取出失效帳號 {}", h.email);
                assert!(a.inflight <= cap, "超過 cap: {} inflight={}", h.email, a.inflight);
                assert!(a.rate_limited_until <= now_secs(), "取出限流帳號 {}", h.email);
                n += 1;
                assert!(n < 1000, "疑似無限迴圈");
            }
            n
        };

        let n_idx = drain(true);
        let n_scan = drain(false);
        assert_eq!(n_idx, n_scan, "索引版與掃描版可取數不一致");
        // a,b,g 各可取 2 次(cap)，e 可取 1 次(已 inflight=1)，f 已滿 → 2+2+2+1 = 7
        assert_eq!(n_scan, 7, "預期可取 7 次");
    }

    /// min_interval>0 時：取一次後帳號進入 cooldown，立即再取應取不到該帳號（風控不變）。
    #[test]
    fn cooldown_blocks_immediate_reacquire() {
        let accounts = vec![mk("solo", true, 0, 0.0, 0.0, 0.0)];
        let pool = test_pool(accounts, true, 3000, 2);
        let mut st = pool.state.try_lock().unwrap();
        let ex = HashSet::new();
        let first = pool.acquire_locked(&mut st, None, &ex);
        assert!(first.is_some(), "首次應取得");
        let second = pool.acquire_locked(&mut st, None, &ex);
        assert!(second.is_none(), "min_interval 內不應再取得同一帳號");
    }

    /// exclude 全部命中時應落空而非無限迴圈，且未被排除者仍可取。
    #[test]
    fn exclude_falls_through() {
        let accounts = vec![
            mk("x", true, 0, 0.0, 0.0, 0.0),
            mk("y", true, 0, 0.0, 0.0, 0.0),
        ];
        let pool = test_pool(accounts, true, 0, 1);
        let mut st = pool.state.try_lock().unwrap();
        let mut ex = HashSet::new();
        ex.insert("x".to_string());
        // 排除 x，應取得 y
        let h = pool.acquire_locked(&mut st, None, &ex);
        assert_eq!(h.map(|h| h.email), Some("y".to_string()));
        // 再排除 y（x 也排除），應落空
        ex.insert("y".to_string());
        assert!(pool.acquire_locked(&mut st, None, &ex).is_none());
    }

    /// mark_invalid 行為（修正 13 個 auth_error 帳號的根因）：
    /// - err 必寫入 last_error（先前 hot-path 完全沒寫）。
    /// - reason=="auth_error" 需累積到門檻才 valid=false；其他 reason 立即失效。
    /// - mark_success 重置 consecutive_failures，使「真死」與「瞬時錯誤」能區分。
    #[tokio::test]
    async fn mark_invalid_writes_last_error_and_honors_threshold() {
        // 建一個門檻 3 的 pool：test_pool 預設 threshold=1，直接 mut 賦值即可（AccountPool 為單一 owner）。
        let mut pool = test_pool(vec![mk("t", true, 0, 0.0, 0.0, 0.0)], true, 0, 2);
        pool.auth_error_threshold = 3;

        // 第 1 次 auth_error：last_error 落盤、failures=1、仍 valid（未達門檻）。
        pool.mark_invalid("t", "auth_error", "upstream 401 X").await;
        {
            let st = pool.state.lock().await;
            let a = &st.accounts[account_pos(&st, "t").unwrap()];
            assert_eq!(a.last_error, "upstream 401 X", "last_error 必寫入");
            assert_eq!(a.status_code, "auth_error");
            assert_eq!(a.consecutive_failures, 1);
            assert!(a.valid, "未達門檻不應立即失效");
        }

        // 第 2 次：failures=2 仍 valid。
        pool.mark_invalid("t", "auth_error", "upstream 401 Y").await;
        {
            let st = pool.state.lock().await;
            let a = &st.accounts[account_pos(&st, "t").unwrap()];
            assert_eq!(a.consecutive_failures, 2);
            assert!(a.valid, "第 2 次仍未達門檻");
            assert_eq!(a.last_error, "upstream 401 Y", "last_error 覆寫為最新");
        }

        // 第 3 次：達門檻 → valid=false。
        pool.mark_invalid("t", "auth_error", "upstream 401 Z").await;
        {
            let st = pool.state.lock().await;
            let a = &st.accounts[account_pos(&st, "t").unwrap()];
            assert_eq!(a.consecutive_failures, 3);
            assert!(!a.valid, "達門檻應永久失效");
        }

        // mark_success 應重置 failures（驗證「真死/瞬時」可區分）。
        pool.apply_verify("t", true, "valid", "").await;
        {
            let st = pool.state.lock().await;
            let a = &st.accounts[account_pos(&st, "t").unwrap()];
            assert_eq!(a.consecutive_failures, 0);
            assert!(a.valid);
        }

        // 非 auth_error reason 必須立即失效（pending_activation 不該享受門檻）。
        pool.mark_invalid("t", "pending_activation", "尚未激活").await;
        {
            let st = pool.state.lock().await;
            let a = &st.accounts[account_pos(&st, "t").unwrap()];
            assert!(!a.valid, "pending_activation 應立即失效");
            assert!(a.activation_pending, "activation_pending 旗標必設");
        }
    }

    /// replace_token：refresh 流程的核心。新 token 寫入 + 健康狀態完整 reset。
    /// 重點：先前累積的 consecutive_failures、last_error、status_code 都不能殘留。
    #[tokio::test]
    async fn replace_token_resets_full_health() {
        let pool = test_pool(vec![mk("r", false, 0, 0.0, 0.0, 0.0)], true, 0, 2);
        // 模擬一個「死了」的帳號：valid=false、有 last_error、failures=2、status=auth_error
        {
            let mut st = pool.state.lock().await;
            let pos = account_pos(&st, "r").unwrap();
            let a = &mut st.accounts[pos];
            a.valid = false;
            a.status_code = "auth_error".to_string();
            a.last_error = "舊上游 401 訊息".to_string();
            a.consecutive_failures = 2;
            a.rate_limit_strikes = 1;
            a.activation_pending = true;
            a.token = "OLD".to_string();
        }

        let ok = pool.replace_token("r", "NEW_209_CHAR_JWT".to_string()).await;
        assert!(ok);

        let st = pool.state.lock().await;
        let a = &st.accounts[account_pos(&st, "r").unwrap()];
        assert_eq!(a.token, "NEW_209_CHAR_JWT");
        assert!(a.valid, "valid 必還原");
        assert_eq!(a.status_code, "valid");
        assert_eq!(a.last_error, "", "last_error 必清");
        assert_eq!(a.consecutive_failures, 0, "failures 必歸零");
        assert_eq!(a.rate_limit_strikes, 0, "rate_limit_strikes 必歸零");
        assert!(!a.activation_pending, "activation_pending 必清");
    }

    /// replace_token 對不存在的 email 應回 false 且不 panic。
    #[tokio::test]
    async fn replace_token_unknown_email_is_noop() {
        let pool = test_pool(vec![mk("x", true, 0, 0.0, 0.0, 0.0)], true, 0, 2);
        let ok = pool.replace_token("nonexistent@nope", "X".to_string()).await;
        assert!(!ok);
    }

    /// release 後（min_interval=0）帳號應可再次取得；global_in_use 收支平衡。
    #[test]
    fn release_returns_to_ready() {
        let accounts = vec![mk("r", true, 0, 0.0, 0.0, 0.0)];
        let pool = test_pool(accounts, true, 0, 1);
        let ex = HashSet::new();
        {
            let mut st = pool.state.try_lock().unwrap();
            assert!(pool.acquire_locked(&mut st, None, &ex).is_some());
            // cap=1 已滿，再取落空
            assert!(pool.acquire_locked(&mut st, None, &ex).is_none());
            assert_eq!(st.global_in_use, 1);
        }
        // release（async，用 try_lock 內部）— 直接走內部邏輯
        {
            let mut st = pool.state.try_lock().unwrap();
            let pos = account_pos(&st, "r").unwrap();
            st.accounts[pos].inflight = 0;
            reindex_email(&mut st, "r", 0);
            st.global_in_use = 0;
            // 釋放後應可再取
            assert!(pool.acquire_locked(&mut st, None, &ex).is_some());
        }
    }

    /// 驗證索引與帳號真實狀態一致（min_interval=0 時為時間穩定不變式）。
    fn check_consistency(st: &PoolState, min_ms: u64) {
        let Some(idx) = st.idx.as_ref() else { return };
        let cap = st.max_inflight_per_account;
        // pos 正確且與帳號數一致
        assert_eq!(idx.pos.len(), st.accounts.len(), "pos 數量不符");
        for (email, &pos) in &idx.pos {
            assert!(pos < st.accounts.len(), "pos 越界");
            assert_eq!(&st.accounts[pos].email, email, "pos 指向錯帳號");
        }
        // loc 必須等於依即時狀態算出的期望（任何遺漏 reindex 都會被抓到）
        for a in &st.accounts {
            let expected = if !a.valid || a.inflight >= cap {
                Loc::Idle
            } else if a.is_available(min_ms) {
                Loc::Ready
            } else {
                Loc::Cooldown
            };
            assert_eq!(idx.loc.get(&a.email).copied(), Some(expected), "{} loc 不一致", a.email);
        }
        // ready 堆中 loc 仍為 Ready 者，必須真的可取
        for Reverse((_, _, email)) in &idx.ready {
            if idx.loc.get(email) == Some(&Loc::Ready) {
                let pos = idx.pos[email];
                let a = &st.accounts[pos];
                assert!(a.valid && a.is_available(min_ms) && a.inflight < cap, "ready 含不可用帳號 {email}");
            }
        }
    }

    /// 隨機操作序列下，索引始終與帳號狀態一致、global_in_use 收支平衡、取出者皆合法。
    #[tokio::test]
    async fn random_ops_keep_index_consistent() {
        use rand::{Rng, SeedableRng};
        let mut rng = rand::rngs::StdRng::seed_from_u64(0x00C0FFEE);
        let n = 12usize;
        let cap = 2i64;
        // min_interval=0：無風控冷卻 → 不變式時間穩定（rate-limit 冷卻 600s 不會於測試內到期）。
        let min_ms = 0u64;
        let accounts: Vec<Account> = (0..n).map(|i| mk(&format!("acc{i}"), true, 0, 0.0, 0.0, 0.0)).collect();
        let pool = test_pool(accounts, true, min_ms, cap);
        let emails: Vec<String> = (0..n).map(|i| format!("acc{i}")).collect();
        let ex = HashSet::new();
        let mut held: Vec<String> = Vec::new();

        for _ in 0..4000 {
            match rng.gen_range(0..7) {
                0 | 1 => {
                    let mut st = pool.state.try_lock().unwrap();
                    if let Some(h) = pool.acquire_locked(&mut st, None, &ex) {
                        let pos = account_pos(&st, &h.email).unwrap();
                        let a = &st.accounts[pos];
                        assert!(a.valid && a.inflight <= cap, "取出非法帳號 {}", h.email);
                        held.push(h.email);
                    }
                }
                2 => {
                    if !held.is_empty() {
                        let i = rng.gen_range(0..held.len());
                        let e = held.remove(i);
                        pool.release(&e).await;
                    }
                }
                3 => {
                    let e = emails[rng.gen_range(0..n)].clone();
                    pool.mark_rate_limited(&e).await;
                }
                4 => {
                    let e = emails[rng.gen_range(0..n)].clone();
                    pool.mark_success(&e).await;
                }
                5 => {
                    let e = emails[rng.gen_range(0..n)].clone();
                    pool.mark_invalid(&e, "auth_error", "rand-test-error").await;
                }
                _ => {
                    // 復活，避免帳號全失效後無事可測
                    let e = emails[rng.gen_range(0..n)].clone();
                    pool.apply_verify(&e, true, "valid", "").await;
                }
            }
            let st = pool.state.lock().await;
            check_consistency(&st, min_ms);
            assert_eq!(st.global_in_use, held.len() as i64, "global_in_use 與持有數不符");
        }
    }

    /// 就緒堆依 (inflight, last_used) 排序：min_interval=0、cap>1 時應選負載最少者（對齊 acquire_scan）。
    /// 鎖定對 impl-review minor#1 的修正（取代舊 FIFO，消除熱帳號偏斜）。
    #[test]
    fn ready_prefers_least_inflight() {
        let accounts = vec![
            mk("a", true, 1, 0.0, 0.0, 0.0), // 已有 1 個在途
            mk("b", true, 0, 0.0, 0.0, 0.0), // 閒置
        ];
        let pool = test_pool(accounts, true, 0, 2);
        let mut st = pool.state.try_lock().unwrap();
        let ex = HashSet::new();
        let h = pool.acquire_locked(&mut st, None, &ex);
        assert_eq!(h.map(|h| h.email), Some("b".to_string()), "應選 inflight 最少的 b");
    }
}
