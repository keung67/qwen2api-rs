/* ============================================================
   qwen2API 企业网关 - 管理台前端逻辑
   纯原生 JS（无框架/无构建/可离线）。
   组织方式：共用工具 (auth/fetch/toast/dom) + 路由 + 每页一个 render 函式。
   ============================================================ */

'use strict';

/* ============================================================
   1. 共用工具：认证 / 请求封装 / toast / DOM 辅助
   ============================================================ */

const KEY_STORE = 'qwen2api_key';

/** 读取当前会话 Key（默认 admin） */
function getKey() {
  return localStorage.getItem(KEY_STORE) || 'admin';
}
/** 写入会话 Key */
function setKey(k) {
  if (k) localStorage.setItem(KEY_STORE, k);
  else localStorage.removeItem(KEY_STORE);
}
/** 共用认证头：所有请求带 Bearer */
function authHeaders(extra) {
  return Object.assign(
    { 'Authorization': 'Bearer ' + getKey() },
    extra || {}
  );
}

/**
 * fetch 封装（JSON）。返回 { ok, status, data }。
 * 自动带 auth 头；body 为对象时自动序列化并加 Content-Type。
 */
async function api(path, opts) {
  opts = opts || {};
  const headers = authHeaders(opts.headers);
  let body = opts.body;
  if (body && typeof body === 'object' && !(body instanceof FormData)) {
    headers['Content-Type'] = 'application/json';
    body = JSON.stringify(body);
  }
  const resp = await fetch(path, {
    method: opts.method || 'GET',
    headers,
    body,
  });
  let data = null;
  const text = await resp.text();
  if (text) {
    try { data = JSON.parse(text); }
    catch (e) { data = { raw: text }; }
  }
  return { ok: resp.ok, status: resp.status, data };
}

/** toast 通知：type = success | error | info */
function toast(msg, type) {
  type = type || 'info';
  const box = document.getElementById('toasts');
  const el = document.createElement('div');
  el.className = 'toast ' + type;
  const icons = { success: '✅', error: '❌', info: 'ℹ️' };
  el.innerHTML = `<span class="t-ico">${icons[type] || ''}</span><span>${escapeHtml(msg)}</span>`;
  box.appendChild(el);
  setTimeout(() => {
    el.classList.add('hide');
    setTimeout(() => el.remove(), 250);
  }, 3200);
}

/** HTML 转义，避免注入 */
function escapeHtml(s) {
  if (s == null) return '';
  return String(s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

/** 复制到剪贴板（带降级方案） */
async function copyText(text) {
  try {
    await navigator.clipboard.writeText(text);
    return true;
  } catch (e) {
    // 降级：用临时 textarea
    try {
      const ta = document.createElement('textarea');
      ta.value = text;
      ta.style.position = 'fixed';
      ta.style.opacity = '0';
      document.body.appendChild(ta);
      ta.select();
      document.execCommand('copy');
      ta.remove();
      return true;
    } catch (e2) { return false; }
  }
}

/** 简易 DOM 工厂：el('div', {class:'x'}, ['hi']) */
function el(tag, attrs, children) {
  const node = document.createElement(tag);
  if (attrs) {
    for (const k in attrs) {
      if (k === 'class') node.className = attrs[k];
      else if (k === 'html') node.innerHTML = attrs[k];
      else if (k.startsWith('on') && typeof attrs[k] === 'function') {
        node.addEventListener(k.slice(2), attrs[k]);
      } else if (attrs[k] != null) {
        node.setAttribute(k, attrs[k]);
      }
    }
  }
  if (children != null) {
    (Array.isArray(children) ? children : [children]).forEach(c => {
      if (c == null) return;
      node.appendChild(typeof c === 'string' ? document.createTextNode(c) : c);
    });
  }
  return node;
}

/** 取得视图容器并清空 */
function viewEl() {
  const v = document.getElementById('view');
  v.innerHTML = '';
  return v;
}

/** 更新侧边栏连接状态指示灯 */
function setConn(ok, text) {
  const dot = document.getElementById('connDot');
  const t = document.getElementById('connText');
  dot.className = 'dot ' + (ok ? 'ok' : 'bad');
  t.textContent = text || (ok ? '已连接' : '未连接');
}

/* ============================================================
   2. 路由（hash）
   ============================================================ */

const routes = {
  '/': renderDashboard,
  '/stats': renderStats,
  '/accounts': renderAccounts,
  '/tokens': renderTokens,
  '/test': renderTest,
  '/images': renderImages,
  '/videos': renderVideos,
  '/settings': renderSettings,
};

let cleanupFn = null; // 离开页面时调用（清定时器等）

function currentRoute() {
  let h = location.hash.replace(/^#/, '');
  if (!h) h = '/';
  return h;
}

function router() {
  // 先执行上一页的清理
  if (typeof cleanupFn === 'function') {
    try { cleanupFn(); } catch (e) {}
    cleanupFn = null;
  }
  const path = currentRoute();
  const fn = routes[path] || renderDashboard;

  // 高亮侧边栏
  document.querySelectorAll('.nav-item').forEach(a => {
    a.classList.toggle('active', a.getAttribute('data-route') === path);
  });

  // 移动端：选中后收合侧边栏
  document.body.classList.remove('nav-open');

  fn();
}

window.addEventListener('hashchange', router);

/* ============================================================
   3. 页面 1：运行状态 (Dashboard)
   ============================================================ */

let dashStatusFailNotified = false; // 状态获取失败只提示一次

function renderDashboard() {
  const v = viewEl();
  dashStatusFailNotified = false;

  v.appendChild(el('div', { class: 'page-head' }, [
    el('div', null, [
      el('h1', null, '运行状态'),
      el('p', null, '实时监控网关账号、并发与预热池（每 3 秒刷新）'),
    ]),
  ]));

  const statsBox = el('div', { id: 'dashStats', class: 'stats-grid' });
  v.appendChild(statsBox);

  // 账号并发详情
  const accCard = el('div', { class: 'card' }, [
    el('div', { class: 'card-title' }, '账号并发详情'),
    el('div', { id: 'dashTable' }, [loadingNode()]),
  ]);
  v.appendChild(accCard);

  // 静态接口池
  v.appendChild(apiPoolCard());

  let timer = null;
  async function poll() {
    const r = await api('/api/admin/status');
    if (!r.ok) {
      setConn(false, '会话异常');
      if (!dashStatusFailNotified) {
        toast('状态获取失败，请在系统设置检查会话 Key', 'error');
        dashStatusFailNotified = true;
      }
      return;
    }
    setConn(true);
    dashStatusFailNotified = false;
    renderDashStats(statsBox, r.data);
    renderDashTable(document.getElementById('dashTable'), r.data);
  }

  poll();
  timer = setInterval(poll, 3000);
  cleanupFn = () => clearInterval(timer);
}

/** 渲染顶部统计卡 */
function renderDashStats(box, d) {
  const a = d.accounts || {};
  const pool = d.chat_id_pool;
  const rt = d.runtime || {};
  const cards = [
    {
      label: '可用账号', ico: '✅',
      num: `${a.valid ?? 0}<small> / ${a.total ?? 0}</small>`,
      foot: '有效账号 / 总账号',
    },
    {
      label: '当前并发', ico: '⚡',
      num: `${a.global_in_use ?? a.in_use ?? 0}`,
      foot: (a.global_max_inflight ?? 0) > 0
        ? `全局上限 ${a.global_max_inflight}（有效号 × 单号并发）`
        : '全局上限 自动（无可用账号）',
    },
    {
      label: '排队请求', ico: '⏳',
      num: `${a.waiting ?? 0}`,
      foot: `队列上限 ${a.max_queue_size ?? 0}`,
    },
    {
      label: '限流 / 失效号', ico: '🚫',
      num: `${a.rate_limited ?? 0}<small> / ${a.invalid ?? 0}</small>`,
      foot: '限流号 / 失效号',
    },
    {
      label: 'Chat_ID 预热池', ico: '🔥',
      num: pool ? `${pool.total_cached ?? 0}` : '—',
      foot: pool
        ? `每账号目标 ${pool.target_per_account ?? 0} · TTL ${Math.round((pool.ttl_seconds ?? 0) / 60)} 分钟`
        : '未启用',
    },
    {
      label: '异步任务', ico: '🧵',
      num: `${rt.asyncio_running_tasks ?? 0}`,
      foot: '运行中任务数',
    },
    {
      label: 'T2V 跳过集', ico: '⏭️',
      num: `${d.no_t2v_skipped ?? 0}`,
      foot: '已学到的无 t2v 权限帐号',
    },
  ];
  box.innerHTML = '';
  cards.forEach(c => {
    box.appendChild(el('div', { class: 'stat' }, [
      el('div', { class: 'label' }, [el('span', { class: 'ico' }, c.ico), c.label]),
      el('div', { class: 'num', html: c.num }),
      el('div', { class: 'foot' }, c.foot),
    ]));
  });
}

/** 渲染账号并发详情表 */
function renderDashTable(host, d) {
  const list = d.per_account || [];
  const pool = d.chat_id_pool;
  const poolPer = (pool && pool.per_account) || {};
  host.innerHTML = '';
  if (!list.length) {
    host.appendChild(el('div', { class: 'empty' }, '暂无账号数据'));
    return;
  }
  const rows = list.map(p => {
    const warmed = poolPer[p.email] != null ? poolPer[p.email] : '—';
    return `<tr>
      <td class="mono">${escapeHtml(p.email)}</td>
      <td>${statusBadge(p.status)}</td>
      <td>${p.inflight ?? 0} / ${p.max_inflight ?? 0}</td>
      <td>${warmed}</td>
      <td>${p.consecutive_failures ?? 0}</td>
      <td>${p.rate_limit_strikes ?? 0}</td>
    </tr>`;
  }).join('');
  host.appendChild(elTable(
    ['邮箱', '状态', '在途', '预热chat_id', '连失', '限流次'],
    rows
  ));
}

/** dashboard 状态字符串 -> 徽章（valid绿 / rate_limited橙 / 其他红） */
function statusBadge(status) {
  const s = String(status || '').toLowerCase();
  if (s === 'valid') return `<span class="badge badge-green">valid</span>`;
  if (s === 'rate_limited') return `<span class="badge badge-orange">rate_limited</span>`;
  return `<span class="badge badge-red">${escapeHtml(status || 'unknown')}</span>`;
}

/** 静态 API 接口池卡片 */
function apiPoolCard() {
  const apis = [
    { m: 'POST', p: '/v1/chat/completions', t: 'OpenAI' },
    { m: 'POST', p: '/v1/messages', t: 'Anthropic' },
    { m: 'POST', p: '/v1beta/models/{model}:generateContent', t: 'Gemini' },
    { m: 'POST', p: '/v1/images/generations', t: 'Image' },
    { m: 'POST', p: '/v1/videos/generations', t: 'Video' },
    { m: 'POST', p: '/v1/files', t: 'Files' },
    { m: 'GET', p: '/', t: '健康检查' },
  ];
  const rows = apis.map(a => el('div', { class: 'api-row' }, [
    el('span', { class: 'method ' + a.m.toLowerCase() }, a.m),
    el('span', { class: 'path' }, a.p),
    el('span', { class: 'tag' }, a.t),
  ]));
  return el('div', { class: 'card' }, [
    el('div', { class: 'card-title' }, 'API 接口池'),
    el('div', { class: 'api-list' }, rows),
  ]);
}

/* ============================================================
   3.5 页面：数据统计 (Stats Dashboard)
   ============================================================ */

const STATS_RANGES = [
  { key: '1h', label: '近 1 小时' },
  { key: '6h', label: '近 6 小时' },
  { key: '24h', label: '近 24 小时' },
  { key: '7d', label: '近 7 天' },
  { key: 'all', label: '全部' },
];
// 跨进入保留所选区间
const statsState = { range: '24h' };

/** 千分位整数 */
function fmtInt(n) {
  if (n == null) return '—';
  return Number(n).toLocaleString('en-US');
}
/** 毫秒 → 友好耗时（<1s 显示 ms，否则 s） */
function fmtMs(ms) {
  if (ms == null) return '—';
  if (ms < 1000) return Math.round(ms) + ' ms';
  return (ms / 1000).toFixed(2) + ' s';
}
/** 毫秒时间戳 → 本地 HH:MM:SS */
function fmtClock(ms) {
  if (!ms) return '—';
  const d = new Date(ms);
  const p = (x) => String(x).padStart(2, '0');
  return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}

function renderStats() {
  const v = viewEl();

  const head = el('div', { class: 'page-head' }, [
    el('div', null, [
      el('h1', null, '数据统计'),
      el('p', null, '请求量、Token 用量、首字延迟与完成耗时（每 5 秒刷新）'),
    ]),
    el('div', { class: 'head-actions', id: 'statsRanges' },
      STATS_RANGES.map(r => el('button', {
        class: 'btn btn-sm' + (r.key === statsState.range ? ' btn-primary' : ''),
        'data-range': r.key,
        onclick: () => { statsState.range = r.key; markRange(); poll(); },
      }, r.label))
    ),
  ]);
  v.appendChild(head);

  function markRange() {
    document.querySelectorAll('#statsRanges button').forEach(b => {
      b.classList.toggle('btn-primary', b.getAttribute('data-range') === statsState.range);
    });
  }

  // 总览卡
  const statsBox = el('div', { id: 'statsOverview', class: 'stats-grid' });
  v.appendChild(statsBox);

  // 趋势图
  const chartCard = el('div', { class: 'card' }, [
    el('div', { class: 'card-title' }, '请求趋势'),
    el('div', { id: 'statsChart' }, [loadingNode()]),
  ]);
  v.appendChild(chartCard);

  // 按模型 / 按接口
  const splitRow = el('div', { class: 'stats-two-col' }, [
    el('div', { class: 'card' }, [
      el('div', { class: 'card-title' }, '按模型'),
      el('div', { id: 'statsByModel' }, [loadingNode()]),
    ]),
    el('div', { class: 'card' }, [
      el('div', { class: 'card-title' }, '按接口'),
      el('div', { id: 'statsBySurface' }, [loadingNode()]),
    ]),
  ]);
  v.appendChild(splitRow);

  // 最近请求
  v.appendChild(el('div', { class: 'card' }, [
    el('div', { class: 'card-title' }, '最近请求'),
    el('div', { id: 'statsRecent' }, [loadingNode()]),
  ]));

  let failNotified = false;
  async function poll() {
    const [r, rr] = await Promise.all([
      api('/api/admin/stats?range=' + encodeURIComponent(statsState.range)),
      api('/api/admin/stats/recent?limit=50'),
    ]);
    if (!r.ok) {
      setConn(false, '会话异常');
      if (!failNotified) { toast('统计获取失败，请在系统设置检查会话 Key', 'error'); failNotified = true; }
      return;
    }
    setConn(true);
    failNotified = false;
    renderStatsOverview(statsBox, r.data);
    renderStatsChart(document.getElementById('statsChart'), r.data);
    renderStatsByModel(document.getElementById('statsByModel'), r.data);
    renderStatsBySurface(document.getElementById('statsBySurface'), r.data);
    if (rr.ok) renderStatsRecent(document.getElementById('statsRecent'), rr.data);
  }

  poll();
  const timer = setInterval(poll, 5000);
  cleanupFn = () => clearInterval(timer);
}

function renderStatsOverview(box, d) {
  const s = (d && d.summary) || {};
  const rate = s.requests ? Math.round((s.success_rate || 0) * 100) : 0;
  const cards = [
    { label: '总请求', ico: '📨', num: fmtInt(s.requests || 0), foot: `成功 ${fmtInt(s.success || 0)} · 失败 ${fmtInt(s.failed || 0)}` },
    { label: '成功率', ico: '✅', num: `${rate}<small>%</small>`, foot: (d && d.dropped) ? `统计丢弃 ${fmtInt(d.dropped)} 笔` : '采样完整' },
    { label: '即时 RPM', ico: '⚡', num: fmtInt(d ? d.rpm_now : 0), foot: '最近 60 秒请求数' },
    { label: '输入 Tokens', ico: '⬆️', num: fmtInt(s.prompt_tokens || 0), foot: 'prompt_tokens 累计' },
    { label: '输出 Tokens', ico: '⬇️', num: fmtInt(s.completion_tokens || 0), foot: `含思考 ${fmtInt(s.reasoning_tokens || 0)}` },
    { label: '总 Tokens', ico: '🔢', num: fmtInt(s.total_tokens || 0), foot: '输入 + 输出' },
    { label: '平均首字延迟', ico: '⏱', num: fmtMs(s.avg_ttft_ms), foot: 'TTFT 平均' },
    { label: '平均完成耗时', ico: '🏁', num: fmtMs(s.avg_duration_ms), foot: '请求总耗时平均' },
  ];
  box.innerHTML = '';
  cards.forEach(c => {
    box.appendChild(el('div', { class: 'stat' }, [
      el('div', { class: 'label' }, [el('span', { class: 'ico' }, c.ico), c.label]),
      el('div', { class: 'num', html: String(c.num) }),
      el('div', { class: 'foot' }, c.foot),
    ]));
  });
}

/** 趋势图：请求数（面积折线）+ 旁注 token / 平均延迟 */
function renderStatsChart(host, d) {
  const pts = (d && d.timeseries && d.timeseries.points) || [];
  host.innerHTML = '';
  if (!pts.length) {
    host.appendChild(el('div', { class: 'empty' }, '所选区间暂无请求数据'));
    return;
  }
  const bucketMs = (d.timeseries && d.timeseries.bucket_ms) || 60000;
  const peak = Math.max(...pts.map(p => p.requests || 0), 1);
  host.appendChild(svgAreaChart(pts.map(p => p.requests || 0), {
    labels: pts.map(p => fmtClock(p.t)),
    tipFmt: (val, i) => `${fmtClock(pts[i].t)} · ${val} 请求 · ${fmtInt(pts[i].tokens || 0)} tokens`,
  }));
  host.appendChild(el('div', { class: 'chart-legend' }, [
    el('span', null, `峰值 ${peak} 请求 / 桶`),
    el('span', null, `分桶 ${bucketMs >= 3600000 ? (bucketMs / 3600000) + ' 小时' : (bucketMs / 60000) + ' 分钟'}`),
    el('span', null, `${pts.length} 个数据点`),
  ]));
}

/**
 * 纯 SVG 面积折线图（无外部依赖）。values：数值数组；opts.tipFmt 生成 hover 提示。
 */
function svgAreaChart(values, opts) {
  opts = opts || {};
  const W = 720, H = 180, padT = 12, padB = 14, padX = 6;
  const n = values.length;
  const maxV = Math.max(1, ...values);
  const xAt = (i) => padX + (n <= 1 ? (W - 2 * padX) / 2 : i * (W - 2 * padX) / (n - 1));
  const yAt = (v) => padT + (1 - v / maxV) * (H - padT - padB);
  const line = values.map((v, i) => `${xAt(i).toFixed(1)},${yAt(v).toFixed(1)}`).join(' ');
  const area = `${xAt(0).toFixed(1)},${(H - padB).toFixed(1)} ${line} ${xAt(n - 1).toFixed(1)},${(H - padB).toFixed(1)}`;
  // 数据点稀疏（≤24）时显示实心圆点，单点也清晰可见；密集时仅保留 hover 命中区。
  const showDots = n <= 24;
  const dots = values.map((v, i) => {
    const cx = xAt(i).toFixed(1), cy = yAt(v).toFixed(1);
    const tip = `<title>${escapeHtml(opts.tipFmt ? opts.tipFmt(v, i) : String(v))}</title>`;
    const visible = showDots ? `<circle cx="${cx}" cy="${cy}" r="3" fill="#7c5cff"/>` : '';
    return `${visible}<circle cx="${cx}" cy="${cy}" r="7" fill="transparent">${tip}</circle>`;
  }).join('');
  const baseline = `M ${padX} ${(H - padB).toFixed(1)} H ${(W - padX).toFixed(1)}`;
  const wrap = el('div', { class: 'chart-wrap' });
  wrap.innerHTML =
    `<svg viewBox="0 0 ${W} ${H}" class="chart-svg" preserveAspectRatio="none">
      <defs><linearGradient id="cgrad" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0" stop-color="#7c5cff" stop-opacity="0.38"/>
        <stop offset="1" stop-color="#7c5cff" stop-opacity="0"/>
      </linearGradient></defs>
      <path d="${baseline}" stroke="var(--border)" stroke-width="1" fill="none" vector-effect="non-scaling-stroke"/>
      <polygon points="${area}" fill="url(#cgrad)"/>
      <polyline points="${line}" fill="none" stroke="#7c5cff" stroke-width="2.5" stroke-linejoin="round" vector-effect="non-scaling-stroke"/>
      ${dots}
    </svg>`;
  return wrap;
}

function renderStatsByModel(host, d) {
  const list = (d && d.by_model) || [];
  host.innerHTML = '';
  if (!list.length) { host.appendChild(el('div', { class: 'empty' }, '暂无数据')); return; }
  // 平均值無法反映尾部退化；後端在 by_model 上補了 min/p50/p95/max TTFT，
  // 此處主顯 p50（典型）+ p95（尾部）；min/max 放 title 供 hover 細看。
  const rows = list.map(m => {
    const p50 = m.p50_ttft_ms, p95 = m.p95_ttft_ms;
    const hasPct = p50 != null || p95 != null;
    const main = hasPct
      ? `${p50 != null ? fmtMs(p50) : '—'} <small style="color:var(--fg-dim)">/ ${p95 != null ? fmtMs(p95) : '—'}</small>`
      : (m.avg_ttft_ms != null ? fmtMs(m.avg_ttft_ms) : '—');
    const tip = hasPct
      ? `min ${m.min_ttft_ms != null ? fmtMs(m.min_ttft_ms) : '—'} · p50 ${p50 != null ? fmtMs(p50) : '—'} · p95 ${p95 != null ? fmtMs(p95) : '—'} · max ${m.max_ttft_ms != null ? fmtMs(m.max_ttft_ms) : '—'} · 平均 ${m.avg_ttft_ms != null ? fmtMs(m.avg_ttft_ms) : '—'}`
      : '该模型暂无首字延迟样本';
    return `<tr>
      <td class="mono">${escapeHtml(m.model || '—')}</td>
      <td>${fmtInt(m.requests)}</td>
      <td>${fmtInt(m.total_tokens)}</td>
      <td title="${escapeHtml(tip)}">${main}</td>
      <td>${fmtMs(m.avg_duration_ms)}</td>
    </tr>`;
  }).join('');
  host.appendChild(elTable(['模型', '请求', 'Tokens', 'TTFT p50/p95', '平均耗时'], rows));
}

function renderStatsBySurface(host, d) {
  const list = (d && d.by_surface) || [];
  host.innerHTML = '';
  if (!list.length) { host.appendChild(el('div', { class: 'empty' }, '暂无数据')); return; }
  const SURFACE_CN = { openai: 'OpenAI', anthropic: 'Anthropic', gemini: 'Gemini', responses: 'Responses', images: '图片', videos: '影片', embeddings: '向量' };
  const rows = list.map(s => `<tr>
    <td>${escapeHtml(SURFACE_CN[s.surface] || s.surface || '—')}</td>
    <td>${fmtInt(s.requests)}</td>
    <td>${fmtInt(s.total_tokens)}</td>
  </tr>`).join('');
  host.appendChild(elTable(['接口', '请求', 'Tokens'], rows));
}

function renderStatsRecent(host, d) {
  const list = (d && d.items) || [];
  host.innerHTML = '';
  if (!list.length) { host.appendChild(el('div', { class: 'empty' }, '暂无请求记录')); return; }
  const rows = list.map(it => {
    const ok = it.success;
    const badge = ok
      ? `<span class="badge badge-green">成功</span>`
      : `<span class="badge badge-red" title="${escapeHtml(it.error || '')}">失败</span>`;
    const type = it.chat_type && it.chat_type !== 't2t' ? it.chat_type : '';
    return `<tr>
      <td class="mono">${fmtClock(it.ts_ms)}</td>
      <td>${escapeHtml(it.surface || '')}${type ? ` <small style="color:var(--fg-dim)">${escapeHtml(type)}</small>` : ''}</td>
      <td class="mono">${escapeHtml(it.model || '')}</td>
      <td>${fmtInt(it.prompt_tokens)} / ${fmtInt(it.completion_tokens)}</td>
      <td>${it.ttft_ms != null ? fmtMs(it.ttft_ms) : '—'}</td>
      <td>${fmtMs(it.duration_ms)}</td>
      <td>${badge}</td>
    </tr>`;
  }).join('');
  host.appendChild(elTable(['时间', '接口', '模型', '输入/输出', '首字', '耗时', '状态'], rows));
}

/* ============================================================
   4. 页面 2：账号管理
   ============================================================ */

// 账号列表视图状态（分页 / 搜索 / 状态过滤），操作后刷新保留当前查询
const accState = { page: 1, page_size: 50, q: '', status: '' };

// 账号 status_code -> 中文
const ACC_STATUS_CN = {
  valid: '可用',
  pending_activation: '未激活',
  rate_limited: '限流',
  banned: '封禁',
  auth_error: '认证失效',
};
function accStatusCn(code) {
  return ACC_STATUS_CN[code] || '失效';
}
// status_code -> 徽章样式
function accStatusBadge(code) {
  const map = {
    valid: 'badge-green',
    pending_activation: 'badge-blue',
    rate_limited: 'badge-orange',
    banned: 'badge-red',
    auth_error: 'badge-red',
  };
  const cls = map[code] || 'badge-gray';
  return `<span class="badge ${cls}">${escapeHtml(accStatusCn(code))}</span>`;
}

function renderAccounts() {
  const v = viewEl();
  v.appendChild(el('div', { class: 'page-head' }, [
    el('div', null, [
      el('h1', null, '账号管理'),
      el('p', null, '注入 / 验证 / 删除 Qwen 账号，监控可用状态'),
    ]),
    el('div', { class: 'head-actions' }, [
      el('button', { class: 'btn', onclick: () => loadAccounts() }, '🔄 刷新'),
      el('button', { class: 'btn btn-primary', id: 'btnVerifyAll', onclick: verifyAll }, '🔍 全量巡检'),
    ]),
  ]));

  // 统计卡
  v.appendChild(el('div', { id: 'accStats', class: 'stats-grid' }));

  // 手动注入表单
  const injectCard = el('div', { class: 'card' }, [
    el('div', { class: 'card-title' }, '手动注入账号'),
    el('div', { class: 'notice notice-warn' }, [
      el('span', { class: 'ico' }, '⚠️'),
      el('span', null, '请先在 chat.qwen.ai 登录，F12 在 Application/本地存储 中找到 token 原始值直接粘贴，不要带 Bearer 前缀，不要从 Network 请求头提取。'),
    ]),
    el('div', { class: 'field' }, [
      el('label', null, 'Token（必填）'),
      el('input', { class: 'input', id: 'injToken', placeholder: '粘贴 token 原始值' }),
    ]),
    el('div', { class: 'form-row' }, [
      el('div', { class: 'field' }, [
        el('label', null, '邮箱（选填）'),
        el('input', { class: 'input', id: 'injEmail', placeholder: '留空将自动生成' }),
      ]),
      el('div', { class: 'field' }, [
        el('label', null, '密码（选填）'),
        el('input', { class: 'input', id: 'injPass', type: 'password', placeholder: '可选' }),
      ]),
    ]),
    el('button', { class: 'btn btn-primary', id: 'btnInject', onclick: injectAccount }, '注入账号'),
  ]);
  v.appendChild(injectCard);

  // 账号列表（搜索 + 状态过滤 + 分页）
  const searchInput = el('input', { class: 'input', id: 'accSearch', placeholder: '搜索邮箱 / 用户名…', value: accState.q });
  const statusSel = el('select', { class: 'select', id: 'accStatusFilter' }, [
    el('option', { value: '' }, '全部状态'),
    el('option', { value: 'valid' }, '可用'),
    el('option', { value: 'pending_activation' }, '未激活'),
    el('option', { value: 'rate_limited' }, '限流'),
    el('option', { value: 'banned' }, '封禁'),
    el('option', { value: 'auth_error' }, '认证失效'),
    el('option', { value: 'invalid' }, '失效'),
  ]);
  statusSel.value = accState.status;
  const doSearch = () => {
    accState.q = searchInput.value.trim();
    accState.status = statusSel.value;
    accState.page = 1;
    loadAccounts();
  };
  searchInput.addEventListener('keydown', (e) => { if (e.key === 'Enter') { e.preventDefault(); doSearch(); } });
  statusSel.addEventListener('change', doSearch);

  v.appendChild(el('div', { class: 'card' }, [
    el('div', { class: 'card-title' }, '账号列表'),
    el('div', { class: 'acc-filter' }, [
      searchInput, statusSel,
      el('button', { class: 'btn', onclick: doSearch }, '🔍 搜索'),
    ]),
    el('div', { id: 'accTable' }, [loadingNode()]),
    el('div', { id: 'accPager', class: 'pager' }),
  ]));

  loadAccounts();
}

async function loadAccounts() {
  const host = document.getElementById('accTable');
  const statsHost = document.getElementById('accStats');
  if (host) { host.innerHTML = ''; host.appendChild(loadingNode()); }
  const params = new URLSearchParams({ page: String(accState.page), page_size: String(accState.page_size) });
  if (accState.q) params.set('q', accState.q);
  if (accState.status) params.set('status', accState.status);
  const r = await api('/api/admin/accounts?' + params.toString());
  if (!r.ok) {
    setConn(false);
    if (host) { host.innerHTML = ''; host.appendChild(el('div', { class: 'empty' }, '加载失败，请检查会话 Key')); }
    toast('账号加载失败', 'error');
    return;
  }
  setConn(true);
  const d = r.data || {};
  const accounts = d.accounts || [];
  renderAccStats(statsHost, d.counts || {});
  renderAccTable(host, accounts);
  renderAccPager(document.getElementById('accPager'), d.total || 0, d.page || accState.page, d.page_size || accState.page_size);
}

function renderAccStats(host, counts) {
  if (!host) return;
  counts = counts || {};
  const grand = Object.values(counts).reduce((a, b) => a + (b || 0), 0);
  const cards = [
    { label: '总账号', ico: '📦', num: grand },
    { label: '可用', ico: '✅', num: counts.valid || 0 },
    { label: '未激活', ico: '🆕', num: counts.pending_activation || 0 },
    { label: '限流', ico: '⏳', num: counts.rate_limited || 0 },
    { label: '封禁', ico: '🚫', num: counts.banned || 0 },
    { label: '认证失效', ico: '⚠️', num: (counts.auth_error || 0) + (counts.invalid || 0) },
  ];
  host.innerHTML = '';
  cards.forEach(c => {
    host.appendChild(el('div', { class: 'stat' }, [
      el('div', { class: 'label' }, [el('span', { class: 'ico' }, c.ico), c.label]),
      el('div', { class: 'num', html: String(c.num) }),
    ]));
  });
}

/** 分页控制条：显示总数/页码 + 上一页/下一页 */
function renderAccPager(host, total, page, pageSize) {
  if (!host) return;
  host.innerHTML = '';
  const pages = Math.max(1, Math.ceil(total / pageSize));
  page = Math.min(Math.max(1, page), pages);
  host.appendChild(el('span', { class: 'pager-info' }, `共 ${total} 个 · 第 ${page} / ${pages} 页`));
  const prev = el('button', { class: 'btn btn-sm', onclick: () => { if (accState.page > 1) { accState.page--; loadAccounts(); } } }, '‹ 上一页');
  const next = el('button', { class: 'btn btn-sm', onclick: () => { if (accState.page < pages) { accState.page++; loadAccounts(); } } }, '下一页 ›');
  if (page <= 1) prev.disabled = true;
  if (page >= pages) next.disabled = true;
  host.appendChild(el('div', { class: 'pager-btns' }, [prev, next]));
}

function renderAccTable(host, accounts) {
  host.innerHTML = '';
  if (!accounts.length) {
    host.appendChild(el('div', { class: 'empty' }, [el('span', { class: 'big' }, '👤'), '暂无账号，请在上方注入']));
    return;
  }
  const now = Date.now() / 1000;
  const rows = accounts.map(a => {
    // 说明：限流恢复倒计时 或 last_error
    let note = '';
    if (a.rate_limited_until && a.rate_limited_until > now) {
      note = `预计 ${Math.ceil(a.rate_limited_until - now)} 秒后恢复`;
    } else if (a.last_error) {
      note = escapeHtml(a.last_error);
    }
    const email = a.email || '';
    const enc = encodeURIComponent(email);
    // 激活按钮：仅在 status_code 非 valid/rate_limited/banned 时显示
    const showActivate = !['valid', 'rate_limited', 'banned'].includes(a.status_code);
    const activateBtn = showActivate
      ? `<button class="btn btn-sm" data-act="activate" data-email="${escapeHtml(enc)}">激活</button>` : '';
    return `<tr>
      <td class="mono">${escapeHtml(email)}</td>
      <td>${accStatusBadge(a.status_code)}</td>
      <td>${a.inflight ?? 0} 线程</td>
      <td style="max-width:260px;color:var(--fg-dim)">${note || '—'}</td>
      <td><div class="cell-actions">
        <button class="btn btn-sm" data-act="verify" data-email="${escapeHtml(enc)}">验证</button>
        ${activateBtn}
        <button class="btn btn-sm btn-danger" data-act="delete" data-email="${escapeHtml(enc)}" data-raw="${escapeHtml(email)}">删除</button>
      </div></td>
    </tr>`;
  }).join('');

  const tableWrap = elTable(
    ['账号', '状态', '并发负载', '说明', '操作'],
    rows
  );
  host.appendChild(tableWrap);

  // 事件委派
  tableWrap.addEventListener('click', async (e) => {
    const btn = e.target.closest('button[data-act]');
    if (!btn) return;
    const act = btn.getAttribute('data-act');
    const enc = btn.getAttribute('data-email');
    if (act === 'verify') {
      btn.disabled = true; btn.textContent = '验证中…';
      const rr = await api(`/api/admin/accounts/${enc}/verify`, { method: 'POST' });
      if (rr.ok) toast(rr.data && rr.data.valid ? '账号有效' : '账号无效', rr.data && rr.data.valid ? 'success' : 'error');
      else toast('验证请求失败', 'error');
      loadAccounts();
    } else if (act === 'activate') {
      btn.disabled = true;
      const rr = await api(`/api/admin/accounts/${enc}/activate`, { method: 'POST' });
      // 后端已移除该功能，通常返回 501
      const msg = (rr.data && rr.data.error) || '激活功能已移除';
      toast(msg, 'error');
      btn.disabled = false;
    } else if (act === 'delete') {
      const raw = btn.getAttribute('data-raw');
      if (!confirm(`确认删除账号 ${raw} ？`)) return;
      btn.disabled = true;
      const rr = await api(`/api/admin/accounts/${enc}`, { method: 'DELETE' });
      if (rr.ok) toast('已删除', 'success'); else toast('删除失败', 'error');
      loadAccounts();
    }
  });
}

async function injectAccount() {
  const btn = document.getElementById('btnInject');
  const token = document.getElementById('injToken').value.trim();
  const email = document.getElementById('injEmail').value.trim();
  const password = document.getElementById('injPass').value;
  if (!token) { toast('请填写 Token', 'error'); return; }
  btn.disabled = true; const old = btn.textContent; btn.textContent = '注入中…';
  const body = {
    email: email || `manual_${Date.now()}@qwen`,
    password: password || '',
    token,
  };
  const r = await api('/api/admin/accounts', { method: 'POST', body });
  btn.disabled = false; btn.textContent = old;
  if (r.ok && r.data && r.data.ok) {
    toast(`账号注入成功：${r.data.email || body.email}`, 'success');
    document.getElementById('injToken').value = '';
    document.getElementById('injEmail').value = '';
    document.getElementById('injPass').value = '';
    loadAccounts();
  } else {
    const err = (r.data && r.data.error) || `请求失败 (${r.status})`;
    toast('注入失败：' + err, 'error');
  }
}

async function verifyAll() {
  const btn = document.getElementById('btnVerifyAll');
  btn.disabled = true; const old = btn.textContent; btn.textContent = '巡检中…';
  const r = await api('/api/admin/verify', { method: 'POST' });
  btn.disabled = false; btn.textContent = old;
  if (r.ok && r.data && r.data.ok) {
    const s = r.data.summary;
    toast('全量巡检完成' + (s ? '：' + (typeof s === 'string' ? s : JSON.stringify(s)) : ''), 'success');
    loadAccounts();
  } else {
    toast('巡检失败', 'error');
  }
}

/* ============================================================
   5. 页面 3：API Key
   ============================================================ */

function renderTokens() {
  const v = viewEl();
  v.appendChild(el('div', { class: 'page-head' }, [
    el('div', null, [
      el('h1', null, 'API Key'),
      el('p', null, '管理调用网关的访问密钥'),
    ]),
    el('div', { class: 'head-actions' }, [
      el('button', { class: 'btn', onclick: loadKeys }, '🔄 刷新'),
      el('button', { class: 'btn btn-primary', id: 'btnGenKey', onclick: genKey }, '➕ 生成新 Key'),
    ]),
  ]));
  v.appendChild(el('div', { class: 'card' }, [
    el('div', { class: 'card-title' }, 'Key 列表'),
    el('div', { id: 'keyTable' }, [loadingNode()]),
  ]));
  loadKeys();
}

async function loadKeys() {
  const host = document.getElementById('keyTable');
  host.innerHTML = ''; host.appendChild(loadingNode());
  const r = await api('/api/admin/keys');
  if (!r.ok) {
    setConn(false);
    host.innerHTML = ''; host.appendChild(el('div', { class: 'empty' }, '加载失败，请检查会话 Key'));
    return;
  }
  setConn(true);
  const keys = (r.data && r.data.keys) || [];
  host.innerHTML = '';
  if (!keys.length) {
    host.appendChild(el('div', { class: 'empty' }, [el('span', { class: 'big' }, '🔑'), '暂无 Key，点击右上角生成']));
    return;
  }
  const rows = keys.map((k, i) => {
    const enc = encodeURIComponent(k);
    return `<tr>
      <td>${i + 1}</td>
      <td class="mono">${escapeHtml(k)}</td>
      <td><div class="cell-actions">
        <button class="btn btn-sm" data-act="copy" data-key="${escapeHtml(k)}">复制</button>
        <button class="btn btn-sm btn-danger" data-act="del" data-enc="${escapeHtml(enc)}" data-key="${escapeHtml(k)}">删除</button>
      </div></td>
    </tr>`;
  }).join('');
  const tbl = elTable(['序号', 'API Key', '操作'], rows);
  host.appendChild(tbl);
  tbl.addEventListener('click', async (e) => {
    const btn = e.target.closest('button[data-act]');
    if (!btn) return;
    if (btn.getAttribute('data-act') === 'copy') {
      const ok = await copyText(btn.getAttribute('data-key'));
      toast(ok ? '已复制到剪贴板' : '复制失败', ok ? 'success' : 'error');
    } else {
      const key = btn.getAttribute('data-key');
      if (!confirm(`确认删除 Key ${key} ？`)) return;
      const rr = await api(`/api/admin/keys/${btn.getAttribute('data-enc')}`, { method: 'DELETE' });
      if (rr.ok) toast('已删除', 'success'); else toast('删除失败', 'error');
      loadKeys();
    }
  });
}

async function genKey() {
  const btn = document.getElementById('btnGenKey');
  btn.disabled = true; const old = btn.textContent; btn.textContent = '生成中…';
  const r = await api('/api/admin/keys', { method: 'POST' });
  btn.disabled = false; btn.textContent = old;
  if (r.ok && r.data && r.data.ok) {
    const ok = await copyText(r.data.key);
    toast(ok ? '新 Key 已生成并复制' : '新 Key 已生成', 'success');
    loadKeys();
  } else {
    toast('生成失败', 'error');
  }
}

/* ============================================================
   6. 页面 4：接口测试（聊天）
   ============================================================ */

const FALLBACK_MODEL = { id: 'qwen3.7-plus', family: '默认', display_name: 'qwen3.7-plus', capabilities: {} };

function renderTest() {
  const v = viewEl();
  v.appendChild(el('div', { class: 'page-head' }, [
    el('div', null, [
      el('h1', null, '接口测试'),
      el('p', null, '直连 /v1/chat/completions 验证模型与流式输出'),
    ]),
  ]));

  // 聊天状态（闭包内维护）
  const state = {
    messages: [],     // {role, content, reasoning}
    streaming: true,
    thinking: false,  // 思考模式
    sending: false,
  };

  const wrap = el('div', { class: 'chat-wrap' });

  // 工具栏
  const modelSel = el('select', { class: 'select', id: 'chatModel' }, [el('option', null, '加载中…')]);
  const streamSwitch = el('label', { class: 'switch' }, [
    el('input', { type: 'checkbox', id: 'chatStream', checked: 'checked' }),
    el('span', null, '流式输出'),
  ]);
  const seg = el('div', { class: 'seg' }, [
    el('button', { id: 'modeFast', class: 'on', onclick: () => setMode(false) }, '快速'),
    el('button', { id: 'modeThink', onclick: () => setMode(true) }, '思考'),
  ]);
  const newBtn = el('button', { class: 'btn btn-sm', onclick: () => { state.messages = []; renderChat(); } }, '🆕 新建对话');

  const toolbar = el('div', { class: 'chat-toolbar' }, [modelSel, streamSwitch, seg, newBtn]);

  const chatBody = el('div', { class: 'chat-body', id: 'chatBody' });
  const input = el('textarea', { class: 'textarea', id: 'chatInput', rows: '1', placeholder: '输入消息，Enter 发送，Shift+Enter 换行…' });
  const sendBtn = el('button', { class: 'btn btn-primary', id: 'chatSend', onclick: send }, '发送');
  const inputBar = el('div', { class: 'chat-input-bar' }, [input, sendBtn]);

  wrap.appendChild(toolbar);
  wrap.appendChild(chatBody);
  wrap.appendChild(inputBar);
  v.appendChild(wrap);

  streamSwitch.querySelector('input').addEventListener('change', (e) => { state.streaming = e.target.checked; });

  function setMode(think) {
    state.thinking = think;
    document.getElementById('modeFast').classList.toggle('on', !think);
    document.getElementById('modeThink').classList.toggle('on', think);
  }

  // Enter 发送
  input.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); send(); }
  });

  // 渲染整个聊天历史
  function renderChat() {
    chatBody.innerHTML = '';
    if (!state.messages.length) {
      chatBody.appendChild(el('div', { class: 'empty' }, [el('span', { class: 'big' }, '💬'), '开始一段对话吧']));
      return;
    }
    state.messages.forEach(m => chatBody.appendChild(renderMsg(m)));
    chatBody.scrollTop = chatBody.scrollHeight;
  }

  // 加载模型列表
  (async function loadModels() {
    const r = await api('/v1/models');
    modelSel.innerHTML = '';
    let models = (r.ok && r.data && r.data.data) ? r.data.data : null;
    if (!models || !models.length) {
      models = [FALLBACK_MODEL];
      if (!r.ok) toast('模型列表获取失败，已使用默认模型', 'info');
    }
    // 按 family 分组
    const groups = {};
    models.forEach(m => {
      const fam = m.family || '其他';
      (groups[fam] = groups[fam] || []).push(m);
    });
    Object.keys(groups).forEach(fam => {
      const og = el('optgroup', { label: fam });
      groups[fam].forEach(m => {
        const caps = capsLabel(m.capabilities);
        og.appendChild(el('option', { value: m.id }, (m.display_name || m.id) + (caps ? ' · ' + caps : '')));
      });
      modelSel.appendChild(og);
    });
  })();

  // 能力标签简写
  function capsLabel(caps) {
    if (!caps) return '';
    const map = { thinking: '思考', search: '搜索', vision: '视觉', deep_research: '深研', image_gen: '图', video_gen: '视频', web_dev: 'Web', slides: 'PPT' };
    return Object.keys(map).filter(k => caps[k]).map(k => map[k]).join('/');
  }

  // 发送消息
  async function send() {
    if (state.sending) return;
    const text = input.value.trim();
    if (!text) return;
    const model = modelSel.value || FALLBACK_MODEL.id;

    state.messages.push({ role: 'user', content: text });
    input.value = '';
    const assistantMsg = { role: 'assistant', content: '', reasoning: '', t0: performance.now(), ttft: 0, total: 0 };
    state.messages.push(assistantMsg);
    renderChat();
    const bubbleEl = chatBody.lastElementChild;

    state.sending = true;
    sendBtn.disabled = true; sendBtn.textContent = '生成中…';

    const body = {
      model,
      // 留所有 user + 有 content 的 assistant；剛 push 的空 assistant placeholder 因無 content 自然被濾掉，
      // 不可再 .slice(0,-1)（會把當輪的 user 訊息一起砍掉，導致 AI 永遠晚一輪回答）。
      messages: state.messages
        .filter(m => m.role === 'user' || (m.role === 'assistant' && m.content))
        .map(m => ({ role: m.role, content: m.content })),
      stream: state.streaming,
      include_reasoning: state.thinking,
      enable_thinking: state.thinking,
    };

    try {
      if (state.streaming) {
        await streamChat(body, assistantMsg, bubbleEl);
      } else {
        await nonStreamChat(body, assistantMsg, bubbleEl);
      }
    } catch (err) {
      assistantMsg.error = String(err && err.message || err);
      updateBubble(bubbleEl, assistantMsg);
    } finally {
      state.sending = false;
      sendBtn.disabled = false; sendBtn.textContent = '发送';
      // 完成耗時；若沒有首 token 計時（如非流式），首 token 視同完成
      assistantMsg.total = (performance.now() - assistantMsg.t0) / 1000;
      if (!assistantMsg.ttft) assistantMsg.ttft = assistantMsg.total;
      updateBubble(bubbleEl, assistantMsg);
      chatBody.scrollTop = chatBody.scrollHeight;
    }
  }

  // 非流式
  async function nonStreamChat(body, msg, bubbleEl) {
    const resp = await fetch('/v1/chat/completions', {
      method: 'POST', headers: authHeaders({ 'Content-Type': 'application/json' }), body: JSON.stringify(body),
    });
    if (!resp.ok) {
      const t = await resp.text();
      msg.error = `请求失败 (${resp.status}): ${t.slice(0, 300)}`;
      updateBubble(bubbleEl, msg); return;
    }
    const data = await resp.json();
    const m = data.choices && data.choices[0] && data.choices[0].message;
    if (m) {
      msg.content = normalizeContent(m.content);
      msg.reasoning = m.reasoning_content || m.reasoning || '';
    } else {
      msg.error = '响应无内容';
    }
    updateBubble(bubbleEl, msg);
  }

  // 流式（手动解析 SSE）
  async function streamChat(body, msg, bubbleEl) {
    const resp = await fetch('/v1/chat/completions', {
      method: 'POST', headers: authHeaders({ 'Content-Type': 'application/json' }), body: JSON.stringify(body),
    });
    if (!resp.ok || !resp.body) {
      const t = resp.body ? await resp.text() : '';
      msg.error = `请求失败 (${resp.status})${t ? ': ' + t.slice(0, 300) : ''}`;
      updateBubble(bubbleEl, msg); return;
    }
    const reader = resp.body.getReader();
    const decoder = new TextDecoder('utf-8');
    let buf = '';
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });
      // 以双换行分割 SSE 事件
      let idx;
      while ((idx = buf.indexOf('\n\n')) !== -1) {
        const raw = buf.slice(0, idx);
        buf = buf.slice(idx + 2);
        const line = raw.split('\n').find(l => l.startsWith('data:'));
        if (!line) continue;
        const payload = line.slice(5).trim();
        if (payload === '[DONE]') return;
        try {
          const j = JSON.parse(payload);
          const delta = j.choices && j.choices[0] && j.choices[0].delta;
          if (delta) {
            if ((delta.content || delta.reasoning_content) && !msg.ttft) {
              msg.ttft = (performance.now() - msg.t0) / 1000; // 首 token 耗時
            }
            if (delta.content) msg.content += delta.content;
            if (delta.reasoning_content) msg.reasoning += delta.reasoning_content;
          }
          // 後端錯誤幀（SSE 內嵌 error）
          if (j.error) { msg.error = (j.error.message || JSON.stringify(j.error)); }
        } catch (e) { /* 忽略非 JSON 行 */ }
        updateBubble(bubbleEl, msg, true);
        chatBody.scrollTop = chatBody.scrollHeight;
      }
    }
  }

  // 初次渲染
  renderChat();
}

/** content 可能是字符串或数组（多模态），归一为字符串 */
function normalizeContent(content) {
  if (typeof content === 'string') return content;
  if (Array.isArray(content)) {
    return content.map(part => {
      if (typeof part === 'string') return part;
      if (part && part.type === 'text') return part.text || '';
      if (part && part.type === 'image_url') return (part.image_url && part.image_url.url) || '';
      return '';
    }).join('');
  }
  return content == null ? '' : String(content);
}

/** 渲染单条消息节点 */
function renderMsg(m) {
  const isUser = m.role === 'user';
  const node = el('div', { class: 'msg ' + m.role });
  node.appendChild(el('div', { class: 'avatar' }, isUser ? '🙂' : '🤖'));
  const inner = el('div', null);
  fillMsgInner(inner, m);
  node.appendChild(inner);
  return node;
}

/** 更新 assistant 气泡（流式时频繁调用） */
function updateBubble(msgNode, m, typing) {
  // msgNode 是 .msg 容器
  const inner = msgNode.children[1];
  if (!inner) return;
  fillMsgInner(inner, m, typing);
}

function fillMsgInner(inner, m, typing) {
  inner.innerHTML = '';
  // 思考过程折叠块
  if (m.reasoning) {
    const det = el('details', { class: 'reasoning' });
    det.appendChild(el('summary', null, '💭 思考过程'));
    det.appendChild(el('div', { class: 'reasoning-body' }, m.reasoning));
    inner.appendChild(det);
  }
  const bubble = el('div', { class: 'bubble' + (m.error ? ' error' : '') + (typing ? ' cursor' : '') });
  if (m.error) {
    bubble.textContent = m.error;
  } else {
    bubble.innerHTML = renderRichText(m.content || '');
  }
  inner.appendChild(bubble);
  // 耗時資訊：首 token / 完成（僅 assistant 且有計時）
  if (m.role === 'assistant' && (m.ttft || m.total)) {
    const parts = [];
    if (m.ttft) parts.push(`首 token ${m.ttft.toFixed(2)}s`);
    if (m.total) parts.push(`完成 ${m.total.toFixed(2)}s`);
    inner.appendChild(el('div', { class: 'msg-timing' }, '⏱ ' + parts.join(' · ')));
  }
}

/**
 * 渲染富文本：转义后把 markdown 图片 / 裸图片 URL 转成 <img>。
 * 仅处理图片，不做完整 markdown，以保持轻量与安全。
 */
function renderRichText(text) {
  let html = escapeHtml(text);
  // markdown 图片：![alt](url)
  html = html.replace(/!\[[^\]]*\]\((https?:\/\/[^\s)]+)\)/g, (_, url) => mediaTag(url));
  // 裸 URL（图片 / 视频）：含图片后缀、视频后缀、或 Qwen 生成路径 /t2i/ /t2v/
  html = html.replace(/(?<!["'(=])(https?:\/\/[^\s<]+(?:\.(?:png|jpe?g|gif|webp|bmp|mp4|webm|mov)(?:\?[^\s<]*)?|\/t2[iv]\/[^\s<]*))/gi,
    (m) => mediaTag(m));
  return html;
}

/** 依 URL 判斷圖片或影片，產生對應標籤 */
function mediaTag(url) {
  const low = url.toLowerCase();
  const isVideo = /\.(mp4|webm|mov)(\?|$)/.test(low) || low.includes('/t2v/');
  if (isVideo) {
    return `<video src="${url}" controls preload="metadata" style="max-width:100%;border-radius:10px"></video>`;
  }
  return `<img src="${url}" alt="image" loading="lazy">`;
}

/* ============================================================
   7. 页面 5/6：图片 / 影片生成（共用任务队列 + 画廊）
   ============================================================ */

const ASPECT_RATIOS = [
  { ratio: '1:1', w: 1328, h: 1328 },
  { ratio: '16:9', w: 1664, h: 928 },
  { ratio: '9:16', w: 928, h: 1664 },
  { ratio: '4:3', w: 1472, h: 1140 },
  { ratio: '3:4', w: 1140, h: 1472 },
];
const VIDEO_RATIOS_NEW = [
  { ratio: '16:9', label: '横屏' },
  { ratio: '9:16', label: '竖屏' },
  { ratio: '1:1', label: '方形' },
];

/** 媒体页：任务队列 + 画廊（图片/影片共用） */
function renderMediaPage(kind) {
  const isVideo = kind === 'video';
  const v = viewEl();
  const ratios = isVideo ? VIDEO_RATIOS_NEW : ASPECT_RATIOS;
  const state = { ratio: ratios[0].ratio };

  v.appendChild(el('div', { class: 'page-head' }, [
    el('div', null, [
      el('h1', null, isVideo ? '影片生成' : '图片生成'),
      el('p', null, isVideo
        ? '任务队列异步生成；自动重试 + 智能跳过无 t2v 权限的账号；本地永久保存'
        : '支持多提示词批次提交，后台逐一生成并永久保存到本地画廊'),
    ]),
  ]));

  // 控件：prompt（textarea，每行一个提示词）+ 比例 + 数量（仅图片）
  const promptInput = el('textarea', { class: 'textarea', rows: '3',
    placeholder: isVideo ? '描述你想生成的影片画面…' : '描述你想生成的画面（每行一个提示词，可批次提交）…' });

  const ratioGrid = el('div', { class: 'ratio-grid' }, ratios.map(r =>
    el('button', {
      class: 'ratio-opt' + (r.ratio === state.ratio ? ' on' : ''),
      'data-ratio': r.ratio,
      onclick: () => {
        state.ratio = r.ratio;
        ratioGrid.querySelectorAll('.ratio-opt').forEach(b => b.classList.toggle('on', b.getAttribute('data-ratio') === r.ratio));
      },
    }, [document.createTextNode(r.ratio), el('small', null, isVideo ? r.label : `${r.w}×${r.h}`)])
  ));

  // 仅图片：数量
  let nState = { n: 1 };
  let countGrid = null;
  if (!isVideo) {
    countGrid = el('div', { class: 'ratio-grid' }, [1, 2, 4].map(n =>
      el('button', {
        class: 'count-opt' + (n === nState.n ? ' on' : ''),
        'data-n': String(n),
        onclick: () => {
          nState.n = n;
          countGrid.querySelectorAll('.count-opt').forEach(b => b.classList.toggle('on', +b.getAttribute('data-n') === n));
        },
      }, n + ' 张/提示词')
    ));
  }

  const submitBtn = el('button', { class: 'btn btn-primary', onclick: submit }, isVideo ? '🎬 加入任务队列' : '🎨 加入任务队列');

  const submitCard = el('div', { class: 'card' }, [
    el('div', { class: 'card-title' }, '提交任务'),
    el('div', { class: 'img-controls' }, [
      el('div', { class: 'field' }, [
        el('label', null, isVideo ? '提示词' : '提示词（多行＝批次）'),
        promptInput,
      ]),
      el('div', { class: 'field' }, [el('label', null, '比例'), ratioGrid]),
      ...(isVideo ? [
        el('div', { class: 'notice notice-warn' }, [
          el('span', { class: 'ico' }, 'ℹ️'),
          el('span', null, '影片功能受 Qwen 帐号权限硬性限制：多数帐号回应空白；系统会自动重试 + 记住无权限帐号下次自动跳过。'),
        ]),
      ] : [
        el('div', { class: 'field' }, [el('label', null, '每个提示词张数'), countGrid]),
      ]),
      submitBtn,
    ]),
  ]);
  v.appendChild(submitCard);

  // 画廊（任务列表）
  const galleryHost = el('div', { id: 'mediaGallery' }, [loadingNode()]);
  v.appendChild(el('div', { class: 'card' }, [
    el('div', { class: 'card-title' }, [
      el('span', null, isVideo ? '影片画廊' : '图片画廊'),
      el('button', { class: 'btn btn-sm', style: 'float:right', onclick: refresh }, '🔄 刷新'),
    ]),
    galleryHost,
  ]));

  async function submit() {
    const raw = promptInput.value.trim();
    if (!raw) { toast('请输入提示词', 'error'); return; }
    const prompts = isVideo
      ? [raw]
      : raw.split('\n').map(s => s.trim()).filter(Boolean);
    const body = isVideo
      ? { kind: 'video', prompt: prompts[0], ratio: state.ratio }
      : (() => {
          const r = ASPECT_RATIOS.find(x => x.ratio === state.ratio);
          return {
            kind: 'image',
            prompts,
            ratio: state.ratio,
            size: `${r.w}x${r.h}`,
            width: r.w, height: r.h,
            n: nState.n,
          };
        })();
    submitBtn.disabled = true; const old = submitBtn.textContent; submitBtn.textContent = '提交中…';
    const resp = await api('/api/admin/media/tasks', { method: 'POST', body });
    submitBtn.disabled = false; submitBtn.textContent = old;
    if (resp.ok && resp.data && resp.data.ok) {
      toast(`已提交 ${resp.data.count} 个任务，后台生成中…`, 'success');
      promptInput.value = '';
      refresh();
    } else {
      const err = (resp.data && resp.data.error) || `请求失败 (${resp.status})`;
      toast('提交失败：' + err, 'error');
    }
  }

  async function refresh() {
    const r = await api(`/api/admin/media/tasks?kind=${kind}&limit=80`);
    if (!r.ok) {
      setConn(false);
      galleryHost.innerHTML = '';
      galleryHost.appendChild(el('div', { class: 'empty' }, '加载失败，请检查会话 Key'));
      return;
    }
    setConn(true);
    renderGallery(galleryHost, (r.data && r.data.tasks) || [], isVideo);
  }

  refresh();
  // 有 queued/running 时每 3s 刷新；全部 done/failed 时每 30s 刷新（节流）
  let timer = setInterval(() => {
    const hasActive = !!document.querySelector('.media-task.queued, .media-task.running');
    // 简单策略：定时调用 refresh，但 hasActive=false 时跳过部分轮次
    if (hasActive || (Date.now() % 30000) < 3000) refresh();
  }, 3000);
  cleanupFn = () => clearInterval(timer);
}

/** 渲染媒体画廊（任务卡片） */
function renderGallery(host, tasks, isVideo) {
  host.innerHTML = '';
  if (!tasks.length) {
    host.appendChild(el('div', { class: 'empty' }, [
      el('span', { class: 'big' }, isVideo ? '🎬' : '🖼️'),
      isVideo ? '尚无影片任务' : '尚无图片任务',
    ]));
    return;
  }
  const grid = el('div', { class: 'media-grid' });
  tasks.forEach(t => grid.appendChild(renderTaskCard(t, isVideo)));
  host.appendChild(grid);
}

function renderTaskCard(t, isVideo) {
  const card = el('div', { class: 'media-task ' + t.status });
  // 顶部：状态徽章 + 时间
  const STATUS_CN = { queued: '排队中', running: '生成中…', done: '完成', failed: '失败' };
  const STATUS_CLS = { queued: 'badge-gray', running: 'badge-orange', done: 'badge-green', failed: 'badge-red' };
  const dt = new Date(t.ts_created || Date.now());
  const dur = (t.ts_done && t.ts_created) ? ((t.ts_done - t.ts_created) / 1000).toFixed(1) + 's' : '';
  card.appendChild(el('div', { class: 'task-head' }, [
    el('span', { class: 'badge ' + (STATUS_CLS[t.status] || 'badge-gray') }, STATUS_CN[t.status] || t.status),
    el('span', { class: 'task-time' }, fmtClock(t.ts_created) + (dur ? ` · ${dur}` : '')),
    t.attempts > 1 ? el('span', { class: 'task-meta' }, `🔁 ${t.attempts} 次`) : null,
  ]));
  // 媒体内容（done 才有）
  if (t.status === 'done' && t.results && t.results.length) {
    const media = el('div', { class: 'task-media' });
    t.results.forEach((r, i) => {
      // 优先用本地 URL（永久），回退 CDN
      const src = r.local_url || r.cdn_url;
      const dl = r.local_url || r.cdn_url;
      if (!src) return;
      const inner = el('div', { class: 'media-cell' });
      if (isVideo) {
        inner.appendChild(el('video', { src, controls: 'controls', preload: 'metadata' }));
      } else {
        const a = el('a', { href: src, target: '_blank' }, [
          el('img', { src, alt: t.prompt, loading: 'lazy' }),
        ]);
        inner.appendChild(a);
      }
      const ext = isVideo ? 'mp4' : 'png';
      inner.appendChild(el('div', { class: 'media-actions' }, [
        el('a', { class: 'btn btn-sm', href: dl, download: `qwen-${isVideo ? 'video' : 'image'}-${t.id}-${i + 1}.${ext}`, target: '_blank' }, '下载'),
        r.local_url ? el('span', { class: 'tag-local' }, '✅ 本地永久') : el('span', { class: 'tag-remote' }, '⚠️ 仅 CDN'),
      ]));
      media.appendChild(inner);
    });
    card.appendChild(media);
  } else if (t.status === 'running' || t.status === 'queued') {
    card.appendChild(el('div', { class: 'task-loading' }, [
      el('div', { class: 'spinner' }),
      t.status === 'queued' ? '排队中…' : '正在生成…（图片约 30s / 影片更慢）',
    ]));
  } else if (t.status === 'failed') {
    card.appendChild(el('div', { class: 'task-err' }, t.error || '生成失败'));
  }
  // 提示词
  card.appendChild(el('div', { class: 'task-prompt' }, t.prompt));
  return card;
}

function renderImages() { return renderMediaPage('image'); }
function renderVideos() { return renderMediaPage('video'); }


/* ============================================================
   8. 页面 6：系统设置
   ============================================================ */

function renderSettings() {
  const v = viewEl();
  v.appendChild(el('div', { class: 'page-head' }, [
    el('div', null, [
      el('h1', null, '系统设置'),
      el('p', null, '会话密钥、并发参数、预热池与模型映射'),
    ]),
  ]));

  // --- 当前会话 Key ---
  const keyInput = el('input', { class: 'input', id: 'sessKey', type: 'password', value: getKey() });
  v.appendChild(el('div', { class: 'card' }, [
    el('div', { class: 'card-title' }, '当前会话 Key'),
    el('div', { class: 'field' }, [
      el('label', null, '用于管理接口认证（Authorization: Bearer）'),
      keyInput,
      el('div', { class: 'hint' }, '保存在浏览器 localStorage，默认 admin'),
    ]),
    el('div', { class: 'head-actions' }, [
      el('button', {
        class: 'btn btn-primary', onclick: () => {
          setKey(keyInput.value.trim());
          toast('会话 Key 已保存', 'success');
          loadSettings();
        },
      }, '保存'),
      el('button', {
        class: 'btn', onclick: () => {
          setKey('');
          keyInput.value = 'admin';
          toast('已清除，恢复默认 admin', 'info');
        },
      }, '清除'),
    ]),
  ]));

  // --- 连接信息 ---
  v.appendChild(el('div', { class: 'card' }, [
    el('div', { class: 'card-title' }, '连接信息'),
    el('div', { class: 'kv' }, [el('span', { class: 'k' }, 'Base URL'), el('span', { class: 'v' }, location.origin)]),
    el('div', { class: 'kv' }, [el('span', { class: 'k' }, '版本'), el('span', { class: 'v', id: 'setVersion' }, '—')]),
  ]));

  // --- 核心并发参数 ---
  const maxInflight = el('input', { class: 'input', id: 'setMaxInflight', type: 'number', min: '1', max: '10' });
  const globalMax = el('input', { class: 'input', id: 'setGlobalMax', type: 'number', min: '0' });
  const minInterval = el('input', { class: 'input', id: 'setMinInterval', type: 'number', min: '0', step: '500' });
  v.appendChild(el('div', { class: 'card' }, [
    el('div', { class: 'card-title' }, '核心并发参数'),
    el('div', { class: 'form-row' }, [
      el('div', { class: 'field' }, [el('label', null, '单账号最大并发 (1-10)'), maxInflight]),
      el('div', { class: 'field' }, [el('label', null, '全局并发上限 (0=不限)'), globalMax]),
    ]),
    el('div', { class: 'form-row' }, [
      el('div', { class: 'field' }, [
        el('label', null, '同账号最小间隔 / 风控休息 (毫秒, 0=不限)'),
        minInterval,
        el('div', { class: 'field-hint' }, '同一账号两次请求之间的强制休息，从上次请求结束起算，避免单账号被打太快而封号。'),
      ]),
    ]),
    el('button', {
      class: 'btn btn-primary', id: 'btnSaveConcurrency', onclick: async () => {
        const body = {
          max_inflight_per_account: parseInt(maxInflight.value, 10),
          global_max_inflight: parseInt(globalMax.value, 10),
          account_min_interval_ms: parseInt(minInterval.value, 10) || 0,
        };
        await saveSettings(body, '并发/风控参数已保存（即时生效）');
      },
    }, '保存并发参数'),
  ]));

  // --- 出口全局代理（风控） ---
  const proxyInput = el('input', { class: 'input', id: 'setUpstreamProxy', type: 'text', placeholder: 'http://user:pass@host:port（留空=用环境变量/不走代理）' });
  v.appendChild(el('div', { class: 'card' }, [
    el('div', { class: 'card-title' }, '出口全局代理'),
    el('div', { class: 'field' }, [
      el('label', null, '上游出口代理 (HTTP/HTTPS/SOCKS)'),
      proxyInput,
      el('div', { class: 'field-hint' }, '所有对 Qwen 上游与 OSS 的请求都经此代理出口（风控：轮换出口 IP，避免账号被封）。即时生效并持久化；留空则回退环境变量 HTTP(S)_PROXY 或不走代理。'),
    ]),
    el('div', { class: 'form-row' }, [
      el('button', {
        class: 'btn btn-primary', onclick: async () => {
          await saveSettings({ upstream_proxy: proxyInput.value.trim() }, '出口代理已保存（即时生效）');
        },
      }, '保存代理'),
      el('button', {
        class: 'btn', onclick: async () => {
          proxyInput.value = '';
          await saveSettings({ upstream_proxy: '' }, '出口代理已清除');
        },
      }, '清除代理'),
    ]),
  ]));

  // --- Chat_ID 预热池 ---
  const poolTarget = el('input', { class: 'input', id: 'setPoolTarget', type: 'number', min: '0' });
  const poolTtl = el('input', { class: 'input', id: 'setPoolTtl', type: 'number', min: '0' });
  v.appendChild(el('div', { class: 'card' }, [
    el('div', { class: 'card-title' }, 'Chat_ID 预热池'),
    el('div', { class: 'form-row' }, [
      el('div', { class: 'field' }, [el('label', null, '每账号目标数'), poolTarget]),
      el('div', { class: 'field' }, [el('label', null, 'TTL（分钟）'), poolTtl]),
    ]),
    el('button', {
      class: 'btn btn-primary', onclick: async () => {
        const body = {
          chat_id_pool_target: parseInt(poolTarget.value, 10),
          chat_id_pool_ttl_seconds: parseInt(poolTtl.value, 10) * 60, // 分钟 -> 秒
        };
        await saveSettings(body, '预热池参数已保存');
      },
    }, '保存预热池参数'),
  ]));

  // --- 模型映射规则 ---
  const aliasArea = el('textarea', { class: 'textarea', id: 'setAliases', rows: '8', placeholder: '{ }' });
  v.appendChild(el('div', { class: 'card' }, [
    el('div', { class: 'card-title' }, '模型映射规则'),
    el('div', { class: 'field' }, [
      el('label', null, 'model_aliases（JSON）'),
      aliasArea,
      el('div', { class: 'hint' }, '键为对外模型名，值为上游实际模型；编辑后点保存' )
    ]),
    el('button', {
      class: 'btn btn-primary', onclick: async () => {
        let obj;
        try { obj = JSON.parse(aliasArea.value); }
        catch (e) { toast('JSON 格式错误：' + e.message, 'error'); return; }
        await saveSettings({ model_aliases: obj }, '模型映射已保存');
      },
    }, '保存模型映射'),
  ]));

  // --- 使用示例 ---
  v.appendChild(buildExamplesCard());

  loadSettings();
}

async function loadSettings() {
  const r = await api('/api/admin/settings');
  if (!r.ok) { setConn(false); toast('设置加载失败，请检查会话 Key', 'error'); return; }
  setConn(true);
  const d = r.data || {};
  const set = (id, val) => { const e = document.getElementById(id); if (e) e.value = val; };
  const setTxt = (id, val) => { const e = document.getElementById(id); if (e) e.textContent = val; };
  setTxt('setVersion', d.version || '—');
  set('setMaxInflight', d.max_inflight_per_account ?? 3);
  set('setGlobalMax', d.global_max_inflight ?? 0);
  set('setMinInterval', d.account_min_interval_ms ?? 0);
  set('setUpstreamProxy', d.upstream_proxy ?? '');
  set('setPoolTarget', d.chat_id_pool_target ?? 0);
  set('setPoolTtl', Math.round((d.chat_id_pool_ttl_seconds ?? 0) / 60)); // 秒 -> 分钟
  set('setAliases', JSON.stringify(d.model_aliases || {}, null, 2));
}

async function saveSettings(body, okMsg) {
  const r = await api('/api/admin/settings', { method: 'PUT', body });
  if (r.ok) {
    toast(okMsg || '已保存', 'success');
    loadSettings();
  } else {
    const err = (r.data && r.data.error) || `请求失败 (${r.status})`;
    toast('保存失败：' + err, 'error');
  }
}

function buildExamplesCard() {
  const origin = location.origin;
  const key = '<YOUR_API_KEY>';
  const openai = `curl ${origin}/v1/chat/completions \\
  -H "Authorization: Bearer ${key}" \\
  -H "Content-Type: application/json" \\
  -d '{"model":"qwen3.7-plus","messages":[{"role":"user","content":"你好"}]}'`;
  const anthropic = `curl ${origin}/v1/messages \\
  -H "Authorization: Bearer ${key}" \\
  -H "Content-Type: application/json" \\
  -d '{"model":"qwen3.7-plus","max_tokens":1024,"messages":[{"role":"user","content":"你好"}]}'`;
  const gemini = `curl "${origin}/v1beta/models/qwen3.7-plus:generateContent" \\
  -H "Authorization: Bearer ${key}" \\
  -H "Content-Type: application/json" \\
  -d '{"contents":[{"parts":[{"text":"你好"}]}]}'`;
  const images = `curl ${origin}/v1/images/generations \\
  -H "Authorization: Bearer ${key}" \\
  -H "Content-Type: application/json" \\
  -d '{"model":"dall-e-3","prompt":"一只赛博朋克猫","n":1,"size":"1328x1328"}'`;

  const card = el('div', { class: 'card example-block' }, [el('div', { class: 'card-title' }, '使用示例')]);
  [['OpenAI', openai], ['Anthropic', anthropic], ['Gemini', gemini], ['Images', images]].forEach(([title, code]) => {
    card.appendChild(el('h4', null, title));
    card.appendChild(el('div', { class: 'code' }, code));
  });
  return card;
}

/* ============================================================
   9. 通用 DOM helper：加载节点 / 表格
   ============================================================ */

function loadingNode() {
  return el('div', { class: 'loading' }, [el('div', { class: 'spinner' }), '加载中…']);
}

/** 用表头数组 + 已生成的 tbody HTML 构造表格（含横向滚动包裹） */
function elTable(headers, bodyHtml) {
  const thead = '<thead><tr>' + headers.map(h => `<th>${escapeHtml(h)}</th>`).join('') + '</tr></thead>';
  const wrap = el('div', { class: 'table-wrap' });
  wrap.innerHTML = `<table>${thead}<tbody>${bodyHtml}</tbody></table>`;
  return wrap;
}

/* ============================================================
   10. 启动 / 移动端侧边栏交互
   ============================================================ */

function bindMobileNav() {
  const toggle = document.getElementById('menuToggle');
  const overlay = document.getElementById('overlay');
  if (toggle) toggle.addEventListener('click', () => document.body.classList.toggle('nav-open'));
  if (overlay) overlay.addEventListener('click', () => document.body.classList.remove('nav-open'));
}

document.addEventListener('DOMContentLoaded', () => {
  bindMobileNav();
  router();
});
