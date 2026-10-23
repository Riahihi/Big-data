// js cho trang web, dữ liệu lấy từ các api trong web/app.py
"use strict";

// ===== tiện ích chung =====
const $ = (s) => document.querySelector(s);
const css = (v) => getComputedStyle(document.documentElement).getPropertyValue(v).trim();
const nf = (d = 0) => new Intl.NumberFormat("vi-VN", { minimumFractionDigits: d, maximumFractionDigits: d });
const fmt = (v, d = 0) => (v === null || v === undefined || Number.isNaN(v) ? "–" : nf(d).format(v));
const MONTHS = ["T1", "T2", "T3", "T4", "T5", "T6", "T7", "T8", "T9", "T10", "T11", "T12"];
const SERIES = () => [1, 2, 3, 4, 5, 6, 7, 8].map((i) => css(`--series-${i}`));
const SEQ = () => [1, 2, 3, 4, 5].map((i) => css(`--seq-${i}`));
const WARM = () => [1, 2, 3, 4, 5].map((i) => css(`--warm-${i}`));
const DIV = () => ["--div-neg", "--div-neg-2", "--div-mid", "--div-pos-2", "--div-pos"].map(css);

async function api(path, body) {
  const r = await fetch(path, body ? { method: "POST", headers: { "Content-Type": "application/json" },
                                       body: JSON.stringify(body) } : undefined);
  const j = await r.json();
  if (!r.ok) throw new Error(j.error || r.statusText);
  return j;
}

// lưu lại hàm vẽ của từng biểu đồ để đổi sáng/tối thì vẽ lại
const charts = {};
function draw(id, builder) {
  const el = document.getElementById(id);
  if (!el) return;
  charts[id] = charts[id] || { inst: echarts.init(el, null, { renderer: "canvas" }) };
  charts[id].builder = builder;
  charts[id].inst.setOption(builder(), true);
}
function redrawAll() { Object.values(charts).forEach((c) => c.inst.setOption(c.builder(), true)); }
window.addEventListener("resize", () => Object.values(charts).forEach((c) => c.inst.resize()));

// option chung cho các biểu đồ
function base(extra = {}) {
  const ink2 = css("--text-secondary"), grid = css("--border");
  const axis = { axisLine: { lineStyle: { color: grid } }, axisTick: { show: false },
                 axisLabel: { color: ink2, fontSize: 11 }, splitLine: { lineStyle: { color: grid, width: 0.6 } },
                 nameTextStyle: { color: ink2, fontSize: 11 } };
  return {
    backgroundColor: "transparent", animationDuration: 300,
    textStyle: { color: ink2, fontFamily: "system-ui, sans-serif" },
    grid: { left: 48, right: 16, top: 52, bottom: 36, containLabel: true },
    tooltip: { trigger: "axis", backgroundColor: css("--surface-1"), borderColor: grid,
               textStyle: { color: css("--text-primary"), fontSize: 12 }, confine: true },
    legend: { top: 0, left: 0, textStyle: { color: ink2, fontSize: 11 }, itemWidth: 14, itemHeight: 8, icon: "roundRect" },
    _axis: axis, ...extra,
  };
}
const ax = (o, extra = {}) => ({ ...o._axis, ...extra });

function tableHTML(rows, cols) {
  const th = cols.map((c) => `<th class="${c.num ? "num" : ""}">${c.label}</th>`).join("");
  const tr = rows.map((r) => "<tr>" + cols.map((c) => {
    const v = c.f ? c.f(r) : r[c.k];
    return `<td class="${c.num ? "num" : ""}">${v ?? "–"}</td>`;
  }).join("") + "</tr>").join("");
  return `<table class="data"><thead><tr>${th}</tr></thead><tbody>${tr}</tbody></table>`;
}

// ===== trạng thái =====
const S = {
  ov: null, adv: null, LOC: {}, order: [], core: [],
  scope: { q1: "core", q2: "core", q3: "core" },
  selected: [], slot: {},             // địa điểm đang chọn & ô màu cố định của từng địa điểm
  q: {},                               // cache kết quả câu hỏi theo "q1:core" …
  rlSel: ["HUE", "DNG", "HNI", "HCM"], rlSlot: {},
};
const label = (id) => { const l = S.LOC[id]; return !l ? id : (l.site_name === l.location_name ? l.location_name : `${l.location_name} (${l.site_name})`); };
const shortLabel = (id) => S.LOC[id] ? S.LOC[id].location_name : id;
const inScope = (scope) => (scope === "core" ? S.order.filter((id) => S.core.includes(id)) : S.order);

// mỗi địa điểm giữ 1 màu cố định khi chọn / bỏ chọn
function toggleSel(list, slots, id, max) {
  const i = list.indexOf(id);
  if (i >= 0) { list.splice(i, 1); delete slots[id]; return; }
  if (list.length >= max) return;
  const used = new Set(Object.values(slots));
  slots[id] = [0, 1, 2, 3, 4, 5, 6, 7].find((k) => !used.has(k));
  list.push(id);
}
function renderChips(el, ids, list, slots, onChange, max) {
  const colors = SERIES();
  el.innerHTML = ids.map((id) => {
    const on = list.includes(id);
    return `<button class="chip" type="button" data-id="${id}" aria-pressed="${on}">
      <span class="dot" style="background:${on ? colors[slots[id]] : "var(--border)"}"></span>${shortLabel(id)}</button>`;
  }).join("");
  el.querySelectorAll(".chip").forEach((b) => b.addEventListener("click", () => {
    toggleSel(list, slots, b.dataset.id, max); onChange();
  }));
}

// ===== tab & giao diện =====
const loaded = {};
document.querySelectorAll("nav.tabs button").forEach((b) => b.addEventListener("click", () => openTab(b.dataset.tab)));
function openTab(t) {
  document.querySelectorAll("nav.tabs button").forEach((b) => b.setAttribute("aria-selected", b.dataset.tab === t));
  document.querySelectorAll("section.tab").forEach((s) => s.classList.toggle("active", s.id === `tab-${t}`));
  try { localStorage.setItem("tab", t); } catch (e) { /* không có localStorage */ }
  if (!loaded[t]) { loaded[t] = true; ({ q1: initQ1, q2: initQ2, q3: initQ3, adv: initAdv, verify: initVerify, sql: initSql }[t] || (() => {}))(); }
  setTimeout(() => Object.values(charts).forEach((c) => c.inst.resize()), 0);
}
const THEMES = ["auto", "light", "dark"], THEME_LABEL = { auto: "tự động", light: "sáng", dark: "tối" };
function applyTheme(t) {
  if (t === "auto") delete document.documentElement.dataset.theme; else document.documentElement.dataset.theme = t;
  $("#themeBtn").textContent = `Chế độ: ${THEME_LABEL[t]}`;
  try { localStorage.setItem("theme", t); } catch (e) { /* bỏ qua */ }
  redrawAll();
  if (loaded.q1) renderQ1Chips(); if (loaded.q2) renderQ2Chips(); if (loaded.adv) renderRlChips();
}
$("#themeBtn").addEventListener("click", () => {
  const cur = document.documentElement.dataset.theme || "auto";
  applyTheme(THEMES[(THEMES.indexOf(cur) + 1) % 3]);
});
matchMedia("(prefers-color-scheme: dark)").addEventListener("change", redrawAll);

document.querySelectorAll(".seg[data-scope-for]").forEach((seg) => seg.querySelectorAll("button").forEach((b) =>
  b.addEventListener("click", () => {
    const q = seg.dataset.scopeFor;
    seg.querySelectorAll("button").forEach((x) => x.setAttribute("aria-pressed", x === b));
    S.scope[q] = b.dataset.scope;
    ({ q1: loadQ1, q2: loadQ2, q3: loadQ3 })[q]();
  })));

async function getQ(q, scope) {
  const k = `${q}:${scope}`;
  if (!S.q[k]) S.q[k] = await api(`/api/q/${q}?scope=${scope}`);
  return S.q[k];
}
async function getAdv() { if (!S.adv) S.adv = await api("/api/advanced"); return S.adv; }

// ===== tổng quan =====
async function initOverview() {
  const o = (S.ov = await api("/api/overview"));
  o.locations.forEach((l) => (S.LOC[l.location_id] = l));
  S.order = [...o.locations].sort((a, b) => b.latitude - a.latitude).map((l) => l.location_id);
  S.core = o.core_ids;
  S.selected = []; S.slot = {};
  ["HNI", "DNG", "HCM", "LDG"].forEach((id) => toggleSel(S.selected, S.slot, id, 8));
  S.rlSel.forEach((id) => (S.rlSlot[id] = Object.keys(S.rlSlot).length));

  const cs = o.clean_stats || {}, v = o.verification || {}, run = (o.pipeline_run || {}).steps || {};
  const nPass = Object.values(v).filter((x) => x && x.passed === true).length;
  const nChecks = Object.values(v).filter((x) => x && typeof x === "object" && !x.skipped).length;
  const procTime = ["clean", "analysis", "advanced", "verify"].reduce((s, k) => s + (run[k] ? run[k].elapsed_s : 0), 0);
  const [y0, y1] = o.scope.full;
  $("#ovLead").textContent = `Dữ liệu thời tiết theo giờ của ${o.locations.length} tỉnh/thành từ ${y0} đến ${y1}, lấy từ Open-Meteo. ` +
    `Ngoài phạm vi theo đề (10 địa điểm, ${o.scope.core[0]}-${o.scope.core[1]}), nhóm lấy thêm dữ liệu để mở rộng.`;
  const kpi = (l, val, h) => `<div class="kpi"><div class="label">${l}</div><div class="value">${val}</div><div class="hint">${h}</div></div>`;
  $("#kpis").innerHTML = [
    kpi("Số bản ghi", fmt(cs.clean_rows), "dữ liệu theo giờ"),
    kpi("Tỉnh/thành", o.locations.length, `${S.core.length} điểm theo đề`),
    kpi("Số năm", y1 - y0 + 1, `${y0} - ${y1}`),
    kpi("Số biến", o.variables.length, "nhiệt độ, mưa, độ ẩm, gió..."),
    kpi("Kiểm tra", `${nPass}/${nChecks}`, v.all_passed ? "đều đạt" : "xem tab kiểm tra"),
    kpi("Thời gian xử lý", procTime ? `${fmt(procTime, 0)} s` : "-", "từ làm sạch đến kiểm tra"),
  ].join("");

  const t = (k) => (run[k] ? (run[k].elapsed_s >= 600 ? `${fmt(run[k].elapsed_s / 60, 0)} phút` : `${fmt(run[k].elapsed_s, 1)} s`) : "");
  const steps = [
    ["Chọn nguồn dữ liệu", `${o.locations.length} điểm, ${o.variables.length} biến`, ""],
    ["Thu thập", `gọi API ${o.manifest.files} lần`, t("ingest")],
    ["Lưu dữ liệu gốc", `file json.gz (${fmt(o.manifest.bytes_gz / 1e6, 0)} MB) và manifest`, ""],
    ["Làm sạch", "kiểu dữ liệu, giờ thiếu, dòng trùng, giá trị lỗi", t("clean")],
    ["Phân tích", "3 câu hỏi và phần làm thêm", run.analysis && run.advanced ? `${fmt(run.analysis.elapsed_s + run.advanced.elapsed_s, 1)} s` : ""],
    ["Lưu kết quả", "file parquet", ""],
    ["Kiểm tra", "tính lại bằng pandas", t("verify")],
    ["Vẽ biểu đồ", "matplotlib và trang web", t("figures")],
  ];
  $("#flow").innerHTML = steps.map((s, i) => `<div class="step"><div class="n">Bước ${i + 1}</div><div class="t">${s[0]}</div>
     <div class="d">${s[1]}</div>${s[2] ? `<div class="time">${s[2]}</div>` : ""}</div>`).join("");

  const m = o.manifest;
  $("#sourceInfo").innerHTML = tableHTML([
    { k: "API", v: `<code>${o.api.replace("https://", "")}</code>` },
    { k: "Mô hình", v: `${o.model.toUpperCase()} (dữ liệu tái phân tích, ô lưới khoảng 28 km, không phải số đo tại trạm)` },
    { k: "Múi giờ", v: `${o.timezone} (UTC+7)` },
    { k: "File gốc", v: `${m.files} file, ${fmt(m.records)} bản ghi` },
    { k: "Dung lượng", v: `${fmt(m.bytes_raw / 1e6, 0)} MB (nén còn ${fmt(m.bytes_gz / 1e6, 0)} MB)` },
    { k: "Ngày tải", v: m.first_fetch.slice(0, 10).split("-").reverse().join("/") },
  ], [{ k: "k", label: "Mục" }, { k: "v", label: "Giá trị" }]);
  $("#qualityInfo").innerHTML = tableHTML([
    { k: "Số bản ghi đọc vào", v: fmt(cs.raw_rows) },
    { k: "Dòng trùng", v: fmt(cs.duplicates_removed) },
    { k: "Giờ bị thiếu", v: fmt(cs.missing_rows) },
    { k: "Giá trị ngoài ngưỡng", v: fmt(cs.out_of_range_total) },
    { k: "Giờ được nội suy", v: fmt(cs.rows_imputed) },
    { k: "Đọc lại parquet đủ dòng", v: cs.parquet_roundtrip_ok ? '<span class="badge pass">✓ Đạt</span>' : '<span class="badge fail">✗</span>' },
  ], [{ k: "k", label: "Kiểm tra" }, { k: "v", label: "Số lượng", num: true }]) +
    `<p class="note">Dữ liệu tải về đầy đủ, không bị thiếu hay trùng. Các bước xử lý thiếu/trùng được test riêng bằng dữ liệu giả.</p>`;

  await getAdv();
  $("#mapMetric").addEventListener("change", drawMap);
  drawMap();
}

function locAgg() {
  // tính TB các chỉ số theo địa điểm để tô màu
  const a = S.adv, out = {};
  a.a1_climate_indices.forEach((r) => {
    const o = (out[r.location_id] = out[r.location_id] || { t: [], p: [], su: [], rx: [] });
    o.t.push(r.annual_mean_temp); o.p.push(r.annual_precip_mm); o.su.push(r.SU35); o.rx.push(r.Rx1day);
  });
  const mean = (x) => x.reduce((s, v) => s + v, 0) / x.length;
  const tr = Object.fromEntries(a.a5_temperature_trends.map((r) => [r.location_id, r.trend_c_per_decade_monthly]));
  return Object.fromEntries(Object.entries(out).map(([id, o]) => [id, {
    temp: mean(o.t), rain: mean(o.p), su35: mean(o.su), rx1: Math.max(...o.rx), trend: tr[id] }]));
}

function drawMap() {
  const metric = $("#mapMetric").value, agg = locAgg();
  const spec = {
    temp: { name: "Nhiệt độ TB năm", unit: "°C", d: 1, colors: WARM() },
    rain: { name: "Tổng mưa năm", unit: "mm", d: 0, colors: SEQ() },
    su35: { name: "Ngày nắng nóng/năm", unit: "ngày", d: 1, colors: WARM() },
    trend: { name: "Xu hướng", unit: "°C/10 năm", d: 2, colors: DIV(), diverging: true },
    rx1: { name: "Mưa ngày lớn nhất", unit: "mm", d: 0, colors: SEQ() },
  }[metric];
  draw("mapChart", () => {
    const o = base();
    const data = S.ov.locations.map((l) => [l.longitude, l.latitude, agg[l.location_id] ? agg[l.location_id][metric] : null, l.location_id]);
    const vals = data.map((d) => d[2]).filter((v) => v !== null);
    let lo = Math.min(...vals), hi = Math.max(...vals);
    if (spec.diverging) { const m = Math.max(Math.abs(lo), Math.abs(hi)); lo = -m; hi = m; }
    return {
      ...o,
      grid: { left: 40, right: 20, top: 10, bottom: 70, containLabel: true },
      tooltip: { ...o.tooltip, trigger: "item", formatter: (p) => {
        if (p.seriesIndex === 1) return p.data[2];
        const l = S.LOC[p.data[3]];
        return `<b>${label(l.location_id)}</b><br>${l.region}<br>${spec.name}: <b>${fmt(p.data[2], spec.d)} ${spec.unit}</b>` +
               `<br>Tọa độ: ${fmt(l.latitude, 2)}°N, ${fmt(l.longitude, 2)}°E · cao ${fmt(l.elevation_m)} m`; } },
      xAxis: ax(o, { type: "value", min: 102, max: 116, name: "Kinh độ", nameLocation: "middle", nameGap: 24,
                     axisLabel: { ...o._axis.axisLabel, formatter: "{value}°E" } }),
      yAxis: ax(o, { type: "value", min: 8, max: 23.6, axisLabel: { ...o._axis.axisLabel, formatter: "{value}°N" } }),
      visualMap: { min: lo, max: hi, dimension: 2, seriesIndex: 0, orient: "horizontal", left: "center", bottom: 0,
                   calculable: true, itemWidth: 12, itemHeight: 200, formatter: (x) => `${fmt(x, spec.d)} ${spec.unit}`,
                   textStyle: { color: css("--text-secondary"), fontSize: 11 }, inRange: { color: spec.colors } },
      series: [
        { type: "scatter", data, symbolSize: 16, itemStyle: { borderColor: css("--surface-1"), borderWidth: 2 },
          label: { show: true, position: "right", formatter: (p) => S.LOC[p.data[3]].site_name, fontSize: 10,
                   color: css("--text-secondary") }, labelLayout: { hideOverlap: true },
          emphasis: { scale: 1.4 } },
        // 2 quần đảo, chỉ ghi tên
        { type: "scatter", silent: false, symbol: "circle", symbolSize: 5, itemStyle: { color: css("--text-muted") },
          data: [[111.9, 16.5, "Quần đảo Hoàng Sa (Đà Nẵng) – không thu thập"], [114.2, 10.0, "Quần đảo Trường Sa (Khánh Hòa) – không thu thập"]],
          label: { show: true, position: "bottom", formatter: (p) => p.data[2].split(" (")[0], fontSize: 10, color: css("--text-muted") } },
      ],
    };
  });
}

// ===== câu 1 =====
function initQ1() { loadQ1(); }
function renderQ1Chips() {
  const ids = inScope(S.scope.q1);
  renderChips($("#q1Chips"), ids, S.selected, S.slot, () => { renderQ1Chips(); drawQ1Line(); }, 8);
}
async function loadQ1() {
  const scope = S.scope.q1;
  // chuyển về phạm vi theo đề thì bỏ các tỉnh không thuộc 10 điểm
  S.selected.filter((id) => !inScope(scope).includes(id)).forEach((id) => toggleSel(S.selected, S.slot, id, 8));
  if (!S.selected.length) inScope(scope).slice(0, 3).forEach((id) => toggleSel(S.selected, S.slot, id, 8));
  renderQ1Chips();
  $("#q1Csv").href = `/download/q1_monthly_temp_${scope}.csv`;
  await getQ("q1", scope);
  drawQ1Line(); drawQ1Heat();
}
function drawQ1Line() {
  const scope = S.scope.q1, d = S.q[`q1:${scope}`][`q1_monthly_temp_${scope}`];
  $("#q1Hint").textContent = S.selected.length === 1 ? "Phần tô mờ là khoảng nhiệt độ thấp nhất - cao nhất trung bình trong ngày." : "Chọn tối đa 8 địa điểm. Chọn 1 địa điểm sẽ thấy thêm khoảng min - max.";
  draw("q1Line", () => {
    const o = base(), colors = SERIES();
    const months = [...new Set(d.map((r) => r.year_month))].sort();
    const series = [];
    S.selected.forEach((id) => {
      const rows = Object.fromEntries(d.filter((r) => r.location_id === id).map((r) => [r.year_month, r]));
      const c = colors[S.slot[id]];
      if (S.selected.length === 1) {
        series.push({ name: "_min", type: "line", stack: "band", data: months.map((m) => rows[m]?.avg_daily_min_c), symbol: "none",
                      lineStyle: { opacity: 0 }, tooltip: { show: false } });
        series.push({ name: "_band", type: "line", stack: "band", symbol: "none", lineStyle: { opacity: 0 },
                      areaStyle: { color: c, opacity: 0.15 }, tooltip: { show: false },
                      data: months.map((m) => rows[m] ? +(rows[m].avg_daily_max_c - rows[m].avg_daily_min_c).toFixed(2) : null) });
      }
      series.push({ name: shortLabel(id), type: "line", data: months.map((m) => rows[m]?.avg_temp_c), showSymbol: false,
                    lineStyle: { width: 2, color: c }, itemStyle: { color: c }, emphasis: { focus: "series" },
                    endLabel: { show: S.selected.length <= 4, formatter: "{a}", color: css("--text-secondary"), fontSize: 11 } });
    });
    return { ...o,
      legend: { ...o.legend, data: S.selected.map(shortLabel) },
      grid: { ...o.grid, right: S.selected.length <= 4 ? 110 : 16 },
      tooltip: { ...o.tooltip, valueFormatter: (v) => `${fmt(v, 2)} °C` },
      xAxis: ax(o, { type: "category", data: months, boundaryGap: false }),
      yAxis: ax(o, { type: "value", name: "°C", scale: true }),
      dataZoom: [{ type: "inside" }], series };
  });
}
function climHeat(id, rows, valueKey, colors, unit, d = 1, scope) {
  const ids = inScope(scope).filter((x) => rows.some((r) => r.location_id === x));
  const data = rows.map((r) => [r.month - 1, ids.indexOf(r.location_id), r[valueKey]]).filter((x) => x[1] >= 0);
  const vals = data.map((x) => x[2]);
  draw(id, () => { const o = base(); return { ...o,
    grid: { left: 24, right: 16, top: 8, bottom: 60, containLabel: true },
    tooltip: { ...o.tooltip, trigger: "item", formatter: (p) => `${label(ids[p.data[1]])} · ${MONTHS[p.data[0]]}<br><b>${fmt(p.data[2], d)} ${unit}</b>` },
    xAxis: ax(o, { type: "category", data: MONTHS, splitLine: { show: false } }),
    yAxis: ax(o, { type: "category", data: ids.map(label), inverse: true, splitLine: { show: false } }),
    visualMap: { min: Math.min(...vals), max: Math.max(...vals), orient: "horizontal", left: "center", bottom: 0, calculable: true,
                 itemHeight: 220, itemWidth: 12, textStyle: { color: css("--text-secondary"), fontSize: 11 }, inRange: { color: colors() },
                 formatter: (x) => `${fmt(x, d)} ${unit}` },
    series: [{ type: "heatmap", data, itemStyle: { borderColor: css("--surface-1"), borderWidth: 1 },
               label: { show: ids.length <= 12, fontSize: 10, formatter: (p) => fmt(p.data[2], d === 0 ? 0 : 1) } }] }; });
}
function drawQ1Heat() {
  const scope = S.scope.q1, c = S.q[`q1:${scope}`][`q1_climatology_${scope}`];
  $("#q1Heat").style.height = scope === "full" ? "860px" : "460px";
  charts.q1Heat?.inst.resize();
  climHeat("q1Heat", c, "clim_temp_c", WARM, "°C", 1, scope);
  const ids = inScope(scope);
  $("#q1Table").innerHTML = tableHTML(ids.map((id) => {
    const r = { id }; c.filter((x) => x.location_id === id).forEach((x) => (r[x.month] = x.clim_temp_c)); return r; }),
    [{ label: "Địa điểm", f: (r) => label(r.id) }, ...MONTHS.map((m, i) => ({ label: m, num: true, f: (r) => fmt(r[i + 1], 1) }))]);
}

// ===== câu 2 =====
function initQ2() {
  $("#q2Focus").addEventListener("change", drawQ2Series);
  loadQ2();
}
function renderQ2Chips() {
  renderChips($("#q2Chips"), inScope(S.scope.q2), S.selected, S.slot, () => { renderQ2Chips(); drawQ2Compare(); }, 8);
}
async function loadQ2() {
  const scope = S.scope.q2;
  S.selected.filter((id) => !inScope(scope).includes(id)).forEach((id) => toggleSel(S.selected, S.slot, id, 8));
  if (!S.selected.length) inScope(scope).slice(0, 3).forEach((id) => toggleSel(S.selected, S.slot, id, 8));
  renderQ2Chips();
  $("#q2Csv").href = `/download/q2_monthly_precip_${scope}.csv`;
  const prev = $("#q2Focus").value;
  $("#q2Focus").innerHTML = inScope(scope).map((id) => `<option value="${id}">${label(id)}</option>`).join("");
  $("#q2Focus").value = inScope(scope).includes(prev) ? prev : (inScope(scope).includes("HUE") ? "HUE" : inScope(scope)[0]);
  await getQ("q2", scope);
  drawQ2Compare(); drawQ2Series(); drawQ2Heat();
}
function q2rows() { const s = S.scope.q2; return S.q[`q2:${s}`][`q2_monthly_precip_${s}`]; }
function drawQ2Compare() {
  const d = q2rows(), sel = S.selected.slice(0, 4);
  draw("q2Clim", () => { const o = base(), colors = SERIES(); return { ...o,
    tooltip: { ...o.tooltip, valueFormatter: (v) => `${fmt(v, 0)} mm` },
    legend: { ...o.legend, data: sel.map(shortLabel) },
    xAxis: ax(o, { type: "category", data: MONTHS }), yAxis: ax(o, { type: "value", name: "mm/tháng" }),
    series: sel.map((id) => ({ name: shortLabel(id), type: "bar", barGap: "10%", barMaxWidth: 14,
      itemStyle: { color: colors[S.slot[id]], borderRadius: [4, 4, 0, 0] },
      data: MONTHS.map((_, m) => { const r = d.filter((x) => x.location_id === id && x.month === m + 1);
                                   return r.length ? +(r.reduce((s, x) => s + x.total_precip_mm, 0) / r.length).toFixed(1) : null; }) })) }; });
  const years = [...new Set(d.map((r) => r.year))].sort();
  draw("q2Annual", () => { const o = base(), colors = SERIES(); return { ...o,
    tooltip: { ...o.tooltip, valueFormatter: (v) => `${fmt(v, 0)} mm` },
    legend: { ...o.legend, data: sel.map(shortLabel) },
    xAxis: ax(o, { type: "category", data: years }), yAxis: ax(o, { type: "value", name: "mm/năm" }),
    series: sel.map((id) => ({ name: shortLabel(id), type: "bar", barGap: "10%", barMaxWidth: 16,
      itemStyle: { color: colors[S.slot[id]], borderRadius: [4, 4, 0, 0] },
      data: years.map((y) => +d.filter((x) => x.location_id === id && x.year === y).reduce((s, x) => s + x.total_precip_mm, 0).toFixed(0)) })) }; });
}
function drawQ2Series() {
  const id = $("#q2Focus").value, d = q2rows().filter((r) => r.location_id === id).sort((a, b) => a.year_month.localeCompare(b.year_month));
  const incomplete = d.filter((r) => !r.is_complete_month).length;
  $("#q2SeriesHint").textContent = `${label(id)}, ${d.length} tháng${incomplete ? ` (${incomplete} tháng thiếu dữ liệu)` : ""}`;
  draw("q2Series", () => { const o = base(); return { ...o,
    tooltip: { ...o.tooltip, formatter: (ps) => { const r = d[ps[0].dataIndex];
      return `<b>${r.year_month}</b><br>Tổng mưa: <b>${fmt(r.total_precip_mm, 1)} mm</b><br>Ngày mưa ≥ 1 mm: ${r.rain_days_ge_1mm}<br>Mưa ngày lớn nhất: ${fmt(r.max_daily_precip_mm, 1)} mm`; } },
    xAxis: ax(o, { type: "category", data: d.map((r) => r.year_month) }), yAxis: ax(o, { type: "value", name: "mm/tháng" }),
    dataZoom: [{ type: "inside" }],
    series: [{ type: "bar", data: d.map((r) => r.total_precip_mm), barMaxWidth: 12,
               itemStyle: { color: css("--series-1"), borderRadius: [4, 4, 0, 0] } }] }; });
}
function drawQ2Heat() {
  const scope = S.scope.q2, d = q2rows(), agg = {};
  d.forEach((r) => { const k = `${r.location_id}|${r.month}`; (agg[k] = agg[k] || []).push(r.total_precip_mm); });
  const rows = Object.entries(agg).map(([k, v]) => { const [location_id, m] = k.split("|");
    return { location_id, month: +m, p: v.reduce((s, x) => s + x, 0) / v.length }; });
  $("#q2Heat").style.height = scope === "full" ? "860px" : "460px";
  charts.q2Heat?.inst.resize();
  climHeat("q2Heat", rows, "p", SEQ, "mm", 0, scope);
  $("#q2Table").innerHTML = tableHTML(inScope(scope).map((id) => {
    const r = { id }; rows.filter((x) => x.location_id === id).forEach((x) => (r[x.month] = x.p)); return r; }),
    [{ label: "Địa điểm", f: (r) => label(r.id) }, ...MONTHS.map((m, i) => ({ label: m, num: true, f: (r) => fmt(r[i + 1], 0) })),
     { label: "Cả năm", num: true, f: (r) => fmt(MONTHS.reduce((s, _, i) => s + (r[i + 1] || 0), 0), 0) }]);
}

// ===== câu 3 =====
function initQ3() {
  $("#trLoc").innerHTML += S.order.map((id) => `<option value="${id}">${label(id)}</option>`).join("");
  $("#trRun").addEventListener("click", runTopRain);
  loadQ3();
}
async function loadQ3() {
  const scope = S.scope.q3;
  $("#q3Csv").href = `/download/q3_top5_rain_days_${scope}.csv`;
  const d = (await getQ("q3", scope))[`q3_top5_rain_days_${scope}`];
  const max = Math.max(...d.map((r) => r.precip_mm));
  $("#q3Cards").innerHTML = inScope(scope).map((id) => {
    const rows = d.filter((r) => r.location_id === id).sort((a, b) => a.rank - b.rank || a.date.localeCompare(b.date));
    return `<div class="top5"><h4>${label(id)}</h4>${rows.map((r) => `<div class="row" title="${r.rain_class} · mưa giờ lớn nhất ${fmt(r.precip_max_1h, 1)} mm · ${r.wet_hours} giờ có mưa">
      <span class="r">#${r.rank}</span><span>${r.date.split("-").reverse().join("/")}</span>
      <span><div class="bar" style="width:${(100 * r.precip_mm / max).toFixed(1)}%"></div></span>
      <span class="v">${fmt(r.precip_mm, 1)}</span></div>${r.n_tied > 1 ? `<div class="tie">↳ đồng hạng với ${r.n_tied - 1} ngày khác</div>` : ""}`).join("")}</div>`;
  }).join("");
}
async function runTopRain() {
  const btn = $("#trRun"); btn.disabled = true; $("#trStatus").textContent = "Đang chạy...";
  try {
    const loc = $("#trLoc").value;
    const r = await api("/api/spark/top-rain", { locations: loc ? [loc] : [], start: $("#trStart").value, end: $("#trEnd").value, n: +$("#trN").value });
    $("#trStatus").textContent = `${r.n_rows} dòng, ${fmt(r.elapsed_s, 2)} s`;
    $("#trTable").innerHTML = tableHTML(r.rows, [
      { label: "Địa điểm", f: (x) => label(x.location_id) }, { k: "rank", label: "Hạng", num: true },
      { k: "date", label: "Ngày" }, { label: "Mưa ngày (mm)", num: true, f: (x) => fmt(x.precip_mm, 1) },
      { label: "Mưa giờ lớn nhất", num: true, f: (x) => fmt(x.precip_max_1h, 1) }, { k: "wet_hours", label: "Giờ có mưa", num: true },
      { label: "Nhiệt độ TB", num: true, f: (x) => fmt(x.t_mean, 1) }]);
    $("#trPlan").textContent = r.plan;
  } catch (e) { $("#trStatus").textContent = "Lỗi: " + e.message; }
  btn.disabled = false;
}

// ===== nâng cao =====
async function initAdv() {
  const a = await getAdv();
  const opts = S.order.map((id) => `<option value="${id}">${label(id)}</option>`).join("");
  ["#hwLoc", "#diLoc", "#ciLoc"].forEach((s) => ($(s).innerHTML = opts));
  $("#hwLoc").value = "HNI"; $("#diLoc").value = "HCM"; $("#ciLoc").value = "HUE";
  $("#hwLoc").addEventListener("change", drawHw);
  $("#diLoc").addEventListener("change", drawDiurnal); $("#diVar").addEventListener("change", drawDiurnal);
  $("#ciLoc").addEventListener("change", drawCi);

  // dị thường theo năm
  const an = a.a5_annual_anomaly, years = [...new Set(an.map((r) => r.year))].sort();
  const ids = S.order.filter((id) => an.some((r) => r.location_id === id));
  const lim = Math.max(...an.map((r) => Math.abs(r.anomaly_c)));
  draw("advStripes", () => { const o = base(); return { ...o,
    grid: { left: 24, right: 16, top: 8, bottom: 60, containLabel: true },
    tooltip: { ...o.tooltip, trigger: "item", formatter: (p) => `${label(ids[p.data[1]])} · ${years[p.data[0]]}<br>Dị thường: <b>${p.data[2] > 0 ? "+" : ""}${fmt(p.data[2], 2)} °C</b>` },
    xAxis: ax(o, { type: "category", data: years, splitLine: { show: false } }),
    yAxis: ax(o, { type: "category", data: ids.map(label), inverse: true, splitLine: { show: false } }),
    visualMap: { min: -lim, max: lim, orient: "horizontal", left: "center", bottom: 0, calculable: true, itemHeight: 220, itemWidth: 12,
                 formatter: (x) => `${x > 0 ? "+" : ""}${fmt(x, 2)} °C`, textStyle: { color: css("--text-secondary"), fontSize: 11 }, inRange: { color: DIV() } },
    series: [{ type: "heatmap", data: an.map((r) => [years.indexOf(r.year), ids.indexOf(r.location_id), r.anomaly_c]),
               itemStyle: { borderColor: css("--surface-1"), borderWidth: 1 } }] }; });

  // xu hướng nhiệt độ
  const tr = [...a.a5_temperature_trends].sort((x, y) => x.trend_c_per_decade_monthly - y.trend_c_per_decade_monthly);
  const nSig = tr.filter((r) => r.p_monthly < 0.05).length, nSigA = tr.filter((r) => r.p_annual < 0.05).length;
  $("#trendHint").textContent = `°C/10 năm (hồi quy tuyến tính). Chỉ ${nSig}/${tr.length} điểm có p < 0,05, dữ liệu 10 năm còn ngắn nên chưa kết luận được.`;
  const trendTip = (p) => { const r = tr[p.seriesType === "custom" ? p.data[0] : p.data[1]];
    return `<b>${label(r.location_id)}</b><br>Xu hướng: <b>${fmt(r.trend_c_per_decade_monthly, 2)} °C/10 năm</b><br>khoảng tin cậy 95%: ±${fmt(19.6 * r.se_monthly, 2)}<br>p (tháng) = ${fmt(r.p_monthly, 4)} · p (năm) = ${fmt(r.p_annual, 3)}`; };
  draw("advTrend", () => { const o = base();
    return { ...o, grid: { left: 8, right: 24, top: 28, bottom: 30, containLabel: true },
      tooltip: { ...o.tooltip, trigger: "item", formatter: trendTip },
      legend: { ...o.legend, data: ["p < 0,05", "không có ý nghĩa"] },
      xAxis: ax(o, { type: "value", name: "°C/10 năm", nameLocation: "middle", nameGap: 22 }),
      yAxis: ax(o, { type: "category", data: tr.map((r) => r.location_name), splitLine: { show: false } }),
      series: [
        // thanh khoảng tin cậy 95%
        { type: "custom", encode: { x: [1, 2], y: 0 }, z: 1,
          data: tr.map((r, i) => [i, r.trend_c_per_decade_monthly - 19.6 * r.se_monthly, r.trend_c_per_decade_monthly + 19.6 * r.se_monthly]),
          renderItem: (p, A) => { const lo = A.coord([A.value(1), A.value(0)]), hi = A.coord([A.value(2), A.value(0)]);
            return { type: "line", shape: { x1: lo[0], y1: lo[1], x2: hi[0], y2: hi[1] }, style: { stroke: css("--border"), lineWidth: 3, lineCap: "round" } }; } },
        // p < 0.05 thì tô đặc
        { name: "p < 0,05", type: "scatter", symbolSize: 10, z: 3,
          itemStyle: { color: css("--series-8"), borderColor: css("--surface-1"), borderWidth: 2 },
          data: tr.map((r, i) => [r.trend_c_per_decade_monthly, i]).filter((_, i) => tr[i].p_monthly < 0.05) },
        { name: "không có ý nghĩa", type: "scatter", symbolSize: 10, z: 3,
          itemStyle: { color: css("--surface-1"), borderColor: css("--text-secondary"), borderWidth: 1.5 },
          data: tr.map((r, i) => [r.trend_c_per_decade_monthly, i]).filter((_, i) => tr[i].p_monthly >= 0.05) },
      ] };
  });

  // cả nước
  const na = a.a5_national_annual_anomaly;
  draw("advNational", () => { const o = base(); return { ...o,
    tooltip: { ...o.tooltip, valueFormatter: (v) => `${v > 0 ? "+" : ""}${fmt(v, 2)} °C` },
    xAxis: ax(o, { type: "category", data: na.map((r) => r.year) }), yAxis: ax(o, { type: "value", name: "°C" }),
    series: [{ type: "bar", barMaxWidth: 28, data: na.map((r) => ({ value: r.anomaly_c,
      itemStyle: { color: r.anomaly_c >= 0 ? css("--div-pos") : css("--div-neg"), borderRadius: r.anomaly_c >= 0 ? [4, 4, 0, 0] : [0, 0, 4, 4] } })),
      label: { show: true, position: "top", fontSize: 10, color: css("--text-secondary"), formatter: (p) => (p.value > 0 ? "+" : "") + fmt(p.value, 2) } }] }; });

  // nắng nóng theo vùng
  const ci = a.a1_climate_indices, regions = [...new Set(ci.map((r) => r.region))];
  const yrs = [...new Set(ci.map((r) => r.year))].sort();
  const regMean = (rg, y) => { const v = ci.filter((r) => r.region === rg && r.year === y).map((r) => r.SU35); return +(v.reduce((s, x) => s + x, 0) / v.length).toFixed(1); };
  const regOrder = regions.map((rg) => [rg, yrs.reduce((s, y) => s + regMean(rg, y), 0)]).sort((p, q) => q[1] - p[1]).map((p) => p[0]);
  draw("advHot", () => { const o = base(), colors = SERIES(); return { ...o,
    legend: { ...o.legend, type: "scroll" },
    tooltip: { ...o.tooltip, valueFormatter: (v) => `${fmt(v, 1)} ngày` },
    xAxis: ax(o, { type: "category", data: yrs }), yAxis: ax(o, { type: "value", name: "ngày" }),
    series: regOrder.map((rg, i) => ({ name: rg, type: "line", data: yrs.map((y) => regMean(rg, y)), symbolSize: 6,
      lineStyle: { width: 2, color: colors[i] }, itemStyle: { color: colors[i] }, emphasis: { focus: "series" } })) }; });

  const hs = a.a2_heatwave_summary;
  $("#hwTable").innerHTML = tableHTML(S.order.map((id) => {
    const ab = hs.find((r) => r.location_id === id && r.kind.startsWith("Tuyệt")) || {}, re = hs.find((r) => r.location_id === id && r.kind.startsWith("Tương")) || {};
    return { id, ab, re }; }), [
    { label: "Địa điểm", f: (r) => label(r.id) }, { label: "P90 Tmax (°C)", num: true, f: (r) => fmt(r.re.tmax_p90 ?? r.ab.tmax_p90, 1) },
    { label: "Số đợt ≥35 °C", num: true, f: (r) => r.ab.n_events ?? 0 }, { label: "Dài nhất (ngày)", num: true, f: (r) => r.ab.longest_days ?? "–" },
    { label: "Tmax đỉnh (°C)", num: true, f: (r) => fmt(r.ab.peak_tmax ?? r.re.peak_tmax, 1) },
    { label: "Số đợt > P90", num: true, f: (r) => r.re.n_events ?? 0 }, { label: "Dài nhất > P90", num: true, f: (r) => r.re.longest_days ?? "–" }]);
  drawHw();

  renderRlChips(); drawRL();

  // cấp mưa (bỏ "không mưa" cho dễ nhìn)
  const rc = a.a4_rain_class_distribution;
  const classes = ["Mưa nhỏ (1–15 mm)", "Mưa vừa (16–50 mm)", "Mưa to (51–100 mm)", "Mưa rất to (>100 mm)"];
  draw("advClass", () => { const o = base(), seq = SEQ(); return { ...o,
    grid: { left: 8, right: 16, top: 30, bottom: 20, containLabel: true },
    tooltip: { ...o.tooltip, axisPointer: { type: "shadow" }, valueFormatter: (v) => `${fmt(v, 1)} ngày/năm` },
    legend: { ...o.legend, data: classes },
    xAxis: ax(o, { type: "value", name: "ngày/năm" }),
    yAxis: ax(o, { type: "category", data: S.order.map(shortLabel), inverse: true, splitLine: { show: false }, axisLabel: { ...o._axis.axisLabel, fontSize: 10 } }),
    series: classes.map((c, i) => ({ name: c, type: "bar", stack: "x", barWidth: "70%",
      itemStyle: { color: seq[i + 1], borderColor: css("--surface-1"), borderWidth: 1 },
      data: S.order.map((id) => (rc.find((r) => r.location_id === id && r.rain_class === c) || {}).days_per_year || 0) })) }; });

  // các đợt mưa lớn
  $("#evTable").innerHTML = tableHTML(a.a8_multiday_rain_events, [
    { k: "national_rank", label: "#", num: true }, { label: "Địa điểm", f: (r) => label(r.location_id) }, { k: "region", label: "Vùng" },
    { label: "Từ ngày", f: (r) => r.start_date.split("-").reverse().join("/") }, { label: "Đến ngày", f: (r) => r.end_date.split("-").reverse().join("/") },
    { label: "Tổng mưa 3 ngày (mm)", num: true, f: (r) => fmt(r.rain_3d_mm, 1) }]);

  drawDiurnal(); drawClusters(); drawCi();

  const dry = [...a.a3_dry_spells_top3].sort((x, y) => y.length_days - x.length_days);
  $("#dryTable").innerHTML = tableHTML(dry, [
    { label: "Địa điểm", f: (r) => label(r.location_id) }, { k: "rank", label: "Hạng (trong tỉnh)", num: true },
    { label: "Bắt đầu", f: (r) => r.start_date.split("-").reverse().join("/") }, { label: "Kết thúc", f: (r) => r.end_date.split("-").reverse().join("/") },
    { k: "length_days", label: "Số ngày", num: true }, { label: "Tổng mưa trong chuỗi (mm)", num: true, f: (r) => fmt(r.total_precip_mm, 1) }]);
}

async function drawHw() {
  const id = $("#hwLoc").value, ev = await api(`/api/heatwaves/${id}`);
  const kinds = ["Tuyệt đối: Tmax ≥ 35 °C", "Tương đối: Tmax > P90 địa phương"];
  const day = 86400000;
  draw("advHw", () => { const o = base(), colors = [css("--series-2"), css("--series-1")]; return { ...o,
    grid: { left: 8, right: 16, top: 10, bottom: 30, containLabel: true },
    tooltip: { ...o.tooltip, trigger: "item", formatter: (p) => { const e = ev[p.data[3]];
      return `<b>${e.kind}</b><br>${e.start_date.split("-").reverse().join("/")} → ${e.end_date.split("-").reverse().join("/")} (${e.length_days} ngày)<br>Tmax đỉnh ${fmt(e.peak_tmax, 1)} °C · TB ${fmt(e.mean_tmax, 1)} °C`; } },
    xAxis: ax(o, { type: "time", min: "2016-01-01", max: "2026-01-01" }),
    yAxis: ax(o, { type: "category", data: ["≥ 35 °C", "> P90"], splitLine: { show: false } }),
    series: [{ type: "custom", encode: { x: [1, 2], y: 0 },
      data: ev.map((e, i) => [kinds.indexOf(e.kind), Date.parse(e.start_date), Date.parse(e.end_date) + day, i]),
      renderItem: (p, A) => { const s = A.coord([A.value(1), A.value(0)]), e = A.coord([A.value(2), A.value(0)]), h = A.size([0, 1])[1] * 0.5;
        return { type: "rect", shape: { x: s[0], y: s[1] - h / 2, width: Math.max(e[0] - s[0], 2), height: h, r: 2 },
                 style: { fill: colors[A.value(0)] } }; } }] }; });
}

function renderRlChips() {
  renderChips($("#rlChips"), S.order, S.rlSel, S.rlSlot, () => { renderRlChips(); drawRL(); }, 4);
}
function drawRL() {
  const rl = S.adv.a7_return_levels;
  draw("advRL", () => { const o = base(), colors = SERIES(); return { ...o, grid: { ...o.grid, right: 100 },
    tooltip: { ...o.tooltip, valueFormatter: (v) => `${fmt(v, 0)} mm` },
    legend: { ...o.legend, data: S.rlSel.map(shortLabel) },
    xAxis: ax(o, { type: "log", logBase: 10, min: 2, max: 100, name: "Chu kỳ lặp lại (năm)", nameLocation: "middle", nameGap: 24 }),
    yAxis: ax(o, { type: "value", name: "mm/ngày" }),
    series: S.rlSel.map((id) => ({ name: shortLabel(id), type: "line", symbolSize: 7,
      lineStyle: { width: 2, color: colors[S.rlSlot[id]] }, itemStyle: { color: colors[S.rlSlot[id]] },
      endLabel: { show: true, formatter: "{a}", color: css("--text-secondary"), fontSize: 11 },
      data: rl.filter((r) => r.location_id === id).sort((p, q) => p.return_period_years - q.return_period_years).map((r) => [r.return_period_years, r.return_level_mm]) })) }; });
}

async function drawDiurnal() {
  const id = $("#diLoc").value, v = $("#diVar").value, d = await api(`/api/diurnal/${id}`);
  const isRain = v === "wet_hour_pct", unit = isRain ? "%" : "°C";
  $("#diHint").textContent = `${label(id)}: ${isRain ? "% số giờ có mưa" : "nhiệt độ TB"} theo từng giờ và từng tháng.`;
  const vals = d.map((r) => r[v]);
  draw("advDiurnal", () => { const o = base(); return { ...o, grid: { left: 24, right: 16, top: 8, bottom: 60, containLabel: true },
    tooltip: { ...o.tooltip, trigger: "item", formatter: (p) => `${MONTHS[p.data[0]]} · ${p.data[1]}:00<br><b>${fmt(p.data[2], 1)} ${unit}</b>` },
    xAxis: ax(o, { type: "category", data: MONTHS, splitLine: { show: false } }),
    yAxis: ax(o, { type: "category", data: [...Array(24).keys()].map((h) => `${h}h`), splitLine: { show: false } }),
    visualMap: { min: Math.min(...vals), max: Math.max(...vals), orient: "horizontal", left: "center", bottom: 0, calculable: true,
                 itemHeight: 220, itemWidth: 12, textStyle: { color: css("--text-secondary"), fontSize: 11 }, inRange: { color: isRain ? SEQ() : WARM() },
                 formatter: (x) => `${fmt(x, 1)} ${unit}` },
    series: [{ type: "heatmap", data: d.map((r) => [r.month - 1, r.hour, r[v]]), itemStyle: { borderColor: css("--surface-1"), borderWidth: 1 } }] }; });
}

function drawClusters() {
  const cl = S.adv.a9_climate_clusters, info = S.ov.cluster_info || {};
  const ks = [...new Set(cl.map((r) => r.cluster))].sort();
  // TB đặc trưng của từng cụm
  const desc = ks.map((k) => { const m = cl.filter((r) => r.cluster === k);
    const avg = (f) => m.reduce((s, r) => s + f(r), 0) / m.length;
    return { k, n: m.length, t1: avg((r) => r.temp_m01), t7: avg((r) => r.temp_m07), p: avg((r) => r.annual_precip_mm) }; });
  const sc = (info.k_scores || []).map((s) => `k=${s.k}: ${fmt(s.silhouette, 3)}`).join(" · ");
  $("#clHint").textContent = `Dùng nhiệt độ và lượng mưa từng tháng để chia nhóm, chọn k = ${info.best_k} theo silhouette.`;
  draw("advCluster", () => { const o = base(), colors = SERIES(); return { ...o,
    grid: { left: 8, right: 24, top: 56, bottom: 30, containLabel: true },
    legend: { ...o.legend, data: desc.map((c) => `Cụm ${c.k + 1}`) },
    tooltip: { ...o.tooltip, trigger: "item", formatter: (p) => { const r = cl.find((x) => x.location_id === p.data[2]); const c = desc[ks.indexOf(r.cluster)];
      return `<b>${label(r.location_id)}</b> · Cụm ${r.cluster + 1}<br>Nhiệt độ T1 ${fmt(r.temp_m01, 1)} °C · T7 ${fmt(r.temp_m07, 1)} °C<br>Mưa năm ${fmt(r.annual_precip_mm)} mm<br><span style="color:${css("--text-muted")}">TB cụm: T1 ${fmt(c.t1, 1)} °C, mưa ${fmt(c.p)} mm</span>`; } },
    xAxis: ax(o, { type: "value", min: 102, max: 110, name: "Kinh độ", nameLocation: "middle", nameGap: 22, axisLabel: { ...o._axis.axisLabel, formatter: "{value}°E" } }),
    yAxis: ax(o, { type: "value", min: 8.5, max: 23, axisLabel: { ...o._axis.axisLabel, formatter: "{value}°N" } }),
    series: desc.map((c) => ({ name: `Cụm ${c.k + 1}`, type: "scatter", symbolSize: 14,
      itemStyle: { color: colors[c.k], borderColor: css("--surface-1"), borderWidth: 2 },
      label: { show: true, position: "right", fontSize: 10, color: css("--text-secondary"), formatter: (p) => S.LOC[p.data[2]].site_name },
      labelLayout: { hideOverlap: true },
      data: cl.filter((r) => r.cluster === c.k).map((r) => [S.LOC[r.location_id].longitude, S.LOC[r.location_id].latitude, r.location_id]) })) }; });
}

function drawCi() {
  const id = $("#ciLoc").value, rows = S.adv.a1_climate_indices.filter((r) => r.location_id === id).sort((a, b) => a.year - b.year);
  $("#ciTable").innerHTML = tableHTML(rows, [
    { k: "year", label: "Năm" }, { label: "T TB (°C)", num: true, f: (r) => fmt(r.annual_mean_temp, 2) },
    { label: "TXx", num: true, f: (r) => fmt(r.TXx, 1) }, { label: "TNn", num: true, f: (r) => fmt(r.TNn, 1) },
    { k: "SU35", label: "SU35", num: true }, { label: "Mưa năm (mm)", num: true, f: (r) => fmt(r.annual_precip_mm, 0) },
    { k: "wet_days", label: "Ngày mưa", num: true }, { k: "R50mm", label: "R50mm", num: true },
    { label: "Rx1day", num: true, f: (r) => fmt(r.Rx1day, 1) }, { label: "Rx5day", num: true, f: (r) => fmt(r.Rx5day, 1) },
    { k: "CDD", label: "CDD", num: true }, { k: "CWD", label: "CWD", num: true }, { label: "SDII", num: true, f: (r) => fmt(r.SDII, 1) }]);
}

// ===== kiểm chứng & hiệu năng =====
function badge(v) {
  if (!v) return '<span class="badge skip">–</span>';
  if (v.skipped) return '<span class="badge skip">Bỏ qua</span>';
  return v.passed ? '<span class="badge pass">✓ Đạt</span>' : '<span class="badge fail">✗ Không đạt</span>';
}
function initVerify() {
  const v = S.ov.verification || {}, b = S.ov.benchmark;
  $("#verifyAll").innerHTML = v.all_passed ? '<span class="badge pass">✓ Đạt hết</span>' : '<span class="badge fail">Có kiểm tra chưa đạt</span>';
  const q1 = v.V1_q1_monthly_temp, q2 = v.V1_q2_monthly_precip, q3 = v.V1_q3_top5, d = v.V2_open_meteo_daily, m = v.V3_manual_sample, p = v.V4_parquet_roundtrip;
  const dp = d && d.precipitation_sum_vs_precip_mm, dt = d && d.temperature_2m_mean_vs_t_mean;
  const items = [
    [q1, "Câu 1 tính lại bằng pandas", q1 && `${q1.rows_spark} dòng khớp, lệch nhiều nhất ${fmt(q1.avg_temp_c.max_abs_diff, 3)} °C do làm tròn.`],
    [q2, "Câu 2 tính lại bằng pandas", q2 && `${q2.rows_spark} dòng khớp.`],
    [q3, "Câu 3 tính lại bằng pandas", q3 && `${q3.rows_spark} dòng khớp.`],
    [d, "So với dữ liệu ngày của Open-Meteo", d && (d.skipped ? d.skipped : `So ${fmt(d.n_days_compared)} ngày ở ${d.n_locations} điểm: lượng mưa khớp ${fmt(dp["pct_within_0.1"], 0)}%, nhiệt độ TB khớp ${fmt(dt["pct_within_0.1"], 0)}%. Lúc đầu nhóm cộng mưa từ 0h đến 23h nên chỉ khớp khoảng 79%, sau khi sửa lại cách tính ngày mưa thì khớp hết.`)],
    [m, "Cộng tay 1 ngày", m && `${label(m.location_id)} ngày ${m.date.split("-").reverse().join("/")}: cộng tay được ${fmt(m.manual_sum_mm, 1)} mm, Spark ra ${fmt(m.spark_value_mm, 1)} mm.`],
    [p, "Đọc lại file parquet", p && `Đọc lại được ${fmt(p.clean_rows)} dòng, đúng bằng số giờ cần có.`],
  ];
  $("#checks").innerHTML = items.map(([x, t, desc]) => `<div class="check">${badge(x)}<b>${t}</b><div class="desc">${desc || "chưa chạy"}</div></div>`).join("");
  if (m) {
    $("#manualHint").textContent = `${label(m.location_id)} ngày ${m.date.split("-").reverse().join("/")}, lấy 24 giá trị từ 01:00 đến 00:00 hôm sau, tổng ${fmt(m.manual_sum_mm, 1)} mm`;
    draw("manualChart", () => { const o = base(); return { ...o,
      tooltip: { ...o.tooltip, valueFormatter: (v2) => `${fmt(v2, 1)} mm` },
      xAxis: ax(o, { type: "category", data: m.hour_labels ? m.hour_labels.map((x) => x.slice(6)) : [...Array(24).keys()].map((h) => `${h}h`) }), yAxis: ax(o, { type: "value", name: "mm/giờ" }),
      series: [{ type: "bar", data: m.hourly_precip_mm, barMaxWidth: 18, itemStyle: { color: css("--series-1"), borderRadius: [4, 4, 0, 0] } }] }; });
  }
  if (!b) { $("#benchEnv").textContent = "Chưa có kết quả đo."; return; }
  const e = b.environment;
  $("#benchEnv").textContent = `Đo trên server của nhóm (dùng chung với người khác), Spark ${e.spark_version} chạy ${e.spark_master}, ${fmt(b.n_rows)} bản ghi. Load average lúc đo khoảng ${Math.round(e.load_average_1m_5m_15m[0])}.`;
  const labels = { raw_json_uncompressed: "JSON gốc", raw_json_gzip: "JSON gốc (gzip)", csv_uncompressed: "CSV", parquet_snappy_flat: "Parquet (snappy)", parquet_snappy_partitioned: "Parquet phân vùng" };
  const keys = Object.keys(labels).filter((k) => k in b.B1_storage_MB);
  draw("bStorage", () => { const o = base(); return { ...o, grid: { left: 24, right: 60, top: 10, bottom: 20, containLabel: true },
    tooltip: { ...o.tooltip, axisPointer: { type: "shadow" }, valueFormatter: (v2) => `${fmt(v2, 1)} MB` },
    xAxis: ax(o, { type: "value", name: "MB" }), yAxis: ax(o, { type: "category", data: keys.map((k) => labels[k]), inverse: true, splitLine: { show: false } }),
    series: [{ type: "bar", data: keys.map((k) => b.B1_storage_MB[k]), barMaxWidth: 18, itemStyle: { color: css("--series-1"), borderRadius: [0, 4, 4, 0] },
               label: { show: true, position: "right", color: css("--text-secondary"), fontSize: 11, formatter: (p2) => `${fmt(p2.value, 0)} MB` } }] }; });
  const f = b.B2_q1_seconds;
  draw("bFormat", () => { const o = base(); return { ...o, grid: { left: 24, right: 60, top: 10, bottom: 20, containLabel: true },
    tooltip: { ...o.tooltip, axisPointer: { type: "shadow" }, valueFormatter: (v2) => `${fmt(v2, 2)} s` },
    xAxis: ax(o, { type: "value", name: "giây" }), yAxis: ax(o, { type: "category", data: ["CSV", "Parquet"], inverse: true, splitLine: { show: false } }),
    series: [{ type: "bar", data: [f.csv[0], f.parquet[0]], barMaxWidth: 18, itemStyle: { color: css("--series-1"), borderRadius: [0, 4, 4, 0] },
               label: { show: true, position: "right", color: css("--text-secondary"), fontSize: 11, formatter: (p2) => `${fmt(p2.value, 2)} s` } }] }; });
  const pr = b.B3_partition_pruning;
  const pruneRows = [["partitioned", "Phân vùng location_id / year"], ["unpartitioned_shuffled", "Không phân vùng, dữ liệu xáo trộn"],
                     ["unpartitioned_sorted", "Không phân vùng, đã sắp xếp"]].filter(([k]) => pr[k]).map(([k, t]) => ({ t, ...pr[k] }));
  $("#bPrune").innerHTML = tableHTML(pruneRows,
    [{ k: "t", label: "Cách lưu" }, { label: "Thời gian (s)", num: true, f: (r) => fmt(r.seconds[0], 3) },
     { label: "File phải đọc", num: true, f: (r) => `${r.files_read} / ${r.files_total}` },
     { label: "MB trong các file đó", num: true, f: (r) => `${fmt(r.mb_in_files_read, 1)} / ${fmt(r.mb_total, 1)}` }]) +
    `<p class="note">Có phân vùng thì Spark chỉ đọc ${fmt(pr.partitioned.mb_in_files_read, 2)} MB thay vì ${fmt(pr.unpartitioned_shuffled.mb_in_files_read, 1)} MB, nhưng thời gian lại không nhanh hơn vì phải đọc danh sách ${pr.partitioned.files_total} file nhỏ. Với dữ liệu lớn hơn thì phân vùng mới có lợi rõ.</p>`;
  const sc = b.B4_thread_scaling_daily_agg, th = Object.keys(sc);
  draw("bScale", () => { const o = base(); return { ...o,
    tooltip: { ...o.tooltip, formatter: (ps) => `local[${th[ps[0].dataIndex]}]<br>${fmt(sc[th[ps[0].dataIndex]].seconds[0], 2)} s · tăng tốc ×${fmt(sc[th[ps[0].dataIndex]].speedup_vs_1, 2)}` },
    xAxis: ax(o, { type: "category", data: th.map((t) => `${t} luồng`) }), yAxis: ax(o, { type: "value", name: "giây" }),
    series: [{ type: "line", data: th.map((t) => sc[t].seconds[0]), symbolSize: 8, lineStyle: { width: 2, color: css("--series-1") }, itemStyle: { color: css("--series-1") },
               label: { show: true, position: "top", fontSize: 11, color: css("--text-secondary"), formatter: (p2) => `×${fmt(sc[th[p2.dataIndex]].speedup_vs_1, 1)}` } }] }; });
  const pd = b.B5_daily_agg_seconds;
  $("#bPandas").innerHTML = tableHTML([{ k: "pandas", s: pd.pandas_single_process }, { k: "Spark local[8]", s: pd.spark_local8 }],
    [{ k: "k", label: "Công cụ" }, { label: "Trung vị (s)", num: true, f: (r) => fmt(r.s[0], 3) }, { label: "Các lần đo (s)", f: (r) => r.s[1].map((x) => fmt(x, 2)).join(" · ") }]) +
    `<p class="note">${pd.note}</p>`;
}

// ===== spark sql lab =====
const EXAMPLES = [
  ["10 giờ mưa lớn nhất cả nước", "SELECT location_name, ts, precipitation, temperature_2m, wind_gusts_10m\nFROM hourly\nORDER BY precipitation DESC\nLIMIT 10"],
  ["Nhiệt độ TB theo vùng và năm", "SELECT region, year, ROUND(AVG(temperature_2m), 2) AS t_mean, COUNT(*) AS n_hours\nFROM hourly\nGROUP BY region, year\nORDER BY region, year"],
  ["Ngày nóng nhất mỗi tỉnh", "WITH r AS (\n  SELECT location_name, date, t_max,\n         ROW_NUMBER() OVER (PARTITION BY location_id ORDER BY t_max DESC) AS rn\n  FROM daily\n)\nSELECT location_name, date, t_max FROM r WHERE rn = 1 ORDER BY t_max DESC"],
  ["Mưa ở Huế năm 2020", "SELECT month, ROUND(SUM(precip_mm), 1) AS rain_mm, SUM(CASE WHEN precip_mm > 50 THEN 1 ELSE 0 END) AS heavy_days\nFROM daily\nWHERE location_id = 'HUE' AND year = 2020\nGROUP BY month ORDER BY month"],
  ["Mưa theo giờ trong ngày ở TP.HCM", "SELECT hour, ROUND(100 * AVG(CASE WHEN precipitation >= 0.1 THEN 1 ELSE 0 END), 1) AS pct_wet_hours\nFROM hourly\nWHERE location_id = 'HCM' AND month BETWEEN 5 AND 10\nGROUP BY hour ORDER BY hour"],
  ["Nhiệt độ cảm nhận cao nhất", "SELECT location_name, MAX(apparent_temperature) AS max_feels_like, MAX(temperature_2m) AS max_temp\nFROM hourly\nGROUP BY location_name\nORDER BY max_feels_like DESC LIMIT 10"],
];
function initSql() {
  $("#sqlExamples").innerHTML = EXAMPLES.map((e, i) => `<button class="btn ghost" type="button" data-i="${i}">${e[0]}</button>`).join("");
  $("#sqlExamples").querySelectorAll("button").forEach((b) => b.addEventListener("click", () => { $("#sqlText").value = EXAMPLES[b.dataset.i][1]; runSql(); }));
  $("#sqlText").value = EXAMPLES[0][1];
  $("#sqlRun").addEventListener("click", runSql);
  $("#sqlText").addEventListener("keydown", (e) => { if (e.key === "Enter" && (e.ctrlKey || e.metaKey)) runSql(); });
}
async function runSql() {
  const btn = $("#sqlRun"); btn.disabled = true; $("#sqlError").textContent = ""; $("#sqlStatus").textContent = "Đang chạy...";
  try {
    const r = await api("/api/spark/sql", { sql: $("#sqlText").value, limit: 500 });
    $("#sqlStatus").textContent = `${r.n_rows} dòng · ${fmt(r.elapsed_s, 2)} s`;
    $("#sqlTable").innerHTML = tableHTML(r.rows, r.columns.map((c) => ({ k: c, label: c, num: typeof (r.rows[0] || {})[c] === "number",
      f: (x) => (typeof x[c] === "number" ? fmt(x[c], Number.isInteger(x[c]) ? 0 : 2) : x[c]) })));
    $("#sqlPlan").textContent = r.plan;
  } catch (e) { $("#sqlError").textContent = e.message; $("#sqlStatus").textContent = ""; }
  btn.disabled = false;
}

// ===== khởi động =====
(async function main() {
  let theme = "auto", tab = "overview";
  try { theme = localStorage.getItem("theme") || "auto"; tab = localStorage.getItem("tab") || "overview"; } catch (e) { /* bỏ qua */ }
  if (theme !== "auto") document.documentElement.dataset.theme = theme;
  $("#themeBtn").textContent = `Chế độ: ${THEME_LABEL[theme]}`;
  try {
    await initOverview();
    loaded.overview = true;
    if (tab !== "overview") openTab(tab);
  } catch (e) {
    $("#ovLead").innerHTML = `<span class="error">Không tải được dữ liệu: ${e.message}</span>`;
  }
})();
