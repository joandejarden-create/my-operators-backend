/**
 * Single-brand economics profile — GET /api/fdd-intelligence/brands/:brandName/economics only (no secrets).
 */

import { emptyFddWorkspaceSummary, readFddKpiSessionSummary, renderFddWorkspaceKpiStrip } from "/js/fdd-workspace-kpi.js";

const API = "/api/fdd-intelligence";

const CATEGORY_ORDER = [
  "Franchise Fee",
  "Recurring Brand Fee",
  "Sales / Marketing / Loyalty / Reservation Program",
  "Required System / Technology Cost",
  "Training / Conference / Education",
  "Transfer / Renewal / Relicensing",
  "Termination / Default / Penalty",
  "Estimated Initial Investment",
  "Legal / Operational Obligation",
  "Optional Program",
  "Pass-Through / Third-Party Cost",
  "Other / Needs Review",
];

/** Default sort matches prior grouped-by-category ordering. */
let econSortColumn = "commercialCategory";
let econSortDirection = "asc";

let lastEconomics = [];
let lastMeta = { brandName: "", includeNeedsReview: false, fddYear: null, country: null };
/** @type {object[]} */
let registryDocuments = [];

/** @type {number | null} */
let econSearchDebounce = null;

/** Semantic preset (excludes category-dropdown presets). */
let econFilterPreset = null;

/** @type {Set<string>} */
const selectedEconRowIds = new Set();

/** @type {"comfortable" | "compact"} */
let econTableDensity = "comfortable";

const ECON_CSV_HEADERS = [
  "commercialCategory",
  "reviewStatus",
  "feeOrObligationName",
  "feeType",
  "amount",
  "basis",
  "normalizedCostBasis",
  "amountFormulaType",
  "basisConfidence",
  "basisNeedsReview",
  "percentageRate",
  "unitRate",
  "fixedAmount",
  "revenueBase",
  "calculationUnit",
  "formulaNotes",
  "rawCostBasisText",
  "frequency",
  "lifecyclePhase",
  "sourceItemNumber",
  "sourceItemTitle",
  "possibleDuplicate",
  "duplicateGroupKey",
  "needsLegalReview",
  "needsCommercialReview",
  "auditScore",
  "auditStatus",
  "autoApproveEligible",
  "auditIssues",
];

const ECON_PRESET_CATEGORY = {
  cat_franchise_fee: "Franchise Fee",
  cat_recurring_brand: "Recurring Brand Fee",
  cat_transfer: "Transfer / Renewal / Relicensing",
  cat_termination: "Termination / Default / Penalty",
  cat_other: "Other / Needs Review",
};

function normBrandKey(s) {
  return String(s || "")
    .trim()
    .toLowerCase();
}

function $(sel) {
  return document.querySelector(sel);
}

function escapeHtml(s) {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

/**
 * Pretty-print amounts: comma separators for thousands, preserves $ and surrounding text
 * (e.g. ranges, "plus $400 per year").
 */
function formatAmountDisplay(raw) {
  if (raw == null) return "";
  if (typeof raw === "number" && Number.isFinite(raw)) {
    return Number.isInteger(raw)
      ? raw.toLocaleString("en-US")
      : raw.toLocaleString("en-US", { maximumFractionDigits: 10 });
  }
  const s = String(raw).trim();
  if (!s) return "";
  if (!/\d/.test(s)) return s;

  return s.replace(/\$?\d[\d,]*(?:\.\d+)?/g, (chunk) => {
    const hadDollar = chunk.startsWith("$");
    const inner = hadDollar ? chunk.slice(1) : chunk;
    const plain = inner.replace(/,/g, "");
    if (!/^\d+(?:\.\d+)?$/.test(plain)) return chunk;
    const n = Number(plain);
    if (!Number.isFinite(n)) return chunk;
    const formatted = Number.isInteger(n)
      ? n.toLocaleString("en-US")
      : n.toLocaleString("en-US", { maximumFractionDigits: 10 });
    return hadDollar ? `$${formatted}` : formatted;
  });
}

function csvEscape(s) {
  const t = String(s ?? "");
  if (/[",\n\r]/.test(t)) return `"${t.replace(/"/g, '""')}"`;
  return t;
}

function bucketCategory(cat) {
  const c = String(cat || "").trim();
  return CATEGORY_ORDER.includes(c) ? c : "Other / Needs Review";
}

function reviewPill(status) {
  let s = status;
  if (s && typeof s === "object" && !Array.isArray(s) && typeof s.name === "string") s = s.name;
  if (Array.isArray(s) && s.length) s = s[0];
  s = String(s ?? "")
    .replace(/\u00a0/g, " ")
    .trim();
  const lower = s.toLowerCase();
  if (lower === "approved") return '<span class="pill approved">Approved</span>';
  if (lower === "needs review" || lower === "need review") return '<span class="pill needs">Needs Review</span>';
  return `<span class="pill">${escapeHtml(s || "—")}</span>`;
}

function ynPill(on) {
  return on ? '<span class="pill yes">Yes</span>' : '<span class="pill no">No</span>';
}

function sourceItemCell(r) {
  const num = escapeHtml(r.sourceItemNumber || "—");
  const t = String(r.sourceItemTitle || "");
  const short = t.length > 36 ? t.slice(0, 36) + "…" : t;
  return `${num} · ${escapeHtml(short)}`;
}

/** FDD wording + normalized label; tooltip carries raw / merged text when helpful. */
function costBasisCell(r) {
  const basis = String(r.basis ?? "").trim();
  const norm = String(r.normalizedCostBasis ?? "").trim();
  const raw = String(r.rawCostBasisText ?? "").trim();
  const tipParts = [];
  if (raw) tipParts.push(raw);
  else if (basis) tipParts.push(basis);
  if (norm) tipParts.push(norm);
  const tip = tipParts.filter(Boolean).filter((x, i, a) => a.indexOf(x) === i).join(" · ");
  const title = escapeHtml(tip.slice(0, 900));
  if (basis && norm && norm.toLowerCase() !== basis.toLowerCase()) {
    return `<td class="col-wrap" title="${title}">${escapeHtml(basis)}<span class="cost-basis-sub">${escapeHtml(norm)}</span></td>`;
  }
  if (basis) {
    return `<td class="col-wrap" title="${title}">${escapeHtml(basis)}</td>`;
  }
  if (norm) {
    return `<td class="col-wrap muted-td" title="${title}">${escapeHtml(norm)}</td>`;
  }
  return `<td class="col-wrap muted-td" title="">—</td>`;
}

const SORT_INDICATOR = `<span class="sort-indicator"><span class="sort-indicator-arrow sort-indicator-arrow-up"></span><span class="sort-indicator-arrow sort-indicator-arrow-down"></span></span>`;

function sortableTh(label, key) {
  const active = econSortColumn === key;
  const cls = active ? (econSortDirection === "asc" ? "sort-asc" : "sort-desc") : "";
  return `<th data-sort="${key}" class="${cls}"><span style="display:inline-flex;align-items:center;white-space:nowrap;">${label}${SORT_INDICATOR}</span></th>`;
}

function sortableThCenter(label, key) {
  const active = econSortColumn === key;
  const cls = `${active ? (econSortDirection === "asc" ? "sort-asc" : "sort-desc") : ""} col-center`.trim();
  return `<th data-sort="${key}" class="${cls}"><span style="display:inline-flex;align-items:center;justify-content:center;white-space:nowrap;">${label}${SORT_INDICATOR}</span></th>`;
}

function getSearchHaystack(r) {
  const cat = bucketCategory(r.commercialCategory);
  const amtRaw = r.amount != null ? String(r.amount) : "";
  const amtFmt = formatAmountDisplay(r.amount);
  return `${r.feeOrObligationName || ""} ${r.feeType || ""} ${amtRaw} ${amtFmt} ${cat} ${r.reviewStatus || ""}`.toLowerCase();
}

function reviewStatusLower(r) {
  let s = r.reviewStatus;
  if (s && typeof s === "object" && !Array.isArray(s) && typeof s.name === "string") s = s.name;
  if (Array.isArray(s) && s.length) s = s[0];
  return String(s ?? "")
    .replace(/\u00a0/g, " ")
    .trim()
    .toLowerCase();
}

/** Stable key for selection / export (prefers server `id`). */
function econRowKey(r) {
  const id = r?.id;
  if (id != null && String(id).trim() !== "") return String(id).trim();
  return `anon:${bucketCategory(r.commercialCategory)}|${String(r.sourceItemNumber || "").trim()}|${String(r.feeOrObligationName || "").trim()}`;
}

function rowMatchesEconPreset(r) {
  if (!econFilterPreset) return true;
  const rs = reviewStatusLower(r);
  switch (econFilterPreset) {
    case "needs_review":
      return rs.includes("needs review") || rs === "need review";
    case "possible_dup":
      return r.possibleDuplicate === true;
    case "needs_legal":
      return r.needsLegalReview === true;
    case "needs_commercial":
      return r.needsCommercialReview === true;
    case "basis_needs":
      return r.basisNeedsReview === true;
    default:
      return true;
  }
}

function syncEconPresetChips() {
  const host = $("#econ-preset-chips");
  if (!host) return;
  const catFilter = ($("#econ-category-filter")?.value || "").trim();
  for (const btn of host.querySelectorAll("[data-preset]")) {
    const p = btn.getAttribute("data-preset");
    let active = false;
    if (p === "all") active = !econFilterPreset && !catFilter;
    else if (p && p.startsWith("cat_")) active = !econFilterPreset && catFilter === (ECON_PRESET_CATEGORY[p] || "");
    else active = econFilterPreset === p;
    btn.classList.toggle("active", active);
  }
}

function setEconBulkStatus(msg, show) {
  const el = $("#econ-bulk-status");
  if (!el) return;
  if (!show) {
    el.hidden = true;
    el.textContent = "";
    return;
  }
  el.hidden = false;
  el.innerHTML = msg;
}

function syncEconSortSelectFromState() {
  const sel = $("#econ-rows-sort");
  if (!sel) return;
  const token = `${econSortColumn}|${econSortDirection}`;
  if ([...sel.options].some((o) => o.value === token)) sel.value = token;
}

function applyEconSortFromSelect() {
  const sel = $("#econ-rows-sort");
  if (!sel) return;
  const raw = String(sel.value || "");
  const pipe = raw.indexOf("|");
  if (pipe < 0) return;
  const col = raw.slice(0, pipe);
  const dir = raw.slice(pipe + 1);
  if (!col || (dir !== "asc" && dir !== "desc")) return;
  econSortColumn = col;
  econSortDirection = dir;
  if (lastEconomics.length) renderEconomicsTable();
}

function setEconTableDensity(mode) {
  econTableDensity = mode === "compact" ? "compact" : "comfortable";
  const comfy = $("#econ-view-comfortable");
  const comp = $("#econ-view-compact");
  const scroll = document.querySelector("#econ-table-host .econ-table-scroll");
  if (scroll) scroll.classList.toggle("econ-table-scroll--compact", econTableDensity === "compact");
  if (comfy) {
    comfy.classList.toggle("active", econTableDensity === "comfortable");
    comfy.setAttribute("aria-pressed", econTableDensity === "comfortable" ? "true" : "false");
  }
  if (comp) {
    comp.classList.toggle("active", econTableDensity === "compact");
    comp.setAttribute("aria-pressed", econTableDensity === "compact" ? "true" : "false");
  }
}

function filterEconomicsRows(rows) {
  const q = ($("#econ-search")?.value || "").trim().toLowerCase();
  const catFilter = ($("#econ-category-filter")?.value || "").trim();
  return rows.filter((r) => {
    if (!rowMatchesEconPreset(r)) return false;
    if (catFilter && bucketCategory(r.commercialCategory) !== catFilter) return false;
    if (!q) return true;
    return getSearchHaystack(r).includes(q);
  });
}

function sortKeyValue(r, col) {
  switch (col) {
    case "commercialCategory":
      return bucketCategory(r.commercialCategory);
    case "reviewStatus":
      return String(r.reviewStatus || "").toLowerCase();
    case "feeOrObligationName":
      return String(r.feeOrObligationName || "").toLowerCase();
    case "feeType":
      return String(r.feeType || "").toLowerCase();
    case "amount":
      return String(r.amount != null ? r.amount : "").toLowerCase();
    case "costBasis":
      return `${String(r.basis || "").trim()} ${String(r.normalizedCostBasis || "").trim()}`.toLowerCase();
    case "basisConfidence":
      return String(r.basisConfidence || "").toLowerCase();
    case "basisNeedsReview":
      return r.basisNeedsReview === true ? 1 : 0;
    case "frequency":
      return String(r.frequency || "").toLowerCase();
    case "lifecyclePhase":
      return String(r.lifecyclePhase || "").toLowerCase();
    case "sourceItem":
      return `${r.sourceItemNumber || ""} ${r.sourceItemTitle || ""}`.toLowerCase();
    case "needsLegalReview":
      return r.needsLegalReview === true ? 1 : 0;
    case "needsCommercialReview":
      return r.needsCommercialReview === true ? 1 : 0;
    default:
      return "";
  }
}

function compareByColumn(a, b, col) {
  const va = sortKeyValue(a, col);
  const vb = sortKeyValue(b, col);
  if (typeof va === "number" && typeof vb === "number") return va - vb;
  return String(va).localeCompare(String(vb), undefined, { sensitivity: "base", numeric: true });
}

function sortEconomicsRows(rows) {
  const col = econSortColumn;
  const dir = econSortDirection === "desc" ? -1 : 1;
  const out = [...rows];
  out.sort((a, b) => {
    let c = compareByColumn(a, b, col);
    if (c === 0 && col !== "feeOrObligationName") {
      c = compareByColumn(a, b, "feeOrObligationName");
    }
    return c * dir;
  });
  return out;
}

function buildOwnerQuestions(rows) {
  const cats = new Set(rows.map((r) => bucketCategory(r.commercialCategory)));
  const out = [];
  if (cats.has("Recurring Brand Fee")) {
    out.push("Confirm exact basis for recurring brand fees and whether any fee ramps apply.");
  }
  if (cats.has("Sales / Marketing / Loyalty / Reservation Program")) {
    out.push("Clarify which sales, marketing, loyalty, reservation, and program fees are mandatory versus optional.");
  }
  if (cats.has("Required System / Technology Cost")) {
    out.push("Confirm required systems, implementation timing, vendor responsibility, and ongoing support costs.");
  }
  if (cats.has("Transfer / Renewal / Relicensing")) {
    out.push("Review transfer, renewal, relicensing, comfort letter, and ownership-change costs.");
  }
  if (cats.has("Termination / Default / Penalty")) {
    out.push("Review default triggers, cure periods, liquidated damages, non-compliance penalties, and post-termination obligations.");
  }
  if (cats.has("Legal / Operational Obligation")) {
    out.push("Review operational compliance obligations with legal and financial advisors.");
  }
  if (rows.some((r) => r.possibleDuplicate === true)) {
    out.push("Review possible duplicate rows before relying on totals or comparisons.");
  }
  return out;
}

function renderSummaryCards(summary) {
  const el = $("#fdd-summary-cards");
  const s = summary
    ? {
        approvedCount: Number(summary.approvedCount) || 0,
        needsReviewCount: Number(summary.needsReviewCount) || 0,
        possibleDuplicateCount: Number(summary.possibleDuplicateCount) || 0,
        legalReviewCount: Number(summary.legalReviewCount) || 0,
        commercialReviewCount: Number(summary.commercialReviewCount) || 0,
      }
    : emptyFddWorkspaceSummary();
  renderFddWorkspaceKpiStrip(el, s, "economics");
}

function renderEconomicsTable(disclaimerText) {
  const host = $("#econ-table-host");
  const wrap = $("#econ-results-wrap");
  if (!host) return;

  const scrollEl = host.querySelector(".econ-table-scroll");
  const prevScroll = scrollEl ? { l: scrollEl.scrollLeft, t: scrollEl.scrollTop } : null;

  if (disclaimerText) {
    const el = $("#disclaimer");
    if (el) {
      el.innerHTML = `<p class="disclaimer-box__api-text">${escapeHtml(disclaimerText)}</p>`;
    }
  }

  if (!lastEconomics.length) {
    wrap.hidden = true;
    selectedEconRowIds.clear();
    econFilterPreset = null;
    setEconBulkStatus("", false);
    host.innerHTML =
      '<p class="muted" style="margin-top:0;">Load a profile to see fee rows. Choose a brand and click <strong>Load profile</strong>.</p>';
    syncEconPresetChips();
    return;
  }

  const filtered = filterEconomicsRows(lastEconomics);
  const sorted = sortEconomicsRows(filtered);

  wrap.hidden = false;
  syncEconPresetChips();
  syncEconSortSelectFromState();

  const countEl = $("#econ-results-count");
  if (countEl) {
    const total = lastEconomics.length;
    const n = sorted.length;
    const selN = selectedEconRowIds.size;
    let range = "";
    if (n > 0) range = `Showing <strong>1</strong>–<strong>${n}</strong> of <strong>${total}</strong> fee obligations`;
    else range = `Showing <strong>0</strong> of <strong>${total}</strong> fee obligations`;
    const sel =
      selN > 0
        ? ` <span class="econ-results-meta">· <strong>${selN}</strong> selected</span>`
        : "";
    countEl.innerHTML = `${range}${sel}`;
  }

  if (!sorted.length) {
    host.innerHTML =
      '<div class="econ-table-panel"><div class="econ-empty-state">No rows match your filters or search. Try <strong>Reset view</strong>, another preset, or clear the search box.</div></div>';
    setEconTableDensity(econTableDensity);
    return;
  }

  const rowsHtml = sorted
    .map((r) => {
      const k = econRowKey(r);
      const encK = encodeURIComponent(k);
      const rowSel = selectedEconRowIds.has(k) ? " econ-row-selected" : "";
      const checked = selectedEconRowIds.has(k) ? " checked" : "";
      const dup = r.possibleDuplicate === true ? '<span class="pill dup" title="Possible duplicate">Dup</span>' : "";
      const cat = bucketCategory(r.commercialCategory);
      const amtDisplay = formatAmountDisplay(r.amount);
      const amtCell = amtDisplay !== "" ? escapeHtml(amtDisplay) : "—";
      return `<tr class="${rowSel.trim()}" data-row-key="${encK}">
        <td class="econ-cell-cb"><input type="checkbox" class="econ-row-cb" data-row-key="${encK}"${checked} aria-label="Select row" /></td>
        <td class="col-wrap cat-cell">${escapeHtml(cat)}</td>
        <td class="col-wrap">${reviewPill(r.reviewStatus)} ${dup}</td>
        <td class="col-wrap fee-name">${escapeHtml(r.feeOrObligationName || "")}</td>
        <td class="col-wrap muted">${escapeHtml(r.feeType || "—")}</td>
        <td class="col-wrap">${amtCell}</td>
        ${costBasisCell(r)}
        <td class="col-wrap">${escapeHtml(r.basisConfidence || "—")}</td>
        <td class="col-center">${ynPill(r.basisNeedsReview === true)}</td>
        <td class="col-wrap">${escapeHtml(r.frequency || "—")}</td>
        <td class="col-wrap">${escapeHtml(r.lifecyclePhase || "—")}</td>
        <td class="col-wrap muted">${sourceItemCell(r)}</td>
        <td class="col-center">${ynPill(r.needsLegalReview === true)}</td>
        <td class="col-center">${ynPill(r.needsCommercialReview === true)}</td>
      </tr>`;
    })
    .join("");

  const visibleKeys = sorted.map((r) => econRowKey(r));
  const allVisibleSelected = visibleKeys.length > 0 && visibleKeys.every((k) => selectedEconRowIds.has(k));
  const someVisibleSelected = visibleKeys.some((k) => selectedEconRowIds.has(k));

  host.innerHTML = `
    <div class="econ-table-panel">
      <div class="econ-table-scroll">
        <table class="econ-deals-table">
          <thead>
            <tr>
              <th class="econ-cell-cb"><input type="checkbox" id="econ-head-cb" title="Select visible" aria-label="Select all visible rows"${allVisibleSelected ? " checked" : ""} /></th>
              ${sortableTh("Category", "commercialCategory")}
              ${sortableTh("Review", "reviewStatus")}
              ${sortableTh("Fee / obligation", "feeOrObligationName")}
              ${sortableTh("Fee type", "feeType")}
              ${sortableTh("Amount", "amount")}
              ${sortableTh("Cost basis", "costBasis")}
              ${sortableTh("Basis confidence", "basisConfidence")}
              ${sortableThCenter("Basis review", "basisNeedsReview")}
              ${sortableTh("Frequency", "frequency")}
              ${sortableTh("Lifecycle", "lifecyclePhase")}
              ${sortableTh("Source item", "sourceItem")}
              ${sortableThCenter("Legal", "needsLegalReview")}
              ${sortableThCenter("Commercial", "needsCommercialReview")}
            </tr>
          </thead>
          <tbody>${rowsHtml}</tbody>
        </table>
      </div>
    </div>
  `;

  const headCb = $("#econ-head-cb");
  if (headCb) {
    headCb.indeterminate = !allVisibleSelected && someVisibleSelected;
  }

  setEconTableDensity(econTableDensity);

  const scrollNew = host.querySelector(".econ-table-scroll");
  if (scrollNew && prevScroll) {
    scrollNew.scrollLeft = prevScroll.l;
    scrollNew.scrollTop = prevScroll.t;
  }
}

function renderOwnerQuestions(rows) {
  const qs = buildOwnerQuestions(rows);
  const wrap = $("#owner-questions");
  const ul = $("#owner-questions-list");
  if (!qs.length) {
    wrap.hidden = true;
    ul.innerHTML = "";
    return;
  }
  wrap.hidden = false;
  ul.innerHTML = qs.map((q) => `<li>${escapeHtml(q)}</li>`).join("");
}

function onEconomicsTableClick(e) {
  const th = e.target.closest("th[data-sort]");
  if (!th) return;
  const key = th.getAttribute("data-sort");
  if (!key) return;
  if (econSortColumn === key) {
    econSortDirection = econSortDirection === "asc" ? "desc" : "asc";
  } else {
    econSortColumn = key;
    econSortDirection = "asc";
  }
  syncEconSortSelectFromState();
  renderEconomicsTable();
}

function scheduleEconomicsFilterRender() {
  if (econSearchDebounce) clearTimeout(econSearchDebounce);
  econSearchDebounce = window.setTimeout(() => {
    econSearchDebounce = null;
    if (lastEconomics.length) renderEconomicsTable();
  }, 200);
}

function resetEconomicsView() {
  const s = $("#econ-search");
  const c = $("#econ-category-filter");
  if (s) s.value = "";
  if (c) c.value = "";
  econFilterPreset = null;
  selectedEconRowIds.clear();
  econSortColumn = "commercialCategory";
  econSortDirection = "asc";
  econTableDensity = "comfortable";
  setEconBulkStatus("", false);
  if (lastEconomics.length) {
    syncEconSortSelectFromState();
    renderEconomicsTable();
  }
}

function populateCategoryFilter() {
  const sel = $("#econ-category-filter");
  if (!sel) return;
  const cur = sel.value;
  sel.innerHTML = '<option value="">All categories</option>';
  for (const cat of CATEGORY_ORDER) {
    const opt = document.createElement("option");
    opt.value = cat;
    opt.textContent = cat;
    sel.appendChild(opt);
  }
  if (cur && [...sel.options].some((o) => o.value === cur)) sel.value = cur;
}

function buildEconomicsCsvLines(rowList) {
  const lines = [ECON_CSV_HEADERS.join(",")];
  for (const r of rowList) {
    const row = ECON_CSV_HEADERS.map((h) => csvEscape(r[h]));
    lines.push(row.join(","));
  }
  return lines;
}

function exportCsv() {
  if (!lastEconomics.length) {
    alert("Load a profile first.");
    return;
  }
  const lines = buildEconomicsCsvLines(lastEconomics);
  const blob = new Blob([lines.join("\r\n")], { type: "text/csv;charset=utf-8" });
  const a = document.createElement("a");
  const safe = (lastMeta.brandName || "brand").replace(/[^\w\-]+/g, "_").slice(0, 80);
  a.href = URL.createObjectURL(blob);
  a.download = `fdd-economics-${safe}.csv`;
  a.click();
  URL.revokeObjectURL(a.href);
}

function exportCsvSelected() {
  if (!lastEconomics.length) {
    alert("Load a profile first.");
    return;
  }
  if (!selectedEconRowIds.size) {
    alert("Select at least one row (use checkboxes or Select visible).");
    return;
  }
  const picked = lastEconomics.filter((r) => selectedEconRowIds.has(econRowKey(r)));
  if (!picked.length) {
    alert("No matching rows in the loaded profile for the current selection.");
    return;
  }
  const lines = buildEconomicsCsvLines(picked);
  const blob = new Blob([lines.join("\r\n")], { type: "text/csv;charset=utf-8" });
  const a = document.createElement("a");
  const safe = (lastMeta.brandName || "brand").replace(/[^\w\-]+/g, "_").slice(0, 80);
  a.href = URL.createObjectURL(blob);
  a.download = `fdd-economics-selected-${safe}.csv`;
  a.click();
  URL.revokeObjectURL(a.href);
  setEconBulkStatus(`Exported <strong>${picked.length}</strong> selected row(s).`, true);
}

async function populateBrandSelect() {
  const sel = $("#brand-select");
  if (!sel) return;
  sel.innerHTML = '<option value="">Loading brands…</option>';
  registryDocuments = [];
  try {
    const res = await fetch(`${API}/documents`, { headers: { Accept: "application/json" } });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      sel.innerHTML = '<option value="">Could not load brands</option>';
      const errMsg = data.error || res.statusText || "Documents request failed";
      const hint404 =
        res.status === 404
          ? " This usually means the API process on this port is not this repo’s server.js (FDD routes missing), or Node was not restarted after updating routes. Try: stop all Node processes, run `npm start` from deal-capture-proxy, open http://localhost:8080/api/fdd-intelligence/health — you should see JSON { ok: true, … }."
          : "";
      $("#status-line").textContent = `${errMsg}${hint404}`;
      return;
    }
    const docs = Array.isArray(data.documents) ? data.documents : [];
    registryDocuments = docs;
    const set = new Set();
    for (const d of docs) {
      const n = String(d.brandName || "").trim();
      if (n) set.add(n);
    }
    const brands = [...set].sort((a, b) => a.localeCompare(b, undefined, { sensitivity: "base" }));
    sel.innerHTML = '<option value="">Choose a brand…</option>';
    for (const b of brands) {
      const opt = document.createElement("option");
      opt.value = b;
      opt.textContent = b;
      sel.appendChild(opt);
    }
    if (!brands.length) {
      sel.innerHTML = '<option value="">No brands in registry yet</option>';
    }
  } catch (e) {
    sel.innerHTML = '<option value="">Could not load brands</option>';
    $("#status-line").textContent = e.message || String(e);
  }
}

function populateYearSelectForBrand(brand) {
  const sel = $("#year-select");
  if (!sel) return;
  const nb = normBrandKey(brand);
  sel.innerHTML = '<option value="">All years</option>';
  if (!nb) return;
  const years = new Set();
  for (const d of registryDocuments) {
    if (normBrandKey(d.brandName) !== nb) continue;
    const fy = d.fddYear;
    const n = typeof fy === "number" && Number.isFinite(fy) ? fy : parseInt(String(fy ?? "").trim(), 10);
    if (Number.isFinite(n) && n >= 1990 && n <= 2100) years.add(n);
  }
  const sorted = [...years].sort((a, b) => b - a);
  for (const yr of sorted) {
    const opt = document.createElement("option");
    opt.value = String(yr);
    opt.textContent = String(yr);
    sel.appendChild(opt);
  }
}

function populateCountrySelectForBrand(brand) {
  const sel = $("#country-select");
  if (!sel) return;
  const nb = normBrandKey(brand);
  const cur = sel.value;
  sel.innerHTML = '<option value="">All countries</option>';
  if (!nb) return;
  const countries = new Set();
  for (const d of registryDocuments) {
    if (normBrandKey(d.brandName) !== nb) continue;
    const c = String(d.country ?? "")
      .trim()
      .replace(/\u00a0/g, " ");
    if (c) countries.add(c);
  }
  const sorted = [...countries].sort((a, b) => a.localeCompare(b, undefined, { sensitivity: "base" }));
  for (const c of sorted) {
    const opt = document.createElement("option");
    opt.value = c;
    opt.textContent = c;
    sel.appendChild(opt);
  }
  if (cur && [...sel.options].some((o) => o.value === cur)) sel.value = cur;
}

function applyQueryCountryToSelect() {
  const sp = new URLSearchParams(window.location.search);
  const c = sp.get("country");
  const sel = $("#country-select");
  if (!sel || c == null || String(c).trim() === "") return;
  const cTrim = String(c).trim();
  let found = false;
  for (const opt of sel.querySelectorAll("option")) {
    if (opt.value === cTrim) {
      found = true;
      sel.value = cTrim;
      break;
    }
  }
  if (!found && cTrim) {
    const opt = document.createElement("option");
    opt.value = cTrim;
    opt.textContent = `${cTrim} (from link)`;
    sel.appendChild(opt);
    sel.value = cTrim;
  }
}

function applyQueryYearToSelect() {
  const sp = new URLSearchParams(window.location.search);
  const y = sp.get("fddYear");
  const sel = $("#year-select");
  if (!sel || y == null || String(y).trim() === "") return;
  const yTrim = String(y).trim();
  let found = false;
  for (const opt of sel.querySelectorAll("option")) {
    if (opt.value === yTrim) {
      found = true;
      sel.value = yTrim;
      break;
    }
  }
  if (!found && /^\d{4}$/.test(yTrim)) {
    const opt = document.createElement("option");
    opt.value = yTrim;
    opt.textContent = `${yTrim} (from link)`;
    sel.appendChild(opt);
    sel.value = yTrim;
  }
}

function applyQueryBrandToSelect() {
  const sp = new URLSearchParams(window.location.search);
  const b = sp.get("brand");
  const sel = $("#brand-select");
  if (!b || !sel) return;
  const decoded = decodeURIComponent(b).trim();
  if (!decoded) return;
  let found = false;
  for (const opt of sel.querySelectorAll("option")) {
    if (opt.value === decoded) {
      found = true;
      sel.value = decoded;
      break;
    }
  }
  if (!found) {
    const opt = document.createElement("option");
    opt.value = decoded;
    opt.textContent = `${decoded} (from link)`;
    sel.appendChild(opt);
    sel.value = decoded;
  }
}

async function loadProfile() {
  const brand = $("#brand-select").value.trim();
  const yearRaw = $("#year-select").value.trim();
  const countryRaw = $("#country-select")?.value?.trim() ?? "";
  const includeNeeds = document.querySelector('input[name="review-scope"]:checked')?.value === "include-needs";
  $("#status-line").textContent = "";
  const host = $("#econ-table-host");
  if (host) host.innerHTML = "";
  $("#econ-results-wrap").hidden = true;
  $("#owner-questions").hidden = true;
  selectedEconRowIds.clear();
  econFilterPreset = null;
  setEconBulkStatus("", false);

  if (!brand) {
    $("#status-line").textContent = "Select a brand from the list.";
    renderSummaryCards(null);
    return;
  }

  const params = new URLSearchParams();
  if (includeNeeds) params.set("includeNeedsReview", "1");
  if (yearRaw) params.set("fddYear", yearRaw);
  if (countryRaw) params.set("country", countryRaw);
  const q = params.toString();
  const url = `${API}/brands/${encodeURIComponent(brand)}/economics${q ? `?${q}` : ""}`;

  try {
    const res = await fetch(url, { headers: { Accept: "application/json" } });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      $("#status-line").textContent = data.error || res.statusText || "Request failed";
      renderSummaryCards(null);
      return;
    }
    lastEconomics = Array.isArray(data.economics) ? data.economics : [];
    lastMeta = {
      brandName: data.brandName || brand,
      includeNeedsReview: !!data.includeNeedsReview,
      fddYear: data.fddYear != null ? data.fddYear : null,
      country: data.country != null && String(data.country).trim() !== "" ? String(data.country).trim() : null,
    };
    const summary = data.summary || null;
    renderSummaryCards(summary);
    renderEconomicsTable(data.disclaimer);
    renderOwnerQuestions(lastEconomics);
    const needsN = summary ? Number(summary.needsReviewCount) || 0 : 0;
    const modeLabel = includeNeeds ? "Approved + Needs Review" : "Approved only";
    let tail = "";
    if (!includeNeeds && needsN > 0) {
      tail = ` ${needsN} Needs Review row${needsN === 1 ? "" : "s"} hidden — switch to Include to see them.`;
    } else if (!includeNeeds && needsN === 0) {
      tail = " No Needs Review rows in scope, so this matches Include.";
    } else if (includeNeeds && needsN === 0) {
      tail = " No Needs Review rows in scope.";
    }
    const countryNote = countryRaw ? ` · Country: ${countryRaw}` : "";
    $("#status-line").textContent =
      `Loaded ${lastEconomics.length} row(s) · ${modeLabel}${countryNote}.${tail}`.trim();
  } catch (e) {
    $("#status-line").textContent = e.message || String(e);
    renderSummaryCards(null);
  }
}

function initScopeFromQuery() {
  const sp = new URLSearchParams(window.location.search);
  const inc = sp.get("includeNeedsReview");
  if (inc === "1" || inc === "true") {
    const r = document.querySelector('input[name="review-scope"][value="include-needs"]');
    if (r) r.checked = true;
  }
}

function wireEconomicsUi() {
  const host = $("#econ-table-host");
  if (host && !host.dataset.bound) {
    host.dataset.bound = "1";
    host.addEventListener("click", onEconomicsTableClick);
  }
  if (host && !host.dataset.econSelBound) {
    host.dataset.econSelBound = "1";
    host.addEventListener("change", (e) => {
      const t = e.target;
      if (!t) return;
      if (t.id === "econ-head-cb") {
        const on = /** @type {HTMLInputElement} */ (t).checked;
        const keys = [...host.querySelectorAll("tbody .econ-row-cb")].map((c) =>
          decodeURIComponent(/** @type {HTMLInputElement} */ (c).getAttribute("data-row-key") || "")
        );
        if (on) for (const k of keys) selectedEconRowIds.add(k);
        else for (const k of keys) selectedEconRowIds.delete(k);
        renderEconomicsTable();
        return;
      }
      if (t.classList.contains("econ-row-cb")) {
        const k = decodeURIComponent(/** @type {HTMLInputElement} */ (t).getAttribute("data-row-key") || "");
        if (/** @type {HTMLInputElement} */ (t).checked) selectedEconRowIds.add(k);
        else selectedEconRowIds.delete(k);
        renderEconomicsTable();
      }
    });
  }
  const chipHost = $("#econ-preset-chips");
  if (chipHost && !chipHost.dataset.bound) {
    chipHost.dataset.bound = "1";
    chipHost.addEventListener("click", (e) => {
      const b = e.target.closest("[data-preset]");
      if (!b) return;
      const preset = b.getAttribute("data-preset");
      const search = $("#econ-search");
      const cat = $("#econ-category-filter");
      if (preset === "all") {
        econFilterPreset = null;
        if (cat) cat.value = "";
        if (search) search.value = "";
      } else if (preset && preset.startsWith("cat_")) {
        econFilterPreset = null;
        if (cat) cat.value = ECON_PRESET_CATEGORY[preset] || "";
      } else {
        econFilterPreset = preset;
        if (cat) cat.value = "";
      }
      if (lastEconomics.length) renderEconomicsTable();
    });
  }
  const sortSel = $("#econ-rows-sort");
  if (sortSel && !sortSel.dataset.bound) {
    sortSel.dataset.bound = "1";
    sortSel.addEventListener("change", applyEconSortFromSelect);
  }
  const comfy = $("#econ-view-comfortable");
  const comp = $("#econ-view-compact");
  if (comfy && !comfy.dataset.bound) {
    comfy.dataset.bound = "1";
    comfy.addEventListener("click", () => setEconTableDensity("comfortable"));
  }
  if (comp && !comp.dataset.bound) {
    comp.dataset.bound = "1";
    comp.addEventListener("click", () => setEconTableDensity("compact"));
  }
  const btnVis = $("#btn-econ-select-visible");
  if (btnVis && !btnVis.dataset.bound) {
    btnVis.dataset.bound = "1";
    btnVis.addEventListener("click", () => {
      if (!lastEconomics.length) return;
      const filtered = filterEconomicsRows(lastEconomics);
      const sorted = sortEconomicsRows(filtered);
      for (const r of sorted) selectedEconRowIds.add(econRowKey(r));
      renderEconomicsTable();
      setEconBulkStatus(`Selected <strong>${sorted.length}</strong> visible row(s).`, true);
    });
  }
  const btnClr = $("#btn-econ-clear-selection");
  if (btnClr && !btnClr.dataset.bound) {
    btnClr.dataset.bound = "1";
    btnClr.addEventListener("click", () => {
      selectedEconRowIds.clear();
      setEconBulkStatus("", false);
      renderEconomicsTable();
    });
  }
  const btnCsvSel = $("#btn-econ-csv-selected");
  if (btnCsvSel && !btnCsvSel.dataset.bound) {
    btnCsvSel.dataset.bound = "1";
    btnCsvSel.addEventListener("click", () => exportCsvSelected());
  }
  const search = $("#econ-search");
  if (search && !search.dataset.bound) {
    search.dataset.bound = "1";
    search.addEventListener("input", scheduleEconomicsFilterRender);
  }
  const cat = $("#econ-category-filter");
  if (cat && !cat.dataset.bound) {
    cat.dataset.bound = "1";
    cat.addEventListener("change", () => {
      econFilterPreset = null;
      if (lastEconomics.length) renderEconomicsTable();
    });
  }
  const resetBtn = $("#btn-reset-view");
  if (resetBtn && !resetBtn.dataset.bound) {
    resetBtn.dataset.bound = "1";
    resetBtn.addEventListener("click", resetEconomicsView);
  }
}

async function boot() {
  initScopeFromQuery();
  await populateBrandSelect();
  populateCategoryFilter();
  applyQueryBrandToSelect();
  const b0 = $("#brand-select").value.trim();
  populateYearSelectForBrand(b0);
  populateCountrySelectForBrand(b0);
  applyQueryYearToSelect();
  applyQueryCountryToSelect();
  wireEconomicsUi();
  $("#brand-select").addEventListener("change", () => {
    const b = $("#brand-select").value.trim();
    populateYearSelectForBrand(b);
    populateCountrySelectForBrand(b);
    applyQueryYearToSelect();
    applyQueryCountryToSelect();
  });
  $("#btn-load").addEventListener("click", () => loadProfile());
  $("#btn-csv").addEventListener("click", () => exportCsv());
  renderSummaryCards(readFddKpiSessionSummary("economics"));
}

boot().catch((e) => {
  $("#status-line").textContent = e.message || String(e);
});
