/**
 * FDD Intelligence admin UI — calls same-origin /api/fdd-intelligence/* only (no secrets).
 */

import { runFddFullExtractWithMonitoring } from "/js/fdd-extract-full-progress-ui.js";
import {
  emptyFddWorkspaceSummary,
  fddRowsToWorkspaceSummary,
  readFddKpiSessionSummary,
  renderFddWorkspaceKpiStrip,
} from "/js/fdd-workspace-kpi.js";

const API = "/api/fdd-intelligence";

let documents = [];
let selectedId = null;

/** All rows for the selected document (from last GET …/rows). */
let allRowsForDoc = [];
/** Selected row ids (may include rows not currently visible after filter). */
const selectedRowIds = new Set();

const COMMERCIAL_CATEGORIES = [
  "Franchise Fee",
  "Recurring Brand Fee",
  "Estimated Initial Investment",
  "Required System / Technology Cost",
  "Training / Conference / Education",
  "Sales / Marketing / Loyalty / Reservation Program",
  "Transfer / Renewal / Relicensing",
  "Termination / Default / Penalty",
  "Legal / Operational Obligation",
  "Optional Program",
  "Pass-Through / Third-Party Cost",
  "Other / Needs Review",
];

/** When set, quick preset applies instead of the advanced filter dropdowns (dropdowns cleared on preset). */
let filterPreset = null;

/** Sort order for visible (filtered) rows. */
let rowsSort = "name_asc";

/** "comfortable" | "compact" — table density. */
let rowsViewDensity = "comfortable";

const FILTER_SELECT_IDS = [
  "f-review-status",
  "f-audit-status",
  "f-commercial-cat",
  "f-commercial-review",
  "f-legal-review",
  "f-duplicate",
  "f-basis-review",
  "f-auto-eligible",
];

const $ = (sel) => document.querySelector(sel);

async function fetchJson(url, options) {
  const res = await fetch(url, {
    ...options,
    headers: {
      Accept: "application/json",
      ...(options && options.headers),
    },
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || res.statusText || "Request failed");
  return data;
}

function escapeHtml(s) {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function renderFullExtractionSummaryHtml(data) {
  const f = data.fees || {};
  const t = data.terms || {};
  const lines = [];
  lines.push(
    `<strong>Full extraction</strong> · <code>${escapeHtml(String(data.fullExtractionRunId || ""))}</code> · text source <strong>${escapeHtml(
      String(data.fullTextSource || "—")
    )}</strong> · ${data.sectionsCount ?? 0} section(s)`
  );
  lines.push(
    `Fees: <strong>${f.rowsCreated ?? 0}</strong> row(s) — ${f.ok ? "OK" : "<span style=\"color:#ffb4b8\">failed</span>"}`
  );
  lines.push(
    `Terms: <strong>${t.rowsCreated ?? 0}</strong> row(s) — ${t.ok ? "OK" : "<span style=\"color:#ffb4b8\">failed</span>"}`
  );
  if (data.partialSuccess) {
    lines.push(`<strong style="color:#ffe082">Partial success</strong> — one leg failed; check fees.ok and terms.ok in the API response.`);
  }
  const warn = [];
  if (Array.isArray(data.warnings)) for (const w of data.warnings) if (w) warn.push(String(w));
  if (Array.isArray(f.warnings)) for (const w of f.warnings) if (w) warn.push(`Fees: ${w}`);
  if (Array.isArray(t.warnings)) for (const w of t.warnings) if (w) warn.push(`Terms: ${w}`);
  if (warn.length) lines.push("Warnings: " + warn.slice(0, 14).map(escapeHtml).join(" · "));
  lines.push(
    `<a class="btn ghost" href="/franchise-intelligence-admin.html" style="display:inline-block;margin-top:0.4rem;">Open Franchise Intelligence Admin (fees + terms) →</a>`
  );
  return `<div class="bulk-status-msg">${lines.join("<br />")}</div>`;
}

function pillClass(status) {
  const s = String(status || "");
  if (s === "Approved") return "pill approved";
  if (s === "Rejected") return "pill rejected";
  if (s === "Needs Review") return "pill needs";
  return "pill draft";
}

function auditStatusPill(status) {
  const s = String(status || "").trim();
  if (!s) return "—";
  let cls = "audit-pill";
  if (s === "High Confidence") cls += " audit-pill--high";
  else if (s === "Quick Review") cls += " audit-pill--quick";
  else if (s === "Needs Review") cls += " audit-pill--needs";
  else if (s === "Manual Review Required") cls += " audit-pill--manual";
  else if (s === "Do Not Auto-Approve") cls += " audit-pill--block";
  return `<span class="${cls}">${escapeHtml(s)}</span>`;
}

function renderDocTable() {
  const tb = $("#doc-table tbody");
  tb.innerHTML = "";
  for (const d of documents) {
    const tr = document.createElement("tr");
    if (d.id === selectedId) tr.classList.add("selected");
    tr.innerHTML = `
      <td>${escapeHtml(d.brandName || "")}</td>
      <td>${escapeHtml(d.parentCompany || "—")}</td>
      <td>${escapeHtml(String(d.fddYear ?? ""))}</td>
      <td>${escapeHtml(d.country || "")}</td>
      <td>${escapeHtml(d.extractionStatus || "")}</td>
    `;
    tr.addEventListener("click", () => selectDocument(d.id));
    tb.appendChild(tr);
  }
}

async function loadDocuments() {
  const data = await fetchJson(`${API}/documents`);
  documents = data.documents || [];
  $("#storage-banner").textContent =
    data.storage === "airtable"
      ? "Storage: Airtable (documents, sections, and fee rows are persisted in your base)."
      : "Storage: in-memory (data clears when the server restarts). Set AIRTABLE_TABLE_FDD_* with API credentials for Airtable persistence.";
  if (data.airtable && data.airtable !== null) {
    $("#storage-banner").textContent += ` Airtable: ${data.airtable}.`;
  }
  renderDocTable();
}

function clearRowSelection() {
  selectedRowIds.clear();
  syncHeaderCheckbox();
}

function clearAllFilterSelects() {
  for (const id of FILTER_SELECT_IDS) {
    const el = document.getElementById(id);
    if (el) el.value = "";
  }
}

function anyFilterSelectActive() {
  return FILTER_SELECT_IDS.some((id) => {
    const el = document.getElementById(id);
    return el && String(el.value || "").trim() !== "";
  });
}

function syncPresetChips() {
  const host = $("#fdd-admin-preset-chips");
  if (!host) return;
  for (const btn of host.querySelectorAll(".fdd-admin-chip[data-preset]")) {
    const p = btn.getAttribute("data-preset");
    let active = false;
    if (filterPreset) {
      active = p === filterPreset;
    } else if (p === "all") {
      active = !anyFilterSelectActive();
    }
    btn.classList.toggle("active", active);
  }
}

function applyPresetFromChip(preset) {
  if (preset === "all") {
    filterPreset = null;
    clearAllFilterSelects();
    setBulkStatus("");
    renderRowsTable();
    return;
  }
  clearAllFilterSelects();
  filterPreset = preset;
  setBulkStatus("");
  if (preset === "safe") {
    renderRowsTable();
    const vis = getVisibleRows();
    clearRowSelection();
    for (const r of vis) selectedRowIds.add(r.id);
    setBulkStatus(
      `<p class="bulk-status-msg">Safe Commercial Review Queue: <strong>${vis.length}</strong> row(s). Visible rows are selected. Choose <strong>All</strong> to clear.</p>`
    );
  }
  renderRowsTable();
}

function applyRowsViewDensity() {
  const wrap = $("#fdd-rows-wrap");
  if (!wrap) return;
  wrap.classList.toggle("fdd-rows-wrap--compact", rowsViewDensity === "compact");
  const c = $("#fdd-view-comfortable");
  const k = $("#fdd-view-compact");
  if (c) {
    c.classList.toggle("active", rowsViewDensity === "comfortable");
    c.setAttribute("aria-pressed", rowsViewDensity === "comfortable" ? "true" : "false");
  }
  if (k) {
    k.classList.toggle("active", rowsViewDensity === "compact");
    k.setAttribute("aria-pressed", rowsViewDensity === "compact" ? "true" : "false");
  }
}

function resetRowsToolUiForNewDocument() {
  filterPreset = null;
  rowsSort = "name_asc";
  rowsViewDensity = "comfortable";
  clearAllFilterSelects();
  const sortEl = $("#fdd-rows-sort");
  if (sortEl) sortEl.value = "name_asc";
  applyRowsViewDensity();
  syncPresetChips();
}

function getFilterState() {
  return {
    reviewStatus: ($("#f-review-status")?.value || "").trim(),
    auditStatus: ($("#f-audit-status")?.value || "").trim(),
    commercialCategory: ($("#f-commercial-cat")?.value || "").trim(),
    commercialReview: ($("#f-commercial-review")?.value || "").trim(),
    legalReview: ($("#f-legal-review")?.value || "").trim(),
    duplicate: ($("#f-duplicate")?.value || "").trim(),
    basisReview: ($("#f-basis-review")?.value || "").trim(),
    autoEligible: ($("#f-auto-eligible")?.value || "").trim(),
  };
}

function rowMatchesDropdownFilters(r) {
  const f = getFilterState();
  const rs = String(r.reviewStatus || "").trim();
  if (f.reviewStatus && rs !== f.reviewStatus) return false;

  const as = String(r.auditStatus || "").trim();
  if (f.auditStatus && as !== f.auditStatus) return false;

  const cat = String(r.commercialCategory || "").trim();
  if (f.commercialCategory && cat !== f.commercialCategory) return false;

  if (f.commercialReview === "needs") {
    if (r.needsCommercialReview !== true) return false;
  } else if (f.commercialReview === "cleared") {
    if (r.needsCommercialReview !== false) return false;
  }

  if (f.legalReview === "needs") {
    if (r.needsLegalReview !== true) return false;
  } else if (f.legalReview === "no") {
    if (r.needsLegalReview === true) return false;
  }

  if (f.duplicate === "dup") {
    if (r.possibleDuplicate !== true) return false;
  } else if (f.duplicate === "not") {
    if (r.possibleDuplicate === true) return false;
  }

  if (f.basisReview === "needs") {
    if (r.basisNeedsReview !== true) return false;
  } else if (f.basisReview === "clear") {
    if (r.basisNeedsReview === true) return false;
  }

  if (f.autoEligible === "yes") {
    if (r.autoApproveEligible !== true) return false;
  } else if (f.autoEligible === "no") {
    if (r.autoApproveEligible === true) return false;
  }

  return true;
}

const SAFE_QUEUE_BLOCKED_CATS = new Set([
  "Other / Needs Review",
  "Legal / Operational Obligation",
  "Termination / Default / Penalty",
  "Transfer / Renewal / Relicensing",
]);

function rowMatchesSafeCommercialQueue(r) {
  if (String(r.reviewStatus || "").trim() !== "Needs Review") return false;
  const as = String(r.auditStatus || "").trim();
  if (as !== "High Confidence" && as !== "Quick Review") return false;
  if (r.needsCommercialReview !== true) return false;
  if (r.needsLegalReview === true) return false;
  if (r.possibleDuplicate === true) return false;
  if (r.basisNeedsReview === true) return false;
  const cat = String(r.commercialCategory || "").trim();
  if (SAFE_QUEUE_BLOCKED_CATS.has(cat)) return false;
  return true;
}

function rowMatchesPresetOrDropdowns(r) {
  if (filterPreset === "safe") return rowMatchesSafeCommercialQueue(r);
  if (filterPreset === "needs_review") return String(r.reviewStatus || "").trim() === "Needs Review";
  if (filterPreset === "high_quick") {
    const a = String(r.auditStatus || "").trim();
    return a === "High Confidence" || a === "Quick Review";
  }
  if (filterPreset === "needs_commercial") return r.needsCommercialReview === true;
  if (filterPreset === "auto_eligible") return r.autoApproveEligible === true;
  if (filterPreset === "possible_dup") return r.possibleDuplicate === true;
  if (filterPreset === "needs_legal") return r.needsLegalReview === true;
  if (filterPreset === "basis_needs") return r.basisNeedsReview === true;
  if (filterPreset === "manual_or_blocked") {
    const a = String(r.auditStatus || "").trim();
    return a === "Manual Review Required" || a === "Do Not Auto-Approve";
  }
  return rowMatchesDropdownFilters(r);
}

function getFilteredRows() {
  return allRowsForDoc.filter(rowMatchesPresetOrDropdowns);
}

function auditScoreNum(r) {
  const n = Number(r.auditScore);
  return Number.isFinite(n) ? n : null;
}

function amountNum(r) {
  const n = Number(r.amount);
  return Number.isFinite(n) ? n : null;
}

function sortRowsInPlace(list) {
  const keyName = (r) => String(r.feeOrObligationName || "").toLowerCase();
  const keyCat = (r) => String(r.commercialCategory || "").toLowerCase();
  const keyReview = (r) => String(r.reviewStatus || "").toLowerCase();
  const cmpStr = (a, b) => (a < b ? -1 : a > b ? 1 : 0);

  list.sort((a, b) => {
    if (rowsSort === "name_asc") return cmpStr(keyName(a), keyName(b));
    if (rowsSort === "name_desc") return cmpStr(keyName(b), keyName(a));
    if (rowsSort === "cat_asc") return cmpStr(keyCat(a), keyCat(b)) || cmpStr(keyName(a), keyName(b));
    if (rowsSort === "review_asc") return cmpStr(keyReview(a), keyReview(b)) || cmpStr(keyName(a), keyName(b));
    const asA = auditScoreNum(a);
    const asB = auditScoreNum(b);
    if (rowsSort === "audit_desc") {
      const va = asA != null ? asA : -1;
      const vb = asB != null ? asB : -1;
      if (vb !== va) return vb - va;
      return cmpStr(keyName(a), keyName(b));
    }
    if (rowsSort === "audit_asc") {
      const va = asA != null ? asA : 1e9;
      const vb = asB != null ? asB : 1e9;
      if (va !== vb) return va - vb;
      return cmpStr(keyName(a), keyName(b));
    }
    const amA = amountNum(a);
    const amB = amountNum(b);
    if (rowsSort === "amount_desc") {
      const va = amA != null ? amA : -Infinity;
      const vb = amB != null ? amB : -Infinity;
      if (vb !== va) return vb - va;
      return cmpStr(keyName(a), keyName(b));
    }
    if (rowsSort === "amount_asc") {
      const va = amA != null ? amA : Infinity;
      const vb = amB != null ? amB : Infinity;
      if (va !== vb) return va - vb;
      return cmpStr(keyName(a), keyName(b));
    }
    return cmpStr(keyName(a), keyName(b));
  });
  return list;
}

function getVisibleRows() {
  const copy = [...getFilteredRows()];
  return sortRowsInPlace(copy);
}

function updateRowsCountLine() {
  const vis = getVisibleRows().length;
  const total = allRowsForDoc.length;
  const sel = selectedRowIds.size;
  const rc = $("#fdd-rows-results-count");
  if (rc) {
    const selPart = sel > 0 ? ` <span class="fdd-results-meta">· ${escapeHtml(String(sel))} selected</span>` : "";
    rc.innerHTML = `Showing <strong>${escapeHtml(String(vis))}</strong> of <strong>${escapeHtml(String(total))}</strong> fee rows${selPart}`;
  }
}

function syncHeaderCheckbox() {
  const head = $("#rows-head-cb");
  if (!head) return;
  const vis = getVisibleRows();
  const visIds = vis.map((r) => r.id);
  const allSel = visIds.length > 0 && visIds.every((id) => selectedRowIds.has(id));
  const someSel = visIds.some((id) => selectedRowIds.has(id));
  head.checked = allSel;
  head.indeterminate = someSel && !allSel;
}

function renderRowsTable() {
  const tb = $("#rows-table tbody");
  if (!tb) return;
  tb.innerHTML = "";
  const list = getVisibleRows();
  for (const r of list) {
    const tr = document.createElement("tr");
    const leg = r.needsLegalReview === true;
    const com = r.needsCommercialReview === true;
    const sel = selectedRowIds.has(r.id);
    const dupPill =
      r.possibleDuplicate === true
        ? '<span class="pill pill-dup" title="Possible duplicate">Dup</span>'
        : "";
    const autoPill =
      r.autoApproveEligible === true ? '<span class="pill pill-auto">Auto</span>' : "";
    tr.innerHTML = `
      <td class="cell-cb"><input type="checkbox" data-row-select="${escapeHtml(r.id)}" ${sel ? "checked" : ""} /></td>
      <td>${escapeHtml(r.feeOrObligationName || "")}</td>
      <td>${escapeHtml(r.feeType || "")}</td>
      <td>${escapeHtml(r.commercialCategory || "—")}</td>
      <td class="muted" title="${escapeHtml(r.duplicateGroupKey || "")}">${dupPill}</td>
      <td>${escapeHtml(r.amount != null ? String(r.amount) : "—")}</td>
      <td class="muted" style="font-size:0.75rem;max-width:140px;" title="${escapeHtml(r.rawCostBasisText || "")}">${escapeHtml(r.normalizedCostBasis || "—")}</td>
      <td class="muted" style="font-size:0.75rem;">${escapeHtml(r.amountFormulaType || "—")}</td>
      <td>${escapeHtml(r.basisConfidence || "—")}</td>
      <td>${ynPill(r.basisNeedsReview === true)}</td>
      <td>${escapeHtml(r.lifecyclePhase || "")}</td>
      <td><span class="${pillClass(r.reviewStatus)}">${escapeHtml(r.reviewStatus || "")}</span></td>
      <td>${r.auditScore != null && r.auditScore !== "" ? escapeHtml(String(r.auditScore)) : "—"}</td>
      <td class="muted" style="font-size:0.75rem;">${auditStatusPill(r.auditStatus)}</td>
      <td class="muted" style="font-size:0.7rem;max-width:120px;" title="${escapeHtml(r.auditIssues || "")}">${escapeHtml(shortAuditIssues(r.auditIssues))}</td>
      <td>${autoPill}</td>
      <td class="muted" style="font-size:0.75rem;">${sourceItemCell(r)}</td>
      <td>${escapeHtml(r.extractionConfidence || "—")}</td>
      <td>${ynPill(leg)}</td>
      <td>${ynPill(com)}</td>
      <td class="muted" style="font-size:0.7rem;max-width:90px;overflow:hidden;text-overflow:ellipsis;" title="${escapeHtml(r.extractionRunId || "")}">${escapeHtml(shortRunId(r.extractionRunId))}</td>
      <td class="actions-cell">
        <button type="button" data-act="needs" data-id="${escapeHtml(r.id)}">Needs review</button>
        <button type="button" data-act="ok" data-id="${escapeHtml(r.id)}">Approve</button>
        <button type="button" data-act="no" data-id="${escapeHtml(r.id)}">Reject</button>
        <button type="button" data-act="tgl-leg" data-id="${escapeHtml(r.id)}" data-val="${leg ? "1" : "0"}">Toggle Legal</button>
        <button type="button" data-act="tgl-com" data-id="${escapeHtml(r.id)}" data-val="${com ? "1" : "0"}">Toggle Commercial</button>
      </td>
    `;
    tb.appendChild(tr);
  }
  updateRowsCountLine();
  syncHeaderCheckbox();
  syncPresetChips();
  applyRowsViewDensity();
  const kpiHost = $("#fdd-summary-cards");
  renderFddWorkspaceKpiStrip(kpiHost, fddRowsToWorkspaceSummary(allRowsForDoc), "admin");
}

function shortRunId(id) {
  const s = String(id || "");
  return s.length <= 16 ? s : "…" + s.slice(-12);
}

function sourceItemCell(r) {
  const num = String(r.sourceItemNumber || "").trim();
  const looksUsItem = /^\d{1,2}$/.test(num);
  const label = looksUsItem ? `Item ${escapeHtml(num)}` : escapeHtml(num || "—");
  const t = String(r.sourceItemTitle || "");
  const short = t.length > 40 ? t.slice(0, 40) + "…" : t;
  return `${label} · ${escapeHtml(short)}`;
}

function ynPill(v) {
  const on = v === true;
  return `<span class="pill ${on ? "yesno-yes" : "yesno-no"}">${on ? "Yes" : "No"}</span>`;
}

function shortAuditIssues(text) {
  const t = String(text || "").trim();
  if (!t) return "—";
  const cut = t.split(";")[0].trim();
  return cut.length > 48 ? cut.slice(0, 46) + "…" : cut;
}

async function refreshFddFormatNote(docId, doc) {
  const noteEl = document.getElementById("fdd-format-note");
  if (!noteEl) return;
  try {
    const secData = await fetchJson(`${API}/documents/${encodeURIComponent(docId)}/sections`);
    const sections = secData.sections || [];
    const hasRoman = sections.some((s) => String(s.sourceFormat || "") === "roman_numeral_disclosure");
    const hasUs = sections.some((s) => String(s.sourceFormat || "") === "us_fdd_item");
    const country = String(doc?.country || "").trim().toUpperCase();
    const nonUs = country && country !== "US" && country !== "USA";
    if (nonUs || hasRoman) {
      noteEl.hidden = false;
      let msg =
        "This document uses a non-U.S. disclosure format or Roman numeral section headings. Dealality mapped sections to internal extraction targets; source references use the section label (e.g. V) instead of U.S. Item numbers where applicable.";
      if (hasRoman && hasUs) msg += " Mixed U.S. Item and Roman-style headings were detected.";
      noteEl.textContent = msg;
    } else {
      noteEl.hidden = true;
      noteEl.textContent = "";
    }
  } catch {
    noteEl.hidden = true;
    noteEl.textContent = "";
  }
}

async function loadRows(docId, { preserveSelection = false } = {}) {
  const data = await fetchJson(`${API}/documents/${encodeURIComponent(docId)}/rows`);
  allRowsForDoc = data.rows || [];
  if (!preserveSelection) clearRowSelection();
  renderRowsTable();
}

async function selectDocument(id) {
  selectedId = id;
  clearRowSelection();
  resetRowsToolUiForNewDocument();
  setBulkStatus("");
  renderDocTable();
  $("#detail-empty").hidden = true;
  $("#detail-panel").hidden = false;
  const data = await fetchJson(`${API}/documents/${encodeURIComponent(id)}`);
  const d = data.document;
  $("#d-brand").textContent = d.brandName || "";
  $("#d-meta").textContent = `${d.parentCompany || ""} · ${d.fddYear || ""} · ${d.country || ""} · ${d.extractionStatus || ""}`;
  const link = $("#link-file");
  if (d.filePath || d.fileName) {
    link.href = `${API}/documents/${encodeURIComponent(id)}/file`;
    link.style.display = "inline-block";
  } else {
    link.style.display = "none";
    link.removeAttribute("href");
  }
  $("#text-debug").style.display = "none";
  $("#text-debug").textContent = "";
  const fullSum = $("#full-extract-summary");
  if (fullSum) {
    fullSum.style.display = "none";
    fullSum.innerHTML = "";
  }
  await loadRows(id, { preserveSelection: false });
  await refreshFddFormatNote(id, d);
}

function setBulkStatus(html) {
  const el = $("#bulk-status");
  if (el) el.innerHTML = html;
}

function summarizeBulkResponse(data) {
  const skipped = data.skipped || [];
  const top = {};
  for (const s of skipped) {
    const r = s.reason || "unknown";
    top[r] = (top[r] || 0) + 1;
  }
  const topLines = Object.entries(top)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 8)
    .map(([k, v]) => `${escapeHtml(k)}: ${v}`);
  const parts = [
    `<strong>${escapeHtml(data.action || "")}</strong>`,
    `Requested: <strong>${data.requestedCount ?? 0}</strong>`,
    `Updated: <strong>${data.updatedCount ?? 0}</strong>`,
    `Skipped: <strong>${data.skippedCount ?? 0}</strong>`,
  ];
  if (topLines.length) parts.push("Top skip reasons: " + topLines.join(" · "));
  return `<p class="bulk-status-msg">${parts.join(" · ")}</p>`;
}

async function postBulk(action) {
  if (!selectedId) return;
  const ids = [...selectedRowIds];
  if (!ids.length) {
    alert("Select at least one row.");
    return;
  }
  const body = { rowIds: ids, action };
  const data = await fetchJson(`${API}/documents/${encodeURIComponent(selectedId)}/bulk-update-rows`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  return data;
}

async function handleRowsTableClick(e) {
  const btn = e.target.closest("button[data-act]");
  if (!btn || !selectedId) return;
  const rowId = btn.getAttribute("data-id");
  const act = btn.getAttribute("data-act");
  if (!rowId || !act) return;
  try {
    if (act === "tgl-leg") {
      const cur = btn.getAttribute("data-val") === "1";
      await fetchJson(`${API}/rows/${encodeURIComponent(rowId)}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ needsLegalReview: !cur }),
      });
    } else if (act === "tgl-com") {
      const cur = btn.getAttribute("data-val") === "1";
      await fetchJson(`${API}/rows/${encodeURIComponent(rowId)}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ needsCommercialReview: !cur }),
      });
    } else {
      const map = { needs: "Needs Review", ok: "Approved", no: "Rejected" };
      await fetchJson(`${API}/rows/${encodeURIComponent(rowId)}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ reviewStatus: map[act] }),
      });
    }
    await loadRows(selectedId, { preserveSelection: true });
  } catch (err) {
    alert(err.message || String(err));
  }
}

function wireRowsTable() {
  const table = $("#rows-table");
  if (!table || table.dataset.bound) return;
  table.dataset.bound = "1";
  table.addEventListener("click", handleRowsTableClick);
  table.addEventListener("change", (e) => {
    const cb = e.target.closest("input[data-row-select]");
    if (!cb) return;
    const id = cb.getAttribute("data-row-select");
    if (!id) return;
    if (cb.checked) selectedRowIds.add(id);
    else selectedRowIds.delete(id);
    updateRowsCountLine();
    syncHeaderCheckbox();
  });
}

function wireRowFilters() {
  const ids = [
    "f-review-status",
    "f-audit-status",
    "f-commercial-cat",
    "f-commercial-review",
    "f-legal-review",
    "f-duplicate",
    "f-basis-review",
    "f-auto-eligible",
  ];
  for (const id of ids) {
    const el = document.getElementById(id);
    if (el && !el.dataset.bound) {
      el.dataset.bound = "1";
      el.addEventListener("change", () => {
        filterPreset = null;
        renderRowsTable();
      });
    }
  }
  const chipHost = $("#fdd-admin-preset-chips");
  if (chipHost && !chipHost.dataset.bound) {
    chipHost.dataset.bound = "1";
    chipHost.addEventListener("click", (e) => {
      const btn = e.target.closest(".fdd-admin-chip[data-preset]");
      if (!btn) return;
      const preset = btn.getAttribute("data-preset");
      if (!preset) return;
      if (preset !== "all" && !selectedId) return;
      applyPresetFromChip(preset);
    });
  }
  const sortEl = $("#fdd-rows-sort");
  if (sortEl && !sortEl.dataset.bound) {
    sortEl.dataset.bound = "1";
    sortEl.addEventListener("change", () => {
      rowsSort = String(sortEl.value || "name_asc").trim() || "name_asc";
      renderRowsTable();
    });
  }
  const vc = $("#fdd-view-comfortable");
  if (vc && !vc.dataset.bound) {
    vc.dataset.bound = "1";
    vc.addEventListener("click", () => {
      rowsViewDensity = "comfortable";
      applyRowsViewDensity();
    });
  }
  const vk = $("#fdd-view-compact");
  if (vk && !vk.dataset.bound) {
    vk.dataset.bound = "1";
    vk.addEventListener("click", () => {
      rowsViewDensity = "compact";
      applyRowsViewDensity();
    });
  }
  const head = $("#rows-head-cb");
  if (head && !head.dataset.bound) {
    head.dataset.bound = "1";
    head.addEventListener("change", () => {
      const vis = getVisibleRows();
      if (head.checked) {
        for (const r of vis) selectedRowIds.add(r.id);
      } else {
        for (const r of vis) selectedRowIds.delete(r.id);
      }
      renderRowsTable();
    });
  }
  const selVis = $("#btn-select-visible");
  if (selVis && !selVis.dataset.bound) {
    selVis.dataset.bound = "1";
    selVis.addEventListener("click", () => {
      for (const r of getVisibleRows()) selectedRowIds.add(r.id);
      renderRowsTable();
    });
  }
  const clr = $("#btn-clear-selection");
  if (clr && !clr.dataset.bound) {
    clr.dataset.bound = "1";
    clr.addEventListener("click", () => {
      clearRowSelection();
      renderRowsTable();
    });
  }

  const bindBulk = (btnId, action, confirmText) => {
    const b = document.getElementById(btnId);
    if (!b || b.dataset.bound) return;
    b.dataset.bound = "1";
    b.addEventListener("click", async () => {
      if (!selectedId) return;
      if (confirmText && !confirm(confirmText)) return;
      try {
        const data = await postBulk(action);
        setBulkStatus(summarizeBulkResponse(data));
        clearRowSelection();
        await loadDocuments();
        await loadRows(selectedId, { preserveSelection: false });
      } catch (err) {
        alert(err.message || String(err));
      }
    });
  };

  bindBulk(
    "btn-bulk-clear-commercial",
    "clear_commercial_review",
    "You are clearing commercial review for selected rows. They will be re-audited and may become auto-approve eligible. Continue?"
  );
  bindBulk("btn-bulk-mark-commercial", "mark_commercial_review_needed", null);
  bindBulk(
    "btn-bulk-approve",
    "approve",
    "You are about to approve selected rows that pass the safety checks. Legal, duplicate, unclear-basis, transfer, termination/default, and Other/Needs Review rows will be skipped. Continue?"
  );
  bindBulk("btn-bulk-reject", "reject", "Mark selected rows as Rejected?");
  bindBulk("btn-bulk-needs-review", "needs_review", "Mark selected rows as Needs Review?");

  const auditBtn = $("#btn-bulk-audit-doc");
  if (auditBtn && !auditBtn.dataset.bound) {
    auditBtn.dataset.bound = "1";
    auditBtn.addEventListener("click", async () => {
      if (!selectedId) return;
      if (!confirm("Run audit for the entire document (all rows)?")) return;
      try {
        const data = await fetchJson(`${API}/documents/${encodeURIComponent(selectedId)}/audit`, { method: "POST" });
        setBulkStatus(
          `<p class="bulk-status-msg">Document audit complete. Audited <strong>${data.auditedCount ?? 0}</strong> row(s). Auto-approve eligible: <strong>${data.autoApproveEligibleCount ?? 0}</strong>.</p>`
        );
        await loadRows(selectedId, { preserveSelection: true });
      } catch (err) {
        alert(err.message || String(err));
      }
    });
  }
}

$("#btn-refresh").addEventListener("click", () => loadDocuments().catch(alertErr));
$("#btn-open-register").addEventListener("click", () => {
  $("#dlg-register").showModal();
});
$("#btn-cancel-dlg").addEventListener("click", () => $("#dlg-register").close());

$("#form-register").addEventListener("submit", async (e) => {
  e.preventDefault();
  const form = $("#form-register");
  const fd = new FormData(form);
  const file = fd.get("file");
  const hasFile = file && file.size > 0;
  try {
    if (hasFile) {
      const up = new FormData();
      for (const [k, v] of fd.entries()) {
        if (k === "file" && v && v.size) up.append("file", v);
        else if (k !== "file" && typeof v === "string") up.append(k, v);
      }
      await fetchJson(`${API}/documents`, { method: "POST", body: up });
    } else {
      const body = {};
      for (const [k, v] of fd.entries()) {
        if (k === "file") continue;
        if (typeof v === "string" && v.trim()) body[k] = v.trim();
      }
      await fetchJson(`${API}/documents`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
    }
    $("#dlg-register").close();
    form.reset();
    await loadDocuments();
  } catch (err) {
    alert(err.message || String(err));
  }
});

$("#btn-extract-full").addEventListener("click", async () => {
  if (!selectedId) return;
  const sumEl = $("#full-extract-summary");
  const btn = $("#btn-extract-full");
  btn.disabled = true;
  try {
    const job = await runFddFullExtractWithMonitoring({ apiBase: API, documentId: selectedId });
    const data = job && job.result ? job.result : {};
    if (sumEl) {
      sumEl.style.display = "block";
      sumEl.innerHTML = renderFullExtractionSummaryHtml(data);
    }
    await loadDocuments();
    await loadRows(selectedId, { preserveSelection: true });
    try {
      const docData = await fetchJson(`${API}/documents/${encodeURIComponent(selectedId)}`);
      const d = docData.document;
      $("#d-meta").textContent = `${d.parentCompany || ""} · ${d.fddYear || ""} · ${d.country || ""} · ${d.extractionStatus || ""}`;
    } catch (_) {
      /* ignore */
    }
    if (!data.ok && !data.partialSuccess) {
      alert(data.error || data.warnings?.[0] || "Full extraction failed");
    }
  } catch (err) {
    if (sumEl) {
      sumEl.style.display = "block";
      sumEl.innerHTML = `<p class="bulk-status-msg" style="color:#ffb4b8;">${escapeHtml(err.message || String(err))}</p>`;
    }
    alert(err.message || String(err));
  } finally {
    btn.disabled = false;
  }
});

$("#btn-extract").addEventListener("click", async () => {
  if (!selectedId) return;
  try {
    await fetchJson(`${API}/documents/${encodeURIComponent(selectedId)}/extract`, { method: "POST" });
    await loadDocuments();
    await selectDocument(selectedId);
  } catch (err) {
    alert(err.message || String(err));
  }
});

$("#btn-audit").addEventListener("click", async () => {
  if (!selectedId) return;
  try {
    const data = await fetchJson(`${API}/documents/${encodeURIComponent(selectedId)}/audit`, { method: "POST" });
    const parts = [
      `Audited ${data.auditedCount ?? 0} row(s).`,
      `High confidence: ${data.highConfidenceCount ?? 0}`,
      `Quick review: ${data.quickReviewCount ?? 0}`,
      `Needs review: ${data.needsReviewCount ?? 0}`,
      `Manual / blocked: ${data.manualReviewRequiredCount ?? 0}`,
      `Auto-approve eligible: ${data.autoApproveEligibleCount ?? 0}`,
    ];
    alert(parts.join("\n"));
    await loadRows(selectedId, { preserveSelection: true });
  } catch (err) {
    alert(err.message || String(err));
  }
});

$("#btn-approve-auto").addEventListener("click", async () => {
  if (!selectedId) return;
  if (
    !confirm(
      "Approve all currently auto-eligible rows for this document? Only rows that pass strict safeguards will be approved."
    )
  ) {
    return;
  }
  try {
    const data = await fetchJson(`${API}/documents/${encodeURIComponent(selectedId)}/approve-auto-eligible`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({}),
    });
    const skipped = data.skippedReasonsSummary || {};
    const skipText = Object.keys(skipped).length
      ? "\nSkipped reasons:\n" +
          Object.entries(skipped)
            .map(([k, v]) => `  ${k}: ${v}`)
            .join("\n")
      : "";
    alert(`Approved ${data.approvedCount ?? 0} row(s). Skipped ${data.skippedCount ?? 0}.${skipText}`);
    clearRowSelection();
    await loadDocuments();
    await loadRows(selectedId, { preserveSelection: false });
  } catch (err) {
    alert(err.message || String(err));
  }
});

$("#btn-load-text").addEventListener("click", async () => {
  if (!selectedId) return;
  try {
    const data = await fetchJson(`${API}/documents/${encodeURIComponent(selectedId)}?includeText=1`);
    $("#text-debug").style.display = "block";
    $("#text-debug").textContent = data.fullText || "(empty)";
  } catch (err) {
    alert(err.message || String(err));
  }
});

function alertErr(err) {
  alert(err.message || String(err));
}

function fillCategorySelect() {
  const sel = $("#f-commercial-cat");
  if (!sel || sel.dataset.filled) return;
  sel.dataset.filled = "1";
  for (const c of COMMERCIAL_CATEGORIES) {
    const opt = document.createElement("option");
    opt.value = c;
    opt.textContent = c;
    sel.appendChild(opt);
  }
}

wireRowsTable();
wireRowFilters();
fillCategorySelect();
applyRowsViewDensity();
syncPresetChips();

loadDocuments().catch(alertErr);

renderFddWorkspaceKpiStrip(
  $("#fdd-summary-cards"),
  readFddKpiSessionSummary("admin") || emptyFddWorkspaceSummary(),
  "admin"
);
