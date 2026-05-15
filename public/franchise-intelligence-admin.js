/**
 * FDD Intelligence admin UI — calls same-origin /api/fdd-intelligence/* only (no secrets).
 */

import { runFddFullExtractWithMonitoring } from "/js/fdd-extract-full-progress-ui.js";

const API = "/api/fdd-intelligence";

/** Unified workspace state (fee vs term selections stay independent). */
const state = {
  documents: [],
  selectedDocument: null,
  selectedDocumentId: null,
  feeRows: [],
  termRows: [],
  feeFilters: {},
  termFilters: {},
  selectedFeeRowIds: null,
  selectedTermIds: null,
  activePanel: "overview",
  lastActionSummary: "",
};

let documents = [];
let selectedId = null;

/** All rows for the selected document (from last GET …/rows). */
let allRowsForDoc = [];
/** Selected row ids (may include rows not currently visible after filter). */
const selectedRowIds = new Set();
state.selectedFeeRowIds = selectedRowIds;

let allTermsForDoc = [];
let lastTermsStorage = "";
const selectedTermIds = new Set();
state.selectedTermIds = selectedTermIds;
let useSafeTermsQueuePreset = false;

const TERM_CATEGORIES = [
  "Territory / Area Protection",
  "Franchise Term",
  "Renewal Rights",
  "Transfer / Change of Ownership",
  "Termination / Default",
  "Liquidated Damages",
  "Post-Termination Obligations",
  "PIP / Renovation / Brand Standards",
  "Required Systems / Technology",
  "Training / Staffing / Operator Requirements",
  "Reporting / Audit / Records",
  "Approved Suppliers / Procurement",
  "Insurance / Indemnification",
  "Financial Performance Representation",
  "System Health / Outlets",
  "Dispute Resolution / Governing Law",
  "Other / Needs Review",
];

const NORMALIZED_TERM_BUCKETS = [
  "Protected Territory / Area Rights",
  "No Protected Territory",
  "Brand Carveouts / Affiliate Rights",
  "Initial Franchise Term",
  "Renewal Right / Renewal Conditions",
  "Then-Current Agreement Requirement",
  "Transfer Approval Requirement",
  "Change of Control Restriction",
  "Lender / Foreclosure Rights",
  "Transfer Fee / Transfer Process",
  "Termination for Cause",
  "Immediate Termination Event",
  "Owner Termination Right",
  "Liquidated Damages Formula",
  "Post-Termination De-Identification",
  "Non-Compete / Restrictive Covenant",
  "PIP Trigger / Renovation Requirement",
  "Brand Standards Compliance",
  "Required Systems Obligation",
  "Approved Supplier Requirement",
  "Reporting / Audit Rights",
  "Insurance Requirement",
  "Indemnification Obligation",
  "Dispute Resolution / Arbitration",
  "Governing Law / Venue",
  "Financial Performance Representation",
  "Outlet / System Health Disclosure",
  "Other / Needs Mapping",
];

const COMPARABLE_TERM_GROUPS = [
  "Territory & Competitive Protection",
  "Term & Renewal Flexibility",
  "Transfer & Exit Flexibility",
  "Termination & Default Exposure",
  "Post-Termination Restrictions",
  "Capex / PIP / Brand Standards",
  "Operating Control / Systems Requirements",
  "Supplier / Procurement Restrictions",
  "Reporting / Audit / Compliance",
  "Legal / Dispute / Indemnity",
  "Performance / System Health",
  "Other / Needs Mapping",
];

const TERM_LEGAL_SENSITIVE_BLOCKED = new Set([
  "Territory / Area Protection",
  "Renewal Rights",
  "Transfer / Change of Ownership",
  "Termination / Default",
  "Liquidated Damages",
  "Post-Termination Obligations",
  "Insurance / Indemnification",
  "Dispute Resolution / Governing Law",
]);

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
  const body = options && options.body;
  const isJsonBody = typeof body === "string" && !(options && options.headers && options.headers["Content-Type"]);
  const res = await fetch(url, {
    ...options,
    headers: {
      Accept: "application/json",
      ...(isJsonBody ? { "Content-Type": "application/json" } : {}),
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
    `<span style="display:inline-flex;flex-wrap:wrap;gap:0.4rem;margin-top:0.45rem;">` +
      `<button type="button" class="ghost fi-jump-panel" data-fi-panel="fees">Fee &amp; Cost Review</button>` +
      `<button type="button" class="ghost fi-jump-panel" data-fi-panel="terms">Terms &amp; Obligations Review</button>` +
      `</span>`
  );
  return `<div class="bulk-status-msg">${lines.join("<br />")}</div>`;
}

/** Short line for #fi-action-summary after full extraction (detail stays in #full-extract-summary). */
function buildFullExtractActionSummaryHtml(data) {
  const f = data.fees || {};
  const t = data.terms || {};
  const lines = [];
  lines.push(
    `Fees: <strong>${escapeHtml(String(f.rowsCreated ?? 0))}</strong> created — ${f.ok ? "OK" : "<span style=\"color:#ffb4b8\">failed</span>"}.`
  );
  lines.push(
    `Terms: <strong>${escapeHtml(String(t.rowsCreated ?? 0))}</strong> created — ${t.ok ? "OK" : "<span style=\"color:#ffb4b8\">failed</span>"}.`
  );
  if (data.partialSuccess) {
    lines.push('<strong style="color:#ffe082">Partial success</strong> — one leg may have failed.');
  }
  const w = [];
  if (Array.isArray(data.warnings)) for (const x of data.warnings) if (x) w.push(String(x));
  if (Array.isArray(f.warnings)) for (const x of f.warnings) if (x) w.push(`Fees: ${x}`);
  if (Array.isArray(t.warnings)) for (const x of t.warnings) if (x) w.push(`Terms: ${x}`);
  if (w.length) lines.push("Warnings: " + w.slice(0, 10).map(escapeHtml).join(" · "));
  if (data.error) lines.push(`<span style="color:#ffb4b8">${escapeHtml(String(data.error))}</span>`);
  return `<p class="bulk-status-msg" style="margin:0;">${lines.join("<br />")}</p>`;
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
  const tb = $("#fi-doc-table tbody");
  if (!tb) return;
  tb.innerHTML = "";
  for (const d of documents) {
    const tr = document.createElement("tr");
    if (d.id === selectedId) tr.classList.add("selected");
    const feeCol = d.id === selectedId ? String(allRowsForDoc.length) : "—";
    const termCol = d.id === selectedId ? String(allTermsForDoc.length) : "—";
    const ext = d.extractedAt ? escapeHtml(String(d.extractedAt).replace("T", " ").slice(0, 19)) : "—";
    tr.innerHTML = `
      <td>${escapeHtml(d.brandName || "")}</td>
      <td>${escapeHtml(d.parentCompany || "—")}</td>
      <td>${escapeHtml(String(d.fddYear ?? ""))}</td>
      <td>${escapeHtml(d.country || "")}</td>
      <td>${escapeHtml(d.extractionStatus || "")}</td>
      <td class="cell-num">${escapeHtml(feeCol)}</td>
      <td class="cell-num">${escapeHtml(termCol)}</td>
      <td class="muted" style="font-size:0.72rem;">${ext}</td>
    `;
    tr.addEventListener("click", () => selectDocument(d.id));
    tb.appendChild(tr);
  }
}

async function loadDocuments() {
  const data = await fetchJson(`${API}/documents`);
  documents = data.documents || [];
  let msg =
    data.storage === "airtable"
      ? "Storage: Airtable (documents, sections, fee rows, and terms when configured)."
      : "Storage: in-memory (data clears when the server restarts). Set AIRTABLE_TABLE_FDD_* and AIRTABLE_TABLE_FDD_TERMS for Airtable persistence.";
  if (data.airtable && data.airtable !== null) {
    msg += ` Airtable: ${data.airtable}.`;
  }
  if (lastTermsStorage) {
    msg += ` Terms storage: ${lastTermsStorage}.`;
  }
  if (useSafeTermsQueuePreset) msg += " Preset: Safe terms review queue.";
  const ban = $("#storage-banner");
  if (ban) ban.textContent = msg;
  renderDocTable();
  syncUnifiedState();
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
    syncUnifiedState();
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
  syncUnifiedState();
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

function getTermFilterState() {
  return {
    reviewStatus: ($("#f-term-review")?.value || "").trim(),
    termCategory: ($("#f-term-category")?.value || "").trim(),
    normalizedBucket: ($("#f-term-bucket")?.value || "").trim(),
    comparableGroup: ($("#f-term-comparable")?.value || "").trim(),
    riskLevel: ($("#f-term-risk")?.value || "").trim(),
    flexibilityLevel: ($("#f-term-flex")?.value || "").trim(),
    legalReview: ($("#f-term-legal")?.value || "").trim(),
    commercialReview: ($("#f-term-comm")?.value || "").trim(),
    duplicate: ($("#f-term-dup")?.value || "").trim(),
    termAuditStatus: ($("#f-term-audit-status")?.value || "").trim(),
    autoEligible: ($("#f-term-auto")?.value || "").trim(),
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
      <td class="cell-cb cell-sticky-col-0"><input type="checkbox" data-row-select="${escapeHtml(r.id)}" ${sel ? "checked" : ""} /></td>
      <td class="cell-sticky-col-1">${escapeHtml(r.feeOrObligationName || "")}</td>
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
  const presetMsg =
    "This document uses a non-U.S. or mixed disclosure format. Dealality mapped source sections to internal extraction targets.";
  const setNotes = (visible, text) => {
    for (const el of document.querySelectorAll(".fi-sync-format-note")) {
      el.hidden = !visible;
      el.textContent = visible ? text : "";
    }
  };
  try {
    const secData = await fetchJson(`${API}/documents/${encodeURIComponent(docId)}/sections`);
    const sections = secData.sections || [];
    const hasRoman = sections.some((s) => String(s.sourceFormat || "") === "roman_numeral_disclosure");
    const hasMixedFmt = sections.some((s) => String(s.sourceFormat || "") === "mixed");
    const hasUs = sections.some((s) => String(s.sourceFormat || "") === "us_fdd_item");
    const country = String(doc?.country || "").trim().toUpperCase();
    const nonUs = country && country !== "US" && country !== "USA";
    const notes = String(doc?.extractionNotes || "").toLowerCase();
    const notesHint =
      notes.includes("roman_numeral") || notes.includes("mixed") || notes.includes("roman heading");
    if (nonUs || hasRoman || hasMixedFmt || (hasRoman && hasUs) || notesHint) {
      setNotes(true, presetMsg);
    } else {
      setNotes(false, "");
    }
  } catch {
    setNotes(false, "");
  }
}

async function loadRows(docId, { preserveSelection = false } = {}) {
  const data = await fetchJson(`${API}/documents/${encodeURIComponent(docId)}/rows`);
  allRowsForDoc = data.rows || [];
  if (!preserveSelection) clearRowSelection();
  renderRowsTable();
  syncUnifiedState();
  renderDocKpiStrip();
  renderOverviewPanel();
  renderDocTable();
}

function summarizeSkippedByReasonTerms(skipped) {
  const m = {};
  for (const s of skipped || []) {
    const r = String(s.reason || "unknown");
    m[r] = (m[r] || 0) + 1;
  }
  return m;
}

function formatSkippedSummary(skipped, label) {
  if (!skipped || !skipped.length) return "";
  const m = summarizeSkippedByReasonTerms(skipped);
  const parts = Object.keys(m).map((k) => `${k}: ${m[k]}`);
  return `${label} skipped (${skipped.length}): ` + parts.join(", ");
}

function setTermsBulkResult(msg) {
  const el = $("#bulk-result");
  if (el) el.textContent = msg || "";
}

function populateTermCategoryFilter() {
  const sel = $("#f-term-category");
  if (!sel) return;
  const cur = sel.value;
  sel.innerHTML = '<option value="">All</option>';
  for (const c of TERM_CATEGORIES) {
    const o = document.createElement("option");
    o.value = c;
    o.textContent = c;
    sel.appendChild(o);
  }
  if ([...sel.options].some((o) => o.value === cur)) sel.value = cur;
}

function populateTermSelectFromList(selId, list) {
  const sel = $(selId);
  if (!sel) return;
  const cur = sel.value;
  sel.innerHTML = '<option value="">All</option>';
  for (const c of list) {
    const o = document.createElement("option");
    o.value = c;
    o.textContent = c;
    sel.appendChild(o);
  }
  if ([...sel.options].some((o) => o.value === cur)) sel.value = cur;
}

function ynPillClassYesNo(v) {
  return v ? "pill yesno-yes" : "pill yesno-no";
}

function getVisibleTerms() {
  if (useSafeTermsQueuePreset) {
    return allTermsForDoc.filter(safeTermsQueuePredicate);
  }
  const cat = ($("#f-term-category") && $("#f-term-category").value) || "";
  const rev = ($("#f-term-review") && $("#f-term-review").value) || "";
  const legal = ($("#f-term-legal") && $("#f-term-legal").value) || "";
  const comm = ($("#f-term-comm") && $("#f-term-comm").value) || "";
  const risk = ($("#f-term-risk") && $("#f-term-risk").value) || "";
  const flex = ($("#f-term-flex") && $("#f-term-flex").value) || "";
  const bucket = ($("#f-term-bucket") && $("#f-term-bucket").value) || "";
  const comparable = ($("#f-term-comparable") && $("#f-term-comparable").value) || "";
  const dup = ($("#f-term-dup") && $("#f-term-dup").value) || "";
  const auditSt = ($("#f-term-audit-status") && $("#f-term-audit-status").value) || "";
  const auto = ($("#f-term-auto") && $("#f-term-auto").value) || "";

  return allTermsForDoc.filter((t) => {
    if (cat && String(t.termCategory || "") !== cat) return false;
    if (rev && String(t.reviewStatus || "") !== rev) return false;
    if (legal === "yes" && !t.legalReviewRequired) return false;
    if (legal === "no" && t.legalReviewRequired) return false;
    if (comm === "yes" && !t.commercialReviewRequired) return false;
    if (comm === "no" && t.commercialReviewRequired) return false;
    if (risk && String(t.riskLevel || "") !== risk) return false;
    if (flex && String(t.flexibilityLevel || "") !== flex) return false;
    if (bucket && String(t.normalizedTermBucket || "") !== bucket) return false;
    if (comparable && String(t.comparableTermGroup || "") !== comparable) return false;
    if (dup === "yes" && !t.possibleDuplicateTerm) return false;
    if (dup === "no" && t.possibleDuplicateTerm) return false;
    if (auditSt && String(t.termAuditStatus || "") !== auditSt) return false;
    if (auto === "yes" && !t.autoApproveEligible) return false;
    if (auto === "no" && t.autoApproveEligible) return false;
    return true;
  });
}

function renderTermsTable() {
  const tb = $("#terms-table tbody");
  if (!tb) return;
  tb.innerHTML = "";
  const rows = getVisibleTerms();
  const tcl = $("#terms-count-label");
  if (tcl) {
    tcl.textContent =
      selectedId && rows.length
        ? `${rows.length} visible / ${allTermsForDoc.length} total · ${selectedTermIds.size} selected`
        : selectedId
          ? `${allTermsForDoc.length} term(s) · ${selectedTermIds.size} selected`
          : "";
  }

  for (const t of rows) {
    const tr = document.createElement("tr");
    const sum = String(t.termSummary || "").slice(0, 120);
    const iss = String(t.termAuditIssues || "").slice(0, 100);
    const id = String(t.id);
    const checked = selectedTermIds.has(id);
    tr.innerHTML = `
      <td class="cell-cb cell-sticky-col-0"><input type="checkbox" data-term-id="${escapeHtml(id)}" ${checked ? "checked" : ""} aria-label="Select term" /></td>
      <td class="cell-clip cell-sticky-col-1" title="${escapeHtml(t.termObligationName || "")}">${escapeHtml(t.termObligationName || "—")}</td>
      <td class="cell-clip-sm" title="${escapeHtml(t.termCategory || "")}">${escapeHtml(t.termCategory || "—")}</td>
      <td class="cell-clip-sm" title="${escapeHtml(t.normalizedTermBucket || "")}">${escapeHtml(t.normalizedTermBucket || "—")}</td>
      <td class="cell-clip-sm" title="${escapeHtml(t.comparableTermGroup || "")}">${escapeHtml(t.comparableTermGroup || "—")}</td>
      <td><span class="${ynPillClassYesNo(!!t.possibleDuplicateTerm)}">${t.possibleDuplicateTerm ? "Yes" : "No"}</span></td>
      <td>${escapeHtml(t.sourceItemNumber || "—")}</td>
      <td class="cell-clip" title="${escapeHtml(t.termSummary || "")}">${escapeHtml(sum)}${(t.termSummary || "").length > 120 ? "…" : ""}</td>
      <td>${escapeHtml(t.riskLevel || "—")}</td>
      <td>${escapeHtml(t.flexibilityLevel || "—")}</td>
      <td><span class="${ynPillClassYesNo(!!t.legalReviewRequired)}">${t.legalReviewRequired ? "Yes" : "No"}</span></td>
      <td><span class="${ynPillClassYesNo(!!t.commercialReviewRequired)}">${t.commercialReviewRequired ? "Yes" : "No"}</span></td>
      <td><span class="${pillClass(t.reviewStatus)}">${escapeHtml(t.reviewStatus || "")}</span></td>
      <td class="cell-num">${escapeHtml(String(t.termAuditScore ?? "—"))}</td>
      <td class="cell-clip-sm" title="${escapeHtml(t.termAuditStatus || "")}">${escapeHtml(t.termAuditStatus || "—")}</td>
      <td class="cell-clip-sm" title="${escapeHtml(t.termAuditIssues || "")}">${escapeHtml(iss)}${(t.termAuditIssues || "").length > 100 ? "…" : ""}</td>
      <td><span class="${ynPillClassYesNo(!!t.autoApproveEligible)}">${t.autoApproveEligible ? "Yes" : "No"}</span></td>
      <td class="actions-cell" data-term-id="${escapeHtml(id)}"></td>
    `;
    const cb = tr.querySelector('input[type="checkbox"][data-term-id]');
    cb.addEventListener("change", () => {
      if (cb.checked) selectedTermIds.add(id);
      else selectedTermIds.delete(id);
      renderTermsTable();
    });
    const cell = tr.querySelector(".actions-cell");
    const mkBtn = (label, action) => {
      const b = document.createElement("button");
      b.type = "button";
      b.textContent = label;
      b.addEventListener("click", () => patchTermAction(id, action));
      cell.appendChild(b);
    };
    mkBtn("Approve", "approve");
    mkBtn("Reject", "reject");
    mkBtn("Needs review", "needs_review");
    mkBtn("Toggle legal", "toggle_legal");
    mkBtn("Toggle commercial", "toggle_commercial");
    tb.appendChild(tr);
  }
}

async function patchTermAction(termId, action) {
  await fetchJson(`${API}/terms/${encodeURIComponent(termId)}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ action }),
  });
  await loadTermsForSelected();
}

async function loadTermsForSelected() {
  if (!selectedId) return;
  const data = await fetchJson(`${API}/documents/${encodeURIComponent(selectedId)}/terms`);
  lastTermsStorage = data.storage || "";
  allTermsForDoc = data.terms || [];
  renderTermsTable();
  populateTermCategoryFilter();
  populateTermSelectFromList("#f-term-bucket", NORMALIZED_TERM_BUCKETS);
  populateTermSelectFromList("#f-term-comparable", COMPARABLE_TERM_GROUPS);
  await loadDocuments();
  syncUnifiedState();
  renderDocKpiStrip();
  renderOverviewPanel();
  renderDocTable();
}

async function postBulkTerms(action) {
  const ids = [...selectedTermIds];
  if (!selectedId) return;
  if (!ids.length) {
    alert("Select at least one term (checkboxes).");
    return;
  }
  if (action === "clear_legal_review") {
    if (!confirm("Clear Legal Review Required for selected rows? This is sensitive; confirm only if policy allows.")) return;
  }
  if (action === "approve") {
    if (!confirm("Bulk approve selected terms using safeguards (score ≥80, no duplicates, no legal-review rows, etc.)?")) return;
  }
  if (action === "reject") {
    if (!confirm("Mark selected terms as Rejected?")) return;
  }
  const data = await fetchJson(`${API}/documents/${encodeURIComponent(selectedId)}/bulk-update-terms`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ termIds: ids, action }),
  });
  selectedTermIds.clear();
  const bits = [`Updated ${data.updatedCount ?? 0}.`, data.skippedCount ? formatSkippedSummary(data.skipped, "Skipped rows") : ""];
  const summaryText = bits.filter(Boolean).join(" ");
  setTermsBulkResult(summaryText);
  setActionSummary(summaryText ? `<p class="bulk-status-msg" style="margin:0;">${escapeHtml(summaryText)}</p>` : "");
  await loadTermsForSelected();
}

async function runExtractTerms() {
  if (!selectedId) return;
  const b = $("#btn-extract-terms");
  if (b) b.disabled = true;
  $("#extract-warnings").hidden = true;
  const fullSum = $("#full-extract-summary");
  if (fullSum) {
    fullSum.style.display = "none";
    fullSum.innerHTML = "";
  }
  try {
    const data = await fetchJson(`${API}/documents/${encodeURIComponent(selectedId)}/extract-terms`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({}),
    });
    lastTermsStorage = data.storage || "";
    allTermsForDoc = data.terms || [];
    const w = data.warnings;
    const box = $("#extract-warnings");
    if (Array.isArray(w) && w.length && box) {
      box.hidden = false;
      box.innerHTML = "<strong>Run notes:</strong><br />" + w.map((x) => escapeHtml(x)).join("<br />");
    } else if (box) {
      box.hidden = true;
      box.textContent = "";
    }
    renderTermsTable();
    await loadTermsForSelected();
    const n = (data.terms || []).length;
    setActionSummary(
      `<p class="bulk-status-msg" style="margin:0;">Terms-only extraction finished. Term rows loaded: <strong>${escapeHtml(String(n))}</strong>.</p>`
    );
  } finally {
    if (b) b.disabled = false;
  }
}

async function runTermsAuditDoc() {
  if (!selectedId) return;
  const disableIds = ["btn-audit-terms", "fi-term-btn-run-audit"];
  for (const id of disableIds) {
    const b = document.getElementById(id);
    if (b) b.disabled = true;
  }
  try {
    const data = await fetchJson(`${API}/documents/${encodeURIComponent(selectedId)}/audit-terms`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({}),
    });
    const by = data.summary && data.summary.byStatus;
    const statusLine =
      by && typeof by === "object"
        ? ` By audit status: ${Object.entries(by)
            .map(([k, v]) => `${k}: ${v}`)
            .join(", ")}.`
        : "";
    const msg = `Audited ${data.termCount ?? 0} term(s).${statusLine}`;
    setTermsBulkResult(msg);
    setActionSummary(`<p class="bulk-status-msg" style="margin:0;">${escapeHtml(msg)}</p>`);
    await loadTermsForSelected();
  } catch (e) {
    alert(e.message || String(e));
  } finally {
    for (const id of disableIds) {
      const b = document.getElementById(id);
      if (b) b.disabled = false;
    }
  }
}

async function runApproveAutoEligibleTerms() {
  if (!selectedId) return;
  if (
    !confirm(
      "Approve every term that is Auto-Approve Eligible after re-audit? Legal-sensitive rows stay excluded by server rules."
    )
  ) {
    return;
  }
  const b = $("#btn-approve-auto-terms");
  if (b) b.disabled = true;
  try {
    const data = await fetchJson(`${API}/documents/${encodeURIComponent(selectedId)}/approve-auto-eligible-terms`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({}),
    });
    const lines = [`Approved ${data.approvedCount ?? 0}.`, formatSkippedSummary(data.skipped, "Not auto-approved")].filter(Boolean);
    const summaryText = lines.join(" ");
    setTermsBulkResult(summaryText);
    setActionSummary(`<p class="bulk-status-msg" style="margin:0;">${escapeHtml(summaryText)}</p>`);
    await loadTermsForSelected();
  } catch (e) {
    alert(e.message || String(e));
  } finally {
    if (b) b.disabled = false;
  }
}

function wireTermsTable() {
  const table = $("#terms-table");
  if (!table || table.dataset.bound) return;
  table.dataset.bound = "1";
}

function wireTermFilters() {
  const ids = [
    "f-term-category",
    "f-term-review",
    "f-term-legal",
    "f-term-comm",
    "f-term-risk",
    "f-term-flex",
    "f-term-bucket",
    "f-term-comparable",
    "f-term-dup",
    "f-term-audit-status",
    "f-term-auto",
  ];
  for (const id of ids) {
    const el = document.getElementById(id);
    if (el && !el.dataset.bound) {
      el.dataset.bound = "1";
      el.addEventListener("change", () => {
        renderTermsTable();
        syncUnifiedState();
      });
    }
  }
}

function wireTermBulkBar() {
  const presetSafe = $("#fi-term-btn-preset-safe");
  if (presetSafe && !presetSafe.dataset.listener) {
    presetSafe.dataset.listener = "1";
    presetSafe.addEventListener("click", () => {
      useSafeTermsQueuePreset = true;
      loadDocuments().catch(() => {});
      renderTermsTable();
      syncUnifiedState();
    });
  }
  const presetClr = $("#fi-term-btn-preset-clear");
  if (presetClr && !presetClr.dataset.listener) {
    presetClr.dataset.listener = "1";
    presetClr.addEventListener("click", () => {
      useSafeTermsQueuePreset = false;
      loadDocuments().catch(() => {});
      renderTermsTable();
      syncUnifiedState();
    });
  }
  const selVis = $("#fi-term-btn-select-visible");
  if (selVis && !selVis.dataset.listener) {
    selVis.dataset.listener = "1";
    selVis.addEventListener("click", () => {
      for (const t of getVisibleTerms()) selectedTermIds.add(String(t.id));
      renderTermsTable();
      syncUnifiedState();
    });
  }
  const clr = $("#fi-term-btn-clear-selection");
  if (clr && !clr.dataset.listener) {
    clr.dataset.listener = "1";
    clr.addEventListener("click", () => {
      selectedTermIds.clear();
      renderTermsTable();
      syncUnifiedState();
    });
  }
  const runAud = $("#fi-term-btn-run-audit");
  if (runAud && !runAud.dataset.listener) {
    runAud.dataset.listener = "1";
    runAud.addEventListener("click", () => runTermsAuditDoc().catch((e) => alert(e.message || String(e))));
  }
  const mapBtn = (btnId, action) => {
    const el = document.getElementById(btnId);
    if (!el || el.dataset.listener) return;
    el.dataset.listener = "1";
    el.addEventListener("click", () => postBulkTerms(action).catch((e) => alert(e.message || String(e))));
  };
  mapBtn("fi-term-btn-bulk-clear-comm", "clear_commercial_review");
  mapBtn("fi-term-btn-bulk-clear-legal", "clear_legal_review");
  mapBtn("fi-term-btn-bulk-approve", "approve");
  mapBtn("fi-term-btn-bulk-reject", "reject");
  mapBtn("fi-term-btn-bulk-needs", "needs_review");
}

function syncUnifiedState() {
  state.documents = documents;
  state.selectedDocumentId = selectedId;
  const fromList = documents.find((x) => x.id === selectedId) || null;
  if (fromList && state.selectedDocument && String(state.selectedDocument.id) === String(selectedId)) {
    state.selectedDocument = { ...fromList, ...state.selectedDocument };
  } else {
    state.selectedDocument = fromList || state.selectedDocument;
  }
  state.feeRows = allRowsForDoc;
  state.termRows = allTermsForDoc;
  state.feeFilters = getFilterState();
  state.termFilters = getTermFilterState();
}

function auditStatusHistogram(rows, field) {
  const m = {};
  for (const r of rows) {
    const k = String(r[field] || "").trim() || "—";
    m[k] = (m[k] || 0) + 1;
  }
  return m;
}

function formatAuditHistogramForOverview(m) {
  return Object.entries(m)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10)
    .map(([k, v]) => `${escapeHtml(k)}: <strong>${v}</strong>`)
    .join(" · ");
}

function getFranchiseWorkspaceMetrics() {
  const fees = allRowsForDoc;
  const terms = allTermsForDoc;
  const feeApprovedCount = fees.filter((r) => String(r.reviewStatus || "").trim() === "Approved").length;
  const feeNeedsReviewCount = fees.filter((r) => String(r.reviewStatus || "").trim() === "Needs Review").length;
  const feeRejectedCount = fees.filter((r) => String(r.reviewStatus || "").trim() === "Rejected").length;
  const termApprovedCount = terms.filter((t) => String(t.reviewStatus || "").trim() === "Approved").length;
  const termNeedsReviewCount = terms.filter((t) => String(t.reviewStatus || "").trim() === "Needs Review").length;
  const termRejectedCount = terms.filter((t) => String(t.reviewStatus || "").trim() === "Rejected").length;
  const feeAutoEligibleCount = fees.filter((r) => r.autoApproveEligible === true).length;
  const termAutoEligibleCount = terms.filter((t) => t.autoApproveEligible === true).length;
  const feeLegalReviewCount = fees.filter((r) => r.needsLegalReview === true).length;
  const termLegalReviewCount = terms.filter((t) => t.legalReviewRequired === true).length;
  const feeDuplicateCount = fees.filter((r) => r.possibleDuplicate === true).length;
  const termDuplicateCount = terms.filter((t) => t.possibleDuplicateTerm === true).length;
  const feeCommercialNeedsCount = fees.filter((r) => r.needsCommercialReview === true).length;
  const termCommercialNeedsCount = terms.filter((t) => t.commercialReviewRequired === true).length;
  const feeBasisNeedsCount = fees.filter((r) => r.basisNeedsReview === true).length;
  const termHighRiskCount = terms.filter((t) => String(t.riskLevel || "").trim() === "High").length;
  const termLowFlexCount = terms.filter((t) => String(t.flexibilityLevel || "").trim() === "Low").length;

  const feeAuditHist = auditStatusHistogram(fees, "auditStatus");
  const termAuditHist = auditStatusHistogram(terms, "termAuditStatus");
  const feeAuditLine = fees.length ? formatAuditHistogramForOverview(feeAuditHist) : "";
  const termAuditLine = terms.length ? formatAuditHistogramForOverview(termAuditHist) : "";

  let readiness = "Not Started";
  const totalRows = fees.length + terms.length;
  if (totalRows === 0) readiness = "Not Started";
  else {
    const approved = feeApprovedCount + termApprovedCount;
    const needs = feeNeedsReviewCount + termNeedsReviewCount;
    const highRiskOpen = terms.filter(
      (t) => String(t.riskLevel || "") === "High" && String(t.reviewStatus || "").trim() !== "Approved"
    ).length;
    if (approved === 0) readiness = "Extracted / Needs Review";
    else if (
      (fees.length === 0 || feeApprovedCount >= Math.min(3, fees.length)) &&
      (terms.length === 0 || termApprovedCount >= Math.min(3, terms.length)) &&
      needs <= Math.ceil(totalRows * 0.25) &&
      highRiskOpen <= 2
    ) {
      readiness = "Review Ready";
    } else if (approved > 0) readiness = "Partially Reviewed";
    else readiness = "Extracted / Needs Review";
  }

  return {
    readiness,
    feeTotal: fees.length,
    termTotal: terms.length,
    feeApprovedCount,
    termApprovedCount,
    feeNeedsReviewCount,
    termNeedsReviewCount,
    feeRejectedCount,
    termRejectedCount,
    feeDuplicateCount,
    termDuplicateCount,
    feeAutoEligibleCount,
    termAutoEligibleCount,
    feeLegalReviewCount,
    termLegalReviewCount,
    feeCommercialNeedsCount,
    termCommercialNeedsCount,
    feeBasisNeedsCount,
    termHighRiskCount,
    termLowFlexCount,
    feeAuditLine,
    termAuditLine,
  };
}

function renderDocKpiStrip() {
  const wrap = $("#fi-doc-kpi-wrap");
  if (!wrap) return;
  if (!selectedId) return;
  const m = getFranchiseWorkspaceMetrics();
  const set = (id, v) => {
    const el = document.getElementById(id);
    if (el) el.textContent = v != null ? String(v) : "—";
  };
  set("fi-kpi-v-summary", m.readiness);
  set("fi-kpi-v-fee-total", m.feeTotal);
  set("fi-kpi-v-term-total", m.termTotal);
  set("fi-kpi-v-fee-approved", m.feeApprovedCount);
  set("fi-kpi-v-term-approved", m.termApprovedCount);
  set("fi-kpi-v-fee-needs", m.feeNeedsReviewCount);
  set("fi-kpi-v-term-needs", m.termNeedsReviewCount);
  set("fi-kpi-v-fee-dup", m.feeDuplicateCount);
  set("fi-kpi-v-term-dup", m.termDuplicateCount);
  set("fi-kpi-v-fee-auto", m.feeAutoEligibleCount);
  set("fi-kpi-v-term-auto", m.termAutoEligibleCount);
  set("fi-kpi-v-fee-legal", m.feeLegalReviewCount);
  set("fi-kpi-v-term-legal", m.termLegalReviewCount);
}

function renderOverviewPanel() {
  const host = $("#fi-overview-body");
  if (!host) return;
  const m = getFranchiseWorkspaceMetrics();

  host.innerHTML = `
    <p class="muted" style="margin:0 0 0.5rem;">Review readiness: <span class="fi-readiness-pill">${escapeHtml(m.readiness)}</span></p>
    <div class="fi-overview-section">
      <h4>Fees &amp; costs</h4>
      <div class="fi-overview-grid-stats">
        <div class="fi-overview-metric">Total <strong>${m.feeTotal}</strong></div>
        <div class="fi-overview-metric">Approved <strong>${m.feeApprovedCount}</strong></div>
        <div class="fi-overview-metric">Needs review <strong>${m.feeNeedsReviewCount}</strong></div>
        <div class="fi-overview-metric">Rejected <strong>${m.feeRejectedCount}</strong></div>
        <div class="fi-overview-metric">Auto-eligible <strong>${m.feeAutoEligibleCount}</strong></div>
        <div class="fi-overview-metric">Possible duplicate <strong>${m.feeDuplicateCount}</strong></div>
        <div class="fi-overview-metric">Legal review <strong>${m.feeLegalReviewCount}</strong></div>
        <div class="fi-overview-metric">Commercial review <strong>${m.feeCommercialNeedsCount}</strong></div>
        <div class="fi-overview-metric">Basis needs review <strong>${m.feeBasisNeedsCount}</strong></div>
      </div>
      ${m.feeAuditLine ? `<p class="muted" style="margin:0.45rem 0 0; font-size:0.78rem;">Audit status (fee rows): ${m.feeAuditLine}</p>` : ""}
    </div>
    <div class="fi-overview-section">
      <h4>Terms &amp; obligations</h4>
      <div class="fi-overview-grid-stats">
        <div class="fi-overview-metric">Total <strong>${m.termTotal}</strong></div>
        <div class="fi-overview-metric">Approved <strong>${m.termApprovedCount}</strong></div>
        <div class="fi-overview-metric">Needs review <strong>${m.termNeedsReviewCount}</strong></div>
        <div class="fi-overview-metric">Rejected <strong>${m.termRejectedCount}</strong></div>
        <div class="fi-overview-metric">Auto-eligible <strong>${m.termAutoEligibleCount}</strong></div>
        <div class="fi-overview-metric">Possible duplicate <strong>${m.termDuplicateCount}</strong></div>
        <div class="fi-overview-metric">Legal review <strong>${m.termLegalReviewCount}</strong></div>
        <div class="fi-overview-metric">Commercial review <strong>${m.termCommercialNeedsCount}</strong></div>
        <div class="fi-overview-metric">High risk <strong>${m.termHighRiskCount}</strong></div>
        <div class="fi-overview-metric">Low flexibility <strong>${m.termLowFlexCount}</strong></div>
      </div>
      ${m.termAuditLine ? `<p class="muted" style="margin:0.45rem 0 0; font-size:0.78rem;">Audit status (terms): ${m.termAuditLine}</p>` : ""}
    </div>
    <p style="margin-top:0.75rem;">
      <button type="button" class="ghost fi-jump-panel" data-fi-panel="fees">Open Fee &amp; Cost Review</button>
      <button type="button" class="ghost fi-jump-panel" data-fi-panel="terms">Open Terms &amp; Obligations Review</button>
    </p>
  `;
}

function showPanel(panel) {
  if (!["overview", "fees", "terms"].includes(panel)) return;
  state.activePanel = panel;
  for (const p of ["overview", "fees", "terms"]) {
    const el = document.getElementById(`fi-panel-${p}`);
    if (el) el.hidden = p !== panel;
    const tab = document.querySelector(`.fi-panel-tab[data-fi-panel="${p}"]`);
    if (tab) {
      tab.classList.toggle("active", p === panel);
      tab.setAttribute("aria-selected", p === panel ? "true" : "false");
    }
  }
  if (panel === "overview") renderOverviewPanel();
}

function wirePanelTabs() {
  const host = document.querySelector(".fi-panel-tabs");
  if (!host || host.dataset.bound) return;
  host.dataset.bound = "1";
  host.addEventListener("click", (e) => {
    const btn = e.target.closest(".fi-panel-tab[data-fi-panel]");
    if (!btn) return;
    showPanel(btn.getAttribute("data-fi-panel"));
  });
}

function wireExtractionSummaryJumps() {
  const sum = $("#full-extract-summary");
  if (!sum || sum.dataset.bound) return;
  sum.dataset.bound = "1";
  sum.addEventListener("click", (e) => {
    const b = e.target.closest(".fi-jump-panel");
    if (!b) return;
    const panel = b.getAttribute("data-fi-panel");
    if (panel) showPanel(panel);
  });
}

function wireOverviewJumps() {
  const ov = $("#fi-panel-overview");
  if (!ov || ov.dataset.boundJumps) return;
  ov.dataset.boundJumps = "1";
  ov.addEventListener("click", (e) => {
    const b = e.target.closest(".fi-jump-panel");
    if (!b) return;
    const panel = b.getAttribute("data-fi-panel");
    if (panel) showPanel(panel);
  });
}

async function selectDocument(id) {
  selectedId = id;
  clearRowSelection();
  selectedTermIds.clear();
  useSafeTermsQueuePreset = false;
  resetRowsToolUiForNewDocument();
  setBulkStatus("");
  setTermsBulkResult("");
  setActionSummary("");
  renderDocTable();
  $("#detail-empty").hidden = true;
  $("#detail-panel").hidden = false;
  const data = await fetchJson(`${API}/documents/${encodeURIComponent(id)}`);
  const d = data.document;
  state.selectedDocument = d;
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
  $("#extract-warnings").hidden = true;
  await loadRows(id, { preserveSelection: false });
  await loadTermsForSelected();
  await refreshFddFormatNote(id, d);
  syncUnifiedState();
  renderOverviewPanel();
  showPanel(state.activePanel || "overview");
  renderDocTable();
}

function setBulkStatus(html) {
  const el = $("#bulk-status");
  if (el) el.innerHTML = html;
}

function setActionSummary(html) {
  const raw = html != null ? String(html) : "";
  state.lastActionSummary = raw;
  const el = $("#fi-action-summary");
  if (!el) return;
  if (!raw.trim()) {
    el.hidden = true;
    el.innerHTML = "";
    return;
  }
  el.hidden = false;
  el.innerHTML = raw;
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
        syncUnifiedState();
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
    await loadTermsForSelected();
    try {
      const docData = await fetchJson(`${API}/documents/${encodeURIComponent(selectedId)}`);
      const d = docData.document;
      $("#d-meta").textContent = `${d.parentCompany || ""} · ${d.fddYear || ""} · ${d.country || ""} · ${d.extractionStatus || ""}`;
      await refreshFddFormatNote(selectedId, d);
    } catch (_) {
      /* ignore */
    }
    setActionSummary(buildFullExtractActionSummaryHtml(data));
    if (!data.ok && !data.partialSuccess) {
      alert(data.error || data.warnings?.[0] || "Full extraction failed");
    }
  } catch (err) {
    if (sumEl) {
      sumEl.style.display = "block";
      sumEl.innerHTML = `<p class="bulk-status-msg" style="color:#ffb4b8;">${escapeHtml(err.message || String(err))}</p>`;
    }
    setActionSummary(`<p class="bulk-status-msg" style="margin:0;color:#ffb4b8;">${escapeHtml(err.message || String(err))}</p>`);
    alert(err.message || String(err));
  } finally {
    btn.disabled = false;
  }
});

$("#btn-extract").addEventListener("click", async () => {
  if (!selectedId) return;
  try {
    const data = await fetchJson(`${API}/documents/${encodeURIComponent(selectedId)}/extract`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({}),
    });
    setActionSummary(
      `<p class="bulk-status-msg" style="margin:0;">Fees-only extraction finished. Rows created: <strong>${escapeHtml(
        String(data.rowsCreated ?? "—")
      )}</strong>.</p>`
    );
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
    setActionSummary(
      `<p class="bulk-status-msg" style="margin:0;">Fee audit: audited <strong>${data.auditedCount ?? 0}</strong> · high confidence <strong>${
        data.highConfidenceCount ?? 0
      }</strong> · quick <strong>${data.quickReviewCount ?? 0}</strong> · needs review <strong>${data.needsReviewCount ?? 0}</strong> · manual/blocked <strong>${
        data.manualReviewRequiredCount ?? 0
      }</strong> · auto-eligible <strong>${data.autoApproveEligibleCount ?? 0}</strong>.</p>`
    );
    await loadRows(selectedId, { preserveSelection: true });
  } catch (err) {
    alert(err.message || String(err));
  }
});

$("#btn-extract-terms")?.addEventListener("click", () => runExtractTerms().catch(alertErr));
$("#btn-audit-terms")?.addEventListener("click", () => runTermsAuditDoc().catch(alertErr));

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
    const skipHtml = Object.keys(skipped).length
      ? `<br />Skipped reasons: ${Object.entries(skipped)
          .map(([k, v]) => `${escapeHtml(k)}: <strong>${escapeHtml(String(v))}</strong>`)
          .join(" · ")}`
      : "";
    setActionSummary(
      `<p class="bulk-status-msg" style="margin:0;">Approved <strong>${data.approvedCount ?? 0}</strong> fee row(s). Skipped <strong>${data.skippedCount ?? 0}</strong>.${skipHtml}</p>`
    );
    clearRowSelection();
    await loadDocuments();
    await loadRows(selectedId, { preserveSelection: false });
  } catch (err) {
    alert(err.message || String(err));
  }
});

$("#btn-approve-auto-terms")?.addEventListener("click", () => runApproveAutoEligibleTerms().catch(alertErr));

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
wireTermsTable();
wireTermFilters();
wireTermBulkBar();
wirePanelTabs();
wireExtractionSummaryJumps();
wireOverviewJumps();
fillCategorySelect();
populateTermCategoryFilter();
populateTermSelectFromList("#f-term-bucket", NORMALIZED_TERM_BUCKETS);
populateTermSelectFromList("#f-term-comparable", COMPARABLE_TERM_GROUPS);
applyRowsViewDensity();
syncPresetChips();
showPanel(state.activePanel || "overview");

loadDocuments().catch(alertErr);
