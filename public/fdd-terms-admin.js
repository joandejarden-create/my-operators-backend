/**
 * FDD Terms & Obligations admin — same-origin /api/fdd-intelligence/* only (no secrets).
 */

import { runFddFullExtractWithMonitoring } from "/js/fdd-extract-full-progress-ui.js";

const API = "/api/fdd-intelligence";

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

let documents = [];
let selectedId = null;
let allTermsForDoc = [];
let lastTermsStorage = "";
let selectedIds = new Set();
let useSafeTermsQueuePreset = false;

const $ = (sel) => document.querySelector(sel);

async function fetchJson(url, options) {
  const res = await fetch(url, {
    ...options,
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
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
    `<strong>Full extraction</strong> · fees <strong>${f.rowsCreated ?? 0}</strong> (${f.ok ? "OK" : "failed"}) · terms <strong>${t.rowsCreated ?? 0}</strong> (${t.ok ? "OK" : "failed"})`
  );
  if (data.partialSuccess) lines.push(`<strong>Partial success</strong> — check fees.ok / terms.ok.`);
  const warn = [];
  if (Array.isArray(data.warnings)) for (const w of data.warnings) if (w) warn.push(String(w));
  if (Array.isArray(f.warnings)) for (const w of f.warnings) if (w) warn.push(`Fees: ${w}`);
  if (Array.isArray(t.warnings)) for (const w of t.warnings) if (w) warn.push(`Terms: ${w}`);
  if (warn.length) lines.push(escapeHtml(warn.slice(0, 10).join(" · ")));
  return lines.join("<br />");
}

function pillClass(status) {
  const s = String(status || "");
  if (s === "Approved") return "pill approved";
  if (s === "Rejected") return "pill rejected";
  if (s === "Needs Review") return "pill needs";
  return "pill draft";
}

function ynPill(v) {
  return v ? "pill yesno-yes" : "pill yesno-no";
}

function summarizeSkippedByReason(skipped) {
  const m = {};
  for (const s of skipped || []) {
    const r = String(s.reason || "unknown");
    m[r] = (m[r] || 0) + 1;
  }
  return m;
}

function formatSkippedSummary(skipped, label) {
  if (!skipped || !skipped.length) return "";
  const m = summarizeSkippedByReason(skipped);
  const parts = Object.keys(m).map((k) => `${k}: ${m[k]}`);
  return `${label} skipped (${skipped.length}): ` + parts.join(", ");
}

function setBulkResult(msg) {
  const el = $("#bulk-result");
  if (el) el.textContent = msg || "";
}

function populateCategoryFilter() {
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

function populateSelectFromList(selId, list) {
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

function safeTermsQueuePredicate(t) {
  if (String(t.reviewStatus || "").trim() !== "Needs Review") return false;
  const st = String(t.termAuditStatus || "").trim();
  if (st !== "High Confidence" && st !== "Quick Review") return false;
  if (!t.commercialReviewRequired) return false;
  if (t.legalReviewRequired) return false;
  if (t.possibleDuplicateTerm) return false;
  if (TERM_LEGAL_SENSITIVE_BLOCKED.has(String(t.termCategory || "").trim())) return false;
  if (String(t.normalizedTermBucket || "").trim() === "Other / Needs Mapping") return false;
  return true;
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

function renderDocTable() {
  const tb = $("#doc-table tbody");
  tb.innerHTML = "";
  for (const d of documents) {
    const tr = document.createElement("tr");
    if (d.id === selectedId) tr.classList.add("selected");
    tr.innerHTML = `
      <td>${escapeHtml(d.brandName || "")}</td>
      <td>${escapeHtml(String(d.fddYear ?? ""))}</td>
      <td>${escapeHtml(d.country || "")}</td>
    `;
    tr.addEventListener("click", () => selectDocument(d.id));
    tb.appendChild(tr);
  }
}

async function loadDocuments() {
  const data = await fetchJson(`${API}/documents`);
  documents = data.documents || [];
  let banner =
    data.storage === "airtable"
      ? "Documents / sections / fee rows: Airtable."
      : "Documents: in-memory or partial Airtable (see fee admin for fee table persistence).";
  if (lastTermsStorage) {
    banner += ` Terms storage: ${lastTermsStorage}.`;
  } else {
    banner += " Set AIRTABLE_TABLE_FDD_TERMS in .env to persist terms separately from fee rows.";
  }
  if (useSafeTermsQueuePreset) banner += " Preset: Safe terms review queue.";
  $("#storage-banner").textContent = banner;
  renderDocTable();
}

function syncCheckboxSelection(tr, termId, checked) {
  const cb = tr.querySelector('input[type="checkbox"][data-term-id]');
  if (cb) cb.checked = checked;
}

function renderTermsTable() {
  const tb = $("#terms-table tbody");
  tb.innerHTML = "";
  const rows = getVisibleTerms();
  $("#terms-count-label").textContent =
    selectedId && rows.length
      ? `${rows.length} visible / ${allTermsForDoc.length} total · ${selectedIds.size} selected`
      : selectedId
        ? `${allTermsForDoc.length} term(s) · ${selectedIds.size} selected`
        : "";

  for (const t of rows) {
    const tr = document.createElement("tr");
    const sum = String(t.termSummary || "").slice(0, 120);
    const iss = String(t.termAuditIssues || "").slice(0, 100);
    const id = String(t.id);
    const checked = selectedIds.has(id);
    tr.innerHTML = `
      <td><input type="checkbox" data-term-id="${escapeHtml(id)}" ${checked ? "checked" : ""} aria-label="Select term" /></td>
      <td class="cell-clip" title="${escapeHtml(t.termObligationName || "")}">${escapeHtml(t.termObligationName || "—")}</td>
      <td class="cell-clip-sm" title="${escapeHtml(t.termCategory || "")}">${escapeHtml(t.termCategory || "—")}</td>
      <td class="cell-clip-sm" title="${escapeHtml(t.normalizedTermBucket || "")}">${escapeHtml(t.normalizedTermBucket || "—")}</td>
      <td class="cell-clip-sm" title="${escapeHtml(t.comparableTermGroup || "")}">${escapeHtml(t.comparableTermGroup || "—")}</td>
      <td><span class="${ynPill(!!t.possibleDuplicateTerm)}">${t.possibleDuplicateTerm ? "Yes" : "No"}</span></td>
      <td>${escapeHtml(t.sourceItemNumber || "—")}</td>
      <td class="cell-clip" title="${escapeHtml(t.termSummary || "")}">${escapeHtml(sum)}${(t.termSummary || "").length > 120 ? "…" : ""}</td>
      <td>${escapeHtml(t.riskLevel || "—")}</td>
      <td>${escapeHtml(t.flexibilityLevel || "—")}</td>
      <td><span class="${ynPill(!!t.legalReviewRequired)}">${t.legalReviewRequired ? "Yes" : "No"}</span></td>
      <td><span class="${ynPill(!!t.commercialReviewRequired)}">${t.commercialReviewRequired ? "Yes" : "No"}</span></td>
      <td><span class="${pillClass(t.reviewStatus)}">${escapeHtml(t.reviewStatus || "")}</span></td>
      <td class="cell-num">${escapeHtml(String(t.termAuditScore ?? "—"))}</td>
      <td class="cell-clip-sm" title="${escapeHtml(t.termAuditStatus || "")}">${escapeHtml(t.termAuditStatus || "—")}</td>
      <td class="cell-clip-sm" title="${escapeHtml(t.termAuditIssues || "")}">${escapeHtml(iss)}${(t.termAuditIssues || "").length > 100 ? "…" : ""}</td>
      <td><span class="${ynPill(!!t.autoApproveEligible)}">${t.autoApproveEligible ? "Yes" : "No"}</span></td>
      <td class="actions-cell" data-term-id="${escapeHtml(id)}"></td>
    `;
    const cb = tr.querySelector('input[type="checkbox"][data-term-id]');
    cb.addEventListener("change", () => {
      if (cb.checked) selectedIds.add(id);
      else selectedIds.delete(id);
      $("#terms-count-label").textContent =
        selectedId && getVisibleTerms().length
          ? `${getVisibleTerms().length} visible / ${allTermsForDoc.length} total · ${selectedIds.size} selected`
          : "";
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
  await loadDocuments();
}

async function selectDocument(id) {
  selectedId = id;
  selectedIds.clear();
  useSafeTermsQueuePreset = false;
  renderDocTable();
  const d = documents.find((x) => x.id === id);
  $("#detail-empty").hidden = !!d;
  $("#detail-panel").hidden = !d;
  if (!d) {
    allTermsForDoc = [];
    renderTermsTable();
    return;
  }
  $("#d-brand").textContent = d.brandName || "";
  $("#d-meta").textContent = [d.parentCompany, d.fddYear, d.country].filter(Boolean).join(" · ");
  const fullSum = $("#full-extract-summary");
  if (fullSum) {
    fullSum.style.display = "none";
    fullSum.innerHTML = "";
  }
  setBulkResult("");
  await loadTermsForSelected();
}

async function runExtractFull() {
  if (!selectedId) return;
  const sumEl = $("#full-extract-summary");
  const btn = $("#btn-extract-full");
  btn.disabled = true;
  $("#extract-warnings").hidden = true;
  try {
    const job = await runFddFullExtractWithMonitoring({ apiBase: API, documentId: selectedId });
    const data = job && job.result ? job.result : {};
    if (sumEl) {
      sumEl.style.display = "block";
      sumEl.innerHTML = renderFullExtractionSummaryHtml(data);
    }
    await loadTermsForSelected();
    await loadDocuments();
    if (!data.ok && !data.partialSuccess) {
      alert(data.error || data.warnings?.[0] || "Full extraction failed");
    }
  } catch (e) {
    if (sumEl) {
      sumEl.style.display = "block";
      sumEl.innerHTML = `<p class="bulk-status-msg" style="color:#ffb4b8;">${escapeHtml(e.message || String(e))}</p>`;
    }
    alert(e.message || String(e));
  } finally {
    $("#btn-extract-full").disabled = false;
  }
}

async function runExtractTerms() {
  if (!selectedId) return;
  $("#btn-extract-terms").disabled = true;
  $("#extract-warnings").hidden = true;
  const fullSum = $("#full-extract-summary");
  if (fullSum) {
    fullSum.style.display = "none";
    fullSum.innerHTML = "";
  }
  try {
    const data = await fetchJson(`${API}/documents/${encodeURIComponent(selectedId)}/extract-terms`, {
      method: "POST",
      body: JSON.stringify({}),
    });
    lastTermsStorage = data.storage || "";
    allTermsForDoc = data.terms || [];
    const w = data.warnings;
    const box = $("#extract-warnings");
    if (Array.isArray(w) && w.length) {
      box.hidden = false;
      box.innerHTML = "<strong>Run notes:</strong><br />" + w.map((x) => escapeHtml(x)).join("<br />");
    } else {
      box.hidden = true;
      box.textContent = "";
    }
    renderTermsTable();
    await loadDocuments();
  } finally {
    $("#btn-extract-terms").disabled = false;
  }
}

async function runTermsAudit() {
  if (!selectedId) return;
  $("#btn-audit-terms").disabled = true;
  try {
    const data = await fetchJson(`${API}/documents/${encodeURIComponent(selectedId)}/audit-terms`, {
      method: "POST",
      body: "{}",
    });
    const by = data.summary && data.summary.byStatus;
    const statusLine =
      by && typeof by === "object" ? ` By audit status: ${Object.entries(by).map(([k, v]) => `${k}: ${v}`).join(", ")}.` : "";
    setBulkResult(`Audited ${data.termCount ?? 0} term(s).${statusLine}`);
    await loadTermsForSelected();
  } catch (e) {
    alert(e.message || String(e));
  } finally {
    $("#btn-audit-terms").disabled = false;
  }
}

async function runApproveAutoEligibleTerms() {
  if (!selectedId) return;
  if (!confirm("Approve every term that is Auto-Approve Eligible after re-audit? Legal-sensitive rows stay excluded by server rules.")) return;
  $("#btn-approve-auto-terms").disabled = true;
  try {
    const data = await fetchJson(`${API}/documents/${encodeURIComponent(selectedId)}/approve-auto-eligible-terms`, {
      method: "POST",
      body: "{}",
    });
    const lines = [`Approved ${data.approvedCount ?? 0}.`, formatSkippedSummary(data.skipped, "Not auto-approved")].filter(Boolean);
    setBulkResult(lines.join(" "));
    await loadTermsForSelected();
  } catch (e) {
    alert(e.message || String(e));
  } finally {
    $("#btn-approve-auto-terms").disabled = false;
  }
}

async function postBulkTerms(action) {
  const ids = [...selectedIds];
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
  const data = await fetchJson(`${API}/documents/${encodeURIComponent(selectedId)}/bulk-update-terms`, {
    method: "POST",
    body: JSON.stringify({ termIds: ids, action }),
  });
  selectedIds.clear();
  const bits = [`Updated ${data.updatedCount ?? 0}.`, data.skippedCount ? formatSkippedSummary(data.skipped, "Skipped rows") : ""];
  setBulkResult(bits.filter(Boolean).join(" "));
  await loadTermsForSelected();
}

function wireFilters() {
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
    if (el) el.addEventListener("change", () => renderTermsTable());
  }
}

$("#btn-refresh").addEventListener("click", () => loadDocuments().catch((e) => alert(e.message)));
$("#btn-extract-full").addEventListener("click", () => runExtractFull().catch((e) => alert(e.message)));
$("#btn-extract-terms").addEventListener("click", () => runExtractTerms().catch((e) => alert(e.message)));
$("#btn-audit-terms").addEventListener("click", () => runTermsAudit().catch((e) => alert(e.message)));
$("#btn-approve-auto-terms").addEventListener("click", () => runApproveAutoEligibleTerms().catch((e) => alert(e.message)));

$("#btn-preset-safe").addEventListener("click", () => {
  useSafeTermsQueuePreset = true;
  loadDocuments().catch(() => {});
  renderTermsTable();
});
$("#btn-preset-clear").addEventListener("click", () => {
  useSafeTermsQueuePreset = false;
  loadDocuments().catch(() => {});
  renderTermsTable();
});

$("#btn-select-visible").addEventListener("click", () => {
  for (const t of getVisibleTerms()) selectedIds.add(String(t.id));
  renderTermsTable();
});
$("#btn-clear-selection").addEventListener("click", () => {
  selectedIds.clear();
  renderTermsTable();
});

$("#btn-bulk-clear-comm").addEventListener("click", () =>
  postBulkTerms("clear_commercial_review").catch((e) => alert(e.message))
);
$("#btn-bulk-clear-legal").addEventListener("click", () =>
  postBulkTerms("clear_legal_review").catch((e) => alert(e.message))
);
$("#btn-bulk-approve").addEventListener("click", () => postBulkTerms("approve").catch((e) => alert(e.message)));
$("#btn-bulk-reject").addEventListener("click", () => postBulkTerms("reject").catch((e) => alert(e.message)));
$("#btn-bulk-needs").addEventListener("click", () => postBulkTerms("needs_review").catch((e) => alert(e.message)));

populateCategoryFilter();
populateSelectFromList("#f-term-bucket", NORMALIZED_TERM_BUCKETS);
populateSelectFromList("#f-term-comparable", COMPARABLE_TERM_GROUPS);
wireFilters();

loadDocuments().catch((e) => {
  $("#storage-banner").textContent = e.message || "Failed to load";
});
