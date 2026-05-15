/**
 * Brand comparison — per-brand GET …/brands/:name/economics and …/terms (no secrets).
 */

const API = "/api/fdd-intelligence";

const FEE_CATEGORY_ORDER = [
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

const TERM_BUCKET_ORDER = [
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

/** @type {string[]} */
let brandNames = [];
/** @type {string[]} Brands from FDD documents registry (for dropdowns). */
let registryBrandNames = [];
/** @type {Map<string, object[]>} */
const feeRowsByBrand = new Map();
/** @type {Map<string, object[]>} */
const termRowsByBrand = new Map();
let meta = { includeNeedsReview: false, fddYear: null };
let activeTab = "overview";

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

function csvEscape(s) {
  const t = String(s ?? "");
  if (/[",\n\r]/.test(t)) return `"${t.replace(/"/g, '""')}"`;
  return t;
}

function str(v) {
  return v == null ? "" : String(v);
}

function parseBrands(raw) {
  return String(raw || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

function getSelectedBrands() {
  const a = ($("#bc-brand-a") && $("#bc-brand-a").value.trim()) || "";
  const b = ($("#bc-brand-b") && $("#bc-brand-b").value.trim()) || "";
  const brands = [];
  if (a) brands.push(a);
  if (b && b !== a) brands.push(b);
  if (!a && b) brands.push(b);
  return brands;
}

async function loadRegistry() {
  const data = await fetchJson(`${API}/documents`);
  const docs = data.documents || [];
  const set = new Set();
  for (const d of docs) {
    const n = str(d.brandName).trim();
    if (n) set.add(n);
  }
  registryBrandNames = [...set].sort((a, b) => a.localeCompare(b, undefined, { sensitivity: "base" }));
}

function fillBrandSelect(sel, current) {
  if (!sel) return;
  const cur = str(current).trim();
  sel.textContent = "";
  const def = document.createElement("option");
  def.value = "";
  def.textContent = "Select brand…";
  sel.appendChild(def);
  for (const n of registryBrandNames) {
    const o = document.createElement("option");
    o.value = n;
    o.textContent = n;
    sel.appendChild(o);
  }
  if (cur && registryBrandNames.includes(cur)) sel.value = cur;
}

function populateBrandSelects() {
  const selA = $("#bc-brand-a");
  const selB = $("#bc-brand-b");
  const a = selA ? selA.value : "";
  const b = selB ? selB.value : "";
  fillBrandSelect(selA, a);
  fillBrandSelect(selB, b);
}

function feeBucketKey(r) {
  const nb = str(r.normalizedFeeBucket).trim();
  if (nb) return nb;
  const cat = str(r.commercialCategory).trim();
  if (cat) return cat;
  return "Other / Needs Mapping";
}

function feeComparableMeta(r) {
  return (
    str(r.comparableFeeGroup).trim() ||
    str(r.comparableStackGroup).trim() ||
    str(r.comparableGroup).trim() ||
    "—"
  );
}

function termBucketKey(t) {
  const nb = str(t.normalizedTermBucket).trim();
  if (nb) return nb;
  const cat = str(t.termCategory).trim();
  if (cat) return cat;
  return "Other / Needs Mapping";
}

function sortFeeBuckets(names) {
  const set = new Set(names);
  const out = [];
  for (const c of FEE_CATEGORY_ORDER) if (set.has(c)) out.push(c);
  const rest = [...set].filter((x) => !out.includes(x));
  const LAST = "Other / Needs Mapping";
  const mid = rest.filter((x) => x !== LAST).sort((a, b) => a.localeCompare(b, undefined, { sensitivity: "base" }));
  const end = rest.includes(LAST) ? [...mid, LAST] : mid;
  return [...out, ...end];
}

function termBucketRank(b) {
  const i = TERM_BUCKET_ORDER.indexOf(b);
  return i >= 0 ? i : 999;
}

function sortTermBuckets(names) {
  return [...new Set(names)].sort((a, b) => {
    const ra = termBucketRank(a);
    const rb = termBucketRank(b);
    if (ra !== rb) return ra - rb;
    return a.localeCompare(b, undefined, { sensitivity: "base" });
  });
}

function setStatus(msg, cls) {
  const el = $("#bc-status");
  if (!el) return;
  el.textContent = msg || "";
  el.className = cls || "";
}

function reviewPill(rs) {
  const s = str(rs).trim();
  if (s === "Approved") return '<span class="pill" style="background:rgba(20,202,116,0.18);color:#7bed9f;">Approved</span>';
  if (s === "Needs Review") return '<span class="pill warn">Needs Review</span>';
  return `<span class="pill">${escapeHtml(s || "—")}</span>`;
}

function buildQuery(includeNeeds, year) {
  const p = new URLSearchParams();
  if (includeNeeds) p.set("includeNeedsReview", "1");
  if (year != null && String(year).trim() !== "" && Number.isFinite(Number(year))) p.set("fddYear", String(parseInt(String(year), 10)));
  const qs = p.toString();
  return qs ? `?${qs}` : "";
}

async function fetchJson(url) {
  const res = await fetch(url, { headers: { Accept: "application/json" } });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || res.statusText || "Request failed");
  return data;
}

async function loadAllData() {
  brandNames = getSelectedBrands();
  const includeNeeds = ($("#review-mode") && $("#review-mode").value) === "include_needs";
  const yearRaw = ($("#fdd-year") && $("#fdd-year").value.trim()) || "";
  const year = yearRaw && Number.isFinite(Number(yearRaw)) ? parseInt(yearRaw, 10) : null;
  meta = { includeNeedsReview: includeNeeds, fddYear: year };

  feeRowsByBrand.clear();
  termRowsByBrand.clear();

  if (!brandNames.length) {
    setStatus("Select at least one brand from the dropdowns.", "error");
    return false;
  }

  const q = buildQuery(includeNeeds, year);
  setStatus("Loading…", "muted");

  const errors = [];
  await Promise.all(
    brandNames.map(async (brand) => {
      try {
        const enc = encodeURIComponent(brand);
        const [econ, terms] = await Promise.all([
          fetchJson(`${API}/brands/${enc}/economics${q}`),
          fetchJson(`${API}/brands/${enc}/terms${q}`),
        ]);
        feeRowsByBrand.set(brand, econ.economics || []);
        termRowsByBrand.set(brand, terms.terms || []);
      } catch (e) {
        errors.push(`${brand}: ${e.message || String(e)}`);
        feeRowsByBrand.set(brand, []);
        termRowsByBrand.set(brand, []);
      }
    })
  );

  if (errors.length) setStatus(`Loaded with issues: ${errors.join(" · ")}`, "error");
  else {
    const nFee = brandNames.reduce((a, b) => a + (feeRowsByBrand.get(b) || []).length, 0);
    const nTerm = brandNames.reduce((a, b) => a + (termRowsByBrand.get(b) || []).length, 0);
    setStatus(
      `Loaded ${brandNames.length} brand(s) · ${nFee} economics row(s) · ${nTerm} term row(s)${year != null ? ` · FDD year ${year}` : ""}${
        includeNeeds ? " · including Needs Review" : " · approved only"
      }.`,
      "muted"
    );
  }

  syncUrl();
  return true;
}

function syncUrl() {
  const q = new URLSearchParams();
  const a = ($("#bc-brand-a") && $("#bc-brand-a").value.trim()) || "";
  const b = ($("#bc-brand-b") && $("#bc-brand-b").value.trim()) || "";
  if (a) q.set("brandA", a);
  if (b) q.set("brandB", b);
  const y = ($("#fdd-year") && $("#fdd-year").value.trim()) || "";
  if (y) q.set("fddYear", y);
  if (($("#review-mode") && $("#review-mode").value) === "include_needs") q.set("includeNeedsReview", "1");
  const qs = q.toString();
  history.replaceState(null, "", "/fdd-brand-comparison.html" + (qs ? `?${qs}` : ""));
}

function applyUrl() {
  const q = new URLSearchParams(window.location.search);
  const selA = $("#bc-brand-a");
  const selB = $("#bc-brand-b");
  const brandA = q.get("brandA");
  const brandB = q.get("brandB");
  const legacy = q.get("brands");
  if (brandA && selA) selA.value = brandA;
  if (brandB && selB) selB.value = brandB;
  if (!brandA && legacy && selA) {
    const parts = parseBrands(legacy);
    if (parts[0]) selA.value = parts[0];
    if (parts[1] && selB) selB.value = parts[1];
  }
  const y = q.get("fddYear");
  if (y && $("#fdd-year")) $("#fdd-year").value = y;
  if ($("#review-mode")) {
    $("#review-mode").value = q.get("includeNeedsReview") === "1" ? "include_needs" : "approved";
  }
}

function metricsForBrand(brand) {
  const fees = feeRowsByBrand.get(brand) || [];
  const terms = termRowsByBrand.get(brand) || [];
  const appF = fees.filter((r) => str(r.reviewStatus).trim() === "Approved").length;
  const needF = fees.filter((r) => str(r.reviewStatus).trim() === "Needs Review").length;
  const appT = terms.filter((t) => str(t.reviewStatus).trim() === "Approved").length;
  const needT = terms.filter((t) => str(t.reviewStatus).trim() === "Needs Review").length;
  return {
    feeVisible: fees.length,
    termVisible: terms.length,
    feeApproved: appF,
    feeNeeds: needF,
    termApproved: appT,
    termNeeds: needT,
    dupFee: fees.filter((r) => r.possibleDuplicate === true).length,
    dupTerm: terms.filter((t) => t.possibleDuplicateTerm === true).length,
    basisRev: fees.filter((r) => r.basisNeedsReview === true).length,
    legalTerm: terms.filter((t) => t.legalReviewRequired === true).length,
    highRisk: terms.filter((t) => str(t.riskLevel).trim() === "High").length,
    lowFlex: terms.filter((t) => str(t.flexibilityLevel).trim() === "Low").length,
  };
}

function comparisonReadiness(m) {
  const unresolved = m.dupFee + m.dupTerm + m.basisRev + m.legalTerm + m.highRisk;
  const approvedTotal = m.feeApproved + m.termApproved;
  if (m.feeApproved === 0 && m.termApproved === 0) return "Not enough approved data";
  if (approvedTotal > 0 && unresolved > approvedTotal * 0.55 && (m.feeNeeds + m.termNeeds) > approvedTotal * 0.4) return "Needs review";
  if ((m.feeApproved > 0 && m.termApproved === 0) || (m.feeApproved === 0 && m.termApproved > 0)) return "Partial comparison ready";
  if (m.feeApproved > 0 && m.termApproved > 0 && unresolved <= Math.max(3, approvedTotal * 0.35)) return "Comparison ready";
  if (m.feeApproved > 0 || m.termApproved > 0) return "Partial comparison ready";
  return "Not enough approved data";
}

function recurringPctStackLine(brand) {
  const fees = feeRowsByBrand.get(brand) || [];
  const includeNeeds = meta.includeNeedsReview;
  const isRoomBasis = (b) => {
    const s = str(b).toLowerCase();
    return s.includes("gross room") || s.includes("room revenue") || s.includes("room sales") || s.includes("rooms revenue");
  };
  const isPctFormula = (r) => {
    const ft = str(r.amountFormulaType).toLowerCase();
    return ft.includes("percent") || (r.percentageRate != null && str(r.percentageRate).trim() !== "");
  };
  const parsePct = (rate) => {
    const s = str(rate).replace(/%/g, "").trim();
    const n = parseFloat(s);
    return Number.isFinite(n) ? n : null;
  };
  const looksRecurring = (r) => {
    const c = str(r.commercialCategory).toLowerCase();
    const n = str(r.feeOrObligationName).toLowerCase();
    const blob = c + " " + n;
    return /recurring|brand fee|marketing|loyalty|reservation|program|revenue management|sales support|contribution/i.test(blob);
  };

  const stackers = [];
  for (const r of fees) {
    const rs = str(r.reviewStatus).trim();
    if (rs === "Rejected") continue;
    if (!includeNeeds && rs !== "Approved") continue;
    if (includeNeeds && rs !== "Approved" && rs !== "Needs Review") continue;
    if (r.possibleDuplicate === true || r.potentiallyOverlappingFee === true) continue;
    if (r.basisNeedsReview === true) continue;
    if (!isPctFormula(r) || !isRoomBasis(r.normalizedCostBasis || r.basis)) continue;
    const p = parsePct(r.percentageRate);
    if (p == null || !looksRecurring(r)) continue;
    stackers.push({ p, name: str(r.feeOrObligationName) });
  }
  if (!stackers.length) {
    return "Not enough approved, clear-basis rows to calculate a recurring percentage stack (indicative).";
  }
  const sum = stackers.reduce((a, x) => a + x.p, 0);
  return `${brand} has ${stackers.length} clear percentage-based recurring row(s) totaling ${sum.toFixed(2)}% of room revenue (indicative), subject to review of bundling and applicability.`;
}

function econStackCounts(brand) {
  const fees = feeRowsByBrand.get(brand) || [];
  const ok = (r) => str(r.reviewStatus).trim() !== "Rejected";
  const vis = fees.filter(ok);
  let corePct = 0;
  let annualFixed = 0;
  let monthlyTech = 0;
  let oneTime = 0;
  let variable = 0;
  let basisNeed = 0;
  let dupOverlap = 0;
  for (const r of vis) {
    if (r.basisNeedsReview) basisNeed++;
    if (r.possibleDuplicate || r.potentiallyOverlappingFee) dupOverlap++;
    const lc = str(r.lifecyclePhase).toLowerCase();
    const ft = str(r.amountFormulaType).toLowerCase();
    const cat = str(r.commercialCategory).toLowerCase();
    if (ft.includes("percent") && /recurring|brand fee|marketing|loyalty|reservation/i.test(cat + str(r.feeOrObligationName).toLowerCase())) {
      corePct++;
    } else if (lc.includes("ongoing") || str(r.frequency).toLowerCase().includes("annual")) {
      annualFixed++;
    } else if (/system|technology|pms|software/i.test(str(r.feeOrObligationName)) && /month|per property|per month/i.test(str(r.frequency).toLowerCase())) {
      monthlyTech++;
    } else if (lc.includes("initial") || lc.includes("opening") || lc.includes("one")) {
      oneTime++;
    } else if (/pass|variable|actual/i.test(str(r.passThroughStatus).toLowerCase() + ft)) {
      variable++;
    }
  }
  return { corePct, annualFixed, monthlyTech, oneTime, variable, basisNeed, dupOverlap, total: vis.length };
}

function renderOverview() {
  const host = $("#bc-panel-overview");
  if (!host) return;
  if (!brandNames.length) {
    host.innerHTML = "<p class=\"muted\">Select one or two brands from the dropdowns, then load comparison.</p>";
    return;
  }
  const single = brandNames.length === 1;
  const parts = [];
  if (single) {
    parts.push('<p class="muted">Choose a second brand in Brand B for a side-by-side column.</p>');
  }
  parts.push('<div class="kpi-grid">');
  for (const b of brandNames) {
    const m = metricsForBrand(b);
    const readiness = comparisonReadiness(m);
    const pctLine = recurringPctStackLine(b);
    const unresolved = m.dupFee + m.dupTerm + m.basisRev + m.legalTerm + m.highRisk;
    parts.push(`
      <div class="kpi-card">
        <h4>${escapeHtml(b)}</h4>
        <div class="readiness">${escapeHtml(readiness)}</div>
        <dl>
          <dt>Fee rows (visible)</dt><dd>${m.feeVisible}</dd>
          <dt>Terms (visible)</dt><dd>${m.termVisible}</dd>
          <dt>Approved fees</dt><dd>${m.feeApproved}</dd>
          <dt>Approved terms</dt><dd>${m.termApproved}</dd>
          <dt>Needs review fees</dt><dd>${m.feeNeeds}</dd>
          <dt>Needs review terms</dt><dd>${m.termNeeds}</dd>
          <dt>Possible duplicate fees</dt><dd>${m.dupFee}</dd>
          <dt>Possible duplicate terms</dt><dd>${m.dupTerm}</dd>
          <dt>Basis review (fees)</dt><dd>${m.basisRev}</dd>
          <dt>Legal review (terms)</dt><dd>${m.legalTerm}</dd>
          <dt>High risk terms</dt><dd>${m.highRisk}</dd>
          <dt>Low flexibility terms</dt><dd>${m.lowFlex}</dd>
          <dt>Key unresolved (heuristic)</dt><dd>${unresolved}</dd>
        </dl>
        <p class="muted" style="margin-top:0.5rem;font-size:0.75rem;line-height:1.45;">${escapeHtml(pctLine)}</p>
      </div>
    `);
  }
  parts.push("</div>");
  host.innerHTML = parts.join("");
}

/** One economics line item across brands (union of bucket + fee name). */
function collectEconomicsCompareEntries() {
  /** @type {Map<string, { bucket: string; item: string; rowsByBrand: Map<string, object[]> }>} */
  const byKey = new Map();
  for (const b of brandNames) {
    for (const r of feeRowsByBrand.get(b) || []) {
      const bucket = feeBucketKey(r);
      const item = str(r.feeOrObligationName).trim() || "(Unnamed fee)";
      const key = `${bucket}\u0000${item}`;
      if (!byKey.has(key)) byKey.set(key, { bucket, item, rowsByBrand: new Map() });
      const e = byKey.get(key);
      if (!e.rowsByBrand.has(b)) e.rowsByBrand.set(b, []);
      e.rowsByBrand.get(b).push(r);
    }
  }
  const bucketOrder = sortFeeBuckets([...new Set([...byKey.values()].map((v) => v.bucket))]);
  const bucketIdx = (bk) => {
    const i = bucketOrder.indexOf(bk);
    return i >= 0 ? i : 999;
  };
  return [...byKey.values()].sort((A, B) => {
    const d = bucketIdx(A.bucket) - bucketIdx(B.bucket);
    if (d !== 0) return d;
    return A.item.localeCompare(B.item, undefined, { sensitivity: "base" });
  });
}

function feeLeftMetricHtml(entry) {
  const cats = new Set();
  const bases = new Set();
  let comp = "";
  for (const b of brandNames) {
    for (const r of entry.rowsByBrand.get(b) || []) {
      const c = str(r.commercialCategory).trim();
      if (c) cats.add(c);
      const bs = str(r.normalizedCostBasis || r.basis).trim();
      if (bs) bases.add(bs);
      if (!comp) comp = feeComparableMeta(r);
    }
  }
  const catLine = cats.size === 1 ? [...cats][0] : cats.size > 1 ? "Various categories" : entry.bucket;
  const catSub = cats.size === 1 ? entry.bucket : [...cats].slice(0, 3).join(" · ") || entry.bucket;
  const basisLine = [...bases].join(" · ") || "—";
  return `<div class="fdd-left-cat">${escapeHtml(catLine)}</div>
    <div class="fdd-left-basis" style="margin-top:0.15rem;">${escapeHtml(catSub)}</div>
    <div class="fdd-left-item">${escapeHtml(entry.item)}</div>
    <div class="fdd-left-basis">Cost basis: ${escapeHtml(basisLine)}${
    comp && comp !== "—" ? ` · Comparable: ${escapeHtml(comp)}` : ""
  }</div>`;
}

function feeNotesHtml(entry) {
  const notes = new Set();
  for (const b of brandNames) {
    for (const r of entry.rowsByBrand.get(b) || []) {
      if (str(r.comparabilityNotes).trim()) notes.add(str(r.comparabilityNotes).trim().slice(0, 200));
      if (str(r.auditIssues).trim()) notes.add(str(r.auditIssues).trim().slice(0, 150));
      if (str(r.comparableAgainst).trim()) notes.add(`vs ${str(r.comparableAgainst).trim().slice(0, 80)}`);
    }
  }
  return escapeHtml([...notes].slice(0, 5).join(" · ") || "—");
}

function feeBrandCellHtml(rows) {
  if (!rows || !rows.length) return `<td class="fdd-brand-cell muted">—</td>`;
  const blocks = rows.map((r) => {
    const amtRaw =
      str(r.amount).trim() ||
      [
        r.percentageRate != null && str(r.percentageRate).trim() !== "" ? `${str(r.percentageRate)}%` : "",
        r.fixedAmount != null ? `fixed ${str(r.fixedAmount)}` : "",
        r.unitRate != null ? `unit ${str(r.unitRate)}` : "",
      ]
        .filter(Boolean)
        .join(" · ") ||
      "—";
    const warn = r.possibleDuplicate || r.potentiallyOverlappingFee ? '<span class="pill dup">Overlap</span> ' : "";
    const line2 = [str(r.amountFormulaType), str(r.frequency), str(r.lifecyclePhase)].filter((x) => x).join(" · ");
    const bun = str(r.bundlingStatus).trim();
    return `<div class="fdd-econ-block" style="margin-bottom:0.55rem;">${warn}<div class="fdd-cost-primary">${escapeHtml(
      amtRaw
    )}</div>${line2 ? `<div class="fdd-cell-detail">${escapeHtml(line2)}</div>` : ""}${
      bun ? `<div class="fdd-cell-detail">${escapeHtml(bun)}</div>` : ""
    }<div class="fdd-cell-detail">${reviewPill(r.reviewStatus)} · ${escapeHtml(str(r.auditStatus) || "—")}</div><div class="fdd-cell-detail">Item ${escapeHtml(
      str(r.sourceItemNumber) || "—"
    )} · ${escapeHtml(str(r.documentationReference).slice(0, 44))}${str(r.documentationReference).length > 44 ? "…" : ""}</div></div>`;
  });
  return `<td class="fdd-brand-cell">${blocks.join("")}</td>`;
}

function renderEconomicsMatrix() {
  const host = $("#bc-panel-economics");
  if (!host) return;
  if (!brandNames.length) {
    host.innerHTML = "<p class=\"muted\">Load comparison first.</p>";
    return;
  }

  const entries = collectEconomicsCompareEntries();
  if (!entries.length) {
    host.innerHTML =
      '<p class="muted">No approved economics rows found. Try Include Needs Review or review fee rows in Franchise Intelligence Admin.</p>';
    return;
  }

  const stackBlocks = brandNames
    .map((br) => {
      const c = econStackCounts(br);
      return `<div class="stack-summary"><h4>${escapeHtml(br)} — indicative stack counts</h4>
        Core recurring % rows (heuristic): <strong>${c.corePct}</strong> · Annual fixed: <strong>${c.annualFixed}</strong> · Monthly/property tech: <strong>${c.monthlyTech}</strong> · One-time/upfront: <strong>${c.oneTime}</strong> · Variable/pass-through/unclear: <strong>${c.variable}</strong> · Basis needs review: <strong>${c.basisNeed}</strong> · Duplicate/overlap flags: <strong>${c.dupOverlap}</strong></div>`;
    })
    .join("");

  const thead = `<tr><th class="fdd-metric-th">Category · Item · cost basis</th>${brandNames
    .map((b) => `<th class="fdd-brand-th">${escapeHtml(b)}</th>`)
    .join("")}<th class="fdd-notes-th">Notes</th></tr>`;

  const rows = entries.map(
    (entry) =>
      `<tr><td class="fdd-metric-cell">${feeLeftMetricHtml(entry)}</td>${brandNames
        .map((b) => feeBrandCellHtml(entry.rowsByBrand.get(b)))
        .join("")}<td class="fdd-notes-cell">${feeNotesHtml(entry)}</td></tr>`
  );

  host.innerHTML = `
    ${stackBlocks}
    <p class="econ-note">Each row is one fee line item. Category, item, and cost basis stay on the left; each brand column shows that brand&rsquo;s amount and details. Bundled or overlapping rows should be reviewed before relying on totals.</p>
    <div class="fdd-compare-wrap"><table class="fdd-compare-table"><thead>${thead}</thead><tbody>${rows.join("")}</tbody></table></div>
  `;
}

function collectTermsCompareEntries() {
  /** @type {Map<string, { bucket: string; item: string; rowsByBrand: Map<string, object[]> }>} */
  const byKey = new Map();
  for (const b of brandNames) {
    for (const t of termRowsByBrand.get(b) || []) {
      const bucket = termBucketKey(t);
      const item = str(t.termObligationName).trim() || "(Unnamed term)";
      const key = `${bucket}\u0000${item}`;
      if (!byKey.has(key)) byKey.set(key, { bucket, item, rowsByBrand: new Map() });
      const e = byKey.get(key);
      if (!e.rowsByBrand.has(b)) e.rowsByBrand.set(b, []);
      e.rowsByBrand.get(b).push(t);
    }
  }
  const bucketOrder = sortTermBuckets([...new Set([...byKey.values()].map((v) => v.bucket))]);
  const bucketIdx = (bk) => {
    const i = bucketOrder.indexOf(bk);
    return i >= 0 ? i : 999;
  };
  return [...byKey.values()].sort((A, B) => {
    const d = bucketIdx(A.bucket) - bucketIdx(B.bucket);
    if (d !== 0) return d;
    return A.item.localeCompare(B.item, undefined, { sensitivity: "base" });
  });
}

function termLeftMetricHtml(entry) {
  const tCats = new Set();
  let comp = "";
  for (const b of brandNames) {
    for (const t of entry.rowsByBrand.get(b) || []) {
      const c = str(t.termCategory).trim();
      if (c) tCats.add(c);
      if (!comp) comp = str(t.comparableTermGroup).trim();
    }
  }
  const catLine = tCats.size === 1 ? [...tCats][0] : tCats.size > 1 ? "Various categories" : entry.bucket;
  const catSub = tCats.size === 1 ? entry.bucket : [...tCats].slice(0, 3).join(" · ") || entry.bucket;
  return `<div class="fdd-left-cat">${escapeHtml(catLine)}</div>
    <div class="fdd-left-basis" style="margin-top:0.15rem;">${escapeHtml(catSub)}</div>
    <div class="fdd-left-item">${escapeHtml(entry.item)}</div>
    <div class="fdd-left-basis">${comp ? `Comparable: ${escapeHtml(comp)}` : "—"}</div>`;
}

function termNotesHtml(entry) {
  const notes = new Set();
  for (const b of brandNames) {
    for (const t of entry.rowsByBrand.get(b) || []) {
      if (str(t.termAuditIssues).trim()) notes.add(str(t.termAuditIssues).trim().slice(0, 160));
      if (t.possibleDuplicateTerm) notes.add("Possible duplicate term");
    }
  }
  return escapeHtml([...notes].slice(0, 5).join(" · ") || "—");
}

function termBrandCellHtml(rows) {
  if (!rows || !rows.length) return `<td class="fdd-brand-cell muted">—</td>`;
  const blocks = rows.map((t) => {
    const sum = str(t.termSummary).trim();
    const sumHtml = escapeHtml(sum.slice(0, 220)) + (sum.length > 220 ? "…" : "");
    const dup = t.possibleDuplicateTerm ? '<span class="pill dup">Duplicate</span> ' : "";
    return `<div class="fdd-econ-block" style="margin-bottom:0.55rem;">${dup}<div class="fdd-cost-primary">${escapeHtml(
      str(t.requiredConditionalOptional) || "—"
    )}</div><div class="fdd-cell-detail">${sumHtml}</div><div class="fdd-cell-detail">Impact: ${escapeHtml(
      str(t.ownerImpact).slice(0, 100)
    )}${str(t.ownerImpact).length > 100 ? "…" : ""}</div><div class="fdd-cell-detail">${escapeHtml(str(t.riskLevel) || "—")} / ${escapeHtml(
      str(t.flexibilityLevel) || "—"
    )} / ${escapeHtml(str(t.negotiability) || "—")}</div><div class="fdd-cell-detail">${reviewPill(t.reviewStatus)} · Legal ${
      t.legalReviewRequired ? "Y" : "N"
    } · ${escapeHtml(str(t.termAuditStatus) || "—")}</div><div class="fdd-cell-detail">Src ${escapeHtml(
      str(t.sourceItemNumber) || "—"
    )} · ${escapeHtml(str(t.documentationReference).slice(0, 40))}${str(t.documentationReference).length > 40 ? "…" : ""}</div></div>`;
  });
  return `<td class="fdd-brand-cell">${blocks.join("")}</td>`;
}

function renderTermsMatrix() {
  const host = $("#bc-panel-terms");
  if (!host) return;
  if (!brandNames.length) {
    host.innerHTML = "<p class=\"muted\">Load comparison first.</p>";
    return;
  }

  const entries = collectTermsCompareEntries();
  if (!entries.length) {
    host.innerHTML =
      '<p class="muted">No approved terms found. Try Include Needs Review or review terms in Franchise Intelligence Admin.</p>';
    return;
  }

  const flexBlocks = brandNames
    .map((br) => {
      const terms = termRowsByBrand.get(br) || [];
      const territory = terms.filter((t) => /territory|area protection|protected territory|no protected/i.test(termBucketKey(t) + str(t.termCategory))).length;
      const transferExit = terms.filter((t) =>
        /transfer|change of control|termination|liquidated|renewal|then-current/i.test(termBucketKey(t) + str(t.termCategory))
      ).length;
      const high = terms.filter((t) => str(t.riskLevel).trim() === "High").length;
      const lowFlex = terms.filter((t) => str(t.flexibilityLevel).trim() === "Low").length;
      const legal = terms.filter((t) => t.legalReviewRequired).length;
      const needs = terms.filter((t) => str(t.reviewStatus).trim() === "Needs Review").length;
      const app = terms.filter((t) => str(t.reviewStatus).trim() === "Approved").length;
      const interp = [];
      if (legal >= 4) interp.push("Legal review heavy — plan counsel time.");
      if (transferExit >= 3) interp.push("Transfer/exit-sensitive topics present.");
      if (territory > 0 || terms.some((t) => termBucketKey(t).includes("No Protected Territory")))
        interp.push("Territory review needed.");
      if (terms.some((t) => /required system|supplier|reporting|audit/i.test(termBucketKey(t) + str(t.termCategory))))
        interp.push("Operational controls present (systems/suppliers/reporting).");
      if (!interp.length) interp.push("No strong automated cross-signals beyond counts.");
      return `<div class="flex-card"><h4>${escapeHtml(br)}</h4>
        <div>Territory-related rows: <strong>${territory}</strong> · Transfer/exit-related: <strong>${transferExit}</strong> · High risk: <strong>${high}</strong> · Low flexibility: <strong>${lowFlex}</strong></div>
        <div>Legal review required: <strong>${legal}</strong> · Needs review: <strong>${needs}</strong> · Approved: <strong>${app}</strong></div>
        <p class="muted" style="margin-top:0.35rem;">${escapeHtml(interp.join(" "))}</p></div>`;
    })
    .join("");

  const thead = `<tr><th class="fdd-metric-th">Category · Term · comparable group</th>${brandNames
    .map((b) => `<th class="fdd-brand-th">${escapeHtml(b)}</th>`)
    .join("")}<th class="fdd-notes-th">Watchouts</th></tr>`;

  const rows = entries.map(
    (entry) =>
      `<tr><td class="fdd-metric-cell">${termLeftMetricHtml(entry)}</td>${brandNames
        .map((b) => termBrandCellHtml(entry.rowsByBrand.get(b)))
        .join("")}<td class="fdd-notes-cell">${termNotesHtml(entry)}</td></tr>`
  );

  host.innerHTML = `
    <div class="flex-summary-grid">${flexBlocks}</div>
    <p class="terms-note">Interpretation is decision-support language, not legal advice. Each row is one mapped term; summaries and risk tags appear in brand columns.</p>
    <div class="fdd-compare-wrap"><table class="fdd-compare-table"><thead>${thead}</thead><tbody>${rows.join("")}</tbody></table></div>
  `;
}

function renderWatchouts() {
  const host = $("#bc-panel-watchouts");
  if (!host) return;
  if (!brandNames.length) {
    host.innerHTML = "<p class=\"muted\">Load comparison first.</p>";
    return;
  }

  const econQs = [
    "Which brand has the higher recurring percentage-based cost stack, and are all components comparable or bundled differently?",
    "Are loyalty, marketing, reservation, and program service fees separately charged or bundled into a broader system/program contribution?",
    "Which technology/system costs are one-time implementation versus ongoing?",
    "Which fees are pass-through, variable, actual cost, or basis-needs-review?",
  ];
  const termQs = [
    "Which brand offers more meaningful territory or competitive protection?",
    "Which brand has more restrictive transfer/change-of-control provisions?",
    "Which brand creates greater termination or liquidated damages exposure?",
    "Which brand requires then-current agreement terms at renewal?",
    "Which brand has heavier approved supplier, system, reporting, or audit obligations?",
  ];

  let bundled = 0;
  let basis = 0;
  let legalTerms = 0;
  let otherMap = 0;
  const thinTermsBrands = [];
  const thinFeeBrands = [];

  for (const b of brandNames) {
    const fees = feeRowsByBrand.get(b) || [];
    const terms = termRowsByBrand.get(b) || [];
    if (!terms.length) thinTermsBrands.push(b);
    if (!fees.length) thinFeeBrands.push(b);
    for (const r of fees) {
      const bs = str(r.bundlingStatus).toLowerCase();
      if (bs.includes("bundled") || bs.includes("partially")) bundled++;
      if (r.basisNeedsReview) basis++;
    }
    legalTerms += terms.filter((t) => t.legalReviewRequired).length;
    otherMap += terms.filter((t) => termBucketKey(t).includes("Other") || str(t.termCategory).includes("Needs Review")).length;
  }

  const watch = [];
  if (bundled >= 3) watch.push("Several economics rows are marked bundled or partially bundled — confirm what is included before comparing line items.");
  if (basis >= 3) watch.push("Multiple fee rows still need basis confirmation — totals and percentage stacks may shift after review.");
  if (legalTerms >= 1) watch.push("Legal-review-required terms are present — involve counsel before relying on term comparisons.");
  if (thinTermsBrands.length) watch.push(`Terms comparison is incomplete for: ${thinTermsBrands.join(", ")}.`);
  if (thinFeeBrands.length) watch.push(`Economics comparison is incomplete for: ${thinFeeBrands.join(", ")}.`);
  if (otherMap >= 4) watch.push("Several terms remain in Other / Needs Mapping — mapping quality may limit apples-to-apples confidence.");

  host.innerHTML = `
    <div class="questions-block"><h3>Economics — cross-brand questions</h3><ul>${econQs.map((q) => `<li>${escapeHtml(q)}</li>`).join("")}</ul></div>
    <div class="questions-block"><h3>Terms &amp; flexibility — cross-brand questions</h3><ul>${termQs.map((q) => `<li>${escapeHtml(q)}</li>`).join("")}</ul></div>
    <div class="questions-block"><h3>Watchouts from this load</h3><ul>${watch.length ? watch.map((w) => `<li>${escapeHtml(w)}</li>`).join("") : "<li class=\"muted\">No automated watchouts beyond reviewing each matrix.</li>"}</ul></div>
  `;
}

function showTab(id) {
  activeTab = id;
  for (const btn of document.querySelectorAll(".compare-tabs [data-bc-tab]")) {
    const on = btn.getAttribute("data-bc-tab") === id;
    btn.classList.toggle("active", on);
    btn.setAttribute("aria-selected", on ? "true" : "false");
  }
  for (const p of document.querySelectorAll(".bc-panel")) {
    p.classList.toggle("active", p.id === `bc-panel-${id}`);
  }
}

function renderAllPanels() {
  renderOverview();
  renderEconomicsMatrix();
  renderTermsMatrix();
  renderWatchouts();
  showTab(activeTab);
}

function csvRowEconomics(r, brand) {
  const bucket = feeBucketKey(r);
  const cg = feeComparableMeta(r);
  const dup = r.possibleDuplicate || r.potentiallyOverlappingFee ? "Yes" : "No";
  return [
    "Economics",
    brand,
    str(r.parentCompany),
    r.fddYear,
    str(r.country),
    bucket,
    cg,
    str(r.comparableStackGroup),
    str(r.feeOrObligationName),
    str(r.amount),
    str(r.normalizedCostBasis || r.basis),
    str(r.amountFormulaType),
    "",
    "",
    "",
    str(r.reviewStatus),
    str(r.auditStatus),
    str(r.basisConfidence || r.confidence),
    dup,
    r.needsLegalReview ? "Yes" : "No",
    r.needsCommercialReview ? "Yes" : "No",
    `${str(r.sourceItemNumber)} ${str(r.sourceItemTitle)}`.trim(),
    str(r.documentationReference),
    str(r.comparabilityNotes || r.auditIssues),
  ].map(csvEscape);
}

function csvRowTerms(t, brand) {
  const bucket = termBucketKey(t);
  const dup = t.possibleDuplicateTerm ? "Yes" : "No";
  return [
    "Terms",
    brand,
    str(t.parentCompany),
    t.fddYear,
    str(t.country),
    bucket,
    str(t.comparableTermGroup),
    "",
    str(t.termObligationName),
    str(t.termSummary),
    "",
    "",
    str(t.riskLevel),
    str(t.flexibilityLevel),
    str(t.negotiability),
    str(t.reviewStatus),
    str(t.termAuditStatus),
    str(t.confidence),
    dup,
    t.legalReviewRequired ? "Yes" : "No",
    t.commercialReviewRequired ? "Yes" : "No",
    `${str(t.sourceItemNumber)} ${str(t.sourceItemTitle)}`.trim(),
    str(t.documentationReference),
    str(t.termAuditIssues),
  ].map(csvEscape);
}

function exportCsv() {
  const headers = [
    "Record Type",
    "Brand Name",
    "Parent Company",
    "FDD Year",
    "Country",
    "Normalized Bucket",
    "Comparable Group",
    "Comparable Stack Group",
    "Name",
    "Summary / Amount",
    "Cost Basis",
    "Formula Type",
    "Risk Level",
    "Flexibility Level",
    "Negotiability",
    "Review Status",
    "Audit Status",
    "Confidence",
    "Duplicate / Overlap",
    "Legal Review",
    "Commercial Review",
    "Source Item",
    "Documentation Reference",
    "Notes",
  ];
  const lines = [headers.map(csvEscape).join(",")];
  for (const b of brandNames) {
    for (const r of feeRowsByBrand.get(b) || []) lines.push(csvRowEconomics(r, b).join(","));
    for (const t of termRowsByBrand.get(b) || []) lines.push(csvRowTerms(t, b).join(","));
  }
  if (lines.length <= 1) {
    setStatus("Nothing to export. Load comparison first.", "error");
    return;
  }
  const blob = new Blob([lines.join("\r\n")], { type: "text/csv;charset=utf-8" });
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = "fdd-brand-comparison.csv";
  a.click();
  URL.revokeObjectURL(a.href);
  setStatus(`Exported ${lines.length - 1} record(s).`, "muted");
}

async function loadComparison() {
  await loadAllData();
  renderAllPanels();
}

document.querySelector(".compare-tabs")?.addEventListener("click", (e) => {
  const btn = e.target.closest("[data-bc-tab]");
  if (!btn) return;
  const id = btn.getAttribute("data-bc-tab");
  if (!id) return;
  activeTab = id;
  showTab(id);
});

$("#btn-load")?.addEventListener("click", () => loadComparison().catch((err) => setStatus(err.message || String(err), "error")));
$("#btn-csv")?.addEventListener("click", () => exportCsv());

(async function bootstrapFddCompare() {
  try {
    await loadRegistry();
    populateBrandSelects();
    applyUrl();
    if (getSelectedBrands().length > 0) {
      await loadComparison();
    }
  } catch (err) {
    setStatus(err.message || String(err), "error");
  }
})();
