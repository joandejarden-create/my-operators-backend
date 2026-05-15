/**
 * Brand Terms Profile — GET /api/fdd-intelligence/brands/:brandName/terms (no secrets).
 */

const API = "/api/fdd-intelligence";

const CATEGORY_ORDER = [
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

const CATEGORY_DESCRIPTIONS = {
  "Territory / Area Protection": "How the FDD describes market protection, exclusivity, and brand carveouts.",
  "Franchise Term": "Initial term length, extension options, and how the franchise term is described.",
  "Renewal Rights": "Conditions and limitations around renewing the franchise relationship.",
  "Transfer / Change of Ownership": "Approval requirements, buyer qualification, and transfer restrictions.",
  "Termination / Default": "Default triggers, cure rights, and termination exposure.",
  "Liquidated Damages": "How damages are calculated, what fee or revenue base applies, and duration of exposure.",
  "Post-Termination Obligations": "De-identification, non-compete, survival, and post-exit obligations.",
  "PIP / Renovation / Brand Standards": "Renovation, brand standards, design, and ongoing compliance obligations.",
  "Required Systems / Technology": "Mandatory systems, platforms, and operational technology obligations.",
  "Training / Staffing / Operator Requirements": "Training attendance, staffing, and operator or key-personnel requirements.",
  "Reporting / Audit / Records": "Reporting, audit, recordkeeping, inspection, and data-access rights.",
  "Approved Suppliers / Procurement": "Approved supplier lists, procurement channels, and substitution rules.",
  "Insurance / Indemnification": "Insurance, indemnity, and risk allocation obligations for counsel review.",
  "Financial Performance Representation": "Financial performance disclosures, samples, and reliance caveats.",
  "System Health / Outlets": "Openings, closures, transfers, terminations, and non-renewals as system signals.",
  "Dispute Resolution / Governing Law": "Governing law, venue, arbitration, waivers, and dispute process.",
  "Other / Needs Review": "Terms that remain unmapped or need advisor review before relying on this profile.",
};

const OWNER_QUESTIONS = {
  "Territory / Area Protection":
    "Does the brand provide meaningful area protection, and what carveouts allow affiliated brands, reservation channels, or competing hotels nearby?",
  "Franchise Term":
    "What is the initial franchise term, and does the term align with the owner’s hold period, financing, and exit strategy?",
  "Renewal Rights":
    "What conditions must be satisfied to renew, and does renewal require signing the then-current franchise agreement?",
  "Transfer / Change of Ownership":
    "Would a sale, change of control, lender action, or management company change trigger brand approval, fees, a new agreement, or a new PIP?",
  "Termination / Default":
    "Which defaults allow termination, which have cure rights, and which allow immediate termination?",
  "Liquidated Damages":
    "How are damages calculated, what fee or revenue base applies, and how long could the exposure continue?",
  "Post-Termination Obligations":
    "What de-identification, non-compete, survival, or post-termination obligations would remain after exit?",
  "PIP / Renovation / Brand Standards":
    "What renovation, refresh, or brand standards obligations may create future capex exposure?",
  "Required Systems / Technology":
    "Which brand systems are mandatory, when must they be implemented, and who bears setup and ongoing responsibility?",
  "Training / Staffing / Operator Requirements":
    "Who must attend training, when is it required, and does the brand require approval of the operator or key personnel?",
  "Reporting / Audit / Records":
    "What reporting, audit, recordkeeping, inspection, and data-access rights does the brand retain?",
  "Approved Suppliers / Procurement":
    "Which purchases must be made from approved suppliers or brand procurement channels, and are substitutes allowed?",
  "Insurance / Indemnification":
    "What insurance, indemnity, and risk allocation obligations should legal counsel review?",
  "Financial Performance Representation":
    "What financial performance information is disclosed, what sample is used, and what caveats limit reliance?",
  "System Health / Outlets":
    "What do openings, closures, transfers, terminations, and non-renewals suggest about brand momentum or system risk?",
  "Dispute Resolution / Governing Law":
    "What law, venue, arbitration, waiver, or dispute process applies if a conflict arises?",
  "Other / Needs Review":
    "What terms remain unmapped or unclear and require advisor review before relying on the profile?",
};

const READINESS_HINTS = {
  "No Terms Loaded": "Load a brand that has extracted and reviewed terms in Franchise Intelligence Admin.",
  "Terms Extracted / Needs Review":
    "Terms exist but none are approved yet. Use Include Needs Review to preview work-in-progress, or complete review in Franchise Intelligence Admin.",
  "Partially Reviewed":
    "Some terms are approved; continue review to strengthen confidence before relying on this profile alone.",
  "Terms Profile Ready":
    "A solid set of approved terms spans multiple categories with limited open review load — still confirm with counsel for your situation.",
  "Legal Review Heavy":
    "Many items are flagged for legal review — plan for counsel time before advancing.",
};

const CSV_HEADERS = [
  "Brand Name",
  "Parent Company",
  "FDD Year",
  "Country",
  "Term / Obligation Name",
  "Term Category",
  "Normalized Term Bucket",
  "Comparable Term Group",
  "Term Summary",
  "Owner Impact",
  "Required / Conditional / Optional",
  "Trigger",
  "Applies When",
  "Risk Level",
  "Flexibility Level",
  "Negotiability",
  "Legal Review Required",
  "Commercial Review Required",
  "Review Status",
  "Confidence",
  "Source Item Number",
  "Source Item Title",
  "Documentation Reference",
  "Documentation Reference Page Number",
  "Source Text Excerpt",
  "Possible Duplicate Term",
  "Duplicate Term Group Key",
  "Term Audit Score",
  "Term Audit Status",
  "Term Audit Issues",
];

/** @type {object[]} */
let lastTerms = [];
let lastMeta = { brandName: "", includeNeedsReview: false, fddYear: null };

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
  if (v == null) return "";
  return String(v);
}

function bucketCategory(cat) {
  const c = String(cat || "").trim();
  return CATEGORY_ORDER.includes(c) ? c : "Other / Needs Review";
}

function setStatus(msg, cls) {
  const el = $("#status-msg");
  if (!el) return;
  el.textContent = msg || "";
  el.className = cls || "muted";
}

function buildQueryParams() {
  const brand = ($("#brand-input") && $("#brand-input").value.trim()) || "";
  const yearRaw = ($("#year-input") && $("#year-input").value.trim()) || "";
  const mode = ($("#review-mode") && $("#review-mode").value) || "approved";
  const params = new URLSearchParams();
  if (mode === "include_needs") params.set("includeNeedsReview", "1");
  if (yearRaw && Number.isFinite(Number(yearRaw))) params.set("fddYear", String(parseInt(yearRaw, 10)));
  return { brand, params };
}

function computeMetrics(terms) {
  const approved = terms.filter((t) => String(t.reviewStatus || "").trim() === "Approved").length;
  const needs = terms.filter((t) => String(t.reviewStatus || "").trim() === "Needs Review").length;
  const legal = terms.filter((t) => t.legalReviewRequired === true).length;
  const commercial = terms.filter((t) => t.commercialReviewRequired === true).length;
  const highRisk = terms.filter((t) => String(t.riskLevel || "").trim() === "High").length;
  const lowFlex = terms.filter((t) => String(t.flexibilityLevel || "").trim() === "Low").length;
  const dup = terms.filter((t) => t.possibleDuplicateTerm === true).length;
  const autoElig = terms.filter((t) => t.autoApproveEligible === true).length;
  const manualAudit = terms.filter((t) =>
    ["Manual Review Required", "Do Not Auto-Approve"].includes(String(t.termAuditStatus || "").trim())
  ).length;
  return {
    total: terms.length,
    approved,
    needs,
    legal,
    commercial,
    highRisk,
    lowFlex,
    dup,
    autoElig,
    manualAudit,
  };
}

function computeReadiness(terms, m) {
  if (!terms.length) return { label: "No Terms Loaded", hint: READINESS_HINTS["No Terms Loaded"] };
  if (m.approved === 0)
    return { label: "Terms Extracted / Needs Review", hint: READINESS_HINTS["Terms Extracted / Needs Review"] };
  const legalRatio = m.legal / Math.max(terms.length, 1);
  if (m.legal >= 5 && legalRatio >= 0.2) {
    return { label: "Legal Review Heavy", hint: READINESS_HINTS["Legal Review Heavy"] };
  }
  const cats = new Set(terms.filter((t) => String(t.reviewStatus || "").trim() === "Approved").map((t) => bucketCategory(t.termCategory)));
  const needsRatio = m.needs / Math.max(terms.length, 1);
  const highOpen = terms.filter((t) => String(t.riskLevel || "").trim() === "High" && String(t.reviewStatus || "").trim() !== "Approved").length;
  if (m.approved >= 5 && cats.size >= 3 && needsRatio <= 0.35 && highOpen <= 2) {
    return { label: "Terms Profile Ready", hint: READINESS_HINTS["Terms Profile Ready"] };
  }
  if (m.approved > 0) return { label: "Partially Reviewed", hint: READINESS_HINTS["Partially Reviewed"] };
  return { label: "Terms Extracted / Needs Review", hint: READINESS_HINTS["Terms Extracted / Needs Review"] };
}

function reviewPill(status) {
  const s = String(status || "").trim();
  if (s === "Approved") return '<span class="pill approved">Approved</span>';
  if (s === "Needs Review") return '<span class="pill needs">Needs Review</span>';
  return `<span class="pill">${escapeHtml(s || "—")}</span>`;
}

function ynPill(v) {
  return v ? "Yes" : "No";
}

function renderReadiness(readiness) {
  const host = $("#readiness-host");
  if (!host) return;
  host.hidden = false;
  host.innerHTML = `
    <div class="readiness-strip">
      <div class="label">Readiness</div>
      <div class="title">${escapeHtml(readiness.label)}</div>
      <div class="hint">${escapeHtml(readiness.hint)}</div>
    </div>
  `;
}

function renderKpis(m) {
  const host = $("#kpi-host");
  if (!host) return;
  host.hidden = false;
  const items = [
    ["Total visible terms", m.total],
    ["Approved terms", m.approved],
    ["Needs Review terms", m.needs],
    ["Legal review required", m.legal],
    ["Commercial review required", m.commercial],
    ["High risk terms", m.highRisk],
    ["Low flexibility terms", m.lowFlex],
    ["Possible duplicate terms", m.dup],
    ["Auto-approve eligible terms", m.autoElig],
    ["Manual review required terms", m.manualAudit],
  ];
  host.innerHTML = `<div class="kpi-grid">${items
    .map(
      ([k, v]) => `
    <div class="kpi-card">
      <div class="k">${escapeHtml(k)}</div>
      <div class="v">${escapeHtml(String(v))}</div>
    </div>`
    )
    .join("")}</div>`;
}

function renderOwnerSummary(terms, m) {
  const host = $("#owner-summary-host");
  if (!host) return;
  if (!terms.length) {
    host.hidden = true;
    host.innerHTML = "";
    return;
  }
  host.hidden = false;
  const cats = new Set(terms.map((t) => bucketCategory(t.termCategory)));
  const paras = [];
  paras.push(
    "<p><strong>Note:</strong> This section is rules-based decision support, not legal advice. It highlights patterns in the loaded rows only.</p>"
  );
  if (m.legal >= 4) {
    paras.push(
      "<p>This brand profile includes several legal-sensitive terms that should be reviewed with counsel before advancing.</p>"
    );
  }
  if (cats.has("Territory / Area Protection")) {
    paras.push(
      "<p>Territory and competitive protection terms were identified and should be reviewed for carveouts and affiliate brand rights.</p>"
    );
  }
  if (cats.has("Transfer / Change of Ownership") || cats.has("Termination / Default")) {
    paras.push(
      "<p>Transfer, termination, and exit-related provisions may affect liquidity, lender flexibility, and downside exposure.</p>"
    );
  }
  if (cats.has("PIP / Renovation / Brand Standards")) {
    paras.push(
      "<p>PIP, renovation, and brand standards obligations may create future capex or compliance requirements.</p>"
    );
  }
  if (cats.has("Required Systems / Technology")) {
    paras.push(
      "<p>Mandatory system and technology obligations may affect implementation timing, operating model, and ongoing responsibilities.</p>"
    );
  }
  if (paras.length === 1) {
    paras.push("<p>No strong automated signals beyond the counts above — review category sections for details.</p>");
  }
  host.innerHTML = `<div class="owner-summary-box"><h2>Owner / advisor summary</h2>${paras.join("")}</div>`;
}

function renderQuestions(terms) {
  const host = $("#questions-host");
  if (!host) return;
  const presentCats = new Set(terms.map((t) => bucketCategory(t.termCategory)));
  const ordered = CATEGORY_ORDER.filter((c) => presentCats.has(c) && OWNER_QUESTIONS[c]);
  if (!ordered.length) {
    host.hidden = true;
    host.innerHTML = "";
    return;
  }
  host.hidden = false;
  const lis = ordered
    .map(
      (c) => `<li>${escapeHtml(OWNER_QUESTIONS[c])}<span class="q-cat">${escapeHtml(c)}</span></li>`
    )
    .join("");
  host.innerHTML = `<div class="questions"><h2>Key owner questions</h2><ul>${lis}</ul></div>`;
}

function termDlItem(label, value) {
  const v = str(value).trim() || "—";
  return `<dt>${escapeHtml(label)}</dt><dd>${escapeHtml(v)}</dd>`;
}

function renderTermCard(t) {
  const excerpt = str(t.sourceTextExcerpt).trim();
  const excerptBlock =
    excerpt.length > 0
      ? `<div class="excerpt"><details><summary>Source text excerpt</summary><pre>${escapeHtml(excerpt)}</pre></details></div>`
      : "";
  const riskPill =
    String(t.riskLevel || "").trim() === "High"
      ? ' <span class="pill risk-high">High risk</span>'
      : "";
  const dupPill = t.possibleDuplicateTerm ? ' <span class="pill dup">Possible duplicate</span>' : "";
  return `
    <article class="term-card">
      <h4>${escapeHtml(str(t.termObligationName) || "(Unnamed term)")}${riskPill}${dupPill}</h4>
      <div style="margin-bottom:0.35rem;">${reviewPill(t.reviewStatus)}</div>
      <dl class="term-meta-grid">
        ${termDlItem("Normalized term bucket", t.normalizedTermBucket)}
        ${termDlItem("Comparable term group", t.comparableTermGroup)}
        ${termDlItem("Term summary", t.termSummary)}
        ${termDlItem("Owner impact", t.ownerImpact)}
        ${termDlItem("Required / conditional / optional", t.requiredConditionalOptional)}
        ${termDlItem("Trigger", t.trigger)}
        ${termDlItem("Applies when", t.appliesWhen)}
        ${termDlItem("Risk level", t.riskLevel)}
        ${termDlItem("Flexibility level", t.flexibilityLevel)}
        ${termDlItem("Negotiability", t.negotiability)}
        ${termDlItem("Legal review required", ynPill(!!t.legalReviewRequired))}
        ${termDlItem("Commercial review required", ynPill(!!t.commercialReviewRequired))}
        ${termDlItem("Confidence", t.confidence)}
        ${termDlItem("Source item", `${str(t.sourceItemNumber) || "—"} · ${str(t.sourceItemTitle)}`)}
        ${termDlItem("Documentation reference", t.documentationReference)}
        ${termDlItem("Documentation ref. page", t.documentationReferencePageNumber)}
        ${termDlItem("Term audit status", t.termAuditStatus)}
        ${termDlItem("Term audit score", t.termAuditScore != null ? String(t.termAuditScore) : "")}
        ${termDlItem("Term audit issues", t.termAuditIssues)}
        ${termDlItem("Duplicate group key", t.duplicateTermGroupKey)}
      </dl>
      ${excerptBlock}
    </article>
  `;
}

function renderGrouped(terms) {
  const host = $("#terms-grouped-host");
  if (!host) return;
  if (!terms.length) {
    host.innerHTML = "";
    return;
  }
  const byCat = new Map();
  for (const c of CATEGORY_ORDER) byCat.set(c, []);
  for (const t of terms) {
    const c = bucketCategory(t.termCategory);
    if (!byCat.has(c)) byCat.set(c, []);
    byCat.get(c).push(t);
  }
  const parts = [];
  for (const cat of CATEGORY_ORDER) {
    const list = byCat.get(cat) || [];
    if (!list.length) continue;
    list.sort((a, b) => str(a.termObligationName).localeCompare(str(b.termObligationName), undefined, { sensitivity: "base" }));
    const desc = CATEGORY_DESCRIPTIONS[cat] || "";
    parts.push(`
      <section class="cat-section" id="cat-sec-${CATEGORY_ORDER.indexOf(cat)}">
        <h3>${escapeHtml(cat)} <span class="muted" style="font-weight:500;font-size:0.85rem;">(${list.length})</span></h3>
        <p class="cat-desc">${escapeHtml(desc)}</p>
        ${list.map(renderTermCard).join("")}
      </section>
    `);
  }
  host.innerHTML = parts.join("") || '<p class="muted">No terms to display.</p>';
}

function termToCsvRow(t) {
  return [
    t.brandName,
    t.parentCompany,
    t.fddYear,
    t.country,
    t.termObligationName,
    bucketCategory(t.termCategory),
    t.normalizedTermBucket,
    t.comparableTermGroup,
    t.termSummary,
    t.ownerImpact,
    t.requiredConditionalOptional,
    t.trigger,
    t.appliesWhen,
    t.riskLevel,
    t.flexibilityLevel,
    t.negotiability,
    t.legalReviewRequired ? "Yes" : "No",
    t.commercialReviewRequired ? "Yes" : "No",
    t.reviewStatus,
    t.confidence,
    t.sourceItemNumber,
    t.sourceItemTitle,
    t.documentationReference,
    t.documentationReferencePageNumber,
    t.sourceTextExcerpt,
    t.possibleDuplicateTerm ? "Yes" : "No",
    t.duplicateTermGroupKey,
    t.termAuditScore,
    t.termAuditStatus,
    t.termAuditIssues,
  ].map(csvEscape);
}

function exportCsv() {
  const { brand } = buildQueryParams();
  if (!lastTerms.length) {
    setStatus("No terms loaded to export. Load a profile first.", "error");
    return;
  }
  const lines = [CSV_HEADERS.map(csvEscape).join(",")];
  for (const t of lastTerms) lines.push(termToCsvRow(t).join(","));
  const blob = new Blob([lines.join("\r\n")], { type: "text/csv;charset=utf-8" });
  const a = document.createElement("a");
  const safe = (brand || "brand").replace(/[^\w\-]+/g, "_").slice(0, 60);
  a.href = URL.createObjectURL(blob);
  a.download = `fdd-brand-terms-${safe}.csv`;
  a.click();
  URL.revokeObjectURL(a.href);
  setStatus(`Exported ${lastTerms.length} row(s) to CSV.`, "muted");
}

function syncUrl() {
  const { brand, params } = buildQueryParams();
  const q = new URLSearchParams();
  if (brand) q.set("brand", brand);
  const y = ($("#year-input") && $("#year-input").value.trim()) || "";
  if (y) q.set("fddYear", y);
  if (params.get("includeNeedsReview")) q.set("includeNeedsReview", "1");
  const qs = q.toString();
  const path = "/fdd-brand-terms-profile.html" + (qs ? `?${qs}` : "");
  history.replaceState(null, "", path);
}

function applyQueryFromUrl() {
  const q = new URLSearchParams(window.location.search);
  const brand = q.get("brand");
  const year = q.get("fddYear");
  const incl = q.get("includeNeedsReview");
  if (brand && $("#brand-input")) $("#brand-input").value = brand;
  if (year && $("#year-input")) $("#year-input").value = year;
  if ($("#review-mode")) {
    $("#review-mode").value = incl === "1" || String(incl).toLowerCase() === "true" ? "include_needs" : "approved";
  }
}

async function loadProfile() {
  const { brand, params } = buildQueryParams();
  if (!brand) {
    setStatus("Enter a brand name to load approved terms.", "error");
    $("#readiness-host").hidden = true;
    $("#readiness-host").innerHTML = "";
    $("#kpi-host").hidden = true;
    $("#kpi-host").innerHTML = "";
    $("#owner-summary-host").hidden = true;
    $("#owner-summary-host").innerHTML = "";
    $("#questions-host").hidden = true;
    $("#questions-host").innerHTML = "";
    $("#terms-grouped-host").innerHTML = "";
    lastTerms = [];
    return;
  }
  setStatus("Loading…", "muted");
  const url = `${API}/brands/${encodeURIComponent(brand)}/terms${params.toString() ? `?${params}` : ""}`;
  try {
    const res = await fetch(url, { headers: { Accept: "application/json" } });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      setStatus(`Unable to load brand terms profile. ${data.error || res.statusText || ""}`.trim(), "error");
      lastTerms = [];
      $("#readiness-host").hidden = true;
      $("#readiness-host").innerHTML = "";
      $("#kpi-host").hidden = true;
      $("#kpi-host").innerHTML = "";
      $("#owner-summary-host").hidden = true;
      $("#owner-summary-host").innerHTML = "";
      $("#questions-host").hidden = true;
      $("#questions-host").innerHTML = "";
      $("#terms-grouped-host").innerHTML = "";
      return;
    }
    lastTerms = data.terms || [];
    lastMeta = {
      brandName: data.brandName || brand,
      includeNeedsReview: !!data.includeNeedsReview,
      fddYear: data.fddYear != null ? data.fddYear : null,
    };
    syncUrl();

    if (!lastTerms.length) {
      setStatus(
        "No approved terms found for this brand. Try Include Needs Review or review terms in Franchise Intelligence Admin.",
        "muted"
      );
      const readiness = { label: "No Terms Loaded", hint: READINESS_HINTS["No Terms Loaded"] };
      renderReadiness(readiness);
      $("#kpi-host").hidden = true;
      $("#kpi-host").innerHTML = "";
      $("#owner-summary-host").hidden = true;
      $("#owner-summary-host").innerHTML = "";
      $("#questions-host").hidden = true;
      $("#questions-host").innerHTML = "";
      $("#terms-grouped-host").innerHTML = "";
      return;
    }

    setStatus(
      `${lastTerms.length} term(s) · storage: ${data.storage || "—"}${lastMeta.fddYear != null ? ` · FDD year ${lastMeta.fddYear}` : ""}${
        lastMeta.includeNeedsReview ? " · including Needs Review" : " · approved only"
      }`,
      "muted"
    );
    const m = computeMetrics(lastTerms);
    const readiness = computeReadiness(lastTerms, m);
    renderReadiness(readiness);
    renderKpis(m);
    renderOwnerSummary(lastTerms, m);
    renderQuestions(lastTerms);
    renderGrouped(lastTerms);
  } catch (e) {
    setStatus(`Unable to load brand terms profile. ${e.message || String(e)}`, "error");
    lastTerms = [];
    $("#readiness-host").hidden = true;
    $("#readiness-host").innerHTML = "";
    $("#kpi-host").hidden = true;
    $("#kpi-host").innerHTML = "";
    $("#owner-summary-host").hidden = true;
    $("#owner-summary-host").innerHTML = "";
    $("#questions-host").hidden = true;
    $("#questions-host").innerHTML = "";
    $("#terms-grouped-host").innerHTML = "";
  }
}

$("#btn-load").addEventListener("click", () => loadProfile());
$("#btn-csv").addEventListener("click", () => exportCsv());

applyQueryFromUrl();
if (($("#brand-input") && $("#brand-input").value.trim()) || window.location.search) {
  loadProfile();
}
