/**
 * FDD Terms & Obligations — normalization, duplicate detection, and rules-based audit.
 * Term-specific only; does not use fee cost-basis logic.
 */

export const FDD_TERM_AUDIT_VERSION = "fdd-term-audit-v1";

export const NORMALIZED_TERM_BUCKETS = [
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

export const COMPARABLE_TERM_GROUPS = [
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

const BUCKET_SET = new Set(NORMALIZED_TERM_BUCKETS);
const COMPARABLE_SET = new Set(COMPARABLE_TERM_GROUPS);

/** Categories that must never auto-approve via automation. */
export const TERM_LEGAL_SENSITIVE_CATEGORIES = new Set([
  "Territory / Area Protection",
  "Renewal Rights",
  "Transfer / Change of Ownership",
  "Termination / Default",
  "Liquidated Damages",
  "Post-Termination Obligations",
  "Insurance / Indemnification",
  "Dispute Resolution / Governing Law",
]);

const BUCKET_TO_COMPARABLE = {
  "Protected Territory / Area Rights": "Territory & Competitive Protection",
  "No Protected Territory": "Territory & Competitive Protection",
  "Brand Carveouts / Affiliate Rights": "Territory & Competitive Protection",
  "Initial Franchise Term": "Term & Renewal Flexibility",
  "Renewal Right / Renewal Conditions": "Term & Renewal Flexibility",
  "Then-Current Agreement Requirement": "Term & Renewal Flexibility",
  "Transfer Approval Requirement": "Transfer & Exit Flexibility",
  "Change of Control Restriction": "Transfer & Exit Flexibility",
  "Lender / Foreclosure Rights": "Transfer & Exit Flexibility",
  "Transfer Fee / Transfer Process": "Transfer & Exit Flexibility",
  "Termination for Cause": "Termination & Default Exposure",
  "Immediate Termination Event": "Termination & Default Exposure",
  "Owner Termination Right": "Termination & Default Exposure",
  "Liquidated Damages Formula": "Termination & Default Exposure",
  "Post-Termination De-Identification": "Post-Termination Restrictions",
  "Non-Compete / Restrictive Covenant": "Post-Termination Restrictions",
  "PIP Trigger / Renovation Requirement": "Capex / PIP / Brand Standards",
  "Brand Standards Compliance": "Capex / PIP / Brand Standards",
  "Required Systems Obligation": "Operating Control / Systems Requirements",
  "Approved Supplier Requirement": "Supplier / Procurement Restrictions",
  "Reporting / Audit Rights": "Reporting / Audit / Compliance",
  "Insurance Requirement": "Legal / Dispute / Indemnity",
  "Indemnification Obligation": "Legal / Dispute / Indemnity",
  "Dispute Resolution / Arbitration": "Legal / Dispute / Indemnity",
  "Governing Law / Venue": "Legal / Dispute / Indemnity",
  "Financial Performance Representation": "Performance / System Health",
  "Outlet / System Health Disclosure": "Performance / System Health",
  "Other / Needs Mapping": "Other / Needs Mapping",
};

function clamp(n, lo, hi) {
  return Math.max(lo, Math.min(hi, n));
}

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

function normKeyPart(s) {
  return norm(s).replace(/[^a-z0-9]+/g, "_").slice(0, 120);
}

function tokens(s) {
  return norm(s)
    .replace(/[^a-z0-9\s]/g, " ")
    .split(/\s+/)
    .filter((w) => w.length >= 3);
}

function textMeaningful(s, minLen = 40) {
  return String(s || "").trim().length >= minLen;
}

function textVague(s) {
  const t = norm(s);
  if (!t || t.length < 25) return true;
  if (/^unclear$|^n\/a$|^not stated$|^tbd$/i.test(t)) return true;
  if (/\bunclear\b|\bnot specified\b|\bto be determined\b/i.test(t)) return true;
  return false;
}

function nameReflectedInExcerpt(name, excerpt) {
  const ex = norm(excerpt);
  if (!ex.length) return false;
  const toks = tokens(name);
  if (!toks.length) return true;
  let hit = 0;
  for (const w of toks) if (ex.includes(w)) hit++;
  return hit / toks.length >= 0.34;
}

/**
 * Infer Normalized Term Bucket + Comparable Term Group from term text and category.
 * @param {Record<string, unknown>} term
 * @returns {{ normalizedTermBucket: string, comparableTermGroup: string }}
 */
export function inferTermNormalizationFields(term) {
  const hay = norm(
    `${term.termObligationName || ""} ${term.termSummary || ""} ${term.ownerImpact || ""} ${term.trigger || ""} ${term.appliesWhen || ""}`
  );
  const cat = String(term.termCategory || "").trim();

  function pick(bucket) {
    const b = BUCKET_SET.has(bucket) ? bucket : "Other / Needs Mapping";
    const g = BUCKET_TO_COMPARABLE[b] || "Other / Needs Mapping";
    return { normalizedTermBucket: b, comparableTermGroup: COMPARABLE_SET.has(g) ? g : "Other / Needs Mapping" };
  }

  // --- Territory ---
  if (/\bno\s+(exclusive\s+)?protected\s+territory\b|\bno\s+exclusive\s+territory\b|\bnon[-\s]?exclusive\b.*\bterritory\b/i.test(hay)) {
    return pick("No Protected Territory");
  }
  if (/\baffiliate\b|\bcompeting\s+brand\b|\bcarve[-\s]?out\b|\breservation\s+of\s+rights\b|\binternet\s+sales\b.*\bexclusion\b/i.test(hay)) {
    return pick("Brand Carveouts / Affiliate Rights");
  }
  if (/\bprotected\s+territory\b|\bexclusive\s+territory\b|\barea\s+of\s+protection\b|\bterritory\s+grant\b|\bexclusivity\b/i.test(hay) || cat === "Territory / Area Protection") {
    return pick("Protected Territory / Area Rights");
  }

  // --- Term / renewal ---
  if (/\bthen[-\s]?current\b.*\b(agreement|fdd|disclosure)\b|\bcurrent\s+form\s+of\b/i.test(hay)) {
    return pick("Then-Current Agreement Requirement");
  }
  if (/\brenewal\b|\brenew\b|\brenewable\b|\brenewal\s+term\b/i.test(hay) || cat === "Renewal Rights") {
    return pick("Renewal Right / Renewal Conditions");
  }
  if (/\binitial\s+term\b|\bfranchise\s+term\b|\bterm\s+of\s+years\b|\b\d+\s+year\s+term\b/i.test(hay) || cat === "Franchise Term") {
    return pick("Initial Franchise Term");
  }

  // --- Transfer ---
  if (/\btransfer\s+fee\b|\btransfer\s+process\b|\bassignment\s+fee\b/i.test(hay)) {
    return pick("Transfer Fee / Transfer Process");
  }
  if (/\bcomfort\s+letter\b|\blender\b|\bforeclosure\b|\bsecurity\s+interest\b/i.test(hay)) {
    return pick("Lender / Foreclosure Rights");
  }
  if (/\bchange\s+of\s+control\b|\bchange\s+in\s+control\b/i.test(hay)) {
    return pick("Change of Control Restriction");
  }
  if (/\btransfer\b|\bassignment\b|\bsale\s+of\b|\bapproval\s+of\s+transfer\b/i.test(hay) || cat === "Transfer / Change of Ownership") {
    return pick("Transfer Approval Requirement");
  }

  // --- Termination ---
  if (/\bimmediate\s+termination\b|\bterminate\s+immediately\b/i.test(hay)) {
    return pick("Immediate Termination Event");
  }
  if (/\bowner\s+.*\bterminat|\bfranchisee\s+.*\bterminat|\bright\s+to\s+terminate\b/i.test(hay)) {
    return pick("Owner Termination Right");
  }
  if (/\bliquidated\s+damages\b|\bld\b.*\bdamages\b/i.test(hay) || cat === "Liquidated Damages") {
    return pick("Liquidated Damages Formula");
  }
  if (/\btermination\s+for\s+cause\b|\bdefault\b.*\bterminat|\bterminat.*\bdefault\b/i.test(hay) || cat === "Termination / Default") {
    return pick("Termination for Cause");
  }

  // --- Post-termination ---
  if (/\bde[-\s]?identif|\bsignage\s+removal\b|\bremove\s+marks\b/i.test(hay) || cat === "Post-Termination Obligations") {
    return pick("Post-Termination De-Identification");
  }
  if (/\bnon[-\s]?compete\b|\brestrictive\s+covenant\b|\bcovenant\s+not\s+to\s+compete\b/i.test(hay)) {
    return pick("Non-Compete / Restrictive Covenant");
  }

  // --- PIP / standards ---
  if (/\bpip\b|\bproperty\s+improvement\b|\brenovation\b|\brefresh\b|\bcapital\s+improvement\b/i.test(hay) || cat === "PIP / Renovation / Brand Standards") {
    return pick("PIP Trigger / Renovation Requirement");
  }
  if (/\bbrand\s+standards\b|\bquality\s+standards\b|\bstandards\s+manual\b/i.test(hay)) {
    return pick("Brand Standards Compliance");
  }

  // --- Operations ---
  if (/\bapproved\s+supplier\b|\bapproved\s+vendor\b|\bprocurement\b|\brequired\s+vendor\b/i.test(hay) || cat === "Approved Suppliers / Procurement") {
    return pick("Approved Supplier Requirement");
  }
  if (/\bpms\b|\bcrs\b|\bpos\b|\bloyalty\s+program\b|\brequired\s+system\b|\bcomputer\s+system\b|\bsoftware\s+obligation\b/i.test(hay) || cat === "Required Systems / Technology") {
    return pick("Required Systems Obligation");
  }
  if (/\breporting\b|\baudit\b|\brecords\b|\binspection\b/i.test(hay) || cat === "Reporting / Audit / Records") {
    return pick("Reporting / Audit Rights");
  }

  // --- Legal ---
  if (/\bgoverning\s+law\b|\bvenue\b|\bjurisdiction\b/i.test(hay) || (cat === "Dispute Resolution / Governing Law" && /\bvenue\b|\blaw\b|\bstate\b/i.test(hay))) {
    return pick("Governing Law / Venue");
  }
  if (/\barbitration\b|\bmediation\b|\bdispute\s+resolution\b|\bjury\s+waiver\b/i.test(hay) || cat === "Dispute Resolution / Governing Law") {
    return pick("Dispute Resolution / Arbitration");
  }
  if (/\bindemnif/i.test(hay) || cat === "Insurance / Indemnification") {
    return pick("Indemnification Obligation");
  }
  if (/\binsurance\b|\bcertificate\s+of\s+insurance\b/i.test(hay)) {
    return pick("Insurance Requirement");
  }

  // --- Item 19 / 20 ---
  if (/\bitem\s*19\b|\bfinancial\s+performance\s+representation\b|\bfpr\b/i.test(hay) || cat === "Financial Performance Representation") {
    return pick("Financial Performance Representation");
  }
  if (/\bitem\s*20\b|\boutlets?\b|\bopenings?\b|\bclosures?\b|\bnon[-\s]?renewals?\b/i.test(hay) || cat === "System Health / Outlets") {
    return pick("Outlet / System Health Disclosure");
  }

  // --- Category fallback ---
  if (cat === "Territory / Area Protection") return pick("Protected Territory / Area Rights");
  if (cat === "Training / Staffing / Operator Requirements") return pick("Required Systems Obligation");

  return pick("Other / Needs Mapping");
}

/**
 * Set duplicateTermGroupKey and possibleDuplicateTerm on each term (same document batch).
 * @param {Record<string, unknown>[]} terms
 * @returns {{ inputRows: number, duplicateGroups: number, duplicateRows: number }}
 */
export function detectDuplicateTerms(terms) {
  const list = Array.isArray(terms) ? terms : [];
  if (!list.length) return { inputRows: 0, duplicateGroups: 0, duplicateRows: 0 };

  for (const t of list) {
    const docId = String(t.fddDocumentId || "");
    const brand = normKeyPart(t.brandName);
    const year = String(t.fddYear ?? "");
    const bucket = normKeyPart(t.normalizedTermBucket);
    const name = normKeyPart(t.termObligationName);
    const tr = normKeyPart(t.trigger);
    const aw = normKeyPart(t.appliesWhen);
    const key = `${docId}|${brand}|${year}|${bucket}|${name}|${tr}|${aw}`;
    t.duplicateTermGroupKey = key.length > 400 ? key.slice(0, 400) : key;
    t.possibleDuplicateTerm = false;
  }

  const byRun = new Map();
  for (const t of list) {
    const run = String(t.extractionRunId || "");
    const gk = `${String(t.fddDocumentId || "")}|${run}`;
    if (!byRun.has(gk)) byRun.set(gk, []);
    byRun.get(gk).push(t);
  }

  let duplicateGroups = 0;
  let duplicateRows = 0;
  for (const group of byRun.values()) {
    const counts = new Map();
    for (const t of group) {
      counts.set(t.duplicateTermGroupKey, (counts.get(t.duplicateTermGroupKey) || 0) + 1);
    }
    for (const [, c] of counts) {
      if (c >= 2) duplicateGroups += 1;
    }
    for (const t of group) {
      const c = counts.get(t.duplicateTermGroupKey) || 0;
      if (c >= 2) {
        t.possibleDuplicateTerm = true;
        duplicateRows += 1;
      }
    }
  }

  return { inputRows: list.length, duplicateGroups, duplicateRows };
}

/**
 * @param {Record<string, unknown>} term
 * @returns {Record<string, unknown>}
 */
export function auditFddTerm(term) {
  const issues = [];
  let score = 100;

  let sourceSupportScore = 100;
  let categoryQualityScore = 100;
  let riskQualityScore = 100;
  let ownerImpactScore = 100;
  let legalSensitivityScore = 100;

  const reviewStatus = String(term.reviewStatus || "").trim();
  const excerpt = String(term.sourceTextExcerpt || "").trim();
  const name = String(term.termObligationName || "").trim();
  const summary = String(term.termSummary || "").trim();
  const impact = String(term.ownerImpact || "").trim();
  const bucket = String(term.normalizedTermBucket || "").trim();
  const cat = String(term.termCategory || "").trim();
  const risk = String(term.riskLevel || "").trim();
  const flex = String(term.flexibilityLevel || "").trim();
  const neg = String(term.negotiability || "").trim();
  const srcItem = String(term.sourceItemNumber || "").trim();
  const legalReq = !!term.legalReviewRequired;
  const dup = !!term.possibleDuplicateTerm;

  if (reviewStatus === "Rejected") {
    return {
      termAuditScore: 0,
      termAuditConfidence: "Low",
      termAuditStatus: "Do Not Auto-Approve",
      termAuditIssues: "Rejected term",
      autoApproveEligible: false,
      lastAuditedAt: new Date().toISOString(),
      auditVersion: FDD_TERM_AUDIT_VERSION,
      sourceSupportScore: 0,
      categoryQualityScore: 0,
      riskQualityScore: 0,
      ownerImpactScore: 0,
      legalSensitivityScore: 0,
    };
  }

  // --- Source support ---
  if (!textMeaningful(excerpt, 25)) {
    score -= 25;
    sourceSupportScore -= 25;
    issues.push("Source excerpt missing or very short");
  } else if (name && !nameReflectedInExcerpt(name, excerpt)) {
    score -= 10;
    sourceSupportScore -= 10;
    issues.push("Term name not reflected in excerpt");
  }
  if (!srcItem) {
    score -= 5;
    sourceSupportScore -= 5;
    issues.push("Source item number missing");
  }

  // --- Category quality ---
  if (cat === "Other / Needs Review") {
    score -= 20;
    categoryQualityScore -= 20;
    issues.push("Term category is Other / Needs Review");
  }
  if (bucket === "Other / Needs Mapping") {
    score -= 20;
    categoryQualityScore -= 20;
    issues.push("Normalized bucket is Other / Needs Mapping");
  }

  // --- Risk / flexibility ---
  if (risk === "Unclear") {
    score -= 10;
    riskQualityScore -= 10;
    issues.push("Risk level unclear");
  }
  if (flex === "Unclear") {
    score -= 10;
    riskQualityScore -= 10;
    issues.push("Flexibility level unclear");
  }
  if (neg === "Unclear") {
    score -= 5;
    riskQualityScore -= 5;
    issues.push("Negotiability unclear");
  }

  // --- Owner impact / summary ---
  if (textVague(impact)) {
    score -= 15;
    ownerImpactScore -= 15;
    issues.push("Owner impact missing or vague");
  }
  if (textVague(summary)) {
    score -= 10;
    ownerImpactScore -= 10;
    issues.push("Term summary missing or vague");
  }

  // --- Duplicate ---
  if (dup) {
    score -= 20;
    issues.push("Possible duplicate term");
  }

  // --- Legal sensitivity ---
  if (legalReq) {
    score -= 30;
    legalSensitivityScore -= 30;
    issues.push("Legal review required");
  }
  const sensCats = [
    "Termination / Default",
    "Liquidated Damages",
    "Transfer / Change of Ownership",
    "Renewal Rights",
    "Territory / Area Protection",
    "Insurance / Indemnification",
    "Dispute Resolution / Governing Law",
  ];
  if (sensCats.includes(cat)) {
    score -= 20;
    legalSensitivityScore -= 20;
    issues.push("Legally sensitive term category");
  }

  score = clamp(Math.round(score), 0, 100);
  sourceSupportScore = clamp(Math.round(sourceSupportScore), 0, 100);
  categoryQualityScore = clamp(Math.round(categoryQualityScore), 0, 100);
  riskQualityScore = clamp(Math.round(riskQualityScore), 0, 100);
  ownerImpactScore = clamp(Math.round(ownerImpactScore), 0, 100);
  legalSensitivityScore = clamp(Math.round(legalSensitivityScore), 0, 100);

  let termAuditConfidence = "Low";
  if (score >= 90) termAuditConfidence = "High";
  else if (score >= 70) termAuditConfidence = "Medium";

  const audit = {
    termAuditScore: score,
    termAuditConfidence,
    termAuditStatus: "Needs Review",
    termAuditIssues: issues.join("; "),
    autoApproveEligible: false,
    lastAuditedAt: new Date().toISOString(),
    auditVersion: FDD_TERM_AUDIT_VERSION,
    sourceSupportScore,
    categoryQualityScore,
    riskQualityScore,
    ownerImpactScore,
    legalSensitivityScore,
  };

  if (reviewStatus === "Approved") {
    audit.autoApproveEligible = false;
  } else {
    audit.autoApproveEligible = isTermAutoApproveEligible(term, audit);
  }

  if (score >= 90 && audit.autoApproveEligible) {
    audit.termAuditStatus = "High Confidence";
  } else if (score >= 80) {
    audit.termAuditStatus = "Quick Review";
  } else if (score >= 60) {
    audit.termAuditStatus = "Needs Review";
  } else {
    audit.termAuditStatus = "Manual Review Required";
  }

  return audit;
}

/**
 * @param {Record<string, unknown>} term
 * @param {Record<string, unknown>} audit from auditFddTerm (partial merge ok)
 */
export function isTermAutoApproveEligible(term, audit) {
  const score = Number(audit.termAuditScore ?? term.termAuditScore ?? 0);
  const reviewStatus = String(term.reviewStatus || "").trim();
  if (reviewStatus !== "Needs Review") return false;
  if (score < 90) return false;
  if (term.legalReviewRequired === true) return false;
  if (term.commercialReviewRequired !== false) return false;
  if (term.possibleDuplicateTerm === true) return false;
  const bucket = String(term.normalizedTermBucket || "").trim();
  if (bucket === "Other / Needs Mapping") return false;
  const cat = String(term.termCategory || "").trim();
  if (TERM_LEGAL_SENSITIVE_CATEGORIES.has(cat)) return false;
  const risk = String(term.riskLevel || "").trim();
  if (risk !== "Low" && risk !== "Medium") return false;
  const excerpt = String(term.sourceTextExcerpt || "").trim();
  if (!textMeaningful(excerpt, 40)) return false;
  if (textVague(term.termSummary)) return false;
  if (textVague(term.ownerImpact)) return false;
  return true;
}

/**
 * Apply infer → duplicate flags → audit for each term (mutates array).
 * @param {Record<string, unknown>[]} terms
 */
export function auditFddTerms(terms) {
  if (!Array.isArray(terms)) return;
  for (const t of terms) {
    const inf = inferTermNormalizationFields(t);
    t.normalizedTermBucket = inf.normalizedTermBucket;
    t.comparableTermGroup = inf.comparableTermGroup;
  }
  detectDuplicateTerms(terms);
  for (const t of terms) {
    const a = auditFddTerm(t);
    Object.assign(t, a);
  }
}

/** Bulk approve selected: same safeguards as POST approve-auto-eligible-terms row-level checks (score ≥ 80, etc.). */
export function termBulkApproveBlockedReason(term) {
  const rs = String(term.reviewStatus || "").trim();
  if (rs !== "Needs Review") return "not_needs_review";
  if (term.legalReviewRequired === true) return "legal_review_required";
  if (term.possibleDuplicateTerm === true) return "duplicate";
  const cat = String(term.termCategory || "").trim();
  if (TERM_LEGAL_SENSITIVE_CATEGORIES.has(cat)) return "legal_sensitive_category";
  const s = Number(term.termAuditScore ?? 0);
  if (s < 80) return "score_below_80";
  if (String(term.normalizedTermBucket || "").trim() === "Other / Needs Mapping") return "bucket_other";
  return null;
}
