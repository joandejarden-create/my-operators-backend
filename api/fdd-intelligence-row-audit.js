/**
 * Rules-based FDD fee row audit (0–100 score, status bands, auto-approve eligibility).
 * Does not call external services. Does not approve rows — callers set Review Status.
 */

export const FDD_ROW_AUDIT_VERSION = "fdd-row-audit-v1";

/** @type {ReadonlySet<string>} */
export const AUDIT_CONFIDENCE_VALUES = new Set(["High", "Medium", "Low"]);

/** @type {ReadonlySet<string>} */
export const AUDIT_STATUS_VALUES = new Set([
  "High Confidence",
  "Quick Review",
  "Needs Review",
  "Manual Review Required",
  "Do Not Auto-Approve",
]);

/** Keys written onto row objects / Airtable (for merge / compare). */
export const AUDIT_ROW_FIELD_KEYS = [
  "auditScore",
  "auditConfidence",
  "auditStatus",
  "auditIssues",
  "autoApproveEligible",
  "lastAuditedAt",
  "auditVersion",
  "sourceSupportScore",
  "amountQualityScore",
  "basisQualityScore",
  "categoryQualityScore",
  "duplicateRiskScore",
  "legalRiskScore",
];

function clamp(n, lo, hi) {
  return Math.max(lo, Math.min(hi, n));
}

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

function tokens(s) {
  return norm(s)
    .replace(/[^a-z0-9\s]/g, " ")
    .split(/\s+/)
    .filter((w) => w.length >= 3);
}

function excerptMeaningful(ex) {
  return String(ex || "").trim().length >= 40;
}

/** Unclear / missing amount patterns (penalize heavily). */
function amountIsUnclear(amount) {
  const t = norm(amount);
  if (!t || t === "unclear") return true;
  if (/\bnot\s+stated\b|\bnot\s+specified\b|\bn\/a\b|\btbd\b|\bto\s+be\s+determined\b|\bnot\s+available\b/i.test(t)) return true;
  return false;
}

/** Variable / soft amount (moderate penalty). */
function amountIsVariable(amount) {
  const t = norm(amount);
  if (!t) return false;
  if (/\bvaries\b|\bvariable\b|\bactual\s+cost\b|\bthen[-\s]?current\b|\bnot\s+determinable\b|\bas\s+incurred\b/i.test(t)) return true;
  return false;
}

/** Has concrete numeric / currency / percent signal. */
function amountLooksConcrete(amount) {
  const t = String(amount || "");
  return /%|\$|€|£|\d/.test(t);
}

function nameSupportedByExcerpt(name, excerpt) {
  const ex = norm(excerpt);
  if (!ex.length) return false;
  const toks = tokens(name);
  if (!toks.length) return true;
  let hit = 0;
  for (const w of toks) if (ex.includes(w)) hit++;
  return hit / toks.length >= 0.34;
}

function amountSupportedByExcerpt(amount, excerpt) {
  if (!amountLooksConcrete(amount)) return true;
  const ex = norm(excerpt);
  const am = String(amount || "");
  const m = am.match(/\$[\d,]+(?:\.\d{1,2})?/);
  if (m && ex.includes(norm(m[0]))) return true;
  const pct = am.match(/(\d+(?:\.\d+)?)\s*%/);
  if (pct && ex.includes(pct[1])) return true;
  const nums = am.match(/\d[\d,]*/g);
  if (nums) {
    for (const n of nums) {
      const d = n.replace(/,/g, "");
      if (d.length >= 3 && ex.includes(d)) return true;
    }
  }
  return ex.length > 80 && amountIsVariable(amount) === false && /\d/.test(am) === false;
}

function categoryInconsistent(row) {
  const cat = String(row.commercialCategory || "").trim();
  const hay = norm(`${row.feeOrObligationName || ""} ${row.feeType || ""} ${row.amount || ""}`);
  if (!cat || !hay) return false;

  const royaltyLike = /\b(royalty|continuing\s+fee|ongoing\s+royalty)\b/i.test(hay);
  if (royaltyLike && cat !== "Recurring Brand Fee") return true;

  const sysLike = /\b(pms|pos|software|credit\s+card|gateway|server|network|technology\s+fee|saas|subscription)\b/i.test(hay);
  if (sysLike && cat !== "Required System / Technology Cost") return true;

  const mktLike = /\b(marketing|loyalty|reservation|gso|revenue\s+management|advertising\s+fund)\b/i.test(hay);
  if (mktLike && cat !== "Sales / Marketing / Loyalty / Reservation Program") return true;

  const xferLike = /\b(transfer|renewal|relicensing|comfort\s+letter)\b/i.test(hay);
  if (xferLike && cat !== "Transfer / Renewal / Relicensing") return true;

  const termLike = /\b(termination|default|liquidated|non[-\s]?compliance|penalty)\b/i.test(hay);
  if (termLike && cat !== "Termination / Default / Penalty") return true;

  return false;
}

/**
 * @param {Record<string, unknown>} row
 * @returns {Record<string, unknown>}
 */
export function auditFddFeeRow(row) {
  const issues = [];
  let score = 100;

  let sourceSupportScore = 100;
  let amountQualityScore = 100;
  let basisQualityScore = 100;
  let categoryQualityScore = 100;
  let duplicateRiskScore = 100;
  let legalRiskScore = 100;

  const reviewStatus = normalizeReviewStatus(row.reviewStatus);
  const excerpt = String(row.sourceTextExcerpt || "").trim();
  const name = String(row.feeOrObligationName || "").trim();
  const amount = row.amount != null ? String(row.amount) : "";
  const normBasis = String(row.normalizedCostBasis || "").trim();
  const rawBasis = String(row.rawCostBasisText || "").trim();
  const bConf = String(row.basisConfidence || "").trim();
  const cat = String(row.commercialCategory || "").trim();

  if (reviewStatus === "Rejected") {
    return {
      auditScore: 0,
      auditConfidence: "Low",
      auditStatus: "Do Not Auto-Approve",
      auditIssues: "Rejected row",
      autoApproveEligible: false,
      lastAuditedAt: new Date().toISOString(),
      auditVersion: FDD_ROW_AUDIT_VERSION,
      sourceSupportScore: 0,
      amountQualityScore: 0,
      basisQualityScore: 0,
      categoryQualityScore: 0,
      duplicateRiskScore: 0,
      legalRiskScore: 0,
    };
  }

  // --- Source support ---
  if (!excerptMeaningful(excerpt)) {
    score -= 20;
    sourceSupportScore -= 20;
    issues.push("Missing source excerpt");
  } else if (name && !nameSupportedByExcerpt(name, excerpt)) {
    score -= 10;
    sourceSupportScore -= 10;
    issues.push("Source support weak");
  }
  if (amountLooksConcrete(amount) && excerptMeaningful(excerpt) && !amountSupportedByExcerpt(amount, excerpt)) {
    score -= 10;
    sourceSupportScore -= 10;
    issues.push("Amount not found in source excerpt");
  }
  const docRef = String(row.documentationReference || "").trim();
  const srcItem = String(row.sourceItemNumber || "").trim();
  if (!docRef || /^unclear$/i.test(docRef) || !srcItem || /^unclear$/i.test(srcItem)) {
    score -= 5;
    sourceSupportScore -= 5;
    issues.push("Documentation or source item reference missing");
  }

  // --- Amount quality ---
  if (amountIsUnclear(amount)) {
    score -= 20;
    amountQualityScore -= 20;
    issues.push("Amount unclear");
  } else if (amountIsVariable(amount)) {
    score -= 10;
    amountQualityScore -= 10;
    issues.push("Amount variable");
  }

  // --- Basis quality ---
  if (normBasis === "Not Stated / Unclear") {
    score -= 20;
    basisQualityScore -= 20;
    issues.push("Basis unclear");
  }
  if (row.basisNeedsReview === true) {
    score -= 15;
    basisQualityScore -= 15;
    issues.push("Basis needs review");
  }
  if (normBasis === "Other / Custom Basis") {
    score -= 10;
    basisQualityScore -= 10;
    issues.push("Custom basis requires review");
  }
  if (!rawBasis) {
    score -= 5;
    basisQualityScore -= 5;
    issues.push("Raw cost basis text missing");
  }
  if (bConf === "Medium") {
    score -= 5;
    basisQualityScore -= 5;
  } else if (bConf === "Low" || !bConf) {
    score -= 10;
    basisQualityScore -= 10;
    if (bConf === "Low") issues.push("Basis confidence low");
  }

  // --- Category ---
  if (cat === "Other / Needs Review") {
    score -= 20;
    categoryQualityScore -= 20;
    issues.push("Category is Other / Needs Review");
  } else if (categoryInconsistent(row)) {
    score -= 10;
    categoryQualityScore -= 10;
    issues.push("Category-basis mismatch");
  }

  // --- Duplicate ---
  if (row.possibleDuplicate === true) {
    score -= 20;
    duplicateRiskScore -= 20;
    issues.push("Possible duplicate");
  }

  // --- Legal / commercial / sensitive categories ---
  if (row.needsLegalReview === true) {
    score -= 30;
    legalRiskScore -= 30;
    issues.push("Legal review required");
  }
  if (row.needsCommercialReview === true) {
    score -= 10;
    legalRiskScore -= 10;
  }
  if (cat === "Legal / Operational Obligation") {
    score -= 20;
    legalRiskScore -= 20;
    issues.push("Legal/termination/transfer row excluded from auto-approval");
  }
  if (cat === "Termination / Default / Penalty") {
    score -= 20;
    legalRiskScore -= 20;
    if (!issues.includes("Legal/termination/transfer row excluded from auto-approval")) {
      issues.push("Legal/termination/transfer row excluded from auto-approval");
    }
  }
  if (cat === "Transfer / Renewal / Relicensing") {
    score -= 10;
    legalRiskScore -= 10;
    if (!issues.includes("Legal/termination/transfer row excluded from auto-approval")) {
      issues.push("Legal/termination/transfer row excluded from auto-approval");
    }
  }

  score = clamp(Math.round(score), 0, 100);
  sourceSupportScore = clamp(Math.round(sourceSupportScore), 0, 100);
  amountQualityScore = clamp(Math.round(amountQualityScore), 0, 100);
  basisQualityScore = clamp(Math.round(basisQualityScore), 0, 100);
  categoryQualityScore = clamp(Math.round(categoryQualityScore), 0, 100);
  duplicateRiskScore = clamp(Math.round(duplicateRiskScore), 0, 100);
  legalRiskScore = clamp(Math.round(legalRiskScore), 0, 100);

  let auditConfidence = "Low";
  if (score >= 90) auditConfidence = "High";
  else if (score >= 70) auditConfidence = "Medium";

  let autoApproveEligible = computeAutoApproveEligibleCore(row, score);

  if (reviewStatus === "Approved") {
    autoApproveEligible = false;
  }

  const audit = {
    auditScore: score,
    auditConfidence,
    auditStatus: "Needs Review",
    auditIssues: issues.join("; "),
    autoApproveEligible,
    lastAuditedAt: new Date().toISOString(),
    auditVersion: FDD_ROW_AUDIT_VERSION,
    sourceSupportScore,
    amountQualityScore,
    basisQualityScore,
    categoryQualityScore,
    duplicateRiskScore,
    legalRiskScore,
  };

  if (score >= 90 && autoApproveEligible) {
    audit.auditStatus = "High Confidence";
  } else if (score >= 80) {
    audit.auditStatus = "Quick Review";
  } else if (score >= 60) {
    audit.auditStatus = "Needs Review";
  } else {
    audit.auditStatus = "Manual Review Required";
  }

  return audit;
}

/** Eligibility per spec (excluding Rejected / Approved handled by caller). */
function computeAutoApproveEligibleCore(row, auditScore) {
  if (auditScore < 90) return false;
  if (normalizeReviewStatus(row.reviewStatus) !== "Needs Review") return false;
  if (row.possibleDuplicate === true) return false;
  if (row.needsLegalReview === true) return false;
  if (row.basisNeedsReview === true) return false;
  const bc = String(row.basisConfidence || "").trim();
  if (bc !== "High" && bc !== "Medium") return false;
  if (!excerptMeaningful(String(row.sourceTextExcerpt || ""))) return false;
  const amount = row.amount != null ? String(row.amount) : "";
  if (amountIsUnclear(amount) || amountIsVariable(amount)) return false;
  const cat = String(row.commercialCategory || "").trim();
  const blocked = new Set([
    "Other / Needs Review",
    "Legal / Operational Obligation",
    "Termination / Default / Penalty",
    "Transfer / Renewal / Relicensing",
  ]);
  if (blocked.has(cat)) return false;
  if (row.needsCommercialReview === true) return false;
  return true;
}

function normalizeReviewStatus(s) {
  const t = String(s || "")
    .trim()
    .toLowerCase();
  if (t === "approved" || t === "approve") return "Approved";
  if (t === "rejected" || t === "reject") return "Rejected";
  if (t === "needs review" || t === "need review") return "Needs Review";
  if (t === "draft") return "Draft";
  return String(s || "").trim() || "Needs Review";
}

/**
 * Strict auto-approve eligibility (bulk approval gate).
 * @param {Record<string, unknown>} row
 * @param {Record<string, unknown>} [audit] — from auditFddFeeRow(row); recomputed if omitted
 */
export function isAutoApproveEligible(row, audit) {
  const a = audit || auditFddFeeRow(row);
  if (a.autoApproveEligible !== true) return false;
  return computeAutoApproveEligibleCore(row, Number(a.auditScore) || 0);
}

/**
 * @param {Record<string, unknown>[]} rows
 */
export function auditFddFeeRows(rows) {
  if (!Array.isArray(rows)) return [];
  for (const r of rows) {
    mergeAuditResultIntoRow(r);
  }
  return rows;
}

/**
 * Mutates row with latest audit fields.
 * @param {Record<string, unknown>} row
 */
export function mergeAuditResultIntoRow(row) {
  const a = auditFddFeeRow(row);
  for (const k of AUDIT_ROW_FIELD_KEYS) {
    if (Object.prototype.hasOwnProperty.call(a, k)) row[k] = a[k];
  }
  return row;
}
