/**
 * FDD fee row — normalized cost basis inference and read-time backfill.
 * Used by api/fdd-intelligence.js (Airtable + memory + AI extraction).
 */

/** @type {readonly string[]} */
export const COST_BASIS_TEXT_KEYS = [
  "normalizedCostBasis",
  "rawCostBasisText",
  "amountFormulaType",
  "calculationUnit",
  "revenueBase",
  "unitRate",
  "percentageRate",
  "fixedAmount",
  "formulaNotes",
  "basisConfidence",
];

export const NORMALIZED_COST_BASIS_VALUES = new Set([
  "Gross Room Sales / Room Revenue",
  "Gross Revenue / Total Revenue",
  "F&B Revenue",
  "Residential Revenue",
  "Per Guestroom / Per Key",
  "Per Occupied Room / Room Night",
  "Per Reservation / Booking",
  "Per Lead / Referral",
  "Per Transaction",
  "Per User / Device / Workstation",
  "Per Property / Hotel",
  "Per Month",
  "Per Year / Annual",
  "Lump Sum / One-Time",
  "Greater-Of Formula",
  "Mixed Formula",
  "Actual Cost / Pass-Through",
  "Variable / As Incurred",
  "Other / Custom Basis",
  "Not Stated / Unclear",
]);

export const AMOUNT_FORMULA_TYPE_VALUES = new Set([
  "Fixed",
  "Percentage",
  "Per-Unit",
  "Recurring Fixed",
  "Greater-Of",
  "Mixed Formula",
  "Actual Cost",
  "Variable",
  "Unclear",
]);

export const BASIS_CONFIDENCE_VALUES = new Set(["High", "Medium", "Low"]);

export function parseBasisNeedsReviewFlag(v) {
  if (v === true) return true;
  if (v === false) return false;
  const s = String(v ?? "")
    .trim()
    .toLowerCase();
  if (s === "true" || s === "1" || s === "yes") return true;
  return false;
}

export function isBlankCostBasisVal(v) {
  const s = String(v ?? "").trim();
  if (!s) return true;
  if (s.toLowerCase() === "unclear") return true;
  return false;
}

function clampLine(s, max = 900) {
  const t = String(s ?? "").trim();
  return t.length > max ? t.slice(0, max) : t;
}

function haystackFromRow(row) {
  return [
    row.amount,
    row.basis,
    row.frequency,
    row.dueTiming,
    row.feeOrObligationName,
    row.sourceTextExcerpt,
    row.commercialCategory,
    row.lifecyclePhase,
    row.appliesWhen,
    row.amountType,
  ]
    .filter((x) => x != null && String(x).trim() !== "")
    .join(" ")
    .toLowerCase();
}

function verbatimBasisFromRow(row) {
  const b = String(row.basis ?? "").trim();
  const ex = String(row.sourceTextExcerpt ?? "").trim();
  if (b.length >= 8 && !/^unclear$/i.test(b)) return b;
  if (ex.length >= 12) return ex;
  return [b, ex].filter((x) => x && String(x).trim()).join(" · ").trim() || String(row.amount ?? "").trim();
}

/** Plain-English unit for non-standard "per …" bases (comfort letter, audit, day, etc.). */
export function inferCustomCalculationUnit(hay, basis) {
  const t = `${String(basis || "")} ${String(hay || "")}`.toLowerCase();
  const perPhrase = t.match(/\bper\s+([a-z0-9][a-z0-9\s\-]{1,70})(?:[.,;]|$)/i);
  if (perPhrase) {
    const frag = perPhrase[1].trim().replace(/\s+/g, " ").slice(0, 72);
    if (frag.length >= 2) return `per ${frag}`;
  }
  if (/\bcomfort\s+letter\b/i.test(t)) return "per comfort letter";
  if (/\bper\s+audit\b|\baudit\s+fee\b/i.test(t)) return "per audit";
  if (/\bper\s+person\b/i.test(t)) return "per person";
  if (/\bper\s+day\b/i.test(t)) return "per day";
  if (/\bper\s+request\b/i.test(t)) return "per request";
  if (/\bper\s+event\b/i.test(t)) return "per event";
  if (/\bper\s+mile\b/i.test(t)) return "per mile";
  if (/\bper\s+kilometer\b/i.test(t)) return "per kilometer";
  return "";
}

function hasMeaningfulBasisText(row) {
  const b = String(row.basis ?? "").trim();
  if (b.length >= 12 && !/^unclear$/i.test(b)) return true;
  const ex = String(row.sourceTextExcerpt ?? "").trim();
  if (ex.length >= 50) return true;
  const a = String(row.amount ?? "").trim();
  if (a.length >= 10 && !/^unclear$/i.test(a)) return true;
  return false;
}

/** True only when the FDD excerpt + basis spell out a standard basis clearly (avoid false "review not needed"). */
function basisSourceVeryClear(row) {
  const ex = String(row.sourceTextExcerpt || "").trim();
  const b = String(row.basis || "").trim();
  const t = `${ex} ${b}`.toLowerCase();
  if (ex.length < 100) return false;
  if (/\b%.*(gross\s+room|room\s+sales|room\s+revenue|rooms\s+revenue)\b/i.test(t)) return true;
  if (/\b%.*(gross\s+revenue|total\s+revenue)\b/i.test(t)) return true;
  if (/\bpercent(age)?\s+of\s+gross\s+room\b/i.test(t)) return true;
  if (/\bpercent(age)?\s+of\s+(gross|total)\s+revenue\b/i.test(t)) return true;
  if (/\bper\s+(guestroom|key|unit|suite)\b/i.test(t) && /\$/.test(t)) return true;
  if (/\bpass[-\s]?through\b|\bactual\s+cost\b|\breimbursed\b/i.test(t) && ex.length >= 120) return true;
  return false;
}

/** First percentage like 6.0% or 3.85% from text. */
export function extractPercentageRate(text) {
  const t = String(text || "");
  const m = t.match(/(\d+(?:\.\d+)?)\s*%/);
  return m ? `${m[1]}%` : "";
}

/** Primary dollar amount token (first substantial $ match). */
export function extractFixedAmountSnippet(text) {
  const t = String(text || "");
  const m = t.match(/\$[\d,]+(?:\.\d{1,2})?/);
  return m ? m[0] : "";
}

/** e.g. "$500 per guestroom" */
export function extractUnitRateSnippet(text) {
  const t = String(text || "");
  const m = t.match(/\$[\d,]+(?:\.\d{1,2})?\s*(?:\/|\s+per\s+)\s*[^.;,]{1,80}/i);
  return m ? m[0].trim().slice(0, 200) : "";
}

/**
 * Rules-based cost basis inference (MVP). Does not call external services.
 * @param {Record<string, unknown>} row
 * @returns {Record<string, unknown>}
 */
export function inferCostBasisFields(row) {
  const hay = haystackFromRow(row);
  const amount = String(row.amount ?? "").trim();
  const basis = String(row.basis ?? "").trim();
  const freq = String(row.frequency ?? "").trim().toLowerCase();
  const rawBits = [basis, amount, row.frequency, row.dueTiming].filter(Boolean).join(" · ");
  let rawCostBasisText = clampLine(rawBits || hay.slice(0, 500), 8000);

  let normalizedCostBasis = "Not Stated / Unclear";
  let amountFormulaType = "Unclear";
  let calculationUnit = "";
  let revenueBase = "";
  let unitRate = "";
  let percentageRate = extractPercentageRate(amount) || extractPercentageRate(basis) || extractPercentageRate(hay);
  let fixedAmount = extractFixedAmountSnippet(amount) || extractFixedAmountSnippet(basis) || extractFixedAmountSnippet(hay);
  let formulaNotes = "";
  let basisConfidence = "Low";
  let basisNeedsReview = true;

  const hasPct = /%|\bpercent(age)?\b/i.test(hay);
  const hasDollar = /\$[\d,]+/.test(hay);
  const hasPer = /\bper\b/i.test(hay);

  if (/\b(actual\s+cost|pass-?through|reimbursed|third[-\s]?party|as\s+billed|invoice\s+from\s+vendor)\b/i.test(hay)) {
    normalizedCostBasis = "Actual Cost / Pass-Through";
    amountFormulaType = "Actual Cost";
    basisConfidence = hasPct || hasDollar ? "Medium" : "Medium";
    basisNeedsReview = false;
  } else if (/\bgreater\s+of\b|\blesser\s+of\b/i.test(hay)) {
    normalizedCostBasis = "Greater-Of Formula";
    amountFormulaType = "Greater-Of";
    basisConfidence = "Medium";
    basisNeedsReview = true;
  } else if (/\b(varies|variable|then[-\s]?current|not\s+determinable|cannot\s+determine|tbd|n\/a)\b/i.test(hay) && !hasPct && !hasDollar) {
    normalizedCostBasis = "Variable / As Incurred";
    amountFormulaType = "Variable";
    basisConfidence = "Low";
    basisNeedsReview = true;
  } else if (
    (hasPct && hasDollar && hasPer) ||
    (hay.split("%").length > 2 && hasDollar) ||
    (/\bplus\b/i.test(hay) && hasPct && hasDollar)
  ) {
    normalizedCostBasis = "Mixed Formula";
    amountFormulaType = "Mixed Formula";
    formulaNotes = "Multiple amount components suggested by text.";
    basisConfidence = "Medium";
    basisNeedsReview = true;
  } else if (/\b(group\s+)?lead\b|\bcatering\s+lead\b|\bextended[-\s]?stay\s+lead\b|\bper\s+lead\b|\breferral\b/i.test(hay)) {
    normalizedCostBasis = "Per Lead / Referral";
    amountFormulaType = "Per-Unit";
    calculationUnit = "lead";
    unitRate = extractUnitRateSnippet(`${amount} ${basis}`) || fixedAmount;
    basisConfidence = unitRate ? "Medium" : "Low";
    basisNeedsReview = !unitRate;
  } else if (/\bper\s+(occupied\s+)?room\s+night\b|\broom\s+night\b|\bconsumed\s+room\s+night\b/i.test(hay)) {
    normalizedCostBasis = "Per Occupied Room / Room Night";
    amountFormulaType = "Per-Unit";
    calculationUnit = "room night";
    unitRate = extractUnitRateSnippet(`${amount} ${basis}`) || fixedAmount;
    basisConfidence = unitRate ? "Medium" : "Low";
    basisNeedsReview = !unitRate;
  } else if (
    /\bper\s+(guestroom|key|unit|suite)\b|\bper\s+available\s+room\b/i.test(hay)
  ) {
    normalizedCostBasis = "Per Guestroom / Per Key";
    amountFormulaType = "Per-Unit";
    calculationUnit = "guestroom";
    unitRate = extractUnitRateSnippet(`${amount} ${basis}`) || fixedAmount;
    basisConfidence = unitRate ? "Medium" : "Low";
    basisNeedsReview = !unitRate;
  } else if (/\bper\s+(reservation|booking)\b|\bper\s+call\b/i.test(hay)) {
    normalizedCostBasis = "Per Reservation / Booking";
    amountFormulaType = "Per-Unit";
    calculationUnit = "reservation";
    unitRate = extractUnitRateSnippet(`${amount} ${basis}`) || fixedAmount;
    basisConfidence = unitRate ? "Medium" : "Low";
    basisNeedsReview = !unitRate;
  } else if (/\bper\s+transaction\b|\bsettlement\s+transaction\b/i.test(hay)) {
    normalizedCostBasis = "Per Transaction";
    amountFormulaType = "Per-Unit";
    calculationUnit = "transaction";
    unitRate = extractUnitRateSnippet(`${amount} ${basis}`) || fixedAmount;
    basisConfidence = unitRate ? "Medium" : "Low";
    basisNeedsReview = !unitRate;
  } else if (/\bper\s+(user|device|workstation|merchant\s+id|pc)\b/i.test(hay)) {
    normalizedCostBasis = "Per User / Device / Workstation";
    amountFormulaType = "Per-Unit";
    unitRate = extractUnitRateSnippet(`${amount} ${basis}`) || fixedAmount;
    basisConfidence = unitRate ? "Medium" : "Low";
    basisNeedsReview = !unitRate;
  } else if (/\bper\s+(hotel|property)\b/i.test(hay)) {
    normalizedCostBasis = "Per Property / Hotel";
    amountFormulaType = /\bmonth(ly)?\b|\bannual\b|\bper\s+year\b/i.test(hay) ? "Recurring Fixed" : "Per-Unit";
    calculationUnit = "property";
    unitRate = extractUnitRateSnippet(`${amount} ${basis}`) || fixedAmount;
    basisConfidence = "Medium";
    basisNeedsReview = !unitRate && !fixedAmount;
  } else if (/\b(f&b|food\s+and\s+beverage)\s+revenue\b|\bpercent(age)?\s+of\s+f&b\b/i.test(hay)) {
    normalizedCostBasis = "F&B Revenue";
    revenueBase = "F&B Revenue";
    amountFormulaType = hasPct ? "Percentage" : "Unclear";
    basisConfidence = hasPct ? "High" : "Medium";
    basisNeedsReview = !hasPct;
  } else if (/\bresidential\b.*\brevenue\b|\bresidential\s+revenue\b/i.test(hay)) {
    normalizedCostBasis = "Residential Revenue";
    revenueBase = "Residential Revenue";
    amountFormulaType = hasPct ? "Percentage" : "Unclear";
    basisConfidence = hasPct ? "Medium" : "Low";
    basisNeedsReview = !hasPct;
  } else if (
    /%.*(gross\s+room|room\s+sales|room\s+revenue|rooms\s+revenue)/i.test(hay) ||
    /(gross\s+room|room\s+sales|room\s+revenue|rooms\s+revenue).*%/i.test(hay) ||
    /\bpercent(age)?\s+of\s+gross\s+room\b/i.test(hay)
  ) {
    normalizedCostBasis = "Gross Room Sales / Room Revenue";
    revenueBase = "Gross Room Sales";
    amountFormulaType = "Percentage";
    basisConfidence = hasPct ? "High" : "Medium";
    basisNeedsReview = !hasPct;
  } else if (/%.*(gross\s+revenue|total\s+revenue)/i.test(hay) || /(gross\s+revenue|total\s+revenue).*%/i.test(hay) || /\bpercent(age)?\s+of\s+(gross|total)\s+revenue\b/i.test(hay)) {
    normalizedCostBasis = "Gross Revenue / Total Revenue";
    revenueBase = "Total Revenue";
    amountFormulaType = "Percentage";
    basisConfidence = hasPct ? "High" : "Medium";
    basisNeedsReview = !hasPct;
  } else if (/\b(monthly|per\s+month|each\s+month)\b/i.test(hay) && (hasDollar || /\bflat\b/i.test(hay))) {
    normalizedCostBasis = "Per Month";
    amountFormulaType = "Recurring Fixed";
    calculationUnit = "month";
    if (!fixedAmount && hasDollar) fixedAmount = extractFixedAmountSnippet(hay);
    basisConfidence = fixedAmount ? "Medium" : "Low";
    basisNeedsReview = !fixedAmount;
  } else if (/\b(annual|per\s+year|yearly)\b/i.test(hay) && (hasDollar || /\bflat\b/i.test(hay))) {
    normalizedCostBasis = "Per Year / Annual";
    amountFormulaType = "Recurring Fixed";
    calculationUnit = "year";
    if (!fixedAmount && hasDollar) fixedAmount = extractFixedAmountSnippet(hay);
    basisConfidence = fixedAmount ? "Medium" : "Low";
    basisNeedsReview = !fixedAmount;
  } else if (/\b(lump\s+sum|one[-\s]?time|initial\s+fee|due\s+with\s+application|upon\s+signing)\b/i.test(hay) || /\bone[-\s]?time\b/i.test(freq)) {
    normalizedCostBasis = "Lump Sum / One-Time";
    amountFormulaType = "Fixed";
    if (!fixedAmount && hasDollar) fixedAmount = extractFixedAmountSnippet(hay);
    basisConfidence = fixedAmount || hasDollar ? "Medium" : "Low";
    basisNeedsReview = !(fixedAmount || hasDollar);
  } else if (hasPct) {
    normalizedCostBasis = "Other / Custom Basis";
    amountFormulaType = "Percentage";
    calculationUnit = inferCustomCalculationUnit(hay, basis);
    formulaNotes = "Percentage found without a standard revenue-base phrase; see raw text.";
    basisConfidence = percentageRate ? "Medium" : "Low";
    basisNeedsReview = true;
  } else if (hasPer && hasDollar) {
    normalizedCostBasis = "Other / Custom Basis";
    amountFormulaType = "Per-Unit";
    calculationUnit = inferCustomCalculationUnit(hay, basis);
    unitRate = extractUnitRateSnippet(`${amount} ${basis}`);
    basisConfidence = unitRate ? "Medium" : "Low";
    basisNeedsReview = true;
    formulaNotes = "Per-unit dollar charge; unit type not mapped to a standard category.";
  } else if (hasDollar && !hasPct) {
    normalizedCostBasis = "Other / Custom Basis";
    amountFormulaType = "Fixed";
    if (!fixedAmount) fixedAmount = extractFixedAmountSnippet(hay);
    calculationUnit = inferCustomCalculationUnit(hay, basis);
    basisConfidence = fixedAmount ? "Medium" : "Low";
    basisNeedsReview = true;
    formulaNotes = "Dollar amount without a clear standard fee shape; see FDD.";
  } else if (hasMeaningfulBasisText(row)) {
    normalizedCostBasis = "Other / Custom Basis";
    if (hasPct) amountFormulaType = "Percentage";
    else if (hasPer) amountFormulaType = "Per-Unit";
    else if (hasDollar) amountFormulaType = "Fixed";
    else amountFormulaType = "Unclear";
    calculationUnit = inferCustomCalculationUnit(hay, basis);
    basisConfidence = "Low";
    basisNeedsReview = true;
  }

  if (normalizedCostBasis === "Other / Custom Basis") {
    rawCostBasisText = clampLine(verbatimBasisFromRow(row), 8000);
    if (!calculationUnit) calculationUnit = inferCustomCalculationUnit(hay, basis);
    if (basisSourceVeryClear(row)) {
      basisNeedsReview = false;
      basisConfidence = "High";
    } else {
      basisNeedsReview = true;
      if (basisConfidence === "Low" && (percentageRate || unitRate || fixedAmount)) basisConfidence = "Medium";
    }
  }

  if (String(row.sourceTextExcerpt || "").trim().length > 40 && basisConfidence === "Low" && normalizedCostBasis !== "Not Stated / Unclear" && normalizedCostBasis !== "Other / Custom Basis") {
    basisConfidence = "Medium";
  }

  if (normalizedCostBasis === "Not Stated / Unclear") {
    basisNeedsReview = true;
    amountFormulaType = "Unclear";
  }

  const out = {
    normalizedCostBasis,
    rawCostBasisText,
    amountFormulaType,
    calculationUnit: clampLine(calculationUnit, 200),
    revenueBase: clampLine(revenueBase, 200),
    unitRate: clampLine(unitRate, 500),
    percentageRate: clampLine(percentageRate, 80),
    fixedAmount: clampLine(fixedAmount, 200),
    formulaNotes: clampLine(formulaNotes, 8000),
    basisConfidence: BASIS_CONFIDENCE_VALUES.has(basisConfidence) ? basisConfidence : "Low",
    basisNeedsReview: !!basisNeedsReview,
  };

  if (!NORMALIZED_COST_BASIS_VALUES.has(out.normalizedCostBasis)) out.normalizedCostBasis = "Not Stated / Unclear";
  if (!AMOUNT_FORMULA_TYPE_VALUES.has(out.amountFormulaType)) out.amountFormulaType = "Unclear";

  return out;
}

/**
 * Fills missing cost-basis fields from inference (legacy Airtable rows, partial AI rows).
 * @param {Record<string, unknown>} row — mutated in place
 */
export function mergeCostBasisFromInference(row) {
  const inferred = inferCostBasisFields(row);
  for (const k of COST_BASIS_TEXT_KEYS) {
    if (isBlankCostBasisVal(row[k]) && !isBlankCostBasisVal(inferred[k])) {
      row[k] = inferred[k];
    }
  }
  if (isBlankCostBasisVal(row.rawCostBasisText) && inferred.rawCostBasisText) {
    row.rawCostBasisText = inferred.rawCostBasisText;
  }
  row.basisNeedsReview = !!row.basisNeedsReview || !!inferred.basisNeedsReview;
  if (isBlankCostBasisVal(row.basisConfidence) || String(row.basisConfidence).toLowerCase() === "unclear") {
    row.basisConfidence = inferred.basisConfidence;
  }
  if (isBlankCostBasisVal(row.normalizedCostBasis) || String(row.normalizedCostBasis) === "Not Stated / Unclear") {
    if (!isBlankCostBasisVal(inferred.normalizedCostBasis) && inferred.normalizedCostBasis !== "Not Stated / Unclear") {
      row.normalizedCostBasis = inferred.normalizedCostBasis;
    }
  }
  if (isBlankCostBasisVal(row.formulaNotes) && !isBlankCostBasisVal(inferred.formulaNotes)) {
    row.formulaNotes = inferred.formulaNotes;
  }
  if (isBlankCostBasisVal(row.percentageRate) && inferred.percentageRate) row.percentageRate = inferred.percentageRate;
  if (isBlankCostBasisVal(row.unitRate) && inferred.unitRate) row.unitRate = inferred.unitRate;
  if (isBlankCostBasisVal(row.fixedAmount) && inferred.fixedAmount) row.fixedAmount = inferred.fixedAmount;
  if (isBlankCostBasisVal(row.calculationUnit) && inferred.calculationUnit) row.calculationUnit = inferred.calculationUnit;
  if (isBlankCostBasisVal(row.revenueBase) && inferred.revenueBase) row.revenueBase = inferred.revenueBase;
  if (!row.normalizedCostBasis || String(row.normalizedCostBasis).trim() === "" || row.normalizedCostBasis === "Not Stated / Unclear") {
    row.basisNeedsReview = true;
  }
}
