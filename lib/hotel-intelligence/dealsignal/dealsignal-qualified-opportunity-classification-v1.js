/**
 * DealSignal — Permanent Qualified Opportunity classification (founder rule).
 *
 * Completed / committed / renewed / retained / awarded commercial events are
 * NEVER Qualified solely because they are verified facts.
 *
 * Qualified requires ALL of:
 *   1. A relevant BCT/MCT decision exists
 *   2. The decision remains unresolved
 *   3. A credible competitive engagement pathway is evidenced
 *
 * Formal RFP is NOT required. Early-engagement / Emerging may be preserved
 * without promotion to Qualified. Historical Verified facts must not be
 * promoted to Verified-open opportunities.
 *
 * Research assessment labels are outputs — not authorization for new Airtable enums.
 * CUSTOMER_BLOCKED. Coordinate with Integration Controller central eligibility fix.
 */

export const DEALSIGNAL_QUALIFIED_OPPORTUNITY_CLASSIFICATION_V1 =
  "dealsignal-qualified-opportunity-classification-v1";

/** Research assessment outputs (not Airtable select values unless already contracted). */
export const QUALIFIED_ASSESSMENT = Object.freeze({
  QUALIFIED: "QUALIFIED",
  EMERGING_PROSPECTIVE: "EMERGING_PROSPECTIVE",
  EARLY_ENGAGEMENT: "EARLY_ENGAGEMENT",
  WATCHLIST: "WATCHLIST",
  HISTORICAL_EXECUTED: "HISTORICAL_EXECUTED",
  COMMITTED_CLOSED: "COMMITTED_CLOSED",
  RETAINED_RENEWED: "RETAINED_RENEWED",
  AMBIGUOUS_REVIEW: "AMBIGUOUS_REVIEW",
  NOT_COMMERCIAL: "NOT_COMMERCIAL",
});

/** Commercial-status buckets that are never open Qualified. */
export const NON_OPEN_STATUSES = Object.freeze([
  "COMMITTED",
  "COMMITTED_CLOSED",
  "CLOSED",
  "EXECUTED",
  "HISTORICAL",
  "RETAINED",
  "RENEWED",
  "AWARDED",
  "COMPLETED",
]);

const CLOSED_STATUS_RE =
  /\b(committed|closed|executed|awarded|completed|retained|renewed|historical|reflagged|conversion\s+complete|already\s+(?:converted|appointed|selected)|operator\s+has\s+been\s+appointed|brand\s+signed)\b/i;

const UNRESOLVED_RE =
  /\b(unresolved|uncommitted|unnamed\s+brand|no\s+hotel\s+partner|no\s+partner\s+identified|seeking\s+(?:brand|operator|partner|proposals)|brand\s+(?:not|still\s+not)(?:\s+\w+){0,3}\s+(?:named|selected)|shopping\s+brands?|vigorously\s+engage\s+(?:hotel\s+)?brands?|evaluating\s+alternatives?|selection\s+(?:underway|open|process)|pre[- ]?selection|partner\s+not\s+(?:yet\s+)?(?:selected|identified|named))\b/i;

const PATHWAY_RE =
  /\b(competitive\s+pathway|brand\s+development|approach(?:able)?|pitch|outreach|owner\s+(?:shopping|engaging|seeking)|credible\s+(?:bct|mct)|engagement\s+pathway)\b/i;

const BCT_MCT_RE =
  /\b(bct|mct|brand\s+conversion|franchise|soft[- ]?brand|management\s+(?:contract|agreement|change)|operator\s+selection|hma|reflag|affiliation)\b/i;

/** Expiry / at-will / contractual watch alone ≠ Qualified. */
const CONTRACTUAL_ONLY_RE =
  /\b(expiration[- ]only|at[- ]will|contractual\s+(?:watch|flexibility)|franchise\s+expiration|term\s+end|auto(?:matic)?[- ]renewal|no\s+affirmative\s+owner[- ]process|without\s+(?:owner[- ]process|outreach)\s+evidence|contractual_watch)\b/i;

/**
 * Hard exclusions — verified commercial events that must not become open Qualified.
 * Founder examples + Track A patterns.
 */
export const HARD_EXCLUSION_PATTERNS = Object.freeze([
  {
    id: "AC_CARTAGENA_COMPLETED_REFLAG",
    test: (t) => /ac\s+hotel\s+cartagena/i.test(t) && /reflag|converted|completed|now\s+operat/i.test(t),
    reason: "COMPLETED_REFLAG_NOT_OPEN_QUALIFIED",
    assessment: QUALIFIED_ASSESSMENT.HISTORICAL_EXECUTED,
  },
  {
    id: "GHL_SONESTA_MFA_RENEWAL_2034",
    test: (t) => /ghl/i.test(t) && /sonesta/i.test(t) && /(2034|master\s+franchise|mfa|extension|renew)/i.test(t),
    reason: "MASTER_FRANCHISE_RENEWAL_RETAINED_NOT_OPEN",
    assessment: QUALIFIED_ASSESSMENT.RETAINED_RENEWED,
  },
  {
    id: "COMPLETED_FRANCHISE_RENEWAL",
    test: (t) => /franchise\s+renew/i.test(t) && /(completed|extended|renewed|through\s+20)/i.test(t),
    reason: "COMPLETED_FRANCHISE_RENEWAL_NOT_OPEN",
    assessment: QUALIFIED_ASSESSMENT.RETAINED_RENEWED,
  },
  {
    id: "ALREADY_APPOINTED_OPERATOR",
    test: (t) =>
      /(already[- ]appointed|operator\s+(?:has\s+been\s+)?(?:selected|appointed)|selected\s+to\s+manage|aimbridge\s+(?:latam\s+)?(?:selected|to\s+manage))/i.test(
        t,
      ) && !/seeking|unresolved|no\s+partner/i.test(t),
    reason: "OPERATOR_ALREADY_APPOINTED_NOT_OPEN",
    assessment: QUALIFIED_ASSESSMENT.COMMITTED_CLOSED,
  },
  {
    id: "EXECUTED_MANAGEMENT_REPLACEMENT",
    test: (t) =>
      /(management\s+replacement|hma\s+terminated|new\s+hma|highgate\s+hma|completed\s+manager\s+transition)/i.test(t) &&
      /(effective|completed|historical|closed)/i.test(t),
    reason: "EXECUTED_MANAGEMENT_REPLACEMENT_NOT_OPEN",
    assessment: QUALIFIED_ASSESSMENT.HISTORICAL_EXECUTED,
  },
]);

function textBlob(row = {}) {
  return [
    row.hotelName,
    row.owner,
    row.commercialStatus,
    row.recordType,
    row.assessmentLabel,
    row.commercialTrigger,
    row.decisionEvidence,
    row.commercialCategory,
    row.opportunityType,
    row.notes,
    row.newBrand,
    row.priorBrand,
    typeof row.commercialThesis === "object"
      ? Object.values(row.commercialThesis || {}).join(" ")
      : row.commercialThesis,
  ]
    .filter(Boolean)
    .join(" ");
}

function normalizeStatus(row = {}) {
  return String(row.commercialStatus || row.status || "")
    .trim()
    .toUpperCase()
    .replace(/\s+/g, "_");
}

/**
 * True when commercialStatus / stream already marks a non-open outcome.
 */
export function isNonOpenCommercialStatus(row = {}) {
  const s = normalizeStatus(row);
  if (!s) return false;
  if (NON_OPEN_STATUSES.some((x) => s === x || s.includes(x))) return true;
  if (String(row.stream || "").toUpperCase() === "HISTORICAL") return true;
  if (String(row.recordType || "").toUpperCase().includes("HISTORICAL")) return true;
  if (String(row.recordType || "").toUpperCase().includes("RETAIN")) return true;
  return false;
}

/**
 * Gate: may this row count as open Qualified / first-100 prospective?
 * Never invents openness from verification alone.
 */
export function classifyQualifiedOpportunity(row = {}) {
  const blob = textBlob(row);
  const reasons = [];

  for (const pattern of HARD_EXCLUSION_PATTERNS) {
    if (pattern.test(blob)) {
      return {
        isQualified: false,
        assessment: pattern.assessment,
        reasons: [pattern.reason, pattern.id],
        countsTowardFirst100: false,
        mayPersistAsHistorical: true,
        ruleId: DEALSIGNAL_QUALIFIED_OPPORTUNITY_CLASSIFICATION_V1,
      };
    }
  }

  if (isNonOpenCommercialStatus(row)) {
    const assessment = /retain|renew/i.test(blob)
      ? QUALIFIED_ASSESSMENT.RETAINED_RENEWED
      : /commit/i.test(normalizeStatus(row))
        ? QUALIFIED_ASSESSMENT.COMMITTED_CLOSED
        : QUALIFIED_ASSESSMENT.HISTORICAL_EXECUTED;
    return {
      isQualified: false,
      assessment,
      reasons: ["NON_OPEN_COMMERCIAL_STATUS", normalizeStatus(row) || "HISTORICAL_STREAM"],
      countsTowardFirst100: false,
      mayPersistAsHistorical: true,
      ruleId: DEALSIGNAL_QUALIFIED_OPPORTUNITY_CLASSIFICATION_V1,
    };
  }

  const statusNorm = normalizeStatus(row);
  if (statusNorm === "WATCHLIST" || /CONTRACTUAL_WATCH/i.test(String(row.recordType || ""))) {
    return {
      isQualified: false,
      assessment: QUALIFIED_ASSESSMENT.WATCHLIST,
      reasons: ["WATCHLIST_OR_CONTRACTUAL_WATCH_NOT_QUALIFIED"],
      countsTowardFirst100: false,
      mayPersistAsHistorical: true,
      ruleId: DEALSIGNAL_QUALIFIED_OPPORTUNITY_CLASSIFICATION_V1,
    };
  }

  // Explicit closed language without open/unresolved cues
  if (CLOSED_STATUS_RE.test(blob) && !UNRESOLVED_RE.test(blob) && row.decisionUnresolved !== true) {
    return {
      isQualified: false,
      assessment: QUALIFIED_ASSESSMENT.COMMITTED_CLOSED,
      reasons: ["CLOSED_LANGUAGE_WITHOUT_UNRESOLVED_EVIDENCE"],
      countsTowardFirst100: false,
      mayPersistAsHistorical: true,
      ruleId: DEALSIGNAL_QUALIFIED_OPPORTUNITY_CLASSIFICATION_V1,
    };
  }

  // Expiry / at-will / contractual flexibility alone — never Qualified
  if (
    (CONTRACTUAL_ONLY_RE.test(blob) || row.contractualWatchOnly === true) &&
    row.decisionUnresolved !== true &&
    !UNRESOLVED_RE.test(blob)
  ) {
    return {
      isQualified: false,
      assessment: QUALIFIED_ASSESSMENT.WATCHLIST,
      reasons: ["CONTRACTUAL_FLEXIBILITY_ALONE_NOT_QUALIFIED"],
      countsTowardFirst100: false,
      mayPersistAsHistorical: true,
      ruleId: DEALSIGNAL_QUALIFIED_OPPORTUNITY_CLASSIFICATION_V1,
    };
  }

  const hasBctMct =
    row.bctMctDecisionExists === true ||
    /^(BCT|MCT)$/i.test(String(row.opportunityType || "")) ||
    BCT_MCT_RE.test(blob);
  if (!hasBctMct) {
    return {
      isQualified: false,
      assessment: QUALIFIED_ASSESSMENT.NOT_COMMERCIAL,
      reasons: ["NO_RELEVANT_BCT_MCT_DECISION"],
      countsTowardFirst100: false,
      mayPersistAsHistorical: false,
      ruleId: DEALSIGNAL_QUALIFIED_OPPORTUNITY_CLASSIFICATION_V1,
    };
  }
  reasons.push("BCT_MCT_DECISION_PRESENT");

  // Unresolved requires explicit evidence — ENGAGE_NOW alone is insufficient
  const unresolved = row.decisionUnresolved === true || UNRESOLVED_RE.test(blob);
  if (!unresolved) {
    return {
      isQualified: false,
      assessment: /ENGAGE_NOW|BUILD_RELATIONSHIP/i.test(String(row.engagementPriority || ""))
        ? String(row.engagementPriority).toUpperCase() === "ENGAGE_NOW"
          ? QUALIFIED_ASSESSMENT.EMERGING_PROSPECTIVE
          : QUALIFIED_ASSESSMENT.EARLY_ENGAGEMENT
        : QUALIFIED_ASSESSMENT.EMERGING_PROSPECTIVE,
      reasons: [...reasons, "DECISION_NOT_EVIDENCED_UNRESOLVED"],
      countsTowardFirst100: false,
      mayPersistAsHistorical: true,
      ruleId: DEALSIGNAL_QUALIFIED_OPPORTUNITY_CLASSIFICATION_V1,
    };
  }
  reasons.push("DECISION_UNRESOLVED");

  const pathway =
    row.credibleCompetitivePathway === true ||
    row.brandPathway?.relevant === true ||
    row.managementPathway?.relevant === true ||
    PATHWAY_RE.test(blob);
  if (!pathway) {
    return {
      isQualified: false,
      assessment: QUALIFIED_ASSESSMENT.EARLY_ENGAGEMENT,
      reasons: [...reasons, "NO_CREDIBLE_COMPETITIVE_PATHWAY"],
      countsTowardFirst100: false,
      mayPersistAsHistorical: true,
      ruleId: DEALSIGNAL_QUALIFIED_OPPORTUNITY_CLASSIFICATION_V1,
    };
  }
  reasons.push("CREDIBLE_COMPETITIVE_PATHWAY");

  // All three gates passed — Qualified research assessment
  // Still: do not auto-set Airtable commercialStatus=VERIFIED without separate contract
  return {
    isQualified: true,
    assessment: QUALIFIED_ASSESSMENT.QUALIFIED,
    reasons: [...reasons, "QUALIFIED_ALL_GATES_PASSED", "RFP_NOT_REQUIRED"],
    countsTowardFirst100: row.countsTowardFirst100 === false ? false : true,
    mayPersistAsHistorical: false,
    ruleId: DEALSIGNAL_QUALIFIED_OPPORTUNITY_CLASSIFICATION_V1,
  };
}

/**
 * Enforce founder rule on a candidate before counting / first-100 / Qualified labels.
 * Mutates a shallow copy — never upgrades historical to Qualified.
 */
export function applyQualifiedClassificationGuard(candidate = {}) {
  const gate = classifyQualifiedOpportunity(candidate);
  const out = { ...candidate, qualifiedClassification: gate };

  if (!gate.isQualified) {
    out.countsTowardFirst100 = false;
    if (
      gate.assessment === QUALIFIED_ASSESSMENT.HISTORICAL_EXECUTED ||
      gate.assessment === QUALIFIED_ASSESSMENT.COMMITTED_CLOSED ||
      gate.assessment === QUALIFIED_ASSESSMENT.RETAINED_RENEWED
    ) {
      if (!out.commercialStatus || /VERIFIED|EMERGING/i.test(String(out.commercialStatus))) {
        if (gate.assessment === QUALIFIED_ASSESSMENT.RETAINED_RENEWED) {
          out.commercialStatus = out.commercialStatus || "COMMITTED";
        } else if (gate.assessment === QUALIFIED_ASSESSMENT.COMMITTED_CLOSED) {
          out.commercialStatus = "COMMITTED";
        }
      }
      if (String(out.assessmentLabel || "").includes("QUALIFIED")) {
        out.assessmentLabel = gate.assessment;
      }
    } else if (String(out.assessmentLabel || "").toUpperCase().includes("QUALIFIED")) {
      out.assessmentLabel = gate.assessment;
    }
  } else if (!out.assessmentLabel) {
    out.assessmentLabel = "QUALIFIED_ACTIONABLE";
  }

  return out;
}

/**
 * Scorecard helper — never inflate Qualified from historical Verified facts.
 */
export function summarizeQualifiedYield(candidates = []) {
  let qualified = 0;
  let emerging = 0;
  let early = 0;
  let historicalOrCommitted = 0;
  let rejectedInflationAttempts = 0;
  const qualifiedIds = [];

  for (const c of candidates) {
    const gate = classifyQualifiedOpportunity(c);
    if (gate.isQualified) {
      qualified += 1;
      qualifiedIds.push(c.id || c.findingId || c.hotelName);
    } else if (
      gate.assessment === QUALIFIED_ASSESSMENT.HISTORICAL_EXECUTED ||
      gate.assessment === QUALIFIED_ASSESSMENT.COMMITTED_CLOSED ||
      gate.assessment === QUALIFIED_ASSESSMENT.RETAINED_RENEWED
    ) {
      historicalOrCommitted += 1;
      if (c.countsTowardFirst100 === true || /QUALIFIED/i.test(String(c.assessmentLabel || ""))) {
        rejectedInflationAttempts += 1;
      }
    } else if (gate.assessment === QUALIFIED_ASSESSMENT.EMERGING_PROSPECTIVE) {
      emerging += 1;
    } else if (gate.assessment === QUALIFIED_ASSESSMENT.EARLY_ENGAGEMENT) {
      early += 1;
    }
  }

  return {
    ruleId: DEALSIGNAL_QUALIFIED_OPPORTUNITY_CLASSIFICATION_V1,
    qualified,
    emerging,
    early,
    historicalOrCommitted,
    rejectedInflationAttempts,
    qualifiedIds,
    note: "Do not inflate Qualified counts to reach commercial critical mass.",
  };
}
