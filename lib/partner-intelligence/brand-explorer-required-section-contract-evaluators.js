/**
 * Brand Explorer required-section contract evaluators v27B.
 * Parent-company-aware portfolio context, standards governance, and demand scenario completion.
 */
import {
  evaluateStandardsDetailApprovalState,
  governanceBodiesMatchApproval,
  requirementRowHasRequiredColumns,
  parseRequirementColumns,
  MIN_REQUIREMENT_ROWS as TRIBUTE_MIN_REQUIREMENT_ROWS,
  INTRO_SLOT,
  LAST_REVIEWED_SLOT,
  SOURCE_CONFIDENCE_SLOT,
  REQUIREMENT_SLOT,
} from "./brand-explorer-tribute-standard-detail-review-approval-writer.js";
import {
  GENERIC_LADDER_FALLBACK_LABELS,
  isMarriottParent,
  isHiltonParent,
  isChoiceParent,
  portfolioContextNarrative,
  nz,
  hasVal,
} from "./brand-explorer-portfolio-ladder-mapping.js";

export const EVALUATOR_VERSION = "27B";
export const MIN_REQUIREMENT_ROWS_GENERAL = 5;
export const DEMAND_SCENARIO_MINIMUM = 3;

const TRIBUTE_RECORD_ID = "recCvV0PuZOi8c3hC";

const PLACEHOLDER_RE =
  /^(tbd|n\/a|na|pending|placeholder|lorem ipsum|coming soon|todo|xxx|\.\.\.)$/i;

const FIT_STRENGTH_RE =
  /strong|moderate|selective|high|low|weak|limited|broad|mixed|variable|tier|fit/i;

const CARRYOVER_PHRASE_CHECKS = [
  { phrase: "curio collection by hilton", match: (brand) => /curio/i.test(nz(brand?.name)) },
  { phrase: "tapestry collection by hilton", match: (brand) => /tapestry/i.test(nz(brand?.name)) },
  { phrase: "kimpton hotels", match: (brand) => /kimpton/i.test(nz(brand?.name)) },
  { phrase: "tribute portfolio", match: (brand) => /tribute/i.test(nz(brand?.name)) },
  { phrase: "autograph collection", match: (brand) => /autograph/i.test(nz(brand?.name)) },
  { phrase: "design hotels", match: (brand) => /design hotels/i.test(nz(brand?.name)) },
  { phrase: "moxy hotels", match: (brand) => /moxy/i.test(nz(brand?.name)) },
  { phrase: "radisson blu", match: (brand) => /radisson blu/i.test(nz(brand?.name)) },
  { phrase: "exactly like nothing else", match: (brand) => /curio/i.test(nz(brand?.name)) },
  { phrase: "bonvoy", match: (brand) => isMarriottParent(brand?.parentCompany) },
  { phrase: "hilton honors", match: (brand) => isHiltonParent(brand?.parentCompany) },
  { phrase: "choice privileges", match: (brand) => isChoiceParent(brand?.parentCompany) },
  { phrase: "ihg one rewards", match: (brand) => /ihg|kimpton/i.test(nz(brand?.name) + nz(brand?.parentCompany)) },
];

const RAW_LEGAL_FRAGMENT_RE =
  /item\s*19|franchise disclosure document|§\s*\d|hereinafter|pursuant to the agreement|exhibit\s+[a-z]\b|whereas\b/i;

const MARRIOTT_VALIDATION_RE =
  /marriott\s+validated|validated\s+by\s+marriott|company-validated\s+standards|company validated standards/i;

const COMPANY_VALIDATION_RE = /company.validated(?!\s*·\s*not)/i;

const GOVERNANCE_REVIEW_RE =
  /founder-reviewed|owner-planning guidance|source-backed|planning checklist|owner planning/i;

const GOVERNANCE_CAVEAT_RE =
  /not company validated|no company sign-off|legal\/transaction confirmation required|confirm with current transaction|confirm current brand standards|confirm every row|confirm standards manual|confirm with brand|before capital commitments|before underwriting|pip scope|agreement vintage|design manual/i;

export function blocksForSlot(brand, slotKey) {
  const blocks = Array.isArray(brand?.brandExplorer?.blocks) ? brand.brandExplorer.blocks : [];
  return blocks.filter((b) => b && nz(b.slotKey) === nz(slotKey));
}

export function mergedSlotBody(brand, slotKey) {
  return blocksForSlot(brand, slotKey)
    .map((b) => nz(b.body))
    .filter(Boolean)
    .join("\n\n");
}

export function resolveParentPortfolioFamily(parentCompany) {
  if (isMarriottParent(parentCompany)) return "marriott_bonvoy";
  if (isHiltonParent(parentCompany)) return "hilton";
  if (isChoiceParent(parentCompany)) return "choice";
  if (/ihg|intercontinental/i.test(nz(parentCompany))) return "ihg";
  if (/radisson/i.test(nz(parentCompany))) return "radisson_choice";
  return "generic";
}

function isPlaceholderText(text) {
  const t = nz(text);
  if (!t) return true;
  if (PLACEHOLDER_RE.test(t)) return true;
  if (t.length < 2) return true;
  return false;
}

function narrativeHasSubstance(text) {
  const t = nz(text);
  return t.length >= 40 || t.split(/\s+/).filter(Boolean).length >= 8;
}

function onlyGenericLadderFallback(text) {
  const t = nz(text).toLowerCase();
  const hits = GENERIC_LADDER_FALLBACK_LABELS.filter((label) => t.includes(label.toLowerCase()));
  return hits.length >= 2 && t.split(/\s+/).length < 25;
}

function parentEcosystemSignal(brand, narrative) {
  const parent = nz(brand?.parentCompany);
  const name = nz(brand?.name).toLowerCase();
  const text = nz(narrative).toLowerCase();
  const family = resolveParentPortfolioFamily(parent);

  if (family === "marriott_bonvoy") {
    return /marriott|bonvoy|collection|autograph|tribute|design hotels|moxy|soft.?brand|lifestyle/i.test(text);
  }
  if (family === "hilton") {
    return (
      /hilton|curio|tapestry|waldorf|canopy|signia|tempo|lifestyle|collection|soft.?brand/i.test(text) ||
      text.includes(name)
    );
  }
  if (family === "choice" || family === "radisson_choice") {
    return /choice|ascend|radisson|comfort|quality|clarion|cambria|soft.?brand|collection/i.test(text);
  }
  if (family === "ihg") {
    return /ihg|kimpton|intercontinental|boutique|collection|lifestyle/i.test(text);
  }
  return /parent|portfolio|collection|soft.?brand|lifestyle|independent|ecosystem|sibling|competitive/i.test(
    text
  );
}

/** Legacy v25C-1 portfolio context gate (Marriott/Tribute-calibrated). */
export function legacyPortfolioContextReady(contextBody) {
  const body = nz(contextBody);
  return hasVal(body) && !/lower-scale|mid-scale|upscale|upper-scale/i.test(body);
}

/** v27B parent-company portfolio context readiness. */
export function evaluatePortfolioContextReadiness(brand, contextRows = null, mixRows = null) {
  const rows = contextRows || blocksForSlot(brand, "overview.portfolio_context");
  const mix = mixRows || blocksForSlot(brand, "footprint.portfolio_mix");
  const narrative = portfolioContextNarrative(brand) || nz(rows[0]?.body);
  const legacyReady = legacyPortfolioContextReady(narrative);
  const family = resolveParentPortfolioFamily(brand?.parentCompany);

  const blockers = [];
  if (!rows.length) blockers.push("missing_portfolio_context_row");
  if (!hasVal(narrative)) blockers.push("portfolio_context_body_empty");
  if (!narrativeHasSubstance(narrative)) blockers.push("portfolio_context_too_thin");
  if (onlyGenericLadderFallback(narrative)) blockers.push("generic_ladder_fallback_only");
  if (hasVal(narrative) && !parentEcosystemSignal(brand, narrative) && mix.length < 1) {
    blockers.push("missing_parent_ecosystem_explanation");
  }

  const ready = blockers.length === 0;
  return {
    ready,
    legacyReady,
    falseNegativeVsLegacy: ready && !legacyReady,
    family,
    narrativePreview: narrative.slice(0, 160),
    contextRowCount: rows.length,
    mixRowCount: mix.length,
    blockers,
  };
}

function detectWrongBrandCarryover(brand, text) {
  const lower = nz(text).toLowerCase();
  return CARRYOVER_PHRASE_CHECKS.some(
    ({ phrase, match }) => lower.includes(phrase) && !match(brand)
  );
}

/** Brand-aware standards copy safety — blocks wrong-brand carryover and raw legal fragments. */
export function scanStandardsCopySafetyForBrand(brand, text) {
  const combined = nz(text);
  const issues = [];

  if (detectWrongBrandCarryover(brand, combined)) issues.push("reference_brand_copy");

  if (RAW_LEGAL_FRAGMENT_RE.test(combined)) issues.push("raw_fdd_legal_fragment");
  if (MARRIOTT_VALIDATION_RE.test(combined)) issues.push("marriott_validation_language");
  if (COMPANY_VALIDATION_RE.test(combined) && !/not company validated/i.test(combined)) {
    issues.push("company_validation_language");
  }
  return issues;
}

export function governanceLanguageAcceptable(...texts) {
  const combined = texts.filter(hasVal).join("\n");
  if (!hasVal(combined)) return false;
  const hasReview = GOVERNANCE_REVIEW_RE.test(combined);
  const hasCaveat = GOVERNANCE_CAVEAT_RE.test(combined);
  const avoidsFalseValidation =
    !COMPANY_VALIDATION_RE.test(combined) || /not company validated|no company sign-off/i.test(combined);
  return hasReview && hasCaveat && avoidsFalseValidation;
}

function brandRecordId(brand) {
  return nz(brand?.recordId || brand?.id);
}

function minRequirementRowsForBrand(brand) {
  return brandRecordId(brand) === TRIBUTE_RECORD_ID ? TRIBUTE_MIN_REQUIREMENT_ROWS : MIN_REQUIREMENT_ROWS_GENERAL;
}

/** Legacy demand row completion (Tribute case-summary shaped). */
export function legacyDemandScenarioRowComplete(row) {
  const title = nz(row?.title);
  const body = nz(row?.body);
  const implication = nz(row?.caseSummaryOwnerObjective) || nz(row?.caseSummaryInterpretation);
  return [title, body, implication].every(hasVal);
}

/** v27B brand-neutral demand scenario row completion. */
export function evaluateDemandScenarioRowComplete(row) {
  const title = nz(row?.title);
  const body = nz(row?.body);
  const ownerObjective =
    nz(row?.caseSummaryOwnerObjective) || nz(row?.caseSummaryOverview) || nz(row?.caseSummaryBrandRelevance);
  const implication = nz(row?.caseSummaryInterpretation) || nz(row?.caseSummaryBrandRelevance);
  const missing = [];

  if (isPlaceholderText(title)) missing.push("title_or_scenario_label");
  if (isPlaceholderText(body) && !hasVal(implication)) missing.push("fit_signal_or_body");

  const hasLabel = hasVal(title) && !isPlaceholderText(title);
  const hasContext =
    (hasVal(title) && title.split(/\s+/).length >= 2) || (hasVal(ownerObjective) && !isPlaceholderText(ownerObjective));
  const hasFit =
    (hasVal(body) && !isPlaceholderText(body) && (body.length >= 3 || FIT_STRENGTH_RE.test(body))) ||
    FIT_STRENGTH_RE.test(implication);
  const hasImplication =
    (hasVal(implication) && !isPlaceholderText(implication)) ||
    (hasVal(body) && body.split(/\s+/).length >= 8) ||
    (hasVal(ownerObjective) && ownerObjective.split(/\s+/).length >= 6);

  const chipStyleComplete = hasLabel && hasContext && hasFit && hasVal(body);
  const narrativeComplete = hasLabel && hasContext && hasFit && hasImplication;

  const complete = chipStyleComplete || narrativeComplete;
  if (!complete) {
    if (!hasLabel) missing.push("title_or_scenario_label");
    if (!hasContext) missing.push("owner_objective_or_asset_context");
    if (!hasFit) missing.push("fit_signal_or_fit_strength");
    if (!hasImplication && !chipStyleComplete) missing.push("implication_or_reason_body");
  }

  return {
    complete,
    legacyComplete: legacyDemandScenarioRowComplete(row),
    falseNegativeVsLegacy: complete && !legacyDemandScenarioRowComplete(row),
    missingFields: [...new Set(missing)],
    recordId: row?.recordId || null,
    titlePreview: title.slice(0, 80),
  };
}

export function evaluateDemandScenarioReadiness(brand, demandRows = null) {
  const rows = demandRows || blocksForSlot(brand, "commercial.demand");
  const evaluated = rows.map(evaluateDemandScenarioRowComplete);
  const complete = evaluated.filter((r) => r.complete);
  const legacyComplete = evaluated.filter((r) => r.legacyComplete);
  const ready = complete.length >= DEMAND_SCENARIO_MINIMUM;

  return {
    ready,
    legacyReady: legacyComplete.length >= DEMAND_SCENARIO_MINIMUM,
    falseNegativeVsLegacy: ready && legacyComplete.length < DEMAND_SCENARIO_MINIMUM,
    rowCount: rows.length,
    completeCount: complete.length,
    legacyCompleteCount: legacyComplete.length,
    requiredMinimum: DEMAND_SCENARIO_MINIMUM,
    incompleteRows: evaluated.filter((r) => !r.complete).slice(0, 6),
    blockers: ready ? [] : [`incomplete_demand_rows:${complete.length}<${DEMAND_SCENARIO_MINIMUM}`],
  };
}

/** v27B generalized standards detail approval. Preserves Tribute v25C-5C strict path. */
export function evaluateStandardsDetailReadinessGeneralized(brand, requirementRows = null) {
  const tributeStrict =
    brandRecordId(brand) === TRIBUTE_RECORD_ID
      ? evaluateStandardsDetailApprovalState(brand, requirementRows)
      : null;

  if (tributeStrict) {
    return {
      ...tributeStrict,
      evaluator: "tribute_strict_v25C_5C",
      legacyReady: tributeStrict.ready,
      falseNegativeVsLegacy: false,
      minRequirementRows: TRIBUTE_MIN_REQUIREMENT_ROWS,
    };
  }

  const rows = requirementRows || blocksForSlot(brand, REQUIREMENT_SLOT);
  const introBody = mergedSlotBody(brand, INTRO_SLOT);
  const lastReviewedBody = mergedSlotBody(brand, LAST_REVIEWED_SLOT);
  const sourceConfidenceBody = mergedSlotBody(brand, SOURCE_CONFIDENCE_SLOT);
  const minRows = minRequirementRowsForBrand(brand);
  const completeRows = rows.filter(requirementRowHasRequiredColumns);

  const governanceTexts = [introBody, lastReviewedBody, sourceConfidenceBody].filter(hasVal);
  const tributeSlotGovernance = governanceBodiesMatchApproval(lastReviewedBody, sourceConfidenceBody);
  const flexibleGovernance = governanceLanguageAcceptable(...governanceTexts);

  const copyTexts = [introBody, lastReviewedBody, sourceConfidenceBody, ...rows.map((r) => `${r.title}\n${r.body}`)];
  const copySafetyIssues = [...new Set(copyTexts.flatMap((t) => scanStandardsCopySafetyForBrand(brand, t)))];

  const legacyEval = evaluateStandardsDetailApprovalState(brand, rows);

  const blockers = [];
  if (rows.length < minRows) blockers.push(`insufficient_requirement_rows:${rows.length}<${minRows}`);
  if (completeRows.length < minRows) {
    blockers.push(`incomplete_requirement_columns:${completeRows.length}<${minRows}`);
  }
  if (!hasVal(introBody)) blockers.push("missing_standards_intro");
  if (!tributeSlotGovernance && !flexibleGovernance) blockers.push("governance_review_state_incomplete");
  if (copySafetyIssues.length) blockers.push(`copy_safety:${copySafetyIssues.join(",")}`);

  const ready = blockers.length === 0;

  return {
    requirementRowCount: rows.length,
    completeRequirementRowCount: completeRows.length,
    introPresent: hasVal(introBody),
    governanceLastReviewedPresent: hasVal(lastReviewedBody),
    governanceSourceConfidencePresent: hasVal(sourceConfidenceBody),
    founderReviewGovernancePresent: tributeSlotGovernance || flexibleGovernance,
    copySafetyPassed: copySafetyIssues.length === 0,
    copySafetyIssues,
    ready,
    legacyReady: legacyEval.ready,
    falseNegativeVsLegacy: ready && !legacyEval.ready,
    blockers,
    evaluator: "generalized_v27B",
    minRequirementRows: minRows,
  };
}

export {
  requirementRowHasRequiredColumns,
  parseRequirementColumns,
  legacyDemandScenarioRowComplete as demandIsCompleteLegacy,
};
