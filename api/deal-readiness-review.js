/**
 * Deal Readiness Review — deterministic scoring from merged deal fields (no LLM required).
 * Uses the same required field list as Deal Setup (REQUIRED_DEAL_SETUP_FIELDS), except the
 * Lease Structure block is omitted when Preferred Deal Structure is not Lease / Flexible/Open.
 * POST /api/ai/deal-readiness-review loads the deal via fetchDealWithMergedLinkedRecords.
 */

import { fetchDealWithMergedLinkedRecords, REQUIRED_DEAL_SETUP_FIELDS, isFieldFilled } from "./my-deals.js";
import {
  DEALS_TABLE,
  DEAL_READINESS_SCORE_AIRTABLE_FIELD,
  DEAL_READINESS_STAGE_AIRTABLE_FIELD,
  DEAL_READINESS_SUMMARY_AIRTABLE_FIELD,
  DEAL_READINESS_LAST_REVIEWED_AIRTABLE_FIELD,
  LEASE_STRUCTURE_FORM_FIELDS,
  isLeaseStructureDealApplicableFromMergedFields,
} from "./schemas/deal-setup-fields.js";
import { READINESS_TAB_ORDER, readinessTabForField } from "./deal-readiness-field-tabs.js";
import {
  inferReadinessContext,
  getFieldRelevanceForContext,
  buildContextScoringProfile,
  computeContextAwareCaps,
  gapSeverityFromRelevance,
  isContextualRequirementMet,
  listContextFoundationalGaps,
} from "./deal-readiness-context.js";

const LEASE_STRUCTURE_FIELD_SET = new Set(LEASE_STRUCTURE_FORM_FIELDS);

/** Draft snapshots are for owner/advisor validation; avoid implying perfection at 100. */
const DRAFT_VALIDATION_OUTPUT_STATUS = "draft_for_validation";
const DRAFT_VALIDATION_MAX_READINESS_SCORE = 98;

/** Map computed stages to values accepted by the Deals table single-select (if options differ). */
function readinessStageForAirtable(stage) {
  const s = String(stage ?? "").trim();
  if (!s) return "";
  if (s === "Ready for External Review") return "Ready";
  return s;
}

/** Same as REQUIRED_DEAL_SETUP_FIELDS except lease-structure keys when the lease tab is not applicable. */
function requiredFieldNamesForReadiness(fields) {
  if (isLeaseStructureDealApplicableFromMergedFields(fields)) return REQUIRED_DEAL_SETUP_FIELDS;
  return REQUIRED_DEAL_SETUP_FIELDS.filter((f) => !LEASE_STRUCTURE_FIELD_SET.has(f));
}
const READINESS_ALTERNATE_KEYS = {
  "Are you open to lesser-known or emerging brands with favorable terms?": [
    "Are you open to considering other brands with favorable terms?",
  ],
  "Has there ever been a franchise, branded management, affiliation or similar agreeement pertaining to the proposed hotel or site?": [
    "Has there ever been a franchise, branded management, affiliation or similar agreement pertaining to the proposed hotel or site?",
  ],
  /** Market–Performance column label in some bases; merged GET uses form key after MP_TABLE_TO_FORM. */
  "Regulatory or Permitting Issues Description": ["Regulatory or Permitting Issues Text"],
};

function getFieldValueForReadiness(fields, canonicalKey) {
  const extras = READINESS_ALTERNATE_KEYS[canonicalKey] || [];
  for (const k of [canonicalKey, ...extras]) {
    if (fields[k] !== undefined && fields[k] !== null) return fields[k];
  }
  return undefined;
}

function isFilledForReadiness(fields, canonicalKey) {
  const v = getFieldValueForReadiness(fields, canonicalKey);
  return isFieldFilled(v);
}

function primaryDemandDriversSelected(fields) {
  const raw = getFieldValueForReadiness(fields, "Primary Demand Drivers");
  if (Array.isArray(raw)) {
    return raw.map((x) => (typeof x === "string" ? x : (x && x.name) || "").trim()).filter(Boolean);
  }
  if (typeof raw === "string" && raw.trim() !== "") {
    return raw
      .split(/\s*,\s*/)
      .map((s) => s.trim())
      .filter(Boolean);
  }
  return [];
}

/**
 * Required fields for readiness (see requiredFieldNamesForReadiness), but respects Deal Setup conditionals:
 * e.g. regulatory description is cleared in the UI when issues = "No"; do not count as missing then.
 */
function isReadinessRequirementMet(fields, fname) {
  return isContextualRequirementMet(fields, fname);
}

/**
 * True only for obvious non-answers in free-text strings.
 * Do not use short length: selects save values like "No" or "Yes"; numbers may serialize as short strings.
 * Arrays (multi-select) and numbers are handled in buildReadinessFromFields and never pass here.
 */
function isWeakText(val) {
  if (val == null) return true;
  if (typeof val !== "string") return false;
  const s = val.trim();
  if (s === "") return true;
  const low = s.toLowerCase();
  if (["tbd", "n/a", "na", "none", "unknown", "—", "-", "pending", "todo", "lorem ipsum"].includes(low)) return true;
  return false;
}

function rowForMissing(field) {
  const tab = readinessTabForField(field);
  return {
    field,
    highlightField: field,
    label: field,
    section: tab,
    relatedTab: tab,
  };
}

function rowForWeak(field) {
  const tab = readinessTabForField(field);
  return {
    field,
    highlightField: field,
    label: field + " (placeholder-style text)",
    section: tab,
    relatedTab: tab,
  };
}

function rowWithSeverity(field, severity) {
  return { ...rowForMissing(field), severity };
}

// ---------------------------------------------------------------------------
// Weighted readiness domains (weights sum to 100)
// ---------------------------------------------------------------------------

const READINESS_DOMAINS = [
  { id: "coreProject", label: "Core Project Definition", weight: 18 },
  { id: "locationMarket", label: "Location & Market Context", weight: 12 },
  { id: "assetProperty", label: "Asset / Property Profile", weight: 12 },
  { id: "ownershipControl", label: "Ownership & Control", weight: 10 },
  { id: "brandOperator", label: "Brand & Operator Starting Point", weight: 12 },
  { id: "dealCapital", label: "Deal / Capital / Agreement Structure", weight: 14 },
  { id: "strategicIntent", label: "Strategic Intent & Owner Priorities", weight: 10 },
  { id: "documentation", label: "Documentation & Supporting Materials", weight: 8 },
  { id: "contactComm", label: "Contact / Communication Readiness", weight: 4 },
];

/** @type {Record<string, string>} */
const FIELD_TO_READINESS_DOMAIN = {
  "Property Name": "coreProject",
  "Project Type": "coreProject",
  "Stage of Development": "coreProject",
  "Full Address": "locationMarket",
  "City & State": "locationMarket",
  "Country": "locationMarket",
  "Hotel Submarket & Location": "locationMarket",
  "Hotel Chain Scale": "locationMarket",
  "Zoned for Hotel Development": "locationMarket",
  "Site/Development Restrictions?": "locationMarket",
  "Total Site Size": "locationMarket",
  "Total Site Size Unit": "locationMarket",
  "Max height Allowed By Zoning": "locationMarket",
  "Max height Unit": "locationMarket",
  "Current Form of Site Control": "locationMarket",
  "Zoning Status": "locationMarket",
  "Parking Ratio": "locationMarket",
  "Access to Transit or Highway": "locationMarket",
  "Primary Demand Drivers": "locationMarket",
  "Primary Demand Drivers Other": "locationMarket",
  "Estimated or Actual RevPAR": "locationMarket",
  "Regulatory or Permitting Issues?": "locationMarket",
  "Regulatory or Permitting Issues Description": "locationMarket",
  "Key Competitors": "locationMarket",
  "Group vs Transient Mix": "locationMarket",
  "Hotel Type": "assetProperty",
  "Hotel Service Model": "assetProperty",
  "Total Number of Rooms/Keys": "assetProperty",
  "Number of Standard Rooms": "assetProperty",
  "Number of Suites": "assetProperty",
  "Building Type": "assetProperty",
  "Number of Stories": "assetProperty",
  "F&B Outlets?": "assetProperty",
  "Meeting Space": "assetProperty",
  "Number of Meeting Rooms": "assetProperty",
  "Condo Residences?": "assetProperty",
  "Hotel Rental Program?": "assetProperty",
  "Parking Amenities?": "assetProperty",
  "Additional Amenities": "assetProperty",
  "Ownership Type": "ownershipControl",
  "Ownership Structure": "ownershipControl",
  "Ownership/Brand History or Track Record": "ownershipControl",
  "Has there ever been a franchise, branded management, affiliation or similar agreeement pertaining to the proposed hotel or site?":
    "brandOperator",
  "Is the hotel currently branded?": "brandOperator",
  "Is the hotel currently managed by a third-party operator?": "brandOperator",
  "Are you open to lesser-known or emerging brands with favorable terms?": "brandOperator",
  "Have you worked with any of your preferred brands/operators before?": "brandOperator",
  "Minimum Operator Experience (years)": "brandOperator",
  "Preferred Third-Party Operators (names)": "brandOperator",
  "Preferred Third-Party Operator Profile": "brandOperator",
  "Services Required From Operator": "brandOperator",
  "Other Operator Criteria or Notes": "brandOperator",
  "Level of Involvement in Day-to-Day Ops": "brandOperator",
  "Total Project Cost Range": "dealCapital",
  "PIP Budget Range (if conversion)": "dealCapital",
  "Equity vs Debt Split": "dealCapital",
  "Preferred Deal Structure": "dealCapital",
  "PIP / CapEx Status": "dealCapital",
  "Lease Type": "dealCapital",
  "Soft vs Hard Brand Preference": "strategicIntent",
  "Preferred Brands (up to 4)": "strategicIntent",
  "IRR/Yield Goals": "strategicIntent",
  "Open to Outside Capital or Partnerships?": "strategicIntent",
  "Plan to Self-Manage or Hire Third Party?": "strategicIntent",
  "Preferred Chain Scales": "strategicIntent",
  "Open to Soft Brand First Then Reflag?": "strategicIntent",
  "Target Guest Segment": "strategicIntent",
  "Brand Flexibility vs Prestige": "strategicIntent",
  "Planned Hold Period": "strategicIntent",
  "Primary Goal for the Hotel": "strategicIntent",
  "Top Priorities for Project": "strategicIntent",
  "Top Concerns for this Project": "strategicIntent",
  "Top 3 Success Metrics": "strategicIntent",
  "Top 3 Deal Breakers": "strategicIntent",
  "Must-haves From Brand or Operator": "strategicIntent",
  "Decision Timeline for Brand/Operator": "strategicIntent",
  "Would you like to filter out brands without key money?": "documentation",
  "Would you like to meet consultants?": "documentation",
  "Legal Support Needed?": "documentation",
  "Financial Model Available?": "documentation",
  "Proposal Deadline": "documentation",
  "Would you like to receive regular updates?": "documentation",
  "Working with Broker/Advisor?": "documentation",
  "Other Projects Nearing Contract Expiration?": "documentation",
  "Who should receive bids for this project?": "contactComm",
  "Main Contact Name": "contactComm",
  "Entity or Company Name": "contactComm",
  "Company HQ Location": "contactComm",
  "Email Address": "contactComm",
};

/** Default domain when a required field has no explicit mapping (by Deal Setup tab). */
const TAB_DEFAULT_READINESS_DOMAIN = {
  "Project Overview": "coreProject",
  "Brand & Op. Status": "brandOperator",
  "Location & Site Details": "locationMarket",
  "Property Specs": "assetProperty",
  "Amenities & Facilities": "assetProperty",
  "Market & Performance": "locationMarket",
  "Deal & Capital Structure": "dealCapital",
  "Lease Structure": "dealCapital",
  "Strategic Intent": "strategicIntent",
  "Operational Expectations": "brandOperator",
  "Challenges & Priorities": "strategicIntent",
  "Support & Comm.": "documentation",
  "Contact Info": "contactComm",
  "Uploads & Attachments": "documentation",
  "Deal Room": "documentation",
};

// ---------------------------------------------------------------------------
// Gap severity classification (blocking / limiting / enhancement)
// ---------------------------------------------------------------------------

const BLOCKING_GAP_FIELDS = new Set([
  "Country",
  "Project Type",
  "Ownership Type",
  "Ownership Structure",
  "Total Number of Rooms/Keys",
  "Preferred Deal Structure",
]);

const LIMITING_GAP_FIELDS = new Set([
  "Stage of Development",
  "Is the hotel currently branded?",
  "Is the hotel currently managed by a third-party operator?",
  "PIP / CapEx Status",
  "Primary Goal for the Hotel",
  "Top Priorities for Project",
  "Key Competitors",
  "City & State",
  "Hotel Submarket & Location",
]);

/** Fields that must be present for stage = Ready (foundational completeness). */
const FOUNDATIONAL_READY_FIELDS = [
  "Project Type",
  "Stage of Development",
  "Country",
  "Total Number of Rooms/Keys",
  "Is the hotel currently branded?",
  "Is the hotel currently managed by a third-party operator?",
  "Preferred Deal Structure",
  "PIP / CapEx Status",
  "Primary Goal for the Hotel",
];

function gapSeverityForField(fieldName) {
  if (BLOCKING_GAP_FIELDS.has(fieldName)) return "blocking";
  if (LIMITING_GAP_FIELDS.has(fieldName)) return "limiting";
  return "enhancement";
}

function weakPenaltyPoints(severity) {
  if (severity === "blocking") return 3;
  if (severity === "limiting") return 2;
  return 1;
}

function readinessDomainForField(fieldName) {
  return (
    FIELD_TO_READINESS_DOMAIN[fieldName] ||
    TAB_DEFAULT_READINESS_DOMAIN[readinessTabForField(fieldName)] ||
    "strategicIntent"
  );
}

function hasMarketCountryAnchor(fields) {
  if (isReadinessRequirementMet(fields, "Country")) return true;
  return (
    isReadinessRequirementMet(fields, "City & State") &&
    isReadinessRequirementMet(fields, "Hotel Submarket & Location")
  );
}

function hasOwnershipControl(fields) {
  return (
    isReadinessRequirementMet(fields, "Ownership Type") ||
    isReadinessRequirementMet(fields, "Ownership Structure")
  );
}

function hasContactDecisionMaker(fields) {
  return (
    isReadinessRequirementMet(fields, "Main Contact Name") &&
    isReadinessRequirementMet(fields, "Email Address")
  );
}

function hasDocumentationPackage(fields) {
  return (
    isReadinessRequirementMet(fields, "Financial Model Available?") ||
    isReadinessRequirementMet(fields, "Working with Broker/Advisor?")
  );
}

function hasOwnerObjectives(fields) {
  return (
    isReadinessRequirementMet(fields, "Primary Goal for the Hotel") ||
    isReadinessRequirementMet(fields, "Top Priorities for Project")
  );
}

/**
 * Foundational score caps — lowest applicable cap wins.
 * Applied when a foundational input is missing or materially weak.
 */
function computeFoundationalCaps(fields) {
  const caps = [];
  if (!hasMarketCountryAnchor(fields)) {
    caps.push({ id: "marketCountry", maxScore: 59, reason: "Missing market / country" });
  }
  if (!isReadinessRequirementMet(fields, "Project Type")) {
    caps.push({ id: "projectType", maxScore: 74, reason: "Missing project type" });
  }
  if (!isReadinessRequirementMet(fields, "Stage of Development")) {
    caps.push({ id: "developmentStage", maxScore: 78, reason: "Missing stage of development" });
  }
  if (!isReadinessRequirementMet(fields, "Total Number of Rooms/Keys")) {
    caps.push({ id: "keyCount", maxScore: 79, reason: "Missing key count" });
  }
  if (!hasOwnershipControl(fields)) {
    caps.push({ id: "ownershipControl", maxScore: 79, reason: "Missing ownership / control status" });
  }
  if (!isReadinessRequirementMet(fields, "Is the hotel currently branded?")) {
    caps.push({ id: "brandStatus", maxScore: 84, reason: "Missing current brand status" });
  }
  if (!isReadinessRequirementMet(fields, "Is the hotel currently managed by a third-party operator?")) {
    caps.push({ id: "operatorStatus", maxScore: 86, reason: "Missing current operator status" });
  }
  if (!isReadinessRequirementMet(fields, "Preferred Deal Structure")) {
    caps.push({ id: "dealStructure", maxScore: 84, reason: "Missing preferred deal structure" });
  }
  if (!isReadinessRequirementMet(fields, "PIP / CapEx Status")) {
    caps.push({ id: "capexPip", maxScore: 86, reason: "Missing capex / PIP status" });
  }
  if (!hasOwnerObjectives(fields)) {
    caps.push({ id: "ownerObjectives", maxScore: 88, reason: "Missing owner objectives / priorities" });
  }
  if (!hasContactDecisionMaker(fields)) {
    caps.push({ id: "contactInfo", maxScore: 90, reason: "Missing contact / decision-maker info" });
  }
  if (!hasDocumentationPackage(fields)) {
    caps.push({ id: "documentation", maxScore: 92, reason: "Missing documentation package signals" });
  }
  return caps;
}

function listFoundationalGaps(fields) {
  const gaps = [];
  for (const fname of FOUNDATIONAL_READY_FIELDS) {
    if (!isReadinessRequirementMet(fields, fname)) {
      gaps.push(rowForMissing(fname));
    }
  }
  if (!hasOwnershipControl(fields)) {
    gaps.push({
      field: "Ownership / Control",
      highlightField: "Ownership Type",
      label: "Ownership / control status",
      section: "Location & Site Details",
      relatedTab: "Location & Site Details",
    });
  }
  return gaps;
}

function computeWeightedCompletionScore(fields, reqNames) {
  const byDomain = Object.fromEntries(
    READINESS_DOMAINS.map((d) => [d.id, { ...d, filled: 0, total: 0 }])
  );
  for (const fname of reqNames) {
    const domId = readinessDomainForField(fname);
    const cell = byDomain[domId];
    if (!cell) continue;
    cell.total += 1;
    if (isReadinessRequirementMet(fields, fname)) cell.filled += 1;
  }
  let score = 0;
  const domainScores = [];
  for (const domain of READINESS_DOMAINS) {
    const cell = byDomain[domain.id];
    const ratio = cell.total > 0 ? cell.filled / cell.total : 1;
    const contribution = domain.weight * ratio;
    score += contribution;
    domainScores.push({
      id: domain.id,
      label: domain.label,
      weight: domain.weight,
      filled: cell.filled,
      total: cell.total,
      percent: cell.total > 0 ? Math.round(100 * ratio) : 100,
      contribution: Math.round(contribution * 10) / 10,
    });
  }
  return { weightedCompletionScore: Math.round(score), domainScores };
}

/**
 * Stage derivation (weighted-v2):
 * - Discovery: score < 50 or any blocking gaps
 * - Shaping: 50–69 (blocking caps stage at Advancing when score ≥ 70)
 * - Advancing: 70–84
 * - Ready for External Review: 85–94, or 95+ with foundational gaps
 * - Ready: 95–100 with no foundational gaps and no blocking gaps
 */
function deriveStage(finalScore, { blockingCount, foundationalGapCount }) {
  const score = Math.round(finalScore);
  if (blockingCount > 0) {
    if (score < 50) return "Discovery";
    if (score < 70) return "Shaping";
    return "Advancing";
  }
  if (score < 50) return "Discovery";
  if (score < 70) return "Shaping";
  if (score < 85) return "Advancing";
  if (score >= 95 && foundationalGapCount === 0) return "Ready";
  if (score >= 85) return "Ready for External Review";
  return "Advancing";
}

function buildTabScores(fields, reqNames) {
  const byTab = {};
  for (const tab of READINESS_TAB_ORDER) {
    byTab[tab] = { filled: 0, total: 0 };
  }
  const names = reqNames || requiredFieldNamesForReadiness(fields);
  for (const fname of names) {
    const tab = readinessTabForField(fname);
    if (!byTab[tab]) byTab[tab] = { filled: 0, total: 0 };
    byTab[tab].total += 1;
    if (isReadinessRequirementMet(fields, fname)) byTab[tab].filled += 1;
  }
  const sectionScores = {};
  const sectionScoresLabeled = [];
  for (const tab of READINESS_TAB_ORDER) {
    const cell = byTab[tab];
    let pct = null;
    if (cell && cell.total > 0) {
      pct = Math.round((100 * cell.filled) / cell.total);
    }
    sectionScores[tab] = pct;
    sectionScoresLabeled.push({ id: tab, label: tab, score: pct });
  }
  return { sectionScores, sectionScoresLabeled };
}

function buildReadinessFromFields(fields) {
  const readinessContext = inferReadinessContext(fields);
  const baseRequiredNames = requiredFieldNamesForReadiness(fields);
  const scoringProfile = buildContextScoringProfile(fields, readinessContext, baseRequiredNames);
  const reqNames = scoringProfile.activeRequiredFields;
  const relevanceByField = scoringProfile.relevanceByField;

  const missingReq = reqNames.filter((f) => !isReadinessRequirementMet(fields, f));
  const weakFields = reqNames.filter((f) => {
    if (!isReadinessRequirementMet(fields, f)) return false;
    const v = getFieldValueForReadiness(fields, f);
    if (typeof v === "number" && Number.isFinite(v)) return false;
    if (Array.isArray(v)) return false;
    if (typeof v === "object" && v !== null) return false;
    if (typeof v !== "string") return false;
    return isWeakText(v);
  });

  const nReq = reqNames.length;
  const baseRequiredCount = baseRequiredNames.length;

  // Weighted domain completion (replaces equal per-field counting for headline score).
  const { weightedCompletionScore, domainScores } = computeWeightedCompletionScore(fields, reqNames);

  // Weak-field penalty by gap severity (enhancement gaps penalize lightly).
  let weakPenalty = 0;
  for (const fname of weakFields) {
    const sev = gapSeverityFromRelevance(fname, fields, readinessContext, relevanceByField);
    if (sev === "none") continue;
    weakPenalty += weakPenaltyPoints(sev === "blocking" ? "blocking" : sev === "limiting" ? "limiting" : "enhancement");
  }

  const contextAppliedCaps = computeContextAwareCaps(fields, readinessContext);
  const appliedScoreCaps = contextAppliedCaps;
  const lowestCap =
    appliedScoreCaps.length > 0 ? Math.min(...appliedScoreCaps.map((c) => c.maxScore)) : 100;

  const preCapScore = Math.max(0, weightedCompletionScore - weakPenalty);
  const computedReadinessScore = Math.max(0, Math.min(100, Math.min(preCapScore, lowestCap)));

  let draftValidationCapApplied = false;
  let dealReadinessScore = computedReadinessScore;
  if (computedReadinessScore >= 100) {
    draftValidationCapApplied = true;
    dealReadinessScore = DRAFT_VALIDATION_MAX_READINESS_SCORE;
  }

  const missingInformation = missingReq.map(rowForMissing);
  const weakInformation = weakFields.map(rowForWeak);

  const blockingIssues = [];
  const limitingIssues = [];
  const enhancementIssues = [];
  for (const fname of missingReq) {
    const severity = gapSeverityFromRelevance(fname, fields, readinessContext, relevanceByField);
    if (severity === "none") continue;
    const row = rowWithSeverity(fname, severity);
    if (severity === "blocking") blockingIssues.push(row);
    else if (severity === "limiting") limitingIssues.push(row);
    else enhancementIssues.push(row);
  }

  const foundationalGaps = listContextFoundationalGaps(
    fields,
    readinessContext,
    reqNames,
    relevanceByField
  ).map((g) => {
    if (g.section) return g;
    const tab = readinessTabForField(g.field);
    return { ...g, section: tab, relatedTab: tab };
  });
  const foundationalGapCount = foundationalGaps.length;

  const gapSeverityCounts = {
    blocking: blockingIssues.length,
    limiting: limitingIssues.length,
    enhancement: enhancementIssues.length,
  };

  const { sectionScores, sectionScoresLabeled } = buildTabScores(fields, reqNames);
  const readinessStage = deriveStage(computedReadinessScore, {
    blockingCount: blockingIssues.length,
    foundationalGapCount,
  });

  if (draftValidationCapApplied) {
    appliedScoreCaps.push({
      id: "draftValidation",
      maxScore: DRAFT_VALIDATION_MAX_READINESS_SCORE,
      reason: "Draft for validation output (owner/advisor validation still required)",
    });
  }

  const priorityActions = [...blockingIssues, ...limitingIssues, ...enhancementIssues]
    .slice(0, 8)
    .map((row) => ({
      label: `Complete “${row.field}”`,
      reason: "Required on Deal Setup for a complete intake and reliable outreach packaging.",
      relatedField: row.field,
      relatedTab: row.relatedTab || readinessTabForField(row.field),
      severity: row.severity === "blocking" ? "high" : row.severity === "limiting" ? "medium" : "low",
    }));

  const capNote =
    appliedScoreCaps.length > 0
      ? ` Score cap(s) applied (${appliedScoreCaps.map((c) => c.reason).join("; ")}).`
      : "";
  const draftNote = draftValidationCapApplied
    ? ` Draft output capped at ${DRAFT_VALIDATION_MAX_READINESS_SCORE}/100 (computed ${computedReadinessScore}/100); stage reflects computed readiness.`
    : "";

  const contextNote =
    scoringProfile.contextExcludedFields.length || scoringProfile.contextTooEarlyFields.length
      ? ` Context-adjusted required count: ${nReq} (from ${baseRequiredCount} base required); ${scoringProfile.contextExcludedFields.length} excluded as not applicable, ${scoringProfile.contextTooEarlyFields.length} deferred as too early.`
      : "";

  const humanReadableSummary =
    `Readiness uses context-aware weighted domain scoring across ${nReq} applicable Deal Setup fields (scoring model weighted-v2). ` +
    `Inferred context: ${readinessContext.contextSummary || "see readinessContext"}.` +
    `Weighted completion: ${weightedCompletionScore}/100` +
    (weakPenalty > 0 ? `, weak-field adjustment: −${weakPenalty}` : "") +
    `, headline score: ${dealReadinessScore}/100, stage: ${readinessStage}.` +
    ` ${missingReq.length} applicable field(s) missing (${gapSeverityCounts.blocking} blocking, ${gapSeverityCounts.limiting} limiting, ${gapSeverityCounts.enhancement} enhancement).` +
    capNote +
    draftNote +
    contextNote +
    ` Tab percentages measure fill rate within each setup section and may differ from the headline score. ` +
    `Re-run after saving Deal Setup (the modal saves score and stage to your deal record when the run finishes).`;

  return {
    success: true,
    dealReadinessScore,
    readinessStage,
    outputStatus: DRAFT_VALIDATION_OUTPUT_STATUS,
    draftValidationCapApplied,
    computedReadinessScore: draftValidationCapApplied ? computedReadinessScore : undefined,
    requiredFieldCount: nReq,
    missingRequiredCount: missingReq.length,
    scoringModelVersion: "weighted-v2",
    contextAwareScoring: true,
    readinessContext,
    contextAdjustedRequiredFieldCount: scoringProfile.contextAdjustedRequiredFieldCount,
    contextExcludedFields: scoringProfile.contextExcludedFields,
    contextConditionalFields: scoringProfile.contextConditionalFields,
    contextTooEarlyFields: scoringProfile.contextTooEarlyFields,
    contextAppliedCaps,
    contextRelevanceNotes: scoringProfile.contextRelevanceNotes,
    weightedCompletionScore,
    appliedScoreCaps,
    foundationalGaps,
    foundationalGapCount,
    gapSeverityCounts,
    domainScores,
    scoreBreakdown: {
      weightedCompletionScore,
      weakPenalty,
      preCapScore,
      computedReadinessScore,
      draftValidationCapApplied,
      draftValidationMaxScore: draftValidationCapApplied ? DRAFT_VALIDATION_MAX_READINESS_SCORE : null,
      lowestCap: appliedScoreCaps.length ? lowestCap : null,
      appliedScoreCaps,
    },
    missingInformation,
    weakInformation,
    blockingIssues,
    limitingIssues,
    enhancementIssues,
    sectionScores,
    sectionScoresLabeled,
    tabScores: sectionScores,
    tabScoresLabeled: sectionScoresLabeled,
    humanReadableSummary,
    workflowRecommendation: {
      label: "Unified review",
      explanation: "Complete missing and weak fields on Deal Setup before external outreach.",
      allowedNextActions: ["Edit deal and highlight gaps", "Save, then re-run readiness"],
    },
    scoreImprovementPlan: {
      targetScoreLabel: "Target: 95+ Ready with zero foundational gaps",
      priorityActions,
    },
    ai: null,
  };
}

export function getDealReadinessMeta(req, res) {
  res.json({
    success: true,
    dealFields: {
      score: DEAL_READINESS_SCORE_AIRTABLE_FIELD,
      stage: DEAL_READINESS_STAGE_AIRTABLE_FIELD,
    },
  });
}

export async function postDealReadinessReview(req, res) {
  try {
    const dealId = req.body && typeof req.body.dealId === "string" ? req.body.dealId.trim() : "";
    if (!dealId || !dealId.startsWith("rec")) {
      return res.status(400).json({ success: false, error: "Valid dealId (Airtable record id) is required" });
    }
    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) {
      return res.status(500).json({ success: false, error: "Airtable credentials not configured" });
    }

    const full = await fetchDealWithMergedLinkedRecords(baseId, apiKey, dealId);
    if (!full) {
      return res.status(404).json({ success: false, error: "Deal not found" });
    }

    const mergedFields = full.deal.fields || {};
    const payload = buildReadinessFromFields(mergedFields);
    res.json({
      ...payload,
      normalized: full.normalized,
      deal: { id: full.deal.id, fields: mergedFields },
      sourceFields: mergedFields,
    });
  } catch (err) {
    console.error("postDealReadinessReview:", err);
    res.status(500).json({ success: false, error: err.message || "Internal Server Error" });
  }
}

/** @internal Exported for read-only audit scripts (scripts/audit-deal-readiness-fields.mjs). */
export {
  buildReadinessFromFields,
  inferReadinessContext,
  getFieldRelevanceForContext,
};

export async function postDealReadinessSave(req, res) {
  try {
    const dealId = req.body && typeof req.body.dealId === "string" ? req.body.dealId.trim() : "";
    const review = req.body && req.body.review && typeof req.body.review === "object" ? req.body.review : null;
    if (!dealId || !dealId.startsWith("rec")) {
      return res.status(400).json({ success: false, error: "Valid dealId is required" });
    }
    if (!review) {
      return res.status(400).json({ success: false, error: "review payload is required" });
    }

    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) {
      return res.status(500).json({ success: false, error: "Airtable credentials not configured" });
    }

    const scNum = Number(review.dealReadinessScore);
    const displayStage = String(review.readinessStage != null ? review.readinessStage : "").trim();
    const airtableStage = readinessStageForAirtable(displayStage);
    const savedAt = new Date().toISOString();
    const airtableFields = {
      [DEAL_READINESS_SCORE_AIRTABLE_FIELD]: Number.isFinite(scNum) ? scNum : 0,
      [DEAL_READINESS_STAGE_AIRTABLE_FIELD]: airtableStage,
    };
    if (DEAL_READINESS_LAST_REVIEWED_AIRTABLE_FIELD) {
      airtableFields[DEAL_READINESS_LAST_REVIEWED_AIRTABLE_FIELD] = savedAt;
    }
    if (DEAL_READINESS_SUMMARY_AIRTABLE_FIELD && review.humanReadableSummary != null) {
      const text = String(review.humanReadableSummary).trim();
      if (text) airtableFields[DEAL_READINESS_SUMMARY_AIRTABLE_FIELD] = text.slice(0, 8000);
    }
    const missCol = process.env.DEAL_READINESS_MISSING_COUNT_FIELD || "";
    if (missCol && Array.isArray(review.missingInformation)) {
      airtableFields[missCol] = review.missingInformation.length;
    }
    const blockCol = process.env.DEAL_READINESS_BLOCKING_COUNT_FIELD || "";
    if (blockCol && Array.isArray(review.blockingIssues)) {
      airtableFields[blockCol] = review.blockingIssues.length;
    }

    const tableEnc = encodeURIComponent(DEALS_TABLE);
    const patchRes = await fetch(`https://api.airtable.com/v0/${baseId}/${tableEnc}/${encodeURIComponent(dealId)}`, {
      method: "PATCH",
      headers: {
        Authorization: "Bearer " + apiKey,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ fields: airtableFields }),
    });
    const body = await patchRes.json().catch(() => ({}));
    if (!patchRes.ok || body.error) {
      const msg = body.error?.message || body.error || `Airtable error (${patchRes.status})`;
      return res.status(patchRes.status >= 400 ? patchRes.status : 502).json({
        success: false,
        error: msg,
        hint:
          "Ensure your Deals table has columns for score and stage (defaults: Deal Readiness Score, Deal Readiness Stage), or set DEAL_READINESS_SCORE_FIELD / DEAL_READINESS_STAGE_FIELD in .env. Optional: Deal Readiness Last Reviewed, or set DEAL_READINESS_LAST_REVIEWED_FIELD=0 to skip that column on save.",
      });
    }

    res.json({
      success: true,
      savedAt,
      dealReadinessScore: airtableFields[DEAL_READINESS_SCORE_AIRTABLE_FIELD],
      readinessStage: displayStage || airtableStage,
      readinessStageAirtable: airtableStage,
      dealReadinessMissingCount: Array.isArray(review.missingInformation) ? review.missingInformation.length : null,
      dealReadinessBlockingCount: Array.isArray(review.blockingIssues) ? review.blockingIssues.length : null,
    });
  } catch (err) {
    console.error("postDealReadinessSave:", err);
    res.status(500).json({ success: false, error: err.message || "Internal Server Error" });
  }
}
