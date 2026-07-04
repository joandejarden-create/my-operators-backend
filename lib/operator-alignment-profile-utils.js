/**
 * Operator Alignment Snapshot — profile-level archetype evaluation (Phase 1).
 * No specific-operator scoring; no Airtable writes.
 *
 * @see fixtures/operator-profile-archetypes.json
 * @see docs/operator-alignment-field-matrix.md
 */

import { readFileSync } from "fs";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import {
  DEALS_FIELDS,
  LOCATION_FIELDS,
  SI_FIELDS,
  strVal,
  listVal,
  isOperatorInScopeFromFields,
  inferPrimaryMarketRegionFromCountry,
  CALA_COUNTRIES,
} from "./operator-capability-inputs.js";
import {
  normalizeProjectTypeLabel,
  resolveProjectTypeKind,
  isConversionDealProjectType,
} from "./project-type.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ARCHETYPES_PATH = join(__dirname, "..", "fixtures", "operator-profile-archetypes.json");

export const OAS_FEATURE_NAME = "Operator Alignment Snapshot";
export const OAS_MODE_PROFILE = "profile";

export const ALIGNMENT_BANDS = [
  "Strong Alignment Signals",
  "Moderate Alignment Signals",
  "Conditional Alignment Signals",
  "Limited Alignment Signals",
  "Insufficient Data",
];

export const OPERATOR_REVIEW_SIGNAL_LEVELS = ["High", "Medium", "Low", "Insufficient Data"];

const NOT_PROVIDED = "Not provided";

/** @type {Record<string, string>} */
const METHODOLOGY_NOTE =
  "Operator profile alignment in this snapshot is derived from current deal intake fields and a fixed set of operator profile categories. " +
  "It organizes owner/advisor review and does not evaluate named operators, rank management companies, or indicate approval or commercial terms.";

let _archetypesCache = null;

/**
 * @returns {{ version: string, archetypes: object[] }}
 */
export function loadOperatorProfileArchetypes() {
  if (_archetypesCache) return _archetypesCache;
  const raw = readFileSync(ARCHETYPES_PATH, "utf8");
  _archetypesCache = JSON.parse(raw);
  return _archetypesCache;
}

/**
 * Merge deal + linked table fields into one lookup object (form/Airtable names).
 * @param {object} dealFields
 * @param {object|null} locationData
 * @param {object|null} siData
 * @param {object|null} mpData
 */
export function mergeDealFieldsForAlignment(dealFields, locationData, siData, mpData) {
  return {
    ...(dealFields || {}),
    ...(locationData || {}),
    ...(siData || {}),
    ...(mpData || {}),
  };
}

function loc(merged, airtableKey, normalizedKey) {
  return strVal(merged[airtableKey] ?? merged[normalizedKey]);
}

function parseRoomCount(merged) {
  const keys = [
    "Total Number of Rooms/Keys",
    "Total Number of Rooms",
    "Number of Rooms",
    "Total Rooms",
    "Keys",
  ];
  for (const k of keys) {
    const v = strVal(merged[k]);
    if (!v) continue;
    const n = Number(String(v).replace(/,/g, ""));
    if (Number.isFinite(n) && n > 0) return n;
  }
  return null;
}

function isTruthyYes(val) {
  const s = strVal(val).toLowerCase();
  return s === "yes" || s === "true" || s === "1";
}

function importanceAtLeast(merged, fieldName, min = 3) {
  const v = merged[fieldName];
  if (v == null || v === "") return false;
  const n = Number(v);
  if (Number.isFinite(n)) return n >= min;
  const s = strVal(v).toLowerCase();
  return /high|very|critical|essential/i.test(s);
}

/**
 * @param {Record<string, unknown>} merged
 */
export function buildDealContextFromMerged(merged) {
  const m = merged || {};
  const country = loc(m, LOCATION_FIELDS.country, "country") || strVal(m.Country);
  const city = strVal(m["City & State"] || m.City || m.city);
  const region =
    loc(m, LOCATION_FIELDS.primaryMarketRegion, "primaryMarketRegion") ||
    inferPrimaryMarketRegionFromCountry(country) ||
    "";
  const projectType = normalizeProjectTypeLabel(
    m[DEALS_FIELDS.projectType] || m["Project Type"]
  );
  const kind = resolveProjectTypeKind(projectType);
  const currentOp = strVal(m[DEALS_FIELDS.currentOperatingModel] || m["Current Operating Model"]);
  const futureOp = strVal(m[SI_FIELDS.preferredFutureOperatingModel] || m["Preferred Future Operating Model"]);
  const branded =
    strVal(m["Is the hotel currently branded?"]) ||
    strVal(m["Is the hotel currently branded"]);
  const brandStatus = branded
    ? branded
    : isTruthyYes(m["Are you open to considering other brands with favorable terms?"])
      ? "Brand path under review"
      : NOT_PROVIDED;

  return {
    dealName:
      strVal(m["Property Name"]) ||
      strVal(m["Project Name"]) ||
      strVal(m.Name) ||
      NOT_PROVIDED,
    country: country || NOT_PROVIDED,
    cityOrMarket: city || NOT_PROVIDED,
    primaryMarketRegion: region || NOT_PROVIDED,
    roomCount: parseRoomCount(m),
    projectType: projectType || NOT_PROVIDED,
    projectTypeKind: kind,
    assetStatus: strVal(m["Stage of Development"] || m["Deal Status"]) || NOT_PROVIDED,
    brandStatus,
    currentOperatingModel: currentOp || NOT_PROVIDED,
    desiredOperatingModel: futureOp || NOT_PROVIDED,
    serviceModel: strVal(m["Hotel Service Model"]) || NOT_PROVIDED,
    chainScale: strVal(m["Hotel Chain Scale"]) || NOT_PROVIDED,
    indicators: {
      conversionOrReflag: kind === "conversion_reflag" || isConversionDealProjectType(projectType),
      repositioning: kind === "renovation_repositioning",
      newBuild: kind === "new_build",
      preopeningOrReopening: /pre-opening|reopening|rebranding in place|construction/i.test(
        strVal(m[DEALS_FIELDS.openingTransitionPhase] || m["Opening / Transition Phase"])
      ),
      operatorInScope: isOperatorInScopeFromFields(m),
      operatorStrategyStatus:
        strVal(m[SI_FIELDS.operatorStrategyStatus] || m["Operator Strategy Status"]) || NOT_PROVIDED,
      preferredOperatorProfiles: listVal(m["Preferred Third-Party Operator Profile"]),
      fbOutlets: strVal(m["F&B Outlets?"]) || NOT_PROVIDED,
    },
  };
}

/**
 * @typedef {object} DealSignalContext
 * @property {Record<string, unknown>} merged
 * @property {ReturnType<typeof buildDealContextFromMerged>} dealContext
 */

/**
 * @param {Record<string, unknown>} merged
 * @returns {DealSignalContext}
 */
export function buildDealSignalContext(merged) {
  const dealContext = buildDealContextFromMerged(merged);
  return { merged, dealContext };
}

/**
 * @param {string} signalKey
 * @param {DealSignalContext} ctx
 * @returns {boolean}
 */
export function evaluateDealSignal(signalKey, ctx) {
  const { merged, dealContext: d } = ctx;
  const m = merged;
  const profiles = listVal(m["Preferred Third-Party Operator Profile"]);
  const services = listVal(m["Services Required From Operator"]);
  const priorities = listVal(m[SI_FIELDS.operatorCapabilityPriorities] || m["Operator Capability Priorities"]);
  const futureOp = strVal(m[SI_FIELDS.preferredFutureOperatingModel] || m["Preferred Future Operating Model"]);
  const strategy = strVal(m[SI_FIELDS.operatorStrategyStatus] || m["Operator Strategy Status"]);
  const country = strVal(d.country);
  const countryLower = country.toLowerCase();
  const chain = strVal(d.chainScale).toLowerCase();
  const hotelType = strVal(m["Hotel Type"]).toLowerCase();
  const scale = chain;

  switch (signalKey) {
    case "country_or_market_provided":
      return (
        (country && country !== NOT_PROVIDED) ||
        (d.primaryMarketRegion && d.primaryMarketRegion !== NOT_PROVIDED)
      );
    case "cala_market":
      return (
        d.primaryMarketRegion === "CALA" ||
        CALA_COUNTRIES.has(countryLower) ||
        /cala|caribbean|mexico|latin america/i.test(strVal(d.primaryMarketRegion))
      );
    case "operator_in_scope":
      return isOperatorInScopeFromFields(m);
    case "project_type_known":
      return d.projectType !== NOT_PROVIDED && d.projectTypeKind !== "unknown";
    case "conversion_or_reflag":
      return d.indicators.conversionOrReflag;
    case "repositioning_project":
      return d.indicators.repositioning;
    case "new_build_project":
      return d.indicators.newBuild;
    case "preopening_or_reopening_phase":
      return d.indicators.preopeningOrReopening;
    case "preferred_profile_regional":
      return profiles.some((p) => /^regional$/i.test(p));
    case "preferred_profile_international":
      return profiles.some((p) => /^international$/i.test(p));
    case "preferred_profile_independent_boutique":
      return profiles.some((p) => /independent|boutique/i.test(p));
    case "preferred_profile_no_preference":
      return profiles.some((p) => /no preference/i.test(p)) || profiles.length === 0;
    case "preferred_future_owner_operated":
      return /^owner-operated/i.test(futureOp);
    case "preferred_future_owner_operated_only":
      return (
        /^owner-operated$/i.test(futureOp) ||
        /franchise\/license only/i.test(futureOp)
      );
    case "preferred_future_owner_operated_path":
      return (
        /^owner-operated/i.test(futureOp) ||
        /franchise\/license only/i.test(futureOp) ||
        (profiles.length === 0 && /self-manage|owner-operated/i.test(strVal(m[SI_FIELDS.planSelfManage])))
      );
    case "preferred_future_brand_managed":
      return /brand-managed|brand \+ third/i.test(futureOp);
    case "preferred_future_brand_managed_only":
      return /^brand-managed$/i.test(futureOp);
    case "preferred_future_third_party":
      return /third.party|third-party|brand \+ third/i.test(futureOp);
    case "preferred_future_third_party_only":
      return /^third-party management only$/i.test(futureOp);
    case "services_partial_not_full_management":
      return (
        services.length > 0 &&
        !services.some((s) => /^full management$/i.test(s)) &&
        (services.some((s) => /revenue|accounting|sales|marketing|hr/i.test(s)) ||
          priorities.length > 0)
      );
    case "services_require_partial_management":
      return services.length > 0 && !services.every((s) => /^full management$/i.test(s));
    case "operator_capability_full_management_only":
      return (
        priorities.length > 0 &&
        priorities.every((p) => /full hotel management/i.test(p)) &&
        services.some((s) => /^full management$/i.test(s))
      );
    case "commercial_support_priority":
      return (
        importanceAtLeast(m, "Revenue / Yield Management Importance") ||
        importanceAtLeast(m, "Marketing & Distribution Importance") ||
        priorities.some((p) => /revenue|sales|accounting|commercial/i.test(p))
      );
    case "revenue_management_priority":
      return (
        importanceAtLeast(m, "Revenue / Yield Management Importance") ||
        priorities.some((p) => /revenue management/i.test(p)) ||
        services.some((s) => /revenue management/i.test(s))
      );
    case "full_service_or_upper_scale":
      return (
        /full[- ]?service|upper upscale|luxury|upscale|resort/i.test(scale) ||
        /resort|full[- ]?service/i.test(strVal(m["Hotel Service Model"]).toLowerCase()) ||
        /resort|full[- ]?service/i.test(hotelType)
      );
    case "select_service_only_scale":
      return (
        /select[- ]?service|extended stay|midscale|economy|upper midscale/i.test(scale) &&
        !/resort|luxury|upper upscale/i.test(scale)
      );
    case "lifestyle_positioning":
      return (
        /lifestyle|boutique|collection|independent/i.test(scale) ||
        /lifestyle|boutique/i.test(hotelType) ||
        /lifestyle|boutique|conversion/i.test(d.projectType.toLowerCase())
      );
    case "fb_outlets_present":
      return isTruthyYes(m["F&B Outlets?"]) || Number(m["Number of F&B Outlets"]) > 0;
    case "brand_path_active":
      return (
        isTruthyYes(m["Is the hotel currently branded?"]) ||
        isTruthyYes(m["Are you open to considering other brands with favorable terms?"]) ||
        listVal(m["Preferred Brands (up to 4)"] || m["Preferred Brands"]).length > 0
      );
    case "brand_affiliation_under_review":
      return (
        isTruthyYes(m["Are you open to considering other brands with favorable terms?"]) ||
        d.indicators.conversionOrReflag
      );
    case "operator_strategy_exploring_or_ready":
      return /exploring|shortlist|ready for structured|already in discussion/i.test(strategy);
    case "operator_strategy_closed":
      return /not seeking operator input/i.test(strategy);
    case "chain_scale_upper":
      return /upscale|upper upscale|luxury/i.test(scale);
    default:
      return false;
  }
}

/**
 * @param {string[]} signalKeys
 * @param {DealSignalContext} ctx
 */
function evaluateSignalList(signalKeys, ctx) {
  const matched = [];
  const missing = [];
  for (const key of signalKeys || []) {
    if (evaluateDealSignal(key, ctx)) matched.push(key);
    else missing.push(key);
  }
  return { matched, missing };
}

/**
 * Map positive match ratio to alignment band (no numeric score in Phase 1).
 * @param {object} archetype
 * @param {{ matchedRequired: string[], missingRequired: string[], matchedPositive: string[], matchedNegative: string[] }} eval
 */
export function resolveAlignmentBand(archetype, evalResult) {
  const { matchedRequired, missingRequired, matchedPositive, matchedNegative } = evalResult;
  if (missingRequired.length > 0) {
    if (matchedRequired.length === 0) return "Insufficient Data";
    return "Conditional Alignment Signals";
  }
  const pos = matchedPositive.length;
  const neg = matchedNegative.length;
  if (neg >= 2 && pos <= 1) return "Limited Alignment Signals";
  if (pos >= 4 && neg === 0) return "Strong Alignment Signals";
  if (pos >= 2 && neg <= 1) return "Moderate Alignment Signals";
  if (pos >= 1 || matchedRequired.length > 0) return "Conditional Alignment Signals";
  return archetype.defaultAlignmentBand || "Limited Alignment Signals";
}

/**
 * @param {object} archetype
 * @param {DealSignalContext} ctx
 */
export function evaluateOperatorProfileArchetype(archetype, ctx) {
  const req = evaluateSignalList(archetype.requiredDealSignals, ctx);
  const pos = evaluateSignalList(archetype.positiveDealSignals, ctx);
  const neg = evaluateSignalList(archetype.negativeOrConditionalDealSignals, ctx);

  const legacyProfiles = listVal(
    ctx.merged["Preferred Third-Party Operator Profile"]
  );
  const legacyMatch = (archetype.legacyProfileOptions || []).some((opt) =>
    legacyProfiles.some((p) => p.toLowerCase() === String(opt).toLowerCase())
  );

  const evalResult = {
    matchedRequired: req.matched,
    missingRequired: req.missing,
    matchedPositive: pos.matched,
    matchedNegative: neg.matched,
    legacyMatch,
  };

  let alignmentBand = resolveAlignmentBand(archetype, evalResult);
  if (legacyMatch && alignmentBand === "Limited Alignment Signals") {
    alignmentBand = "Conditional Alignment Signals";
  }
  if (legacyMatch && pos.matched.length >= 2 && req.missing.length === 0) {
    alignmentBand = "Moderate Alignment Signals";
  }

  const matchedDealSignals = [...req.matched, ...pos.matched];
  if (legacyMatch) matchedDealSignals.push("legacy_profile_option_match");

  const explanationParts = [];
  if (req.missing.length > 0) {
    explanationParts.push(
      "Some required deal signals are not present: " + req.missing.join(", ") + "."
    );
  }
  if (pos.matched.length > 0) {
    explanationParts.push(
      "Matched deal signals include: " + pos.matched.join(", ") + "."
    );
  }
  if (neg.matched.length > 0) {
    explanationParts.push(
      "Conditional or offsetting signals: " + neg.matched.join(", ") + "."
    );
  }
  if (legacyMatch) {
    explanationParts.push("Owner intake includes a legacy operator profile option that maps to this category.");
  }

  return {
    profileKey: archetype.key,
    displayLabel: archetype.displayLabel,
    shortLabel: archetype.shortLabel,
    alignmentBand,
    alignmentScoreOptional: null,
    alignmentSignals: [...(archetype.alignmentSignals || [])],
    reviewConsiderations: [...(archetype.reviewConsiderations || [])],
    questionsToClarify: [...(archetype.questionsToClarify || [])],
    dataGaps: [...(archetype.dataGaps || [])],
    suggestedWorkflowActions: [...(archetype.suggestedWorkflowActions || [])],
    explanation: explanationParts.join(" ") || archetype.description,
    matchedDealSignals,
    missingDealSignals: req.missing,
    sortPriority: archetype.sortPriority ?? 100,
    bestUseCase: archetype.bestUseCase,
    description: archetype.description,
  };
}

/**
 * @param {Record<string, unknown>} merged
 * @returns {{ level: string, rationale: string, matchedSignals: string[] }}
 */
export function computeOperatorReviewSignal(merged) {
  const ctx = buildDealSignalContext(merged);
  const relevanceSignals = [
    "conversion_or_reflag",
    "repositioning_project",
    "new_build_project",
    "preopening_or_reopening_phase",
    "brand_path_active",
    "brand_affiliation_under_review",
    "preferred_future_third_party",
    "preferred_future_brand_managed",
    "operator_in_scope",
    "full_service_or_upper_scale",
    "lifestyle_positioning",
    "fb_outlets_present",
    "commercial_support_priority",
    "cala_market",
    "operator_strategy_exploring_or_ready",
  ];

  const matched = relevanceSignals.filter((k) => evaluateDealSignal(k, ctx));
  const d = ctx.dealContext;

  const missingCore =
    d.projectType === NOT_PROVIDED &&
    d.country === NOT_PROVIDED &&
    d.desiredOperatingModel === NOT_PROVIDED;

  if (missingCore) {
    return {
      level: "Insufficient Data",
      rationale:
        "Core deal fields (project type, market/country, desired operating model) are incomplete, so operator review relevance cannot be classified reliably.",
      matchedSignals: matched,
    };
  }

  if (/not seeking operator input/i.test(d.indicators.operatorStrategyStatus)) {
    return {
      level: "Low",
      rationale:
        "Operator strategy status indicates operator input is not currently sought; operator profile review may still be informational.",
      matchedSignals: matched,
    };
  }

  if (
    /^owner-operated$/i.test(d.desiredOperatingModel) &&
    !evaluateDealSignal("services_partial_not_full_management", ctx) &&
    !evaluateDealSignal("operator_in_scope", ctx)
  ) {
    return {
      level: "Low",
      rationale:
        "The deal points to owner-operated execution without third-party management scope or partial commercial services.",
      matchedSignals: matched,
    };
  }

  if (matched.length >= 5) {
    return {
      level: "High",
      rationale:
        "Multiple deal characteristics suggest structured operator profile review may be relevant (transition complexity, brand/operator path, or commercial scope).",
      matchedSignals: matched,
    };
  }

  if (matched.length >= 2) {
    return {
      level: "Medium",
      rationale: "Some deal characteristics suggest operator profile review may be relevant for clarification.",
      matchedSignals: matched,
    };
  }

  return {
    level: "Low",
    rationale: "Limited deal characteristics indicate operator profile review is optional at this stage.",
    matchedSignals: matched,
  };
}

/**
 * @param {Record<string, unknown>} merged
 * @param {object[]} [archetypes]
 */
export function evaluateOperatorProfilesForReview(merged, archetypes = null) {
  const list = archetypes || loadOperatorProfileArchetypes().archetypes || [];
  const ctx = buildDealSignalContext(merged);
  const results = list.map((a) => evaluateOperatorProfileArchetype(a, ctx));
  results.sort((a, b) => (a.sortPriority ?? 100) - (b.sortPriority ?? 100));
  return results;
}

/**
 * Aggregate top-level data gaps and workflow actions.
 * @param {ReturnType<typeof evaluateOperatorProfilesForReview>} profiles
 * @param {ReturnType<typeof computeOperatorReviewSignal>} reviewSignal
 * @param {ReturnType<typeof buildDealContextFromMerged>} dealContext
 */
export function aggregateProfileSnapshotMeta(profiles, reviewSignal, dealContext) {
  const dataGaps = new Set();
  const actions = new Set();

  if (dealContext.country === NOT_PROVIDED) {
    dataGaps.add("Country / market is not provided on the deal record.");
  }
  if (dealContext.projectType === NOT_PROVIDED) {
    dataGaps.add("Project type is not provided.");
  }
  if (dealContext.desiredOperatingModel === NOT_PROVIDED) {
    dataGaps.add("Preferred future operating model is not provided.");
  }
  if (reviewSignal.level === "Insufficient Data") {
    dataGaps.add(reviewSignal.rationale);
  }

  for (const p of profiles) {
    for (const g of p.dataGaps || []) dataGaps.add(g);
    for (const a of p.suggestedWorkflowActions || []) actions.add(a);
  }

  if (reviewSignal.level === "High" || reviewSignal.level === "Medium") {
    actions.add(
      "Suggested workflow action: review operator profile categories against clarified deal inputs before named operator comparison."
    );
  }

  return {
    dataGaps: [...dataGaps],
    suggestedWorkflowActions: [...actions],
  };
}

/**
 * Build full profile-mode snapshot payload (no Airtable).
 * @param {string} dealId
 * @param {Record<string, unknown>} merged
 */
export function buildOperatorAlignmentProfileSnapshot(dealId, merged) {
  const dealContext = buildDealContextFromMerged(merged);
  const operatorReviewSignal = computeOperatorReviewSignal(merged);
  const profilesForReview = evaluateOperatorProfilesForReview(merged);
  const meta = aggregateProfileSnapshotMeta(
    profilesForReview,
    operatorReviewSignal,
    dealContext
  );

  return {
    dealId,
    generatedAt: new Date().toISOString(),
    featureName: OAS_FEATURE_NAME,
    mode: OAS_MODE_PROFILE,
    methodologyNote: METHODOLOGY_NOTE,
    dealContext,
    operatorReviewSignal: {
      level: operatorReviewSignal.level,
      rationale: operatorReviewSignal.rationale,
      matchedSignals: operatorReviewSignal.matchedSignals,
    },
    profilesForReview,
    dataGaps: meta.dataGaps,
    suggestedWorkflowActions: meta.suggestedWorkflowActions,
  };
}

export { METHODOLOGY_NOTE, ARCHETYPES_PATH };
