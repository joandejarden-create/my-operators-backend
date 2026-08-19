/**
 * Scenario-specific commercial peer eligibility.
 * Deterministic matrix from archetypes + material overrides.
 * Eligibility ≠ comparability. No full-set fallback.
 */

import { loadScenarioRegistry } from "../scenario-registry.js";
import {
  IDS,
  COMMERCIAL_RELATIONS,
  SCENARIO_IDS,
} from "./benchmark-brand-ids.js";
import {
  loadBenchmarkEligibleUniverse,
  getBenchmarkEligibleMember,
  listBenchmarkEligibleMembers,
} from "./benchmark-eligible-universe.js";

export const SCENARIO_PEER_ELIGIBILITY_VERSION = "scenario_peer_eligibility_v1";
export const NO_FULL_SET_FALLBACK = true;
export const UNION_GRAIN_BENCHMARK = "PROHIBITED";

const S = SCENARIO_IDS;

const SOFT_CHAIN_COLLECTIONS = new Set([
  IDS.AUTOGRAPH,
  IDS.CURIO,
  IDS.TRIBUTE,
  IDS.TAPESTRY,
  IDS.VIGNETTE,
  IDS.ASCEND,
]);

const SOFT_PLATFORM_SECONDARY = new Set([
  IDS.HANDWRITTEN,
  IDS.MGALLERY,
  IDS.PREFERRED,
  IDS.RAD_IND,
]);

const SOFT_PLATFORM_CONDITIONAL = new Set([
  IDS.TRADEMARK,
  IDS.BW_PREMIER,
  IDS.BW_SIGNATURE,
  IDS.SLH,
  IDS.DESIGN,
]);

const LIFESTYLE_CORE = new Set([
  IDS.INDIGO,
  IDS.KIMPTON,
  IDS.CANOPY,
  IDS.TEMPO,
  IDS.VOCO,
  IDS.AC,
]);

/** Design-led / lifestyle-adjacent flags — not direct enough for the CORE denominator. */
const LIFESTYLE_SECONDARY = new Set([IDS.DESIGN, IDS.RAD_RED]);

/**
 * Preferred is an independent affiliation/representation network, not a lifestyle-flag alternative.
 * Even is wellness/select — a different owner decision than lifestyle individuality.
 */
const LIFESTYLE_PREFERRED_CONDITIONAL = IDS.PREFERRED;
const LIFESTYLE_EVEN_NON_COMPARABLE = IDS.EVEN;

const HARD_UU_CORE = new Set([IDS.WESTIN, IDS.RAD_BLU, IDS.DOUBLETREE, IDS.RADISSON]);

const OWNER_FLEX_CORE = new Set([
  IDS.ASCEND,
  IDS.RAD_IND,
  IDS.TRADEMARK,
  IDS.HANDWRITTEN,
  IDS.PREFERRED,
  IDS.VIGNETTE,
]);

const OWNER_FLEX_SECONDARY = new Set([
  IDS.AUTOGRAPH,
  IDS.CURIO,
  IDS.TAPESTRY,
  IDS.SLH,
  IDS.BW_PREMIER,
]);

const RESIDENCES_CORE = new Set([
  IDS.VIGNETTE,
  IDS.AUTOGRAPH,
  IDS.CURIO,
  IDS.KIMPTON,
  IDS.INDIGO,
  IDS.WESTIN,
]);

const EFFECTIVE_DATE = "2026-08-18";

function hasArch(member, arch) {
  return (member?.archetypes || []).includes(arch);
}

function relation(rel, reason, dimensions, symmetry = "YES", condition = null) {
  return {
    commercialRelation: rel,
    eligibilityReason: reason,
    governedDimensionsUsed: dimensions,
    symmetryExpected: symmetry,
    condition,
    effectiveDate: EFFECTIVE_DATE,
    version: SCENARIO_PEER_ELIGIBILITY_VERSION,
  };
}

function nonComparable(reason, dimensions) {
  return relation("NON_COMPARABLE", reason, dimensions, "YES");
}

/**
 * Commercial relation for one subject-peer pair under one governed scenario.
 */
export function classifyScenarioPeerRelation(subjectId, peerId, scenarioId, opts = {}) {
  if (!subjectId || !peerId || subjectId === peerId) {
    return nonComparable("same_or_missing_entity", ["identity"]);
  }
  const cfg = opts.universe || loadBenchmarkEligibleUniverse();
  const subject = getBenchmarkEligibleMember(subjectId, cfg);
  const peer = getBenchmarkEligibleMember(peerId, cfg);
  if (!subject || !peer) {
    return nonComparable("peer_not_benchmark_eligible", ["benchmark_eligible_universe_v1"]);
  }

  switch (scenarioId) {
    case S.SOFT_BRAND:
      return classifySoftBrand(subjectId, peerId, subject, peer);
    case S.INDEPENDENT_UU_CONVERSION:
    case S.CONVERSION_SUITABILITY:
      return classifyConversion(subjectId, peerId, subject, peer, scenarioId);
    case S.OWNER_FLEXIBILITY:
      return classifyOwnerFlex(subjectId, peerId);
    case S.LIFESTYLE:
      return classifyLifestyle(subjectId, peerId, subject, peer);
    case S.NEWBUILD_UU:
      return classifyNewBuild(subjectId, peerId, subject, peer);
    case S.CHAIN_SCALE:
      return classifyChainScale(subjectId, peerId, subject, peer);
    case S.MARKET_ENTRY:
      return classifyMarketEntry(subjectId, peerId, subject, peer);
    case S.BRANDED_RESIDENCES:
      return classifyResidences(subjectId, peerId);
    case S.OWNER_ECONOMICS:
      return classifyOwnerEconomics(subjectId, peerId, subject, peer);
    case S.DISTRIBUTION_LOYALTY:
      return classifyDistribution(subjectId, peerId, subject, peer);
    case S.HMA_VS_FRANCHISE:
      return classifyHmaFranchise(subjectId, peerId, subject, peer);
    default:
      return nonComparable("scenario_not_in_governed_registry", ["scenario_registry_v1"]);
  }
}

function classifySoftBrand(subjectId, peerId, subject, peer) {
  const dims = ["brandModel", "collectionAffiliation", "ownerIndividuality"];
  if (SOFT_CHAIN_COLLECTIONS.has(subjectId) && SOFT_CHAIN_COLLECTIONS.has(peerId)) {
    return relation("CORE", "direct_chain_collection_affiliation_alternative", dims);
  }
  if (SOFT_CHAIN_COLLECTIONS.has(subjectId) && SOFT_PLATFORM_SECONDARY.has(peerId)) {
    return relation("SECONDARY", "independent_or_parent_collection_platform", dims);
  }
  if (SOFT_PLATFORM_SECONDARY.has(subjectId) && SOFT_CHAIN_COLLECTIONS.has(peerId)) {
    return relation("SECONDARY", "independent_or_parent_collection_platform", dims);
  }
  if (SOFT_PLATFORM_SECONDARY.has(subjectId) && SOFT_PLATFORM_SECONDARY.has(peerId)) {
    return relation("SECONDARY", "collection_platform_to_platform", dims);
  }
  if (
    (SOFT_CHAIN_COLLECTIONS.has(subjectId) || SOFT_PLATFORM_SECONDARY.has(subjectId)) &&
    SOFT_PLATFORM_CONDITIONAL.has(peerId)
  ) {
    return relation(
      "CONDITIONAL",
      "affiliation_without_major_flag_loyalty_as_primary",
      dims,
      "YES",
      "independent_or_soft_affiliation_without_major_loyalty_priority"
    );
  }
  if (
    SOFT_PLATFORM_CONDITIONAL.has(subjectId) &&
    (SOFT_CHAIN_COLLECTIONS.has(peerId) || SOFT_PLATFORM_SECONDARY.has(peerId))
  ) {
    return relation(
      "CONDITIONAL",
      "affiliation_without_major_flag_loyalty_as_primary",
      dims,
      "YES",
      "independent_or_soft_affiliation_without_major_loyalty_priority"
    );
  }
  if (hasArch(subject, "SOFT_COLLECTION") && hasArch(peer, "SOFT_COLLECTION")) {
    return relation("SECONDARY", "shared_soft_collection_archetype", dims);
  }
  return nonComparable("collection_affiliation_not_commercially_equivalent", dims);
}

function classifyConversion(subjectId, peerId, subject, peer, scenarioId) {
  const dims = ["conversionSuitability", "existingAssetReflag", "brandModel"];
  const soft = classifySoftBrand(subjectId, peerId, subject, peer);
  if (soft.commercialRelation !== "NON_COMPARABLE") {
    if (soft.commercialRelation === "CONDITIONAL" && scenarioId === S.INDEPENDENT_UU_CONVERSION) {
      return relation(
        "SECONDARY",
        "independent_conversion_widens_soft_affiliation_set",
        dims
      );
    }
    return { ...soft, governedDimensionsUsed: dims };
  }
  const lifestyleConv = LIFESTYLE_CORE.has(subjectId) && LIFESTYLE_CORE.has(peerId);
  if (lifestyleConv) {
    return relation("CORE", "lifestyle_brand_conversion_alternative", dims);
  }
  const hardConv = HARD_UU_CORE.has(subjectId) && HARD_UU_CORE.has(peerId);
  if (hardConv) {
    return relation("CORE", "hard_upper_upscale_reflag_alternative", dims);
  }
  if (LIFESTYLE_CORE.has(subjectId) && LIFESTYLE_SECONDARY.has(peerId)) {
    return relation("SECONDARY", "adjacent_lifestyle_conversion_option", dims);
  }
  if (LIFESTYLE_SECONDARY.has(subjectId) && LIFESTYLE_CORE.has(peerId)) {
    return relation("SECONDARY", "adjacent_lifestyle_conversion_option", dims);
  }
  if (
    (SOFT_CHAIN_COLLECTIONS.has(subjectId) || hasArch(subject, "SOFT_COLLECTION")) &&
    peerId === IDS.VOCO
  ) {
    return relation(
      "CONDITIONAL",
      "voco_conversion_upscale_not_collection_model",
      dims,
      "YES",
      "owner_open_to_hard_or_upscale_flag_conversion"
    );
  }
  if (
    subjectId === IDS.VOCO &&
    (SOFT_CHAIN_COLLECTIONS.has(peerId) || hasArch(peer, "SOFT_COLLECTION"))
  ) {
    return relation(
      "CONDITIONAL",
      "voco_conversion_upscale_not_collection_model",
      dims,
      "YES",
      "owner_open_to_hard_or_upscale_flag_conversion"
    );
  }
  return nonComparable("conversion_models_not_commercially_substitutable", dims);
}

function classifyOwnerFlex(subjectId, peerId) {
  const dims = ["ownerControl", "affiliationFlexibility", "brandModel"];
  if (OWNER_FLEX_CORE.has(subjectId) && OWNER_FLEX_CORE.has(peerId)) {
    return relation("CORE", "owner_control_affiliation_platform", dims);
  }
  if (OWNER_FLEX_CORE.has(subjectId) && OWNER_FLEX_SECONDARY.has(peerId)) {
    return relation("SECONDARY", "major_collection_with_partial_owner_flexibility", dims);
  }
  if (OWNER_FLEX_SECONDARY.has(subjectId) && OWNER_FLEX_CORE.has(peerId)) {
    return relation("SECONDARY", "major_collection_with_partial_owner_flexibility", dims);
  }
  if (OWNER_FLEX_SECONDARY.has(subjectId) && OWNER_FLEX_SECONDARY.has(peerId)) {
    return relation("SECONDARY", "collection_flexibility_adjacent", dims);
  }
  return nonComparable("hard_or_lifestyle_flag_control_model_not_equivalent", dims);
}

function classifyLifestyle(subjectId, peerId, subject, peer) {
  const dims = ["lifestylePositioning", "designIndividuality"];
  if (LIFESTYLE_CORE.has(subjectId) && LIFESTYLE_CORE.has(peerId)) {
    return relation("CORE", "lifestyle_positioning_alternative", dims);
  }
  if (
    (LIFESTYLE_CORE.has(subjectId) && peerId === LIFESTYLE_EVEN_NON_COMPARABLE) ||
    (subjectId === LIFESTYLE_EVEN_NON_COMPARABLE && LIFESTYLE_CORE.has(peerId))
  ) {
    return nonComparable("even_is_wellness_select_not_lifestyle_individuality_alternative", dims);
  }
  if (
    (LIFESTYLE_CORE.has(subjectId) && peerId === LIFESTYLE_PREFERRED_CONDITIONAL) ||
    (subjectId === LIFESTYLE_PREFERRED_CONDITIONAL && LIFESTYLE_CORE.has(peerId))
  ) {
    return relation(
      "CONDITIONAL",
      "preferred_is_affiliation_platform_not_lifestyle_flag",
      dims,
      "YES",
      "owner_comparing_lifestyle_flag_to_independent_affiliation_network"
    );
  }
  if (LIFESTYLE_CORE.has(subjectId) && LIFESTYLE_SECONDARY.has(peerId)) {
    return relation("SECONDARY", "adjacent_lifestyle_or_design_option", dims);
  }
  if (LIFESTYLE_SECONDARY.has(subjectId) && LIFESTYLE_CORE.has(peerId)) {
    return relation("SECONDARY", "adjacent_lifestyle_or_design_option", dims);
  }
  if (LIFESTYLE_SECONDARY.has(subjectId) && LIFESTYLE_SECONDARY.has(peerId)) {
    return relation("SECONDARY", "lifestyle_adjacent_to_adjacent", dims);
  }
  if (subjectId === IDS.TRIBUTE && LIFESTYLE_CORE.has(peerId)) {
    return relation(
      "CONDITIONAL",
      "tribute_is_lifestyle_collection_not_hard_lifestyle_flag",
      dims,
      "YES",
      "owner_values_lifestyle_story_inside_collection_model"
    );
  }
  if (peerId === IDS.TRIBUTE && LIFESTYLE_CORE.has(subjectId)) {
    return relation(
      "CONDITIONAL",
      "tribute_is_lifestyle_collection_not_hard_lifestyle_flag",
      dims,
      "YES",
      "owner_values_lifestyle_story_inside_collection_model"
    );
  }
  if (hasArch(subject, "LIFESTYLE") && hasArch(peer, "LIFESTYLE")) {
    const subjectCollection = hasArch(subject, "SOFT_COLLECTION");
    const peerCollection = hasArch(peer, "SOFT_COLLECTION");
    if (subjectCollection !== peerCollection) {
      return relation(
        "CONDITIONAL",
        "lifestyle_collection_vs_lifestyle_flag",
        dims,
        "YES",
        "owner_values_lifestyle_story_inside_or_outside_collection_model"
      );
    }
    return relation("SECONDARY", "shared_lifestyle_archetype", dims);
  }
  return nonComparable("collection_or_hard_brand_is_not_lifestyle_substitute", dims);
}

function classifyNewBuild(subjectId, peerId, subject, peer) {
  const dims = ["newBuild", "chainScale", "brandFamily"];
  if (LIFESTYLE_CORE.has(subjectId) && LIFESTYLE_CORE.has(peerId)) {
    return relation("CORE", "new_build_lifestyle_shortlist", dims);
  }
  if (HARD_UU_CORE.has(subjectId) && HARD_UU_CORE.has(peerId)) {
    return relation("CORE", "new_build_hard_upper_upscale_shortlist", dims);
  }
  if (SOFT_CHAIN_COLLECTIONS.has(subjectId) && SOFT_CHAIN_COLLECTIONS.has(peerId)) {
    return relation(
      "CONDITIONAL",
      "new_build_collection_affiliation_less_typical_than_flag",
      dims,
      "YES",
      "owner_pursuing_collection_for_new_build"
    );
  }
  if (LIFESTYLE_CORE.has(subjectId) && LIFESTYLE_SECONDARY.has(peerId)) {
    return relation("SECONDARY", "new_build_adjacent_lifestyle", dims);
  }
  if (LIFESTYLE_SECONDARY.has(subjectId) && LIFESTYLE_CORE.has(peerId)) {
    return relation("SECONDARY", "new_build_adjacent_lifestyle", dims);
  }
  return nonComparable("new_build_shortlist_not_commercially_overlapping", dims);
}

function classifyChainScale(subjectId, peerId, subject, peer) {
  const dims = ["hotelChainScale", "positioning"];
  if (HARD_UU_CORE.has(subjectId) && HARD_UU_CORE.has(peerId)) {
    return relation("CORE", "upper_upscale_hard_brand_positioning", dims);
  }
  if (LIFESTYLE_CORE.has(subjectId) && LIFESTYLE_CORE.has(peerId)) {
    return relation("CORE", "upper_upscale_lifestyle_positioning", dims);
  }
  if (SOFT_CHAIN_COLLECTIONS.has(subjectId) && SOFT_CHAIN_COLLECTIONS.has(peerId)) {
    return relation("SECONDARY", "collection_still_upper_upscale_but_different_model", dims);
  }
  if (
    (HARD_UU_CORE.has(subjectId) && peerId === IDS.AC) ||
    (subjectId === IDS.AC && HARD_UU_CORE.has(peerId))
  ) {
    return relation("SECONDARY", "urban_uu_positioning_adjacent_to_hard_flag", dims);
  }
  if (LIFESTYLE_CORE.has(subjectId) && LIFESTYLE_SECONDARY.has(peerId)) {
    return relation("SECONDARY", "lifestyle_positioning_adjacent", dims);
  }
  if (LIFESTYLE_SECONDARY.has(subjectId) && LIFESTYLE_CORE.has(peerId)) {
    return relation("SECONDARY", "lifestyle_positioning_adjacent", dims);
  }
  return nonComparable("chain_scale_positioning_not_substitutable", dims);
}

function classifyMarketEntry(subjectId, peerId, subject, peer) {
  const dims = ["geographicRelevance", "brandFamily"];
  if (SOFT_CHAIN_COLLECTIONS.has(subjectId) && SOFT_CHAIN_COLLECTIONS.has(peerId)) {
    return relation("CORE", "same_collection_model_market_entry", dims);
  }
  if (LIFESTYLE_CORE.has(subjectId) && LIFESTYLE_CORE.has(peerId)) {
    return relation("CORE", "same_lifestyle_model_market_entry", dims);
  }
  if (HARD_UU_CORE.has(subjectId) && HARD_UU_CORE.has(peerId)) {
    return relation("CORE", "same_hard_uu_model_market_entry", dims);
  }
  const shareSoft = hasArch(subject, "SOFT_COLLECTION") && hasArch(peer, "SOFT_COLLECTION");
  const shareLife = hasArch(subject, "LIFESTYLE") && hasArch(peer, "LIFESTYLE");
  const shareHard = hasArch(subject, "HARD_UU") && hasArch(peer, "HARD_UU");
  if (shareSoft || shareLife || shareHard) {
    return relation("SECONDARY", "adjacent_uu_market_entry_option", dims);
  }
  return nonComparable("market_entry_set_not_the_same_commercial_model", dims);
}

function classifyResidences(subjectId, peerId) {
  const dims = ["brandedResidencesStatus"];
  if (RESIDENCES_CORE.has(subjectId) && RESIDENCES_CORE.has(peerId)) {
    return relation("CORE", "governed_residences_capable_alternative", dims);
  }
  if (RESIDENCES_CORE.has(subjectId) || RESIDENCES_CORE.has(peerId)) {
    return relation(
      "CONDITIONAL",
      "residences_only_if_peer_has_governed_capability",
      dims,
      "YES",
      "branded_residences_in_program"
    );
  }
  return nonComparable("no_governed_residences_capability_overlap", dims);
}

function classifyOwnerEconomics(subjectId, peerId, subject, peer) {
  const dims = ["ownerEconomics", "brandModel"];
  const soft = classifySoftBrand(subjectId, peerId, subject, peer);
  if (soft.commercialRelation === "CORE" || soft.commercialRelation === "SECONDARY") {
    return { ...soft, eligibilityReason: "economics_compared_within_affiliation_model", governedDimensionsUsed: dims };
  }
  const life = classifyLifestyle(subjectId, peerId, subject, peer);
  if (life.commercialRelation === "CORE" || life.commercialRelation === "SECONDARY") {
    return { ...life, eligibilityReason: "economics_compared_within_lifestyle_set", governedDimensionsUsed: dims };
  }
  const hard = HARD_UU_CORE.has(subjectId) && HARD_UU_CORE.has(peerId);
  if (hard) {
    return relation("CORE", "economics_compared_within_hard_uu_set", dims);
  }
  return nonComparable("fee_and_economic_structure_not_same_deal_type", dims);
}

function classifyDistribution(subjectId, peerId, subject, peer) {
  const dims = ["distribution", "loyalty"];
  if (SOFT_CHAIN_COLLECTIONS.has(subjectId) && SOFT_CHAIN_COLLECTIONS.has(peerId)) {
    return relation("SECONDARY", "major_parent_loyalty_platforms_among_collections", dims);
  }
  if (LIFESTYLE_CORE.has(subjectId) && LIFESTYLE_CORE.has(peerId)) {
    return relation("SECONDARY", "major_parent_loyalty_among_lifestyle", dims);
  }
  if (HARD_UU_CORE.has(subjectId) && HARD_UU_CORE.has(peerId)) {
    return relation("CORE", "full_service_loyalty_and_distribution", dims);
  }
  return nonComparable("distribution_loyalty_not_comparable_across_models", dims);
}

function classifyHmaFranchise(subjectId, peerId, subject, peer) {
  const dims = ["contractStructure"];
  if (HARD_UU_CORE.has(subjectId) && HARD_UU_CORE.has(peerId)) {
    return relation(
      "CONDITIONAL",
      "hma_vs_franchise_is_contract_choice_not_brand_shortlist",
      dims,
      "YES",
      "owner_comparing_franchise_versus_management_under_uu_flags"
    );
  }
  return nonComparable("hma_vs_franchise_is_operator_or_contract_axis_not_brand_peer_set", dims);
}

export function listGovernedBenchmarkScenarios(registry) {
  const reg = registry || loadScenarioRegistry();
  return (reg.scenarios || []).filter((s) => s.monitoringPanel === "CORE");
}

/**
 * Peers listed for diagnostic mixed-set comparison: CORE + SECONDARY.
 * Production CORE-first policy uses CORE only as the denominator.
 * CONDITIONAL excluded unless conditionSatisfied=true.
 */
export function resolveScenarioCommercialPeers(subjectId, scenarioId, opts = {}) {
  const cfg = opts.universe || loadBenchmarkEligibleUniverse();
  const includeConditional = opts.includeConditional === true;
  const members = listBenchmarkEligibleMembers(cfg);
  const core = [];
  const secondary = [];
  const conditional = [];
  const nonComparable = [];
  const rows = [];

  for (const peer of members) {
    if (peer.brandId === subjectId) continue;
    const rel = classifyScenarioPeerRelation(subjectId, peer.brandId, scenarioId, { universe: cfg });
    const row = {
      scenarioId,
      subjectBrandId: subjectId,
      peerBrandId: peer.brandId,
      peerBrandName: peer.brandName,
      customerVisible: peer.customerVisible,
      internalBenchmarkOnly: peer.internalBenchmarkOnly,
      ...rel,
    };
    rows.push(row);
    if (rel.commercialRelation === "CORE") core.push(row);
    else if (rel.commercialRelation === "SECONDARY") secondary.push(row);
    else if (rel.commercialRelation === "CONDITIONAL") conditional.push(row);
    else nonComparable.push(row);
  }

  const calculationPeers = [
    ...core,
    ...secondary,
    ...(includeConditional ? conditional : []),
  ];

  return {
    ok: true,
    subjectBrandId: subjectId,
    scenarioId,
    version: SCENARIO_PEER_ELIGIBILITY_VERSION,
    NO_FULL_SET_FALLBACK,
    usedBroaderFallback: false,
    core,
    secondary,
    conditional,
    nonComparable,
    calculationPeers,
    rows,
  };
}

/**
 * Named mandatory CORE peers that cannot be replaced by a pile of SECONDARY brands.
 */
export const MANDATORY_CORE_BY_SUBJECT_SCENARIO = Object.freeze({
  [IDS.AUTOGRAPH]: {
    [S.SOFT_BRAND]: [IDS.CURIO, IDS.VIGNETTE],
    [S.INDEPENDENT_UU_CONVERSION]: [IDS.CURIO],
    [S.CONVERSION_SUITABILITY]: [IDS.CURIO],
  },
  [IDS.CURIO]: {
    [S.SOFT_BRAND]: [IDS.AUTOGRAPH, IDS.VIGNETTE],
    [S.INDEPENDENT_UU_CONVERSION]: [IDS.AUTOGRAPH],
  },
  [IDS.INDIGO]: {
    [S.LIFESTYLE]: [IDS.KIMPTON, IDS.VOCO],
  },
  [IDS.WESTIN]: {
    [S.CHAIN_SCALE]: [IDS.RAD_BLU],
    [S.NEWBUILD_UU]: [IDS.RAD_BLU],
  },
  [IDS.ASCEND]: {
    [S.OWNER_FLEXIBILITY]: [IDS.RAD_IND, IDS.VIGNETTE],
    [S.SOFT_BRAND]: [IDS.AUTOGRAPH, IDS.CURIO],
  },
});

export function listMandatoryCorePeerIds(subjectId, scenarioId) {
  return MANDATORY_CORE_BY_SUBJECT_SCENARIO[subjectId]?.[scenarioId] || [];
}

export function auditRelationSymmetry(opts = {}) {
  const cfg = opts.universe || loadBenchmarkEligibleUniverse();
  const scenarios = (opts.scenarioIds || listGovernedBenchmarkScenarios().map((s) => s.scenarioId));
  const members = listBenchmarkEligibleMembers(cfg);
  let symmetric = 0;
  let asymmetricJustified = 0;
  let asymmetricUnjustified = 0;
  const unjustifiedPairs = [];

  for (const scenarioId of scenarios) {
    for (let i = 0; i < members.length; i += 1) {
      for (let j = i + 1; j < members.length; j += 1) {
        const a = members[i].brandId;
        const b = members[j].brandId;
        const ab = classifyScenarioPeerRelation(a, b, scenarioId, { universe: cfg });
        const ba = classifyScenarioPeerRelation(b, a, scenarioId, { universe: cfg });
        const comparable = (rel) => rel === "CORE" || rel === "SECONDARY";
        if (ab.commercialRelation === ba.commercialRelation) {
          symmetric += 1;
          continue;
        }
        if (ab.symmetryExpected === "NO" || ba.symmetryExpected === "NO") {
          asymmetricJustified += 1;
          continue;
        }
        if (comparable(ab.commercialRelation) || comparable(ba.commercialRelation)) {
          if (ab.commercialRelation !== ba.commercialRelation) {
            asymmetricUnjustified += 1;
            unjustifiedPairs.push({
              scenarioId,
              a,
              b,
              ab: ab.commercialRelation,
              ba: ba.commercialRelation,
            });
          }
        } else {
          symmetric += 1;
        }
      }
    }
  }

  return {
    SYMMETRIC: symmetric,
    ASYMMETRIC_JUSTIFIED: asymmetricJustified,
    ASYMMETRIC_UNJUSTIFIED: asymmetricUnjustified,
    unjustifiedPairs,
    COMMERCIAL_RELATIONS,
  };
}

export function relevantScenarioIdsForSubject(subjectId, opts = {}) {
  const cfg = opts.universe || loadBenchmarkEligibleUniverse();
  const scenarios = listGovernedBenchmarkScenarios().map((s) => s.scenarioId);
  const relevant = [];
  for (const scenarioId of scenarios) {
    const resolved = resolveScenarioCommercialPeers(subjectId, scenarioId, { universe: cfg });
    if (resolved.core.length + resolved.secondary.length > 0) relevant.push(scenarioId);
  }
  return relevant;
}
