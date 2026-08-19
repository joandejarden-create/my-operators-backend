/**
 * Independent commercial review of included peers — not a copy of the rule engine.
 * Eligibility ≠ confirmed commercial fit.
 */

import { IDS, SCENARIO_IDS as S } from "./benchmark-brand-ids.js";

const SOFT_CORE = new Set([
  IDS.AUTOGRAPH,
  IDS.CURIO,
  IDS.TRIBUTE,
  IDS.TAPESTRY,
  IDS.VIGNETTE,
  IDS.ASCEND,
]);

const LIFESTYLE_CORE = new Set([
  IDS.INDIGO,
  IDS.KIMPTON,
  IDS.CANOPY,
  IDS.TEMPO,
  IDS.VOCO,
  IDS.AC,
]);

const HARD_CORE = new Set([IDS.WESTIN, IDS.RAD_BLU, IDS.DOUBLETREE, IDS.RADISSON]);

const OWNER_FLEX_CORE = new Set([
  IDS.ASCEND,
  IDS.RAD_IND,
  IDS.TRADEMARK,
  IDS.HANDWRITTEN,
  IDS.PREFERRED,
  IDS.VIGNETTE,
]);

const RESIDENCES_MIXED_ARCHITECTURE = new Set([
  ...SOFT_CORE,
  ...LIFESTYLE_CORE,
  IDS.WESTIN,
]);

/**
 * Independent commercial judgment for one included calculation peer.
 */
export function reviewIncludedPeer(subjectId, scenarioId, peerId, relation) {
  if (relation === "CORE") {
    if (scenarioId === S.SOFT_BRAND || scenarioId === S.CONVERSION_SUITABILITY || scenarioId === S.INDEPENDENT_UU_CONVERSION) {
      if (SOFT_CORE.has(subjectId) && SOFT_CORE.has(peerId)) {
        return { review: "CORE_CONFIRMED", why: "direct_chain_collection_alternative" };
      }
      if (LIFESTYLE_CORE.has(subjectId) && LIFESTYLE_CORE.has(peerId)) {
        return { review: "CORE_CONFIRMED", why: "lifestyle_conversion_alternative" };
      }
      if (HARD_CORE.has(subjectId) && HARD_CORE.has(peerId)) {
        return { review: "CORE_CONFIRMED", why: "hard_uu_reflag_alternative" };
      }
    }
    if (scenarioId === S.LIFESTYLE && LIFESTYLE_CORE.has(subjectId) && LIFESTYLE_CORE.has(peerId)) {
      return { review: "CORE_CONFIRMED", why: "lifestyle_positioning_alternative" };
    }
    if (scenarioId === S.OWNER_FLEXIBILITY && OWNER_FLEX_CORE.has(subjectId) && OWNER_FLEX_CORE.has(peerId)) {
      return { review: "CORE_CONFIRMED", why: "owner_control_affiliation_platform" };
    }
    if ((scenarioId === S.CHAIN_SCALE || scenarioId === S.NEWBUILD_UU) && HARD_CORE.has(subjectId) && HARD_CORE.has(peerId)) {
      return { review: "CORE_CONFIRMED", why: "hard_uu_positioning_alternative" };
    }
    if ((scenarioId === S.CHAIN_SCALE || scenarioId === S.NEWBUILD_UU) && LIFESTYLE_CORE.has(subjectId) && LIFESTYLE_CORE.has(peerId)) {
      return { review: "CORE_CONFIRMED", why: "lifestyle_uu_shortlist" };
    }
    if (scenarioId === S.BRANDED_RESIDENCES) {
      if (RESIDENCES_MIXED_ARCHITECTURE.has(subjectId) && RESIDENCES_MIXED_ARCHITECTURE.has(peerId)) {
        const subjectFam = SOFT_CORE.has(subjectId) ? "collection" : LIFESTYLE_CORE.has(subjectId) ? "lifestyle" : "hard";
        const peerFam = SOFT_CORE.has(peerId) ? "collection" : LIFESTYLE_CORE.has(peerId) ? "lifestyle" : "hard";
        if (subjectFam !== peerFam) {
          return {
            review: "QUESTIONABLE",
            why: "residences_capability_overlaps_but_owner_deal_type_differs_across_architecture",
          };
        }
        return { review: "CORE_CONFIRMED", why: "residences_capable_same_architecture" };
      }
    }
    if (scenarioId === S.OWNER_ECONOMICS && SOFT_CORE.has(subjectId) && SOFT_CORE.has(peerId)) {
      return { review: "CORE_CONFIRMED", why: "economics_within_collection_set" };
    }
    if (scenarioId === S.MARKET_ENTRY && SOFT_CORE.has(subjectId) && SOFT_CORE.has(peerId)) {
      return { review: "CORE_CONFIRMED", why: "same_collection_model_market_entry" };
    }
    return { review: "QUESTIONABLE", why: "core_label_not_in_named_commercial_clique" };
  }

  if (relation === "SECONDARY") {
    if (scenarioId === S.LIFESTYLE && (peerId === IDS.DESIGN || peerId === IDS.RAD_RED)) {
      return { review: "SECONDARY_CONFIRMED", why: "adjacent_lifestyle_or_design" };
    }
    if (
      (scenarioId === S.SOFT_BRAND || scenarioId === S.CONVERSION_SUITABILITY) &&
      [IDS.HANDWRITTEN, IDS.MGALLERY, IDS.PREFERRED, IDS.RAD_IND].includes(peerId)
    ) {
      return { review: "SECONDARY_CONFIRMED", why: "collection_platform_adjacent" };
    }
    if (scenarioId === S.OWNER_FLEXIBILITY && SOFT_CORE.has(peerId)) {
      return { review: "SECONDARY_CONFIRMED", why: "major_collection_partial_flexibility" };
    }
    if (scenarioId === S.CHAIN_SCALE && LIFESTYLE_CORE.has(subjectId) && SOFT_CORE.has(peerId)) {
      return { review: "QUESTIONABLE", why: "collection_on_lifestyle_chain_scale_shortlist" };
    }
    return { review: "SECONDARY_CONFIRMED", why: "governed_secondary_comparator" };
  }

  return { review: "INCORRECT", why: "conditional_or_non_comparable_in_calculation_set" };
}
