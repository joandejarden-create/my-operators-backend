/**
 * Documented legacy free-text research segment → canonical demand segment.
 *
 * Source of truth for Opportunity Type remains the twelve/fourteen GDI demand
 * segments. This map only normalizes known research vocabulary that hunters
 * historically wrote into `segment` / `researchSegment` instead of
 * `demandSegmentId`. It does NOT invent categories from opportunity titles.
 *
 * Lifecycle / mechanism labels (FUTURE_CYCLE, FUTURE_WATCH, OVERFLOW_HOUSING)
 * are never treated as demand segments — see NON_DEMAND in opportunity-type-filter.
 *
 * Contract: gdi-legacy-demand-segment-alias-map-v1
 */

import {
  resolveDemandSegment,
  DEMAND_SEGMENT,
} from "./demand-segments/eight-demand-segment-registry-v1.js";

export const LEGACY_DEMAND_SEGMENT_ALIAS_MAP_V1 =
  "gdi-legacy-demand-segment-alias-map-v1";

const A = DEMAND_SEGMENT.ASSOCIATIONS_CONFERENCES_EVENTS.id;
const SPORTS = DEMAND_SEGMENT.SPORTS_ENTERTAINMENT.id;
const SOCIAL = DEMAND_SEGMENT.SOCIAL_SPECIAL_EVENTS.id;
const MEETINGS = DEMAND_SEGMENT.CORPORATE_MEETINGS_INCENTIVES.id;
const TRADE = DEMAND_SEGMENT.TRAVEL_TRADE_GROUP_INTERMEDIARIES.id;
const EDU = DEMAND_SEGMENT.EDUCATION_SCIENTIFIC.id;
const GOV = DEMAND_SEGMENT.GOVERNMENT_PUBLIC_SECTOR.id;
const PROJECTS = DEMAND_SEGMENT.PROJECTS_INFRASTRUCTURE.id;
const RELOC = DEMAND_SEGMENT.RELOCATION_EXTENDED_STAY.id;
const CELEB = DEMAND_SEGMENT.CORPORATE_CELEBRATIONS_PRIVATE_EVENTS.id;
const CATERING = DEMAND_SEGMENT.LOCAL_CATERING_DAY_MEETINGS.id;

/**
 * Exact (normalized) free-text research segment labels → canonical id.
 * Keys must be lowercase trimmed forms of hunter-written segment values.
 */
export const LEGACY_RESEARCH_SEGMENT_ALIASES = Object.freeze({
  // Overflow / housing is a demand mechanism; research used it as segment text.
  "housing / overflow": A,
  "overflow / housing": A,
  "housing/overflow": A,
  "overflow housing": A,
  "hotel block": A,
  "group demand": A,
  "group housing": A,
  convention: A,
  exhibition: A,
  exhibitor: A,
  "exhibitor block": A,
  "exhibitor services": A,
  "exhibitor services / hospitality": A,
  "marine / boat show": A,
  "marine industry": A,
  yachting: A,
  automotive: A,
  partner: A,
  sponsor: A,
  "team partner": A,
  "ai for good global summit 2027 child account": A,

  // Sports & entertainment research labels
  championship: SPORTS,
  "a coruña championship": SPORTS,
  "international fans": SPORTS,
  "artist hospitality / production": SPORTS,

  // Social / cultural / wedding
  wedding: SOCIAL,
  weddings: SOCIAL,
  cultural: SOCIAL,
  "cultural exchange": SOCIAL,
  festival: SOCIAL,
  leisure: SOCIAL,
  community: SOCIAL,
  "public viewing": SOCIAL,
  "non-profit": SOCIAL,
  nonprofit: SOCIAL,
  "nonprofit retreat": SOCIAL,
  arts: SOCIAL,
  "arts action": A,

  // Corporate meetings / incentives / retreats
  business: MEETINGS,
  leadership: MEETINGS,
  retreat: MEETINGS,
  "leadership retreat": MEETINGS,
  "it leadership": MEETINGS,
  insurance: MEETINGS,
  technology: MEETINGS,
  "luxury retreat": MEETINGS,

  // Travel trade / hospitality intermediaries
  "travel advisor": TRADE,
  "travel advisors": TRADE,
  tourism: TRADE,
  travel: TRADE,
  hospitality: TRADE,
  "hotel promotion": TRADE,
  "luxury travel": TRADE,
  voluntourism: TRADE,
  "guest services / hospitality": TRADE,

  // Education / scientific / medical research labels
  veterinary: EDU,
  "medical devices": EDU,
  biotechnology: EDU,

  // Government / projects / relocation / catering / celebrations
  government: GOV,
  "projects & infrastructure": PROJECTS,
  "new residential development for youth": PROJECTS,
  relocation: RELOC,
  "extended stay": RELOC,
  catering: CATERING,
  "holiday celebration": CELEB,
  "corporate celebration": CELEB,
});

/** Lifecycle / commercial-status values that must never drive Opportunity Type. */
export const NON_DEMAND_LIFECYCLE_TYPES = Object.freeze([
  "COMPETITIVE_MARKET_INTELLIGENCE",
  "SALES_OPPORTUNITIES",
  "ALL_COMMERCIAL_INTELLIGENCE",
  "FUTURE_CYCLE",
  "FUTURE_WATCH",
  "RESEARCH_CONTINUATION",
  "CLOSED_DISQUALIFIED",
  "HISTORICAL_COMPETITIVE",
  "NEEDS_FURTHER_VERIFICATION",
  "GROUP_DEMAND",
  "GROUP_HOUSING",
]);

function normKey(v) {
  return String(v || "")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
}

/**
 * Resolve a free-text research segment to a canonical demand segment id.
 * Returns null when the label is unknown / non-demand ("Discovered", blank).
 */
export function resolveLegacyResearchSegmentAlias(segment) {
  const key = normKey(segment);
  if (!key || key === "discovered" || key === "unknown" || key === "other") {
    return null;
  }
  if (LEGACY_RESEARCH_SEGMENT_ALIASES[key]) {
    return LEGACY_RESEARCH_SEGMENT_ALIASES[key];
  }
  // Registry soft resolve (assoc/conference aliases, mice, etc.)
  const resolved = resolveDemandSegment(key);
  if (resolved?.id) return resolved.id;
  // Mechanism phrases embedded in free text
  if (/overflow|housing\s*\/\s*overflow|room\s*block/.test(key)) return A;
  if (/\bwedding/.test(key)) return SOCIAL;
  if (/\b(championship|tournament|copa)\b/.test(key)) return SPORTS;
  if (/\b(exhibitor|sponsor|partner)\b/.test(key)) return A;
  if (/\b(travel\s*advisor|dmc|tour\s*operator)\b/.test(key)) return TRADE;
  return null;
}

/**
 * Recover canonical demandSegmentId from research evidence fields only.
 * Never uses opportunity title as primary authority.
 */
export function inferDemandSegmentIdFromResearchEvidence(opp = {}) {
  const explicit =
    opp.demandSegmentId ||
    opp.customerDemandSegmentId ||
    opp.demandSegmentRouting?.customerDemandSegmentId ||
    null;
  if (explicit) {
    const resolved = resolveDemandSegment(explicit);
    if (resolved?.id) {
      return {
        demandSegmentId: resolved.id,
        source: "explicit",
        from: String(explicit),
      };
    }
  }

  const axis = opp.researchAxis || opp.researchSegmentKey || null;
  if (axis) {
    const resolved = resolveDemandSegment(axis);
    if (resolved?.id && resolved.id !== "corporate_accounts_scaling") {
      return {
        demandSegmentId: resolved.id,
        source: "research_axis",
        from: String(axis),
      };
    }
  }

  const segment = opp.researchSegment || opp.segment || "";
  const fromAlias = resolveLegacyResearchSegmentAlias(segment);
  if (fromAlias) {
    return {
      demandSegmentId: fromAlias,
      source: "legacy_research_segment_alias",
      from: String(segment),
    };
  }

  const typeLabel = normKey(opp.opportunityTypeLabel || opp.demandTypeLabel || "");
  if (/overflow|housing/.test(typeLabel)) {
    return {
      demandSegmentId: A,
      source: "opportunity_type_label_mechanism",
      from: typeLabel,
    };
  }

  // Research-system title contracts (hunter naming), not free-prose guessing.
  const title = String(
    opp.title || opp.displayTitle || opp.opportunityName || opp.eventName || ""
  );
  if (/\bEXHIBITOR BLOCK\b/i.test(title)) {
    return {
      demandSegmentId: A,
      source: "research_title_contract_exhibitor_block",
      from: "EXHIBITOR BLOCK",
    };
  }
  if (/\b(SPONSOR|PARTNER) BLOCK\b/i.test(title)) {
    return {
      demandSegmentId: A,
      source: "research_title_contract_partner_block",
      from: "PARTNER/SPONSOR BLOCK",
    };
  }
  // Named congress / conference / forum research titles → associations.
  if (
    /\b(?:Congreso|Congress|Conference|Forum|Symposium|Summit)\b/i.test(title) ||
    /\bBioCultura\b/i.test(title)
  ) {
    return {
      demandSegmentId: A,
      source: "research_title_contract_named_conference",
      from: title.slice(0, 80),
    };
  }
  // Sports tournament / cup / league / golf-break research titles.
  if (
    /\b(?:Super\s+Copa|Copa\s+de\s+|Liga\b|Tournament|Championship|Golf\s+Planet|Golf\s+Break)\b/i.test(
      title
    )
  ) {
    return {
      demandSegmentId: "sports_entertainment",
      source: "research_title_contract_sports",
      from: title.slice(0, 80),
    };
  }
  // Education / scientific research programme titles.
  if (
    /\b(?:EDULEARN|Faculty\s+of|University\s+of|Universidade|Universidad)\b/i.test(
      title
    )
  ) {
    return {
      demandSegmentId: "education_scientific",
      source: "research_title_contract_education",
      from: title.slice(0, 80),
    };
  }
  // Public-sector / government programme research titles.
  if (
    /\b(?:Xunta\s+de\s+|Government\s+of|Ministerio|Ministerial|Public\s+Sector)\b/i.test(
      title
    ) ||
    /\bNew\s+Residential\s+Development\b/i.test(title)
  ) {
    return {
      demandSegmentId: "government_public_sector",
      source: "research_title_contract_government",
      from: title.slice(0, 80),
    };
  }

  const rationale = `${opp.roomDemandRationale || ""} ${opp.hotelDemandThesis || ""}`;
  if (/overflow\s*\/\s*housing|overflow housing|housing-list participation/i.test(rationale)) {
    return {
      demandSegmentId: A,
      source: "room_demand_overflow_mechanism",
      from: "roomDemandRationale",
    };
  }

  const subtype = normKey(
    opp.demandOpportunitySubtype || opp.demandSignalType || opp.demandFamily || ""
  );
  if (subtype) {
    const fromSubtype = resolveLegacyResearchSegmentAlias(subtype);
    if (fromSubtype) {
      return {
        demandSegmentId: fromSubtype,
        source: "demand_signal_or_subtype",
        from: subtype,
      };
    }
    const resolved = resolveDemandSegment(subtype);
    if (resolved?.id) {
      return {
        demandSegmentId: resolved.id,
        source: "demand_signal_registry",
        from: subtype,
      };
    }
  }

  return {
    demandSegmentId: null,
    source: "unresolved_requires_research",
    from: segment || null,
  };
}

/**
 * Read-path normalization: attach recovered demandSegmentId without wiping
 * evidence, contacts, Market Scope, or lifecycle opportunityType.
 */
export function normalizeOpportunityDemandClassification(opp = {}) {
  const inferred = inferDemandSegmentIdFromResearchEvidence(opp);
  if (!inferred.demandSegmentId) {
    return {
      ...opp,
      demandSegmentNormalization: inferred,
    };
  }
  if (
    opp.demandSegmentId &&
    resolveDemandSegment(opp.demandSegmentId)?.id === inferred.demandSegmentId
  ) {
    return opp;
  }
  return {
    ...opp,
    demandSegmentId: opp.demandSegmentId || inferred.demandSegmentId,
    customerDemandSegmentId:
      opp.customerDemandSegmentId || inferred.demandSegmentId,
    demandSegmentNormalization: inferred,
  };
}

/**
 * Merge guard for research updates / persistence: never let lifecycle types
 * erase a valid demand segment; never treat FUTURE_* as demandSegmentId.
 */
export function preserveDemandSegmentOnResearchUpdate(prior = {}, next = {}) {
  const priorId =
    prior.demandSegmentId ||
    prior.customerDemandSegmentId ||
    inferDemandSegmentIdFromResearchEvidence(prior).demandSegmentId ||
    null;
  const nextRaw = next.demandSegmentId || next.customerDemandSegmentId || null;
  const nextIsLifecycle = NON_DEMAND_LIFECYCLE_TYPES.includes(
    String(nextRaw || "").toUpperCase()
  );
  const nextResolved = nextRaw && !nextIsLifecycle ? resolveDemandSegment(nextRaw) : null;

  const demandSegmentId = nextResolved?.id || priorId || null;
  const opportunityTypeRaw = String(next.opportunityType || "").toUpperCase();
  let opportunityType = next.opportunityType;
  // If update only carries lifecycle type and prior had a real demand mechanism type, keep prior mechanism when next is pure lifecycle overwrite without demandSegmentId.
  if (
    NON_DEMAND_LIFECYCLE_TYPES.includes(opportunityTypeRaw) &&
    prior.opportunityType &&
    !NON_DEMAND_LIFECYCLE_TYPES.includes(String(prior.opportunityType).toUpperCase()) &&
    !next.demandSegmentId &&
    priorId
  ) {
    // Keep lifecycle on opportunityType (commercial status) but restore demandSegmentId.
    opportunityType = next.opportunityType;
  }

  return {
    ...next,
    opportunityType,
    ...(demandSegmentId
      ? {
          demandSegmentId,
          customerDemandSegmentId:
            next.customerDemandSegmentId || demandSegmentId,
        }
      : {}),
  };
}
