/**
 * Customer-surface revalidation — no grandfathering.
 * Classifies each opportunity for active GDI customer visibility.
 */

import {
  ACTIVE_DATE_CLASS,
  classifyActiveDate,
  applyActiveDateClassification,
} from "./active-eligibility-v1.js";
import { ENTITY_CLASS, classifyEntityTruth } from "./entity-truth-gate-v1.js";
import { PRIORITY } from "./claim-types.js";

export const CUSTOMER_SURFACE_DISPOSITION = Object.freeze({
  KEEP_ACTIVE: "KEEP_ACTIVE",
  DOWNGRADE_TO_MARKET_ENTITY: "DOWNGRADE_TO_MARKET_ENTITY",
  DOWNGRADE_TO_DEMAND_GENERATOR: "DOWNGRADE_TO_DEMAND_GENERATOR",
  PAST_CLOSED: "PAST_CLOSED",
  INVALID_ENTITY: "INVALID_ENTITY",
  INSUFFICIENT_LODGING_PROOF: "INSUFFICIENT_LODGING_PROOF",
  INSUFFICIENT_TEAM_PROOF: "INSUFFICIENT_TEAM_PROOF",
  MARKET_HIDDEN_CANDIDATE: "MARKET_HIDDEN_CANDIDATE",
  DQ_OTHER: "DQ_OTHER",
});

const INVALID_ENTITY_CLASSES = new Set([
  ENTITY_CLASS.UI_CHROME,
  ENTITY_CLASS.CTA_TEXT,
  ENTITY_CLASS.PLATFORM_ATTRIBUTION,
  ENTITY_CLASS.NAVIGATION_TEXT,
  ENTITY_CLASS.SESSION_TITLE,
  ENTITY_CLASS.ARTICLE_TITLE,
  ENTITY_CLASS.DIRECTORY_TITLE,
  ENTITY_CLASS.CATEGORY_LABEL,
  ENTITY_CLASS.CITY_NAME,
  ENTITY_CLASS.VENUE_NAME_ONLY,
  ENTITY_CLASS.GENERIC_MARKET_PAGE,
  ENTITY_CLASS.PROMOTION,
  ENTITY_CLASS.HOTEL_PROMOTION,
  ENTITY_CLASS.SUPPLY_SIDE_PROMOTION,
  ENTITY_CLASS.UNKNOWN_INVALID,
]);

function lodgingProof(opp = {}) {
  const le = opp.lodgingEvidence || opp.lodging || opp.hiddenDemand?.lodgingEvidence || null;
  if (!le) return { level: "NONE", overflow: false, mentioned: false };
  if (typeof le === "string") {
    return {
      level: le,
      overflow: /OVERFLOW|HOUSING|BLOCK/i.test(le),
      mentioned: !/UNKNOWN|NONE|MISSING/i.test(le),
    };
  }
  const mentioned = Boolean(le.roomBlockMentioned || le.housingPageFound);
  const overflow = Boolean(le.overflowMentioned);
  const status = String(le.status || le.lodgingProof || "");
  if (/CONFIRMED|STRONG|OFFICIAL/i.test(status) && mentioned) {
    return { level: "CREDIBLE", overflow, mentioned };
  }
  if (mentioned) return { level: "WEAK", overflow, mentioned };
  // overflowMentioned alone (no housing page / room-block) is not lodging proof
  if (overflow) return { level: "NONE", overflow: true, mentioned: false };
  return { level: "NONE", overflow: false, mentioned: false };
}

function isOutOfMarketMegaEvent(opp = {}) {
  const title = String(opp.title || "");
  if (!/\b(world cup|fifa|olympics?)\b/i.test(title)) return false;
  const dest = String(opp.destinationStatus || "");
  // Destination city clearly remote from Midtown NYC / Bethesda patterns
  if (/\blos angeles\b|\bparis\b|\blondon\b|\bdoha\b|\brussia\b/i.test(dest)) {
    return true;
  }
  if (/^LA\s+20\d{2}\s+Olympics/i.test(title) && !/\bnew york\b|\bnyc\b/i.test(dest)) {
    return true;
  }
  return false;
}

function teamProof(opp = {}) {
  if (opp.teamSupported === true) return "PRESENT";
  const te = opp.teamEvidence || opp.hiddenDemand?.teamEvidence || null;
  if (!te) return "MISSING";
  if (te === true || te === "present") return "PRESENT";
  if (typeof te === "object") {
    if (te.teamSupported || te.multiPerson || te.outOfMarket) return "PRESENT";
    if (te.teamEvidenceLevel === "present") return "PRESENT";
  }
  return "MISSING";
}

function isExhibitorStyle(opp = {}) {
  const title = String(opp.title || "");
  const type = String(opp.opportunityType || "");
  return (
    /EXHIBITOR|VENDOR BLOCK|SPONSOR BLOCK/i.test(title) ||
    Boolean(opp.boothNumber) ||
    opp.hiddenDemand === true ||
    /HIDDEN|EXHIBITOR/i.test(String(opp.demandFamily || "")) ||
    type === "FUTURE_WATCH"
  );
}

function isBarePublicDemandGenerator(opp = {}) {
  const title = String(opp.title || "");
  const type = String(opp.opportunityType || "");
  if (isOutOfMarketMegaEvent(opp)) return true;
  // Obvious mega / public events without hotel-specific motion depth
  if (/\b(world cup|fifa|olympics?)\b/i.test(title) && !hasExplicitHotelMotionCopy(opp)) {
    return true;
  }
  if (
    (type === "FUTURE_CYCLE" || type === "PRIMARY_PURSUIT") &&
    lodgingProof(opp).level === "NONE" &&
    teamProof(opp) === "MISSING" &&
    !hasExplicitHotelMotionCopy(opp) &&
    (!opp.venueStatus || /^UNKNOWN$/i.test(String(opp.venueStatus)))
  ) {
    return true;
  }
  return false;
}

function hasExplicitHotelMotionCopy(opp = {}) {
  const thesis = String(opp.hotelOpportunityThesis || "");
  const whyNow = String(opp.whyNow || "");
  const matters = String(opp.summaryWhyMatters || "");
  // Auto-generated capacity / monitor boilerplate is not a hotel opportunity thesis
  const boilerplateThesis =
    /is a credible (overflow|corporate block|sports housing) option for/i.test(thesis);
  const boilerplateWhy =
    /^Monitor — housing \/ hotel sourcing not yet public/i.test(whyNow) ||
    /^Planning for hotel accommodations is essential/i.test(matters);
  if (
    (boilerplateThesis || boilerplateWhy) &&
    lodgingProof(opp).level === "NONE" &&
    (!opp.venueStatus || /^UNKNOWN$/i.test(String(opp.venueStatus)))
  ) {
    const rest = `${opp.venueStatus || ""} ${opp.destinationStatus || ""} ${opp.title || ""}`;
    return /host hotel|hotel TBA|not[_ ]?announced|destination TBD|location TBD|hotel\/venue not|preferred lodging|stay.?to.?play|overflow play|housing pending|room block coming/i.test(
      rest
    );
  }
  const blob = `${boilerplateThesis ? "" : thesis} ${
    /Strong fit signals because event size|\d+-room Midtown|guestrooms in Times Square/i.test(
      String(opp.summaryWhyHotel || "")
    )
      ? ""
      : opp.summaryWhyHotel || ""
  } ${boilerplateWhy ? "" : matters} ${opp.venueStatus || ""} ${opp.destinationStatus || ""} ${opp.title || ""}`;
  return /overflow|housing|room block|host hotel|hotel TBA|not[_ ]?announced|destination TBD|location TBD|hotel\/venue not|preferred lodging|stay.?to.?play|midtown lodging/i.test(
    blob
  );
}

function hasHotelOpportunityThesis(opp = {}) {
  const title = String(opp.title || "");
  const vs = String(opp.venueStatus || "");
  const ds = String(opp.destinationStatus || "");
  const blob = [
    // Ignore capacity boilerplate thesis in the language scan
    /is a credible (overflow|corporate block|sports housing) option for/i.test(
      String(opp.hotelOpportunityThesis || "")
    )
      ? ""
      : opp.hotelOpportunityThesis,
    /Strong fit signals because event size/i.test(String(opp.summaryWhyHotel || "")) ||
    /\d+-room Midtown|guestrooms in Times Square/i.test(String(opp.summaryWhyHotel || ""))
      ? ""
      : opp.summaryWhyHotel,
    /Strong fit signals because event size/i.test(String(opp.fitExplanation || ""))
      ? ""
      : opp.fitExplanation,
    /^Planning for hotel accommodations is essential/i.test(String(opp.summaryWhyMatters || ""))
      ? ""
      : opp.summaryWhyMatters,
    opp.summaryWhat,
    /^Monitor — housing \/ hotel sourcing not yet public/i.test(String(opp.whyNow || ""))
      ? ""
      : opp.whyNow,
    vs,
    ds,
    title,
  ]
    .map((x) => String(x || ""))
    .join(" ");
  if (
    /overflow|housing|room block|host hotel|hotel TBA|hotel(?:\s|&| and)\s*travel|location TBD|venue TBD|destination TBD|not[_ ]?announced|not yet (named|announced|evidenced)|not (yet )?specified|hotel\/venue not|hotel list not|partner hotel|preferred lodging|lodging partnership|vendor\/attendee overflow|stay.?to.?play|recommended-hotel/i.test(
      blob
    )
  ) {
    return true;
  }
  // Competitor or host hotel already named → overflow / pursue motion (Bethesda bar)
  if (
    vs &&
    !/^UNKNOWN$/i.test(vs) &&
    /hotel|marriott|hilton|hyatt|ritz|gaylord|westin|sheraton|omni|venue/i.test(vs)
  ) {
    return true;
  }
  const type = String(opp.opportunityType || "");
  const lodging = lodgingProof(opp);
  if (/OVERFLOW|HOUSING/i.test(type)) {
    if (
      lodging.mentioned ||
      lodging.level === "CREDIBLE" ||
      lodging.level === "WEAK" ||
      hasExplicitHotelMotionCopy(opp) ||
      (vs && !/^UNKNOWN$/i.test(vs))
    ) {
      return true;
    }
  }
  if (/VENUE_PARTNERSHIP/i.test(type)) {
    return true;
  }
  if (isOpenEndedLodgingOpportunity(opp)) return true;
  if (lodging.level === "CREDIBLE" || lodging.level === "WEAK") return true;
  return false;
}

function isOpenEndedLodgingOpportunity(opp = {}) {
  if (opp.peVenueId || opp.hotelVenueFitId || opp.peSignalId) return true;
  const blob = `${opp.title || ""} ${opp.demandType || ""} ${opp.opportunityType || ""}`;
  return /preferred lodging|lodging partnership|venue partnership|corporate offsite|recurring .*pattern/i.test(
    blob
  );
}

/**
 * Classify one opportunity against the current customer-surface contract.
 */
export function classifyCustomerSurfaceOpportunity(opp = {}, opts = {}) {
  const date = classifyActiveDate(opp, opts);
  const entity = classifyEntityTruth(opp);
  const lodging = lodgingProof(opp);
  const team = teamProof(opp);
  const reasons = [];

  if (date.activeDateClass === ACTIVE_DATE_CLASS.PAST_CLOSED) {
    return {
      disposition: CUSTOMER_SURFACE_DISPOSITION.PAST_CLOSED,
      activeDateClass: date.activeDateClass,
      entityClass: entity.entityClass,
      reasons: ["past_event_end_date"],
      keepActive: false,
    };
  }

  if (date.activeDateClass === ACTIVE_DATE_CLASS.UNKNOWN_DATE && !date.activeEligible) {
    if (isOpenEndedLodgingOpportunity(opp) && entity.validEntity) {
      // Preferred-lodging / PE / recurring patterns may lack a single event end date
      // but still represent active commercial lodging motions.
    } else {
      reasons.push("unknown_date_without_future_evidence");
      return {
        disposition: CUSTOMER_SURFACE_DISPOSITION.DQ_OTHER,
        activeDateClass: date.activeDateClass,
        entityClass: entity.entityClass,
        reasons,
        keepActive: false,
      };
    }
  }

  if (!entity.validEntity || INVALID_ENTITY_CLASSES.has(entity.entityClass)) {
    const promo =
      entity.entityClass === ENTITY_CLASS.HOTEL_PROMOTION ||
      entity.entityClass === ENTITY_CLASS.SUPPLY_SIDE_PROMOTION ||
      entity.entityClass === ENTITY_CLASS.PROMOTION;
    return {
      disposition: promo
        ? CUSTOMER_SURFACE_DISPOSITION.DQ_OTHER
        : CUSTOMER_SURFACE_DISPOSITION.INVALID_ENTITY,
      activeDateClass: date.activeDateClass,
      entityClass: entity.entityClass,
      reasons: entity.reasons.concat(promo ? ["supply_side_or_promotion"] : []),
      keepActive: false,
    };
  }

  if (isOutOfMarketMegaEvent(opp)) {
    return {
      disposition: CUSTOMER_SURFACE_DISPOSITION.DOWNGRADE_TO_DEMAND_GENERATOR,
      activeDateClass: date.activeDateClass,
      entityClass: entity.entityClass,
      reasons: ["out_of_market_mega_event"],
      keepActive: false,
    };
  }

  if (isBarePublicDemandGenerator(opp) && !hasExplicitHotelMotionCopy(opp)) {
    return {
      disposition: CUSTOMER_SURFACE_DISPOSITION.DOWNGRADE_TO_DEMAND_GENERATOR,
      activeDateClass: date.activeDateClass,
      entityClass: entity.entityClass,
      reasons: ["public_demand_generator_without_hotel_thesis"],
      keepActive: false,
    };
  }

  if (isExhibitorStyle(opp)) {
    if (team === "MISSING") {
      return {
        disposition: CUSTOMER_SURFACE_DISPOSITION.INSUFFICIENT_TEAM_PROOF,
        activeDateClass: date.activeDateClass,
        entityClass: entity.entityClass,
        reasons: ["exhibitor_style_missing_team_proof"],
        keepActive: false,
      };
    }
    if (lodging.level === "NONE") {
      return {
        disposition: CUSTOMER_SURFACE_DISPOSITION.MARKET_HIDDEN_CANDIDATE,
        activeDateClass: date.activeDateClass,
        entityClass: entity.entityClass,
        reasons: ["team_present_lodging_unproven"],
        keepActive: false,
      };
    }
  }

  // Non-exhibitor hotel opportunities still need a lodging/thesis signal
  if (!isExhibitorStyle(opp) && !hasHotelOpportunityThesis(opp) && lodging.level === "NONE") {
    return {
      disposition: CUSTOMER_SURFACE_DISPOSITION.DOWNGRADE_TO_MARKET_ENTITY,
      activeDateClass: date.activeDateClass,
      entityClass: entity.entityClass,
      reasons: ["market_entity_without_hotel_opportunity_depth"],
      keepActive: false,
    };
  }

  if (!isExhibitorStyle(opp) && team === "MISSING" && lodging.level === "WEAK" && !hasHotelOpportunityThesis(opp)) {
    return {
      disposition: CUSTOMER_SURFACE_DISPOSITION.INSUFFICIENT_LODGING_PROOF,
      activeDateClass: date.activeDateClass,
      entityClass: entity.entityClass,
      reasons: ["weak_lodging_without_thesis"],
      keepActive: false,
    };
  }

  return {
    disposition: CUSTOMER_SURFACE_DISPOSITION.KEEP_ACTIVE,
    activeDateClass: date.activeDateClass,
    entityClass: entity.entityClass,
    reasons: ["passes_customer_surface_contract"],
    keepActive: true,
  };
}

/**
 * Apply disposition onto the opportunity for durable persistence.
 * History is preserved; active list uses customerVisible / customerActiveEligible.
 */
export function applyCustomerSurfaceDisposition(opp = {}, classification, opts = {}) {
  const cls =
    classification || classifyCustomerSurfaceOpportunity(opp, opts);
  const dated = applyActiveDateClassification(opp, opts);
  const keep = cls.keepActive === true;

  let next = {
    ...dated,
    entityClass: cls.entityClass,
    customerSurfaceDisposition: cls.disposition,
    customerSurfaceReasons: cls.reasons,
    customerSurfaceRevalidatedAt: new Date().toISOString(),
    customerActiveEligible: keep,
    customerVisible: keep,
    // Preserve prior visibility for audit — never treat as eligibility
    previouslyCustomerVisible: opp.customerVisible !== false,
  };

  if (!keep) {
    // Soft-remove from salesperson active list without deleting the record
    if (opp.priority && opp.priority !== PRIORITY.DISQUALIFIED) {
      next.priorityBeforeSurfaceDq = opp.priority;
    }
    if (
      cls.disposition === CUSTOMER_SURFACE_DISPOSITION.PAST_CLOSED ||
      cls.disposition === CUSTOMER_SURFACE_DISPOSITION.INVALID_ENTITY ||
      cls.disposition === CUSTOMER_SURFACE_DISPOSITION.DQ_OTHER
    ) {
      next.priority = PRIORITY.DISQUALIFIED;
      next.customerFacingState = "CLOSED";
    } else if (
      cls.disposition === CUSTOMER_SURFACE_DISPOSITION.DOWNGRADE_TO_DEMAND_GENERATOR ||
      cls.disposition === CUSTOMER_SURFACE_DISPOSITION.DOWNGRADE_TO_MARKET_ENTITY
    ) {
      next.priority = PRIORITY.DISQUALIFIED;
      next.customerFacingState = "MARKET_INTELLIGENCE_ONLY";
      next.marketIntelligenceOnly = true;
    } else if (
      cls.disposition === CUSTOMER_SURFACE_DISPOSITION.MARKET_HIDDEN_CANDIDATE ||
      cls.disposition === CUSTOMER_SURFACE_DISPOSITION.INSUFFICIENT_TEAM_PROOF ||
      cls.disposition === CUSTOMER_SURFACE_DISPOSITION.INSUFFICIENT_LODGING_PROOF
    ) {
      next.priority = PRIORITY.DISQUALIFIED;
      next.customerFacingState = "MARKET_HIDDEN_CANDIDATE";
      next.hiddenDemandCandidateState = "MARKET_HIDDEN_CANDIDATE";
    }
  } else {
    // Reactivate previously soft-DQ'd rows that now pass the contract
    if (next.priority === PRIORITY.DISQUALIFIED) {
      next.priority = opp.priorityBeforeSurfaceDq || "WATCHLIST";
    }
    if (
      next.customerFacingState === "CLOSED" ||
      next.customerFacingState === "MARKET_INTELLIGENCE_ONLY" ||
      next.customerFacingState === "MARKET_HIDDEN_CANDIDATE"
    ) {
      next.customerFacingState = "ACTIVE";
    }
    // Presentation is owned by the UI tile — do not stamp card-contract fields here.
  }

  return next;
}

/**
 * Revalidate a full opportunity list; returns bag + tally.
 */
export function revalidateCustomerSurfacePopulation(opportunities = [], opts = {}) {
  const tally = {
    KEEP_ACTIVE: 0,
    PAST_CLOSED: 0,
    INVALID_ENTITY: 0,
    DOWNGRADE_TO_DEMAND_GENERATOR: 0,
    DOWNGRADE_TO_MARKET_ENTITY: 0,
    MARKET_HIDDEN_CANDIDATE: 0,
    INSUFFICIENT_TEAM_PROOF: 0,
    INSUFFICIENT_LODGING_PROOF: 0,
    DQ_OTHER: 0,
  };
  const rows = [];
  const next = (opportunities || []).map((opp) => {
    const cls = classifyCustomerSurfaceOpportunity(opp, opts);
    tally[cls.disposition] = (tally[cls.disposition] || 0) + 1;
    const applied = applyCustomerSurfaceDisposition(opp, cls, opts);
    rows.push({
      id: opp.id,
      title: opp.title,
      organizationName: opp.organizationName,
      disposition: cls.disposition,
      entityClass: cls.entityClass,
      activeDateClass: cls.activeDateClass,
      reasons: cls.reasons,
      keepActive: cls.keepActive,
    });
    return applied;
  });

  return {
    opportunities: next,
    tally,
    rows,
    activeCount: next.filter((o) => o.customerActiveEligible === true).length,
  };
}

export function isCustomerSurfaceActiveEligible(opp = {}, opts = {}) {
  if (opp == null || typeof opp !== "object") return false;
  if (opp.isTestData === true) return false;
  if (opp.customerVisible === false) return false;
  if (opp.customerActiveEligible === false) return false;
  if (opp.priority === PRIORITY.DISQUALIFIED) return false;
  if (
    opp.customerSurfaceDisposition &&
    opp.customerSurfaceDisposition !== CUSTOMER_SURFACE_DISPOSITION.KEEP_ACTIVE
  ) {
    return false;
  }
  // Runtime revalidation — no grandfathering even when disposition not yet stamped
  const cls = classifyCustomerSurfaceOpportunity(opp, opts);
  return cls.keepActive === true;
}
