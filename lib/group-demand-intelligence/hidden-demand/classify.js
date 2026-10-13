/**
 * Obvious market demand vs hidden hotel opportunity classification.
 * Generic — uses calendar/source heuristics, not hotel names.
 */

import { DISCOVERY_DEPTH, RECLASS_LABEL, DEMAND_FAMILY } from "./constants.js";

const OBVIOUS_CALENDAR_RE =
  /\b(javits|cvent|cvb|nyc\s*tourism|convention\s*center|events?\s*calendar|trade\s*show\s*calendar|major\s*events?\s*calendar)\b/i;

const OBVIOUS_MEGA_EVENT_RE =
  /\b(nrf|jpmorgan|new\york\s*comic\s*con|fashion\s*week|us\s*open|world\s*cup|olympics|knicks|rangers|mets|yankees|broadway\s*week|hotel\s*week)\b/i;

const HIDDEN_ENTITY_RE =
  /\b(exhibitor|sponsor|vendor|agency|production|crew|installer|distributor|tour\s*operator|delegation|cohort|project\s*team|advisory\s*board|committee|board\s*meeting|leadership\s*summit|showroom|activation)\b/i;

const DIRECT_DEPLOY_RE =
  /\b(\d{1,3}[\s-]?(person|people|staff|employees|consultants|attendees)|hotel\s*block|room\s*block|temporary\s*assignment|field\s*team|implementation\s*team|training\s*cohort)\b/i;

/**
 * Classify discovery depth from title/org/source blob.
 */
export function classifyDiscoveryDepth({
  title,
  organizationName,
  sourceUrl,
  sourceClass,
  hasParentGenerator,
  hasSubgroupEvidence,
  lodgingSignalStrength,
} = {}) {
  const blob = `${title || ""} ${organizationName || ""} ${sourceUrl || ""} ${sourceClass || ""}`;
  if (DIRECT_DEPLOY_RE.test(blob) && !OBVIOUS_MEGA_EVENT_RE.test(title || "")) {
    return DISCOVERY_DEPTH.DIRECT_HIDDEN_SIGNAL;
  }
  if (hasSubgroupEvidence || (hasParentGenerator && HIDDEN_ENTITY_RE.test(blob))) {
    return DISCOVERY_DEPTH.TWO_LAYERS_DEEP;
  }
  if (HIDDEN_ENTITY_RE.test(blob) || hasParentGenerator) {
    return DISCOVERY_DEPTH.ONE_LAYER_DEEP;
  }
  if (
    OBVIOUS_CALENDAR_RE.test(blob) ||
    OBVIOUS_MEGA_EVENT_RE.test(blob) ||
    /annual\s+(meeting|conference|convention)\b/i.test(title || "")
  ) {
    return DISCOVERY_DEPTH.OBVIOUS_MARKET_DEMAND;
  }
  if (lodgingSignalStrength === "STRONG" || lodgingSignalStrength === "MEDIUM") {
    return DISCOVERY_DEPTH.ONE_LAYER_DEEP;
  }
  return DISCOVERY_DEPTH.OBVIOUS_MARKET_DEMAND;
}

/**
 * Should this be customer-promoted as hotel opportunity (vs demand generator only)?
 */
export function isCustomerPromotableHiddenDemand(depth, { deeperMotion = false } = {}) {
  if (
    depth === DISCOVERY_DEPTH.TWO_LAYERS_DEEP ||
    depth === DISCOVERY_DEPTH.DIRECT_HIDDEN_SIGNAL
  ) {
    return true;
  }
  if (depth === DISCOVERY_DEPTH.ONE_LAYER_DEEP && deeperMotion) return true;
  return false;
}

/**
 * Reclassify an existing GDI opportunity row.
 * Obvious calendar titles with a hotel-addressable lodging motion are
 * DEEPER_MOTION_EXISTS (keep as WATCH) — not erased.
 */
export function reclassifyExistingOpportunity(opp = {}) {
  const motionBlob = `${opp.commercialMotion || ""} ${opp.lodgingPrimaryMotion || ""} ${opp.opportunityType || ""} ${opp.roomDemandStatus || ""}`;
  const hasLodgingMotion = /OVERFLOW|HOUSING|ROOM_BLOCK|CREW|TOUR|ASSOCIATION_HOUSING|CITYWIDE_CONVENTION_HOUSING|SPORTS_HOUSING|SOCIAL_HOUSING|ENTERTAINMENT/i.test(
    motionBlob
  );
  const depth = classifyDiscoveryDepth({
    title: opp.title,
    organizationName: opp.organizationName,
    sourceUrl: opp.officialSource || opp.sources?.[0]?.url,
    hasParentGenerator: Boolean(opp.demandGeneratorId || opp.demandTrigger),
    hasSubgroupEvidence: Boolean(
      opp.hiddenDemandId ||
        opp.lodgingPrimaryMotion ||
        /EXHIBITOR|CREW|COHORT|DELEGATION|SUBGROUP|PROJECT/i.test(motionBlob)
    ),
    lodgingSignalStrength:
      opp.lodgingEvidence?.roomBlockMentioned || hasLodgingMotion ? "MEDIUM" : "UNKNOWN",
  });

  if (depth === DISCOVERY_DEPTH.DIRECT_HIDDEN_SIGNAL || depth === DISCOVERY_DEPTH.TWO_LAYERS_DEEP) {
    return {
      label: RECLASS_LABEL.GENUINELY_HIDDEN,
      depth,
      action: "KEEP",
    };
  }
  if (hasLodgingMotion) {
    return {
      label: RECLASS_LABEL.DEEPER_MOTION_EXISTS,
      depth: depth === DISCOVERY_DEPTH.OBVIOUS_MARKET_DEMAND ? DISCOVERY_DEPTH.ONE_LAYER_DEEP : depth,
      action: "KEEP_AS_WATCH",
    };
  }
  if (depth === DISCOVERY_DEPTH.ONE_LAYER_DEEP) {
    return {
      label: RECLASS_LABEL.DEEPER_MOTION_EXISTS,
      depth,
      action: "KEEP_AS_WATCH",
    };
  }
  if (depth === DISCOVERY_DEPTH.OBVIOUS_MARKET_DEMAND) {
    return {
      label: RECLASS_LABEL.OBVIOUS_GENERATOR_ONLY,
      depth,
      action: "DOWNGRADE_TO_GENERATOR",
    };
  }
  return {
    label: RECLASS_LABEL.NEEDS_MORE_RESEARCH,
    depth,
    action: "RESEARCH",
  };
}

/**
 * Infer demand family from text.
 */
export function inferDemandFamily(blob = "") {
  const t = String(blob || "").toLowerCase();
  if (/exhibitor|sponsor booth|vendor|installer/.test(t)) return DEMAND_FAMILY.EXHIBITOR_VENDOR;
  if (/production|av\b|staging|crew|lighting|show.?manage/.test(t)) return DEMAND_FAMILY.PRODUCTION_CREW;
  if (/consulting|implementation|office open|product launch|relocation/.test(t)) {
    return DEMAND_FAMILY.CORPORATE_PROJECT;
  }
  if (/training|academy|cohort|certification|new.?hire/.test(t)) return DEMAND_FAMILY.TRAINING;
  if (/tour operator|escorted tour|student tour|affinity tour/.test(t)) return DEMAND_FAMILY.TOUR_SERIES;
  if (/trade mission|delegation|buying mission|chamber/.test(t)) return DEMAND_FAMILY.DELEGATION;
  if (/university|mba|alumni|student club|model un|faculty/.test(t)) return DEMAND_FAMILY.EDUCATION;
  if (/college team|youth team|officials|sports.?product|supporter club/.test(t)) {
    return DEMAND_FAMILY.SPORTS_ADJACENT;
  }
  if (/advertising agency|experiential|pr firm|brand activation/.test(t)) return DEMAND_FAMILY.AGENCY;
  if (/fashion|showroom|market week|buyer/.test(t)) return DEMAND_FAMILY.FASHION;
  // Association / lodging congress before pharma "advisory board" false positive
  if (
    /board meeting|committee|leadership summit|task.?force|workshop|lodging congress|independent lodging|association/.test(
      t
    )
  ) {
    return DEMAND_FAMILY.ASSOCIATION_SUBGROUP;
  }
  if (/pharma|investigator|advisory board|medical education|clinical/.test(t)) {
    return DEMAND_FAMILY.MEDICAL_PHARMA;
  }
  if (/wedding|reunion|private event|social venue/.test(t)) return DEMAND_FAMILY.SOCIAL;
  return DEMAND_FAMILY.EXHIBITOR_VENDOR;
}
