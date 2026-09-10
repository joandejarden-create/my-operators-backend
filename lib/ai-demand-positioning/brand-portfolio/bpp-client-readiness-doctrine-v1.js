/**
 * ADP Brand & Portfolio — client-readiness doctrine.
 * BPP_POPULATION_IS_EXPECTED_FOR_AFFILIATED_HOTELS.
 * BPP_SUPPRESSION_IS_AN_EXCEPTION, NOT A DEFAULT.
 * CLIENT_READY_REQUIRES_RESOLVED_AND_MAXIMALLY_POPULATED_BPP.
 *
 * Methodology unchanged — interpretation/readiness governance only.
 */

import { METHODOLOGY_IS_GOVERNED_QUALITY_CONTROLS_LEARN } from "../governance/adp-methodology-governance-v1.js";
import { getPortfolioMapping } from "./brand-portfolio-affiliation-mapping-v1.js";
import { getBrandPortfolioPeerSet } from "./brand-portfolio-peer-set-v1.js";
import { getBppHierarchyExhaustion } from "./bpp-hierarchy-exhaustion-ledger-v1.js";
import {
  evaluatePeerSetAdequacy,
  PEER_EXPANSION_HIERARCHY_V1,
  INSUFFICIENT_PORTFOLIO_PEER_SET,
} from "./peer-expansion-hierarchy-v1.js";
import {
  assertSuppressionInventoryEvidence,
  buildSuppressionInventoryEvidence,
} from "./bpp-canonical-ecosystem-discovery-v1.js";
import {
  BRAND_PORTFOLIO_STATUS,
  isBrandPortfolioCustomerReady,
} from "./build-brand-portfolio-position-payload-v1.js";
import { PORTFOLIO_TYPES } from "./brand-portfolio-position-contract-v1.js";

export const BPP_POPULATION_IS_EXPECTED_FOR_AFFILIATED_HOTELS =
  "BPP_POPULATION_IS_EXPECTED_FOR_AFFILIATED_HOTELS.";
export const BPP_SUPPRESSION_IS_AN_EXCEPTION_NOT_A_DEFAULT =
  "BPP_SUPPRESSION_IS_AN_EXCEPTION, NOT A DEFAULT.";
export const CLIENT_READY_REQUIRES_RESOLVED_AND_MAXIMALLY_POPULATED_BPP =
  "CLIENT_READY_REQUIRES_RESOLVED_AND_MAXIMALLY_POPULATED_BPP.";

export const ADP_CLIENT_READY_REQUIRES_BPP_POPULATION_ATTEMPT =
  "ADP_CLIENT_READY_REQUIRES_BPP_POPULATION_ATTEMPT";
export const ADP_CLIENT_READY_REQUIRES_MAXIMALLY_RESOLVED_BPP =
  "ADP_CLIENT_READY_REQUIRES_MAXIMALLY_RESOLVED_BPP";
export const ADP_BPP_ELIGIBILITY_MUST_BE_CLASSIFIED =
  "ADP_BPP_ELIGIBILITY_MUST_BE_CLASSIFIED";
export const ADP_BPP_ELIGIBLE_REQUIRES_CANONICAL_AFFILIATION =
  "ADP_BPP_ELIGIBLE_REQUIRES_CANONICAL_AFFILIATION";
export const ADP_BPP_POPULATE_BEFORE_SUPPRESS = "ADP_BPP_POPULATE_BEFORE_SUPPRESS";
export const ADP_BPP_SUPPRESSION_REQUIRES_EXHAUSTIVE_RESOLUTION =
  "ADP_BPP_SUPPRESSION_REQUIRES_EXHAUSTIVE_RESOLUTION";
export const ADP_BPP_SUPPRESSION_REQUIRES_FULL_ECOSYSTEM_DISCOVERY =
  "ADP_BPP_SUPPRESSION_REQUIRES_FULL_ECOSYSTEM_DISCOVERY";
export const ADP_BPP_ECOSYSTEM_MARKET_INVENTORY_COMPLETENESS =
  "ADP_BPP_ECOSYSTEM_MARKET_INVENTORY_COMPLETENESS";
export const ADP_BPP_CANONICAL_ECOSYSTEM_DISCOVERY =
  "ADP_BPP_CANONICAL_ECOSYSTEM_DISCOVERY";
export const ADP_BPP_READY_REQUIRES_CERTIFIED_PEER_SET =
  "ADP_BPP_READY_REQUIRES_CERTIFIED_PEER_SET";
export const ADP_BPP_READY_REQUIRES_CANONICAL_METRICS =
  "ADP_BPP_READY_REQUIRES_CANONICAL_METRICS";
export const ADP_BPP_REPORT_SNAPSHOT_CANONICAL_PARITY =
  "ADP_BPP_REPORT_SNAPSHOT_CANONICAL_PARITY";
export const ADP_BPP_CANONICAL_PAYLOAD_TO_RENDERER_PARITY =
  "ADP_BPP_CANONICAL_PAYLOAD_TO_RENDERER_PARITY";
export const ADP_NO_INTERNAL_CONFIGURATION_LANGUAGE_CUSTOMER_FACING =
  "ADP_NO_INTERNAL_CONFIGURATION_LANGUAGE_CUSTOMER_FACING";

/** Valid client-ready BPP resolution classes. */
export const BPP_CLIENT_READY_CLASS = Object.freeze({
  BPP_READY_POPULATED: "BPP_READY_POPULATED",
  BPP_EXCEPTION_SUPPRESSED: "BPP_EXCEPTION_SUPPRESSED",
});

/** Invalid / incomplete classes — NOT client ready. */
export const BPP_INVALID_CLASS = Object.freeze({
  NOT_CONFIGURED: "NOT_CONFIGURED",
  AFFILIATION_MISSING: "AFFILIATION_MISSING",
  PEER_DISCOVERY_NOT_RUN: "PEER_DISCOVERY_NOT_RUN",
  PEER_SET_NOT_FROZEN: "PEER_SET_NOT_FROZEN",
  MEASUREMENT_NOT_ATTEMPTED: "MEASUREMENT_NOT_ATTEMPTED",
  AWAITING_FIRST_MONITORING: "AWAITING_FIRST_MONITORING",
  PAYLOAD_MISSING: "PAYLOAD_MISSING",
  RENDERER_MISSING: "RENDERER_MISSING",
  UNKNOWN: "UNKNOWN",
});

const FORBIDDEN_CUSTOMER_PHRASES = Object.freeze([
  "Portfolio monitoring not yet available",
  "affiliation lens is configured",
  "affiliation lens",
  "not configured",
  "setup pending",
  "configuration incomplete",
]);

export function customerFacingBppCopyIsClean(text) {
  const s = String(text || "").toLowerCase();
  return !FORBIDDEN_CUSTOMER_PHRASES.some((p) => s.includes(p.toLowerCase()));
}

/**
 * Classify affiliated vs independent from mapping / profile.
 */
export function classifyBppEligibility(propertyId, profile = {}) {
  const mapping = getPortfolioMapping(propertyId);
  if (!mapping) {
    const brand = profile.brand || profile.affiliation || "";
    const parent = profile.parentCompany || "";
    const looksAffiliated = Boolean(
      parent ||
        /marriott|hilton|hyatt|ihg|choice|radisson|westin|st\.?\s*regis|jw\s*marriott|faranda/i.test(
          `${brand} ${parent}`
        )
    );
    return {
      propertyId,
      affiliated: looksAffiliated,
      independent: /independent/i.test(brand) && !looksAffiliated,
      mappingPresent: false,
      eligibility: looksAffiliated ? "AFFILIATED_UNMAPPED" : "UNCLASSIFIED",
      defaultLensId: null,
      ecosystemId: null,
      sectionMode: null,
      gate: ADP_BPP_ELIGIBILITY_MUST_BE_CLASSIFIED,
      pass: false,
      invalidClass: looksAffiliated
        ? BPP_INVALID_CLASS.AFFILIATION_MISSING
        : BPP_INVALID_CLASS.NOT_CONFIGURED,
    };
  }

  const lens = (mapping.lenses || []).find((l) => l.lensId === mapping.defaultLensId);
  const independent =
    mapping.sectionMode === "INDEPENDENT_POSITIONING" ||
    lens?.portfolioType === PORTFOLIO_TYPES.INDEPENDENT_POSITIONING;

  return {
    propertyId,
    affiliated: !independent,
    independent: Boolean(independent),
    mappingPresent: true,
    eligibility: independent ? "INDEPENDENT" : "AFFILIATED",
    defaultLensId: mapping.defaultLensId,
    ecosystemId: lens?.ecosystemId || lens?.lensId || null,
    sectionMode: mapping.sectionMode,
    gate: ADP_BPP_ELIGIBLE_REQUIRES_CANONICAL_AFFILIATION,
    pass: true,
    invalidClass: null,
  };
}

/**
 * Population attempt resolution for one property (read-only classification).
 * Does not invent peer sets or change methodology.
 */
export function resolveBppPopulationAttempt({
  propertyId,
  profile = {},
  bppPayload = null,
  peerDiscovery = null,
}) {
  const eligibility = classifyBppEligibility(propertyId, profile);
  const peerSet = getBrandPortfolioPeerSet(propertyId);
  const exhaustion = getBppHierarchyExhaustion(propertyId);
  const hierarchy = PEER_EXPANSION_HIERARCHY_V1.map((h) => h.id);
  const discovery = peerDiscovery ||
    (exhaustion
      ? {
          run: exhaustion.run === true,
          levelsAttempted: [...(exhaustion.levelsAttempted || hierarchy)],
          candidateCount: exhaustion.candidateCount ?? 0,
          certifiedPeerCount: exhaustion.certifiedPeerCount ?? 0,
          hierarchyExhausted: exhaustion.hierarchyExhausted === true,
          suppressionReason: exhaustion.suppressionReason || null,
          notes: exhaustion.note ? [exhaustion.note] : [],
        }
      : {
          run: Boolean(peerSet) || eligibility.mappingPresent,
          levelsAttempted: peerSet ? hierarchy.slice(0, peerSet.expansionLevelUsed || 1) : [],
          candidateCount: peerSet?.peerCountExcludingSubject ?? 0,
          certifiedPeerCount: peerSet?.peerCountExcludingSubject ?? 0,
          hierarchyExhausted: false,
          notes: [],
        });

  const ready =
    isBrandPortfolioCustomerReady(bppPayload) ||
    (bppPayload?.status === BRAND_PORTFOLIO_STATUS.READY &&
      Array.isArray(bppPayload?.kpis) &&
      bppPayload.kpis.length > 0 &&
      bppPayload?.ranking?.rows?.length > 0);
  const status = bppPayload?.status || null;

  if (ready || (bppPayload?.status === BRAND_PORTFOLIO_STATUS.READY && bppPayload?.customerPublished)) {
    return {
      propertyId,
      clientReadyClass: BPP_CLIENT_READY_CLASS.BPP_READY_POPULATED,
      clientReady: true,
      eligibility,
      peerSetPresent: Boolean(peerSet),
      peerCount: peerSet?.peerCountExcludingSubject ?? 0,
      adequacy: peerSet?.adequacy || null,
      bppStatus: status || BRAND_PORTFOLIO_STATUS.READY,
      discovery,
      populateBeforeSuppress: true,
      suppressionReason: null,
      gates: {
        [ADP_CLIENT_READY_REQUIRES_BPP_POPULATION_ATTEMPT]: true,
        [ADP_CLIENT_READY_REQUIRES_MAXIMALLY_RESOLVED_BPP]: true,
        [ADP_BPP_READY_REQUIRES_CERTIFIED_PEER_SET]: Boolean(peerSet),
        [ADP_BPP_READY_REQUIRES_CANONICAL_METRICS]: true,
        [ADP_BPP_POPULATE_BEFORE_SUPPRESS]: true,
      },
    };
  }

  if (bppPayload?.status === BRAND_PORTFOLIO_STATUS.EXCEPTION_SUPPRESSED) {
    const inventoryEvidence =
      exhaustion?.inventoryEvidence ||
      bppPayload?.hierarchyExhaustion?.inventoryEvidence ||
      buildSuppressionInventoryEvidence(propertyId);
    const inventoryGate = assertSuppressionInventoryEvidence(inventoryEvidence);
    return {
      propertyId,
      clientReadyClass: BPP_CLIENT_READY_CLASS.BPP_EXCEPTION_SUPPRESSED,
      clientReady: inventoryGate.pass === true,
      eligibility,
      peerSetPresent: Boolean(peerSet),
      peerCount: peerSet?.peerCountExcludingSubject ?? discovery.certifiedPeerCount ?? 0,
      adequacy: peerSet?.adequacy || evaluatePeerSetAdequacy(discovery.certifiedPeerCount || 0),
      bppStatus: BRAND_PORTFOLIO_STATUS.EXCEPTION_SUPPRESSED,
      discovery: { ...discovery, hierarchyExhausted: true, run: true, inventoryEvidence },
      populateBeforeSuppress: true,
      suppressionReason:
        bppPayload.suppressionReason ||
        discovery.suppressionReason ||
        "NO_METHODOLOGY_COMPLIANT_CERTIFIED_PEER_SET_AFTER_HIERARCHY_EXHAUSTION",
      gates: {
        [ADP_BPP_SUPPRESSION_REQUIRES_EXHAUSTIVE_RESOLUTION]: true,
        [ADP_BPP_SUPPRESSION_REQUIRES_FULL_ECOSYSTEM_DISCOVERY]: inventoryGate.pass,
        [ADP_BPP_ECOSYSTEM_MARKET_INVENTORY_COMPLETENESS]: inventoryGate.pass,
        [ADP_BPP_POPULATE_BEFORE_SUPPRESS]: true,
        [ADP_CLIENT_READY_REQUIRES_MAXIMALLY_RESOLVED_BPP]: inventoryGate.pass,
      },
      inventoryGate,
    };
  }

  if (!eligibility.mappingPresent && eligibility.affiliated) {
    return {
      propertyId,
      clientReadyClass: BPP_INVALID_CLASS.AFFILIATION_MISSING,
      clientReady: false,
      eligibility,
      peerSetPresent: false,
      peerCount: 0,
      adequacy: null,
      bppStatus: status || BRAND_PORTFOLIO_STATUS.NOT_CONFIGURED,
      discovery,
      populateBeforeSuppress: false,
      suppressionReason: null,
      gates: {
        [ADP_CLIENT_READY_REQUIRES_BPP_POPULATION_ATTEMPT]: false,
        [ADP_BPP_ELIGIBLE_REQUIRES_CANONICAL_AFFILIATION]: false,
      },
    };
  }

  // Exception suppression only after exhaustive hierarchy + no certified peer path remains
  const adequacy = peerSet
    ? peerSet.adequacy
    : evaluatePeerSetAdequacy(discovery.certifiedPeerCount || 0);
  const thin =
    !peerSet ||
    adequacy?.status === INSUFFICIENT_PORTFOLIO_PEER_SET ||
    (discovery.certifiedPeerCount || 0) < 3;

  if (
    eligibility.mappingPresent &&
    discovery.hierarchyExhausted === true &&
    thin &&
    discovery.run === true
  ) {
    const inventoryEvidence =
      exhaustion?.inventoryEvidence || buildSuppressionInventoryEvidence(propertyId);
    const inventoryGate = assertSuppressionInventoryEvidence(inventoryEvidence);
    return {
      propertyId,
      clientReadyClass: BPP_CLIENT_READY_CLASS.BPP_EXCEPTION_SUPPRESSED,
      clientReady: inventoryGate.pass === true,
      eligibility,
      peerSetPresent: Boolean(peerSet),
      peerCount: peerSet?.peerCountExcludingSubject ?? discovery.certifiedPeerCount ?? 0,
      adequacy,
      bppStatus: "EXCEPTION_SUPPRESSED",
      discovery: { ...discovery, inventoryEvidence },
      populateBeforeSuppress: true,
      suppressionReason:
        discovery.suppressionReason ||
        "NO_METHODOLOGY_COMPLIANT_CERTIFIED_PEER_SET_AFTER_HIERARCHY_EXHAUSTION",
      gates: {
        [ADP_BPP_SUPPRESSION_REQUIRES_EXHAUSTIVE_RESOLUTION]: true,
        [ADP_BPP_SUPPRESSION_REQUIRES_FULL_ECOSYSTEM_DISCOVERY]: inventoryGate.pass,
        [ADP_BPP_ECOSYSTEM_MARKET_INVENTORY_COMPLETENESS]: inventoryGate.pass,
        [ADP_BPP_POPULATE_BEFORE_SUPPRESS]: true,
        [ADP_CLIENT_READY_REQUIRES_MAXIMALLY_RESOLVED_BPP]: inventoryGate.pass,
      },
      inventoryGate,
    };
  }

  if (eligibility.mappingPresent && !peerSet) {
    return {
      propertyId,
      clientReadyClass: discovery.run
        ? BPP_INVALID_CLASS.PEER_SET_NOT_FROZEN
        : BPP_INVALID_CLASS.PEER_DISCOVERY_NOT_RUN,
      clientReady: false,
      eligibility,
      peerSetPresent: false,
      peerCount: discovery.candidateCount || 0,
      adequacy,
      bppStatus: status || BRAND_PORTFOLIO_STATUS.AWAITING_FIRST_MONITORING,
      discovery,
      populateBeforeSuppress: true,
      suppressionReason: null,
      gates: {
        [ADP_BPP_POPULATE_BEFORE_SUPPRESS]: discovery.run === true,
        [ADP_BPP_READY_REQUIRES_CERTIFIED_PEER_SET]: false,
      },
    };
  }

  if (eligibility.mappingPresent && peerSet && !ready) {
    return {
      propertyId,
      clientReadyClass: BPP_INVALID_CLASS.MEASUREMENT_NOT_ATTEMPTED,
      clientReady: false,
      eligibility,
      peerSetPresent: true,
      peerCount: peerSet.peerCountExcludingSubject,
      adequacy: peerSet.adequacy,
      bppStatus: status || BRAND_PORTFOLIO_STATUS.AWAITING_FIRST_MONITORING,
      discovery: {
        ...discovery,
        run: true,
        certifiedPeerCount: peerSet.peerCountExcludingSubject,
        levelsAttempted: hierarchy.slice(0, peerSet.expansionLevelUsed || 1),
      },
      populateBeforeSuppress: true,
      suppressionReason: null,
      measurementRequired: true,
      gates: {
        [ADP_BPP_READY_REQUIRES_CERTIFIED_PEER_SET]: true,
        [ADP_BPP_READY_REQUIRES_CANONICAL_METRICS]: false,
      },
    };
  }

  return {
    propertyId,
    clientReadyClass: BPP_INVALID_CLASS.UNKNOWN,
    clientReady: false,
    eligibility,
    peerSetPresent: Boolean(peerSet),
    peerCount: peerSet?.peerCountExcludingSubject ?? 0,
    adequacy: peerSet?.adequacy || null,
    bppStatus: status,
    discovery,
    populateBeforeSuppress: false,
    suppressionReason: null,
    gates: {},
  };
}

export function evaluateBppClientReadinessGate(resolution) {
  const ok =
    resolution?.clientReady === true &&
    (resolution.clientReadyClass === BPP_CLIENT_READY_CLASS.BPP_READY_POPULATED ||
      resolution.clientReadyClass === BPP_CLIENT_READY_CLASS.BPP_EXCEPTION_SUPPRESSED);
  return {
    gate: ADP_CLIENT_READY_REQUIRES_MAXIMALLY_RESOLVED_BPP,
    doctrine: [
      METHODOLOGY_IS_GOVERNED_QUALITY_CONTROLS_LEARN,
      BPP_POPULATION_IS_EXPECTED_FOR_AFFILIATED_HOTELS,
      BPP_SUPPRESSION_IS_AN_EXCEPTION_NOT_A_DEFAULT,
      CLIENT_READY_REQUIRES_RESOLVED_AND_MAXIMALLY_POPULATED_BPP,
    ],
    pass: ok,
    clientReady: ok ? "YES" : "NO",
    class: resolution?.clientReadyClass || BPP_INVALID_CLASS.UNKNOWN,
    propertyId: resolution?.propertyId || null,
  };
}

export {
  FORBIDDEN_CUSTOMER_PHRASES,
  PEER_EXPANSION_HIERARCHY_V1,
};
