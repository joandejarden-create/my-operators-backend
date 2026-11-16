/**
 * Resolve Brand & Portfolio Position when measurement exists but customer
 * publication has not occurred (founder-reviewed / staged).
 *
 * Governance:
 *   measurement_available ≠ customer_published
 *   LOYALTY_BPP family must win for portfolio section
 *   Do not require CUSTOMER_READY / customerPublished for local founder display
 */

import { loadPublishedReport } from "../published-snapshot.js";
import { loadStagedOwnerPayload } from "../review/adp-staged-report-store-v1.js";
import {
  loadLatestLoyaltyPeriod,
  loadLatestCustomerPeriod,
  loadPeriod,
  isLoyaltyMeasurementPeriod,
} from "../data-model.js";
import { bppComparisonCertifiedIsRenderable } from "../certification/adp-bpp-source-api-dom-parity-v1.js";
import { loadLoyaltyBppStagedPack } from "./loyalty-bpp-staged-pack-store-v1.js";
import { resolvePortfolioMapping } from "./brand-portfolio-affiliation-mapping-v1.js";
import { applyBppCustomerPresentationV2 } from "./bpp-customer-presentation-v2.js";
import {
  buildAdpBppCanonicalEvidenceSetV1,
  attachCanonicalEvidenceToBppPayload,
} from "./adp-bpp-canonical-evidence-set-v1.js";
import { hydrateRankingCompetitiveColumns } from "./bpp-peer-competitive-columns-v1.js";

export const ADP_MEASUREMENT_AVAILABLE_BPP_RESOLVER_V1 =
  "ADP_MEASUREMENT_AVAILABLE_BPP_RESOLVER_V1";

const LENS_LABELS = Object.freeze({
  choice_privileges: "Choice Privileges",
  marriott_bonvoy: "Marriott Bonvoy",
  ihg_one_rewards: "IHG One Rewards",
  hilton_honors: "Hilton Honors",
  wyndham_rewards: "Wyndham Rewards",
  radisson_rewards: "Radisson Rewards",
});

function hasRenderableMeasurement(bpp) {
  if (!bpp || typeof bpp !== "object") return false;
  if (bppComparisonCertifiedIsRenderable(bpp)) return true;
  // Defensive: READY + populated KPIs/rows + comparison-certified state
  const state = bpp.bppCustomerState || bpp.bppMeasurementState || "";
  const assurance = bpp.assuranceStatus || "";
  const certifiedish =
    state === "BPP_COMPARISON_CERTIFIED" ||
    state === "BPP_READY_POPULATED_FULL" ||
    state === "BPP_READY_POPULATED_RANK_ONLY" ||
    assurance === "STAGED_COMPARISON_CERTIFIED" ||
    assurance === "CUSTOMER_READY" ||
    assurance === "PERIOD_2_CERTIFIED_LOCAL";
  return (
    bpp.status === "READY" &&
    certifiedish &&
    Array.isArray(bpp.kpis) &&
    bpp.kpis.length > 0 &&
    bpp.ranking &&
    Array.isArray(bpp.ranking.rows) &&
    bpp.ranking.rows.length > 0
  );
}

function hydrateLens(bpp, profile) {
  if (bpp?.lens?.label) return bpp;
  const lensId = bpp?.lensId || bpp?.lens?.lensId || null;
  const mapping = resolvePortfolioMapping(profile?.propertyId || bpp?.propertyId, profile);
  const fromMap =
    mapping?.lenses?.find((l) => l.lensId === (lensId || mapping.defaultLensId)) ||
    mapping?.lenses?.[0] ||
    null;
  if (fromMap) {
    return {
      ...bpp,
      lensId: fromMap.lensId,
      lens: {
        lensId: fromMap.lensId,
        label: fromMap.label,
        portfolioType: fromMap.portfolioType,
        status: fromMap.status || "DEFAULT",
      },
    };
  }
  if (lensId) {
    return {
      ...bpp,
      lensId,
      lens: {
        lensId,
        label: LENS_LABELS[lensId] || lensId.replace(/_/g, " "),
        portfolioType: "LOYALTY_ECOSYSTEM",
        status: "DEFAULT",
      },
    };
  }
  return bpp;
}

function resolveLoyaltyPeriodId(bpp, propertyId) {
  return (
    bpp?.periodId ||
    bpp?.measurement?.periodId ||
    bpp?._bppLoyaltyPeriodId ||
    bpp?.ranking?.rows?.find((r) => r.isSubject && r.periodId)?.periodId ||
    loadLatestLoyaltyPeriod(propertyId)?.periodId ||
    null
  );
}

function peerSetFromRanking(bpp, propertyId) {
  const rows = bpp?.ranking?.rows || [];
  const included = rows
    .filter((r) => !r.isSubject)
    .map((r) => ({
      peerHotel: r.name || r.displayName,
      canonicalEntityId: r.canonicalEntityId || null,
      brand: r.brand || null,
    }))
    .filter((p) => p.peerHotel);
  return {
    peerSetId: bpp?.peerSetId || null,
    propertyId,
    included,
  };
}

function ensureEvidence(bpp, profile) {
  if (
    bpp?.evidence &&
    ((bpp.evidence.positive || []).length ||
      (bpp.evidence.missing || []).length ||
      (bpp.evidence.displacement || []).length)
  ) {
    return bpp;
  }
  const propertyId = profile?.propertyId || bpp?.propertyId;
  const periodId = resolveLoyaltyPeriodId(bpp, propertyId);
  if (!periodId || !profile) return bpp;
  const period = loadPeriod(periodId);
  if (!period || !isLoyaltyMeasurementPeriod(period)) return bpp;
  if (!Array.isArray(period.observations) || !period.observations.length) return bpp;

  const evidenceSet = buildAdpBppCanonicalEvidenceSetV1({
    propertyId,
    periodId,
    observations: period.observations,
    profile,
    peerSet: peerSetFromRanking(bpp, propertyId),
    // Loyalty STANDARD-28 × 4 providers = 112; keep full inspectable set.
    limit: 200,
  });
  return attachCanonicalEvidenceToBppPayload(bpp, evidenceSet);
}

function normalizeMeasurementAvailablePayload(bpp, profile, source) {
  if (!bpp) return null;
  let next = {
    ...bpp,
    customerPublished: false,
    stagedReviewOnly: bpp.stagedReviewOnly !== false,
    showAnalyticalScaffolding: bpp.showAnalyticalScaffolding !== false,
    measurementFamily: bpp.measurementFamily || "LOYALTY_BPP",
    _bppPackSource: bpp._bppPackSource || source,
    _bppMeasurementAvailable: true,
    _bppResolver: ADP_MEASUREMENT_AVAILABLE_BPP_RESOLVER_V1,
  };
  next = hydrateLens(next, profile);
  const periodId = resolveLoyaltyPeriodId(next, profile?.propertyId);
  if (periodId) {
    next.periodId = periodId;
    next.portfolioPeriodId = periodId;
    next.measurement = {
      ...(next.measurement || {}),
      periodId,
      measurementFamily: "LOYALTY_BPP",
      customerPublished: false,
    };
  }
  next = ensureEvidence(next, profile);
  next = hydrateRankingCompetitiveColumns(next, profile);
  next = applyBppCustomerPresentationV2(next);
  // Keep analytical scaffolding on for founder/staged display.
  next.showAnalyticalScaffolding = true;
  if (!next.assuranceStatus || next.assuranceStatus === "NOT_READY") {
    next.assuranceStatus = "STAGED_COMPARISON_CERTIFIED";
  }
  if (!next.bppCustomerState && next.status === "READY") {
    next.bppCustomerState = "BPP_COMPARISON_CERTIFIED";
  }
  return next;
}

/**
 * Load founder/staged measurement-available BPP (never customer-published pack).
 * @returns {object|null}
 */
export function loadMeasurementAvailableBrandPortfolio(propertyId, profile = null) {
  const id = String(propertyId || "").trim();
  if (!id) return null;

  // 1) Dedicated Loyalty BPP staged pack store
  const loyaltyPack = loadLoyaltyBppStagedPack(id);
  if (loyaltyPack?.ok && hasRenderableMeasurement(loyaltyPack.payload)) {
    return normalizeMeasurementAvailablePayload(
      {
        ...loyaltyPack.payload,
        periodId: loyaltyPack.periodId || loyaltyPack.payload.periodId,
      },
      profile,
      "loyalty_bpp_staged"
    );
  }

  // 2) Durable published-current embedded BPP (founder-staged packages)
  const durable = loadPublishedReport(id);
  if (hasRenderableMeasurement(durable?.brandPortfolioPosition)) {
    return normalizeMeasurementAvailablePayload(
      durable.brandPortfolioPosition,
      profile || durable.property,
      "durable_published_embedded"
    );
  }

  // 3) Staged owner report embedded BPP
  const staged = loadStagedOwnerPayload(id);
  if (hasRenderableMeasurement(staged?.payload?.brandPortfolioPosition)) {
    return normalizeMeasurementAvailablePayload(
      staged.payload.brandPortfolioPosition,
      profile || staged.payload.property,
      "staged_owner_embedded"
    );
  }

  return null;
}

export function resolveReportPeriodIds(propertyId, bpp = null) {
  const core = loadLatestCustomerPeriod(propertyId);
  const loyalty =
    bpp?.portfolioPeriodId ||
    bpp?.periodId ||
    bpp?.measurement?.periodId ||
    loadLatestLoyaltyPeriod(propertyId)?.periodId ||
    null;
  return {
    corePeriodId: core?.periodId || null,
    portfolioPeriodId: loyalty || null,
    portfolioMeasurementFamily: loyalty ? "LOYALTY_BPP" : null,
  };
}

export function isMeasurementAvailableBpp(bpp) {
  return hasRenderableMeasurement(bpp);
}
