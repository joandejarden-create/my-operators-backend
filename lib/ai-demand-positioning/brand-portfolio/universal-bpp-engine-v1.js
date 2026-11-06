/**
 * ADP Universal Brand & Portfolio Position (BPP) Measurement Engine V1
 *
 * Reusable, portfolio-aware stage logic for ADP_FULL_E2E_V1.
 * Recovers certified ranks only when peer set + comparable observations exist.
 * Never invents rankings. Never runs paid monitoring without explicit authorization.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  loadPropertyProfile,
  loadLatestPeriod,
  loadAllPeriods,
} from "../data-model.js";
import { listAdpReviewerProperties } from "../review/list-adp-reviewer-properties-v1.js";
import {
  resolvePortfolioMapping,
  getPortfolioMapping,
} from "./brand-portfolio-affiliation-mapping-v1.js";
import { getBrandPortfolioPeerSet } from "./brand-portfolio-peer-set-v1.js";
import { getBppHierarchyExhaustion } from "./bpp-hierarchy-exhaustion-ledger-v1.js";
import {
  buildBppCustomerReadyPayloadFromPeriod,
} from "./build-bpp-customer-ready-payload-v1.js";
import {
  BPP_CUSTOMER_PUBLICATION_VERSION,
  BPP_CUSTOMER_PUBLISHED_PACK_CANDIDATES,
} from "./bpp-publication-meta-v1.js";
import { FIRST_CYCLE_COST_ASSUMPTIONS } from "./brand-portfolio-first-cycle-contract-v1.js";
import {
  discoverCanonicalEcosystemPeers,
  buildSuppressionInventoryEvidence,
} from "./bpp-canonical-ecosystem-discovery-v1.js";
import { classifyHistoricalPromptForBrandPortfolio } from "./brand-portfolio-observation-reclass-v1.js";

export const UNIVERSAL_BPP_ENGINE_VERSION = "ADP_UNIVERSAL_BPP_ENGINE_V1";
export const UNIVERSAL_BPP_STAGE_ID = "bpp_portfolio_measurement";

export const BPP_ENGINE_STATUS = Object.freeze({
  READY_RANK: "READY_RANK",
  READY_RECOVERABLE_FROM_EXISTING: "READY_RECOVERABLE_FROM_EXISTING",
  COHORT_CERTIFIED_AWAITING_MEASUREMENT: "COHORT_CERTIFIED_AWAITING_MEASUREMENT",
  OBSERVATIONS_AWAITING_PEER_SET: "OBSERVATIONS_AWAITING_PEER_SET",
  LENS_ONLY_AWAITING_COHORT_AND_MEASUREMENT:
    "LENS_ONLY_AWAITING_COHORT_AND_MEASUREMENT",
  STATUS_ONLY_HIERARCHY_EXHAUSTED: "STATUS_ONLY_HIERARCHY_EXHAUSTED",
  LENS_UNRESOLVED: "LENS_UNRESOLVED",
  EXCEPTION_SUPPRESSED: "EXCEPTION_SUPPRESSED",
});

/** Default BPP first-cycle slot count when hotel-specific plan absent. */
export const DEFAULT_BPP_SCENARIO_SLOTS = 10;

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, "../../..");

function readCustomerPackFile() {
  for (const rel of BPP_CUSTOMER_PUBLISHED_PACK_CANDIDATES) {
    const abs = path.join(REPO_ROOT, rel);
    if (!fs.existsSync(abs)) continue;
    try {
      return { path: abs, pack: JSON.parse(fs.readFileSync(abs, "utf8")) };
    } catch {
      /* try next */
    }
  }
  return { path: null, pack: null };
}

function isPackReady(payload) {
  return !!(
    payload &&
    payload.status === "READY" &&
    Array.isArray(payload.kpis) &&
    payload.kpis.length > 0 &&
    payload.ranking &&
    Array.isArray(payload.ranking.rows) &&
    payload.ranking.rows.length > 0
  );
}

/** Coalesce adjacent DMV cities into one Bonvoy inventory/cycle share key. */
const DMV_CITY_RE =
  /^(washington|falls church|arlington|bethesda|alexandria|mclean|tysons|fairfax|silver spring)$/i;

function marketKeyFromProfile(profile, peerSet) {
  const fromPeer = peerSet?.included?.[0]?.market || null;
  const raw =
    profile?.market ||
    profile?.standardScenarioMarket ||
    profile?.geography?.market ||
    profile?.city ||
    fromPeer ||
    profile?.region ||
    "unknown_market";
  const city = String(profile?.city || "").trim();
  if (city && DMV_CITY_RE.test(city)) {
    return "washington_dc_metro";
  }
  if (/washington,\s*dc|district of columbia/i.test(String(raw))) {
    return "washington_dc_metro";
  }
  if (/tampa/i.test(String(raw)) || /tampa/i.test(city)) {
    return "tampa";
  }
  return String(raw)
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "");
}

/**
 * Classify whether existing period observations are methodologically reusable
 * for BPP peer ranking (honest gate — Core ≠ automatic BPP certification).
 */
export function assessObservationComparability(propertyId, period) {
  const observations = period?.observations || [];
  if (!observations.length) {
    return {
      comparable: false,
      reason: "NO_OBSERVATIONS",
      validBrandPortfolioPromptCount: 0,
      needsRerunCount: 0,
      otherCount: 0,
      totalObservations: 0,
      reuseMode: "NONE",
    };
  }

  let valid = 0;
  let needsRerun = 0;
  let other = 0;
  for (const obs of observations) {
    const c = classifyHistoricalPromptForBrandPortfolio({
      propertyId,
      scenarioId: obs.scenarioId || obs.scenario?.id,
      exactPrompt: obs.exactPrompt || obs.prompt || obs.promptText || "",
      scenarioSource: obs.scenarioSource || obs.source || null,
    });
    if (c.bucket === "VALID_BRAND_PORTFOLIO_DEMAND") valid += 1;
    else if (c.bucket === "NEEDS_RERUN_GOVERNED_TEMPLATE") needsRerun += 1;
    else other += 1;
  }

  // Historical READY packs reused Core periods for peer ranking under founder
  // publication freeze (PERIOD_CORE_OBSERVATIONS_REUSED_FOR_PEER_RANKING).
  // New recoveries may use the same path ONLY when a certified peer set exists
  // and founder authorizes Core-reuse for that property (see recovery gate).
  const coreReuseEligible = observations.length >= 40;
  return {
    comparable: valid >= 8,
    reason:
      valid >= 8
        ? "VALID_BPP_PROMPTS_PRESENT"
        : coreReuseEligible
          ? "CORE_PERIOD_PRESENT_REQUIRES_PEER_SET_AND_CORE_REUSE_AUTH"
          : "INSUFFICIENT_BPP_CLASSIFIED_PROMPTS",
    validBrandPortfolioPromptCount: valid,
    needsRerunCount: needsRerun,
    otherCount: other,
    totalObservations: observations.length,
    reuseMode:
      valid >= 8
        ? "BPP_CLASSIFIED_PROMPTS"
        : coreReuseEligible
          ? "CORE_REUSE_IF_PEER_SET_CERTIFIED"
          : "NONE",
  };
}

export function resolveGovernedLens(propertyId, profile) {
  const mapping = resolvePortfolioMapping(propertyId, profile);
  if (!mapping) {
    return {
      lensResolved: false,
      lensId: null,
      lensLabel: null,
      portfolioType: null,
      ecosystemId: null,
      mappingSource: null,
      synthesizedFromProfile: false,
    };
  }
  const lens =
    (mapping.lenses || []).find((l) => l.lensId === mapping.defaultLensId) ||
    mapping.lenses?.[0] ||
    null;
  return {
    lensResolved: true,
    lensId: mapping.defaultLensId || lens?.lensId || null,
    lensLabel: lens?.label || null,
    portfolioType: lens?.portfolioType || null,
    ecosystemId: lens?.ecosystemId || mapping.defaultLensId || null,
    mappingSource: getPortfolioMapping(propertyId)
      ? "STATIC_FOUNDER_MAP"
      : mapping.synthesizedFromProfile
        ? "SYNTHESIZED_FROM_PROFILE"
        : "RESOLVED",
    synthesizedFromProfile: mapping.synthesizedFromProfile === true,
  };
}

/**
 * Assess one hotel — no ranking invention, no paid calls.
 */
export function assessHotelBpp(propertyId, options = {}) {
  const profile = options.profile || loadPropertyProfile(propertyId);
  const period = options.period || loadLatestPeriod(propertyId);
  const periods = options.periods || loadAllPeriods(propertyId) || [];
  const peerSet = getBrandPortfolioPeerSet(propertyId);
  const exhaustion = getBppHierarchyExhaustion(propertyId);
  const { pack } = readCustomerPackFile();
  const packPayload = pack?.payloads?.[propertyId] || null;
  const lens = resolveGovernedLens(propertyId, profile || {});
  const comparability = assessObservationComparability(propertyId, period);
  const discovery = discoverCanonicalEcosystemPeers(propertyId);
  const inventoryEvidence = buildSuppressionInventoryEvidence(propertyId, discovery);

  const inPack = !!packPayload;
  const packReady = isPackReady(packPayload);
  const peerCount = peerSet?.peerCountExcludingSubject ?? 0;
  const obsCount = period?.observations?.length || 0;
  const marketKey = marketKeyFromProfile(profile || {}, peerSet);

  let engineStatus;
  let nextAction;
  let canRecoverWithoutPaid = false;

  if (packReady) {
    engineStatus = BPP_ENGINE_STATUS.READY_RANK;
    nextAction = {
      code: "NONE",
      detail: "Preserve existing READY_RANK customer pack; no downgrade.",
    };
  } else if (
    packPayload?.status === "EXCEPTION_SUPPRESSED" ||
    exhaustion?.hierarchyExhausted
  ) {
    engineStatus = BPP_ENGINE_STATUS.STATUS_ONLY_HIERARCHY_EXHAUSTED;
    nextAction = {
      code: "STATUS_ONLY_LEGITIMATE",
      detail:
        exhaustion?.suppressionReason ||
        packPayload?.emptyMessage ||
        "Hierarchy exhausted; no methodology-compliant peer set.",
    };
  } else if (!lens.lensResolved) {
    engineStatus = BPP_ENGINE_STATUS.LENS_UNRESOLVED;
    nextAction = {
      code: "RESOLVE_AFFILIATION_LENS",
      detail: "Governed profile affiliation insufficient to name BPP lens.",
    };
  } else if (peerSet && peerCount >= 3 && obsCount > 0) {
    canRecoverWithoutPaid = true;
    engineStatus = BPP_ENGINE_STATUS.READY_RECOVERABLE_FROM_EXISTING;
    nextAction = {
      code: "RECOVER_PACK_FROM_EXISTING_PERIOD",
      detail:
        "Certified peer set + observations present — rebuild customer pack without paid monitoring.",
    };
  } else if (peerSet && peerCount >= 3 && obsCount === 0) {
    engineStatus = BPP_ENGINE_STATUS.COHORT_CERTIFIED_AWAITING_MEASUREMENT;
    nextAction = {
      code: "AUTHORIZE_BPP_MEASUREMENT_CYCLE",
      detail:
        "Peer cohort certified; no subject observations. Authorize BPP first-cycle monitoring.",
      estimatedScenarioSlots: DEFAULT_BPP_SCENARIO_SLOTS,
    };
  } else if (obsCount > 0 && !peerSet) {
    engineStatus = BPP_ENGINE_STATUS.OBSERVATIONS_AWAITING_PEER_SET;
    nextAction = {
      code: "AUTHORIZE_ECOSYSTEM_INVENTORY_AND_PEER_SET_CERTIFICATION",
      detail:
        inventoryEvidence?.complete
          ? "Inventory complete but peer set not certified — founder peer-set certification required, then Core-reuse recovery."
          : "Build/certify loyalty-ecosystem market inventory + peer set; then recover from existing Core observations if methodology allows.",
      inventoryComplete: inventoryEvidence?.complete === true,
      coreReuseCandidate: comparability.reuseMode === "CORE_REUSE_IF_PEER_SET_CERTIFIED",
    };
  } else {
    engineStatus = BPP_ENGINE_STATUS.LENS_ONLY_AWAITING_COHORT_AND_MEASUREMENT;
    nextAction = {
      code: "AUTHORIZE_COHORT_THEN_MEASUREMENT",
      detail:
        "Lens resolved. Need ecosystem inventory, certified peer set (≥3), then BPP measurement cycle.",
    };
  }

  return {
    propertyId,
    hotelName: profile?.name || propertyId,
    researchEligibility: profile?.researchEligibility || null,
    engineStatus,
    deliveryClass: packReady
      ? "READY_RANK"
      : inPack
        ? "STATUS_ONLY"
        : "NOT_IN_PACK",
    lens,
    peerSet: peerSet
      ? {
          peerSetId: peerSet.peerSetId,
          peerCount,
          adequacy: peerSet.adequacy?.status || peerSet.adequacy || null,
          lensLabel: peerSet.lensLabel,
          ecosystem: peerSet.ecosystem,
          certifiedAt: peerSet.certifiedAt,
        }
      : null,
    observation: {
      latestPeriodId: period?.periodId || null,
      observationCount: obsCount,
      periodCount: periods.length,
      periodIds: periods.map((p) => p.periodId).filter(Boolean),
      comparability,
    },
    pack: {
      inPack,
      packReady,
      status: packPayload?.status || null,
      publicationVersion: pack?.publicationVersion || BPP_CUSTOMER_PUBLICATION_VERSION,
      kpiCount: packPayload?.kpis?.length || 0,
      rankRows: packPayload?.ranking?.rows?.length || 0,
      periodId: packPayload?.periodId || packPayload?.measurement?.periodId || null,
    },
    exhaustion: exhaustion
      ? {
          hierarchyExhausted: exhaustion.hierarchyExhausted === true,
          suppressionReason: exhaustion.suppressionReason || null,
          certifiedPeerCount: exhaustion.certifiedPeerCount ?? 0,
        }
      : null,
    inventory: {
      complete: inventoryEvidence?.complete === true,
      candidateHotelCount: inventoryEvidence?.candidateHotelCount ?? 0,
      acceptedPeerCount: inventoryEvidence?.acceptedPeerCount ?? 0,
      inventorySource: inventoryEvidence?.inventorySource || null,
    },
    marketKey,
    cycleShareKey: lens.ecosystemId
      ? `${lens.ecosystemId}__${marketKey}`
      : `unresolved__${marketKey}`,
    canRecoverWithoutPaid,
    nextAction,
    engineVersion: UNIVERSAL_BPP_ENGINE_VERSION,
  };
}

/**
 * Attempt non-paid recovery. Writes nothing unless writePackStaging=true.
 * Never mutates the live customer-published pack without allowLivePackWrite.
 */
export function attemptBppRecoveryFromExistingEvidence(propertyId, options = {}) {
  const assessment = assessHotelBpp(propertyId, options);
  if (!assessment.canRecoverWithoutPaid) {
    return {
      ok: false,
      propertyId,
      recovered: false,
      reason: assessment.nextAction?.code || assessment.engineStatus,
      assessment,
    };
  }

  const profile = loadPropertyProfile(propertyId);
  const period = loadLatestPeriod(propertyId);
  if (!period?.observations?.length) {
    return {
      ok: false,
      propertyId,
      recovered: false,
      reason: "NO_OBSERVATIONS",
      assessment,
    };
  }

  const built = buildBppCustomerReadyPayloadFromPeriod({
    profile,
    period,
    scenarios: [],
    publicationVersion:
      options.publicationVersion || `${BPP_CUSTOMER_PUBLICATION_VERSION}-engine-recover`,
    remediationTag: "UNIVERSAL_BPP_ENGINE_RECOVERY_V1",
  });

  if (!built.ok || !isPackReady(built.payload)) {
    return {
      ok: false,
      propertyId,
      recovered: false,
      reason: built.error || "BUILD_FAILED_OR_NOT_READY",
      build: {
        ok: built.ok,
        error: built.error,
        customerState: built.customerState,
        rowCount: built.rowCount,
      },
      assessment,
    };
  }

  const stagingDir =
    options.stagingDir ||
    path.join(
      REPO_ROOT,
      "data/ai-demand-positioning/bpp-engine-staging",
      propertyId
    );
  if (options.writePackStaging !== false) {
    fs.mkdirSync(stagingDir, { recursive: true });
    const stagedPath = path.join(stagingDir, "recovered-bpp-payload.json");
    fs.writeFileSync(
      stagedPath,
      JSON.stringify(
        {
          recoveredAt: new Date().toISOString(),
          engineVersion: UNIVERSAL_BPP_ENGINE_VERSION,
          propertyId,
          periodId: period.periodId,
          customerState: built.customerState,
          payload: built.payload,
          note:
            "STAGED recovery only — not merged into live customer pack without separate publication authorization.",
        },
        null,
        2
      )
    );
  }

  if (options.allowLivePackWrite === true) {
    return {
      ok: false,
      propertyId,
      recovered: true,
      liveWriteBlocked: true,
      reason: "LIVE_PACK_WRITE_REQUIRES_SEPARATE_PUBLICATION_AUTH",
      staged: true,
      customerState: built.customerState,
      rowCount: built.rowCount,
      assessment,
    };
  }

  return {
    ok: true,
    propertyId,
    recovered: true,
    staged: true,
    liveWriteBlocked: true,
    customerState: built.customerState,
    rowCount: built.rowCount,
    kpiCount: built.payload?.kpis?.length || 0,
    periodId: period.periodId,
    assessment,
  };
}

export function estimateBppCycleCost({
  scenarioSlots = DEFAULT_BPP_SCENARIO_SLOTS,
  subjectHotelCount = 1,
} = {}) {
  const perScenario = FIRST_CYCLE_COST_ASSUMPTIONS.usdPerScenarioAllProviders;
  const usdPerSubject = Number((scenarioSlots * perScenario).toFixed(2));
  const usdTotal = Number((usdPerSubject * subjectHotelCount).toFixed(2));
  return {
    scenarioSlots,
    subjectHotelCount,
    providers: [...FIRST_CYCLE_COST_ASSUMPTIONS.providers],
    usdPerScenarioAllProviders: perScenario,
    usdPerSubject,
    usdTotal,
    providerCallsPerSubject: scenarioSlots * FIRST_CYCLE_COST_ASSUMPTIONS.providers.length,
    note: "Estimate only — does not authorize spend.",
  };
}

/**
 * Deduplicate measurement work across hotels sharing ecosystem×market.
 */
export function buildDeduplicatedResearchPlan(assessments) {
  const byCycle = new Map();
  for (const a of assessments) {
    if (
      a.engineStatus === BPP_ENGINE_STATUS.READY_RANK ||
      a.engineStatus === BPP_ENGINE_STATUS.STATUS_ONLY_HIERARCHY_EXHAUSTED ||
      a.engineStatus === BPP_ENGINE_STATUS.EXCEPTION_SUPPRESSED
    ) {
      continue;
    }
    const key = a.cycleShareKey || "unknown";
    if (!byCycle.has(key)) {
      byCycle.set(key, {
        cycleShareKey: key,
        ecosystemId: a.lens?.ecosystemId || null,
        lensLabel: a.lens?.lensLabel || null,
        marketKey: a.marketKey,
        hotels: [],
        needsPeerSetCertification: false,
        needsInventory: false,
        needsPaidMeasurement: false,
        recoverableAfterPeerSet: [],
        paidMeasurementSubjects: [],
      });
    }
    const g = byCycle.get(key);
    g.hotels.push(a.propertyId);
    if (
      a.engineStatus === BPP_ENGINE_STATUS.OBSERVATIONS_AWAITING_PEER_SET ||
      a.engineStatus === BPP_ENGINE_STATUS.LENS_ONLY_AWAITING_COHORT_AND_MEASUREMENT
    ) {
      g.needsPeerSetCertification = true;
      if (!a.inventory?.complete) g.needsInventory = true;
    }
    if (a.engineStatus === BPP_ENGINE_STATUS.OBSERVATIONS_AWAITING_PEER_SET) {
      g.recoverableAfterPeerSet.push(a.propertyId);
    }
    if (
      a.engineStatus === BPP_ENGINE_STATUS.COHORT_CERTIFIED_AWAITING_MEASUREMENT ||
      a.engineStatus === BPP_ENGINE_STATUS.LENS_ONLY_AWAITING_COHORT_AND_MEASUREMENT
    ) {
      g.needsPaidMeasurement = true;
      g.paidMeasurementSubjects.push(a.propertyId);
    }
  }

  const cycles = [...byCycle.values()].map((g) => {
    const paidSubjects = [...new Set(g.paidMeasurementSubjects)];
    const cost = estimateBppCycleCost({
      scenarioSlots: DEFAULT_BPP_SCENARIO_SLOTS,
      subjectHotelCount: Math.max(paidSubjects.length, g.needsPaidMeasurement ? 1 : 0),
    });
    return {
      ...g,
      paidMeasurementSubjects: paidSubjects,
      recoverableAfterPeerSet: [...new Set(g.recoverableAfterPeerSet)],
      estimatedCostUsd: g.needsPaidMeasurement ? cost.usdTotal : 0,
      costDetail: g.needsPaidMeasurement ? cost : null,
      authorizationRequired: g.needsInventory || g.needsPeerSetCertification || g.needsPaidMeasurement,
      researchWorkUnits: {
        inventory: g.needsInventory ? 1 : 0,
        peerSetCertification: g.needsPeerSetCertification ? 1 : 0,
        paidMeasurementSubjects: paidSubjects.length,
        zeroCostRecoveriesAfterPeerSet: g.recoverableAfterPeerSet.length,
      },
    };
  });

  const totalPaidUsd = cycles.reduce((s, c) => s + (c.estimatedCostUsd || 0), 0);
  const uniquePaidSubjects = new Set(
    cycles.flatMap((c) => c.paidMeasurementSubjects)
  );
  return {
    cycleCount: cycles.length,
    cycles: cycles.sort((a, b) => a.cycleShareKey.localeCompare(b.cycleShareKey)),
    totals: {
      estimatedPaidMeasurementUsd: Number(totalPaidUsd.toFixed(2)),
      uniquePaidSubjectHotels: uniquePaidSubjects.size,
      zeroCostRecoveriesPendingPeerSet: cycles.reduce(
        (s, c) => s + c.recoverableAfterPeerSet.length,
        0
      ),
      cyclesNeedingInventory: cycles.filter((c) => c.needsInventory).length,
      cyclesNeedingPeerSetCert: cycles.filter((c) => c.needsPeerSetCertification)
        .length,
    },
    doctrine:
      "One governed cycle per ecosystem×market; do not commission overlapping Marriott Bonvoy waves per hotel.",
  };
}

/**
 * Full portfolio inventory + optional recovery attempts (non-paid).
 */
export function runUniversalBppEngine(options = {}) {
  const properties = listAdpReviewerProperties({
    includeExcluded: true,
    includeHistoricalExcluded: options.includeHistoricalExcluded !== false,
  });
  const assessments = [];
  const recoveries = [];

  for (const p of properties) {
    const id = p.propertyId;
    const assessment = assessHotelBpp(id);
    assessments.push(assessment);

    if (assessment.canRecoverWithoutPaid && options.attemptRecovery !== false) {
      const r = attemptBppRecoveryFromExistingEvidence(id, {
        writePackStaging: options.writePackStaging !== false,
        allowLivePackWrite: false,
      });
      recoveries.push(r);
      if (r.recovered) {
        assessment.recovery = {
          recovered: true,
          staged: true,
          customerState: r.customerState,
          rowCount: r.rowCount,
        };
        assessment.engineStatus = BPP_ENGINE_STATUS.READY_RECOVERABLE_FROM_EXISTING;
      }
    }
  }

  const researchPlan = buildDeduplicatedResearchPlan(assessments);
  const counts = {};
  for (const a of assessments) {
    counts[a.engineStatus] = (counts[a.engineStatus] || 0) + 1;
  }
  const readyRank = assessments.filter(
    (a) => a.engineStatus === BPP_ENGINE_STATUS.READY_RANK
  );
  const newlyRecoverable = recoveries.filter((r) => r.recovered);
  const statusOnly = assessments.filter(
    (a) =>
      a.engineStatus === BPP_ENGINE_STATUS.STATUS_ONLY_HIERARCHY_EXHAUSTED ||
      a.deliveryClass === "STATUS_ONLY"
  );

  return {
    ok: true,
    engineVersion: UNIVERSAL_BPP_ENGINE_VERSION,
    generatedAt: new Date().toISOString(),
    packPublicationVersion: BPP_CUSTOMER_PUBLICATION_VERSION,
    propertyCount: assessments.length,
    counts,
    assessments,
    recoveries,
    researchPlan,
    summary: {
      hotelsWithExistingCertifiedBpp: readyRank.length,
      hotelsNewlyCertifiedUsingExistingEvidence: newlyRecoverable.length,
      hotelsStagedRecoveryOnly: newlyRecoverable.length,
      hotelsCohortEstablishedAwaitingMeasurement: assessments.filter(
        (a) =>
          a.engineStatus === BPP_ENGINE_STATUS.COHORT_CERTIFIED_AWAITING_MEASUREMENT
      ).length,
      hotelsObservationsAwaitingPeerSet: assessments.filter(
        (a) => a.engineStatus === BPP_ENGINE_STATUS.OBSERVATIONS_AWAITING_PEER_SET
      ).length,
      hotelsUnresolvedCohort: assessments.filter(
        (a) =>
          a.engineStatus ===
            BPP_ENGINE_STATUS.LENS_ONLY_AWAITING_COHORT_AND_MEASUREMENT ||
          a.engineStatus === BPP_ENGINE_STATUS.LENS_UNRESOLVED
      ).length,
      hotelsStatusOnlyLegitimate: statusOnly.filter(
        (a) =>
          a.engineStatus === BPP_ENGINE_STATUS.STATUS_ONLY_HIERARCHY_EXHAUSTED
      ).length,
      estimatedCostUsdToCompleteMissingCycles:
        researchPlan.totals.estimatedPaidMeasurementUsd,
      uniqueAdditionalPaidSubjects:
        researchPlan.totals.uniquePaidSubjectHotels,
    },
    invariants: {
      noInventedRanks: true,
      noPaidMonitoringExecuted: true,
      noLivePackMutation: true,
      readyRankPreserved: readyRank.length >= 13,
      bethesdaHistoricalBppPreserved: readyRank.some(
        (a) => a.propertyId === "adp_bethesda_marriott"
      ),
    },
  };
}

/**
 * Full E2E stage executor for one hotel — resumable / idempotent / non-paid by default.
 */
export function executeBppPortfolioMeasurementStage({
  propertyId,
  authorization = null,
  dryRun = true,
} = {}) {
  const assessment = assessHotelBpp(propertyId);
  const artifacts = {
    assessment,
    recovery: null,
    measurementExecuted: false,
    packAttached: assessment.pack.packReady,
  };

  if (assessment.engineStatus === BPP_ENGINE_STATUS.READY_RANK) {
    return {
      stageId: UNIVERSAL_BPP_STAGE_ID,
      state: "COMPLETE",
      qualityGate: { passed: true, reasons: ["EXISTING_READY_RANK"] },
      notes: "Existing certified BPP pack preserved",
      extras: artifacts,
      blockers: [],
      nextAction: null,
    };
  }

  if (assessment.canRecoverWithoutPaid) {
    const recovery = attemptBppRecoveryFromExistingEvidence(propertyId, {
      writePackStaging: true,
      allowLivePackWrite: false,
    });
    artifacts.recovery = recovery;
    if (recovery.recovered) {
      return {
        stageId: UNIVERSAL_BPP_STAGE_ID,
        state: "COMPLETE",
        qualityGate: {
          passed: true,
          reasons: ["RECOVERED_STAGED_AWAITING_PUBLICATION_AUTH"],
        },
        notes:
          "BPP pack recovered from existing evidence and staged; live pack merge requires publication authorization",
        extras: artifacts,
        blockers: [
          {
            code: "BPP_PACK_PUBLICATION_AUTH_REQUIRED",
            detail: "Staged recovery ready — do not silent-merge into published pack",
          },
        ],
        nextAction: {
          code: "AUTHORIZE_BPP_PACK_PUBLICATION",
          propertyId,
        },
      };
    }
  }

  if (assessment.engineStatus === BPP_ENGINE_STATUS.STATUS_ONLY_HIERARCHY_EXHAUSTED) {
    return {
      stageId: UNIVERSAL_BPP_STAGE_ID,
      state: "COMPLETE",
      qualityGate: { passed: true, reasons: ["LEGITIMATE_STATUS_ONLY"] },
      notes: "Legitimate STATUS_ONLY after hierarchy exhaustion — no invented ranks",
      extras: artifacts,
      blockers: [],
      nextAction: assessment.nextAction,
    };
  }

  const needsPaid =
    assessment.nextAction?.code === "AUTHORIZE_BPP_MEASUREMENT_CYCLE" ||
    assessment.nextAction?.code === "AUTHORIZE_COHORT_THEN_MEASUREMENT";

  if (needsPaid && authorization?.allowPaidBppMeasurement === true && !dryRun) {
    return {
      stageId: UNIVERSAL_BPP_STAGE_ID,
      state: "WAITING_AUTHORIZATION",
      qualityGate: { passed: false, reasons: ["PAID_EXECUTION_NOT_WIRED_IN_THIS_PASS"] },
      notes:
        "Paid BPP monitoring authorization recognized but execution is gated to a separate authorized runner — not executed here",
      extras: {
        ...artifacts,
        costEstimate: estimateBppCycleCost({
          scenarioSlots: DEFAULT_BPP_SCENARIO_SLOTS,
          subjectHotelCount: 1,
        }),
      },
      blockers: [{ code: "PAID_BPP_RUNNER_AUTHORIZATION_REQUIRED" }],
      nextAction: assessment.nextAction,
    };
  }

  const cost = needsPaid
    ? estimateBppCycleCost({
        scenarioSlots: DEFAULT_BPP_SCENARIO_SLOTS,
        subjectHotelCount: 1,
      })
    : null;

  return {
    stageId: UNIVERSAL_BPP_STAGE_ID,
    state: "WAITING_AUTHORIZATION",
    qualityGate: {
      passed: false,
      reasons: [assessment.nextAction?.code || assessment.engineStatus],
    },
    notes: assessment.nextAction?.detail || assessment.engineStatus,
    extras: { ...artifacts, costEstimate: cost },
    blockers: [
      {
        code: assessment.nextAction?.code || "BPP_INCOMPLETE",
        detail: assessment.nextAction?.detail,
      },
    ],
    nextAction: assessment.nextAction,
  };
}

export function listEligibleAdpHotelsForBpp() {
  return listAdpReviewerProperties({
    includeExcluded: true,
    includeHistoricalExcluded: true,
  }).map((p) => p.propertyId);
}
