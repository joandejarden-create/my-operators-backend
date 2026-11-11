/**
 * BPP_MEASUREMENT_COMPARABILITY_V1
 *
 * Canonical Brand & Portfolio Position measurement protocols and Full E2E gate.
 * Separates (a) dedicated loyalty-constrained BPP cycles from (b) Core peer-ranking
 * observation reuse. Hotels on different protocols must not be treated as
 * silently comparable in customer product claims.
 *
 * Does not invent observations or ranks. Does not authorize paid monitoring.
 */

import { ADP_SCENARIO_DENSITY_TIER_STANDARD_V1 } from "../certification/adp-scenario-universe-certification-v1.js";
import {
  LOYALTY_BPP_SCENARIO_DENSITY_STANDARD_V1,
  LOYALTY_BPP_SCENARIO_DENSITY_LEGACY_PULSE_V1,
  MEASUREMENT_FAMILY_LOYALTY_BPP,
} from "./loyalty-bpp-scenario-standard-v1.js";

export const BPP_MEASUREMENT_COMPARABILITY_V1 = "BPP_MEASUREMENT_COMPARABILITY_V1";

/** Dedicated loyalty / independent BPP monitoring cycle (first/second cycle family). */
export const BPP_PROTOCOL_LOYALTY_CONSTRAINED_V1 =
  "BPP_MEASUREMENT_PROTOCOL_LOYALTY_CONSTRAINED_V1";

/** Core ADP period observations reused for peer ranking (Bethesda-class path). */
export const BPP_PROTOCOL_CORE_PEER_RANKING_REUSE_V1 =
  "BPP_MEASUREMENT_PROTOCOL_CORE_PEER_RANKING_REUSE_V1";

export const BPP_PROTOCOL_EXCEPTION_SUPPRESSED =
  "BPP_MEASUREMENT_PROTOCOL_EXCEPTION_SUPPRESSED";

export const BPP_PROTOCOL_ABSENT = "BPP_MEASUREMENT_PROTOCOL_ABSENT";

/**
 * Canonical product policy (commercially defensible):
 * - Loyalty-constrained BPP is the true Brand & Portfolio protocol.
 * - Default dedicated density is a governed portfolio pulse (~8–10 scenarios × 4 providers).
 * - Core STANDARD density (60–65) is NOT automatically required for BPP loyalty cycles.
 * - Core-reuse is a separate certified protocol (general demand + peer filter), not a density upgrade of loyalty BPP.
 * - Cross-hotel comparable BPP claims require the SAME protocol class (and disclosed density).
 */
export const BPP_CANONICAL_MEASUREMENT_STANDARD_V1 = Object.freeze({
  id: "BPP_CANONICAL_MEASUREMENT_STANDARD_V1",
  measurementObjective:
    "Measure how often AI surfaces this hotel within its governed loyalty / collection / independent portfolio lens versus certified same-ecosystem peers.",
  preferredProtocol: BPP_PROTOCOL_LOYALTY_CONSTRAINED_V1,
  loyaltyConstrained: Object.freeze({
    protocolId: BPP_PROTOCOL_LOYALTY_CONSTRAINED_V1,
    minScenarios: 8,
    targetScenarios: 10,
    maxScenariosPulse: 12,
    providersRequired: 4,
    providerIds: Object.freeze(["openai", "gemini", "perplexity", "claude"]),
    densityBandLabel: "BPP_PORTFOLIO_PULSE_V1",
    notes:
      "Intentional portfolio pulse. Not interchangeable with Core ADP_SCENARIO_DENSITY_TIER_STANDARD_V1 without protocol migration authorization.",
  }),
  corePeerRankingReuse: Object.freeze({
    protocolId: BPP_PROTOCOL_CORE_PEER_RANKING_REUSE_V1,
    reuseTag: "PERIOD_CORE_OBSERVATIONS_REUSED_FOR_PEER_RANKING",
    inheritsCoreDensityTier: ADP_SCENARIO_DENSITY_TIER_STANDARD_V1.id,
    targetScenarios: ADP_SCENARIO_DENSITY_TIER_STANDARD_V1.targetScenarioCount,
    allowedMin: ADP_SCENARIO_DENSITY_TIER_STANDARD_V1.allowedMin,
    allowedMax: ADP_SCENARIO_DENSITY_TIER_STANDARD_V1.allowedMax,
    notes:
      "Scores subject + certified peers on a Core monitoring corpus (typically NEUTRAL_DEMAND). Different research question than loyalty-constrained prompts.",
  }),
  peerCohort: Object.freeze({
    minPeersForRank: 3,
    minPeersForBenchmarkIndex: 5,
  }),
  customerDisclosureRequired: true,
  crossProtocolComparableClaims: false,
  coreDensityDoesNotGovernLoyaltyBpp: true,
});

export function classifyBppMeasurementProtocol(payload = {}) {
  if (!payload || payload.sectionVisible === false) {
    return {
      protocolId: BPP_PROTOCOL_ABSENT,
      scenarioCount: null,
      observationCount: null,
      classification: "ABSENT",
    };
  }
  if (
    payload.status === "EXCEPTION_SUPPRESSED" ||
    payload.bppCustomerState === "BPP_EXCEPTION_SUPPRESSED"
  ) {
    return {
      protocolId: BPP_PROTOCOL_EXCEPTION_SUPPRESSED,
      scenarioCount: null,
      observationCount: null,
      classification: "EXCEPTION_SUPPRESSED",
    };
  }

  const measurement = payload.measurement || {};
  const periodId = String(measurement.periodId || payload.periodId || "");
  const reuse =
    measurement.compatibleObservationReuse ||
    payload.compatibleObservationReuse ||
    null;
  const family = measurement.measurementFamily || payload.measurementFamily || null;

  const presenceMeta = String(
    (payload.kpis || []).find((k) => k.id === "portfolioAiPresence")?.meta ||
      (payload.kpis || []).find((k) => k.id === "portfolioAiPresence")?.help ||
      ""
  );
  const n1Meta = String(
    (payload.kpis || []).find((k) => k.id === "numberOneAppearance")?.meta || ""
  );
  const obsMatch = presenceMeta.match(/(\d+)\s+of\s+(\d+)\s+AI observations/i);
  const scMatch = n1Meta.match(/(\d+)\s+of\s+(\d+)\s+scenarios/i);
  const observationCount =
    Number.isFinite(Number(measurement.observationCount))
      ? Number(measurement.observationCount)
      : obsMatch
        ? Number(obsMatch[2])
        : null;
  const scenarioCount = Number.isFinite(Number(measurement.scenarioCount))
    ? Number(measurement.scenarioCount)
    : scMatch
      ? Number(scMatch[2])
      : observationCount != null && observationCount % 4 === 0
        ? observationCount / 4
        : null;

  // Explicit Loyalty family / protocol wins over Core-reuse heuristics.
  if (
    family === MEASUREMENT_FAMILY_LOYALTY_BPP ||
    measurement.bppMeasurementProtocol === BPP_PROTOCOL_LOYALTY_CONSTRAINED_V1 ||
    measurement.loyaltyScenarioStandard
  ) {
    const std = LOYALTY_BPP_SCENARIO_DENSITY_STANDARD_V1;
    const legacy = LOYALTY_BPP_SCENARIO_DENSITY_LEGACY_PULSE_V1;
    const densityBand =
      scenarioCount != null &&
      scenarioCount >= std.allowedMin &&
      scenarioCount <= std.allowedMax
        ? std.id
        : scenarioCount != null &&
            scenarioCount >= legacy.allowedMin &&
            scenarioCount <= legacy.allowedMax
          ? legacy.id
          : "LOYALTY_CONSTRAINED";
    return {
      protocolId: BPP_PROTOCOL_LOYALTY_CONSTRAINED_V1,
      scenarioCount,
      observationCount,
      periodId: periodId || null,
      reuseTag: null,
      classification:
        densityBand === std.id
          ? "LOYALTY_STANDARD_28"
          : "LOYALTY_CONSTRAINED_PULSE",
      densityBand,
      measurementFamily: MEASUREMENT_FAMILY_LOYALTY_BPP,
    };
  }

  if (
    reuse === "PERIOD_CORE_OBSERVATIONS_REUSED_FOR_PEER_RANKING" ||
    (periodId.startsWith("adp_period_") &&
      scenarioCount != null &&
      scenarioCount >= 60 &&
      family !== MEASUREMENT_FAMILY_LOYALTY_BPP)
  ) {
    return {
      protocolId: BPP_PROTOCOL_CORE_PEER_RANKING_REUSE_V1,
      scenarioCount,
      observationCount,
      periodId: periodId || null,
      reuseTag: reuse || "PERIOD_CORE_OBSERVATIONS_REUSED_FOR_PEER_RANKING",
      classification: "CORE_PEER_RANKING_REUSE",
      densityBand: "CORE_STANDARD_INHERITED",
    };
  }

  if (
    periodId.startsWith("bpp_") ||
    (scenarioCount != null &&
      scenarioCount >= LOYALTY_BPP_SCENARIO_DENSITY_LEGACY_PULSE_V1.allowedMin &&
      scenarioCount <= LOYALTY_BPP_SCENARIO_DENSITY_STANDARD_V1.allowedMax)
  ) {
    const std = LOYALTY_BPP_SCENARIO_DENSITY_STANDARD_V1;
    return {
      protocolId: BPP_PROTOCOL_LOYALTY_CONSTRAINED_V1,
      scenarioCount,
      observationCount,
      periodId: periodId || null,
      reuseTag: null,
      classification:
        scenarioCount >= std.allowedMin && scenarioCount <= std.allowedMax
          ? "LOYALTY_STANDARD_28"
          : "LOYALTY_CONSTRAINED_PULSE",
      densityBand:
        scenarioCount >= std.allowedMin && scenarioCount <= std.allowedMax
          ? std.id
          : "BPP_PORTFOLIO_PULSE_V1",
    };
  }

  return {
    protocolId: BPP_PROTOCOL_ABSENT,
    scenarioCount,
    observationCount,
    periodId: periodId || null,
    classification: "UNCLASSIFIED",
  };
}

export function buildBppMethodologyDisclosure(protocol = {}) {
  const std = BPP_CANONICAL_MEASUREMENT_STANDARD_V1;
  if (protocol.protocolId === BPP_PROTOCOL_CORE_PEER_RANKING_REUSE_V1) {
    const n = protocol.scenarioCount || std.corePeerRankingReuse.targetScenarios;
    const obs =
      protocol.observationCount ||
      (n != null ? n * std.loyaltyConstrained.providersRequired : null);
    return {
      protocolId: protocol.protocolId,
      customerLabel: "Core monitoring reused for portfolio peer ranking",
      customerBody: `Portfolio metrics use this hotel's Core ADP monitoring corpus (${n} scenarios · ${obs} observations across four AI providers), scored against a certified same-ecosystem peer cohort. This is not a separate loyalty-prompt research cycle.`,
      scenarioCount: n,
      observationCount: obs,
      comparableToCoreStandardDensity: true,
      comparableToLoyaltyPulse: false,
      disclosureRequired: true,
    };
  }
  if (protocol.protocolId === BPP_PROTOCOL_LOYALTY_CONSTRAINED_V1) {
    const n =
      protocol.scenarioCount ||
      LOYALTY_BPP_SCENARIO_DENSITY_STANDARD_V1.targetScenarioCount;
    const obs =
      protocol.observationCount ||
      (n != null ? n * LOYALTY_BPP_SCENARIO_DENSITY_STANDARD_V1.providerCount : null);
    const isStandard28 =
      protocol.classification === "LOYALTY_STANDARD_28" ||
      protocol.densityBand === LOYALTY_BPP_SCENARIO_DENSITY_STANDARD_V1.id ||
      (n != null &&
        n >= LOYALTY_BPP_SCENARIO_DENSITY_STANDARD_V1.allowedMin &&
        n <= LOYALTY_BPP_SCENARIO_DENSITY_STANDARD_V1.allowedMax);
    return {
      protocolId: protocol.protocolId,
      customerLabel: isStandard28
        ? "Loyalty STANDARD-28 portfolio measurement"
        : "Loyalty / portfolio pulse measurement",
      customerBody: isStandard28
        ? `Portfolio metrics use the governed Loyalty BPP STANDARD-28 prompt set (${n} scenarios · ${obs} observations across four AI providers) within the hotel's loyalty or independent lens. This is not Core ADP peer-reuse (${std.corePeerRankingReuse.targetScenarios}-scenario monitoring).`
        : `Portfolio metrics use a governed Brand & Portfolio prompt set (${n} scenarios · ${obs} observations across four AI providers) within the hotel's loyalty or independent lens. This portfolio pulse is not the same research design as Core ADP's ${std.corePeerRankingReuse.targetScenarios}-scenario monitoring densification.`,
      scenarioCount: n,
      observationCount: obs,
      comparableToCoreStandardDensity: false,
      comparableToLoyaltyPulse: true,
      disclosureRequired: true,
    };
  }
  if (protocol.protocolId === BPP_PROTOCOL_EXCEPTION_SUPPRESSED) {
    return {
      protocolId: protocol.protocolId,
      customerLabel: "Portfolio measurement not shown",
      customerBody:
        "Brand & Portfolio Position is suppressed for this hotel until a certified lens and peer cohort are available.",
      disclosureRequired: true,
      comparableToCoreStandardDensity: false,
      comparableToLoyaltyPulse: false,
    };
  }
  return {
    protocolId: protocol.protocolId || BPP_PROTOCOL_ABSENT,
    customerLabel: "Portfolio measurement not yet established",
    customerBody:
      "A certified Brand & Portfolio measurement cycle has not been attached for this hotel.",
    disclosureRequired: true,
    comparableToCoreStandardDensity: false,
    comparableToLoyaltyPulse: false,
  };
}

/**
 * Full E2E gate: bpp_measurement_comparability_certification
 * Returns pass/fail with structured blockers — never invents ranks.
 */
export function certifyBppMeasurementComparability(payload = {}, options = {}) {
  const failures = [];
  const warnings = [];
  const protocol = classifyBppMeasurementProtocol(payload);
  const disclosure = buildBppMethodologyDisclosure(protocol);
  const std = BPP_CANONICAL_MEASUREMENT_STANDARD_V1;
  const ready =
    payload?.status === "READY" ||
    String(payload?.bppCustomerState || "").startsWith("BPP_READY");

  if (!ready) {
    return {
      gate: "bpp_measurement_comparability_certification",
      contract: BPP_MEASUREMENT_COMPARABILITY_V1,
      pass: true,
      ready: false,
      protocol,
      disclosure,
      failures: [],
      warnings: ["NOT_READY_RANK_SKIP_DENSITY"],
      customerComparableClaimsAllowed: false,
    };
  }

  if (!protocol.protocolId || protocol.classification === "UNCLASSIFIED") {
    failures.push("BPP_PROTOCOL_UNCLASSIFIED");
  }

  if (protocol.protocolId === BPP_PROTOCOL_LOYALTY_CONSTRAINED_V1) {
    const n = protocol.scenarioCount;
    if (n == null) failures.push("LOYALTY_SCENARIO_COUNT_UNKNOWN");
    else if (n < std.loyaltyConstrained.minScenarios) {
      failures.push(`LOYALTY_SCENARIO_DENSITY_BELOW_MIN_${n}`);
    } else if (n > std.loyaltyConstrained.maxScenariosPulse) {
      warnings.push(`LOYALTY_SCENARIO_DENSITY_ABOVE_PULSE_${n}`);
    }
    if (
      protocol.observationCount != null &&
      n != null &&
      protocol.observationCount !== n * 4
    ) {
      warnings.push("OBS_NOT_EQUAL_SCENARIOS_TIMES_FOUR");
    }
  }

  if (protocol.protocolId === BPP_PROTOCOL_CORE_PEER_RANKING_REUSE_V1) {
    const n = protocol.scenarioCount;
    if (n == null) failures.push("CORE_REUSE_SCENARIO_COUNT_UNKNOWN");
    else if (
      n < std.corePeerRankingReuse.allowedMin ||
      n > std.corePeerRankingReuse.allowedMax
    ) {
      // Expanded Core tier may exist — flag, do not auto-fail if disclosed
      if (options.requireCoreStandardBand !== false) {
        warnings.push(`CORE_REUSE_OUTSIDE_STANDARD_BAND_${n}`);
      }
    }
  }

  const peerCount =
    payload?.peerSet?.peerCount ??
    Math.max(0, (payload?.ranking?.rows || []).length - 1);
  if (peerCount < std.peerCohort.minPeersForRank) {
    failures.push(`PEER_COHORT_BELOW_RANK_MINIMUM_${peerCount}`);
  }

  if (!payload?.measurement?.kpiContractVersion && !payload?.kpis?.length) {
    failures.push("MISSING_KPI_CONTRACT_OR_KPIS");
  }

  // Silent cross-protocol product claim is never certified here —
  // callers must not treat loyalty pulse ranks as Core-density equivalents.
  const customerComparableClaimsAllowed =
    failures.length === 0 &&
    (protocol.protocolId === BPP_PROTOCOL_LOYALTY_CONSTRAINED_V1 ||
      protocol.protocolId === BPP_PROTOCOL_CORE_PEER_RANKING_REUSE_V1);

  return {
    gate: "bpp_measurement_comparability_certification",
    contract: BPP_MEASUREMENT_COMPARABILITY_V1,
    pass: failures.length === 0,
    ready: true,
    protocol,
    disclosure,
    peerCount,
    failures,
    warnings,
    customerComparableClaimsAllowed,
    crossProtocolComparableClaims: std.crossProtocolComparableClaims,
    note: "Ranks within one protocol class may be certified; cross-protocol silent comparison is prohibited.",
  };
}

/**
 * Attach methodology disclosure onto a BPP payload (presentation / attach-at-read).
 * Does not alter certified KPI valueRaw fields.
 */
export function attachBppMethodologyDisclosure(payload) {
  if (!payload || typeof payload !== "object") return payload;
  const na =
    payload.status === "BPP_NOT_APPLICABLE" ||
    payload.bppCustomerState === "BPP_NOT_APPLICABLE" ||
    payload.customerState === "BPP_NOT_APPLICABLE" ||
    payload.renderMode === "STATUS_ONLY" ||
    payload.disclosureReason === "LENS_NOT_APPLICABLE" ||
    payload.measuredRank === "BPP_NOT_APPLICABLE";
  if (na) {
    return {
      ...payload,
      measurementComparability: {
        contract: BPP_MEASUREMENT_COMPARABILITY_V1,
        protocolId: BPP_PROTOCOL_EXCEPTION_SUPPRESSED,
        classification: "NOT_APPLICABLE",
        scenarioCount: null,
        observationCount: null,
        densityBand: null,
        gatePass: true,
        gateFailures: [],
        crossProtocolComparableClaims: false,
      },
      methodologyDisclosure: {
        protocolId: BPP_PROTOCOL_EXCEPTION_SUPPRESSED,
        customerLabel: "Brand & Portfolio not applicable",
        customerBody:
          payload.body ||
          payload.headline ||
          "Brand & Portfolio comparison is not applicable — no certified same-lens peer cohort.",
        disclosureRequired: true,
        comparableToCoreStandardDensity: false,
        comparableToLoyaltyPulse: false,
      },
    };
  }
  const cert = certifyBppMeasurementComparability(payload);
  return {
    ...payload,
    measurementComparability: {
      contract: BPP_MEASUREMENT_COMPARABILITY_V1,
      protocolId: cert.protocol.protocolId,
      classification: cert.protocol.classification,
      scenarioCount: cert.protocol.scenarioCount,
      observationCount: cert.protocol.observationCount,
      densityBand: cert.protocol.densityBand || null,
      gatePass: cert.pass,
      gateFailures: cert.failures,
      crossProtocolComparableClaims: false,
    },
    methodologyDisclosure: cert.disclosure,
  };
}
