/**
 * ADP_COMPLETE_LOCAL_TO_EXTERNAL_REPORT_PARITY_V1
 *
 * Atomic report-version parity: Core + paired BPP + every customer subsection
 * must come from one certified report artifact — never mix historical BPP packs
 * with corrected Core KPIs.
 */

import { createHash } from "crypto";
import { auditProviderPresenceGrain } from "../metrics/adp-provider-presence-grain-lock-v1.js";
import { derivePropertyRealityCoverage } from "../presentation/adp-trends-presentation-v1.js";
import { ADP_CANONICAL_SECTIONS_V1 } from "../presentation/adp-master-report-presentation-v1.js";
import { loadLoyaltyBppStagedPack } from "../brand-portfolio/loyalty-bpp-staged-pack-store-v1.js";
import {
  isEmbeddedCertifiedLoyaltyBpp,
  preferEmbeddedLoyaltyBppForAtomicReport,
  ADP_ATOMIC_REPORT_VERSION_LOADING_V1,
} from "../brand-portfolio/adp-atomic-bpp-pairing-v1.js";

export const ADP_COMPLETE_LOCAL_TO_EXTERNAL_REPORT_PARITY_V1 =
  "ADP_COMPLETE_LOCAL_TO_EXTERNAL_REPORT_PARITY_V1";

export { ADP_ATOMIC_REPORT_VERSION_LOADING_V1 };

export const WATERSTONE_CANONICAL = Object.freeze({
  propertyId: "adp_waterstone_boca_raton",
  stagedVersionId: "staged_mw14o9lq_bb8bd9570c",
  corePeriodId: "adp_period_adp_waterstone_boca_raton_20260902231159_77334c",
  bppPeriodId: "adp_period_adp_waterstone_boca_raton_20261020200247_349108",
  bppLoyaltyVersionId: "loy_bpp_mvhttl2i_87e189faf8",
  bppArtifactHash:
    "f0a22942dd8cc33846362927f0854919c7a025f099b766e84be7bdbbb77faf83",
  methodologyId: "ADP_AI_CONSIDERATION_INDEPENDENT_UNPROMPTED_V1",
  consideration: { rate: 53.5, present: 137, comparable: 256 },
  scenarioPresence: { rate: 76.6, captured: 49, eligible: 64 },
  providers: {
    gemini: { mentioned: 44, comparable: 64 },
    perplexity: { mentioned: 33, comparable: 64 },
    claude: { mentioned: 32, comparable: 64 },
    openai: { mentioned: 28, comparable: 64 },
  },
  realityCoverage: 24.1,
  realityGap: 69,
  competitiveRank: 2,
  bpp: {
    lens: "Hilton Honors",
    standard: "LOYALTY_STANDARD_28",
    scenarios: 28,
    observations: 112,
    portfolioPresence: "2.7%",
    portfolioPresenceMeta: "3 of 112",
    portfolioRank: "#5 of 6",
    numberOne: "0.0%",
    numberOneMeta: "0 of 28",
    top3: "7.1%",
    top3Meta: "2 of 28",
    positiveEvidence: 3,
    missingEvidence: 40,
    portfolioDisplacement: 25,
    peerSetId: "bps_adp_waterstone_boca_raton_hilton_honors_certified_v1",
  },
});

function kpiMap(bpp) {
  return Object.fromEntries(
    (bpp?.kpis || []).map((k) => [k.id, { value: k.value, meta: k.meta }])
  );
}

export function extractReportFingerprint(payload, label = "report") {
  if (!payload || typeof payload !== "object") {
    return { label, error: "missing_payload" };
  }
  const em = payload.executiveMetrics || {};
  const bpp = payload.brandPortfolioPosition || {};
  const grain = auditProviderPresenceGrain(payload);
  const overall = payload.competitiveRankingByTerritory?.byTerritory?.overall;
  const subject = (overall?.displayRows || []).find((r) => r?.isSubject);
  const providers = Object.fromEntries(
    (payload.evidence?.providers || []).map((p) => [
      p.provider,
      {
        mentioned: p.mentioned,
        comparable: p.comparable ?? p.total,
        presence: p.presence,
      },
    ])
  );
  return {
    label,
    reportVersion:
      payload._adpStagedVersionId ||
      payload.sourceStagedVersionId ||
      payload._adpReportVersion ||
      null,
    corePeriodId: payload.period?.periodId || null,
    methodologyId: em.considerationRate?.methodologyId || null,
    consideration: em.considerationRate || null,
    scenarioPresence: em.scenarioPresence || null,
    providers,
    grainPass: grain.pass === true,
    mentionSum: grain.mentionSum,
    comparableSum: grain.comparableSum,
    has78: grain.has78 === true,
    realityCoverage: derivePropertyRealityCoverage(payload.realityGap || {}),
    realityGap: payload.realityGap?.gapScore ?? null,
    competitiveRank: subject?.displayRank ?? subject?.observedRank ?? null,
    competitiveAppearances: subject?.appearances ?? null,
    competitiveComparableN: overall?.comparableN ?? null,
    displacementScenarios:
      em.competitorPresentScenarios?.scenarioCount ?? null,
    actionsCount: Array.isArray(payload.actions)
      ? payload.actions.length
      : payload.actions?.items?.length ?? null,
    bpp: {
      lens: bpp.lens?.label || bpp.lens?.lensId || null,
      periodId: bpp.measurement?.periodId || bpp.periodId || null,
      scenarios: bpp.measurement?.scenarioCount ?? null,
      observations: bpp.measurement?.observationCount ?? null,
      measurementFamily: bpp.measurement?.measurementFamily || null,
      bppVersion: bpp._bppLoyaltyStagedVersionId || null,
      bppHash: bpp._bppLoyaltyArtifactHash || null,
      packSource: bpp._bppPackSource || null,
      selectionPolicy: bpp._bppSelectionPolicy || null,
      atomicPairing: bpp._bppAtomicPairing === true,
      customerPublished: bpp.customerPublished ?? bpp.measurement?.customerPublished,
      status: bpp.status || null,
      kpis: kpiMap(bpp),
      ranking: (bpp.ranking?.rows || []).map((r) => ({
        name: r.name,
        rank: r.rank ?? r.displayRank,
        isSubject: !!r.isSubject,
      })),
      positive: bpp.evidence?.positive?.length ?? bpp.evidence?.counts?.positive ?? null,
      missing: bpp.evidence?.missing?.length ?? bpp.evidence?.counts?.missing ?? null,
      displacement:
        bpp.evidence?.displacement?.length ??
        bpp.evidence?.counts?.displacement ??
        null,
    },
    atomicSnapshot: payload._adpAtomicCertification || null,
    bppPublicationVersion: payload._bppPublicationVersion || null,
  };
}

export function verifyWaterstoneCoreFingerprint(fp) {
  const exp = WATERSTONE_CANONICAL;
  const failures = [];
  if (fp.consideration?.rate !== exp.consideration.rate) {
    failures.push("CORE_CONSIDERATION_RATE");
  }
  if (
    fp.consideration?.presentObservations !== exp.consideration.present ||
    fp.consideration?.comparableObservations !== exp.consideration.comparable
  ) {
    failures.push("CORE_CONSIDERATION_COUNTS");
  }
  if (fp.scenarioPresence?.rate !== exp.scenarioPresence.rate) {
    failures.push("CORE_SCENARIO_PRESENCE_RATE");
  }
  if (
    fp.scenarioPresence?.capturedScenarios !== exp.scenarioPresence.captured ||
    fp.scenarioPresence?.eligibleScenarios !== exp.scenarioPresence.eligible
  ) {
    failures.push("CORE_SCENARIO_PRESENCE_COUNTS");
  }
  for (const [prov, expRow] of Object.entries(exp.providers)) {
    const got = fp.providers?.[prov];
    if (got?.mentioned !== expRow.mentioned || got?.comparable !== expRow.comparable) {
      failures.push(`PROVIDER_${prov.toUpperCase()}`);
    }
  }
  if (fp.realityCoverage !== exp.realityCoverage) failures.push("REALITY_COVERAGE");
  if (fp.realityGap !== exp.realityGap) failures.push("REALITY_GAP");
  if (Number(fp.competitiveRank) !== exp.competitiveRank) {
    failures.push("COMPETITIVE_RANK");
  }
  if (fp.grainPass !== true || fp.has78) failures.push("PROVIDER_GRAIN");
  if (fp.methodologyId !== exp.methodologyId) failures.push("METHODOLOGY");
  return { ok: failures.length === 0, failures };
}

export function verifyWaterstoneBppFingerprint(fp) {
  const exp = WATERSTONE_CANONICAL.bpp;
  const failures = [];
  const bpp = fp.bpp || {};
  if (bpp.lens !== exp.lens) failures.push("BPP_LENS");
  if (bpp.scenarios !== exp.scenarios) failures.push("BPP_SCENARIOS");
  if (bpp.observations !== exp.observations) failures.push("BPP_OBSERVATIONS");
  if (bpp.kpis?.portfolioAiPresence?.value !== exp.portfolioPresence) {
    failures.push("BPP_PORTFOLIO_PRESENCE");
  }
  if (!String(bpp.kpis?.portfolioAiPresence?.meta || "").includes(exp.portfolioPresenceMeta)) {
    failures.push("BPP_PORTFOLIO_PRESENCE_META");
  }
  if (bpp.kpis?.portfolioRank?.value !== exp.portfolioRank) {
    failures.push("BPP_PORTFOLIO_RANK");
  }
  if (bpp.kpis?.numberOneAppearance?.value !== exp.numberOne) {
    failures.push("BPP_NUMBER_ONE");
  }
  if (bpp.kpis?.top3Appearance?.value !== exp.top3) {
    failures.push("BPP_TOP3");
  }
  if (bpp.positive !== exp.positiveEvidence) failures.push("BPP_POSITIVE_EVIDENCE");
  if (bpp.missing !== exp.missingEvidence) failures.push("BPP_MISSING_EVIDENCE");
  if (bpp.displacement !== exp.portfolioDisplacement) {
    failures.push("BPP_PORTFOLIO_DISPLACEMENT");
  }
  if (bpp.ranking?.length !== 6) failures.push("BPP_RANKING_SIZE");
  const subject = (bpp.ranking || []).find((r) => r.isSubject);
  if (Number(subject?.rank) !== 5) failures.push("BPP_SUBJECT_RANK");
  if (bpp.bppVersion && bpp.bppVersion !== WATERSTONE_CANONICAL.bppLoyaltyVersionId) {
    failures.push("BPP_VERSION_MISMATCH");
  }
  // Historical contaminated pack signals
  if (bpp.observations === 40 || bpp.scenarios === 10) {
    failures.push("BPP_HISTORICAL_PEER_REUSE_PACK");
  }
  if (String(fp.bppPublicationVersion || "").includes("20260902-p2") && bpp.observations === 40) {
    failures.push("BPP_GLOBAL_PACK_OVERLAY");
  }
  return { ok: failures.length === 0, failures };
}

export function verifyLoyaltyBppPackIndependent(propertyId = WATERSTONE_CANONICAL.propertyId) {
  const pack = loadLoyaltyBppStagedPack(propertyId);
  const failures = [];
  if (!pack?.ok || !pack.payload) failures.push("PACK_MISSING");
  const cert = pack?.certification || {};
  if (cert.status !== "STAGED_LOYALTY_BPP_READY") failures.push("PACK_NOT_READY");
  if (cert.protocolClassification !== "LOYALTY_STANDARD_28") {
    failures.push("PACK_NOT_STANDARD_28");
  }
  if (cert.readyRank !== true) failures.push("PACK_RANK_NOT_READY");
  if (pack?.versionId !== WATERSTONE_CANONICAL.bppLoyaltyVersionId) {
    failures.push("PACK_VERSION");
  }
  if (pack?.artifactHash !== WATERSTONE_CANONICAL.bppArtifactHash) {
    failures.push("PACK_HASH");
  }
  const fp = extractReportFingerprint({
    brandPortfolioPosition: pack?.payload,
  });
  const kpi = verifyWaterstoneBppFingerprint(fp);
  failures.push(...kpi.failures);
  const presenceRaw = pack?.payload?.kpis?.find((k) => k.id === "portfolioAiPresence")?.valueRaw;
  if (Math.abs(Number(presenceRaw) - 3 / 112) > 1e-12) {
    failures.push("PACK_PRESENCE_ARITHMETIC");
  }
  return {
    ok: failures.length === 0,
    failures,
    versionId: pack?.versionId || null,
    artifactHash: pack?.artifactHash || null,
    peerSetId: pack?.payload?.peerSet?.peerSetId || cert.peerSetId || null,
    lens: pack?.payload?.lens?.label || null,
    hiltonEligible: pack?.payload?.lens?.lensId === "hilton_honors",
    periodId: pack?.payload?.measurement?.periodId || pack?.periodId || null,
  };
}

export function diffFingerprints(localFp, otherFp) {
  const diffs = [];
  function walk(a, b, path = "") {
    if (a === b) return;
    if (
      a == null ||
      b == null ||
      typeof a !== "object" ||
      typeof b !== "object"
    ) {
      diffs.push({ path: path || "(root)", local: a, other: b });
      return;
    }
    const keys = new Set([...Object.keys(a), ...Object.keys(b)]);
    for (const k of keys) {
      if (k === "label") continue;
      walk(a[k], b[k], path ? `${path}.${k}` : k);
    }
  }
  walk(localFp, otherFp);
  return diffs;
}

/**
 * Simulate published API BPP selection against an embedded Core payload
 * (mirrors api published-path atomic preference without importing the API module).
 */
export function simulatePublishedAtomicBppSelection(corePayload) {
  return preferEmbeddedLoyaltyBppForAtomicReport(corePayload?.brandPortfolioPosition, {
    reportVersion: "published",
  });
}

function isGovernedBppNotApplicable(bpp) {
  if (!bpp || typeof bpp !== "object") return false;
  return (
    bpp.status === "BPP_NOT_APPLICABLE" ||
    bpp.customerState === "BPP_NOT_APPLICABLE" ||
    bpp.bppCustomerState === "BPP_NOT_APPLICABLE" ||
    bpp.measuredRank === "BPP_NOT_APPLICABLE" ||
    bpp.disclosureReason === "LENS_NOT_APPLICABLE" ||
    (bpp.renderMode === "STATUS_ONLY" &&
      /not applicable/i.test(String(bpp.headline || bpp.body || "")))
  );
}

export function assessAtomicBppPairingGate({ publishedPayload, stagedPayload }) {
  const failures = [];
  const embedded = publishedPayload?.brandPortfolioPosition;
  const stagedBppNa = stagedPayload?.brandPortfolioPosition;
  // Core-only / greenfield hotels may embed governed BPP_NOT_APPLICABLE (no Loyalty
  // pack). That is a valid atomic Core↔BPP pairing — not a missing Loyalty embed.
  if (isGovernedBppNotApplicable(embedded) || isGovernedBppNotApplicable(stagedBppNa)) {
    return {
      gate: ADP_ATOMIC_REPORT_VERSION_LOADING_V1,
      ok: true,
      failures: [],
      selectedPolicy: "CORE_WITH_GOVERNED_BPP_NOT_APPLICABLE",
      selectedBppVersion: null,
      bppNotApplicable: true,
    };
  }
  if (!isEmbeddedCertifiedLoyaltyBpp(embedded)) {
    failures.push("PUBLISHED_CORE_MISSING_EMBEDDED_LOYALTY_BPP");
  }
  const selected = simulatePublishedAtomicBppSelection(publishedPayload);
  if (
    !selected ||
    selected._bppSelectionPolicy !==
      "PUBLISHED_ATOMIC_EMBEDDED_LOYALTY_BPP_PAIRED_WITH_CORE"
  ) {
    failures.push("PUBLISHED_PATH_NOT_USING_ATOMIC_EMBEDDED_BPP");
  }
  const stagedBpp = stagedPayload?.brandPortfolioPosition;
  if (
    stagedBpp?._bppLoyaltyStagedVersionId &&
    selected?._bppLoyaltyStagedVersionId &&
    stagedBpp._bppLoyaltyStagedVersionId !== selected._bppLoyaltyStagedVersionId
  ) {
    failures.push("STAGED_PUBLISHED_BPP_VERSION_DRIFT");
  }
  if (
    selected?.measurement?.observationCount === 40 ||
    selected?.measurement?.scenarioCount === 10
  ) {
    failures.push("SELECTED_BPP_IS_HISTORICAL_40_OBS_PACK");
  }
  return {
    gate: ADP_ATOMIC_REPORT_VERSION_LOADING_V1,
    ok: failures.length === 0,
    failures,
    selectedPolicy: selected?._bppSelectionPolicy || null,
    selectedBppVersion: selected?._bppLoyaltyStagedVersionId || null,
  };
}

export function listCanonicalSubsections() {
  return ADP_CANONICAL_SECTIONS_V1.map((s) => ({
    id: s.id,
    label: s.label,
    position: s.position,
    required: s.required !== false,
  }));
}

export function hashFingerprint(fp) {
  return createHash("sha256").update(JSON.stringify(fp)).digest("hex");
}
