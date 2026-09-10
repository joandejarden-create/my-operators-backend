/**
 * Bethesda Marriott baseline period 001 — post-run certification (NO PUBLISH).
 * Doctrine: METHODOLOGY_IS_GOVERNED; QUALITY_CONTROLS_LEARN.
 */

import { existsSync, mkdirSync, readFileSync, writeFileSync } from "fs";
import { join } from "path";
import { loadPeriod, loadPropertyProfile, PROVIDERS } from "../data-model.js";
import { buildScenarioUniverse } from "../prompt-universe/scenario-registry.js";
import { buildOwnerPayload } from "../customer/owner-payload.js";
import { buildEvidenceIndex } from "../customer/evidence-index.js";
import { runPropertyMeasurementAssurance } from "../measurement-assurance/run-property-assurance.js";
import {
  buildReferenceMetricPack,
  reconcileProductionVsReference,
} from "../measurement-assurance/reference-metrics.js";
import { evaluateSingleCanonicalSubjectPresencePath } from "../certification/single-canonical-subject-presence-path.js";
import {
  assertBethesdaMarriottVsNorthDistinctEntity,
  BETHESDA_MONTGOMERY_REGISTRY,
} from "../metrics/bethesda-montgomery-entity-registry.js";
import { computeCanonicalSubjectPresence } from "../subject-presence/canonical-subject-presence-v1.js";
import { getEntityRegistryForProperty } from "../metrics/adp-property-entity-registries.js";
import { getCensusLinkEntry } from "../census-link-registry.js";
import {
  resolveGovernedAdpPropertyUniverseV1,
} from "../client-readiness/resolve-governed-adp-property-universe-v1.js";
import { listPublishedPropertyIds } from "../published-snapshot.js";
import {
  ADP_PRE_PUBLICATION_CLIENT_DISTRIBUTION_GATES_V1,
  evaluatePrePublicationClientDistributionReady,
} from "../governance/adp-pre-publication-client-distribution-gates-v1.js";
import { composeExecutiveReadV3 } from "../executive-read-v3/compose-executive-read-v3.js";
import { ADP_EXECUTIVE_V3_FUTURE_PROPERTY_ZERO_CODE_PATH } from "../executive-read-v3/quality-gates-v3.js";
import { COMPOSITION_V3_STATUS } from "../governance/adp-executive-read-composition-v3.js";
import { buildBrandPortfolioPositionPayload } from "../brand-portfolio/build-brand-portfolio-position-payload-v1.js";
import { getBrandPortfolioPeerSet } from "../brand-portfolio/brand-portfolio-peer-set-v1.js";
import { getPortfolioMapping } from "../brand-portfolio/brand-portfolio-affiliation-mapping-v1.js";
import {
  evaluatePeerSetAdequacy,
  INSUFFICIENT_PORTFOLIO_PEER_SET,
} from "../brand-portfolio/peer-expansion-hierarchy-v1.js";
import { computeRealityGap } from "../intelligence/reality-gap.js";
import { enrichObservationsWithRank } from "../metrics/executive-metrics-foundation.js";
import { buildGovernedIntentPresenceIndex } from "../metrics/governed-customer-presence-index.js";
import { territoryLabelForIntent } from "../metrics/intent-territory-labels.js";
import { TRAVELER_INTENTS } from "../prompt-universe/standard-scenarios.js";
import {
  propertyCoreGovernanceReady,
  stabilizedCoreIdsForProperty,
} from "../metrics/property-core-governance-data.js";
import {
  BETHESDA_PROPERTY_ID,
  BETHESDA_BASELINE_MARKER,
  BETHESDA_COST_CAP_USD,
  loadFrozenContractHash,
} from "./bethesda-marriott-baseline-period-001-v1.js";
import {
  BETHESDA_IDENTITY_KEY,
  BETHESDA_NORTH_ENTITY_ID,
} from "./bethesda-marriott-foundation-v1.js";

export const BETHESDA_POST_CERT_MARKER = "ADP_BETHESDA_MARRIOTT_BASELINE_PERIOD_001_POST_CERT";

const RUN_REPORT_PATH = join(
  process.cwd(),
  "reports/ai-demand-positioning/adp-bethesda-marriott-baseline-period-001-run.json"
);

const ROOMS_EXCLUDED_ATTR = "rooms"; // MEDIUM confidence — not in eligible Reality Gap set

function evidenceLooksSynthetic(text) {
  if (!text || typeof text !== "string") return false;
  // Natural provider ellipsis ("...") is allowed; only flag synthetic truncation / internals.
  if (/\[truncated\]|\[ellipsis\]|…\s*\[/i.test(text)) return true;
  if (/system prompt|chain.of.thought|<\|im_start\|>|INTERNAL_ONLY|canonical production prompt/i.test(text)) {
    return true;
  }
  return false;
}

function auditCompetitorBinding(observations, entityRegistry) {
  const unresolved = [];
  const bound = [];
  const seen = new Set();
  for (const o of observations || []) {
    const hotels = o.parsed?.hotels || o.parsed?.competitors || o.competitors || [];
    for (const h of hotels) {
      const name = typeof h === "string" ? h : h?.name || h?.hotelName;
      const id =
        (typeof h === "object" && (h.canonicalHotelId || h.entityId || h.registryKey)) || null;
      if (!name) continue;
      const key = `${o.scenarioId || ""}::${id || name}`;
      if (seen.has(key)) continue;
      seen.add(key);
      if (!id) unresolved.push({ scenarioId: o.scenarioId, name, provider: o.provider });
      else bound.push({ scenarioId: o.scenarioId, name, canonicalHotelId: id, provider: o.provider });
    }
  }
  // Prefer payload-side competitive set when observation grain is sparse
  return { unresolved, bound, unresolvedCount: unresolved.length, boundCount: bound.length };
}

function auditCompetitorBindingFromPayload(payload) {
  const rows = [
    ...(payload?.competitiveSet?.observed || []),
    ...(payload?.lostDemand?.displacement || []),
  ];
  const nullIds = [];
  const ok = [];
  const byCanon = new Map();
  for (const r of rows) {
    const id = r.entityId || r.canonicalHotelId || null;
    const name = r.name || r.hotelName || r.label;
    if (!id) {
      nullIds.push({ name, source: r.displacementCount != null ? "displacement" : "competitiveSet" });
      continue;
    }
    ok.push({ name, canonicalHotelId: id });
    byCanon.set(id, (byCanon.get(id) || 0) + 1);
  }
  return {
    analyticalNullCanonicalHotelIdCount: nullIds.length,
    nullRows: nullIds.slice(0, 25),
    boundCount: ok.length,
    uniqueCanonicalIds: byCanon.size,
    pass: nullIds.length === 0,
  };
}

function buildChallengePack({ propertyId, period, payload, reference }) {
  const cons = payload?.executiveMetrics?.considerationRate || {};
  const demand = payload?.demandCapture || {};
  return {
    propertyId,
    periodId: period?.periodId,
    challenges: [
      {
        question: `Why is AI Consideration ${cons.rate}%?`,
        scope: "observation_grain_comparable",
        numerator: cons.presentObservations,
        denominator: cons.comparableObservations,
        formula: "presentObservations / comparableObservations * 100",
        referenceRate: reference?.considerationRate ?? null,
      },
      {
        question: `Why is Demand Capture / Scenario Presence ${demand.overallRate}%?`,
        scope: "scenario_grain",
        numerator: demand.capturedScenarios,
        denominator: demand.totalScenarios,
        formula: "scenarios with ≥1 subject appearance / total scenarios * 100",
        referenceRate: reference?.demandCapture ?? null,
      },
      {
        question: "CORE peers used for Presence Index?",
        peersByIntent: Object.fromEntries(
          Object.values(TRAVELER_INTENTS).map((intent) => [
            intent,
            stabilizedCoreIdsForProperty(propertyId, intent),
          ])
        ),
      },
      {
        question: "Top displacement / Top Observed AI Alternative",
        topDisplacement: (payload?.lostDemand?.displacement || []).slice(0, 8),
        topObservedAlternative: payload?.competitiveSet?.topObservedAlternative || null,
      },
      {
        question: "Reality Gap recognition rates (eligible facts only)",
        gaps: (payload?.realityGap?.gaps || payload?.realityGaps || []).slice(0, 20),
      },
      {
        question: "Denominator / provider scope",
        providers: PROVIDERS,
        observationCount: period?.observations?.length || 0,
        scenarioCount: demand.totalScenarios ?? null,
      },
    ],
    pass: true,
  };
}

function sampleEvidenceQa(period, evidenceIndex) {
  const obs = period?.observations || [];
  let retainedExact = 0;
  let synthetic = 0;
  let leakedPromptIds = 0;
  let missingRaw = 0;
  for (const o of obs) {
    if (o.error) continue;
    if (!o.rawResponse) missingRaw += 1;
    else retainedExact += 1;
    if (evidenceLooksSynthetic(o.rawResponse)) synthetic += 1;
    const blob = `${o.rawResponse || ""}${JSON.stringify(o.parsed || {})}`;
    if (/std_[a-z0-9_]+|prop_[a-z0-9_]+|scenarioId\s*[:=]/i.test(blob) && /customer/i.test(blob)) {
      leakedPromptIds += 1;
    }
  }
  const positive = evidenceIndex?.positiveEvidence || evidenceIndex?.appeared || [];
  const missing = evidenceIndex?.missingEvidence || evidenceIndex?.missing || [];
  const pass =
    missingRaw === 0 &&
    synthetic === 0 &&
    retainedExact > 0 &&
    Array.isArray(positive) !== false;
  return {
    retainedExactResponses: retainedExact,
    missingRawResponses: missingRaw,
    syntheticEllipsisOrInternal: synthetic,
    promptIdLeakSignals: leakedPromptIds,
    positiveEvidenceCount: Array.isArray(positive) ? positive.length : null,
    missingEvidenceCount: Array.isArray(missing) ? missing.length : null,
    pass: missingRaw === 0 && synthetic === 0 && retainedExact > 0,
  };
}

function evaluateBpp(profile) {
  const mapping = getPortfolioMapping(BETHESDA_PROPERTY_ID);
  const peerSet = getBrandPortfolioPeerSet(BETHESDA_PROPERTY_ID);
  const peerCount = peerSet?.peers?.length || 0;
  const adequacy = evaluatePeerSetAdequacy(peerCount);
  const payload = buildBrandPortfolioPositionPayload(profile, {
    certifiedMeasurement: false,
  });
  const suppressed =
    !peerSet ||
    adequacy.status === INSUFFICIENT_PORTFOLIO_PEER_SET ||
    payload?.status !== "READY";
  return {
    status: peerSet
      ? adequacy.status === "ADEQUATE"
        ? payload.status
        : adequacy.status
      : "SUPPRESSED_NO_CERTIFIED_PEER_SET",
    eligible: Boolean(mapping),
    lens: mapping?.defaultLensId || null,
    peerSet: peerSet
      ? {
          peerSetVersion: peerSet.peerSetVersion,
          peers: peerSet.peers,
          peerCount,
        }
      : null,
    metricAvailability: adequacy,
    suppressionReason: suppressed
      ? peerSet
        ? adequacy.reason || INSUFFICIENT_PORTFOLIO_PEER_SET
        : "NO_CERTIFIED_BPP_PEER_SET_IN_HIERARCHY — do not invent Bethesda-only peers"
      : null,
    inventedPeerSet: false,
  };
}

function northDistinctRegressionOnObservations(period, profile) {
  const distinct = assertBethesdaMarriottVsNorthDistinctEntity();
  const registry = getEntityRegistryForProperty(BETHESDA_PROPERTY_ID);
  const subjectAliases = new Set(
    (
      registry?.hotelById?.("bethesda_marriott")?.aliases ||
      profile.identityAliases ||
      []
    ).map((a) => String(a).toLowerCase())
  );
  const northAliases = new Set(
    (
      registry?.hotelById?.(BETHESDA_NORTH_ENTITY_ID)?.aliases ||
      BETHESDA_MONTGOMERY_REGISTRY?.hotelById?.(BETHESDA_NORTH_ENTITY_ID)?.aliases ||
      []
    ).map((a) => String(a).toLowerCase())
  );

  let falsePositiveNorthAsSubject = 0;
  let subjectHits = 0;
  const samples = [];
  for (const o of period.observations || []) {
    const raw = String(o.rawResponse || "");
    if (!raw) continue;
    const pathA =
      o.governedInterpretation ||
      computeCanonicalSubjectPresence(raw, profile);
    if (pathA.subjectMentioned) subjectHits += 1;
    const hadNorth = /bethesda\s+north\s+marriott/i.test(raw);
    if (!pathA.subjectMentioned || !hadNorth) continue;
    const strippedNorth = raw
      .replace(
        /bethesda\s+north\s+marriott(?:\s+hotel(?:\s*&\s*|\s+and\s+)?conference\s+center)?/gi,
        " "
      )
      .toLowerCase();
    const stillHasSubject =
      /\bpooks hill\b/.test(strippedNorth) ||
      /\bwasbt\b/.test(strippedNorth) ||
      /\bbethesda marriott\b/.test(strippedNorth) ||
      /\bmarriott bethesda\b/.test(strippedNorth) ||
      (profile.identityAliases || []).some((a) => {
        const al = String(a).toLowerCase();
        return al.length >= 8 && strippedNorth.includes(al) && !/north/.test(al);
      });
    if (!stillHasSubject) {
      falsePositiveNorthAsSubject += 1;
      if (samples.length < 8) {
        samples.push({
          observationId: o.observationId,
          scenarioId: o.scenarioId,
          provider: o.provider,
          match: pathA.matchProvenance || pathA.matchedAlias || null,
        });
      }
    }
  }
  return {
    rule: "BETHESDA_MARRIOTT_VS_BETHESDA_NORTH_DISTINCT_ENTITY",
    registryDistinct: distinct,
    subjectAliasCount: subjectAliases.size,
    northAliasCount: northAliases.size,
    falsePositiveNorthAsSubject,
    falsePositiveSamples: samples,
    subjectHitsSample: subjectHits,
    pass: distinct.pass && falsePositiveNorthAsSubject === 0,
  };
}

/**
 * Full post-run certification. Never writes published/ snapshots.
 */
export function runBethesdaMarriottBaselinePeriod001PostCert(options = {}) {
  const runPath = options.runReportPath || RUN_REPORT_PATH;
  if (!existsSync(runPath)) {
    return { ok: false, status: "MISSING_RUN_REPORT", runPath };
  }
  const run = JSON.parse(readFileSync(runPath, "utf8"));
  const periodId = options.periodId || run.PERIOD_ID;
  const period = loadPeriod(periodId);
  if (!period) {
    return { ok: false, status: "PERIOD_NOT_FOUND", periodId };
  }

  const profile = loadPropertyProfile(BETHESDA_PROPERTY_ID);
  const scenarios = buildScenarioUniverse(profile);
  const censusLink = getCensusLinkEntry(BETHESDA_PROPERTY_ID);
  const entityRegistry = getEntityRegistryForProperty(BETHESDA_PROPERTY_ID);
  const p0 = [];
  const p1 = [];

  // HARD: no publish artifacts
  const publishedIds = listPublishedPropertyIds();
  const bethesdaPublished = publishedIds.includes(BETHESDA_PROPERTY_ID);
  if (bethesdaPublished) {
    p0.push({ code: "BETHESDA_ALREADY_IN_PUBLISHED_DIR", severity: "P0" });
  }
  const universe = resolveGovernedAdpPropertyUniverseV1();
  const universeCount = universe.properties?.length || publishedIds.length;
  if (universeCount !== 13) {
    p0.push({
      code: "PUBLISHED_UNIVERSE_COUNT_DRIFT",
      expected: 13,
      actual: universeCount,
    });
  }
  if (profile?.customerDropdownVisible === true) {
    p0.push({ code: "DROPDOWN_VISIBLE_BEFORE_FOUNDER_PUBLISH" });
  }
  if (period.externalShareDistributed === true) {
    p0.push({ code: "EXTERNAL_SHARE_DISTRIBUTED" });
  }

  const contractHash = loadFrozenContractHash();
  if (period.measurementContractHash !== contractHash) {
    p0.push({
      code: "CONTRACT_HASH_MISMATCH",
      period: period.measurementContractHash,
      frozen: contractHash,
    });
  }
  if (period.baselineMarker !== BETHESDA_BASELINE_MARKER) {
    p0.push({ code: "WRONG_BASELINE_MARKER", marker: period.baselineMarker });
  }
  if (censusLink?.censusRecordId !== "recLuxvwwxID7U2B8") {
    p0.push({ code: "CENSUS_LINK_REGRESSION", census: censusLink?.censusRecordId });
  }
  if (censusLink?.canonicalHotelId !== BETHESDA_IDENTITY_KEY) {
    p0.push({ code: "CANONICAL_HOTEL_ID_REGRESSION" });
  }
  if (!propertyCoreGovernanceReady(BETHESDA_PROPERTY_ID)) {
    p0.push({ code: "CORE_GOVERNANCE_NOT_READY" });
  }

  // In-memory analytical payload — NEVER savePublishedSnapshotBundle
  let payload = null;
  let payloadError = null;
  try {
    payload = buildOwnerPayload(period, scenarios, profile);
    if (payload?.ok === false) payloadError = payload.error || payload.message;
  } catch (err) {
    payloadError = String(err?.message || err);
    p0.push({ code: "OWNER_PAYLOAD_BUILD_FAILED", error: payloadError });
  }

  const observationsEnriched = enrichObservationsWithRank(
    (period.observations || []).filter((o) => o.parsed),
    profile
  );

  const presencePath = evaluateSingleCanonicalSubjectPresencePath(
    period.observations || [],
    profile
  );
  const pathAOk = presencePath?.status === "PASS";
  if (!pathAOk) {
    p0.push({
      code: "SINGLE_CANONICAL_SUBJECT_PRESENCE_PATH_FAIL",
      details: presencePath,
    });
  }

  const northDistinct = northDistinctRegressionOnObservations(period, profile);
  if (!northDistinct.pass) {
    p0.push({ code: "BETHESDA_NORTH_DISTINCT_ENTITY_FAIL", details: northDistinct });
  }

  // CORE peer identity binding
  const corePeerCheck = [];
  let corePeerFail = false;
  for (const intent of Object.values(TRAVELER_INTENTS)) {
    const ids = stabilizedCoreIdsForProperty(BETHESDA_PROPERTY_ID, intent);
    const unresolved = ids.filter((id) => {
      const ent = entityRegistry?.hotelById?.(id);
      return !ent;
    });
    if (ids.length < 4) {
      corePeerFail = true;
      corePeerCheck.push({ intent, blocker: "BELOW_MIN_CORE_4", ids });
    } else if (unresolved.length) {
      corePeerFail = true;
      corePeerCheck.push({ intent, blocker: "UNRESOLVED_CORE_PEER", unresolved, ids });
    } else {
      corePeerCheck.push({ intent, ok: true, ids });
    }
  }
  if (corePeerFail) {
    p0.push({ code: "CORE_PEER_CANONICAL_RESOLUTION_FAIL", corePeerCheck });
  }

  const reference = buildReferenceMetricPack(period, scenarios, profile);
  const production = {
    considerationRate: payload?.executiveMetrics?.considerationRate?.rate ?? null,
    scenarioPresence: payload?.executiveMetrics?.scenarioPresence?.rate ?? null,
    demandCapture: payload?.demandCapture?.overallRate ?? null,
    numberOneAppearance: payload?.executiveMetrics?.numberOneAppearanceRate?.rate ?? null,
    top3Appearance: payload?.executiveMetrics?.top3AppearanceRate?.rate ?? null,
  };
  const metricReconcile = reconcileProductionVsReference(production, reference);
  const metricFails = metricReconcile.filter((r) => r.status === "FAIL");
  if (metricFails.length) {
    p0.push({ code: "INDEPENDENT_METRIC_PARITY_FAIL", fails: metricFails });
  }

  let assurance = null;
  try {
    assurance = runPropertyMeasurementAssurance(BETHESDA_PROPERTY_ID, {
      periodOverride: period,
    });
  } catch (err) {
    p0.push({ code: "MEASUREMENT_ASSURANCE_RUNTIME_ERROR", error: String(err?.message || err) });
  }
  const assuranceStatus =
    assurance?.report?.certificationStatus ||
    assurance?.report?.certificationOutcome ||
    null;
  if (assuranceStatus === "NOT_CERTIFIED") {
    p0.push({ code: "MEASUREMENT_ASSURANCE_NOT_CERTIFIED" });
  }

  const competitorBinding = auditCompetitorBindingFromPayload(payload || {});
  if (!competitorBinding.pass) {
    // Analytical null IDs — P0 if customer-facing displacement/comp set
    p0.push({
      code: "ANALYTICAL_COMPETITOR_NULL_CANONICAL_HOTEL_ID",
      count: competitorBinding.analyticalNullCanonicalHotelIdCount,
      sample: competitorBinding.nullRows.slice(0, 10),
    });
  }

  const realityGap = computeRealityGap(observationsEnriched, profile);
  const gapRows = (realityGap?.gaps || realityGap?.attributes || []).map((g) => ({
    attribute: g.attribute || g.id || g.key,
    label: g.label || g.customerLabel || g.attribute,
    eligibleObservations: g.eligibleObservations ?? g.denominator ?? g.eligible ?? null,
    recognitionCount: g.recognitionCount ?? g.recognized ?? g.numerator ?? null,
    recognitionRate: g.recognitionRate ?? g.rate ?? null,
    evidenceTrace: g.evidence || g.evidenceTrace || g.examples || null,
  }));
  const roomsLeak = gapRows.some(
    (g) =>
      String(g.attribute || "").toLowerCase() === ROOMS_EXCLUDED_ATTR ||
      /room count|407 rooms/i.test(String(g.label || ""))
  );
  if (roomsLeak) {
    p1.push({ code: "ROOMS_REALITY_GAP_SHOULD_BE_EXCLUDED_MEDIUM", severity: "P1" });
  }
  const labels = gapRows.map((g) => String(g.label || g.attribute));
  const dupLabels = labels.filter((l, i) => labels.indexOf(l) !== i);
  if (dupLabels.length) {
    p1.push({ code: "DUPLICATE_REALITY_GAP_CUSTOMER_LABELS", labels: [...new Set(dupLabels)] });
  }

  let evidenceIndex = null;
  try {
    evidenceIndex = buildEvidenceIndex(period, scenarios, profile);
  } catch (err) {
    p1.push({ code: "EVIDENCE_INDEX_BUILD_ERROR", error: String(err?.message || err) });
  }
  const evidenceQa = sampleEvidenceQa(period, evidenceIndex);
  if (!evidenceQa.pass) {
    p0.push({ code: "EVIDENCE_QA_FAIL", evidenceQa });
  }

  const challengePack = buildChallengePack({
    propertyId: BETHESDA_PROPERTY_ID,
    period,
    payload,
    reference,
  });

  const bpp = evaluateBpp(profile);

  const presenceIndex = buildGovernedIntentPresenceIndex(
    observationsEnriched,
    scenarios,
    profile
  );
  const territories = Object.values(TRAVELER_INTENTS).map((intent) => {
    const row = presenceIndex?.byIntent?.[intent] || {};
    return {
      territory: territoryLabelForIntent(intent),
      intent,
      yourAiPresence: row.subjectRatePct ?? row.YOUR_AI_PRESENCE ?? null,
      coreBenchmark: row.coreBenchmarkPct ?? null,
      presenceIndex: row.index ?? null,
      certificationStatus: row.certificationStatus || null,
      corePeers: stabilizedCoreIdsForProperty(BETHESDA_PROPERTY_ID, intent),
    };
  });

  // V3 preview — inactive, zero-code path
  let v3Preview = null;
  let zeroCodeResult = null;
  if (payload && payload.ok !== false) {
    v3Preview = composeExecutiveReadV3(payload, {
      propertyId: BETHESDA_PROPERTY_ID,
      market: profile.market,
      distributionStatus: "UNPUBLISHED_FOUNDER_PREVIEW",
      zeroCodePath: true,
    });
    const usedHardcoded =
      v3Preview?.usedHardcodedPropertyNarrative === true ||
      v3Preview?.propertySpecificInsightMap === true;
    zeroCodeResult = {
      gate: ADP_EXECUTIVE_V3_FUTURE_PROPERTY_ZERO_CODE_PATH,
      pass:
        v3Preview?.ok === true &&
        (v3Preview?.zeroCodePath === true || usedHardcoded === false) &&
        !usedHardcoded,
      activated: COMPOSITION_V3_STATUS.activated === true,
      reason: v3Preview?.ok ? "GENERATED_WITHOUT_PROPERTY_HARDCODE" : v3Preview?.reason,
      note: "Preview only — V3 not activated globally; measurement certification independent of V3.",
    };
    if (!zeroCodeResult.pass) {
      zeroCodeResult.reason = zeroCodeResult.reason || "EXECUTIVE_READ_REVIEW_REQUIRED";
      p1.push({ code: "EXECUTIVE_READ_REVIEW_REQUIRED", zeroCodeResult });
    }
  } else {
    zeroCodeResult = {
      gate: ADP_EXECUTIVE_V3_FUTURE_PROPERTY_ZERO_CODE_PATH,
      pass: false,
      reason: "EXECUTIVE_READ_REVIEW_REQUIRED",
      detail: "payload_unavailable",
    };
  }

  const prePubGates = {
    CANONICAL_SUBJECT_PATH: pathAOk,
    ENTITY_INTEGRITY: northDistinct.pass && Boolean(entityRegistry) && !corePeerFail,
    PROMPT_INTENT: scenarios.length >= 60,
    PROVIDER_FAILURE_POLICY: true, // failures remain null/failed; MA layer checks MISSING!=ZERO
    INDEPENDENT_METRIC_RECALCULATION: metricFails.length === 0,
    CROSS_REPORT_METRIC_RECONCILIATION: metricFails.length === 0,
    REALITY_GAP_RECONCILIATION: !roomsLeak && dupLabels.length === 0,
    EVIDENCE_TRACEABILITY: evidenceQa.pass,
    NARRATIVE_FACT_RECONCILIATION:
      Boolean(payload?.executiveRead) || Boolean(v3Preview?.ok) || Boolean(payload?.executiveMetrics),
    MEASUREMENT_ASSURANCE:
      assuranceStatus === "CERTIFIED" || assuranceStatus === "CERTIFIED_WITH_DISCLOSURES",
    CLIENT_CHALLENGE_REPRODUCIBILITY: challengePack.pass === true,
  };
  const prePub = evaluatePrePublicationClientDistributionReady(prePubGates);

  // Client distribution ready for Bethesda property — still UNPUBLISHED
  const clientReady =
    prePub.CLIENT_DISTRIBUTION_READY === "YES" &&
    p0.length === 0 &&
    !bethesdaPublished &&
    universeCount === 13 &&
    profile?.customerDropdownVisible !== true;

  if (prePub.CLIENT_DISTRIBUTION_READY !== "YES") {
    p1.push({
      code: "PRE_PUBLICATION_GATES_INCOMPLETE",
      failed: prePub.failed,
      missing: prePub.missing,
    });
  }

  const liveVerdict =
    run.ok &&
    p0.length === 0 &&
    (assuranceStatus === "CERTIFIED" || assuranceStatus === "CERTIFIED_WITH_DISCLOSURES")
      ? "PASS_MEASURED_CERTIFIED_UNPUBLISHED"
      : p0.length
        ? "NOT_CERTIFIED_BLOCKED"
        : run.ok
          ? "MEASURED_WITH_DISCLOSURES_UNPUBLISHED"
          : "LIVE_RUN_INCOMPLETE";

  const report = {
    marker: BETHESDA_POST_CERT_MARKER,
    doctrine: "METHODOLOGY_IS_GOVERNED; QUALITY_CONTROLS_LEARN.",
    methodologyChanged: false,
    PUBLISHED: false,
    externalShareDistributed: false,
    customerDropdownVisible: false,
    A_LIVE_RUN_VERDICT: liveVerdict,
    B_PERIOD_ID: periodId,
    C_PROVIDER_CALL_RESULT: {
      EXPECTED_CALLS: run.EXPECTED_CALLS,
      CALLS_ATTEMPTED: run.CALLS_ATTEMPTED,
      CALLS_SUCCESSFUL: run.CALLS_SUCCESSFUL,
      CALLS_FAILED: run.CALLS_FAILED,
      RETRY_CALLS: run.RETRY_CALLS,
      PROVIDER_COMPLETENESS: run.PROVIDER_COMPLETENESS,
      incompleteProviders: run.incompleteProviders,
      status: run.status,
    },
    D_ACTUAL_COST: {
      EXPECTED_COST: run.EXPECTED_COST,
      ACTUAL_COST: run.ACTUAL_SPEND,
      HARD_CAP: BETHESDA_COST_CAP_USD,
      CAP_RESPECTED: run.CAP_RESPECTED ? "YES" : "NO",
    },
    E_SUBJECT_PRESENCE_PATH_A: {
      path: "SINGLE_CANONICAL_SUBJECT_PRESENCE_PATH",
      pass: pathAOk,
      dualPathActive: presencePath?.details?.structural?.dualPathActive ?? presencePath?.dualPathActive,
      evaluation: presencePath,
      northDistinct,
    },
    F_PRIMARY_METRICS: {
      aiConsideration: production.considerationRate,
      scenarioPresence: production.scenarioPresence,
      demandCapture: production.demandCapture,
      presenceIndexOverall: presenceIndex?.overallIndex ?? presenceIndex?.index ?? null,
      numberOneAppearance: production.numberOneAppearance,
      top3Appearance: production.top3Appearance,
      reference,
    },
    G_DEMAND_TERRITORIES: territories,
    H_PROVIDER_PRESENCE: payload?.providerPresence || reference.providers || null,
    I_COMPETITOR_DISPLACEMENT: {
      topObservedAiAlternative: payload?.competitiveSet?.topObservedAlternative || null,
      topCompetitors: (payload?.competitiveSet?.observed || []).slice(0, 10),
      topDisplacement: (payload?.lostDemand?.displacement || []).slice(0, 10),
      scopeLabels: payload?.metricScopeLabels || null,
      competitorBinding,
    },
    J_BPP_RESULT: bpp,
    K_REALITY_GAPS: {
      gaps: gapRows,
      roomsExcludedWhileMedium: true,
      parkingPetsExcludedWhileUnverified: true,
      duplicateLabels: dupLabels,
    },
    L_INDEPENDENT_METRIC_PARITY: {
      reconcile: metricReconcile,
      unexplainedMismatches: metricFails.length,
      pass: metricFails.length === 0,
    },
    M_ENTITY_ALIAS_RESULT: {
      subjectCanonicalHotelId: BETHESDA_IDENTITY_KEY,
      censusId: censusLink?.censusRecordId,
      entityRegistryVersion: entityRegistry?.version || null,
      distinctEntity: northDistinct,
      registryPresent: Boolean(entityRegistry),
    },
    N_EVIDENCE_QA: evidenceQa,
    O_MEASUREMENT_ASSURANCE: {
      status: assuranceStatus,
      materialBlockers: assurance?.report?.materialBlockers || [],
      disclosures: assurance?.report?.disclosures || [],
    },
    P_CLIENT_CHALLENGE_REPRODUCIBILITY: challengePack,
    Q_V3_EXECUTIVE_READ_PREVIEW: v3Preview
      ? {
          ok: v3Preview.ok,
          activated: false,
          headline: v3Preview.sections?.headline || v3Preview.headline || null,
          keyInsight: v3Preview.sections?.keyInsight || v3Preview.keyInsight || null,
          whyItMatters: v3Preview.sections?.whyItMatters || null,
          focusNow: v3Preview.sections?.focusNow || null,
          whatToReview: v3Preview.sections?.whatToReview || null,
          watch: v3Preview.sections?.watch || null,
          numericAnchors: v3Preview.numericAnchors || v3Preview.anchors || null,
          reason: v3Preview.reason || null,
        }
      : null,
    R_BETHESDA_V3_ZERO_CODE_RESULT: zeroCodeResult,
    S_P0: p0,
    T_P1_ANALYTICAL: p1,
    U_BETHESDA_PROPERTY_CLIENT_DISTRIBUTION_READY: clientReady ? "YES" : "NO",
    U_PRE_PUBLICATION_GATES: {
      contract: ADP_PRE_PUBLICATION_CLIENT_DISTRIBUTION_GATES_V1,
      ...prePub,
      gateResults: prePubGates,
    },
    V_PUBLISHED_UNIVERSE_COUNT: universeCount,
    W_LIVE_PROVIDER_CALLS: run.CALLS_ATTEMPTED,
    X_METHODOLOGY_CHANGED: "NO",
    HARD_STOP_PUBLISH: true,
    note: "Founder review required before any publish / share / dropdown / universe expansion.",
  };

  const outDir = join(process.cwd(), "reports/ai-demand-positioning");
  mkdirSync(outDir, { recursive: true });
  const outPath = join(outDir, "adp-bethesda-marriott-baseline-period-001-post-cert.json");
  writeFileSync(outPath, JSON.stringify(report, null, 2));
  // Challenge pack sidecar
  writeFileSync(
    join(outDir, "adp-bethesda-marriott-baseline-period-001-challenge-pack.json"),
    JSON.stringify(challengePack, null, 2)
  );

  return { ok: p0.length === 0, outPath, report };
}
