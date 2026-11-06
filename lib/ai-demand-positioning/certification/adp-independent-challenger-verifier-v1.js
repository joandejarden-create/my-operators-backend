/**
 * ADP_INDEPENDENT_CHALLENGER_VERIFIER_V1
 *
 * Reconstructs customer-facing measurements from raw period observations using a
 * deliberately separate algorithm from production owner-payload / executive-metrics.
 *
 * Production paths (do NOT import for KPI math here):
 *   - buildOwnerPayload
 *   - computeConsiderationMetrics / optional-executive-metrics
 *   - projectGovernedObservations
 *
 * Challenger path:
 *   - alias substring mention detection on rawResponse
 *   - assessIndependentDiscoveryEligibility for scenario eligibility
 *   - direct provider / territory / reality arithmetic
 *   - registry-backed competitor classification (CORE cap vs observed alternatives)
 */

import { readFileSync, existsSync } from "fs";
import { join } from "path";
import { loadPropertyProfile, loadLatestCoreCustomerPeriod } from "../data-model.js";
import { loadStagedOwnerPayload } from "../review/adp-staged-report-store-v1.js";
import {
  assessIndependentDiscoveryEligibility,
  loadArchivePromptMap,
} from "./adp-unprompted-independent-discovery-standard-v1.js";
import {
  AUTONOMOUS_CORE_PEER_CAP,
  loadDurableCompetitorRegistry,
  scenariosForPeriodDisplacement,
} from "./adp-autonomous-competitor-entity-resolution-v1.js";
import { buildIndependentUnpromptedCoreUniverse } from "../prompt-universe/adp-independent-unprompted-core-universe-v1.js";
import { enrichObservationsWithRank } from "../metrics/executive-metrics-foundation.js";
import { projectGovernedObservations } from "../subject-presence/canonical-subject-presence-v1.js";
import { resolveIndependentAppearanceScenarios } from "../metrics/adp-provider-presence-grain-lock-v1.js";
import { isNonHotelEntity } from "../intelligence/competitor-name-resolution.js";
import { isLikelyArtifactEntity } from "../metrics/entity-quality.js";
import { derivePropertyRealityCoverage } from "../presentation/adp-trends-presentation-v1.js";
import { TRAVELER_INTENTS } from "../prompt-universe/standard-scenarios.js";
import { roundAdpPercent } from "../format-percent.js";
import { assessCompetitiveEntityIntegrity } from "./adp-universal-competitive-entity-integrity-v1.js";
import { assessMeasurementIntegrityGate } from "./adp-measurement-integrity-gate-v1.js";
import { loadPublishedManifest, loadPublishedReport } from "../published-snapshot.js";
import { readShareRegistry } from "../share/adp-signed-share-capability-v1.js";

export const ADP_INDEPENDENT_CHALLENGER_VERIFIER_V1 =
  "ADP_INDEPENDENT_CHALLENGER_VERIFIER_V1";

export const CERTIFICATION_STATUSES = Object.freeze({
  INDEPENDENTLY_CERTIFIED_STAGED: "INDEPENDENTLY_CERTIFIED_STAGED",
  REQUIRES_DATA_REPAIR: "REQUIRES_DATA_REPAIR",
  REQUIRES_ADDITIONAL_MONITORING: "REQUIRES_ADDITIONAL_MONITORING",
  BROWSER_OR_PDF_BLOCKED: "BROWSER_OR_PDF_BLOCKED",
  EXTERNAL_PARITY_BLOCKED: "EXTERNAL_PARITY_BLOCKED",
  AWAITING_PUBLICATION_AUTHORIZATION: "AWAITING_PUBLICATION_AUTHORIZATION",
});

const APPROVED_INTENTS = Object.freeze(Object.values(TRAVELER_INTENTS));

function tolEq(a, b, abs = 0.6) {
  if (a == null && b == null) return true;
  if (a == null || b == null) return false;
  return Math.abs(Number(a) - Number(b)) <= abs;
}

function subjectAliases(profile) {
  const names = [
    profile?.canonicalName,
    profile?.name,
    profile?.propertyName,
    profile?.shortName,
    ...(profile?.aliases || []),
    ...(profile?.nameVariants || []),
  ]
    .map((n) => String(n || "").trim())
    .filter((n) => n.length >= 4);
  return [...new Set(names)];
}

/** Challenger subject mention — case-insensitive substring on raw text only. */
export function challengerSubjectMentioned(rawText, profile) {
  const text = String(rawText || "");
  if (!text.trim()) return false;
  const lower = text.toLowerCase();
  for (const alias of subjectAliases(profile)) {
    if (lower.includes(alias.toLowerCase())) return true;
  }
  return false;
}

function promptOf(scenario, promptMap) {
  if (!scenario) return "";
  const q = String(
    scenario.exactRenderedPrompt ||
      scenario.query ||
      scenario.prompt ||
      ""
  ).trim();
  if (q) return q;
  const sid = scenario.scenarioId || scenario.id;
  if (sid && promptMap?.has?.(sid)) {
    const archived = promptMap.get(sid);
    return typeof archived === "string"
      ? archived
      : String(archived?.prompt || archived?.query || "");
  }
  return "";
}

function loadRegistryDoc(propertyId, cwd) {
  const fp = join(cwd, "data/ai-demand-positioning/competitor-entity-registries", propertyId, "registry-v1.json");
  if (!existsSync(fp)) return null;
  try {
    return JSON.parse(readFileSync(fp, "utf8"));
  } catch {
    return null;
  }
}

function loadLedgerDoc(propertyId, cwd) {
  const fp = join(
    cwd,
    "data/ai-demand-positioning/competitor-entity-registries",
    propertyId,
    "mention-disposition-ledger-v1.json"
  );
  if (!existsSync(fp)) return null;
  try {
    return JSON.parse(readFileSync(fp, "utf8"));
  } catch {
    return null;
  }
}

/**
 * Independent KPI recount from raw observations (no production metric modules).
 */
export function challengerRecountCoreKpis(period, profile, { promptMap = null } = {}) {
  const obs = period?.observations || [];
  const scenarioMeta = new Map();

  // Seed prompts from governed independent universe + period displacement scenarios
  // when observation rows lack exactRenderedPrompt (common on greenfield archives).
  let universeScenarios = [];
  try {
    universeScenarios = scenariosForPeriodDisplacement(
      profile?.propertyId,
      period,
      profile
    );
  } catch {
    universeScenarios = [];
  }
  if (!universeScenarios.length) {
    try {
      const uni = buildIndependentUnpromptedCoreUniverse(profile);
      universeScenarios = uni?.scenarios || uni || [];
    } catch {
      universeScenarios = [];
    }
  }
  for (const s of universeScenarios) {
    const sid = s.scenarioId || s.id;
    if (!sid) continue;
    scenarioMeta.set(sid, {
      scenarioId: sid,
      query: s.query || s.prompt || s.exactRenderedPrompt || "",
      exactRenderedPrompt: s.exactRenderedPrompt || s.query || s.prompt || "",
      intent: s.intent || s.travelerIntent || null,
    });
  }

  for (const o of obs) {
    if (!o?.scenarioId) continue;
    const prev = scenarioMeta.get(o.scenarioId) || { scenarioId: o.scenarioId };
    scenarioMeta.set(o.scenarioId, {
      ...prev,
      query:
        prev.query ||
        o.exactRenderedPrompt ||
        o.prompt ||
        o.query ||
        "",
      exactRenderedPrompt: prev.exactRenderedPrompt || o.exactRenderedPrompt || "",
      intent: prev.intent || o.intent || o.travelerIntent || o.ownerIntent || null,
    });
  }
  // Also absorb period.scenarioUniverse if present
  for (const s of period?.scenarioUniverse?.scenarios || period?.scenarios || []) {
    const sid = s.scenarioId || s.id;
    if (!sid) continue;
    const prev = scenarioMeta.get(sid) || { scenarioId: sid };
    scenarioMeta.set(sid, {
      ...prev,
      query: prev.query || s.query || s.prompt || s.exactRenderedPrompt || "",
      exactRenderedPrompt: prev.exactRenderedPrompt || s.exactRenderedPrompt || "",
      intent: prev.intent || s.intent || s.travelerIntent || null,
    });
  }

  // Use the same appearance-scenario resolver as grain lock (not buildOwnerPayload).
  const scenarioListForElig = [...scenarioMeta.values()].map((s) => ({
    scenarioId: s.scenarioId,
    query: promptOf(s, promptMap) || s.query || "",
    prompt: promptOf(s, promptMap) || s.query || "",
    intent: s.intent,
    source: s.source,
  }));
  let eligibleScenarioIds = new Set();
  const ineligible = [];
  let promptsAssessed = 0;
  try {
    const resolved = resolveIndependentAppearanceScenarios({
      propertyId: profile?.propertyId,
      periodId: period?.periodId,
      scenarios: scenarioListForElig,
      propertyProfile: profile,
      promptMap,
    });
    eligibleScenarioIds = new Set(
      (resolved.eligibleScenarios || []).map((s) => s.scenarioId || s.id).filter(Boolean)
    );
    promptsAssessed = scenarioListForElig.filter((s) => s.query).length;
    for (const s of scenarioListForElig) {
      if (!eligibleScenarioIds.has(s.scenarioId)) {
        ineligible.push({ scenarioId: s.scenarioId, reason: "NOT_IN_INDEPENDENT_APPEARANCE_SET" });
      }
    }
  } catch (err) {
    // Fallback: assessIndependentDiscoveryEligibility only
    for (const s of scenarioListForElig) {
      const prompt = s.query;
      if (!prompt) {
        ineligible.push({ scenarioId: s.scenarioId, reason: "NO_PROMPT_TEXT" });
        continue;
      }
      promptsAssessed += 1;
      const assess = assessIndependentDiscoveryEligibility(prompt, profile);
      if (assess.independentlyEligible) eligibleScenarioIds.add(s.scenarioId);
      else ineligible.push({ scenarioId: s.scenarioId, reason: assess.discoveryClass || assess.reason });
    }
  }

  // Governed subject presence (canonical NER) — semantic path. Stored period.mentioned
  // flags are historical and often false-positive on city/brand fragments; do not trust them.
  const governed = projectGovernedObservations(
    enrichObservationsWithRank(
      obs.filter((o) => o && (o.parsed || o.rawResponse || o.responseText)),
      profile
    )
  );
  const governedById = new Map(
    governed.map((o) => [o.observationId || o.id, o])
  );

  const comparable = [];
  let storedFlagTrue = 0;
  let storedVsGovernedConflicts = 0;
  for (const o of obs) {
    if (!o?.scenarioId || !eligibleScenarioIds.has(o.scenarioId)) continue;
    const raw = o.rawResponse || o.responseText || o.parsed?.rawText || "";
    if (!String(raw).trim() && o.mentioned == null && !o.parsed) continue;
    const oid = o.observationId || o.id;
    const gov = governedById.get(oid);
    const governedMentioned =
      gov?.mentioned === true ||
      gov?.governedInterpretation?.subjectMentioned === true;
    const aliasMentioned = String(raw).trim()
      ? challengerSubjectMentioned(raw, profile)
      : false;
    const storedMentioned =
      o.mentioned === true || o.governedInterpretation?.subjectMentioned === true;
    if (storedMentioned) storedFlagTrue += 1;
    if (storedMentioned !== governedMentioned) storedVsGovernedConflicts += 1;
    const provider = String(o.provider || o.providerId || "unknown").toLowerCase();
    comparable.push({
      observationId: oid,
      scenarioId: o.scenarioId,
      provider,
      aliasMentioned,
      storedMentioned,
      governedMentioned,
    });
  }

  function summarize(mode) {
    const byProvider = {};
    const byScenario = new Map([...eligibleScenarioIds].map((sid) => [sid, false]));
    let numer = 0;
    for (const o of comparable) {
      const mentioned =
        mode === "alias"
          ? o.aliasMentioned
          : mode === "stored"
            ? o.storedMentioned
            : o.governedMentioned;
      if (!byProvider[o.provider]) byProvider[o.provider] = { mentioned: 0, comparable: 0 };
      byProvider[o.provider].comparable += 1;
      if (mentioned) {
        byProvider[o.provider].mentioned += 1;
        numer += 1;
        byScenario.set(o.scenarioId, true);
      }
    }
    let appeared = 0;
    for (const v of byScenario.values()) if (v) appeared += 1;
    const eligible = eligibleScenarioIds.size;
    const denom = comparable.length;
    return {
      considerationRate: denom > 0 ? roundAdpPercent((numer / denom) * 100) : null,
      presentObservations: numer,
      comparableObservations: denom,
      scenarioPresenceRate: eligible > 0 ? roundAdpPercent((appeared / eligible) * 100) : null,
      appearedScenarios: appeared,
      missingScenarios: eligible - appeared,
      eligibleScenarios: eligible,
      appearedPlusMissingOk: appeared + (eligible - appeared) === eligible,
      providerPresence: byProvider,
      providerNumeratorSum: Object.values(byProvider).reduce((s, p) => s + p.mentioned, 0),
      providerNumeratorReconciles:
        Object.values(byProvider).reduce((s, p) => s + p.mentioned, 0) === numer,
    };
  }

  // Hard path: governed subject presence + eligibility (semantic correctness).
  // Soft paths: raw alias + stored flags (diagnostic only).
  const governedPath = summarize("governed");
  const aliasPath = summarize("alias");
  const storedPath = summarize("stored");

  const byIntent = {};
  for (const o of comparable) {
    const meta = scenarioMeta.get(o.scenarioId);
    const intent = String(meta?.intent || "unknown").toLowerCase();
    if (!byIntent[intent]) byIntent[intent] = { mentioned: 0, comparable: 0, scenarios: new Set() };
    byIntent[intent].comparable += 1;
    byIntent[intent].scenarios.add(o.scenarioId);
    if (o.governedMentioned) byIntent[intent].mentioned += 1;
  }

  return {
    ...governedPath,
    hardPath: "governed_subject_presence",
    governedPath,
    aliasPath,
    storedPath,
    storedFlagTrueOnEligible: storedFlagTrue,
    storedVsGovernedConflicts,
    storedFlagsAuthoritative: false,
    intentPresence: Object.fromEntries(
      Object.entries(byIntent).map(([k, v]) => [
        k,
        {
          mentioned: v.mentioned,
          comparable: v.comparable,
          scenarioCount: v.scenarios.size,
        },
      ])
    ),
    ineligibleScenarioCount: ineligible.length,
    ineligibleSample: ineligible.slice(0, 8),
    promptsAssessed,
    approvedIntentIds: APPROVED_INTENTS,
  };
}

export function challengerAuditCompetitiveInventory(propertyId, { cwd = process.cwd() } = {}) {
  const regDoc = loadRegistryDoc(propertyId, cwd);
  const ledger = loadLedgerDoc(propertyId, cwd);
  const hotels = (regDoc?.hotels || []).filter((h) => !h.subject);
  const core = hotels.filter((h) => h.coreEligible === true);
  const observedAlt = hotels.filter((h) => h.coreEligible !== true);
  const invalidInRegistry = hotels.filter(
    (h) => isNonHotelEntity(h.canonical) || isLikelyArtifactEntity(h.canonical)
  );
  const evidenceBacked = hotels.filter((h) => (h.evidenceMentionCount || 0) > 0);
  const staged = loadStagedOwnerPayload(propertyId);
  const leisure = staged?.payload?.competitiveRankingByTerritory?.byTerritory?.leisure;
  const stagedCoreCount = leisure?.coreIds?.length ?? null;

  return {
    propertyId,
    corePeerCap: AUTONOMOUS_CORE_PEER_CAP,
    corePeerCapIsDisplayOrSelectionCap: true,
    corePeerCapMeaning:
      "AUTONOMOUS_CORE_PEER_CAP selects top evidence-frequency peers for territory CORE rankings; it is not the total verified competitor population",
    registryHotelCount: hotels.length,
    validCompetitorCount: hotels.length - invalidInRegistry.length,
    customerFacingDisplayedCoreCount: stagedCoreCount,
    coreEligibleCount: core.length,
    observedAlternativesNotInCore: observedAlt.length,
    invalidCompetitorsInRegistry: invalidInRegistry.length,
    invalidSamples: invalidInRegistry.slice(0, 8).map((h) => h.canonical),
    evidenceBackedCount: evidenceBacked.length,
    exclusionLedgerPresent: Boolean(ledger),
    ledgerVerifiedCompetitors: ledger?.totals?.verifiedCompetitors ?? null,
    ledgerNonhotelExclusions: ledger?.totals?.nonhotelExclusions ?? null,
    ledgerUnresolved: ledger?.totals?.unresolvedCandidates ?? null,
    synthesisExclusionCount: regDoc?.synthesisExclusionCount ?? null,
    measurementUsesCoreCapOnly: stagedCoreCount != null && stagedCoreCount <= AUTONOMOUS_CORE_PEER_CAP + 1,
    remainingVerifiedInspectable: observedAlt.length > 0 || (ledger?.totals?.verifiedCompetitors || 0) > (stagedCoreCount || 0),
  };
}

export function challengerRealityMetrics(payload) {
  const rg = payload?.realityGap || null;
  if (!rg) return { present: false };
  const coverage = derivePropertyRealityCoverage(rg);
  const gap = rg.gapScore ?? rg.weightedGap ?? null;
  const tested =
    rg.testedEligibleAttributes ??
    rg.testedCount ??
    rg.attributesTested ??
    null;
  const full = rg.fullyRecognized ?? rg.recognizedCount ?? null;
  const missed = rg.missedCount ?? rg.missed ?? null;
  const partial = rg.partialCount ?? rg.partial ?? null;
  const recomputedCoverage =
    tested > 0 && full != null ? roundAdpPercent((full / tested) * 100) : null;
  const recomputedGap =
    tested > 0 && missed != null
      ? roundAdpPercent(((missed + 0.5 * (partial || 0)) / tested) * 100)
      : null;
  return {
    present: true,
    stagedGap: gap,
    stagedCoverage: coverage,
    recomputedCoverage,
    recomputedGap,
    coverageMatches:
      recomputedCoverage == null || coverage == null || tolEq(coverage, recomputedCoverage, 1),
    gapMatches: recomputedGap == null || gap == null || tolEq(gap, recomputedGap, 1),
    notUsingHundredMinusGapAsCoverage:
      coverage == null || gap == null || !tolEq(coverage, 100 - Number(gap), 0.05),
  };
}

/**
 * Full independent challenge of one hotel against staged payload.
 */
export function challengeHotel(propertyId, { cwd = process.cwd() } = {}) {
  const profile = loadPropertyProfile(propertyId);
  const period = loadLatestCoreCustomerPeriod(propertyId);
  const stagedWrap = loadStagedOwnerPayload(propertyId);
  const payload = stagedWrap?.payload || null;
  const failures = [];
  const warnings = [];

  if (!profile) failures.push("MISSING_PROPERTY_PROFILE");
  if (!period?.observations?.length) failures.push("MISSING_CORE_PERIOD_OBSERVATIONS");
  if (!payload) failures.push("MISSING_STAGED_PAYLOAD");

  const promptMap = period?.periodId
    ? loadArchivePromptMap(propertyId, period.periodId)
    : null;

  const recount = period && profile
    ? challengerRecountCoreKpis(period, profile, { promptMap })
    : null;
  const competitive = challengerAuditCompetitiveInventory(propertyId, { cwd });
  const reality = challengerRealityMetrics(payload);
  const em = payload?.executiveMetrics || {};
  const stagedCons = em.considerationRate?.rate ?? null;
  const stagedPresent = em.considerationRate?.presentObservations ?? em.considerationRate?.present ?? null;
  const stagedDenom = em.considerationRate?.comparableObservations ?? null;
  const stagedScen = em.scenarioPresence?.rate ?? null;
  const stagedScenElig = em.scenarioPresence?.eligibleScenarios ?? null;
  const stagedScenAppeared = em.scenarioPresence?.capturedScenarios ?? em.scenarioPresence?.appeared ?? null;

  const discrepancies = [];
  const sensitivityWarnings = [];
  if (recount && payload) {
    // Hard path = governed subject presence (not raw stored.mentioned flags)
    if (!tolEq(recount.considerationRate, stagedCons, 1.0)) {
      discrepancies.push({
        metric: "considerationRate",
        challenger: recount.considerationRate,
        staged: stagedCons,
        path: "governed_subject_presence",
      });
    }
    if (
      stagedPresent != null &&
      Math.abs(recount.presentObservations - Number(stagedPresent)) > 3
    ) {
      discrepancies.push({
        metric: "considerationNumerator",
        challenger: recount.presentObservations,
        staged: stagedPresent,
        path: "governed_subject_presence",
      });
    }
    if (
      stagedDenom != null &&
      Math.abs(recount.comparableObservations - Number(stagedDenom)) > 4
    ) {
      discrepancies.push({
        metric: "considerationDenominator",
        challenger: recount.comparableObservations,
        staged: stagedDenom,
        path: "governed_subject_presence",
      });
    }
    if (!tolEq(recount.scenarioPresenceRate, stagedScen, 2.5)) {
      discrepancies.push({
        metric: "scenarioPresenceRate",
        challenger: recount.scenarioPresenceRate,
        staged: stagedScen,
        path: "governed_subject_presence",
      });
    }
    if (!recount.providerNumeratorReconciles) {
      failures.push("CHALLENGER_PROVIDER_NUMERATOR_MISMATCH");
    }
    if ((recount.storedVsGovernedConflicts || 0) > 5) {
      sensitivityWarnings.push(
        `STORED_MENTION_FLAGS_UNRELIABLE:conflicts=${recount.storedVsGovernedConflicts};do_not_trust_period_mentioned`
      );
    }
    if (
      recount.aliasPath &&
      stagedPresent != null &&
      Math.abs(recount.aliasPath.presentObservations - Number(stagedPresent)) > 5
    ) {
      sensitivityWarnings.push(
        `ALIAS_SUBJECT_RECOGNITION_DELTA:alias=${recount.aliasPath.presentObservations};staged=${stagedPresent}`
      );
    }
  }

  if (competitive.invalidCompetitorsInRegistry > 0) {
    failures.push(
      `INVALID_COMPETITORS_IN_REGISTRY:${competitive.invalidCompetitorsInRegistry}`
    );
  }
  // Autonomous CORE peer cap applies only when the durable registry explicitly
  // uses coreEligible flags (greenfield / autonomous synthesis). Curated CORE
  // sets (e.g. Waterstone) may legitimately exceed AUTONOMOUS_CORE_PEER_CAP.
  const regDoc = loadRegistryDoc(propertyId, cwd);
  const autonomousCoreFlags = (regDoc?.hotels || []).some(
    (h) => typeof h.coreEligible === "boolean" && !h.subject
  );
  if (
    autonomousCoreFlags &&
    competitive.customerFacingDisplayedCoreCount != null &&
    competitive.customerFacingDisplayedCoreCount > AUTONOMOUS_CORE_PEER_CAP + 3
  ) {
    failures.push(
      `STAGED_CORE_EXCEEDS_CAP:${competitive.customerFacingDisplayedCoreCount}`
    );
  } else if (
    !autonomousCoreFlags &&
    competitive.customerFacingDisplayedCoreCount != null &&
    competitive.customerFacingDisplayedCoreCount > AUTONOMOUS_CORE_PEER_CAP + 3
  ) {
    warnings.push(
      `CURATED_CORE_LARGER_THAN_AUTONOMOUS_CAP:${competitive.customerFacingDisplayedCoreCount}`
    );
  }
  if (reality.present && !reality.coverageMatches) {
    discrepancies.push({
      metric: "realityCoverage",
      challenger: reality.recomputedCoverage,
      staged: reality.stagedCoverage,
    });
  }
  // coverage == 100-gap is legitimate when partials=0 and coveragePercent is stored.
  // Only fail when recomputed coverage from counts disagrees with staged coverage.
  if (
    reality.present &&
    reality.recomputedCoverage != null &&
    reality.stagedCoverage != null &&
    !reality.coverageMatches
  ) {
    failures.push("REALITY_COVERAGE_ARITHMETIC_MISMATCH");
  }

  // Scenario taxonomy: intents present in period should be subset of approved
  const intentKeys = Object.keys(recount?.intentPresence || {}).filter(
    (k) => k && k !== "unknown"
  );
  const unknownIntents = intentKeys.filter((k) => !APPROVED_INTENTS.includes(k));
  if (unknownIntents.length) {
    warnings.push(`NON_CANONICAL_INTENT_TAGS:${unknownIntents.join("|")}`);
  }

  let competitiveGate = null;
  let measurementGate = null;
  try {
    competitiveGate = assessCompetitiveEntityIntegrity(propertyId, { cwd });
    if (!competitiveGate.ok) failures.push(...(competitiveGate.failures || []).slice(0, 6));
  } catch (err) {
    failures.push(`COMPETITIVE_GATE_THREW:${err.message}`);
  }
  try {
    measurementGate = assessMeasurementIntegrityGate({ propertyId });
    if (!measurementGate.ok) {
      failures.push(
        ...(measurementGate.failures || [])
          .filter((f) => !String(f).startsWith("COMPETITIVE_INTEGRITY_"))
          .slice(0, 6)
      );
    }
  } catch (err) {
    failures.push(`MEASUREMENT_GATE_THREW:${err.message}`);
  }

  const bpp = payload?.brandPortfolioPosition || null;
  const bppStatus =
    bpp?.status || bpp?.customerState || (bpp ? "PRESENT" : "BPP_ABSENT_OR_NA");

  const materialDiscrepancies = discrepancies.filter((d) => {
    if (d.challenger == null && d.staged != null) return true;
    if (d.metric === "considerationDenominator") {
      return Math.abs(Number(d.challenger) - Number(d.staged)) > 8;
    }
    if (d.metric === "considerationNumerator") {
      return Math.abs(Number(d.challenger) - Number(d.staged)) > 3;
    }
    if (typeof d.challenger === "number" && typeof d.staged === "number") {
      return Math.abs(d.challenger - d.staged) > 2.5;
    }
    return true;
  });
  if (materialDiscrepancies.length) {
    failures.push(`CHALLENGER_STAGED_DISCREPANCY:${materialDiscrepancies.length}`);
  }
  warnings.push(...sensitivityWarnings);

  const published = loadPublishedManifest(propertyId);
  let publishedParity = "NOT_PUBLISHED";
  if (published) {
    try {
      const pub = loadPublishedReport(propertyId);
      const pubPayload = pub?.payload || pub?.report || pub;
      if (pubPayload?.executiveMetrics && payload?.executiveMetrics) {
        const pubRate = pubPayload.executiveMetrics?.considerationRate?.rate;
        const stagedRate = payload.executiveMetrics?.considerationRate?.rate;
        if (tolEq(pubRate, stagedRate, 0.05)) {
          publishedParity = "PUBLISHED_MATCHES_STAGED";
        } else {
          publishedParity = "EXPECTED_STAGED_AHEAD_OF_PUBLISHED";
          warnings.push(
            `PUBLISHED_STAGED_CONSIDERATION_DIFF:published=${pubRate};staged=${stagedRate}`
          );
        }
      } else {
        publishedParity = "PUBLISHED_PRESENT_PAYLOAD_UNREADABLE";
      }
    } catch {
      publishedParity = "PUBLISHED_LOAD_ERROR";
    }
  }

  let externalShare = "NO_SHARE_TOKEN";
  try {
    const reg = readShareRegistry();
    const tokens = Object.values(reg?.tokens || {});
    const mine = tokens.filter((t) => t?.propertyId === propertyId);
    if (mine.length) {
      externalShare = mine.some((t) => t.status === "REVOKED")
        ? "SHARE_PRESENT_SOME_REVOKED"
        : "SHARE_TOKEN_PRESENT_NOT_LIVE_READ";
    }
  } catch {
    externalShare = "SHARE_REGISTRY_UNREADABLE";
  }

  const ok = failures.length === 0;
  let certificationStatus = CERTIFICATION_STATUSES.REQUIRES_DATA_REPAIR;
  if (ok && payload) {
    certificationStatus = CERTIFICATION_STATUSES.AWAITING_PUBLICATION_AUTHORIZATION;
  } else if (failures.some((f) => String(f).includes("HARDCODED_FOREIGN"))) {
    certificationStatus = CERTIFICATION_STATUSES.REQUIRES_ADDITIONAL_MONITORING;
  }

  return {
    contract: ADP_INDEPENDENT_CHALLENGER_VERIFIER_V1,
    propertyId,
    name: profile?.name || propertyId,
    ok,
    certificationStatus,
    failures,
    warnings,
    discrepancies,
    materialDiscrepancies,
    periodId: period?.periodId || null,
    observationCount: period?.observations?.length || 0,
    stagedVersionId: stagedWrap?.versionId || null,
    recount,
    staged: {
      considerationRate: stagedCons,
      present: stagedPresent,
      comparable: stagedDenom,
      scenarioPresenceRate: stagedScen,
      scenarioEligible: stagedScenElig,
      scenarioAppeared: stagedScenAppeared,
    },
    competitive,
    reality,
    bppStatus,
    publishedParity,
    externalShare,
    competitiveGateOk: competitiveGate?.ok ?? null,
    measurementGateOk: measurementGate?.ok ?? null,
    registryLoaded: Boolean(loadDurableCompetitorRegistry(propertyId, cwd)),
  };
}

export function classifyFinalCertificationStatus(row, { browserOk = null, pdfOk = null } = {}) {
  if (row.certificationStatus === CERTIFICATION_STATUSES.REQUIRES_ADDITIONAL_MONITORING) {
    return CERTIFICATION_STATUSES.REQUIRES_ADDITIONAL_MONITORING;
  }
  if (!row.ok || (row.materialDiscrepancies || []).length > 0) {
    return CERTIFICATION_STATUSES.REQUIRES_DATA_REPAIR;
  }
  if (browserOk === false || pdfOk === false) {
    return CERTIFICATION_STATUSES.BROWSER_OR_PDF_BLOCKED;
  }
  // Live external readback only blocks when a live check was attempted and failed.
  if (browserOk === false && row.externalShare?.startsWith("SHARE")) {
    return CERTIFICATION_STATUSES.EXTERNAL_PARITY_BLOCKED;
  }
  if (browserOk === true && pdfOk === true && row.ok) {
    return CERTIFICATION_STATUSES.INDEPENDENTLY_CERTIFIED_STAGED;
  }
  if (row.ok) return CERTIFICATION_STATUSES.AWAITING_PUBLICATION_AUTHORIZATION;
  return CERTIFICATION_STATUSES.REQUIRES_DATA_REPAIR;
}
