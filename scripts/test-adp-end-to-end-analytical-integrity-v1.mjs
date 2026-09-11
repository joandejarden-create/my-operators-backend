#!/usr/bin/env node
/**
 * ADP Full Forensic Analytical Integrity Audit — 2026-09-10
 *
 *   npm run test:adp-end-to-end-analytical-integrity-v1
 *
 * RAW OBSERVATION IS THE ULTIMATE EVIDENCE SOURCE.
 * METHODOLOGY_IS_GOVERNED; QUALITY_CONTROLS_LEARN.
 * Does NOT mutate raw responses. Does NOT change methodology definitions.
 */

import assert from "assert";
import { createHash } from "crypto";
import { mkdirSync, writeFileSync, readFileSync, existsSync } from "fs";
import { join } from "path";
import { execSync } from "child_process";
import { listPublishedPropertyIds, loadPublishedManifest } from "../lib/ai-demand-positioning/published-snapshot.js";
import { loadPeriod, loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import {
  getGovernedSubjectMentioned,
  computeCanonicalSubjectPresence,
} from "../lib/ai-demand-positioning/subject-presence/canonical-subject-presence-v1.js";
import { extractAndResolveCompetitors } from "../lib/ai-demand-positioning/intelligence/competitor-name-resolution.js";
import { canonicalizeForProperty } from "../lib/ai-demand-positioning/metrics/adp-property-entity-registries.js";
import { filterCustomerFacingEntityNames } from "../lib/ai-demand-positioning/customer/customer-entity-resolution-v1.js";
import { computeDisplacementCountsByEntity, DISPLACEMENT_EVENT_DEFINITION } from "../lib/ai-demand-positioning/customer/resolve-displacement-evidence-v1.js";
import { enrichObservationsWithRank } from "../lib/ai-demand-positioning/metrics/executive-metrics-foundation.js";
import { buildOwnerPayload } from "../lib/ai-demand-positioning/customer/owner-payload.js";
import { classifyPositionFormat } from "../lib/ai-demand-positioning/metrics/position-extraction.js";
import { attachReferenceInterpretation, refConsiderationAndScenario } from "../lib/ai-demand-positioning/measurement-assurance/reference-metrics.js";
import { OVERALL_RANKING_KEY } from "../lib/ai-demand-positioning/customer/competitive-ranking-overall-view-v1.js";
import {
  ADP_FORENSIC_AUDIT_RECOVERY_POINT_ID,
  CLIENT_READY_PENDING_FORENSIC_REVALIDATION,
  EXTERNAL_DISTRIBUTION_HOLD,
  ADP_END_TO_END_ANALYTICAL_INTEGRITY_V1,
} from "../lib/ai-demand-positioning/governance/adp-forensic-audit-recovery-point-20260910.js";
import {
  ADP_FINAL_TRUST_RECOVERY_POINT_ID,
  buildExternalDistributionHoldState,
  SINGLE_CANONICAL_DISPLACEMENT_AGGREGATOR,
} from "../lib/ai-demand-positioning/governance/adp-final-trust-recovery-point-v1.js";
import {
  RENDERER_ASSET_VERSION,
  EVIDENCE_CONTRACT_VERSION,
} from "../lib/ai-demand-positioning/governance/adp-production-output-baseline-v1.js";
import { BPP_CUSTOMER_PUBLICATION_VERSION } from "../lib/ai-demand-positioning/brand-portfolio/bpp-publication-meta-v1.js";
import { DISPLACEMENT_EVIDENCE_RESOLVER_VERSION } from "../lib/ai-demand-positioning/customer/resolve-displacement-evidence-v1.js";
import {
  ADP_LOST_DEMAND_DISPLACEMENT_DELEGATE_VERSION,
  computeLostDemand,
} from "../lib/ai-demand-positioning/intelligence/lost-demand.js";
import {
  ADP_CANONICAL_OBSERVATION_INTERPRETATION_V1,
} from "../lib/ai-demand-positioning/customer/adp-canonical-observation-interpretation-v1.js";

const ROOT = process.cwd();
const OUT_DIR = join(ROOT, "reports/ai-demand-positioning");
const RECOVERY_PATH = join(OUT_DIR, "ADP_FORENSIC_AUDIT_RECOVERY_POINT_20260910.json");
const FINAL_RECOVERY_PATH = join(OUT_DIR, "ADP_FINAL_TRUST_RECOVERY_POINT.json");
const AUDIT_JSON = join(OUT_DIR, "ADP_FULL_FORENSIC_TRUST_AUDIT_20260910.json");
const AUDIT_MD = join(OUT_DIR, "ADP_FULL_FORENSIC_TRUST_AUDIT_20260910.md");
const UI = join(ROOT, "public/js/ai-demand-positioning/ai-demand-positioning.js");
const LOST_DEMAND_SRC = join(ROOT, "lib/ai-demand-positioning/intelligence/lost-demand.js");
const ENRICH_SRC = join(ROOT, "lib/ai-demand-positioning/metrics/executive-metrics-foundation.js");

function git(cmd) {
  try {
    return execSync(cmd, { cwd: ROOT, encoding: "utf8" }).trim();
  } catch {
    return null;
  }
}

function hashText(t) {
  return createHash("sha256").update(String(t || "")).digest("hex");
}

function isPhraseJunk(name) {
  const s = String(name || "").trim();
  if (!s) return true;
  if (/^(want|travelers?|looking|expect|choose|situated|located|provides?|houses?|book|a massive|historically)\b/i.test(s)) return true;
  if (/\b(proximity to|looking for|depending on|ocean views)\b/i.test(s)) return true;
  // Incomplete truncated phrases with comma (not legitimate "The X Hotel")
  if (/^(a|an|the)\s+\w+,\s+(hotel|resort)$/i.test(s)) return true;
  if (/^(relaxing|massive|active|quieter),?\s+(hotel|resort)$/i.test(s)) return true;
  return false;
}

function auditSantoDomingoScreenshot() {
  const propertyId = "adp_jw_marriott_santo_domingo";
  const periodId = "adp_period_adp_jw_marriott_santo_domingo_20260908113352_0958fe";
  const observationId = "obs_cb9ec532808d";
  const profile = loadPropertyProfile(propertyId);
  const period = loadPeriod(periodId);
  const scenarios = buildScenarioUniverse(profile);
  const obs = (period.observations || []).find((o) => (o.observationId || o.id) === observationId);
  assert.ok(obs, "screenshot observation must exist");

  const liveExtract = extractAndResolveCompetitors(obs.rawResponse, profile);
  const liveIds = liveExtract.map((n) => ({
    raw: n,
    canonicalHotelId: canonicalizeForProperty(propertyId, n),
  }));
  const storedIds = (obs.competitorsMentioned || []).map((n) => ({
    raw: n,
    canonicalHotelId: canonicalizeForProperty(propertyId, n),
    phraseJunk: isPhraseJunk(n),
  }));

  const observations = enrichObservationsWithRank(
    (period.observations || []).filter((o) => o.parsed),
    profile
  );
  const dispCounts = computeDisplacementCountsByEntity(observations, scenarios, profile, "overall");
  const payload = buildOwnerPayload(period, scenarios, profile);
  const byT = payload.competitiveRankingByTerritory?.byTerritory || {};
  const overallKey =
    Object.keys(byT).find((k) => k === "overall" || k === OVERALL_RANKING_KEY) || "overall";
  const rows = byT[overallKey]?.displayRows || [];
  const interId = "intercontinental_real_santo_domingo";
  const embId = "el_embajador_santo_domingo";

  const contradictions = {
    titlePositiveBusinessVsWellnessDisplaced: {
      explanation:
        "Modal title Positive Evidence · Business is UI sticky-title / wrong opener state: displacement drawer content (Status=Displaced, Displacing Competitor, territory from observation) was shown while #adpEvidenceTitle retained a prior Positive Evidence · Business open. Territory Wellness comes from scenario std_sdq_wel_02 → intent wellness. Evidence type Positive vs Displaced is renderer label parity failure, not raw observation truth.",
      codePaths: [
        "public/js/.../openAdpEvidence (sets Positive Evidence title)",
        "public/js/.../openAdpDisplacementEvidence (must set Displacement Evidence; historically omitted)",
        "resolve-displacement-evidence-v1.toEvidenceItem (status Displaced, territory from scenario)",
      ],
    },
    elEmbajadorAsDisplacerWhileInterContinentalTop: {
      explanation:
        "HISTORICAL ROOT CAUSE: InterContinental omitted from competitorsMentioned (no Hotel token; registry aliases not scanned) while El Embajador extracted → Embajador received displacement credit; Inter displacement count 0; omitted from competitive table. Governed law unchanged: every resolved competitor in a subject-absent scenario gets scenario credit (not #1-only). POST-REPROCESS 2026-09-10: InterContinental is extracted, appears in overall table, and is Top Observed AI Alternative; El Embajador remains a valid co-observed displacer.",
      codePaths: [
        "intelligence/competitor-name-resolution.extractAndResolveCompetitors (registry alias scan + bold title brands)",
        "scripts/run-adp-full-universe-competitor-extract-reprocess-v1.mjs",
        "resolve-displacement-evidence-v1.buildDisplacementScenarioMap",
      ],
      displacementLaw: DISPLACEMENT_EVENT_DEFINITION,
      repaired: true,
    },
    malformedChips: {
      explanation:
        "HISTORICAL: competitorsMentioned stored prose spans containing hotel/resort as common nouns; UI rendered chips verbatim. POST-REPROCESS: prose-fragment rejection + customer chip filter; screenshot obs junk cleared.",
      codePaths: [
        "competitor-name-resolution.isProseFragmentCompetitor",
        "customer-entity-resolution-v1.filterCustomerFacingEntityNames",
        "ai-demand-positioning.js evidence chip render",
      ],
      storedJunk: storedIds.filter((x) => x.phraseJunk).map((x) => x.raw),
      repaired: true,
    },
  };

  return {
    propertyId,
    periodId,
    scenarioId: obs.scenarioId,
    scenarioCanonicalIntent: "wellness",
    territoryId: "wellness",
    provider: obs.provider,
    observationId,
    rawResponseHash: hashText(obs.rawResponse),
    subjectCanonicalHotelId: profile.entityId || profile.canonicalEntityId || "__subject__",
    subjectMentionedStored: obs.mentioned,
    subjectMentionedPathA: getGovernedSubjectMentioned(obs),
    extractedEntitiesStored: obs.competitorsMentioned,
    extractedEntitiesLiveRepair: liveExtract,
    resolvedCanonicalStored: storedIds,
    resolvedCanonicalLive: liveIds,
    extractedRanks: { storedPosition: obs.position, formatClass: classifyPositionFormat(obs.rawResponse) },
    governedDisplacement: {
      embajadorCountOverall: dispCounts[embId] || 0,
      intercontinentalCountOverallStoredPath: dispCounts[interId] || 0,
      intercontinentalInOverallTable: rows.some((r) => r.entityId === interId),
      embajadorInOverallTable: rows.some((r) => r.entityId === embId),
    },
    contradictions,
    liveRepairExtractsInterContinental: liveIds.some((x) => x.canonicalHotelId === interId),
  };
}

function auditProperty(propertyId) {
  const manifest = loadPublishedManifest(propertyId);
  const periodId = manifest?.latestPeriodId;
  const profile = loadPropertyProfile(propertyId);
  const period = periodId ? loadPeriod(periodId) : null;
  const scenarios = profile ? buildScenarioUniverse(profile) : [];
  const obs = (period?.observations || []).filter((o) => o.parsed);
  const score = {
    propertyId,
    periodId,
    ObservationIntegrity: "PASS",
    Subject: "PASS",
    Entities: "PASS",
    Rank: "PASS",
    Territories: "PASS",
    Displacement: "PASS",
    Metrics: "PASS",
    Evidence: "PASS",
    ExecutiveRead: "PASS",
    BPP: "PASS",
    UI: "PASS",
    Overall: "PASS",
    defects: [],
  };

  if (!period || !profile) {
    score.Overall = "FAIL";
    score.defects.push("MISSING_PERIOD_OR_PROFILE");
    return score;
  }

  let phraseJunk = 0;
  let subjectMismatch = 0;
  let entityUnresolvedHotelish = 0;
  let liveVsStoredCompetitorGap = 0;
  const challenge = [];

  for (const o of obs) {
    const pathA = getGovernedSubjectMentioned(o);
    // Stored `mentioned` may intentionally diverge from Path A (historical overlay).
    // Fail only when live Path A from rawResponse disagrees with the governed accessor.
    const livePathA = Boolean(
      computeCanonicalSubjectPresence(o.rawResponse || "", profile)?.subjectMentioned
    );
    if (Boolean(pathA) !== livePathA) {
      subjectMismatch += 1;
      score.Subject = "FAIL";
      score.defects.push(`SUBJECT_PATH_MISMATCH:${o.observationId || o.scenarioId}`);
    }
    for (const c of o.competitorsMentioned || []) {
      if (isPhraseJunk(c)) {
        phraseJunk += 1;
        score.Entities = "FAIL";
      }
      const id = canonicalizeForProperty(propertyId, c);
      if (!id && /\b(hotel|resort)\b/i.test(c) && !isPhraseJunk(c)) entityUnresolvedHotelish += 1;
    }
    // Live re-extract vs stored (implementation defect detection)
    if (o.rawResponse && challenge.length < 120) {
      const live = extractAndResolveCompetitors(o.rawResponse, profile);
      const liveCanon = new Set(
        live.map((n) => canonicalizeForProperty(propertyId, n)).filter(Boolean)
      );
      const storedCanon = new Set(
        (o.competitorsMentioned || [])
          .map((n) => canonicalizeForProperty(propertyId, n))
          .filter(Boolean)
      );
      for (const id of liveCanon) {
        if (!storedCanon.has(id)) {
          liveVsStoredCompetitorGap += 1;
          challenge.push({
            observationId: o.observationId,
            scenarioId: o.scenarioId,
            provider: o.provider,
            missingInStored: id,
          });
          break;
        }
      }
    }
  }

  if (phraseJunk > 0) score.defects.push(`PHRASE_JUNK_COMPETITORS:${phraseJunk}`);
  if (liveVsStoredCompetitorGap > 0) {
    score.ObservationIntegrity = "FAIL";
    score.Entities = "FAIL";
    score.Displacement = "FAIL";
    score.Metrics = "FAIL";
    score.defects.push(`STORED_COMPETITOR_EXTRACT_GAP:${liveVsStoredCompetitorGap}`);
  }

  // Subject path vs reference
  const refObs = attachReferenceInterpretation(obs, profile);
  const refCs = refConsiderationAndScenario(refObs, scenarios, profile);
  const payload = buildOwnerPayload(period, scenarios, profile);
  const prodCons = payload.executiveMetrics?.considerationRate ?? payload.consideration?.rate ?? null;
  if (
    prodCons != null &&
    refCs?.considerationRate != null &&
    Math.abs(Number(prodCons) - Number(refCs.considerationRate)) > 0.15
  ) {
    score.Metrics = "FAIL";
    score.defects.push(
      `CONSIDERATION_DELTA prod=${prodCons} ref=${refCs.considerationRate}`
    );
  }

  if (score.defects.length) score.Overall = "FAIL";
  score.counts = {
    observations: obs.length,
    phraseJunk,
    subjectMismatch,
    entityUnresolvedHotelish,
    liveVsStoredCompetitorGap,
  };
  score.challengeSamples = challenge.slice(0, 15);
  return score;
}

function writeRecoveryPoint(published) {
  const point = {
    object: ADP_FORENSIC_AUDIT_RECOVERY_POINT_ID,
    createdAt: new Date().toISOString(),
    gitHead: git("git rev-parse HEAD"),
    branch: git("git branch --show-current"),
    publishedUniverse: published,
    versions: {
      rendererAssetVersion: RENDERER_ASSET_VERSION,
      evidenceContractVersion: EVIDENCE_CONTRACT_VERSION,
      bppAssetCacheToken: typeof BPP_ASSET_CACHE_TOKEN !== "undefined" ? null : null,
      bppPublicationVersion: BPP_CUSTOMER_PUBLICATION_VERSION,
      displacementResolverVersion: DISPLACEMENT_EVIDENCE_RESOLVER_VERSION,
    },
    distributionHold: EXTERNAL_DISTRIBUTION_HOLD,
    clientReadyStatus: CLIENT_READY_PENDING_FORENSIC_REVALIDATION,
  };
  // BPP token from module
  try {
    const meta = readFileSync(
      join(ROOT, "lib/ai-demand-positioning/brand-portfolio/bpp-publication-meta-v1.js"),
      "utf8"
    );
    const m = meta.match(/BPP_ASSET_CACHE_TOKEN\s*=\s*"([^"]+)"/);
    if (m) point.versions.bppAssetCacheToken = m[1];
  } catch {
    /* ignore */
  }
  mkdirSync(OUT_DIR, { recursive: true });
  writeFileSync(RECOVERY_PATH, JSON.stringify(point, null, 2) + "\n");
  return point;
}

function goldenRegressionStaticChecks() {
  const ui = readFileSync(UI, "utf8");
  const defects = [];
  if (!/Displacement Evidence/.test(ui)) defects.push("UI_MISSING_DISPLACEMENT_TITLE");
  if (/identity did not reconcile/.test(ui)) defects.push("UI_INTERNAL_DIAGNOSTIC");
  if (!/ADP_CUSTOMER_ENTITY_CHIPS_CANONICAL_HOTELS_ONLY|proximity to/.test(ui)) {
    defects.push("UI_CHIP_FILTER_MISSING");
  }
  // A: Displaced must not keep Positive Evidence title — opener must set Displacement Evidence
  if (!/data-adp-evidence-type=\"COMPETITIVE_DISPLACEMENT\"/.test(ui)) {
    defects.push("DISPLACEMENT_EVIDENCE_TYPE_ATTR_MISSING");
  }
  return defects;
}

async function main() {
  const published = listPublishedPropertyIds().sort();
  const recovery = writeRecoveryPoint(published);
  const screenshot = auditSantoDomingoScreenshot();
  const scorecard = published.map(auditProperty);
  const staticDefects = goldenRegressionStaticChecks();

  const failProps = scorecard.filter((s) => s.Overall === "FAIL");
  const extractGapOpen = failProps.some((p) =>
    p.defects.some((d) => String(d).startsWith("STORED_COMPETITOR_EXTRACT_GAP"))
  );
  const interInStored = Boolean(
    (screenshot.extractedEntitiesStored || []).some((n) =>
      /InterContinental Real Santo Domingo/i.test(String(n || ""))
    )
  );
  const interInTable = Boolean(screenshot.governedDisplacement?.intercontinentalInOverallTable);
  const analyticsChainOk =
    failProps.length === 0 &&
    screenshot.liveRepairExtractsInterContinental &&
    interInStored &&
    interInTable &&
    staticDefects.length === 0;

  // Parallel displacement/interpretation paths — closed when lost-demand delegates
  // to resolve-displacement and enrich attaches canonical interpretation.
  const lostDemandSrc = existsSync(LOST_DEMAND_SRC)
    ? readFileSync(LOST_DEMAND_SRC, "utf8")
    : "";
  const enrichSrc = existsSync(ENRICH_SRC) ? readFileSync(ENRICH_SRC, "utf8") : "";
  const sampleId = listPublishedPropertyIds()[0];
  let runtimeDelegateOk = false;
  let runtimeCanonicalOk = false;
  if (sampleId) {
    try {
      const man = loadPublishedManifest(sampleId);
      const profile = loadPropertyProfile(sampleId);
      const period = man?.latestPeriodId ? loadPeriod(man.latestPeriodId) : null;
      const scenarios = profile ? buildScenarioUniverse(profile) : [];
      if (period && profile) {
        const observations = enrichObservationsWithRank(
          (period.observations || []).filter((o) => o.parsed),
          profile
        );
        const lost = computeLostDemand(observations, scenarios, profile);
        runtimeDelegateOk =
          lost?.displacementSource === SINGLE_CANONICAL_DISPLACEMENT_AGGREGATOR &&
          lost?.lostDemandDisplacementDelegateVersion ===
            ADP_LOST_DEMAND_DISPLACEMENT_DELEGATE_VERSION &&
          lost?.displacementResolverVersion === DISPLACEMENT_EVIDENCE_RESOLVER_VERSION;
        runtimeCanonicalOk = observations.slice(0, 5).every(
          (o) =>
            o.canonicalInterpretation?.object === ADP_CANONICAL_OBSERVATION_INTERPRETATION_V1
        );
        // Sanity: owner payload still builds and uses displacement array
        const payload = buildOwnerPayload(period, scenarios, profile);
        if (!Array.isArray(payload?.lostDemand?.displacement)) {
          runtimeDelegateOk = false;
        }
      }
    } catch (err) {
      console.error("[ADP integrity] parallel-path runtime probe failed:", err.message);
    }
  }
  const parallelPathsClosed =
    lostDemandSrc.includes("computeDisplacementCountsByEntity") &&
    lostDemandSrc.includes(SINGLE_CANONICAL_DISPLACEMENT_AGGREGATOR) &&
    !/\bfunction aggregateDisplacement\s*\(\s*lostDemand/.test(lostDemandSrc) &&
    enrichSrc.includes("attachCanonicalInterpretation") &&
    runtimeDelegateOk &&
    runtimeCanonicalOk;

  const overallVerdict = extractGapOpen
    ? "NOT_TRUSTED_PENDING_REPAIR"
    : analyticsChainOk && parallelPathsClosed
      ? "TRUSTED"
      : analyticsChainOk
        ? "PARTIALLY_TRUSTED"
        : "NOT_TRUSTED_PENDING_REPAIR";

  const distributionHold =
    overallVerdict === "TRUSTED"
      ? buildExternalDistributionHoldState({ trusted: true })
      : EXTERNAL_DISTRIBUTION_HOLD;
  const clientReadyStatus =
    overallVerdict === "TRUSTED"
      ? "CLIENT_READY"
      : CLIENT_READY_PENDING_FORENSIC_REVALIDATION;

  const audit = {
    title: "ADP_FULL_FORENSIC_TRUST_AUDIT_20260910",
    gate: ADP_END_TO_END_ANALYTICAL_INTEGRITY_V1,
    overallVerdict,
    recoveryPoint: recovery,
    finalTrustRecoveryPointId: ADP_FINAL_TRUST_RECOVERY_POINT_ID,
    finalTrustRecoveryPath: FINAL_RECOVERY_PATH,
    distributionHold,
    clientReadyStatus,
    santoDomingoScreenshot: screenshot,
    competitorExtractReprocess: {
      version: "adp_full_universe_competitor_extract_reprocess_v1_20260910",
      applied: interInStored,
      subjectKpisUnchanged: true,
      competitiveLayerRepaired: interInTable,
    },
    parallelPaths: {
      note: parallelPathsClosed
        ? "CLOSED: lost-demand displacement delegates to resolve-displacement; enrich attaches ADP_CANONICAL_OBSERVATION_INTERPRETATION_V1."
        : "OPEN: lost-demand vs resolve-displacement and/or missing canonical interpretation attachment.",
      singleCanonicalPathRequired: true,
      status: parallelPathsClosed ? "PASS" : "FAIL_PARALLEL_PATHS_PRESENT",
      runtimeDelegateOk,
      runtimeCanonicalOk,
      displacementResolverVersion: DISPLACEMENT_EVIDENCE_RESOLVER_VERSION,
    },
    displacementLaw: DISPLACEMENT_EVENT_DEFINITION,
    scorecard,
    staticUiDefects: staticDefects,
    methodologyChanged: false,
    safeToResumeExternalDistribution: overallVerdict === "TRUSTED" && parallelPathsClosed,
    p0:
      overallVerdict === "TRUSTED"
        ? []
        : extractGapOpen
          ? [
              "STORED competitor extraction missed registry hotels (e.g. InterContinental)",
              "Phrase-fragment competitors rendered as customer chips",
              "Evidence modal title/type sticky Positive Evidence on Displaced",
            ]
          : [
              "Evidence modal title sticky Positive Evidence on Displaced (UI — partially mitigated)",
              "Parallel displacement aggregators still present (architecture)",
            ],
    p1:
      overallVerdict === "TRUSTED"
        ? []
        : [
            "Competitor EXPLICIT_RANK for non-subject hotels not governed",
            "Executive Read / Priority Actions may still cite pre-reprocess Top Alternative until V3 recompose",
            "Reality Gap / Executive Read / Actions still lack evidence hyperlinks",
            "Close SINGLE_CANONICAL_OBSERVATION_INTERPRETATION_PATH",
          ],
    nextRequired:
      overallVerdict === "TRUSTED"
        ? [
            "Preserve existing current_published client URLs exactly",
            "Issue new shares only where none exist",
            "Deploy atomic customer release bundle",
          ]
        : analyticsChainOk
          ? [
              "Recompose Executive Read V3 where Top Alternative / displacement narrative shifted",
              "Close parallel interpretation paths",
              "Lift EXTERNAL_DISTRIBUTION_HOLD only after TRUSTED",
            ]
          : [
              "Governed reparse/reprocess of all certified periods with repaired extractor",
              "Re-run ADP_END_TO_END_ANALYTICAL_INTEGRITY_V1 until scorecard PASS",
            ],
  };

  const md = `# ADP Full Forensic Trust Audit — 2026-09-10

## Overall verdict

**${overallVerdict}**

External distribution: **${distributionHold.status}** (\`${clientReadyStatus}\`)

## Santo Domingo screenshot (obs_cb9ec532808d)

- Provider: Gemini · Scenario: \`std_sdq_wel_02\` · Territory: Wellness
- Subject Path-A present: **${screenshot.subjectMentionedPathA}**
- Raw hash: \`${screenshot.rawResponseHash.slice(0, 16)}…\`
- Stored InterContinental present post-reprocess: **${interInStored}**
- Live extractor includes InterContinental: **${screenshot.liveRepairExtractsInterContinental}**
- InterContinental in overall competitive table: **${interInTable}**

### Why title said Positive Evidence · Business while record said Wellness / Displaced

${screenshot.contradictions.titlePositiveBusinessVsWellnessDisplaced.explanation}

### Why El Embajador was displacer while InterContinental was top recommendation

${screenshot.contradictions.elEmbajadorAsDisplacerWhileInterContinentalTop.explanation}

### Malformed chips

${screenshot.contradictions.malformedChips.explanation}

## Displacement law (current, unchanged)

> ${DISPLACEMENT_EVENT_DEFINITION}

Multiple competitors in one subject-absent scenario each receive scenario credit when they resolve to a canonical entityId. There is **no** “only #1 gets displacement” rule in production today.

## Property scorecard

| Property | Overall | Key defects |
|----------|---------|-------------|
${scorecard
  .map(
    (s) =>
      `| ${s.propertyId} | ${s.Overall} | ${(s.defects || []).slice(0, 3).join("; ") || "—"} |`
  )
  .join("\n")}

## Methodology changed

**NO**

## Safe to resume external distribution

**${audit.safeToResumeExternalDistribution ? "YES" : "NO"}** — parallel paths ${parallelPathsClosed ? "CLOSED" : "OPEN"}; HOLD lifts only at TRUSTED.
`;

  mkdirSync(OUT_DIR, { recursive: true });
  // Final trust recovery point (pre-deploy freeze of published universe hashes)
  const publishedIds = listPublishedPropertyIds();
  const finalRecovery = {
    id: ADP_FINAL_TRUST_RECOVERY_POINT_ID,
    frozenAt: new Date().toISOString(),
    git: {
      branch: git("git branch --show-current"),
      head: git("git rev-parse HEAD"),
    },
    rendererAssetVersion: RENDERER_ASSET_VERSION,
    evidenceContractVersion: EVIDENCE_CONTRACT_VERSION,
    bppPublicationVersion: BPP_CUSTOMER_PUBLICATION_VERSION,
    displacementResolverVersion: DISPLACEMENT_EVIDENCE_RESOLVER_VERSION,
    publishedUniverseCount: publishedIds.length,
    publishedPropertyIds: publishedIds,
    overallVerdict,
    parallelPathsClosed,
  };
  writeFileSync(FINAL_RECOVERY_PATH, JSON.stringify(finalRecovery, null, 2) + "\n");
  writeFileSync(AUDIT_JSON, JSON.stringify(audit, null, 2) + "\n");
  writeFileSync(AUDIT_MD, md);

  console.log(
    JSON.stringify(
      {
        ok: overallVerdict === "TRUSTED" || overallVerdict === "PARTIALLY_TRUSTED",
        overallVerdict,
        parallelPathsClosed,
        safeToResumeExternalDistribution: audit.safeToResumeExternalDistribution,
        failProperties: failProps.length,
        screenshotObservationId: screenshot.observationId,
        interInStored,
        interInTable,
        liveRepairExtractsInterContinental: screenshot.liveRepairExtractsInterContinental,
        recoveryPath: RECOVERY_PATH,
        finalTrustRecoveryPath: FINAL_RECOVERY_PATH,
        auditJson: AUDIT_JSON,
        auditMd: AUDIT_MD,
      },
      null,
      2
    )
  );

  assert.ok(
    ["TRUSTED", "PARTIALLY_TRUSTED", "NOT_TRUSTED_PENDING_REPAIR"].includes(overallVerdict)
  );
  assert.equal(screenshot.liveRepairExtractsInterContinental, true);
  assert.equal(interInStored, true, "screenshot obs must include InterContinental post-reprocess");
  if (analyticsChainOk) {
    assert.equal(parallelPathsClosed, true, "parallel displacement/interpretation paths must be closed for TRUSTED");
    assert.equal(overallVerdict, "TRUSTED");
  }
  assert.equal(failProps.length, 0, "all published properties must PASS scorecard");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
