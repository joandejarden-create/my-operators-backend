#!/usr/bin/env node
/**
 * Governed competitor-extract reprocess (forensic QC — not methodology change).
 *
 * Rewrites derived `competitorsMentioned` from rawResponse via the repaired
 * extractAndResolveCompetitors. Does NOT mutate rawResponse. Does NOT call providers.
 * Preserves governedInterpretation / historical mentioned / position.
 *
 *   node scripts/run-adp-full-universe-competitor-extract-reprocess-v1.mjs --dry-run
 *   node scripts/run-adp-full-universe-competitor-extract-reprocess-v1.mjs --apply
 *   node scripts/run-adp-full-universe-competitor-extract-reprocess-v1.mjs --apply --property=adp_jw_marriott_santo_domingo
 *
 * Doctrine: METHODOLOGY_IS_GOVERNED; QUALITY_CONTROLS_LEARN.
 * External distribution remains HOLD until ADP_END_TO_END_ANALYTICAL_INTEGRITY_V1 PASS.
 */
import "../load-env.js";
import { copyFileSync, existsSync, mkdirSync, writeFileSync } from "fs";
import { join } from "path";
import { loadPeriod, savePeriod, loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import { extractAndResolveCompetitors } from "../lib/ai-demand-positioning/intelligence/competitor-name-resolution.js";
import { canonicalizeForProperty } from "../lib/ai-demand-positioning/metrics/adp-property-entity-registries.js";
import {
  buildPublishedSnapshotBundle,
  savePublishedSnapshotBundle,
  loadPublishedManifest,
  loadPublishedReport,
} from "../lib/ai-demand-positioning/published-snapshot.js";
import { resolveGovernedAdpPropertyUniverseV1 } from "../lib/ai-demand-positioning/client-readiness/resolve-governed-adp-property-universe-v1.js";
import {
  buildCertifiedPeriodCorrectionRecord,
  writeCertifiedPeriodCorrection,
} from "../lib/ai-demand-positioning/certification/adp-certified-period-correction-v1.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { MEASUREMENT_CONTRACT_VERSION } from "../lib/ai-demand-positioning/contracts/adp-measurement-contract-v1.js";
import { enrichObservationsWithRank } from "../lib/ai-demand-positioning/metrics/executive-metrics-foundation.js";
import { computeDisplacementCountsByEntity } from "../lib/ai-demand-positioning/customer/resolve-displacement-evidence-v1.js";
import { buildOwnerPayload } from "../lib/ai-demand-positioning/customer/owner-payload.js";
import {
  CLIENT_READY_PENDING_FORENSIC_REVALIDATION,
  EXTERNAL_DISTRIBUTION_HOLD,
} from "../lib/ai-demand-positioning/governance/adp-forensic-audit-recovery-point-20260910.js";

const APPLY = process.argv.includes("--apply");
const DRY = !APPLY;
const propertyArg = process.argv.find((a) => a.startsWith("--property="));
const PROPERTY_FILTER = propertyArg
  ? propertyArg
      .slice("--property=".length)
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean)
  : null;

const VERSION = "adp_full_universe_competitor_extract_reprocess_v1_20260910";
const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
const BACKUP_ROOT = join(
  process.cwd(),
  "reports/ai-demand-positioning/full-universe-competitor-extract-reprocess-backup",
  stamp
);
const OUT = join(
  process.cwd(),
  "reports/ai-demand-positioning",
  `adp-full-universe-competitor-extract-reprocess-v1-${stamp}.json`
);
const LATEST = join(
  process.cwd(),
  "reports/ai-demand-positioning",
  "adp-full-universe-competitor-extract-reprocess-v1-latest.json"
);

function canonSet(names, propertyId) {
  return new Set(
    (names || []).map((n) => canonicalizeForProperty(propertyId, n)).filter(Boolean)
  );
}

function setDiff(a, b) {
  const out = [];
  for (const x of a) if (!b.has(x)) out.push(x);
  return out;
}

function metricSnap(payload) {
  return {
    considerationRate: payload?.executiveMetrics?.considerationRate?.rate ?? null,
    scenarioPresence: payload?.executiveMetrics?.scenarioPresence?.rate ?? null,
    demandCapture: payload?.demandCapture?.overallRate ?? null,
    topObservedAlternative:
      payload?.competitiveSet?.topObservedAlternative?.name ||
      payload?.competitiveSet?.topObservedAlternative ||
      null,
  };
}

function afterMetricsFromPeriod(period, profile, scenarios) {
  const payload = buildOwnerPayload(period, scenarios, profile);
  const projected = enrichObservationsWithRank(period.observations || [], profile);
  const disp = computeDisplacementCountsByEntity(projected, scenarios, profile, "overall");
  const top =
    payload?.competitiveSet?.topObservedAlternative?.name ||
    payload?.competitiveSet?.topObservedAlternative ||
    null;
  return {
    considerationRate: payload?.executiveMetrics?.considerationRate?.rate ?? null,
    scenarioPresence: payload?.executiveMetrics?.scenarioPresence?.rate ?? null,
    demandCapture: payload?.demandCapture?.overallRate ?? null,
    topObservedAlternative: top,
    observedCompetitorCount: payload?.competitiveSet?.observedCount ?? null,
    displacementEntityCount: Object.keys(disp || {}).length,
    intercontinentalDisplacement: disp?.intercontinental_real_santo_domingo || 0,
  };
}

function reextractCompetitorsOnPeriod(period, profile) {
  const propertyId = profile.propertyId;
  const ts = new Date().toISOString();
  let obsChanged = 0;
  let addedCanonical = 0;
  let removedCanonical = 0;
  let phraseJunkRemoved = 0;

  const observations = (period.observations || []).map((obs) => {
    if (!obs?.rawResponse) return obs;
    const before = obs.competitorsMentioned || [];
    const after = extractAndResolveCompetitors(obs.rawResponse, profile);
    const beforeCanon = canonSet(before, propertyId);
    const afterCanon = canonSet(after, propertyId);
    const added = setDiff(afterCanon, beforeCanon);
    const removed = setDiff(beforeCanon, afterCanon);
    const junkBefore = before.filter((n) =>
      /^(want|travelers?|looking|a massive|historically)\b/i.test(String(n || ""))
    ).length;

    const same =
      before.length === after.length &&
      before.every((n, i) => String(n) === String(after[i]));
    if (same && added.length === 0 && removed.length === 0) return obs;

    obsChanged += 1;
    addedCanonical += added.length;
    removedCanonical += removed.length;
    phraseJunkRemoved += junkBefore;

    return {
      ...obs,
      rawResponse: obs.rawResponse,
      mentioned: obs.mentioned,
      position: obs.position,
      context: obs.context,
      governedInterpretation: obs.governedInterpretation,
      originalParse:
        obs.originalParse ||
        {
          mentioned: obs.mentioned,
          position: obs.position,
          competitorsMentioned: before,
          snapshotAt: ts,
        },
      competitorsMentioned: after,
      competitorExtractReprocessAt: ts,
      competitorExtractReprocessVersion: VERSION,
      parsed: true,
    };
  });

  return {
    period: {
      ...period,
      observations,
      competitorExtractReprocessAt: ts,
      competitorExtractReprocessVersion: VERSION,
      forensicClientReadyStatus: CLIENT_READY_PENDING_FORENSIC_REVALIDATION,
      externalDistributionHold: EXTERNAL_DISTRIBUTION_HOLD,
    },
    stats: { obsChanged, addedCanonical, removedCanonical, phraseJunkRemoved },
  };
}

const universe = resolveGovernedAdpPropertyUniverseV1();
const targets = universe.properties
  .filter((p) => !PROPERTY_FILTER || PROPERTY_FILTER.includes(p.propertyId))
  .filter((p) => p.currentCertifiedPeriodId)
  .map((p) => ({ propertyId: p.propertyId, periodId: p.currentCertifiedPeriodId }));

const results = [];

for (const row of targets) {
  const { propertyId, periodId } = row;
  const period = loadPeriod(periodId);
  const profile = loadPropertyProfile(propertyId);
  if (!period || !profile) {
    results.push({ propertyId, periodId, ok: false, error: "MISSING_PERIOD_OR_PROFILE" });
    continue;
  }

  const beforeManifest = loadPublishedManifest(propertyId);
  const beforeReport = loadPublishedReport(propertyId);
  const beforePayload = beforeReport?.payload || beforeReport;
  const beforeMetrics = metricSnap(beforePayload);
  const scenarios = buildScenarioUniverse(profile);

  const { period: reprocessed, stats } = reextractCompetitorsOnPeriod(period, profile);
  const afterMetrics = afterMetricsFromPeriod(reprocessed, profile, scenarios);

  const metricsChanged =
    beforeMetrics.considerationRate !== afterMetrics.considerationRate ||
    beforeMetrics.scenarioPresence !== afterMetrics.scenarioPresence ||
    beforeMetrics.demandCapture !== afterMetrics.demandCapture ||
    String(beforeMetrics.topObservedAlternative || "") !==
      String(afterMetrics.topObservedAlternative || "");

  const competitiveChanged =
    stats.obsChanged > 0 ||
    beforeMetrics.topObservedAlternative !== afterMetrics.topObservedAlternative ||
    (beforePayload?.competitiveSet?.observedCount ?? null) !== afterMetrics.observedCompetitorCount;

  const out = {
    propertyId,
    periodId,
    dryRun: DRY,
    stats,
    beforeMetrics,
    afterMetrics,
    metricsChanged,
    competitiveChanged,
  };

  if (DRY) {
    out.ok = true;
    results.push(out);
    continue;
  }

  mkdirSync(BACKUP_ROOT, { recursive: true });
  const pubDir = join(process.cwd(), "data/ai-demand-positioning/published", propertyId);
  if (existsSync(pubDir)) {
    const dest = join(BACKUP_ROOT, propertyId);
    mkdirSync(dest, { recursive: true });
    for (const name of ["manifest.json", beforeManifest?.reportFile, beforeManifest?.evidenceFile].filter(
      Boolean
    )) {
      const src = join(pubDir, name);
      if (existsSync(src)) copyFileSync(src, join(dest, name));
    }
  }
  const runtimeSrc = join(process.cwd(), "data/ai-demand-positioning/runtime", `${periodId}.json`);
  if (existsSync(runtimeSrc)) {
    mkdirSync(join(BACKUP_ROOT, "runtime"), { recursive: true });
    copyFileSync(runtimeSrc, join(BACKUP_ROOT, "runtime", `${periodId}.json`));
  }

  savePeriod(reprocessed);

  let correctionId = null;
  if (metricsChanged || competitiveChanged) {
    const corr = buildCertifiedPeriodCorrectionRecord({
      propertyId,
      periodId,
      rootCause:
        "FORENSIC 2026-09-10 — competitor extract QC: registry alias scan + prose-fragment rejection; rewrite competitorsMentioned from rawResponse; rebuild competitive/displacement aggregates. Methodology unchanged.",
      evidence: { beforeMetrics, afterMetrics, stats },
      affectedMetrics: [
        "Competitive Set",
        "Displacement",
        "Top Observed AI Alternative",
        "Demand Capture (if unchanged note EXACT)",
        "AI Consideration (subject path unchanged)",
      ],
      oldValues: beforeMetrics,
      newValues: afterMetrics,
      supersededVersion:
        beforeManifest?.clientReadinessCorrectionVersion || "ORIGINAL_CERTIFIED_PUBLISH",
      correctedVersion: VERSION,
      clientExposureStatus: CLIENT_READY_PENDING_FORENSIC_REVALIDATION,
      externalShareDistributed: false,
    });
    writeCertifiedPeriodCorrection(corr);
    correctionId = corr.correctionId;
  }

  const bundle = buildPublishedSnapshotBundle({ period: reprocessed, profile });
  if (!bundle.ok) {
    out.ok = false;
    out.error = bundle.message || "BUNDLE_FAILED";
    results.push(out);
    continue;
  }

  Object.assign(bundle.manifest, {
    certified: beforeManifest?.certified ?? true,
    certificationStatus: beforeManifest?.certificationStatus || "CERTIFIED",
    publishStatus: beforeManifest?.publishStatus || "Live",
    measurementContractVersion:
      beforeManifest?.measurementContractVersion || MEASUREMENT_CONTRACT_VERSION,
    measurementContractHash: beforeManifest?.measurementContractHash,
    baselineMarker: beforeManifest?.baselineMarker,
    officialPeriod: beforeManifest?.officialPeriod,
    firstOfficialPropertyPeriod: beforeManifest?.firstOfficialPropertyPeriod,
    censusRecordId: beforeManifest?.censusRecordId || bundle.manifest.censusRecordId,
    clientReadinessCorrectionVersion: VERSION,
    clientReadinessCorrectionAt: new Date().toISOString(),
    supersedesCorrectionId: correctionId,
    forensicClientReadyStatus: CLIENT_READY_PENDING_FORENSIC_REVALIDATION,
    externalDistributionHold: EXTERNAL_DISTRIBUTION_HOLD,
  });

  const saved = savePublishedSnapshotBundle(bundle, { seed: false });
  const afterPub = loadPublishedReport(propertyId);
  out.ok = Boolean(saved?.ok);
  out.publishedMetrics = metricSnap(afterPub?.payload || afterPub);
  out.correctionId = correctionId;
  results.push(out);
}

const summary = {
  ok: results.every((r) => r.ok !== false),
  mode: DRY ? "dry-run" : "apply",
  version: VERSION,
  stamp,
  LIVE_PROVIDER_CALLS: 0,
  METHODOLOGY_CHANGED: false,
  EXTERNAL_DISTRIBUTION_HOLD: true,
  propertyFilter: PROPERTY_FILTER,
  targetCount: targets.length,
  results,
  totals: {
    obsChanged: results.reduce((s, r) => s + (r.stats?.obsChanged || 0), 0),
    addedCanonical: results.reduce((s, r) => s + (r.stats?.addedCanonical || 0), 0),
    removedCanonical: results.reduce((s, r) => s + (r.stats?.removedCanonical || 0), 0),
    phraseJunkRemoved: results.reduce((s, r) => s + (r.stats?.phraseJunkRemoved || 0), 0),
    metricsChangedProps: results.filter((r) => r.metricsChanged).length,
  },
};

writeFileSync(OUT, JSON.stringify(summary, null, 2) + "\n");
writeFileSync(LATEST, JSON.stringify(summary, null, 2) + "\n");
console.log(JSON.stringify(summary, null, 2));
if (!summary.ok) process.exit(1);
