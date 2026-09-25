#!/usr/bin/env node
/**
 * W Rome ADP baseline completion rebuild — NO new LLM calls.
 *
 * Replays owner-payload + published snapshot from the existing certified period
 * after entity-registry / attribute / affiliation repairs.
 *
 *   node scripts/rebuild-adp-w-rome-from-existing-responses-v1.mjs --dry-run
 *   node scripts/rebuild-adp-w-rome-from-existing-responses-v1.mjs --apply
 *
 * Doctrine: METHODOLOGY_IS_GOVERNED; QUALITY_CONTROLS_LEARN.
 * This is a derived rebuild of period adp_period_adp_w_rome_20260925105322_1192cf,
 * not a new monitoring run.
 */
import "../load-env.js";
import { copyFileSync, existsSync, mkdirSync, writeFileSync } from "fs";
import { join } from "path";
import { loadPeriod, savePeriod, loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import { parseObservation } from "../lib/ai-demand-positioning/execution/response-parser.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { buildOwnerPayload } from "../lib/ai-demand-positioning/customer/owner-payload.js";
import { computeCompetitiveSet } from "../lib/ai-demand-positioning/intelligence/competitive-set.js";
import { computeRealityGap } from "../lib/ai-demand-positioning/intelligence/reality-gap.js";
import { computeDisplacementCountsByEntity } from "../lib/ai-demand-positioning/customer/resolve-displacement-evidence-v1.js";
import { resolveCustomerFacingEntity } from "../lib/ai-demand-positioning/customer/customer-entity-resolution-v1.js";
import { getEntityRegistryForProperty } from "../lib/ai-demand-positioning/metrics/adp-property-entity-registries.js";
import { getPortfolioMapping } from "../lib/ai-demand-positioning/brand-portfolio/brand-portfolio-affiliation-mapping-v1.js";
import {
  buildPublishedSnapshotBundle,
  savePublishedSnapshotBundle,
  loadPublishedManifest,
  loadPublishedReport,
} from "../lib/ai-demand-positioning/published-snapshot.js";

const APPLY = process.argv.includes("--apply");
const PROPERTY_ID = "adp_w_rome";
const PERIOD_ID = "adp_period_adp_w_rome_20260925105322_1192cf";
const REBUILD_VERSION = "adp_w_rome_baseline_completion_rebuild_v1";
const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);

const BACKUP_ROOT = join(
  process.cwd(),
  "reports/ai-demand-positioning/w-rome-baseline-completion-rebuild-backup",
  stamp
);
const REPORT_OUT = join(
  process.cwd(),
  "reports/ai-demand-positioning",
  `adp-w-rome-baseline-completion-rebuild-${stamp}.json`
);

function forceReparsePeriod(period, profile) {
  return {
    ...period,
    observations: (period.observations || []).map((obs) =>
      parseObservation({ ...obs, parsed: false }, profile)
    ),
    status: "PARSED",
    rebuildVersion: REBUILD_VERSION,
    rebuildAt: new Date().toISOString(),
    rebuildSourcePeriodId: PERIOD_ID,
    rebuildNote: "Derived rebuild from existing 80 responses — no new LLM calls",
  };
}

function summarize(profile, period, scenarios) {
  const obs = period.observations || [];
  const cs = computeCompetitiveSet(obs, profile);
  const rg = computeRealityGap(
    obs.filter((o) => o.mentioned),
    profile
  );
  const disp = computeDisplacementCountsByEntity(obs, scenarios, profile);
  const reasons = {};
  let bound = 0;
  let unbound = 0;
  for (const o of obs) {
    for (const c of o.competitorsMentioned || []) {
      const r = resolveCustomerFacingEntity(c, profile);
      if (r.ok) bound += 1;
      else {
        unbound += 1;
        reasons[r.reason] = (reasons[r.reason] || 0) + 1;
      }
    }
  }
  const payload = buildOwnerPayload(period, scenarios, profile);
  return {
    observations: obs.length,
    registryPresent: !!getEntityRegistryForProperty(PROPERTY_ID),
    portfolioMapped: !!getPortfolioMapping(PROPERTY_ID),
    competitorMentionsBound: bound,
    competitorMentionsUnbound: unbound,
    unboundReasons: reasons,
    competitiveSetObserved: cs.observedCount,
    competitiveSetTop: (cs.observed || []).slice(0, 10).map((r) => ({
      name: r.name,
      mentions: r.mentions,
      scenarioCount: r.scenarioCount,
      entityId: r.entityId,
    })),
    topObservedAlternative: payload.competitiveSet?.topObservedAlternative || null,
    displacementEntities: Object.keys(disp || {}).length,
    displacementTop: Object.entries(disp || {})
      .sort((a, b) => b[1] - a[1])
      .slice(0, 8)
      .map(([entityId, count]) => ({ entityId, count })),
    lostDemandDisplacement: (payload.lostDemand?.displacement || []).slice(0, 8),
    reality: {
      totalAttributes: rg.totalAttributes,
      recognizedCount: rg.recognizedCount,
      gapCount: rg.gapCount,
      gapScore: rg.gapScore,
      recognized: (rg.recognized || []).map((r) => r.attribute),
      gaps: (rg.gaps || []).map((g) => g.attribute),
    },
    actions: (payload.actions || []).map((a) => a.title || a.id),
    competitorPresentScenarios:
      payload.executiveMetrics?.competitorPresentScenarios ?? null,
  };
}

const profile = loadPropertyProfile(PROPERTY_ID);
if (!profile) {
  console.error("Missing profile for", PROPERTY_ID);
  process.exit(1);
}
const period = loadPeriod(PERIOD_ID);
if (!period) {
  console.error("Missing period", PERIOD_ID);
  process.exit(1);
}
const scenarios = buildScenarioUniverse(profile);
const before = summarize(profile, period, scenarios);
const rebuilt = forceReparsePeriod(period, profile);
const after = summarize(profile, rebuilt, scenarios);

const report = {
  ok: true,
  apply: APPLY,
  rebuildVersion: REBUILD_VERSION,
  sourcePeriodId: PERIOD_ID,
  propertyId: PROPERTY_ID,
  newLlmCalls: 0,
  before,
  after,
  timestamp: new Date().toISOString(),
};

if (APPLY) {
  mkdirSync(BACKUP_ROOT, { recursive: true });
  const runtimePath = join(
    process.cwd(),
    "data/ai-demand-positioning/runtime",
    `${PERIOD_ID}.json`
  );
  const pubDir = join(process.cwd(), "data/ai-demand-positioning/published", PROPERTY_ID);
  if (existsSync(runtimePath)) {
    copyFileSync(runtimePath, join(BACKUP_ROOT, `${PERIOD_ID}.json`));
  }
  if (existsSync(join(pubDir, "manifest.json"))) {
    copyFileSync(join(pubDir, "manifest.json"), join(BACKUP_ROOT, "manifest.json"));
  }
  const priorReport = loadPublishedReport(PROPERTY_ID);
  if (priorReport) {
    writeFileSync(
      join(BACKUP_ROOT, "prior-published-report.json"),
      JSON.stringify(priorReport, null, 2)
    );
  }

  savePeriod(rebuilt);
  const bundle = buildPublishedSnapshotBundle({
    period: rebuilt,
    profile,
    censusRecordId: profile.censusRecordId || null,
  });
  if (!bundle.ok) {
    console.error("Publish bundle failed", bundle);
    process.exit(1);
  }
  savePublishedSnapshotBundle(bundle);
  const publishedReport = bundle.report?.payload || bundle.report;
  report.published = {
    periodId: bundle.manifest?.latestPeriodId,
    demandCaptureRate: bundle.manifest?.demandCaptureRate,
    censusRecordId: bundle.manifest?.censusRecordId,
    observedCount: publishedReport?.competitiveSet?.observedCount ?? null,
    displacementCount: publishedReport?.lostDemand?.displacement?.length ?? null,
    realityAttrs: publishedReport?.realityGap?.totalAttributes ?? null,
  };
  report.manifest = loadPublishedManifest(PROPERTY_ID);
}

writeFileSync(REPORT_OUT, JSON.stringify(report, null, 2));
console.log(JSON.stringify({
  apply: APPLY,
  report: REPORT_OUT,
  before: {
    observed: before.competitiveSetObserved,
    displacement: before.displacementEntities,
    realityAttrs: before.reality.totalAttributes,
    actions: before.actions.length,
  },
  after: {
    observed: after.competitiveSetObserved,
    displacement: after.displacementEntities,
    realityAttrs: after.reality.totalAttributes,
    recognized: after.reality.recognized,
    gaps: after.reality.gaps,
    topAlt: after.topObservedAlternative,
    actions: after.actions.length,
    portfolioMapped: after.portfolioMapped,
  },
}, null, 2));

if (!APPLY) {
  console.log("\nDry-run only. Re-run with --apply to write period + published snapshot.");
}
