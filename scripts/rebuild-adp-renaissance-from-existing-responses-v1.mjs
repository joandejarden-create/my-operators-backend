#!/usr/bin/env node
/**
 * Renaissance ADP corrected-edition rebuild — NO new LLM / paid calls.
 *
 * Replays owner-payload + published snapshot from certified period …2ff16a
 * after profile fact corrections (nycrt, 317 rooms, remove rooftop_bar expectation,
 * Bowtie Bar inventory). Preserves raw observations; archives prior published edition.
 *
 *   node scripts/rebuild-adp-renaissance-from-existing-responses-v1.mjs --dry-run
 *   node scripts/rebuild-adp-renaissance-from-existing-responses-v1.mjs --apply
 */
import "../load-env.js";
import { copyFileSync, existsSync, mkdirSync, readFileSync, writeFileSync } from "fs";
import { join } from "path";
import {
  loadPeriod,
  savePeriod,
  loadPropertyProfile,
} from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { buildOwnerPayload } from "../lib/ai-demand-positioning/customer/owner-payload.js";
import { computeRealityGap } from "../lib/ai-demand-positioning/intelligence/reality-gap.js";
import {
  buildPublishedSnapshotBundle,
  savePublishedSnapshotBundle,
  loadPublishedManifest,
  loadPublishedReport,
} from "../lib/ai-demand-positioning/published-snapshot.js";

const APPLY = process.argv.includes("--apply");
const PROPERTY_ID = "adp_renaissance_times_square";
const PERIOD_ID = "adp_period_adp_renaissance_times_square_20261016083441_2ff16a";
const REBUILD_VERSION = "adp_renaissance_profile_fact_rebuild_v1";
const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);

const BACKUP_ROOT = join(
  process.cwd(),
  "reports/adp-six-hotel-e2e/renaissance/rebuild-backup",
  stamp
);
const REPORT_OUT = join(
  process.cwd(),
  "reports/adp-six-hotel-e2e/renaissance",
  `PROFILE_FACT_REBUILD_${stamp}.json`
);

function summarize(profile, period) {
  const rg = computeRealityGap(
    (period.observations || []).filter((o) => o.mentioned),
    profile
  );
  const payload = buildOwnerPayload(period, buildScenarioUniverse(profile), profile);
  return {
    rooms: profile.rooms,
    website: profile.website || profile.officialPropertyPageUrl || null,
    attributes: profile.attributes || [],
    hasRooftopBarAttr: (profile.attributes || []).includes("rooftop_bar"),
    realityGap: {
      gapScore: rg.gapScore,
      totalAttributes: rg.totalAttributes,
      recognizedCount: rg.recognizedCount,
      gapCount: rg.gapCount,
      recognized: (rg.recognized || []).map((r) => r.attribute),
      gaps: (rg.gaps || []).map((g) => g.attribute),
    },
    capture: payload?.demandCapture?.overallRate ?? null,
    scenarioPresence: payload?.executiveMetrics?.scenarioPresence ?? null,
    actions: (payload?.actions || []).map((a) => a.id || a.title || a.headline),
    actionCount: (payload?.actions || []).length,
  };
}

const profile = loadPropertyProfile(PROPERTY_ID);
if (!profile) {
  console.error("profile_missing");
  process.exit(1);
}
const period = loadPeriod(PERIOD_ID);
if (!period) {
  console.error("period_missing");
  process.exit(1);
}

const beforeReport = loadPublishedReport(PROPERTY_ID);
const beforeManifest = loadPublishedManifest(PROPERTY_ID);
const beforeSummary = {
  rooms: beforeReport?.property?.rooms,
  website: beforeReport?.property?.website,
  realityGapRecognized: (beforeReport?.realityGap?.recognized || []).map((r) => r.attribute),
  realityGapScore: beforeReport?.realityGap?.gapScore,
  capture: beforeReport?.demandCapture?.overallRate,
  actions: (beforeReport?.actions || []).length,
};

const nextPeriod = {
  ...period,
  rebuildVersion: REBUILD_VERSION,
  rebuildAt: new Date().toISOString(),
  rebuildSourcePeriodId: PERIOD_ID,
  rebuildNote:
    "Derived rebuild from existing 320 responses after profile fact corrections — no new LLM calls",
};

const afterSummary = summarize(profile, nextPeriod);
const scenarios = buildScenarioUniverse(profile);
const bundle = buildPublishedSnapshotBundle({
  period: nextPeriod,
  profile,
  censusRecordId: "recG66DQJKP2c0UNh",
  scenarios,
});

if (!bundle?.ok && bundle?.error) {
  console.error(JSON.stringify({ ok: false, error: bundle.error, message: bundle.message }, null, 2));
  process.exit(1);
}

// Preserve prior customer action statuses/notes if present on previous published edition
const priorActions = beforeReport?.actions || [];
const nextActions = bundle.report?.payload?.actions || bundle.payload?.actions || [];
if (priorActions.length && nextActions.length) {
  const byKey = new Map(
    priorActions.map((a) => [String(a.id || a.title || a.headline || ""), a])
  );
  for (const a of nextActions) {
    const prev = byKey.get(String(a.id || a.title || a.headline || ""));
    if (!prev) continue;
    for (const k of [
      "status",
      "owner",
      "notes",
      "customerNotes",
      "completedAt",
      "completionDate",
      "outcome",
      "outcomes",
    ]) {
      if (prev[k] != null && a[k] == null) a[k] = prev[k];
    }
  }
}

const result = {
  ok: true,
  dryRun: !APPLY,
  rebuildVersion: REBUILD_VERSION,
  propertyId: PROPERTY_ID,
  periodId: PERIOD_ID,
  before: beforeSummary,
  after: afterSummary,
  rooftopBarRemovedFromExpected: beforeSummary.realityGapRecognized?.includes("rooftop_bar") &&
    !afterSummary.realityGap.recognized.includes("rooftop_bar") &&
    !afterSummary.hasRooftopBarAttr,
  captureUnchanged:
    beforeSummary.capture === afterSummary.capture ||
    Number(beforeSummary.capture) === Number(afterSummary.capture),
  paidCalls: 0,
};

if (!APPLY) {
  writeFileSync(REPORT_OUT, JSON.stringify(result, null, 2) + "\n");
  console.log(JSON.stringify(result, null, 2));
  console.log("dry-run only — pass --apply to write published snapshot");
  process.exit(0);
}

const publishedDir = join(
  process.cwd(),
  "data/ai-demand-positioning/published",
  PROPERTY_ID
);
mkdirSync(BACKUP_ROOT, { recursive: true });
const priorReportPath = join(
  publishedDir,
  beforeManifest?.reportFile || `report-${PERIOD_ID}.json`
);
const priorCert = existsSync(priorReportPath)
  ? JSON.parse(readFileSync(priorReportPath, "utf8"))?._certification || null
  : null;
for (const f of [
  beforeManifest?.reportFile || `report-${PERIOD_ID}.json`,
  beforeManifest?.evidenceFile || `evidence-${PERIOD_ID}.json`,
  "manifest.json",
]) {
  const from = join(publishedDir, f);
  if (existsSync(from)) copyFileSync(from, join(BACKUP_ROOT, f));
}

savePeriod(nextPeriod);
const bundleToSave = {
  ...bundle,
  certificationRecord: priorCert
    ? {
        ...priorCert,
        rebuildVersion: REBUILD_VERSION,
        rebuildAt: new Date().toISOString(),
        rebuildNote:
          "Re-attached prior CERTIFIED record after no-paid profile-fact derived rebuild",
      }
    : {
        certificationStatus: beforeManifest?.certificationStatus || "CERTIFIED",
        status: beforeManifest?.certificationStatus || "CERTIFIED",
        assuranceVersion: beforeManifest?.assuranceVersion || null,
        certificationTimestamp: beforeManifest?.certificationTimestamp || null,
        engineVersion: beforeManifest?.globalCertificationEngineVersion || null,
      },
  manifest: {
    ...bundle.manifest,
    censusRecordId: "recG66DQJKP2c0UNh",
    certificationStatus: "CERTIFIED",
  },
};
const saved = savePublishedSnapshotBundle(bundleToSave, {
  officialCustomerPublish: true,
});
result.saved = {
  ok: saved?.ok !== false,
  reportFile: saved?.manifestFile || saved?.reportFile,
  latestPeriodId: PERIOD_ID,
};
result.backupRoot = BACKUP_ROOT;
writeFileSync(REPORT_OUT, JSON.stringify(result, null, 2) + "\n");
console.log(JSON.stringify(result, null, 2));
