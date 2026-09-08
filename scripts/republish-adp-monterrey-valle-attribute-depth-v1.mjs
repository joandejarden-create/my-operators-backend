#!/usr/bin/env node
/**
 * Reparse + republish Monterrey Valle ADP snapshots after attribute dictionary expansion.
 *
 * No new LLM calls. Forces observation reparse so Reality Gap picks up newly governed
 * attributes. Existing share tokens (reportScope=current_published) read the updated
 * published snapshot after deploy.
 *
 *   node scripts/republish-adp-monterrey-valle-attribute-depth-v1.mjs --dry-run
 *   node scripts/republish-adp-monterrey-valle-attribute-depth-v1.mjs --apply
 */
import "../load-env.js";
import { copyFileSync, existsSync, mkdirSync, readFileSync, writeFileSync } from "fs";
import { join } from "path";
import { loadPeriod, savePeriod, loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import { parseObservation } from "../lib/ai-demand-positioning/execution/response-parser.js";
import { computeRealityGap } from "../lib/ai-demand-positioning/intelligence/reality-gap.js";
import {
  buildPublishedSnapshotBundle,
  savePublishedSnapshotBundle,
  loadPublishedManifest,
  loadPublishedReport,
} from "../lib/ai-demand-positioning/published-snapshot.js";
import {
  MONTERREY_VALLE_PROPERTY_IDS,
  MONTERREY_VALLE_BASELINE_MARKERS,
  loadFrozenContractHash,
} from "../lib/ai-demand-positioning/execution/monterrey-valle-baseline-period-001-v1.js";
import { MEASUREMENT_CONTRACT_VERSION } from "../lib/ai-demand-positioning/contracts/adp-measurement-contract-v1.js";

const APPLY = process.argv.includes("--apply");
const DRY = !APPLY;

const PERIODS = {
  adp_jw_marriott_monterrey_valle:
    "adp_period_adp_jw_marriott_monterrey_valle_20260908010206_4d363a",
  adp_westin_monterrey_valle: "adp_period_adp_westin_monterrey_valle_20260908013810_b5bb91",
};

const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
const BACKUP_ROOT = join(
  process.cwd(),
  "reports/ai-demand-positioning/monterrey-attribute-depth-republish-backup",
  stamp
);
const REPORT_OUT = join(
  process.cwd(),
  "reports/ai-demand-positioning",
  `adp-monterrey-valle-attribute-depth-republish-${stamp}.json`
);

function forceReparsePeriod(period, profile) {
  const next = {
    ...period,
    observations: (period.observations || []).map((obs) =>
      parseObservation({ ...obs, parsed: false }, profile)
    ),
  };
  next.status = "PARSED";
  next.attributeDepthRepublishAt = new Date().toISOString();
  next.attributeDepthRepublishVersion = "monterrey_valle_attribute_depth_v1";
  return next;
}

function summarizeReality(profile, period) {
  const mentioned = (period.observations || []).filter((o) => o.mentioned);
  const rg = computeRealityGap(mentioned, profile);
  return {
    totalAttributes: rg.totalAttributes,
    recognizedCount: rg.recognizedCount,
    gapCount: rg.gapCount,
    gapScore: rg.gapScore,
    recognized: (rg.recognized || []).map((r) => r.attribute),
    gaps: (rg.gaps || []).map((g) => g.attribute),
  };
}

const hash = loadFrozenContractHash();
const results = [];

for (const propertyId of MONTERREY_VALLE_PROPERTY_IDS) {
  const periodId = PERIODS[propertyId];
  const period = loadPeriod(periodId);
  if (!period) {
    results.push({ propertyId, periodId, ok: false, error: "PERIOD_NOT_FOUND" });
    continue;
  }
  const profile = loadPropertyProfile(propertyId);
  if (!profile) {
    results.push({ propertyId, periodId, ok: false, error: "PROFILE_NOT_FOUND" });
    continue;
  }

  const beforeManifest = loadPublishedManifest(propertyId);
  const beforeReport = loadPublishedReport(propertyId);
  const beforeRg = beforeReport?.payload?.realityGap || beforeReport?.realityGap || null;

  const reparsed = forceReparsePeriod(period, profile);
  const afterRgPreview = summarizeReality(profile, reparsed);

  const row = {
    propertyId,
    periodId,
    profileAttributeCount: (profile.attributes || []).length,
    beforeTotalAttributes: beforeRg?.totalAttributes ?? null,
    afterTotalAttributes: afterRgPreview.totalAttributes,
    beforeRecognizedCount: beforeRg?.recognizedCount ?? null,
    afterRecognizedCount: afterRgPreview.recognizedCount,
    afterGaps: afterRgPreview.gaps,
    afterRecognized: afterRgPreview.recognized,
    dryRun: DRY,
  };

  if (afterRgPreview.totalAttributes < 12) {
    row.ok = false;
    row.error = `ATTRIBUTE_DEPTH_TOO_LOW:${afterRgPreview.totalAttributes}`;
    results.push(row);
    continue;
  }

  if (DRY) {
    row.ok = true;
    results.push(row);
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

  savePeriod(reparsed);

  const baselineMarker = MONTERREY_VALLE_BASELINE_MARKERS[propertyId];
  const bundle = buildPublishedSnapshotBundle({ period: reparsed, profile });
  if (!bundle.ok) {
    row.ok = false;
    row.error = bundle.message || "BUNDLE_FAILED";
    results.push(row);
    continue;
  }

  bundle.manifest.officialPeriod = true;
  bundle.manifest.baselineMarker = baselineMarker;
  bundle.manifest.firstOfficialPropertyPeriod = true;
  bundle.manifest.measurementContractVersion = MEASUREMENT_CONTRACT_VERSION;
  bundle.manifest.measurementContractHash = hash;
  bundle.manifest.certified = true;
  bundle.manifest.certificationStatus =
    beforeManifest?.certificationStatus || "CERTIFIED";
  bundle.manifest.publishStatus = beforeManifest?.publishStatus || "Live";
  bundle.manifest.censusRecordId = beforeManifest?.censusRecordId || bundle.manifest.censusRecordId;
  bundle.manifest.attributeDepthRepublishVersion = "monterrey_valle_attribute_depth_v1";
  bundle.manifest.attributeDepthRepublishAt = new Date().toISOString();

  if (bundle.report?.payload?.realityGap) {
    /* payload already rebuilt via buildOwnerPayload */
  }

  const saved = savePublishedSnapshotBundle(bundle, { seed: false });
  const publishedAfter = loadPublishedReport(propertyId);
  const afterRg =
    publishedAfter?.payload?.realityGap || publishedAfter?.realityGap || null;

  row.ok = Boolean(saved?.ok);
  row.publishedDir = saved?.dir || null;
  row.publishedTotalAttributes = afterRg?.totalAttributes ?? null;
  row.publishedRecognizedCount = afterRg?.recognizedCount ?? null;
  results.push(row);
}

const summary = {
  ok: results.every((r) => r.ok),
  mode: DRY ? "dry-run" : "apply",
  stamp,
  backupRoot: DRY ? null : BACKUP_ROOT,
  results,
};

writeFileSync(REPORT_OUT, JSON.stringify(summary, null, 2) + "\n");
console.log(JSON.stringify(summary, null, 2));
if (!summary.ok) process.exit(1);
