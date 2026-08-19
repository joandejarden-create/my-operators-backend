/**
 * AI Demand Positioning — Published snapshot storage (pre-computed UI payloads).
 * Raw observation corpora stay internal; production serves published snapshots only.
 */

import { readFileSync, writeFileSync, existsSync, mkdirSync, readdirSync } from "fs";
import { join, dirname } from "path";
import { buildOwnerPayload, ADP_PRODUCT_VERSION } from "./customer/owner-payload.js";
import { buildEvidenceIndex } from "./customer/evidence-index.js";
import { buildScenarioUniverse } from "./prompt-universe/scenario-registry.js";
import { loadAllPeriods, loadPropertyProfile } from "./data-model.js";

const PUBLISHED_DIR = join(process.cwd(), "data/ai-demand-positioning/published");
const PUBLISHED_SEED_DIR = join(process.cwd(), "fixtures/ai-demand-positioning/published");

function propertyDir(propertyId, useSeed = false) {
  return join(useSeed ? PUBLISHED_SEED_DIR : PUBLISHED_DIR, propertyId);
}

function manifestPath(propertyId, useSeed = false) {
  return join(propertyDir(propertyId, useSeed), "manifest.json");
}

function reportPath(propertyId, periodId, useSeed = false) {
  return join(propertyDir(propertyId, useSeed), `report-${periodId}.json`);
}

function evidencePath(propertyId, periodId, useSeed = false) {
  return join(propertyDir(propertyId, useSeed), `evidence-${periodId}.json`);
}

function readJson(path) {
  if (!existsSync(path)) return null;
  return JSON.parse(readFileSync(path, "utf-8"));
}

function writeJson(path, data) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, JSON.stringify(data, null, 2));
}

function buildTrends(propertyId, scenarios, periods) {
  if (!periods || periods.length <= 1) return undefined;
  return periods.map((p) => {
    const profile = loadPropertyProfile(propertyId);
    const pPayload = buildOwnerPayload(p, scenarios, profile);
    return {
      periodId: p.periodId,
      date: p.executionDate,
      demandCaptureRate: pPayload.demandCapture ? pPayload.demandCapture.overallRate : null,
      providerCount: p.providers ? p.providers.length : p.providerCount || null,
      observationCount: p.observations ? p.observations.length : 0,
    };
  });
}

export function buildPublishedSnapshotBundle({ period, profile, censusRecordId = null }) {
  const scenarios = buildScenarioUniverse(profile);
  const payload = buildOwnerPayload(period, scenarios, profile);
  if (!payload.ok) {
    return { ok: false, error: payload.error, message: payload.message };
  }

  const allPeriods = loadAllPeriods(profile.propertyId);
  const trends = buildTrends(profile.propertyId, scenarios, allPeriods);
  if (trends) payload.trends = trends;

  const evidenceIndex = buildEvidenceIndex(period, scenarios);

  const publishedAt = new Date().toISOString();
  const report = {
    ok: true,
    publishedAt,
    productVersion: ADP_PRODUCT_VERSION,
    propertyId: profile.propertyId,
    periodId: period.periodId,
    censusRecordId: censusRecordId || null,
    payload,
  };

  const manifest = {
    propertyId: profile.propertyId,
    propertyName: profile.name,
    city: profile.city,
    state: profile.state,
    market: profile.market || null,
    latestPeriodId: period.periodId,
    latestPublishedAt: publishedAt,
    productVersion: ADP_PRODUCT_VERSION,
    censusRecordId: censusRecordId || null,
    publishStatus: "Live",
    reportFile: `report-${period.periodId}.json`,
    evidenceFile: `evidence-${period.periodId}.json`,
    demandCaptureRate: payload.demandCapture?.overallRate ?? null,
    providerCount: payload.period?.providerCount ?? null,
  };

  return {
    ok: true,
    manifest,
    report,
    evidenceIndex,
    summary: {
      propertyId: profile.propertyId,
      periodId: period.periodId,
      demandCapture: payload.demandCapture?.display || null,
      payloadBytes: JSON.stringify(payload).length,
      evidenceBytes: JSON.stringify(evidenceIndex).length,
    },
  };
}

export function savePublishedSnapshotBundle(bundle, { seed = false } = {}) {
  if (!bundle?.ok) {
    throw new Error(bundle?.message || "Cannot save invalid published snapshot bundle");
  }

  const dir = propertyDir(bundle.manifest.propertyId, seed);
  mkdirSync(dir, { recursive: true });

  const reportFile = reportPath(bundle.manifest.propertyId, bundle.manifest.latestPeriodId, seed);
  const evidenceFile = evidencePath(bundle.manifest.propertyId, bundle.manifest.latestPeriodId, seed);
  const manifestFile = manifestPath(bundle.manifest.propertyId, seed);

  writeJson(reportFile, bundle.report);
  writeJson(evidenceFile, bundle.evidenceIndex);
  writeJson(manifestFile, bundle.manifest);

  return {
    ok: true,
    dir,
    manifestFile,
    reportFile,
    evidenceFile,
    seed,
  };
}

function loadManifest(propertyId) {
  const runtime = readJson(manifestPath(propertyId, false));
  if (runtime) return { manifest: runtime, seed: false };
  const seeded = readJson(manifestPath(propertyId, true));
  if (seeded) return { manifest: seeded, seed: true };
  return null;
}

export function loadPublishedReport(propertyId) {
  const loaded = loadManifest(propertyId);
  if (!loaded) return null;

  const { manifest, seed } = loaded;
  const reportFile = join(propertyDir(propertyId, seed), manifest.reportFile || `report-${manifest.latestPeriodId}.json`);
  const report = readJson(reportFile);
  if (!report?.payload) return null;
  return report.payload;
}

export function loadPublishedEvidenceIndex(propertyId, periodId = null) {
  const loaded = loadManifest(propertyId);
  if (!loaded) return null;

  const { manifest, seed } = loaded;
  const targetPeriodId = periodId || manifest.latestPeriodId;
  const evidenceFile = join(
    propertyDir(propertyId, seed),
    manifest.evidenceFile || `evidence-${targetPeriodId}.json`
  );
  return readJson(evidenceFile);
}

export function listPublishedPropertyIds() {
  const ids = new Set();
  for (const root of [PUBLISHED_DIR, PUBLISHED_SEED_DIR]) {
    if (!existsSync(root)) continue;
    for (const entry of readdirSync(root, { withFileTypes: true })) {
      if (entry.isDirectory()) ids.add(entry.name);
    }
  }
  return [...ids];
}

export function loadPublishedManifest(propertyId) {
  return loadManifest(propertyId)?.manifest || null;
}
