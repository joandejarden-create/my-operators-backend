/**
 * Inventory raw ADP observation corpora — never treat PDFs/snapshots as raw evidence.
 */

import fs from "fs";
import path from "path";
import { createHash } from "crypto";
import { fileURLToPath } from "url";
import {
  loadLatestPeriod,
  loadAllPeriods,
  loadPropertyProfile,
} from "../data-model.js";
import { listAdpReviewerProperties } from "../review/list-adp-reviewer-properties-v1.js";

export const RAW_EVIDENCE_CLASSES = Object.freeze({
  COMPLETE_RAW_CORPUS: "COMPLETE_RAW_CORPUS",
  RECOVERABLE_RAW_CORPUS: "RECOVERABLE_RAW_CORPUS",
  PARTIAL_RAW_CORPUS: "PARTIAL_RAW_CORPUS",
  DERIVED_ARTIFACTS_ONLY: "DERIVED_ARTIFACTS_ONLY",
  NO_MONITORING_EVIDENCE: "NO_MONITORING_EVIDENCE",
});

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, "../../..");
const RUNTIME_DIR = path.join(REPO_ROOT, "data/ai-demand-positioning/runtime");
const SEED_DIR = path.join(REPO_ROOT, "fixtures/ai-demand-positioning/seed-periods");
const ARCHIVE_INDEX_DIR = path.join(
  REPO_ROOT,
  "data/ai-demand-positioning/observation-archive-index"
);

function shaFile(p) {
  try {
    return createHash("sha256").update(fs.readFileSync(p)).digest("hex").slice(0, 16);
  } catch {
    return null;
  }
}

function listPeriodFilesOnDisk(propertyId) {
  const out = [];
  for (const dir of [RUNTIME_DIR, SEED_DIR]) {
    if (!fs.existsSync(dir)) continue;
    for (const f of fs.readdirSync(dir)) {
      if (
        f.startsWith("adp_period_") &&
        f.includes(propertyId) &&
        f.endsWith(".json")
      ) {
        const abs = path.join(dir, f);
        let obsCount = 0;
        let periodId = null;
        try {
          const j = JSON.parse(fs.readFileSync(abs, "utf8"));
          obsCount = Array.isArray(j.observations) ? j.observations.length : 0;
          periodId = j.periodId || f.replace(/\.json$/, "");
        } catch {
          /* ignore */
        }
        out.push({
          path: abs,
          relative: path.relative(REPO_ROOT, abs),
          source: dir === RUNTIME_DIR ? "runtime" : "seed",
          periodId,
          observationCount: obsCount,
          hash: shaFile(abs),
        });
      }
    }
  }
  return out.sort((a, b) => a.relative.localeCompare(b.relative));
}

function publishedMeta(propertyId) {
  const dir = path.join(
    REPO_ROOT,
    "data/ai-demand-positioning/published",
    propertyId
  );
  if (!fs.existsSync(dir)) return { present: false };
  const manifestPath = path.join(dir, "manifest.json");
  const manifest = fs.existsSync(manifestPath)
    ? JSON.parse(fs.readFileSync(manifestPath, "utf8"))
    : null;
  const evidenceFiles = fs
    .readdirSync(dir)
    .filter((f) => f.startsWith("evidence-") && f.endsWith(".json"));
  let evidenceHasObservations = false;
  for (const f of evidenceFiles) {
    try {
      const j = JSON.parse(fs.readFileSync(path.join(dir, f), "utf8"));
      if (Array.isArray(j.observations) && j.observations.length > 0) {
        evidenceHasObservations = true;
      }
    } catch {
      /* ignore */
    }
  }
  return {
    present: true,
    latestPeriodId: manifest?.latestPeriodId || null,
    publishStatus: manifest?.publishStatus || null,
    evidenceFiles,
    evidenceHasObservations,
    files: fs.readdirSync(dir),
  };
}

/**
 * Persist durable index pointing at canonical runtime period files (not copies of corpora).
 */
export function writeObservationArchiveIndex(propertyId, inventoryRow) {
  fs.mkdirSync(ARCHIVE_INDEX_DIR, { recursive: true });
  const dir = path.join(ARCHIVE_INDEX_DIR, propertyId);
  fs.mkdirSync(dir, { recursive: true });
  const body = {
    propertyId,
    indexedAt: new Date().toISOString(),
    evidenceClass: inventoryRow.evidenceClass,
    periodFiles: inventoryRow.periodFiles,
    latestPeriodId: inventoryRow.latestPeriodId,
    observationCount: inventoryRow.observationCount,
    note:
      "Index only — raw observations remain in runtime/seed period JSON. Do not treat this index as observations.",
  };
  fs.writeFileSync(
    path.join(dir, "latest.json"),
    JSON.stringify(body, null, 2) + "\n",
    "utf8"
  );
  return body;
}

export function inventoryHotelRawEvidence(propertyId) {
  const profile = loadPropertyProfile(propertyId);
  const period = loadLatestPeriod(propertyId);
  const periods = loadAllPeriods(propertyId) || [];
  const periodFiles = listPeriodFilesOnDisk(propertyId);
  const published = publishedMeta(propertyId);
  const obsCount = period?.observations?.length || 0;
  const providers = new Set();
  const scenarios = new Set();
  for (const o of period?.observations || []) {
    if (o.provider) providers.add(String(o.provider).toLowerCase());
    if (o.scenarioId) scenarios.add(o.scenarioId);
  }

  let evidenceClass;
  if (obsCount > 0 && periodFiles.length > 0) {
    evidenceClass =
      periodFiles.some((f) => f.observationCount > 0)
        ? RAW_EVIDENCE_CLASSES.COMPLETE_RAW_CORPUS
        : RAW_EVIDENCE_CLASSES.RECOVERABLE_RAW_CORPUS;
  } else if (obsCount > 0 && periodFiles.length === 0) {
    evidenceClass = RAW_EVIDENCE_CLASSES.RECOVERABLE_RAW_CORPUS;
  } else if (published.evidenceHasObservations) {
    evidenceClass = RAW_EVIDENCE_CLASSES.RECOVERABLE_RAW_CORPUS;
  } else if (published.present && obsCount === 0) {
    evidenceClass = RAW_EVIDENCE_CLASSES.DERIVED_ARTIFACTS_ONLY;
  } else if (obsCount > 0) {
    evidenceClass = RAW_EVIDENCE_CLASSES.PARTIAL_RAW_CORPUS;
  } else {
    evidenceClass = RAW_EVIDENCE_CLASSES.NO_MONITORING_EVIDENCE;
  }

  // Expected count: from period metadata when present; else published-only → unknown
  const expectedObservationCount =
    period?.scenarioCount && period?.providerCount
      ? Number(period.scenarioCount) * Number(period.providerCount)
      : period?.observations?.length || null;

  return {
    propertyId,
    hotelName: profile?.name || propertyId,
    evidenceClass,
    latestPeriodId: period?.periodId || published.latestPeriodId || null,
    observationCount: obsCount,
    expectedObservationCount,
    periodCount: periods.length,
    periodIds: periods.map((p) => p.periodId).filter(Boolean),
    periodFiles,
    providerCoverage: [...providers].sort(),
    scenarioCoverageCount: scenarios.size,
    published,
    canIndependentlyReproduceMetrics: obsCount > 0,
    remonitorRequired: obsCount === 0 && !published.evidenceHasObservations,
  };
}

export function inventoryAllHotelsRawEvidence() {
  const props = listAdpReviewerProperties({
    includeExcluded: true,
    includeHistoricalExcluded: true,
  });
  const rows = [];
  const recoveredIndexes = [];
  for (const p of props) {
    const row = inventoryHotelRawEvidence(p.propertyId);
    rows.push(row);
    if (
      row.evidenceClass === RAW_EVIDENCE_CLASSES.COMPLETE_RAW_CORPUS ||
      row.evidenceClass === RAW_EVIDENCE_CLASSES.RECOVERABLE_RAW_CORPUS
    ) {
      recoveredIndexes.push(writeObservationArchiveIndex(p.propertyId, row));
    }
  }
  const byClass = {};
  for (const r of rows) {
    byClass[r.evidenceClass] = (byClass[r.evidenceClass] || 0) + 1;
  }
  return {
    generatedAt: new Date().toISOString(),
    hotelCount: rows.length,
    byClass,
    rows,
    recoveredIndexCount: recoveredIndexes.length,
  };
}
