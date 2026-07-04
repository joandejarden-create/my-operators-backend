/**
 * Build Mexico Cancún delta demand anchor candidates (net-new / corrected only).
 */

import { readFileSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { normalizeAnchorName, nameSimilarity, coordsWithinTolerance } from "../demand-anchors/import-validation.js";
import { getMexicoCancunCandidates } from "./mexico-cancun-demand-anchors-candidates.js";
import { MEXICO_CANCUN_EXCLUDED_CLASSIFICATIONS } from "./mexico-cancun-excluded-record-classification.js";
import { applyMexicoCancunPlaceReviewCorrections } from "./mexico-cancun-google-place-review-corrections.js";
import { MEXICO_RADAR_REGION } from "./mexico-radar-region.js";

const MARKET = "Cancún / Riviera Maya";

function loadImportedKeys(dedupAuditPath) {
  if (!dedupAuditPath || !existsSync(dedupAuditPath)) return new Set();
  const audit = JSON.parse(readFileSync(dedupAuditPath, "utf8"));
  return new Set(audit.importedNameKeys || []);
}

function loadImportedRecords(dedupAuditPath) {
  if (!dedupAuditPath || !existsSync(dedupAuditPath)) return [];
  const audit = JSON.parse(readFileSync(dedupAuditPath, "utf8"));
  const names = new Set(audit.importedNameKeys || []);
  const safe = audit.safeToKeep || [];
  const fromDupes = [];
  for (const d of audit.definiteDuplicates || []) {
    fromDupes.push(d.recordA, d.recordB);
  }
  for (const d of audit.possibleDuplicates || []) {
    fromDupes.push(d.recordA, d.recordB);
  }
  const all = [...safe, ...fromDupes];
  const seen = new Set();
  const out = [];
  for (const r of all) {
    const key = normalizeAnchorName(r.name);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(r);
  }
  return out;
}

function isAlreadyImported(point, importedRecords, importedKeys) {
  const key = normalizeAnchorName(point.name);
  if (importedKeys.has(key)) return true;
  for (const ex of importedRecords) {
    const existKey = normalizeAnchorName(ex.name);
    if (key && existKey && key === existKey) return true;
    if (
      coordsWithinTolerance(point.latitude, point.longitude, ex.latitude, ex.longitude) &&
      String(point.pointType || "") === String(ex.pointType || "")
    ) {
      return true;
    }
    const sim = nameSimilarity(point.name, ex.name);
    if (
      sim >= 0.92 &&
      String(point.city || "").toLowerCase() === String(ex.city || "").toLowerCase() &&
      String(point.pointType || "") === String(ex.pointType || "")
    ) {
      return true;
    }
  }
  return false;
}

/**
 * @param {object} [options]
 * @param {string} [options.dedupAuditPath]
 */
export function buildMexicoCancunDeltaCandidates(options = {}) {
  const root = join(dirname(fileURLToPath(import.meta.url)), "../..");
  const dedupAuditPath =
    options.dedupAuditPath || join(root, "data/mexico-cancun-demand-anchor-dedup-audit.json");

  const importedKeys = loadImportedKeys(dedupAuditPath);
  const importedRecords = loadImportedRecords(dedupAuditPath);

  const deltaNames = new Set(
    MEXICO_CANCUN_EXCLUDED_CLASSIFICATIONS.filter((r) => r.includeInDeltaFixture).map(
      (r) => r.candidateName
    )
  );

  const allCandidates = getMexicoCancunCandidates("all");
  const selected = allCandidates.filter((p) => deltaNames.has(p.name));
  const corrected = applyMexicoCancunPlaceReviewCorrections(selected);

  const points = [];
  const skipped = [];

  for (const point of corrected) {
    if (isAlreadyImported(point, importedRecords, importedKeys)) {
      skipped.push({ name: point.name, reason: "already_imported_or_duplicate" });
      continue;
    }
    const { googleSearchQuery, manuallyVerified, ...clean } = point;
    points.push(clean);
  }

  const bySubmarket = {};
  const byPointType = {};
  for (const p of points) {
    bySubmarket[p.submarket] = (bySubmarket[p.submarket] || 0) + 1;
    byPointType[p.pointType] = (byPointType[p.pointType] || 0) + 1;
  }

  return {
    country: "Mexico",
    region: MEXICO_RADAR_REGION,
    market: MARKET,
    buildBatch: "delta",
    status: "candidate_pre_verification",
    generatedAt: new Date().toISOString(),
    summary: {
      deltaCandidateTarget: deltaNames.size,
      selectedFromSource: selected.length,
      netNewCandidates: points.length,
      skippedAlreadyImported: skipped.length,
      bySubmarket,
      byPointType,
    },
    skipped,
    points: corrected.filter((p) => !skipped.some((s) => s.name === p.name)),
  };
}

/**
 * Strip google-only fields before writing public fixture.
 * @param {object[]} points
 */
export function stripGoogleFieldsFromPoints(points) {
  return (points || []).map((p) => {
    const { googleSearchQuery, manuallyVerified, ...rest } = p;
    return rest;
  });
}
