#!/usr/bin/env node
/**
 * Rebuild ADP_EXISTING_HOTEL_CERTIFICATION_BASELINE_V1.json from authoritative
 * filesystem published manifests (PRIMARY_SOT).
 *
 * Does NOT call paid APIs, does NOT write Airtable, does NOT change customer visibility.
 *
 * Usage:
 *   node scripts/rebuild-adp-existing-hotel-certification-baseline-v1.mjs --dry-run
 *   node scripts/rebuild-adp-existing-hotel-certification-baseline-v1.mjs --apply
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const PUBLISHED = path.join(ROOT, "data/ai-demand-positioning/published");
const OUT_DIR = path.join(ROOT, "reports/ai-demand-positioning/certification-baseline");
const OUT_FILE = path.join(OUT_DIR, "ADP_EXISTING_HOTEL_CERTIFICATION_BASELINE_V1.json");

const apply = process.argv.includes("--apply");
const dryRun = process.argv.includes("--dry-run") || !apply;

function listPeriodIds(propertyDir) {
  const files = fs.readdirSync(propertyDir);
  const periodIds = new Set();
  for (const f of files) {
    const m = f.match(/^report-(adp_period_[^.]+)\.json$/);
    if (m) periodIds.add(m[1]);
  }
  return [...periodIds].sort();
}

function rebuild() {
  const properties = [];
  const dirs = fs
    .readdirSync(PUBLISHED)
    .filter((d) => d.startsWith("adp_") && fs.statSync(path.join(PUBLISHED, d)).isDirectory())
    .sort();

  for (const propertyId of dirs) {
    const dir = path.join(PUBLISHED, propertyId);
    const manifestPath = path.join(dir, "manifest.json");
    if (!fs.existsSync(manifestPath)) continue;
    const manifest = JSON.parse(fs.readFileSync(manifestPath, "utf8"));
    const periods = listPeriodIds(dir);
    const latest = manifest.latestPeriodId || periods[periods.length - 1] || null;
    const priors = periods.filter((p) => p !== latest);
    const priorPeriodId = priors.length ? priors[priors.length - 1] : null;
    if (priorPeriodId && priorPeriodId === latest) {
      throw new Error(`prior equals current for ${propertyId}`);
    }
    properties.push({
      propertyId,
      propertyName: manifest.propertyName || null,
      censusRecordId: manifest.censusRecordId || null,
      periodId: latest,
      priorPeriodId,
      priorComparablePeriodId: priorPeriodId,
      publishStatus: manifest.publishStatus || null,
      certificationStatus: manifest.certificationStatus || null,
      measurementContractVersion: manifest.measurementContractVersion || null,
      source: "published_manifest_primary_sot",
    });
  }

  const doc = {
    schema: "ADP_EXISTING_HOTEL_CERTIFICATION_BASELINE_V1",
    version: "adp_existing_hotel_certification_baseline_v1_reconstructed",
    reconstructedAt: new Date().toISOString(),
    reconstructionNote:
      "Rebuilt from data/ai-demand-positioning/published/*/manifest.json because the original freeze artifact was missing from the tree. priorPeriodId = previous published report period when multiple exist. Does not certify hotels or change customer visibility.",
    authority: "filesystem_primary_sot_published_manifests",
    propertyCount: properties.length,
    properties,
  };

  return doc;
}

const doc = rebuild();
console.log(
  JSON.stringify(
    {
      dryRun,
      apply,
      outFile: OUT_FILE,
      propertyCount: doc.propertyCount,
      sample: doc.properties.slice(0, 3).map((p) => ({
        propertyId: p.propertyId,
        periodId: p.periodId,
        priorPeriodId: p.priorPeriodId,
      })),
    },
    null,
    2
  )
);

if (dryRun) {
  console.log("DRY_RUN — pass --apply to write baseline file");
  process.exit(0);
}

fs.mkdirSync(OUT_DIR, { recursive: true });
fs.writeFileSync(OUT_FILE, JSON.stringify(doc, null, 2) + "\n");
console.log("WROTE", OUT_FILE);
