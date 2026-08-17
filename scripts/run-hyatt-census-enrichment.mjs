#!/usr/bin/env node
/**
 * Hyatt CALA Hotel Census Phase 0: official sitemap/CDX → match → fill-blank Website + Property ID.
 *
 * Live hyatt.com is often 429/Kasada-blocked. Default extract uses local Wayback sitemap cache
 * (data/hyatt-sitemap-wayback-20240126.xml) plus optional Wayback CDX supplement.
 *
 *   node scripts/run-hyatt-census-enrichment.mjs
 *   node scripts/run-hyatt-census-enrichment.mjs --skip-cdx
 *   node scripts/run-hyatt-census-enrichment.mjs --apply
 *   node scripts/run-hyatt-census-enrichment.mjs --apply --probe-amenities --amenity-probe-limit=5
 *
 * Requires load-env (AIRTABLE_API_KEY, AIRTABLE_BASE_ID_ALT). Fill-blank only. No invented amenities.
 */
import "../load-env.js";
import { existsSync, mkdirSync, writeFileSync, appendFileSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import {
  DEFAULT_HYATT_DIRECTORY_JSON,
  hyattStewardReviewRows,
  planHyattCensusEnrichment,
} from "../lib/hotel-census/plan-hyatt-census-enrichment.js";
import {
  fetchHyattHotelAmenities,
  fetchHyattSitemapXml,
  harvestHyattCalaFromWaybackCdx,
  loadHyattSitemapFromFile,
  mergeHyattDirectoryRows,
  writeHyattDirectoryExtract,
} from "../lib/hyatt-brand-directory-extract.js";
import { HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";
import { CENSUS_PROPERTY_ID_FIELD } from "../lib/hotel-census/hilton-property-id-contract.js";
import { MAP_DIRECTORY_ENRICHMENT } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { writeCsv } from "../lib/str-census-import/report-utils.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORTS = join(ROOT, "reports");
const DATA = join(ROOT, "data");
const LOCAL_SITEMAP = join(DATA, "hyatt-sitemap-wayback-20240126.xml");
const DIRECTORY_JSON = join(REPORTS, "hyatt-cala-directory-extract.json");
const PLAN_JSON = join(REPORTS, "hyatt-census-enrichment-plan.json");
const STEWARD_CSV = join(REPORTS, "hyatt-census-steward-review.csv");
const UNMATCHED_CSV = join(REPORTS, "hyatt-census-unmatched.csv");
const APPLY_LOG = join(REPORTS, "hyatt-census-enrichment-apply-log.csv");
const AMENITY_JSON = join(REPORTS, "hyatt-amenities-probe.json");
const SUMMARY_JSON = join(REPORTS, "hyatt-census-enrichment-summary.json");
const BATCH = 10;

function parseArgs() {
  const args = process.argv.slice(2);
  const get = (flag, fallback = null) => {
    const eq = args.find((a) => a.startsWith(`${flag}=`));
    if (eq) return eq.slice(flag.length + 1);
    const i = args.indexOf(flag);
    return i >= 0 && args[i + 1] && !args[i + 1].startsWith("--") ? args[i + 1] : fallback;
  };
  return {
    apply: args.includes("--apply"),
    skipExtract: args.includes("--skip-extract"),
    skipCdx: args.includes("--skip-cdx"),
    forceExtract: args.includes("--force-extract"),
    probeAmenities: args.includes("--probe-amenities"),
    applyAmenities: args.includes("--apply-amenities"),
    amenityProbeLimit: Number(get("--amenity-probe-limit", "5")),
    delayMs: Number(get("--delay-ms", "1500")),
    cdxDelayMs: Number(get("--cdx-delay-ms", "1200")),
    minScore: Number(get("--min-score", "58")),
    minNameSim: Number(get("--min-name-sim", "0.45")),
    minConfidence: get("--min-confidence", "low"),
    includeLowOnApply: args.includes("--include-low"),
  };
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function appendApplyLog(row) {
  if (!existsSync(APPLY_LOG)) {
    appendFileSync(
      APPLY_LOG,
      "appliedAt,censusRecordId,censusName,propertyId,propertyUrl,matchScore,nameSim,confidence,fields\n"
    );
  }
  const fields = Object.keys(row.applyFields || {}).join("|");
  appendFileSync(
    APPLY_LOG,
    `${new Date().toISOString()},${row.censusRecordId},"${String(row.censusName || "").replace(/"/g, '""')}",${row.propertyId},${row.propertyUrl},${row.matchScore},${row.nameSim},${row.matchConfidence},${fields}\n`
  );
}

async function ensureDirectoryExtract(opts) {
  if (opts.skipExtract && existsSync(DIRECTORY_JSON) && !opts.forceExtract) {
    console.log("Using existing directory extract:", DIRECTORY_JSON);
    return JSON.parse(readFileSync(DIRECTORY_JSON, "utf8"));
  }

  console.log("=== Hyatt directory extract (official URLs) ===\n");

  /** @type {object[]} */
  let sitemapRows = [];
  /** @type {object} */
  let sitemapMeta = {};

  if (existsSync(LOCAL_SITEMAP)) {
    console.log("Loading local Wayback sitemap cache:", LOCAL_SITEMAP);
    const loaded = loadHyattSitemapFromFile(LOCAL_SITEMAP, { calaOnly: true });
    sitemapRows = loaded.propertyRows || [];
    sitemapMeta = { source: loaded.source, locCount: loaded.locCount, ok: loaded.ok };
    console.log("  CALA unique properties from sitemap:", sitemapRows.length);
  } else {
    console.log("No local sitemap cache; trying live then Wayback…");
    const fetched = await fetchHyattSitemapXml({ delayMs: opts.delayMs });
    sitemapMeta = { ok: fetched.ok, label: fetched.label, errors: fetched.errors };
    if (fetched.ok) {
      mkdirSync(DATA, { recursive: true });
      writeFileSync(LOCAL_SITEMAP, fetched.xml, "utf8");
      const loaded = loadHyattSitemapFromFile(LOCAL_SITEMAP, { calaOnly: true });
      sitemapRows = loaded.propertyRows || [];
      console.log("  Saved sitemap + CALA properties:", sitemapRows.length);
    } else {
      console.warn("  Sitemap fetch failed:", JSON.stringify(fetched.errors));
    }
  }

  /** @type {object[]} */
  let cdxRows = [];
  /** @type {object} */
  let cdxMeta = { skipped: true };
  if (!opts.skipCdx) {
    console.log("\nHarvesting Wayback CDX for CALA region prefixes (rate-limited)…");
    const cdx = await harvestHyattCalaFromWaybackCdx({ delayMs: opts.cdxDelayMs });
    cdxRows = cdx.propertyRows || [];
    cdxMeta = {
      skipped: false,
      locCount: cdx.locCount,
      unique: cdxRows.length,
      fetchLog: cdx.fetchLog,
    };
    console.log("  CDX unique CALA properties:", cdxRows.length);
  } else {
    console.log("\nSkipping CDX harvest (--skip-cdx)");
  }

  const merged = mergeHyattDirectoryRows(sitemapRows, cdxRows);
  const propertyRows = merged.propertyRows;
  const payload = {
    generatedAt: new Date().toISOString(),
    sourcePolicy: "official_hyatt_com_urls_only",
    sitemapMeta,
    cdxMeta,
    propertyIdConflicts: merged.propertyIdConflicts || [],
    uniqueProperties: propertyRows.length,
    propertyRows,
  };
  writeHyattDirectoryExtract(DIRECTORY_JSON, payload);
  console.log("\nWrote", DIRECTORY_JSON, "—", propertyRows.length, "properties");
  return payload;
}

async function applyPlanRows(planRows) {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID_ALT;
  if (!apiKey || !baseId) throw new Error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");

  const base = new Airtable({ apiKey }).base(baseId);
  let updated = 0;
  let errors = 0;
  /** @type {{ id: string, fields: object }[]} */
  let batch = [];

  async function flush() {
    if (!batch.length) return;
    try {
      await base(HOTEL_CENSUS_TABLE).update(batch);
      updated += batch.length;
    } catch (err) {
      errors += batch.length;
      console.error("Airtable batch error:", err?.message || err);
    }
    batch = [];
  }

  for (const row of planRows) {
    batch.push({ id: row.censusRecordId, fields: row.applyFields });
    appendApplyLog(row);
    if (batch.length >= BATCH) await flush();
  }
  await flush();
  return { updated, errors };
}

async function probeAmenities(planRows, opts) {
  const limit = Math.max(0, opts.amenityProbeLimit || 5);
  const sample = planRows.slice(0, limit);
  console.log(`\n=== Amenity probe (limit ${limit}) ===\n`);

  /** @type {object[]} */
  const probes = [];
  for (let i = 0; i < sample.length; i++) {
    const row = sample[i];
    console.log(` [${i + 1}/${sample.length}] ${row.censusName} → ${row.propertyUrl}`);
    const fetched = await fetchHyattHotelAmenities(row.propertyUrl);
    console.log(
      `   status=${fetched.status} blocked=${fetched.blocked} amenities=${fetched.amenities?.length || 0} err=${fetched.error || ""}`
    );
    probes.push({
      censusRecordId: row.censusRecordId,
      censusName: row.censusName,
      propertyUrl: row.propertyUrl,
      propertyId: row.propertyId,
      ...fetched,
      applyFields:
        fetched.ok && fetched.amenitiesText
          ? { [MAP_DIRECTORY_ENRICHMENT.amenities]: fetched.amenitiesText }
          : null,
    });
    await sleep(opts.delayMs);
  }

  writeFileSync(
    AMENITY_JSON,
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        probeCount: probes.length,
        readyCount: probes.filter((p) => p.applyFields).length,
        blockedCount: probes.filter((p) => p.blocked || p.status === 429).length,
        probes,
        policy:
          "Apply amenities only when fetch succeeds and amenitiesText is non-empty; never invent.",
      },
      null,
      2
    ) + "\n"
  );
  console.log("Wrote", AMENITY_JSON);
  return probes;
}

async function main() {
  const opts = parseArgs();
  mkdirSync(REPORTS, { recursive: true });

  await ensureDirectoryExtract(opts);

  console.log("\n=== Hyatt census match plan ===\n");
  const plan = await planHyattCensusEnrichment({
    jsonPath: DIRECTORY_JSON,
    minScore: opts.minScore,
    minNameSim: opts.minNameSim,
    minConfidence: opts.minConfidence,
  });

  writeFileSync(
    PLAN_JSON,
    JSON.stringify({ generatedAt: new Date().toISOString(), ...plan }, null, 2) + "\n"
  );
  writeCsv(STEWARD_CSV, hyattStewardReviewRows(plan.stewardReview));
  writeCsv(UNMATCHED_CSV, plan.unmatchedCensus || []);

  console.log("Hyatt parent rows (all geos):", plan.hyattParentRows);
  console.log("CALA census scanned:", plan.censusRowsScanned);
  console.log("Directory rows:", plan.directoryRowsLoaded);
  console.log("Matched:", plan.matched);
  console.log("Ready to apply (medium/high):", plan.readyToApply);
  console.log("Steward review (low):", plan.stewardReviewCount);
  console.log("Unmatched blank:", plan.unmatchedBlankCount);
  console.log("Field mapping:", plan.fieldMapping);
  console.log("Plan:", PLAN_JSON);
  console.log("Steward CSV:", STEWARD_CSV);
  console.log("Unmatched CSV:", UNMATCHED_CSV);

  /** @type {object[]} */
  let amenityProbes = [];
  if (opts.probeAmenities || opts.applyAmenities) {
    const probeSource = opts.includeLowOnApply
      ? [...plan.planRows, ...plan.stewardReview.filter((r) => r.applyFields)]
      : plan.planRows;
    amenityProbes = await probeAmenities(probeSource, opts);
  }

  const applyRows = opts.includeLowOnApply
    ? [...plan.planRows, ...plan.stewardReview.filter((r) => r.applyFields)]
    : plan.planRows;

  /** @type {object} */
  let applyResult = { updated: 0, errors: 0, skipped: true };
  /** @type {object} */
  let amenityApply = { updated: 0, errors: 0, skipped: true };

  if (!opts.apply) {
    console.log("\nDry-run only. Pass --apply to write Website + Property ID (fill-blank).");
    console.log("Low-confidence matches are in steward CSV and excluded from apply unless --include-low.");
  } else {
    console.log(`\n=== Applying ${applyRows.length} fill-blank Website + Property ID ===\n`);
    applyResult = { ...(await applyPlanRows(applyRows)), skipped: false };
    console.log("Updated:", applyResult.updated, "errors:", applyResult.errors);
  }

  if (opts.applyAmenities) {
    const ready = amenityProbes.filter((p) => p.applyFields);
    if (!ready.length) {
      console.log("\nNo reliable amenity payloads to apply (blocked or empty). Skipping amenity writes.");
      amenityApply = { updated: 0, errors: 0, skipped: false, reason: "no_reliable_amenities" };
    } else {
      console.log(`\n=== Applying ${ready.length} amenity fill-blanks ===\n`);
      amenityApply = {
        ...(await applyPlanRows(
          ready.map((p) => ({
            censusRecordId: p.censusRecordId,
            censusName: p.censusName,
            propertyId: p.propertyId,
            propertyUrl: p.propertyUrl,
            matchScore: "",
            nameSim: "",
            matchConfidence: "amenity_probe",
            applyFields: p.applyFields,
          }))
        )),
        skipped: false,
      };
      console.log("Amenity updated:", amenityApply.updated, "errors:", amenityApply.errors);
    }
  }

  const summary = {
    generatedAt: new Date().toISOString(),
    mode: opts.apply ? "apply" : "dry-run",
    directoryJson: DIRECTORY_JSON,
    directoryUnique: plan.directoryRowsLoaded,
    hyattParentRows: plan.hyattParentRows,
    calaCensusScanned: plan.censusRowsScanned,
    matched: plan.matched,
    readyToApply: plan.readyToApply,
    stewardReview: plan.stewardReviewCount,
    unmatchedBlank: plan.unmatchedBlankCount,
    appliedWebsitePropertyId: applyResult.updated,
    applyErrors: applyResult.errors,
    amenityProbe: {
      probed: amenityProbes.length,
      ready: amenityProbes.filter((p) => p.applyFields).length,
      blocked: amenityProbes.filter((p) => p.blocked || p.status === 429).length,
      applied: amenityApply.updated,
      skipped: amenityApply.skipped,
      reason: amenityApply.reason || null,
    },
    fieldMapping: plan.fieldMapping,
    reports: {
      directory: DIRECTORY_JSON,
      plan: PLAN_JSON,
      steward: STEWARD_CSV,
      unmatched: UNMATCHED_CSV,
      applyLog: APPLY_LOG,
      amenities: AMENITY_JSON,
      summary: SUMMARY_JSON,
    },
    limitations: [
      "Live hyatt.com often returns 429/Kasada from server IP; directory uses archived official sitemap + CDX of hyatt.com URLs.",
      "Sitemap snapshot dated 2024-01-26 may miss newer openings; CDX supplement helps but is not exhaustive.",
      "Amenities applied only when live property fetch returns parseable amenityFeature; otherwise skipped.",
      "Low-confidence matches excluded from apply by default (steward CSV).",
    ],
  };
  writeFileSync(SUMMARY_JSON, JSON.stringify(summary, null, 2) + "\n");
  console.log("\nSummary:", SUMMARY_JSON);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
