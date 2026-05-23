/**
 * Phase 2B+ / 2D — OpenStreetMap dry-run importer (DRY-RUN ONLY).
 *
 * Fetches nodes, ways, and relations via Overpass (`out center`), normalizes via
 * lib/independent-census/normalize-candidate.js, applies optional hotel-focused filters.
 *
 * DOES NOT write to Airtable. Rejects --apply.
 */
import { readFileSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  fetchOsmHotelCandidates,
  summarizeCandidates,
  OSM_COPYRIGHT,
  DEFAULT_MAX_ELEMENTS,
} from "../lib/independent-census/sources/osm.js";
import { sourcePolicySummaryForReport } from "../lib/independent-census/source-registry.js";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS_DIR = join(__dirname, "..", "reports");

const DEFAULT_COUNTRY = "Dominican Republic";
const EXPANDED_BASELINE_BATCH = "osm-dominican-republic-expanded-2026-05-20";

function parseArgs() {
  if (process.argv.includes("--apply")) {
    throw new Error(
      "--apply is not supported. OSM import is dry-run / report-only. No Airtable writes."
    );
  }

  let country = DEFAULT_COUNTRY;
  let city = "";
  let bbox = "";
  let limit = null;
  let maxElements = null;
  let batchId = "";
  let useDefaultMax = false;
  let hotelFocused = false;
  let includeApartments = false;
  let includeUnnamed = false;
  let minQuality = 0;

  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--country" && argv[i + 1]) country = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--country="))
      country = a.slice("--country=".length).replace(/^"|"$/g, "");
    else if (a === "--city" && argv[i + 1]) city = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--city=")) city = a.slice("--city=".length).replace(/^"|"$/g, "");
    else if (a === "--bbox" && argv[i + 1]) bbox = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--bbox=")) bbox = a.slice("--bbox=".length).replace(/^"|"$/g, "");
    else if (a === "--limit" && argv[i + 1]) limit = parseInt(argv[++i], 10);
    else if (a.startsWith("--limit=")) limit = parseInt(a.slice("--limit=".length), 10);
    else if (a === "--max-elements" && argv[i + 1])
      maxElements = parseInt(argv[++i], 10);
    else if (a.startsWith("--max-elements="))
      maxElements = parseInt(a.slice("--max-elements=".length), 10);
    else if (a === "--batch-id" && argv[i + 1]) batchId = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--batch-id="))
      batchId = a.slice("--batch-id=".length).replace(/^"|"$/g, "");
    else if (a === "--min-quality" && argv[i + 1])
      minQuality = parseInt(argv[++i], 10);
    else if (a.startsWith("--min-quality="))
      minQuality = parseInt(a.slice("--min-quality=".length), 10);
    else if (a === "--default-max-elements") useDefaultMax = true;
    else if (a === "--hotel-focused") hotelFocused = true;
    else if (a === "--include-apartments") includeApartments = true;
    else if (a === "--include-unnamed") includeUnnamed = true;
  }

  if (limit != null && (!Number.isFinite(limit) || limit < 1)) {
    throw new Error("--limit must be a positive integer");
  }
  if (maxElements != null && (!Number.isFinite(maxElements) || maxElements < 1)) {
    throw new Error("--max-elements must be a positive integer");
  }
  if (minQuality != null && (!Number.isFinite(minQuality) || minQuality < 0)) {
    throw new Error("--min-quality must be >= 0");
  }

  if (maxElements == null && useDefaultMax) {
    maxElements = DEFAULT_MAX_ELEMENTS;
  }

  if (!batchId) {
    const slug = normalizeBatchSlug(country, city);
    const date = new Date().toISOString().slice(0, 10);
    batchId = `osm-${slug}-${date}`;
  }

  return {
    country,
    city,
    bbox,
    limit,
    maxElements,
    batchId,
    hotelFocused,
    includeApartments,
    includeUnnamed,
    minQuality,
  };
}

function normalizeBatchSlug(country, city) {
  const parts = [country, city]
    .filter(Boolean)
    .join("-")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
  return parts || "geo";
}

const CSV_COLUMNS = [
  "sourceRecordId",
  "sourceName",
  "sourceType",
  "sourceLicense",
  "sourceUrl",
  "rawHotelName",
  "rawAddress",
  "rawCity",
  "rawCountry",
  "rawLatitude",
  "rawLongitude",
  "rawWebsite",
  "rawPhone",
  "rawBrand",
  "importBatchId",
  "importedAt",
  "reviewStatus",
  "possibleMatchConfidence",
  "recommendedAction",
  "candidateDedupeKey",
  "qualityScore",
  "qualityTier",
  "osmElementType",
  "osmTourismTag",
  "missingFields",
];

function candidateToCsvRow(c) {
  return {
    sourceRecordId: c.sourceRecordId,
    sourceName: c.sourceName,
    sourceType: c.sourceType,
    sourceLicense: c.sourceLicense,
    sourceUrl: c.sourceUrl,
    rawHotelName: c.rawHotelName,
    rawAddress: c.rawAddress,
    rawCity: c.rawCity,
    rawCountry: c.rawCountry,
    rawLatitude: c.rawLatitude ?? "",
    rawLongitude: c.rawLongitude ?? "",
    rawWebsite: c.rawWebsite,
    rawPhone: c.rawPhone,
    rawBrand: c.rawBrand,
    importBatchId: c.importBatchId,
    importedAt: c.importedAt,
    reviewStatus: c.reviewStatus,
    possibleMatchConfidence: c.possibleMatchConfidence,
    recommendedAction: c.recommendedAction,
    candidateDedupeKey: c.candidateDedupeKey,
    qualityScore: c.qualityScore ?? "",
    qualityTier: c.qualityTier ?? "",
    osmElementType: c._osmElementType,
    osmTourismTag: c._osmTourismTag,
    missingFields: (c.missingFields || c._missingFields || []).join("; "),
  };
}

function stripInternalFields(c) {
  const {
    _osmElementType,
    _osmTourismTag,
    _missingFieldFlags,
    _missingFields,
    _hasRoomsTag,
    _hasStarsTag,
    _hasBrandOrOperator,
    ...rest
  } = c;
  return {
    ...rest,
    osmElementType: _osmElementType,
    osmTourismTag: _osmTourismTag,
    missingFieldFlags: _missingFieldFlags || c.missingFieldFlags,
    missingFields: _missingFields || c.missingFields,
    hasRoomsTag: _hasRoomsTag,
    hasStarsTag: _hasStarsTag,
    hasBrandOrOperator: _hasBrandOrOperator,
  };
}

function formatCappingStatus(capping) {
  const parts = [];
  if (capping.uncappedOverpass) {
    parts.push("Overpass: uncapped");
  } else {
    parts.push(
      `Overpass: capped at ${capping.overpassMaxElements} (returned ${capping.overpassReturned}${capping.overpassCapped ? ", likely truncated" : ""})`
    );
  }
  if (capping.uncappedCandidates) {
    parts.push("Candidates: uncapped");
  } else {
    parts.push(
      `Candidates: capped at ${capping.candidateLimit} (from ${capping.candidatesBeforeLimit})`
    );
  }
  if (capping.hotelFocused) {
    parts.push(`Query tags: hotel-focused (${(capping.tourismTagsQueried || []).join(", ")})`);
  }
  return parts.join(" | ");
}

function loadBaselineComparison(batchId) {
  const path = join(REPORTS_DIR, `independent-census-osm-dry-run-${EXPANDED_BASELINE_BATCH}.json`);
  if (batchId === EXPANDED_BASELINE_BATCH || !existsSync(path)) return null;
  try {
    const baseline = JSON.parse(readFileSync(path, "utf8"));
    return {
      baselineBatchId: EXPANDED_BASELINE_BATCH,
      baselineCandidateCount: baseline.candidateCount,
      baselineCoverage: baseline.summary?.coverage,
      baselineByElementType: baseline.summary?.byElementType,
      baselineByTourismTag: baseline.summary?.byTourismTag,
    };
  } catch {
    return null;
  }
}

function buildComparisonMetrics(baseline, current) {
  if (!baseline?.baselineCoverage || !current) return null;
  const b = baseline.baselineCoverage;
  const c = current;
  const pct = (n, t) => (t ? `${((100 * n) / t).toFixed(1)}%` : "n/a");
  return {
    candidateCountDelta: c.total - (baseline.baselineCandidateCount || 0),
    withCity: {
      before: b.withCity,
      after: c.withCity,
      rateBefore: pct(b.withCity, b.total),
      rateAfter: pct(c.withCity, c.total),
    },
    withWebsite: {
      before: b.withWebsite,
      after: c.withWebsite,
      rateBefore: pct(b.withWebsite, b.total),
      rateAfter: pct(c.withWebsite, c.total),
    },
    withPhone: {
      before: b.withPhone,
      after: c.withPhone,
      rateBefore: pct(b.withPhone, b.total),
      rateAfter: pct(c.withPhone, c.total),
    },
    missingName: {
      before: b.missingName,
      after: c.missingName,
    },
  };
}

async function main() {
  const args = parseArgs();
  const {
    country,
    city,
    bbox,
    limit,
    maxElements,
    batchId,
    hotelFocused,
    includeApartments,
    includeUnnamed,
    minQuality,
  } = args;

  const jsonPath = join(REPORTS_DIR, `independent-census-osm-dry-run-${batchId}.json`);
  const csvPath = join(REPORTS_DIR, `independent-census-osm-dry-run-${batchId}.csv`);

  console.log("=== Independent census OSM import (DRY-RUN, Phase 2D) ===\n");
  console.log("Local reports only — no Airtable writes.\n");
  console.log(`Batch ID:          ${batchId}`);
  console.log(`Country:           ${country}`);
  console.log(`City:              ${city || "(none)"}`);
  console.log(`BBox:              ${bbox || "(none)"}`);
  console.log(
    `Max elements:      ${maxElements == null ? "none (uncapped Overpass)" : maxElements}`
  );
  console.log(`Candidate limit:   ${limit == null ? "none" : limit}`);
  console.log(`Hotel-focused:     ${hotelFocused}`);
  console.log(`Include apartments:${includeApartments}`);
  console.log(`Include unnamed:   ${includeUnnamed}`);
  console.log(`Min quality:       ${minQuality || "none"}`);
  console.log(`OSM attribution:   ${OSM_COPYRIGHT}\n`);

  console.log("Fetching from Overpass API…");

  const result = await fetchOsmHotelCandidates({
    country,
    city: city || undefined,
    bbox: bbox || undefined,
    maxElements,
    limit,
    batchId,
    hotelFocused,
    includeApartments,
    includeUnnamed,
    minQuality,
  });

  const { query, elements, allCandidates, candidates, filtering, capping } = result;
  const baseline = loadBaselineComparison(batchId);
  const summary = summarizeCandidates(candidates, {
    filtering,
    capping,
    sourcePolicy: sourcePolicySummaryForReport(),
    comparison: buildComparisonMetrics(baseline, summarizeCandidates(candidates).coverage),
  });

  const report = {
    generatedAt: new Date().toISOString(),
    phase: "2D-dry-run",
    batchId,
    geography: { country, city: city || null, bbox: bbox || null },
    filterOptions: {
      hotelFocused,
      includeApartments,
      includeUnnamed,
      minQuality: minQuality || null,
    },
    dryRun: true,
    airtableWrites: false,
    hotelCensusWrites: false,
    brandAliasWrites: false,
    stagingTableWrites: false,
    overpassQuery: query,
    overpassRawElementCount: result.rawElementCount,
    overpassElementCount: elements.length,
    normalizedBeforeFilter: allCandidates.length,
    candidateCount: candidates.length,
    filtering: {
      ...filtering,
      excludedTotal:
        filtering.beforeCount - filtering.afterCount,
    },
    uniqueDedupeKeys: summary.dedupe.uniqueKeys,
    duplicateDedupeKeyRows: summary.dedupe.duplicateRows,
    summary,
    baselineComparison: baseline,
    reportFiles: { json: jsonPath, csv: csvPath },
    candidates: candidates.map(stripInternalFields),
  };

  writeJson(jsonPath, report);
  writeCsv(csvPath, candidates.map(candidateToCsvRow), CSV_COLUMNS);

  console.log(formatCappingStatus(capping));
  console.log(`\nRaw Overpass elements:  ${result.rawElementCount}`);
  console.log(`Normalized (pre-filter): ${allCandidates.length}`);
  if (filtering.beforeCount !== filtering.afterCount) {
    console.log("Post-filter exclusions:");
    console.log(`  apartments:      ${filtering.excluded.apartments}`);
    console.log(`  unnamed:         ${filtering.excluded.unnamed}`);
    console.log(`  low quality:     ${filtering.excluded.lowQuality}`);
    console.log(`  tourism other:   ${filtering.excluded.tourismExcluded}`);
  }
  console.log(`Candidates (final):     ${candidates.length}`);

  const cov = summary.coverage;
  console.log("\n--- Coverage ---");
  console.log(`  City:      ${cov.withCity}/${cov.total} (${pct(cov.withCity, cov.total)})`);
  console.log(`  Website:   ${cov.withWebsite}/${cov.total} (${pct(cov.withWebsite, cov.total)})`);
  console.log(`  Phone:     ${cov.withPhone}/${cov.total} (${pct(cov.withPhone, cov.total)})`);
  console.log(`  Brand/op:  ${cov.withBrandOrOperator}/${cov.total}`);
  console.log(`  Rooms tag: ${cov.withRoomsTag}/${cov.total}`);
  console.log(`  Stars tag: ${cov.withStarsTag}/${cov.total}`);

  console.log("\n--- Element types ---");
  console.log(JSON.stringify(summary.byElementType, null, 2));
  console.log("\n--- Quality tiers ---");
  console.log(JSON.stringify(summary.byQualityTier, null, 2));

  if (report.baselineComparison && summary.comparison) {
    console.log("\n--- vs expanded 2B+ baseline ---");
    console.log(JSON.stringify(summary.comparison, null, 2));
  }

  console.log("\nReport files:");
  console.log(`  ${jsonPath}`);
  console.log(`  ${csvPath}`);
  console.log("\n✓ Dry-run complete. No Airtable writes. Hotel Census untouched.");
}

function pct(n, total) {
  if (!total) return "0%";
  return `${((100 * n) / total).toFixed(1)}%`;
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
