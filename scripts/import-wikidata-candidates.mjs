/**
 * Phase 2E — Wikidata dry-run importer (REPORT ONLY).
 *
 * Queries Wikidata Query Service (SPARQL), normalizes to shared candidate shape.
 * Rejects --apply. No Airtable writes.
 */
import { join } from "path";
import {
  fetchWikidataHotelCandidates,
  summarizeWikidataCandidates,
  WIKIDATA_SOURCE_LICENSE,
} from "../lib/independent-census/sources/wikidata.js";
import { sourcePolicySummaryForReport } from "../lib/independent-census/source-registry.js";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";

const REPORTS_DIR = join(process.cwd(), "reports");
const DEFAULT_COUNTRY = "Dominican Republic";

function parseArgs() {
  if (process.argv.includes("--apply")) {
    throw new Error(
      "--apply is not supported. Wikidata import is dry-run / report-only. No Airtable writes."
    );
  }

  let country = DEFAULT_COUNTRY;
  let city = "";
  let limit = 500;
  let batchId = "";

  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--country" && argv[i + 1]) country = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--country="))
      country = a.slice("--country=".length).replace(/^"|"$/g, "");
    else if (a === "--city" && argv[i + 1]) city = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--city=")) city = a.slice("--city=".length).replace(/^"|"$/g, "");
    else if (a === "--limit" && argv[i + 1]) limit = parseInt(argv[++i], 10);
    else if (a.startsWith("--limit=")) limit = parseInt(a.slice("--limit=".length), 10);
    else if (a === "--batch-id" && argv[i + 1]) batchId = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--batch-id="))
      batchId = a.slice("--batch-id=".length).replace(/^"|"$/g, "");
  }

  if (!Number.isFinite(limit) || limit < 1) {
    throw new Error("--limit must be a positive integer");
  }

  if (!batchId) {
    const slug = country
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-|-$/g, "");
    batchId = `wikidata-${slug}-${new Date().toISOString().slice(0, 10)}`;
  }

  return { country, city, limit, batchId };
}

const CSV_COLUMNS = [
  "sourceRecordId",
  "sourceName",
  "sourceType",
  "sourceLicense",
  "sourceUrl",
  "rawHotelName",
  "rawCity",
  "rawCountry",
  "rawLatitude",
  "rawLongitude",
  "rawWebsite",
  "rawBrand",
  "qualityScore",
  "qualityTier",
  "candidateDedupeKey",
  "missingFields",
  "hasWikipediaUrl",
  "hasOperator",
  "hasOwner",
];

function candidateToCsvRow(c) {
  return {
    sourceRecordId: c.sourceRecordId,
    sourceName: c.sourceName,
    sourceType: c.sourceType,
    sourceLicense: c.sourceLicense,
    sourceUrl: c.sourceUrl,
    rawHotelName: c.rawHotelName,
    rawCity: c.rawCity,
    rawCountry: c.rawCountry,
    rawLatitude: c.rawLatitude ?? "",
    rawLongitude: c.rawLongitude ?? "",
    rawWebsite: c.rawWebsite,
    rawBrand: c.rawBrand,
    qualityScore: c.qualityScore ?? "",
    qualityTier: c.qualityTier ?? "",
    candidateDedupeKey: c.candidateDedupeKey,
    missingFields: (c.missingFields || []).join("; "),
    hasWikipediaUrl: c._hasWikipediaUrl ? "yes" : "no",
    hasOperator: c._hasOperator ? "yes" : "no",
    hasOwner: c._hasOwner ? "yes" : "no",
  };
}

function stripInternal(c) {
  const {
    _wikidataQid,
    _wikidataDescription,
    _wikidataOperator,
    _wikidataOwner,
    _wikidataWikipediaUrl,
    _hasWikipediaUrl,
    _hasOperator,
    _hasOwner,
    sourcePolicyFlags,
    ...rest
  } = c;
  return {
    ...rest,
    wikidataQid: _wikidataQid,
    wikidataDescription: _wikidataDescription,
    wikidataOperator: _wikidataOperator,
    wikidataOwner: _wikidataOwner,
    wikidataWikipediaUrl: _wikidataWikipediaUrl,
    hasWikipediaUrl: _hasWikipediaUrl,
    hasOperator: _hasOperator,
    hasOwner: _hasOwner,
    sourcePolicyFlags,
  };
}

async function main() {
  const { country, city, limit, batchId } = parseArgs();
  const jsonPath = join(REPORTS_DIR, `independent-census-wikidata-dry-run-${batchId}.json`);
  const csvPath = join(REPORTS_DIR, `independent-census-wikidata-dry-run-${batchId}.csv`);

  console.log("=== Independent census Wikidata import (DRY-RUN, Phase 2E) ===\n");
  console.log("Local reports only — no Airtable writes.\n");
  console.log(`Batch ID:   ${batchId}`);
  console.log(`Country:    ${country}`);
  console.log(`City:       ${city || "(none)"}`);
  console.log(`Limit:      ${limit}`);
  console.log(`License:    ${WIKIDATA_SOURCE_LICENSE}\n`);

  console.log("Querying Wikidata Query Service (SPARQL)…");
  const result = await fetchWikidataHotelCandidates({
    country,
    city: city || undefined,
    limit,
    batchId,
  });

  const summary = summarizeWikidataCandidates(result.candidates, {
    rawCount: result.rawCount,
    geography: { country, city: city || null },
    sourcePolicy: sourcePolicySummaryForReport().find((p) => p.sourceType === "wikidata"),
  });

  const report = {
    generatedAt: new Date().toISOString(),
    phase: "2E-dry-run",
    batchId,
    geography: { country, city: city || null },
    limit,
    sparqlQuery: result.query,
    sparqlBindings: result.rawCount,
    candidateCount: result.candidates.length,
    dryRun: true,
    airtableWrites: false,
    hotelCensusWrites: false,
    verifiedTableWrites: false,
    evidenceTableWrites: false,
    stagingTableWrites: false,
    strFieldsUsed: false,
    summary,
    reportFiles: { json: jsonPath, csv: csvPath },
    candidates: result.candidates.map(stripInternal),
  };

  writeJson(jsonPath, report);
  writeCsv(csvPath, result.candidates.map(candidateToCsvRow), CSV_COLUMNS);

  console.log(`SPARQL bindings:  ${result.rawCount}`);
  console.log(`Candidates:       ${result.candidates.length}`);
  console.log("\n--- Coverage ---");
  console.log(`  Coordinates:  ${summary.withCoordinates}/${summary.total}`);
  console.log(`  Website:      ${summary.withWebsite}/${summary.total}`);
  console.log(`  Operator:     ${summary.withOperator}/${summary.total}`);
  console.log(`  Owner:        ${summary.withOwner}/${summary.total}`);
  console.log(`  Wikipedia:    ${summary.withWikipediaUrl}/${summary.total}`);
  console.log("\n--- Quality tiers ---");
  console.log(JSON.stringify(summary.byQualityTier, null, 2));
  console.log("\nReport files:");
  console.log(`  ${jsonPath}`);
  console.log(`  ${csvPath}`);
  console.log("\n✓ Dry-run complete. No Airtable writes. Hotel Census untouched.");
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
