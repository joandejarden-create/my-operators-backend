#!/usr/bin/env node
/**
 * CALA audit: STR *Regional values in Submarket and corridor mapping coverage.
 *
 * Usage:
 *   node scripts/audit-cala-regional-submarkets.mjs
 */

import fs from "node:fs";
import path from "node:path";
import "../load-env.js";
import { getPlatformBase } from "../lib/hotel-census/platform-base.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import {
  CALA_CENSUS_COUNTRIES,
  isCalaCensusCountry,
  isStrRegionalSubmarket,
  proposeCensusSubmarketCorridor,
} from "../lib/hotel-census/census-dealality-submarket.js";

const REPORT_DIR = path.join(process.cwd(), "reports");
const STAMP = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);

const SELECT_FIELDS = [
  CENSUS_FIELDS.name,
  CENSUS_FIELDS.city,
  CENSUS_FIELDS.country,
  CENSUS_FIELDS.region,
  CENSUS_FIELDS.market,
  CENSUS_FIELDS.submarket,
  "STR Number",
];

function csvEscape(value) {
  const s = String(value ?? "");
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

async function main() {
  const base = getPlatformBase();
  const records = await base(HOTEL_CENSUS_TABLE)
    .select({ fields: SELECT_FIELDS, pageSize: 100 })
    .all();

  const calaRecords = records.filter((rec) =>
    isCalaCensusCountry(rec.fields[CENSUS_FIELDS.country])
  );

  const regionalBuckets = {};
  const countryCoverage = {};
  const regionalRows = [];
  const proposalSummary = {
    wouldMap: 0,
    noMatch: 0,
    alreadyCorridor: 0,
  };

  for (const rec of calaRecords) {
    const row = rec.fields;
    const country = String(row[CENSUS_FIELDS.country] || "").trim();
    const strSub = String(row[CENSUS_FIELDS.submarket] || "").trim();

    if (!countryCoverage[country]) {
      countryCoverage[country] = {
        country,
        hotels: 0,
        strRegional: 0,
        wouldMapFromRegional: 0,
        regionalUnmapped: 0,
        distinctSubmarkets: new Set(),
      };
    }
    const bucket = countryCoverage[country];
    bucket.hotels += 1;
    if (strSub) bucket.distinctSubmarkets.add(strSub);

    if (!isStrRegionalSubmarket(strSub)) continue;

    regionalBuckets[strSub] = (regionalBuckets[strSub] || 0) + 1;
    bucket.strRegional += 1;

    const proposal = proposeCensusSubmarketCorridor(
      { ...row, id: rec.id },
      { overwriteRegional: true, minConfidence: "Medium" }
    );

    const entry = {
      recordId: rec.id,
      name: row[CENSUS_FIELDS.name],
      country,
      city: row[CENSUS_FIELDS.city],
      currentSubmarket: strSub,
      proposedSubmarket: proposal.submarket || "",
      confidence: proposal.confidence,
      reason: proposal.reason,
      status: proposal.submarket && !proposal.skipped ? "mappable" : "unmapped",
    };
    regionalRows.push(entry);

    if (proposal.submarket && !proposal.skipped) {
      proposalSummary.wouldMap += 1;
      bucket.wouldMapFromRegional += 1;
    } else if (!proposal.submarket) {
      proposalSummary.noMatch += 1;
      bucket.regionalUnmapped += 1;
    } else {
      proposalSummary.alreadyCorridor += 1;
    }
  }

  const regionalSorted = Object.entries(regionalBuckets).sort((a, b) => b[1] - a[1]);
  const countryRows = Object.values(countryCoverage)
    .map((c) => ({
      country: c.country,
      hotels: c.hotels,
      strRegional: c.strRegional,
      wouldMapFromRegional: c.wouldMapFromRegional,
      regionalUnmapped: c.regionalUnmapped,
      distinctSubmarkets: c.distinctSubmarkets.size,
    }))
    .sort((a, b) => b.strRegional - a.strRegional || b.hotels - a.hotels);

  const summary = {
    generatedAt: new Date().toISOString(),
    targetField: CENSUS_FIELDS.submarket,
    calaCountriesInScope: CALA_CENSUS_COUNTRIES.length,
    calaHotelRows: calaRecords.length,
    strRegionalRows: regionalSorted.reduce((s, [, n]) => s + n, 0),
    distinctRegionalLabels: regionalSorted.length,
    regionalLabels: regionalSorted.map(([label, count]) => ({ label, count })),
    proposalFromRegional: proposalSummary,
    countryCoverage: countryRows,
  };

  fs.mkdirSync(REPORT_DIR, { recursive: true });
  const jsonPath = path.join(REPORT_DIR, `cala-regional-submarket-audit-${STAMP}.json`);
  fs.writeFileSync(jsonPath, JSON.stringify(summary, null, 2));

  const detailCsvPath = path.join(
    REPORT_DIR,
    `cala-regional-submarket-audit-detail-${STAMP}.csv`
  );
  const header = [
    "status",
    "recordId",
    "name",
    "country",
    "city",
    "currentSubmarket",
    "proposedSubmarket",
    "confidence",
    "reason",
  ];
  const lines = [header.join(",")];
  for (const row of regionalRows) {
    lines.push(header.map((h) => csvEscape(row[h])).join(","));
  }
  fs.writeFileSync(detailCsvPath, lines.join("\n"));

  console.log("CALA regional Submarket audit");
  console.log("  Target field:", CENSUS_FIELDS.submarket);
  console.log("  CALA hotel rows:", summary.calaHotelRows);
  console.log("  STR *Regional rows:", summary.strRegionalRows);
  console.log("  Would map from regional:", proposalSummary.wouldMap);
  console.log("  Regional unmapped:", proposalSummary.noMatch);
  console.log("  JSON:", jsonPath);
  console.log("  Detail CSV:", detailCsvPath);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
