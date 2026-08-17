#!/usr/bin/env node
/**
 * Apply steward-verified Choice URLs for Mexico/Brazil stragglers (fill-blank).
 * Usage: node scripts/apply-choice-straggler-urls-batch.mjs [--dry-run]
 */
import "../load-env.js";
import { appendFileSync, existsSync } from "node:fs";
import { join } from "node:path";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";

const DRY_RUN = process.argv.includes("--dry-run");
const LOG_PATH = join("reports", "choice-steward-verified-applies.csv");

function canonicalChoiceUrl(url) {
  return String(url || "")
    .trim()
    .replace(/^(https:\/\/www\.choicehotels\.com)\/en-[a-z]{2}\//i, "$1/");
}

/** Verified via regional JSON-LD, sitemap, or hotel official Choice link. */
const STEWARD = [
  {
    id: "rec7Ny9PfihzkgEGK",
    code: "MX092",
    name: "Comfort Inn Irapuato",
    url: "https://www.choicehotels.com/guanajuato/irapuato/comfort-inn-hotels/mx092",
    note: "sitemap + brand/city",
  },
  {
    id: "rec9nG8vcyOmZXJSR",
    code: "MX043",
    name: "Hotel Chihuahua San Francisco",
    url: "https://www.choicehotels.com/chihuahua/chihuahua/quality-inn-hotels/mx043",
    note: "Quality Inn San Francisco; MX043 on choicehotels.com",
  },
  {
    id: "recbl4EWE8dzSS0lO",
    code: "MX228",
    name: "Amberes 64 Ascend Hotel Collection",
    url: "https://www.choicehotels.com/mexico/mexico-city/ascend-hotels/mx228",
    note: "Mexico regional JSON-LD official name",
  },
  {
    id: "reco6dhOREJ8JyAfa",
    code: "BR080",
    name: "Comfort Inn Joinville",
    url: "https://www.choicehotels.com/santa-catarina/joinville/comfort-inn-hotels/br080",
    note: "Brazil regional JSON-LD",
  },
  {
    id: "recC9ZIeByrNob1dw",
    code: "BR090",
    name: "Quality Hotel St Paul Rio Preto",
    url: "https://www.choicehotels.com/sao-paulo/sao-jose-do-rio-preto/quality-inn-hotels/br090",
    note: "sitemap quality-inn rio preto",
  },
  {
    id: "recrq5oWLlU3I3Wdg",
    code: "BR084",
    name: "Quality Hotel Porto Alegre",
    url: "https://www.choicehotels.com/rio-grande-do-sul/porto-alegre/quality-inn-hotels/br084",
    note: "sitemap quality-inn porto alegre",
  },
  {
    id: "recchbmgK8qZTKQz3",
    code: "BR075",
    name: "Former Comfort Suites Macae",
    url: "https://www.choicehotels.com/rio-de-janeiro/macae/comfort-suites-hotels/br075",
    note: "sitemap macae comfort-suites",
  },
  {
    id: "recy5vv0a3sGia5Fn",
    code: "BR162",
    name: "Radisson Hotel Pinheiros Sao Paulo",
    url: "https://www.choicehotels.com/sao-paulo/pinheiros/radisson-hotels/br162",
    note: "Brazil regional JSON-LD (distinct from BR183)",
  },
];

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID_ALT
);

function appendLog(row, url) {
  if (!existsSync(LOG_PATH)) {
    appendFileSync(
      LOG_PATH,
      "appliedAt,censusRecordId,censusName,propertyId,propertyUrl,source,amenityCount\n"
    );
  }
  appendFileSync(
    LOG_PATH,
    `${new Date().toISOString()},${row.id},"${row.name}",${row.code},${url},choice_straggler_steward,0\n`
  );
}

let applied = 0;
for (const entry of STEWARD) {
  const website = canonicalChoiceUrl(entry.url);
  const rec = await base(HOTEL_CENSUS_TABLE).find(entry.id);
  const f = rec.fields || {};
  const fields = {};

  if (isBlankCensusValue(f.Website)) fields.Website = website;
  if (isBlankCensusValue(f["Property ID"])) fields["Property ID"] = entry.code;

  console.log(`\n${f.name} -> ${entry.code} (${entry.note})`);
  console.log("Before:", { Website: f.Website, PID: f["Property ID"] });
  console.log("Apply:", fields);

  if (!Object.keys(fields).length) {
    console.log("Skip: already filled");
    continue;
  }

  if (DRY_RUN) {
    console.log("Dry-run");
    continue;
  }

  await base(HOTEL_CENSUS_TABLE).update(rec.id, fields, { typecast: true });
  appendLog({ id: rec.id, name: f.name, code: entry.code }, website);
  applied++;
}

console.log(`\n${DRY_RUN ? "Dry-run" : `Applied ${applied}`} straggler URLs.`);
