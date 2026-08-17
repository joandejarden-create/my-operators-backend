#!/usr/bin/env node
/**
 * Resolve 16 stewarded Choice Radisson Individuals insert candidates (official URL + name cleanup).
 * Does not write Airtable by itself — production-cycle applies resolved High inserts.
 */
import "dotenv/config";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import {
  resolvePat,
  resolveTargetBase,
} from "../lib/research-engine-v2/production-census-schema-create.js";
import { TABLE_IDS } from "../lib/research-engine-v2/production-census-write.js";
import {
  loadChoiceRadissonStewardCases,
  resolveChoiceRadissonStewardBatch,
  writeChoiceRadissonStewardResolutionReports,
  RESOLUTION_CLASS,
} from "../lib/research-engine-v2/census-autopilot-choice-radisson-steward-resolution.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const CENSUS_TABLE_ID = TABLE_IDS["Hotel Property Census"];

async function listCensusForRededupe(baseId, token) {
  const out = [];
  let offset;
  const fields = [
    "Property Identity Key",
    "Property Name",
    "Current Brand",
    "Country",
    "City",
    "Address",
    "Source URL",
    "Official Property URL",
  ];
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of fields) params.append("fields[]", f);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CENSUS_TABLE_ID)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(`census list ${res.status}`);
    out.push(...(json.records || []));
    offset = json.offset;
    await new Promise((r) => setTimeout(r, 120));
  } while (offset);
  return out;
}

const loaded = loadChoiceRadissonStewardCases();
if (!loaded.ok) {
  console.error(JSON.stringify({ ok: false, error: loaded.error }, null, 2));
  process.exit(1);
}

console.log(`[choice-radisson-steward] loaded ${loaded.count} cases from ${loaded.path}`);

const token = resolvePat();
const bases = resolveTargetBase();
const censusRecords = await listCensusForRededupe(bases.target_base_id, token);
console.log(`[choice-radisson-steward] census rows for rededupe: ${censusRecords.length}`);

const report = resolveChoiceRadissonStewardBatch(loaded.items, { censusRecords });
const paths = writeChoiceRadissonStewardResolutionReports(report, {
  runDir: dirname(loaded.path),
});

console.log(
  JSON.stringify(
    {
      ok: true,
      input: report.input_count,
      resolved: report.counts[RESOLUTION_CLASS.RESOLVED],
      still_steward: report.counts[RESOLUTION_CLASS.STILL_STEWARD],
      duplicate: report.counts[RESOLUTION_CLASS.DUPLICATE],
      source_insufficient: report.counts[RESOLUTION_CLASS.SOURCE_INSUFFICIENT],
      identity_conflict: report.counts[RESOLUTION_CLASS.IDENTITY_CONFLICT],
      reports: paths.reports,
      sample: (report.resolved_inserts || []).slice(0, 3).map((r) => ({
        identity_key: r.identity_key,
        name: r.property_name,
        city: r.fields.City,
        source_url: r.fields["Source URL"],
      })),
    },
    null,
    2
  )
);
