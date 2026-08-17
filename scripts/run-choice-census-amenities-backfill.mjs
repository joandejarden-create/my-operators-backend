#!/usr/bin/env node
/**
 * Choice census amenities backfill — verified property-page HTML only.
 *
 *   node scripts/run-choice-census-amenities-backfill.mjs --limit 20
 *   node scripts/run-choice-census-amenities-backfill.mjs --apply --use-puppeteer --limit 10
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import Airtable from "airtable";
import { fetchChoiceHotelAmenities } from "../lib/choice-hotel-content-fetch.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { writeCsv } from "../lib/str-census-import/report-utils.mjs";

const REPORTS = "reports";
const PLAN_JSON = join(REPORTS, "choice-census-amenities-backfill-plan.json");
const BLOCKED_CSV = join(REPORTS, "choice-census-amenities-blocked-steward.csv");

function parseArgs() {
  const args = process.argv.slice(2);
  let limit = 0;
  const limitEq = args.find((a) => a.startsWith("--limit="));
  if (limitEq) limit = Number(limitEq.split("=")[1] || 0);
  else {
    const idx = args.indexOf("--limit");
    if (idx >= 0 && args[idx + 1] && !args[idx + 1].startsWith("--")) {
      limit = Number(args[idx + 1] || 0);
    }
  }
  return {
    apply: args.includes("--apply"),
    usePuppeteer: args.includes("--use-puppeteer"),
    noPuppeteer: args.includes("--no-puppeteer"),
    headed: args.includes("--headed"),
    limit,
    delayMs: Number(args.find((a) => a.startsWith("--delay-ms="))?.split("=")[1] || 500),
  };
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function main() {
  const opts = parseArgs();
  mkdirSync(REPORTS, { recursive: true });

  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );

  const records = await base(HOTEL_CENSUS_TABLE)
    .select({
      fields: ["name", "Website", "Amenities", CENSUS_FIELDS.country, CENSUS_FIELDS.parentCompany],
      filterByFormula: `FIND("Choice", {${CENSUS_FIELDS.parentCompany}})`,
    })
    .all();

  const targets = records.filter((r) => {
    const url = String(r.fields?.Website || "").trim();
    return url && /choicehotels\.com/i.test(url) && isBlankCensusValue(r.fields?.Amenities);
  });

  const slice = opts.limit > 0 ? targets.slice(0, opts.limit) : targets;
  console.log(`Choice census with Website + blank Amenities: ${targets.length}`);
  console.log(`Processing: ${slice.length}`);

  /** @type {object[]} */
  const plan = [];
  let applied = 0;

  for (let i = 0; i < slice.length; i++) {
    const rec = slice[i];
    const url = String(rec.fields.Website).trim();
    console.log(`\n[${i + 1}/${slice.length}] ${rec.fields.name}`);

    const fetched = await fetchChoiceHotelAmenities(url, {
      usePuppeteer: opts.usePuppeteer && !opts.noPuppeteer ? true : false,
      headed: opts.headed,
    });
    await sleep(opts.delayMs);

    const row = {
      censusRecordId: rec.id,
      censusName: rec.fields.name,
      censusCountry: rec.fields[CENSUS_FIELDS.country],
      propertyUrl: url,
      status: fetched.status,
      fetchMethod: fetched.fetchMethod || "plain_fetch",
      amenityCount: fetched.amenities?.length || 0,
      amenitiesPreview: (fetched.amenitiesText || "").slice(0, 200),
      parseErrors: (fetched.parseErrors || []).join(";"),
    };
    plan.push(row);
    console.log(`  ${row.status} (${row.fetchMethod}) amenities=${row.amenityCount}`);

    if (opts.apply && fetched.amenitiesText) {
      await base(HOTEL_CENSUS_TABLE).update(rec.id, { Amenities: fetched.amenitiesText }, { typecast: true });
      applied++;
    }
  }

  const blocked = plan.filter((r) => r.status === "blocked" || r.amenityCount === 0);
  writeFileSync(
    PLAN_JSON,
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        options: opts,
        targets: targets.length,
        processed: slice.length,
        applied,
        ok: plan.filter((r) => r.amenityCount > 0).length,
        blocked: blocked.length,
        plan,
      },
      null,
      2
    )
  );
  console.log("\nWrote:", PLAN_JSON);

  if (blocked.length) {
    writeCsv(
      BLOCKED_CSV,
      blocked.map((r) => ({
        censusRecordId: r.censusRecordId,
        censusName: r.censusName,
        censusCountry: r.censusCountry,
        propertyUrl: r.propertyUrl,
        status: r.status,
        fetchMethod: r.fetchMethod,
        parseErrors: r.parseErrors,
      })),
      [
        "censusRecordId",
        "censusName",
        "censusCountry",
        "propertyUrl",
        "status",
        "fetchMethod",
        "parseErrors",
      ]
    );
    console.log("Wrote:", BLOCKED_CSV);
  }

  if (!opts.apply) {
    console.log("\nDry-run. Pass --apply to write Amenities (fill-blank, verified HTML only).");
  } else {
    console.log(`\nApplied ${applied} amenity updates.`);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
