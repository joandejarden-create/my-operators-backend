#!/usr/bin/env node
/**
 * Wave 5: fill-blank Hotel Description for Design Hotels from designhotels.com pages.
 *
 *   node scripts/backfill-design-hotels-wave5-descriptions.mjs
 *   node scripts/backfill-design-hotels-wave5-descriptions.mjs --apply
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { CENSUS_DESCRIPTION_FIELD } from "../lib/hotel-census/hilton-description-enrichment-contract.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { isCalaCountry } from "../lib/design-hotels-census-enrichment.js";

const APPLY = process.argv.includes("--apply");
const UA = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  Accept: "text/html",
};

function extractDesc(html) {
  const scripts = [
    ...String(html || "").matchAll(
      /<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi
    ),
  ];
  for (const m of scripts) {
    try {
      const json = JSON.parse(m[1]);
      const arr = Array.isArray(json) ? json : [json];
      for (const o of arr) {
        const types = Array.isArray(o["@type"]) ? o["@type"] : [o["@type"]];
        if (!types.some((t) => /Hotel|Lodging|Resort/i.test(String(t)))) continue;
        const d = String(o.description || "").replace(/\s+/g, " ").trim();
        if (d.length >= 60) return d;
      }
    } catch {
      /* skip */
    }
  }
  const meta = html.match(/<meta[^>]+name=["']description["'][^>]+content=["']([^"']+)["']/i);
  if (meta) {
    const d = meta[1].replace(/\s+/g, " ").trim();
    if (d.length >= 60) return d;
  }
  return "";
}

async function main() {
  mkdirSync("reports", { recursive: true });
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );
  const records = await base(HOTEL_CENSUS_TABLE)
    .select({
      fields: ["name", CENSUS_FIELDS.country, "Website", CENSUS_DESCRIPTION_FIELD],
      filterByFormula: `{${CENSUS_FIELDS.affiliation}}="Design Hotels"`,
      pageSize: 100,
    })
    .all();

  const planRows = [];
  const skipped = [];
  for (const rec of records) {
    if (!isCalaCountry(rec.fields[CENSUS_FIELDS.country])) continue;
    if (!isBlankCensusValue(rec.fields[CENSUS_DESCRIPTION_FIELD])) {
      skipped.push({ id: rec.id, reason: "present" });
      continue;
    }
    const website = String(rec.fields.Website || "").trim();
    if (!/designhotels\.com/i.test(website)) {
      skipped.push({ id: rec.id, name: rec.fields.name, reason: "no_designhotels_url" });
      continue;
    }
    try {
      const res = await fetch(website, { headers: UA, redirect: "follow" });
      const html = await res.text();
      const desc = extractDesc(html);
      if (!desc) {
        skipped.push({ id: rec.id, name: rec.fields.name, reason: "empty", status: res.status });
        continue;
      }
      planRows.push({
        censusRecordId: rec.id,
        censusName: rec.fields.name,
        website,
        applyFields: { [CENSUS_DESCRIPTION_FIELD]: desc },
      });
    } catch (err) {
      skipped.push({ id: rec.id, name: rec.fields.name, reason: "error", error: String(err?.message || err) });
    }
  }

  writeFileSync(
    "reports/design-hotels-wave5-descriptions-plan.json",
    JSON.stringify(
      { generatedAt: new Date().toISOString(), mode: APPLY ? "apply" : "dry-run", readyToApply: planRows.length, planRows, skipped },
      null,
      2
    )
  );
  console.log("Ready:", planRows.length);
  for (const r of planRows) console.log(" ", r.censusName, "→", r.applyFields[CENSUS_DESCRIPTION_FIELD].slice(0, 100));
  if (!APPLY) {
    console.log("DRY-RUN");
    return;
  }
  for (const row of planRows) {
    await base(HOTEL_CENSUS_TABLE).update([{ id: row.censusRecordId, fields: row.applyFields }], {
      typecast: true,
    });
  }
  console.log("Updated:", planRows.length);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
