#!/usr/bin/env node
/**
 * Wave 5: fill-blank Hotel Description for Accor Active brands (MGallery, Handwritten)
 * from official all.accor.com / brand property pages (JSON-LD description or meta).
 *
 *   node scripts/backfill-accor-wave5-descriptions.mjs
 *   node scripts/backfill-accor-wave5-descriptions.mjs --apply
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import Airtable from "airtable";
import { ACCOR_FETCH_HEADERS } from "../lib/accor-brand-directory-extract.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { CENSUS_DESCRIPTION_FIELD } from "../lib/hotel-census/hilton-description-enrichment-contract.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { isCalaCountry } from "../lib/design-hotels-census-enrichment.js";

const APPLY = process.argv.includes("--apply");
const AFFS = ["MGallery Collection", "Handwritten Collection"];
const DELAY = 300;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function extractJsonLdHotelDescription(html) {
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
        if (!o || typeof o !== "object") continue;
        const types = Array.isArray(o["@type"]) ? o["@type"] : [o["@type"]];
        if (!types.some((t) => /Hotel|LodgingBusiness|Resort/i.test(String(t)))) continue;
        const d = String(o.description || "")
          .replace(/\s+/g, " ")
          .trim();
        if (d.length >= 60) return d;
      }
    } catch {
      /* skip */
    }
  }
  return "";
}

function extractMetaDescription(html) {
  const m = String(html || "").match(
    /<meta[^>]+name=["']description["'][^>]+content=["']([^"']+)["']/i
  );
  if (m) return m[1].replace(/\s+/g, " ").trim();
  const m2 = String(html || "").match(
    /content=["']([^"']+)["'][^>]+name=["']description["']/i
  );
  return m2 ? m2[1].replace(/\s+/g, " ").trim() : "";
}

function isUsable(desc) {
  const t = String(desc || "").trim();
  if (t.length < 60) return false;
  if (/^all\s*-\s*accor/i.test(t)) return false;
  if (/find a hotel|book your stay/i.test(t) && t.length < 100) return false;
  return true;
}

async function fetchDescription(url) {
  const res = await fetch(url, { headers: ACCOR_FETCH_HEADERS, redirect: "follow" });
  if (!res.ok) return { ok: false, status: res.status, description: "" };
  const html = await res.text();
  const fromLd = extractJsonLdHotelDescription(html);
  const fromMeta = extractMetaDescription(html);
  const description = isUsable(fromLd) ? fromLd : isUsable(fromMeta) ? fromMeta : "";
  return { ok: Boolean(description), status: res.status, description, fromLd, fromMeta };
}

async function main() {
  mkdirSync("reports", { recursive: true });
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );
  const formula = `OR(${AFFS.map((a) => `{${CENSUS_FIELDS.affiliation}}="${a}"`).join(",")})`;
  const records = await base(HOTEL_CENSUS_TABLE)
    .select({
      fields: [
        "name",
        CENSUS_FIELDS.affiliation,
        CENSUS_FIELDS.country,
        "Website",
        CENSUS_DESCRIPTION_FIELD,
      ],
      filterByFormula: formula,
      pageSize: 100,
    })
    .all();

  const planRows = [];
  const skipped = [];
  let n = 0;
  for (const rec of records) {
    if (!isCalaCountry(rec.fields[CENSUS_FIELDS.country])) continue;
    if (!isBlankCensusValue(rec.fields[CENSUS_DESCRIPTION_FIELD])) {
      skipped.push({ id: rec.id, reason: "description_present" });
      continue;
    }
    const website = String(rec.fields.Website || "").trim();
    if (!website || !/accor\.com/i.test(website)) {
      skipped.push({
        id: rec.id,
        name: rec.fields.name,
        reason: "no_accor_website",
        website,
      });
      continue;
    }
    n++;
    console.log(` [${n}] ${rec.fields.name}`);
    try {
      const got = await fetchDescription(website);
      await sleep(DELAY);
      if (!got.ok) {
        skipped.push({
          id: rec.id,
          name: rec.fields.name,
          reason: "empty_description",
          status: got.status,
        });
        continue;
      }
      planRows.push({
        censusRecordId: rec.id,
        censusName: rec.fields.name,
        affiliation: rec.fields[CENSUS_FIELDS.affiliation],
        website,
        applyFields: { [CENSUS_DESCRIPTION_FIELD]: got.description },
      });
    } catch (err) {
      skipped.push({
        id: rec.id,
        name: rec.fields.name,
        reason: "fetch_error",
        error: String(err?.message || err),
      });
    }
  }

  writeFileSync(
    "reports/accor-wave5-descriptions-plan.json",
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        mode: APPLY ? "apply" : "dry-run",
        readyToApply: planRows.length,
        planRows,
        skipped,
      },
      null,
      2
    )
  );
  console.log("\nReady:", planRows.length);
  for (const r of planRows) {
    console.log(" ", r.affiliation, "|", r.censusName, "|", r.applyFields[CENSUS_DESCRIPTION_FIELD].slice(0, 100));
  }

  if (!APPLY) {
    console.log("DRY-RUN");
    return;
  }
  for (const row of planRows) {
    await base(HOTEL_CENSUS_TABLE).update([{ id: row.censusRecordId, fields: row.applyFields }], {
      typecast: true,
    });
  }
  writeFileSync(
    "reports/accor-wave5-descriptions-apply-log.json",
    JSON.stringify({ generatedAt: new Date().toISOString(), updated: planRows.length, planRows }, null, 2)
  );
  console.log("Updated:", planRows.length);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
