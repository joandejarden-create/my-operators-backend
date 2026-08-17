#!/usr/bin/env node
/**
 * Sibling IHG: fill-blank Hotel Description for CALA IHG-parent rows that are
 * NOT Active soft brands, from official ihg.com property pages (JSON-LD / meta).
 *
 *   node scripts/backfill-ihg-sibling-descriptions.mjs
 *   node scripts/backfill-ihg-sibling-descriptions.mjs --apply --limit=80
 *   node scripts/backfill-ihg-sibling-descriptions.mjs --affiliations="Holiday Inn|Holiday Inn Express"
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import Airtable from "airtable";
import { BRAND_STATUS_ACTIVE_FORMULA } from "../lib/brand-status-active.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { CENSUS_DESCRIPTION_FIELD } from "../lib/hotel-census/hilton-description-enrichment-contract.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { isCalaCountry } from "../lib/design-hotels-census-enrichment.js";

const APPLY = process.argv.includes("--apply");
const limitArg = process.argv.find((a) => a.startsWith("--limit="));
const LIMIT = limitArg ? Number(limitArg.split("=")[1]) : 80;
const affArg = process.argv.find((a) => a.startsWith("--affiliations="))?.split("=")[1];
const AFF_FILTER = affArg
  ? affArg.split("|").map((s) => s.trim()).filter(Boolean)
  : null;
const DELAY = 350;
const UA = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  Accept: "text/html,application/xhtml+xml",
  "Accept-Language": "en-US,en;q=0.9",
};

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function decodeHtmlEntities(text) {
  return String(text || "")
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&apos;/g, "'")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/\s+/g, " ")
    .trim();
}

function extractDesc(html) {
  for (const m of String(html || "").matchAll(
    /<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi
  )) {
    try {
      const json = JSON.parse(m[1]);
      const arr = Array.isArray(json) ? json : [json];
      for (const o of arr) {
        if (!o || typeof o !== "object") continue;
        const types = Array.isArray(o["@type"]) ? o["@type"] : [o["@type"]];
        if (!types.some((t) => /Hotel|LodgingBusiness|Resort/i.test(String(t)))) continue;
        const d = decodeHtmlEntities(o.description || "");
        if (d.length >= 60) return d;
      }
    } catch {
      /* skip */
    }
  }
  const meta = String(html || "").match(
    /<meta[^>]+name=["']description["'][^>]+content=["']([^"']+)["']/i
  );
  if (meta) {
    const d = decodeHtmlEntities(meta[1]);
    if (d.length >= 80 && !/^ihg\b/i.test(d)) return d;
  }
  return "";
}

async function main() {
  mkdirSync("reports", { recursive: true });
  const apiKey = process.env.AIRTABLE_API_KEY;
  const mvp = new Airtable({ apiKey }).base(process.env.AIRTABLE_BASE_ID);
  const plat = new Airtable({ apiKey }).base(process.env.AIRTABLE_BASE_ID_ALT);

  const active = new Set(
    (
      await mvp("Brand Setup - Brand Basics")
        .select({ fields: ["Brand Name"], filterByFormula: BRAND_STATUS_ACTIVE_FORMULA })
        .all()
    )
      .map((r) => String(r.fields["Brand Name"] || "").trim())
      .filter(Boolean)
  );

  const records = await plat(HOTEL_CENSUS_TABLE)
    .select({
      fields: [
        "name",
        CENSUS_FIELDS.affiliation,
        CENSUS_FIELDS.parentCompany,
        CENSUS_FIELDS.country,
        "Website",
        CENSUS_DESCRIPTION_FIELD,
      ],
      filterByFormula: `OR(FIND("IHG", {${CENSUS_FIELDS.parentCompany}}), FIND("InterContinental", {${CENSUS_FIELDS.parentCompany}}))`,
      pageSize: 100,
    })
    .all();

  /** @type {import('airtable').Record[]} */
  const candidates = [];
  for (const rec of records) {
    if (!isCalaCountry(rec.fields[CENSUS_FIELDS.country])) continue;
    const aff = String(rec.fields[CENSUS_FIELDS.affiliation] || "").trim();
    if (!aff || active.has(aff)) continue;
    if (AFF_FILTER && !AFF_FILTER.some((a) => a.toLowerCase() === aff.toLowerCase())) continue;
    if (!isBlankCensusValue(rec.fields[CENSUS_DESCRIPTION_FIELD])) continue;
    const website = String(rec.fields.Website || "").trim();
    if (!website || !/ihg\.com/i.test(website)) continue;
    candidates.push(rec);
  }

  const work = candidates.slice(0, LIMIT);
  console.log(`IHG sibling candidates: ${candidates.length}; working: ${work.length}`);

  const planRows = [];
  const skipped = [];
  let n = 0;
  for (const rec of work) {
    n++;
    const website = String(rec.fields.Website || "").trim();
    console.log(` [${n}/${work.length}] ${rec.fields[CENSUS_FIELDS.affiliation]} | ${rec.fields.name}`);
    try {
      const res = await fetch(website, { headers: UA, redirect: "follow" });
      const html = await res.text();
      await sleep(DELAY);
      const desc = extractDesc(html);
      if (!desc) {
        skipped.push({
          id: rec.id,
          name: rec.fields.name,
          reason: "empty_or_blocked",
          status: res.status,
          htmlLen: html.length,
        });
        continue;
      }
      planRows.push({
        censusRecordId: rec.id,
        censusName: rec.fields.name,
        affiliation: rec.fields[CENSUS_FIELDS.affiliation],
        website,
        applyFields: { [CENSUS_DESCRIPTION_FIELD]: desc },
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

  const byAff = {};
  for (const r of planRows) byAff[r.affiliation] = (byAff[r.affiliation] || 0) + 1;

  writeFileSync(
    "reports/ihg-sibling-descriptions-plan.json",
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        mode: APPLY ? "apply" : "dry-run",
        limit: LIMIT,
        readyToApply: planRows.length,
        byAffiliation: byAff,
        planRows,
        skipped,
      },
      null,
      2
    )
  );
  console.log("\nReady:", planRows.length, byAff);
  console.log("Skipped:", skipped.length);
  const blocked = skipped.filter((s) => s.reason === "empty_or_blocked").length;
  if (blocked) console.log("  empty_or_blocked:", blocked);

  if (!APPLY) {
    console.log("DRY-RUN");
    return;
  }

  let updated = 0;
  let batch = [];
  async function flush() {
    if (!batch.length) return;
    await plat(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
    updated += batch.length;
    batch = [];
  }
  for (const row of planRows) {
    batch.push({ id: row.censusRecordId, fields: row.applyFields });
    if (batch.length >= 10) await flush();
  }
  await flush();
  writeFileSync(
    "reports/ihg-sibling-descriptions-apply-log.json",
    JSON.stringify({ generatedAt: new Date().toISOString(), updated, byAff, planRows }, null, 2)
  );
  console.log("Updated:", updated);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
