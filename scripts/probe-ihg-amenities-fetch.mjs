#!/usr/bin/env node
/**
 * Probe amenity extraction on matched IHG hoteldetail URLs (no inventing).
 *
 *   node scripts/probe-ihg-amenities-fetch.mjs
 *   node scripts/probe-ihg-amenities-fetch.mjs --urls=3
 */
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { IHG_FETCH_HEADERS } from "../lib/ihg-brand-directory-extract.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");

function parseArgs() {
  const args = process.argv.slice(2);
  return {
    limit: Number(args.find((a) => a.startsWith("--urls="))?.split("=")[1] || 5),
  };
}

/**
 * Extract amenity labels only from explicit page structures — never invent.
 * @param {string} html
 */
export function extractIhgAmenitiesFromHtml(html) {
  /** @type {string[]} */
  const labels = [];
  const seen = new Set();

  const push = (raw) => {
    const s = String(raw || "")
      .replace(/<[^>]+>/g, " ")
      .replace(/\s+/g, " ")
      .trim();
    if (!s || s.length < 2 || s.length > 80) return;
    const key = s.toLowerCase();
    if (seen.has(key)) return;
    seen.add(key);
    labels.push(s);
  };

  for (const m of html.matchAll(/class="[^"]*amenity-list__item[^"]*"[^>]*>([\s\S]*?)<\/li>/gi)) {
    push(m[1]);
  }
  for (const m of html.matchAll(/class="[^"]*amenity-list__item[^"]*"[^>]*>([\s\S]*?)<\/(?:div|span|li)>/gi)) {
    push(m[1]);
  }

  // JSON-LD amenityFeature
  for (const block of html.matchAll(/<script type="application\/ld\+json">([\s\S]*?)<\/script>/gi)) {
    try {
      const json = JSON.parse(block[1]);
      const arr = Array.isArray(json) ? json : [json];
      for (const obj of arr) {
        const feats = obj?.amenityFeature;
        const list = Array.isArray(feats) ? feats : feats ? [feats] : [];
        for (const f of list) {
          if (f?.name) push(f.name);
        }
      }
    } catch {
      /* skip */
    }
  }

  // GraphQL/hotel JSON embeds with amenity name arrays (only explicit name fields)
  for (const m of html.matchAll(/"amenityName"\s*:\s*"([^"]+)"/gi)) push(m[1]);
  for (const m of html.matchAll(/"amenity"\s*:\s*\{\s*"name"\s*:\s*"([^"]+)"/gi)) push(m[1]);

  labels.sort((a, b) => a.localeCompare(b));
  return labels;
}

async function main() {
  const opts = parseArgs();
  mkdirSync(REPORTS, { recursive: true });

  const planPath = join(REPORTS, "ihg-census-directory-match-plan.json");
  const extractPath = join(REPORTS, "ihg-cala-directory-extract.json");
  /** @type {string[]} */
  let urls = [];
  if (existsSync(planPath)) {
    const plan = JSON.parse(readFileSync(planPath, "utf8"));
    urls = (plan.planRows || [])
      .filter((r) => r.matchConfidence === "high" || r.matchConfidence === "medium")
      .map((r) => r.propertyUrl)
      .filter(Boolean);
  }
  if (!urls.length && existsSync(extractPath)) {
    const extract = JSON.parse(readFileSync(extractPath, "utf8"));
    urls = (extract.propertyRows || []).map((r) => r.propertyUrl).filter(Boolean);
  }
  if (!urls.length) {
    urls = [
      "https://www.ihg.com/holidayinn/hotels/us/en/santo-domingo/sdqex/hoteldetail",
      "https://www.ihg.com/intercontinental/hotels/us/en/santo-domingo/sdqic/hoteldetail",
      "https://www.ihg.com/hotelindigo/hotels/us/en/mexico-city/mexind/hoteldetail",
    ];
  }

  const sample = urls.slice(0, opts.limit);
  /** @type {object[]} */
  const results = [];

  for (const url of sample) {
    console.log("\n===", url);
    try {
      const res = await fetch(url, {
        headers: IHG_FETCH_HEADERS,
        redirect: "follow",
      });
      const html = await res.text();
      const blocked = /access denied|captcha|robot|akamai|please enable javascript|attention required/i.test(
        html
      );
      const amenities = extractIhgAmenitiesFromHtml(html);
      const amenityListItems = (html.match(/amenity-list__item/gi) || []).length;
      const graphql = /apis\.ihg\.com\/graphql/i.test(html);
      const result = {
        url,
        finalUrl: res.url,
        status: res.status,
        htmlLength: html.length,
        blocked,
        redirectedAwayFromHoteldetail: !/\/hoteldetail/i.test(res.url),
        amenityListItems,
        amenityCount: amenities.length,
        amenitiesSample: amenities.slice(0, 20),
        hasGraphqlConfig: graphql,
        usable: res.ok && !blocked && !/\/explore\/?$/i.test(res.url) && amenities.length > 0,
      };
      results.push(result);
      console.log(
        "status",
        res.status,
        "final",
        res.url.slice(0, 80),
        "blocked",
        blocked,
        "amenities",
        amenities.length,
        "list items",
        amenityListItems
      );
    } catch (err) {
      results.push({ url, error: String(err?.message || err), usable: false });
      console.log("ERR", err.message);
    }
  }

  const usableCount = results.filter((r) => r.usable).length;
  const summary = {
    generatedAt: new Date().toISOString(),
    probed: results.length,
    usableCount,
    amenityBackfillRecommended: usableCount > 0,
    note:
      usableCount === 0
        ? "Amenity fetch blocked or empty — skip Amenities writes."
        : "Amenities extractable from hoteldetail HTML — safe to backfill blanks only.",
    results,
  };

  writeFileSync(join(REPORTS, "ihg-amenities-fetch-probe.json"), JSON.stringify(summary, null, 2));
  console.log("\nUsable:", usableCount, "/", results.length);
  console.log("Wrote reports/ihg-amenities-fetch-probe.json");
  if (!usableCount) {
    console.log("SKIP amenity backfill — fetch did not yield reliable amenity lists.");
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
