#!/usr/bin/env node
/**
 * Wave 9B: 5-property Chrome-channel puppeteer pilot (BWH + Autograph).
 * Pass/fail only — does not write Airtable.
 *
 * Uses Chrome channel (not default Chromium) + isolated userDataDir under
 * data/wave9-chrome-profile (does not attach to your daily Chrome profile).
 *
 *   node scripts/run-wave9-puppeteer-pilot.mjs
 *   node scripts/run-wave9-puppeteer-pilot.mjs --headed
 */
import "../load-env.js";
import { mkdirSync, writeFileSync, existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import puppeteer from "puppeteer";
import { parseMarriottOverviewHtml } from "../lib/marriott-hotel-content-fetch.js";
import {
  fetchBwhHotelDetails,
  extractBwhAmenitiesFromHotelDetails,
} from "../lib/bwh-brand-directory-extract.js";

const HEADED = process.argv.includes("--headed");
const PROFILE = join("data", "wave9-chrome-profile");
const OUT = "reports/wave9-puppeteer-pilot-report.json";
const OUT_MD = "reports/wave9-puppeteer-pilot-report.md";

/** Fixed pilot set: 3 BWH + 2 Marriott Autograph-family */
const PILOT = [
  {
    id: "bwh-70262",
    family: "bwh",
    name: "Best Western Premier Monterrey Aeropuerto",
    propertyId: "70262",
    url: "https://www.bestwestern.com/en_US/book/hotels-in-apodaca/best-western-premier-monterrey-aeropuerto/propertyCode.70262.html",
  },
  {
    id: "bwh-71032",
    family: "bwh",
    name: "Aruba Boutique & Art Hotel BW Signature Collection",
    propertyId: "71032",
    url: "https://www.bestwestern.com/en_US/book/noord/hotel-rooms/aruba-boutique-art-hotel-bw-signature-collection/propertyCode.71032.html",
  },
  {
    id: "bwh-76413",
    family: "bwh",
    name: "BW Signature Collection Libre Hotel",
    propertyId: "76413",
    url: "https://www.bestwestern.com/en_US/book/hotels-in-lima/libre-hotel-bw-signature-collection/propertyCode.76413.html",
  },
  {
    id: "mar-sjuao",
    family: "marriott",
    name: "Hato Rey / Alma San Juan, Autograph Collection",
    propertyId: "SJUAO",
    url: "https://www.marriott.com/en-us/hotels/sjuao-alma-san-juan-puerto-rico-autograph-collection/overview/",
  },
  {
    id: "mar-curak",
    family: "marriott",
    name: "The Pyrmont Curaçao, Autograph Collection",
    propertyId: "CURAK",
    url: "https://www.marriott.com/en-us/hotels/curak-the-pyrmont-curacao-an-autograph-collection-all-inclusive-resort-adults-only/overview/",
  },
];

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function isBlocked(html) {
  const h = String(html || "");
  return /access denied|attention required|captcha|cf-challenge|robot check|perimeterx|akamai/i.test(
    h.slice(0, 8000)
  );
}

function extractMetaDescription(html) {
  const m = String(html || "").match(
    /<meta[^>]+name=["']description["'][^>]+content=["']([^"']+)["']/i
  );
  return m ? m[1].replace(/\s+/g, " ").trim() : "";
}

function extractBwhLabelsFromHtml(html) {
  /** @type {string[]} */
  const labels = [];
  const seen = new Set();
  const push = (raw) => {
    const s = String(raw || "")
      .replace(/<[^>]+>/g, " ")
      .replace(/\s+/g, " ")
      .trim();
    if (!s || s.length < 3 || s.length > 80) return;
    if (/best western|book now|sign in|menu/i.test(s)) return;
    const key = s.toLowerCase();
    if (seen.has(key)) return;
    seen.add(key);
    labels.push(s);
  };
  for (const m of String(html || "").matchAll(
    /data-amenity[^>]*>([^<]+)</gi
  )) {
    push(m[1]);
  }
  for (const m of String(html || "").matchAll(
    /"amenity(?:Name|Label|Description)"\s*:\s*"([^"]+)"/gi
  )) {
    push(m[1]);
  }
  for (const m of String(html || "").matchAll(
    /<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi
  )) {
    try {
      const json = JSON.parse(m[1]);
      const arr = Array.isArray(json) ? json : [json];
      for (const o of arr) {
        const feats = Array.isArray(o?.amenityFeature)
          ? o.amenityFeature
          : o?.amenityFeature
            ? [o.amenityFeature]
            : [];
        for (const f of feats) if (f?.name) push(f.name);
      }
    } catch {
      /* skip */
    }
  }
  return labels.sort((a, b) => a.localeCompare(b));
}

async function fetchWithBrowser(browser, url) {
  const page = await browser.newPage();
  try {
    await page.evaluateOnNewDocument(() => {
      Object.defineProperty(navigator, "webdriver", { get: () => false });
    });
    await page.setUserAgent(
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    );
    await page.setExtraHTTPHeaders({ "Accept-Language": "en-US,en;q=0.9" });
    const resp = await page.goto(url, { waitUntil: "networkidle2", timeout: 120000 });
    await sleep(3500);
    const html = await page.content();
    return {
      finalUrl: page.url(),
      title: await page.title(),
      status: resp?.status() || 0,
      html,
      htmlLen: html.length,
      blocked: isBlocked(html),
    };
  } finally {
    await page.close();
  }
}

async function evaluatePilot(browser, item) {
  const started = Date.now();
  /** @type {object} */
  const result = {
    id: item.id,
    family: item.family,
    name: item.name,
    propertyId: item.propertyId,
    url: item.url,
    pass: false,
    reasons: [],
    descriptionLen: 0,
    amenityCount: 0,
    descriptionPreview: "",
    amenitySample: [],
    elapsedMs: 0,
  };

  try {
    const got = await fetchWithBrowser(browser, item.url);
    result.fetch = {
      status: got.status,
      title: got.title,
      finalUrl: got.finalUrl,
      htmlLen: got.htmlLen,
      blocked: got.blocked,
    };

    if (got.blocked || got.htmlLen < 5000) {
      result.reasons.push(got.blocked ? "page_blocked" : "html_too_small");
    }

    if (item.family === "marriott") {
      const parsed = parseMarriottOverviewHtml(got.html);
      result.descriptionLen = (parsed.description || "").length;
      result.amenityCount = (parsed.amenities || []).length;
      result.descriptionPreview = String(parsed.description || "").slice(0, 140);
      result.amenitySample = (parsed.amenities || []).slice(0, 8);
      if (result.descriptionLen >= 40) result.reasons.push("description_ok");
      if (result.amenityCount >= 3) result.reasons.push("amenities_ok");
      result.pass = result.descriptionLen >= 40 || result.amenityCount >= 3;
      if (!result.pass && !got.blocked) result.reasons.push("parsed_empty");
    } else {
      // BWH: HTML parse + hotelDetails proxy from same network via page context optional
      const labels = extractBwhLabelsFromHtml(got.html);
      const meta = extractMetaDescription(got.html);
      result.descriptionLen = meta.length;
      result.amenityCount = labels.length;
      result.descriptionPreview = meta.slice(0, 140);
      result.amenitySample = labels.slice(0, 8);

      const api = await fetchBwhHotelDetails(item.propertyId);
      result.hotelDetails = {
        ok: api.ok,
        status: api.status,
        error: api.error || null,
      };
      if (api.ok) {
        const apiLabels = extractBwhAmenitiesFromHotelDetails(api.json);
        if (apiLabels.length > result.amenityCount) {
          result.amenityCount = apiLabels.length;
          result.amenitySample = apiLabels.slice(0, 8);
          result.reasons.push("hotelDetails_amenities");
        }
      } else {
        result.reasons.push(`hotelDetails_${api.error || api.status || "fail"}`);
      }

      if (result.descriptionLen >= 40) result.reasons.push("meta_description_ok");
      if (result.amenityCount >= 3) result.reasons.push("amenities_ok");
      result.pass = result.descriptionLen >= 40 || result.amenityCount >= 3;
      if (!result.pass && !got.blocked) result.reasons.push("parsed_empty");
    }
  } catch (err) {
    result.reasons.push("error");
    result.error = String(err?.message || err);
  }

  result.elapsedMs = Date.now() - started;
  return result;
}

async function main() {
  mkdirSync("reports", { recursive: true });
  mkdirSync(PROFILE, { recursive: true });

  console.log("Launching Chrome channel puppeteer…");
  console.log("  profile:", PROFILE);
  console.log("  headed:", HEADED);

  let browser;
  try {
    browser = await puppeteer.launch({
      headless: HEADED ? false : "new",
      channel: "chrome",
      userDataDir: PROFILE,
      args: [
        "--no-sandbox",
        "--disable-blink-features=AutomationControlled",
        "--disable-dev-shm-usage",
      ],
    });
  } catch (err) {
    console.error("Chrome launch failed:", err?.message || err);
    console.error("Is Google Chrome installed? Falling back to default Chromium…");
    browser = await puppeteer.launch({
      headless: HEADED ? false : "new",
      userDataDir: PROFILE,
      args: ["--no-sandbox", "--disable-blink-features=AutomationControlled"],
    });
  }

  /** @type {object[]} */
  const results = [];
  try {
    for (const item of PILOT) {
      console.log(`\n=== ${item.id} ${item.name} ===`);
      const row = await evaluatePilot(browser, item);
      results.push(row);
      console.log(
        `  pass=${row.pass} desc=${row.descriptionLen} amen=${row.amenityCount} reasons=${row.reasons.join(",")}`
      );
      await sleep(1500);
    }
  } finally {
    await browser.close();
  }

  const passed = results.filter((r) => r.pass).length;
  const report = {
    generatedAt: new Date().toISOString(),
    mode: HEADED ? "headed" : "headless",
    profile: PROFILE,
    pilotCount: results.length,
    passed,
    failed: results.length - passed,
    verdict:
      passed >= 3
        ? "promising — expand puppeteer path for this family"
        : passed >= 1
          ? "mixed — keep steward browser-save as primary"
          : "blocked — do not invest in automation; steward-only",
    results,
  };

  writeFileSync(OUT, JSON.stringify(report, null, 2));

  const md = [
    "# Wave 9 puppeteer pilot report",
    "",
    `**Generated:** ${report.generatedAt}`,
    `**Mode:** ${report.mode} · Chrome channel · profile \`${PROFILE}\``,
    `**Pass:** ${passed}/${results.length}`,
    `**Verdict:** ${report.verdict}`,
    "",
    "| ID | Family | Hotel | Pass | Desc len | Amenities | Reasons |",
    "|----|--------|-------|------|---------:|----------:|---------|",
    ...results.map(
      (r) =>
        `| ${r.id} | ${r.family} | ${r.name.replace(/\|/g, "/")} | ${r.pass ? "YES" : "no"} | ${r.descriptionLen} | ${r.amenityCount} | ${(r.reasons || []).join("; ")} |`
    ),
    "",
    "## Implication",
    "",
    report.verdict.includes("promising")
      ? "- Schedule a larger puppeteer batch for the family that passed."
      : report.verdict.includes("mixed")
        ? "- Keep Choice/BWH/Marriott steward openers as primary; use puppeteer only where pass=true."
        : "- Stop automation investment for these blocked families; finish Choice next-20 steward sprint instead.",
    "",
    `JSON: \`${OUT}\``,
    "",
  ].join("\n");
  writeFileSync(OUT_MD, md);

  console.log(`\nPass ${passed}/${results.length}`);
  console.log("Verdict:", report.verdict);
  console.log("Report:", OUT_MD);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
