#!/usr/bin/env node
/**
 * Wave 10: Chrome-channel puppeteer pilot on Choice next-20 sample.
 * If amenity markers parse, save HTML under reports/choice-amenity-html/.
 *
 *   node scripts/run-wave10-choice-puppeteer-pilot.mjs
 *   node scripts/run-wave10-choice-puppeteer-pilot.mjs --limit=8 --apply-html
 */
import { mkdirSync, writeFileSync, readFileSync, existsSync } from "node:fs";
import { join } from "node:path";
import puppeteer from "puppeteer";
import {
  parseChoiceAmenitiesFromHtml,
  hasChoiceAmenityMarkers,
} from "../lib/choice-hotel-content-fetch.js";

const SPRINT = "reports/choice-next20-steward-sprint.json";
const HTML_DIR = "reports/choice-amenity-html";
const PROFILE = join("data", "wave9-chrome-profile");
const APPLY_HTML = process.argv.includes("--apply-html");
const limitArg = process.argv.find((a) => a.startsWith("--limit="));
const LIMIT = limitArg ? Number(limitArg.split("=")[1]) : 8;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function isBlocked(html) {
  return /access denied|attention required|captcha|robot check|akamai/i.test(
    String(html || "").slice(0, 8000)
  );
}

function extractMetaDescription(html) {
  const m = String(html || "").match(
    /<meta[^>]+name=["']description["'][^>]+content=["']([^"']+)["']/i
  );
  return m ? m[1].replace(/\s+/g, " ").trim() : "";
}

async function main() {
  mkdirSync(HTML_DIR, { recursive: true });
  mkdirSync(PROFILE, { recursive: true });
  if (!existsSync(SPRINT)) throw new Error(`Missing ${SPRINT}`);
  const sprint = JSON.parse(readFileSync(SPRINT, "utf8"));
  const rows = (sprint.top20 || []).slice(0, LIMIT);

  console.log(`Choice puppeteer pilot: ${rows.length} URLs (save HTML=${APPLY_HTML})`);

  const browser = await puppeteer.launch({
    headless: "new",
    channel: "chrome",
    userDataDir: PROFILE,
    args: ["--no-sandbox", "--disable-blink-features=AutomationControlled"],
  });

  const results = [];
  try {
    for (let i = 0; i < rows.length; i++) {
      const r = rows[i];
      console.log(`\n[${i + 1}/${rows.length}] ${r.propertyId} ${r.censusName}`);
      const page = await browser.newPage();
      /** @type {object} */
      const out = {
        propertyId: r.propertyId,
        censusName: r.censusName,
        website: r.website,
        pass: false,
        reasons: [],
      };
      try {
        await page.evaluateOnNewDocument(() => {
          Object.defineProperty(navigator, "webdriver", { get: () => false });
        });
        await page.setUserAgent(
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        );
        const resp = await page.goto(r.website, {
          waitUntil: "networkidle2",
          timeout: 120000,
        });
        await sleep(4000);
        const html = await page.content();
        out.status = resp?.status() || 0;
        out.htmlLen = html.length;
        out.blocked = isBlocked(html);
        const parsed = parseChoiceAmenitiesFromHtml(html);
        out.amenityCount = parsed.amenities?.length || 0;
        out.hasMarkers = Boolean(parsed.hasAmenityMarkers || hasChoiceAmenityMarkers?.(html));
        out.metaDescLen = extractMetaDescription(html).length;
        out.amenitySample = (parsed.amenities || []).slice(0, 6);

        if (out.blocked) out.reasons.push("blocked");
        if (out.hasMarkers && out.amenityCount >= 3) {
          out.pass = true;
          out.reasons.push("amenities_ok");
          if (APPLY_HTML) {
            const path = join(HTML_DIR, `${String(r.propertyId).toLowerCase()}.html`);
            writeFileSync(path, html);
            out.savedHtml = path;
            out.reasons.push("html_saved");
          }
        } else if (out.metaDescLen >= 80 && !out.blocked) {
          out.reasons.push("meta_only");
          // Still save if apply-html — description path can use it
          if (APPLY_HTML && out.metaDescLen >= 80) {
            const path = join(HTML_DIR, `${String(r.propertyId).toLowerCase()}.html`);
            writeFileSync(path, html);
            out.savedHtml = path;
            out.pass = true;
            out.reasons.push("html_saved_meta");
          }
        } else if (!out.blocked) {
          out.reasons.push("no_amenity_markers");
        }
      } catch (err) {
        out.reasons.push("error");
        out.error = String(err?.message || err);
      } finally {
        await page.close();
      }
      results.push(out);
      console.log(
        `  pass=${out.pass} amen=${out.amenityCount} markers=${out.hasMarkers} blocked=${out.blocked} ${out.reasons.join(",")}`
      );
      await sleep(1500);
    }
  } finally {
    await browser.close();
  }

  const passed = results.filter((r) => r.pass).length;
  const report = {
    generatedAt: new Date().toISOString(),
    limit: LIMIT,
    applyHtml: APPLY_HTML,
    passed,
    failed: results.length - passed,
    verdict:
      passed >= Math.ceil(rows.length / 2)
        ? "promising — batch Choice puppeteer for next-20"
        : passed >= 1
          ? "mixed — save only passes; steward for the rest"
          : "blocked — steward-only for Choice",
    results,
  };
  writeFileSync("reports/wave10-choice-puppeteer-pilot-report.json", JSON.stringify(report, null, 2));
  writeFileSync(
    "reports/wave10-choice-puppeteer-pilot-report.md",
    [
      "# Wave 10 Choice puppeteer pilot",
      "",
      `**Pass:** ${passed}/${results.length}`,
      `**Verdict:** ${report.verdict}`,
      "",
      "| PID | Hotel | Pass | Amenities | Markers | Reasons |",
      "|-----|-------|------|----------:|:-------:|---------|",
      ...results.map(
        (r) =>
          `| ${r.propertyId} | ${String(r.censusName || "").replace(/\|/g, "/")} | ${r.pass ? "YES" : "no"} | ${r.amenityCount || 0} | ${r.hasMarkers ? "yes" : "no"} | ${(r.reasons || []).join("; ")} |`
      ),
      "",
    ].join("\n")
  );
  console.log(`\nPass ${passed}/${results.length}`);
  console.log("Verdict:", report.verdict);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
