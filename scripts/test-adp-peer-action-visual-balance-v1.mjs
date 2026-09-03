#!/usr/bin/env node
/**
 * PEER_TEXT_VISUAL_BALANCE · CUSTOMER_ACTION_PROPER_CASE · PEER_ACTION_TEXT_IMBALANCE
 *
 * Static scan of ADP UI sources + Playwright geometry for peer evidence actions.
 *
 *   node scripts/test-adp-peer-action-visual-balance-v1.mjs
 *   ADP_QA_BASE=http://127.0.0.1:8080 node scripts/test-adp-peer-action-visual-balance-v1.mjs --playwright
 */
import { readFileSync, writeFileSync, mkdirSync, existsSync } from "fs";
import { join } from "path";

export const PEER_TEXT_VISUAL_BALANCE = "PEER_TEXT_VISUAL_BALANCE";
export const CUSTOMER_ACTION_PROPER_CASE = "CUSTOMER_ACTION_PROPER_CASE";
export const PEER_ACTION_TEXT_IMBALANCE = "PEER_ACTION_TEXT_IMBALANCE";

const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
const OUT = join(process.cwd(), "reports/ai-demand-positioning/peer-action-visual-balance", stamp);
mkdirSync(OUT, { recursive: true });

const ADP_JS = join(process.cwd(), "public/js/ai-demand-positioning/ai-demand-positioning.js");
const source = readFileSync(ADP_JS, "utf8");

const forbiddenLowerActions = [
  /View missing/i,
  /View examples/i,
  /Show more(?!["\w])/i,
  /Show full response/i,
  /Show less(?!["\w])/i,
  /View \d+ missing/i,
];

// After fix we require the peer helper + Proper Case literals
const required = [
  { re: /adpPeerEvidenceActionHtml/, label: "peer_helper" },
  { re: /adp-evidence-action-btn/, label: "peer_css_class" },
  { re: /"View Missing"|'View Missing'|aria-label="' \+\s*aria/, label: "aria_view_missing_or_dynamic" },
  { re: /Show More/, label: "show_more_proper" },
  { re: /Jump to Mention|formatAiResponse/, label: "ai_response_verbatim_path" },
];

const defects = [];

// Gold case regression: must NOT contain one-line lowercase peer action button labels
if (/>View missing</.test(source) || />View examples</.test(source)) {
  defects.push({
    code: PEER_ACTION_TEXT_IMBALANCE,
    detail: "Gold case: lowercase one-line View missing / View examples still present in source",
  });
}
if (/View ' \+ missing \+ ' missing/.test(source) || /View " \+ missing \+ " missing/.test(source)) {
  defects.push({
    code: PEER_ACTION_TEXT_IMBALANCE,
    detail: "Provider missing action still embeds count in label (breaks peer balance)",
  });
}
if (/>Show more</.test(source)) {
  defects.push({
    code: CUSTOMER_ACTION_PROPER_CASE,
    detail: "Drawer actions not Proper Case",
  });
}

for (const r of required) {
  if (!r.re.test(source)) {
    defects.push({ code: PEER_TEXT_VISUAL_BALANCE, detail: `missing ${r.label}` });
  }
}

const cssPath = join(process.cwd(), "public/js/ai-demand-positioning/ai-demand-positioning.css");
const css = readFileSync(cssPath, "utf8");
if (!css.includes("adp-evidence-action-btn") || !css.includes("PEER_TEXT_VISUAL_BALANCE")) {
  defects.push({ code: PEER_TEXT_VISUAL_BALANCE, detail: "CSS peer action rules missing" });
}

let playwright = null;
const runPw = process.argv.includes("--playwright");

if (runPw) {
  const { chromium } = await import("playwright");
  const BASE = process.env.ADP_QA_BASE || "http://127.0.0.1:8080";
  const VIEWPORTS = [
    { name: "1440", width: 1440, height: 900 },
    { name: "1280", width: 1280, height: 800 },
    { name: "1024", width: 1024, height: 768 },
    { name: "768", width: 768, height: 1024 },
    { name: "390", width: 390, height: 844 },
  ];
  const browser = await chromium.launch({ headless: true });
  playwright = { failed: 0, results: [] };

  for (const vp of VIEWPORTS) {
    const context = await browser.newContext({ viewport: { width: vp.width, height: vp.height } });
    const page = await context.newPage();
    await page.goto(
      `${BASE}/owner-ai-demand.html?propertyId=adp_hotel_phillips_kansas_city&v=peer-balance`,
      { waitUntil: "networkidle", timeout: 90000 }
    );
    await page.waitForSelector("#adpIntentTableContainer .adp-evidence-action-btn, #adpProviderTableContainer .adp-evidence-action-btn", {
      timeout: 60000,
    });

    const geo = await page.evaluate(() => {
      function measurePair(row) {
        const miss = row.querySelector('[data-adp-evidence-type="missing"].adp-evidence-action-btn');
        const pos = row.querySelector('[data-adp-evidence-type="present"].adp-evidence-action-btn');
        if (!miss || !pos) return null;
        const m = miss.getBoundingClientRect();
        const p = pos.getBoundingClientRect();
        const mLines = miss.querySelectorAll(".adp-evidence-action__line").length;
        const pLines = pos.querySelectorAll(".adp-evidence-action__line").length;
        return {
          missH: m.height,
          posH: p.height,
          missY: m.top,
          posY: p.top,
          mLines,
          pLines,
          missLabel: (miss.getAttribute("aria-label") || "").trim(),
          posLabel: (pos.getAttribute("aria-label") || "").trim(),
          heightDelta: Math.abs(m.height - p.height),
          yDelta: Math.abs(m.top - p.top),
        };
      }
      const intentRows = [...document.querySelectorAll("#adpIntentTableContainer tbody tr")];
      const providerRows = [...document.querySelectorAll("#adpProviderTableContainer tbody tr")];
      const pairs = [];
      for (const row of intentRows) {
        const m = measurePair(row);
        if (m) pairs.push({ surface: "demand_territory", ...m });
      }
      for (const row of providerRows) {
        const m = measurePair(row);
        if (m) pairs.push({ surface: "provider_presence", ...m });
      }
      return pairs;
    });

    const rowDefects = [];
    for (const pair of geo) {
      if (pair.mLines !== 2 || pair.pLines !== 2) {
        rowDefects.push({ code: PEER_ACTION_TEXT_IMBALANCE, detail: `line count ${pair.mLines}/${pair.pLines}` });
      }
      if (pair.missLabel !== "View Missing" || pair.posLabel !== "View Examples") {
        rowDefects.push({
          code: CUSTOMER_ACTION_PROPER_CASE,
          detail: `aria ${pair.missLabel} / ${pair.posLabel}`,
        });
      }
      // Material imbalance: >6px height or >4px y drift
      if (pair.heightDelta > 6 || pair.yDelta > 4) {
        rowDefects.push({
          code: PEER_ACTION_TEXT_IMBALANCE,
          detail: `geometry hΔ=${pair.heightDelta.toFixed(1)} yΔ=${pair.yDelta.toFixed(1)}`,
        });
      }
    }

    const pass = rowDefects.length === 0 && geo.length > 0;
    if (!pass) playwright.failed += 1;
    playwright.results.push({
      viewport: vp.name,
      pairs: geo.length,
      sample: geo.slice(0, 2),
      defects: rowDefects,
      pass,
    });
    await context.close();
  }
  await browser.close();
}

const status =
  defects.length === 0 && (!playwright || playwright.failed === 0) ? "PASS" : "FAIL";

const report = {
  title: "ADP_PEER_ACTION_VISUAL_BALANCE_V1",
  gates: [PEER_TEXT_VISUAL_BALANCE, CUSTOMER_ACTION_PROPER_CASE, PEER_ACTION_TEXT_IMBALANCE],
  stamp,
  status,
  staticDefects: defects,
  playwright,
};
writeFileSync(join(OUT, "peer-action-report.json"), JSON.stringify(report, null, 2) + "\n");
console.log(JSON.stringify({ status, stamp, staticDefects: defects.length, playwrightFailed: playwright?.failed ?? null, out: join(OUT, "peer-action-report.json") }, null, 2));
if (status !== "PASS") process.exitCode = 1;
