#!/usr/bin/env node
/**
 * ADP visual regression recovery gates.
 * npm run test:adp-visual-regression-recovery-v1
 */
import { readFileSync, mkdirSync, writeFileSync } from "fs";
import { join } from "path";
import { chromium } from "playwright";
import {
  issueShareCapability,
  revokeShareCapability,
} from "../lib/ai-demand-positioning/share/adp-signed-share-capability-v1.js";
import "../load-env.js";

process.env.ADP_SHARE_CAPABILITY_ALLOW_DEV_SECRET = "1";
process.env.ADP_SHARE_CAPABILITY_ENFORCE = "1";

const BASE = process.env.ADP_BASE_URL || "http://127.0.0.1:8080";
const OUT = join(process.cwd(), "reports/ai-demand-positioning/visual-regression-recovery");
mkdirSync(OUT, { recursive: true });

const css = readFileSync(
  join(process.cwd(), "public/js/ai-demand-positioning/ai-demand-positioning.css"),
  "utf8"
);

const gates = {};
function mark(name, pass, detail = null) {
  gates[name] = pass ? "PASS" : "FAIL";
  if (!pass) throw new Error(`${name}${detail ? `: ${detail}` : ""}`);
}

// Static CSS ownership
mark(
  "BPP_DESKTOP_SIX_KPI_SINGLE_ROW",
  css.includes('adp-bpp-kpi-row.aiv-kpi-row') &&
    css.includes("repeat(6, minmax(0, 1fr))") &&
    css.includes('data-kpi-count="4"') &&
    css.includes("KPI_GRID_NO_ORPHANED_FINAL_ROW_WHEN_BALANCED_LAYOUT_AVAILABLE")
);
mark(
  "CUSTOMER_ACTION_TEXT_SPACING_INTEGRITY",
  css.includes("adp-evidence-action-btn") &&
    css.includes("adp-evidence-action__line") &&
    css.includes("PEER_TEXT_VISUAL_BALANCE") &&
    /adp-evidence-action-btn[\s\S]{0,200}gap:/.test(css)
);
mark(
  "EVIDENCE_ACTION_CELL_LAYOUT_INTEGRITY",
  css.includes("adp-evidence-action-cell") &&
    css.includes("min-width: 5.75rem")
);
mark(
  "VISUAL_BALANCE_BEFORE_DECORATION",
  css.includes("BRAND_PORTFOLIO_VISUAL_INTEGRITY") &&
    css.includes('data-adp-peer-grid="trends-kpi"')
);

const results = { stamp: new Date().toISOString(), gates, cases: [] };
const EXPECTED = {
  adp_renaissance_times_square: 6,
  adp_waterstone_boca_raton: 6,
  adp_hotel_phillips_kansas_city: 6,
  adp_cambridge_beaches_bermuda: 4,
  adp_now_now_noho: 6,
};

async function measure(page) {
  return page.evaluate(() => {
    const kpiRow = document.getElementById("adpBrandPortfolioKpis");
    const cards = [...(kpiRow?.querySelectorAll(".aiv-kpi") || [])];
    const tops = cards.map((c) => Math.round(c.getBoundingClientRect().top));
    const rowTops = [...new Set(tops)].sort((a, b) => a - b);
    const colsGuess =
      rowTops.length <= 1
        ? cards.length
        : cards.filter((c) => Math.round(c.getBoundingClientRect().top) === rowTops[0]).length;
    const trend = document.querySelector('.aiv-detail-trend-summary[data-adp-peer-grid="trends-kpi"]');
    const trendCards = [...(trend?.querySelectorAll(".aiv-detail-trend-stat") || [])];
    const tTops = trendCards.map((c) => Math.round(c.getBoundingClientRect().top));
    const tRows = [...new Set(tTops)].sort((a, b) => a - b);
    const trendCols =
      tRows.length <= 1
        ? trendCards.length
        : trendCards.filter((c) => Math.round(c.getBoundingClientRect().top) === tRows[0]).length;

    const pairs = [];
    for (const row of document.querySelectorAll("#adpIntentTableContainer tbody tr, #adpProviderTableContainer tbody tr")) {
      const miss = row.querySelector('[data-adp-evidence-type="missing"].adp-evidence-action-btn');
      const pos = row.querySelector('[data-adp-evidence-type="present"].adp-evidence-action-btn');
      if (!miss || !pos) continue;
      const m = miss.getBoundingClientRect();
      const p = pos.getBoundingClientRect();
      const gap = p.left - m.right;
      const mText = (miss.innerText || "").replace(/\s+/g, "");
      const pText = (pos.innerText || "").replace(/\s+/g, "");
      pairs.push({
        gap,
        mLines: miss.querySelectorAll(".adp-evidence-action__line").length,
        pLines: pos.querySelectorAll(".adp-evidence-action__line").length,
        mText,
        pText,
        concatenated: mText === "ViewMissing" && pText === "ViewExamples",
        displayBlock: getComputedStyle(miss.querySelector(".adp-evidence-action__line") || miss).display,
      });
    }
    return {
      kpiCount: cards.length,
      dataCount: kpiRow?.getAttribute("data-kpi-count"),
      colsGuess,
      rowCount: rowTops.length,
      trendCols,
      trendCount: trendCards.length,
      pairs,
      cssHasSix: getComputedStyle(kpiRow || document.body).gridTemplateColumns.split(" ").filter(Boolean).length,
    };
  });
}

const browser = await chromium.launch({ headless: true });
try {
  // Owner-app desktop BPP + evidence
  for (const [propertyId, expectKpis] of Object.entries(EXPECTED)) {
    const page = await browser.newPage({ viewport: { width: 1440, height: 1100 } });
    await page.goto(`${BASE}/owner-ai-demand.html?propertyId=${encodeURIComponent(propertyId)}&v=visual-restore`, {
      waitUntil: "domcontentloaded",
      timeout: 60000,
    });
    await page.waitForSelector("#adpBrandPortfolioSection[data-bpp-ready='1'], #adpBrandPortfolioSection[data-bpp-ready='0']", {
      timeout: 90000,
    });
    await page.waitForTimeout(800);
    const m = await measure(page);
    const shot = join(OUT, `owner_${propertyId}_1440.png`);
    await page.locator("#adpBrandPortfolioSection").screenshot({ path: shot }).catch(() => {});
    const bppOk =
      Number(m.dataCount) === expectKpis &&
      m.kpiCount === expectKpis &&
      m.colsGuess === expectKpis &&
      m.rowCount === 1;
    const evidenceOk =
      m.pairs.length === 0 ||
      m.pairs.every(
        (p) =>
          p.mLines === 2 &&
          p.pLines === 2 &&
          p.gap >= 8 &&
          p.displayBlock === "block" &&
          p.mText === "ViewMissing" &&
          p.pText === "ViewExamples"
      );
    const trendsOk = !m.trendCount || (m.trendCount === 4 && m.trendCols === 4);
    results.cases.push({
      surface: "owner",
      propertyId,
      bppOk,
      evidenceOk,
      trendsOk,
      m,
      shot,
      pass: bppOk && evidenceOk && trendsOk,
    });
    await page.close();
  }

  // External share parity (Waterstone)
  const issued = issueShareCapability({
    propertyId: "adp_waterstone_boca_raton",
    label: "visual-parity",
  });
  {
    const page = await browser.newPage({ viewport: { width: 1440, height: 1100 } });
    await page.goto(
      `${BASE}/owner-ai-demand-share.html?share=${encodeURIComponent(issued.token)}&v=visual-restore`,
      { waitUntil: "domcontentloaded", timeout: 60000 }
    );
    await page.waitForSelector("#adpBrandPortfolioSection[data-bpp-ready='1']", { timeout: 90000 });
    await page.waitForTimeout(600);
    const m = await measure(page);
    await page.locator("#adpBrandPortfolioSection").screenshot({ path: join(OUT, "share_waterstone_1440.png") }).catch(() => {});
    const pass = m.colsGuess === 6 && m.rowCount === 1;
    results.cases.push({ surface: "share", propertyId: "adp_waterstone_boca_raton", pass, m });
    await page.close();
  }
  revokeShareCapability(issued.tokenId);

  // Breakpoints Waterstone
  for (const width of [1600, 1280, 1024, 768, 390]) {
    const page = await browser.newPage({ viewport: { width, height: 1100 } });
    await page.goto(
      `${BASE}/owner-ai-demand.html?propertyId=adp_waterstone_boca_raton&v=bp-${width}`,
      { waitUntil: "domcontentloaded", timeout: 60000 }
    );
    await page.waitForSelector("#adpBrandPortfolioSection[data-bpp-ready='1']", { timeout: 90000 });
    await page.waitForTimeout(400);
    const m = await measure(page);
    let expectCols = 6;
    if (width < 600) expectCols = 1;
    else if (width < 1024) expectCols = 2;
    else if (width < 1280) expectCols = 3;
    const pass = m.colsGuess === expectCols;
    results.cases.push({ surface: "breakpoint", width, expectCols, colsGuess: m.colsGuess, pass });
    await page.close();
  }
} finally {
  await browser.close();
}

const ownerPass = results.cases.filter((c) => c.surface === "owner").every((c) => c.pass);
const sharePass = results.cases.filter((c) => c.surface === "share").every((c) => c.pass);
const bpPass = results.cases.filter((c) => c.surface === "breakpoint").every((c) => c.pass);

mark("OWNER_SHARE_VISUAL_PARITY", ownerPass && sharePass);
mark("PEER_GRID_ORPHAN", results.cases.filter((c) => c.surface === "owner").every((c) => c.m?.rowCount === 1));
mark("DEAD_SPACE_LAYOUT_DEFECT", bpPass);
mark("KPI_GRID_NO_ORPHANED_FINAL_ROW_WHEN_BALANCED_LAYOUT_AVAILABLE", ownerPass);

results.pass = Object.values(gates).every((v) => v === "PASS") && ownerPass && sharePass && bpPass;
writeFileSync(join(OUT, "visual-regression-recovery-v1.json"), JSON.stringify(results, null, 2));
console.log(
  JSON.stringify(
    {
      ok: results.pass,
      gates,
      failCount: results.cases.filter((c) => c.pass === false).length,
      fails: results.cases.filter((c) => c.pass === false),
    },
    null,
    2
  )
);
if (!results.pass) process.exit(1);
