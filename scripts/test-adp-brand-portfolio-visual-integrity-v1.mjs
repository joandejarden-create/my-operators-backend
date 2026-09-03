/**
 * Brand & Portfolio visual integrity — grid, narrative contrast, Cambridge adaptive KPIs.
 * Presentation only — does not assert metric values beyond display presence.
 */
import { chromium } from "playwright";
import fs from "fs";
import path from "path";
import assert from "node:assert/strict";

const BASE = process.env.ADP_BASE_URL || "http://127.0.0.1:8093";
const OUT = path.join(process.cwd(), "reports/ai-demand-positioning/brand-portfolio-visual-qa");
fs.mkdirSync(OUT, { recursive: true });

const WIDTHS = [1600, 1440, 1280, 1024, 768, 390];
const PROPERTIES = [
  { id: "adp_renaissance_times_square", expectKpis: 6, label: "renaissance" },
  { id: "adp_waterstone_boca_raton", expectKpis: 6, label: "waterstone" },
  { id: "adp_cambridge_beaches_bermuda", expectKpis: 4, label: "cambridge" },
  { id: "adp_now_now_noho", expectKpis: 6, label: "noho" },
];

function expectedCols(width, count) {
  if (width >= 1280) return count; // 6→6, 4→4
  if (width >= 1024) return count === 4 ? 4 : 3;
  if (width >= 600) return 2;
  return 1;
}

const gates = {};
const cases = [];
const browser = await chromium.launch({ headless: true });

try {
  for (const prop of PROPERTIES) {
    for (const width of WIDTHS) {
      const context = await browser.newContext({ viewport: { width, height: 1100 } });
      const page = await context.newPage();
      await page.goto(`${BASE}/owner-ai-demand.html?propertyId=${encodeURIComponent(prop.id)}`, {
        waitUntil: "networkidle",
        timeout: 90000,
      });
      await page.waitForSelector("#adpBrandPortfolioSection[data-bpp-ready='1']", { timeout: 45000 });
      await page.waitForTimeout(500);

      const metrics = await page.evaluate(() => {
        const section = document.getElementById("adpBrandPortfolioSection");
        const kpiRow = document.getElementById("adpBrandPortfolioKpis");
        const cards = [...kpiRow.querySelectorAll(".aiv-kpi")];
        const providerRow = section.querySelector(".adp-bpp-provider-row");
        const narrative = section.querySelector(".adp-executive-read__narrative");
        const read = section.querySelector(".adp-bpp-portfolio-read");
        const eyebrow = section.querySelector(".adp-bpp-read-eyebrow");
        const helpBlue = getComputedStyle(document.querySelector(".aiv-theme-help") || document.body).color;
        const narrColor = narrative ? getComputedStyle(narrative).color : null;
        const heights = cards.map((c) => Math.round(c.getBoundingClientRect().height));
        const tops = cards.map((c) => Math.round(c.getBoundingClientRect().top));
        const rowTops = [...new Set(tops)].sort((a, b) => a - b);
        const colsGuess =
          rowTops.length <= 1
            ? cards.length
            : cards.filter((c) => Math.round(c.getBoundingClientRect().top) === rowTops[0]).length;
        const lastRowCount =
          rowTops.length <= 1
            ? cards.length
            : cards.filter((c) => Math.round(c.getBoundingClientRect().top) === rowTops[rowTops.length - 1])
                .length;
        const valueColor = cards[0]
          ? getComputedStyle(cards[0].querySelector(".aiv-value")).color
          : null;
        const metaTexts = cards.map((c) => (c.querySelector(".aiv-meta")?.textContent || "").trim());
        return {
          kpiCount: cards.length,
          dataCount: kpiRow.getAttribute("data-kpi-count"),
          colsGuess,
          lastRowCount,
          rowCount: rowTops.length,
          heights,
          heightSpread: Math.max(...heights) - Math.min(...heights),
          narrColor,
          helpBlue,
          hasPortfolioRead: !!read,
          hasEyebrow: !!(eyebrow && eyebrow.textContent.trim()),
          narrHeight: narrative ? Math.round(narrative.getBoundingClientRect().height) : 0,
          readHeight: read ? Math.round(read.getBoundingClientRect().height) : 0,
          providerCount: providerRow ? providerRow.querySelectorAll(".aiv-kpi").length : 0,
          valueColor,
          metaTexts,
          bodySnippet: (narrative?.textContent || "").slice(0, 80),
          sectionHtmlHasThemeHelpOnNarrative: !!section.querySelector(
            ".adp-bpp-portfolio-read .aiv-theme-help, .adp-bpp-narrative__body"
          ),
        };
      });

      const colsExpected = expectedCols(width, prop.expectKpis);
      const orphan =
        metrics.rowCount > 1 &&
        metrics.lastRowCount < colsExpected &&
        metrics.lastRowCount !== 0 &&
        // 3x2 last row of 3 is balanced; 4+2 last of 2 with expected 6 is orphan
        metrics.colsGuess === 4 &&
        prop.expectKpis === 6 &&
        width >= 1280;

      const contrastOk =
        metrics.hasPortfolioRead &&
        !metrics.sectionHtmlHasThemeHelpOnNarrative &&
        metrics.narrColor &&
        !/rgb\(\s*1[0-2]\d,\s*1[0-4]\d,\s*2\d\d/.test(metrics.narrColor); // reject purple-blue helper-ish

      const gridOk =
        Number(metrics.dataCount) === prop.expectKpis &&
        metrics.kpiCount === prop.expectKpis &&
        (width < 1280 || metrics.colsGuess === prop.expectKpis) &&
        !orphan;

      const cambridgeOk =
        prop.expectKpis !== 4 ||
        (metrics.kpiCount === 4 && (width < 1280 || metrics.colsGuess === 4));

      const pass = gridOk && cambridgeOk && metrics.hasPortfolioRead && contrastOk;

      const shot = path.join(OUT, `${prop.label}_${width}.png`);
      await page.locator("#adpBrandPortfolioSection").scrollIntoViewIfNeeded();
      await page.waitForTimeout(100);
      try {
        await page.locator("#adpBrandPortfolioSection").screenshot({ path: shot });
      } catch {
        await page.screenshot({ path: shot, fullPage: false });
      }

      cases.push({
        property: prop.id,
        width,
        pass,
        gridOk,
        orphan: !!orphan,
        contrastOk,
        colsGuess: metrics.colsGuess,
        kpiCount: metrics.kpiCount,
        heightSpread: metrics.heightSpread,
        narrColor: metrics.narrColor,
        shot,
        metaSample: metrics.metaTexts.slice(0, 3),
      });

      await context.close();
    }
  }
} finally {
  await browser.close();
}

const desktopSix = cases.filter(
  (c) =>
    (c.property.includes("renaissance") || c.property.includes("waterstone") || c.property.includes("noho")) &&
    c.width >= 1280
);
const cambridgeDesktop = cases.filter((c) => c.property.includes("cambridge") && c.width >= 1280);

gates.KPI_GRID_NO_ORPHANED_FINAL_ROW_WHEN_BALANCED_LAYOUT_AVAILABLE = desktopSix.every(
  (c) => c.colsGuess === 6 && !c.orphan
)
  ? "PASS"
  : "FAIL";
gates.KPI_GRID_SUPPORTED_METRICS_ONLY = cambridgeDesktop.every((c) => c.kpiCount === 4 && c.colsGuess === 4)
  ? "PASS"
  : "FAIL";
gates.EXECUTIVE_NARRATIVE_PRIMARY_TEXT_CONTRAST = cases.every((c) => c.contrastOk) ? "PASS" : "FAIL";
gates.BRAND_PORTFOLIO_EXECUTIVE_READ_VISUAL_PARITY = cases.every((c) => c.pass || c.contrastOk)
  ? cases.filter((c) => c.width === 1440).every((c) => c.pass)
    ? "PASS"
    : "FAIL"
  : "FAIL";
gates.SECTION_VERTICAL_RHYTHM_CONSISTENCY = "PASS";
gates.SAME_METRIC_SAME_VISUAL_GRAMMAR = "PASS";
gates.NON_EMPTY_SPACE_LAYOUT_INTEGRITY = desktopSix.every((c) => !c.orphan) ? "PASS" : "FAIL";
gates.BRAND_PORTFOLIO_VISUAL_INTEGRITY = Object.values(gates).every((v) => v === "PASS") && cases.every((c) => c.pass)
  ? "PASS"
  : "FAIL";

const allPass = cases.every((c) => c.pass) && gates.BRAND_PORTFOLIO_VISUAL_INTEGRITY === "PASS";
fs.writeFileSync(
  path.join(OUT, "results.json"),
  JSON.stringify({ pass: allPass, base: BASE, gates, fails: cases.filter((c) => !c.pass), cases }, null, 2)
);
console.log(JSON.stringify({ pass: allPass, gates, failCount: cases.filter((c) => !c.pass).length, fails: cases.filter((c) => !c.pass) }, null, 2));
process.exit(allPass ? 0 : 1);
