#!/usr/bin/env node
/**
 * Founder Comparison Workspace demo — Curio + Ascend on canonical URL.
 * No Gate 2 content regen. No Airtable writes.
 */
import { mkdirSync, writeFileSync } from "fs";
import { join } from "path";
import { createRequire } from "module";

const require = createRequire(import.meta.url);
const root = process.cwd();
const outDir = join(root, "reports/brand-explorer/comparison-studio/founder-workspace-demo-v1");
const shotDir = join(outDir, "screenshots");
mkdirSync(shotDir, { recursive: true });

const CANONICAL = "http://127.0.0.1:8082/brand-explorer-compare.html?beInternalPreview=1";

const BRANDS = [
  { id: "receQkxgjlezsc1xg", slug: "curio-collection", name: "Curio Collection by Hilton" },
  { id: "reclkgOzvAcBheUSo", slug: "ascend", name: "Ascend Hotel Collection" },
];

const TABS = [
  "atelier-overview",
  "atelier-value-owners",
  "atelier-ops",
  "atelier-standards-owner",
  "atelier-economics",
  "atelier-materials",
];

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function measure(page) {
  return page.evaluate(() => {
    const leftHead = document.querySelector(".becs-col-heads > div:first-child strong");
    const rightHead = document.querySelector(".becs-col-heads > div:last-child strong");
    const pairs = [...document.querySelectorAll("#becsPairedDoc .becs-pair[data-product-section]")];
    const titles = pairs.map((p) => p.getAttribute("data-product-section") || "").filter(Boolean);
    const banner = document.querySelector("#becsSourceBanner")?.innerText || "";
    const completeness = document.querySelector("#becsCompleteness")?.innerText || "";
    const status = document.querySelector("#becsStatus")?.textContent || "";
    const hook = window.__BE_COMPARE_STUDIO__ || null;
    return {
      url: location.href,
      leftLabel: leftHead?.textContent?.trim() || null,
      rightLabel: rightHead?.textContent?.trim() || null,
      sectionTitles: titles,
      sectionCount: titles.length,
      hasSection1: titles.some((t) => /^Section\s+\d+$/i.test(t)),
      productMounts: document.querySelectorAll(".becs-product-mount").length,
      bannerSnippet: banner.slice(0, 400),
      completenessSnippet: completeness.slice(0, 400),
      status,
      hook,
      rendererParityActive: !!document.querySelector('[data-becs-mode="gate1"].active'),
    };
  });
}

async function main() {
  const puppeteer = require("puppeteer");
  const chromePath =
    process.env.PUPPETEER_EXECUTABLE_PATH ||
    "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe";
  const browser = await puppeteer.launch({
    headless: "new",
    executablePath: chromePath,
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
  });

  const report = {
    version: "BE_FOUNDER_WORKSPACE_DEMO_V1",
    capturedAt: new Date().toISOString(),
    canonicalUrl: CANONICAL,
    airtableWrites: 0,
    gate2: "BLOCKED",
    founderApprovalStatus: "NOT_FOUNDER_APPROVED",
    brands: {},
  };

  try {
    for (const brand of BRANDS) {
      const page = await browser.newPage();
      await page.setViewport({ width: 1440, height: 1100 });
      const url = `${CANONICAL}&brand=${encodeURIComponent(brand.id)}`;
      await page.goto(url, { waitUntil: "networkidle0", timeout: 120000 });
      await sleep(1800);
      const brandResult = { url, tabs: [], checks: {} };
      const boot = await measure(page);
      brandResult.checks.labels = {
        pass:
          /CURRENT/i.test(boot.leftLabel || "") &&
          /PROPOSED|STAGED/i.test(boot.rightLabel || "") &&
          !boot.rendererParityActive,
        leftLabel: boot.leftLabel,
        rightLabel: boot.rightLabel,
      };
      brandResult.boot = boot;

      for (const tabId of TABS) {
        await page.evaluate((id) => {
          const btn = document.querySelector(`[data-becs-tab="${id}"]`);
          if (btn) btn.click();
        }, tabId);
        await sleep(900);
        await page.evaluate(() => {
          const sc = document.querySelector("#becsDocScroll");
          if (sc) sc.scrollTop = 0;
        });
        const file = `${brand.slug}__${tabId}.png`;
        await page.screenshot({ path: join(shotDir, file), fullPage: false });
        const metrics = await measure(page);
        brandResult.tabs.push({
          tabId,
          screenshot: `screenshots/${file}`,
          metrics,
        });
      }

      const standards = brandResult.tabs.find((t) => t.tabId === "atelier-standards-owner");
      const economics = brandResult.tabs.find((t) => t.tabId === "atelier-economics");
      brandResult.checks.standardsFour = {
        pass: !!standards && standards.metrics.sectionCount === 4,
        observed: standards?.metrics?.sectionTitles || [],
      };
      brandResult.checks.economicsNoSection1 = {
        pass: !!economics && economics.metrics.hasSection1 === false,
        observed: economics?.metrics?.sectionTitles || [],
      };
      brandResult.checks.productRenderer = {
        pass: brandResult.tabs.every((t) => t.metrics.productMounts > 0),
      };
      report.brands[brand.slug] = brandResult;
      await page.close();
    }
  } finally {
    await browser.close();
  }

  const allPass = Object.values(report.brands).every((b) =>
    Object.values(b.checks).every((c) => c.pass === true),
  );
  report.readyForFounderReview = allPass;

  writeFileSync(join(outDir, "01-demo-report.json"), JSON.stringify(report, null, 2));
  writeFileSync(
    join(outDir, "00-FOUNDER_WORKSPACE_STATUS.md"),
    `# Brand Explorer — Unified Founder Comparison Workspace

**Canonical URL:** \`${CANONICAL}\`

**Airtable writes:** 0  
**Publication applied:** false  
**Gate 2 content regen:** BLOCKED until Gate 1 approval  
**FOUNDER_APPROVED:** false

## What the founder sees

| Side | Label | Source |
|---|---|---|
| LEFT | CURRENT · SAVED PROFILE | Canonical live Presentation (read-only) |
| RIGHT | PROPOSED · STAGED CANDIDATE | Latest phase2h staged overlay (no silent 2G fallback) |

Same production atelier renderer both sides. Renderer Parity remains a selectable diagnostic only.

## Demo brands

- Curio Collection by Hilton
- Ascend Hotel Collection

## Checks

${Object.entries(report.brands)
  .map(([slug, b]) => {
    const lines = Object.entries(b.checks)
      .map(([k, v]) => `- ${slug} · ${k}: ${v.pass}`)
      .join("\n");
    return lines;
  })
  .join("\n")}

**readyForFounderReview = ${allPass}**

Screenshots: \`screenshots/\`
`,
  );

  console.log(
    JSON.stringify(
      {
        ok: allPass,
        canonicalUrl: CANONICAL,
        readyForFounderReview: allPass,
        brands: Object.fromEntries(
          Object.entries(report.brands).map(([k, v]) => [
            k,
            Object.fromEntries(Object.entries(v.checks).map(([ck, cv]) => [ck, cv.pass])),
          ]),
        ),
        outDir,
      },
      null,
      2,
    ),
  );
  if (!allPass) process.exit(2);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
