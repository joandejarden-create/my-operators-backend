#!/usr/bin/env node
/**
 * Gate 1 — ten-tab visual capture (Puppeteer + system Chrome).
 * Identical frozen Curio payload; no content mutation.
 * Does NOT set FOUNDER_APPROVED.
 */
import { mkdirSync, writeFileSync, readFileSync } from "fs";
import { createHash } from "crypto";
import { join } from "path";
import { createRequire } from "module";

const require = createRequire(import.meta.url);
const root = process.cwd();
const outDir = join(root, "reports/brand-explorer/phase2h-a-renderer-parity/ten-tab-visual-v1");
const shotDir = join(outDir, "screenshots");
mkdirSync(shotDir, { recursive: true });

const PAYLOAD = "public/brand-explorer-phase2e-preview/curio-gate1-renderer-parity-payload.json";

const TABS = [
  { id: "atelier-overview", productLabel: "Overview", compareLabel: "Overview" },
  { id: "atelier-value-owners", productLabel: "Value to Owners", compareLabel: "Value" },
  { id: "atelier-ops", productLabel: "Operating Model", compareLabel: "Ops" },
  { id: "atelier-standards-owner", productLabel: "Owner Considerations", compareLabel: "Standards" },
  { id: "atelier-commercial", productLabel: "Commercial Engine", compareLabel: "Commercial" },
  { id: "atelier-economics", productLabel: "Economics & Obligations", compareLabel: "Economics" },
  { id: "atelier-loyalty", productLabel: "Loyalty Program", compareLabel: "Loyalty" },
  { id: "atelier-footprint", productLabel: "Footprint & Growth", compareLabel: "Footprint" },
  { id: "atelier-materials", productLabel: "Brand Materials", compareLabel: "Materials" },
  { id: "atelier-insight", productLabel: "Dealality Insight", compareLabel: "Insight" },
];

const URLS = {
  product:
    "http://127.0.0.1:8082/brand-explorer-renderer-parity.html?beInternalPreview=1&payload=curio-gate1-renderer-parity-payload.json",
  compare:
    "http://127.0.0.1:8082/brand-explorer-compare.html?beInternalPreview=1&rendererParity=1&brand=receQkxgjlezsc1xg",
  compareEqual:
    "http://127.0.0.1:8082/brand-explorer-compare.html?beInternalPreview=1&rendererParity=1&equalWidth=1&brand=receQkxgjlezsc1xg",
};

function sha256File(p) {
  return createHash("sha256").update(readFileSync(p)).digest("hex");
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function activateProductTab(page, tabId) {
  return page.evaluate((id) => {
    const btn = document.querySelector(`.section-nav-item[data-atelier-tab="${id}"]`);
    if (!btn) return false;
    btn.click();
    return btn.classList.contains("active");
  }, tabId);
}

async function activateCompareTab(page, tabId) {
  return page.evaluate((id) => {
    const btn = document.querySelector(`[data-becs-tab="${id}"]`);
    if (!btn) return false;
    btn.click();
    return true;
  }, tabId);
}

async function measureProduct(page) {
  return page.evaluate(() => {
    const panel = document.querySelector(".be-atelier-tab-panel.active");
    const root = panel || document.querySelector("#beAtelierExplorer") || document.body;
    const titles = [...root.querySelectorAll("h2.oe-section-title, .oe-section-title")].map((el) =>
      el.textContent.trim(),
    );
    const firstTitle = root.querySelector("h2.oe-section-title, .oe-section-title");
    const cs = firstTitle ? getComputedStyle(firstTitle) : null;
    const timelines = [...root.querySelectorAll(".timeline")].map((t) =>
      [...t.querySelectorAll(".timeline__phase strong")].map((s) => s.textContent.trim()),
    );
    return {
      activePanel: panel ? panel.getAttribute("data-atelier-panel") : null,
      sectionTitles: titles,
      sectionCount: titles.length,
      titleStyle: cs
        ? { fontFamily: cs.fontFamily, fontSize: cs.fontSize, fontWeight: cs.fontWeight, color: cs.color }
        : null,
      timelines,
      tableCount: root.querySelectorAll("table").length,
      imageCount: [...root.querySelectorAll("img")].filter((i) => i.offsetParent !== null).length,
      bulletCount: root.querySelectorAll("ul li, .bullet-list li").length,
      cardCount: root.querySelectorAll(
        ".scenario-card, .oe-card, .proof-point-card, .explorer-detail-card, .presence-intel-card",
      ).length,
    };
  });
}

async function measureCompare(page) {
  return page.evaluate(() => {
    const pairs = [...document.querySelectorAll("#becsPairedDoc .becs-pair-gate1")];
    const titles = pairs.map((p) => (p.getAttribute("data-gate1-section") || "").trim()).filter(Boolean);
    const leftMount = document.querySelector('[data-gate1-side="A-compare-left"] .becs-product-mount');
    const firstTitle = leftMount && leftMount.querySelector("h2.oe-section-title, .oe-section-title");
    const cs = firstTitle ? getComputedStyle(firstTitle) : null;
    const leftRoots = [...document.querySelectorAll('[data-gate1-side="A-compare-left"] .becs-product-mount')];
    const timelines = leftRoots.flatMap((root) =>
      [...root.querySelectorAll(".timeline")].map((t) =>
        [...t.querySelectorAll(".timeline__phase strong")].map((s) => s.textContent.trim()),
      ),
    );
    let identical = leftRoots.length > 0;
    const rightRoots = [...document.querySelectorAll('[data-gate1-side="B-compare-right"] .becs-product-mount')];
    if (leftRoots.length !== rightRoots.length) identical = false;
    for (let i = 0; i < leftRoots.length; i++) {
      if (!rightRoots[i] || leftRoots[i].innerHTML !== rightRoots[i].innerHTML) identical = false;
    }
    return {
      sectionTitles: titles,
      sectionCount: titles.length,
      titleStyle: cs
        ? { fontFamily: cs.fontFamily, fontSize: cs.fontSize, fontWeight: cs.fontWeight, color: cs.color }
        : null,
      timelines,
      tableCount: leftRoots.reduce((n, r) => n + r.querySelectorAll("table").length, 0),
      imageCount: leftRoots.reduce(
        (n, r) => n + [...r.querySelectorAll("img")].filter((i) => i.offsetParent !== null).length,
        0,
      ),
      bulletCount: leftRoots.reduce((n, r) => n + r.querySelectorAll("ul li, .bullet-list li").length, 0),
      cardCount: leftRoots.reduce(
        (n, r) =>
          n +
          r.querySelectorAll(
            ".scenario-card, .oe-card, .proof-point-card, .explorer-detail-card, .presence-intel-card",
          ).length,
        0,
      ),
      pairRowCount: pairs.length,
      leftRightHtmlEqual: identical,
      gate1Hook: window.__BE_GATE1_COMPARE__ || null,
    };
  });
}

async function captureProduct(browser) {
  const page = await browser.newPage();
  await page.setViewport({ width: 1440, height: 1100 });
  await page.goto(URLS.product, { waitUntil: "networkidle0", timeout: 90000 });
  await sleep(700);
  const results = [];
  for (const tab of TABS) {
    const ok = await activateProductTab(page, tab.id);
    await sleep(400);
    await page.evaluate(() => window.scrollTo(0, 0));
    const file = `A-product__${tab.id}.png`;
    await page.screenshot({ path: join(shotDir, file), fullPage: false });
    const metrics = await measureProduct(page);
    results.push({
      tabId: tab.id,
      tabName: tab.productLabel,
      clicked: ok,
      screenshot: `screenshots/${file}`,
      metrics,
    });
  }
  await page.close();
  return results;
}

async function captureCompare(browser, envKey, url, tabSubset) {
  const page = await browser.newPage();
  await page.setViewport({ width: 1440, height: 1100 });
  await page.goto(url, { waitUntil: "networkidle0", timeout: 90000 });
  await sleep(1000);
  const results = [];
  const list = tabSubset || TABS;
  for (const tab of list) {
    const ok = await activateCompareTab(page, tab.id);
    await sleep(900);
    await page.evaluate(() => window.scrollTo(0, 0));
    const file = `${envKey}__${tab.id}.png`;
    await page.screenshot({ path: join(shotDir, file), fullPage: false });
    const metrics = await measureCompare(page);
    results.push({
      tabId: tab.id,
      tabName: tab.compareLabel,
      clicked: ok,
      screenshot: `screenshots/${file}`,
      metrics,
    });
  }
  await page.close();
  return results;
}

async function main() {
  const puppeteer = require("puppeteer");
  const chromePath =
    process.env.PUPPETEER_EXECUTABLE_PATH ||
    "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe";
  const payloadHash = sha256File(join(root, PAYLOAD));
  const browser = await puppeteer.launch({
    headless: "new",
    executablePath: chromePath,
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
  });

  const report = {
    version: "BE_GATE1_TEN_TAB_VISUAL_V1",
    capturedAt: new Date().toISOString(),
    founderApprovalStatus: "NOT_FOUNDER_APPROVED",
    contentMutation: false,
    engine: "puppeteer+system-chrome",
    payloadPath: PAYLOAD,
    payloadHashSha256: payloadHash,
    urls: URLS,
    environments: {},
    remainingVisualDifferences: [],
    gate2: "BLOCKED",
    requirementIdsAddressed: [
      "BE-REQ-045",
      "BE-REQ-046",
      "BE-REQ-047",
      "BE-REQ-048",
      "BE-REQ-049",
      "BE-REQ-050",
      "BE-REQ-059",
      "BE-REQ-080",
    ],
  };

  try {
    report.environments.A_product = { url: URLS.product, tabs: await captureProduct(browser) };
    report.environments.B_compare = {
      url: URLS.compare,
      tabs: await captureCompare(browser, "B-compare", URLS.compare, TABS),
    };
    report.environments.B_compare_equalWidth = {
      url: URLS.compareEqual,
      tabs: await captureCompare(browser, "B-equal", URLS.compareEqual, [
        TABS[0],
        TABS[1],
        TABS[2],
        TABS[6],
        TABS[8],
      ]),
    };
  } finally {
    await browser.close();
  }

  const diffs = [];
  for (const tab of TABS) {
    const a = report.environments.A_product.tabs.find((t) => t.tabId === tab.id);
    const b = report.environments.B_compare.tabs.find((t) => t.tabId === tab.id);
    if (!a?.clicked) diffs.push({ tabId: tab.id, issue: "product_tab_click_failed" });
    if (!b?.clicked) diffs.push({ tabId: tab.id, issue: "compare_tab_click_failed" });
    if (a && b && a.clicked && b.clicked) {
      // Compare paired studio may duplicate section titles (left+right) — normalize unique
      const aSet = a.metrics.sectionTitles || [];
      const bUnique = [...new Set(b.metrics.sectionTitles || [])];
      if (aSet.join("|") !== bUnique.join("|")) {
        diffs.push({
          tabId: tab.id,
          issue: "section_title_set_differs",
          product: aSet,
          compareUnique: bUnique,
        });
      }
      if (a.metrics.titleStyle?.color !== b.metrics.titleStyle?.color) {
        diffs.push({
          tabId: tab.id,
          issue: "title_color_mismatch",
          product: a.metrics.titleStyle,
          compare: b.metrics.titleStyle,
        });
      }
      if (b.metrics.leftRightHtmlEqual === false) {
        diffs.push({ tabId: tab.id, issue: "compare_left_right_html_not_equal" });
      }
    }
  }

  diffs.push({
    issue: "documented_acceptable_differences",
    items: [
      "Comparison Studio chrome (topbar/sidebar) outside OE markup",
      "Default compare column width ≈ half viewport vs product max-width 1120px — use equalWidth=1 for matched geometry",
      "Compare tab button labels are shortened (Value/Ops/…) vs product multi-line labels — same tab IDs",
    ],
  });

  report.remainingVisualDifferences = diffs;
  report.screenshotCount = Object.values(report.environments).reduce(
    (n, env) => n + (env.tabs?.length || 0),
    0,
  );
  report.tabClickSummary = {
    productOk: report.environments.A_product.tabs.filter((t) => t.clicked).length,
    compareOk: report.environments.B_compare.tabs.filter((t) => t.clicked).length,
  };

  writeFileSync(join(outDir, "01-ten-tab-capture-report.json"), JSON.stringify(report, null, 2));
  writeFileSync(
    join(outDir, "00-FOUNDER_GATE1_STATUS.md"),
    `# Gate 1 — Ten-Tab Visual Acceptance Pack

**FOUNDER_APPROVED = false**

## Working URLs (port 8082)

| Env | URL |
|---|---|
| A Product | ${URLS.product} |
| B Compare (all 10 tabs) | ${URLS.compare} |
| B Equal-width | ${URLS.compareEqual} |

## Evidence

- Payload SHA-256: \`${payloadHash}\`
- Screenshots: \`screenshots/\` (${report.screenshotCount} files)
- Full report: \`01-ten-tab-capture-report.json\`
- Product tab clicks OK: ${report.tabClickSummary.productOk}/10
- Compare tab clicks OK: ${report.tabClickSummary.compareOk}/10

## Requirement IDs

${report.requirementIdsAddressed.join(", ")}

## Gate 2

**BLOCKED** until Gate 1 founder approval.

Inspect screenshots for each tab before approving. This pack does **not** auto-approve Gate 1.
`,
  );

  console.log(
    JSON.stringify(
      {
        ok: true,
        screenshots: report.screenshotCount,
        tabClickSummary: report.tabClickSummary,
        diffIssues: diffs.filter((d) => d.tabId).length,
        outDir,
        founderApproved: false,
      },
      null,
      2,
    ),
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
