#!/usr/bin/env node
/**
 * GDI universal presentation visual + data parity harness.
 *
 * Compares local auth vs local share using the same approved fixture DTOs
 * rendered through DealalityGdiUi (shared renderer). Production share compare
 * is optional via --production-share-url and is skipped by default (STOP BEFORE
 * PRODUCTION).
 *
 * Does not issue tokens, deploy, or write Airtable.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";
import { chromium } from "playwright";
import { certifyPresentationParity } from "../lib/group-demand-intelligence/contracts/gdi-presentation-contract-v1.js";
import { preparePublicationSnapshot } from "../lib/group-demand-intelligence/contracts/gdi-publication-contract-v1.js";

const require = createRequire(import.meta.url);
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-universal-presentation-publication-parity-v1/visual-parity",
);
const ENRICHED = path.join(
  ROOT,
  "reports/gdi-production-publication-full-content-parity-v1/enriched-snapshots",
);
const UI_PATH = path.join(
  ROOT,
  "public/js/group-demand-intelligence/dealality-gdi-ui.js",
);
const CSS_PATH = path.join(ROOT, "public/css/group-demand-intelligence.css");

const APPROVED = [
  "gdi_s4912_ba7b1cb8a3ab",
  "gdi_s4912_b956aa67a4a2",
  "gdi_s4912_b6d9b1929c05",
  "gdi_s4912_cb2d71ab1b15",
  "gdi_s4912_cc2a8da6ca6e",
  "gdi_s4912_03deda917778",
  "gdi_s4912_3cafec77a21f",
];
const AVENTA = "gdi_sc_f255eb5e9202";

const VIEWPORTS = [
  { name: "desktop", width: 1440, height: 900 },
  { name: "mobile", width: 390, height: 844 },
];

function loadFixtures() {
  return APPROVED.map((id) => {
    const p = path.join(ENRICHED, `${id}.json`);
    return JSON.parse(fs.readFileSync(p, "utf8"));
  });
}

function buildHarnessHtml(mode, opportunities) {
  const payload = JSON.stringify(opportunities);
  const css = fs.readFileSync(CSS_PATH, "utf8");
  // Load UI via file:// is awkward; inject after navigating about:blank with inline script tag from disk.
  return `<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>GDI parity harness (${mode})</title>
<style>${css}</style>
</head>
<body class="gdi-page aiv-page ${mode === "share" ? "gdi-share-mode" : ""}">
<div id="root" class="gdi-shell"></div>
<dialog class="gdi-drawer" id="drawer"><div class="gdi-drawer-inner">
<header class="gdi-drawer-head"><h2 id="drawerTitle">Opportunity</h2>
<button type="button" id="drawerClose">Close</button></header>
<div class="gdi-drawer-body" id="drawerBody"></div>
</div></dialog>
<script>
window.__GDI_PARITY_FIXTURES__ = ${payload};
window.__GDI_PARITY_MODE__ = ${JSON.stringify(mode)};
</script>
</body></html>`;
}

async function injectUiAndRender(page, mode) {
  const uiSource = fs.readFileSync(UI_PATH, "utf8");
  await page.addScriptTag({ content: uiSource });
  await page.evaluate((m) => {
    const UI = window.DealalityGdiUi;
    if (!UI) throw new Error("DealalityGdiUi missing");
    const rows = window.__GDI_PARITY_FIXTURES__ || [];
    const root = document.getElementById("root");
    const chrome = UI.opportunityBrowseChromeHtml({
      mode: m,
      showInventoryViewToggle: false,
      inventoryView: "customer",
      customerInventoryCount: rows.length,
      researchInventoryCount: rows.length + 1,
      shown: rows.length,
      total: rows.length,
      sort: "priority",
      viewMode: "tiles",
      noun: "Opportunities",
      priorityCounts: {},
      salesOpportunityCounts: {},
    });
    const cards = UI.opportunityCardsGridHtml(rows, "tiles", {});
    root.innerHTML = chrome + cards;
    window.__GDI_PARITY_OPEN__ = function (id) {
      const o = rows.find((r) => r.id === id) || rows[0];
      const drawer = document.getElementById("drawer");
      const title = document.getElementById("drawerTitle");
      const body = document.getElementById("drawerBody");
      title.textContent =
        o.displayTitle || o.drawerHeaderTitle || o.title || "Opportunity";
      body.innerHTML = UI.intelligenceDetailHtml(o, {}, {
        mode: m,
        permissionContext:
          m === "share"
            ? { canValidate: false, canRecordAction: false, canRecordOutcome: false }
            : { canValidate: true },
      });
      if (drawer.showModal) drawer.showModal();
      else drawer.setAttribute("open", "open");
    };
  }, mode);
}

async function captureSurface(browser, mode, opportunities, viewport) {
  const page = await browser.newPage({
    viewport: { width: viewport.width, height: viewport.height },
  });
  await page.setContent(buildHarnessHtml(mode, opportunities), {
    waitUntil: "domcontentloaded",
  });
  await injectUiAndRender(page, mode);
  const tileShot = path.join(OUT, `${mode}-${viewport.name}-tiles.png`);
  await page.locator("#root").screenshot({ path: tileShot });

  const hangersId = "gdi_s4912_b6d9b1929c05";
  await page.evaluate((id) => window.__GDI_PARITY_OPEN__(id), hangersId);
  await page.waitForTimeout(200);
  const drawerShot = path.join(OUT, `${mode}-${viewport.name}-drawer-hangers.png`);
  await page.locator("#drawer").screenshot({ path: drawerShot });

  const dataProbe = await page.evaluate(() => {
    const rawIds = Array.from(document.querySelectorAll("[data-open]"))
      .map((el) => el.getAttribute("data-open"))
      .filter(Boolean);
    const ids = Array.from(new Set(rawIds));
    const body = document.getElementById("drawerBody")?.innerText || "";
    return {
      tileCount: ids.length,
      ids,
      drawerHasWhat: /what is happening|happening/i.test(body),
      drawerHasPotential: /potential/i.test(body),
      drawerHasContact: /khaleen|contact/i.test(body),
      drawerTextLen: body.length,
    };
  });

  await page.close();
  return { tileShot, drawerShot, dataProbe };
}

function pixelDiffRough(aPath, bPath) {
  // Lightweight structural check without sharp dependency: file size ratio + existence.
  if (!fs.existsSync(aPath) || !fs.existsSync(bPath)) {
    return { ok: false, reason: "missing_screenshot" };
  }
  const a = fs.statSync(aPath).size;
  const b = fs.statSync(bPath).size;
  const ratio = a && b ? Math.min(a, b) / Math.max(a, b) : 0;
  return {
    ok: ratio >= 0.55,
    sizeA: a,
    sizeB: b,
    sizeRatio: Number(ratio.toFixed(3)),
    note: "Size-ratio gate only; field parity is authoritative. Full pixelmatch optional when dependency available.",
  };
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const fixtures = loadFixtures();
  const fieldParity = [];
  for (const snap of fixtures) {
    const cert = certifyPresentationParity(snap, {
      hotelId: "recKRJjcPnb4tVDDS",
    });
    const pub = preparePublicationSnapshot(snap, {
      hotelId: "recKRJjcPnb4tVDDS",
    });
    fieldParity.push({
      id: snap.id,
      parityOk: cert.ok,
      publicationOk: pub.ok,
      fieldDiffs: cert.fieldDiffs.length,
    });
  }

  const browser = await chromium.launch({ headless: true });
  const visual = [];
  for (const vp of VIEWPORTS) {
    const auth = await captureSurface(browser, "auth", fixtures, vp);
    const share = await captureSurface(browser, "share", fixtures, vp);
    visual.push({
      viewport: vp.name,
      auth,
      share,
      tileDiff: pixelDiffRough(auth.tileShot, share.tileShot),
      drawerDiff: pixelDiffRough(auth.drawerShot, share.drawerShot),
      dataParity: {
        authTileCount: auth.dataProbe.tileCount,
        shareTileCount: share.dataProbe.tileCount,
        authDrawerLen: auth.dataProbe.drawerTextLen,
        shareDrawerLen: share.dataProbe.drawerTextLen,
        bothHaveWhat: auth.dataProbe.drawerHasWhat && share.dataProbe.drawerHasWhat,
        bothHavePotential:
          auth.dataProbe.drawerHasPotential && share.dataProbe.drawerHasPotential,
        bothHaveContact:
          auth.dataProbe.drawerHasContact && share.dataProbe.drawerHasContact,
      },
    });
  }
  await browser.close();

  const productionUrl = process.argv.includes("--production-share-url")
    ? process.argv[process.argv.indexOf("--production-share-url") + 1]
    : null;

  const report = {
    ok:
      fieldParity.every((r) => r.parityOk && r.publicationOk) &&
      visual.every(
        (v) =>
          v.dataParity.bothHaveWhat &&
          v.dataParity.bothHavePotential &&
          v.dataParity.bothHaveContact &&
          v.auth.dataProbe.tileCount === 7 &&
          v.share.dataProbe.tileCount === 7,
      ),
    approvedCount: 7,
    aventaExcluded: AVENTA,
    fieldParity,
    visual,
    productionCompare: productionUrl
      ? { status: "NOT_RUN_IN_THIS_WAVE", url: productionUrl }
      : {
          status: "SKIPPED_STOP_BEFORE_PRODUCTION",
          note: "Pass --production-share-url after founder-authorized deploy",
        },
    controller: "gdi-universal-presentation-controller-v1",
    renderer: "dealality-gdi-ui.js",
  };

  fs.writeFileSync(
    path.join(
      ROOT,
      "reports/gdi-universal-presentation-publication-parity-v1/12-visual-parity-results.json",
    ),
    JSON.stringify(report, null, 2),
  );
  console.log(JSON.stringify(report, null, 2));
  process.exit(report.ok ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
