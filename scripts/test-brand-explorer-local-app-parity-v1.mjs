#!/usr/bin/env node
/**
 * Brand Explorer — local application parity regression (BE preview vs canonical app).
 *
 * Compares the same brand + same staged OR same saved payload across:
 *   A) BE worktree preview (default :8082)
 *   B) Canonical local Dealality app (default :8080)
 *
 * Fail when tabs/components/styles/assets/data-sources diverge for identical content.
 * Do NOT treat unpublished Airtable candidate ≠ saved Presentation as a renderer failure
 * unless --require-live-equals-staged is set.
 *
 * Usage:
 *   node scripts/test-brand-explorer-local-app-parity-v1.mjs
 *   node scripts/test-brand-explorer-local-app-parity-v1.mjs --app-port 8080 --preview-port 8082
 *   node scripts/test-brand-explorer-local-app-parity-v1.mjs --screenshots
 */
import { createHash } from "node:crypto";
import { createRequire } from "node:module";
import { mkdirSync, writeFileSync, readFileSync, existsSync } from "node:fs";
import { join } from "node:path";
import { spawnSync } from "node:child_process";

const require = createRequire(import.meta.url);
const root = process.cwd();

function arg(flag, fallback = "") {
  const i = process.argv.indexOf(flag);
  return i >= 0 ? String(process.argv[i + 1] || "") : fallback;
}
function has(flag) {
  return process.argv.includes(flag);
}

const APP_PORT = Number(arg("--app-port", process.env.BE_APP_PORT || "8080"));
const PREVIEW_PORT = Number(arg("--preview-port", process.env.BE_PREVIEW_PORT || "8082"));
const BRAND_ID = arg("--brand", "receQkxgjlezsc1xg");
const PAYLOAD = arg("--payload", "curio-gate1-renderer-parity-payload.json");
const VIEWPORT = { width: Number(arg("--width", "1440")), height: Number(arg("--height", "900")) };
const WANT_SHOTS = has("--screenshots");
const REQUIRE_LIVE_EQ_STAGED = has("--require-live-equals-staged");

const outDir = join(root, "reports/brand-explorer/local-app-parity-v1");
const shotDir = join(outDir, "screenshots");
mkdirSync(shotDir, { recursive: true });

const TABS = [
  { id: "atelier-overview", label: "Overview" },
  { id: "atelier-value-owners", label: "Value to Owners" },
  { id: "atelier-ops", label: "Operating Model" },
  { id: "atelier-standards-owner", label: "Owner Considerations" },
  { id: "atelier-commercial", label: "Commercial Engine" },
  { id: "atelier-economics", label: "Economics & Obligations" },
  { id: "atelier-loyalty", label: "Loyalty Program" },
  { id: "atelier-footprint", label: "Footprint & Growth" },
  { id: "atelier-materials", label: "Brand Materials" },
  { id: "atelier-insight", label: "Dealality Insight" },
];

const ASSETS = [
  "/js/brand-explorer-atelier-from-api.js",
  "/js/brand-explorer-compare-studio.js",
  "/css/brand-explorer-atelier-oe-panels.css",
  "/css/brand-explorer-gold-surface.css",
  "/brand-explorer-combined.html",
  "/brand-explorer-compare.html",
  "/brand-explorer-renderer-parity.html",
];

function sha256(buf) {
  return createHash("sha256").update(buf).digest("hex");
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function gitHead(cwd) {
  const r = spawnSync("git", ["rev-parse", "HEAD"], { cwd, encoding: "utf8" });
  return r.status === 0 ? String(r.stdout || "").trim() : null;
}

async function fetchText(url) {
  const r = await fetch(url);
  const text = await r.text();
  return { ok: r.ok, status: r.status, text, sha: sha256(text) };
}

async function fetchJson(url) {
  const r = await fetch(url);
  const json = await r.json().catch(() => null);
  return { ok: r.ok, status: r.status, json };
}

function blocksHash(blocks) {
  const norm = (blocks || [])
    .map((b) => ({
      slotKey: String(b?.slotKey || "").trim(),
      title: String(b?.title || "").trim(),
      body: String(b?.body || "").trim(),
      imageUrl: b?.imageUrl || null,
      sort: typeof b?.sort === "number" ? b.sort : null,
    }))
    .sort((a, b) => String(a.slotKey).localeCompare(String(b.slotKey)));
  return sha256(JSON.stringify(norm));
}

function footprintSummary(brand) {
  const fp = brand?.footprint || {};
  return {
    open: fp.open ?? fp.openCount ?? fp.Open ?? null,
    pipeline: fp.pipeline ?? fp.pipelineCount ?? fp.Pipeline ?? null,
    total: fp.total ?? fp.totalCount ?? null,
    byRegionKeys: Object.keys(fp.byRegion || fp.region || {}).length,
    byCountryKeys: Object.keys(fp.byCountry || fp.country || {}).length,
    byArchetypeKeys: Object.keys(fp.byArchetype || fp.assetArchetype || fp.byAssetArchetype || {}).length,
    zeroedUnavailable: false,
  };
}

async function measureProductPage(page) {
  return page.evaluate(() => {
    const allNav = [...document.querySelectorAll(".section-nav-item[data-atelier-tab]")].map((b) => ({
      id: b.getAttribute("data-atelier-tab"),
      label: (b.textContent || "").replace(/\s+/g, " ").trim(),
      active: b.classList.contains("active"),
      visible: !!(b.offsetParent || (b.getClientRects && b.getClientRects().length)),
    }));
    // Combined shell may render desktop + mobile nav (same 10 ids twice). Count unique ids.
    const seen = new Set();
    const nav = [];
    for (const n of allNav) {
      if (!n.id || seen.has(n.id)) continue;
      seen.add(n.id);
      nav.push(n);
    }
    const panel = document.querySelector(".be-atelier-tab-panel.active");
    const root = panel || document.querySelector("#beAtelierExplorer") || document.body;
    const titles = [...root.querySelectorAll("h2.oe-section-title, .oe-section-title")].map((el) =>
      (el.textContent || "").trim(),
    );
    const firstTitle = root.querySelector("h2.oe-section-title, .oe-section-title");
    const cs = firstTitle ? getComputedStyle(firstTitle) : null;
    const ampersandTitles = titles.filter((t) => t.includes("&"));
    const andTitles = titles.filter((t) => /\bAnd\b/.test(t));
    const repeatedInBody = titles.filter((t) => {
      if (!t) return false;
      const body = root.innerText || "";
      return body.split(t).length > 3;
    });
    return {
      tabNodeCount: allNav.length,
      tabCount: nav.length,
      tabIds: nav.map((n) => n.id),
      tabLabels: nav.map((n) => n.label),
      activePanel: panel ? panel.getAttribute("data-atelier-panel") : null,
      sectionTitles: titles,
      sectionCount: titles.length,
      titleStyle: cs
        ? {
            fontFamily: cs.fontFamily,
            fontSize: cs.fontSize,
            fontWeight: cs.fontWeight,
            color: cs.color,
          }
        : null,
      ampersandTitleCount: ampersandTitles.length,
      andTitleCount: andTitles.length,
      tableCount: root.querySelectorAll("table").length,
      imageCount: [...root.querySelectorAll("img")].filter((i) => i.offsetParent !== null).length,
      bulletCount: root.querySelectorAll("ul li, .bullet-list li").length,
      cardCount: root.querySelectorAll(
        ".scenario-card, .oe-card, .proof-point-card, .explorer-detail-card, .presence-intel-card, .kpi-card, .brand-markets-kpi",
      ).length,
      overflowSuspects: [...root.querySelectorAll("*")]
        .filter((el) => {
          const s = getComputedStyle(el);
          return s.overflow === "hidden" && el.scrollWidth > el.clientWidth + 2;
        })
        .slice(0, 5)
        .map((el) => el.className || el.tagName),
      titleEchoSuspectCount: repeatedInBody.length,
    };
  });
}

async function activateTab(page, tabId) {
  return page.evaluate((id) => {
    const btn = document.querySelector(`.section-nav-item[data-atelier-tab="${id}"]`);
    if (!btn) return false;
    btn.click();
    return true;
  }, tabId);
}

async function main() {
  const failures = [];
  const notes = [];
  const report = {
    contract: "BE_LOCAL_APP_PARITY_V1",
    generatedAt: new Date().toISOString(),
    brandId: BRAND_ID,
    viewport: VIEWPORT,
    ports: { app: APP_PORT, preview: PREVIEW_PORT },
    contentClasses: {
      stagedPayload: `public/brand-explorer-phase2e-preview/${PAYLOAD}`,
      savedAirtable: `GET /api/brand-library/brand?brandId=${BRAND_ID}`,
      rule: "Renderer parity requires identical payload. Saved≠staged is a publication gap, not a renderer fail.",
    },
    runtime: {},
    assets: {},
    api: {},
    tabs: {},
    screenshots: [],
    failures: [],
    notes: [],
    verdict: "PENDING",
  };

  // --- Runtime / commit ownership (caller supplies via env or we probe via asset equality) ---
  const beHead = gitHead(root);
  report.runtime.beWorkspaceHead = beHead;
  report.runtime.originMainHint = gitHead(root); // same repo tip when on home

  // Asset byte parity (proves same frontend load)
  for (const path of ASSETS) {
    const a = await fetchText(`http://127.0.0.1:${APP_PORT}${path}`);
    const b = await fetchText(`http://127.0.0.1:${PREVIEW_PORT}${path}`);
    const row = {
      path,
      appStatus: a.status,
      previewStatus: b.status,
      appSha: a.sha,
      previewSha: b.sha,
      match: a.ok && b.ok && a.sha === b.sha,
    };
    report.assets[path] = row;
    if (!row.match) failures.push(`ASSET_MISMATCH ${path}`);
  }

  // Live Airtable saved content (both servers)
  const apiPath = `/api/brand-library/brand?brandId=${encodeURIComponent(BRAND_ID)}&beInternalPreview=1`;
  const apiApp = await fetchJson(`http://127.0.0.1:${APP_PORT}${apiPath}`);
  const apiPrev = await fetchJson(`http://127.0.0.1:${PREVIEW_PORT}${apiPath}`);
  const brandApp = apiApp.json?.brand || null;
  const brandPrev = apiPrev.json?.brand || null;
  const blocksApp = brandApp?.brandExplorer?.blocks || [];
  const blocksPrev = brandPrev?.brandExplorer?.blocks || [];
  const liveHashApp = blocksHash(blocksApp);
  const liveHashPrev = blocksHash(blocksPrev);
  report.api.savedPresentation = {
    appHttp: apiApp.status,
    previewHttp: apiPrev.status,
    appBlockCount: blocksApp.length,
    previewBlockCount: blocksPrev.length,
    appHash: liveHashApp,
    previewHash: liveHashPrev,
    hashesMatch: liveHashApp === liveHashPrev,
    footprintApp: footprintSummary(brandApp),
    footprintPreview: footprintSummary(brandPrev),
  };
  if (!apiApp.ok || !apiPrev.ok) failures.push("API_BRAND_LIBRARY_HTTP_FAIL");
  if (liveHashApp !== liveHashPrev) failures.push("LIVE_SAVED_CONTENT_HASH_MISMATCH_BETWEEN_PORTS");

  // Staged Gate 1 payload (identical content for renderer compare)
  const stagedPath = join(root, "public/brand-explorer-phase2e-preview", PAYLOAD);
  if (!existsSync(stagedPath)) {
    failures.push(`MISSING_STAGED_PAYLOAD ${stagedPath}`);
  } else {
    const staged = JSON.parse(readFileSync(stagedPath, "utf8"));
    const stagedBlocks = staged.brandExplorer?.blocks || staged.brand?.brandExplorer?.blocks || [];
    const stagedHash = blocksHash(stagedBlocks);
    report.api.stagedPayload = {
      path: stagedPath,
      blockCount: stagedBlocks.length,
      hash: stagedHash,
      vsLiveAppEqual: stagedHash === liveHashApp,
      vsLivePreviewEqual: stagedHash === liveHashPrev,
    };
    if (stagedHash !== liveHashApp) {
      notes.push(
        "STAGED_NE_SAVED: Gate1 frozen payload differs from live Airtable Presentation — expected until authorized writeback. Not a renderer failure.",
      );
      if (REQUIRE_LIVE_EQ_STAGED) failures.push("REQUIRE_LIVE_EQUALS_STAGED_FAILED");
    }
  }

  // Puppeteer DOM parity on identical staged payload
  const puppeteer = require("puppeteer");
  const executablePath =
    process.env.PUPPETEER_EXECUTABLE_PATH ||
    "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe";
  const browser = await puppeteer.launch({
    headless: "new",
    executablePath: existsSync(executablePath) ? executablePath : undefined,
    args: ["--no-sandbox", "--disable-dev-shm-usage", `--window-size=${VIEWPORT.width},${VIEWPORT.height}`],
  });

  try {
    const urlApp = `http://127.0.0.1:${APP_PORT}/brand-explorer-renderer-parity.html?beInternalPreview=1&payload=${encodeURIComponent(PAYLOAD)}`;
    const urlPrev = `http://127.0.0.1:${PREVIEW_PORT}/brand-explorer-renderer-parity.html?beInternalPreview=1&payload=${encodeURIComponent(PAYLOAD)}`;

    const pageApp = await browser.newPage();
    const pagePrev = await browser.newPage();
    await pageApp.setViewport(VIEWPORT);
    await pagePrev.setViewport(VIEWPORT);
    await pageApp.goto(urlApp, { waitUntil: "networkidle2", timeout: 90000 });
    await pagePrev.goto(urlPrev, { waitUntil: "networkidle2", timeout: 90000 });
    await sleep(1500);

    for (const tab of TABS) {
      const okA = await activateTab(pageApp, tab.id);
      const okB = await activateTab(pagePrev, tab.id);
      await sleep(400);
      const mA = await measureProductPage(pageApp);
      const mB = await measureProductPage(pagePrev);

      const tabFail = [];
      if (!okA || !okB) tabFail.push("TAB_ACTIVATE_FAIL");
      if (mA.tabCount !== 10 || mB.tabCount !== 10) tabFail.push(`TAB_COUNT app=${mA.tabCount} preview=${mB.tabCount}`);
      if (JSON.stringify(mA.tabIds) !== JSON.stringify(mB.tabIds)) tabFail.push("TAB_ID_SET_MISMATCH");
      if (mA.sectionCount !== mB.sectionCount) tabFail.push(`SECTION_COUNT app=${mA.sectionCount} preview=${mB.sectionCount}`);
      if (JSON.stringify(mA.sectionTitles) !== JSON.stringify(mB.sectionTitles)) tabFail.push("SECTION_TITLES_MISMATCH");
      if (mA.cardCount !== mB.cardCount) tabFail.push(`CARD_COUNT app=${mA.cardCount} preview=${mB.cardCount}`);
      if (mA.tableCount !== mB.tableCount) tabFail.push(`TABLE_COUNT app=${mA.tableCount} preview=${mB.tableCount}`);
      if (mA.imageCount !== mB.imageCount) tabFail.push(`IMAGE_COUNT app=${mA.imageCount} preview=${mB.imageCount}`);
      if (mA.titleStyle?.color && mB.titleStyle?.color && mA.titleStyle.color !== mB.titleStyle.color) {
        tabFail.push(`TITLE_COLOR app=${mA.titleStyle.color} preview=${mB.titleStyle.color}`);
      }
      if (mA.andTitleCount > 0 || mB.andTitleCount > 0) {
        notes.push(`${tab.id}: heading uses literal 'And' (prefer &) count app=${mA.andTitleCount} preview=${mB.andTitleCount}`);
      }

      if (WANT_SHOTS) {
        const aPath = join(shotDir, `app-${tab.id}.png`);
        const bPath = join(shotDir, `preview-${tab.id}.png`);
        await pageApp.screenshot({ path: aPath, fullPage: true });
        await pagePrev.screenshot({ path: bPath, fullPage: true });
        report.screenshots.push({ tab: tab.id, app: aPath, preview: bPath });
      }

      report.tabs[tab.id] = {
        label: tab.label,
        app: mA,
        preview: mB,
        fail: tabFail,
        pass: tabFail.length === 0,
      };
      for (const f of tabFail) failures.push(`${tab.id}: ${f}`);
    }

    // Customer-facing combined route smoke (live saved content — may differ from staged)
    const combinedApp = `http://127.0.0.1:${APP_PORT}/brand-explorer-combined.html?id=${encodeURIComponent(BRAND_ID)}&beInternalPreview=1`;
    const combinedPrev = `http://127.0.0.1:${PREVIEW_PORT}/brand-explorer-combined.html?id=${encodeURIComponent(BRAND_ID)}&beInternalPreview=1`;
    await pageApp.goto(combinedApp, { waitUntil: "networkidle2", timeout: 90000 });
    await pagePrev.goto(combinedPrev, { waitUntil: "networkidle2", timeout: 90000 });
    await sleep(3500);
    const cA = await measureProductPage(pageApp);
    const cB = await measureProductPage(pagePrev);
    const expectedIds = TABS.map((t) => t.id);
    report.combinedLiveSaved = {
      urls: { app: combinedApp, preview: combinedPrev },
      appTabCount: cA.tabCount,
      previewTabCount: cB.tabCount,
      appTabNodeCount: cA.tabNodeCount,
      previewTabNodeCount: cB.tabNodeCount,
      tabIdsMatch: JSON.stringify(cA.tabIds) === JSON.stringify(cB.tabIds),
      expectedTenPresentApp: expectedIds.every((id) => cA.tabIds.includes(id)),
      expectedTenPresentPreview: expectedIds.every((id) => cB.tabIds.includes(id)),
      sectionCountApp: cA.sectionCount,
      sectionCountPreview: cB.sectionCount,
      note: "Live saved Presentation content — may differ from staged Gate1 payload until writeback. Duplicate nav nodes (desktop+mobile) are OK if unique tab ids === 10.",
    };
    if (cA.tabCount !== 10 || cB.tabCount !== 10) failures.push("COMBINED_LIVE_MISSING_TEN_TABS");
    if (!report.combinedLiveSaved.expectedTenPresentApp || !report.combinedLiveSaved.expectedTenPresentPreview) {
      failures.push("COMBINED_LIVE_EXPECTED_TAB_IDS_MISSING");
    }
    if (JSON.stringify(cA.tabIds) !== JSON.stringify(cB.tabIds)) failures.push("COMBINED_LIVE_TAB_ID_MISMATCH");

    // Compare Studio routes
    for (const port of [APP_PORT, PREVIEW_PORT]) {
      const u = `http://127.0.0.1:${port}/brand-explorer-compare.html?beInternalPreview=1`;
      const r = await fetchText(u);
      if (!r.ok) failures.push(`COMPARE_STUDIO_HTTP_${port}`);
    }
  } finally {
    await browser.close();
  }

  report.failures = failures;
  report.notes = notes;
  report.verdict = failures.length === 0 ? "PASS" : "FAIL";
  report.airtableWrites = 0;
  report.publicationApplied = false;

  const outJson = join(outDir, "parity-report.json");
  const outMd = join(outDir, "FOUNDER-LOCAL-APP-PARITY-REPORT.md");
  writeFileSync(outJson, JSON.stringify(report, null, 2));

  const md = [];
  md.push("# Brand Explorer — Local Application Parity Report");
  md.push("");
  md.push(`Generated: ${report.generatedAt}`);
  md.push(`Verdict: **${report.verdict}**`);
  md.push("");
  md.push("## Runtime");
  md.push(`- BE workspace HEAD: \`${report.runtime.beWorkspaceHead}\``);
  md.push(`- App port: \`${APP_PORT}\``);
  md.push(`- Preview port: \`${PREVIEW_PORT}\``);
  md.push(`- Brand: \`${BRAND_ID}\``);
  md.push("");
  md.push("## Asset parity");
  for (const [p, row] of Object.entries(report.assets)) {
    md.push(`- ${row.match ? "PASS" : "FAIL"} \`${p}\` sha=${row.appSha.slice(0, 12)}…`);
  }
  md.push("");
  md.push("## Content classes");
  md.push(`- Live saved hash (app): \`${report.api.savedPresentation?.appHash}\``);
  md.push(`- Live saved hash (preview): \`${report.api.savedPresentation?.previewHash}\``);
  md.push(`- Staged Gate1 hash: \`${report.api.stagedPayload?.hash || "n/a"}\``);
  md.push(`- Staged == live: \`${report.api.stagedPayload?.vsLiveAppEqual}\``);
  md.push("");
  md.push("## Ten-tab staged-payload renderer parity");
  for (const tab of TABS) {
    const row = report.tabs[tab.id];
    md.push(`- ${row?.pass ? "PASS" : "FAIL"} **${tab.label}** (\`${tab.id}\`) sections=${row?.app?.sectionCount}`);
    if (row?.fail?.length) for (const f of row.fail) md.push(`  - ${f}`);
  }
  md.push("");
  md.push("## Failures");
  if (!failures.length) md.push("- none");
  else for (const f of failures) md.push(`- ${f}`);
  md.push("");
  md.push("## Notes");
  for (const n of notes) md.push(`- ${n}`);
  md.push("");
  md.push("## Governance");
  md.push("- Airtable writes: 0");
  md.push("- Publication applied: false");
  md.push("- Unpublished candidate ≠ saved Presentation is classified as publication gap, not renderer fail.");
  writeFileSync(outMd, md.join("\n"));

  console.log(JSON.stringify({ ok: failures.length === 0, verdict: report.verdict, failures, outJson, outMd }, null, 2));
  process.exit(failures.length ? 1 : 0);
}

main().catch((err) => {
  console.error(err);
  process.exit(2);
});
