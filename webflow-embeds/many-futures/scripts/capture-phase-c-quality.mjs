#!/usr/bin/env node
/**
 * Phase C image-quality gate captures at actual display sizes.
 * Writes to /opt/cursor/artifacts and repo visual-review/phase-c/.
 */
import { chromium } from "playwright";
import { createServer } from "http";
import {
  readFileSync,
  existsSync,
  mkdirSync,
  statSync,
  writeFileSync,
  copyFileSync,
} from "fs";
import { join, extname, dirname } from "path";
import { fileURLToPath } from "url";
import { createRequire } from "module";

const require = createRequire(import.meta.url);
const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const outDir = "/opt/cursor/artifacts/many-futures/phase-c-quality";
const repoOut = join(root, "visual-review", "phase-c");
mkdirSync(outDir, { recursive: true });
mkdirSync(repoOut, { recursive: true });

const mime = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".webp": "image/webp",
};

const server = createServer((req, res) => {
  const urlPath = decodeURIComponent((req.url || "/").split("?")[0]);
  let filePath = join(root, urlPath === "/" ? "preview.html" : urlPath);
  if (!filePath.startsWith(root) || !existsSync(filePath) || statSync(filePath).isDirectory()) {
    res.writeHead(404);
    res.end("Not found");
    return;
  }
  res.writeHead(200, { "Content-Type": mime[extname(filePath)] || "application/octet-stream" });
  res.end(readFileSync(filePath));
});

await new Promise((r) => server.listen(0, "127.0.0.1", r));
const port = server.address().port;
const base = `http://127.0.0.1:${port}/preview.html`;

const browser = await chromium.launch({
  executablePath: "/usr/local/bin/google-chrome",
  args: ["--no-sandbox", "--disable-dev-shm-usage"],
});

const questions = ["rebrand", "operators", "affiliation", "residences", "proposals", "clarify"];

async function openPage(width, height, dpr = 1) {
  const page = await browser.newPage({
    viewport: { width, height },
    deviceScaleFactor: dpr,
  });
  await page.goto(base, { waitUntil: "networkidle" });
  await page.waitForSelector("#dealality-many-futures.mf-js-ready");
  await page.waitForTimeout(400);
  return page;
}

async function activate(page, id) {
  await page.click(`.mf-q[data-q="${id}"]`);
  await page.waitForTimeout(450);
}

async function save(page, name, clipSelector = "#dealality-many-futures") {
  const el = await page.$(clipSelector);
  const path = join(outDir, name);
  if (el) {
    await el.scrollIntoViewIfNeeded();
    await page.waitForTimeout(200);
    await el.screenshot({ path, type: "png" });
  } else {
    await page.screenshot({ path, type: "png", fullPage: true });
  }
  copyFileSync(path, join(repoOut, name));
  return path;
}

const metrics = [];

async function measureRendered(page, label) {
  const data = await page.evaluate(() => {
    const root = document.getElementById("dealality-many-futures");
    const imgs = [...root.querySelectorAll("img")].map((img) => {
      const r = img.getBoundingClientRect();
      return {
        asset: img.getAttribute("data-mf-asset") || img.getAttribute("src")?.split("/").pop(),
        naturalW: img.naturalWidth,
        naturalH: img.naturalHeight,
        displayW: Math.round(r.width * 10) / 10,
        displayH: Math.round(r.height * 10) / 10,
        complete: img.complete,
        currentSrc: img.currentSrc || img.src,
        className: img.className,
        hasSrc: !!img.getAttribute("src"),
      };
    });
    const hotel = root.querySelector(".mf-hotel-media");
    const hr = hotel?.getBoundingClientRect();
    const primary = root.querySelector(".mf-panel.is-active .mf-feat--primary .mf-feat-visual");
    const pr = primary?.getBoundingClientRect();
    const support = [...root.querySelectorAll(".mf-panel.is-active .mf-feat--support .mf-feat-visual")].map(
      (n) => {
        const r = n.getBoundingClientRect();
        return { w: Math.round(r.width), h: Math.round(r.height), ui: n.classList.contains("mf-feat-visual--ui") };
      }
    );
    return {
      hotel: hr ? { w: Math.round(hr.width), h: Math.round(hr.height) } : null,
      primary: pr ? { w: Math.round(pr.width), h: Math.round(pr.height), ui: primary.classList.contains("mf-feat-visual--ui") } : null,
      support,
      imgs,
      loadedBytesEstimate: imgs
        .filter((i) => i.hasSrc && i.complete)
        .map((i) => i.currentSrc),
    };
  });
  metrics.push({ label, ...data });
  return data;
}

// Default-state captures
const viewports = [
  ["desktop-1440", 1440, 1100, 1],
  ["laptop-1200", 1200, 1000, 1],
  ["tablet-768", 768, 1100, 1],
  ["mobile-390", 390, 1400, 1],
  ["mobile-320", 320, 1400, 1],
  ["desktop-1440-2x", 1440, 1100, 2],
  ["mobile-390-2x", 390, 1400, 2],
];

for (const [name, w, h, dpr] of viewports) {
  const page = await openPage(w, h, dpr);
  await activate(page, "rebrand");
  await measureRendered(page, `${name}-q1`);
  await save(page, `qa-${name}-q1-rebrand.png`);

  // zoom checks on desktop/mobile only
  if (name === "desktop-1440") {
    await page.evaluate(() => {
      document.body.style.zoom = "1.25";
    });
    await page.waitForTimeout(250);
    await save(page, "qa-desktop-1440-zoom125-q1.png");
    await page.evaluate(() => {
      document.body.style.zoom = "2";
    });
    await page.waitForTimeout(250);
    await save(page, "qa-desktop-1440-zoom200-q1.png");
    await page.evaluate(() => {
      document.body.style.zoom = "1";
    });
  }
  await page.close();
}

// All six questions desktop + mobile
for (const id of questions) {
  const desk = await openPage(1440, 1200, 1);
  await activate(desk, id);
  await measureRendered(desk, `desktop-1440-${id}`);
  await save(desk, `qa-desktop-1440-${id}.png`);
  // primary visual crop
  const primary = await desk.$(".mf-panel.is-active .mf-feat--primary .mf-feat-visual");
  if (primary) {
    const p = join(outDir, `qa-panel-primary-desk-${id}.png`);
    await primary.screenshot({ path: p, type: "png" });
    copyFileSync(p, join(repoOut, `qa-panel-primary-desk-${id}.png`));
  }
  await desk.close();

  const mob = await openPage(390, 1600, 1);
  await activate(mob, id);
  await measureRendered(mob, `mobile-390-${id}`);
  await save(mob, `qa-mobile-390-${id}.png`);
  const primaryM = await mob.$(".mf-panel.is-active .mf-feat--primary .mf-feat-visual");
  if (primaryM) {
    const p = join(outDir, `qa-panel-primary-mob-${id}.png`);
    await primaryM.screenshot({ path: p, type: "png" });
    copyFileSync(p, join(repoOut, `qa-panel-primary-mob-${id}.png`));
  }
  await mob.close();
}

// Transition flash / CLS check: switch questions and record layout shifts
const flashPage = await openPage(1440, 1100, 1);
await activate(flashPage, "rebrand");
const flashLog = [];
await flashPage.evaluate(() => {
  window.__mfCls = 0;
  new PerformanceObserver((list) => {
    for (const e of list.getEntries()) {
      if (e.hadRecentInput) continue;
      window.__mfCls += e.value;
    }
  }).observe({ type: "layout-shift", buffered: true });
});
for (const id of questions) {
  const t0 = Date.now();
  await activate(flashPage, id);
  const blank = await flashPage.evaluate(() => {
    const panel = document.querySelector(".mf-panel.is-active");
    const imgs = [...panel.querySelectorAll("img.mf-feat-img")].filter((i) => {
      const style = getComputedStyle(i);
      return style.display !== "none" && i.getAttribute("src");
    });
    const incomplete = imgs.filter((i) => !i.complete || i.naturalWidth === 0).length;
    const ui = panel.querySelectorAll(".mf-feat-visual--ui .mf-ui").length;
    return { incomplete, ui, imgCount: imgs.length };
  });
  flashLog.push({ id, ms: Date.now() - t0, ...blank });
}
const cls = await flashPage.evaluate(() => window.__mfCls || 0);
flashLog.push({ cls });
await save(flashPage, "qa-desktop-after-all-six.png");
await flashPage.close();

// Asset weight accounting
function fileBytes(rel) {
  const p = join(root, rel);
  return existsSync(p) ? statSync(p).size : 0;
}

const featureDesktop = [
  "brand-explorer-desktop.png",
  "smart-matching-desktop.png",
  "radar-desktop.png",
  "operator-explorer-desktop.png",
  "operator-track-record-desktop.png",
  "fee-estimator-desktop.png",
  "opportunity-review-desktop.png",
  "deal-compare-desktop.png",
];
const featureMobile = featureDesktop.map((f) => f.replace("-desktop.png", "-mobile.png"));

const defaultDesktopBytes =
  fileBytes("assets/hotel-final.jpg") +
  fileBytes("assets/features/brand-explorer-desktop.png") +
  fileBytes("assets/features/smart-matching-desktop.png") +
  fileBytes("assets/features/radar-desktop.png");

// With WebP hotel preferred in picture element — estimate default using 960 webp as typical laptop choice
const defaultHotelWebp = fileBytes("assets/hotel-final-960.webp");

const allUniqueRaster = new Set([
  ...featureDesktop.map((f) => `assets/features/${f}`),
  ...featureMobile.map((f) => `assets/features/${f}`),
  "assets/hotel-final.jpg",
  "assets/hotel-final-640.webp",
  "assets/hotel-final-960.webp",
  "assets/hotel-final-1280.webp",
]);

let allBytes = 0;
for (const rel of allUniqueRaster) allBytes += fileBytes(rel);

const report = {
  capturedAt: new Date().toISOString(),
  previewBase: base,
  note: "Local preview.html captures (parity with Phase C markup/CSS/JS). Webflow Designer Preview snapshots captured separately via MCP.",
  viewports,
  flashLog,
  cls,
  defaultStateImageWeightApprox: {
    desktopEagerRasterBytes: defaultDesktopBytes,
    desktopEagerRasterKB: Math.round(defaultDesktopBytes / 1024),
    hotelJpgKB: Math.round(fileBytes("assets/hotel-final.jpg") / 1024),
    hotelWebp960KB: Math.round(defaultHotelWebp / 1024),
    note: "Default Q1 loads hotel + Brand Explorer desk + Smart Matching desk + Radar desk. Support/mobile variants and inactive panels are deferred (src stripped until activation).",
  },
  allStatesAdditionalApprox: {
    allDeployedRasterBytes: allBytes,
    allDeployedRasterKB: Math.round(allBytes / 1024),
    additionalAfterDefaultKB: Math.round((allBytes - defaultDesktopBytes) / 1024),
  },
  renderedMetrics: metrics,
};

writeFileSync(join(outDir, "quality-metrics.json"), JSON.stringify(report, null, 2));
writeFileSync(join(repoOut, "quality-metrics.json"), JSON.stringify(report, null, 2));

console.log(JSON.stringify({ outDir, repoOut, cls, flashLog, defaultKB: report.defaultStateImageWeightApprox.desktopEagerRasterKB, allKB: report.allStatesAdditionalApprox.allDeployedRasterKB }, null, 2));

await browser.close();
server.close();
