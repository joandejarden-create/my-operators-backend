#!/usr/bin/env node
/**
 * Phase B delivery screenshots:
 * breakpoints, six question states, unique feature closeups.
 */
import { chromium } from "playwright";
import { createServer } from "http";
import { readFileSync, existsSync, mkdirSync, statSync } from "fs";
import { join, extname, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const outDir = "/opt/cursor/artifacts/many-futures/phase-b";
mkdirSync(outDir, { recursive: true });

const mime = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".svg": "image/svg+xml",
};

const server = createServer((req, res) => {
  const urlPath = decodeURIComponent((req.url || "/").split("?")[0]);
  let filePath = join(root, urlPath === "/" ? "preview.html" : urlPath);
  if (!filePath.startsWith(root) || !existsSync(filePath) || statSync(filePath).isDirectory()) {
    res.writeHead(404);
    res.end("Not found");
    return;
  }
  const type = mime[extname(filePath)] || "application/octet-stream";
  res.writeHead(200, { "Content-Type": type });
  res.end(readFileSync(filePath));
});

await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
const port = server.address().port;
const base = `http://127.0.0.1:${port}/preview.html`;

const browser = await chromium.launch({
  executablePath: "/usr/local/bin/google-chrome",
  args: ["--no-sandbox", "--disable-dev-shm-usage"],
});

const questions = [
  "rebrand",
  "operators",
  "affiliation",
  "residences",
  "proposals",
  "clarify",
];

async function shotPage(page, name) {
  const path = join(outDir, name);
  await page.screenshot({ path, fullPage: true });
  console.log("wrote", path);
}

async function captureViewport(width, height, label) {
  const page = await browser.newPage({
    viewport: { width, height },
    deviceScaleFactor: 1,
  });
  await page.goto(base, { waitUntil: "networkidle" });
  await page.waitForSelector("#dealality-many-futures.mf-js-ready");
  await page.waitForTimeout(400);

  // Default state (Q1)
  await shotPage(page, `mf-phase-b-${label}-${width}.png`);

  if (label === "desktop") {
    for (const q of questions) {
      await page.click(`.mf-q[data-q="${q}"]`);
      await page.waitForTimeout(350);
      await shotPage(page, `mf-phase-b-state-${q}-${width}.png`);
    }
  }

  if (label === "mobile") {
    for (const q of questions) {
      await page.click(`.mf-q[data-q="${q}"]`);
      await page.waitForTimeout(350);
      await shotPage(page, `mf-phase-b-mobile-state-${q}.png`);
    }
  }

  await page.close();
}

await captureViewport(1440, 1100, "desktop");
await captureViewport(1200, 1000, "laptop");
await captureViewport(768, 1024, "tablet");
await captureViewport(390, 900, "mobile");

/* Unique feature closeups from a dedicated gallery page */
const features = [
  "brand-explorer",
  "operator-explorer",
  "fee-estimator",
  "radar",
  "opportunity-review",
  "deal-compare",
  "smart-matching",
  "deal-readiness",
  "clause-library",
  "financial-term-library",
  "submit-proposal",
];

const galleryHtml = `<!DOCTYPE html><html><head><meta charset="utf-8"/><style>
body{margin:0;background:#0b1228;color:#fff;font:14px/1.4 system-ui}
.grid{display:grid;grid-template-columns:1fr;gap:24px;padding:24px;max-width:1100px;margin:0 auto}
figure{margin:0;border:1px solid rgba(255,255,255,.12);border-radius:12px;overflow:hidden;background:#111b3a}
figcaption{padding:10px 14px;font-weight:600}
img{display:block;width:100%;height:auto}
</style></head><body><div class="grid">
${features
  .map(
    (f) => `<figure id="${f}"><img src="/assets/features/${f}-desktop.png" alt="${f}"/><figcaption>${f} — desktop</figcaption></figure>`
  )
  .join("\n")}
</div></body></html>`;

const galleryPath = join(root, "feature-gallery.html");
const { writeFileSync } = await import("fs");
writeFileSync(galleryPath, galleryHtml);

const page = await browser.newPage({
  viewport: { width: 1100, height: 800 },
  deviceScaleFactor: 1,
});
await page.goto(`http://127.0.0.1:${port}/feature-gallery.html`, {
  waitUntil: "networkidle",
});
await page.waitForTimeout(300);

for (const f of features) {
  const el = page.locator(`#${f}`);
  await el.scrollIntoViewIfNeeded();
  await page.waitForTimeout(100);
  await el.screenshot({ path: join(outDir, `mf-phase-b-feature-${f}.png`) });
  console.log("wrote feature", f);
}

await page.close();
await browser.close();
server.close();
console.log("Done. Output:", outDir);
