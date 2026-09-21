import { chromium } from "playwright";
import { createServer } from "http";
import { readFileSync, existsSync, mkdirSync, writeFileSync, copyFileSync } from "fs";
import { join, extname, dirname } from "path";
import { fileURLToPath } from "url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const outDir = "/opt/cursor/artifacts/many-futures/sticky-click-fix";
const repoOut = join(root, "visual-review/sticky-click-fix");
mkdirSync(outDir, { recursive: true });
mkdirSync(repoOut, { recursive: true });

const mime = {
  ".html": "text/html",
  ".css": "text/css",
  ".js": "application/javascript",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".webp": "image/webp",
};

const server = createServer((req, res) => {
  const u = decodeURIComponent((req.url || "/").split("?")[0]);
  const p = join(root, u === "/" ? "preview.html" : u);
  if (!existsSync(p)) {
    res.writeHead(404);
    res.end();
    return;
  }
  res.writeHead(200, { "Content-Type": mime[extname(p)] || "application/octet-stream" });
  res.end(readFileSync(p));
});

await new Promise((r) => server.listen(0, "127.0.0.1", r));
const port = server.address().port;
const browser = await chromium.launch({
  executablePath: "/usr/local/bin/google-chrome",
  args: ["--no-sandbox", "--disable-dev-shm-usage"],
});
const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
await page.goto(`http://127.0.0.1:${port}/preview.html`, { waitUntil: "networkidle" });
await page.waitForSelector("#dealality-many-futures.mf-js-ready");

const results = { hoverDoesNotSelect: null, clickAndWorkspace: [], stickyDeepScroll: null };

await page.locator('.mf-q[data-q="actions"]').hover();
await page.waitForTimeout(200);
await page.locator(".mf-workspace").hover({ position: { x: 40, y: 40 } });
await page.waitForTimeout(200);
results.hoverDoesNotSelect = await page.evaluate(() => ({
  q: document.querySelector(".mf-q.is-active")?.dataset.q,
  panel: document.querySelector(".mf-panel.is-active")?.dataset.panel,
}));

const ids = await page.evaluate(() =>
  [...document.querySelectorAll(".mf-q[data-q]")].map((e) => e.dataset.q)
);
for (const id of ids) {
  await page.locator(`.mf-q[data-q="${id}"]`).click();
  await page.locator(".mf-workspace").hover({ position: { x: 60, y: 80 } });
  await page.waitForTimeout(80);
  const s = await page.evaluate(() => ({
    q: document.querySelector(".mf-q.is-active")?.dataset.q,
    panel: document.querySelector(".mf-panel.is-active")?.dataset.panel,
  }));
  results.clickAndWorkspace.push({ id, ...s, ok: s.q === id && s.panel === id });
}

await page.locator('.mf-q[data-q="actions"]').click();
await page.waitForTimeout(200);
const before = await page.evaluate(() => {
  const sticky = document.querySelector(".mf-rail-sticky");
  const rail = document.querySelector(".mf-rail");
  const layout = document.querySelector(".mf-layout");
  return {
    railH: Math.round(rail.getBoundingClientRect().height),
    layoutH: Math.round(layout.getBoundingClientRect().height),
    stickyPos: getComputedStyle(sticky).position,
    stickyTop: Math.round(sticky.getBoundingClientRect().top),
  };
});
await page.evaluate(() => window.scrollBy(0, 700));
await page.waitForTimeout(250);
const after = await page.evaluate(() => {
  const sticky = document.querySelector(".mf-rail-sticky");
  const hotel = document.querySelector(".mf-hotel");
  const q = document.querySelector(".mf-question-list, .mf-questions");
  return {
    scrollY: Math.round(scrollY),
    stickyTop: Math.round(sticky.getBoundingClientRect().top),
    hotelTop: Math.round(hotel.getBoundingClientRect().top),
    qTop: Math.round(q.getBoundingClientRect().top),
    hotelInView:
      hotel.getBoundingClientRect().bottom > 40 &&
      hotel.getBoundingClientRect().top < innerHeight - 40,
    qInView:
      q.getBoundingClientRect().bottom > 40 && q.getBoundingClientRect().top < innerHeight - 40,
    active: document.querySelector(".mf-q.is-active")?.dataset.q,
    panel: document.querySelector(".mf-panel.is-active")?.dataset.panel,
    stickyPos: getComputedStyle(sticky).position,
  };
});
results.stickyDeepScroll = {
  before,
  after,
  stuckNearTop: after.stickyTop >= 15 && after.stickyTop <= 40,
};

const rootEl = await page.$("#dealality-many-futures");
await rootEl.screenshot({ path: join(outDir, "01-q7-actions-selected.png") });
await page.evaluate(() => window.scrollBy(0, 200));
await page.waitForTimeout(200);
await rootEl.screenshot({ path: join(outDir, "02-q7-after-deep-scroll.png") });

writeFileSync(join(outDir, "qa.json"), JSON.stringify(results, null, 2));
for (const f of ["01-q7-actions-selected.png", "02-q7-after-deep-scroll.png", "qa.json"]) {
  copyFileSync(join(outDir, f), join(repoOut, f));
}
console.log(JSON.stringify(results, null, 2));
const fail =
  results.clickAndWorkspace.some((x) => !x.ok) ||
  !results.stickyDeepScroll.stuckNearTop ||
  results.hoverDoesNotSelect.q !== "rebrand";
console.log(fail ? "FAIL" : "PASS");
await browser.close();
server.close();
process.exit(fail ? 1 : 0);
