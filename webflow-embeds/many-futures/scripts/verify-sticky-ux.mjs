import { chromium } from "playwright";
import { createServer } from "http";
import { readFileSync, existsSync, mkdirSync, writeFileSync, copyFileSync } from "fs";
import { join, extname, dirname } from "path";
import { fileURLToPath } from "url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const outDir = "/opt/cursor/artifacts/many-futures/nine-question-extension";
const repoOut = join(root, "visual-review/nine-question-extension");
mkdirSync(outDir, { recursive: true });
mkdirSync(repoOut, { recursive: true });

const mime = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".webp": "image/webp",
};

const server = createServer((req, res) => {
  const urlPath = decodeURIComponent((req.url || "/").split("?")[0]);
  const filePath = join(root, urlPath === "/" ? "preview.html" : urlPath);
  if (!filePath.startsWith(root) || !existsSync(filePath)) {
    res.writeHead(404);
    res.end("Not found");
    return;
  }
  res.writeHead(200, { "Content-Type": mime[extname(filePath)] || "application/octet-stream" });
  res.end(readFileSync(filePath));
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

await page.evaluate(() => document.querySelector('.mf-q[data-q="actions"]').click());
await page.waitForTimeout(350);
await page.hover(".mf-workspace");
await page.waitForTimeout(250);

const check = await page.evaluate(() => {
  const active = document.querySelector(".mf-q.is-active")?.getAttribute("data-q");
  const q = document.querySelector(".mf-questions");
  const h = document.querySelector(".mf-hotel");
  return {
    activeAfterWorkspaceHover: active,
    questionsSticky: q && getComputedStyle(q).position === "sticky",
    hotelSticky: h && getComputedStyle(h).position === "sticky",
  };
});
writeFileSync(join(outDir, "ux-sticky-selection.json"), JSON.stringify(check, null, 2));
console.log(check);

const el = await page.$("#dealality-many-futures");
await el.screenshot({ path: join(outDir, "10-desktop-ux-sticky-q7-actions.png") });
await page.evaluate(() => window.scrollBy(0, 480));
await page.waitForTimeout(250);
await el.screenshot({ path: join(outDir, "11-desktop-ux-after-scroll-q7.png") });

for (const f of [
  "10-desktop-ux-sticky-q7-actions.png",
  "11-desktop-ux-after-scroll-q7.png",
  "ux-sticky-selection.json",
]) {
  copyFileSync(join(outDir, f), join(repoOut, f));
}

await browser.close();
server.close();
console.log("ok");
