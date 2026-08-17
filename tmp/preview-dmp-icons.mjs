import puppeteer from "puppeteer";
import fs from "fs";
import path from "path";
import http from "http";

const root = path.resolve("public/marketing");
const server = http.createServer((req, res) => {
  const file = path.join(root, decodeURIComponent(req.url.split("?")[0]));
  if (!file.startsWith(root) || !fs.existsSync(file) || fs.statSync(file).isDirectory()) {
    res.writeHead(404);
    res.end("missing");
    return;
  }
  const ext = path.extname(file);
  const type =
    ext === ".css"
      ? "text/css"
      : ext === ".js"
        ? "text/javascript"
        : ext === ".html"
          ? "text/html"
          : "application/octet-stream";
  res.writeHead(200, { "Content-Type": type });
  fs.createReadStream(file).pipe(res);
});

await new Promise((r) => server.listen(4188, "127.0.0.1", r));
const browser = await puppeteer.launch({ headless: "new" });
const page = await browser.newPage();
await page.setViewport({ width: 1440, height: 1200, deviceScaleFactor: 1 });
await page.goto("http://127.0.0.1:4188/old-home-manual-process-icon-qa.html", {
  waitUntil: "domcontentloaded",
});
await page.evaluate(async () => {
  if (document.fonts?.ready) await document.fonts.ready;
  document.getElementById("dealality-manual-process")?.classList.add("is-drawn");
});
await new Promise((r) => setTimeout(r, 400));
const out = path.resolve("docs/old-home-manual-process-staging-qa-20260731");
fs.mkdirSync(out, { recursive: true });
const cards = await page.$(".dmp-problems");
await cards.screenshot({ path: path.join(out, "cards-icons-benefits-style-1440.png") });
await page.setViewport({ width: 390, height: 900, deviceScaleFactor: 1 });
await new Promise((r) => setTimeout(r, 200));
await cards.screenshot({ path: path.join(out, "cards-icons-benefits-style-390.png") });
console.log("ok");
await browser.close();
server.close();
