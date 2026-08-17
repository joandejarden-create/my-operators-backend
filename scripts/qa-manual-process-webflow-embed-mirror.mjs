/**
 * QA the exact Webflow CDN embed chain (shell CSS + section CSS + HTML fetch + boot/draw JS).
 * Usage: serve docs/ then: node scripts/qa-manual-process-webflow-embed-mirror.mjs
 */
import fs from "fs";
import path from "path";
import http from "http";
import { fileURLToPath } from "url";
import puppeteer from "puppeteer";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "..");
const OUT = path.resolve(root, "public/marketing/qa-shots/designer-preview-manual-process");
const APPROVED = {
  1440: 969,
  1200: 969,
  768: 1851,
  390: 1669,
  320: 1617,
};

fs.mkdirSync(OUT, { recursive: true });

const filePath = path.join(root, "docs/old-home-manual-process-webflow-embed-mirror.html");
const html = fs.readFileSync(filePath);

const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
  res.end(html);
});
await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
const { port } = server.address();
const URL = `http://127.0.0.1:${port}/`;

const widths = [
  { name: "desktop-1440", width: 1440, height: 1200 },
  { name: "desktop-1200", width: 1200, height: 1200 },
  { name: "tablet-768", width: 768, height: 2200 },
  { name: "mobile-390", width: 390, height: 2200 },
  { name: "mobile-320", width: 320, height: 2200 },
];

const browser = await puppeteer.launch({
  headless: "new",
  args: ["--no-sandbox", "--disable-setuid-sandbox"],
});
const page = await browser.newPage();
const consoleLogs = [];
page.on("console", (msg) => consoleLogs.push({ type: msg.type(), text: msg.text() }));
page.on("pageerror", (err) => consoleLogs.push({ type: "pageerror", text: String(err) }));

const report = { url: URL, approved: APPROVED, metrics: {}, closeups: {}, console: consoleLogs };

for (const w of widths) {
  await page.setViewport({ width: w.width, height: w.height, deviceScaleFactor: 1 });
  await page.goto(URL, { waitUntil: "networkidle0", timeout: 60000 });
  await page.waitForSelector("#dealality-manual-process", { timeout: 20000 });
  await page.waitForFunction(() => {
    const root = document.getElementById("dealality-manual-process");
    return root && root.classList.contains("is-drawn");
  }, { timeout: 15000 }).catch(() => {});
  await new Promise((r) => setTimeout(r, 400));

  const metrics = await page.evaluate(() => {
    const root = document.getElementById("dealality-manual-process");
    const about = document.getElementById("about");
    const solid = [...document.querySelectorAll(".dmp-connectors--desktop .dmp-line-out-solid")];
    const dots = [...document.querySelectorAll(".dmp-connectors--desktop .dmp-line-out-dot")];
    return {
      sectionHeight: Math.round(root.getBoundingClientRect().height),
      aboutHeight: Math.round(about.getBoundingClientRect().height),
      hasHScroll: document.documentElement.scrollWidth > document.documentElement.clientWidth + 1,
      solidDesktop: solid.length,
      dottedDesktop: dots.length,
      anyCircleInOutSvg: !!document.querySelector(".dmp-connectors--out circle"),
      anyTextX: [...document.querySelectorAll(".dmp-connectors--out text")].some((t) =>
        /x/i.test(t.textContent || "")
      ),
      ctaHref: document.querySelector(".dmp-cta")?.href || null,
      pathCount: document.querySelectorAll(".dmp-path-label").length,
      closeCount: document.querySelectorAll(".dmp-close-q").length,
      eyebrow: document.querySelector(".dmp-eyebrow")?.textContent?.trim(),
      h2: document.querySelector(".dmp-h2")?.innerText?.replace(/\s+/g, " ").trim(),
      hostGone: !document.getElementById("dealality-manual-process-host"),
      problemTitles: [...document.querySelectorAll(".dmp-problem-h")].map((n) => n.textContent.trim()),
      problemBodies: [...document.querySelectorAll(".dmp-problem-p")].map((n) => n.textContent.trim()),
      clippedProblems: [...document.querySelectorAll(".dmp-problem")].some((el) => {
        return el.scrollHeight > el.clientHeight + 2 || el.scrollWidth > el.clientWidth + 2;
      }),
    };
  });
  report.metrics[w.name] = {
    ...metrics,
    approved: APPROVED[w.width],
    delta: metrics.sectionHeight - APPROVED[w.width],
  };

  // mobile close-up of problem rows
  if (w.width <= 390) {
    const problems = await page.$(".dmp-problems");
    if (problems) {
      await problems.screenshot({ path: path.join(OUT, `${w.name}-problems.png`) });
    }
  }

  const shot = path.join(OUT, `${w.name}.png`);
  const el = await page.$("#dealality-manual-process");
  await el.screenshot({ path: shot });
}

// Close-ups at 1440
await page.setViewport({ width: 1440, height: 1200, deviceScaleFactor: 1 });
await page.goto(URL, { waitUntil: "networkidle0", timeout: 60000 });
await page.waitForSelector("#dealality-manual-process", { timeout: 20000 });
await page.waitForFunction(() => document.getElementById("dealality-manual-process")?.classList.contains("is-drawn"), { timeout: 15000 }).catch(() => {});

const closeups = [
  { name: "incoming-paths", sel: ".dmp-connectors--desktop.dmp-connectors--in" },
  { name: "outgoing-paths", sel: ".dmp-connectors--desktop.dmp-connectors--out" },
  { name: "problem-blocks", sel: ".dmp-problems" },
  { name: "close-cta", sel: ".dmp-close" },
];
for (const c of closeups) {
  const handle = await page.$(c.sel);
  if (!handle) continue;
  const p = path.join(OUT, `${c.name}.png`);
  await handle.screenshot({ path: p });
  report.closeups[c.name] = p;
}

// Reduced motion
await page.emulateMediaFeatures([{ name: "prefers-reduced-motion", value: "reduce" }]);
await page.reload({ waitUntil: "networkidle0" });
await page.waitForSelector("#dealality-manual-process", { timeout: 20000 });
await new Promise((r) => setTimeout(r, 500));
const reduced = await page.evaluate(() => {
  const root = document.getElementById("dealality-manual-process");
  return {
    drawn: root?.classList.contains("is-drawn"),
    solid: document.querySelectorAll(".dmp-line-out-solid").length,
    dots: document.querySelectorAll(".dmp-line-out-dot").length,
    h2: document.querySelector(".dmp-h2")?.innerText?.replace(/\s+/g, " ").trim(),
  };
});
report.reducedMotion = reduced;
await page.screenshot({ path: path.join(OUT, "reduced-motion-1440.png"), fullPage: true });

fs.writeFileSync(path.join(OUT, "report.json"), JSON.stringify(report, null, 2));
console.log(JSON.stringify(report, null, 2));
await browser.close();
server.close();
