/**
 * Local QA capture for Manual Process problem section (compression pass).
 * Usage: node scripts/qa-manual-process-preview.mjs
 */
import fs from "fs";
import path from "path";
import puppeteer from "puppeteer";

const OUT = path.resolve("public/marketing/qa-shots/manual-process-v1");
const URL = "http://127.0.0.1:4177/old-home-manual-process-preview-inline.html";
const BEFORE = {
  "desktop-1440": 1300,
  "desktop-1200": 1297,
  "tablet-768": 2702,
  "mobile-390": 2604,
  "mobile-320": 2551,
};

fs.mkdirSync(OUT, { recursive: true });

const widths = [
  { name: "desktop-1440", width: 1440, height: 1400 },
  { name: "desktop-1200", width: 1200, height: 1400 },
  { name: "tablet-768", width: 768, height: 2400 },
  { name: "mobile-390", width: 390, height: 2400 },
  { name: "mobile-320", width: 320, height: 2400 },
];

async function measure(page) {
  return page.evaluate(() => {
    const root = document.getElementById("dealality-manual-process");
    const overflows = [];
    document.querySelectorAll("#dealality-manual-process, #dealality-manual-process *").forEach((el) => {
      const rect = el.getBoundingClientRect();
      if (rect.right > window.innerWidth + 2 || rect.left < -2) {
        overflows.push({
          tag: el.tagName,
          cls: (el.className && String(el.className).slice(0, 80)) || "",
        });
      }
    });
    const solid = [...document.querySelectorAll(".dmp-connectors--desktop .dmp-line-out-solid")];
    const dots = [...document.querySelectorAll(".dmp-connectors--desktop .dmp-line-out-dot")];
    const h = Math.round(root.getBoundingClientRect().height);
    return {
      width: window.innerWidth,
      sectionHeight: h,
      viewportScreens: Number((h / window.innerHeight).toFixed(2)),
      scrollWidth: document.documentElement.scrollWidth,
      clientWidth: document.documentElement.clientWidth,
      hasHScroll: document.documentElement.scrollWidth > document.documentElement.clientWidth + 1,
      overflowCount: overflows.length,
      solidDesktop: solid.length,
      dottedDesktop: dots.length,
      solidStroke: solid[0] ? getComputedStyle(solid[0]).stroke : null,
      dottedDash: dots[0] ? getComputedStyle(dots[0]).strokeDasharray : null,
      anyCircleInOutSvg: !!document.querySelector(".dmp-connectors--out circle"),
      anyTextX: [...document.querySelectorAll(".dmp-connectors--out text")].some((t) =>
        /x/i.test(t.textContent || "")
      ),
      ctaHref: document.querySelector(".dmp-cta")?.href || null,
      copyCheck: {
        eyebrow: document.querySelector(".dmp-eyebrow")?.textContent?.trim(),
        h2: document.querySelector(".dmp-h2")?.innerText?.replace(/\s+/g, " ").trim(),
        close: document.querySelector(".dmp-close-q")?.innerText?.replace(/\s+/g, " ").trim(),
        cta: document.querySelector(".dmp-cta")?.innerText?.replace(/\s+/g, " ").trim(),
        paths: [...document.querySelectorAll(".dmp-path-label")].map((n) => n.textContent.trim()),
        problems: [...document.querySelectorAll(".dmp-problem-h")].map((n) => n.textContent.trim()),
      },
    };
  });
}

const browser = await puppeteer.launch({
  headless: "new",
  args: ["--no-sandbox", "--disable-setuid-sandbox"],
});
const page = await browser.newPage();
const report = { console: [], metrics: {}, beforeAfter: {}, shots: [] };

page.on("console", (msg) => {
  report.console.push({ type: msg.type(), text: msg.text() });
});
page.on("pageerror", (err) => {
  report.console.push({ type: "pageerror", text: String(err) });
});

for (const w of widths) {
  await page.setViewport({ width: w.width, height: w.height, deviceScaleFactor: 1 });
  await page.goto(URL, { waitUntil: "networkidle2", timeout: 60000 });
  await page.waitForSelector("#dealality-manual-process");
  await page.evaluate(() => {
    const root = document.getElementById("dealality-manual-process");
    root.classList.add("is-drawn");
    root.querySelectorAll("[data-dmp-draw]").forEach((p) => {
      const kind = p.getAttribute("data-dmp-draw");
      p.style.strokeDashoffset = "0";
      p.style.strokeDasharray = kind === "out-dot" ? "3 5" : "none";
    });
  });
  await new Promise((r) => setTimeout(r, 500));
  const metrics = await measure(page);
  report.metrics[w.name] = metrics;
  report.beforeAfter[w.name] = {
    before: BEFORE[w.name],
    after: metrics.sectionHeight,
    delta: metrics.sectionHeight - BEFORE[w.name],
  };
  const fullPath = path.join(OUT, `${w.name}.png`);
  await page.screenshot({ path: fullPath, fullPage: true });
  report.shots.push(fullPath);

  if (w.width === 1440) {
    const journey = await page.$(".dmp-journey-row");
    if (journey) await journey.screenshot({ path: path.join(OUT, "closeup-journey.png") });
    const inConn = await page.$(".dmp-connectors--desktop.dmp-connectors--in");
    if (inConn) await inConn.screenshot({ path: path.join(OUT, "closeup-incoming.png") });
    const outConn = await page.$(".dmp-connectors--desktop.dmp-connectors--out");
    if (outConn) await outConn.screenshot({ path: path.join(OUT, "closeup-outgoing.png") });
    const problems = await page.$(".dmp-problems");
    if (problems) await problems.screenshot({ path: path.join(OUT, "closeup-problems.png") });
    const close = await page.$(".dmp-close");
    if (close) await close.screenshot({ path: path.join(OUT, "closeup-close-cta.png") });
  }

  if (w.width === 390) {
    // Full-page already captures screens; also viewport-framed shot from top
    await page.screenshot({
      path: path.join(OUT, "mobile-390-viewport-screens.png"),
      fullPage: true,
    });
    const opp = await page.$(".dmp-card--opp");
    if (opp) await opp.screenshot({ path: path.join(OUT, "mobile-closeup-opportunity.png") });
    const man = await page.$(".dmp-card--manual");
    if (man) await man.screenshot({ path: path.join(OUT, "mobile-closeup-manual.png") });
    const outM = await page.$(".dmp-connectors--mobile.dmp-connectors--out");
    if (outM) await outM.screenshot({ path: path.join(OUT, "mobile-closeup-outgoing.png") });
    const probs = await page.$(".dmp-problems");
    if (probs) await probs.screenshot({ path: path.join(OUT, "mobile-closeup-problems.png") });
  }
}

await page.setViewport({ width: 1440, height: 1400, deviceScaleFactor: 1 });
await page.emulateMediaFeatures([{ name: "prefers-reduced-motion", value: "reduce" }]);
await page.goto(URL, { waitUntil: "networkidle2", timeout: 60000 });
await page.waitForSelector("#dealality-manual-process");
await new Promise((r) => setTimeout(r, 600));
report.metrics["reduced-motion-1440"] = await measure(page);
await page.screenshot({
  path: path.join(OUT, "reduced-motion-1440.png"),
  fullPage: true,
});

fs.writeFileSync(path.join(OUT, "qa-report.json"), JSON.stringify(report, null, 2));
console.log(JSON.stringify({ beforeAfter: report.beforeAfter, metrics: report.metrics, console: report.console }, null, 2));
await browser.close();
