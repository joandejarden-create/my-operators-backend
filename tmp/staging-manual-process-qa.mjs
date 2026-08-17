import puppeteer from "puppeteer";
import fs from "fs";
import path from "path";

const OUT = path.resolve("docs/old-home-manual-process-staging-qa-20260731");
fs.mkdirSync(OUT, { recursive: true });

const URL =
  "https://mvp-deal-capture.webflow.io/old-home?nocache=" + Date.now();
const WIDTHS = [1440, 1200, 768, 390, 320];
const BASELINE = {
  1440: { prior: 969, expected: 956, delta: -13 },
  1200: { prior: 969, expected: 973, delta: 4 },
  768: { prior: 1851, expected: 1865, delta: 14 },
  390: { prior: 1669, expected: 1650, delta: -19 },
  320: { prior: 1617, expected: 1626, delta: 9 },
};

const EXPECTED_TITLES = [
  "Manual. Fragmented. Costly.",
  "Too Few Options. Hard to Compare.",
  "Value Can Be Left on the Table.",
];

async function waitForSection(page) {
  await page.waitForFunction(
    () => !!document.querySelector("#dealality-manual-process"),
    { timeout: 20000 }
  );
  await page.evaluate(async () => {
    if (document.fonts?.ready) await document.fonts.ready;
    const r = document.querySelector("#dealality-manual-process");
    if (r) {
      r.classList.add("is-drawn");
      r.classList.remove("is-animating");
    }
  });
  await new Promise((r) => setTimeout(r, 400));
}

async function measure(page) {
  return page.evaluate(() => {
    const about = document.querySelector("#about");
    const r = document.querySelector("#dealality-manual-process");
    const mf = document.querySelector("#many-futures");
    const pf = document.getElementById("platform-features");
    const card = r?.querySelector(".dmp-problem");
    const icon = card?.querySelector(".dmp-problem-icon");
    const text = card?.querySelector(".dmp-problem-text");
    const titles = [...document.querySelectorAll(".dmp-problem-h")].map((e) =>
      e.textContent.trim()
    );
    const bodies = [...document.querySelectorAll(".dmp-problem-p")].map((e) =>
      e.textContent.trim()
    );
    const old = [
      "Fragmented Process",
      "Comparison Weakened",
      "Value Left Unseen",
    ].filter((t) => (document.body.innerText || "").includes(t));
    const perf = performance
      .getEntriesByType("resource")
      .filter((e) => /manual-process/i.test(e.name))
      .map((e) => ({
        name: e.name.split("/").pop(),
        status: e.responseStatus || null,
        transfer: e.transferSize,
      }));
    return {
      vw: innerWidth,
      dataOh: about?.getAttribute("data-oh-problem"),
      sectionH: r ? Math.round(r.getBoundingClientRect().height) : null,
      aboutH: about ? Math.round(about.getBoundingClientRect().height) : null,
      titles,
      bodies,
      old,
      overflowX: document.documentElement.scrollWidth > innerWidth + 2,
      initCount: document.querySelectorAll('[data-dmp-bound="1"]').length,
      bound: r?.getAttribute("data-dmp-bound"),
      version: r?.getAttribute("data-dmp-version"),
      cardFlex: card ? getComputedStyle(card).flexDirection : null,
      iconLeftOfText:
        icon && text
          ? icon.getBoundingClientRect().left < text.getBoundingClientRect().left
          : null,
      clippedBodies: [...document.querySelectorAll(".dmp-problem-p")].map(
        (e) => e.scrollHeight > e.clientHeight + 1
      ),
      mf: mf
        ? {
            exists: true,
            h2: (mf.querySelector("h2")?.textContent || "").trim(),
            display: getComputedStyle(mf).display,
          }
        : { exists: false },
      pf: pf
        ? {
            exists: true,
            display: getComputedStyle(pf).display,
            visibility: getComputedStyle(pf).visibility,
          }
        : { exists: false },
      aboutNext: about?.nextElementSibling?.id || null,
      assets: perf,
      probe: /probe|DEBUG|temporary/i.test(r?.innerHTML || ""),
      hostVisibleText: (
        document.querySelector("#dealality-manual-process-host")?.textContent ||
        ""
      )
        .trim()
        .slice(0, 80),
    };
  });
}

async function shot(page, sel, file) {
  const el = await page.$(sel);
  if (!el) throw new Error("missing " + sel);
  await el.scrollIntoViewIfNeeded();
  await new Promise((r) => setTimeout(r, 200));
  await el.screenshot({ path: path.join(OUT, file) });
}

const browser = await puppeteer.launch({
  headless: "new",
  defaultViewport: { width: 1440, height: 1600, deviceScaleFactor: 1 },
  args: ["--disable-http-cache", "--window-size=1440,1600"],
});

const page = await browser.newPage();
await page.setCacheEnabled(false);
page.setDefaultNavigationTimeout(90000);

const consoleMsgs = [];
page.on("console", (msg) => {
  if (msg.type() === "error") consoleMsgs.push("error:" + msg.text());
});
page.on("pageerror", (err) => consoleMsgs.push("pageerror:" + err.message));

const failedReqs = [];
page.on("requestfailed", (req) => {
  const u = req.url();
  if (/manual-process/i.test(u))
    failedReqs.push({ url: u, err: req.failure()?.errorText });
});

const assetStatuses = {};
page.on("response", (res) => {
  const u = res.url();
  if (/old-home-manual-process/i.test(u)) {
    assetStatuses[u.split("/").pop()] = res.status();
  }
});

await page.goto(URL, { waitUntil: "domcontentloaded", timeout: 90000 });
await waitForSection(page);

const heights = {};
const perWidth = {};

for (const w of WIDTHS) {
  const h = w <= 768 ? Math.max(2800, 2200) : 1600;
  await page.setViewport({ width: w, height: h, deviceScaleFactor: 1 });
  await waitForSection(page);
  const m = await measure(page);
  heights[w] = m.sectionH;
  perWidth[w] = m;
  await shot(page, "#dealality-manual-process", `full-${w}.png`);
  if (w === 1440) {
    await shot(page, ".dmp-problems", "cards-desktop.png");
    // context: end of about + next section start
    await page.evaluate(() => {
      const about = document.querySelector("#about");
      about?.scrollIntoView({ block: "end" });
    });
    await new Promise((r) => setTimeout(r, 250));
    await page.screenshot({
      path: path.join(OUT, "context-after-about.png"),
      clip: { x: 0, y: 0, width: 1440, height: 900 },
    });
    await page.evaluate(() => {
      document.querySelector("#many-futures")?.scrollIntoView({ block: "start" });
    });
    await new Promise((r) => setTimeout(r, 250));
    await page.screenshot({
      path: path.join(OUT, "many-futures.png"),
      clip: { x: 0, y: 0, width: 1440, height: 900 },
    });
  }
  if (w === 390) {
    await shot(page, ".dmp-problems", "cards-mobile.png");
  }
}

// production safety spot-check
const prod = await browser.newPage();
await prod.setCacheEnabled(false);
await prod.goto("https://www.dealality.com/old-home?nocache=" + Date.now(), {
  waitUntil: "domcontentloaded",
  timeout: 90000,
});
const prodCheck = await prod.evaluate(() => {
  const about = document.querySelector("#about");
  return {
    dataOh: about?.getAttribute("data-oh-problem"),
    hasManual: !!document.querySelector("#dealality-manual-process, #oh-manual-process-embed"),
    hasDealDesk: /deal-desk|oh-deal-desk|dealality-problem-desk/i.test(
      document.documentElement.innerHTML
    ),
    rootPathCheck: location.pathname,
  };
});
await prod.close();

const heightTable = WIDTHS.map((w) => {
  const exp = BASELINE[w];
  const got = heights[w];
  return {
    width: w,
    expected: exp.expected,
    measured: got,
    deltaVsExpected: got - exp.expected,
    priorApproved: exp.prior,
    plannedDelta: exp.delta,
  };
});

const report = {
  stagingUrl: URL.split("?")[0],
  publishObservedAt: "2026-07-31T17:56:47.825Z",
  publishPayloadInferred: {
    site_id: "68108c29063eeb5d1bd7ae4a",
    publishToWebflowSubdomain: true,
    customDomains: [],
  },
  titlesOk:
    JSON.stringify(perWidth[1440].titles) === JSON.stringify(EXPECTED_TITLES),
  heights: heightTable,
  sample1440: perWidth[1440],
  sample390: perWidth[390],
  assetStatuses,
  failedManualProcessReqs: failedReqs,
  consoleErrorsRelated: consoleMsgs.filter((m) =>
    /manual-process|dmp-|dealality-manual/i.test(m)
  ),
  consoleErrorsAll: consoleMsgs.slice(0, 30),
  production: prodCheck,
  customDomainsLastPublished: {
    "dealality.com": "2026-07-31T15:18:27.790Z",
    "www.dealality.com": "2026-07-31T15:18:27.790Z",
  },
};

fs.writeFileSync(path.join(OUT, "qa-report.json"), JSON.stringify(report, null, 2));
console.log(JSON.stringify(report, null, 2));
await browser.close();
