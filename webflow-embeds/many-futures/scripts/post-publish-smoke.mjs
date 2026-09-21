/**
 * Post-publish smoke test against live Old Home.
 * No design changes.
 */
import { chromium } from "playwright";
import { mkdirSync, writeFileSync } from "fs";
import { join } from "path";

const outDir = "/opt/cursor/artifacts/many-futures/post-publish-smoke";
mkdirSync(outDir, { recursive: true });

const LIVE = "https://www.dealality.com/old-home";
const APPROVED_CSS =
  "https://cdn.jsdelivr.net/gh/joandejarden-create/my-operators-backend@6e5ea99e0c868c238e1f8966fa401b272d6ccfb8/webflow-embeds/many-futures/dist/many-futures.7b38cc86f994.css";
const APPROVED_JS =
  "https://cdn.jsdelivr.net/gh/joandejarden-create/my-operators-backend@6e5ea99e0c868c238e1f8966fa401b272d6ccfb8/webflow-embeds/many-futures/dist/many-futures.cf482eb7cce1.js";

const IDS = [
  "rebrand",
  "operators",
  "affiliation",
  "residences",
  "confidential",
  "market",
  "actions",
  "proposals",
  "clarify",
];

const report = {
  url: LIVE,
  publishedAt: new Date().toISOString(),
  css: APPROVED_CSS,
  js: APPROVED_JS,
  pinPresent: false,
  assetsOk: true,
  failedAssets: [],
  consoleErrors: [],
  mfConsoleErrors: [],
  nineOk: false,
  platformFeatures: null,
  overflow: {},
  screenshots: {},
  discrepancy: null,
};

const browser = await chromium.launch({
  executablePath: "/usr/local/bin/google-chrome",
  args: ["--no-sandbox", "--disable-dev-shm-usage"],
});

const page = await browser.newPage({ viewport: { width: 1440, height: 2200 } });
page.on("console", (msg) => {
  if (msg.type() === "error") {
    const t = msg.text();
    report.consoleErrors.push(t);
    if (/many-futures|jsdelivr.*many-futures|6e5ea99/i.test(t)) {
      report.mfConsoleErrors.push(t);
    }
  }
});
page.on("pageerror", (err) => {
  report.consoleErrors.push(`pageerror: ${err.message}`);
  if (/many-futures|dealality-many-futures/i.test(err.message)) {
    report.mfConsoleErrors.push(err.message);
  }
});
page.on("response", (res) => {
  const url = res.url();
  if (res.status() >= 400 && /many-futures|6e5ea99/i.test(url)) {
    report.assetsOk = false;
    report.failedAssets.push({ status: res.status(), url });
  }
});

await page.goto(LIVE, { waitUntil: "networkidle", timeout: 90000 });
await page.waitForSelector("#dealality-many-futures", { timeout: 45000 });
await page.waitForSelector(".mf-q.is-active", { timeout: 30000 });
await page.evaluate(async () => {
  await document.fonts.ready;
  await Promise.all(
    [...document.images].map((i) =>
      i.complete
        ? 1
        : new Promise((r) => {
            i.onload = i.onerror = r;
          })
    )
  );
});
await page.waitForTimeout(500);

report.pinPresent = await page.evaluate((css) => {
  const links = [...document.querySelectorAll('link[rel="stylesheet"]')].map(
    (l) => l.href
  );
  const scripts = [...document.querySelectorAll("script[src]")].map((s) => s.src);
  return {
    css: links.some((h) => h.includes("many-futures.7b38cc86f994.css")),
    js: scripts.some((h) => h.includes("many-futures.cf482eb7cce1.js")),
    sha: document.documentElement.outerHTML.includes(
      "6e5ea99e0c868c238e1f8966fa401b272d6ccfb8"
    ),
    cssHref: links.find((h) => h.includes("many-futures")),
    jsSrc: scripts.find((h) => h.includes("many-futures")),
  };
}, APPROVED_CSS);

report.platformFeatures = await page.evaluate(() => {
  const el = document.getElementById("platform-features");
  return {
    present: !!el,
    h2: document.getElementById("platform-features-h2")?.textContent?.trim(),
    unchangedMarker: !!document.getElementById("platform-features-inner"),
  };
});

async function measure(id) {
  await page.click(`.mf-q[data-q="${id}"]`);
  await page.waitForTimeout(220);
  return page.evaluate((panelId) => {
    const root = document.getElementById("dealality-many-futures");
    const panel = root.querySelector(`.mf-panel[data-panel="${panelId}"]`);
    const outcomes = panel.querySelectorAll(".mf-decision-outcome, .mf-outcome");
    const order = [];
    for (const child of panel.children) {
      if (child.classList.contains("mf-decision")) order.push("decision");
      else if (child.classList.contains("mf-capabilities"))
        order.push("capabilities");
      else if (
        child.classList.contains("mf-decision-outcome") ||
        child.classList.contains("mf-outcome")
      )
        order.push("outcome");
    }
    const hotel = root.querySelector(".mf-hotel");
    const questions = root.querySelector(".mf-questions");
    const libHeadings = [...panel.querySelectorAll(".mf-ui-lib-heading")].map(
      (el) => el.textContent.trim()
    );
    return {
      active: panel.classList.contains("is-active"),
      outcomeCount: outcomes.length,
      order,
      hotelH: hotel ? Math.round(hotel.getBoundingClientRect().height) : null,
      questionsH: questions
        ? Math.round(questions.getBoundingClientRect().height)
        : null,
      decisionTitle: panel
        .querySelector(".mf-decision-title")
        ?.textContent.trim(),
      libHeadings,
      overflowX: document.documentElement.scrollWidth > window.innerWidth + 1,
    };
  }, id);
}

const states = {};
for (const id of IDS) {
  states[id] = await measure(id);
}
report.states = states;
report.nineOk = IDS.every(
  (id) =>
    states[id].active &&
    states[id].outcomeCount === 1 &&
    states[id].order.join(",") === "decision,capabilities,outcome"
);

// Desktop screenshots Q01 default + Q05
await page.click('.mf-q[data-q="rebrand"]');
await page.waitForTimeout(200);
await page.locator("#dealality-many-futures").scrollIntoViewIfNeeded();
await page.locator("#dealality-many-futures").screenshot({
  path: join(outDir, "live-desktop-q01.png"),
});
report.screenshots.desktop = join(outDir, "live-desktop-q01.png");

await page.click('.mf-q[data-q="confidential"]');
await page.waitForTimeout(200);
await page.locator("#dealality-many-futures").screenshot({
  path: join(outDir, "live-desktop-q05.png"),
});

await page.click('.mf-q[data-q="actions"]');
await page.waitForTimeout(200);
await page.locator("#dealality-many-futures").screenshot({
  path: join(outDir, "live-desktop-q07.png"),
});

await page.click('.mf-q[data-q="clarify"]');
await page.waitForTimeout(200);
await page.locator("#dealality-many-futures").screenshot({
  path: join(outDir, "live-desktop-q09.png"),
});

// Mobile
const mobile = await browser.newPage({
  viewport: { width: 390, height: 1800 },
});
const mobErrs = [];
mobile.on("console", (msg) => {
  if (msg.type() === "error") mobErrs.push(msg.text());
});
await mobile.goto(LIVE, { waitUntil: "networkidle", timeout: 90000 });
await mobile.waitForSelector("#dealality-many-futures", { timeout: 45000 });
await mobile.waitForTimeout(400);
await mobile.click('.mf-q[data-q="confidential"]');
await mobile.waitForTimeout(200);
await mobile.locator("#dealality-many-futures").screenshot({
  path: join(outDir, "live-mobile-q05.png"),
});
await mobile.click('.mf-q[data-q="clarify"]');
await mobile.waitForTimeout(200);
await mobile.locator("#dealality-many-futures").screenshot({
  path: join(outDir, "live-mobile-q09.png"),
});
report.screenshots.mobile = join(outDir, "live-mobile-q05.png");
report.overflow.mobile = await mobile.evaluate(
  () => document.documentElement.scrollWidth > window.innerWidth + 1
);
report.consoleErrors.push(...mobErrs.map((e) => `mobile: ${e}`));
await mobile.close();

report.overflow.desktop = states.rebrand.overflowX;
report.pass =
  report.pinPresent.sha &&
  report.nineOk &&
  report.assetsOk &&
  report.mfConsoleErrors.length === 0 &&
  report.platformFeatures?.present &&
  !report.overflow.desktop &&
  !report.overflow.mobile;

writeFileSync(join(outDir, "smoke.json"), JSON.stringify(report, null, 2));
console.log(
  JSON.stringify(
    {
      pass: report.pass,
      publishedAt: report.publishedAt,
      pinPresent: report.pinPresent,
      nineOk: report.nineOk,
      assetsOk: report.assetsOk,
      failedAssets: report.failedAssets,
      mfConsoleErrors: report.mfConsoleErrors,
      consoleErrors: report.consoleErrors,
      platformFeatures: report.platformFeatures,
      hotelH: states.rebrand.hotelH,
      questionsH: states.rebrand.questionsH,
      q05Title: states.confidential.decisionTitle,
      q09Libs: states.clarify.libHeadings,
      overflow: report.overflow,
    },
    null,
    2
  )
);

await browser.close();
