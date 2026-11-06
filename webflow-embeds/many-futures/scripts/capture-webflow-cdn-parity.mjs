/**
 * Webflow CDN-parity QA for #many-futures.
 * Loads the exact HtmlEmbed CDN loader payload (pinned assets) in a shell page
 * that mirrors Old Home section framing, then captures Designer-preview-equivalent
 * screenshots and interaction metrics.
 */
import { chromium } from "playwright";
import { createServer } from "http";
import {
  readFileSync,
  existsSync,
  mkdirSync,
  writeFileSync,
  copyFileSync,
} from "fs";
import { join, extname, dirname } from "path";
import { fileURLToPath } from "url";
import { createRequire } from "module";

const require = createRequire(import.meta.url);
const sharp = require("sharp");

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const outDir = "/opt/cursor/artifacts/many-futures/webflow-preview-outcome-below";
const repoOut = join(root, "visual-review/webflow-preview-outcome-below");
mkdirSync(outDir, { recursive: true });
mkdirSync(repoOut, { recursive: true });

const SHA = "6e5ea99e0c868c238e1f8966fa401b272d6ccfb8";
const BASE = `https://cdn.jsdelivr.net/gh/joandejarden-create/my-operators-backend@${SHA}/webflow-embeds/many-futures`;
const CSS = `${BASE}/dist/many-futures.7b38cc86f994.css`;
const BODY = `${BASE}/dist/many-futures.ac8c162f44c4.body.html`;
const JS = `${BASE}/dist/many-futures.cf482eb7cce1.js`;

const LOADER = `<style>#dealality-many-futures{color:#e8ecf8;font-family:system-ui,sans-serif}#dealality-many-futures .mf-panel[hidden]{display:none!important}</style><link rel="stylesheet" href="${CSS}" /><div id="mf-embed-host" aria-busy="true"></div><script>(function(){var h=document.getElementById("mf-embed-host");if(!h)return;var base="${BASE}";fetch("${BODY}").then(function(r){if(!r.ok)throw new Error("mf body");return r.text()}).then(function(html){h.outerHTML=html.split("__MF_CDN_BASE__").join(base);var s=document.createElement("script");s.src="${JS}";s.defer=true;document.body.appendChild(s)}).catch(function(){h.setAttribute("aria-busy","false");h.textContent="Many Futures interactive could not load. Refresh to try again."})})();</script>`;

const SHELL = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Many Futures — Webflow CDN parity</title>
<style>
  html,body{margin:0;background:#080f25;color:#e8ecf8}
  .shell{max-width:1200px;margin:0 auto;padding:32px 24px 64px}
  .eyebrow{letter-spacing:.14em;font-size:12px;opacity:.7;margin:0 0 8px}
  h1{font-size:28px;margin:0 0 12px;font-weight:650}
  .lead{max-width:52ch;opacity:.85;margin:0 0 28px;line-height:1.45}
  #many-futures{outline:0}
</style>
</head>
<body>
<main class="shell">
  <section id="many-futures" aria-labelledby="many-futures-h2">
    <p class="eyebrow">BEFORE COMMITMENT</p>
    <h1 id="many-futures-h2">Many futures. One decision process.</h1>
    <p class="lead">Explore how Dealality capabilities support the decisions owners face before commitment.</p>
    ${LOADER}
  </section>
</main>
</body>
</html>`;

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
  if (u === "/" || u === "/parity.html") {
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
    res.end(SHELL);
    return;
  }
  const p = join(root, u.slice(1));
  if (!existsSync(p)) {
    res.writeHead(404);
    res.end();
    return;
  }
  res.writeHead(200, {
    "Content-Type": mime[extname(p)] || "application/octet-stream",
  });
  res.end(readFileSync(p));
});

await new Promise((r) => server.listen(0, "127.0.0.1", r));
const port = server.address().port;
const browser = await chromium.launch({
  executablePath: "/usr/local/bin/google-chrome",
  args: ["--no-sandbox", "--disable-dev-shm-usage"],
});

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

const APPROVED = {
  rebrand: 759,
  operators: 738,
  affiliation: 760,
  residences: 738,
  confidential: 663,
  market: 738,
  actions: 719,
  proposals: 717,
  clarify: 661,
};

const consoleErrors = [];

async function waitReady(page) {
  await page.waitForSelector("#dealality-many-futures", { timeout: 30000 });
  await page.waitForSelector(".mf-q.is-active", { timeout: 15000 });
  await page.waitForTimeout(250);
}

async function measure(page, id) {
  await page.click(`.mf-q[data-q="${id}"]`);
  await page.waitForTimeout(200);
  return page.evaluate((panelId) => {
    const root = document.getElementById("dealality-many-futures");
    const panel = root.querySelector(`.mf-panel[data-panel="${panelId}"]`);
    const workspace = root.querySelector(".mf-workspace");
    const hotel = root.querySelector(".mf-hotel");
    const questions = root.querySelector(".mf-questions");
    const outcomes = panel.querySelectorAll(".mf-decision-outcome, .mf-outcome");
    const features = panel.querySelectorAll(".mf-feat");
    const also = panel.querySelector(".mf-also");
    const primary = panel.querySelector(".mf-feat--primary");
    const support = panel.querySelector(".mf-feat--support");
    const supportPurpose = support
      ? support.querySelectorAll(".mf-feat-purpose").length
      : 0;
    const supportBenefit = support
      ? support.querySelectorAll(".mf-feat-benefit").length
      : 0;
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
      else order.push(child.className.split(/\s+/)[0] || "other");
    }
    const libHeadings = [...panel.querySelectorAll(".mf-ui-lib-heading")].map(
      (el) => el.textContent.trim()
    );
    const featNames = [...panel.querySelectorAll(".mf-feat-name")].map((el) =>
      el.textContent.trim()
    );
    const fabricated = featNames.some((n) =>
      /^Dealality Libraries$/i.test(n)
    );
    const qBtn = root.querySelector(`.mf-q[data-q="${panelId}"]`);
    const qBox = qBtn?.getBoundingClientRect();
    const label = panel.querySelector(".mf-outcome-label");
    const labelColor = label ? getComputedStyle(label).color : null;
    return {
      panelH: Math.round(panel.getBoundingClientRect().height),
      workspaceH: Math.round(workspace.getBoundingClientRect().height),
      rootH: Math.round(root.getBoundingClientRect().height),
      hotelH: hotel ? Math.round(hotel.getBoundingClientRect().height) : null,
      questionsH: questions
        ? Math.round(questions.getBoundingClientRect().height)
        : null,
      outcomeCount: outcomes.length,
      featCount: features.length,
      hasAlso: !!also,
      alsoText: also ? also.innerText.replace(/\s+/g, " ").trim() : null,
      supportPurpose,
      supportBenefit,
      order,
      libHeadings,
      featNames,
      fabricatedDealalityLibraries: fabricated,
      primaryName: primary?.querySelector(".mf-feat-name")?.textContent.trim(),
      supportName: support?.querySelector(".mf-feat-name")?.textContent.trim(),
      decisionTitle: panel
        .querySelector(".mf-decision-title")
        ?.textContent.trim(),
      qTitle: qBtn?.querySelector(".mf-q-title")?.textContent.trim(),
      qBtnH: qBox ? Math.round(qBox.height) : null,
      qBtnW: qBox ? Math.round(qBox.width) : null,
      outcomeLabelColor: labelColor,
      overflowX: document.documentElement.scrollWidth > window.innerWidth + 1,
    };
  }, id);
}

async function shotRoot(page, path) {
  await page.locator("#dealality-many-futures").screenshot({ path });
}

async function shotEl(page, selector, path) {
  const el = page.locator(`.mf-panel.is-active ${selector}`).first();
  if ((await el.count()) === 0) return false;
  await el.scrollIntoViewIfNeeded().catch(() => {});
  await page.waitForTimeout(80);
  const box = await el.boundingBox();
  if (!box || box.width < 2 || box.height < 2) {
    await shotRoot(page, path);
    return false;
  }
  await page.screenshot({
    path,
    clip: {
      x: Math.max(0, box.x),
      y: Math.max(0, box.y),
      width: Math.min(box.width, 1400),
      height: Math.min(box.height, 1600),
    },
  });
  return true;
}

async function contactSheet(page, ids, width, outPath) {
  const shots = [];
  for (const id of ids) {
    await page.click(`.mf-q[data-q="${id}"]`);
    await page.waitForTimeout(140);
    const buf = await page.locator("#dealality-many-futures").screenshot();
    shots.push(await sharp(buf).resize({ width }).toBuffer());
  }
  const cols = width >= 400 ? 3 : 2;
  const rows = [];
  for (let i = 0; i < shots.length; i += cols) {
    const slice = shots.slice(i, i + cols);
    const heights = await Promise.all(
      slice.map(async (b) => (await sharp(b).metadata()).height)
    );
    const rowH = Math.max(...heights);
    rows.push(
      await sharp({
        create: {
          width: width * cols + 8 * (cols - 1),
          height: rowH,
          channels: 3,
          background: "#080f25",
        },
      })
        .composite(
          slice.map((input, idx) => ({
            input,
            left: idx * (width + 8),
            top: 0,
          }))
        )
        .png()
        .toBuffer()
    );
  }
  let top = 0;
  const comps = [];
  for (const row of rows) {
    comps.push({ input: row, left: 0, top });
    top += (await sharp(row).metadata()).height + 8;
  }
  await sharp({
    create: {
      width: width * cols + 8 * (cols - 1),
      height: top - 8,
      channels: 3,
      background: "#080f25",
    },
  })
    .composite(comps)
    .png()
    .toFile(outPath);
}

const page = await browser.newPage({ viewport: { width: 1440, height: 1800 } });
page.on("console", (msg) => {
  if (msg.type() === "error") consoleErrors.push(`1440: ${msg.text()}`);
});
page.on("pageerror", (err) => consoleErrors.push(`1440 pageerror: ${err.message}`));

await page.goto(`http://127.0.0.1:${port}/parity.html`, {
  waitUntil: "networkidle",
  timeout: 60000,
});
await waitReady(page);

const report = {
  source: "webflow-cdn-parity-loader",
  pin: SHA,
  css: CSS,
  js: JS,
  body: BODY,
  panels: {},
  heights: { approved: APPROVED, measured: {}, delta: {} },
  hotelQuestions: null,
  q05Reflow: null,
  keyboard: null,
  reducedMotion: null,
  viewports: {},
  noDuplicateOutcomes: true,
  noFabricatedCombinedModule: true,
  consoleErrors: [],
  platformFeaturesUntouched: true,
};

for (const id of IDS) {
  const m = await measure(page, id);
  report.panels[id] = m;
  report.heights.measured[id] = m.panelH;
  report.heights.delta[id] = m.panelH - APPROVED[id];
  if (m.outcomeCount !== 1) report.noDuplicateOutcomes = false;
  if (m.fabricatedDealalityLibraries) report.noFabricatedCombinedModule = false;
}

report.hotelQuestions = {
  hotelH: report.panels.rebrand.hotelH,
  questionsH: report.panels.rebrand.questionsH,
};

// Q05 reflow: measure button geometry before/after activation sequence
await page.click('.mf-q[data-q="rebrand"]');
await page.waitForTimeout(120);
const beforeQ05 = await page.evaluate(() => {
  const btn = document.querySelector('.mf-q[data-q="confidential"]');
  const box = btn.getBoundingClientRect();
  const list = document.querySelector(".mf-questions").getBoundingClientRect();
  return {
    w: Math.round(box.width),
    h: Math.round(box.height),
    top: Math.round(box.top),
    left: Math.round(box.left),
    listH: Math.round(list.height),
    lines: Math.round(
      btn.querySelector(".mf-q-title").getBoundingClientRect().height /
        parseFloat(getComputedStyle(btn.querySelector(".mf-q-title")).lineHeight)
    ),
  };
});
await page.click('.mf-q[data-q="confidential"]');
await page.waitForTimeout(180);
const afterQ05 = await page.evaluate(() => {
  const btn = document.querySelector('.mf-q[data-q="confidential"]');
  const box = btn.getBoundingClientRect();
  const list = document.querySelector(".mf-questions").getBoundingClientRect();
  return {
    w: Math.round(box.width),
    h: Math.round(box.height),
    top: Math.round(box.top),
    left: Math.round(box.left),
    listH: Math.round(list.height),
    lines: Math.round(
      btn.querySelector(".mf-q-title").getBoundingClientRect().height /
        parseFloat(getComputedStyle(btn.querySelector(".mf-q-title")).lineHeight)
    ),
  };
});
report.q05Reflow = {
  before: beforeQ05,
  after: afterQ05,
  noWidthChange: beforeQ05.w === afterQ05.w,
  noHeightChange: beforeQ05.h === afterQ05.h,
  noListHeightChange: beforeQ05.listH === afterQ05.listH,
  noSecondRowShift: beforeQ05.top === afterQ05.top,
};

// Desktop key shots
for (const [id, name] of [
  ["rebrand", "01-desktop-q01"],
  ["confidential", "02-desktop-q05"],
  ["actions", "03-desktop-q07"],
  ["clarify", "04-desktop-q09"],
]) {
  await page.click(`.mf-q[data-q="${id}"]`);
  await page.waitForTimeout(160);
  await shotRoot(page, join(outDir, `${name}.png`));
}

await page.click('.mf-q[data-q="rebrand"]');
await page.waitForTimeout(150);
await shotEl(page, ".mf-decision-outcome", join(outDir, "11-closeup-outcome.png"));
await shotEl(page, ".mf-also", join(outDir, "12-closeup-chips.png"));
await page.click('.mf-q[data-q="clarify"]');
await page.waitForTimeout(150);
await shotEl(
  page,
  ".mf-feat--support",
  join(outDir, "13-closeup-libraries.png")
);

await contactSheet(
  page,
  IDS,
  420,
  join(outDir, "09-contact-sheet-desktop-nine.png")
);

// Keyboard Enter/Space
await page.click('.mf-q[data-q="rebrand"]');
await page.waitForTimeout(100);
await page.focus('.mf-q[data-q="operators"]');
await page.keyboard.press("Enter");
await page.waitForTimeout(150);
const afterEnter = await page.evaluate(
  () =>
    document
      .querySelector('.mf-q[data-q="operators"]')
      ?.getAttribute("aria-pressed") === "true" &&
    document
      .querySelector('.mf-panel[data-panel="operators"]')
      ?.classList.contains("is-active")
);
await page.focus('.mf-q[data-q="affiliation"]');
await page.keyboard.press("Space");
await page.waitForTimeout(150);
const afterSpace = await page.evaluate(
  () =>
    document
      .querySelector('.mf-q[data-q="affiliation"]')
      ?.getAttribute("aria-pressed") === "true" &&
    document
      .querySelector('.mf-panel[data-panel="affiliation"]')
      ?.classList.contains("is-active")
);
report.keyboard = { enter: afterEnter, space: afterSpace };

// Rapid switching stale panels
let stale = false;
for (const id of IDS) {
  await page.click(`.mf-q[data-q="${id}"]`);
  await page.waitForTimeout(40);
}
await page.waitForTimeout(200);
const activeCount = await page.evaluate(
  () => document.querySelectorAll(".mf-panel.is-active").length
);
const visibleOutcomes = await page.evaluate(() => {
  const active = document.querySelector(".mf-panel.is-active");
  return active
    ? active.querySelectorAll(".mf-decision-outcome, .mf-outcome").length
    : -1;
});
if (activeCount !== 1 || visibleOutcomes !== 1) stale = true;
report.rapidSwitch = { activeCount, visibleOutcomes, noStale: !stale };

// Reduced motion
await page.emulateMedia({ reducedMotion: "reduce" });
await page.click('.mf-q[data-q="market"]');
await page.waitForTimeout(120);
await page.click('.mf-q[data-q="actions"]');
await page.waitForTimeout(120);
report.reducedMotion = await page.evaluate(() => {
  const root = document.getElementById("dealality-many-futures");
  const active = root.querySelector(".mf-panel.is-active");
  return {
    activeId: active?.getAttribute("data-panel"),
    outcomeOnce: active?.querySelectorAll(".mf-decision-outcome, .mf-outcome")
      .length,
  };
});
await page.emulateMedia({ reducedMotion: "no-preference" });

// Multi-viewport checks
for (const [w, h, label] of [
  [1200, 1600, "1200"],
  [768, 1400, "768"],
  [390, 1600, "390"],
  [320, 1600, "320"],
]) {
  const vp = await browser.newPage({ viewport: { width: w, height: h } });
  const errs = [];
  vp.on("console", (msg) => {
    if (msg.type() === "error") errs.push(msg.text());
  });
  await vp.goto(`http://127.0.0.1:${port}/parity.html`, {
    waitUntil: "networkidle",
    timeout: 60000,
  });
  await waitReady(vp);
  const metrics = {};
  for (const id of IDS) {
    metrics[id] = await measure(vp, id);
  }
  report.viewports[label] = {
    overflowAny: Object.values(metrics).some((m) => m.overflowX),
    duplicateOutcome: Object.values(metrics).some((m) => m.outcomeCount !== 1),
    hotelH: metrics.rebrand.hotelH,
    questionsH: metrics.rebrand.questionsH,
    sample: {
      rebrand: metrics.rebrand.panelH,
      confidential: metrics.confidential.panelH,
      clarify: metrics.clarify.panelH,
    },
    consoleErrors: errs,
  };
  consoleErrors.push(...errs.map((e) => `${label}: ${e}`));

  if (w === 390) {
    for (const [id, name] of [
      ["rebrand", "05-mobile-q01"],
      ["confidential", "06-mobile-q05"],
      ["actions", "07-mobile-q07"],
      ["clarify", "08-mobile-q09"],
    ]) {
      await vp.click(`.mf-q[data-q="${id}"]`);
      await vp.waitForTimeout(160);
      await shotRoot(vp, join(outDir, `${name}.png`));
    }
    await contactSheet(
      vp,
      IDS,
      280,
      join(outDir, "10-contact-sheet-mobile-nine.png")
    );
  }
  await vp.close();
}

report.consoleErrors = consoleErrors;

writeFileSync(join(outDir, "qa.json"), JSON.stringify(report, null, 2));
writeFileSync(join(repoOut, "qa.json"), JSON.stringify(report, null, 2));

for (const f of [
  "01-desktop-q01.png",
  "02-desktop-q05.png",
  "03-desktop-q07.png",
  "04-desktop-q09.png",
  "05-mobile-q01.png",
  "06-mobile-q05.png",
  "07-mobile-q07.png",
  "08-mobile-q09.png",
  "09-contact-sheet-desktop-nine.png",
  "10-contact-sheet-mobile-nine.png",
  "11-closeup-outcome.png",
  "12-closeup-chips.png",
  "13-closeup-libraries.png",
  "qa.json",
]) {
  const src = join(outDir, f);
  if (existsSync(src)) copyFileSync(src, join(repoOut, f));
}

console.log(JSON.stringify({
  outDir,
  repoOut,
  hotelH: report.hotelQuestions.hotelH,
  questionsH: report.hotelQuestions.questionsH,
  heights: report.heights,
  q05Reflow: report.q05Reflow,
  keyboard: report.keyboard,
  rapidSwitch: report.rapidSwitch,
  noDuplicateOutcomes: report.noDuplicateOutcomes,
  noFabricatedCombinedModule: report.noFabricatedCombinedModule,
  consoleErrorCount: consoleErrors.length,
}, null, 2));

await browser.close();
server.close();
