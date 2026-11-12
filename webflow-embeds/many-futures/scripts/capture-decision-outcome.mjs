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
const outDir = "/opt/cursor/artifacts/many-futures/decision-outcome";
const repoOut = join(root, "visual-review/decision-outcome");
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
  bottomOutcomeRemoved: true,
  noDuplicateOutcomes: true,
  hotelQuestionHeightAligned: null,
  outcomeFits: [],
  shortened: [
    {
      id: "affiliation",
      note: "Evaluate→Determine; costs→fees; then tightened to “affiliation’s … value justifies …” for 320px wrap",
    },
    {
      id: "residences",
      note: "as part of→within",
    },
    {
      id: "confidential",
      note: "retaining control of participation, information sharing, and process timing",
    },
    {
      id: "market",
      note: "reframed around evolving market/brand/competitive context",
    },
    {
      id: "actions",
      note: "next steps→next actions; opportunities→the process",
    },
    {
      id: "proposals",
      note: "dropped owner from value proposition",
    },
    {
      id: "clarify",
      note: "potential decision risks→decision risks",
    },
  ],
  clickOnly: null,
  panels: {},
};

async function measure(page) {
  return page.evaluate(() => {
    const root = document.getElementById("dealality-many-futures");
    const hotel = root.querySelector(".mf-hotel");
    const q = root.querySelector(".mf-question-list, .mf-questions");
    const active = root.querySelector(".mf-panel.is-active");
    const outcomes = [
      ...active.querySelectorAll(":scope > .mf-decision-outcome, :scope > .mf-outcome"),
    ];
    const labels = outcomes.map((o) => o.querySelector(".mf-outcome-label")?.textContent?.trim());
    const texts = outcomes.map((o) => o.querySelector(".mf-outcome-text")?.textContent?.trim());
    const orderOk =
      active.children[0]?.classList.contains("mf-decision") &&
      (active.children[1]?.classList.contains("mf-decision-outcome") ||
        active.children[1]?.classList.contains("mf-outcome")) &&
      active.children[2]?.classList.contains("mf-features");
    const outcomeAfterFeatures = !!active?.querySelector(
      ".mf-features ~ .mf-outcome, .mf-features ~ .mf-decision-outcome"
    );
    const outcomeEl = outcomes[0];
    const outRect = outcomeEl?.getBoundingClientRect();
    const textEl = outcomeEl?.querySelector(".mf-outcome-text");
    const lineH = textEl ? parseFloat(getComputedStyle(textEl).lineHeight) : 0;
    const textH = textEl?.getBoundingClientRect().height || 0;
    const lines = lineH ? Math.ceil(textH / lineH - 0.05) : 0;
    const benefit = active?.querySelector(".mf-feat-purpose + .mf-feat-kicker");
    const purpose = active?.querySelector(".mf-feat-kicker");
    return {
      hotelH: Math.round(hotel.getBoundingClientRect().height),
      qH: Math.round(q.getBoundingClientRect().height),
      outcomeCount: outcomes.length,
      labels,
      texts,
      orderOk,
      outcomeAfterFeatures,
      outcomeLines: lines,
      outcomeText: texts[0],
      benefitColor: benefit ? getComputedStyle(benefit).color : null,
      purposeColor: purpose ? getComputedStyle(purpose).color : null,
      outcomeLabelColor: outcomeEl
        ? getComputedStyle(outcomeEl.querySelector(".mf-outcome-label")).color
        : null,
      outTop: outRect ? Math.round(outRect.top) : null,
    };
  });
}

// Desktop
const desk = await browser.newPage({ viewport: { width: 1440, height: 900 } });
await desk.goto(`http://127.0.0.1:${port}/preview.html`, { waitUntil: "networkidle" });
await desk.waitForSelector("#dealality-many-futures.mf-js-ready");

// Confirm click vs hover
await desk.locator('.mf-q[data-q="actions"]').hover();
await desk.waitForTimeout(150);
await desk.locator(".mf-workspace").hover({ position: { x: 40, y: 40 } });
const afterHover = await desk.evaluate(
  () => document.querySelector(".mf-q.is-active")?.dataset.q
);
await desk.locator('.mf-q[data-q="actions"]').click();
await desk.locator(".mf-workspace").hover({ position: { x: 40, y: 40 } });
const afterClick = await desk.evaluate(
  () => document.querySelector(".mf-q.is-active")?.dataset.q
);
report.clickOnly = { afterHoverOnly: afterHover, afterClickStay: afterClick };

const deskShots = [];
for (let i = 0; i < IDS.length; i++) {
  const id = IDS[i];
  await desk.locator(`.mf-q[data-q="${id}"]`).click();
  await desk.waitForTimeout(200);
  const m = await measure(desk);
  report.panels[id] = m;
  if (m.outcomeCount !== 1 || m.outcomeAfterFeatures || !m.orderOk) {
    report.noDuplicateOutcomes = false;
    report.bottomOutcomeRemoved = false;
  }
  report.outcomeFits.push({
    id,
    viewport: "desktop",
    lines: m.outcomeLines,
    words: m.outcomeText?.split(/\s+/).length,
  });
  if (i === 0) {
    report.hotelQuestionHeightAligned = {
      hotelH: m.hotelH,
      qH: m.qH,
      delta: Math.abs(m.hotelH - m.qH),
    };
  }
  const rootEl = await desk.$("#dealality-many-futures");
  const name = `desktop-q${String(i + 1).padStart(2, "0")}-${id}.png`;
  await rootEl.screenshot({ path: join(outDir, name) });
  deskShots.push(join(outDir, name));
}

// Featured desktop shots
for (const [id, file] of [
  ["rebrand", "01-desktop-q01-rebrand.png"],
  ["confidential", "02-desktop-q05-confidential.png"],
  ["actions", "03-desktop-q07-actions.png"],
  ["proposals", "04-desktop-q08-proposals.png"],
]) {
  await desk.locator(`.mf-q[data-q="${id}"]`).click();
  await desk.waitForTimeout(200);
  const rootEl = await desk.$("#dealality-many-futures");
  await rootEl.screenshot({ path: join(outDir, file) });
}

// Close-ups
await desk.locator('.mf-q[data-q="rebrand"]').click();
await desk.waitForTimeout(200);
const decisionShot = await desk.evaluate(() => {
  const active = document.querySelector(".mf-panel.is-active");
  const d = active.querySelector(".mf-decision").getBoundingClientRect();
  const o = active.querySelector(".mf-decision-outcome").getBoundingClientRect();
  const root = document.getElementById("dealality-many-futures").getBoundingClientRect();
  return {
    x: Math.min(d.x, o.x) - root.x - 4,
    y: Math.min(d.y, o.y) - root.y - 4,
    width: Math.max(d.right, o.right) - Math.min(d.x, o.x) + 8,
    height: Math.max(d.bottom, o.bottom) - Math.min(d.y, o.y) + 8,
  };
});
const rootElClose = await desk.$("#dealality-many-futures");
await rootElClose.screenshot({
  path: join(outDir, "09-closeup-decision-outcome.png"),
  clip: decisionShot,
});

await desk.locator('.mf-q[data-q="rebrand"]').click();
const copyBlock = await desk.$(".mf-panel.is-active .mf-feat--primary .mf-feat-copy");
await copyBlock.screenshot({ path: join(outDir, "10-closeup-purpose-vs-benefit.png") });

await desk.close();

// Mobile 390
const mob = await browser.newPage({ viewport: { width: 390, height: 844 } });
await mob.goto(`http://127.0.0.1:${port}/preview.html`, { waitUntil: "networkidle" });
await mob.waitForSelector("#dealality-many-futures.mf-js-ready");
const mobShots = [];
for (let i = 0; i < IDS.length; i++) {
  const id = IDS[i];
  await mob.locator(`.mf-q[data-q="${id}"]`).click();
  await mob.waitForTimeout(220);
  const m = await measure(mob);
  report.outcomeFits.push({
    id,
    viewport: "mobile390",
    lines: m.outcomeLines,
    words: m.outcomeText?.split(/\s+/).length,
  });
  if (m.outcomeCount !== 1 || m.outcomeAfterFeatures) {
    report.noDuplicateOutcomes = false;
    report.bottomOutcomeRemoved = false;
  }
  const rootEl = await mob.$("#dealality-many-futures");
  const name = `mobile-q${String(i + 1).padStart(2, "0")}-${id}.png`;
  await rootEl.screenshot({ path: join(outDir, name) });
  mobShots.push(join(outDir, name));
}
for (const [id, file] of [
  ["rebrand", "05-mobile-q01-rebrand.png"],
  ["confidential", "06-mobile-q05-confidential.png"],
  ["actions", "07-mobile-q07-actions.png"],
  ["proposals", "08-mobile-q08-proposals.png"],
]) {
  await mob.locator(`.mf-q[data-q="${id}"]`).click();
  await mob.waitForTimeout(200);
  const rootEl = await mob.$("#dealality-many-futures");
  await rootEl.screenshot({ path: join(outDir, file) });
}
await mob.close();

// Mobile 320 fit check
const mob320 = await browser.newPage({ viewport: { width: 320, height: 720 } });
await mob320.goto(`http://127.0.0.1:${port}/preview.html`, { waitUntil: "networkidle" });
await mob320.waitForSelector("#dealality-many-futures.mf-js-ready");
for (const id of IDS) {
  await mob320.locator(`.mf-q[data-q="${id}"]`).click();
  await mob320.waitForTimeout(150);
  const m = await measure(mob320);
  report.outcomeFits.push({
    id,
    viewport: "mobile320",
    lines: m.outcomeLines,
    words: m.outcomeText?.split(/\s+/).length,
  });
}
await mob320.close();

async function contactSheet(paths, outPath, cols, cellW, cellH) {
  const rows = Math.ceil(paths.length / cols);
  const composites = [];
  for (let i = 0; i < paths.length; i++) {
    const buf = await sharp(paths[i])
      .resize(cellW, cellH, { fit: "contain", background: "#080f25" })
      .png()
      .toBuffer();
    composites.push({
      input: buf,
      left: (i % cols) * cellW,
      top: Math.floor(i / cols) * cellH,
    });
  }
  await sharp({
    create: {
      width: cols * cellW,
      height: rows * cellH,
      channels: 3,
      background: "#080f25",
    },
  })
    .composite(composites)
    .png()
    .toFile(outPath);
}

await contactSheet(
  deskShots,
  join(outDir, "11-desktop-nine-state-contact.png"),
  3,
  640,
  420
);
await contactSheet(
  mobShots,
  join(outDir, "12-mobile-nine-state-contact.png"),
  3,
  390,
  700
);

writeFileSync(join(outDir, "qa-report.json"), JSON.stringify(report, null, 2));

const files = [
  "01-desktop-q01-rebrand.png",
  "02-desktop-q05-confidential.png",
  "03-desktop-q07-actions.png",
  "04-desktop-q08-proposals.png",
  "05-mobile-q01-rebrand.png",
  "06-mobile-q05-confidential.png",
  "07-mobile-q07-actions.png",
  "08-mobile-q08-proposals.png",
  "09-closeup-decision-outcome.png",
  "10-closeup-purpose-vs-benefit.png",
  "11-desktop-nine-state-contact.png",
  "12-mobile-nine-state-contact.png",
  "qa-report.json",
];
for (const f of files) copyFileSync(join(outDir, f), join(repoOut, f));

console.log(JSON.stringify(report, null, 2));
await browser.close();
server.close();
console.log("ok", outDir);
