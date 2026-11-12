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
const outDir = "/opt/cursor/artifacts/many-futures/outcome-below-features";
const repoOut = join(root, "visual-review/outcome-below-features");
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

const BEFORE = {
  rebrand: 796,
  operators: 717,
  affiliation: 775,
  residences: 876,
  confidential: 777,
  market: 775,
  actions: 756,
  proposals: 754,
  clarify: 775,
};

async function shotEl(page, selector, path) {
  const el = page.locator(`.mf-panel.is-active ${selector}`).first();
  if ((await el.count()) === 0) return false;
  await el.scrollIntoViewIfNeeded().catch(() => {});
  await page.waitForTimeout(80);
  const box = await el.boundingBox();
  if (!box || box.width < 2 || box.height < 2) {
    // Fallback: full root crop if element reports no box
    await page.locator("#dealality-many-futures").screenshot({ path });
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

async function measure(page, id) {
  await page.click(`.mf-q[data-q="${id}"]`);
  await page.waitForTimeout(180);
  return page.evaluate((panelId) => {
    const root = document.getElementById("dealality-many-futures");
    const panel = root.querySelector(`.mf-panel[data-panel="${panelId}"]`);
    const workspace = root.querySelector(".mf-workspace");
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
      else if (child.classList.contains("mf-capabilities")) order.push("capabilities");
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
    const fabricated = [...panel.querySelectorAll(".mf-feat-name")].some((el) =>
      /^Dealality Libraries$/i.test(el.textContent.trim())
    );
    return {
      panelH: Math.round(panel.getBoundingClientRect().height),
      workspaceH: Math.round(workspace.getBoundingClientRect().height),
      rootH: Math.round(root.getBoundingClientRect().height),
      outcomeCount: outcomes.length,
      featCount: features.length,
      hasAlso: !!also,
      alsoText: also ? also.innerText.replace(/\s+/g, " ").trim() : null,
      supportPurpose,
      supportBenefit,
      order,
      libHeadings,
      fabricatedDealalityLibraries: fabricated,
      primaryName: primary?.querySelector(".mf-feat-name")?.textContent.trim(),
      supportName: support?.querySelector(".mf-feat-name")?.textContent.trim(),
      decisionTitle: panel
        .querySelector(".mf-decision-title")
        ?.textContent.trim(),
      qTitle: root
        .querySelector(`.mf-q[data-q="${panelId}"] .mf-q-title`)
        ?.textContent.trim(),
    };
  }, id);
}

const page = await browser.newPage({ viewport: { width: 1440, height: 1600 } });
await page.goto(`http://127.0.0.1:${port}/preview.html`, {
  waitUntil: "networkidle",
});
await page.waitForSelector("#dealality-many-futures");

const report = {
  panels: {},
  heights: { before: BEFORE, after: {}, delta: {} },
  noDuplicateOutcomes: true,
  noFabricatedCombinedModule: true,
  clickOnly: null,
};

for (const id of IDS) {
  const m = await measure(page, id);
  report.panels[id] = m;
  report.heights.after[id] = m.panelH;
  report.heights.delta[id] = m.panelH - BEFORE[id];
  if (m.outcomeCount !== 1) report.noDuplicateOutcomes = false;
  if (m.fabricatedDealalityLibraries) report.noFabricatedCombinedModule = false;
}

// Desktop key states
for (const id of ["rebrand", "confidential", "actions", "proposals", "clarify"]) {
  await page.click(`.mf-q[data-q="${id}"]`);
  await page.waitForTimeout(150);
  await page.locator("#dealality-many-futures").screenshot({
    path: join(outDir, `01-desktop-${id}.png`),
  });
}

// Close-ups on rebrand
await page.click('.mf-q[data-q="rebrand"]');
await page.waitForTimeout(150);
await shotEl(page, ".mf-feat--primary", join(outDir, "03-closeup-primary.png"));
await shotEl(page, ".mf-feat--support", join(outDir, "04-closeup-support.png"));
await shotEl(page, ".mf-also", join(outDir, "05-closeup-chips.png"));
await shotEl(
  page,
  ".mf-decision-outcome",
  join(outDir, "07-closeup-outcome-below.png")
);

// Libraries close-up
await page.click('.mf-q[data-q="clarify"]');
await page.waitForTimeout(150);
await shotEl(
  page,
  ".mf-feat--support",
  join(outDir, "06-closeup-libraries-grouped.png")
);

// Contact sheet desktop
const deskShots = [];
for (const id of IDS) {
  await page.click(`.mf-q[data-q="${id}"]`);
  await page.waitForTimeout(120);
  const buf = await page.locator("#dealality-many-futures").screenshot();
  deskShots.push(await sharp(buf).resize({ width: 420 }).toBuffer());
}
const deskRows = [];
for (let i = 0; i < 9; i += 3) {
  deskRows.push(
    await sharp({
      create: {
        width: 420 * 3 + 16,
        height: Math.max(
          ...(await Promise.all(
            deskShots.slice(i, i + 3).map(async (b) => (await sharp(b).metadata()).height)
          ))
        ),
        channels: 3,
        background: "#080f25",
      },
    })
      .composite(
        deskShots.slice(i, i + 3).map((input, idx) => ({
          input,
          left: idx * (420 + 8),
          top: 0,
        }))
      )
      .png()
      .toBuffer()
  );
}
const deskH = (
  await Promise.all(deskRows.map(async (b) => (await sharp(b).metadata()).height))
).reduce((a, b) => a + b + 8, -8);
await sharp({
  create: {
    width: 420 * 3 + 16,
    height: deskH,
    channels: 3,
    background: "#080f25",
  },
})
  .composite(
    await (async () => {
      let top = 0;
      const comps = [];
      for (const row of deskRows) {
        comps.push({ input: row, left: 0, top });
        top += (await sharp(row).metadata()).height + 8;
      }
      return comps;
    })()
  )
  .png()
  .toFile(join(outDir, "08-contact-sheet-desktop-nine.png"));

// Mobile
const mobile = await browser.newPage({
  viewport: { width: 390, height: 1400 },
});
await mobile.goto(`http://127.0.0.1:${port}/preview.html`, {
  waitUntil: "networkidle",
});
await mobile.waitForSelector("#dealality-many-futures");

for (const id of ["rebrand", "confidential", "actions", "proposals", "clarify"]) {
  await mobile.click(`.mf-q[data-q="${id}"]`);
  await mobile.waitForTimeout(150);
  await mobile.locator("#dealality-many-futures").screenshot({
    path: join(outDir, `02-mobile-${id}.png`),
    fullPage: false,
  });
}

const mobShots = [];
for (const id of IDS) {
  await mobile.click(`.mf-q[data-q="${id}"]`);
  await mobile.waitForTimeout(120);
  const buf = await mobile.locator("#dealality-many-futures").screenshot();
  mobShots.push(await sharp(buf).resize({ width: 260 }).toBuffer());
}
const mobRows = [];
for (let i = 0; i < 9; i += 3) {
  const heights = await Promise.all(
    mobShots.slice(i, i + 3).map(async (b) => (await sharp(b).metadata()).height)
  );
  const h = Math.max(...heights);
  mobRows.push(
    await sharp({
      create: {
        width: 260 * 3 + 16,
        height: h,
        channels: 3,
        background: "#080f25",
      },
    })
      .composite(
        mobShots.slice(i, i + 3).map((input, idx) => ({
          input,
          left: idx * (260 + 8),
          top: 0,
        }))
      )
      .png()
      .toBuffer()
  );
}
const mobTotalH = (
  await Promise.all(mobRows.map(async (b) => (await sharp(b).metadata()).height))
).reduce((a, b) => a + b + 8, -8);
await sharp({
  create: {
    width: 260 * 3 + 16,
    height: mobTotalH,
    channels: 3,
    background: "#080f25",
  },
})
  .composite(
    await (async () => {
      let top = 0;
      const comps = [];
      for (const row of mobRows) {
        comps.push({ input: row, left: 0, top });
        top += (await sharp(row).metadata()).height + 8;
      }
      return comps;
    })()
  )
  .png()
  .toFile(join(outDir, "09-contact-sheet-mobile-nine.png"));

// Click-only check
await page.setViewportSize({ width: 1440, height: 1100 });
await page.goto(`http://127.0.0.1:${port}/preview.html`, {
  waitUntil: "networkidle",
});
await page.hover('.mf-q[data-q="actions"]');
await page.waitForTimeout(120);
const stillRebrand = await page.evaluate(
  () =>
    document
      .querySelector('.mf-panel[data-panel="rebrand"]')
      ?.classList.contains("is-active") === true
);
await page.click('.mf-q[data-q="actions"]');
await page.waitForTimeout(120);
const clickedActions = await page.evaluate(
  () =>
    document
      .querySelector('.mf-panel[data-panel="actions"]')
      ?.classList.contains("is-active") === true
);
report.clickOnly = { hoverKeepsRebrand: stillRebrand, clickStays: clickedActions };

writeFileSync(join(outDir, "qa.json"), JSON.stringify(report, null, 2) + "\n");

const heightRows = IDS.map((id) => {
  const b = BEFORE[id];
  const a = report.heights.after[id];
  return `| ${id} | ${b} | ${a} | ${a - b} |`;
}).join("\n");

const review = `# Outcome below features — local review

**Webflow not updated. Not published. Click-only unchanged.**

## Sequence
Decision to evaluate → capabilities (primary + supporting + optional chips) → Decision outcome

## Mapping
| Q | Primary | Supporting | Chips |
|---|---|---|---|
| 01 | Brand Explorer | Smart Matching | Dealality Radar |
| 02 | Operator Explorer | Smart Matching | Operator Explorer — Proof & Track Record |
| 03 | Dealality Radar | Fee Estimator | Deal Readiness |
| 04 | Opportunity Review | Brand Explorer | Dealality Radar |
| 05 | Outreach Setup | Opportunity Review | — |
| 06 | Market Alerts | Dealality Radar | Brand Explorer — Footprint & Growth |
| 07 | Action Tracking | Deal Compare | Submit Proposal — Brand Response Workflow |
| 08 | Deal Compare | Fee Estimator | Submit Proposal — Brand Response Workflow |
| 09 | Deal Readiness | Clause Library & Financial Term Library (grouped) | — |

## Confirmations
- No duplicated outcomes: **${report.noDuplicateOutcomes}**
- No fabricated “Dealality Libraries” product module: **${report.noFabricatedCombinedModule}**
- Q05 rail: **Confidentiality & control?** / decision: **How do I maintain confidentiality and control?**
- Click-only: hoverKeepsRebrand=${stillRebrand}, clickStays=${clickedActions}

## Workspace heights (panel px)
| Question | Before | After | Δ |
|---|---:|---:|---:|
${heightRows}
`;

writeFileSync(join(outDir, "REVIEW.md"), review);
writeFileSync(join(repoOut, "REVIEW.md"), review);
writeFileSync(join(repoOut, "qa.json"), JSON.stringify(report, null, 2) + "\n");

const files = [
  "01-desktop-rebrand.png",
  "01-desktop-confidential.png",
  "01-desktop-actions.png",
  "01-desktop-proposals.png",
  "01-desktop-clarify.png",
  "02-mobile-rebrand.png",
  "02-mobile-confidential.png",
  "02-mobile-actions.png",
  "02-mobile-proposals.png",
  "02-mobile-clarify.png",
  "03-closeup-primary.png",
  "04-closeup-support.png",
  "05-closeup-chips.png",
  "06-closeup-libraries-grouped.png",
  "07-closeup-outcome-below.png",
  "08-contact-sheet-desktop-nine.png",
  "09-contact-sheet-mobile-nine.png",
];
for (const f of files) {
  copyFileSync(join(outDir, f), join(repoOut, f));
}

console.log(JSON.stringify({
  heights: report.heights,
  noDuplicateOutcomes: report.noDuplicateOutcomes,
  noFabricatedCombinedModule: report.noFabricatedCombinedModule,
  clickOnly: report.clickOnly,
  sample: {
    rebrand: report.panels.rebrand,
    confidential: report.panels.confidential,
    clarify: report.panels.clarify,
  },
}, null, 2));

await browser.close();
server.close();
