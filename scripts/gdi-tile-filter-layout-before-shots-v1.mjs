/**
 * Capture "before" screenshots from HEAD (pre-change) shared UI.
 */
import fs from "node:fs";
import path from "node:path";
import { execSync } from "node:child_process";
import { pathToFileURL } from "node:url";
import { createRequire } from "node:module";
import vm from "node:vm";

const ROOT = process.cwd();
const OUT = path.join(ROOT, "reports/gdi-tile-filter-layout");
fs.mkdirSync(OUT, { recursive: true });

function loadFromGit(spec) {
  const src = execSync(`git show ${spec}`, {
    encoding: "utf8",
    maxBuffer: 8e6,
  });
  const sandbox = {
    window: {},
    globalThis: {},
    console,
    URL,
    URLSearchParams,
  };
  sandbox.window = sandbox;
  sandbox.globalThis = sandbox;
  vm.runInNewContext(src, sandbox, { filename: spec });
  return sandbox.DealalityGdiUi;
}

const UI = loadFromGit("HEAD:public/js/group-demand-intelligence/dealality-gdi-ui.js");
let oldCss = fs.readFileSync(
  path.join(ROOT, "public/css/group-demand-intelligence.css"),
  "utf8"
);
try {
  oldCss = execSync("git show HEAD:public/css/group-demand-intelligence.css", {
    encoding: "utf8",
    maxBuffer: 8e6,
  });
} catch {
  /* keep working tree css */
}
const shellCss = fs.readFileSync(
  path.join(ROOT, "public/css/deal-workspace-shell.css"),
  "utf8"
);

const bar = UI.propertyBarHtml({
  mode: "auth",
  hotel: {
    hotelName: "The Mayflower Hotel, Autograph Collection",
    city: "Washington",
    state: "DC",
  },
  hotelId: "rec0eTEnCqlsGJJt9",
  hotels: [
    {
      hotelId: "rec0eTEnCqlsGJJt9",
      hotelName: "The Mayflower Hotel, Autograph Collection",
      locationLine: "Washington, DC",
    },
    {
      hotelId: "recnPB3Q02NO8Ma1o",
      hotelName: "Wyndham San Diego Bayside",
      locationLine: "San Diego, CA",
    },
  ],
  lastResearch: "Oct 15, 2026, 12:00 PM EDT",
  runStatus: "Completed",
  lastPublishedUpdate: "Oct 15, 2026, 9:24 AM EDT",
  actionHtml:
    '<button type="button" class="aiv-btn-apply" id="gdiRunBtn">Run Research</button>' +
    '<button type="button" class="btn-clear" id="gdiResetBtn">Reset View</button>',
});

const may = await (
  await fetch(
    "http://127.0.0.1:8080/api/group-demand-intelligence/hotels/rec0eTEnCqlsGJJt9/opportunities"
  )
).json();
const byId = (id) => (may.opportunities || []).find((o) => o.id === id);
const named =
  byId("gdi_opp_prospect_mv8fnmju_53cfad28") ||
  (may.opportunities || []).find((o) =>
    String(o.primaryContact?.email || "").includes(".")
  );
const org = byId("gdi_sc_056f479083d48dc1");
const quorum = byId("gdi_sc_43a6be41ced811a0");

const tiles = [named, org, quorum]
  .filter(Boolean)
  .map((o) => UI.opportunityTileHtml(UI.opportunityToBriefItem(o), o, {}))
  .join("");

const html = `<!doctype html><html><head><meta charset="utf-8"/>
<style>${shellCss}\n${oldCss}
body{margin:0;background:#0b0d12;color:#fff}
.proof{max-width:1200px;margin:0 auto;padding:24px}
.grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:16px}
</style></head>
<body class="gdi-page aiv-dashboard"><div class="proof">
<div id="bar">${bar}</div>
<div class="grid" id="tiles">${tiles}</div>
</div></body></html>`;

const outHtml = path.join(OUT, "before-layout-proof.html");
fs.writeFileSync(outHtml, html);

const { chromium } = createRequire(import.meta.url)("playwright");
const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({ viewport: { width: 1440, height: 1100 } });
await page.goto(pathToFileURL(outHtml).href, { waitUntil: "load" });
await page.locator("#bar").screenshot({ path: path.join(OUT, "before-property-bar.png") });
await page.locator("#tiles").screenshot({ path: path.join(OUT, "before-tiles-mayflower.png") });
await browser.close();
console.log("before shots written to", OUT);
