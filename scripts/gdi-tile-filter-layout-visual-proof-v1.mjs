/**
 * Local visual proof for compact tile contacts + one-row property bar.
 * Does not trigger paid research.
 *
 *   node scripts/gdi-tile-filter-layout-visual-proof-v1.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { createRequire } from "node:module";
import vm from "node:vm";

const ROOT = process.cwd();
const OUT_DIR = path.join(ROOT, "reports/gdi-tile-filter-layout");
const BASE = process.env.GDI_LOCAL_BASE || "http://127.0.0.1:8080";
const MAY = "rec0eTEnCqlsGJJt9";
const SD = "recnPB3Q02NO8Ma1o";

fs.mkdirSync(OUT_DIR, { recursive: true });

function loadUi() {
  const src = fs.readFileSync(
    path.join(ROOT, "public/js/group-demand-intelligence/dealality-gdi-ui.js"),
    "utf8"
  );
  const sandbox = {
    window: {},
    globalThis: {},
    console,
    URL,
    URLSearchParams,
  };
  sandbox.window = sandbox;
  sandbox.globalThis = sandbox;
  vm.runInNewContext(src, sandbox, { filename: "dealality-gdi-ui.js" });
  return sandbox.DealalityGdiUi || sandbox.window.DealalityGdiUi;
}

async function fetchJson(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`${res.status} ${url}`);
  return res.json();
}

function pickCases(opps, UI) {
  const scored = (opps || []).map((o) => {
    const block = UI.selectCompactTileContact(
      UI.opportunityToBriefItem ? UI.opportunityToBriefItem(o) : o,
      o
    );
    return { o, block };
  });
  const named = scored.find(
    (x) => x.block.kind === "person" || x.block.kind === "referral"
  )?.o;
  const org = scored.find(
    (x) =>
      x.block.kind === "organization" &&
      x.block.channelKind === "email" &&
      x.block.channelText
  )?.o;
  const form = scored.find(
    (x) => x.block.kind === "organization" && x.block.channelKind === "form"
  )?.o;
  const namedNoChannel = scored.find(
    (x) =>
      x.o.primaryContact?.name &&
      x.block.kind === "organization" &&
      x.block.channelText
  )?.o;
  // Live inventory may currently lack form-only routes; keep a synthetic case so
  // visual proof covers Contact form link text without mutating Airtable.
  const formSynthetic = {
    id: "gdi_synthetic_form_only_v1",
    opportunityId: "gdi_synthetic_form_only_v1",
    title: "Harbor Events Center — Preferred Lodging Partnership",
    displayTitle: "Harbor Events Center — Preferred Lodging Partnership",
    organizationName: "Harbor Events Center",
    summaryWhat:
      "Nearby venue without lodging; public contact form is the only published route.",
    opportunityType: "VENUE_PARTNERSHIP",
    publicContactPath: "https://example.com/contact-us/",
    priority: "WATCHLIST",
  };
  return { named, org, form: form || formSynthetic, namedNoChannel };
}

const UI = loadUi();
const may = await fetchJson(
  `${BASE}/api/group-demand-intelligence/hotels/${MAY}/opportunities`
);
const sd = await fetchJson(
  `${BASE}/api/group-demand-intelligence/hotels/${SD}/opportunities`
);
const mayCases = pickCases(may.opportunities || [], UI);
const sdCases = pickCases(sd.opportunities || [], UI);

const bar = UI.propertyBarHtml({
  mode: "auth",
  hotel: {
    hotelName: "The Mayflower Hotel, Autograph Collection",
    city: "Washington",
    state: "DC",
  },
  hotelId: MAY,
  hotels: [
    {
      hotelId: MAY,
      hotelName: "The Mayflower Hotel, Autograph Collection",
      locationLine: "Washington, DC",
    },
    {
      hotelId: SD,
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

function tileFor(o) {
  if (!o) return '<p class="miss">No matching opportunity</p>';
  return UI.opportunityTileHtml(UI.opportunityToBriefItem(o), o, {});
}

const drawerSample =
  mayCases.namedNoChannel ||
  may.opportunities?.find((o) => o.primaryContact?.name) ||
  may.opportunities?.[0];
const drawer = drawerSample
  ? UI.whoShouldSalesContactHtml(drawerSample, {})
  : "<p>No drawer sample</p>";

const css = fs.readFileSync(
  path.join(ROOT, "public/css/group-demand-intelligence.css"),
  "utf8"
);
const shellCss = fs.readFileSync(
  path.join(ROOT, "public/css/deal-workspace-shell.css"),
  "utf8"
);

const html = `<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"/>
<title>GDI tile + filter layout proof</title>
<style>${shellCss}\n${css}
body{margin:0;background:#0b0d12;color:#fff;font-family:Inter,system-ui,sans-serif}
.proof{max-width:1200px;margin:0 auto;padding:24px}
.proof h1{font-size:20px;margin:0 0 8px}
.proof h2{font-size:15px;margin:28px 0 10px;color:#c9ced8}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:16px}
.note{font-size:12px;color:#9aa3b2;margin:0 0 16px}
.drawer-box{border:1px solid #333;border-radius:10px;padding:16px;background:#141821}
</style>
</head><body class="gdi-page aiv-dashboard">
<div class="proof">
  <h1>After — property bar (one primary row + history footer)</h1>
  <p class="note">Desktop-width proof. Labels above controls; history compact inside panel.</p>
  <div id="bar">${bar}</div>

  <h2>Mayflower — compact contact tiles</h2>
  <div class="grid">
    <div><p class="note">Named + usable person channel</p>${tileFor(mayCases.named)}</div>
    <div><p class="note">Organization / company route</p>${tileFor(mayCases.org)}</div>
    <div><p class="note">Named without personal channel → org fallback</p>${tileFor(mayCases.namedNoChannel)}</div>
  </div>

  <h2>San Diego — form / org spot-check</h2>
  <div class="grid">
    <div><p class="note">Named person</p>${tileFor(sdCases.named)}</div>
    <div><p class="note">Organization email</p>${tileFor(sdCases.org)}</div>
    <div><p class="note">Form / contact page</p>${tileFor(sdCases.form)}</div>
  </div>

  <h2>Drawer retains person + organization channels</h2>
  <div class="drawer-box" id="drawer">${drawer}</div>
</div>
</body></html>`;

const outHtml = path.join(OUT_DIR, "after-layout-proof.html");
fs.writeFileSync(outHtml, html, "utf8");

const casesJson = {
  may: {
    named: mayCases.named?.id,
    org: mayCases.org?.id,
    namedNoChannel: mayCases.namedNoChannel?.id,
  },
  sd: {
    named: sdCases.named?.id,
    org: sdCases.org?.id,
    form: sdCases.form?.id,
  },
  drawer: drawerSample?.id,
};
fs.writeFileSync(
  path.join(OUT_DIR, "case-selection.json"),
  JSON.stringify(casesJson, null, 2)
);

let shotPaths = [];
try {
  const require = createRequire(import.meta.url);
  const { chromium } = require("playwright");
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({ viewport: { width: 1440, height: 1100 } });
  await page.goto(pathToFileURL(outHtml).href, { waitUntil: "load" });
  const barShot = path.join(OUT_DIR, "after-property-bar.png");
  await page.locator("#bar").screenshot({ path: barShot });
  shotPaths.push(barShot);
  const tilesShot = path.join(OUT_DIR, "after-tiles-mayflower.png");
  await page.locator(".proof h2").nth(0).evaluate((el) => {
    const grid = el.nextElementSibling;
    return grid?.getBoundingClientRect();
  });
  await page
    .locator(".proof .grid")
    .first()
    .screenshot({ path: tilesShot });
  shotPaths.push(tilesShot);
  const sdShot = path.join(OUT_DIR, "after-tiles-san-diego.png");
  await page.locator(".proof .grid").nth(1).screenshot({ path: sdShot });
  shotPaths.push(sdShot);
  const drawerShot = path.join(OUT_DIR, "after-drawer-contact.png");
  await page.locator("#drawer").screenshot({ path: drawerShot });
  shotPaths.push(drawerShot);

  // Production "before" property bar (live assets still on prior bust until deploy)
  try {
    const prod = await browser.newPage({ viewport: { width: 1440, height: 900 } });
    await prod.goto(
      "https://my-operators-backend-production.up.railway.app/group-demand-intelligence.html",
      { waitUntil: "domcontentloaded", timeout: 45000 }
    );
    const beforeBar = path.join(OUT_DIR, "before-property-bar-prod-login-or-shell.png");
    await prod.screenshot({ path: beforeBar, fullPage: false });
    shotPaths.push(beforeBar);
    await prod.close();
  } catch (e) {
    fs.writeFileSync(
      path.join(OUT_DIR, "before-prod-screenshot-note.txt"),
      String(e && e.message ? e.message : e)
    );
  }

  await browser.close();
} catch (e) {
  fs.writeFileSync(
    path.join(OUT_DIR, "screenshot-error.txt"),
    String(e && e.stack ? e.stack : e)
  );
}

console.log(
  JSON.stringify(
    {
      outHtml,
      casesJson,
      shotPaths,
    },
    null,
    2
  )
);
