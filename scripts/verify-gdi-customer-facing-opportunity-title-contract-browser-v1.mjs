#!/usr/bin/env node
/**
 * Browser evidence: customer-facing title contract on cards/lists for
 * Bethesda Marriott + two additional GDI hotels.
 *
 * Auth: mocked /api/me (NOT live Memberstack).
 *
 *   DEALALITY_LOCAL_ADMIN_BASE=http://127.0.0.1:8080 \
 *     node scripts/verify-gdi-customer-facing-opportunity-title-contract-browser-v1.mjs
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";
import { customerFacingOpportunityTitle } from "../lib/group-demand-intelligence/customer-facing-opportunity-title-contract-v1.js";
import { accountLevelDisplayTitle } from "../lib/group-demand-intelligence/opportunity-list-dto.js";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const BASE = (
  process.env.DEALALITY_LOCAL_ADMIN_BASE || "http://127.0.0.1:8080"
).replace(/\/$/, "");
const OUT = path.join(
  ROOT,
  "reports/gdi-customer-facing-opportunity-title-contract-v1/screenshots"
);
fs.mkdirSync(OUT, { recursive: true });

const HOTELS = [
  {
    id: "recLuxvwwxID7U2B8",
    label: "bethesda-marriott",
    nameNeedle: "Bethesda Marriott",
    mustCleanIds: {
      gdi_opp_amwa_2027_interim: "AMWA 2027 Interim Meeting",
      gdi_opp_asae_annual_2029: "ASAE Annual Meeting & Exposition 2029",
    },
    expectVisibleTitles: [
      "AMWA 2027 Interim Meeting",
      "ASAE Annual Meeting & Exposition 2029",
    ],
    forbidVisible: ["Sep 11–12", "(early watch)", "(Early Watch)"],
  },
  {
    id: "gdi_hotel_waterstone_boca_raton",
    label: "waterstone-boca",
    nameNeedle: "Waterstone",
    mustCleanIds: {},
    expectVisibleTitles: [],
    forbidVisible: [],
  },
  {
    id: "rec2PVBDavppGpenm",
    label: "ac-hotel-a-coruna",
    nameNeedle: "A Coruña",
    mustCleanIds: {},
    expectVisibleTitles: [],
    forbidVisible: [],
  },
];

const FOUNDER = [
  {
    before: "AMWA 2027 Interim Meeting — Sep 11–12",
    after: "AMWA 2027 Interim Meeting",
  },
  {
    before: "PDA TRI Bethesda Training — December 2026 Lodging",
    after: "PDA TRI Bethesda Training",
  },
  {
    before: "ASAE Annual Meeting & Exposition 2029 — Washington, DC (Early Watch)",
    after: "ASAE Annual Meeting & Exposition 2029",
  },
];
for (const ex of FOUNDER) {
  assert.equal(customerFacingOpportunityTitle(ex.before), ex.after);
  assert.equal(customerFacingOpportunityTitle(ex.after), ex.after);
}

const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({ viewport: { width: 1440, height: 1100 } });

await page.route("**/api/me**", async (route) => {
  await route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify({
      ok: true,
      success: true,
      user: { email: "dealalitydemo@dealality.com" },
      dealality: {
        email: "dealalitydemo@dealality.com",
        isAdmin: true,
        isDemo: true,
        flags: { isAdmin: true, gdiPilot: true },
      },
    }),
  });
});

const evidence = [];

for (const hotel of HOTELS) {
  const doc = await loadOpportunitiesCanonical(hotel.id);
  const opps = Array.isArray(doc?.opportunities) ? doc.opportunities : [];
  assert.ok(opps.length > 0, `no opportunities for ${hotel.id}`);

  for (const opp of opps.slice(0, 120)) {
    const displayed = accountLevelDisplayTitle(opp);
    if (!displayed) continue;
    assert.equal(
      customerFacingOpportunityTitle(displayed),
      displayed,
      `${hotel.id} ${opp.id || opp.opportunityId}: DTO title not contract-idempotent (${displayed})`
    );
  }

  for (const [id, expected] of Object.entries(hotel.mustCleanIds || {})) {
    const row = opps.find((o) => (o.id || o.opportunityId) === id);
    assert.ok(row, `${hotel.id} missing ${id}`);
    assert.equal(accountLevelDisplayTitle(row), expected);
  }

  /** @type {any} */
  let apiPayload = null;
  page.removeAllListeners("response");
  page.on("response", async (res) => {
    try {
      const u = res.url();
      if (
        u.includes(`/hotels/${encodeURIComponent(hotel.id)}/opportunities`) ||
        u.includes(`/hotels/${hotel.id}/opportunities`)
      ) {
        if (!u.match(/\/opportunities\/[^/?]+/)) {
          apiPayload = await res.json();
        }
      }
    } catch {
      /* ignore */
    }
  });

  await page.addInitScript((hotelId) => {
    try {
      localStorage.setItem("gdi.hotelId", hotelId);
      localStorage.setItem("dealality.gdi.hotelId", hotelId);
    } catch {
      /* ignore */
    }
  }, hotel.id);

  const url = `${BASE}/group-demand-intelligence.html?hotelId=${encodeURIComponent(
    hotel.id
  )}`;
  await page.goto(url, { waitUntil: "domcontentloaded", timeout: 90000 });
  await page.waitForFunction(
    (needle) => {
      const t = document.body?.innerText || "";
      return t.includes(needle) && !t.includes("Loading Group Demand Intelligence");
    },
    hotel.nameNeedle,
    { timeout: 90000 }
  );
  await page.waitForTimeout(1500);

  if (apiPayload) {
    const apiOpps =
      apiPayload.opportunities ||
      apiPayload.items ||
      apiPayload.data?.opportunities ||
      [];
    for (const [id, expected] of Object.entries(hotel.mustCleanIds || {})) {
      const row = apiOpps.find((o) => (o.id || o.opportunityId) === id);
      if (!row) continue;
      const title =
        row.displayTitle || row.drawerHeaderTitle || row.title || "";
      assert.equal(
        customerFacingOpportunityTitle(title),
        expected,
        `${hotel.label} API title for ${id}`
      );
    }
  }

  const bodyText = await page.locator("body").innerText();
  assert.ok(
    bodyText.includes(hotel.nameNeedle),
    `${hotel.label} property not loaded (${hotel.nameNeedle})`
  );

  for (const clean of hotel.expectVisibleTitles || []) {
    assert.ok(
      bodyText.includes(clean),
      `${hotel.label} missing visible title: ${clean}`
    );
  }
  // Dates/locations may correctly appear in Event Date pills or summaries —
  // only title nodes must stay free of research suffixes.
  const dirtyTitles = await page.evaluate((forbidden) => {
    const sels = [
      ".gdi-tile-title",
      ".gdi-opp-title",
      ".gdi-list-title",
      "[data-field='displayTitle']",
      "h3",
      "h4",
    ];
    const nodes = [];
    for (const sel of sels) {
      document.querySelectorAll(sel).forEach((el) => nodes.push(el));
    }
    const bad = [];
    for (const el of nodes) {
      const t = (el.textContent || "").trim();
      for (const frag of forbidden) {
        if (t.includes(frag)) bad.push({ text: t.slice(0, 120), frag });
      }
    }
    return bad;
  }, hotel.forbidVisible || []);
  assert.equal(
    dirtyTitles.length,
    0,
    `${hotel.label} dirty titles: ${JSON.stringify(dirtyTitles)}`
  );

  // Switch to List view when available for denser title evidence.
  const listBtn = page.locator('button:has-text("List"), [data-view="list"]');
  if ((await listBtn.count()) > 0) {
    await listBtn.first().click().catch(() => {});
    await page.waitForTimeout(800);
  }

  if ((hotel.expectVisibleTitles || []).length) {
    const search = page.locator(
      'input[type="search"], input[placeholder*="Search" i], input[aria-label*="Search" i]'
    );
    if ((await search.count()) > 0) {
      await search.first().fill(hotel.expectVisibleTitles[0]);
      await page.waitForTimeout(1000);
    }
  }

  const shot = path.join(OUT, `${hotel.label}-list.png`);
  await page.screenshot({ path: shot, fullPage: false });
  evidence.push({
    hotelId: hotel.id,
    label: hotel.label,
    screenshot: shot,
    apiCaptured: Boolean(apiPayload),
    opportunityCount: opps.length,
    visibleSnippet: (bodyText.match(/Showing[^\n]+/) || [null])[0],
  });
}

await browser.close();

const report = {
  ok: true,
  version: "gdi-customer-facing-opportunity-title-contract-v1",
  generatedAt: new Date().toISOString(),
  hotels: evidence,
  founderExamples: FOUNDER,
};
fs.writeFileSync(
  path.join(
    ROOT,
    "reports/gdi-customer-facing-opportunity-title-contract-v1/02-browser-verify.json"
  ),
  JSON.stringify(report, null, 2)
);
console.log(JSON.stringify(report, null, 2));
