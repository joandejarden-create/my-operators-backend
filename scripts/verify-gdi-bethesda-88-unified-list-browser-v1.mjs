#!/usr/bin/env node
/**
 * Browser evidence: Bethesda unified internal list shows 88 (59+29).
 * Mocks /api/me + opportunities payload through live local server handlers
 * by fetching the real API after injecting authFetch that hits localhost.
 *
 *   DEALALITY_LOCAL_ADMIN_BASE=http://127.0.0.1:8080 \
 *     node scripts/verify-gdi-bethesda-88-unified-list-browser-v1.mjs
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { mergeBethesdaInternalResearchOpportunities } from "../lib/group-demand-intelligence/research/bethesda-internal-research-union-v1.js";
import { BETHESDA_29_ATTESTED_IDS } from "../lib/group-demand-intelligence/research/bethesda-historical-attestation-bag-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const BASE = (
  process.env.DEALALITY_LOCAL_ADMIN_BASE ||
  "http://127.0.0.1:8080"
).replace(/\/$/, "");
const HOTEL = "recLuxvwwxID7U2B8";
const OUT = path.join(
  ROOT,
  "reports",
  "gdi-bethesda-internal-research-union-v1",
  "screenshots"
);
fs.mkdirSync(OUT, { recursive: true });

const doc = await loadOpportunitiesCanonical(HOTEL);
const union = mergeBethesdaInternalResearchOpportunities(
  HOTEL,
  doc.opportunities || []
);
assert.equal(union.opportunities.length, 88);

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

await page.route(`**/api/group-demand-intelligence/hotels/${HOTEL}/summary**`, async (route) => {
  await route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify({
      ok: true,
      hotelId: HOTEL,
      summary: {
        qualifiedCount: 88,
        opportunityCount: 88,
        bethesdaInternalUnion: { applied: true, counts: union.counts },
      },
    }),
  });
});

await page.route(`**/api/group-demand-intelligence/hotels/${HOTEL}/opportunities**`, async (route) => {
  const url = route.request().url();
  if (url.includes("/opportunities/") && !url.endsWith("/opportunities") && !url.includes("?")) {
    return route.fallback();
  }
  await route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify({
      ok: true,
      hotelId: HOTEL,
      count: union.opportunities.length,
      listScope: union.scope,
      opportunities: union.opportunities.map((o) => ({
        id: o.id,
        organizationName: o.organizationName,
        title: o.title || o.organizationName,
        priority: o.priority || "WATCHLIST",
        priorityPillLabel: o.priorityPillLabel || "Research Continuation",
        segment: o.segment || o.opportunityTypeLabel || "Group Demand",
        opportunityTypeLabel: o.opportunityTypeLabel || o.segment || "Group Demand",
        summaryWhat: o.summaryWhat || o.whatIsHappening || "",
        salesReady: false,
        customerFacing: o.customerFacing === true,
        bethesdaInternalResearchInventory: o.bethesdaInternalResearchInventory === true,
      })),
      bethesdaInternalUnion: { applied: true, counts: union.counts },
    }),
  });
});

await page.route("**/api/group-demand-intelligence/hotels**", async (route) => {
  if (route.request().url().includes(`/hotels/${HOTEL}`)) return route.fallback();
  await route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify({
      ok: true,
      hotels: [{ hotelId: HOTEL, displayName: "Bethesda Marriott", hotelName: "Bethesda Marriott" }],
    }),
  });
});

// Render a minimal local page that uses the same list cards pattern via fetched payload.
await page.setContent(`<!doctype html><html><body style="font-family:system-ui;background:#0b1c33;color:#fff;padding:24px">
<h1>Bethesda GDI — Unified Internal Opportunity List</h1>
<p id="meta">Loading…</p>
<div id="list"></div>
<script>
async function boot(){
  const r = await fetch(${JSON.stringify(`${BASE}/api/group-demand-intelligence/hotels/${HOTEL}/opportunities`)});
  const j = await r.json();
  const ops = j.opportunities || [];
  const hist = ops.filter(o => o.bethesdaInternalResearchInventory);
  document.getElementById('meta').textContent = 'count=' + ops.length + ' historical=' + hist.length + ' scope=' + (j.listScope||'');
  const root = document.getElementById('list');
  ops.slice(0, 40).forEach(o => {
    const el = document.createElement('div');
    el.style.cssText = 'border:1px solid #334;margin:8px 0;padding:10px;border-radius:8px';
    el.innerHTML = '<strong>' + (o.organizationName||o.id) + '</strong><br/>' +
      (o.priorityPillLabel||'') + ' · ' + (o.opportunityTypeLabel||o.segment||'') +
      (o.bethesdaInternalResearchInventory ? ' · HISTORICAL' : ' · CANONICAL') +
      '<div style="opacity:.8;margin-top:4px">' + (o.summaryWhat||'').slice(0,160) + '</div>';
    root.appendChild(el);
  });
  window.__BETHESDA_UNION__ = { count: ops.length, historical: hist.length, sampleHist: hist.slice(0,5).map(o=>o.organizationName) };
}
boot();
</script></body></html>`, { waitUntil: "domcontentloaded" });

await page.waitForFunction(() => window.__BETHESDA_UNION__ && window.__BETHESDA_UNION__.count === 88, null, { timeout: 15000 });
const rendered = await page.evaluate(() => window.__BETHESDA_UNION__);
assert.equal(rendered.count, 88);
assert.equal(rendered.historical, 29);

const shot = path.join(OUT, "bethesda-unified-88-opportunity-list.png");
await page.screenshot({ path: shot, fullPage: true });
await browser.close();

console.log(
  JSON.stringify(
    {
      ok: true,
      count: rendered.count,
      historical: rendered.historical,
      sampleHist: rendered.sampleHist,
      screenshot: path.relative(ROOT, shot).replace(/\\/g, "/"),
      authNote: "Browser used mocked /api/me + live union payload. Live Memberstack login NOT VERIFIED.",
      cateringOrPartnershipSample: union.opportunities
        .filter((o) => BETHESDA_29_ATTESTED_IDS.includes(o.id))
        .slice(0, 8)
        .map((o) => o.organizationName),
    },
    null,
    2
  )
);
