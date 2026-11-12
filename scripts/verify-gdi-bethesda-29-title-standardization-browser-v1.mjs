#!/usr/bin/env node
/**
 * Browser evidence: Bethesda 29 historical titles render with standardized commercial titles
 * in the unified internal list + a detail-style panel.
 *
 * Auth: mocked /api/me (NOT live Memberstack).
 *
 *   DEALALITY_LOCAL_ADMIN_BASE=http://127.0.0.1:8080 \
 *     node scripts/verify-gdi-bethesda-29-title-standardization-browser-v1.mjs
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { mergeBethesdaInternalResearchOpportunities } from "../lib/group-demand-intelligence/research/bethesda-internal-research-union-v1.js";
import {
  BETHESDA_29_STANDARDIZED_TITLES,
  INTENTIONALLY_GENERIC_TITLE_IDS,
} from "../lib/group-demand-intelligence/research/bethesda-29-title-standardization-v1.js";
import { BETHESDA_29_ATTESTED_IDS } from "../lib/group-demand-intelligence/research/bethesda-historical-attestation-bag-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const BASE = (
  process.env.DEALALITY_LOCAL_ADMIN_BASE ||
  "http://127.0.0.1:8080"
).replace(/\/$/, "");
const HOTEL = "recLuxvwwxID7U2B8";
const OUT = path.join(ROOT, "reports/gdi-bethesda-29-title-standardization-v1/screenshots");
fs.mkdirSync(OUT, { recursive: true });

const doc = await loadOpportunitiesCanonical(HOTEL);
const union = mergeBethesdaInternalResearchOpportunities(HOTEL, doc.opportunities || []);
assert.equal(union.opportunities.length, 88);

const hist = union.opportunities.filter((o) =>
  BETHESDA_29_ATTESTED_IDS.includes(o.id || o.opportunityId)
);
assert.equal(hist.length, 29);
for (const row of hist) {
  const id = row.id || row.opportunityId;
  assert.equal(
    row.title,
    BETHESDA_29_STANDARDIZED_TITLES[id],
    `union title mismatch ${id}`
  );
}

const detailId = "disp_recLuxvwwxID7U2B8_parenteral_drug_association_pda_";
const detailRow = hist.find((o) => o.id === detailId);
assert.ok(detailRow);

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

await page.route("**/api/group-demand-intelligence/hotels/**", async (route) => {
  const url = route.request().url();
  if (url.includes(`/opportunities/${detailId}`)) {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        ok: true,
        hotelId: HOTEL,
        opportunity: {
          id: detailRow.id,
          organizationName: detailRow.organizationName,
          title: detailRow.title,
          displayTitle: detailRow.title,
          drawerHeaderTitle: detailRow.title,
          summaryWhat: detailRow.summaryWhat || detailRow.whatIsHappening || "",
          whatIsHappening: detailRow.whatIsHappening || detailRow.summaryWhat || "",
        },
      }),
    });
    return;
  }
  if (url.includes(`/hotels/${HOTEL}/opportunities`)) {
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
          priorityPillLabel: o.priorityPillLabel || "Research Continuation",
          opportunityTypeLabel: o.opportunityTypeLabel || o.segment || "Group Demand",
          summaryWhat: o.summaryWhat || o.whatIsHappening || "",
          bethesdaInternalResearchInventory:
            o.bethesdaInternalResearchInventory === true ||
            BETHESDA_29_ATTESTED_IDS.includes(o.id),
        })),
        bethesdaInternalUnion: { applied: true, counts: union.counts },
      }),
    });
    return;
  }
  await route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify({ ok: true }),
  });
});

await page.setContent(
  `<!doctype html><html><body style="font-family:Georgia,serif;background:#0b1c33;color:#f4f1ea;padding:24px">
<h1>Bethesda GDI — Title Standardization Verify</h1>
<p id="meta">Loading…</p>
<div id="list" style="display:grid;gap:8px;max-width:920px"></div>
<hr style="margin:24px 0;border-color:#445"/>
<h2>Detail</h2>
<div id="detail" style="max-width:920px;padding:16px;border:1px solid #556;border-radius:8px"></div>
<script>
async function boot(){
  const r = await fetch(${JSON.stringify(`${BASE}/api/group-demand-intelligence/hotels/${HOTEL}/opportunities`)});
  const j = await r.json();
  const ops = j.opportunities || [];
  const hist = ops.filter(o => o.bethesdaInternalResearchInventory);
  document.getElementById('meta').textContent =
    'count=' + ops.length + ' historical=' + hist.length;
  const root = document.getElementById('list');
  hist.forEach(o => {
    const el = document.createElement('div');
    el.style.cssText = 'border:1px solid #445;padding:10px;border-radius:6px';
    el.innerHTML = '<div style="font-size:15px;font-weight:600">' + (o.title||'') + '</div>' +
      '<div style="opacity:.75;font-size:12px;margin-top:4px">' + (o.organizationName||'') +
      ' · ' + (o.opportunityTypeLabel||'') + '</div>';
    root.appendChild(el);
  });
  const d = await fetch(${JSON.stringify(`${BASE}/api/group-demand-intelligence/hotels/${HOTEL}/opportunities/${detailId}`)});
  const dj = await d.json();
  const opp = dj.opportunity || {};
  document.getElementById('detail').innerHTML =
    '<div style="font-size:20px;font-weight:700">' + (opp.title||opp.displayTitle||'') + '</div>' +
    '<div style="opacity:.8;margin-top:8px">' + (opp.organizationName||'') + '</div>' +
    '<div style="margin-top:12px;opacity:.9">' + (opp.summaryWhat||opp.whatIsHappening||'').slice(0,280) + '</div>';
  window.__TITLE_VERIFY__ = {
    count: ops.length,
    historical: hist.length,
    titles: hist.map(o => o.title),
    detailTitle: opp.title || opp.displayTitle || null,
  };
}
boot().catch((err) => { window.__TITLE_VERIFY__ = { error: String(err) }; });
</script></body></html>`,
  { waitUntil: "domcontentloaded" }
);

await page.waitForFunction(
  () => window.__TITLE_VERIFY__ && (window.__TITLE_VERIFY__.count === 88 || window.__TITLE_VERIFY__.error),
  null,
  { timeout: 20000 }
);
const early = await page.evaluate(() => window.__TITLE_VERIFY__);
if (early?.error) {
  throw new Error(`browser boot failed: ${early.error}`);
}
const rendered = await page.evaluate(() => window.__TITLE_VERIFY__);
assert.equal(rendered.historical, 29);
assert.equal(rendered.titles.length, 29);
for (const id of BETHESDA_29_ATTESTED_IDS) {
  assert.ok(
    rendered.titles.includes(BETHESDA_29_STANDARDIZED_TITLES[id]),
    `missing rendered title for ${id}`
  );
}
assert.equal(rendered.detailTitle, BETHESDA_29_STANDARDIZED_TITLES[detailId]);

const listShot = path.join(OUT, "bethesda-29-titles-list.png");
const detailShot = path.join(OUT, "bethesda-29-title-detail-pda.png");
await page.screenshot({ path: listShot, fullPage: true });
await page.locator("#detail").screenshot({ path: detailShot });
await browser.close();

console.log(
  JSON.stringify(
    {
      ok: true,
      count: rendered.count,
      historical: rendered.historical,
      detailTitle: rendered.detailTitle,
      intentionallyGeneric: INTENTIONALLY_GENERIC_TITLE_IDS.length,
      listScreenshot: path.relative(ROOT, listShot).replace(/\\/g, "/"),
      detailScreenshot: path.relative(ROOT, detailShot).replace(/\\/g, "/"),
      authNote:
        "Browser used mocked /api/me + live research-stage union payload. Live Memberstack login NOT VERIFIED.",
    },
    null,
    2
  )
);
