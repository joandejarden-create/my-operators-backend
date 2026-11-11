#!/usr/bin/env node
/**
 * Browser evidence: Bethesda 88-list shows restored commercial categories
 * (no Other/Unclassified for the 29 historical rows).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { mergeBethesdaInternalResearchOpportunities } from "../lib/group-demand-intelligence/research/bethesda-internal-research-union-v1.js";
import { BETHESDA_29_ATTESTED_IDS } from "../lib/group-demand-intelligence/research/bethesda-historical-attestation-bag-v1.js";
import {
  classifyOpportunityTypeFilter,
  OPPORTUNITY_TYPE_FILTER,
  OPPORTUNITY_TYPE_FILTER_LABEL,
} from "../lib/group-demand-intelligence/opportunity-type-filter-v1.js";
import { mapOpportunitiesToPresentationDtos } from "../lib/group-demand-intelligence/presentation/gdi-universal-presentation-pipeline-v1.js";
import { GDI_PRESENTATION_SURFACES } from "../lib/group-demand-intelligence/contracts/gdi-presentation-contract-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const BASE = (
  process.env.DEALALITY_LOCAL_ADMIN_BASE || "http://127.0.0.1:8080"
).replace(/\/$/, "");
const HOTEL = "recLuxvwwxID7U2B8";
const OUT = path.join(
  ROOT,
  "reports",
  "gdi-bethesda-29-commercial-quality-cleanup-v1",
  "screenshots"
);
fs.mkdirSync(OUT, { recursive: true });

const doc = await loadOpportunitiesCanonical(HOTEL);
const union = mergeBethesdaInternalResearchOpportunities(
  HOTEL,
  doc.opportunities || []
);
assert.equal(union.opportunities.length, 88);
const listDtos = mapOpportunitiesToPresentationDtos(union.opportunities, {
  hotelId: HOTEL,
  surface: GDI_PRESENTATION_SURFACES.LOCAL_AUTH,
});
assert.equal(listDtos.length, 88);
const hist = listDtos.filter((o) =>
  BETHESDA_29_ATTESTED_IDS.includes(o.id || o.opportunityId)
);
assert.equal(hist.length, 29);
for (const o of hist) {
  assert.notEqual(
    o.opportunityTypeFilterCategory || classifyOpportunityTypeFilter(o),
    OPPORTUNITY_TYPE_FILTER.OTHER_UNCLASSIFIED,
    o.id
  );
}

const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({ viewport: { width: 1440, height: 1200 } });

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
  if (/\/opportunities\/[^/?]+/.test(url) && !url.includes("?")) {
    return route.fallback();
  }
  await route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify({
      ok: true,
      hotelId: HOTEL,
      count: listDtos.length,
      listScope: union.scope,
      opportunities: listDtos,
    }),
  });
});

function histFilter(o) {
  return (
    o.opportunityTypeFilterCategory || classifyOpportunityTypeFilter(o)
  );
}
const sampleCatering = hist.find(
  (o) => histFilter(o) === OPPORTUNITY_TYPE_FILTER.LOCAL_CATERING_DAY_MEETINGS
);
const samplePlp = hist.find(
  (o) => histFilter(o) === OPPORTUNITY_TYPE_FILTER.PREFERRED_LODGING_PARTNERSHIP
);
const samplePep = hist.find(
  (o) =>
    histFilter(o) ===
    OPPORTUNITY_TYPE_FILTER.PROFESSIONAL_EVENT_PLANNER_PARTNERSHIP
);

for (const sample of [sampleCatering, samplePlp, samplePep].filter(Boolean)) {
  await page.route(
    `**/api/group-demand-intelligence/hotels/${HOTEL}/opportunities/${sample.id}**`,
    async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          ok: true,
          hotelId: HOTEL,
          opportunity: {
            ...sample,
            summaryWhat: sample.summaryWhat || sample.whatIsHappening,
            summaryWhyHotel: sample.summaryWhyHotel || sample.hotelFitRationale,
            recommendedAction: sample.recommendedAction,
            opportunityTypeLabel: sample.opportunityTypeLabel,
            segment: sample.segment,
          },
        }),
      });
    }
  );
}

await page.goto(
  `${BASE}/group-demand-intelligence.html?hotelId=${HOTEL}&hotelName=Bethesda%20Marriott`,
  { waitUntil: "domcontentloaded", timeout: 60000 }
);
await page.waitForTimeout(2500);

const listShot = path.join(OUT, "bethesda-88-corrected-categories-desktop.png");
await page.screenshot({ path: listShot, fullPage: true });

const detailShots = [];
for (const [label, sample] of [
  ["catering", sampleCatering],
  ["plp", samplePlp],
  ["pep", samplePep],
]) {
  if (!sample) continue;
  await page.goto(
    `${BASE}/group-demand-intelligence.html?hotelId=${HOTEL}&hotelName=Bethesda%20Marriott&opportunityId=${encodeURIComponent(sample.id)}`,
    { waitUntil: "domcontentloaded", timeout: 60000 }
  );
  await page.waitForTimeout(2000);
  const p = path.join(OUT, `bethesda-detail-${label}-${sample.id.slice(0, 24)}.png`);
  await page.screenshot({ path: p, fullPage: true });
  detailShots.push({
    label,
    id: sample.id,
    organizationName: sample.organizationName,
    opportunityTypeLabel: sample.opportunityTypeLabel,
    filter: OPPORTUNITY_TYPE_FILTER_LABEL[classifyOpportunityTypeFilter(sample)],
    path: path.relative(ROOT, p).replace(/\\/g, "/"),
  });
}

await browser.close();

const categoryCounts = {};
for (const o of hist) {
  const f = classifyOpportunityTypeFilter(o);
  const label = OPPORTUNITY_TYPE_FILTER_LABEL[f] || f;
  categoryCounts[label] = (categoryCounts[label] || 0) + 1;
}

const report = {
  ok: true,
  memberstack: "NOT_VERIFIED_MOCKED_AUTH",
  unionCount: 88,
  historicalCount: 29,
  otherUnclassifiedHistorical: 0,
  categoryCounts,
  screenshots: {
    list: path.relative(ROOT, listShot).replace(/\\/g, "/"),
    details: detailShots,
  },
};
fs.writeFileSync(
  path.join(
    ROOT,
    "reports/gdi-bethesda-29-commercial-quality-cleanup-v1/05-browser-verify.json"
  ),
  `${JSON.stringify(report, null, 2)}\n`
);
console.log(JSON.stringify(report, null, 2));
