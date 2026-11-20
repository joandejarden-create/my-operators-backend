#!/usr/bin/env node
/**
 * Surfe Usage V1.3 — UI attribution, deep links, fixture separation, layout regression.
 * No paid Surfe API calls. Uses real canonical GDI hotel + opportunity records.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

const {
  appendSurfeLiveAccounting,
  summarizeSurfeLiveAccounting,
  FORBIDDEN_PII_KEYS,
} = await import("../lib/group-demand-intelligence/gdi-surfe-live-gate-v1.js");

const {
  enrichSurfeUsageContext,
  buildHotelContextMap,
  buildOpportunityDrawerHref,
  formatHotelIdentityDisplay,
  classifyUsageClass,
  USAGE_CLASS,
} = await import("../lib/group-demand-intelligence/gdi-surfe-usage-context-v1.js");

const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-surfe-v13-"));
const accountingFile = path.join(tmpRoot, "accounting.json");
const env = {
  GDI_SURFE_ACCOUNTING_PATH: accountingFile,
  GDI_SURFE_USAGE_ENVIRONMENT: "development",
};

function loadOpp(hotelId, opportunityId) {
  const file = path.join(
    ROOT,
    "data/group-demand-intelligence/hotels",
    hotelId,
    "opportunities.json",
  );
  const raw = JSON.parse(fs.readFileSync(file, "utf8"));
  const arr = Array.isArray(raw) ? raw : raw.opportunities || raw.items || [];
  const hit = arr.find(
    (o) => String(o.id || o.opportunityId) === String(opportunityId),
  );
  assert.ok(hit, `missing opportunity ${opportunityId}`);
  return hit;
}

const hotelMap = buildHotelContextMap();

// --- Phase 2: real hotel identity from canonical registry ---
{
  const beth = hotelMap.get("recLuxvwwxID7U2B8");
  assert.ok(beth, "Bethesda Marriott must be in GDI hotel registry");
  assert.equal(beth.hotelName, "Bethesda Marriott");
  assert.equal(beth.city, "Bethesda");
  assert.ok(beth.country);
  const display = formatHotelIdentityDisplay(
    enrichSurfeUsageContext({
      hotelId: "recLuxvwwxID7U2B8",
      hotelMap,
      env,
    }),
  );
  // formatHotelIdentityDisplay expects hotel fields; enrich returns hotelName etc.
  const hotelCtx = enrichSurfeUsageContext({
    hotelId: "recLuxvwwxID7U2B8",
    hotelMap,
    env,
  });
  const idDisplay = formatHotelIdentityDisplay({
    hotelName: hotelCtx.hotelName,
    city: hotelCtx.hotelCity,
    market: hotelCtx.hotelMarket,
    country: hotelCtx.hotelCountry,
    locationLine: hotelCtx.hotelLocationLine,
  });
  assert.equal(idDisplay.primary, "Bethesda Marriott");
  assert.match(idDisplay.secondary, /Bethesda/);
  assert.equal(idDisplay.unresolved, false);
}

// Unresolved hotel stays UNKNOWN + investigation flag
{
  const ctx = enrichSurfeUsageContext({
    hotelId: "recNOT_IN_REGISTRY_XYZ",
    hotelMap,
    env,
  });
  assert.equal(ctx.hotelName, "UNKNOWN");
  assert.equal(ctx.hotelIdentityInvestigationFlag, true);
}

// Never invent names from technical fixture IDs
{
  const ctx = enrichSurfeUsageContext({
    hotelId: "hotel_parallel",
    opportunityId: "opp_demo_3",
    hotelMap,
    env,
  });
  assert.equal(ctx.hotelName, "UNKNOWN");
  assert.equal(ctx.opportunityTitle, "UNKNOWN");
  assert.equal(ctx.usageClass, USAGE_CLASS.FIXTURE_TEST);
}

// --- Phase 3: real opportunity title + deep link ---
{
  const hotelId = "recLuxvwwxID7U2B8";
  const opportunityId = "gdi_opp_nado_ddaa_washcon_2028";
  const opportunity = loadOpp(hotelId, opportunityId);
  const ctx = enrichSurfeUsageContext({
    hotelId,
    opportunityId,
    opportunity: { ...opportunity, hotelId },
    hotelMap,
    env,
  });
  assert.match(ctx.opportunityTitle, /NADO/);
  assert.match(String(ctx.opportunityOrganizationName), /NADO|DDAA/i);
  assert.equal(
    ctx.opportunityDrawerHref,
    buildOpportunityDrawerHref(hotelId, opportunityId),
  );
  assert.match(
    ctx.opportunityDrawerHref,
    /hotelId=recLuxvwwxID7U2B8/,
  );
  assert.match(
    ctx.opportunityDrawerHref,
    /opportunityId=gdi_opp_nado_ddaa_washcon_2028/,
  );
}

// Deep-link open wiring present in GDI app
{
  const appJs = fs.readFileSync(
    path.join(ROOT, "public/js/group-demand-intelligence/app.js"),
    "utf8",
  );
  assert.match(appJs, /openOpportunityFromQueryOnce/);
  assert.match(appJs, /params\.get\("opportunityId"\)/);
  assert.match(appJs, /openDetail\(opportunityId\)/);
}

// --- Phase 4: request origin separate from opportunity hotel ---
{
  const hotelId = "recCAGqkERSXgiku4";
  const opportunityId = "disc_1490e4966ee8";
  const opportunity = loadOpp(hotelId, opportunityId);
  const ctx = enrichSurfeUsageContext({
    hotelId,
    opportunityId,
    opportunity: { ...opportunity, hotelId },
    dealalityUser: {
      companyName: "Acme Portfolio Co",
      companyProfileId: "port_acme",
    },
    body: {
      requestingHotelOrOfficeId: "office_miami_regional",
      requestingOfficeCity: "Miami",
      requestingOfficeMarket: "South Florida",
    },
    hotelMap,
    env,
  });
  assert.equal(ctx.hotelName, "Sheraton Guadalajara Expo");
  assert.match(String(ctx.hotelLocationLine || ctx.hotelCity), /Guadalajara/i);
  assert.equal(ctx.requestingOrganizationName, "Acme Portfolio Co");
  assert.equal(ctx.requestingHotelOrOfficeId, "office_miami_regional");
  assert.notEqual(ctx.requestingHotelOrOfficeId, ctx.hotelId);
  assert.equal(ctx.requestingOfficeCity, "Miami");
  assert.notEqual(ctx.requestingOfficeCity, ctx.hotelCity);
}

// --- Phase 7 simulation: append + summarize + no PII + no duplicate ---
{
  const hotelId = "recH9h3McgMNFQEla";
  const opportunityId = "rb_gdi_disc_dc428c1bd4c4e412";
  const opportunity = loadOpp(hotelId, opportunityId);
  const ctx = enrichSurfeUsageContext({
    hotelId,
    opportunityId,
    opportunity: { ...opportunity, hotelId },
    body: {
      clientRequestId: "cr_v13_sim_toronto_1",
      requestingOrganizationName: "Toronto Portfolio Ops",
      requestingHotelOrOfficeId: "office_toronto_ops",
      requestingOfficeCity: "Toronto",
      requestingOfficeMarket: "Greater Toronto",
      requestingPortfolioId: "port_toronto_demo",
    },
    hotelMap,
    env,
  });
  assert.equal(ctx.hotelName, "Radisson Blu Toronto Downtown");
  assert.match(ctx.opportunityTitle, /ISCT/);

  const first = appendSurfeLiveAccounting(
    {
      ...ctx,
      _enrichedContext: ctx,
      clientRequestId: "cr_v13_sim_toronto_1",
      channel: "email",
      lookupType: "email",
      emailReturned: true,
      phoneReturned: false,
      surfeAttempted: true,
      email: "secret@example.com",
      phone: "+15551212",
      contactName: "Secret",
      creditsCharged: 1,
      creditCertainty: "estimated",
      costEstimateUsd: 0.15,
      costCertainty: "estimated",
      usageClass: USAGE_CLASS.DEVELOPMENT,
      dataEnvironment: "development",
      layoutValidationOnly: true,
    },
    env,
  );
  assert.equal(first.deduplicated, false);
  assert.equal(first.entry.hotelName, "Radisson Blu Toronto Downtown");
  assert.match(first.entry.opportunityTitle, /ISCT/);
  assert.equal(first.entry.requestingOrganizationName, "Toronto Portfolio Ops");
  for (const k of FORBIDDEN_PII_KEYS) {
    assert.equal(first.entry[k], undefined);
  }
  assert.equal(first.entry.email, undefined);
  assert.equal(first.entry.phone, undefined);

  const dup = appendSurfeLiveAccounting(
    {
      ...ctx,
      _enrichedContext: ctx,
      clientRequestId: "cr_v13_sim_toronto_1",
      channel: "email",
      emailReturned: true,
      surfeAttempted: true,
      usageClass: USAGE_CLASS.DEVELOPMENT,
      dataEnvironment: "development",
    },
    env,
  );
  assert.equal(dup.deduplicated, true);

  // Fixture row remains fixture
  appendSurfeLiveAccounting(
    {
      hotelId: "hotel_parallel",
      opportunityId: "opp_demo_3",
      clientRequestId: "cr_parallel_v13_keep_fixture",
      channel: "both",
      emailReturned: false,
      phoneReturned: false,
      surfeAttempted: true,
    },
    env,
  );

  const withFixtures = summarizeSurfeLiveAccounting({ hideFixtures: false }, env);
  const hidden = summarizeSurfeLiveAccounting({ hideFixtures: true }, env);
  assert.ok(withFixtures.totals.fixtureTestCount >= 1);
  assert.ok(hidden.totals.fixtureTestCount === 0 || hidden.filters.hideFixtures === true);
  const visibleHotels = hidden.hotels.map((h) => h.hotelId);
  assert.ok(visibleHotels.includes("recH9h3McgMNFQEla"));
  assert.ok(!visibleHotels.includes("hotel_parallel"));

  const latest = hidden.latestActivity.find(
    (e) => e.clientRequestId === "cr_v13_sim_toronto_1",
  );
  assert.ok(latest);
  assert.equal(latest.hotelName, "Radisson Blu Toronto Downtown");
  assert.match(latest.opportunityTitle, /ISCT/);
  assert.equal(latest.requestingOrganizationName, "Toronto Portfolio Ops");
  assert.ok(latest.opportunityDrawerHref);
  assert.match(latest.opportunityDrawerHref, /opportunityId=/);

  const org = hidden.requestingOrganizations.find(
    (o) => o.requestingOrganizationName === "Toronto Portfolio Ops",
  );
  assert.ok(org);
  assert.ok(org.hotelsResearchedCount >= 1);

  const top = hidden.topOpportunities.find(
    (o) => o.opportunityId === opportunityId,
  );
  assert.ok(top);
  assert.match(top.opportunityTitle, /ISCT/);
  assert.equal(top.hotelName, "Radisson Blu Toronto Downtown");
}

// --- Phase 8: layout / CSS / HTML regression gates ---
{
  const css = fs.readFileSync(
    path.join(ROOT, "public/css/admin-surfe-usage.css"),
    "utf8",
  );
  const html = fs.readFileSync(
    path.join(ROOT, "public/app/admin/surfe-usage.html"),
    "utf8",
  );
  const js = fs.readFileSync(
    path.join(ROOT, "public/js/admin-surfe-usage.js"),
    "utf8",
  );

  // Stacked sections — not three equal narrow columns
  assert.match(html, /asu-top/);
  assert.match(html, /asu-middle/);
  assert.match(html, /asu-bottom/);
  assert.match(html, /Usage by opportunity hotel/);
  assert.match(html, /Usage by requesting organization/);
  assert.match(html, /Most researched opportunities/);
  assert.match(html, /Activity history/);
  assert.match(html, /asuHideFixtures" checked/);

  // Overflow handled via per-table scroll, not nested max-height grid squeeze
  assert.match(css, /\.asu-middle/);
  assert.match(css, /overflow-x:\s*auto/);
  assert.match(css, /overflow-x:\s*hidden/); // shell
  assert.match(css, /@media \(max-width: 720px\)/);
  assert.match(css, /@media \(max-width: 1100px\)/);
  assert.doesNotMatch(css, /grid-template-columns:\s*repeat\(3/);

  // Dual-line identity renderer + request-origin separation
  assert.match(js, /function identityCell/);
  assert.match(js, /asu-identity__primary/);
  assert.match(js, /requestingOrganizationName/);
  assert.match(js, /hideFixtures=1/);
  assert.match(js, /asuHideFixtures"\)\.checked = true/);
  assert.match(html, /Opportunity hotel/);
  assert.match(html, /Requested from/);
  assert.match(html, /City \/ Market \/ Country/);
  assert.match(html, /Hotels researched/);
}

// Airtable remains proposed / gated (no live sync required for V1.3)
{
  const syncPath = path.join(
    ROOT,
    "lib/group-demand-intelligence/gdi-surfe-usage-airtable-sync-v1.js",
  );
  assert.ok(fs.existsSync(syncPath));
  const syncSrc = fs.readFileSync(syncPath, "utf8");
  assert.match(syncSrc, /PROPOSED|dryRun|GDI_SURFE_AIRTABLE/);
}

console.log("test-gdi-surfe-usage-v1-3: PASS");
