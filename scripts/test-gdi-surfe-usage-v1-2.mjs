#!/usr/bin/env node
/**
 * Surfe Usage V1.2 regression — context, classification, Airtable plan, privacy.
 * No paid Surfe calls.
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
  classifyUsageClass,
  resolveRequestOriginContext,
  resolveOpportunityContext,
  USAGE_CLASS,
} = await import("../lib/group-demand-intelligence/gdi-surfe-usage-context-v1.js");

const {
  planSurfeUsageAirtableSync,
  buildAirtableFieldsFromUsageEvent,
  PROPOSED_GDI_SURFE_USAGE_EVENTS_TABLE,
  syncSurfeUsageEventsToAirtable,
} = await import(
  "../lib/group-demand-intelligence/gdi-surfe-usage-airtable-sync-v1.js"
);

const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-surfe-v12-"));
const accountingFile = path.join(tmpRoot, "accounting.json");
const env = { GDI_SURFE_ACCOUNTING_PATH: accountingFile };

// Hotel name resolution — unknown when not in canonical map
{
  const ctx = enrichSurfeUsageContext({
    hotelId: "recFAKEHOTEL",
    opportunityId: "opp_real_1",
    opportunity: {
      title: "Euroguss 2027",
      organizationName: "NürnbergMesse",
      hotelId: "recFAKEHOTEL",
    },
    dealalityUser: {
      companyProfileId: "recCOMPANY1",
      companyName: "Regional Sales Co",
    },
    body: { requestingHotelOrOfficeId: "recOFFICE99" },
    hotelMap: new Map(),
    env: { ...env, GDI_SURFE_USAGE_ENVIRONMENT: "development" },
  });
  assert.equal(ctx.hotelName, "UNKNOWN");
  assert.equal(ctx.opportunityTitle, "Euroguss 2027");
  assert.equal(ctx.opportunityOrganizationName, "NürnbergMesse");
  assert.equal(ctx.requestingOrganizationName, "Regional Sales Co");
  assert.equal(ctx.requestingHotelOrOfficeId, "recOFFICE99");
  assert.notEqual(ctx.requestingHotelOrOfficeId, ctx.hotelId);
  assert.equal(ctx.usageClass, USAGE_CLASS.DEVELOPMENT);
}

// Cross-hotel request origin — opportunity hotel ≠ requesting office
{
  const origin = resolveRequestOriginContext({
    dealalityUser: { companyName: "Acme Hospitality", companyProfileId: "p1" },
    body: { requestingHotelOrOfficeId: "office_miami" },
    opportunity: { hotelId: "hotel_opportunity_only" },
  });
  assert.equal(origin.requestingHotelOrOfficeId, "office_miami");
  assert.equal(origin.requestingOrganizationName, "Acme Hospitality");
}

// Opportunity unresolved stays UNKNOWN (no invented name from ID)
{
  const opp = resolveOpportunityContext({}, "hist_abcdef");
  assert.equal(opp.opportunityTitle, "UNKNOWN");
  assert.equal(opp.resolution, "unresolved");
}

// Fixture classification for historical parallel/demo/a
{
  assert.equal(
    classifyUsageClass({ hotelId: "hotel_parallel", opportunityId: "opp_p_0" }),
    USAGE_CLASS.FIXTURE_TEST,
  );
  assert.equal(
    classifyUsageClass({ hotelId: "hotel_demo", clientRequestId: "cr_x" }),
    USAGE_CLASS.FIXTURE_TEST,
  );
  assert.equal(
    classifyUsageClass({ hotelId: "hotel_a", opportunityId: "opp_1" }),
    USAGE_CLASS.FIXTURE_TEST,
  );
}

// Append stores V1.2 fields; privacy strip
{
  const r = appendSurfeLiveAccounting(
    {
      hotelId: "recHotelX",
      opportunityId: "opp_live_1",
      channel: "email",
      emailReturned: true,
      clientRequestId: "cr_v12_1",
      email: "secret@example.com",
      contactName: "Secret Person",
      hotelName: "Test Hotel Canonical",
      opportunityTitle: "Annual Meeting",
      opportunityOrganizationName: "Assoc X",
      requestingOrganizationName: "Buyer Org",
      requestingHotelOrOfficeId: "office_1",
      dataEnvironment: "staging",
      usageClass: "STAGING",
    },
    env,
  );
  assert.equal(r.entry.schema, "gdi-surfe-usage-tracking-v1.2");
  assert.equal(r.entry.hotelName, "Test Hotel Canonical");
  assert.equal(r.entry.opportunityTitle, "Annual Meeting");
  assert.equal(r.entry.requestingOrganizationName, "Buyer Org");
  assert.equal(r.entry.dataEnvironment, "staging");
  assert.equal(r.entry.usageClass, "STAGING");
  for (const k of FORBIDDEN_PII_KEYS) {
    assert.equal(r.entry[k], undefined);
  }
  assert.equal(r.entry.email, undefined);
  assert.equal(r.entry.contactName, undefined);
}

// Summary exposes requesting orgs + top opportunities + sync health
{
  const summary = summarizeSurfeLiveAccounting({ hotelNames: {} }, env);
  assert.equal(summary.version, "gdi-surfe-usage-tracking-v1.2");
  assert.ok(Array.isArray(summary.requestingOrganizations));
  assert.ok(Array.isArray(summary.topOpportunities));
  assert.ok(summary.airtableSyncHealth);
  assert.equal(summary.latestActivity[0].opportunityTitle, "Annual Meeting");
}

// Airtable plan: fixtures excluded; dry-run blocked without enable
{
  const plan = planSurfeUsageAirtableSync([
    {
      lookupEventId: "evt_fix",
      hotelId: "hotel_parallel",
      usageClass: "FIXTURE_TEST",
    },
    {
      lookupEventId: "evt_real",
      hotelId: "recHotelX",
      opportunityTitle: "Annual Meeting",
      dataEnvironment: "staging",
      usageClass: "STAGING",
      creditsCharged: 1,
      creditCertainty: "estimated",
    },
  ]);
  assert.equal(plan.plannedCount, 1);
  assert.equal(plan.planned[0].identity, "evt_real");
  assert.equal(PROPOSED_GDI_SURFE_USAGE_EVENTS_TABLE.tableId, null);

  const fields = buildAirtableFieldsFromUsageEvent(plan.planned[0].fields);
  // buildAirtableFields expects entry not nested fields — test entry shape
  const fields2 = buildAirtableFieldsFromUsageEvent({
    lookupEventId: "evt_real",
    hotelId: "recHotelX",
    opportunityTitle: "Annual Meeting",
    email: "should-not-appear@x.com",
    phone: "+100000",
  });
  assert.equal(fields2["Lookup Event ID"], "evt_real");
  assert.equal(fields2.Email, undefined);
  assert.equal(fields2.Phone, undefined);

  const sync = await syncSurfeUsageEventsToAirtable(
    [{ lookupEventId: "evt_real", hotelId: "recHotelX" }],
    { env: { ...env, GDI_SURFE_USAGE_AIRTABLE_SYNC_ENABLE: "0" } },
  );
  assert.equal(sync.blocked, true);
  assert.equal(sync.reason, "SYNC_DISABLED");
}

// Concurrent append still works
{
  await Promise.all(
    [0, 1, 2].map((i) =>
      Promise.resolve(
        appendSurfeLiveAccounting(
          {
            hotelId: "recHotelX",
            opportunityId: `opp_c_${i}`,
            channel: "email",
            emailReturned: true,
            clientRequestId: `cr_v12_conc_${i}`,
            opportunityTitle: `Opp ${i}`,
            dataEnvironment: "development",
          },
          env,
        ),
      ),
    ),
  );
  const doc = JSON.parse(fs.readFileSync(accountingFile, "utf8"));
  assert.ok(doc.entries.length >= 4);
}

// Assets exist
for (const rel of [
  "lib/group-demand-intelligence/gdi-surfe-usage-context-v1.js",
  "lib/group-demand-intelligence/gdi-surfe-usage-airtable-sync-v1.js",
  "scripts/gdi-surfe-usage-classify-historical-v1.mjs",
  "public/js/admin-surfe-usage.js",
  "public/app/admin/surfe-usage.html",
]) {
  assert.ok(fs.existsSync(path.join(ROOT, rel)), rel);
}

console.log("PASS gdi-surfe-usage-v1-2");
