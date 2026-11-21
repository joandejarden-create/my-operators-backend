/**
 * FullEnrich benchmark eligibility — genuine Surfe miss capture (no paid FE).
 *
 *   node scripts/test-gdi-fullenrich-benchmark-eligibility-v1.mjs
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  appendSurfeLiveAccounting,
  contactRevealIdentityKey,
  readSurfeLiveAccounting,
} from "../lib/group-demand-intelligence/gdi-surfe-live-gate-v1.js";
import {
  personIdentityKey,
  contactNameHash,
  isGenuineSurfeWorkEmailMiss,
  assessFullEnrichIdentityReadiness,
  buildFullEnrichBenchmarkEligibilityReport,
} from "../lib/group-demand-intelligence/gdi-fullenrich-benchmark-eligibility-v1.js";
import {
  reserveFullEnrichSpend,
  resetFullEnrichSpendLedgerForTests,
} from "../lib/group-demand-intelligence/gdi-fullenrich-spend-ledger-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

let passed = 0;
function check(name, fn) {
  fn();
  passed += 1;
  console.log(`  PASS ${name}`);
}

const CONTACT = "Maria Lopez";
const ORG = "Lopez Events Group";
const personKey = personIdentityKey({
  contactName: CONTACT,
  organizationName: ORG,
});

function makeOpp(hotelId, opportunityId, extras = {}) {
  return {
    id: opportunityId,
    opportunityId,
    title: extras.title || `Opp ${opportunityId}`,
    organizationName: ORG,
    organizationDomain: "lopez-events.example",
    primaryContact: {
      name: CONTACT,
      role: "Director of Events",
      linkedinUrl: "https://www.linkedin.com/in/maria-lopez-example",
      ...extras.primaryContact,
    },
    ...extras,
  };
}

function oppLoader(map) {
  return (hotelId, opportunityId) => {
    const k = `${hotelId}|${opportunityId}`;
    return map.get(k) || null;
  };
}

check("personIdentityKey requires first+last and organization", () => {
  assert.ok(personKey);
  assert.equal(
    personIdentityKey({ contactName: "Maria", organizationName: ORG }),
    null,
  );
  assert.equal(
    personIdentityKey({ contactName: CONTACT, organizationName: "" }),
    null,
  );
  // Same name, different org → different keys (no name-only merge)
  const other = personIdentityKey({
    contactName: CONTACT,
    organizationName: "Other Corp LLC",
  });
  assert.ok(other);
  assert.notEqual(personKey, other);
});

check("isGenuineSurfeWorkEmailMiss classifies + excludes fixtures", () => {
  assert.equal(
    isGenuineSurfeWorkEmailMiss({
      contactProvider: "SURFE",
      surfeAttempted: true,
      channel: "email",
      miss: true,
      emailReturned: false,
      usageClass: "PRODUCTION",
      dataEnvironment: "production",
      hotelId: "htl_real_san_juan",
      opportunityId: "opp_real_001",
    }),
    true,
  );
  assert.equal(
    isGenuineSurfeWorkEmailMiss({
      contactProvider: "SURFE",
      surfeAttempted: true,
      channel: "email",
      miss: true,
      emailReturned: false,
      usageClass: "FIXTURE_TEST",
      hotelId: "hotel_a",
      opportunityId: "opp_test_1",
    }),
    false,
  );
  assert.equal(
    isGenuineSurfeWorkEmailMiss({
      contactProvider: "FULLENRICH",
      surfeAttempted: false,
      channel: "email",
      miss: true,
      emailReturned: false,
      usageClass: "PRODUCTION",
      hotelId: "htl_real_san_juan",
      opportunityId: "opp_real_001",
    }),
    false,
  );
});

check("FullEnrich identity readiness needs company or LinkedIn", () => {
  const ready = assessFullEnrichIdentityReadiness({
    name: CONTACT,
    organizationName: ORG,
    companyDomain: "lopez-events.example",
  });
  assert.equal(ready.ready, true);
  const notReady = assessFullEnrichIdentityReadiness({
    name: "Maria",
    organizationName: "",
  });
  assert.equal(notReady.ready, false);
});

{
  const surfeDir = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-fe-elig-surfe-"));
  const feDir = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-fe-elig-fe-"));
  const env = {
    GDI_SURFE_ACCOUNTING_DIR: surfeDir,
    GDI_FULLENRICH_SPEND_DIR: feDir,
    GDI_FULLENRICH_SPEND_CAP: "20",
    NODE_ENV: "test",
  };
  resetFullEnrichSpendLedgerForTests(env);

  const hotelA = "htl_benchmark_hotel_a";
  const hotelB = "htl_benchmark_hotel_b";
  const oppA = "opp_benchmark_real_a";
  const oppB = "opp_benchmark_real_b";
  const keyA = contactRevealIdentityKey({
    hotelId: hotelA,
    opportunityId: oppA,
    contactName: CONTACT,
    channel: "email",
  });
  const keyB = contactRevealIdentityKey({
    hotelId: hotelB,
    opportunityId: oppB,
    contactName: CONTACT,
    channel: "email",
  });

  const oppMap = new Map([
    [`${hotelA}|${oppA}`, makeOpp(hotelA, oppA, { title: "San Juan gala" })],
    [`${hotelB}|${oppB}`, makeOpp(hotelB, oppB, { title: "Ponce summit" })],
  ]);

  // Fixture miss — must never enter shortlist
  appendSurfeLiveAccounting(
    {
      hotelId: "hotel_a",
      opportunityId: "opp_test_fixture_miss",
      identityKey: "fixture-key",
      channel: "email",
      channelRequested: "email",
      miss: true,
      emailReturned: false,
      phoneReturned: false,
      surfeAttempted: true,
      contactProvider: "SURFE",
      usageClass: "FIXTURE_TEST",
      dataEnvironment: "test",
      clientRequestId: "cr_benchmark_fixture_1",
      at: "2026-11-01T10:00:00.000Z",
    },
    env,
  );

  // Genuine miss hotel A
  appendSurfeLiveAccounting(
    {
      hotelId: hotelA,
      opportunityId: oppA,
      identityKey: keyA,
      personIdentityKey: personKey,
      contactNameHash: contactNameHash(CONTACT),
      channel: "email",
      channelRequested: "email",
      miss: true,
      status: "no_match",
      emailReturned: false,
      phoneReturned: false,
      surfeAttempted: true,
      contactProvider: "SURFE",
      usageClass: "PRODUCTION",
      dataEnvironment: "production",
      hotelName: "Hotel Benchmark A",
      opportunityTitle: "San Juan gala",
      opportunityOrganizationName: ORG,
      requestingOrganizationName: "Dealality Pilot Org",
      clientRequestId: "cr_benchmark_prod_a",
      at: "2026-11-10T12:00:00.000Z",
      fullEnrichIdentityInputs: {
        ready: true,
        hasFirstLast: true,
        hasCompanyName: true,
        hasCompanyDomain: true,
        hasLinkedInUrl: true,
      },
    },
    env,
  );

  // Same person, hotel B — cross-hotel dedupe
  appendSurfeLiveAccounting(
    {
      hotelId: hotelB,
      opportunityId: oppB,
      identityKey: keyB,
      personIdentityKey: personKey,
      contactNameHash: contactNameHash(CONTACT),
      channel: "email",
      channelRequested: "email",
      miss: true,
      status: "no_match",
      emailReturned: false,
      phoneReturned: false,
      surfeAttempted: true,
      contactProvider: "SURFE",
      usageClass: "PRODUCTION",
      dataEnvironment: "production",
      hotelName: "Hotel Benchmark B",
      opportunityTitle: "Ponce summit",
      opportunityOrganizationName: ORG,
      requestingOrganizationName: "Dealality Pilot Org",
      clientRequestId: "cr_benchmark_prod_b",
      at: "2026-11-11T12:00:00.000Z",
      fullEnrichIdentityInputs: {
        ready: true,
        hasFirstLast: true,
        hasCompanyName: true,
        hasCompanyDomain: true,
        hasLinkedInUrl: true,
      },
    },
    env,
  );

  check("cross-hotel same person → one shortlist row, two opportunities", () => {
    const report = buildFullEnrichBenchmarkEligibilityReport({
      env,
      loadOpportunity: oppLoader(oppMap),
    });
    assert.equal(report.summary.genuineEligibleMissEvents, 2);
    assert.equal(report.summary.distinctVerifiedContacts, 1);
    assert.equal(report.summary.associatedHotels, 2);
    assert.equal(report.summary.associatedOpportunities, 2);
    assert.equal(report.summary.estimatedBenchmarkCreditExposure, 1);
    assert.ok(report.summary.excluded.fixture >= 1);
    assert.equal(report.shortlist.length, 1);
    assert.equal(report.shortlist[0].personIdentityKey, personKey);
    assert.equal(report.shortlist[0].hotelCount, 2);
    assert.equal(report.shortlist[0].opportunityCount, 2);
    assert.equal(report.shortlist[0].contactDisplayName, CONTACT);
    assert.equal(report.privacy.includesProviderReturnedEmails, false);
    assert.equal(report.controls.paidLookupsTriggered, false);
    assert.equal(report.controls.customerDisplayBlocked, true);
  });

  check("subsequent email recovery removes eligibility", () => {
    appendSurfeLiveAccounting(
      {
        hotelId: hotelA,
        opportunityId: oppA,
        identityKey: keyA,
        personIdentityKey: personKey,
        contactNameHash: contactNameHash(CONTACT),
        channel: "email",
        channelRequested: "email",
        miss: false,
        status: "success",
        emailReturned: true,
        phoneReturned: false,
        surfeAttempted: true,
        contactProvider: "SURFE",
        usageClass: "PRODUCTION",
        dataEnvironment: "production",
        clientRequestId: "cr_benchmark_prod_recover",
        at: "2026-11-12T12:00:00.000Z",
      },
      env,
    );
    const report = buildFullEnrichBenchmarkEligibilityReport({
      env,
      loadOpportunity: oppLoader(oppMap),
    });
    assert.equal(report.summary.distinctVerifiedContacts, 0);
    assert.ok(report.summary.excluded.laterRecovered >= 1);
  });
}

{
  // Fresh ledgers for prior-FE exclusion
  const surfeDir = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-fe-elig2-surfe-"));
  const feDir = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-fe-elig2-fe-"));
  const env = {
    GDI_SURFE_ACCOUNTING_DIR: surfeDir,
    GDI_FULLENRICH_SPEND_DIR: feDir,
    GDI_FULLENRICH_SPEND_CAP: "20",
    NODE_ENV: "test",
  };
  resetFullEnrichSpendLedgerForTests(env);

  const hotelId = "htl_benchmark_prior_fe";
  const opportunityId = "opp_benchmark_prior_fe";
  const identityKey = contactRevealIdentityKey({
    hotelId,
    opportunityId,
    contactName: CONTACT,
    channel: "email",
  });
  const oppMap = new Map([
    [`${hotelId}|${opportunityId}`, makeOpp(hotelId, opportunityId)],
  ]);

  appendSurfeLiveAccounting(
    {
      hotelId,
      opportunityId,
      identityKey,
      personIdentityKey: personKey,
      contactNameHash: contactNameHash(CONTACT),
      channel: "email",
      miss: true,
      status: "no_match",
      emailReturned: false,
      surfeAttempted: true,
      contactProvider: "SURFE",
      usageClass: "PRODUCTION",
      dataEnvironment: "production",
      clientRequestId: "cr_benchmark_prior_miss",
      at: "2026-11-13T12:00:00.000Z",
      fullEnrichIdentityInputs: {
        ready: true,
        hasFirstLast: true,
        hasCompanyName: true,
        hasCompanyDomain: true,
        hasLinkedInUrl: false,
      },
    },
    env,
  );

  const reserved = reserveFullEnrichSpend(
    {
      hotelId,
      opportunityId,
      identityKey,
      personIdentityKey: personKey,
      clientRequestId: "cr_benchmark_prior_fe",
      creditsReserved: 1,
    },
    env,
  );
  assert.equal(reserved.ok, true);

  check("prior FullEnrich attempt excludes contact", () => {
    const report = buildFullEnrichBenchmarkEligibilityReport({
      env,
      loadOpportunity: oppLoader(oppMap),
    });
    assert.equal(report.summary.distinctVerifiedContacts, 0);
    assert.ok(report.summary.excluded.priorFullEnrich >= 1);
  });

  check("accounting append stores personIdentityKey without contactName", () => {
    const { doc } = readSurfeLiveAccounting(env);
    const row = doc.entries.find((e) => e.identityKey === identityKey);
    assert.ok(row);
    assert.equal(row.personIdentityKey, personKey);
    assert.equal(row.contactName, undefined);
    assert.ok(!("email" in row) || row.email == null);
  });
}

check("admin route + UI wiring present", () => {
  const server = fs.readFileSync(path.join(ROOT, "server.js"), "utf8");
  assert.match(
    server,
    /\/api\/admin\/surfe-usage\/fullenrich-benchmark-eligibility/,
  );
  assert.match(server, /getAdminFullEnrichBenchmarkEligibility/);

  const api = fs.readFileSync(
    path.join(ROOT, "api/admin-surfe-usage.js"),
    "utf8",
  );
  assert.match(api, /getAdminFullEnrichBenchmarkEligibility/);
  assert.match(api, /buildFullEnrichBenchmarkEligibilityReport/);
  assert.match(api, /paidLookupsTriggered:\s*false/);

  const html = fs.readFileSync(
    path.join(ROOT, "public/app/admin/surfe-usage.html"),
    "utf8",
  );
  assert.match(html, /asuFeBenchmarkTable/);
  assert.match(html, /FullEnrich benchmark readiness/);

  const client = fs.readFileSync(
    path.join(ROOT, "public/js/admin-surfe-usage.js"),
    "utf8",
  );
  assert.match(
    client,
    /\/api\/admin\/surfe-usage\/fullenrich-benchmark-eligibility/,
  );
  assert.match(client, /loadFeBenchmark/);
});

check("eligibility module never imports paid FullEnrich client", () => {
  const src = fs.readFileSync(
    path.join(
      ROOT,
      "lib/group-demand-intelligence/gdi-fullenrich-benchmark-eligibility-v1.js",
    ),
    "utf8",
  );
  assert.doesNotMatch(src, /fullenrich\.io|FULLENRICH_API_KEY|createGdiFullEnrichContactRevealProvider/);
  assert.doesNotMatch(src, /contactDetails\.email|providerReturned/);
});

console.log(`\ntest-gdi-fullenrich-benchmark-eligibility-v1: PASS (${passed})`);
