/**
 * Cross-hotel person-cache reuse (process-local + subsequent-run fixture).
 *
 *   node scripts/test-gdi-contact-intelligence-v2-1-cross-hotel-reuse.mjs
 *
 * No paid providers. No live Airtable. Fixtures only (non-sensitive).
 */
import assert from "node:assert/strict";
import {
  runPublicContactDiscoveryWorkflow,
  clearPublicContactResearchCacheForTests,
  getCachedPublicContactResearch,
  putCachedPublicContactResearch,
} from "../lib/group-demand-intelligence/presentation/gdi-public-contact-discovery-workflow-v1.js";
import { publicContactPersonKey } from "../lib/group-demand-intelligence/presentation/gdi-public-contact-verification-v1.js";

let passed = 0;
function check(name, fn) {
  const r = fn();
  if (r && typeof r.then === "function") {
    return r.then(() => {
      passed += 1;
      console.log(`  PASS ${name}`);
    });
  }
  passed += 1;
  console.log(`  PASS ${name}`);
  return undefined;
}

const PERSON = {
  name: "Alex Rivera",
  organization: "Rivera Meetings Group",
};
const personKey = publicContactPersonKey({
  name: PERSON.name,
  organization: PERSON.organization,
});

const verifiedOpp = {
  id: "opp-hotel-a",
  organizationName: PERSON.organization,
  organizationDomain: "rivera-meetings.example",
  contactDataOrigin: "PUBLIC_SOURCE",
  primaryContact: {
    name: PERSON.name,
    role: "Director of Events",
    title: "Director of Events",
    email: "alex.rivera@rivera-meetings.example",
    phone: "+1-202-555-0147",
    sourceUrl: "https://rivera-meetings.example/team/alex-rivera",
  },
};

const incompleteOppHotelB = {
  id: "opp-hotel-b",
  organizationName: PERSON.organization,
  organizationDomain: "rivera-meetings.example",
  primaryContact: {
    name: PERSON.name,
    role: "Director of Events",
    title: "Director of Events",
    // Channels intentionally absent — must come from person cache
  },
};

clearPublicContactResearchCacheForTests();

await check("A: same-process multi-hotel reuse avoids duplicate fetch", async () => {
  clearPublicContactResearchCacheForTests();
  const first = await runPublicContactDiscoveryWorkflow({
    opportunity: verifiedOpp,
    hotelId: "hotel_a",
    authorizePaidPublicSearch: false,
    persist: false,
    reuseCrossHotelCache: true,
    skipDns: true,
    effort: { maxQueries: 0, maxPages: 1 },
  });
  assert.equal(first.ok, true);
  assert.ok(getCachedPublicContactResearch(personKey), "cache seeded on verified exit");

  const second = await runPublicContactDiscoveryWorkflow({
    opportunity: incompleteOppHotelB,
    hotelId: "hotel_b",
    authorizePaidPublicSearch: false,
    persist: false,
    reuseCrossHotelCache: true,
    skipDns: true,
    effort: { maxQueries: 0, maxPages: 3 },
  });
  assert.equal(second.ok, true);
  assert.equal(second.metrics.crossHotelReuse, true);
  assert.equal(second.metrics.pagesFetched, 0);
  assert.equal(second.after?.hasVerifiedDirectEmail, true);
  assert.equal(second.after?.hasVerifiedDirectPhone, true);
  assert.ok(
    second.methods.some((m) => m.method === "cross_hotel_person_reuse"),
    "reuse method recorded",
  );
});

await check("B: subsequent-run reuse via fixture cache (non-durable PII store)", async () => {
  clearPublicContactResearchCacheForTests();
  // Simulate a later process loading a prior non-sensitive session fixture
  putCachedPublicContactResearch(personKey, {
    hotelId: "hotel_a_prior_run",
    opportunityId: "opp-hotel-a",
    found: {
      email: "alex.rivera@rivera-meetings.example",
      phone: "+1-202-555-0147",
      sourceUrl: "https://rivera-meetings.example/team/alex-rivera",
      verificationTimestamp: "2026-11-21T16:59:39.000Z",
    },
  });
  const later = await runPublicContactDiscoveryWorkflow({
    opportunity: incompleteOppHotelB,
    hotelId: "hotel_c",
    authorizePaidPublicSearch: false,
    persist: false,
    reuseCrossHotelCache: true,
    skipDns: true,
    effort: { maxQueries: 0, maxPages: 3 },
  });
  assert.equal(later.metrics.crossHotelReuse, true);
  assert.equal(later.metrics.pagesFetched, 0);
  assert.equal(later.after?.hasVerifiedDirectEmail, true);
});

await check("identity collision: different org does not reuse", async () => {
  clearPublicContactResearchCacheForTests();
  putCachedPublicContactResearch(personKey, {
    hotelId: "hotel_a",
    found: {
      email: "alex.rivera@rivera-meetings.example",
      phone: "+1-202-555-0147",
      sourceUrl: "https://rivera-meetings.example/team/alex-rivera",
    },
  });
  const otherOrg = await runPublicContactDiscoveryWorkflow({
    opportunity: {
      id: "opp-other",
      organizationName: "Other Events LLC",
      primaryContact: { name: PERSON.name, role: "Planner" },
    },
    hotelId: "hotel_x",
    authorizePaidPublicSearch: false,
    persist: false,
    reuseCrossHotelCache: true,
    skipDns: true,
    effort: { maxQueries: 0, maxPages: 1 },
  });
  assert.equal(otherOrg.metrics.crossHotelReuse, false);
});

console.log(`\nOK ${passed} checks — cross-hotel reuse v2.1`);
