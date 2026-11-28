/**
 * Person identity collision protection V2.3
 *
 *   node scripts/test-gdi-person-identity-collision-v1.mjs
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  IDENTITY_MATCH,
  assessPersonIdentityMatch,
  buildIdentityDisambiguationFingerprint,
  durablePersonRecordKey,
} from "../lib/group-demand-intelligence/presentation/gdi-person-identity-collision-v1.js";
import {
  createDurableContactStore,
  CHANNEL_KIND,
} from "../lib/group-demand-intelligence/presentation/gdi-durable-contact-intelligence-store-v1.js";
import { createCrossWeekReuseHarness } from "../lib/group-demand-intelligence/presentation/gdi-contact-intelligence-cross-week-reuse-v1.js";
import { clearPublicContactResearchCacheForTests } from "../lib/group-demand-intelligence/presentation/gdi-public-contact-discovery-workflow-v1.js";
import { GDI_PUBLIC_CONTACT_LIVE_WRITE_ENABLED } from "../lib/group-demand-intelligence/presentation/gdi-public-contact-persistence-readiness-v1.js";
import { GDI_DURABLE_CONTACT_LIVE_AIRTABLE_WRITE_ENABLED } from "../lib/group-demand-intelligence/presentation/gdi-durable-contact-intelligence-store-v1.js";

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
}

check("live writes remain off", () => {
  assert.equal(GDI_PUBLIC_CONTACT_LIVE_WRITE_ENABLED, false);
  assert.equal(GDI_DURABLE_CONTACT_LIVE_AIRTABLE_WRITE_ENABLED, false);
});

check("same name+org without evidence stays ambiguous", () => {
  const m = assessPersonIdentityMatch(
    { name: "Jordan Lee", organization: "Acme Events" },
    { name: "Jordan Lee", organization: "Acme Events" },
  );
  assert.equal(m.match, IDENTITY_MATCH.AMBIGUOUS_UNRESOLVED);
  assert.equal(m.mergeAllowed, false);
});

check("same name+org conflicting profiles are distinct", () => {
  const m = assessPersonIdentityMatch(
    {
      name: "Jordan Lee",
      organization: "Acme Events",
      professionalProfileUrl: "https://linkedin.com/in/jordan-lee-a",
    },
    {
      name: "Jordan Lee",
      organization: "Acme Events",
      professionalProfileUrl: "https://linkedin.com/in/jordan-lee-b",
    },
  );
  assert.equal(m.match, IDENTITY_MATCH.DISTINCT_PERSONS);
  assert.equal(m.mergeAllowed, false);
});

check("same name+org matching source URL confirms same person", () => {
  const m = assessPersonIdentityMatch(
    {
      name: "Jordan Lee",
      organization: "Acme Events",
      personSpecificSourceUrl: "https://acme.example/team/jordan-lee",
    },
    {
      name: "Jordan Lee",
      organization: "Acme Events",
      personSpecificSourceUrl: "https://acme.example/team/jordan-lee",
    },
  );
  assert.equal(m.match, IDENTITY_MATCH.SAME_PERSON_CONFIRMED);
  assert.equal(m.mergeAllowed, true);
});

check("different orgs without continuity are distinct", () => {
  const m = assessPersonIdentityMatch(
    { name: "Jordan Lee", organization: "Acme Events" },
    { name: "Jordan Lee", organization: "Other Org LLC" },
  );
  assert.equal(m.match, IDENTITY_MATCH.DISTINCT_PERSONS);
  assert.equal(m.mergeAllowed, false);
});

check("durable keys diverge for conflicting fingerprints", () => {
  const a = durablePersonRecordKey({
    name: "Jordan Lee",
    organization: "Acme Events",
    professionalProfileUrl: "https://linkedin.com/in/jordan-a",
  });
  const b = durablePersonRecordKey({
    name: "Jordan Lee",
    organization: "Acme Events",
    professionalProfileUrl: "https://linkedin.com/in/jordan-b",
  });
  assert.notEqual(a, b);
  assert.ok(a.includes("::"));
  assert.ok(b.includes("::"));
});

await check("store refuses ambiguous second person under same name+org", () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-collision-"));
  const store = createDurableContactStore({
    storePath: path.join(dir, "store.json"),
  });
  const first = store.upsertPerson({
    name: "Sam Rivera",
    organization: "Twin Org",
    permissionToRetain: true,
    sourceUrl: "https://twin.example/team/sam-a",
    professionalProfileUrl: "https://linkedin.com/in/sam-a",
    origin: "PUBLIC_SOURCE",
    channels: {
      [CHANNEL_KIND.VERIFIED_DIRECT_WORK_EMAIL]: {
        value: "sam.a@twin.example",
        origin: "PUBLIC_SOURCE",
      },
    },
  });
  assert.equal(first.ok, true);
  const second = store.upsertPerson({
    name: "Sam Rivera",
    organization: "Twin Org",
    permissionToRetain: true,
    sourceUrl: "https://twin.example/team/sam-b",
    professionalProfileUrl: "https://linkedin.com/in/sam-b",
    origin: "PUBLIC_SOURCE",
    channels: {
      [CHANNEL_KIND.VERIFIED_DIRECT_WORK_EMAIL]: {
        value: "sam.b@twin.example",
        origin: "PUBLIC_SOURCE",
      },
    },
  });
  assert.equal(second.ok, true);
  assert.notEqual(first.personIdentityKey, second.personIdentityKey);
  assert.equal(store.listPeople().length, 2);
});

await check("cross-week reuse + rights DENIED display + provider refuse", () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-v23-week-"));
  const storePath = path.join(dir, "week.json");
  const h1 = createCrossWeekReuseHarness({ storePath });
  const name = "Taylor Brooks";
  const organization = "Brooks Meetings";
  h1.persistVerified({
    name,
    organization,
    sourceUrl: "https://brooks.example/team/taylor",
    origin: "PUBLIC_SOURCE",
    permissionToRetain: true,
    permissionToDisplayToHotelClients: false,
    hotelIds: ["hotel_a", "hotel_b"],
    opportunityIds: ["opp_1", "opp_2"],
    channels: {
      [CHANNEL_KIND.VERIFIED_DIRECT_WORK_EMAIL]: {
        value: "taylor@brooks.example",
        origin: "PUBLIC_SOURCE",
      },
    },
  });
  clearPublicContactResearchCacheForTests();
  const h2 = createCrossWeekReuseHarness({ storePath });
  const r = h2.resolve({ name, organization });
  assert.equal(r.ok, true);
  assert.equal(r.durableHit, true);
  const person = h2.store.getPerson(r.personIdentityKey) || h2.store.findByNameOrg(name, organization);
  assert.equal(person.rights.permissionToDisplayToHotelClients, false);
  const refused = h2.store.upsertPerson({
    name: "Provider Pat",
    organization: "Org",
    origin: "SURFE_PROVIDER_EPHEMERAL",
    permissionToRetain: true,
  });
  assert.equal(refused.ok, false);
});

check("fingerprint helper is stable", () => {
  const a = buildIdentityDisambiguationFingerprint({
    personSpecificSourceUrl: "https://Example.com/Team/X/",
    verifiedRole: "Director",
  });
  const b = buildIdentityDisambiguationFingerprint({
    personSpecificSourceUrl: "https://example.com/team/x",
    verifiedRole: "Director",
  });
  assert.equal(a, b);
});

console.log(`\nOK ${passed} checks — person identity collision v1`);
