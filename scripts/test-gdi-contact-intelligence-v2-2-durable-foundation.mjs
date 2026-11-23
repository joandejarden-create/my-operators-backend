/**
 * GDI Contact Intelligence V2.2 — durable foundation + cross-week reuse tests.
 *
 *   node scripts/test-gdi-contact-intelligence-v2-2-durable-foundation.mjs
 *
 * Fixture-only. No live Airtable. No paid providers.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  clearPublicContactResearchCacheForTests,
  putCachedPublicContactResearch,
} from "../lib/group-demand-intelligence/presentation/gdi-public-contact-discovery-workflow-v1.js";
import { publicContactPersonKey } from "../lib/group-demand-intelligence/presentation/gdi-public-contact-verification-v1.js";
import {
  CHANNEL_KIND,
  REUSE_LAYER,
  createDurableContactStore,
  GDI_DURABLE_CONTACT_LIVE_AIRTABLE_WRITE_ENABLED,
} from "../lib/group-demand-intelligence/presentation/gdi-durable-contact-intelligence-store-v1.js";
import {
  resolveReusableContactIntelligence,
  createCrossWeekReuseHarness,
} from "../lib/group-demand-intelligence/presentation/gdi-contact-intelligence-cross-week-reuse-v1.js";
import { getCanonicalContactModelRecommendation } from "../lib/group-demand-intelligence/presentation/gdi-contact-intelligence-canonical-model-v1.js";
import { getContactIntelligenceCommercialReadiness } from "../lib/group-demand-intelligence/presentation/gdi-contact-intelligence-commercial-readiness-v1.js";
import { GDI_PUBLIC_CONTACT_LIVE_WRITE_ENABLED } from "../lib/group-demand-intelligence/presentation/gdi-public-contact-persistence-readiness-v1.js";

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

const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-contact-v22-"));
const storePath = path.join(tmpDir, "durable-contacts.json");

check("live writes remain hard-off", () => {
  assert.equal(GDI_PUBLIC_CONTACT_LIVE_WRITE_ENABLED, false);
  assert.equal(GDI_DURABLE_CONTACT_LIVE_AIRTABLE_WRITE_ENABLED, false);
});

check("canonical model reuses embedded destinations; no Contacts table", () => {
  const m = getCanonicalContactModelRecommendation();
  assert.equal(m.createNewContactsTable, false);
  assert.equal(m.strategy, "EMBEDDED_PRIMARY_CONTACT_PLUS_DURABLE_PERSON_INDEX");
  assert.ok(m.airtableDestinationsReused.length >= 3);
  assert.ok(m.justifiedSchemaAdditionsPendingFounder.length >= 1);
});

check("commercial readiness preserves Surfe-first; providers off", () => {
  const c = getContactIntelligenceCommercialReadiness();
  assert.equal(c.surfeFirstPreserved, true);
  assert.equal(c.findymailActivation, "OFF");
  assert.equal(c.pdlActivation, "OFF");
  assert.equal(c.fullEnrichActivation, "OFF");
  assert.equal(c.liveCustomerPublication, "CUSTOMER_BLOCKED");
});

await check("CREATE + READ-BACK + DEDUPE in durable store", () => {
  const store = createDurableContactStore({ storePath, persist: true });
  const name = "Alex Rivera";
  const organization = "Rivera Meetings Group";
  const key = publicContactPersonKey({ name, organization });
  const created = store.upsertPerson({
    name,
    organization,
    role: "Director of Events",
    sourceUrl: "https://rivera-meetings.example/team/alex",
    origin: "PUBLIC_SOURCE",
    permissionToRetain: true,
    permissionToDisplayToHotelClients: false,
    opportunityIds: ["opp-a"],
    hotelIds: ["hotel_a"],
    channels: {
      [CHANNEL_KIND.VERIFIED_DIRECT_WORK_EMAIL]: {
        kind: CHANNEL_KIND.VERIFIED_DIRECT_WORK_EMAIL,
        value: "alex.rivera@rivera-meetings.example",
        origin: "PUBLIC_SOURCE",
      },
      [CHANNEL_KIND.VERIFIED_DIRECT_BUSINESS_PHONE]: {
        kind: CHANNEL_KIND.VERIFIED_DIRECT_BUSINESS_PHONE,
        value: "+1-202-555-0147",
        origin: "PUBLIC_SOURCE",
      },
    },
  });
  assert.equal(created.ok, true);
  assert.equal(created.created, true);
  const again = store.upsertPerson({
    name,
    organization,
    permissionToRetain: true,
    opportunityIds: ["opp-b"],
    hotelIds: ["hotel_b"],
    sourceUrl: "https://rivera-meetings.example/team/alex",
    origin: "PUBLIC_SOURCE",
    channels: {
      [CHANNEL_KIND.VERIFIED_DIRECT_WORK_EMAIL]: {
        kind: CHANNEL_KIND.VERIFIED_DIRECT_WORK_EMAIL,
        value: "alex.rivera@rivera-meetings.example",
        origin: "PUBLIC_SOURCE",
      },
    },
  });
  assert.equal(again.ok, true);
  assert.equal(again.created, false);
  const people = store.listPeople().filter((p) => p.personIdentityKey === key);
  assert.equal(people.length, 1, "no duplicate person");
  assert.deepEqual(people[0].associations.hotelIds.sort(), ["hotel_a", "hotel_b"]);
  assert.deepEqual(people[0].associations.opportunityIds.sort(), ["opp-a", "opp-b"]);
});

await check("provider origin refused by durable store", () => {
  const store = createDurableContactStore({
    storePath: path.join(tmpDir, "provider-refuse.json"),
  });
  const r = store.upsertPerson({
    name: "Pat Provider",
    organization: "Acme Events",
    origin: "SURFE_PROVIDER_EPHEMERAL",
    permissionToRetain: true,
    channels: {
      [CHANNEL_KIND.VERIFIED_DIRECT_WORK_EMAIL]: {
        value: "pat@acme.example",
        origin: "SURFE",
      },
    },
  });
  assert.equal(r.ok, false);
  assert.equal(r.reason, "PROVIDER_DATA_PERSISTENCE_PROHIBITED");
});

await check("cross-hotel process-local reuse vs durable cross-week reuse", () => {
  clearPublicContactResearchCacheForTests();
  const name = "Blair Chen";
  const organization = "Chen Housing Bureau";
  const key = publicContactPersonKey({ name, organization });

  // Process-local only
  putCachedPublicContactResearch(key, {
    hotelId: "hotel_1",
    found: {
      email: "blair@chen-housing.example",
      phone: "+1-202-555-0199",
      sourceUrl: "https://chen-housing.example/staff/blair",
    },
  });
  const local = resolveReusableContactIntelligence({ name, organization });
  assert.equal(local.ok, true);
  assert.equal(local.layer, REUSE_LAYER.PROCESS_LOCAL_CACHE);
  assert.equal(local.processLocalHit, true);
  assert.equal(local.durableHit, false);

  // New "week": clear process cache; load durable store from disk
  clearPublicContactResearchCacheForTests();
  const harness = createCrossWeekReuseHarness({
    storePath: path.join(tmpDir, "week2.json"),
  });
  harness.persistVerified({
    name,
    organization,
    role: "Housing Manager",
    sourceUrl: "https://chen-housing.example/staff/blair",
    origin: "PUBLIC_SOURCE",
    permissionToRetain: true,
    permissionToDisplayToHotelClients: true,
    hotelIds: ["hotel_1"],
    opportunityIds: ["opp-1"],
    channels: {
      [CHANNEL_KIND.VERIFIED_DIRECT_WORK_EMAIL]: {
        kind: CHANNEL_KIND.VERIFIED_DIRECT_WORK_EMAIL,
        value: "blair@chen-housing.example",
        origin: "PUBLIC_SOURCE",
      },
      [CHANNEL_KIND.VERIFIED_DIRECT_BUSINESS_PHONE]: {
        kind: CHANNEL_KIND.VERIFIED_DIRECT_BUSINESS_PHONE,
        value: "+1-202-555-0199",
        origin: "PUBLIC_SOURCE",
      },
    },
  });
  // Simulate subsequent weekly run: new process cache empty, durable file reload
  clearPublicContactResearchCacheForTests();
  const week2 = createCrossWeekReuseHarness({
    storePath: path.join(tmpDir, "week2.json"),
  });
  const durable = week2.resolve({ name, organization });
  assert.equal(durable.ok, true);
  assert.equal(durable.layer, REUSE_LAYER.DURABLE_FIXTURE_STORE);
  assert.equal(durable.durableHit, true);
  assert.equal(durable.processLocalHit, false);
  assert.equal(durable.found.email, "blair@chen-housing.example");
});

await check("employer change invalidates prior key and clears channels", () => {
  const store = createDurableContactStore({
    storePath: path.join(tmpDir, "employer-change.json"),
  });
  const name = "Casey Ng";
  const oldOrg = "Old Org LLC";
  const newOrg = "New Org Inc";
  const priorKey = publicContactPersonKey({ name, organization: oldOrg });
  store.upsertPerson({
    name,
    organization: oldOrg,
    permissionToRetain: true,
    sourceUrl: "https://old.example/team",
    origin: "PUBLIC_SOURCE",
    channels: {
      [CHANNEL_KIND.VERIFIED_DIRECT_WORK_EMAIL]: {
        kind: CHANNEL_KIND.VERIFIED_DIRECT_WORK_EMAIL,
        value: "casey@old.example",
        origin: "PUBLIC_SOURCE",
      },
    },
  });
  const changed = store.recordEmployerChange({
    priorPersonIdentityKey: priorKey,
    name,
    newOrganization: newOrg,
    permissionToRetain: true,
  });
  assert.equal(changed.ok, true);
  assert.equal(changed.priorInvalidated, true);
  assert.equal(changed.channelsCleared, true);
  assert.notEqual(changed.newPersonIdentityKey, priorKey);
  assert.equal(store.getPerson(priorKey).invalid, true);
  const successor = store.getPerson(changed.newPersonIdentityKey);
  assert.equal(successor.organization, newOrg);
  assert.equal(
    Object.keys(successor.channels || {}).length,
    0,
    "must re-verify channels under new employer",
  );
});

await check("expiry + correction + deletion + audit history", () => {
  const store = createDurableContactStore({
    storePath: path.join(tmpDir, "lifecycle.json"),
  });
  const name = "Drew Patel";
  const organization = "Patel Events";
  const key = publicContactPersonKey({ name, organization });
  store.upsertPerson({
    name,
    organization,
    permissionToRetain: true,
    sourceUrl: "https://patel.example/team",
    origin: "PUBLIC_SOURCE",
    verifiedAt: "2020-01-01T00:00:00.000Z",
    revalidationRequiredAfterDays: 30,
    channels: {
      [CHANNEL_KIND.VERIFIED_DIRECT_WORK_EMAIL]: {
        kind: CHANNEL_KIND.VERIFIED_DIRECT_WORK_EMAIL,
        value: "drew@patel.example",
        origin: "PUBLIC_SOURCE",
      },
    },
  });
  const expired = store.expireStale(new Date("2026-11-23T00:00:00.000Z"));
  assert.equal(expired.expired >= 1, true);
  assert.equal(store.getPerson(key).invalidReason, "EVIDENCE_STALE_REVALIDATION_REQUIRED");

  // Fresh recreate for correction/delete path
  const key2 = publicContactPersonKey({
    name: "Evan Soto",
    organization: "Soto Meetings",
  });
  store.upsertPerson({
    name: "Evan Soto",
    organization: "Soto Meetings",
    permissionToRetain: true,
    sourceUrl: "https://soto.example/team",
    origin: "PUBLIC_SOURCE",
    verifiedAt: new Date().toISOString(),
    channels: {
      [CHANNEL_KIND.VERIFIED_DIRECT_WORK_EMAIL]: {
        kind: CHANNEL_KIND.VERIFIED_DIRECT_WORK_EMAIL,
        value: "evan@soto.example",
        origin: "PUBLIC_SOURCE",
      },
    },
  });
  const corrected = store.correctChannel(
    key2,
    CHANNEL_KIND.VERIFIED_DIRECT_WORK_EMAIL,
    {
      kind: CHANNEL_KIND.VERIFIED_DIRECT_WORK_EMAIL,
      value: "evan.soto@soto.example",
      origin: "PUBLIC_SOURCE",
    },
  );
  assert.equal(corrected.ok, true);
  const del = store.deletePerson(key2, "TEST_DELETE");
  assert.equal(del.ok, true);
  assert.equal(store.getPerson(key2), null);
  const actions = store.getAuditLog().map((a) => a.action);
  assert.ok(actions.includes("CREATE"));
  assert.ok(actions.includes("EXPIRE"));
  assert.ok(actions.includes("CORRECT_CHANNEL"));
  assert.ok(actions.includes("DELETE"));
});

await check("cross-opportunity association without duplicate person", () => {
  const store = createDurableContactStore({
    storePath: path.join(tmpDir, "assoc.json"),
  });
  const name = "Fran Okada";
  const organization = "Okada Bureau";
  const key = publicContactPersonKey({ name, organization });
  store.upsertPerson({
    name,
    organization,
    permissionToRetain: true,
    sourceUrl: "https://okada.example/team",
    origin: "PUBLIC_SOURCE",
    opportunityIds: ["opp-1"],
    hotelIds: ["hotel_1"],
    channels: {
      [CHANNEL_KIND.VERIFIED_DIRECT_BUSINESS_PHONE]: {
        kind: CHANNEL_KIND.VERIFIED_DIRECT_BUSINESS_PHONE,
        value: "+1-202-555-0111",
        origin: "PUBLIC_SOURCE",
      },
    },
  });
  store.associateOpportunity(key, {
    opportunityId: "opp-2",
    hotelId: "hotel_2",
  });
  assert.equal(store.listPeople().length, 1);
  assert.deepEqual(store.getPerson(key).associations.opportunityIds.sort(), [
    "opp-1",
    "opp-2",
  ]);
});

console.log(`\nOK ${passed} checks — contact intelligence v2.2 durable foundation`);
