#!/usr/bin/env node
/**
 * Shared Canonical Contact Intelligence V1 — fixture + pure gate tests.
 * Live Airtable certification is a separate validator run.
 */

import assert from "node:assert/strict";
import {
  SHARED_CANONICAL_CONTACT_INTELLIGENCE_V1,
  CONTACT_CONSISTENCY_GATE,
  GATE_STATUS,
  scoreIdentityEvidence,
  resolveCanonicalPersonId,
  loadPersonIdMap,
  resolveCanonicalHotelContacts,
  assertGdiHeChannelParity,
  toGdiContactPresentation,
  toHeContactDirectoryRow,
  computeSharedContactability,
  planFindymailLookup,
  assertSurfeTransientOnly,
  runCanonicalContactConsistencyGates,
  BETHESDA_GOLDEN,
} from "../lib/shared-contact-intelligence/index.js";

const fixturePeople = [
  {
    airtableId: "recA",
    personId: "dle_jeff",
    displayName: "Jeff Mullen",
    organizationId: "org_mullen",
    organizationName: "Mullen Real Estate Capital",
    title: "Principal",
    verificationState: "Validated",
    notes: "https://www.linkedin.com/in/jeffmullen/",
    channels: [
      {
        airtableId: "recCh1",
        kind: "Email",
        value: "jeff.mullen@mullencap.com",
        classification: "Direct Personal",
        verificationStatus: "Verified Direct",
        provider: "findymail",
        source: "HI_CONTACT_CHANNELS",
      },
    ],
  },
  {
    airtableId: "recA2",
    personId: "dle_jeff_dup",
    displayName: "Jeff Mullen",
    organizationId: "org_mullen",
    organizationName: "Mullen Real Estate Capital",
    title: "Principal",
    verificationState: "Evidence Collected",
    notes: "",
    channels: [],
  },
  {
    airtableId: "recB",
    personId: "dle_meinrad",
    displayName: "Meinrad Lang III",
    organizationId: "org_marriott",
    organizationName: "Marriott International",
    title: "GM",
    verificationState: "Validated",
    notes: "https://www.linkedin.com/in/meinrad/",
    channels: [],
  },
];

console.log("1) constants");
assert.equal(
  SHARED_CANONICAL_CONTACT_INTELLIGENCE_V1,
  "shared-canonical-contact-intelligence-v1",
);
assert.equal(BETHESDA_GOLDEN.hotelId, "recLuxvwwxID7U2B8");

console.log("2) identity — name-only insufficient");
const nameOnly = scoreIdentityEvidence(
  { displayName: "Jeff Mullen", organizationName: "A" },
  { displayName: "Jeff Mullen", organizationName: "B" },
);
assert.equal(nameOnly.match, false);
assert.ok(nameOnly.reasons.includes("NAME_ONLY_INSUFFICIENT"));

console.log("3) identity — person id direct");
const direct = resolveCanonicalPersonId({ canonicalPersonId: "dle_jeff" });
assert.equal(direct.ok, true);
assert.equal(direct.canonicalPersonId, "dle_jeff");

console.log("4) Bethesda map seed loads");
const map = loadPersonIdMap();
assert.ok(map.entries.length >= 5);
assert.ok(map.byCanonicalPersonId["dle_06GZE1QEZWGPH6C9QSHJ2CTH1B"]);

console.log("5) fixture hotel resolve + dedupe");
const hotel = await resolveCanonicalHotelContacts("recLuxvwwxID7U2B8", {
  fixturePeople,
});
assert.equal(hotel.ok, true);
assert.equal(hotel.people.length, 2); // Jeff preferred Validated + Meinrad
const jeff = hotel.people.find((p) => p.displayName === "Jeff Mullen");
assert.equal(jeff.email, "jeff.mullen@mullencap.com");
assert.ok(jeff.linkedinUrl);

console.log("6) GDI/HE parity from same resolver");
const he = toHeContactDirectoryRow(jeff);
const gdi = toGdiContactPresentation(jeff);
const parity = assertGdiHeChannelParity(
  {
    canonicalPersonId: jeff.canonicalPersonId,
    email: he.email,
    phone: he.phone,
    linkedinUrl: he.linkedin,
  },
  {
    canonicalPersonId: gdi.canonicalPersonId,
    email: gdi.contactDetails.email,
    phone: gdi.contactDetails.phone,
    linkedinUrl: gdi.contactDetails.linkedinUrl,
  },
);
assert.equal(parity.ok, true);

console.log("7) contactability excludes empty channels; Surfe never counts");
const metrics = computeSharedContactability(hotel.people);
assert.equal(metrics.metrics.verifiedDirectEmails, 1);
assert.equal(metrics.metrics.directlyContactableProfessionals, 1);
assert.equal(metrics.metrics.directContactabilityRate, 0.5);
assert.equal(metrics.surfeExcludedFromDurable, true);

console.log("8) Findymail reuse when email present");
const plan = planFindymailLookup(jeff, { allowPaid: true, authorizationId: "X" });
assert.equal(plan.executePaidLookup, false);
assert.equal(plan.action, "REUSE_CANONICAL");

console.log("9) Surfe transient only");
const surfeOk = assertSurfeTransientOnly({
  provider: "surfe",
  sessionOnly: true,
  surfeDerived: true,
});
assert.equal(surfeOk.ok, true);
const surfeBad = assertSurfeTransientOnly({
  provider: "surfe",
  sessionOnly: false,
  persisted: true,
  airtableId: "recX",
});
assert.equal(surfeBad.ok, false);

console.log("10) gates on fixture = NOT_TESTED for Airtable SoT (not Live PASS)");
const gates = await runCanonicalContactConsistencyGates("recLuxvwwxID7U2B8", {
  hotelResult: hotel,
});
assert.equal(
  gates.gates[CONTACT_CONSISTENCY_GATE.CONTACT_AIRTABLE_SOURCE_OF_TRUTH].status,
  GATE_STATUS.NOT_TESTED,
);
assert.equal(
  gates.gates[CONTACT_CONSISTENCY_GATE.CONTACT_GDI_HE_CHANNEL_PARITY].status,
  GATE_STATUS.PASS,
);
assert.equal(
  gates.gates[CONTACT_CONSISTENCY_GATE.CONTACT_SURFE_TRANSIENT_ONLY].status,
  GATE_STATUS.PASS,
);

console.log("PASS shared-canonical-contact-intelligence-v1");
