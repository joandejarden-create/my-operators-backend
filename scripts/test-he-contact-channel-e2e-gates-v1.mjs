#!/usr/bin/env node
/**
 * Permanent HE Contact Channel E2E gates.
 * Named person + LinkedIn alone must NEVER equal complete Contact Intelligence.
 */
import assert from "node:assert/strict";
import {
  computeContactabilityMetrics,
  formatContactabilityFactoryFields,
} from "../lib/hotel-intelligence/contact-intelligence/he-contactability-metrics-v1.js";
import { runHeFactoryFindymailBridge } from "../lib/hotel-intelligence/he-factory/he-factory-findymail-bridge-v1.js";

const linkedInOnlyPeople = [
  {
    name: "Jeff Mullen",
    organization: "Mullen Real Estate Capital",
    verificationStatus: "VERIFIED",
    linkedinUrl: "https://www.linkedin.com/in/jeffmmullen/",
    channels: [
      {
        kind: "linkedin",
        value: "https://www.linkedin.com/in/jeffmmullen/",
        ephemeral: false,
      },
    ],
  },
  {
    name: "Katherine Button",
    organization: "Mullen Real Estate Capital",
    verificationStatus: "VERIFIED",
    linkedinUrl: "https://www.linkedin.com/in/katherine-button/",
    channels: [
      {
        kind: "linkedin",
        value: "https://www.linkedin.com/in/katherine-button/",
        ephemeral: false,
      },
    ],
  },
];

const before = computeContactabilityMetrics(linkedInOnlyPeople);
assert.equal(before.verifiedNamedContacts, 2);
assert.equal(before.verifiedLinkedInProfiles, 2);
assert.equal(before.directlyContactableProfessionals, 0);
assert.equal(before.verifiedDirectEmailCount, 0);
assert.equal(before.contactabilityComplete, false);
assert.equal(before.identificationComplete, true);

const fields = formatContactabilityFactoryFields(before);
assert.equal(fields.VerifiedPeople, 2);
assert.equal(fields.VerifiedDirectEmails, 0);
assert.equal(fields.DirectContactabilityRate, 0);

// Surfe/ephemeral must never count
const withSurfe = computeContactabilityMetrics([
  {
    name: "Test Person",
    organization: "Acme",
    verificationStatus: "VERIFIED",
    channels: [
      {
        kind: "email",
        value: "test@acme.com",
        provider: "surfe",
        ephemeral: true,
        surfeDerived: true,
      },
    ],
  },
]);
assert.equal(withSurfe.directlyContactableProfessionals, 0);
assert.equal(withSurfe.verifiedDirectEmailCount, 0);

// Bridge without authorization must BLOCK (no paid calls)
const bridge = await runHeFactoryFindymailBridge({
  hotelId: "recLuxvwwxID7U2B8",
  people: linkedInOnlyPeople.map((p) => ({
    ...p,
    display_name: p.name,
    organization_name: p.organization,
  })),
  authorizeFindymail: false,
  dryRun: true,
  airtableWriteAuthorized: false,
});
assert.equal(bridge.authorizeFindymail, false);
assert.equal(bridge.executedLookups, 0);
assert.equal(bridge.surfeAuto, false);
assert.ok(
  bridge.rows.every(
    (r) =>
      r.outcome === "BLOCKED_NOT_AUTHORIZED" ||
      String(r.outcome).startsWith("SKIPPED_"),
  ),
);
assert.equal(bridge.gates.HE_CONTACT_IDENTITY_VERIFIED, "PASS");
assert.equal(bridge.gates.HE_FINDYMAIL_EXECUTED, "SKIPPED");
assert.equal(bridge.gates.HE_PERSISTENCE_AUTHORIZED, "NO");
assert.equal(bridge.gates.HE_CONTACTABILITY_COMPLETE, "NO");

/** Gate matrix required in permanent reports */
const REQUIRED_GATES = [
  "HE_CONTACT_IDENTITY_VERIFIED",
  "HE_DIRECT_CHANNEL_RESEARCH_ATTEMPTED",
  "HE_FINDYMAIL_EXECUTED",
  "HE_PROVIDER_RETURNED_USABLE_DATA",
  "HE_PERSISTENCE_AUTHORIZED",
  "HE_AIRTABLE_WRITE_SUCCEEDED",
  "HE_INDEPENDENT_READBACK_SUCCEEDED",
  "HE_API_HYDRATED_CONTACT",
  "HE_CONTACT_DETAILS_CELL_CORRECT",
  "HE_CONTACTABILITY_COMPLETE",
];
for (const g of REQUIRED_GATES) {
  assert.ok(bridge.gates[g] != null, `missing gate ${g}`);
}

console.log("ok - he-contact-channel-e2e-gates-v1");
