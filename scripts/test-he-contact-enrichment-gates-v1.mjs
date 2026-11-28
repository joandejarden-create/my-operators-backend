#!/usr/bin/env node
/**
 * HE contact enrichment gates — honest SKIPPED/BLOCKED vocabulary + Bethesda audit.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  HE_CONTACT_ENRICHMENT_GATE,
  GATE_STATUS,
  auditFindymailExecutionForHotel,
  evaluateHeContactEnrichmentGates,
  buildBethesdaContactEnrichmentRecoveryReport,
} from "../lib/hotel-intelligence/he-factory/he-contact-enrichment-gates-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const factory = fs.readFileSync(
  path.join(ROOT, "lib/hotel-intelligence/he-factory/he-intelligence-factory-v1.js"),
  "utf8",
);

assert.match(factory, /HE_FINDYMAIL_EXECUTION_AUDITED/);
assert.match(factory, /CONTACT_ENRICHMENT_AUDIT/);
assert.match(factory, /gdi-findymail-stage4-enrichment-v1/);
assert.doesNotMatch(factory, /authorizeSurfe:\s*true/);

const people = [
  {
    display_name: "Jeff Mullen",
    title: "President and CEO",
    organization_name: "Mullen Real Estate Capital",
    channels: [
      { channel_kind: "PERSON_LINKEDIN", value: "https://www.linkedin.com/in/jeffmmullen/" },
      { channel_kind: "ORG_WEBSITE", value: "https://mullencap.com/" },
    ],
  },
  {
    display_name: "Jenny Pyne",
    title: "Director of Sales / Area Sales Leader",
    organization_name: "Marriott International",
    channels: [
      {
        channel_kind: "PERSON_LINKEDIN",
        value: "https://www.linkedin.com/in/jenny-pyne-9b50976/",
      },
      { channel_kind: "ORG_WEBSITE", value: "https://www.marriott.com/" },
    ],
  },
];

const audit = auditFindymailExecutionForHotel({
  hotelId: "recLuxvwwxID7U2B8",
  people,
});
assert.equal(audit.findymailInvokedForHotel, false);
assert.equal(audit.ledgerHotelRowCount, 0);
assert.ok(audit.people.every((p) => p.eligible === true));

const evaluated = evaluateHeContactEnrichmentGates({
  people,
  findymailAudit: audit,
  findymailAuthorizationStatus: "PROPOSED_NOT_AUTHORIZED",
  findymailSkipped: true,
  heFactoryFindymailWired: true,
  airtableWriteAuthorized: false,
  identityContactsPersisted: true,
  identityAirtableReadback: true,
  uiHydration: true,
});

assert.equal(
  evaluated.gates[HE_CONTACT_ENRICHMENT_GATE.HE_NAMED_CONTACTS_VERIFIED],
  GATE_STATUS.PASS,
);
assert.equal(
  evaluated.gates[HE_CONTACT_ENRICHMENT_GATE.HE_EXISTING_CHANNELS_RECOVERED],
  GATE_STATUS.PASS,
);
assert.equal(
  evaluated.gates[HE_CONTACT_ENRICHMENT_GATE.HE_FINDYMAIL_EXECUTION_AUDITED],
  GATE_STATUS.BLOCKED,
);
assert.equal(
  evaluated.gates[HE_CONTACT_ENRICHMENT_GATE.HE_CONTACTABILITY_METRIC_GENERATED],
  GATE_STATUS.PASS,
);
assert.equal(evaluated.contactabilityMetrics.contactabilityComplete, false);
assert.equal(evaluated.sharedPipeline.parallelHeEngine, false);
assert.equal(evaluated.sharedPipeline.surfeAutoInFactory, false);

const report = buildBethesdaContactEnrichmentRecoveryReport({
  people,
  authorizationStatus: "PROPOSED_NOT_AUTHORIZED",
  airtableWriteAuthorized: false,
});
assert.equal(report.founderAnswers.findymailCalledPreviousE2E, false);
assert.equal(report.stageAudit.findymailLookup, GATE_STATUS.SKIPPED);
assert.equal(report.FULL_HE_E2E_VALIDATED, "NO");
assert.ok(report.founderAnswers.eligibleContacts.includes("Jeff Mullen"));

console.log("ok - he-contact-enrichment-gates-v1");
