#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  buildHotelContactBestEffortLedger,
  evaluateContactBestEffortCertificationGate,
  buildContactChannelLedgerForPerson,
  CONTACT_CHANNEL_RESEARCH_STATUS,
  GDI_CONTACT_RESEARCH_BEST_EFFORT_V1,
} from "../lib/group-demand-intelligence/workflow/gdi-contact-research-best-effort-v1.js";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/index.js";

assert.equal(GDI_CONTACT_RESEARCH_BEST_EFFORT_V1, "gdi-contact-research-best-effort-v1");

const suzanne = buildContactChannelLedgerForPerson({
  name: "Suzanne K.",
  title: "Global Travel & Expense Manager",
  organization: "Saronic Technologies",
  linkedinUrl: "https://linkedin.com/in/suzanne-k-549a9822",
  identityStatus: "PARTIAL_IDENTITY_ABBREVIATED_SURNAME",
});
assert.equal(suzanne.identityFullyVerified, false);
assert.equal(
  suzanne.channels.email.status,
  CONTACT_CHANNEL_RESEARCH_STATUS.IDENTITY_RESOLUTION_REQUIRED,
);
assert.ok(suzanne.channels.linkedin.value);

const lily = buildContactChannelLedgerForPerson({
  name: "Lily Yuan",
  email: "lily.yuan@borabiologics.com",
  linkedinUrl: "https://linkedin.com/in/lilyyuan26",
  title: "Senior Director, Supply Chain Management",
  organization: "Bora Biologics",
});
assert.equal(lily.identityFullyVerified, true);
assert.equal(
  lily.channels.email.status,
  CONTACT_CHANNEL_RESEARCH_STATUS.VERIFIED_CONTACT_INFORMATION_FOUND,
);
assert.equal(lily.findymail.eligible, false);

const store = createGdiPublicationPackageStore({});
const ptr = store.readPointer("recnPB3Q02NO8Ma1o", "STAGED");
const loaded = store.loadPackage("recnPB3Q02NO8Ma1o", ptr.publicationVersion);
assert.equal(loaded.ok, true);
const ledger = buildHotelContactBestEffortLedger(loaded.package.opportunities, {
  hotelId: "recnPB3Q02NO8Ma1o",
});
assert.ok(ledger.scorecard.opportunityCount >= 26);
assert.ok(ledger.scorecard.uniqueCommercialContacts >= 20);
const gate = evaluateContactBestEffortCertificationGate(ledger);
assert.equal(typeof gate.ok, "boolean");
assert.ok(Array.isArray(gate.failures));
console.log(
  JSON.stringify(
    {
      ok: true,
      gateOk: gate.ok,
      failures: gate.failures,
      email: ledger.scorecard.emailVerifiedCoverage,
      phone: ledger.scorecard.telephoneVerifiedCoverage,
      linkedin: ledger.scorecard.linkedinVerifiedCoverage,
    },
    null,
    2,
  ),
);
console.log("OK test:gdi-contact-research-best-effort-v1");
