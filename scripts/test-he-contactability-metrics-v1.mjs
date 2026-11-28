#!/usr/bin/env node
/**
 * Contactability ≠ identification. Five named people with no durable
 * email/phone must not report contactabilityComplete.
 */
import assert from "node:assert/strict";
import {
  HE_CONTACTABILITY_METRICS_V1,
  computeContactabilityMetrics,
  formatContactabilityFactoryFields,
} from "../lib/hotel-intelligence/contact-intelligence/he-contactability-metrics-v1.js";

assert.equal(HE_CONTACTABILITY_METRICS_V1, "he-contactability-metrics-v1");

const identifiedOnly = [
  { name: "Alex Rivera", title: "Asset Manager", verificationStatus: "VERIFIED" },
  { name: "Blake Chen", title: "VP Investments", verified: true },
  { name: "Casey Ng", title: "Director of Development", confidence: "HIGH" },
  { name: "Dana Ortiz", title: "Portfolio Manager", verificationStatus: "PUBLIC" },
  {
    name: "Evan Brooks",
    title: "Partner",
    emailContacts: [
      {
        value: "evan@surfe-session.example",
        provider: "surfe",
        ephemeral: true,
        surfeDerived: true,
      },
    ],
  },
];

const m1 = computeContactabilityMetrics(identifiedOnly);
assert.equal(m1.namedContactCount, 5);
assert.ok(m1.verifiedNamedContacts >= 4);
assert.equal(m1.verifiedBusinessEmails, 0);
assert.equal(m1.verifiedDirectTelephones, 0);
assert.equal(m1.directlyContactableProfessionals, 0);
assert.equal(m1.unresolvedContactChannels, m1.relevantDecisionMakerCoverage);
assert.equal(m1.contactabilityComplete, false);
assert.equal(m1.identificationComplete, true);

const contactable = [
  {
    name: "Alex Rivera",
    title: "Asset Manager",
    verificationStatus: "VERIFIED",
    emailContacts: [{ value: "alex.rivera@mullen.example", verificationStatus: "VERIFIED" }],
    phoneContacts: [{ value: "+1 301 555 0100" }],
    linkedinUrl: "https://www.linkedin.com/in/alex-rivera-example",
  },
  {
    name: "Blake Chen",
    title: "VP Investments",
    verified: true,
    emails: [{ email: "blake.chen@mullen.example" }],
  },
  {
    name: "Info Desk",
    title: "Front Desk",
    emails: [{ value: "info@hotel.example" }],
  },
];

const m2 = computeContactabilityMetrics(contactable);
assert.equal(m2.verifiedBusinessEmails, 2);
assert.equal(m2.verifiedDirectTelephones, 1);
assert.equal(m2.verifiedLinkedInProfiles, 1);
assert.equal(m2.directlyContactableProfessionals, 2);
assert.equal(m2.contactabilityComplete, false);
assert.ok(m2.unresolvedContactChannels >= 1);

const fully = computeContactabilityMetrics([
  {
    name: "Alex Rivera",
    title: "Asset Manager",
    verificationStatus: "VERIFIED",
    emailContacts: [{ value: "alex.rivera@mullen.example" }],
  },
]);
assert.equal(fully.contactabilityComplete, true);
assert.equal(fully.directlyContactableProfessionals, 1);

const fields = formatContactabilityFactoryFields(fully);
assert.equal(fields.VerifiedDirectEmails, 1);
assert.equal(fields.SurfeUserInitiatedLookups, "N/A_UNATTENDED");
assert.equal(fields.FindymailLookups, "NOT_RUN");

console.log("ok - he-contactability-metrics-v1");
