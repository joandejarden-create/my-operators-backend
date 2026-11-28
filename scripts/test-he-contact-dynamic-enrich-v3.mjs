#!/usr/bin/env node
/**
 * HE Contact Dynamic Enrichment V3 — permanent regression gates (mocked).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  computeMissingContactChannels,
  surfeIncludeFromMissingState,
  isEligibleVerifiedChannel,
  HE_CONTACT_MISSING_CHANNEL_V3,
  MISSING_CHANNEL_ENRICHMENT_V3,
} from "../lib/shared-contact-intelligence/missing-channel-enrichment-v3.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

const GATES = {
  CONTACT_DYNAMIC_MISSING_CHANNEL_DETECTION: "CONTACT_DYNAMIC_MISSING_CHANNEL_DETECTION",
  CONTACT_SURFE_PHONE_ONLY_REQUEST: "CONTACT_SURFE_PHONE_ONLY_REQUEST",
  CONTACT_SURFE_EMAIL_ONLY_REQUEST: "CONTACT_SURFE_EMAIL_ONLY_REQUEST",
  CONTACT_SURFE_BOTH_CHANNEL_REQUEST: "CONTACT_SURFE_BOTH_CHANNEL_REQUEST",
  CONTACT_NO_DUPLICATE_PROVIDER_CREDITS: "CONTACT_NO_DUPLICATE_PROVIDER_CREDITS",
  CONTACT_BACKEND_AIRTABLE_RECHECK: "CONTACT_BACKEND_AIRTABLE_RECHECK",
  CONTACT_PARTIAL_RESULT_HANDLING: "CONTACT_PARTIAL_RESULT_HANDLING",
  CONTACT_SURFE_SESSION_ONLY: "CONTACT_SURFE_SESSION_ONLY",
};

const results = {};
function pass(gate, detail = {}) {
  results[gate] = { status: "PASS", ...detail };
}
function fail(gate, reason) {
  results[gate] = { status: "FAIL", reason };
  throw new Error(`${gate}: ${reason}`);
}

console.log("HE Contact Dynamic Enrichment V3 gates…");

// --- Missing-channel detection matrix ---
const bothPresent = computeMissingContactChannels({
  email: "a@example.com",
  phone: "+15551212",
  linkedinUrl: "https://www.linkedin.com/in/x",
  channels: [
    { channelType: "Email", channelValue: "a@example.com", verificationStatus: "Verified Direct" },
    { channelType: "Telephone", channelValue: "+15551212", verificationStatus: "Verified Direct" },
  ],
});
assert.equal(bothPresent.needsEmail, false);
assert.equal(bothPresent.needsPhone, false);
assert.equal(bothPresent.requestChannel, "none");
assert.equal(bothPresent.buttonEnabled, false);
assert.equal(bothPresent.dimmedComplete, true);

const emailOnlyMissing = computeMissingContactChannels({
  phone: "+15551212",
  channels: [
    { channelType: "Telephone", channelValue: "+15551212", verificationStatus: "Verified Direct" },
  ],
});
assert.equal(emailOnlyMissing.needsEmail, true);
assert.equal(emailOnlyMissing.needsPhone, false);
assert.equal(emailOnlyMissing.requestChannel, "email");
assert.equal(emailOnlyMissing.contextualDescription, "Find Missing Email");

const phoneOnlyMissing = computeMissingContactChannels({
  email: "a@example.com",
  linkedinUrl: "https://www.linkedin.com/in/meinradlang/",
  channels: [
    { channelType: "Email", channelValue: "a@example.com", verificationStatus: "Verified Direct" },
    {
      channelType: "LinkedIn",
      channelValue: "https://www.linkedin.com/in/meinradlang/",
      verificationStatus: "Public Listed",
    },
  ],
});
assert.equal(phoneOnlyMissing.needsEmail, false);
assert.equal(phoneOnlyMissing.needsPhone, true);
assert.equal(phoneOnlyMissing.hasVerifiedLinkedIn, true);
assert.equal(phoneOnlyMissing.requestChannel, "phone");
assert.equal(phoneOnlyMissing.contextualDescription, "Find Missing Phone");
// LinkedIn must not substitute for phone
assert.equal(phoneOnlyMissing.linkedInIsIdentityOnly, true);

const bothMissing = computeMissingContactChannels({
  linkedinUrl: "https://www.linkedin.com/in/x",
  channels: [],
});
assert.equal(bothMissing.needsEmail, true);
assert.equal(bothMissing.needsPhone, true);
assert.equal(bothMissing.requestChannel, "both");

const noLinkedInBothMissing = computeMissingContactChannels({ channels: [] });
assert.equal(noLinkedInBothMissing.hasVerifiedLinkedIn, false);
assert.equal(noLinkedInBothMissing.requestChannel, "both");

// Session-only Surfe must not count as verified durable
const surfeSessionIgnored = computeMissingContactChannels({
  channels: [
    {
      channelType: "Email",
      channelValue: "session@example.com",
      sessionOnly: true,
      surfeDerived: true,
      verificationStatus: "PROVIDER_RETURNED_UNREVIEWED",
    },
  ],
});
assert.equal(surfeSessionIgnored.needsEmail, true);
assert.equal(isEligibleVerifiedChannel({ sessionOnly: true, channelValue: "x" }), false);

pass(GATES.CONTACT_DYNAMIC_MISSING_CHANNEL_DETECTION, {
  version: HE_CONTACT_MISSING_CHANNEL_V3,
  cases: ["both_present", "email_missing", "phone_missing", "both_missing", "li_not_substitute"],
});

// --- Surfe include mapping ---
const phoneOnlyInclude = surfeIncludeFromMissingState(phoneOnlyMissing);
assert.deepEqual(
  { email: phoneOnlyInclude.email, mobile: phoneOnlyInclude.mobile },
  { email: false, mobile: true },
);
pass(GATES.CONTACT_SURFE_PHONE_ONLY_REQUEST, { include: phoneOnlyInclude });

const emailOnlyInclude = surfeIncludeFromMissingState(emailOnlyMissing);
assert.deepEqual(
  { email: emailOnlyInclude.email, mobile: emailOnlyInclude.mobile },
  { email: true, mobile: false },
);
pass(GATES.CONTACT_SURFE_EMAIL_ONLY_REQUEST, { include: emailOnlyInclude });

const bothInclude = surfeIncludeFromMissingState(bothMissing);
assert.deepEqual(
  { email: bothInclude.email, mobile: bothInclude.mobile },
  { email: true, mobile: true },
);
pass(GATES.CONTACT_SURFE_BOTH_CHANNEL_REQUEST, { include: bothInclude });

// Phone-only must not request email credits; email-only must not request mobile.
assert.equal(phoneOnlyInclude.email, false, "phone-only must not consume email credits");
assert.equal(emailOnlyInclude.mobile, false, "email-only must not consume mobile credits");
pass(GATES.CONTACT_NO_DUPLICATE_PROVIDER_CREDITS, {
  phoneOnlySkipsEmail: true,
  emailOnlySkipsMobile: true,
});

// --- Backend Airtable recheck + Surfe session-only + partial ---
const apiSrc = fs.readFileSync(path.join(ROOT, "api/contact-intelligence.js"), "utf8");
const hybridSrc = fs.readFileSync(
  path.join(ROOT, "lib/hotel-intelligence/contact-intelligence/he-contact-hybrid-enrich-v1.js"),
  "utf8",
);
const jsSrc = fs.readFileSync(
  path.join(ROOT, "public/js/hotel-contact-intelligence.js"),
  "utf8",
);
const missingSrc = fs.readFileSync(
  path.join(
    ROOT,
    "lib/shared-contact-intelligence/missing-channel-enrichment-v3.js",
  ),
  "utf8",
);

assert.match(apiSrc, /resolveCanonicalPersonChannels/);
assert.match(apiSrc, /computeMissingContactChannels/);
assert.match(apiSrc, /requestedInclude/);
assert.match(apiSrc, /needsEmail/);
assert.match(apiSrc, /includeEmail: surfeInclude\.email/);
assert.match(apiSrc, /includeMobile: surfeInclude\.mobile/);
assert.match(hybridSrc, /AIRTABLE_RECHECK/);
assert.match(hybridSrc, /AIRTABLE_RECHECK_AFTER/);
assert.match(hybridSrc, /computeMissingContactChannels/);
pass(GATES.CONTACT_BACKEND_AIRTABLE_RECHECK, { api: true, hybrid: true });

assert.match(hybridSrc, /Partial durable channels/);
assert.match(jsSrc, /surfeFallbackAvailable/);
assert.match(jsSrc, /missingChannelState/);
assert.match(apiSrc, /surfe_pending/);
pass(GATES.CONTACT_PARTIAL_RESULT_HANDLING, { pendingDistinct: true });

assert.match(apiSrc, /persist_forbidden|persist === true/);
assert.match(apiSrc, /NOT_WRITTEN/);
assert.match(apiSrc, /Surfe · Session Only/);
assert.match(jsSrc, /Surfe · Session Only|data-surfe-session/);
assert.match(missingSrc, /sessionOnly/);
assert.doesNotMatch(apiSrc, /meinrad\.lang@marriott\.com/i);
pass(GATES.CONTACT_SURFE_SESSION_ONLY, { persistForbidden: true });

// UI: approved label constant + contextual title for phone-only
assert.match(jsSrc, /Find Missing Phone/);
assert.match(jsSrc, /Find Missing Email/);
assert.match(jsSrc, /FIND_LABEL = "Find Email & Phone"/);
assert.match(jsSrc, /data-needs-email/);
assert.match(jsSrc, /data-needs-phone/);
assert.match(jsSrc, /hci-btn--enrich-dimmed/);

const outDir = path.join(
  ROOT,
  "reports/hotel-explorer/he-contact-dynamic-enrich-v3",
);
fs.mkdirSync(outDir, { recursive: true });
const report = {
  at: new Date().toISOString(),
  version: HE_CONTACT_MISSING_CHANNEL_V3,
  gates: results,
};
fs.writeFileSync(path.join(outDir, "DYNAMIC_ENRICH_V3_GATES.json"), JSON.stringify(report, null, 2));
console.log(JSON.stringify(report, null, 2));
console.log("PASS — HE Contact Dynamic Enrichment V3 gates");
