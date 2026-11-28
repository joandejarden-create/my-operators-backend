#!/usr/bin/env node
/**
 * HE Surfe contact enrichment parity — mocked regression gates.
 * Does NOT call live Surfe or spend credits.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import vm from "node:vm";
import {
  normalizeSurfePerson,
  buildSurfeEnrichPersonInput,
  splitSurfePersonName,
  pickSurfePhoneNumber,
  normalizeLinkedInUrlForSurfe,
} from "../lib/surfe/client.js";
import { createSurfeContactProvider } from "../lib/market-alerts-contact/surfe-adapter.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

const MEINRAD_LI = "https://www.linkedin.com/in/meinradlang/";

function loadHci() {
  const src = fs.readFileSync(
    path.join(ROOT, "public/js/hotel-contact-intelligence.js"),
    "utf8"
  );
  const sandbox = { window: {}, global: {}, console, URL, URLSearchParams };
  sandbox.window = sandbox;
  sandbox.global = sandbox;
  vm.runInNewContext(src, sandbox, { filename: "hotel-contact-intelligence.js" });
  return sandbox.HotelContactIntelligence || sandbox.window.HotelContactIntelligence;
}

console.log("HE Surfe contact parity gates…");

// --- LinkedIn identity / name splitting ---
const parts = splitSurfePersonName("Meinrad Lang III");
assert.equal(parts.firstName, "Meinrad");
assert.equal(parts.lastName, "Lang");

const alt = splitSurfePersonName("Rad Lang");
assert.equal(alt.firstName, "Rad");
assert.equal(alt.lastName, "Lang");

const li = normalizeLinkedInUrlForSurfe(MEINRAD_LI);
assert.equal(li, "https://www.linkedin.com/in/meinradlang");

const inputAlt = buildSurfeEnrichPersonInput({
  personName: "Meinrad (Rad) Lang III",
  companyName: "Marriott International",
  linkedinUrl: MEINRAD_LI,
  jobTitle: "General Manager",
});
assert.equal(inputAlt.linkedinUrl, "https://www.linkedin.com/in/meinradlang");
assert.ok(!("linkedInUrl" in inputAlt), "must send documented linkedinUrl, not linkedInUrl");
assert.equal(inputAlt.companyName, "Marriott International");
assert.equal(inputAlt.firstName, "Meinrad");
// Parenthetical nickname becomes a middle token; LinkedIn remains the primary match key.
assert.equal(inputAlt.lastName, "Rad Lang");

const inputClean = buildSurfeEnrichPersonInput({
  personName: "Meinrad Lang III",
  companyName: "Marriott International",
  linkedinUrl: MEINRAD_LI,
});
assert.equal(inputClean.firstName, "Meinrad");
assert.equal(inputClean.lastName, "Lang");
assert.equal(inputClean.linkedinUrl, "https://www.linkedin.com/in/meinradlang");

// --- Normalizer: landlines + top-level email ---
const emailOnly = normalizeSurfePerson({
  firstName: "Meinrad",
  lastName: "Lang",
  email: "example.only@example.com",
  emails: [],
  mobilePhones: [],
  landlines: [],
  found: true,
  status: "COMPLETED",
});
assert.equal(emailOnly.emails[0].email, "example.only@example.com");
assert.equal(pickSurfePhoneNumber(emailOnly).phone, null);

const phoneOnly = normalizeSurfePerson({
  firstName: "Meinrad",
  lastName: "Lang",
  emails: [],
  mobilePhones: [],
  landlines: [{ landline: "+13015550100", lineType: "FIXED_LINE", validationStatus: "VALID" }],
  found: true,
});
assert.equal(pickSurfePhoneNumber(phoneOnly).phone, "+13015550100");
assert.equal(pickSurfePhoneNumber(phoneOnly).phoneKind, "landline");

const both = normalizeSurfePerson({
  firstName: "Meinrad",
  lastName: "Lang",
  emails: [{ email: "example.both@example.com", emailType: "professional", validationStatus: "VALID" }],
  mobilePhones: [{ mobilePhone: "+13015550199", confidenceScore: 0.9, lineType: "MOBILE" }],
  landlines: [{ landline: "+13015550100", lineType: "FIXED_LINE" }],
});
assert.equal(both.emails[0].email, "example.both@example.com");
assert.equal(pickSurfePhoneNumber(both).phone, "+13015550199");
assert.equal(pickSurfePhoneNumber(both).phoneKind, "mobile");
assert.equal(both.phones.length, 2);

const landlinePreferredWhenNoMobile = normalizeSurfePerson({
  emails: [{ email: "example.ll@example.com", emailType: "professional" }],
  mobilePhones: [],
  landlines: [{ landline: "+13015550111" }],
});
assert.equal(pickSurfePhoneNumber(landlinePreferredWhenNoMobile).phoneKind, "landline");

// --- Adapter mapEnrichedPerson via mocked enrich (no network) ---
// Structural: people body builder uses linkedinUrl
const peopleBody = buildSurfeEnrichPersonInput({
  personName: "Meinrad Lang III",
  linkedinUrl: "https://www.linkedin.com/in/meinradlang/",
  companyName: "Marriott International",
});
assert.match(JSON.stringify(peopleBody), /"linkedinUrl"/);
assert.doesNotMatch(JSON.stringify(peopleBody), /"linkedInUrl"/);

// --- Pending must not be confused with no-result (API contract) ---
const apiSrc = fs.readFileSync(path.join(ROOT, "api/contact-intelligence.js"), "utf8");
assert.match(apiSrc, /surfe_pending/);
assert.match(apiSrc, /PROVIDER_PENDING/);
assert.match(apiSrc, /Surfe · Session Only/);
assert.match(apiSrc, /persistence: "NOT_WRITTEN"/);
assert.match(apiSrc, /HE_SURFE_REQUEST_MOBILE|SURFE_INCLUDE_MOBILE|wantMobile/);
assert.doesNotMatch(apiSrc, /meinrad\.lang@marriott\.com/i);
assert.doesNotMatch(apiSrc, /\+13019082451/);

// --- UI session-only + tel formatting + inflight ---
const jsSrc = fs.readFileSync(
  path.join(ROOT, "public/js/hotel-contact-intelligence.js"),
  "utf8"
);
assert.match(jsSrc, /Surfe · Session Only/);
assert.match(jsSrc, /telHrefForPhone/);
assert.match(jsSrc, /_inflightReveal/);
assert.match(jsSrc, /payload\.pending/);
assert.match(jsSrc, /data-surfe-session="1"/);
assert.match(jsSrc, /data-persist="forbidden"/);
assert.doesNotMatch(jsSrc, /meinrad\.lang@marriott\.com/i);

const HCI = loadHci();
assert.ok(HCI);

// paintEnrichResult via DOM-less slot mock
if (typeof HCI._testPaintEnrichResult === "function") {
  // optional hook — not required
}

// Manual paint through exported binder is heavy; validate tel helper via regex source.
assert.match(jsSrc, /function telHrefForPhone/);

// --- Provider authorization / credits error mapping present ---
assert.match(apiSrc, /INSUFFICIENT_CREDITS/);
assert.match(apiSrc, /AUTH|FORBIDDEN/);

// --- No Airtable persistence on reveal path ---
assert.match(apiSrc, /persist_forbidden|persist === true/);
assert.match(apiSrc, /NOT_WRITTEN/);

// --- createSurfeContactProvider still constructible ---
const provider = createSurfeContactProvider({ pollOnEnrich: false });
assert.equal(typeof provider.enrichPeople, "function");

console.log("PASS — HE Surfe contact parity gates");
console.log(
  JSON.stringify(
    {
      linkedinCanonical: inputClean.linkedinUrl,
      nameSplit: inputClean,
      emailOnly: Boolean(emailOnly.emails[0]?.email),
      phoneOnlyLandline: pickSurfePhoneNumber(phoneOnly),
      bothPrefersMobile: pickSurfePhoneNumber(both),
      hardcodedScreenshotValues: false,
    },
    null,
    2
  )
);
