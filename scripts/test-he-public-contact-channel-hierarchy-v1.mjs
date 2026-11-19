/**
 * HE-QL-032 — Public contact hierarchy + Surfe session-only gates.
 * No live Surfe / Airtable writes.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  CONTACT_DISPLAY_LABELS,
  buildHePersonChannelDisplay,
  looksLikeInventedEmail,
  proposeHePublicContactUpsert,
  resolveEnrichAction,
  selectDirectEmails,
} from "../lib/hotel-intelligence/contact-intelligence/he-public-contact-channel-hierarchy-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const read = (rel) => fs.readFileSync(path.join(ROOT, rel), "utf8");

function mustInclude(hay, needle) {
  assert.ok(hay.includes(needle), `expected to include: ${needle}`);
}

function mustNotMatch(hay, re) {
  assert.ok(!re.test(hay), `expected not to match: ${re}`);
}

// A — public email + phone → no Surfe CTA
{
  const person = {
    name: "Gissella Palma",
    organization: "Euroguss",
    emailContacts: [
      { value: "gissella.palma@nmna.us", verification_status: "VERIFIED_PUBLIC", source: "official_web" },
    ],
    phoneContacts: [
      { value: "+1 (770) 618-5833", verification_status: "VERIFIED_PUBLIC", source: "official_web" },
    ],
  };
  const d = buildHePersonChannelDisplay(person);
  assert.equal(d.demoState, "A");
  assert.equal(d.emailLabel, CONTACT_DISPLAY_LABELS.directEmail);
  assert.equal(d.phoneLabel, CONTACT_DISPLAY_LABELS.directPhone);
  assert.equal(d.enrichAction.label, null);
  assert.equal(d.enrichAction.enabled, false);
}

// B — email only → Find Phone
{
  const person = {
    name: "Dana Torres",
    organization: "CX Forum",
    emailContacts: [{ value: "dana@example.com" }],
    phoneContacts: [],
  };
  const a = resolveEnrichAction(person, { providerReady: true });
  assert.equal(a.label, CONTACT_DISPLAY_LABELS.findPhone);
  assert.equal(a.channel, "phone");
  assert.equal(a.enabled, true);
  assert.equal(a.demoState, "B");
}

// C — phone only → Find Email
{
  const person = {
    name: "Alex Rivera",
    organization: "Example Co",
    emailContacts: [],
    phoneContacts: [{ value: "+1 555 0100" }],
  };
  const a = resolveEnrichAction(person, { providerReady: true });
  assert.equal(a.label, CONTACT_DISPLAY_LABELS.findEmail);
  assert.equal(a.channel, "email");
  assert.equal(a.demoState, "C");
}

// D — named + org fallback only → Find Email & Phone
{
  const person = {
    name: "Katherine Button",
    organization: "Mullen Real Estate Capital",
    contactClass: "RELEVANT_COMMERCIAL_ENTRY_POINT",
    emailContacts: [],
    phoneContacts: [],
  };
  const d = buildHePersonChannelDisplay(person, {
    emails: [{ value: "info@mullencap.com", classification: "official_org" }],
    website: "https://mullencap.com/contact/",
  });
  assert.ok(d.organizationalRoutes.length >= 1);
  assert.equal(d.organizationalRoutes[0].label, CONTACT_DISPLAY_LABELS.orgEmail);
  assert.equal(d.enrichAction.label, CONTACT_DISPLAY_LABELS.findBoth);
  assert.equal(selectDirectEmails(person).length, 0);
}

// E — influencer classification without inventing signing authority
{
  const d = buildHePersonChannelDisplay({
    name: "Patrick Reece",
    organization: "Mullen Real Estate Capital",
    title: "Director, Asset Management",
    contactClass: "RELEVANT_COMMERCIAL_ENTRY_POINT",
  });
  assert.equal(d.roleClassification.signingAuthorityClaimed, false);
  assert.ok(
    d.demoState === "E" || d.demoState === "D",
    "influencer / named without direct channels"
  );
}

// Invented emails rejected
assert.equal(looksLikeInventedEmail("jeff.mullen@mullencap.com", "Jeff Mullen"), true);
assert.equal(looksLikeInventedEmail("jmullen@mullencap.com", "Jeff Mullen"), true);
assert.equal(looksLikeInventedEmail("info@mullencap.com", "Jeff Mullen"), false);

// Org mislabel must not become direct
{
  const person = {
    name: "Katherine Button",
    organization: "Mullen Real Estate Capital",
    emailContacts: [
      {
        value: "info@mullencap.com",
        classification: "official_org",
        note: "Organization channel",
      },
    ],
  };
  assert.equal(selectDirectEmails(person).length, 0);
  const a = resolveEnrichAction(person, { providerReady: true });
  assert.equal(a.label, CONTACT_DISPLAY_LABELS.findBoth);
}

// G — upsert dry-run never includes Surfe
{
  const upsert = proposeHePublicContactUpsert({
    name: "Jeff Mullen",
    organization: "Mullen Real Estate Capital",
    title: "President and CEO",
    linkedinUrl: "https://www.linkedin.com/in/jeffmmullen/",
    lastVerified: "2026-11-19",
  });
  assert.ok(upsert);
  assert.equal(upsert.execute, false);
  assert.equal(upsert.applyAuthorized, false);
  assert.equal(upsert.fields.surfeDerived, false);
  assert.ok(upsert.fields.linkedInUrl.includes("jeffmmullen"));
}

// F / persistence — API + client forbid Surfe writes
{
  const api = read("api/contact-intelligence.js");
  mustInclude(api, "persist_forbidden");
  mustInclude(api, "ephemeral");
  mustInclude(api, "NOT_WRITTEN");
  mustNotMatch(api, /base\(['\"]Contacts['\"]\)\.(create|update)|airtable.*\.create\(/i);
  mustInclude(api, "Never persists to Airtable");
  const hci = read("public/js/hotel-contact-intelligence.js");
  mustInclude(hci, "Direct Business Email");
  mustInclude(hci, "Direct Business Phone");
  mustInclude(hci, "Organizational Contact Route");
  mustInclude(hci, "Find Phone");
  mustInclude(hci, "Find Email");
  mustInclude(hci, "session-only");
  mustInclude(hci, "_sessionSurfeByPerson");
  mustInclude(hci, 'persist: false');
  mustNotMatch(hci, /localStorage\.setItem\([^\)]*surfe/i);
}

// Bethesda research package — public gaps + org routes, no invented directs
{
  const research = JSON.parse(
    read("reports/hotel-explorer/phase4-quality/HE-BETH-OWNER-DECISION-MAKER-RESEARCH-STAGED-V1.json")
  );
  assert.equal(research.applyAuthorized, false);
  assert.equal(research.surfeCallsAuthorized, false);
  for (const p of research.people) {
    assert.equal((p.emailContacts || []).length, 0);
    assert.equal((p.phoneContacts || []).length, 0);
    assert.ok(p.linkedinUrl.includes("linkedin.com/in/"));
  }
  assert.ok(
    research.corporateChannels.some((c) => c.value === "info@mullencap.com")
  );
  assert.ok(
    research.corporateChannels.some((c) => String(c.value).includes("610") && c.kind === "phone")
  );
  const upserts = JSON.parse(
    read("reports/hotel-explorer/phase4-quality/HE-BETH-PUBLIC-CONTACT-UPSERTS-DRY-RUN-V1.json")
  );
  assert.equal(upserts.execute, false);
  assert.equal(upserts.surfeDerivedIncluded, false);
  assert.ok(upserts.count >= 3);
}

// Cross-product label parity with GDI founder directive
assert.equal(CONTACT_DISPLAY_LABELS.directEmail, "Direct Business Email");
assert.equal(CONTACT_DISPLAY_LABELS.findBoth, "Find Email & Phone");

console.log("PASS test:he-public-contact-channel-hierarchy-v1");
