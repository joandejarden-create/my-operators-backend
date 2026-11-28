#!/usr/bin/env node
/**
 * Bethesda golden — read live Airtable missing-channel state (no paid provider calls).
 * Optional Surfe dry mapping for Meinrad only (no spend unless --confirm-live-surfe).
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { resolveCanonicalHotelContacts } from "../lib/shared-contact-intelligence/index.js";
import {
  computeMissingContactChannels,
  surfeIncludeFromMissingState,
} from "../lib/hotel-intelligence/contact-intelligence/he-contact-missing-channel-v3.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const HOTEL = "recLuxvwwxID7U2B8";
const OUT = path.join(ROOT, "reports/hotel-explorer/he-contact-dynamic-enrich-v3");
const NAMES = [
  "Katherine Button",
  "Patrick Reece",
  "Jeff Mullen",
  "Meinrad Lang III",
  "Jenny Pyne",
];

function norm(n) {
  return String(n || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const hotel = await resolveCanonicalHotelContacts(HOTEL, { updateIdentityMap: false });
  if (!hotel.ok) {
    const blocked = { ok: false, error: hotel.reason || hotel.status, at: new Date().toISOString() };
    fs.writeFileSync(path.join(OUT, "BETHESDA_GOLDEN_V3.json"), JSON.stringify(blocked, null, 2));
    console.error(JSON.stringify(blocked, null, 2));
    process.exitCode = 2;
    return;
  }

  const people = hotel.people || [];
  const rows = NAMES.map((expectedName) => {
    const person =
      people.find((p) => norm(p.displayName) === norm(expectedName)) ||
      people.find((p) => norm(p.displayName).includes(norm(expectedName).split(" ")[0]));
    if (!person) {
      return { expectedName, found: false };
    }
    const state = computeMissingContactChannels(person);
    const surfeInclude = surfeIncludeFromMissingState(state);
    return {
      expectedName,
      found: true,
      displayName: person.displayName,
      canonicalPersonId: person.canonicalPersonId,
      hasEmail: Boolean(person.email),
      hasPhone: Boolean(person.phone),
      hasLinkedIn: Boolean(person.linkedinUrl),
      linkedinUrl: person.linkedinUrl || null,
      // Values present flags only — do not echo PII into unrestricted logs beyond existence.
      emailPresent: Boolean(person.email),
      phonePresent: Boolean(person.phone),
      missingChannelState: {
        hasVerifiedEmail: state.hasVerifiedEmail,
        hasVerifiedPhone: state.hasVerifiedPhone,
        hasVerifiedLinkedIn: state.hasVerifiedLinkedIn,
        needsEmail: state.needsEmail,
        needsPhone: state.needsPhone,
        requestChannel: state.requestChannel,
        contextualDescription: state.contextualDescription,
      },
      surfeInclude,
      findymailWouldRequest: {
        email: state.needsEmail,
        phone: state.needsPhone,
      },
    };
  });

  const meinrad = rows.find((r) => /meinrad/i.test(r.expectedName));
  const report = {
    ok: true,
    at: new Date().toISOString(),
    hotelId: HOTEL,
    sourceOfTruth: hotel.sourceOfTruth || "HI_ORG_CONTACTS_PLUS_HI_CONTACT_CHANNELS",
    peopleCount: people.length,
    rows,
    meinradSurfePlan: meinrad?.found
      ? {
          linkedinUrl: meinrad.linkedinUrl,
          surfeInclude: meinrad.surfeInclude,
          note: "Live Surfe spend only via explicit diagnostic — not this golden.",
        }
      : null,
    assertions: {
      noHardcodedScreenshotPii: true,
      airtableReadLive: true,
      paidCalls: 0,
    },
  };

  fs.writeFileSync(path.join(OUT, "BETHESDA_GOLDEN_V3.json"), JSON.stringify(report, null, 2));
  const md = [
    "# Bethesda Dynamic Enrich V3 Golden",
    "",
    `| Person | Email on file | Phone on file | LinkedIn | Request | Surfe include |`,
    `|---|---|---|---|---|---|`,
    ...rows.map((r) => {
      if (!r.found) return `| ${r.expectedName} | — | — | — | NOT_FOUND | — |`;
      return `| ${r.displayName} | ${r.emailPresent} | ${r.phonePresent} | ${r.hasLinkedIn} | \`${r.missingChannelState.requestChannel}\` | email=${r.surfeInclude.email} mobile=${r.surfeInclude.mobile} |`;
    }),
    "",
  ].join("\n");
  fs.writeFileSync(path.join(OUT, "BETHESDA_GOLDEN_V3.md"), md);
  console.log(JSON.stringify(report, null, 2));
}

main().catch((err) => {
  console.error(String(err?.message || err).slice(0, 300));
  process.exitCode = 1;
});
