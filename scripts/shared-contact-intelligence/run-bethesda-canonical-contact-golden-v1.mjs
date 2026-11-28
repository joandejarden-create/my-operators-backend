#!/usr/bin/env node
/**
 * Bethesda Marriott golden-reference — Shared Canonical Contact Intelligence V1.
 * Hotel: recLuxvwwxID7U2B8
 *
 * Does not fabricate contacts or force verification upgrades.
 * Does not execute paid Findymail (reuse / audit only).
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  BETHESDA_GOLDEN,
  resolveCanonicalHotelContacts,
  runCanonicalContactConsistencyGates,
  computeSharedContactability,
  assertGdiHeChannelParity,
  toGdiContactPresentation,
  toHeContactDirectoryRow,
  planFindymailLookup,
  loadPersonIdMap,
} from "../../lib/shared-contact-intelligence/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const hotelId = BETHESDA_GOLDEN.hotelId;

const hotelResult = await resolveCanonicalHotelContacts(hotelId, {
  updateIdentityMap: true,
});
const gates = await runCanonicalContactConsistencyGates(hotelId, { hotelResult });
const map = loadPersonIdMap();
const contactability = computeSharedContactability(hotelResult.people || [], {
  hotelId,
});

const expectedNames = BETHESDA_GOLDEN.professionals.map((n) => n.toLowerCase());
const recovered = [];

for (const expected of BETHESDA_GOLDEN.professionals) {
  const person = (hotelResult.people || []).find(
    (p) => String(p.displayName || "").toLowerCase() === expected.toLowerCase(),
  );
  const mapEntry = (map.entries || []).find(
    (e) => String(e.displayName || "").toLowerCase() === expected.toLowerCase(),
  );
  const he = person ? toHeContactDirectoryRow(person) : null;
  const gdi = person ? toGdiContactPresentation(person) : null;
  const parity = person
    ? assertGdiHeChannelParity(
        {
          canonicalPersonId: person.canonicalPersonId,
          email: person.email,
          phone: person.phone,
          linkedinUrl: person.linkedinUrl,
        },
        {
          canonicalPersonId: gdi.canonicalPersonId,
          email: gdi.contactDetails.email,
          phone: gdi.contactDetails.phone,
          linkedinUrl: gdi.contactDetails.linkedinUrl,
        },
      )
    : { ok: false, status: "FAIL", reason: "PERSON_NOT_IN_RESOLVER" };

  const findymailPlan = person
    ? planFindymailLookup(person, { allowPaid: false, authorizationId: null })
    : null;

  recovered.push({
    expectedName: expected,
    found: Boolean(person),
    canonicalPersonId: person?.canonicalPersonId || mapEntry?.canonicalPersonId || null,
    hiAirtableId: person?.hiAirtableId || mapEntry?.hiAirtableId || null,
    gdiPersonIdentityKey:
      person?.gdiPersonIdentityKey || mapEntry?.gdiPersonIdentityKey || null,
    email: person?.email || null,
    phone: person?.phone || null,
    linkedinUrl: person?.linkedinUrl || null,
    channels: person?.channels || [],
    heDisplay: he,
    gdiDisplay: gdi,
    parity,
    findymailPlan,
    duplicatePaidLookupRequired: findymailPlan
      ? findymailPlan.executePaidLookup === true &&
        !(person?.email && /@/.test(person.email))
      : null,
  });
}

const report = {
  version: "bethesda-canonical-contact-golden-v1",
  hotelId,
  hotelName: BETHESDA_GOLDEN.hotelName,
  generatedAt: new Date().toISOString(),
  airtableLive: Boolean(hotelResult.airtableLive),
  expectedProfessionals: BETHESDA_GOLDEN.professionals,
  recovered,
  missingExpected: expectedNames.filter(
    (n) =>
      !(hotelResult.people || []).some(
        (p) => String(p.displayName || "").toLowerCase() === n,
      ),
  ),
  duplicatePersonRowsCollapsed: hotelResult.duplicatePersonRowsCollapsed || 0,
  contactability,
  consistencyGates: gates,
  newChannelsPersistedThisRun: [],
  findymailSpendThisRunUsd: 0,
  note: "Audit/read-back only — no paid Findymail in golden run.",
};

const outDir = path.join(ROOT, "reports", "shared-contact-intelligence");
fs.mkdirSync(outDir, { recursive: true });
const outPath = path.join(outDir, "BETHESDA-CANONICAL-CONTACT-GOLDEN-V1.json");
fs.writeFileSync(outPath, JSON.stringify(report, null, 2) + "\n", "utf8");

console.log(
  JSON.stringify(
    {
      ok: true,
      hotelId,
      airtableLive: report.airtableLive,
      recovered: recovered.filter((r) => r.found).length,
      withEmail: recovered.filter((r) => r.email).length,
      overallGates: gates.overall,
      outPath,
    },
    null,
    2,
  ),
);

if (gates.overall === "FAIL") process.exit(1);
process.exit(0);
