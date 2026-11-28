#!/usr/bin/env node
/**
 * Shared Canonical Contact Intelligence validator — used by HE Factory + GDI E2E.
 * One implementation; both PowerShell runners invoke this script.
 *
 * Usage:
 *   node scripts/shared-contact-intelligence/run-shared-canonical-contact-validator-v1.mjs \
 *     --hotelId=recLuxvwwxID7U2B8 [--live] [--updateMap]
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  BETHESDA_GOLDEN,
  resolveCanonicalHotelContacts,
  runCanonicalContactConsistencyGates,
  computeSharedContactability,
  toGdiContactPresentation,
  toHeContactDirectoryRow,
  planFindymailLookup,
} from "../../lib/shared-contact-intelligence/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

function arg(name, fallback = null) {
  const hit = process.argv.find((a) => a.startsWith(`--${name}=`));
  if (hit) return hit.slice(name.length + 3);
  if (process.argv.includes(`--${name}`)) return true;
  return fallback;
}

const hotelId = String(arg("hotelId", BETHESDA_GOLDEN.hotelId) || "").trim();
const live = Boolean(arg("live", false));
const updateMap = Boolean(arg("updateMap", false));
const outDir = path.join(ROOT, "reports", "shared-contact-intelligence");
fs.mkdirSync(outDir, { recursive: true });

const hotelResult = await resolveCanonicalHotelContacts(hotelId, {
  updateIdentityMap: updateMap,
});

const gates = await runCanonicalContactConsistencyGates(hotelId, {
  hotelResult,
});

const contactability = computeSharedContactability(hotelResult.people || [], {
  hotelId,
});

const perPerson = (hotelResult.people || []).map((p) => {
  const findymailPlan = planFindymailLookup(p, {
    authorizationId: null,
    allowPaid: false,
  });
  return {
    canonicalPersonId: p.canonicalPersonId,
    hiAirtableId: p.hiAirtableId,
    displayName: p.displayName,
    organizationName: p.organizationName,
    gdiPersonIdentityKey: p.gdiPersonIdentityKey,
    email: p.email,
    phone: p.phone,
    linkedinUrl: p.linkedinUrl,
    channels: p.channels,
    heRow: toHeContactDirectoryRow(p),
    gdiPresentation: toGdiContactPresentation(p),
    findymailPlan,
    parity: "SAME_RESOLVER",
  };
});

const report = {
  version: "shared-canonical-contact-validator-v1",
  hotelId,
  generatedAt: new Date().toISOString(),
  liveRequested: live,
  airtableLive: Boolean(hotelResult.airtableLive),
  hotelResult: {
    ok: hotelResult.ok,
    status: hotelResult.status,
    reason: hotelResult.reason || null,
    sourceOfTruth: hotelResult.sourceOfTruth || null,
    rawOrgContactCount: hotelResult.rawOrgContactCount || 0,
    duplicatePersonRowsCollapsed: hotelResult.duplicatePersonRowsCollapsed || 0,
    peopleCount: (hotelResult.people || []).length,
  },
  perPersonReconciliation: perPerson,
  hotelContactability: contactability,
  consistencyGates: gates,
  founderAcceptance:
    "One canonical identity → One eligible verified channel record in Airtable → One shared backend resolver → Consistent display in GDI and Hotel Explorer.",
};

const outPath = path.join(
  outDir,
  `SHARED-CANONICAL-CONTACT-VALIDATOR-${hotelId}.json`,
);
fs.writeFileSync(outPath, JSON.stringify(report, null, 2) + "\n", "utf8");

const perPersonPath = path.join(
  outDir,
  `SHARED-CANONICAL-CONTACT-PER-PERSON-${hotelId}.json`,
);
fs.writeFileSync(perPersonPath, JSON.stringify(perPerson, null, 2) + "\n", "utf8");

const contactabilityPath = path.join(
  outDir,
  `SHARED-CANONICAL-CONTACTABILITY-${hotelId}.json`,
);
fs.writeFileSync(
  contactabilityPath,
  JSON.stringify(contactability, null, 2) + "\n",
  "utf8",
);

console.log(
  JSON.stringify(
    {
      ok: hotelResult.ok || gates.overall === "NOT_TESTED" || gates.overall === "BLOCKED",
      hotelId,
      overall: gates.overall,
      airtableLive: report.airtableLive,
      people: perPerson.length,
      contactabilityRate: contactability.metrics?.directContactabilityRate,
      outPath,
      perPersonPath,
      contactabilityPath,
    },
    null,
    2,
  ),
);

// Fail closed only on explicit FAIL (not BLOCKED/NOT_TESTED without credentials).
if (gates.overall === "FAIL") process.exit(1);
if (live && !hotelResult.airtableLive) {
  console.error("LIVE requested but Airtable not reachable — NOT_TESTED/BLOCKED");
  process.exit(2);
}
process.exit(0);
