#!/usr/bin/env node
/**
 * LEGACY_HOTEL_CENSUS_INDEPENDENCE_GATE (HE-QL-041)
 *
 * Instrumented proof that HE production-intended paths do not call legacy
 * Hotel Census `tblgj2qEwxjTcg6q0`. Would fail if a hidden fallback re-enabled
 * census search without the migration opt-in.
 *
 * No production Airtable writes. Live HPC collision may run when credentials exist.
 */
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import {
  assertLegacyHotelCensusAccess,
  getLegacyCensusAccessLog,
  resetLegacyCensusAccessLog,
  summarizeLegacyCensusIndependence,
  LEGACY_HOTEL_CENSUS_TABLE_ID,
  LEGACY_HOTEL_CENSUS_TABLE_NAME,
  guardFetch,
  isLegacyHotelCensusTableRef,
} from "../lib/hotel-intelligence/hpc-population/legacy-hotel-census-access-guard-v1.js";
import { searchHotelCensusIdentityCandidates } from "../lib/hotel-intelligence/hpc-population/hotel-census-identity-search-v1.js";
import {
  resolvePilotHotelIdentity,
  resolveMullenPilotIdentities,
} from "../lib/hotel-intelligence/hpc-population/mullen-pilot-identity-resolution-v1.js";
import { MULLEN_PILOT_HOTELS } from "../lib/hotel-intelligence/hpc-population/mullen-pilot-cohort-v1.js";
import {
  buildHe50DryRunMutationPackage,
  runHe50PilotSupervisor,
  SEVEN_HOTEL_FREE_IDENTITY_PACKETS,
  HE50_HOTEL_CENSUS_TABLE_ID_RETIRED,
} from "../lib/hotel-intelligence/he-5-0-full-e2e/index.js";
import { resolvePropertyCensusOwnership } from "../lib/hotel-intelligence/property-census-identity-v1.js";

const require = createRequire(import.meta.url);
try {
  require("dotenv").config({ path: "C:/Dev/dealality/my-operators-backend/.env" });
} catch {
  /* optional */
}

// Ensure migration env is OFF for independence proof
delete process.env.HE_LEGACY_HOTEL_CENSUS_MIGRATION_READ;

resetLegacyCensusAccessLog();

assert.equal(HE50_HOTEL_CENSUS_TABLE_ID_RETIRED, LEGACY_HOTEL_CENSUS_TABLE_ID);
assert.equal(isLegacyHotelCensusTableRef(LEGACY_HOTEL_CENSUS_TABLE_ID), true);
assert.equal(isLegacyHotelCensusTableRef(LEGACY_HOTEL_CENSUS_TABLE_NAME), true);
assert.equal(isLegacyHotelCensusTableRef("tbl9aY5ijiuIzzWam"), false);

// Direct access must throw (GET and mutating methods)
for (const method of ["GET", "POST", "PATCH", "PUT", "DELETE"]) {
  assert.throws(
    () =>
      assertLegacyHotelCensusAccess(LEGACY_HOTEL_CENSUS_TABLE_ID, {
        method,
        workflow: "independence-gate",
        forceBlock: true,
      }),
    (err) => err && err.code === "LEGACY_HOTEL_CENSUS_ACCESS_BLOCKED"
  );
}

// Migration read requires explicit opt-in — default search must fail closed
await assert.rejects(
  () =>
    searchHotelCensusIdentityCandidates({
      hotelName: "Secrets Papagayo Costa Rica",
      allowLegacyHotelCensusRead: false,
    }),
  (err) => err && err.code === "LEGACY_HOTEL_CENSUS_ACCESS_BLOCKED"
);

// guardFetch blocks census URLs
const blockedFetch = guardFetch(async () => {
  throw new Error("underlying_fetch_should_not_run");
}, { workflow: "independence-gate-fetch", forceBlock: true });
await assert.rejects(
  () =>
    blockedFetch(
      `https://api.airtable.com/v0/appCCUsuGsE1ifoLk/${LEGACY_HOTEL_CENSUS_TABLE_ID}?maxRecords=1`,
      { method: "GET" }
    ),
  (err) => err && err.code === "LEGACY_HOTEL_CENSUS_ACCESS_BLOCKED"
);
await assert.rejects(
  () =>
    blockedFetch(
      `https://api.airtable.com/v0/appCCUsuGsE1ifoLk/${encodeURIComponent("Hotel Census")}`,
      { method: "POST", body: "{}" }
    ),
  (err) => err && err.code === "LEGACY_HOTEL_CENSUS_ACCESS_BLOCKED"
);

resetLegacyCensusAccessLog();

// HE 5.0 dry-run + supervisor — no legacy table IDs as canonical hotel ids
const dry = buildHe50DryRunMutationPackage();
assert.ok(!dry.mutations.some((m) => m.action === "STEWARD_PROMOTE_FROM_CENSUS"));
assert.ok(
  !dry.mutations.some(
    (m) =>
      m.canonicalEntityId === "recyX1wLsHBJYRPw7" ||
      m.destinationTable === LEGACY_HOTEL_CENSUS_TABLE_ID
  )
);
const pap = dry.mutations.find((m) => m.id.includes("papagayo"));
assert.equal(pap.action, "CREATE");
assert.equal(pap.legacyCensusRuntimeDependency, false);

const papPkt = SEVEN_HOTEL_FREE_IDENTITY_PACKETS.find((p) => p.pilotKey === "secrets_papagayo");
assert.equal(papPkt.identityOutcome, "NEW_HPC_CANDIDATE");
assert.equal(papPkt.hotelCensusRecordId, null);

const supervisor = runHe50PilotSupervisor({ enablePaid: false });
assert.equal(supervisor.aggregate.matchedCensus, 0);
assert.ok(supervisor.aggregate.newHpcCandidates >= 4);
assert.ok(
  !supervisor.hotelScorecard.some(
    (r) => r.hpcIdentity === "MATCHED_HOTEL_CENSUS" || r.hotelCensusId
  )
);

// Mullen identity resolution — HPC-only; census search skipped (no network to legacy)
const papagayoHotel = MULLEN_PILOT_HOTELS.find((h) => h.pilotKey === "secrets_papagayo");
assert.ok(papagayoHotel);

let liveIdentityOk = false;
try {
  const one = await resolvePilotHotelIdentity(papagayoHotel, {
    allowLegacyHotelCensusRead: false,
  });
  assert.equal(one.evidence?.censusSkipped, true);
  assert.equal(one.evidence?.legacyHotelCensusAccess, "BLOCKED_HPC_ONLY");
  assert.ok(Array.isArray(one.hotelCensusRecordIds));
  assert.equal(one.hotelCensusRecordIds.length, 0);
  assert.notEqual(one.identityOutcome, "MATCHED_HOTEL_CENSUS");
  // Must not treat legacy provenance as runtime SoT
  assert.ok(!one.hpcRecordId || String(one.hpcRecordId).startsWith("rec"));

  const cohort = await resolveMullenPilotIdentities({
    allowLegacyHotelCensusRead: false,
  });
  assert.equal(cohort.summary.MATCHED_HOTEL_CENSUS || 0, 0);
  for (const r of cohort.results || []) {
    assert.notEqual(r.identityOutcome, "MATCHED_HOTEL_CENSUS");
    assert.equal(r.evidence?.censusSkipped, true);
    assert.equal((r.hotelCensusRecordIds || []).length, 0);
  }
  liveIdentityOk = true;
} catch (err) {
  // Credentials / network may be unavailable — structural path still proven above
  if (err?.code === "HPC_CONFIG_MISSING" || /AIRTABLE|fetch|ENOTFOUND|credential/i.test(err?.message || "")) {
    console.log("SKIP live Mullen HPC collision —", err.message || err.code);
  } else {
    throw err;
  }
}

// Property ownership: Bethesda HPC without probing census
const key = process.env.AIRTABLE_API_KEY;
const baseId = process.env.AIRTABLE_BASE_ID_ALT;
if (key && baseId === "appCCUsuGsE1ifoLk") {
  const Airtable = (await import("airtable")).default;
  const base = new Airtable({ apiKey: key }).base(baseId);
  const beth = await resolvePropertyCensusOwnership(base, "recLuxvwwxID7U2B8");
  assert.equal(beth.owningTableKind, "HPC");
  assert.equal(beth.legacyCensusProbed, false);
}

const summary = summarizeLegacyCensusIndependence(getLegacyCensusAccessLog());
// Blocks from intentional assert.throws are expected; migration reads must be zero
assert.equal(summary.migrationReads, 0, "HE independence path must not allow migration reads");

// Hidden-fallback tripwire: if resolvePilotHotelIdentity called census search without opt-in,
// searchHotelCensusIdentityCandidates would throw — already asserted. Additional: if someone
// removes the skip and the guard, this assert on MATCHED_HOTEL_CENSUS count would still fail
// when live results return census-only matches — covered by matchedCensus === 0 above.

console.log(
  JSON.stringify(
    {
      gate: "LEGACY_HOTEL_CENSUS_INDEPENDENCE_GATE",
      heLegacyIndependent: true,
      liveIdentityOk,
      dryRunPapagayoAction: pap.action,
      supervisorMatchedCensus: supervisor.aggregate.matchedCensus,
      supervisorNewHpc: supervisor.aggregate.newHpcCandidates,
      accessSummary: summary,
      certificationHint: "HE_LEGACY_INDEPENDENT — GLOBAL_RETIREMENT_READY blocked by cross-product consumers",
    },
    null,
    2
  )
);
console.log("test:he-legacy-hotel-census-independence-v1 PASS");
