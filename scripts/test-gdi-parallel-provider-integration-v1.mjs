#!/usr/bin/env node
/**
 * Focused tests: Parallel provider integration into GDI baseline.
 * Uses fixtures + mocks only — no paid Parallel/Surfe calls.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-parallel-test-"));
process.env.GDI_DATA_ROOT = tmpRoot;
process.env.GDI_PARALLEL_BUDGET_LEDGER_PATH = path.join(tmpRoot, "budget-ledger.json");
process.env.GDI_PARALLEL_JOB_STORE_PATH = path.join(tmpRoot, "jobs");
delete process.env.GDI_PARALLEL_ENABLED;
delete process.env.GDI_PARALLEL_KILL_SWITCH;
delete process.env.GDI_PARALLEL_SURFE_FALLBACK_ENABLED;

const {
  isGdiParallelEnabled,
  isGdiParallelHotelAllowed,
  getGdiParallelFlagState,
  GDI_PARALLEL_SAN_DIEGO_HOTEL_ID,
} = await import("../lib/group-demand-intelligence/parallel/feature-flag.js");
const {
  reserveParallelBudget,
  commitParallelBudgetReservation,
  releaseParallelBudgetReservation,
  remainingParallelBudgetUsd,
  readParallelBudgetLedger,
} = await import("../lib/group-demand-intelligence/parallel/budget-ledger-v1.js");
const {
  beginOrResumeParallelJob,
  markParallelJobSubmitted,
  markParallelJobUncertain,
  markParallelJobCompleted,
  loadParallelJob,
} = await import("../lib/group-demand-intelligence/parallel/job-store-v1.js");
const {
  evaluateContactChannels,
  CHANNEL_VERIFICATION,
  ROUTE_CLASS,
} = await import("../lib/group-demand-intelligence/parallel/contact-channel-policy-v1.js");
const {
  dedupeAccountTripCandidates,
  normalizeEventAliasKey,
} = await import("../lib/group-demand-intelligence/parallel/identity-dedupe-v1.js");
const { attachClaimEvidence } = await import(
  "../lib/group-demand-intelligence/parallel/claim-evidence-v1.js"
);
const { loadSanDiegoFixtureCards } = await import(
  "../lib/group-demand-intelligence/parallel/fixture-pack-v1.js"
);
const {
  runParallelDiscoveryStage,
  DISCOVERY_MODE,
} = await import(
  "../lib/group-demand-intelligence/workflow/parallel-discovery-adapter-v1.js"
);
const { reconcileSanDiegoReviewSet } = await import(
  "../lib/group-demand-intelligence/parallel/san-diego-reconcile-v1.js"
);

let passed = 0;
function ok(name) {
  passed += 1;
  console.log(`  PASS  ${name}`);
}

console.log("\n[1] Feature flag OFF preserves behavior");
assert.equal(isGdiParallelEnabled({}), false);
assert.equal(
  isGdiParallelHotelAllowed(GDI_PARALLEL_SAN_DIEGO_HOTEL_ID, {}),
  false
);
assert.equal(
  isGdiParallelHotelAllowed(GDI_PARALLEL_SAN_DIEGO_HOTEL_ID, {
    GDI_PARALLEL_ENABLED: "1",
  }),
  true
);
assert.equal(
  isGdiParallelHotelAllowed("recLuxvwwxID7U2B8", {
    GDI_PARALLEL_ENABLED: "1",
  }),
  false,
  "Bethesda not in default allowlist"
);
ok("default OFF + San Diego scoping");

console.log("\n[2] Fixture pack available in-repo (no lab required)");
const pack = loadSanDiegoFixtureCards();
assert.equal(pack.available, true);
assert.equal(pack.cards.length, 15);
ok("15 San Diego fixture cards");

console.log("\n[3] Account/trip dedupe + ASCB/EMBO alias + shared housing");
assert.equal(normalizeEventAliasKey("ASCB/EMBO Cell Bio 2026"), "ascb_embo_cell_bio");
assert.equal(normalizeEventAliasKey("Cell Bio 2026 with EMBO"), "ascb_embo_cell_bio");
const deduped = dedupeAccountTripCandidates([
  {
    candidateId: "a",
    organizationName: "Lockheed Martin",
    title: "WEST 2027",
    eventStartDate: "2027-02-01",
    sourcingStatus: "WEST Housing Center / SPARGO exhibitor block",
    primaryContact: { name: "WEST Housing Center" },
  },
  {
    candidateId: "b",
    organizationName: "L3Harris Technologies",
    title: "WEST 2027",
    eventStartDate: "2027-02-01",
    sourcingStatus: "official housing center",
    primaryContact: { name: "WEST Housing Center" },
  },
  {
    candidateId: "a2",
    organizationName: "Lockheed Martin",
    title: "WEST 2027",
    eventStartDate: "2027-02-01",
    travelingCohort: "default",
  },
]);
assert.ok(deduped.unique.length >= 2);
assert.ok(deduped.duplicateCount >= 1);
assert.ok(deduped.sharedHousingPathways.length >= 1);
assert.equal(
  deduped.unique.find((c) => c.candidateId === "a")?.independentlyVerifiedBuyer,
  false
);
ok("dedupe + shared housing pathway marking");

console.log("\n[4] Contact channel policy");
const emailOnly = evaluateContactChannels(
  {
    name: "Jane Doe",
    identityKind: "named_person",
    email: "jane@example.org",
    emailQuality: CHANNEL_VERIFICATION.OFFICIALLY_PUBLISHED,
    phone: null,
  },
  { organizationName: "Example Org" }
);
assert.equal(emailOnly.hasUsableChannel, true);
assert.equal(emailOnly.hasUsableEmail, true);
assert.equal(emailOnly.secondChannelPurchaseBlocked, true);
assert.equal(emailOnly.surfeEligible, false);

const phoneOnly = evaluateContactChannels(
  {
    name: "Jane Doe",
    identityKind: "named_person",
    email: null,
    phone: "+1 202-555-0100",
    phoneKind: "department_phone",
    phoneQuality: CHANNEL_VERIFICATION.OFFICIALLY_PUBLISHED,
  },
  { organizationName: "Example Org" }
);
assert.equal(phoneOnly.hasUsablePhone, true);
assert.equal(phoneOnly.surfeEligible, false);

const functional = evaluateContactChannels(
  {
    name: "General inquiries",
    identityKind: "functional_department",
    email: "info@example.org",
    emailQuality: CHANNEL_VERIFICATION.LOCAL_PUBLIC,
  },
  { organizationName: "Example Org" }
);
assert.equal(functional.routeClass, ROUTE_CLASS.USABLE_FUNCTIONAL);

const routing = evaluateContactChannels(
  {
    name: "WEST Housing Center",
    identityKind: "routing",
    email: null,
    phone: null,
  },
  { organizationName: "Lockheed" }
);
assert.equal(routing.routeClass, ROUTE_CLASS.ROUTING_ONLY);
assert.equal(routing.surfeEligible, false);

const generated = evaluateContactChannels(
  {
    name: "Jane Doe",
    identityKind: "named_person",
    email: "jane.doe@example.org",
    emailQuality: CHANNEL_VERIFICATION.GENERATED_VARIANT,
  },
  { organizationName: "Example Org" }
);
assert.equal(generated.hasUsableChannel, false);
assert.ok(
  generated.routeClass === ROUTE_CLASS.QUARANTINED ||
    generated.quarantinedEmails.length >= 1
);

const notFound = evaluateContactChannels(
  {
    name: "Jane Doe",
    identityKind: "named_person",
    email: "info@ascb.org",
    emailQuality: CHANNEL_VERIFICATION.NOT_FOUND,
  },
  { organizationName: "ASCB" }
);
assert.equal(notFound.hasUsableChannel, false);

const surfeReady = evaluateContactChannels(
  {
    name: "Rebekah Bell",
    identityKind: "named_person",
    email: null,
    phone: null,
  },
  { organizationName: "Sol-Ark" }
);
assert.equal(surfeReady.surfeEligible, true);
ok("email-only / phone-only / functional / routing / quarantine / Surfe eligibility");

console.log("\n[5] Claim evidence persistence");
const withEv = attachClaimEvidence(
  { organizationName: "Test" },
  {
    confirmed_facts: ["Event dates published"],
    inferences: ["May need lodging"],
    unknowns: ["Peak rooms"],
  },
  { evidenceLinks: ["https://example.org/event"] }
);
assert.ok(withEv.claimEvidence.length >= 3);
assert.ok(withEv.evidenceStats.facts >= 1);
assert.ok(
  withEv.claimEvidence.some((e) => e.evidenceStatus === "URL_ONLY_UNVERIFIED")
);
ok("claim evidence fact vs inference");

console.log("\n[6] Budget reservation concurrency / idempotency");
process.env.GDI_PARALLEL_ENABLED = "1";
const r1 = reserveParallelBudget({
  label: "t1",
  jobKey: "job-a",
  estimatedUsd: 0.1,
  hotelId: GDI_PARALLEL_SAN_DIEGO_HOTEL_ID,
  env: process.env,
});
const r1b = reserveParallelBudget({
  label: "t1-dup",
  jobKey: "job-a",
  estimatedUsd: 0.1,
  hotelId: GDI_PARALLEL_SAN_DIEGO_HOTEL_ID,
  env: process.env,
});
assert.equal(r1b.idempotent, true);
commitParallelBudgetReservation(r1.reservationId, { env: process.env });
const ledger = readParallelBudgetLedger({ env: process.env });
assert.ok(ledger.estimatedSpentUsd >= 0.1 - 1e-9);
assert.ok(remainingParallelBudgetUsd(ledger) < 10);
ok("reserve idempotent + commit");

console.log("\n[7] Job store resume / no duplicate submit / uncertain");
const jobKey = "test-job-sd-1";
const first = beginOrResumeParallelJob({
  jobKey,
  hotelId: GDI_PARALLEL_SAN_DIEGO_HOTEL_ID,
});
assert.equal(first.shouldSubmit, true);
markParallelJobSubmitted(jobKey, {
  providerRunId: "trun_test_abc",
  reservationId: r1.reservationId,
});
const second = beginOrResumeParallelJob({
  jobKey,
  hotelId: GDI_PARALLEL_SAN_DIEGO_HOTEL_ID,
});
assert.equal(second.shouldSubmit, false);
assert.equal(second.providerRunId, "trun_test_abc");
markParallelJobUncertain(jobKey, "submission_timeout");
const uncertain = loadParallelJob(jobKey);
assert.equal(uncertain.status, "uncertain");
const third = beginOrResumeParallelJob({
  jobKey,
  hotelId: GDI_PARALLEL_SAN_DIEGO_HOTEL_ID,
});
assert.equal(third.shouldSubmit, false);
markParallelJobCompleted(jobKey, { candidateCount: 0, paidCalls: 0 });
ok("resume known run_id; uncertain blocks resubmit");

console.log("\n[8] Kill switch blocks new reservations");
process.env.GDI_PARALLEL_KILL_SWITCH = "1";
let killed = false;
try {
  reserveParallelBudget({
    label: "killed",
    jobKey: "job-kill",
    estimatedUsd: 0.01,
    env: process.env,
  });
} catch (err) {
  killed = err.code === "PARALLEL_KILL_SWITCH";
}
assert.equal(killed, true);
delete process.env.GDI_PARALLEL_KILL_SWITCH;
ok("kill switch");

console.log("\n[9] Discovery stage: flag OFF live blocked; replay works");
delete process.env.GDI_PARALLEL_ENABLED;
const blocked = await runParallelDiscoveryStage(GDI_PARALLEL_SAN_DIEGO_HOTEL_ID, {
  runDiscovery: true,
  mode: DISCOVERY_MODE.LIVE,
  authorizePaidDiscovery: true,
  env: process.env,
});
assert.equal(blocked.reason, "parallel_flag_or_hotel_scope_blocked");
assert.equal(blocked.paidCalls, 0);

const replay = await runParallelDiscoveryStage(GDI_PARALLEL_SAN_DIEGO_HOTEL_ID, {
  runDiscovery: true,
  mode: DISCOVERY_MODE.REPLAY,
  env: process.env,
});
assert.equal(replay.status, "completed_replay");
assert.equal(replay.paidCalls, 0);
assert.equal(replay.freshDiscoveryClaimed, false);
assert.ok((replay.candidates || []).length >= 10);
assert.equal(replay.customerVisibleWrites, 0);
ok("replay from fixtures; live gated");

console.log("\n[10] San Diego dry-run reconcile");
const recon = await reconcileSanDiegoReviewSet({
  skipBaseline: false,
  env: { GDI_PARALLEL_ENABLED: "1" },
});
assert.equal(recon.hotelId, GDI_PARALLEL_SAN_DIEGO_HOTEL_ID);
assert.equal(recon.fixturePack.cardCount, 15);
assert.equal(recon.confirmedOpenDemand, 0);
assert.equal(recon.customerVisibleProposed.length, 0);
assert.ok(recon.internalOnly.length >= 10);
assert.ok(Array.isArray(recon.localRecovery));
assert.ok(recon.localRecovery.some((r) => r.orgKey === "sol-ark"));
ok("SD reconcile preview counts");

console.log("\n[11] Reviewed-publication boundary");
assert.ok(
  replay.candidates.every((c) => c.publicationBlocked === true || c.customerVisible === false)
);
ok("no customer-visible writes from provider replay");

console.log(`\nOK — ${passed} assertion groups passed`);
console.log(`tmp=${tmpRoot}`);
