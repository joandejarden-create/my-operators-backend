#!/usr/bin/env node
/**
 * Founder QA — 3 internal leak-audit examples (Phase 1).
 * Creates request → approve → run → client report → promote stub.
 * Asserts isolation from production ADP paths.
 */
import assert from "node:assert/strict";
import { mkdtempSync, rmSync, readFileSync, existsSync, readdirSync, statSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { createHash } from "node:crypto";
import {
  createLeakAuditStore,
  runLeakAuditPhase1,
  promoteLeakAuditToPilotStub,
  buildClientSafeReportPayload,
  assertClientReportSafety,
  REQUEST_STATUS,
} from "../lib/ai-demand-positioning/leak-audit/index.js";

function fingerprint(p) {
  if (!existsSync(p)) return null;
  const st = statSync(p);
  if (st.isDirectory()) {
    return createHash("sha256").update(readdirSync(p).sort().join("|")).digest("hex");
  }
  return createHash("sha256").update(readFileSync(p)).digest("hex");
}

const watch = [
  join(process.cwd(), "data/ai-demand-positioning/runtime"),
  join(process.cwd(), "data/ai-demand-positioning/published"),
].map((path) => ({ path, before: fingerprint(path) }));

const hotels = [
  {
    hotelName: "Cambridge Beaches Resort & Spa",
    hotelWebsite: "https://www.cambridgebeaches.com/",
    city: "Sandys Parish",
    country: "BM",
    market: "Bermuda",
    demandSegmentOfInterest: "meetings_groups",
  },
  {
    hotelName: "NOW NOW NOHO",
    hotelWebsite: "https://www.nownownoho.com/",
    city: "New York",
    country: "US",
    market: "New York City",
    demandSegmentOfInterest: "couples",
  },
  {
    hotelName: "Hotel Phillips Kansas City",
    hotelWebsite: "https://www.hotelphillips.com/",
    city: "Kansas City",
    country: "US",
    market: "Kansas City",
    demandSegmentOfInterest: "business",
  },
];

const tmpRoot = mkdtempSync(join(tmpdir(), "leak-audit-founder-qa-"));
const store = createLeakAuditStore({ root: tmpRoot });
const results = [];

for (const hotel of hotels) {
  const request = store.createRequest({
    ...hotel,
    contactName: "Founder QA",
    contactEmail: "founder-qa@dealality.example",
    companyName: "Dealality Internal",
    role: "Founder",
    source: "manual",
    notes: "Phase 1 founder QA",
  });
  assert.equal(request.status, REQUEST_STATUS.REQUESTED);

  store.updateRequest(request.id, { status: REQUEST_STATUS.APPROVED });

  const runResult = await runLeakAuditPhase1(store, {
    requestId: request.id,
    liveProviderCalls: false,
  });

  assert.equal(runResult.liveProviderCalls, 0);
  assert.ok(runResult.run.id.startsWith("leak_run_"));
  assert.equal(runResult.run.auditRequestId, request.id);
  assert.ok(runResult.observations.every((o) => o.auditRunId === runResult.run.id));
  assert.ok(runResult.observations.every((o) => !("fullPromptText" in o)));

  // Observations only in leak-audit store root
  assert.ok(store.root.includes(tmpRoot) || store.root === tmpRoot);

  const client = buildClientSafeReportPayload({
    report: runResult.report,
    request: runResult.request,
    run: runResult.run,
  });
  const safety = assertClientReportSafety(client);
  assert.equal(safety.ok, true, `${hotel.hotelName}: ${JSON.stringify(safety.failures)}`);

  const blob = JSON.stringify(client);
  assert.equal(/\badp_[a-z0-9_]+/i.test(blob), false);
  assert.equal(/\brec[a-z0-9]{14}\b/i.test(blob), false);
  assert.equal(/fullPromptText/i.test(blob), false);
  assert.ok(client.whoDoesTheWork);
  assert.ok(client.fixes?.length >= 3);
  assert.ok(client.biggestDemandLeak);
  assert.match(client.recommendedNextStep, /limited diagnostic/i);

  // Promote stub only
  const promo = promoteLeakAuditToPilotStub(store, {
    requestId: request.id,
    confirmed: true,
    confirmedBy: "founder-qa",
  });
  assert.equal(promo.productionAdpRecordCreated, false);
  assert.equal(promo.request.status, REQUEST_STATUS.CONVERTED);

  results.push({
    hotelName: hotel.hotelName,
    requestId: request.id,
    runId: runResult.run.id,
    reportId: client.reportId,
    reportUrl: `/adp-leak-audit/${client.reportId}`,
    observationCount: runResult.observations.length,
    matchedHotelIdOnRunOnly: runResult.run.matchedHotelId || null,
    clientHasProductionIds: false,
    productionAdpRecordCreated: false,
    status: promo.request.status,
  });
}

for (const item of watch) {
  assert.equal(fingerprint(item.path), item.before, `production path changed: ${item.path}`);
}

rmSync(tmpRoot, { recursive: true, force: true });

console.log(
  JSON.stringify(
    {
      pass: true,
      gate: "ADP_LEAK_AUDIT_FOUNDER_QA_V1",
      hotels: results,
      productionRecordsTouched: false,
    },
    null,
    2
  )
);
