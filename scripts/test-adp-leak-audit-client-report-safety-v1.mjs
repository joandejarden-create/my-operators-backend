#!/usr/bin/env node
/**
 * Client report safety gate for AI Demand Leak Audit.
 * npm run test:adp-leak-audit-client-report-safety-v1
 */
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import {
  createLeakAuditStore,
  runLeakAuditPhase1,
  buildClientSafeReportPayload,
  assertClientReportSafety,
  assertNoFullPromptInClientPayload,
  loadLeakAuditSampleReport,
  REQUEST_STATUS,
  WHO_DOES_THE_WORK,
} from "../lib/ai-demand-positioning/leak-audit/index.js";

const tmpRoot = mkdtempSync(join(tmpdir(), "leak-audit-report-safety-"));
const store = createLeakAuditStore({ root: tmpRoot });

const request = store.createRequest({
  hotelName: "Cambridge Beaches Resort & Spa",
  hotelWebsite: "https://www.cambridgebeaches.com/",
  city: "Sandys Parish",
  country: "BM",
  demandSegmentOfInterest: "meetings_groups",
  source: "manual",
  contactName: "Founder QA",
  contactEmail: "qa@dealality.example",
});
store.updateRequest(request.id, { status: REQUEST_STATUS.APPROVED });

const result = await runLeakAuditPhase1(store, {
  requestId: request.id,
  liveProviderCalls: false,
  seedObservations: [
    {
      provider: "openai",
      demandTerritory: "Meetings & Groups",
      promptId: "leak_meetings_retreat_01",
      promptLabel: "Meetings and retreats",
      promptIntentSummary: "Planner asking for meeting or retreat hotel options",
      subjectHotelMentioned: false,
      displacedByCompetitor: true,
      displacedCompetitorName: "Rosewood Bermuda",
      competitorsMentioned: ["Rosewood Bermuda", "The Loren at Pink Beach"],
      aiResponseExcerpt:
        "For executive retreats in Bermuda, travelers often consider Rosewood Bermuda for meeting space and coastal setting.",
    },
    {
      provider: "gemini",
      demandTerritory: "Leisure Travel",
      promptId: "leak_leisure_stay_01",
      promptLabel: "Leisure stay discovery",
      promptIntentSummary: "Leisure traveler asking where to stay in this market",
      subjectHotelMentioned: true,
      subjectMentionRank: 1,
      competitorsMentioned: [],
      displacedByCompetitor: false,
      aiResponseExcerpt:
        "Cambridge Beaches Resort & Spa is a cottage-style option for leisure stays in Bermuda.",
    },
  ],
});

const client = buildClientSafeReportPayload({
  report: result.report,
  request: result.request,
  run: result.run,
});

assert.ok(client);
assert.equal(assertNoFullPromptInClientPayload(client).ok, true);

const safety = assertClientReportSafety(client);
assert.equal(safety.ok, true, JSON.stringify(safety.failures));

// Explicit founder checklist
assert.match(JSON.stringify(client), /limited diagnostic/i);
assert.ok(client.whoDoesTheWork);
assert.deepEqual(
  client.whoDoesTheWork.dealalityPrepares.slice(0, 3),
  WHO_DOES_THE_WORK.dealalityPrepares.slice(0, 3)
);
assert.ok(client.biggestDemandLeak);
assert.ok(client.bottomLineSummary);
assert.ok(client.fixes?.length >= 3 || (client.firstFix && client.secondFix && client.thirdFix));
assert.ok(client.competitorDisplacement?.length >= 1);
assert.equal(client.competitorDisplacement[0].competitorName, "Rosewood Bermuda");
assert.ok(Number(client.competitorDisplacement[0].displacementCount) >= 1);
assert.ok(client.competitorDisplacement[0].evidenceExcerpt);

const blob = JSON.stringify(client);
assert.equal(/\brec[a-z0-9]{14}\b/i.test(blob), false);
assert.equal(/\bapp[a-z0-9]{14}\b/i.test(blob), false);
assert.equal(/\badp_[a-z0-9_]+/i.test(blob), false);
assert.equal(/hotel property census/i.test(blob), false);
assert.equal(/fullPromptText/i.test(blob), false);
assert.equal(/INTERNAL —/i.test(blob), false);
assert.equal(/\bthis proves\b/i.test(blob), false);
assert.equal(/\bai is penalizing\b/i.test(blob), false);
assert.equal(/\bthis will improve ranking\b/i.test(blob), false);
assert.equal(Object.prototype.hasOwnProperty.call(client, "matchedHotelId"), false);
assert.equal(Object.prototype.hasOwnProperty.call(client, "propertyId"), false);
assert.equal(Object.prototype.hasOwnProperty.call(client, "airtableId"), false);

// Matched hotel id may exist on run internally but must not reach client payload
assert.ok(result.run);
assert.equal(Object.prototype.hasOwnProperty.call(client, "matchMeta"), false);

// Sample pack must include How to Read bridge + inferred priority language
const sample = loadLeakAuditSampleReport();
assert.match(sample.howToRead?.title || "", /How to Read This Report/);
assert.match(JSON.stringify(sample.howToRead), /Executive Summary/);
assert.match(JSON.stringify(sample.howToRead), /Executive Signal/);
assert.match(JSON.stringify(sample.howToRead), /Demand Area to Review/);
assert.match(JSON.stringify(sample.howToRead), /Competitors Showing Up Instead/);
assert.match(JSON.stringify(sample.howToRead), /Supporting Evidence/);
assert.match(JSON.stringify(sample.howToRead), /Priority AI Demand Improvements/);
assert.equal(/Inferred Demand Leak/.test(JSON.stringify(sample.howToRead)), false);
assert.equal(/First 3 Fixes/.test(JSON.stringify(sample.howToRead)), false);
assert.match(
  sample.howToRead.adpBridgeNote || "",
  /The full ADP pilot uses the same reading logic/
);
assert.equal(sample.layoutMode, "three_page_v1");
assert.equal(sample.prioritySource, "inferred_from_results");
assert.match(
  sample.limitedDiagnosticDisclaimer || "",
  /No client-stated commercial priority was used/
);
assert.ok(sample.inferredDemandLeak);
assert.ok(sample.evidenceLibrary?.length >= 4);
assert.ok(sample.competitorDisplacementRank);
assert.match(
  (sample.executiveSummary?.paragraphs || []).join(" ") ||
    sample.inferredDemandLeak.summary ||
    "",
  /does not prove/i
);
assert.equal(/stated priority/i.test(JSON.stringify(sample)), false);
assert.equal(assertClientReportSafety(sample).ok, true);

rmSync(tmpRoot, { recursive: true, force: true });

console.log(
  JSON.stringify(
    {
      pass: true,
      gate: "ADP_LEAK_AUDIT_CLIENT_REPORT_SAFETY_V1",
      reportId: client.reportId,
      hotelName: client.hotelName,
      competitor: client.competitorDisplacement[0].competitorName,
      fixes: client.fixes?.length || 3,
      hasWhoDoesTheWork: true,
      limitedDiagnostic: true,
    },
    null,
    2
  )
);
