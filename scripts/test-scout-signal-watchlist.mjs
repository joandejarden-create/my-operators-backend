/**
 * Scout signal watchlist integration tests (Scout Opportunity Signals table writes only).
 *
 * Usage: node scripts/test-scout-signal-watchlist.mjs
 */
import "../load-env.js";
import { ensureScoutOpportunitySignalsSchema } from "../lib/scout/ensure-scout-opportunity-signals-schema.js";
import { buildOpportunitySignalsReport } from "../lib/scout/opportunity-signals.js";
import {
  saveOrUpdateSignal,
  listSavedSignals,
  patchSavedSignal,
  findSavedSignalBySignalId,
  annotateGeneratedSignalsWithSavedStatus,
  SCOUT_OPPORTUNITY_SIGNALS_TABLE,
} from "../lib/scout/scout-signal-watchlist.js";
import { HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";

function assert(cond, msg) {
  if (!cond) throw new Error(msg);
}

async function main() {
  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID_ALT) {
    console.error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID_ALT");
    process.exit(1);
  }

  console.log("=== Scout signal watchlist test ===\n");

  console.log("1) Ensure Scout Opportunity Signals table...");
  const ensure = await ensureScoutOpportunitySignalsSchema({ apply: true });
  assert(ensure.ok, ensure.errors?.join("; ") || "ensure failed");
  console.log(
    "   tableId:",
    ensure.tableId,
    "| created:",
    ensure.tableCreated,
    "| fieldsCreated:",
    ensure.fieldsCreated.length
  );

  console.log("\n2) Generate live test signal (Mexico + Choice parent gap)...");
  const report = await buildOpportunitySignalsReport({
    country: "Mexico",
    parentCompany: "Choice Hotels International, Inc.",
    signalType: "parent_company_market_gap",
    includePipeline: "1",
    limit: 1,
  });
  assert(report.ok, report.error || "generation failed");
  assert(report.signals.length >= 1, "expected at least one generated signal");
  const testSignal = report.signals[0];
  console.log("   signalId:", testSignal.signalId);

  console.log("\n3) Save as Watchlist...");
  const save1 = await saveOrUpdateSignal({
    signal: testSignal,
    reviewStatus: "Watchlist",
    internalNotes: "Scout watchlist test",
    assignedTo: "qa@test",
  });
  assert(save1.ok, save1.error || "save failed");
  assert(save1.status === "created" || save1.status === "updated", "unexpected save status");
  const firstRecordId = save1.recordId;
  console.log("   status:", save1.status, "| recordId:", firstRecordId);

  console.log("\n4) List saved signals...");
  const listed = await listSavedSignals({
    reviewStatus: "Watchlist",
    country: "Mexico",
    signalType: "parent_company_market_gap",
    limit: 50,
  });
  assert(listed.ok, listed.error || "list failed");
  const found = listed.signals.find((s) => s.signalId === testSignal.signalId);
  assert(found, "saved signal not found in list");
  console.log("   found in list:", found.signalTitle);

  console.log("\n5) Patch to Researching...");
  const patched = await patchSavedSignal(testSignal.signalId, {
    reviewStatus: "Researching",
    internalNotes: "Moved to researching",
  });
  assert(patched.ok, patched.error || "patch failed");
  assert(patched.updatedFields.includes("reviewStatus"), "reviewStatus not updated");
  console.log("   reviewStatus:", patched.saved.reviewStatus);

  console.log("\n6) Re-save same signal (idempotent upsert)...");
  const save2 = await saveOrUpdateSignal({
    signal: { ...testSignal, priorityScore: testSignal.priorityScore },
    reviewStatus: "Researching",
    internalNotes: "Re-save test",
  });
  assert(save2.ok, save2.error || "re-save failed");
  assert(save2.status === "updated", `expected updated, got ${save2.status}`);
  assert(save2.recordId === firstRecordId, "re-save created duplicate record");
  console.log("   same recordId:", save2.recordId);

  console.log("\n7) Annotate generated signals with saved metadata...");
  const annotated = await annotateGeneratedSignalsWithSavedStatus(report.signals);
  const match = annotated.find((s) => s.signalId === testSignal.signalId);
  assert(match?.saved === true, "expected saved:true on generated signal");
  assert(match?.savedRecordId === firstRecordId, "savedRecordId mismatch");
  assert(match?.savedReviewStatus === "Researching", "savedReviewStatus mismatch");
  console.log("   saved:", match.saved, "| savedReviewStatus:", match.savedReviewStatus);

  console.log("\n8) Safety checks...");
  const lookup = await findSavedSignalBySignalId(testSignal.signalId);
  assert(lookup.found, "lookup failed");
  assert(lookup.saved.source === "Hotel Census", "source field on saved record");

  console.log("   writes table:", SCOUT_OPPORTUNITY_SIGNALS_TABLE);
  console.log("   census table unchanged (read-only):", HOTEL_CENSUS_TABLE);
  console.log("   no Radar / Brand Explorer modules touched in this test");

  console.log("\nPASS: Scout signal watchlist flow");
  process.exit(0);
}

main().catch((err) => {
  console.error("FAIL:", err.message);
  process.exit(1);
});
