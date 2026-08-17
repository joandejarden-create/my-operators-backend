/**
 * Acceptance tests for Dealality ChatGPT Airtable wrapper (service layer).
 *
 *   node scripts/test-dealality-airtable-chatgpt.mjs
 *   node scripts/test-dealality-airtable-chatgpt.mjs --cleanup CREATED_RECORD_ID
 */
import "../load-env.js";
import {
  createRecordsByTableId,
  getRecordById,
  listDealalityTables,
  listRecordsByTableId,
  summarizeRecordsByTableId,
  updateRecordByTableId,
} from "../lib/gtm-owner-target/dealality-airtable-chatgpt-service.js";

const TABLE_ID = "tblpCg0QZ0kIPXihE";
const cleanupId = process.argv.includes("--cleanup") ? process.argv[process.argv.indexOf("--cleanup") + 1] : null;

function log(title, data) {
  console.log(`\n=== ${title} ===`);
  console.log(JSON.stringify(data, null, 2));
}

async function main() {
  if (cleanupId) {
    console.log("Cleanup note: delete endpoint is intentionally unavailable. Remove test row manually:", cleanupId);
    return;
  }

  // Test 1 — read
  const list = await listRecordsByTableId({ tableId: TABLE_ID, maxRecords: 3 });
  log("Test 1 listRecordsByTableId", {
    tableId: list.tableId,
    count: list.records.length,
    sampleTask: list.records[0]?.fields?.Task,
  });

  // Test 2 — create (omit Duration (Days) — formula field is read-only)
  const createPayload = {
    tableId: TABLE_ID,
    records: [
      {
        fields: {
          Task: "Tool write test — Founder Project Plan",
          Phase: "Pilot Readiness",
          Workstream: "Pilot Pipeline",
          Status: "Not Started",
          "Assigned To": "Joan D.",
          Progress: "0%",
          Priority: "P0",
          "Sprint / Wave": "Pilot Wave 1",
          "Related Table": "Founder Project Plan",
          "Next Action": "Confirm ChatGPT can create full Airtable records",
          "Success Metric": "Record is created with all expected fields populated",
        },
      },
    ],
  };
  const created = await createRecordsByTableId(createPayload);
  const createdId = created.records[0]?.id;
  log("Test 2 createRecordsByTableId", {
    id: createdId,
    fields: created.records[0]?.fields,
  });

  if (!createdId) throw new Error("Create test did not return a record id");

  // Test 3 — update
  const updated = await updateRecordByTableId({
    tableId: TABLE_ID,
    recordId: createdId,
    fields: {
      Status: "In Progress",
      Progress: "10%",
      "Next Action": "Confirm ChatGPT can update full Airtable records",
    },
  });
  log("Test 3 updateRecordByTableId", {
    id: updated.id,
    Status: updated.fields.Status,
    Progress: updated.fields.Progress,
    NextAction: updated.fields["Next Action"],
  });

  // Test 4 — summarize
  const summary = await summarizeRecordsByTableId({
    tableId: TABLE_ID,
    statusField: "Status",
    phaseField: "Phase",
    priorityField: "Priority",
    maxRecords: 100,
  });
  log("Test 4 summarizeRecordsByTableId", {
    totalRecords: summary.totalRecords,
    byStatus: summary.counts.byStatus,
    byPhaseKeys: Object.keys(summary.counts.byPhase).length,
    byPriority: summary.counts.byPriority,
    sampleCount: summary.sampleRecords.length,
  });

  // Bonus — list tables
  const tables = await listDealalityTables();
  log("listDealalityTables", {
    tableCount: tables.tables.length,
    names: tables.tables.map((t) => t.name),
  });

  const got = await getRecordById({ tableId: TABLE_ID, recordId: createdId });
  log("getRecordById", { id: got.id, Task: got.fields.Task, Status: got.fields.Status });

  console.log("\nAll tests passed.");
  console.log("Test record created:", createdId, "(delete manually in Airtable if desired)");
}

main().catch((err) => {
  console.error("TEST FAILED:", err.message || err);
  if (err.details) console.error(err.details);
  process.exit(1);
});
