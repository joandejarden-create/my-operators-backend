#!/usr/bin/env node
/**
 * Unit/integration tests for GDI universal operational workflow + Airtable ownership.
 * No production Airtable writes. No paid research.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  mergeResearchPayloadPreservingSales,
  applySalesOwnershipToAirtableFields,
  mapLifecycleToActionStatus,
  GDI_WORKFLOW_LIFECYCLE,
  SALES_OWNED_PAYLOAD_KEYS,
} from "../lib/group-demand-intelligence/workflow/gdi-airtable-field-ownership-v1.js";
import { MAP_GDI_OPPORTUNITY as F } from "../lib/group-demand-intelligence/opportunity-field-map.js";
import {
  runGdiUniversalOperationalWorkflow,
  WORKFLOW_ACTIONS,
} from "../lib/group-demand-intelligence/workflow/gdi-universal-operational-workflow-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const SPICE = "recKRJjcPnb4tVDDS";
const BETHESDA = "recLuxvwwxID7U2B8";
const WATERSTONE = "recgMYovrrZDJMqzX";

const results = [];
function check(name, fn) {
  try {
    const r = fn();
    if (r && typeof r.then === "function") {
      return r
        .then(() => results.push({ name, ok: true }))
        .catch((err) =>
          results.push({ name, ok: false, error: String(err?.stack || err) }),
        );
    }
    results.push({ name, ok: true });
  } catch (err) {
    results.push({ name, ok: false, error: String(err?.stack || err) });
  }
}

await check("actions_contract", () => {
  assert.ok(WORKFLOW_ACTIONS.includes("Research"));
  assert.ok(WORKFLOW_ACTIONS.includes("SyncAirtable"));
  assert.ok(WORKFLOW_ACTIONS.includes("Publish"));
  assert.ok(WORKFLOW_ACTIONS.includes("Verify"));
  assert.ok(WORKFLOW_ACTIONS.includes("FullRun"));
});

await check("sales_feedback_preserved_on_research_merge", () => {
  const existing = {
    id: "opp1",
    title: "Old",
    hotelSalesFeedback: "Already known",
    salesOwner: "Alex",
    nextFollowUpDate: "2026-12-01",
    gdiPublicationStatus: "PUBLISHED",
    gdiPublicationId: "pub_old",
  };
  const research = {
    id: "opp1",
    title: "New researched title",
    hotelSalesFeedback: "SHOULD_NOT_WIN",
    whyNow: "Fresh research",
  };
  const merged = mergeResearchPayloadPreservingSales(existing, research);
  assert.equal(merged.title, "New researched title");
  assert.equal(merged.hotelSalesFeedback, "Already known");
  assert.equal(merged.salesOwner, "Alex");
  assert.equal(merged.gdiPublicationStatus, "PUBLISHED");
  assert.equal(merged.whyNow, "Fresh research");
  assert.ok(SALES_OWNED_PAYLOAD_KEYS.includes("hotelSalesFeedback"));
});

await check("action_status_not_clobbered", () => {
  const next = applySalesOwnershipToAirtableFields(
    { [F.actionStatus]: "READY_FOR_REVIEW", [F.opportunityName]: "X" },
    { [F.actionStatus]: "SALES_FOLLOW_UP", [F.decisionId]: "dec1" },
  );
  assert.equal(next[F.actionStatus], "SALES_FOLLOW_UP");
  assert.equal(next[F.opportunityName], "X");
});

await check("lifecycle_mapping_respects_existing", () => {
  assert.equal(
    mapLifecycleToActionStatus(GDI_WORKFLOW_LIFECYCLE.PUBLISHED, "WORKING"),
    "WORKING",
  );
  assert.equal(
    mapLifecycleToActionStatus(GDI_WORKFLOW_LIFECYCLE.PUBLISHED, null),
    "PUBLISHED",
  );
});

await check("powershell_entry_exists", () => {
  const ps1 = path.join(ROOT, "scripts/gdi-workflow.ps1");
  assert.ok(fs.existsSync(ps1));
  const body = fs.readFileSync(ps1, "utf8");
  assert.ok(body.includes("gdi-universal-operational-workflow-v1.mjs"));
  assert.ok(body.includes("SyncAirtable"));
  assert.ok(body.includes("ApprovalReference"));
});

await check("validate_spice_dry", async () => {
  const result = await runGdiUniversalOperationalWorkflow({
    hotelId: SPICE,
    action: "Validate",
    dryRun: true,
    environment: "Local",
    reportPath: path.join(
      ROOT,
      "reports/gdi-universal-operational-workflow-v1/test-runs",
    ),
  });
  assert.equal(result.report.action, "Validate");
  assert.ok(result.reportFile);
  assert.ok(fs.existsSync(result.reportFile));
});

await check("review_pauses_fullrun", async () => {
  const result = await runGdiUniversalOperationalWorkflow({
    hotelId: SPICE,
    action: "FullRun",
    dryRun: true,
    environment: "Local",
    reportPath: path.join(
      ROOT,
      "reports/gdi-universal-operational-workflow-v1/test-runs",
    ),
  });
  assert.equal(result.paused, true);
  assert.equal(result.report.approvalStatus, "AWAITING_FOUNDER_APPROVAL");
  assert.ok(result.report.stages.review?.pausedForApproval);
});

await check("publish_dry_run_allowed_without_approval", async () => {
  const result = await runGdiUniversalOperationalWorkflow({
    hotelId: SPICE,
    action: "Publish",
    dryRun: true,
    environment: "Local",
    reportPath: path.join(
      ROOT,
      "reports/gdi-universal-operational-workflow-v1/test-runs",
    ),
  });
  // Dry-run may succeed (plan) or fail closed on incomplete approved source —
  // but must not require ApprovalReference.
  assert.notEqual(
    result.report.approvalStatus,
    "BLOCKED_MISSING_APPROVAL_REFERENCE",
  );
});

await check("publish_apply_blocked_without_approval", async () => {
  const result = await runGdiUniversalOperationalWorkflow({
    hotelId: SPICE,
    action: "Publish",
    dryRun: false,
    environment: "Production",
    reportPath: path.join(
      ROOT,
      "reports/gdi-universal-operational-workflow-v1/test-runs",
    ),
  });
  assert.equal(result.ok, false);
  assert.equal(result.report.approvalStatus, "BLOCKED_MISSING_APPROVAL_REFERENCE");
});

await check("verify_spice_offline", async () => {
  // Uses default published inventory root from env/local; NOT_PUBLISHED is acceptable offline.
  const result = await runGdiUniversalOperationalWorkflow({
    hotelId: SPICE,
    action: "Verify",
    dryRun: true,
    environment: "Local",
    reportPath: path.join(
      ROOT,
      "reports/gdi-universal-operational-workflow-v1/test-runs",
    ),
  });
  assert.ok(result.report.fullContentParityResult);
  assert.ok(
    ["VERIFIED", "MISMATCH", "NOT_PUBLISHED", "NOT_VERIFIABLE"].includes(
      result.report.fullContentParityResult,
    ),
  );
});

await check("inventory_read_only", async () => {
  const result = await runGdiUniversalOperationalWorkflow({
    action: "Inventory",
    dryRun: true,
    reportPath: path.join(
      ROOT,
      "reports/gdi-universal-operational-workflow-v1/test-runs",
    ),
  });
  assert.ok(result.report.stages.inventory?.summary);
});

await check("peer_hotels_validate_dry", async () => {
  for (const hotelId of [BETHESDA, WATERSTONE]) {
    const result = await runGdiUniversalOperationalWorkflow({
      hotelId,
      action: "Validate",
      dryRun: true,
      environment: "Local",
      reportPath: path.join(
        ROOT,
        "reports/gdi-universal-operational-workflow-v1/test-runs",
      ),
    });
    assert.ok(result.report.stages.validate);
  }
});

const failed = results.filter((r) => !r.ok);
console.log(
  JSON.stringify(
    {
      ok: failed.length === 0,
      passed: results.filter((r) => r.ok).length,
      failed: failed.length,
      results,
    },
    null,
    2,
  ),
);
process.exit(failed.length ? 1 : 0);
