#!/usr/bin/env node
/**
 * Foundation test — ADP published snapshot read path.
 *   npm run test:ai-demand-positioning-published-foundation
 */

import assert from "assert";
import { loadLatestPeriod, loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import {
  buildPublishedSnapshotBundle,
  savePublishedSnapshotBundle,
  loadPublishedReport,
  loadPublishedEvidenceIndex,
} from "../lib/ai-demand-positioning/published-snapshot.js";
import { getPublishedOwnerReport, getPublishedEvidenceResponse } from "../lib/ai-demand-positioning/published-read-service.js";
import { queryEvidenceIndex, buildEvidenceIndex } from "../lib/ai-demand-positioning/customer/evidence-index.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { buildOwnerPayload } from "../lib/ai-demand-positioning/customer/owner-payload.js";
import { validatePublishedReportUpsert } from "../lib/ai-demand-positioning/airtable-published-report.js";

const PROPERTY_ID = "adp_waterstone_boca_raton";

async function main() {
  const profile = loadPropertyProfile(PROPERTY_ID);
  const period = loadLatestPeriod(PROPERTY_ID);
  assert.ok(profile, "profile exists");
  assert.ok(period, "period exists");

  const bundle = buildPublishedSnapshotBundle({ period, profile });
  assert.equal(bundle.ok, true);
  // Filesystem PRIMARY_SOT may exceed Airtable long-text limits; that is expected.
  // Airtable Live overlay must fit via compaction / storageOnly stubs (not raw FS size).
  assert.ok(
    Number.isFinite(bundle.summary.payloadBytes) && bundle.summary.payloadBytes > 0,
    "payload bytes recorded"
  );
  assert.ok(
    Number.isFinite(bundle.summary.evidenceBytes) && bundle.summary.evidenceBytes > 0,
    "evidence bytes recorded"
  );

  const validation = validatePublishedReportUpsert({
    manifest: bundle.manifest,
    report: bundle.report,
    evidenceIndex: bundle.evidenceIndex,
  });
  assert.equal(validation.ok, true, validation.errors?.join("; "));
  const fieldValues = Object.values(validation.sanitizedFields || {}).filter((v) => typeof v === "string");
  const oversized = fieldValues.filter((v) => v.length > 95000);
  assert.equal(
    oversized.length,
    0,
    `Airtable overlay fields must be ≤95000 chars after compaction; oversized=${oversized.map((v) => v.length).join(",")}`
  );
  assert.ok(
    validation.sanitizedFields?.["Payload JSON"],
    "Airtable overlay includes compacted Payload JSON"
  );
  assert.ok(
    validation.sanitizedFields?.["Evidence Index JSON"],
    "Airtable overlay includes Evidence Index JSON (full or storageOnly stub)"
  );

  // Do NOT rewrite PRIMARY_SOT published hotels in this foundation gate.
  // Persistence write/read is covered by test:adp-published-store-durability-v1 (probe id).
  void savePublishedSnapshotBundle;

  const published = loadPublishedReport(PROPERTY_ID);
  assert.ok(published?.demandCapture, "published report loads from disk");

  const evidenceIndex = loadPublishedEvidenceIndex(PROPERTY_ID);
  assert.equal(evidenceIndex.ok, true);

  const scenarios = buildScenarioUniverse(profile);
  const ownerPayload = buildOwnerPayload(period, scenarios, profile);
  assert.equal(ownerPayload.property?.propertyId, PROPERTY_ID, "owner payload includes propertyId");

  const builtIndex = buildEvidenceIndex(period, scenarios);
  const missing = queryEvidenceIndex(builtIndex, { intent: "leisure", type: "missing" });
  assert.ok(Array.isArray(missing.evidence));

  const readResult = await getPublishedOwnerReport(PROPERTY_ID);
  assert.equal(readResult.ok, true);
  assert.ok(readResult.payload.demandCapture);

  const evidenceResult = await getPublishedEvidenceResponse(PROPERTY_ID, {
    intent: "leisure",
    type: "missing",
  });
  assert.equal(evidenceResult.ok, true);
  assert.equal(evidenceResult.propertyId, PROPERTY_ID);

  const nohoEvidence = await getPublishedEvidenceResponse("adp_now_now_noho", {
    intent: "business",
    type: "missing",
  });
  if (nohoEvidence.ok && nohoEvidence.evidence?.length) {
    const sample = nohoEvidence.evidence[0];
    assert.ok(
      !String(sample.scenarioLabel || sample.scenarioId || "").includes("boca"),
      "NOW NOW NOHO evidence must not use Boca scenario ids"
    );
  }

  console.log("test:ai-demand-positioning-published-foundation — PASS");
}

main().catch((err) => {
  console.error("test:ai-demand-positioning-published-foundation — FAIL", err.message);
  process.exit(1);
});
