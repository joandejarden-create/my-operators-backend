#!/usr/bin/env node
/**
 * Airtable Live overlay must compact oversize payloads without mid-string truncation.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  compactPayloadForAirtableOverlay,
  validatePublishedReportUpsert,
} from "../lib/ai-demand-positioning/airtable-published-report.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const PERIOD = "adp_period_adp_renaissance_times_square_20261016083441_2ff16a";
const PROPERTY = "adp_renaissance_times_square";
const reportPath = path.join(
  ROOT,
  "data/ai-demand-positioning/published",
  PROPERTY,
  `report-${PERIOD}.json`
);
const evidencePath = path.join(
  ROOT,
  "data/ai-demand-positioning/published",
  PROPERTY,
  `evidence-${PERIOD}.json`
);
const manifestPath = path.join(
  ROOT,
  "data/ai-demand-positioning/published",
  PROPERTY,
  "manifest.json"
);

const report = JSON.parse(fs.readFileSync(reportPath, "utf8"));
const evidenceIndex = JSON.parse(fs.readFileSync(evidencePath, "utf8"));
const manifest = JSON.parse(fs.readFileSync(manifestPath, "utf8"));

const rawPayloadChars = JSON.stringify(report.payload).length;
assert.ok(rawPayloadChars > 95000, `fixture payload expected >95k, got ${rawPayloadChars}`);

const compacted = compactPayloadForAirtableOverlay(report.payload, {
  payloadStoreRef: `published/${PROPERTY}/report-${PERIOD}.json`,
});
assert.equal(compacted.compacted, true);
assert.ok(compacted.stubbedKeys.includes("evidence"));
assert.ok(JSON.stringify(compacted.payload).length <= 95000);
assert.equal(compacted.payload.demandCapture?.overallRate, report.payload.demandCapture?.overallRate);
assert.equal((compacted.payload.actions || []).length, (report.payload.actions || []).length);
assert.equal(compacted.payload.evidence?.storageOnly, true);

const validation = validatePublishedReportUpsert({
  manifest,
  report,
  evidenceIndex,
  publishStatus: "Live",
  censusRecordId: "recG66DQJKP2c0UNh",
  payloadStoreRef: `published/${PROPERTY}/report-${PERIOD}.json`,
});
assert.equal(validation.ok, true, validation.errors?.join("; "));
assert.ok(validation.sanitizedFields["Payload JSON"].length <= 95000);
assert.ok(validation.payloadCompaction?.compacted);
assert.match(String(validation.sanitizedFields["Payload Store Ref"] || ""), /published\//);

console.log(
  JSON.stringify(
    {
      ok: true,
      rawPayloadChars,
      compactedPayloadChars: JSON.stringify(compacted.payload).length,
      stubbedKeys: compacted.stubbedKeys,
      validationOk: validation.ok,
    },
    null,
    2
  )
);
console.log("OK adp-airtable-payload-compaction-v1");
