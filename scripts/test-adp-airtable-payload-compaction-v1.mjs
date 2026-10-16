#!/usr/bin/env node
/**
 * Airtable Live overlay compaction + safe read reconstruction.
 *
 * Covers:
 * - Complete payload within limit
 * - Oversized payload compacted and correctly resolved from PRIMARY_SOT
 * - Missing reference / wrong property / wrong period / invalid path
 * - Unavailable authoritative store
 * - No customer-visible storageOnly stubs after resolve
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  compactPayloadForAirtableOverlay,
  validatePublishedReportUpsert,
  resolveCompactedAirtablePayload,
  validateAdpPublishedStoreRef,
  listStorageOnlyStubKeys,
  ADP_AIRTABLE_PAYLOAD_READ,
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
const storeRef = `published/${PROPERTY}/report-${PERIOD}.json`;

// 1) Complete (synthetic small) payload — no compaction
{
  const small = {
    ok: true,
    property: { propertyId: PROPERTY },
    period: { periodId: PERIOD },
    demandCapture: { overallRate: 0.45 },
    actions: [{ id: "a1" }],
  };
  const c = compactPayloadForAirtableOverlay(small, { payloadStoreRef: storeRef });
  assert.equal(c.compacted, false);
  assert.deepEqual(c.stubbedKeys, []);
  const r = resolveCompactedAirtablePayload(c.payload, {
    propertyId: PROPERTY,
    periodId: PERIOD,
    payloadStoreRef: storeRef,
  });
  assert.equal(r.ok, true);
  assert.equal(r.status, ADP_AIRTABLE_PAYLOAD_READ.COMPLETE);
  assert.equal(listStorageOnlyStubKeys(r.payload).length, 0);
}

// 2) Oversized real payload compacted + reconstructed
{
  const rawPayloadChars = JSON.stringify(report.payload).length;
  assert.ok(rawPayloadChars > 95000, `fixture payload expected >95k, got ${rawPayloadChars}`);

  const compacted = compactPayloadForAirtableOverlay(report.payload, {
    payloadStoreRef: storeRef,
  });
  assert.equal(compacted.compacted, true);
  assert.ok(compacted.stubbedKeys.includes("evidence"));
  assert.ok(JSON.stringify(compacted.payload).length <= 95000);
  assert.equal(compacted.payload.demandCapture?.overallRate, report.payload.demandCapture?.overallRate);
  assert.equal((compacted.payload.actions || []).length, (report.payload.actions || []).length);
  assert.equal(compacted.payload.evidence?.storageOnly, true);

  const resolved = resolveCompactedAirtablePayload(compacted.payload, {
    propertyId: PROPERTY,
    periodId: PERIOD,
    payloadStoreRef: storeRef,
  });
  assert.equal(resolved.ok, true, resolved.error);
  assert.equal(resolved.status, ADP_AIRTABLE_PAYLOAD_READ.RESOLVED_FROM_PRIMARY_SOT);
  assert.equal(listStorageOnlyStubKeys(resolved.payload).length, 0);
  assert.ok(resolved.payload.evidence && resolved.payload.evidence.storageOnly !== true);
  assert.equal(
    resolved.payload.demandCapture?.overallRate,
    report.payload.demandCapture?.overallRate
  );

  const validation = validatePublishedReportUpsert({
    manifest,
    report,
    evidenceIndex,
    publishStatus: "Live",
    censusRecordId: "recG66DQJKP2c0UNh",
    payloadStoreRef: storeRef,
  });
  assert.equal(validation.ok, true, validation.errors?.join("; "));
  assert.ok(validation.sanitizedFields["Payload JSON"].length <= 95000);
  assert.ok(validation.payloadCompaction?.compacted);
}

// 3) Missing reference
{
  const compacted = compactPayloadForAirtableOverlay(report.payload, {
    payloadStoreRef: storeRef,
  });
  const bad = resolveCompactedAirtablePayload(compacted.payload, {
    propertyId: PROPERTY,
    periodId: PERIOD,
    payloadStoreRef: "",
  });
  // stubs embed storeRef — strip it to force missing
  const stripped = {
    ...compacted.payload,
    evidence: { ...compacted.payload.evidence, payloadStoreRef: "" },
  };
  const missing = resolveCompactedAirtablePayload(stripped, {
    propertyId: PROPERTY,
    periodId: PERIOD,
    payloadStoreRef: "",
  });
  assert.equal(missing.ok, false);
  assert.ok(
    [
      ADP_AIRTABLE_PAYLOAD_READ.INVALID_STORE_REF,
      ADP_AIRTABLE_PAYLOAD_READ.INCOMPLETE_STORAGE_STUB,
    ].includes(missing.code)
  );
}

// 4) Wrong property
{
  const v = validateAdpPublishedStoreRef(storeRef, {
    propertyId: "adp_other_hotel",
    periodId: PERIOD,
    kind: "report",
  });
  assert.equal(v.ok, false);
  assert.equal(v.code, ADP_AIRTABLE_PAYLOAD_READ.PROPERTY_MISMATCH);

  const compacted = compactPayloadForAirtableOverlay(report.payload, {
    payloadStoreRef: storeRef,
  });
  const wrongProp = resolveCompactedAirtablePayload(compacted.payload, {
    propertyId: "adp_other_hotel",
    periodId: PERIOD,
    payloadStoreRef: storeRef,
  });
  assert.equal(wrongProp.ok, false);
  assert.equal(wrongProp.code, ADP_AIRTABLE_PAYLOAD_READ.PROPERTY_MISMATCH);
}

// 5) Wrong period
{
  const v = validateAdpPublishedStoreRef(storeRef, {
    propertyId: PROPERTY,
    periodId: "adp_period_other_00000000000000_abcdef",
    kind: "report",
  });
  assert.equal(v.ok, false);
  assert.equal(v.code, ADP_AIRTABLE_PAYLOAD_READ.PERIOD_MISMATCH);
}

// 6) Invalid storage path (traversal)
{
  const v = validateAdpPublishedStoreRef(`published/${PROPERTY}/../evil.json`, {
    propertyId: PROPERTY,
    periodId: PERIOD,
    kind: "report",
  });
  assert.equal(v.ok, false);
  assert.equal(v.code, ADP_AIRTABLE_PAYLOAD_READ.INVALID_STORE_REF);
}

// 7) Unavailable authoritative store
{
  const ghostRef = `published/${PROPERTY}/report-adp_period_does_not_exist_00000000000000_zzzzzz.json`;
  const compacted = compactPayloadForAirtableOverlay(report.payload, {
    payloadStoreRef: storeRef,
  });
  const ghost = {
    ...compacted.payload,
    evidence: {
      ...compacted.payload.evidence,
      payloadStoreRef: ghostRef,
      periodId: "adp_period_does_not_exist_00000000000000_zzzzzz",
    },
  };
  const missingFile = resolveCompactedAirtablePayload(ghost, {
    propertyId: PROPERTY,
    periodId: "adp_period_does_not_exist_00000000000000_zzzzzz",
    payloadStoreRef: ghostRef,
  });
  assert.equal(missingFile.ok, false);
  assert.equal(missingFile.code, ADP_AIRTABLE_PAYLOAD_READ.PRIMARY_SOT_UNAVAILABLE);
}

// 8) Idempotent compaction of already-compacted payload stays within limit
{
  const once = compactPayloadForAirtableOverlay(report.payload, { payloadStoreRef: storeRef });
  const twice = compactPayloadForAirtableOverlay(once.payload, { payloadStoreRef: storeRef });
  assert.ok(JSON.stringify(twice.payload).length <= 95000);
  assert.equal(twice.payload.evidence?.storageOnly, true);
}

console.log(
  JSON.stringify(
    {
      ok: true,
      cases: [
        "complete_within_limit",
        "oversized_resolved_from_primary_sot",
        "missing_reference",
        "wrong_property",
        "wrong_period",
        "invalid_path",
        "unavailable_store",
        "idempotent_compaction",
        "no_customer_visible_stubs_after_resolve",
      ],
    },
    null,
    2
  )
);
console.log("OK adp-airtable-payload-compaction-v1");
