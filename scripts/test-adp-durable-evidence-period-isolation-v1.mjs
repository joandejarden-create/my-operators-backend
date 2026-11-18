#!/usr/bin/env node
/**
 * Durable evidence reads must honor the requested measurement period.
 * A historical request must not receive CURRENT evidence, and a checksum
 * mismatch must fail closed instead of serving another period.
 */
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  ADP_DURABLE_EVIDENCE_PERIOD_UNAVAILABLE,
  ADP_DURABLE_INTEGRITY_FAILURE,
  createFilesystemDurablePublishedStore,
  isDurableEvidenceSupportingComplete,
} from "../lib/ai-demand-positioning/durable-publication/filesystem-durable-store-v1.js";
import { loadPublishedEvidenceIndex } from "../lib/ai-demand-positioning/published-snapshot.js";

const PROPERTY_ID = "adp_period_isolation_fixture";
const PERIOD_OLD = `adp_period_${PROPERTY_ID}_20260101000000_aaaaaa`;
const PERIOD_CURRENT = `adp_period_${PROPERTY_ID}_20260202000000_bbbbbb`;
const PERIOD_MISSING = `adp_period_${PROPERTY_ID}_20260303000000_cccccc`;

function sha256(text) {
  return crypto.createHash("sha256").update(text).digest("hex");
}

function writeJson(file, value) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const body = `${JSON.stringify(value, null, 2)}\n`;
  fs.writeFileSync(file, body);
  return sha256(body);
}

function buildStore() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "adp-period-isolation-"));
  const oldSha = writeJson(path.join(root, "snapshots", PROPERTY_ID, `evidence-${PERIOD_OLD}.json`), {
    ok: true,
    propertyId: PROPERTY_ID,
    periodId: PERIOD_OLD,
    marker: "HISTORICAL",
    items: [{ scenarioId: "historical-only" }],
  });
  const currentSha = writeJson(
    path.join(root, "snapshots", PROPERTY_ID, `evidence-${PERIOD_CURRENT}.json`),
    {
      ok: true,
      propertyId: PROPERTY_ID,
      periodId: PERIOD_CURRENT,
      marker: "CURRENT",
      items: [],
    }
  );
  writeJson(path.join(root, "snapshots", PROPERTY_ID, `manifest-${PERIOD_OLD}.json`), {
    propertyId: PROPERTY_ID,
    latestPeriodId: PERIOD_OLD,
    evidenceFile: `evidence-${PERIOD_OLD}.json`,
    evidenceSha256: oldSha,
  });
  writeJson(path.join(root, "snapshots", PROPERTY_ID, `manifest-${PERIOD_CURRENT}.json`), {
    propertyId: PROPERTY_ID,
    latestPeriodId: PERIOD_CURRENT,
    evidenceFile: `evidence-${PERIOD_CURRENT}.json`,
    evidenceSha256: currentSha,
  });
  writeJson(path.join(root, "current", PROPERTY_ID, "manifest.json"), {
    propertyId: PROPERTY_ID,
    latestPeriodId: PERIOD_CURRENT,
    evidenceFile: `evidence-${PERIOD_CURRENT}.json`,
    evidenceSha256: currentSha,
    publishStatus: "Live",
  });
  writeJson(path.join(root, "registry", "published-current-registry-v1.json"), {
    properties: [{ propertyId: PROPERTY_ID, periodId: PERIOD_CURRENT }],
  });
  return root;
}

const root = buildStore();
const previousRoot = process.env.ADP_DURABLE_PUBLISHED_ROOT;
const previousMode = process.env.ADP_DURABLE_PUBLISHED_MODE;
process.env.ADP_DURABLE_PUBLISHED_ROOT = root;
process.env.ADP_DURABLE_PUBLISHED_MODE = "off";

try {
  const store = createFilesystemDurablePublishedStore({ root });

  const historical = store.readPublishedEvidence(PROPERTY_ID, PERIOD_OLD);
  assert.equal(historical.marker, "HISTORICAL");
  assert.equal(historical.periodId, PERIOD_OLD);
  assert.equal(isDurableEvidenceSupportingComplete(historical), true);

  const current = store.readPublishedEvidence(PROPERTY_ID);
  assert.equal(current.marker, "CURRENT");
  assert.equal(current.periodId, PERIOD_CURRENT);
  assert.equal(isDurableEvidenceSupportingComplete(current), false);

  const explicitCurrent = store.readPublishedEvidence(PROPERTY_ID, PERIOD_CURRENT);
  assert.equal(explicitCurrent.marker, "CURRENT");
  assert.notEqual(explicitCurrent.periodId, historical.periodId);

  const missing = store.readPublishedEvidence(PROPERTY_ID, PERIOD_MISSING);
  assert.equal(missing.available, false);
  assert.equal(missing.code, ADP_DURABLE_EVIDENCE_PERIOD_UNAVAILABLE);
  assert.equal(missing.reason, "PERIOD_MANIFEST_MISSING");
  assert.equal(missing.marker, undefined);

  const wrongPropertyPeriod = store.readPublishedEvidence(
    PROPERTY_ID,
    "adp_period_adp_other_hotel_20260101000000_aaaaaa"
  );
  assert.equal(wrongPropertyPeriod.reason, "PERIOD_PROPERTY_MISMATCH");

  const traversal = store.readPublishedEvidence(
    PROPERTY_ID,
    `adp_period_${PROPERTY_ID}_../secret`
  );
  assert.equal(traversal.reason, "PERIOD_ID_INVALID");

  const indexedHistorical = loadPublishedEvidenceIndex(PROPERTY_ID, PERIOD_OLD);
  assert.equal(indexedHistorical.marker, "HISTORICAL");
  assert.notEqual(indexedHistorical.marker, "CURRENT");

  const indexedMissing = loadPublishedEvidenceIndex(PROPERTY_ID, PERIOD_MISSING);
  assert.equal(indexedMissing.available, false);
  assert.equal(indexedMissing.reason, "PERIOD_MANIFEST_MISSING");
  assert.equal(indexedMissing.marker, undefined);

  const evidenceFile = path.join(root, "snapshots", PROPERTY_ID, `evidence-${PERIOD_OLD}.json`);
  fs.appendFileSync(evidenceFile, "\n");
  assert.throws(
    () => store.readPublishedEvidence(PROPERTY_ID, PERIOD_OLD),
    (err) => err?.code === ADP_DURABLE_INTEGRITY_FAILURE
  );
  assert.throws(
    () => loadPublishedEvidenceIndex(PROPERTY_ID, PERIOD_OLD),
    (err) => err?.code === ADP_DURABLE_INTEGRITY_FAILURE
  );
  const currentAfterTamper = store.readPublishedEvidence(PROPERTY_ID);
  assert.equal(currentAfterTamper.marker, "CURRENT");
} finally {
  if (previousRoot == null) delete process.env.ADP_DURABLE_PUBLISHED_ROOT;
  else process.env.ADP_DURABLE_PUBLISHED_ROOT = previousRoot;
  if (previousMode == null) delete process.env.ADP_DURABLE_PUBLISHED_MODE;
  else process.env.ADP_DURABLE_PUBLISHED_MODE = previousMode;
  fs.rmSync(root, { recursive: true, force: true });
}

console.log(
  JSON.stringify(
    {
      ok: true,
      gate: "adp-durable-evidence-period-isolation-v1",
      historicalIsolated: true,
      missingPeriodUnavailable: true,
      checksumMismatchFailClosed: true,
      emptyIndexNotComplete: true,
    },
    null,
    2
  )
);
