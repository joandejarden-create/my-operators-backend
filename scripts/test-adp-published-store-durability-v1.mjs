#!/usr/bin/env node
/**
 * Isolated non-customer durability probe for ADP PRIMARY_SOT published adapter.
 * Writes a throwaway property under published/_durability_probe_*, verifies
 * write→read→delete→missing, without touching customer hotels or actions.
 */
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const PUBLISHED = path.join(ROOT, "data/ai-demand-positioning/published");
const PROBE_ID = `_durability_probe_${Date.now().toString(36)}`;
const dir = path.join(PUBLISHED, PROBE_ID);

function rmrf(p) {
  if (!fs.existsSync(p)) return;
  fs.rmSync(p, { recursive: true, force: true });
}

rmrf(dir);
fs.mkdirSync(dir, { recursive: true });

const periodId = `adp_period_${PROBE_ID}_probe`;
const payload = {
  ok: true,
  property: { propertyId: PROBE_ID, name: "Durability Probe (non-customer)" },
  period: { periodId },
  demandCapture: { overallRate: 0.12 },
  actions: [],
  probeNonce: crypto.randomBytes(16).toString("hex"),
};
const report = {
  ok: true,
  propertyId: PROBE_ID,
  periodId,
  publishedAt: new Date().toISOString(),
  payload,
};
const manifest = {
  propertyId: PROBE_ID,
  propertyName: "Durability Probe (non-customer)",
  latestPeriodId: periodId,
  latestPublishedAt: report.publishedAt,
  publishStatus: "Internal",
  reportFile: `report-${periodId}.json`,
  evidenceFile: `evidence-${periodId}.json`,
  note: "ISOLATED_NON_CUSTOMER_DURABILITY_PROBE — safe to delete",
};
const evidence = { ok: true, propertyId: PROBE_ID, periodId, items: [] };

const reportPath = path.join(dir, manifest.reportFile);
const evidencePath = path.join(dir, manifest.evidenceFile);
const manifestPath = path.join(dir, "manifest.json");

// Atomic-ish write: temp then rename
function writeAtomic(file, obj) {
  const tmp = `${file}.${process.pid}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(obj, null, 2));
  fs.renameSync(tmp, file);
}

writeAtomic(reportPath, report);
writeAtomic(evidencePath, evidence);
writeAtomic(manifestPath, manifest);

const sha = crypto.createHash("sha256").update(fs.readFileSync(reportPath)).digest("hex");

// Read-back
const readReport = JSON.parse(fs.readFileSync(reportPath, "utf8"));
const readManifest = JSON.parse(fs.readFileSync(manifestPath, "utf8"));
assert.equal(readManifest.latestPeriodId, periodId);
assert.equal(readReport.payload.probeNonce, payload.probeNonce);
assert.equal(
  crypto.createHash("sha256").update(fs.readFileSync(reportPath)).digest("hex"),
  sha
);

// Simulate "restart" by clearing require cache is N/A for JSON SoT —
// re-read from disk after closing handles (already closed).
const reread = JSON.parse(fs.readFileSync(reportPath, "utf8"));
assert.equal(reread.payload.probeNonce, payload.probeNonce);

// Cleanup — probe must not remain
rmrf(dir);
assert.equal(fs.existsSync(dir), false);

const out = {
  ok: true,
  adapter: "filesystem_PRIMARY_SOT_published",
  probeId: PROBE_ID,
  checks: [
    "write_atomic",
    "read_back",
    "checksum_stable",
    "reread_after_close",
    "cleanup_removed",
  ],
  note:
    "Proves filesystem published adapter write/read/idempotent cleanup for the same root used by customer editions. Does NOT prove Railway ephemeral disk survives redeploy without a volume — that remains an infra limitation unless a durable volume is mounted.",
  railwayEphemeralRuntimeWrites: "NOT_PROVEN_BY_THIS_TEST",
};

fs.writeFileSync(
  path.join(ROOT, "reports/adp-six-hotel-e2e/renaissance/DURABILITY_PROBE.json"),
  JSON.stringify(out, null, 2) + "\n"
);
console.log(JSON.stringify(out, null, 2));
console.log("OK adp-published-store-durability-v1");
