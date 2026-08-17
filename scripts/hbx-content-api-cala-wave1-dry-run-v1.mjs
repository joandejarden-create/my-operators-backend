/**
 * HBX CALA Wave 1 dry-run ingest — candidate pack only.
 *
 * Usage:
 *   node scripts/hbx-content-api-cala-wave1-dry-run-v1.mjs
 *
 * Forces no Airtable writes.
 */
import "dotenv/config";
import { runHbxCalaWave1DryRunV1 } from "../lib/research-engine-v2/hbx-content-api-cala-wave1-dry-run-v1.js";

process.env.ENABLE_HBX_CENSUS_WRITES = "0";
process.env.ENABLE_HBX_INSERTS = "0";
process.env.HBX_DRY_RUN = "1";
process.env.HBX_WAVE1_COUNTRIES =
  process.env.HBX_WAVE1_COUNTRIES ||
  "Mexico,Dominican Republic,Colombia,Costa Rica,Panama";
process.env.HBX_BATCH_SIZE = process.env.HBX_BATCH_SIZE || "100";
process.env.HBX_MAX_HOTELS_PER_COUNTRY =
  process.env.HBX_MAX_HOTELS_PER_COUNTRY || "1000";

const report = await runHbxCalaWave1DryRunV1({
  log: (msg) => console.log(msg),
});

console.log(
  JSON.stringify(
    {
      ok: report.ok,
      status: report.status,
      totals: report.totals,
      airtable_writes: report.airtable_writes,
      census_writes: report.census_writes,
      inserts: report.inserts,
      secrets_logged: report.secrets_logged,
      artifacts: [
        "reports/research-engine-v2/hbx-content-api-cala-wave1-dry-run-v1.md",
        "reports/research-engine-v2/hbx-content-api-cala-wave1-dry-run-v1.json",
        "reports/research-engine-v2/hbx-cala-wave1-candidate-pack.json",
        "reports/research-engine-v2/hbx-cala-wave1-write-plan.json",
        "docs/data-intelligence/hbx-content-api-cala-wave1-dry-run-v1.md",
      ],
    },
    null,
    2
  )
);

process.exit(report.ok ? 0 : 1);
