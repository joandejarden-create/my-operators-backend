/**
 * HBX / Hotelbeds Content API smoke test v1 — read-only.
 *
 * Usage:
 *   node scripts/hbx-content-api-smoke-test-v1.mjs
 *
 * Requires: HBX_API_KEY, HBX_API_SECRET
 * Forces: ENABLE_HBX_CENSUS_WRITES=0 ENABLE_HBX_INSERTS=0
 * Never writes Airtable / Census / Brand Explorer / Brand Setup.
 */
import "dotenv/config";
import { runHbxContentApiSmokeTestV1 } from "../lib/research-engine-v2/hbx-content-api-smoke-test-v1.js";

process.env.ENABLE_HBX_CENSUS_WRITES = "0";
process.env.ENABLE_HBX_INSERTS = "0";

const report = await runHbxContentApiSmokeTestV1({
  log: (msg) => console.log(msg),
});

console.log(
  JSON.stringify(
    {
      ok: report.ok,
      status: report.status,
      auth_success: report.auth_success,
      sample_hotel_count: report.sample_hotel_count,
      sample_hotel_codes: report.sample_hotel_codes,
      capabilities: report.capabilities,
      airtable_writes: report.airtable_writes,
      secrets_logged: report.secrets_logged,
      reports: [
        "reports/research-engine-v2/hbx-content-api-smoke-test-v1.md",
        "reports/research-engine-v2/hbx-content-api-smoke-test-v1.json",
        "docs/data-intelligence/hbx-content-api-smoke-test-v1.md",
      ],
    },
    null,
    2
  )
);

process.exit(
  report.status?.includes("blocked") || report.status?.includes("auth_failed")
    ? 1
    : 0
);
