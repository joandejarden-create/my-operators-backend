#!/usr/bin/env node
/**
 * HBX content inventory + Rooms / Keys field hunt v1 (read-only).
 * Usage: npm run hbx:content-inventory-rooms-hunt
 */
import "dotenv/config";
import { runHbxContentInventoryAndRoomsFieldHuntV1 } from "../lib/research-engine-v2/hbx-content-inventory-and-rooms-field-hunt-v1.js";

process.env.ENABLE_HBX_CENSUS_WRITES = "0";
process.env.ENABLE_HBX_INSERTS = "0";

const report = await runHbxContentInventoryAndRoomsFieldHuntV1({ env: process.env });
console.log(
  JSON.stringify(
    {
      status: report.status,
      secondary_statuses: report.secondary_statuses,
      airtable_writes: report.airtable_writes,
      sample_sizes: report.sample_sizes,
      true_total_rooms_supported:
        report.rooms_keys_investigation?.true_total_rooms_supported,
      hbx_support_confirmation_needed: report.hbx_support_confirmation_needed,
      license_policy_needed: report.license_policy_needed,
    },
    null,
    2
  )
);
process.exit(report.ok === false && report.status?.includes("blocked") ? 1 : 0);
