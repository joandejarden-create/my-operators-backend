#!/usr/bin/env node
/**
 * Add "Team Members" on Company Profile → links to Users (replaces UM team links).
 *   node scripts/ensure-company-profile-team-members-field.mjs [--dry-run]
 */
import "../load-env.js";
import {
  PLATFORM_USERS_COMPANY_TABLE_ID,
  PLATFORM_USERS_TABLE_ID,
} from "../lib/airtable/platform-users-table.js";

const FIELD_NAME = process.env.COMPANY_PROFILE_TEAM_FIELD_NAME || "Team Members";

async function metaFetch(baseId, token, path, init = {}) {
  const res = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}${path}`, {
    ...init,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      ...(init.headers || {}),
    },
  });
  const text = await res.text();
  return { res, json: text ? JSON.parse(text) : {} };
}

async function main() {
  const dryRun = process.argv.includes("--dry-run");
  const token = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!token || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");

  const { res, json } = await metaFetch(baseId, token, "/tables");
  if (!res.ok) throw new Error(JSON.stringify(json));

  const cp = (json.tables || []).find((t) => t.id === PLATFORM_USERS_COMPANY_TABLE_ID);
  if (!cp) throw new Error("Company Profile table not found");

  if ((cp.fields || []).some((f) => f.name === FIELD_NAME)) {
    console.log(`"${FIELD_NAME}" already exists on ${cp.name}.`);
    return;
  }

  const body = {
    name: FIELD_NAME,
    type: "multipleRecordLinks",
    options: { linkedTableId: PLATFORM_USERS_TABLE_ID },
  };

  if (dryRun) {
    console.log("Dry run — would create on Company Profile:", body);
    return;
  }

  const created = await metaFetch(baseId, token, `/tables/${cp.id}/fields`, {
    method: "POST",
    body: JSON.stringify(body),
  });
  if (!created.res.ok) {
    throw new Error(`Create failed: ${created.res.status} ${JSON.stringify(created.json)}`);
  }
  console.log(`Created "${FIELD_NAME}" (${created.json.id}) on Company Profile.`);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
