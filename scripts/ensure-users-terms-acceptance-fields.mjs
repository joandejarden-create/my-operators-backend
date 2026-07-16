/**
 * Ensure Terms acceptance fields on Users table (Deal Capture MVP).
 *
 *   node scripts/ensure-users-terms-acceptance-fields.mjs
 *   node scripts/ensure-users-terms-acceptance-fields.mjs --apply
 */
import "../load-env.js";
import {
  PLATFORM_USERS_TABLE_ID,
} from "../lib/airtable/platform-users-table.js";

const APPLY = process.argv.includes("--apply");

const FIELDS = [
  {
    name: "Terms & Privacy Accepted",
    type: "checkbox",
    options: { icon: "check", color: "greenBright" },
    description: "User checked agreement on signup form",
  },
  {
    name: "Terms Accepted At",
    type: "dateTime",
    options: {
      dateFormat: { name: "iso" },
      timeFormat: { name: "24hour" },
      timeZone: "utc",
    },
    description: "When Terms & Privacy were accepted on signup",
  },
  {
    name: "Terms Version Accepted",
    type: "singleLineText",
    description: "Terms/Privacy version date at acceptance (e.g. 2026-07-16)",
  },
];

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
  const token = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!token || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");

  console.log(`Mode: ${APPLY ? "apply" : "dry-run"}`);
  const { res, json } = await metaFetch(baseId, token, "/tables");
  if (!res.ok) throw new Error(JSON.stringify(json));

  const users =
    (json.tables || []).find((t) => t.id === PLATFORM_USERS_TABLE_ID) ||
    (json.tables || []).find((t) => t.name === "Users");
  if (!users) throw new Error("Users table not found");

  const existing = new Set((users.fields || []).map((f) => f.name));
  for (const field of FIELDS) {
    if (existing.has(field.name)) {
      console.log(`Skip (exists): ${field.name}`);
      continue;
    }
    console.log(`${APPLY ? "Creating" : "[dry-run] Would create"}: ${field.name}`);
    if (!APPLY) continue;
    const created = await metaFetch(baseId, token, `/tables/${users.id}/fields`, {
      method: "POST",
      body: JSON.stringify(field),
    });
    if (!created.res.ok) {
      throw new Error(`Create ${field.name} failed: ${JSON.stringify(created.json)}`);
    }
    console.log(`  → ${created.json.id}`);
  }

  if (!APPLY) console.log("\nRe-run with --apply to write fields.");
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
