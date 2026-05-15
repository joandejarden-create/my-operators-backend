/**
 * Create Number fields on Airtable User Management for Partner Directory individual card stats.
 *
 * Fields (integer precision):
 *   - Closed Deals
 *   - Unique Brands (Deals)  ← unique brands represented across that person's deals
 *   - Submitted Bids
 *
 * Usage:
 *   node scripts/add-user-management-partner-stats-fields.mjs
 *
 * Requires env:
 *   AIRTABLE_API_KEY or AIRTABLE_METADATA_TOKEN — Personal Access Token with
 *   `schema.bases:write` for the target base (standard data PAT alone cannot create fields).
 *
 * If creation fails with 403, add the fields manually in Airtable (same names), then run:
 *   node scripts/backfill-user-management-partner-stats.mjs --apply
 */
import "../load-env.js";

const BASE_ID = process.env.AIRTABLE_BASE_ID;
const TOKEN = process.env.AIRTABLE_METADATA_TOKEN || process.env.AIRTABLE_API_KEY;
const TABLE_ID = process.env.USER_MANAGEMENT_TABLE_ID || "tblQEpYKf2aYNKKjw";

const FIELDS_TO_CREATE = [
  { name: "Closed Deals", type: "number", options: { precision: 0 } },
  { name: "Unique Brands (Deals)", type: "number", options: { precision: 0 } },
  { name: "Submitted Bids", type: "number", options: { precision: 0 } },
];

async function createField(body) {
  const url = `https://api.airtable.com/v0/meta/bases/${BASE_ID}/tables/${TABLE_ID}/fields`;
  const res = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });
  const text = await res.text();
  let json;
  try {
    json = JSON.parse(text);
  } catch {
    json = { raw: text };
  }
  return { ok: res.ok, status: res.status, json };
}

async function main() {
  if (!BASE_ID || !TOKEN) {
    console.error("Set AIRTABLE_BASE_ID and AIRTABLE_METADATA_TOKEN (or AIRTABLE_API_KEY with schema.bases:write).");
    process.exit(1);
  }

  for (const field of FIELDS_TO_CREATE) {
    const { ok, status, json } = await createField(field);
    if (ok) {
      console.log(`OK created field "${field.name}"`);
      continue;
    }
    const msg = json?.error?.message || json?.error?.type || JSON.stringify(json);
    if (/duplicate|already exists|NAME_NOT_UNIQUE/i.test(String(msg))) {
      console.log(`Skip "${field.name}" (already exists): ${msg}`);
      continue;
    }
    console.error(`Failed "${field.name}" (${status}): ${msg}`);
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
