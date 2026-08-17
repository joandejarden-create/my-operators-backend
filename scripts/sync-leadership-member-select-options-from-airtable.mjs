#!/usr/bin/env node
/**
 * Pull multiple-select choices from Airtable Meta API and print JSON for
 * LEADERSHIP_MEMBER_SELECT_OPTIONS (compare / manual sync with operator-leadership-member-map.js).
 *
 * Usage:
 *   node scripts/sync-leadership-member-select-options-from-airtable.mjs
 */
import "../load-env.js";

const TABLE_ID = "tbl8jX7BoOcwUIEOd";
const FIELD_KEYS = {
  languages: "languages",
  market_experience: "marketExperience",
  core_expertise: "coreExpertise",
  relevant_asset_types: "relevantAssetTypes",
};

async function main() {
  const token = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!token || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");

  const res = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const json = await res.json();
  if (!res.ok) throw new Error(`Meta API ${res.status}: ${JSON.stringify(json)}`);

  const table = (json.tables || []).find((t) => t.id === TABLE_ID);
  if (!table) throw new Error(`Table not found: ${TABLE_ID}`);

  const out = {};
  for (const [airtableName, optionKey] of Object.entries(FIELD_KEYS)) {
    const field = (table.fields || []).find((f) => f.name === airtableName);
    out[optionKey] = (field?.options?.choices || []).map((c) => c.name);
    console.log(`${optionKey}: ${out[optionKey].length} choices`);
  }
  console.log("\nJSON for LEADERSHIP_MEMBER_SELECT_OPTIONS:\n");
  console.log(JSON.stringify(out, null, 2));
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
