/**
 * Validate Outreach Setup UI ↔ Airtable field mapping against live base meta.
 *
 * Usage: node scripts/test-outreach-setup-field-map.mjs
 */
import "../load-env.js";
import {
  MAP_OUTREACH_UI_TO_AIRTABLE,
  OUTREACH_UI_FIELD_KEYS,
  OUTREACH_USER_FIELD,
  OUTREACH_USER_WEBFLOW_FIELD,
  map_outreachUiFieldsToAirtable,
  map_outreachAirtableToUiFields,
} from "../api/schemas/outreach-setup-fields.js";

const baseId = process.env.AIRTABLE_BASE_ID;
const apiKey = process.env.AIRTABLE_API_KEY;
if (!baseId || !apiKey) {
  console.error("Missing AIRTABLE_BASE_ID or AIRTABLE_API_KEY");
  process.exit(1);
}

let failed = 0;
function ok(msg) {
  console.log("ok:", msg);
}
function fail(msg) {
  console.error("FAIL:", msg);
  failed += 1;
}

const metaRes = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
  headers: { Authorization: `Bearer ${apiKey}` },
});
const meta = await metaRes.json();
if (meta.error) {
  console.error("Meta API error:", meta.error.message);
  process.exit(1);
}

const tableName = process.env.AIRTABLE_TABLE_OUTREACH_SETUP || "Outreach Setup";
const table = (meta.tables || []).find((t) => t.name === tableName);
if (!table) {
  fail(`table not found: ${tableName}`);
  process.exit(1);
}
ok(`found table ${tableName}`);

const airtableNames = new Set(table.fields.map((f) => f.name));

for (const uiKey of OUTREACH_UI_FIELD_KEYS) {
  const atName = MAP_OUTREACH_UI_TO_AIRTABLE[uiKey];
  if (!atName) fail(`missing mapping for UI key ${uiKey}`);
  else if (!airtableNames.has(atName)) fail(`Airtable missing column for ${uiKey}: ${JSON.stringify(atName)}`);
  else ok(`map ${uiKey}`);
}

for (const required of [OUTREACH_USER_FIELD, OUTREACH_USER_WEBFLOW_FIELD]) {
  if (!airtableNames.has(required)) fail(`required link/text field missing: ${required}`);
  else ok(`required field ${required}`);
}

const sampleUi = {
  Confidentiality: "Yes, keep fully confidential",
  "Identity Disclosure": "From the start",
  "Outreach Start Date": "2026-07-01",
};
const mapped = map_outreachUiFieldsToAirtable(sampleUi);
if (!mapped["Do you want this opportunity to remain fully confidential?"]) {
  fail("map_outreachUiFieldsToAirtable did not map Confidentiality");
} else ok("ui → airtable map sample");

const round = map_outreachAirtableToUiFields(mapped);
if (round.Confidentiality !== sampleUi.Confidentiality) {
  fail("round-trip Confidentiality mismatch");
} else ok("airtable → ui round-trip");

const legacyBad = "Confidentiality";
if (airtableNames.has(legacyBad)) {
  fail("legacy short field name Confidentiality should not exist on Outreach Setup table");
} else ok("legacy Confidentiality column absent (expected)");

console.log(`\ntest-outreach-setup-field-map: ${OUTREACH_UI_FIELD_KEYS.length + 4 - failed} checks, ${failed} failed`);
process.exit(failed ? 1 : 0);
