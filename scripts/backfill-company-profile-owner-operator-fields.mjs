/**
 * Backfill empty Owner-Operator extension fields on Company Profile.
 * Derives from Company Type, ecosystem role, and overview text — does not overwrite filled values.
 *
 *   node scripts/backfill-company-profile-owner-operator-fields.mjs
 *   node scripts/backfill-company-profile-owner-operator-fields.mjs --apply
 *   node scripts/backfill-company-profile-owner-operator-fields.mjs --apply --id recXXX
 */
import "../load-env.js";
import { writeFileSync } from "fs";
import { join } from "path";
import { buildCompanyProfileOwnerOperatorBackfillPatch } from "../lib/company-profile-owner-operator-backfill.js";
import { finalizeCompanyProfileFieldsForAirtableWrite } from "../lib/company-profile-owner-operator-fields.js";

const TABLE = process.env.COMPANY_PROFILE_TABLE_ID || "tblItyfH6MlOnMKZ9";
const baseId = process.env.AIRTABLE_BASE_ID;
const apiKey = process.env.AIRTABLE_API_KEY;
const apply = process.argv.includes("--apply");
const onlyId = (() => {
  const idx = process.argv.indexOf("--id");
  return idx >= 0 ? process.argv[idx + 1] : "";
})();

async function fetchAll() {
  const records = [];
  let offset;
  for (;;) {
    const qs = new URLSearchParams({ pageSize: "100" });
    if (offset) qs.set("offset", offset);
    const res = await fetch(
      `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(TABLE)}?${qs}`,
      { headers: { Authorization: `Bearer ${apiKey}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(JSON.stringify(json));
    records.push(...(json.records || []));
    offset = json.offset;
    if (!offset) break;
  }
  return records;
}

function extractUnknownFieldName(errBody) {
  const msg =
    typeof errBody === "string"
      ? errBody
      : errBody && errBody.error && errBody.error.message
        ? String(errBody.error.message)
        : JSON.stringify(errBody || "");
  const match = msg.match(/Unknown field name:\s*"([^"]+)"/i);
  return match ? match[1] : "";
}

async function patchOneRecord(id, fields, removedGlobal) {
  const working = { ...fields };
  const maxRetries = Math.max(20, Object.keys(working).length + 5);
  let attempts = 0;

  while (attempts <= maxRetries) {
    finalizeCompanyProfileFieldsForAirtableWrite(working, { loud: false });
    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(TABLE)}/${id}`;
    const res = await fetch(url, {
      method: "PATCH",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ fields: working, typecast: false }),
    });
    const json = await res.json();
    if (res.ok) return { removed: removedGlobal };
    const unknown = extractUnknownFieldName(json);
    if (!unknown || !Object.prototype.hasOwnProperty.call(working, unknown)) {
      throw new Error(JSON.stringify(json));
    }
    delete working[unknown];
    if (!removedGlobal.includes(unknown)) removedGlobal.push(unknown);
    attempts += 1;
    if (!Object.keys(working).length) {
      throw new Error(`No writable fields left for ${id} after stripping unknown columns`);
    }
  }
  throw new Error(`Exceeded unknown-field retries for ${id}`);
}

if (!baseId || !apiKey) {
  console.error("Missing AIRTABLE_BASE_ID or AIRTABLE_API_KEY");
  process.exit(1);
}

const records = await fetchAll();
const planned = [];

for (const rec of records) {
  if (onlyId && rec.id !== onlyId) continue;
  const f = rec.fields || {};
  const name = String(f["Company Name"] || "(no name)").trim();
  const { patch, reasons } = buildCompanyProfileOwnerOperatorBackfillPatch(f);
  if (!Object.keys(patch).length) continue;

  const working = { ...patch };
  finalizeCompanyProfileFieldsForAirtableWrite(working, { loud: false });

  planned.push({
    id: rec.id,
    name,
    fields: working,
    reasons,
  });
}

console.log(apply ? "APPLY mode (typecast: false)" : "DRY-RUN");
console.log(`Planned updates: ${planned.length} of ${records.length} companies\n`);

for (const p of planned.sort((a, b) => a.name.localeCompare(b.name))) {
  console.log(`${p.name} (${p.id})`);
  console.log("  fields:", JSON.stringify(p.fields));
  console.log("  reasons:", p.reasons.join("; "));
}

const logPath = join("reports", "company-profile-oo-fields-backfill-log.json");
writeFileSync(
  logPath,
  JSON.stringify(
    {
      generatedAt: new Date().toISOString(),
      mode: apply ? "apply" : "dry-run",
      planned,
    },
    null,
    2
  ),
  "utf8"
);
console.log("\nWrote", logPath);

if (apply && planned.length) {
  const strippedUnknown = [];
  for (let i = 0; i < planned.length; i++) {
    await patchOneRecord(planned[i].id, planned[i].fields, strippedUnknown);
    if ((i + 1) % 10 === 0 || i === planned.length - 1) {
      console.log(`Patched ${i + 1} / ${planned.length}`);
    }
  }
  if (strippedUnknown.length) {
    console.warn("Ignored unknown Airtable columns:", [...new Set(strippedUnknown)].join(", "));
  }
  console.log(`Done. Updated ${planned.length} record(s).`);
} else if (!apply) {
  console.log(`\nPass --apply to write ${planned.length} record(s).`);
}
