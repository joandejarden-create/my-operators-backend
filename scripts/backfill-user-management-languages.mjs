/**
 * Backfill Airtable User Management "Languages" for rows where it is empty.
 * Uses varied 1–3 language combinations typical for hospitality / CALA roles.
 *
 * Usage:
 *   node scripts/backfill-user-management-languages.mjs           # dry-run
 *   node scripts/backfill-user-management-languages.mjs --apply   # write to Airtable
 *
 * Uses { typecast: true } so new multi-select options can be created if needed.
 *
 * Requires: AIRTABLE_API_KEY, AIRTABLE_BASE_ID (via load-env / .env)
 */
import "../load-env.js";
import Airtable from "airtable";

const TABLE_ID = process.env.USER_MANAGEMENT_TABLE_ID || "tblQEpYKf2aYNKKjw";
const FIELD = "Languages";

function hashRecordId(recordId) {
  let h = 0;
  for (let i = 0; i < recordId.length; i++) {
    h = Math.imul(31, h) + recordId.charCodeAt(i);
    h |= 0;
  }
  return Math.abs(h);
}

function pickLanguages(recordId, combos) {
  const idx = hashRecordId(recordId) % combos.length;
  return combos[idx];
}

function languagesIsMissing(fields) {
  const v = fields[FIELD];
  if (v == null || v === "") return true;
  if (Array.isArray(v) && v.length === 0) return true;
  if (typeof v === "string" && !String(v).trim()) return true;
  return false;
}

function displayName(fields) {
  const fn = fields["First Name"] || "";
  const ln = fields["Last Name"] || "";
  return `${fn} ${ln}`.trim() || "(no name)";
}

/** Each entry is a multi-select value (array of language names). Mix singles, pairs, triples. */
const LANGUAGE_COMBOS = [
  ["English"],
  ["Spanish"],
  ["Portuguese"],
  ["English", "Spanish"],
  ["English", "Portuguese"],
  ["Spanish", "Portuguese"],
  ["English", "Spanish", "Portuguese"],
  ["English", "French"],
  ["Spanish", "French"],
  ["English", "German"],
  ["Spanish", "English", "French"],
  ["Portuguese", "English"],
  ["English", "Italian"],
  ["Spanish", "German"],
  ["Dutch", "English"],
  ["English", "Spanish", "Italian"],
  ["French", "Portuguese"],
  ["English", "Spanish", "French"],
  ["Spanish", "Italian"],
  ["English", "Dutch"],
  ["Creole", "English", "French"],
  ["English", "Mandarin"],
  ["Spanish", "English", "German"],
];

function formatLanguagesForLog(value) {
  if (Array.isArray(value)) return value.join(", ");
  return String(value);
}

async function main() {
  const apply = process.argv.includes("--apply");
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) {
    console.error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");
    process.exit(1);
  }

  const base = new Airtable({ apiKey }).base(baseId);
  const table = base(TABLE_ID);

  const records = [];
  await new Promise((resolve, reject) => {
    table
      .select({ pageSize: 100 })
      .eachPage(
        (rows, next) => {
          records.push(...rows);
          next();
        },
        (err) => (err ? reject(err) : resolve())
      );
  });

  const toUpdate = [];
  for (const rec of records) {
    const f = rec.fields || {};
    if (!languagesIsMissing(f)) continue;
    const langs = pickLanguages(rec.id, LANGUAGE_COMBOS);
    toUpdate.push({
      id: rec.id,
      fields: { [FIELD]: langs },
      name: displayName(f),
    });
  }

  console.log(`Found ${records.length} User Management rows; ${toUpdate.length} missing "${FIELD}".`);
  if (toUpdate.length === 0) {
    return;
  }

  for (const u of toUpdate.slice(0, 15)) {
    console.log(`  • ${u.id}  ${u.name}  →  ${formatLanguagesForLog(u.fields[FIELD])}`);
  }
  if (toUpdate.length > 15) {
    console.log(`  … and ${toUpdate.length - 15} more`);
  }

  if (!apply) {
    console.log("\nDry-run only. Re-run with --apply to write to Airtable.");
    return;
  }

  const BATCH = 10;
  for (let i = 0; i < toUpdate.length; i += BATCH) {
    const chunk = toUpdate.slice(i, i + BATCH).map(({ id, fields }) => ({ id, fields }));
    await table.update(chunk, { typecast: true });
    console.log(`Updated ${Math.min(i + BATCH, toUpdate.length)} / ${toUpdate.length}`);
  }
  console.log("Done.");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
