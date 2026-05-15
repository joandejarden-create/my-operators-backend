/**
 * Backfill Airtable User Management "Coverage Territories" for rows where it is empty.
 * Values are CALA-focused: countries, subregions, zones, and multi-country groupings.
 *
 * Usage:
 *   node scripts/backfill-user-management-coverage-territories.mjs           # dry-run (prints plan)
 *   node scripts/backfill-user-management-coverage-territories.mjs --apply   # write to Airtable
 *
 * Requires: AIRTABLE_API_KEY, AIRTABLE_BASE_ID (via load-env / .env)
 */
import "../load-env.js";
import Airtable from "airtable";

const TABLE_ID = process.env.USER_MANAGEMENT_TABLE_ID || "tblQEpYKf2aYNKKjw";
const FIELD = "Coverage Territories";

/** Deterministic pick from record id so re-runs stay stable per row. */
function pickCoverage(recordId, options) {
  let h = 0;
  for (let i = 0; i < recordId.length; i++) {
    h = Math.imul(31, h) + recordId.charCodeAt(i);
    h |= 0;
  }
  const idx = Math.abs(h) % options.length;
  return options[idx];
}

function coverageIsMissing(fields) {
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

// Mix: single countries, subregions, corridors, and country groupings — all within CALA.
const CALA_COVERAGE_OPTIONS = [
  "Mexico — CDMX & central highlands",
  "Mexico — Baja California & Los Cabos corridor",
  "Mexico — Pacific coast (Puerto Vallarta to Manzanillo)",
  "Mexico — Quintana Roo & Riviera Maya",
  "Mexico, Guatemala & Belize triangle",
  "Central America — CA-4 + Costa Rica",
  "Panama & Costa Rica — Pacific belt",
  "Greater Antilles — Cuba, Jamaica, Hispaniola",
  "Dominican Republic & Puerto Rico",
  "Eastern Caribbean — Leeward & Windward islands",
  "Trinidad & Tobago — southern Caribbean hub",
  "Colombia — Bogotá, Medellín & coffee axis",
  "Colombia — Caribbean coast (CTG, SMR, BAQ)",
  "Venezuela & Dutch Caribbean gateways",
  "Ecuador — Quito, Guayaquil & Pacific resorts",
  "Peru — Lima & north Pacific ports",
  "Brazil — Southeast (São Paulo, Rio, Belo Horizonte)",
  "Brazil — Northeast (Salvador, Recife, Fortaleza)",
  "Brazil — South (Curitiba, Porto Alegre)",
  "Southern Cone — Argentina & Uruguay (BA, MVD, ROS)",
  "Southern Cone — Chile (Santiago & Valparaíso)",
  "Andean south — Chile & Argentina Patagonia gateways",
  "Paraguay & Bolivia — landlocked CALA",
  "Northern South America — Guyana, Suriname & French Guiana",
  "CALA — Spanish-speaking South America (excl. Brazil pockets)",
  "CALA — Lusophone Brazil + Atlantic itineraries",
  "Caribbean & Central America — inbound US feeder markets",
  "Latin America — Pacific resort & urban mix",
];

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
    if (!coverageIsMissing(f)) continue;
    const value = pickCoverage(rec.id, CALA_COVERAGE_OPTIONS);
    toUpdate.push({ id: rec.id, fields: { [FIELD]: value }, name: displayName(f) });
  }

  console.log(`Found ${records.length} User Management rows; ${toUpdate.length} missing "${FIELD}".`);
  if (toUpdate.length === 0) {
    return;
  }

  for (const u of toUpdate.slice(0, 15)) {
    console.log(`  • ${u.id}  ${u.name}  →  ${u.fields[FIELD]}`);
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
    await table.update(chunk);
    console.log(`Updated ${Math.min(i + BATCH, toUpdate.length)} / ${toUpdate.length}`);
  }
  console.log("Done.");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
