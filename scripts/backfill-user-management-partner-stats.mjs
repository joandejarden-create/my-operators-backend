/**
 * Backfill Partner Directory stats on User Management (sample integers per row).
 *
 * Airtable columns (Number, precision 0):
 *   - Closed Deals
 *   - Unique Brands (Deals)  — count of unique brands across deals that person represents
 *   - Submitted Bids
 *
 * Create columns first (manually or): node scripts/add-user-management-partner-stats-fields.mjs
 *
 * Usage:
 *   node scripts/backfill-user-management-partner-stats.mjs           # dry-run
 *   node scripts/backfill-user-management-partner-stats.mjs --apply   # write
 *   node scripts/backfill-user-management-partner-stats.mjs --apply --force  # overwrite existing values
 *
 * Requires: AIRTABLE_API_KEY, AIRTABLE_BASE_ID (via load-env / .env)
 */
import "../load-env.js";
import Airtable from "airtable";

const TABLE_ID = process.env.USER_MANAGEMENT_TABLE_ID || "tblQEpYKf2aYNKKjw";

const F_CLOSED = "Closed Deals";
const F_BRANDS = "Unique Brands (Deals)";
const F_BIDS = "Submitted Bids";

function hashRecordId(recordId) {
  let h = 0;
  for (let i = 0; i < recordId.length; i++) {
    h = Math.imul(31, h) + recordId.charCodeAt(i);
    h |= 0;
  }
  return Math.abs(h);
}

/**
 * Deterministic demo-only stats (replace with real figures from Deals / CRM).
 *
 * Realistic pattern: people submit many more bids than they win. When closed > 0, bids are
 * derived as ~1.5×–4× closed (plus scatter), always strictly greater than closed.
 */
function sampleStats(recordId) {
  const h = hashRecordId(recordId);
  const closed = h % 23; // 0–22 wins in this demo slice

  let bids;
  if (closed === 0) {
    bids = (h >>> 10) % 22; // early pipeline / no wins yet
  } else {
    // bids-per-win spread in tenths: 1.5 .. 4.2 (typical “lots of shots, fewer wins”)
    const bpwTenths = 15 + ((h >>> 6) % 28); // 15–42 → 1.5–4.2
    bids = Math.ceil((closed * bpwTenths) / 10);
    bids += (h >>> 12) % Math.max(3, Math.floor(closed / 2) + 2);
    bids = Math.max(closed + 1, bids);
    // Avoid cartoonish ratios for high win counts in demo data
    bids = Math.min(bids, Math.max(closed + 2, closed * 6));
    bids = Math.min(bids, 180);
  }

  const brands =
    closed === 0
      ? ((h >>> 15) % 11 === 0 ? 0 : 1 + ((h >>> 15) % 9))
      : 1 + ((h >>> 20) % Math.min(14, closed + 4));

  return { [F_CLOSED]: closed, [F_BRANDS]: brands, [F_BIDS]: bids };
}

function displayName(fields) {
  const fn = fields["First Name"] || "";
  const ln = fields["Last Name"] || "";
  return `${fn} ${ln}`.trim() || "(no name)";
}

function statsNeedFill(fields, force) {
  if (force) return true;
  const c = fields[F_CLOSED];
  const b = fields[F_BRANDS];
  const s = fields[F_BIDS];
  const missing = (v) => v == null || v === "";
  return missing(c) || missing(b) || missing(s);
}

async function main() {
  const apply = process.argv.includes("--apply");
  const force = process.argv.includes("--force");
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
    if (!statsNeedFill(f, force)) continue;
    const stats = sampleStats(rec.id);
    toUpdate.push({
      id: rec.id,
      fields: stats,
      name: displayName(f),
    });
  }

  console.log(
    `Found ${records.length} User Management rows; ${toUpdate.length} to update` +
      (force ? " (--force: all sampled rows)." : " (missing any of the three stats).")
  );
  if (toUpdate.length === 0) {
    console.log("Nothing to do.");
    return;
  }

  for (const u of toUpdate.slice(0, 15)) {
    console.log(`  • ${u.id}  ${u.name}  →  closed=${u.fields[F_CLOSED]} brands=${u.fields[F_BRANDS]} bids=${u.fields[F_BIDS]}`);
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
