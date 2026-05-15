/**
 * Copy Fee Structure field values from a source brand to a target brand (by Brand Basics record id).
 *
 *   node scripts/copy-fee-structure-between-brands.mjs --from-brand-name Radisson --to-brand-name "Radisson (Choice)"
 *   node scripts/copy-fee-structure-between-brands.mjs --from recXYvwtNQGUzFZcn --to recywbx1YQSTCPqW1 --dry-run
 */
import "../load-env.js";
import Airtable from "airtable";

const TABLE_BASICS = "Brand Setup - Brand Basics";
const TABLE_FEE = "Brand Setup - Fee Structure";
const LINK = "Brand Setup - Fee Structure";

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);

function parseArgs() {
  const args = process.argv.slice(2);
  const out = { dryRun: false, from: "", to: "", fromName: "", toName: "" };
  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--dry-run") out.dryRun = true;
    else if (args[i] === "--from" && args[i + 1]) out.from = args[++i];
    else if (args[i] === "--to" && args[i + 1]) out.to = args[++i];
    else if (args[i] === "--from-brand-name" && args[i + 1]) out.fromName = args[++i];
    else if (args[i] === "--to-brand-name" && args[i + 1]) out.toName = args[++i];
  }
  return out;
}

async function findBasicsByName(name) {
  const esc = String(name).replace(/"/g, '\\"');
  const rows = await base(TABLE_BASICS)
    .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 1 })
    .firstPage();
  return rows[0] || null;
}

async function resolveId(idOrName, nameOpt) {
  if (idOrName && idOrName.startsWith("rec")) return idOrName;
  if (nameOpt) {
    const row = await findBasicsByName(nameOpt);
    if (!row) throw new Error(`Brand not found: ${nameOpt}`);
    return row.id;
  }
  throw new Error("Provide --from/--to record ids or --from-brand-name/--to-brand-name");
}

async function feeRecordIdForBrand(basicsId) {
  const basics = await base(TABLE_BASICS).find(basicsId);
  const link = basics.fields[LINK];
  if (Array.isArray(link) && link[0]) return link[0];
  throw new Error(`No Fee Structure link on brand ${basicsId} (${basics.fields["Brand Name"]})`);
}

const args = parseArgs();
const fromId = await resolveId(args.from, args.fromName);
const toId = await resolveId(args.to, args.toName);
const fromFeeId = await feeRecordIdForBrand(fromId);
const toFeeId = await feeRecordIdForBrand(toId);

const src = await base(TABLE_FEE).find(fromFeeId);
const tgt = await base(TABLE_FEE).find(toFeeId);

const skip = new Set(["Brand", "Brand Name", "Created", "Last modified"]);
const fields = {};
for (const [key, val] of Object.entries(src.fields)) {
  if (skip.has(key)) continue;
  if (/lookup|formula|rollup|count/i.test(key)) continue;
  if (val === undefined || val === null || val === "") continue;
  fields[key] = val;
}

async function patchFeeRecord(recordId, patch) {
  const keys = Object.keys(patch);
  try {
    await base(TABLE_FEE).update(recordId, patch);
    return;
  } catch (err) {
    const msg = String(err.message || err);
    const m = msg.match(/Field "([^"]+)"/);
    if (err.error === "INVALID_VALUE_FOR_COLUMN" && m) {
      const bad = m[1];
      const next = { ...patch };
      delete next[bad];
      if (!Object.keys(next).length) throw err;
      console.warn(`Skipping computed/read-only field: ${bad}`);
      return patchFeeRecord(recordId, next);
    }
    throw err;
  }
}

console.log(
  JSON.stringify(
    {
      from: { basicsId: fromId, feeId: fromFeeId, brandName: src.fields["Brand Name"] || src.fields.Brand },
      to: { basicsId: toId, feeId: toFeeId, brandName: tgt.fields["Brand Name"] || tgt.fields.Brand },
      fieldCount: Object.keys(fields).length,
      sample: {
        royalty: [fields["Min - Typical Royalty Fee Range"], fields["Max - Typical Royalty Fee Range"]],
        application: [fields["Min - Typical Application Fee"], fields["Max - Typical Application Fee"]],
      },
    },
    null,
    2
  )
);

if (args.dryRun) {
  console.log("Dry run — no update.");
  process.exit(0);
}

await patchFeeRecord(toFeeId, fields);
console.log(`Updated Fee Structure ${toFeeId} from ${fromFeeId}.`);
