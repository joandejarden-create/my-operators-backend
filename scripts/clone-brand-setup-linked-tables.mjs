/**
 * Clone all Brand Setup child-table rows linked from source Basics → target Basics
 * (Footprint, Fee Structure, Standards, Deal Terms, etc.) and wire link fields on Basics.
 *
 * Usage:
 *   node scripts/clone-brand-setup-linked-tables.mjs --from "Radisson (Choice)" --to "Radisson Blu (Choice)" --dry-run
 *   node scripts/clone-brand-setup-linked-tables.mjs --from "Radisson (Choice)" --to "Radisson Blu (Choice)"
 */
import "../load-env.js";
import Airtable from "airtable";

const BASICS = "Brand Setup - Brand Basics";

/** Basics column name → child table (same name as link field on Basics row). */
const LINKED_TABLES = [
  { linkField: "Brand Setup - Sustainability & ESG", table: "Brand Setup - Sustainability & ESG" },
  { linkField: "Brand Setup - Brand Footprint", table: "Brand Setup - Brand Footprint" },
  { linkField: "Brand Setup - Project Fit", table: "Brand Setup - Project Fit" },
  { linkField: "Brand Setup - Portfolio & Performance", table: "Brand Setup - Portfolio & Performance" },
  { linkField: "Brand Setup - Brand Standards", table: "Brand Setup - Brand Standards" },
  { linkField: "Brand Setup - Fee Structure", table: "Brand Setup - Fee Structure" },
  { linkField: "Brand Setup - Deal Terms", table: "Brand Setup - Deal Terms" },
  { linkField: "Brand Setup - Operational Support", table: "Brand Setup - Operational Support" },
  { linkField: "Brand Setup - Legal Terms", table: "Brand Setup - Legal Terms" },
  { linkField: "Brand Setup - Loyalty & Commercial", table: "Brand Setup - Loyalty & Commercial" },
];

const BRAND_LINK_CANDIDATES = ["Brand", "Brand_Basic_ID", "Brand Setup - Brand Basics", "Brand Basics"];

const SKIP_FIELD = /^(Created|Last modified|Record_ID|Record ID)$/i;
const SKIP_FIELD_PATTERN = /(Completion Rate|lookup|formula|rollup|count)/i;

function parseArgs() {
  const argv = process.argv.slice(2);
  let dryRun = false;
  let from = "";
  let to = "";
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "--dry-run") dryRun = true;
    else if (argv[i] === "--from" && argv[i + 1]) from = argv[++i].trim();
    else if (argv[i] === "--to" && argv[i + 1]) to = argv[++i].trim();
  }
  if (!from || !to) throw new Error("Require --from and --to brand names.");
  return { dryRun, from, to };
}

function getBase() {
  return new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);
}

async function findBasics(base, name) {
  const esc = name.replace(/"/g, '\\"');
  const rows = await base(BASICS)
    .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 1 })
    .all();
  if (rows.length !== 1) throw new Error(`Expected one Basics row for "${name}", got ${rows.length}`);
  return rows[0];
}

function copyFields(fromRec, targetBrandName) {
  const out = {};
  for (const [key, val] of Object.entries(fromRec.fields || {})) {
    if (SKIP_FIELD.test(key) || SKIP_FIELD_PATTERN.test(key)) continue;
    if (BRAND_LINK_CANDIDATES.includes(key)) continue;
    if (key === "Brand Name") continue;
    if (val == null || val === "") continue;
    if (Array.isArray(val) && val.length && typeof val[0] === "object" && val[0].url) {
      continue;
    }
    out[key] = val;
  }
  return out;
}

async function createMinimalLinkedRow(base, table, targetBrandName, targetBasicsId) {
  const tries = [
    { "Brand Name": targetBrandName, Brand: [targetBasicsId] },
    { "Brand Name": targetBrandName, Brand_Basic_ID: [targetBasicsId] },
    { "Brand Name": targetBrandName, "Brand Setup - Brand Basics": [targetBasicsId] },
    { "Brand Name": targetBrandName },
  ];
  let lastErr;
  for (const fields of tries) {
    try {
      const [created] = await base(table).create([{ fields }]);
      return created;
    } catch (err) {
      lastErr = err;
      if (err.error === "UNKNOWN_FIELD_NAME") continue;
      throw err;
    }
  }
  throw lastErr || new Error(`Could not create minimal row in ${table}`);
}

async function patchWithFieldPruning(base, table, recordId, fields, { typecast = false } = {}) {
  let payload = { ...fields };
  const opts = typecast ? { typecast: true } : undefined;
  for (let attempt = 0; attempt < 80; attempt++) {
    if (!Object.keys(payload).length) return;
    try {
      await base(table).update(recordId, payload, opts);
      return;
    } catch (err) {
      const msg = String(err.message || err);
      const m1 = msg.match(/Field "([^"]+)"/);
      const m2 = msg.match(/Unknown field name:\s*['"]?([^'"]+)['"]?/i);
      const m3 = msg.match(/Field\s+['"](.+?)['"]\s+cannot accept/i);
      const bad = (m1 && m1[1]) || (m2 && m2[1]) || (m3 && m3[1]);
      if (
        bad &&
        (err.error === "INVALID_VALUE_FOR_COLUMN" ||
          err.error === "UNKNOWN_FIELD_NAME" ||
          /cannot accept/i.test(msg))
      ) {
        if (!(bad in payload)) throw err;
        delete payload[bad];
        continue;
      }
      throw err;
    }
  }
  throw new Error(`Could not update ${table} ${recordId}`);
}

async function cloneChildRow(base, table, srcRec, targetBrandName, targetBasicsId, { typecast = false } = {}) {
  const created = await createMinimalLinkedRow(base, table, targetBrandName, targetBasicsId);
  const dataFields = copyFields(srcRec, targetBrandName);
  await patchWithFieldPruning(base, table, created.id, dataFields, { typecast });
  return created;
}

async function main() {
  const { dryRun, from, to } = parseArgs();
  const base = getBase();
  const fromBasics = await findBasics(base, from);
  const toBasics = await findBasics(base, to);

  console.log(`From: ${from} (${fromBasics.id})`);
  console.log(`To:   ${to} (${toBasics.id})`);
  if (dryRun) console.log("[dry-run]\n");

  const basicsPatch = {};

  for (const { linkField, table } of LINKED_TABLES) {
    const existingOnTarget = toBasics.get(linkField);
    if (Array.isArray(existingOnTarget) && existingOnTarget[0]) {
      const tgtId = existingOnTarget[0];
      const srcIds = fromBasics.get(linkField);
      const srcId = Array.isArray(srcIds) ? srcIds[0] : null;
      if (srcId && !dryRun) {
        const srcRec = await base(table).find(srcId);
        const useTypecast = table === "Brand Setup - Brand Footprint" || table === "Brand Setup - Project Fit";
        await patchWithFieldPruning(base, table, tgtId, copyFields(srcRec, to), { typecast: useTypecast });
        console.log(`SYNC ${table}: refreshed ${tgtId} from ${srcId}`);
      } else {
        console.log(`SKIP ${table}: target already linked (${tgtId})`);
      }
      basicsPatch[linkField] = [tgtId];
      continue;
    }

    const srcIds = fromBasics.get(linkField);
    const srcId = Array.isArray(srcIds) ? srcIds[0] : null;
    if (!srcId) {
      console.log(`SKIP ${table}: no link on source Basics`);
      continue;
    }

    const existingCount = await base(table)
      .select({
        filterByFormula: `OR({Brand} = "${toBasics.id}", {Brand Name} = "${to.replace(/"/g, '\\"')}")`,
        maxRecords: 1,
      })
      .firstPage();
    if (existingCount.length) {
      console.log(`SKIP ${table}: child row already exists (${existingCount[0].id})`);
      basicsPatch[linkField] = [existingCount[0].id];
      continue;
    }

    const srcRec = await base(table).find(srcId);
    const dataFieldCount = Object.keys(copyFields(srcRec, to)).length;
    const useTypecast = table === "Brand Setup - Brand Footprint" || table === "Brand Setup - Project Fit";

    if (dryRun) {
      console.log(`Would create ${table} from ${srcId} (${dataFieldCount} data fields, typecast=${useTypecast})`);
      basicsPatch[linkField] = ["recDRYRUN"];
      continue;
    }

    const created = await cloneChildRow(base, table, srcRec, to, toBasics.id, { typecast: useTypecast });
    basicsPatch[linkField] = [created.id];
    console.log(`Created ${table}: ${created.id} (from ${srcId})`);
  }

  if (!dryRun && Object.keys(basicsPatch).length) {
    await base(BASICS).update(toBasics.id, basicsPatch);
    console.log(`Updated Basics ${toBasics.id} with ${Object.keys(basicsPatch).length} link(s).`);
  }
  console.log("Done.");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
