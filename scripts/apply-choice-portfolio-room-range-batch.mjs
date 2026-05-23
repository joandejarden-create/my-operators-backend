/**
 * Copy Project Fit room ranges → Portfolio & Performance min/max property size (rooms).
 *
 *   node scripts/apply-choice-portfolio-room-range-batch.mjs --dry-run
 *   node scripts/apply-choice-portfolio-room-range-batch.mjs --overwrite
 */
import "../load-env.js";
import Airtable from "airtable";
import { TARGET_BRANDS } from "./lib/choice-project-fit-profiles.mjs";

const BASICS_TABLE = "Brand Setup - Brand Basics";
const PF_TABLE = "Brand Setup - Project Fit";
const PP_TABLE = "Brand Setup - Portfolio & Performance";

/** Airtable uses a non-breaking space in Maximum Property Size (Rooms) only. */
function canonicalColumnName(s) {
  return String(s).trim().replace(/\u00A0/g, " ").replace(/\u2013|\u2014/g, "-");
}

async function resolvePortfolioRoomFieldNames(base) {
  const rows = await base(PP_TABLE).select({ maxRecords: 80 }).all();
  const keys = [...new Set(rows.flatMap((r) => Object.keys(r.fields)))];
  const pick = (re) => keys.find((k) => re.test(canonicalColumnName(k))) || null;
  const minKey = pick(/^minimum property size \(rooms\)$/i);
  const maxKey = pick(/^maximum property size \(rooms\)$/i);
  if (!minKey || !maxKey) {
    throw new Error(
      `Could not resolve Portfolio room-size columns (min=${minKey}, max=${maxKey}). Found: ${keys.filter((k) => /property size/i.test(k)).join(", ")}`
    );
  }
  return { minKey, maxKey };
}

function parseArgs(argv) {
  const args = argv.slice(2);
  const bi = args.indexOf("--brand");
  return {
    dryRun: args.includes("--dry-run"),
    overwrite: args.includes("--overwrite"),
    brandFilter: bi >= 0 ? String(args[bi + 1] || "").trim() : "",
  };
}

async function createWithPruning(base, fields) {
  let payload = { ...fields };
  for (let attempt = 0; attempt < 20; attempt++) {
    try {
      const [created] = await base(PP_TABLE).create([{ fields: payload }], { typecast: true });
      return created;
    } catch (err) {
      if (err.error === "UNKNOWN_FIELD_NAME") {
        const m = String(err.message || "").match(/Unknown field name: "([^"]+)"/);
        if (m && Object.hasOwn(payload, m[1])) {
          delete payload[m[1]];
          continue;
        }
      }
      throw err;
    }
  }
  throw new Error("Could not create Portfolio row");
}

async function updateWithPruning(base, recordId, fields) {
  let payload = { ...fields };
  for (let attempt = 0; attempt < 20; attempt++) {
    if (!Object.keys(payload).length) return;
    try {
      await base(PP_TABLE).update(recordId, payload, { typecast: true });
      return;
    } catch (err) {
      if (err.error === "UNKNOWN_FIELD_NAME") {
        const m = String(err.message || "").match(/Unknown field name: "([^"]+)"/);
        if (m && Object.hasOwn(payload, m[1])) {
          delete payload[m[1]];
          continue;
        }
      }
      throw err;
    }
  }
}

async function findByName(base, table, brandName) {
  const esc = brandName.replace(/"/g, '\\"');
  const rows = await base(table)
    .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 3 })
    .all();
  return rows[0] || null;
}

async function main() {
  const { dryRun, overwrite, brandFilter } = parseArgs(process.argv);
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID
  );
  const { minKey, maxKey } = await resolvePortfolioRoomFieldNames(base);

  let brands = TARGET_BRANDS;
  if (brandFilter) brands = [brandFilter];

  for (const brandName of brands) {
    const pf = await findByName(base, PF_TABLE, brandName);
    if (!pf) {
      console.warn(`Skip ${brandName}: no Project Fit row`);
      continue;
    }
    const min = pf.get("Min - Room Count");
    const max = pf.get("Max - Room Count");
    if (min == null && max == null) {
      console.warn(`Skip ${brandName}: Project Fit has no room range`);
      continue;
    }

    let pp = await findByName(base, PP_TABLE, brandName);
    const basics = await findByName(base, BASICS_TABLE, brandName);
    const patch = {};
    if (min != null) patch[minKey] = min;
    if (max != null) patch[maxKey] = max;

    if (!pp) {
      if (dryRun) {
        console.log(`[dry-run] Would CREATE Portfolio row for ${brandName}`, patch);
        continue;
      }
      const createFields = {
        "Brand Name": brandName,
        ...patch,
      };
      if (basics) createFields.Brand = [basics.id];
      pp = await createWithPruning(base, createFields);
      console.log(`Created Portfolio ${pp.id} for ${brandName}`, patch);
      continue;
    }

    const toWrite = {};
    for (const [k, v] of Object.entries(patch)) {
      const cur = pp.get(k);
      if (overwrite || cur == null || cur === "") toWrite[k] = v;
    }
    if (!Object.keys(toWrite).length) {
      console.log(`Skip ${brandName}: Portfolio room range already set`);
      continue;
    }
    if (dryRun) {
      console.log(`[dry-run] ${brandName} Portfolio patch`, toWrite);
      continue;
    }
    await updateWithPruning(base, pp.id, toWrite);
    console.log(`Updated ${brandName} Portfolio`, toWrite);
  }
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
