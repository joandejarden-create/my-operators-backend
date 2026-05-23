/**
 * Populate Brand Setup - Project Fit for Choice CHI brands.
 *
 *   node scripts/apply-choice-project-fit-batch.mjs --dry-run
 *   node scripts/apply-choice-project-fit-batch.mjs --overwrite
 *   node scripts/apply-choice-project-fit-batch.mjs --overwrite --brand "Cambria Hotels"
 */
import "../load-env.js";
import Airtable from "airtable";
import { TARGET_BRANDS, buildProjectFitFormForBrand } from "./lib/choice-project-fit-profiles.mjs";
import { projectFitFormToAirtableFields } from "./lib/choice-project-fit-builder.mjs";

const BASICS_TABLE = "Brand Setup - Brand Basics";
const PF_TABLE = "Brand Setup - Project Fit";

function parseArgs(argv) {
  const args = argv.slice(2);
  const bi = args.indexOf("--brand");
  return {
    dryRun: args.includes("--dry-run"),
    overwrite: args.includes("--overwrite"),
    brandFilter: bi >= 0 ? String(args[bi + 1] || "").trim() : "",
  };
}

function isEmpty(v) {
  if (v == null || v === "") return true;
  if (Array.isArray(v) && !v.length) return true;
  return false;
}

function patchRecord(record, fields, { overwrite = false } = {}) {
  const patch = {};
  const skipped = [];
  for (const [key, value] of Object.entries(fields)) {
    if (value == null || value === "") continue;
    if (Array.isArray(value) && !value.length && !overwrite) continue;
    if (!overwrite && !isEmpty(record.get(key))) {
      skipped.push(key);
      continue;
    }
    patch[key] = value;
  }
  return { patch, skipped };
}

async function findBasics(base, brandName) {
  const esc = brandName.replace(/"/g, '\\"');
  const rows = await base(BASICS_TABLE)
    .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 5 })
    .all();
  return rows.find((r) =>
    String(r.get("Parent Company") || "").includes("Choice Hotels International")
  );
}

async function findProjectFit(base, brandName, basicsId) {
  const esc = brandName.replace(/"/g, '\\"');
  const byName = await base(PF_TABLE)
    .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 5 })
    .all();
  if (byName[0]) return byName[0];

  for (const linkField of ["Brand", "Brand_Basic_ID", "Brand Setup - Brand Basics", "Brand Basics"]) {
    try {
      const formula = `FIND("${basicsId}", ARRAYJOIN({${linkField}})) > 0`;
      const rows = await base(PF_TABLE).select({ filterByFormula: formula, maxRecords: 1 }).all();
      if (rows[0]) return rows[0];
    } catch {
      /* field may not exist */
    }
  }
  return null;
}

async function createProjectFit(base, brandName, basicsId, fields) {
  const tries = [
    { "Brand Name": brandName, Brand: [basicsId], ...fields },
    { "Brand Name": brandName, Brand_Basic_ID: [basicsId], ...fields },
    { "Brand Name": brandName, "Brand Setup - Brand Basics": [basicsId], ...fields },
    { "Brand Name": brandName, ...fields },
  ];
  let lastErr;
  for (const payload of tries) {
    try {
      const [created] = await base(PF_TABLE).create([{ fields: payload }], { typecast: true });
      return created;
    } catch (err) {
      lastErr = err;
      if (err.error === "UNKNOWN_FIELD_NAME") {
        const msg = String(err.message || "");
        const m = msg.match(/Unknown field name: "([^"]+)"/);
        if (m) delete payload[m[1]];
        continue;
      }
      throw err;
    }
  }
  throw lastErr || new Error(`Could not create Project Fit for ${brandName}`);
}

async function updateWithPruning(base, recordId, fields) {
  let payload = { ...fields };
  for (let attempt = 0; attempt < 80; attempt++) {
    if (!Object.keys(payload).length) return;
    try {
      await base(PF_TABLE).update(recordId, payload, { typecast: true });
      return;
    } catch (err) {
      if (err.error === "UNKNOWN_FIELD_NAME") {
        const msg = String(err.message || "");
        const m = msg.match(/Unknown field name: "([^"]+)"/);
        if (m && Object.hasOwn(payload, m[1])) {
          delete payload[m[1]];
          continue;
        }
      }
      if (err.error === "INVALID_MULTIPLE_CHOICE_OPTIONS") {
        const msg = String(err.message || "");
        const m = msg.match(/option "([^"]+)"/);
        if (m) {
          for (const [k, v] of Object.entries(payload)) {
            if (Array.isArray(v)) {
              const next = v.filter((x) => x !== m[1]);
              if (next.length !== v.length) payload[k] = next;
            }
          }
          continue;
        }
      }
      throw err;
    }
  }
}

async function main() {
  const { dryRun, overwrite, brandFilter } = parseArgs(process.argv);
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID
  );

  let brands = TARGET_BRANDS;
  if (brandFilter) {
    if (!brands.includes(brandFilter)) throw new Error(`Unknown brand: ${brandFilter}`);
    brands = [brandFilter];
  }

  let updated = 0;
  let created = 0;

  for (const brandName of brands) {
    const form = buildProjectFitFormForBrand(brandName);
    if (!form) {
      console.warn(`Skip ${brandName}: no profile`);
      continue;
    }
    const fields = projectFitFormToAirtableFields(form);

    const basics = await findBasics(base, brandName);
    if (!basics) {
      console.warn(`Skip ${brandName}: no CHI Brand Basics row`);
      continue;
    }

    let pf = await findProjectFit(base, brandName, basics.id);
    if (!pf) {
      if (dryRun) {
        console.log(`[dry-run] Would CREATE Project Fit for ${brandName} (${Object.keys(fields).length} fields)`);
        created++;
        continue;
      }
      pf = await createProjectFit(base, brandName, basics.id, fields);
      console.log(`Created Project Fit ${pf.id} for ${brandName} (${Object.keys(fields).length} fields)`);
      created++;
      continue;
    }

    const { patch, skipped } = patchRecord(pf, fields, { overwrite });
    if (!Object.keys(patch).length) {
      console.log(`Skip ${brandName}: nothing to patch (${skipped.length} existing)`);
      continue;
    }

    if (dryRun) {
      console.log(
        `[dry-run] ${brandName} (${pf.id}) would patch ${Object.keys(patch).length} fields` +
          (skipped.length ? `; skip ${skipped.length}` : "")
      );
      updated++;
      continue;
    }

    await updateWithPruning(base, pf.id, patch);
    console.log(`Updated ${brandName} (${pf.id}): ${Object.keys(patch).length} fields`);
    if (skipped.length && !overwrite) console.log(`  kept existing: ${skipped.length}`);
    updated++;
  }

  console.log(`\nDone. ${created} created, ${updated} updated/patched.`);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
