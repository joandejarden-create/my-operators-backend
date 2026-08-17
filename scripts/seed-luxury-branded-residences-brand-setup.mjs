/**
 * Seed Brand Setup for luxury brands missing from the base (Four Seasons, Aman, Design Hotels).
 * Creates Brand Basics + empty linked child tabs; seeds Project Fit branded-residence fields.
 *
 *   node scripts/seed-luxury-branded-residences-brand-setup.mjs --dry-run
 *   node scripts/seed-luxury-branded-residences-brand-setup.mjs --apply
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import "../load-env.js";
import Airtable from "airtable";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const DEFAULT_FIXTURE_PATH = path.join(ROOT, "fixtures", "luxury-branded-residences-brand-setup.json");

const BASICS_TABLE = "Brand Setup - Brand Basics";

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

function parseArgs(argv) {
  const args = argv.slice(2);
  const flags = new Set(args.filter((a) => a.startsWith("--")));
  let fixture = DEFAULT_FIXTURE_PATH;
  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--fixture" && args[i + 1]) {
      fixture = path.isAbsolute(args[i + 1]) ? args[i + 1] : path.join(ROOT, args[i + 1]);
      break;
    }
  }
  return {
    dryRun: flags.has("--dry-run"),
    apply: flags.has("--apply"),
    fixturePath: fixture,
  };
}

function isEmpty(v) {
  if (v == null || v === "") return true;
  if (Array.isArray(v) && !v.length) return true;
  return false;
}

function extractBadFieldName(err) {
  const msg = String(err?.message || err || "");
  const m =
    msg.match(/Unknown field name:\s*['"](.+?)['"]/i) ||
    msg.match(/Field\s+['"](.+?)['"]\s+cannot accept/i) ||
    msg.match(/Unknown field name:\s*(.+?)(?:\s|$)/i);
  return m ? m[1].trim() : null;
}

async function writeWithFieldPruning(base, table, recordId, fields, { typecast = true, create = false } = {}) {
  let payload = { ...fields };
  const removed = [];
  const api = base(table);
  const opts = typecast ? { typecast: true } : undefined;
  for (let attempt = 0; attempt < 80; attempt++) {
    if (!Object.keys(payload).length) return { record: null, removed };
    try {
      if (create) {
        const [created] = await api.create([{ fields: payload }], opts);
        return { record: created, removed };
      }
      await api.update(recordId, payload, opts);
      return { record: null, removed };
    } catch (err) {
      const bad = extractBadFieldName(err);
      if (bad && Object.prototype.hasOwnProperty.call(payload, bad)) {
        delete payload[bad];
        removed.push(bad);
        continue;
      }
      throw err;
    }
  }
  throw new Error(`writeWithFieldPruning exceeded retries for ${table}`);
}

async function findBasicsByName(base, brandName) {
  const esc = brandName.replace(/"/g, '\\"');
  const rows = await base(BASICS_TABLE)
    .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 5 })
    .all();
  if (rows.length > 1) {
    throw new Error(`Multiple Basics rows for "${brandName}": ${rows.map((r) => r.id).join(", ")}`);
  }
  return rows[0] || null;
}

function patchMissingOnly(record, fields) {
  const patch = {};
  const skipped = [];
  for (const [key, value] of Object.entries(fields)) {
    if (value == null || value === "") continue;
    if (!isEmpty(record.get(key))) {
      skipped.push(key);
      continue;
    }
    patch[key] = value;
  }
  return { patch, skipped };
}

async function createMinimalLinkedRow(base, table, brandName, basicsId) {
  const tries = [
    { "Brand Name": brandName, Brand: [basicsId] },
    { "Brand Name": brandName, Brand_Basic_ID: [basicsId] },
    { "Brand Name": brandName, "Brand Setup - Brand Basics": [basicsId] },
    { "Brand Name": brandName },
  ];
  let lastErr;
  for (const fields of tries) {
    try {
      const { record } = await writeWithFieldPruning(base, table, null, fields, { create: true });
      return record;
    } catch (err) {
      lastErr = err;
      if (/unknown field/i.test(String(err.message))) continue;
      throw err;
    }
  }
  throw lastErr || new Error(`Could not create row in ${table}`);
}

async function ensureChildLinks(base, basicsRec, brandName, dryRun) {
  const basicsId = basicsRec.id;
  const basicsPatch = {};

  for (const { linkField, table } of LINKED_TABLES) {
    const existing = basicsRec.get(linkField);
    if (Array.isArray(existing) && existing[0]) continue;

    const orphan = await base(table)
      .select({
        filterByFormula: `{Brand Name} = "${brandName.replace(/"/g, '\\"')}"`,
        maxRecords: 1,
      })
      .firstPage();
    if (orphan.length) {
      basicsPatch[linkField] = [orphan[0].id];
      continue;
    }
    if (dryRun) {
      basicsPatch[linkField] = ["recDRYRUN_CHILD"];
      continue;
    }
    const child = await createMinimalLinkedRow(base, table, brandName, basicsId);
    basicsPatch[linkField] = [child.id];
    console.log(`  linked ${table}: ${child.id}`);
  }

  if (!dryRun && Object.keys(basicsPatch).length) {
    await base(BASICS_TABLE).update(basicsId, basicsPatch);
    return await base(BASICS_TABLE).find(basicsId);
  }
  return basicsRec;
}

async function applyChildTables(base, basicsRec, brandSpec, dryRun) {
  const brandName = brandSpec.brandName;
  const tables = brandSpec.childTables || {};
  const report = [];

  for (const { linkField, table } of LINKED_TABLES) {
    const fields = tables[table];
    if (!fields || !Object.keys(fields).length) continue;

    let childId = Array.isArray(basicsRec.get(linkField)) ? basicsRec.get(linkField)[0] : null;
    if (!childId && !dryRun) {
      const child = await createMinimalLinkedRow(base, table, brandName, basicsRec.id);
      childId = child.id;
      await base(BASICS_TABLE).update(basicsRec.id, { [linkField]: [childId] });
      console.log(`  created+linked ${table}: ${childId}`);
    }

    if (dryRun) {
      console.log(`  ${table}: would write`, Object.keys(fields).join(", "));
      report.push({ table, dryRun: true, fields: Object.keys(fields) });
      continue;
    }

    if (!childId) {
      console.warn(`  skip ${table}: no linked record`);
      continue;
    }

    const payload = { ...fields, "Brand Name": brandName };
    const existing = await base(table).find(childId);
    const { patch } = patchMissingOnly(existing, payload);
    if (!Object.keys(patch).length) {
      console.log(`  ${table}: nothing to patch`);
      continue;
    }
    const { removed } = await writeWithFieldPruning(base, table, childId, patch);
    console.log(`  ${table}: updated ${childId}`, Object.keys(patch).join(", "));
    if (removed.length) console.warn(`    dropped fields:`, removed.join(", "));
    report.push({ table, recordId: childId, fields: Object.keys(patch), removed });
  }

  return report;
}

async function main() {
  const opts = parseArgs(process.argv);
  if (!opts.apply && !opts.dryRun) {
    console.error("Pass --dry-run or --apply");
    process.exit(1);
  }
  const dryRun = opts.dryRun && !opts.apply;
  const spec = JSON.parse(fs.readFileSync(opts.fixturePath, "utf8"));

  const key = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!key || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");

  const base = new Airtable({ apiKey: key }).base(baseId);
  const summary = [];

  console.log(dryRun ? "[dry-run]" : "[apply]", "luxury branded-residence brands");
  console.log("Fixture:", opts.fixturePath, "\n");

  for (const brand of spec.brands || []) {
    const brandName = String(brand.brandName || "").trim();
    if (!brandName) continue;
    const fields = { "Brand Name": brandName, ...(brand.fields || {}) };

    console.log(`Brand: ${brandName}`);
    let basicsRec = await findBasicsByName(base, brandName);
    let action = "exists";

    if (!basicsRec) {
      action = "create";
      if (dryRun) {
        console.log("  Basics: would create");
        basicsRec = { id: "recDRYRUN_BASICS", get: () => undefined };
      } else {
        const { record, removed } = await writeWithFieldPruning(base, BASICS_TABLE, null, fields, {
          create: true,
        });
        basicsRec = record;
        console.log(`  Basics: created ${basicsRec.id}`);
        if (removed.length) console.warn(`    dropped fields:`, removed.join(", "));
      }
    } else {
      const { patch, skipped } = patchMissingOnly(basicsRec, fields);
      if (dryRun) {
        console.log(`  Basics: exists ${basicsRec.id}; would patch`, Object.keys(patch).join(", ") || "(none)");
        if (skipped.length) console.log("  skip (already set):", skipped.join(", "));
      } else if (Object.keys(patch).length) {
        const { removed } = await writeWithFieldPruning(base, BASICS_TABLE, basicsRec.id, patch);
        console.log(`  Basics: patched ${basicsRec.id}`, Object.keys(patch).join(", "));
        if (removed.length) console.warn(`    dropped fields:`, removed.join(", "));
        basicsRec = await base(BASICS_TABLE).find(basicsRec.id);
      } else {
        console.log(`  Basics: exists ${basicsRec.id} (no empty fields to patch)`);
      }
    }

    basicsRec = await ensureChildLinks(base, basicsRec, brandName, dryRun);
    if (!dryRun && basicsRec?.id?.startsWith("rec")) {
      basicsRec = await base(BASICS_TABLE).find(basicsRec.id);
    }

    const childReport = await applyChildTables(base, basicsRec, brand, dryRun);
    summary.push({
      brandName,
      basicsId: basicsRec.id,
      action,
      childReport,
      brandedResidencesStatus: fields["Branded Residences Status"] || null,
      projectFitAllowed: brand.childTables?.["Brand Setup - Project Fit"]?.["Branded Residences Allowed"] || null,
    });
    console.log("");
  }

  const out = path.join(ROOT, "reports", "luxury-branded-residences-brand-setup-seed.json");
  fs.mkdirSync(path.dirname(out), { recursive: true });
  fs.writeFileSync(
    out,
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        dryRun,
        fixturePath: opts.fixturePath,
        brands: summary,
      },
      null,
      2
    )
  );
  console.log("Wrote", out);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
