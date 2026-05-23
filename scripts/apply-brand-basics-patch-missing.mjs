/**
 * PATCH Brand Setup - Brand Basics: write fixture fields only when Airtable cell is empty.
 * Use --overwrite to replace existing narrative fields (external-voice refresh).
 *
 *   node scripts/apply-brand-basics-patch-missing.mjs --dry-run --brand-record-id recXXX --fixture fixtures/foo.json
 *   node scripts/apply-brand-basics-patch-missing.mjs --brand-name "Cambria Hotels" --fixture fixtures/foo.json
 *   node scripts/apply-brand-basics-patch-missing.mjs --dry-run --fixture fixtures/foo.json --all-alpha-brand-studios
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import "../load-env.js";
import Airtable from "airtable";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const BASICS_TABLE = "Brand Setup - Brand Basics";

function parseArgs(argv) {
  const args = argv.slice(2);
  const flags = new Set();
  const kv = {};
  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    if (a === "--dry-run") flags.add("dry-run");
    else if (a === "--overwrite") flags.add("overwrite");
    else if (a === "--skip-target-guest-segments") flags.add("skip-target-guest-segments");
    else if (a === "--all-alpha-brand-studios") flags.add("all-alpha-brand-studios");
    else if (a.startsWith("--") && args[i + 1] && !args[i + 1].startsWith("--")) {
      kv[a.slice(2)] = args[++i];
    }
  }
  const fixtureRel = typeof kv.fixture === "string" ? kv.fixture : "";
  return {
    dryRun: flags.has("dry-run"),
    overwrite: flags.has("overwrite"),
    skipTargetGuestSegments: flags.has("skip-target-guest-segments"),
    allAlpha: flags.has("all-alpha-brand-studios"),
    brandName: String(kv["brand-name"] || "").trim(),
    brandRecordId: String(kv["brand-record-id"] || "").trim(),
    fixturePath: fixtureRel
      ? path.isAbsolute(fixtureRel)
        ? fixtureRel
        : path.resolve(ROOT, fixtureRel)
      : "",
  };
}

function getBase() {
  const key = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!key || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID in .env");
  return new Airtable({ apiKey: key }).base(baseId);
}

function isEmpty(v) {
  if (v == null || v === "") return true;
  if (Array.isArray(v) && !v.length) return true;
  return false;
}

async function findBasicsByName(base, brandName) {
  const esc = brandName.replace(/"/g, '\\"');
  const records = await base(BASICS_TABLE)
    .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 20 })
    .all();
  if (records.length === 1) return records[0];
  if (records.length > 1) {
    throw new Error(
      `Multiple Brand Basics rows for "${brandName}": ${records.map((r) => r.id).join(", ")}. Use --brand-record-id.`
    );
  }
  return null;
}

function isAlphaBrandStudios(rec) {
  return String(rec.get("Parent Company") || "")
    .toLowerCase()
    .includes("alpha brand studios");
}

async function listAlphaBrandStudios(base) {
  const rows = await base(BASICS_TABLE).select({ maxRecords: 500 }).all();
  return rows.filter(isAlphaBrandStudios);
}

/**
 * @param {import('airtable').Record} record
 * @param {Record<string, string>} fields from fixture
 */
function patchFieldsForRecord(record, fields, { overwrite = false } = {}) {
  const patch = {};
  const skipped = [];
  for (const [key, value] of Object.entries(fields)) {
    if (value == null || value === "") continue;
    const current = record.get(key);
    if (!overwrite && !isEmpty(current)) {
      skipped.push(key);
      continue;
    }
    patch[key] = value;
  }
  return { patch, skipped };
}

async function applyOne(base, opts, spec) {
  const defaultName = String(spec.targetBrandBasicsName || "").trim();
  const brandName = opts.brandName || defaultName;
  let record;
  if (opts.brandRecordId) {
    record = await base(BASICS_TABLE).find(opts.brandRecordId);
  } else {
    record = await findBasicsByName(base, brandName);
    if (!record) throw new Error(`No Brand Basics row: "${brandName}"`);
  }

  if (opts.allAlpha && !isAlphaBrandStudios(record)) {
    console.log(`Skip (not Alpha Brand Studios): ${record.get("Brand Name")}`);
    return { applied: 0, skipped: 0 };
  }

  const fields = { ...(spec.fields || {}) };
  if (opts.skipTargetGuestSegments) delete fields["Target Guest Segments"];

  const { patch, skipped } = patchFieldsForRecord(record, fields, { overwrite: opts.overwrite });
  const patchKeys = Object.keys(patch);
  const name = record.get("Brand Name") || brandName;

  console.log(`\n${name} (${record.id})`);
  if (skipped.length) console.log(`  keep existing (${skipped.length}): ${skipped.join(", ")}`);
  if (!patchKeys.length) {
    console.log("  nothing to patch — all fixture fields already set");
    return { applied: 0, skipped: skipped.length };
  }
  console.log(`  will patch (${patchKeys.length}): ${patchKeys.join(", ")}`);
  if (opts.dryRun) return { applied: patchKeys.length, skipped: skipped.length };

  await base(BASICS_TABLE).update(record.id, patch);
  console.log("  updated OK");
  return { applied: patchKeys.length, skipped: skipped.length };
}

async function main() {
  const opts = parseArgs(process.argv);
  if (!opts.fixturePath || !fs.existsSync(opts.fixturePath)) {
    throw new Error("Require --fixture path/to.json");
  }
  const spec = JSON.parse(fs.readFileSync(opts.fixturePath, "utf8"));
  const base = getBase();

  if (opts.allAlpha && !opts.brandRecordId && !opts.brandName) {
    throw new Error("--all-alpha-brand-studios requires fixture with targetBrandBasicsName per brand; use batch script.");
  }

  await applyOne(base, opts, spec);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
