/**
 * Apply all fixtures/brand-basics-from-choice-materials/*.json with patch-missing-only.
 * Only updates rows where Parent Company includes "Choice Hotels International".
 *
 *   node scripts/apply-choice-brand-basics-batch.mjs --dry-run
 *   node scripts/apply-choice-brand-basics-batch.mjs
 *   node scripts/apply-choice-brand-basics-batch.mjs --skip-target-guest-segments
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import "../load-env.js";
import Airtable from "airtable";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const FIXTURE_DIR = path.join(ROOT, "fixtures", "brand-basics-from-choice-materials");
const BASICS_TABLE = "Brand Setup - Brand Basics";

function parseArgs(argv) {
  const flags = new Set(argv.slice(2).filter((a) => a.startsWith("--")));
  return {
    dryRun: flags.has("--dry-run"),
    skipTargetGuestSegments: flags.has("--skip-target-guest-segments"),
  };
}

function isEmpty(v) {
  if (v == null || v === "") return true;
  if (Array.isArray(v) && !v.length) return true;
  return false;
}

function isChoiceHotelsInternational(rec) {
  return String(rec.get("Parent Company") || "").includes("Choice Hotels International");
}

async function findBasicsByName(base, brandName) {
  const esc = brandName.replace(/"/g, '\\"');
  const records = await base(BASICS_TABLE)
    .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 20 })
    .all();
  if (records.length === 1) return records[0];
  if (records.length > 1) {
    throw new Error(`Multiple rows for "${brandName}": ${records.map((r) => r.id).join(", ")}`);
  }
  return null;
}

function patchFieldsForRecord(record, fields) {
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

async function main() {
  const opts = parseArgs(process.argv);
  const key = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!key || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");
  const base = new Airtable({ apiKey: key }).base(baseId);

  const files = fs
    .readdirSync(FIXTURE_DIR)
    .filter((f) => f.endsWith(".json"))
    .sort();

  let totalPatched = 0;
  let brandsUpdated = 0;

  for (const file of files) {
    const spec = JSON.parse(fs.readFileSync(path.join(FIXTURE_DIR, file), "utf8"));
    const brandName = String(spec.targetBrandBasicsName || "").trim();
    if (!brandName) {
      console.warn(`Skip ${file}: no targetBrandBasicsName`);
      continue;
    }

    const record = await findBasicsByName(base, brandName);
    if (!record) {
      console.warn(`Skip ${brandName}: no Airtable row`);
      continue;
    }
    if (!isChoiceHotelsInternational(record)) {
      console.warn(`Skip ${brandName}: Parent Company is not Choice Hotels International`);
      continue;
    }

    const fields = { ...(spec.fields || {}) };
    if (opts.skipTargetGuestSegments) {
      delete fields["Target Guest Segments"];
    } else if (Array.isArray(spec.targetGuestSegmentOptions) && spec.targetGuestSegmentOptions.length) {
      delete fields["Target Guest Segments"];
      if (isEmpty(record.get("Target Guest Segments"))) {
        fields["Target Guest Segments"] = spec.targetGuestSegmentOptions;
      }
    }

    const { patch, skipped } = patchFieldsForRecord(record, fields);
    const patchKeys = Object.keys(patch);

    console.log(`\n${brandName} (${record.id}) ← ${file}`);
    if (skipped.length) console.log(`  keep existing: ${skipped.join(", ")}`);
    if (!patchKeys.length) {
      console.log("  nothing to patch");
      continue;
    }
    console.log(`  patch ${patchKeys.length}: ${patchKeys.join(", ")}`);

    if (!opts.dryRun) {
      try {
        await base(BASICS_TABLE).update(record.id, patch);
        console.log("  updated OK");
        brandsUpdated++;
        totalPatched += patchKeys.length;
      } catch (e) {
        console.error(`  FAILED: ${e.message || e}`);
        if (String(e.message || e).includes("Target Guest Segments")) {
          console.error("  Hint: re-run with --skip-target-guest-segments");
        }
      }
    } else {
      brandsUpdated++;
      totalPatched += patchKeys.length;
    }
  }

  console.log(
    `\n${opts.dryRun ? "Dry run" : "Done"}: ${brandsUpdated} brand(s), ${totalPatched} field(s) ${opts.dryRun ? "would be" : ""} patched.`
  );
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
