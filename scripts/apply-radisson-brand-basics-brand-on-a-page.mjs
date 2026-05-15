/**
 * PATCH only the Brand Basics fields in fixtures/brand-basics-radisson-brand-on-a-page.json
 * onto the Radisson row (or a given --brand-record-id) via Airtable API.
 * Does not touch Brand Explorer Presentation or any other table.
 *
 * Usage:
 *   node scripts/apply-radisson-brand-basics-brand-on-a-page.mjs --dry-run
 *   node scripts/apply-radisson-brand-basics-brand-on-a-page.mjs
 *   node scripts/apply-radisson-brand-basics-brand-on-a-page.mjs --brand-record-id recXXXXXXXX
 *   node scripts/apply-radisson-brand-basics-brand-on-a-page.mjs --fixture path/to/other.json
 *
 * If "Target Guest Segments" fails (multi-select vs long text in your base), re-run with:
 *   node scripts/apply-radisson-brand-basics-brand-on-a-page.mjs --skip-target-guest-segments
 *
 * Env: AIRTABLE_API_KEY, AIRTABLE_BASE_ID
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import "../load-env.js";
import Airtable from "airtable";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

const BASICS_TABLE = "Brand Setup - Brand Basics";
const DEFAULT_FIXTURE = path.join(ROOT, "fixtures", "brand-basics-radisson-brand-on-a-page.json");

function parseArgs(argv) {
  const args = argv.slice(2);
  const flags = new Set();
  const kv = {};
  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    if (a === "--dry-run") flags.add("dry-run");
    else if (a === "--skip-target-guest-segments") flags.add("skip-target-guest-segments");
    else if (a.startsWith("--")) {
      const key = a.slice(2);
      const next = args[i + 1];
      if (next && !next.startsWith("--")) {
        kv[key] = next;
        i++;
      } else kv[key] = true;
    }
  }
  const fixtureRel = typeof kv.fixture === "string" ? kv.fixture : "";
  const fixturePath = fixtureRel
    ? path.isAbsolute(fixtureRel)
      ? fixtureRel
      : path.resolve(ROOT, fixtureRel)
    : DEFAULT_FIXTURE;
  return {
    dryRun: flags.has("dry-run"),
    skipTargetGuestSegments: flags.has("skip-target-guest-segments"),
    brandName: String(kv["brand-name"] || "").trim(),
    brandRecordId: String(kv["brand-record-id"] || "").trim(),
    fixturePath,
  };
}

function getBase() {
  const key = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!key || !baseId) {
    throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID (e.g. in .env at repo root).");
  }
  return new Airtable({ apiKey: key }).base(baseId);
}

async function findBasicsByName(base, brandName) {
  const esc = brandName.replace(/"/g, '\\"');
  const records = await base(BASICS_TABLE)
    .select({
      filterByFormula: `{Brand Name} = "${esc}"`,
      maxRecords: 20,
    })
    .all();
  if (records.length === 1) return records[0];
  if (records.length > 1) {
    const ids = records.map((r) => r.id).join(", ");
    throw new Error(
      `Multiple Brand Basics rows match "${brandName}": ${records.length}. Use --brand-record-id. IDs: ${ids}`
    );
  }
  return null;
}

async function main() {
  const opts = parseArgs(process.argv);
  if (!fs.existsSync(opts.fixturePath)) {
    throw new Error(`Fixture not found: ${opts.fixturePath}`);
  }
  const spec = JSON.parse(fs.readFileSync(opts.fixturePath, "utf8"));
  const defaultName = String(spec.targetBrandBasicsName || "Radisson").trim();
  const brandName = opts.brandName || defaultName;

  const base = getBase();
  let record;
  if (opts.brandRecordId) {
    record = await base(BASICS_TABLE).find(opts.brandRecordId);
  } else {
    record = await findBasicsByName(base, brandName);
    if (!record) {
      throw new Error(`No Brand Basics row found with Brand Name = "${brandName}". Use --brand-record-id rec…`);
    }
  }

  const fields = { ...(spec.fields || {}) };
  if (opts.skipTargetGuestSegments) {
    delete fields["Target Guest Segments"];
    console.log("Skipping field: Target Guest Segments");
  }

  const keys = Object.keys(fields);
  if (keys.length === 0) {
    throw new Error("No fields in fixture `fields` object.");
  }

  console.log(`Brand Basics record: ${record.id} (${record.fields["Brand Name"] || brandName})`);
  console.log(`Will update ${keys.length} field(s): ${keys.join(", ")}`);

  if (opts.dryRun) {
    console.log("Dry run — no Airtable write.");
    return;
  }

  const updated = await base(BASICS_TABLE).update(record.id, fields);
  console.log("Updated OK:", updated.id, updated.fields["Brand Name"]);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
