/**
 * Clone Brand Explorer Presentation rows from one brand to another (same slot keys).
 *
 * Usage:
 *   node scripts/clone-brand-explorer-presentation.mjs --from "Radisson (Choice)" --to "Radisson Blu" --dry-run
 *   node scripts/clone-brand-explorer-presentation.mjs --from "Radisson (Choice)" --to "Radisson Blu" --only-missing
 */
import "../load-env.js";
import Airtable from "airtable";

const TABLE = "Brand Setup - Brand Explorer Presentation";
const BASICS = "Brand Setup - Brand Basics";
const LINK_FIELD_CANDIDATES = ["Brand", "Brand_Basic_ID", "Brand Setup - Brand Basics", "Brand Basics"];

function parseArgs() {
  const argv = process.argv.slice(2);
  const kv = {};
  let dryRun = false;
  let onlyMissing = false;
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--dry-run") dryRun = true;
    else if (a === "--only-missing") onlyMissing = true;
    else if (a.startsWith("--") && argv[i + 1] && !argv[i + 1].startsWith("--")) {
      kv[a.slice(2)] = argv[++i];
    }
  }
  const from = String(kv.from || "").trim();
  const to = String(kv.to || "").trim();
  if (!from || !to) throw new Error("Require --from and --to brand names (Brand Basics Brand Name).");
  return { dryRun, onlyMissing, from, to };
}

function getBase() {
  const key = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!key || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");
  return new Airtable({ apiKey: key }).base(baseId);
}

async function findBasicsByName(base, brandName) {
  const esc = brandName.replace(/"/g, '\\"');
  const records = await base(BASICS)
    .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 20 })
    .all();
  if (records.length === 1) return records[0];
  if (records.length > 1) {
    throw new Error(`Multiple Basics for "${brandName}": ${records.map((r) => r.id).join(", ")}`);
  }
  return null;
}

async function selectPresentationForBrand(base, brandName) {
  const escapedName = String(brandName || "").replace(/"/g, '\\"');
  const merged = [];
  const seen = new Set();
  const pushAll = (records) => {
    for (const r of records) {
      if (!seen.has(r.id)) {
        seen.add(r.id);
        merged.push(r);
      }
    }
  };
  try {
    pushAll(
      await base(TABLE).select({ filterByFormula: `{Brand Name} = "${escapedName}"`, maxRecords: 500 }).all()
    );
  } catch {
    /* optional Brand Name column */
  }
  try {
    pushAll(await base(TABLE).select({ filterByFormula: `{Brand} = "${escapedName}"`, maxRecords: 500 }).all());
  } catch {
    /* optional */
  }
  return merged;
}

function detectLinkField(sampleRec) {
  for (const f of LINK_FIELD_CANDIDATES) {
    if (sampleRec.get(f) != null) return f;
  }
  return "Brand";
}

function slotKeyFromRecord(rec) {
  return String(rec.get("Slot Key") || "").trim();
}

function fieldStr(rec, name) {
  const v = rec.get(name);
  if (v == null) return "";
  return String(v).trim();
}

async function main() {
  const { dryRun, onlyMissing, from, to } = parseArgs();
  const base = getBase();
  const srcBasics = await findBasicsByName(base, from);
  const dstBasics = await findBasicsByName(base, to);
  if (!srcBasics) throw new Error(`No Brand Basics for source: ${from}`);
  if (!dstBasics) throw new Error(`No Brand Basics for target: ${to}`);

  const srcRows = await selectPresentationForBrand(base, from);
  const dstRows = await selectPresentationForBrand(base, to);
  const dstSlots = new Set(dstRows.map(slotKeyFromRecord).filter(Boolean));
  const linkField = srcRows.length ? detectLinkField(srcRows[0]) : "Brand";

  const toCreate = [];
  for (const rec of srcRows) {
    const sk = slotKeyFromRecord(rec);
    if (!sk) continue;
    if (onlyMissing && dstSlots.has(sk)) continue;
    toCreate.push(rec);
  }

  console.log(`Source "${from}": ${srcRows.length} rows`);
  console.log(`Target "${to}": ${dstRows.length} rows (${dstSlots.size} slot keys)`);
  console.log(`${dryRun ? "[dry-run] " : ""}Would create ${toCreate.length} rows`);

  if (dryRun || !toCreate.length) return;

  let created = 0;
  for (let i = 0; i < toCreate.length; i += 10) {
    const chunk = toCreate.slice(i, i + 10);
    const payload = chunk.map((rec) => {
      const fields = {
        [linkField]: [dstBasics.id],
        "Slot Key": slotKeyFromRecord(rec),
        Title: fieldStr(rec, "Title"),
        Body: fieldStr(rec, "Body"),
        "Sort Order": rec.get("Sort Order") ?? 0,
        Active: rec.get("Active") !== false,
      };
      const brandNameCol = fieldStr(rec, "Brand Name");
      if (brandNameCol || to) fields["Brand Name"] = to;
      for (const opt of [
        "Case Summary Overview",
        "Case Summary Owner Objective",
        "Case Summary Brand Relevance",
        "Case Summary Interpretation",
        "Case Summary Tags",
        "Summary URL",
      ]) {
        const v = fieldStr(rec, opt);
        if (v) fields[opt] = v;
      }
      return { fields };
    });
    await base(TABLE).create(payload);
    created += chunk.length;
    console.log(`Created ${created}/${toCreate.length}`);
  }
  console.log("Done.");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
