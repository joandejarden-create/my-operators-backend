/**
 * Duplicate Brand Explorer setup: new Brand Basics row + clone all presentation rows.
 *
 * Usage:
 *   node scripts/duplicate-brand-explorer-brand.mjs --from "Radisson Blu" --to "Radisson Blu (Choice)" --dry-run
 *   node scripts/duplicate-brand-explorer-brand.mjs --from "Radisson Blu" --to "Radisson Blu (Choice)"
 */
import "../load-env.js";
import Airtable from "airtable";

const BASICS = "Brand Setup - Brand Basics";
const PRESENTATION = "Brand Setup - Brand Explorer Presentation";
const LINK_FIELD_CANDIDATES = ["Brand", "Brand_Basic_ID", "Brand Setup - Brand Basics", "Brand Basics"];

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
  if (!from || !to) throw new Error("Require --from and --to (Brand Name values).");
  return { dryRun, from, to };
}

function getBase() {
  return new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);
}

async function findBasicsByName(base, brandName) {
  const esc = brandName.replace(/"/g, '\\"');
  const records = await base(BASICS)
    .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 5 })
    .all();
  if (records.length === 1) return records[0];
  if (records.length > 1) throw new Error(`Multiple Basics for "${brandName}"`);
  return null;
}

async function selectPresentation(base, brandName) {
  const esc = brandName.replace(/"/g, '\\"');
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
    pushAll(await base(PRESENTATION).select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 500 }).all());
  } catch {
    /* */
  }
  try {
    pushAll(await base(PRESENTATION).select({ filterByFormula: `{Brand} = "${esc}"`, maxRecords: 500 }).all());
  } catch {
    /* */
  }
  return merged;
}

function detectLinkField(sampleRec) {
  for (const f of LINK_FIELD_CANDIDATES) {
    if (sampleRec?.get?.(f) != null) return f;
  }
  return "Brand";
}

/** Writable Brand Basics columns (skip formulas, lookups, rollups). */
const BASICS_WRITABLE = new Set([
  "Brand Name",
  "Parent Company",
  "Hotel Chain Scale",
  "Brand Architecture",
  "Brand Model Format",
  "Hotel Service Model",
  "Year Brand Launched",
  "Brand Development Stage",
  "Brand Positioning",
  "Brand Tagline",
  "Brand Tagline / Motto",
  "Brand Customer Promise",
  "Brand Value Proposition",
  "Brand Pillars",
  "Brand History",
  "Company History",
  "Target Guest Segments",
  "Guest Psychographics Description",
  "Key Brand Differentiators",
  "Sustainability Positioning",
  "Brand Website",
  "Brand Status",
  "Brand Profile Analysis",
  "Region Offered",
  "Explorer Hero Verification Label",
  "Explorer Hero Data Source Label",
]);

function copyBasicsFields(fromRec, newName) {
  const fields = { "Brand Name": newName };
  for (const key of BASICS_WRITABLE) {
    if (key === "Brand Name") continue;
    const val = fromRec.get(key);
    if (val == null || val === "") continue;
    fields[key] = val;
  }
  return fields;
}

function presentationFieldsFromRecord(rec, dstBasicsId, linkField, brandName) {
  const fields = {
    [linkField]: [dstBasicsId],
    "Brand Name": brandName,
    "Slot Key": String(rec.get("Slot Key") || "").trim(),
    Title: String(rec.get("Title") || ""),
    Body: String(rec.get("Body") || ""),
    "Sort Order": rec.get("Sort Order") ?? 0,
    Active: rec.get("Active") !== false,
  };
  for (const opt of [
    "Case Summary Overview",
    "Case Summary Owner Objective",
    "Case Summary Brand Relevance",
    "Case Summary Interpretation",
    "Case Summary Tags",
    "Summary URL",
  ]) {
    const v = rec.get(opt);
    if (v != null && String(v).trim()) fields[opt] = String(v).trim();
  }
  return fields;
}

async function main() {
  const { dryRun, from, to } = parseArgs();
  const base = getBase();

  const srcBasics = await findBasicsByName(base, from);
  if (!srcBasics) throw new Error(`Source brand not found: ${from}`);

  const existingDst = await findBasicsByName(base, to);
  if (existingDst) {
    throw new Error(`Target "${to}" already exists (${existingDst.id}). Use a different name or delete first.`);
  }

  const srcPresentation = await selectPresentation(base, from);
  const linkField = srcPresentation.length ? detectLinkField(srcPresentation[0]) : "Brand";

  console.log(`Source: ${from} (${srcBasics.id}) — ${srcPresentation.length} presentation row(s)`);
  console.log(`${dryRun ? "[dry-run] " : ""}Create Basics: "${to}" and clone presentation.`);

  if (dryRun) return;

  const [createdBasics] = await base(BASICS).create([{ fields: copyBasicsFields(srcBasics, to) }]);
  console.log(`Created Basics: ${createdBasics.id} (${to})`);

  let n = 0;
  for (let i = 0; i < srcPresentation.length; i += 10) {
    const chunk = srcPresentation.slice(i, i + 10);
    const payload = chunk
      .map((rec) => ({
        fields: presentationFieldsFromRecord(rec, createdBasics.id, linkField, to),
      }))
      .filter((row) => row.fields["Slot Key"]);
    if (!payload.length) continue;
    await base(PRESENTATION).create(payload);
    n += chunk.length;
    console.log(`Cloned presentation ${n}/${srcPresentation.length}`);
  }
  console.log("Done.");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
