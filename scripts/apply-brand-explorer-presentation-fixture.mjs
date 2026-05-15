/**
 * Create rows in Airtable "Brand Setup - Brand Explorer Presentation" from a fixture JSON.
 *
 * Does NOT create the table or columns — add those in your base first
 * (see docs/brand-explorer-presentation-slots.md). This only inserts rows.
 *
 * Usage:
 *   node scripts/apply-brand-explorer-presentation-fixture.mjs --dry-run --brand-name Radisson
 *   node scripts/apply-brand-explorer-presentation-fixture.mjs --brand-name Radisson
 *   node scripts/apply-brand-explorer-presentation-fixture.mjs --brand-record-id recXXXXXXXX
 *   node scripts/apply-brand-explorer-presentation-fixture.mjs --fixture fixtures/other.json --brand-name Radisson
 *   node scripts/apply-brand-explorer-presentation-fixture.mjs --brand-name Radisson --replace
 *   node scripts/apply-brand-explorer-presentation-fixture.mjs --brand-name Radisson --only-missing
 *   node scripts/apply-brand-explorer-presentation-fixture.mjs --brand-name Radisson --only-missing --slot-keys operations.standards_philosophy,operations.operator_compat.summary
 *   node scripts/apply-brand-explorer-presentation-fixture.mjs --brand-name Radisson --prune-except-slot-keys operations.standards_philosophy,operations.operator_compat.summary,operations.operator_compat.tags,operations.operator_compat.fit
 *
 * --only-missing   Create only fixture rows whose Slot Key is not already present for this brand.
 * --slot-keys      Comma-separated Slot Key filter (applied before --only-missing).
 * --prune-except-slot-keys  Delete existing rows for this brand whose Slot Key is NOT in the list (dry-run supported).
 *
 * Env: AIRTABLE_API_KEY, AIRTABLE_BASE_ID. Loads ../load-env.js when present.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import "../load-env.js";
import Airtable from "airtable";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

const TABLE = "Brand Setup - Brand Explorer Presentation";
const BASICS = "Brand Setup - Brand Basics";

const LINK_FIELD_CANDIDATES = ["Brand", "Brand_Basic_ID", "Brand Setup - Brand Basics", "Brand Basics"];

function parseArgs(argv) {
  const args = argv.slice(2);
  const flags = new Set();
  const kv = {};
  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    if (a === "--dry-run") flags.add("dry-run");
    else if (a === "--replace") flags.add("replace");
    else if (a === "--only-missing") flags.add("only-missing");
    else if (a.startsWith("--")) {
      const key = a.slice(2);
      const next = args[i + 1];
      if (next && !next.startsWith("--")) {
        kv[key] = next;
        i++;
      } else kv[key] = true;
    }
  }
  const fixtureRel = typeof kv.fixture === "string" ? kv.fixture : "fixtures/brand-explorer-presentation-radisson.example.json";
  const slotKeysRaw = String(kv["slot-keys"] || kv["prune-except-slot-keys"] || "").trim();
  const slotKeysFilter = slotKeysRaw
    ? slotKeysRaw
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean)
    : null;
  return {
    dryRun: flags.has("dry-run"),
    replace: flags.has("replace"),
    onlyMissing: flags.has("only-missing"),
    pruneExceptSlotKeys: kv["prune-except-slot-keys"] ? slotKeysFilter : null,
    slotKeysFilter: kv["slot-keys"] ? slotKeysFilter : null,
    brandName: String(kv["brand-name"] || "").trim(),
    brandRecordId: String(kv["brand-record-id"] || "").trim(),
    fixturePath: path.isAbsolute(fixtureRel) ? fixtureRel : path.resolve(ROOT, fixtureRel),
  };
}

function slotKeyFromRecord(rec) {
  return String(rec.get("Slot Key") || rec.fields?.["Slot Key"] || "").trim();
}

function existingSlotKeysSet(records) {
  const set = new Set();
  for (const rec of records) {
    const sk = slotKeyFromRecord(rec);
    if (sk) set.add(sk);
  }
  return set;
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
  const records = await base(BASICS)
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

async function selectPresentationForBrand(base, brandRecordId, brandName) {
  const escapedId = String(brandRecordId).replace(/"/g, '\\"');
  const escapedName = String(brandName || "").replace(/"/g, '\\"');
  // Linked-field formulas use linked *primary* values, not record ids, so
  // FIND(recId, ARRAYJOIN({Brand})) usually matches nothing — use Brand Name and/or {Brand} = name.
  if (escapedName) {
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
      const byName = await base(TABLE).select({ filterByFormula: `{Brand Name} = "${escapedName}"`, maxRecords: 500 }).all();
      pushAll(byName);
    } catch {
      /* optional Brand Name column missing */
    }
    try {
      const byLinkPrimary = await base(TABLE).select({ filterByFormula: `{Brand} = "${escapedName}"`, maxRecords: 500 }).all();
      pushAll(byLinkPrimary);
    } catch {
      /* schema differs */
    }
    if (merged.length > 0) return merged;
  }
  for (const linkField of LINK_FIELD_CANDIDATES) {
    try {
      const formula = `FIND("${escapedId}", ARRAYJOIN({${linkField}})) > 0`;
      const records = await base(TABLE).select({ filterByFormula: formula }).all();
      if (records.length > 0) return records;
    } catch {
      /* wrong link field name or table missing */
    }
  }
  return [];
}

async function deleteRecords(base, ids) {
  for (let i = 0; i < ids.length; i += 10) {
    const chunk = ids.slice(i, i + 10);
    await base(TABLE).destroy(chunk);
  }
}

function buildFieldsForRow(brandRecordId, linkField, r, brandNameForRow) {
  const fields = {
    [linkField]: [brandRecordId],
    "Slot Key": r.slotKey,
    Title: r.title ?? "",
    Body: r.body ?? "",
    "Sort Order": typeof r.sort === "number" ? r.sort : Number(r.sort) || 0,
    Active: true,
  };
  const su = String(r.summaryUrl ?? "").trim();
  if (su) fields["Summary URL"] = su;
  const csOverview = String(r.caseSummaryOverview ?? "").trim();
  const csOwner = String(r.caseSummaryOwnerObjective ?? "").trim();
  const csBrand = String(r.caseSummaryBrandRelevance ?? "").trim();
  const csInterp = String(r.caseSummaryInterpretation ?? "").trim();
  const csTags = String(r.caseSummaryTags ?? "").trim();
  if (csOverview) fields["Case Summary Overview"] = csOverview;
  if (csOwner) fields["Case Summary Owner Objective"] = csOwner;
  if (csBrand) fields["Case Summary Brand Relevance"] = csBrand;
  if (csInterp) fields["Case Summary Interpretation"] = csInterp;
  if (csTags) fields["Case Summary Tags"] = csTags;
  const n = String(brandNameForRow || "").trim();
  if (n) fields["Brand Name"] = n;
  return fields;
}

async function createAllRows(base, brandRecordId, rows, dryRun, brandNameForRow) {
  const namePasses = String(brandNameForRow || "").trim()
    ? [String(brandNameForRow).trim(), ""]
    : [""];

  let lastErr;
  for (const namePass of namePasses) {
    for (const linkField of LINK_FIELD_CANDIDATES) {
      const payload = rows.map((r) => ({
        fields: buildFieldsForRow(brandRecordId, linkField, r, namePass),
      }));
      try {
        if (dryRun) {
          const note = namePass ? ` (Brand Name="${namePass}")` : "";
          console.log(`Dry run: would create ${payload.length} row(s) using link field "${linkField}"${note}.`);
          console.log("First row:", JSON.stringify(payload[0], null, 2));
          return linkField;
        }
        const created = [];
        for (let i = 0; i < payload.length; i += 10) {
          const chunk = payload.slice(i, i + 10);
          const out = await base(TABLE).create(chunk);
          created.push(...out.map((rec) => rec.id));
        }
        const note = namePass ? `; set Brand Name` : "";
        console.log(
          `Created ${created.length} row(s) using link field "${linkField}"${note}. First ids: ${created.slice(0, 5).join(", ")}${created.length > 5 ? "…" : ""}`
        );
        return linkField;
      } catch (e) {
        lastErr = e;
      }
    }
  }
  throw lastErr || new Error("Could not create presentation rows (link field name or table schema mismatch).");
}

async function main() {
  const opts = parseArgs(process.argv);
  if (!opts.brandRecordId && !opts.brandName) {
    console.error(
      "Usage: node scripts/apply-brand-explorer-presentation-fixture.mjs [--dry-run] [--replace] [--only-missing] [--slot-keys k1,k2] [--prune-except-slot-keys k1,k2] --brand-name \"Radisson\" | --brand-record-id recXXX [--fixture path/to.json]"
    );
    process.exit(1);
  }

  if (!fs.existsSync(opts.fixturePath)) {
    throw new Error(`Fixture not found: ${opts.fixturePath}`);
  }
  const data = JSON.parse(fs.readFileSync(opts.fixturePath, "utf8"));
  let rows = data.rows;
  if (!Array.isArray(rows) || !rows.length) {
    throw new Error("fixture.rows must be a non-empty array");
  }

  if (opts.slotKeysFilter?.length) {
    const allow = new Set(opts.slotKeysFilter);
    rows = rows.filter((r) => allow.has(String(r.slotKey || "").trim()));
    if (!rows.length) {
      throw new Error(`No fixture rows match --slot-keys (${opts.slotKeysFilter.join(", ")})`);
    }
    console.log(`--slot-keys: ${rows.length} fixture row(s) after filter.`);
  }

  const base = getBase();

  let brandRecId = opts.brandRecordId;
  let brandNameForSelect = String(opts.brandName || "").trim();
  if (!brandRecId) {
    const rec = await findBasicsByName(base, opts.brandName);
    if (!rec) {
      console.error(`No Brand Basics row with Brand Name = "${opts.brandName}".`);
      process.exit(2);
    }
    brandRecId = rec.id;
    brandNameForSelect = opts.brandName;
    console.log(`Resolved --brand-name "${opts.brandName}" → ${brandRecId}`);
  } else if (!brandNameForSelect) {
    try {
      const basics = await base(BASICS).find(brandRecId);
      brandNameForSelect = String(basics.get("Brand Name") || basics.get("Name") || "").trim();
    } catch {
      /* --brand-record-id only; Brand Name optional for replace/select */
    }
  }

  const existing = await selectPresentationForBrand(base, brandRecId, brandNameForSelect);

  if (opts.pruneExceptSlotKeys?.length) {
    const keep = new Set(opts.pruneExceptSlotKeys);
    const toDrop = existing.filter((rec) => !keep.has(slotKeyFromRecord(rec)));
    if (toDrop.length === 0) {
      console.log("--prune-except-slot-keys: nothing to delete.");
    } else {
      const keys = [...new Set(toDrop.map(slotKeyFromRecord).filter(Boolean))];
      console.log(
        `--prune-except-slot-keys: would delete ${toDrop.length} row(s) (${keys.length} slot key(s) not in keep list).`
      );
      if (!opts.dryRun) {
        await deleteRecords(
          base,
          toDrop.map((r) => r.id)
        );
        console.log(`Deleted ${toDrop.length} row(s).`);
      }
    }
    if (!opts.onlyMissing && !opts.replace && opts.pruneExceptSlotKeys) {
      return;
    }
  }

  if (opts.replace) {
    const freshExisting = opts.pruneExceptSlotKeys && !opts.dryRun
      ? await selectPresentationForBrand(base, brandRecId, brandNameForSelect)
      : existing;
    if (freshExisting.length === 0) {
      console.log("--replace: no existing presentation rows found for this brand.");
    } else {
      console.log(`--replace: deleting ${freshExisting.length} existing row(s) in ${TABLE}…`);
      if (!opts.dryRun) {
        await deleteRecords(
          base,
          freshExisting.map((r) => r.id)
        );
      }
    }
  }

  let rowsToCreate = rows;
  if (opts.onlyMissing) {
    const afterPrune = opts.pruneExceptSlotKeys && !opts.dryRun
      ? await selectPresentationForBrand(base, brandRecId, brandNameForSelect)
      : opts.replace && !opts.dryRun
        ? []
        : existing;
    const have = existingSlotKeysSet(afterPrune);
    rowsToCreate = rows.filter((r) => !have.has(String(r.slotKey || "").trim()));
    const skipped = rows.length - rowsToCreate.length;
    if (skipped) {
      console.log(`--only-missing: skipping ${skipped} row(s) — slot key already present.`);
    }
    if (!rowsToCreate.length) {
      console.log("--only-missing: nothing to create.");
      return;
    }
    console.log(`--only-missing: will create ${rowsToCreate.length} row(s).`);
  }

  await createAllRows(base, brandRecId, rowsToCreate, opts.dryRun, brandNameForSelect);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
