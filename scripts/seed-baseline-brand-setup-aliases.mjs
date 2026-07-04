/**
 * Seed one baseline identity alias per Brand Setup brand (Platform Brand Alias Mapping only).
 *
 * Baseline row per brand:
 *   Canonical Brand Name = display name
 *   Alias / Source Brand Name = display name
 *   Parent Company = MVP parent company
 *   Active = true, Match Confidence = Medium
 *   Notes = "Baseline alias from Brand Setup display name"
 *
 * Upsert key: Canonical + Alias + Parent (trim-exact). Creates only; never deletes or deactivates.
 * Skips when exact key already exists. Does not update existing rows (including other aliases).
 *
 * Usage:
 *   node scripts/seed-baseline-brand-setup-aliases.mjs
 *   node scripts/seed-baseline-brand-setup-aliases.mjs --dry-run
 *
 * Does not modify Hotel Census, Radar, or Brand Footprint.
 */
import "../load-env.js";
import Airtable from "airtable";
import { ALIAS_FIELDS } from "../lib/hotel-census/fields.js";
import { exactMatchKey } from "../lib/hotel-census/brand-alias-resolve.js";
import { upsertKey, DEFAULT_ALIAS_TABLE } from "./lib/brand-alias-seed.mjs";

const BASICS_TABLE = "Brand Setup - Brand Basics";
const BRAND_NAME_FIELD = "Brand Name";
const PARENT_FIELD = "Parent Company";

const BASELINE_NOTE = "Baseline alias from Brand Setup display name";

async function chunkCreate(table, records) {
  const CHUNK = 10;
  for (let i = 0; i < records.length; i += CHUNK) {
    await table.create(records.slice(i, i + CHUNK));
  }
}

async function main() {
  const dryRun = process.argv.includes("--dry-run");

  const apiKey = process.env.AIRTABLE_API_KEY;
  const mvpBaseId = process.env.AIRTABLE_BASE_ID;
  const platformBaseId = process.env.AIRTABLE_BASE_ID_ALT;
  if (!apiKey || !mvpBaseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");
  if (!platformBaseId) throw new Error("Set AIRTABLE_BASE_ID_ALT");

  const mvpBase = new Airtable({ apiKey }).base(mvpBaseId);
  const platformBase = new Airtable({ apiKey }).base(platformBaseId);
  const aliasTable = platformBase(DEFAULT_ALIAS_TABLE);

  console.log("Loading Brand Setup - Brand Basics...");
  const brandRecs = await mvpBase(BASICS_TABLE)
    .select({ fields: [BRAND_NAME_FIELD, PARENT_FIELD], pageSize: 100 })
    .all();

  const brands = brandRecs
    .map((rec) => ({
      id: rec.id,
      brandName: exactMatchKey(rec.fields?.[BRAND_NAME_FIELD]),
      parentCompany: exactMatchKey(rec.fields?.[PARENT_FIELD]),
    }))
    .filter((b) => b.brandName);

  console.log(`Loading ${DEFAULT_ALIAS_TABLE} (existing rows)...`);
  let existing = [];
  try {
    existing = await aliasTable
      .select({ fields: Object.values(ALIAS_FIELDS), pageSize: 100 })
      .all();
  } catch (err) {
    throw new Error(`Cannot read alias table: ${err?.message || err}`);
  }

  const byKey = new Map();
  for (const rec of existing) {
    const f = rec.fields || {};
    const key = upsertKey(
      f[ALIAS_FIELDS.canonicalBrandName],
      f[ALIAS_FIELDS.aliasSourceBrandName],
      f[ALIAS_FIELDS.parentCompany]
    );
    byKey.set(key, rec);
  }

  const stats = {
    dryRun,
    totalBrandSetupBrandsReviewed: brands.length,
    baselineAliasesCreated: 0,
    baselineAliasesAlreadyExisted: 0,
    parentCompanyMismatches: [],
    skippedRecords: [],
    errors: [],
  };

  const toCreate = [];

  for (const brand of brands) {
    const { brandName, parentCompany, id: basicsId } = brand;
    const key = upsertKey(brandName, brandName, parentCompany);

    if (byKey.has(key)) {
      stats.baselineAliasesAlreadyExisted += 1;
      continue;
    }

    const mismatches = [];
    for (const rec of existing) {
      const f = rec.fields || {};
      const canonical = exactMatchKey(f[ALIAS_FIELDS.canonicalBrandName]);
      const alias = exactMatchKey(f[ALIAS_FIELDS.aliasSourceBrandName]);
      const parent = exactMatchKey(f[ALIAS_FIELDS.parentCompany]);
      if (canonical === brandName && alias === brandName && parent && parent !== parentCompany) {
        mismatches.push({
          aliasRecordId: rec.id,
          brandName,
          mvpParentCompany: parentCompany || "(empty)",
          existingParentCompany: parent,
          brandSetupRecordId: basicsId,
        });
      }
    }

    if (mismatches.length) {
      stats.parentCompanyMismatches.push(...mismatches);
    }

    const fields = {
      [ALIAS_FIELDS.canonicalBrandName]: brandName,
      [ALIAS_FIELDS.aliasSourceBrandName]: brandName,
      [ALIAS_FIELDS.parentCompany]: parentCompany,
      [ALIAS_FIELDS.active]: true,
      [ALIAS_FIELDS.matchConfidence]: "Medium",
      [ALIAS_FIELDS.notes]: BASELINE_NOTE,
    };

    toCreate.push({ fields });
    stats.baselineAliasesCreated += 1;
  }

  if (dryRun) {
    console.log(
      JSON.stringify(
        {
          ...stats,
          wouldCreate: stats.baselineAliasesCreated,
          message: "Dry run — no writes to Brand Alias Mapping",
        },
        null,
        2
      )
    );
    return;
  }

  try {
    if (toCreate.length) {
      console.log(`Creating ${toCreate.length} baseline alias rows...`);
      await chunkCreate(aliasTable, toCreate);
    }
  } catch (err) {
    stats.errors.push(err?.message || String(err));
    throw err;
  }

  console.log(JSON.stringify(stats, null, 2));

  if (stats.parentCompanyMismatches.length) {
    console.log("\nParent company mismatches (same brand+alias, different parent on existing row):");
    stats.parentCompanyMismatches.slice(0, 20).forEach((m) => {
      console.log(
        `  ${m.brandName}: MVP "${m.mvpParentCompany}" vs existing "${m.existingParentCompany}" (${m.aliasRecordId})`
      );
    });
    if (stats.parentCompanyMismatches.length > 20) {
      console.log(`  … and ${stats.parentCompanyMismatches.length - 20} more`);
    }
  }

  console.log("\nConfirmations:");
  console.log("  Hotel Census: not modified");
  console.log("  Radar: not modified");
  console.log("  Brand Footprint: not modified");
  console.log("  Existing non-baseline alias rows: not modified");
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
