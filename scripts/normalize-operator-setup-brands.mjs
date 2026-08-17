#!/usr/bin/env node
/**
 * Normalize Operator Setup Profile brand relationships for all Masters:
 * - Brand Families Operated (multi-select)
 * - brands (linked Brand Basics)
 * - numberOfBrands (derived)
 *
 * Ensures Brand Basics "Independent" exists (Draft + Internal Only) when needed.
 *
 *   node scripts/normalize-operator-setup-brands.mjs --dry-run
 *   node scripts/normalize-operator-setup-brands.mjs --apply --approve-normalize-operator-setup-brands
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import {
  resolveOperatorBrands,
  OPERATOR_SETUP_BRANDS_BY_SLUG,
  OPERATOR_SETUP_INDEPENDENT_BRAND_NAME,
  OPERATOR_SETUP_BRAND_FAMILIES_ALLOWED,
} from "../lib/partner-intelligence/operator-setup-brands-registry.js";
import { getOperatorFactoryQueueEntry } from "../lib/partner-intelligence/operator-explorer-factory-queue.js";
import { getOperatorQualityBaselineEntry } from "../lib/partner-intelligence/operator-explorer-quality-baseline.js";
import { upsertOperatorOneToOneTable } from "../api/lib/operator-setup-new-base-writer.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const MASTER_TABLE = process.env.AIRTABLE_OPERATOR_SETUP_MASTER_TABLE || "Operator Setup - Master";
const PROFILE_TABLE = "Operator Setup - Profile & Positioning";
const BRAND_TABLE = process.env.AIRTABLE_BRAND_BASICS_TABLE || "Brand Setup - Brand Basics";

function parseArgs(argv) {
  const out = { apply: false, approve: false };
  for (const a of argv) {
    if (a === "--apply") out.apply = true;
    else if (a === "--dry-run") out.apply = false;
    else if (a === "--approve-normalize-operator-setup-brands") out.approve = true;
  }
  return out;
}

function getBase() {
  const apiKey = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) throw new Error("Missing AIRTABLE_API_KEY/PAT or AIRTABLE_BASE_ID");
  return new Airtable({ apiKey }).base(baseId);
}

function normalizeKey(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function sameStringSet(a, b) {
  const aa = [...new Set((a || []).map(String))].sort();
  const bb = [...new Set((b || []).map(String))].sort();
  if (aa.length !== bb.length) return false;
  return aa.every((v, i) => v === bb[i]);
}

async function listAllMasters(base) {
  const rows = await base(MASTER_TABLE).select({ fields: ["company_name"], pageSize: 100 }).all();
  return rows.map((r) => ({
    recordId: r.id,
    companyName: String(r.fields?.company_name || "").trim(),
  }));
}

/**
 * @returns {Promise<Map<string, { profileId: string, brandFamilies: string[], brandIds: string[], numberOfBrands: number|null }>>}
 */
async function indexProfilesByMaster(base) {
  const map = new Map();
  const rows = await base(PROFILE_TABLE)
    .select({
      fields: ["Operator", "brands", "Brand Families Operated", "numberOfBrands"],
      pageSize: 100,
    })
    .all();
  for (const r of rows) {
    const ops = r.fields?.Operator;
    if (!Array.isArray(ops)) continue;
    const brandFamilies = Array.isArray(r.fields?.["Brand Families Operated"])
      ? r.fields["Brand Families Operated"].map(String)
      : [];
    const brandIds = Array.isArray(r.fields?.brands) ? r.fields.brands.map(String) : [];
    const nobRaw = r.fields?.numberOfBrands;
    const numberOfBrands =
      nobRaw != null && nobRaw !== "" && Number.isFinite(Number(nobRaw)) ? Number(nobRaw) : null;
    for (const masterId of ops) {
      if (!masterId || map.has(masterId)) continue;
      map.set(masterId, {
        profileId: r.id,
        brandFamilies,
        brandIds,
        numberOfBrands,
      });
    }
  }
  return map;
}

/**
 * @returns {Promise<{ byId: Map<string,string>, byName: Map<string,string[]>, byParent: Map<string,string[]>, records: object[] }>}
 */
async function loadBrandBasics(base) {
  const rows = await base(BRAND_TABLE)
    .select({ fields: ["Brand Name", "Parent Company", "Brand Status", "External Display Status"], pageSize: 100 })
    .all();
  const byId = new Map();
  const byName = new Map();
  /** @type {Map<string, string[]>} */
  const byParent = new Map();
  for (const r of rows) {
    const name = String(r.fields?.["Brand Name"] || "").trim();
    if (!name) continue;
    byId.set(r.id, name);
    const key = normalizeKey(name);
    if (!byName.has(key)) byName.set(key, []);
    byName.get(key).push(r.id);
    const parent = String(r.fields?.["Parent Company"] || "").trim();
    if (parent) {
      const pk = normalizeKey(parent);
      if (!byParent.has(pk)) byParent.set(pk, []);
      byParent.get(pk).push(r.id);
    }
  }
  return { byId, byName, byParent, records: rows };
}

/**
 * Ensure Independent Brand Basics exists (Draft + Internal Only).
 * @returns {Promise<{ id: string, created: boolean }>}
 */
async function ensureIndependentBrand(base, brandIndex, { apply }) {
  const key = normalizeKey(OPERATOR_SETUP_INDEPENDENT_BRAND_NAME);
  const existing = brandIndex.byName.get(key);
  if (existing?.length) {
    return { id: existing[0], created: false };
  }
  if (!apply) {
    return { id: "recWOULD_CREATE_INDEPENDENT", created: true };
  }
  const created = await base(BRAND_TABLE).create(
    {
      "Brand Name": OPERATOR_SETUP_INDEPENDENT_BRAND_NAME,
      "Brand Status": "Draft",
      "External Display Status": "Internal Only",
      "Parent Company": "Dealality Operator Setup placeholder",
      "Evidence Notes":
        "Placeholder Brand Basics row for Operator Setup brands links when an operator manages proprietary / unflagged / independent assets. Not a Brand Explorer public brand.",
    },
    { typecast: true }
  );
  brandIndex.byId.set(created.id, OPERATOR_SETUP_INDEPENDENT_BRAND_NAME);
  brandIndex.byName.set(key, [created.id]);
  return { id: created.id, created: true };
}

function expandParentBrandIds(brandIndex, parentSubstrings) {
  const ids = [];
  const parents = parentSubstrings || [];
  for (const [parentKey, recIds] of brandIndex.byParent.entries()) {
    for (const needle of parents) {
      const n = normalizeKey(needle);
      if (!n) continue;
      if (parentKey.includes(n) || n.includes(parentKey)) {
        ids.push(...recIds);
      }
    }
  }
  return [...new Set(ids)];
}

function resolveBrandNamesToIds(brandIndex, names, { allowWouldCreateIndependent }) {
  const linkedIds = [];
  const unresolved = [];
  const duplicates = [];
  for (const label of names || []) {
    const key = normalizeKey(label);
    if (!key) continue;
    if (key === normalizeKey(OPERATOR_SETUP_INDEPENDENT_BRAND_NAME) && allowWouldCreateIndependent) {
      // placeholder id already injected into byName when dry-run creates virtual Independent
      const matches = brandIndex.byName.get(key) || [];
      if (matches.length) {
        linkedIds.push(matches[0]);
        continue;
      }
    }
    const matches = brandIndex.byName.get(key) || [];
    if (!matches.length) {
      unresolved.push(label);
      continue;
    }
    if (matches.length > 1) duplicates.push(label);
    linkedIds.push(matches[0]);
  }
  return {
    linkedIds: [...new Set(linkedIds)],
    unresolved,
    duplicates,
  };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.apply && !args.approve) {
    console.error("Apply requires --approve-normalize-operator-setup-brands");
    process.exit(1);
  }

  console.log(
    `[normalize-brands] dryRun=${!args.apply} fields=Brand Families Operated,brands,numberOfBrands`
  );

  const base = getBase();
  const masters = await listAllMasters(base);
  const profilesByMaster = await indexProfilesByMaster(base);
  const brandIndex = await loadBrandBasics(base);

  const needsIndependent = Object.values(OPERATOR_SETUP_BRANDS_BY_SLUG).some((s) =>
    (s.brands || []).includes(OPERATOR_SETUP_INDEPENDENT_BRAND_NAME)
  );

  let independentMeta = { id: null, created: false };
  if (needsIndependent) {
    independentMeta = await ensureIndependentBrand(base, brandIndex, { apply: args.apply });
    if (!args.apply && independentMeta.created) {
      brandIndex.byName.set(normalizeKey(OPERATOR_SETUP_INDEPENDENT_BRAND_NAME), [
        independentMeta.id,
      ]);
      brandIndex.byId.set(independentMeta.id, OPERATOR_SETUP_INDEPENDENT_BRAND_NAME);
    }
    console.log(
      `[normalize-brands] Independent Brand Basics: ${independentMeta.created ? (args.apply ? "created" : "would_create") : "exists"} id=${independentMeta.id}`
    );
  }

  const results = [];
  let alreadyCorrect = 0;
  let wouldUpdate = 0;
  let updated = 0;
  let needsManual = 0;
  let noProfile = 0;
  let okUnmapped = 0;

  for (const m of masters) {
    const baseline = getOperatorQualityBaselineEntry(m.recordId);
    const queued = getOperatorFactoryQueueEntry(m.recordId);
    const slug = baseline?.slug || queued?.slug || null;
    const spec = resolveOperatorBrands({ slug, companyName: m.companyName });
    const profile = profilesByMaster.get(m.recordId) || null;

    const row = {
      recordId: m.recordId,
      companyName: m.companyName,
      slug,
      profileId: profile?.profileId || null,
      sourceNote: spec?.sourceNote || null,
      action: "noop",
      currentFamilies: profile?.brandFamilies || [],
      targetFamilies: spec?.brandFamiliesOperated || [],
      currentBrandIds: profile?.brandIds || [],
      currentBrandNames: (profile?.brandIds || []).map((id) => brandIndex.byId.get(id) || id),
      targetBrandNames: [],
      targetBrandIds: [],
      unresolvedBrands: [],
      numberOfBrands: null,
    };

    if (!spec) {
      row.action = "needs_manual_brands";
      needsManual += 1;
      console.log(`needs_manual             ${m.companyName}`);
      results.push(row);
      continue;
    }

    if (!profile) {
      row.action = "skip_no_profile";
      noProfile += 1;
      console.log(`skip_no_profile          ${m.companyName}`);
      results.push(row);
      continue;
    }

    const invalidFamilies = (spec.brandFamiliesOperated || []).filter(
      (f) => !OPERATOR_SETUP_BRAND_FAMILIES_ALLOWED.includes(f)
    );
    if (invalidFamilies.length) {
      row.action = "needs_manual_brands";
      row.unresolvedBrands = invalidFamilies;
      needsManual += 1;
      console.log(`invalid_families         ${m.companyName}: ${invalidFamilies.join(", ")}`);
      results.push(row);
      continue;
    }

    const fromNames = resolveBrandNamesToIds(brandIndex, spec.brands, {
      allowWouldCreateIndependent: true,
    });
    const fromParents = expandParentBrandIds(brandIndex, spec.brandsExpandParents);
    const targetBrandIds = [...new Set([...fromNames.linkedIds, ...fromParents])];
    const targetBrandNames = targetBrandIds.map((id) => brandIndex.byId.get(id) || id);
    row.targetBrandIds = targetBrandIds;
    row.targetBrandNames = targetBrandNames;
    row.unresolvedBrands = fromNames.unresolved;
    row.numberOfBrands = targetBrandIds.length;

    if (fromNames.unresolved.length) {
      row.action = "needs_manual_brands";
      needsManual += 1;
      console.log(
        `unresolved               ${m.companyName}: ${fromNames.unresolved.join(", ")}`
      );
      results.push(row);
      continue;
    }

    const familiesMatch = sameStringSet(profile.brandFamilies, spec.brandFamiliesOperated);
    const brandsMatch = sameStringSet(profile.brandIds, targetBrandIds);
    const nobMatch = profile.numberOfBrands === targetBrandIds.length;

    if (familiesMatch && brandsMatch && nobMatch) {
      row.action = "already_correct";
      alreadyCorrect += 1;
      console.log(
        `already_correct          ${m.companyName}: families=${spec.brandFamiliesOperated.length} brands=${targetBrandIds.length}`
      );
      results.push(row);
      continue;
    }

    const payload = {
      "Brand Families Operated": spec.brandFamiliesOperated,
      brands: targetBrandIds,
      numberOfBrands: targetBrandIds.length,
    };

    if (!args.apply) {
      row.action = "would_update";
      wouldUpdate += 1;
      console.log(
        `would_update             ${m.companyName}: families ${JSON.stringify(profile.brandFamilies)} → ${JSON.stringify(spec.brandFamiliesOperated)}; brands ${profile.brandIds.length} → ${targetBrandIds.length}`
      );
      results.push(row);
      continue;
    }

    await upsertOperatorOneToOneTable(
      PROFILE_TABLE,
      m.recordId,
      payload,
      `brands-normalize-${slug || m.recordId}`
    );
    row.action = "updated";
    updated += 1;
    console.log(
      `updated                  ${m.companyName}: families → ${JSON.stringify(spec.brandFamiliesOperated)}; brands → ${targetBrandIds.length}`
    );
    results.push(row);
  }

  // Count unmapped with no change needed separately — none expected
  void okUnmapped;

  const summary = {
    masters: masters.length,
    independentBrand: independentMeta,
    alreadyCorrect,
    wouldUpdate,
    updated,
    needsManual,
    noProfile,
    okUnmapped,
  };

  const outPath = path.join(ROOT, "reports", "normalize-operator-setup-brands.json");
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify({ summary, results }, null, 2));
  console.log(`Wrote ${outPath}`);
  console.log(JSON.stringify(summary, null, 2));

  if (needsManual > 0 && args.apply) process.exitCode = 2;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
