/**
 * Read-only live Brand Basics + Brand Alias Mapping loader for AI Visibility.
 * Never writes Airtable. Eligibility: Brand Status ∈ {Active, Live}.
 */

import Airtable from "airtable";
import { createHash } from "crypto";
import { BRAND_STATUS_ACTIVE_FORMULA, isBrandStatusActive } from "../brand-status-active.js";
import {
  loadActiveBrandAliasRows,
  exactMatchKey,
} from "../hotel-census/brand-alias-resolve.js";
import { normalizeMatchKey } from "./normalize-entities.js";

const BRAND_BASICS_TABLE =
  process.env.AIRTABLE_BRAND_SETUP_BASICS_TABLE || "Brand Setup - Brand Basics";

const BRAND_FIELDS = Object.freeze({
  name: "Brand Name",
  parentCompany: "Parent Company",
  status: "Brand Status",
  chainScale: "Hotel Chain Scale",
  brandModel: "Brand Model",
});

function getProductBase() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) return null;
  return new Airtable({ apiKey }).base(baseId);
}

function cellToString(v) {
  if (v == null) return "";
  if (Array.isArray(v)) return cellToString(v[0]);
  if (typeof v === "object" && v.name != null) return String(v.name).trim();
  return String(v).trim();
}

function isLikelyParentCompanyLabel(name, parentCompany) {
  const n = normalizeMatchKey(name);
  const p = normalizeMatchKey(parentCompany);
  if (!n) return false;
  if (n.includes("international") || n.endsWith(" hotels corporation")) return true;
  if (p && n === p) return true;
  return false;
}

/**
 * @param {{ fetchBrandRecords?: Function, loadAliases?: Function }} [deps]
 */
export async function loadLiveBrandEntities(deps = {}) {
  const fetchBrandRecords =
    deps.fetchBrandRecords ||
    (async () => {
      const base = getProductBase();
      if (!base) {
        const err = new Error(
          "Airtable product base not configured (AIRTABLE_API_KEY / AIRTABLE_BASE_ID)"
        );
        err.code = "airtable_config_missing";
        throw err;
      }
      return base(BRAND_BASICS_TABLE)
        .select({
          filterByFormula: BRAND_STATUS_ACTIVE_FORMULA,
          fields: Object.values(BRAND_FIELDS),
          pageSize: 100,
        })
        .all();
    });

  const loadAliases = deps.loadAliases || loadActiveBrandAliasRows;

  const records = await fetchBrandRecords();
  const aliasRows = await loadAliases();

  /** @type {Map<string, object>} */
  const byNameKey = new Map();
  const brands = [];

  for (const rec of records || []) {
    const f = rec.fields || {};
    const name = cellToString(f[BRAND_FIELDS.name]);
    const status = cellToString(f[BRAND_FIELDS.status]);
    if (!name || !isBrandStatusActive(status)) continue;

    const parentCompany = cellToString(f[BRAND_FIELDS.parentCompany]) || null;
    const entity = {
      id: rec.id,
      name,
      entityType: "brand",
      aliases: [],
      firstPartyDomains: [],
      parentCompany,
      isParentCompanyLabel: isLikelyParentCompanyLabel(name, parentCompany),
      brandStatus: status,
      chainScale: cellToString(f[BRAND_FIELDS.chainScale]) || null,
      brandModel: cellToString(f[BRAND_FIELDS.brandModel]) || null,
      sourceSystem: "brand_setup_brand_basics",
    };
    brands.push(entity);
    byNameKey.set(normalizeMatchKey(name), entity);
  }

  let aliasesAttached = 0;
  const unmatchedAliasCanonicals = [];

  for (const row of aliasRows || []) {
    const canonical = exactMatchKey(row.canonicalBrandName);
    const alias = exactMatchKey(row.aliasSourceBrandName);
    if (!canonical || !alias) continue;
    const entity = byNameKey.get(normalizeMatchKey(canonical));
    if (!entity) {
      unmatchedAliasCanonicals.push(canonical);
      continue;
    }
    if (normalizeMatchKey(alias) === normalizeMatchKey(entity.name)) continue;
    if (!entity.aliases.includes(alias)) {
      entity.aliases.push(alias);
      aliasesAttached += 1;
    }
    if (!entity.parentCompany && row.parentCompany) {
      entity.parentCompany = exactMatchKey(row.parentCompany);
    }
  }

  brands.sort((a, b) => a.name.localeCompare(b.name, undefined, { sensitivity: "base" }));

  const fingerprint = createHash("sha256")
    .update(
      brands
        .map((b) => `${b.id}|${b.name}|${(b.aliases || []).slice().sort().join(",")}`)
        .join("\n")
    )
    .digest("hex")
    .slice(0, 16);

  return {
    entities: brands,
    meta: {
      table: BRAND_BASICS_TABLE,
      eligibility: "Brand Status Active/Live",
      brandCount: brands.length,
      aliasRowsLoaded: (aliasRows || []).length,
      aliasesAttached,
      unmatchedAliasCanonicalSample: [...new Set(unmatchedAliasCanonicals)].slice(0, 25),
      fingerprint,
      airtableWrites: 0,
    },
  };
}

/**
 * Filter live brands to a governed cohort by exact name match (case-insensitive).
 * Missing names are returned for founder review — never invented.
 */
export function selectBrandsByCanonicalNames(entities, requestedNames) {
  const byKey = new Map(
    (entities || []).map((e) => [normalizeMatchKey(e.name), e])
  );
  const selected = [];
  const missing = [];
  for (const name of requestedNames || []) {
    const hit = byKey.get(normalizeMatchKey(name));
    if (hit) selected.push(hit);
    else missing.push(name);
  }
  return { selected, missing };
}

export { BRAND_FIELDS, BRAND_BASICS_TABLE };
