/**
 * Company Profile "Brands You Operate / Support" backfill — verified sources only.
 * Never invents brand names; only links Brand Setup - Brand Basics record IDs.
 */

import { normalizeCompanyTypeToFilterKey } from "./company-type-normalize.js";

export const MAP_CP_BRANDS_AIRTABLE = {
  brandLinkField: "Brands You Operate / Support",
  brandLinkFieldAlt: "Brands You Operate/Support",
};

export const BRAND_LOOKUP_NAME_FIELDS = [
  "Brand Name (from Brands You Operate / Support)",
  "Brand Name from Brands You Operate / Support",
  "Brands You Operate / Support (Names)",
];

const BRAND_BASICS_NAME_FIELD = "Brand Name";
const BRAND_BASICS_PARENT_FIELD = "Parent Company";

const OPERATOR_BASICS_TABLE =
  process.env.AIRTABLE_THIRD_PARTY_OPERATORS_TABLE || "3rd Party Operator - Basics";
const OPERATOR_NEW_BASE_PROFILE_TABLE =
  process.env.AIRTABLE_OPERATOR_SETUP_PROFILE_TABLE || "Operator Setup - Profile & Positioning";

const OPERATOR_BRAND_FIELD_CANDIDATES = new Set(
  [
    "Brands Managed",
    "Brands Supported",
    "brands",
    "Brands",
  ].map((s) => s.toLowerCase())
);

function toStr(v) {
  return v == null ? "" : String(v).trim();
}

export function normalizeCompanyNameKey(name) {
  return toStr(name).replace(/\s+/g, " ").toLowerCase();
}

/** Match keys for Company Profile ↔ Operator Setup (drops parentheticals like "(CALA)"). */
export function normalizeCompanyNameMatchKey(name) {
  return normalizeCompanyNameKey(toStr(name).replace(/\s*\([^)]*\)\s*/g, " ").trim());
}

export function normalizeBrandNameKey(name) {
  return toStr(name).replace(/\s+/g, " ").toLowerCase();
}

/** Strip trailing legal suffixes so "Marriott International" matches "Marriott International, Inc." */
export function normalizeParentCompanyKey(name) {
  return toStr(name)
    .replace(/,?\s*(inc\.?|incorporated|llc|l\.?l\.?c\.?|ltd\.?|limited|corp\.?|corporation|co\.?|company|plc|s\.?a\.?|gmbh)\.?\s*$/gi, "")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
}

export function isRecordId(value) {
  return typeof value === "string" && /^rec[a-zA-Z0-9]{5,}$/.test(value.trim());
}

function toArray(v) {
  if (Array.isArray(v)) return v;
  if (v == null) return [];
  if (typeof v === "string") {
    const raw = v.trim();
    if (!raw) return [];
    return raw.split(",").map((s) => s.trim()).filter(Boolean);
  }
  return [];
}

export function extractRecordIdsFromValue(value, out = []) {
  if (value == null) return out;
  if (Array.isArray(value)) {
    value.forEach((item) => extractRecordIdsFromValue(item, out));
    return out;
  }
  if (typeof value === "string" && isRecordId(value)) {
    out.push(value.trim());
    return out;
  }
  if (typeof value === "object") {
    if (isRecordId(value.id)) out.push(String(value.id).trim());
    if (isRecordId(value.recordId)) out.push(String(value.recordId).trim());
  }
  return out;
}

function extractNamesFromLookupValue(value) {
  const names = [];
  for (const item of toArray(value)) {
    if (typeof item === "string" && !isRecordId(item)) {
      const s = toStr(item);
      if (s) names.push(s);
    } else if (item && typeof item === "object") {
      const n =
        toStr(item.name) ||
        toStr(item.label) ||
        toStr(item.fields?.[BRAND_BASICS_NAME_FIELD]) ||
        "";
      if (n) names.push(n);
    }
  }
  return names;
}

function readLinkIdsFromCompanyFields(fields) {
  const ids = [];
  const keys = [MAP_CP_BRANDS_AIRTABLE.brandLinkField, MAP_CP_BRANDS_AIRTABLE.brandLinkFieldAlt];
  for (const key of keys) {
    if (fields[key] != null) extractRecordIdsFromValue(fields[key], ids);
  }
  return [...new Set(ids)];
}

function readLookupNamesFromCompanyFields(fields) {
  const names = [];
  for (const key of BRAND_LOOKUP_NAME_FIELDS) {
    if (fields[key] == null) continue;
    extractNamesFromLookupValue(fields[key]).forEach((n) => names.push(n));
  }
  return [...new Set(names.map(toStr).filter(Boolean))];
}

function fieldKeyMatchesOperatorBrands(fieldName) {
  const k = toStr(fieldName).toLowerCase();
  if (OPERATOR_BRAND_FIELD_CANDIDATES.has(k)) return true;
  return k === "brands managed" || k === "brands supported";
}

function readOperatorBrandLinkIds(fields) {
  const ids = [];
  if (!fields || typeof fields !== "object") return ids;
  for (const [key, value] of Object.entries(fields)) {
    if (!fieldKeyMatchesOperatorBrands(key)) continue;
    extractRecordIdsFromValue(value, ids);
  }
  return [...new Set(ids)];
}

function readOperatorCompanyName(fields) {
  return (
    toStr(fields["Company Name"]) ||
    toStr(fields.company_name) ||
    toStr(fields.companyName) ||
    ""
  );
}

/**
 * @param {Array<{ id: string, fields?: object }>} brandRecords — Brand Setup - Brand Basics
 */
export function buildBrandBasicsIndex(brandRecords) {
  const idToName = new Map();
  const nameToIds = new Map();
  const parentCompanyKeyToBrandIds = new Map();

  for (const rec of brandRecords || []) {
    if (!rec?.id) continue;
    const f = rec.fields || {};
    const brandName = toStr(f[BRAND_BASICS_NAME_FIELD]);
    if (brandName) {
      idToName.set(rec.id, brandName);
      const nameKey = normalizeBrandNameKey(brandName);
      if (!nameToIds.has(nameKey)) nameToIds.set(nameKey, []);
      nameToIds.get(nameKey).push(rec.id);
    }
    const parent = toStr(f[BRAND_BASICS_PARENT_FIELD]);
    if (parent) {
      const parentKey = normalizeParentCompanyKey(parent);
      if (!parentKey) continue;
      if (!parentCompanyKeyToBrandIds.has(parentKey)) {
        parentCompanyKeyToBrandIds.set(parentKey, []);
      }
      parentCompanyKeyToBrandIds.get(parentKey).push(rec.id);
    }
  }

  return { idToName, nameToIds, parentCompanyKeyToBrandIds };
}

/**
 * Exact name match only; skips ambiguous duplicate Brand Name rows.
 */
export function resolveBrandNamesToIds(names, nameToIds) {
  const resolved = [];
  const unresolved = [];
  const ambiguous = [];

  for (const rawName of names || []) {
    const name = toStr(rawName);
    if (!name) continue;
    const key = normalizeBrandNameKey(name);
    const matches = nameToIds.get(key) || [];
    if (matches.length === 0) {
      unresolved.push(name);
      continue;
    }
    if (matches.length > 1) {
      ambiguous.push({ name, recordIds: [...matches] });
      continue;
    }
    resolved.push(matches[0]);
  }

  return {
    resolved: [...new Set(resolved)],
    unresolved,
    ambiguous,
  };
}

function isFranchisorCompanyType(companyTypeRaw) {
  const key = normalizeCompanyTypeToFilterKey(companyTypeRaw);
  return key === "HOTEL BRANDS (FRANCHISE)";
}

function sortedIds(ids) {
  return [...new Set(ids)].sort();
}

function idsEqual(a, b) {
  const sa = sortedIds(a);
  const sb = sortedIds(b);
  return sa.length === sb.length && sa.every((id, i) => id === sb[i]);
}

/**
 * @param {{ id: string, fields?: object }} companyRecord
 * @param {{
 *   idToName: Map<string,string>,
 *   nameToIds: Map<string,string[]>,
 *   parentCompanyKeyToBrandIds: Map<string,string[]>,
 *   operatorBasicsByCompanyKey: Map<string, { recordId: string, brandIds: string[] }>,
 *   operatorProfileByCompanyKey: Map<string, { recordId: string, brandIds: string[] }>,
 * }} ctx
 */
export function buildCompanyProfileBrandsBackfillPlan(companyRecord, ctx) {
  const fields = companyRecord?.fields || {};
  const companyName = toStr(fields["Company Name"]) || "(no name)";
  const companyKey = normalizeCompanyNameMatchKey(companyName);
  const companyTypeRaw = toStr(fields["Company Type"]);

  const existingIds = readLinkIdsFromCompanyFields(fields);
  const verifiedIds = new Set(existingIds);
  const sources = [];

  if (existingIds.length) {
    sources.push({
      source: "existing_link",
      count: existingIds.length,
      detail: existingIds.map((id) => ctx.idToName.get(id) || id).join(", "),
    });
  }

  const lookupNames = readLookupNamesFromCompanyFields(fields);
  if (lookupNames.length) {
    const { resolved, unresolved, ambiguous } = resolveBrandNamesToIds(
      lookupNames,
      ctx.nameToIds
    );
    resolved.forEach((id) => verifiedIds.add(id));
    sources.push({
      source: "lookup_field_names",
      count: resolved.length,
      names: lookupNames,
      unresolved,
      ambiguous,
    });
  }

  const basics = findOperatorEntryByCompanyName(companyName, ctx.operatorBasicsByCompanyKey);
  if (basics?.brandIds?.length) {
    basics.brandIds.forEach((id) => verifiedIds.add(id));
    sources.push({
      source: OPERATOR_BASICS_TABLE,
      operatorRecordId: basics.recordId,
      operatorLabel: basics.label,
      count: basics.brandIds.length,
    });
  }

  const profile = findOperatorEntryByCompanyName(companyName, ctx.operatorProfileByCompanyKey);
  if (profile?.brandIds?.length) {
    profile.brandIds.forEach((id) => verifiedIds.add(id));
    sources.push({
      source: OPERATOR_NEW_BASE_PROFILE_TABLE,
      profileRecordId: profile.recordId,
      operatorLabel: profile.label,
      count: profile.brandIds.length,
    });
  }

  if (isFranchisorCompanyType(companyTypeRaw) && companyKey) {
    const parentLookupKey = normalizeParentCompanyKey(companyName);
    const parentIds = ctx.parentCompanyKeyToBrandIds.get(parentLookupKey) || [];
    if (parentIds.length) {
      parentIds.forEach((id) => verifiedIds.add(id));
      sources.push({
        source: "brand_basics_parent_company",
        count: parentIds.length,
        companyType: companyTypeRaw,
      });
    }
  }

  const mergedIds = sortedIds([...verifiedIds]);
  const patch = {};
  if (!idsEqual(existingIds, mergedIds)) {
    patch[MAP_CP_BRANDS_AIRTABLE.brandLinkField] = mergedIds;
  }

  const brandLabels = mergedIds.map((id) => ctx.idToName.get(id) || id);

  return {
    companyId: companyRecord.id,
    companyName,
    companyType: companyTypeRaw,
    existingIds,
    mergedIds,
    brandLabels,
    patch,
    sources,
    hasChange: Object.keys(patch).length > 0,
  };
}

function pickBetterOperatorEntry(prev, next) {
  if (!prev) return next;
  if (!next) return prev;
  if (next.brandIds.length > prev.brandIds.length) return next;
  return prev;
}

/**
 * Index operator rows by normalized company name (parentheticals stripped).
 * @param {Array<{ id: string, fields?: object }>} records
 */
export function indexOperatorRowsByCompanyName(records) {
  const map = new Map();
  for (const rec of records || []) {
    const f = rec.fields || {};
    const name = readOperatorCompanyName(f);
    const key = normalizeCompanyNameMatchKey(name);
    if (!key) continue;
    const brandIds = readOperatorBrandLinkIds(f);
    if (!brandIds.length) continue;
    map.set(key, pickBetterOperatorEntry(map.get(key), { recordId: rec.id, brandIds, label: name }));
  }
  return map;
}

/** Resolve operator row for a Company Profile name (exact match key after normalization). */
export function findOperatorEntryByCompanyName(companyName, operatorByKey) {
  const key = normalizeCompanyNameMatchKey(companyName);
  if (!key) return null;
  return operatorByKey.get(key) || null;
}

export const BRAND_BACKFILL_TABLES = {
  companyProfile: process.env.COMPANY_PROFILE_TABLE_ID || "tblItyfH6MlOnMKZ9",
  brandBasics: process.env.AIRTABLE_BRAND_SETUP_BASICS_TABLE || "Brand Setup - Brand Basics",
  operatorBasics: OPERATOR_BASICS_TABLE,
  operatorProfile: OPERATOR_NEW_BASE_PROFILE_TABLE,
};
