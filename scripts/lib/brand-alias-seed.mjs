/**
 * Shared idempotent upsert for Brand Alias Mapping rows.
 */
import { readFileSync } from "fs";
import Airtable from "airtable";
import { ALIAS_FIELDS } from "../../lib/hotel-census/fields.js";
import { exactMatchKey } from "../../lib/hotel-census/brand-alias-resolve.js";

export const DEFAULT_ALIAS_TABLE =
  process.env.AIRTABLE_BRAND_ALIAS_TABLE || "Brand Alias Mapping";

export function upsertKey(canonical, alias, parent) {
  return [canonical, alias, parent].map((s) => exactMatchKey(s)).join("\u0001");
}

function parseFixtureActive(value) {
  if (value === false || value === "false") return false;
  if (value === true || value === "true") return true;
  return true;
}

function isRecordActive(fields) {
  const v = fields?.[ALIAS_FIELDS.active];
  if (v === false) return false;
  const s = String(v ?? "")
    .trim()
    .toLowerCase();
  return s === "yes" || s === "true" || s === "1" || s === "active";
}

export function rowFromFixture(row) {
  return {
    canonical: exactMatchKey(row["Canonical Brand Name"]),
    alias: exactMatchKey(row["Alias / Source Brand Name"]),
    parent: exactMatchKey(row["Parent Company"]),
    active: parseFixtureActive(row.Active),
    matchConfidence: row["Match Confidence"] || null,
    notes: row.Notes || "",
  };
}

export function rowFromReviewed(row) {
  const base = rowFromFixture(row);
  return base;
}

function airtableFields(row) {
  return {
    [ALIAS_FIELDS.canonicalBrandName]: row.canonical,
    [ALIAS_FIELDS.aliasSourceBrandName]: row.alias,
    [ALIAS_FIELDS.parentCompany]: row.parent,
    [ALIAS_FIELDS.active]: row.active,
    [ALIAS_FIELDS.matchConfidence]: row.matchConfidence,
    [ALIAS_FIELDS.notes]: row.notes,
  };
}

function fieldsDiffer(existing, desired) {
  const e = existing || {};
  const d = desired;
  if (Boolean(e[ALIAS_FIELDS.active]) !== Boolean(d[ALIAS_FIELDS.active])) return true;
  if (exactMatchKey(e[ALIAS_FIELDS.matchConfidence]) !== exactMatchKey(d[ALIAS_FIELDS.matchConfidence]))
    return true;
  if (exactMatchKey(e[ALIAS_FIELDS.notes]) !== exactMatchKey(d[ALIAS_FIELDS.notes])) return true;
  if (exactMatchKey(e[ALIAS_FIELDS.parentCompany]) !== exactMatchKey(d[ALIAS_FIELDS.parentCompany]))
    return true;
  return false;
}

async function chunkBatch(table, records, method) {
  const CHUNK = 10;
  for (let i = 0; i < records.length; i += CHUNK) {
    await table[method](records.slice(i, i + CHUNK));
  }
}

/**
 * @param {object} opts
 * @param {string} opts.fixturePath
 * @param {boolean} [opts.dryRun]
 * @param {string} [opts.apiKey]
 * @param {string} [opts.baseId]
 * @param {string} [opts.tableName]
 */
export async function upsertAliasRowsFromFixture(opts) {
  const apiKey = opts.apiKey || process.env.AIRTABLE_API_KEY;
  const baseId = opts.baseId || process.env.AIRTABLE_BASE_ID_ALT;
  const tableName = opts.tableName || DEFAULT_ALIAS_TABLE;
  const dryRun = !!opts.dryRun;

  if (!apiKey) throw new Error("Set AIRTABLE_API_KEY for seeding");
  if (!baseId) throw new Error("Set AIRTABLE_BASE_ID_ALT");

  const fixture = JSON.parse(readFileSync(opts.fixturePath, "utf8"));
  const seedRows = (fixture.rows || []).map(rowFromFixture);

  const seen = new Set();
  for (const row of seedRows) {
    if (!row.canonical || !row.alias) {
      throw new Error(`Invalid row (missing canonical or alias): ${JSON.stringify(row)}`);
    }
    const key = upsertKey(row.canonical, row.alias, row.parent);
    if (seen.has(key)) throw new Error(`Duplicate seed key: ${key}`);
    seen.add(key);
  }

  const base = new Airtable({ apiKey }).base(baseId);
  const table = base(tableName);

  let existing = [];
  try {
    existing = await table
      .select({ fields: Object.values(ALIAS_FIELDS), pageSize: 100 })
      .all();
  } catch (err) {
    const msg = err?.message || String(err);
    if (/could not find|not found|not authorized/i.test(msg)) {
      throw new Error(`Cannot read "${tableName}": ${msg}`);
    }
    throw err;
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

  const toCreate = [];
  const toUpdate = [];
  let deactivated = 0;

  for (const row of seedRows) {
    const key = upsertKey(row.canonical, row.alias, row.parent);
    const fields = airtableFields(row);
    const found = byKey.get(key);
    if (!found) {
      toCreate.push({ fields });
      if (!row.active) deactivated += 1;
      continue;
    }
    if (fieldsDiffer(found.fields, fields)) {
      toUpdate.push({ id: found.id, fields });
      if (!row.active && isRecordActive(found.fields)) deactivated += 1;
    }
  }

  const stats = {
    fixture: opts.fixturePath,
    created: toCreate.length,
    updated: toUpdate.length,
    deactivated,
    skipped: seedRows.length - toCreate.length - toUpdate.length,
    totalRows: seedRows.length,
    errors: [],
  };

  if (dryRun) return stats;

  if (toCreate.length) await chunkBatch(table, toCreate, "create");
  if (toUpdate.length) await chunkBatch(table, toUpdate, "update");

  return stats;
}
