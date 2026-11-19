/**
 * Read-only Hotel Census identity search — LEGACY / MIGRATION ONLY.
 *
 * HE production-intended identity resolution must use HPC collision search.
 * This module is blocked by default via legacy-hotel-census-access-guard-v1.
 * Opt-in: HE_LEGACY_HOTEL_CENSUS_MIGRATION_READ=1 or allowLegacyHotelCensusRead:true.
 * Never writes.
 */

import "dotenv/config";
import {
  assertLegacyHotelCensusAccess,
  LEGACY_HOTEL_CENSUS_TABLE_ID,
  LEGACY_HOTEL_CENSUS_TABLE_NAME,
} from "./legacy-hotel-census-access-guard-v1.js";

export const HOTEL_CENSUS_IDENTITY_SEARCH_V1 = "HOTEL_CENSUS_IDENTITY_SEARCH_V1";
export const HOTEL_CENSUS_TABLE_NAME = LEGACY_HOTEL_CENSUS_TABLE_NAME;
export const HOTEL_CENSUS_TABLE_ID = LEGACY_HOTEL_CENSUS_TABLE_ID;

function airtableToken() {
  return (
    process.env.AIRTABLE_API_KEY ||
    process.env.AIRTABLE_PAT ||
    process.env.AIRTABLE_TOKEN ||
    ""
  ).trim();
}

function platformBaseId() {
  return (process.env.AIRTABLE_BASE_ID_ALT || "").trim();
}

function escapeFormula(s) {
  return String(s || "").replace(/'/g, "\\'");
}

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

async function airtableList(formula, maxRecords = 25, guardOpts = {}) {
  assertLegacyHotelCensusAccess(HOTEL_CENSUS_TABLE_ID, {
    method: "GET",
    workflow: "hotel-census-identity-search-v1",
    ...guardOpts,
  });
  const token = airtableToken();
  const baseId = platformBaseId();
  if (!token || !baseId) {
    const err = new Error("Missing AIRTABLE_API_KEY/PAT or AIRTABLE_BASE_ID_ALT");
    err.code = "HPC_CONFIG_MISSING";
    throw err;
  }
  const q = new URLSearchParams({
    maxRecords: String(maxRecords),
    filterByFormula: formula,
  });
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(
    HOTEL_CENSUS_TABLE_NAME
  )}?${q}`;
  assertLegacyHotelCensusAccess(url, {
    method: "GET",
    workflow: "hotel-census-identity-search-v1",
    ...guardOpts,
  });
  const res = await fetch(url, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    const err = new Error(
      `Hotel Census ${res.status}: ${json?.error?.message || JSON.stringify(json).slice(0, 180)}`
    );
    err.status = res.status;
    throw err;
  }
  return (json.records || []).map((r) => {
    const f = r.fields || {};
    return {
      id: r.id,
      name: f.name || f["Property Name"] || null,
      brand: f.Affiliation || f["Current Brand"] || null,
      city: f.city || f.City || null,
      country: f.country || f.Country || null,
      rooms: f.rooms ?? f["Rooms / Keys"] ?? null,
      market: f.Market || null,
      managementCompany: f["Management Company"] || null,
      table: HOTEL_CENSUS_TABLE_NAME,
    };
  });
}

function jaccard(a, b) {
  const ta = new Set(norm(a).split(" ").filter(Boolean));
  const tb = new Set(norm(b).split(" ").filter(Boolean));
  if (!ta.size || !tb.size) return 0;
  let inter = 0;
  for (const t of ta) if (tb.has(t)) inter += 1;
  return inter / (ta.size + tb.size - inter);
}

/**
 * @returns {Promise<{ hits: object[], queryLog: object[], ok: boolean, error?: string }>}
 */
export async function searchHotelCensusIdentityCandidates({
  hotelName,
  aliases = [],
  city = null,
  brand = null,
  country = null,
  maxRecords = 20,
  allowLegacyHotelCensusRead = false,
} = {}) {
  const guardOpts = { allowLegacyHotelCensusRead };
  // Fail closed before any network when migration flag is off.
  assertLegacyHotelCensusAccess(HOTEL_CENSUS_TABLE_ID, {
    method: "GET",
    workflow: "hotel-census-identity-search-v1",
    ...guardOpts,
  });
  const names = [hotelName, ...aliases]
    .map((n) => String(n || "").trim())
    .filter(Boolean);
  const unique = [...new Set(names)];
  const byId = new Map();
  const queryLog = [];

  for (const n of unique.slice(0, 8)) {
    const e = escapeFormula(n);
    const el = escapeFormula(n.toLowerCase());
    const formulas = [
      {
        label: `census_name:${n}`,
        f: `OR({name}='${e}', FIND(LOWER('${el}'), LOWER({name}&'')))`,
      },
    ];
    if (brand) {
      formulas.push({
        label: `census_affiliation_token:${n}`,
        f: `AND(FIND(LOWER('${escapeFormula(norm(brand))}'), LOWER({Affiliation}&'')), FIND(LOWER('${el}'), LOWER({name}&'')))`,
      });
    }
    for (const { label, f } of formulas) {
      try {
        const rows = await airtableList(f, maxRecords, guardOpts);
        queryLog.push({ label, hitCount: rows.length, ok: true });
        for (const row of rows) {
          if (!byId.has(row.id)) byId.set(row.id, row);
        }
      } catch (err) {
        queryLog.push({ label, ok: false, error: err.message || String(err) });
        if (err.code === "HPC_CONFIG_MISSING" || err.code === "LEGACY_HOTEL_CENSUS_ACCESS_BLOCKED") {
          throw err;
        }
      }
    }
  }

  if (city && brand) {
    const label = "census_city_brand";
    try {
      const f = `AND(FIND(LOWER('${escapeFormula(norm(city))}'), LOWER({city}&'')), FIND(LOWER('${escapeFormula(
        norm(brand)
      )}'), LOWER({Affiliation}&'')))`;
      const rows = await airtableList(f, maxRecords, guardOpts);
      queryLog.push({ label, hitCount: rows.length, ok: true });
      for (const row of rows) {
        if (!byId.has(row.id)) byId.set(row.id, row);
      }
    } catch (err) {
      queryLog.push({ label, ok: false, error: err.message || String(err) });
    }
  }

  const hits = [...byId.values()]
    .map((h) => {
      const nameScore = Math.max(
        jaccard(hotelName, h.name),
        ...aliases.map((a) => jaccard(a, h.name))
      );
      let score = nameScore;
      if (city && h.city && norm(city) === norm(h.city)) score += 0.08;
      if (country && h.country && norm(country) === norm(h.country)) score += 0.05;
      if (brand && h.brand && norm(h.brand).includes(norm(brand))) score += 0.05;
      return { ...h, score: Math.max(0, Math.min(1, Number(score.toFixed(4)))) };
    })
    .sort((a, b) => b.score - a.score);

  return { ok: true, hits, queryLog };
}
