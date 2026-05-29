/**
 * Operator-scoped deal metadata for My Operator Deals (Phase 4).
 * Shared extractors aligned with brand deal-meta / my-deals display.
 */

import {
  MARKET_PERFORMANCE_LINK_FIELD,
  MARKET_PERFORMANCE_TABLE,
  LOCATION_PROPERTY_TABLE,
  LOCATION_LINK_FIELD,
  CONTACT_UPLOADS_TABLE,
  CONTACT_UPLOADS_LINK_FIELD,
} from "../../api/schemas/deal-setup-fields.js";
import { escapeAirtableFormulaValue } from "../airtable-utils.js";

const DEALS_TABLE = process.env.AIRTABLE_TABLE_DEALS || "Deals";

export function dealMetaValueToStr(v) {
  if (v == null) return "";
  if (typeof v === "string") return v.trim();
  if (typeof v === "number" && !Number.isNaN(v)) return String(v);
  if (typeof v === "object" && v !== null && typeof v.name === "string") return v.name.trim();
  if (Array.isArray(v) && v[0]) return dealMetaValueToStr(v[0]);
  return "";
}

export function pickDealField(f, keys) {
  for (let i = 0; i < keys.length; i++) {
    const v = dealMetaValueToStr(f[keys[i]]);
    if (v) return v;
  }
  return "";
}

function formatCityCountry(city, country) {
  const c = String(city || "").trim();
  const co = String(country || "").trim();
  if (c && co) return `${c}, ${co}`;
  return c || co || "";
}

export function extractDealLocationLine(f) {
  const fromPick = pickDealField(f, [
    "Project Location (Core)",
    "Project Location",
    "Hotel Location",
    "Location",
    "Hotel Submarket & Location",
  ]);
  if (fromPick) return fromPick;
  return formatCityCountry(
    pickDealField(f, ["City & State", "City", "City & State (from Location & Property)"]),
    pickDealField(f, ["Country", "Country (from Location & Property)"]),
  );
}

export function extractDealCountry(f) {
  return pickDealField(f, ["Country", "Country (from Location & Property)"]) || null;
}

export function extractOwnerCompany(f) {
  return (
    pickDealField(f, [
      "Entity or Company Name",
      "Parent Company Name",
      "Company Name",
      "Owner Company Name",
    ]) || null
  );
}

export function extractDealRooms(f) {
  const raw = pickDealField(f, [
    "Total Number of Rooms/Keys",
    "Number of Keys",
    "Total Rooms",
    "Rooms",
    "Room Count",
  ]);
  if (!raw) return null;
  const n = Number(String(raw).replace(/,/g, "").trim());
  if (!Number.isNaN(n) && Number.isFinite(n)) return n;
  return raw;
}

function combineLocationAndProjectTitle(locationLine, projectNameRaw) {
  const loc = String(locationLine || "").trim();
  const proj = String(projectNameRaw || "").trim();
  if (!loc && !proj) return "";
  if (!loc) return proj;
  if (!proj) return loc;
  const pl = proj.toLowerCase();
  const ll = loc.toLowerCase();
  if (pl.includes(ll) || ll.includes(pl)) return proj;
  return `${loc} – ${proj}`;
}

function getFirstLinkedRecordId(fields, linkField) {
  const raw = fields?.[linkField];
  if (!Array.isArray(raw) || raw.length === 0) return null;
  const id = typeof raw[0] === "string" ? raw[0] : raw[0]?.id;
  return id && String(id).startsWith("rec") ? id : null;
}

function collectLinkedIds(records, linkField) {
  const ids = new Set();
  for (const rec of records || []) {
    const id = getFirstLinkedRecordId(rec.fields, linkField);
    if (id) ids.add(id);
  }
  return [...ids];
}

async function fetchLinkedRecordsById(base, tableName, ids) {
  const map = new Map();
  const chunkSize = 40;
  for (let i = 0; i < ids.length; i += chunkSize) {
    const chunk = ids.slice(i, i + chunkSize);
    const formula = `OR(${chunk.map((id) => `RECORD_ID()='${escapeAirtableFormulaValue(id)}'`).join(",")})`;
    const recs = await base(tableName).select({ filterByFormula: formula }).all();
    recs.forEach((r) => map.set(r.id, r.fields || {}));
  }
  return map;
}

/** Merge deal + linked Location & Property + Contact & Uploads (owner-entered intake shape). */
export function mergeDealContextFields(dealFields, locationFields, contactFields) {
  const d = dealFields || {};
  const lp = locationFields || {};
  const cu = contactFields || {};
  return {
    ...lp,
    ...cu,
    ...d,
    Country: pickDealField(d, ["Country", "Country (from Location & Property)"]) || pickDealField(lp, ["Country"]),
    "City & State":
      pickDealField(d, ["City & State", "City", "City & State (from Location & Property)"]) ||
      pickDealField(lp, ["City", "City & State"]),
    "Hotel Submarket & Location":
      pickDealField(d, ["Hotel Submarket & Location"]) || pickDealField(lp, ["Hotel Submarket & Location"]),
    "Total Number of Rooms/Keys":
      pickDealField(d, ["Total Number of Rooms/Keys"]) ||
      (lp["Total Number of Rooms/Keys"] != null ? String(lp["Total Number of Rooms/Keys"]) : ""),
    "Entity or Company Name":
      pickDealField(d, ["Entity or Company Name", "Company Name", "Parent Company Name"]) ||
      pickDealField(cu, ["Entity or Company Name", "Company Name", "Parent Company Name"]),
  };
}

export function buildDealMetaFromMergedFields(mergedFields, preferredDealStructureFromMp) {
  return buildDealMetaFromFields(mergedFields, preferredDealStructureFromMp);
}

function getLinkedMarketPerformanceId(fields) {
  if (!fields) return null;
  const raw = fields[MARKET_PERFORMANCE_LINK_FIELD];
  if (!Array.isArray(raw) || raw.length === 0) return null;
  const id = typeof raw[0] === "string" ? raw[0] : raw[0]?.id;
  return id && typeof id === "string" && id.startsWith("rec") ? id : null;
}

export function buildDealMetaFromFields(fields, preferredDealStructureFromMp) {
  const f = fields || {};
  const mpFallback = dealMetaValueToStr(preferredDealStructureFromMp);
  let dealType =
    dealMetaValueToStr(f["Preferred Deal Structure"]) ||
    dealMetaValueToStr(f["Preferred Deal Structure (from Market - Performance - Deal & Capital Structure)"]) ||
    "";
  if (!dealType && mpFallback) dealType = mpFallback;
  if (!dealType) {
    dealType = pickDealField(f, ["Deal Type (Core)", "Deal Type", "Deal Structure"]);
  }

  const projectNameRaw = pickDealField(f, ["Project Name", "Property Name", "Name"]);
  const locationLine = extractDealLocationLine(f);
  const country = extractDealCountry(f);
  const ownerCompany = extractOwnerCompany(f);
  const primaryName = combineLocationAndProjectTitle(locationLine, projectNameRaw) || projectNameRaw || "";

  const projectName = projectNameRaw || primaryName || null;
  const title = primaryName || projectNameRaw || null;
  const rooms = extractDealRooms(f);

  return {
    title,
    projectName,
    locationLine: locationLine || null,
    country,
    ownerCompany,
    rooms,
    dealType: dealType || null,
  };
}

/**
 * Fetch deal metadata for allowed deal record ids (max 40 per call).
 */
export async function fetchDealMetaForIds(base, dealIds) {
  const ids = [...new Set((dealIds || []).filter((id) => String(id).startsWith("rec")))].slice(0, 40);
  if (!ids.length) return [];

  const chunkSize = 40;
  const allDealRecs = [];
  for (let i = 0; i < ids.length; i += chunkSize) {
    const chunk = ids.slice(i, i + chunkSize);
    const formula = `OR(${chunk.map((id) => `RECORD_ID()='${escapeAirtableFormulaValue(id)}'`).join(",")})`;
    const recs = await base(DEALS_TABLE).select({ filterByFormula: formula }).all();
    recs.forEach((r) => allDealRecs.push(r));
  }

  const locationIds = collectLinkedIds(allDealRecs, LOCATION_LINK_FIELD);
  const contactIds = collectLinkedIds(allDealRecs, CONTACT_UPLOADS_LINK_FIELD);
  const [locationById, contactById] = await Promise.all([
    locationIds.length ? fetchLinkedRecordsById(base, LOCATION_PROPERTY_TABLE, locationIds) : new Map(),
    contactIds.length ? fetchLinkedRecordsById(base, CONTACT_UPLOADS_TABLE, contactIds) : new Map(),
  ]);

  const mpIds = [
    ...new Set(allDealRecs.map((r) => getLinkedMarketPerformanceId(r.fields)).filter(Boolean)),
  ];
  const mpPreferredById = new Map();
  for (let j = 0; j < mpIds.length; j += chunkSize) {
    const mpChunk = mpIds.slice(j, j + chunkSize);
    const mpFormula = `OR(${mpChunk.map((id) => `RECORD_ID()='${escapeAirtableFormulaValue(id)}'`).join(",")})`;
    const mpRecs = await base(MARKET_PERFORMANCE_TABLE).select({ filterByFormula: mpFormula }).all();
    mpRecs.forEach((mp) => {
      const v = dealMetaValueToStr((mp.fields || {})["Preferred Deal Structure"]);
      if (v) mpPreferredById.set(mp.id, v);
    });
  }

  const byId = new Map();
  allDealRecs.forEach((r) => {
    const locId = getFirstLinkedRecordId(r.fields, LOCATION_LINK_FIELD);
    const cuId = getFirstLinkedRecordId(r.fields, CONTACT_UPLOADS_LINK_FIELD);
    const merged = mergeDealContextFields(
      r.fields,
      locId ? locationById.get(locId) : null,
      cuId ? contactById.get(cuId) : null,
    );
    const mpId = getLinkedMarketPerformanceId(r.fields);
    const fromMp = mpId ? mpPreferredById.get(mpId) : "";
    byId.set(r.id, buildDealMetaFromFields(merged, fromMp));
  });
  return ids.map((id) => {
    const m = byId.get(id);
    if (!m) {
      return {
        dealId: id,
        title: null,
        projectName: null,
        locationLine: null,
        country: null,
        ownerCompany: null,
        rooms: null,
      };
    }
    return { dealId: id, ...m };
  });
}
