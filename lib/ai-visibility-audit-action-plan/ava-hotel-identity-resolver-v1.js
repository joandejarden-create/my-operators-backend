/**
 * Resolve AVA hotel identity from Airtable record ID, propertyId, or name slug.
 * Does not invent Airtable fields — optional mapping file only.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listCertifiedAdpUniverse } from "./adp-ava-reference-contract-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const PAYLOAD_DIR = path.join(ROOT, "lib/ai-visibility-audit-action-plan/payloads");
const IDENTITY_MAP = path.join(
  ROOT,
  "data/ai-visibility-audit-action-plan/ava-hotel-identity-map-v1.json"
);

function loadIdentityMap() {
  if (!fs.existsSync(IDENTITY_MAP)) {
    return { contract: "AVA_HOTEL_IDENTITY_MAP_V1", hotels: [] };
  }
  return JSON.parse(fs.readFileSync(IDENTITY_MAP, "utf8"));
}

function payloadFileForPropertyId(propertyId) {
  const files = fs.readdirSync(PAYLOAD_DIR).filter((f) => f.endsWith("-v1.json"));
  for (const f of files) {
    try {
      const p = JSON.parse(fs.readFileSync(path.join(PAYLOAD_DIR, f), "utf8"));
      if (p.property?.propertyId === propertyId) return f;
    } catch {
      /* skip */
    }
  }
  return null;
}

export function listAvaEligibleHotels() {
  const map = loadIdentityMap();
  const byId = new Map((map.hotels || []).map((h) => [h.propertyId, h]));
  return listCertifiedAdpUniverse().map((h) => {
    const mapped = byId.get(h.propertyId) || {};
    return {
      propertyId: h.propertyId,
      name: h.name,
      periodId: h.periodId,
      airtableRecordId: mapped.airtableRecordId || null,
      payloadFile: payloadFileForPropertyId(h.propertyId),
    };
  });
}

/**
 * @param {string} hotelRef - Airtable rec…, adp_* propertyId, or payload slug
 */
export function resolveAvaHotelIdentity(hotelRef) {
  const raw = String(hotelRef || "").trim();
  if (!raw) {
    return { ok: false, error: "HOTEL_REF_MISSING" };
  }
  const map = loadIdentityMap();
  const hotels = listAvaEligibleHotels();

  if (/^rec[a-zA-Z0-9]{10,}$/i.test(raw)) {
    const hit =
      (map.hotels || []).find((h) => h.airtableRecordId === raw) ||
      hotels.find((h) => h.airtableRecordId === raw);
    if (!hit) {
      return {
        ok: false,
        error: "AIRTABLE_RECORD_NOT_MAPPED",
        message:
          `Airtable record ${raw} is not in ava-hotel-identity-map-v1.json. ` +
          "Add the mapping before running by HotelId=rec…, or pass -PropertyId adp_…",
        hotelRef: raw,
      };
    }
    const propertyId = hit.propertyId;
    const universe = hotels.find((h) => h.propertyId === propertyId);
    if (!universe?.payloadFile) {
      return { ok: false, error: "AVA_PAYLOAD_MISSING", propertyId, hotelRef: raw };
    }
    return {
      ok: true,
      propertyId,
      airtableRecordId: raw,
      name: universe.name || hit.name || propertyId,
      payloadFile: universe.payloadFile,
      payloadPath: path.join(PAYLOAD_DIR, universe.payloadFile),
    };
  }

  if (/^adp_/i.test(raw)) {
    const universe = hotels.find((h) => h.propertyId === raw);
    if (!universe?.payloadFile) {
      return { ok: false, error: "AVA_PAYLOAD_MISSING", propertyId: raw, hotelRef: raw };
    }
    return {
      ok: true,
      propertyId: raw,
      airtableRecordId: universe.airtableRecordId || null,
      name: universe.name,
      payloadFile: universe.payloadFile,
      payloadPath: path.join(PAYLOAD_DIR, universe.payloadFile),
    };
  }

  // slug / filename stem
  const slug = raw.replace(/-v1\.json$/i, "").toLowerCase();
  const file = `${slug}-v1.json`;
  if (fs.existsSync(path.join(PAYLOAD_DIR, file))) {
    const p = JSON.parse(fs.readFileSync(path.join(PAYLOAD_DIR, file), "utf8"));
    return {
      ok: true,
      propertyId: p.property.propertyId,
      airtableRecordId:
        (map.hotels || []).find((h) => h.propertyId === p.property.propertyId)
          ?.airtableRecordId || null,
      name: p.property.name,
      payloadFile: file,
      payloadPath: path.join(PAYLOAD_DIR, file),
    };
  }

  const byName = hotels.find(
    (h) =>
      String(h.name || "").toLowerCase() === raw.toLowerCase() ||
      String(h.propertyId || "").toLowerCase() === raw.toLowerCase()
  );
  if (byName?.payloadFile) {
    return {
      ok: true,
      propertyId: byName.propertyId,
      airtableRecordId: byName.airtableRecordId || null,
      name: byName.name,
      payloadFile: byName.payloadFile,
      payloadPath: path.join(PAYLOAD_DIR, byName.payloadFile),
    };
  }

  return { ok: false, error: "HOTEL_NOT_FOUND", hotelRef: raw };
}

export function ensureIdentityMapScaffold() {
  if (fs.existsSync(IDENTITY_MAP)) return IDENTITY_MAP;
  fs.mkdirSync(path.dirname(IDENTITY_MAP), { recursive: true });
  const hotels = listCertifiedAdpUniverse().map((h) => ({
    propertyId: h.propertyId,
    name: h.name,
    airtableRecordId: null,
    note: "Populate airtableRecordId when Hotel Master link is authorized",
  }));
  const doc = {
    contract: "AVA_HOTEL_IDENTITY_MAP_V1",
    updatedAt: new Date().toISOString(),
    hotels,
  };
  fs.writeFileSync(IDENTITY_MAP, `${JSON.stringify(doc, null, 2)}\n`);
  return IDENTITY_MAP;
}
