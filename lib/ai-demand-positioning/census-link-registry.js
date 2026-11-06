/**
 * AI Demand Positioning — Census link registry.
 * Maps ADP Property ID → Hotel Property Census rec… for cross-base joins.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { isValidCensusRecordId } from "./airtable-field-map.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const REGISTRY_PATH = path.join(ROOT, "fixtures/ai-demand-positioning/census-links-v1.json");

let cachedRegistry = null;

export function loadCensusLinkRegistry() {
  if (cachedRegistry) return cachedRegistry;
  if (!fs.existsSync(REGISTRY_PATH)) {
    cachedRegistry = { version: "adp_census_links_v1", links: {} };
    return cachedRegistry;
  }
  cachedRegistry = JSON.parse(fs.readFileSync(REGISTRY_PATH, "utf8"));
  return cachedRegistry;
}

export function getCensusRecordIdForAdpProperty(adpPropertyId) {
  const registry = loadCensusLinkRegistry();
  const entry = registry.links?.[adpPropertyId];
  if (!entry?.censusRecordId) return null;
  const id = String(entry.censusRecordId).trim();
  return isValidCensusRecordId(id) ? id : null;
}

export function getCensusLinkEntry(adpPropertyId) {
  const registry = loadCensusLinkRegistry();
  return registry.links?.[adpPropertyId] || null;
}

/** Invalidate in-memory cache after durable registry writes. */
export function clearCensusLinkRegistryCache() {
  cachedRegistry = null;
}

/**
 * Find ADP property ids linked in the census registry by exact/fuzzy property name.
 * Used for post-HPC-create greenfield identity (not limited to Posadas).
 */
export function findCensusLinkedPropertiesByHotelQuery(hotelQuery) {
  const q = String(hotelQuery || "").trim();
  if (!q) return [];
  const norm = (s) =>
    String(s || "")
      .toLowerCase()
      .normalize("NFKD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/[^a-z0-9]+/g, " ")
      .trim();
  const needle = norm(q);
  if (!needle) return [];
  const registry = loadCensusLinkRegistry();
  const hits = [];
  for (const [propertyId, entry] of Object.entries(registry.links || {})) {
    const name = String(entry?.propertyName || "").trim();
    const nameN = norm(name);
    const idN = norm(String(propertyId).replace(/^adp_/, "").replace(/_/g, " "));
    const exact =
      propertyId === q ||
      (nameN && nameN === needle) ||
      (idN && idN === needle);
    // Guard: never treat empty/short labels as fuzzy includes("") matches.
    const fuzzyName =
      Boolean(nameN) &&
      nameN.length >= 6 &&
      needle.length >= 6 &&
      (nameN.includes(needle) || needle.includes(nameN));
    const fuzzyId =
      Boolean(idN) &&
      idN.length >= 6 &&
      needle.length >= 6 &&
      (idN.includes(needle) || needle.includes(idN));
    const score = exact ? 1 : fuzzyName || fuzzyId ? 0.85 : 0;
    if (exact || score >= 0.85) {
      hits.push({
        propertyId,
        hotelName: name || propertyId,
        hpcId: entry?.censusRecordId || null,
        score,
        exact,
        linkStatus: entry?.linkStatus || null,
      });
    }
  }
  hits.sort((a, b) => Number(b.exact) - Number(a.exact) || b.score - a.score);
  return hits;
}

/** Airtable record URL for ops / admin (cross-base). */
export function buildCensusRecordUrl(censusRecordId) {
  const id = String(censusRecordId || "").trim();
  if (!isValidCensusRecordId(id)) return null;
  const baseId = process.env.AIRTABLE_BASE_ID_ALT || process.env.AIRTABLE_BASE_ID;
  if (!baseId) return null;
  return `https://airtable.com/${baseId}/${id}`;
}

export function resolveCensusRecordIdForPublish(adpPropertyId, override = null) {
  const explicit = String(override || "").trim();
  if (explicit) {
    if (!isValidCensusRecordId(explicit)) {
      throw new Error(`Invalid --census-id format: ${explicit}`);
    }
    return explicit;
  }
  return getCensusRecordIdForAdpProperty(adpPropertyId);
}
