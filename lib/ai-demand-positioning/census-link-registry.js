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
