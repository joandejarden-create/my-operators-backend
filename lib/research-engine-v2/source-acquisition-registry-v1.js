/**
 * Persistent source acquisition registry for overnight Mode B.
 * Learns official domains across runs — never stores secrets.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const SOURCE_REGISTRY_VERSION = "source-acquisition-registry-v1";

export const SOURCE_REGISTRY_FP = path.join(
  ROOT,
  "data/research-engine-v2/overnight-census-enrichment/source-registry.json"
);

export const SOURCE_DISCOVERY_STATE = Object.freeze({
  UNEXPLORED: "UNEXPLORED",
  DISCOVERING: "DISCOVERING",
  ACTIVE_HIGH_YIELD: "ACTIVE_HIGH_YIELD",
  ACTIVE_LOW_YIELD: "ACTIVE_LOW_YIELD",
  EXHAUSTED: "EXHAUSTED",
  TEMP_BLOCKED: "TEMP_BLOCKED",
  LONG_BLOCKED: "LONG_BLOCKED",
  USAGE_REVIEW: "USAGE_REVIEW",
  RETRY_LATER: "RETRY_LATER",
});

export function emptySourceEntry(partial = {}) {
  return {
    SOURCE_ID: partial.SOURCE_ID || partial.id || "",
    DOMAIN: partial.DOMAIN || partial.domain || "",
    COMPANY: partial.COMPANY || partial.company || "",
    SOURCE_TYPE: partial.SOURCE_TYPE || partial.source_type || "official_directory",
    GEOGRAPHIES: partial.GEOGRAPHIES || [],
    FIELDS_AVAILABLE: partial.FIELDS_AVAILABLE || [
      "Current Brand",
      "Address",
      "Website",
    ],
    DISCOVERY_METHOD: partial.DISCOVERY_METHOD || "seeded",
    ACCESS_STATUS: partial.ACCESS_STATUS || "public",
    USAGE_STATUS: partial.USAGE_STATUS || SOURCE_DISCOVERY_STATE.UNEXPLORED,
    LAST_CRAWLED: partial.LAST_CRAWLED || null,
    PAGES_DISCOVERED: Number(partial.PAGES_DISCOVERED || 0),
    PROPERTIES_EXTRACTED: Number(partial.PROPERTIES_EXTRACTED || 0),
    HIGH_MATCHES: Number(partial.HIGH_MATCHES || 0),
    FIELDS_WRITTEN: Number(partial.FIELDS_WRITTEN || 0),
    YIELD: Number(partial.YIELD || 0),
    ERROR_RATE: Number(partial.ERROR_RATE || 0),
    RATE_LIMIT: partial.RATE_LIMIT || "1_req_400ms",
    PLATEAU_STATUS: partial.PLATEAU_STATUS || null,
    NEXT_RETRY: partial.NEXT_RETRY || null,
    REQUESTS: Number(partial.REQUESTS || 0),
    ERRORS: Number(partial.ERRORS || 0),
    COUNTRIES_DONE: partial.COUNTRIES_DONE || [],
  };
}

export function loadSourceRegistry() {
  try {
    if (!fs.existsSync(SOURCE_REGISTRY_FP)) {
      return { version: SOURCE_REGISTRY_VERSION, sources: {} };
    }
    const raw = fs.readFileSync(SOURCE_REGISTRY_FP, "utf8").replace(/^\uFEFF/, "");
    const json = JSON.parse(raw);
    return {
      version: SOURCE_REGISTRY_VERSION,
      sources: json.sources || {},
      updated_at: json.updated_at || null,
    };
  } catch {
    return { version: SOURCE_REGISTRY_VERSION, sources: {} };
  }
}

export function saveSourceRegistry(registry) {
  fs.mkdirSync(path.dirname(SOURCE_REGISTRY_FP), { recursive: true });
  const next = {
    version: SOURCE_REGISTRY_VERSION,
    updated_at: new Date().toISOString(),
    sources: registry.sources || {},
  };
  fs.writeFileSync(SOURCE_REGISTRY_FP, JSON.stringify(next, null, 2));
  return next;
}

export function upsertSource(registry, partial) {
  const id = String(partial.SOURCE_ID || partial.id || "").trim();
  if (!id) return registry;
  const prev = registry.sources[id] || emptySourceEntry({ SOURCE_ID: id });
  registry.sources[id] = { ...prev, ...emptySourceEntry({ ...prev, ...partial }) };
  registry.sources[id].SOURCE_ID = id;
  return registry.sources[id];
}

export function bumpSourceStats(entry, delta = {}) {
  if (!entry) return entry;
  for (const [k, v] of Object.entries(delta)) {
    if (typeof entry[k] === "number") entry[k] += Number(v) || 0;
    else if (v != null) entry[k] = v;
  }
  const req = Math.max(1, Number(entry.REQUESTS || 0));
  entry.YIELD = Number(
    ((Number(entry.HIGH_MATCHES || 0) + Number(entry.FIELDS_WRITTEN || 0)) / req).toFixed(4)
  );
  entry.ERROR_RATE = Number((Number(entry.ERRORS || 0) / req).toFixed(4));
  if (entry.HIGH_MATCHES >= 3 && entry.YIELD >= 0.05) {
    entry.USAGE_STATUS = SOURCE_DISCOVERY_STATE.ACTIVE_HIGH_YIELD;
  } else if (entry.REQUESTS >= 20 && entry.HIGH_MATCHES === 0) {
    entry.USAGE_STATUS = SOURCE_DISCOVERY_STATE.ACTIVE_LOW_YIELD;
  }
  return entry;
}

export function markSourceState(entry, state, extra = {}) {
  if (!entry) return entry;
  entry.USAGE_STATUS = state;
  Object.assign(entry, extra);
  entry.LAST_CRAWLED = extra.LAST_CRAWLED || new Date().toISOString();
  return entry;
}

export function sourceIsRunnable(entry) {
  const st = entry?.USAGE_STATUS;
  return (
    !st ||
    st === SOURCE_DISCOVERY_STATE.UNEXPLORED ||
    st === SOURCE_DISCOVERY_STATE.DISCOVERING ||
    st === SOURCE_DISCOVERY_STATE.ACTIVE_HIGH_YIELD ||
    st === SOURCE_DISCOVERY_STATE.ACTIVE_LOW_YIELD ||
    st === SOURCE_DISCOVERY_STATE.RETRY_LATER
  );
}

export function topYieldSources(registry, n = 20) {
  return Object.values(registry.sources || {})
    .sort((a, b) => Number(b.HIGH_MATCHES || 0) - Number(a.HIGH_MATCHES || 0))
    .slice(0, n);
}
