/**
 * Packet 2.8C-2 — Query / URL / document research cache.
 * Avoid paying for the same SerpApi query or document fetch twice in-process.
 */

import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../../../../");
const CACHE_DIR = path.join(ROOT, "data/hotel-intelligence/native-langchain/cache");

export const RESEARCH_CACHE_VERSION = "research-cache-v1";

function ensure() {
  fs.mkdirSync(CACHE_DIR, { recursive: true });
}

function hashKey(kind, value) {
  return crypto.createHash("sha256").update(`${kind}:${value}`).digest("hex").slice(0, 24);
}

function fileFor(kind, key) {
  return path.join(CACHE_DIR, `${kind}_${hashKey(kind, key)}.json`);
}

function readCache(kind, key, maxAgeMs) {
  ensure();
  const f = fileFor(kind, key);
  if (!fs.existsSync(f)) return null;
  try {
    const row = JSON.parse(fs.readFileSync(f, "utf8"));
    if (maxAgeMs && Date.now() - Date.parse(row.cached_at) > maxAgeMs) return null;
    return row.payload;
  } catch {
    return null;
  }
}

function writeCache(kind, key, payload) {
  ensure();
  fs.writeFileSync(
    fileFor(kind, key),
    JSON.stringify({ version: RESEARCH_CACHE_VERSION, kind, key, cached_at: new Date().toISOString(), payload }, null, 2)
  );
}

export function createResearchCache({ maxAgeMs = 1000 * 60 * 60 * 24 } = {}) {
  const stats = {
    query_hits: 0,
    query_misses: 0,
    url_hits: 0,
    url_misses: 0,
    cost_avoided_search_credits: 0,
    cost_avoided_doc_fetches: 0,
  };

  return {
    version: RESEARCH_CACHE_VERSION,
    stats,
    getQuery(query) {
      const hit = readCache("query", String(query).trim().toLowerCase(), maxAgeMs);
      if (hit) {
        stats.query_hits += 1;
        stats.cost_avoided_search_credits += 1;
        return hit;
      }
      stats.query_misses += 1;
      return null;
    },
    setQuery(query, payload) {
      writeCache("query", String(query).trim().toLowerCase(), payload);
    },
    getUrl(url) {
      const hit = readCache("url", String(url).trim(), maxAgeMs);
      if (hit) {
        stats.url_hits += 1;
        stats.cost_avoided_doc_fetches += 1;
        return hit;
      }
      stats.url_misses += 1;
      return null;
    },
    setUrl(url, payload) {
      writeCache("url", String(url).trim(), payload);
    },
    summarize() {
      return { ...stats, version: RESEARCH_CACHE_VERSION };
    },
  };
}
