/**
 * Dealality SerpApi research cache — local durable cache for reproducibility.
 * Does not store API keys. Separates research cache from SerpApi provider cache.
 */

import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { redactSecrets } from "../providers/serpapi-google-hotels/client.js";

export const SERPAPI_CACHE_VERSION = "dealality-serpapi-research-cache-v1";

function defaultCacheRoot(repoRoot) {
  return path.join(repoRoot, "data/research-engine-v2/serpapi-research-cache");
}

function hashKey(parts) {
  return createHash("sha256").update(parts.filter((p) => p != null).join("|")).digest("hex").slice(0, 40);
}

/**
 * @param {{ q?: string, property_token?: string, request_type: string, property_identity_id?: string, gl?: string, check_in_date?: string, check_out_date?: string }} params
 */
export function cacheKey(params) {
  return hashKey([
    SERPAPI_CACHE_VERSION,
    params.request_type || "search",
    String(params.q || "").toLowerCase().trim(),
    params.property_token || "",
    params.property_identity_id || "",
    params.gl || "us",
    params.check_in_date || "",
    params.check_out_date || "",
  ]);
}

/**
 * @param {string} repoRoot
 * @param {{ ttlDays?: number }} [opts]
 */
export function createSerpApiResearchCache(repoRoot, opts = {}) {
  const root = defaultCacheRoot(repoRoot);
  const metaDir = path.join(root, "meta");
  const rawDir = path.join(root, "raw");
  fs.mkdirSync(metaDir, { recursive: true });
  fs.mkdirSync(rawDir, { recursive: true });
  const ttlMs = (opts.ttlDays ?? 30) * 24 * 60 * 60 * 1000;

  function metaPath(key) {
    return path.join(metaDir, `${key}.json`);
  }
  function rawPath(key) {
    return path.join(rawDir, `${key}.json`);
  }

  return {
    version: SERPAPI_CACHE_VERSION,
    root,

    get(params) {
      const key = cacheKey(params);
      const mp = metaPath(key);
      if (!fs.existsSync(mp)) return null;
      let meta;
      try {
        meta = JSON.parse(fs.readFileSync(mp, "utf8"));
      } catch {
        return null;
      }
      const age = Date.now() - new Date(meta.retrieved_at).getTime();
      if (age > ttlMs) {
        return { hit: false, expired: true, key, meta };
      }
      let raw = null;
      try {
        raw = JSON.parse(fs.readFileSync(rawPath(key), "utf8"));
      } catch {
        return { hit: false, corrupt: true, key, meta };
      }
      return { hit: true, key, meta, raw, from_dealality_cache: true };
    },

    set(params, payload) {
      const key = cacheKey(params);
      const retrieved_at = new Date().toISOString();
      const redacted = redactSecrets(payload);
      const response_hash = createHash("sha256")
        .update(JSON.stringify(redacted))
        .digest("hex")
        .slice(0, 24);
      const meta = {
        version: SERPAPI_CACHE_VERSION,
        key,
        retrieved_at,
        request_type: params.request_type || "search",
        q: params.q || null,
        property_token: params.property_token || null,
        property_identity_id: params.property_identity_id || null,
        response_hash,
        source_state: payload?.ok === false ? "FAILED" : "SUCCESS",
        match_confidence: params.match_confidence || null,
        eligible_fields: params.eligible_fields || [],
        raw_response_location: `raw/${key}.json`,
        expiry_policy: `ttl_days_${opts.ttlDays ?? 30}`,
        expires_at: new Date(Date.now() + ttlMs).toISOString(),
      };
      fs.writeFileSync(metaPath(key), JSON.stringify(meta, null, 2));
      fs.writeFileSync(rawPath(key), JSON.stringify(redacted, null, 2));
      return { key, meta };
    },

    stats() {
      const metas = fs.existsSync(metaDir) ? fs.readdirSync(metaDir).filter((f) => f.endsWith(".json")) : [];
      return { entries: metas.length, root };
    },
  };
}
