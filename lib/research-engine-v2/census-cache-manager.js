/**
 * Lightweight HTML / payload cache for Autopilot dry-runs.
 * Avoids re-fetching the same official page within a run (and across runs when TTL allows).
 */

import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";

const DEFAULT_TTL_MS = 6 * 60 * 60 * 1000; // 6h

function hashKey(url) {
  return crypto.createHash("sha256").update(String(url || "")).digest("hex").slice(0, 24);
}

/**
 * @param {string} cacheDir
 */
export function createCensusCacheManager(cacheDir, opts = {}) {
  const ttlMs = opts.ttlMs ?? DEFAULT_TTL_MS;
  const mem = new Map();

  function ensureDir() {
    fs.mkdirSync(cacheDir, { recursive: true });
  }

  function filePath(url) {
    return path.join(cacheDir, `${hashKey(url)}.json`);
  }

  /**
   * @param {string} url
   * @returns {{ hit: boolean, body?: string, meta?: object }|null}
   */
  function get(url) {
    const key = String(url || "").trim();
    if (!key) return null;
    if (mem.has(key)) {
      const v = mem.get(key);
      if (Date.now() - v.ts <= ttlMs) return { hit: true, body: v.body, meta: v.meta, source: "memory" };
      mem.delete(key);
    }
    try {
      const fp = filePath(key);
      if (!fs.existsSync(fp)) return { hit: false };
      const raw = JSON.parse(fs.readFileSync(fp, "utf8"));
      if (Date.now() - Number(raw.ts || 0) > ttlMs) return { hit: false, expired: true };
      mem.set(key, raw);
      return { hit: true, body: raw.body, meta: raw.meta, source: "disk" };
    } catch {
      return { hit: false, error: true };
    }
  }

  /**
   * @param {string} url
   * @param {string} body
   * @param {object} [meta]
   */
  function set(url, body, meta = {}) {
    const key = String(url || "").trim();
    if (!key) return;
    ensureDir();
    const entry = { ts: Date.now(), url: key, body: String(body || ""), meta };
    mem.set(key, entry);
    try {
      fs.writeFileSync(filePath(key), JSON.stringify(entry), "utf8");
    } catch {
      /* memory-only fallback */
    }
  }

  function stats() {
    return { memory_entries: mem.size, cache_dir: cacheDir, ttl_ms: ttlMs };
  }

  return { get, set, stats, hashKey };
}
