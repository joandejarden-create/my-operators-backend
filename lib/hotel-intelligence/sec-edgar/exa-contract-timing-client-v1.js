/**
 * Exa client for contract-timing complementary discovery.
 * Reuses EXA_API_KEY / EXA_KEY; never logs the key.
 * Findings are leads until verified against authoritative documents.
 */

import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { PROVIDER_UNIT_COST_USD } from "./contract-timing-v2-budget-v1.js";

export const EXA_CONTRACT_TIMING_CLIENT_V1 = "exa-contract-timing-client-v1";
const EXA_SEARCH_URL = "https://api.exa.ai/search";
const EXA_CONTENTS_URL = "https://api.exa.ai/contents";

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

export function loadExaContractTimingConfig(env = process.env) {
  const apiKey = String(env.EXA_API_KEY || env.EXA_KEY || "").trim();
  return {
    enabled: Boolean(apiKey),
    apiKeyPresent: Boolean(apiKey),
    apiKeyLen: apiKey.length,
    cacheDir:
      String(env.EXA_CONTRACT_TIMING_CACHE_DIR || "").trim() ||
      path.join(process.cwd(), "data", "sec-edgar", "exa-contract-timing-cache"),
    timeoutMs: Math.max(5000, Number(env.EXA_TIMEOUT_MS || 20000) || 20000),
  };
}

export function createExaContractTimingClient(opts = {}) {
  const env = opts.env || process.env;
  const config = { ...loadExaContractTimingConfig(env), ...opts };
  const apiKey = String(env.EXA_API_KEY || env.EXA_KEY || "").trim();
  const budget = opts.budget || null;
  const stats = { requests: 0, cacheHits: 0, errors: 0, status429: 0, search: 0, contents: 0 };

  function ensureCache() {
    fs.mkdirSync(config.cacheDir, { recursive: true });
  }

  function cachePath(kind, payload) {
    const h = crypto.createHash("sha1").update(JSON.stringify({ kind, payload })).digest("hex");
    return path.join(config.cacheDir, `${kind}_${h}.json`);
  }

  function readCache(kind, payload) {
    if (opts.skipCache) return null;
    const p = cachePath(kind, payload);
    if (!fs.existsSync(p)) return null;
    try {
      const raw = JSON.parse(fs.readFileSync(p, "utf8"));
      stats.cacheHits += 1;
      return raw;
    } catch {
      return null;
    }
  }

  function writeCache(kind, payload, result) {
    if (opts.skipCacheWrite) return;
    ensureCache();
    fs.writeFileSync(
      cachePath(kind, payload),
      JSON.stringify({ cachedAt: Date.now(), kind, payload, result }, null, 2),
      "utf8",
    );
  }

  async function search(params = {}) {
    if (!apiKey) return { ok: false, error: "EXA_API_KEY_ABSENT", status: 0 };
    const body = {
      query: String(params.query || "").trim(),
      type: params.type || "lite",
      numResults: Math.min(10, Number(params.numResults || 5) || 5),
    };
    if (params.startPublishedDate) body.startPublishedDate = params.startPublishedDate;
    if (params.endPublishedDate) body.endPublishedDate = params.endPublishedDate;
    if (params.includeDomains) body.includeDomains = params.includeDomains;
    if (!body.query) return { ok: false, error: "QUERY_REQUIRED", status: 0 };

    const cached = readCache("search", body);
    if (cached?.result) return { ...cached.result, fromCache: true };

    const reserve = PROVIDER_UNIT_COST_USD.EXA_SEARCH_LITE;
    if (budget) {
      const gate = budget.canSpend(reserve);
      if (!gate.ok) return { ok: false, error: gate.error, status: 0, budget: gate };
    }

    const fp = budget?.fingerprint?.("EXA", "search", body);
    stats.requests += 1;
    stats.search += 1;
    const ac = new AbortController();
    const timer = setTimeout(() => ac.abort(), config.timeoutMs);
    try {
      const res = await fetch(EXA_SEARCH_URL, {
        method: "POST",
        headers: { "content-type": "application/json", "x-api-key": apiKey },
        body: JSON.stringify(body),
        signal: ac.signal,
      });
      clearTimeout(timer);
      const json = await res.json().catch(() => ({}));
      if (res.status === 429) {
        stats.status429 += 1;
        await sleep(2000);
        return { ok: false, status: 429, error: "EXA_RATE_LIMITED" };
      }
      if (!res.ok) {
        stats.errors += 1;
        return { ok: false, status: res.status, error: `EXA_HTTP_${res.status}` };
      }
      const reported = Number(json?.costDollars?.total);
      const usd = Number.isFinite(reported) ? reported : reserve;
      if (budget) {
        budget.record({
          provider: "EXA",
          operation: "search",
          usd,
          fingerprint: fp,
          note: `results=${(json.results || []).length}`,
        });
      }
      const result = {
        ok: true,
        status: 200,
        results: json.results || [],
        costDollars: json.costDollars || null,
        fromCache: false,
      };
      writeCache("search", body, result);
      return result;
    } catch (err) {
      clearTimeout(timer);
      stats.errors += 1;
      return { ok: false, status: 0, error: String(err.message || err).slice(0, 160) };
    }
  }

  async function contents(urls = [], params = {}) {
    if (!apiKey) return { ok: false, error: "EXA_API_KEY_ABSENT", status: 0 };
    const body = {
      urls: (urls || []).slice(0, 5),
      text: true,
      highlights: params.highlights === true,
    };
    if (!body.urls.length) return { ok: false, error: "URLS_REQUIRED", status: 0 };
    const cached = readCache("contents", body);
    if (cached?.result) return { ...cached.result, fromCache: true };

    const reserve = PROVIDER_UNIT_COST_USD.EXA_CONTENTS * body.urls.length;
    if (budget) {
      const gate = budget.canSpend(reserve);
      if (!gate.ok) return { ok: false, error: gate.error, status: 0, budget: gate };
    }
    const fp = budget?.fingerprint?.("EXA", "contents", body);
    stats.requests += 1;
    stats.contents += 1;
    const ac = new AbortController();
    const timer = setTimeout(() => ac.abort(), config.timeoutMs);
    try {
      const res = await fetch(EXA_CONTENTS_URL, {
        method: "POST",
        headers: { "content-type": "application/json", "x-api-key": apiKey },
        body: JSON.stringify(body),
        signal: ac.signal,
      });
      clearTimeout(timer);
      const json = await res.json().catch(() => ({}));
      if (!res.ok) {
        stats.errors += 1;
        return { ok: false, status: res.status, error: `EXA_HTTP_${res.status}` };
      }
      const reported = Number(json?.costDollars?.total);
      const usd = Number.isFinite(reported) ? reported : reserve;
      if (budget) {
        budget.record({
          provider: "EXA",
          operation: "contents",
          usd,
          fingerprint: fp,
          note: `urls=${body.urls.length}`,
        });
      }
      const result = {
        ok: true,
        status: 200,
        results: json.results || [],
        costDollars: json.costDollars || null,
        fromCache: false,
      };
      writeCache("contents", body, result);
      return result;
    } catch (err) {
      clearTimeout(timer);
      stats.errors += 1;
      return { ok: false, status: 0, error: String(err.message || err).slice(0, 160) };
    }
  }

  function normalizeHit(r, meta = {}) {
    return {
      provider: "EXA",
      title: r.title || null,
      url: r.url || null,
      publishedDate: r.publishedDate || r.published || null,
      score: r.score ?? null,
      snippet: r.text || r.highlights?.[0] || r.summary || null,
      queryFamily: meta.queryFamily || null,
      query: meta.query || null,
      discoveryChannel: "EXA_SEARCH",
      leadOnly: true,
    };
  }

  return {
    version: EXA_CONTRACT_TIMING_CLIENT_V1,
    config: { ...config, apiKey: undefined },
    stats: () => ({ ...stats }),
    search,
    contents,
    normalizeHit,
  };
}

export default {
  createExaContractTimingClient,
  loadExaContractTimingConfig,
  EXA_CONTRACT_TIMING_CLIENT_V1,
};
