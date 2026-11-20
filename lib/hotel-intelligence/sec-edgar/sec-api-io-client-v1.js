/**
 * SEC-API.io client — Full-Text Search + Filing Query.
 * Auth via SEC_API_IO_KEY (Authorization header, never logged).
 * Does not replace free official SEC EDGAR client.
 */

import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import {
  PROVIDER_UNIT_COST_USD,
} from "./contract-timing-v2-budget-v1.js";

export const SEC_API_IO_CLIENT_V1 = "sec-api-io-client-v1";
const FULL_TEXT_URL = "https://api.sec-api.io/full-text-search";
const FILING_QUERY_URL = "https://api.sec-api.io";

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

export function loadSecApiIoConfig(env = process.env) {
  const apiKey = String(env.SEC_API_IO_KEY || "").trim();
  return {
    enabled: Boolean(apiKey),
    apiKeyPresent: Boolean(apiKey),
    apiKeyLen: apiKey.length,
    cacheDir:
      String(env.SEC_API_IO_CACHE_DIR || "").trim() ||
      path.join(process.cwd(), "data", "sec-edgar", "sec-api-io-cache"),
    timeoutMs: Math.max(5000, Number(env.SEC_API_IO_TIMEOUT_MS || 30000) || 30000),
  };
}

export function createSecApiIoClient(opts = {}) {
  const env = opts.env || process.env;
  const config = { ...loadSecApiIoConfig(env), ...opts };
  const apiKey = String(env.SEC_API_IO_KEY || "").trim();
  const budget = opts.budget || null;
  const stats = {
    requests: 0,
    cacheHits: 0,
    errors: 0,
    status403: 0,
    status429: 0,
    fullText: 0,
    filingQuery: 0,
  };

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

  async function postJson(url, body, kind) {
    if (!apiKey) {
      return { ok: false, error: "SEC_API_IO_KEY_ABSENT", status: 0 };
    }
    const cached = readCache(kind, body);
    if (cached?.result) {
      return { ...cached.result, fromCache: true };
    }

    const fp = budget?.fingerprint?.("SEC_API_IO", kind, body);
    if (budget && fp && budget.hasFingerprint(fp) && opts.preferFingerprintSkip) {
      return { ok: false, error: "FINGERPRINT_SKIP", status: 0, fromCache: true };
    }

    const unitCost =
      kind === "full_text"
        ? PROVIDER_UNIT_COST_USD.SEC_API_IO_FULL_TEXT
        : PROVIDER_UNIT_COST_USD.SEC_API_IO_FILING_QUERY;
    if (budget) {
      const gate = budget.canSpend(unitCost);
      if (!gate.ok) return { ok: false, error: gate.error, status: 0, budget: gate };
    }

    const maxAttempts = 4;
    let last = null;
    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      stats.requests += 1;
      if (kind === "full_text") stats.fullText += 1;
      else stats.filingQuery += 1;
      const ac = new AbortController();
      const timer = setTimeout(() => ac.abort(), config.timeoutMs);
      try {
        const res = await fetch(url, {
          method: "POST",
          headers: {
            "content-type": "application/json",
            Authorization: apiKey,
          },
          body: JSON.stringify(body),
          signal: ac.signal,
        });
        clearTimeout(timer);
        const json = await res.json().catch(() => ({}));
        if (res.status === 403) {
          stats.status403 += 1;
          stats.errors += 1;
          return { ok: false, status: 403, error: "SEC_API_IO_FORBIDDEN", body: { message: json?.message || json?.error || null } };
        }
        if (res.status === 429) {
          stats.status429 += 1;
          await sleep(Math.min(30000, 1000 * 2 ** attempt));
          continue;
        }
        if (res.status >= 500) {
          await sleep(Math.min(20000, 500 * 2 ** attempt));
          continue;
        }
        const result = {
          ok: res.ok,
          status: res.status,
          body: json,
          filings: Array.isArray(json.filings) ? json.filings : [],
          total: json.total || null,
          fromCache: false,
        };
        if (!res.ok) {
          stats.errors += 1;
          result.error = json.error || json.message || `HTTP_${res.status}`;
        } else {
          writeCache(kind, body, result);
          if (budget) {
            budget.record({
              provider: "SEC_API_IO",
              operation: kind,
              usd: unitCost,
              fingerprint: fp,
              note: `status=${res.status};filings=${result.filings.length}`,
            });
          }
        }
        return result;
      } catch (err) {
        clearTimeout(timer);
        last = err;
        await sleep(Math.min(20000, 500 * 2 ** attempt));
      }
    }
    stats.errors += 1;
    return {
      ok: false,
      status: 0,
      error: "SEC_API_IO_REQUEST_FAILED",
      message: String(last?.message || last || "failed"),
    };
  }

  /**
   * Full-text search (indexed since ~2001). page is 1-based string; 100 results/page.
   */
  async function fullTextSearch(params = {}) {
    const body = {
      query: String(params.query || "").trim(),
      page: String(params.page || "1"),
    };
    if (params.startDate) body.startDate = params.startDate;
    if (params.endDate) body.endDate = params.endDate;
    if (params.formTypes) body.formTypes = params.formTypes;
    if (params.ciks) body.ciks = params.ciks;
    if (!body.query) return { ok: false, error: "QUERY_REQUIRED", status: 0 };
    return postJson(FULL_TEXT_URL, body, "full_text");
  }

  /**
   * Filing Query API (Lucene metadata search).
   */
  async function filingQuery(params = {}) {
    const body = {
      query: String(params.query || "").trim(),
      from: String(params.from || "0"),
      size: String(Math.min(50, Number(params.size || 10) || 10)),
    };
    if (params.sort) body.sort = params.sort;
    else body.sort = [{ filedAt: { order: "desc" } }];
    if (!body.query) return { ok: false, error: "QUERY_REQUIRED", status: 0 };
    return postJson(FILING_QUERY_URL, body, "filing_query");
  }

  function normalizeHit(f, meta = {}) {
    const accession = f.accessionNo || f.accessionNumber || null;
    const cik = f.cik != null ? String(f.cik).padStart(10, "0") : null;
    return {
      provider: "SEC_API_IO",
      accessionNumber: accession,
      cik,
      companyName: f.companyNameLong || f.companyName || f.entityName || null,
      ticker: f.ticker || null,
      formType: f.formType || f.form || null,
      filedAt: f.filedAt || f.filingDate || null,
      description: f.description || f.type || null,
      filingUrl: f.filingUrl || f.linkToFilingDetails || f.linkToHtml || null,
      documentType: f.type || null,
      queryFamily: meta.queryFamily || null,
      query: meta.query || null,
      discoveryChannel: meta.channel || "SEC_API_IO_FULL_TEXT",
    };
  }

  return {
    version: SEC_API_IO_CLIENT_V1,
    config: { ...config, apiKey: undefined },
    stats: () => ({ ...stats }),
    fullTextSearch,
    filingQuery,
    normalizeHit,
  };
}

export default { createSecApiIoClient, loadSecApiIoConfig, SEC_API_IO_CLIENT_V1 };
