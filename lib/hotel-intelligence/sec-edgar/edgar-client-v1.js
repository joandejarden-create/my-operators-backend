/**
 * Official SEC EDGAR HTTP client — fair access, rate limit, retries, cache.
 * No API key. User-Agent required by SEC.
 */

import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";

export const SEC_EDGAR_CLIENT_VERSION = "sec-edgar-client-v1";

const DEFAULT_BASE = "https://data.sec.gov";
const DEFAULT_WWW = "https://www.sec.gov";
const DEFAULT_UA = "Dealality Research joan@aohospitalityadvisors.com";
const DEFAULT_RATE = 2; // requests per second across this client
const DEFAULT_TIMEOUT_MS = 15000;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function sha1(s) {
  return crypto.createHash("sha1").update(String(s)).digest("hex");
}

export function loadSecEdgarConfig(env = process.env) {
  const enabled =
    String(env.SEC_EDGAR_ENABLED || "true").trim().toLowerCase() !== "false";
  return {
    enabled,
    baseUrl: String(env.SEC_EDGAR_BASE_URL || DEFAULT_BASE).replace(/\/$/, ""),
    wwwUrl: String(env.SEC_EDGAR_WWW_URL || DEFAULT_WWW).replace(/\/$/, ""),
    userAgent: String(env.SEC_EDGAR_USER_AGENT || DEFAULT_UA).trim(),
    rateLimitPerSec: Math.max(0.2, Number(env.SEC_EDGAR_RATE_LIMIT || DEFAULT_RATE) || DEFAULT_RATE),
    timeoutMs: Math.max(3000, Number(env.SEC_EDGAR_TIMEOUT_MS || DEFAULT_TIMEOUT_MS) || DEFAULT_TIMEOUT_MS),
    cacheDir:
      String(env.SEC_EDGAR_CACHE_DIR || "").trim() ||
      path.join(process.cwd(), "data", "sec-edgar", "cache"),
  };
}

export function createSecEdgarClient(opts = {}) {
  const config = { ...loadSecEdgarConfig(opts.env || process.env), ...opts };
  if (!config.userAgent || config.userAgent.length < 8) {
    throw new Error("SEC_EDGAR_USER_AGENT required (descriptive contact per SEC fair access)");
  }

  let nextAllowedAt = 0;
  const stats = {
    requests: 0,
    cacheHits: 0,
    retries: 0,
    errors: 0,
    status403: 0,
    status429: 0,
  };

  function ensureCacheDir() {
    fs.mkdirSync(config.cacheDir, { recursive: true });
  }

  function cachePath(url) {
    ensureCacheDir();
    return path.join(config.cacheDir, `${sha1(url)}.json`);
  }

  function readCache(url, maxAgeMs) {
    if (opts.skipCache) return null;
    const p = cachePath(url);
    if (!fs.existsSync(p)) return null;
    try {
      const raw = JSON.parse(fs.readFileSync(p, "utf8"));
      if (maxAgeMs != null && Date.now() - Number(raw.cachedAt || 0) > maxAgeMs) return null;
      stats.cacheHits += 1;
      return raw;
    } catch {
      return null;
    }
  }

  function writeCache(url, payload) {
    if (opts.skipCacheWrite) return;
    ensureCacheDir();
    const p = cachePath(url);
    fs.writeFileSync(
      p,
      JSON.stringify({
        url,
        cachedAt: Date.now(),
        status: payload.status,
        contentType: payload.contentType,
        body: payload.body,
        text: payload.text,
      }),
      "utf8",
    );
  }

  async function throttle() {
    const minGap = Math.ceil(1000 / config.rateLimitPerSec);
    const now = Date.now();
    const wait = Math.max(0, nextAllowedAt - now);
    nextAllowedAt = Math.max(nextAllowedAt, now) + minGap;
    if (wait > 0) await sleep(wait);
  }

  async function request(url, requestOpts = {}) {
    if (!config.enabled) {
      return { ok: false, error: "SEC_EDGAR_DISABLED", status: 0 };
    }
    const maxAgeMs = requestOpts.maxAgeMs;
    const cached = readCache(url, maxAgeMs);
    if (cached && (cached.body != null || cached.text != null)) {
      return {
        ok: cached.status >= 200 && cached.status < 300,
        status: cached.status,
        contentType: cached.contentType,
        body: cached.body,
        text: cached.text,
        fromCache: true,
        url,
      };
    }

    const maxAttempts = requestOpts.retries ?? 4;
    let lastErr = null;
    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      await throttle();
      stats.requests += 1;
      const ac = new AbortController();
      const timer = setTimeout(() => ac.abort(), config.timeoutMs);
      try {
        const res = await fetch(url, {
          method: "GET",
          headers: {
            "User-Agent": config.userAgent,
            Accept: requestOpts.accept || "application/json, text/html, */*",
            "Accept-Encoding": "gzip, deflate",
          },
          signal: ac.signal,
        });
        clearTimeout(timer);
        const contentType = res.headers.get("content-type") || "";
        if (res.status === 403) {
          stats.status403 += 1;
          stats.errors += 1;
          return {
            ok: false,
            status: 403,
            error: "SEC_FORBIDDEN",
            message:
              "HTTP 403 from SEC — check User-Agent fair-access compliance; do not bypass.",
            url,
          };
        }
        if (res.status === 429) {
          stats.status429 += 1;
          stats.retries += 1;
          const backoff = Math.min(60000, 1000 * 2 ** attempt + Math.random() * 250);
          await sleep(backoff);
          continue;
        }
        if (res.status >= 500) {
          stats.retries += 1;
          await sleep(Math.min(30000, 500 * 2 ** attempt));
          continue;
        }
        let text = null;
        let body = null;
        if (/json/i.test(contentType) || String(url).endsWith(".json")) {
          text = await res.text();
          try {
            body = JSON.parse(text);
          } catch {
            body = null;
          }
        } else {
          text = await res.text();
        }
        const payload = {
          ok: res.ok,
          status: res.status,
          contentType,
          body,
          text,
          fromCache: false,
          url,
        };
        if (res.ok) writeCache(url, payload);
        if (!res.ok) {
          stats.errors += 1;
          payload.error = `HTTP_${res.status}`;
        }
        return payload;
      } catch (err) {
        clearTimeout(timer);
        lastErr = err;
        stats.retries += 1;
        await sleep(Math.min(30000, 500 * 2 ** attempt));
      }
    }
    stats.errors += 1;
    return {
      ok: false,
      status: 0,
      error: "SEC_REQUEST_FAILED",
      message: String(lastErr?.message || lastErr || "request_failed"),
      url,
    };
  }

  async function getSubmissions(cik) {
    const padded = String(cik || "").replace(/\D/g, "").padStart(10, "0");
    const url = `${config.baseUrl}/submissions/CIK${padded}.json`;
    return request(url, { maxAgeMs: 6 * 60 * 60 * 1000 });
  }

  async function getCompanyFacts(cik) {
    const padded = String(cik || "").replace(/\D/g, "").padStart(10, "0");
    const url = `${config.baseUrl}/api/xbrl/companyfacts/CIK${padded}.json`;
    return request(url, { maxAgeMs: 24 * 60 * 60 * 1000 });
  }

  async function getCompanyTickers() {
    const url = `${config.wwwUrl}/files/company_tickers.json`;
    return request(url, { maxAgeMs: 24 * 60 * 60 * 1000 });
  }

  async function resolveCikByTicker(ticker) {
    const t = String(ticker || "").trim().toUpperCase();
    if (!t) return null;
    const res = await getCompanyTickers();
    if (!res.ok || !res.body) return null;
    const rows = Object.values(res.body);
    const hit = rows.find((r) => String(r.ticker || "").toUpperCase() === t);
    if (!hit) return null;
    return {
      ticker: hit.ticker,
      title: hit.title,
      cik: String(hit.cik_str).padStart(10, "0"),
    };
  }

  function filingDocumentUrl(cik, accession, primaryDocument) {
    const cikNum = String(cik).replace(/\D/g, "");
    const accNodash = String(accession).replace(/-/g, "");
    return `${config.wwwUrl}/Archives/edgar/data/${cikNum}/${accNodash}/${primaryDocument}`;
  }

  async function getFilingDocument(cik, accession, primaryDocument) {
    const url = filingDocumentUrl(cik, accession, primaryDocument);
    return request(url, {
      accept: "text/html, application/xhtml+xml, text/plain, */*",
      maxAgeMs: 30 * 24 * 60 * 60 * 1000,
    });
  }

  return {
    version: SEC_EDGAR_CLIENT_VERSION,
    config,
    stats: () => ({ ...stats }),
    request,
    getSubmissions,
    getCompanyFacts,
    getCompanyTickers,
    resolveCikByTicker,
    filingDocumentUrl,
    getFilingDocument,
  };
}

export default { createSecEdgarClient, loadSecEdgarConfig };
