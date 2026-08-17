/**
 * HBX / Hotelbeds Content API client (read-only helpers).
 *
 * Auth: Api-key + X-Signature = SHA256(apiKey + secret + unixSeconds)
 * Never log secret or full signature.
 */
import crypto from "node:crypto";

export const HBX_CONTENT_API_CLIENT_VERSION = "hbx-content-api-client-v1";

export function resolveHbxConfig(env = process.env) {
  const apiKey = String(env.HBX_API_KEY || "").trim();
  const apiSecret = String(env.HBX_API_SECRET || "").trim();
  const hbxEnv = String(env.HBX_ENV || "test").trim().toLowerCase() || "test";
  const apiBase =
    String(env.HBX_API_BASE_URL || "").trim() ||
    (hbxEnv === "live" || hbxEnv === "prod" || hbxEnv === "production"
      ? "https://api.hotelbeds.com"
      : "https://api.test.hotelbeds.com");
  const contentBase =
    String(env.HBX_CONTENT_API_BASE_URL || "").trim() ||
    `${apiBase.replace(/\/$/, "")}/hotel-content-api/1.0`;

  return {
    ok: Boolean(apiKey && apiSecret),
    missing: [
      !apiKey ? "HBX_API_KEY" : null,
      !apiSecret ? "HBX_API_SECRET" : null,
    ].filter(Boolean),
    apiKey,
    apiSecret,
    hbxEnv,
    apiBase: apiBase.replace(/\/$/, ""),
    contentBase: contentBase.replace(/\/$/, ""),
    writesEnabled: String(env.ENABLE_HBX_CENSUS_WRITES || "0").trim() === "1",
    insertsEnabled: String(env.ENABLE_HBX_INSERTS || "0").trim() === "1",
    apiKeyFingerprint: apiKey ? `${apiKey.slice(0, 4)}…(len=${apiKey.length})` : null,
  };
}

export function buildHbxSignature(apiKey, apiSecret, unixSeconds = Math.floor(Date.now() / 1000)) {
  const ts = Number(unixSeconds);
  const raw = `${apiKey}${apiSecret}${ts}`;
  const signature = crypto.createHash("sha256").update(raw).digest("hex");
  return {
    timestamp: ts,
    signature,
    signature_fingerprint: `${signature.slice(0, 8)}…(len=${signature.length})`,
  };
}

export function buildHbxHeaders(cfg, opts = {}) {
  const { signature, timestamp, signature_fingerprint } = buildHbxSignature(
    cfg.apiKey,
    cfg.apiSecret,
    opts.unixSeconds
  );
  return {
    headers: {
      Accept: "application/json",
      "Accept-Encoding": "gzip",
      "Api-key": cfg.apiKey,
      "X-Signature": signature,
      ...(opts.extraHeaders || {}),
    },
    auth_meta: {
      timestamp,
      signature_fingerprint,
      api_key_fingerprint: cfg.apiKeyFingerprint,
    },
  };
}

/**
 * Safe GET — never returns secret/signature in error payloads.
 */
export async function hbxFetchJson(url, cfg, opts = {}) {
  const timeoutMs = Number(opts.timeoutMs || process.env.HBX_FETCH_TIMEOUT_MS || 30000);
  const { headers, auth_meta } = buildHbxHeaders(cfg);
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  const started = Date.now();
  try {
    const res = await fetch(url, {
      method: "GET",
      headers,
      signal: controller.signal,
    });
    const text = await res.text();
    let json = null;
    try {
      json = text ? JSON.parse(text) : null;
    } catch {
      json = null;
    }
    return {
      ok: res.ok,
      status: res.status,
      url: redactUrl(url),
      elapsed_ms: Date.now() - started,
      auth_meta: {
        timestamp: auth_meta.timestamp,
        // Never return signature or key material in fetch results
        signature_hex_length: 64,
      },
      body: json,
      body_text_preview:
        !json && text
          ? String(text).slice(0, 240).replace(/\s+/g, " ")
          : null,
      error_code: extractHbxErrorCode(json),
      error_message: extractHbxErrorMessage(json, text),
      response_headers: pickRateLimitHeaders(res),
    };
  } catch (err) {
    return {
      ok: false,
      status: 0,
      url: redactUrl(url),
      elapsed_ms: Date.now() - started,
      auth_meta,
      body: null,
      body_text_preview: null,
      error_code: String(err?.name || "fetch_error"),
      error_message: String(err?.message || err).slice(0, 200),
    };
  } finally {
    clearTimeout(timer);
  }
}

function redactUrl(url) {
  try {
    const u = new URL(url);
    return `${u.origin}${u.pathname}${u.search}`;
  } catch {
    return String(url || "").split("?")[0];
  }
}

/** Hotelbeds often returns `error` as a plain string (e.g. "Quota exceeded"). */
export function extractHbxErrorMessage(json, text) {
  const err = json?.error;
  if (typeof err === "string" && err.trim()) return err.trim();
  if (err && typeof err === "object") {
    const msg = err.message || err.description || err.code;
    if (msg != null) return String(msg);
  }
  if (json?.message) return String(json.message);
  if (!json && text) return String(text).slice(0, 240).replace(/\s+/g, " ");
  return null;
}

export function extractHbxErrorCode(json) {
  const err = json?.error;
  if (err && typeof err === "object" && err.code != null) return String(err.code);
  if (typeof err === "string" && /quota/i.test(err)) return "QUOTA_EXCEEDED";
  return null;
}

function pickRateLimitHeaders(res) {
  const keys = [
    "retry-after",
    "x-ratelimit-limit",
    "x-ratelimit-remaining",
    "x-ratelimit-reset",
    "ratelimit-limit",
    "ratelimit-remaining",
    "ratelimit-reset",
    "x-request-id",
    "x-correlation-id",
  ];
  const out = {};
  for (const k of keys) {
    const v = res.headers.get(k);
    if (v) out[k] = v;
  }
  return out;
}

export function contentUrl(cfg, pathAndQuery) {
  const p = String(pathAndQuery || "").replace(/^\//, "");
  return `${cfg.contentBase}/${p}`;
}

export function apiUrl(cfg, pathAndQuery) {
  const p = String(pathAndQuery || "").replace(/^\//, "");
  return `${cfg.apiBase}/${p}`;
}
