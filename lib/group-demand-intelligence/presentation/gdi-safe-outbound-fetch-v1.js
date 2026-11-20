/**
 * Controlled outbound HTTP for GDI research workflows only.
 *
 * Never use URL parsing alone as SSRF defense: resolve DNS, reject private /
 * loopback / link-local / cloud-metadata destinations, validate redirect hops,
 * cap response size, and enforce timeouts.
 *
 * Customer opportunity-detail rendering must not call this module.
 */
import dns from "node:dns/promises";
import net from "node:net";

export const GDI_SAFE_OUTBOUND_FETCH_V1 = "gdi-safe-outbound-fetch-v1";

const DEFAULT_TIMEOUT_MS = 8000;
const DEFAULT_MAX_BYTES = 350_000;
const DEFAULT_MAX_REDIRECTS = 3;

const BLOCKED_HOSTS = new Set([
  "localhost",
  "metadata.google.internal",
  "metadata.google.com",
  "instance-data",
]);

function text(v) {
  return String(v == null ? "" : v).trim();
}

export function isPrivateOrLocalIp(ip) {
  const value = text(ip).toLowerCase();
  if (!value) return true;
  if (net.isIPv4(value)) {
    const parts = value.split(".").map((x) => Number(x));
    const [a, b] = parts;
    if (a === 0 || a === 10 || a === 127) return true;
    if (a === 169 && b === 254) return true; // link-local + cloud metadata
    if (a === 172 && b >= 16 && b <= 31) return true;
    if (a === 192 && b === 168) return true;
    if (a === 100 && b >= 64 && b <= 127) return true; // CGNAT
    return false;
  }
  if (net.isIPv6(value)) {
    if (value === "::1") return true;
    if (value === "::") return true;
    if (value.startsWith("fc") || value.startsWith("fd")) return true;
    if (value.startsWith("fe80")) return true;
    if (value.startsWith("::ffff:")) {
      return isPrivateOrLocalIp(value.slice("::ffff:".length));
    }
    return false;
  }
  return true;
}

/**
 * @param {string} rawUrl
 * @param {{ skipDns?: boolean }} [opts]
 */
export async function validatePublicOutboundUrl(rawUrl, opts = {}) {
  const value = text(rawUrl);
  if (!value) return { ok: false, reason: "not_provided", url: "" };
  let parsed;
  try {
    parsed = new URL(value);
  } catch {
    return { ok: false, reason: "invalid_url", url: value };
  }
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
    return { ok: false, reason: "invalid_protocol", url: value };
  }
  const host = text(parsed.hostname).toLowerCase();
  if (!host) return { ok: false, reason: "invalid_url", url: value };
  if (BLOCKED_HOSTS.has(host) || host.endsWith(".local") || host.endsWith(".internal")) {
    return { ok: false, reason: "blocked_localhost", url: value };
  }
  if (net.isIP(host) && isPrivateOrLocalIp(host)) {
    return { ok: false, reason: "blocked_private_ip", url: value };
  }
  if (!opts.skipDns) {
    try {
      const records = await dns.lookup(host, { all: true });
      if (!records?.length) {
        return { ok: false, reason: "dns_lookup_failed", url: value };
      }
      if (records.some((r) => isPrivateOrLocalIp(r.address))) {
        return { ok: false, reason: "blocked_private_ip", url: value };
      }
    } catch {
      return { ok: false, reason: "dns_lookup_failed", url: value };
    }
  }
  return { ok: true, url: parsed.toString(), host };
}

/**
 * Fetch HTML with hop-by-hop redirect validation (no silent private redirect).
 *
 * @param {string} rawUrl
 * @param {{
 *   fetchImpl?: typeof fetch,
 *   timeoutMs?: number,
 *   maxBytes?: number,
 *   maxRedirects?: number,
 *   skipDns?: boolean,
 *   headers?: Record<string, string>,
 * }} [opts]
 */
export async function fetchPublicHtmlSafe(rawUrl, opts = {}) {
  const fetchImpl = opts.fetchImpl || fetch;
  const timeoutMs = Number(opts.timeoutMs || DEFAULT_TIMEOUT_MS);
  const maxBytes = Number(opts.maxBytes || DEFAULT_MAX_BYTES);
  const maxRedirects = Number(opts.maxRedirects || DEFAULT_MAX_REDIRECTS);

  let current = text(rawUrl);
  for (let hop = 0; hop <= maxRedirects; hop += 1) {
    const validated = await validatePublicOutboundUrl(current, {
      skipDns: opts.skipDns === true,
    });
    if (!validated.ok) {
      return {
        ok: false,
        reason: validated.reason,
        url: current,
        version: GDI_SAFE_OUTBOUND_FETCH_V1,
      };
    }

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const res = await fetchImpl(validated.url, {
        method: "GET",
        redirect: "manual",
        signal: controller.signal,
        headers: {
          "User-Agent": "DealalityGdiSafeOutbound/1.0",
          Accept: "text/html,application/xhtml+xml",
          ...(opts.headers || {}),
        },
      });
      clearTimeout(timer);

      if ([301, 302, 303, 307, 308].includes(res.status)) {
        const location = text(res.headers.get("location"));
        if (!location) {
          return {
            ok: false,
            reason: "redirect_missing_location",
            statusCode: res.status,
            url: validated.url,
            version: GDI_SAFE_OUTBOUND_FETCH_V1,
          };
        }
        current = new URL(location, validated.url).toString();
        continue;
      }

      if (!res.ok) {
        return {
          ok: false,
          reason: "http_error",
          statusCode: res.status,
          url: validated.url,
          version: GDI_SAFE_OUTBOUND_FETCH_V1,
        };
      }

      const buf = Buffer.from(await res.arrayBuffer());
      if (buf.length > maxBytes) {
        return {
          ok: false,
          reason: "response_too_large",
          statusCode: res.status,
          url: validated.url,
          bytes: buf.length,
          version: GDI_SAFE_OUTBOUND_FETCH_V1,
        };
      }
      return {
        ok: true,
        html: buf.toString("utf8"),
        statusCode: res.status,
        url: validated.url,
        bytes: buf.length,
        version: GDI_SAFE_OUTBOUND_FETCH_V1,
      };
    } catch (err) {
      clearTimeout(timer);
      if (err?.name === "AbortError") {
        return {
          ok: false,
          reason: "timeout",
          url: validated.url,
          version: GDI_SAFE_OUTBOUND_FETCH_V1,
        };
      }
      return {
        ok: false,
        reason: "fetch_failed",
        url: validated.url,
        error: err?.message || String(err),
        version: GDI_SAFE_OUTBOUND_FETCH_V1,
      };
    }
  }

  return {
    ok: false,
    reason: "too_many_redirects",
    url: current,
    version: GDI_SAFE_OUTBOUND_FETCH_V1,
  };
}
