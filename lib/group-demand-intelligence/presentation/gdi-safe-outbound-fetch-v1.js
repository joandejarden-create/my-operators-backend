/**
 * Controlled outbound HTTP for GDI research workflows only.
 *
 * Never use URL parsing alone as SSRF defense: resolve DNS, reject private /
 * loopback / link-local / cloud-metadata destinations, pin the resolved
 * destination for the TCP connection (DNS-rebinding defense), validate
 * redirect hops, enforce response-size limits while streaming (not after
 * full buffering), and enforce timeouts.
 *
 * Customer opportunity-detail rendering must not call this module.
 */
import dns from "node:dns/promises";
import http from "node:http";
import https from "node:https";
import net from "node:net";
import { URL } from "node:url";

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
 * Resolve host and return only public addresses (fail closed).
 * @param {string} host
 * @returns {Promise<{ ok: true, addresses: Array<{address:string,family:number}> } | { ok: false, reason: string }>}
 */
export async function resolvePublicAddresses(host) {
  const name = text(host).toLowerCase();
  if (!name) return { ok: false, reason: "invalid_url" };
  if (net.isIP(name)) {
    if (isPrivateOrLocalIp(name)) {
      return { ok: false, reason: "blocked_private_ip" };
    }
    return {
      ok: true,
      addresses: [{ address: name, family: net.isIPv6(name) ? 6 : 4 }],
    };
  }
  try {
    const records = await dns.lookup(name, { all: true });
    if (!records?.length) {
      return { ok: false, reason: "dns_lookup_failed" };
    }
    const publicRecords = records.filter((r) => !isPrivateOrLocalIp(r.address));
    if (!publicRecords.length) {
      return { ok: false, reason: "blocked_private_ip" };
    }
    if (records.some((r) => isPrivateOrLocalIp(r.address))) {
      // Mixed public+private A/AAAA → refuse (rebinding / dual-homed risk).
      return { ok: false, reason: "blocked_private_ip" };
    }
    return { ok: true, addresses: publicRecords };
  } catch {
    return { ok: false, reason: "dns_lookup_failed" };
  }
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

  let pinnedAddresses = null;
  if (!opts.skipDns) {
    const resolved = await resolvePublicAddresses(host);
    if (!resolved.ok) {
      return { ok: false, reason: resolved.reason, url: value };
    }
    pinnedAddresses = resolved.addresses;
  }

  return {
    ok: true,
    url: parsed.toString(),
    host,
    pinnedAddresses,
  };
}

/**
 * Stream response body with an early abort once maxBytes is exceeded.
 * @param {import('node:http').IncomingMessage} res
 * @param {number} maxBytes
 * @returns {Promise<{ ok: true, buffer: Buffer } | { ok: false, reason: string, bytes: number }>}
 */
export function readResponseBodyLimited(res, maxBytes) {
  return new Promise((resolve) => {
    const chunks = [];
    let total = 0;
    let settled = false;

    const finish = (result) => {
      if (settled) return;
      settled = true;
      res.removeAllListeners("data");
      res.removeAllListeners("end");
      res.removeAllListeners("error");
      // Stop consuming further bytes when oversize.
      try {
        res.destroy();
      } catch {
        /* ignore */
      }
      resolve(result);
    };

    res.on("data", (chunk) => {
      const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      total += buf.length;
      if (total > maxBytes) {
        finish({
          ok: false,
          reason: "response_too_large",
          bytes: total,
        });
        return;
      }
      chunks.push(buf);
    });
    res.on("end", () => {
      finish({ ok: true, buffer: Buffer.concat(chunks, total) });
    });
    res.on("error", (err) => {
      finish({
        ok: false,
        reason: "fetch_failed",
        bytes: total,
        error: err?.message || String(err),
      });
    });
  });
}

function pinnedLookup(pinnedAddresses) {
  const pins = Array.isArray(pinnedAddresses) ? pinnedAddresses.filter(Boolean) : [];
  return (hostname, options, callback) => {
    // Destination pinning: ignore re-resolution; always use validated public IP(s).
    // Node may call lookup(hostname, cb) or lookup(hostname, { all:true, family }, cb).
    const cb = typeof options === "function" ? options : callback;
    const opts = typeof options === "function" || !options ? {} : options;
    if (typeof cb !== "function" || !pins.length) {
      if (typeof cb === "function") {
        cb(new Error("pinned_lookup_unavailable"));
      }
      return;
    }
    if (opts.all) {
      cb(
        null,
        pins.map((a) => ({ address: a.address, family: a.family || 4 })),
      );
      return;
    }
    const wantFamily = opts.family || 0;
    const match =
      (wantFamily
        ? pins.find((a) => Number(a.family) === Number(wantFamily))
        : null) || pins[0];
    cb(null, match.address, match.family || 4);
  };
}

/**
 * Fetch HTML with hop-by-hop redirect validation, DNS destination pinning,
 * and streaming response-size enforcement.
 *
 * @param {string} rawUrl
 * @param {{
 *   fetchImpl?: typeof fetch,
 *   timeoutMs?: number,
 *   maxBytes?: number,
 *   maxRedirects?: number,
 *   skipDns?: boolean,
 *   headers?: Record<string, string>,
 *   httpGet?: typeof http.get,
 *   httpsGet?: typeof https.get,
 * }} [opts]
 */
export async function fetchPublicHtmlSafe(rawUrl, opts = {}) {
  const timeoutMs = Number(opts.timeoutMs || DEFAULT_TIMEOUT_MS);
  const maxBytes = Number(opts.maxBytes || DEFAULT_MAX_BYTES);
  const maxRedirects = Number(opts.maxRedirects || DEFAULT_MAX_REDIRECTS);
  const httpGet = opts.httpGet || http.get;
  const httpsGet = opts.httpsGet || https.get;

  // Legacy/test path: injectable fetchImpl without pinning (still validates URL
  // + streams via arrayBuffer only when fetchImpl is provided). Prefer native
  // pinned path in production research.
  if (opts.fetchImpl) {
    return fetchPublicHtmlSafeViaFetchImpl(rawUrl, opts);
  }

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

    const parsed = new URL(validated.url);
    const isHttps = parsed.protocol === "https:";
    const getter = isHttps ? httpsGet : httpGet;
    const pinned = validated.pinnedAddresses || null;

    const result = await new Promise((resolve) => {
      const req = getter(
        {
          protocol: parsed.protocol,
          hostname: parsed.hostname,
          port: parsed.port || (isHttps ? 443 : 80),
          path: `${parsed.pathname}${parsed.search}`,
          method: "GET",
          timeout: timeoutMs,
          headers: {
            Host: parsed.host,
            "User-Agent": "DealalityGdiSafeOutbound/1.0",
            Accept: "text/html,application/xhtml+xml",
            Connection: "close",
            ...(opts.headers || {}),
          },
          // DNS rebinding defense: connect only to previously validated public IPs.
          lookup: pinned && !opts.skipDns ? pinnedLookup(pinned) : undefined,
          // Do not follow redirects automatically — validate each hop.
        },
        async (res) => {
          const status = res.statusCode || 0;
          if ([301, 302, 303, 307, 308].includes(status)) {
            const location = text(res.headers.location);
            res.resume();
            if (!location) {
              resolve({
                ok: false,
                reason: "redirect_missing_location",
                statusCode: status,
                url: validated.url,
                version: GDI_SAFE_OUTBOUND_FETCH_V1,
              });
              return;
            }
            resolve({
              ok: false,
              reason: "__redirect__",
              location: new URL(location, validated.url).toString(),
              statusCode: status,
            });
            return;
          }

          if (status < 200 || status >= 300) {
            res.resume();
            resolve({
              ok: false,
              reason: "http_error",
              statusCode: status,
              url: validated.url,
              version: GDI_SAFE_OUTBOUND_FETCH_V1,
            });
            return;
          }

          // Early Content-Length rejection when declared oversize.
          const declared = Number(res.headers["content-length"] || 0);
          if (declared > maxBytes) {
            res.resume();
            resolve({
              ok: false,
              reason: "response_too_large",
              statusCode: status,
              url: validated.url,
              bytes: declared,
              version: GDI_SAFE_OUTBOUND_FETCH_V1,
            });
            return;
          }

          const body = await readResponseBodyLimited(res, maxBytes);
          if (!body.ok) {
            resolve({
              ok: false,
              reason: body.reason,
              statusCode: status,
              url: validated.url,
              bytes: body.bytes,
              error: body.error,
              version: GDI_SAFE_OUTBOUND_FETCH_V1,
            });
            return;
          }

          resolve({
            ok: true,
            html: body.buffer.toString("utf8"),
            statusCode: status,
            url: validated.url,
            bytes: body.buffer.length,
            pinnedAddress: pinned?.[0]?.address || null,
            version: GDI_SAFE_OUTBOUND_FETCH_V1,
          });
        },
      );

      req.on("timeout", () => {
        req.destroy();
        resolve({
          ok: false,
          reason: "timeout",
          url: validated.url,
          version: GDI_SAFE_OUTBOUND_FETCH_V1,
        });
      });
      req.on("error", (err) => {
        resolve({
          ok: false,
          reason: "fetch_failed",
          url: validated.url,
          error: err?.message || String(err),
          version: GDI_SAFE_OUTBOUND_FETCH_V1,
        });
      });
    });

    if (result.reason === "__redirect__") {
      current = result.location;
      continue;
    }
    return result;
  }

  return {
    ok: false,
    reason: "too_many_redirects",
    url: current,
    version: GDI_SAFE_OUTBOUND_FETCH_V1,
  };
}

/**
 * Test/compat path using injectable fetchImpl. Still validates each hop and
 * enforces size while streaming when body.getReader is available; otherwise
 * rejects oversized arrayBuffers after read (compat only).
 */
async function fetchPublicHtmlSafeViaFetchImpl(rawUrl, opts = {}) {
  const fetchImpl = opts.fetchImpl;
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

      const declared = Number(res.headers.get("content-length") || 0);
      if (declared > maxBytes) {
        return {
          ok: false,
          reason: "response_too_large",
          statusCode: res.status,
          url: validated.url,
          bytes: declared,
          version: GDI_SAFE_OUTBOUND_FETCH_V1,
        };
      }

      // Prefer streaming reader when available.
      if (res.body && typeof res.body.getReader === "function") {
        const reader = res.body.getReader();
        const chunks = [];
        let total = 0;
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          const buf = Buffer.from(value);
          total += buf.length;
          if (total > maxBytes) {
            try {
              await reader.cancel();
            } catch {
              /* ignore */
            }
            return {
              ok: false,
              reason: "response_too_large",
              statusCode: res.status,
              url: validated.url,
              bytes: total,
              version: GDI_SAFE_OUTBOUND_FETCH_V1,
            };
          }
          chunks.push(buf);
        }
        const buffer = Buffer.concat(chunks, total);
        return {
          ok: true,
          html: buffer.toString("utf8"),
          statusCode: res.status,
          url: validated.url,
          bytes: buffer.length,
          pinnedAddress: validated.pinnedAddresses?.[0]?.address || null,
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
        pinnedAddress: validated.pinnedAddresses?.[0]?.address || null,
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

export default {
  GDI_SAFE_OUTBOUND_FETCH_V1,
  isPrivateOrLocalIp,
  resolvePublicAddresses,
  validatePublicOutboundUrl,
  readResponseBodyLimited,
  fetchPublicHtmlSafe,
};
