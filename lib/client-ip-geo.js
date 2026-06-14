import geoip from "geoip-lite";

const countryNames =
  typeof Intl !== "undefined" && Intl.DisplayNames
    ? new Intl.DisplayNames(["en"], { type: "region" })
    : null;

function sanitizeGeoPart(value, maxLen = 64) {
  if (value == null) return null;
  const s = String(value).trim().slice(0, maxLen);
  return s || null;
}

export function countryLabel(code) {
  const c = sanitizeGeoPart(code, 8);
  if (!c) return null;
  try {
    return countryNames?.of(c) || c;
  } catch (_err) {
    return c;
  }
}

/**
 * Best-effort client IP behind Railway / reverse proxies.
 * @param {import('express').Request} req
 */
export function getClientIp(req) {
  const xff = req.headers["x-forwarded-for"];
  if (xff) {
    const first = String(xff).split(",")[0].trim();
    if (first) return normalizeIp(first);
  }
  const candidates = [
    req.headers["x-real-ip"],
    req.headers["cf-connecting-ip"],
    req.socket?.remoteAddress,
    req.connection?.remoteAddress,
  ];
  for (const raw of candidates) {
    if (!raw) continue;
    const ip = normalizeIp(String(raw).trim());
    if (ip) return ip;
  }
  return null;
}

function normalizeIp(ip) {
  if (!ip) return null;
  if (ip.startsWith("::ffff:")) return ip.slice(7);
  if (ip === "::1") return "127.0.0.1";
  return ip;
}

function isPrivateIp(ip) {
  if (!ip) return true;
  if (ip === "127.0.0.1" || ip === "localhost") return true;
  if (ip.startsWith("10.")) return true;
  if (ip.startsWith("192.168.")) return true;
  const m = /^172\.(\d+)\./.exec(ip);
  if (m) {
    const n = Number(m[1]);
    if (n >= 16 && n <= 31) return true;
  }
  return false;
}

/**
 * @param {import('express').Request} req
 * @returns {{ geoCountry: string|null, geoCountryName: string|null, geoRegion: string|null, geoCity: string|null, geoLabel: string|null }}
 */
export function resolveGeoFromRequest(req) {
  const cfCountry = sanitizeGeoPart(req.headers["cf-ipcountry"], 8);
  const cfCity = sanitizeGeoPart(req.headers["cf-ipcity"], 64);

  const ip = getClientIp(req);
  if (!ip || isPrivateIp(ip)) {
    if (cfCountry) {
      const countryName = countryLabel(cfCountry);
      const label = [cfCity, countryName].filter(Boolean).join(", ") || countryName;
      return {
        geoCountry: cfCountry,
        geoCountryName: countryName,
        geoRegion: null,
        geoCity: cfCity,
        geoLabel: label,
      };
    }
    return {
      geoCountry: null,
      geoCountryName: null,
      geoRegion: null,
      geoCity: null,
      geoLabel: null,
    };
  }

  const hit = geoip.lookup(ip);
  const geoCountry = sanitizeGeoPart(hit?.country || cfCountry, 8);
  const geoCountryName = countryLabel(geoCountry);
  const geoRegion = sanitizeGeoPart(hit?.region, 32);
  const geoCity = sanitizeGeoPart(hit?.city || cfCity, 64);

  const labelParts = [geoCity, geoRegion, geoCountryName].filter(Boolean);
  const seen = new Set();
  const deduped = labelParts.filter((p) => {
    const key = p.toLowerCase();
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });

  return {
    geoCountry,
    geoCountryName,
    geoRegion,
    geoCity,
    geoLabel: deduped.length ? deduped.join(", ") : geoCountryName,
  };
}
