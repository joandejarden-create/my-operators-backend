/**
 * Market Alerts RSS dedupe helpers.
 * Primary key remains URL hash (Airtable Dedupe ID); title soft-key catches cross-feed syndication.
 */
import axios from "axios";

const TRACKING_PARAMS = new Set([
  "utm_source",
  "utm_medium",
  "utm_campaign",
  "utm_content",
  "utm_term",
  "utm_id",
  "fbclid",
  "gclid",
  "mc_cid",
  "mc_eid",
  "ncid",
  "ref",
  "source",
]);

/** Canonicalize article URL for comparison / hashing. */
export function canonicalizeSourceUrl(raw) {
  const input = String(raw || "").trim();
  if (!input) return "";
  try {
    const u = new URL(input);
    for (const key of [...u.searchParams.keys()]) {
      if (TRACKING_PARAMS.has(key.toLowerCase()) || key.toLowerCase().startsWith("utm_")) {
        u.searchParams.delete(key);
      }
    }
    u.hash = "";
    const host = u.hostname.toLowerCase().replace(/^www\./, "");
    const path = (u.pathname || "/").replace(/\/+$/, "") || "";
    const search = u.searchParams.toString();
    return `https://${host}${path}${search ? `?${search}` : ""}`.toLowerCase();
  } catch {
    return input
      .toLowerCase()
      .replace(/^https?:\/\//, "")
      .replace(/^www\./, "")
      .replace(/\/+$/, "")
      .replace(/\?.*$/, "");
  }
}

/**
 * Soft title key for cross-feed duplicates
 * (same story on Hospitality Net vs Hotel Executive vs Google News).
 */
export function normalizeAlertTitle(title) {
  let t = String(title || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "");

  // Strip publisher suffixes (" - Hotel Dive") but keep edition/date tails
  // (" - Week Ending 7 August 2026") so weekly newsletters stay distinct.
  t = t.replace(/\s+[-–—|]\s+([a-z0-9 .,&'()]{2,80})$/i, (full, suffix) => {
    if (/\b(week ending|week of|newsletter|vol\.?\s*\d|part\s+\d)\b/i.test(suffix)) return full;
    if (
      /\b(january|february|march|april|may|june|july|august|september|october|november|december|\d{4})\b/i.test(
        suffix
      ) &&
      suffix.length >= 12
    ) {
      return full;
    }
    return "";
  });
  t = t.replace(/\s+\([^)]{2,60}\)\s*$/g, "");

  t = t
    .replace(/&[a-z#0-9]+;/gi, " ")
    .replace(/[''`´]/g, "")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\b(exclusive|breaking|update|updated)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  return t;
}

/** True when two titles are likely the same story. */
export function titlesLikelyDuplicate(a, b) {
  const na = normalizeAlertTitle(a);
  const nb = normalizeAlertTitle(b);
  if (!na || !nb) return false;
  if (na === nb) return true;
  if (na.length >= 40 && nb.length >= 40) {
    if (na.includes(nb) || nb.includes(na)) return true;
  }
  return false;
}

/**
 * Prefer a direct publisher URL over Google News redirect wrappers when possible.
 * @param {string} link
 * @param {string} [block] raw RSS item XML
 */
export function preferDirectArticleUrl(link, block = "") {
  const raw = String(link || "").trim();
  const xml = String(block || "");
  const isGoogleNews =
    /news\.google\.com/i.test(raw) || /news\.google\.com/i.test(xml);

  if (!isGoogleNews) return raw;

  const hrefInDesc =
    xml.match(
      /<description[^>]*>[\s\S]*?href=["'](https?:\/\/(?!news\.google\.com)[^"']+)["']/i
    )?.[1] || "";
  if (hrefInDesc) return hrefInDesc.trim();

  return raw;
}

/**
 * Follow redirects to resolve a Google News article wrapper to the publisher URL.
 * @param {string} url
 * @param {{ timeoutMs?: number }} [opts]
 */
export async function resolveGoogleNewsArticleUrl(url, opts = {}) {
  const target = String(url || "").trim();
  if (!target || !/news\.google\.com/i.test(target)) return target;

  const timeoutMs = opts.timeoutMs ?? 8000;
  try {
    const res = await axios.get(target, {
      timeout: timeoutMs,
      maxRedirects: 8,
      responseType: "text",
      headers: {
        "User-Agent": "DealCapture-MarketAlerts/1.1 (Hospitality news aggregator)",
        Accept: "text/html,application/xhtml+xml",
      },
      validateStatus: (s) => s >= 200 && s < 400,
    });
    const finalUrl = res.request?.res?.responseUrl || res.request?.responseURL || "";
    if (finalUrl && !/news\.google\.com/i.test(finalUrl)) return finalUrl;

    // Some Google News pages embed the destination in a meta refresh / JS url.
    const html = typeof res.data === "string" ? res.data : "";
    const meta =
      html.match(/<meta[^>]+http-equiv=["']refresh["'][^>]+content=["'][^"']*url=([^"']+)["']/i)?.[1] ||
      html.match(/data-n-head[^>]*>[\s\S]*?href=["'](https?:\/\/(?!news\.google\.com)[^"']+)["']/i)?.[1] ||
      "";
    if (meta && !/news\.google\.com/i.test(meta)) return meta.trim();
  } catch {
    // Keep original wrapper on failure.
  }
  return target;
}
