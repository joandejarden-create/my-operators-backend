/**
 * Resolve Marriott hotel property cover/hero image URLs from official overview HTML.
 * Metadata-only — returns URL references, does not download binaries.
 */
import { parseNextDataFromHtml } from "../marriott-brand-directory-extract.js";

const REJECT_URL = /favicon|placeholder|1x1|pixel|spacer|icon|logo\.svg|apple-touch/i;
const COVER_KEY_RE =
  /^(hero|heroImage|heroUrl|primaryImage|primaryPhoto|coverImage|leadImage|featuredImage|ogImage|mainImage|imageUrl|thumbnailUrl|propertyImage)$/i;
const COVER_KEY_PARTIAL_RE = /hero|primary|cover|lead|featured|ogimage|mainimage/i;

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function decodeMetaContent(value) {
  return nz(value)
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'");
}

function isMarriottImageUrl(url) {
  if (!url || !/^https?:\/\//i.test(url)) return false;
  if (REJECT_URL.test(url)) return false;
  try {
    const host = new URL(url).hostname.toLowerCase();
    return host.endsWith("marriott.com");
  } catch {
    return false;
  }
}

function metaContent(html, property) {
  const re1 = new RegExp(
    `<meta[^>]+property=["']${property}["'][^>]+content=["']([^"']+)["']`,
    "i"
  );
  const re2 = new RegExp(
    `<meta[^>]+content=["']([^"']+)["'][^>]+property=["']${property}["']`,
    "i"
  );
  const m = html.match(re1) || html.match(re2);
  return m ? decodeMetaContent(m[1]) : "";
}

function walkNextDataForImages(obj, out, depth = 0, keyPath = "") {
  if (!obj || depth > 28) return;
  if (Array.isArray(obj)) {
    for (let i = 0; i < obj.length; i++) {
      walkNextDataForImages(obj[i], out, depth + 1, `${keyPath}[${i}]`);
    }
    return;
  }
  if (typeof obj !== "object") return;

  for (const [key, value] of Object.entries(obj)) {
    const path = keyPath ? `${keyPath}.${key}` : key;
    if (typeof value === "string" && isMarriottImageUrl(value)) {
      let priority = 10;
      let source = `next_data:${key}`;
      if (COVER_KEY_RE.test(key)) {
        priority = 2;
      } else if (COVER_KEY_PARTIAL_RE.test(key)) {
        priority = 4;
      } else if (key === "contentUrl") {
        priority = 5;
      }
      out.push({ url: value, source, priority, keyPath: path });
    } else if (value && typeof value === "object") {
      walkNextDataForImages(value, out, depth + 1, path);
    }
  }
}

/**
 * @param {string} html
 * @param {string} [pageUrl]
 * @returns {{ url: string, source: string, imageRole: 'cover' } | null}
 */
export function resolveMarriottCoverImageFromHtml(html, pageUrl = "") {
  const raw = String(html || "");
  if (!raw || /access denied/i.test(raw)) return null;

  /** @type {{ url: string, source: string, priority: number }[]} */
  const candidates = [];

  const og = metaContent(raw, "og:image");
  if (isMarriottImageUrl(og)) {
    candidates.push({ url: og, source: "og:image", priority: 1 });
  }

  const twitter = metaContent(raw, "twitter:image");
  if (isMarriottImageUrl(twitter)) {
    candidates.push({ url: twitter, source: "twitter:image", priority: 2 });
  }

  try {
    const data = parseNextDataFromHtml(raw);
    walkNextDataForImages(data, candidates);
  } catch {
    // __NEXT_DATA__ optional
  }

  const seen = new Set();
  const unique = candidates.filter((c) => {
    const base = c.url.replace(/[?#].*$/, "");
    if (seen.has(base)) return false;
    seen.add(base);
    return true;
  });

  unique.sort((a, b) => a.priority - b.priority);
  const best = unique[0];
  if (!best) return null;

  return {
    url: best.url,
    source: best.source,
    imageRole: "cover",
    sourcePageUrl: pageUrl || null,
    resolvedFrom: best.source,
  };
}
