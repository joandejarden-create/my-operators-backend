/**
 * Extract hotel assets from first-party owner portfolio HTML (P1.7).
 * Conservative: skip junk names; do not invent properties.
 */

import { isJunkBuildingName } from "../../../gtm-owner-target/owner-portfolio-audit.js";
import { classifyPortfolioRelationship } from "./classify-claim.js";

export const PORTFOLIO_EXTRACT_VERSION = "ownership-portfolio-extract-v1";

const HOTELISH =
  /\b(hotel|resort|inn|suites?|palace|hacienda|estancia|pousada| lodge|hyatt|marriott|hilton|westin|sheraton|ibis|novotel|fiesta inn|live aqua|krystal|fasano|one hotels?)\b/i;

const SKIP_LINE =
  /cookie|privacy|subscribe|newsletter|copyright|all rights reserved|linkedin|instagram|facebook|investor relations|press release|announces|sustainability report|desarrolla tu talento|leading hotel owner|\bportfolio\b|\bportafolio\b|^©/i;

function decodeEntities(s) {
  return String(s || "")
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&aacute;/gi, "á")
    .replace(/&eacute;/gi, "é")
    .replace(/&iacute;/gi, "í")
    .replace(/&oacute;/gi, "ó")
    .replace(/&uacute;/gi, "ú")
    .replace(/&ntilde;/gi, "ñ")
    .replace(/&uuml;/gi, "ü")
    .replace(/&#(\d+);/g, (_, n) => String.fromCharCode(Number(n)))
    .replace(/&#x([0-9a-f]+);/gi, (_, h) => String.fromCharCode(parseInt(h, 16)));
}

function stripHtml(html) {
  return decodeEntities(
    String(html || "")
      .replace(/<script[\s\S]*?<\/script>/gi, " ")
      .replace(/<style[\s\S]*?<\/style>/gi, " ")
      .replace(/<[^>]+>/g, "\n")
  )
    .replace(/\s+\n/g, "\n")
    .replace(/\n+/g, "\n")
    .replace(/[ \t]+/g, " ");
}

function parseJsonLdHotels(html) {
  const out = [];
  const re = /<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi;
  let m;
  while ((m = re.exec(String(html || ""))) !== null) {
    try {
      const json = JSON.parse(m[1]);
      const nodes = Array.isArray(json) ? json : json["@graph"] ? json["@graph"] : [json];
      for (const n of nodes) {
        const type = String(n["@type"] || "");
        if (!/hotel|lodging|resort/i.test(type)) continue;
        out.push({
          name: n.name || null,
          city: n.address?.addressLocality || n.address?.city || null,
          country: n.address?.addressCountry || null,
          address: typeof n.address === "string" ? n.address : n.address?.streetAddress || null,
          website: n.url || n.sameAs || null,
          brand: n.brand?.name || n.brand || null,
          source: "json_ld",
        });
      }
    } catch {
      /* ignore malformed ld+json */
    }
  }
  return out;
}

function extractAnchorHotels(html) {
  const out = [];
  const re = /<a\s[^>]*href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi;
  let m;
  while ((m = re.exec(String(html || ""))) !== null) {
    const href = m[1];
    const text = decodeEntities(
      String(m[2] || "")
        .replace(/<[^>]+>/g, " ")
        .replace(/\s+/g, " ")
        .trim()
    );
    if (!text || text.length < 4 || text.length > 80) continue;
    if (SKIP_LINE.test(text)) continue;
    // Require hotelish label — bare /portfolio hrefs produce nav junk.
    if (!HOTELISH.test(text)) continue;
    if (isJunkBuildingName(text)) continue;
    out.push({ name: text, website: href, source: "anchor" });
  }
  return out;
}

function extractLineHotels(text) {
  const out = [];
  for (const line of String(text || "").split("\n")) {
    const t = line.trim();
    if (t.length < 6 || t.length > 90) continue;
    if (SKIP_LINE.test(t)) continue;
    if (!HOTELISH.test(t)) continue;
    if (isJunkBuildingName(t)) continue;
    out.push({ name: t, source: "text_line" });
  }
  return out;
}

function inferCityCountry(name, pageText) {
  const cityCountry =
    String(name).match(/[—\-|]\s*([A-Za-zÁÉÍÓÚÑáéíóúñü .]{3,40})(?:,\s*([A-Za-z ]{3,40}))?$/);
  if (cityCountry) {
    return {
      city: cityCountry[1].trim(),
      country: cityCountry[2] ? cityCountry[2].trim() : null,
    };
  }
  return { city: null, country: null };
}

/**
 * @param {string} html
 * @param {{ pageUrl?: string, pageTextPrior?: string, relationshipPrior?: string }} [meta]
 */
export function extractPortfolioAssetsFromHtml(html, meta = {}) {
  const text = stripHtml(html);
  const pageClass = classifyPortfolioRelationship(
    `${meta.pageTextPrior || ""}\n${text.slice(0, 4000)}`,
    "",
    meta.relationshipPrior || null
  );

  const raw = [
    ...parseJsonLdHotels(html),
    ...extractAnchorHotels(html),
    ...extractLineHotels(text),
  ];

  const seen = new Set();
  const assets = [];
  for (const item of raw) {
    const name = String(item.name || "").replace(/\s+/g, " ").trim();
    if (!name || isJunkBuildingName(name)) continue;
    const key = name.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    const loc = inferCityCountry(name, text);
    const nearby = text.includes(name)
      ? text.slice(Math.max(0, text.indexOf(name) - 180), text.indexOf(name) + name.length + 180)
      : "";
    const classified = classifyPortfolioRelationship(
      text.slice(0, 2500),
      nearby,
      meta.relationshipPrior || pageClass.relationship_type
    );
    assets.push({
      name,
      city: item.city || loc.city,
      country: item.country || loc.country,
      address: item.address || null,
      website: item.website || null,
      brand: item.brand || null,
      source_url: meta.pageUrl || null,
      extract_source: item.source,
      classification: classified,
    });
  }

  return {
    page_classification: pageClass,
    assets: assets.slice(0, 80),
    raw_count: raw.length,
  };
}

export const PORTFOLIO_PAGE_PATHS = Object.freeze([
  "",
  "/portafolio",
  "/portfolio",
  "/hoteles",
  "/hotels",
  "/properties",
  "/nuestros-hoteles",
  "/nossos-hoteis",
  "/assets",
  "/propiedades",
]);
