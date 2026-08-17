/**
 * Property Name cleanup extractor — detect marketing/tagline names; extract clean hotel names.
 * Official HTML / JSON-LD only. No Webhound.
 */

export const PROPERTY_NAME_CLEANUP_EXTRACTOR_VERSION =
  "production-census-property-name-cleanup-extractor-v1";

const MARKETING_START = /^(welcome\s+to|book\s+now|discover\s+|experience\s+|stay\s+at|explore\s+|find\s+your|your\s+home\s+away|check\s+in\s+to)\b/i;
const MARKETING_PHRASE =
  /\b(where\s+the\s+essentials|done\s+right\.?\s*every\s+time|book\s+direct|best\s+rate\s+guarantee|official\s+site|click\s+here|learn\s+more|sign\s+up|subscribe)\b/i;
const SEO_BOILERPLATE =
  /\b(home\s*page|official\s+website|hotel\s+deals|cheap\s+hotels|book\s+online|reservations?)\b/i;
const CTA =
  /\b(book\s+now|reserve\s+now|get\s+rates|view\s+deals|shop\s+now)\b/i;

/**
 * @param {string} name
 */
export function classifyPropertyNameProblems(name) {
  const raw = String(name || "").trim();
  const reasons = [];
  if (!raw) {
    return { malformed: false, reasons: ["blank"], severity: "none" };
  }
  if (MARKETING_START.test(raw)) reasons.push("starts_with_marketing_intro");
  if (MARKETING_PHRASE.test(raw)) reasons.push("contains_marketing_phrase");
  if (SEO_BOILERPLATE.test(raw)) reasons.push("seo_or_homepage_boilerplate");
  if (CTA.test(raw)) reasons.push("booking_cta");
  if (raw.length >= 90) reasons.push("overly_long");
  if (raw.length >= 60 && /[,!]{2,}|\.\s+[A-Z]/.test(raw)) reasons.push("multi_sentence_marketing");
  if (/^\s*https?:\/\//i.test(raw)) reasons.push("url_as_name");
  if (/^(hotel|hotels|property|properties)$/i.test(raw)) reasons.push("generic_only");

  const severity =
    reasons.length === 0
      ? "none"
      : reasons.includes("starts_with_marketing_intro") ||
          reasons.includes("contains_marketing_phrase") ||
          reasons.includes("booking_cta")
        ? "high"
        : "medium";

  return {
    malformed: reasons.length > 0,
    reasons,
    severity,
    length: raw.length,
  };
}

/**
 * Normalize a candidate hotel name for comparison.
 * @param {string} name
 */
export function normalizeHotelName(name) {
  return String(name || "")
    .replace(/\s+/g, " ")
    .replace(/[|·•].*$/, "")
    .replace(/\s*[-–—]\s*(official|home|hotel\s+website).*$/i, "")
    .replace(/\s*,?\s*a\s+member\s+of\s+.+$/i, "")
    .trim();
}

/**
 * Prefer more property-specific names (brand + place > marketing blob).
 * @param {string} current
 * @param {string} proposed
 */
export function isMoreSpecificPropertyName(current, proposed) {
  const c = normalizeHotelName(current);
  const p = normalizeHotelName(proposed);
  if (!p || p.length < 4) return false;
  if (p.toLowerCase() === c.toLowerCase()) return false;
  const cProb = classifyPropertyNameProblems(c);
  const pProb = classifyPropertyNameProblems(p);
  if (pProb.malformed) return false;
  if (cProb.malformed && !pProb.malformed) return true;
  // Prefer shorter clean names over long marketing; prefer names with location tokens
  if (p.length < c.length && p.length >= 8 && cProb.malformed) return true;
  if (!cProb.malformed && p.length < 8) return false;
  return !cProb.malformed ? false : true;
}

function decodeHtmlEntities(s) {
  return String(s || "")
    .replace(/&amp;/gi, "&")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;|&apos;/gi, "'")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&#(\d+);/g, (_, n) => String.fromCharCode(Number(n)))
    .replace(/&nbsp;/gi, " ");
}

function stripTags(html) {
  return decodeHtmlEntities(String(html || "").replace(/<[^>]+>/g, " ")).replace(/\s+/g, " ").trim();
}

/**
 * Extract hotel name candidates from official property HTML.
 * @param {string} html
 * @param {{ url?: string, brand?: string, city?: string, propertyName?: string }} [opts]
 */
export function extractPropertyNamesFromOfficialHtml(html, opts = {}) {
  const text = String(html || "");
  const hits = [];

  const push = (name, method, confidenceBoost = 0) => {
    const cleaned = normalizeHotelName(stripTags(name));
    if (!cleaned || cleaned.length < 4 || cleaned.length > 120) return;
    const problems = classifyPropertyNameProblems(cleaned);
    if (problems.malformed && problems.severity === "high") return;
    hits.push({
      name: cleaned,
      method,
      confidence_boost: confidenceBoost,
      problems,
    });
  };

  // JSON-LD Hotel / LodgingBusiness
  const ldBlocks = [...text.matchAll(/<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi)];
  for (const m of ldBlocks) {
    try {
      const json = JSON.parse(m[1]);
      const nodes = Array.isArray(json) ? json : json["@graph"] ? json["@graph"] : [json];
      for (const node of nodes) {
        const type = String(node["@type"] || "");
        if (/Hotel|LodgingBusiness|Resort/i.test(type) && node.name) {
          push(node.name, "json_ld_hotel_name", 2);
        }
      }
    } catch {
      /* ignore bad json-ld */
    }
  }

  const og = text.match(/<meta[^>]+property=["']og:title["'][^>]+content=["']([^"']+)["']/i)
    || text.match(/<meta[^>]+content=["']([^"']+)["'][^>]+property=["']og:title["']/i);
  if (og?.[1]) push(og[1], "og_title", 1);

  const title = text.match(/<title[^>]*>([\s\S]*?)<\/title>/i);
  if (title?.[1]) push(title[1], "html_title", 0);

  const h1 = text.match(/<h1[^>]*>([\s\S]*?)<\/h1>/i);
  if (h1?.[1]) push(h1[1], "h1", 1);

  // IHG / Marriott style hotel name in data attributes
  const dataName = text.match(/data-hotel-name=["']([^"']+)["']/i)
    || text.match(/"hotelName"\s*:\s*"([^"]+)"/i)
    || text.match(/"propertyName"\s*:\s*"([^"]+)"/i);
  if (dataName?.[1]) push(dataName[1], "page_data_hotel_name", 2);

  // Deduplicate by normalized name
  const byKey = new Map();
  for (const h of hits) {
    const key = h.name.toLowerCase();
    const prev = byKey.get(key);
    if (!prev || (h.confidence_boost || 0) > (prev.confidence_boost || 0)) byKey.set(key, h);
  }
  return {
    version: PROPERTY_NAME_CLEANUP_EXTRACTOR_VERSION,
    hits: [...byKey.values()],
    url: opts.url || null,
  };
}

/**
 * Select best clean name vs current Census name + brand/city context.
 * @param {object[]} hits
 * @param {{ currentName?: string, brand?: string, city?: string }} ctx
 */
export function selectBestPropertyNameHit(hits = [], ctx = {}) {
  const current = String(ctx.currentName || "").trim();
  const brand = String(ctx.brand || "").trim().toLowerCase();
  const city = String(ctx.city || "").trim().toLowerCase();
  const usable = hits.filter((h) => !classifyPropertyNameProblems(h.name).malformed || classifyPropertyNameProblems(h.name).severity === "none");

  if (!usable.length) return { hit: null, confidence: "Low", reason: "no_clean_candidate" };

  const scored = usable.map((h) => {
    let score = (h.confidence_boost || 0) * 10;
    const n = h.name.toLowerCase();
    if (brand && n.includes(brand.split(/\s+/)[0])) score += 5;
    if (city && city.length >= 3 && n.includes(city)) score += 8;
    if (isMoreSpecificPropertyName(current, h.name)) score += 12;
    if (/json_ld|page_data|h1/.test(h.method)) score += 3;
    if (h.name.length > 70) score -= 5;
    return { ...h, score };
  });
  scored.sort((a, b) => b.score - a.score);
  const best = scored[0];
  const second = scored[1];

  if (second && Math.abs(second.score - best.score) <= 2 && second.name.toLowerCase() !== best.name.toLowerCase()) {
    return {
      hit: best,
      confidence: "Hold",
      reason: "multiple_candidate_names",
      alternatives: scored.slice(0, 3).map((s) => s.name),
    };
  }

  if (!isMoreSpecificPropertyName(current, best.name) && !classifyPropertyNameProblems(current).malformed) {
    return { hit: null, confidence: "Low", reason: "current_name_already_valid" };
  }

  const currentProblems = classifyPropertyNameProblems(current);
  if (!currentProblems.malformed) {
    return { hit: null, confidence: "Low", reason: "name_appears_valid_already" };
  }

  // Brand + city match → High; otherwise Medium if method strong
  const nameLower = best.name.toLowerCase();
  const brandOk = !brand || nameLower.includes(brand.split(/\s+/)[0]) || brand.includes("avid");
  const cityOk = !city || city.length < 3 || nameLower.includes(city);
  const strongMethod = /json_ld_hotel_name|page_data_hotel_name|h1|og_title/.test(best.method);

  if (currentProblems.severity === "high" && strongMethod && brandOk) {
    return {
      hit: best,
      confidence: cityOk || /avid hotels/i.test(best.name) ? "High" : "Medium",
      reason: "official_clean_name_replaces_marketing_phrase",
    };
  }

  if (strongMethod) {
    return {
      hit: best,
      confidence: "Medium",
      reason: "likely_correct_but_formatting_ambiguous",
    };
  }

  return {
    hit: best,
    confidence: "Low",
    reason: "fuzzy_name_extraction",
  };
}
