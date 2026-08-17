/**
 * Official-page description / amenity extraction for Hotel Property Census.
 * No Webhound. Blank beats fake. AI summary only when grounded in Source Text.
 */

const JSON_LD_RE =
  /<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi;

const BOOKING_BOILERPLATE_RE =
  /best price guarantee|read guest reviews and book|book your stay|kids stay and eat free|official site of .{0,80}\. read guest/i;

const MIN_NARRATIVE_LEN = 80;
const MIN_AMENITY_COUNT = 2;

/**
 * @typedef {{
 *   text: string,
 *   method: string,
 *   confidence: 'High'|'Medium'|'Low',
 *   rejected?: boolean,
 *   reject_reason?: string
 * }} DescHit
 */

function unescapeJson(s) {
  return String(s || "")
    .replace(/\\u([0-9a-fA-F]{4})/g, (_, h) => String.fromCharCode(parseInt(h, 16)))
    .replace(/\\n/g, "\n")
    .replace(/\\"/g, '"')
    .replace(/\\\\/g, "\\");
}

function cleanText(s) {
  return String(s || "")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/&#34;/g, '"')
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/\s+/g, " ")
    .trim();
}

function parseJsonSafe(raw) {
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function pickMeta(html, nameOrProp) {
  const re1 = new RegExp(
    `(?:property|name)=["']${nameOrProp}["'][^>]*content=["']([^"']+)["']`,
    "i"
  );
  const re2 = new RegExp(
    `content=["']([^"']+)["'][^>]*(?:property|name)=["']${nameOrProp}["']`,
    "i"
  );
  return cleanText((html.match(re1) || html.match(re2))?.[1] || "");
}

/**
 * Reject marketing/booking CTAs that are not real property descriptions.
 * @param {string} text
 */
export function isBookingBoilerplate(text) {
  const t = cleanText(text);
  if (!t) return true;
  if (BOOKING_BOILERPLATE_RE.test(t) && t.length < 220) return true;
  if (/^official site of /i.test(t) && /book your stay/i.test(t)) return true;
  return false;
}

/**
 * Score narrative description quality.
 * @param {string} text
 * @returns {{ ok: boolean, confidence: 'High'|'Medium'|'Low', reason?: string }}
 */
export function assessDescriptionQuality(text) {
  const t = cleanText(text);
  if (!t || t.length < MIN_NARRATIVE_LEN) {
    return { ok: false, confidence: "Low", reason: "too_short" };
  }
  if (isBookingBoilerplate(t)) {
    return { ok: false, confidence: "Low", reason: "booking_boilerplate" };
  }
  // Prefer text with place/facility substance
  const substance =
    /located|offers|features|restaurant|pool|suite|resort|downtown|airport|beach|meeting|spa|guest room|walk to|minutes from/i.test(
      t
    );
  if (substance && t.length >= 120) {
    return { ok: true, confidence: "High" };
  }
  if (substance || t.length >= 160) {
    return { ok: true, confidence: "Medium" };
  }
  return { ok: false, confidence: "Low", reason: "insufficient_substance" };
}

function walkJsonLdDescriptions(node, hits) {
  if (!node || typeof node !== "object") return;
  if (Array.isArray(node)) {
    for (const item of node) walkJsonLdDescriptions(item, hits);
    return;
  }
  const types = []
    .concat(node["@type"] || [])
    .map((t) => String(t).toLowerCase());
  const isLodging = types.some((t) =>
    /hotel|lodgingbusiness|resort|motel|bedandbreakfast/i.test(t)
  );
  if (typeof node.description === "string" && node.description.trim()) {
    hits.push({
      text: cleanText(node.description),
      method: isLodging ? "json_ld_hotel_description" : "json_ld_description",
      lodging: isLodging,
    });
  }
  for (const v of Object.values(node)) {
    if (v && typeof v === "object") walkJsonLdDescriptions(v, hits);
  }
}

function walkJsonLdAmenities(node, out) {
  if (!node || typeof node !== "object") return;
  if (Array.isArray(node)) {
    for (const item of node) walkJsonLdAmenities(item, out);
    return;
  }
  const types = []
    .concat(node["@type"] || [])
    .map((t) => String(t).toLowerCase());
  if (
    types.includes("locationfeaturespecification") ||
    types.includes("locationfeature")
  ) {
    const name = cleanText(node.name || node.value || "");
    if (name) out.push(name);
  }
  if (Array.isArray(node.amenityFeature)) {
    for (const a of node.amenityFeature) {
      const name = cleanText(a?.name || a?.value || "");
      if (name) out.push(name);
    }
  }
  for (const v of Object.values(node)) {
    if (v && typeof v === "object") walkJsonLdAmenities(v, out);
  }
}

/**
 * Extract description candidates from official HTML.
 * @param {string} html
 * @param {{ url?: string, family?: string, propertyName?: string }} [opts]
 */
export function extractDescriptionsFromOfficialHtml(html, opts = {}) {
  const text = String(html || "");
  const family = String(opts.family || "").toLowerCase();
  /** @type {DescHit[]} */
  const hits = [];
  const patterns_matched = [];

  // JSON-LD
  let m;
  const re = new RegExp(JSON_LD_RE.source, "gi");
  const ldHits = [];
  while ((m = re.exec(text))) {
    const parsed = parseJsonSafe(m[1].trim());
    if (!parsed) continue;
    walkJsonLdDescriptions(parsed, ldHits);
  }
  for (const h of ldHits) {
    const q = assessDescriptionQuality(h.text);
    hits.push({
      text: h.text,
      method: h.method,
      confidence: q.ok ? (h.lodging ? "High" : q.confidence) : "Low",
      rejected: !q.ok,
      reject_reason: q.reason,
    });
    if (q.ok) patterns_matched.push(h.method);
  }

  // Meta / OG
  for (const [prop, method] of [
    ["og:description", "og_description"],
    ["description", "meta_description"],
    ["twitter:description", "twitter_description"],
  ]) {
    const v = pickMeta(text, prop);
    if (!v) continue;
    const q = assessDescriptionQuality(v);
    hits.push({
      text: v,
      method,
      confidence: q.ok ? "Medium" : "Low",
      rejected: !q.ok,
      reject_reason: q.reason,
    });
    if (q.ok) patterns_matched.push(method);
  }

  // Family payload strings
  const familyKeys = [
    [/"(?:hotelOverview|overviewDescription|longDescription|propertyDescription|aboutTheHotel)"\s*:\s*"((?:\\.|[^"\\])+)"/i, "family_overview_json"],
    [/"(?:seoDescription|marketingDescription)"\s*:\s*"((?:\\.|[^"\\])+)"/i, "family_seo_json"],
  ];
  for (const [rx, method] of familyKeys) {
    const mm = text.match(rx);
    if (!mm) continue;
    const v = cleanText(unescapeJson(mm[1]));
    const q = assessDescriptionQuality(v);
    hits.push({
      text: v,
      method,
      confidence: q.ok ? "High" : "Low",
      rejected: !q.ok,
      reject_reason: q.reason,
    });
    if (q.ok) patterns_matched.push(method);
  }

  // Visible FAQ / about paragraphs (secondary)
  const paras = [...text.matchAll(/<p[^>]*>([\s\S]{60,500}?)<\/p>/gi)]
    .map((x) => cleanText(x[1]))
    .filter(Boolean);
  for (const p of paras.slice(0, 30)) {
    if (!/hotel|located|offers|features|guest|pool|restaurant/i.test(p)) continue;
    if (/cookie|javascript|browser|parking description\./i.test(p)) continue;
    const q = assessDescriptionQuality(p);
    if (!q.ok) continue;
    hits.push({
      text: p,
      method: "html_paragraph",
      confidence: "Medium",
      rejected: false,
    });
    patterns_matched.push("html_paragraph");
  }

  void family;
  return { hits, patterns_matched: [...new Set(patterns_matched)] };
}

/**
 * Extract amenity tags from official HTML.
 * @param {string} html
 */
export function extractAmenitiesFromOfficialHtml(html) {
  const text = String(html || "");
  /** @type {string[]} */
  const tags = [];
  const patterns_matched = [];

  let m;
  const re = new RegExp(JSON_LD_RE.source, "gi");
  while ((m = re.exec(text))) {
    const parsed = parseJsonSafe(m[1].trim());
    if (!parsed) continue;
    const before = tags.length;
    walkJsonLdAmenities(parsed, tags);
    if (tags.length > before) patterns_matched.push("json_ld_amenity_feature");
  }

  // Common JSON arrays of amenity names
  for (const rx of [
    /"amenities"\s*:\s*\[([^\]]{10,2000})\]/i,
    /"facilities"\s*:\s*\[([^\]]{10,2000})\]/i,
  ]) {
    const mm = text.match(rx);
    if (!mm) continue;
    const names = [...mm[1].matchAll(/"name"\s*:\s*"((?:\\.|[^"\\])+)"/gi)].map((x) =>
      cleanText(unescapeJson(x[1]))
    );
    const strings = [...mm[1].matchAll(/"((?:\\.|[^"\\]){3,60})"/g)]
      .map((x) => cleanText(unescapeJson(x[1])))
      .filter((s) => !/https?:|true|false|null/i.test(s));
    for (const n of [...names, ...strings]) {
      if (
        n &&
        /pool|wifi|gym|fitness|parking|restaurant|breakfast|spa|meeting|bar|laundry|pet|kitchen|business|concierge|air conditioning|elevator/i.test(
          n
        )
      ) {
        tags.push(n);
      }
    }
    if (names.length || strings.length) patterns_matched.push("json_amenities_array");
  }

  const unique = [...new Set(tags.map((t) => t.trim()).filter(Boolean))].slice(0, 40);
  return {
    tags: unique,
    source_text: unique.join("; "),
    confidence: unique.length >= 5 ? "High" : unique.length >= MIN_AMENITY_COUNT ? "Medium" : "Low",
    patterns_matched: [...new Set(patterns_matched)],
    ok: unique.length >= MIN_AMENITY_COUNT,
  };
}

/**
 * Select best non-rejected description hit.
 * @param {DescHit[]} hits
 */
export function selectBestDescriptionHit(hits) {
  const ok = (hits || []).filter((h) => h && !h.rejected && h.text);
  if (!ok.length) return null;
  const rank = { High: 3, Medium: 2, Low: 1 };
  ok.sort((a, b) => {
    const dr = (rank[b.confidence] || 0) - (rank[a.confidence] || 0);
    if (dr) return dr;
    return b.text.length - a.text.length;
  });
  const best = ok[0];
  if (best.confidence === "Low") return null;
  return best;
}

/**
 * Grounded AI summary: compress Source Text without inventing facts.
 * @param {string} sourceText
 * @param {{ maxLen?: number }} [opts]
 */
export function buildGroundedAiSummary(sourceText, opts = {}) {
  const maxLen = opts.maxLen || 400;
  const t = cleanText(sourceText);
  if (!t) return null;
  if (isBookingBoilerplate(t)) return null;
  // Keep first sentences up to maxLen
  const sentences = t.split(/(?<=[.!?])\s+/).filter(Boolean);
  let out = "";
  for (const s of sentences) {
    const next = out ? `${out} ${s}` : s;
    if (next.length > maxLen) break;
    out = next;
  }
  if (!out) out = t.slice(0, maxLen);
  return out.trim() || null;
}

/**
 * Build factual Source Text from amenities + address when narrative is absent.
 * Only used when amenities are High/Medium from the same official page.
 * @param {{ propertyName?: string, amenities?: string[], address?: string, sourceUrl?: string }} input
 */
export function buildFactualSourceTextFromAmenities(input) {
  const name = cleanText(input.propertyName || "");
  const amenities = (input.amenities || []).map(cleanText).filter(Boolean);
  const address = cleanText(input.address || "");
  if (amenities.length < MIN_AMENITY_COUNT) return null;
  const parts = [];
  if (name) parts.push(`${name} (official property page).`);
  parts.push(`Facilities listed: ${amenities.join("; ")}.`);
  if (address) parts.push(`Address: ${address}.`);
  const text = parts.join(" ");
  return {
    text,
    method: "official_page_amenities_factual_assembly",
    confidence: amenities.length >= 5 ? "Medium" : "Medium",
  };
}

/**
 * Extract a street/locality address snippet from HTML for factual assembly.
 * @param {string} html
 */
export function extractAddressSnippet(html) {
  const text = String(html || "");
  const street =
    text.match(/"streetAddress"\s*:\s*"((?:\\.|[^"\\])+)"/i) ||
    text.match(/itemprop=["']streetAddress["'][^>]*>([^<]+)</i);
  const locality =
    text.match(/"addressLocality"\s*:\s*"((?:\\.|[^"\\])+)"/i) ||
    text.match(/itemprop=["']addressLocality["'][^>]*>([^<]+)</i);
  const region = text.match(/"addressRegion"\s*:\s*"((?:\\.|[^"\\])+)"/i);
  const parts = [
    street ? unescapeJson(street[1]) : null,
    locality ? unescapeJson(locality[1]) : null,
    region ? unescapeJson(region[1]) : null,
  ]
    .map(cleanText)
    .filter(Boolean);
  return parts.length ? parts.join(", ") : null;
}

/**
 * Full extraction package for one official page.
 * @param {string} html
 * @param {{ url?: string, family?: string, propertyName?: string }} opts
 */
export function extractOfficialPageEnrichment(html, opts = {}) {
  const descPack = extractDescriptionsFromOfficialHtml(html, opts);
  const amenPack = extractAmenitiesFromOfficialHtml(html);
  const address = extractAddressSnippet(html);
  let description = selectBestDescriptionHit(descPack.hits);

  // If narrative missing, assemble factual Source Text from same-page amenities (not invention)
  if (!description && amenPack.ok) {
    const assembled = buildFactualSourceTextFromAmenities({
      propertyName: opts.propertyName,
      amenities: amenPack.tags,
      address,
      sourceUrl: opts.url,
    });
    if (assembled) {
      description = {
        text: assembled.text,
        method: assembled.method,
        confidence: assembled.confidence,
        rejected: false,
      };
      descPack.patterns_matched.push(assembled.method);
    }
  }

  const aiSummary = description ? buildGroundedAiSummary(description.text) : null;

  return {
    description,
    ai_summary: aiSummary,
    amenities: amenPack,
    address,
    patterns_matched: [
      ...new Set([...descPack.patterns_matched, ...amenPack.patterns_matched]),
    ],
    description_hits_total: descPack.hits.length,
    description_hits_rejected: descPack.hits.filter((h) => h.rejected).length,
  };
}

export const DESCRIPTION_EXTRACTOR_VERSION = "production-census-description-extractor-v1";
