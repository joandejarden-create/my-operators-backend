/**
 * Shared adapter helpers for Research Engine V2 directory checks.
 */

export const DEFAULT_FETCH_HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
  "Accept-Language": "en-US,en;q=0.9",
};

/**
 * @param {number} ms
 */
export function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * @param {string} url
 * @param {{ headers?: Record<string,string>, timeoutMs?: number }} [opts]
 */
export async function fetchText(url, opts = {}) {
  const controller = new AbortController();
  const timeoutMs = opts.timeoutMs ?? 25000;
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      headers: { ...DEFAULT_FETCH_HEADERS, ...(opts.headers || {}) },
      redirect: "follow",
      signal: controller.signal,
    });
    const text = await res.text();
    return {
      ok: res.ok,
      status: res.status,
      url: res.url,
      text,
      retrievedAt: new Date().toISOString(),
    };
  } finally {
    clearTimeout(timer);
  }
}

/**
 * @param {string} a
 * @param {string} b
 */
export function tokenize(a) {
  return String(a || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9\s]/g, " ")
    .split(/\s+/)
    .filter((t) => t.length > 2 && !STOP.has(t));
}

const STOP = new Set([
  "the",
  "and",
  "hotel",
  "hotels",
  "resort",
  "resorts",
  "spa",
  "by",
  "a",
  "an",
  "of",
  "member",
  "collection",
  "portfolio",
  "indigo",
  "kimpton",
  "tribute",
  "avani",
  "radisson",
  "individuals",
  "faranda",
  "boutique",
  "grand",
  "express",
]);

/**
 * Simple token Jaccard similarity 0..1
 * @param {string} a
 * @param {string} b
 */
export function tokenSimilarity(a, b) {
  const ta = new Set(tokenize(a));
  const tb = new Set(tokenize(b));
  if (!ta.size || !tb.size) return 0;
  let inter = 0;
  for (const t of ta) if (tb.has(t)) inter++;
  const union = ta.size + tb.size - inter;
  return union ? inter / union : 0;
}

/**
 * @param {object} hotel
 * @param {object[]} directoryRows
 * @param {{ nameKeys?: string[], cityKeys?: string[], countryKeys?: string[], urlKeys?: string[], minScore?: number }} [opts]
 */
export function matchHotelToDirectory(hotel, directoryRows, opts = {}) {
  const nameKeys = opts.nameKeys || ["name", "inferredHotelName", "propertyName", "title"];
  const cityKeys = opts.cityKeys || ["city", "citySlug"];
  const countryKeys = opts.countryKeys || ["country", "countryRegion", "countryCode"];
  const minScore = opts.minScore ?? 0.35;

  const hotelName = String(hotel.name || hotel.hotelName || "");
  const hotelCity = String(hotel.city || "");
  const hotelCountry = String(hotel.country || "").toLowerCase();

  let best = null;
  let bestScore = 0;

  for (const row of directoryRows || []) {
    const dirName = String(nameKeys.map((k) => row[k]).find(Boolean) || "");
    const dirCity = String(cityKeys.map((k) => row[k]).find(Boolean) || "");
    const dirCountry = String(countryKeys.map((k) => row[k]).find(Boolean) || "").toLowerCase();

    let score = tokenSimilarity(hotelName, dirName);
    if (hotelCity && dirCity && tokenSimilarity(hotelCity, dirCity) >= 0.4) score += 0.15;
    if (hotelCountry && dirCountry) {
      if (dirCountry.includes(hotelCountry) || hotelCountry.includes(dirCountry.split(",")[0])) {
        score += 0.1;
      } else if (hotelCountry.length > 2 && dirCountry.length > 2 && !dirCountry.includes(hotelCountry.slice(0, 4))) {
        // soft penalty for clear country mismatch
        score -= 0.2;
      }
    }
    if (score > bestScore) {
      bestScore = score;
      best = row;
    }
  }

  if (!best || bestScore < minScore) {
    return { match: null, score: bestScore, confidence: "none" };
  }
  return {
    match: best,
    score: bestScore,
    confidence: bestScore >= 0.65 ? "high" : bestScore >= 0.45 ? "medium" : "low",
  };
}

/**
 * Normalized adapter observation.
 * @param {object} partial
 */
export function normalizeAdapterObservation(partial = {}) {
  return {
    hotelFound: Boolean(partial.hotelFound),
    officialHotelName: partial.officialHotelName || null,
    brand: partial.brand || null,
    parent: partial.parent || null,
    city: partial.city || null,
    country: partial.country || null,
    operatingStatus: partial.operatingStatus || null,
    bookable: partial.bookable ?? null,
    officialUrl: partial.officialUrl || null,
    evidenceTimestamp: partial.evidenceTimestamp || new Date().toISOString(),
    sourceType: partial.sourceType || "official_brand_directory",
    sourceDate: partial.sourceDate || null,
    adapter: partial.adapter || "unknown",
    confidence: partial.confidence ?? null,
    notes: partial.notes || "",
    sourceState: partial.sourceState || null,
    sourceStateReason: partial.sourceStateReason || null,
    rawSignals: partial.rawSignals || {},
  };
}
