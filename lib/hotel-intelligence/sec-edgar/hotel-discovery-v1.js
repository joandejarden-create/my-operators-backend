/**
 * Stage 1 — Independent hotel discovery from SEC filings.
 * Discovers named hotels even when no contract dates are present.
 * Does NOT require HPC existence.
 */

import crypto from "node:crypto";

export const HOTEL_DISCOVERY_VERSION = "sec-edgar-hotel-discovery-v1";

const US_STATE =
  /\b(AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|IA|ID|IL|IN|KS|KY|LA|MA|MD|ME|MI|MN|MO|MS|MT|NC|ND|NE|NH|NJ|NM|NV|NY|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VA|VT|WA|WI|WV|WY|DC)\b/;

const BRAND_PREFIX =
  /(?:Marriott|JW Marriott|Courtyard|Residence Inn|SpringHill Suites|Fairfield(?: Inn)?|Aloft|Moxy|Westin|Sheraton|Renaissance|Ritz[- ]Carlton|St\.?\s*Regis|W Hotel|Edition|Autograph Collection|Tribute Portfolio|Design Hotels|Hilton|Hampton Inn|Embassy Suites|DoubleTree|Homewood Suites|Home2 Suites|Hyatt|Andaz|Park Hyatt|Grand Hyatt|Hyatt Place|Hyatt House|Kimpton|Four Seasons|Ritz|Waldorf Astoria|Conrad|Canopy|Curio|Tapestry|Spark|Motto|InterContinental|Holiday Inn|Crowne Plaza|Staybridge|Candlewood|voco|Hotel Indigo|Even Hotels|Avid|Kimpton|1 Hotel|Ace Hotel|Graduate|Graduate Hotels)/i;

const BLOCK =
  /\b(agreement|amended|restated|bylaws|llc|l\.l\.c|inc\.?|corp\.?|corporation|partnership|purchaser|director|award|stock|shareholder|indemnification|limited liability|registrant|exhibit|table of contents|form 10|depreciation|operating expenses|operating revenue|caused|increase|decrease|business|portfolio|collection|operations overview|beverage other|overview|investors|comparable hotel|blue chip|economic indicators|hotel ownership total|marriott bonvoy|marriott-managed)\b/i;

const NAME_PREFIX_BLOCK =
  /^(Operations|Overview|Beverage|Acquired|Depreciation|American|Business|Other|Total|All|Our|The Company|Contents|See|TRS|Comparable|Hotel Ownership)\b/i;

const STATE_PREFIX_RE =
  /^(AZ|CA|NY|FL|TX|CO|WA|IL|MA|MD|NJ|NV|VA|PA|GA|NC|OR|OH|MI|TN|IN|WI|MN|MO|SC|LA|AL|KY|OK|CT|IA|UT|AR|MS|KS|NM|NE|ID|WV|HI|NH|ME|RI|MT|DE|SD|ND|AK|VT|WY|DC)\s+/i;

/** Trailing sentence verbs / accounting noise after a brand mention */
const TRAILING_NOISE =
  /\b(caused|increased|decreased|resulted|compared|including|related|pursuant|respectively|approximately|totaled|comprised|acquired|depreciation|life|revenue|expense|expenses|income|ebitda|occupancy|adr|revpar)\b/i;

function stripHtml(html) {
  return String(html || "")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<\/(td|th|tr|p|div|li|br|h\d)>/gi, "\n")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/[ \t]+/g, " ")
    .replace(/\n{2,}/g, "\n")
    .trim();
}

function discoveryId(name, city, state, _cik) {
  // Physical identity = name + location. Registrant CIK is metadata, not identity.
  const raw = [normalizeKey(name), normalizeKey(city), normalizeKey(state)]
    .join("|")
    .toLowerCase();
  return `disc_sec_${crypto.createHash("sha1").update(raw).digest("hex").slice(0, 20)}`;
}

function normalizeKey(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function cleanName(name) {
  let n = String(name || "")
    .replace(/\s+/g, " ")
    .replace(/^[\-\d.\s]+/, "")
    .replace(/[,\s]+$/, "")
    .trim();
  // Strip leading US state codes that leaked from adjacent table columns
  n = n.replace(STATE_PREFIX_RE, "").trim();
  // Truncate at trailing noise verbs (prose bleed)
  const noise = n.search(TRAILING_NOISE);
  if (noise > 8) n = n.slice(0, noise).trim();
  // Multi-hotel conjunctions are not a single property — reject in isPlausible
  // Drop overly long "X and Y" chains for cleanup only when clearly narrative
  if (/\band\b/i.test(n) && n.split(/\s+/).length > 8) {
    n = n.split(/\band\b/i)[0].trim();
  }
  return n.replace(/[,\s]+$/, "").trim();
}

export function isPlausibleHotelName(name) {
  const n = cleanName(name);
  if (n.length < 6 || n.length > 72) return false;
  if (NAME_PREFIX_BLOCK.test(n)) return false;
  if (BLOCK.test(n)) return false;
  if (TRAILING_NOISE.test(n)) return false;
  if (!/[A-Za-z]/.test(n)) return false;
  // Reject bare "Sunstone Hotel" / registrant-style labels without place
  if (/^(Sunstone|Ashford|Host|Marriott)\s+Hotel(s)?$/i.test(n)) return false;
  // Reject bare brand-family labels without a property place/word
  if (
    /^(Autograph Collection|Tribute Portfolio|Design Hotels|Marriott|Hilton|Hyatt|IHG|AC Hotel|Courtyard(?: by Marriott)?|Residence Inn|Embassy Suites|Hampton Inn|SpringHill Suites|TownePlace Suites|Hyatt Place|Hyatt Regency|Hotel Indigo)$/i.test(
      n,
    )
  ) {
    return false;
  }
  // Reject multi-hotel "X and Y" when both sides look like hotels
  if (/\band\b/i.test(n) && !/\bresort\s+and\s+spa\b|\bresort\s+and\s+residences?\b/i.test(n)) {
    const parts = n.split(/\band\b/i);
    if (parts.length >= 2 && BRAND_PREFIX.test(parts[1] || "")) return false;
  }
  const hasPropertyWord = /\b(Hotel|Inn|Resort|Suites|Lodge|House|Place)\b/i.test(n);
  const hasBrand = BRAND_PREFIX.test(n);
  if (!hasPropertyWord && !hasBrand) return false;
  // Brand-only mentions need a place-like token (capitalized word after brand)
  if (hasBrand && !hasPropertyWord) {
    const afterBrand = n.replace(BRAND_PREFIX, "").trim();
    if (!afterBrand || afterBrand.split(/\s+/).length < 1) return false;
    if (/^(Inc|Corp|International|Hotels|Resorts)\b/i.test(afterBrand)) return false;
  }
  // Too many lowercase function words → prose fragment
  const lowerRatio =
    (n.match(/\b(the|and|to|of|for|with|from|by|as|at|in|on)\b/gi) || []).length /
    Math.max(1, n.split(/\s+/).length);
  if (lowerRatio > 0.35) return false;
  return true;
}

/**
 * Extract location hints near a hotel name mention.
 */
function locationNear(text, index, radius = 120) {
  const slice = text.slice(Math.max(0, index - 20), Math.min(text.length, index + radius));
  const cityState = slice.match(
    /([A-Z][a-zA-Z .'-]{2,40}),\s*([A-Z]{2})(?:\s+\d{5})?/,
  );
  if (cityState && US_STATE.test(cityState[2])) {
    const city = cityState[1].trim();
    // Owner/entity fragments are not cities
    if (!/\b(Owner|LLC|Inc|Corp|LP|Trust)\b/i.test(city) && city.split(/\s+/).length <= 4) {
      return {
        city,
        stateRegion: cityState[2],
        country: "United States",
      };
    }
  }
  const stateOnly = slice.match(US_STATE);
  return {
    city: null,
    stateRegion: stateOnly ? stateOnly[1] : null,
    country: stateOnly ? "United States" : null,
  };
}

/**
 * Property-schedule style lines:
 * "Westin Chicago River North Chicago IL 407"
 * "Courtyard San Diego Downtown, San Diego, CA"
 */
function extractFromScheduleLines(text, meta = {}) {
  const found = [];
  const lines = text.split(/\n/).map((l) => l.trim()).filter(Boolean);
  for (const line of lines) {
    if (line.length < 10 || line.length > 160) continue;
    if (BLOCK.test(line) && !BRAND_PREFIX.test(line)) continue;

    // Brand + place + optional city/state
    let m = line.match(
      new RegExp(
        `^(${BRAND_PREFIX.source}[A-Za-z0-9&.'\\- ]{0,50}?)(?:,\\s*|\\s+)([A-Z][a-zA-Z .'-]{2,40}),\\s*([A-Z]{2})\\b`,
        "i",
      ),
    );
    if (m && isPlausibleHotelName(m[1])) {
      found.push({
        hotelName: cleanName(m[1]),
        city: m[2].trim(),
        stateRegion: m[3],
        country: "United States",
        discoveryMethod: "SCHEDULE_LINE_CITY_STATE",
        sourceLine: line.slice(0, 200),
      });
      continue;
    }

    // "... Hotel, City, ST"
    m = line.match(
      /^((?:The\s+)?[A-Z][A-Za-z0-9&.'\- ]{3,60}\b(?:Hotel|Inn|Resort|Suites))\s*,\s*([A-Z][a-zA-Z .'-]{2,40})\s*,\s*([A-Z]{2})\b/,
    );
    if (m && isPlausibleHotelName(m[1])) {
      found.push({
        hotelName: cleanName(m[1]),
        city: m[2].trim(),
        stateRegion: m[3],
        country: "United States",
        discoveryMethod: "SCHEDULE_LINE_HOTEL_CITY_STATE",
        sourceLine: line.slice(0, 200),
      });
      continue;
    }

    // Brand-prefixed property without city — stop before noise verbs
    m = line.match(
      new RegExp(
        `^(${BRAND_PREFIX.source}(?:\\s+[A-Z][A-Za-z0-9&.'\\-]{1,28}){0,6})\\b`,
        "i",
      ),
    );
    if (m && isPlausibleHotelName(m[1]) && !/agreement|exhibit/i.test(line)) {
      const loc = locationNear(line, 0, line.length);
      found.push({
        hotelName: cleanName(m[1]),
        city: loc.city,
        stateRegion: loc.stateRegion,
        country: loc.country || "United States",
        discoveryMethod: "SCHEDULE_LINE_BRAND",
        sourceLine: line.slice(0, 200),
      });
    }
  }
  return found.map((h) => attachMeta(h, meta));
}

/**
 * Inline prose mentions of branded hotels / * Hotel names.
 */
function extractFromProse(text, meta = {}) {
  const found = [];
  const patterns = [
    new RegExp(
      `\\b((?:The\\s+)?${BRAND_PREFIX.source}[A-Za-z0-9&.'\\- ]{0,45}?)(?=,|\\s+in\\s+|\\s+located|\\s+\\(|$)`,
      "gi",
    ),
    /\b((?:The\s+)?(?:\d+\s+)?[A-Z][A-Za-z0-9&.'\-]{1,30}(?:\s+[A-Z][A-Za-z0-9&.'\-]{1,25}){0,5}\s+(?:Hotel|Inn|Resort|Suites))\b/g,
  ];
  for (const re of patterns) {
    re.lastIndex = 0;
    let m;
    while ((m = re.exec(text)) !== null) {
      const name = cleanName(m[1]);
      if (!isPlausibleHotelName(name)) continue;
      const loc = locationNear(text, m.index);
      found.push(
        attachMeta(
          {
            hotelName: name,
            city: loc.city,
            stateRegion: loc.stateRegion,
            country: loc.country,
            discoveryMethod: "PROSE_MENTION",
            sourceLine: text.slice(m.index, m.index + 160).replace(/\s+/g, " "),
          },
          meta,
        ),
      );
      if (found.length >= (meta.maxHotels || 200)) break;
    }
  }
  return found;
}

function attachMeta(hotel, meta) {
  const id = discoveryId(hotel.hotelName, hotel.city, hotel.stateRegion, meta.cik);
  return {
    discoveryId: id,
    hotelName: hotel.hotelName,
    city: hotel.city || null,
    stateRegion: hotel.stateRegion || null,
    country: hotel.country || null,
    address: hotel.address || null,
    brandHint: detectBrand(hotel.hotelName),
    registrant: meta.companyName || null,
    cik: meta.cik || null,
    sourceFiling: meta.form || null,
    sourceAccession: meta.accessionNumber || null,
    sourceUrl: meta.documentUrl || null,
    discoveryMethod: hotel.discoveryMethod,
    sourceLine: hotel.sourceLine || null,
    discoveredAt: new Date().toISOString(),
    discoveryVersion: HOTEL_DISCOVERY_VERSION,
  };
}

function detectBrand(name) {
  const m = String(name || "").match(BRAND_PREFIX);
  return m ? m[0] : null;
}

/**
 * Deduplicate discovered hotels by normalized name + location.
 */
export function dedupeDiscoveredHotels(hotels = []) {
  const map = new Map();
  for (const h of hotels) {
    const cleaned = cleanName(h.hotelName);
    if (!isPlausibleHotelName(cleaned)) continue;
    const row = { ...h, hotelName: cleaned };
    const key = [
      normalizeKey(row.hotelName),
      normalizeKey(row.city),
      normalizeKey(row.stateRegion),
      row.cik || "",
    ].join("|");
    const prev = map.get(key);
    if (!prev) {
      map.set(key, { ...row, mentionCount: 1 });
      continue;
    }
    prev.mentionCount = (prev.mentionCount || 1) + 1;
    // Prefer rows with more location detail
    if (!prev.city && row.city) prev.city = row.city;
    if (!prev.stateRegion && row.stateRegion) prev.stateRegion = row.stateRegion;
    if (!prev.sourceUrl && row.sourceUrl) prev.sourceUrl = row.sourceUrl;
  }
  return [...map.values()];
}

/**
 * Stage 1 entry: discover hotels from a filing document.
 */
export function discoverHotelsFromDocument(doc, meta = {}) {
  const raw = doc?.text || doc?.html || "";
  const text = stripHtml(raw);
  const fromSchedule = extractFromScheduleLines(text, meta);
  const fromProse = extractFromProse(text, {
    ...meta,
    maxHotels: meta.maxHotels || 200,
  });
  const hotels = dedupeDiscoveredHotels([...fromSchedule, ...fromProse]);
  return {
    ok: true,
    version: HOTEL_DISCOVERY_VERSION,
    characterCount: text.length,
    hotelCount: hotels.length,
    hotels,
    yieldNote:
      "Named hotels retained even without contract dates. HPC match is a later stage.",
  };
}
