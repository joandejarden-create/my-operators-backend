/**
 * Cvent Supplier Network venue discovery + HTML parse for Census fills.
 * Disk-cached under reports/cvent-venue-cache/. Rate-limited fetches.
 */
import { createHash } from "node:crypto";
import {
  existsSync,
  mkdirSync,
  readFileSync,
  writeFileSync,
} from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

export const CVENT_VENUE_CLIENT_VERSION = "census-cvent-venue-client-v4";

/** Venue-type IDs observed on Cvent Supplier Network → census Property Type. */
export const CVENT_VENUE_TYPE_TO_PROPERTY_TYPE = Object.freeze({
  HOTEL: "Hotel",
  RESORT: "Resort",
  BOUTIQUE_HOTEL: "Boutique Hotel",
});

/** Airport Asset Context gate (imperial miles). */
export const CVENT_AIRPORT_ASSET_CONTEXT_MAX_MI = 3;

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "../..");
const CACHE_DIR = join(ROOT, "reports", "cvent-venue-cache");

const UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

const CHOICE_CHAIN_NEEDLES = [
  "choice hotels and radisson americas",
  "choice hotels®",
  "connections happen here. group bookings made easy by choice hotels",
];

/** Known Choice sitewide rooms false positive — never accept. */
export const CVENT_FORBIDDEN_ROOMS = Object.freeze([25]);

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function cacheKey(kind, input) {
  const h = createHash("sha1").update(`${kind}:${input}`).digest("hex").slice(0, 20);
  return join(CACHE_DIR, `${kind}-${h}.json`);
}

function readCache(path) {
  if (!existsSync(path)) return null;
  try {
    return JSON.parse(readFileSync(path, "utf8"));
  } catch {
    return null;
  }
}

function writeCache(path, payload) {
  mkdirSync(CACHE_DIR, { recursive: true });
  writeFileSync(path, JSON.stringify(payload, null, 2));
}

export function normCventText(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function slugify(s) {
  return normCventText(s).replace(/\s+/g, "-");
}

/**
 * Candidate venue URL shapes from name + city (best-effort; must still parse).
 */
export function buildCventSlugCandidates({ name, city, country = "Mexico" }) {
  const n = slugify(name);
  const c = slugify(city);
  if (!n || !c) return [];
  const countrySlug = slugify(country) || "mexico";
  return [
    `https://www.cvent.com/venues/${c}/hotel/${n}/`,
    `https://www.cvent.com/venues/${countrySlug}/${c}/hotel/${n}/`,
  ];
}

/**
 * Extract cvent.com/venues/.../venue-{uuid} links from HTML.
 */
export function extractCventVenueUrls(html) {
  const out = new Set();
  const re =
    /https?:\/\/(?:www\.)?cvent\.com\/venues\/[a-z0-9%/_\-.]+\/venue-[a-f0-9-]{36}/gi;
  for (const m of String(html || "").matchAll(re)) {
    let u = m[0].replace(/&amp;/g, "&").split(/[?#]/)[0];
    // Normalize encoding
    try {
      u = decodeURIComponent(u);
    } catch {
      /* keep raw */
    }
    out.add(u);
  }
  // Relative /venues/.../venue-uuid
  const reRel =
    /\/venues\/[a-z0-9%/_\-.]+\/venue-[a-f0-9-]{36}/gi;
  for (const m of String(html || "").matchAll(reRel)) {
    out.add(`https://www.cvent.com${m[0]}`);
  }
  return [...out];
}

async function fetchHtml(url, { timeoutMs = 25000 } = {}) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      signal: ctrl.signal,
      redirect: "follow",
      headers: {
        "User-Agent": UA,
        Accept: "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
      },
    });
    const text = await res.text();
    return { ok: res.ok, status: res.status, bytes: text.length, text, finalUrl: res.url };
  } catch (e) {
    return {
      ok: false,
      status: 0,
      bytes: 0,
      text: "",
      error: String(e?.message || e),
    };
  } finally {
    clearTimeout(t);
  }
}

/**
 * Discover Cvent venue URLs via DuckDuckGo HTML + slug guesses.
 */
export async function discoverCventVenueUrls(
  { name, brand, city, country = "Mexico" } = {},
  opts = {}
) {
  const throttleMs = Number(opts.throttleMs ?? 1100);
  const useCache = opts.useCache !== false;
  const qKey = `${name}|${brand}|${city}|${country}`;
  const path = cacheKey("discover-v2", qKey);
  if (useCache) {
    const hit = readCache(path);
    if (hit?.urls) return { ...hit, from_cache: true };
  }

  const urls = new Set();
  const queries = [
    `site:cvent.com/venues ${name} ${city}`,
    `site:cvent.com/venues ${brand || ""} ${city} Choice Hotels`,
    `site:cvent.com/venues "${name}"`,
  ]
    .map((q) => q.replace(/\s+/g, " ").trim())
    .filter((q) => q.length > 20);

  const discoverLog = [];
  async function runSearch(label, buildUrl) {
    for (const q of queries) {
      const page = await fetchHtml(buildUrl(q));
      await sleep(throttleMs);
      const found = extractCventVenueUrls(page.text);
      for (const u of found) urls.add(u);
      discoverLog.push({
        engine: label,
        query: q,
        status: page.status,
        found: found.length,
        error: page.error || null,
      });
      if ([...urls].some((u) => /\/venue-[a-f0-9-]{36}/i.test(u))) return true;
    }
    return false;
  }

  const ddgOk = await runSearch(
    "duckduckgo",
    (q) => `https://html.duckduckgo.com/html/?q=${encodeURIComponent(q)}`
  );
  if (!ddgOk) {
    await runSearch(
      "bing",
      (q) => `https://www.bing.com/search?q=${encodeURIComponent(q)}`
    );
  }

  // Slug candidates are weak — only keep if fetch later succeeds; store as candidates
  const slugCandidates = buildCventSlugCandidates({ name, city, country });

  const payload = {
    version: CVENT_VENUE_CLIENT_VERSION,
    queried_at: new Date().toISOString(),
    query: qKey,
    urls: [...urls],
    slug_candidates: slugCandidates,
    discover_log: discoverLog,
  };
  if (useCache) writeCache(path, payload);
  return { ...payload, from_cache: false };
}

function unescapeCventFlightFragment(s) {
  return String(s || "")
    .replace(/\\u003c/gi, "<")
    .replace(/\\u003e/gi, ">")
    .replace(/\\u0026/gi, "&")
    .replace(/\\"/g, '"')
    .replace(/\\n/g, "\n")
    .replace(/\\\\/g, "\\");
}

function pickJsonLdHotel(html) {
  const re =
    /<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi;
  for (const m of String(html || "").matchAll(re)) {
    try {
      const obj = JSON.parse(m[1]);
      const type = obj?.["@type"];
      const types = Array.isArray(type) ? type : [type];
      if (types.some((t) => String(t || "").toLowerCase() === "hotel")) {
        return obj;
      }
    } catch {
      /* skip malformed ld+json */
    }
  }
  return null;
}

/**
 * Pull a scalar from Cvent Next.js flight / basicProfile payloads (escaped or plain).
 */
export function pickCventPayloadField(html, key, { haystack } = {}) {
  const text = String(haystack || html || "");
  const escStr = text.match(
    new RegExp(`\\\\"${key}\\\\":\\\\"((?:\\\\.|[^"\\\\])*)\\\\"`)
  );
  if (escStr) {
    return unescapeCventFlightFragment(escStr[1]);
  }
  const escNum = text.match(new RegExp(`\\\\"${key}\\\\":(-?\\d+(?:\\.\\d+)?)`));
  if (escNum) return Number(escNum[1]);
  const plainStr = text.match(
    new RegExp(`"${key}"\\s*:\\s*"((?:\\\\.|[^"\\\\])*)"`)
  );
  if (plainStr) {
    try {
      return JSON.parse(`"${plainStr[1]}"`);
    } catch {
      return plainStr[1];
    }
  }
  const plainNum = text.match(new RegExp(`"${key}"\\s*:\\s*(-?\\d+(?:\\.\\d+)?)`));
  if (plainNum) return Number(plainNum[1]);
  return null;
}

/**
 * Isolate Cvent basicProfile JSON fragment (Next.js flight data).
 * Prefer this over whole-page key scans (avoids microsite name collisions).
 */
export function extractCventBasicProfileHaystack(html) {
  const text = String(html || "");
  const markers = ['"basicProfile":{', '\\"basicProfile\\":{'];
  for (const marker of markers) {
    const i = text.indexOf(marker);
    if (i < 0) continue;
    const fromBrace = text.slice(i + marker.length - 1);
    // Truncate near known trailing keys to keep parse cheap
    const cut = fromBrace.slice(0, 12000);
    const endHints = [
      cut.indexOf(',"meetingRooms":'),
      cut.indexOf(',\\"meetingRooms\\":'),
      cut.indexOf(',"promotions":'),
      cut.indexOf(',\\"promotions\\":'),
    ].filter((n) => n > 0);
    const end = endHints.length ? Math.min(...endHints) : cut.lastIndexOf("}");
    if (end <= 0) continue;
    let frag = cut.slice(0, end);
    // Ensure closing brace for profile object
    if (!frag.trim().endsWith("}")) frag += "}";
    if (marker.startsWith("\\")) frag = unescapeCventFlightFragment(frag);
    return frag;
  }
  // Fallback: name+address1 co-occurrence window (escaped flight chunk)
  const nameAddr = text.match(
    /\\"name\\":\\"([^"\\]{3,120})\\",\\"address1\\":\\"([^"\\]*)\\"/
  );
  if (nameAddr) {
    const i = nameAddr.index ?? text.indexOf(nameAddr[0]);
    return unescapeCventFlightFragment(text.slice(i, i + 4000));
  }
  return "";
}

/** Label then immediate sibling value div — used for Total guest rooms, etc. */
export function pickCventHtmlLabelValue(html, label) {
  const esc = String(label).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const re = new RegExp(
    `>${esc}<\\/div>\\s*<div[^>]*>\\s*([^<]{1,80})\\s*<\\/div>`,
    "i"
  );
  const m = String(html || "").match(re);
  if (!m) return null;
  const v = String(m[1] || "").trim();
  if (!v || /^(available|-|n\/?a)$/i.test(v)) return null;
  return v;
}

function parseMeetingSpaceArea(raw) {
  const s = String(raw || "").trim();
  const m = s.match(/([\d,]+(?:\.\d+)?)\s*(sq\.?\s*ft\.?|sq\.?\s*m\.?|m²|ft²)?/i);
  if (!m) return null;
  const value = Number(String(m[1]).replace(/,/g, ""));
  if (!Number.isFinite(value) || value <= 0) return null;
  const unitRaw = String(m[2] || "").toLowerCase();
  let unit = null;
  if (/m/.test(unitRaw)) unit = "sq_m";
  else if (/ft|foot/.test(unitRaw)) unit = "sq_ft";
  return { value, unit, raw: s };
}

function parseAirportDistance(html, labelValue) {
  const fromLabel = String(labelValue || "").match(
    /([\d.]+)\s*(mi|km|miles?|kilometers?)/i
  );
  if (fromLabel) {
    const value = Number(fromLabel[1]);
    const unit = /^mi/i.test(fromLabel[2]) ? "mi" : "km";
    if (Number.isFinite(value)) return { value, unit, raw: fromLabel[0] };
  }
  // Embedded object: "airportDistance":{"imperialValue":25.91,"metricValue":41.7}
  const obj = String(html || "").match(
    /\\?"airportDistance\\?"\s*:\s*\{\s*\\?"imperialValue\\?"\s*:\s*([\d.]+)\s*,\s*\\?"metricValue\\?"\s*:\s*([\d.]+)/i
  );
  if (obj) {
    const mi = Number(obj[1]);
    const km = Number(obj[2]);
    if (Number.isFinite(mi)) {
      return { value: mi, unit: "mi", metricValue: km, raw: `${mi} mi` };
    }
  }
  const plain = String(html || "").match(
    /Distance from airport[^0-9]{0,40}([\d.]+)\s*(mi|km)/i
  );
  if (plain) {
    const value = Number(plain[1]);
    const unit = /^mi/i.test(plain[2]) ? "mi" : "km";
    if (Number.isFinite(value)) return { value, unit, raw: plain[0] };
  }
  return null;
}

function isNonCventHttpUrl(url) {
  const s = String(url || "").trim();
  if (!/^https?:\/\//i.test(s)) return false;
  try {
    const host = new URL(s).hostname.toLowerCase();
    if (!host || host.includes("cvent.com")) return false;
    return true;
  } catch {
    return false;
  }
}

function isPlausibleAddress(cand) {
  const s = String(cand || "").trim();
  if (s.length < 12 || s.length > 200) return false;
  if (!/\d/.test(s)) return false;
  if (
    /no information available|contact us|select venue|favorite|overview|meeting space/i.test(
      s
    )
  ) {
    return false;
  }
  if (
    /(blvd|boulevard|av\.|avenida|calle|prol|km\b|carr\.|carretera|street|road|ave\b)/i.test(
      s
    ) ||
    /\d{4,5}/.test(s)
  ) {
    return true;
  }
  return /,\s*.+,\s*.+/.test(s);
}

function buildAddressFromParts({ street, city, region, postal, country }) {
  const parts = [street, city, region, postal, country]
    .map((p) => String(p || "").trim())
    .filter(Boolean);
  if (!parts.length) return null;
  // Prefer street-only when street already looks street-level; else join
  if (street && isPlausibleAddress(street) && parts.length === 1) return street;
  const joined = parts.join(", ");
  return isPlausibleAddress(joined) || isPlausibleAddress(street)
    ? joined
    : street || null;
}

/**
 * Parse a Cvent venue HTML page into structured fields for Census fills.
 * Prefer JSON-LD + Next basicProfile + label/value pairs over noisy plain text.
 *
 * guestRooms = sleeping/guest rooms only — never meeting-room counts.
 */
export function parseCventVenueHtml(html, sourceUrl = "") {
  const text = String(html || "");
  const plain = text
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, "\n")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/\s+\n/g, "\n")
    .replace(/\n+/g, "\n");

  const ld = pickJsonLdHotel(text);
  const ldAddr = ld?.address && typeof ld.address === "object" ? ld.address : {};
  const profileHay = extractCventBasicProfileHaystack(text);
  const pickProfile = (key) =>
    pickCventPayloadField(text, key, { haystack: profileHay }) ??
    pickCventPayloadField(text, key);

  const titleMatch =
    text.match(/<h1[^>]*>\s*([^<]{3,120})\s*<\/h1>/i) ||
    text.match(/<title>\s*([^|<]+)/i);
  const titleFromDom = String(titleMatch?.[1] || "")
    .replace(/\s*\|\s*Cvent.*$/i, "")
    .trim();

  const profileName = pickProfile("name");
  const title =
    (typeof profileName === "string" && profileName.length >= 3
      ? profileName
      : null) ||
    ld?.name ||
    titleFromDom ||
    null;

  const chainMatch =
    plain.match(/Chain\s*\n?\s*([^\n]{3,80})/i) ||
    plain.match(/Cadena\s*\n?\s*([^\n]{3,80})/i);
  const brandMatch =
    plain.match(/Brand\s*\n?\s*([A-Za-zÁÉÍÓÚáéíóú0-9 &®()\-]{3,60})/i) ||
    plain.match(/Marca\s*\n?\s*([A-Za-zÁÉÍÓÚáéíóú0-9 &®()\-]{3,60})/i);

  let guestRooms = null;
  // Prefer Total guest rooms only — "Guest Rooms" label often hits a tab with wrong sibling
  const guestLabel = pickCventHtmlLabelValue(text, "Total guest rooms");
  if (guestLabel && /^\d{1,5}$/.test(guestLabel)) {
    guestRooms = Number(guestLabel);
  }
  if (guestRooms == null) {
    const roomsPatterns = [
      /Total guest rooms\s*\n?\s*(\d{1,5})/i,
      /Total de habitaciones para huéspedes\s*\n?\s*(\d{1,5})/i,
      /Habitaciones para huéspedes\s*\n?\s*(\d{1,5})/i,
      /There are (\d{1,5}) guest rooms/i,
      /(\d{2,5})\s+spacious and stylish sleeping rooms/i,
      /(\d{2,5})\s+sleeping rooms/i,
    ];
    for (const re of roomsPatterns) {
      const m = plain.match(re) || text.match(re);
      if (m) {
        guestRooms = Number(m[1]);
        break;
      }
    }
  }
  // Reject obviously bad single-digit / tiny values from UI chrome
  if (guestRooms != null && guestRooms < 10) guestRooms = null;

  // Meeting rooms — NEVER map to Rooms / Keys
  let meetingRoomsCount = null;
  const meetLabel = pickCventHtmlLabelValue(text, "Meeting rooms");
  if (meetLabel && /^\d{1,4}$/.test(meetLabel)) {
    meetingRoomsCount = Number(meetLabel);
  }
  const meetingSpaceRaw =
    pickCventHtmlLabelValue(text, "Total meeting space") ||
    (() => {
      const m = plain.match(/Total meeting space\s*\n?\s*([^\n]{3,40})/i);
      return m?.[1] || null;
    })();
  const totalMeetingSpace = parseMeetingSpaceArea(meetingSpaceRaw);

  const street =
    pickProfile("address1") ||
    ldAddr.streetAddress ||
    null;
  const city =
    pickProfile("city") ||
    ldAddr.addressLocality ||
    null;
  const region =
    pickProfile("stateName") ||
    ldAddr.addressRegion ||
    null;
  const postalCode =
    pickProfile("postalCode") ||
    ldAddr.postalCode ||
    null;
  const countryName =
    pickProfile("countryName") ||
    ldAddr.addressCountry ||
    null;

  let address = buildAddressFromParts({
    street,
    city,
    region,
    postal: postalCode,
    country: countryName,
  });
  if (!address) {
    const titleEsc = title
      ? String(title).replace(/[.*+?^${}()|[\]\\]/g, "\\$&")
      : "";
    if (titleEsc) {
      const addrBlock = plain.match(
        new RegExp(`${titleEsc}\\s*\\n\\s*([^\\n]{12,180})\\s*\\n`, "i")
      );
      if (addrBlock && isPlausibleAddress(addrBlock[1])) {
        address = addrBlock[1].trim();
      }
    }
  }
  if (!address) {
    const lineHits = plain
      .split("\n")
      .map((l) => l.trim())
      .filter((l) => isPlausibleAddress(l));
    address =
      lineHits.find((l) => /,\s*[A-Za-zÁÉÍÓÚáéíóú]/.test(l)) ||
      lineHits[0] ||
      null;
  }

  const websiteRaw = pickProfile("website");
  const website = isNonCventHttpUrl(websiteRaw) ? String(websiteRaw).trim() : null;

  const listingTextRaw = pickProfile("listingText");
  const listingText =
    typeof listingTextRaw === "string" && listingTextRaw.trim().length >= 40
      ? listingTextRaw.trim()
      : null;

  const latRaw = pickProfile("latitude");
  const lngRaw = pickProfile("longitude");
  const latitude =
    typeof latRaw === "number" && Number.isFinite(latRaw) ? latRaw : null;
  const longitude =
    typeof lngRaw === "number" && Number.isFinite(lngRaw) ? lngRaw : null;

  const venueTypeIdRaw = pickProfile("venueTypeId");
  const venueTypeId =
    typeof venueTypeIdRaw === "string" && venueTypeIdRaw.trim()
      ? venueTypeIdRaw.trim().toUpperCase()
      : null;
  const propertyType = venueTypeId
    ? CVENT_VENUE_TYPE_TO_PROPERTY_TYPE[venueTypeId] || null
    : null;

  const airportDistance = parseAirportDistance(
    text,
    pickCventHtmlLabelValue(text, "Distance from airport")
  );

  const phoneLd = String(ld?.telephone || "").trim();
  const phone = phoneLd && phoneLd.length >= 7 ? phoneLd : null;

  const sourceIdRaw = pickProfile("sourceId");
  const sourceId =
    typeof sourceIdRaw === "string" && sourceIdRaw.trim()
      ? sourceIdRaw.trim()
      : null;

  const suitesLabel = pickCventHtmlLabelValue(text, "Suites");
  const suites =
    suitesLabel && /^\d{1,4}$/.test(suitesLabel) ? Number(suitesLabel) : null;

  // Built / Renovated — NEVER map to Opening Date / Renovation Date (forbidden).
  const builtYear = parseCventYearLabel(
    pickCventHtmlLabelValue(text, "Built") ||
      pickCventHtmlLabelValue(text, "Year built") ||
      plain.match(/\bBuilt\s*\n?\s*(\d{4})\b/i)?.[1]
  );
  const renovatedYear = parseCventYearLabel(
    pickCventHtmlLabelValue(text, "Renovated") ||
      pickCventHtmlLabelValue(text, "Year renovated") ||
      plain.match(/\bRenovated\s*\n?\s*(\d{4})\b/i)?.[1]
  );

  const amenitiesSourceText = extractCventAmenitiesSourceText(plain, text);
  const venueDescriptionRaw = pickProfile("venueDescription");
  const venueDescription =
    typeof venueDescriptionRaw === "string" &&
    venueDescriptionRaw.trim().length >= 40
      ? venueDescriptionRaw.trim()
      : null;
  const descriptionText = listingText || venueDescription || null;

  // URL path type (hotel/resort/boutique-hotel) as Property Type fallback
  const pathTypeMatch = String(sourceUrl || "").match(
    /\/venues\/[^/]+\/([a-z0-9-]+)\//i
  );
  const pathType = pathTypeMatch ? pathTypeMatch[1].toLowerCase() : null;
  let propertyTypeResolved = propertyType;
  if (!propertyTypeResolved && pathType === "resort") propertyTypeResolved = "Resort";
  if (!propertyTypeResolved && pathType === "hotel") propertyTypeResolved = "Hotel";
  if (!propertyTypeResolved && pathType === "boutique-hotel") {
    propertyTypeResolved = "Boutique Hotel";
  }

  const chain = String(chainMatch?.[1] || "").trim();
  const brand = String(brandMatch?.[1] || "").trim();
  const hay = normCventText(
    [chain, brand, title, descriptionText, plain.slice(0, 4000)]
      .filter(Boolean)
      .join(" ")
  );
  const choiceAffiliated = CHOICE_CHAIN_NEEDLES.some((n) =>
    hay.includes(normCventText(n))
  );

  const hasMeetingSignal = Boolean(
    (meetingRoomsCount != null && meetingRoomsCount > 0) ||
      totalMeetingSpace ||
      (descriptionText && /meeting room/i.test(descriptionText))
  );

  const venueUuid =
    String(sourceUrl || "").match(/venue-([a-f0-9-]{36})/i)?.[1]?.toLowerCase() ||
    null;

  return {
    sourceUrl,
    venueUuid,
    title: title || null,
    chain: chain || null,
    brand: brand || null,
    address: address || null,
    addressParts: {
      street: street ? String(street).trim() : null,
      city: city ? String(city).trim() : null,
      region: region ? String(region).trim() : null,
      postalCode: postalCode ? String(postalCode).trim() : null,
      country: countryName ? String(countryName).trim() : null,
    },
    guestRooms:
      guestRooms != null && Number.isFinite(guestRooms) ? guestRooms : null,
    /** Meeting-room count — must never write to Rooms / Keys. */
    meetingRoomsCount:
      meetingRoomsCount != null && Number.isFinite(meetingRoomsCount)
        ? meetingRoomsCount
        : null,
    totalMeetingSpace,
    hasMeetingSignal,
    website,
    listingText: descriptionText,
    amenitiesSourceText,
    builtYear,
    renovatedYear,
    phone,
    latitude,
    longitude,
    venueTypeId,
    pathType,
    propertyType: propertyTypeResolved,
    airportDistance,
    suites,
    sourceId,
    choiceAffiliated,
    parse_ok: Boolean(
      title ||
        chain ||
        guestRooms ||
        address ||
        website ||
        descriptionText ||
        amenitiesSourceText
    ),
  };
}

function parseCventYearLabel(raw) {
  const s = String(raw || "").trim();
  if (!s || s === "-" || /n\/?a|available|unknown/i.test(s)) return null;
  const m = s.match(/\b(19|20)\d{2}\b/);
  if (!m) return null;
  const y = Number(m[0]);
  if (!Number.isFinite(y) || y < 1850 || y > new Date().getFullYear() + 1) {
    return null;
  }
  return y;
}

/**
 * Pull amenities-looking lines from Cvent venue plain text / HTML labels.
 * Stored as Amenities - Source Text (raw) — never invent structured tags.
 */
export function extractCventAmenitiesSourceText(plain, html = "") {
  const lines = String(plain || "")
    .split("\n")
    .map((l) => l.trim())
    .filter(Boolean);
  const startIdx = lines.findIndex((l) =>
    /^(amenities|room features and guest services|guest services|facility amenities)$/i.test(
      l
    )
  );
  const collected = [];
  if (startIdx >= 0) {
    for (let i = startIdx + 1; i < Math.min(lines.length, startIdx + 40); i++) {
      const l = lines[i];
      if (
        /^(meeting|guest rooms|location|affiliations|faqs?|overview|chain|brand|built|renovated|distance from airport|total meeting|explore)/i.test(
          l
        )
      ) {
        break;
      }
      if (l.length < 2 || l.length > 120) continue;
      if (/no information available|no response/i.test(l)) continue;
      collected.push(l);
    }
  }
  // Label hits for common amenity rows
  for (const label of [
    "Room service",
    "Complimentary parking",
    "Parking in the area",
    "High-speed internet",
    "Wi-Fi",
    "Fitness center",
    "Pool",
    "Spa",
    "Restaurant",
  ]) {
    if (new RegExp(label, "i").test(html) || new RegExp(label, "i").test(plain)) {
      if (!collected.some((c) => c.toLowerCase() === label.toLowerCase())) {
        collected.push(label);
      }
    }
  }
  const uniq = [...new Set(collected)].slice(0, 60);
  if (!uniq.length) return null;
  return uniq.join("\n").slice(0, 8000);
}

/**
 * Fetch + parse one venue URL (cached).
 */
export async function fetchCventVenue(url, opts = {}) {
  const throttleMs = Number(opts.throttleMs ?? 1100);
  const useCache = opts.useCache !== false;
  const path = cacheKey("venue", url);
  if (useCache) {
    const hit = readCache(path);
    if (hit?.parsed) return { ...hit, from_cache: true };
  }

  const page = await fetchHtml(url);
  await sleep(throttleMs);
  if (!page.ok && page.bytes < 500) {
    const payload = {
      version: CVENT_VENUE_CLIENT_VERSION,
      fetched_at: new Date().toISOString(),
      url,
      ok: false,
      status: page.status,
      error: page.error || `http_${page.status}`,
      parsed: null,
    };
    if (useCache) writeCache(path, payload);
    return { ...payload, from_cache: false };
  }

  const parsed = parseCventVenueHtml(page.text, page.finalUrl || url);
  const payload = {
    version: CVENT_VENUE_CLIENT_VERSION,
    fetched_at: new Date().toISOString(),
    url: page.finalUrl || url,
    ok: true,
    status: page.status,
    bytes: page.bytes,
    parsed,
  };
  if (useCache) writeCache(path, payload);
  return { ...payload, from_cache: false };
}

/**
 * Discover URLs then fetch until first Choice-affiliated parse succeeds.
 * @param {object} input
 * @param {{ throttleMs?: number, useCache?: boolean, seedUrls?: string[] }} [opts]
 */
export async function resolveCventVenueForHotel(input = {}, opts = {}) {
  const seedUrls = Array.isArray(opts.seedUrls) ? opts.seedUrls : [];
  const disc = opts.skipDiscover
    ? { urls: [], slug_candidates: [], discover_log: [{ skipped: true }] }
    : await discoverCventVenueUrls(input, opts);
  const tried = [];
  const queue = [
    ...seedUrls,
    ...(disc.urls || []),
    ...(opts.skipDiscover ? [] : disc.slug_candidates || []),
  ];
  const seen = new Set();

  for (const url of queue) {
    if (!url || seen.has(url)) continue;
    seen.add(url);
    if (!/cvent\.com\/venues\//i.test(url)) continue;
    const fetched = await fetchCventVenue(url, opts);
    tried.push({
      url,
      ok: fetched.ok,
      status: fetched.status,
      choice: fetched.parsed?.choiceAffiliated || false,
      rooms: fetched.parsed?.guestRooms ?? null,
      title: fetched.parsed?.title || null,
    });
    if (fetched.ok && fetched.parsed?.choiceAffiliated) {
      return {
        ok: true,
        venue: fetched.parsed,
        sourceUrl: fetched.url || url,
        discover: disc,
        tried,
      };
    }
  }

  return {
    ok: false,
    reason: queue.length ? "no_choice_affiliated_venue" : "no_cvent_urls_discovered",
    venue: null,
    sourceUrl: null,
    discover: disc,
    tried,
  };
}
