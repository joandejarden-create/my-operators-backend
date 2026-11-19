/**
 * Hotel Explorer amenity strip display policy (ADP / verified attributes).
 * Visual presentation is a horizontal carousel (KGPV baseline) — not bordered cards.
 *
 * Version: he-amenity-display-v1
 */

export const HE_AMENITY_DISPLAY_VERSION = "he-amenity-display-v1";
export const HE_AMENITIES_DISPLAY_MAX = 20;
export const HE_AMENITIES_DISPLAY_TARGET_MIN = 12;

/** Higher = shown earlier when truncating to MAX. */
export const HE_AMENITY_DISPLAY_PRIORITY = Object.freeze({
  meetingEventSpace: 100,
  meetingRooms: 98,
  ballroom: 96,
  large_ballroom: 96,
  dining: 90,
  coopers_mill: 90,
  spa: 88,
  full_service_spa: 88,
  outdoorPool: 86,
  indoorPool: 85,
  pool: 84,
  fitnessCenter: 82,
  fitness_center_24hr: 82,
  peloton: 80,
  m_club: 78,
  executiveLounge: 78,
  concierge: 76,
  businessCenter: 74,
  freeParking: 72,
  on_site_parking: 72,
  valetParking: 71,
  airportShuttle: 70,
  metro_shuttle: 69,
  dc_metro_access: 68,
  nih_access: 67,
  roomAccessibility: 66,
  room_accessibility: 66,
  beach: 64,
  beach_access: 64,
  private_beaches: 64,
  resort: 60,
  roomService: 55,
  room_service: 55,
  freeWifi: 54,
  free_wifi: 54,
  petsAllowed: 50,
  pet_friendly: 50,
  business_center: 74,
  concierge: 76,
  evCharging: 48,
  weddingServices: 46,
  kidsRecreation: 40,
});

const GEO_OR_BRAND_ONLY_RE =
  /\b(location|brand|loyalty|positioning|marriott hotels|marriott bonvoy|hilton honors|ihg one rewards|full service hotel|urban lifestyle|urban hotel|island resort|resort experience)\b/i;

/**
 * Attributes that are geo / brand / marketing tags — not guest amenity icons.
 */
export function isNonGuestAmenityLabel(label, id = null) {
  const key = String(id || label || "")
    .toLowerCase()
    .replace(/_/g, " ")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
  if (!key) return true;
  if (
    /^(brand|loyalty|location|market|submarket|city|state|country|full service|full service hotel)$/.test(
      key
    )
  ) {
    return true;
  }
  if (GEO_OR_BRAND_ONLY_RE.test(key) && !/\b(access|shuttle|parking|metro|airport|nih)\b/.test(key)) {
    return true;
  }
  if (/^(bethesda md|pooks hill|montgomery county|times square location|mexico city location)\b/.test(key)) {
    return true;
  }
  return false;
}

export function amenityDisplayPriority(item = {}) {
  const id = String(item.id || item.attributeKey || item.key || "").trim();
  if (id && HE_AMENITY_DISPLAY_PRIORITY[id] != null) return HE_AMENITY_DISPLAY_PRIORITY[id];
  const snake = id.replace(/([a-z])([A-Z])/g, "$1_$2").toLowerCase();
  if (snake && HE_AMENITY_DISPLAY_PRIORITY[snake] != null) {
    return HE_AMENITY_DISPLAY_PRIORITY[snake];
  }
  const label = String(item.label || item.name || "").toLowerCase();
  if (/meeting|event|ballroom/.test(label)) return 95;
  if (/restaurant|dining|bar|lounge/.test(label)) return 88;
  if (/spa/.test(label)) return 86;
  if (/pool/.test(label)) return 84;
  if (/fitness|peloton|gym/.test(label)) return 82;
  if (/club|concierge/.test(label)) return 76;
  if (/business/.test(label)) return 74;
  if (/park/.test(label)) return 70;
  if (/access|shuttle|metro|airport|nih/.test(label)) return 66;
  if (/accessib/.test(label)) return 64;
  return 30;
}

/**
 * Deduplicate, filter non-amenities, sort by priority, cap at MAX.
 * Never fabricates amenities to reach TARGET_MIN.
 *
 * @returns {{ displayed: object[], overflow: object[], coverage: object }}
 */
/**
 * Collapse near-duplicate amenity labels (HPC short tags vs ADP long labels).
 */
export function amenityDedupeKey(label) {
  let s = String(label || "")
    .toLowerCase()
    .replace(/['’]/g, "")
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
  s = s
    .replace(/\bwashington dc\b/g, "dc")
    .replace(/\bcomplimentary\b/g, "")
    .replace(/\bon[\s-]?site\b/g, "onsite")
    .replace(/\b24[\s-]?hour\b/g, "24hr")
    .replace(/\b24hr\b/g, "24hr");
  const tokens = s
    .split(/\s+/)
    .filter((t) => t && !["and", "the", "a", "an", "level", "service", "rooms"].includes(t));
  if (
    tokens.includes("coopers") ||
    (tokens.includes("cooper") && tokens.includes("mill"))
  ) {
    return "coopers_mill";
  }
  if (tokens.includes("peloton")) return "peloton";
  if (tokens.includes("fitness") && (tokens.includes("24hr") || tokens.includes("gym"))) {
    return "fitness_24hr";
  }
  if (tokens.includes("fitness")) return "fitness_center";
  if (tokens.includes("metro") && tokens.includes("shuttle")) return "metro_shuttle";
  if (tokens.includes("metro") && tokens.includes("access")) return "metro_access";
  if (tokens.includes("m") && tokens.includes("club")) return "m_club";
  if (tokens.includes("ballroom")) return "ballroom";
  if (
    tokens.includes("meeting") &&
    (tokens.includes("event") || tokens.includes("space") || tokens.includes("room"))
  ) {
    return "meeting_space";
  }
  if (tokens.includes("wifi") || (tokens.includes("wi") && tokens.includes("fi"))) {
    return "wifi";
  }
  if (tokens.includes("pet") || tokens.includes("pets")) return "pets";
  if (tokens.includes("parking")) return "parking";
  if (tokens.includes("business") && tokens.includes("center")) return "business_center";
  if (tokens.includes("room") && tokens.includes("service")) return "room_service";
  if (tokens.includes("accessib") || tokens.includes("accessible") || tokens.includes("ada")) {
    return "accessibility";
  }
  if (tokens.includes("concierge") && !tokens.includes("club")) return "concierge";
  return tokens.slice().sort().join("_");
}

/**
 * Union verified + HPC/display amenity sources (dedupe by human label).
 * Verified rows win on collision. Never fabricates amenities.
 */
export function mergeAmenityDisplaySources(sources = []) {
  const seen = new Map();
  for (const list of sources) {
    for (const raw of Array.isArray(list) ? list : []) {
      if (!raw) continue;
      const label = String(raw.label || raw.name || "").trim();
      const id = raw.id || raw.attributeKey || raw.key || null;
      if (!label) continue;
      if (isNonGuestAmenityLabel(label, id)) continue;
      if (
        raw.factKind === "UNSUPPORTED_CLAIM" ||
        raw.factKind === "INFERRED_SUITABILITY" ||
        raw.factKind === "REALITY_GAP_RECOGNITION" ||
        raw.factKind === "POSITIONING"
      ) {
        continue;
      }
      const key = amenityDedupeKey(label);
      if (!key) continue;
      if (seen.has(key)) {
        const prev = seen.get(key);
        // Prefer rows that already carry verification / source URL.
        if (!prev.sourceUrl && raw.sourceUrl) {
          seen.set(key, { ...prev, sourceUrl: raw.sourceUrl });
        }
        if (!prev.verificationStatus && (raw.verificationStatus || raw.factKind)) {
          seen.set(key, {
            ...seen.get(key),
            verificationStatus: raw.verificationStatus || raw.factKind,
          });
        }
        continue;
      }
      seen.set(key, {
        id: id || null,
        label,
        verificationStatus: raw.verificationStatus || raw.factKind || null,
        sourceUrl: raw.sourceUrl || null,
        lastVerified: raw.lastVerified || raw.lastVerifiedAt || null,
        attributeKey: raw.attributeKey || id || null,
      });
    }
  }
  return [...seen.values()];
}

/**
 * Research-completeness assessment for amenity coverage (HE-QL-030).
 * Flags undercoverage for research — never encourages fabrication.
 */
export function assessAmenityCoverageResearchNeed(coverage = {}, opts = {}) {
  const targetMin =
    Number.isFinite(coverage.targetMin) && coverage.targetMin > 0
      ? coverage.targetMin
      : HE_AMENITIES_DISPLAY_TARGET_MIN;
  const verifiedCount = Number(coverage.verifiedCount) || 0;
  const shortfall = Math.max(0, targetMin - verifiedCount);
  const belowTargetMin = verifiedCount < targetMin;
  const hotelId = opts.hotelId || null;
  const hotelName = opts.hotelName || null;
  return {
    version: "he-amenity-coverage-research-v1",
    hotelId,
    hotelName,
    verifiedCount,
    targetMin,
    max: Number.isFinite(coverage.max) ? coverage.max : HE_AMENITIES_DISPLAY_MAX,
    belowTargetMin,
    shortfall,
    researchTrigger: belowTargetMin,
    status: belowTargetMin ? "RESEARCH_GAP" : "COVERAGE_OK",
    customerFacingCompletenessIndicator: false,
    guidance: belowTargetMin
      ? "Investigate ADP catalog, HPC amenities, and first-party brand materials for additional verified guest attributes. Do not invent amenities to meet the minimum."
      : "Verified amenity coverage meets the research target minimum.",
  };
}

export function selectAmenitiesForHotelExplorerDisplay(items = [], opts = {}) {
  const max = Number.isFinite(opts.max) ? opts.max : HE_AMENITIES_DISPLAY_MAX;
  const targetMin = Number.isFinite(opts.targetMin)
    ? opts.targetMin
    : HE_AMENITIES_DISPLAY_TARGET_MIN;
  const merged = mergeAmenityDisplaySources([items]);
  const cleaned = merged.map((row) => ({
    ...row,
    priority: amenityDisplayPriority(row),
  }));
  cleaned.sort((a, b) => b.priority - a.priority || a.label.localeCompare(b.label));
  const displayed = cleaned.slice(0, Math.max(0, max));
  const overflow = cleaned.slice(Math.max(0, max));
  const coverage = {
    verifiedCount: cleaned.length,
    displayedCount: displayed.length,
    overflowCount: overflow.length,
    targetMin,
    max,
    belowTargetMin: cleaned.length < targetMin,
    shortfall: Math.max(0, targetMin - cleaned.length),
  };
  return {
    displayed,
    overflow,
    coverage,
    research: assessAmenityCoverageResearchNeed(coverage, opts),
  };
}
