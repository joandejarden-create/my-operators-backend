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
  valetParking: 71,
  airportShuttle: 70,
  dc_metro_access: 68,
  nih_access: 67,
  roomAccessibility: 66,
  beach: 64,
  beach_access: 64,
  private_beaches: 64,
  resort: 60,
  roomService: 55,
  petsAllowed: 50,
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
export function selectAmenitiesForHotelExplorerDisplay(items = [], opts = {}) {
  const max = Number.isFinite(opts.max) ? opts.max : HE_AMENITIES_DISPLAY_MAX;
  const targetMin = Number.isFinite(opts.targetMin)
    ? opts.targetMin
    : HE_AMENITIES_DISPLAY_TARGET_MIN;
  const list = Array.isArray(items) ? items : [];
  const seen = new Set();
  const cleaned = [];
  for (const raw of list) {
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
    const dedupeKey = String(label)
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "_");
    if (seen.has(dedupeKey)) continue;
    seen.add(dedupeKey);
    cleaned.push({
      id: id || null,
      label,
      verificationStatus: raw.verificationStatus || raw.factKind || null,
      sourceUrl: raw.sourceUrl || null,
      lastVerified: raw.lastVerified || raw.lastVerifiedAt || null,
      attributeKey: raw.attributeKey || id || null,
      priority: amenityDisplayPriority({ id, label }),
    });
  }
  cleaned.sort((a, b) => b.priority - a.priority || a.label.localeCompare(b.label));
  const displayed = cleaned.slice(0, Math.max(0, max));
  const overflow = cleaned.slice(Math.max(0, max));
  return {
    displayed,
    overflow,
    coverage: {
      verifiedCount: cleaned.length,
      displayedCount: displayed.length,
      overflowCount: overflow.length,
      targetMin,
      max,
      belowTargetMin: cleaned.length < targetMin,
      shortfall: Math.max(0, targetMin - cleaned.length),
    },
  };
}
