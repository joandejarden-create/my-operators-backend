/**
 * Deepened independent field extraction from official property HTML.
 * Never uses legacy values. Absence ≠ No for amenities.
 */

/**
 * @param {string} html
 * @param {string} [url]
 */
export function extractDeepOfficialPageSignals(html, url = "") {
  const text = String(html || "");
  const lower = text.toLowerCase();

  /** @type {object} */
  const out = {
    rooms: null,
    phone: null,
    openDateHint: null,
    openDateKind: null, // announced | expected | actual | now_open
    latitude: null,
    longitude: null,
    amenitiesMentioned: [],
    amenitiesExplicitNo: [],
    meetingFacilities: null, // Yes | Unknown
    fbMentioned: null,
    spaWellness: null,
    poolMentioned: null,
    managementHint: null,
    ownerHint: null,
  };

  // Keys — do not infer from room-type card counts or media filenames (e.g. rooms1507.jpg)
  const roomPatterns = [
    /"numberOfRooms"\s*:\s*(\d{2,4})/i,
    /"roomCount"\s*:\s*(\d{2,4})/i,
    /(\d{2,4})\s+(?:guest\s+)?rooms?\b/i,
    /\brooms?\s*:\s*(\d{2,4})\b/i,
    /total\s+of\s+(\d{2,4})\s+rooms/i,
  ];
  for (const re of roomPatterns) {
    const m = text.match(re);
    if (m) {
      const n = Number(m[1]);
      if (n >= 20 && n <= 2000) {
        out.rooms = n;
        break;
      }
    }
  }

  const phoneMatch = text.match(/tel:([+\d][\d\-.\s()]{7,})/i);
  if (phoneMatch) out.phone = phoneMatch[1].replace(/\s+/g, " ").trim();

  // Opening date kinds
  if (/now open|newly opened|grand opening/i.test(text)) {
    out.openDateKind = "now_open";
  }
  const actualOpen = text.match(
    /(?:opened|opened its doors)\s+(?:on\s+|in\s+)?((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}|\d{4})/i
  );
  const expectedOpen = text.match(
    /(?:expected to open|scheduled to open|opening in|opens in)\s+((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}|\d{4}|Q[1-4]\s*\d{4})/i
  );
  const announced = text.match(/"openingDate"\s*:\s*"([^"]+)"/i);
  if (actualOpen) {
    out.openDateHint = actualOpen[1];
    out.openDateKind = out.openDateKind || "actual";
  } else if (expectedOpen) {
    out.openDateHint = expectedOpen[1];
    out.openDateKind = "expected";
  } else if (announced) {
    out.openDateHint = announced[1];
    out.openDateKind = "announced";
  }

  // Coordinates from JSON-LD / meta / maps
  const latLng =
    text.match(/"latitude"\s*:\s*(-?\d+\.?\d*)\s*,\s*"longitude"\s*:\s*(-?\d+\.?\d*)/i) ||
    text.match(/content="(-?\d+\.\d+),\s*(-?\d+\.\d+)"[^>]*(?:geo|icbm)/i) ||
    text.match(/@(-?\d+\.\d+),(-?\d+\.\d+)/);
  if (latLng) {
    const lat = Number(latLng[1]);
    const lng = Number(latLng[2]);
    if (Math.abs(lat) <= 90 && Math.abs(lng) <= 180) {
      out.latitude = lat;
      out.longitude = lng;
    }
  }

  // Amenities — Yes only when explicitly mentioned; never invent No from absence
  const amenityRules = [
    [/pool|swimming pool|piscina/i, "Pool"],
    [/spa|wellness|sauna|steam room/i, "Spa / Wellness"],
    [/fitness|gym|fitness center|fitness centre/i, "Fitness"],
    [/restaurant|dining|bistro|cafe|café/i, "Restaurant / F&B"],
    [/bar\b|lounge|rooftop bar/i, "Bar / Lounge"],
    [/meeting|conference|event space|banquet|ballroom/i, "Meeting / Events"],
    [/parking|valet/i, "Parking"],
    [/wifi|wi-fi|wireless internet/i, "Wi-Fi"],
    [/pet.?friendly|pets allowed/i, "Pet Friendly"],
    [/business center|business centre/i, "Business Center"],
  ];
  for (const [re, label] of amenityRules) {
    if (re.test(lower)) out.amenitiesMentioned.push(label);
  }
  out.amenitiesMentioned = [...new Set(out.amenitiesMentioned)];

  if (/no pool|pool not available/i.test(lower)) out.amenitiesExplicitNo.push("Pool");
  if (/meeting|conference|event space|banquet|ballroom/i.test(lower)) out.meetingFacilities = "Yes";
  else out.meetingFacilities = "Unknown";
  if (/restaurant|dining|bistro|cafe|café|bar\b/i.test(lower)) out.fbMentioned = "Yes";
  else out.fbMentioned = "Unknown";
  if (/spa|wellness/i.test(lower)) out.spaWellness = "Yes";
  else out.spaWellness = "Unknown";
  if (/pool|piscina/i.test(lower)) out.poolMentioned = "Yes";
  else out.poolMentioned = "Unknown";

  // Operator/owner — only explicit language; never infer from brand
  const mgmt = text.match(
    /(?:managed by|management by|operated by|under management of)\s+([A-Z][A-Za-z0-9 &.'-]{2,80})/
  );
  if (mgmt) out.managementHint = mgmt[1].trim().replace(/\s+/g, " ").slice(0, 120);
  const own = text.match(/(?:owned by|owner[:\s]+)\s+([A-Z][A-Za-z0-9 &.'-]{2,80})/);
  if (own) out.ownerHint = own[1].trim().replace(/\s+/g, " ").slice(0, 120);

  out.sourceUrl = url || null;
  return out;
}

/**
 * Research priority ladders (documentation + runtime reference).
 */
export const FIELD_RESEARCH_PLANS = Object.freeze({
  rooms: {
    field: "rooms",
    never: ["infer from room-type listing counts", "legacy census rooms"],
    priority: [
      "official property page",
      "official development page",
      "owner announcement",
      "operator page",
      "official press release",
      "high-quality trade source (corroboration)",
    ],
  },
  openDate: {
    field: "Open Date",
    kinds: ["announced", "expected", "actual", "now_open"],
    priority: [
      "official opening announcement",
      "official hotel page",
      "parent/company release",
      "owner/operator release",
      "reliable dated trade press",
    ],
  },
  managementCompany: {
    field: "Management Company",
    never: ["infer operator from brand affiliation"],
    priority: [
      "operator official portfolio",
      "owner announcement",
      "operator press release",
      "brand development announcement",
      "management agreement announcement",
    ],
  },
  owner: {
    field: "Owner",
    escalate: ["opaque UBO"],
    priority: [
      "owner official source",
      "transaction announcement",
      "official development source",
      "first-party validation",
      "public corporate/government source where permitted",
    ],
  },
  amenities: {
    field: "Amenities",
    values: ["Yes", "No — Explicit", "Unknown", "Conflicting Evidence"],
    rule: "Absence from one page ≠ No",
  },
  coordinates: {
    fields: ["Latitude", "Longitude"],
    never: ["copy legacy lat/lng"],
    priority: ["official property coordinates", "permitted independent geocode of official address"],
  },
  marketSubmarket: {
    fields: ["Market", "Submarket"],
    rule: "Do not recreate STR market definitions from legacy; use Dealality-owned geography or Legal/Source Review Required",
  },
});
