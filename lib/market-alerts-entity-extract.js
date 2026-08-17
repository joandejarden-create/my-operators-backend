import { isUsableEntityName, inferPublisherTokens } from "./market-alerts-qualification-gate.js";
import { isGeographyOnlyLabel } from "./market-alerts-geo-keywords.js";

/** Longer / more specific tokens first. */
const BRAND_TOKENS = [
  "JW Marriott",
  "Courtyard by Marriott",
  "Residence Inn",
  "SpringHill Suites",
  "Fairfield Inn",
  "AC Hotels",
  "AC Hotel",
  "Moxy Hotels",
  "Moxy",
  "Noted Collection",
  "Garner",
  "Signia",
  "Autograph Collection",
  "Tribute Portfolio",
  "Design Hotels",
  "Moxy Hotels",
  "Four Points",
  "St. Regis",
  "W Hotels",
  "The Ritz-Carlton",
  "Ritz-Carlton",
  "Edition",
  "Marriott",
  "Hilton Garden Inn",
  "Hampton by Hilton",
  "Hampton Inn",
  "DoubleTree",
  "Embassy Suites",
  "Homewood Suites",
  "Home2 Suites",
  "Curio Collection",
  "Tapestry Collection",
  "LXR Hotels",
  "Waldorf Astoria",
  "Conrad Hotels",
  "Canopy by Hilton",
  "Tempo by Hilton",
  "Spark by Hilton",
  "Hilton",
  "Holiday Inn Express",
  "Holiday Inn",
  "Crowne Plaza",
  "InterContinental",
  "Hotel Indigo",
  "Kimpton",
  "voco",
  "Six Senses",
  "Regent",
  "IHG",
  "Park Hyatt",
  "Grand Hyatt",
  "Hyatt Regency",
  "Hyatt Place",
  "Hyatt House",
  "Andaz",
  "Thompson Hotels",
  "Destination by Hyatt",
  "JdV by Hyatt",
  "Hyatt",
  "Wyndham Grand",
  "Wyndham Garden",
  "Trademark Collection",
  "La Quinta",
  "Days Inn",
  "Super 8",
  "Microtel",
  "Travelodge",
  "Wyndham",
  "Radisson Collection",
  "Radisson Blu",
  "Radisson RED",
  "Radisson",
  "Choice Hotels",
  "Comfort Inn",
  "Comfort Suites",
  "Quality Inn",
  "Sleep Inn",
  "Cambria",
  "Ascend Hotel Collection",
  "Ascend",
  "Best Western",
  "Aloft",
  "Element",
  "Westin",
  "Sheraton",
  "Le Méridien",
  "Le Meridien",
  "Renaissance",
  "Gaylord",
  "Omni Hotels",
  "Omni",
  "Four Seasons",
  "Mandarin Oriental",
  "Shangri-La",
  "Accor",
  "Sofitel",
  "Novotel",
  "Mercure",
  "ibis",
  "Fairmont",
  "Raffles",
  "Swissôtel",
  "Swissotel",
  "Pullman",
  "Mövenpick",
  "Movenpick",
  "Hard Rock",
  "Virgin Hotels",
  "CitizenM",
  "Standard Hotels",
  "1 Hotels",
  "Aman",
  "Rosewood",
  "Peninsula",
  "Belmond",
];

const OPERATOR_TOKENS = [
  "Aimbridge",
  "Highgate",
  "Host Hotels",
  "Hilton Worldwide",
  "Marriott International",
  "IHG Hotels",
  "Hyatt Hotels",
  "Wyndham Hotels",
  "Choice Hotels",
  "Accor",
  "Minor Hotels",
  "Playa Hotels",
  "Playa Resorts",
  "Remington",
  "White Lodging",
  "Pyramid Hotel Group",
  "Crestline",
  "Davidson Hospitality",
  "HEI Hotels",
  "Pacifica Hotels",
  "Driftwood Hospitality",
  "Arbor Lodging",
  "Hotel Equities",
  "McKibbon Hospitality",
  "McKibbon",
  "Atlantis",
  "Four Seasons Hotels and Resorts",
];

const OWNER_HINT_RE =
  /\b(?:owned by|owner(?:ship)?(?: of)?|developer(?: of)?|developed by|acquired by|sold (?:by|to)|buyer|seller)\s+([A-Z][A-Za-z0-9&.'\-]+(?:\s+[A-Z][A-Za-z0-9&.'\-]+){0,5})/;

const ROOMS_RE =
  /\b(\d{2,4})\s*[- ]?(?:key|keys|room|rooms|guest rooms?|keys?)\b/i;

const HOTEL_IN_LOCATION_RE =
  /\b(?:Hotel|Resort|Inn|Lodge|Suites|Palace|Tower)\s+in\s+([A-Z][A-Za-z0-9.'\-]+(?:\s+[A-Z][A-Za-z0-9.'\-]+){0,4}(?:,\s*[A-Z]{2})?)\b/;

const NAMED_HOTEL_RE =
  /\b((?:The\s+)?[A-Z][A-Za-z0-9&.'\-]+(?:\s+[A-Z][A-Za-z0-9&.'\-]+){0,5}\s+(?:Hotel|Resort|Inn|Suites|Lodge|Palace|Tower))\b/;

const HOTEL_PHRASE_RE = NAMED_HOTEL_RE;

const HEADLINE_FRAGMENT_RE =
  /\b(signs?|signed|brings?|announced|partners? with|buys? its first|lands?|landed)\b/i;

const FOR_SALE_HOTEL_RE =
  /\b((?:The\s+)?[A-Z][A-Za-z0-9&.'\-]+(?:\s+[A-Z][A-Za-z0-9&.'\-]+){0,5})\s+(?:Hotel|Resort|Inn|Suites).{0,40}\bfor sale\b/i;

/**
 * @param {string} text
 * @param {string[]} tokens
 * @returns {string|null}
 */
function findFirstToken(text, tokens) {
  if (!text) return null;
  for (const token of tokens) {
    const re = new RegExp(`\\b${escapeRegExp(token)}\\b`, "i");
    if (re.test(text)) return token;
  }
  return null;
}

export function hasKnownBrandToken(text) {
  return !!findFirstToken(text, BRAND_TOKENS);
}

function escapeRegExp(s) {
  return String(s).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function slugKey(parts) {
  return parts
    .filter(Boolean)
    .map((p) =>
      String(p)
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "-")
        .replace(/^-|-$/g, "")
    )
    .filter(Boolean)
    .join("|");
}

const CITY_ANCHOR_RE =
  /\b(phoenix|scottsdale|austin|dallas|houston|miami|orlando|tampa|denver|seattle|boston|chicago|atlanta|nashville|charlotte|raleigh|san diego|los angeles|san francisco|las vegas|portland|minneapolis|detroit|cleveland|cincinnati|indianapolis|milwaukee|kansas city|salt lake city|san antonio|sacramento|pittsburgh|baltimore|philadelphia)\b/i;

/**
 * Stable correlation key for feed dedupe — especially office→hotel clusters with varying headlines.
 * @param {{ text?: string, eventType?: string|null, hotelProject?: string|null, brandInvolved?: string|null, ownerDeveloper?: string|null, rooms?: number|null }} input
 * @returns {string|null}
 */
export function inferCorrelationEntityKey(input = {}) {
  const text = String(input.text || "");
  const eventType = input.eventType || null;
  const hotelProject = input.hotelProject || null;
  const brandInvolved = input.brandInvolved || null;
  const ownerDeveloper = input.ownerDeveloper || null;
  const rooms = input.rooms ?? null;
  let resolvedRooms = rooms;
  if (!resolvedRooms) {
    const roomsMatch = text.match(/\b(\d{2,4})[-\s]?(?:room|key)s?\b/i);
    if (roomsMatch) {
      const n = parseInt(roomsMatch[1], 10);
      if (Number.isFinite(n) && n >= 20 && n <= 5000) resolvedRooms = n;
    }
  }

  const base = slugKey([
    hotelProject || brandInvolved || ownerDeveloper || null,
    resolvedRooms ? `${resolvedRooms}-keys` : null,
  ]);

  if (eventType === "Adaptive Reuse Proposal" && brandInvolved) {
    const officeToHotel =
      /\b(office|tower|building|vacant).{0,120}(?:hotel|resort|jw marriott|marriott|converting|transform|become|second act)\b/i.test(
        text
      );

    if (officeToHotel) {
      let metroAnchor = null;
      if (
        (/\bphoenix\b/i.test(text) && /\b(office|tower|arizona center)\b/i.test(text)) ||
        /\barizona center\b/i.test(text)
      ) {
        metroAnchor = "phoenix";
      } else {
        const cityAnchor = text.match(CITY_ANCHOR_RE)?.[1];
        if (cityAnchor && /\b(office|tower|building|vacant)\b/i.test(text)) {
          metroAnchor = cityAnchor.toLowerCase();
        }
      }

      if (metroAnchor) {
        return (
          slugKey([metroAnchor, brandInvolved, "office-to-hotel"]) ||
          base
        );
      }

      const propertyAnchor = text.match(/\b(phoenix office tower)\b/i)?.[1];
      if (propertyAnchor) {
        return (
          slugKey([propertyAnchor, brandInvolved, rooms ? `${rooms}-keys` : "office-to-hotel"]) ||
          base
        );
      }
    }
  }

  return base || null;
}

/**
 * Infer coarse asset/project stage from event + text.
 * @param {string|null} eventType
 * @param {string} text
 */
export function inferAssetProjectStage(eventType, text = "") {
  if (eventType === "Distress") return "Distressed";
  if (
    eventType === "Planning Approval" ||
    eventType === "Planning Application" ||
    eventType === "New Development" ||
    eventType === "Development Proposal" ||
    eventType === "Site Acquisition" ||
    eventType === "Adaptive Reuse Proposal" ||
    /\b(planned|planning|proposed|pipeline)\b/i.test(text)
  ) {
    return "Planning";
  }
  if (
    eventType === "Construction Start" ||
    /\b(under construction|construction|breaks? ground)\b/i.test(text)
  ) {
    return "Construction";
  }
  if (
    eventType === "Hotel For Sale" ||
    eventType === "Acquisition" ||
    eventType === "Sale" ||
    eventType === "Reflag" ||
    eventType === "Brand Signing" ||
    eventType === "Operator Appointment" ||
    eventType === "Operator Change" ||
    eventType === "Major Renovation" ||
    eventType === "Repositioning" ||
    eventType === "Management Agreement"
  ) {
    return "Operating";
  }
  return "Unknown";
}

/**
 * @param {{ title?: string, summary?: string, eventType?: string|null }} input
 */
export function extractMarketAlertEntities(input = {}) {
  const title = String(input.title || "");
  const summary = String(input.summary || "");
  const text = `${title} ${summary}`.trim();
  const eventType = input.eventType || null;
  const publishers = inferPublisherTokens(title, input.sourceName || input.source || "");

  let rooms = null;
  const roomsMatch = text.match(ROOMS_RE);
  if (roomsMatch) {
    const n = parseInt(roomsMatch[1], 10);
    if (Number.isFinite(n) && n >= 20 && n <= 5000) rooms = n;
  }

  let hotelProject = null;
  const forSale = title.match(FOR_SALE_HOTEL_RE) || text.match(FOR_SALE_HOTEL_RE);
  if (forSale) {
    hotelProject = forSale[1].trim();
  }
  if (!hotelProject) {
    const inLoc = title.match(HOTEL_IN_LOCATION_RE) || text.match(HOTEL_IN_LOCATION_RE);
    if (inLoc && !isGeographyOnlyLabel(inLoc[1].trim())) {
      hotelProject = inLoc[1].trim();
    }
  }
  if (!hotelProject) {
    let searchTitle = title;
    for (const p of publishers) {
      searchTitle = searchTitle.replace(new RegExp(`\\s+[-–|]\\s+${escapeRegExp(p)}\\s*$`, "i"), "");
    }
    const phrase = searchTitle.match(NAMED_HOTEL_RE) || text.match(NAMED_HOTEL_RE);
    if (phrase) hotelProject = phrase[1].trim();
  }
  if (hotelProject && !isUsableEntityName(hotelProject, { publishers })) {
    hotelProject = null;
  }
  if (hotelProject && HEADLINE_FRAGMENT_RE.test(hotelProject)) {
    hotelProject = null;
  }

  const brandInvolved = findFirstToken(text, BRAND_TOKENS);
  const operatorInvolved = findFirstToken(text, OPERATOR_TOKENS);

  let ownerDeveloper = null;
  const ownerMatch = text.match(OWNER_HINT_RE);
  if (ownerMatch) {
    ownerDeveloper = ownerMatch[1].trim().replace(/[.,;:]+$/, "");
    if (ownerDeveloper.length < 3 || ownerDeveloper.length > 80) ownerDeveloper = null;
  }

  const assetProjectStage = inferAssetProjectStage(eventType, text);
  const entityKey = inferCorrelationEntityKey({
    text,
    eventType,
    hotelProject,
    brandInvolved,
    ownerDeveloper,
    rooms,
  });

  return {
    hotelProject: hotelProject || null,
    ownerDeveloper: ownerDeveloper || null,
    brandInvolved: brandInvolved || null,
    operatorInvolved: operatorInvolved || null,
    rooms,
    assetProjectStage,
    entityKey: entityKey || null,
  };
}
