/**
 * AI Demand Positioning — Response Parser.
 * Extracts property mentions, competitors, attributes, and sources from AI responses.
 */

import { extractAndResolveCompetitors } from "../intelligence/competitor-name-resolution.js";
import { subjectRegistryAliasesForProperty } from "../subject-presence/subject-entity-ids-v1.js";

/**
 * Check if target property is mentioned in a response.
 * Uses name variants (exact substring on normalized text) — no unrestricted fuzzy merge.
 * Rejects matches that are substrings of profile.identityConfusableExclusions (distinct entities).
 */
export function detectPropertyMention(response, propertyProfile) {
  if (!response) return { mentioned: false, position: null, context: null, matchedVariant: null };
  const text = normalizeSubjectHaystack(response);
  const nameVariants = buildNameVariants(propertyProfile);
  const confusable = (propertyProfile.identityConfusableExclusions || []).map((c) =>
    normalizeSubjectHaystack(c)
  );

  // Prefer longer variants first so "Bethesda Marriott Hotel" wins over shorter stems.
  const ordered = [...nameVariants].sort(
    (a, b) => normalizeSubjectHaystack(b).length - normalizeSubjectHaystack(a).length
  );

  for (const variant of ordered) {
    const needle = normalizeSubjectHaystack(variant);
    if (needle.length < 4) continue;
    let from = 0;
    while (from < text.length) {
      const idx = text.indexOf(needle, from);
      if (idx === -1) break;
      const spannedByConfusable = confusable.some((c) => {
        if (!c || c.length <= needle.length) return false;
        let cFrom = 0;
        while (cFrom < text.length) {
          const cIdx = text.indexOf(c, cFrom);
          if (cIdx === -1) break;
          if (idx >= cIdx && idx + needle.length <= cIdx + c.length) return true;
          cFrom = cIdx + 1;
        }
        return false;
      });
      if (spannedByConfusable) {
        from = idx + needle.length;
        continue;
      }
      // Prefer original-case context from raw response
      const rawLower = response.toLowerCase();
      const rawIdx = rawLower.indexOf(String(variant).toLowerCase());
      const useIdx = rawIdx !== -1 ? rawIdx : Math.min(idx, response.length - 1);
      const contextStart = Math.max(0, useIdx - 50);
      const contextEnd = Math.min(response.length, useIdx + String(variant).length + 100);
      const context = response.slice(contextStart, contextEnd).trim();
      const position = detectPosition(response, useIdx >= 0 ? useIdx : 0);
      return { mentioned: true, position, context, matchedVariant: variant };
    }
  }
  return { mentioned: false, position: null, context: null, matchedVariant: null };
}

/** Normalize for subject substring matching: lowercase, &→and, collapse punctuation/space. */
export function normalizeSubjectHaystack(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[’']/g, "")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Find needle in a normalized haystack only on token boundaries
 * (start/end of string or whitespace). Returns index or -1.
 */
export function findTokenBoundaryIndex(haystack, needle, fromIndex = 0) {
  const text = String(haystack || "");
  const n = String(needle || "");
  if (!n) return -1;
  let from = Math.max(0, Number(fromIndex) || 0);
  while (from <= text.length - n.length) {
    const idx = text.indexOf(n, from);
    if (idx === -1) return -1;
    const beforeOk = idx === 0 || /\s/.test(text[idx - 1]);
    const afterOk = idx + n.length === text.length || /\s/.test(text[idx + n.length]);
    if (beforeOk && afterOk) return idx;
    from = idx + 1;
  }
  return -1;
}

/**
 * Governed subject name variants — exact/alias only (no fuzzy competitor merges).
 */
/** Geographic / market labels that must never stand alone as subject mentions. */
const GEOGRAPHIC_ONLY_BLOCKLIST = new Set(
  [
    "falls church",
    "mexico city",
    "ciudad de mexico",
    "ciudad de méxico",
    "cdmx",
    "santo domingo",
    "san diego",
    "new york",
    "times square",
    "washington",
    "washington dc",
    "arlington",
    "tampa",
    "charlotte",
    "monterrey",
    "munich",
    "munchen",
    "münchen",
    "mallorca",
    "palma",
    "geneva",
    "london",
    "bogota",
    "bogotá",
    "kansas city",
    "boca raton",
    "fairview park",
  ].map((s) => s.toLowerCase())
);

const BRAND_OR_PRODUCT_TOKEN =
  /\b(marriott|hilton|hyatt|westin|sheraton|renaissance|radisson|nobu|yotel|edition|autograph|curio|tapestry|st\.?\s*regis|ritz|carlton|jw|ac\s+hotel|courtyard|fairfield|aloft|element|moxy|tribute|four\s+points|hotel|resort|inn|suites|collection|embassy|hampton|homewood|conrad|waldorf|intercontinental|kimpton|mandarin|four\s+seasons|fairmont|sofitel|novotel|ibis|holiday\s+inn|crowne|plaza|wyndham|ramada|melia|meliá|nh\s+collection|eurostars|hesperia)\b/i;

function isGeographicOnlyVariant(candidate, profile) {
  const c = normalizeSubjectHaystack(candidate);
  if (!c || c.length < 4) return true;
  if (GEOGRAPHIC_ONLY_BLOCKLIST.has(c)) return true;
  const city = normalizeSubjectHaystack(profile?.city || "");
  const market = normalizeSubjectHaystack(profile?.market || "");
  if (city && c === city) return true;
  if (market && c === market) return true;
  // Two-token city/market phrases without a brand/product token are not hotel IDs.
  const tokens = c.split(/\s+/).filter(Boolean);
  if (tokens.length <= 2 && !BRAND_OR_PRODUCT_TOKEN.test(c)) return true;
  return false;
}

export function buildNameVariants(profile) {
  const variants = [];
  const addDerived = (v) => {
    const t = String(v || "").trim();
    if (t.length < 4) return;
    if (isGeographicOnlyVariant(t, profile)) return;
    variants.push(t);
  };
  /** Explicit profile/registry aliases — allow property codes; still block pure city labels. */
  const addExplicit = (v) => {
    const t = String(v || "").trim();
    if (t.length < 4) return;
    const norm = normalizeSubjectHaystack(t);
    if (GEOGRAPHIC_ONLY_BLOCKLIST.has(norm)) return;
    const city = normalizeSubjectHaystack(profile?.city || "");
    const market = normalizeSubjectHaystack(profile?.market || "");
    if ((city && norm === city) || (market && norm === market)) return;
    variants.push(t);
  };
  const add = addDerived;

  addExplicit(profile.name);
  if (profile.name?.includes("&")) addExplicit(profile.name.replace(/&/g, "and"));
  if (profile.name?.includes(" and ")) addExplicit(profile.name.replace(/\band\b/gi, "&"));

  for (const alias of profile.identityAliases || profile.aliases || []) addExplicit(alias);
  for (const s of profile.commonShorthand || []) addExplicit(s);
  for (const s of profile.brandShorthand || []) addExplicit(s);
  // Market entity registry subject aliases — scoped to THIS property only.
  for (const alias of subjectRegistryAliasesForProperty(profile)) addExplicit(alias);
  // Brand + neighborhood composites (identity canary BRAND_LOCATION_SHORTHAND)
  const brandShorthand = [...(profile.brandShorthand || [])].map((s) => String(s || "").trim()).filter(Boolean);
  const neighborhoodAliases = [...(profile.neighborhoodAliases || [])]
    .map((s) => String(s || "").trim())
    .filter(Boolean);
  for (const b of brandShorthand) {
    for (const n of neighborhoodAliases) add(`${b} ${n}`);
  }

  // Leading "The …"
  for (const v of [...variants]) {
    if (!/^the\s+/i.test(v)) add(`The ${v}`);
    else add(v.replace(/^the\s+/i, ""));
  }

  // "Hotel Phillips …" ↔ "The Phillips Hotel" / "Phillips Kansas City"
  const name = String(profile.name || "");
  const hotelLead = name.match(/^hotel\s+(.+)$/i);
  if (hotelLead) {
    const rest = hotelLead[1].replace(/,.*/, "").trim(); // drop affiliation clause
    add(`The ${rest.split(/\s+/).slice(0, 2).join(" ")} Hotel`);
    add(`The ${rest} Hotel`);
    add(rest);
    const firstToken = rest.split(/\s+/)[0];
    if (firstToken && firstToken.length >= 5) {
      add(`The ${firstToken} Hotel`);
      add(`${firstToken} Hotel`);
    }
  }

  const words = name.replace(/,/g, " ").split(/\s+/).filter(Boolean);
  const genericLead = new Set(["hotel", "the", "a", "an"]);
  const requiredLocationTokens = [];
  if (/times\s*square/i.test(name)) requiredLocationTokens.push("times square");
  if (/boca\s*raton/i.test(name)) requiredLocationTokens.push("boca raton");
  if (/kansas\s*city/i.test(name)) requiredLocationTokens.push("kansas city");
  if (/noho/i.test(name)) requiredLocationTokens.push("noho");

  function retainsRequiredLocation(candidate) {
    const c = normalizeSubjectHaystack(candidate);
    return requiredLocationTokens.every((tok) => c.includes(tok));
  }

  if (words.length > 2) {
    const two = words.slice(0, 2).join(" ");
    if ((!genericLead.has(words[0].toLowerCase()) || words[1]) && retainsRequiredLocation(two)) add(two);
    if (words.length > 3) {
      const three = words.slice(0, 3).join(" ");
      if (retainsRequiredLocation(three)) add(three);
    }
    const beforeComma = name.split(",")[0].trim().split(/\s+/).filter(Boolean);
    if (beforeComma.length >= 2) {
      const bc2 = beforeComma.slice(0, 2).join(" ");
      if (retainsRequiredLocation(bc2)) add(bc2);
    }
    if (beforeComma.length >= 3) {
      const bc3 = beforeComma.slice(0, 3).join(" ");
      if (retainsRequiredLocation(bc3)) add(bc3);
    }
    if (beforeComma.length >= 4 && retainsRequiredLocation(beforeComma.join(" "))) {
      add(beforeComma.join(" "));
    }
  }
  if (profile.affiliation) {
    add(`${profile.name}, ${profile.affiliation}`);
    const short = name.split(",")[0].trim();
    if (short) add(`${short}, ${profile.affiliation}`);
  }
  return [...new Set(variants.filter(Boolean))];
}

function detectPosition(response, mentionIndex) {
  const before = response.slice(0, mentionIndex);
  const numberedPattern = /(\d+)\.\s*$/;
  const match = before.match(numberedPattern);
  if (match) return parseInt(match[1], 10);

  const lines = before.split("\n");
  let numbered = 0;
  for (const line of lines) {
    if (/^\s*\d+[\.\)]\s/.test(line)) numbered++;
  }
  return numbered > 0 ? numbered + 1 : null;
}

/**
 * Extract competitor hotel names from response (resolved to specific property names).
 */
export function extractCompetitors(response, propertyProfile) {
  if (!response) return [];
  const market = propertyProfile?.market || "";
  return extractAndResolveCompetitors(response, propertyProfile, { market });
}

/**
 * Detect which property attributes AI recognizes in its response.
 * Keyword map is the eligibility gate for Core AI testing (see isAttributeEligibleForCoreAiTesting).
 */
export const ATTRIBUTE_DETECTION_KEYWORDS_V1 = {
    // Waterstone
    waterfront: ["waterfront", "water view", "intracoastal", "lake boca", "waterway"],
    marina: ["marina", "boat", "dock", "yacht", "boating"],
    watersports: ["paddleboard", "kayak", "jet ski", "water sport", "snorkel"],
    intracoastal_waterway: ["intracoastal", "waterway", "lake boca"],
    walking_distance_beach: ["walking distance", "walk to the beach", "steps from the beach", "near the beach"],
    near_mizner_park: ["mizner park", "mizner"],
    heated_outdoor_pool: ["heated pool", "outdoor pool", "pool"],
    pet_friendly: ["pet-friendly", "pet friendly", "dog-friendly", "dogs welcome"],
    soft_brand: ["soft brand", "curio collection", "independent feel", "individuality"],
    // Do not use bare "hilton" alone for recognition without honors/brand context —
    // competitor Hilton mentions must not certify subject Hilton Honors recognition.
    hilton_honors: ["hilton honors", "honors points", "hilton rewards", "curio collection"],
    meeting_space: ["meeting", "conference", "ballroom", "event space", "banquet"],
    event_space_outdoor: ["outdoor event", "terrace event", "outdoor terrace", "outdoor venue"],
    ballroom: ["ballroom", "atlantic ballroom"],
    boutique_feel: ["boutique", "intimate", "charming", "unique character"],
    panoramic_views: ["panoramic", "panoramic view", "sweeping view"],
    private_balconies: ["balcony", "private balcony", "private terrace"],
    ev_charging: ["ev charging", "electric vehicle", "ev station"],

    // Renaissance Times Square
    times_square_location: ["times square", "heart of times square", "two times square"],
    broadway_theater_district: ["broadway", "theater district", "theatre district", "shows"],
    midtown_manhattan: ["midtown", "midtown manhattan"],
    rooftop_bar: ["rooftop", "r lounge", "rooftop bar", "rooftop lounge"],
    times_square_views: ["times square view", "view of times square", "overlooking times square"],
    business_center: ["business center", "work station"],
    marriott_bonvoy: [
      "marriott bonvoy",
      "bonvoy",
      "marriott rewards",
      "marriott loyalty",
      "marriott hotels",
      "a marriott",
    ],
    walking_distance_broadway: ["walk to broadway", "walking distance to broadway", "steps from broadway", "near broadway"],
    near_central_park: ["central park", "near central park"],
    near_rockefeller_center: ["rockefeller", "rock center", "30 rock"],
    urban_lifestyle: ["urban", "lifestyle", "city lifestyle", "urban lifestyle"],
    design_forward: ["design", "modern design", "design-forward", "contemporary design", "stylish"],
    full_service: ["full-service", "full service"],
    concierge: ["concierge", "navigator"],

    // Cambridge Beaches
    private_beaches: ["private beach", "private beaches", "secluded beach", "own beach"],
    five_private_coves: ["five beach", "five cove", "5 beach", "five private", "multiple beach", "multiple cove"],
    cottage_style: ["cottage", "cottage colony", "cottage-style", "cottages"],
    oceanfront: ["oceanfront", "ocean front", "ocean view", "beachfront", "seaside"],
    full_service_spa: ["spa", "ocean spa", "spa treatment", "wellness center"],
    heated_infinity_pool: ["infinity pool", "heated pool", "pool"],
    tennis_courts: ["tennis", "tennis court"],
    kayaking: ["kayak", "kayaking"],
    snorkeling: ["snorkel", "snorkeling", "reef"],
    paddleboard: ["paddleboard", "paddle board", "stand-up paddle", "sup"],
    scuba_diving: ["scuba", "diving", "dive"],
    wedding_venue: ["wedding", "weddings", "wedding venue", "ceremony"],
    honeymoon_destination: ["honeymoon", "romantic getaway", "couples retreat"],
    adults_only: ["adults-only", "adults only", "no children", "adult only"],
    historic_property: ["historic", "heritage", "since 1947", "established", "storied"],
    bermuda_heritage: ["bermuda", "bermudian", "island heritage"],
    all_inclusive_option: ["all-inclusive", "all inclusive", "inclusive package"],
    island_resort: ["island resort", "island getaway", "tropical resort"],

    // JW Marriott / Westin Monterrey Valle
    monterrey_valle: [
      "monterrey valle",
      "valle oriente",
      "valle del campestre",
      "monterrey's valle",
      "in the valle",
    ],
    san_pedro_garza_garcia: [
      "san pedro garza",
      "san pedro",
      "garza garcía",
      "garza garcia",
    ],
    valle_del_campestre: ["valle del campestre", "campestre"],
    punto_valle: ["punto valle"],
    luxury: ["luxury", "luxury hotel", "luxury brand"],
    upper_upscale: ["upper-upscale", "upper upscale", "upscale"],
    jw_marriott: ["jw marriott", "j.w. marriott", "jw marriott hotel"],
    westin: ["westin", "the westin"],
    spa: ["spa", "spa by jw", "spa treatments"],
    heavenly_spa: ["heavenly spa", "heavenly bed", "westin spa"],
    outdoor_pool: ["outdoor pool", "al fresco pool", "open-air pool"],
    infinity_pool: ["infinity pool", "infinity-edge pool"],
    fitness_center: ["fitness center", "fitness centre", "gym", "workout"],
    fitness_center_24hr: [
      "24-hour fitness",
      "24 hour fitness",
      "24/7 fitness",
      "fitness center open 24",
    ],
    wellness: ["wellness", "wellbeing", "well-being", "wellness amenities", "wellness hotel"],
    large_ballroom: ["large ballroom", "ballroom", "grand ballroom", "1053", "1,053"],
    executive_lounge: ["executive lounge", "club lounge", "lounge access"],
    westin_club: ["westin club", "club level"],
    rooftop_dining: [
      "rooftop dining",
      "rooftop restaurant",
      "deck11",
      "deck 11",
      "brooklyn new york brasserie",
      "rooftop bar",
    ],
    business_district: [
      "business district",
      "corporate district",
      "business corridor",
      "office district",
    ],
    financial_center: [
      "financial center",
      "financial district",
      "financial corridor",
      "banking district",
    ],
    aimbridge_managed: ["aimbridge", "aimbridge latam", "aimbridge hospitality"],

    // CALA six-hotel cohort
    paseo_de_la_reforma: ["paseo de la reforma", "reforma", "reforma avenue"],
    mexico_city: ["mexico city", "ciudad de méxico", "ciudad de mexico", "cdmx"],
    st_regis: ["st. regis", "st regis", "saint regis"],
    butler_service: ["butler", "butler service", "personal butler"],
    fine_dining: ["fine dining", "gourmet dining", "destination dining"],
    king_cole_bar: ["king cole bar", "king cole"],
    urban_luxury: ["urban luxury", "city luxury", "luxury urban"],
    five_diamond: ["five diamond", "5 diamond", "aaa five"],
    cap_cana: ["cap cana", "cap-cana"],
    punta_cana: ["punta cana"],
    beachfront: ["beachfront", "beach front", "on the beach"],
    private_beach: ["private beach", "exclusive beach"],
    swim_out: ["swim-out", "swim out", "swimout"],
    resort: ["resort", "resort hotel"],
    santo_domingo: ["santo domingo"],
    piantini: ["piantini"],
    blue_mall: ["blue mall"],
    naco: ["naco"],
    tiradentes: ["tiradentes"],
    radisson: ["radisson"],
    choice_hotels: ["choice hotels", "choice privileges"],
    suite_inventory: ["suites", "suite inventory", "junior suite"],
    cartagena: ["cartagena"],
    bocagrande: ["bocagrande", "boca grande"],
    faranda_grand: ["faranda grand", "faranda"],
    radisson_individuals: ["radisson individuals", "member of radisson"],
    beach_access: ["beach access", "access to the beach", "steps to the beach"],
    family_friendly: ["family-friendly", "family friendly", "families"],
    historic_city_access: [
      "walled city",
      "ciudad amurallada",
      "historic center",
      "centro histórico",
      "zona colonial",
      "ciudad colonial",
      "colonial city",
      "colonial zone",
    ],
    zona_colonial: [
      "zona colonial",
      "ciudad colonial",
      "colonial city",
      "colonial zone",
      "unesco",
    ],
    vignette_collection: ["vignette collection", "vignette", "ihg vignette"],
    ihg_one_rewards: ["ihg one rewards", "ihg rewards", "ihg one", "ihg hotels"],
    bogota: ["bogotá", "bogota"],
    bogota_norte: ["bogotá norte", "bogota norte", "norte de bogotá"],
    faranda_collection: ["faranda collection"],
    upscale: ["upscale", "upscale hotel"],
    urban_hotel: ["urban hotel", "city hotel"],

    // Bethesda / Montgomery County
    bethesda_md_location: ["bethesda", "bethesda maryland", "bethesda, md", "bethesda md"],
    pooks_hill: ["pooks hill", "pook's hill", "5151 pooks"],
    montgomery_county: ["montgomery county", "montgomery md"],
    marriott_hotels: ["marriott hotels", "marriott hotel"],
    m_club: ["m club", "m-club", "concierge level", "club level"],
    coopers_mill: ["cooper's mill", "coopers mill", "cooper’s mill"],
    peloton: ["peloton", "peloton bike"],
    nih_access: ["nih", "national institutes of health", "near nih"],
    dc_metro_access: [
      "washington dc",
      "washington, dc",
      "dc metro",
      "metro to dc",
      "access to washington",
    ],

    // Portable / previously unregistered keys
    lifestyle: ["lifestyle hotel", "lifestyle", "design-led", "lifestyle brand"],
    meetings: ["meeting", "meetings", "conference", "event space", "events"],
    restaurant: ["restaurant", "dining", "on-site restaurant", "on site restaurant"],
    on_site_restaurant: ["restaurant", "on-site restaurant", "on site dining"],
    wellness_fitness: ["fitness", "gym", "wellness", "workout"],
    meeting_rooms: ["meeting room", "meeting rooms", "conference room"],
    meeting_space_small: ["boardroom", "small meeting", "intimate meeting"],
    private_dining: ["private dining", "private dinner", "private restaurant"],
    connecting_rooms: ["connecting rooms", "adjoining rooms", "connecting room"],
    room_service: ["room service", "in-room dining", "in room dining"],
    valet_parking: ["valet", "valet parking"],
    airport_shuttle: ["airport shuttle", "hotel shuttle", "shuttle to airport"],
    park_and_fly: ["park and fly", "park-and-fly", "park & fly"],
    digital_key: ["digital key", "mobile key", "app key"],
    leisure_resort: ["leisure resort", "resort leisure", "leisure stay"],
    golf_resort: ["golf resort", "golf course", "on-site golf"],
    golf_access: ["golf", "golf course", "golf access", "golf club"],
    sports_golf_travel: ["golf trip", "golf travel", "sports travel"],
    wedding_celebrations: ["wedding", "weddings", "celebration", "reception"],
    luxury_collection: ["luxury collection", "a luxury collection"],
    curio_collection: ["curio collection", "curio"],
    sheraton: ["sheraton"],
    hyatt_world_of_hyatt: ["world of hyatt", "hyatt loyalty", "hyatt points"],
    wyndham_rewards: [
      "wyndham rewards",
      "wyndham loyalty",
      "wyndham points",
      "wyndham hotels",
    ],
    historic_building: ["historic", "historic building", "heritage building"],
    historic_art_deco: ["art deco", "art-deco", "historic art deco"],
    nrhp_listed: ["national register", "nrhp", "historic register"],
    heritage_castle: ["castle", "heritage castle", "historic castle"],
    sky_lobby: ["sky lobby", "lobby with views"],
    city_views: ["city view", "city views", "skyline view"],
    large_spacious_rooms: ["spacious room", "large room", "spacious guest"],
    terrace_rooms: ["terrace room", "terrace rooms", "private terrace"],
    recently_renovated: ["renovated", "recently renovated", "refreshed"],
    art_forward: ["art", "art-forward", "gallery", "contemporary art"],
    speakeasy: ["speakeasy", "hidden bar", "prohibition"],
    social_event_lounge: ["event lounge", "social lounge", "lounge for events"],
    walkable_downtown: ["walkable", "downtown walk", "walk to downtown"],
    neighborhood_navigator: ["navigator", "neighborhood guide", "local expert"],
    near_penn_station: ["penn station", "moynihan", "near penn"],
    near_empire_state: ["empire state"],
    near_madison_square_garden: ["madison square garden", "msg"],
    near_bryant_park: ["bryant park"],
    near_grand_central: ["grand central"],
    near_javits_center: ["javits", "javits center"],
    near_convention_center: ["convention center"],
    near_t_mobile_center: ["t-mobile center", "t mobile center"],
    near_kauffman_center: ["kauffman center", "kaufman center"],
    downtown_kansas_city: ["downtown kansas city", "kansas city downtown", "downtown kc"],
    power_and_light_district: ["power and light", "power & light"],
    streetcar_access: ["streetcar", "kc streetcar"],
    noho_location: ["noho", "north of houston"],
    lower_manhattan: ["lower manhattan", "downtown manhattan"],
    soho_adjacent: ["soho", "soho adjacent"],
    greenwich_village_access: ["greenwich village", "west village"],
    lafayette_street: ["lafayette street", "lafayette"],
    near_nyu: ["nyu", "new york university"],
    near_washington_square: ["washington square"],
    lake_boca_raton: ["lake boca", "intracoastal", "boca raton lake"],
    croquet: ["croquet"],
    mangrove_bay: ["mangrove bay", "mangrove"],
    "3_diamond": ["three diamond", "3 diamond", "aaa three"],
    marylebone_location: ["marylebone", "portman square"],
    nobu_brand: ["nobu hotel", "nobu hotels", "nobu"],
    nobu_restaurant: ["nobu restaurant", "nobu dining", "nobu matsu"],
    west_end_access: ["west end", "oxford street", "bond street"],
    japanese_hospitality: ["japanese", "omakase", "japanese hospitality"],
    ballroom_capacity: ["ballroom", "nobu ballroom", "event capacity", "700 reception"],
    yotel_brand: ["yotel", "yotel geneva"],
    geneva_lake_location: ["geneva lake", "lac léman", "lac leman", "founex", "la côte", "la cote"],
    airport_adjacent: ["airport", "near airport", "gva", "airport adjacent"],
    cabin_rooms: ["cabin", "cabins", "yotel cabin"],
    select_service: ["select-service", "select service"],
    charlotte_uptown: ["uptown charlotte", "charlotte city center", "charlotte uptown"],
};

export function getAttributeDetectionKeywordMap() {
  return ATTRIBUTE_DETECTION_KEYWORDS_V1;
}

export function isAttributeEligibleForCoreAiTesting(attributeId) {
  return Boolean(ATTRIBUTE_DETECTION_KEYWORDS_V1[String(attributeId || "").trim()]);
}

export function extractRecognizedAttributes(response, propertyProfile) {
  if (!response) return [];
  const text = response.toLowerCase();
  const recognized = [];
  for (const [attr, keywords] of Object.entries(ATTRIBUTE_DETECTION_KEYWORDS_V1)) {
    if (keywords.some((kw) => text.includes(kw))) {
      recognized.push(attr);
    }
  }
  return recognized;
}

/**
 * Extract source citations from response (mainly Perplexity).
 */
export function extractSources(response) {
  if (!response) return [];
  const urlPattern = /https?:\/\/[^\s\]\)]+/g;
  const urls = response.match(urlPattern) || [];
  const bracketPattern = /\[([^\]]+)\]\(([^)]+)\)/g;
  const sources = [];
  let match;
  while ((match = bracketPattern.exec(response)) !== null) {
    sources.push({ label: match[1], url: match[2] });
  }
  for (const url of urls) {
    if (!sources.find((s) => s.url === url)) {
      sources.push({ label: null, url });
    }
  }
  return sources.slice(0, 20);
}

/**
 * Parse a single observation's raw response and populate structured fields.
 */
export function parseObservation(observation, propertyProfile) {
  const response = observation.rawResponse || "";
  const mention = detectPropertyMention(response, propertyProfile);
  const competitors = extractCompetitors(response, propertyProfile);
  const attributes = mention.mentioned ? extractRecognizedAttributes(response, propertyProfile) : [];
  let sources = extractSources(response);
  // Perplexity returns citations as a separate array on the observation
  if (!sources.length && observation.providerCitations && observation.providerCitations.length) {
    sources = observation.providerCitations.map((url) => ({ label: null, url }));
  }

  return {
    ...observation,
    mentioned: mention.mentioned,
    position: mention.position,
    context: mention.context,
    competitorsMentioned: competitors,
    attributesRecognized: attributes,
    sourcesCited: sources,
    parsed: true,
  };
}

/**
 * Parse all observations in a period.
 * Always reprojects mentioned via detectPropertyMention when rawResponse exists,
 * even if the observation was previously marked parsed (stale flags are common).
 */
export function parsePeriodObservations(period, propertyProfile) {
  period.observations = period.observations.map((obs) => {
    if (!obs) return obs;
    if (String(obs.rawResponse || "").trim()) {
      return parseObservation(obs, propertyProfile);
    }
    if (obs.parsed) {
      // No response text — fail closed rather than trusting a stale true flag.
      return { ...obs, mentioned: false, _mentionProjection: "response_text_missing_fail_closed" };
    }
    return parseObservation(obs, propertyProfile);
  });
  period.status = "PARSED";
  return period;
}

/**
 * Reproject stored mentioned flags from response text via detectPropertyMention.
 * Use before persisting periods or when consuming historical runtime flags that
 * may credit other hotels / brand collisions.
 *
 * Does not invent mentions when rawResponse is empty — leaves mentioned=false.
 */
export function reprojectObservationMentionFlags(period, propertyProfile) {
  const observations = (period?.observations || []).map((obs) => {
    const text = obs?.rawResponse || "";
    if (!String(text).trim()) {
      return {
        ...obs,
        mentioned: false,
        position: null,
        context: null,
        _mentionProjection: "response_text_missing_fail_closed",
      };
    }
    const mention = detectPropertyMention(text, propertyProfile);
    return {
      ...obs,
      mentioned: mention.mentioned === true,
      position: mention.position,
      context: mention.context,
      _mentionProjection: "detectPropertyMention",
      _matchedVariant: mention.matchedVariant || null,
    };
  });
  return { ...period, observations };
}
