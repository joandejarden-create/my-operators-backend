/**
 * GDI Discovery Hygiene V2 — commercial purity on an existing candidate universe.
 * Does NOT change qualification scoring or WHO V9.
 * Preserves venue alias normalization; requires evidence for open/RFP labels.
 */

import { normalizeVenueStatus } from "./provider-normalization.js";

export const DISCOVERY_HYGIENE_V2 = "gdi_discovery_hygiene_v2";

export const HYGIENE_STATE = Object.freeze({
  VALID_ACTIONABLE: "VALID_ACTIONABLE",
  VALID_WATCH: "VALID_WATCH",
  VALID_FUTURE: "VALID_FUTURE",
  INSUFFICIENT: "INSUFFICIENT",
  INVALID: "INVALID",
});

export const REJECTION_CODE = Object.freeze({
  AGGREGATOR_SPAM: "AGGREGATOR_SPAM",
  PREDATORY_CONFERENCE: "PREDATORY_CONFERENCE",
  GEOGRAPHY_MISMATCH: "GEOGRAPHY_MISMATCH",
  GENERIC_EVENT: "GENERIC_EVENT",
  DUPLICATE_EVENT: "DUPLICATE_EVENT",
  UNSUPPORTED_RFP: "UNSUPPORTED_RFP",
  UNSUPPORTED_OPEN_SOURCING: "UNSUPPORTED_OPEN_SOURCING",
  ROOM_DEMAND_TOO_WEAK: "ROOM_DEMAND_TOO_WEAK",
  EVENT_IDENTITY_UNCLEAR: "EVENT_IDENTITY_UNCLEAR",
  SOURCE_AUTHORITY_TOO_LOW: "SOURCE_AUTHORITY_TOO_LOW",
  OTHER: "OTHER",
});

export const GEO_CLASS = Object.freeze({
  LOCAL: "LOCAL",
  PRIMARY_MARKET: "PRIMARY_MARKET",
  SECONDARY_FEEDER: "SECONDARY_FEEDER",
  REGIONAL_RELEVANT: "REGIONAL_RELEVANT",
  DESTINATION_RELEVANT: "DESTINATION_RELEVANT",
  OUT_OF_MARKET: "OUT_OF_MARKET",
  UNKNOWN: "UNKNOWN",
});

export const SOURCE_TIER = Object.freeze({
  TIER_1: "TIER_1",
  TIER_2: "TIER_2",
  TIER_3: "TIER_3",
  AGGREGATOR: "AGGREGATOR",
  UNKNOWN: "UNKNOWN",
});

export const ROOM_DEMAND_CLASS = Object.freeze({
  ROOMS_CONFIRMED: "ROOMS_CONFIRMED",
  ROOMS_ESTIMATED: "ROOMS_ESTIMATED",
  ROOMS_INFERRED: "ROOMS_INFERRED",
  ROOMS_UNKNOWN: "ROOMS_UNKNOWN",
});

export const HOTEL_GEO_CONTRACTS = Object.freeze({
  recGkME49yYuxQl0u: {
    slug: "now-now-noho",
    primaryTokens: [
      "noho",
      "soho",
      "greenwich village",
      "east village",
      "lower manhattan",
      "manhattan",
      "new york city",
      "new york, ny",
      "nyc",
      "brooklyn",
      "tribeca",
      "hudson square",
    ],
    outOfMarketTokens: [
      "washington, dc",
      "washington dc",
      "district of columbia",
      "boston",
      "philadelphia",
      "chicago",
      "los angeles",
      "miami",
      "san francisco",
    ],
    allowNycWideOverflow: true,
  },
  recIwaP1etgx2g9nA: {
    slug: "cambridge-beaches",
    primaryTokens: [
      "bermuda",
      "hamilton",
      "sandys",
      "somerset",
      "dockyard",
      "west end bermuda",
      "mangrove bay",
    ],
    destinationTokens: ["bermuda retreat", "bermuda incentive", "bermuda destination"],
    outOfMarketTokens: ["miami", "florida", "bahamas", "cayman", "cayman islands"],
    allowDestinationDemand: true,
  },
  recsn3BUKJ9PNfeZW: {
    slug: "jw-monterrey",
    primaryTokens: [
      "monterrey",
      "san pedro",
      "garza garcía",
      "garza garcia",
      "valle oriente",
      "nuevo león",
      "nuevo leon",
      "santa catarina",
    ],
    regionalTokens: ["saltillo", "nuevo leon", "northern mexico", "texas mexico"],
    outOfMarketTokens: ["mexico city", "cdmx", "guadalajara", "cancun", "cancún"],
  },
  // Controlled Expansion Wave 1 — onboarded with hotel configs (not mid-wave patches)
  recRXmrakhSAuctwz: {
    slug: "st-regis-mexico-city",
    primaryTokens: [
      "mexico city",
      "ciudad de mexico",
      "ciudad de méxico",
      "cdmx",
      "reforma",
      "paseo de la reforma",
      "juarez",
      "juárez",
      "polanco",
      "zona rosa",
      "cuauhtemoc",
      "cuauhtémoc",
    ],
    regionalTokens: ["toluca", "puebla", "queretaro", "querétaro"],
    outOfMarketTokens: ["cancun", "cancún", "monterrey", "guadalajara", "miami", "las vegas"],
  },
  recN76iEE6yAaPh8H: {
    slug: "st-regis-cap-cana",
    primaryTokens: [
      "cap cana",
      "cap-cana",
      "punta cana",
      "bavaro",
      "bávaro",
      "la altagracia",
    ],
    destinationTokens: [
      "dominican republic",
      "dr incentive",
      "punta cana incentive",
      "cap cana wedding",
    ],
    outOfMarketTokens: ["cancun", "cancún", "miami", "bahamas", "jamaica", "mexico city"],
    allowDestinationDemand: true,
  },
  rec8hHupaSwiWI3r7: {
    slug: "hotel-phillips-kansas-city",
    primaryTokens: [
      "kansas city",
      "kansas city mo",
      "kcmo",
      "downtown kansas city",
      "power and light",
      "power & light",
      "crossroads",
      "country club plaza",
    ],
    regionalTokens: ["overland park", "lawrence", "independence mo", "lee's summit"],
    outOfMarketTokens: [
      "st louis",
      "st. louis",
      "chicago",
      "omaha",
      "wichita",
      "las vegas",
    ],
  },
  // Controlled Expansion Wave 2 — onboarded with hotel configs (not mid-wave patches)
  recESHsNsWUFYZrxR: {
    slug: "jw-marriott-santo-domingo",
    primaryTokens: [
      "santo domingo",
      "piantini",
      "blue mall",
      "winston churchill",
      "distrito nacional",
      "naco",
      "ensanche piantini",
    ],
    regionalTokens: ["santiago dominican", "punta cana overflow santo domingo"],
    outOfMarketTokens: [
      "punta cana only",
      "cap cana only",
      "miami",
      "cancun",
      "cancún",
      "cartagena",
    ],
  },
  recCEpdskZeUBvQwG: {
    slug: "hotel-caribe-cartagena",
    primaryTokens: [
      "cartagena",
      "bocagrande",
      "cartagena de indias",
      "castillo grande",
      "el laguito",
    ],
    destinationTokens: [
      "cartagena incentive",
      "cartagena destination",
      "colombia caribbean incentive",
    ],
    outOfMarketTokens: [
      "bogota",
      "bogotá",
      "medellin",
      "medellín",
      "cancun",
      "cancún",
      "miami",
    ],
    allowDestinationDemand: true,
  },
  recD17Kxn6BcJjGFh: {
    slug: "westin-monterrey-valle",
    primaryTokens: [
      "monterrey",
      "san pedro",
      "garza garcía",
      "garza garcia",
      "valle oriente",
      "punto valle",
      "nuevo león",
      "nuevo leon",
      "santa catarina",
    ],
    regionalTokens: ["saltillo", "nuevo leon", "northern mexico", "texas mexico"],
    outOfMarketTokens: ["mexico city", "cdmx", "guadalajara", "cancun", "cancún"],
  },
  // Pilot hotel — DMV territory from hotel demand config (data/config, not delta rules)
  recLuxvwwxID7U2B8: {
    slug: "bethesda-marriott",
    primaryTokens: [
      "bethesda",
      "montgomery county",
      "rockville",
      "north bethesda",
      "chevy chase",
      "silver spring",
      "nih",
      "walter reed",
    ],
    regionalTokens: [
      "washington dc",
      "washington, dc",
      "district of columbia",
      "dmv",
      "northern virginia",
      "arlington",
      "alexandria",
      "tysons",
      "fairfax",
      "prince george",
      "college park",
      "gaithersburg",
    ],
    outOfMarketTokens: [
      "new york",
      "nyc",
      "boston",
      "chicago",
      "los angeles",
      "miami",
      "orlando",
      "las vegas",
      "san francisco",
      "atlanta",
      "dallas",
      "houston",
      "seattle",
      "denver",
    ],
  },
});

const AGGREGATOR_HOST_RE =
  /(waset\.org|conferencealerts|allconferencealert|10times\.com|eventbrite\.com|meetup\.com|clocate\.com|conferenceindex|worldacademy|academicworldresearch|iird\.?global)/i;

const PREDATORY_TITLE_RE =
  /^(international|world|global)\s+conference\s+(on|of)\s+/i;

const RFP_EVIDENCE_RE =
  /\b(rfp|request for proposal|hotel(?:\/venue)? sourcing|proposal request|host hotel|housing (?:rfp|procurement|bureau)|soliciting (?:hotels|proposals)|submit (?:a )?proposal|official hotel (?:block|program))\b/i;

const TBD_EVIDENCE_RE =
  /\b(venue tbd|hotel tbd|location tbd|venue (?:not yet|to be) (?:selected|announced|confirmed)|hotel (?:not yet|to be) (?:selected|announced)|seeking (?:a )?venue|venue TBA)\b/i;

const OPEN_EVIDENCE_RE =
  /\b(housing (?:still )?open|room block (?:still )?open|hotels (?:still )?needed|accepting hotel|overflow housing|official hotel list open)\b/i;

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

function collectText(opp = {}) {
  const bits = [
    opp.title,
    opp.organizationName,
    opp.destinationStatus,
    opp.location,
    opp.venue,
    opp.whyNow,
    opp.hotelOpportunityThesis,
    opp.summaryWhyHotel,
    opp.housingEvidence,
    opp.roomDemandEvidence,
    opp.officialSource,
  ];
  for (const e of opp.evidence || opp.evidenceSources || []) {
    if (!e) continue;
    if (typeof e === "string") bits.push(e);
    else bits.push(e.url, e.title, e.value, e.sourceTitle);
  }
  return bits.filter(Boolean).join(" \n ");
}

function collectUrls(opp = {}) {
  const urls = [];
  if (opp.officialSource) urls.push(String(opp.officialSource));
  for (const e of opp.evidence || opp.evidenceSources || opp.sources || []) {
    if (!e) continue;
    if (typeof e === "string" && /^https?:/i.test(e)) urls.push(e);
    else if (e.url) urls.push(String(e.url));
    else if (e.sourceUrl) urls.push(String(e.sourceUrl));
  }
  return [...new Set(urls.filter(Boolean))];
}

export function classifySourceAuthority(opp = {}) {
  const urls = collectUrls(opp);
  const blob = collectText(opp);
  if (!urls.length && !blob) {
    return { tier: SOURCE_TIER.UNKNOWN, urls, aggregatorOnly: true };
  }

  let best = SOURCE_TIER.UNKNOWN;
  let aggregatorHits = 0;
  let nonAgg = 0;

  for (const url of urls) {
    let host = "";
    try {
      host = new URL(url).hostname.replace(/^www\./, "");
    } catch {
      host = url;
    }
    if (AGGREGATOR_HOST_RE.test(host) || AGGREGATOR_HOST_RE.test(url)) {
      aggregatorHits += 1;
      continue;
    }
    nonAgg += 1;
    if (
      /\.(org|edu|gov)(\.|$)/i.test(host) ||
      /association|society|institute|ieee|limra|insuranceerm|noho\.nyc|villagepreservation|coro\.nyc|upcea|bilitr|prestel/i.test(
        host
      )
    ) {
      best = SOURCE_TIER.TIER_1;
    } else if (
      best !== SOURCE_TIER.TIER_1 &&
      /(marriott|hilton|hyatt|cvent|cvb|visit|convention|university|itesm|uanl|willowbank|kpmg)/i.test(
        host
      )
    ) {
      best = SOURCE_TIER.TIER_2;
    } else if (best === SOURCE_TIER.UNKNOWN) {
      best = SOURCE_TIER.TIER_3;
    }
  }

  if (nonAgg === 0 && (aggregatorHits > 0 || PREDATORY_TITLE_RE.test(norm(opp.title)))) {
    return { tier: SOURCE_TIER.AGGREGATOR, urls, aggregatorOnly: true };
  }
  if (best === SOURCE_TIER.UNKNOWN && aggregatorHits > 0) {
    return { tier: SOURCE_TIER.AGGREGATOR, urls, aggregatorOnly: true };
  }
  return {
    tier: best === SOURCE_TIER.UNKNOWN ? SOURCE_TIER.TIER_3 : best,
    urls,
    aggregatorOnly: nonAgg === 0 && aggregatorHits > 0,
  };
}

export function classifyGeography(hotelId, opp = {}) {
  const contract = HOTEL_GEO_CONTRACTS[hotelId];
  if (!contract) return { class: GEO_CLASS.UNKNOWN, detail: "no_contract" };

  const loc = norm(
    `${opp.destinationStatus || ""} ${opp.location || ""} ${opp.venue || ""}`
  );
  if (!loc.trim()) return { class: GEO_CLASS.UNKNOWN, detail: "missing_location" };

  for (const tok of contract.outOfMarketTokens || []) {
    if (loc.includes(norm(tok))) {
      // Event LOCATION governs. Mention of hotel market as "representatives attending"
      // does not convert an out-of-market host city into destination demand.
      const outIsPrimaryHost =
        loc.startsWith(norm(tok)) ||
        new RegExp(`^[^,]*(${norm(tok).replace(/[.*+?^${}()|[\]\\]/g, "\\$&")})`, "i").test(
          loc
        );
      if (outIsPrimaryHost) {
        return { class: GEO_CLASS.OUT_OF_MARKET, detail: `matched_out_host:${tok}` };
      }
      if (
        contract.allowDestinationDemand &&
        (contract.primaryTokens || []).some((p) => loc.includes(norm(p)))
      ) {
        // Only allow when primary market is the host, not a parenthetical aside
        continue;
      }
      return { class: GEO_CLASS.OUT_OF_MARKET, detail: `matched_out:${tok}` };
    }
  }

  for (const tok of contract.primaryTokens || []) {
    if (loc.includes(norm(tok))) {
      return { class: GEO_CLASS.PRIMARY_MARKET, detail: `matched_primary:${tok}` };
    }
  }

  for (const tok of contract.destinationTokens || []) {
    if (loc.includes(norm(tok))) {
      return { class: GEO_CLASS.DESTINATION_RELEVANT, detail: `matched_dest:${tok}` };
    }
  }

  for (const tok of contract.regionalTokens || []) {
    if (loc.includes(norm(tok))) {
      return { class: GEO_CLASS.REGIONAL_RELEVANT, detail: `matched_regional:${tok}` };
    }
  }

  if (contract.allowNycWideOverflow && /\bnew york\b|\bnyc\b/.test(loc)) {
    return { class: GEO_CLASS.SECONDARY_FEEDER, detail: "nyc_wide" };
  }

  return { class: GEO_CLASS.UNKNOWN, detail: "unresolved_location" };
}

export function classifyRoomDemand(opp = {}) {
  const rooms = opp.estimatedPeakRooms;
  const status = String(opp.roomDemandStatus || "");
  if (
    /VERIFIED_ROOM_BLOCK|VERIFIED_HOUSING/i.test(status) &&
    rooms != null &&
    rooms !== "UNKNOWN"
  ) {
    return ROOM_DEMAND_CLASS.ROOMS_CONFIRMED;
  }
  if (rooms != null && rooms !== "" && String(rooms).toUpperCase() !== "UNKNOWN") {
    return ROOM_DEMAND_CLASS.ROOMS_ESTIMATED;
  }
  if (/ESTIMATED|STRONG_ROOM|OVERFLOW/i.test(status)) {
    return ROOM_DEMAND_CLASS.ROOMS_INFERRED;
  }
  return ROOM_DEMAND_CLASS.ROOMS_UNKNOWN;
}

export function evidenceBackedSourcingStatus(opp = {}) {
  const raw =
    opp.venueSourcingStatus || opp.sourcingStatus || opp.venueStatus || "UNKNOWN";
  const normalized = normalizeVenueStatus(raw);
  // Evidence for open/RFP must come from research artifacts — not engine
  // boilerplate that already says "request RFP" in recommendedAction/thesis.
  const evidenceBlob = [
    opp.housingEvidence,
    opp.roomDemandEvidence,
    opp.venueStatus,
    opp.venue,
    ...(opp.evidence || []).map((e) =>
      typeof e === "string" ? e : e?.value || e?.title || e?.field || ""
    ),
    ...(opp.evidenceSources || []).map((e) =>
      typeof e === "string" ? e : e?.value || e?.title || ""
    ),
  ]
    .filter(Boolean)
    .join(" ");

  const openCodes = new Set([
    "RFP_ACTIVE_SOURCING",
    "HOTEL_VENUE_TBD",
    "OPEN_UNRESOLVED",
    "PARTIALLY_PLACED",
    "PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE",
  ]);

  if (!openCodes.has(normalized)) {
    return {
      status: normalized,
      supported: normalized !== "UNKNOWN",
      evidence: null,
      downgraded: false,
    };
  }

  if (normalized === "RFP_ACTIVE_SOURCING") {
    if (RFP_EVIDENCE_RE.test(evidenceBlob)) {
      return {
        status: normalized,
        supported: true,
        evidence: "rfp_language",
        downgraded: false,
      };
    }
    return {
      status: "UNKNOWN",
      supported: false,
      evidence: null,
      downgraded: true,
      reason: REJECTION_CODE.UNSUPPORTED_RFP,
      priorStatus: normalized,
    };
  }

  if (normalized === "HOTEL_VENUE_TBD") {
    if (
      TBD_EVIDENCE_RE.test(evidenceBlob) ||
      /^TBD$/i.test(String(opp.venueStatus || "")) ||
      /TBD/i.test(String(opp.venue || "")) ||
      /TBD|HOTEL_VENUE_TBD/i.test(String(raw))
    ) {
      return {
        status: normalized,
        supported: true,
        evidence: "tbd_language_or_status",
        downgraded: false,
      };
    }
    return {
      status: "UNKNOWN",
      supported: false,
      evidence: null,
      downgraded: true,
      reason: REJECTION_CODE.UNSUPPORTED_OPEN_SOURCING,
      priorStatus: normalized,
    };
  }

  if (normalized === "OPEN_UNRESOLVED" || normalized === "PARTIALLY_PLACED") {
    if (OPEN_EVIDENCE_RE.test(evidenceBlob) || TBD_EVIDENCE_RE.test(evidenceBlob)) {
      return {
        status: normalized,
        supported: true,
        evidence: "open_language",
        downgraded: false,
      };
    }
    return {
      status: "UNKNOWN",
      supported: false,
      evidence: null,
      downgraded: true,
      reason: REJECTION_CODE.UNSUPPORTED_OPEN_SOURCING,
      priorStatus: normalized,
    };
  }

  if (normalized === "PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE") {
    if (/overflow|housing|official hotel|multi-?hotel/i.test(evidenceBlob)) {
      return {
        status: normalized,
        supported: true,
        evidence: "overflow_language",
        downgraded: false,
      };
    }
    return {
      status: "UNKNOWN",
      supported: false,
      evidence: null,
      downgraded: true,
      reason: REJECTION_CODE.UNSUPPORTED_OPEN_SOURCING,
      priorStatus: normalized,
    };
  }

  return { status: normalized, supported: true, evidence: null, downgraded: false };
}

function isPredatoryAggregatorPattern(opp, source) {
  const title = norm(opp.title);
  if (!PREDATORY_TITLE_RE.test(title)) return false;
  if (source.tier === SOURCE_TIER.TIER_1 && !source.aggregatorOnly) return false;
  if (source.aggregatorOnly || source.tier === SOURCE_TIER.AGGREGATOR) return true;
  const org = norm(opp.organizationName);
  if (
    !org ||
    /international conference|world academy|waset|organizing committee|academic world|iird/i.test(
      org
    )
  ) {
    return true;
  }
  return source.tier === SOURCE_TIER.UNKNOWN || source.tier === SOURCE_TIER.TIER_3;
}

function isThinEventIdentity(opp, source) {
  const title = norm(opp.title);
  if (/^(ccn|pm)\b/i.test(title) && title.length < 28) return true;
  if (/^pm\s+monterrey/i.test(title)) return true;
  if (/private markets meeting/i.test(title) && source.tier === SOURCE_TIER.TIER_3) {
    return true;
  }
  return false;
}

function isGenericNeedingIdentity(opp, source) {
  const title = norm(opp.title);
  const generic =
    /^(hr leaders summit|family office|leadership conference|infrastructure forum|orthopaedic conference|public service summit|ccn\b|international dispute resolution)/i.test(
      title
    );
  if (!generic) return false;
  const hasOrg = Boolean(String(opp.organizationName || "").trim());
  const hasDate = Boolean(opp.eventStartDate);
  const hasMarket = Boolean(opp.destinationStatus || opp.location);
  const credible =
    source.tier === SOURCE_TIER.TIER_1 || source.tier === SOURCE_TIER.TIER_2;
  return !(hasOrg && hasDate && hasMarket && credible);
}

function eventIdentityKey(opp = {}) {
  const title = norm(opp.title)
    .replace(/\s*[-–|].*$/, "")
    .replace(/\b20\d{2}\b/g, "")
    .replace(/\b(12th|xviii|annual)\b/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
  const org = norm(opp.organizationName).slice(0, 40);
  const date = String(opp.eventStartDate || "").slice(0, 10);
  const market = norm(opp.destinationStatus || opp.location)
    .replace(/[^a-z0-9]+/g, " ")
    .slice(0, 40);
  return `${title}|${date}|${market}|${org}`;
}

function looseTitleKey(opp = {}) {
  let loose = norm(opp.title)
    .replace(/\s*[-–].*$/, "")
    .replace(/\b20\d{2}\b/g, "")
    .replace(/\b(12th|xviii)\b/g, "")
    .replace(/energy leaders/g, "")
    .trim();
  if (loose.includes("mexico infrastructure projects forum")) {
    loose = "mexico infrastructure projects forum";
  }
  const date = String(opp.eventStartDate || "").slice(0, 10);
  return `loose:${loose}|${date}`;
}

export function hygieneScoreOpportunity(hotelId, opp, { seenKeys = new Set() } = {}) {
  const filters = [];
  const source = classifySourceAuthority(opp);
  const geo = classifyGeography(hotelId, opp);
  const rooms = classifyRoomDemand(opp);
  const sourcing = evidenceBackedSourcingStatus(opp);
  const key = eventIdentityKey(opp);
  const loose = looseTitleKey(opp);

  if (seenKeys.has(key) || seenKeys.has(loose)) {
    filters.push({ code: REJECTION_CODE.DUPLICATE_EVENT, detail: key });
  } else {
    seenKeys.add(key);
    seenKeys.add(loose);
  }

  if (isPredatoryAggregatorPattern(opp, source)) {
    filters.push({
      code: source.aggregatorOnly
        ? REJECTION_CODE.AGGREGATOR_SPAM
        : REJECTION_CODE.PREDATORY_CONFERENCE,
      detail: `tier=${source.tier}`,
    });
  } else if (source.aggregatorOnly) {
    filters.push({
      code: REJECTION_CODE.SOURCE_AUTHORITY_TOO_LOW,
      detail: "aggregator_only_uncorroborated",
    });
  }

  if (geo.class === GEO_CLASS.OUT_OF_MARKET) {
    filters.push({ code: REJECTION_CODE.GEOGRAPHY_MISMATCH, detail: geo.detail });
  }

  if (isGenericNeedingIdentity(opp, source) || isThinEventIdentity(opp, source)) {
    filters.push({
      code: REJECTION_CODE.GENERIC_EVENT,
      detail: isThinEventIdentity(opp, source)
        ? "thin_event_identity"
        : "generic_title_missing_identity",
    });
  }

  if (!String(opp.title || "").trim()) {
    filters.push({ code: REJECTION_CODE.EVENT_IDENTITY_UNCLEAR, detail: "missing_title" });
  }

  if (sourcing.downgraded) {
    filters.push({
      code: sourcing.reason || REJECTION_CODE.UNSUPPORTED_OPEN_SOURCING,
      detail: `prior=${sourcing.priorStatus}`,
      soft: true,
    });
  }

  const hard = filters.filter((f) => !f.soft);
  let state = HYGIENE_STATE.VALID_ACTIONABLE;
  let rejectionCode = null;

  if (hard.some((f) => f.code === REJECTION_CODE.DUPLICATE_EVENT)) {
    state = HYGIENE_STATE.INVALID;
    rejectionCode = REJECTION_CODE.DUPLICATE_EVENT;
  } else if (
    hard.some((f) =>
      [
        REJECTION_CODE.AGGREGATOR_SPAM,
        REJECTION_CODE.PREDATORY_CONFERENCE,
        REJECTION_CODE.GEOGRAPHY_MISMATCH,
        REJECTION_CODE.SOURCE_AUTHORITY_TOO_LOW,
      ].includes(f.code)
    )
  ) {
    state = HYGIENE_STATE.INVALID;
    rejectionCode = hard.find((f) =>
      [
        REJECTION_CODE.AGGREGATOR_SPAM,
        REJECTION_CODE.PREDATORY_CONFERENCE,
        REJECTION_CODE.GEOGRAPHY_MISMATCH,
        REJECTION_CODE.SOURCE_AUTHORITY_TOO_LOW,
      ].includes(f.code)
    ).code;
  } else if (hard.some((f) => f.code === REJECTION_CODE.GENERIC_EVENT)) {
    state = HYGIENE_STATE.INSUFFICIENT;
    rejectionCode = REJECTION_CODE.GENERIC_EVENT;
  } else if (geo.class === GEO_CLASS.UNKNOWN && !opp.destinationStatus && !opp.location) {
    state = HYGIENE_STATE.INSUFFICIENT;
    rejectionCode = REJECTION_CODE.EVENT_IDENTITY_UNCLEAR;
  } else {
    const supportedOpen = sourcing.supported && sourcing.status !== "UNKNOWN";
    const fullyClosed = /FULLY_PLACED|NO_OVERFLOW_EVIDENCE|CURRENT_CYCLE_CLOSED/i.test(
      sourcing.status
    );
    const timingOk =
      !opp.eventStartDate || Number(String(opp.eventStartDate).slice(0, 4)) <= 2028;
    const geoOk = [
      GEO_CLASS.PRIMARY_MARKET,
      GEO_CLASS.LOCAL,
      GEO_CLASS.SECONDARY_FEEDER,
      GEO_CLASS.DESTINATION_RELEVANT,
      GEO_CLASS.REGIONAL_RELEVANT,
    ].includes(geo.class);

    const credibleSource =
      source.tier === SOURCE_TIER.TIER_1 ||
      source.tier === SOURCE_TIER.TIER_2 ||
      (source.tier === SOURCE_TIER.TIER_3 &&
        !source.aggregatorOnly &&
        Boolean(String(opp.organizationName || "").trim()));
    const pursuitThesis =
      (supportedOpen && !source.aggregatorOnly) ||
      (sourcing.status === "UNKNOWN" && credibleSource && geoOk);

    if (fullyClosed) {
      state = HYGIENE_STATE.VALID_WATCH;
    } else if (timingOk && geoOk && pursuitThesis && credibleSource) {
      state = HYGIENE_STATE.VALID_ACTIONABLE;
      // T3 + unknown sourcing → watch (not actionable) unless open status is evidence-backed
      if (
        sourcing.status === "UNKNOWN" &&
        source.tier === SOURCE_TIER.TIER_3 &&
        !supportedOpen
      ) {
        state = HYGIENE_STATE.VALID_WATCH;
      }
      // Local speak / parish-style series — watch, not sales pursuit
      if (/\bspeaks\b|parish council/i.test(String(opp.title || "") + " " + String(opp.organizationName || ""))) {
        state = HYGIENE_STATE.VALID_WATCH;
      }
    } else if (timingOk && geoOk && credibleSource) {
      state = HYGIENE_STATE.VALID_WATCH;
    } else if (!timingOk) {
      state = HYGIENE_STATE.VALID_FUTURE;
    } else if (geoOk && timingOk) {
      state = String(opp.organizationName || "").trim()
        ? HYGIENE_STATE.VALID_WATCH
        : HYGIENE_STATE.INSUFFICIENT;
    } else {
      state = HYGIENE_STATE.INSUFFICIENT;
      rejectionCode = REJECTION_CODE.OTHER;
    }
  }

  return {
    hygieneState: state,
    rejectionCode,
    filters,
    source,
    geo,
    rooms,
    sourcing: {
      status: sourcing.status,
      supported: sourcing.supported,
      evidence: sourcing.evidence,
      downgraded: Boolean(sourcing.downgraded),
      priorStatus: sourcing.priorStatus || null,
    },
    identityKey: key,
  };
}

export function applyDiscoveryHygieneV2(hotelId, opportunities = []) {
  const seenKeys = new Set();
  const filterCounts = {
    AGGREGATOR_SPAM: 0,
    PREDATORY_CONFERENCE: 0,
    GEOGRAPHY_MISMATCH: 0,
    UNSUPPORTED_RFP: 0,
    UNSUPPORTED_OPEN_SOURCING: 0,
    DUPLICATE_EVENT: 0,
    GENERIC_EVENT: 0,
    SOURCE_AUTHORITY_TOO_LOW: 0,
    OTHER: 0,
  };

  const ordered = [...opportunities].sort(
    (a, b) => String(b.title || "").length - String(a.title || "").length
  );

  const rows = [];
  for (const opp of ordered) {
    const result = hygieneScoreOpportunity(hotelId, opp, { seenKeys });
    for (const f of result.filters) {
      if (filterCounts[f.code] != null) filterCounts[f.code] += 1;
      else filterCounts.OTHER += 1;
    }
    rows.push({
      ...opp,
      venueSourcingStatus: result.sourcing.status,
      hygiene: result,
      hygieneState: result.hygieneState,
    });
  }

  const byState = {};
  for (const r of rows) {
    byState[r.hygieneState] = (byState[r.hygieneState] || 0) + 1;
  }

  return {
    version: DISCOVERY_HYGIENE_V2,
    hotelId,
    filterCounts,
    byState,
    rows,
    validActionable: rows.filter(
      (r) => r.hygieneState === HYGIENE_STATE.VALID_ACTIONABLE
    ),
  };
}

export function assertVenueAliasRegression() {
  const cases = [
    ["TBD", "HOTEL_VENUE_TBD"],
    ["VENUE TBD", "HOTEL_VENUE_TBD"],
    ["HOTEL TBD", "HOTEL_VENUE_TBD"],
    ["RFP", "RFP_ACTIVE_SOURCING"],
    ["RFP ACTIVE", "RFP_ACTIVE_SOURCING"],
  ];
  const failures = [];
  for (const [raw, expected] of cases) {
    const got = normalizeVenueStatus(raw);
    if (got !== expected) failures.push({ raw, expected, got });
  }
  return { ok: failures.length === 0, failures };
}

export function mapManualReasonToCode(reason = "", title = "") {
  const r = String(reason || "").toLowerCase();
  const t = String(title || "");
  if (/geo|dc|miami|broad/i.test(r)) return REJECTION_CODE.GEOGRAPHY_MISMATCH;
  if (/aggregator|predatory/i.test(r)) return REJECTION_CODE.AGGREGATOR_SPAM;
  if (/duplicate/i.test(r)) return REJECTION_CODE.DUPLICATE_EVENT;
  if (/generic|thin identity/i.test(r)) return REJECTION_CODE.GENERIC_EVENT;
  if (/contamination|thin \+/i.test(r)) return REJECTION_CODE.EVENT_IDENTITY_UNCLEAR;
  if (PREDATORY_TITLE_RE.test(t)) return REJECTION_CODE.PREDATORY_CONFERENCE;
  return REJECTION_CODE.OTHER;
}
