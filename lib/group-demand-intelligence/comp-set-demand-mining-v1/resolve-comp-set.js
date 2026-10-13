/**
 * Comp set resolution — ADP declaredCompSet when available; YOTEL airport corridor fallback only.
 * Do not invent competitors. Phone/address/domain may be UNKNOWN until public resolve.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { buildFormattedPhoneVariants } from "./phone-variants.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

/** Target hotels for Comp Set Demand Mining V1 pilot. */
export const COMP_SET_TARGET_HOTELS = Object.freeze([
  {
    hotelId: "recrPQcZg7SFARRb2",
    hotelKey: "YOTEL",
    hotelName: "YOTEL Geneva Lake",
    market: "Geneva / Lake Geneva / La Côte",
    countryHint: "CH",
    languages: ["fr", "en", "de"],
    serpGl: "ch",
    serpHl: "fr",
    adpFixture: "fixtures/ai-demand-positioning/yotel-geneva-lake-property-profile.json",
    // No ADP declaredCompSet — airport / La Côte corridor set used as documented GDI fallback only
    fallbackCompetitors: [
      { canonicalName: "Ibis Styles Geneva Airport", brand: "Ibis Styles", market: "Geneva Airport" },
      { canonicalName: "Hilton Geneva Hotel", brand: "Hilton", market: "Geneva Airport" },
      { canonicalName: "Mövenpick Hotel Geneva", brand: "Mövenpick", market: "Geneva Airport" },
      { canonicalName: "Novotel Genève Centre", brand: "Novotel", market: "Geneva" },
    ],
    rooms: 237,
    productFit: "urban_airport_corridor_upsale",
    defaultFitScore: 52,
  },
  {
    hotelId: "rec2PVBDavppGpenm",
    hotelKey: "AC",
    hotelName: "AC Hotel A Coruña",
    market: "A Coruña / Galicia",
    countryHint: "ES",
    languages: ["es", "gl", "en"],
    serpGl: "es",
    serpHl: "es",
    adpFixture: "fixtures/ai-demand-positioning/ac-hotel-a-coruna-property-profile.json",
    rooms: 142,
    productFit: "urban_upscale",
    defaultFitScore: 50,
  },
  {
    hotelId: "recKRJjcPnb4tVDDS",
    hotelKey: "SPICE",
    hotelName: "Spice Island Beach Resort",
    market: "Grenada / Grand Anse",
    countryHint: "GD",
    languages: ["en"],
    serpGl: "us",
    serpHl: "en",
    adpFixture: "fixtures/ai-demand-positioning/spice-island-beach-resort-property-profile.json",
    rooms: 64,
    productFit: "luxury_beach_resort",
    defaultFitScore: 48,
  },
  {
    hotelId: "recIwaP1etgx2g9nA",
    hotelKey: "CAMBRIDGE",
    hotelName: "Cambridge Beaches Resort & Spa",
    market: "Bermuda",
    countryHint: "BM",
    languages: ["en"],
    serpGl: "us",
    serpHl: "en",
    adpFixture: "fixtures/ai-demand-positioning/cambridge-beaches-bermuda-property-profile.json",
    rooms: 86,
    productFit: "cottage_colony_resort",
    defaultFitScore: 48,
  },
  {
    hotelId: "recGkME49yYuxQl0u",
    hotelKey: "NOW_NOW",
    hotelName: "NOW NOW NOHO",
    market: "New York / NoHo",
    countryHint: "US",
    languages: ["en"],
    serpGl: "us",
    serpHl: "en",
    adpFixture: "fixtures/ai-demand-positioning/now-now-noho-property-profile.json",
    rooms: 60,
    productFit: "urban_boutique",
    defaultFitScore: 50,
  },
]);

function loadAdp(fixtureRel) {
  const p = path.join(ROOT, fixtureRel);
  if (!fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

function slugId(name) {
  return `comp_${String(name || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "")
    .slice(0, 48)}`;
}

/**
 * Resolve canonical competitor set for a target hotel.
 * Prefer ADP declaredCompSet; YOTEL uses documented fallback only.
 */
export function resolveCanonicalCompSet(targetHotel = {}, opts = {}) {
  const adp = targetHotel.adpFixture ? loadAdp(targetHotel.adpFixture) : null;
  const declared = Array.isArray(adp?.declaredCompSet) ? adp.declaredCompSet : [];
  const maxComps = opts.maxCompetitors ?? 4;

  let rawNames = declared;
  let source = "ADP_DECLARED_COMP_SET";
  if (!rawNames.length && Array.isArray(targetHotel.fallbackCompetitors)) {
    rawNames = targetHotel.fallbackCompetitors.map((c) => c.canonicalName || c);
    source = "GDI_DOCUMENTED_FALLBACK_NO_ADP_DECLARED";
  }

  const competitors = rawNames.slice(0, maxComps).map((entry, i) => {
    const name = typeof entry === "string" ? entry : entry.canonicalName;
    const fb =
      typeof entry === "object"
        ? entry
        : (targetHotel.fallbackCompetitors || []).find((c) => c.canonicalName === name) || {};
    return {
      competitorHotelId: slugId(name),
      canonicalName: name,
      brand: fb.brand || name.split(/\s+/).slice(0, 2).join(" "),
      market: fb.market || targetHotel.market,
      address: null,
      publicPhone: null,
      formattedPhoneVariants: [],
      domain: null,
      currentWebsite: null,
      formerNames: [],
      knownAliases: [name],
      meetingSpaceNames: [],
      latitude: null,
      longitude: null,
      identitySource: source,
      identityComplete: false,
      sortOrder: i + 1,
      countryHint: targetHotel.countryHint,
    };
  });

  return {
    targetHotelId: targetHotel.hotelId,
    targetHotelKey: targetHotel.hotelKey,
    targetHotelName: targetHotel.hotelName,
    market: targetHotel.market,
    compSetSource: source,
    adpDeclaredCount: declared.length,
    competitors,
    targetRooms: targetHotel.rooms,
    productFit: targetHotel.productFit,
    defaultFitScore: targetHotel.defaultFitScore,
    languages: targetHotel.languages,
    serpGl: targetHotel.serpGl,
    serpHl: targetHotel.serpHl,
    countryHint: targetHotel.countryHint,
  };
}

/**
 * Merge public identity fields discovered later (phone/address/domain).
 * Never invent — only apply when provided from page/SERP extract.
 */
export function applyPublicIdentity(competitor = {}, identity = {}) {
  const next = { ...competitor };
  if (identity.publicPhone && !next.publicPhone) {
    next.publicPhone = identity.publicPhone;
    next.formattedPhoneVariants = buildFormattedPhoneVariants(
      identity.publicPhone,
      next.countryHint || identity.countryHint
    );
  }
  if (identity.address && !next.address) next.address = identity.address;
  if (identity.domain && !next.domain) next.domain = identity.domain;
  if (identity.currentWebsite && !next.currentWebsite) next.currentWebsite = identity.currentWebsite;
  if (Array.isArray(identity.formerNames) && identity.formerNames.length) {
    next.formerNames = [...new Set([...(next.formerNames || []), ...identity.formerNames])];
  }
  if (Array.isArray(identity.knownAliases) && identity.knownAliases.length) {
    next.knownAliases = [...new Set([...(next.knownAliases || []), ...identity.knownAliases])];
  }
  if (Array.isArray(identity.meetingSpaceNames) && identity.meetingSpaceNames.length) {
    next.meetingSpaceNames = [
      ...new Set([...(next.meetingSpaceNames || []), ...identity.meetingSpaceNames]),
    ];
  }
  next.identityComplete = Boolean(next.publicPhone || next.address || next.domain);
  return next;
}
