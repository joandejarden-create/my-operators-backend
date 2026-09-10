/**
 * Bethesda Marriott ADP foundation constants — reusable market pack key, not one-off logic.
 * Property: WASBT · 5151 Pooks Hill Road · distinct from WASBN (Bethesda North).
 */

export const BETHESDA_ADP_PROPERTY_ID = "adp_bethesda_marriott";
export const BETHESDA_REGISTRY_KEY = "bethesda_marriott";
export const BETHESDA_ENTITY_ID = "bethesda_marriott";
export const BETHESDA_NORTH_ENTITY_ID = "bethesda_north_marriott";
export const BETHESDA_MARKET_KEY = "bethesda_montgomery";
export const BETHESDA_MARKET_LABEL = "Bethesda / Montgomery County";
export const BETHESDA_IDENTITY_KEY = "ind_marriott_us_wasbt";
export const BETHESDA_MARRIOTT_COST_CAP_USD = 12;

export const BETHESDA_FIRST_PARTY = Object.freeze({
  displayName: "Bethesda Marriott",
  marriottCode: "WASBT",
  addressLine1: "5151 Pooks Hill Road",
  city: "Bethesda",
  state: "MD",
  stateFull: "Maryland",
  postalCode: "20814",
  country: "United States",
  countryCode: "US",
  brand: "Marriott Hotels",
  parentCompany: "Marriott International",
  affiliation: "Marriott Hotels",
  chainScale: "Upper Upscale",
  officialPropertyPageUrl:
    "https://www.marriott.com/en-us/hotels/wasbt-bethesda-marriott/overview/",
  eventsUrl: "https://www.marriott.com/en-us/hotels/wasbt-bethesda-marriott/events/",
  diningUrl: "https://www.marriott.com/en-us/hotels/wasbt-bethesda-marriott/dining/",
  roomsUrl: "https://www.marriott.com/en-us/hotels/wasbt-bethesda-marriott/rooms/",
  rooms: 407,
  roomCountConfidence: "MEDIUM",
  roomCountNotes:
    "407 guest rooms/units corroborated by AAA TripCanvas (407 Units) and Maryland transaction reporting on the 5151 Pooks Hill Road Marriott; Marriott.com rooms page does not publish a numeric total — retain MEDIUM until Census steward HIGH.",
  meetingSpace: Object.freeze({
    totalSqFt: 18719,
    meetingRooms: 27,
    largestRoom: Object.freeze({
      name: "Grand Ballroom",
      sqFt: 4592,
      capacity: 450,
    }),
    source: "https://www.marriott.com/en-us/hotels/wasbt-bethesda-marriott/events/",
    confidence: "HIGH",
  }),
  restaurants: Object.freeze([
    Object.freeze({
      name: "Cooper's Mill",
      cuisine: "American",
      setting: "Restaurant",
      source: "https://www.marriott.com/en-us/hotels/wasbt-bethesda-marriott/dining/",
    }),
  ]),
  attributesVerified: Object.freeze([
    "bethesda_md_location",
    "pooks_hill",
    "marriott_hotels",
    "marriott_bonvoy",
    "m_club",
    "coopers_mill",
    "fitness_center_24hr",
    "peloton",
    "meeting_space",
    "ballroom",
    "full_service",
    "nih_access",
    "dc_metro_access",
  ]),
  poolStatus: Object.freeze({
    fact: "Outdoor / pool patio referenced on Marriott wedding/events copy",
    confidence: "MEDIUM",
    realityGapEligible: true,
    attributeId: "outdoor_pool",
    source: "https://www.marriott.com/en-us/hotels/wasbt-bethesda-marriott/events/weddings/",
  }),
  parking: Object.freeze({
    fact: "UNVERIFIED — parking terms not frozen from first-party in this foundation pass",
    confidence: "UNVERIFIED",
    realityGapEligible: false,
  }),
  petPolicy: Object.freeze({
    fact: "UNVERIFIED",
    confidence: "UNVERIFIED",
    realityGapEligible: false,
  }),
});

/** Verified aliases only — Bethesda North is DISTINCT_ENTITY, never an alias. */
export const BETHESDA_VERIFIED_ALIASES = Object.freeze([
  { alias: "Bethesda Marriott", evidence: "Marriott first-party display name", status: "PRIMARY" },
  { alias: "Bethesda Marriott Hotel", evidence: "AAA / commercial naming for same WASBT address", status: "VERIFIED" },
  { alias: "Marriott Bethesda", evidence: "Common inverted naming; same WASBT property — UNSAFE as standalone Path-A alias (collides with Marriott Bethesda Downtown / Residence Inn by Marriott Bethesda Downtown). Prefer Bethesda Marriott + Pooks Hill variants; keep as documentation only.", status: "DOCUMENTED_NOT_PATH_A_ALIAS" },
  {
    alias: "Bethesda Marriott at Pooks Hill",
    evidence: "Address-linked commercial variant for 5151 Pooks Hill Road",
    status: "VERIFIED",
  },
  { alias: "WASBT", evidence: "Marriott hotel code", status: "INTERNAL_CODE" },
]);

export const BETHESDA_NEGATIVE_ALIASES = Object.freeze([
  {
    alias: "Bethesda North Marriott Hotel & Conference Center",
    entityId: BETHESDA_NORTH_ENTITY_ID,
    marriottCode: "WASBN",
    status: "DISTINCT_ENTITY",
    rationale: "Separate conference-center property in North Bethesda; must never merge with WASBT",
  },
]);

export const BETHESDA_MARKET_DEFINITION = Object.freeze({
  canonicalMarketId: BETHESDA_MARKET_KEY,
  customerFacingLabel: BETHESDA_MARKET_LABEL,
  geographicScope:
    "Bethesda and North Bethesda / Montgomery County upper-upscale and upscale full-service demand; DC metro access without downtown-DC luxury pack",
  rationale:
    "WASBT sits on Pooks Hill / Rockville Pike with NIH, Walter Reed, corporate, and DC-access demand. Substitution is primarily Bethesda–Montgomery full-service/upscale hotels, not downtown DC monument-core luxury.",
  rejectedAlternatives: Object.freeze([
    {
      option: "Bethesda-only municipal",
      reason: "Excludes North Bethesda / conference and Pike corridor substitutes travelers actually use",
    },
    {
      option: "Downtown Washington DC",
      reason: "Different trip purpose and hotel set; would dilute CORE and scenario relevance",
    },
    {
      option: "Reuse nyc/kansas/monterrey packs",
      reason: "Wrong geography and demand generators",
    },
  ]),
  stewardReviewRequired: false,
});

/**
 * Peer challenge outcomes — CORE only when defendable to an owner/GM.
 */
export const BETHESDA_PEER_SET = Object.freeze([
  {
    entityId: "hyatt_regency_bethesda",
    canonical: "Hyatt Regency Bethesda",
    brand: "Hyatt Regency",
    location: "Bethesda, MD",
    positioning: "Full-service upper-upscale",
    hotelType: "full_service_hotel",
    overlap: "Business, meetings, leisure, medical/corporate visitors",
    role: "CORE",
    peerConfidence: "HIGH",
    inclusion:
      "Same Bethesda municipal demand node; full-service substitute across business and group needs",
  },
  {
    entityId: "the_bethesdan_hotel",
    canonical: "The Bethesdan Hotel, Tapestry Collection by Hilton",
    brand: "Tapestry Collection",
    location: "Bethesda, MD",
    positioning: "Upper-upscale soft-brand boutique",
    hotelType: "lifestyle_boutique_hotel",
    overlap: "Leisure, couples, celebration, selective business",
    role: "CORE",
    peerConfidence: "HIGH",
    inclusion: "Local Bethesda alternative with overlapping upscale leisure/business travelers",
  },
  {
    entityId: "marriott_bethesda_downtown",
    canonical: "Marriott Bethesda Downtown at Marriott HQ",
    brand: "Marriott Hotels",
    location: "Bethesda, MD",
    positioning: "Upper-upscale Marriott Hotels",
    hotelType: "full_service_hotel",
    overlap: "Business, Bonvoy, meetings",
    role: "CORE",
    peerConfidence: "HIGH",
    inclusion: "Same brand family and Bethesda demand; distinct physical hotel (WASBD)",
  },
  {
    entityId: BETHESDA_NORTH_ENTITY_ID,
    canonical: "Bethesda North Marriott Hotel & Conference Center",
    brand: "Marriott Hotels",
    location: "North Bethesda, MD",
    positioning: "Large conference / full-service",
    hotelType: "conference_center_hotel",
    overlap: "Group/meeting and some business",
    role: "CORE",
    peerConfidence: "MEDIUM",
    inclusion:
      "Montgomery County Marriott conference substitute for meetings/group; DISTINCT from WASBT — never an alias",
    challengeNote: "Product/size differs; retained for group_meeting/business CORE with MEDIUM confidence",
  },
  {
    entityId: "ac_hotel_bethesda_downtown",
    canonical: "AC Hotel Bethesda Downtown",
    brand: "AC Hotels",
    location: "Bethesda, MD",
    positioning: "Upscale select-service lifestyle",
    hotelType: "select_service_lifestyle",
    overlap: "Leisure, couples, selective business",
    role: "CORE",
    peerConfidence: "MEDIUM",
    inclusion: "Bethesda urban lifestyle substitute; lighter meetings proposition than WASBT",
  },
  {
    entityId: "hilton_garden_inn_bethesda",
    canonical: "Hilton Garden Inn Bethesda Downtown",
    brand: "Hilton Garden Inn",
    location: "Bethesda, MD",
    positioning: "Upscale select-service",
    hotelType: "select_service_hotel",
    overlap: "Price-sensitive business",
    role: "SECONDARY",
    peerConfidence: "MEDIUM",
    exclusionFromCore: "Service model below full-service Marriott; not a primary CORE substitute",
  },
  {
    entityId: "residence_inn_bethesda_downtown",
    canonical: "Residence Inn Bethesda Downtown",
    brand: "Residence Inn",
    location: "Bethesda, MD",
    positioning: "Extended-stay",
    hotelType: "extended_stay",
    overlap: "Longer-stay business",
    role: "EXCLUDE",
    peerConfidence: "HIGH",
    exclusionFromCore: "Extended-stay use case — not a primary transient full-service substitute",
  },
]);

export function bethesdaCoreIdsForIntent(intent) {
  const core = BETHESDA_PEER_SET.filter((p) => p.role === "CORE").map((p) => p.entityId);
  // Subject never in CORE list — peers only
  const peers = core.filter((id) => id !== BETHESDA_ENTITY_ID);
  if (intent === "group_meeting" || intent === "business") {
    return peers.slice(0, 5); // includes Bethesda North
  }
  if (intent === "wellness" || intent === "adventure") {
    return peers.filter((id) => id !== BETHESDA_NORTH_ENTITY_ID).slice(0, 4);
  }
  return peers.filter((id) => id !== BETHESDA_NORTH_ENTITY_ID || intent === "celebration").slice(0, 4);
}
