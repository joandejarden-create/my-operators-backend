/**
 * Apify Actor registry V1 — verified Store Actors only.
 *
 * Actor IDs, publishers, pricing and input fields were verified via Apify Store
 * MCP search (2026-10-30). Only explicitly enabled Actors may execute.
 * Live runs require separate founder authorization + APIFY_TOKEN.
 */

export const APIFY_ACTOR_REGISTRY_V1 = "gdi-apify-actor-registry-v1";

export const APIFY_DISCOVERY_FAMILY = Object.freeze({
  WEDDINGS_SOCIAL: "WEDDINGS_SOCIAL",
  CORPORATE_MEETINGS: "CORPORATE_MEETINGS",
  PUBLISHED_EVENTS: "PUBLISHED_EVENTS",
  SPORTS_ENTERTAINMENT: "SPORTS_ENTERTAINMENT",
  LOCAL_DEMAND_PARTNERSHIPS: "LOCAL_DEMAND_PARTNERSHIPS",
  WEB_EXTRACTION: "WEB_EXTRACTION",
  SEARCH_SUPPLEMENT: "SEARCH_SUPPLEMENT",
});

/**
 * @typedef {object} ApifyActorRegistryEntry
 * @property {string} actorId
 * @property {string} title
 * @property {string} publisher
 * @property {'Apify'|'community'} publisherType
 * @property {string} storeUrl
 * @property {boolean} enabled
 * @property {string[]} discoveryFamilies
 * @property {string[]} demandSegments
 * @property {boolean} geoFilter
 * @property {boolean} dateFilter
 * @property {string} pricingModel
 * @property {string} pricingSummary
 * @property {number|null} monthlyUsers
 * @property {number|null} rating
 * @property {string[]} inputFields
 * @property {string} expectedOutput
 * @property {string} reliabilityNote
 * @property {string[]} tosNotes
 * @property {'VERIFIED_STORE'|'NOT_VERIFIED'} verificationStatus
 */

/** @type {ApifyActorRegistryEntry[]} */
export const APIFY_ACTOR_REGISTRY = Object.freeze([
  Object.freeze({
    actorId: "parseforge/eventbrite-scraper",
    title: "Eventbrite Event Scraper",
    publisher: "parseforge",
    publisherType: "community",
    storeUrl: "https://apify.com/parseforge/eventbrite-scraper",
    enabled: true,
    discoveryFamilies: [APIFY_DISCOVERY_FAMILY.PUBLISHED_EVENTS],
    demandSegments: [
      "associations_conferences_events",
      "corporate_meetings_incentives",
      "education_scientific",
      "social_special_events",
      "local_catering_day_meetings",
    ],
    geoFilter: true,
    dateFilter: true,
    pricingModel: "pay_per_event",
    pricingSummary:
      "Bronze tier ~$0.00433/result + optional organizer ~$0.0029 + actor start ~$0.00583 (tier-dependent; verify at pricing URL)",
    monthlyUsers: 81,
    rating: 5.0,
    inputFields: [
      "maxItems",
      "startUrl",
      "city",
      "category",
      "date",
      "format",
      "price",
      "online",
      "retrieveOrganizerData",
    ],
    expectedOutput: "Event title, date, venue, price; optional organizer",
    reliabilityNote:
      "High recent usage vs peers. DATE LIMITATION: input.date only supports relative windows (today…next-month) — NOT suitable alone for Aug–Oct 2027 Waterstone need dates; use aitorsm/eventbrite or solidcode/eventbrite-scraper for absolute YYYY-MM-DD ranges.",
    tosNotes: [
      "Scraping Eventbrite may be subject to Eventbrite ToS and robots restrictions — legal review before production scale.",
    ],
    verificationStatus: "VERIFIED_STORE",
  }),
  Object.freeze({
    actorId: "aitorsm/eventbrite",
    title: "Eventbrite Scraper — Event & Organizer Data",
    publisher: "aitorsm",
    publisherType: "community",
    storeUrl: "https://apify.com/aitorsm/eventbrite",
    enabled: true,
    discoveryFamilies: [APIFY_DISCOVERY_FAMILY.PUBLISHED_EVENTS],
    demandSegments: [
      "associations_conferences_events",
      "corporate_meetings_incentives",
      "education_scientific",
      "social_special_events",
      "local_catering_day_meetings",
      "corporate_celebrations_private_events",
    ],
    geoFilter: true,
    dateFilter: true,
    pricingModel: "pay_per_event",
    pricingSummary:
      "result ~$0.015 + optional organizer enrichment ~$0.015 + start ~$0.00005 (Bronze; higher than parseforge)",
    monthlyUsers: 86,
    rating: null,
    inputFields: [
      "country",
      "city",
      "category",
      "keyword",
      "price",
      "startDate",
      "endDate",
      "maxResults",
      "enrichOrganizers",
    ],
    expectedOutput: "Eventbrite events with absolute startDate/endDate filter support (YYYY-MM-DD)",
    reliabilityNote:
      "PREFERRED for Waterstone Aug–Oct 2027 dated discovery — supports absolute date range. Higher per-result cost than parseforge/solidcode.",
    tosNotes: ["Eventbrite ToS — constrain volume; public listings only."],
    verificationStatus: "VERIFIED_STORE",
  }),
  Object.freeze({
    actorId: "solidcode/eventbrite-scraper",
    title: "Eventbrite Scraper",
    publisher: "solidcode",
    publisherType: "community",
    storeUrl: "https://apify.com/solidcode/eventbrite-scraper",
    enabled: true,
    discoveryFamilies: [APIFY_DISCOVERY_FAMILY.PUBLISHED_EVENTS],
    demandSegments: [
      "associations_conferences_events",
      "corporate_meetings_incentives",
      "social_special_events",
      "education_scientific",
    ],
    geoFilter: true,
    dateFilter: true,
    pricingModel: "pay_per_event",
    pricingSummary: "result ~$0.00113 + start ~$0.00005 (Bronze) — lowest Eventbrite unit cost among verified options",
    monthlyUsers: 23,
    rating: 5.0,
    inputFields: [
      "eventUrls",
      "country",
      "city",
      "category",
      "keyword",
      "startDate",
      "endDate",
      "includeSeriesDates",
      "eventFormat",
      "priceFilter",
      "maxResults",
    ],
    expectedOutput: "Events with GPS venue, organizer, absolute date range; optional series dates",
    reliabilityNote:
      "Good absolute date support for 2027 pilot; lower monthly users — validate output quality in first live run.",
    tosNotes: ["Eventbrite ToS; autoUpgradeNetwork may increase proxy cost if blocked."],
    verificationStatus: "VERIFIED_STORE",
  }),
  Object.freeze({
    actorId: "memo23/eventbrite-scraper",
    title: "Eventbrite, Lu.ma & Meetup Events + Organizer Leads",
    publisher: "memo23",
    publisherType: "community",
    storeUrl: "https://apify.com/memo23/eventbrite-scraper",
    enabled: true,
    discoveryFamilies: [APIFY_DISCOVERY_FAMILY.PUBLISHED_EVENTS],
    demandSegments: [
      "associations_conferences_events",
      "corporate_meetings_incentives",
      "social_special_events",
      "education_scientific",
    ],
    geoFilter: true,
    dateFilter: true,
    pricingModel: "pay_per_event",
    pricingSummary:
      "Actor start ~$0.025 + result ~$0.00099 + optional additional data ~$0.001 (Bronze; tier-dependent)",
    monthlyUsers: 35,
    rating: 5.0,
    inputFields: [
      "startUrls",
      "location",
      "category",
      "keyword",
      "lumaCity",
      "dateFilter",
      "maxItems",
      "enrichOrganizer",
    ],
    expectedOutput: "Multi-platform events (Eventbrite/Luma/Meetup) with organizer fields",
    reliabilityNote: "Lower monthly users than Eventbrite specialists; useful for Luma+Meetup combo.",
    tosNotes: [
      "Multi-platform scraping — review each source ToS separately.",
    ],
    verificationStatus: "VERIFIED_STORE",
  }),
  Object.freeze({
    actorId: "filip_cicvarek/meetup-scraper",
    title: "Meetup Scraper",
    publisher: "filip_cicvarek",
    publisherType: "community",
    storeUrl: "https://apify.com/filip_cicvarek/meetup-scraper",
    enabled: true,
    discoveryFamilies: [APIFY_DISCOVERY_FAMILY.PUBLISHED_EVENTS],
    demandSegments: [
      "associations_conferences_events",
      "education_scientific",
      "local_catering_day_meetings",
      "corporate_meetings_incentives",
    ],
    geoFilter: true,
    dateFilter: true,
    pricingModel: "pay_per_event",
    pricingSummary: "~$0.0008 per scraped event (Bronze; tier-dependent)",
    monthlyUsers: 69,
    rating: 4.93,
    inputFields: [
      "mode",
      "searchKeyword",
      "city",
      "state",
      "country",
      "startDateRange",
      "endDateRange",
      "maxResults",
    ],
    expectedOutput: "Meetup events/groups with dates, venue GPS, RSVP, organizer",
    reliabilityNote: "Solid monthly usage for Meetup-specific discovery.",
    tosNotes: ["Meetup ToS / public data only — no private member scraping."],
    verificationStatus: "VERIFIED_STORE",
  }),
  Object.freeze({
    actorId: "compass/crawler-google-places",
    title: "Google Maps Scraper",
    publisher: "compass",
    publisherType: "Apify",
    storeUrl: "https://apify.com/compass/crawler-google-places",
    enabled: true,
    discoveryFamilies: [
      APIFY_DISCOVERY_FAMILY.WEDDINGS_SOCIAL,
      APIFY_DISCOVERY_FAMILY.LOCAL_DEMAND_PARTNERSHIPS,
      APIFY_DISCOVERY_FAMILY.CORPORATE_MEETINGS,
    ],
    demandSegments: [
      "social_special_events",
      "preferred_lodging_partnerships",
      "professional_event_planners",
      "local_catering_day_meetings",
      "corporate_accounts_scaling",
      "travel_trade_group_intermediaries",
    ],
    geoFilter: true,
    dateFilter: false,
    pricingModel: "pay_per_event",
    pricingSummary:
      "Scraped place ~$0.003 + optional contact/leads/review add-ons (Bronze; many event types — verify pricing page)",
    monthlyUsers: 46606,
    rating: 4.7,
    inputFields: [
      "searchStringsArray",
      "locationQuery",
      "maxCrawledPlacesPerSearch",
      "categoryFilterWords",
      "scrapeContacts",
    ],
    expectedOutput: "Local businesses: venues, planners, offices — relationship prospects, not dated events",
    reliabilityNote: "Apify-maintained; highest usage Maps Actor on Store.",
    tosNotes: [
      "Google Maps scraping subject to Google ToS — constrain volume; use for public business listings only.",
      "Venue listing ≠ wedding booking; planner listing ≠ future wedding.",
    ],
    verificationStatus: "VERIFIED_STORE",
  }),
  Object.freeze({
    actorId: "dionysus_way/the-knot-marketplace-scraper---wedding-vendor-leads",
    title: "The Knot Marketplace Scraper - Wedding Vendor Leads",
    publisher: "dionysus_way",
    publisherType: "community",
    storeUrl:
      "https://apify.com/dionysus_way/the-knot-marketplace-scraper---wedding-vendor-leads",
    enabled: true,
    discoveryFamilies: [APIFY_DISCOVERY_FAMILY.WEDDINGS_SOCIAL],
    demandSegments: [
      "social_special_events",
      "preferred_lodging_partnerships",
      "professional_event_planners",
    ],
    geoFilter: true,
    dateFilter: false,
    pricingModel: "pay_per_event",
    pricingSummary: "Actor start ~$0.00005 + vendor listing ~$0.0057 (Bronze; tier-dependent)",
    monthlyUsers: 11,
    rating: null,
    inputFields: ["startUrls", "maxPages"],
    expectedOutput: "Wedding vendor listings: contact, ratings, pricing bands",
    reliabilityNote: "Lower monthly users — pilot carefully; vendor directories are relationship track only.",
    tosNotes: [
      "The Knot ToS may restrict scraping — legal review before scale.",
      "Do not infer private wedding dates or scrape private individual data.",
    ],
    verificationStatus: "VERIFIED_STORE",
  }),
  Object.freeze({
    actorId: "hanamira/wedding-vendor-scraper",
    title: "Wedding Vendor Scraper - Bridebook, The Knot, WeddingWire, Zola",
    publisher: "hanamira",
    publisherType: "community",
    storeUrl: "https://apify.com/hanamira/wedding-vendor-scraper",
    enabled: true,
    discoveryFamilies: [APIFY_DISCOVERY_FAMILY.WEDDINGS_SOCIAL],
    demandSegments: [
      "social_special_events",
      "preferred_lodging_partnerships",
      "professional_event_planners",
    ],
    geoFilter: true,
    dateFilter: false,
    pricingModel: "pay_per_event",
    pricingSummary: "Actor start ~$0.00005 + result ~$0.005 (Bronze; tier-dependent)",
    monthlyUsers: 7,
    rating: null,
    inputFields: [
      "mode",
      "sources",
      "location",
      "region",
      "categories",
      "maxResultsPerCategory",
    ],
    expectedOutput: "Multi-marketplace wedding vendors with contact/pricing/reviews",
    reliabilityNote: "Low monthly users — supplemental; prefer alongside Maps + The Knot specialist.",
    tosNotes: [
      "Multi-platform wedding directories — ToS risk per source.",
      "Relationship prospecting only until dated demand evidenced via Exa/ORS.",
    ],
    verificationStatus: "VERIFIED_STORE",
  }),
  Object.freeze({
    actorId: "dionysus_way/weddingwire-vendor-scraper-email",
    title: "WeddingWire Venue & Vendor Scraper(+Email)",
    publisher: "dionysus_way",
    publisherType: "community",
    storeUrl: "https://apify.com/dionysus_way/weddingwire-vendor-scraper-email",
    enabled: false,
    discoveryFamilies: [APIFY_DISCOVERY_FAMILY.WEDDINGS_SOCIAL],
    demandSegments: ["social_special_events", "preferred_lodging_partnerships"],
    geoFilter: true,
    dateFilter: false,
    pricingModel: "pay_per_event",
    pricingSummary: "~$0.0054/result + start ~$0.0001 (Bronze)",
    monthlyUsers: 3,
    rating: null,
    inputFields: ["categories", "locations", "maxResults"],
    expectedOutput: "WeddingWire vendors/venues",
    reliabilityNote: "Very low usage — disabled by default until pilot validation.",
    tosNotes: ["WeddingWire ToS; email enrichment increases privacy/ToS risk."],
    verificationStatus: "VERIFIED_STORE",
  }),
  Object.freeze({
    actorId: "apify/facebook-events-scraper",
    title: "Facebook Events Scraper",
    publisher: "apify",
    publisherType: "Apify",
    storeUrl: "https://apify.com/apify/facebook-events-scraper",
    enabled: true,
    discoveryFamilies: [
      APIFY_DISCOVERY_FAMILY.PUBLISHED_EVENTS,
      APIFY_DISCOVERY_FAMILY.SPORTS_ENTERTAINMENT,
      APIFY_DISCOVERY_FAMILY.WEDDINGS_SOCIAL,
    ],
    demandSegments: [
      "associations_conferences_events",
      "sports_entertainment",
      "social_special_events",
      "corporate_celebrations_private_events",
    ],
    geoFilter: true,
    dateFilter: false,
    pricingModel: "pay_per_event",
    pricingSummary: "~$0.01 per event + $0.001 actor start (Bronze)",
    monthlyUsers: 669,
    rating: 4.97,
    inputFields: ["searchQueries", "startUrls", "maxEvents"],
    expectedOutput: "Facebook event name, location, description, interest counts",
    reliabilityNote: "Apify-maintained; useful for public community/sports/social events.",
    tosNotes: ["Facebook ToS / Meta restrictions — public events only; constrain volume."],
    verificationStatus: "VERIFIED_STORE",
  }),
  Object.freeze({
    actorId: "hgservices/apify-actor-espn",
    title: "ESPN Sports Scores & Schedules",
    publisher: "hgservices",
    publisherType: "community",
    storeUrl: "https://apify.com/hgservices/apify-actor-espn",
    enabled: true,
    discoveryFamilies: [APIFY_DISCOVERY_FAMILY.SPORTS_ENTERTAINMENT],
    demandSegments: ["sports_entertainment"],
    geoFilter: false,
    dateFilter: true,
    pricingModel: "pay_per_event",
    pricingSummary: "Actor start ~$0.00005 + result ~$0.001 (Bronze)",
    monthlyUsers: 144,
    rating: 5.0,
    inputFields: [
      "leagues",
      "dateMode",
      "date",
      "startDate",
      "endDate",
      "teams",
    ],
    expectedOutput: "Sports schedules/scores — filter heavily for South Florida relevance",
    reliabilityNote: "Schedule utility; lodging demand not automatic — ORS must prove group travel thesis.",
    tosNotes: ["ESPN content terms — verify redistribution limits."],
    verificationStatus: "VERIFIED_STORE",
  }),
  Object.freeze({
    actorId: "apify/website-content-crawler",
    title: "Website Content Crawler",
    publisher: "apify",
    publisherType: "Apify",
    storeUrl: "https://apify.com/apify/website-content-crawler",
    enabled: true,
    discoveryFamilies: [APIFY_DISCOVERY_FAMILY.WEB_EXTRACTION],
    demandSegments: [
      "associations_conferences_events",
      "education_scientific",
      "government_public_sector",
      "preferred_lodging_partnerships",
      "corporate_accounts_scaling",
    ],
    geoFilter: false,
    dateFilter: false,
    pricingModel: "platform_usage",
    pricingSummary: "Free Actor fee; charged for Apify platform compute/usage only",
    monthlyUsers: 14050,
    rating: 4.61,
    inputFields: ["startUrls", "maxCrawlPages", "maxCrawlDepth", "crawlerType"],
    expectedOutput: "Clean markdown/text from target association/venue/corporate pages",
    reliabilityNote: "Apify official; best as targeted extraction after discovery, not broad discovery.",
    tosNotes: ["Respect robots.txt; crawl only authorized public pages."],
    verificationStatus: "VERIFIED_STORE",
  }),
  Object.freeze({
    actorId: "apify/google-search-scraper",
    title: "Google Search Results Scraper",
    publisher: "apify",
    publisherType: "Apify",
    storeUrl: "https://apify.com/apify/google-search-scraper",
    enabled: true,
    discoveryFamilies: [APIFY_DISCOVERY_FAMILY.SEARCH_SUPPLEMENT],
    demandSegments: [
      "associations_conferences_events",
      "corporate_meetings_incentives",
      "corporate_accounts_scaling",
      "education_scientific",
      "government_public_sector",
    ],
    geoFilter: true,
    dateFilter: false,
    pricingModel: "pay_per_event",
    pricingSummary:
      "SERP page ~$0.0025 + optional AI/ads/leads add-ons (Bronze; verify pricing page)",
    monthlyUsers: 26079,
    rating: 4.65,
    inputFields: ["queries", "maxPagesPerQuery", "countryCode", "searchLanguage"],
    expectedOutput: "SERP organic results for association calendars / programme discovery queries",
    reliabilityNote: "Supplemental to Exa; do not replace Exa Agent discovery.",
    tosNotes: ["Google ToS; use bounded pages; prefer Exa for reasoning-heavy discovery."],
    verificationStatus: "VERIFIED_STORE",
  }),
]);

export function listEnabledApifyActors() {
  return APIFY_ACTOR_REGISTRY.filter((a) => a.enabled === true);
}

export function getApifyActorById(actorId) {
  const id = String(actorId || "").trim();
  return APIFY_ACTOR_REGISTRY.find((a) => a.actorId === id) || null;
}

export function listApifyActorsForSegment(segmentId) {
  const sid = String(segmentId || "").trim();
  return APIFY_ACTOR_REGISTRY.filter(
    (a) => a.enabled && (a.demandSegments || []).includes(sid),
  );
}

export function listApifyActorsForFamily(family) {
  return APIFY_ACTOR_REGISTRY.filter(
    (a) => a.enabled && (a.discoveryFamilies || []).includes(family),
  );
}
