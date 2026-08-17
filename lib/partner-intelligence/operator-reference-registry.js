/**
 * Curated public sources for third-party operator reference harvesting.
 * Explorer-relevant pages only — not full property microsites.
 */

/** @typedef {{ url: string, title: string, category?: string, typeKey?: string }} OperatorSourcePage */

/** @typedef {{ key: string, companyFolder: string, domain: string, operatorId?: string, websitePages: OperatorSourcePage[], pressPages?: OperatorSourcePage[] }} OperatorHarvestProfile */

/** @type {Record<string, OperatorHarvestProfile>} */
export const OPERATOR_HARVEST_PROFILES = {
  arborLodging: {
    key: "arborLodging",
    companyFolder: "Arbor Lodging",
    domain: "arborlodging.com",
    operatorId: "recF5Z87OAqFgndoq",
    /** Cloudflare blocks rapid sequential navigation in one browser session */
    cloudflareSensitive: true,
    websitePages: [
      { url: "https://www.arborlodging.com/", title: "Arbor Lodging home" },
      {
        url: "https://www.arborlodging.com/hotel-real-estate-investment-company",
        title: "About Arbor Lodging investment and management",
      },
      { url: "https://www.arborlodging.com/platforms", title: "Arbor platforms and CALA hub" },
      { url: "https://www.arborlodging.com/portfolio", title: "Arbor portfolio" },
      { url: "https://www.arborlodging.com/team", title: "Arbor leadership team" },
      { url: "https://www.arborlodging.com/services", title: "Arbor services overview" },
      {
        url: "https://www.arborlodging.com/services/hotel-management",
        title: "Arbor hotel management services",
      },
      {
        url: "https://www.arborlodging.com/services/hotel-accounting",
        title: "Arbor hotel accounting services",
      },
      {
        url: "https://www.arborlodging.com/services/hotel-investment",
        title: "Arbor hotel investment services",
      },
      { url: "https://www.arborlodging.com/press", title: "Arbor press and news" },
      { url: "https://www.arborlodging.com/contact", title: "Arbor contact" },
    ],
    pressPages: [
      {
        url: "https://www.hotelinvestmenttoday.com/Development/Owners/What-makes-sense-today-for-Arbor-Lodging",
        title: "Hotel Investment Today — Arbor Lodging profile",
        category: "press",
        typeKey: "press",
      },
    ],
  },

  brittainResorts: {
    key: "brittainResorts",
    companyFolder: "Brittain Resorts & Hotels",
    domain: "brittainresorts.com",
    websitePages: [
      { url: "https://www.brittainresorts.com/", title: "Brittain Resorts home" },
      { url: "https://www.brittainresorts.com/about-us/", title: "About Brittain Resorts" },
      { url: "https://www.brittainresorts.com/services/", title: "Brittain management services" },
      { url: "https://www.brittainresorts.com/operations/", title: "Brittain operations" },
      { url: "https://www.brittainresorts.com/revenue/", title: "Brittain revenue management" },
      { url: "https://www.brittainresorts.com/sales/", title: "Brittain sales and marketing" },
      { url: "https://www.brittainresorts.com/marketing/", title: "Brittain marketing" },
      { url: "https://www.brittainresorts.com/human-resources/", title: "Brittain human resources" },
      { url: "https://www.brittainresorts.com/accounting/", title: "Brittain accounting" },
      { url: "https://www.brittainresorts.com/technology/", title: "Brittain technology" },
      { url: "https://www.brittainresorts.com/project-management/", title: "Brittain project management" },
      { url: "https://www.brittainresorts.com/our-portfolio/", title: "Brittain portfolio" },
      { url: "https://www.brittainresorts.com/meet-our-team/", title: "Brittain leadership team" },
      { url: "https://www.brittainresorts.com/news/", title: "Brittain news and awards" },
      { url: "https://www.brittainresorts.com/contact-us/", title: "Brittain owner contact" },
      { url: "https://www.brittainresorts.com/homeowner-services/", title: "Brittain homeowner services" },
      { url: "https://www.brittainresorts.com/giving-back/", title: "Brittain community giving" },
      {
        url: "https://www.brittainresorts.com/owner-partnerships-that-last-the-foundation-of-successful-resort-management/",
        title: "Brittain owner partnerships article",
        category: "website",
      },
      {
        url: "https://www.brittainresorts.com/the-rise-of-the-condo-hotel-what-owners-should-know/",
        title: "Brittain condo hotel owner article",
        category: "website",
      },
      {
        url: "https://www.brittainresorts.com/from-ota-dependence-to-direct-booking-strength/",
        title: "Brittain direct booking article",
        category: "website",
      },
    ],
    pressPages: [
      {
        url: "https://www.hospitalitynet.org/news/4125779/brittain-resorts-hotels-transforms-three-premier-properties-with-58-million-investment",
        title: "Hospitality Net — Brittain 58M investment",
        category: "press",
        typeKey: "press",
      },
    ],
  },

  playaHotelsResorts: {
    key: "playaHotelsResorts",
    companyFolder: "Playa Hotels & Resorts",
    domain: "playaresorts.com",
    websitePages: [
      { url: "https://www.playaresorts.com/", title: "Playa / Resorts by Hyatt home" },
      {
        url: "https://investors.playaresorts.com/",
        title: "Playa Hotels & Resorts investor relations",
        typeKey: "website-capture",
        category: "website",
      },
      {
        url: "https://investors.playaresorts.com/2025-05-05-Playa-Hotels-Resorts-N-V-Reports-First-Quarter-2025-Results",
        title: "Playa Q1 2025 results — portfolio description",
        typeKey: "website-capture",
        category: "website",
      },
    ],
    pressPages: [
      {
        url: "https://www.hospitalitynet.org/organization/17016574/playa-hotels",
        title: "Hospitality Net — Playa Hotels & Resorts profile",
        category: "press",
        typeKey: "press",
      },
      {
        url: "https://investors.hyatt.com/news/investor-news/news-details/2025/Hyatt-Strengthens-Leadership-in-All-Inclusive-Segment-with-Acquisition-of-Playa-Hotels--Resorts-N-V-/default.aspx",
        title: "Hyatt — acquisition of Playa Hotels & Resorts N.V.",
        category: "press",
        typeKey: "press",
      },
    ],
  },
};

/**
 * @param {string} keyOrFolder
 * @returns {OperatorHarvestProfile|undefined}
 */
const OPERATOR_ALIASES = {
  arbor: "arborLodging",
  "arbor lodging": "arborLodging",
  brittain: "brittainResorts",
  "brittain resorts": "brittainResorts",
  "brittain resorts & hotels": "brittainResorts",
  playa: "playaHotelsResorts",
  "playa hotels": "playaHotelsResorts",
  "playa hotels & resorts": "playaHotelsResorts",
  "playa-hotels-resorts": "playaHotelsResorts",
};

export function getOperatorHarvestProfile(keyOrFolder) {
  const raw = String(keyOrFolder || "").trim().toLowerCase();
  const resolvedKey = String(OPERATOR_ALIASES[raw] || raw).toLowerCase();
  return Object.values(OPERATOR_HARVEST_PROFILES).find(
    (p) =>
      p.key.toLowerCase() === resolvedKey ||
      p.companyFolder.toLowerCase() === raw ||
      p.companyFolder.toLowerCase() === resolvedKey
  );
}

export function listOperatorHarvestProfileKeys() {
  return Object.keys(OPERATOR_HARVEST_PROFILES);
}
