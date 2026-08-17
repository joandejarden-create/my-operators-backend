/**
 * Website-sourced Operator Setup content packs (Wave B).
 * Values from official operator sites (fetched 2026-07-24) — not invented.
 * Keys MUST be Airtable field names on the new-base 1:1 Setup tables
 * (see api/lib/operator-setup-new-base-build-sheet-rows.json). Prefill-only
 * names (industryRecognition, notableAchievements, bestFitOwnerTypes, regions)
 * are folded into real long-text fields below.
 */
import { OPERATOR_FACTORY_QUEUE } from "./operator-explorer-factory-queue.js";
import { BRAND_MANAGED_WEBSITE_CONTENT_PACKS } from "./operator-setup-brand-managed-content.js";
import { PLAYA_WEBSITE_CONTENT_PACK } from "./operator-setup-playa-hotels-content.js";
import { WAVE_D_WEBSITE_CONTENT_PACKS } from "./operator-setup-wave-d-content.js";
import { WAVE_E_WEBSITE_CONTENT_PACKS } from "./operator-setup-wave-e-content.js";

/** @typedef {{ profile?: object, platformMarkets?: object, commercial?: object, governance?: object, sources: Array<{title:string,url:string}> }} SetupContentPack */

function packFor(slug) {
  return OPERATOR_FACTORY_QUEUE.find((o) => o.slug === slug) || null;
}

/** @type {Record<string, SetupContentPack>} */
export const OPERATOR_SETUP_WEBSITE_CONTENT_PACKS = Object.freeze({
  ...BRAND_MANAGED_WEBSITE_CONTENT_PACKS,
  "playa-hotels-resorts": PLAYA_WEBSITE_CONTENT_PACK,
  ...WAVE_D_WEBSITE_CONTENT_PACKS,
  ...WAVE_E_WEBSITE_CONTENT_PACKS,
  "tafer-hotels-resorts": Object.freeze({
    sources: [
      { title: "TAFER About Us", url: "https://www.taferresorts.com/about-us" },
      { title: "TAFER Hotels & Resorts", url: "https://www.taferresorts.com/" },
    ],
    profile: {
      website: "https://www.taferresorts.com/",
      primaryServiceModel: "Resort ownership, development, and management (Mexico leisure)",
      companyDescription:
        "TAFER Hotels & Resorts is a Mexico leisure and hospitality company with an evolving collection of award-winning hotels, resorts, and boutique villas. Official materials describe hotel design and construction, resort management, vacation ownership, residences, marketing and concept design, and tour/travel agent services.",
      companyHistory:
        "TAFER positions itself as a forward-thinking leisure company with more than 30 years of team experience in leisure and tourism. It is the principal owner of The Villagroup Resorts, whose properties feature within the TAFER collection. Portfolio highlights on the official About page include Hotel Mousai (Puerto Vallarta and Cancún), Garza Blanca Preserve Resort & Spa (Puerto Vallarta), Villa del Palmar Cancun Resort & Spa, Sierra Lago Resort & Spa, and Sian Ka’an Village, with continued development and operations throughout Mexico.",
      missionStatement:
        "Merge excellence, quality and creativity to provide extraordinary vacation experiences that can be enjoyed for a day, a week, or a lifetime (taferresorts.com).",
      differentiators:
        "Mexico beach/leisure resort platform spanning design-build, management, vacation ownership, and distinctive brand collection (Mousai, Garza Blanca, Villa del Palmar, Sierra Lago). Official site describes an award-winning collection; specific award names remain diligence items unless separately sourced. Published collection includes Hotel Mousai (PV & Cancún), Garza Blanca Preserve (PV), Villa del Palmar Cancún, Sierra Lago, and Sian Ka’an Village; Villagroup Resorts ownership within the TAFER collection.",
    },
    platformMarkets: {
      specificMarkets:
        "Mexico / Latin America leisure destinations — Puerto Vallarta; Cancún; Los Cabos / Diamante; Mascota (Jalisco) — per official resort list on taferresorts.com",
    },
    commercial: {
      bf_operating_situations:
        "Leisure resort owners and developers in Mexico beach destinations seeking an integrated design-build / management / vacation-ownership platform. Best-fit geographies: Mexico Pacific and Caribbean leisure corridors (PV, Cancún, Los Cabos, Jalisco highlands resort settings).",
      bf_not_ideal_for:
        "Urban-only limited-service mandates outside TAFER leisure resort model; assets outside Mexico without clear expansion plan",
    },
    governance: {
      risk_programs_narrative:
        "Confirm brand, safety, and insurance certifications per asset in diligence — not fully enumerated on public About page. Source: taferresorts.com/about-us.",
    },
  }),

  "grupo-presidente": Object.freeze({
    sources: [
      { title: "Grupo Presidente home", url: "https://grupopresidente.com.mx/" },
    ],
    profile: {
      website: "https://grupopresidente.com.mx/",
      primaryServiceModel: "Hotel and restaurant operations (Mexico)",
      companyDescription:
        "Grupo Presidente is a 100% Mexican hospitality company with more than 50 years of experience. Official materials state its purpose is to create unique experiences that satisfy guests and diners, with excellence as a core value. The company represents Marriott, Hyatt, and IHG brands in Mexico across major cities and beach destinations, and operates more than 50 bars and restaurants including Au Pied de Cochon, Alfredo di Roma, Chapulín, and The Palm.",
      companyHistory:
        "Public site positions Grupo Presidente as a multi-decade Mexican hospitality operator representing major international hotel brands (Marriott, Hyatt, IHG) in cities and beaches across Mexico, with a parallel luxury F&B portfolio and a Ballesol alliance for senior living residences. 50+ years hospitality experience cited on official site.",
      missionStatement:
        "Create unique experiences that bring satisfaction to guests and diners (grupopresidente.com.mx).",
      differentiators:
        "Mexico-only operator representing Marriott, Hyatt, and IHG; city+beach portfolio; 50+ bars/restaurants with named prestige concepts; Travacacion city-to-beach credit program.",
    },
    platformMarkets: {
      specificMarkets:
        "Mexico — major cities and beach destinations including Cancún, Cozumel, Tulum, and Ixtapa (Presidente InterContinental / Kimpton / Holiday Inn Resort examples on official site)",
    },
    commercial: {
      bf_operating_situations:
        "Owners of branded full-service and resort hotels in Mexico seeking a Mexican operator with Marriott/Hyatt/IHG experience and strong F&B. Best-fit geographies: Mexico city and beach gateways.",
      bf_not_ideal_for: "Mandates outside Mexico; owners needing a U.S.-only remote management model",
    },
    governance: {},
  }),

  highgate: Object.freeze({
    sources: [
      { title: "Highgate corporate", url: "https://www.highgate.com/" },
    ],
    profile: {
      website: "https://www.highgate.com/",
      primaryServiceModel: "Hotel management, investment, technology, and development",
      companyDescription:
        "Highgate is a hotel management, investment, technology and development firm with a diverse portfolio across North America, Europe, the Caribbean, and Latin America. Official corporate metrics cite a 30-year track record, 200+ person corporate team, 79,348 keys across 400+ properties, and over $15B aggregate real estate value / $5B+ revenue under management.",
      companyHistory:
        "Highgate presents a multi-decade hospitality investment and management platform spanning lifestyle & luxury resort, full-service, and select-service assets, with in-house venture capital focused on hospitality-linked technology. CALA examples on the corporate site include properties such as Hotel Paracas, a Luxury Collection Resort (Peru) — treat enterprise scale as labeled parent/platform context for any CALA-specific Explorer profile. Official: 400+ properties / 79,348 keys; $15B+ aggregate real estate value; $5B+ revenue under management; 30-year track record.",
      missionStatement:
        "Operational optimization, branding, experiential curation, and favorable partner returns through operational outperformance (highgate.com).",
      differentiators:
        "Integrated management + investment + development + tech VC; large multi-region portfolio; deep revenue management and branding capability. Explorer must label CALA asset examples vs global enterprise scale.",
    },
    platformMarkets: {
      specificMarkets:
        "North America, Europe, Caribbean, Latin America — confirm active CALA managed footprint per asset list in diligence (corporate site cites Caribbean and Latin America among regions)",
      totalProperties: "400+",
      totalRooms: "79,348",
    },
    commercial: {
      bf_operating_situations:
        "Institutional and private owners seeking a full-stack management/investment partner with multi-region capability. Caribbean and Latin America assets within Highgate’s regional coverage — confirm in diligence.",
      bf_not_ideal_for:
        "Owners wanting a Mexico-only boutique operator without global platform; unfunded complex redevelopments without capital plan",
    },
    governance: {
      risk_programs_narrative:
        "Corporate ESG/sustainability program described on highgate.com — confirm property-level certifications in diligence.",
    },
  }),

  "grupo-hotelero-santa-fe": Object.freeze({
    sources: [
      { title: "Grupo Hotelero Santa Fe corporate EN", url: "https://gsf-hotels.com/corporativo/en/" },
    ],
    profile: {
      website: "https://gsf-hotels.com/corporativo/en/",
      primaryServiceModel: "Acquire, develop, and operate proprietary and third-party hotels (Mexico)",
      companyDescription:
        "Grupo Hotelero Santa Fe (S.A.B. de C.V., BMV: HOTEL) is a leading Mexican hotel company focused on acquiring, developing and operating proprietary and third-party hotels. Official materials describe a multi-brand management model with Krystal®, Hyatt®, Hilton® and Secrets®, combining strategic location and quality across Mexico.",
      companyHistory:
        "Public corporate site positions GSF as a publicly listed Mexican hotel group (BMV: HOTEL) operating owned and third-party hotels under Krystal and major international brands, with continuous portfolio growth across beach and city destinations (e.g. Cancún, Vallarta, Mexico City Insurgentes, Tulum, Los Cabos, San Miguel de Allende).",
      missionStatement:
        "Strategic location, exceptional quality, and multi-brand management across proprietary and third-party hotels (gsf-hotels.com).",
      differentiators:
        "Publicly listed Mexican hotel group; proprietary Krystal brand plus Hyatt, Hilton, and Secrets; owner-operator and third-party management mix. Portfolio highlights include Hyatt Regency Mexico City Insurgentes, Secrets Tulum, Krystal Grand properties in Cancún, Vallarta, Los Cabos, and San Miguel de Allende residences.",
    },
    platformMarkets: {
      specificMarkets:
        "Mexico — Cancún; Puerto Vallarta; Mexico City; Tulum; Los Cabos; San Miguel de Allende — per official corporate portfolio highlights",
    },
    commercial: {
      bf_operating_situations:
        "Owners of branded and Krystal-family hotels in Mexico seeking a listed Mexican operator with multi-brand capability. Best-fit geographies: Mexico beach and city destinations in GSF footprint.",
      bf_not_ideal_for: "Assets outside Mexico; owners requiring non-public small-operator intimacy without institutional reporting",
    },
    governance: {},
  }),

  "arriva-hospitality-group": Object.freeze({
    sources: [
      { title: "Arriva Hospitality Group About", url: "https://www.arrivahotels.mx/arriva-hospitality-group" },
      { title: "Arriva Hospitality Group", url: "https://www.arrivahotels.mx/" },
    ],
    profile: {
      website: "https://www.arrivahotels.mx/",
      primaryServiceModel: "Hotel ownership and operations (Mexico city and beach)",
      companyDescription:
        "Arriva Hospitality Group (AHG) is a Mexican hospitality group with more than 50 years in the tourist market. Official materials describe city and beach hotels under brands such as Crown Paradise, Vista, and Sensira, and cite creation of Arriva Hospitality Group in 2012 following a multi-decade hotel operating history dating to 1967.",
      companyHistory:
        "Timeline on the official About page: hotel operations from 1967 (Presidente and Alameda Morelia), Vista Hotels from 1978, Crown Paradise Club Cancun (1990), Crown Paradise Golden Puerto Vallarta (1997), fusion of Aranzazú and Vista (2005), Arriva Hospitality Group created (2012), Westin Cozumel (2017), Ibis Tijuana (2019), Sensira Resort & Spa Riviera Maya (2020). Public home materials also cite roughly 1,700+ rooms across beaches and cities.",
      missionStatement:
        "Make people happy while investments grow — seeking aligned investors (arrivahotels.mx).",
      differentiators:
        "50+ years Mexico tourism operating history; proprietary Crown Paradise / Vista / Sensira brands with select international flags (e.g. Westin, Ibis) in timeline; city+beach mix. Official timeline milestones from 1967–2020; Crown Paradise and Vista portfolios across Cancún, Puerto Vallarta, Riviera Maya, Manzanillo, Morelia, Guadalajara.",
      yearEstablished: 1967,
    },
    platformMarkets: {
      specificMarkets:
        "Mexico — Cancún; Puerto Vallarta; Riviera Maya; Manzanillo; Morelia; Guadalajara; Cozumel; Tijuana — per official hotel list / timeline",
      totalRooms: "1,700+",
    },
    commercial: {
      bf_operating_situations:
        "Investors and owners of Mexico city/beach hotels seeking a long-tenured Mexican operator with proprietary leisure brands. Best-fit geographies: Mexico leisure and secondary city markets in AHG footprint.",
      bf_not_ideal_for: "Outside Mexico; owners needing a pure third-party-only global brand platform without AHG brand mix",
    },
    governance: {},
  }),

  "brittain-resorts-hotels": Object.freeze({
    sources: [
      { title: "Brittain Resorts & Hotels", url: "https://brittainresorts.com/" },
    ],
    profile: {
      website: "https://brittainresorts.com/",
      headquarters: "Myrtle Beach, United States",
      primaryServiceModel: "Full-service hospitality management (US Southeast)",
      companyDescription:
        "Brittain Resorts & Hotels (BRH) is a full-service hospitality management company founded in 1943 and based in the US Southeast (Myrtle Beach, SC). Official materials cite multi-layered expertise across hotel and resort operations, more than 4,700 rooms/suites/condos under management, and 45+ restaurants and bars.",
      companyHistory:
        "Founded in 1943, BRH presents itself as a leading full-service management company in the Southeast with decades of measured operational strategies and proprietary data-driven approaches in HR, revenue management, and sales & marketing. Official: 4,700+ rooms/suites/condos under management; 45+ restaurants and bars.",
      missionStatement:
        "Enrich the lives of team members, guests, partners, and communities through exceptional guest experiences and superior returns (brittainresorts.com).",
      differentiators:
        "Long-tenured Southeast US full-service manager; large F&B footprint (45+ outlets); integrated ops, HR, revenue, sales & marketing, and call center services.",
      yearEstablished: 1943,
    },
    platformMarkets: {
      specificMarkets:
        "United States — Southeast US resort and hotel markets (HQ Myrtle Beach, SC) — confirm CALA relevance before Active Explorer release",
      totalRooms: "4,700+",
    },
    commercial: {
      bf_operating_situations:
        "US Southeast hotel/resort owners seeking full-service third-party management with strong F&B capability. CALA owners should treat as non-core unless a specific mandate fits.",
      bf_not_ideal_for: "CALA-only mandates without US Southeast operating relevance",
    },
    governance: {},
  }),

  "atlantica-hotels-international": Object.freeze({
    sources: [
      { title: "Let's Atlantica — Nossos Hotéis", url: "https://atlanticahotels.com.br/nossos-hoteis" },
    ],
    profile: {
      website: "https://atlanticahotels.com.br/",
      primaryServiceModel: "Hotel management and distribution (Brazil)",
      companyDescription:
        "Atlantica Hotels International (AHI) operates the Let’s Atlantica official sales platform assembling more than 195 hotels and residential properties across more than 75 destinations in Brazil. Public materials associate the platform with brands such as Radisson, Quality, Comfort, Quality Suites, and Metropolitan across business and leisure travel.",
      companyHistory:
        "AHI is a Brazil-focused hospitality management/distribution platform. Official Let’s Atlantica pages emphasize national coverage (195+ hotels / 75+ destinations) and ongoing systems readiness (including tax-reform operational notes published for 2026).",
      missionStatement:
        "Let’s Travel. Let’s Enjoy. Let’s Experience. — national Brazil hotel distribution/operations platform (atlanticahotels.com.br).",
      differentiators:
        "Brazil-wide scale (195+ hotels / 75+ destinations); multi-brand midscale to upper-midscale portfolio (Radisson, Quality, Comfort, etc.). Official: 195+ hotels and residentials in 75+ Brazilian destinations on Let’s Atlantica.",
    },
    platformMarkets: {
      specificMarkets:
        "Brazil / Latin America — 75+ destinations including Northeast leisure/business hubs (e.g. Fortaleza, Natal, Recife, Aracaju) cited in Atlantica destination materials",
      totalProperties: "195+",
    },
    commercial: {
      bf_operating_situations:
        "Owners of midscale/upper-midscale branded hotels in Brazil seeking a national operator/distribution platform. Best-fit geography: Brazil nationwide — confirm any non-Brazil CALA expansion separately.",
      bf_not_ideal_for: "Assets outside Brazil without AHI operating coverage",
    },
    governance: {},
  }),

  "aimbridge-latam": Object.freeze({
    sources: [
      { title: "Aimbridge LATAM home", url: "https://aimbridgelatam.com/" },
      { title: "Aimbridge LATAM home (EN)", url: "https://aimbridgelatam.com/en/home/" },
    ],
    profile: {
      website: "https://aimbridgelatam.com/en/home/",
      primaryServiceModel: "Third-party hotel management (LATAM + All-Inclusive)",
      companyDescription:
        "Aimbridge LATAM is Aimbridge Hospitality’s Latin America third-party hotel management division. Official materials describe operating a diverse portfolio for the modern traveler, combining international brand standards, deep Latin American market knowledge, and a results-oriented culture. The platform partners with owners across Mexico and Latin America on major international brands and independent assets, with a dedicated All-Inclusive division.",
      companyHistory:
        "Parent Aimbridge Hospitality is positioned as a leading global third-party manager; this profile is the LATAM division lens. Public 2026 leadership: Alex Fiz appointed President of the LATAM and All-Inclusive Divisions (effective March 2, 2026), succeeding Leandro Castillo (advisory transition). Named development leadership includes Luis René Sánchez (VP Development — Mexico & Central America) and Javier Sánchez (VP Business Development — Caribbean & All-Inclusive). F&B: Davide Preziuso, Director of Food & Beverage for LATAM & All-Inclusive.",
      missionStatement:
        "Inspiring experiences in every destination — international standards with Latin American market knowledge and a results-oriented culture (aimbridgelatam.com).",
      differentiators:
        "Aimbridge enterprise depth plus in-market LATAM leadership; dedicated All-Inclusive division; public brand alliances with IHG, Wyndham, Marriott, and Hilton; Mexico-weighted growth with Caribbean expansion signals (e.g. Noval Properties Dominican Republic alliance; Grupo Satli / Marriott Riviera Maya all-inclusive project announced May 2026).",
    },
    platformMarkets: {
      specificMarkets:
        "Mexico (properties throughout the Mexican Republic per aimbridgelatam.com) plus Latin America growth corridors; Caribbean & All-Inclusive development coverage; Dominican Republic expansion via Noval Properties alliance; Riviera Maya all-inclusive pipeline with Marriott / Grupo Satli",
    },
    commercial: {
      bf_operating_situations:
        "Owners of branded (IHG, Wyndham, Marriott, Hilton) or independent hotels in Mexico and Latin America seeking third-party management with Aimbridge enterprise backing and in-region LATAM / All-Inclusive capability. Best-fit geographies: Mexico city and leisure markets, Central America, Caribbean all-inclusive.",
      bf_not_ideal_for:
        "Owners needing a Mexico-only boutique independent operator without enterprise platform; assets outside Aimbridge LATAM operating coverage without clear regional mandate",
    },
    governance: {
      risk_programs_narrative:
        "Brand-standard QA and franchise compliance by asset (Marriott, Hilton, IHG, Wyndham). Aimbridge enterprise safety/continuity frameworks scaled to LATAM — confirm property-level certifications in diligence. Source: aimbridgelatam.com.",
    },
  }),
});

export function listWebsiteContentPackSlugs() {
  return Object.keys(OPERATOR_SETUP_WEBSITE_CONTENT_PACKS);
}

export function getWebsiteContentPack(slug) {
  return OPERATOR_SETUP_WEBSITE_CONTENT_PACKS[slug] || null;
}

export function resolvePackMasterMeta(slug) {
  const q = packFor(slug);
  const pack = getWebsiteContentPack(slug);
  if (!q || !pack) return null;
  return {
    slug,
    recordId: q.recordId,
    companyName: q.companyName,
    domain: q.domain,
    pack,
  };
}
