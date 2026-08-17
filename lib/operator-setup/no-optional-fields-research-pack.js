/**
 * D.4C curated research pack — field-isolated facts for required narratives.
 * Sources: official company / filings / established corporate history.
 * Controlled states only after OE + targeted research.
 */
import { CTRL } from "./no-optional-fields-policy.js";

/**
 * @typedef {{
 *   companyDescription?: string,
 *   companyHistory?: string,
 *   differentiators?: string|typeof CTRL.NO_DIFF,
 *   cap_profile_operational?: string|typeof CTRL.NO_OPS,
 *   headquarters?: string,
 *   companySize?: string,
 *   softBrand?: string,
 *   brandFamilies?: string[],
 *   serviceModels?: string[],
 *   propertyTypes?: string[],
 *   additionalExperience?: string[],
 *   chainScales?: string[],
 *   specificMarkets?: string,
 *   researchNotes?: string[],
 *   sources?: {title:string,url:string}[],
 *   fidelity?: {companyDescription?:string,companyHistory?:string,differentiators?:string,cap_profile_operational?:string}
 * }} ResearchRow
 */

/** @type {Record<string, ResearchRow>} */
export const RESEARCH_PACK = {
  recF5Z87OAqFgndoq: {
    companyDescription:
      "Arbor Lodging Partners is a third-party hotel management company with a CALA-focused operating presence managing branded hotels for owners.",
    companyHistory:
      "Arbor Lodging Partners developed as a U.S. hotel management platform and expanded a CALA-oriented third-party management practice for branded hotels.",
    differentiators:
      "CALA-oriented third-party branded-hotel management with documented regional operating leadership and owner-facing operating discipline (Arbor CALA exemplar).",
    brandFamilies: ["Marriott", "Hilton", "IHG", "Hyatt", "Other"],
    serviceModels: ["Third-Party Management", "Select-service", "Full-service"],
    propertyTypes: ["Select Service", "Full Service"],
    chainScales: ["Upscale", "Upper Midscale", "Midscale"],
    softBrand: "Limited",
    researchNotes: ["Exemplar — KEEP EXISTING narrative cells when already high quality"],
    sources: [{ title: "Arbor Lodging", url: "https://arborlodging.com" }],
    fidelity: {
      companyDescription: "SUPPORTED_SYNTHESIS",
      companyHistory: "SUPPORTED_SYNTHESIS",
      differentiators: "SUPPORTED_SYNTHESIS",
    },
  },
  recWPKu5laVZxsvpn: {
    companyDescription:
      "Hotel Equities is a third-party hotel management company with a CALA practice managing branded hotels for owners.",
    companyHistory:
      "Hotel Equities developed as a U.S. hotel management company and built a CALA-oriented management practice for branded hotels.",
    differentiators:
      "CALA third-party branded-hotel management with documented regional operating resources and owner-aligned operating discipline (Hotel Equities CALA exemplar).",
    brandFamilies: ["Marriott", "Hilton", "IHG", "Hyatt", "Other"],
    serviceModels: ["Third-Party Management", "Select-service", "Full-service"],
    propertyTypes: ["Select Service", "Full Service"],
    chainScales: ["Upscale", "Upper Midscale", "Midscale"],
    softBrand: "Limited",
    researchNotes: ["Exemplar — KEEP EXISTING narrative cells when already high quality"],
    sources: [{ title: "Hotel Equities CALA", url: "https://hotelequities.com/cala.htm" }],
    fidelity: {
      companyDescription: "SUPPORTED_SYNTHESIS",
      companyHistory: "SUPPORTED_SYNTHESIS",
      differentiators: "SUPPORTED_SYNTHESIS",
    },
  },
  recGmiPhRt6hiayd9: {
    companyDescription:
      "Marriott International’s managed-hotel channel operates branded hotels under Marriott brand standards with corporate management agreements (Managed by Marriott / MxM), distinct from franchise-only delivery.",
    companyHistory:
      "Marriott began as a root-beer stand (1927) and entered lodging with the Twin Bridges Motor Hotel (1957); it grew into a global brand-manager and franchise platform, later acquiring Starwood (2016) and expanding the managed and franchised estate under Marriott International, Inc.",
    differentiators:
      "Managed-by-Marriott operating agreements combine brand standards with Marriott corporate hotel management (MxM), rather than franchise-only or third-party independent management.",
    cap_profile_operational:
      "Managed hotels run under Marriott brand operating systems with corporate managed-hotel oversight (Managed by Marriott / MxM), not a third-party multi-brand management-company stack.",
    brandFamilies: ["Marriott"],
    serviceModels: ["Full-service", "Select-service", "Extended stay", "Lifestyle"],
    propertyTypes: ["Full Service", "Select Service", "Extended Stay", "Lifestyle"],
    chainScales: ["Luxury", "Upper Upscale", "Upscale", "Upper Midscale"],
    softBrand: "Moderate",
    sources: [{ title: "Marriott Hotel Development — Managed by Marriott", url: "https://www.hotel-development.marriott.com/how-we-work-together/managed-by-marriott" }],
    fidelity: {
      companyDescription: "SUPPORTED_SYNTHESIS",
      companyHistory: "VERIFIED",
      differentiators: "SUPPORTED_SYNTHESIS",
      cap_profile_operational: "SUPPORTED_SYNTHESIS",
    },
  },
  rec3Uwxe6ovpiokuN: {
    companyDescription:
      "Hilton’s managed channel operates Hilton-branded hotels under Hilton brand standards and Hilton management agreements, alongside a larger franchise estate.",
    companyHistory:
      "Conrad Hilton opened the first Hilton in 1919 (Cisco, Texas). Hilton Worldwide Holdings today is a global brand company using franchise and management contracts across Hilton, Curio, Tapestry, Waldorf Astoria and other brands.",
    differentiators:
      "Hilton Honors-linked brand systems and Hilton management agreements (including HITS / OnQ environments) structure managed-hotel delivery separately from pure franchise.",
    brandFamilies: ["Hilton", "Soft brands / collections"],
    serviceModels: ["Full-service", "Select-service", "Lifestyle", "Extended stay"],
    propertyTypes: ["Full Service", "Select Service", "Lifestyle", "Extended Stay"],
    chainScales: ["Luxury", "Upper Upscale", "Upscale", "Upper Midscale", "Midscale"],
    softBrand: "Moderate",
    sources: [{ title: "Hilton corporate", url: "https://www.hilton.com" }],
    fidelity: {
      companyDescription: "SUPPORTED_SYNTHESIS",
      companyHistory: "VERIFIED",
      differentiators: "SUPPORTED_SYNTHESIS",
    },
  },
  recF2WqLqNVyKGz9E: {
    companyDescription:
      "Accor is a global hospitality group that franchises and manages hotels across economy-to-luxury brands (including Sofitel, Fairmont, Novotel, ibis), with a managed-hotel development channel for owners.",
    companyHistory:
      "Accor was founded in 1967 (Novotel) in France and expanded through multi-brand growth and acquisitions (including Fairmont Raffles Hotels International), operating today as Accor SA with franchise and management contracts worldwide.",
    differentiators:
      "Multi-brand Accor platform spanning economy through luxury with distinct brand operating standards under one corporate owner-facing development organization.",
    brandFamilies: ["Accor", "Soft brands / collections"],
    serviceModels: ["Limited-service", "Select-service", "Full-service", "Lifestyle"],
    propertyTypes: ["Full Service", "Select Service", "Lifestyle"],
    chainScales: ["Luxury", "Upper Upscale", "Upscale", "Upper Midscale", "Midscale", "Economy"],
    softBrand: "Moderate",
    sources: [{ title: "Accor Hotel Development", url: "https://group.accor.com/en/hotel-development" }],
    fidelity: {
      companyDescription: "SUPPORTED_SYNTHESIS",
      companyHistory: "VERIFIED",
      differentiators: "SUPPORTED_SYNTHESIS",
    },
  },
  rec7IXYQYpKMYsrDl: {
    companyDescription:
      "IHG Hotels & Resorts is a global brand company that franchises and manages hotels under InterContinental, Hotel Indigo, Holiday Inn, Crowne Plaza, Kimpton and other brands.",
    companyHistory:
      "InterContinental Hotels roots date to 1946; today’s IHG PLC combines InterContinental, Holiday Inn and later acquisitions (including Kimpton) under a predominantly franchise model with selective management contracts.",
    differentiators:
      "IHG Concerto / Digital Advantage brand operating stack and Strategic Alliance structures differentiate managed/franchised delivery from independent third-party operators.",
    brandFamilies: ["IHG", "Soft brands / collections"],
    serviceModels: ["Full-service", "Select-service", "Lifestyle", "Extended stay"],
    propertyTypes: ["Full Service", "Select Service", "Lifestyle", "Extended Stay"],
    chainScales: ["Luxury", "Upper Upscale", "Upscale", "Upper Midscale", "Midscale"],
    softBrand: "Moderate",
    sources: [{ title: "IHG", url: "https://www.ihg.com" }],
    fidelity: {
      companyDescription: "SUPPORTED_SYNTHESIS",
      companyHistory: "VERIFIED",
      differentiators: "SUPPORTED_SYNTHESIS",
    },
  },
  reculkMOYWDxX14Pv: {
    companyDescription:
      "Hyatt Hotels Corporation franchises and manages hotels under Hyatt, Park Hyatt, Andaz, Unbound Collection and other brands, with a managed-hotel channel for owners.",
    companyHistory:
      "Hyatt’s modern lodging business began with the Hyatt House near Los Angeles International Airport (1957) under the Pritzker family; Hyatt Hotels Corporation today is a global brand-manager and franchisor.",
    differentiators:
      "Hyatt brand operating systems (including World of Hyatt and brand-specific standards) structure managed hotels separately from third-party independent management companies.",
    brandFamilies: ["Hyatt", "Soft brands / collections"],
    serviceModels: ["Full-service", "Select-service", "Lifestyle"],
    propertyTypes: ["Full Service", "Select Service", "Lifestyle", "Resort"],
    chainScales: ["Luxury", "Upper Upscale", "Upscale"],
    softBrand: "Moderate",
    sources: [{ title: "Hyatt", url: "https://www.hyatt.com" }],
    fidelity: {
      companyDescription: "SUPPORTED_SYNTHESIS",
      companyHistory: "VERIFIED",
      differentiators: "SUPPORTED_SYNTHESIS",
    },
  },
  recLjxtxIIVJaGbXK: {
    companyDescription:
      "Highgate is a third-party hotel investment and management company operating branded and independent hotels across lifestyle/luxury, resort, and full-/select-service segments.",
    companyHistory:
      "Highgate was formed in the early 2000s as a hotel investment and operating platform and expanded into a large U.S. and international third-party management and ownership platform.",
    differentiators:
      "Segment-organized operating business units (lifestyle & luxury, Caribbean & Latin America resort, full- and select-service) with dedicated support teams rather than a single undifferentiated ops pool.",
    brandFamilies: ["Marriott", "Hilton", "Hyatt", "IHG", "Independent", "Soft brands / collections"],
    serviceModels: ["Third-Party Management", "Full-service", "Select-service", "Lifestyle", "Resort"],
    propertyTypes: ["Full Service", "Select Service", "Lifestyle", "Resort", "Boutique"],
    chainScales: ["Luxury", "Upper Upscale", "Upscale", "Upper Midscale"],
    softBrand: "Moderate",
    additionalExperience: ["Urban", "Resort", "Conversion"],
    sources: [{ title: "Highgate", url: "https://highgate.com" }],
    fidelity: {
      companyDescription: "SUPPORTED_SYNTHESIS",
      companyHistory: "SUPPORTED_SYNTHESIS",
      differentiators: "SUPPORTED_SYNTHESIS",
    },
  },
  recGWxIJqnYHkJZFD: {
    companyDescription:
      "Aimbridge Hospitality (LATAM) is the Latin America division of Aimbridge Hospitality, a third-party hotel management company operating branded hotels across the region.",
    companyHistory:
      "Aimbridge Hospitality grew as a U.S. third-party management platform and expanded regionally; the LATAM division operates branded hotels under Aimbridge’s enterprise operating model.",
    differentiators:
      "S.P.A.R.K. property-operations discipline (standardized checklists, visit planning, action tracking) applied across Aimbridge regions including LATAM.",
    brandFamilies: ["Marriott", "Hilton", "Hyatt", "IHG", "Other"],
    serviceModels: ["Third-Party Management", "Full-service", "Select-service"],
    propertyTypes: ["Full Service", "Select Service"],
    chainScales: ["Upper Upscale", "Upscale", "Upper Midscale", "Midscale"],
    softBrand: "Limited",
    sources: [{ title: "Aimbridge", url: "https://www.aimbridge.com" }],
    fidelity: {
      companyDescription: "SUPPORTED_SYNTHESIS",
      companyHistory: "SUPPORTED_SYNTHESIS",
      differentiators: "DIRECTLY SUPPORTED",
    },
  },
  reciI2tYQBfMoMK9G: {
    companyDescription:
      "GHL Hoteles (GHL Holding) is a Latin American hotel operator and brand platform managing proprietary GHL brands and selected branded affiliations across Andean and regional markets.",
    companyHistory:
      "GHL Holding developed as a Colombian-rooted hotel group and expanded across Latin America with owned brands and third-party management relationships.",
    differentiators:
      "Regional-operator-led platform combining proprietary GHL brands with branded affiliations under in-market operating leadership.",
    brandFamilies: ["Independent", "Other", "Soft brands / collections"],
    serviceModels: ["Third-Party Management", "Full-service", "Select-service"],
    propertyTypes: ["Full Service", "Select Service", "Boutique"],
    chainScales: ["Upscale", "Upper Midscale", "Midscale"],
    softBrand: "Limited",
    sources: [{ title: "GHL Hoteles", url: "https://www.ghlhoteles.com" }],
    fidelity: {
      companyDescription: "SUPPORTED_SYNTHESIS",
      companyHistory: "SUPPORTED_SYNTHESIS",
      differentiators: "SUPPORTED_SYNTHESIS",
    },
  },
  rec6UB6RpMKSs2tAo: {
    companyDescription:
      "Remington Hospitality is a U.S.-based third-party hotel management company operating branded select- and full-service hotels for owners.",
    companyHistory:
      "Remington developed as a U.S. hotel management company focused on branded hotel operations for institutional and private owners; public materials emphasize management services rather than a disclosed proprietary ops named platform.",
    differentiators: CTRL.NO_DIFF,
    cap_profile_operational: CTRL.NO_OPS,
    brandFamilies: ["Marriott", "Hilton", "Hyatt", "IHG", "Other"],
    serviceModels: ["Third-Party Management", "Select-service", "Full-service"],
    propertyTypes: ["Select Service", "Full Service"],
    chainScales: ["Upscale", "Upper Midscale", "Midscale"],
    softBrand: "Limited",
    researchNotes: [
      "Targeted review of remingtonhospitality.com — marketing-level ops claims only; no named operating mechanism suitable for Writer v2",
    ],
    sources: [{ title: "Remington Hospitality", url: "https://www.remingtonhospitality.com/" }],
    fidelity: {
      companyDescription: "SUPPORTED_SYNTHESIS",
      companyHistory: "SUPPORTED_SYNTHESIS",
      differentiators: "NOT_PUBLICLY_DISCLOSED",
      cap_profile_operational: "NOT_PUBLICLY_DISCLOSED",
    },
  },
  recwEHUotSGpfkZEJ: {
    companyDescription:
      "Grupo Iberostar is an integrated owner–brand–operator of beachfront and urban hotels, primarily under Iberostar Hotels & Resorts brands.",
    companyHistory:
      "Iberostar began as a Mallorca family travel business and developed into Grupo Iberostar, an integrated hotel owner-operator with a large beachfront resort portfolio and Wave of Change sustainability program.",
    differentiators:
      "Integrated owner–brand–operator model with beachfront resort operating depth and Wave of Change sustainability operating commitments.",
    brandFamilies: ["Independent", "Other"],
    serviceModels: ["Full-service", "All-inclusive", "Resort"],
    propertyTypes: ["Resort", "All-Inclusive", "Full Service"],
    chainScales: ["Upper Upscale", "Upscale"],
    softBrand: "Limited",
    additionalExperience: ["Resort"],
    sources: [{ title: "Iberostar", url: "https://www.iberostar.com" }],
    fidelity: {
      companyDescription: "SUPPORTED_SYNTHESIS",
      companyHistory: "VERIFIED",
      differentiators: "SUPPORTED_SYNTHESIS",
    },
  },
  rec04aLAfmupWG4ZK: {
    companyDescription:
      "Barceló Hotel Group is an integrated Spanish owner–brand–operator running hotels under Barceló, Occidental, Allegro and Royal Hideaway brands.",
    companyHistory:
      "Barceló began in Mallorca (1931 travel roots; hotel growth mid-20th century) and expanded into a global integrated hotel group with owned brands across leisure and urban hotels.",
    differentiators:
      "Integrated owner–brand–operator control across Barceló/Occidental/Allegro/Royal Hideaway rather than a third-party management-company model.",
    brandFamilies: ["Independent", "Other"],
    serviceModels: ["Full-service", "All-inclusive", "Resort"],
    propertyTypes: ["Resort", "All-Inclusive", "Full Service"],
    chainScales: ["Upper Upscale", "Upscale", "Upper Midscale"],
    softBrand: "Limited",
    sources: [{ title: "Barceló", url: "https://www.barcelo.com" }],
    fidelity: {
      companyDescription: "SUPPORTED_SYNTHESIS",
      companyHistory: "VERIFIED",
      differentiators: "SUPPORTED_SYNTHESIS",
    },
  },
  rec28eZ7ERwc92XWd: {
    companyDescription:
      "Meliá Hotels International is a Spanish hospitality company that owns brands (including Meliá, Gran Meliá, ME, Innside) and operates hotels through management and franchise contracts.",
    companyHistory:
      "Founded by Gabriel Escarrer in Palma (1956), Meliá grew into a listed Spanish hotel company with a multi-brand portfolio and international management/franchise footprint.",
    differentiators:
      "Spanish multi-brand hotel company combining owned brands with management and franchise contracts under Meliá corporate brand governance.",
    cap_profile_operational:
      "Hotels operate under Meliá brand standards with corporate brand-operator oversight across Meliá’s brand family, rather than as a third-party multi-brand management-company platform.",
    brandFamilies: ["Independent", "Other", "Soft brands / collections"],
    serviceModels: ["Full-service", "Lifestyle", "Resort", "All-inclusive"],
    propertyTypes: ["Full Service", "Lifestyle", "Resort", "All-Inclusive"],
    chainScales: ["Luxury", "Upper Upscale", "Upscale"],
    softBrand: "Moderate",
    sources: [{ title: "Meliá", url: "https://www.melia.com" }],
    fidelity: {
      companyDescription: "SUPPORTED_SYNTHESIS",
      companyHistory: "VERIFIED",
      differentiators: "SUPPORTED_SYNTHESIS",
      cap_profile_operational: "SUPPORTED_SYNTHESIS",
    },
  },
  rec8XpNv6G0WOlMwu: {
    companyDescription:
      "Shangri-La Group is an Asia-rooted luxury brand-operator managing Shangri-La and related hotels primarily across Asia Pacific, the Middle East, Europe and Africa.",
    companyHistory:
      "Shangri-La opened its first hotel in Singapore (1971) under the Kuok group and developed into Shangri-La Asia Ltd, a listed luxury hotel owner-operator with a predominantly Asia-centered portfolio.",
    differentiators:
      "Integrated luxury brand-operator platform centered on Shangri-La brand standards with a primarily Asia Pacific and Middle East footprint.",
    brandFamilies: ["Independent", "Other"],
    serviceModels: ["Full-service"],
    propertyTypes: ["Full Service", "Resort"],
    chainScales: ["Luxury", "Upper Upscale"],
    softBrand: "None documented",
    specificMarkets:
      "Global brand-operator footprint across Asia Pacific, Middle East, Europe and Africa (~106 hotels / 22 countries-regions YE2025 per company disclosures). No Shangri-La inventory mapped into Dealality CALA Active Countries taxonomy.",
    sources: [{ title: "Shangri-La", url: "https://www.shangri-la.com" }],
    fidelity: {
      companyDescription: "SUPPORTED_SYNTHESIS",
      companyHistory: "VERIFIED",
      differentiators: "SUPPORTED_SYNTHESIS",
    },
  },
  rechnXKjpeiNMaqjJ: {
    companyDescription:
      "Four Seasons Hotels and Resorts is a luxury brand-operator that manages Four Seasons hotels and resorts worldwide under long-term management agreements.",
    companyHistory:
      "Four Seasons was founded by Isadore Sharp in Toronto (1960) and grew into a global luxury management company; ownership later included Cascade Investment / Bill Gates and Kingdom Holding.",
    differentiators:
      "Luxury-only brand-managed operating platform under Four Seasons standards with management-agreement delivery (not a multi-brand third-party operator).",
    brandFamilies: ["Independent", "Other"],
    serviceModels: ["Full-service"],
    propertyTypes: ["Full Service", "Resort"],
    chainScales: ["Luxury"],
    softBrand: "None documented",
    sources: [{ title: "Four Seasons", url: "https://www.fourseasons.com" }],
    fidelity: {
      companyDescription: "SUPPORTED_SYNTHESIS",
      companyHistory: "VERIFIED",
      differentiators: "SUPPORTED_SYNTHESIS",
    },
  },
  recji1awMffccwox2: {
    companyDescription:
      "Rosewood Hotel Group is a luxury brand-operator managing Rosewood and related hotels under brand management agreements.",
    companyHistory:
      "Rosewood originated in the United States and was later acquired by New World Development / Chou family interests; today Rosewood Hotel Group manages ultra-luxury Rosewood-branded hotels globally.",
    differentiators:
      "Ultra-luxury Rosewood brand-managed operating platform (A Sense of Place) distinct from multi-brand third-party management companies.",
    cap_profile_operational:
      "Rosewood hotels are operated under Rosewood brand management standards and corporate brand operating oversight rather than a third-party multi-brand management-company model.",
    brandFamilies: ["Independent", "Other"],
    serviceModels: ["Full-service"],
    propertyTypes: ["Full Service", "Resort"],
    chainScales: ["Luxury"],
    softBrand: "None documented",
    sources: [{ title: "Rosewood", url: "https://www.rosewoodhotels.com" }],
    fidelity: {
      companyDescription: "SUPPORTED_SYNTHESIS",
      companyHistory: "SUPPORTED_SYNTHESIS",
      differentiators: "SUPPORTED_SYNTHESIS",
      cap_profile_operational: "SUPPORTED_SYNTHESIS",
    },
  },
  recVtNxNeeYlngtUk: {
    companyDescription:
      "Auberge Resorts Collection is a luxury resort and hotel brand-operator managing Auberge-branded and affiliated lifestyle resorts.",
    companyHistory:
      "Auberge Resorts Collection grew from California resort roots into a luxury collection brand-operator managing destination resorts and hotels under the Auberge brand platform.",
    differentiators:
      "Luxury destination-resort brand collection with brand-managed operating standards rather than a broad third-party multi-brand management platform.",
    cap_profile_operational:
      "Properties operate under Auberge Resorts Collection brand standards with brand-managed leadership for luxury destination resorts.",
    brandFamilies: ["Independent", "Soft brands / collections", "Other"],
    serviceModels: ["Full-service", "Lifestyle", "Resort"],
    propertyTypes: ["Resort", "Lifestyle", "Full Service"],
    chainScales: ["Luxury", "Upper Upscale"],
    softBrand: "Strong",
    additionalExperience: ["Resort"],
    sources: [{ title: "Auberge", url: "https://www.aubergeresorts.com" }],
    fidelity: {
      companyDescription: "SUPPORTED_SYNTHESIS",
      companyHistory: "SUPPORTED_SYNTHESIS",
      differentiators: "SUPPORTED_SYNTHESIS",
      cap_profile_operational: "SUPPORTED_SYNTHESIS",
    },
  },
  rec5xdV2THfFjEUPk: {
    companyDescription:
      "Mandarin Oriental Hotel Group is a luxury brand-operator managing Mandarin Oriental hotels under management agreements, with a historically Asia-centered portfolio.",
    companyHistory:
      "Mandarin Oriental traces to The Mandarin (Hong Kong, 1963) and The Oriental (Bangkok); the group combined under Jardine Matheson-related ownership as Mandarin Oriental Hotel Group.",
    differentiators:
      "Ultra-luxury Mandarin Oriental brand-managed platform with corporate brand operating standards.",
    cap_profile_operational:
      "Hotels operate under Mandarin Oriental brand standards with corporate brand-operator oversight rather than third-party multi-brand management.",
    brandFamilies: ["Independent", "Other"],
    serviceModels: ["Full-service"],
    propertyTypes: ["Full Service", "Resort"],
    chainScales: ["Luxury"],
    softBrand: "None documented",
    sources: [{ title: "Mandarin Oriental", url: "https://www.mandarinoriental.com" }],
    fidelity: {
      companyDescription: "SUPPORTED_SYNTHESIS",
      companyHistory: "VERIFIED",
      differentiators: "SUPPORTED_SYNTHESIS",
      cap_profile_operational: "SUPPORTED_SYNTHESIS",
    },
  },
  rec8SrT3VjRkkYTxm: {
    companyDescription:
      "Minor Hotels is the hotel arm of Minor International, operating and managing brands including Anantara, Avani, NH Hotels, Tivoli and others across multiple regions.",
    companyHistory:
      "Minor International expanded from Thailand into a multi-brand hotel group; acquisitions (including NH Hotel Group) created today’s Minor Hotels managed and franchised portfolio.",
    differentiators:
      "Multi-brand Minor Hotels platform (Anantara, Avani, NH, Tivoli and others) under Minor International corporate hotel operations.",
    cap_profile_operational:
      "Minor Hotels operates a multi-brand hotel platform in which property operations follow brand-specific standards under Minor corporate hotel operating oversight.",
    brandFamilies: ["Other", "Independent", "Soft brands / collections"],
    serviceModels: ["Full-service", "Select-service", "Lifestyle", "Resort"],
    propertyTypes: ["Full Service", "Select Service", "Lifestyle", "Resort"],
    chainScales: ["Luxury", "Upper Upscale", "Upscale", "Upper Midscale"],
    softBrand: "Moderate",
    sources: [{ title: "Minor Hotels", url: "https://www.minorhotels.com" }],
    fidelity: {
      companyDescription: "SUPPORTED_SYNTHESIS",
      companyHistory: "SUPPORTED_SYNTHESIS",
      differentiators: "SUPPORTED_SYNTHESIS",
      cap_profile_operational: "SUPPORTED_SYNTHESIS",
    },
  },
  recIq0XYgt5Ghvcsz: {
    companyDescription:
      "Sonesta International Hotels Corporation is a brand-operator and franchisor of Sonesta, Royal Sonesta, Sonesta Select and related brands, with managed and franchised hotels.",
    companyHistory:
      "Sonesta originated in the mid-20th century and was rebuilt as a modern U.S. brand platform (including Hospitality International / Red Lion integrations in recent years) under Sonesta International.",
    differentiators:
      "Sonesta brand family (including Royal Sonesta and Sonesta Select/ES) under a brand-operator/franchisor model rather than pure third-party independent management.",
    cap_profile_operational:
      "Hotels operate under Sonesta brand standards with corporate brand-operator and franchise support systems rather than an independent third-party-only management stack.",
    brandFamilies: ["Sonesta"],
    serviceModels: ["Full-service", "Select-service", "Extended stay"],
    propertyTypes: ["Full Service", "Select Service", "Extended Stay"],
    chainScales: ["Upper Upscale", "Upscale", "Upper Midscale", "Midscale"],
    softBrand: "Limited",
    sources: [{ title: "Sonesta", url: "https://www.sonesta.com" }],
    fidelity: {
      companyDescription: "SUPPORTED_SYNTHESIS",
      companyHistory: "SUPPORTED_SYNTHESIS",
      differentiators: "SUPPORTED_SYNTHESIS",
      cap_profile_operational: "SUPPORTED_SYNTHESIS",
    },
  },
  rec3TUHT9Z4AnFp5P: {
    companyDescription:
      "Playa Hotels & Resorts is an owner–operator of all-inclusive resorts in Mexico, the Dominican Republic and Jamaica, primarily under Hyatt Inclusive Collection and proprietary brands.",
    companyHistory:
      "Playa was formed as a Caribbean/Mexico all-inclusive resort owner-operator and became a publicly listed company focused on resort ownership and operation with brand partners.",
    differentiators:
      "All-inclusive resort owner–operator with proprietary commercial tooling (direct booking, travel-agent portal, yield and upsell) above brand-partner property systems.",
    brandFamilies: ["Hyatt", "Independent", "Other"],
    serviceModels: ["All-inclusive", "Resort", "Full-service"],
    propertyTypes: ["All-Inclusive", "Resort"],
    chainScales: ["Upper Upscale", "Upscale"],
    softBrand: "Limited",
    additionalExperience: ["Resort"],
    sources: [{ title: "Playa Resorts", url: "https://www.playaresorts.com" }],
    fidelity: {
      companyDescription: "SUPPORTED_SYNTHESIS",
      companyHistory: "SUPPORTED_SYNTHESIS",
      differentiators: "SUPPORTED_SYNTHESIS",
    },
  },
  recKVILWcRLqrQlWs: {
    companyDescription:
      "Driftwood Hospitality Management is a U.S. third-party hotel management company operating branded hotels for owners across multiple chain scales.",
    companyHistory:
      "Driftwood grew as a Florida-rooted hotel management company into a multi-state third-party operator of branded select- and full-service hotels.",
    differentiators:
      "Third-party branded-hotel management platform with documented owner reporting and operations playbooks oriented to U.S. branded assets.",
    brandFamilies: ["Marriott", "Hilton", "IHG", "Hyatt", "Other"],
    serviceModels: ["Third-Party Management", "Select-service", "Full-service"],
    propertyTypes: ["Select Service", "Full Service"],
    chainScales: ["Upscale", "Upper Midscale", "Midscale"],
    softBrand: "Limited",
    sources: [{ title: "Driftwood", url: "https://www.driftwoodhospitality.com" }],
    fidelity: {
      companyDescription: "SUPPORTED_SYNTHESIS",
      companyHistory: "SUPPORTED_SYNTHESIS",
      differentiators: "SUPPORTED_SYNTHESIS",
    },
  },
  recfwDdU5t9h4uFnZ: {
    companyDescription:
      "Atlantica Hotels International (AHI) is a Brazilian hospitality group that owns brands and manages/franchises hotels across Brazil under Atlantica and partner brands.",
    companyHistory:
      "Atlantica developed as a Brazilian hotel operator and franchisor and expanded into one of Brazil’s larger multi-brand hotel platforms.",
    differentiators:
      "Brazil-centered multi-brand hotel operating and franchise platform (Atlantica Hospitality International) rather than a U.S. enterprise management company.",
    cap_profile_operational:
      "Atlantica operates a Brazil-centered multi-brand hotel platform combining proprietary/partner brands with corporate operating support for managed and franchised hotels.",
    brandFamilies: ["Other", "Independent", "Choice", "Wyndham"],
    serviceModels: ["Full-service", "Select-service", "Third-Party Management"],
    propertyTypes: ["Full Service", "Select Service"],
    chainScales: ["Upscale", "Upper Midscale", "Midscale", "Economy"],
    softBrand: "Limited",
    sources: [{ title: "Atlantica / AHI", url: "https://www.ahi.com.br" }],
    fidelity: {
      companyDescription: "SUPPORTED_SYNTHESIS",
      companyHistory: "SUPPORTED_SYNTHESIS",
      differentiators: "SUPPORTED_SYNTHESIS",
      cap_profile_operational: "SUPPORTED_SYNTHESIS",
    },
  },
  recJ6NPSYveCTo3At: {
    companyDescription:
      "Tafer Hotels & Resorts is a Los Cabos–centered integrated owner–operator of resort hotels and related hospitality assets in Mexico.",
    companyHistory:
      "Tafer developed as a Los Cabos resort owner-operator and expanded a concentrated Baja California Sur resort portfolio under the Tafer Hotels & Resorts platform.",
    differentiators:
      "Concentrated Los Cabos integrated owner–operator resort platform rather than a diversified multi-country third-party management company.",
    cap_profile_operational:
      "Tafer operates an integrated owner–operator resort platform concentrated in Los Cabos, with property operations under corporate resort operating control.",
    brandFamilies: ["Independent", "Other", "Hyatt"],
    serviceModels: ["Full-service", "Resort", "All-inclusive"],
    propertyTypes: ["Resort", "All-Inclusive", "Full Service"],
    chainScales: ["Upper Upscale", "Upscale"],
    softBrand: "Limited",
    additionalExperience: ["Resort"],
    sources: [{ title: "Tafer Resorts", url: "https://www.taferresorts.com/" }],
    fidelity: {
      companyDescription: "SUPPORTED_SYNTHESIS",
      companyHistory: "SUPPORTED_SYNTHESIS",
      differentiators: "SUPPORTED_SYNTHESIS",
      cap_profile_operational: "SUPPORTED_SYNTHESIS",
    },
  },
  recOc5kpsg4Muip9Y: {
    companyDescription:
      "Royalton Hotels & Resorts is an all-inclusive resort brand/operator platform associated with Blue Diamond Hotels & Resorts in the Caribbean and Mexico.",
    companyHistory:
      "Royalton developed as an all-inclusive resort brand within the Blue Diamond Hotels & Resorts group, expanding across Caribbean and Mexico resort destinations.",
    differentiators:
      "All-inclusive resort brand-operator platform (Royalton / Blue Diamond) focused on Caribbean and Mexico resort operations.",
    cap_profile_operational:
      "Royalton resorts operate under an all-inclusive brand-operator model with corporate resort operating standards across Caribbean and Mexico destinations.",
    brandFamilies: ["Independent", "Other"],
    serviceModels: ["All-inclusive", "Resort", "Full-service"],
    propertyTypes: ["All-Inclusive", "Resort"],
    chainScales: ["Upper Upscale", "Upscale"],
    softBrand: "None documented",
    additionalExperience: ["Resort"],
    sources: [{ title: "Royalton", url: "https://www.royaltonresorts.com/" }],
    fidelity: {
      companyDescription: "SUPPORTED_SYNTHESIS",
      companyHistory: "SUPPORTED_SYNTHESIS",
      differentiators: "SUPPORTED_SYNTHESIS",
      cap_profile_operational: "SUPPORTED_SYNTHESIS",
    },
  },
  recJtFkhjaO57rSDC: {
    companyDescription:
      "Grupo Presidente is a Mexican hotel group operating Presidente and related hotels, combining ownership and operating roles in Mexico.",
    companyHistory:
      "Grupo Presidente developed as a Mexican hotel company centered on Presidente-branded hotels and related hospitality assets in Mexico.",
    differentiators:
      "Mexico-centered Presidente hotel platform combining ownership and operating roles rather than a global third-party management company.",
    cap_profile_operational:
      "Grupo Presidente operates a Mexico-centered hotel platform under Presidente brand/operating control for its portfolio hotels.",
    brandFamilies: ["Independent", "Other"],
    serviceModels: ["Full-service"],
    propertyTypes: ["Full Service"],
    chainScales: ["Upper Upscale", "Upscale"],
    softBrand: "None documented",
    additionalExperience: ["Urban"],
    sources: [{ title: "Grupo Presidente", url: "https://grupopresidente.com.mx/" }],
    fidelity: {
      companyDescription: "SUPPORTED_SYNTHESIS",
      companyHistory: "SUPPORTED_SYNTHESIS",
      differentiators: "SUPPORTED_SYNTHESIS",
      cap_profile_operational: "SUPPORTED_SYNTHESIS",
    },
  },
  reckyv9O0Y3auYpJJ: {
    companyDescription:
      "Grupo Hotelero Santa Fe is a Mexican hotel owner-operator and developer operating branded and proprietary hotels primarily in Mexico.",
    companyHistory:
      "Grupo Hotelero Santa Fe developed as a Mexican hotel investment and operating group with a portfolio of branded and proprietary hotels.",
    differentiators:
      "Mexico hotel owner-operator/developer platform (GSF) combining ownership with operating roles for branded hotels.",
    cap_profile_operational:
      "GSF operates a Mexico-centered owner-operator hotel platform combining asset ownership with hotel operating control for its portfolio.",
    brandFamilies: ["Marriott", "Hilton", "IHG", "Independent", "Other"],
    serviceModels: ["Full-service", "Select-service"],
    propertyTypes: ["Full Service", "Select Service"],
    chainScales: ["Upper Upscale", "Upscale", "Upper Midscale"],
    softBrand: "Limited",
    sources: [{ title: "GSF Hoteles", url: "https://www.gsf-hoteles.com" }],
    fidelity: {
      companyDescription: "SUPPORTED_SYNTHESIS",
      companyHistory: "SUPPORTED_SYNTHESIS",
      differentiators: "SUPPORTED_SYNTHESIS",
      cap_profile_operational: "SUPPORTED_SYNTHESIS",
    },
  },
  recuEDrp6oeJIEuRX: {
    companyDescription:
      "Grupo Marta Hospitality is a Mexican hospitality group involved in hotel ownership and operations for selected Mexico assets.",
    companyHistory: CTRL.NPD,
    differentiators: CTRL.NO_DIFF,
    cap_profile_operational: CTRL.NO_OPS,
    brandFamilies: ["Other", "Independent"],
    serviceModels: ["Full-service"],
    propertyTypes: ["Full Service"],
    chainScales: [CTRL.RV],
    softBrand: "Unknown",
    researchNotes: [
      "grupomarta.com reviewed — identity and Mexico hospitality role confirmed; no named operating mechanism or differentiated public claim suitable for Writer v2",
    ],
    sources: [{ title: "Grupo Marta", url: "https://www.grupomarta.com/" }],
    fidelity: {
      companyDescription: "SUPPORTED_SYNTHESIS",
      companyHistory: "NOT_PUBLICLY_DISCLOSED",
      differentiators: "NOT_PUBLICLY_DISCLOSED",
      cap_profile_operational: "NOT_PUBLICLY_DISCLOSED",
    },
  },
  reck6gjQd3wdeugmZ: {
    companyDescription:
      "Arriva Hospitality Group (AHG) is a Mexico-based hospitality group operating Arriva hotels with an owner-operator orientation.",
    companyHistory: CTRL.NPD,
    differentiators: CTRL.NO_DIFF,
    cap_profile_operational: CTRL.NO_OPS,
    brandFamilies: ["Independent", "Other"],
    serviceModels: ["Full-service"],
    propertyTypes: ["Full Service"],
    chainScales: [CTRL.RV],
    softBrand: "None documented",
    researchNotes: [
      "arrivahotels.mx reviewed — owner-operator Mexico hotels; no specific named operating platform mechanism documented",
    ],
    sources: [{ title: "Arriva Hotels", url: "https://www.arrivahotels.mx/" }],
    fidelity: {
      companyDescription: "SUPPORTED_SYNTHESIS",
      companyHistory: "NOT_PUBLICLY_DISCLOSED",
      differentiators: "NOT_PUBLICLY_DISCLOSED",
      cap_profile_operational: "NOT_PUBLICLY_DISCLOSED",
    },
  },
  receHCdI6CEsJqdG4: {
    companyDescription:
      "Brittain Resorts & Hotels (BRH) is a third-party resort and hotel management company focused on resort operations for owners.",
    companyHistory:
      "Brittain Resorts & Hotels developed as a resort-focused management company; public materials emphasize resort management services for owners.",
    differentiators:
      "Resort-focused third-party management orientation (Brittain Resorts & Hotels) rather than broad urban select-service management.",
    cap_profile_operational:
      "BRH operates as a resort-focused third-party management platform with property operations oriented to resort assets for owners.",
    brandFamilies: ["Independent", "Other", "Marriott", "Hilton"],
    serviceModels: ["Third-Party Management", "Resort", "Full-service"],
    propertyTypes: ["Resort", "Full Service"],
    chainScales: ["Upper Upscale", "Upscale"],
    softBrand: "Limited",
    additionalExperience: ["Resort"],
    sources: [{ title: "Brittain Resorts", url: "https://brittainresorts.com/" }],
    fidelity: {
      companyDescription: "SUPPORTED_SYNTHESIS",
      companyHistory: "SUPPORTED_SYNTHESIS",
      differentiators: "SUPPORTED_SYNTHESIS",
      cap_profile_operational: "SUPPORTED_SYNTHESIS",
    },
  },
  recQ6Cf8O2z0tiqBz: {
    companyDescription:
      "Cenote Azul Operadores is a Mexico-based hotel operating company providing operating services for selected Mexico hospitality assets.",
    companyHistory: CTRL.NPD,
    differentiators: CTRL.NO_DIFF,
    cap_profile_operational: CTRL.NO_OPS,
    brandFamilies: ["Other", "Independent"],
    serviceModels: ["Third-Party Management"],
    propertyTypes: [CTRL.NO_MULTI],
    chainScales: [CTRL.RV],
    softBrand: "Unknown",
    researchNotes: [
      "cenoteazul.mx / public materials — operator identity confirmed; founding chronology and named ops mechanism not reliably disclosed",
    ],
    sources: [{ title: "Cenote Azul", url: "https://cenoteazul.mx" }],
    fidelity: {
      companyDescription: "SUPPORTED_SYNTHESIS",
      companyHistory: "NOT_PUBLICLY_DISCLOSED",
      differentiators: "NOT_PUBLICLY_DISCLOSED",
      cap_profile_operational: "NOT_PUBLICLY_DISCLOSED",
    },
  },
  rectsHzacZDFTH1Ze: {
    companyDescription:
      "OxoHotel is a Latin American hotel operator offering concept-to-operation hotel development and management services across branded and independent projects.",
    companyHistory:
      "OxoHotel developed as a LatAm hotel operating and development services company emphasizing concept-to-operation delivery for owners and developers.",
    differentiators:
      "Concept-to-operation hotel delivery model spanning development support through hotel operations for LatAm projects.",
    brandFamilies: ["Independent", "Other", "Marriott", "Hilton"],
    serviceModels: ["Third-Party Management", "Full-service", "Select-service"],
    propertyTypes: ["Full Service", "Select Service", "Boutique"],
    chainScales: ["Upscale", "Upper Midscale"],
    softBrand: "Limited",
    sources: [{ title: "OxoHotel", url: "https://www.oxohotel.com/en/" }],
    fidelity: {
      companyDescription: "SUPPORTED_SYNTHESIS",
      companyHistory: "SUPPORTED_SYNTHESIS",
      differentiators: "SUPPORTED_SYNTHESIS",
    },
  },
  rec9JSyGQjvodsPSJ: {
    companyDescription:
      "AADESA is an Argentina-based hotel management company providing third-party hotel operating services in Argentina.",
    companyHistory: CTRL.NPD,
    differentiators: CTRL.NO_DIFF,
    cap_profile_operational: CTRL.NO_OPS,
    brandFamilies: ["Other", "Independent"],
    serviceModels: ["Third-Party Management"],
    propertyTypes: [CTRL.NO_MULTI],
    chainScales: [CTRL.RV],
    softBrand: "Unknown",
    researchNotes: ["aadesa.com.ar reviewed — third-party Argentina operator; thin public operating-mechanism disclosure"],
    sources: [{ title: "AADESA", url: "https://aadesa.com.ar/" }],
    fidelity: {
      companyDescription: "SUPPORTED_SYNTHESIS",
      companyHistory: "NOT_PUBLICLY_DISCLOSED",
      differentiators: "NOT_PUBLICLY_DISCLOSED",
      cap_profile_operational: "NOT_PUBLICLY_DISCLOSED",
    },
  },
  recHj56wpRLUnJ5Wx: {
    companyDescription:
      "Tremun Hoteles is an Argentina-based hotel operator managing hotels in Argentina under the Tremun platform.",
    companyHistory: CTRL.NPD,
    differentiators: CTRL.NO_DIFF,
    cap_profile_operational: CTRL.NO_OPS,
    brandFamilies: ["Independent", "Other"],
    serviceModels: ["Full-service", "Third-Party Management"],
    propertyTypes: ["Full Service"],
    chainScales: [CTRL.RV],
    softBrand: "None documented",
    researchNotes: ["tremunhoteles.com reviewed — Argentina operator identity; limited public ops-mechanism detail"],
    sources: [{ title: "Tremun", url: "https://tremunhoteles.com/" }],
    fidelity: {
      companyDescription: "SUPPORTED_SYNTHESIS",
      companyHistory: "NOT_PUBLICLY_DISCLOSED",
      differentiators: "NOT_PUBLICLY_DISCLOSED",
      cap_profile_operational: "NOT_PUBLICLY_DISCLOSED",
    },
  },
  recjgHXqTJktijFUR: {
    companyDescription:
      "Álvarez Argüelles Hoteles is an Argentina-based hotel operator providing hotel operating services in Argentina.",
    companyHistory: CTRL.NPD,
    differentiators: CTRL.NO_DIFF,
    cap_profile_operational: CTRL.NO_OPS,
    brandFamilies: ["Independent", "Other"],
    serviceModels: ["Third-Party Management", "Full-service"],
    propertyTypes: [CTRL.NO_MULTI],
    chainScales: [CTRL.RV],
    softBrand: "Unknown",
    researchNotes: ["alvarezarguelles.com reviewed — Argentina operator; founding chronology and named ops stack not reliably public"],
    sources: [{ title: "Álvarez Argüelles", url: "https://www.alvarezarguelles.com/" }],
    fidelity: {
      companyDescription: "SUPPORTED_SYNTHESIS",
      companyHistory: "NOT_PUBLICLY_DISCLOSED",
      differentiators: "NOT_PUBLICLY_DISCLOSED",
      cap_profile_operational: "NOT_PUBLICLY_DISCLOSED",
    },
  },
};

/** Fill companyHistory when fidelity says NPD but text missing */
export function resolveHistory(row) {
  if (!row) return CTRL.NPD;
  if (row.fidelity?.companyHistory === "NOT_PUBLICLY_DISCLOSED") return CTRL.NPD;
  return row.companyHistory || CTRL.NPD;
}
