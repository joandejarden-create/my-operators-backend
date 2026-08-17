/**
 * Shared remaining Tab Factory prefill extras for factory-queue operators.
 * Written to fixtures/operator-prefill-extras-{suffix}.json at materialize time.
 */
export function buildGhlPrefillExtras() {
  return {
    companyDescription:
      "GHL Hoteles operates a Latin America hotel platform with approximately 35 hotels across 4 countries and 18 destinations (~3,433 rooms, ~2,000 collaborators), spanning proprietary GHL brand families with full-service urban and leisure hotels, meetings, F&B, and events capability.",
    yearEstablished: "",
    yearsInBusiness: "",
    primaryServiceModel: "Hotel operations (operator-branded Latin America platform)",
    totalProperties: 35,
    totalRooms: 3433,
    website: "https://www.ghlhoteles.com/en/",
    certifications:
      "GHL full-service category standards (meetings, F&B, amenities). Confirm safety/insurance certifications per asset.",
    companySize: "~2,000 collaborators (official site)",
    offeredServices:
      "Full-service hotel operations; meetings and events; F&B/restaurants; brand-family management (GHL, Style, Relax, Collection, Geotel, resort labels); direct booking and leisure packaging",
    brand_soft_independent_narrative:
      "Portfolio is primarily proprietary GHL family brands. Collection and resort labels provide softer/collection-style positioning; third-party franchise flags require asset-level confirmation.",
    brand_relationship_depth_json: [
      { title: "GHL core full-service", description: "100+ room urban hotels with meetings and F&B depth." },
      { title: "Style / Relax / Collection", description: "Lifestyle and leisure brand-family relationships within the platform." },
      { title: "Geotel & resort labels", description: "Regional and resort labels extending the operator brand set." },
    ],
    brand_execution_capabilities_json: [
      { title: "Brand-family standards", description: "Category operating standards across GHL family hotels." },
      { title: "Events execution", description: "Meetings and celebrations with facilities and advisory support." },
      { title: "F&B programming", description: "Restaurant and bar concepts featured as guest demand drivers." },
    ],
    brand_governance_compliance_json: [
      { title: "Category compliance", description: "Full-service amenity and service standards for GHL category hotels." },
      { title: "Multi-brand governance", description: "Operating consistency across Style, Relax, Collection, and Geotel." },
    ],
    teamExperienceMarkets:
      "Colombia (primary); Peru (Lima, Cusco, Puno); Chile (Osorno, Antofagasta, Calama, Villarrica); Guatemala (Quetzaltenango)",
    mkt_regional_expertise_json: [
      { title: "Colombia urban & leisure", description: "Deepest published hotel concentration across major and secondary cities." },
      { title: "Andean destinations", description: "Peru highland and gateway hotels including Lago Titicaca." },
      { title: "Southern Cone / Chile", description: "City and destination hotels in Chile." },
    ],
    mkt_market_fit_signals_json: [
      { title: "Full-service urban fit", description: "Economic-hub cities with meetings and F&B demand." },
      { title: "Leisure destination fit", description: "Relax/Style positioning for leisure travelers." },
    ],
    ownerReportingCadence: "Monthly operating reviews; quarterly strategy sessions (confirm in agreement)",
    ownerResponseTime: "Business-day responsive owner cadence (confirm in agreement)",
    reportTypes: "Owner P&L, forecast, commercial pacing, F&B/events contribution, brand QA status",
    ownerPortalFeatures: "Agreement-defined owner document exchange (not published as a public portal)",
    operatingCollaborationMode:
      "In-region Latin America brand-platform operations with agreement-defined owner governance",
    ownerReportingLevel: "Agreement-defined structured owner reporting",
    ov_owner_tools_json: [
      { title: "Owner review packs", description: "Monthly operating and financial materials (agreement-defined)." },
      { title: "Escalation path", description: "Regional operating leadership for brand and commercial issues." },
    ],
    ov_lifecycle_support_json: [
      { title: "Opening & ramp", description: "Full-service opening playbooks and events/F&B activation." },
      { title: "Ongoing operations", description: "Multi-brand hotel operations across four countries." },
      { title: "Repositioning", description: "Brand-family repositioning within GHL labels." },
    ],
    infra_technology_maturity_level: "Structured",
    infra_technology_stack_json: [
      { title: "PMS / Property Operations", description: "Property systems for reservations and daily operations—confirm vendor by hotel." },
      { title: "Direct Booking / Web", description: "ghlhoteles.com booking engine and plan packaging." },
      { title: "Events Systems", description: "Meetings and celebrations inventory managed with hotel facilities teams." },
      { title: "Owner Reporting", description: "Structured packs where third-party management agreements apply." },
    ],
    infra_services_offered_json: [
      { title: "Full-service hotel operations", description: "Rooms, F&B, wet areas, gym, and meetings for GHL category hotels." },
      { title: "Events & celebrations", description: "Group events with facilities and professional advisory." },
      { title: "Brand-family management", description: "Operating standards across Style, Relax, Collection, Geotel, and resort labels." },
    ],
    infra_data_domains_json: [
      { title: "Operating KPIs", description: "Hotel performance and commercial pacing domains." },
      { title: "Events & F&B", description: "Group and restaurant contribution tracking." },
    ],
    infra_data_governance_json: [
      { title: "Owner data access", description: "Financial and operating data access defined in management agreements." },
    ],
    infra_analytics_support_json: [
      { title: "Commercial pacing", description: "Leisure plan and events demand tracking for portfolio hotels." },
    ],
    bf_preferred_deal_profile_json: [
      { title: "Urban full-service", description: "100+ room hotels with meetings/F&B in Latin America." },
      { title: "Lifestyle / leisure brand family", description: "Style or Relax positioning within GHL family." },
    ],
    bf_evaluation_path_json: [
      { title: "Destination & brand fit", description: "Confirm country/destination fit and GHL brand-family match." },
      { title: "Agreement diligence", description: "Owner reporting, PIP funding, and third-party scope if applicable." },
    ],
    bf_red_flags_json: [
      { title: "Unfunded brand obligations", description: "PIP or amenity funding gaps for full-service standards." },
      { title: "Outside Latin America", description: "Assets outside GHL four-country operating depth." },
    ],
    case_studies_json: [
      {
        title: "Latin America multi-brand platform scale",
        summary:
          "Official materials cite ~35 hotels / 4 countries / 18 destinations / 3,433 rooms—evidence of multi-country operating scale for owners evaluating GHL brand families.",
        hotel_name: "GHL portfolio (multi-asset)",
      },
      {
        title: "Full-service urban hotels with events",
        summary:
          "GHL category hotels emphasize 100+ rooms with meetings, restaurants, wet areas, and gym—supporting group and leisure demand in Latin America cities.",
        hotel_name: "GHL full-service category",
      },
    ],
    op_preopening_transition_json: {
      intro: "Full-service openings and brand-family transitions across Latin America destinations.",
      items: [
        { title: "Full-service opening playbooks", description: "GHL category hotels with meetings, F&B, wet areas, and gym readiness." },
        { title: "Events & F&B activation", description: "Celebrations and restaurant programming as part of opening ramp." },
        { title: "Brand-family standards cutover", description: "Operating consistency across Style, Relax, Collection, and Geotel labels." },
      ],
    },
    op_conversion_repositioning_json: {
      intro: "Selective conversion/repositioning within GHL brand families.",
      items: [
        { title: "Brand-family repositioning", description: "Move assets across Style, Relax, Collection, or core GHL positioning." },
        { title: "Urban full-service upgrades", description: "Amenity and meetings upgrades for economic-hub hotels." },
      ],
    },
    op_fb_lifestyle_resort_json: {
      intro: "F&B, lifestyle, and resort labels are core to GHL guest proposition.",
      items: [
        { title: "Signature restaurants & bars", description: "Named F&B venues featured across portfolio hotels." },
        { title: "GHL Relax / Lifestyle", description: "Leisure and lifestyle brand-family positioning." },
        { title: "Irotama Resort label", description: "Resort operating label within the public brand list." },
      ],
    },
  };
}

export function buildAimbridgePrefillExtras() {
  return {
    companyDescription:
      "Aimbridge LATAM is Aimbridge Hospitality’s Latin America third-party hotel management division, partnering with owners across Mexico and Latin America on major international brands (IHG, Wyndham, Marriott, Hilton) and independent assets, with a dedicated All-Inclusive division and in-market development coverage for Mexico/Central America and the Caribbean.",
    primaryServiceModel: "Third-party hotel management (LATAM + All-Inclusive)",
    website: "https://aimbridgelatam.com/en/home/",
    certifications:
      "Brand-standard QA and franchise compliance by asset (Marriott, Hilton, IHG, Wyndham). Aimbridge enterprise safety/continuity frameworks scaled to LATAM.",
    companySize: "Not published for LATAM division alone — backed by Aimbridge Hospitality enterprise (labeled parent context)",
    offeredServices:
      "Third-party hotel management; all-inclusive resort operations; pre-opening and transitions; brand conversions/reflags; owner reporting; F&B leadership for LATAM & All-Inclusive; development support Mexico/Central America and Caribbean",
    brand_soft_independent_narrative:
      "Aimbridge LATAM manages both major-brand hotels and independent properties. Soft/collection formats appear within franchise ecosystems; independents are explicitly in public positioning.",
    brand_relationship_depth_json: [
      { title: "Marriott International", description: "Active LATAM relationships including JW Marriott Monterrey Valle recognition and AI project signals." },
      { title: "Hilton Worldwide", description: "Cited alliance partner across Mexico/LATAM." },
      { title: "IHG Hotels & Resorts", description: "Cited alliance partner across Mexico/LATAM." },
      { title: "Wyndham Hotels & Resorts", description: "Cited alliance partner across Mexico/LATAM." },
    ],
    brand_execution_capabilities_json: [
      { title: "Franchise operating discipline", description: "Brand-system operations across major families." },
      { title: "All-Inclusive execution", description: "Dedicated AI division for resort leisure assets." },
      { title: "Conversion / reflag", description: "Lifestyle and brand conversions within franchise ecosystems." },
    ],
    brand_governance_compliance_json: [
      { title: "Brand QA", description: "Franchise compliance and QA coordinated with LATAM operating leaders." },
      { title: "Enterprise standards", description: "Aimbridge Hospitality platform standards behind regional teams." },
    ],
    teamExperienceMarkets:
      "Mexico (core); Caribbean / Dominican Republic (growth); Central America (development remit); Riviera Maya all-inclusive corridor",
    mkt_regional_expertise_json: [
      { title: "Mexico", description: "Properties throughout the Mexican Republic with major brand alliances." },
      { title: "Caribbean & All-Inclusive", description: "Dedicated AI division and Caribbean BD leadership." },
      { title: "Central America development", description: "Business hotel and resort development remit." },
    ],
    mkt_market_fit_signals_json: [
      { title: "Branded full-service / select", description: "Major brand hotels needing third-party management." },
      { title: "All-inclusive resorts", description: "Experience-driven leisure assets for AI division." },
    ],
    ownerReportingCadence: "Monthly operating reviews; quarterly strategy sessions",
    ownerResponseTime: "Regional leadership escalation with Aimbridge enterprise support",
    reportTypes: "Owner asset performance, forecast, brand QA, commercial pacing, pipeline updates",
    ownerPortalFeatures: "Institutional owner packs via Aimbridge reporting practices (confirm portal features in agreement)",
    operatingCollaborationMode:
      "In-market LATAM leadership with Aimbridge Hospitality enterprise platform backing",
    ownerReportingLevel: "Structured owner reporting",
    ov_owner_tools_json: [
      { title: "Monthly owner packs", description: "Operating and financial summary with variance narrative." },
      { title: "Brand QA visibility", description: "Franchise obligation tracking by flag." },
    ],
    ov_lifecycle_support_json: [
      { title: "Pre-opening", description: "Openings and takeover support for branded and AI assets." },
      { title: "Stabilization", description: "Commercial and F&B ramp with regional leadership." },
      { title: "Growth / conversion", description: "Reflag and pipeline development support." },
    ],
    infra_technology_maturity_level: "Structured",
    infra_technology_stack_json: [
      { title: "PMS / Property Operations", description: "Brand-mandated property systems by franchise/soft brand." },
      { title: "RMS / Revenue", description: "Brand RMS plus Aimbridge commercial support." },
      { title: "CRS / Distribution", description: "Brand central reservations and channel connectivity." },
      { title: "Owner Reporting", description: "Institutional owner packs via Aimbridge reporting practices." },
      { title: "F&B Systems", description: "LATAM & All-Inclusive F&B leadership supporting culinary programs." },
    ],
    infra_services_offered_json: [
      { title: "Third-party hotel management", description: "Full operating management for branded and independent hotels." },
      { title: "All-Inclusive resort operations", description: "Specialized division for AI resort assets." },
      { title: "Pre-opening & transitions", description: "Openings, reflags, and takeover support." },
      { title: "Owner reporting", description: "Structured financial and operating reporting." },
    ],
    infra_data_domains_json: [
      { title: "Operating & financial KPIs", description: "Asset performance domains for owner packs." },
      { title: "Brand compliance", description: "QA and franchise obligation data." },
    ],
    infra_data_governance_json: [
      { title: "Owner data discipline", description: "Agreement-defined access to financial and operating data." },
    ],
    infra_analytics_support_json: [
      { title: "Commercial pacing analytics", description: "Owner-facing pacing and forecast support." },
    ],
    bf_preferred_deal_profile_json: [
      { title: "Branded hotels in Mexico/LATAM", description: "IHG, Wyndham, Marriott, Hilton assets seeking third-party management." },
      { title: "All-inclusive resorts", description: "Leisure AI assets for dedicated division." },
    ],
    bf_evaluation_path_json: [
      { title: "Market & brand fit", description: "Confirm LATAM corridor fit and brand family." },
      { title: "Leadership & agreement", description: "Underwrite Fiz-era LATAM leadership and management agreement terms." },
    ],
    bf_red_flags_json: [
      { title: "No regional presence needed", description: "Owners wanting remote U.S.-only management without LATAM depth." },
      { title: "Unfunded PIP", description: "Brand obligations without capital plan." },
    ],
    case_studies_json: [
      {
        title: "Noval Properties — Dominican Republic alliance",
        summary:
          "Public announcement of strategic alliance with Noval Properties to expand Aimbridge presence in the Dominican Republic—owner-relevant Caribbean growth signal.",
        hotel_name: "Noval Properties alliance (DR)",
      },
      {
        title: "Grupo Satli / Marriott — Riviera Maya all-inclusive",
        summary:
          "2026 public announcement of a landmark all-inclusive resort project in Riviera Maya with Marriott International and Aimbridge All-Inclusive Division operating intent.",
        hotel_name: "Riviera Maya all-inclusive (announced)",
      },
    ],
    op_preopening_transition_json: {
      intro: "Openings, takeovers, and all-inclusive resort transitions across Mexico, Caribbean, and Central America.",
      items: [
        { title: "All-inclusive openings", description: "Specialized AI division for large resort projects." },
        { title: "Brand conversions", description: "Reflag/lifestyle conversions within major franchise ecosystems." },
        { title: "Cross-border growth", description: "DR alliances and Caribbean BD expanding beyond Mexico core." },
      ],
    },
    op_conversion_repositioning_json: {
      intro: "Active conversion and lifestyle repositioning within major brand families.",
      items: [
        { title: "Major brand reflags", description: "Move assets into IHG, Wyndham, Marriott, or Hilton systems." },
        { title: "Lifestyle repositioning", description: "Public pipeline includes lifestyle/premium brand conversions." },
      ],
    },
    op_fb_lifestyle_resort_json: {
      intro: "F&B and all-inclusive resort capability are explicit LATAM leadership priorities.",
      items: [
        { title: "LATAM & AI F&B leadership", description: "Davide Preziuso — Director F&B for LATAM & All-Inclusive." },
        { title: "All-Inclusive division", description: "Dedicated division for experience-driven resort leisure assets." },
        { title: "Culinary recognition", description: "Public LATAM channels cite JW Marriott Monterrey Valle culinary recognition." },
      ],
    },
  };
}

