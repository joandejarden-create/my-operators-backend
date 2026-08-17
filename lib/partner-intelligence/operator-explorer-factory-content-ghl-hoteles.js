/**
 * Factory-queue Operator Explorer content packs (GHL + Aimbridge LATAM).
 * Source-backed from official operator sites + labeled parent context.
 * Materialized into fixtures/operator-*-{suffix}.json — not Airtable writes.
 */
export const FACTORY_CONTENT_VERSION = "operator-factory-content-v1";

/** Shared intentional suppress for owner/lender refs on public Explorer. */
export const FACTORY_INTENTIONAL_SUPPRESS = Object.freeze({
  "op.proof.ownerReferences": "Owner references not published on Explorer (confidential)",
  "op.proof.lenderReferences": "Lender references not published on Explorer (confidential)",
});

/**
 * GHL Hoteles — Latin America hotel operator / brand platform (GHL Holding).
 * Sources: ghlhoteles.com EN home, destinations, brands, hotels, events (PI extract 2026-07-06).
 */
export const GHL_HOTELES_FACTORY_CONTENT = Object.freeze({
  slug: "ghl-hoteles",
  recordId: "reciI2tYQBfMoMK9G",
  companyName: "GHL Hoteles (GHL Holding)",
  domain: "ghlhoteles.com",
  suffix: "ghl-hoteles",
  intentionalSuppress: {
    ...FACTORY_INTENTIONAL_SUPPRESS,
    "op.snapshot.parentCompany":
      "GHL Holding / Latam Hotel Corporation context labeled in narrative; parentCompany scalar not forced on Explorer",
    "op.snapshot.yearEstablished":
      "Founding year not published on official EN site — confirm in diligence; not inventing a year",
    "op.proof.yearsInBusiness":
      "Years in business not published as a single figure on official EN site — confirm in diligence",
  },
  fixtures: {
    "operator-profile-explorer": {
      _meta: {
        operatorMasterId: "reciI2tYQBfMoMK9G",
        operatorName: "GHL Hoteles (GHL Holding)",
        sourceUrl: "https://www.ghlhoteles.com/en/",
        note: "CALA/Latin America operator profile from official GHL Hotels site. Scale figures from public home page.",
      },
      profileFields: {
        companyHistory:
          "GHL Hoteles (GHL Holding) is a Latin America hotel operator and brand platform with public presence across Colombia, Peru, Chile, and Guatemala. Official materials describe approximately 35 hotels, 18 destinations, 3,433 rooms, and 2,000 collaborators. The company operates proprietary brands (GHL, GHL Style, GHL Relax, GHL Collection, Geotel) alongside resort and Latam Hotel Corporation labels, with full-service urban and leisure hotels featuring meetings, F&B, and events capability.",
        missionStatement:
          "Deliver memorable Latin America hotel stays through owned and managed brand platforms—combining destination depth, full-service guest amenities, and events capability for leisure and business travelers.",
        differentiators:
          "In-region Latin America footprint (not a U.S.-remote model); multi-brand GHL family plus Geotel/Irotama/Latam Hotel Corporation labels; urban full-service hotels with meetings and events; Colombia-weighted portfolio with selective Andean and Central America presence.",
        managementPhilosophy:
          "Operator-branded hospitality with destination-led experiences, on-property service depth, and events/MICE support for celebrations and corporate gatherings.",
        industryRecognition:
          "Public positioning emphasizes Latin America destination coverage, multi-brand portfolio scale, and full-service guest amenities; third-party awards are not treated as verified Explorer claims without named sources.",
        notableAchievements:
          "Published scale of ~35 hotels / 4 countries / 18 destinations / 3,433 rooms / 2,000 collaborators; brand families spanning GHL core, Style, Relax, Collection, Geotel, Irotama Resort, and Latam Hotel Corporation labels.",
        overview_signal_1_value: "35 hotels · 4 countries · 18 destinations (ghlhoteles.com/en)",
        overview_signal_2_value: "3,433 rooms · ~2,000 collaborators (official site)",
        overview_signal_3_value: "Brand families: GHL, GHL Style, GHL Relax, GHL Collection, Geotel (+ resort / Latam Hotel Corporation labels)",
      },
    },
    "operator-operating-explorer": {
      _meta: {
        operatorMasterId: "reciI2tYQBfMoMK9G",
        operatorName: "GHL Hoteles (GHL Holding)",
        sourceUrl: "https://www.ghlhoteles.com/en/brands/ghl/",
        note: "Operating platform narrative from official GHL brand/service descriptions.",
      },
      platformFields: {
        cap_kpi_operating_model: "Operator-Branded Latin America Platform",
        cap_kpi_execution_strength: "Proven",
        cap_kpi_transition: "Structured",
        cap_kpi_reporting: "Structured",
        cap_signal_budget: "Not Measured / N/A",
        cap_signal_lift: "Not Measured / N/A",
        cap_signal_trans: "Not Measured / N/A",
        cap_profile_operational:
          "In-region Latin America hotel operations across Colombia, Peru, Chile, and Guatemala\nFull-service hotels (100+ rooms in GHL category) with restaurants, wet areas, gym, and meeting rooms\nMulti-brand operating standards across GHL family, Geotel, and resort labels\nEvents and celebrations staffing with facilities and advisory support\nDestination-led guest experience programs across 18 published destinations",
        cap_profile_commercial:
          "Direct booking platform with best-price messaging on official site\nLeisure plans (weekend, romantic, spa, wedding night) and advance-purchase offers\nMeetings and events capability for corporate and social groups\nRestaurant and F&B as guest and local demand drivers\nBrand-segmented inventory across Style, Relax, Collection, and core GHL",
        cap_profile_transition:
          "Portfolio growth via brand-family hotels and destination expansion in Latin America\nFull-service opening and operating playbooks for urban economic hubs\nEvents ramp and F&B activation as part of hotel guest services\nConfirm takeover/PIP scope in any third-party management agreement (public site is primarily operator-branded inventory)",
        cap_card_asset_positioning:
          "GHL is a Latin America operator-branded platform concentrated in urban and leisure full-service hotels—Colombia-weighted with selective Peru, Chile, and Guatemala presence—not a global third-party scale narrative.",
        cap_card_service_diff:
          "Differentiation comes from proprietary brand families, in-region destination depth, and meetings/events + F&B intensity rather than a remote U.S. management-company model.",
        cap_card_execution_rel:
          "Published portfolio scale and multi-country presence support operating credibility; owners should diligence agreement terms separately because the public site emphasizes branded hotels more than third-party management contracts.",
        cap_card_governance:
          "Public materials do not publish owner-committee structures; treat owner reporting cadence as agreement-defined for any third-party mandate.",
        cap_deep_revenue_systems:
          "Official direct booking and plan packaging\nLeisure and events demand segmentation\nBrand-family rate architecture across GHL Style / Relax / Collection / core\nConfirm RMS/CRS stack by asset in diligence",
        cap_deep_execution_infra:
          "In-region hotel operating teams across four countries\nEvents facilities and professional advisory for celebrations\nFull-service amenities (meetings, F&B, wet areas, gym) in GHL category hotels\nMulti-brand standards across Geotel and GHL family labels",
      },
      operatingPlatform: {
        snapshotKpis: [
          { rowKey: "revenue_management_capability", title: "Commercial Engine", value: "Operator-Branded Latin America Platform" },
          { rowKey: "owner_reporting_level", title: "Owner Reporting", value: "Agreement-Defined" },
          { rowKey: "pre_opening_support", title: "Pre-Opening Support", value: "Structured" },
          { rowKey: "conversion_reflag", title: "Conversion Capability", value: "Selective" },
          { rowKey: "fb_capability", title: "F&B & Events", value: "Proven" },
        ],
        positioningCards: [
          {
            rowKey: "cap_card_asset_positioning",
            title: "Asset Positioning",
            description:
              "GHL is a Latin America operator-branded platform concentrated in urban and leisure full-service hotels—Colombia-weighted with selective Peru, Chile, and Guatemala presence.",
          },
          {
            rowKey: "cap_card_service_diff",
            title: "Service Differentiation",
            description:
              "Proprietary brand families, destination depth, and meetings/events + F&B intensity—distinct from a remote U.S.-only third-party model.",
          },
          {
            rowKey: "cap_card_execution_rel",
            title: "Execution Reliability",
            description:
              "Published multi-country portfolio scale supports operating credibility; confirm third-party management terms separately from branded inventory narratives.",
          },
        ],
        pillars: {
          commercialEngine: {
            title: "Commercial Engine",
            description:
              "Direct booking, leisure packaging, and events demand across GHL brand families in Latin America destinations.",
            items: [
              { title: "Direct booking platform", description: "Official site booking with plan packages and advance-purchase offers." },
              { title: "Leisure segmentation", description: "Weekend, romantic, spa, wedding, and destination-led packages." },
              { title: "Events & MICE", description: "Meetings and celebrations with facilities and professional advisory." },
              { title: "Brand-family rate architecture", description: "Inventory segmented across Style, Relax, Collection, and core GHL." },
              { title: "F&B as demand driver", description: "Signature restaurants and bars featured as guest and local experiences." },
              { title: "Multi-country commercial coverage", description: "Colombia-led with Peru, Chile, and Guatemala destinations." },
            ],
          },
          ownerReporting: {
            title: "Owner Reporting",
            description:
              "Public site is guest/brand oriented; owner reporting cadence for third-party mandates should be confirmed in executed agreements.",
            items: [
              { title: "Agreement-defined packs", description: "Owner P&L and operating reviews per management agreement—not published as a fixed public calendar." },
              { title: "Asset operating visibility", description: "Hotel-level performance narrative for owned/operated branded assets." },
              { title: "Events & F&B tracking", description: "Group and F&B contribution relevant to full-service hotels." },
            ],
          },
          preOpeningTransition: {
            title: "Transitions & Openings",
            description:
              "Full-service urban hotel operating playbooks and events/F&B activation across Latin America destinations.",
            items: [
              { title: "Full-service openings", description: "GHL category hotels emphasize 100+ rooms, meetings, F&B, wet areas, and gym." },
              { title: "Brand-family standards", description: "Operating consistency across GHL Style, Relax, Collection, Geotel, and resort labels." },
              { title: "Destination expansion", description: "Growth framed through additional Latin America destinations and brand inventory." },
            ],
          },
          conversionRepositioning: {
            title: "Conversion & Repositioning",
            description: "Selective brand-family repositioning and urban full-service upgrades.",
            items: [
              { title: "Brand-family repositioning", description: "Move assets across Style, Relax, Collection, or core GHL positioning." },
              { title: "Urban full-service upgrades", description: "Amenity and meetings upgrades for economic-hub hotels." },
            ],
          },
          fbLifestyleResort: {
            title: "F&B, Lifestyle & Resort",
            description: "F&B, lifestyle, and resort labels are core to the GHL guest proposition.",
            items: [
              { title: "Signature restaurants & bars", description: "Named F&B venues featured across portfolio hotels." },
              { title: "GHL Relax / Lifestyle", description: "Leisure and lifestyle brand-family positioning." },
              { title: "Irotama Resort label", description: "Resort operating label within the public brand list." },
            ],
          },
        },
      },
    },
    "operator-brand-explorer": {
      _meta: {
        operatorMasterId: "reciI2tYQBfMoMK9G",
        operatorName: "GHL Hoteles (GHL Holding)",
        sourceUrl: "https://www.ghlhoteles.com/en/brands/ghl/",
      },
      profileFields: {
        numberOfBrands: 7,
        brandedVsIndependentMix:
          "Primarily proprietary GHL family brands (GHL, Style, Relax, Collection, Geotel) plus Irotama Resort and Latam Hotel Corporation labels; some hotels appear under Sonesta-named properties in public F&B listings—confirm flag relationship per asset.",
        brand_conversion_project_count: "Not published",
        brand_narrative_compliance:
          "Brand standards are enforced through GHL family operating categories (e.g. full-service GHL hotels with meetings, F&B, wet areas, gym). Franchise compliance for third-party flags must be diligence-confirmed.",
        brand_narrative_relationship:
          "GHL operates a multi-brand Latin America platform spanning core GHL, lifestyle Style, leisure Relax, Collection, Geotel, Irotama Resort, and Latam Hotel Corporation. Public materials center proprietary brands more than exclusive big-box franchise exclusivity.",
        brand_signal_audit: "Not Measured / N/A",
        brand_signal_reflag: "Selective",
        brand_signal_franchise_align: "Selective",
        brand_signal_soft_retention: "Moderate",
      },
      brandRelationships: {
        snapshotMetrics: [
          { rowKey: "brand_relationships_count", title: "Brand Families", value: "7" },
          { rowKey: "branded_portfolio", title: "Branded Portfolio", value: "GHL family–led" },
          { rowKey: "independent_soft", title: "Collection / Resort Labels", value: "Active" },
          { rowKey: "conversion_reflag", title: "Conversion / Reflag", value: "Not published" },
          { rowKey: "approved_families", title: "Published Brand Labels", value: "7" },
          { rowKey: "primary_segments", title: "Primary Segments", value: "Full-service · Lifestyle · Leisure · Resort" },
        ],
        portfolioMix: [
          { brandFlagType: "GHL", portfolioMix: "Core full-service", assetContext: "100+ room urban hotels with meetings/F&B", relationshipStatus: "Active" },
          { brandFlagType: "GHL Style", portfolioMix: "Lifestyle", assetContext: "Style-positioned hotels in Latin America cities", relationshipStatus: "Active" },
          { brandFlagType: "GHL Relax", portfolioMix: "Leisure", assetContext: "Leisure/relax positioning (e.g. Sunrise)", relationshipStatus: "Active" },
          { brandFlagType: "GHL Collection", portfolioMix: "Collection", assetContext: "Collection-labeled hotels in portfolio", relationshipStatus: "Active" },
          { brandFlagType: "Geotel", portfolioMix: "Regional brand", assetContext: "Geotel family within GHL platform", relationshipStatus: "Active" },
          { brandFlagType: "Irotama Resort", portfolioMix: "Resort", assetContext: "Resort label in public brand list", relationshipStatus: "Active" },
          { brandFlagType: "Latam Hotel Corporation", portfolioMix: "Platform label", assetContext: "Corporate/platform brand label on public site", relationshipStatus: "Active" },
        ],
      },
      brandFields: {
        brandFamiliesOperated: [
          "GHL",
          "GHL Style",
          "GHL Relax",
          "GHL Collection",
          "Geotel",
          "Irotama Resort",
          "Latam Hotel Corporation",
        ],
      },
    },
    "operator-markets-explorer": {
      _meta: {
        operatorMasterId: "reciI2tYQBfMoMK9G",
        operatorName: "GHL Hoteles (GHL Holding)",
        sourceUrl: "https://www.ghlhoteles.com/en/",
      },
      marketsFields: {
        activeCountries: ["Colombia", "Peru", "Chile", "Guatemala"],
        activeMarkets:
          "Colombia (primary — Bogotá, Medellín, Cartagena, Barranquilla, Cali, and secondary cities); Peru (Lima/Miraflores, Cusco, Puno/Lago Titicaca, Yucay); Chile (Osorno, Antofagasta, Calama, Villarrica); Guatemala (Quetzaltenango)",
        priorityMarkets: "Colombia urban and leisure gateways; selective Andean and Central America destinations already in portfolio",
        targetGrowthMarkets: "Additional Latin America destinations where GHL brand families and full-service operating model fit owner/developer needs",
        regions: "Latin America",
        markets_regional_portfolio_json: [
          {
            title: "Colombia (primary weight)",
            description:
              "Largest published hotel count on official destinations pages; urban full-service and leisure hotels across major and secondary cities.",
          },
          {
            title: "Andean & Southern Cone",
            description:
              "Peru and Chile presence spanning gateway, highland, and southern destinations (including Lago Titicaca and Osorno listings).",
          },
          {
            title: "Central America",
            description: "Guatemala representation (e.g. Latam Plaza Pradera Quetzaltenango) within the four-country footprint.",
          },
        ],
      },
      footprintGeoFields: {
        overview_signal_1_value: "4 countries · 18 destinations (official)",
        overview_signal_2_value: "~35 hotels / 3,433 rooms published scale",
      },
    },
    "operator-engagement-explorer": {
      _meta: {
        operatorMasterId: "reciI2tYQBfMoMK9G",
        operatorName: "GHL Hoteles (GHL Holding)",
        note: "Owner engagement is agreement-defined; public site is guest/brand oriented.",
      },
      commercialFields: {
        ov_card_discipline:
          "GHL presents as an in-region Latin America operator with published multi-country scale. Owner-facing reporting discipline for third-party mandates should be confirmed in the management agreement.",
        ov_card_commercial:
          "Commercial strength is visible in direct booking, leisure packaging, and events/MICE capability across brand families.",
        ov_card_communication:
          "Public materials do not publish a fixed owner reporting calendar—characterize owner communication as structured but agreement-defined.",
        ov_card_flexibility:
          "Multi-brand portfolio (Style, Relax, Collection, Geotel, resort labels) suggests flexibility across urban full-service and leisure assets.",
        ov_card_risk:
          "Primary diligence risk: public narrative is operator-branded inventory; third-party management references, PIP funding, and owner pack formats need confirmation.",
        ov_cluster_interaction:
          "Expected owner interaction for managed assets includes monthly operating reviews and quarterly strategy sessions—confirm cadence in executed agreements.",
        ov_cluster_deliverables:
          "Typical deliverables: operating plans, forecasts, commercial pacing, F&B/events contribution, and brand QA status tailored to ownership requirements.",
        ownerEngagementNarrative:
          "GHL is positioned as a Latin America hotel platform with deep destination and brand-family operating presence. Owners evaluating third-party mandates should underwrite agreement-level reporting and governance separately from the public guest website.",
      },
      engagementReporting: {
        strategicOwnerValue: [
          {
            title: "In-region Latin America platform",
            description: "Four-country footprint with Colombia weight—local operating depth rather than remote U.S.-only management.",
          },
          {
            title: "Multi-brand guest proposition",
            description: "GHL family brands plus Geotel/resort labels allow matching asset positioning to Style, Relax, Collection, or core full-service.",
          },
          {
            title: "Events & F&B intensity",
            description: "Meetings, celebrations, and restaurant programming support full-service hotel economics.",
          },
          {
            title: "Published portfolio scale",
            description: "Official ~35 hotels / 3,433 rooms / 2,000 collaborators provides tangible operating scale context.",
          },
        ],
        engagementCadence: [
          { cadence: "Monthly", engagementType: "Owner operating review", focus: "P&L, forecast, commercial pacing, guest feedback—confirm in agreement." },
          { cadence: "Quarterly", engagementType: "Strategic business review", focus: "Market, brand strategy, capex priorities across destinations." },
          { cadence: "Annually", engagementType: "Budget & business plan", focus: "Operating budget, staffing, commercial plan with owner approval workflow." },
          { cadence: "Ad hoc", engagementType: "Events & brand escalations", focus: "Large groups, reputation, or brand-sensitive incidents." },
        ],
        controlsGovernance: [
          { title: "Budget & forecast", description: "Annual budget with owner review and monthly variance narrative (agreement-defined)." },
          { title: "Brand standards", description: "GHL category standards for full-service amenities and service delivery." },
          { title: "Events governance", description: "Group booking and celebration execution with facilities and advisory support." },
        ],
        reportsReceived: [
          { title: "Monthly owner pack", description: "Operating and financial summary for managed assets (confirm format)." },
          { title: "Commercial pacing", description: "Leisure and events demand tracking by hotel." },
        ],
      },
    },
    "operator-infrastructure-explorer": {
      _meta: {
        operatorMasterId: "reciI2tYQBfMoMK9G",
        operatorName: "GHL Hoteles (GHL Holding)",
      },
      governanceFields: {
        infra_technology_maturity_level: "Structured",
        infra_asset_management_reporting:
          "Monthly owner operating packs (agreement-defined)\nCommercial and events pacing for full-service hotels\nBrand-family standards tracking across GHL labels",
        infra_systems_technology:
          "PMS/RMS: Confirm per asset (not published portfolio-wide)\nDistribution: Official direct booking site + channel mix by hotel\nAccounting: Property-level finance with regional operator support\nOwner reporting: Agreement-defined packs",
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
        infra_data_governance_json: [
          { title: "Owner data access", description: "Financial and operating data access defined in management agreements." },
        ],
        infra_analytics_support_json: [
          { title: "Commercial pacing", description: "Leisure plan and events demand tracking for portfolio hotels." },
        ],
      },
    },
    "operator-leadership-explorer": {
      _meta: {
        operatorMasterId: "reciI2tYQBfMoMK9G",
        operatorName: "GHL Hoteles (GHL Holding)",
        note: "Named public C-suite roster not fully published on EN marketing site—org cards describe platform functions; executive bios limited to verified public roles.",
      },
      lead_avg_hospitality_experience: "Not published",
      lead_org_structure_json: [
        {
          title: "Latin America Hotel Platform",
          description: "In-region operating platform spanning Colombia, Peru, Chile, and Guatemala with multi-brand inventory.",
          tags: ["GHL Holding", "4 Countries", "Multi-Brand"],
        },
        {
          title: "Brand Families",
          description: "Operating oversight across GHL, Style, Relax, Collection, Geotel, Irotama Resort, and Latam Hotel Corporation labels.",
          tags: ["GHL", "Geotel", "Collection"],
        },
        {
          title: "Full-Service & Events",
          description: "Hotels with meetings, F&B, wet areas, and gym—plus celebrations advisory for groups.",
          tags: ["Meetings", "F&B", "Events"],
        },
        {
          title: "Destination Commercial",
          description: "Direct booking, leisure plans, and destination marketing across 18 published destinations.",
          tags: ["Direct Booking", "Leisure Plans"],
        },
      ],
      lead_team_depth_json: [
        {
          function: "Operations",
          leadRole: "Regional hotel operations (public roster TBD)",
          depth: "Strong",
          relevance: "Multi-country full-service hotel operations across proprietary brand families.",
        },
        {
          function: "Commercial / Revenue",
          leadRole: "Direct booking & leisure packaging",
          depth: "Strong",
          relevance: "Official site commercial engine and plan packaging across destinations.",
        },
        {
          function: "Events & F&B",
          leadRole: "Events advisory + restaurant programming",
          depth: "Strong",
          relevance: "Meetings, celebrations, and signature F&B as operating differentiators.",
        },
        {
          function: "Brand Standards",
          leadRole: "GHL family brand governance",
          depth: "Strong",
          relevance: "Category standards for full-service GHL hotels and sister brands.",
        },
      ],
      lead_language_capability_json: [
        { language: "Spanish", capability: "Primary operating language across Latin America portfolio" },
        { language: "English", capability: "Official EN website and international guest communications" },
      ],
      lead_governance_cadence_json: [
        { cadence: "Monthly", focus: "Hotel operating and commercial reviews (internal / owner as agreed)" },
        { cadence: "Quarterly", focus: "Brand-family and destination strategy" },
      ],
      lead_team_markets_json: [
        { market: "Colombia", relevance: "Primary hotel concentration" },
        { market: "Peru", relevance: "Gateway and highland destinations" },
        { market: "Chile", relevance: "Southern and northern city hotels" },
        { market: "Guatemala", relevance: "Central America representation" },
      ],
      lead_owner_relationship_json: [
        {
          title: "Agreement-defined owner model",
          description: "Public site is guest-facing; owner reporting and escalation paths for third-party mandates confirmed in contracts.",
        },
      ],
      leadership_executives_json: [
        {
          name: "GHL Holding Leadership",
          title: "Latin America Hotel Platform (named C-suite roster not fully published on EN site)",
          bio: "Public EN marketing pages emphasize portfolio brands and destinations more than named executives. Diligence should request current org chart and decision-makers for management agreements.",
        },
      ],
    },
    "operator-best-fit": {
      operatorMasterId: "reciI2tYQBfMoMK9G",
      companyName: "GHL Hoteles (GHL Holding)",
      commercialFields: {
        bf_not_ideal_for:
          "Owners seeking U.S.-only remote management; ultra-luxury standalone outside GHL brand families without funding; mandates outside Latin America; exploratory inquiries without decision authority",
        bestFitOwnerTypes:
          "Owners and developers of full-service urban or leisure hotels in Latin America seeking an in-region multi-brand operator platform",
        bestFitGeographies: "Colombia; Peru; Chile; Guatemala; broader Latin America where GHL brand families fit",
        bf_fit_criteria_json: [
          { fitCriteria: "Market Fit", operatorLooksFor: "Latin America destinations aligned to GHL four-country operating depth.", importance: "High" },
          { fitCriteria: "Asset Type Fit", operatorLooksFor: "Full-service hotels with meetings/F&B potential; lifestyle and leisure brand-family fits.", importance: "High" },
          { fitCriteria: "Ownership Fit", operatorLooksFor: "Owners wanting in-region brand-platform operations with events capability.", importance: "High" },
          { fitCriteria: "Brand / Flag Fit", operatorLooksFor: "Assets that fit GHL, Style, Relax, Collection, Geotel, or related labels—or clearly scoped third-party terms.", importance: "High" },
        ],
        bf_best_fit_project_types_json: [
          { fitLevel: "Best Fit", projectType: "Urban Full-Service", ownerContext: "100+ room hotels with meetings, F&B, and gym/wet areas." },
          { fitLevel: "Best Fit", projectType: "Lifestyle / Leisure Brand Family", ownerContext: "Style or Relax positioning within GHL family." },
          { fitLevel: "Selective Fit", projectType: "Resort / Collection", ownerContext: "When Irotama/Collection-style operating model and destination fit." },
          { fitLevel: "Selective Fit", projectType: "Third-Party Management", ownerContext: "Only when agreement, reporting, and brand scope are explicit—public site is brand-inventory led." },
        ],
        lessIdealSituations:
          "Unfunded brand obligations; assets outside Latin America; owners needing published institutional owner-portal proof without agreement diligence",
        minPropertySize: 80,
        maxPropertySize: 400,
      },
    },
    "operator-recognition-explorer": {
      _meta: {
        operatorMasterId: "reciI2tYQBfMoMK9G",
        operatorName: "GHL Hoteles (GHL Holding)",
      },
      profileFields: {
        overview_signal_1_value: "35 hotels · 4 countries · 18 destinations (ghlhoteles.com)",
        overview_signal_2_value: "3,433 rooms · ~2,000 collaborators",
        overview_signal_3_value: "Multi-brand Latin America platform (GHL family + Geotel/resort labels)",
      },
      commercialFields: {
        industryRecognition:
          "Public company materials emphasize Latin America destination coverage and multi-brand hotel platform scale. Named third-party awards are not asserted without separate verified sources.",
      },
      governanceFields: {
        certifications:
          "Brand-family operating standards for full-service GHL category hotels (meetings, F&B, amenities)\nConfirm safety, insurance, and franchise certifications per asset in diligence",
      },
    },
  },
  diligence: [
    {
      question: "What is GHL's published Latin America scale?",
      answer:
        "Official EN home page cites approximately 35 hotels, 4 countries, 18 destinations, 3,433 rooms, and 2,000 collaborators.",
    },
    {
      question: "Which countries are in the active footprint?",
      answer: "Colombia, Peru, Chile, and Guatemala per official destinations and home pages.",
    },
    {
      question: "What brand families does GHL publish?",
      answer:
        "GHL, GHL Style, GHL Relax, GHL Collection, Geotel, Irotama Resort, and Latam Hotel Corporation labels appear on official brand/hotel pages.",
    },
    {
      question: "Is the public site a third-party management brochure?",
      answer:
        "Primarily an operator-branded hotel platform for guests. Treat third-party management, owner reporting, and PIP terms as agreement-level diligence items.",
    },
  ],
});
