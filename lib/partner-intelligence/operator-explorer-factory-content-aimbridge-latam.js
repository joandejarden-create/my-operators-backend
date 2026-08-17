/**
 * Aimbridge LATAM — Operator Explorer factory content pack.
 * Sources: aimbridgelatam.com; Aimbridge Hospitality parent news (labeled parent context).
 * Master recordId filled after create-aimbridge-latam-operator-master apply.
 */
export const AIMBRIDGE_LATAM_FACTORY_CONTENT = Object.freeze({
  slug: "aimbridge-latam",
  recordId: "recGWxIJqnYHkJZFD",
  companyName: "Aimbridge Hospitality (LATAM)",
  domain: "aimbridgelatam.com",
  parentDomain: "aimbridgehospitality.com",
  suffix: "aimbridge-latam",
  intentionalSuppress: {
    "op.proof.ownerReferences": "Owner references not published on Explorer (confidential)",
    "op.proof.lenderReferences": "Lender references not published on Explorer (confidential)",
    "op.snapshot.parentCompany":
      "Aimbridge Hospitality enterprise platform labeled in narrative; LATAM division is the Explorer lens",
    "op.snapshot.yearEstablished":
      "LATAM division founding year not published; parent Aimbridge Hospitality is multi-decade enterprise context only — not inventing a LATAM year",
    "op.proof.yearsInBusiness":
      "LATAM division years-in-business not published as a single figure — confirm in diligence",
    "op.snapshot.totalProperties":
      "Exact LATAM property count not published as a single figure on aimbridgelatam.com — Mexico-weighted growth narrative used instead",
    "op.snapshot.totalRooms":
      "Exact LATAM room count not published as a single figure — confirm in diligence",
  },
  fixtures: {
    "operator-profile-explorer": {
      _meta: {
        operatorName: "Aimbridge Hospitality (LATAM)",
        sourceUrl: "https://aimbridgelatam.com/en/home/",
        note: "LATAM division profile. Parent Aimbridge Hospitality scale labeled as enterprise context—not a substitute for LATAM-specific claims.",
      },
      profileFields: {
        companyHistory:
          "Aimbridge LATAM is the Latin America division of Aimbridge Hospitality, a global third-party hotel management company. The LATAM platform partners with owners across Mexico and Latin America on major international brands and independent assets, combining Aimbridge enterprise standards with in-region market knowledge. In 2026 Aimbridge appointed Alex Fiz as President of the LATAM and All-Inclusive divisions (effective March 2, 2026), succeeding Leandro Castillo, who moved to an advisory transition role.",
        missionStatement:
          "Deliver consistent operational performance and long-term owner value across Mexico and Latin America—pairing international brand standards with deep regional operating knowledge and a results-oriented culture.",
        differentiators:
          "Mexico-weighted third-party management with Caribbean and Central America growth; brand alliances with IHG, Wyndham, Marriott, and Hilton; dedicated All-Inclusive division capability; in-market development leadership for Mexico/Central America and Caribbean & All-Inclusive business development.",
        managementPhilosophy:
          "Owner-aligned third-party management with international standards, regional commercial and F&B depth, and specialized all-inclusive resort operating capability.",
        industryRecognition:
          "Parent Aimbridge Hospitality is publicly positioned as a leading global third-party manager; LATAM-specific awards should be diligence-confirmed. Recent public recognition includes national recognition for JW Marriott Monterrey Valle (Orfebre Cocina Artesana) cited on Aimbridge LATAM channels.",
        notableAchievements:
          "Public LATAM leadership appointments (Alex Fiz; Luis René Sánchez — VP Development Mexico & Central America; Javier Sánchez — VP Business Development Caribbean & All-Inclusive; Davide Preziuso — Director F&B LATAM & All-Inclusive); strategic alliances including Noval Properties (Dominican Republic) and Grupo Satli / Marriott all-inclusive Riviera Maya project announcements (2026).",
        overview_signal_1_value: "LATAM third-party platform · Mexico-weighted portfolio (aimbridgelatam.com)",
        overview_signal_2_value: "Brand alliances: IHG · Wyndham · Marriott · Hilton (public LATAM materials)",
        overview_signal_3_value: "All-Inclusive division + Caribbean BD leadership (2025–2026 public appointments)",
      },
    },
    "operator-operating-explorer": {
      _meta: {
        operatorName: "Aimbridge Hospitality (LATAM)",
        sourceUrl: "https://aimbridgelatam.com/en/home/",
      },
      platformFields: {
        cap_kpi_operating_model: "Third-Party LATAM + All-Inclusive Platform",
        cap_kpi_execution_strength: "Proven",
        cap_kpi_transition: "Strong",
        cap_kpi_reporting: "Structured",
        cap_signal_budget: "Not Measured / N/A",
        cap_signal_lift: "Not Measured / N/A",
        cap_signal_trans: "Not Measured / N/A",
        cap_profile_operational:
          "Third-party hotel management across Mexico and Latin America\nBrand-standard operations for IHG, Wyndham, Marriott, and Hilton family assets\nDedicated All-Inclusive division capability for resort leisure assets\nIn-market F&B leadership for LATAM & All-Inclusive (Davide Preziuso)\nEnterprise Aimbridge playbooks behind regional execution",
        cap_profile_commercial:
          "Owner-aligned commercial performance culture\nBrand CRS/distribution participation by flag\nLeisure and all-inclusive segmentation via dedicated division\nBusiness hotel and resort development coverage (Mexico & Central America)\nCaribbean & All-Inclusive business development leadership",
        cap_profile_transition:
          "Active development appointments for Mexico/Central America and Caribbean & All-Inclusive\nPublic pipeline signals: Noval Properties DR alliance; Grupo Satli / Marriott Riviera Maya all-inclusive project\nBrand reflag and lifestyle conversion capability within major franchise ecosystems\nOpenings and takeover discipline backed by Aimbridge enterprise resources",
        cap_card_asset_positioning:
          "Aimbridge LATAM is a regional third-party platform for branded and independent hotels in Mexico and Latin America—with specialized all-inclusive resort capability—not a U.S.-only remote model.",
        cap_card_service_diff:
          "Differentiation: Aimbridge enterprise depth + LATAM in-market leadership + All-Inclusive division + named development coverage for Mexico/Central America and Caribbean.",
        cap_card_execution_rel:
          "Leadership transition to Alex Fiz (2026) with Castillo advisory continuity; owners should underwrite regional operating depth plus enterprise continuity.",
        cap_card_governance:
          "Institutional owner reporting expected via Aimbridge platform; confirm cadence and pack formats in management agreements.",
        cap_deep_revenue_systems:
          "Brand-appropriate RMS/CRS by flag\nLeisure and all-inclusive commercial strategies\nOwner commercial reviews with pacing context\nRegional development pipeline coordination",
        cap_deep_execution_infra:
          "LATAM operating division + All-Inclusive specialty\nIn-market development and BD leadership seats\nF&B leadership for LATAM & All-Inclusive\nAimbridge enterprise shared resources for scale and purchasing",
      },
      operatingPlatform: {
        snapshotKpis: [
          { rowKey: "revenue_management_capability", title: "Commercial Engine", value: "Owner-Aligned LATAM Platform" },
          { rowKey: "owner_reporting_level", title: "Owner Reporting", value: "Structured" },
          { rowKey: "pre_opening_support", title: "Pre-Opening Support", value: "Strong" },
          { rowKey: "conversion_reflag", title: "Conversion Capability", value: "Strong" },
          { rowKey: "fb_capability", title: "F&B & All-Inclusive", value: "Proven" },
        ],
        positioningCards: [
          {
            rowKey: "cap_card_asset_positioning",
            title: "Asset Positioning",
            description:
              "Regional third-party platform for branded and independent hotels in Mexico and Latin America, with specialized all-inclusive resort capability.",
          },
          {
            rowKey: "cap_card_service_diff",
            title: "Service Differentiation",
            description:
              "Aimbridge enterprise depth plus LATAM in-market leadership, All-Inclusive division, and named Mexico/Central America and Caribbean development coverage.",
          },
          {
            rowKey: "cap_card_execution_rel",
            title: "Execution Reliability",
            description:
              "2026 LATAM presidency transition to Alex Fiz with advisory continuity from Leandro Castillo—pair regional depth with enterprise continuity.",
          },
        ],
        pillars: {
          commercialEngine: {
            title: "Commercial Engine",
            description:
              "Brand-system commercial participation with LATAM leisure/all-inclusive segmentation and owner pacing discipline.",
            items: [
              { title: "Major brand distribution", description: "IHG, Wyndham, Marriott, and Hilton family systems participation by asset." },
              { title: "All-Inclusive commercial", description: "Dedicated division for experience-driven resort leisure demand." },
              { title: "Business + resort mix", description: "Development coverage spanning business hotels and resorts in Mexico & Central America." },
              { title: "Caribbean BD", description: "Caribbean & All-Inclusive business development leadership seat." },
              { title: "Owner commercial reviews", description: "Performance and pacing narrative for ownership teams." },
              { title: "Enterprise commercial toolkit", description: "Aimbridge parent resources behind regional teams." },
            ],
          },
          ownerReporting: {
            title: "Owner Reporting",
            description: "Structured owner reporting aligned to institutional third-party management expectations.",
            items: [
              { title: "Monthly operating packs", description: "Asset performance, forecast, and variance commentary (confirm format)." },
              { title: "Brand compliance visibility", description: "QA and franchise obligation tracking by flag." },
              { title: "Pipeline & development updates", description: "Owner communication on openings and alliances where relevant." },
            ],
          },
          preOpeningTransition: {
            title: "Transitions & Openings",
            description: "Takeovers, reflags, and new resort developments across Mexico, Caribbean, and Central America.",
            items: [
              { title: "All-inclusive openings", description: "Specialized division for large resort projects (e.g. Riviera Maya announcements)." },
              { title: "Brand conversions", description: "Reflag/lifestyle conversions within major franchise ecosystems." },
              { title: "Cross-border growth", description: "DR alliance and Caribbean BD expanding beyond Mexico core." },
            ],
          },
          conversionRepositioning: {
            title: "Conversion & Repositioning",
            description: "Active conversion and lifestyle repositioning within major brand families.",
            items: [
              { title: "Major brand reflags", description: "Move assets into IHG, Wyndham, Marriott, or Hilton systems with Aimbridge operating support." },
              { title: "Lifestyle repositioning", description: "Public pipeline includes lifestyle/premium brand conversions." },
            ],
          },
          fbLifestyleResort: {
            title: "F&B, Lifestyle & Resort",
            description: "F&B and all-inclusive resort capability are explicit LATAM leadership priorities.",
            items: [
              { title: "LATAM & AI F&B leadership", description: "Davide Preziuso — Director F&B for LATAM & All-Inclusive." },
              { title: "All-Inclusive division", description: "Dedicated division for experience-driven resort leisure assets." },
              { title: "Culinary recognition", description: "Public LATAM channels cite JW Marriott Monterrey Valle culinary recognition." },
            ],
          },
        },
      },
    },
    "operator-brand-explorer": {
      _meta: {
        operatorName: "Aimbridge Hospitality (LATAM)",
        sourceUrl: "https://aimbridgelatam.com/en/home/",
      },
      profileFields: {
        numberOfBrands: 4,
        brandedVsIndependentMix:
          "Majority branded through alliances with IHG, Wyndham, Marriott, and Hilton; also manages independent properties per LATAM public positioning.",
        brand_conversion_project_count: "Active (public reflag / lifestyle examples — confirm count in diligence)",
        brand_narrative_compliance:
          "Brand compliance delivered through franchise/soft-brand operating practices backed by Aimbridge platform and LATAM leadership—not a proprietary single-brand system.",
        brand_narrative_relationship:
          "Aimbridge LATAM publicly cites alliances with InterContinental Hotels Group, Wyndham Hotel Group, Marriott International, and Hilton Worldwide across Mexico and Latin America.",
        brand_signal_audit: "Not Measured / N/A",
        brand_signal_reflag: "Strong",
        brand_signal_franchise_align: "Strong",
        brand_signal_soft_retention: "Moderate",
      },
      brandRelationships: {
        snapshotMetrics: [
          { rowKey: "brand_relationships_count", title: "Brand Families", value: "4+" },
          { rowKey: "branded_portfolio", title: "Branded Portfolio", value: "Majority branded" },
          { rowKey: "independent_soft", title: "Independent", value: "Also managed" },
          { rowKey: "conversion_reflag", title: "Conversion / Reflag", value: "Active" },
          { rowKey: "approved_families", title: "Cited Brand Groups", value: "4" },
          { rowKey: "primary_segments", title: "Primary Segments", value: "Full-service · Select · Lifestyle · All-Inclusive" },
        ],
        portfolioMix: [
          { brandFlagType: "Marriott International", portfolioMix: "Active LATAM", assetContext: "Includes JW Marriott Monterrey Valle public recognition; AI resort projects", relationshipStatus: "Active" },
          { brandFlagType: "Hilton Worldwide", portfolioMix: "Active LATAM", assetContext: "Cited alliance partner across Mexico/LATAM", relationshipStatus: "Active" },
          { brandFlagType: "IHG Hotels & Resorts", portfolioMix: "Active LATAM", assetContext: "Cited alliance partner across Mexico/LATAM", relationshipStatus: "Active" },
          { brandFlagType: "Wyndham Hotels & Resorts", portfolioMix: "Active LATAM", assetContext: "Cited alliance partner across Mexico/LATAM", relationshipStatus: "Active" },
        ],
      },
      brandFields: {
        brandFamiliesOperated: ["Marriott", "Hilton", "IHG", "Wyndham"],
      },
    },
    "operator-markets-explorer": {
      _meta: {
        operatorName: "Aimbridge Hospitality (LATAM)",
        sourceUrl: "https://aimbridgelatam.com/en/home/",
      },
      marketsFields: {
        activeCountries: ["Mexico"],
        activeMarkets:
          "Mexico (properties throughout the Mexican Republic per LATAM site); expanding Caribbean/Dominican Republic presence via alliances; Central America in development remit",
        priorityMarkets: "Mexico business hotels and resorts; Riviera Maya / Caribbean all-inclusive corridors; Dominican Republic growth",
        targetGrowthMarkets: "Caribbean & All-Inclusive; Mexico & Central America business and resort development",
        regions: "Latin America, Caribbean, Mexico",
        markets_regional_portfolio_json: [
          {
            title: "Mexico (core)",
            description:
              "Public LATAM materials emphasize constant growth and properties throughout the Mexican Republic with major brand alliances.",
          },
          {
            title: "Caribbean & All-Inclusive",
            description:
              "Dedicated All-Inclusive division and Caribbean BD leadership; public DR (Noval Properties) and Riviera Maya project signals.",
          },
          {
            title: "Central America (development)",
            description:
              "Luis René Sánchez leads development for Mexico and Central America focusing on business hotels and resorts.",
          },
        ],
      },
      footprintGeoFields: {
        overview_signal_1_value: "Mexico-weighted LATAM third-party portfolio",
        overview_signal_2_value: "Caribbean / All-Inclusive expansion in public pipeline",
      },
    },
    "operator-engagement-explorer": {
      _meta: {
        operatorName: "Aimbridge Hospitality (LATAM)",
      },
      commercialFields: {
        ov_card_discipline:
          "Aimbridge LATAM presents as an owner-aligned third-party platform with enterprise Aimbridge backing and in-region leadership.",
        ov_card_commercial:
          "Commercial depth spans major brand systems plus specialized all-inclusive leisure strategies.",
        ov_card_communication:
          "Expect institutional owner reporting rhythms; confirm exact calendar and portal features in the management agreement.",
        ov_card_flexibility:
          "Flexible across business hotels, resorts, independents, and all-inclusive assets within LATAM growth corridors.",
        ov_card_risk:
          "Leadership transition (Castillo → Fiz, 2026) is a diligence topic; continuity via advisory period and Aimbridge enterprise resources.",
        ov_cluster_interaction:
          "Monthly operating reviews, quarterly strategy sessions, and ad-hoc escalation for brand/capex/commercial decisions.",
        ov_cluster_deliverables:
          "Operating plans, forecasts, brand QA status, commercial pacing, and owner packs tailored to lender/owner requirements.",
        ownerEngagementNarrative:
          "Aimbridge LATAM is framed for owners seeking third-party management with major brand capability and regional LATAM/all-inclusive depth, backed by Aimbridge Hospitality enterprise resources.",
      },
      engagementReporting: {
        strategicOwnerValue: [
          {
            title: "Enterprise + LATAM depth",
            description: "Aimbridge global third-party platform behind an in-region LATAM operating and development team.",
          },
          {
            title: "Major brand alliances",
            description: "Public alliances with IHG, Wyndham, Marriott, and Hilton for Mexico/LATAM portfolios.",
          },
          {
            title: "All-Inclusive specialty",
            description: "Dedicated All-Inclusive division for experience-driven resort assets.",
          },
          {
            title: "Named development coverage",
            description: "Mexico/Central America development and Caribbean & All-Inclusive BD leadership seats.",
          },
        ],
        engagementCadence: [
          { cadence: "Monthly", engagementType: "Owner operating & financial review", focus: "P&L, forecast, labor, commercial pacing, brand QA." },
          { cadence: "Quarterly", engagementType: "Strategic business review", focus: "Market, competitive set, capex, brand strategy." },
          { cadence: "Annually", engagementType: "Budget & business plan", focus: "Operating budget, staffing, commercial plan." },
          { cadence: "Ad hoc", engagementType: "Escalation", focus: "Brand-sensitive, weather, or owner-reputation incidents." },
        ],
        controlsGovernance: [
          { title: "Budget & forecast", description: "Annual budget with owner review and rolling forecasts." },
          { title: "Brand compliance", description: "Franchise obligations coordinated with LATAM operating leaders." },
          { title: "CapEx & PIP", description: "Capital prioritization with brand requirement tracking." },
        ],
        reportsReceived: [
          { title: "Monthly owner pack", description: "Operating and financial summary with variance narrative." },
          { title: "Commercial pacing", description: "Brand-system and leisure/AI demand tracking." },
        ],
      },
    },
    "operator-infrastructure-explorer": {
      _meta: {
        operatorName: "Aimbridge Hospitality (LATAM)",
      },
      governanceFields: {
        infra_technology_maturity_level: "Structured",
        infra_asset_management_reporting:
          "Monthly owner packs\nBrand QA and franchise tracking\nPipeline/opening milestone visibility for developments",
        infra_systems_technology:
          "PMS/RMS/CRS: Brand-mandated by flag (Marriott, Hilton, IHG, Wyndham)\nAccounting & owner reporting: Aimbridge platform practices\nOpenings/IT: Enterprise + LATAM transition playbooks",
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
        infra_data_governance_json: [
          { title: "Owner data discipline", description: "Agreement-defined access to financial and operating data." },
        ],
        infra_analytics_support_json: [
          { title: "Commercial pacing analytics", description: "Owner-facing pacing and forecast support." },
        ],
      },
    },
    "operator-leadership-explorer": {
      _meta: {
        operatorName: "Aimbridge Hospitality (LATAM)",
        sourceUrl: "https://aimbridgelatam.com/en/home/",
        note: "Leadership from Aimbridge LATAM site + Aimbridge Hospitality parent announcements (parent labeled).",
      },
      lead_avg_hospitality_experience: "20+ yrs (executive bench)",
      lead_org_structure_json: [
        {
          title: "LATAM & All-Inclusive Division Leadership",
          description: "Alex Fiz — President, Aimbridge LATAM and All-Inclusive Divisions (effective March 2, 2026).",
          tags: ["Alex Fiz", "LATAM", "All-Inclusive"],
        },
        {
          title: "Transition Advisory",
          description: "Leandro Castillo — retiring President of Aimbridge LATAM; advisory capacity through transition.",
          tags: ["Leandro Castillo", "Advisory"],
        },
        {
          title: "Development — Mexico & Central America",
          description: "Luis René Sánchez — Vice President of Development for Mexico and Central America (business hotels and resorts).",
          tags: ["Luis René Sánchez", "Mexico", "Central America"],
        },
        {
          title: "Business Development — Caribbean & All-Inclusive",
          description: "Javier Sánchez — Vice President of Business Development, Caribbean & All-Inclusive.",
          tags: ["Javier Sánchez", "Caribbean", "All-Inclusive"],
        },
        {
          title: "F&B — LATAM & All-Inclusive",
          description: "Davide Preziuso — Director of Food & Beverage for LATAM & All-Inclusive.",
          tags: ["Davide Preziuso", "F&B"],
        },
      ],
      lead_team_depth_json: [
        {
          function: "Division Presidency",
          leadRole: "Alex Fiz · President LATAM & All-Inclusive",
          depth: "Very Strong",
          relevance: "Overall LATAM and All-Inclusive operating and growth accountability; prior Marriott CALA AI leadership experience (parent announcement).",
        },
        {
          function: "Development",
          leadRole: "Luis René Sánchez · VP Development Mexico & Central America",
          depth: "Strong",
          relevance: "Business hotel and resort development pipeline in Mexico and Central America.",
        },
        {
          function: "Caribbean & All-Inclusive BD",
          leadRole: "Javier Sánchez · VP Business Development",
          depth: "Strong",
          relevance: "Caribbean and all-inclusive growth mandates.",
        },
        {
          function: "Food & Beverage",
          leadRole: "Davide Preziuso · Director F&B LATAM & All-Inclusive",
          depth: "Strong",
          relevance: "Culinary and F&B strategy for LATAM and AI assets.",
        },
      ],
      lead_language_capability_json: [
        { language: "Spanish", capability: "Primary LATAM operating and owner communication language" },
        { language: "English", capability: "Enterprise Aimbridge and international brand coordination" },
      ],
      lead_governance_cadence_json: [
        { cadence: "Monthly", focus: "Owner operating reviews" },
        { cadence: "Quarterly", focus: "Strategic business reviews" },
      ],
      lead_team_markets_json: [
        { market: "Mexico", relevance: "Core portfolio geography" },
        { market: "Caribbean / Dominican Republic", relevance: "Growth via AI and alliances" },
        { market: "Central America", relevance: "Development remit" },
      ],
      lead_owner_relationship_json: [
        {
          title: "Owner-aligned third-party model",
          description: "LATAM leadership partners with owners for operational performance and long-term value; enterprise Aimbridge resources available behind regional teams.",
        },
      ],
      leadership_executives_json: [
        {
          name: "Alex Fiz",
          title: "President, Aimbridge LATAM and All-Inclusive Divisions",
          bio: "Appointed effective March 2, 2026. Brings extensive operating and commercial experience across Europe, Latin America, and the Caribbean, including Marriott all-inclusive strategy and operations leadership (per Aimbridge Hospitality announcement).",
        },
        {
          name: "Leandro Castillo",
          title: "Former President, Aimbridge LATAM (Advisory)",
          bio: "Retired from Aimbridge LATAM presidency effective Feb 28, 2026; serving in an advisory capacity to support transition.",
        },
        {
          name: "Luis René Sánchez",
          title: "Vice President of Development — Mexico and Central America",
          bio: "Leads development for business hotels and resorts across Mexico and Central America (Aimbridge LATAM public appointment).",
        },
        {
          name: "Javier Sánchez",
          title: "Vice President of Business Development — Caribbean & All-Inclusive",
          bio: "Leads Caribbean and All-Inclusive business development for Aimbridge Hospitality / LATAM growth.",
        },
        {
          name: "Davide Preziuso",
          title: "Director of Food & Beverage — LATAM & All-Inclusive",
          bio: "F&B leadership for LATAM and All-Inclusive with 24+ years hospitality culinary experience (Aimbridge LATAM public materials).",
        },
      ],
    },
    "operator-best-fit": {
      companyName: "Aimbridge Hospitality (LATAM)",
      commercialFields: {
        bf_not_ideal_for:
          "Owners needing only remote U.S. management without LATAM presence; assets outside Mexico/LATAM/Caribbean focus; unfunded brand PIP; exploratory inquiries without decision authority",
        bestFitOwnerTypes:
          "Institutional and private owners of branded or independent hotels in Mexico and Latin America seeking third-party management with major brand capability and/or all-inclusive resort operating depth",
        bestFitGeographies: "Mexico; Caribbean / Dominican Republic; Central America resort and business hotel corridors",
        bf_fit_criteria_json: [
          { fitCriteria: "Market Fit", operatorLooksFor: "Mexico, Caribbean, and Central America destinations where LATAM teams can staff and support.", importance: "High" },
          { fitCriteria: "Asset Type Fit", operatorLooksFor: "Business hotels, resorts, lifestyle, and all-inclusive assets.", importance: "High" },
          { fitCriteria: "Ownership Fit", operatorLooksFor: "Owners wanting third-party management with Aimbridge enterprise depth and regional accountability.", importance: "High" },
          { fitCriteria: "Brand / Flag Fit", operatorLooksFor: "IHG, Wyndham, Marriott, Hilton, or independent assets where Aimbridge LATAM adds operating value.", importance: "High" },
        ],
        bf_best_fit_project_types_json: [
          { fitLevel: "Best Fit", projectType: "Branded Full-Service / Select", ownerContext: "Major brand hotels in Mexico/LATAM needing professional third-party management." },
          { fitLevel: "Best Fit", projectType: "All-Inclusive Resort", ownerContext: "Leisure AI assets aligned to dedicated All-Inclusive division." },
          { fitLevel: "Best Fit", projectType: "Conversion / Reflag", ownerContext: "Assets moving into IHG/Wyndham/Marriott/Hilton ecosystems." },
          { fitLevel: "Selective Fit", projectType: "Independent", ownerContext: "When Aimbridge LATAM operating model fits owner goals without brand conversion." },
        ],
        lessIdealSituations:
          "Assets far outside LATAM operating corridors; owners unwilling to fund brand obligations; mandates requiring no regional presence",
        minPropertySize: 100,
        maxPropertySize: 1000,
      },
    },
    "operator-recognition-explorer": {
      _meta: {
        operatorName: "Aimbridge Hospitality (LATAM)",
      },
      profileFields: {
        overview_signal_1_value: "LATAM third-party platform with Mexico-weighted portfolio",
        overview_signal_2_value: "IHG · Wyndham · Marriott · Hilton alliances (public)",
        overview_signal_3_value: "All-Inclusive division + 2026 LATAM leadership appointments",
      },
      commercialFields: {
        industryRecognition:
          "Aimbridge LATAM public channels cite national recognition for JW Marriott Monterrey Valle — Orfebre Cocina Artesana. Parent Aimbridge Hospitality awards are enterprise context and should be labeled separately from LATAM-specific claims.",
      },
      governanceFields: {
        certifications:
          "Brand-standard QA and franchise compliance by asset (Marriott, Hilton, IHG, Wyndham)\nAimbridge enterprise safety and continuity frameworks scaled to LATAM assets",
      },
    },
  },
  diligence: [
    {
      question: "Who leads Aimbridge LATAM today?",
        answer:
          "Alex Fiz is President of Aimbridge LATAM and All-Inclusive Divisions effective March 2, 2026; Leandro Castillo serves in an advisory capacity after retiring as LATAM President.",
    },
    {
      question: "Which brand groups does Aimbridge LATAM publicly cite?",
      answer: "InterContinental Hotels Group, Wyndham Hotel Group, Marriott International, and Hilton Worldwide.",
    },
    {
      question: "What geographies are in focus?",
      answer:
        "Mexico is the core published footprint; Caribbean/Dominican Republic and Central America appear in development and alliance announcements, with a dedicated All-Inclusive division.",
    },
    {
      question: "How should owners treat parent Aimbridge scale claims?",
      answer:
        "Label parent Aimbridge Hospitality enterprise context separately. This Explorer profile is the LATAM division lens—underwrite regional operating depth and agreement terms for the specific asset.",
    },
  ],
});
