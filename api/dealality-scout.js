function getPriorityLabel(score) {
  if (score >= 85) return "Priority Target";
  if (score >= 70) return "Strong Target";
  if (score >= 55) return "Monitor / Needs Validation";
  if (score >= 40) return "Low Priority";
  return "Insufficient Evidence";
}

function buildMockData() {
  const markets = [
    {
      id: "mkt-punta-cana",
      marketName: "Punta Cana",
      country: "Dominican Republic",
      submarket: "Bavaro / Uvero Alto",
      scoutOpportunityScore: 88,
      confidenceScore: 82,
      confidenceLevel: "High",
      totalHotels: 158,
      brandedHotels: 62,
      independentHotels: 96,
      parentCompaniesPresent: ["Marriott International", "Hyatt", "Meliá", "Hilton"],
      brandsPresent: ["Westin", "JW Marriott", "Dreams", "Paradisus", "Hilton"],
      brandsMissing: ["Kimpton", "Voco", "Motto by Hilton"],
      topDemandDrivers: ["All-inclusive demand", "Airport throughput growth", "MICE pipeline"],
      topWhiteSpaceOpportunities: ["Lifestyle all-inclusive", "Adults-only upper-upscale"],
      recommendedGrowthPlays: ["Conversion bundle", "Operator-led repositioning"],
      recommendedNextAction: "Request validation on independent luxury cluster"
    },
    {
      id: "mkt-santo-domingo",
      marketName: "Santo Domingo",
      country: "Dominican Republic",
      submarket: "Piantini / Colonial Zone",
      scoutOpportunityScore: 79,
      confidenceScore: 76,
      confidenceLevel: "High",
      totalHotels: 132,
      brandedHotels: 58,
      independentHotels: 74,
      parentCompaniesPresent: ["Marriott International", "Hilton", "IHG"],
      brandsPresent: ["Sheraton", "Embassy Suites", "Holiday Inn"],
      brandsMissing: ["AC Hotels", "Canopy", "Curio Collection"],
      topDemandDrivers: ["Corporate travel", "Government demand", "Medical tourism"],
      topWhiteSpaceOpportunities: ["Upper-upscale CBD lifestyle", "Extended stay"],
      recommendedGrowthPlays: ["CBD conversion program", "Corporate RFP lead-in"],
      recommendedNextAction: "Build owner short-list for CBD rebrand targets"
    },
    {
      id: "mkt-cartagena",
      marketName: "Cartagena",
      country: "Colombia",
      submarket: "Bocagrande / Walled City",
      scoutOpportunityScore: 84,
      confidenceScore: 80,
      confidenceLevel: "High",
      totalHotels: 117,
      brandedHotels: 44,
      independentHotels: 73,
      parentCompaniesPresent: ["Accor", "Hilton", "Hyatt"],
      brandsPresent: ["Sofitel", "Hilton", "Hyatt Regency"],
      brandsMissing: ["Tribute Portfolio", "Curio Collection", "NH Collection"],
      topDemandDrivers: ["Leisure premium", "Cruise spillover", "Destination weddings"],
      topWhiteSpaceOpportunities: ["Soft brand luxury", "Boutique collection positioning"],
      recommendedGrowthPlays: ["Soft-brand conversion", "Historic asset repositioning"],
      recommendedNextAction: "Validate 3 distressed independents near Bocagrande"
    },
    {
      id: "mkt-medellin",
      marketName: "Medellín",
      country: "Colombia",
      submarket: "El Poblado / Laureles",
      scoutOpportunityScore: 74,
      confidenceScore: 68,
      confidenceLevel: "Medium",
      totalHotels: 149,
      brandedHotels: 52,
      independentHotels: 97,
      parentCompaniesPresent: ["Marriott International", "Hilton", "Accor"],
      brandsPresent: ["Marriott", "Four Points", "Novotel"],
      brandsMissing: ["Aloft", "Moxy", "Tru by Hilton"],
      topDemandDrivers: ["Tech ecosystem", "Medical travel", "Regional corporate HQs"],
      topWhiteSpaceOpportunities: ["Lifestyle select-service", "Branded extended stay"],
      recommendedGrowthPlays: ["Multi-asset conversion funnel", "Operator partner outreach"],
      recommendedNextAction: "Map operator relationships in El Poblado corridor"
    },
    {
      id: "mkt-san-jose",
      marketName: "San José",
      country: "Costa Rica",
      submarket: "Escazú / Santa Ana",
      scoutOpportunityScore: 71,
      confidenceScore: 66,
      confidenceLevel: "Medium",
      totalHotels: 91,
      brandedHotels: 39,
      independentHotels: 52,
      parentCompaniesPresent: ["Marriott International", "Hilton", "IHG"],
      brandsPresent: ["AC Hotels", "Courtyard", "Hilton Garden Inn"],
      brandsMissing: ["Hyatt Place", "Moxy", "Kimpton"],
      topDemandDrivers: ["Nearshore business", "Gateway demand", "Airport investment"],
      topWhiteSpaceOpportunities: ["Upper-midscale conversion", "Airport-linked select-service"],
      recommendedGrowthPlays: ["Airport district targeting", "Franchise pathway"],
      recommendedNextAction: "Prioritize owner outreach in Escazú independents"
    },
    {
      id: "mkt-cancun-riviera",
      marketName: "Cancún / Riviera Maya",
      country: "Mexico",
      submarket: "Hotel Zone / Playa del Carmen",
      scoutOpportunityScore: 91,
      confidenceScore: 85,
      confidenceLevel: "High",
      totalHotels: 211,
      brandedHotels: 88,
      independentHotels: 123,
      parentCompaniesPresent: ["Marriott International", "Hyatt", "Hilton", "Accor", "IHG"],
      brandsPresent: ["Dreams", "Secrets", "Waldorf Astoria", "Marriott", "Hilton"],
      brandsMissing: ["Edition", "Motto by Hilton", "Vignette Collection"],
      topDemandDrivers: ["International airlift", "All-inclusive momentum", "Luxury leisure"],
      topWhiteSpaceOpportunities: ["Soft-brand premium", "Wellness luxury conversion"],
      recommendedGrowthPlays: ["Distress acquisition monitor", "Luxury brand fit campaigns"],
      recommendedNextAction: "Launch validation sprint on transition assets in Hotel Zone"
    }
  ];

  const assets = [
    ["asset-01", "Coral Bay Resort", "Punta Cana", "Dominican Republic", "Bavaro", "Independent", "", "Blue Current", "Palm Capital", 312, "Conversion Target", 89, "High", "Needs Validation"],
    ["asset-02", "Ocean Dune Suites", "Punta Cana", "Dominican Republic", "Uvero Alto", "Independent", "", "N/A", "Seaside Holdings", 228, "Rebrand Target", 82, "High", "Under Review"],
    ["asset-03", "Metropolitan Santo Domingo", "Santo Domingo", "Dominican Republic", "Piantini", "Independent", "", "Metro Operators", "Urbis Group", 190, "Conversion Target", 77, "High", "Needs Validation"],
    ["asset-04", "Colonial Gate Hotel", "Santo Domingo", "Dominican Republic", "Colonial Zone", "Independent", "", "Old City Hospitality", "Nexus Capital", 144, "Distressed / Transition", 73, "Medium", "Pending Analyst Review"],
    ["asset-05", "Bocagrande Towers Hotel", "Cartagena", "Colombia", "Bocagrande", "Independent", "", "Caribe Ops", "Inversiones Costa", 264, "Rebrand Target", 86, "High", "Under Review"],
    ["asset-06", "Muralla Heritage Suites", "Cartagena", "Colombia", "Walled City", "Independent", "", "Heritage Living", "Andes Family Office", 98, "Distressed / Transition", 68, "Medium", "Needs Validation"],
    ["asset-07", "Poblado Skyline Inn", "Medellín", "Colombia", "El Poblado", "Independent", "", "Nova Hospitality", "Lumen Partners", 176, "Conversion Target", 72, "Medium", "Pending Analyst Review"],
    ["asset-08", "Laureles Business Hotel", "Medellín", "Colombia", "Laureles", "Independent", "", "Urbania Operators", "Grupo Verde", 129, "Conversion Target", 66, "Medium", "Needs Validation"],
    ["asset-09", "Escazu Prime Hotel", "San José", "Costa Rica", "Escazú", "Independent", "", "Costa Ops", "Pacifica Assets", 155, "Rebrand Target", 70, "Medium", "Under Review"],
    ["asset-10", "Santa Ana Select Stay", "San José", "Costa Rica", "Santa Ana", "Independent", "", "Andina Ops", "Pura Vida Capital", 122, "Conversion Target", 63, "Medium", "Needs Validation"],
    ["asset-11", "Caribe Sol Cancun", "Cancún / Riviera Maya", "Mexico", "Hotel Zone", "Independent", "", "Maya Coast Mgmt", "Blue Reef Holdings", 340, "Distressed / Transition", 90, "High", "Under Review"],
    ["asset-12", "Playa Vista Collection", "Cancún / Riviera Maya", "Mexico", "Playa del Carmen", "Independent", "", "Sun Corridor Ops", "Mirador Ventures", 208, "Rebrand Target", 83, "High", "Needs Validation"],
    ["asset-13", "Avenida Centro Suites", "Santo Domingo", "Dominican Republic", "Gazcue", "Independent", "", "City Core Mgmt", "Alpha Crest", 116, "Monitor", 58, "Medium", "Pending Analyst Review"],
    ["asset-14", "Laguna Bay Lifestyle", "Punta Cana", "Dominican Republic", "Cap Cana", "Independent", "", "Coral Hospitality", "Argo Hospitality RE", 276, "Conversion Target", 81, "High", "Under Review"],
    ["asset-15", "Centro Empresarial Hotel", "Medellín", "Colombia", "Milla de Oro", "Independent", "", "MDE Operations", "Cordillera Capital", 134, "Monitor", 54, "Low", "Needs Validation"],
    ["asset-16", "Riviera Port Suites", "Cancún / Riviera Maya", "Mexico", "Puerto Morelos", "Independent", "", "Litoral Operators", "Mar Azul Group", 184, "Conversion Target", 75, "Medium", "Under Review"]
  ].map((row) => ({
    id: row[0],
    assetName: row[1],
    market: row[2],
    country: row[3],
    submarket: row[4],
    currentBrandOrIndependent: row[5],
    brand: row[6] || "Independent",
    parentCompany: row[7] || "Independent",
    operator: row[8],
    owner: row[9],
    roomCount: row[10],
    chainScale: row[10] >= 260 ? "Upper Upscale" : row[10] >= 180 ? "Upscale" : row[10] >= 130 ? "Upper Midscale" : "Midscale",
    opportunityType: row[11],
    targetPriorityScore: row[12],
    priorityLabel: getPriorityLabel(row[12]),
    topSignals: ["Rate under-indexing", "Ownership transition chatter", "Low brand penetration"],
    whyItMatters: "High fit against white-space needs with near-term conversion timing.",
    potentialBrandFit: ["Curio Collection", "Tribute Portfolio", "Kimpton"],
    potentialOperatorFit: ["Aimbridge", "Highgate", "Local white-label operator"],
    relationshipPathStatus: row[13] === "High" ? "Path Identified" : "Needs Mapping",
    recommendedNextAction: "Request Validation",
    confidenceScore: row[12] - 6,
    confidenceLevel: row[13],
    reviewStatus: row[14],
    serviceModel: row[11] === "Distressed / Transition" ? "Asset-light" : "Franchise"
  }));

  const signals = [
    ["sig-01", "Pipeline slowdown", "Punta Cana", "Market", "Strong", 82, "STR trend brief", "Pipeline deceleration opens conversion window.", "Prioritize conversion outreach", "Needs Validation"],
    ["sig-02", "RevPAR softening", "Coral Bay Resort", "Asset", "Moderate", 74, "CoStar scrape", "Rate compression suggests repositioning pressure.", "Request valuation refresh", "Under Review"],
    ["sig-03", "Owner refinancing event", "Caribe Sol Cancun", "Asset", "Strong", 88, "Public filing", "Debt event creates transition probability.", "Create outreach plan", "Pending Analyst Review"],
    ["sig-04", "Corporate demand growth", "Santo Domingo", "Market", "Moderate", 71, "Airport + GDP mix", "Demand growth supports upper-upscale product.", "Validate new demand centers", "Needs Validation"],
    ["sig-05", "Brand saturation gap", "Cartagena", "Market", "Strong", 81, "Brand census map", "Luxury soft-brand penetration remains low.", "Evaluate soft-brand candidates", "Under Review"],
    ["sig-06", "Operator turnover", "Poblado Skyline Inn", "Asset", "Moderate", 69, "Local news", "Turnover can accelerate reflag timing.", "Add relationship notes", "Needs Validation"],
    ["sig-07", "Convention center expansion", "Medellín", "Market", "Moderate", 66, "Municipal release", "MICE uplift supports branded conversion.", "Monitor target", "Pending Analyst Review"],
    ["sig-08", "Airport corridor growth", "San José", "Market", "Strong", 78, "Infrastructure memo", "Airport nodes improving demand reliability.", "Target airport submarket assets", "Under Review"],
    ["sig-09", "Distress watchlist trigger", "Muralla Heritage Suites", "Asset", "Strong", 84, "Broker note", "Capex burden elevates distress risk.", "Request validation", "Needs Validation"],
    ["sig-10", "Owner succession risk", "Riviera Port Suites", "Asset", "Moderate", 65, "Relationship interview", "Succession timing may unlock rebrand.", "Prepare outreach sequence", "Under Review"]
  ].map((s) => ({
    id: s[0],
    signalType: s[1],
    linkedEntity: s[2],
    linkedEntityType: s[3],
    signalStrength: s[4],
    confidenceScore: s[5],
    confidenceLevel: s[5] >= 75 ? "High" : s[5] >= 60 ? "Medium" : "Low",
    source: s[6],
    aiInterpretation: s[7],
    recommendedAction: s[8],
    humanValidationStatus: s[9],
    reviewStatus: s[9]
  }));

  const demandCenters = [
    { id: "dc-01", market: "Punta Cana", demandCenter: "PUJ Airport", demandType: "Airlift", strength: "High", confidenceScore: 83, note: "International seats +9% YoY" },
    { id: "dc-02", market: "Santo Domingo", demandCenter: "CBD Corporate District", demandType: "Corporate", strength: "High", confidenceScore: 77, note: "Stable weekday demand" },
    { id: "dc-03", market: "Cartagena", demandCenter: "Cruise Port Zone", demandType: "Leisure", strength: "Medium", confidenceScore: 70, note: "Seasonal but growing" },
    { id: "dc-04", market: "Medellín", demandCenter: "Innovation District", demandType: "Corporate", strength: "Medium", confidenceScore: 67, note: "Tech-driven travel" },
    { id: "dc-05", market: "San José", demandCenter: "Free Trade Zone", demandType: "Nearshore", strength: "Medium", confidenceScore: 64, note: "Growing tenant base" },
    { id: "dc-06", market: "Cancún / Riviera Maya", demandCenter: "Hotel Zone Beaches", demandType: "Leisure", strength: "High", confidenceScore: 86, note: "Luxury ADR resilience" },
    { id: "dc-07", market: "Cancún / Riviera Maya", demandCenter: "MICE Convention Hub", demandType: "MICE", strength: "Medium", confidenceScore: 69, note: "Corporate events recovering" },
    { id: "dc-08", market: "Cartagena", demandCenter: "Historic Center", demandType: "Lifestyle", strength: "High", confidenceScore: 75, note: "High boutique demand" }
  ];

  const relationshipPaths = [
    { id: "rel-01", linkedAsset: "Coral Bay Resort", company: "Palm Capital", contactRole: "Asset Manager", relationshipPathStatus: "Path Identified", warmIntroSource: "Regional lender", publicContactSource: "LinkedIn", confidenceScore: 79, confidenceLevel: "High", notes: "Warm intro available via debt advisor.", lastValidated: "2026-05-03" },
    { id: "rel-02", linkedAsset: "Caribe Sol Cancun", company: "Blue Reef Holdings", contactRole: "Portfolio Director", relationshipPathStatus: "Warm Intro Ready", warmIntroSource: "Former operator partner", publicContactSource: "Press release", confidenceScore: 84, confidenceLevel: "High", notes: "Known interest in franchise conversion.", lastValidated: "2026-05-01" },
    { id: "rel-03", linkedAsset: "Bocagrande Towers Hotel", company: "Inversiones Costa", contactRole: "Owner Rep", relationshipPathStatus: "Needs Mapping", warmIntroSource: "None yet", publicContactSource: "Company website", confidenceScore: 58, confidenceLevel: "Medium", notes: "Need board-level contact.", lastValidated: "2026-04-28" },
    { id: "rel-04", linkedAsset: "Metropolitan Santo Domingo", company: "Urbis Group", contactRole: "CEO Office", relationshipPathStatus: "Path Identified", warmIntroSource: "Local counsel", publicContactSource: "Conference panel", confidenceScore: 72, confidenceLevel: "High", notes: "Decision-maker reachable in 2 hops.", lastValidated: "2026-05-04" },
    { id: "rel-05", linkedAsset: "Escazu Prime Hotel", company: "Pacifica Assets", contactRole: "Investment Director", relationshipPathStatus: "Public Contact Only", warmIntroSource: "None", publicContactSource: "Industry directory", confidenceScore: 52, confidenceLevel: "Medium", notes: "Intro quality low.", lastValidated: "2026-04-25" },
    { id: "rel-06", linkedAsset: "Muralla Heritage Suites", company: "Andes Family Office", contactRole: "Principal", relationshipPathStatus: "Needs Mapping", warmIntroSource: "None", publicContactSource: "Registry filing", confidenceScore: 49, confidenceLevel: "Low", notes: "Relationship development needed.", lastValidated: "2026-04-20" },
    { id: "rel-07", linkedAsset: "Poblado Skyline Inn", company: "Lumen Partners", contactRole: "COO", relationshipPathStatus: "Path Identified", warmIntroSource: "Operator referral", publicContactSource: "LinkedIn", confidenceScore: 68, confidenceLevel: "Medium", notes: "Operator willing to introduce.", lastValidated: "2026-05-02" },
    { id: "rel-08", linkedAsset: "Riviera Port Suites", company: "Mar Azul Group", contactRole: "Managing Partner", relationshipPathStatus: "Warm Intro Ready", warmIntroSource: "Broker network", publicContactSource: "MX corporate filing", confidenceScore: 74, confidenceLevel: "High", notes: "Good fit for transition thesis.", lastValidated: "2026-05-05" }
  ];

  const targetOpportunities = assets.slice(0, 10).map((asset, idx) => ({
    id: `to-${idx + 1}`,
    linkedAssetId: asset.id,
    linkedAsset: asset.assetName,
    opportunityType: asset.opportunityType,
    targetPriorityScore: asset.targetPriorityScore,
    priorityLabel: asset.priorityLabel,
    confidenceScore: asset.confidenceScore,
    confidenceLevel: asset.confidenceLevel,
    reviewStatus: asset.reviewStatus,
    recommendedNextAction: asset.recommendedNextAction
  }));

  const sources = [
    { id: "src-01", linkedEntity: "Punta Cana", sourceType: "Market Data", sourceName: "STR Weekly Snapshot", sourceUrl: "https://example.local/source/str-pcj", dataExtracted: "RevPAR trend + occupancy by segment", confidenceScore: 79, reviewStatus: "Under Review" },
    { id: "src-02", linkedEntity: "Caribe Sol Cancun", sourceType: "Public Filing", sourceName: "Debt Disclosure", sourceUrl: "https://example.local/source/debt-cancun", dataExtracted: "Refinancing timeline + debt maturity", confidenceScore: 86, reviewStatus: "Validated" },
    { id: "src-03", linkedEntity: "Cartagena", sourceType: "Travel Data", sourceName: "Airport Traffic Bulletin", sourceUrl: "https://example.local/source/air-ctg", dataExtracted: "Inbound seat growth and mix", confidenceScore: 73, reviewStatus: "Needs Validation" },
    { id: "src-04", linkedEntity: "Escazu Prime Hotel", sourceType: "Broker Intelligence", sourceName: "Off-market note", sourceUrl: "https://example.local/source/broker-sjo", dataExtracted: "Owner disposition signal", confidenceScore: 61, reviewStatus: "Pending Analyst Review" },
    { id: "src-05", linkedEntity: "Metropolitan Santo Domingo", sourceType: "Relationship Note", sourceName: "Counsel interview", sourceUrl: "https://example.local/source/interview-sdq", dataExtracted: "Decision-maker org map", confidenceScore: 75, reviewStatus: "Under Review" },
    { id: "src-06", linkedEntity: "Medellín", sourceType: "Municipal News", sourceName: "Convention expansion report", sourceUrl: "https://example.local/source/mde-mice", dataExtracted: "MICE demand uplift projection", confidenceScore: 67, reviewStatus: "Needs Validation" }
  ];

  const outcomes = [
    { id: "out-01", linkedTargetOpportunity: "Coral Bay Resort", outreachPathUsed: "Lender intro + owner rep", responseReceived: "Yes", meetingCreated: "Yes", convertedToDealalityOpportunity: "No", lessonsLearned: "Debt pressure raises openness to brand conversion." },
    { id: "out-02", linkedTargetOpportunity: "Caribe Sol Cancun", outreachPathUsed: "Former operator referral", responseReceived: "Yes", meetingCreated: "Yes", convertedToDealalityOpportunity: "Yes", lessonsLearned: "Warm intros outperform public-contact-only outreach." },
    { id: "out-03", linkedTargetOpportunity: "Bocagrande Towers Hotel", outreachPathUsed: "Public contact", responseReceived: "No", meetingCreated: "No", convertedToDealalityOpportunity: "No", lessonsLearned: "Need local partner for credibility in first touch." },
    { id: "out-04", linkedTargetOpportunity: "Escazu Prime Hotel", outreachPathUsed: "Broker note follow-up", responseReceived: "Yes", meetingCreated: "No", convertedToDealalityOpportunity: "No", lessonsLearned: "Incomplete source confidence slowed conversion." },
    { id: "out-05", linkedTargetOpportunity: "Riviera Port Suites", outreachPathUsed: "Broker network warm intro", responseReceived: "Yes", meetingCreated: "Yes", convertedToDealalityOpportunity: "No", lessonsLearned: "Transition assets require faster diligence packet." }
  ];

  const kpis = {
    opportunityMarkets: markets.length,
    priorityTargets: assets.filter((asset) => asset.targetPriorityScore >= 85).length,
    whiteSpaceSignals: signals.length,
    needsValidation: [...assets, ...signals, ...sources].filter((row) =>
      ["Needs Validation", "Pending Analyst Review"].includes(row.reviewStatus || row.humanValidationStatus)
    ).length,
    relationshipPathsFound: relationshipPaths.length
  };

  return {
    mode: "mock",
    kpis,
    markets,
    assets,
    signals,
    demandCenters,
    relationshipPaths,
    targetOpportunities,
    sources,
    outcomes,
    lastUpdated: new Date().toISOString()
  };
}

function uniqueSorted(values) {
  return [...new Set(values.filter(Boolean))].sort((a, b) => String(a).localeCompare(String(b)));
}

function buildFilters(data) {
  return {
    countries: uniqueSorted(data.markets.map((m) => m.country)),
    markets: uniqueSorted(data.markets.map((m) => m.marketName)),
    submarkets: uniqueSorted(data.markets.map((m) => m.submarket).concat(data.assets.map((a) => a.submarket))),
    brands: uniqueSorted(data.markets.flatMap((m) => m.brandsPresent).concat(data.assets.map((a) => a.brand))),
    parentCompanies: uniqueSorted(data.assets.map((a) => a.parentCompany)),
    chainScales: ["Luxury", "Upper Upscale", "Upscale", "Upper Midscale", "Midscale", "Economy", "Independent"],
    serviceModels: uniqueSorted(data.assets.map((a) => a.serviceModel)),
    opportunityTypes: uniqueSorted(data.assets.map((a) => a.opportunityType)),
    confidenceLevels: ["High", "Medium", "Low"],
    reviewStatuses: uniqueSorted(
      data.assets.map((a) => a.reviewStatus)
        .concat(data.signals.map((s) => s.reviewStatus))
        .concat(data.sources.map((s) => s.reviewStatus))
    ),
    relationshipPathStatuses: uniqueSorted(data.relationshipPaths.map((r) => r.relationshipPathStatus))
  };
}

export function getDealalityScout(_req, res) {
  const data = buildMockData();
  res.json(data);
}

export function getDealalityScoutFilters(_req, res) {
  const data = buildMockData();
  res.json(buildFilters(data));
}
