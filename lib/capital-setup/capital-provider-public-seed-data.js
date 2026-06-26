/**
 * Curated public-source payloads for Capital Provider Explorer seeding.
 * Every populated field is traceable to sources[] on each provider package.
 */
import {
  LAST_VERIFIED,
  OWNER_DISCLAIMER,
  SEED_SOURCE_TAG,
} from "./capital-provider-public-seed-constants.js";

function ownerNotes(regionLabel) {
  return `This capital provider may be relevant for hotel owners seeking financing for tourism, hospitality, or accommodation-related projects in ${regionLabel}, based on publicly available information and prior hotel/tourism financing activity. ${OWNER_DISCLAIMER}`;
}

export const PUBLIC_SEED_PROVIDERS = [
  {
    name: "IDB Invest",
    provider: {
      institutionType: "Development Finance Institution",
      profileStatus: "Active",
      visibilityLevel: "Public",
      shortDescription:
        "IDB Group private-sector arm financing sustainable tourism and hospitality investments across Latin America and the Caribbean.",
      institutionOverview:
        "IDB Invest is the private sector arm of the Inter-American Development Bank Group. Its Tourism sector page states the institution finances sustainable tourism projects that create jobs, support local communities, and protect natural resources, with experience in hotels, resorts, and tourism infrastructure across Latin America and the Caribbean. Public project announcements include co-financing for Grupo Piñero (Bahia Principe) and Hotel La Compañía in Panama, a Four Points by Sheraton in Guyana, and mezzanine financing for Selina.",
      hotelLendingFocus:
        "Hotels, resorts, sustainable tourism infrastructure, and hospitality-related development in LAC.",
      headquarters: "Washington, D.C., United States",
      website: "https://www.idbinvest.org/",
      primaryRegion: "CALA",
      geographicCoverage: ["Latin America", "Caribbean"],
      preferredMarkets: ["Latin America", "Caribbean", "Mexico", "Dominican Republic", "Panama", "Colombia"],
      typicalDealTypes: [
        "Acquisition Financing",
        "Refinance",
        "Renovation Financing",
        "Redevelopment Financing",
        "Sustainable Tourism Financing",
        "Mezzanine Financing",
      ],
      loanProductsOffered: ["Senior Debt", "Mezzanine Debt", "Equity"],
      preferredAssetTypes: ["Full-Service Hotel", "Resort", "Select-Service Hotel", "Lifestyle Hotel"],
      projectStageAppetite: ["Operating / Stabilized", "Value-Add", "Ground-Up Development"],
      currentLendingAppetite: "Active",
      contactPathwaySelect: "Direct Contact Available",
      processOverview:
        "IDB Invest Tourism sector contact: tourism@idbinvest.org (listed on official sector page). Application pathways described on idbinvest.org.",
      ownerFacingNotes: ownerNotes("Latin America and the Caribbean"),
      sourceType: "Public Source",
      sourceConfidence: "High",
      lastVerifiedDate: LAST_VERIFIED,
      createdBySeedSource: SEED_SOURCE_TAG,
    },
    sources: [
      {
        sourceName: "IDB Invest — Tourism sector",
        sourceType: "Public Source",
        sourceUrl: "https://www.idbinvest.org/en/sectors/tourism",
        sourceSummary:
          "Official sector page describing IDB Invest financing for sustainable tourism projects including hotels and resorts across LAC, with tourism@idbinvest.org contact.",
        relevantFields:
          "Institution Overview, Hotel Lending Focus, Geographic Coverage, Contact Pathway, Loan Products Offered",
        confidence: "High",
      },
      {
        sourceName: "IDB Invest — Grupo Piñero sustainable tourism financing",
        sourceType: "Prior Transaction",
        sourceUrl:
          "https://www.idbinvest.org/en/news-media/idb-invest-and-banco-popular-dominicano-support-sustainable-tourism-caribbean-together",
        sourceSummary:
          "IDB Invest US$120M financing to Grupo Piñero plus US$80M from Banco Popular Dominicano (US$200M total) for sustainable tourism and hotel reopening in the Dominican Republic and Jamaica.",
        relevantFields: "Typical Deal Types, Preferred Asset Types, Preferred Markets",
        confidence: "High",
      },
      {
        sourceName: "IDB Invest — Hotel Los Mandarinos / La Compañía del Valle (Panama)",
        sourceType: "Prior Transaction",
        sourceUrl:
          "https://idbinvest.org/en/news-media/idb-invest-supports-expansion-sustainable-tourism-el-valle-de-anton-panama",
        sourceSummary:
          "IDB Invest US$5M package for Hotel Los Mandarinos / La Compañía del Valle with US$2M mobilized through BAC International Bank; references prior Casco Viejo collaboration.",
        relevantFields: "Typical Deal Types, Preferred Markets",
        confidence: "High",
      },
    ],
    criteria: [
      {
        criteriaName: "IDB Invest — Sustainable tourism project finance",
        loanProduct: "Senior Debt",
        dealTypes: ["Construction", "Renovation / PIP", "Acquisition"],
        appetite: "Active",
        ownerSummary:
          "IDB Invest publicly states it finances sustainable tourism projects including hotels and resorts in Latin America and the Caribbean. Specific terms are transaction-specific.",
        sourceConfidence: "High",
      },
      {
        criteriaName: "IDB Invest — Tourism mezzanine (example: Selina)",
        loanProduct: "Mezzanine Debt",
        dealTypes: ["Construction", "Acquisition"],
        appetite: "Active",
        ownerSummary:
          "IDB Invest has announced mezzanine financing for hospitality platforms (e.g., Selina). Terms are not published as a standard product sheet.",
        sourceConfidence: "High",
      },
    ],
    documents: [],
    contacts: [
      {
        contactName: "IDB Invest Tourism Sector Team",
        title: "Sector contact",
        department: "Tourism",
        email: "tourism@idbinvest.org",
        regionCoverage: "CALA",
        contactRole: "Capital Markets",
        preferredMethod: "Email",
        contactStatus: "Active",
        contactNotes: "Email listed on official IDB Invest Tourism sector page.",
        internalOnly: false,
      },
    ],
    fieldsSkipped: [
      "Minimum Loan Size",
      "Maximum Loan Size",
      "Typical Loan Size Summary",
      "Pricing Guidance",
      "Leverage Guidance",
      "Brand Preference",
      "Operator Preference",
    ],
    warnings: [],
  },

  {
    name: "IFC",
    provider: {
      institutionType: "Multilateral Institution",
      profileStatus: "Active",
      visibilityLevel: "Public",
      shortDescription:
        "World Bank Group member financing and advising on tourism, retail, and property investments globally.",
      institutionOverview:
        "IFC (International Finance Corporation), a member of the World Bank Group, lists Tourism, Retail & Property as a sector expertise area. IFC states it has invested in hotels and tourism projects worldwide and offers long-term financing, equity, and advisory services to support sustainable tourism development.",
      hotelLendingFocus: "Hotels, resorts, and tourism-related property development globally.",
      headquarters: "Washington, D.C., United States",
      website: "https://www.ifc.org/",
      primaryRegion: "Global",
      geographicCoverage: ["Global"],
      preferredMarkets: ["Global"],
      typicalDealTypes: ["Construction Financing", "Acquisition Financing", "Refinance"],
      loanProductsOffered: ["Senior Debt", "Mezzanine Debt", "Equity"],
      preferredAssetTypes: ["Full-Service Hotel", "Resort", "Select-Service Hotel"],
      projectStageAppetite: ["Operating / Stabilized", "Value-Add", "Ground-Up Development"],
      currentLendingAppetite: "Active",
      contactPathwaySelect: "Direct Contact Available",
      requiredInformationSummary:
        "IFC's public application guidance references an investment proposal and feasibility study among materials for project review.",
      processOverview:
        "IFC reviews investment proposals for development impact and viability; public guidance describes submitting an investment proposal and feasibility study.",
      ownerFacingNotes: ownerNotes("IFC member countries globally"),
      sourceType: "Public Source",
      sourceConfidence: "High",
      lastVerifiedDate: LAST_VERIFIED,
      createdBySeedSource: SEED_SOURCE_TAG,
    },
    sources: [
      {
        sourceName: "IFC — Tourism, Retail & Property",
        sourceType: "Public Source",
        sourceUrl: "https://www.ifc.org/en/what-we-do/sector-expertise/tourism-retail-property",
        sourceSummary:
          "Official sector page: IFC invests in hotels and tourism projects worldwide; offers long-term financing, equity, and advisory.",
        relevantFields:
          "Institution Overview, Hotel Lending Focus, Loan Products Offered, Geographic Coverage",
        confidence: "High",
      },
      {
        sourceName: "IFC — How to apply for financing",
        sourceType: "Public Source",
        sourceUrl: "https://www.ifc.org/en/what-we-do/how-to-apply-for-financing",
        sourceSummary:
          "Official guidance on applying for IFC financing, including investment proposal and feasibility study requirements.",
        relevantFields: "Process Overview, Required Information Summary",
        confidence: "High",
      },
      {
        sourceName: "IFC — Banco Sabadell Mexico green tourism line",
        sourceType: "Prior Transaction",
        sourceUrl:
          "https://pressroom.ifc.org/all/pages/PressDetail.aspx?ID=16651",
        sourceDate: "2019-12-11",
        sourceSummary:
          "IFC press release on US$100M loan to Banco Sabadell Mexico for on-lending to green buildings and sustainable tourism (hotels) in Mexico.",
        relevantFields: "Preferred Markets (Mexico example)",
        confidence: "High",
      },
    ],
    criteria: [
      {
        criteriaName: "IFC — Tourism & hospitality project finance",
        loanProduct: "Senior Debt",
        dealTypes: ["Construction", "Acquisition"],
        appetite: "Active",
        ownerSummary:
          "IFC publicly finances hotels and tourism projects in emerging markets. Terms are transaction-specific and subject to IFC due diligence.",
        sourceConfidence: "High",
      },
    ],
    documents: [
      {
        documentRequirementName: "IFC — Investment proposal",
        documentName: "Investment proposal",
        category: "Financing Request",
        requiredLevel: "Required",
        dealTypes: ["Construction", "Acquisition", "Refinance"],
        description: "IFC public application guidance references an investment proposal.",
        ownerInstructions:
          "Prepare per IFC's public 'How to apply for financing' guidance before contacting IFC.",
        visibility: "Owner Visible",
        sortOrder: 1,
      },
      {
        documentRequirementName: "IFC — Feasibility study",
        documentName: "Feasibility study",
        category: "Development / Construction",
        requiredLevel: "Required",
        dealTypes: ["Construction", "Acquisition"],
        description: "IFC public application guidance references a feasibility study.",
        ownerInstructions: "Include development or acquisition feasibility per IFC public guidance.",
        visibility: "Owner Visible",
        sortOrder: 2,
      },
    ],
    contacts: [],
    fieldsSkipped: [
      "Minimum Loan Size",
      "Maximum Loan Size",
      "Typical Loan Size Summary",
      "Brand Preference",
      "Operator Preference",
    ],
    warnings: [],
  },

  {
    name: "Banco Popular Dominicano",
    provider: {
      institutionType: "Regional Bank",
      profileStatus: "Active",
      visibilityLevel: "Limited",
      shortDescription:
        "Leading Dominican commercial bank with documented tourism and hospitality lending activity.",
      institutionOverview:
        "Banco Popular Dominicano is a major Dominican Republic bank. IDB Invest announced Banco Popular as co-lender in a US$80M financing package for Grupo Piñero sustainable tourism investments. Public reporting quotes Banco Popular leadership describing the bank as a leading lender to the tourism sector in the Dominican Republic.",
      hotelLendingFocus:
        "Tourism and hospitality projects in the Dominican Republic; sustainable tourism co-lending with development finance partners.",
      headquarters: "Santo Domingo, Dominican Republic",
      website: "https://www.popularenlinea.com/",
      primaryRegion: "Dominican Republic",
      geographicCoverage: ["Dominican Republic"],
      preferredMarkets: ["Dominican Republic"],
      typicalDealTypes: ["Renovation Financing", "Refinance", "Redevelopment Financing"],
      loanProductsOffered: ["Senior Debt"],
      preferredAssetTypes: ["Resort", "Full-Service Hotel"],
      projectStageAppetite: ["Operating / Stabilized", "Value-Add", "Ground-Up Development"],
      currentLendingAppetite: "Unknown",
      contactPathwaySelect: "Direct Contact Available",
      ownerFacingNotes: ownerNotes("the Dominican Republic"),
      sourceType: "Prior Transaction",
      sourceConfidence: "High",
      lastVerifiedDate: LAST_VERIFIED,
      createdBySeedSource: SEED_SOURCE_TAG,
    },
    sources: [
      {
        sourceName: "IDB Invest — Grupo Piñero financing (Banco Popular co-lender)",
        sourceType: "Prior Transaction",
        sourceUrl:
          "https://www.idbinvest.org/en/news-media/idb-invest-and-banco-popular-dominicano-support-sustainable-tourism-caribbean-together",
        sourceSummary:
          "IDB Invest press release names Banco Popular Dominicano as US$80M co-lender in US$200M Grupo Piñero tourism package.",
        relevantFields:
          "Institution Overview, Hotel Lending Focus, Loan Products Offered, Typical Deal Types",
        confidence: "High",
      },
      {
        sourceName: "IDB Invest — Back to Business on the Beach (Grupo Piñero)",
        sourceType: "Public Source",
        sourceUrl: "https://idbinvest.org/en/back-business-beach-0",
        sourceSummary:
          "IDB Invest feature on Grupo Piñero alliance with Banco Popular Dominicano and tourism sector recovery investments.",
        relevantFields: "Hotel Lending Focus, Institution Overview",
        confidence: "High",
      },
    ],
    criteria: [
      {
        criteriaName: "Banco Popular — Tourism sector lending (public transaction example)",
        loanProduct: "Senior Debt",
        dealTypes: ["Renovation / PIP", "Construction"],
        appetite: "Unknown",
        ownerSummary:
          "Public announcement shows co-lending for large-scale sustainable tourism (Grupo Piñero). Standard product terms are not published.",
        sourceConfidence: "High",
      },
    ],
    documents: [],
    contacts: [],
    fieldsSkipped: [
      "Minimum Loan Size",
      "Maximum Loan Size",
      "Contact records (no public department email found)",
    ],
    warnings: [
      "Current lending appetite not stated in official bank product pages reviewed; marked Unknown.",
    ],
  },

  {
    name: "Banco BHD",
    provider: {
      institutionType: "Regional Bank",
      profileStatus: "Active",
      visibilityLevel: "Limited",
      shortDescription:
        "Dominican bank with public tourism financing solutions and documented large hotel transactions.",
      institutionOverview:
        "Banco BHD publishes a tourism sector solutions page (open.bhd.com.do) for hospitality clients. Public reporting documents BHD financing of the Hyatt Vivid Grand Island (Jewel Palm Beach) project in Punta Cana at US$78.8M, recognized by Asonahores as a major tourism-sector transaction.",
      hotelLendingFocus:
        "Hotels and tourism projects in the Dominican Republic; dedicated tourism banking solutions.",
      headquarters: "Santo Domingo, Dominican Republic",
      website: "https://www.bhd.com.do/",
      primaryRegion: "Dominican Republic",
      geographicCoverage: ["Dominican Republic"],
      preferredMarkets: ["Dominican Republic"],
      typicalDealTypes: ["Construction Financing", "Acquisition Financing", "Renovation Financing"],
      loanProductsOffered: ["Senior Debt"],
      preferredAssetTypes: ["Resort", "Full-Service Hotel"],
      projectStageAppetite: ["Ground-Up Development", "Value-Add"],
      currentLendingAppetite: "Unknown",
      contactPathwaySelect: "Direct Contact Available",
      processOverview:
        "BHD tourism solutions portal: https://open.bhd.com.do/soluciones/turismo",
      requiredInformationSummary:
        "BHD's tourism solutions portal indicates hospitality clients should prepare project overview, financial information, and development details for tourism-sector financing review.",
      ownerFacingNotes: ownerNotes("the Dominican Republic"),
      sourceType: "Public Source",
      sourceConfidence: "High",
      lastVerifiedDate: LAST_VERIFIED,
      createdBySeedSource: SEED_SOURCE_TAG,
    },
    sources: [
      {
        sourceName: "BHD — Soluciones Turismo",
        sourceType: "Lender Confirmed",
        sourceUrl: "https://open.bhd.com.do/soluciones/turismo",
        sourceSummary:
          "Official BHD open banking portal page for tourism sector solutions.",
        relevantFields: "Hotel Lending Focus, Contact Pathway",
        confidence: "High",
      },
      {
        sourceName: "Listín Diario — Hyatt Vivid Palm Beach US$78.8M financing",
        sourceType: "Prior Transaction",
        sourceUrl:
          "https://listindiario.com/economia/2024/02/01/bhd-financia-us78-millones-proyecto-hotelero-punta-cana-849142",
        sourceDate: "2024-02-01",
        sourceSummary:
          "News report on BHD financing US$78.8M for Hyatt Vivid Grand Island (Jewel Palm Beach) in Punta Cana.",
        relevantFields: "Typical Deal Types, Preferred Asset Types",
        confidence: "Medium",
      },
    ],
    criteria: [
      {
        criteriaName: "BHD — Tourism / hotel project finance (transaction example)",
        loanProduct: "Senior Debt",
        dealTypes: ["Construction"],
        maxLoan: 78800000,
        currency: "USD",
        appetite: "Unknown",
        ownerSummary:
          "Public transaction example: US$78.8M financing for Hyatt Vivid Grand Island (2024 reporting). Not a published standard maximum.",
        sourceConfidence: "Medium",
      },
    ],
    documents: [],
    contacts: [],
    financings: [
      {
        financingName: "BHD — Hyatt Vivid Grand Island",
        projectName: "Hyatt Vivid Grand Island (Jewel Palm Beach)",
        location: "Punta Cana, Dominican Republic",
        dealType: "Construction",
        loanAmountLabel: "US$78.8M",
        loanAmountUsd: 78800000,
        transactionYear: "2024",
        ownerSummary:
          "Public reporting documents BHD financing of US$78.8M for the Hyatt Vivid Grand Island resort development in Punta Cana.",
        sourceName: "Listín Diario — Hyatt Vivid Palm Beach financing",
        sourceUrl:
          "https://listindiario.com/economia/2024/02/01/bhd-financia-us78-millones-proyecto-hotelero-punta-cana-849142",
        sortOrder: 1,
      },
    ],
    fieldsSkipped: ["Minimum Loan Size", "Typical Loan Size Summary (no published range)"],
    warnings: [
      "US$78.8M reflects one reported transaction, not a published lending limit.",
    ],
  },

  {
    name: "Banreservas",
    provider: {
      institutionType: "Bank",
      profileStatus: "Active",
      visibilityLevel: "Public",
      shortDescription:
        "Dominican government-linked bank with public tourism-sector financing programs and FITUR participation.",
      institutionOverview:
        "Banco de Reservas (Banreservas) publicly promotes tourism-sector financing and investment support. Official communications reference financing for hotel and tourism projects, participation at FITUR, and fiduciary tourism trust structures via Fiduciaria Reservas.",
      hotelLendingFocus:
        "Hotel and tourism project financing in the Dominican Republic; tourism investment programs.",
      headquarters: "Santo Domingo, Dominican Republic",
      website: "https://www.banreservas.com/",
      primaryRegion: "Dominican Republic",
      geographicCoverage: ["Dominican Republic"],
      preferredMarkets: ["Dominican Republic"],
      typicalDealTypes: ["Construction Financing", "Refinance", "Renovation Financing"],
      loanProductsOffered: ["Senior Debt"],
      preferredAssetTypes: ["Resort", "Full-Service Hotel", "Select-Service Hotel"],
      projectStageAppetite: ["Operating / Stabilized", "Value-Add", "Ground-Up Development"],
      currentLendingAppetite: "Active",
      contactPathwaySelect: "Direct Contact Available",
      processOverview:
        "Official Banreservas website and Fiduciaria Reservas tourism trust page for structured tourism investments.",
      ownerFacingNotes: ownerNotes("the Dominican Republic"),
      sourceType: "Public Source",
      sourceConfidence: "High",
      lastVerifiedDate: LAST_VERIFIED,
      createdBySeedSource: SEED_SOURCE_TAG,
    },
    sources: [
      {
        sourceName: "Banreservas — FITUR 2025 tourism investment announcement",
        sourceType: "Lender Confirmed",
        sourceUrl:
          "https://www.banreservas.com/articulos/banreservas-anuncia-mas-de-us-4-000-millones-gestionados-en-fitur-para-traer-inversiones-a-rd/",
        sourceSummary:
          "Official Banreservas article on tourism investment capture at FITUR 2025, including hotel and tourism infrastructure financing across Dominican regions.",
        relevantFields: "Hotel Lending Focus, Institution Overview, Current Lending Appetite",
        confidence: "High",
      },
      {
        sourceName: "Banreservas — Spanish tourism investments financing",
        sourceType: "Lender Confirmed",
        sourceUrl:
          "https://www.banreservas.com/noticiasyprensa/banreservas-financia-inversiones-espanolas-en-turismo-por-mas-de-us-175-millones-y-aportara-9-500-habitaciones-adicionales/",
        sourceSummary:
          "Official press release: Banreservas disbursed US$175M+ for Spanish tourism projects; tourism credit portfolio exceeds RD$61,000 million.",
        relevantFields: "Typical Deal Types, Loan Products Offered",
        confidence: "High",
      },
      {
        sourceName: "Fiduciaria Reservas — Fideicomiso turístico",
        sourceType: "Lender Confirmed",
        sourceUrl: "https://www.fiduciariareservas.com/fideicomiso-turistico",
        sourceSummary:
          "Official fiduciary tourism trust structure for tourism investments in the Dominican Republic.",
        relevantFields: "Typical Deal Types, Process Overview",
        confidence: "High",
      },
    ],
    criteria: [
      {
        criteriaName: "Banreservas — Tourism sector credit",
        loanProduct: "Senior Debt",
        dealTypes: ["Construction", "Renovation / PIP", "Refinance"],
        appetite: "Active",
        ownerSummary:
          "Banreservas publicly offers tourism-sector financing in the Dominican Republic. Specific terms require bank review.",
        sourceConfidence: "High",
      },
    ],
    documents: [],
    contacts: [],
    fieldsSkipped: ["Minimum Loan Size", "Maximum Loan Size"],
    warnings: [],
  },

  {
    name: "BAC International Bank / BAC Credomatic",
    provider: {
      institutionType: "Regional Bank",
      profileStatus: "Active",
      visibilityLevel: "Limited",
      shortDescription:
        "Panama-based regional bank with IDB Invest-documented hospitality financing in Central America.",
      institutionOverview:
        "BAC International Bank (BAC Credomatic network) is described in IDB Invest press releases as a leading regional bank. IDB Invest mobilized financing alongside BAC for Hotel La Compañía in Panama and Hotel Los Mandarinos, demonstrating hospitality project finance activity in Central America.",
      hotelLendingFocus:
        "Hospitality and tourism projects in Central America and Panama; heritage hotel restorations.",
      headquarters: "Panama City, Panama",
      website: "https://www.baccredomatic.com/",
      primaryRegion: "Central America",
      geographicCoverage: ["Central America", "Panama"],
      preferredMarkets: ["Panama", "Central America"],
      typicalDealTypes: ["Renovation Financing", "Construction Financing", "Acquisition Financing"],
      loanProductsOffered: ["Senior Debt"],
      preferredAssetTypes: ["Lifestyle Hotel", "Full-Service Hotel"],
      projectStageAppetite: ["Value-Add", "Ground-Up Development"],
      currentLendingAppetite: "Unknown",
      contactPathwaySelect: "Direct Contact Available",
      processOverview: "Official BAC Credomatic corporate site and local business banking offices.",
      requiredInformationSummary:
        "Public IDB Invest co-lending announcements reference renovation and hospitality projects. Owners should prepare project summary, sponsor background, sources & uses, and financial projections before approaching BAC business banking offices.",
      ownerFacingNotes: ownerNotes("Central America and Panama"),
      sourceType: "Prior Transaction",
      sourceConfidence: "High",
      lastVerifiedDate: LAST_VERIFIED,
      createdBySeedSource: SEED_SOURCE_TAG,
    },
    sources: [
      {
        sourceName: "IDB Invest — Hotel Los Mandarinos / La Compañía del Valle (BAC co-lender)",
        sourceType: "Prior Transaction",
        sourceUrl:
          "https://idbinvest.org/en/news-media/idb-invest-supports-expansion-sustainable-tourism-el-valle-de-anton-panama",
        sourceSummary:
          "IDB Invest US$5M package with US$2M mobilized through BAC International Bank for Hotel Los Mandarinos renovation; references prior Hotel La Compañía Casco Viejo collaboration.",
        relevantFields:
          "Institution Overview, Hotel Lending Focus, Geographic Coverage, Typical Deal Types, Preferred Asset Types",
        confidence: "High",
      },
    ],
    criteria: [
      {
        criteriaName: "BAC — Hospitality project finance (public examples)",
        loanProduct: "Senior Debt",
        dealTypes: ["Renovation / PIP", "Construction"],
        appetite: "Unknown",
        ownerSummary:
          "Public IDB Invest announcements show BAC co-lending on Panama hotel projects. Standard criteria not published.",
        sourceConfidence: "High",
      },
    ],
    documents: [],
    contacts: [],
    financings: [
      {
        financingName: "BAC — Hotel Los Mandarinos",
        projectName: "Hotel Los Mandarinos",
        location: "El Valle de Antón, Panama",
        dealType: "Renovation / PIP",
        loanAmountLabel: "US$2M mobilized through BAC (IDB Invest US$5M package)",
        transactionYear: "2024",
        ownerSummary:
          "IDB Invest announced US$5M financing for Hotel Los Mandarinos renovation with US$2M mobilized through BAC International Bank.",
        sourceName: "IDB Invest — Hotel Los Mandarinos / La Compañía del Valle",
        sourceUrl:
          "https://idbinvest.org/en/news-media/idb-invest-supports-expansion-sustainable-tourism-el-valle-de-anton-panama",
        sortOrder: 1,
      },
      {
        financingName: "BAC — Hotel La Compañía",
        projectName: "Hotel La Compañía",
        location: "Casco Viejo, Panama City, Panama",
        dealType: "Renovation / PIP",
        loanAmountLabel: "Not publicly disclosed",
        transactionYear: "Referenced in 2024 IDB release",
        ownerSummary:
          "IDB Invest press release references prior BAC collaboration on Hotel La Compañía heritage hotel restoration in Casco Viejo.",
        sourceName: "IDB Invest — Hotel Los Mandarinos / La Compañía del Valle",
        sourceUrl:
          "https://idbinvest.org/en/news-media/idb-invest-supports-expansion-sustainable-tourism-el-valle-de-anton-panama",
        sortOrder: 2,
      },
    ],
    fieldsSkipped: ["Minimum Loan Size", "Maximum Loan Size", "Public contact email"],
    warnings: [],
  },

  {
    name: "Scotiabank Caribbean",
    provider: {
      institutionType: "Regional Bank",
      profileStatus: "Active",
      visibilityLevel: "Public",
      shortDescription:
        "Caribbean corporate bank unit with a dedicated hospitality financing page for hotels and resorts.",
      institutionOverview:
        "Scotiabank's Global Transaction Banking hospitality page states the bank provides financing for construction, acquisition, and renovation of hotels and resorts, alongside treasury and trade solutions for the hospitality sector in the Caribbean and Central America.",
      hotelLendingFocus:
        "Construction, acquisition, and renovation financing for hotels and resorts.",
      headquarters: "Toronto, Canada (Scotiabank); Caribbean operations across region",
      website: "https://www.scotiabank.com/",
      primaryRegion: "Caribbean",
      geographicCoverage: ["Caribbean", "Central America"],
      preferredMarkets: ["Caribbean", "Central America"],
      typicalDealTypes: ["Construction Financing", "Acquisition Financing", "Renovation Financing"],
      loanProductsOffered: ["Senior Debt"],
      preferredAssetTypes: ["Full-Service Hotel", "Resort", "Select-Service Hotel"],
      projectStageAppetite: ["Ground-Up Development", "Value-Add", "Operating / Stabilized"],
      currentLendingAppetite: "Active",
      contactPathwaySelect: "Direct Contact Available",
      processOverview:
        "Scotiabank Global Transaction Banking hospitality page; regional corporate banking offices.",
      ownerFacingNotes: ownerNotes("the Caribbean and Central America"),
      sourceType: "Lender Confirmed",
      sourceConfidence: "High",
      lastVerifiedDate: LAST_VERIFIED,
      createdBySeedSource: SEED_SOURCE_TAG,
    },
    sources: [
      {
        sourceName: "Scotiabank GTB — Hospitality",
        sourceType: "Lender Confirmed",
        sourceUrl:
          "https://gtb.scotiabank.com/global/en/commercial-banking/industry-expertise/hospitality.html",
        sourceSummary:
          "Official Scotiabank page: financing for construction, acquisition, and renovation of hotels and resorts.",
        relevantFields:
          "Hotel Lending Focus, Typical Deal Types, Loan Products Offered, Project Stage Appetite",
        confidence: "High",
      },
    ],
    criteria: [
      {
        criteriaName: "Scotiabank Caribbean — Hospitality financing",
        loanProduct: "Senior Debt",
        dealTypes: ["Construction", "Acquisition", "Renovation / PIP"],
        appetite: "Active",
        ownerSummary:
          "Scotiabank publicly offers construction, acquisition, and renovation financing for hotels and resorts in its regional footprint.",
        sourceConfidence: "High",
      },
    ],
    documents: [],
    contacts: [],
    fieldsSkipped: ["Minimum Loan Size", "Maximum Loan Size"],
    warnings: [],
  },

  {
    name: "CIBC Caribbean / FirstCaribbean",
    provider: {
      institutionType: "Regional Bank",
      profileStatus: "Needs Review",
      visibilityLevel: "Limited",
      shortDescription:
        "Caribbean regional bank offering business loans and credit lines; limited public hotel-specific product detail.",
      institutionOverview:
        "CIBC Caribbean (formerly CIBC FirstCaribbean) provides business banking including business loans, credit lines, cash management, and corporate banking across the Caribbean. Public materials reviewed do not include a dedicated hospitality financing product page comparable to peer banks.",
      hotelLendingFocus:
        "Not explicitly stated on public site; general business and corporate lending may apply to commercial real estate.",
      headquarters: "Bridgetown, Barbados (regional)",
      website: "https://www.cibccaribbean.com/",
      primaryRegion: "Caribbean",
      geographicCoverage: ["Caribbean"],
      preferredMarkets: ["Caribbean"],
      typicalDealTypes: ["Refinance", "Renovation Financing"],
      loanProductsOffered: ["Senior Debt"],
      preferredAssetTypes: [],
      projectStageAppetite: ["Operating / Stabilized", "Value-Add"],
      currentLendingAppetite: "Unknown",
      contactPathwaySelect: "Unknown",
      processOverview:
        "CIBC Caribbean business banking and local branch relationship managers.",
      ownerFacingNotes: ownerNotes("the Caribbean"),
      sourceType: "Dealality Research",
      sourceConfidence: "Needs Verification",
      lastVerifiedDate: LAST_VERIFIED,
      createdBySeedSource: SEED_SOURCE_TAG,
    },
    sources: [
      {
        sourceName: "CIBC Caribbean — Business Banking",
        sourceType: "Lender Confirmed",
        sourceUrl: "https://www.cibccaribbean.com/business-banking",
        sourceSummary:
          "Official page: business loans, credit lines, cash management for SMEs and businesses.",
        relevantFields: "Institution Overview, Loan Products Offered",
        confidence: "High",
      },
      {
        sourceName: "CIBC Caribbean — Corporate Banking",
        sourceType: "Lender Confirmed",
        sourceUrl: "https://www.cibccaribbean.com/corporate-banking",
        sourceSummary:
          "Official corporate banking services across the Caribbean.",
        relevantFields: "Geographic Coverage, Contact Pathway",
        confidence: "High",
      },
    ],
    criteria: [],
    documents: [],
    contacts: [],
    fieldsSkipped: [
      "Hotel-specific criteria (no public hospitality product page)",
      "Minimum Loan Size",
      "Maximum Loan Size",
      "Preferred Asset Types (not stated)",
    ],
    warnings: [
      "No dedicated public hospitality financing page found; profile marked Needs Review.",
      "Hotel lending focus inferred only from general business lending — not hotel-specific.",
    ],
  },

  {
    name: "Republic Bank",
    provider: {
      institutionType: "Regional Bank",
      profileStatus: "Active",
      visibilityLevel: "Public",
      shortDescription:
        "Caribbean bank with published corporate hospitality financing capabilities.",
      institutionOverview:
        "Republic Bank publishes corporate and commercial hospitality pages describing financing and banking solutions for the hospitality sector, including hotels, across its Caribbean footprint (e.g., BVI, Dominica regional sites).",
      hotelLendingFocus: "Hotels and hospitality businesses; corporate and commercial hospitality banking.",
      headquarters: "Port of Spain, Trinidad and Tobago",
      website: "https://www.republictt.com/",
      primaryRegion: "Caribbean",
      geographicCoverage: ["Caribbean"],
      preferredMarkets: ["Caribbean"],
      typicalDealTypes: ["Acquisition Financing", "Renovation Financing", "Refinance"],
      loanProductsOffered: ["Senior Debt"],
      preferredAssetTypes: ["Full-Service Hotel", "Resort"],
      projectStageAppetite: ["Operating / Stabilized", "Value-Add"],
      currentLendingAppetite: "Active",
      contactPathwaySelect: "Direct Contact Available",
      processOverview:
        "Republic Bank corporate & commercial hospitality pages; regional offices.",
      ownerFacingNotes: ownerNotes("the Caribbean"),
      sourceType: "Lender Confirmed",
      sourceConfidence: "High",
      lastVerifiedDate: LAST_VERIFIED,
      createdBySeedSource: SEED_SOURCE_TAG,
    },
    sources: [
      {
        sourceName: "Republic Bank BVI — Hospitality",
        sourceType: "Lender Confirmed",
        sourceUrl:
          "https://www.republicbankbvi.com/corporate-and-commercial/hospitality",
        sourceSummary:
          "Official Republic Bank BVI hospitality corporate banking page.",
        relevantFields: "Hotel Lending Focus, Typical Deal Types",
        confidence: "High",
      },
      {
        sourceName: "Republic Bank Dominica — Hospitality",
        sourceType: "Lender Confirmed",
        sourceUrl:
          "https://www.republicbankdm.com/corporate-and-commercial/hospitality",
        sourceSummary:
          "Official Republic Bank Dominica hospitality corporate banking page.",
        relevantFields: "Geographic Coverage, Loan Products Offered",
        confidence: "High",
      },
    ],
    criteria: [
      {
        criteriaName: "Republic Bank — Corporate hospitality finance",
        loanProduct: "Senior Debt",
        dealTypes: ["Acquisition", "Renovation / PIP", "Refinance"],
        appetite: "Active",
        ownerSummary:
          "Republic Bank publicly markets corporate hospitality financing in Caribbean markets.",
        sourceConfidence: "High",
      },
    ],
    documents: [],
    contacts: [],
    fieldsSkipped: ["Minimum Loan Size", "Maximum Loan Size"],
    warnings: [],
  },

  {
    name: "BBVA México",
    provider: {
      institutionType: "National Bank",
      profileStatus: "Active",
      visibilityLevel: "Limited",
      shortDescription:
        "Major Mexican bank with documented hotel and tourism sustainable finance transactions.",
      institutionOverview:
        "BBVA México (BBVA CIB) has announced multiple hotel and tourism-related financings, including sustainable linked loans for Palace Resorts, The Cape Thompson Hotel (Park Hyatt Los Cabos), and Valentín hotels. BBVA positions these as hospitality/tourism sector lending in Mexico.",
      hotelLendingFocus:
        "Hotels and tourism resorts in Mexico; sustainable finance structures for hospitality.",
      headquarters: "Mexico City, Mexico",
      website: "https://www.bbva.mx/",
      primaryRegion: "Mexico",
      geographicCoverage: ["Mexico"],
      preferredMarkets: ["Mexico"],
      typicalDealTypes: ["Construction Financing", "Refinance", "Renovation Financing"],
      loanProductsOffered: ["Senior Debt", "Sustainability-Linked Loan", "Green Loan"],
      preferredAssetTypes: ["Resort", "Full-Service Hotel"],
      projectStageAppetite: ["Operating / Stabilized", "Value-Add", "Ground-Up Development"],
      currentLendingAppetite: "Unknown",
      contactPathwaySelect: "Unknown",
      processOverview:
        "BBVA México corporate and investment banking; bbvacib.com deal announcements.",
      ownerFacingNotes: ownerNotes("Mexico"),
      sourceType: "Prior Transaction",
      sourceConfidence: "High",
      lastVerifiedDate: LAST_VERIFIED,
      createdBySeedSource: SEED_SOURCE_TAG,
    },
    sources: [
      {
        sourceName: "BBVA CIB — Palace Resorts sustainable linked loan",
        sourceType: "Prior Transaction",
        sourceUrl:
          "https://www.bbvacib.com/insights/news/bbva-mexico-structures-sustainable-linked-loan-for-palace-resorts/",
        sourceSummary:
          "BBVA México structured a sustainable linked loan for Palace Resorts hospitality portfolio.",
        relevantFields: "Hotel Lending Focus, Preferred Asset Types, Loan Products Offered",
        confidence: "High",
      },
      {
        sourceName: "BBVA CIB — The Cape Thompson Hotel (Park Hyatt Los Cabos)",
        sourceType: "Prior Transaction",
        sourceUrl:
          "https://www.bbvacib.com/insights/news/bbva-mexico-finances-the-cape-thompson-hotel/",
        sourceSummary:
          "BBVA México financing for The Cape Thompson Hotel / Park Hyatt Los Cabos.",
        relevantFields: "Typical Deal Types, Preferred Markets",
        confidence: "High",
      },
    ],
    criteria: [
      {
        criteriaName: "BBVA México — Hospitality / sustainable linked loans",
        loanProduct: "Senior Debt",
        dealTypes: ["Construction", "Refinance", "Renovation / PIP"],
        appetite: "Unknown",
        ownerSummary:
          "BBVA has publicly announced hotel financings including sustainable linked loan structures. Standard criteria not published.",
        sourceConfidence: "High",
      },
    ],
    documents: [],
    contacts: [],
    fieldsSkipped: ["Minimum Loan Size", "Maximum Loan Size"],
    warnings: ["Transaction-based profile; current appetite not publicly stated."],
  },

  {
    name: "Bancomext",
    provider: {
      institutionType: "Export Credit / Government Finance",
      profileStatus: "Active",
      visibilityLevel: "Public",
      shortDescription:
        "Mexican federal development bank with tourism-sector financing programs for hotels and tourism infrastructure.",
      institutionOverview:
        "Bancomext (Banco Nacional de Comercio Exterior) is Mexico's federal export development bank. Public materials describe tourism-sector support including financing for hotels, PyMEX Turismo programs, and the 'Mejora tu Hotel' initiative for hotel improvement projects.",
      hotelLendingFocus:
        "Hotels, tourism infrastructure, and SME tourism businesses in Mexico.",
      headquarters: "Mexico City, Mexico",
      website: "https://www.bancomext.com/",
      primaryRegion: "Mexico",
      geographicCoverage: ["Mexico"],
      preferredMarkets: ["Mexico"],
      typicalDealTypes: ["Construction Financing", "Renovation Financing", "Refinance"],
      loanProductsOffered: ["Senior Debt"],
      preferredAssetTypes: ["Full-Service Hotel", "Resort", "Select-Service Hotel"],
      projectStageAppetite: ["Value-Add", "Operating / Stabilized"],
      currentLendingAppetite: "Active",
      contactPathwaySelect: "Direct Contact Available",
      processOverview:
        "Bancomext official website; PyMEX Turismo and Mejora tu Hotel program pages.",
      processOverview:
        "Bancomext publishes FAQs and program pages describing application steps for tourism and SME programs.",
      ownerFacingNotes: ownerNotes("Mexico"),
      sourceType: "Lender Confirmed",
      sourceConfidence: "High",
      lastVerifiedDate: LAST_VERIFIED,
      createdBySeedSource: SEED_SOURCE_TAG,
    },
    sources: [
      {
        sourceName: "Bancomext — Preguntas frecuentes",
        sourceType: "Lender Confirmed",
        sourceUrl: "https://www.bancomext.com/soporte/preguntas-frecuentes/",
        sourceSummary:
          "Official Bancomext FAQ on products, eligibility, and application process.",
        relevantFields: "Process Overview, Contact Pathway",
        confidence: "High",
      },
      {
        sourceName: "Bancomext — PyMEX Turismo",
        sourceType: "Lender Confirmed",
        sourceUrl: "https://www.bancomext.com/pymes/pymex-turismo/",
        sourceSummary:
          "Official PyMEX Turismo program for tourism SMEs including hotels.",
        relevantFields: "Hotel Lending Focus, Loan Products Offered, Typical Deal Types",
        confidence: "High",
      },
      {
        sourceName: "Bancomext — Mejora tu Hotel",
        sourceType: "Lender Confirmed",
        sourceUrl: "https://www.bancomext.com/programas/mejora-tu-hotel/",
        sourceSummary:
          "Official hotel improvement financing program.",
        relevantFields: "Typical Deal Types, Project Stage Appetite",
        confidence: "High",
      },
    ],
    criteria: [
      {
        criteriaName: "Bancomext — PyMEX Turismo",
        loanProduct: "Senior Debt",
        dealTypes: ["Renovation / PIP", "Refinance"],
        appetite: "Active",
        ownerSummary:
          "Bancomext PyMEX Turismo program supports tourism SMEs per official program page.",
        sourceConfidence: "High",
      },
      {
        criteriaName: "Bancomext — Mejora tu Hotel",
        loanProduct: "Senior Debt",
        dealTypes: ["Renovation / PIP"],
        appetite: "Active",
        ownerSummary:
          "Bancomext Mejora tu Hotel program for hotel improvement projects per official page.",
        sourceConfidence: "High",
      },
    ],
    documents: [],
    contacts: [],
    fieldsSkipped: ["Minimum Loan Size", "Maximum Loan Size (program-specific; not seeded without explicit public amounts)"],
    warnings: [],
  },

  {
    name: "Banco Sabadell México",
    provider: {
      institutionType: "Bank",
      profileStatus: "Active",
      visibilityLevel: "Limited",
      shortDescription:
        "Mexican bank with IFC-backed green and sustainable tourism lending line for hotels.",
      institutionOverview:
        "Banco Sabadell México received a US$100M loan from IFC to expand financing for green buildings and sustainable tourism, including hotels in destinations such as Riviera Maya and Jalisco, per IFC press release.",
      hotelLendingFocus:
        "Sustainable tourism and green building financing including hotels in Mexico.",
      headquarters: "Mexico City, Mexico",
      website: "https://www.bancosabadell.mx/",
      primaryRegion: "Mexico",
      geographicCoverage: ["Mexico"],
      preferredMarkets: ["Mexico"],
      typicalDealTypes: ["Construction Financing", "Renovation Financing", "Refinance"],
      loanProductsOffered: ["Senior Debt", "Green Loan"],
      preferredAssetTypes: ["Resort", "Full-Service Hotel"],
      projectStageAppetite: ["Value-Add", "Ground-Up Development"],
      currentLendingAppetite: "Unknown",
      contactPathwaySelect: "Direct Contact Available",
      processOverview: "Banco Sabadell México official website and corporate banking.",
      ownerFacingNotes: ownerNotes("Mexico"),
      sourceType: "Prior Transaction",
      sourceConfidence: "High",
      lastVerifiedDate: LAST_VERIFIED,
      createdBySeedSource: SEED_SOURCE_TAG,
    },
    sources: [
      {
        sourceName: "IFC — US$100M loan to Banco Sabadell Mexico",
        sourceType: "Prior Transaction",
        sourceUrl:
          "https://pressroom.ifc.org/all/pages/PressDetail.aspx?ID=16651",
        sourceDate: "2019-12-11",
        sourceSummary:
          "IFC US$100M loan for on-lending to green buildings and sustainable tourism hotels in Mexico.",
        relevantFields:
          "Institution Overview, Hotel Lending Focus, Preferred Markets, Loan Products Offered",
        confidence: "High",
      },
      {
        sourceName: "Banco Sabadell México — Official site",
        sourceType: "Lender Confirmed",
        sourceUrl: "https://www.bancosabadell.mx/",
        sourceSummary: "Official bank website for corporate and commercial banking.",
        relevantFields: "Website, Contact Pathway",
        confidence: "High",
      },
    ],
    criteria: [
      {
        criteriaName: "Sabadell México — Sustainable tourism on-lending (IFC line)",
        loanProduct: "Senior Debt",
        dealTypes: ["Construction", "Renovation / PIP"],
        appetite: "Unknown",
        ownerSummary:
          "IFC facility targets sustainable tourism including hotels; individual loan terms set by Sabadell México.",
        sourceConfidence: "High",
      },
    ],
    documents: [],
    contacts: [],
    fieldsSkipped: ["Minimum Loan Size", "Maximum Loan Size"],
    warnings: [
      "IFC line announcement dated 2019; current program status not verified on bank site.",
    ],
  },
];

export function getPublicSeedProviderNames() {
  return PUBLIC_SEED_PROVIDERS.map((p) => p.name);
}
