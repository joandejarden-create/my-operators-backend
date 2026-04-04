/**
 * Operator Explorer API
 * List: delegates to GET /api/third-party-operators (Operator Setup — … tables).
 * Detail fallback below still uses mock rows when no Airtable match is available.
 */

import listThirdPartyOperators from "./third-party-operators-list.js";
import getThirdPartyOperatorDetail from "./third-party-operator-detail.js";

// Mock operator data – used only for GET /api/operator-explorer/operator fallback
const MOCK_OPERATORS = [
  {
    id: "op-1",
    operator_name: "Crestwood Hospitality",
    logo_url: "",
    company_type: "3rd Party Operator",
    parent_company: "Crestwood Holdings",
    hq_city: "Atlanta",
    hq_country: "USA",
    website: "https://www.crestwoodhospitality.com",
    member_since: "2022",
    overview_short: "Full-service management for branded and independent hotels across upper-upscale and luxury segments.",
    overview_long: "Crestwood Hospitality manages hotels for institutional and family office owners. Strong experience in pre-opening, conversion, and turnaround situations. Focus on revenue optimization, brand compliance, and owner communication.",
    hotels_managed_count: 42,
    rooms_managed_count: 8400,
    year_founded: 1998,
    years_in_operation: 26,
    employee_count_band: "500-1000",
    regions_served: 8,
    countries_served: 2,
    top_markets: ["Southeast USA", "Midwest USA"],
    asset_classes: ["Full-Service", "Resort", "Lifestyle"],
    hotel_types: ["Urban", "Suburban", "Resort", "Lifestyle"],
    chain_scales: ["Luxury", "Upper Upscale", "Upscale"],
    branded_experience: true,
    independent_experience: true,
    operating_situations: ["Pre-Opening", "Conversion", "Transition", "Turnaround", "Stabilized"],
    service_models: ["Third-Party Management"],
    primary_services: ["Hotel Management", "Revenue Management", "Sales & Marketing"],
    additional_services: ["Pre-Opening", "Conversion Support", "Above-Property"],
    best_fit_for: ["Institutional owners", "Family offices seeking hands-off management", "Upper-upscale and luxury assets"],
    less_proven_areas: ["All-inclusive resorts", "Economy segment"],
    ownership_fit: ["Institutional", "Family Office", "Multi-Property Owner"],
    market_fit: ["Secondary and tertiary US markets", "Resort destinations"],
    capability_tags: ["Luxury", "Resort", "Branded", "Independent", "Pre-Opening", "Turnaround"],
    brands_managed: ["Marriott", "Hilton", "Hyatt", "IHG", "Independent"],
    geography: ["Southeast USA", "Midwest USA", "Texas"],
  },
  {
    id: "op-2",
    operator_name: "Pacific Shore Management",
    logo_url: "",
    company_type: "3rd Party Operator",
    parent_company: "",
    hq_city: "San Diego",
    hq_country: "USA",
    website: "https://www.pacificshore.com",
    member_since: "2021",
    overview_short: "West Coast specialist for select-service and extended-stay properties. Strong conversion and transition experience.",
    overview_long: "Pacific Shore Management focuses on select-service and extended-stay hotels in California, Arizona, and Nevada. Expertise in conversion projects and brand transitions. Above-property revenue management and sales support.",
    hotels_managed_count: 28,
    rooms_managed_count: 3600,
    year_founded: 2005,
    years_in_operation: 19,
    employee_count_band: "200-500",
    regions_served: 3,
    countries_served: 1,
    top_markets: ["California", "Arizona", "Nevada"],
    asset_classes: ["Select Service", "Extended Stay"],
    hotel_types: ["Urban", "Suburban", "Airport"],
    chain_scales: ["Upscale", "Upper Midscale", "Midscale"],
    branded_experience: true,
    independent_experience: false,
    operating_situations: ["Pre-Opening", "Conversion", "Transition", "Stabilized"],
    service_models: ["Third-Party Management"],
    primary_services: ["Hotel Management", "Revenue Management"],
    additional_services: ["Conversion Support", "Above-Property Revenue"],
    best_fit_for: ["Select-service and extended-stay owners", "Conversion projects", "West Coast markets"],
    less_proven_areas: ["Full-service", "Luxury", "Resort"],
    ownership_fit: ["Single-Asset Owner", "Multi-Property Owner"],
    market_fit: ["West Coast USA"],
    capability_tags: ["Select Service", "Extended Stay", "Branded", "Conversion", "West Coast"],
    brands_managed: ["Marriott", "Hilton", "IHG", "Choice"],
    geography: ["California", "Arizona", "Nevada"],
  },
  {
    id: "op-3",
    operator_name: "Summit Hotel Partners",
    logo_url: "",
    company_type: "3rd Party Operator",
    parent_company: "Summit Capital",
    hq_city: "Denver",
    hq_country: "USA",
    website: "https://www.summit-hotel.com",
    member_since: "2023",
    overview_short: "Boutique and lifestyle specialist. Independent and soft-brand focus with strong F&B and design capabilities.",
    overview_long: "Summit Hotel Partners operates boutique and lifestyle hotels for design-conscious owners. Strong F&B operations, creative marketing, and local market positioning. Best suited for urban and resort boutique assets.",
    hotels_managed_count: 15,
    rooms_managed_count: 2100,
    year_founded: 2012,
    years_in_operation: 12,
    employee_count_band: "100-200",
    regions_served: 5,
    countries_served: 2,
    top_markets: ["Mountain West", "Pacific Northwest"],
    asset_classes: ["Boutique", "Lifestyle", "Resort"],
    hotel_types: ["Urban", "Resort", "Lifestyle"],
    chain_scales: ["Luxury", "Upper Upscale", "Upscale"],
    branded_experience: true,
    independent_experience: true,
    operating_situations: ["Pre-Opening", "Conversion", "Repositioning", "Stabilized"],
    service_models: ["Third-Party Management", "Asset Management"],
    primary_services: ["Hotel Management", "F&B Operations", "Revenue Management", "Sales & Marketing"],
    additional_services: ["Design Consulting", "Repositioning"],
    best_fit_for: ["Boutique and lifestyle owners", "Independent and soft-brand assets", "F&B-forward properties"],
    less_proven_areas: ["Large convention hotels", "Economy segment"],
    ownership_fit: ["Family Office", "Single-Asset Owner", "Institutional"],
    market_fit: ["Mountain and resort markets", "Urban lifestyle"],
    capability_tags: ["Lifestyle", "Boutique", "Branded", "Independent", "F&B", "Repositioning"],
    brands_managed: ["Marriott (Autograph)", "Hyatt (Unbound)", "Independent"],
    geography: ["Mountain West", "Pacific Northwest", "Colorado"],
  },
  {
    id: "op-4",
    operator_name: "Eastern Standard Management",
    logo_url: "",
    company_type: "3rd Party Operator",
    parent_company: "",
    hq_city: "Philadelphia",
    hq_country: "USA",
    website: "https://www.easternstandardmgmt.com",
    member_since: "2020",
    overview_short: "Northeast and Mid-Atlantic specialist. Full-service and resort experience with strong turnaround capability.",
    overview_long: "Eastern Standard Management operates full-service and resort hotels in the Northeast and Mid-Atlantic. Proven track record in turnaround and transition situations. Strong local market relationships and labor management.",
    hotels_managed_count: 35,
    rooms_managed_count: 6200,
    year_founded: 1995,
    years_in_operation: 29,
    employee_count_band: "500-1000",
    regions_served: 6,
    countries_served: 1,
    top_markets: ["Northeast USA", "Mid-Atlantic"],
    asset_classes: ["Full-Service", "Resort"],
    hotel_types: ["Urban", "Suburban", "Resort"],
    chain_scales: ["Luxury", "Upper Upscale", "Upscale"],
    branded_experience: true,
    independent_experience: true,
    operating_situations: ["Conversion", "Transition", "Turnaround", "Stabilized"],
    service_models: ["Third-Party Management"],
    primary_services: ["Hotel Management", "Revenue Management", "Sales & Marketing"],
    additional_services: ["Turnaround Planning", "Above-Property"],
    best_fit_for: ["Turnaround situations", "Full-service and resort owners", "Northeast markets"],
    less_proven_areas: ["Pre-opening", "Extended stay"],
    ownership_fit: ["Institutional", "Family Office", "Multi-Property Owner"],
    market_fit: ["Northeast USA", "Mid-Atlantic"],
    capability_tags: ["Luxury", "Resort", "Turnaround", "Branded", "Independent", "Urban"],
    brands_managed: ["Marriott", "Hilton", "Hyatt", "IHG", "Independent"],
    geography: ["Northeast USA", "Mid-Atlantic", "Pennsylvania", "New York"],
  },
  {
    id: "op-5",
    operator_name: "All-Inclusive Experts Group",
    logo_url: "",
    company_type: "3rd Party Operator",
    parent_company: "AIEG Holdings",
    hq_city: "Miami",
    hq_country: "USA",
    website: "https://www.aieg.com",
    member_since: "2024",
    overview_short: "Caribbean and Mexico all-inclusive specialist. Pre-opening and conversion expertise for resort operations.",
    overview_long: "All-Inclusive Experts Group specializes in all-inclusive resort operations in the Caribbean and Mexico. Strong pre-opening and conversion experience. Full F&B, activities, and guest experience management.",
    hotels_managed_count: 12,
    rooms_managed_count: 4800,
    year_founded: 2010,
    years_in_operation: 14,
    employee_count_band: "500-1000",
    regions_served: 4,
    countries_served: 4,
    top_markets: ["Caribbean", "Mexico"],
    asset_classes: ["Resort", "All-Inclusive"],
    hotel_types: ["Resort", "Beach"],
    chain_scales: ["Luxury", "Upper Upscale"],
    branded_experience: true,
    independent_experience: true,
    operating_situations: ["Pre-Opening", "Conversion", "Stabilized"],
    service_models: ["Third-Party Management"],
    primary_services: ["Resort Management", "F&B Operations", "Activities", "Revenue Management"],
    additional_services: ["Pre-Opening", "Conversion", "Quality Assurance"],
    best_fit_for: ["All-inclusive resort owners", "Caribbean and Mexico projects", "Pre-opening and conversion"],
    less_proven_areas: ["Urban", "Select-service", "USA markets"],
    ownership_fit: ["Institutional", "Family Office"],
    market_fit: ["Caribbean", "Mexico", "Latin America"],
    capability_tags: ["All-Inclusive", "Resort", "Luxury", "Pre-Opening", "Conversion", "Caribbean"],
    brands_managed: ["Marriott", "Hyatt", "Independent"],
    geography: ["Caribbean", "Mexico", "Dominican Republic", "Jamaica"],
  },
];

/** GET /api/operator-explorer/operators – same handler as GET /api/third-party-operators (pass ?activeOnly=1 for Explorer) */
export async function listOperators(req, res) {
  return listThirdPartyOperators(req, res);
}

/** GET /api/operator-explorer/operator – Single operator detail by id */
export async function getOperatorById(req, res) {
  try {
    const { operatorId } = req.query;
    const id = String(operatorId || req.params?.operatorId || "").trim();
    if (!id) {
      return res.status(400).json({ success: false, error: "Operator ID required" });
    }

    /** Real Operator Setup rows use Airtable record ids — delegate to intake detail (same as My Operators / Gold Mock). */
    if (/^rec[a-zA-Z0-9]{14,}$/.test(id)) {
      const detailReq = { ...req, params: { ...(req.params || {}), recordId: id } };
      return getThirdPartyOperatorDetail(detailReq, res);
    }

    const operator = MOCK_OPERATORS.find(
      (o) => o.id === id || o.operator_name?.toLowerCase() === String(id).toLowerCase()
    );

    if (!operator) {
      return res.status(404).json({ success: false, error: "Operator not found" });
    }

    // Enrich detail with extended fields for tab content
    const detail = {
      ...operator,
      // Operating capabilities
      revenue_management_support: "Above-property revenue management team. Weekly pricing review, channel optimization.",
      sales_support: "Regional sales directors, group and transient focus.",
      marketing_support: "Digital marketing, brand compliance support.",
      qa_support: "Quarterly QA audits, brand standard compliance.",
      training_platform: "In-house LMS, brand-specific training programs.",
      reporting_style: "Monthly P&L, variance analysis, KPI dashboard.",
      owner_reporting_cadence: "Monthly owner calls, quarterly business reviews.",
      food_beverage_capability: operator.asset_classes?.includes("Resort") || operator.asset_classes?.includes("Full-Service") ? "Full F&B operations, banquet, catering." : "Limited F&B, grab-and-go focus.",
      procurement_support: "Centralized procurement, preferred vendor programs.",
      engineering_support: "Above-property chief engineer support, CapEx planning.",
      systems_stack: ["Opera", "M3", "Duetto or IDeaS"],
      // Diligence questions (example)
      owner_questions: [
        { category: "Comparable Experience", questions: ["What similar assets have you opened or converted in the last 24 months?", "Can you share references from owners with similar project types?"] },
        { category: "Reporting Cadence", questions: ["How often do you provide financial and operational reports?", "What KPIs do you track and share with owners?"] },
        { category: "First 90-Day Transition", questions: ["What is your typical transition timeline?", "How do you handle key personnel during a takeover?"] },
        { category: "Revenue Management", questions: ["Is revenue management in-house or outsourced?", "What systems and tools do you use?"] },
      ],
      // Case studies (placeholders)
      case_studies: [
        { hotel_type: "Upper Upscale Full-Service", region: "Southeast USA", branded: true, situation_type: "Conversion", services: "Management, Revenue, Sales", outcome: "Successfully converted and stabilized within 12 months.", owner_relevance: "Similar to urban conversion projects." },
        { hotel_type: "Luxury Resort", region: "Mountain West", branded: true, situation_type: "Pre-Opening", services: "Pre-Opening, Management", outcome: "Opened on schedule. Exceeded first-year RevPAR forecast.", owner_relevance: "Relevant for resort pre-opening." },
      ],
      // Fit to deal (placeholder when no active deal)
      fit_score: null,
      fit_reason_summary: null,
      gap_flags: [],
    };

    res.json({ success: true, operator: detail });
  } catch (err) {
    console.error("Operator Explorer detail error:", err);
    res.status(500).json({ success: false, error: err.message });
  }
}
