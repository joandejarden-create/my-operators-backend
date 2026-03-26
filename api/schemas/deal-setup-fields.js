/**
 * Central field mapping for Deal Setup (PATCH) write path.
 * Single source of truth for Airtable table names, link fields, and form→Airtable column names.
 * Used by api/my-deals.js updateMyDealById and by deal-setup validation.
 */

// ---------------------------------------------------------------------------
// Table names (env or default)
// ---------------------------------------------------------------------------
export const DEALS_TABLE = process.env.AIRTABLE_TABLE_DEALS || "Deals";
export const LOCATION_PROPERTY_TABLE = process.env.AIRTABLE_TABLE_LOCATION_PROPERTY || "Location & Property";
export const MARKET_PERFORMANCE_TABLE = "Market - Performance - Deal & Capital Structure";
export const STRATEGIC_INTENT_TABLE = process.env.AIRTABLE_TABLE_STRATEGIC_INTENT || "Strategic Intent - Operational - Key Challenges";
export const CONTACT_UPLOADS_TABLE = process.env.AIRTABLE_TABLE_CONTACT_UPLOADS || "Contact & Uploads";
export const LEASE_STRUCTURE_TABLE = process.env.AIRTABLE_TABLE_LEASE_STRUCTURE || "Lease Structure";

// ---------------------------------------------------------------------------
// Deal Status: one field only (env or default "Deal Status")
// ---------------------------------------------------------------------------
export const DEALS_STATUS_FIELD = process.env.AIRTABLE_DEALS_STATUS_FIELD || "Deal Status";

// ---------------------------------------------------------------------------
// Deals table: form field name → Airtable column name (Batch 1 and other Deals-only fields)
// Use when form key and Airtable column differ (typos, renames).
// Franchise/affiliation: many bases use the legacy typo "agreeement"; we default write to that.
// If your base uses the correct spelling "agreement", set AIRTABLE_DEALS_FRANCHISE_AFFILIATION_FIELD
// to the full correct column name.
// ---------------------------------------------------------------------------
/** Correct spelling (for read mapping and env override). */
const DEALS_FRANCHISE_AFFILIATION_AIRTABLE_CORRECT =
  "Has there ever been a franchise, branded management, affiliation or similar agreement pertaining to the proposed hotel or site?";
/** Typo column name (legacy): base often has "agreeement"; default PATCH write to this so write succeeds. */
const DEALS_FRANCHISE_AFFILIATION_AIRTABLE_TYPO =
  "Has there ever been a franchise, branded management, affiliation or similar agreeement pertaining to the proposed hotel or site?";

const DEALS_FRANCHISE_AFFILIATION_AIRTABLE =
  process.env.AIRTABLE_DEALS_FRANCHISE_AFFILIATION_FIELD ||
  DEALS_FRANCHISE_AFFILIATION_AIRTABLE_TYPO;

const DEALS_OPERATOR_NAME_AIRTABLE =
  process.env.AIRTABLE_DEALS_OPERATOR_NAME_FIELD || "Operator Name";

const DEALS_FRANCHISE_AFFILIATION_FORM_KEY =
  "Has there ever been a franchise, branded management, affiliation or similar agreement pertaining to the proposed hotel or site?";

export const DEALS_FORM_TO_AIRTABLE = {
  [DEALS_FRANCHISE_AFFILIATION_FORM_KEY]: DEALS_FRANCHISE_AFFILIATION_AIRTABLE,
  "Are you open to considering other brands with favorable terms?": "Are you open to lesser-known or emerging brands with favorable terms?",
  "Operator Name Current": DEALS_OPERATOR_NAME_AIRTABLE,
};

/** Airtable column name → form field name (for GET merge so client rebind uses form keys). */
export const DEALS_AIRTABLE_TO_FORM = {
  [DEALS_FRANCHISE_AFFILIATION_AIRTABLE_CORRECT]: DEALS_FRANCHISE_AFFILIATION_FORM_KEY,
  [DEALS_FRANCHISE_AFFILIATION_AIRTABLE_TYPO]: DEALS_FRANCHISE_AFFILIATION_FORM_KEY,
  "Are you open to lesser-known or emerging brands with favorable terms?": "Are you open to considering other brands with favorable terms?",
  [DEALS_OPERATOR_NAME_AIRTABLE]: "Operator Name Current",
};

// ---------------------------------------------------------------------------
// Link fields (Deals ↔ linked tables)
// ---------------------------------------------------------------------------
export const LOCATION_LINK_FIELD = "Location & Property";
export const LOCATION_LINK_ALIAS = "Location and Property";
export const LOCATION_PROPERTY_ID_FIELD = process.env.AIRTABLE_LOCATION_PROPERTY_ID_FIELD || "Location_Property_ID";

export const MARKET_PERFORMANCE_LINK_FIELD = "Market - Performance - Deal & Capital Structure";
export const MP_DEAL_LINK_FIELD = process.env.AIRTABLE_MP_DEAL_LINK_FIELD || "Deal_ID";

export const STRATEGIC_INTENT_LINK_FIELD = "Strategic Intent - Operational - Key Challenges";

export const CONTACT_UPLOADS_LINK_FIELD = "Contact & Uploads";
export const CU_DEAL_LINK_FIELD = process.env.AIRTABLE_CU_DEAL_LINK_FIELD || "Deal_ID";
/** Airtable column name for attachment field on Contact & Uploads (Deal Setup Tab 13). */
export const CU_ATTACHMENT_FIELD = process.env.AIRTABLE_CU_ATTACHMENT_FIELD || "Upload Supporting Docs";

export const LEASE_STRUCTURE_LINK_FIELD = process.env.AIRTABLE_DEALS_LINK_FIELD_LEASE_STRUCTURE || "Lease Structure";
export const LS_DEAL_LINK_FIELD = process.env.AIRTABLE_LEASE_STRUCTURE_DEAL_LINK_FIELD || "Deal_ID";

// ---------------------------------------------------------------------------
// Location & Property: form → Airtable column name
// Tota Site Size Unit: default is typo "Tota". If your base uses "Total Site Size Unit", set:
//   AIRTABLE_LOCATION_TOTAL_SITE_SIZE_UNIT_FIELD=Total Site Size Unit
// ---------------------------------------------------------------------------
const LOCATION_TOTAL_SITE_SIZE_UNIT_AIRTABLE = process.env.AIRTABLE_LOCATION_TOTAL_SITE_SIZE_UNIT_FIELD || "Tota Site Size Unit";

export const LOCATION_FORM_TO_AIRTABLE = {
  "Full Address": "Full Address",
  "City & State": "City",
  "Country": "Country",
  "Hotel Type": "Hotel Type",
  "Hotel Chain Scale": "Hotel Chain Scale",
  "Hotel Submarket & Location": "Hotel Submarket & Location",
  "Hotel Service Model": "Hotel Service Model",
  "Ownership/Brand History or Track Record": "Ownership/Brand History or Track Record",
  "Portfolio Size": "Portfolio Size",
  "Company Executive Summary": "Company Executive Summary",
  "Zoned for Hotel Development": "Zoned for Hotel Development",
  "Site/Development Restrictions?": "Site/Development Restrictions?",
  "Site/Development Restrictions Description": "Site/Development Restrictions Description",
  "Total Site Size": "Total Site Size",
  "Total Site Size Unit": LOCATION_TOTAL_SITE_SIZE_UNIT_AIRTABLE,
  "Max height Allowed By Zoning": "Max Height Allowed By Zoning",
  "Max Height Allowed By Zoning": "Max Height Allowed By Zoning",
  "Max height Unit": "Max Height Allowed By Zoning Unit",
  "Max Height Allowed By Zoning Unit": "Max Height Allowed By Zoning Unit",
  "Ownership Type": "Ownership Type",
  "Ownership Type Other Text": "Ownership Type Other Text",
  "Current Form of Site Control": "Current Form of Site Control",
  "Current Form of Site Control Other Text": "Current Form of Site Control Other Text",
  "Zoning Status": "Zoning Status",
  "Zoning Status Other Text": "Zoning Status Other Text",
  "Parking Ratio": "Parking Ratio",
  "Access to Transit or Highway": "Access to Transit / Highway",
  "Access to Transit or Highway Other Text": "Access to Transit / Highway Other Text",
  "Total Number of Rooms/Keys": "Total Number of Rooms/Keys",
  "Number of Standard Rooms": "Number of Standard Rooms",
  "Number of Suites": "Number of Suites",
  // Form uses "Number of Stories"; Airtable column is "# of Stories" in Location & Property
  "Number of Stories": "# of Stories",
  "# of Stories": "# of Stories",
  "Building Type": "Building Type",
  "Year Built (Years Open as a Hotel)": "Year Built (Years Open as a Hotel)",
  "PMS or Tech is in Place": "PMS or Tech is in Place",
  "Ceiling Heights": "Ceiling Heights",
  "Ceiling Heights Unit": "Ceiling Heights Unit",
  "Column Spacing": "Column Spacing",
  "Column Spacing Unit": "Column Spacing Unit",
  "Existing MEP Capacity (Conversion)": "Existing MEP Capacity (Conversion)",
  "F&B Outlets?": "F&B Outlets?",
  "Number of F&B Outlets": "Number of F&B Outlets",
  "F&B Program Type": "F&B Program Type",
  "Outlet Names / Concepts": "Outlet Names / Concepts",
  "Total F&B Outlet Size": "Total F&B Outlet Size",
  "Total F&B Outlet Size Unit": "Total F&B Outlet Size Unit",
  "Meeting Space": "Meeting Space",
  "Meeting Space Unit": "Meeting Space Unit",
  "Number of Meeting Rooms": "Number of Meeting Rooms",
  "Condo Residences?": "Condo Residences?",
  "Hotel Rental Program?": "Hotel Rental Program?",
};

/** Form field names that belong to Location & Property (used to route and delete from deal fields). */
export const LOCATION_FORM_FIELDS = Object.keys(LOCATION_FORM_TO_AIRTABLE);

/**
 * expandedLocation (camelCase) key → form field name. Used to merge Location data into deal.fields
 * so GET and save-response rebind have form keys (Batch 2 Q2).
 */
export const LOCATION_EXPANDED_TO_FORM = {
  fullAddress: "Full Address",
  city: "City & State",
  country: "Country",
  hotelType: "Hotel Type",
  hotelChainScale: "Hotel Chain Scale",
  submarket: "Hotel Submarket & Location",
  hotelServiceModel: "Hotel Service Model",
  ownershipTrackRecord: "Ownership/Brand History or Track Record",
  portfolioSize: "Portfolio Size",
  companyExecutiveSummary: "Company Executive Summary",
  zonedForHotelDevelopment: "Zoned for Hotel Development",
  siteDevelopmentRestrictions: "Site/Development Restrictions?",
  siteDevelopmentRestrictionsDescription: "Site/Development Restrictions Description",
  totalSiteSize: "Total Site Size",
  totalSiteSizeUnit: "Total Site Size Unit",
  maxHeightAllowedByZoning: "Max height Allowed By Zoning",
  maxHeightAllowedByZoningUnit: "Max height Unit",
  ownershipType: "Ownership Type",
  ownershipTypeOtherText: "Ownership Type Other Text",
  currentFormOfSiteControl: "Current Form of Site Control",
  currentFormOfSiteControlOtherText: "Current Form of Site Control Other Text",
  zoningStatus: "Zoning Status",
  zoningStatusOtherText: "Zoning Status Other Text",
  parkingRatio: "Parking Ratio",
  accessToTransit: "Access to Transit or Highway",
  accessToTransitOtherText: "Access to Transit or Highway Other Text",
  totalNumberOfRoomsKeys: "Total Number of Rooms/Keys",
  numberStandardRooms: "Number of Standard Rooms",
  numberSuites: "Number of Suites",
  numberStories: "Number of Stories",
  buildingType: "Building Type",
  yearBuilt: "Year Built (Years Open as a Hotel)",
  pmsOrTech: "PMS or Tech is in Place",
  ceilingHeights: "Ceiling Heights",
  ceilingHeightsUnit: "Ceiling Heights Unit",
  columnSpacing: "Column Spacing",
  columnSpacingUnit: "Column Spacing Unit",
  existingMEPCapacity: "Existing MEP Capacity (Conversion)",
};

// ---------------------------------------------------------------------------
// Market - Performance
// ---------------------------------------------------------------------------
export const MARKET_PERFORMANCE_FIELD_NAMES = new Set([
  "Primary Demand Drivers",
  "Primary Demand Drivers Other",
  "Estimated or Actual RevPAR",
  "Regulatory or Permitting Issues?",
  "Regulatory or Permitting Issues Description",
  "Key Competitors",
  "Group vs Transient Mix",
  "Total Project Cost Range",
  "PIP Budget Range (if conversion)",
  "Equity vs Debt Split",
  "Ownership Structure",
  "Preferred Deal Structure",
  "PIP / CapEx Status",
]);

export const MP_FORM_TO_TABLE = {
  "Group vs Transient Mix": "Group vs Transient Mix (If Known)",
  "Regulatory or Permitting Issues Description": "Regulatory or Permitting Issues Text",
  "Primary Demand Drivers Other": "Primary Demand Drivers Other Text",
  "PIP Budget Range (if conversion)": "PIP Budget Range (If Conversion)",
};

export const MP_TABLE_TO_FORM = {
  "Group vs Transient Mix (If Known)": "Group vs Transient Mix",
  "Regulatory or Permitting Issues Text": "Regulatory or Permitting Issues Description",
  "Primary Demand Drivers Other Text": "Primary Demand Drivers Other",
  "PIP Budget Range (If Conversion)": "PIP Budget Range (if conversion)",
};

// ---------------------------------------------------------------------------
// Strategic Intent
// ---------------------------------------------------------------------------
export const STRATEGIC_INTENT_FORM_FIELDS = [
  "Soft vs Hard Brand Preference",
  "Preferred Chain Scales",
  "Open to Soft Brand First Then Reflag?",
  "Target Guest Segment",
  "Target Guest Segment Other",
  "Brand Flexibility vs Prestige",
  "IRR/Yield Goals",
  "Open to Outside Capital or Partnerships?",
  "Preferred Brands (up to 4)",
  "Planned Hold Period",
  "Primary Goal for the Hotel",
  "Primary Goal for the Hotel Other",
  "Plan to Self-Manage or Hire Third Party?",
  "Who should receive bids for this project?",
  "Minimum Operator Experience (years)",
  "Preferred Third-Party Operators (names)",
  "Preferred Third-Party Operator Profile",
  "Services Required From Operator",
  "Other Operator Criteria or Notes",
  "Level of Involvement in Day-to-Day Ops",
  "Preferred Reporting Frequency",
  "On-Site vs Remote Owner Representation",
  "Speed to Market Importance",
  "Development / Renovation Timeline Importance",
  "CapEx / PIP Execution Importance",
  "Revenue / Yield Management Importance",
  "Marketing & Distribution Importance",
  "Loyalty Program Importance",
  "Brand Recognition Importance",
  "Brand Equity Increase on Exit Importance",
  "Guest Experience / Satisfaction Importance",
  "Cost Control / Operational Efficiency Importance",
  "Staffing & Talent Importance",
  "Technology & Systems Importance",
  "Incentive Alignment Importance",
  "ESG / Sustainability Importance",
  "Top 3 Success Metrics",
  "Top 3 Success Metrics Other",
  "Top Priorities for Project",
  "Top Priorities for Project Other",
  "Top Concerns for this Project",
  "Top Concerns for this Project Other",
  "Decision Timeline for Brand/Operator",
  "Critical deadlines for application",
  "Critical Deadlines Description",
  "Top 3 Deal Breakers",
  "Top 3 Deal Breakers Other",
  "Must-haves From Brand or Operator",
  "Must-haves From Brand or Operator Other",
  "Incentive Types Interested In",
  "Incentive Types Interested In Other"
];

export const SI_FORM_TO_AIRTABLE = {
  "Preferred Brands (up to 4)": "Preferred Brands",
  "Open to Soft Brand First Then Reflag?": "Open to Soft Brand First, Then Reflag?",
  "Primary Goal for the Hotel Other": "Primary Goal for the Hotel Other Text",
  "Target Guest Segment Other": "Target Guest Segment Other Text",
  "Who should receive bids for this project?": "Who Should Receive Bids for This Project?",
  "Minimum Operator Experience (years)": "Minimum Operator Experience (Years)",
  "Preferred Third-Party Operators (names)": "Preferred Third-Party Operators (Names)",
  "Speed to Market Importance": "Speed to Market",
  "Development / Renovation Timeline Importance": "Development / Renovation Timeline",
  "CapEx / PIP Execution Importance": "CapEx / PIP Execution",
  "Revenue / Yield Management Importance": "Revenue / Yield Management",
  "Marketing & Distribution Importance": "Marketing & Distribution",
  "Loyalty Program Importance": "Loyalty Program",
  "Brand Recognition Importance": "Brand Recognition",
  "Brand Equity Increase on Exit Importance": "Brand Equity Increase on Exit",
  "Guest Experience / Satisfaction Importance": "Guest Experience / Satisfaction",
  "Cost Control / Operational Efficiency Importance": "Cost Control / Operational Efficiency",
  "Staffing & Talent Importance": "Staffing & Talent",
  "Technology & Systems Importance": "Technology & Systems",
  "Incentive Alignment Importance": "Incentive Alignment",
  "ESG / Sustainability Importance": "ESG / Sustainability",
  "Critical deadlines for application": "Critical Deadlines",
  "Critical Deadlines Description": "Critical Deadlines Text",
  "Must-haves From Brand or Operator": "Must-Haves From Brand/Operator",
  "Must-haves From Brand or Operator Other": "Must-Haves From Brand/Operator Other Text",
  "Incentive Types Interested In Other": "Incentive Types Interested In Other Text"
};

export const SI_AIRTABLE_TO_FORM = {
  "Preferred Brands": "Preferred Brands (up to 4)",
  "Open to Soft Brand First, Then Reflag?": "Open to Soft Brand First Then Reflag?",
  "Primary Goal for the Hotel Other Text": "Primary Goal for the Hotel Other",
  "Target Guest Segment Other Text": "Target Guest Segment Other",
  "Who Should Receive Bids for This Project?": "Who should receive bids for this project?",
  "Minimum Operator Experience (Years)": "Minimum Operator Experience (years)",
  "Preferred Third-Party Operators (Names)": "Preferred Third-Party Operators (names)",
  "Speed to Market": "Speed to Market Importance",
  "Development / Renovation Timeline": "Development / Renovation Timeline Importance",
  "CapEx / PIP Execution": "CapEx / PIP Execution Importance",
  "Revenue / Yield Management": "Revenue / Yield Management Importance",
  "Marketing & Distribution": "Marketing & Distribution Importance",
  "Loyalty Program": "Loyalty Program Importance",
  "Brand Recognition": "Brand Recognition Importance",
  "Brand Equity Increase on Exit": "Brand Equity Increase on Exit Importance",
  "Guest Experience / Satisfaction": "Guest Experience / Satisfaction Importance",
  "Cost Control / Operational Efficiency": "Cost Control / Operational Efficiency Importance",
  "Staffing & Talent": "Staffing & Talent Importance",
  "Technology & Systems": "Technology & Systems Importance",
  "Incentive Alignment": "Incentive Alignment Importance",
  "ESG / Sustainability": "ESG / Sustainability Importance",
  "Critical Deadlines": "Critical deadlines for application",
  "Critical Deadlines Text": "Critical Deadlines Description",
  "Must-Haves From Brand/Operator": "Must-haves From Brand or Operator",
  "Must-Haves From Brand/Operator Other Text": "Must-haves From Brand or Operator Other",
  "Incentive Types Interested In Other Text": "Incentive Types Interested In Other"
};

// ---------------------------------------------------------------------------
// Contact & Uploads
// ---------------------------------------------------------------------------
export const CONTACT_UPLOADS_FORM_FIELDS = [
  "Would you like to filter out brands without key money?",
  "Would you like to meet consultants?",
  "Legal Support Needed?",
  "Financial Model Available?",
  "Proposal Deadline",
  "Would you like to receive regular updates?",
  "Working with Broker/Advisor?",
  "Broker/Advisor Company and Contract Details",
  "Other Projects Nearing Contract Expiration?",
  "Contact Source",
  "Main Contact Name",
  "Main Contact Title",
  "Entity or Company Name",
  "Company HQ Location",
  "Email Address",
  "Secondary Contact",
  "Best Time or Method to Reach",
  "What makes this opportunity stand out to a brand or operator?",
  "Additional Notes or Unique Project Aspects",
  "Anything else you'd like to add?",
  "Upload Supporting Docs",
];

export const CU_FORM_TO_AIRTABLE = {
  "Would you like to filter out brands without key money?": "Would You Like to Filter Out Brands Without Key Money?",
  "Would you like to meet consultants?": "Would You Like to Meet Consultants?",
  "Would you like to receive regular updates?": "Would You Like to Receive Regular Updates?",
};

// ---------------------------------------------------------------------------
// Lease Structure
// ---------------------------------------------------------------------------
export const LEASE_STRUCTURE_FORM_FIELDS = [
  "Lease Type",
  "Initial Lease Term (years)",
  "Lease Start Date (or Availability)",
  "Lease Expiration or End Date",
  "Base Rent (annual or structure)",
  "Percentage Rent (if applicable)",
  "CAM Insurance Tax Responsibility",
  "Key Money or TI Allowance",
  "Renewal Options",
  "Early Termination or Break Clause",
  "Security Deposit or Guarantees",
  "Lease Structure Notes",
];

export const LS_FORM_TO_AIRTABLE = {
  "Initial Lease Term (years)": "Initial Lease Term (Years)",
  "Lease Expiration or End Date": "Lease Expiration / End Date",
  "Base Rent (annual or structure)": "Base Rent (Annual or Structure)",
  "CAM Insurance Tax Responsibility": "CAM / Insurance / Tax Responsibility",
  "Key Money or TI Allowance": "Key Money / TI Allowance",
  "Early Termination or Break Clause": "Early Termination / Break Clause",
  "Security Deposit or Guarantees": "Security Deposit / Guarantees",
};

export const LS_AIRTABLE_TO_FORM = {
  "Initial Lease Term (Years)": "Initial Lease Term (years)",
  "Lease Expiration / End Date": "Lease Expiration or End Date",
  "Base Rent (Annual or Structure)": "Base Rent (annual or structure)",
  "CAM / Insurance / Tax Responsibility": "CAM Insurance Tax Responsibility",
  "Key Money / TI Allowance": "Key Money or TI Allowance",
  "Early Termination / Break Clause": "Early Termination or Break Clause",
  "Security Deposit / Guarantees": "Security Deposit or Guarantees",
};

// ---------------------------------------------------------------------------
// Required fields by section (M1 source of truth; section 7 = Lease, skip when Lease hidden)
// ---------------------------------------------------------------------------
export const REQUIRED_DEAL_SETUP_FIELDS_BY_SECTION = {
  0: ["Property Name", "Project Type", "Stage of Development"],
  1: [
    "Has there ever been a franchise, branded management, affiliation or similar agreement pertaining to the proposed hotel or site?",
    "Is the hotel currently branded?",
    "Is the hotel currently managed by a third-party operator?",
    "Are you open to considering other brands with favorable terms?",
    "Have you worked with any of your preferred brands/operators before?",
  ],
  2: [
    "Full Address",
    "Hotel Chain Scale",
    "Hotel Type",
    "Hotel Submarket & Location",
    "Hotel Service Model",
    "Ownership/Brand History or Track Record",
    "Zoned for Hotel Development",
    "Site/Development Restrictions?",
    "Total Site Size",
    "Total Site Size Unit",
    "Max height Allowed By Zoning",
    "Max height Unit",
    "Current Form of Site Control",
  ],
  3: ["Total Number of Rooms/Keys", "Number of Standard Rooms", "Number of Suites", "Building Type", "Number of Stories"],
  4: ["F&B Outlets?", "Meeting Space", "Number of Meeting Rooms", "Condo Residences?", "Hotel Rental Program?", "Parking Amenities?", "Additional Amenities"],
  5: [
    "Estimated or Actual RevPAR",
    "Regulatory or Permitting Issues?",
    "Key Competitors",
  ],
  6: [
    "Total Project Cost Range",
    "PIP Budget Range (if conversion)",
    "Equity vs Debt Split",
    "Ownership Structure",
    "Preferred Deal Structure",
    "PIP / CapEx Status",
  ],
  7: ["Lease Type"],
  8: [
    "Soft vs Hard Brand Preference",
    "Preferred Brands (up to 4)",
    "IRR/Yield Goals",
    "Open to Outside Capital or Partnerships?",
    "Plan to Self-Manage or Hire Third Party?",
    "Preferred Chain Scales",
    "Open to Soft Brand First Then Reflag?",
    "Target Guest Segment",
    "Brand Flexibility vs Prestige",
    "Planned Hold Period",
    "Primary Goal for the Hotel",
    "Who should receive bids for this project?",
    "Minimum Operator Experience (years)",
    "Preferred Third-Party Operators (names)",
    "Preferred Third-Party Operator Profile",
    "Services Required From Operator",
    "Other Operator Criteria or Notes",
    "Level of Involvement in Day-to-Day Ops",
  ],
  9: ["Top Priorities for Project", "Top Concerns for this Project", "Top 3 Success Metrics", "Top 3 Deal Breakers", "Must-haves From Brand or Operator", "Decision Timeline for Brand/Operator"],
  10: [],
  11: [
    "Would you like to filter out brands without key money?",
    "Would you like to meet consultants?",
    "Legal Support Needed?",
    "Financial Model Available?",
    "Proposal Deadline",
    "Would you like to receive regular updates?",
    "Working with Broker/Advisor?",
    "Other Projects Nearing Contract Expiration?",
  ],
  12: ["Main Contact Name", "Entity or Company Name", "Company HQ Location", "Email Address"],
  13: [],
};
