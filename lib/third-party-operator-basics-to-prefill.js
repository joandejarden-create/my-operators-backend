/**
 * Maps 3rd Party Operator — Basics Airtable column names → intake form field names (req.body / input name).
 * Keeps edit/prefill in sync with third-party-operator-intake.js create payload.
 *
 * Canonical write surface for submission is **Basics** (see `third-party-operator-intake.js` header).
 * A few names here pair with `BASICS_COLUMN_CANONICAL_TO_ALTERNATE` when the base uses alternate column titles.
 */

import { formatListValue, parseMultiValue, safeParseJsonArray } from "./third-party-operator-value-utils.js";

/** @type {Record<string, string>} */
export const BASICS_AIRTABLE_TO_FORM_KEY = {
  "Company Name": "companyName",
  Website: "website",
  "Headquarters Location": "headquarters",
  "Year Established": "yearEstablished",
  "Primary Contact Email": "contactEmail",
  "Primary Contact Phone": "contactPhone",
  "Contact Name": "contactName",
  "Preferred Contact Method": "preferredContactMethod",
  "Company Description": "companyDescription",
  "Company Tagline": "companyTagline",
  "Mission Statement": "missionStatement",
  "Primary Service Model": "primaryServiceModel",
  "Company Size": "companySize",
  "Years in Business": "yearsInBusiness",
  "Number of Markets Operated In": "numberOfMarkets",
  "Portfolio Metrics As of Date": "portfolioMetricsAsOf",
  "Service Offering Summary": "serviceDifferentiators",
  "Typical Response Time for Owner Inquiries": "ownerResponseTime",
  "Typical Concern Resolution Time": "concernResolutionTime",
  "Owner Education Programs": "ownerEducation",
  "Owner Satisfaction Score (NPS)": "ownerSatisfactionScore",
  "Owner Portal Features": "ownerPortalFeatures",
  "Number of Brands Supported": "numberOfBrands",
  "Brands Managed": "brands",
  "Chain Scales You Support": "chainScalesSupported",
  "Additional Brands": "additionalBrands",
  "Brands Portfolio Detail": "brandsPortfolioDetail",
  "Brand Units & Staffing Detail": "brandsPortfolioDetail",
  "Brand Units and Staffing Detail": "brandsPortfolioDetail",
  /** Long text JSON blob — client `applyExplorerProfileJsonPrefill` fills overview_/cap_/brand_/… fields. */
  "Explorer Profile JSON": "explorerProfileJson",
  "Regions Supported": "regions",
  "Specific Markets": "specificMarkets",
  "Location Type Urban": "locationTypeUrban",
  "Location Type Suburban": "locationTypeSuburban",
  /** Basics dedupe column pairing (see airtable-field-corrections.csv). */
  "Location Type Airport": "locationTypeResort",
  "Location Type Resort": "locationTypeAirport",
  "Location Type Highway": "locationTypeSmallMetro",
  "Location Type Other": "locationTypeInterstate",
  "Location Type Total": "locationTypeTotal",
  "# of Exits / Deflaggings (Units) in Past 24 Months": "exitsDeflaggings",
  "Figures As Of": "figuresAsOf",
  "Geo NA Existing Hotels": "geo_na_existing_hotels",
  "NA Existing Rooms": "geo_na_existing_rooms",
  "Geo NA Pipeline Hotels": "geo_na_pipeline_hotels",
  "NA Pipeline Rooms": "geo_na_pipeline_rooms",
  "Geo NA Total Hotels": "geo_na_total_hotels",
  "Geo NA Total Rooms": "geo_na_total_rooms",
  "CALA Existing Hotels": "geo_cala_existing_hotels",
  "CALA Existing Rooms": "geo_cala_existing_rooms",
  "CALA Pipeline Hotels": "geo_cala_pipeline_hotels",
  "CALA Pipeline Rooms": "geo_cala_pipeline_rooms",
  "Geo CALA Total Hotels": "geo_cala_total_hotels",
  "Geo CALA Total Rooms": "geo_cala_total_rooms",
  "Geo EU Existing Hotels": "geo_eu_existing_hotels",
  "EU Existing Rooms": "geo_eu_existing_rooms",
  "Geo EU Pipeline Hotels": "geo_eu_pipeline_hotels",
  "EU Pipeline Rooms": "geo_eu_pipeline_rooms",
  "Geo EU Total Hotels": "geo_eu_total_hotels",
  "Geo EU Total Rooms": "geo_eu_total_rooms",
  "Geo MEA Existing Hotels": "geo_mea_existing_hotels",
  "MEA Existing Rooms": "geo_mea_existing_rooms",
  "Geo MEA Pipeline Hotels": "geo_mea_pipeline_hotels",
  "MEA Pipeline Rooms": "geo_mea_pipeline_rooms",
  "Geo MEA Total Hotels": "geo_mea_total_hotels",
  "Geo MEA Total Rooms": "geo_mea_total_rooms",
  "Geo APAC Existing Hotels": "geo_apac_existing_hotels",
  "APAC Existing Rooms": "geo_apac_existing_rooms",
  "Geo APAC Pipeline Hotels": "geo_apac_pipeline_hotels",
  "APAC Pipeline Rooms": "geo_apac_pipeline_rooms",
  "Geo APAC Total Hotels": "geo_apac_total_hotels",
  "Geo APAC Total Rooms": "geo_apac_total_rooms",
  "Geo Total Existing Hotels": "geo_total_existing_hotels",
  "Geo Total Existing Rooms": "geo_total_existing_rooms",
  "Geo Total Pipeline Hotels": "geo_total_pipeline_hotels",
  "Geo Total Pipeline Rooms": "geo_total_pipeline_rooms",
  "Geo Total Hotels": "geo_total_total_hotels",
  "Geo Total Rooms": "geo_total_total_rooms",
  "Chain Scale": "chainScale",
  "Total Properties Managed": "totalProperties",
  "Total Rooms Managed": "totalRooms",
  "Property Types": "propertyTypes",
  "Luxury Properties Managed": "luxuryProperties",
  "Luxury Rooms Managed": "luxuryRooms",
  "Luxury Avg Staff": "luxuryAvgStaff",
  "Luxury Existing Properties": "luxuryExistingProperties",
  "Luxury Existing Rooms": "luxuryExistingRooms",
  "Luxury Pipeline Properties": "luxuryPipelineProperties",
  "Luxury Pipeline Rooms": "luxuryPipelineRooms",
  "Upper Upscale Properties Managed": "upperUpscaleProperties",
  "Upper Upscale Rooms Managed": "upperUpscaleRooms",
  "Upper Upscale Avg Staff": "upperUpscaleAvgStaff",
  "Upper Upscale Existing Properties": "upperUpscaleExistingProperties",
  "Upper Upscale Existing Rooms": "upperUpscaleExistingRooms",
  "Upper Upscale Pipeline Properties": "upperUpscalePipelineProperties",
  "Upper Upscale Pipeline Rooms": "upperUpscalePipelineRooms",
  "Upscale Properties Managed": "upscaleProperties",
  "Upscale Rooms Managed": "upscaleRooms",
  "Upscale Avg On-Site Staff Per Property": "upscaleAvgStaff",
  "Upscale Existing Properties": "upscaleExistingProperties",
  "Upscale Existing Rooms": "upscaleExistingRooms",
  "Upscale Pipeline Properties": "upscalePipelineProperties",
  "Upscale Pipeline Rooms": "upscalePipelineRooms",
  "Upper Midscale Properties Managed": "upperMidscaleProperties",
  "Upper Midscale Rooms Managed": "upperMidscaleRooms",
  "Upper Midscale Avg Staff": "upperMidscaleAvgStaff",
  "Upper Midscale Existing Properties": "upperMidscaleExistingProperties",
  "Upper Midscale Existing Rooms": "upperMidscaleExistingRooms",
  "Upper Midscale Pipeline Properties": "upperMidscalePipelineProperties",
  "Upper Midscale Pipeline Rooms": "upperMidscalePipelineRooms",
  "Midscale Properties Managed": "midscaleProperties",
  "Midscale Rooms Managed": "midscaleRooms",
  "Midscale Avg Staff": "midscaleAvgStaff",
  "Midscale Existing Properties": "midscaleExistingProperties",
  "Midscale Existing Rooms": "midscaleExistingRooms",
  "Midscale Pipeline Properties": "midscalePipelineProperties",
  "Midscale Pipeline Rooms": "midscalePipelineRooms",
  "Economy Properties Managed": "economyProperties",
  "Economy Rooms Managed": "economyRooms",
  "Economy Avg On-Site Staff Per Property": "economyAvgStaff",
  "Economy Existing Properties": "economyExistingProperties",
  "Economy Existing Rooms": "economyExistingRooms",
  "Economy Pipeline Properties": "economyPipelineProperties",
  "Economy Pipeline Rooms": "economyPipelineRooms",
  "Company History": "companyHistory",
  "Key Differentiators": "differentiators",
  "Notable Achievements": "achievements",
  "Management Philosophy": "managementPhilosophy",
  "Portfolio Value": "portfolioValue",
  "Annual Revenue Managed": "annualRevenueManaged",
  "Portfolio Growth Rate": "portfolioGrowthRate",
  "Min Property Size": "minPropertySize",
  "Max Property Size": "maxPropertySize",
  "Avg Property Size": "avgPropertySize",
  "RevPAR Improvement": "revparImprovement",
  "Average Occupancy Improvement": "occupancyImprovement",
  "NOI Improvement": "noiImprovement",
  "Owner Retention Rate": "ownerRetention",
  "Average Contract Renewal Rate": "renewalRate",
  "Properties Turned Around": "turnaroundCount",
  "Time to Stabilization": "stabilizationTime",
  "Total Employees": "totalEmployees",
  "Avg On-Site Staff": "avgOnSiteStaff",
  "Regional Teams": "regionalTeams",
  "Avg Experience Years": "avgExperience",
  "Key Leadership": "keyLeadership",
  Certifications: "certifications",
  "Revenue Management Services": "revenueManagementServices",
  "Sales Marketing Support": "salesMarketingSupport",
  "Accounting Reporting": "accountingReporting",
  "Procurement Services": "procurementServices",
  "HR Training Services": "hrTrainingServices",
  "Technology Services": "technologyServices",
  "Design Renovation Support": "designRenovationSupport",
  "Development Services": "developmentServices",
  "New Build Experience": "newBuildExperience",
  "Conversion Experience": "conversionExperience",
  "Turnaround Experience": "turnaroundExperience",
  "Pre-opening Experience": "preOpeningExperience",
  "Pre-Opening Ramp Lead Time (Months)": "preOpeningRampLeadTimeMonths",
  "Transition Experience": "transitionExperience",
  "Stabilized / Ongoing-Operations Experience": "stabilizedExperience",
  "Renovation/Rebrand Experience": "renovationExperience",
  "Additional Experience Types": "additionalExperience",
  "Primary PMS": "primaryPMS",
  "Revenue Management System": "revenueManagementSystem",
  "Accounting System": "accountingSystem",
  "Guest Communication": "guestCommunication",
  "Analytics Platform": "analyticsPlatform",
  "Mobile Check-in": "mobileCheckin",
  "Owner Portal": "ownerPortal",
  "API Integrations": "apiIntegrations",
  "Reporting Frequency": "reportingFrequency",
  "Report Types": "reportTypes",
  "Budget Process": "budgetProcess",
  "Capex Planning": "capexPlanning",
  "CapEx Tolerance": "capexTolerance",
  "Performance Reviews": "performanceReviews",
  "Base Fee Range": "baseFeeRange",
  "Mgmt Fee Min": "mgmtFeeMin",
  "Mgmt Fee Max": "mgmtFeeMax",
  "Mgmt Fee Basis": "mgmtFeeBasis",
  "Mgmt Fee Notes": "mgmtFeeNotes",
  "Incentive Fee Min": "incentiveFeeMin",
  "Incentive Fee Max": "incentiveFeeMax",
  "Incentive Fee Basis": "incentiveFeeBasis",
  "Incentive Fee Notes": "incentiveFeeNotes",
  "Incentive Excess Min": "incentiveExcessMin",
  "Incentive Excess Max": "incentiveExcessMax",
  "Incentive Excess Basis": "incentiveExcessBasis",
  "Incentive Excess Notes": "incentiveExcessNotes",
  "Incentive Fee Structure": "incentiveFeeStructure",
  "Additional Fees": "additionalFees",
  "Additional Fee Details": "additionalFeeDetails",
  "Fee Transparency": "feeTransparency",
  "Performance Adjustments": "performanceAdjustments",
  "Communication Style": "communicationStyle",
  "Owner Involvement": "ownerInvolvement",
  "Operating Collaboration Mode": "operatingCollaborationMode",
  "Decision Making Process": "decisionMaking",
  "Dispute Resolution": "disputeResolution",
  "Owner Advisory Board": "ownerAdvisoryBoard",
  "Owner References": "ownerReferences",
  "Owner Diligence Document Links": "diligenceDocumentLinks",
  "Testimonial Links": "testimonialLinks",
  "Industry Recognition": "industryRecognition",
  "Lender References": "lenderReferences",
  "Major Lenders": "majorLenders",
  "Min Initial Term Qty": "minInitialTermQty",
  "Min Initial Term Length": "minInitialTermLength",
  "Min Initial Term Duration": "minInitialTermDuration",
  "Renewal Option Qty": "renewalOptionQty",
  "Renewal Option Length": "renewalOptionLength",
  "Renewal Option Duration": "renewalOptionDuration",
  "Renewal Notice Qty": "renewalNoticeQty",
  "Renewal Notice Duration": "renewalNoticeDuration",
  "Renewal Structure": "renewalStructure",
  "Renewal Notice Responsibility": "renewalNoticeResponsibility",
  "Renewal Conditions": "renewalConditions",
  "Performance Test Requirement": "performanceTestRequirement",
  "Cure Period Qty": "curePeriodQty",
  "Cure Period Duration": "curePeriodDuration",
  "QA Compliance Requirement": "qaComplianceRequirement",
  "PIP at Renewal": "pipAtRenewal",
  "PIP for Conversions": "pipForConversions",
  "Base Fee Escalation": "baseFeeEscalation",
  "Base Fee Escalation How": "baseFeeEscalationHow",
  "Minimum Fee Floor": "feeMinimumFloor",
  "Minimum Fee Floor Min": "feeMinimumFloorMin",
  "Minimum Fee Floor Max": "feeMinimumFloorMax",
  "Minimum Fee Floor Basis": "feeMinimumFloorBasis",
  "Central Service Allocations": "centralServiceAllocations",
  "Central Service Allocations Notes": "centralServiceAllocationsNotes",
  "Pre-Opening Fees Types": "preOpeningFees",
  "Pre-Opening Fees Notes": "preOpeningFeesNotes",
  "Performance Metrics Used": "performanceMetricsUsed",
  "Performance Lookback Period": "performanceLookbackPeriod",
  "Performance Termination Rights": "performanceTerminationRights",
  "Owner Early Termination Rights": "ownerEarlyTerminationRights",
  "Owner Early Termination Notes": "ownerEarlyTerminationNotes",
  "Termination Fee Structure": "terminationFeeStructure",
  "Termination Fee Structure Notes": "terminationFeeStructureNotes",
  "Key Money / Co-Investment": "keyMoneyCoInvestment",
  "Owner-Funded Reserves Expectations": "ownerFundedReserves",
  "Cap Operator Reimbursable Expenses": "capReimbursableExpenses",
  "Audit Rights Required": "auditRightsRequired",
  "Deal Terms Additional Notes": "dealTermsAdditionalNotes",
  "Typical Contract Length": "typicalContractLength",
  "Early Termination": "earlyTermination",
  "Renewal Terms": "renewalTerms",
  "Customization Willingness": "customizationWillingness",
  "Owner Exit Rights": "ownerExitRights",
  "Performance Guarantees": "performanceGuarantees",
  "Emergency Response": "emergencyResponse",
  "Business Continuity": "businessContinuity",
  "Crisis Experience": "crisisExperience",
  "24/7 Support": "support24x7",
  "Insurance Coverage": "insuranceCoverage",
  "Sustainability Programs": "sustainabilityPrograms",
  "ESG Reporting": "esgReporting",
  "Energy Efficiency": "energyEfficiency",
  "Waste Reduction": "wasteReduction",
  "Carbon Tracking": "carbonTracking",
  "Average Contract Term": "avgContractTerm",
  "Fee Structure": "feeStructure",
  Specializations: "specializations",
  "Technology & Systems": "technology",
  "Owner Testimonials": "testimonials",
  "Additional Notes": "additionalNotes",
  "Ideal Project Types": "idealProjectTypes",
  "Ideal Building Types": "idealBuildingTypes",
  "Ideal Agreement Types": "idealAgreementTypes",
  "Ideal Room Count Min": "idealRoomCountMin",
  "Ideal Room Count Max": "idealRoomCountMax",
  "Ideal Project Size Min": "idealProjectSizeMin",
  "Ideal Project Size Max": "idealProjectSizeMax",
  "Min Lead Time Months": "minLeadTimeMonths",
  "Preferred Owner Type": "preferredOwnerType",
  "Co-Branding Allowed": "coBrandingAllowed",
  "Branded Residences Allowed": "brandedResidencesAllowed",
  "Mixed-Use Allowed": "mixedUseAllowed",
  "Priority Markets": "priorityMarkets",
  "Markets To Avoid": "marketsToAvoid",
  "Market Expansion Comfort": "marketExpansionComfort",
  "Market Expansion Ramp Lead Time (Months)": "marketExpansionRampTimeMonths",
  "Owner Hotel Experience": "ownerHotelExperience",
  "Acceptable Project Stages": "projectStage",
  "Milestone Operator Selection Min Months": "milestoneOperatorSelectionMinMonths",
  "Milestone Construction Start Min Months": "milestoneConstructionStartMinMonths",
  "Milestone Soft Opening Min Months": "milestoneSoftOpeningMinMonths",
  "Milestone Grand Opening Min Months": "milestoneGrandOpeningMinMonths",
  "Date Flexibility": "dateFlexibility",
  "Brand Status Scenarios": "brandStatus",
  "PIP / Repositioning Details": "pipRepositioningDetails",
  "Acceptable Owner Involvement Levels": "ownerInvolvementLevel",
  "Owner Non-Negotiable Types": "ownerNonNegotiableTypes",
  "Owner Non-Negotiables & Decision Rights": "ownerNonNegotiables",
  "Acceptable Fee Expectations vs Market": "feeExpectationVsMarket",
  "CapEx and FF&E Support": "capexSupport",
  "Acceptable Exit Horizon": "exitHorizon",
  "Acceptable Capital Status at Engagement": "capitalStatus",
  "Known Red Flag Items": "knownRedFlags",
  "ESG / Sustainability Expectations": "esgExpectations",
  "Ideal Projects Additional Notes": "idealProjectsAdditionalNotes",
};

/** Alternate Basics column labels → same form keys (older bases / seeds). */
export const BASICS_AIRTABLE_ALIASES_TO_FORM_KEY = {
  /** New-base tables often use snake_case field names; intake HTML uses camelCase `name` attributes. */
  company_name: "companyName",
  Headquarters: "headquarters",
  "Headquarters Location": "headquarters",
  "Contact Email": "contactEmail",
  "Contact Phone": "contactPhone",
  "Certifications Held": "certifications",
  "Energy Efficiency Initiatives": "energyEfficiency",
  "Waste Reduction Programs": "wasteReduction",
  "Carbon Footprint Tracking": "carbonTracking",
  /** Form section titles used as Airtable column names (canonical names are in BASICS_AIRTABLE_TO_FORM_KEY). */
  "Average Years of Industry Experience": "avgExperience",
  "Regional Management Teams": "regionalTeams",
  "Property Types Managed": "propertyTypes",
  "Additional Experience / Location Contexts": "additionalExperience",
  "Emergency Response Plan": "emergencyResponse",
  "Business Continuity Planning": "businessContinuity",
  "24/7 Support Availability": "support24x7",
  "Support 24/7 Availability": "support24x7",
  "Crisis Management Experience": "crisisExperience",
  "Lender References Available": "lenderReferences",
  "Report Types Provided": "reportTypes",
  "Financial Reporting Frequency": "reportingFrequency",
  "Mobile Check-in Capability": "mobileCheckin",
  "Data Analytics Platform": "analyticsPlatform",
  "Decision-Making Process": "decisionMaking",
  "Owner Education/Training Provided": "ownerEducation",
  "Primary PMS System": "primaryPMS",
  "Minimum Property Size": "minPropertySize",
  "Maximum Property Size": "maxPropertySize",
  "Dispute Resolution Approach": "disputeResolution",
  "Total Portfolio Value": "portfolioValue",
  "Average Annual Revenue Managed": "annualRevenueManaged",
  "Owner Non-Negotiables (Types)": "ownerNonNegotiableTypes",
  "ESG / Sustainability Expectations You Prefer Projects to Meet": "esgExpectations",
  "Average NOI Improvement": "noiImprovement",
  "Red Flag Items That Typically Make You Decline or Proceed With Caution": "knownRedFlags",
  "Featured Differentiators": "differentiators",
  "Major Lenders Worked With": "majorLenders",
  "Mixed-Use Development Allowed": "mixedUseAllowed",
  /**
   * "3rd Party Operator - Footprint" often uses these labels; same form keys as Basics canonical columns.
   * applyFootprintFieldsToPrefill uses airtableBasicsFieldsToPrefill(footprintRow) so these must resolve.
   */
  "Location Type % Urban": "locationTypeUrban",
  "Location Type % Suburban": "locationTypeSuburban",
  "Location Type % Resort": "locationTypeResort",
  "Location Type % Airport": "locationTypeAirport",
  "Location Type % Small Metro/Town": "locationTypeSmallMetro",
  "Location Type % Interstate": "locationTypeInterstate",
  "Location Type % Total": "locationTypeTotal",
  "Location Type Urban": "locationTypeUrban",
  "Location Type Suburban": "locationTypeSuburban",
  "Location Type Resort": "locationTypeResort",
  "Location Type Airport": "locationTypeAirport",
  /** Small-metro % → Highway column; interstate % → Other column (Basics canonical names). */
  "Location Type Highway": "locationTypeSmallMetro",
  "Location Type Other": "locationTypeInterstate",
  "Specific Markets/Cities": "specificMarkets",
  "Chain Scales Supported": "chainScalesSupported",
  /** Long-form region CSV on some Footprint bases */
  Regions: "regions",
  /** Footprint label variants for exits/date fields. */
  "Exits/Deflaggings (Past 24 Months)": "exitsDeflaggings",
  "Exits / Deflaggings (Units) in Past 24 Months": "exitsDeflaggings",
  "Figures as of": "figuresAsOf",
  "Figures as of ": "figuresAsOf",
  "Figures As Of ": "figuresAsOf",
  "Pre-opening Ramp Lead Time (Months)": "preOpeningRampLeadTimeMonths",
  "Pre-Opening Experience": "preOpeningExperience",
  "Stabilized Experience": "stabilizedExperience",
  "Renovation Experience": "renovationExperience",
  "Typical Owner Response Time": "ownerResponseTime",
  "Mgmt Fee Min %": "mgmtFeeMin",
  "Mgmt Fee Max %": "mgmtFeeMax",
  "Incentive Fee Min %": "incentiveFeeMin",
  "Incentive Fee Max %": "incentiveFeeMax",
  "Incentive Excess Min %": "incentiveExcessMin",
  "Incentive Excess Max %": "incentiveExcessMax",
  // Footprint short staff labels used on some bases.
  "Luxury Avg Staff": "luxuryAvgStaff",
  "Upper Upscale Avg On-Site Staff Per Property": "upperUpscaleAvgStaff",
  "Upper Upscale Avg Staff": "upperUpscaleAvgStaff",
  "Upscale Avg Staff": "upscaleAvgStaff",
  "Upper Midscale Avg On-Site Staff Per Property": "upperMidscaleAvgStaff",
  "Upper Midscale Avg Staff": "upperMidscaleAvgStaff",
  "Midscale Avg On-Site Staff Per Property": "midscaleAvgStaff",
  "Midscale Avg Staff": "midscaleAvgStaff",
  "Economy Avg Staff": "economyAvgStaff",
  "Renewal Rate": "renewalRate",
  "Occupancy Improvement": "occupancyImprovement",
  "Markets to Avoid": "marketsToAvoid",
  "Milestone Min Months - First Discussion to Operator Selection": "milestoneOperatorSelectionMinMonths",
  "Milestone Min Months - Operator Selection to Construction Start": "milestoneConstructionStartMinMonths",
  "Milestone Min Months - Pre-Opening Ramp to Soft Opening": "milestoneSoftOpeningMinMonths",
  "Milestone Min Months - Soft Opening to Grand Opening": "milestoneGrandOpeningMinMonths",

  /** Legacy Geo-prefixed room columns (deduped in Airtable). */
  "Geo NA Existing Rooms": "geo_na_existing_rooms",
  "Geo NA Pipeline Rooms": "geo_na_pipeline_rooms",
  "Geo CALA Existing Hotels": "geo_cala_existing_hotels",
  "Geo CALA Existing Rooms": "geo_cala_existing_rooms",
  "Geo CALA Pipeline Hotels": "geo_cala_pipeline_hotels",
  "Geo CALA Pipeline Rooms": "geo_cala_pipeline_rooms",
  "Geo MEA Existing Rooms": "geo_mea_existing_rooms",
  "Geo MEA Pipeline Rooms": "geo_mea_pipeline_rooms",
  "Geo APAC Existing Rooms": "geo_apac_existing_rooms",
  "Geo APAC Pipeline Rooms": "geo_apac_pipeline_rooms",

  /** Short geo column labels used on some Footprint bases. */
  "NA Existing Hotels": "geo_na_existing_hotels",
  "NA Existing Rooms": "geo_na_existing_rooms",
  "NA Pipeline Hotels": "geo_na_pipeline_hotels",
  "NA Pipeline Rooms": "geo_na_pipeline_rooms",

  "CALA Existing Hotels": "geo_cala_existing_hotels",
  "CALA Existing Rooms": "geo_cala_existing_rooms",
  "CALA Pipeline Hotels": "geo_cala_pipeline_hotels",
  "CALA Pipeline Rooms": "geo_cala_pipeline_rooms",

  "EU Existing Hotels": "geo_eu_existing_hotels",
  "EU Existing Rooms": "geo_eu_existing_rooms",
  "EU Pipeline Hotels": "geo_eu_pipeline_hotels",
  "EU Pipeline Rooms": "geo_eu_pipeline_rooms",

  "MEA Existing Hotels": "geo_mea_existing_hotels",
  "MEA Existing Rooms": "geo_mea_existing_rooms",
  "MEA Pipeline Hotels": "geo_mea_pipeline_hotels",
  "MEA Pipeline Rooms": "geo_mea_pipeline_rooms",

  "APAC Existing Hotels": "geo_apac_existing_hotels",
  "APAC Existing Rooms": "geo_apac_existing_rooms",
  "APAC Pipeline Hotels": "geo_apac_pipeline_hotels",
  "APAC Pipeline Rooms": "geo_apac_pipeline_rooms",

  // Optional short totals if present. (Client also recomputes totals from existing + pipeline.)
  "NA Total Hotels": "geo_na_total_hotels",
  "NA Total Rooms": "geo_na_total_rooms",
  "CALA Total Hotels": "geo_cala_total_hotels",
  "CALA Total Rooms": "geo_cala_total_rooms",
  "EU Total Hotels": "geo_eu_total_hotels",
  "EU Total Rooms": "geo_eu_total_rooms",
  "MEA Total Hotels": "geo_mea_total_hotels",
  "MEA Total Rooms": "geo_mea_total_rooms",
  "APAC Total Hotels": "geo_apac_total_hotels",
  "APAC Total Rooms": "geo_apac_total_rooms",
};

const MULTI_ARRAY_KEYS = new Set([
  "brands",
  "chainScalesSupported",
  "propertyTypes",
  "additionalExperience",
  "reportTypes",
  "performanceMetricsUsed",
  "preOpeningFees",
  "idealProjectTypes",
  "idealBuildingTypes",
  "idealAgreementTypes",
  "projectStage",
  "priorityMarkets",
  "marketsToAvoid",
  "brandStatus",
  "ownerHotelExperience",
  "ownerInvolvementLevel",
  "ownerNonNegotiableTypes",
  "feeExpectationVsMarket",
  "exitHorizon",
  "capitalStatus",
  "revenueManagementServices",
  "salesMarketingSupport",
  "accountingReporting",
  "procurementServices",
  "hrTrainingServices",
  "technologyServices",
  "designRenovationSupport",
  "developmentServices",
  "additionalFees",
]);

const HIDDEN_COMMA_KEYS = new Set(["regions", "chainScale"]);

const NUMERIC_STRING_KEYS = new Set([
  "yearEstablished",
  "yearsInBusiness",
  "numberOfMarkets",
  "exitsDeflaggings",
  "totalProperties",
  "totalRooms",
  "ownerReferences",
  "ownerSatisfactionScore",
  "minPropertySize",
  "maxPropertySize",
  "avgPropertySize",
  "revparImprovement",
  "occupancyImprovement",
  "noiImprovement",
  "ownerRetention",
  "renewalRate",
  "stabilizationTime",
  "totalEmployees",
  "avgOnSiteStaff",
  "regionalTeams",
  "avgExperience",
  "preOpeningRampLeadTimeMonths",
  "idealRoomCountMin",
  "idealRoomCountMax",
  "idealProjectSizeMin",
  "idealProjectSizeMax",
  "minLeadTimeMonths",
  "marketExpansionRampTimeMonths",
  "milestoneOperatorSelectionMinMonths",
  "milestoneConstructionStartMinMonths",
  "milestoneSoftOpeningMinMonths",
  "milestoneGrandOpeningMinMonths",
  "numberOfBrands",
]);

const GEO_KEYS = new Set(
  Object.values(BASICS_AIRTABLE_TO_FORM_KEY).filter((k) => /^geo_/.test(k))
);

function isEmptyCell(v) {
  if (v == null) return true;
  if (typeof v === "string" && v.trim() === "") return true;
  if (Array.isArray(v) && v.length === 0) return true;
  return false;
}

function isEmptyPrefillOverlayValue(v) {
  if (v == null) return true;
  if (typeof v === "string" && v.trim() === "") return true;
  if (Array.isArray(v) && v.length === 0) return true;
  return false;
}

function normalizeDateForDateInput(raw) {
  const s = formatListValue(raw);
  if (!s) return "";
  // Airtable may return full ISO datetime strings; date inputs require YYYY-MM-DD.
  const m = String(s).trim().match(/^(\d{4}-\d{2}-\d{2})/);
  return m ? m[1] : String(s).trim();
}

/**
 * Overlays linked Footprint row values onto prefill (same Airtable column names as Basics for shared fields).
 * Footprint wins when it has a non-empty value so the form reflects the satellite table.
 *
 * @param {Record<string, unknown>} prefill Mutated in place.
 * @param {Record<string, unknown>} footprintFields Airtable `fields` from "3rd Party Operator - Footprint"
 */
export function applyFootprintFieldsToPrefill(prefill, footprintFields) {
  if (!footprintFields || typeof footprintFields !== "object") return;
  const fromFoot = airtableBasicsFieldsToPrefill(footprintFields);
  for (const [k, v] of Object.entries(fromFoot)) {
    if (isEmptyPrefillOverlayValue(v)) continue;
    prefill[k] = v;
  }
}

/**
 * @param {Record<string, unknown>} fields Airtable Basics `fields`
 * @returns {Record<string, unknown>} prefill keys matching form `name` attributes
 */
export function airtableBasicsFieldsToPrefill(fields) {
  const f = fields || {};
  const out = {};
  const normalizedFieldNameToActual = {};
  for (const key of Object.keys(f)) {
    const n = String(key || "")
      .toLowerCase()
      .replace(/\s+/g, " ")
      .trim();
    if (n && !normalizedFieldNameToActual[n]) normalizedFieldNameToActual[n] = key;
  }

  function assignFromAirtable(airtableName, formKey) {
    let sourceKey = null;
    if (Object.prototype.hasOwnProperty.call(f, airtableName)) {
      sourceKey = airtableName;
    } else {
      const normalizedLookup = String(airtableName || "")
        .toLowerCase()
        .replace(/\s+/g, " ")
        .trim();
      sourceKey = normalizedFieldNameToActual[normalizedLookup] || null;
    }
    if (!sourceKey) return;
    const raw = f[sourceKey];
    if (isEmptyCell(raw)) return;

    if (formKey === "brandsPortfolioDetail") {
      const parsed = safeParseJsonArray(raw);
      if (parsed.length) out[formKey] = parsed;
      return;
    }

    if (MULTI_ARRAY_KEYS.has(formKey)) {
      const arr = Array.isArray(raw) ? raw.map((x) => formatListValue(x)).filter(Boolean) : parseMultiValue(raw);
      if (arr.length) out[formKey] = arr;
      return;
    }

    if (HIDDEN_COMMA_KEYS.has(formKey)) {
      const parts = Array.isArray(raw) ? raw.map((x) => formatListValue(x)).filter(Boolean) : parseMultiValue(raw);
      if (parts.length) out[formKey] = parts.join(", ");
      return;
    }

    if (formKey === "figuresAsOf") {
      const normalized = normalizeDateForDateInput(raw);
      if (normalized !== "") out[formKey] = normalized;
      return;
    }

    if (formKey === "brands") {
      const arr = Array.isArray(raw) ? raw.map((x) => formatListValue(x)).filter(Boolean) : parseMultiValue(formatListValue(raw));
      if (arr.length) out[formKey] = arr;
      return;
    }

    if (GEO_KEYS.has(formKey) || NUMERIC_STRING_KEYS.has(formKey)) {
      if (typeof raw === "number" && Number.isFinite(raw)) {
        out[formKey] = String(raw);
        return;
      }
      const s = formatListValue(raw);
      if (s !== "") out[formKey] = s;
      return;
    }

    if (typeof raw === "number" && Number.isFinite(raw)) {
      out[formKey] = String(raw);
      return;
    }
    if (Array.isArray(raw)) {
      out[formKey] = formatListValue(raw);
      return;
    }
    out[formKey] = String(raw).trim();
  }

  for (const [at, fk] of Object.entries(BASICS_AIRTABLE_TO_FORM_KEY)) {
    assignFromAirtable(at, fk);
  }
  for (const [at, fk] of Object.entries(BASICS_AIRTABLE_ALIASES_TO_FORM_KEY)) {
    if (out[fk] != null && out[fk] !== "") continue;
    assignFromAirtable(at, fk);
  }

  return out;
}
