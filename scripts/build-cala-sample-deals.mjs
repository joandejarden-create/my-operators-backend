#!/usr/bin/env node
/**
 * Generate CALA primary demo sample-deal fixtures (11 remaining + optional regen).
 * Usage: node scripts/build-cala-sample-deals.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { CALA_DEALS_PART2 } from "./cala-sample-deals-data.mjs";
import { normalizeDealSetupFields } from "../lib/deal-setup-form-value-normalize.js";
import { applyCalaIntakeCompletion } from "../lib/cala-sample-intake-completion.js";
import { sanitizeCalaDealConfig } from "../lib/demo-intake-copy-sanitize.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const OUT_DIR = path.join(ROOT, "fixtures", "sample-deals");

const CALA_DEAL_STATUS = process.env.CALA_SAMPLE_DEALS_STATUS || "In Review";

/** @param {object} cfg */
function buildFixture(cfg) {
  cfg = sanitizeCalaDealConfig(cfg);
  const refFields = { ...cfg.referenceFields };
  const ficFields = {
    "Project Name": cfg.projectName,
    "Property Name": cfg.projectName,
    "Project Type": cfg.projectType,
    "Stage of Development": cfg.stage,
    "Expected Opening or Rebranding Date": cfg.openingDate,
    "Who should receive bids for this project?": cfg.bidsRecipient,
    "Full Address": cfg.fictionalAddress,
    "City & State": cfg.cityState,
    Country: cfg.country,
    "Ownership Type": cfg.ownershipType || "Fee Simple",
    "Ownership Structure": cfg.ownershipStructure,
    "Ownership/Brand History or Track Record": cfg.ownerTrackRecord,
    "Portfolio Size": cfg.portfolioSize,
    "Has there ever been a franchise, branded management, affiliation or similar agreement pertaining to the proposed hotel or site?":
      cfg.priorFranchise || "No",
    "Is the hotel currently branded?": cfg.currentlyBranded,
    "Current Brand Affiliation": cfg.currentBrand,
    "Is the hotel currently managed by a third-party operator?": cfg.currentlyManaged,
    "Operator Name Current":
      cfg.operatorCurrent ??
      (cfg.currentlyManaged === "Yes"
        ? cfg.operatorNameDefault || "Latin America Lodging Partners"
        : cfg.currentlyManaged === "No" && cfg.currentlyBranded === "No"
          ? ""
          : ""),
    "Are you open to considering other brands with favorable terms?": cfg.openOtherBrands || "Yes",
    "Have you worked with any of your preferred brands/operators before?":
      cfg.workedWithPreferred || "No",
    "Plan to Self-Manage or Hire Third Party?": cfg.operatorPlan,
    "Preferred Brands (up to 4)": cfg.preferredBrands,
    "Preferred Chain Scales": cfg.preferredChainScales,
    "Soft vs Hard Brand Preference": cfg.softHardPreference,
    "Hotel Chain Scale": cfg.targetChainScale,
    "Hotel Type": cfg.hotelType,
    "Hotel Service Model": cfg.serviceModel,
    "Hotel Submarket & Location": cfg.submarket,
    "Total Number of Rooms/Keys": String(cfg.keys),
    "Number of Standard Rooms": String(cfg.standardRooms),
    "Number of Suites": String(cfg.suites),
    "Building Type": cfg.buildingType,
    "Number of Stories": String(cfg.stories),
    "Meeting Space": cfg.meetingSpace,
    "Meeting Space Unit": "Sq. Ft.",
    "Number of Meeting Rooms": String(cfg.meetingRooms),
    "F&B Outlets?": cfg.fbOutlets,
    "Number of F&B Outlets": cfg.fbCount != null && cfg.fbCount !== "" ? String(cfg.fbCount) : "",
    "Additional Amenities": cfg.amenities,
    "Parking Amenities?": cfg.parking,
    "Zoned for Hotel Development": cfg.zonedHotel || "Yes",
    "Zoning Status": cfg.zoningStatus || "Conditional / In Progress",
    "Current Form of Site Control": cfg.siteControl,
    "Total Site Size": cfg.siteSize,
    "Total Site Size Unit": cfg.siteSizeUnit || "Sq. Ft.",
    "Site/Development Restrictions?": cfg.siteRestrictions || "No",
    "Access to Transit or Highway": cfg.transitAccess,
    "Primary Demand Drivers": cfg.demandDrivers,
    "Primary Demand Drivers Other": cfg.demandDriversOther || "",
    "Key Competitors": cfg.keyCompetitors,
    "Group vs Transient Mix": cfg.mix || "",
    "Estimated or Actual RevPAR": cfg.revpar,
    "Regulatory or Permitting Issues?": cfg.regulatory || "No",
    "Total Project Cost Range": cfg.projectCost,
    "Equity vs Debt Split": cfg.equityDebt,
    "PIP / CapEx Status": cfg.pipStatus,
    "PIP Budget Range (if conversion)": cfg.pipBudget,
    "IRR/Yield Goals": cfg.irrGoals,
    "Preferred Deal Structure": cfg.dealStructure,
    "Lease Type": cfg.leaseType || "",
    "Open to Outside Capital or Partnerships?": cfg.openCapital || "Yes",
    "Would you like to filter out brands without key money?": "No",
    "Target Guest Segment": cfg.targetGuest,
    "Brand Flexibility vs Prestige": cfg.brandFlexibility,
    "Open to Soft Brand First Then Reflag?": cfg.softFirstReflag || "Maybe",
    "Planned Hold Period": cfg.holdPeriod,
    "Primary Goal for the Hotel": cfg.primaryGoal,
    "Top 3 Success Metrics": cfg.successMetrics,
    "Top Priorities for Project": cfg.priorities,
    "Top Concerns for this Project": cfg.concerns,
    "Top 3 Deal Breakers": cfg.dealBreakers,
    "Top 3 Deal Breakers Other": cfg.dealBreakersOther || "",
    "Must-haves From Brand or Operator": cfg.mustHaves,
    "Must-haves From Brand or Operator Other": cfg.mustHavesOther || "",
    "Minimum Operator Experience (years)": cfg.minOperatorYears || "10+ Years",
    "Preferred Third-Party Operators (names)": cfg.operatorNames || "",
    "Services Required From Operator": cfg.operatorServices,
    "Level of Involvement in Day-to-Day Ops": cfg.ownerInvolvement,
    "Decision Timeline for Brand/Operator": cfg.decisionTimeline,
    "Proposal Deadline": cfg.proposalDeadline,
    "Financial Model Available?": cfg.financialModel || "Yes",
    "Legal Support Needed?": cfg.legalSupport || "Yes — Connect me With a Legal Advisor",
    "Working with Broker/Advisor?": cfg.broker,
    "Would you like to receive regular updates?": cfg.regularUpdates || "Weekly Summary",
    "Company Executive Summary": cfg.executiveSummary,
    "Main Contact Name": cfg.contactName,
    "Entity or Company Name": cfg.ownerEntity,
    "Email Address": cfg.contactEmail,
    "Company HQ Location": cfg.hqLocation,
    ...cfg.extraFictionalFields,
  };

  applyCalaIntakeCompletion(refFields, { ...cfg, layer: "reference" });
  applyCalaIntakeCompletion(ficFields, cfg);

  const refNorm = normalizeDealSetupFields(refFields).fields;
  const ficNorm = normalizeDealSetupFields(ficFields).fields;
  Object.assign(refFields, refNorm);
  Object.assign(ficFields, ficNorm);

  const fieldSources = {
    "Project Name": { sourceType: "fictional_sample_assumption", layer: "fictional_deal" },
    "Property Name": { sourceType: "fictional_sample_assumption", layer: "fictional_deal" },
    "Total Number of Rooms/Keys": {
      sourceType: "fictional_sample_assumption",
      layer: "fictional_deal",
      note: cfg.keysNote,
      sourceUrl: cfg.primarySourceUrl,
    },
    "Full Address": {
      sourceType: "fictional_sample_assumption",
      layer: "fictional_deal",
      note: "Site address for this opportunity",
    },
    "Preferred Brands (up to 4)": {
      sourceType: "fictional_sample_assumption",
      layer: "fictional_deal",
      note: "Owner review set — not recommendations",
    },
    ...cfg.extraFieldSources,
  };

  for (const g of cfg.intentionalGaps) {
    if (!fieldSources[g.field]) {
      fieldSources[g.field] = {
        sourceType: "needs_validation",
        layer: "fictional_deal",
        note: g.reason,
      };
    }
  }

  const airtableRows = [
    {
      table: "Deals",
      field: "Deal Status",
      value: CALA_DEAL_STATUS,
      sourceType: "fictional_sample_assumption",
      layer: "fictional_deal",
    },
    {
      table: "Location & Property",
      field: "Full Address",
      value: cfg.fictionalAddress,
      sourceType: "fictional_sample_assumption",
      layer: "fictional_deal",
    },
    {
      table: "Location & Property",
      field: "Hotel Submarket & Location",
      value: cfg.submarket,
      sourceType: cfg.submarketSourceType || "inferred_from_reference",
      layer: "reference_property",
      sourceUrl: cfg.primarySourceUrl,
    },
    ...(cfg.extraAirtableRows || []),
    {
      table: "Strategic Intent - Operational - Key Challenges",
      field: "Soft vs Hard Brand Preference",
      value: cfg.softHardPreference,
      sourceType: "fictional_sample_assumption",
      layer: "fictional_deal",
    },
    {
      table: "Strategic Intent - Operational - Key Challenges",
      field: "Top Concerns for this Project",
      value: cfg.concerns,
      sourceType: "fictional_sample_assumption",
      layer: "fictional_deal",
      notes: "Commercial incentive assumptions to confirm — not an offer",
    },
    {
      table: "Contact & Uploads",
      field: "Main Contact Name",
      value: cfg.contactName,
      sourceType: "fictional_sample_assumption",
      layer: "fictional_deal",
    },
    {
      table: "Contact & Uploads",
      field: "Email Address",
      value: cfg.contactEmail,
      sourceType: "fictional_sample_assumption",
      layer: "fictional_deal",
    },
  ];

  return {
    meta: {
      isSample: true,
      sampleId: cfg.sampleId,
      sampleTier: "demo",
      calaDemoSet: true,
      calaDemoSetRole: "primary",
      market: cfg.market,
      region: "CALA",
      expectedReadinessStage: cfg.expectedReadinessStage,
      createdFor: cfg.createdFor,
    },
    disclaimer: "",
    referenceProperty: {
      displayLabel: cfg.displayLabel,
      publicName: cfg.publicName,
      secondaryReferenceHotels: cfg.secondaryHotels,
      sources: cfg.sources,
      factsSummary: cfg.factsSummary,
      fields: refFields,
    },
    fictionalDeal: {
      projectName: cfg.projectName,
      ownerEntity: cfg.ownerEntity,
      fields: ficFields,
    },
    fieldSources,
    airtableRows,
    targetListRows: cfg.targetListRows,
    intentionalGaps: cfg.intentionalGaps,
    expectedReadinessStage: cfg.expectedReadinessStage,
    expectedBrandAlignmentBehavior: cfg.expectedBrandAlignmentBehavior,
  };
}

const CALA_DEALS = [
  {
    file: "proyecto-reforma-urban-conversion.example.json",
    sampleId: "cala-proyecto-reforma-urban-conversion-001",
    projectName: "Proyecto Reforma Urban Conversion",
    ownerEntity: "Reforma Urban Hospitality MX, S.A. de C.V. (fictional sample)",
    contactName: "Alejandro Morales",
    contactEmail: "alejandro.morales@dealality.sample",
    market: "Mexico City — Reforma / Centro corridor",
    cityState: "Mexico City, CDMX",
    country: "Mexico",
    publicName: "Downtown Mexico, a Member of Design Hotels",
    secondaryHotels: ["Círculo Mexicano", "Umbral, Curio Collection by Hilton"],
    displayLabel: "Public reference comp — urban upscale lifestyle hotel, Mexico City Centro/Reforma corridor",
    primarySourceUrl: "https://www.hilton.com/en/hotels/mexubqq-umbral-curio-collection/",
    sources: [
      { label: "Umbral, Curio Collection by Hilton — public hotel page", url: "https://www.hilton.com/en/hotels/mexubqq-umbral-curio-collection/", accessed: "2026-05-21", note: "Secondary comp — collection/soft-brand urban prototype" },
      { label: "Círculo Mexicano — public property website", url: "https://www.circulomexicano.com/", accessed: "2026-05-21", note: "Secondary comp — boutique urban" },
      { label: "Downtown Mexico — Design Hotels member listing", url: "https://www.designhotels.com/hotels/2284-downtown-mexico", accessed: "2026-05-21", note: "Primary reference — urban lifestyle positioning" },
    ],
    factsSummary: "Urban upscale/lifestyle prototype in CDMX historic core; multi-outlet F&B and design-forward guestrooms typical of Design Hotels / collection class; secondary comps include boutique Círculo Mexicano and Curio Umbral.",
    referenceFields: {
      "City & State": "Mexico City, CDMX",
      Country: "Mexico",
      "Hotel Submarket & Location": "Centro Histórico / Reforma corridor",
      "Hotel Chain Scale": "Upscale",
      "Hotel Type": "Urban",
      "Hotel Service Model": "Lifestyle / Boutique",
      "Total Number of Rooms/Keys": "140",
      "Number of Standard Rooms": "118",
      "Number of Suites": "22",
      "Building Type": "Historic / Mixed-Use",
      "Number of Stories": "6",
      "Meeting Space": 3200,
      "Number of Meeting Rooms": "2",
      "F&B Outlets?": "Yes",
      "Number of F&B Outlets": "2",
      "F&B Program Type": "Signature restaurant + bar (public listing class)",
      "Additional Amenities": "Rooftop bar, fitness, design-led public spaces (public listing class)",
      "Parking Amenities?": "Limited urban parking (inferred)",
      "Primary Demand Drivers": "Corporate / Business Travel, Leisure / Tourism, Meetings",
      "Access to Transit or Highway": "Metro and urban transit access (inferred)",
    },
    keys: 142,
    standardRooms: 120,
    suites: 22,
    keysNote: "Planned 142 keys (130–150 band); reference comps vary by property",
    projectType: "Conversion / Reflag",
    stage: "Entitlements in Process",
    openingDate: "2027-12-01",
    bidsRecipient: "Both brands and third-party operators",
    fictionalAddress: "Av. Reforma Demo 218, Col. Juárez, Ciudad de México 06600, Mexico",
    submarket: "Centro Histórico / Reforma — urban conversion (sample)",
    buildingType: "Historic / Mixed-Use",
    stories: 7,
    meetingSpace: 2800,
    meetingRooms: 3,
    currentlyBranded: "Yes",
    currentBrand: "Independent (legacy urban flag)",
    workedWithPreferred: "No",
    currentlyManaged: "Yes",
    operatorPlan: "Hire third-party operator (sample)",
    preferredBrands: "Curio Collection by Hilton, Autograph Collection, Hyatt Unbound Collection, Tapestry Collection",
    preferredChainScales: "Upscale, Upper Upscale",
    softHardPreference: "Open to soft/collection first; hard brand if economics justify",
    targetChainScale: "Upscale",
    hotelType: "Urban",
    serviceModel: "Lifestyle / Boutique",
    siteControl: "Leasehold — term sheet executed (sample)",
    siteSize: "0.6",
    siteSizeUnit: "Acres",
    transitAccess: "Metro access assumed; loading dock constraints TBD",
    demandDrivers: "Corporate / Business Travel, Leisure / Tourism, Meetings",
    keyCompetitors: "",
    revpar: "",
    projectCost: "USD 52–68M (sample)",
    equityDebt: "50% equity / 50% debt (sample)",
    pipStatus: "",
    pipBudget: "",
    dealStructure: "Franchise + 3rd Party Mgmt.",
    targetGuest: "Urban creative/leisure, corporate weekday demand",
    brandFlexibility: "Prioritize identity preservation and flexible standards",
    primaryGoal: "Maximize Cash Flow",
    priorities: "Brand credibility, Distribution, Operator capability, Standards flexibility",
    concerns: "PIP/capex scope, Soft vs hard brand path, Competitive set completeness, Commercial incentive assumptions",
    dealBreakers: ["Rigid prototype that eliminates local character", "Uncapped PIP requirements", "Long lock-in without performance tests"],
    mustHaves: ["Design approval collaboration", "Flexible F&B standards", "Experienced urban operator (e.g. 10+ years)"],
    operatorServices: "Revenue management, Sales, Accounting, HR, Marketing",
    ownerInvolvement: "Active asset management (sample)",
    decisionTimeline: "Q1 2027",
    proposalDeadline: "2026-11-30",
    executiveSummary: "Fictional CDMX owner repositioning an urban hospitality asset toward upscale/lifestyle affiliation with moderate capex sensitivity and interest in soft/collection pathways.",
    broker: "Yes",
    expectedReadinessStage: "Advancing",
    intentionalGaps: [
      { field: "PIP / CapEx Status", reason: "Capex/PIP budget incomplete", demoWeakness: true },
      { field: "PIP Budget Range (if conversion)", reason: "Conversion budget range not finalized", demoWeakness: true },
      { field: "Soft vs Hard Brand Preference", reason: "Strategic preference still open", demoWeakness: true },
      { field: "Key Competitors", reason: "Competitive set incomplete", demoWeakness: true },
    ],
    extraFieldSources: {
      "PIP / CapEx Status": { sourceType: "needs_validation", layer: "fictional_deal", note: "Intentional gap" },
      "Meeting Space": { sourceType: "fictional_sample_assumption", layer: "fictional_deal", note: "Sample conversion program" },
    },
    extraAirtableRows: [
      { table: "Market - Performance - Deal & Capital Structure", field: "Key Competitors", value: "", sourceType: "needs_validation", layer: "fictional_deal", notes: "Intentional gap" },
      { table: "Market - Performance - Deal & Capital Structure", field: "PIP / CapEx Status", value: "", sourceType: "needs_validation", layer: "fictional_deal", notes: "Intentional gap" },
    ],
    targetListRows: [
      { brandName: "Curio Collection by Hilton", parentCompany: "Hilton", whyInReviewSet: "Owner preference; Umbral comp — collection urban path", sourceType: "fictional_sample_assumption", reviewSetSource: "owner_preferred" },
      { brandName: "Autograph Collection", parentCompany: "Marriott International", whyInReviewSet: "Owner preference; soft/collection comparison", sourceType: "fictional_sample_assumption", reviewSetSource: "owner_preferred" },
      { brandName: "Tapestry Collection by Hilton", parentCompany: "Hilton", whyInReviewSet: "Pipeline — soft brand flexibility stress-test", sourceType: "fictional_sample_assumption", reviewSetSource: "pipeline" },
      { brandName: "Hyatt Unbound Collection", parentCompany: "Hyatt", whyInReviewSet: "Owner preference; lifestyle pathway", sourceType: "fictional_sample_assumption", reviewSetSource: "owner_preferred" },
      { brandName: "MGallery Hotel Collection", parentCompany: "Accor", whyInReviewSet: "Sample demo — international collection path", sourceType: "fictional_sample_assumption", reviewSetSource: "sample_demo" },
      { brandName: "Marriott Hotels", parentCompany: "Marriott International", whyInReviewSet: "Sample demo — hard-brand contrast vs collection preference", sourceType: "fictional_sample_assumption", reviewSetSource: "sample_demo" },
      { brandName: "Canopy by Hilton", parentCompany: "Hilton", whyInReviewSet: "Pipeline — lifestyle with more structure than Curio", sourceType: "fictional_sample_assumption", reviewSetSource: "pipeline" },
    ],
    expectedBrandAlignmentBehavior: {
      summary: "Urban conversion with soft/hard openness; expect stronger collection/soft signals for Curio, Autograph, Unbound; conditional for full-service hard brands.",
      dominantThemes: ["Conversion pathway", "Chain scale upscale", "Standards flexibility", "Owner preference inputs"],
      likelyHigherAlignmentBrands: ["Curio Collection by Hilton", "Autograph Collection", "Hyatt Unbound Collection"],
      likelyConditionalBrands: ["Marriott Hotels", "Canopy by Hilton", "MGallery Hotel Collection"],
      keyConsiderationPatterns: ["Collection-style path if owner wants distribution with identity preserved", "Validate PIP and soft vs hard preference before outreach"],
      rationaleLayerExpectations: { ownerFacingParagraph: true, technicalFactorsDeemphasized: true, commercialIncentiveSoftening: true, perBrandQuestionsNotGeneric: true },
      validationScenarios: ["Fill PIP budget — readiness improves", "Clarify soft vs hard — alignment questions narrow"],
    },
  },
  {
    file: "playa-dorada-resort-repositioning.example.json",
    sampleId: "cala-playa-dorada-resort-repositioning-001",
    projectName: "Playa Dorada Resort Repositioning",
    ownerEntity: "Playa Dorada Resort Holdings, S.R.L. (fictional sample)",
    contactName: "Carmen Reyes",
    contactEmail: "carmen.reyes@dealality.sample",
    market: "Punta Cana / Bávaro, Dominican Republic",
    cityState: "Punta Cana, La Altagracia",
    country: "Dominican Republic",
    publicName: "W Punta Cana, Adult All-Inclusive",
    secondaryHotels: ["Hyatt Ziva Cap Cana", "Lopesan Costa Bávaro Resort Spa & Casino"],
    displayLabel: "Public reference comp — adult all-inclusive beach resort, Punta Cana",
    primarySourceUrl: "https://www.hyatt.com/en-US/hotel/dominican-republic/hyatt-ziva-cap-cana/pujif",
    sources: [
      { label: "W Punta Cana — Marriott public listing", url: "https://www.marriott.com/en-us/hotels/pujwh-w-punta-cana-adult-all-inclusive-resort/overview/", accessed: "2026-05-21", note: "Primary AI resort reference" },
      { label: "Hyatt Ziva Cap Cana — public hotel page", url: "https://www.hyatt.com/en-US/hotel/dominican-republic/hyatt-ziva-cap-cana/pujif", accessed: "2026-05-21", note: "Secondary AI / resort comp" },
      { label: "Lopesan Costa Bávaro — public resort profile", url: "https://www.lopesan.com/en/hotels/dominican-republic/costa-bavaro/", accessed: "2026-05-21", note: "Secondary large-format beach resort comp" },
    ],
    factsSummary: "Large-format beach resort with extensive F&B and entertainment programming; adult all-inclusive and premium resort comps in Cap Cana/Bávaro submarket.",
    referenceFields: {
      "City & State": "Punta Cana, La Altagracia",
      Country: "Dominican Republic",
      "Hotel Submarket & Location": "Bávaro / Cap Cana beach resort strip",
      "Hotel Chain Scale": "Luxury",
      "Hotel Type": "Resort",
      "Hotel Service Model": "All-Inclusive Resort",
      "Total Number of Rooms/Keys": "520",
      "Number of Standard Rooms": "480",
      "Number of Suites": "40",
      "Building Type": "Low-Rise Resort",
      "Number of Stories": "4",
      "Meeting Space": 15000,
      "Number of Meeting Rooms": "6",
      "F&B Outlets?": "Yes",
      "Number of F&B Outlets": "8",
      "F&B Program Type": "Multiple restaurants + bars; all-inclusive programming (public class)",
      "Additional Amenities": "Pools, spa, beach frontage, entertainment venues (public class)",
      "Parking Amenities?": "Resort parking (inferred)",
      "Primary Demand Drivers": "Leisure / Tourism, Weddings, Groups",
    },
    keys: 224,
    standardRooms: 200,
    suites: 24,
    keysNote: "Planned 224 keys (200–240 band); public AI resorts often larger",
    projectType: "Conversion / Reflag",
    stage: "Stabilized Operating Asset",
    openingDate: "2027-03-01",
    bidsRecipient: "Both brands and third-party operators",
    fictionalAddress: "Carretera Playa Dorada Demo Km 4, Punta Cana 23302, Dominican Republic",
    submarket: "Bávaro — beach resort repositioning (sample)",
    buildingType: "Low-Rise Resort",
    stories: 5,
    meetingSpace: 12000,
    meetingRooms: 5,
    currentlyBranded: "Yes",
    currentBrand: "Independent (local resort operator)",
    fbCount: "5",
    currentlyManaged: "Yes",
    operatorCurrent: "Caribbean Resort Operations Ltd.",
    operatorPlan: "Third-party Managed",
    preferredBrands: "Hyatt Ziva, Hyatt Zilara, Hilton All-Inclusive, Marriott All-Inclusive",
    preferredChainScales: "Upper Upscale, Luxury",
    softHardPreference: "Operator-led all-inclusive model required",
    targetChainScale: "Upper Upscale",
    hotelType: "Resort",
    serviceModel: "All-Inclusive Resort",
    siteControl: "Owned — fee simple (sample)",
    siteSize: "12",
    siteSizeUnit: "Acres",
    transitAccess: "PUJ airport highway access (sample)",
    demandDrivers: "Leisure / Tourism, Weddings, Groups",
    keyCompetitors: "Hyatt Ziva Cap Cana; Lopesan Costa Bávaro (sample partial set)",
    revpar: "USD 185–210 (sample range — needs validation)",
    projectCost: "USD 95–120M (sample)",
    equityDebt: "45% equity / 55% debt (sample)",
    pipStatus: "Scope under development",
    pipBudget: "USD 18–25M (sample range)",
    dealStructure: "Franchise + 3rd Party Mgmt.",
    targetGuest: "Adult leisure, groups, destination weddings",
    brandFlexibility: "Prioritize resort operator capability over flag prestige alone",
    primaryGoal: "Maximize Cash Flow",
    priorities: "Operator capability, Resort programming, Brand distribution, PIP feasibility",
    concerns: "All-inclusive operating model definition, PIP scope, Operator role clarity, Brand standards tolerance",
    dealBreakers: ["Operator without AI resort track record", "PIP without phasing plan", "F&B minimums that break resort economics"],
    mustHaves: ["Proven all-inclusive operator", "Phased PIP plan", "Group/wedding sales support"],
    operatorServices: "Revenue management, Sales, Accounting, HR, Marketing, F&B management",
    ownerInvolvement: "Governance board — monthly (sample)",
    decisionTimeline: "Q2 2027",
    proposalDeadline: "2027-01-20",
    executiveSummary: "Fictional DR beach resort owner evaluating all-inclusive repositioning with emphasis on operator capability, resort programming, and phased capital plan.",
    broker: "Yes",
    expectedReadinessStage: "Ready for External Review",
    intentionalGaps: [
      { field: "Plan to Self-Manage or Hire Third Party?", reason: "All-inclusive operator role needs clarification", demoWeakness: true },
      { field: "PIP / CapEx Status", reason: "PIP scope needs validation", demoWeakness: true },
      { field: "Soft vs Hard Brand Preference", reason: "Brand standards tolerance unclear", demoWeakness: true },
      { field: "Group vs Transient Mix", reason: "AI model mix assumptions incomplete", demoWeakness: true },
    ],
    extraFieldSources: {
      "Estimated or Actual RevPAR": { sourceType: "needs_validation", layer: "fictional_deal", note: "Sample range only" },
    },
    extraAirtableRows: [
      { table: "Strategic Intent - Operational - Key Challenges", field: "Plan to Self-Manage or Hire Third Party?", value: "", sourceType: "needs_validation", layer: "fictional_deal", notes: "Operator role TBD" },
    ],
    targetListRows: [
      { brandName: "Hyatt Ziva", parentCompany: "Hyatt", whyInReviewSet: "Owner preference; family/all-inclusive pathway reference", sourceType: "fictional_sample_assumption", reviewSetSource: "owner_preferred" },
      { brandName: "Hyatt Zilara", parentCompany: "Hyatt", whyInReviewSet: "Owner preference; adults-oriented AI path", sourceType: "fictional_sample_assumption", reviewSetSource: "owner_preferred" },
      { brandName: "W Hotels", parentCompany: "Marriott International", whyInReviewSet: "Primary comp class — adult-oriented resort", sourceType: "fictional_sample_assumption", reviewSetSource: "sample_demo" },
      { brandName: "Dreams Resorts & Spas", parentCompany: "Hyatt", whyInReviewSet: "Pipeline — AI operator ecosystem", sourceType: "fictional_sample_assumption", reviewSetSource: "pipeline" },
      { brandName: "Breathless Resorts & Spas", parentCompany: "Hyatt", whyInReviewSet: "Pipeline — adult AI positioning", sourceType: "fictional_sample_assumption", reviewSetSource: "pipeline" },
      { brandName: "Renaissance Hotels", parentCompany: "Marriott International", whyInReviewSet: "Sample demo — non-AI contrast", sourceType: "fictional_sample_assumption", reviewSetSource: "sample_demo" },
      { brandName: "JW Marriott", parentCompany: "Marriott International", whyInReviewSet: "Sample demo — luxury hard-brand contrast", sourceType: "fictional_sample_assumption", reviewSetSource: "sample_demo" },
    ],
    expectedBrandAlignmentBehavior: {
      summary: "Resort AI repositioning; expect conditional alignment unless operator/AI model clarified; lifestyle/luxury hard brands likely conditional vs AI specialists.",
      dominantThemes: ["Resort programming", "Operator capability", "Project type conversion", "PIP/capex validation"],
      likelyHigherAlignmentBrands: ["Hyatt Ziva", "Hyatt Zilara"],
      likelyConditionalBrands: ["W Hotels", "JW Marriott", "Dreams Resorts & Spas"],
      keyConsiderationPatterns: ["Resort operator path must be validated before outreach", "Commercial and PIP assumptions require confirmation"],
      rationaleLayerExpectations: { ownerFacingParagraph: true, technicalFactorsDeemphasized: true, commercialIncentiveSoftening: true, perBrandQuestionsNotGeneric: true },
      validationScenarios: ["Define AI operator model — alignment strengthens for AI brands"],
    },
  },
];

const ALL_DEALS = [...CALA_DEALS, ...CALA_DEALS_PART2];

function writeMarkdownSummary(cfg, outPath) {
  const slug = cfg.file.replace(".example.json", "");
  const lines = [
    `# CALA demo sample — ${cfg.projectName}`,
    "",
    `**Fixture:** \`fixtures/sample-deals/${cfg.file}\``,
    `**Sample ID:** \`${cfg.sampleId}\``,
    "",
    "> Sample deal for product demonstration only. Reference properties are public comps for factual context only.",
    "",
    "| Item | Value |",
    "| --- | --- |",
    `| **Fictional project** | ${cfg.projectName} |`,
    `| **Expected readiness** | ${cfg.expectedReadinessStage} |`,
    `| **Primary reference** | ${cfg.publicName} |`,
    `| **Target list count** | ${cfg.targetListRows.length} |`,
    "",
    "## Reference URLs",
    "",
    ...cfg.sources.map((s, i) => `${i + 1}. ${s.url}`),
    "",
    "## Intentional gaps",
    "",
    "| Field | Reason |",
    "| --- | --- |",
    ...cfg.intentionalGaps.map((g) => `| ${g.field} | ${g.reason} |`),
    "",
    "## Commands",
    "",
    "```bash",
    `node scripts/validate-sample-deal-fixture.mjs fixtures/sample-deals/${cfg.file}`,
    `node scripts/print-sample-deal-airtable-map.mjs fixtures/sample-deals/${cfg.file}`,
    "```",
    "",
  ];
  fs.writeFileSync(outPath, lines.join("\n"), "utf8");
}

function main() {
  const results = [];
  for (const cfg of ALL_DEALS) {
    const record = buildFixture(cfg);
    const outJson = path.join(OUT_DIR, cfg.file);
    fs.writeFileSync(outJson, JSON.stringify(record, null, 2) + "\n", "utf8");
    const mdPath = path.join(ROOT, "docs", "sample-deals", cfg.file.replace(".example.json", ".md"));
    fs.mkdirSync(path.dirname(mdPath), { recursive: true });
    writeMarkdownSummary(cfg, mdPath);
    results.push({ file: cfg.file, ok: true });
    console.log("Wrote", outJson);
  }
  console.log(`\nGenerated ${results.length} fixtures. Run validate-sample-deal-fixture.mjs on each.`);
}

main();
