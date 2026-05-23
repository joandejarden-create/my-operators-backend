/**
 * Default Deal Setup intake fields for CALA demo sample deals.
 * Fills gaps so Airtable records resemble a diligent owner submission.
 */
import { sanitizeDemoIntakeFields } from "./demo-intake-copy-sanitize.js";
import { applyCalaIntakeConsistency, reconcileFbFields } from "./cala-sample-intake-consistency.js";

/** @param {string} stage */
function openingPhaseForStage(stage) {
  const s = String(stage || "").toLowerCase();
  if (s.includes("stabilized")) return "N/A (stabilized operating)";
  if (s.includes("under construction")) return "Construction";
  if (s.includes("entitlement")) return "Entitlements in Process";
  if (s.includes("land under control")) return "Planning / entitlement";
  return "Pre-construction";
}

/** @param {object} cfg */
function operatingModelFor(cfg) {
  if (cfg.currentlyManaged === "Yes") {
    return cfg.currentlyBranded === "Yes"
      ? "Third-party managed (branded)"
      : "Third-party managed (independent/collection)";
  }
  if (cfg.currentlyBranded === "Yes") return "Owner-operated (branded/franchised)";
  if (cfg.projectType?.includes("New Build")) return "Owner-operated (unbranded)";
  return "Owner-operated (unbranded)";
}

/** @param {object} cfg */
function operatorNameFor(cfg) {
  if (cfg.operatorCurrent) return cfg.operatorCurrent;
  if (cfg.currentlyManaged !== "Yes") return "";
  return (
    cfg.operatorNameDefault ||
    "Latin America Lodging Partners"
  );
}

/** @param {number} keys @param {number} outlets */
function fbOutletSqFt(keys, outlets = 2) {
  const base = Math.round(keys * 12 + outlets * 400);
  return Math.max(1800, Math.min(18000, base));
}

/** @param {object} cfg */
function milestonesFor(cfg) {
  if (cfg.keyMilestones) return cfg.keyMilestones;
  const open = cfg.openingDate || "2028-01-01";
  const y = open.slice(0, 4);
  const stage = cfg.stage || "";
  if (stage.includes("Stabilized")) {
    return `Rebrand planning ${Number(y) - 1} Q3–Q4; PIP mobilization ${y} Q1; soft opening ${open}; stabilization ${y} Q3.`;
  }
  if (stage.includes("Under Construction")) {
    return `Structure topping out ${Number(y) - 1} Q4; MEP & interior ${y} Q1–Q2; pre-opening ${y} Q3; target opening ${open}.`;
  }
  if (stage.includes("Entitlement")) {
    return `Design development ${Number(y) - 1} Q2–Q3; permitting ${Number(y) - 1} Q4–${y} Q1; ground break ${y} Q2; opening ${open}.`;
  }
  return `LOI/site control secured; design & permitting ${Number(y) - 1} Q3–Q4; vertical construction ${y} Q1–Q3; opening ${open}.`;
}

/** @param {object} cfg */
function gcFor(cfg) {
  return (
    cfg.generalContractor ||
    {
      Mexico: "Consorcio Constructor Hospitality México",
      Colombia: "Andina Hotel Builders S.A.S.",
      "Dominican Republic": "Caribe Hospitality Constructors S.R.L.",
      Peru: "Cusco Build Partners S.A.C.",
      "Puerto Rico": "Isla Verde Construction Group",
      Panama: "Pacífico Tower Builders S.A.",
      "El Salvador": "Centroamérica Hotel Works S.A. de C.V.",
    }[cfg.country] ||
    "Regional Hospitality Constructors"
  );
}

/** @param {object} cfg */
function architectFor(cfg) {
  return (
    cfg.architect ||
    {
      Mexico: "Estudio Lote 14 Arquitectura",
      Colombia: "Taller Plazoleta Arquitectos",
      "Dominican Republic": "Studio Bávaro Design",
      Peru: "Altura Andina Arquitectura",
      "Puerto Rico": "Bahía Design Collaborative",
      Panama: "Corredor Pacífico Studio",
      "El Salvador": "Volcán Plaza Arquitectos",
    }[cfg.country] ||
    "Hospitality Design Collaborative"
  );
}

/** @param {object} cfg */
function outletNamesFor(cfg) {
  if (cfg.outletNames) return cfg.outletNames;
  if (cfg.fbProgram?.includes("All-Inclusive") || cfg.serviceModel?.includes("All-Inclusive")) {
    return "Main buffet & show kitchen; specialty steakhouse; lobby bar; pool bar; café grab-and-go.";
  }
  if (cfg.hotelType?.includes("Airport") || cfg.submarket?.toLowerCase().includes("airport")) {
    return "All-day dining restaurant; lobby bar; grab-and-go market.";
  }
  if (cfg.hotelType?.includes("Historic") || cfg.buildingType?.includes("Historic")) {
    return "Signature restaurant; courtyard bar; in-room dining.";
  }
  return "All-day restaurant; lobby bar; grab-and-go or café.";
}

/** @param {object} cfg */
function fbProgramTypesFor(cfg) {
  if (Array.isArray(cfg.fbProgramTypes)) return cfg.fbProgramTypes;
  if (cfg.serviceModel?.includes("All-Inclusive")) {
    return ["Full-Service Restaurant + Bar", "Pool Bar / Rooftop Bar / Feature Bar", "Coffee Shop / Cafe"];
  }
  if (cfg.hotelType === "Airport") {
    return ["Full-Service Restaurant + Bar", "Coffee Shop / Cafe", "Minimal / Grab & Go"];
  }
  return ["Full-Service Restaurant + Bar", "Coffee Shop / Cafe"];
}

/** @param {object} cfg */
function importanceDefaults(cfg) {
  const hi = cfg.importanceHigh || 4;
  const med = cfg.importanceMed || 3;
  return {
    "Speed to Market Importance": cfg.speedToMarketImportance ?? hi,
    "Development / Renovation Timeline Importance": cfg.devTimelineImportance ?? hi,
    "CapEx / PIP Execution Importance": cfg.capexImportance ?? hi,
    "Revenue / Yield Management Importance": cfg.revenueImportance ?? hi,
    "Marketing & Distribution Importance": cfg.marketingImportance ?? hi,
    "Loyalty Program Importance": cfg.loyaltyImportance ?? med,
    "Brand Recognition Importance": cfg.brandRecognitionImportance ?? hi,
    "Brand Equity Increase on Exit Importance": cfg.brandEquityImportance ?? med,
    "Guest Experience / Satisfaction Importance": cfg.guestExpImportance ?? hi,
    "Cost Control / Operational Efficiency Importance": cfg.costControlImportance ?? hi,
    "Staffing & Talent Importance": cfg.staffingImportance ?? med,
    "Technology & Systems Importance": cfg.techImportance ?? med,
    "Incentive Alignment Importance": cfg.incentiveAlignImportance ?? med,
    "ESG / Sustainability Importance": cfg.esgImportance ?? 2,
  };
}

/**
 * @param {object} cfg — build-cala-sample-deals config
 * @returns {Record<string, unknown>}
 */
export function buildCalaIntakeCompletion(cfg) {
  const keys = Number(cfg.keys) || 120;
  const outlets = Number(cfg.fbCount) || 2;
  const fbSqFt = fbOutletSqFt(keys, outlets);
  const isConversion =
    String(cfg.projectType || "").includes("Conversion") ||
    String(cfg.projectType || "").includes("Renovation");
  const isNewBuild = String(cfg.projectType || "").includes("New Build");
  const managed = cfg.currentlyManaged === "Yes";
  const branded = cfg.currentlyBranded === "Yes";

  const completion = {
    "Opening / Transition Phase": cfg.openingPhase || openingPhaseForStage(cfg.stage),
    "Current Operating Model": cfg.currentOperatingModel || operatingModelFor(cfg),
    "Key Milestones / Target Dates": milestonesFor(cfg),
    "General Contractor (if known)": gcFor(cfg),
    "Architect (if known)": architectFor(cfg),
    "Existing flag staying or being replaced?":
      cfg.existingFlagStaying ||
      (branded
        ? isConversion
          ? "Being Replaced / Reflag"
          : "Staying"
        : isNewBuild
          ? "Not Applicable (Unbranded or New Build)"
          : "Not Applicable (Unbranded or New Build)"),
    "Operator Name Current": operatorNameFor(cfg),
    "Parent Company Name": cfg.parentCompanyName || "",
    "Outlet Names / Concepts": outletNamesFor(cfg),
    "F&B Program Type": fbProgramTypesFor(cfg),
    "Total F&B Outlet Size": cfg.fbOutletSize ?? fbSqFt,
    "Total F&B Outlet Size Unit": cfg.fbOutletSizeUnit || "Sq. Ft.",
    "Hotel Rental Program?": cfg.hotelRentalProgram || "No",
    "Condo Residences?": cfg.condoResidences || "No",
    "Parking Program Type": cfg.parkingProgram || "Surface Lot",
    "Number of Parking Spaces": cfg.parkingSpaces || String(Math.round(keys * 0.45)),
    "Parking Ratio": cfg.parkingRatio || "1.0 spaces per key",
    "Max height Allowed By Zoning": cfg.maxHeight || "45",
    "Max height Unit": cfg.maxHeightUnit || "Feet",
    "Primary Market Region": cfg.primaryMarketRegion || "CALA",
    "Micro-Location Type": cfg.microLocation || "Suburban / Secondary Node",
    "Demand Mix Targets": cfg.demandMixTargets || [
      "Corporate Transient",
      "Leisure FIT",
      "Group / MICE",
    ],
    "Operational Complexity Profile": cfg.operationalComplexity || [
      "Meaningful Group / Events Mix",
    ],
    "Year Built (Years Open as a Hotel)": cfg.yearBuilt || (isNewBuild ? "" : "2012"),
    "PMS or Tech is in Place": cfg.pmsInPlace || (isNewBuild ? "No" : "Yes"),
    "Ceiling Heights": cfg.ceilingHeights || "9",
    "Ceiling Heights Unit": cfg.ceilingHeightsUnit || "Feet",
    "Column Spacing": cfg.columnSpacing || "",
    "Column Spacing Unit": cfg.columnSpacingUnit || "Feet",
    "Existing MEP Capacity (Conversion)": cfg.mepCapacity || (isConversion ? "Adequate for select-service conversion with targeted MEP upgrades" : ""),
    "Group vs Transient Mix":
      cfg.mix ||
      (cfg.hotelType === "Airport"
        ? "55% transient / 35% group / 10% crew"
        : "60% transient / 40% group"),
    "Royalty Fee Expectations": cfg.royaltyFees || "4% - 5%",
    "Marketing Fee Expectations": cfg.marketingFees || "3% - 4%",
    "Loyalty Fee Expectations": cfg.loyaltyFees || "3% - 4%",
    "Estimated Dev. Cost per Key (Room)": cfg.devCostPerKey || "$200K – $300K",
    "Is Financing Secured?": cfg.financingSecured || (isNewBuild ? "Partially Secured" : "Fully Secured"),
    "Comfort Level with Upfront Investment": cfg.comfortUpfront || "Moderate",
    "Fee Tolerance Level": cfg.feeTolerance || "Moderate - Balanced",
    "Incentive Requirement Level": cfg.incentiveLevel || "Preferred",
    "Primary Incentive Type": cfg.primaryIncentive || "Key Money",
    "CapEx Tolerance Band": cfg.capexTolerance || "Moderate",
    "Preferred Future Operating Model":
      cfg.preferredFutureOperatingModel ||
      (cfg.operatorPlan?.includes("Third")
        ? "Third-party management only"
        : "Franchise/license only (owner or third-party operator)"),
    "Operator Strategy Status": cfg.operatorStrategyStatus || "Ready for structured operator review",
    "Operator Capability Priorities": cfg.operatorCapabilityPriorities || [
      "Revenue management & distribution",
      "Pre-opening / opening support",
      "Accounting & owner reporting",
    ],
    "Owner Reporting Package": cfg.ownerReportingPackage || [
      "Monthly P&L",
      "Monthly operating metrics",
      "Quarterly board pack",
    ],
    "Owner Reporting Frequency": cfg.ownerReportingFrequency || "Monthly",
    "Preferred Reporting Frequency": cfg.preferredReportingFrequency || "Monthly",
    "On-Site vs Remote Owner Representation": cfg.ownerRepresentation || "Hybrid",
    "Strategy Type": cfg.strategyType || "Yield / Cash Flow Optimization",
    "Brand Role Intent": cfg.brandRoleIntent || "Distribution and Loyalty Engine",
    "Decision Horizon": cfg.decisionHorizon || "6-12 months",
    "Owner Control Priorities": cfg.ownerControlPriorities || ["Budget Approval Rights", "CapEx Approval Rights"],
    "Contract Flexibility Priorities": cfg.contractFlexPriorities || [
      "Performance Termination Rights",
      "Fee Step-Down / Ramp",
    ],
    "Incentive Types Interested In": cfg.incentiveTypes || [
      "Key Money / Upfront Incentive",
      "Reduced Royalty Period",
    ],
    "Critical deadlines for application": cfg.criticalDeadlines || "No",
    "Other Operator Criteria or Notes":
      cfg.operatorNotes ||
      "Owner seeks transparent reporting and realistic pre-opening timeline.",
    "Main Contact Title": cfg.contactTitle || "Managing Director",
    "Secondary Contact": cfg.secondaryContact || "",
    "Best Time or Method to Reach": cfg.bestTimeToReach || "Email; WhatsApp for urgent items",
    "What makes this opportunity stand out to a brand or operator?":
      cfg.standout ||
      `CALA ${cfg.market || cfg.country} project with clear chain-scale positioning and a defined brand review set.`,
    "Additional Notes or Unique Project Aspects": cfg.additionalNotes || "",
    "Anything else you'd like to add?": cfg.anythingElse || "",
    "Would you like to meet consultants?": cfg.meetConsultants || "Not Right Now",
    "Other Projects Nearing Contract Expiration?": cfg.otherProjectsExpiring || "No",
    ...importanceDefaults(cfg),
  };

  if (cfg.workedWithPreferred === "Yes") {
    completion["Parent Company Name"] =
      cfg.parentCompanyName || "Prior relationship with preferred brand parents";
  }

  if (managed && branded) {
    completion["Current Franchise/Management Contract End Date"] =
      cfg.franchiseContractEnd || "2027-12-31";
  }

  if (isConversion) {
    completion["Is the property encumbered"] = cfg.encumbered || ["Management Agreement"];
    completion["Property Encumbered Description"] =
      cfg.encumberedDescription || "Existing management agreement expires at rebranding.";
  }

  if (!completion["Estimated or Actual RevPAR"] && cfg.revpar) {
    completion["Estimated or Actual RevPAR"] = cfg.revpar;
  } else if (!cfg.revpar && !cfg.intentionalRevparGap) {
    completion["Estimated or Actual RevPAR"] = cfg.estimatedRevpar || "$100 – $149";
  }

  if (!cfg.pipBudget && isConversion && cfg.pipStatus) {
    completion["PIP Budget Range (if conversion)"] = cfg.pipBudgetDefault || "$3M – $5M";
  }

  if (cfg.preferredThirdPartyOperators) {
    completion["Preferred Third-Party Operators (names)"] = cfg.preferredThirdPartyOperators;
  } else if (cfg.operatorPlan?.includes("Third")) {
    completion["Preferred Third-Party Operators (names)"] =
      "Open to qualified regional operators with CALA track record; owner shortlist TBD.";
  }

  if (Array.isArray(cfg.priorities) && !cfg.priorities.includes("Other")) {
    completion["Top Priorities for Project"] = cfg.priorities;
  } else if (!cfg.priorities || (typeof cfg.priorities === "string" && cfg.priorities.includes(","))) {
    completion["Top Priorities for Project"] = [
      "Strong Financial Performance",
      "Brand Recognition",
      "Operational Expertise",
    ];
  }

  if (Array.isArray(cfg.concerns) && !cfg.concerns.includes("Other")) {
    completion["Top Concerns for this Project"] = cfg.concerns;
  } else {
    completion["Top Concerns for this Project"] = ["High Costs", "Inflexibility", "Underperformance"];
  }

  if (Array.isArray(cfg.dealBreakers) && !cfg.dealBreakers.every((d) => d === "Other")) {
    completion["Top 3 Deal Breakers"] = cfg.dealBreakers.filter((d) => d !== "Other");
  }

  if (Array.isArray(cfg.mustHaves) && !cfg.mustHaves.every((d) => d === "Other")) {
    completion["Must-haves From Brand or Operator"] = cfg.mustHaves.filter((d) => d !== "Other");
  }

  if (cfg.targetGuest && cfg.targetGuest !== "Other") {
    completion["Target Guest Segment"] = cfg.targetGuest;
  } else if (completion["Target Guest Segment"] === "Other" || !cfg.targetGuest) {
    completion["Target Guest Segment"] = "Corporate / Business";
  }

  return completion;
}

/**
 * Merge completion into fields; fictional cfg values win when non-empty.
 * @param {Record<string, unknown>} fields
 * @param {object} cfg
 */
export function applyCalaIntakeCompletion(fields, cfg) {
  const completion = sanitizeDemoIntakeFields(buildCalaIntakeCompletion(cfg));
  for (const [k, v] of Object.entries(completion)) {
    if (v === undefined || v === null) continue;
    if (typeof v === "string" && v.trim() === "") continue;
    if (Array.isArray(v) && v.length === 0) continue;
    const cur = fields[k];
    if (cur === undefined || cur === null || cur === "") {
      fields[k] = v;
      continue;
    }
    if (typeof cur === "string" && cur.trim() === "") fields[k] = v;
  }
  if (cfg.layer === "reference") {
    reconcileFbFields(fields, cfg);
  } else {
    applyCalaIntakeConsistency(fields, cfg);
  }
  return fields;
}
