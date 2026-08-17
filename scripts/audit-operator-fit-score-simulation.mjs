#!/usr/bin/env node
/**
 * AUDIT UTILITY — Operator Fit Engine synthetic score differentiation simulation.
 *
 * Runs the CURRENT scoreOperatorMatchForDeal logic against local synthetic
 * deal + operator fixtures. Does NOT call Airtable. Does NOT change weights.
 *
 * Usage:
 *   node scripts/audit-operator-fit-score-simulation.mjs
 *   node scripts/audit-operator-fit-score-simulation.mjs --out reports/operator-fit-score-simulation.json
 */
import { writeFileSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { scoreOperatorMatchForDeal } from "../api/my-deals.js";
import { OPERATOR_MATCH_WEIGHTS } from "../lib/operator-alignment-scoring-weight-config.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const args = process.argv.slice(2);
const outFlag = args.indexOf("--out");
const outPath =
  outFlag >= 0 && args[outFlag + 1]
    ? join(root, args[outFlag + 1])
    : join(root, "reports", "operator-fit-score-simulation.json");

function si(fields) {
  return fields;
}

function loc(fields) {
  return fields;
}

function mp(fields) {
  return fields;
}

function deals(fields) {
  return fields;
}

/** Eight project archetypes from the audit brief. */
const SCENARIOS = [
  {
    id: "upper-upscale-urban-new-build",
    label: "Upper-upscale urban new build",
    deal: deals({
      "Project Type": "New Build",
      "F&B Complexity": "Full-service F&B",
      "Opening Timeline": "Pre-development",
    }),
    location: loc({
      Country: "Mexico",
      City: "Mexico City",
      "Hotel Chain Scale": "Upper Upscale",
      "Building Type": "High-Rise",
      "Stage of Development": "Pre-development / Planning",
    }),
    mp: mp({
      "Royalty Fee Expectations": "4-5%",
      "Marketing Fee Expectations": "2%",
      "Preferred Deal Structure": "Franchise Only",
    }),
    si: si({
      "Brand Agreement Structure": "Franchise",
      "Operating Model": "Third-party managed",
      "Preferred Management Structure": ["Franchise with third-party operator", "Full third-party management"],
      "Must-Have Operator Services": [
        "Full hotel management",
        "Pre-opening planning",
        "Revenue management",
        "Sales",
        "Owner reporting",
      ],
      "Market Presence Requirement": "Active country operations required",
      "Pre-Opening Support Needed": "Yes — full pre-opening",
      "Owner Reporting Expectations": "Institutional monthly package",
      "Owner Control Preference": "High oversight / approval rights",
      "Preferred Brands": ["Marriott", "Hilton"],
      "Top 3 Deal Breakers": ["No local presence"],
    }),
  },
  {
    id: "luxury-leisure-resort",
    label: "Luxury leisure resort",
    deal: deals({
      "Project Type": "New Build",
      "F&B Complexity": "Complex multi-outlet F&B",
      "Opening Timeline": "Under construction",
    }),
    location: loc({
      Country: "Dominican Republic",
      City: "Punta Cana",
      "Hotel Chain Scale": "Luxury",
      "Building Type": "Resort Campus",
      "Stage of Development": "Under Construction",
    }),
    mp: mp({ "Royalty Fee Expectations": "5%", "Marketing Fee Expectations": "3%" }),
    si: si({
      "Brand Agreement Structure": "Management",
      "Operating Model": "Brand-managed",
      "Preferred Management Structure": ["Brand-managed", "Full third-party management"],
      "Must-Have Operator Services": [
        "Full hotel management",
        "F&B management",
        "Pre-opening planning",
        "Revenue management",
      ],
      "Market Presence Requirement": "Active country operations required",
      "Preferred Brands": ["Four Seasons", "Ritz-Carlton"],
    }),
  },
  {
    id: "select-service-conversion",
    label: "Select-service conversion",
    deal: deals({
      "Project Type": "Conversion / Reflag",
      "F&B Complexity": "Limited F&B",
      "Opening Timeline": "Near-term opening",
    }),
    location: loc({
      Country: "Mexico",
      City: "Cancún",
      "Hotel Chain Scale": "Upper Midscale",
      "Building Type": "Mid-Rise",
      "Stage of Development": "Existing / Operating",
    }),
    mp: mp({
      "Royalty Fee Expectations": "5%",
      "Preferred Deal Structure": "Franchise Only",
    }),
    si: si({
      "Brand Agreement Structure": "Franchise",
      "Operating Model": "Third-party managed",
      "Preferred Management Structure": ["Franchise with third-party operator"],
      "Must-Have Operator Services": [
        "Full hotel management",
        "Brand compliance support",
        "Revenue management",
        "Owner reporting",
      ],
      "Market Presence Requirement": "Active country operations required",
      "Preferred Brands": ["Courtyard", "Hampton"],
    }),
  },
  {
    id: "mixed-use-branded-residences",
    label: "Mixed-use hotel with branded residences",
    deal: deals({
      "Project Type": "Mixed-Use Hospitality Project",
      "F&B Complexity": "Full-service F&B",
    }),
    location: loc({
      Country: "Panama",
      City: "Panama City",
      "Hotel Chain Scale": "Upscale",
      "Building Type": "Mixed-Use Tower",
    }),
    mp: mp({ "Royalty Fee Expectations": "4%" }),
    si: si({
      "Brand Agreement Structure": "Franchise",
      "Operating Model": "Hybrid / project-specific",
      "Preferred Management Structure": ["Full third-party management", "Franchise with third-party operator"],
      "Must-Have Operator Services": [
        "Full hotel management",
        "Pre-opening planning",
        "Owner reporting",
        "Asset management support",
      ],
      "Preferred Brands": ["W Hotels", "Kimpton"],
    }),
  },
  {
    id: "large-group-convention",
    label: "Large group and convention hotel",
    deal: deals({
      "Project Type": "Existing Operating Hotel",
      "F&B Complexity": "Complex multi-outlet F&B",
    }),
    location: loc({
      Country: "Colombia",
      City: "Bogotá",
      "Hotel Chain Scale": "Upper Upscale",
      "Building Type": "High-Rise",
    }),
    mp: mp({ "Royalty Fee Expectations": "4%" }),
    si: si({
      "Brand Agreement Structure": "Franchise",
      "Operating Model": "Third-party managed",
      "Preferred Management Structure": ["Full third-party management"],
      "Must-Have Operator Services": [
        "Full hotel management",
        "Sales",
        "Revenue management",
        "Owner reporting",
      ],
      "Preferred Brands": ["Hyatt", "Hilton"],
    }),
  },
  {
    id: "independent-lifestyle-soft-brand",
    label: "Independent lifestyle hotel considering soft brand",
    deal: deals({
      "Project Type": "Existing Operating Hotel",
      "F&B Complexity": "Lifestyle F&B",
    }),
    location: loc({
      Country: "Mexico",
      City: "Mérida",
      "Hotel Chain Scale": "Upscale",
      "Building Type": "Adaptive Reuse / Heritage",
    }),
    mp: mp({}),
    si: si({
      "Brand Agreement Structure": "Soft brand / collection affiliation",
      "Operating Model": "Owner-operated",
      "Preferred Management Structure": ["Owner-operated with commercial support", "Commercial-only support"],
      "Must-Have Operator Services": ["Sales", "Revenue management", "Brand compliance support"],
      "Owner Control Preference": "Owner-operated with light commercial support",
      "Preferred Brands": ["Design Hotels", "Autograph"],
    }),
  },
  {
    id: "turnaround-underperforming",
    label: "Underperforming hotel requiring turnaround",
    deal: deals({
      "Project Type": "Renovation / Repositioning",
      "F&B Complexity": "Full-service F&B",
    }),
    location: loc({
      Country: "Puerto Rico",
      City: "San Juan",
      "Hotel Chain Scale": "Upscale",
      "Building Type": "Mid-Rise",
      "Stage of Development": "Existing / Operating",
    }),
    mp: mp({ "Royalty Fee Expectations": "5%" }),
    si: si({
      "Brand Agreement Structure": "Franchise",
      "Operating Model": "Third-party managed",
      "Preferred Management Structure": ["Full third-party management"],
      "Must-Have Operator Services": [
        "Full hotel management",
        "Revenue management",
        "Sales",
        "Owner reporting",
      ],
      "Top 3 Deal Breakers": ["No turnaround experience", "No local presence"],
      "Preferred Brands": ["Marriott"],
    }),
  },
  {
    id: "institutional-lender-reporting",
    label: "Institutionally owned hotel requiring lender-grade reporting",
    deal: deals({
      "Project Type": "Existing Operating Hotel",
      "F&B Complexity": "Limited F&B",
    }),
    location: loc({
      Country: "Mexico",
      City: "Monterrey",
      "Hotel Chain Scale": "Upper Midscale",
      "Building Type": "Mid-Rise",
    }),
    mp: mp({ "Royalty Fee Expectations": "5%", "Marketing Fee Expectations": "2%" }),
    si: si({
      "Brand Agreement Structure": "Franchise",
      "Operating Model": "Third-party managed",
      "Preferred Management Structure": ["Full third-party management", "Franchise with third-party operator"],
      "Must-Have Operator Services": [
        "Full hotel management",
        "Owner reporting",
        "Revenue management",
        "Accounting / finance support",
      ],
      "Owner Reporting Expectations": "Institutional monthly package",
      "Owner Control Preference": "Institutional / lender-grade oversight",
      "Commercial Priority": "Transparent fees and reporting",
      "Preferred Brands": ["Courtyard", "Hampton", "Holiday Inn Express"],
    }),
  },
];

/** Synthetic operators spanning niche vs generic capability profiles. */
const OPERATORS = [
  {
    id: "generic-full-service-claims",
    name: "Generic Full-Service Claims Co",
    note: "Claims all table-stakes services; broad geography; no niche signal",
    prefill: {
      companyName: "Generic Full-Service Claims Co",
      activeCountries: ["Mexico", "Colombia", "Panama", "Dominican Republic", "Puerto Rico"],
      activeMarkets: ["Mexico City", "Cancún", "Bogotá", "Panama City", "Punta Cana", "San Juan", "Monterrey", "Mérida"],
      marketPresenceType: ["Active operations"],
      chainScalesSupported: ["Upper Midscale", "Upscale", "Upper Upscale", "Luxury"],
      bestFitAssetTypes: ["Urban", "Resort", "Mixed-Use", "Conversion", "New Build"],
      operatingSituations: ["New Build", "Conversion / Reflag", "Stabilized", "Renovation"],
      managementStructuresSupported: [
        "Full third-party management",
        "Franchise support",
        "Brand-managed",
        "Commercial-only support",
        "Pre-opening / transition support",
        "Asset management support",
      ],
      offeredServices: [
        "Full hotel management",
        "Pre-opening planning",
        "Revenue management",
        "Sales",
        "Marketing",
        "Procurement",
        "Accounting / finance support",
        "Owner reporting",
        "HR / training",
        "Digital distribution",
        "Brand compliance support",
        "F&B management",
        "Asset management support",
      ],
      feeStructureSummary: "Competitive base + incentive fees",
      technologySystems: ["PMS", "RMS", "BI"],
      ownerReportingCadence: "Monthly",
      ownerReportingLevel: "Institutional monthly package",
      governanceCadence: "Monthly owner meeting",
      ownerCommunicationStyle: "collaborative monthly reporting with owner refs",
      brands: ["Marriott", "Hilton", "Hyatt", "IHG", "Courtyard", "Hampton"],
      revenueManagementCapability: "Full RM",
      preOpeningSupportCapability: "Full pre-opening",
      newBuildOpeningExperience: "Extensive",
      lessIdealSituations: "",
    },
  },
  {
    id: "yucatan-select-specialist",
    name: "Yucatán Select Specialist",
    note: "Strong Mexico select-service / conversion niche",
    prefill: {
      companyName: "Yucatán Select Specialist",
      activeCountries: ["Mexico"],
      activeMarkets: ["Cancún", "Mérida", "Playa del Carmen"],
      marketPresenceType: ["Active operations"],
      chainScalesSupported: ["Midscale", "Upper Midscale", "Upscale"],
      bestFitAssetTypes: ["Select-service", "Urban", "Conversion"],
      operatingSituations: ["Conversion / Reflag", "New Build", "Stabilized"],
      managementStructuresSupported: ["Full third-party management", "Franchise support"],
      offeredServices: [
        "Full hotel management",
        "Brand compliance support",
        "Revenue management",
        "Owner reporting",
        "Pre-opening planning",
      ],
      feeStructureSummary: "Base fee + GOP incentive",
      technologySystems: ["PMS", "RMS"],
      ownerReportingLevel: "Standard monthly package",
      ownerCommunicationStyle: "monthly collaborative",
      brands: ["Courtyard", "Hampton", "Holiday Inn Express"],
      preOpeningSupportCapability: "Full pre-opening",
      newBuildOpeningExperience: "Moderate",
      lessIdealSituations: "Ultra-luxury resorts; branded residences without hotel ops",
    },
  },
  {
    id: "cala-resort-luxury",
    name: "CALA Resort Luxury Operator",
    note: "Resort / all-inclusive / luxury leisure depth",
    prefill: {
      companyName: "CALA Resort Luxury Operator",
      activeCountries: ["Dominican Republic", "Mexico", "Puerto Rico"],
      activeMarkets: ["Punta Cana", "Cancún", "San Juan"],
      marketPresenceType: ["Active operations"],
      chainScalesSupported: ["Upscale", "Upper Upscale", "Luxury"],
      bestFitAssetTypes: ["Resort", "All-inclusive", "Leisure"],
      operatingSituations: ["New Build", "Stabilized", "Renovation"],
      managementStructuresSupported: ["Full third-party management", "Brand-managed", "Hybrid / project-specific"],
      offeredServices: [
        "Full hotel management",
        "F&B management",
        "Pre-opening planning",
        "Revenue management",
        "Sales",
        "Owner reporting",
      ],
      feeStructureSummary: "Resort management fee schedule",
      technologySystems: ["PMS", "RMS", "CRM"],
      ownerReportingLevel: "Institutional monthly package",
      ownerCommunicationStyle: "weekly collaborative owner advisory",
      brands: ["Ritz-Carlton", "Four Seasons", "Marriott"],
      preOpeningSupportCapability: "Full pre-opening",
      newBuildOpeningExperience: "Extensive",
      lessIdealSituations: "Airport select-service; limited-service conversions",
    },
  },
  {
    id: "turnaround-specialist",
    name: "Caribbean Turnaround Specialist",
    note: "Repositioning / underperformance focus; narrower footprint",
    prefill: {
      companyName: "Caribbean Turnaround Specialist",
      activeCountries: ["Puerto Rico", "Dominican Republic"],
      activeMarkets: ["San Juan", "Santo Domingo"],
      marketPresenceType: ["Active operations"],
      chainScalesSupported: ["Upscale", "Upper Upscale"],
      bestFitAssetTypes: ["Urban", "Resort", "Renovation"],
      operatingSituations: ["Renovation", "Conversion / Reflag", "Stabilized", "Turnaround"],
      managementStructuresSupported: ["Full third-party management"],
      offeredServices: [
        "Full hotel management",
        "Revenue management",
        "Sales",
        "Owner reporting",
        "Pre-opening planning",
      ],
      feeStructureSummary: "Higher base during turnaround + incentive",
      technologySystems: ["PMS", "RMS", "BI"],
      ownerReportingLevel: "Institutional monthly package",
      ownerCommunicationStyle: "weekly collaborative",
      brands: ["Marriott", "Hilton"],
      lessIdealSituations: "Greenfield luxury with no local ops team",
    },
  },
  {
    id: "commercial-support-only",
    name: "Commercial Support Boutique",
    note: "Sales/RM only — weak for full management mandates",
    prefill: {
      companyName: "Commercial Support Boutique",
      activeCountries: ["Mexico"],
      activeMarkets: ["Mérida", "Mexico City"],
      marketPresenceType: ["Active operations"],
      chainScalesSupported: ["Upscale", "Upper Upscale"],
      bestFitAssetTypes: ["Lifestyle", "Boutique", "Adaptive Reuse"],
      operatingSituations: ["Stabilized"],
      managementStructuresSupported: ["Commercial-only support", "Pre-opening / transition support"],
      offeredServices: ["Sales", "Revenue management", "Brand compliance support", "Marketing"],
      feeStructureSummary: "Retainer + commission",
      technologySystems: ["CRM"],
      ownerReportingLevel: "Light quarterly",
      ownerCommunicationStyle: "collaborative",
      brands: ["Design Hotels", "Autograph", "Kimpton"],
      lessIdealSituations: "Full third-party management mandates; institutional reporting",
    },
  },
  {
    id: "institutional-reporting-platform",
    name: "Institutional Reporting Platform",
    note: "Strong governance/reporting; average asset niche",
    prefill: {
      companyName: "Institutional Reporting Platform",
      activeCountries: ["Mexico", "Colombia", "Panama"],
      activeMarkets: ["Monterrey", "Mexico City", "Bogotá", "Panama City"],
      marketPresenceType: ["Active operations"],
      chainScalesSupported: ["Upper Midscale", "Upscale", "Upper Upscale"],
      bestFitAssetTypes: ["Urban", "Select-service", "Mixed-Use"],
      operatingSituations: ["Stabilized", "New Build", "Conversion / Reflag"],
      managementStructuresSupported: [
        "Full third-party management",
        "Franchise support",
        "Asset management support",
      ],
      offeredServices: [
        "Full hotel management",
        "Owner reporting",
        "Accounting / finance support",
        "Revenue management",
        "Sales",
        "Brand compliance support",
      ],
      feeStructureSummary: "Transparent fee schedule with owner audit rights",
      technologySystems: ["PMS", "RMS", "ERP", "Owner portal"],
      ownerReportingCadence: "Monthly",
      ownerReportingLevel: "Institutional monthly package",
      governanceCadence: "Monthly board pack",
      ownerCommunicationStyle: "monthly collaborative owner refs advisory",
      brands: ["Courtyard", "Hampton", "Holiday Inn Express", "Hilton", "Marriott"],
      lessIdealSituations: "",
    },
  },
  {
    id: "sparse-data-operator",
    name: "Sparse Data Operator",
    note: "Missing most structured fields — should exclude factors / lower confidence",
    prefill: {
      companyName: "Sparse Data Operator",
      activeCountries: ["Mexico"],
      chainScalesSupported: ["Upscale"],
      offeredServices: [],
      managementStructuresSupported: [],
    },
  },
  {
    id: "wrong-geo-broad-claims",
    name: "Wrong-Geo Broad Claims",
    note: "Claims all services but wrong geography vs most CALA deals",
    prefill: {
      companyName: "Wrong-Geo Broad Claims",
      activeCountries: ["Spain", "Portugal"],
      activeMarkets: ["Madrid", "Lisbon"],
      marketPresenceType: ["Active operations"],
      chainScalesSupported: ["Upper Midscale", "Upscale", "Upper Upscale", "Luxury"],
      bestFitAssetTypes: ["Urban", "Resort", "Mixed-Use", "New Build", "Conversion"],
      operatingSituations: ["New Build", "Conversion / Reflag", "Stabilized", "Renovation"],
      managementStructuresSupported: [
        "Full third-party management",
        "Franchise support",
        "Brand-managed",
        "Asset management support",
      ],
      offeredServices: [
        "Full hotel management",
        "Pre-opening planning",
        "Revenue management",
        "Sales",
        "Marketing",
        "Owner reporting",
        "F&B management",
        "Accounting / finance support",
      ],
      feeStructureSummary: "Competitive fees",
      technologySystems: ["PMS", "RMS"],
      ownerReportingLevel: "Institutional monthly package",
      ownerCommunicationStyle: "monthly collaborative",
      brands: ["Marriott", "Hilton", "Hyatt", "Courtyard", "Hampton"],
    },
  },
];

function stats(scores) {
  const nums = scores.filter((n) => n != null && !Number.isNaN(n)).sort((a, b) => a - b);
  if (!nums.length) {
    return { count: 0, mean: null, median: null, min: null, max: null, stdev: null, withinFiveOfTop: 0 };
  }
  const mean = nums.reduce((a, b) => a + b, 0) / nums.length;
  const mid = Math.floor(nums.length / 2);
  const median = nums.length % 2 ? nums[mid] : (nums[mid - 1] + nums[mid]) / 2;
  const variance = nums.reduce((a, b) => a + (b - mean) ** 2, 0) / nums.length;
  const stdev = Math.sqrt(variance);
  const max = nums[nums.length - 1];
  const withinFiveOfTop = nums.filter((n) => max - n <= 5).length;
  return {
    count: nums.length,
    mean: Math.round(mean * 10) / 10,
    median: Math.round(median * 10) / 10,
    min: nums[0],
    max,
    stdev: Math.round(stdev * 10) / 10,
    withinFiveOfTop,
  };
}

function factorAverages(rows) {
  const sums = {};
  const counts = {};
  for (const row of rows) {
    for (const [k, f] of Object.entries(row.breakdown || {})) {
      if (f.score === "—" || f.score == null) continue;
      const n = Number(f.score);
      if (Number.isNaN(n)) continue;
      sums[k] = (sums[k] || 0) + n;
      counts[k] = (counts[k] || 0) + 1;
    }
  }
  return Object.keys(sums)
    .map((k) => ({
      factor: k,
      weight: OPERATOR_MATCH_WEIGHTS[k],
      avgScore: Math.round((sums[k] / counts[k]) * 10) / 10,
      scoredCount: counts[k],
    }))
    .sort((a, b) => a.avgScore - b.avgScore);
}

function runScenario(scenario) {
  const rows = OPERATORS.map((op) => {
    const { score, breakdownDetails } = scoreOperatorMatchForDeal(
      scenario.deal,
      scenario.location,
      scenario.mp,
      scenario.si,
      op.prefill
    );
    return {
      operatorId: op.id,
      operatorName: op.name,
      note: op.note,
      score,
      breakdown: breakdownDetails,
    };
  }).sort((a, b) => (b.score ?? -1) - (a.score ?? -1));

  const s = stats(rows.map((r) => r.score));
  const factorAvgs = factorAverages(rows);
  const nonDifferentiating = factorAvgs.filter((f) => f.scoredCount >= rows.length - 1 && f.avgScore >= 70);
  const top = rows[0];
  const generic = rows.find((r) => r.operatorId === "generic-full-service-claims");
  const nicheWinsUnexpectedly =
    generic && top && generic.operatorId === top.operatorId
      ? "Generic capability claimant ranked #1"
      : null;
  const sparse = rows.find((r) => r.operatorId === "sparse-data-operator");

  return {
    scenarioId: scenario.id,
    label: scenario.label,
    distribution: s,
    ranking: rows.map((r, i) => ({
      rank: i + 1,
      operatorId: r.operatorId,
      operatorName: r.operatorName,
      score: r.score,
      note: r.note,
    })),
    factorAverages: factorAvgs,
    nonDifferentiatingFactors: nonDifferentiating.map((f) => f.factor),
    anomalies: [
      nicheWinsUnexpectedly,
      sparse && sparse.score != null && sparse.score >= (s.median || 0)
        ? `Sparse-data operator scored ${sparse.score} at/above median`
        : null,
      s.withinFiveOfTop >= Math.ceil(rows.length * 0.5)
        ? `${s.withinFiveOfTop}/${rows.length} operators within 5 points of top`
        : null,
      s.stdev != null && s.stdev < 8 ? `Low stdev (${s.stdev}) — clustering` : null,
    ].filter(Boolean),
    topBreakdownSample: top?.breakdown || null,
  };
}

function main() {
  const results = SCENARIOS.map(runScenario);
  const report = {
    auditUtility: "audit-operator-fit-score-simulation",
    mode: "synthetic-local-no-airtable",
    generatedAt: new Date().toISOString(),
    weights: OPERATOR_MATCH_WEIGHTS,
    operatorFixtureCount: OPERATORS.length,
    scenarioCount: SCENARIOS.length,
    scenarios: results,
    crossScenarioFindings: {
      genericRankedFirstCount: results.filter((r) => r.ranking[0]?.operatorId === "generic-full-service-claims")
        .length,
      avgStdev:
        Math.round(
          (results.reduce((a, r) => a + (r.distribution.stdev || 0), 0) / results.length) * 10
        ) / 10,
      avgWithinFiveOfTop:
        Math.round(
          (results.reduce((a, r) => a + (r.distribution.withinFiveOfTop || 0), 0) / results.length) * 10
        ) / 10,
      factorsOftenNonDifferentiating: (() => {
        const c = {};
        for (const r of results) {
          for (const f of r.nonDifferentiatingFactors || []) c[f] = (c[f] || 0) + 1;
        }
        return Object.entries(c)
          .sort((a, b) => b[1] - a[1])
          .map(([factor, scenarioHits]) => ({ factor, scenarioHits }));
      })(),
    },
  };

  mkdirSync(dirname(outPath), { recursive: true });
  writeFileSync(outPath, JSON.stringify(report, null, 2), "utf8");
  console.log("[score-sim] Wrote", outPath);
  console.log(
    JSON.stringify(
      {
        crossScenarioFindings: report.crossScenarioFindings,
        scenarioSummaries: results.map((r) => ({
          id: r.scenarioId,
          mean: r.distribution.mean,
          median: r.distribution.median,
          min: r.distribution.min,
          max: r.distribution.max,
          stdev: r.distribution.stdev,
          withinFiveOfTop: r.distribution.withinFiveOfTop,
          top: r.ranking[0]?.operatorName,
          anomalies: r.anomalies,
        })),
      },
      null,
      2
    )
  );
}

main();
