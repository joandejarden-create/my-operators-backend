/**
 * Propose census-backed Brand Alias Mapping rows for all active Brand Explorer brands.
 * Read-only on Hotel Census and Brand Footprint. Writes review files only.
 *
 * Usage:
 *   node scripts/propose-census-backed-brand-aliases.mjs
 *
 * Prerequisite (recommended): node scripts/audit-brand-explorer-census-coverage.mjs
 *
 * Outputs:
 *   reports/census-backed-brand-alias-proposals.json
 *   reports/census-backed-brand-alias-proposals.csv
 *   reports/census-backed-brand-alias-proposals-reviewed.example.json
 *   reports/brand-explorer-footprint-action-plan.csv
 */
import "../load-env.js";
import { mkdirSync, writeFileSync, readFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { getPlatformBase } from "../lib/hotel-census/platform-base.js";
import { getGovernanceFieldAvailability } from "../lib/hotel-census/census-governance.js";
import { CENSUS_FIELDS, HOTEL_CENSUS_TABLE, ALIAS_FIELDS } from "../lib/hotel-census/fields.js";
import { buildBrandCensusSummary } from "../lib/hotel-census/build-brand-census-summary.js";
import { exactMatchKey } from "../lib/hotel-census/brand-alias-resolve.js";
import { readFootprintVerificationFromFields } from "../lib/brand-footprint-verification.js";
import {
  brandFootprintTrustInput,
  displaySourceRecommendation,
} from "../lib/brand-explorer-footprint-trust.js";
import {
  buildAffiliationIndex,
  buildAffiliationInventory,
  buildAliasKeyIndex,
  attachExistingAliasStatus,
  proposeAliasesForBrand,
  proposalToCsvRow,
  csvEscape,
} from "../lib/census-backed-alias-proposals.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");
const JSON_OUT = join(REPORTS, "census-backed-brand-alias-proposals.json");
const CSV_OUT = join(REPORTS, "census-backed-brand-alias-proposals.csv");
const EXAMPLE_OUT = join(REPORTS, "census-backed-brand-alias-proposals-reviewed.example.json");
const ACTION_PLAN_OUT = join(REPORTS, "brand-explorer-footprint-action-plan.csv");
const COVERAGE_PATH = join(REPORTS, "brand-explorer-census-coverage.csv");

const BASICS_TABLE = "Brand Setup - Brand Basics";
const FOOTPRINT_TABLE = "Brand Setup - Brand Footprint";
const BRAND_NAME_FIELD = "Brand Name";
const PARENT_FIELD = "Parent Company";

function loadCoverageByBrand() {
  try {
    const text = readFileSync(COVERAGE_PATH, "utf8");
    const lines = text.trim().split(/\r?\n/);
    if (lines.length < 2) return new Map();
    const headers = parseCsvLine(lines[0]);
    const map = new Map();
    for (const line of lines.slice(1)) {
      const cols = parseCsvLine(line);
      const row = {};
      headers.forEach((h, i) => {
        row[h.trim()] = (cols[i] || "").trim();
      });
      if (row.brandName) map.set(exactMatchKey(row.brandName), row);
    }
    return map;
  } catch {
    return new Map();
  }
}

function parseCsvLine(line) {
  const out = [];
  let cur = "";
  let inQ = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (inQ) {
      if (c === '"' && line[i + 1] === '"') {
        cur += '"';
        i++;
      } else if (c === '"') inQ = false;
      else cur += c;
    } else if (c === '"') inQ = true;
    else if (c === ",") {
      out.push(cur);
      cur = "";
    } else cur += c;
  }
  out.push(cur);
  return out;
}

async function loadFootprintFields(mvpBase, brandName) {
  const esc = (brandName || "").replace(/"/g, '\\"');
  const recs = await mvpBase(FOOTPRINT_TABLE)
    .select({
      filterByFormula: `{${BRAND_NAME_FIELD}} = "${esc}"`,
      maxRecords: 1,
    })
    .all();
  return recs[0]?.fields || null;
}

function recommendedActionForBrand(ctx) {
  const {
    censusBacked,
    displayRec,
    footprintStatus,
    proposalCount,
    bestProposal,
    fallbackRecommended,
  } = ctx;

  if (censusBacked) {
    return { action: "Already Census-Backed", notes: "Census metrics display in Brand Explorer" };
  }

  if (proposalCount > 0 && bestProposal) {
    const review = bestProposal["Requires Human Review"];
    return {
      action: "Approve Alias",
      notes: review
        ? `Review and approve alias "${bestProposal["Alias / Source Brand Name"]}" (${bestProposal.censusEvidence?.openHotels || 0} open hotels)`
        : `High-confidence alias candidate: ${bestProposal["Alias / Source Brand Name"]}`,
    };
  }

  if (footprintStatus === "Verified") {
    return {
      action: "Mark MVP Footprint Verified",
      notes: "No census alias match; explicit Footprint Data Status Verified",
    };
  }

  if (footprintStatus === "Estimated") {
    return {
      action: "Mark MVP Footprint Estimated",
      notes: "No census alias match; explicit Footprint Data Status Estimated",
    };
  }

  if (footprintStatus === "Placeholder" || footprintStatus === "Needs Review") {
    return {
      action: "Keep Hidden",
      notes: `Footprint Data Status ${footprintStatus}`,
    };
  }

  if (displayRec === "Unverified / Do Not Display" && fallbackRecommended) {
    return {
      action: "Research Census Affiliation",
      notes: "No census affiliation match found for display name",
    };
  }

  return {
    action: "Needs Review",
    notes: "Classify footprint status or approve proposed alias",
  };
}

async function main() {
  const mvpKey = process.env.AIRTABLE_API_KEY;
  const mvpBaseId = process.env.AIRTABLE_BASE_ID;
  const platformBase = getPlatformBase();
  if (!mvpKey || !mvpBaseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");
  if (!platformBase) throw new Error("Set AIRTABLE_BASE_ID_ALT");

  const mvpBase = new Airtable({ apiKey: mvpKey }).base(mvpBaseId);
  const coverageMap = loadCoverageByBrand();

  console.log("Loading active brands (Brand Setup - Brand Basics)...");
  const brandRecs = await mvpBase(BASICS_TABLE)
    .select({
      filterByFormula: "FIND('Active', {Brand Status}) > 0",
      fields: [BRAND_NAME_FIELD, PARENT_FIELD],
    })
    .all();

  console.log("Loading Brand Alias Mapping (active + inactive)...");
  const aliasRecords = await platformBase(process.env.AIRTABLE_BRAND_ALIAS_TABLE || "Brand Alias Mapping")
    .select({ fields: Object.values(ALIAS_FIELDS), pageSize: 100 })
    .all();
  const aliasKeyIndex = buildAliasKeyIndex(aliasRecords);

  const governance = await getGovernanceFieldAvailability(platformBase);
  const selectFields = [
    CENSUS_FIELDS.affiliation,
    CENSUS_FIELDS.parentCompany,
    CENSUS_FIELDS.status,
    CENSUS_FIELDS.rooms,
    CENSUS_FIELDS.country,
    CENSUS_FIELDS.name,
  ];
  if (governance.includeInBrandExplorer) {
    selectFields.push(CENSUS_FIELDS.includeInBrandExplorer);
  }

  console.log("Loading Hotel Census (read-only)...");
  const censusRecords = await platformBase(HOTEL_CENSUS_TABLE)
    .select({ fields: selectFields, pageSize: 100 })
    .all();

  const inventory = buildAffiliationInventory(censusRecords, governance);
  const affiliationIndex = buildAffiliationIndex(inventory);
  console.log(`Census affiliation groups: ${inventory.length}`);

  const allProposals = [];
  const actionPlanRows = [];
  const stats = {
    activeBrandCount: brandRecs.length,
    alreadyCensusBacked: 0,
    withProposedAliases: 0,
    noCensusRecordsFound: 0,
    keepHidden: 0,
    totalProposals: 0,
    highConfidenceProposals: 0,
  };

  for (const rec of brandRecs) {
    const f = rec.fields || {};
    const brandName = exactMatchKey(f[BRAND_NAME_FIELD]);
    const parentCompany = exactMatchKey(f[PARENT_FIELD]);
    if (!brandName) continue;

    const summary = await buildBrandCensusSummary(brandName, parentCompany || null);
    const censusBacked = summary.available === true && summary.fallbackRecommended === false;

    const fpFields = await loadFootprintFields(mvpBase, brandName);
    const verification = readFootprintVerificationFromFields(fpFields);
    const footprintStatus = verification?.status || "";

    const trustBrand = brandFootprintTrustInput({
      name: brandName,
      parentCompany,
      footprint: { verification: verification || undefined },
      censusSummary: summary,
    });
    const displayRec = displaySourceRecommendation(trustBrand);

    const rawProposals = proposeAliasesForBrand(
      brandName,
      parentCompany,
      inventory,
      affiliationIndex
    );
    const proposals = attachExistingAliasStatus(rawProposals, aliasKeyIndex);
    allProposals.push(...proposals);

    const cov = coverageMap.get(brandName);
    const best = proposals[0] || null;

    if (censusBacked) stats.alreadyCensusBacked += 1;
    if (proposals.length) stats.withProposedAliases += 1;
    if (!censusBacked && !proposals.length) stats.noCensusRecordsFound += 1;

    const { action, notes } = recommendedActionForBrand({
      censusBacked,
      displayRec,
      footprintStatus,
      proposalCount: proposals.length,
      bestProposal: best,
      fallbackRecommended: summary.fallbackRecommended,
    });

    if (action === "Keep Hidden") stats.keepHidden += 1;

    actionPlanRows.push({
      "Brand Name": brandName,
      "Parent Company": parentCompany,
      "Current Census Hotels":
        cov?.censusOpenHotels ?? (summary.metrics?.totalOpenHotels ?? ""),
      "Current Census Keys": cov?.censusOpenKeys ?? (summary.metrics?.totalOpenKeys ?? ""),
      "Current Display Source Recommendation": cov?.["Display Source Recommendation"] ?? displayRec,
      "Current fallbackRecommended": summary.fallbackRecommended ? "yes" : "no",
      "Existing Alias Used": cov?.aliasUsed ?? (summary.alias?.usedAliasTable ? "yes" : "no"),
      "Proposed Alias Count": proposals.length,
      "Best Proposed Alias": best ? best["Alias / Source Brand Name"] : "",
      "Proposed Alias Open Hotels": best?.censusEvidence?.openHotels ?? "",
      "Proposed Alias Open Keys": best?.censusEvidence?.openKeys ?? "",
      "Recommended Action": action,
      Notes: notes,
      "Footprint Data Status": cov?.["Footprint Data Status"] ?? footprintStatus,
    });
  }

  stats.totalProposals = allProposals.length;
  stats.highConfidenceProposals = allProposals.filter(
    (p) => p["Match Confidence"] === "High" && !p["Requires Human Review"]
  ).length;

  mkdirSync(REPORTS, { recursive: true });

  const jsonPayload = {
    generatedAt: new Date().toISOString(),
    instructions:
      "Review proposals. Copy to census-backed-brand-alias-proposals-reviewed.json and set Approved: true (and Active: true) only for rows to seed. Never auto-activate from this file.",
    stats,
    affiliationInventoryCount: inventory.length,
    rows: allProposals,
  };
  writeFileSync(JSON_OUT, JSON.stringify(jsonPayload, null, 2), "utf8");

  const csvHeaders = [
    "Canonical Brand Name",
    "Alias / Source Brand Name",
    "Parent Company",
    "Open Hotels",
    "Open Keys",
    "Countries",
    "Match Confidence",
    "Proposal Reason",
    "Requires Human Review",
    "Approved",
    "Existing Alias Status",
    "Notes",
  ];
  const csvLines = [
    csvHeaders.join(","),
    ...allProposals.map((p) =>
      csvHeaders.map((h) => csvEscape(proposalToCsvRow(p)[h])).join(",")
    ),
  ];
  writeFileSync(CSV_OUT, csvLines.join("\n") + "\n", "utf8");

  const examplePayload = {
    instructions:
      "Copy to census-backed-brand-alias-proposals-reviewed.json. Set Approved: true and Active: true only for rows to upsert via scripts/seed-reviewed-brand-aliases.mjs",
    rows: allProposals.slice(0, 5).map((p) => ({
      ...p,
      Approved: false,
      Active: false,
      _example: "Set Approved true to seed this row",
    })),
  };
  writeFileSync(EXAMPLE_OUT, JSON.stringify(examplePayload, null, 2), "utf8");

  const apHeaders = Object.keys(actionPlanRows[0] || {});
  const apCsv = [
    apHeaders.join(","),
    ...actionPlanRows.map((r) => apHeaders.map((h) => csvEscape(r[h])).join(",")),
  ];
  writeFileSync(ACTION_PLAN_OUT, apCsv.join("\n") + "\n", "utf8");

  const topHigh = allProposals
    .filter((p) => p["Match Confidence"] === "High")
    .sort((a, b) => (b.censusEvidence?.openHotels || 0) - (a.censusEvidence?.openHotels || 0))
    .slice(0, 20);

  const researchBrands = actionPlanRows
    .filter((r) => r["Recommended Action"] === "Research Census Affiliation")
    .map((r) => r["Brand Name"]);

  console.log("\n=== Summary ===");
  console.log(JSON.stringify(stats, null, 2));
  console.log(`\nWrote ${JSON_OUT}`);
  console.log(`Wrote ${CSV_OUT}`);
  console.log(`Wrote ${EXAMPLE_OUT}`);
  console.log(`Wrote ${ACTION_PLAN_OUT}`);

  console.log("\n=== Top high-confidence proposals (sample) ===");
  topHigh.forEach((p) => {
    console.log(
      `  ${p["Canonical Brand Name"]} → ${p["Alias / Source Brand Name"]} (${p.censusEvidence?.openHotels || 0} hotels, review=${p["Requires Human Review"] ? "yes" : "no"}, existing=${p.existingAliasStatus})`
    );
  });

  console.log("\n=== Brands requiring research (sample) ===");
  researchBrands.slice(0, 15).forEach((n) => console.log(`  ${n}`));
  if (researchBrands.length > 15) {
    console.log(`  … and ${researchBrands.length - 15} more`);
  }

  console.log("\nConfirmations:");
  console.log("  Hotel Census: not modified (read-only)");
  console.log("  Radar: not modified");
  console.log("  Brand Footprint: not modified");
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
