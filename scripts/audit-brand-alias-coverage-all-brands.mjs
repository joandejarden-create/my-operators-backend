/**
 * Full Brand Setup → Brand Alias Mapping coverage audit (all parent companies).
 * Read-only on Hotel Census, Radar, and Brand Footprint. Writes report files only.
 *
 * Usage:
 *   node scripts/audit-brand-alias-coverage-all-brands.mjs
 *   node scripts/audit-brand-alias-coverage-all-brands.mjs --includeInactive=true
 *   node scripts/audit-brand-alias-coverage-all-brands.mjs --activeOnly=false
 *   node scripts/audit-brand-alias-coverage-all-brands.mjs --parentCompany="Marriott International"
 *
 * Outputs:
 *   reports/all-brand-alias-coverage.csv
 *   reports/all-brand-alias-proposals.json
 *   reports/all-brand-alias-proposals.csv
 *   reports/all-brand-alias-proposals-reviewed.example.json
 *   reports/brand-alias-coverage-by-parent.csv
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { getPlatformBase } from "../lib/hotel-census/platform-base.js";
import { getGovernanceFieldAvailability } from "../lib/hotel-census/census-governance.js";
import {
  CENSUS_FIELDS,
  HOTEL_CENSUS_TABLE,
  ALIAS_FIELDS,
  CENSUS_INDEPENDENT_AFFILIATION,
  STATUS_OPEN,
  STATUS_PIPELINE,
} from "../lib/hotel-census/fields.js";
import {
  exactMatchKey,
  normalizeParentCompanyKey,
  resolveBrandAffiliationMatchers,
} from "../lib/hotel-census/brand-alias-resolve.js";
import { shouldIncludeRowForBrandExplorer } from "../lib/hotel-census/census-governance.js";
import { readFootprintVerificationFromFields } from "../lib/brand-footprint-verification.js";
import {
  brandFootprintTrustInput,
  displaySourceRecommendation,
} from "../lib/brand-explorer-footprint-trust.js";
import {
  buildAffiliationIndex,
  buildAffiliationInventory,
  buildAliasKeyIndex,
  buildAliasesByCanonical,
  attachExistingAliasStatus,
  proposeAliasesForBrand,
  csvEscape,
  detectParentCompanyMismatch,
} from "../lib/census-backed-alias-proposals.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");
const COVERAGE_CSV = join(REPORTS, "all-brand-alias-coverage.csv");
const PROPOSALS_JSON = join(REPORTS, "all-brand-alias-proposals.json");
const PROPOSALS_CSV = join(REPORTS, "all-brand-alias-proposals.csv");
const PROPOSALS_EXAMPLE = join(REPORTS, "all-brand-alias-proposals-reviewed.example.json");
const BY_PARENT_CSV = join(REPORTS, "brand-alias-coverage-by-parent.csv");

const BASICS_TABLE = "Brand Setup - Brand Basics";
const FOOTPRINT_TABLE = "Brand Setup - Brand Footprint";
const BRAND_NAME_FIELD = "Brand Name";
const PARENT_FIELD = "Parent Company";
const STATUS_FIELD = "Brand Status";

function parseArgs(argv) {
  let activeOnly = true;
  let includeInactive = false;
  const parentFilters = [];

  for (const arg of argv) {
    if (arg === "--includeInactive=true") includeInactive = true;
    if (arg === "--activeOnly=false") activeOnly = false;
    if (arg === "--activeOnly=true") activeOnly = true;
    if (arg.startsWith("--parentCompany=")) {
      parentFilters.push(arg.slice("--parentCompany=".length).trim());
    }
  }
  if (includeInactive) activeOnly = false;

  return { activeOnly, includeInactive, parentFilters };
}

function isBrandStatusActive(statusRaw) {
  const s = String(statusRaw ?? "");
  return /active/i.test(s);
}

function matchesParentFilter(mvpParent, filters) {
  if (!filters.length) return true;
  const norm = normalizeParentCompanyKey(mvpParent);
  if (!norm) return false;

  return filters.some((f) => {
    const fn = normalizeParentCompanyKey(f);
    if (!fn) return false;
    if (norm === fn) return true;
    if (norm.includes(fn) || fn.includes(norm)) return true;
    const pairs = [
      ["choice", "choice"],
      ["marriott", "marriott"],
      ["hilton", "hilton"],
      ["hyatt", "hyatt"],
      ["ihg", "ihg"],
      ["accor", "accor"],
    ];
    return pairs.some(([a, b]) => norm.includes(a) && fn.includes(b));
  });
}

function parseNum(v) {
  const n = parseInt(v, 10);
  return Number.isFinite(n) ? n : 0;
}

function normalizeStatus(raw) {
  const s = exactMatchKey(raw);
  if (s === "open" || s === STATUS_OPEN) return STATUS_OPEN;
  if (s === "pipeline" || s === STATUS_PIPELINE) return STATUS_PIPELINE;
  return s || "Other";
}

function readActiveAliasRows(aliasRecords) {
  return aliasRecords
    .map((rec) => {
      const f = rec.fields || {};
      const v = f.Active ?? f.active;
      const active =
        v === false
          ? false
          : ["yes", "true", "1", "active"].includes(String(v ?? "").trim().toLowerCase());
      if (!active) return null;
      return {
        recordId: rec.id,
        canonicalBrandName: exactMatchKey(f[ALIAS_FIELDS.canonicalBrandName]),
        aliasSourceBrandName: exactMatchKey(f[ALIAS_FIELDS.aliasSourceBrandName]),
        parentCompany: exactMatchKey(f[ALIAS_FIELDS.parentCompany]),
        active: true,
        matchConfidence: f[ALIAS_FIELDS.matchConfidence] ?? null,
        notes: f[ALIAS_FIELDS.notes] ?? null,
      };
    })
    .filter(Boolean);
}

/**
 * In-memory census rollup (same filters as aggregateCensusPresenceSummary).
 */
function summarizeCensusFromCache(censusRecords, affiliationMatchers, parentCompany, governance) {
  const matcherSet = new Set(
    (affiliationMatchers || []).map((a) => exactMatchKey(a)).filter(Boolean)
  );
  if (matcherSet.size === 0) {
    return { ok: false, error: "No affiliation matchers" };
  }

  const parentFilter = exactMatchKey(parentCompany);
  let openHotels = 0;
  let openKeys = 0;
  let pipelineHotels = 0;
  const countries = new Set();
  let recordsMatched = 0;

  for (const rec of censusRecords) {
    const f = rec.fields || {};
    const affiliation = exactMatchKey(f[CENSUS_FIELDS.affiliation]);
    if (!matcherSet.has(affiliation)) continue;
    if (affiliation === CENSUS_INDEPENDENT_AFFILIATION) continue;

    if (
      !shouldIncludeRowForBrandExplorer(
        f,
        CENSUS_FIELDS.includeInBrandExplorer,
        governance.includeInBrandExplorer
      )
    ) {
      continue;
    }

    const rowParent = exactMatchKey(f[CENSUS_FIELDS.parentCompany]);
    if (parentFilter && rowParent !== parentFilter) continue;

    recordsMatched += 1;
    const status = normalizeStatus(f[CENSUS_FIELDS.status]);
    const rooms = parseNum(f[CENSUS_FIELDS.rooms]);
    const country = exactMatchKey(f[CENSUS_FIELDS.country]);

    if (status === STATUS_OPEN) {
      openHotels += 1;
      openKeys += rooms;
      if (country && country !== "Unknown") countries.add(country);
    } else if (status === STATUS_PIPELINE) {
      pipelineHotels += 1;
    }
  }

  return {
    ok: true,
    metrics: {
      totalOpenHotels: openHotels,
      totalOpenKeys: openKeys,
      totalPipelineHotels: pipelineHotels,
      countryCount: countries.size,
    },
    censusRecordsMatched: recordsMatched,
  };
}

async function buildCensusSummaryFromCache(
  brandName,
  parentCompany,
  activeAliases,
  censusRecords,
  governance
) {
  const parent = (parentCompany || "").trim() || null;
  const brand = exactMatchKey(brandName);
  if (!brand) {
    return { available: false, fallbackRecommended: true, warnings: ["brand name required"] };
  }

  const resolution = await resolveBrandAffiliationMatchers(brand, parent, {
    preloadedAliases: activeAliases,
  });

  if (!resolution.ok) {
    return { available: false, fallbackRecommended: true, warnings: [resolution.error] };
  }

  let censusParentFilter = parent;

  if (parent && resolution.aliasRecordsUsed?.length) {
    const normReq = normalizeParentCompanyKey(parent);
    const matched = resolution.aliasRecordsUsed.find(
      (r) => normalizeParentCompanyKey(r.parentCompany) === normReq
    );
    if (matched?.parentCompany) {
      censusParentFilter = matched.parentCompany;
    } else if (normReq) {
      censusParentFilter = null;
    }
  } else if (parent && resolution.canonicalBrandName && !resolution.usedAliasTable) {
    censusParentFilter = null;
  }

  let summary = summarizeCensusFromCache(
    censusRecords,
    resolution.affiliationMatchers,
    censusParentFilter,
    governance
  );

  if (
    summary.ok &&
    censusParentFilter &&
    resolution.usedAliasTable &&
    summary.metrics.totalOpenHotels === 0 &&
    summary.metrics.totalPipelineHotels === 0
  ) {
    const relaxed = summarizeCensusFromCache(
      censusRecords,
      resolution.affiliationMatchers,
      null,
      governance
    );
    if (
      relaxed.ok &&
      (relaxed.metrics.totalOpenHotels > 0 || relaxed.metrics.totalPipelineHotels > 0)
    ) {
      summary = relaxed;
      censusParentFilter = null;
    }
  }

  if (!summary.ok) {
    return { available: false, fallbackRecommended: true, warnings: [summary.error] };
  }

  const fallbackRecommended =
    !resolution.usedAliasTable ||
    resolution.warnings.some((w) =>
      /^(ALIAS_TABLE_UNAVAILABLE|NO_ACTIVE_ALIAS_ROWS|NO_ALIAS_FOR_REQUESTED_BRAND|NO_ALIAS_ROWS_FOR_CANONICAL)/.test(
        w
      )
    ) ||
    (summary.metrics.totalOpenHotels === 0 && summary.metrics.totalPipelineHotels === 0);

  return {
    available: true,
    fallbackRecommended,
    alias: {
      usedAliasTable: resolution.usedAliasTable,
      affiliationMatchers: resolution.affiliationMatchers,
    },
    metrics: summary.metrics,
    warnings: resolution.warnings,
  };
}

function recommendedAction(ctx) {
  const {
    parentMismatch,
    censusBacked,
    footprintStatus,
    activeAliasCount,
    proposals,
    fallbackRecommended,
    displayRec,
  } = ctx;

  if (parentMismatch) {
    return {
      action: "Parent Company Mismatch",
      notes: "Census affiliation exists under a different parent company than MVP",
    };
  }

  if (censusBacked) {
    return { action: "Already Census-Backed", notes: "Census metrics available without fallback" };
  }

  if (activeAliasCount > 0 && fallbackRecommended) {
    return {
      action: "Deactivate Bad Alias",
      notes: "Active alias rows present but census rollup is empty or fallback",
    };
  }

  const missingExact = proposals.filter(
    (p) => p.existingAliasStatus === "missing" && !p["Requires Human Review"]
  );
  if (missingExact.length) {
    return {
      action: "Add Alias",
      notes: `Exact proposals: ${missingExact.map((p) => p["Alias / Source Brand Name"]).join("; ")}`,
    };
  }

  const missingReview = proposals.filter((p) => p.existingAliasStatus === "missing");
  if (missingReview.length) {
    return {
      action: "Review Proposed Alias",
      notes: missingReview.map((p) => p["Alias / Source Brand Name"]).join("; "),
    };
  }

  if (footprintStatus === "Verified") {
    return {
      action: "Mark MVP Footprint Verified",
      notes: "No census match; Footprint Data Status is Verified",
    };
  }

  if (footprintStatus === "Estimated") {
    return {
      action: "Mark MVP Footprint Estimated",
      notes: "No census match; Footprint Data Status is Estimated",
    };
  }

  if (footprintStatus === "Placeholder" || footprintStatus === "Needs Review") {
    return {
      action: "Keep Hidden",
      notes: `Footprint Data Status ${footprintStatus}`,
    };
  }

  if (displayRec === "Unverified / Do Not Display" && fallbackRecommended && !proposals.length) {
    return { action: "Needs Research", notes: "No census affiliation proposals found" };
  }

  return { action: "Needs Research", notes: "Review footprint status or census naming" };
}

async function loadAllFootprints(mvpBase) {
  const map = new Map();
  const recs = await mvpBase(FOOTPRINT_TABLE).select({ pageSize: 100 }).all();
  for (const rec of recs) {
    const name = exactMatchKey(rec.fields?.[BRAND_NAME_FIELD]);
    if (name && !map.has(name)) map.set(name, rec.fields || {});
  }
  return map;
}

function inventoryToReportRows(inventory) {
  return inventory.map((g) => ({
    Affiliation: g.affiliation,
    "Parent Company": g.parentCompany,
    "Open Hotels": g.openHotels,
    "Open Keys": g.openKeys,
    "Pipeline Hotels": g.pipelineHotels,
    "Country Count": g.countryCount,
    "Example Hotel Names": (g.exampleHotelNames || []).join("; "),
    "Example Countries": (g.exampleCountries || []).join("; "),
  }));
}

function proposalToFullCsvRow(p) {
  const ev = p.censusEvidence || {};
  return {
    "Canonical Brand Name": p["Canonical Brand Name"],
    "Alias / Source Brand Name": p["Alias / Source Brand Name"],
    "Parent Company": p["Parent Company"],
    Active: p.Active === true ? "yes" : "no",
    Approved: p.Approved === true ? "yes" : "no",
    "Match Confidence": p["Match Confidence"],
    "Requires Human Review": p["Requires Human Review"] ? "yes" : "no",
    "Proposal Reason": p["Proposal Reason"],
    "Open Hotels": ev.openHotels ?? "",
    "Open Keys": ev.openKeys ?? "",
    "Pipeline Hotels": ev.pipelineHotels ?? "",
    "Country Count": ev.countryCount ?? "",
    "Parent Company In Census": ev.parentCompanyInCensus ?? "",
    "Example Hotel Names": (ev.exampleHotelNames || []).join("; "),
    "Example Countries": (ev.exampleCountries || []).join("; "),
    "Existing Alias Status": p.existingAliasStatus || "",
    Notes: p.Notes || "",
  };
}

async function main() {
  const { activeOnly, includeInactive, parentFilters } = parseArgs(process.argv.slice(2));

  const mvpKey = process.env.AIRTABLE_API_KEY;
  const mvpBaseId = process.env.AIRTABLE_BASE_ID;
  const platformBase = getPlatformBase();
  if (!mvpKey || !mvpBaseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");
  if (!platformBase) throw new Error("Set AIRTABLE_BASE_ID_ALT");

  const mvpBase = new Airtable({ apiKey: mvpKey }).base(mvpBaseId);

  console.log(
    `Loading Brand Setup - Brand Basics (${activeOnly ? "Active only" : "all statuses"})...`
  );
  const brandRecs = await mvpBase(BASICS_TABLE)
    .select({
      fields: [BRAND_NAME_FIELD, PARENT_FIELD, STATUS_FIELD],
      pageSize: 100,
    })
    .all();

  let brands = brandRecs
    .map((rec) => ({
      id: rec.id,
      fields: rec.fields || {},
    }))
    .filter((b) => exactMatchKey(b.fields[BRAND_NAME_FIELD]));

  if (activeOnly) {
    brands = brands.filter((b) => isBrandStatusActive(b.fields[STATUS_FIELD]));
  }

  if (parentFilters.length) {
    brands = brands.filter((b) =>
      matchesParentFilter(b.fields[PARENT_FIELD], parentFilters)
    );
  }

  console.log(`Brands in scope: ${brands.length}`);

  console.log("Loading Brand Alias Mapping...");
  const aliasRecords = await platformBase(process.env.AIRTABLE_BRAND_ALIAS_TABLE || "Brand Alias Mapping")
    .select({ fields: Object.values(ALIAS_FIELDS), pageSize: 100 })
    .all();
  const aliasKeyIndex = buildAliasKeyIndex(aliasRecords);
  const aliasesByCanonical = buildAliasesByCanonical(aliasRecords);
  const activeAliases = readActiveAliasRows(aliasRecords);

  console.log("Loading Brand Footprint (read-only)...");
  const footprintByBrand = await loadAllFootprints(mvpBase);

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

  const coverageRows = [];
  const allProposals = [];
  const parentBuckets = new Map();

  const stats = {
    brandsReviewed: 0,
    censusBacked: 0,
    withActiveAlias: 0,
    needingAlias: 0,
    noCensusMatch: 0,
    mvpVerifiedRecommended: 0,
    keepHiddenOrResearch: 0,
    parentMismatch: 0,
    totalProposals: 0,
  };

  let processed = 0;
  for (const b of brands) {
    const f = b.fields;
    const brandName = exactMatchKey(f[BRAND_NAME_FIELD]);
    const parentCompany = exactMatchKey(f[PARENT_FIELD]);
    const brandStatus = exactMatchKey(f[STATUS_FIELD]);
    const shownInExplorer = isBrandStatusActive(brandStatus);

    const aliasBucket = aliasesByCanonical.get(brandName) || { active: [], inactive: [] };
    const activeAliasCount = aliasBucket.active.length;
    const inactiveAliasCount = aliasBucket.inactive.length;

    const fpFields = footprintByBrand.get(brandName) || null;
    const verification = readFootprintVerificationFromFields(fpFields);
    const footprintStatus = verification?.status || "";

    const summary = await buildCensusSummaryFromCache(
      brandName,
      parentCompany || null,
      activeAliases,
      censusRecords,
      governance
    );

    const censusBacked = summary.available === true && summary.fallbackRecommended === false;
    const metrics = summary.metrics || {};
    const parentMismatch = detectParentCompanyMismatch(
      brandName,
      parentCompany,
      affiliationIndex
    );

    const trustBrand = brandFootprintTrustInput({
      name: brandName,
      parentCompany,
      footprint: { verification: verification || undefined },
      censusSummary: summary.available
        ? {
            available: true,
            fallbackRecommended: summary.fallbackRecommended,
            metrics: summary.metrics,
          }
        : { available: false, fallbackRecommended: true },
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

    const { action, notes } = recommendedAction({
      parentMismatch,
      censusBacked,
      footprintStatus,
      activeAliasCount,
      proposals,
      fallbackRecommended: summary.fallbackRecommended,
      displayRec,
    });

    coverageRows.push({
      "Brand Name": brandName,
      "Brand Setup Record ID": b.id,
      "Parent Company": parentCompany,
      "Brand Status": brandStatus,
      "Shown In Brand Explorer": shownInExplorer ? "yes" : "no",
      "Active Alias Count": activeAliasCount,
      "Inactive Alias Count": inactiveAliasCount,
      "Active Alias Source Brand Names": aliasBucket.active.join("; "),
      "Matching Census Hotels": metrics.totalOpenHotels ?? "",
      "Matching Census Keys": metrics.totalOpenKeys ?? "",
      "Country Count": metrics.countryCount ?? "",
      "Pipeline Hotels": metrics.totalPipelineHotels ?? "",
      fallbackRecommended: summary.fallbackRecommended ? "yes" : "no",
      "Current Display Source Recommendation": displayRec,
      "Footprint Data Status": footprintStatus,
      "Footprint Data Source": verification?.source ?? "",
      "Footprint Figures As Of": verification?.figuresAsOf ?? "",
      "Recommended Action": action,
      "Recommended Action Notes": notes,
      "Proposal Count": proposals.length,
    });

    stats.brandsReviewed += 1;
    if (censusBacked) stats.censusBacked += 1;
    if (activeAliasCount > 0) stats.withActiveAlias += 1;
    if (!censusBacked && proposals.some((p) => p.existingAliasStatus === "missing")) {
      stats.needingAlias += 1;
    }
    if (!censusBacked && proposals.length === 0) stats.noCensusMatch += 1;
    if (action === "Mark MVP Footprint Verified") stats.mvpVerifiedRecommended += 1;
    if (action === "Keep Hidden" || action === "Needs Research") stats.keepHiddenOrResearch += 1;
    if (action === "Parent Company Mismatch") stats.parentMismatch += 1;

    const parentKey = parentCompany || "(no parent)";
    if (!parentBuckets.has(parentKey)) {
      parentBuckets.set(parentKey, {
        "Parent Company": parentCompany || "(no parent)",
        "Total Brand Setup Brands": 0,
        "Census-Backed Brands": 0,
        "Brands With Active Alias": 0,
        "Brands Needing Alias": 0,
        "Brands With No Census Match": 0,
        "Brands Using Verified MVP": 0,
        "Brands Hidden / Needs Research": 0,
      });
    }
    const bucket = parentBuckets.get(parentKey);
    bucket["Total Brand Setup Brands"] += 1;
    if (censusBacked) bucket["Census-Backed Brands"] += 1;
    if (activeAliasCount > 0) bucket["Brands With Active Alias"] += 1;
    if (!censusBacked && proposals.some((p) => p.existingAliasStatus === "missing")) {
      bucket["Brands Needing Alias"] += 1;
    }
    if (!censusBacked && proposals.length === 0) bucket["Brands With No Census Match"] += 1;
    if (action === "Mark MVP Footprint Verified" || footprintStatus === "Verified") {
      bucket["Brands Using Verified MVP"] += 1;
    }
    if (action === "Keep Hidden" || action === "Needs Research") {
      bucket["Brands Hidden / Needs Research"] += 1;
    }

    processed += 1;
    if (processed % 25 === 0) {
      console.log(`  … ${processed}/${brands.length} brands`);
    }
  }

  stats.totalProposals = allProposals.length;

  mkdirSync(REPORTS, { recursive: true });

  const coverageHeaders = Object.keys(coverageRows[0] || {});
  writeFileSync(
    COVERAGE_CSV,
    [
      coverageHeaders.join(","),
      ...coverageRows.map((r) => coverageHeaders.map((h) => csvEscape(r[h])).join(",")),
    ].join("\n") + "\n",
    "utf8"
  );

  const proposalPayload = {
    generatedAt: new Date().toISOString(),
    options: { activeOnly, includeInactive, parentFilters },
    stats,
    affiliationInventoryCount: inventory.length,
    affiliationInventory: inventoryToReportRows(inventory),
    rows: allProposals,
  };
  writeFileSync(PROPOSALS_JSON, JSON.stringify(proposalPayload, null, 2), "utf8");

  const propCsvHeaders = Object.keys(proposalToFullCsvRow(allProposals[0] || {}));
  writeFileSync(
    PROPOSALS_CSV,
    [
      propCsvHeaders.join(","),
      ...allProposals.map((p) =>
        propCsvHeaders.map((h) => csvEscape(proposalToFullCsvRow(p)[h])).join(",")
      ),
    ].join("\n") + "\n",
    "utf8"
  );

  writeFileSync(
    PROPOSALS_EXAMPLE,
    JSON.stringify(
      {
        instructions:
          "Copy to all-brand-alias-proposals-reviewed.json. Set Approved: true only for rows to seed. Active: true only when reviewed. Never auto-activate guessed aliases.",
        rows: allProposals.slice(0, 8).map((p) => ({
          ...p,
          Approved: false,
          Active: false,
          _example: "Set Approved true to seed via scripts/seed-reviewed-brand-aliases.mjs",
        })),
      },
      null,
      2
    ),
    "utf8"
  );

  const parentRows = [...parentBuckets.values()].sort(
    (a, b) => b["Total Brand Setup Brands"] - a["Total Brand Setup Brands"]
  );
  const parentHeaders = Object.keys(parentRows[0] || {});
  writeFileSync(
    BY_PARENT_CSV,
    [
      parentHeaders.join(","),
      ...parentRows.map((r) => parentHeaders.map((h) => csvEscape(r[h])).join(",")),
    ].join("\n") + "\n",
    "utf8"
  );

  const topByParent = new Map();
  for (const p of allProposals) {
    if (p.existingAliasStatus !== "missing") continue;
    const parent = p["Parent Company"] || "(no parent)";
    if (!topByParent.has(parent)) topByParent.set(parent, []);
    const list = topByParent.get(parent);
    if (list.length < 5) {
      list.push({
        brand: p["Canonical Brand Name"],
        alias: p["Alias / Source Brand Name"],
        hotels: p.censusEvidence?.openHotels || 0,
        review: p["Requires Human Review"],
      });
    }
  }

  console.log("\n=== Summary ===");
  console.log(JSON.stringify(stats, null, 2));
  console.log(`\nWrote ${COVERAGE_CSV}`);
  console.log(`Wrote ${PROPOSALS_JSON}`);
  console.log(`Wrote ${PROPOSALS_CSV}`);
  console.log(`Wrote ${PROPOSALS_EXAMPLE}`);
  console.log(`Wrote ${BY_PARENT_CSV}`);

  console.log("\n=== By parent company (top 12) ===");
  parentRows.slice(0, 12).forEach((r) => {
    console.log(
      `  ${r["Parent Company"]}: ${r["Total Brand Setup Brands"]} brands, ${r["Census-Backed Brands"]} census-backed, ${r["Brands Needing Alias"]} need alias`
    );
  });

  console.log("\n=== Top proposed aliases by parent (missing alias, sample) ===");
  for (const [parent, list] of [...topByParent.entries()].slice(0, 8)) {
    console.log(`  ${parent}:`);
    list.forEach((x) => {
      console.log(
        `    ${x.brand} → ${x.alias} (${x.hotels} hotels, review=${x.review ? "yes" : "no"})`
      );
    });
  }

  const mismatchParents = parentRows.filter((r) => {
    const cov = coverageRows.filter(
      (c) =>
        c["Parent Company"] === r["Parent Company"] &&
        c["Recommended Action"] === "Parent Company Mismatch"
    );
    return cov.length > 0;
  });
  if (mismatchParents.length) {
    console.log("\n=== Parent company mismatch brands (sample) ===");
    coverageRows
      .filter((c) => c["Recommended Action"] === "Parent Company Mismatch")
      .slice(0, 10)
      .forEach((c) => {
        console.log(`  ${c["Brand Name"]} (${c["Parent Company"]})`);
      });
  }

  console.log("\nConfirmations:");
  console.log("  Hotel Census: not modified (read-only)");
  console.log("  Radar: not modified");
  console.log("  Brand Footprint: not overwritten (read-only)");
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
