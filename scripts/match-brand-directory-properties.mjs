/**
 * Phase 4K / 4N — Match Choice brand-directory property URLs to staging candidates + verified (READ-ONLY).
 */
import "../load-env.js";
import { join } from "path";
import { existsSync } from "fs";
import { SOURCE_TYPES } from "../lib/independent-census/fields.js";
import {
  loadPropertyUrlExtractReport,
  filterChoicePropertiesForMatch,
  loadVerifiedReadOnly,
  loadOsmCandidatesReadOnly,
  loadPrioritizedCandidatePool,
  compareToPhase4N,
  parseCountryFilterList,
  parseBatchIdList,
  matchChoicePropertiesToStaging,
  MATCH_CSV_COLUMNS,
  matchRowToCsv,
} from "../lib/independent-census/match-brand-directory-properties.js";
import { loadStagingCandidatesReadOnly } from "../lib/independent-census/match-staging-candidates.js";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";

const REPORTS_DIR = join(process.cwd(), "reports");

function parseArgs() {
  if (process.argv.includes("--apply")) {
    throw new Error("--apply is not supported. Read-only matching only.");
  }

  let propertyUrlReport =
    "reports/independent-census-choice-property-url-extract-cala-2026-05-20.json";
  let parentCompany = "Choice Hotels International";
  let candidateBatchId = "";
  let candidateBatchIds = "";
  let countryFilter = "";
  let sourceTypeFilter = SOURCE_TYPES.OSM;
  let allOsmCandidates = false;
  let candidateRetentionReport = "";
  let includeRetention = "";
  let phase4nCompareReport = "";
  let batchId = "choice-property-match-cala-2026-05-20";

  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--property-url-report" && argv[i + 1])
      propertyUrlReport = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--property-url-report="))
      propertyUrlReport = a.slice("--property-url-report=".length).replace(/^"|"$/g, "");
    else if (a === "--parent-company" && argv[i + 1])
      parentCompany = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--parent-company="))
      parentCompany = a.slice("--parent-company=".length).replace(/^"|"$/g, "");
    else if (a === "--candidate-batch-id" && argv[i + 1])
      candidateBatchId = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--candidate-batch-id="))
      candidateBatchId = a.slice("--candidate-batch-id=".length).replace(/^"|"$/g, "");
    else if (a === "--candidate-batch-ids" && argv[i + 1])
      candidateBatchIds = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--candidate-batch-ids="))
      candidateBatchIds = a.slice("--candidate-batch-ids=".length).replace(/^"|"$/g, "");
    else if (a === "--country-filter" && argv[i + 1])
      countryFilter = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--country-filter="))
      countryFilter = a.slice("--country-filter=".length).replace(/^"|"$/g, "");
    else if (a === "--source-type-filter" && argv[i + 1])
      sourceTypeFilter = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--source-type-filter="))
      sourceTypeFilter = a.slice("--source-type-filter=".length).replace(/^"|"$/g, "");
    else if (a === "--all-osm-candidates") allOsmCandidates = true;
    else if (a === "--candidate-retention-report" && argv[i + 1])
      candidateRetentionReport = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--candidate-retention-report="))
      candidateRetentionReport = a
        .slice("--candidate-retention-report=".length)
        .replace(/^"|"$/g, "");
    else if (a === "--include-retention" && argv[i + 1])
      includeRetention = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--include-retention="))
      includeRetention = a.slice("--include-retention=".length).replace(/^"|"$/g, "");
    else if (a === "--phase4n-compare-report" && argv[i + 1])
      phase4nCompareReport = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--phase4n-compare-report="))
      phase4nCompareReport = a.slice("--phase4n-compare-report=".length).replace(/^"|"$/g, "");
    else if (a === "--batch-id" && argv[i + 1]) batchId = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--batch-id="))
      batchId = a.slice("--batch-id=".length).replace(/^"|"$/g, "");
  }

  const batchIdList = parseBatchIdList(candidateBatchIds);
  if (candidateBatchId && !batchIdList.includes(candidateBatchId)) {
    batchIdList.unshift(candidateBatchId);
  }

  const reportBase = batchId.startsWith("choice-property-match")
    ? batchId
    : `choice-property-match-${batchId}`;

  if (!candidateRetentionReport && batchId.includes("prioritized")) {
    candidateRetentionReport =
      "reports/independent-census-candidate-coverage-dedupe-2026-05-20.json";
  }

  return {
    propertyUrlReportPath: join(process.cwd(), propertyUrlReport),
    parentCompany,
    candidateBatchId,
    candidateBatchIds: batchIdList,
    countryFilter: parseCountryFilterList(countryFilter),
    sourceTypeFilter,
    allOsmCandidates,
    candidateRetentionReportPath: candidateRetentionReport
      ? join(process.cwd(), candidateRetentionReport)
      : "",
    includeRetention,
    phase4nCompareReportPath: phase4nCompareReport
      ? join(process.cwd(), phase4nCompareReport)
      : "",
    batchId,
    jsonPath: join(REPORTS_DIR, `independent-census-${reportBase}.json`),
    csvPath: join(REPORTS_DIR, `independent-census-${reportBase}.csv`),
  };
}

async function loadCandidatePool(args) {
  if (args.allOsmCandidates) {
    return loadOsmCandidatesReadOnly({
      allOsmCandidates: true,
      sourceTypeFilter: args.sourceTypeFilter,
      countryFilters: args.countryFilter.length ? args.countryFilter : undefined,
    });
  }

  if (args.candidateBatchIds.length > 0) {
    return loadOsmCandidatesReadOnly({
      importBatchIds: args.candidateBatchIds,
      sourceTypeFilter: args.sourceTypeFilter,
      countryFilters: args.countryFilter.length ? args.countryFilter : undefined,
    });
  }

  const singleBatch =
    args.candidateBatchId || "osm-dominican-republic-hotel-focused-2026-05-20";
  const staging = await loadStagingCandidatesReadOnly({
    importBatchId: singleBatch,
    sourceType: args.sourceTypeFilter,
  });
  return {
    totalLoaded: staging.totalLoaded,
    matchingPoolSize: staging.matchingPoolSize,
    importBatchIds: [singleBatch],
    sourceTypeFilter: args.sourceTypeFilter,
    countryFilters: args.countryFilter,
    filterByFormula: `batch=${singleBatch}`,
    pool: { rows: staging.rows, byCountry: null, batchIds: [singleBatch] },
    rows: staging.rows,
  };
}

async function main() {
  const args = parseArgs();
  if (!existsSync(args.propertyUrlReportPath)) {
    throw new Error(`Property URL report not found: ${args.propertyUrlReportPath}`);
  }

  const phase = args.candidateRetentionReportPath
    ? "4P"
    : args.allOsmCandidates || args.candidateBatchIds.length > 1
      ? "4N"
      : "4K";

  console.log(`=== Choice property URL match (Phase ${phase}, read-only) ===\n`);
  console.log(`Property URL report: ${args.propertyUrlReportPath}`);
  console.log(`Parent company:      ${args.parentCompany}`);
  if (args.candidateRetentionReportPath) {
    console.log(`Candidate pool:      Phase 4O retention subset`);
    console.log(`Retention report:    ${args.candidateRetentionReportPath}`);
    console.log(
      `Include retention:   ${args.includeRetention || "keep_high_priority,enrich_next,keep_for_matching"}`
    );
  } else if (args.allOsmCandidates) {
    console.log(`Candidate pool:      all OSM CALA expansion batches`);
  } else if (args.candidateBatchIds.length) {
    console.log(`Candidate batches:   ${args.candidateBatchIds.join(", ")}`);
  } else {
    console.log(
      `Candidate batch:     ${args.candidateBatchId || "osm-dominican-republic-hotel-focused-2026-05-20"}`
    );
  }
  if (args.countryFilter.length) {
    console.log(`Country filter:      ${args.countryFilter.join(", ")}`);
  }
  console.log(`Source type filter:  ${args.sourceTypeFilter}\n`);

  const { data: extractReport, rows: allRows } = loadPropertyUrlExtractReport(
    args.propertyUrlReportPath
  );
  const choiceRows = filterChoicePropertiesForMatch(allRows);
  console.log(`Choice rows in extract:     ${allRows.length}`);
  console.log(`Eligible for match (CALA):  ${choiceRows.length}\n`);

  console.log("Loading Independent Hotel Source Candidates (read-only)…");
  let candidates;
  let poolMeta = null;
  if (args.candidateRetentionReportPath) {
    if (!existsSync(args.candidateRetentionReportPath)) {
      throw new Error(`Retention report not found: ${args.candidateRetentionReportPath}`);
    }
    candidates = await loadPrioritizedCandidatePool({
      retentionReportPath: args.candidateRetentionReportPath,
      includeRetention: args.includeRetention,
      allOsmCandidates: true,
      sourceTypeFilter: args.sourceTypeFilter,
    });
    poolMeta = candidates;
    console.log(`  Pool before filter:  ${candidates.totalLoadedBeforeFilter}`);
    console.log(`  Pool after filter:   ${candidates.totalLoadedAfterFilter}`);
    console.log(
      `  Reduction:           ${candidates.poolReduction} (${candidates.poolReductionPct}%)`
    );
    console.log("  Included by retention:");
    Object.entries(candidates.includedCounts || {})
      .sort((a, b) => b[1] - a[1])
      .forEach(([k, v]) => console.log(`    ${k}: ${v}`));
  } else {
    candidates = await loadCandidatePool(args);
    console.log(`  Candidates loaded: ${candidates.totalLoaded}`);
    if (candidates.importBatchIds?.length) {
      console.log(`  Distinct batches:  ${candidates.importBatchIds.length}`);
      candidates.importBatchIds.slice(0, 5).forEach((b) => console.log(`    - ${b}`));
      if (candidates.importBatchIds.length > 5) {
        console.log(`    … and ${candidates.importBatchIds.length - 5} more`);
      }
    }
  }

  const pool = candidates.pool?.byCountry
    ? candidates.pool
    : { rows: candidates.rows, byCountry: null };

  console.log("Loading Verified Independent Hotel Census (read-only)…");
  const verified = await loadVerifiedReadOnly();
  console.log(`  Verified loaded:   ${verified.totalLoaded}\n`);

  console.log("Matching…");
  const { rows, summary } = matchChoicePropertiesToStaging(choiceRows, pool, verified, {
    useCountryIndex: !!pool.byCountry,
    phase,
  });

  const comparisonPhase4N = compareToPhase4N(
    args.phase4nCompareReportPath,
    summary,
    poolMeta || {
      totalLoadedBeforeFilter: candidates.totalLoaded,
      totalLoadedAfterFilter: candidates.totalLoaded,
      poolReduction: 0,
      poolReductionPct: 0,
    }
  );

  const report = {
    generatedAt: new Date().toISOString(),
    phase: `${phase}-choice-property-match`,
    batchId: args.batchId,
    parentCompany: args.parentCompany,
    propertyUrlReportPath: args.propertyUrlReportPath,
    propertyUrlExtractBatchId: extractReport.batchId || null,
    candidatePool: {
      allOsmCandidates: args.allOsmCandidates || !!args.candidateRetentionReportPath,
      candidateBatchIds: candidates.importBatchIds || args.candidateBatchIds,
      sourceTypeFilter: args.sourceTypeFilter,
      countryFilter: args.countryFilter,
      filterByFormula: candidates.filterByFormula || null,
      retentionReportPath: args.candidateRetentionReportPath || null,
      includeRetention: args.includeRetention || null,
      totalLoadedBeforeFilter: poolMeta?.totalLoadedBeforeFilter ?? candidates.totalLoaded,
      totalLoadedAfterFilter: poolMeta?.totalLoadedAfterFilter ?? candidates.totalLoaded,
      poolReduction: poolMeta?.poolReduction ?? 0,
      includedRetentionCounts: poolMeta?.includedCounts ?? null,
    },
    comparisonPhase4N,
    dryRun: true,
    airtableWrites: false,
    hotelCensusWrites: false,
    brandSetupWrites: false,
    brandAliasWrites: false,
    independentCensusWrites: false,
    strFieldsUsed: false,
    googleApiUsed: false,
    propertyHtmlFetched: false,
    candidatesLoaded: poolMeta?.totalLoadedAfterFilter ?? candidates.totalLoaded,
    verifiedLoaded: verified.totalLoaded,
    summary,
    reportFiles: { json: args.jsonPath, csv: args.csvPath },
    matches: rows,
  };

  writeJson(args.jsonPath, report);
  writeCsv(args.csvPath, rows.map(matchRowToCsv), MATCH_CSV_COLUMNS);

  console.log("--- Candidate match confidence ---");
  console.log(`  high:   ${summary.candidateHigh}`);
  console.log(`  medium: ${summary.candidateMedium}`);
  console.log(`  low:    ${summary.candidateLow}`);
  console.log(`  none:   ${summary.candidateNone}`);
  console.log("\n--- Verified match confidence ---");
  console.log(`  high:   ${summary.verifiedHigh}`);
  console.log(`  medium: ${summary.verifiedMedium}`);
  console.log(`  low:    ${summary.verifiedLow}`);
  console.log(`  none:   ${summary.verifiedNone}`);
  console.log("\n--- Outcomes ---");
  console.log(`  Likely new official-source: ${summary.likelyNewOfficialSourceOpportunities}`);
  console.log(`  Manual review:              ${summary.needsManualReview}`);
  console.log(`  Link to candidate:          ${summary.linkToCandidateReview}`);
  console.log(`  Link to verified:           ${summary.linkToVerifiedReview}`);
  console.log(`  Unmatched brand:            ${summary.unmatchedBrandCount}`);

  console.log("\n  By country:");
  Object.entries(summary.byCountry)
    .sort((a, b) => b[1] - a[1])
    .forEach(([k, v]) => console.log(`    ${k}: ${v}`));

  console.log("\n  By brand (top):");
  Object.entries(summary.byBrand)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 12)
    .forEach(([k, v]) => console.log(`    ${k}: ${v}`));

  if (comparisonPhase4N) {
    console.log("\n--- vs Phase 4N (expanded OSM) ---");
    const p = comparisonPhase4N.candidatePoolLoaded;
    console.log(
      `  Pool size:     ${p.phase4N} → ${p.phase4P} (${p.reduction} dropped, ${p.reductionPct}%)`
    );
    const mc = comparisonPhase4N.matchConfidence;
    console.log(
      `  high:          ${mc.candidateHigh.phase4N} → ${mc.candidateHigh.phase4P} (${mc.candidateHigh.delta >= 0 ? "+" : ""}${mc.candidateHigh.delta})`
    );
    console.log(
      `  medium:        ${mc.candidateMedium.phase4N} → ${mc.candidateMedium.phase4P} (${mc.candidateMedium.delta >= 0 ? "+" : ""}${mc.candidateMedium.delta})`
    );
    console.log(
      `  low:           ${mc.candidateLow.phase4N} → ${mc.candidateLow.phase4P} (${mc.candidateLow.delta >= 0 ? "+" : ""}${mc.candidateLow.delta})`
    );
    console.log(
      `  none:          ${mc.candidateNone.phase4N} → ${mc.candidateNone.phase4P} (${mc.candidateNone.delta >= 0 ? "+" : ""}${mc.candidateNone.delta})`
    );
    const o = comparisonPhase4N.outcomes;
    console.log(
      `  link candidate: ${o.linkToCandidateReview.phase4N} → ${o.linkToCandidateReview.phase4P} (${o.linkToCandidateReview.delta >= 0 ? "+" : ""}${o.linkToCandidateReview.delta})`
    );
    console.log(
      `  manual review:  ${o.needsManualReview.phase4N} → ${o.needsManualReview.phase4P} (${o.needsManualReview.delta >= 0 ? "+" : ""}${o.needsManualReview.delta})`
    );
    console.log(
      `  new official:   ${o.likelyNewOfficialSourceOpportunities.phase4N} → ${o.likelyNewOfficialSourceOpportunities.phase4P} (${o.likelyNewOfficialSourceOpportunities.delta >= 0 ? "+" : ""}${o.likelyNewOfficialSourceOpportunities.delta})`
    );
  }

  console.log("\nReport files:");
  console.log(`  ${args.jsonPath}`);
  console.log(`  ${args.csvPath}`);
  console.log("\n✓ Read-only complete. No Airtable writes.");
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
