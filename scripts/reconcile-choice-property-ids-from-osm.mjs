/**
 * Phase 4T — Reconcile Choice property IDs from OSM website URLs (READ-ONLY).
 */
import "../load-env.js";
import { existsSync } from "fs";
import { join } from "path";
import {
  loadOsmCandidatesForReconciliation,
  loadPropertyUrlExtractReport,
  indexSitemapProperties,
  reconcileCandidatesFromOsmWebsites,
  summarizeReconciliation,
  reconciliationRowToCsv,
  RECONCILE_CSV_COLUMNS,
} from "../lib/independent-census/choice-property-id-reconciliation.js";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";

const REPORTS_DIR = join(process.cwd(), "reports");

function parseArgs() {
  let propertyUrlReport = "";
  let candidateRetentionReport = "";
  let batchId = "choice-property-id-reconcile-2026-05-20";
  let includeRetention = "";
  let skipRetentionFilter = false;

  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--property-url-report" && argv[i + 1])
      propertyUrlReport = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--property-url-report="))
      propertyUrlReport = a.slice("--property-url-report=".length).replace(/^"|"$/g, "");
    else if (a === "--candidate-retention-report" && argv[i + 1])
      candidateRetentionReport = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--candidate-retention-report="))
      candidateRetentionReport = a
        .slice("--candidate-retention-report=".length)
        .replace(/^"|"$/g, "");
    else if (a === "--batch-id" && argv[i + 1]) batchId = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--batch-id="))
      batchId = a.slice("--batch-id=".length).replace(/^"|"$/g, "");
    else if (a === "--include-retention" && argv[i + 1])
      includeRetention = argv[++i];
    else if (a.startsWith("--include-retention="))
      includeRetention = a.slice("--include-retention=".length);
    else if (a === "--all-osm-candidates") skipRetentionFilter = true;
    else if (a === "--apply") {
      throw new Error("--apply is not supported. Phase 4T is report-only.");
    }
  }

  if (!propertyUrlReport) {
    throw new Error("Required: --property-url-report");
  }

  const reportSlug = batchId.includes("reconcile")
    ? batchId.replace(/-reconcile-/, "-reconciliation-")
    : `${batchId}-reconciliation`;

  return {
    propertyUrlReportPath: join(process.cwd(), propertyUrlReport),
    candidateRetentionReportPath: candidateRetentionReport
      ? join(process.cwd(), candidateRetentionReport)
      : null,
    batchId,
    reportSlug,
    includeRetention,
    skipRetentionFilter,
  };
}

async function main() {
  const args = parseArgs();
  if (!existsSync(args.propertyUrlReportPath)) {
    throw new Error(`Property URL report not found: ${args.propertyUrlReportPath}`);
  }
  if (
    !args.skipRetentionFilter &&
    args.candidateRetentionReportPath &&
    !existsSync(args.candidateRetentionReportPath)
  ) {
    throw new Error(
      `Retention report not found: ${args.candidateRetentionReportPath}`
    );
  }

  console.log("=== Choice property ID reconciliation (Phase 4T, read-only) ===\n");
  console.log(`Batch ID:              ${args.batchId}`);
  console.log(`Property URL report:   ${args.propertyUrlReportPath}`);
  if (args.candidateRetentionReportPath && !args.skipRetentionFilter) {
    console.log(`Retention report:      ${args.candidateRetentionReportPath}`);
    console.log(
      `Include retention:     ${args.includeRetention || "keep_high_priority,enrich_next,keep_for_matching"}`
    );
  } else {
    console.log("Retention filter:      off (all CALA OSM candidates)");
  }

  console.log("\nLoading Choice property sitemap extract (local)…");
  const extractReport = loadPropertyUrlExtractReport(args.propertyUrlReportPath);
  const sitemapIndex = indexSitemapProperties(extractReport);
  console.log(`  Sitemap property rows: ${(extractReport.propertyRows || []).length}`);

  console.log("Loading Independent Hotel Source Candidates (read-only)…");
  const loaded = await loadOsmCandidatesForReconciliation({
    retentionReportPath: args.skipRetentionFilter
      ? null
      : args.candidateRetentionReportPath,
    includeRetention: args.includeRetention,
    useRetentionFilter: !args.skipRetentionFilter && !!args.candidateRetentionReportPath,
  });
  console.log(`  OSM candidates scanned: ${loaded.totalScanned}`);

  const { rows, withChoiceUrls } = reconcileCandidatesFromOsmWebsites(
    loaded.rows,
    sitemapIndex
  );
  const summary = summarizeReconciliation(rows);

  const jsonPath = join(
    REPORTS_DIR,
    `independent-census-${args.reportSlug}.json`
  );
  const csvPath = join(
    REPORTS_DIR,
    `independent-census-${args.reportSlug}.csv`
  );

  const report = {
    generatedAt: new Date().toISOString(),
    phase: "4T-choice-property-id-reconciliation",
    batchId: args.batchId,
    propertyUrlReportPath: args.propertyUrlReportPath,
    candidateRetentionReportPath: args.candidateRetentionReportPath,
    retentionFilterApplied: !args.skipRetentionFilter && !!args.candidateRetentionReportPath,
    retentionMeta: loaded.retentionMeta,
    osmCandidatesScanned: loaded.totalScanned,
    osmCandidatesWithChoiceFamilyUrls: withChoiceUrls,
    sitemapPropertyCount: (extractReport.propertyRows || []).length,
    summary,
    dryRun: true,
    airtableWrites: false,
    hotelCensusWrites: false,
    brandSetupWrites: false,
    brandAliasWrites: false,
    candidateTableWrites: false,
    evidenceTableWrites: false,
    verifiedTableWrites: false,
    strFieldsUsed: false,
    googleApiUsed: false,
    propertyHtmlFetched: false,
    reconciliationRows: rows,
    reportFiles: { json: jsonPath, csv: csvPath },
  };

  writeJson(jsonPath, report);
  writeCsv(csvPath, rows.map(reconciliationRowToCsv), RECONCILE_CSV_COLUMNS);

  console.log("\n--- Reconciliation summary ---");
  console.log(`  Rows with Choice-family URLs:  ${withChoiceUrls}`);
  console.log(`  Reconciliation rows emitted:   ${summary.reconciliationRows}`);
  console.log(`  direct_property_id_match:      ${summary.direct_property_id_match}`);
  console.log(`  direct_property_url_match:     ${summary.direct_property_url_match}`);
  console.log(`  website_host_only:             ${summary.website_host_only}`);
  console.log(`  no_sitemap_match:              ${summary.no_sitemap_match}`);
  console.log(`  ready_for_choice_evidence:     ${summary.ready_for_choice_evidence}`);
  console.log(`  needs_manual_review:           ${summary.needs_manual_review}`);
  console.log(`  hold_for_source_policy:        ${summary.hold_for_source_policy}`);
  console.log(`  reject_no_direct_property_match: ${summary.reject_no_direct_property_match}`);

  const mazatlan = rows.filter(
    (r) =>
      normalizeKey(r.osmCandidateRecordId) === "recqbBwg4XpRea80l" ||
      /mazatl/i.test(r.osmCity || r.osmCandidateName || "")
  );
  if (mazatlan.length) {
    console.log("\n--- Mazatlán / recqb sample ---");
    for (const r of mazatlan.slice(0, 3)) {
      console.log(
        `  ${r.osmCandidateName}: ${r.matchType} ${r.extractedChoicePropertyId} → ${r.recommendedAction}`
      );
    }
  }

  console.log("\nReport files:");
  console.log(`  ${jsonPath}`);
  console.log(`  ${csvPath}`);
  console.log(
    "\n✓ No Airtable writes. Hotel Census, Brand Setup, Brand Alias, Candidates, Evidence, and Verified untouched."
  );
}

function normalizeKey(s) {
  return String(s ?? "")
    .trim()
    .toLowerCase();
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
