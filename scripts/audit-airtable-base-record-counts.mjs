/**
 * Read-only audit: record counts per table across an Airtable base.
 *
 * Deal Capture Platform (UI): appCCUsuGsE1ifoLk — use:
 *   npm run airtable:base:record-counts -- --base-id appCCUsuGsE1ifoLk --label deal-capture-platform-ui
 */
import "../load-env.js";
import { join } from "path";
import {
  auditBaseRecordCounts,
  resolveBaseConfigs,
  listAccessibleBases,
  slugifyReportLabel,
} from "../lib/airtable-base-record-count-audit.mjs";
import { writeCsv, writeJson } from "../lib/str-census-import/report-utils.mjs";

const REPORTS_DIR = join(process.cwd(), "reports");

const UI_EXPECTED = {
  "deal-capture-platform-ui": 67386,
  "deal-capture-mvp": 8956,
};

function parseArgs() {
  let reportDate = "2026-05-20";
  let base = "";
  let baseId = "";
  let label = "";
  let uiExpected = null;
  let listBasesOnly = false;

  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--report-date" && argv[i + 1])
      reportDate = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--report-date="))
      reportDate = a.slice("--report-date=".length).replace(/^"|"$/g, "");
    else if (a === "--base" && argv[i + 1]) base = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--base="))
      base = a.slice("--base=".length).replace(/^"|"$/g, "");
    else if (a === "--base-id" && argv[i + 1])
      baseId = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--base-id="))
      baseId = a.slice("--base-id=".length).replace(/^"|"$/g, "");
    else if (a === "--label" && argv[i + 1])
      label = argv[++i].replace(/^"|"$/g, "");
    else if (a.startsWith("--label="))
      label = a.slice("--label=".length).replace(/^"|"$/g, "");
    else if (a === "--ui-expected" && argv[i + 1])
      uiExpected = parseInt(argv[++i], 10);
    else if (a.startsWith("--ui-expected="))
      uiExpected = parseInt(a.slice("--ui-expected=".length), 10);
    else if (a === "--list-bases") listBasesOnly = true;
  }

  if (uiExpected == null && label && UI_EXPECTED[label] != null) {
    uiExpected = UI_EXPECTED[label];
  }

  return { reportDate, base, baseId, label, uiExpected, listBasesOnly };
}

function findTableCount(audits, tableName) {
  for (const audit of audits) {
    const row = audit.tables.find(
      (t) => t.tableName.toLowerCase() === tableName.toLowerCase()
    );
    if (row) return { ...row, baseLabel: audit.baseLabel, baseId: audit.baseId };
  }
  return null;
}

function reportSlug(args) {
  if (args.label) return slugifyReportLabel(args.label);
  if (args.baseId) return args.baseId.replace(/^app/, "app");
  return args.reportDate;
}

async function main() {
  const args = parseArgs();
  const apiKey = process.env.AIRTABLE_API_KEY;

  if (args.listBasesOnly) {
    const bases = await listAccessibleBases(apiKey);
    console.log(JSON.stringify(bases, null, 2));
    return;
  }

  const baseConfigs = resolveBaseConfigs(args.base, args.baseId, args.label);

  if (args.label === "deal-capture-platform-ui" && !args.baseId) {
    const bases = await listAccessibleBases(apiKey);
    const platform = bases.find((b) => /deal capture platform/i.test(b.name));
    if (platform) {
      baseConfigs[0].baseId = platform.id;
      baseConfigs[0].label = `${args.label} (${platform.name})`;
    }
  }

  console.log("Airtable base record count audit (read-only)\n");

  const baseAudits = [];
  for (const cfg of baseConfigs) {
    console.log(`Auditing ${cfg.label} (${cfg.baseId})…`);
    const audit = await auditBaseRecordCounts({
      apiKey,
      baseId: cfg.baseId,
      baseLabel: cfg.label,
      onTableComplete: (name, count) => {
        process.stdout.write(`  ${name}: ${count}\n`);
      },
    });
    baseAudits.push(audit);
    console.log(
      `  → ${audit.tableCount} tables, ${audit.totalRecords} records (exact: ${audit.totalRecordsExact})\n`
    );
  }

  const primary = baseAudits[0];
  const allTables = baseAudits.flatMap((a) => a.tables);
  allTables.sort((a, b) => (b.recordCount || 0) - (a.recordCount || 0));

  const over1000 = allTables.filter((t) => (t.recordCount || 0) > 1000);
  const over10000 = allTables.filter((t) => (t.recordCount || 0) > 10000);

  const uiExpected = args.uiExpected;
  const apiTotal = primary?.totalRecords ?? baseAudits.reduce((s, a) => s + a.totalRecords, 0);
  const uiMatch =
    uiExpected != null ? apiTotal === uiExpected : null;
  const uiDelta = uiExpected != null ? apiTotal - uiExpected : null;

  const slug = reportSlug(args);
  const jsonPath = join(REPORTS_DIR, `airtable-base-record-counts-${slug}-${args.reportDate}.json`);
  const csvPath = join(REPORTS_DIR, `airtable-base-record-counts-${slug}-${args.reportDate}.csv`);

  const report = {
    generatedAt: new Date().toISOString(),
    phase: "airtable-base-record-count-audit",
    reportDate: args.reportDate,
    reportLabel: args.label || null,
    auditedBaseId: primary?.baseId,
    auditedBaseLabel: primary?.baseLabel,
    airtableUiExpectedRecords: uiExpected,
    apiTotalRecords: apiTotal,
    matchesAirtableUiCount: uiMatch,
    uiCountDelta: uiDelta,
    uiCountNote:
      uiExpected != null && !uiMatch
        ? "API live-record sum may differ from Airtable Usage quota (deleted records in trash, revision history, sync lag, or non-data record types)."
        : null,
    basesAudited: baseAudits.map((a) => ({
      baseId: a.baseId,
      baseLabel: a.baseLabel,
      tableCount: a.tableCount,
      totalRecords: a.totalRecords,
      totalRecordsExact: a.totalRecordsExact,
      metadataAvailable: a.metadataAvailable,
      tablesOver1000: a.tablesOver1000,
      tablesOver10000: a.tablesOver10000,
    })),
    top20Tables: allTables.slice(0, 20).map((t) => ({
      tableName: t.tableName,
      tableId: t.tableId,
      recordCount: t.recordCount,
      countExact: t.countExact,
      safeCleanupCandidate: t.safeCleanupCandidate,
      cleanupHint: t.cleanupHint,
    })),
    tablesOver1000: over1000.map((t) => ({
      tableName: t.tableName,
      recordCount: t.recordCount,
    })),
    tablesOver10000: over10000.map((t) => ({
      tableName: t.tableName,
      recordCount: t.recordCount,
    })),
    independentHotelSourceCandidates: findTableCount(
      baseAudits,
      "Independent Hotel Source Candidates"
    ),
    hotelCensus: findTableCount(baseAudits, "Hotel Census"),
    suspiciousStagingTables: allTables.filter(
      (t) => t.safeCleanupCandidate || /import|staging|raw|archive|temp|duplicate/i.test(t.tableName)
    ),
    safeCleanupCandidates: baseAudits.flatMap((a) => a.safeCleanupCandidates),
    airtableWrites: false,
    hotelCensusWrites: false,
    brandSetupWrites: false,
    brandAliasWrites: false,
    strFieldsUsed: false,
    sensitiveFieldsLoaded: false,
  };

  const csvRows = allTables.map((t) => ({
    baseLabel: t.baseLabel,
    baseId: t.baseId,
    tableName: t.tableName,
    tableId: t.tableId,
    recordCount: t.recordCount,
    countExact: t.countExact ? "yes" : "no",
    countMethod: t.countMethod,
    countFieldUsed: t.countFieldUsed,
    protectedTable: t.protectedTable ? "yes" : "no",
    safeCleanupCandidate: t.safeCleanupCandidate ? "yes" : "no",
    cleanupHint: t.cleanupHint || "",
    error: t.error || "",
  }));

  await writeJson(jsonPath, report);
  await writeCsv(csvPath, csvRows, [
    "baseLabel",
    "baseId",
    "tableName",
    "tableId",
    "recordCount",
    "countExact",
    "countMethod",
    "countFieldUsed",
    "protectedTable",
    "safeCleanupCandidate",
    "cleanupHint",
    "error",
  ]);

  console.log("--- Summary ---");
  console.log(`Base ID:                 ${primary?.baseId}`);
  console.log(`API total (live rows):   ${apiTotal}`);
  if (uiExpected != null) {
    console.log(`Airtable UI expected:    ${uiExpected}`);
    console.log(`Matches UI:              ${uiMatch ? "yes" : "no"} (delta ${uiDelta})`);
  }
  console.log("\nTop 20 tables:");
  for (const t of report.top20Tables) {
    console.log(`  ${t.recordCount}\t${t.tableName}`);
  }
  const cand = report.independentHotelSourceCandidates;
  if (cand) console.log(`\nIndependent Hotel Source Candidates: ${cand.recordCount}`);
  const census = report.hotelCensus;
  if (census) console.log(`Hotel Census: ${census.recordCount}`);
  console.log(`\nTables > 1,000: ${over1000.length}`);
  console.log(`Tables > 10,000: ${over10000.length}`);
  console.log(`\nReports:\n  ${jsonPath}\n  ${csvPath}`);
  console.log("\nNo Airtable writes were performed.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
