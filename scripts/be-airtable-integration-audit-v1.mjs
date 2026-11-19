#!/usr/bin/env node
/**
 * Read-only Brand Explorer Airtable integration audit (Phase 1).
 * Default: dry-run / local contract summary. Optional --live uses GET-only Airtable reads.
 *
 * Usage:
 *   npm run be-airtable-integration-audit-v1 -- --dry-run
 *   npm run be-airtable-integration-audit-v1 -- --live --brand "Courtyard by Marriott"
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  BE_AIRTABLE_INTEGRATION_SOT_V1,
  BE_FOOTPRINT_METRIC_CONTRACT,
  BE_IDENTITY_CONTRACT,
  buildCoverageLimitations,
  summarizeContract,
} from "../lib/brand-explorer/be-airtable-integration-sot-v1.js";

async function ensureEnvLoaded() {
  // Prefer repo loader when dependencies are installed; otherwise skip (dry-run still works).
  try {
    await import("../load-env.js");
  } catch {
    // no-op
  }
}

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REPORT_DIR = path.join(ROOT, "reports/brand-explorer");
const REPORT_JSON = path.join(REPORT_DIR, "be-airtable-integration-audit-v1.json");
const REPORT_MD = path.join(REPORT_DIR, "be-airtable-integration-audit-v1.md");

function parseArgs(argv) {
  const live = argv.includes("--live");
  const dryRun = argv.includes("--dry-run") || !live;
  const brandIdx = argv.indexOf("--brand");
  const brand = brandIdx >= 0 ? String(argv[brandIdx + 1] || "").trim() : "Courtyard by Marriott";
  if (argv.includes("--apply") || argv.includes("--write")) {
    console.error("be-airtable-integration-audit-v1 refuses writes. Read-only only.");
    process.exit(2);
  }
  return { live, dryRun, brand };
}

async function maybeLiveCensus(brand) {
  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID_ALT) {
    return {
      attempted: true,
      ok: false,
      error: "Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT",
    };
  }
  const { buildBrandCensusSummary } = await import("../lib/hotel-census/build-brand-census-summary.js");
  const summary = await buildBrandCensusSummary(brand, null);
  return {
    attempted: true,
    ok: summary.available === true,
    transitionalAuthority: BE_FOOTPRINT_METRIC_CONTRACT.transitionalAuthority,
    designatedAuthority: BE_FOOTPRINT_METRIC_CONTRACT.designatedAuthority,
    brand,
    available: summary.available,
    fallbackRecommended: summary.fallbackRecommended,
    metrics: summary.metrics || null,
    warnings: summary.warnings || [],
    match: summary.match || null,
    source: summary.source || null,
    coverageLimitations: buildCoverageLimitations({
      usingTransitional: true,
      unmatchedAliases: (summary.warnings || [])
        .filter((w) => /NO_ALIAS|ALIAS_TABLE/.test(w))
        .slice(0, 5),
      recordsMatched: summary.census?.recordsMatched || 0,
    }),
  };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  if (opts.live) await ensureEnvLoaded();
  const contract = summarizeContract();

  const report = {
    contract: BE_AIRTABLE_INTEGRATION_SOT_V1,
    generatedAt: new Date().toISOString(),
    dryRun: opts.dryRun,
    airtableWrites: false,
    identity: BE_IDENTITY_CONTRACT,
    footprint: BE_FOOTPRINT_METRIC_CONTRACT,
    contractSummary: contract,
    liveCensusProbe: null,
    gaps: [
      "HPC brand rollup not yet wired as primary BE censusSummary path",
      "TAB_CONTRACTS on main still 7 tabs vs atelier 10 (factory restore Phase 2)",
      "ADP/GDI brand-level read contracts reserved only",
    ],
    stopConditions: [
      "No bulk brand population",
      "No production Airtable writes",
      "No ADP/GDI workstream modifications",
    ],
  };

  if (opts.live) {
    report.liveCensusProbe = await maybeLiveCensus(opts.brand);
  }

  fs.mkdirSync(REPORT_DIR, { recursive: true });
  fs.writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2));
  const md = [
    `# Brand Explorer Airtable integration audit v1`,
    ``,
    `- Contract: \`${report.contract}\``,
    `- Generated: ${report.generatedAt}`,
    `- Dry-run: ${report.dryRun}`,
    `- Airtable writes: ${report.airtableWrites}`,
    `- Designated footprint authority: **${report.footprint.designatedAuthority}**`,
    `- Transitional footprint authority: **${report.footprint.transitionalAuthority}**`,
    ``,
    `## Gaps`,
    ...report.gaps.map((g) => `- ${g}`),
    ``,
    `## Live census probe`,
    report.liveCensusProbe
      ? "```json\n" + JSON.stringify(report.liveCensusProbe, null, 2) + "\n```"
      : "_Skipped (pass `--live` for GET-only Hotel Census probe)._",
    ``,
    `## STOP`,
    ...report.stopConditions.map((s) => `- ${s}`),
    ``,
  ].join("\n");
  fs.writeFileSync(REPORT_MD, md);

  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${REPORT_MD}`);
  console.log(
    `Summary: designated=${report.footprint.designatedAuthority} transitional=${report.footprint.transitionalAuthority} live=${Boolean(report.liveCensusProbe)} writes=0`,
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
