#!/usr/bin/env node
/**
 * Archive Bethesda ADP Sep historical + Oct 2 official baseline (immutable).
 * Does not modify GDI archives.
 *
 * Usage:
 *   node scripts/archive-bethesda-adp-oct2-baseline-v1.mjs --dry-run
 *   node scripts/archive-bethesda-adp-oct2-baseline-v1.mjs --apply
 */
import { createHash } from "crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync, copyFileSync } from "fs";
import { join } from "path";
import { execSync } from "child_process";
import {
  publishReportArchiveEntry,
  listReportArchiveEntries,
} from "../lib/dealality-report-archive/report-archive-store-v1.js";

const APPLY = process.argv.includes("--apply");
const ROOT = process.cwd();
const HOTEL_ID = "recLuxvwwxID7U2B8";
const PROPERTY_ID = "adp_bethesda_marriott";
const OUT = join(ROOT, "reports/bethesda-pilot/adp-baseline/2026-10-02");
const SEP_PERIOD = "adp_period_adp_bethesda_marriott_20260909091016_9f3a60";
const OCT_PERIOD = "adp_period_adp_bethesda_marriott_20261002142154_62428a";

function sha256File(p) {
  return createHash("sha256").update(readFileSync(p)).digest("hex");
}
function gitSha() {
  try {
    return execSync("git rev-parse HEAD", { encoding: "utf8" }).trim();
  } catch {
    return null;
  }
}

mkdirSync(OUT, { recursive: true });

const metrics = JSON.parse(
  readFileSync(join(OUT, "OCT2_ADP_METRICS.json"), "utf8")
);
const v2 = JSON.parse(
  readFileSync(
    join(ROOT, "data/pilots/bethesda-marriott-001/adp-october-baseline-v2.json"),
    "utf8"
  )
);
const pdfPath = join(
  ROOT,
  "reports/ai-demand-positioning/current-report-pdf/adp_bethesda_marriott/report.pdf"
);
const pdfMeta = JSON.parse(
  readFileSync(
    join(
      ROOT,
      "reports/ai-demand-positioning/current-report-pdf/adp_bethesda_marriott/meta.json"
    ),
    "utf8"
  )
);

const entries = [
  {
    hotelId: HOTEL_ID,
    reportType: "ADP",
    reportClass: "PRE_PILOT_REFERENCE",
    versionLabel: "adp_sep_historical_2026-09-09_v1",
    reportDate: "2026-09-09",
    title: "Bethesda Marriott ADP — September historical / pre-pilot reference",
    periodId: SEP_PERIOD,
    baselineId: "BETHESDA_ADP_BASELINE_V1",
    status: "PRE_PILOT_REFERENCE",
    immutable: true,
    snapshot: {
      periodId: SEP_PERIOD,
      role: "PRE_PILOT_REFERENCE",
      metricsHistorical: {
        AI_CONSIDERATION: 42.1,
        SCENARIO_PRESENCE: 81,
        TOP3: 76.7,
        NUMBER_ONE: 33.3,
        REALITY_COVERAGE: 26.7,
      },
      note: "Observation corpus immutable. Designated Oct 1 control baseline was this period; superseded as official by Oct 2 V2.",
    },
  },
  {
    hotelId: HOTEL_ID,
    reportType: "ADP",
    reportClass: "OFFICIAL_BASELINE",
    versionLabel: "adp_oct2_official_baseline_2026-10-02_v2",
    reportDate: "2026-10-02",
    title: "Bethesda Marriott ADP — Official Pilot Baseline Oct 2, 2026",
    periodId: OCT_PERIOD,
    baselineId: "BETHESDA_ADP_BASELINE_V2",
    status: "OFFICIAL_BASELINE",
    immutable: true,
    pdfPath: existsSync(pdfPath) ? pdfPath : null,
    snapshot: {
      ...v2,
      metrics,
      pdfMeta,
      pdfSha256: existsSync(pdfPath) ? sha256File(pdfPath) : null,
      codeSha: gitSha(),
      querySetVersion: "BETHESDA_ADP_OCTOBER_CONTROL_QUERY_SET_V1",
      methodology: "ADP_MEASUREMENT_CONTRACT_V1",
      providersModels: {
        openai: "gpt-4o",
        gemini: "gemini-3.6-flash",
        perplexity: "sonar",
        claude: "claude-sonnet-4-6",
      },
    },
  },
];

const preview = {
  mode: APPLY ? "APPLY" : "DRY_RUN",
  entries: entries.map((e) => ({
    reportType: e.reportType,
    reportClass: e.reportClass,
    versionLabel: e.versionLabel,
    periodId: e.periodId,
    hasPdf: !!e.pdfPath,
  })),
};
writeFileSync(join(OUT, "ARCHIVE_PREVIEW.json"), JSON.stringify(preview, null, 2));
console.log(JSON.stringify(preview, null, 2));

if (!APPLY) {
  console.log("[DRY RUN] no archive writes");
  process.exit(0);
}

const results = [];
for (const e of entries) {
  const published = publishReportArchiveEntry(e);
  results.push(published);
}
writeFileSync(join(OUT, "ARCHIVE_RESULT.json"), JSON.stringify(results, null, 2));
const listed = listReportArchiveEntries({ hotelId: HOTEL_ID });
writeFileSync(
  join(OUT, "ARCHIVE_LIST_BETHESDA.json"),
  JSON.stringify(listed, null, 2)
);
console.log("ARCHIVED", results.length, "listed", listed?.length ?? listed);
