#!/usr/bin/env node
/**
 * ADP_38_ADMIN_PUBLISHED_VS_MEASURED_DATES_AUDIT_V1
 *
 * Read-only local audit: distinguish Actual publication timestamp vs
 * certified measurement period vs customer-access verification timestamp.
 *
 * Does NOT deploy, remesure, republish, or mutate tokens.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { buildPublishedAdpReviewCoverageV1 } from "../lib/ai-demand-positioning/monthly-review/admin/published-review-coverage-v1.js";
import {
  monthYearFromPeriodId,
  monthYearFromPublicationTimestamp,
  resolveAuthoritativePublishedColumn,
} from "../lib/ai-demand-positioning/monthly-review/admin/resolve-admin-publication-columns-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT_DIR = path.join(
  ROOT,
  "reports/dealality-adp-single-version-publication-contract-v1/admin-published-vs-measured-dates"
);
const PACK_DIR = path.join(
  ROOT,
  "reports/dealality-adp-single-version-publication-contract-v1/approved-packages"
);
const DURABLE_REG = path.join(
  ROOT,
  "reports/dealality-adp-single-version-publication-contract-v1/production-activation/durable-staging-prefer/registry/published-current-registry-v1.json"
);
const ACTIVATION = path.join(
  ROOT,
  "reports/dealality-adp-single-version-publication-contract-v1/production-activation/EXECUTION_RESULT.json"
);
const LEDGER = path.join(
  ROOT,
  "config/client-share/external-share-live-verify-v1.json"
);

process.env.ADP_DURABLE_PUBLISHED_MODE =
  process.env.ADP_DURABLE_PUBLISHED_MODE || "prefer";
process.env.ADP_DURABLE_PUBLISHED_ROOT =
  process.env.ADP_DURABLE_PUBLISHED_ROOT ||
  path.join(
    ROOT,
    "reports/dealality-adp-single-version-publication-contract-v1/production-activation/durable-staging-prefer"
  );

fs.mkdirSync(OUT_DIR, { recursive: true });

const approved = fs
  .readdirSync(PACK_DIR)
  .filter((f) => f.endsWith(".approved-package.json"))
  .map((f) => JSON.parse(fs.readFileSync(path.join(PACK_DIR, f), "utf8")));
const byApproved = Object.fromEntries(approved.map((p) => [p.propertyId, p]));

const durableReg = JSON.parse(fs.readFileSync(DURABLE_REG, "utf8"));
const durableProps = Array.isArray(durableReg.properties)
  ? durableReg.properties
  : Object.values(durableReg.properties || {});
const byDurable = Object.fromEntries(
  durableProps.map((e) => [e.propertyId, e])
);

const ledger = fs.existsSync(LEDGER)
  ? JSON.parse(fs.readFileSync(LEDGER, "utf8"))
  : { hotels: {} };
const activation = fs.existsSync(ACTIVATION)
  ? JSON.parse(fs.readFileSync(ACTIVATION, "utf8"))
  : null;

const coverage = buildPublishedAdpReviewCoverageV1({
  includeSyntheticFixtures: false,
});
const rows = Array.isArray(coverage?.rows) ? coverage.rows : [];

const matrix = [];
for (const pkg of approved.sort((a, b) =>
  String(a.propertyId).localeCompare(String(b.propertyId))
)) {
  const propertyId = pkg.propertyId;
  const durable = byDurable[propertyId] || null;
  const auth = resolveAuthoritativePublishedColumn(propertyId);
  const cov = rows.find((r) => r.propertyId === propertyId) || null;
  const live = ledger.hotels?.[propertyId] || null;

  const measurementPeriodId =
    pkg.measurementPeriodId ||
    durable?.periodId ||
    auth.measurementPeriodId ||
    null;
  const publishedAt = durable?.latestPublishedAt || auth.publishedAt || null;
  const measuredMonthYear = monthYearFromPeriodId(measurementPeriodId);
  const publishedMonthYear = monthYearFromPublicationTimestamp(publishedAt);
  const legacyWrongLabel = monthYearFromPeriodId(measurementPeriodId);

  const issues = [];
  if (!publishedAt) issues.push("MISSING_PUBLICATION_TIMESTAMP");
  if (!measuredMonthYear) issues.push("MISSING_MEASURED_MONTH");
  if (
    publishedMonthYear &&
    legacyWrongLabel &&
    publishedMonthYear !== legacyWrongLabel
  ) {
    issues.push("LEGACY_ADMIN_WOULD_SHOW_MEASUREMENT_AS_PUBLISHED");
  }
  if (cov && cov.publishedMonthYear !== publishedMonthYear) {
    issues.push(
      `COVERAGE_PUBLISHED_MISMATCH:got_${cov.publishedMonthYear}_expected_${publishedMonthYear}`
    );
  }
  if (cov && (cov.measuredMonthYear || null) !== measuredMonthYear) {
    issues.push(
      `COVERAGE_MEASURED_MISMATCH:got_${cov.measuredMonthYear}_expected_${measuredMonthYear}`
    );
  }
  if (
    cov?.publishedConsideration != null &&
    pkg.certifiedMeasurement?.considerationRate != null &&
    Math.abs(
      Number(cov.publishedConsideration) -
        Number(pkg.certifiedMeasurement.considerationRate)
    ) > 0.05
  ) {
    issues.push("CONSIDERATION_MISMATCH");
  }

  matrix.push({
    propertyId,
    hotelName: cov?.propertyName || null,
    measurementPeriodId,
    measuredMonthYear,
    publishedAt,
    publishedMonthYear,
    legacyAdminWouldHaveShown: legacyWrongLabel,
    considerationRate:
      auth.publishedConsideration ??
      durable?.considerationRate ??
      pkg.certifiedMeasurement?.considerationRate ??
      null,
    customerAccessLabel: cov?.customerAccess?.label || null,
    customerAccessVerifiedAt:
      cov?.customerAccessVerifiedAt || live?.verifiedAt || null,
    pdfLabel: cov?.overallPdfStatus?.label || null,
    authoritySource: auth.authoritySource,
    publicationTimestampMissing: !publishedAt,
    issues,
    ok: issues.length === 0,
  });
}

const measuredBuckets = {};
const publishedBuckets = {};
const correctedFromMeasurement = [];
for (const row of matrix) {
  measuredBuckets[row.measuredMonthYear || "?"] =
    (measuredBuckets[row.measuredMonthYear || "?"] || 0) + 1;
  publishedBuckets[row.publishedMonthYear || "?"] =
    (publishedBuckets[row.publishedMonthYear || "?"] || 0) + 1;
  if (
    row.publishedMonthYear &&
    row.legacyAdminWouldHaveShown &&
    row.publishedMonthYear !== row.legacyAdminWouldHaveShown
  ) {
    correctedFromMeasurement.push({
      propertyId: row.propertyId,
      wasShowing: row.legacyAdminWouldHaveShown,
      correctPublished: row.publishedMonthYear,
      measured: row.measuredMonthYear,
    });
  }
}

const summary = {
  contract: "ADP_38_ADMIN_PUBLISHED_VS_MEASURED_DATES_AUDIT_V1",
  generatedAt: new Date().toISOString(),
  durableMode: process.env.ADP_DURABLE_PUBLISHED_MODE,
  hotelCount: matrix.length,
  okCount: matrix.filter((r) => r.ok).length,
  failCount: matrix.filter((r) => !r.ok).length,
  measuredBuckets,
  publishedBuckets,
  hotelsWherePublishedDifferedFromMeasuredLabel:
    correctedFromMeasurement.length,
  correctedFromMeasurement,
  activationWindow: activation
    ? {
        startedAt: activation.startedAt || null,
        finishedAt: activation.finishedAt || null,
        note: "Production single-version activation publication window (corroborates durable latestPublishedAt; not used as Admin source)",
      }
    : null,
  rootCause:
    "Admin Published previously used monthYearFromPeriodId(measurementPeriodId), which displays measurement dates (Oct/Nov mix) instead of durable latestPublishedAt publication timestamps (all November 2026 for the 38-hotel activation).",
  fix:
    "Published ← monthYearFromPublicationTimestamp(latestPublishedAt); Measured ← monthYearFromPeriodId(periodId); fail closed when publishedAt missing.",
  stopBeforeProductionDeploy: true,
};

fs.writeFileSync(
  path.join(OUT_DIR, "AUDIT_SUMMARY.json"),
  `${JSON.stringify(summary, null, 2)}\n`
);
fs.writeFileSync(
  path.join(OUT_DIR, "AUDIT_MATRIX_38.json"),
  `${JSON.stringify({ ok: summary.failCount === 0, matrix }, null, 2)}\n`
);

const md = [
  "# ADP Admin — Published vs Measured dates audit",
  "",
  `**Generated:** ${summary.generatedAt}`,
  `**Hotels:** ${summary.hotelCount}`,
  `**Coverage OK:** ${summary.okCount}/${summary.hotelCount}`,
  "",
  "## Root cause",
  "",
  summary.rootCause,
  "",
  "## Fix",
  "",
  summary.fix,
  "",
  "## Buckets",
  "",
  `- Measured months: ${JSON.stringify(measuredBuckets)}`,
  `- Published months (authoritative): ${JSON.stringify(publishedBuckets)}`,
  `- Hotels where legacy Admin label ≠ true Published: ${correctedFromMeasurement.length}`,
  "",
  "## Activation corroboration",
  "",
  activation
    ? `- Window: ${activation.startedAt} → ${activation.finishedAt}`
    : "- No activation record found",
  "",
  "## Matrix (compact)",
  "",
  "| Hotel | Measured | Published | Cons. | CA | PDF |",
  "| --- | --- | --- | ---: | --- | --- |",
  ...matrix.map(
    (r) =>
      `| \`${r.propertyId}\` | ${r.measuredMonthYear || "—"} | ${r.publishedMonthYear || "MISSING"} | ${r.considerationRate ?? "—"} | ${r.customerAccessLabel || "—"} | ${r.pdfLabel || "—"} |`
  ),
  "",
  "**STOP before production deployment** — local validation only.",
  "",
].join("\n");
fs.writeFileSync(path.join(OUT_DIR, "AUDIT_REPORT.md"), md);

console.log(
  JSON.stringify(
    {
      ok: summary.failCount === 0,
      hotelCount: summary.hotelCount,
      okCount: summary.okCount,
      measuredBuckets,
      publishedBuckets,
      correctedCount: correctedFromMeasurement.length,
      outDir: OUT_DIR,
    },
    null,
    2
  )
);
process.exit(summary.failCount === 0 ? 0 : 1);
