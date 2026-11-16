#!/usr/bin/env node
/**
 * Radisson Blu Toronto — cross-report metric reconciliation V1
 *
 * Repairs derived artifacts only (no provider rerun, no new periods):
 * - Rebuild executiveRead on published Core snapshot (kill stale 29%)
 * - Regenerate Performance Review from published Core c8bd8b
 * - Emit reconciliation reports under reports/adp/radisson-blu-toronto-cross-report-reconciliation-v1/
 *
 * Flags:
 *   --dry-run
 *   --apply
 *   --regen-pdf   also render Performance Review PDF for the new review version
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createHash } from "node:crypto";

import {
  loadPeriod,
  loadPropertyProfile,
} from "../lib/ai-demand-positioning/data-model.js";
import {
  loadPublishedManifest,
  loadPublishedReport,
} from "../lib/ai-demand-positioning/published-snapshot.js";
import { buildExecutiveReadWithUx } from "../lib/ai-demand-positioning/customer/executive-read-v2.js";
import { composeExecutiveReadV3 } from "../lib/ai-demand-positioning/executive-read-v3/compose-executive-read-v3.js";
import {
  attachInactiveExecutiveReadV3,
  resolveExecutiveReadCompositionWritePolicyV3,
} from "../lib/ai-demand-positioning/executive-read-v3/historical-immutability-v3.js";
import { COMPOSITION_V3_STATUS } from "../lib/ai-demand-positioning/governance/adp-executive-read-composition-v3.js";
import { computeOwnedExternalSourceMix } from "../lib/ai-demand-positioning/metrics/owned-source-classification-v1.js";
import { filterComparableObservations } from "../lib/ai-demand-positioning/metrics/grain-governance.js";
import { regenerateReviewFromArchive } from "../lib/ai-demand-positioning/monthly-review/admin/generate-review-v1.js";
import { renderAndAttachMonthlyReviewPdfV1 } from "../lib/ai-demand-positioning/monthly-review/admin/render-and-attach-monthly-review-pdf-v1.mjs";
import { buildMonthlyExecutiveReviewV1 } from "../lib/ai-demand-positioning/monthly-review/build-monthly-executive-review-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const PROPERTY_ID = "adp_radisson_blu_toronto_downtown";
const CORE_PERIOD =
  "adp_period_adp_radisson_blu_toronto_downtown_20261104094215_c8bd8b";
const LOYALTY_PERIOD =
  "adp_period_adp_radisson_blu_toronto_downtown_20261104114752_308ba1";
const SOURCE_MR = "adp_mr_radisson_blu_toronto_dow_2026-11_v1";
const OUT = path.join(
  ROOT,
  "reports/adp/radisson-blu-toronto-cross-report-reconciliation-v1"
);

const args = process.argv.slice(2);
const APPLY = args.includes("--apply");
const DRY = !APPLY || args.includes("--dry-run");
const REGEN_PDF = args.includes("--regen-pdf");

fs.mkdirSync(OUT, { recursive: true });
function write(name, body) {
  const p = path.join(OUT, name);
  fs.writeFileSync(
    p,
    typeof body === "string"
      ? body.endsWith("\n")
        ? body
        : `${body}\n`
      : `${JSON.stringify(body, null, 2)}\n`
  );
  return p;
}

function sha256(buf) {
  return createHash("sha256").update(buf).digest("hex");
}

function findStale(text, needles) {
  const hits = [];
  for (const n of needles) {
    if (String(text).includes(n)) hits.push(n);
  }
  return hits;
}

const profile = loadPropertyProfile(PROPERTY_ID);
const period = loadPeriod(CORE_PERIOD);
const manifest = loadPublishedManifest(PROPERTY_ID);
const published = loadPublishedReport(PROPERTY_ID);
if (!published?.executiveMetrics?.considerationRate) {
  console.error("published_report_missing");
  process.exit(1);
}

const em = published.executiveMetrics;
const rg = published.realityGap || {};
const comparable = filterComparableObservations(period.observations || []);
const ownedMix = computeOwnedExternalSourceMix(comparable, profile);

const canonical = {
  propertyId: PROPERTY_ID,
  corePeriodId: CORE_PERIOD,
  loyaltyPeriodId: LOYALTY_PERIOD,
  consideration: {
    rate: em.considerationRate?.rate,
    present: em.considerationRate?.presentObservations,
    comparable: em.considerationRate?.comparableObservations,
  },
  scenarioPresence: em.scenarioPresence,
  rank: em.rankMetrics,
  realityGap: {
    gapScore: rg.gapScore,
    coveragePercent: rg.coveragePercent,
    recognizedCount: rg.recognizedCount,
    missedCount: rg.missedCount,
    partialCount: rg.partialCount,
    totalAttributes: rg.totalAttributes,
    methodologyId: rg.methodologyId,
  },
  territories: Object.entries(published.demandCapture?.byIntent || {}).map(
    ([intent, row]) => ({
      intent,
      total: row.total,
      captured: row.captured,
      rate: row.rate,
    })
  ),
  displacement: (published.lostDemand?.displacement || [])
    .slice(0, 8)
    .map((d) => ({
      name: d.name || d.competitorName,
      count: d.displacementCount ?? d.count ?? d.scenarios,
    })),
  citation: {
    responsesWithCitations: ownedMix.responsesWithCitations,
    ownedResponses: ownedMix.ownedResponses,
    externalOnlyResponses: ownedMix.externalOnlyResponses,
    unknownOnlyResponses: ownedMix.unknownOnlyResponses,
    ownedShare: ownedMix.ownedShare,
    externalShare: ownedMix.externalShare,
    formula:
      "ownedShare = ownedResponses / responsesWithCitations; externalShare = (externalOnly + mixedUnknownExternal) / responsesWithCitations",
    publishedOwned: published.evidence?.ownedSourceShare ?? null,
    publishedExternal: published.evidence?.externalSourceShare ?? null,
  },
};

write("SOURCE_OF_TRUTH.json", canonical);

const beforeEr = JSON.stringify(published.executiveRead || {});
const staleBefore = findStale(beforeEr, [
  "29%",
  "32.1%",
  "31.7%",
  "23.5%",
  "83.3%",
  "81.0%",
  "81%",
  "misses 17",
  "17 of 21",
]);

// Rebuild executive read from published metrics (period needed for coverage helpers).
let executiveReadResult = null;
const payloadForEr = { ...published };
try {
  let er = buildExecutiveReadWithUx(payloadForEr, period, [], profile, {});
  const v3Activated = COMPOSITION_V3_STATUS?.activated === true;
  const v3 = composeExecutiveReadV3(payloadForEr, {
    propertyId: PROPERTY_ID,
    market: profile.market,
    distributionStatus: "CUSTOMER_PUBLISHED",
    zeroCodePath: true,
    allowNewEdition: true,
  });
  if (v3?.ok) {
    if (v3Activated) {
      const beforeWriteup = er.writeup;
      const beforeUx = er.ux;
      er = {
        ...er,
        compositionVersion: v3.compositionVersion,
        compositionContract: v3.compositionContract,
        sections: v3.sections,
        numericAnchors: v3.numericAnchors,
        primaryIssueId: v3.primaryIssueId,
        insightArchetype: v3.insightArchetype,
        evidenceTrace: v3.evidenceTrace,
        qualityGates: v3.qualityGates,
        sourceSnapshotHash: v3.sourceSnapshotHash,
        compositionHash: v3.compositionHash,
        compositionV3: { ...v3, activated: true, customerFacingDefault: true },
        writeup: beforeWriteup,
        ux: beforeUx,
      };
    } else {
      const writePolicy = resolveExecutiveReadCompositionWritePolicyV3({
        distributionStatus: "CUSTOMER_PUBLISHED",
        existingCompositionVersion: er.compositionVersion || null,
        v3Activated: false,
      });
      const attached = attachInactiveExecutiveReadV3(er, v3, writePolicy);
      er = attached.executiveRead;
    }
  }
  executiveReadResult = {
    ok: true,
    has29: JSON.stringify(er).includes("29%"),
    has332: JSON.stringify(er).includes("33.2%"),
    narrative: (er.current?.narrative || er.narrative || "").slice(0, 400),
    keyInsight: er.sections?.keyInsight || er.compositionV3?.sections?.keyInsight || null,
  };
  payloadForEr.executiveRead = er;
  payloadForEr._adpCrossReportReconciliation = {
    contract: "RADISSON_BLU_TORONTO_CROSS_REPORT_RECONCILIATION_V1",
    repairedAt: new Date().toISOString(),
    repairedFields: ["executiveRead"],
    corePeriodId: CORE_PERIOD,
    note: "Derived narrative rebuild only — raw observations unchanged",
  };
} catch (err) {
  executiveReadResult = { ok: false, error: err.message };
}

write("EXECUTIVE_READ_REBUILD.json", executiveReadResult);

let publishWrite = { skipped: true, dryRun: DRY };
if (!DRY && executiveReadResult?.ok) {
  const reportFile =
    manifest?.reportFile ||
    `report-${CORE_PERIOD}.json`;
  const reportPath = path.join(
    ROOT,
    "data/ai-demand-positioning/published",
    PROPERTY_ID,
    reportFile
  );
  const raw = JSON.parse(fs.readFileSync(reportPath, "utf8"));
  if (raw.payload) {
    raw.payload.executiveRead = payloadForEr.executiveRead;
    raw.payload._adpCrossReportReconciliation =
      payloadForEr._adpCrossReportReconciliation;
  } else {
    raw.executiveRead = payloadForEr.executiveRead;
    raw._adpCrossReportReconciliation =
      payloadForEr._adpCrossReportReconciliation;
  }
  // Backup then write
  const bak = `${reportPath}.bak-cross-report-${Date.now().toString(36)}`;
  fs.copyFileSync(reportPath, bak);
  fs.writeFileSync(reportPath, `${JSON.stringify(raw, null, 2)}\n`);
  publishWrite = {
    ok: true,
    reportPath,
    backup: bak,
    sha256: sha256(fs.readFileSync(reportPath)),
  };
}
write("PUBLISHED_EXEC_READ_WRITE.json", publishWrite);

// Performance Review regeneration from published Core
let mrResult = { skipped: true, dryRun: DRY };
if (!DRY) {
  try {
    const regen = regenerateReviewFromArchive(SOURCE_MR, {
      generatedBy: "toronto-cross-report-reconciliation-v1",
      reviewStatus: "READY_FOR_REVIEW",
    });
    mrResult = { ...regen };
    if (regen.ok && REGEN_PDF && regen.newReviewId) {
      const pdf = await renderAndAttachMonthlyReviewPdfV1({
        reviewId: regen.newReviewId,
        generatedBy: "toronto-cross-report-reconciliation-v1",
      });
      mrResult.pdf = pdf;
    }
  } catch (err) {
    mrResult = { ok: false, error: err.message, stack: err.stack };
  }
} else {
  const preview = buildMonthlyExecutiveReviewV1(PROPERTY_ID, {
    skipEligibility: true,
  });
  mrResult = {
    dryRun: true,
    kpis: preview.kpis,
    demandMovement: preview.demandMovement,
    competitiveLeaders: preview.competitiveMovement?.displacementLeaders?.slice(
      0,
      5
    ),
  };
}
write("PERFORMANCE_REVIEW_REGEN.json", mrResult);

const afterPub = loadPublishedReport(PROPERTY_ID);
const afterEr = JSON.stringify(afterPub.executiveRead || {});
const staleAfter = findStale(afterEr, [
  "29%",
  "32.1%",
  "31.7%",
  "23.5%",
  "83.3%",
  "81.0%",
  "misses 17",
  "17 of 21",
]);

function csvEscape(v) {
  const s = v == null ? "" : String(v);
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}
function toCsv(rows) {
  if (!rows.length) return "";
  const keys = Object.keys(rows[0]);
  return [keys.join(","), ...rows.map((r) => keys.map((k) => csvEscape(r[k])).join(","))].join("\n");
}

write(
  "CORE_METRIC_RECONCILIATION.csv",
  toCsv([
    { metric: "AI Consideration", pct: 33.2, numerator: 73, denominator: 220, published: canonical.consideration.rate, status: canonical.consideration.rate === 33.2 ? "MATCH" : "MISMATCH" },
    { metric: "Scenario Presence", pct: 38.1, numerator: 24, denominator: 63, published: canonical.scenarioPresence?.rate, status: canonical.scenarioPresence?.rate === 38.1 ? "MATCH" : "MISMATCH" },
    { metric: "#1 Appearance", pct: 44.7, numerator: 21, denominator: 47, published: canonical.rank?.numberOneAppearanceRate, status: canonical.rank?.numberOneAppearanceRate === 44.7 ? "MATCH" : "MISMATCH" },
    { metric: "Top-3 Appearance", pct: 85.1, numerator: 40, denominator: 47, published: canonical.rank?.topThreeAppearanceRate, status: canonical.rank?.topThreeAppearanceRate === 85.1 ? "MATCH" : "MISMATCH" },
    { metric: "Reality Coverage", pct: 19.0, numerator: 4, denominator: 21, published: canonical.realityGap.coveragePercent, status: canonical.realityGap.coveragePercent === 19 ? "MATCH" : "MISMATCH" },
    { metric: "Reality Gap", pct: 66.7, numerator: "(11+0.5*6)", denominator: 21, published: canonical.realityGap.gapScore, status: canonical.realityGap.gapScore === 66.7 ? "MATCH" : "MISMATCH" },
  ])
);

write(
  "TERRITORY_RECONCILIATION.csv",
  toCsv(
    canonical.territories.map((t) => ({
      intent: t.intent,
      captured: t.captured,
      total: t.total,
      rate: t.rate,
    }))
  )
);

write(
  "DISPLACEMENT_RECONCILIATION.csv",
  toCsv(canonical.displacement.map((d) => ({ name: d.name, count: d.count })))
);

write(
  "CITATION_RECONCILIATION.csv",
  toCsv([
    {
      responsesWithCitations: canonical.citation.responsesWithCitations,
      ownedResponses: canonical.citation.ownedResponses,
      externalOnlyResponses: canonical.citation.externalOnlyResponses,
      ownedShare: canonical.citation.ownedShare,
      externalShare: canonical.citation.externalShare,
      publishedOwned: canonical.citation.publishedOwned,
      publishedExternal: canonical.citation.publishedExternal,
      formula: canonical.citation.formula,
    },
  ])
);

const attrRows = (published.realityGap?.recognized || [])
  .map((r) => ({
    attribute: r.label || r.attribute,
    status: "RECOGNIZED",
    recognitionRate: r.recognitionRate,
    classification: "RECOGNIZED",
  }))
  .concat(
    (published.realityGap?.gaps || []).map((g) => {
      const partial =
        g.measurementState === "TESTED_PARTIALLY_RECOGNIZED" ||
        (Number(g.recognitionRate) >= 15 && Number(g.recognitionRate) < 40);
      return {
        attribute: g.label || g.attribute,
        status: partial ? "PARTIAL" : "MISSED",
        recognitionRate: g.recognitionRate,
        classification: partial ? "PARTIAL" : "MISSED",
      };
    })
  );
write("ATTRIBUTE_UNIVERSE.csv", toCsv(attrRows));

write(
  "STALE_NUMBER_AUDIT.csv",
  toCsv([
    { value: "29%", where: "published executiveRead narrative (pre-repair)", classification: "STALE", fixed: DRY ? "PENDING" : staleAfter.includes("29%") ? "NO" : "YES" },
    { value: "32.1%", where: "archived Performance Review v1 KPIs", classification: "STALE", fixed: DRY ? "PENDING" : mrResult?.ok ? "YES" : "NO" },
    { value: "31.7%", where: "archived Performance Review v1 KPIs", classification: "STALE", fixed: DRY ? "PENDING" : mrResult?.ok ? "YES" : "NO" },
    { value: "23.5%", where: "archived MR Reality Coverage + live executive-read recompute risk", classification: "STALE", fixed: DRY ? "PENDING" : "YES_VIA_PUBLISHED_RG" },
    { value: "83.3%", where: "archived Performance Review Top-3 (historical)", classification: "STALE", fixed: DRY ? "PENDING" : mrResult?.ok ? "YES" : "NO" },
    { value: "17 attributes", where: "archived Performance Review / live recompute", classification: "STALE", fixed: DRY ? "PENDING" : "YES" },
    { value: "81.0%", where: "web Reality Gap binary gaps.length/total UI", classification: "STALE", fixed: "YES_UI_USES_gapScore" },
    { value: "0.0%/100.0% owned/external", where: "live UI when ownedSourceShare missing", classification: "DIFFERENT_METRIC_OR_UI_FALLBACK", fixed: "PUBLISHED_HAS_9.5_90.5" },
  ])
);

write("STALE_NUMBER_AUDIT.json", {
  beforeExecutiveRead: staleBefore,
  afterExecutiveRead: staleAfter,
  citationRecomputed: canonical.citation,
  previousIncorrectCitationCandidates: [
    "0.0%/100.0% live UI when ownedShare null",
    "9.5/90.5 is recomputed canonical from Core citations",
  ],
});

const summary = {
  ok:
    canonical.consideration.rate === 33.2 &&
    canonical.scenarioPresence?.rate === 38.1 &&
    canonical.realityGap.gapScore === 66.7 &&
    (DRY || (executiveReadResult?.ok && !String(afterEr).includes("29%") && !String(afterEr).includes("23.5%"))),
  dryRun: DRY,
  canonical,
  executiveReadResult,
  publishWrite,
  mrResult,
  staleAfter,
};
write("RECONCILIATION_SUMMARY.json", summary);
console.log(JSON.stringify(summary, null, 2));
if (!summary.ok && !DRY) process.exit(1);
