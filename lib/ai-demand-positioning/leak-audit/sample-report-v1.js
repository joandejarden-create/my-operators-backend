/**
 * Sales-ready sample report loaders for AI Demand Leak Audit demo pack.
 * Static fixtures only — no production ADP reads/writes.
 */

import { readFileSync } from "node:fs";
import { join } from "node:path";
import { assertClientReportSafety } from "./client-report-payload-v1.js";

export const SAMPLE_REPORT_FIXTURE_PATH = join(
  process.cwd(),
  "fixtures/ai-demand-positioning/leak-audit-sample-report-v1.json"
);

export const PORTFOLIO_SAMPLE_FIXTURE_PATH = join(
  process.cwd(),
  "fixtures/ai-demand-positioning/leak-audit-portfolio-sample-v1.json"
);

function stripUnsafe(payload) {
  const safety = assertClientReportSafety(payload);
  if (!safety.ok) {
    const err = new Error(`sample_report_safety_failed:${safety.failures.join(",")}`);
    err.failures = safety.failures;
    throw err;
  }
  return payload;
}

export function loadLeakAuditSampleReport() {
  const raw = JSON.parse(readFileSync(SAMPLE_REPORT_FIXTURE_PATH, "utf8"));
  return stripUnsafe({
    productName: raw.productName,
    productFamily: raw.productFamily || "Dealality AI Demand Positioning",
    researchMode: raw.researchMode || "leak_audit_lite",
    promptSetVersion: raw.promptSetVersion || "leak_audit_lite_v1",
    diagnosticScopeLabel: raw.diagnosticScopeLabel || null,
    scopeDisclaimer: raw.scopeDisclaimer || null,
    scenarioSummaryLabel: raw.scenarioSummaryLabel || null,
    title: raw.title,
    sample: true,
    layoutMode: raw.layoutMode || "three_page_v1",
    sampleBanner: raw.sampleBanner,
    sampleSubtext: raw.sampleSubtext || "",
    confidentialLabel: raw.confidentialLabel,
    limitedDiagnosticDisclaimer: raw.limitedDiagnosticDisclaimer,
    coverDisclaimer: raw.coverDisclaimer || raw.limitedDiagnosticDisclaimer || "",
    hotelName: raw.hotelName,
    hotelLocation: raw.hotelLocation || raw.locationLabel || null,
    locationLabel: raw.locationLabel || raw.hotelLocation || null,
    sampleFindingCount:
      raw.sampleFindingCount != null
        ? raw.sampleFindingCount
        : raw.coverMeta && raw.coverMeta.sampleFindings != null
          ? raw.coverMeta.sampleFindings
          : null,
    coverMeta: raw.coverMeta || null,
    runDate: raw.runDate,
    providersUsed: raw.providersUsed,
    reportType: raw.reportType || "Limited diagnostic",
    reportStatus: raw.reportStatus,
    reportMode: raw.reportMode || "single_property",
    prioritySource: raw.prioritySource || "inferred_from_results",
    inferredDemandPriority: raw.inferredDemandPriority || null,
    auditContext: raw.auditContext || null,
    howToRead: raw.howToRead || null,
    evidenceLibrary: Array.isArray(raw.evidenceLibrary) ? raw.evidenceLibrary : [],
    pdfEvidenceRefs: Array.isArray(raw.pdfEvidenceRefs) ? raw.pdfEvidenceRefs : [],
    executiveSignals: Array.isArray(raw.executiveSignals) ? raw.executiveSignals : [],
    supportingMetrics: raw.supportingMetrics || {},
    executiveSummary: raw.executiveSummary || null,
    bottomLineSummary: raw.bottomLineSummary,
    bottomLinePriority: raw.bottomLinePriority || "",
    demandAreaToReview: raw.demandAreaToReview || raw.inferredDemandLeak || null,
    inferredDemandLeak: raw.demandAreaToReview || raw.inferredDemandLeak || null,
    biggestDemandLeak: raw.biggestDemandLeak,
    demandLeakSummary: raw.demandLeakSummary || null,
    mainCompetitorShowingUpInstead: raw.mainCompetitorShowingUpInstead,
    competitorPriorityNote: raw.competitorPriorityNote || "",
    competitorDisplacementRank: raw.competitorDisplacementRank || null,
    competitorDisplacement: raw.competitorDisplacement,
    supportingEvidence: raw.supportingEvidence || null,
    whyThisMayMatterCommercially: raw.whyThisMayMatterCommercially || null,
    likelyReason: raw.likelyReason,
    firstFix: raw.firstFix,
    secondFix: raw.secondFix,
    thirdFix: raw.thirdFix,
    fixes: raw.fixes,
    whoDoesTheWork: raw.whoDoesTheWork,
    scenariosMonitored: raw.scenariosMonitored || null,
    recommendedNextStep: raw.recommendedNextStep,
    reportId: raw.reportId,
  });
}

export function loadLeakAuditPortfolioSampleReport() {
  const raw = JSON.parse(readFileSync(PORTFOLIO_SAMPLE_FIXTURE_PATH, "utf8"));
  return stripUnsafe({
    productName: raw.productName,
    productFamily: raw.productFamily || "Dealality AI Demand Intelligence",
    title: raw.title,
    sample: true,
    layoutMode: raw.layoutMode || "compressed_sales_diagnostic",
    sampleBanner: raw.sampleBanner,
    sampleSubtext: raw.sampleSubtext || "",
    confidentialLabel: raw.confidentialLabel,
    limitedDiagnosticDisclaimer: raw.limitedDiagnosticDisclaimer,
    reportMode: "portfolio_rollup",
    prioritySource: raw.prioritySource || "inferred_from_results",
    portfolioName: raw.portfolioName,
    portfolioGroupId: raw.portfolioGroupId,
    runDate: raw.runDate,
    providersUsed: raw.providersUsed,
    reportStatus: raw.reportStatus,
    portfolioAuditContext: raw.portfolioAuditContext,
    portfolioSummary: raw.portfolioSummary,
    executiveSignals: raw.executiveSignals || [],
    hotelsNeedingAttention: raw.hotelsNeedingAttention || [],
    inferredDemandSegmentsLeaking: raw.inferredDemandSegmentsLeaking || [],
    competitorsBenefiting: raw.competitorsBenefiting || [],
    portfolioPattern: raw.portfolioPattern,
    portfolioActions: raw.portfolioActions || [],
    howToUseThis: raw.howToUseThis,
    recommendedNextStep: raw.recommendedNextStep,
    whoDoesTheWork: raw.whoDoesTheWork || {
      title: "Who Does the Work?",
      dealalityPrepares: ["Source audits", "Gap analysis", "Draft copy", "Competitor comparison", "Action checklists"],
      hotelApproves: ["Facts", "Claims", "Final language", "Publishing decisions"],
      hotelOrAgencyPublishes: ["Website", "OTAs", "Google Business Profile", "TripAdvisor"],
      dealalityMonitors: ["Whether signals change", "Whether competitors continue to appear instead"],
    },
    firstFix: raw.portfolioActions?.[0]?.title || "Portfolio fact packs",
    secondFix: raw.portfolioActions?.[1]?.title || "Reconcile positioning",
    thirdFix: raw.portfolioActions?.[2]?.title || "Monitor displacement",
    bottomLineSummary: raw.portfolioSummary,
    biggestDemandLeak: raw.inferredDemandSegmentsLeaking?.[0]?.segment || "Inferred demand leak",
    mainCompetitorShowingUpInstead: raw.competitorsBenefiting?.[0]?.competitorName || "",
    competitorDisplacement: (raw.competitorsBenefiting || []).map((c) => ({
      competitorName: c.competitorName,
      demandSegment: (c.demandSegments || []).join(", "),
      displacementCount: c.hotelsAffected,
      evidenceExcerpt: c.evidenceExcerpt,
      interpretation: "Sample portfolio displacement signal.",
    })),
    reportId: raw.reportId,
  });
}
