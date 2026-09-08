/**
 * Single-property Leak Audit report generation from store records.
 * Writes only to Leak Audit collections. Never mutates production ADP.
 */

import { mkdirSync } from "node:fs";
import { join } from "node:path";
import { REQUEST_STATUS, REPORT_STATUS, RUN_STATUS } from "./schema-v1.js";
import { loadLeakAuditSampleReport } from "./sample-report-v1.js";
import { buildClientSafeReportPayload } from "./client-report-payload-v1.js";
import { runLeakAuditPhase1 } from "./run-audit-v1.js";

function seedChildRecords(store, { runId, hotelAuditId, sample }) {
  const metrics = [];
  (sample.executiveSignals || []).forEach((kpi, i) => {
    metrics.push(
      store.createMetric({
        runId,
        hotelAuditId,
        metricKey: kpi.id || `metric_${i + 1}`,
        metricLabel: kpi.label,
        metricValue: kpi.value,
        plainEnglishDefinition: kpi.help || "",
        infoTooltipText: kpi.help || "",
        displayOrder: i + 1,
      })
    );
  });

  const competitors = [];
  (sample.competitorDisplacementRank?.rows || []).forEach((row) => {
    competitors.push(
      store.createCompetitor({
        runId,
        hotelAuditId,
        competitorName: row.competitorName,
        appearedWhenSubjectAbsentCount: row.displacementCount,
        demandArea: row.demandSegment,
        whatThisMayMean: row.whatThisMayMean,
        rank: row.rank,
      })
    );
  });

  const evidence = [];
  (sample.evidenceLibrary || []).forEach((ev, i) => {
    evidence.push(
      store.createEvidence({
        id: ev.id || undefined,
        runId,
        hotelAuditId,
        evidenceType: ev.type || "neutral",
        label: ev.label || ev.title || "",
        headline: ev.headline || ev.title || "",
        description: ev.summary || ev.description || "",
        provider: ev.provider || "",
        demandArea: ev.demandArea || "",
        responseExcerpt: ev.excerpt || ev.responseExcerpt || "",
        sourceLinks: ev.sourceLinks || [],
        whyItMatters: ev.whyItMatters || "",
        managementReview: ev.managementReview || "",
        displayOrder: i + 1,
      })
    );
  });

  const actions = [];
  (sample.fixes || []).forEach((fix, i) => {
    actions.push(
      store.createAction({
        runId,
        hotelAuditId,
        actionNumber: i + 1,
        actionTitle: fix.title,
        whyItMatters: fix.whyThisMatters,
        dealalityCanHelpPrepare: fix.dealalityCanPrepare,
        hotelConfirms: fix.hotelMustConfirm,
        sourcesNextCheck: fix.targetSources,
        displayOrder: i + 1,
      })
    );
  });

  return { metrics, competitors, evidence, actions };
}

/**
 * Generate a full single-property report from an approved request.
 * @param {object} store
 * @param {object} options
 * @param {string} options.requestId
 * @param {'phase1'|'sample_seed'} [options.mode]
 * @param {string} [options.root] temp root already on store
 */
export async function generateSinglePropertyLeakAuditReport(store, options = {}) {
  const request = store.getRequest(options.requestId);
  if (!request) throw new Error("request_not_found");

  const hotel = store.createHotel({
    requestId: request.id,
    hotelName: request.hotelName,
    hotelWebsite: request.hotelWebsite,
    city: request.city,
    market: request.market,
    country: request.country,
    locationLabel: [request.city, request.country].filter(Boolean).join(", "),
    matchConfidence: "none",
    matchBasis: "none",
  });

  if (request.status === REQUEST_STATUS.REQUESTED) {
    store.updateRequest(request.id, { status: REQUEST_STATUS.APPROVED });
  }

  const mode = options.mode || "sample_seed";
  if (mode === "phase1") {
    const result = await runLeakAuditPhase1(store, {
      requestId: request.id,
      liveProviderCalls: false,
    });
    const report = store.updateReport(result.report.id, {
      requestId: request.id,
      hotelAuditId: hotel.id,
      reportType: "single_property",
      layoutMode: "three_page_v1",
    });
    return {
      ok: true,
      mode,
      request: store.getRequest(request.id),
      hotel,
      run: result.run,
      report,
      shareUrl: report.shareUrl,
      productionAdpTouched: false,
    };
  }

  const sample = loadLeakAuditSampleReport();
  const run = store.createRun({
    auditRequestId: request.id,
    requestId: request.id,
    hotelAuditId: hotel.id,
    matchedHotelId: null,
    researchMode: request.researchMode || "leak_audit_lite",
    runDate: sample.runDate || new Date().toISOString().slice(0, 10),
    providersUsed: sample.providersUsed || ["openai", "gemini", "perplexity", "claude"],
    demandTerritoriesTested: ["Leisure", "Couples", "Meetings & Groups", "Business", "Wellness"],
    maxScenarios: 15,
    maxProviders: 4,
    maxObservations: 60,
    scenarioCount: 15,
    providerCount: 4,
    actualScenariosRun: 15,
    actualProvidersRun: 4,
    actualObservations: 60,
    scenariosMonitoredLabel: "15 × 4",
    observationsLabel: "60 observations",
    totalPromptsRun: 60,
    totalObservations: 60,
    subjectMentionCount: 40,
    competitorMentionCount: 12,
    displacementCount: 5,
    confidenceFlag: "high",
    completenessFlag: "adequate",
    status: RUN_STATUS.COMPLETED,
  });

  const children = seedChildRecords(store, {
    runId: run.id,
    hotelAuditId: hotel.id,
    sample,
  });

  const report = store.createReport({
    auditRunId: run.id,
    requestId: request.id,
    hotelAuditId: hotel.id,
    reportType: "single_property",
    reportStatus: REPORT_STATUS.DRAFT,
    layoutMode: "three_page_v1",
    researchMode: request.researchMode || "leak_audit_lite",
    hotelName: request.hotelName || sample.hotelName,
    hotelLocation: request.city
      ? [request.city, request.country].filter(Boolean).join(", ")
      : sample.hotelLocation,
    locationLabel: sample.locationLabel,
    runDate: run.runDate,
    providersUsed: run.providersUsed,
    coverMetaLine:
      sample.coverMeta?.coverMetaLine ||
      "PROVIDERS 4 · SCENARIOS 15 · OBSERVATIONS 60 · ACTION ITEMS 3",
    diagnosticScopeLabel:
      sample.diagnosticScopeLabel ||
      "Limited diagnostic · 15 traveler scenarios across 4 AI providers",
    scenarioSummaryLabel: sample.scenariosMonitored?.value || "15 × 4",
    bottomLineSummary: sample.bottomLineSummary,
    bottomLinePriority: sample.bottomLinePriority,
    biggestDemandLeak: sample.biggestDemandLeak,
    mainCompetitorShowingUpInstead: sample.mainCompetitorShowingUpInstead,
    competitorDisplacement: sample.competitorDisplacementRank?.rows || [],
    competitorDisplacementRank: sample.competitorDisplacementRank,
    likelyReason: sample.likelyReason || "",
    firstFix: sample.fixes?.[0]?.summary || "",
    secondFix: sample.fixes?.[1]?.summary || "",
    thirdFix: sample.fixes?.[2]?.summary || "",
    fixes: sample.fixes,
    whoDoesTheWork: sample.whoDoesTheWork,
    recommendedNextStep: sample.recommendedNextStep,
    executiveSummary: sample.executiveSummary,
    executiveSignals: sample.executiveSignals,
    demandAreaToReview: sample.demandAreaToReview,
    inferredDemandLeak: sample.inferredDemandLeak,
    supportingEvidence: sample.supportingEvidence,
    evidenceLibrary: sample.evidenceLibrary,
    scenariosMonitored: sample.scenariosMonitored,
    howToRead: sample.howToRead,
    coverDisclaimer: sample.coverDisclaimer,
    coverMeta: sample.coverMeta,
    sampleFindingCount: sample.sampleFindingCount,
  });

  store.updateRequest(request.id, { status: REQUEST_STATUS.REPORT_READY });

  const clientPayload = buildClientSafeReportPayload({
    report,
    request: store.getRequest(request.id),
    run,
  });

  return {
    ok: true,
    mode,
    request: store.getRequest(request.id),
    hotel,
    run,
    report,
    children,
    clientPayload,
    shareUrl: report.shareUrl,
    productionAdpTouched: false,
  };
}

export function markLeakAuditReportSent(store, reportId) {
  const report = store.getReport(reportId);
  if (!report) throw new Error("report_not_found");
  const updated = store.updateReport(reportId, {
    reportStatus: REPORT_STATUS.SENT || "sent",
    sentAt: new Date().toISOString(),
  });
  if (report.requestId) {
    store.updateRequest(report.requestId, { status: REQUEST_STATUS.SENT });
  }
  return updated;
}

export function ensureLeakAuditTempRoot(label = "gen") {
  const root = join(
    process.cwd(),
    "data/ai-demand-positioning/leak-audit/_tmp",
    `${label}_${Date.now().toString(36)}`
  );
  mkdirSync(root, { recursive: true });
  return root;
}
