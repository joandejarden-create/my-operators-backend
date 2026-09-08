/**
 * Orchestrate a Phase 1 leak audit run (manual / synthetic observations; no live provider spend).
 * Writes only to leak-audit store collections.
 */

import {
  FREE_AUDIT_SCOPE,
  PROVIDER_SET_VERSION,
  PROMPT_SET_VERSION,
  REQUEST_STATUS,
  RUN_STATUS,
  CONFIDENCE_FLAG,
  REPORT_STATUS,
  RESEARCH_MODES,
} from "./schema-v1.js";
import { matchHotelReadOnly } from "./hotel-match-v1.js";
import { selectDemandTerritories } from "./territory-select-v1.js";
import { buildLitePromptPlan } from "./prompt-catalog-v1.js";
import { buildPhase1ManualObservations } from "./observation-process-v1.js";
import { generateLeakAuditReportContent } from "./report-generator-v1.js";
import {
  LITE_PROVIDERS,
  buildScenariosMonitoredLabel,
  buildCoverMetaLine,
  buildDiagnosticScopeLabel,
  assertLiteCostControls,
} from "./research-mode-lite-v1.js";

const DEFAULT_PROVIDERS = [...LITE_PROVIDERS];

/**
 * @param {object} store — createLeakAuditStore()
 * @param {object} options
 * @param {string} options.requestId
 * @param {string[]} [options.providers]
 * @param {object[]} [options.seedObservations]
 * @param {boolean} [options.liveProviderCalls=false] — Phase 1 must stay false
 * @param {string[]} [options.failedProviders] — providers that failed; continue with rest
 */
export async function runLeakAuditPhase1(store, options = {}) {
  const requestId = options.requestId;
  const request = store.getRequest(requestId);
  if (!request) throw new Error("request_not_found");
  if (request.status === REQUEST_STATUS.REJECTED) {
    throw new Error("request_rejected");
  }

  if (options.liveProviderCalls === true) {
    throw new Error("live_provider_calls_not_enabled_phase1");
  }

  const costCheck = assertLiteCostControls(FREE_AUDIT_SCOPE);
  if (!costCheck.ok) {
    throw new Error(`lite_cost_controls_invalid:${costCheck.failures.join(",")}`);
  }

  const failedProviders = new Set(options.failedProviders || []);
  const providers = (options.providers || DEFAULT_PROVIDERS)
    .filter((p) => !failedProviders.has(p))
    .slice(0, FREE_AUDIT_SCOPE.maxProviders);
  if (providers.length < FREE_AUDIT_SCOPE.minProviders) {
    throw new Error("insufficient_providers");
  }

  const territories = selectDemandTerritories(request.demandSegmentOfInterest, {
    hotelName: request.hotelName,
    notes: request.notes,
  });
  const planned = buildLitePromptPlan(territories.keys, providers, {
    maxScenarios: FREE_AUDIT_SCOPE.maxScenarios,
    maxProviders: FREE_AUDIT_SCOPE.maxProviders,
    maxObservations: FREE_AUDIT_SCOPE.maxObservations,
  });
  const plan = planned.plan;

  const match = matchHotelReadOnly(request);

  store.updateRequest(requestId, { status: REQUEST_STATUS.RUNNING });

  const completenessFlag =
    failedProviders.size > 0 || providers.length < FREE_AUDIT_SCOPE.maxProviders
      ? "partial"
      : "adequate";

  let run = store.createRun({
    auditRequestId: requestId,
    matchedHotelId: match.matchedHotelId,
    researchMode: RESEARCH_MODES.LEAK_AUDIT_LITE,
    maxScenarios: FREE_AUDIT_SCOPE.maxScenarios,
    maxProviders: FREE_AUDIT_SCOPE.maxProviders,
    maxObservations: FREE_AUDIT_SCOPE.maxObservations,
    actualScenariosRun: planned.scenariosRun,
    actualProvidersRun: planned.providersRun,
    actualObservations: planned.observationsPlanned,
    providerSetVersion: PROVIDER_SET_VERSION,
    promptSetVersion: PROMPT_SET_VERSION,
    demandTerritoriesTested: territories.labels,
    providersUsed: providers,
    totalPromptsRun: plan.length,
    completenessFlag,
    costControlNotes: failedProviders.size
      ? `Provider failure(s): ${[...failedProviders].join(", ")}`
      : "leak_audit_lite cost controls applied",
    status: RUN_STATUS.RUNNING,
    errorLog: [],
  });

  try {
    const observationInputs = buildPhase1ManualObservations({
      hotelName: request.hotelName,
      territoryKeys: territories.keys,
      providers,
      seedObservations: options.seedObservations,
      plan,
    }).slice(0, FREE_AUDIT_SCOPE.maxObservations);

    const savedObs = [];
    for (const obs of observationInputs) {
      const { fullPromptText, promptText, _internalFullPromptText, ...safe } = obs;
      void fullPromptText;
      void promptText;
      void _internalFullPromptText;
      savedObs.push(
        store.createObservation({
          ...safe,
          auditRunId: run.id,
        })
      );
    }

    const subjectMentionCount = savedObs.filter((o) => o.subjectHotelMentioned).length;
    const competitorMentionCount = savedObs.reduce(
      (n, o) => n + (o.competitorsMentioned?.length || 0),
      0
    );
    const displacementCount = savedObs.filter((o) => o.displacedByCompetitor).length;

    const confidenceFlag =
      savedObs.length >= 45
        ? CONFIDENCE_FLAG.HIGH
        : savedObs.length >= 30
          ? CONFIDENCE_FLAG.MEDIUM
          : CONFIDENCE_FLAG.LOW;

    const content = generateLeakAuditReportContent({
      hotelName: request.hotelName,
      observations: savedObs,
      providersUsed: providers,
    });

    const scenariosMonitored = buildScenariosMonitoredLabel({
      scenariosRun: planned.scenariosRun,
      providersRun: providers.length,
    });

    run = store.updateRun(run.id, {
      status: RUN_STATUS.COMPLETED,
      totalObservations: savedObs.length,
      actualObservations: savedObs.length,
      actualProvidersRun: providers.length,
      subjectMentionCount,
      competitorMentionCount,
      displacementCount,
      confidenceFlag,
      completenessFlag:
        completenessFlag === "partial"
          ? "partial"
          : savedObs.length >= FREE_AUDIT_SCOPE.maxObservations * 0.6
            ? "adequate"
            : "partial",
      matchMeta: {
        matched: match.matched,
        matchMethod: match.matchMethod,
        confidence: match.confidence,
        readOnly: true,
      },
    });

    const report = store.createReport({
      auditRunId: run.id,
      reportStatus: REPORT_STATUS.DRAFT,
      hotelName: request.hotelName,
      runDate: run.runDate,
      providersUsed: providers,
      researchMode: RESEARCH_MODES.LEAK_AUDIT_LITE,
      diagnosticScopeLabel: buildDiagnosticScopeLabel({
        scenarios: planned.scenariosRun,
        providers: providers.length,
      }),
      scenarioSummaryLabel: scenariosMonitored.value,
      scenariosMonitored,
      coverMeta: {
        providers: providers.length,
        scenarios: planned.scenariosRun,
        observations: savedObs.length,
        actionItems: FREE_AUDIT_SCOPE.maxActionItems,
        coverMetaLine: buildCoverMetaLine({
          providers: providers.length,
          scenarios: planned.scenariosRun,
          observations: savedObs.length,
          actionItems: FREE_AUDIT_SCOPE.maxActionItems,
        }),
      },
      bottomLineSummary: content.bottomLineSummary,
      biggestDemandLeak: content.biggestDemandLeak,
      mainCompetitorShowingUpInstead: content.mainCompetitorShowingUpInstead,
      competitorDisplacement: (content.competitorDisplacement || []).slice(
        0,
        FREE_AUDIT_SCOPE.maxCompetitorsShown
      ),
      likelyReason: content.likelyReason,
      firstFix: content.firstFix,
      secondFix: content.secondFix,
      thirdFix: content.thirdFix,
      fixes: (content.fixes || []).slice(0, FREE_AUDIT_SCOPE.maxActionItems),
      whoDoesTheWork: content.whoDoesTheWork,
      recommendedNextStep: content.recommendedNextStep,
    });

    store.updateRequest(requestId, { status: REQUEST_STATUS.COMPLETED });

    return {
      ok: true,
      researchMode: RESEARCH_MODES.LEAK_AUDIT_LITE,
      request: store.getRequest(requestId),
      run,
      observations: savedObs,
      report,
      match,
      liveProviderCalls: 0,
      productionAdpTouched: false,
    };
  } catch (err) {
    const message = err?.message || String(err);
    store.updateRun(run.id, {
      status: RUN_STATUS.FAILED,
      errorLog: [
        ...(run.errorLog || []),
        { at: new Date().toISOString(), message, phase: "phase1_lite_run" },
      ],
    });
    store.updateRequest(requestId, { status: REQUEST_STATUS.ERROR });
    console.error("[leak-audit] run failed:", message);
    throw err;
  }
}

/**
 * Record a failed provider call on the leak-audit run only (no production writes).
 */
export function logLeakAuditProviderFailure(store, runId, failure) {
  const run = store.getRun(runId);
  if (!run) throw new Error("run_not_found");
  const entry = {
    at: new Date().toISOString(),
    provider: failure?.provider || "unknown",
    message: failure?.message || "provider_call_failed",
    phase: "provider_call",
  };
  return store.updateRun(runId, {
    errorLog: [...(run.errorLog || []), entry],
  });
}
