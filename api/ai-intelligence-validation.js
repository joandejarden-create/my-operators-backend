/**
 * AI Intelligence Validation Scorecard read API — no provider calls.
 */

import {
  loadLatestValidationReport,
  runAiIntelligenceValidation,
  METHODOLOGY_NOTE,
  resolveValidationStorageRoot,
  VALIDATION_RUNNER_VERSION,
} from "../lib/ai-visibility/validation/run-validation.js";
import { VALIDATION_STORAGE_VERSION } from "../lib/ai-visibility/validation/validation-storage-root.js";

export const AI_INTELLIGENCE_VALIDATION_API_VERSION =
  "ai_intelligence_validation_api_v1";

export { VALIDATION_RUNNER_VERSION };

function ok(res, data) {
  if (process.env.NODE_ENV !== "production") {
    res.setHeader("X-Dealality-Validation-Api", AI_INTELLIGENCE_VALIDATION_API_VERSION);
    res.setHeader("X-Dealality-Validation-Runner", VALIDATION_RUNNER_VERSION || "");
  }
  return res.json({ ok: true, success: true, ...data });
}

function fail(res, status, code, message, extra = {}) {
  return res.status(status).json({
    ok: false,
    success: false,
    error: code,
    code,
    message,
    ...extra,
  });
}

async function loadOrRunReport(req, { refresh = false } = {}) {
  const root = resolveValidationStorageRoot({});
  let report = loadLatestValidationReport({});
  const wantRefresh = refresh || req.query.refresh === "1";
  if (!report || wantRefresh) {
    report = await runAiIntelligenceValidation({ writeFiles: true });
  }
  if (!report) {
    const err = new Error("VALIDATION_REPORT_MISSING");
    err.code = "VALIDATION_REPORT_MISSING";
    throw err;
  }
  return { report, root };
}

function wrap(handler) {
  return async (req, res) => {
    try {
      return await handler(req, res);
    } catch (err) {
      console.error("[ai-intelligence-validation]", err?.message || err);
      const code = err?.code || "SERVER_ERROR";
      if (code === "VALIDATION_REPORT_MISSING") {
        return fail(res, 404, "VALIDATION_REPORT_MISSING", "Validation report not found. Run npm run ai-intelligence:validate.");
      }
      if (code === "VALIDATION_REPORT_INVALID") {
        return fail(res, 500, "VALIDATION_REPORT_INVALID", "Validation report could not be parsed.");
      }
      return fail(res, 500, "SERVER_ERROR", "Validation Scorecard could not load results.");
    }
  };
}

export const getAiIntelligenceValidationSummary = wrap(async (req, res) => {
  const { report, root } = await loadOrRunReport(req, { refresh: req.query.refresh === "1" });
  return ok(res, {
    summary: report.summary,
    systemCards: report.systemCards,
    topSummary: report.topSummary || null,
    scorecardSections: report.scorecardSections || null,
    methodologyNote: report.methodologyNote || METHODOLOGY_NOTE,
    operationalMethodologyNote: report.operationalMethodologyNote || null,
    recommendation: report.recommendation,
    generatedAt: report.generatedAt,
    freshness: report.freshness || null,
    storage: report.storage || {
      VALIDATION_WRITE_ROOT: root.rootDir,
      VALIDATION_READ_ROOT: root.rootDir,
      ROOTS_MATCH: true,
    },
    automaticPublicationBlocking: report.automaticPublicationBlocking,
    publicationGateUnchanged: report.publicationGateUnchanged !== false,
    classificationThreshold: report.classificationThreshold || null,
    portfolioIntegrity: report.portfolioIntegrity || null,
    humanReview: report.humanReview || null,
    apiVersion: AI_INTELLIGENCE_VALIDATION_API_VERSION,
    storageVersion: VALIDATION_STORAGE_VERSION,
  });
});

export const getAiIntelligenceValidationGates = wrap(async (req, res) => {
  const { report } = await loadOrRunReport(req);
  return ok(res, { gates: report.gates || [], generatedAt: report.generatedAt });
});

export const getAiIntelligenceValidationClassification = wrap(async (req, res) => {
  const { report } = await loadOrRunReport(req);
  return ok(res, {
    goldenSet: report.goldenSet || null,
    classificationThreshold: report.classificationThreshold || null,
    generatedAt: report.generatedAt,
  });
});

export const getAiIntelligenceValidationBatches = wrap(async (req, res) => {
  const { report } = await loadOrRunReport(req);
  return ok(res, { batches: report.batches || [], generatedAt: report.generatedAt });
});

export const getAiIntelligenceValidationIssues = wrap(async (req, res) => {
  const { report } = await loadOrRunReport(req);
  return ok(res, {
    issues: report.issues || [],
    generatedAt: report.generatedAt,
  });
});

export const getAiIntelligenceValidationVariability = wrap(async (req, res) => {
  const { report } = await loadOrRunReport(req);
  return ok(res, {
    variability: report.variability || null,
    manualReview: report.manualReview || null,
    humanReview: report.humanReview || null,
    methodologyNote: METHODOLOGY_NOTE,
    operationalMethodologyNote: report.operationalMethodologyNote || null,
    freshness: report.freshness || null,
    generatedAt: report.generatedAt,
  });
});

export const getAiIntelligenceValidationOperations = wrap(async (req, res) => {
  const { report } = await loadOrRunReport(req);
  const ops = report.monitoringOperations || null;
  if (!ops) {
    return fail(
      res,
      404,
      "MONITORING_OPS_MISSING",
      "Monitoring operations not present in validation report. Re-run npm run ai-intelligence:validate."
    );
  }
  return ok(res, {
    monitoringOperations: ops,
    topSummary: report.topSummary || null,
    scorecardSections: report.scorecardSections || null,
    operationalMethodologyNote: report.operationalMethodologyNote || null,
    generatedAt: report.generatedAt,
  });
});

export const getAiIntelligenceValidationBatchDetail = wrap(async (req, res) => {
  const { report } = await loadOrRunReport(req);
  const batchId = req.params?.batchId || req.query?.batchId;
  if (!batchId) {
    return fail(res, 400, "BATCH_ID_REQUIRED", "batchId is required");
  }
  const fromBatches = (report.batches || []).find((b) => b.BATCH_ID === batchId);
  const fromInventory = (report.monitoringOperations?.inventory || []).find(
    (b) => b.BATCH_ID === batchId
  );
  if (!fromBatches && !fromInventory) {
    return fail(res, 404, "BATCH_NOT_FOUND", `Batch ${batchId} not found in validation report`);
  }
  return ok(res, {
    batch: {
      ...(fromBatches || {}),
      inventory: fromInventory || null,
      note: "Raw provider responses are not exposed by default.",
    },
    generatedAt: report.generatedAt,
  });
});
