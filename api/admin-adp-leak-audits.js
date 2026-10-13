/**
 * Admin APIs — ADP Demand Leak Audit Runner (Phase 1).
 * All mutating handlers require adpMonthlyReviewAdminAuth on the route (server.js).
 */

import {
  createLeakAuditStore,
  createLeakAuditRepository,
  detectLeakAuditStorageBackend,
  REQUEST_STATUS,
  REQUEST_SOURCES,
  LEAK_AUDIT_DEMAND_TERRITORIES,
  promoteLeakAuditToPilotStub,
  PROMOTE_CONFIRMATION_TEXT,
  buildClientSafeReportPayload,
  loadLeakAuditSampleReport,
  loadLeakAuditPortfolioSampleReport,
  generateSinglePropertyLeakAuditReport,
  markLeakAuditReportSent,
} from "../lib/ai-demand-positioning/leak-audit/index.js";

function storeFromReq(req) {
  // Prefer Phase 2A live repository (filesystem fallback or Airtable dual-write).
  // LEAK_AUDIT_ROOT still isolates test harnesses.
  if (process.env.LEAK_AUDIT_ROOT) {
    return createLeakAuditStore({ root: process.env.LEAK_AUDIT_ROOT });
  }
  return createLeakAuditRepository();
}

function adminIdentity(req) {
  return (
    req.dealalityUser?.email ||
    req.dealalityUser?.memberstackId ||
    req.dealalityUser?.id ||
    "admin"
  );
}

function enrichRequest(store, request) {
  const runs = store.listRuns({ auditRequestId: request.id });
  const latestRun = runs[0] || null;
  const reports = latestRun ? store.listReports({ auditRunId: latestRun.id }) : [];
  const latestReport = reports[0] || null;
  return {
    ...request,
    latestRunId: latestRun?.id || null,
    latestRunStatus: latestRun?.status || null,
    reportId: latestReport?.id || null,
    reportUrl: latestReport?.reportUrl || null,
    actions: availableActions(request, latestReport),
  };
}

function availableActions(request, report) {
  const status = request.status;
  return {
    approve: status === REQUEST_STATUS.REQUESTED,
    runAudit:
      status === REQUEST_STATUS.APPROVED ||
      status === REQUEST_STATUS.COMPLETED ||
      status === REQUEST_STATUS.ERROR,
    viewReport: Boolean(report?.id),
    markSent: status === REQUEST_STATUS.COMPLETED || status === REQUEST_STATUS.SENT,
    promoteToPilot:
      status === REQUEST_STATUS.COMPLETED ||
      status === REQUEST_STATUS.SENT ||
      status === REQUEST_STATUS.CONVERTED,
  };
}

export function getAdminLeakAuditMeta(req, res) {
  try {
    const storage = detectLeakAuditStorageBackend();
    return res.json({
      ok: true,
      productName: "AI Demand Leak Audit",
      phase: "2A",
      liveProviderCalls: false,
      researchModeDefault: "leak_audit_lite",
      storageBackend: storage.backend,
      storageInfo: storage,
      requestStatuses: Object.values(REQUEST_STATUS),
      sources: REQUEST_SOURCES,
      demandTerritories: LEAK_AUDIT_DEMAND_TERRITORIES,
      promoteConfirmationText: PROMOTE_CONFIRMATION_TEXT,
    });
  } catch (err) {
    console.error("[leak-audit admin] meta error:", err);
    return res.status(500).json({ ok: false, error: "internal_error", message: err.message });
  }
}

export function getAdminLeakAudits(req, res) {
  try {
    const store = storeFromReq(req);
    const rows = store.listRequests({
      status: req.query.status || undefined,
      q: req.query.q || req.query.search || undefined,
    });
    const counts = {};
    for (const s of Object.values(REQUEST_STATUS)) counts[s] = 0;
    for (const r of store.listRequests()) {
      counts[r.status] = (counts[r.status] || 0) + 1;
    }
    return res.json({
      ok: true,
      counts,
      requests: rows.map((r) => enrichRequest(store, r)),
      liveProviderCalls: 0,
    });
  } catch (err) {
    console.error("[leak-audit admin] list error:", err);
    return res.status(500).json({ ok: false, error: "internal_error", message: err.message });
  }
}

function mapRequestStatusToPickerStatus(status) {
  const s = String(status || "").toLowerCase();
  if (s === REQUEST_STATUS.SENT) return "Sent";
  if (s === REQUEST_STATUS.CONVERTED) return "Converted";
  if (s === REQUEST_STATUS.ERROR || s === REQUEST_STATUS.REJECTED) return "Error";
  if (s === REQUEST_STATUS.APPROVED || s === REQUEST_STATUS.COMPLETED) return "Approved";
  return "Draft";
}

function mapSourceToPickerLabel(source) {
  const raw = String(source || "").trim();
  if (!raw) return "Manual";
  const lower = raw.toLowerCase();
  if (lower === "linkedin") return "LinkedIn";
  if (lower === "referral") return "Referral";
  if (lower === "direct") return "Direct";
  if (lower === "manual") return "Manual";
  if (lower === "sample") return "Sample";
  if (lower === "other") return "Manual";
  return raw.charAt(0).toUpperCase() + raw.slice(1);
}

function catalogLabel(name, reportTypeLabel, statusLabel, runDate) {
  return `${name} — ${reportTypeLabel} — ${statusLabel} — ${runDate}`;
}

function buildSampleCatalogEntries() {
  const single = loadLeakAuditSampleReport();
  const portfolio = loadLeakAuditPortfolioSampleReport();
  const singleDate = String(single.runDate || "2026-09-07").slice(0, 10);
  const portfolioDate = String(portfolio.runDate || "2026-09-07").slice(0, 10);
  const singleName = single.hotelName || "Cambridge Beaches Resort & Spa";
  const portfolioName = "Dovetail Portfolio";
  return [
    {
      catalogId: "sample:single:cambridge-beaches",
      isSample: true,
      reportType: "single_property",
      reportTypeLabel: "Single-property",
      status: "Sample",
      source: "Sample",
      displayName: singleName,
      runDate: singleDate,
      label: catalogLabel(singleName, "Single-property", "Sample", singleDate),
      previewUrl: "/adp-leak-audit/sample",
      shareUrl: "/adp-leak-audit/sample",
      reportId: null,
      requestId: null,
      canMarkSent: false,
      canPromote: false,
      canDownloadPdf: true,
    },
    {
      catalogId: "sample:portfolio:dovetail",
      isSample: true,
      reportType: "portfolio",
      reportTypeLabel: "Portfolio",
      status: "Sample",
      source: "Sample",
      displayName: portfolioName,
      runDate: portfolioDate,
      label: catalogLabel(portfolioName, "Portfolio", "Sample", portfolioDate),
      previewUrl: "/adp-leak-audit/sample-portfolio",
      shareUrl: "/adp-leak-audit/sample-portfolio",
      reportId: null,
      requestId: null,
      canMarkSent: false,
      canPromote: false,
      canDownloadPdf: true,
      portfolioName: portfolio.portfolioName || null,
    },
  ];
}

/**
 * Admin report picker catalog — samples + completed client reports (newest first).
 * Query: reportType=all|single_property|portfolio|sample, status=, source=
 */
export function getAdminLeakAuditReportCatalog(req, res) {
  try {
    const store = storeFromReq(req);
    const reportTypeFilter = String(req.query.reportType || "all").toLowerCase();
    const statusFilter = String(req.query.status || "all");
    const sourceFilter = String(req.query.source || "all");

    const entries = [...buildSampleCatalogEntries()];

    for (const request of store.listRequests()) {
      const enriched = enrichRequest(store, request);
      if (!enriched.reportId) continue;
      const report = store.getReport(enriched.reportId);
      if (!report) continue;
      const runDate = String(report.runDate || report.createdAt || request.updatedAt || "")
        .slice(0, 10) || "—";
      const name = report.hotelName || request.hotelName || "Untitled hotel";
      const statusLabel = mapRequestStatusToPickerStatus(request.status);
      const sourceLabel = mapSourceToPickerLabel(request.source);
      const isPortfolio = Boolean(request.portfolioGroupId || request.reportMode === "portfolio");
      const reportType = isPortfolio ? "portfolio" : "single_property";
      const reportTypeLabel = isPortfolio ? "Portfolio" : "Single-property";
      const sharePath = report.reportUrl || `/adp-leak-audit/${report.id}`;
      entries.push({
        catalogId: `report:${report.id}`,
        isSample: false,
        reportType,
        reportTypeLabel,
        status: statusLabel,
        source: sourceLabel,
        displayName: name,
        runDate,
        label: catalogLabel(name, reportTypeLabel, statusLabel, runDate),
        previewUrl: sharePath,
        shareUrl: sharePath,
        reportId: report.id,
        requestId: request.id,
        canMarkSent: Boolean(enriched.actions?.markSent),
        canPromote: Boolean(enriched.actions?.promoteToPilot),
        canDownloadPdf: true,
        sortAt: report.createdAt || request.updatedAt || runDate,
      });
    }

    entries.sort((a, b) => {
      const aKey = String(a.sortAt || a.runDate || "");
      const bKey = String(b.sortAt || b.runDate || "");
      const byDate = bKey.localeCompare(aKey);
      if (byDate !== 0) return byDate;
      return String(a.label || "").localeCompare(String(b.label || ""));
    });

    const filtered = entries.filter((row) => {
      if (reportTypeFilter && reportTypeFilter !== "all") {
        if (reportTypeFilter === "sample") {
          if (!row.isSample) return false;
        } else if (row.reportType !== reportTypeFilter) {
          return false;
        }
      }
      if (statusFilter && statusFilter !== "all") {
        if (String(row.status).toLowerCase() !== statusFilter.toLowerCase()) return false;
      }
      if (sourceFilter && sourceFilter !== "all") {
        if (String(row.source).toLowerCase() !== sourceFilter.toLowerCase()) return false;
      }
      return true;
    });

    return res.json({
      ok: true,
      promoteConfirmationText: PROMOTE_CONFIRMATION_TEXT,
      storageBackend: store.storageBackend || detectLeakAuditStorageBackend().backend,
      reports: filtered.map(({ sortAt, ...rest }) => rest),
      total: filtered.length,
    });
  } catch (err) {
    console.error("[leak-audit admin] report-catalog error:", err);
    return res.status(500).json({ ok: false, error: "internal_error", message: err.message });
  }
}

export function postAdminLeakAuditCreate(req, res) {
  try {
    const store = storeFromReq(req);
    const body = req.body || {};
    const request = store.createRequest({
      hotelName: body.hotelName,
      hotelWebsite: body.hotelWebsite,
      city: body.city,
      market: body.market,
      country: body.country,
      contactName: body.contactName,
      contactEmail: body.contactEmail,
      companyName: body.companyName,
      role: body.role,
      demandSegmentOfInterest: body.demandSegmentOfInterest,
      commercialPriority: body.commercialPriority,
      demandPriority: body.demandPriority,
      secondaryDemandPriority: body.secondaryDemandPriority,
      portfolioName: body.portfolioName,
      portfolioGroupId: body.portfolioGroupId,
      audienceType: body.audienceType,
      reportMode: body.reportMode,
      decisionContext: body.decisionContext,
      urgencyReason: body.urgencyReason,
      prioritySource: body.prioritySource,
      researchMode: body.researchMode || "leak_audit_lite",
      requestType: body.requestType || body.reportMode || "single_property",
      source: body.source,
      sourceCampaign: body.sourceCampaign,
      sourceUrl: body.sourceUrl,
      notes: body.notes,
    });
    return res.status(201).json({
      ok: true,
      request: enrichRequest(store, request),
      storageBackend: store.storageBackend || detectLeakAuditStorageBackend().backend,
      productionRecordsTouched: false,
    });
  } catch (err) {
    console.error("[leak-audit admin] create error:", err);
    const status = err.message === "hotelName_required" ? 400 : 500;
    return res.status(status).json({ ok: false, error: err.message });
  }
}

export function getAdminLeakAuditById(req, res) {
  try {
    const store = storeFromReq(req);
    const request = store.getRequest(req.params.requestId);
    if (!request) {
      return res.status(404).json({ ok: false, error: "not_found" });
    }
    const runs = store.listRuns({ auditRequestId: request.id });
    const observations = runs.flatMap((run) =>
      store.listObservations({ auditRunId: run.id })
    );
    const reports = runs.flatMap((run) => store.listReports({ auditRunId: run.id }));
    return res.json({
      ok: true,
      request: enrichRequest(store, request),
      runs,
      observations: observations.map(stripInternalObservation),
      reports,
    });
  } catch (err) {
    console.error("[leak-audit admin] get error:", err);
    return res.status(500).json({ ok: false, error: "internal_error", message: err.message });
  }
}

function stripInternalObservation(obs) {
  const { fullPromptText, promptText, _internalFullPromptText, ...rest } = obs || {};
  void fullPromptText;
  void promptText;
  void _internalFullPromptText;
  return rest;
}

export function postAdminLeakAuditApprove(req, res) {
  try {
    const store = storeFromReq(req);
    const request = store.getRequest(req.params.requestId);
    if (!request) return res.status(404).json({ ok: false, error: "not_found" });
    if (request.status !== REQUEST_STATUS.REQUESTED) {
      return res.status(400).json({
        ok: false,
        error: "invalid_status",
        message: `Cannot approve from status ${request.status}`,
      });
    }
    const updated = store.updateRequest(request.id, { status: REQUEST_STATUS.APPROVED });
    return res.json({ ok: true, request: enrichRequest(store, updated) });
  } catch (err) {
    console.error("[leak-audit admin] approve error:", err);
    return res.status(500).json({ ok: false, error: "internal_error", message: err.message });
  }
}

export async function postAdminLeakAuditRun(req, res) {
  try {
    const store = storeFromReq(req);
    const request = store.getRequest(req.params.requestId);
    if (!request) return res.status(404).json({ ok: false, error: "not_found" });
    if (
      request.status !== REQUEST_STATUS.APPROVED &&
      request.status !== REQUEST_STATUS.COMPLETED &&
      request.status !== REQUEST_STATUS.ERROR
    ) {
      return res.status(400).json({
        ok: false,
        error: "invalid_status",
        message: "Approve the request before running the audit.",
      });
    }

    const result = await generateSinglePropertyLeakAuditReport(store, {
      requestId: request.id,
      mode: req.body?.mode === "phase1" ? "phase1" : "sample_seed",
    });

    return res.json({
      ok: true,
      ...result,
      request: enrichRequest(store, store.getRequest(request.id)),
      storageBackend: store.storageBackend || detectLeakAuditStorageBackend().backend,
      productionRecordsTouched: false,
      liveProviderCalls: 0,
    });
  } catch (err) {
    console.error("[leak-audit admin] run error:", err);
    return res.status(500).json({ ok: false, error: "run_failed", message: err.message });
  }
}

export function postAdminLeakAuditMarkSent(req, res) {
  try {
    const store = storeFromReq(req);
    const request = store.getRequest(req.params.requestId);
    if (!request) return res.status(404).json({ ok: false, error: "not_found" });

    const runs = store.listRuns({ auditRequestId: request.id });
    const latestRun = runs[0];
    if (!latestRun) {
      return res.status(400).json({ ok: false, error: "no_run" });
    }
    const reports = store.listReports({ auditRunId: latestRun.id });
    const report = reports[0];
    if (!report) {
      return res.status(400).json({ ok: false, error: "no_report" });
    }

    const updatedReport = store.updateReport(report.id, {
      reportStatus: "sent",
      sentAt: new Date().toISOString(),
    });
    const updatedRequest = store.updateRequest(request.id, {
      status: REQUEST_STATUS.SENT,
    });

    return res.json({
      ok: true,
      request: enrichRequest(store, updatedRequest),
      report: updatedReport,
    });
  } catch (err) {
    console.error("[leak-audit admin] mark-sent error:", err);
    return res.status(500).json({ ok: false, error: "internal_error", message: err.message });
  }
}

export function postAdminLeakAuditPromote(req, res) {
  try {
    const store = storeFromReq(req);
    const confirmed = req.body?.confirmed === true;
    if (!confirmed) {
      return res.status(400).json({
        ok: false,
        error: "confirmation_required",
        confirmationText: PROMOTE_CONFIRMATION_TEXT,
        message: "Explicit confirmation is required before promote-to-pilot.",
      });
    }

    const result = promoteLeakAuditToPilotStub(store, {
      requestId: req.params.requestId,
      confirmed: true,
      confirmedBy: adminIdentity(req),
    });

    return res.json({
      ok: true,
      ...result,
      request: enrichRequest(store, result.request),
      productionAdpRecordCreated: false,
      productionRecordsTouched: false,
    });
  } catch (err) {
    console.error("[leak-audit admin] promote error:", err);
    const status = err.code === "PROMOTION_CONFIRMATION_REQUIRED" ? 400 : 500;
    return res.status(status).json({
      ok: false,
      error: err.code || "promote_failed",
      message: err.message,
      confirmationText: PROMOTE_CONFIRMATION_TEXT,
    });
  }
}

/** Public client-safe report (no admin auth). Unguessable report id or share token. */
export function getLeakAuditClientReport(req, res) {
  try {
    const store = storeFromReq(req);
    const key = req.params.reportId || req.params.shareToken;
    let report = store.getReport(key);
    if (!report) report = store.getReportByShareToken(key);
    if (!report) {
      const portfolioReport = store.getPortfolioReportByShareToken(key);
      if (portfolioReport?.clientReport) {
        return res.json({
          ok: true,
          report: portfolioReport.clientReport,
          reportMode: "portfolio_rollup",
          shareToken: portfolioReport.shareToken,
        });
      }
      return res.status(404).json({ ok: false, error: "not_found" });
    }
    const run = store.getRun(report.auditRunId);
    const request = run ? store.getRequest(run.auditRequestId) : null;
    const payload = buildClientSafeReportPayload({ report, request, run });
    return res.json({ ok: true, report: payload });
  } catch (err) {
    console.error("[leak-audit] client report error:", err);
    return res.status(500).json({ ok: false, error: "internal_error", message: err.message });
  }
}

/** Public sales sample report — static fixture only. */
export function getLeakAuditSampleReport(req, res) {
  try {
    const report = loadLeakAuditSampleReport();
    return res.json({
      ok: true,
      sample: true,
      report,
      productionRecordsTouched: false,
    });
  } catch (err) {
    console.error("[leak-audit] sample report error:", err);
    return res.status(500).json({
      ok: false,
      error: "sample_unavailable",
      message: err.message,
      failures: err.failures || undefined,
    });
  }
}

/** Public portfolio sample roll-up — static fixture only. */
export function getLeakAuditPortfolioSampleReport(req, res) {
  try {
    const report = loadLeakAuditPortfolioSampleReport();
    return res.json({
      ok: true,
      sample: true,
      reportMode: "portfolio_rollup",
      report,
      productionRecordsTouched: false,
    });
  } catch (err) {
    console.error("[leak-audit] portfolio sample error:", err);
    return res.status(500).json({
      ok: false,
      error: "portfolio_sample_unavailable",
      message: err.message,
      failures: err.failures || undefined,
    });
  }
}

/**
 * Public diagnostic payload for demo report access / styling QA.
 * Confirms fixture load + required sections without exposing prompts/IDs.
 */
export function getLeakAuditDemoDiagnostics(req, res) {
  try {
    const sample = loadLeakAuditSampleReport();
    const portfolio = loadLeakAuditPortfolioSampleReport();
    const sampleSections = {
      whoDoesTheWork: Boolean(sample.whoDoesTheWork),
      executiveSignals: Array.isArray(sample.executiveSignals)
        ? sample.executiveSignals.length
        : 0,
      howToRead: Boolean(sample.howToRead),
      inferredDemandLeak: Boolean(sample.inferredDemandLeak),
      evidenceLibrary: Array.isArray(sample.evidenceLibrary)
        ? sample.evidenceLibrary.length
        : 0,
      competitorDisplacementRank: Array.isArray(
        sample.competitorDisplacementRank?.rows
      )
        ? sample.competitorDisplacementRank.rows.length
        : 0,
      fixes: Array.isArray(sample.fixes) ? sample.fixes.length : 0,
      layoutMode: sample.layoutMode || null,
    };
    const missingSample = Object.entries(sampleSections)
      .filter(([k, v]) => {
        if (k === "layoutMode") return !v;
        return !v;
      })
      .map(([k]) => k);

    return res.json({
      ok: true,
      productionRecordsTouched: false,
      routes: {
        singlePropertySample: "/adp-leak-audit/sample",
        portfolioSample: "/adp-leak-audit/sample-portfolio",
        adminList: "/admin/adp-leak-audits",
        adminWorkflow: "/admin/adp-leak-audits/workflow",
        sampleApi: "/api/adp-leak-audit/sample-report",
        portfolioApi: "/api/adp-leak-audit/sample-portfolio-report",
      },
      cssStack: [
        "/css/brand-alignment-snapshot.css",
        "/css/dealality-report-print-chrome.css",
        "/css/dealality-report-system-v1.css",
        "/css/adp-monthly-review-report-v1.css",
        "/css/adp-leak-audit-report-v2.css",
      ],
      reportFamily: "dealality-report-system-v1",
      styleReference: "/adp-monthly-review-pdf-render.html",
      sample: {
        reportId: sample.reportId,
        fixtureLoaded: true,
        sections: sampleSections,
        missingSections: missingSample,
        evidenceObjectsFound: sampleSections.evidenceLibrary,
        wouldShowUnavailable: missingSample.length > 0,
      },
      portfolio: {
        reportId: portfolio.reportId,
        fixtureLoaded: true,
        hotelsNeedingAttention: portfolio.hotelsNeedingAttention?.length || 0,
        segments: portfolio.inferredDemandSegmentsLeaking?.length || 0,
        competitors: portfolio.competitorsBenefiting?.length || 0,
        actions: portfolio.portfolioActions?.length || 0,
      },
    });
  } catch (err) {
    console.error("[leak-audit] demo diagnostics error:", err);
    return res.status(500).json({
      ok: false,
      error: "demo_diagnostics_failed",
      message: err.message,
    });
  }
}
