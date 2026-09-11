/**
 * Filesystem store for AdpLeakAudit* collections.
 * Isolated under data/ai-demand-positioning/leak-audit/ (or LEAK_AUDIT_ROOT).
 */

import {
  existsSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  writeFileSync,
  renameSync,
} from "node:fs";
import { join, resolve } from "node:path";
import { randomBytes } from "node:crypto";
import { assertLeakAuditWritePathAllowed } from "./isolation-guards-v1.js";

const COLLECTIONS = Object.freeze({
  requests: "requests",
  hotels: "hotels",
  runs: "runs",
  observations: "observations",
  metrics: "metrics",
  competitors: "competitors",
  evidence: "evidence",
  actions: "actions",
  reports: "reports",
  portfolios: "portfolios",
  portfolioHotels: "portfolio-hotels",
  portfolioRuns: "portfolio-runs",
  portfolioReports: "portfolio-reports",
  promotions: "promotions",
});

export function resolveLeakAuditRoot(overrideRoot) {
  if (overrideRoot) return resolve(overrideRoot);
  if (process.env.LEAK_AUDIT_ROOT) return resolve(process.env.LEAK_AUDIT_ROOT);
  return resolve(process.cwd(), "data/ai-demand-positioning/leak-audit");
}

function ensureDir(dir, root) {
  assertLeakAuditWritePathAllowed(dir, { root });
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
}

function collectionDir(root, collection) {
  const dir = join(root, collection);
  ensureDir(dir, root);
  return dir;
}

function nowIso() {
  return new Date().toISOString();
}

export function newLeakAuditId(prefix) {
  return `${prefix}_${Date.now().toString(36)}_${randomBytes(4).toString("hex")}`;
}

function atomicWriteJson(filePath, data, root) {
  assertLeakAuditWritePathAllowed(filePath, { root });
  const tmp = `${filePath}.${process.pid}.tmp`;
  writeFileSync(tmp, JSON.stringify(data, null, 2), "utf8");
  renameSync(tmp, filePath);
}

function readJson(filePath) {
  return JSON.parse(readFileSync(filePath, "utf8"));
}

function listJsonFiles(dir) {
  if (!existsSync(dir)) return [];
  return readdirSync(dir).filter((f) => f.endsWith(".json") && !f.endsWith(".tmp"));
}

function recordPath(root, collection, id) {
  return join(collectionDir(root, collection), `${id}.json`);
}

export function createLeakAuditStore(options = {}) {
  const root = resolveLeakAuditRoot(options.root);
  ensureDir(root, root);
  for (const c of Object.values(COLLECTIONS)) ensureDir(join(root, c), root);

  function save(collection, record) {
    if (!record?.id) throw new Error(`leak_audit_missing_id:${collection}`);
    const path = recordPath(root, collection, record.id);
    atomicWriteJson(path, record, root);
    return record;
  }

  function load(collection, id) {
    if (!id) return null;
    const path = recordPath(root, collection, id);
    if (!existsSync(path)) return null;
    return readJson(path);
  }

  function list(collection) {
    const dir = collectionDir(root, collection);
    return listJsonFiles(dir)
      .map((f) => {
        try {
          return readJson(join(dir, f));
        } catch (err) {
          console.error(`[leak-audit] failed to read ${collection}/${f}:`, err.message);
          return null;
        }
      })
      .filter(Boolean);
  }

  function update(collection, id, patch) {
    const existing = load(collection, id);
    if (!existing) throw new Error(`leak_audit_not_found:${collection}:${id}`);
    const next = {
      ...existing,
      ...patch,
      id: existing.id,
      updatedAt: nowIso(),
    };
    return save(collection, next);
  }

  return {
    root,
    COLLECTIONS,

    createRequest(fields) {
      const ts = nowIso();
      const optional = (v) => String(v || "").trim();
      const record = {
        id: newLeakAuditId("leak_req"),
        hotelName: optional(fields.hotelName),
        hotelWebsite: optional(fields.hotelWebsite),
        city: optional(fields.city),
        market: optional(fields.market),
        country: optional(fields.country),
        contactName: optional(fields.contactName),
        contactEmail: optional(fields.contactEmail),
        companyName: optional(fields.companyName),
        role: optional(fields.role),
        demandSegmentOfInterest: optional(fields.demandSegmentOfInterest),
        // Optional strategy fields — free audit does not require these
        commercialPriority: optional(fields.commercialPriority) || null,
        demandPriority: optional(fields.demandPriority) || null,
        secondaryDemandPriority: optional(fields.secondaryDemandPriority) || null,
        portfolioName: optional(fields.portfolioName) || null,
        portfolioGroupId: optional(fields.portfolioGroupId) || null,
        audienceType: optional(fields.audienceType) || null,
        reportMode: optional(fields.reportMode) || "single_property",
        decisionContext: optional(fields.decisionContext) || null,
        urgencyReason: optional(fields.urgencyReason) || null,
        prioritySource:
          optional(fields.prioritySource) ||
          (optional(fields.demandPriority) || optional(fields.commercialPriority)
            ? "client_confirmed"
            : "inferred_from_results"),
        researchMode: optional(fields.researchMode) || "leak_audit_lite",
        sourceCampaign: optional(fields.sourceCampaign) || null,
        sourceUrl: optional(fields.sourceUrl) || null,
        optionalDemandPriority:
          optional(fields.optionalDemandPriority) || optional(fields.demandPriority) || null,
        optionalCommercialPriority:
          optional(fields.optionalCommercialPriority) ||
          optional(fields.commercialPriority) ||
          null,
        status: "requested",
        source: optional(fields.source) || "manual",
        requestType: optional(fields.requestType) || "single_property",
        notes: optional(fields.notes),
        createdAt: ts,
        updatedAt: ts,
      };
      if (!record.hotelName) throw new Error("hotelName_required");
      return save(COLLECTIONS.requests, record);
    },

    getRequest(id) {
      return load(COLLECTIONS.requests, id);
    },

    listRequests(filters = {}) {
      let rows = list(COLLECTIONS.requests);
      if (filters.status) rows = rows.filter((r) => r.status === filters.status);
      if (filters.q) {
        const q = String(filters.q).toLowerCase();
        rows = rows.filter((r) =>
          [r.hotelName, r.contactName, r.companyName, r.contactEmail, r.city]
            .join(" ")
            .toLowerCase()
            .includes(q)
        );
      }
      rows.sort((a, b) => String(b.createdAt).localeCompare(String(a.createdAt)));
      return rows;
    },

    updateRequest(id, patch) {
      return update(COLLECTIONS.requests, id, patch);
    },

    createRun(fields) {
      const ts = nowIso();
      const maxScenarios = fields.maxScenarios != null ? Number(fields.maxScenarios) : 15;
      const maxProviders = fields.maxProviders != null ? Number(fields.maxProviders) : 4;
      const maxObservations =
        fields.maxObservations != null ? Number(fields.maxObservations) : 60;
      const providersUsed = fields.providersUsed || [];
      const scenarioCount =
        fields.scenarioCount != null
          ? Number(fields.scenarioCount)
          : fields.actualScenariosRun != null
            ? Number(fields.actualScenariosRun)
            : maxScenarios;
      const providerCount =
        fields.providerCount != null
          ? Number(fields.providerCount)
          : fields.actualProvidersRun != null
            ? Number(fields.actualProvidersRun)
            : providersUsed.length || maxProviders;
      const record = {
        id: newLeakAuditId("leak_run"),
        auditRequestId: fields.auditRequestId || fields.requestId,
        requestId: fields.requestId || fields.auditRequestId || null,
        hotelAuditId: fields.hotelAuditId || null,
        matchedHotelId: fields.matchedHotelId || null,
        researchMode: fields.researchMode || "leak_audit_lite",
        maxScenarios,
        maxProviders,
        maxObservations,
        scenarioCount,
        providerCount,
        scenariosMonitoredLabel:
          fields.scenariosMonitoredLabel || `${scenarioCount} × ${providerCount}`,
        observationsLabel:
          fields.observationsLabel ||
          `${scenarioCount * providerCount} observations`,
        actualScenariosRun:
          fields.actualScenariosRun != null ? Number(fields.actualScenariosRun) : scenarioCount,
        actualProvidersRun:
          fields.actualProvidersRun != null ? Number(fields.actualProvidersRun) : providerCount,
        actualObservations:
          fields.actualObservations != null
            ? Number(fields.actualObservations)
            : fields.totalObservations || 0,
        estimatedProviderCost:
          fields.estimatedProviderCost != null ? Number(fields.estimatedProviderCost) : null,
        providerCostCurrency: fields.providerCostCurrency || "USD",
        costControlNotes: fields.costControlNotes || "",
        liveProviderCalls: fields.liveProviderCalls != null ? Number(fields.liveProviderCalls) : 0,
        runDate: fields.runDate || ts.slice(0, 10),
        providerSetVersion: fields.providerSetVersion || "leak_audit_provider_set_v1",
        promptSetVersion: fields.promptSetVersion || "leak_audit_lite_v1",
        demandTerritoriesTested: fields.demandTerritoriesTested || [],
        providersUsed,
        totalPromptsRun: fields.totalPromptsRun || 0,
        totalObservations: fields.totalObservations || 0,
        subjectMentionCount: fields.subjectMentionCount || 0,
        subjectAbsentCount: fields.subjectAbsentCount || 0,
        competitorMentionCount: fields.competitorMentionCount || 0,
        competitorAbsentAppearanceCount:
          fields.competitorAbsentAppearanceCount || fields.displacementCount || 0,
        displacementCount: fields.displacementCount || 0,
        primaryAreaToReview: fields.primaryAreaToReview || null,
        confidenceFlag: fields.confidenceFlag || "low",
        completenessFlag: fields.completenessFlag || "partial",
        status: fields.status || "queued",
        errorLog: fields.errorLog || [],
        createdAt: ts,
        updatedAt: ts,
      };
      if (!record.auditRequestId) throw new Error("auditRequestId_required");
      return save(COLLECTIONS.runs, record);
    },

    getRun(id) {
      return load(COLLECTIONS.runs, id);
    },

    listRuns(filters = {}) {
      let rows = list(COLLECTIONS.runs);
      if (filters.auditRequestId) {
        rows = rows.filter((r) => r.auditRequestId === filters.auditRequestId);
      }
      rows.sort((a, b) => String(b.createdAt).localeCompare(String(a.createdAt)));
      return rows;
    },

    updateRun(id, patch) {
      return update(COLLECTIONS.runs, id, patch);
    },

    createObservation(fields) {
      const ts = nowIso();
      const record = {
        id: newLeakAuditId("leak_obs"),
        auditRunId: fields.auditRunId,
        provider: fields.provider,
        demandTerritory: fields.demandTerritory,
        promptId: fields.promptId,
        promptLabel: fields.promptLabel,
        promptIntentSummary: fields.promptIntentSummary,
        subjectHotelMentioned: Boolean(fields.subjectHotelMentioned),
        subjectMentionRank:
          fields.subjectMentionRank == null ? null : Number(fields.subjectMentionRank),
        competitorsMentioned: Array.isArray(fields.competitorsMentioned)
          ? fields.competitorsMentioned
          : [],
        displacedByCompetitor: Boolean(fields.displacedByCompetitor),
        displacedCompetitorName: fields.displacedCompetitorName || null,
        aiResponseExcerpt: String(fields.aiResponseExcerpt || "").slice(0, 800),
        citedSources: Array.isArray(fields.citedSources) ? fields.citedSources : [],
        sourceUrls: Array.isArray(fields.sourceUrls) ? fields.sourceUrls : [],
        notes: String(fields.notes || ""),
        createdAt: ts,
        // Never persist full proprietary prompt text
      };
      if (!record.auditRunId) throw new Error("auditRunId_required");
      if ("fullPromptText" in fields || "promptText" in fields) {
        throw new Error("full_prompt_text_forbidden_in_observations");
      }
      return save(COLLECTIONS.observations, record);
    },

    listObservations(filters = {}) {
      let rows = list(COLLECTIONS.observations);
      if (filters.auditRunId) {
        rows = rows.filter((r) => r.auditRunId === filters.auditRunId);
      }
      rows.sort((a, b) => String(a.createdAt).localeCompare(String(b.createdAt)));
      return rows;
    },

    createReport(fields) {
      const ts = nowIso();
      const id = fields.id || newLeakAuditId("leak_rpt");
      const shareToken =
        fields.shareToken ||
        `share_${randomBytes(12).toString("hex")}`;
      const record = {
        id,
        auditRunId: fields.auditRunId,
        requestId: fields.requestId || null,
        hotelAuditId: fields.hotelAuditId || null,
        reportType: fields.reportType || "single_property",
        reportStatus: fields.reportStatus || "draft",
        layoutMode: fields.layoutMode || "three_page_v1",
        bottomLineSummary: fields.bottomLineSummary || "",
        biggestDemandLeak: fields.biggestDemandLeak || "",
        mainCompetitorShowingUpInstead: fields.mainCompetitorShowingUpInstead || "",
        competitorDisplacement: Array.isArray(fields.competitorDisplacement)
          ? fields.competitorDisplacement
          : [],
        competitorDisplacementRank: fields.competitorDisplacementRank || null,
        likelyReason: fields.likelyReason || "",
        firstFix: fields.firstFix || "",
        secondFix: fields.secondFix || "",
        thirdFix: fields.thirdFix || "",
        fixes: Array.isArray(fields.fixes) ? fields.fixes : [],
        whoDoesTheWork: fields.whoDoesTheWork || null,
        recommendedNextStep: fields.recommendedNextStep || "",
        executiveSummary: fields.executiveSummary || null,
        executiveSignals: fields.executiveSignals || null,
        demandAreaToReview: fields.demandAreaToReview || null,
        inferredDemandLeak: fields.inferredDemandLeak || null,
        supportingEvidence: fields.supportingEvidence || null,
        evidenceLibrary: fields.evidenceLibrary || null,
        scenariosMonitored: fields.scenariosMonitored || null,
        howToRead: fields.howToRead || null,
        coverDisclaimer: fields.coverDisclaimer || null,
        coverMeta: fields.coverMeta || null,
        coverMetaLine: fields.coverMetaLine || fields.coverMeta?.coverMetaLine || null,
        researchMode: fields.researchMode || "leak_audit_lite",
        diagnosticScopeLabel: fields.diagnosticScopeLabel || null,
        scenarioSummaryLabel: fields.scenarioSummaryLabel || null,
        scopeDisclaimer: fields.scopeDisclaimer || null,
        hotelLocation: fields.hotelLocation || fields.locationLabel || "",
        locationLabel: fields.locationLabel || fields.hotelLocation || "",
        shareToken,
        reportUrl: fields.reportUrl || `/adp-leak-audit/share/${shareToken}`,
        pdfUrl: fields.pdfUrl || null,
        shareUrl: fields.shareUrl || `/adp-leak-audit/share/${shareToken}`,
        sentAt: fields.sentAt || null,
        hotelName: fields.hotelName || "",
        runDate: fields.runDate || null,
        providersUsed: fields.providersUsed || [],
        sampleFindingCount: fields.sampleFindingCount || null,
        createdAt: ts,
        updatedAt: ts,
      };
      if (!record.auditRunId) throw new Error("auditRunId_required");
      return save(COLLECTIONS.reports, record);
    },

    getReport(id) {
      return load(COLLECTIONS.reports, id);
    },

    getReportByShareToken(shareToken) {
      if (!shareToken) return null;
      const rows = list(COLLECTIONS.reports);
      return rows.find((r) => r.shareToken === shareToken) || null;
    },

    listReports(filters = {}) {
      let rows = list(COLLECTIONS.reports);
      if (filters.auditRunId) {
        rows = rows.filter((r) => r.auditRunId === filters.auditRunId);
      }
      if (filters.requestId) {
        rows = rows.filter((r) => r.requestId === filters.requestId);
      }
      if (filters.reportStatus) {
        rows = rows.filter((r) => r.reportStatus === filters.reportStatus);
      }
      rows.sort((a, b) => String(b.createdAt).localeCompare(String(a.createdAt)));
      return rows;
    },

    updateReport(id, patch) {
      return update(COLLECTIONS.reports, id, patch);
    },

    createHotel(fields) {
      const ts = nowIso();
      const record = {
        id: newLeakAuditId("leak_hotel"),
        requestId: fields.requestId || null,
        portfolioId: fields.portfolioId || null,
        hotelName: String(fields.hotelName || "").trim(),
        shortHotelName:
          String(fields.shortHotelName || "").trim() ||
          String(fields.hotelName || "")
            .trim()
            .split(/\s+/)
            .slice(0, 2)
            .join(" "),
        hotelWebsite: String(fields.hotelWebsite || "").trim(),
        city: String(fields.city || "").trim(),
        market: String(fields.market || "").trim(),
        country: String(fields.country || "").trim(),
        locationLabel: String(fields.locationLabel || "").trim(),
        matchedProductionHotelId: fields.matchedProductionHotelId || null,
        matchConfidence: fields.matchConfidence || "none",
        matchBasis: fields.matchBasis || "none",
        productionMatchReadOnly: true,
        createdAt: ts,
        updatedAt: ts,
      };
      if (!record.hotelName) throw new Error("hotelName_required");
      return save(COLLECTIONS.hotels, record);
    },

    getHotel(id) {
      return load(COLLECTIONS.hotels, id);
    },

    listHotels(filters = {}) {
      let rows = list(COLLECTIONS.hotels);
      if (filters.requestId) rows = rows.filter((r) => r.requestId === filters.requestId);
      if (filters.portfolioId) {
        rows = rows.filter((r) => r.portfolioId === filters.portfolioId);
      }
      return rows;
    },

    createMetric(fields) {
      const ts = nowIso();
      const record = {
        id: newLeakAuditId("leak_metric"),
        runId: fields.runId,
        hotelAuditId: fields.hotelAuditId || null,
        metricKey: fields.metricKey,
        metricLabel: fields.metricLabel || fields.metricKey,
        metricValue: fields.metricValue,
        metricUnit: fields.metricUnit || "",
        plainEnglishDefinition: fields.plainEnglishDefinition || "",
        infoTooltipText: fields.infoTooltipText || "",
        displayOrder: fields.displayOrder != null ? Number(fields.displayOrder) : 0,
        clientSafe: true,
        createdAt: ts,
      };
      if (!record.runId || !record.metricKey) throw new Error("metric_runId_and_key_required");
      return save(COLLECTIONS.metrics, record);
    },

    listMetrics(filters = {}) {
      let rows = list(COLLECTIONS.metrics);
      if (filters.runId) rows = rows.filter((r) => r.runId === filters.runId);
      rows.sort((a, b) => (a.displayOrder || 0) - (b.displayOrder || 0));
      return rows;
    },

    createCompetitor(fields) {
      const ts = nowIso();
      const record = {
        id: newLeakAuditId("leak_comp"),
        runId: fields.runId,
        hotelAuditId: fields.hotelAuditId || null,
        competitorName: fields.competitorName,
        appearedWhenSubjectAbsentCount: Number(fields.appearedWhenSubjectAbsentCount) || 0,
        demandArea: fields.demandArea || "",
        whatThisMayMean: fields.whatThisMayMean || "",
        rank: Number(fields.rank) || 0,
        clientSafe: true,
        createdAt: ts,
      };
      if (!record.runId || !record.competitorName) throw new Error("competitor_required");
      return save(COLLECTIONS.competitors, record);
    },

    listCompetitors(filters = {}) {
      let rows = list(COLLECTIONS.competitors);
      if (filters.runId) rows = rows.filter((r) => r.runId === filters.runId);
      rows.sort((a, b) => (a.rank || 0) - (b.rank || 0));
      return rows;
    },

    createEvidence(fields) {
      const ts = nowIso();
      const record = {
        id: fields.id || newLeakAuditId("leak_ev"),
        runId: fields.runId,
        hotelAuditId: fields.hotelAuditId || null,
        evidenceType: fields.evidenceType || "neutral",
        label: fields.label || "",
        headline: fields.headline || "",
        description: fields.description || "",
        provider: fields.provider || "",
        demandArea: fields.demandArea || "",
        responseExcerpt: String(fields.responseExcerpt || "").slice(0, 800),
        sourceLinks: Array.isArray(fields.sourceLinks) ? fields.sourceLinks : [],
        whyItMatters: fields.whyItMatters || "",
        managementReview: fields.managementReview || "",
        displayOrder: fields.displayOrder != null ? Number(fields.displayOrder) : 0,
        clientSafe: true,
        createdAt: ts,
      };
      if (!record.runId) throw new Error("evidence_runId_required");
      if ("fullPromptText" in fields || "promptText" in fields) {
        throw new Error("full_prompt_text_forbidden_in_evidence");
      }
      return save(COLLECTIONS.evidence, record);
    },

    listEvidence(filters = {}) {
      let rows = list(COLLECTIONS.evidence);
      if (filters.runId) rows = rows.filter((r) => r.runId === filters.runId);
      rows.sort((a, b) => (a.displayOrder || 0) - (b.displayOrder || 0));
      return rows;
    },

    createAction(fields) {
      const ts = nowIso();
      const record = {
        id: newLeakAuditId("leak_act"),
        runId: fields.runId,
        hotelAuditId: fields.hotelAuditId || null,
        actionNumber: Number(fields.actionNumber) || 0,
        actionTitle: fields.actionTitle || fields.title || "",
        whyItMatters: fields.whyItMatters || "",
        dealalityCanHelpPrepare: fields.dealalityCanHelpPrepare || fields.dealalityCanPrepare || [],
        hotelConfirms: fields.hotelConfirms || fields.hotelMustConfirm || [],
        sourcesNextCheck: fields.sourcesNextCheck || fields.targetSources || [],
        displayOrder: fields.displayOrder != null ? Number(fields.displayOrder) : 0,
        clientSafe: true,
        createdAt: ts,
      };
      if (!record.runId || !record.actionTitle) throw new Error("action_required");
      return save(COLLECTIONS.actions, record);
    },

    listActions(filters = {}) {
      let rows = list(COLLECTIONS.actions);
      if (filters.runId) rows = rows.filter((r) => r.runId === filters.runId);
      rows.sort((a, b) => (a.displayOrder || a.actionNumber || 0) - (b.displayOrder || b.actionNumber || 0));
      return rows;
    },

    createPortfolio(fields) {
      const ts = nowIso();
      const record = {
        id: newLeakAuditId("leak_port"),
        requestId: fields.requestId || null,
        portfolioName: String(fields.portfolioName || "").trim(),
        companyName: String(fields.companyName || "").trim(),
        contactName: String(fields.contactName || "").trim(),
        contactEmail: String(fields.contactEmail || "").trim(),
        audienceType: fields.audienceType || "portfolio_company",
        source: fields.source || "manual",
        status: fields.status || "requested",
        createdAt: ts,
        updatedAt: ts,
      };
      if (!record.portfolioName) throw new Error("portfolioName_required");
      return save(COLLECTIONS.portfolios, record);
    },

    getPortfolio(id) {
      return load(COLLECTIONS.portfolios, id);
    },

    listPortfolios() {
      return list(COLLECTIONS.portfolios).sort((a, b) =>
        String(b.createdAt).localeCompare(String(a.createdAt))
      );
    },

    createPortfolioHotel(fields) {
      const ts = nowIso();
      const record = {
        id: newLeakAuditId("leak_ph"),
        portfolioId: fields.portfolioId,
        hotelAuditId: fields.hotelAuditId || null,
        hotelName: fields.hotelName || "",
        hotelWebsite: fields.hotelWebsite || "",
        city: fields.city || "",
        country: fields.country || "",
        inclusionStatus: fields.inclusionStatus || "included",
        createdAt: ts,
        updatedAt: ts,
      };
      if (!record.portfolioId || !record.hotelName) throw new Error("portfolio_hotel_required");
      return save(COLLECTIONS.portfolioHotels, record);
    },

    listPortfolioHotels(filters = {}) {
      let rows = list(COLLECTIONS.portfolioHotels);
      if (filters.portfolioId) {
        rows = rows.filter((r) => r.portfolioId === filters.portfolioId);
      }
      return rows;
    },

    createPortfolioRun(fields) {
      const ts = nowIso();
      const record = {
        id: newLeakAuditId("leak_prun"),
        portfolioId: fields.portfolioId,
        requestId: fields.requestId || null,
        runDate: fields.runDate || ts.slice(0, 10),
        hotelCount: Number(fields.hotelCount) || 0,
        completedHotelRuns: Number(fields.completedHotelRuns) || 0,
        failedHotelRuns: Number(fields.failedHotelRuns) || 0,
        providersUsed: fields.providersUsed || [],
        promptSetVersion: fields.promptSetVersion || "leak_audit_prompt_set_v1",
        providerSetVersion: fields.providerSetVersion || "leak_audit_provider_set_v1",
        mostCommonAreaToReview: fields.mostCommonAreaToReview || "",
        mostFrequentCompetitor: fields.mostFrequentCompetitor || "",
        hotelsWithPriorityAreasToReview: Number(fields.hotelsWithPriorityAreasToReview) || 0,
        status: fields.status || "queued",
        errorLog: fields.errorLog || [],
        createdAt: ts,
        updatedAt: ts,
      };
      if (!record.portfolioId) throw new Error("portfolioId_required");
      return save(COLLECTIONS.portfolioRuns, record);
    },

    createPortfolioReport(fields) {
      const ts = nowIso();
      const shareToken =
        fields.shareToken || `share_${randomBytes(12).toString("hex")}`;
      const id = fields.id || newLeakAuditId("leak_prpt");
      const record = {
        id,
        portfolioId: fields.portfolioId,
        portfolioRunId: fields.portfolioRunId,
        reportStatus: fields.reportStatus || "draft",
        reportUrl: fields.reportUrl || `/adp-leak-audit/share/${shareToken}`,
        pdfUrl: fields.pdfUrl || null,
        shareToken,
        shareUrl: fields.shareUrl || `/adp-leak-audit/share/${shareToken}`,
        clientReport: fields.clientReport || null,
        portfolioSummary: fields.portfolioSummary || "",
        executiveSignal: fields.executiveSignal || null,
        hotelsNeedingAttention: fields.hotelsNeedingAttention || [],
        areasToReviewMostOften: fields.areasToReviewMostOften || [],
        competitorsShowingUpInstead: fields.competitorsShowingUpInstead || [],
        portfolioPattern: fields.portfolioPattern || "",
        firstPortfolioAction: fields.firstPortfolioAction || "",
        secondPortfolioAction: fields.secondPortfolioAction || "",
        thirdPortfolioAction: fields.thirdPortfolioAction || "",
        nextStepCta: fields.nextStepCta || "",
        createdAt: ts,
        updatedAt: ts,
        sentAt: fields.sentAt || null,
      };
      if (!record.portfolioId || !record.portfolioRunId) {
        throw new Error("portfolio_report_links_required");
      }
      return save(COLLECTIONS.portfolioReports, record);
    },

    getPortfolioReport(id) {
      return load(COLLECTIONS.portfolioReports, id);
    },

    getPortfolioReportByShareToken(shareToken) {
      if (!shareToken) return null;
      return (
        list(COLLECTIONS.portfolioReports).find((r) => r.shareToken === shareToken) ||
        null
      );
    },

    listPortfolioReports() {
      return list(COLLECTIONS.portfolioReports);
    },

    createPromotionStub(fields) {
      const ts = nowIso();
      const record = {
        id: newLeakAuditId("leak_promo"),
        auditRequestId: fields.auditRequestId,
        auditRunId: fields.auditRunId || null,
        reportId: fields.reportId || null,
        confirmed: Boolean(fields.confirmed),
        confirmedAt: fields.confirmed ? ts : null,
        confirmedBy: fields.confirmedBy || null,
        note:
          "Phase 1 stub only. Free audit records remain separate. No production ADP records created.",
        productionAdpRecordCreated: false,
        createdAt: ts,
        updatedAt: ts,
      };
      if (!record.confirmed) {
        throw new Error("promotion_requires_explicit_confirmation");
      }
      return save(COLLECTIONS.promotions, record);
    },

    listPromotions(filters = {}) {
      let rows = list(COLLECTIONS.promotions);
      if (filters.auditRequestId) {
        rows = rows.filter((r) => r.auditRequestId === filters.auditRequestId);
      }
      return rows;
    },
  };
}

export const defaultLeakAuditStore = createLeakAuditStore();
