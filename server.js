// server.js
import "./load-env.js";

// Fail fast if required Airtable env vars are missing (before importing Airtable-dependent routes)
const missing = [];
if (!process.env.AIRTABLE_API_KEY) missing.push("AIRTABLE_API_KEY");
if (!process.env.AIRTABLE_BASE_ID) missing.push("AIRTABLE_BASE_ID");
if (missing.length > 0) {
  console.error("MISSING ENV:", missing.join(", "));
  console.error("Create .env.local in the repo root and add:");
  console.error("AIRTABLE_API_KEY=...");
  console.error("AIRTABLE_BASE_ID=...");
  process.exit(1);
}

// DEV: self-identify server file and working dir; guard against wrong folder
const serverPath = new URL(import.meta.url).pathname;
const cwd = process.cwd();
console.log("Using server file:", serverPath);
console.log("Working directory:", cwd);
if (process.env.NODE_ENV !== "production") {
  if (!cwd.endsWith("deal-capture-proxy")) {
    console.error("ERROR: Server started from wrong directory:", cwd);
    console.error("Please run from: C:\\Users\\joand\\OneDrive\\Documents\\deal-capture-proxy");
    console.error("Stopping.");
    process.exit(1);
  }
}

import express from "express";
import path from "path";
import fs from "fs";
import multer from "multer";
import { fileURLToPath } from "url";
import dealIntake from "./api/intake-deal.js";
import userIntake from "./api/intake-user.js";
import {
  listMarketAlerts,
  getMarketAlertsRail,
  markAlertRead,
  saveAlert,
  dismissAlert,
} from "./api/market-alerts.js";
import { getMarketAlertsNews } from "./api/market-alerts-news.js";
import { cronMarketAlertsRssSync } from "./api/run-market-alerts-rss-sync.js";
import { startMarketAlertsRssScheduler } from "./api/market-alerts-rss-scheduler.js";
import { analyzeDeal } from "./api/deal-intelligence.js";
import { getBrandPresence, getBrandPresenceHotelById, getBrandStatistics, getWhiteSpaceOpportunities, exportBrandPresenceData, getLocationTypes, getParentCompanies, getBrands, getChainScales } from "./api/brand-presence.js";
import { getLargestOperatorsByBrandRegion, getOperatorsByBrandRegionFilters } from "./api/operators-by-brand-region.js";
import { getTravelInfrastructure, getRadarMapTravelInfrastructurePoints, postTravelInfrastructureImportPreview, postTravelInfrastructureImportCommit } from "./api/travel-infrastructure.js";
import { getDemandAnchors, getRadarMapDemandAnchorsPoints, postDemandAnchorsImportPreview, postDemandAnchorsImportCommit } from "./api/demand-anchors.js";
import { getRadarBuildoutCountries, getRadarBuildoutCountry } from "./api/radar-buildout.js";
import { getAiDemandPositioningProperties, getAiDemandPositioningReport, getAiDemandPositioningCostEstimate, getAiDemandPositioningEvidence, getAiDemandPositioningReadHealth } from "./api/ai-demand-positioning.js";
import { logAdpPublishedReadSourceAtStartup } from "./lib/ai-demand-positioning/published-read-service.js";
import {
  getGrowthSignalsSummary,
  getGrowthSignalTypes,
  getGrowthSignalsCountries,
  getGrowthSignalsCountryDetail,
  getGrowthSignalsSubmarket,
  getGrowthSignalsFlat,
} from "./api/growth-signals.js";
import { getDealalityScout, getDealalityScoutFilters } from "./api/dealality-scout.js";
import { getScoutMarketCoverage } from "./api/scout-market-coverage.js";
import { getScoutOpportunitySignals } from "./api/scout-opportunity-signals.js";
import {
  postScoutOpportunitySignalSave,
  getScoutOpportunitySignalsSaved,
  patchScoutOpportunitySignal,
} from "./api/scout-opportunity-signals-watchlist.js";
import { getScoutMarketMap } from "./api/scout-market-map.js";
import { getScoutDemandOverlays } from "./api/scout-demand-overlays.js";
import { getScoutMarketInsights } from "./api/scout-market-insights.js";
import { getScoutInsightReview } from "./api/scout-insight-review.js";
import { getBrandReviewDeals, updateDealStatus, getDealDetails, bulkUpdateDeals, getBrandReviewStats, getMatchedBrands } from "./api/brand-review.js";
import { analyzeBrandFit, getDealBrandFit, getAllDealsForAnalysis } from "./api/brand-fit-analyzer.js";
import { getClauses, getClauseById, getClauseVariables, getClauseIds, createClause } from "./api/clause-library.js";
import { getTerms, getTermById, getTermIds, createTerm } from "./api/financial-term-library.js";
import {
  getBrandLibraryBrands,
  getBrandLibraryBrandById,
  getBrandStatusOptions,
  getBrandsGroupedByParentCompany,
  updateBrandBasicsById,
  updateSustainabilityEsgByBrandId,
  updateBrandFootprintByBrandId,
  updateLoyaltyCommercialByBrandId,
  updateProjectFitByBrandId,
  updatePortfolioPerformanceByBrandId,
  updateBrandStandardsByBrandId,
  updateFeeStructureByBrandId,
  updateDealTermsByBrandId,
  updateOperationalSupportByBrandId,
  updateLegalTermsByBrandId,
  getOperationalSupportByBrandId
} from "./api/brand-library.js";
import submitThirdPartyOperator from "./api/third-party-operator-intake.js";
import listThirdPartyOperators from "./api/third-party-operators-list.js";
import getThirdPartyOperatorDetail from "./api/third-party-operator-detail.js";
import getOperatorCensusFootprint from "./api/operator-census-footprint.js";
import getThirdPartyOperatorMappingReport from "./api/third-party-operator-mapping-report.js";
import getThirdPartyOperatorPrefillQa from "./api/third-party-operator-prefill-qa.js";
import updateThirdPartyOperatorStatus from "./api/third-party-operator-status.js";
import signup from "./api/signup.js";
import signupConfig from "./api/signup-config.js";
import signupTermsAcceptance from "./api/signup-terms-acceptance.js";
import memberstackWebhook from "./api/memberstack-webhook.js";
import { getPartners, createUser, updateUser } from "./api/partner-directory.js";
import { getUserFavorites, createFavorite, deleteFavorite, updateFavorite } from "./api/partner-directory-favorites.js";
import {
  getBrandExplorerFavorites,
  createBrandExplorerFavorite,
  deleteBrandExplorerFavorite,
} from "./api/brand-explorer-favorites.js";
import {
  getOperatorExplorerFavorites,
  createOperatorExplorerFavorite,
  deleteOperatorExplorerFavorite,
} from "./api/operator-explorer-favorites.js";
import {
  getCapitalExplorerFavorites,
  createCapitalExplorerFavorite,
  deleteCapitalExplorerFavorite,
} from "./api/capital-explorer-favorites.js";
import {
  createCompanyProfile,
  updateCompanyProfile,
  getCompanyProfilePrefill,
  getMyCompanyProfilePrefill,
} from "./api/company-profile.js";
import {
  listUsers as listUserManagementUsers,
  createUser as createUserManagementUser,
  updateUser as updateUserManagementUser,
  deleteUser as deleteUserManagementUser,
  bulkDeleteUsers,
  listCompanies as listUserManagementCompanies,
} from "./api/user-management.js";
import { getMyDeals, postMyDealsInitialMatchedSupport, getDealById, updateMyDealById, createDeal, addRecommendedBrand, getAlternativeBrands, getMatchScoreBreakdown, getOperatorMatchScoreBreakdown, refreshDealBrandCache, uploadDealAttachments, ALLOWED_ATTACHMENT_EXTENSIONS, MAX_ATTACHMENT_FILE_SIZE_BYTES } from "./api/my-deals.js";
import { getDealReadinessMeta, postDealReadinessReview, postDealReadinessSave } from "./api/deal-readiness-review.js";
import { postBrandAlignmentSnapshot } from "./api/brand-alignment-snapshot.js";
import {
  getCommercialReadinessSnapshot,
  postCommercialReadinessGenerate,
  postCommercialReadinessGenerateStandalone,
  postCommercialReadinessSaveInputs,
} from "./api/commercial-readiness-snapshot.js";
import {
  getConversionFinancingPackage,
  getHotelCapitalOpportunity,
  patchConversionFinancingSharing,
  postConversionFinancingGenerate,
  postConversionFinancingSaveInputs,
} from "./api/conversion-financing-package.js";
import {
  getOperatorCapabilitySnapshot,
  postOperatorCapabilitySnapshot,
} from "./api/operator-capability-snapshot.js";
import {
  getDealDemandCenters,
  getDealNearbyHotelSupply,
  getDealMarketDemandSnapshot,
  postGenerateMarketDemandSnapshot,
  postImportDemandCenters,
  postPreviewDemandCenterImport,
} from "./api/market-demand.js";
import {
  getOperatorAlignmentSnapshotProfile,
  getOperatorAlignmentSnapshotCompanies,
} from "./api/operator-alignment-snapshot.js";
import {
  getOperatorFitV2Flag,
  getOperatorFitV2Top5,
} from "./api/operator-fit-v2.js";
import {
  getOperatorFitInternalPilotAccess,
  getOperatorFitInternalPilotPayload,
  postOperatorFitInternalPilotEvent,
  getOperatorFitInternalPilotShortlist,
  postOperatorFitInternalPilotShortlist,
  postOperatorFitInternalPilotShortlistRemove,
  postOperatorFitInternalPilotShortlistStatus,
  postOperatorFitInternalPilotCompare,
  postOperatorFitInternalPilotRankingDifference,
  postOperatorFitInternalPilotRankingChangeValidations,
  getOperatorFitInternalPilotAdvisorScorecards,
  postOperatorFitInternalPilotAdvisorScorecard,
} from "./api/operator-fit-internal-pilot.js";
import { getOutreachSetup, updateOutreachSetup, getOutreachDefault, updateOutreachDefault, deleteOutreachSetup } from "./api/outreach-setup.js";
import { getFranchiseApplication, updateFranchiseApplication } from "./api/franchise-application.js";
import { list as outreachHubList, get as outreachHubGet, create as outreachHubCreate, update as outreachHubUpdate, remove as outreachHubRemove } from "./api/outreach-hub.js";
import { getOutreachDealActivityLog } from "./api/outreach-deal-activity-log.js";
import { getDashboardHome } from "./api/dashboard-home.js";
import { getMarketingDemoEmbeds } from "./api/marketing-demo-embeds.js";
import marketingBetaNotify from "./api/marketing-beta-notify.js";
import marketingOpportunityReview from "./api/marketing-opportunity-review.js";
import marketingLandingEvents from "./api/marketing-landing-events.js";
import marketingLandingConfig from "./api/marketing-landing-config.js";
import {
  getMarketingLandingEventsReport,
} from "./api/marketing-landing-events-report.js";
import { getMarketingLandingEventsSession } from "./api/marketing-landing-events-session.js";
import { getMarketingLandingEventsExport } from "./api/marketing-landing-events-export.js";
import { landingAnalyticsReportAuth } from "./middleware/landingAnalyticsReportAuth.js";
import { getTargetList, addToTargetList, updateTarget, removeFromTargetList, batchRemoveFromTargetList, markAsDeleted, restoreFromDeleted } from "./api/target-list.js";
import { createRequest as createBrandDealRequest, listForBrand as listBrandDealRequests, listAll as listBrandDealRequestsAll, listForDealRoom as listBrandDealRequestsForDealRoom, listForDeals as listBrandDealRequestsByDeals, listForDealsPost as listBrandDealRequestsByDealsPost, updateStatus as updateBrandDealRequestStatus, bulkUpdateStatus as bulkUpdateBrandDealRequestStatus, getActivityLog as getBrandDealActivityLog, getDealMetaBatch as getBrandDealMetaBatch, getProposalDraft, submitProposal, getById as getBrandDealRequestById } from "./api/brand-deal-requests.js";
import { getBrandWorkspaceKpiHistory, postBrandWorkspaceKpiSnapshot } from "./api/brand-workspace-kpi-history.js";
import {
  listOperatorDealRequests,
  getOperatorDealRequestById,
  getOperatorDealActivity,
  getOperatorDealMetaBatch,
  updateOperatorDealRequest,
  bulkUpdateOperatorDealRequests,
} from "./api/operator-deal-requests.js";
import {
  list as listDealRoomDocuments,
  listForBrandRequest as listDealRoomDocumentsForBrandRequest,
  create as createDealRoomDocument,
  update as updateDealRoomDocument,
  remove as deleteDealRoomDocument,
  uploadFile as uploadDealRoomDocumentFile,
  serveFile as serveDealRoomDocumentFile,
  DEAL_ROOM_DOCS_UPLOAD_DIR,
} from "./api/deal-room-documents.js";
import { getProposalsForDeal } from "./api/deal-compare.js";
import { listBrands as listBrandExplorerBrands, getBrand as getBrandExplorerBrand, fitToDeal as brandExplorerFitToDeal } from "./api/brand-explorer.js";
import { listOperators, getOperatorById } from "./api/operator-explorer.js";
import {
  listCapitalProviders,
  getCapitalProviderById,
  handleCapitalProviderExplorer,
} from "./api/capital-provider-explorer.js";
import { optionalDealalityAuth } from "./middleware/optionalDealalityAuth.js";
import {
  createMyDealsOperatorRequest,
  listMyDealsOperatorRequestsByDeals,
} from "./api/my-deals-operator-requests.js";
import { getMe } from "./api/me.js";
import { getAuthMe } from "./api/auth-me.js";
import { getMemberstackPublicConfig } from "./api/auth-memberstack-config.js";
import { getOwnerPilotProvisioningRunbookHandler } from "./api/support-owner-pilot-provisioning-runbook.js";
import { getScoringWeightModelHandler } from "./api/support-scoring-weight-model.js";
import { getAiVisibilityBenchmarkAdminHandler } from "./api/support-ai-visibility-benchmark-admin.js";
import { getOperatorFitDataReadinessHandler } from "./api/support-operator-fit-data-readiness.js";
import { getOperatorIntelligenceCalibrationHandler } from "./api/support-operator-intelligence-calibration.js";
import {
  getOperatorMatchScoringConfigHandler,
  getOperatorMatchScoringConfigBrowserScript,
} from "./api/operator-match-scoring-config.js";
import {
  dealalityChatgptAuth,
  postCreateRecordsByTableId,
  postGetRecordById,
  postListDealalityTables,
  postListRecordsByTableId,
  postSummarizeRecordsByTableId,
  postUpdateRecordByTableId,
  postUpdateRecordsByTableId,
} from "./api/dealality-airtable-chatgpt.js";
import { memberstackAuth } from "./middleware/memberstackAuth.js";
import { requireDealalityUser } from "./middleware/requireDealalityUser.js";
import { requireMyDealsAccess } from "./middleware/requireMyDealsAccess.js";
import { requireOperatorDealsAccess } from "./middleware/requireOperatorDealsAccess.js";
import { requireBrandAiVisibilityAccess } from "./middleware/requireBrandAiVisibilityAccess.js";
import { requireOperatorAiVisibilityAccess } from "./middleware/requireOperatorAiVisibilityAccess.js";
import { requireAiIntelligenceValidationAccess } from "./middleware/requireAiIntelligenceValidationAccess.js";
import { assertBrandAiVisibilityRoutesRegistered } from "./lib/ai-visibility/route-registration-guard.js";
import {
  getBrandPortfolio as getAiVisibilityBrandPortfolio,
  getBrandExecutiveSummary as getAiVisibilityBrandExecutiveSummary,
  getBrandOverview as getAiVisibilityBrandOverview,
  getBrandTrend as getAiVisibilityBrandTrend,
  getBrandQuestions as getAiVisibilityBrandQuestions,
  getBrandCompetitors as getAiVisibilityBrandCompetitors,
  getBrandSources as getAiVisibilityBrandSources,
  getBrandEvidence as getAiVisibilityBrandEvidence,
  getAiVisibilityBrandBenchmark,
  getAiVisibilityBrandBenchmarkDiagnostics,
} from "./api/ai-visibility-brand.js";
import { getOperatorAiFoundation, getOperatorAiCustomerUniverse, getOperatorAiCustomerPayload } from "./api/ai-visibility-operator.js";
import {
  getAiIntelligenceValidationSummary,
  getAiIntelligenceValidationGates,
  getAiIntelligenceValidationClassification,
  getAiIntelligenceValidationBatches,
  getAiIntelligenceValidationIssues,
  getAiIntelligenceValidationVariability,
  getAiIntelligenceValidationOperations,
  getAiIntelligenceValidationBatchDetail,
} from "./api/ai-intelligence-validation.js";
import {
  getGoldenSetReviewQueue,
  getGoldenSetReviewProgress,
  getGoldenSetReviewCase,
  postGoldenSetReviewCase,
  postGoldenSetPromote,
  getGoldenSetReviewPacket,
  postGoldenSetReviewPacketsBatch,
  getGoldenSetReviewLearning,
  getGoldenSetReviewExport,
  getGoldenSetReviewExportAll,
  getGoldenSetReviewExportFiltered,
  getGoldenSetReviewExportPackets,
  getGoldenSetReviewAssistanceTemplate,
  postGoldenSetReviewImportPreview,
  postGoldenSetReviewImportApply,
  postGoldenSetReviewAcceptAssisted,
  postGoldenSetReviewDiffPreview,
} from "./api/ai-intelligence-golden-set-review.js";
import {
  getTaxonomyReviewReady,
  getTaxonomyReviewValidate,
  getTaxonomyReviewQueue,
  postTaxonomyReviewDecision,
  postTaxonomyReviewAcceptAllProposals,
  postTaxonomyReviewPreviewApply,
  postTaxonomyReviewApply,
} from "./api/ai-intelligence-recommendation-taxonomy-review.js";
import {
  getPresenceValidationReviewQueue,
  postPresenceValidationReviewDecision,
  getPresenceValidationReviewSummary,
  getPresenceValidationReviewExport,
  getPresenceValidationReviewExportPreview,
  postPresenceValidationAssistedImport,
  getPresenceValidationAssistedProposals,
  getPresenceValidationBulkApprovalPreview,
  postPresenceValidationBulkApproval,
} from "./api/ai-intelligence-presence-validation-review.js";
import { requireOwnerOdrCreateAccess } from "./middleware/requireOwnerOdrCreateAccess.js";
import { requirePartnerIntelligenceAccess } from "./middleware/requirePartnerIntelligenceAccess.js";
import {
  getPartnerIntelligencePilot,
  listPartnerIntelligenceSources,
  getPartnerIntelligenceSourceById,
  createPartnerIntelligenceSource,
  patchPartnerIntelligenceSource,
  uploadPartnerIntelligenceSourceFile,
  resolvePartnerIntelligenceUploadDir,
} from "./api/partner-intelligence-sources.js";
import {
  postPartnerIntelligenceExtractionRun,
  getPartnerIntelligenceExtractionContext,
  listPartnerIntelligenceFacts,
  getPartnerIntelligenceFactById,
  patchPartnerIntelligenceFactReview,
  postPartnerIntelligencePublish,
} from "./api/partner-intelligence-workflow.js";
import {
  PARTNER_SOURCE_ALLOWED_UPLOAD_EXT,
  PARTNER_SOURCE_MAX_UPLOAD_BYTES,
} from "./lib/partner-intelligence/validate-source-payload.js";
import { requireDealRecordAccess } from "./middleware/requireDealRecordAccess.js";
import { mapBodyDealIdToRecordId } from "./middleware/mapBodyDealIdToRecordId.js";
import { mapParamDealIdToRecordId } from "./middleware/mapParamDealIdToRecordId.js";
import { requireAdminAccess } from "./middleware/requireAdminAccess.js";
import { requireInternalRunbookAdmin } from "./middleware/requireInternalRunbookAdmin.js";
import {
  postAcquisitionConnectionsPreview,
  postAcquisitionConnectionsImport,
  getAcquisitionImportBatches,
  getAcquisitionSummary,
  postAcquisitionClassify,
  getAcquisitionRelationships,
} from "./api/acquisition-intelligence.js";
import { requireTargetListRecordAccess } from "./middleware/requireTargetListRecordAccess.js";
import { requireOwnerBdrRecordAccess } from "./middleware/requireOwnerBdrRecordAccess.js";
import {
  gateOwnerBdrActivity,
  gateOwnerBdrDealMeta,
  gateOwnerBdrDealIdsQuery,
  gateOwnerBdrListAll,
} from "./middleware/gateOwnerBdrRoutes.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Keep process alive on unhandled rejection (e.g. in GET /api/my-deals) so server does not exit and client gets a proper error on retry
process.on("unhandledRejection", (reason, promise) => {
  console.error("Unhandled Rejection:", reason);
  if (reason && typeof reason === "object" && reason.stack) console.error(reason.stack);
});

// Uploads directory for Company Settings logo (public so /uploads/* is served by express.static)
const UPLOADS_DIR = path.join(__dirname, "public", "uploads");
if (!fs.existsSync(UPLOADS_DIR)) {
  fs.mkdirSync(UPLOADS_DIR, { recursive: true });
}

// Deal Setup attachments: stored outside public, served via GET /api/my-deals/:recordId/attachments/:filename
const DEAL_ATTACHMENTS_DIR = path.join(__dirname, "uploads", "deal-attachments");
if (!fs.existsSync(DEAL_ATTACHMENTS_DIR)) {
  fs.mkdirSync(DEAL_ATTACHMENTS_DIR, { recursive: true });
}

if (!fs.existsSync(DEAL_ROOM_DOCS_UPLOAD_DIR)) {
  fs.mkdirSync(DEAL_ROOM_DOCS_UPLOAD_DIR, { recursive: true });
}

const companyProfileUpload = multer({
  storage: multer.diskStorage({
    destination: (_req, _file, cb) => cb(null, UPLOADS_DIR),
    filename: (_req, file, cb) => {
      const safe = (file.originalname || "logo").replace(/[^a-zA-Z0-9.-]/g, "_");
      cb(null, `${Date.now()}-${safe}`);
    },
  }),
  limits: { fileSize: 5 * 1024 * 1024 }, // 5 MB
});

/** Optional company logo on Third Party Operator intake (same public /uploads/ URLs as company profile). */
const operatorIntakeUpload = multer({
  storage: multer.diskStorage({
    destination: (_req, _file, cb) => cb(null, UPLOADS_DIR),
    filename: (_req, file, cb) => {
      const safe = (file.originalname || "logo").replace(/[^a-zA-Z0-9.-]/g, "_");
      cb(null, `tpo-${Date.now()}-${safe}`);
    },
  }),
  limits: { fileSize: 5 * 1024 * 1024 },
  fileFilter: (_req, file, cb) => {
    const mimeOk = /^image\/(png|jpe?g)$/i.test(file.mimetype || "");
    const nameOk = /\.(png|jpe?g)$/i.test(file.originalname || "");
    if (mimeOk || nameOk) return cb(null, true);
    cb(new Error("Logo must be PNG or JPEG"));
  },
});

function handleThirdPartyOperatorIntake(req, res) {
  const ct = req.headers["content-type"] || "";
  if (ct.includes("multipart/form-data")) {
    return operatorIntakeUpload.single("companyLogo")(req, res, (err) => {
      if (err) {
        if (err.code === "LIMIT_FILE_SIZE") {
          return res.status(413).json({ error: "Logo file too large (maximum 5 MB)." });
        }
        return res.status(400).json({ error: err.message || "Upload failed" });
      }
      try {
        const raw = req.body && req.body.payload;
        if (typeof raw !== "string" || !raw.length) {
          return res.status(400).json({ error: "Missing multipart field: payload (JSON string)." });
        }
        req.body = JSON.parse(raw);
      } catch {
        return res.status(400).json({ error: "Invalid intake payload JSON" });
      }
      return submitThirdPartyOperator(req, res);
    });
  }
  return submitThirdPartyOperator(req, res);
}

const dealAttachmentsUpload = multer({
  storage: multer.diskStorage({
    destination: (req, _file, cb) => {
      const dir = path.join(DEAL_ATTACHMENTS_DIR, req.params.recordId || "unknown");
      if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
      cb(null, dir);
    },
    filename: (_req, file, cb) => {
      const base = (file.originalname || "file").replace(/[^a-zA-Z0-9.-]/g, "_");
      cb(null, `${Date.now()}-${base}`);
    },
  }),
  limits: { fileSize: MAX_ATTACHMENT_FILE_SIZE_BYTES },
  fileFilter: (req, file, cb) => {
    const ext = path.extname(file.originalname || "").toLowerCase();
    if (ALLOWED_ATTACHMENT_EXTENSIONS.includes(ext)) {
      cb(null, true);
    } else {
      cb(new Error("File type not allowed. Allowed: " + ALLOWED_ATTACHMENT_EXTENSIONS.join(", ")), false);
    }
  },
});

const dealRoomDocsUpload = multer({
  storage: multer.diskStorage({
    destination: (req, _file, cb) => {
      const dir = path.join(DEAL_ROOM_DOCS_UPLOAD_DIR, req.params.dealId || "unknown");
      if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
      cb(null, dir);
    },
    filename: (_req, file, cb) => {
      const base = (file.originalname || "file").replace(/[^a-zA-Z0-9.-]/g, "_");
      cb(null, `${Date.now()}-${base}`);
    },
  }),
  limits: { fileSize: MAX_ATTACHMENT_FILE_SIZE_BYTES },
  fileFilter: (_req, file, cb) => {
    const ext = path.extname(file.originalname || "").toLowerCase();
    if (ALLOWED_ATTACHMENT_EXTENSIONS.includes(ext)) {
      cb(null, true);
    } else {
      cb(new Error("File type not allowed. Allowed: " + ALLOWED_ATTACHMENT_EXTENSIONS.join(", ")), false);
    }
  },
});

/** Acquisition Intelligence — LinkedIn Connections CSV (memory; admin-only routes). */
const acquisitionConnectionsUpload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 15 * 1024 * 1024 }, // 15 MB
  fileFilter: (_req, file, cb) => {
    const name = String(file.originalname || "").toLowerCase();
    const mime = String(file.mimetype || "").toLowerCase();
    if (name.endsWith(".csv") || mime.includes("csv") || mime === "text/plain" || mime === "application/vnd.ms-excel") {
      return cb(null, true);
    }
    cb(new Error("Only LinkedIn Connections CSV files are accepted."), false);
  },
});

const partnerSourceUpload = multer({
  storage: multer.diskStorage({
    destination: (req, _file, cb) => {
      const dir =
        req.partnerIntelligenceUploadDir ||
        path.join(__dirname, "data", "partner-sources", "inbox");
      if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
      cb(null, dir);
    },
    filename: (_req, file, cb) => {
      const base = (file.originalname || "source").replace(/[^a-zA-Z0-9._-]/g, "_");
      cb(null, `${Date.now()}-${base}`);
    },
  }),
  limits: { fileSize: PARTNER_SOURCE_MAX_UPLOAD_BYTES },
  fileFilter: (_req, file, cb) => {
    const ext = path.extname(file.originalname || "").toLowerCase();
    if (PARTNER_SOURCE_ALLOWED_UPLOAD_EXT.includes(ext)) {
      cb(null, true);
    } else {
      cb(
        new Error(
          "File type not allowed. Allowed: " + PARTNER_SOURCE_ALLOWED_UPLOAD_EXT.join(", ")
        ),
        false
      );
    }
  },
});

function parseCompanyProfileArrays(req, res, next) {
  if (req.body.regionsJson) {
    try { req.body.regions = JSON.parse(req.body.regionsJson); } catch (_) {}
    delete req.body.regionsJson;
  }
  if (req.body.primaryServicesJson) {
    try { req.body.primaryServices = JSON.parse(req.body.primaryServicesJson); } catch (_) {}
    delete req.body.primaryServicesJson;
  }
  if (req.body.additionalServicesJson) {
    try { req.body.additionalServices = JSON.parse(req.body.additionalServicesJson); } catch (_) {}
    delete req.body.additionalServicesJson;
  }
  if (req.body.companyCapabilitiesJson) {
    try {
      req.body.companyCapabilities = JSON.parse(req.body.companyCapabilitiesJson);
    } catch (_) {}
    delete req.body.companyCapabilitiesJson;
  }
  if (req.body.companyTypeTagsJson) {
    try { req.body.companyTypeTags = JSON.parse(req.body.companyTypeTagsJson); } catch (_) {}
    delete req.body.companyTypeTagsJson;
  }
  if (req.body.workspaceAccessJson) {
    try { req.body.workspaceAccess = JSON.parse(req.body.workspaceAccessJson); } catch (_) {}
    delete req.body.workspaceAccessJson;
  }
  if (req.body.potentialConflictFlagsJson) {
    try {
      req.body.potentialConflictFlags = JSON.parse(req.body.potentialConflictFlagsJson);
    } catch (_) {}
    delete req.body.potentialConflictFlagsJson;
  }
  // Form must not send Airtable column names on req.body (only mapped in api/company-profile.js).
  if (req.body && Object.prototype.hasOwnProperty.call(req.body, "Company Type")) {
    delete req.body["Company Type"];
  }
  next();
}

const app = express();
const PRODUCTION_CANONICAL_HOSTS = new Set(["dealality.com", "www.dealality.com"]);

function requestHostname(req) {
  const host = String(req.hostname || req.headers.host || "")
    .toLowerCase()
    .trim();
  return host.replace(/:\d+$/, "");
}

const DEFAULT_EMBED_ANCESTORS = [
  "https://www.dealality.com",
  "https://dealality.com",
  "https://deal-capture.design.webflow.com",
  "https://mvp-deal-capture.webflow.io",
  "https://*.webflow.io",
  "https://*.webflow.com",
  "http://localhost:*",
  "http://127.0.0.1:*",
];
const EMBED_ALLOWED_ANCESTORS = [
  ...new Set([
    ...DEFAULT_EMBED_ANCESTORS,
    ...(process.env.FRAME_ANCESTORS || "")
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean),
  ]),
];

function isEmbeddableShellRequest(req) {
  const p = req.path || "";
  if (p === "/app" || p.startsWith("/app/")) return true;
  return (
    p === "/operator-explorer-gold-mock.html" ||
    p === "/operator-explorer-gold-mock" ||
    p === "/operator-explorer-gold-mock/" ||
    p === "/brand-explorer-gold-mock.html" ||
    p === "/brand-explorer-gold-mock" ||
    p === "/brand-explorer-gold-mock/"
  );
}

function applyEmbedFramePolicy(res) {
  res.removeHeader("X-Frame-Options");
  res.setHeader(
    "Content-Security-Policy",
    "frame-ancestors 'self' " + EMBED_ALLOWED_ANCESTORS.join(" ") + ";"
  );
}

// CORS so Webflow + live origins can call API from browser.
// Env config is additive, so core production origins stay allowed.
const DEFAULT_CORS_ALLOWED_ORIGINS = [
  "https://mvp-deal-capture.webflow.io",
  "https://dealality.com",
  "https://www.dealality.com",
];
const configuredCorsOrigins = (process.env.CORS_ORIGINS || process.env.CORS_ORIGIN || "")
  .split(",")
  .map((o) => o.trim())
  .filter(Boolean);
const CORS_ALLOWED_ORIGINS = [...new Set([...DEFAULT_CORS_ALLOWED_ORIGINS, ...configuredCorsOrigins])];

app.use((req, res, next) => {
  const origin = req.headers.origin;
  if (origin && CORS_ALLOWED_ORIGINS.includes(origin)) {
    res.setHeader("Access-Control-Allow-Origin", origin);
    res.setHeader("Access-Control-Allow-Credentials", "true");
    res.setHeader("Vary", "Origin");
  }
  res.setHeader("Access-Control-Allow-Methods", "GET, PATCH, POST, PUT, DELETE, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  if (req.method === "OPTIONS") return res.sendStatus(204);
  next();
});

// Security headers for deployment
app.use((req, res, next) => {
  res.setHeader("X-Content-Type-Options", "nosniff");
  const host = requestHostname(req);
  if (!PRODUCTION_CANONICAL_HOSTS.has(host)) {
    res.setHeader("X-Robots-Tag", "noindex, nofollow");
  }
  const isEmbedRequest = req.query && String(req.query.embed || "") === "1";
  if (isEmbeddableShellRequest(req) || isEmbedRequest) {
    applyEmbedFramePolicy(res);
  } else {
    res.setHeader("X-Frame-Options", "SAMEORIGIN");
  }
  res.setHeader("X-XSS-Protection", "1; mode=block");
  res.setHeader("Referrer-Policy", "strict-origin-when-cross-origin");
  next();
});

// Company Profile must run BEFORE body parsers: multipart body is only for multer; if json() runs first it can leave req.body empty
app.post(
  "/api/company-profile",
  companyProfileUpload.single("companyLogo"),
  parseCompanyProfileArrays,
  createCompanyProfile
);
app.patch(
  "/api/company-profile/:recordId",
  companyProfileUpload.single("companyLogo"),
  parseCompanyProfileArrays,
  updateCompanyProfile
);
app.get("/api/company-profile/prefill", getCompanyProfilePrefill);

// Golden Set assisted/human import payloads can exceed Express default 100kb (~330 cases).
app.use(
  "/api/ai-intelligence/golden-set-review/import",
  express.json({ limit: "5mb" })
);
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Memberstack + Airtable user context (Phase A)
app.get("/api/me", getMe);
app.post("/api/me", getMe);

// Memberstack auth + Dealality user (MVP)
app.get("/api/auth/me", memberstackAuth, requireDealalityUser, getAuthMe);
app.get(
  "/api/company-profile/mine",
  memberstackAuth,
  requireDealalityUser,
  getMyCompanyProfilePrefill
);
app.get("/api/auth/memberstack-config", getMemberstackPublicConfig);

app.post("/api/intake/third-party-operator", handleThirdPartyOperatorIntake);
app.post("/api/third-party-operators/submit", handleThirdPartyOperatorIntake);
app.get("/api/intake/third-party-operator/mapping-report", getThirdPartyOperatorMappingReport);
app.get("/api/intake/third-party-operator/prefill-qa", getThirdPartyOperatorPrefillQa);
// Operator list for My 3rd Party Ops. dashboard (multiple paths for proxies / older clients)
app.get("/api/intake/third-party-operators/:recordId/census-footprint", getOperatorCensusFootprint);
app.get("/api/intake/third-party-operators/:recordId", getThirdPartyOperatorDetail);
app.get("/api/intake/third-party-operators", listThirdPartyOperators);
app.get("/api/third-party-operators/list", listThirdPartyOperators);
app.get("/api/third-party-operators", listThirdPartyOperators);
// Aliases for My Operators (new) clients / docs
app.get("/api/third-party-operators-new/list", listThirdPartyOperators);
app.get("/api/third-party-operators-new", listThirdPartyOperators);
app.patch("/api/intake/third-party-operators/:recordId/status", updateThirdPartyOperatorStatus);

// Marketing — public live iframe URL manifest (Webflow embeds)
app.get("/api/marketing/demo-embeds", getMarketingDemoEmbeds);
app.get("/api/marketing/landing-config", marketingLandingConfig);
app.post("/api/marketing/beta-notify", marketingBetaNotify);
app.post("/api/marketing/opportunity-review", marketingOpportunityReview);
app.post("/api/marketing/landing-events", marketingLandingEvents);
app.get(
  "/api/marketing/landing-events/report",
  ...landingAnalyticsReportAuth,
  getMarketingLandingEventsReport
);
app.get(
  "/api/marketing/landing-events/session",
  ...landingAnalyticsReportAuth,
  getMarketingLandingEventsSession
);
app.get(
  "/api/marketing/landing-events/export",
  ...landingAnalyticsReportAuth,
  getMarketingLandingEventsExport
);

// My Deals API (more specific routes first so /outreach-default and /outreach-setup are not treated as recordId)
const myDealsAuth = [memberstackAuth, requireDealalityUser, requireMyDealsAccess];
const partnerIntelligenceAuth = [memberstackAuth, requireDealalityUser, requirePartnerIntelligenceAccess];
const myDealsDealAuth = [...myDealsAuth, requireDealRecordAccess];
const adminAuth = [memberstackAuth, requireDealalityUser, requireAdminAccess];
const internalRunbookAuth = [memberstackAuth, requireDealalityUser, requireInternalRunbookAdmin];
const ownerOdrAuth = [...myDealsAuth, requireOwnerOdrCreateAccess];
const ownerOdrDealAuth = [...myDealsDealAuth, requireOwnerOdrCreateAccess];
const brandAiVisibilityAuth = [
  memberstackAuth,
  requireDealalityUser,
  requireBrandAiVisibilityAccess,
];
const operatorAiVisibilityAuth = [
  memberstackAuth,
  requireDealalityUser,
  requireOperatorAiVisibilityAccess,
];
const aiIntelligenceValidationAuth = [
  memberstackAuth,
  requireDealalityUser,
  requireAiIntelligenceValidationAccess,
];

app.get("/api/ai-visibility/brand/portfolio", ...brandAiVisibilityAuth, getAiVisibilityBrandPortfolio);
app.get("/api/ai-visibility/brand/executive-summary", ...brandAiVisibilityAuth, getAiVisibilityBrandExecutiveSummary);
app.get("/api/ai-visibility/brand/:brandId/overview", ...brandAiVisibilityAuth, getAiVisibilityBrandOverview);
app.get("/api/ai-visibility/brand/:brandId/trend", ...brandAiVisibilityAuth, getAiVisibilityBrandTrend);
app.get("/api/ai-visibility/brand/:brandId/questions", ...brandAiVisibilityAuth, getAiVisibilityBrandQuestions);
app.get("/api/ai-visibility/brand/:brandId/competitors", ...brandAiVisibilityAuth, getAiVisibilityBrandCompetitors);
app.get("/api/ai-visibility/brand/:brandId/sources", ...brandAiVisibilityAuth, getAiVisibilityBrandSources);
app.get("/api/ai-visibility/brand/:brandId/benchmark/diagnostics", ...internalRunbookAuth, getAiVisibilityBrandBenchmarkDiagnostics);
app.get("/api/ai-visibility/brand/:brandId/benchmark", ...brandAiVisibilityAuth, getAiVisibilityBrandBenchmark);
app.get("/api/ai-visibility/brand/:brandId/evidence", ...brandAiVisibilityAuth, getAiVisibilityBrandEvidence);
app.get("/api/ai-visibility/operator/foundation", ...operatorAiVisibilityAuth, getOperatorAiFoundation);
app.get("/api/ai-visibility/operator/universe", ...operatorAiVisibilityAuth, getOperatorAiCustomerUniverse);
app.get(
  "/api/ai-visibility/operator/:operatorId/customer",
  ...operatorAiVisibilityAuth,
  getOperatorAiCustomerPayload
);
app.get("/api/ai-demand-positioning/properties", getAiDemandPositioningProperties);
app.get("/api/ai-demand-positioning/read-health", getAiDemandPositioningReadHealth);
app.get("/api/ai-demand-positioning/property/:propertyId/report", getAiDemandPositioningReport);
app.get("/api/ai-demand-positioning/property/:propertyId/evidence", getAiDemandPositioningEvidence);
app.get("/api/ai-demand-positioning/property/:propertyId/cost-estimate", getAiDemandPositioningCostEstimate);

app.get(
  "/api/ai-intelligence/validation/summary",
  ...aiIntelligenceValidationAuth,
  getAiIntelligenceValidationSummary
);
app.get(
  "/api/ai-intelligence/validation/gates",
  ...aiIntelligenceValidationAuth,
  getAiIntelligenceValidationGates
);
app.get(
  "/api/ai-intelligence/validation/classification",
  ...aiIntelligenceValidationAuth,
  getAiIntelligenceValidationClassification
);
app.get(
  "/api/ai-intelligence/validation/batches",
  ...aiIntelligenceValidationAuth,
  getAiIntelligenceValidationBatches
);
app.get(
  "/api/ai-intelligence/validation/issues",
  ...aiIntelligenceValidationAuth,
  getAiIntelligenceValidationIssues
);
app.get(
  "/api/ai-intelligence/validation/variability",
  ...aiIntelligenceValidationAuth,
  getAiIntelligenceValidationVariability
);
app.get(
  "/api/ai-intelligence/validation/operations",
  ...aiIntelligenceValidationAuth,
  getAiIntelligenceValidationOperations
);
app.get(
  "/api/ai-intelligence/validation/batches/:batchId",
  ...aiIntelligenceValidationAuth,
  getAiIntelligenceValidationBatchDetail
);
app.get(
  "/api/ai-intelligence/golden-set-review/queue",
  ...aiIntelligenceValidationAuth,
  getGoldenSetReviewQueue
);
app.get(
  "/api/ai-intelligence/golden-set-review/progress",
  ...aiIntelligenceValidationAuth,
  getGoldenSetReviewProgress
);
app.get(
  "/api/ai-intelligence/golden-set-review/learning",
  ...aiIntelligenceValidationAuth,
  getGoldenSetReviewLearning
);
app.get(
  "/api/ai-intelligence/golden-set-review/export",
  ...aiIntelligenceValidationAuth,
  getGoldenSetReviewExport
);
app.get(
  "/api/ai-intelligence/golden-set-review/export/all",
  ...aiIntelligenceValidationAuth,
  getGoldenSetReviewExportAll
);
app.get(
  "/api/ai-intelligence/golden-set-review/export/filtered",
  ...aiIntelligenceValidationAuth,
  getGoldenSetReviewExportFiltered
);
app.get(
  "/api/ai-intelligence/golden-set-review/export/packets",
  ...aiIntelligenceValidationAuth,
  getGoldenSetReviewExportPackets
);
app.get(
  "/api/ai-intelligence/golden-set-review/export/assistance-template",
  ...aiIntelligenceValidationAuth,
  getGoldenSetReviewAssistanceTemplate
);
app.post(
  "/api/ai-intelligence/golden-set-review/import/preview",
  ...aiIntelligenceValidationAuth,
  postGoldenSetReviewImportPreview
);
app.post(
  "/api/ai-intelligence/golden-set-review/import/apply",
  ...aiIntelligenceValidationAuth,
  postGoldenSetReviewImportApply
);
app.post(
  "/api/ai-intelligence/golden-set-review/import/accept-assisted",
  ...aiIntelligenceValidationAuth,
  postGoldenSetReviewAcceptAssisted
);
app.post(
  "/api/ai-intelligence/golden-set-review/packets/batch",
  ...aiIntelligenceValidationAuth,
  postGoldenSetReviewPacketsBatch
);
app.get(
  "/api/ai-intelligence/golden-set-review/cases/:caseId/packet",
  ...aiIntelligenceValidationAuth,
  getGoldenSetReviewPacket
);
app.post(
  "/api/ai-intelligence/golden-set-review/cases/:caseId/diff-preview",
  ...aiIntelligenceValidationAuth,
  postGoldenSetReviewDiffPreview
);
app.get(
  "/api/ai-intelligence/golden-set-review/cases/:caseId",
  ...aiIntelligenceValidationAuth,
  getGoldenSetReviewCase
);
app.post(
  "/api/ai-intelligence/golden-set-review/cases/:caseId",
  ...aiIntelligenceValidationAuth,
  postGoldenSetReviewCase
);
app.post(
  "/api/ai-intelligence/golden-set-review/promote",
  ...aiIntelligenceValidationAuth,
  postGoldenSetPromote
);
app.get(
  "/api/ai-intelligence/recommendation-taxonomy-review/ready",
  ...aiIntelligenceValidationAuth,
  getTaxonomyReviewReady
);
app.get(
  "/api/ai-intelligence/recommendation-taxonomy-review/validate",
  ...aiIntelligenceValidationAuth,
  getTaxonomyReviewValidate
);
app.get(
  "/api/ai-intelligence/recommendation-taxonomy-review/queue",
  ...aiIntelligenceValidationAuth,
  getTaxonomyReviewQueue
);
app.post(
  "/api/ai-intelligence/recommendation-taxonomy-review/decision",
  ...aiIntelligenceValidationAuth,
  postTaxonomyReviewDecision
);
app.post(
  "/api/ai-intelligence/recommendation-taxonomy-review/cases/:caseId/decision",
  ...aiIntelligenceValidationAuth,
  postTaxonomyReviewDecision
);
app.post(
  "/api/ai-intelligence/recommendation-taxonomy-review/accept-all-proposals",
  ...aiIntelligenceValidationAuth,
  postTaxonomyReviewAcceptAllProposals
);
app.post(
  "/api/ai-intelligence/recommendation-taxonomy-review/preview-apply",
  ...aiIntelligenceValidationAuth,
  postTaxonomyReviewPreviewApply
);
app.post(
  "/api/ai-intelligence/recommendation-taxonomy-review/apply",
  ...aiIntelligenceValidationAuth,
  postTaxonomyReviewApply
);
app.get(
  "/api/ai-intelligence/presence-validation-review/queue",
  ...aiIntelligenceValidationAuth,
  getPresenceValidationReviewQueue
);
app.get(
  "/api/ai-intelligence/presence-validation-review/summary",
  ...aiIntelligenceValidationAuth,
  getPresenceValidationReviewSummary
);
app.get(
  "/api/ai-intelligence/presence-validation-review/export/preview",
  ...aiIntelligenceValidationAuth,
  getPresenceValidationReviewExportPreview
);
app.get(
  "/api/ai-intelligence/presence-validation-review/export",
  ...aiIntelligenceValidationAuth,
  getPresenceValidationReviewExport
);
app.get(
  "/api/ai-intelligence/presence-validation-review/assisted",
  ...aiIntelligenceValidationAuth,
  getPresenceValidationAssistedProposals
);
app.post(
  "/api/ai-intelligence/presence-validation-review/assisted/import",
  ...aiIntelligenceValidationAuth,
  postPresenceValidationAssistedImport
);
app.get(
  "/api/ai-intelligence/presence-validation-review/bulk-approval/preview",
  ...aiIntelligenceValidationAuth,
  getPresenceValidationBulkApprovalPreview
);
app.post(
  "/api/ai-intelligence/presence-validation-review/bulk-approval/apply",
  ...aiIntelligenceValidationAuth,
  postPresenceValidationBulkApproval
);
app.post(
  "/api/ai-intelligence/presence-validation-review/decide",
  ...aiIntelligenceValidationAuth,
  postPresenceValidationReviewDecision
);
console.log("✅ Brand AI Visibility API routes registered:");
console.log("   GET /api/ai-visibility/brand/portfolio");
console.log("   GET /api/ai-visibility/brand/executive-summary");
console.log("   GET /api/ai-visibility/brand/:brandId/{overview,trend,questions,competitors,sources,evidence}");
console.log("   (hotel-decision-visibility public route retired — HDV consumed via executive/overview)");
console.log("✅ Operator AI Intelligence API routes registered:");
console.log("   GET /api/ai-visibility/operator/foundation");
console.log("✅ AI Intelligence Validation Scorecard routes registered:");
console.log("   GET /api/ai-intelligence/validation/{summary,gates,classification,batches,issues,variability,operations}");
console.log("   GET /api/ai-intelligence/validation/batches/:batchId");
console.log("   GET/POST /api/ai-intelligence/golden-set-review/*");
console.log("   GET/POST /api/ai-intelligence/recommendation-taxonomy-review/*");
console.log("   GET/POST /api/ai-intelligence/presence-validation-review/*");
assertBrandAiVisibilityRoutesRegistered(app);

app.get("/api/my-deals", ...myDealsAuth, getMyDeals);
app.post("/api/my-deals", ...myDealsAuth, createDeal);
app.post("/api/my-deals/initial-matched-support", ...myDealsAuth, postMyDealsInitialMatchedSupport);
app.post("/api/my-deals/operator-requests/by-deals", ...ownerOdrAuth, listMyDealsOperatorRequestsByDeals);
app.get("/api/my-deals/outreach-default", ...myDealsAuth, getOutreachDefault);
app.patch("/api/my-deals/outreach-default", ...myDealsAuth, updateOutreachDefault);
app.get("/api/my-deals/:recordId/outreach-setup", ...myDealsDealAuth, getOutreachSetup);
app.patch("/api/my-deals/:recordId/outreach-setup", ...myDealsDealAuth, updateOutreachSetup);
app.delete("/api/my-deals/:recordId/outreach-setup", ...myDealsDealAuth, deleteOutreachSetup);
app.get("/api/franchise-application/:dealId", mapParamDealIdToRecordId, ...myDealsDealAuth, getFranchiseApplication);
app.patch("/api/franchise-application/:dealId", mapParamDealIdToRecordId, ...myDealsDealAuth, updateFranchiseApplication);
app.get("/api/my-deals/:recordId", ...myDealsDealAuth, getDealById);
app.get("/api/my-deals/:recordId/alternative-brands", ...myDealsDealAuth, getAlternativeBrands);
app.get("/api/my-deals/:recordId/match-score-breakdown", ...myDealsDealAuth, getMatchScoreBreakdown);
app.get("/api/my-deals/:recordId/operator-match-score-breakdown", ...myDealsDealAuth, getOperatorMatchScoreBreakdown);
app.patch("/api/my-deals/:recordId", ...myDealsDealAuth, updateMyDealById);
app.post("/api/my-deals/:recordId/add-recommended-brand", ...myDealsDealAuth, addRecommendedBrand);
app.post("/api/my-deals/:recordId/refresh-brand-cache", ...myDealsDealAuth, refreshDealBrandCache);
app.post("/api/my-deals/:recordId/operator-requests", ...ownerOdrDealAuth, createMyDealsOperatorRequest);
// Deal Readiness Review (deterministic + optional Airtable save via env field names)
app.get("/api/ai/deal-readiness-review/meta", getDealReadinessMeta);
app.post("/api/ai/deal-readiness-review", mapBodyDealIdToRecordId, ...myDealsDealAuth, postDealReadinessReview);
app.post("/api/ai/deal-readiness-review/save", mapBodyDealIdToRecordId, ...myDealsDealAuth, postDealReadinessSave);
app.post("/api/ai/brand-alignment-snapshot", mapBodyDealIdToRecordId, ...myDealsDealAuth, postBrandAlignmentSnapshot);
app.get(
  "/api/deals/:dealId/commercial-readiness-snapshot",
  mapParamDealIdToRecordId,
  ...myDealsDealAuth,
  getCommercialReadinessSnapshot
);
app.post(
  "/api/ai/commercial-readiness-snapshot/save-inputs",
  mapBodyDealIdToRecordId,
  ...myDealsDealAuth,
  postCommercialReadinessSaveInputs
);
app.post(
  "/api/ai/commercial-readiness-snapshot/generate",
  mapBodyDealIdToRecordId,
  ...myDealsDealAuth,
  postCommercialReadinessGenerate
);
app.post("/api/ai/commercial-readiness-snapshot/generate-standalone", postCommercialReadinessGenerateStandalone);
app.get(
  "/api/deals/:dealId/conversion-financing-package",
  (req, _res, next) => {
    req.params.recordId = req.params.dealId;
    next();
  },
  ...myDealsDealAuth,
  getConversionFinancingPackage
);
app.post(
  "/api/deals/:dealId/conversion-financing-package/save-inputs",
  (req, _res, next) => {
    req.params.recordId = req.params.dealId;
    next();
  },
  ...myDealsDealAuth,
  postConversionFinancingSaveInputs
);
app.post(
  "/api/deals/:dealId/conversion-financing-package/generate",
  (req, _res, next) => {
    req.params.recordId = req.params.dealId;
    next();
  },
  ...myDealsDealAuth,
  postConversionFinancingGenerate
);
app.patch(
  "/api/deals/:dealId/conversion-financing-package/sharing",
  (req, _res, next) => {
    req.params.recordId = req.params.dealId;
    next();
  },
  ...myDealsDealAuth,
  patchConversionFinancingSharing
);
app.get(
  "/api/deals/:dealId/hotel-capital-opportunity",
  (req, _res, next) => {
    req.params.recordId = req.params.dealId;
    next();
  },
  ...myDealsDealAuth,
  getHotelCapitalOpportunity
);
app.get(
  "/api/deals/:dealId/operator-capability-snapshot",
  (req, _res, next) => {
    req.params.recordId = req.params.dealId;
    next();
  },
  ...myDealsDealAuth,
  getOperatorCapabilitySnapshot
);
app.post(
  "/api/ai/operator-capability-snapshot",
  mapBodyDealIdToRecordId,
  ...myDealsDealAuth,
  postOperatorCapabilitySnapshot
);
const marketDemandDealAuth = (req, _res, next) => {
  req.params.recordId = req.params.dealId;
  next();
};
app.get("/api/deals/:dealId/demand-centers", marketDemandDealAuth, ...myDealsDealAuth, getDealDemandCenters);
app.get("/api/deals/:dealId/nearby-hotel-supply", marketDemandDealAuth, ...myDealsDealAuth, getDealNearbyHotelSupply);
app.get("/api/deals/:dealId/market-demand-snapshot", marketDemandDealAuth, ...myDealsDealAuth, getDealMarketDemandSnapshot);
app.post("/api/deals/:dealId/generate-market-demand-snapshot", marketDemandDealAuth, ...myDealsDealAuth, postGenerateMarketDemandSnapshot);
app.post("/api/deals/:dealId/preview-demand-center-import", marketDemandDealAuth, ...myDealsDealAuth, postPreviewDemandCenterImport);
app.post("/api/deals/:dealId/import-demand-centers", marketDemandDealAuth, ...myDealsDealAuth, postImportDemandCenters);
app.get(
  "/api/operator-alignment-snapshot/:dealId/profile",
  (req, _res, next) => {
    req.params.recordId = req.params.dealId;
    next();
  },
  ...myDealsDealAuth,
  getOperatorAlignmentSnapshotProfile
);
app.get(
  "/api/operator-alignment-snapshot/:dealId/companies",
  (req, _res, next) => {
    req.params.recordId = req.params.dealId;
    next();
  },
  ...myDealsDealAuth,
  getOperatorAlignmentSnapshotCompanies
);
// Operator Fit Engine v2 (feature-flagged; default off — does not replace legacy OAS)
app.get("/api/operator-fit/v2/flag", ...myDealsAuth, getOperatorFitV2Flag);
app.get(
  "/api/operator-fit/v2/:dealId/top5",
  (req, _res, next) => {
    req.params.recordId = req.params.dealId;
    next();
  },
  ...myDealsDealAuth,
  getOperatorFitV2Top5
);
// Target List (brand shortlist) API — Batch 2A owner auth
const mapTargetListDealParam = (req, _res, next) => {
  req.params.recordId = req.params.dealId;
  next();
};
app.get("/api/target-list/:dealId", mapTargetListDealParam, ...myDealsDealAuth, getTargetList);
app.post("/api/target-list", mapBodyDealIdToRecordId, ...myDealsDealAuth, addToTargetList);
app.post("/api/target-list/batch-delete", ...myDealsAuth, batchRemoveFromTargetList);
app.post("/api/target-list/mark-deleted", mapBodyDealIdToRecordId, ...myDealsDealAuth, markAsDeleted);
app.post("/api/target-list/restore", mapBodyDealIdToRecordId, ...myDealsDealAuth, restoreFromDeleted);
app.patch("/api/target-list/:targetId", ...myDealsAuth, requireTargetListRecordAccess, updateTarget);
app.delete("/api/target-list/:targetId", ...myDealsAuth, requireTargetListRecordAccess, removeFromTargetList);
// Brand Deal Requests — Batch 2A owner-side flows (brand-side deferred)
app.post("/api/brand-deal-requests", mapBodyDealIdToRecordId, ...myDealsDealAuth, createBrandDealRequest);
app.post("/api/brand-deal-requests/by-deals", ...myDealsAuth, listBrandDealRequestsByDealsPost);
app.get("/api/brand-deal-requests/activity", gateOwnerBdrActivity, getBrandDealActivityLog);
app.get("/api/brand-deal-requests/deal-meta", gateOwnerBdrDealMeta, getBrandDealMetaBatch);
// Dedicated path so Deal Room (Brand) never falls through to listForBrand (requires ?brand=) if dealRoom routing is missing or query is altered.
app.get("/api/deal-room/brand-requests", listBrandDealRequestsForDealRoom);
app.get("/api/brand-deal-requests", gateOwnerBdrDealIdsQuery, gateOwnerBdrListAll, (req, res) => {
  if (req.query.dealIds) return listBrandDealRequestsByDeals(req, res);
  const dealRoom = req.query.dealRoom ?? req.query.deal_room;
  if (dealRoom === true || dealRoom === 1) return listBrandDealRequestsForDealRoom(req, res);
  const dealRoomStr =
    dealRoom == null ? "" : String(Array.isArray(dealRoom) ? dealRoom[0] : dealRoom).trim().toLowerCase();
  if (dealRoomStr === "1" || dealRoomStr === "true") return listBrandDealRequestsForDealRoom(req, res);
  const allParam = req.query.all;
  if (allParam === "1" || allParam === "true") return listBrandDealRequestsAll(req, res);
  return listBrandDealRequests(req, res);
});
app.get("/api/brand-deal-requests/:requestId", getBrandDealRequestById);
app.get("/api/brand-deal-requests/:requestId/proposal-draft", getProposalDraft);
app.post("/api/brand-deal-requests/:requestId/submit-proposal", submitProposal);
app.patch(
  "/api/brand-deal-requests/:requestId",
  memberstackAuth,
  requireDealalityUser,
  requireOwnerBdrRecordAccess,
  updateBrandDealRequestStatus
);
app.post("/api/brand-deal-requests/bulk-update", ...myDealsAuth, bulkUpdateBrandDealRequestStatus);
// Operator Deal Requests — Phase 2 scoped (see docs/operator-deal-requests-phase-2-scoping.md)
const operatorDealsAuth = [memberstackAuth, requireDealalityUser, requireOperatorDealsAccess];
app.post("/api/operator-deal-requests/bulk-update", ...operatorDealsAuth, bulkUpdateOperatorDealRequests);
app.get("/api/operator-deal-requests/deal-meta", ...operatorDealsAuth, getOperatorDealMetaBatch);
app.get("/api/operator-deal-requests/activity", ...operatorDealsAuth, getOperatorDealActivity);
app.get("/api/operator-deal-requests", ...operatorDealsAuth, listOperatorDealRequests);
app.get("/api/operator-deal-requests/:requestId", ...operatorDealsAuth, getOperatorDealRequestById);
app.patch("/api/operator-deal-requests/:requestId", ...operatorDealsAuth, updateOperatorDealRequest);
app.get("/api/brand-workspace/kpi-history", getBrandWorkspaceKpiHistory);
app.post("/api/brand-workspace/kpi-history", postBrandWorkspaceKpiSnapshot);
// Deal Room Documents (specific paths before generic :id)
app.get("/api/deal-room-documents/files/:dealId/:filename", serveDealRoomDocumentFile);
app.post(
  "/api/deal-room-documents/upload/:dealId",
  (req, res, next) => {
    dealRoomDocsUpload.single("file")(req, res, (err) => {
      if (err) {
        if (err.code === "LIMIT_FILE_SIZE") {
          return res.status(413).json({ success: false, error: "File too large. Maximum size is 10 MB per file." });
        }
        if (err.message && err.message.includes("File type not allowed")) {
          return res.status(400).json({ success: false, error: err.message });
        }
        return res.status(500).json({ success: false, error: err.message || "Upload failed" });
      }
      next();
    });
  },
  uploadDealRoomDocumentFile
);
app.get("/api/deal-room-documents", listDealRoomDocuments);
app.get("/api/deal-room-documents/brand/:requestId", listDealRoomDocumentsForBrandRequest);
app.post("/api/deal-room-documents", createDealRoomDocument);
app.patch("/api/deal-room-documents/:id", updateDealRoomDocument);
app.delete("/api/deal-room-documents/:id", deleteDealRoomDocument);
// Deal Setup attachments: multipart upload (multer); then business logic in uploadDealAttachments
app.post("/api/my-deals/:recordId/attachments", ...myDealsDealAuth, (req, res, next) => {
  dealAttachmentsUpload.array("files")(req, res, (err) => {
    if (err) {
      if (err.code === "LIMIT_FILE_SIZE") {
        return res.status(413).json({ success: false, error: "File too large. Maximum size is 5 MB per file (Airtable limit)." });
      }
      if (err.message && err.message.includes("File type not allowed")) {
        return res.status(400).json({ success: false, error: err.message });
      }
      return res.status(500).json({ success: false, error: err.message || "Upload failed" });
    }
    next();
  });
}, uploadDealAttachments);
// Serve stored attachment files (path-traversal safe)
app.get("/api/my-deals/:recordId/attachments/:filename", ...myDealsDealAuth, (req, res) => {
  const { recordId, filename } = req.params;
  if (!recordId || !filename || filename.includes("..") || recordId.includes("..")) {
    return res.status(400).send();
  }
  const resolved = path.resolve(DEAL_ATTACHMENTS_DIR, recordId, filename);
  const baseResolved = path.resolve(DEAL_ATTACHMENTS_DIR);
  if (!resolved.startsWith(baseResolved) || resolved === baseResolved) {
    return res.status(403).send();
  }
  if (!fs.existsSync(resolved) || !fs.statSync(resolved).isFile()) {
    return res.status(404).send();
  }
  res.sendFile(resolved);
});

// Outreach Hub API: read/write OutreachPlans, PlanTargets, Threads, Messages, Templates, Sequences, SequenceSteps
// Table slug: plans | plan-targets | threads | messages | templates | sequences | sequence-steps
app.get("/api/outreach-hub/:table", outreachHubList);
app.get("/api/outreach-hub/:table/:recordId", outreachHubGet);
app.post("/api/outreach-hub/:table", outreachHubCreate);
app.patch("/api/outreach-hub/:table/:recordId", outreachHubUpdate);
app.delete("/api/outreach-hub/:table/:recordId", outreachHubRemove);
app.get("/api/outreach/deal-activity-log", getOutreachDealActivityLog);

function sendPublicNoStore(res, publicRelativePath) {
  res.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
  res.setHeader("Pragma", "no-cache");
  res.setHeader("Expires", "0");
  res.sendFile(path.join(__dirname, "public", publicRelativePath));
}

// Serve the unified app hub and brand setup BEFORE static so paths are not treated as static files
// Shell assets must not stick behind stale ETag/cache during local founder QA.
// Read app.js from disk on every request (no sendFile handle reuse surprises).
app.get("/app.js", (req, res) => {
  const appJsPath = path.join(__dirname, "public", "app.js");
  let body;
  try {
    body = fs.readFileSync(appJsPath, "utf8");
  } catch (err) {
    console.error("[shell] failed to read public/app.js", err?.message || err);
    return res.status(500).type("text/plain").send("app.js unavailable");
  }
  res.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
  res.setHeader("Pragma", "no-cache");
  res.setHeader("Expires", "0");
  if (process.env.NODE_ENV !== "production") {
    res.setHeader("X-Dealality-App-Js-Bytes", String(Buffer.byteLength(body, "utf8")));
    res.setHeader(
      "X-Dealality-App-Js-Has-Canonical",
      body.includes("canonicalWorkspaceOptions") ? "1" : "0"
    );
  }
  res.type("application/javascript").send(body);
});
app.get("/app.html", (req, res) => {
  res.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
  res.setHeader("Pragma", "no-cache");
  res.setHeader("Expires", "0");
  res.sendFile(path.join(__dirname, "public", "app.html"));
});
app.get("/app", (req, res) => {
    res.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
    res.sendFile(path.join(__dirname, 'public', 'app.html'));
});
app.get("/app/", (req, res) => {
    res.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
    res.sendFile(path.join(__dirname, 'public', 'app.html'));
});
app.get("/app/home", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'app', 'home.html'));
});
app.get("/app/home/", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'app', 'home.html'));
});
app.get("/app/home.html", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'app', 'home.html'));
});
app.get("/app/home-original", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'app', 'home-original.html'));
});
app.get("/app/home-original/", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'app', 'home-original.html'));
});
app.get("/app/home-original.html", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'app', 'home-original.html'));
});
// Serve app subfolder assets (dashboard.css, dashboard.js, dashboard-adapter.js)
app.use("/app", express.static(path.join(__dirname, 'public', 'app')));
app.get("/brand-setup", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'brand-setup.html'));
});
app.get("/brand-setup/", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'brand-setup.html'));
});
app.get("/deal-setup", (req, res) => {
    const q = req.originalUrl.includes("?") ? req.originalUrl.slice(req.originalUrl.indexOf("?")) : "";
    res.redirect(302, "/deal-setup.html" + q);
});
app.get("/deal-setup/", (req, res) => {
    const q = req.originalUrl.includes("?") ? req.originalUrl.slice(req.originalUrl.indexOf("?")) : "";
    res.redirect(302, "/deal-setup.html" + q);
});
app.get("/new-deal-setup", (req, res) => {
    const q = req.originalUrl.includes("?") ? req.originalUrl.slice(req.originalUrl.indexOf("?")) : "";
    res.redirect(302, "/new-deal-setup.html" + q);
});
app.get("/new-deal-setup/", (req, res) => {
    const q = req.originalUrl.includes("?") ? req.originalUrl.slice(req.originalUrl.indexOf("?")) : "";
    res.redirect(302, "/new-deal-setup.html" + q);
});
app.get("/franchise-application", (req, res) => {
    const q = req.originalUrl.includes("?") ? req.originalUrl.slice(req.originalUrl.indexOf("?")) : "";
    res.redirect(302, "/franchise-application.html" + q);
});
app.get("/franchise-application/", (req, res) => {
    const q = req.originalUrl.includes("?") ? req.originalUrl.slice(req.originalUrl.indexOf("?")) : "";
    res.redirect(302, "/franchise-application.html" + q);
});
app.get("/company-settings", (req, res) => {
    res.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
    res.sendFile(path.join(__dirname, 'public', 'company-settings.html'));
});
app.get("/company-settings/", (req, res) => {
    res.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
    res.sendFile(path.join(__dirname, 'public', 'company-settings.html'));
});
app.get("/profile-settings", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'profile-settings.html'));
});
app.get("/profile-settings/", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'profile-settings.html'));
});
app.get("/user-management", (req, res) => {
    res.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
    res.sendFile(path.join(__dirname, 'public', 'user-management.html'));
});
app.get("/user-management/", (req, res) => {
    res.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
    res.sendFile(path.join(__dirname, 'public', 'user-management.html'));
});
app.get("/my-brands", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'all-brands-dashboard.html'));
});
app.get("/my-brands/", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'all-brands-dashboard.html'));
});
app.get("/my-third-party-operators", (req, res) => {
    res.sendFile(path.join(__dirname, "public", "my-third-party-operators-new.html"));
});
app.get("/my-third-party-operators/", (req, res) => {
    res.sendFile(path.join(__dirname, "public", "my-third-party-operators-new.html"));
});
app.get("/my-third-party-operators-new", (req, res) => {
    res.sendFile(path.join(__dirname, "public", "my-third-party-operators-new.html"));
});
app.get("/my-third-party-operators-new/", (req, res) => {
    res.sendFile(path.join(__dirname, "public", "my-third-party-operators-new.html"));
});
app.get("/my-third-party-operators.html", (req, res) => {
    res.sendFile(path.join(__dirname, "public", "my-third-party-operators-new.html"));
});
app.get("/my-third-party-operators-new.html", (req, res) => {
    res.sendFile(path.join(__dirname, "public", "my-third-party-operators-new.html"));
});
app.get("/my-deals", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'my-deals.html'));
});
app.get("/my-deals/", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'my-deals.html'));
});
app.get("/deal-readiness-snapshot", (req, res) => {
    const q = req.originalUrl.includes("?") ? req.originalUrl.slice(req.originalUrl.indexOf("?")) : "";
    res.redirect(302, "/deal-readiness-snapshot.html" + q);
});
app.get("/deal-readiness-snapshot/", (req, res) => {
    const q = req.originalUrl.includes("?") ? req.originalUrl.slice(req.originalUrl.indexOf("?")) : "";
    res.redirect(302, "/deal-readiness-snapshot.html" + q);
});
app.get("/deal-readiness-snapshot.html", (req, res) => {
    res.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
    res.sendFile(path.join(__dirname, "public", "deal-readiness-snapshot.html"));
});
app.get("/brand-alignment-snapshot", (req, res) => {
    const q = req.originalUrl.includes("?") ? req.originalUrl.slice(req.originalUrl.indexOf("?")) : "";
    res.redirect(302, "/brand-alignment-snapshot.html" + q);
});
app.get("/brand-alignment-snapshot/", (req, res) => {
    const q = req.originalUrl.includes("?") ? req.originalUrl.slice(req.originalUrl.indexOf("?")) : "";
    res.redirect(302, "/brand-alignment-snapshot.html" + q);
});
app.get("/brand-alignment-snapshot.html", (req, res) => {
    res.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
    res.sendFile(path.join(__dirname, "public", "brand-alignment-snapshot.html"));
});
app.get("/operator-capability-snapshot", (req, res) => {
    const q = req.originalUrl.includes("?") ? req.originalUrl.slice(req.originalUrl.indexOf("?")) : "";
    res.redirect(302, "/operator-capability-snapshot.html" + q);
});
app.get("/operator-capability-snapshot/", (req, res) => {
    const q = req.originalUrl.includes("?") ? req.originalUrl.slice(req.originalUrl.indexOf("?")) : "";
    res.redirect(302, "/operator-capability-snapshot.html" + q);
});
app.get("/operator-capability-snapshot.html", (req, res) => {
    res.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
    res.sendFile(path.join(__dirname, "public", "operator-capability-snapshot.html"));
});
app.get("/market-demand", (req, res) => {
    const q = req.originalUrl.includes("?") ? req.originalUrl.slice(req.originalUrl.indexOf("?")) : "";
    res.redirect(302, "/market-demand.html" + q);
});
app.get("/market-demand/", (req, res) => {
    const q = req.originalUrl.includes("?") ? req.originalUrl.slice(req.originalUrl.indexOf("?")) : "";
    res.redirect(302, "/market-demand.html" + q);
});
app.get("/market-demand.html", (req, res) => {
    res.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
    res.sendFile(path.join(__dirname, "public", "market-demand.html"));
});
app.get("/operator-alignment-snapshot", (req, res) => {
    const q = req.originalUrl.includes("?") ? req.originalUrl.slice(req.originalUrl.indexOf("?")) : "";
    res.redirect(302, "/operator-alignment-snapshot.html" + q);
});
app.get("/operator-alignment-snapshot/", (req, res) => {
    const q = req.originalUrl.includes("?") ? req.originalUrl.slice(req.originalUrl.indexOf("?")) : "";
    res.redirect(302, "/operator-alignment-snapshot.html" + q);
});
app.get("/operator-alignment-snapshot.html", (req, res) => {
    res.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
    res.sendFile(path.join(__dirname, "public", "operator-alignment-snapshot.html"));
});
app.get("/operator-fit-alignment", (req, res) => {
  const q = req.originalUrl.includes("?") ? req.originalUrl.slice(req.originalUrl.indexOf("?")) : "";
  res.redirect(302, "/operator-fit-alignment.html" + q);
});
app.get("/operator-fit-alignment.html", (req, res) => {
  res.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
  res.sendFile(path.join(__dirname, "public", "operator-fit-alignment.html"));
});
app.get("/internal/operator-fit-data-readiness", (req, res) => {
  const q = req.originalUrl.includes("?") ? req.originalUrl.slice(req.originalUrl.indexOf("?")) : "";
  res.redirect(302, "/internal/operator-fit-data-readiness.html" + q);
});
app.get("/internal/operator-fit-data-readiness.html", (req, res) => {
  res.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
  res.setHeader("X-Robots-Tag", "noindex, nofollow");
  res.sendFile(path.join(__dirname, "public", "internal", "operator-fit-data-readiness.html"));
});
app.get("/internal/operator-fit-calibration", (req, res) => {
  const q = req.originalUrl.includes("?") ? req.originalUrl.slice(req.originalUrl.indexOf("?")) : "";
  res.redirect(302, "/internal/operator-fit-calibration.html" + q);
});
app.get("/internal/operator-fit-calibration.html", (req, res) => {
  res.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
  res.setHeader("X-Robots-Tag", "noindex, nofollow");
  res.sendFile(path.join(__dirname, "public", "internal", "operator-fit-calibration.html"));
});
app.get("/internal/operator-fit-pilot", (req, res) => {
  const q = req.originalUrl.includes("?") ? req.originalUrl.slice(req.originalUrl.indexOf("?")) : "";
  res.redirect(302, "/internal/operator-fit-pilot.html" + q);
});
app.get("/internal/operator-fit-pilot.html", (req, res) => {
  res.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
  res.setHeader("X-Robots-Tag", "noindex, nofollow");
  res.sendFile(path.join(__dirname, "public", "internal", "operator-fit-pilot.html"));
});
app.get("/internal/operator-intelligence", (req, res) => {
  const q = req.originalUrl.includes("?") ? req.originalUrl.slice(req.originalUrl.indexOf("?")) : "";
  res.redirect(302, "/internal/operator-intelligence.html" + q);
});
app.get("/internal/operator-intelligence.html", (req, res) => {
  res.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
  res.setHeader("X-Robots-Tag", "noindex, nofollow");
  res.sendFile(path.join(__dirname, "public", "internal", "operator-intelligence.html"));
});
app.get("/internal/acquisition-intelligence", (req, res) => {
  const q = req.originalUrl.includes("?") ? req.originalUrl.slice(req.originalUrl.indexOf("?")) : "";
  res.redirect(302, "/internal/acquisition-intelligence.html" + q);
});
app.get("/internal/acquisition-intelligence.html", (req, res) => {
  res.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
  res.setHeader("X-Robots-Tag", "noindex, nofollow");
  res.sendFile(path.join(__dirname, "public", "internal", "acquisition-intelligence.html"));
});
app.get("/commercial-readiness-snapshot", (req, res) => {
    const q = req.originalUrl.includes("?") ? req.originalUrl.slice(req.originalUrl.indexOf("?")) : "";
    res.redirect(302, "/commercial-readiness-snapshot.html" + q);
});
app.get("/commercial-readiness-snapshot/", (req, res) => {
    const q = req.originalUrl.includes("?") ? req.originalUrl.slice(req.originalUrl.indexOf("?")) : "";
    res.redirect(302, "/commercial-readiness-snapshot.html" + q);
});
app.get("/commercial-readiness-snapshot.html", (req, res) => {
    res.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
    res.sendFile(path.join(__dirname, "public", "commercial-readiness-snapshot.html"));
});
app.get("/owner-diagnostic-sample", (req, res) => {
    const q = req.originalUrl.includes("?") ? req.originalUrl.slice(req.originalUrl.indexOf("?")) : "";
    res.redirect(302, "/owner-diagnostic-sample.html" + q);
});
app.get("/owner-diagnostic-sample/", (req, res) => {
    const q = req.originalUrl.includes("?") ? req.originalUrl.slice(req.originalUrl.indexOf("?")) : "";
    res.redirect(302, "/owner-diagnostic-sample.html" + q);
});
app.get("/owner-diagnostic-sample.html", (req, res) => {
    res.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
    res.sendFile(path.join(__dirname, "public", "owner-diagnostic-sample.html"));
});
app.get("/brand-explorer-export", (req, res) => {
    const q = req.originalUrl.includes("?") ? req.originalUrl.slice(req.originalUrl.indexOf("?")) : "";
    res.redirect(302, "/brand-explorer-export.html" + q);
});
app.get("/brand-explorer-export/", (req, res) => {
    const q = req.originalUrl.includes("?") ? req.originalUrl.slice(req.originalUrl.indexOf("?")) : "";
    res.redirect(302, "/brand-explorer-export.html" + q);
});
app.get("/brand-explorer-export.html", (req, res) => {
    res.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
    res.sendFile(path.join(__dirname, "public", "brand-explorer-export.html"));
});
app.get("/operator-explorer-export", (req, res) => {
    const q = req.originalUrl.includes("?") ? req.originalUrl.slice(req.originalUrl.indexOf("?")) : "";
    res.redirect(302, "/operator-explorer-export.html" + q);
});
app.get("/operator-explorer-export/", (req, res) => {
    const q = req.originalUrl.includes("?") ? req.originalUrl.slice(req.originalUrl.indexOf("?")) : "";
    res.redirect(302, "/operator-explorer-export.html" + q);
});
app.get("/operator-explorer-export.html", (req, res) => {
    res.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
    res.sendFile(path.join(__dirname, "public", "operator-explorer-export.html"));
});
app.get("/operator-explorer-share", (req, res) => {
    const q = req.originalUrl.includes("?") ? req.originalUrl.slice(req.originalUrl.indexOf("?")) : "";
    res.redirect(302, "/operator-explorer-share.html" + q);
});
app.get("/operator-explorer-share/", (req, res) => {
    const q = req.originalUrl.includes("?") ? req.originalUrl.slice(req.originalUrl.indexOf("?")) : "";
    res.redirect(302, "/operator-explorer-share.html" + q);
});
app.get("/operator-explorer-share.html", (req, res) => {
    res.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
    res.sendFile(path.join(__dirname, "public", "operator-explorer-share.html"));
});
app.get("/operator-explorer-preview", (req, res) => {
    const q = req.originalUrl.includes("?") ? req.originalUrl.slice(req.originalUrl.indexOf("?")) : "";
    res.redirect(302, "/operator-explorer-share.html" + q);
});
app.get("/operator-explorer-preview/", (req, res) => {
    const q = req.originalUrl.includes("?") ? req.originalUrl.slice(req.originalUrl.indexOf("?")) : "";
    res.redirect(302, "/operator-explorer-share.html" + q);
});
app.get("/brand-explorer-share", (req, res) => {
    const q = req.originalUrl.includes("?") ? req.originalUrl.slice(req.originalUrl.indexOf("?")) : "";
    res.redirect(302, "/brand-explorer-share.html" + q);
});
app.get("/brand-explorer-share/", (req, res) => {
    const q = req.originalUrl.includes("?") ? req.originalUrl.slice(req.originalUrl.indexOf("?")) : "";
    res.redirect(302, "/brand-explorer-share.html" + q);
});
app.get("/brand-explorer-share.html", (req, res) => {
    res.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
    res.sendFile(path.join(__dirname, "public", "brand-explorer-share.html"));
});
app.get("/brand-explorer-preview", (req, res) => {
    const q = req.originalUrl.includes("?") ? req.originalUrl.slice(req.originalUrl.indexOf("?")) : "";
    res.redirect(302, "/brand-explorer-share.html" + q);
});
app.get("/brand-explorer-preview/", (req, res) => {
    const q = req.originalUrl.includes("?") ? req.originalUrl.slice(req.originalUrl.indexOf("?")) : "";
    res.redirect(302, "/brand-explorer-share.html" + q);
});
app.get("/owner-ai-demand-share", (req, res) => {
    const q = req.originalUrl.includes("?") ? req.originalUrl.slice(req.originalUrl.indexOf("?")) : "";
    res.redirect(302, "/owner-ai-demand-share.html" + q);
});
app.get("/owner-ai-demand-share/", (req, res) => {
    const q = req.originalUrl.includes("?") ? req.originalUrl.slice(req.originalUrl.indexOf("?")) : "";
    res.redirect(302, "/owner-ai-demand-share.html" + q);
});
app.get("/owner-ai-demand-share.html", (req, res) => {
    res.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
    res.sendFile(path.join(__dirname, "public", "owner-ai-demand-share.html"));
});
app.get("/ai-demand-positioning-share", (req, res) => {
    const q = req.originalUrl.includes("?") ? req.originalUrl.slice(req.originalUrl.indexOf("?")) : "";
    res.redirect(302, "/owner-ai-demand-share.html" + q);
});
app.get("/ai-demand-positioning-share/", (req, res) => {
    const q = req.originalUrl.includes("?") ? req.originalUrl.slice(req.originalUrl.indexOf("?")) : "";
    res.redirect(302, "/owner-ai-demand-share.html" + q);
});
app.get("/getting-started", (req, res) => {
    res.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
    res.sendFile(path.join(__dirname, 'public', 'getting-started.html'));
});
app.get("/getting-started/", (req, res) => {
    res.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
    res.sendFile(path.join(__dirname, 'public', 'getting-started.html'));
});
app.get("/knowledge-base", (req, res) => {
    res.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
    res.sendFile(path.join(__dirname, 'public', 'knowledge-base.html'));
});
app.get("/knowledge-base/", (req, res) => {
    res.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
    res.sendFile(path.join(__dirname, 'public', 'knowledge-base.html'));
});

// CSP that explicitly sets connect-src so fetch/XHR aren't blocked (e.g. form or API calls)
const SIGNUP_VERIFY_CSP =
  "default-src 'self'; " +
  "connect-src 'self' https:; " +
  "script-src 'self' 'unsafe-inline' https://code.jquery.com https://cdn.jsdelivr.net https://static.memberstack.com; " +
  "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com https://cdn.jsdelivr.net https://cdn.prod.website-files.com; " +
  "font-src https://fonts.gstatic.com https://cdn.prod.website-files.com data:; " +
  "img-src 'self' data: https:;";

// Signup routes (before express.static so they are always matched and we set CSP)
app.get("/signup", (req, res) => {
    res.setHeader("Content-Security-Policy", SIGNUP_VERIFY_CSP);
    res.sendFile(path.join(__dirname, 'public', 'signup.html'));
});
app.get("/signup-temp", (req, res) => {
    res.setHeader("Content-Security-Policy", SIGNUP_VERIFY_CSP);
    res.sendFile(path.join(__dirname, 'public', 'signup-temp.html'));
});
app.get("/signup-temp.html", (req, res) => {
    res.setHeader("Content-Security-Policy", SIGNUP_VERIFY_CSP);
    res.sendFile(path.join(__dirname, 'public', 'signup-temp.html'));
});
app.get(["/terms", "/terms.html"], (req, res) => {
    res.sendFile(path.join(__dirname, "public", "terms.html"));
});
app.get(["/privacy", "/privacy.html"], (req, res) => {
    res.sendFile(path.join(__dirname, "public", "privacy.html"));
});
app.get("/verify", (req, res) => {
    res.setHeader("Content-Security-Policy", SIGNUP_VERIFY_CSP);
    res.setHeader("Cache-Control", "no-store");
    res.sendFile(path.join(__dirname, "public", "verify.html"));
});
app.get("/verify.html", (req, res) => {
    res.setHeader("Content-Security-Policy", SIGNUP_VERIFY_CSP);
    res.setHeader("Cache-Control", "no-store");
    res.sendFile(path.join(__dirname, "public", "verify.html"));
});

// Redirect /deal-compare to the static file so it works even when another server proxies static files
app.get("/api/deal-compare/proposals", getProposalsForDeal);
app.get("/deal-compare", (req, res) => res.redirect(301, "/deal-compare.html"));
app.get("/deal-compare/", (req, res) => res.redirect(301, "/deal-compare.html"));

// Basic routing/test endpoint for Railway + Webflow embedding verification
app.get("/", (req, res) => res.json({ ok: true, service: "deal-capture-radar-backend", message: "Radar backend is running" }));
// Landing support pages – serve from public first so nav/content match new landing (before any other static)
app.get("/about", (req, res) => res.redirect(301, "/about.html"));
app.get("/about.html", (req, res) => {
    res.setHeader("Cache-Control", "no-store");
    res.sendFile(path.join(__dirname, 'public', 'about.html'));
});
app.get("/for-owners.html", (req, res) => res.sendFile(path.join(__dirname, 'public', 'for-owners.html')));
app.get("/for-brands-operators.html", (req, res) => res.sendFile(path.join(__dirname, 'public', 'for-brands-operators.html')));
app.get("/how-it-works.html", (req, res) => res.sendFile(path.join(__dirname, 'public', 'how-it-works.html')));
app.get("/platform.html", (req, res) => res.sendFile(path.join(__dirname, 'public', 'platform.html')));
app.use("/landing", express.static(path.join(__dirname, "public"), { index: "index.html" }));

// Serve market intelligence tool pages
app.get("/competitive-intelligence", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'competitive-intelligence.html'));
});

app.get("/market-forecasting", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'market-forecasting.html'));
});

app.get("/deal-benchmarking", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'deal-benchmarking.html'));
});

app.get("/market-alerts", (req, res) => {
    sendPublicNoStore(res, "market-alerts.html");
});
app.get("/market-alerts.html", (req, res) => {
    sendPublicNoStore(res, "market-alerts.html");
});
app.get("/market-alerts.js", (req, res) => {
    sendPublicNoStore(res, "market-alerts.js");
});
app.get("/market-alerts-back", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'market-alerts-back.html'));
});

app.get("/market-analytics", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'market-analytics.html'));
});

// CSP for app pages that would otherwise get default-src 'none' from sendFile (allows data: images, localhost for DevTools)
const APP_PAGE_CSP =
  "default-src 'self'; " +
  "connect-src 'self' https: http://localhost:* http://127.0.0.1:* ws://localhost:* ws://127.0.0.1:*; " +
  "script-src 'self' 'unsafe-inline' https:; " +
  "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; " +
  "font-src 'self' https://fonts.gstatic.com data:; " +
  "img-src 'self' data: https:;";

app.get("/management-operator-radar", (req, res) => {
  const filePath = path.join(__dirname, "public", "management-operator-radar.html");
  res.setHeader("Content-Security-Policy", APP_PAGE_CSP);
  res.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
  res.type("html");
  res.send(fs.readFileSync(filePath, "utf8"));
});

// Serve the brand review dashboard
app.get("/brand-review", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'brand-review.html'));
});

// Retired standalone page: keep legacy URL but send users to active workflow.
app.get("/deal-brand-fit-analyzer", (req, res) => {
    res.redirect("/app.html#/my-deals");
});

app.get("/production-dashboard", (req, res) => {
    res.redirect("/app.html#/brand-development-dashboard");
});

// Retired standalone brand workspace dashboard: keep URL but redirect to active brand dashboard.
app.get("/webflow-brand-dashboard.html", (req, res) => {
    res.redirect("/app.html#/brand-development-dashboard");
});


// Retired brand workspace pipeline page → active brand development dashboard.
function redirectBrandWorkspacePipeline(req, res) {
    const q = req.url.includes("?") ? req.url.slice(req.url.indexOf("?")) : "";
    res.redirect(302, "/brand-development-dashboard" + q);
}
app.get("/brand-workspace-pipeline", redirectBrandWorkspacePipeline);
app.get("/brand-workspace-pipeline.html", redirectBrandWorkspacePipeline);

// Legacy fit-list page → My Deals Brand Shortlist tab.
function redirectRecommendedFitList(req, res) {
    const params = new URLSearchParams(req.query);
    const dealId = params.get("dealId");
    const target = new URL("/my-deals.html", "http://local");
    target.searchParams.set("tab", "target-list");
    if (dealId) target.searchParams.set("dealId", dealId);
    const qs = target.search;
    res.redirect(302, "/my-deals.html" + qs);
}
app.get("/recommended-fit-list", redirectRecommendedFitList);
app.get("/recommended-fit-list.html", redirectRecommendedFitList);

// Winner-selection page not shipped — send users back to My Deals deal compare tab.
function redirectDealCompareSelectWinner(req, res) {
    const params = new URLSearchParams(req.query);
    const dealId = params.get("dealId");
    const target = new URL("/my-deals.html", "http://local");
    target.searchParams.set("tab", "deal-compare");
    if (dealId) target.searchParams.set("dealId", dealId);
    res.redirect(302, "/my-deals.html" + target.search);
}
app.get("/deal-compare-select-winner", redirectDealCompareSelectWinner);
app.get("/deal-compare-select-winner.html", redirectDealCompareSelectWinner);

// Serve the brand development dashboard
app.get("/brand-development-dashboard", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'brand-development-dashboard.html'));
});

app.get("/ai-visibility", (req, res) => {
    res.sendFile(path.join(__dirname, "public", "ai-visibility-brand.html"));
});
app.get("/ai-visibility/", (req, res) => {
    res.sendFile(path.join(__dirname, "public", "ai-visibility-brand.html"));
});
app.get("/ai-visibility-brand", (req, res) => {
    res.sendFile(path.join(__dirname, "public", "ai-visibility-brand.html"));
});
app.get("/ai-visibility-brand.html", (req, res) => {
    res.sendFile(path.join(__dirname, "public", "ai-visibility-brand.html"));
});
app.get("/operator/ai-intelligence", (req, res) => {
    res.sendFile(path.join(__dirname, "public", "operator-ai-intelligence.html"));
});
app.get("/operator/ai-intelligence/", (req, res) => {
    res.sendFile(path.join(__dirname, "public", "operator-ai-intelligence.html"));
});
app.get("/ai-intelligence-validation", (req, res) => {
  sendPublicNoStore(res, "ai-intelligence-validation.html");
});
app.get("/ai-intelligence-validation.html", (req, res) => {
  sendPublicNoStore(res, "ai-intelligence-validation.html");
});
app.get("/js/ai-visibility/ai-intelligence-validation.js", (req, res) => {
  sendPublicNoStore(res, path.join("js", "ai-visibility", "ai-intelligence-validation.js"));
});
app.get("/ai-intelligence-golden-set-review", (req, res) => {
  sendPublicNoStore(res, "ai-intelligence-golden-set-review.html");
});
app.get("/ai-intelligence-golden-set-review.html", (req, res) => {
  sendPublicNoStore(res, "ai-intelligence-golden-set-review.html");
});
app.get("/ai-intelligence-recommendation-taxonomy-review", (req, res) => {
  sendPublicNoStore(res, "ai-intelligence-recommendation-taxonomy-review.html");
});
app.get("/ai-intelligence-recommendation-taxonomy-review.html", (req, res) => {
  sendPublicNoStore(res, "ai-intelligence-recommendation-taxonomy-review.html");
});
app.get("/js/ai-visibility/ai-intelligence-recommendation-taxonomy-review.js", (req, res) => {
  sendPublicNoStore(
    res,
    path.join("js", "ai-visibility", "ai-intelligence-recommendation-taxonomy-review.js")
  );
});
app.get("/ai-intelligence-presence-validation-review", (req, res) => {
  sendPublicNoStore(res, "ai-intelligence-presence-validation-review.html");
});
app.get("/ai-intelligence-presence-validation-review.html", (req, res) => {
  sendPublicNoStore(res, "ai-intelligence-presence-validation-review.html");
});
app.get("/js/ai-visibility/ai-intelligence-presence-validation-review.js", (req, res) => {
  sendPublicNoStore(
    res,
    path.join("js", "ai-visibility", "ai-intelligence-presence-validation-review.js")
  );
});

// Serve the operator development dashboard (My Operator Deals)
app.get("/operator-development-dashboard", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'operator-development-dashboard.html'));
});
app.get("/my-operator-deals", (req, res) => {
    res.redirect(302, "/operator-development-dashboard");
});

// Serve the My Brands page (Brand Development structure, lists all Airtable brands)
app.get("/all-brands-dashboard", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'all-brands-dashboard.html'));
});

// Serve the valuation widget
app.get("/valuation-widget", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'valuation-widget.html'));
});

// Serve the enhanced valuation widget
app.get("/valuation-widget-enhanced", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'valuation-widget-enhanced.html'));
});

// Serve the realistic valuation widget
app.get("/valuation-widget-realistic", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'valuation-widget-realistic.html'));
});

// Serve the flexible valuation widget
app.get("/valuation-widget-flexible", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'valuation-widget-flexible.html'));
});

// Serve the compact valuation widget
app.get("/valuation-widget-compact", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'valuation-widget-compact.html'));
});


// Serve the partner directory page
app.get("/partner-directory", (req, res) => {
    const filePath = path.join(__dirname, 'public', 'partner-directory.html');
    res.sendFile(filePath, (err) => {
        if (err) {
            console.error('Error serving partner-directory:', err);
            res.status(500).send('Error loading page: ' + err.message);
        }
    });
});

// Also handle with trailing slash
app.get("/partner-directory/", (req, res) => {
    const filePath = path.join(__dirname, 'public', 'partner-directory.html');
    res.sendFile(filePath);
});

// Outreach Hub pages (left nav dropdown: Plans, Inbox, Templates, Sequences, Analytics)
app.get("/outreach-plans", (req, res) => res.sendFile(path.join(__dirname, 'public', 'outreach-plans.html')));
app.get("/outreach-plans/", (req, res) => res.sendFile(path.join(__dirname, 'public', 'outreach-plans.html')));
app.get("/outreach-inbox", (req, res) => res.sendFile(path.join(__dirname, 'public', 'outreach-inbox.html')));
app.get("/outreach-inbox/", (req, res) => res.sendFile(path.join(__dirname, 'public', 'outreach-inbox.html')));
app.get("/outreach-templates", (req, res) => res.sendFile(path.join(__dirname, 'public', 'outreach-template-manager.html')));
app.get("/outreach-templates/", (req, res) => res.sendFile(path.join(__dirname, 'public', 'outreach-template-manager.html')));
app.get("/outreach-sequences", (req, res) => res.sendFile(path.join(__dirname, 'public', 'outreach-sequences.html')));
app.get("/outreach-sequences/", (req, res) => res.sendFile(path.join(__dirname, 'public', 'outreach-sequences.html')));
app.get("/outreach-analytics", (req, res) => res.sendFile(path.join(__dirname, 'public', 'outreach-analytics.html')));
app.get("/outreach-analytics/", (req, res) => res.sendFile(path.join(__dirname, 'public', 'outreach-analytics.html')));
app.get("/outreach-deal-activity-log", (req, res) => res.sendFile(path.join(__dirname, 'public', 'outreach-deal-activity-log.html')));
app.get("/outreach-deal-activity-log/", (req, res) => res.sendFile(path.join(__dirname, 'public', 'outreach-deal-activity-log.html')));

// Serve the LOI Database Dashboard
app.get("/loi-database-dashboard", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'loi-database-dashboard.html'));
});

// Serve the clause library pages
app.get("/clause-library", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'clause-library.html'));
});

app.get("/clause-library-clause", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'clause-library-clause.html'));
});

// Serve the franchise fee estimator
app.get("/franchise-fee-estimator", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'franchise-fee-estimator.html'));
});

// Serve the financial term library pages
app.get("/financial-term-library", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'financial-term-library.html'));
});

app.get("/financial-term-library-term", (req, res) => {
    // Disable caching for development
    res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
    res.setHeader('Pragma', 'no-cache');
    res.setHeader('Expires', '0');
    res.sendFile(path.join(__dirname, 'public', 'financial-term-library-term.html'));
});



// Basic routing/test endpoints for Railway + Webflow embedding verification
app.get("/health", (req, res) => res.json({ ok: true }));

// API endpoints
app.post("/api/intake/deal", dealIntake);
app.post("/api/intake/user", userIntake);
app.post("/api/signup", signup);
app.get("/api/signup/config", signupConfig);
app.post("/api/signup-terms-acceptance", signupTermsAcceptance);
app.post("/api/webhooks/memberstack", memberstackWebhook);

// Market Alerts API endpoints – live beta (Airtable-backed)
app.get("/api/dashboard/home", getDashboardHome);

app.get("/api/market-alerts", listMarketAlerts);
app.get("/api/market-alerts/rail", getMarketAlertsRail);
app.post("/api/market-alerts/:id/read", markAlertRead);
app.post("/api/market-alerts/:id/save", saveAlert);
app.post("/api/market-alerts/:id/dismiss", dismissAlert);
app.get("/api/market-alerts/news", getMarketAlertsNews);
app.get("/api/cron/market-alerts-rss-sync", cronMarketAlertsRssSync);
app.post("/api/cron/market-alerts-rss-sync", cronMarketAlertsRssSync);

// Deal Intelligence API endpoints
app.post("/api/deal-intelligence/analyze", analyzeDeal);

// Brand Presence API endpoints
app.get("/api/brand-presence/hotel/:recordId", getBrandPresenceHotelById);
app.get("/api/brand-presence", getBrandPresence);
app.get("/api/brand-presence/statistics", getBrandStatistics);
app.get("/api/brand-presence/white-space", getWhiteSpaceOpportunities);
app.get("/api/brand-presence/export", exportBrandPresenceData);
app.get("/api/brand-presence/location-types", getLocationTypes);
app.get("/api/brand-presence/parent-companies", getParentCompanies);
app.get("/api/brand-presence/brands", getBrands);
app.get("/api/brand-presence/chain-scales", getChainScales);

// Operator Intelligence (Census)
app.get("/api/operators-by-brand-region", getLargestOperatorsByBrandRegion);
app.get("/api/operators-by-brand-region/filters", getOperatorsByBrandRegionFilters);

// Travel Infrastructure API endpoints
app.get("/api/travel-infrastructure", getTravelInfrastructure);
app.get("/api/radar-map-points/travel-infrastructure", getRadarMapTravelInfrastructurePoints);
app.post("/api/radar-map-points/travel-infrastructure/import-preview", postTravelInfrastructureImportPreview);
app.post("/api/radar-map-points/travel-infrastructure/import-commit", postTravelInfrastructureImportCommit);

// Demand Anchors API endpoints
app.get("/api/demand-anchors", getDemandAnchors);
app.get("/api/radar-map-points/demand-anchors", getRadarMapDemandAnchorsPoints);
app.post("/api/radar-map-points/demand-anchors/import-preview", postDemandAnchorsImportPreview);
app.post("/api/radar-map-points/demand-anchors/import-commit", postDemandAnchorsImportCommit);

// CALA Radar Buildout
app.get("/api/radar-buildout/countries", getRadarBuildoutCountries);
app.get("/api/radar-buildout/countries/:country", getRadarBuildoutCountry);

// CALA submarket growth signals (owner/brand early-entry metadata)
app.get("/api/growth-signals/summary", getGrowthSignalsSummary);
app.get("/api/growth-signals/types", getGrowthSignalTypes);
app.get("/api/growth-signals", getGrowthSignalsFlat);
app.get("/api/growth-signals/profiles", getGrowthSignalsCountries);
app.get("/api/growth-signals/countries/:country", getGrowthSignalsCountryDetail);
app.get("/api/growth-signals/countries/:country/submarkets/:submarket", getGrowthSignalsSubmarket);

// Dealality Scout API endpoints (mock dataset until census-backed)
app.get("/api/dealality-scout", getDealalityScout);
app.get("/api/dealality-scout/filters", getDealalityScoutFilters);

// Scout Phase 1 — census-backed market coverage (read-only)
app.get("/api/scout/market-coverage", getScoutMarketCoverage);
app.get("/api/scout/market-map", getScoutMarketMap);
app.get("/api/scout/demand-overlays", getScoutDemandOverlays);
app.get("/api/scout/market-insights", getScoutMarketInsights);
app.get("/api/scout/insight-review", getScoutInsightReview);

// Scout Phase 2 — opportunity signals (read-only generation; saved metadata annotated)
app.post("/api/scout/opportunity-signals/save", postScoutOpportunitySignalSave);
app.get("/api/scout/opportunity-signals/saved", getScoutOpportunitySignalsSaved);
app.patch("/api/scout/opportunity-signals/:signalId", patchScoutOpportunitySignal);
app.get("/api/scout/opportunity-signals", getScoutOpportunitySignals);

// Brand Review API endpoints
app.get("/api/brand-review/deals", getBrandReviewDeals);
app.post("/api/brand-review/update-status", updateDealStatus);
app.get("/api/brand-review/deal-details", getDealDetails);
app.post("/api/brand-review/bulk-update", bulkUpdateDeals);
app.get("/api/brand-review/stats", getBrandReviewStats);
app.get("/api/brand-review/matched-brands", getMatchedBrands);

// Brand Fit Analyzer API endpoints
app.post("/api/brand-fit-analyzer", analyzeBrandFit);
app.get("/api/brand-fit-analyzer/deal", getDealBrandFit);
app.get("/api/brand-fit-analyzer/deals", getAllDealsForAnalysis);

// Clause Library API endpoints
app.get("/api/clause-library/clauses", getClauses);
app.get("/api/clause-library/clause", getClauseById);
app.get("/api/clause-library/clause-ids", getClauseIds);
app.get("/api/clause-library/variables", getClauseVariables);
app.post("/api/clause-library/clauses", createClause);

// Financial Term Library API endpoints
app.get("/api/financial-term-library/terms", getTerms);
app.get("/api/financial-term-library/term", getTermById);
app.get("/api/financial-term-library/term-ids", getTermIds);
app.post("/api/financial-term-library/terms", createTerm);

// Brand Explorer API (Deal Toolbox)
app.get("/api/brand-explorer/brands", listBrandExplorerBrands);
app.get("/api/brand-explorer/brand/:brand_key", getBrandExplorerBrand);
app.post("/api/brand-explorer/fit-to-deal", brandExplorerFitToDeal);

// Operator Explorer API
app.get("/api/operator-explorer/operators", listOperators);
app.get("/api/operator-explorer/operator", getOperatorById);

// Capital Provider Explorer API (Financing Hub)
app.get("/api/capital-provider-explorer", optionalDealalityAuth, handleCapitalProviderExplorer);
app.get("/api/capital-provider-explorer/providers", optionalDealalityAuth, listCapitalProviders);
app.get("/api/capital-provider-explorer/provider", optionalDealalityAuth, getCapitalProviderById);

// Brand Library API endpoints
app.get("/api/brand-library/operational-support", getOperationalSupportByBrandId);
app.get("/api/brand-library/brands", getBrandLibraryBrands);
app.get("/api/brand-library/brand", getBrandLibraryBrandById);
app.get("/api/brand-library/brand-status-options", getBrandStatusOptions);
app.patch("/api/brand-library/brand/:recordId", updateBrandBasicsById);
app.patch("/api/brand-library/brand/:recordId/sustainability-esg", updateSustainabilityEsgByBrandId);
app.patch("/api/brand-library/brand/:recordId/brand-footprint", updateBrandFootprintByBrandId);
app.patch("/api/brand-library/brand/:recordId/loyalty-commercial", updateLoyaltyCommercialByBrandId);
app.patch("/api/brand-library/brand/:recordId/project-fit", updateProjectFitByBrandId);
app.patch("/api/brand-library/brand/:recordId/portfolio-performance", updatePortfolioPerformanceByBrandId);
app.patch("/api/brand-library/brand/:recordId/brand-standards", updateBrandStandardsByBrandId);
app.patch("/api/brand-library/brand/:recordId/fee-structure", updateFeeStructureByBrandId);
app.patch("/api/brand-library/brand/:recordId/deal-terms", updateDealTermsByBrandId);
app.patch("/api/brand-library/brand/:recordId/operational-support", updateOperationalSupportByBrandId);
app.patch("/api/brand-library/brand/:recordId/legal-terms", updateLegalTermsByBrandId);
app.get("/api/brand-library/brands-grouped", getBrandsGroupedByParentCompany);

// Partner Directory API endpoints
app.get("/api/partner-directory", getPartners);
app.post("/api/partner-directory/users", createUser);
app.put("/api/partner-directory/users/:userId", updateUser);

// Partner Directory Favorites API endpoints
app.get("/api/partner-directory/favorites", getUserFavorites);
app.post("/api/partner-directory/favorites", createFavorite);
app.delete("/api/partner-directory/favorites/:favoriteId", deleteFavorite);
app.put("/api/partner-directory/favorites/:favoriteId", updateFavorite);

// Partner Intelligence — Source Library (Phase 3; admin auth required)
app.get("/api/partner-intelligence/pilot", partnerIntelligenceAuth, getPartnerIntelligencePilot);
app.get("/api/partner-intelligence/sources", partnerIntelligenceAuth, listPartnerIntelligenceSources);
app.get("/api/partner-intelligence/sources/:recordId", partnerIntelligenceAuth, getPartnerIntelligenceSourceById);
app.post("/api/partner-intelligence/sources", partnerIntelligenceAuth, createPartnerIntelligenceSource);
app.patch("/api/partner-intelligence/sources/:recordId", partnerIntelligenceAuth, patchPartnerIntelligenceSource);
app.post(
  "/api/partner-intelligence/sources/:recordId/upload",
  partnerIntelligenceAuth,
  resolvePartnerIntelligenceUploadDir,
  (req, res, next) => {
    partnerSourceUpload.single("file")(req, res, (err) => {
      if (err) {
        if (err.code === "LIMIT_FILE_SIZE") {
          return res.status(413).json({
            ok: false,
            success: false,
            error: "file_too_large",
            message: "File exceeds maximum upload size.",
          });
        }
        return res.status(400).json({
          ok: false,
          success: false,
          error: "upload_failed",
          message: err.message || "Upload failed.",
        });
      }
      return next();
    });
  },
  uploadPartnerIntelligenceSourceFile
);

// Partner Intelligence — extraction, review, publish
app.get(
  "/api/partner-intelligence/extraction/context",
  partnerIntelligenceAuth,
  getPartnerIntelligenceExtractionContext
);
app.post("/api/partner-intelligence/extraction/run", partnerIntelligenceAuth, postPartnerIntelligenceExtractionRun);
app.get("/api/partner-intelligence/facts", partnerIntelligenceAuth, listPartnerIntelligenceFacts);
app.get("/api/partner-intelligence/facts/:recordId", partnerIntelligenceAuth, getPartnerIntelligenceFactById);
app.patch(
  "/api/partner-intelligence/facts/:recordId/review",
  partnerIntelligenceAuth,
  patchPartnerIntelligenceFactReview
);
app.post("/api/partner-intelligence/publish", partnerIntelligenceAuth, postPartnerIntelligencePublish);

app.get("/api/brand-explorer/favorites", getBrandExplorerFavorites);
app.post("/api/brand-explorer/favorites", createBrandExplorerFavorite);
app.delete("/api/brand-explorer/favorites", deleteBrandExplorerFavorite);
app.delete("/api/brand-explorer/favorites/:favoriteId", deleteBrandExplorerFavorite);

app.get("/api/operator-explorer/favorites", getOperatorExplorerFavorites);
app.post("/api/operator-explorer/favorites", createOperatorExplorerFavorite);
app.delete("/api/operator-explorer/favorites", deleteOperatorExplorerFavorite);
app.delete("/api/operator-explorer/favorites/:favoriteId", deleteOperatorExplorerFavorite);

app.get("/api/capital-explorer/favorites", getCapitalExplorerFavorites);
app.post("/api/capital-explorer/favorites", createCapitalExplorerFavorite);
app.delete("/api/capital-explorer/favorites", deleteCapitalExplorerFavorite);
app.delete("/api/capital-explorer/favorites/:favoriteId", deleteCapitalExplorerFavorite);

// Partner Directory config endpoint (for local development)
app.get("/api/user-management", ...adminAuth, listUserManagementUsers);
app.get("/api/user-management/companies", ...adminAuth, listUserManagementCompanies);
app.post("/api/user-management", ...adminAuth, createUserManagementUser);
app.patch("/api/user-management/:recordId", ...adminAuth, updateUserManagementUser);
app.delete("/api/user-management/:recordId", ...adminAuth, deleteUserManagementUser);
app.post("/api/user-management/bulk-delete", ...adminAuth, bulkDeleteUsers);

// Acquisition Intelligence (internal/admin only — LinkedIn Connections CSV Stage 1)
app.post(
  "/api/acquisition-intelligence/connections/preview",
  ...adminAuth,
  (req, res, next) => {
    acquisitionConnectionsUpload.single("file")(req, res, (err) => {
      if (err) {
        if (err.code === "LIMIT_FILE_SIZE") {
          return res.status(413).json({
            ok: false,
            success: false,
            error: "file_too_large",
            message: "CSV exceeds 15 MB limit.",
          });
        }
        return res.status(400).json({
          ok: false,
          success: false,
          error: "upload_error",
          message: err.message || "Upload failed.",
        });
      }
      return next();
    });
  },
  postAcquisitionConnectionsPreview
);
app.post(
  "/api/acquisition-intelligence/connections/import",
  ...adminAuth,
  postAcquisitionConnectionsImport
);
app.get("/api/acquisition-intelligence/import-batches", ...adminAuth, getAcquisitionImportBatches);
app.get("/api/acquisition-intelligence/summary", ...adminAuth, getAcquisitionSummary);
app.post("/api/acquisition-intelligence/classify", ...adminAuth, postAcquisitionClassify);
app.get("/api/acquisition-intelligence/relationships", ...adminAuth, getAcquisitionRelationships);

app.get(
  "/api/support/owner-pilot-provisioning-runbook",
  ...internalRunbookAuth,
  getOwnerPilotProvisioningRunbookHandler
);
app.get(
  "/api/support/scoring-weight-model",
  ...internalRunbookAuth,
  getScoringWeightModelHandler
);
app.get(
  "/api/support/ai-visibility-benchmark-admin",
  ...internalRunbookAuth,
  getAiVisibilityBenchmarkAdminHandler
);
app.get(
  "/api/support/operator-fit-data-readiness",
  ...internalRunbookAuth,
  getOperatorFitDataReadinessHandler
);
app.get(
  "/api/support/operator-intelligence-calibration",
  ...internalRunbookAuth,
  getOperatorIntelligenceCalibrationHandler
);
app.get(
  "/api/support/operator-fit-internal-pilot/access",
  ...internalRunbookAuth,
  getOperatorFitInternalPilotAccess
);
app.get(
  "/api/support/operator-fit-internal-pilot/payload",
  ...internalRunbookAuth,
  getOperatorFitInternalPilotPayload
);
app.post(
  "/api/support/operator-fit-internal-pilot/events",
  ...internalRunbookAuth,
  postOperatorFitInternalPilotEvent
);
app.get(
  "/api/support/operator-fit-internal-pilot/:dealId/shortlist",
  ...internalRunbookAuth,
  getOperatorFitInternalPilotShortlist
);
app.post(
  "/api/support/operator-fit-internal-pilot/:dealId/shortlist",
  ...internalRunbookAuth,
  postOperatorFitInternalPilotShortlist
);
app.post(
  "/api/support/operator-fit-internal-pilot/shortlist/:id/remove",
  ...internalRunbookAuth,
  postOperatorFitInternalPilotShortlistRemove
);
app.post(
  "/api/support/operator-fit-internal-pilot/shortlist/:id/status",
  ...internalRunbookAuth,
  postOperatorFitInternalPilotShortlistStatus
);
app.post(
  "/api/support/operator-fit-internal-pilot/compare",
  ...internalRunbookAuth,
  postOperatorFitInternalPilotCompare
);
app.post(
  "/api/support/operator-fit-internal-pilot/ranking-difference",
  ...internalRunbookAuth,
  postOperatorFitInternalPilotRankingDifference
);
app.post(
  "/api/support/operator-fit-internal-pilot/ranking-change-validations",
  ...internalRunbookAuth,
  postOperatorFitInternalPilotRankingChangeValidations
);
app.get(
  "/api/support/operator-fit-internal-pilot/advisor-scorecards",
  ...internalRunbookAuth,
  getOperatorFitInternalPilotAdvisorScorecards
);
app.post(
  "/api/support/operator-fit-internal-pilot/advisor-scorecards",
  ...internalRunbookAuth,
  postOperatorFitInternalPilotAdvisorScorecard
);

app.get("/api/operator-match/scoring-config", getOperatorMatchScoringConfigHandler);
app.get("/js/generated/operator-match-scoring-config.js", getOperatorMatchScoringConfigBrowserScript);

// Partner Directory config endpoint (for local development)
app.get("/api/partner-directory/config", (req, res) => {
    if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
        return res.status(500).json({ 
            error: "Airtable credentials not configured on server. Please set AIRTABLE_API_KEY and AIRTABLE_BASE_ID environment variables." 
        });
    }
    res.json({
        AIRTABLE_BASE_ID: process.env.AIRTABLE_BASE_ID,
        COMPANY_PROFILE_TABLE_ID: 'tblItyfH6MlOnMKZ9',
        USERS_TABLE_ID: 'tbl6shiyz2wdUqE5F',
        USER_MANAGEMENT_TABLE_ID: process.env.USERS_TABLE_ID || 'tbl6shiyz2wdUqE5F',
        USER_FAVORITES_TABLE_ID: process.env.USER_FAVORITES_TABLE_ID || '', // Add your User Favorites table ID here
        BRAND_BASICS_TABLE_ID: process.env.BRAND_BASICS_TABLE_ID || 'tbl1x6S7I7JwTcRdV',
        MAX_RECORDS_PER_REQUEST: 100
    });
});

// Page routes (before express.static so paths without .html are matched)
app.get("/opportunity-review", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "opportunity-review.html"));
});
app.get("/opportunity-review/", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "opportunity-review.html"));
});
app.get("/largest-operators-by-brand", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'largest-operators-by-brand.html'));
});
app.get("/operator-intelligence-radar-with-list", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'operator-intelligence-radar-with-list.html'));
});
app.get("/operator-intelligence-radar-with-list/", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'operator-intelligence-radar-with-list.html'));
});
app.get("/deal-capture-radar", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'deal-capture-radar-standalone.html'));
});
app.get("/deal-capture-radar-with-ranked-list", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'deal-capture-radar-with-ranked-list.html'));
});
app.get("/dealality-scout", (req, res) => {
    res.sendFile(path.join(__dirname, "public", "dealality-scout.html"));
});
app.get("/dealality-scout/", (req, res) => {
    res.sendFile(path.join(__dirname, "public", "dealality-scout.html"));
});

// AO client engagements (not part of Dealality app) — e.g. AH Hospitality Advisors mockups
const AH_COMMERCIAL_HUB =
    "/engagements/ah-hospitality-advisors/commercial-performance-hub-mockup.html";
const AH_COMMERCIAL_HUB_PUBLIC = "/commercial-performance-hub/";
app.get("/commercial-performance-hub-mockup.html", (req, res) => res.redirect(302, AH_COMMERCIAL_HUB_PUBLIC));
app.get("/commercial-performance-hub-mockup", (req, res) => res.redirect(302, AH_COMMERCIAL_HUB_PUBLIC));
app.get("/commercial-performance-hub", (req, res) => res.redirect(302, AH_COMMERCIAL_HUB_PUBLIC));
app.use("/engagements", express.static(path.join(__dirname, "engagements")));

// Deal Capture landing and subpages (reviews, for-owners, etc.) at root
app.use(express.static(path.join(__dirname, 'deal-capture-landing-webflow')));

// Shared ESM modules (deal workspace pipeline, etc.)
app.use("/lib", express.static(path.join(__dirname, "lib")));

// Static files (public app pages, signup, etc.)
app.use(express.static(path.join(__dirname, 'public')));

// Legacy URL: list UI moved to combined Brand Explorer (app shell aliases /brand-library → /brand-explorer-combined).
app.get("/brand-library", (req, res) => {
    res.redirect(302, "/brand-explorer-combined");
});

// Serve the brand explorer page
app.get("/brand-explorer", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'brand-explorer.html'));
});
app.get("/brand-explorer/", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'brand-explorer.html'));
});

app.get("/brand-library-brand", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'brand-library-brand.html'));
});

app.get("/brand-library-compare", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'brand-library-compare.html'));
});

app.get("/operator-explorer", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'operator-explorer.html'));
});
app.get("/operator-explorer/", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'operator-explorer.html'));
});

app.get("/operator-explorer-detail", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'operator-explorer-detail.html'));
});

// Capital Explorer HTML — not shipped yet (API routes remain for backend work).

app.get("/operator-dna-profile", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'operator-dna-profile.html'));
});
app.get("/operator-dna-profile/", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'operator-dna-profile.html'));
});

app.get("/operator-explorer-gold-mock", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'operator-explorer-gold-mock.html'));
});
app.get("/operator-explorer-gold-mock/", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'operator-explorer-gold-mock.html'));
});

app.get("/brand-explorer-gold-mock", (req, res) => {
    res.sendFile(path.join(__dirname, "public", "brand-explorer-gold-mock.html"));
});
app.get("/brand-explorer-gold-mock/", (req, res) => {
    res.sendFile(path.join(__dirname, "public", "brand-explorer-gold-mock.html"));
});

app.get("/partner-intelligence-review", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'partner-intelligence-review.html'));
});
app.get("/partner-intelligence-review/", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'partner-intelligence-review.html'));
});
app.get("/landing-analytics-report", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'marketing', 'landing-analytics-report.html'));
});
app.get("/landing-analytics-report/", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'marketing', 'landing-analytics-report.html'));
});

// Legacy intake URL → Operator Setup (new two) — preserve ?recordId=… and &embed=… for edit prefill
app.get("/third-party-operator-intake", (req, res) => {
    const i = req.originalUrl.indexOf("?");
    const qs = i >= 0 ? req.originalUrl.slice(i) : "";
    res.redirect(302, "/third-party-operator-setup-new-two.html" + qs);
});

app.get("/third-party-operator-setup-sandbox", (req, res) => {
    res.sendFile(path.join(__dirname, "public", "third-party-operator-setup-sandbox.html"));
});
app.get("/third-party-operator-setup-sandbox/", (req, res) => {
    res.sendFile(path.join(__dirname, "public", "third-party-operator-setup-sandbox.html"));
});
app.get("/third-party-operator-setup-sandbox.html", (req, res) => {
    res.sendFile(path.join(__dirname, "public", "third-party-operator-setup-sandbox.html"));
});

// Request information endpoint
app.post("/api/request-info", async (req, res) => {
    try {
        const { email, firstName, lastName, company, country, message } = req.body;
        
        if (!email || !firstName || !lastName) {
            return res.status(400).json({ error: "Missing required fields" });
        }

        // Here you could integrate with your existing user intake system
        // or create a new table for information requests
        console.log("Information request received:", {
            email,
            firstName,
            lastName,
            company,
            country,
            message,
            timestamp: new Date().toISOString()
        });

        // For now, just return success
        res.json({ 
            success: true, 
            message: "Thank you for your interest! We'll be in touch soon." 
        });
        
    } catch (error) {
        console.error("Error processing information request:", error);
        res.status(500).json({ error: "Internal Server Error" });
    }
});

// Brand profile creation endpoint
app.post("/api/intake/brand", async (req, res) => {
    try {
        const secret = req.headers["x-intake-secret"];
        if (!secret || secret !== process.env.INTAKE_SHARED_SECRET) {
            return res.status(401).json({ error: "Unauthorized" });
        }

        const { 
            brandName, 
            brandType, 
            brandEmail, 
            brandPhone,
            contactFirstName, 
            contactLastName, 
            contactTitle,
            targetRegions,
            minRooms,
            maxRooms,
            budgetRange,
            dealCriteria,
            trackRecord
        } = req.body;

        if (!brandName || !brandEmail || !contactFirstName || !contactLastName) {
            return res.status(400).json({ error: "Missing required fields" });
        }

        // For now, just log the brand profile data
        // In production, you'd create a new Airtable table for brand profiles
        console.log("Brand profile created:", {
            brandName,
            brandType,
            brandEmail,
            contactFirstName,
            contactLastName,
            targetRegions,
            timestamp: new Date().toISOString()
        });

        res.json({ 
            success: true, 
            message: "Brand profile created successfully! We'll start sending you relevant deals." 
        });
        
    } catch (error) {
        console.error("Error creating brand profile:", error);
        res.status(500).json({ error: "Internal Server Error" });
    }
});

// ChatGPT Custom GPT — Dealality GTM Airtable (flexible fields; no delete)
app.post("/api/dealality-airtable/listDealalityTables", dealalityChatgptAuth, postListDealalityTables);
app.post("/api/dealality-airtable/listRecordsByTableId", dealalityChatgptAuth, postListRecordsByTableId);
app.post("/api/dealality-airtable/getRecordById", dealalityChatgptAuth, postGetRecordById);
app.post("/api/dealality-airtable/createRecordsByTableId", dealalityChatgptAuth, postCreateRecordsByTableId);
app.post("/api/dealality-airtable/updateRecordByTableId", dealalityChatgptAuth, postUpdateRecordByTableId);
app.post("/api/dealality-airtable/updateRecordsByTableId", dealalityChatgptAuth, postUpdateRecordsByTableId);
app.post("/api/dealality-airtable/summarizeRecordsByTableId", dealalityChatgptAuth, postSummarizeRecordsByTableId);

// API 404 fallback – must be after ALL API routes so /api/outreach-hub/*, /api/my-deals, etc. are matched first
app.use("/api", (req, res) => {
  res.status(404).json({ success: false, error: "API route not found" });
});

// 404 handler - serve custom 404 page for non-API routes
app.use((req, res, next) => {
  if (req.path.startsWith("/api/")) return next();
  res.status(404).sendFile(path.join(__dirname, 'public', '404.html'));
});

const PORT = process.env.PORT || 8080;

app.listen(PORT, () => {
  console.log(`✅ Server running at http://localhost:${PORT}`);
  try {
    logAdpPublishedReadSourceAtStartup();
  } catch (err) {
    console.error("[ADP read] startup source log failed:", err.message);
  }
  if (process.env.NODE_ENV !== "production") {
    let gitHead = "unavailable";
    try {
      const head = fs.readFileSync(path.join(__dirname, ".git", "HEAD"), "utf8").trim();
      if (head.startsWith("ref:")) {
        const refPath = path.join(__dirname, ".git", head.slice(4).trim());
        if (fs.existsSync(refPath)) {
          gitHead = fs.readFileSync(refPath, "utf8").trim().slice(0, 12);
        }
      } else {
        gitHead = head.slice(0, 12);
      }
    } catch {
      gitHead = "unavailable";
    }
    console.log("DEALALITY_REPO_ROOT:", __dirname);
    console.log("DEALALITY_GIT_HEAD:", gitHead);
  }
  // Optional quick check (only shows first chars, don't log secrets in prod)
  console.log("Airtable key present:", !!process.env.AIRTABLE_API_KEY);
  const smtpOk = !!(process.env.SMTP_HOST && process.env.SMTP_USER && process.env.SMTP_PASS);
  console.log("SMTP (signup emails):", smtpOk ? "configured — " + process.env.SMTP_HOST : "not configured — set SMTP_HOST, SMTP_USER, SMTP_PASS in .env");
  console.log("✅ Partner Directory routes registered:");
  console.log("   GET /partner-directory");
  console.log("   GET /api/partner-directory");
  console.log("   POST /api/partner-directory/users");
  console.log("   PUT /api/partner-directory/users/:userId");
  console.log("✅ Financial Term Library routes registered:");
  console.log("   GET /api/financial-term-library/terms");
  console.log("   GET /api/financial-term-library/term");
  console.log("   POST /api/financial-term-library/terms");
  console.log("✅ Company Profile routes registered:");
  console.log("   POST /api/company-profile  (multipart: fields + optional logo)");
  console.log("   PATCH /api/company-profile/:recordId");
  console.log("   GET /api/company-profile/prefill?recordId=rec...|companyName=...");
  console.log("   GET /api/company-profile/mine  (Memberstack JWT → linked Company Profile)");
  console.log("✅ Third-party operator list (My 3rd Party Ops.):");
  console.log("   GET /api/intake/third-party-operators");
  console.log("   GET /api/third-party-operators/list");
  console.log("   GET /api/third-party-operators");
  console.log("   GET /api/third-party-operators-new/list");
  console.log("   GET /api/third-party-operators-new");
  console.log("   GET /api/intake/third-party-operator/prefill-qa");
  console.log("✅ Operator Alignment Snapshot API (OAS):");
  console.log("   GET /api/operator-alignment-snapshot/:dealId/profile");
  console.log("   GET /api/operator-alignment-snapshot/:dealId/companies");
  startMarketAlertsRssScheduler();
});
