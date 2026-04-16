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
import { analyzeDeal } from "./api/deal-intelligence.js";
import { getBrandPresence, getBrandStatistics, getWhiteSpaceOpportunities, exportBrandPresenceData, getLocationTypes, getParentCompanies, getBrands, getChainScales } from "./api/brand-presence.js";
import { getLargestOperatorsByBrandRegion, getOperatorsByBrandRegionFilters } from "./api/operators-by-brand-region.js";
import { getTravelInfrastructure } from "./api/travel-infrastructure.js";
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
import getThirdPartyOperatorMappingReport from "./api/third-party-operator-mapping-report.js";
import getThirdPartyOperatorPrefillQa from "./api/third-party-operator-prefill-qa.js";
import updateThirdPartyOperatorStatus from "./api/third-party-operator-status.js";
import signup from "./api/signup.js";
import { getPartners, createUser, updateUser } from "./api/partner-directory.js";
import { getUserFavorites, createFavorite, deleteFavorite, updateFavorite } from "./api/partner-directory-favorites.js";
import {
  createCompanyProfile,
  updateCompanyProfile,
  getCompanyProfilePrefill,
} from "./api/company-profile.js";
import {
  listUsers as listUserManagementUsers,
  createUser as createUserManagementUser,
  updateUser as updateUserManagementUser,
  deleteUser as deleteUserManagementUser,
  bulkDeleteUsers,
  listCompanies as listUserManagementCompanies,
} from "./api/user-management.js";
import { getMyDeals, getDealById, updateMyDealById, createDeal, addRecommendedBrand, getAlternativeBrands, getMatchScoreBreakdown, getOperatorMatchScoreBreakdown, refreshDealBrandCache, uploadDealAttachments, ALLOWED_ATTACHMENT_EXTENSIONS, MAX_ATTACHMENT_FILE_SIZE_BYTES } from "./api/my-deals.js";
import { getOutreachSetup, updateOutreachSetup, getOutreachDefault, updateOutreachDefault, deleteOutreachSetup } from "./api/outreach-setup.js";
import { getFranchiseApplication, updateFranchiseApplication } from "./api/franchise-application.js";
import { list as outreachHubList, get as outreachHubGet, create as outreachHubCreate, update as outreachHubUpdate, remove as outreachHubRemove } from "./api/outreach-hub.js";
import { getOutreachDealActivityLog } from "./api/outreach-deal-activity-log.js";
import { getDashboardHome } from "./api/dashboard-home.js";
import { getTargetList, addToTargetList, updateTarget, removeFromTargetList, batchRemoveFromTargetList, markAsDeleted, restoreFromDeleted } from "./api/target-list.js";
import { createRequest as createBrandDealRequest, listForBrand as listBrandDealRequests, listAll as listBrandDealRequestsAll, listForDeals as listBrandDealRequestsByDeals, listForDealsPost as listBrandDealRequestsByDealsPost, updateStatus as updateBrandDealRequestStatus, bulkUpdateStatus as bulkUpdateBrandDealRequestStatus, getActivityLog as getBrandDealActivityLog, getProposalDraft, submitProposal, getById as getBrandDealRequestById } from "./api/brand-deal-requests.js";
import { list as listDealRoomDocuments, listForBrandRequest as listDealRoomDocumentsForBrandRequest, create as createDealRoomDocument, update as updateDealRoomDocument, remove as deleteDealRoomDocument } from "./api/deal-room-documents.js";
import { getProposalsForDeal } from "./api/deal-compare.js";
import { listBrands as listBrandExplorerBrands, getBrand as getBrandExplorerBrand, fitToDeal as brandExplorerFitToDeal } from "./api/brand-explorer.js";
import { listOperators, getOperatorById } from "./api/operator-explorer.js";

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
  next();
}

const app = express();

// CORS so Webflow (and other origins) can call API from the browser
const CORS_ORIGIN = process.env.CORS_ORIGIN || "https://mvp-deal-capture.webflow.io";
app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", CORS_ORIGIN);
  res.setHeader("Access-Control-Allow-Methods", "GET, PATCH, POST, PUT, DELETE, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  if (req.method === "OPTIONS") return res.sendStatus(204);
  next();
});

// Security headers for deployment
app.use((req, res, next) => {
  res.setHeader("X-Content-Type-Options", "nosniff");
  res.setHeader("X-Frame-Options", "SAMEORIGIN");
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
app.patch("/api/company-profile/:recordId", updateCompanyProfile);
app.get("/api/company-profile/prefill", getCompanyProfilePrefill);

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

app.post("/api/intake/third-party-operator", handleThirdPartyOperatorIntake);
app.post("/api/third-party-operators/submit", handleThirdPartyOperatorIntake);
app.get("/api/intake/third-party-operator/mapping-report", getThirdPartyOperatorMappingReport);
app.get("/api/intake/third-party-operator/prefill-qa", getThirdPartyOperatorPrefillQa);
// Operator list for My 3rd Party Ops. dashboard (multiple paths for proxies / older clients)
app.get("/api/intake/third-party-operators/:recordId", getThirdPartyOperatorDetail);
app.get("/api/intake/third-party-operators", listThirdPartyOperators);
app.get("/api/third-party-operators/list", listThirdPartyOperators);
app.get("/api/third-party-operators", listThirdPartyOperators);
// Aliases for My Operators (new) clients / docs
app.get("/api/third-party-operators-new/list", listThirdPartyOperators);
app.get("/api/third-party-operators-new", listThirdPartyOperators);
app.patch("/api/intake/third-party-operators/:recordId/status", updateThirdPartyOperatorStatus);

// My Deals API (more specific routes first so /outreach-default and /outreach-setup are not treated as recordId)
app.get("/api/my-deals", getMyDeals);
app.post("/api/my-deals", createDeal);
app.get("/api/my-deals/outreach-default", getOutreachDefault);
app.patch("/api/my-deals/outreach-default", updateOutreachDefault);
app.get("/api/my-deals/:recordId/outreach-setup", getOutreachSetup);
app.patch("/api/my-deals/:recordId/outreach-setup", updateOutreachSetup);
app.delete("/api/my-deals/:recordId/outreach-setup", deleteOutreachSetup);
app.get("/api/franchise-application/:dealId", getFranchiseApplication);
app.patch("/api/franchise-application/:dealId", updateFranchiseApplication);
app.get("/api/my-deals/:recordId", getDealById);
app.get("/api/my-deals/:recordId/alternative-brands", getAlternativeBrands);
app.get("/api/my-deals/:recordId/match-score-breakdown", getMatchScoreBreakdown);
app.get("/api/my-deals/:recordId/operator-match-score-breakdown", getOperatorMatchScoreBreakdown);
app.patch("/api/my-deals/:recordId", updateMyDealById);
app.post("/api/my-deals/:recordId/add-recommended-brand", addRecommendedBrand);
app.post("/api/my-deals/:recordId/refresh-brand-cache", refreshDealBrandCache);
// Target List (brand shortlist) API
app.get("/api/target-list/:dealId", getTargetList);
app.post("/api/target-list", addToTargetList);
app.post("/api/target-list/batch-delete", batchRemoveFromTargetList);
app.post("/api/target-list/mark-deleted", markAsDeleted);
app.post("/api/target-list/restore", restoreFromDeleted);
app.patch("/api/target-list/:targetId", updateTarget);
app.delete("/api/target-list/:targetId", removeFromTargetList);
// Brand Deal Requests (Brand Development Dashboard)
app.post("/api/brand-deal-requests", createBrandDealRequest);
app.post("/api/brand-deal-requests/by-deals", listBrandDealRequestsByDealsPost);
app.get("/api/brand-deal-requests/activity", getBrandDealActivityLog);
app.get("/api/brand-deal-requests", (req, res) => {
  if (req.query.dealIds) return listBrandDealRequestsByDeals(req, res);
  const allParam = req.query.all;
  if (allParam === "1" || allParam === "true") return listBrandDealRequestsAll(req, res);
  return listBrandDealRequests(req, res);
});
app.get("/api/brand-deal-requests/:requestId", getBrandDealRequestById);
app.get("/api/brand-deal-requests/:requestId/proposal-draft", getProposalDraft);
app.post("/api/brand-deal-requests/:requestId/submit-proposal", submitProposal);
app.patch("/api/brand-deal-requests/:requestId", updateBrandDealRequestStatus);
app.post("/api/brand-deal-requests/bulk-update", bulkUpdateBrandDealRequestStatus);
// Deal Room Documents
app.get("/api/deal-room-documents", listDealRoomDocuments);
app.get("/api/deal-room-documents/brand/:requestId", listDealRoomDocumentsForBrandRequest);
app.post("/api/deal-room-documents", createDealRoomDocument);
app.patch("/api/deal-room-documents/:id", updateDealRoomDocument);
app.delete("/api/deal-room-documents/:id", deleteDealRoomDocument);
// Deal Setup attachments: multipart upload (multer); then business logic in uploadDealAttachments
app.post("/api/my-deals/:recordId/attachments", (req, res, next) => {
  dealAttachmentsUpload.array("files")(req, res, (err) => {
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
}, uploadDealAttachments);
// Serve stored attachment files (path-traversal safe)
app.get("/api/my-deals/:recordId/attachments/:filename", (req, res) => {
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

// Serve the unified app hub and brand setup BEFORE static so paths are not treated as static files
app.get("/app", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'app.html'));
});
app.get("/app/", (req, res) => {
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
app.get("/my-deals", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'my-deals.html'));
});
app.get("/my-deals/", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'my-deals.html'));
});

// CSP that explicitly sets connect-src so fetch/XHR aren't blocked (e.g. form or API calls)
const SIGNUP_CSP =
  "default-src 'self'; " +
  "connect-src 'self' https:; " +
  "script-src 'self' 'unsafe-inline' https://code.jquery.com https://cdn.jsdelivr.net; " +
  "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com https://cdn.jsdelivr.net https://cdn.prod.website-files.com; " +
  "font-src https://fonts.gstatic.com https://cdn.prod.website-files.com data:; " +
  "img-src 'self' data: https:;";

// Signup routes (before express.static so they are always matched and we set CSP)
app.get("/signup", (req, res) => {
    res.setHeader("Content-Security-Policy", SIGNUP_CSP);
    res.sendFile(path.join(__dirname, 'public', 'signup.html'));
});
app.get("/signup-temp", (req, res) => {
    res.setHeader("Content-Security-Policy", SIGNUP_CSP);
    res.sendFile(path.join(__dirname, 'public', 'signup-temp.html'));
});
app.get("/signup-temp.html", (req, res) => {
    res.setHeader("Content-Security-Policy", SIGNUP_CSP);
    res.sendFile(path.join(__dirname, 'public', 'signup-temp.html'));
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
    res.sendFile(path.join(__dirname, 'public', 'market-alerts.html'));
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

// Serve the deal brand fit analyzer
app.get("/deal-brand-fit-analyzer", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'deal-brand-fit-analyzer.html'));
});

app.get("/production-dashboard", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'webflow-brand-dashboard.html'));
});

// Serve the webflow brand dashboard directly
app.get("/webflow-brand-dashboard.html", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'webflow-brand-dashboard.html'));
});

// Serve the brand development dashboard
app.get("/brand-development-dashboard", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'brand-development-dashboard.html'));
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

// Market Alerts API endpoints – live beta (Airtable-backed)
app.get("/api/dashboard/home", getDashboardHome);

app.get("/api/market-alerts", listMarketAlerts);
app.get("/api/market-alerts/rail", getMarketAlertsRail);
app.post("/api/market-alerts/:id/read", markAlertRead);
app.post("/api/market-alerts/:id/save", saveAlert);
app.post("/api/market-alerts/:id/dismiss", dismissAlert);
app.get("/api/market-alerts/news", getMarketAlertsNews);

// Deal Intelligence API endpoints
app.post("/api/deal-intelligence/analyze", analyzeDeal);

// Brand Presence API endpoints
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

// Partner Directory config endpoint (for local development)
app.get("/api/user-management", listUserManagementUsers);
app.get("/api/user-management/companies", listUserManagementCompanies);
app.post("/api/user-management", createUserManagementUser);
app.patch("/api/user-management/:recordId", updateUserManagementUser);
app.delete("/api/user-management/:recordId", deleteUserManagementUser);
app.post("/api/user-management/bulk-delete", bulkDeleteUsers);

// Partner Directory config endpoint (for local development)
app.get("/api/partner-directory/config", (req, res) => {
    if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
        return res.status(500).json({ 
            error: "Airtable credentials not configured on server. Please set AIRTABLE_API_KEY and AIRTABLE_BASE_ID environment variables." 
        });
    }
    res.json({
        AIRTABLE_API_KEY: process.env.AIRTABLE_API_KEY,
        AIRTABLE_BASE_ID: process.env.AIRTABLE_BASE_ID,
        COMPANY_PROFILE_TABLE_ID: 'tblItyfH6MlOnMKZ9',
        USERS_TABLE_ID: 'tbl6shiyz2wdUqE5F',
        USER_MANAGEMENT_TABLE_ID: 'tblQEpYKf2aYNKKjw',
        USER_FAVORITES_TABLE_ID: process.env.USER_FAVORITES_TABLE_ID || '', // Add your User Favorites table ID here
        BRAND_BASICS_TABLE_ID: process.env.BRAND_BASICS_TABLE_ID || 'tbl1x6S7I7JwTcRdV',
        MAX_RECORDS_PER_REQUEST: 100
    });
});

// Page routes (before express.static so paths without .html are matched)
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

// Deal Capture landing and subpages (reviews, for-owners, etc.) at root
app.use(express.static(path.join(__dirname, 'deal-capture-landing-webflow')));

// Static files (public app pages, signup, etc.)
app.use(express.static(path.join(__dirname, 'public')));

// Serve the brand library pages
app.get("/brand-library", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'brand-library.html'));
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

app.get("/operator-explorer-gold-mock", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'operator-explorer-gold-mock.html'));
});
app.get("/operator-explorer-gold-mock/", (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'operator-explorer-gold-mock.html'));
});

// Legacy intake URL → Operator Setup (new two)
app.get("/third-party-operator-intake", (req, res) => {
    res.redirect(302, "/third-party-operator-setup-new-two.html");
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
  console.log("✅ Third-party operator list (My 3rd Party Ops.):");
  console.log("   GET /api/intake/third-party-operators");
  console.log("   GET /api/third-party-operators/list");
  console.log("   GET /api/third-party-operators");
  console.log("   GET /api/third-party-operators-new/list");
  console.log("   GET /api/third-party-operators-new");
  console.log("   GET /api/intake/third-party-operator/prefill-qa");
});
