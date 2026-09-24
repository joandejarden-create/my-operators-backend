/**
 * Contact Intelligence V1 HTTP API.
 * Read-mostly. Paid enrichment disabled unless explicitly enabled + requested.
 */

import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  getDefaultContactIntelligenceService,
  createContactIntelligenceService,
  createContactStore,
  runContactBenchmark100,
  createDefaultBenchmarkResolver,
} from "../lib/hotel-intelligence/contact-intelligence/index.js";
import { LIVE_SLICE_TEN_HOTELS, LIVE_SLICE_LIMITATIONS } from "../lib/hotel-intelligence/contact-intelligence/live-slice-ten.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const LIVE_SLICE_REPORT = path.join(ROOT, "reports", "contact-intelligence-live-slice-ten-v1.1.json");

function sendError(res, status, error, details = {}) {
  return res.status(status).json({ success: false, error, ...details });
}

function serviceForRequest(req) {
  // Tests / ephemeral: allow CONTACT_INTELLIGENCE_DATA_ROOT override via env only.
  return getDefaultContactIntelligenceService();
}

export async function getContactIntelligenceMeta(req, res) {
  try {
    const svc = serviceForRequest(req);
    return res.json({ success: true, ...svc.meta() });
  } catch (err) {
    console.error("[contact-intelligence] meta", err);
    return sendError(res, 500, "contact_meta_failed");
  }
}

export async function getContactIntelligenceHotel(req, res) {
  try {
    const hotelId = String(req.params.hotelId || "").trim();
    if (!hotelId) return sendError(res, 400, "hotel_id_required");
    const audience = String(req.query.audience || "public").trim();
    const result = serviceForRequest(req).hotelGet({ hotel_id: hotelId, audience });
    if (!result.ok) return sendError(res, 404, result.error || "hotel_contact_unavailable", result);
    return res.json({ success: true, ...result });
  } catch (err) {
    console.error("[contact-intelligence] hotel", err);
    return sendError(res, 500, "contact_hotel_failed");
  }
}

export async function getContactIntelligenceOwner(req, res) {
  try {
    const ownerId = String(req.params.ownerId || "").trim();
    if (!ownerId) return sendError(res, 400, "owner_id_required");
    const audience = String(req.query.audience || "public").trim();
    const result = serviceForRequest(req).ownerGet({ owner_id: ownerId, audience });
    if (!result.ok) return sendError(res, 404, result.error || "owner_contact_unavailable", result);
    return res.json({ success: true, ...result });
  } catch (err) {
    console.error("[contact-intelligence] owner", err);
    return sendError(res, 500, "contact_owner_failed");
  }
}

export async function getContactIntelligenceSliceTen(req, res) {
  try {
    const audience = String(req.query.audience || "public").trim();
    const result = serviceForRequest(req).sliceTen({ audience });
    return res.json({ success: true, ...result });
  } catch (err) {
    console.error("[contact-intelligence] slice-ten", err);
    return sendError(res, 500, "contact_slice_ten_failed");
  }
}

export async function getContactIntelligenceBenchmark100(req, res) {
  try {
    const svc = serviceForRequest(req);
    const report = runContactBenchmark100({
      service: svc,
      resolveHotel: createDefaultBenchmarkResolver(svc),
      includeHeldOutDetail: String(req.query.detail || "") === "1",
    });
    return res.json({
      success: true,
      ...report,
      population_wide_enrichment_allowed: false,
    });
  } catch (err) {
    console.error("[contact-intelligence] benchmark-100", err);
    return sendError(res, 500, "contact_benchmark_failed");
  }
}

export async function getContactIntelligenceLiveSliceTen(req, res) {
  try {
    if (fs.existsSync(LIVE_SLICE_REPORT)) {
      const report = JSON.parse(fs.readFileSync(LIVE_SLICE_REPORT, "utf8"));
      return res.json({
        success: true,
        source: "reports/contact-intelligence-live-slice-ten-v1.1.json",
        ...report,
      });
    }
    return res.json({
      success: true,
      status: "NOT_RUN",
      hotels: LIVE_SLICE_TEN_HOTELS,
      limitations: LIVE_SLICE_LIMITATIONS,
      message:
        "Live slice not run yet. Execute: node scripts/contact-intelligence-live-slice-ten.mjs",
    });
  } catch (err) {
    console.error("[contact-intelligence] live-slice", err);
    return sendError(res, 500, "contact_live_slice_failed");
  }
}

export async function postContactIntelligenceHotelRefresh(req, res) {
  try {
    const hotelId = String(req.params.hotelId || "").trim();
    if (!hotelId) return sendError(res, 400, "hotel_id_required");
    const body = req.body || {};
    const result = serviceForRequest(req).refreshHotel({
      hotel_id: hotelId,
      paid: body.paid === true,
      provider_strategy: body.provider_strategy || "NATIVE",
      idempotency_token: body.idempotency_token || null,
      audience: body.audience || "public",
    });
    if (!result.ok && result.error === "paid_enrichment_disabled") {
      return sendError(res, 403, result.error, result);
    }
    if (!result.ok) return sendError(res, 400, result.error || "refresh_failed", result);
    return res.json({ success: true, ...result });
  } catch (err) {
    console.error("[contact-intelligence] refresh", err);
    return sendError(res, 500, "contact_refresh_failed");
  }
}

/**
 * Staging research workflow entry — hotel → owner → person → contact (opt-in).
 * Admin-authenticated. Discovery-only by default (no paid enrichment).
 * Persists a resumable research case. Never promotes canonical ownership/contact.
 * Client budgets/options cannot enable paid enrichment or override trusted deny.
 */
export async function postContactIntelligenceResearchWorkflow(req, res) {
  try {
    const hotelId = String(req.params.hotelId || "").trim();
    if (!hotelId) return sendError(res, 400, "hotel_id_required");
    const body = req.body || {};
    if (body.enable_contact_enrichment === true || body.paid === true) {
      return sendError(res, 403, "paid_enrichment_disabled_on_http", {
        message:
          "HTTP research-workflow is discovery/staging only. Paid enrichment requires explicit offline evaluation scripts.",
      });
    }

    const { runFullResearchWorkflow } = await import(
      "../lib/hotel-intelligence/contact-intelligence/full-research-workflow.js"
    );
    const { createResearchCaseStore } = await import(
      "../lib/hotel-intelligence/contact-intelligence/research-case-store.js"
    );
    const { buildAdminResearchWorkbenchView } = await import(
      "../lib/hotel-intelligence/contact-intelligence/admin-research-workbench.js"
    );

    // Body fields are hints only — HPC loader resolves authoritative facts by hotelId
    const hints = {
      hotel_name: body.hotel_name || body.hotel?.hotel_name || body.name,
      city: body.city || body.hotel?.city || null,
      country: body.country || body.hotel?.country || null,
      address: body.address || body.hotel?.address || null,
      aliases: body.aliases || body.hotel?.aliases || [],
      language: body.language || body.hotel?.language || null,
      requested_hotel_id: body.hotel_id || body.hotel?.hotel_id || null,
    };

    // Trusted execution: client cannot force network/paid. Offline/default deny.
    const allowNetwork = body.allow_network === true && process.env.CONTACT_INTELLIGENCE_ALLOW_HTTP_NETWORK === "1";
    const fixtureDeps =
      process.env.NODE_ENV !== "production" && typeof req._researchDeps === "object";
    const storeRoot =
      process.env.NODE_ENV !== "production" && req.headers["x-research-case-root"]
        ? String(req.headers["x-research-case-root"])
        : undefined;
    const store = createResearchCaseStore(storeRoot ? { root: storeRoot } : {});

    // Offline fixture path: allow small context_dev budget for injected search/scrape only.
    // Never raises serpapi/enrichment; never enables paid contact enrichment.
    const contextCap = allowNetwork ? 40 : fixtureDeps ? 40 : 0;
    const serpCap = allowNetwork ? 5 : 0;
    const fixtureBudgets = fixtureDeps
      ? {
          model_reader_usd_max: Number(body.budgets?.model_reader_usd_max ?? 0) || 0,
          apply_structured_reader: body.apply_structured_reader === true,
          apply_model_follow_up_planner: body.apply_model_follow_up_planner === true,
          iterative_ownership_loop: body.iterative_ownership_loop !== false,
          iterative_max_queries: Number(body.budgets?.iterative_max_queries ?? body.iterative_max_queries ?? 6) || 6,
          iterative_max_documents: Number(body.budgets?.iterative_max_documents ?? body.iterative_max_documents ?? 6) || 6,
        }
      : {};

    const result = await runFullResearchWorkflow(
      {
        hotel: { ...hints, hotel_id: hotelId },
        hotel_id: hotelId,
        // Admin resume of a known case must fail closed when missing.
        // New runs omit case_id (workflow allocates). Explicit resume uses resume_case_id.
        ...(body.resume_case_id || body.resume === true
          ? { resume_case_id: body.resume_case_id || body.case_id || null }
          : body.case_id
            ? { resume_case_id: body.case_id }
            : {}),
        objective: body.objective || "HOTEL_OWNERSHIP_CONTACT",
        force_refresh: body.force_refresh === true,
        ...(fixtureDeps ? { iterative_ownership_loop: body.iterative_ownership_loop !== false } : {}),
      },
      {
        serpapi_max: Math.min(Number(body.budgets?.serpapi_max ?? 0), serpCap),
        context_dev_max: Math.min(Number(body.budgets?.context_dev_max ?? 0), contextCap),
        enrichment_max: 0,
        enable_contact_enrichment: false,
        disable_network: !allowNetwork,
        ...fixtureBudgets,
      },
      {
        store,
        networkBlocked: !allowNetwork,
        enableContactEnrichment: false,
        // Test/fixture injection only when non-production
        ...(fixtureDeps ? req._researchDeps : {}),
      }
    );

    const workbench = buildAdminResearchWorkbenchView(result.case || result);
    return res.json({
      success: true,
      staging: true,
      canonical_writes: false,
      customer_publication: "BLOCKED",
      workbench,
      ...result,
    });
  } catch (err) {
    console.error("[contact-intelligence] research-workflow", err);
    return sendError(res, 500, "research_workflow_failed", {
      message: err.message || "research_workflow_failed",
    });
  }
}

export async function getContactIntelligenceResearchCase(req, res) {
  try {
    const caseId = String(req.params.caseId || "").trim();
    if (!caseId) return sendError(res, 400, "case_id_required");
    const { loadResearchCase } = await import(
      "../lib/hotel-intelligence/contact-intelligence/full-research-workflow.js"
    );
    const { createResearchCaseStore } = await import(
      "../lib/hotel-intelligence/contact-intelligence/research-case-store.js"
    );
    const { buildAdminResearchWorkbenchView } = await import(
      "../lib/hotel-intelligence/contact-intelligence/admin-research-workbench.js"
    );
    const storeRoot =
      process.env.NODE_ENV !== "production" && req.headers["x-research-case-root"]
        ? String(req.headers["x-research-case-root"])
        : undefined;
    const store = createResearchCaseStore(storeRoot ? { root: storeRoot } : {});
    const result = loadResearchCase(caseId, { store });
    if (!result.ok) return sendError(res, 404, result.error || "case_not_found");
    const workbench = buildAdminResearchWorkbenchView(result.case);
    return res.json({ success: true, staging: true, workbench, ...result });
  } catch (err) {
    console.error("[contact-intelligence] research-case", err);
    return sendError(res, 500, "research_case_failed");
  }
}

/**
 * Human review notes/decisions on a staged research case.
 * Never promotes to canonical ownership/contact fields.
 * Lock first, then load history under the lock; reviewer from auth context.
 */
export async function postContactIntelligenceResearchCaseReview(req, res) {
  try {
    const caseId = String(req.params.caseId || "").trim();
    if (!caseId) return sendError(res, 400, "case_id_required");
    const body = req.body || {};
    if (body.canonical_promotion === true || body.promote === true) {
      return sendError(res, 403, "canonical_promotion_forbidden", {
        message: "Workbench review cannot promote findings to canonical ownership/contact data.",
      });
    }
    const { loadResearchCase } = await import(
      "../lib/hotel-intelligence/contact-intelligence/full-research-workflow.js"
    );
    const { createResearchCaseStore } = await import(
      "../lib/hotel-intelligence/contact-intelligence/research-case-store.js"
    );
    const {
      buildHumanReviewUpdate,
      buildAdminResearchWorkbenchView,
      resolveTrustedReviewer,
    } = await import(
      "../lib/hotel-intelligence/contact-intelligence/admin-research-workbench.js"
    );
    const storeRoot =
      process.env.NODE_ENV !== "production" && req.headers["x-research-case-root"]
        ? String(req.headers["x-research-case-root"])
        : undefined;
    const store = createResearchCaseStore(storeRoot ? { root: storeRoot } : {});

    const lock = store.tryAcquireLock(caseId, 15000);
    if (!lock.ok) return sendError(res, 409, "case_locked", { lock: lock.lock });
    try {
      const loaded = loadResearchCase(caseId, { store });
      if (!loaded.ok) return sendError(res, 404, loaded.error || "case_not_found");

      const trustedReviewer = resolveTrustedReviewer(req.dealalityUser);
      const updated = buildHumanReviewUpdate(body, loaded.case.human_review || null, {
        trustedReviewer,
      });
      if (!updated.ok) return sendError(res, 400, updated.error, { allowed: updated.allowed });

      const put = store.putCase(
        {
          case_id: caseId,
          human_review: updated.human_review,
          journal_entry: {
            kind: "human_review",
            event: "review_recorded",
            decision: updated.human_review.latest.decision,
            reviewer: trustedReviewer,
            canonical_promotion: false,
          },
        },
        { lock_token: lock.lock.lock_token }
      );
      if (!put.ok) return sendError(res, 409, put.reason || "review_write_failed");
      const workbench = buildAdminResearchWorkbenchView(put.case);
      return res.json({
        success: true,
        staging: true,
        canonical_writes: false,
        customer_publication: "BLOCKED",
        human_review: updated.human_review,
        workbench,
        case: put.case,
      });
    } finally {
      store.releaseLock(caseId, lock.lock?.lock_token);
    }
  } catch (err) {
    console.error("[contact-intelligence] research-case-review", err);
    return sendError(res, 500, "research_case_review_failed", {
      message: err.message || "research_case_review_failed",
    });
  }
}

/** Test helper — ephemeral service rooted in tmp (not used by production routes). */
export function createEphemeralContactIntelligenceService() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "ci-v1-"));
  const store = createContactStore({ root });
  return createContactIntelligenceService({ store, forceNew: true });
}
