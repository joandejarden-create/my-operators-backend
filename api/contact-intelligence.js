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

/** Test helper — ephemeral service rooted in tmp (not used by production routes). */
export function createEphemeralContactIntelligenceService() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "ci-v1-"));
  const store = createContactStore({ root });
  return createContactIntelligenceService({ store, forceNew: true });
}
