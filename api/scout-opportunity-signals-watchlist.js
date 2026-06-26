/**
 * POST /api/scout/opportunity-signals/save
 * GET  /api/scout/opportunity-signals/saved
 * PATCH /api/scout/opportunity-signals/:signalId
 *
 * Writes only to Scout Opportunity Signals table (AIRTABLE_BASE_ID_ALT).
 */

import {
  saveOrUpdateSignal,
  listSavedSignals,
  patchSavedSignal,
} from "../lib/scout/scout-signal-watchlist.js";
import { ensurePlatformConfig } from "../lib/hotel-census/platform-base.js";

export async function postScoutOpportunitySignalSave(req, res) {
  if (!ensurePlatformConfig(res)) return;

  try {
    const { signal, reviewStatus, internalNotes, assignedTo } = req.body || {};

    if (!signal || !signal.signalId) {
      return res.status(400).json({
        success: false,
        error: "Request body must include signal object with signalId",
      });
    }

    const result = await saveOrUpdateSignal({
      signal,
      reviewStatus,
      internalNotes,
      assignedTo,
    });

    if (!result.ok) {
      const status = /required|Invalid/i.test(result.error || "") ? 400 : 500;
      return res.status(status).json({ success: false, error: result.error });
    }

    return res.json({
      success: true,
      status: result.status,
      recordId: result.recordId,
      signalId: result.signalId,
      reviewStatus: result.reviewStatus,
    });
  } catch (error) {
    console.error("[scout-opportunity-signals/save]", error);
    return res.status(500).json({
      success: false,
      error: "Internal Server Error",
      details: error.message,
    });
  }
}

export async function getScoutOpportunitySignalsSaved(req, res) {
  if (!ensurePlatformConfig(res)) return;

  try {
    const result = await listSavedSignals(req.query || {});
    if (!result.ok) {
      const status = /not found/i.test(result.error || "") ? 503 : 500;
      return res.status(status).json({ success: false, error: result.error });
    }

    return res.json({
      success: true,
      count: result.count,
      filters: result.filters,
      signals: result.signals,
      source: {
        base: "Deal Capture Platform",
        table: "Scout Opportunity Signals",
        readOnly: false,
        writes: false,
      },
    });
  } catch (error) {
    console.error("[scout-opportunity-signals/saved]", error);
    return res.status(500).json({
      success: false,
      error: "Internal Server Error",
      details: error.message,
    });
  }
}

export async function patchScoutOpportunitySignal(req, res) {
  if (!ensurePlatformConfig(res)) return;

  try {
    const signalId = (req.params.signalId || "").trim();
    if (!signalId) {
      return res.status(400).json({ success: false, error: "signalId path parameter is required" });
    }

    const body = req.body || {};
    const result = await patchSavedSignal(signalId, {
      reviewStatus: body.reviewStatus,
      internalNotes: body.internalNotes,
      assignedTo: body.assignedTo,
      createDeal: body.createDeal,
    });

    if (!result.ok) {
      const status = /not found|Invalid|No patchable/i.test(result.error || "") ? 400 : 500;
      return res.status(status).json({ success: false, error: result.error });
    }

    return res.json({
      success: true,
      status: result.status,
      recordId: result.recordId,
      signalId: result.signalId,
      updatedFields: result.updatedFields,
      saved: result.saved,
    });
  } catch (error) {
    console.error("[scout-opportunity-signals/patch]", error);
    return res.status(500).json({
      success: false,
      error: "Internal Server Error",
      details: error.message,
    });
  }
}
