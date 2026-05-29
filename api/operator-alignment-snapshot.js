/**
 * Operator Alignment Snapshot API.
 * GET /api/operator-alignment-snapshot/:dealId/profile — profile-level (Phase 1)
 * GET /api/operator-alignment-snapshot/:dealId/companies — company-level (Phase 4)
 *
 * Does not modify Brand Alignment Snapshot or Operator Capability Snapshot.
 */

import { fetchDealScoringContext } from "./my-deals.js";
import {
  mergeDealFieldsForAlignment,
  buildOperatorAlignmentProfileSnapshot,
} from "../lib/operator-alignment-profile-utils.js";
import { buildOperatorAlignmentCompaniesSnapshot } from "../lib/operator-alignment-company-utils.js";
import { normalizeOperatorAlignmentDealInputs } from "../lib/operator-alignment-deal-normalize.js";
import { buildOperatingPathDisplayLabel } from "../lib/operator-alignment-operating-path-label.js";

function strParam(v) {
  return v != null ? String(v).trim() : "";
}

/**
 * @param {import('express').Request} req
 * @param {import('express').Response} res
 */
export async function getOperatorAlignmentSnapshotProfile(req, res) {
  try {
    const dealId = strParam(req.params.dealId);
    if (!dealId || !dealId.startsWith("rec")) {
      return res.status(400).json({
        success: false,
        error: "Valid dealId (Airtable record id) is required",
      });
    }

    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) {
      return res.status(500).json({
        success: false,
        error: "Airtable credentials not configured",
      });
    }

    const ctx = await fetchDealScoringContext(baseId, apiKey, dealId);
    if (!ctx) {
      return res.status(404).json({ success: false, error: "Deal not found" });
    }

    const merged = mergeDealFieldsForAlignment(
      ctx.dealFields,
      ctx.locationData,
      ctx.siData,
      ctx.mpData
    );

    const payload = buildOperatorAlignmentProfileSnapshot(dealId, merged);
    const normalized = normalizeOperatorAlignmentDealInputs(
      ctx.dealFields,
      ctx.locationData,
      ctx.mpData,
      ctx.siData
    );
    payload.operatingPathLabel = buildOperatingPathDisplayLabel(normalized);

    return res.json({
      success: true,
      ...payload,
    });
  } catch (err) {
    console.error("[operator-alignment-snapshot profile GET]", err);
    return res.status(500).json({
      success: false,
      error: err.message || "Failed to build Operator Alignment Snapshot (profile)",
    });
  }
}

/**
 * @param {import('express').Request} req
 * @param {import('express').Response} res
 */
export async function getOperatorAlignmentSnapshotCompanies(req, res) {
  try {
    const dealId = strParam(req.params.dealId);
    if (!dealId || !dealId.startsWith("rec")) {
      return res.status(400).json({
        success: false,
        error: "Valid dealId (Airtable record id) is required",
      });
    }

    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) {
      return res.status(500).json({
        success: false,
        error: "Airtable credentials not configured",
      });
    }

    const ctx = await fetchDealScoringContext(baseId, apiKey, dealId);
    if (!ctx) {
      return res.status(404).json({ success: false, error: "Deal not found" });
    }

    const payload = await buildOperatorAlignmentCompaniesSnapshot(dealId, {
      dealFields: ctx.dealFields,
      locationData: ctx.locationData,
      mpData: ctx.mpData,
      siData: ctx.siData,
    });

    return res.json({
      success: true,
      ...payload,
    });
  } catch (err) {
    console.error("[operator-alignment-snapshot companies GET]", err);
    return res.status(500).json({
      success: false,
      error: err.message || "Failed to build Operating Companies for Consideration",
    });
  }
}
