/**
 * Operator Capability Snapshot — deal-level capability brief (v1: no operator matching).
 * GET  /api/deals/:dealId/operator-capability-snapshot
 * POST /api/ai/operator-capability-snapshot { dealId }
 */

import { fetchDealWithMergedLinkedRecords } from "./my-deals.js";
import { buildOperatorCapabilitySnapshotV1 } from "../lib/operator-capability-snapshot-build.js";

/**
 * @param {import('express').Request} req
 * @param {import('express').Response} res
 */
export async function getOperatorCapabilitySnapshot(req, res) {
  try {
    const dealId = strParam(req.params.dealId);
    if (!dealId) {
      return res.status(400).json({ success: false, error: "Valid dealId (Airtable record id) is required" });
    }

    const payload = await loadSnapshotPayload(dealId);
    if (!payload) {
      return res.status(404).json({ success: false, error: "Deal not found" });
    }
    return res.json(payload);
  } catch (err) {
    console.error("[operator-capability-snapshot GET]", err);
    return res.status(500).json({
      success: false,
      error: err.message || "Failed to build operator capability snapshot",
    });
  }
}

export async function postOperatorCapabilitySnapshot(req, res) {
  try {
    const dealId =
      req.body && typeof req.body.dealId === "string" ? req.body.dealId.trim() : "";
    if (!dealId || !dealId.startsWith("rec")) {
      return res.status(400).json({ success: false, error: "Valid dealId (Airtable record id) is required" });
    }

    const payload = await loadSnapshotPayload(dealId);
    if (!payload) {
      return res.status(404).json({ success: false, error: "Deal not found" });
    }
    return res.json(payload);
  } catch (err) {
    console.error("[operator-capability-snapshot POST]", err);
    return res.status(500).json({
      success: false,
      error: err.message || "Failed to build operator capability snapshot",
    });
  }
}

function strParam(v) {
  const s = typeof v === "string" ? v.trim() : "";
  return s && s.startsWith("rec") ? s : "";
}

async function loadSnapshotPayload(dealId) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) {
    throw new Error("Airtable credentials not configured");
  }

  const full = await fetchDealWithMergedLinkedRecords(baseId, apiKey, dealId);
  if (!full?.deal) return null;

  return buildOperatorCapabilitySnapshotV1(full.deal.fields || {}, dealId, {
    normalized: full.normalized,
  });
}
