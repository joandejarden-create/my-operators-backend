/**
 * Deal Opportunities API — HIF commercial intelligence for founder local preview.
 * CUSTOMER_BLOCKED. Read-only.
 */

import { loadDealOpportunities } from "../lib/hotel-intelligence/deal-opportunities/hif-deal-opportunities-reader-v1.js";

export async function getDealOpportunities(req, res) {
  try {
    const payload = await loadDealOpportunities();
    return res.json({
      success: true,
      ...payload,
    });
  } catch (err) {
    console.error("[deal-opportunities]", err?.message || err);
    return res.status(err?.status && err.status < 500 ? err.status : 500).json({
      success: false,
      error: "deal_opportunities_load_failed",
      message: err?.message || "Failed to load deal opportunities",
      publicationState: "CUSTOMER_BLOCKED",
    });
  }
}

export async function getDealOpportunityById(req, res) {
  try {
    const id = String(req.params.id || "").trim();
    if (!id) {
      return res.status(400).json({ success: false, error: "missing_id" });
    }
    const payload = await loadDealOpportunities();
    const hit = (payload.opportunities || []).find(
      (o) => o.id === id || o.airtableFindingId === id || o.airtableInventoryId === id
    );
    if (!hit) {
      return res.status(404).json({
        success: false,
        error: "not_found",
        publicationState: "CUSTOMER_BLOCKED",
      });
    }
    return res.json({
      success: true,
      publicationState: "CUSTOMER_BLOCKED",
      opportunity: hit,
      asOf: payload.asOf,
    });
  } catch (err) {
    console.error("[deal-opportunities:id]", err?.message || err);
    return res.status(500).json({
      success: false,
      error: "deal_opportunity_detail_failed",
      message: err?.message || "Failed to load opportunity",
    });
  }
}
