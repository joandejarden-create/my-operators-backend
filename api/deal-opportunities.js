/**
 * Deal Opportunities API — HIF commercial intelligence for founder local preview.
 * CUSTOMER_BLOCKED. Read-only.
 *
 * Optional founderPreview=1 (authorized admin / local Dealality admin only)
 * promotes the Critical-Mass permanent-rule Qualified baseline for internal
 * review without changing the permanent classifier or customer publication.
 */

import { loadDealOpportunities } from "../lib/hotel-intelligence/deal-opportunities/hif-deal-opportunities-reader-v1.js";
import {
  applyDealOpportunitiesFounderPreview,
  isAuthorizedForDealOpportunitiesFounderPreview,
  isDealOpportunitiesFounderPreviewRequested,
} from "../lib/hotel-intelligence/deal-opportunities/hif-deal-opportunities-founder-preview-v1.js";

function recomputeMetrics(list) {
  const qualified = list.filter((o) => o.group === "qualified");
  const watch = list.filter((o) => o.group === "watch");
  const bct = qualified.filter((o) => o.kind === "BCT");
  const mct = qualified.filter((o) => o.kind === "MCT");
  const countryCodes = new Set(
    qualified.map((o) => String(o.country || "").toUpperCase()).filter(Boolean),
  );
  const propertyIds = new Set();
  for (const o of qualified) {
    if (o.relatedHotels?.length) {
      o.relatedHotels.forEach((h) =>
        propertyIds.add(h.inventoryId || h.discoveryPropertyId),
      );
    } else if (o.airtableInventoryId) {
      propertyIds.add(o.airtableInventoryId);
    }
  }
  return {
    qualifiedDecisions: qualified.length,
    brandCount: bct.length,
    managementCount: mct.length,
    propertiesRepresented: propertyIds.size,
    countriesCovered: countryCodes.size,
    countryLabels: [...countryCodes],
    actionReadyContacts: 0,
    emergingWatchlist: watch.length,
    allIntelligence: list.length,
  };
}

function applyPreviewIfAuthorized(req, payload) {
  const requested = isDealOpportunitiesFounderPreviewRequested(req.query || {});
  const authorized = isAuthorizedForDealOpportunitiesFounderPreview(
    req.dealalityUser,
    req,
  );
  const { opportunities, preview } = applyDealOpportunitiesFounderPreview(
    payload.opportunities || [],
    { requested, authorized },
  );
  const next = {
    ...payload,
    opportunities,
    founderPreview: preview,
    publicationState: "CUSTOMER_BLOCKED",
  };
  if (preview.enabled) {
    next.metrics = {
      ...(payload.metrics || {}),
      ...recomputeMetrics(opportunities),
      founderPreviewPromoted: preview.promotedFindingIds.length,
      founderPreviewMatchedHotels: preview.matchedHotels.length,
      founderPreviewMissingHotels: preview.missingHotels,
    };
  }
  return next;
}

export async function getDealOpportunities(req, res) {
  try {
    const payload = await loadDealOpportunities();
    const next = applyPreviewIfAuthorized(req, payload);
    return res.json({
      success: true,
      ...next,
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
    const next = applyPreviewIfAuthorized(req, payload);
    const hit = (next.opportunities || []).find(
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
      asOf: next.asOf,
      founderPreview: next.founderPreview,
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
