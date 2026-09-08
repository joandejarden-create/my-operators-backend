/**
 * Packet 2.8B-2 — Owner Intelligence HTTP API.
 * Read-only. No Webhound. No Census writes.
 */

import { getDefaultOwnershipSurface } from "../lib/hotel-intelligence/ownership/ownership-surface-v1.js";

const surface = getDefaultOwnershipSurface();

function sendError(res, status, error, details = {}) {
  return res.status(status).json({ success: false, error, ...details });
}

export async function getOwnerIntelligenceMeta(req, res) {
  try {
    return res.json({ success: true, ...surface.meta() });
  } catch (err) {
    console.error("[owner-intelligence] meta", err);
    return sendError(res, 500, "owner_meta_failed");
  }
}

export async function listOwnerIntelligenceOwners(req, res) {
  try {
    return res.json({ success: true, ...surface.listOwners() });
  } catch (err) {
    console.error("[owner-intelligence] list", err);
    return sendError(res, 500, "owner_list_failed");
  }
}

export async function getOwnerIntelligenceOwner(req, res) {
  try {
    const ownerId = String(req.params.ownerId || "").trim();
    const result = surface.ownerGet({ owner_id: ownerId });
    if (!result.ok) return sendError(res, 404, result.error || "owner_not_found", result);
    return res.json({ success: true, ...result });
  } catch (err) {
    console.error("[owner-intelligence] get", err);
    return sendError(res, 500, "owner_get_failed");
  }
}

export async function getOwnerIntelligencePortfolio(req, res) {
  try {
    const ownerId = String(req.params.ownerId || "").trim();
    const result = surface.ownerPortfolio({
      owner_id: ownerId,
      focus_hotel_id: req.query.focusHotel || req.query.hotel || null,
    });
    if (!result.ok) return sendError(res, 404, result.error || "owner_portfolio_not_found", result);
    return res.json({ success: true, ...result });
  } catch (err) {
    console.error("[owner-intelligence] portfolio", err);
    return sendError(res, 500, "owner_portfolio_failed");
  }
}

export async function getOwnerIntelligenceRelated(req, res) {
  try {
    const ownerId = String(req.params.ownerId || "").trim();
    const result = surface.ownerRelatedEntities({
      owner_id: ownerId,
      entity_id: req.query.entity || null,
    });
    if (!result.ok) return sendError(res, 404, result.error || "owner_not_found", result);
    return res.json({ success: true, ...result });
  } catch (err) {
    console.error("[owner-intelligence] related", err);
    return sendError(res, 500, "owner_related_failed");
  }
}

export async function getOwnerIntelligenceHotels(req, res) {
  try {
    const ownerId = String(req.params.ownerId || "").trim();
    const result = surface.ownerHotels({
      owner_id: ownerId,
      bucket: req.query.bucket || "all",
    });
    if (!result.ok) return sendError(res, 404, result.error || "owner_not_found", result);
    return res.json({ success: true, ...result });
  } catch (err) {
    console.error("[owner-intelligence] hotels", err);
    return sendError(res, 500, "owner_hotels_failed");
  }
}

export async function getOwnerIntelligencePeople(req, res) {
  try {
    const ownerId = String(req.params.ownerId || "").trim();
    const result = surface.ownerPeople({ owner_id: ownerId });
    if (!result.ok) return sendError(res, 404, result.error || "owner_not_found", result);
    return res.json({ success: true, ...result });
  } catch (err) {
    console.error("[owner-intelligence] people", err);
    return sendError(res, 500, "owner_people_failed");
  }
}

export async function getOwnerIntelligenceSources(req, res) {
  try {
    const ownerId = String(req.params.ownerId || "").trim();
    const result = surface.ownerSources({ owner_id: ownerId });
    if (!result.ok) return sendError(res, 404, result.error || "owner_not_found", result);
    return res.json({ success: true, ...result });
  } catch (err) {
    console.error("[owner-intelligence] sources", err);
    return sendError(res, 500, "owner_sources_failed");
  }
}

export async function getHotelOwnerAnchor(req, res) {
  try {
    const hotelId = String(req.params.hotelId || "").trim();
    if (!hotelId) return sendError(res, 400, "hotel_id_required");
    const result = surface.hotelOwnerAnchor(hotelId);
    if (!result.ok) return sendError(res, 404, result.error || "anchor_not_found", result);
    return res.json({ success: true, ...result });
  } catch (err) {
    console.error("[owner-intelligence] hotel-anchor", err);
    return sendError(res, 500, "hotel_owner_anchor_failed");
  }
}
