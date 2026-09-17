/**
 * Decision & Outcome API — shared across GDI / ADP.
 * Hotel-scoped; never returns cross-hotel private outcomes to clients.
 */

import {
  createDecision,
  getDecision,
  listDecisions,
  getSubjectDecision,
  recordValidation,
  recordAction,
  recordOutcome,
  getDecisionTimeline,
  getHotelDecisionMetrics,
  listAllHotelDecisionSummaries,
  PRODUCT_MODULE,
  ensureGdiOpportunityDecision,
  ensureAdpFindingDecision,
  getGdiPhase1Metrics,
  getAdpPhase1Metrics,
  getPersistenceMode,
} from "../lib/decision-outcomes/index.js";

function actorFromReq(req) {
  const u = req.dealalityUser || {};
  return {
    userId: u.email || u.id || u.memberId || null,
    role: u.role || u.dealalityRole || "OTHER",
    displayName: u.name || u.email || null,
  };
}

function requireHotelParam(req, res) {
  const hotelId = String(req.params.hotelId || req.body?.hotelId || "").trim();
  if (!hotelId) {
    res.status(400).json({ ok: false, error: "hotelId_required" });
    return null;
  }
  return hotelId;
}

export async function postDecision(req, res) {
  try {
    const body = req.body || {};
    const hotelId = String(body.hotelId || "").trim();
    if (!hotelId) {
      return res.status(400).json({ ok: false, error: "hotelId_required" });
    }
    const result = await createDecision({
      ...body,
      createdBy: actorFromReq(req),
    });
    return res.status(result.created ? 201 : 200).json({
      ok: true,
      created: result.created,
      decision: result.decision,
      persistence: result.persistence || getPersistenceMode(),
    });
  } catch (err) {
    console.error("[decision-outcomes] postDecision", err);
    return res.status(400).json({
      ok: false,
      error: err.code || "create_failed",
      message: err.message,
    });
  }
}

export async function getDecisionById(req, res) {
  const hotelId = String(req.query.hotelId || req.params.hotelId || "").trim();
  const decisionId = String(req.params.decisionId || "").trim();
  if (!hotelId || !decisionId) {
    return res
      .status(400)
      .json({ ok: false, error: "hotelId_and_decisionId_required" });
  }
  try {
    const bundle = await getDecision(hotelId, decisionId);
    if (!bundle) return res.status(404).json({ ok: false, error: "not_found" });
    return res.json({
      ok: true,
      ...bundle,
      persistence: getPersistenceMode(),
    });
  } catch (err) {
    if (err.code === "hotel_boundary_violation") {
      return res
        .status(403)
        .json({ ok: false, error: "hotel_boundary_violation" });
    }
    throw err;
  }
}

export async function getDecisionTimelineRoute(req, res) {
  const hotelId = String(req.query.hotelId || req.params.hotelId || "").trim();
  const decisionId = String(req.params.decisionId || "").trim();
  if (!hotelId || !decisionId) {
    return res
      .status(400)
      .json({ ok: false, error: "hotelId_and_decisionId_required" });
  }
  const bundle = await getDecisionTimeline(hotelId, decisionId);
  if (!bundle) return res.status(404).json({ ok: false, error: "not_found" });
  return res.json({ ok: true, ...bundle, persistence: getPersistenceMode() });
}

export async function postDecisionValidation(req, res) {
  const decisionId = String(req.params.decisionId || "").trim();
  const hotelId = String(req.body?.hotelId || req.query.hotelId || "").trim();
  if (!hotelId || !decisionId) {
    return res
      .status(400)
      .json({ ok: false, error: "hotelId_and_decisionId_required" });
  }
  try {
    const result = await recordValidation(hotelId, decisionId, {
      ...req.body,
      ...actorFromReq(req),
      sourceSurface: req.body?.sourceSurface || "api",
    });
    return res.status(201).json({ ok: true, ...result });
  } catch (err) {
    const status = err.code === "decision_not_found" ? 404 : 400;
    return res.status(status).json({
      ok: false,
      error: err.code || "validation_failed",
      message: err.message,
    });
  }
}

export async function postDecisionAction(req, res) {
  const decisionId = String(req.params.decisionId || "").trim();
  const hotelId = String(req.body?.hotelId || req.query.hotelId || "").trim();
  if (!hotelId || !decisionId) {
    return res
      .status(400)
      .json({ ok: false, error: "hotelId_and_decisionId_required" });
  }
  try {
    const result = await recordAction(hotelId, decisionId, {
      ...req.body,
      ...actorFromReq(req),
      sourceSurface: req.body?.sourceSurface || "api",
    });
    return res.status(201).json({ ok: true, ...result });
  } catch (err) {
    const status = err.code === "decision_not_found" ? 404 : 400;
    return res.status(status).json({
      ok: false,
      error: err.code || "action_failed",
      message: err.message,
    });
  }
}

export async function postDecisionOutcome(req, res) {
  const decisionId = String(req.params.decisionId || "").trim();
  const hotelId = String(req.body?.hotelId || req.query.hotelId || "").trim();
  if (!hotelId || !decisionId) {
    return res
      .status(400)
      .json({ ok: false, error: "hotelId_and_decisionId_required" });
  }
  try {
    const result = await recordOutcome(hotelId, decisionId, {
      ...req.body,
      ...actorFromReq(req),
      sourceSurface: req.body?.sourceSurface || "api",
    });
    return res.status(201).json({ ok: true, ...result });
  } catch (err) {
    const status = err.code === "decision_not_found" ? 404 : 400;
    return res.status(status).json({
      ok: false,
      error: err.code || "outcome_failed",
      message: err.message,
    });
  }
}

export async function getHotelDecisions(req, res) {
  const hotelId = requireHotelParam(req, res);
  if (!hotelId) return;
  const productModule = req.query.module || null;
  const rows = await listDecisions(hotelId, { productModule });
  const metrics = await getHotelDecisionMetrics(hotelId, { productModule });
  return res.json({
    ok: true,
    hotelId,
    decisions: rows,
    metrics,
    persistence: getPersistenceMode(),
  });
}

export async function getHotelDecisionMetricsRoute(req, res) {
  const hotelId = requireHotelParam(req, res);
  if (!hotelId) return;
  const productModule = req.query.module || null;
  return res.json({
    ok: true,
    metrics: await getHotelDecisionMetrics(hotelId, { productModule }),
    gdi: await getGdiPhase1Metrics(hotelId),
    adp: await getAdpPhase1Metrics(hotelId),
    persistence: getPersistenceMode(),
  });
}

export async function getSubjectDecisionRoute(req, res) {
  const hotelId = requireHotelParam(req, res);
  if (!hotelId) return;
  const subjectId = String(req.params.subjectId || "").trim();
  const productModule = String(req.query.module || PRODUCT_MODULE.GDI).trim();
  const bundle = await getSubjectDecision(hotelId, {
    productModule,
    subjectId,
    decisionType: req.query.decisionType || null,
  });
  if (!bundle) return res.status(404).json({ ok: false, error: "not_found" });
  return res.json({
    ok: true,
    ...bundle,
    persistence: getPersistenceMode(),
  });
}

/** Founder/admin audit list — still hotel-scoped rows; no cross-hotel private payload mixing in client UI beyond admin. */
export async function getAdminDecisionAudit(req, res) {
  const isAdmin =
    req.dealalityUser?.isAdmin ||
    req.dealalityUser?.role === "admin" ||
    req.query.founder === "1";
  if (!isAdmin && process.env.NODE_ENV === "production") {
    return res.status(403).json({ ok: false, error: "admin_required" });
  }
  const rows = await listAllHotelDecisionSummaries();
  return res.json({
    ok: true,
    count: rows.length,
    decisions: rows,
    persistence: getPersistenceMode(),
    note: "Admin/founder audit surface. Do not expose as client dashboard.",
  });
}

export async function postEnsureGdiDecision(req, res) {
  const hotelId = requireHotelParam(req, res);
  if (!hotelId) return;
  const opportunity = req.body?.opportunity;
  if (!opportunity?.id) {
    return res.status(400).json({ ok: false, error: "opportunity_required" });
  }
  try {
    const result = await ensureGdiOpportunityDecision({ hotelId, opportunity });
    return res.json({
      ok: true,
      ...result,
      persistence: getPersistenceMode(),
    });
  } catch (err) {
    console.error("[decision-outcomes] ensure-gdi", err);
    return res.status(500).json({
      ok: false,
      error: err.code || "ensure_gdi_failed",
      message: err.message,
    });
  }
}

export async function postEnsureAdpDecision(req, res) {
  let hotelId = requireHotelParam(req, res);
  if (!hotelId) return;
  try {
    const { resolveCanonicalHotelId } = await import(
      "../hotel-census/adp-gdi-canonical-identity.js"
    );
    const canonical = resolveCanonicalHotelId(hotelId);
    if (canonical) hotelId = canonical;
    const result = await ensureAdpFindingDecision({
      hotelId,
      findingId: req.body?.findingId,
      recommendation: req.body?.recommendation,
      recommendationSummary: req.body?.recommendationSummary,
      demandSegment: req.body?.demandSegment,
      evidenceSnapshotSummary: req.body?.evidenceSnapshotSummary,
      confidence: req.body?.confidence,
      recommendationVersion: req.body?.recommendationVersion,
    });
    return res.json({
      ok: true,
      hotelId,
      ...result,
      persistence: getPersistenceMode(),
    });
  } catch (err) {
    console.error("[decision-outcomes] ensure-adp", err);
    return res.status(500).json({
      ok: false,
      error: err.code || "ensure_adp_failed",
      message: err.message,
    });
  }
}
