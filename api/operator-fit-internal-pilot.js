/**
 * Operator Fit Internal Pilot API — admin + allowlist gated.
 * Does not wire owner My Deals. Does not use ODR as shortlist.
 */

import { evaluateOperatorFitInternalPilotAccess } from "../lib/operator-fit/internal-pilot-access.js";
import { evaluateOperatorFitForDeal } from "../lib/operator-fit/evaluate-deal.js";
import { explainRankingDifference } from "../lib/operator-fit/ranking-difference.js";
import { listRankingChangeValidations } from "../lib/operator-fit/ranking-change-validations.js";
import { buildShortlistComparison } from "../lib/operator-fit/shortlist-compare.js";
import {
  createShortlistEntry,
  listShortlistForDeal,
  removeShortlistEntry,
  updateShortlistStatus,
  withCurrentVsSnapshot,
  SHORTLIST_STATUS,
} from "../lib/operator-fit/shortlist-store.js";
import { recordPilotEvent, PILOT_EVENT_TYPES } from "../lib/operator-fit/pilot-events.js";
import {
  upsertAdvisorScorecard,
  loadAdvisorScorecards,
  aggregateAdvisorScorecards,
} from "../lib/operator-fit/advisor-scorecards.js";
import { OPERATOR_FIT_ENGINE_VERSION } from "../lib/operator-fit/feature-flag.js";
import { readFileSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const PILOT_PAYLOAD = join(ROOT, "reports", "operator-fit-internal-pilot-ui-payload.json");
const FINAL_PILOT_PAYLOAD = join(
  ROOT,
  "reports",
  "operator-fit-final-internal-pilot-ui-payload.json"
);

function json(res, status, body) {
  res.statusCode = status;
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.end(JSON.stringify(body));
}

function userFromReq(req) {
  const u = req.dealalityUser || req.user || {};
  return {
    email: u.email || req.member?.auth?.email || null,
    isAdmin: Boolean(u.isAdmin || u.dealality?.isAdmin || u.flags?.isAdmin),
    flags: u.flags || u.dealality?.flags || {},
    companyName: u.companyName || u.company || null,
  };
}

function gate(req, res, { dealId = null, requireDeal = false } = {}) {
  const access = evaluateOperatorFitInternalPilotAccess({
    user: userFromReq(req),
    dealId,
    requireDeal,
  });
  if (!access.allowed) {
    json(res, 403, {
      success: false,
      error: "internal_pilot_forbidden",
      reasons: access.reasons,
      engineVersion: access.engineVersion,
      ownerMyDealsWouldExpose: access.ownerMyDealsWouldExpose,
    });
    return null;
  }
  return access;
}

/**
 * GET /api/support/operator-fit-internal-pilot/access
 */
export async function getOperatorFitInternalPilotAccess(req, res) {
  try {
    const dealId = String(req.query?.dealId || "").trim() || null;
    const access = evaluateOperatorFitInternalPilotAccess({
      user: userFromReq(req),
      dealId,
      requireDeal: Boolean(dealId),
    });
    return json(res, 200, {
      success: true,
      ...access,
      eventTypes: PILOT_EVENT_TYPES,
      shortlistStatuses: Object.values(SHORTLIST_STATUS),
      myDealsWired: false,
      odrIsNotShortlist: true,
    });
  } catch (err) {
    console.error("[of-internal-pilot] access", err);
    return json(res, 500, { success: false, error: "access_check_failed" });
  }
}

/**
 * GET /api/support/operator-fit-internal-pilot/payload
 * Serves precomputed internal pilot UI payload (evaluate script).
 */
export async function getOperatorFitInternalPilotPayload(req, res) {
  try {
    if (!gate(req, res, { requireDeal: false })) return;
    const pack = String(req.query?.pack || "").toLowerCase();
    const path = pack === "final" && existsSync(FINAL_PILOT_PAYLOAD) ? FINAL_PILOT_PAYLOAD : PILOT_PAYLOAD;
    if (!existsSync(path)) {
      return json(res, 404, {
        success: false,
        error: "Run operator-fit-internal-pilot-evaluate and/or operator-fit-final-internal-pilot-evaluate",
      });
    }
    const payload = JSON.parse(readFileSync(path, "utf8"));
    recordPilotEvent({
      event: "results_viewed",
      dealId: req.query?.dealId || null,
      internalUser: "internal_admin",
      meta: { source: "payload" },
    });
    return json(res, 200, {
      success: true,
      mode: "internal_pilot",
      myDealsWired: false,
      ...payload,
    });
  } catch (err) {
    console.error("[of-internal-pilot] payload", err);
    return json(res, 500, { success: false, error: "payload_failed" });
  }
}

/**
 * POST /api/support/operator-fit-internal-pilot/events
 */
export async function postOperatorFitInternalPilotEvent(req, res) {
  try {
    if (!gate(req, res, { requireDeal: false })) return;
    const body = typeof req.body === "object" && req.body ? req.body : {};
    const result = recordPilotEvent({
      event: body.event,
      dealId: body.dealId,
      operatorId: body.operatorId,
      internalUser: "internal_admin",
      engineVersion: OPERATOR_FIT_ENGINE_VERSION,
      meta: body.meta,
    });
    if (!result.ok) return json(res, 400, { success: false, ...result });
    return json(res, 200, { success: true, ...result });
  } catch (err) {
    console.error("[of-internal-pilot] event", err);
    return json(res, 500, { success: false, error: "event_failed" });
  }
}

/**
 * GET /api/support/operator-fit-internal-pilot/:dealId/shortlist
 */
export async function getOperatorFitInternalPilotShortlist(req, res) {
  try {
    const dealId = String(req.params.dealId || "").trim();
    if (!gate(req, res, { dealId, requireDeal: true })) return;
    const rows = listShortlistForDeal(dealId).map((r) => withCurrentVsSnapshot(r, {}));
    return json(res, 200, {
      success: true,
      dealId,
      shortlist: rows,
      notOdr: true,
      statuses: Object.values(SHORTLIST_STATUS),
    });
  } catch (err) {
    console.error("[of-internal-pilot] shortlist get", err);
    return json(res, 500, { success: false, error: "shortlist_get_failed" });
  }
}

/**
 * POST /api/support/operator-fit-internal-pilot/:dealId/shortlist
 */
export async function postOperatorFitInternalPilotShortlist(req, res) {
  try {
    const dealId = String(req.params.dealId || "").trim();
    if (!gate(req, res, { dealId, requireDeal: true })) return;
    const body = typeof req.body === "object" && req.body ? req.body : {};
    const entry = createShortlistEntry({
      dealId,
      dealLabel: body.dealLabel || dealId,
      operatorId: body.operatorId,
      operatorRecordId: String(body.operatorId || "").startsWith("rec") ? body.operatorId : null,
      operatorName: body.operatorName,
      brand: body.brand,
      candidateType: body.candidateType || body.lifecycle || "Third-party operator",
      operatingStructure: body.operatingStructure,
      shortlistedBy: "internal_admin",
      advisorNote: body.advisorNote || "",
      alignment: body.alignment,
      confidence: body.confidence,
      coverage: body.coverage,
      eligibility: body.eligibility,
      readiness: body.readiness,
      lifecycle: body.lifecycle,
      reasons: body.reasons || [],
      concerns: body.concerns || [],
      unknowns: body.unknowns || [],
      engineVersion: OPERATOR_FIT_ENGINE_VERSION,
    });
    recordPilotEvent({
      event: "operator_shortlisted",
      dealId,
      operatorId: body.operatorId,
      internalUser: "internal_admin",
    });
    return json(res, 200, {
      success: true,
      shortlist: entry,
      outreachCreated: false,
      odrCreated: false,
    });
  } catch (err) {
    console.error("[of-internal-pilot] shortlist post", err);
    return json(res, 500, { success: false, error: "shortlist_create_failed" });
  }
}

/**
 * POST /api/support/operator-fit-internal-pilot/shortlist/:id/remove
 */
export async function postOperatorFitInternalPilotShortlistRemove(req, res) {
  try {
    const id = String(req.params.id || "").trim();
    const body = typeof req.body === "object" && req.body ? req.body : {};
    const dealId = String(body.dealId || "").trim();
    if (!gate(req, res, { dealId, requireDeal: true })) return;
    const entry = removeShortlistEntry(id, {
      removedBy: "internal_admin",
      reason: body.reason || "",
    });
    if (!entry) return json(res, 404, { success: false, error: "not_found" });
    recordPilotEvent({
      event: "operator_removed",
      dealId,
      operatorId: entry.operatorId,
      internalUser: "internal_admin",
      meta: { reason: body.reason || "" },
    });
    return json(res, 200, { success: true, shortlist: entry });
  } catch (err) {
    console.error("[of-internal-pilot] shortlist remove", err);
    return json(res, 500, { success: false, error: "shortlist_remove_failed" });
  }
}

/**
 * POST /api/support/operator-fit-internal-pilot/shortlist/:id/status
 */
export async function postOperatorFitInternalPilotShortlistStatus(req, res) {
  try {
    const id = String(req.params.id || "").trim();
    const body = typeof req.body === "object" && req.body ? req.body : {};
    const dealId = String(body.dealId || "").trim();
    if (!gate(req, res, { dealId, requireDeal: true })) return;
    const entry = updateShortlistStatus(id, body.status, { advisorNote: body.advisorNote });
    if (!entry) return json(res, 404, { success: false, error: "not_found" });
    return json(res, 200, { success: true, shortlist: entry });
  } catch (err) {
    console.error("[of-internal-pilot] shortlist status", err);
    return json(res, 400, { success: false, error: err?.message || "status_failed" });
  }
}

/**
 * POST /api/support/operator-fit-internal-pilot/compare
 */
export async function postOperatorFitInternalPilotCompare(req, res) {
  try {
    if (!gate(req, res, { requireDeal: false })) return;
    const body = typeof req.body === "object" && req.body ? req.body : {};
    const candidates = Array.isArray(body.candidates) ? body.candidates.slice(0, 4) : [];
    const comparison = buildShortlistComparison(candidates, body.project || null);
    recordPilotEvent({
      event: "comparison_opened",
      dealId: body.dealId || null,
      internalUser: "internal_admin",
      meta: { count: candidates.length },
    });
    return json(res, 200, { success: true, comparison });
  } catch (err) {
    console.error("[of-internal-pilot] compare", err);
    return json(res, 500, { success: false, error: "compare_failed" });
  }
}

/**
 * POST /api/support/operator-fit-internal-pilot/ranking-difference
 */
export async function postOperatorFitInternalPilotRankingDifference(req, res) {
  try {
    if (!gate(req, res, { requireDeal: false })) return;
    const body = typeof req.body === "object" && req.body ? req.body : {};
    const result = explainRankingDifference(body.a || {}, body.b || {}, {
      maxDrivers: body.maxDrivers || 5,
    });
    return json(res, 200, { success: true, ...result });
  } catch (err) {
    console.error("[of-internal-pilot] ranking-difference", err);
    return json(res, 500, { success: false, error: "diff_failed" });
  }
}

/**
 * POST /api/support/operator-fit-internal-pilot/ranking-change-validations
 */
export async function postOperatorFitInternalPilotRankingChangeValidations(req, res) {
  try {
    if (!gate(req, res, { requireDeal: false })) return;
    const body = typeof req.body === "object" && req.body ? req.body : {};
    const items = listRankingChangeValidations(body.project || {}, body.operator || {});
    recordPilotEvent({
      event: "validation_question_viewed",
      dealId: body.dealId || null,
      operatorId: body.operator?.operatorId || body.operatorId || null,
      internalUser: "internal_admin",
    });
    return json(res, 200, { success: true, items });
  } catch (err) {
    console.error("[of-internal-pilot] validations", err);
    return json(res, 500, { success: false, error: "validations_failed" });
  }
}

/**
 * GET /api/support/operator-fit-internal-pilot/advisor-scorecards
 */
export async function getOperatorFitInternalPilotAdvisorScorecards(req, res) {
  try {
    if (!gate(req, res, { requireDeal: false })) return;
    const store = loadAdvisorScorecards();
    return json(res, 200, {
      success: true,
      mutatesAlgorithmScores: false,
      aggregate: aggregateAdvisorScorecards(),
      scorecards: store.scorecards || [],
    });
  } catch (err) {
    console.error("[of-internal-pilot] scorecards get", err);
    return json(res, 500, { success: false, error: "scorecards_get_failed" });
  }
}

/**
 * POST /api/support/operator-fit-internal-pilot/advisor-scorecards
 * Persists advisor assessment — never writes Fit algorithm scores.
 */
export async function postOperatorFitInternalPilotAdvisorScorecard(req, res) {
  try {
    if (!gate(req, res, { requireDeal: false })) return;
    const body = typeof req.body === "object" && req.body ? req.body : {};
    const card = upsertAdvisorScorecard({
      ...body,
      advisorRole: "internal_advisor",
    });
    return json(res, 200, {
      success: true,
      scorecard: card,
      mutatesAlgorithmScores: false,
      algorithmScoresTouched: false,
    });
  } catch (err) {
    console.error("[of-internal-pilot] scorecards post", err);
    return json(res, 400, { success: false, error: err?.message || "scorecards_post_failed" });
  }
}

/** Exported for tests — live evaluate path stays offline via payload for pilot UI. */
export { evaluateOperatorFitForDeal };
