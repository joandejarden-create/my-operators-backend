/**
 * GDI Pursuit API — sales workflow separate from Ready/Watch gates.
 */

import {
  isGroupDemandIntelligenceEnabled,
  isGroupDemandIntelligencePilotReadAllowed,
  getGroupDemandIntelligenceFlagState,
} from "../lib/group-demand-intelligence/index.js";
import {
  loadOpportunitiesCanonical,
  upsertSingleOpportunity,
} from "../lib/group-demand-intelligence/opportunity-persistence.js";
import {
  listPursuits,
  getPursuitById,
  getPursuitByOpportunityId,
  startPursuitFromOpportunity,
  updatePursuit,
  recordPursuitResponse,
  recordPursuitOutcome,
  applyFollowUpDue,
  toCustomerPursuitDto,
  canStartPursuitFromOpportunity,
  GDI_PURSUIT_STATUS,
  GDI_HOTEL_INCLUSION_STATUS,
  GDI_PURSUIT_RESPONSE_STATUS,
  GDI_PURSUIT_OUTCOME,
  isActivePursuitStatus,
  HOTEL_SELECTION_PURSUIT_STATUSES,
  isClosedPursuitStatus,
} from "../lib/group-demand-intelligence/pursuit/index.js";

function flagGate(req, res) {
  if (
    isGroupDemandIntelligenceEnabled() ||
    isGroupDemandIntelligencePilotReadAllowed()
  ) {
    return true;
  }
  res.status(403).json({
    ok: false,
    error: "feature_disabled",
    flag: getGroupDemandIntelligenceFlagState(),
  });
  return false;
}

function actorOf(req) {
  return req.dealalityUser?.email || req.dealalityUser?.id || "hotel_user";
}

export function getGdiPursuitEnums(req, res) {
  if (!flagGate(req, res)) return;
  return res.json({
    ok: true,
    pursuitStatus: Object.values(GDI_PURSUIT_STATUS),
    hotelInclusionStatus: Object.values(GDI_HOTEL_INCLUSION_STATUS),
    responseStatus: Object.values(GDI_PURSUIT_RESPONSE_STATUS),
    outcome: Object.values(GDI_PURSUIT_OUTCOME),
  });
}

export function getGdiPursuits(req, res) {
  if (!flagGate(req, res)) return;
  const hotelId = String(req.params.hotelId || "").trim();
  applyFollowUpDue(hotelId);
  const filter = String(req.query.filter || "").toUpperCase();
  let rows = listPursuits(hotelId);
  if (filter === "ACTIVE" || filter === "ACTIVE_PURSUITS") {
    rows = rows.filter((p) => isActivePursuitStatus(p.pursuitStatus));
  } else if (filter === "FOLLOW_UP_DUE") {
    rows = rows.filter((p) => p.pursuitStatus === GDI_PURSUIT_STATUS.FOLLOW_UP_DUE);
  } else if (filter === "HOTEL_SELECTION") {
    rows = rows.filter((p) => HOTEL_SELECTION_PURSUIT_STATUSES.has(p.pursuitStatus));
  } else if (filter === "CLOSED") {
    rows = rows.filter((p) => isClosedPursuitStatus(p.pursuitStatus));
  }
  return res.json({
    ok: true,
    hotelId,
    count: rows.length,
    pursuits: rows.map(toCustomerPursuitDto),
  });
}

export function getGdiPursuitById(req, res) {
  if (!flagGate(req, res)) return;
  const hotelId = String(req.params.hotelId || "").trim();
  const pursuitId = String(req.params.pursuitId || "").trim();
  applyFollowUpDue(hotelId);
  const pursuit = getPursuitById(hotelId, pursuitId);
  if (!pursuit) {
    return res.status(404).json({ ok: false, error: "pursuit_not_found" });
  }
  return res.json({ ok: true, pursuit: toCustomerPursuitDto(pursuit) });
}

export function getGdiPursuitByOpportunity(req, res) {
  if (!flagGate(req, res)) return;
  const hotelId = String(req.params.hotelId || "").trim();
  const opportunityId = String(req.params.opportunityId || "").trim();
  const pursuit = getPursuitByOpportunityId(hotelId, opportunityId);
  return res.json({
    ok: true,
    hotelId,
    opportunityId,
    pursuit: pursuit ? toCustomerPursuitDto(pursuit) : null,
    canStart: false, // filled by client from opp.outreachReadiness; detail route enriches
  });
}

export async function postGdiStartPursuit(req, res) {
  if (!flagGate(req, res)) return;
  const hotelId = String(req.params.hotelId || "").trim();
  const opportunityId = String(req.params.opportunityId || "").trim();
  const body = req.body || {};
  try {
    const doc = await loadOpportunitiesCanonical(hotelId);
    const opp = (doc.opportunities || []).find((o) => o.id === opportunityId);
    if (!opp) {
      return res.status(404).json({ ok: false, error: "opportunity_not_found" });
    }
    if (!canStartPursuitFromOpportunity(opp) && body.force !== true) {
      return res.status(400).json({
        ok: false,
        error: "pursuit_not_eligible",
        outreachReadiness: opp.outreachReadiness || null,
      });
    }
    const result = startPursuitFromOpportunity(hotelId, opp, {
      actor: actorOf(req),
      language: body.language || opp.draftLanguage || "es",
      draftSubject: body.draftSubject || opp.draftSubject || null,
      draftMessage: body.draftMessage || opp.draftMessage || null,
      force: body.force === true,
    });
    if (!result.ok) {
      return res.status(400).json(result);
    }
    // Mirror pursuitId onto opportunity payload only — never Ready/Watch fields
    try {
      await upsertSingleOpportunity(hotelId, {
        ...opp,
        pursuitId: result.pursuit.pursuitId,
        pursuitStatus: result.pursuit.pursuitStatus,
      });
    } catch {
      /* mirror optional — pursuit store remains source of truth */
    }
    return res.json({
      ok: true,
      created: result.created,
      pursuit: toCustomerPursuitDto(result.pursuit),
    });
  } catch (err) {
    console.error("[gdi-pursuit] start failed", err?.message || err);
    return res.status(500).json({ ok: false, error: "pursuit_start_failed" });
  }
}

export function patchGdiPursuit(req, res) {
  if (!flagGate(req, res)) return;
  const hotelId = String(req.params.hotelId || "").trim();
  const pursuitId = String(req.params.pursuitId || "").trim();
  const body = req.body || {};
  const { pursuit, updated } = updatePursuit(hotelId, pursuitId, body, {
    actor: actorOf(req),
    source: "api_patch",
    eventType: body.eventType || null,
  });
  if (!updated || !pursuit) {
    return res.status(404).json({ ok: false, error: "pursuit_not_found" });
  }
  return res.json({ ok: true, pursuit: toCustomerPursuitDto(pursuit) });
}

export function postGdiPursuitResponse(req, res) {
  if (!flagGate(req, res)) return;
  const hotelId = String(req.params.hotelId || "").trim();
  const pursuitId = String(req.params.pursuitId || "").trim();
  const { pursuit, updated } = recordPursuitResponse(hotelId, pursuitId, req.body || {}, {
    actor: actorOf(req),
  });
  if (!updated || !pursuit) {
    return res.status(404).json({ ok: false, error: "pursuit_not_found" });
  }
  return res.json({ ok: true, pursuit: toCustomerPursuitDto(pursuit) });
}

export function postGdiPursuitOutcome(req, res) {
  if (!flagGate(req, res)) return;
  const hotelId = String(req.params.hotelId || "").trim();
  const pursuitId = String(req.params.pursuitId || "").trim();
  const { pursuit, updated } = recordPursuitOutcome(hotelId, pursuitId, req.body || {}, {
    actor: actorOf(req),
  });
  if (!updated || !pursuit) {
    return res.status(404).json({ ok: false, error: "pursuit_not_found" });
  }
  return res.json({ ok: true, pursuit: toCustomerPursuitDto(pursuit) });
}

export function postGdiPursuitFollowUp(req, res) {
  if (!flagGate(req, res)) return;
  const hotelId = String(req.params.hotelId || "").trim();
  const pursuitId = String(req.params.pursuitId || "").trim();
  const body = req.body || {};
  const { pursuit, updated } = updatePursuit(
    hotelId,
    pursuitId,
    {
      nextFollowUpDate: body.nextFollowUpDate || body.followUpDate || null,
      nextAction: body.nextAction,
      nextActionReason: body.nextActionReason,
      notes: body.notes,
    },
    { actor: actorOf(req), source: "follow_up_update" }
  );
  if (!updated || !pursuit) {
    return res.status(404).json({ ok: false, error: "pursuit_not_found" });
  }
  return res.json({ ok: true, pursuit: toCustomerPursuitDto(pursuit) });
}
