/**
 * Hotel Need Times & Priorities portal API (GDI Tab 2).
 * Restores endpoints expected by historical hotel-need-dates-app.js
 * plus commercial-profile CRUD from hotel-commercial-priorities-contract-v1.
 */

import {
  isGroupDemandIntelligenceEnabled,
  isGroupDemandIntelligencePilotReadAllowed,
} from "../lib/group-demand-intelligence/index.js";
import {
  assertNeedDatesHotelAccess,
  actorUserFromReq,
  buildNeedDatesManagementDashboard,
  createNeedPeriodsFromInput,
  applyPortalNeedPeriodChange,
  getNeedPeriodById,
  getNeedPeriodFromAirtable,
  isNeedDateAirtableLiveEnabled,
  loadCommercialProfile,
  saveCommercialProfile,
  getCommercialPrioritiesEnums,
} from "../lib/group-demand-intelligence/need-date-demand/index.js";

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
    message:
      "Group Demand Intelligence is disabled. Set GROUP_DEMAND_INTELLIGENCE_V1=1 or GROUP_DEMAND_INTELLIGENCE_PILOT_READ=1.",
  });
  return false;
}

function resolveNeedDatesPortalOpts(req, { write = false } = {}) {
  const isolationRaw = String(
    req.query?.isolation || req.body?.isolation || "",
  ).trim();
  const airtableRaw = String(
    req.query?.airtable || req.body?.airtable || "",
  )
    .trim()
    .toLowerCase();
  const isolation =
    isolationRaw === "portal_v1_test" || isolationRaw === "1"
      ? "portal_v1_test"
      : null;
  const useAirtable =
    airtableRaw === "1" || airtableRaw === "true"
      ? true
      : airtableRaw === "0" || airtableRaw === "false"
        ? false
        : isolation
          ? false
          : undefined;
  return {
    isolation,
    useAirtable,
    year: Number(req.query?.year || req.body?.year) || undefined,
    testScenarioOnly: String(req.query?.testScenarioOnly || "") === "1",
    actorUser: null,
    actorSource: write ? "hotel_sales" : "hotel_portal_read",
  };
}

function sendPortalError(res, err, fallbackCode) {
  const status = Number(err.status || err.statusCode || 500);
  return res.status(status).json({
    ok: false,
    error: err.code || fallbackCode,
    message: err.message || fallbackCode,
    currentVersion: err.currentVersion,
    expectedVersion: err.expectedVersion,
  });
}

export async function getGdiNeedDatesDashboard(req, res) {
  try {
    if (!flagGate(req, res)) return;
    const hotelId = String(req.params.hotelId || "").trim();
    assertNeedDatesHotelAccess(req, hotelId, { write: false });
    const opts = resolveNeedDatesPortalOpts(req);
    opts.actorUser = actorUserFromReq(req);
    const dashboard = await buildNeedDatesManagementDashboard(hotelId, opts);
    return res.json(dashboard);
  } catch (err) {
    return sendPortalError(res, err, "need_dates_dashboard_failed");
  }
}

export async function postGdiNeedPeriods(req, res) {
  try {
    if (!flagGate(req, res)) return;
    const hotelId = String(req.params.hotelId || "").trim();
    assertNeedDatesHotelAccess(req, hotelId, { write: true });
    const opts = resolveNeedDatesPortalOpts(req, { write: true });
    opts.actorUser = actorUserFromReq(req);
    opts.actorSource = "hotel_sales";
    const result = await createNeedPeriodsFromInput(
      hotelId,
      req.body || {},
      opts,
    );
    return res.json({ ok: true, ...result });
  } catch (err) {
    return sendPortalError(res, err, "need_period_create_failed");
  }
}

export async function getGdiNeedPeriodDetail(req, res) {
  try {
    if (!flagGate(req, res)) return;
    const hotelId = String(req.params.hotelId || "").trim();
    const needPeriodId = String(req.params.needPeriodId || "").trim();
    assertNeedDatesHotelAccess(req, hotelId, { write: false });
    const opts = resolveNeedDatesPortalOpts(req);
    let hit = null;
    const preferAirtable =
      opts.useAirtable === true ||
      (opts.useAirtable !== false &&
        isNeedDateAirtableLiveEnabled() &&
        !opts.isolation);
    if (preferAirtable) {
      try {
        hit = await getNeedPeriodFromAirtable(hotelId, needPeriodId);
      } catch {
        hit = null;
      }
    }
    if (!hit) {
      hit = getNeedPeriodById(hotelId, needPeriodId, {
        isolation: opts.isolation || null,
      });
    }
    if (!hit) {
      return res.status(404).json({ ok: false, error: "need_period_not_found" });
    }
    return res.json({
      ok: true,
      hotelId,
      period: hit.period || hit,
      changes: hit.changes || [],
    });
  } catch (err) {
    return sendPortalError(res, err, "need_period_detail_failed");
  }
}

export async function patchGdiNeedPeriod(req, res) {
  try {
    if (!flagGate(req, res)) return;
    const hotelId = String(req.params.hotelId || "").trim();
    const needPeriodId = String(req.params.needPeriodId || "").trim();
    assertNeedDatesHotelAccess(req, hotelId, { write: true });
    const opts = resolveNeedDatesPortalOpts(req, { write: true });
    opts.actorUser = actorUserFromReq(req);
    opts.actorSource = "hotel_sales";
    const result = await applyPortalNeedPeriodChange(
      hotelId,
      needPeriodId,
      req.body || {},
      opts,
    );
    return res.json({ ok: true, ...result });
  } catch (err) {
    return sendPortalError(res, err, "need_period_update_failed");
  }
}

export async function getGdiCommercialProfile(req, res) {
  try {
    if (!flagGate(req, res)) return;
    const hotelId = String(req.params.hotelId || "").trim();
    assertNeedDatesHotelAccess(req, hotelId, { write: false });
    const opts = resolveNeedDatesPortalOpts(req);
    const profile = await loadCommercialProfile(hotelId, {
      useAirtable: opts.useAirtable,
      allowLocalFallback: true,
    });
    return res.json({
      ok: true,
      hotelId,
      profile,
      enums: getCommercialPrioritiesEnums(),
    });
  } catch (err) {
    return sendPortalError(res, err, "commercial_profile_load_failed");
  }
}

export async function putGdiCommercialProfile(req, res) {
  try {
    if (!flagGate(req, res)) return;
    const hotelId = String(req.params.hotelId || "").trim();
    assertNeedDatesHotelAccess(req, hotelId, { write: true });
    const opts = resolveNeedDatesPortalOpts(req, { write: true });
    const actorUser = actorUserFromReq(req);
    const body = req.body || {};
    const patch = {
      revenuePriorities: Array.isArray(body.revenuePriorities)
        ? body.revenuePriorities
        : undefined,
      dayPreference: body.dayPreference ?? undefined,
      specificDays: Array.isArray(body.specificDays)
        ? body.specificDays
        : undefined,
      prioritySegments: Array.isArray(body.prioritySegments)
        ? body.prioritySegments
        : undefined,
      excludedSegments: Array.isArray(body.excludedSegments)
        ? body.excludedSegments
        : undefined,
      restrictionNotes:
        body.restrictionNotes != null
          ? String(body.restrictionNotes)
          : undefined,
      peakRoomsPreset: body.peakRoomsPreset ?? undefined,
      bookingWindow: body.bookingWindow ?? undefined,
      meetingSpaceNeed: body.meetingSpaceNeed ?? undefined,
      stayDuration: body.stayDuration ?? undefined,
      segmentMode: body.segmentMode ?? undefined,
      demandGeneratorCategories: Array.isArray(body.demandGeneratorCategories)
        ? body.demandGeneratorCategories
        : undefined,
      businessToAvoid: Array.isArray(body.businessToAvoid)
        ? body.businessToAvoid
        : undefined,
      hotelConsiderations: Array.isArray(body.hotelConsiderations)
        ? body.hotelConsiderations
        : undefined,
      knownAccounts: Array.isArray(body.knownAccounts)
        ? body.knownAccounts
        : undefined,
      provenance: body.provenance || "Hotel Confirmed",
    };
    for (const k of Object.keys(patch)) {
      if (patch[k] === undefined) delete patch[k];
    }
    const result = await saveCommercialProfile(hotelId, patch, {
      useAirtable: opts.useAirtable,
      actorUser,
      actorSource: "hotel_sales",
      reason: body.reason || "Hotel commercial priorities update",
      expectedVersion: body.expectedVersion,
    });
    return res.json(result);
  } catch (err) {
    return sendPortalError(res, err, "commercial_profile_save_failed");
  }
}
