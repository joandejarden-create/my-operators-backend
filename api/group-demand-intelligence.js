/**
 * Group Demand Intelligence API — standalone; does not alter ADP.
 */

import {
  isGroupDemandIntelligenceEnabled,
  isGroupDemandIntelligencePilotReadAllowed,
  getGroupDemandIntelligenceFlagState,
  runGroupDemandResearch,
  listRegisteredHotels,
  listGdiSelectableHotels,
  loadHotelProfile,
  loadLatestSummary,
  listResearchRuns,
  loadResearchRun,
  buildWeeklyBrief,
  filterSalespersonView,
  saveFeedbackItem,
  loadFeedback,
  FEEDBACK_QUALITY,
  SALES_OUTCOME,
  HOTEL_FAMILIARITY,
  HOTEL_COMMERCIAL_STATUS,
  HOTEL_VALUE_FEEDBACK,
  SOURCING_STATUS,
  INCREMENTAL_VALUE_STATUS,
  QUALIFICATION_FAILURE_REASON,
  reconstructScoreAudit,
  buildPilotMetrics,
  PILOT_HOTEL_ID,
  getWebhoundHardCapUsd,
  resolveAmpfyAdapterStatus,
  listGdiResearchMethods,
  GDI_PRODUCT_VERSION,
  extractGdiShareCapabilityFromRequest,
  verifyGdiShareCapability,
  sanitizeOpportunityForShare,
  issueGdiShareCapability,
  revokeGdiShareCapability,
  applyCanonicalOverlaysToOpportunities,
  resolveCanonicalOverlayForOpportunity,
  loadShareValidation,
  saveShareValidationItem,
  getShareValidationForOpportunity,
  buildShareValidationSummary,
  SHARE_FAMILIARITY_STATUS,
  SHARE_FAMILIARITY_LABEL,
  SHARE_COMMERCIAL_VALUE,
  SHARE_COMMERCIAL_VALUE_LABEL,
  SHARE_CONTACT_PERSON_ASSESSMENT,
  SHARE_CONTACT_PERSON_LABEL,
  SHARE_EMAIL_ASSESSMENT,
  SHARE_EMAIL_ASSESSMENT_LABEL,
  SHARE_PHONE_ASSESSMENT,
  SHARE_PHONE_ASSESSMENT_LABEL,
  mapOpportunitiesToListDto,
  toOpportunityListDto,
  GDI_OPPORTUNITY_LIST_SCHEMA,
  applyLiveCommercialQuality,
  buildRelatedOpportunityGroups,
  buildExportCsv,
  customerCsvContentDisposition,
  HOTEL_VALIDATION_REASON,
  HOTEL_VALIDATION_REASON_LABEL,
} from "../lib/group-demand-intelligence/index.js";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import {
  getCachedOpportunityDoc,
  invalidateGdiHotelReadCache,
} from "../lib/group-demand-intelligence/read-cache.js";
import {
  ingestGdiAuthFeedback,
  ingestGdiShareValidation,
  ensureGdiOpportunityDecision,
  recordAction,
  recordOutcome,
  GDI_ACTION_TYPE,
  GDI_OUTCOME_TYPE,
} from "../lib/decision-outcomes/index.js";
import {
  loadGdiCommercialProgressionBySubject,
  toCustomerCommercialProgressionDto,
} from "../lib/decision-outcomes/gdi-commercial-progression.js";

/** Canonical opportunity load (Airtable primary when configured). Short-TTL cached. */
async function loadOppDoc(hotelId) {
  const t0 = Date.now();
  const doc = await loadOpportunitiesCanonical(hotelId);
  const ms = Date.now() - t0;
  if (ms >= 1000 || process.env.GDI_PERF_LOG === "1") {
    console.info(
      JSON.stringify({
        gdi_perf: true,
        route: "loadOppDoc",
        hotelId,
        durationMs: ms,
        cacheHit: doc?.cacheHit === true,
        recordCount: Array.isArray(doc?.opportunities) ? doc.opportunities.length : 0,
      })
    );
  }
  return doc;
}

/**
 * Prefer cached hotel doc; on miss load single Airtable row when possible
 * instead of re-listing the full hotel set for View Details.
 */
async function loadOpportunityForDetail(hotelId, opportunityId) {
  const t0 = Date.now();
  const cached = getCachedOpportunityDoc(hotelId);
  if (cached) {
    const found = (cached.opportunities || []).find((o) => o.id === opportunityId);
    if (found) {
      return {
        opportunity: found,
        source: "cache",
        durationMs: Date.now() - t0,
      };
    }
  }
  // Cache miss: try single-record Airtable path when configured
  try {
    const {
      loadOpportunity,
      isGdiOpportunityAirtableConfigured,
    } = await import("../lib/group-demand-intelligence/airtable-opportunity-store.js");
    if (isGdiOpportunityAirtableConfigured()) {
      const one = await loadOpportunity(hotelId, opportunityId);
      if (one) {
        return {
          opportunity: one,
          source: "airtable_single",
          durationMs: Date.now() - t0,
        };
      }
    }
  } catch (err) {
    if (err?.code === "hotel_boundary_violation") throw err;
    // fall through to full doc
  }
  const doc = await loadOppDoc(hotelId);
  const opportunity = (doc.opportunities || []).find((o) => o.id === opportunityId);
  return {
    opportunity: opportunity || null,
    source: "full_doc",
    durationMs: Date.now() - t0,
  };
}

function asOfToday() {
  return new Date().toISOString().slice(0, 10);
}

/** Apply live commercial quality + related-opportunity grouping (read path). */
function projectOpportunitiesCommercialQuality(opportunities, { nowDate } = {}) {
  const related = buildRelatedOpportunityGroups(opportunities || []);
  return (opportunities || []).map((o) => {
    const cq = applyLiveCommercialQuality(o, { nowDate: nowDate || asOfToday() });
    const relatedIds = related[o.id] || [];
    return {
      ...cq,
      relatedOpportunityIds: relatedIds,
      relatedOpportunityCount: relatedIds.length,
    };
  });
}

function shareSanitizeWithCanonical(hotelId, opportunity) {
  const { opportunity: overlaid } = resolveCanonicalOverlayForOpportunity(
    hotelId,
    opportunity
  );
  const [projected] = projectOpportunitiesCommercialQuality([overlaid]);
  return sanitizeOpportunityForShare(projected);
}

async function attachCommercialProgression(hotelId, opportunities) {
  const bySubject = await loadGdiCommercialProgressionBySubject(hotelId);
  return (opportunities || []).map((o) => {
    const dto = toCustomerCommercialProgressionDto(bySubject.get(o.id));
    return dto ? { ...o, commercialProgression: dto } : o;
  });
}

/** Apply list/export filters (ids + browse facets). */
function filterOpportunitiesForExport(opportunities, query = {}) {
  let rows = opportunities || [];
  const idsFilter = String(query.ids || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  if (idsFilter.length) {
    const set = new Set(idsFilter);
    rows = rows.filter((o) => set.has(o.id) || set.has(o.opportunityId));
  }
  const weekly = String(query.weekly || "").trim().toUpperCase();
  const priority = String(query.priority || "").trim().toUpperCase();
  const booking = String(query.booking || "").trim().toUpperCase();
  const segment = String(query.segment || "").trim();
  const territory = String(query.territory || "").trim().toUpperCase();
  if (weekly && weekly !== "ALL") {
    rows = rows.filter((o) => {
      if (weekly === "NEW") return o.isNewThisWeek === true || o.weeklyDeltaState === "NEW";
      return String(o.weeklyDeltaState || "").toUpperCase() === weekly;
    });
  }
  if (priority && priority !== "ALL") {
    rows = rows.filter((o) => String(o.priority || "").toUpperCase() === priority);
  }
  if (booking && booking !== "ALL") {
    rows = rows.filter(
      (o) => String(o.bookingWindowStatus || "").toUpperCase() === booking
    );
  }
  if (segment) {
    rows = rows.filter((o) => String(o.segment || "") === segment);
  }
  if (territory && territory !== "ALL") {
    rows = rows.filter(
      (o) => String(o.demandTerritoryFit || "").toUpperCase() === territory
    );
  }
  return rows;
}

function sendCustomerCsv(res, opportunities, hotel) {
  const csv = buildExportCsv(opportunities, hotel, {});
  const name =
    hotel?.name ||
    hotel?.displayName ||
    hotel?.identity?.hotelName ||
    hotel?.hotelName ||
    "Hotel";
  res.setHeader("Content-Type", "text/csv; charset=utf-8");
  res.setHeader("Content-Disposition", customerCsvContentDisposition(name));
  res.setHeader("X-Content-Type-Options", "nosniff");
  return res.status(200).send(csv);
}

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
      "Group Demand Intelligence is disabled. Local servers auto-enable pilot read; set GROUP_DEMAND_INTELLIGENCE_V1=1 or GROUP_DEMAND_INTELLIGENCE_PILOT_READ=1, or remove PILOT_READ=0.",
    flag: getGroupDemandIntelligenceFlagState(),
  });
  return false;
}

export function getGdiFlag(req, res) {
  return res.json({
    ok: true,
    flag: getGroupDemandIntelligenceFlagState(),
    productVersion: GDI_PRODUCT_VERSION,
    webhoundHardCapUsd: getWebhoundHardCapUsd(),
    ampfy: resolveAmpfyAdapterStatus(),
    pilotHotelId: PILOT_HOTEL_ID,
  });
}

export function getGdiHotels(req, res) {
  if (!flagGate(req, res)) return;
  const hotels = listGdiSelectableHotels();
  if (hotels.length === 0) {
    // Fallback: materialized profiles only (legacy)
    const registered = listRegisteredHotels();
    if (registered.length === 0) {
      return res.json({
        ok: true,
        hotels: [],
        experimentalLabel: "EXPERIMENTAL / PILOT",
        note: "No GDI-enabled hotels discovered yet. Add config + run research.",
      });
    }
    return res.json({
      ok: true,
      hotels: registered.map((h) => ({
        hotelId: h.hotelId,
        hotelName: h.identity?.hotelName || h.hotelId,
        displayName: h.identity?.hotelName || h.hotelId,
        brand: h.identity?.brand,
        city: h.identity?.city,
        state: h.identity?.state,
        locationLine: [h.identity?.city, h.identity?.state].filter(Boolean).join(", "),
        optionLabel: [h.identity?.hotelName || h.hotelId, [h.identity?.city, h.identity?.state].filter(Boolean).join(", ")]
          .filter(Boolean)
          .join(" — "),
        hasProfile: true,
        hasOpportunities: false,
        identityStatus: h.canonicalIds?.censusRecordId
          ? "canonical_census"
          : "provisional_gdi_key",
      })),
      experimentalLabel: "EXPERIMENTAL / PILOT",
    });
  }
  return res.json({
    ok: true,
    hotels,
    experimentalLabel: "EXPERIMENTAL / PILOT",
  });
}

export async function getGdiSummary(req, res) {
  if (!flagGate(req, res)) return;
  const hotelId = String(req.params.hotelId || "").trim();
  const summary = loadLatestSummary(hotelId);
  const profile = loadHotelProfile(hotelId);
  const doc = await loadOppDoc(hotelId);
  const feedback = loadFeedback(hotelId);
  const pilotMetrics = buildPilotMetrics(doc.opportunities || [], feedback);
  return res.json({
    ok: true,
    experimentalLabel: "EXPERIMENTAL / PILOT",
    hotelId,
    profileReady: Boolean(profile),
    summary,
    competitiveContext: profile
      ? {
          strCompSet: profile.strCompSet || [],
          relevantGroupDemandAlternatives: profile.relevantGroupDemandAlternatives || [],
          demandTerritory: profile.demandTerritory || null,
        }
      : null,
    pilotMetrics,
    flag: getGroupDemandIntelligenceFlagState(),
  });
}

export function getGdiProfile(req, res) {
  if (!flagGate(req, res)) return;
  const hotelId = String(req.params.hotelId || "").trim();
  const profile = loadHotelProfile(hotelId);
  if (!profile) {
    return res.status(404).json({
      ok: false,
      error: "profile_not_found",
      message: "Run research to build the Hotel Group Demand Profile.",
    });
  }
  return res.json({ ok: true, profile });
}

export async function getGdiOpportunities(req, res) {
  if (!flagGate(req, res)) return;
  const hotelId = String(req.params.hotelId || "").trim();
  const includeDisqualified = String(req.query.includeDisqualified || "") === "1";
  const view = String(req.query.view || "list").trim().toLowerCase();
  const format = String(req.query.format || "").trim().toLowerCase();
  const wantFull = view === "full" || view === "complete";
  const doc = await loadOppDoc(hotelId);
  let opportunities = projectOpportunitiesCommercialQuality(doc.opportunities || []);
  if (!includeDisqualified) {
    opportunities = filterSalespersonView(opportunities);
  }

  // CSV via ?format=csv (compat path; dedicated /export.csv also exists)
  if (format === "csv") {
    const hotels = listRegisteredHotels();
    const hotel = hotels.find((h) => h.hotelId === hotelId) || { hotelId, name: hotelId };
    opportunities = filterOpportunitiesForExport(opportunities, req.query);
    opportunities = await attachCommercialProgression(hotelId, opportunities);
    return sendCustomerCsv(res, opportunities, hotel);
  }

  if (!wantFull) {
    opportunities = mapOpportunitiesToListDto(opportunities);
  }
  opportunities = await attachCommercialProgression(hotelId, opportunities);
  res.setHeader("X-GDI-Opportunity-View", wantFull ? "full" : "list");
  return res.json({
    ok: true,
    hotelId,
    updatedAt: doc.updatedAt,
    runId: doc.runId || null,
    count: opportunities.length,
    view: wantFull ? "full" : "list",
    schemaVersion: wantFull ? null : GDI_OPPORTUNITY_LIST_SCHEMA,
    opportunities,
  });
}

export async function getGdiOpportunityDetail(req, res) {
  if (!flagGate(req, res)) return;
  const hotelId = String(req.params.hotelId || "").trim();
  const opportunityId = String(req.params.opportunityId || "").trim();
  const t0 = Date.now();
  const loaded = await loadOpportunityForDetail(hotelId, opportunityId);
  if (!loaded.opportunity) {
    return res.status(404).json({ ok: false, error: "opportunity_not_found" });
  }
  const projected = projectOpportunitiesCommercialQuality([loaded.opportunity]);
  const opportunity = projected[0];
  const bySubject = await loadGdiCommercialProgressionBySubject(hotelId);
  const commercialProgression = toCustomerCommercialProgressionDto(
    bySubject.get(opportunityId)
  );
  const durationMs = Date.now() - t0;
  if (durationMs >= 1000 || process.env.GDI_PERF_LOG === "1") {
    console.info(
      JSON.stringify({
        gdi_perf: true,
        route: "getGdiOpportunityDetail",
        hotelId,
        durationMs,
        loadSource: loaded.source,
        loadMs: loaded.durationMs,
      })
    );
  }
  res.setHeader("X-GDI-Detail-Source", loaded.source);
  return res.json({
    ok: true,
    opportunity: commercialProgression
      ? { ...opportunity, commercialProgression }
      : opportunity,
    scoreAudit: reconstructScoreAudit(opportunity),
    validationReasons: HOTEL_VALIDATION_REASON,
    validationReasonLabels: HOTEL_VALIDATION_REASON_LABEL,
  });
}

/**
 * CSV export of current hotel opportunities (honors includeDisqualified + ids filter).
 * Query: ids=comma (optional), includeDisqualified=1, weekly=, priority=, booking=, segment=, territory=
 */
export async function getGdiOpportunitiesExport(req, res) {
  if (!flagGate(req, res)) return;
  const hotelId = String(req.params.hotelId || "").trim();
  const includeDisqualified = String(req.query.includeDisqualified || "") === "1";

  const doc = await loadOppDoc(hotelId);
  let opportunities = projectOpportunitiesCommercialQuality(doc.opportunities || []);
  if (!includeDisqualified) {
    opportunities = filterSalespersonView(opportunities);
  }
  opportunities = filterOpportunitiesForExport(opportunities, req.query);
  opportunities = await attachCommercialProgression(hotelId, opportunities);

  const hotels = listRegisteredHotels();
  const hotel =
    hotels.find((h) => h.hotelId === hotelId) ||
    { hotelId, name: hotelId };

  return sendCustomerCsv(res, opportunities, hotel);
}

export async function getGdiWeeklyBrief(req, res) {
  if (!flagGate(req, res)) return;
  const hotelId = String(req.params.hotelId || "").trim();
  const doc = await loadOppDoc(hotelId);
  const brief = buildWeeklyBrief(doc.opportunities || []);
  const bySubject = await loadGdiCommercialProgressionBySubject(hotelId);
  const attachItems = (items) =>
    (items || []).map((it) => {
      const dto = toCustomerCommercialProgressionDto(
        bySubject.get(it.opportunityId || it.id)
      );
      return dto ? { ...it, commercialProgression: dto } : it;
    });
  const enriched = {
    ...brief,
    items: attachItems(brief.items),
    highPriority: attachItems(brief.highPriority),
    mediumPriority: attachItems(brief.mediumPriority),
    watchlist: attachItems(brief.watchlist),
  };
  return res.json({ ok: true, hotelId, brief: enriched });
}

export function getGdiResearchRuns(req, res) {
  if (!flagGate(req, res)) return;
  const hotelId = String(req.params.hotelId || "").trim();
  return res.json({
    ok: true,
    hotelId,
    runs: listResearchRuns(hotelId),
    methods: listGdiResearchMethods(),
  });
}

export function getGdiResearchRun(req, res) {
  if (!flagGate(req, res)) return;
  const hotelId = String(req.params.hotelId || "").trim();
  const runId = String(req.params.runId || "").trim();
  const run = loadResearchRun(hotelId, runId);
  if (!run) {
    return res.status(404).json({ ok: false, error: "run_not_found" });
  }
  return res.json({ ok: true, run });
}

export async function postGdiRunResearch(req, res) {
  if (!isGroupDemandIntelligenceEnabled() && !isGroupDemandIntelligencePilotReadAllowed()) {
    return res.status(403).json({
      ok: false,
      error: "feature_disabled",
      message: "Enable GROUP_DEMAND_INTELLIGENCE_V1 to run research.",
    });
  }
  const hotelId = String(req.params.hotelId || "").trim();
  const body = req.body || {};
  try {
    const result = await runGroupDemandResearch({
      hotelId,
      trigger: "admin_api",
      webhoundSessionId: body.webhoundSessionId || null,
      webhoundCostUsd: body.webhoundCostUsd,
      webhoundUrl: body.webhoundUrl || null,
      webhoundQuestion: body.webhoundQuestion || null,
      webhoundMergedOpportunityIds: body.webhoundMergedOpportunityIds || null,
      allowWebhoundWithoutFlag: body.allowWebhoundWithoutFlag === true,
      dryRun: body.dryRun === true,
    });
    return res.json({
      ok: true,
      experimentalLabel: "EXPERIMENTAL / PILOT",
      ...result,
    });
  } catch (err) {
    const status = err.code === "hotel_not_onboarded_for_gdi_pilot" ? 400 : 500;
    console.error("[gdi] run research failed", err);
    return res.status(status).json({
      ok: false,
      error: err.code || "run_failed",
      message: err.message || "Research run failed",
    });
  }
}

export async function postGdiFeedback(req, res) {
  if (!flagGate(req, res)) return;
  const hotelId = String(req.params.hotelId || "").trim();
  const opportunityId = String(req.params.opportunityId || "").trim();
  const body = req.body || {};
  const quality = body.quality || body.value || null;
  const salesOutcome = body.salesOutcome || body.commercialStatus || "Not Reviewed";
  const familiarity = body.familiarity || null;
  const commercialStatus = body.commercialStatus || null;
  const value = body.value || null;
  const sourcingStatus = body.sourcingStatus || null;
  const incrementalValueStatus = body.incrementalValueStatus || null;
  const qualificationFailureReason = body.qualificationFailureReason || null;

  if (quality && !FEEDBACK_QUALITY.includes(quality)) {
    return res.status(400).json({
      ok: false,
      error: "invalid_quality",
      allowed: FEEDBACK_QUALITY,
    });
  }
  if (salesOutcome && !SALES_OUTCOME.includes(salesOutcome)) {
    return res.status(400).json({
      ok: false,
      error: "invalid_sales_outcome",
      allowed: SALES_OUTCOME,
    });
  }
  if (familiarity && !HOTEL_FAMILIARITY.includes(familiarity)) {
    return res.status(400).json({
      ok: false,
      error: "invalid_familiarity",
      allowed: HOTEL_FAMILIARITY,
    });
  }
  if (commercialStatus && !HOTEL_COMMERCIAL_STATUS.includes(commercialStatus)) {
    return res.status(400).json({
      ok: false,
      error: "invalid_commercial_status",
      allowed: HOTEL_COMMERCIAL_STATUS,
    });
  }
  if (value && !HOTEL_VALUE_FEEDBACK.includes(value) && !FEEDBACK_QUALITY.includes(value)) {
    return res.status(400).json({
      ok: false,
      error: "invalid_value",
      allowed: HOTEL_VALUE_FEEDBACK,
    });
  }
  if (sourcingStatus && !Object.values(SOURCING_STATUS).includes(sourcingStatus)) {
    return res.status(400).json({
      ok: false,
      error: "invalid_sourcing_status",
      allowed: Object.values(SOURCING_STATUS),
    });
  }
  if (
    incrementalValueStatus &&
    !Object.values(INCREMENTAL_VALUE_STATUS).includes(incrementalValueStatus)
  ) {
    return res.status(400).json({
      ok: false,
      error: "invalid_incremental_value_status",
      allowed: Object.values(INCREMENTAL_VALUE_STATUS),
    });
  }
  if (
    qualificationFailureReason &&
    !Object.values(QUALIFICATION_FAILURE_REASON).includes(qualificationFailureReason)
  ) {
    return res.status(400).json({
      ok: false,
      error: "invalid_qualification_failure_reason",
      allowed: Object.values(QUALIFICATION_FAILURE_REASON),
    });
  }

  const saved = saveFeedbackItem(hotelId, {
    opportunityId,
    quality,
    salesOutcome,
    familiarity,
    commercialStatus,
    value,
    sourcingStatus,
    incrementalValueStatus,
    qualificationFailureReason,
    comment: body.comment || "",
    actor: req.dealalityUser?.email || req.dealalityUser?.id || "unknown",
    // Hotel validation must never rewrite canonical opportunity facts
    doesNotOverwriteCanonicalFacts: true,
  });

  // Dual-write into canonical Decision & Outcome layer (does not replace legacy store)
  let decisionOutcome = null;
  try {
    const oppDoc = await loadOppDoc(hotelId);
    const opportunity = (oppDoc.opportunities || []).find(
      (o) => o.id === opportunityId
    );
    decisionOutcome = await ingestGdiAuthFeedback({
      hotelId,
      opportunityId,
      opportunity: opportunity || null,
      feedback: saved,
      actor: {
        userId: req.dealalityUser?.email || req.dealalityUser?.id || "unknown",
        role: req.dealalityUser?.role || "OTHER",
      },
    });
  } catch (err) {
    console.error("[gdi] decision-outcome dual-write failed", err?.message || err);
    return res.status(502).json({
      ok: false,
      error: "decision_outcome_persist_failed",
      message: err?.message || "Canonical Decision & Outcome write failed",
      feedback: saved,
      note: "Legacy feedback was stored, but durable Decision & Outcome persistence failed.",
    });
  }

  return res.json({
    ok: true,
    feedback: saved,
    decisionOutcome: decisionOutcome
      ? {
          decisionId:
            decisionOutcome.decision?.decision?.decisionId ||
            decisionOutcome.decision?.decisionId ||
            null,
          eventCount: (decisionOutcome.events || []).length,
        }
      : null,
    note: "Your feedback helps track this opportunity and improve future recommendations.",
  });
}

/**
 * Authenticated customer validation — same field contract as share validation.
 * Writes share-validation store + canonical Decision Events.
 */
export async function postGdiCustomerValidation(req, res) {
  if (!flagGate(req, res)) return;
  const hotelId = String(req.params.hotelId || "").trim();
  const opportunityId = String(req.params.opportunityId || "").trim();
  const doc = await loadOppDoc(hotelId);
  const opportunity = (doc.opportunities || []).find((o) => o.id === opportunityId);
  if (!opportunity || opportunity.priority === "DISQUALIFIED") {
    return res.status(404).json({ ok: false, error: "opportunity_not_found" });
  }
  try {
    const body = req.body || {};
    const { item } = saveShareValidationItem(hotelId, {
      opportunityId,
      validator:
        body.validator ||
        req.dealalityUser?.email ||
        req.dealalityUser?.id ||
        "AUTH_REVIEWER",
      familiarityStatus: body.familiarityStatus || null,
      commercialValue: body.commercialValue || null,
      contactPersonAssessment: body.contactPersonAssessment || null,
      emailAssessment: body.emailAssessment || null,
      phoneAssessment: body.phoneAssessment || null,
      note: body.note || "",
    });

    await ingestGdiShareValidation({
      hotelId,
      opportunityId,
      opportunity,
      validation: item,
      actor: {
        userId: req.dealalityUser?.email || req.dealalityUser?.id || "AUTH_REVIEWER",
        role: req.dealalityUser?.role || "OTHER",
      },
    });

    return res.json({
      ok: true,
      validation: {
        familiarityStatus: item.familiarityStatus,
        commercialValue: item.commercialValue,
        contactPersonAssessment: item.contactPersonAssessment,
        emailAssessment: item.emailAssessment,
        phoneAssessment: item.phoneAssessment,
        note: item.note || "",
        validatedAt: item.validatedAt,
      },
    });
  } catch (err) {
    if (err.code === "invalid_share_validation") {
      return res.status(400).json({ ok: false, error: err.code, errors: err.errors });
    }
    console.error("[gdi] auth customer validation failed", err);
    return res.status(500).json({ ok: false, error: "customer_validation_failed" });
  }
}

export function getGdiFeedback(req, res) {
  if (!flagGate(req, res)) return;
  const hotelId = String(req.params.hotelId || "").trim();
  return res.json({ ok: true, ...loadFeedback(hotelId) });
}

function requireGdiShare(req, res, requiredSurface) {
  const token = extractGdiShareCapabilityFromRequest(req);
  const verified = verifyGdiShareCapability(token, {
    expectedHotelId: req.params.hotelId ? String(req.params.hotelId).trim() : null,
    requiredSurface,
  });
  if (!verified.ok) {
    res.status(403).json({
      ok: false,
      error: verified.code || verified.error,
      code: verified.code || verified.error,
      message:
        verified.customerMessage ||
        "This Dealality access link is no longer active. Please request an updated link from your Dealality contact.",
    });
    return null;
  }
  return verified;
}

/** Public resolve — no Memberstack; token is the auth. */
export async function getGdiShareResolve(req, res) {
  const token = extractGdiShareCapabilityFromRequest(req);
  const verified = verifyGdiShareCapability(token);
  if (!verified.ok) {
    return res.status(403).json({
      ok: false,
      error: verified.code || verified.error,
      code: verified.code || verified.error,
      message:
        verified.customerMessage ||
        "This Dealality access link is no longer active. Please request an updated link from your Dealality contact.",
    });
  }
  const hotelId = verified.claims.hotelId;
  const profile = loadHotelProfile(hotelId);
  const summary = loadLatestSummary(hotelId);
  const oppDoc = await loadOppDoc(hotelId);
  const active = filterSalespersonView(oppDoc.opportunities || []);
  const validationDoc = loadShareValidation(hotelId);
  const validationSummary = buildShareValidationSummary({
    opportunities: active,
    validationItems: validationDoc.items || [],
  });
  return res.json({
    ok: true,
    mode: "read_only",
    capabilities: verified.capabilities || verified.claims.capabilities || [],
    canValidate: (verified.capabilities || []).includes("CAN_VALIDATE"),
    canRecordAction: (verified.capabilities || []).includes("CAN_RECORD_ACTION"),
    canRecordOutcome: (verified.capabilities || []).includes(
      "CAN_RECORD_OUTCOME"
    ),
    pilotLabel: "Pilot",
    hotelId,
    hotelName: profile?.identity?.hotelName || "Hotel",
    city: profile?.identity?.city || null,
    state: profile?.identity?.state || null,
    surfaces: verified.claims.surfaces,
    summary: {
      lastResearchAt: summary.lastResearchAt,
      qualifiedCount: summary.qualifiedCount,
      highPriorityCount: summary.highPriorityCount,
      mediumPriorityCount: summary.mediumPriorityCount,
      watchlistCount: summary.watchlistCount,
      // hotel-confirmed validation KPIs only
      validated: validationSummary.validated,
      pendingValidation: validationSummary.pendingValidation,
      confirmedNew: validationSummary.confirmedNew,
      alreadyKnown: validationSummary.alreadyKnown,
      worthPursuingNow: validationSummary.worthPursuingNow,
      notRelevant: validationSummary.notRelevant,
      // intentionally omit researchCostUsd / admin cost internals
    },
    validationSummary,
    expiresAt: verified.meta?.expiresAt || null,
  });
}

export async function getGdiShareBrief(req, res) {
  const verified = requireGdiShare(req, res, "brief");
  if (!verified) return;
  const hotelId = String(req.params.hotelId || "").trim();
  if (hotelId !== verified.claims.hotelId) {
    return res.status(403).json({ ok: false, error: "SHARE_HOTEL_SCOPE" });
  }
  const doc = await loadOppDoc(hotelId);
  const overlaid = applyCanonicalOverlaysToOpportunities(
    hotelId,
    doc.opportunities || []
  );
  const brief = buildWeeklyBrief(overlaid);
  const validationDoc = loadShareValidation(hotelId);
  return res.json({
    ok: true,
    mode: "read_only",
    hotelId,
    brief: {
      ...brief,
      items: (brief.items || []).map((it) => {
        const full = overlaid.find((o) => o.id === it.opportunityId);
        return {
          ...it,
          primaryContact: full?.primaryContact
            ? {
                name: full.primaryContact.name,
                role: full.primaryContact.role,
                email: full.primaryContact.email,
                phone: full.primaryContact.phone || null,
                phoneTypeLabel: full.primaryContact.phoneTypeLabel || null,
              }
            : it.primaryContact,
          validation: (validationDoc.items || []).find(
            (v) => v.opportunityId === it.opportunityId
          )
            ? {
                familiarityStatus: validationDoc.items.find(
                  (v) => v.opportunityId === it.opportunityId
                ).familiarityStatus,
                commercialValue: validationDoc.items.find(
                  (v) => v.opportunityId === it.opportunityId
                ).commercialValue,
              }
            : null,
        };
      }),
    },
  });
}

export async function getGdiShareOpportunities(req, res) {
  const verified = requireGdiShare(req, res, "opportunities");
  if (!verified) return;
  const hotelId = String(req.params.hotelId || "").trim();
  if (hotelId !== verified.claims.hotelId) {
    return res.status(403).json({ ok: false, error: "SHARE_HOTEL_SCOPE" });
  }
  const doc = await loadOppDoc(hotelId);
  const validationDoc = loadShareValidation(hotelId);
  const byVal = new Map((validationDoc.items || []).map((v) => [v.opportunityId, v]));
  const view = String(req.query.view || "list").trim().toLowerCase();
  const format = String(req.query.format || "").trim().toLowerCase();
  const wantFull = view === "full" || view === "complete";

  if (format === "csv") {
    return getGdiShareOpportunitiesExport(req, res);
  }

  const opportunities = filterSalespersonView(doc.opportunities || []).map((o) => {
    const sanitized = shareSanitizeWithCanonical(hotelId, o);
    const v = byVal.get(o.id);
    const base = wantFull ? sanitized : toOpportunityListDto(sanitized);
    return {
      ...base,
      shareValidation: v
        ? {
            familiarityStatus: v.familiarityStatus,
            commercialValue: v.commercialValue,
            contactPersonAssessment: v.contactPersonAssessment,
            emailAssessment: v.emailAssessment,
            phoneAssessment: v.phoneAssessment,
            note: v.note || "",
            validatedAt: v.validatedAt,
          }
        : null,
    };
  });
  const withProgression = await attachCommercialProgression(
    hotelId,
    opportunities
  );
  res.setHeader("X-GDI-Opportunity-View", wantFull ? "full" : "list");
  return res.json({
    ok: true,
    mode: "read_only",
    hotelId,
    count: withProgression.length,
    view: wantFull ? "full" : "list",
    schemaVersion: wantFull ? null : GDI_OPPORTUNITY_LIST_SCHEMA,
    opportunities: withProgression,
    validationEnums: {
      familiarityStatus: SHARE_FAMILIARITY_STATUS,
      familiarityLabels: SHARE_FAMILIARITY_LABEL,
      commercialValue: SHARE_COMMERCIAL_VALUE,
      commercialValueLabels: SHARE_COMMERCIAL_VALUE_LABEL,
      contactPersonAssessment: SHARE_CONTACT_PERSON_ASSESSMENT,
      contactPersonLabels: SHARE_CONTACT_PERSON_LABEL,
      emailAssessment: SHARE_EMAIL_ASSESSMENT,
      emailAssessmentLabels: SHARE_EMAIL_ASSESSMENT_LABEL,
      phoneAssessment: SHARE_PHONE_ASSESSMENT,
      phoneAssessmentLabels: SHARE_PHONE_ASSESSMENT_LABEL,
      validationReason: Object.values(HOTEL_VALIDATION_REASON),
      validationReasonLabels: HOTEL_VALIDATION_REASON_LABEL,
    },
  });
}

/** Share-scoped CSV export (same capability gate as list). */
export async function getGdiShareOpportunitiesExport(req, res) {
  const verified = requireGdiShare(req, res, "opportunities");
  if (!verified) return;
  const hotelId = String(req.params.hotelId || "").trim();
  if (hotelId !== verified.claims.hotelId) {
    return res.status(403).json({ ok: false, error: "SHARE_HOTEL_SCOPE" });
  }
  const doc = await loadOppDoc(hotelId);
  let opportunities = projectOpportunitiesCommercialQuality(
    filterSalespersonView(doc.opportunities || [])
  );
  opportunities = filterOpportunitiesForExport(opportunities, req.query);
  opportunities = await attachCommercialProgression(hotelId, opportunities);
  const hotels = listRegisteredHotels();
  const hotel = hotels.find((h) => h.hotelId === hotelId) || { hotelId, name: hotelId };
  return sendCustomerCsv(res, opportunities, hotel);
}

export async function getGdiShareOpportunityDetail(req, res) {
  const verified = requireGdiShare(req, res, "opportunity_detail");
  if (!verified) return;
  const hotelId = String(req.params.hotelId || "").trim();
  const opportunityId = String(req.params.opportunityId || "").trim();
  if (hotelId !== verified.claims.hotelId) {
    return res.status(403).json({ ok: false, error: "SHARE_HOTEL_SCOPE" });
  }
  const t0 = Date.now();
  const loaded = await loadOpportunityForDetail(hotelId, opportunityId);
  const opportunity = loaded.opportunity;
  if (!opportunity || opportunity.priority === "DISQUALIFIED") {
    return res.status(404).json({ ok: false, error: "opportunity_not_found" });
  }
  const sanitized = shareSanitizeWithCanonical(hotelId, opportunity);
  const validation = getShareValidationForOpportunity(hotelId, opportunityId);
  const bySubject = await loadGdiCommercialProgressionBySubject(hotelId);
  const commercialProgression = toCustomerCommercialProgressionDto(
    bySubject.get(opportunityId)
  );
  const durationMs = Date.now() - t0;
  if (durationMs >= 1000 || process.env.GDI_PERF_LOG === "1") {
    console.info(
      JSON.stringify({
        gdi_perf: true,
        route: "getGdiShareOpportunityDetail",
        hotelId,
        durationMs,
        loadSource: loaded.source,
        loadMs: loaded.durationMs,
      })
    );
  }
  res.setHeader("X-GDI-Detail-Source", loaded.source);
  return res.json({
    ok: true,
    mode: "read_only",
    opportunity: {
      ...sanitized,
      ...(commercialProgression ? { commercialProgression } : {}),
      shareValidation: validation
        ? {
            familiarityStatus: validation.familiarityStatus,
            commercialValue: validation.commercialValue,
            contactPersonAssessment: validation.contactPersonAssessment,
            emailAssessment: validation.emailAssessment,
            phoneAssessment: validation.phoneAssessment,
            note: validation.note || "",
            validatedAt: validation.validatedAt,
          }
        : null,
    },
    scoreAudit: reconstructScoreAudit(opportunity),
  });
}

/**
 * Share-token validation write — stores separately from research/canonical.
 * Requires CAN_VALIDATE (legacy tokens with opportunity_detail surface qualify).
 */
export async function postGdiShareValidation(req, res) {
  const verified = requireGdiShare(req, res, "opportunity_detail");
  if (!verified) return;
  const caps = verified.capabilities || [];
  if (!caps.includes("CAN_VALIDATE")) {
    return res.status(403).json({
      ok: false,
      error: "SHARE_SURFACE",
      code: "SHARE_SURFACE",
      message:
        "This Dealality access link is no longer active. Please request an updated link from your Dealality contact.",
    });
  }
  const hotelId = String(req.params.hotelId || "").trim();
  const opportunityId = String(req.params.opportunityId || "").trim();
  if (hotelId !== verified.claims.hotelId) {
    return res.status(403).json({
      ok: false,
      error: "SHARE_HOTEL_SCOPE",
      code: "SHARE_HOTEL_SCOPE",
      message:
        "This Dealality access link is no longer active. Please request an updated link from your Dealality contact.",
    });
  }
  const doc = await loadOppDoc(hotelId);
  const opportunity = (doc.opportunities || []).find((o) => o.id === opportunityId);
  if (!opportunity || opportunity.priority === "DISQUALIFIED") {
    return res.status(404).json({ ok: false, error: "opportunity_not_found" });
  }
  try {
    const body = req.body || {};
    const { item } = saveShareValidationItem(hotelId, {
      opportunityId,
      validator: body.validator || "SHARE_REVIEWER",
      familiarityStatus: body.familiarityStatus || null,
      commercialValue: body.commercialValue || null,
      contactPersonAssessment: body.contactPersonAssessment || null,
      emailAssessment: body.emailAssessment || null,
      phoneAssessment: body.phoneAssessment || null,
      note: body.note || "",
    });

    try {
      await ingestGdiShareValidation({
        hotelId,
        opportunityId,
        opportunity,
        validation: item,
        actor: {
          userId: body.validator || "SHARE_REVIEWER",
          role: "OTHER",
        },
      });
    } catch (dualErr) {
      console.error(
        "[gdi] share decision-outcome dual-write failed",
        dualErr?.message || dualErr
      );
    }

    const validationDoc = loadShareValidation(hotelId);
    const summary = buildShareValidationSummary({
      opportunities: filterSalespersonView(doc.opportunities || []),
      validationItems: validationDoc.items || [],
    });
    return res.json({
      ok: true,
      validation: {
        familiarityStatus: item.familiarityStatus,
        commercialValue: item.commercialValue,
        contactPersonAssessment: item.contactPersonAssessment,
        emailAssessment: item.emailAssessment,
        phoneAssessment: item.phoneAssessment,
        note: item.note || "",
        validatedAt: item.validatedAt,
      },
      summary,
      note: "Validation stored separately. Does not overwrite research facts or mutate canonical contacts. Dual-written to canonical Decision layer.",
    });
  } catch (err) {
    if (err.code === "invalid_share_validation") {
      return res.status(400).json({ ok: false, error: err.code, errors: err.errors });
    }
    console.error("[gdi] share validation failed", err);
    return res.status(500).json({ ok: false, error: "share_validation_failed" });
  }
}

export async function getGdiShareValidation(req, res) {
  const verified = requireGdiShare(req, res, "opportunity_detail");
  if (!verified) return;
  const hotelId = String(req.params.hotelId || "").trim();
  if (hotelId !== verified.claims.hotelId) {
    return res.status(403).json({ ok: false, error: "SHARE_HOTEL_SCOPE" });
  }
  const doc = await loadOppDoc(hotelId);
  const validationDoc = loadShareValidation(hotelId);
  const summary = buildShareValidationSummary({
    opportunities: filterSalespersonView(doc.opportunities || []),
    validationItems: validationDoc.items || [],
  });
  return res.json({
    ok: true,
    hotelId,
    items: (validationDoc.items || []).map((v) => ({
      opportunityId: v.opportunityId,
      familiarityStatus: v.familiarityStatus,
      commercialValue: v.commercialValue,
      contactPersonAssessment: v.contactPersonAssessment,
      emailAssessment: v.emailAssessment,
      phoneAssessment: v.phoneAssessment,
      note: v.note || "",
      validatedAt: v.validatedAt,
    })),
    summary,
  });
}

function requireShareCapability(verified, res, capability) {
  const caps = verified.capabilities || [];
  if (!caps.includes(capability)) {
    res.status(403).json({
      ok: false,
      error: "SHARE_SURFACE",
      code: "SHARE_SURFACE",
      message:
        "This Dealality access link is no longer active. Please request an updated link from your Dealality contact.",
    });
    return false;
  }
  return true;
}

/**
 * Share-token ACTION write → canonical Decision Events.
 * Requires CAN_RECORD_ACTION (registry grant or explicit token capability).
 */
export async function postGdiShareAction(req, res) {
  const verified = requireGdiShare(req, res, "opportunity_detail");
  if (!verified) return;
  if (!requireShareCapability(verified, res, "CAN_RECORD_ACTION")) return;

  const hotelId = String(req.params.hotelId || "").trim();
  const opportunityId = String(req.params.opportunityId || "").trim();
  if (hotelId !== verified.claims.hotelId) {
    return res.status(403).json({
      ok: false,
      error: "SHARE_HOTEL_SCOPE",
      code: "SHARE_HOTEL_SCOPE",
      message:
        "This Dealality access link is no longer active. Please request an updated link from your Dealality contact.",
    });
  }
  const doc = await loadOppDoc(hotelId);
  const opportunity = (doc.opportunities || []).find((o) => o.id === opportunityId);
  if (!opportunity || opportunity.priority === "DISQUALIFIED") {
    return res.status(404).json({ ok: false, error: "opportunity_not_found" });
  }
  const actionType = String(req.body?.actionType || "").trim();
  if (!actionType || !Object.values(GDI_ACTION_TYPE).includes(actionType)) {
    return res.status(400).json({ ok: false, error: "invalid_action_type" });
  }
  try {
    const ensured = await ensureGdiOpportunityDecision({ hotelId, opportunity });
    const decision = ensured.decision;
    if (!decision) {
      return res.status(500).json({ ok: false, error: "decision_ensure_failed" });
    }
    const result = await recordAction(hotelId, decision.decisionId, {
      actionType,
      actionDate: req.body?.actionDate || new Date().toISOString(),
      sourceSurface: "gdi_share",
      displayName: req.body?.displayName || "Share reviewer",
      userId: req.body?.validator || "SHARE_REVIEWER",
      provenance: {
        tokenId: verified.claims.tid,
        opportunityId,
        source: "CUSTOMER_SHARE",
      },
    });
    invalidateGdiHotelReadCache(hotelId);
    return res.status(201).json({
      ok: true,
      decisionId: decision.decisionId,
      event: result.event,
      created: result.created !== false,
      current: result.current,
      commercialProgression: result.current?.commercialProgression || null,
    });
  } catch (err) {
    console.error("[gdi] share action failed", err);
    return res.status(500).json({
      ok: false,
      error: err.code || "share_action_failed",
      message: err.message,
    });
  }
}

/**
 * Share-token OUTCOME write → canonical Decision Events.
 * Requires CAN_RECORD_OUTCOME.
 */
export async function postGdiShareOutcome(req, res) {
  const verified = requireGdiShare(req, res, "opportunity_detail");
  if (!verified) return;
  if (!requireShareCapability(verified, res, "CAN_RECORD_OUTCOME")) return;

  const hotelId = String(req.params.hotelId || "").trim();
  const opportunityId = String(req.params.opportunityId || "").trim();
  if (hotelId !== verified.claims.hotelId) {
    return res.status(403).json({
      ok: false,
      error: "SHARE_HOTEL_SCOPE",
      code: "SHARE_HOTEL_SCOPE",
      message:
        "This Dealality access link is no longer active. Please request an updated link from your Dealality contact.",
    });
  }
  const doc = await loadOppDoc(hotelId);
  const opportunity = (doc.opportunities || []).find((o) => o.id === opportunityId);
  if (!opportunity || opportunity.priority === "DISQUALIFIED") {
    return res.status(404).json({ ok: false, error: "opportunity_not_found" });
  }
  const outcomeType = String(req.body?.outcomeType || "").trim();
  if (!outcomeType || !Object.values(GDI_OUTCOME_TYPE).includes(outcomeType)) {
    return res.status(400).json({ ok: false, error: "invalid_outcome_type" });
  }
  try {
    const ensured = await ensureGdiOpportunityDecision({ hotelId, opportunity });
    const decision = ensured.decision;
    if (!decision) {
      return res.status(500).json({ ok: false, error: "decision_ensure_failed" });
    }
    const result = await recordOutcome(hotelId, decision.decisionId, {
      outcomeType,
      outcomeReason: req.body?.outcomeReason || req.body?.lossReason || null,
      outcomeNote: req.body?.note || null,
      outcomeDate: req.body?.outcomeDate || new Date().toISOString(),
      causalConfidence: "UNKNOWN",
      sourceSurface: "gdi_share",
      displayName: req.body?.displayName || "Share reviewer",
      userId: req.body?.validator || "SHARE_REVIEWER",
      provenance: {
        tokenId: verified.claims.tid,
        opportunityId,
        source: "CUSTOMER_SHARE",
      },
    });
    invalidateGdiHotelReadCache(hotelId);
    return res.status(201).json({
      ok: true,
      decisionId: decision.decisionId,
      event: result.event,
      created: result.created !== false,
      current: result.current,
      commercialProgression: result.current?.commercialProgression || null,
    });
  } catch (err) {
    console.error("[gdi] share outcome failed", err);
    return res.status(500).json({
      ok: false,
      error: err.code || "share_outcome_failed",
      message: err.message,
    });
  }
}

/** Admin-only: issue share token */
export function postGdiIssueShare(req, res) {
  if (!flagGate(req, res)) return;
  try {
    const body = req.body || {};
    const hotelId = String(body.hotelId || req.params.hotelId || "").trim();
    const issued = issueGdiShareCapability({
      hotelId,
      label: body.label || null,
      expiresAt: body.expiresAt || null,
    });
    return res.json({
      ok: true,
      tokenId: issued.tokenId,
      hotelId: issued.hotelId,
      sharePath: issued.sharePath,
      expiresAt: issued.meta.expiresAt,
      mode: "read_only",
    });
  } catch (err) {
    console.error("[gdi] issue share failed", err);
    return res.status(500).json({
      ok: false,
      error: "issue_share_failed",
      message: err.message,
    });
  }
}

export function postGdiRevokeShare(req, res) {
  if (!flagGate(req, res)) return;
  const tokenId = String(req.body?.tokenId || req.params.tokenId || "").trim();
  const result = revokeGdiShareCapability(tokenId, req.body?.reason || "admin_revoke");
  if (!result.ok) {
    return res.status(404).json(result);
  }
  return res.json(result);
}
