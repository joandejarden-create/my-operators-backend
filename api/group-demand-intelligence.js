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
} from "../lib/group-demand-intelligence/index.js";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import {
  ingestGdiAuthFeedback,
  ingestGdiShareValidation,
} from "../lib/decision-outcomes/index.js";

/** Canonical opportunity load (Airtable primary when configured). */
async function loadOppDoc(hotelId) {
  return loadOpportunitiesCanonical(hotelId);
}

function shareSanitizeWithCanonical(hotelId, opportunity) {
  const { opportunity: overlaid } = resolveCanonicalOverlayForOpportunity(
    hotelId,
    opportunity
  );
  return sanitizeOpportunityForShare(overlaid);
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
  const wantFull = view === "full" || view === "complete";
  const doc = await loadOppDoc(hotelId);
  let opportunities = doc.opportunities || [];
  if (!includeDisqualified) {
    opportunities = filterSalespersonView(opportunities);
  }
  if (!wantFull) {
    opportunities = mapOpportunitiesToListDto(opportunities);
  }
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
  const doc = await loadOppDoc(hotelId);
  const opportunity = (doc.opportunities || []).find((o) => o.id === opportunityId);
  if (!opportunity) {
    return res.status(404).json({ ok: false, error: "opportunity_not_found" });
  }
  return res.json({
    ok: true,
    opportunity,
    scoreAudit: reconstructScoreAudit(opportunity),
  });
}

export async function getGdiWeeklyBrief(req, res) {
  if (!flagGate(req, res)) return;
  const hotelId = String(req.params.hotelId || "").trim();
  const doc = await loadOppDoc(hotelId);
  const brief = buildWeeklyBrief(doc.opportunities || []);
  return res.json({ ok: true, hotelId, brief });
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
  const wantFull = view === "full" || view === "complete";
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
  res.setHeader("X-GDI-Opportunity-View", wantFull ? "full" : "list");
  return res.json({
    ok: true,
    mode: "read_only",
    hotelId,
    count: opportunities.length,
    view: wantFull ? "full" : "list",
    schemaVersion: wantFull ? null : GDI_OPPORTUNITY_LIST_SCHEMA,
    opportunities,
    validationEnums: {
      familiarityStatus: SHARE_FAMILIARITY_STATUS,
      familiarityLabels: SHARE_FAMILIARITY_LABEL,
      commercialValue: SHARE_COMMERCIAL_VALUE,
      commercialValueLabels: SHARE_COMMERCIAL_VALUE_LABEL,
      contactPerson: SHARE_CONTACT_PERSON_ASSESSMENT,
      contactPersonLabels: SHARE_CONTACT_PERSON_LABEL,
      emailAssessment: SHARE_EMAIL_ASSESSMENT,
      emailAssessmentLabels: SHARE_EMAIL_ASSESSMENT_LABEL,
      phoneAssessment: SHARE_PHONE_ASSESSMENT,
      phoneAssessmentLabels: SHARE_PHONE_ASSESSMENT_LABEL,
    },
  });
}

export async function getGdiShareOpportunityDetail(req, res) {
  const verified = requireGdiShare(req, res, "opportunity_detail");
  if (!verified) return;
  const hotelId = String(req.params.hotelId || "").trim();
  const opportunityId = String(req.params.opportunityId || "").trim();
  if (hotelId !== verified.claims.hotelId) {
    return res.status(403).json({ ok: false, error: "SHARE_HOTEL_SCOPE" });
  }
  const doc = await loadOppDoc(hotelId);
  const opportunity = (doc.opportunities || []).find((o) => o.id === opportunityId);
  if (!opportunity || opportunity.priority === "DISQUALIFIED") {
    return res.status(404).json({ ok: false, error: "opportunity_not_found" });
  }
  const sanitized = shareSanitizeWithCanonical(hotelId, opportunity);
  const validation = getShareValidationForOpportunity(hotelId, opportunityId);
  return res.json({
    ok: true,
    mode: "read_only",
    opportunity: {
      ...sanitized,
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
