/**
 * Waterstone commercial eligibility overlay V1 — zero paid, local preview.
 *
 * Stamps founder-audited dispositions and honest content corrections.
 * Does NOT auto-hide opportunities (visibility changes require founder
 * approval of exact IDs in the overlay proposedRemove list).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { canonicalizePriorityKey } from "./claim-types.js";
import { HOTEL_SOURCING_DISPLAY } from "./events-opportunity-overview-v1.js";
import { assessCommercialCustomerReadiness } from "./commercial-customer-readiness-gate-v1.js";

export const WATERSTONE_COMMERCIAL_ELIGIBILITY_OVERLAY_V1 =
  "gdi-waterstone-commercial-eligibility-overlay-v1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
export const WATERSTONE_COMMERCIAL_ELIGIBILITY_OVERLAY_PATH = path.join(
  ROOT,
  "data/group-demand-intelligence/local-shadow/waterstone-commercial-eligibility-overlay-v1.json",
);

const WATERSTONE_HOTEL_ID = "recgMYovrrZDJMqzX";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function loadOverlayDoc() {
  if (!fs.existsSync(WATERSTONE_COMMERCIAL_ELIGIBILITY_OVERLAY_PATH)) return null;
  try {
    return JSON.parse(
      fs.readFileSync(WATERSTONE_COMMERCIAL_ELIGIBILITY_OVERLAY_PATH, "utf8"),
    );
  } catch {
    return null;
  }
}

export function applyCommercialEligibilityRow(opportunity = {}, row = {}) {
  if (!row?.commercialEligibilityOverlayV1) return opportunity;
  const o = { ...opportunity };

  if (row.customerReadinessDisposition) {
    o.customerReadinessDisposition = row.customerReadinessDisposition;
    o.contactCommercialDisposition = row.customerReadinessDisposition;
  }
  if (row.commercialProspectTier) {
    o.commercialProspectTier = row.commercialProspectTier;
  }
  if (row.commercialCategory) {
    o.commercialCategory = row.commercialCategory;
    o.opportunityCategory = row.commercialCategory;
  }
  if (row.revenueStreamClass) {
    o.revenueStreamClass = row.revenueStreamClass;
    o.revenueClassification = row.revenueStreamClass;
    o.applicableHotelRevenueStreams = row.revenueStreamClass;
  }
  if (row.guestroomThesis) {
    o.guestroomThesis = text(row.guestroomThesis);
    o.hotelDemandHypotheses = {
      ...(o.hotelDemandHypotheses && typeof o.hotelDemandHypotheses === "object"
        ? o.hotelDemandHypotheses
        : {}),
      guestroomHypothesis: text(row.guestroomThesis),
    };
  }
  if (row.eventFnbThesis) {
    o.eventFnbThesis = text(row.eventFnbThesis);
    o.hotelDemandHypotheses = {
      ...(o.hotelDemandHypotheses && typeof o.hotelDemandHypotheses === "object"
        ? o.hotelDemandHypotheses
        : {}),
      meetingsHypothesis: text(row.eventFnbThesis),
      cateringHypothesis:
        o.hotelDemandHypotheses?.cateringHypothesis || text(row.eventFnbThesis),
    };
  }
  if (row.dosGrade) o.dosGrade = row.dosGrade;
  const canonPriority = canonicalizePriorityKey(
    row.priorityFilterKey || row.priority,
  );
  if (canonPriority) {
    o.priority = canonPriority;
    o.priorityFilterKey = canonPriority;
  }

  if (row.hotelSourcingStatusLabel || row.hotelSourcing) {
    const sourcing = text(row.hotelSourcingStatusLabel || row.hotelSourcing);
    if (sourcing) {
      o.hotelSourcingStatusLabel = sourcing;
      o.hotelSourcingStatus = sourcing;
      o.venueSourcingStatusLabel = sourcing;
      if (o.eventsOpportunityOverview && typeof o.eventsOpportunityOverview === "object") {
        o.eventsOpportunityOverview = {
          ...o.eventsOpportunityOverview,
          hotelSourcing: sourcing,
          tileClassificationLine: String(
            o.eventsOpportunityOverview.tileClassificationLine || "",
          ).replace(/Hotel Sourcing Unconfirmed/gi, sourcing),
        };
      }
    }
  }

  if (row.roomDemandStatusLabel || row.roomDemand) {
    const rd = text(row.roomDemandStatusLabel || row.roomDemand);
    if (rd && o.eventsOpportunityOverview && typeof o.eventsOpportunityOverview === "object") {
      o.eventsOpportunityOverview = {
        ...o.eventsOpportunityOverview,
        roomDemand: rd,
      };
      o.roomDemandStatusLabel = rd;
    }
  }

  if (row.drawerSectionPatches && typeof row.drawerSectionPatches === "object") {
    for (const [sectionId, body] of Object.entries(row.drawerSectionPatches)) {
      if (!text(body)) continue;
      o.scalingDrawerModel = patchDrawerSectionBody(
        o.scalingDrawerModel,
        sectionId,
        body,
      );
      o.eventsDrawerModel = patchDrawerSectionBody(
        o.eventsDrawerModel,
        sectionId,
        body,
      );
      o.sharedTenSectionDrawer = patchDrawerSectionBody(
        o.sharedTenSectionDrawer,
        sectionId,
        body,
      );
    }
  }

  if (row.whatIsHappening) {
    o.whatIsHappening = text(row.whatIsHappening);
    o.summaryWhat = text(row.whatIsHappening);
    o.eventsDrawerModel = patchDrawerSectionBody(
      o.eventsDrawerModel,
      "what_is_happening",
      row.whatIsHappening,
    );
    o.sharedTenSectionDrawer = patchDrawerSectionBody(
      o.sharedTenSectionDrawer,
      "what_is_happening",
      row.whatIsHappening,
    );
  }
  if (row.potentialBusinessForHotel || row.hotelOpportunityThesis) {
    const pot = text(row.potentialBusinessForHotel || row.hotelOpportunityThesis);
    o.potentialBusinessForHotel = pot;
    o.proposedHotelUse = pot;
    o.eventsDrawerModel = patchDrawerSectionBody(
      o.eventsDrawerModel,
      "potential_business",
      pot,
    );
    o.sharedTenSectionDrawer = patchDrawerSectionBody(
      o.sharedTenSectionDrawer,
      "potential_business",
      pot,
    );
    if (o.salesBrief && typeof o.salesBrief === "object") {
      o.salesBrief = {
        ...o.salesBrief,
        potentialBusinessForHotel: pot,
        potentialBusinessForThisHotel: pot,
      };
    }
    if (o.associationEventsGoldBrief && typeof o.associationEventsGoldBrief === "object") {
      o.associationEventsGoldBrief = {
        ...o.associationEventsGoldBrief,
        potentialBusinessForHotel: pot,
      };
    }
  }
  if (row.whyActNow) {
    o.whyActNow = text(row.whyActNow);
    o.eventsDrawerModel = patchDrawerSectionBody(
      o.eventsDrawerModel,
      "why_act_now",
      row.whyActNow,
    );
    o.sharedTenSectionDrawer = patchDrawerSectionBody(
      o.sharedTenSectionDrawer,
      "why_act_now",
      row.whyActNow,
    );
  }
  if (row.opportunityOverview || row.establishedFacts) {
    const overview = text(row.opportunityOverview || row.establishedFacts);
    if (overview) {
      o.eventsDrawerModel = patchDrawerSectionBody(
        o.eventsDrawerModel,
        "opportunity_overview",
        overview,
      );
      o.sharedTenSectionDrawer = patchDrawerSectionBody(
        o.sharedTenSectionDrawer,
        "opportunity_overview",
        overview,
      );
      o.salesBriefOverview = overview;
    }
  }
  if (Array.isArray(row.establishedFactsList) && row.establishedFactsList.length) {
    o.establishedFacts = row.establishedFactsList.map(text).filter(Boolean);
  }
  if (Array.isArray(row.uncertaintiesList) && row.uncertaintiesList.length) {
    o.uncertainties = row.uncertaintiesList.map(text).filter(Boolean);
  }
  if (row.conversationOpener) {
    o.conversationOpener = text(row.conversationOpener);
  }
  if (row.salesBriefApproach || row.recommendedAction) {
    const approach = text(row.salesBriefApproach || row.recommendedAction);
    if (o.salesBrief && typeof o.salesBrief === "object") {
      o.salesBrief = {
        ...o.salesBrief,
        recommendedSalesApproach: approach,
      };
    }
    if (o.associationEventsGoldBrief && typeof o.associationEventsGoldBrief === "object") {
      o.associationEventsGoldBrief = {
        ...o.associationEventsGoldBrief,
        recommendedSalesApproach: approach,
      };
    }
  }

  if (row.recommendedAction) {
    o.recommendedAction = row.recommendedAction;
    o.suggestedAction = row.recommendedAction;
    o.recommendedNextStep = row.recommendedAction;
    o.watchCardRecommendedNextStep = row.recommendedAction;
    // Keep pre-built drawer models honest when overlay replaces the action.
    o.scalingDrawerModel = patchDrawerRecommendedAction(
      o.scalingDrawerModel,
      row.recommendedAction,
    );
    o.eventsDrawerModel = patchDrawerRecommendedAction(
      o.eventsDrawerModel,
      row.recommendedAction,
    );
    o.sharedTenSectionDrawer = patchDrawerRecommendedAction(
      o.sharedTenSectionDrawer,
      row.recommendedAction,
    );
  }
  if (row.salesActionRecommended) {
    o.salesActionRecommended = row.salesActionRecommended;
  }
  if (row.whyThisHotel) {
    o.whyThisHotel = row.whyThisHotel;
    o.scalingDrawerModel = patchDrawerSectionBody(
      o.scalingDrawerModel,
      "why_this_hotel",
      row.whyThisHotel,
    );
    o.eventsDrawerModel = patchDrawerSectionBody(
      o.eventsDrawerModel,
      "why_this_hotel",
      row.whyThisHotel,
    );
    o.sharedTenSectionDrawer = patchDrawerSectionBody(
      o.sharedTenSectionDrawer,
      "why_this_hotel",
      row.whyThisHotel,
    );
  }
  if (row.hotelOpportunityThesis) {
    o.hotelOpportunityThesis = row.hotelOpportunityThesis;
  }
  if (row.contactResearchGap) o.contactResearchGap = row.contactResearchGap;
  if (Array.isArray(row.uncertainties) && row.uncertainties.length) {
    o.uncertainties = row.uncertainties;
  }
  // V11 sales activation lane — distinct from customerReadinessDisposition
  if (row.salesActivationCategory) {
    o.salesActivationCategory = String(row.salesActivationCategory);
    o.salesActivationLabel = text(row.salesActivationLabel) || null;
    o.salesActivationRationale = text(row.salesActivationRationale) || null;
  }
  if (row.accountFamilyId) o.accountFamilyId = String(row.accountFamilyId);
  if (row.needDateRelevanceClass) {
    o.needDateRelevanceClass = String(row.needDateRelevanceClass);
  }

  if (row.primaryContact && typeof row.primaryContact === "object") {
    const name = text(row.primaryContact.name);
    // Never invent names; allow clearing functional fake names to org route
    if (!name) {
      o.primaryContact = {
        ...(o.primaryContact && typeof o.primaryContact === "object"
          ? o.primaryContact
          : {}),
        ...row.primaryContact,
        name: null,
      };
      if (!looksLikePersonNameLoose(o.contactName)) o.contactName = null;
    }
  }

  o.commercialEligibilityOverlay = {
    version: WATERSTONE_COMMERCIAL_ELIGIBILITY_OVERLAY_V1,
    disposition: row.customerReadinessDisposition || null,
    provenance: row.provenance || null,
    recommendedAction: text(row.recommendedAction) || null,
    whyThisHotel: text(row.whyThisHotel) || null,
    appliedAt: new Date().toISOString().slice(0, 10),
    autoHideApplied: false,
  };
  o.localShadowPortfolio = true;
  o._commercialEligibilityOverlayLocked = true;
  return o;
}

/**
 * Re-apply locked overlay copy after customer-presentation sanitize, which can
 * regenerate scaling recommended-action prose from earlier research models.
 */
export function reassertWaterstoneCommercialEligibilityOverlay(
  opportunities = [],
  hotelId = null,
) {
  if (String(hotelId) !== WATERSTONE_HOTEL_ID) return opportunities;
  const doc = loadOverlayDoc();
  const byId = new Map(
    (doc?.opportunities || []).map((r) => [String(r.id), r]),
  );
  return (opportunities || []).map((raw) => {
    const row = byId.get(String(raw?.id));
    const stamp = raw?.commercialEligibilityOverlay;
    if (
      !raw?._commercialEligibilityOverlayLocked &&
      !stamp?.recommendedAction &&
      !row?.commercialEligibilityOverlayV1
    ) {
      return raw;
    }
    // Prefer full overlay row re-apply so sanitize rebuilds cannot restore
    // stale Hotel Sourcing Unconfirmed / cold-acquisition drawer copy.
    if (row?.commercialEligibilityOverlayV1) {
      return applyCommercialEligibilityRow(raw, row);
    }
    const action = text(stamp?.recommendedAction || raw.recommendedAction);
    const why = text(stamp?.whyThisHotel || raw.whyThisHotel);
    if (!action && !why) return raw;
    let o = { ...raw };
    if (action) {
      o.recommendedAction = action;
      o.suggestedAction = action;
      o.recommendedNextStep = action;
      o.watchCardRecommendedNextStep = action;
      o.salesActionRecommended = action;
      o.scalingDrawerModel = patchDrawerRecommendedAction(
        o.scalingDrawerModel,
        action,
      );
      o.eventsDrawerModel = patchDrawerRecommendedAction(
        o.eventsDrawerModel,
        action,
      );
      o.sharedTenSectionDrawer = patchDrawerRecommendedAction(
        o.sharedTenSectionDrawer,
        action,
      );
    }
    if (why) {
      o.whyThisHotel = why;
      o.scalingDrawerModel = patchDrawerSectionBody(
        o.scalingDrawerModel,
        "why_this_hotel",
        why,
      );
      o.eventsDrawerModel = patchDrawerSectionBody(
        o.eventsDrawerModel,
        "why_this_hotel",
        why,
      );
    }
    return o;
  });
}

function looksLikePersonNameLoose(name) {
  const n = text(name);
  if (!n || n.length < 4) return false;
  if (/team|staff|management|organizers?|department/i.test(n)) return false;
  return /\s/.test(n);
}

function patchDrawerSectionBody(model, sectionId, body) {
  if (!model || typeof model !== "object" || !text(body)) return model;
  const sections =
    model.sections && typeof model.sections === "object" ? model.sections : null;
  if (!sections || !sections[sectionId]) return model;
  return {
    ...model,
    sections: {
      ...sections,
      [sectionId]: {
        ...sections[sectionId],
        body: text(body),
      },
    },
  };
}

function patchDrawerRecommendedAction(model, action) {
  if (!model || typeof model !== "object" || !text(action)) return model;
  let next = patchDrawerSectionBody(model, "recommended_next_step", action);
  next = patchDrawerSectionBody(next, "recommended_action", action);
  if (next?.recommendedSalesApproach && typeof next.recommendedSalesApproach === "object") {
    next = {
      ...next,
      recommendedSalesApproach: {
        ...next.recommendedSalesApproach,
        nextAction: text(action),
      },
    };
  }
  return next;
}

/**
 * Apply explicit overlay rows, then stamp readiness assessment on all
 * Waterstone opportunities for local preview (does not change visibility).
 */
export function applyWaterstoneCommercialEligibilityOverlay(
  opportunities = [],
  hotelId = null,
  { env = process.env, asOf = new Date() } = {},
) {
  if (String(hotelId) !== WATERSTONE_HOTEL_ID) return opportunities;
  if (
    String(env.GDI_WATERSTONE_COMMERCIAL_ELIGIBILITY_OVERLAY || "1").trim() ===
    "0"
  ) {
    return opportunities;
  }

  const doc = loadOverlayDoc();
  const byId = new Map(
    (doc?.opportunities || []).map((r) => [String(r.id), r]),
  );

  return opportunities.map((raw) => {
    let o = raw;
    const row = byId.get(String(raw?.id));
    if (row) o = applyCommercialEligibilityRow(raw, row);

    const assessment = assessCommercialCustomerReadiness(o, {
      hotelId,
      asOf,
    });
    // Overlay disposition wins when explicitly set
    const disposition =
      o.customerReadinessDisposition || assessment.disposition;

    return {
      ...o,
      customerReadinessDisposition: disposition,
      customerReadinessAssessment: {
        version: assessment.version,
        disposition,
        layers: assessment.layers,
        demandClass: assessment.demand?.demandClass || null,
        timingClass: assessment.timing?.timingClass || null,
        recommendBuyerResearch: assessment.recommendBuyerResearch,
        recommendRemoveFromActiveCustomerView:
          assessment.recommendRemoveFromActiveCustomerView,
        rationale: assessment.rationale,
        founderVisibilityReviewRequired:
          assessment.recommendRemoveFromActiveCustomerView === true,
        autoHideApplied: false,
      },
      // Keep contactCommercialDisposition aligned with commercial readiness
      // when overlay or readiness gate is stricter than contact-only gate.
      contactCommercialDisposition: disposition,
    };
  });
}

export function listProposedVisibilityRemovals() {
  const doc = loadOverlayDoc();
  return doc?.proposedRemoveFromActiveCustomerView || [];
}
