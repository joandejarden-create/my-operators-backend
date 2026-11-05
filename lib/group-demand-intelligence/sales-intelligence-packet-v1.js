/**
 * Shared GDI Sales Intelligence Packet V1
 *
 * Converts researched opportunities into complete, evidence-grounded sales packets
 * for Ready / customer surfaces — without inventing lodging demand, named buyers,
 * or urgency. Reuses buildGdiOpportunitySummary + recommendedAction helpers.
 *
 * Does not grant Ready by itself; stamps fields that Ready already requires when
 * evidence supports them.
 */

import { buildGdiOpportunitySummary, evaluateGdiSummaryQuality, SUMMARY_QUALITY } from "./opportunity-summary-v1.js";
import { recommendedActionForCommercialShape } from "./buyer-contact-path-taxonomy-v1.js";
import { extractContactSurface } from "./prospect-publication-eligibility-v1.js";
import { DEMAND_EVIDENCE_LABEL } from "./research/event-to-demand/event-commercial-disposition-v1.js";
import { OPPORTUNITY_TYPE, OPPORTUNITY_TYPE_LABEL } from "./claim-types.js";
import { enrichOpportunityWithSecondaryNamedContact } from "./secondary-named-contact-v1.js";

export const SALES_INTELLIGENCE_PACKET_VERSION = "gdi-sales-intelligence-packet-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function mechanismOf(opp = {}) {
  return String(
    opp.eventDemandMechanism ||
      opp.proposedHotelUse ||
      opp.eventHotelFitClass ||
      opp.mechanism ||
      ""
  ).toUpperCase();
}

function demandLabelOf(opp = {}) {
  return String(
    opp.demandEvidenceLabel ||
      opp.eventDemandClass ||
      opp.commercialDispositionDemandClass ||
      ""
  ).toUpperCase();
}

function looksLikeRealTimingWindow(window = "") {
  const w = text(window);
  if (!w) return false;
  // Reject relationship / TBD labels that are not actionable timing triggers
  if (
    /^(ongoing|tbd|n\/?a|unknown|to\s*be\s*confirmed|standing|relationship)\b/i.test(w) ||
    /\bongoing corporate relationship\b/i.test(w) ||
    /^watch\b/i.test(w)
  ) {
    return false;
  }
  // Prefer ISO / year / month-year / explicit date ranges
  if (/\b20\d{2}\b/.test(w) || /\b\d{1,2}[\/\-]\d{1,2}([\/\-]\d{2,4})?\b/.test(w)) {
    return true;
  }
  if (/\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\b/i.test(w)) {
    return true;
  }
  // Explicit futureDemand.supported with a non-placeholder window
  return false;
}

function timingTrigger(opp = {}) {
  const candidates = [
    text(opp.futureDemand?.dateWindow),
    text(opp.eventStartDate),
    text(opp.eventDateDisplay),
    text(opp.timing),
    text(opp.dateWindow),
  ].filter(Boolean);
  const window = candidates.find((c) => looksLikeRealTimingWindow(c)) || "";
  const source =
    text(opp.futureDemand?.sourceUrl) ||
    text(opp.officialSource) ||
    (Array.isArray(opp.sources) && (opp.sources[0]?.url || opp.sources[0])) ||
    "";
  const supported =
    (opp.futureDemand?.supported === true && looksLikeRealTimingWindow(window || text(opp.futureDemand?.dateWindow))) ||
    Boolean(window);
  return { window: window || null, source, supported: Boolean(window) };
}

/**
 * Map E2D / research mechanism to a customer-facing commercial category label.
 * Avoids defaulting everything to "Corporate Account Relationship".
 */
export function commercialCategoryForOpportunity(opp = {}) {
  const mech = mechanismOf(opp);
  const type = String(opp.opportunityType || "").toUpperCase();
  const disposition = String(opp.commercialDisposition || "").toUpperCase();

  if (disposition === "STRATEGIC_PARTNERSHIP" || type === "VENUE_PARTNERSHIP") {
    return {
      opportunityType: OPPORTUNITY_TYPE.VENUE_PARTNERSHIP,
      opportunityTypeLabel: OPPORTUNITY_TYPE_LABEL.VENUE_PARTNERSHIP,
      commercialCategory: "Strategic partnership / referral",
    };
  }
  if (mech === "SMALL_GROUP_LODGING") {
    return {
      opportunityType: OPPORTUNITY_TYPE.PRIMARY_PURSUIT,
      opportunityTypeLabel: "Executive Education Lodging",
      commercialCategory: "Executive education lodging",
    };
  }
  if (mech === "EXECUTIVE_DELEGATION") {
    return {
      opportunityType: OPPORTUNITY_TYPE.PRIMARY_PURSUIT,
      opportunityTypeLabel: "Event-Linked Executive Lodging",
      commercialCategory: "Event-linked executive lodging",
    };
  }
  if (mech === "EXHIBITOR_SPONSOR_LODGING") {
    return {
      opportunityType: OPPORTUNITY_TYPE.OVERFLOW_HOUSING,
      opportunityTypeLabel: "Exhibitor / Sponsor Lodging",
      commercialCategory: "Exhibitor / sponsor lodging",
    };
  }
  if (mech === "OFFSITE_MEETING" || mech === "VIP_HOSPITALITY") {
    return {
      opportunityType: OPPORTUNITY_TYPE.PRIMARY_PURSUIT,
      opportunityTypeLabel:
        mech === "VIP_HOSPITALITY" ? "VIP / Hospitality Program" : "Conference Offsite / Meetings",
      commercialCategory:
        mech === "VIP_HOSPITALITY" ? "VIP hospitality" : "Conference offsite / meetings",
    };
  }
  if (type && OPPORTUNITY_TYPE_LABEL[type]) {
    return {
      opportunityType: type,
      opportunityTypeLabel: OPPORTUNITY_TYPE_LABEL[type],
      commercialCategory: OPPORTUNITY_TYPE_LABEL[type],
    };
  }
  return {
    opportunityType: opp.opportunityType || OPPORTUNITY_TYPE.FUTURE_CYCLE,
    opportunityTypeLabel:
      opp.opportunityTypeLabel || OPPORTUNITY_TYPE_LABEL.FUTURE_CYCLE,
    commercialCategory: opp.opportunityTypeLabel || "Group demand prospect",
  };
}

/**
 * Buyer function / role from evidence — never invents a named person.
 */
export function deriveBuyerFunction(opp = {}) {
  const existing = text(
    opp.buyerRole ||
      opp.primaryContactRole ||
      opp.buyerFunction ||
      opp.buyerResolution?.buyerFunction ||
      opp.contactRole ||
      opp.primaryContact?.role ||
      opp.contact?.role ||
      opp.package?.contactRole ||
      opp.package?.buyerRole
  );
  // Reject pure negation stubs — not a usable buyer function label.
  if (
    existing &&
    !/^(unknown|n\/?a|none|not (a )?verified|tbd)$/i.test(existing) &&
    !/^not a verified/i.test(existing)
  ) {
    return existing.replace(/_/g, " ");
  }

  const mech = mechanismOf(opp);
  if (mech === "SMALL_GROUP_LODGING") return "Executive Education Operations";
  if (mech === "EXECUTIVE_DELEGATION") return "Meetings & Events";
  if (mech === "EXHIBITOR_SPONSOR_LODGING") return "Exhibitor Services";
  if (mech === "OFFSITE_MEETING") return "Meetings & Events";
  if (mech === "VIP_HOSPITALITY") return "VIP Hospitality";
  if (/howard de walden|marylebone village/i.test(text(opp.organizationName))) {
    return "Estate / Place Partnership";
  }
  // Segment-default functional desks — evidence-backed category, not a named person.
  const seg = String(opp.segment || opp.researchSegment || "").toLowerCase();
  if (/associations_government|medical_academic/.test(seg)) {
    return "Meetings / Housing / Conference Services";
  }
  if (/corporate_meetings_training|scaling_companies/.test(seg)) {
    return "Meetings & Events / Corporate Travel";
  }
  if (/exhibitor_sponsor/.test(seg)) return "Exhibitor / Sponsor Services";
  if (/sports|entertainment/.test(seg)) return "Event Operations / Hospitality";
  if (/professional_planners|tours_educational|weddings_social|local_meetings/.test(seg)) {
    return "Planner / Destination Services";
  }
  return "";
}

function evidenceStatusLabel(opp = {}) {
  const label = demandLabelOf(opp);
  if (label === DEMAND_EVIDENCE_LABEL.CONFIRMED_BUYING || label === "CONFIRMED") {
    return "Confirmed";
  }
  if (
    label === DEMAND_EVIDENCE_LABEL.INFERRED_DEMAND ||
    label === "INFERRED" ||
    /INFERRED/.test(label)
  ) {
    return "Inferred";
  }
  if (
    label === DEMAND_EVIDENCE_LABEL.PARTNERSHIP_WITHOUT_LODGING ||
    label === DEMAND_EVIDENCE_LABEL.SIGNAL_ONLY
  ) {
    return "Exploratory";
  }
  if (opp.confirmedBuying === true) return "Confirmed";
  return "Exploratory";
}

/**
 * Org-specific executive summary. Prefer buildGdiOpportunitySummary, then
 * enrich with mechanism + inferred labeling when E2D research fields exist.
 */
export function buildExecutiveSalesSummary(opp = {}) {
  const built = buildGdiOpportunitySummary(opp, { force: !text(opp.summaryWhat) });
  let summary = text(built.summaryWhat);
  const org = text(opp.organizationName || opp.company);
  const eventName = text(opp.eventName || opp.title);
  const timing = timingTrigger(opp);
  const mech = mechanismOf(opp);
  const status = evidenceStatusLabel(opp);
  const local = text(opp.localConnection);

  if (!summary || built.summaryQuality === SUMMARY_QUALITY.THIN) {
    const parts = [];
    if (org && eventName) {
      parts.push(
        `${org} is linked to ${eventName}${timing.window ? ` (${timing.window})` : ""}.`
      );
    } else if (org) {
      parts.push(`${org} is a researched demand-owner organization for this hotel.`);
    }
    if (local && /travel|overnight|lodging|delegate|participant|stay/i.test(local)) {
      parts.push(local.replace(/\s+/g, " ").trim());
    } else if (mech) {
      parts.push(
        `The supported commercial mechanism is ${mech.replace(/_/g, " ").toLowerCase()} — hotel need remains ${status.toLowerCase()} until lodging sourcing is confirmed.`
      );
    }
    summary = parts.join(" ").trim();
  }

  // Ensure inferred/confirmed language integrity.
  // Avoid "confirmed … room block" phrasing — summary quality treats that as fabrication
  // when lodging proof is absent (even in negations like "not a confirmed room block").
  if (status !== "Confirmed") {
    summary = summary
      .replace(/\bconfirmed demand\b/gi, "inferred demand opportunity")
      .replace(/\bopen room block\b/gi, "no verified room block")
      .replace(/\bactively sourcing\b/gi, "hotel sourcing unconfirmed")
      .replace(/\bnot a confirmed room block\b/gi, "without verified lodging sourcing")
      .replace(/\bno confirmed room block\b/gi, "without verified lodging sourcing");
    if (
      !/inferred|exploratory|unconfirmed|without verified lodging/i.test(summary) &&
      status === "Inferred"
    ) {
      summary =
        `${summary} Demand is inferred from verified activity and mechanism rationale — without verified lodging sourcing.`.trim();
    }
  }

  const quality = evaluateGdiSummaryQuality({ ...opp, summaryWhat: summary }, summary);
  return {
    summaryWhat: summary || null,
    summaryQuality: quality.quality,
    summaryInsufficient: quality.quality === SUMMARY_QUALITY.THIN || quality.quality === SUMMARY_QUALITY.INVALID,
    reasons: quality.reasons || [],
  };
}

/**
 * Why now — only when a real timing trigger exists.
 */
export function buildWhyNow(opp = {}) {
  const timing = timingTrigger(opp);
  const org = text(opp.organizationName) || "This organization";
  if (!timing.supported || !timing.window) {
    // Open-ended lodging / planner accounts do not require a single dated event.
    const type = String(opp.opportunityType || opp.detailLayout || "");
    if (
      /CORPORATE_ACCOUNT_RELATIONSHIP|VENUE_PARTNERSHIP|PREFERRED_LODGING/i.test(
        type
      )
    ) {
      const whyNow = `${org}: open-ended lodging / planner relationship — engage on current and near-term group needs. No single dated event is required for this account motion; hotel demand remains exploratory until sourcing is confirmed.`;
      return {
        whyNow,
        cardWhyNowLine: "Open-ended lodging account",
        // Action Timing is a separate dimension — open-ended CAR does not imply
        // Pursue Now / Future Watch / Too Early. Leave timing unclassified.
        bookingWindowStatus: null,
        ok: true,
        reason: "open_ended_lodging_account",
        sourceUrl: null,
      };
    }
    return {
      whyNow: null,
      cardWhyNowLine: null,
      ok: false,
      reason: "no_supported_timing_trigger",
    };
  }
  const eventName = text(opp.eventName) || "the supported program";
  const status = evidenceStatusLabel(opp).toLowerCase();
  const whyNow = `${org}: ${eventName} has a supported timing window of ${timing.window}. Use this window to open an exploratory conversation about lodging / hospitality needs — hotel demand remains ${status} until sourcing is confirmed.`;
  return {
    whyNow,
    cardWhyNowLine: `Supported timing: ${timing.window}`,
    // Canonical Action Timing: Future Watch (not legacy FUTURE_WINDOW enum).
    bookingWindowStatus: "WATCH",
    ok: true,
    reason: "future_demand_or_event_date",
    sourceUrl: timing.source || null,
  };
}

/**
 * Hotel-specific fit that is opportunity-specific — strip inventory boilerplate
 * when it is the only content.
 */
export function buildHotelSpecificFit(opp = {}) {
  const hotelName = text(opp.hotelDisplayName || opp.hotelName || "this hotel");
  const rationale = text(opp.hotelFitRationale || opp.fitExplanation || opp.summaryWhyHotel);
  const local = text(opp.localConnection);
  const mech = mechanismOf(opp).replace(/_/g, " ").toLowerCase();
  const status = evidenceStatusLabel(opp).toLowerCase();

  // Strip generic inventory brag lines that are not opportunity-specific.
  let cleaned = rationale
    .replace(/Luxury lifestyle inventory\s*\(\d+\s*keys?\)[^.]*\./gi, "")
    .replace(/private dining\s*\/\s*boardroom stock[^.]*\./gi, "")
    .replace(/\s+/g, " ")
    .trim();

  if (!cleaned || cleaned.length < 40) {
    const bits = [];
    if (local) bits.push(local);
    if (mech) {
      bits.push(
        `${hotelName} is a plausible fit for ${mech} demand in this catchment — lodging need remains ${status}.`
      );
    }
    cleaned = bits.join(" ").trim();
  }

  if (!cleaned) {
    return {
      summaryWhyHotel: null,
      hotelOpportunityThesis: null,
      fitExplanation: null,
      ok: false,
      reason: "insufficient_hotel_fit_evidence",
    };
  }

  return {
    summaryWhyHotel: cleaned,
    hotelOpportunityThesis: cleaned,
    fitExplanation: cleaned,
    hotelFitRationale: text(opp.hotelFitRationale) || cleaned,
    ok: true,
    reason: "research_field_hotel_fit",
  };
}

/**
 * Recommended action matching classification + contact route.
 */
export function buildRecommendedSalesAction(opp = {}) {
  const contact = extractContactSurface(opp);
  const mech = mechanismOf(opp);
  const disposition = String(opp.commercialDisposition || "").toUpperCase();
  const org = text(opp.organizationName) || "the organization";
  const route =
    contact.email ||
    contact.contactUrl ||
    text(opp.publicContactPath) ||
    "";
  const routeHint = contact.email
    ? `via ${contact.email}`
    : contact.contactUrl
      ? `via the published contact path`
      : "via a verified public route when available";

  if (disposition === "STRATEGIC_PARTNERSHIP") {
    return {
      recommendedAction: `Approach ${org} as a local partnership / private-events referral relationship ${routeHint} — do not pitch as a confirmed room-block buyer.`,
      ok: true,
      reason: "partnership_path",
    };
  }

  let action = "";
  if (mech === "SMALL_GROUP_LODGING") {
    action = `Contact executive education operations at ${org} ${routeHint} to ask whether visiting cohorts require external accommodation during multi-day modules — treat lodging need as inferred until confirmed.`;
  } else if (mech === "EXECUTIVE_DELEGATION") {
    action = `Contact the event organizer meetings desk at ${org} ${routeHint} to ask about executive / speaker hospitality arrangements for the supported program window — do not claim an open room block.`;
  } else if (mech === "EXHIBITOR_SPONSOR_LODGING") {
    action = `Contact ${org} exhibitor / gallery operations ${routeHint} to establish whether the participating team needs accommodation or a private client hospitality space during the fair — lodging demand remains inferred.`;
  } else if (mech === "OFFSITE_MEETING") {
    action = `Contact meetings & events at ${org} ${routeHint} to explore whether program partners need nearby offsite meeting space or rooms during the supported week — confirm before selling as ready demand.`;
  } else {
    action = recommendedActionForCommercialShape({
      ...opp,
      buyerRole: deriveBuyerFunction(opp),
      primaryContactRole: deriveBuyerFunction(opp),
    });
  }

  // Never invent named people into the action
  if (/\b(Mr\.|Ms\.|Mrs\.|Dr\.)\s+[A-Z][a-z]+/.test(action) && !text(opp.primaryContact?.name)) {
    action = recommendedActionForCommercialShape(opp);
  }

  return {
    recommendedAction: action,
    recommendedNextStep: action,
    ok: Boolean(action),
    reason: route ? "mechanism_plus_usable_route" : "mechanism_without_route",
  };
}

/**
 * Build a complete sales intelligence packet from existing research evidence.
 * @returns {{ ok, complete, gaps, packet, patched, evidenceUsed }}
 */
export function buildSalesIntelligencePacket(opp = {}, opts = {}) {
  const gaps = [];
  const evidenceUsed = [];
  const category = commercialCategoryForOpportunity(opp);
  const buyerFunction = deriveBuyerFunction(opp);
  const evidenceStatus = evidenceStatusLabel(opp);
  const demandLabel =
    opp.demandEvidenceLabel ||
    (evidenceStatus === "Confirmed"
      ? DEMAND_EVIDENCE_LABEL.CONFIRMED_BUYING
      : evidenceStatus === "Inferred"
        ? DEMAND_EVIDENCE_LABEL.INFERRED_DEMAND
        : DEMAND_EVIDENCE_LABEL.SIGNAL_ONLY);

  const summary = buildExecutiveSalesSummary(opp);
  evidenceUsed.push("localConnection", "eventName", "futureDemand", "organizationName");
  if (!summary.summaryWhat || summary.summaryInsufficient) {
    gaps.push("summary");
  }

  const why = buildWhyNow(opp);
  evidenceUsed.push("futureDemand.dateWindow", "eventStartDate");
  if (!why.ok) gaps.push("whyNow");

  const fit = buildHotelSpecificFit(opp);
  evidenceUsed.push("hotelFitRationale", "localConnection");
  if (!fit.ok) gaps.push("hotelFit");

  const action = buildRecommendedSalesAction(opp);
  evidenceUsed.push("eventDemandMechanism", "contact", "commercialDisposition");
  if (!action.ok) gaps.push("recommendedAction");

  const contact = extractContactSurface(opp);
  if (!contact.email && !contact.contactUrl && !contact.phone) {
    gaps.push("contact_route");
  }
  if (!buyerFunction) gaps.push("buyer_function");

  const title =
    text(opp.displayTitle) ||
    (text(opp.organizationName) && text(opp.eventName)
      ? `${opp.organizationName} — ${opp.eventName}`
      : text(opp.title)) ||
    null;

  const packet = {
    version: SALES_INTELLIGENCE_PACKET_VERSION,
    organizationName: text(opp.organizationName) || null,
    title,
    displayTitle: title,
    commercialCategory: category.commercialCategory,
    opportunityType: category.opportunityType,
    opportunityTypeLabel: category.opportunityTypeLabel,
    demandEvidenceLabel: demandLabel,
    evidenceStatusLabel: evidenceStatus,
    summaryWhat: summary.summaryWhat,
    summaryWhyMatters: summary.summaryWhat,
    summaryQuality: summary.summaryQuality,
    whyNow: why.whyNow,
    cardWhyNowLine: why.cardWhyNowLine,
    bookingWindowStatus: why.bookingWindowStatus || opp.bookingWindowStatus || null,
    summaryWhyHotel: fit.summaryWhyHotel,
    hotelOpportunityThesis: fit.hotelOpportunityThesis,
    fitExplanation: fit.fitExplanation,
    hotelFitRationale: fit.hotelFitRationale || opp.hotelFitRationale || null,
    buyerRole: buyerFunction || null,
    primaryContactRole: buyerFunction || null,
    buyerFunction: buyerFunction || null,
    buyerEntity: text(opp.organizationName) || null,
    recommendedAction: action.recommendedAction,
    recommendedNextStep: action.recommendedNextStep,
    contactEmail: contact.email || null,
    contactUrl: contact.contactUrl || null,
    primaryContactEmail: contact.email || opp.primaryContactEmail || null,
    publicContactPath: contact.contactUrl || opp.publicContactPath || null,
    eventStartDate: opp.eventStartDate || timingTrigger(opp).window || null,
    confirmedBuying: demandLabel === DEMAND_EVIDENCE_LABEL.CONFIRMED_BUYING,
  };

  const complete =
    gaps.length === 0 ||
    (gaps.length === 1 && gaps[0] === "contact_route" && opts.allowMissingContact !== false
      ? false
      : gaps.filter((g) => g !== "contact_route").length === 0 &&
        (contact.email || contact.contactUrl));

  // Strict complete: summary + whyNow + fit + action + buyer function + usable route
  const strictlyComplete =
    Boolean(packet.summaryWhat) &&
    !summary.summaryInsufficient &&
    Boolean(packet.whyNow) &&
    Boolean(packet.summaryWhyHotel) &&
    Boolean(packet.recommendedAction) &&
    Boolean(packet.buyerRole) &&
    Boolean(contact.email || contact.contactUrl);

  const patched = {
    ...opp,
    ...packet,
    primaryContact: {
      ...(opp.primaryContact || {}),
      email: contact.email || opp.primaryContact?.email || undefined,
      phone: contact.phone || opp.primaryContact?.phone || undefined,
      role: buyerFunction || opp.primaryContact?.role || undefined,
      organizationContactRoute:
        contact.contactUrl || opp.primaryContact?.organizationContactRoute || undefined,
      name: opp.primaryContact?.name || contact.person || undefined,
    },
    // Mark contact path researched when a usable route was already collected
    // (functional desk / email) — does not invent a named buyer.
    contactResearchAttempted:
      opp.contactResearchAttempted === true ||
      Boolean(contact.email || contact.contactUrl || contact.phone) ||
      Boolean(opp.contactResearch) ||
      Boolean(opp.buyerResolution),
    prospectCustomerLabel:
      opp.prospectCustomerLabel ||
      `${category.commercialCategory} · ${evidenceStatus}`,
    prospectCommercialLabel:
      opp.prospectCommercialLabel || category.commercialCategory,
    salesIntelligencePacket: {
      version: SALES_INTELLIGENCE_PACKET_VERSION,
      at: new Date().toISOString(),
      complete: strictlyComplete,
      gaps,
      evidenceStatusLabel: evidenceStatus,
      evidenceUsed: [...new Set(evidenceUsed)],
    },
    notes: [
      ...(opp.notes || []).filter((n) => !/^gdi_sales_intelligence_packet_v1/.test(String(n))),
      "gdi_sales_intelligence_packet_v1",
    ],
  };

  // Optional secondary / referral named contact — all segments; never invents channels
  const patchedWithSecondary = enrichOpportunityWithSecondaryNamedContact(patched);

  return {
    ok: strictlyComplete,
    complete: strictlyComplete,
    gaps,
    packet,
    patched: patchedWithSecondary,
    evidenceUsed: [...new Set(evidenceUsed)],
    summaryEval: summary,
  };
}

/**
 * Diagnose Ready field gaps for blocker-driven packet completion.
 */
export function diagnoseSalesPacketGaps(opp = {}, readyFailed = []) {
  const built = buildSalesIntelligencePacket(opp);
  const failed = [].concat(readyFailed || []);
  const next = [];
  if (failed.includes("summary_quality") || built.gaps.includes("summary")) {
    next.push({
      field: "summary",
      action: "synthesize_from_existing_evidence",
      canSynthesizeWithoutSearch: Boolean(built.packet.summaryWhat),
    });
  }
  if (failed.includes("why_now") || built.gaps.includes("whyNow")) {
    next.push({
      field: "whyNow",
      action: built.packet.whyNow
        ? "stamp_from_future_demand"
        : "research_timing_or_keep_blocker",
      canSynthesizeWithoutSearch: Boolean(built.packet.whyNow),
    });
  }
  if (failed.includes("recommended_action") || built.gaps.includes("recommendedAction")) {
    next.push({
      field: "recommendedAction",
      action: "derive_from_mechanism_and_contact",
      canSynthesizeWithoutSearch: Boolean(built.packet.recommendedAction),
    });
  }
  if (failed.includes("account_quality_insufficient")) {
    next.push({
      field: "accountQuality",
      action: "stamp_buyer_function_and_normalize_contact",
      canSynthesizeWithoutSearch: Boolean(built.packet.buyerRole),
    });
  }
  if (failed.includes("hotel_fit") || built.gaps.includes("hotelFit")) {
    next.push({
      field: "hotelFit",
      action: "map_hotelFitRationale_to_summaryWhyHotel",
      canSynthesizeWithoutSearch: Boolean(built.packet.summaryWhyHotel),
    });
  }
  return { built, next };
}

/**
 * Whether this opportunity should receive a lodging sales-intelligence packet.
 * Partnerships and market-intelligence signals use other workflows.
 */
export function shouldBuildLodgingSalesPacket(opp = {}) {
  const disposition = String(
    opp.commercialDisposition || opp.commercialDispositionClass || ""
  ).toUpperCase();
  if (
    disposition === "STRATEGIC_PARTNERSHIP" ||
    disposition === "MARKET_INTELLIGENCE" ||
    disposition === "LEGITIMATELY_REJECTED"
  ) {
    return false;
  }
  const demand = demandLabelOf(opp);
  if (
    demand === DEMAND_EVIDENCE_LABEL.PARTNERSHIP_WITHOUT_LODGING ||
    demand === DEMAND_EVIDENCE_LABEL.UNSUPPORTED
  ) {
    return false;
  }
  // SIGNAL_ONLY exploratory lodging prospects still need sales packets when
  // hotel-use / fit evidence exists — otherwise Ready never sees buyer/whyNow.
  if (demand === DEMAND_EVIDENCE_LABEL.SIGNAL_ONLY) {
    const lodgingEvidence = Boolean(
      text(opp.proposedHotelUse) ||
        text(opp.lodgingOrReferralRationale) ||
        text(opp.hotelFitRationale) ||
        text(opp.summaryWhyHotel) ||
        text(opp.package?.proposedHotelUse) ||
        text(opp.package?.hotelFitRationale)
    );
    if (!lodgingEvidence) return false;
  }
  const type = String(opp.opportunityType || "").toUpperCase();
  // Venue partnership without lodging referral stays non-lodging; preferred
  // lodging planners still receive packets when lodging evidence is present.
  if (type === "VENUE_PARTNERSHIP") {
    const lodgingEvidence = Boolean(
      text(opp.proposedHotelUse) ||
        text(opp.lodgingOrReferralRationale) ||
        text(opp.hotelFitRationale) ||
        text(opp.package?.proposedHotelUse)
    );
    if (!lodgingEvidence) return false;
  }
  return true;
}

/**
 * Apply packet synthesis when eligibility is hotel-facing but Ready fields
 * are missing and can be filled from existing evidence (no web search).
 * Idempotent: one synthesis attempt per opportunity unless force=true.
 */
export function applySalesIntelligencePacketIfNeeded(opp = {}, opts = {}) {
  if (!shouldBuildLodgingSalesPacket(opp) && opts.forceLodging !== true) {
    return {
      changed: false,
      skipped: true,
      reason: "non_lodging_disposition",
      opportunity: opp,
    };
  }
  if (
    (opp.notes || []).some((n) => String(n).includes("gdi_sales_intelligence_packet_v1")) &&
    opts.force !== true
  ) {
    // Already stamped — only re-apply when force, or when Ready still fails
    // and prior stamp left gaps that new evidence can fill.
    const alreadyComplete = opp.salesIntelligencePacket?.complete === true;
    if (alreadyComplete && opts.force !== true) {
      return { changed: false, skipped: true, reason: "already_complete", opportunity: opp };
    }
  }

  const built = buildSalesIntelligencePacket(opp, opts);
  const beforeKeys = ["summaryWhat", "whyNow", "recommendedAction", "buyerRole", "summaryWhyHotel"];
  const changed = beforeKeys.some(
    (k) =>
      Boolean(built.packet[k]) &&
      (!text(opp[k]) ||
        (k === "summaryWhat" && opp.summaryInsufficient === true) ||
        (k === "summaryWhat" && evaluateGdiSummaryQuality(opp).quality === SUMMARY_QUALITY.THIN) ||
        (k === "summaryWhat" && evaluateGdiSummaryQuality(opp).quality === SUMMARY_QUALITY.INVALID))
  );

  if (!changed && built.complete === false && !(opts.force === true)) {
    return {
      changed: false,
      skipped: false,
      reason: "insufficient_evidence_for_packet",
      gaps: built.gaps,
      opportunity: opp,
      built,
    };
  }

  return {
    changed: true,
    skipped: false,
    reason: opts.reason || "synthesize_from_existing_evidence",
    opportunity: built.patched,
    built,
    gaps: built.gaps,
    complete: built.complete,
  };
}
