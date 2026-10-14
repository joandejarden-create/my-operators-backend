/**
 * Drawer content contract — sales-card completeness separate from
 * qualification eligibility and contact-completeness grading.
 *
 * Unknown room volume or unconfirmed sourcing alone must not exclude a prospect.
 * Generic repeated filler must not satisfy content requirements.
 */

import { isGdiCustomerDrawerReady } from "../enrich-gdi-opportunity-for-customer.js";
import { evaluateProspectPublicationEligibility } from "../prospect-publication-eligibility-v1.js";
import { classifyContactCompletenessGroup } from "../prospect-publication-eligibility-v1.js";

export const DRAWER_CONTENT_CONTRACT_VERSION = "gdi-drawer-content-contract-v1";

const GENERIC_FILLER_RE =
  /\b(reach out to discuss|synerg(?:y|ies)|leverage our (?:full-service|premier)|world[- ]class (?:hospitality|experience)|perfect (?:venue|fit) for your (?:next )?event|contact us (?:today|for more)|we look forward to|please (?:let us know|contact)|general inquiries)\b/i;

const HOMEPAGE_ONLY_RE =
  /^https?:\/\/[^/]+\/?(?:en\/|es\/|de\/)?(?:home\/?)?(?:index\.(?:html?|php))?$/i;

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function hasUsefulText(v, min = 24) {
  const t = text(v);
  return t.length >= min && !GENERIC_FILLER_RE.test(t);
}

function evidencePresent(opp = {}) {
  if (text(opp.officialSource) && !HOMEPAGE_ONLY_RE.test(text(opp.officialSource))) {
    return true;
  }
  if (Array.isArray(opp.sources) && opp.sources.some((s) => text(s?.url || s))) {
    return true;
  }
  if (Array.isArray(opp.evidenceLinks) && opp.evidenceLinks.some((u) => text(u))) {
    return true;
  }
  if (text(opp.discoverySource)) return true;
  return false;
}

function contactRouteSummary(opp = {}) {
  const name = text(opp.primaryContact?.name || opp.primaryContactName || opp.contactName);
  const email = text(opp.primaryContact?.email || opp.functionalContactEmail || opp.contactEmail);
  const phone = text(opp.primaryContact?.phone || opp.contactPhone);
  const form = text(opp.functionalContactUrl || opp.contactFormUrl);
  const routingOnly =
    (!name || !/\s/.test(name)) &&
    Boolean(email || phone || form) &&
    !/\s/.test(name || "");
  return {
    hasRoute: Boolean(email || phone || form),
    namedPerson: Boolean(name && /\s/.test(name)),
    routingOnly: routingOnly || (!name && Boolean(email || phone || form)),
    email: Boolean(email),
    phone: Boolean(phone),
    form: Boolean(form),
  };
}

/**
 * Evaluate drawer sales-content completeness for one opportunity/card.
 * Does not mutate qualification or contact grades.
 */
export function evaluateDrawerContentCompleteness(opportunity = {}, opts = {}) {
  const failed = [];
  const warnings = [];
  const account = text(
    opportunity.organizationName || opportunity.company || opportunity.account
  );
  const occasion = text(
    opportunity.title || opportunity.opportunityName || opportunity.occasion
  );
  const timing =
    text(opportunity.eventStartDate) ||
    text(opportunity.timing?.start) ||
    text(opportunity.timing);
  const location =
    text(opportunity.destinationStatus) ||
    text(opportunity.city) ||
    text(opportunity.venue) ||
    text(opportunity.location);
  const thesis = text(
    opportunity.hotelOpportunityThesis ||
      opportunity.whyWyndhamFits ||
      opportunity.summaryWhyHotel ||
      opportunity.fitExplanation
  );
  const demand =
    text(opportunity.summaryWhat) ||
    text(opportunity.demandSignal) ||
    text(opportunity.whyNow);
  const caveats = text(
    opportunity.fitCaveats ||
      (Array.isArray(opportunity.materialLimitations)
        ? opportunity.materialLimitations.join("; ")
        : "") ||
      opportunity.sourcingStatus
  );
  const sourcing = text(
    opportunity.sourcingStatus ||
      opportunity.venueSourcingStatus ||
      opportunity.venueSourcingRationale
  );
  const nextAction = text(
    opportunity.recommendedAction ||
      opportunity.recommendedFirstAction ||
      opportunity.nextAction
  );
  const contact = contactRouteSummary(opportunity);

  if (!account) failed.push("account");
  if (!occasion) failed.push("occasion");
  if (!timing && !location) failed.push("timing_or_location");
  if (!hasUsefulText(demand, 20) && !hasUsefulText(thesis, 20)) {
    failed.push("opportunity_thesis");
  }
  if (!hasUsefulText(thesis, 40)) failed.push("hotel_specific_rationale");
  if (!sourcing && !caveats) failed.push("honest_sourcing_or_caveats");
  if (!contact.hasRoute) failed.push("contact_route");
  if (contact.hasRoute && contact.routingOnly) {
    warnings.push("routing_only_not_verified_named_contact");
  }
  if (!evidencePresent(opportunity)) failed.push("evidence_for_material_claims");
  if (!hasUsefulText(nextAction, 16)) failed.push("useful_next_action");

  // Explicit non-blockers
  const peakRooms =
    opportunity.peakRoomEstimate ?? opportunity.peakRooms ?? opportunity.estimatedPeakRooms;
  if (peakRooms == null || peakRooms === "" || /unknown/i.test(String(peakRooms))) {
    warnings.push("unknown_room_volume_allowed");
  }
  if (/unconfirmed|unknown|not established/i.test(sourcing)) {
    warnings.push("unconfirmed_sourcing_allowed");
  }

  if (GENERIC_FILLER_RE.test(thesis) || GENERIC_FILLER_RE.test(demand)) {
    failed.push("generic_filler");
  }

  const legacyDrawer = isGdiCustomerDrawerReady(opportunity);
  const elig = evaluateProspectPublicationEligibility(opportunity);
  const contactGroup =
    opportunity.contactCompletenessGroup ??
    classifyContactCompletenessGroup(opportunity);

  return {
    version: DRAWER_CONTENT_CONTRACT_VERSION,
    ok: failed.length === 0,
    failed,
    warnings,
    layers: {
      qualification: {
        eligibility: elig.eligibility || null,
        hotelFacing: elig.hotelFacing === true,
        enrichmentQueue: elig.enrichmentQueue === true,
        reasons: elig.reasons || [],
      },
      contactCompleteness: {
        group: contactGroup,
        route: contact,
      },
      drawerContent: {
        ok: failed.length === 0,
        failed,
      },
      legacyDrawerReady: legacyDrawer,
    },
    fieldsPresent: {
      account: Boolean(account),
      occasion: Boolean(occasion),
      timing: Boolean(timing),
      location: Boolean(location),
      thesis: Boolean(thesis),
      demand: Boolean(demand),
      sourcing: Boolean(sourcing),
      nextAction: Boolean(nextAction),
      evidence: evidencePresent(opportunity),
      contactRoute: contact.hasRoute,
    },
    note:
      opts.note ||
      "Qualification, contact completeness, and drawer content are reported separately.",
  };
}

/**
 * Batch report for a hotel package / opportunity list.
 */
export function reportDrawerCompleteness(opportunities = [], opts = {}) {
  const results = (opportunities || []).map((o) => ({
    id: o.id || o.opportunityId || o.cardId || null,
    organizationName: o.organizationName || o.company || o.account || null,
    title: o.title || o.opportunityName || null,
    evaluation: evaluateDrawerContentCompleteness(o, opts),
  }));
  const pass = results.filter((r) => r.evaluation.ok).length;
  const fail = results.length - pass;
  return {
    version: DRAWER_CONTENT_CONTRACT_VERSION,
    hotelId: opts.hotelId || null,
    total: results.length,
    pass,
    fail,
    results,
  };
}

/**
 * Map a Parallel lab San Diego card into a shape the drawer contract can score.
 */
export function mapLabCardToDrawerShape(card = {}, hotelId) {
  const c = card.contact || {};
  return {
    id: card.cardId || card.dedupeKey,
    hotelId,
    organizationName: card.account,
    title: text(card.timing?.raw) || card.account,
    eventStartDate: card.timing?.start || null,
    eventEndDate: card.timing?.end || null,
    demandSignal: card.demandSignal,
    summaryWhat: card.demandSignal,
    hotelOpportunityThesis: card.whyWyndhamFits,
    sourcingStatus: card.sourcingStatus,
    venueSourcingStatus: /open|housing|block|rfp/i.test(String(card.sourcingStatus || ""))
      ? "HOUSING_CHANNEL_OBSERVED"
      : "UNCONFIRMED",
    materialLimitations: card.materialLimitations || [],
    recommendedFirstAction: card.recommendedFirstAction,
    recommendedAction: card.recommendedFirstAction,
    evidenceLinks: card.evidenceLinks || [],
    officialSource: (card.evidenceLinks || [])[0] || null,
    primaryContact: {
      name: c.name || null,
      email: c.email || c.functionalEmail || null,
      phone: c.phone || c.switchboard || null,
      role: c.role || null,
    },
    functionalContactEmail: c.functionalEmail || null,
    noveltyVsBaseline: card.noveltyVsBaseline || "UNKNOWN",
    peakRoomEstimate: null,
    opportunityQualification: "QUALIFIED",
    opportunityType: "GROUP_DEMAND",
    hotelFitScore: 50,
    whyNow: card.demandSignal,
    customerEnrichmentVersion: "gdi_customer_enrichment_v1",
    discoverySource: "gdi_parallel_lab_replay_v1",
  };
}
