/**
 * Commercial delivery readiness, separate from publication eligibility.
 * Eligibility remains evaluateProspectPublicationEligibility via the V4.2 contract.
 * A customer explanation is ready only when it restates a stored fact or a
 * verified official-page extract. Template prose is not evidence.
 */

import { qualifyCanonicalOpportunity, CANONICAL_COMMERCIAL_CONTRACT } from "./canonical-commercial-contract-v1.js";
import { assignPriorityV1, deriveProspectCommercialSection } from "./priority-action-window/assignment-contract-v1.js";
import { marketTermsFromConfig } from "./workflow/gdi-demand-sourcing-v1.js";
import { citedOfficialUrls, classifyEvidenceGap, planExternalAction } from "./workflow/gdi-evidence-acquisition-v2.js";
import { reviewStoredEvidence } from "./workflow/gdi-live-evidence-yield-v1.js";
import { runOpportunityRecovery } from "./workflow/gdi-research-recovery-v2.js";
import { BLOCKER_KEY } from "./research/research-constants-v1.js";
import { HeuristicResearchPlanner, remainingPublicStrategiesForBlocker } from "./research/research-planner-v1.js";

export const COMMERCIAL_DELIVERY_READINESS_VERSION = "GDI_COMMERCIAL_DELIVERY_READINESS_V1";
export const CUSTOMER_DELIVERY_SNAPSHOT_VERSION = "GDI_CUSTOMER_SNAPSHOT_V4_4";

export const DELIVERY_CLASS = Object.freeze({
  ELIGIBLE_READY: "eligible_and_commercially_ready",
  ELIGIBLE_INCOMPLETE: "eligible_but_requiring_commercial_completion",
  INELIGIBLE: "currently_ineligible",
  HISTORICAL_NOT_CURRENT: "historically_published_but_no_longer_currently_eligible",
});

const DELIVERABLE_ROUTES = new Set([
  "verified_individual_buyer",
  "verified_organizing_department",
  "legitimate_functional_organizer_route",
]);

const GENERIC_THESIS_RE = /^(prospect to qualify|exploratory account|exploratory\b)/i;
const GENERIC_FILLER_RE = /\b(reach out to discuss|synerg(?:y|ies)|leverage our|world[- ]class|perfect venue|contact us today)\b/i;
const HOTEL_TOKEN_RE = /\b(mayflower|bethesda|marriott|wyndham|renaissance|waterstone|autograph|connecticut|harbor|bayside|downtown|overflow|corridor|national mall)\b/i;
const UNSUPPORTED_BOOKING_RE = /confirmed room block|open rfp|\brfp\b|venue is available|rooms are blocked|confirmed open buying|pursue now/i;
const STAFF_ONLY_RE = /\/(?:our-)?(?:team|people|staff)\/?$/i;
const RECURRENCE_RE = /\b(annual|recurring|every year|planning cycle|rotat\w*|recurrent|season)\b/i;
const ORG_STOPWORDS = new Set(["association", "national", "american", "international", "society", "group", "hotel", "conference", "meeting", "soccer", "club", "chapter", "the"]);

function text(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function redact(value) {
  const dates = [];
  const preserved = text(value).replace(/\b20\d{2}-\d{2}-\d{2}\b/g, (match) => {
    dates.push(match);
    return `DATE_${dates.length - 1}_TOKEN`;
  });
  return preserved
    .replace(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi, "[redacted-email]")
    .replace(/\+?\d[\d\s().-]{8,}\d/g, "[redacted-phone]")
    .replace(/DATE_(\d+)_TOKEN/g, (_, index) => dates[Number(index)]);
}

function useful(value, min = 40) {
  const raw = text(value);
  return raw.length >= min && !GENERIC_THESIS_RE.test(raw) && !GENERIC_FILLER_RE.test(raw);
}

function hotelTokens(config = {}) {
  const name = text(config.displayName || config.name);
  return name.split(/\W+/).filter((token) => token.length >= 5 && !ORG_STOPWORDS.has(token.toLowerCase()));
}

function mentionsHotel(value, config = {}) {
  const raw = text(value);
  if (HOTEL_TOKEN_RE.test(raw)) return true;
  return hotelTokens(config).some((token) => raw.toLowerCase().includes(token.toLowerCase()));
}

function withoutUnsupportedBooking(value) {
  const parts = text(value).split(/(?<=\.)\s+/);
  return parts.filter((part) => !UNSUPPORTED_BOOKING_RE.test(part)).join(" ").trim();
}

function fieldBlob(opportunity = {}, fields = []) {
  return fields.map((field) => text(opportunity[field])).filter(Boolean).join(" ");
}

function marketTermIn(value, config = {}) {
  const blob = text(value).toLowerCase();
  if (!blob) return null;
  const terms = marketTermsFromConfig(config).slice().sort((left, right) => right.length - left.length);
  return terms.find((term) => blob.includes(String(term).toLowerCase())) || null;
}

function locationFields(opportunity = {}) {
  return fieldBlob(opportunity, [
    "eventLocationSummary",
    "destinationStatus",
    "eventCity",
    "venueCity",
    "city",
    "eventLocationLabel",
    "venue",
  ]);
}

function factValue(opportunity = {}, pattern) {
  const rows = [
    ...(opportunity.evidence || []),
    ...(opportunity.establishedFacts || []).map((item) => (typeof item === "string" ? { value: item, claimKind: "FACT", field: "established" } : item)),
  ];
  return rows.find((item) => String(item?.claimKind || "FACT").toUpperCase() === "FACT" && pattern.test(`${item.field || ""} ${item.value || ""}`) && text(item.value)) || null;
}

function demandSources(opportunity = {}) {
  return citedOfficialUrls(opportunity).filter((url) => {
    if (STAFF_ONLY_RE.test(url)) return false;
    try {
      const parsed = new URL(url);
      if (parsed.pathname === "" || parsed.pathname === "/") return false;
    } catch {
      return false;
    }
    return true;
  });
}

function datesIn(value) {
  const found = [];
  for (const match of String(value || "").matchAll(/\b(20\d{2})-(\d{2})-(\d{2})\b/g)) found.push(match[0]);
  const months = ["january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december"];
  for (const match of String(value || "").matchAll(/\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\s+(20\d{2})\b/gi)) {
    const month = String(months.indexOf(match[1].toLowerCase()) + 1).padStart(2, "0");
    found.push(`${match[3]}-${month}-${String(match[2]).padStart(2, "0")}`);
  }
  return found;
}

function organizationConfirmed(opportunity = {}, pageText = "") {
  const name = text(opportunity.organizationName);
  const page = pageText.toLowerCase();
  if (!name || !page) return false;
  if (page.includes(name.toLowerCase())) return true;
  const tokens = name.split(/\W+/).filter((token) => token.length >= 5 && !ORG_STOPWORDS.has(token.toLowerCase()));
  if (!tokens.length) return false;
  return tokens.some((token) => page.includes(token.toLowerCase()));
}

function supportedSalesAction(opportunity = {}, contract) {
  const organization = text(opportunity.organizationName) || "this organization";
  const mechanism = contract.demand.mechanism;
  if (mechanism === "ACTIVE_SOURCING" && contract.demand.openBookingClaimed === true) {
    return `Stored sourcing rationale supports an open housing question for ${organization}. Contact the verified organizing route. Do not assume a room block at this hotel.`;
  }
  if (mechanism === "FUTURE_ACCOUNT_DEVELOPMENT") {
    return `No open room block or RFP is stored for ${organization}. The supported next step is an account-development conversation with the verified organizing route about a future group program.`;
  }
  if (mechanism === "MONITORING") {
    return `Monitor ${organization} through the verified organizing route. No open room block or RFP is claimed.`;
  }
  return "";
}

function blockerFor(failed = []) {
  if (failed.includes("destination_relevance")) return BLOCKER_KEY.HOTEL_FIT;
  if (failed.includes("evidence_supported_group_demand")) return BLOCKER_KEY.CORROBORATION;
  if (failed.includes("timing_or_planning_cycle")) return BLOCKER_KEY.FUTURE_DEMAND;
  if (failed.includes("property_specific_hotel_fit")) return BLOCKER_KEY.HOTEL_FIT;
  if (failed.includes("verified_route")) return BLOCKER_KEY.CONTACT_ROUTE;
  if (failed.includes("sales_action") || failed.includes("explainable_prioritization")) return BLOCKER_KEY.DRAWER_COMPLETENESS;
  return null;
}

function criterion(id, mandatory, status, provenance = "") {
  return { id, mandatory, status, provenance: provenance || null };
}

function displayPriority(value) {
  const raw = text(value).toUpperCase();
  if (raw.startsWith("HIGH")) return "HIGH";
  if (raw.startsWith("MEDIUM")) return "MEDIUM";
  if (raw.startsWith("WATCH")) return "WATCHLIST";
  if (raw.startsWith("DISQUAL")) return "DISQUALIFIED";
  return raw;
}

export function planMaterialResearch(opportunity = {}, failed = [], config = null) {
  const material = failed.filter((id) => id !== "specific_future_date" && id !== "named_buyer" && id !== "peak_room_count");
  if (!material.length) {
    return {
      material: false,
      blocker: null,
      strategy: null,
      remainingPublicStrategies: [],
      paidBlocked: true,
      externalAction: "ARCHIVE_SUFFICIENT",
      officialUrl: citedOfficialUrls(opportunity)[0] || null,
      reason: "No material commercial gap remains.",
    };
  }
  const blocker = blockerFor(material);
  const planner = new HeuristicResearchPlanner();
  const planned = blocker
    ? planner.plan({ opportunity, blocker: { key: blocker }, options: { paidBlocked: true } })
    : { strategy: null };
  const gap = classifyEvidenceGap(opportunity, { hotelId: opportunity.hotelId });
  const external = planExternalAction(opportunity, gap, { allowPublicFetch: true });
  return {
    material: true,
    gaps: material,
    blocker,
    strategy: planned.strategy || null,
    strategyNotExecuted: true,
    remainingPublicStrategies: blocker ? remainingPublicStrategiesForBlocker(opportunity, blocker) : [],
    paidBlocked: true,
    externalAction: external.action,
    officialUrl: gap.firstOfficialUrl || null,
    reason: external.reason || planned.skipReason || "A public research action is identified. Paid search is not authorized.",
  };
}

export function adoptOfficialPageEvidence({ opportunity = {}, extraction = {}, pageText = "", config = {}, hotelName = "" } = {}) {
  const reviewed = reviewStoredEvidence(extraction);
  const rejected = [...(reviewed.rejectedInferences || [])];
  const page = text(pageText);
  const patch = {};
  const adopted = [];
  if (!organizationConfirmed(opportunity, page)) {
    rejected.push("page_did_not_confirm_organization");
    return { adopted, rejected, patch };
  }
  if (reviewed.eventDate && reviewed.eventDateEvidence && page.toLowerCase().includes(String(reviewed.eventDate).toLowerCase())) {
    patch.timingText = reviewed.eventDate;
    patch.timingProvenance = `official_page:${reviewed.sourceUrl || extraction.sourceUrl || ""}`;
    patch.timingKind = "date";
    adopted.push("timing_or_planning_cycle");
  }
  if (!rejected.includes("location_was_only_in_the_url")) {
    const term = marketTermIn(page, config);
    if (term) {
      patch.locationText = term;
      patch.locationProvenance = `official_page_text:${reviewed.sourceUrl || extraction.sourceUrl || ""}`;
      adopted.push("destination_relevance");
    }
  }
  if (reviewed.venueCommitment === "THIS_HOTEL_NAMED_WITH_LODGING_LANGUAGE" && reviewed.lodgingEvidence && mentionsHotel(reviewed.lodgingEvidence, { displayName: hotelName, ...config })) {
    patch.fitText = redact(`Official page lodging language: ${reviewed.lodgingEvidence}`);
    patch.fitProvenance = `official_page:${reviewed.sourceUrl || extraction.sourceUrl || ""}`;
    adopted.push("property_specific_hotel_fit");
  } else if (reviewed.lodgingEvidence) {
    rejected.push("lodging_language_does_not_name_this_hotel");
  }
  if (reviewed.recurringOrFuture === "RECURRING_LANGUAGE_WITH_DATED_EVENT" && patch.timingText) {
    patch.planningCycleText = "The official page states a dated recurring event.";
    patch.planningCycleProvenance = patch.timingProvenance;
  }
  return { adopted, rejected, patch };
}

export function assessCommercialDeliveryReadiness(opportunity = {}, context = {}) {
  const config = context.config || null;
  const today = context.today || "2026-10-22";
  const contract = context.contract || qualifyCanonicalOpportunity(opportunity, { config });
  const overlay = opportunity.stagedCommercial || {};
  const baselineHotelFacing = context.baselineHotelFacing != null ? context.baselineHotelFacing === true : contract.publication.hotelFacing === true;
  const baselineEligibility = context.baselineEligibility || contract.publication.eligibility;
  const historical = contract.publication.historicalCustomerVisible === true || context.historicalCustomerVisible === true;
  const wordingDefects = [];
  const criteria = [];

  const organization = text(opportunity.organizationName);
  criteria.push(criterion("organization_identity", true, organization.length >= 2 ? "pass" : "fail", organization ? "organizationName" : ""));

  const sources = demandSources(opportunity);
  const demandFact = factValue(opportunity, /demand|lodging|housing|meeting|group|attendance/i);
  const demandPass = contract.demand.mechanism !== "NOT_ACTIONABLE" && sources.length > 0;
  criteria.push(criterion("evidence_supported_group_demand", true, demandPass ? "pass" : "fail", sources[0] || (demandFact ? `evidence.${demandFact.field}` : "")));

  const storedLocation = locationFields(opportunity);
  const titleAndRationale = fieldBlob(opportunity, ["title", "venueSourcingRationale", "roomDemandRationale", "whyNow", "summaryWhat"]);
  const overlayLocation = text(overlay.locationText);
  const locationTerm = marketTermIn(`${storedLocation} ${titleAndRationale} ${overlayLocation}`, config);
  const orgOnlyTerm = !locationTerm && marketTermIn(organization, config);
  let destinationStatus = "fail";
  let destinationProvenance = "";
  let destinationLabel = contract.demand.destination;
  if (contract.demand.destination === "outside") {
    destinationStatus = "fail";
    destinationProvenance = "destination_outside";
  } else if (overlayLocation && marketTermIn(overlayLocation, config)) {
    destinationStatus = "pass";
    destinationLabel = overlayLocation;
    destinationProvenance = overlay.locationProvenance || "staged_official_page";
  } else if (locationTerm && (storedLocation || titleAndRationale)) {
    destinationStatus = "pass";
    destinationLabel = locationTerm;
    destinationProvenance = storedLocation ? "stored_location_field" : "stored_title_or_rationale";
  } else if (orgOnlyTerm) {
    destinationStatus = "fail";
    destinationProvenance = "destination_only_in_organization_name";
  }
  criteria.push(criterion("destination_relevance", true, destinationStatus, destinationProvenance));

  const fitSource = text(overlay.fitText) || [opportunity.hotelOpportunityThesis, opportunity.fitExplanation, opportunity.fitRationale, opportunity.summaryWhyHotel, opportunity.hotelFitRationale]
    .map(text)
    .find((value) => useful(value, 40)) || "";
  const fitClean = withoutUnsupportedBooking(fitSource);
  if (fitSource && fitClean !== fitSource) wordingDefects.push({ id: "unsupported_booking_language", repaired: Boolean(useful(fitClean, 40)) });
  const fitPass = useful(fitClean, 40) && mentionsHotel(fitClean, config);
  criteria.push(criterion("property_specific_hotel_fit", true, fitPass ? "pass" : "fail", overlay.fitProvenance || (fitPass ? "stored_hotel_fit_text" : "")));

  const storedDate = text(opportunity.eventStartDate || opportunity.timing?.start);
  const storedNote = text(opportunity.timingNote || (typeof opportunity.timing === "string" ? opportunity.timing : ""));
  const dateFact = factValue(opportunity, /date|timing|start/i);
  const rationaleYear = fieldBlob(opportunity, ["venueSourcingRationale", "whyNow", "summaryWhyNow", "roomDemandRationale"]).match(/\b20(2[6-9]|3[0-5])\b/);
  const recurrenceBlob = fieldBlob(opportunity, ["title", "whyNow", "summaryWhat", "summaryWhyNow", "roomDemandRationale"]);
  const accountDevelopment = contract.demand.mechanism === "FUTURE_ACCOUNT_DEVELOPMENT" || contract.demand.mechanism === "MONITORING";
  const planningCycle = accountDevelopment && contract.demand.openBookingClaimed !== true && (opportunity.standingOpportunity === true || RECURRENCE_RE.test(recurrenceBlob) || text(overlay.planningCycleText));
  let timingText = "";
  let timingProvenance = "";
  let timingKind = "";
  if (text(overlay.timingText)) {
    timingText = text(overlay.timingText);
    timingProvenance = overlay.timingProvenance || "staged_official_page";
    timingKind = overlay.timingKind || "date";
  } else if (storedDate) {
    timingText = storedDate;
    timingProvenance = "stored_event_date";
    timingKind = "date";
  } else if (useful(storedNote, 12)) {
    timingText = storedNote;
    timingProvenance = "stored_timing_note";
    timingKind = "note";
  } else if (dateFact) {
    timingText = text(dateFact.value);
    timingProvenance = `evidence.${dateFact.field}`;
    timingKind = "fact";
  } else if (planningCycle) {
    timingText = text(overlay.planningCycleText) || "Stored record supports a recurring or planning-cycle program. A specific future event date is not required for this account-development approach.";
    timingProvenance = overlay.planningCycleProvenance || "stored_recurrence_language";
    timingKind = "planning_cycle";
  } else if (rationaleYear && contract.demand.mechanism === "ACTIVE_SOURCING") {
    timingText = `Stored rationale mentions ${rationaleYear[0]}. A specific event date is not stored.`;
    timingProvenance = "stored_rationale_year";
    timingKind = "planning_window";
  }
  const parsedDates = datesIn(timingText);
  const concluded = timingKind !== "planning_cycle" && parsedDates.length > 0 && parsedDates.every((day) => day < today);
  if (concluded) timingProvenance = "date_before_current_cycle";
  criteria.push(criterion("timing_or_planning_cycle", true, timingText && !concluded ? "pass" : "fail", timingProvenance));
  criteria.push(criterion("specific_future_date", false, storedDate || timingKind === "date" ? "pass" : "optional_absent", storedDate ? "stored_event_date" : ""));

  const routePass = contract.readiness.usableRoute === true && DELIVERABLE_ROUTES.has(contract.evidence.routeKind);
  criteria.push(criterion("verified_route", true, routePass ? "pass" : "fail", contract.evidence.routeKind || ""));
  const named = text(opportunity.primaryContact?.name || opportunity.primaryContactName);
  criteria.push(criterion("named_buyer", false, named ? "pass" : "optional_absent", named ? "stored_contact_name" : ""));

  const classificationPass = Boolean(contract.demand.mechanism) && !(contract.demand.openBookingClaimed === true && contract.demand.mechanism !== "ACTIVE_SOURCING");
  criteria.push(criterion("demand_classification", true, classificationPass ? "pass" : "fail", contract.demand.mechanism || ""));
  if (/RFP_ACTIVE_SOURCING|^OPEN$/i.test(text(opportunity.venueSourcingStatus)) && contract.demand.openBookingClaimed !== true) {
    wordingDefects.push({ id: "stored_sourcing_status_not_current_mechanism", repaired: true });
  }

  const action = supportedSalesAction(opportunity, contract);
  const rawAction = text(opportunity.recommendedAction || opportunity.recommendedNextStep || opportunity.bookingWindowReason);
  if (contract.demand.openBookingClaimed !== true && UNSUPPORTED_BOOKING_RE.test(rawAction)) {
    wordingDefects.push({ id: "action_overclaims_booking", repaired: Boolean(action) });
  }
  criteria.push(criterion("sales_action", true, action ? "pass" : "fail", action ? "contract_mechanism_and_verified_route" : ""));

  const assigned = assignPriorityV1(opportunity);
  let stagedPriority = displayPriority(assigned.priority);
  let priorityReason = assigned.reason || "";
  const falseHigh = stagedPriority === "HIGH" && /confirmed open buying|active sourcing/i.test(priorityReason) && contract.demand.openBookingClaimed !== true;
  if (falseHigh) {
    stagedPriority = fitPass ? "MEDIUM" : "WATCHLIST";
    priorityReason = "Hotel fit and the verified route support follow-up. Confirmed open buying is not stored, so High is not used.";
    wordingDefects.push({ id: "priority_used_unsupported_open_buying", repaired: true });
  }
  const fitScore = Number(opportunity.hotelFitScore);
  const unexplainedScore = Number.isFinite(fitScore) && !fitPass;
  if (unexplainedScore) wordingDefects.push({ id: "fit_score_without_explainable_basis", repaired: false });
  const priorityPass = ["HIGH", "MEDIUM", "WATCHLIST"].includes(stagedPriority)
    && Boolean(priorityReason)
    && !unexplainedScore
    && !(stagedPriority === "HIGH" && contract.demand.openBookingClaimed !== true && /confirmed open buying/i.test(priorityReason));
  criteria.push(criterion("explainable_prioritization", true, priorityPass ? "pass" : "fail", unexplainedScore ? "fit_score_without_prose" : priorityReason));
  criteria.push(criterion("peak_room_count", false, opportunity.estimatedPeakRooms || opportunity.peakRooms ? "pass" : "optional_absent", ""));

  const storedSection = deriveProspectCommercialSection(opportunity, { eligibility: contract.eligibility });
  const derivedSection = deriveProspectCommercialSection({ ...opportunity, prospectReviewSection: null }, { eligibility: contract.eligibility });
  let section = storedSection.section;
  let sectionSource = storedSection.source;
  if (section === "PRIORITY" && contract.demand.openBookingClaimed !== true) {
    section = derivedSection.section === "PRIORITY" ? "EXPLORATORY" : (derivedSection.section || "EXPLORATORY");
    sectionSource = "open_booking_not_claimed";
    wordingDefects.push({ id: "priority_section_without_open_booking", repaired: true });
  }
  criteria.push(criterion("consistent_presentation", true, baselineHotelFacing ? (section ? "pass" : "fail") : "pass", sectionSource || ""));

  const recoveryBlock = ["REJECTED", "SUPERSEDED"].includes(opportunity.recovery?.lifecycle) || opportunity.recovery?.suppressPursuit === true;
  if (recoveryBlock) {
    wordingDefects.push({ id: `recovery_${opportunity.recovery?.reason || opportunity.recovery?.lifecycle}`, repaired: false });
    criteria.push(criterion("current_cycle", true, "fail", opportunity.recovery?.reason || opportunity.recovery?.lifecycle || "recovery_blocked"));
  }

  const mandatoryFailed = criteria.filter((item) => item.mandatory && item.status === "fail").map((item) => item.id);
  const commerciallyReady = baselineHotelFacing && contract.publication.hotelFacing === true && mandatoryFailed.length === 0 && !recoveryBlock && contract.readiness.disqualified !== true;
  let deliveryClass = DELIVERY_CLASS.INELIGIBLE;
  if (historical && !baselineHotelFacing) deliveryClass = DELIVERY_CLASS.HISTORICAL_NOT_CURRENT;
  else if (!baselineHotelFacing || contract.readiness.disqualified === true) deliveryClass = DELIVERY_CLASS.INELIGIBLE;
  else if (commerciallyReady) deliveryClass = DELIVERY_CLASS.ELIGIBLE_READY;
  else deliveryClass = DELIVERY_CLASS.ELIGIBLE_INCOMPLETE;

  const venueFact = opportunity.recovery?.whyNowEvidence?.value ? redact(opportunity.recovery.whyNowEvidence.value) : "";
  return {
    version: COMMERCIAL_DELIVERY_READINESS_VERSION,
    policyVersion: CANONICAL_COMMERCIAL_CONTRACT,
    opportunityId: contract.opportunityId,
    hotelId: contract.hotelId || opportunity.hotelId || null,
    organization,
    baselineEligibility,
    baselineHotelFacing,
    stagedEligibility: contract.publication.eligibility,
    stagedHotelFacing: contract.publication.hotelFacing === true,
    historicalCustomerVisible: historical,
    accountDevelopmentCandidate: contract.publication.accountDevelopmentCandidate === true,
    accountDevelopmentPublished: false,
    deliveryClass,
    commerciallyReady,
    mechanism: contract.demand.mechanism,
    openBookingClaimed: contract.demand.openBookingClaimed === true,
    routeKind: contract.evidence.routeKind,
    destinationLabel: redact(destinationLabel),
    destinationProvenance,
    fitText: fitPass ? redact(fitClean) : "",
    fitProvenance: fitPass ? (overlay.fitProvenance || "stored_hotel_fit_text") : "",
    timingText: redact(timingText),
    timingProvenance,
    timingKind,
    salesAction: action,
    stagedPriority,
    priorityReason,
    section,
    sectionSource,
    criteria,
    missingEvidence: mandatoryFailed,
    wordingDefects,
    archivedVenueFact: venueFact,
    researchPlan: planMaterialResearch(opportunity, mandatoryFailed, config),
    severity: commerciallyReady ? "none" : mandatoryFailed.some((id) => ["sales_action", "explainable_prioritization", "consistent_presentation"].includes(id)) && mandatoryFailed.every((id) => ["sales_action", "explainable_prioritization", "consistent_presentation"].includes(id)) ? "major" : "blocker",
  };
}

export function recoverCommercialReadiness(opportunity = {}, context = {}) {
  const config = context.config || null;
  const baseline = qualifyCanonicalOpportunity(opportunity, { config });
  const recovery = runOpportunityRecovery(opportunity, {
    hotelId: context.hotelId || opportunity.hotelId,
    siblings: context.siblings || [],
    canonicalContacts: context.canonicalContacts || [],
    today: context.today,
    feedbackSuppress: context.feedbackSuppress === true,
  });
  const staged = recovery.opportunity || opportunity;
  if (context.officialPatch) staged.stagedCommercial = { ...(staged.stagedCommercial || {}), ...context.officialPatch };
  const assessment = assessCommercialDeliveryReadiness(staged, {
    config,
    baselineHotelFacing: baseline.publication.hotelFacing === true,
    baselineEligibility: baseline.publication.eligibility,
    historicalCustomerVisible: baseline.publication.historicalCustomerVisible === true,
    today: context.today,
  });
  return {
    recovery,
    staged,
    assessment,
    publicationChanged: recovery.publicationChanged === true || staged.customerVisible !== opportunity.customerVisible,
    researchCostUsd: recovery.researchCostUsd || 0,
  };
}

export function customerCardFromAssessment(result = {}) {
  const assessment = result.assessment || result;
  const certified = assessment.commerciallyReady === true && assessment.deliveryClass === DELIVERY_CLASS.ELIGIBLE_READY;
  return {
    opportunityId: assessment.opportunityId,
    hotelId: assessment.hotelId,
    organization: assessment.organization,
    policyVersion: CANONICAL_COMMERCIAL_CONTRACT,
    readinessVersion: COMMERCIAL_DELIVERY_READINESS_VERSION,
    snapshotVersion: CUSTOMER_DELIVERY_SNAPSHOT_VERSION,
    publicationEligibility: assessment.baselineEligibility,
    hotelFacing: assessment.baselineHotelFacing === true,
    historicalCustomerVisible: assessment.historicalCustomerVisible === true,
    deliveryClass: assessment.deliveryClass,
    commerciallyReady: certified,
    certified,
    demandMechanism: assessment.mechanism,
    openBookingClaimed: assessment.openBookingClaimed === true,
    destination: assessment.destinationLabel,
    destinationProvenance: assessment.destinationProvenance,
    hotelFit: assessment.fitText,
    fitProvenance: assessment.fitProvenance,
    timing: assessment.timingText,
    timingProvenance: assessment.timingProvenance,
    timingKind: assessment.timingKind,
    routeKind: assessment.routeKind,
    salesAction: assessment.salesAction,
    priority: assessment.stagedPriority,
    priorityReason: assessment.priorityReason,
    section: assessment.section,
    sectionSource: assessment.sectionSource,
    wordingDefects: (assessment.wordingDefects || []).map((item) => item.id),
    archivedVenueFact: assessment.archivedVenueFact || "",
    accountDevelopmentPublished: false,
    productionPublication: false,
  };
}

export function projectCertifiedList(cards = []) {
  return cards.filter((card) => card.certified === true).map((card) => ({
    opportunityId: card.opportunityId,
    hotelId: card.hotelId,
    section: card.section,
    priority: card.priority,
    hotelFacing: card.hotelFacing,
    deliveryClass: card.deliveryClass,
    historicalCustomerVisible: card.historicalCustomerVisible,
  }));
}

export function projectCertifiedPdf(cards = []) {
  return cards.filter((card) => card.certified === true).map((card) => ({
    opportunityId: card.opportunityId,
    hotelId: card.hotelId,
    hotelFacing: card.hotelFacing,
    deliveryClass: card.deliveryClass,
    demandMechanism: card.demandMechanism,
    openBookingClaimed: card.openBookingClaimed,
    hotelFit: card.hotelFit,
    timing: card.timing,
    salesAction: card.salesAction,
  }));
}

export function projectCertifiedShare(cards = []) {
  return cards.filter((card) => card.certified === true).map((card) => ({
    opportunityId: card.opportunityId,
    hotelId: card.hotelId,
    hotelFacing: card.hotelFacing,
    deliveryClass: card.deliveryClass,
    demandMechanism: card.demandMechanism,
    openBookingClaimed: card.openBookingClaimed,
    accountDevelopmentPublished: false,
  }));
}

export function weeklyReadinessDecision({ prior, next, disqualified = false, newEvidence = false, feedbackHotelId = null, recordHotelId = null } = {}) {
  if (feedbackHotelId && recordHotelId && feedbackHotelId !== recordHotelId) {
    return { applied: false, stable: true, includeInCustomerSnapshot: prior?.deliveryClass === DELIVERY_CLASS.ELIGIBLE_READY, release: false, reason: "feedback_hotel_isolated" };
  }
  if (disqualified || next?.historicalCustomerVisible === true && next?.baselineHotelFacing === false) {
    return { applied: false, stable: false, includeInCustomerSnapshot: false, release: false, reason: "historical_or_disqualified_cannot_reenter" };
  }
  if (!newEvidence && prior && next && prior.opportunityId === next.opportunityId && prior.deliveryClass === next.deliveryClass && prior.demandMechanism === next.demandMechanism && prior.hotelFit === next.hotelFit && prior.timing === next.timing) {
    return { applied: false, stable: true, includeInCustomerSnapshot: next.deliveryClass === DELIVERY_CLASS.ELIGIBLE_READY, release: false, reason: "unchanged" };
  }
  if (newEvidence) {
    return {
      applied: false,
      stable: false,
      includeInCustomerSnapshot: next?.deliveryClass === DELIVERY_CLASS.ELIGIBLE_READY && next?.baselineHotelFacing === true,
      release: false,
      reason: "new_evidence_reassessed_not_released",
    };
  }
  return { applied: false, stable: false, includeInCustomerSnapshot: false, release: false, reason: "review_required" };
}

export function renderCertifiedSnapshotHtml(snapshot, hotelName) {
  const cards = snapshot.certified || [];
  const rows = cards.map((card) => `<article data-certified="true">
    <h2>${escapeHtml(card.organization)}</h2>
    <p>Section ${escapeHtml(card.section || "none")} · Priority ${escapeHtml(card.priority || "none")} · ${escapeHtml(card.demandMechanism)}</p>
    <p>Destination: ${escapeHtml(card.destination)}. Timing: ${escapeHtml(card.timing)}</p>
    <p>${escapeHtml(card.hotelFit)}</p>
    <p>Next action: ${escapeHtml(card.salesAction)}</p>
    <p>Open booking claimed: ${card.openBookingClaimed ? "yes" : "no"}. Evidence: ${escapeHtml(card.fitProvenance)}; ${escapeHtml(card.timingProvenance)}.</p>
  </article>`).join("");
  return `<!doctype html><html><head><meta charset="utf-8"><title>${escapeHtml(hotelName)} commercially ready snapshot</title>
    <style>body{font-family:Georgia,serif;margin:32px;background:#f6f4ef;color:#1c1915}article{background:#fff;border:1px solid #ddd;padding:16px 20px;margin:0 0 12px}h1{font-size:22px}h2{font-size:16px;margin:0 0 6px}p{margin:4px 0}</style>
    </head><body><h1>${escapeHtml(hotelName)} commercially ready staged snapshot</h1>
    <p>Staging only. Not a production publication. ${cards.length} certified of ${snapshot.eligibleCount} hotel-facing opportunities. Incomplete eligible records are withheld from this customer view.</p>
    ${rows || "<p>No commercially ready opportunities.</p>"}</body></html>`;
}

function escapeHtml(value) {
  return String(value || "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}
