/**
 * Local Meetings & Catering research segment V1 (+ subsegment expansion).
 *
 * Qualifies local businesses / organizers / professional planners that may
 * produce meeting-room rental, F&B, or social functions — including opportunities
 * that require NO guestrooms.
 *
 * Preferred-lodging partnerships with outside venues stay on the Private Events
 * path (VENUE_REFERRAL). Cross-reference related accounts; do not duplicate.
 *
 * Guestrooms are NOT a qualification requirement.
 * Generic company directories do not qualify.
 * A nearby event venue is not automatically a catering buyer.
 * Dated growth evidence is NOT required (that belongs to scaling-companies).
 * Do not invent kosher/outside-catering capability; mark unknowns.
 * Do not collect children's / private family / religion-inference data.
 */

import { createHash } from "node:crypto";
import { OPPORTUNITY_TYPE } from "../claim-types.js";
import { DEMAND_FAMILY, DEMAND_SIGNAL_TYPE } from "../demand-signal-types.js";
import {
  ORGANIZATION_TYPE,
  GENERATOR_STATUS,
  PROGRAM_TYPE,
  RECURRENCE_STATUS,
} from "../demand-generators/constants.js";
import {
  computeDemandGeneratorId,
  computeProgramId,
  buildDemandGeneratorEntity,
  buildProgramEntity,
  domainFromUrl,
} from "../demand-generators/entities.js";
import {
  computeHotelGeneratorFitId,
  buildHotelDemandGeneratorFit,
} from "../demand-generators/fit-relevance.js";
import {
  DEMAND_USE_CASE,
  GUESTROOM_STATUS,
  GUESTROOM_STATUS_LABEL,
  RELATIONSHIP_GRAIN,
  guestroomStatusForUseCases,
  normalizeDemandUseCases,
} from "./shared-demand-model-v1.js";

export const LOCAL_MEETINGS_CATERING_RESEARCH_VERSION =
  "gdi-local-meetings-catering-research-v1.2";

/**
 * Professional planner specialties researched under LMC / PE paths.
 * Planners persist as Demand Generators — never as Private Event Venues
 * unless they also operate a verified venue.
 */
export const PLANNER_SPECIALTY = Object.freeze({
  CORPORATE_MEETING_EVENT: "corporate_meeting_event",
  ASSOCIATION_MANAGEMENT_MEETING: "association_management_meeting",
  WEDDING_MITZVAH_SOCIAL: "wedding_mitzvah_social",
  NONPROFIT_GALA_FUNDRAISING: "nonprofit_gala_fundraising",
  DESTINATION_MANAGEMENT_INCENTIVE: "destination_management_incentive",
});

export const PLANNER_SPECIALTY_LABEL = Object.freeze({
  [PLANNER_SPECIALTY.CORPORATE_MEETING_EVENT]:
    "Corporate meeting and event planners",
  [PLANNER_SPECIALTY.ASSOCIATION_MANAGEMENT_MEETING]:
    "Association management and meeting-planning firms",
  [PLANNER_SPECIALTY.WEDDING_MITZVAH_SOCIAL]:
    "Wedding, mitzvah, and social-celebration planners",
  [PLANNER_SPECIALTY.NONPROFIT_GALA_FUNDRAISING]:
    "Nonprofit gala and fundraising-event planners",
  [PLANNER_SPECIALTY.DESTINATION_MANAGEMENT_INCENTIVE]:
    "Destination management companies and incentive agencies",
});

export const PLANNER_RELATIONSHIP_STATUS = Object.freeze({
  EXISTING_RELATIONSHIP: "existing_relationship",
  PREVIOUSLY_PURSUED: "previously_pursued",
  RELATIONSHIP_NOT_YET_CONFIRMED: "relationship_not_yet_confirmed",
});

export const PLANNER_INFLUENCE_ROLE = Object.freeze({
  VENUE_SELECTION_INFLUENCE: "venue_selection_influence",
  LODGING_REFERRAL: "lodging_referral",
  EVENT_COORDINATION: "event_coordination",
  UNKNOWN: "unknown",
});

/**
 * Explicit LMC research subsegments (under LOCAL_MEETINGS_CATERING).
 * Outside-venue preferred lodging remains WEDDINGS_SOCIAL_PRIVATE_EVENTS / PE path.
 */
export const LMC_SUBSEGMENT = Object.freeze({
  CORPORATE_MEETINGS_TRAINING_BOARD:
    "corporate_meetings_training_board",
  MEETING_ROOM_RENTAL_RECURRING_PROFESSIONAL:
    "meeting_room_rental_recurring_professional",
  ASSOCIATION_LUNCHES_CHAPTER_AWARDS:
    "association_lunches_chapter_awards",
  NONPROFIT_FUNDRAISERS_GALAS_RECEPTIONS:
    "nonprofit_fundraisers_galas_receptions",
  SOCIAL_CELEBRATIONS_CATERING:
    "social_celebrations_catering",
  PROFESSIONAL_PLANNERS_INTERMEDIARIES:
    "professional_planners_intermediaries",
});

export const LMC_SUBSEGMENT_LABEL = Object.freeze({
  [LMC_SUBSEGMENT.CORPORATE_MEETINGS_TRAINING_BOARD]:
    "Corporate meetings, training, board meetings & workshops",
  [LMC_SUBSEGMENT.MEETING_ROOM_RENTAL_RECURRING_PROFESSIONAL]:
    "Meeting-room rental & recurring professional gatherings",
  [LMC_SUBSEGMENT.ASSOCIATION_LUNCHES_CHAPTER_AWARDS]:
    "Association lunches, chapter meetings & awards dinners",
  [LMC_SUBSEGMENT.NONPROFIT_FUNDRAISERS_GALAS_RECEPTIONS]:
    "Nonprofit fundraisers, galas & community receptions",
  [LMC_SUBSEGMENT.SOCIAL_CELEBRATIONS_CATERING]:
    "Social catering & celebrations (incl. mitzvah / wedding / milestone)",
  [LMC_SUBSEGMENT.PROFESSIONAL_PLANNERS_INTERMEDIARIES]:
    "Professional planners & intermediaries with recurring event business",
});

/** Constraint verification states — never invent yes/no from absence. */
export const LMC_CONSTRAINT_STATUS = Object.freeze({
  VERIFIED_SUPPORTED: "verified_supported",
  VERIFIED_NOT_SUPPORTED: "verified_not_supported",
  UNKNOWN_NEEDS_VERIFICATION: "unknown_needs_verification",
  NOT_APPLICABLE: "not_applicable",
});

export const LOCAL_CATERING_FAIL = Object.freeze({
  MISSING_BUYER: "MISSING_BUYER",
  MISSING_EVENT_ACTIVITY: "MISSING_EVENT_ACTIVITY",
  MISSING_LOCAL_CONNECTION: "MISSING_LOCAL_CONNECTION",
  MISSING_HOTEL_FIT: "MISSING_HOTEL_FIT",
  MISSING_CONTACT_ROUTE: "MISSING_CONTACT_ROUTE",
  MISSING_WHY_MATTERS: "MISSING_WHY_MATTERS",
  MISSING_NEXT_ACTION: "MISSING_NEXT_ACTION",
  DIRECTORY_ONLY: "DIRECTORY_ONLY",
  VENUE_AS_BUYER: "VENUE_AS_BUYER",
  HOTEL_PROFILE_INCOMPLETE: "HOTEL_PROFILE_INCOMPLETE",
  SUBSEGMENT_NOT_APPLICABLE: "SUBSEGMENT_NOT_APPLICABLE",
  PE_VENUE_REFERRAL_MISROUTE: "PE_VENUE_REFERRAL_MISROUTE",
  PRIVATE_FAMILY_OR_RELIGION_INFERENCE:
    "PRIVATE_FAMILY_OR_RELIGION_INFERENCE",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function lower(v) {
  return text(v).toLowerCase();
}

function hotelMeetingInventory(hotelConfig = {}) {
  const cp = hotelConfig.capabilityProfile || {};
  const rooms =
    Number(cp.meetingRoomCount ?? cp.meetingRoomsIndoor ?? 0) || 0;
  const sqft = Number(cp.totalMeetingSpaceSqFt || 0) || 0;
  const reception = Number(cp.largestReceptionCapacity || 0) || 0;
  const ballroom = Number(cp.largestBallroomSqFt || 0) || 0;
  const notes = text(cp.notes);
  return { cp, rooms, sqft, reception, ballroom, notes };
}

/**
 * Material hotel constraints for LMC prospects.
 * Never invent yes/no — absence → UNKNOWN_NEEDS_VERIFICATION.
 */
export function assessLocalMeetingsCateringConstraints(hotelConfig = {}) {
  const cp = hotelConfig.capabilityProfile || {};
  const policy = hotelConfig.eventPolicy || cp.eventPolicy || {};

  function resolveConstraint(raw) {
    if (raw === true || raw === "supported" || raw === "yes") {
      return LMC_CONSTRAINT_STATUS.VERIFIED_SUPPORTED;
    }
    if (raw === false || raw === "not_supported" || raw === "no") {
      return LMC_CONSTRAINT_STATUS.VERIFIED_NOT_SUPPORTED;
    }
    return LMC_CONSTRAINT_STATUS.UNKNOWN_NEEDS_VERIFICATION;
  }

  return {
    outsideCateringAllowed: resolveConstraint(
      cp.outsideCateringAllowed ?? policy.outsideCateringAllowed
    ),
    certifiedKosherCatering: resolveConstraint(
      cp.certifiedKosherCatering ??
        cp.kosherCatering ??
        policy.certifiedKosherCatering
    ),
    audiovisualSupport: resolveConstraint(
      cp.audiovisualSupport ?? cp.avCapabilities ?? policy.audiovisualSupport
    ),
    accessibilityVerified: resolveConstraint(
      cp.accessibilityVerified ?? policy.accessibilityVerified
    ),
    parkingAccess: resolveConstraint(
      cp.parkingAccess ??
        cp.onSiteParking ??
        policy.parkingAccess ??
        hotelConfig.access?.parking
    ),
    kosherInferencePolicy:
      "Do not infer certified kosher catering or religious-accommodation capability. Mark unknown until verified.",
    notesForVerification: [
      !cp.outsideCateringAllowed && !policy.outsideCateringAllowed
        ? "outside_catering_policy_unknown"
        : null,
      !cp.certifiedKosherCatering &&
      !cp.kosherCatering &&
      !policy.certifiedKosherCatering
        ? "certified_kosher_catering_unknown"
        : null,
      !cp.audiovisualSupport && !cp.avCapabilities
        ? "av_capabilities_unknown"
        : null,
    ].filter(Boolean),
  };
}

/**
 * Per-subsegment applicability from verified inventory + practical catchment.
 * Not every hotel needs every subsegment.
 */
export function assessLocalMeetingsCateringSubsegments(hotelConfig = {}) {
  const { rooms, sqft, reception, ballroom, notes } =
    hotelMeetingInventory(hotelConfig);
  const constraints = assessLocalMeetingsCateringConstraints(hotelConfig);
  const urban =
    /urban|downtown|central|metro|business.?district|bayside|capitol/i.test(
      [
        hotelConfig.capabilityProfile?.classification,
        hotelConfig.capabilityProfile?.serviceLevel,
        hotelConfig.commercialPriorities?.notes,
        notes,
        hotelConfig.city,
        hotelConfig.displayName,
      ]
        .filter(Boolean)
        .join(" ")
    ) || Boolean(hotelConfig.demandTerritory?.includes?.length);

  const hasSmallMeetings = rooms >= 3 || sqft >= 2000 || reception >= 40;
  const hasMidMeetings = rooms >= 8 || sqft >= 8000 || reception >= 150;
  const hasLargeFnB =
    reception >= 200 || ballroom >= 4000 || sqft >= 12000;
  const hasReceptionScale =
    reception >= 80 || ballroom >= 2500 || sqft >= 6000;

  const baseSnapshot = {
    meetingRoomCount: rooms || null,
    totalMeetingSpaceSqFt: sqft || null,
    largestReceptionCapacity: reception || null,
    largestBallroomSqFt: ballroom || null,
    outdoorEventSpace:
      hotelConfig.capabilityProfile?.outdoorEventSpace ?? null,
    serviceLevel: hotelConfig.capabilityProfile?.serviceLevel || null,
    classification: hotelConfig.capabilityProfile?.classification || null,
    city: hotelConfig.city || null,
    catchment: hotelConfig.demandTerritory?.label || null,
  };

  function row(applicable, rationale, extras = {}) {
    return { applicable: Boolean(applicable), rationale, ...extras };
  }

  const subsegments = {
    [LMC_SUBSEGMENT.CORPORATE_MEETINGS_TRAINING_BOARD]: row(
      hasSmallMeetings && (urban || hasMidMeetings),
      hasSmallMeetings && (urban || hasMidMeetings)
        ? "Meeting inventory and positioning support corporate meetings, training, board, and workshop use."
        : "Insufficient meeting inventory or business positioning for corporate/training priority.",
      { preferredUseCases: [DEMAND_USE_CASE.MEETINGS_AND_CATERING] }
    ),
    [LMC_SUBSEGMENT.MEETING_ROOM_RENTAL_RECURRING_PROFESSIONAL]: row(
      hasSmallMeetings,
      hasSmallMeetings
        ? "Published meeting rooms support rental and recurring professional gatherings."
        : "Limited meeting-room inventory for recurring professional rental.",
      { preferredUseCases: [DEMAND_USE_CASE.MEETINGS_AND_CATERING] }
    ),
    [LMC_SUBSEGMENT.ASSOCIATION_LUNCHES_CHAPTER_AWARDS]: row(
      hasReceptionScale || hasMidMeetings,
      hasReceptionScale || hasMidMeetings
        ? "Function space supports association lunches, chapter meetings, and awards dinners."
        : "Reception/function scale insufficient for association awards/lunch priority.",
      { preferredUseCases: [DEMAND_USE_CASE.MEETINGS_AND_CATERING] }
    ),
    [LMC_SUBSEGMENT.NONPROFIT_FUNDRAISERS_GALAS_RECEPTIONS]: row(
      hasLargeFnB || hasReceptionScale,
      hasLargeFnB || hasReceptionScale
        ? "Reception/ballroom inventory supports nonprofit fundraisers, galas, and community receptions."
        : "Function scale limited for gala/fundraiser priority.",
      { preferredUseCases: [DEMAND_USE_CASE.MEETINGS_AND_CATERING] }
    ),
    [LMC_SUBSEGMENT.SOCIAL_CELEBRATIONS_CATERING]: row(
      hasReceptionScale,
      hasReceptionScale
        ? "Function/F&B inventory can support social catering and celebrations when buyer need and hotel policies fit — kosher and outside-catering remain verification items, never assumed."
        : "Limited reception inventory for social celebration catering priority.",
      {
        preferredUseCases: [DEMAND_USE_CASE.MEETINGS_AND_CATERING],
        constraintWatch: [
          "certified_kosher_catering",
          "outside_catering",
          "event_policies",
        ],
        privacyPolicy:
          "Do not collect children's details, private family information, private guest lists, or infer an individual's religion.",
      }
    ),
    [LMC_SUBSEGMENT.PROFESSIONAL_PLANNERS_INTERMEDIARIES]: row(
      hasSmallMeetings || hasReceptionScale,
      hasSmallMeetings || hasReceptionScale
        ? "Hotel meetings/F&B product supports researching professional planners and intermediaries with recurring local event business (Demand Generator path — not Private Event Venues)."
        : "Insufficient meetings/F&B product for planner-intermediary research priority.",
      {
        preferredUseCases: [DEMAND_USE_CASE.MEETINGS_AND_CATERING],
        persistPath: "demand_generator_not_pe_venue",
      }
    ),
  };

  const applicableIds = Object.entries(subsegments)
    .filter(([, v]) => v.applicable)
    .map(([id]) => id);

  return {
    hotelMeetingSnapshot: baseSnapshot,
    constraints,
    subsegments,
    applicableSubsegmentIds: applicableIds,
    notEverySubsegmentRequired: true,
  };
}

/**
 * Applicability from actual hotel meeting/catering inventory — no invented facilities.
 * Includes per-subsegment applicability + constraint unknowns.
 */
export function assessLocalMeetingsCateringApplicability(hotelConfig = {}) {
  const { rooms, sqft, reception, notes } = hotelMeetingInventory(hotelConfig);
  const missingInventory = rooms < 1 && sqft < 1 && reception < 1;
  const sub = assessLocalMeetingsCateringSubsegments(hotelConfig);

  if (missingInventory) {
    return {
      applicable: false,
      needsProfileEnrichment: true,
      rationale:
        "Meeting/catering inventory not established in hotel profile — enrich function-space and catering fields before local meetings research.",
      constraints: sub.constraints,
      subsegments: sub.subsegments,
      applicableSubsegmentIds: [],
      hotelMeetingSnapshot: sub.hotelMeetingSnapshot,
    };
  }

  if (rooms < 3 && sqft < 3000 && reception < 50) {
    return {
      applicable: false,
      needsProfileEnrichment: false,
      rationale:
        "Limited published meeting inventory — local meetings & catering not prioritized for this product profile.",
      constraints: sub.constraints,
      subsegments: sub.subsegments,
      applicableSubsegmentIds: [],
      hotelMeetingSnapshot: sub.hotelMeetingSnapshot,
    };
  }

  const applicableIds = sub.applicableSubsegmentIds;
  const labelList = applicableIds
    .map((id) => LMC_SUBSEGMENT_LABEL[id] || id)
    .slice(0, 4)
    .join("; ");

  return {
    applicable: applicableIds.length > 0,
    needsProfileEnrichment:
      !notes || (sub.constraints.notesForVerification || []).length > 0,
    rationale: `Hotel publishes meeting inventory (${rooms || "n/a"} rooms, ${sqft || "n/a"} sq ft) supporting Local Meetings & Catering. Applicable subsegments: ${labelList || "none"}. Guestrooms not required for qualification. Preferred lodging with outside venues remains Private Events.`,
    hotelMeetingSnapshot: sub.hotelMeetingSnapshot,
    constraints: sub.constraints,
    subsegments: sub.subsegments,
    applicableSubsegmentIds: applicableIds,
  };
}

function isDirectoryOnly(candidate = {}) {
  const src = lower(
    [candidate.sourceUrl, ...(candidate.sourceUrls || []), candidate.sourceType].join(
      " "
    )
  );
  return /yellowpages|yelp\.com|chamber.?directory|member.?directory|crunchbase|linkedin\.com\/company\/?$/.test(
    src
  );
}

function isVenueAsBuyerMisuse(candidate = {}) {
  return (
    candidate.nearbyVenueAsBuyer === true ||
    candidate.treatVenueAsCateringBuyer === true ||
    (candidate.entityKind === "VENUE" &&
      candidate.relationshipGrain === RELATIONSHIP_GRAIN.VENUE_REFERRAL)
  );
}

function isPeVenueReferralMisroute(candidate = {}) {
  return (
    candidate.relationshipGrain === RELATIONSHIP_GRAIN.VENUE_REFERRAL ||
    candidate.persistAsPrivateEventVenue === true ||
    candidate.preferredLodgingPartnership === true
  );
}

function hasPrivateFamilyOrReligionInference(candidate = {}) {
  return (
    candidate.inferredReligion === true ||
    candidate.collectChildrenDetails === true ||
    candidate.privateGuestList === true ||
    Boolean(text(candidate.childGuestNames)) ||
    Boolean(text(candidate.inferredFaithTradition))
  );
}

function resolveLmcSubsegment(candidate = {}, app = {}) {
  const raw =
    text(candidate.lmcSubsegment) ||
    text(candidate.subsegment) ||
    text(candidate.researchSubsegment);
  if (raw && Object.values(LMC_SUBSEGMENT).includes(raw)) return raw;
  // Soft infer from evidence text only for routing — never invent capability.
  const blob = lower(
    [
      candidate.eventActivityEvidence,
      candidate.activityEvidence,
      candidate.whyThisMatters,
      candidate.organizationType,
      candidate.buyerRole,
    ]
      .filter(Boolean)
      .join(" ")
  );
  if (/planner|intermediary|event.?agency|catering.?referral/.test(blob)) {
    return LMC_SUBSEGMENT.PROFESSIONAL_PLANNERS_INTERMEDIARIES;
  }
  if (/fundrais|gala|nonprofit|community.?reception/.test(blob)) {
    return LMC_SUBSEGMENT.NONPROFIT_FUNDRAISERS_GALAS_RECEPTIONS;
  }
  if (/chapter|awards.?dinner|association.?lunch/.test(blob)) {
    return LMC_SUBSEGMENT.ASSOCIATION_LUNCHES_CHAPTER_AWARDS;
  }
  if (
    /mitzvah|quincea|wedding|anniversary|celebration|social.?cater/.test(blob)
  ) {
    return LMC_SUBSEGMENT.SOCIAL_CELEBRATIONS_CATERING;
  }
  if (/board.?meeting|training|workshop|corporate.?meeting/.test(blob)) {
    return LMC_SUBSEGMENT.CORPORATE_MEETINGS_TRAINING_BOARD;
  }
  if (/recurring|monthly.?meeting|room.?rental|professional.?gather/.test(blob)) {
    return LMC_SUBSEGMENT.MEETING_ROOM_RENTAL_RECURRING_PROFESSIONAL;
  }
  const fallback = (app.applicableSubsegmentIds || [])[0] || null;
  return fallback;
}

function resolveRelationshipGrain(candidate = {}) {
  const raw = text(candidate.relationshipGrain);
  if (raw && Object.values(RELATIONSHIP_GRAIN).includes(raw)) return raw;
  if (candidate.datedOccasion || candidate.eventDate || candidate.eventStartDate) {
    return RELATIONSHIP_GRAIN.DATED_OCCASION;
  }
  // Recurring programs + standing planner relationships use STANDING_ACCOUNT
  // (canonical taxonomy). Prefer VENUE_REFERRAL only on the Private Events path.
  return RELATIONSHIP_GRAIN.STANDING_ACCOUNT;
}

/**
 * Qualify local meetings/catering candidate. Guestrooms optional.
 */
export function qualifyLocalMeetingsCateringCandidate(
  candidate = {},
  hotelConfig = {}
) {
  const fail = [];
  const warnings = [];

  const app = assessLocalMeetingsCateringApplicability(hotelConfig);
  if (!app.applicable && app.needsProfileEnrichment) {
    fail.push(LOCAL_CATERING_FAIL.HOTEL_PROFILE_INCOMPLETE);
  }

  const lmcSubsegment = resolveLmcSubsegment(candidate, app);
  if (
    lmcSubsegment &&
    app.subsegments?.[lmcSubsegment] &&
    !app.subsegments[lmcSubsegment].applicable
  ) {
    fail.push(LOCAL_CATERING_FAIL.SUBSEGMENT_NOT_APPLICABLE);
  }

  if (isPeVenueReferralMisroute(candidate)) {
    fail.push(LOCAL_CATERING_FAIL.PE_VENUE_REFERRAL_MISROUTE);
    warnings.push(
      "preferred_lodging_outside_venue_belongs_on_private_events_path"
    );
  }

  if (hasPrivateFamilyOrReligionInference(candidate)) {
    fail.push(LOCAL_CATERING_FAIL.PRIVATE_FAMILY_OR_RELIGION_INFERENCE);
  }

  const buyer = text(
    candidate.organizationName || candidate.buyerName || candidate.company
  );
  if (!buyer) fail.push(LOCAL_CATERING_FAIL.MISSING_BUYER);

  const activity = text(
    candidate.eventActivityEvidence ||
      candidate.recurringMeetingEvidence ||
      candidate.activityEvidence ||
      candidate.datedOccasionNote
  );
  if (!activity) fail.push(LOCAL_CATERING_FAIL.MISSING_EVENT_ACTIVITY);

  const local = text(
    candidate.localConnection || candidate.localPresence || candidate.marketConnection
  );
  if (!local) fail.push(LOCAL_CATERING_FAIL.MISSING_LOCAL_CONNECTION);

  const fit = text(
    candidate.hotelFitRationale || candidate.fitExplanation || candidate.whyThisHotel
  );
  if (!fit) fail.push(LOCAL_CATERING_FAIL.MISSING_HOTEL_FIT);

  const contactEmail = text(candidate.contactEmail);
  const contactPhone = text(candidate.contactPhone);
  const contactUrl = text(candidate.contactUrl || candidate.organizationPath);
  const contactIsNamedPerson = Boolean(text(candidate.contactName));
  const contactIsOrgFallback =
    !contactIsNamedPerson && Boolean(contactEmail || contactPhone || contactUrl);
  if (!contactEmail && !contactPhone && !contactUrl) {
    fail.push(LOCAL_CATERING_FAIL.MISSING_CONTACT_ROUTE);
  } else if (
    contactUrl &&
    !contactEmail &&
    !contactPhone &&
    /\/$/.test(contactUrl) &&
    !/\/contact|\/events|\/about|\/staff/i.test(contactUrl)
  ) {
    warnings.push("contact_url_may_be_homepage_only");
  }
  if (contactIsOrgFallback) {
    warnings.push("organization_inbox_or_form_is_fallback_not_named_buyer");
  }
  if (
    candidate.treatPlannerAsConfirmedBuyer === true &&
    !candidate.buyerAuthorityEvidence
  ) {
    warnings.push("planner_not_confirmed_buyer_without_evidence");
  }

  const whyMatters = text(
    candidate.whyThisMatters || candidate.summaryWhyMatters || candidate.commercialThesis
  );
  if (!whyMatters) fail.push(LOCAL_CATERING_FAIL.MISSING_WHY_MATTERS);

  const nextAction = text(
    candidate.recommendedAction || candidate.recommendedNextStep
  );
  if (!nextAction) fail.push(LOCAL_CATERING_FAIL.MISSING_NEXT_ACTION);

  if (isDirectoryOnly(candidate)) fail.push(LOCAL_CATERING_FAIL.DIRECTORY_ONLY);
  if (isVenueAsBuyerMisuse(candidate)) fail.push(LOCAL_CATERING_FAIL.VENUE_AS_BUYER);

  if (
    contactUrl &&
    !contactEmail &&
    !contactPhone &&
    /^https?:\/\/[^/]+\/?$/i.test(contactUrl)
  ) {
    fail.push(LOCAL_CATERING_FAIL.MISSING_CONTACT_ROUTE);
  }

  const useCases = normalizeDemandUseCases(
    candidate.demandUseCases ||
      candidate.useCases || [DEMAND_USE_CASE.MEETINGS_AND_CATERING]
  );
  if (!useCases.length) useCases.push(DEMAND_USE_CASE.MEETINGS_AND_CATERING);

  // Combine related lunch + meeting space + rooms for same occasion — do not
  // invent separate cards. Caller may pass combined use cases.
  const guestroomStatus = guestroomStatusForUseCases(
    useCases,
    candidate.guestroomStatus != null
      ? candidate.guestroomStatus
      : GUESTROOM_STATUS.NOT_REQUIRED
  );

  const relationshipGrain = resolveRelationshipGrain({
    ...candidate,
    lmcSubsegment,
  });

  const constraintNotes = [];
  if (
    lmcSubsegment === LMC_SUBSEGMENT.SOCIAL_CELEBRATIONS_CATERING &&
    app.constraints?.certifiedKosherCatering ===
      LMC_CONSTRAINT_STATUS.UNKNOWN_NEEDS_VERIFICATION
  ) {
    constraintNotes.push(
      "certified_kosher_catering_unknown — do not assume hotel can provide kosher catering"
    );
  }
  for (const n of app.constraints?.notesForVerification || []) {
    if (!constraintNotes.includes(n)) constraintNotes.push(n);
  }

  return {
    ok: fail.length === 0,
    failReasons: fail,
    warnings,
    buyer,
    activity,
    localConnection: local,
    hotelFit: fit,
    whyMatters,
    recommendedAction: nextAction,
    demandUseCases: useCases,
    guestroomStatus,
    guestroomStatusLabel: GUESTROOM_STATUS_LABEL[guestroomStatus],
    lmcSubsegment,
    lmcSubsegmentLabel: LMC_SUBSEGMENT_LABEL[lmcSubsegment] || lmcSubsegment,
    relationshipGrain,
    hotelMeetingSnapshot: app.hotelMeetingSnapshot || null,
    hotelConstraints: app.constraints || null,
    constraintNotes,
    crossReferencePrivateEvents:
      text(candidate.relatedPrivateEventVenueId) ||
      text(candidate.peCrossReferenceId) ||
      null,
    contactEmail: contactEmail || null,
    contactPhone: contactPhone || null,
    contactUrl: contactUrl || null,
    contactName: text(candidate.contactName) || null,
    contactRole: text(candidate.contactRole) || "events / meetings organizer",
    contactIsNamedPerson,
    contactIsOrgFallback,
  };
}

export function computeLocalMeetingsCateringOpportunityId(
  hotelId,
  organizationName,
  domain,
  {
    relationshipGrain = null,
    occasionKey = null,
    eventDate = null,
  } = {}
) {
  const d = text(domain) || "";
  const n = lower(organizationName).replace(/[^a-z0-9]+/g, " ").trim();
  // Standing accounts: stable hotel|org identity (combine related use cases).
  // Dated occasions: append occasion key so distinct events stay distinct.
  const occasion =
    relationshipGrain === RELATIONSHIP_GRAIN.DATED_OCCASION
      ? text(occasionKey) || text(eventDate) || ""
      : "";
  const seed = occasion
    ? `${text(hotelId)}|lmc|${d || n}|${occasion}`
    : `${text(hotelId)}|lmc|${d || n}`;
  return `gdi_lmc_${createHash("sha256").update(seed).digest("hex").slice(0, 16)}`;
}

export function buildLocalMeetingsCateringOpportunityCard(
  candidate = {},
  opts = {}
) {
  const hotelConfig = opts.hotelConfig || {};
  const hotelId =
    text(opts.hotelId) ||
    text(hotelConfig.hotelId) ||
    text(candidate.hotelId);
  const q = qualifyLocalMeetingsCateringCandidate(candidate, hotelConfig);
  if (!q.ok) {
    return { ok: false, qualification: q, opportunity: null };
  }

  const domain =
    text(candidate.officialDomain) ||
    domainFromUrl(candidate.website || candidate.sourceUrl || q.contactUrl);
  const opportunityId =
    text(candidate.existingOpportunityId) ||
    computeLocalMeetingsCateringOpportunityId(hotelId, q.buyer, domain, {
      relationshipGrain: q.relationshipGrain,
      occasionKey: candidate.occasionKey,
      eventDate: candidate.eventStartDate || candidate.eventDate,
    });

  const publish = candidate.publish !== false;
  const nowIso = new Date().toISOString();
  const sources = []
    .concat(candidate.sources || [])
    .concat(
      (candidate.sourceUrls || []).map((url) => ({
        url,
        type: "event_activity",
        title: "Event activity evidence",
      }))
    );
  if (candidate.sourceUrl && !sources.some((s) => s.url === candidate.sourceUrl)) {
    sources.push({
      url: candidate.sourceUrl,
      type: "event_activity",
      title: "Event activity evidence",
    });
  }

  const dgId = computeDemandGeneratorId({
    organizationName: q.buyer,
    officialDomain: domain,
    website: candidate.website || q.contactUrl,
  });
  const fitId = computeHotelGeneratorFitId({
    hotelId,
    demandGeneratorId: dgId,
  });

  const functionType =
    text(candidate.functionType || candidate.eventType) ||
    q.lmcSubsegmentLabel ||
    "Local meeting / banquet / social function";
  const attendance = text(candidate.attendanceNote || candidate.estimatedAttendance);
  const isDated = q.relationshipGrain === RELATIONSHIP_GRAIN.DATED_OCCASION;
  const recurrence =
    text(candidate.recurrenceNote) ||
    (isDated
      ? "Dated occasion — do not invent recurrence"
      : "Standing local meetings / catering relationship");
  const subLabel = q.lmcSubsegmentLabel || "Local Meetings & Catering";

  return {
    ok: true,
    qualification: q,
    opportunity: {
      id: opportunityId,
      opportunityId,
      hotelId,
      organizationName: q.buyer,
      company: q.buyer,
      title: `${q.buyer} — ${subLabel}`,
      opportunityName: `${q.buyer} — ${subLabel}`,
      opportunityType: OPPORTUNITY_TYPE.CORPORATE_ACCOUNT_RELATIONSHIP,
      opportunityTypeLabel: "Corporate Account Relationship",
      detailLayout: "CORPORATE_ACCOUNT_RELATIONSHIP",
      demandFamily: DEMAND_FAMILY.GROUP_MEETINGS,
      demandSignalType: DEMAND_SIGNAL_TYPE.CORPORATE_MEETING,
      segment: "Local Meetings & Catering",
      researchSegment: "local_meetings_catering",
      lmcSubsegment: q.lmcSubsegment,
      lmcSubsegmentLabel: q.lmcSubsegmentLabel,
      relationshipGrain: q.relationshipGrain,
      demandUseCases: q.demandUseCases,
      standingOpportunity: !isDated,
      cateringOnly: q.guestroomStatus === GUESTROOM_STATUS.NOT_REQUIRED,
      guestroomStatus: q.guestroomStatus,
      guestroomStatusLabel: q.guestroomStatusLabel,
      roomDemandStatus:
        q.guestroomStatus === GUESTROOM_STATUS.NOT_REQUIRED
          ? "NOT_REQUIRED"
          : "UNKNOWN",
      /** LMC assessment profile: guestroom dimensions N/A when rooms not required. */
      hotelFitAssessmentProfile: "local_meetings_catering",
      guestroomDimensionsMode:
        q.guestroomStatus === GUESTROOM_STATUS.NOT_REQUIRED
          ? "not_applicable"
          : "score_when_evidenced",
      eventStartDate: candidate.eventStartDate || candidate.eventDate || null,
      eventEndDate: candidate.eventEndDate || null,
      eventDateDisplay:
        candidate.eventDateDisplay ||
        (isDated
          ? text(candidate.eventStartDate || candidate.eventDate) ||
            "Dated occasion (date not fully established)"
          : "Standing local meetings / catering relationship"),
      timing: recurrence,
      functionType,
      attendanceNote: attendance || null,
      spaceNeedsNote: text(candidate.spaceNeedsNote) || null,
      meetingCateringNeeds: text(candidate.meetingCateringNeeds) || q.activity,
      localPresence: q.localConnection,
      localConnection: q.localConnection,
      eventActivityEvidence: q.activity,
      proposedHotelUse: q.demandUseCases
        .map((u) => u.replace(/_/g, " ").toLowerCase())
        .join("; "),
      whyThisHotel: q.hotelFit,
      hotelOpportunityThesis: q.hotelFit,
      fitExplanation: q.hotelFit,
      summaryWhyHotel: q.hotelFit,
      summaryWhat: `${q.buyer}: ${q.activity}`,
      summaryWhyMatters: q.whyMatters,
      whyNow:
        text(candidate.whyNow) ||
        (isDated
          ? "Dated local meeting/catering occasion — confirm sourcing status before inventing urgency."
          : "Standing local meeting/catering demand — no invented urgency; introduce hotel meeting product and ask how venues are currently selected."),
      whyEstablishContactNow:
        text(candidate.whyNow) ||
        "Local organizer with evidenced meeting/banquet activity in catchment.",
      constraints: [
        "Do not invent attendance, budgets, or room nights.",
        q.guestroomStatus === GUESTROOM_STATUS.NOT_REQUIRED
          ? "Guestrooms are not required for this opportunity."
          : "Guestroom demand is not established unless separately evidenced.",
        ...q.constraintNotes,
        ...(candidate.constraints || []),
      ],
      hotelConstraints: q.hotelConstraints,
      uncertainties: candidate.uncertainties || [
        "Open sourcing / exclusive venue relationships not fully known from public pages.",
        ...(q.constraintNotes || []),
      ],
      establishedFacts: [
        q.activity,
        q.localConnection,
        `Subsegment: ${q.lmcSubsegmentLabel || q.lmcSubsegment}`,
        `Contact route: ${
          q.contactIsNamedPerson
            ? `${q.contactName} (${q.contactRole})`
            : "organization fallback"
        }: ${q.contactEmail || q.contactPhone || q.contactUrl}`,
      ],
      inferredHotelDemand: [
        "Hypothesis: hotel meeting rooms and catering can serve this organizer's local functions when capacity and positioning fit.",
      ],
      contactName: q.contactName,
      contactEmail: q.contactEmail,
      contactPhone: q.contactPhone,
      contactUrl: q.contactUrl,
      contactRole: q.contactRole,
      contactResponsibility: q.contactRole,
      contactIsNamedPerson: q.contactIsNamedPerson,
      contactIsOrgFallback: q.contactIsOrgFallback,
      commercialContactPath: q.contactIsNamedPerson
        ? "NAMED_DIRECT"
        : q.contactEmail || q.contactPhone
          ? "ORGANIZATION_PATH"
          : "ORGANIZATION_PATH",
      recommendedAction: q.recommendedAction,
      recommendedNextStep: q.recommendedAction,
      priority: candidate.priority || "MEDIUM_PRIORITY",
      venueSourcingStatus: "OPEN_UNRESOLVED",
      doNotEstimateAnnualRevenue: true,
      customerFacingState: publish ? "ACTIONABLE_NOW" : "INTERNAL_ONLY",
      customerVisible: publish,
      actionable: publish,
      prospectPublicationBasis: publish ? "REVIEWED_PACKAGE_V1" : null,
      prospectHotelFacing: publish,
      prospectReviewSection: "EXPLORATORY",
      prospectCommercialLabel: q.lmcSubsegmentLabel || "Local meetings & catering",
      /** Planners/buyers → Demand Generator path; never PE venue table. */
      persistPath:
        q.lmcSubsegment === LMC_SUBSEGMENT.PROFESSIONAL_PLANNERS_INTERMEDIARIES
          ? "demand_generator_not_pe_venue"
          : "demand_generator",
      relatedPrivateEventVenueId: q.crossReferencePrivateEvents,
      peCrossReferenceOnly: Boolean(q.crossReferencePrivateEvents),
      dgGeneratorId: dgId,
      hotelGeneratorFitId: fitId,
      schemaVersion: LOCAL_MEETINGS_CATERING_RESEARCH_VERSION,
      researchVersion:
        candidate.researchVersion || LOCAL_MEETINGS_CATERING_RESEARCH_VERSION,
      evidenceFreshnessAt: text(candidate.evidenceDate) || nowIso.slice(0, 10),
      lastVerifiedAt: nowIso,
      firstSeenAt: candidate.firstSeenAt || nowIso,
      updatedAt: nowIso,
      sources,
      officialSource: sources[0]?.url || null,
      hotelMeetingSnapshot: q.hotelMeetingSnapshot,
    },
  };
}

export function buildLocalMeetingsCateringDemandGeneratorPackage(
  candidate = {},
  opts = {}
) {
  const card = buildLocalMeetingsCateringOpportunityCard(candidate, opts);
  if (!card.ok) return card;
  const opp = card.opportunity;
  const domain =
    text(candidate.officialDomain) ||
    domainFromUrl(candidate.website || candidate.sourceUrl);

  const orgType =
    candidate.organizationType ||
    (/assoc|council|chamber|chapter|boma|asid|acec|mba/i.test(opp.organizationName)
      ? ORGANIZATION_TYPE.PROFESSIONAL_BODY
      : /nonprofit|foundation|partnership/i.test(opp.organizationName)
        ? ORGANIZATION_TYPE.NONPROFIT
        : ORGANIZATION_TYPE.OTHER);

  const generator = buildDemandGeneratorEntity({
    organizationName: opp.organizationName,
    officialDomain: domain,
    website: candidate.website || opp.contactUrl,
    organizationType: orgType,
    generatorStatus: GENERATOR_STATUS.VERIFIED,
    notes: `Local meetings & catering account. ${opp.eventActivityEvidence}`,
  });

  const program = buildProgramEntity({
    demandGeneratorId: generator.demandGeneratorId,
    programName: text(candidate.programName) || "Local meetings / banquet series",
    programType: PROGRAM_TYPE.REGIONAL_MEETING,
    recurrenceStatus: RECURRENCE_STATUS.POSSIBLE_RECURRING,
    notes: "Local F&B / meeting demand — guestrooms not required unless evidenced.",
  });

  const fit = buildHotelDemandGeneratorFit({
    hotelId: opp.hotelId,
    demandGeneratorId: generator.demandGeneratorId,
    marketRelevance: candidate.marketRelevance || "MEDIUM",
    productFit: candidate.productFit || "MEDIUM",
    generatorRelevance: "MEDIUM",
    likelyTravelCreation: "LOW",
    programObservability: "MEDIUM",
    buyerAccessibility: opp.contactEmail || opp.contactPhone ? "MEDIUM" : "LOW",
    evidenceQuality: "MEDIUM",
    fitRationale: opp.whyThisHotel,
  });

  return {
    ok: true,
    opportunity: opp,
    demandGenerator: generator,
    program,
    hotelGeneratorFit: fit,
    programId: program.programId || computeProgramId(program),
    dedupeKey: `${opp.hotelId}|${lower(opp.organizationName)}`,
  };
}

export function isLocalMeetingsCateringOpportunity(opp = {}) {
  if (opp.researchSegment === "local_meetings_catering") return true;
  if (opp.cateringOnly === true) return true;
  const uc = normalizeDemandUseCases(opp.demandUseCases || opp.useCases || []);
  if (
    uc.includes(DEMAND_USE_CASE.MEETINGS_AND_CATERING) &&
    opp.guestroomStatus === GUESTROOM_STATUS.NOT_REQUIRED
  ) {
    return true;
  }
  const blob = lower(
    [opp.segment, opp.title, opp.prospectCommercialLabel, opp.summaryWhat].join(" ")
  );
  return /local meetings.?catering|catering.?only|meetings & catering/.test(blob);
}

/**
 * Subsegment-aware research queries. Only emit queries for applicable subsegments.
 * Directory listings alone are discovery leads, not qualification.
 */
export function buildLocalMeetingsCateringResearchQueries(
  hotelConfig = {},
  { subsegmentIds = null } = {}
) {
  const city = text(hotelConfig.city) || "market";
  const hotel = text(hotelConfig.displayName) || "hotel";
  const app = assessLocalMeetingsCateringApplicability(hotelConfig);
  const wanted = new Set(
    subsegmentIds || app.applicableSubsegmentIds || Object.values(LMC_SUBSEGMENT)
  );

  const queries = [];

  if (wanted.has(LMC_SUBSEGMENT.CORPORATE_MEETINGS_TRAINING_BOARD)) {
    queries.push({
      subsegment: LMC_SUBSEGMENT.CORPORATE_MEETINGS_TRAINING_BOARD,
      intent: "corporate_training_board_workshops",
      query: `"${city}" (corporate training OR "board meeting" OR workshop OR "offsite meeting") (hotel OR "meeting room" OR banquet) -crunchbase -yelp`,
    });
  }
  if (wanted.has(LMC_SUBSEGMENT.MEETING_ROOM_RENTAL_RECURRING_PROFESSIONAL)) {
    queries.push({
      subsegment: LMC_SUBSEGMENT.MEETING_ROOM_RENTAL_RECURRING_PROFESSIONAL,
      intent: "recurring_professional_room_rental",
      query: `"${city}" ("monthly meeting" OR "continuing education" OR "professional development") ("meeting space" OR "conference room" OR hotel) contact -directory`,
    });
  }
  if (wanted.has(LMC_SUBSEGMENT.ASSOCIATION_LUNCHES_CHAPTER_AWARDS)) {
    queries.push({
      subsegment: LMC_SUBSEGMENT.ASSOCIATION_LUNCHES_CHAPTER_AWARDS,
      intent: "chapter_awards_dinners",
      query: `"${city}" (awards gala OR holiday luncheon OR chapter dinner OR "engineering excellence") (hotel OR banquet) -crunchbase`,
    });
  }
  if (wanted.has(LMC_SUBSEGMENT.NONPROFIT_FUNDRAISERS_GALAS_RECEPTIONS)) {
    queries.push({
      subsegment: LMC_SUBSEGMENT.NONPROFIT_FUNDRAISERS_GALAS_RECEPTIONS,
      intent: "nonprofit_fundraisers",
      query: `"${city}" (nonprofit OR foundation) (gala OR fundraiser OR awards) (downtown OR banquet) contact -yelp`,
    });
  }
  if (wanted.has(LMC_SUBSEGMENT.SOCIAL_CELEBRATIONS_CATERING)) {
    queries.push({
      subsegment: LMC_SUBSEGMENT.SOCIAL_CELEBRATIONS_CATERING,
      intent: "social_celebrations_planners",
      query: `"${city}" ("event planner" OR "social events" OR "private dining" OR mitzvah OR quinceañera OR anniversary) (hotel OR catering OR banquet) -guestlist -children`,
      notes: [
        "Do not collect children's details, private family info, or infer religion.",
        "Do not assume certified kosher catering — mark hotel constraint unknown until verified.",
        "A directory listing is a discovery lead, not qualification.",
      ],
    });
  }
  if (wanted.has(LMC_SUBSEGMENT.PROFESSIONAL_PLANNERS_INTERMEDIARIES)) {
    queries.push({
      subsegment: LMC_SUBSEGMENT.PROFESSIONAL_PLANNERS_INTERMEDIARIES,
      intent: "professional_planners_intermediaries",
      query: `"${city}" ("meeting planner" OR "event planner" OR "destination management" OR "corporate events") (hotel OR catering) (portfolio OR clients) -yelp`,
      notes: [
        "Persist planners as Demand Generators — not Private Event Venues.",
        "Planner ≠ confirmed buyer without authority evidence.",
      ],
    });
  }

  queries.push({
    intent: "exclude_venue_as_buyer",
    note: `Do not treat a nearby venue as a catering buyer for ${hotel}. Preferred lodging with outside venues stays on the Private Events path.`,
  });
  queries.push({
    intent: "constraint_verification",
    notes: app.constraints?.notesForVerification || [],
    kosherPolicy: app.constraints?.kosherInferencePolicy || null,
  });

  return queries;
}

/**
 * Coverage snapshot for briefs / scorecards (hotel × LMC subsegments).
 */
export function buildLocalMeetingsCateringCoverageSnapshot(
  hotelConfig = {},
  outcomesBySubsegment = {}
) {
  const app = assessLocalMeetingsCateringApplicability(hotelConfig);
  const rows = Object.values(LMC_SUBSEGMENT).map((id) => {
    const sub = app.subsegments?.[id] || { applicable: false };
    const out = outcomesBySubsegment[id] || {};
    return {
      subsegmentId: id,
      label: LMC_SUBSEGMENT_LABEL[id],
      applicable: Boolean(sub.applicable),
      applicabilityRationale: sub.rationale || null,
      researched: Boolean(out.researched),
      depth: out.depth || "not_started",
      accountsAssessed: out.accountsAssessed ?? 0,
      qualified: out.qualified ?? 0,
      published: out.published ?? 0,
      held: out.held ?? 0,
      datedOccasions: out.datedOccasions ?? 0,
      standingRelationships: out.standingRelationships ?? 0,
      namedContacts: out.namedContacts ?? 0,
      fallbackRoutes: out.fallbackRoutes ?? 0,
      remainingGaps: out.remainingGaps || [],
    };
  });

  return {
    version: LOCAL_MEETINGS_CATERING_RESEARCH_VERSION,
    applicable: app.applicable,
    needsProfileEnrichment: app.needsProfileEnrichment,
    rationale: app.rationale,
    hotelMeetingSnapshot: app.hotelMeetingSnapshot,
    constraints: app.constraints,
    applicableSubsegmentIds: app.applicableSubsegmentIds,
    pePathNote:
      "Outside-venue preferred lodging partnerships remain on Private Events — cross-reference only.",
    subsegments: rows,
    totals: {
      applicableSubsegments: rows.filter((r) => r.applicable).length,
      researched: rows.filter((r) => r.researched).length,
      accountsAssessed: rows.reduce((s, r) => s + (r.accountsAssessed || 0), 0),
      qualified: rows.reduce((s, r) => s + (r.qualified || 0), 0),
      published: rows.reduce((s, r) => s + (r.published || 0), 0),
      held: rows.reduce((s, r) => s + (r.held || 0), 0),
    },
  };
}

/**
 * Resolve hotel relationship label for a planner from documented evidence only.
 * "New to GDI" ≠ "new to the hotel."
 */
export function resolvePlannerRelationshipStatus(candidate = {}, hotelContext = {}) {
  if (
    candidate.existingHotelRelationship === true ||
    candidate.relationshipStatus ===
      PLANNER_RELATIONSHIP_STATUS.EXISTING_RELATIONSHIP ||
    hotelContext.knownPlannerIds?.includes?.(candidate.plannerId) ||
    hotelContext.crmKnown === true
  ) {
    return PLANNER_RELATIONSHIP_STATUS.EXISTING_RELATIONSHIP;
  }
  if (
    candidate.previouslyPursued === true ||
    candidate.relationshipStatus ===
      PLANNER_RELATIONSHIP_STATUS.PREVIOUSLY_PURSUED ||
    hotelContext.previouslyPursued === true
  ) {
    return PLANNER_RELATIONSHIP_STATUS.PREVIOUSLY_PURSUED;
  }
  return PLANNER_RELATIONSHIP_STATUS.RELATIONSHIP_NOT_YET_CONFIRMED;
}

/**
 * Qualify a professional planner for LMC standing relationship (not PE venue).
 * Directory / “top planner” articles alone fail.
 */
export function qualifyProfessionalPlannerCandidate(
  candidate = {},
  hotelConfig = {}
) {
  const base = qualifyLocalMeetingsCateringCandidate(
    {
      ...candidate,
      lmcSubsegment:
        candidate.lmcSubsegment ||
        LMC_SUBSEGMENT.PROFESSIONAL_PLANNERS_INTERMEDIARIES,
      demandUseCases:
        candidate.demandUseCases || [DEMAND_USE_CASE.MEETINGS_AND_CATERING],
      guestroomStatus:
        candidate.guestroomStatus != null
          ? candidate.guestroomStatus
          : GUESTROOM_STATUS.NOT_REQUIRED,
    },
    hotelConfig
  );

  const fail = [...(base.failReasons || [])];
  const warnings = [...(base.warnings || [])];

  if (candidate.directoryRankingOnly === true || candidate.topPlannerArticleOnly === true) {
    fail.push(LOCAL_CATERING_FAIL.DIRECTORY_ONLY);
  }
  if (candidate.persistAsPrivateEventVenue === true && !candidate.operatesVerifiedVenue) {
    fail.push(LOCAL_CATERING_FAIL.PE_VENUE_REFERRAL_MISROUTE);
  }

  const specialties = []
    .concat(candidate.plannerSpecialties || [])
    .concat(candidate.specialties || [])
    .filter((s) => Object.values(PLANNER_SPECIALTY).includes(s));
  if (!specialties.length) {
    warnings.push("planner_specialty_unspecified");
  }

  const localMarketEvidence = text(
    candidate.localMarketEvidence || candidate.localConnection || base.localConnection
  );
  const activityEvidence = text(
    candidate.supportedEventActivity || candidate.eventActivityEvidence || base.activity
  );
  const influenceRoles = []
    .concat(candidate.influenceRoles || [])
    .filter((r) => Object.values(PLANNER_INFLUENCE_ROLE).includes(r));
  if (!influenceRoles.length) {
    warnings.push("influence_role_unknown_do_not_assume_purchasing_control");
  }

  const relationshipStatus = resolvePlannerRelationshipStatus(candidate, {
    knownPlannerIds: hotelConfig.knownPlannerIds,
    crmKnown: candidate.crmKnown,
    previouslyPursued: candidate.previouslyPursued,
  });

  return {
    ...base,
    ok: fail.length === 0 && base.ok,
    failReasons: fail,
    warnings,
    plannerSpecialties: specialties,
    localMarketEvidence,
    activityEvidence,
    influenceRoles: influenceRoles.length
      ? influenceRoles
      : [PLANNER_INFLUENCE_ROLE.UNKNOWN],
    relationshipStatus,
    relationshipStatusLabel: relationshipStatus.replace(/_/g, " "),
    persistPath: "demand_generator_not_pe_venue",
    dedupeKeyHint: `planner|${lower(base.buyer)}`,
  };
}

/**
 * Build standing planner relationship opportunity via LMC card path.
 * One planner → one standing card (not one per specialty).
 */
export function buildProfessionalPlannerOpportunityCard(
  candidate = {},
  opts = {}
) {
  const hotelConfig = opts.hotelConfig || {};
  const q = qualifyProfessionalPlannerCandidate(candidate, hotelConfig);
  if (!q.ok) {
    return { ok: false, qualification: q, opportunity: null };
  }

  const specialtiesLabel = (q.plannerSpecialties || [])
    .map((s) => PLANNER_SPECIALTY_LABEL[s] || s)
    .join("; ");

  const card = buildLocalMeetingsCateringOpportunityCard(
    {
      ...candidate,
      organizationName: q.buyer,
      lmcSubsegment: LMC_SUBSEGMENT.PROFESSIONAL_PLANNERS_INTERMEDIARIES,
      relationshipGrain: RELATIONSHIP_GRAIN.STANDING_ACCOUNT,
      guestroomStatus: q.guestroomStatus,
      demandUseCases: q.demandUseCases,
      eventActivityEvidence: q.activityEvidence,
      localConnection: q.localMarketEvidence,
      hotelFitRationale: q.hotelFit,
      whyThisMatters:
        q.whyMatters ||
        `Professional planner with evidenced local activity${specialtiesLabel ? ` (${specialtiesLabel})` : ""}. Relationship status: ${q.relationshipStatusLabel}.`,
      recommendedAction:
        q.recommendedAction ||
        "Ask hotel sales whether they know or already work with this planner; if not, introduce meeting/catering product and ask how they currently select hotels.",
      functionType: specialtiesLabel || "Professional planner relationship",
      recurrenceNote: "Standing planner relationship — no invented forthcoming event",
      constraints: [
        ...(candidate.constraints || []),
        "Do not invent a forthcoming event or confirmed buying requirement.",
        "Planner ≠ confirmed buyer without authority evidence.",
        `Hotel relationship: ${q.relationshipStatus}`,
      ],
      publish: candidate.publish !== false,
    },
    opts
  );

  if (!card.ok) return card;

  card.opportunity.plannerSpecialties = q.plannerSpecialties;
  card.opportunity.plannerInfluenceRoles = q.influenceRoles;
  card.opportunity.plannerRelationshipStatus = q.relationshipStatus;
  card.opportunity.persistPath = "demand_generator_not_pe_venue";
  card.opportunity.prospectCommercialLabel =
    "Professional planner — Local Meetings & Catering";
  card.qualification = q;
  return card;
}

export function buildProfessionalPlannerResearchQueries(hotelConfig = {}) {
  const city = text(hotelConfig.city) || "market";
  return [
    {
      specialty: PLANNER_SPECIALTY.CORPORATE_MEETING_EVENT,
      query: `"${city}" ("meeting planner" OR "corporate event planner" OR "conference planner") (hotel OR venue) (portfolio OR clients) -yelp -directory`,
    },
    {
      specialty: PLANNER_SPECIALTY.ASSOCIATION_MANAGEMENT_MEETING,
      query: `"${city}" ("association management" OR AMC OR "meeting planning firm") (chapter OR conference) hotel -yelp`,
    },
    {
      specialty: PLANNER_SPECIALTY.WEDDING_MITZVAH_SOCIAL,
      query: `"${city}" ("wedding planner" OR "social events" OR mitzvah planner) (hotel OR banquet) -guestlist -children -yelp`,
      notes: [
        "Do not collect children's details or infer religion.",
        "Do not assume kosher catering capability.",
      ],
    },
    {
      specialty: PLANNER_SPECIALTY.NONPROFIT_GALA_FUNDRAISING,
      query: `"${city}" ("gala planner" OR "fundraising event" OR "nonprofit events") (planner OR agency) hotel`,
    },
    {
      specialty: PLANNER_SPECIALTY.DESTINATION_MANAGEMENT_INCENTIVE,
      query: `"${city}" ("destination management" OR DMC OR "incentive travel") (agency OR planner) hotel lodging`,
    },
    {
      intent: "qualification_gate",
      note: "Directory rankings and top-planner articles are discovery leads only. Require local-market work, event-type evidence, hotel-fit, and usable professional contact. Deduplicate across LMC/PE — one planner one standing card.",
    },
  ];
}
