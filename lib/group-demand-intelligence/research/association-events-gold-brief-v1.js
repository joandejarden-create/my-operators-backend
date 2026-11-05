/**
 * Association / Event gold-standard customer brief V1.
 * Builds Director-of-Sales drawer fields without inventing RFPs or room blocks.
 */

import { looksLikePersonName } from "../secondary-named-contact-v1.js";
import { hotelAssociationEventsFitProfile } from "./association-events-discovery-strategies-v1.js";
import { evaluateAssociationEventsPublicationQualification } from "./association-events-qualification-v1.js";
import {
  isAssociationEventsValidPersonName,
  scrubAssociationEventsPhone,
  assessAssociationEventsBuyerCandidate,
} from "./association-events-buyer-integrity-v1.js";
import { dedupeEvidenceByCanonicalUrl } from "./source-evidence-dedupe-v1.js";
import {
  EVENT_CHRONOLOGY_STATUS,
} from "./association-events-chronology-v1.js";
import {
  classifyContactEligibility,
  buildContactEligibilityBriefFields,
} from "../contact-eligibility-standard-v1.js";
import {
  assessBuyerOrgAndAccessibility,
} from "./commercial-accessibility-and-org-route-v1.js";

export const ASSOCIATION_EVENTS_GOLD_BRIEF_V1 =
  "gdi-association-events-gold-brief-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function scrub(s) {
  return text(s)
    .replace(/https?:\/\/\S+/gi, "")
    .replace(/Requires qualification[^.]{0,120}/gi, "")
    .replace(/\s+/g, " ")
    .trim();
}

function hotelLabel(hotel = {}, opp = {}) {
  return (
    text(hotel.displayName || hotel.name) ||
    text(opp.hotelDisplayName || opp.hotelName) ||
    "this hotel"
  );
}

function primaryBuyer(opp = {}) {
  const org = text(opp.organizationName || opp.organizer || opp.eventOwner || "");
  const eventBlob = `${opp.eventName || ""} ${opp.title || ""} ${org}`;
  if (isAssociationEventsValidPersonName(opp.contactName)) {
    const assessed = assessAssociationEventsBuyerCandidate(
      { name: opp.contactName, title: opp.contactRole, organization: opp.contactOrganization },
      { eventBlob, organizerName: org },
    );
    if (assessed.ok) {
      return {
        name: text(opp.contactName),
        title: text(opp.contactRole) || null,
        email: opp.contactEmail || opp.functionalContactEmail || null,
        phone: scrubAssociationEventsPhone(opp.contactPhone || opp.primaryContactPhone) || null,
        url: opp.contactUrl || null,
      };
    }
  }
  const s = (opp.namedStakeholders || []).find((x) => {
    if (!isAssociationEventsValidPersonName(x?.name)) return false;
    return assessAssociationEventsBuyerCandidate(x, { eventBlob, organizerName: org }).ok;
  });
  if (s) {
    return {
      name: text(s.name),
      title: text(s.title || s.role) || null,
      email: s.email || opp.contactEmail || null,
      phone: scrubAssociationEventsPhone(s.phone || opp.contactPhone) || null,
      url: s.url || opp.contactUrl || null,
    };
  }
  return null;
}

function secondaryBuyer(opp = {}, primaryName = "") {
  const candidates = [
    ...(opp.secondaryContacts || []),
    ...(opp.namedStakeholders || []),
  ];
  for (const s of candidates) {
    if (!looksLikePersonName(s?.name)) continue;
    if (text(s.name).toLowerCase() === text(primaryName).toLowerCase()) continue;
    return {
      name: text(s.name),
      title: text(s.title || s.role) || null,
      email: s.email || null,
      phone: s.phone || null,
      url: s.url || null,
    };
  }
  return null;
}

/**
 * Apply event gold-standard presentation fields onto an opportunity.
 */
export function applyAssociationEventsGoldBrief(opportunity = {}, { hotel = {} } = {}) {
  const hotelName = hotelLabel(hotel, opportunity);
  const fit = hotelAssociationEventsFitProfile({ ...hotel, hotelId: opportunity.hotelId || hotel.hotelId });
  const qual = evaluateAssociationEventsPublicationQualification(opportunity, hotel);
  const eventName =
    text(opportunity.eventName) ||
    text(opportunity.title) ||
    text(opportunity.whatIsHappening)?.slice(0, 120) ||
    "Association / conference opportunity";
  const org =
    text(opportunity.organizationName || opportunity.organizer || opportunity.eventOwner) ||
    "the organizing association";
  const primary = primaryBuyer(opportunity);
  const secondary = secondaryBuyer(opportunity, primary?.name);
  const demand =
    scrub(
      opportunity.proposedHotelUse ||
        opportunity.hotelDemandThesis ||
        (Array.isArray(opportunity.inferredHotelDemand)
          ? opportunity.inferredHotelDemand.join(" ")
          : opportunity.inferredHotelDemand) ||
        "",
    ) ||
    `Potential guestrooms and/or meeting/catering demand tied to ${eventName} — verify lodging and venue status before proposing contracted space.`;

  const whySelected = scrub(
    [
      `${org} is linked to ${eventName}.`,
      opportunity.localConnection ? `Location signal: ${opportunity.localConnection}.` : null,
      `Dealality selected this because the activity can create concrete ${fit.positioning === "destination_all_inclusive_meetings" ? "destination residential meeting / lodging" : "mid-size meeting, speaker, or delegate lodging"} demand for ${hotelName}.`,
    ]
      .filter(Boolean)
      .join(" "),
  );

  const whyHotelParts = [];
  const seenWhy = new Set();
  const staleCorridor =
    /medical and association programmes|sits in the Chapultepec\s*\/\s*Reforma\s*\/\s*Polanco corridor used by/i;
  const hasSpecificThesis = Boolean(
    text(opportunity.hotelWinThesis || opportunity.hotelFitRationale),
  );
  // Opportunity-specific thesis first — generic hotel facility strengths are defense-in-depth only.
  // When a win thesis already exists, do NOT re-append localConnection/chapultepecFit that may
  // still carry the Stage-5 corridor boilerplate (that undoes commercial-depth repairs).
  for (const part of [
    opportunity.hotelWinThesis || opportunity.hotelFitRationale || null,
    !hasSpecificThesis && opportunity.localConnection && !staleCorridor.test(String(opportunity.localConnection))
      ? `${hotelName} relevance: ${opportunity.localConnection}`
      : null,
    !hasSpecificThesis
      ? opportunity.chapultepecFit || opportunity.propertyFitNote || null
      : null,
    ...(hasSpecificThesis ? [] : (fit.strengths || []).slice(0, 2)),
    (fit.limitations || [])[0] ? `Limitation to respect: ${fit.limitations[0]}` : null,
  ]) {
    const p = scrub(part);
    if (!p) continue;
    const key = p.toLowerCase();
    if (seenWhy.has(key)) continue;
    seenWhy.add(key);
    whyHotelParts.push(p);
  }
  const whyHotel =
    whyHotelParts.join(". ") ||
    `Assess ${hotelName} against confirmed ${eventName} geography, dates, and lodging mechanism before pitching — generic facilities alone are not a fit thesis.`;

  const buyerAccess = assessBuyerOrgAndAccessibility(opportunity, {
    hotelName,
  });
  const buyingStatus = scrub(
    buyerAccess.accessibilitySummary ||
      opportunity.buyingStatus ||
      opportunity.sourcingStatus ||
      "Venue and lodging selection status is not fully confirmed in public sources — verify organizer sourcing timetable and whether a host hotel or room block is already contracted.",
  );

  // Prefer org-role / accessibility-aware action over generic Identify-organizer copy.
  // Sponsorship contacts remain referral-only via buyerAccess.referralOnly.
  const approach = scrub(
    buyerAccess.recommendedAction ||
      (primary
        ? `Contact ${primary.name}${primary.title ? ` (${primary.title})` : ""} to introduce ${hotelName} for ${demand.slice(0, 160)}. Ask how attendee lodging, speaker rooms, and meeting/catering needs are currently handled for ${eventName}, and whether venue selection is open.`
        : `Identify the meetings / congress / housing owner for ${eventName} (not sponsorship alone) and introduce ${hotelName}; confirm sourcing timetable and whether hotel selection remains open.`),
  );

  const qualificationQuestions = [
    "How many overnight rooms are typically needed for speakers, staff, and delegates?",
    "Is meeting/catering space required on-property, or is lodging the primary need?",
    "Has a host venue or room block already been contracted for this edition?",
    "Who owns hotel sourcing decisions — association staff, appointed PCO, or both?",
    "What is the decision timetable for the next edition?",
  ];

  const sourcePack = dedupeEvidenceByCanonicalUrl(
    [
      ...(opportunity.sources || []),
      ...(opportunity.evidence || []),
    ].map((s) => (typeof s === "string" ? { url: s } : s)),
  );
  const sources = sourcePack.items.filter((s) => s?.url).slice(0, 8);

  const brief = {
    version: ASSOCIATION_EVENTS_GOLD_BRIEF_V1,
    opportunityOverview: scrub(
      `${eventName} — ${org}. Commercial stage: ${qual.status}. Timing: ${text(opportunity.eventStartDate || opportunity.dateWindow || opportunity.timing || "to be verified")}.`,
    ),
    whyDealalitySelected: whySelected,
    organizationBackground: scrub(
      opportunity.organizationBackground ||
        opportunity.companyBlurb ||
        `${org} organizes or sponsors professional association / conference activity relevant to ${hotelName}'s market.`,
    ),
    eventHistoryAndActivity: scrub(
      opportunity.eventHistory ||
        opportunity.whatIsHappening ||
        opportunity.localConnection ||
        `Public evidence links ${org} to ${eventName}; confirm prior editions and the next confirmed cycle.`,
    ),
    potentialBusinessForHotel: demand,
    whyThisHotel: whyHotel || fit.strengths.join("; "),
    buyingAndSourcingStatus: buyingStatus,
    whoToContact: (() => {
      const contactEligibility = primary?.name
        ? classifyContactEligibility(
            {
              name: primary.name,
              role: primary.title || opportunity.contactRole,
              title: primary.title || opportunity.contactRole,
            },
            opportunity,
          )
        : null;
      const briefFields = buildContactEligibilityBriefFields(contactEligibility || {});
      return {
        primary: primary
          ? {
              ...primary,
              contactEligibilityClass: briefFields.contactEligibilityClass,
              contactTypeForBrief: briefFields.contactTypeForBrief,
              isDirectBuyer: briefFields.isDirectBuyer,
              isRelevantCommercialEntryPoint:
                buyerAccess.referralOnly === true
                  ? false
                  : briefFields.isRelevantCommercialEntryPoint,
              contactEligibilityNote:
                buyerAccess.referralOnly === true
                  ? "REFERRAL_ONLY_NOT_LODGING_BUYER"
                  : null,
            }
          : null,
        secondary,
        routes: {
          email: primary?.email || opportunity.contactEmail || null,
          phone: primary?.phone || opportunity.contactPhone || null,
          url:
            primary?.url ||
            opportunity.contactUrl ||
            opportunity.organizationRouteUrl ||
            buyerAccess.orgRouteUrl ||
            null,
        },
        contactEligibilityClass: briefFields.contactEligibilityClass,
        contactTypeForBrief: briefFields.contactTypeForBrief,
        isDirectBuyer: briefFields.isDirectBuyer,
        isRelevantCommercialEntryPoint:
          buyerAccess.referralOnly === true
            ? false
            : briefFields.isRelevantCommercialEntryPoint,
        continueResearchForDirectBuyer: briefFields.continueResearchForDirectBuyer,
        orgRoleClass: buyerAccess.orgRoleClass,
        sourcingAccessibilityClass: buyerAccess.sourcingAccessibilityClass,
        referralOnly: buyerAccess.referralOnly === true,
        contactPromotion: buyerAccess.contactPromotion,
      };
    })(),
    recommendedSalesApproach: approach,
    orgRoleClass: buyerAccess.orgRoleClass,
    sourcingAccessibilityClass: buyerAccess.sourcingAccessibilityClass,
    qualificationQuestions,
    supportingEvidenceAndUncertainties: {
      sources,
      uniqueSourceCount: sourcePack.uniqueSourceCount,
      rawSourceCountBeforeDedupe: sourcePack.rawCount,
      uncertainties: qual.reasons.length
        ? qual.reasons
        : [
            "Confirm next-edition dates and lodging scope",
            "Confirm whether venue/hotel selection is still open",
          ],
    },
    hotelValidationActionOutcome: {
      note: "Reuse existing GDI hotel-sales feedback fields when published.",
      validationStatus: opportunity.hotelValidationStatus || null,
      actionStatus: opportunity.hotelActionStatus || null,
      outcomeStatus: opportunity.hotelOutcomeStatus || null,
    },
    qualification: qual,
  };

  return {
    ...opportunity,
    researchSegment:
      opportunity.researchSegment || "associations_government",
    discoverySource:
      opportunity.discoverySource || "association_events_powershell_v1",
    associationEventsGoldBriefVersion: ASSOCIATION_EVENTS_GOLD_BRIEF_V1,
    associationEventsGoldBrief: brief,
    orgRoleClass: buyerAccess.orgRoleClass,
    sourcingAccessibilityClass: buyerAccess.sourcingAccessibilityClass,
    commercialAccessibilitySummary: buyerAccess.accessibilitySummary,
    buyerOrgAccessibility: buyerAccess,
    recommendedAction: approach,
    salesActionRecommended: approach,
    suggestedAction: approach,
    // Customer-facing field aliases used by drawer contracts
    salesBriefOverview: brief.opportunityOverview,
    salesBriefWhySelected: brief.whyDealalitySelected,
    salesBriefOrganization: brief.organizationBackground,
    salesBriefActivity: brief.eventHistoryAndActivity,
    salesBriefHotelBusiness: brief.potentialBusinessForHotel,
    salesBriefWhyHotel: brief.whyThisHotel,
    salesBriefBuyingStatus: brief.buyingAndSourcingStatus,
    salesBriefApproach: brief.recommendedSalesApproach,
    salesBriefQuestions: brief.qualificationQuestions,
    proposedHotelUse: opportunity.proposedHotelUse || demand,
    hotelFitRationale: opportunity.hotelFitRationale || brief.whyThisHotel,
  };
}

export function scoreAssociationEventsGoldBrief(opportunity = {}) {
  const b = opportunity.associationEventsGoldBrief;
  if (!b) return { ok: false, score: 0, missing: ["gold_brief_absent"] };
  const required = [
    "opportunityOverview",
    "whyDealalitySelected",
    "organizationBackground",
    "eventHistoryAndActivity",
    "potentialBusinessForHotel",
    "whyThisHotel",
    "buyingAndSourcingStatus",
    "recommendedSalesApproach",
  ];
  const missing = required.filter((k) => !text(b[k]));
  const primaryName = b.whoToContact?.primary?.name;
  if (!isAssociationEventsValidPersonName(primaryName)) {
    missing.push("primary_named_buyer");
  }
  // Critical factual errors invalidate commercial quality regardless of prose length.
  // Live phone fields only — scrubbed/_rejected phones are remediation evidence, not
  // an eternal gold-fail after the polluted value was removed from presentation.
  const critical = [];
  const phoneCandidates = [
    b.whoToContact?.primary?.phone,
    opportunity.contactPhone,
    opportunity.companyPhone,
    opportunity.primaryContact?.phone,
  ].filter(Boolean);
  if (phoneCandidates.some((p) => !scrubAssociationEventsPhone(p))) {
    critical.push("polluted_phone");
  }
  if (
    b.whoToContact?.primary?.name &&
    !isAssociationEventsValidPersonName(b.whoToContact.primary.name)
  ) {
    critical.push("invalid_primary_buyer_identity");
  }
  const chrono = opportunity.associationEventsChronology?.status;
  if (
    chrono === EVENT_CHRONOLOGY_STATUS.PAST_COMPLETED &&
    /upcoming|próxim|act now|urgent/i.test(`${b.opportunityOverview} ${b.whyDealalitySelected}`)
  ) {
    critical.push("past_event_presented_as_upcoming");
  }
  if (opportunity.hostHotelContracted === true && opportunity.externalRoomBlocksAvailable === false) {
    critical.push("closed_host_presented_as_open_demand");
  }
  if (/santa fe business forum/i.test(primaryName || "") && /larim|astronom/i.test(opportunity.eventName || "")) {
    critical.push("wrong_buyer_entity");
  }
  const score = Math.max(0, 100 - missing.length * 10 - critical.length * 25);
  return {
    ok: missing.length === 0 && critical.length === 0,
    score,
    missing,
    criticalErrors: critical,
    version: ASSOCIATION_EVENTS_GOLD_BRIEF_V1,
  };
}
