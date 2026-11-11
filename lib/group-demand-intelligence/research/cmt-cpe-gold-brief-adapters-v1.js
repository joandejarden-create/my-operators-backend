/**
 * Phase 3B-R — compatibility adapters mapping CMT/CPE opportunities onto
 * Scaling commercial intelligence brief + Association Events gold-brief shapes.
 * Shadow / offline only — does not publish or mutate hotelFacingEligible.
 */

import { buildScalingCommercialIntelligenceBrief } from "../scaling-commercial-intelligence-brief-v1.js";
import { applyAssociationEventsGoldBrief } from "./association-events-gold-brief-v1.js";
import {
  classifyContactEligibility,
  buildContactEligibilityBriefFields,
} from "../contact-eligibility-standard-v1.js";
import { classifyCommercialDemandThesis } from "../commercial-demand-thesis-standard-v1.js";

export const CMT_CPE_GOLD_BRIEF_ADAPTERS_V1 = "gdi-cmt-cpe-gold-brief-adapters-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function toScalingShapedOpportunity(opportunity = {}, segment = "cmt") {
  const org = text(opportunity.organizationName);
  const activity =
    text(opportunity.activityHeadline) ||
    text(opportunity.eventName) ||
    text(opportunity.whatIsHappening) ||
    org;
  return {
    ...opportunity,
    organizationName: org,
    company: org,
    activityHeadline: activity,
    growthHeadline: activity,
    whatIsHappening:
      text(opportunity.whatIsHappening) ||
      text(opportunity.hotelDemandThesis) ||
      activity,
    proposedHotelUse: text(opportunity.proposedHotelUse) || text(opportunity.hotelDemandThesis),
    hotelDemandThesis: text(opportunity.hotelDemandThesis),
    localConnection: text(opportunity.localConnection || opportunity.city || opportunity.location),
    contactName: opportunity.contactName || opportunity.primaryContact?.name,
    contactRole: opportunity.contactRole || opportunity.primaryContact?.role,
    organizationContactEmail:
      opportunity.organizationContactEmail ||
      opportunity.primaryContact?.email ||
      opportunity.contactEmail,
    sources: opportunity.sources || [],
    researchSegment: opportunity.researchSegment || segment,
  };
}

/**
 * Build a sales-facing shadow brief for CMT or CPE by adapting Scaling + AE gold patterns.
 */
export function buildCmtCpeGoldStandardBrief({
  opportunity = {},
  hotel = {},
  qualification = {},
  hotelMatch = {},
  segment = "cmt",
} = {}) {
  const shaped = toScalingShapedOpportunity(opportunity, segment);
  const thesis = classifyCommercialDemandThesis(shaped, {});
  const contact = classifyContactEligibility(
    {
      name: shaped.contactName,
      role: shaped.contactRole,
      email: shaped.organizationContactEmail,
      organization: shaped.organizationName,
      activityLink: shaped.activityHeadline,
      activityConnectionEvidence: shaped.evidenceQuote || shaped.hotelDemandThesis,
    },
    shaped,
    {
      contactDataOrigin: opportunity.contactDataOrigin || "PUBLIC_SOURCE",
      affiliationVerified: Boolean(shaped.organizationName),
    },
  );
  const contactFields = buildContactEligibilityBriefFields(contact);

  let scalingBrief = null;
  try {
    scalingBrief = buildScalingCommercialIntelligenceBrief(shaped, hotel, {
      continuityEvidence: false,
    });
  } catch {
    scalingBrief = null;
  }

  let associationShaped = null;
  try {
    associationShaped = applyAssociationEventsGoldBrief(
      {
        ...shaped,
        eventName: shaped.activityHeadline,
        organizer: shaped.organizationName,
        inferredHotelDemand: shaped.proposedHotelUse || shaped.hotelDemandThesis,
      },
      { hotel },
    );
  } catch {
    associationShaped = null;
  }

  const confirmedVsInferred = {
    thesisStandard: thesis.thesisStandard,
    confirmedFacts: [
      shaped.organizationName ? `Organization: ${shaped.organizationName}` : null,
      shaped.activityHeadline ? `Activity: ${shaped.activityHeadline}` : null,
      (shaped.sources || [])[0]?.url ? `Source: ${(shaped.sources || [])[0].url}` : null,
    ].filter(Boolean),
    reliableInferences: [
      shaped.hotelDemandThesis || null,
      shaped.proposedHotelUse || null,
      hotelMatch?.fitClass ? `Hotel fit class: ${hotelMatch.fitClass}` : null,
    ].filter(Boolean),
  };

  const gaps = [
    !contact.eligible
      ? "Named direct buyer or verified activity-linked entry point still incomplete"
      : null,
    thesis.thesisStandard === "SPECULATIVE_DEMAND"
      ? "Demand thesis remains speculative — continue research"
      : null,
    !hotelMatch?.commerciallyAccessible
      ? "Hotel-specific commercial accessibility not yet established"
      : null,
  ].filter(Boolean);

  const nextAction =
    qualification?.status === "SHADOW_SALES_READY"
      ? `Introduce ${text(hotel.displayName || hotel.name) || "the hotel"} to ${shaped.contactName || "the verified contact"} regarding ${shaped.activityHeadline}.`
      : gaps[0]
        ? `Continue research: ${gaps[0]}`
        : "Continue bounded commercial research before outreach.";

  return {
    version: CMT_CPE_GOLD_BRIEF_ADAPTERS_V1,
    segment,
    investigationId: opportunity.investigationId || null,
    opportunityAndOrganization: {
      organizationName: shaped.organizationName,
      activityHeadline: shaped.activityHeadline,
      researchSegment: shaped.researchSegment,
      discoveryOrigin: opportunity.discoveryOrigin || null,
    },
    whatIsHappening: shaped.whatIsHappening,
    whyHotelDemand:
      shaped.hotelDemandThesis ||
      shaped.proposedHotelUse ||
      "Hotel demand still under investigation — do not invent room nights or RFPs.",
    whyThisHotel:
      hotelMatch?.rationale ||
      hotelMatch?.fitClass ||
      scalingBrief?.hotelSpecificFit ||
      associationShaped?.whyThisHotel ||
      null,
    confirmedFactsVersusReliableInferences: confirmedVsInferred,
    namedContact: {
      name: shaped.contactName || null,
      contactType: contactFields.contactTypeForBrief || contact.class || null,
      contactEligibilityClass: contact.class || null,
      eligible: contact.eligible === true,
      isDirectBuyer: contactFields.isDirectBuyer === true,
      isRelevantCommercialEntryPoint: contactFields.isRelevantCommercialEntryPoint === true,
      continueResearchForDirectBuyer: contactFields.continueResearchForDirectBuyer === true,
      route: shaped.organizationContactEmail || null,
    },
    commercialResearchGaps: gaps,
    recommendedNextSalesAction: nextAction,
    evidenceAndSources: (shaped.sources || []).slice(0, 8),
    reusedBuilders: {
      scalingCommercialIntelligenceBrief: Boolean(scalingBrief),
      associationEventsGoldBrief: Boolean(associationShaped),
    },
    scalingBriefExcerpt: scalingBrief
      ? {
          whatIsHappening: scalingBrief.whatIsHappening || null,
          recommendedApproach: scalingBrief.recommendedApproach || null,
          researchCompleteness: scalingBrief.researchCompleteness || null,
        }
      : null,
    associationBriefExcerpt: associationShaped
      ? {
          opportunityOverview: associationShaped.opportunityOverview || null,
          recommendedApproach: associationShaped.recommendedApproach || associationShaped.approach || null,
        }
      : null,
    qualificationStatus: qualification?.status || null,
    hotelFacingEligibleUnchanged: true,
    customerVisibilityUnchanged: true,
    doesNotAuthorizePublication: true,
    mockedOrOfflineOnly: true,
  };
}
