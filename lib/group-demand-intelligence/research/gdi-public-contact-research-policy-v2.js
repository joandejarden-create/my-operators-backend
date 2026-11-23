/**
 * GDI Public Contact Research Policy V2
 *
 * Improves buyer identification without paid enrichment (no Surfe / Findymail /
 * PDL / FullEnrich). Preserves verified names without requiring personal email.
 * Maintains strict Sales Ready classification.
 *
 * Coordinates with customer UI contact-display regression (#575): never invent
 * contacts; never overwrite verified customer contacts with gap labels.
 */

export const GDI_PUBLIC_CONTACT_RESEARCH_POLICY_V2 =
  "gdi-public-contact-research-policy-v2";

export const PRIORITY_BUYER_ROLES = Object.freeze([
  "meeting_event_organizer",
  "corporate_travel_procurement_buyer",
  "association_event_manager",
  "destination_event_coordinator",
  "corporate_project_workforce_travel_decision_maker",
  "sales_or_sourcing_entry_point",
]);

export const PUBLIC_SOURCE_CLASSES = Object.freeze([
  "official_org_staff_directory",
  "event_about_or_committee_page",
  "press_release_named_spokesperson",
  "linkedin_public_role_page",
  "destination_dmc_or_cvb_contact",
  "conference_housing_bureau_contact",
]);

/**
 * Decide whether a named person may be retained without a personal email.
 */
export function evaluateNamedContactRetention(contact = {}, opts = {}) {
  const name = String(contact.name || contact.fullName || "").trim();
  const role = String(contact.role || contact.title || "").trim();
  const employer = String(contact.employer || contact.organization || "").trim();
  const email = String(contact.email || contact.personalEmail || "").trim();
  const sourceUrl = String(contact.sourceUrl || contact.url || "").trim();
  const evidence = contact.evidence || contact.sources || [];

  if (!name || name.length < 3) {
    return {
      retain: false,
      reasonCode: "NO_VERIFIED_NAME",
      salesReady: false,
    };
  }
  // Reject gap / pollution labels (align with UI regression #575)
  if (
    /contact research required|research required|unknown contact|tbd|n\/a|not found/i.test(
      name,
    )
  ) {
    return {
      retain: false,
      reasonCode: "GAP_LABEL_NOT_A_PERSON",
      salesReady: false,
    };
  }

  const hasPublicEvidence =
    Boolean(sourceUrl) ||
    (Array.isArray(evidence) && evidence.length > 0) ||
    Boolean(contact.linkedinUrl);

  if (!hasPublicEvidence && opts.requirePublicEvidence !== false) {
    return {
      retain: false,
      reasonCode: "NAME_WITHOUT_PUBLIC_EVIDENCE",
      salesReady: false,
    };
  }

  // Personal email never required to retain a verified name.
  const salesReady =
    contact.salesReady === true &&
    Boolean(email) &&
    /@/.test(email) &&
    opts.allowSalesReadyPromotion === true;

  return {
    retain: true,
    reasonCode: email
      ? "VERIFIED_NAME_WITH_EMAIL"
      : "VERIFIED_NAME_WITHOUT_EMAIL_OK",
    salesReady: salesReady === true,
    preservedFields: {
      name,
      role: role || null,
      employer: employer || null,
      email: email || null,
      sourceUrl: sourceUrl || null,
      relevance: contact.relevance || contact.whyRelevant || null,
    },
    paidEnrichment: {
      surfe: false,
      findymail: false,
      pdl: false,
      fullenrich: false,
    },
  };
}

/**
 * LaLuna pilot gap analysis — destination / association deeps lacked public
 * named organizers; corporate expansion paths (Salesforce/TDCX) surfaced names.
 */
export function analyzeDesignHotelsContactCoverage(packages = []) {
  const byHotel = [];
  for (const hotel of packages) {
    const opps = hotel.opportunities || [];
    let named = 0;
    const gaps = [];
    for (const opp of opps) {
      const names = [
        opp.primaryContactName,
        opp.contactName,
        ...(opp.namedStakeholders || []).map((s) => s?.name),
        ...(opp.contacts || []).map((c) => c?.name),
      ].filter(Boolean);
      if (names.length) named += names.length;
      else {
        gaps.push({
          opportunityId: opp.id || opp.opportunityId,
          title: opp.title,
          organizationName: opp.organizationName,
          recommendedBuyerRoles: PRIORITY_BUYER_ROLES,
          recommendedSources: PUBLIC_SOURCE_CLASSES,
        });
      }
    }
    byHotel.push({
      hotelId: hotel.hotelId,
      hotelName: hotel.hotelName || null,
      opportunityCount: opps.length,
      namedStakeholderCount: named,
      gaps,
      diagnosis:
        named === 0
          ? "NO_NAMED_STAKEHOLDERS — prioritize destination organizers / DMC / association event managers on public pages before next paid deep"
          : "PARTIAL_NAMED_COVERAGE",
    });
  }
  return {
    contract: GDI_PUBLIC_CONTACT_RESEARCH_POLICY_V2,
    byHotel,
    policy: {
      retainNamesWithoutEmail: true,
      inventContacts: false,
      paidEnrichment: false,
      salesReadyFailClosed: true,
      coordinateWithUiRegressionPr: 575,
    },
  };
}

/**
 * Prompt guidance injected into deep contact research (public sources only).
 */
export function buildPublicContactResearchBrief(opportunity = {}, hotelConfig = {}) {
  const classification = String(
    hotelConfig?.capabilityProfile?.classification || "",
  ).toLowerCase();
  const leisure = /leisure|destination|villa|resort/.test(classification);
  return {
    contract: GDI_PUBLIC_CONTACT_RESEARCH_POLICY_V2,
    opportunityId: opportunity.id || opportunity.opportunityId || null,
    priorityRoles: PRIORITY_BUYER_ROLES,
    publicSourceClasses: PUBLIC_SOURCE_CLASSES,
    hotelSpecificGuidance: leisure
      ? [
          "Prefer destination-event coordinators, sailing/regatta organizers, DMC / travel-trade intermediaries, wedding planners with public profiles.",
          "Association conferences: seek local host committee / housing bureau contacts on official event pages.",
          "Do not invent emails; retain verified names + roles + employer + source URL.",
        ]
      : [
          "Prefer meeting organizers, corporate travel/procurement, association event managers, project travel decision-makers.",
          "Corporate expansion: workplace / travel manager / local GM public pages are valid named stakeholders.",
          "Do not invent emails; retain verified names + roles + employer + source URL.",
        ],
    salesReady: "FAIL_CLOSED — name alone is never Sales Ready",
    paidProvidersForbidden: ["surfe", "findymail", "pdl", "fullenrich"],
  };
}

export default {
  GDI_PUBLIC_CONTACT_RESEARCH_POLICY_V2,
  PRIORITY_BUYER_ROLES,
  PUBLIC_SOURCE_CLASSES,
  evaluateNamedContactRetention,
  analyzeDesignHotelsContactCoverage,
  buildPublicContactResearchBrief,
};
