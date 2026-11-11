/**
 * Scaling contact-card presentation contract V1
 *
 * Same shell/rules across contact states; data may differ.
 * Does not trigger paid enrichment.
 */

export const SCALING_CONTACT_CARD_CONTRACT_V1 =
  "gdi-scaling-contact-card-contract-v1";

/** Canonical contact presentation states for Scaling drawers. */
export const SCALING_CONTACT_STATES = Object.freeze([
  "VERIFIED_RELEVANT_BUYER",
  "POTENTIAL_INFLUENCER",
  "ORGANIZATIONAL_REFERRAL_ROUTE",
  "CONTACT_RESEARCH_PENDING",
  "MISSING_CONTACT",
  "PROVIDER_LOOKUP_UNAVAILABLE",
]);

export const REQUIRED_CONTACT_SHELL_MARKERS = Object.freeze([
  "who_to_contact", // section id
  "Who to contact", // heading
]);

/**
 * Classify contact state from opportunity fields (presentation only).
 */
export function classifyScalingContactState(opportunity = {}) {
  const c = opportunity.primaryContact || null;
  const name = String(c?.name || opportunity.contactName || "").trim();
  const email = String(c?.email || opportunity.contactEmail || "").trim();
  const phone = String(c?.phone || opportunity.contactPhone || "").trim();
  const named =
    opportunity.hasVerifiedNamedRelevantContact === true ||
    (name && !/^(relevant contact being researched|—|-)$/i.test(name));
  const orgPath =
    opportunity.commercialContactPath ||
    opportunity.organizationContactEmail ||
    opportunity.publicContactPath ||
    opportunity.contactUrl ||
    null;
  const pending =
    opportunity.contactResearchAttempted === true &&
    !named &&
    /PAGES_SCANNED|RESEARCH|PENDING/i.test(
      String(opportunity.whoStatus || opportunity.scalingQualificationStatus || ""),
    );
  const providerUnavailable =
    opportunity.surfeOnDemandEligible === false &&
    opportunity.contactEnrichmentBlocked === true;

  if (providerUnavailable && named && !email && !phone) {
    return "PROVIDER_LOOKUP_UNAVAILABLE";
  }
  if (named && (email || phone) && opportunity.hasVerifiedNamedRelevantContact === true) {
    return "VERIFIED_RELEVANT_BUYER";
  }
  if (named && !email && !phone) {
    return opportunity.contactRelevance === "INFLUENCER" ||
      /influencer|referral/i.test(String(opportunity.buyerRole || ""))
      ? "POTENTIAL_INFLUENCER"
      : "CONTACT_RESEARCH_PENDING";
  }
  if (orgPath && !named) return "ORGANIZATIONAL_REFERRAL_ROUTE";
  if (pending) return "CONTACT_RESEARCH_PENDING";
  if (!named && !orgPath) return "MISSING_CONTACT";
  if (named) return "POTENTIAL_INFLUENCER";
  return "ORGANIZATIONAL_REFERRAL_ROUTE";
}

/**
 * Audit rendered contact HTML for shared shell rules.
 */
export function auditScalingContactCardHtml(html, opportunity = {}) {
  const discrepancies = [];
  const state = classifyScalingContactState(opportunity);
  const h = String(html || "");

  if (!/data-section-id=["']who_to_contact["']/i.test(h) && !/Who to contact/i.test(h)) {
    discrepancies.push({
      severity: "P0",
      id: "missing_who_to_contact_section",
      message: "Who to contact section missing",
    });
  }

  // Shared shell: contact area should not invent phone/email placeholders
  if (/tel:\s*#|mailto:\s*#|000-000-0000|example@example\.com/i.test(h)) {
    discrepancies.push({
      severity: "P0",
      id: "fabricated_contact_channel",
      message: "Placeholder or fabricated contact channel rendered",
    });
  }

  // CTA may appear for named-without-channels; must use stable marker
  const hasCta = /data-gdi-contact-cta=["']1["']/i.test(h);
  if (
    state === "CONTACT_RESEARCH_PENDING" ||
    state === "POTENTIAL_INFLUENCER" ||
    state === "PROVIDER_LOOKUP_UNAVAILABLE"
  ) {
    // CTA optional depending on tier; if present must be button type=button
    if (hasCta && !/<button[^>]*data-gdi-contact-cta/i.test(h)) {
      discrepancies.push({
        severity: "P0",
        id: "cta_not_button",
        message: "Contact CTA must be a button element",
      });
    }
  }

  // Org route state should acknowledge no named individual when applicable
  if (state === "ORGANIZATIONAL_REFERRAL_ROUTE") {
    const orgHonest =
      /organization|official|operations contact|contact path|No named individual/i.test(h);
    if (!orgHonest) {
      discrepancies.push({
        severity: "P1",
        id: "org_route_not_honest",
        message: "Organizational route should state no named individual or org path clearly",
      });
    }
  }

  if (state === "MISSING_CONTACT") {
    if (!/not yet identified|research|not published|missing/i.test(h)) {
      discrepancies.push({
        severity: "P1",
        id: "missing_contact_not_labeled",
        message: "Missing contact state should be labeled honestly",
      });
    }
  }

  // Must not claim Surfe auto-enrichment succeeded in static render
  if (/Surfe enriched|auto.?enriched email/i.test(h)) {
    discrepancies.push({
      severity: "P0",
      id: "surfe_auto_claim",
      message: "Must not claim Surfe auto-enrichment in static drawer HTML",
    });
  }

  return {
    ok: discrepancies.filter((d) => d.severity === "P0").length === 0,
    state,
    contract: SCALING_CONTACT_CARD_CONTRACT_V1,
    discrepancies,
    hasGetDetailsCta: hasCta,
  };
}

/**
 * Fixture builders for negative/positive contact-card states.
 */
export function buildContactStateFixture(state) {
  const base = {
    researchSegment: "scaling_companies",
    opportunityType: "CORPORATE_ACCOUNT_RELATIONSHIP",
    title: "Contact Fixture — Scaling Corporate Account",
    organizationName: "Contact Fixture Co",
  };
  switch (state) {
    case "VERIFIED_RELEVANT_BUYER":
      return {
        ...base,
        hasVerifiedNamedRelevantContact: true,
        primaryContact: {
          name: "Alex Buyer",
          role: "Director of Corporate Travel",
          email: "alex.buyer@fixture.example",
          phone: "+1-202-555-0100",
        },
        contactName: "Alex Buyer",
      };
    case "POTENTIAL_INFLUENCER":
      return {
        ...base,
        contactRelevance: "INFLUENCER",
        buyerRole: "Influencer — partnerships",
        primaryContact: { name: "Pat Influencer", role: "Partnerships Manager" },
        contactName: "Pat Influencer",
      };
    case "ORGANIZATIONAL_REFERRAL_ROUTE":
      return {
        ...base,
        primaryContact: null,
        commercialContactPath: "ORGANIZATION_CONTACT_FORM",
        commercialContactPathLabel: "Organization contact path",
        organizationContactEmail: "meetings@fixture.example",
        contactUrl: "https://fixture.example/contact",
      };
    case "CONTACT_RESEARCH_PENDING":
      return {
        ...base,
        contactResearchAttempted: true,
        whoStatus: "PAGES_SCANNED_NO_NAMED_STAKEHOLDER",
        primaryContact: { name: "Relevant contact being researched" },
      };
    case "MISSING_CONTACT":
      return {
        ...base,
        primaryContact: null,
        contactName: null,
        commercialContactPath: null,
      };
    case "PROVIDER_LOOKUP_UNAVAILABLE":
      return {
        ...base,
        hasVerifiedNamedRelevantContact: true,
        surfeOnDemandEligible: false,
        contactEnrichmentBlocked: true,
        primaryContact: { name: "Alex Buyer", role: "Travel Manager" },
        contactName: "Alex Buyer",
      };
    default:
      return base;
  }
}
