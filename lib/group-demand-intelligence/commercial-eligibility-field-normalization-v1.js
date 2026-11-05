/**
 * Commercial eligibility field normalization V1
 *
 * Maps equivalent evidence already present on opportunity bags into the
 * canonical fields the shared commercial eligibility evaluator reads.
 *
 * Rules:
 * - Never invent facts, emails, phones, or demand claims.
 * - Never promote narrative-only emails (e.g. in recommendedNextStep) to
 *   verified contact routes.
 * - Prefer source-backed organization contact URLs already attached as evidence.
 * - Hotel self-site URLs are not organization contact routes for external orgs.
 * - Does not weaken Sales Ready criteria — only field recognition.
 */

import { pickPreferredContactUrl } from "./prospect-publication-eligibility-v1.js";

export const COMMERCIAL_ELIGIBILITY_FIELD_NORMALIZATION_V1 =
  "gdi-commercial-eligibility-field-normalization-v1";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

const CONTACT_ROUTE_EVIDENCE_RE =
  /contact\s*route|organizational\s*contact|enquiry\s*route|inquiry\s*route|public\s*enquir|wedding\s*request|contact-us|contact\s*page|business\s*email|organizational\s*email|sponsorship\/exhibition opportunities and organizational contact|organizer partnership/i;

const LINKEDIN_PERSON_RE = /linkedin\.com\/in\//i;

const HOTEL_SELF_HOST_RE =
  /spiceislandbeachresort\.com|marriott\.com|hilton\.com|hyatt\.com|ihg\.com/i;

/**
 * True when a source row explicitly establishes a commercial contact/outreach path.
 */
export function sourceEstablishesContactRoute(source = {}) {
  const blob = `${text(source.title)} ${text(source.establishes)} ${text(source.note)}`;
  return CONTACT_ROUTE_EVIDENCE_RE.test(blob);
}

/**
 * Collect HTTPS source URLs, excluding person LinkedIn and optional hotel self hosts.
 */
export function collectEvidenceSourceUrls(opportunity = {}, opts = {}) {
  const excludeHotelSelf = opts.excludeHotelSelfHosts !== false;
  const out = [];
  for (const s of opportunity.sources || []) {
    const url = text(typeof s === "string" ? s : s?.url);
    if (!/^https?:\/\//i.test(url)) continue;
    if (LINKEDIN_PERSON_RE.test(url)) continue;
    if (excludeHotelSelf && HOTEL_SELF_HOST_RE.test(url)) continue;
    out.push({ url, source: typeof s === "object" ? s : { url } });
  }
  for (const u of opportunity.evidenceUrls || []) {
    const url = text(u);
    if (!/^https?:\/\//i.test(url)) continue;
    if (LINKEDIN_PERSON_RE.test(url)) continue;
    if (excludeHotelSelf && HOTEL_SELF_HOST_RE.test(url)) continue;
    out.push({ url, source: { url } });
  }
  return out;
}

/**
 * Derive a verified organization contact URL from attached source evidence.
 * Does not read narrative next-step prose for emails.
 */
export function deriveVerifiedOrganizationContactUrlFromSources(
  opportunity = {},
  opts = {}
) {
  const rows = collectEvidenceSourceUrls(opportunity, opts);
  if (!rows.length) {
    return { url: null, reason: "no_source_urls", candidates: [] };
  }

  const contactEvidence = rows.filter((r) => sourceEstablishesContactRoute(r.source));
  const preferredFromContactEvidence = pickPreferredContactUrl(
    contactEvidence.map((r) => r.url)
  );
  if (preferredFromContactEvidence) {
    return {
      url: preferredFromContactEvidence,
      reason: "source_establishes_contact_route",
      candidates: contactEvidence.map((r) => r.url),
    };
  }

  const preferredAny = pickPreferredContactUrl(rows.map((r) => r.url));
  if (preferredAny) {
    return {
      url: preferredAny,
      reason: "preferred_functional_or_non_discovery_source_url",
      candidates: rows.map((r) => r.url).slice(0, 8),
    };
  }

  return { url: null, reason: "no_usable_source_contact_url", candidates: [] };
}

/**
 * Alias hotel-specific thesis fields the readiness / eligibility surfaces expect.
 * Copies only — never invents thesis text.
 */
export function aliasHotelDemandThesisFields(opportunity = {}) {
  const why = text(opportunity.whyThisHotel);
  const thesis = text(opportunity.hotelOpportunityThesis);
  const summary = text(opportunity.summaryWhyHotel);
  const rationale = text(opportunity.hotelFitRationale);
  const demand = text(opportunity.hotelDemandThesis || opportunity.demandThesis);
  const fit = text(opportunity.fitExplanation);

  const patches = {};
  if (!summary && why) patches.summaryWhyHotel = why;
  if (!rationale && why) patches.hotelFitRationale = why;
  if (!fit && why) patches.fitExplanation = why;
  if (!demand && (thesis || why)) {
    patches.hotelDemandThesis = thesis || why;
  }
  return patches;
}

/**
 * Normalize an opportunity bag for commercial eligibility / readiness evaluation.
 * Pure function — does not mutate the input object.
 */
export function normalizeOpportunityForCommercialEligibilityEvaluation(
  opportunity = {},
  opts = {}
) {
  if (!opportunity || typeof opportunity !== "object") {
    return {
      opportunity: opportunity,
      applied: false,
      patches: {},
      notes: ["not_an_object"],
    };
  }

  const patches = { ...aliasHotelDemandThesisFields(opportunity) };
  const notes = [];

  const existingRoute = text(
    opportunity.organizationContactUrl ||
      opportunity.publicContactPath ||
      opportunity.contactUrl ||
      opportunity.officialContactPath ||
      opportunity.primaryContact?.organizationContactRoute ||
      ""
  );

  let routeDerivation = null;
  if (!existingRoute) {
    routeDerivation = deriveVerifiedOrganizationContactUrlFromSources(
      opportunity,
      opts
    );
    if (routeDerivation.url) {
      patches.organizationContactUrl = routeDerivation.url;
      patches.publicContactPath = routeDerivation.url;
      notes.push(`contact_route_from_sources:${routeDerivation.reason}`);
    } else {
      notes.push(`contact_route_unavailable:${routeDerivation.reason}`);
    }
  } else {
    notes.push("contact_route_already_structured");
  }

  // Never lift narrative emails into verified fields.
  const narrativeBlob = [
    opportunity.recommendedNextStep,
    opportunity.recommendedAction,
    opportunity.commercialNextStep,
  ]
    .map(text)
    .join(" ");
  const narrativeEmails = [
    ...narrativeBlob.matchAll(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi),
  ].map((m) => m[0]);
  if (narrativeEmails.length && !text(opportunity.contactEmail || opportunity.primaryContact?.email)) {
    notes.push(
      `narrative_emails_ignored_unverified:${narrativeEmails.slice(0, 3).join(",")}`
    );
  }

  if (!Object.keys(patches).length) {
    return {
      opportunity,
      applied: false,
      patches: {},
      notes,
      routeDerivation,
      version: COMMERCIAL_ELIGIBILITY_FIELD_NORMALIZATION_V1,
    };
  }

  const primaryContact = {
    ...(opportunity.primaryContact || {}),
  };
  if (
    patches.organizationContactUrl &&
    !text(primaryContact.organizationContactRoute)
  ) {
    primaryContact.organizationContactRoute = patches.organizationContactUrl;
  }

  const normalized = {
    ...opportunity,
    ...patches,
    primaryContact,
    commercialEligibilityFieldNormalization: {
      version: COMMERCIAL_ELIGIBILITY_FIELD_NORMALIZATION_V1,
      applied: true,
      patches: { ...patches },
      notes,
      routeDerivation,
    },
  };

  return {
    opportunity: normalized,
    applied: true,
    patches,
    notes,
    routeDerivation,
    version: COMMERCIAL_ELIGIBILITY_FIELD_NORMALIZATION_V1,
  };
}
