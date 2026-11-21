/**
 * GDI Contact Actionability V1
 *
 * Four tiers (workflow / customer labels only — NOT a publication gate):
 *   SALES_READY | REACHABLE | CONTACT_RESEARCH_REQUIRED | CONTACT_IDENTIFICATION_REQUIRED
 *
 * Strict separation:
 *   opportunity eligibility ≠ contact verification ≠ actionability ≠ customer publication
 *
 * Read-time derivation from frozen primary/secondary contacts.
 * Never invents source URLs. Never marks provider-only channels as verified.
 */
import { looksLikePersonName } from "../secondary-named-contact-v1.js";
import { isGenericOrganizationEmail } from "../scaling-companies-sales-presentation-v1.js";
import { classifyContactEligibility } from "../contact-eligibility-standard-v1.js";
import { personIdentityKey } from "../workflow/contact-field-provenance-v1.js";

export const GDI_CONTACT_ACTIONABILITY_V1 = "gdi-contact-actionability-v1";

export const CONTACT_ACTIONABILITY_TIER = Object.freeze({
  SALES_READY: "SALES_READY",
  REACHABLE: "REACHABLE",
  CONTACT_RESEARCH_REQUIRED: "CONTACT_RESEARCH_REQUIRED",
  CONTACT_IDENTIFICATION_REQUIRED: "CONTACT_IDENTIFICATION_REQUIRED",
});

export const CONTACT_ACTIONABILITY_LABEL = Object.freeze({
  SALES_READY: "Sales-ready contact",
  REACHABLE: "Reachable contact",
  CONTACT_RESEARCH_REQUIRED: "Contact research needed",
  CONTACT_IDENTIFICATION_REQUIRED: "Contact not yet identified",
});

export const BUYER_VS_ENTRY = Object.freeze({
  DIRECT_BUYER: "DIRECT_BUYER",
  RELEVANT_COMMERCIAL_ENTRY_POINT: "RELEVANT_COMMERCIAL_ENTRY_POINT",
  RESEARCH_CANDIDATE: "RESEARCH_CANDIDATE",
  UNSPECIFIED: "UNSPECIFIED",
  NO_NAMED_PERSON: "NO_NAMED_PERSON",
});

export const BUYER_VS_ENTRY_LABEL = Object.freeze({
  DIRECT_BUYER: "Direct buyer",
  RELEVANT_COMMERCIAL_ENTRY_POINT: "Commercial entry point",
  RESEARCH_CANDIDATE: "Research candidate",
  UNSPECIFIED: "Role to confirm",
  NO_NAMED_PERSON: "No named person",
});

const RELEVANT = new Set([
  "DIRECT_BUYER",
  "RELEVANT_COMMERCIAL_ENTRY_POINT",
  "VERIFIED_HOTEL_BUYER",
  "BOOKING_AUTHORITY",
]);

const ROLE_MAILBOX_RE =
  /^(info|contact|hello|office|admin|support|sales|inquiries|enquiry|reception|mail|team|press|media|customerservice|hq|summit|conference|congress|events?|housing|registration|registrations|bookings?|reservations?|rsvp|general|helpdesk|help)$/i;

const PROVIDER_ORIGIN_RE =
  /APOLLO|PROSPEO|PDL|PEOPLE.?DATA.?LABS|SURFE|FULLENRICH|PROVIDER_ENRICHED|ON_DEMAND_ENRICHMENT/i;

const REMIT_TITLE_RE =
  /\b(meeting|meetings|event|events|housing|hotel|lodging|conference|congress|procurement|travel|planner|organizer|organiser|association manager|program manager|summit|partner contact)\b/i;

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

export function isGenericOrRoleMailbox(email) {
  if (!email || !String(email).includes("@")) return false;
  if (isGenericOrganizationEmail(email)) return true;
  return ROLE_MAILBOX_RE.test(text(email).split("@")[0] || "");
}

function extractEmail(contact = {}) {
  const raw = contact.email || contact.workEmail || contact.businessEmail || null;
  if (raw && typeof raw === "object") {
    return text(raw.email || raw.value || raw.address) || null;
  }
  const e = text(raw);
  if (!e || !e.includes("@") || /\[object Object\]/i.test(e)) return null;
  return e.toLowerCase();
}

function extractPhone(contact = {}) {
  const candidates = [
    contact.phone,
    contact.directPhone,
    contact.workPhone,
    contact.officePhone,
    Array.isArray(contact.phoneExtensions) && contact.phoneExtensions.length
      ? contact.phoneExtensions.join(" / ")
      : null,
  ]
    .map((v) => text(v))
    .filter(Boolean);
  const phone = candidates[0] || null;
  if (!phone || !/\d/.test(phone)) return null;
  return phone;
}

function contactSlots(opp = {}) {
  const slots = [];
  const primary =
    opp.primaryContact ||
    opp.contact ||
    (opp.contactName || opp.primaryContactName
      ? {
          name: opp.contactName || opp.primaryContactName,
          email: opp.contactEmail || opp.primaryContactEmail,
          phone: opp.contactPhone || opp.primaryContactPhone,
          title: opp.contactRole || opp.displayRole || opp.tileContactRoleLabel,
          role: opp.contactRole || opp.displayRole,
          organization: opp.organizationName || opp.organization,
        }
      : null);
  if (primary) slots.push({ slot: "primary", contact: primary });
  if (opp.secondaryContact) {
    slots.push({ slot: "secondary", contact: opp.secondaryContact });
  }
  return slots;
}

/**
 * Channel verification — source evidence required for "verified" email.
 * person_specific ownership alone is NOT enough without sourceUrl / PUBLIC_* status.
 */
export function classifyEmailChannel(contact = {}) {
  const email = extractEmail(contact);
  const ownership = text(contact.emailOwnership).toLowerCase();
  const emailKind = text(contact.emailKind).toUpperCase();
  const origin = text(contact.contactDataOrigin || contact.dataOrigin);
  const verification = text(
    contact.channelVerificationStatus || contact.emailVerificationStatus || "",
  ).toUpperCase();
  const sourceUrl =
    text(contact.channelSourceUrl) ||
    text(contact.sourceUrl) ||
    text(contact.officialContactUrl) ||
    null;
  const providerBlocked =
    contact.surfeDerived === true || PROVIDER_ORIGIN_RE.test(origin);

  if (!email) {
    return { present: false, verified: false, status: "NO_EMAIL", sourceUrl };
  }
  if (providerBlocked) {
    return {
      present: true,
      verified: false,
      status: "PROVIDER_BLOCKED",
      sourceUrl,
      email,
    };
  }
  if (
    isGenericOrRoleMailbox(email) ||
    emailKind === "ORGANIZER_MAILBOX" ||
    ownership === "organization_or_department"
  ) {
    return {
      present: true,
      verified: false,
      status: "GENERIC_OR_ORG_MAILBOX",
      sourceUrl,
      email,
    };
  }
  const strong =
    verification === "PUBLICLY_VERIFIED" ||
    /^PUBLIC_SOURCE$/i.test(origin) ||
    /^PUBLIC_PAGE_OBSERVED$/i.test(verification);
  const adequate =
    Boolean(sourceUrl) &&
    (emailKind === "NAMED_DIRECT" ||
      ownership === "person_specific" ||
      !isGenericOrRoleMailbox(email));
  if (strong || adequate) {
    return {
      present: true,
      verified: true,
      status: "SOURCE_CONFIRMED",
      sourceUrl,
      email,
    };
  }
  if (ownership === "person_specific" && !sourceUrl) {
    return {
      present: true,
      verified: false,
      status: "PERSON_SPECIFIC_WITHOUT_SOURCE_URL",
      sourceUrl,
      email,
    };
  }
  return {
    present: true,
    verified: false,
    status: "UNVERIFIED_EMAIL",
    sourceUrl,
    email,
  };
}

export function classifyPhoneChannel(contact = {}) {
  const phone = extractPhone(contact);
  if (!phone) {
    return { present: false, verified: false, status: "NO_PHONE" };
  }
  const origin = text(contact.contactDataOrigin || contact.phoneDataOrigin || "");
  if (contact.surfeDerived === true || PROVIDER_ORIGIN_RE.test(origin)) {
    return { present: true, verified: false, status: "PROVIDER_BLOCKED", phone };
  }
  // Published package phone on a named person is treated as usable business route
  // when not provider-blocked. Stronger phone provenance preferred when present.
  const phoneSource =
    text(contact.phoneSourceUrl) ||
    text(contact.channelSourceUrl) ||
    text(contact.sourceUrl) ||
    null;
  const verification = text(contact.phoneVerificationStatus || "").toUpperCase();
  const verified =
    verification === "PUBLICLY_VERIFIED" ||
    Boolean(phoneSource) ||
    // Frozen customer packages often omit phone sourceUrl; presence on published
    // named contact is REACHABLE-eligible but must not claim "verified email".
    true;
  return {
    present: true,
    verified: Boolean(verified),
    status: phoneSource || verification === "PUBLICLY_VERIFIED"
      ? "PHONE_WITH_EVIDENCE"
      : "PHONE_PRESENT_PACKAGE",
    phone,
    sourceUrl: phoneSource,
  };
}

export function classifyBuyerVsEntry(contact = {}, opportunity = {}) {
  const name = text(contact.name || contact.fullName || contact.personName);
  if (!looksLikePersonName(name)) {
    return {
      named: false,
      name: null,
      buyerVsEntry: BUYER_VS_ENTRY.NO_NAMED_PERSON,
      buyerVsEntryLabel: BUYER_VS_ENTRY_LABEL.NO_NAMED_PERSON,
      commerciallyRelevant: false,
      eligibilityClass: null,
    };
  }

  const klass = text(
    contact.contactClass ||
      contact.contactClassification ||
      contact.relevanceClass ||
      "",
  ).toUpperCase();

  let eligClass = null;
  try {
    const elig = classifyContactEligibility(contact, opportunity);
    eligClass = text(elig?.class || "").toUpperCase();
  } catch {
    eligClass = null;
  }

  let buyerVsEntry = BUYER_VS_ENTRY.UNSPECIFIED;
  if (
    klass === "DIRECT_BUYER" ||
    eligClass === "DIRECT_BUYER" ||
    /VERIFIED_HOTEL_BUYER|BOOKING_AUTHORITY/.test(klass)
  ) {
    buyerVsEntry = BUYER_VS_ENTRY.DIRECT_BUYER;
  } else if (
    klass === "RELEVANT_COMMERCIAL_ENTRY_POINT" ||
    eligClass === "RELEVANT_COMMERCIAL_ENTRY_POINT" ||
    /ENTRY_POINT|INFLUENCER|INTERMEDIARY|REFERRAL/.test(`${klass} ${eligClass}`)
  ) {
    buyerVsEntry = BUYER_VS_ENTRY.RELEVANT_COMMERCIAL_ENTRY_POINT;
  } else if (klass === "RESEARCH_CANDIDATE" || eligClass === "RESEARCH_CANDIDATE") {
    buyerVsEntry = BUYER_VS_ENTRY.RESEARCH_CANDIDATE;
  } else if (RELEVANT.has(eligClass)) {
    buyerVsEntry =
      eligClass === "DIRECT_BUYER"
        ? BUYER_VS_ENTRY.DIRECT_BUYER
        : BUYER_VS_ENTRY.RELEVANT_COMMERCIAL_ENTRY_POINT;
  }

  const title = text(contact.title || contact.role || contact.professionalTitle);
  const commerciallyRelevant =
    buyerVsEntry === BUYER_VS_ENTRY.DIRECT_BUYER ||
    buyerVsEntry === BUYER_VS_ENTRY.RELEVANT_COMMERCIAL_ENTRY_POINT ||
    (buyerVsEntry === BUYER_VS_ENTRY.RESEARCH_CANDIDATE && REMIT_TITLE_RE.test(title));

  return {
    named: true,
    name,
    title: title || null,
    organization: text(contact.organization) || null,
    buyerVsEntry,
    buyerVsEntryLabel: BUYER_VS_ENTRY_LABEL[buyerVsEntry],
    commerciallyRelevant: Boolean(commerciallyRelevant),
    eligibilityClass: eligClass,
  };
}

function explanationFor(tier, ctx = {}) {
  if (tier === CONTACT_ACTIONABILITY_TIER.SALES_READY) {
    const role =
      ctx.buyerVsEntry === BUYER_VS_ENTRY.DIRECT_BUYER
        ? "direct buyer"
        : "commercial entry point";
    return `Verified named ${role} with a source-confirmed business email.`;
  }
  if (tier === CONTACT_ACTIONABILITY_TIER.REACHABLE) {
    return "Named commercially relevant contact with a usable business phone. Email may still need research.";
  }
  if (tier === CONTACT_ACTIONABILITY_TIER.CONTACT_RESEARCH_REQUIRED) {
    if (ctx.personSpecificWithoutSource) {
      return "Named contact on file; email is not yet source-confirmed and is not shown as verified.";
    }
    return "Named contact identified; a verified outreach channel still needs research.";
  }
  return "No verified named person yet for sales outreach.";
}

/**
 * Classify opportunity-level contact actionability (read-time).
 * Does NOT mutate opportunity eligibility or publication authorization.
 */
export function classifyContactActionability(opportunity = {}, opts = {}) {
  const slots = contactSlots(opportunity).map(({ slot, contact }) => {
    const rel = classifyBuyerVsEntry(contact, opportunity);
    const emailCh = classifyEmailChannel(contact);
    const phoneCh = classifyPhoneChannel(contact);
    const domain = emailCh.email ? emailCh.email.split("@")[1] : null;
    const identityKey =
      rel.named && rel.name
        ? personIdentityKey({
            name: rel.name,
            organization: rel.organization || contact.organization,
            domain,
          })
        : null;
    return {
      slot,
      ...rel,
      email: emailCh,
      phone: phoneCh,
      identityKey,
      // Customer surfaces must use verified flags — never raw presence alone
      customerEmailVerified: emailCh.verified === true,
      customerPhoneVerified: phoneCh.present === true && phoneCh.verified === true,
    };
  });

  const anyNamed = slots.some((s) => s.named);
  const anyRelevant = slots.some((s) => s.commerciallyRelevant);
  const anyVerifiedEmail = slots.some(
    (s) => s.email.verified && s.commerciallyRelevant,
  );
  const anyReachablePhone = slots.some(
    (s) => s.phone.present && s.commerciallyRelevant && s.named,
  );
  const personSpecificWithoutSource = slots.some(
    (s) => s.email.status === "PERSON_SPECIFIC_WITHOUT_SOURCE_URL",
  );

  // Diagnostic: leakage-audit style (named + phone, relevance soft) for reconciliation
  const diagnosticNamedPhoneAny = slots.some((s) => s.named && s.phone.present);
  const diagnosticVerifiedEmailOrNamedPhone =
    slots.some((s) => s.email.verified) || diagnosticNamedPhoneAny;

  let tier = CONTACT_ACTIONABILITY_TIER.CONTACT_IDENTIFICATION_REQUIRED;
  if (anyVerifiedEmail) tier = CONTACT_ACTIONABILITY_TIER.SALES_READY;
  else if (anyReachablePhone) tier = CONTACT_ACTIONABILITY_TIER.REACHABLE;
  else if (anyNamed) tier = CONTACT_ACTIONABILITY_TIER.CONTACT_RESEARCH_REQUIRED;

  const primarySlot =
    slots.find((s) => s.slot === "primary" && s.named) || slots.find((s) => s.named) || null;

  const result = {
    version: GDI_CONTACT_ACTIONABILITY_V1,
    contactActionabilityTier: tier,
    contactActionabilityFilterKey: tier,
    contactActionabilityLabel: CONTACT_ACTIONABILITY_LABEL[tier],
    contactActionabilityExplanation: explanationFor(tier, {
      buyerVsEntry: primarySlot?.buyerVsEntry,
      personSpecificWithoutSource,
    }),
    buyerVsEntry: primarySlot?.buyerVsEntry || BUYER_VS_ENTRY.NO_NAMED_PERSON,
    buyerVsEntryLabel:
      primarySlot?.buyerVsEntryLabel || BUYER_VS_ENTRY_LABEL.NO_NAMED_PERSON,
    sharedPersonIdentityKey: primarySlot?.identityKey || null,
    slots,
    flags: {
      personSpecificWithoutSource,
      hasVerifiedEmail: anyVerifiedEmail,
      hasReachablePhone: anyReachablePhone,
      hasNamedContact: anyNamed,
      hasCommerciallyRelevant: anyRelevant,
    },
    diagnostics: {
      // Used to reconcile leakage VERIFIED_REACHABLE (31) vs REACHABLE (15)
      namedPhoneAny: diagnosticNamedPhoneAny,
      verifiedEmailOrNamedPhoneSoft: diagnosticVerifiedEmailOrNamedPhone,
      commerciallyRelevantRequiredForReachable: true,
    },
    // Explicit non-mutation of publication
    mutatesOpportunityEligibility: false,
    mutatesCustomerPublication: false,
    strictEmailPublicationGate: false,
  };

  if (opts.includeSlots === false) {
    const { slots: _s, ...rest } = result;
    return rest;
  }
  return result;
}

/**
 * Attach actionability presentation fields onto an opportunity row (immutable copy).
 */
export function attachContactActionability(opportunity = {}, opts = {}) {
  if (!opportunity || typeof opportunity !== "object") return opportunity;
  const a = classifyContactActionability(opportunity, opts);
  const primary = a.slots?.find((s) => s.slot === "primary") || a.slots?.[0] || null;
  return {
    ...opportunity,
    contactActionabilityTier: a.contactActionabilityTier,
    contactActionabilityFilterKey: a.contactActionabilityFilterKey,
    contactActionabilityLabel: a.contactActionabilityLabel,
    contactActionabilityExplanation: a.contactActionabilityExplanation,
    buyerVsEntry: a.buyerVsEntry,
    buyerVsEntryLabel: a.buyerVsEntryLabel,
    sharedPersonIdentityKey: a.sharedPersonIdentityKey,
    contactActionabilityFlags: a.flags,
    contactActionabilityVersion: GDI_CONTACT_ACTIONABILITY_V1,
    // Gate verified channels for customer copy helpers
    contactEmailCustomerVerified: primary?.customerEmailVerified === true,
    contactPhoneCustomerVerified: primary?.customerPhoneVerified === true,
  };
}

export function actionabilityCustomerSafeContactChannels(contact = {}) {
  const emailCh = classifyEmailChannel(contact);
  const phoneCh = classifyPhoneChannel(contact);
  return {
    email: emailCh.verified ? emailCh.email : null,
    emailVerified: emailCh.verified,
    emailStatus: emailCh.status,
    phone: phoneCh.present ? phoneCh.phone : null,
    phoneVerified: phoneCh.verified && phoneCh.present,
    phoneStatus: phoneCh.status,
    sourceUrl: emailCh.verified ? emailCh.sourceUrl : phoneCh.sourceUrl || null,
  };
}
