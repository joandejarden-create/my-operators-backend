/**
 * Universal Scaling Companies two-contact standard V1.
 *
 * Primary + secondary named people with honest route labeling.
 * Reuses secondary-named-contact-v1 + sales-presentation — no parallel subsystem.
 *
 * Never invents emails/phones. Org switchboard must never be labeled "Person phone".
 */
import {
  attachSecondaryNamedContact,
  looksLikePersonName,
} from "./secondary-named-contact-v1.js";
import {
  customerSafeSecondaryContact,
  isGenericOrganizationEmail,
  resolveCommercialContactPath,
  secondaryContactHasUsableRoute,
} from "./scaling-companies-sales-presentation-v1.js";

export const SCALING_TWO_CONTACT_STANDARD_V1 =
  "gdi-scaling-two-contact-standard-v1";

export const SCALING_CONTACT_COVERAGE = Object.freeze({
  TWO_NAMED_CONTACTABLE: "TWO_NAMED_CONTACTABLE",
  ONE_NAMED_CONTACTABLE: "ONE_NAMED_CONTACTABLE",
  ORGANIZATION_ONLY: "ORGANIZATION_ONLY",
  NO_USABLE_ROUTE: "NO_USABLE_ROUTE",
  COMMERCIALLY_INELIGIBLE: "COMMERCIALLY_INELIGIBLE",
  RESEARCH_INCOMPLETE: "RESEARCH_INCOMPLETE",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

/** Never leak internal eligibility enums into customer Why relevant. */
export function scrubCustomerFacingWhyContact(raw) {
  let s = text(raw);
  if (!s) return "";
  s = s
    .replace(/\bCONTACT_RELEVANT\b\.?\s*/gi, "")
    .replace(/\bINELIGIBLE_UNSPECIFIED\b\.?\s*/gi, "")
    .replace(/\bCONTRACTING_AUTHORITY_VERIFIED\b/gi, "contracting authority")
    .replace(/\s{2,}/g, " ")
    .replace(/^[\s.;:—-]+/, "")
    .trim();
  return s;
}

function digits(phone) {
  return String(phone || "").replace(/\D/g, "");
}

export function extractPrimaryPersonName(opportunity = {}) {
  const candidates = [
    opportunity.primaryContact?.name,
    opportunity.contactName,
    opportunity.primaryContactName,
    opportunity.contactPackage?.person,
  ];
  for (const c of candidates) {
    const n = text(c);
    if (n && n !== "UNKNOWN" && looksLikePersonName(n)) return n;
  }
  return null;
}

/**
 * Normalize primary contact channels so company switchboards are not
 * mislabeled as person-direct phones/emails.
 */
export function normalizeScalingPrimaryContactChannels(opportunity = {}) {
  const o = opportunity && typeof opportunity === "object" ? { ...opportunity } : {};
  const name = extractPrimaryPersonName(o);
  const pc = o.primaryContact && typeof o.primaryContact === "object"
    ? { ...o.primaryContact }
    : {};

  let personEmail = text(
    pc.email || o.contactEmail || o.contactPackage?.contactMethod || ""
  );
  let personPhone = text(pc.phone || o.contactPhone || "");
  let companyPhone = text(o.companyPhone || pc.companyPhone || "");
  let orgEmail = text(
    o.organizationContactEmail ||
      o.functionalContactEmail ||
      pc.organizationContactRoute ||
      ""
  );

  // Org emails never count as person-direct
  if (personEmail && isGenericOrganizationEmail(personEmail)) {
    if (!orgEmail || !orgEmail.includes("@")) orgEmail = personEmail;
    personEmail = "";
  }
  if (orgEmail && !orgEmail.includes("@")) orgEmail = "";

  // Email local-part must plausibly match the named person — otherwise keep as
  // organizational / alternate route (never fabricate a second identity from it).
  if (name && personEmail && personEmail.includes("@")) {
    const local = personEmail.split("@")[0].toLowerCase().replace(/[^a-z]/g, "");
    const nameCompact = name.toLowerCase().replace(/[^a-z]/g, "");
    const tokens = name
      .toLowerCase()
      .split(/\s+/)
      .map((t) => t.replace(/[^a-z]/g, ""))
      .filter((t) => t.length >= 3);
    const matchesPerson =
      (local && nameCompact.includes(local.slice(0, Math.min(5, local.length)))) ||
      tokens.some((t) => local.includes(t) || t.includes(local.slice(0, 4)));
    if (!matchesPerson) {
      if (!orgEmail) orgEmail = personEmail;
      personEmail = "";
    }
  }

  // Phone ownership: explicit org / missing name ⇒ company phone
  const phoneOwnership =
    pc.phoneOwnership ||
    o.primaryContactPhoneOwnership ||
    pc.phoneType ||
    o.primaryContactPhoneType ||
    "";
  const phoneIsOrg =
    !name ||
    /organization|company|switchboard|office/i.test(String(phoneOwnership)) ||
    pc.phoneType === "company" ||
    pc.phoneType === "organization";

  if (personPhone && phoneIsOrg) {
    if (!companyPhone || digits(companyPhone) === digits(personPhone)) {
      companyPhone = personPhone;
    } else if (digits(companyPhone) !== digits(personPhone)) {
      // keep distinct company phone; drop mislabeled person phone
    }
    personPhone = "";
  }

  // If name missing, never keep person phone/email on primaryContact
  if (!name) {
    if (personPhone && !companyPhone) companyPhone = personPhone;
    personPhone = "";
    if (personEmail) {
      if (!orgEmail && isGenericOrganizationEmail(personEmail)) {
        orgEmail = personEmail;
      }
      // Unnamed primary cannot own a "person" email on the customer surface
      personEmail = "";
    }
  }

  const title = text(
    pc.title ||
      pc.role ||
      o.contactTitle ||
      o.contactRole ||
      o.contactResponsibility ||
      ""
  );

  const why =
    text(
      o.whoPrimaryReason ||
        o.contactRelevance ||
        pc.whyThisContact ||
        o.whyThisContact ||
        ""
    ) ||
    (name
      ? `Named contact at ${text(o.displayCompanyName || o.organizationName) || "the company"} — start here to identify who owns lodging and meetings.`
      : null);

  const nextPrimary = name
    ? {
        ...pc,
        name,
        title: title || null,
        role: title || null,
        email: personEmail || null,
        phone: personPhone || null,
        companyPhone: companyPhone || null,
        organizationContactRoute:
          orgEmail ||
          (pc.organizationContactRoute &&
          String(pc.organizationContactRoute).includes("@")
            ? pc.organizationContactRoute
            : pc.organizationContactRoute &&
                /^https?:/i.test(String(pc.organizationContactRoute))
              ? pc.organizationContactRoute
              : null),
        phoneOwnership: personPhone ? "person_direct" : phoneOwnership || null,
        whyThisContact: why,
      }
    : {
        // Org-only shell — drawer must not treat this as a named person
        name: null,
        title: null,
        role: null,
        email: null,
        phone: null,
        companyPhone: companyPhone || null,
        organizationContactRoute:
          orgEmail ||
          (pc.organizationContactRoute &&
          /^https?:/i.test(String(pc.organizationContactRoute))
            ? pc.organizationContactRoute
            : null),
        contactKind: "ORGANIZATION",
        whyThisContact: why,
      };

  // Drop empty-name primaryContact objects that only confuse the drawer
  const hasOrgShell =
    nextPrimary.companyPhone ||
    nextPrimary.organizationContactRoute ||
    orgEmail;

  return {
    ...o,
    contactName: name,
    contactEmail: personEmail || null,
    contactPhone: personPhone || null,
    companyPhone: companyPhone || null,
    companyPhoneLabel: companyPhone
      ? name
        ? `Company office — ask for ${name}`
        : "Company / office phone"
      : o.companyPhoneLabel || null,
    organizationContactEmail: orgEmail || o.organizationContactEmail || null,
    functionalContactEmail: orgEmail || o.functionalContactEmail || null,
    contactRole: name ? title || o.contactRole || null : null,
    contactResponsibility: name
      ? title || o.contactResponsibility || null
      : null,
    contactRelevance: why || o.contactRelevance || null,
    whoPrimaryReason: why || o.whoPrimaryReason || null,
    primaryContact: name || hasOrgShell ? nextPrimary : null,
    contactChannelSeparation: {
      ...(o.contactChannelSeparation || {}),
      suggestedPerson: name || null,
      personSpecificEmail: personEmail || null,
      personSpecificPhone: personPhone || null,
      organizationRoute: orgEmail || null,
      companyPhone: companyPhone || null,
      askToBeDirectedTo: name
        ? `Ask to be directed to ${title || "the relevant travel / meetings owner"} (${name})`
        : "Ask for the person managing staff travel, training, or internal meetings.",
    },
  };
}

function buildPrimarySlot(opportunity = {}) {
  const o = normalizeScalingPrimaryContactChannels(opportunity);
  const name = extractPrimaryPersonName(o);
  const path = resolveCommercialContactPath(o);

  if (!name) {
    const companyPhone = text(o.companyPhone || "");
    const orgEmail = text(
      o.organizationContactEmail || o.functionalContactEmail || ""
    );
    const evidenceUrl = text(
      o.sources?.find?.((s) => /^https?:/i.test(String(s?.url || "")))?.url ||
        o.evidence?.find?.((e) => /^https?:/i.test(String(e?.sourceUrl || "")))
          ?.sourceUrl ||
        "",
    );
    const orgUrl = text(
      o.primaryContact?.organizationContactRoute ||
        o.primaryContact?.officialContactUrl ||
        o.primaryContact?.url ||
        o.contactOfficialUrl ||
        o.contactUrl ||
        evidenceUrl ||
        "",
    );
    const hasRoute = Boolean(companyPhone || orgEmail || /^https?:/i.test(orgUrl));
    return {
      slot: "PRIMARY",
      status: hasRoute ? "ORGANIZATION_ROUTE_ONLY" : "RESEARCH_INCOMPLETE",
      displayLabel: "Primary contact",
      researchGapCopy: hasRoute
        ? "Buyer Not Yet Identified — official organization route available; named commercially relevant person still required."
        : "Buyer Not Yet Identified.",
      name: null,
      title: null,
      organization:
        text(o.displayCompanyName) || text(o.organizationName) || null,
      whyContact: hasRoute
        ? "No verified named buyer yet. Use the official organization / programme page as a research and referral route only."
        : null,
      email: null,
      emailLabel: null,
      phone: null,
      phoneLabel: null,
      organizationEmail: orgEmail || null,
      organizationEmailLabel: orgEmail ? "Organization email" : null,
      companyPhone: companyPhone || null,
      companyPhoneLabel: companyPhone ? "Organization phone" : null,
      profileUrl: null,
      linkedInUrl: null,
      officialContactUrl:
        text(o.contactOfficialUrl || o.contactUrl || orgUrl) || null,
      commercialContactPath: hasRoute
        ? "ORGANIZATION_ROUTE"
        : path.commercialContactPath,
      commercialContactPathLabel: hasRoute
        ? "Organization contact path"
        : path.commercialContactPathLabel,
      customerFacingNamed: false,
      contactable: hasRoute,
      surfeEnrichmentEligible: false,
      surfeEnrichmentChannel: null,
    };
  }

  const email = text(
    o.primaryContact?.workEmail ||
      o.primaryContact?.email ||
      o.contactEmail ||
      "",
  );
  const phone = text(
    o.primaryContact?.directPhone ||
      o.primaryContact?.phone ||
      o.contactPhone ||
      "",
  );
  const companyPhone = text(
    o.primaryContact?.organizationPhone || o.companyPhone || "",
  );
  const orgEmail = text(
    o.primaryContact?.organizationEmail ||
      o.organizationContactEmail ||
      o.functionalContactEmail ||
      "",
  );
  const title = text(
    o.primaryContact?.title ||
      o.primaryContact?.role ||
      o.contactRole ||
      o.contactResponsibility ||
      "",
  );
  const profileUrl = text(
    o.primaryContact?.profileUrl ||
      o.primaryContact?.linkedinUrl ||
      o.contactPackage?.sourceUrl ||
      "",
  );
  const linkedInUrl = /linkedin\.com/i.test(profileUrl) ? profileUrl : text(o.primaryContact?.linkedinUrl || "");
  const orgDisplay =
    text(o.primaryContact?.organization) ||
    text(o.displayCompanyName) ||
    text(o.organizationName) ||
    null;
  const whyRaw =
    text(o.primaryContact?.whyRelevant) ||
    text(o.primaryContact?.whyRelevantToHotelDemand) ||
    text(o.whoPrimaryReason) ||
    text(o.primaryContact?.whyThisContact) ||
    text(o.contactRelevance) ||
    "";
  const isEventsSeg =
    opportunity.researchSegment === "associations_government" ||
    /association\s*\/\s*conference\s*\/\s*events/i.test(
      String(opportunity.customerSegmentUmbrella || ""),
    );
  const why =
    whyRaw && !/Best available public contact for lodging/i.test(whyRaw)
      ? whyRaw
      : isEventsSeg
        ? title
          ? `${title}${orgDisplay ? ` at ${orgDisplay}` : ""} — approach to confirm event organizing, housing, or hotel-sourcing authority; not assumed buyer solely from name.`
          : `Named contact for ${text(opportunity.eventName || opportunity.title) || "this event"}; confirm commercial relevance before outreach.`
        : title
          ? `${title} at ${orgDisplay || "the company"} — approach to identify who owns visiting-staff lodging and local meetings; hotel-sourcing authority unverified.`
          : `Named contact at ${orgDisplay || "the company"} for account introduction; hotel-sourcing authority unverified.`;

  const contactable = Boolean(
    email || phone || companyPhone || orgEmail
  );
  const missingEmail = !email;
  const missingPhone = !phone;
  let surfeChannel = null;
  if (missingEmail && missingPhone) surfeChannel = "both";
  else if (missingEmail) surfeChannel = "email";
  else if (missingPhone) surfeChannel = "phone";

  return {
    slot: "PRIMARY",
    status: contactable ? "NAMED_CONTACTABLE" : "NAMED_IDENTITY_ONLY",
    displayLabel: "Primary contact",
    researchGapCopy: contactable
      ? null
      : "Named person identified — personal email/phone still being researched.",
    name,
    title: title || null,
    organization: orgDisplay,
    whyContact: scrubCustomerFacingWhyContact(why) || why,
    email: email || null,
    emailLabel: email ? "Direct work email" : null,
    phone: phone || null,
    phoneLabel: phone ? "Direct phone" : null,
    organizationEmail: orgEmail || null,
    organizationEmailLabel: orgEmail
      ? `Organization email — ask for ${name}`
      : null,
    companyPhone: companyPhone || null,
    companyPhoneLabel: companyPhone
      ? `Organization phone — ask for ${name}`
      : null,
    profileUrl: /^https?:/i.test(profileUrl) ? profileUrl : null,
    linkedInUrl: /^https?:/i.test(linkedInUrl) ? linkedInUrl : null,
    officialContactUrl:
      text(
        o.primaryContact?.officialContactUrl ||
          o.contactOfficialUrl ||
          o.contactUrl,
      ) || null,
    commercialContactPath: path.commercialContactPath,
    commercialContactPathLabel:
      text(o.primaryContact?.commercialContactPathLabel) ||
      path.commercialContactPathLabel,
    contactClassification:
      o.primaryContact?.contactClassification ||
      o.primaryContact?.relevanceClass ||
      null,
    customerFacingNamed: true,
    contactable,
    surfeEnrichmentEligible: Boolean(surfeChannel),
    surfeEnrichmentChannel: surfeChannel,
  };
}

function buildSecondarySlot(opportunity = {}) {
  const raw = opportunity.secondaryContact || null;
  const safe = customerSafeSecondaryContact(raw);
  if (!safe || !text(safe.name)) {
    const primaryNamed = Boolean(extractPrimaryPersonName(opportunity));
    return {
      slot: "SECONDARY",
      status: "RESEARCH_INCOMPLETE",
      displayLabel: "Alternative / referral contact",
      // Do not repeat an empty "Buyer Not Yet Identified" card when primary is also missing.
      researchGapCopy: primaryNamed
        ? "Secondary contact not yet identified."
        : "No secondary contact — primary buyer research still incomplete.",
      name: null,
      title: null,
      organization:
        text(opportunity.displayCompanyName) ||
        text(opportunity.organizationName) ||
        null,
      whyContact: null,
      email: null,
      phone: null,
      organizationEmail: null,
      companyPhone: null,
      profileUrl: null,
      linkedInUrl: null,
      officialContactUrl: null,
      customerFacingNamed: false,
      contactable: false,
      surfeEnrichmentEligible: false,
      surfeEnrichmentChannel: null,
    };
  }

  const email = text(safe.workEmail || safe.email || "");
  const phone = text(safe.directPhone || safe.phone || "");
  const companyPhone = text(safe.organizationPhone || safe.companyPhone || "");
  const orgRoute = text(safe.organizationRoutingAlternative || "");
  const orgIsEmail = orgRoute.includes("@");
  const profileUrl = text(safe.profileUrl) || text(safe.linkedinUrl) || text(safe.sourceUrl) || "";
  const linkedInUrl = /linkedin\.com/i.test(profileUrl)
    ? profileUrl
    : text(safe.linkedinUrl || "");
  const orgDisplay =
    text(safe.organization) ||
    text(opportunity.displayCompanyName) ||
    text(opportunity.organizationName) ||
    null;
  const whySec =
    text(safe.whyRelevant) ||
    text(safe.whyRelevantToHotelDemand) ||
    text(safe.whyThisContact) ||
    text(safe.relationshipToOpportunity) ||
    text(safe.contactClassificationLabel) ||
    "Alternative path to the relevant buyer or operations owner; hotel-sourcing authority unverified.";
  const missingEmail = !email;
  const missingPhone = !phone;
  let surfeChannel = null;
  if (missingEmail && missingPhone) surfeChannel = "both";
  else if (missingEmail) surfeChannel = "email";
  else if (missingPhone) surfeChannel = "phone";

  return {
    slot: "SECONDARY",
    status: "NAMED_CONTACTABLE",
    displayLabel: "Alternative / referral contact",
    researchGapCopy: null,
    name: safe.name,
    title: text(safe.title) || null,
    organization: orgDisplay,
    whyContact: scrubCustomerFacingWhyContact(whySec) || whySec,
    email: email || null,
    emailLabel: email ? "Direct work email" : null,
    phone: phone || null,
    phoneLabel: phone ? "Direct phone" : null,
    organizationEmail: orgIsEmail
      ? orgRoute
      : text(safe.organizationEmail) || null,
    organizationEmailLabel: orgIsEmail
      ? safe.organizationRoutingLabel ||
        `Organization email — ask for ${safe.name}`
      : text(safe.organizationEmail)
        ? `Organization email — ask for ${safe.name}`
        : null,
    companyPhone:
      companyPhone || (!orgIsEmail && orgRoute ? orgRoute : null) || null,
    companyPhoneLabel:
      companyPhone || (!orgIsEmail && orgRoute)
        ? safe.organizationRoutingLabel ||
          `Organization phone — ask for ${safe.name}`
        : null,
    profileUrl: /^https?:/i.test(profileUrl) ? profileUrl : null,
    linkedInUrl: /^https?:/i.test(linkedInUrl) ? linkedInUrl : null,
    officialContactUrl: text(safe.officialContactUrl) || null,
    contactClassification: safe.contactClassification || null,
    contactClassificationLabel: safe.contactClassificationLabel || null,
    customerFacingNamed: true,
    contactable: secondaryContactHasUsableRoute(safe),
    surfeEnrichmentEligible: Boolean(surfeChannel),
    surfeEnrichmentChannel: surfeChannel,
  };
}

export function classifyScalingContactCoverage(opportunity = {}, opts = {}) {
  if (
    opts.commerciallyIneligible === true ||
    opportunity.scalingCommercialDisposition === "COMMERCIALLY_INELIGIBLE" ||
    opportunity.scalingCommercialDisposition === "HOLD_NOT_HOTEL_BUYER"
  ) {
    return SCALING_CONTACT_COVERAGE.COMMERCIALLY_INELIGIBLE;
  }
  const presentation = buildScalingTwoContactPresentation(opportunity);
  const p = presentation.primary;
  const s = presentation.secondary;
  if (p.customerFacingNamed && p.contactable && s.customerFacingNamed && s.contactable) {
    return SCALING_CONTACT_COVERAGE.TWO_NAMED_CONTACTABLE;
  }
  if (p.customerFacingNamed && p.contactable) {
    return SCALING_CONTACT_COVERAGE.ONE_NAMED_CONTACTABLE;
  }
  if (s.customerFacingNamed && s.contactable && !p.customerFacingNamed) {
    return SCALING_CONTACT_COVERAGE.ONE_NAMED_CONTACTABLE;
  }
  if (p.contactable || s.contactable) {
    return SCALING_CONTACT_COVERAGE.ORGANIZATION_ONLY;
  }
  if (p.status === "RESEARCH_INCOMPLETE" || s.status === "RESEARCH_INCOMPLETE") {
    return SCALING_CONTACT_COVERAGE.RESEARCH_INCOMPLETE;
  }
  return SCALING_CONTACT_COVERAGE.NO_USABLE_ROUTE;
}

/**
 * Customer-facing two-contact presentation object (drawer + share).
 */
export function buildScalingTwoContactPresentation(opportunity = {}) {
  const normalized = normalizeScalingPrimaryContactChannels(opportunity);
  const primary = buildPrimarySlot(normalized);
  const secondary = buildSecondarySlot(normalized);
  const coverage = (() => {
    if (primary.customerFacingNamed && primary.contactable && secondary.customerFacingNamed && secondary.contactable) {
      return SCALING_CONTACT_COVERAGE.TWO_NAMED_CONTACTABLE;
    }
    if (
      (primary.customerFacingNamed && primary.contactable) ||
      (secondary.customerFacingNamed && secondary.contactable)
    ) {
      return SCALING_CONTACT_COVERAGE.ONE_NAMED_CONTACTABLE;
    }
    if (primary.contactable || secondary.contactable) {
      return SCALING_CONTACT_COVERAGE.ORGANIZATION_ONLY;
    }
    return SCALING_CONTACT_COVERAGE.RESEARCH_INCOMPLETE;
  })();

  return {
    version: SCALING_TWO_CONTACT_STANDARD_V1,
    coverage,
    primary,
    secondary,
    followUpTasks: [
      !primary.customerFacingNamed
        ? {
            code: "RESEARCH_PRIMARY_NAMED_CONTACT",
            note: "Identify a named primary buyer / influencer with commercial relevance.",
          }
        : null,
      primary.customerFacingNamed && !primary.contactable
        ? {
            code: "VERIFY_PRIMARY_CONTACT_ROUTE",
            note: "Primary named but lacks verified email/phone/org routing.",
          }
        : null,
      !secondary.customerFacingNamed
        ? {
            code: "RESEARCH_SECONDARY_NAMED_CONTACT",
            note: "Identify a distinct secondary referral / influencer with a usable route.",
          }
        : null,
    ].filter(Boolean),
  };
}

/**
 * When research found a named contactable person but parked them only in
 * secondaryContact (primary left as org-route), promote that person to primary.
 * Clears secondary so a distinct referral can be attached next.
 * Never invents names/emails — only relocates verified secondary fields.
 */
export function promoteOrphanNamedSecondaryToPrimary(opportunity = {}) {
  const o = opportunity && typeof opportunity === "object" ? { ...opportunity } : {};
  if (extractPrimaryPersonName(o)) return o;

  const sec = o.secondaryContact && typeof o.secondaryContact === "object"
    ? o.secondaryContact
    : null;
  const secName = text(sec?.name);
  if (!secName || !looksLikePersonName(secName)) return o;

  const pc =
    o.primaryContact && typeof o.primaryContact === "object"
      ? { ...o.primaryContact }
      : {};
  const personEmail = text(sec.email) || null;
  const personPhone = text(sec.phone) || null;
  const companyPhone =
    text(sec.companyPhone) || text(pc.companyPhone) || text(o.companyPhone) || null;
  const orgRoute =
    text(sec.organizationRoutingAlternative) ||
    text(pc.organizationContactRoute) ||
    text(o.organizationContactEmail) ||
    text(o.functionalContactEmail) ||
    null;

  o.contactName = secName;
  o.contactRole = text(sec.title) || o.contactRole || null;
  o.primaryContactName = secName;
  if (personEmail) {
    o.contactEmail = personEmail;
  }
  if (personPhone) {
    o.contactPhone = personPhone;
  }
  o.primaryContact = {
    ...pc,
    name: secName,
    title: text(sec.title) || pc.title || null,
    role: text(sec.title) || pc.role || null,
    email: personEmail,
    phone: personPhone,
    companyPhone,
    organizationContactRoute: orgRoute,
    whyThisContact:
      text(sec.relationshipToOpportunity) ||
      text(sec.relevantResponsibility) ||
      pc.whyThisContact ||
      null,
    contactKind: personEmail || personPhone ? "PERSON" : "NAMED_WITH_ORG_ROUTE",
    profileUrl: text(sec.profileUrl) || pc.profileUrl || null,
  };
  if (companyPhone && !o.companyPhone) o.companyPhone = companyPhone;
  if (orgRoute && orgRoute.includes("@") && !o.organizationContactEmail) {
    o.organizationContactEmail = orgRoute;
  }
  // Preserve evidence of prior secondary placement for audit trail
  o.promotedNamedContactFromSecondary = {
    name: secName,
    title: text(sec.title) || null,
    promotedAt: new Date().toISOString().slice(0, 10),
    reason: "ORPHAN_SECONDARY_PROMOTED_TO_PRIMARY",
  };
  delete o.secondaryContact;
  return o;
}

/**
 * Apply two-contact normalization onto a Scaling opportunity (canonical prep).
 * Does not invent contacts. Promotes orphan named secondary → primary, then
 * re-attaches a distinct secondary when candidates exist.
 */
export function applyScalingTwoContactStandard(opportunity = {}, opts = {}) {
  const o = opportunity && typeof opportunity === "object" ? opportunity : {};
  const isScaling =
    o.researchSegment === "scaling_companies" ||
    /scaling compan/i.test(String(o.segment || ""));
  // Events Association/Conference drawers reuse the same two-contact presentation
  // shell (unified Who to Contact). force=true remains for non-commercial fixtures.
  const isEventsAssociation =
    o.researchSegment === "associations_government" ||
    /association\s*\/\s*conference\s*\/\s*events/i.test(
      String(o.customerSegmentUmbrella || o.segment || ""),
    );
  if (!isScaling && !isEventsAssociation && opts.force !== true) return o;

  let next = promoteOrphanNamedSecondaryToPrimary(o);
  // After promotion (or when primary already named), try a distinct secondary
  if (!next.secondaryContact?.name && opts.skipSecondaryAttach !== true) {
    next = attachSecondaryNamedContact(next, {
      force: true,
      allowSavedEvidence: opts.allowSavedEvidence !== false,
      candidates: opts.candidates,
    });
  }

  const normalized = normalizeScalingPrimaryContactChannels(next);
  const presentation = buildScalingTwoContactPresentation(normalized);
  const path = resolveCommercialContactPath(normalized);

  return {
    ...normalized,
    commercialContactPath: path.commercialContactPath,
    commercialContactPathLabel: path.commercialContactPathLabel,
    scalingTwoContactPresentation: presentation,
    scalingContactCoverage: presentation.coverage,
    scalingTwoContactVersion: SCALING_TWO_CONTACT_STANDARD_V1,
    scalingContactFollowUps: presentation.followUpTasks,
  };
}
