/**
 * Secondary Named Contact Intelligence V1
 *
 * Adds one alternative / referral named person beside the primary org or named
 * contact — without inventing emails/phones or lowering publication gates.
 *
 * Customer-facing shape (optional fields only):
 *   secondaryContact: {
 *     name, title, organization, relevantResponsibility,
 *     relationshipToOpportunity, contactClassification,
 *     email, phone, organizationRoutingAlternative,
 *     profileUrl, evidenceUrls, verificationDate, confidence
 *   }
 */

export const SECONDARY_NAMED_CONTACT_V1 = "gdi-secondary-named-contact-v1";
/** Universal enrichment contract — same module, all hotels / all 11 segments. */
export const UNIVERSAL_SECONDARY_CONTACT_V1 =
  "gdi-secondary-contact-universal-backfill-v1";

export const SECONDARY_CONTACT_CLASS = Object.freeze({
  VERIFIED_HOTEL_BUYER: "VERIFIED_HOTEL_BUYER",
  POTENTIAL_BUYER: "POTENTIAL_BUYER",
  INFLUENCER: "INFLUENCER",
  REFERRAL_CONTACT: "REFERRAL_CONTACT",
  LOCAL_OPERATIONS: "LOCAL_OPERATIONS",
});

/** Titles that mark a backup/contact as a desk/entity, not a person. */
const FUNCTIONAL_ENTITY_TITLE_RE =
  /\b(functional entity|organization|org route|housing desk|registration desk|general inbox)\b/i;

const ORG_AS_NAME_RE =
  /\b(association|championships?|conference|university|college|inc\.?|llc|corp\.?|group|campus|quantum|incentive|northstar|allstar|varsity|movements|management|managers?|directors?|officers?|associates?|specialists?|coordinators?|meetings?|content|bureau|foundation|society|institute|summit|open|series|source|desk|services?|service|housing|registration|committee|department|team|staff|office)\b/i;

export const SECONDARY_CONTACT_CLASS_LABEL = Object.freeze({
  [SECONDARY_CONTACT_CLASS.VERIFIED_HOTEL_BUYER]: "Verified hotel / meeting buyer",
  [SECONDARY_CONTACT_CLASS.POTENTIAL_BUYER]: "Potential buyer",
  [SECONDARY_CONTACT_CLASS.INFLUENCER]: "Influencer",
  [SECONDARY_CONTACT_CLASS.REFERRAL_CONTACT]: "Referral contact",
  [SECONDARY_CONTACT_CLASS.LOCAL_OPERATIONS]: "Local operations / workplace contact",
});

const SENIOR_EXEC_RE =
  /\b(ceo|chief executive|founder|founding|managing partner|chairman|co-?founder|president)\b/i;

const BUYER_ROLE_RE =
  /\b(travel|meeting|meetings|event|events|procurement|sourcing|hotel|lodging|housing|corporate travel|meeting planner)\b/i;

const OPS_ROLE_RE =
  /\b(general manager|property manager|tenant experience|workplace|operations|facilities|office manager|asset manager|regional)\b/i;

const PROJECT_ROLE_RE =
  /\b(project manager|program manager|construction|development|leasing)\b/i;

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function nameKey(name) {
  return text(name)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function isSafeHttpUrl(url) {
  try {
    const u = new URL(String(url || ""));
    return u.protocol === "http:" || u.protocol === "https:";
  } catch {
    return false;
  }
}

function looksGenericOrgEmail(email) {
  const e = text(email).toLowerCase();
  if (!e.includes("@")) return false;
  const local = e.split("@")[0] || "";
  return /^(info|contact|hello|office|admin|support|sales|inquiries|enquiry|reception|mail|team|hr|press|media|customerservice|bocaraton|atlanta|miami|dallas|jacksonville|denver|hq)$/i.test(
    local,
  );
}

/**
 * True when the string looks like a real person's name (not an org / desk label).
 */
export function looksLikePersonName(name) {
  const n = text(name);
  if (!n || n === "UNKNOWN") return false;
  if (ORG_AS_NAME_RE.test(n)) return false;
  if (
    /^(the|team|office|events|housing|contact|skip|home|about|menu|search|login|privacy|cookie|follow|share|read|learn|view|click|submit|next|previous)\b/i.test(
      n,
    )
  ) {
    return false;
  }
  // UI chrome / nav fragments
  if (/\b(us|here|more|now|page|site|content|sidebar|navigation)\b/i.test(n)) {
    return false;
  }
  // Prefer First Last (optionally middle / suffix). Allow Unicode letters (e.g. Rongé).
  // Allow lowercase surname particles (de / van / von / der / den / la / le / du).
  const particle = String.raw`(?:de|van|von|der|den|la|le|du|da|di|del|della|st\.?)`;
  const titleCasePerson =
    new RegExp(
      String.raw`^[\p{Lu}][\p{L}'.-]+(?:\s+(?:${particle}|[\p{Lu}][\p{L}'.-]+)){1,4}$`,
      "u",
    ).test(n) && /[\p{Lu}][\p{L}'.-]+$/u.test(n);
  const allCapsPerson =
    /^[\p{Lu}][\p{Lu}'.-]+(?:\s+[\p{Lu}][\p{Lu}'.-]+){1,3}$/u.test(n);
  if (!titleCasePerson && !allCapsPerson) return false;
  // Reject 2-token titles that are clearly not people (e.g. "Contact Us")
  const tokens = n.split(/\s+/);
  if (tokens.length === 2 && tokens.every((t) => t.length <= 3)) return false;
  // Reject plural job-title phrases mistaken for people (e.g. "Meetings Content Managers")
  if (
    /\b(managers|directors|officers|associates|specialists|coordinators|analysts|leads)\b/i.test(
      n,
    )
  ) {
    return false;
  }
  if (/\b(meetings|content|housing|registration|procurement)\b/i.test(n)) {
    return false;
  }
  // UI / product chrome mistaken for people (e.g. "Permissions Learner", "Compliance Labor")
  if (
    /\b(permissions|learner|compliance|labor|labour|settings|dashboard|workspace)\b/i.test(
      n,
    )
  ) {
    return false;
  }
  return true;
}

function isFunctionalEntityCandidate(person = {}) {
  const title = text(person.title || person.role || person.relevantResponsibility);
  const name = text(person.name || person.fullName || person.personName);
  if (FUNCTIONAL_ENTITY_TITLE_RE.test(title)) return true;
  if (person.contactKind === "FUNCTIONAL" || person.contactKind === "FUNCTIONAL_BACKUP") {
    return true;
  }
  if (person.functionalEntity === true) return true;
  if (!looksLikePersonName(name)) return true;
  return false;
}

/**
 * Classify a candidate person for secondary display.
 * Prefer buyers/ops/referral over senior executives.
 */
export function classifySecondaryNamedContact(person = {}, opportunity = {}) {
  const title = text(person.title || person.role || person.relevantResponsibility);
  const why = text(
    person.whyRelevant ||
      person.relationshipToOpportunity ||
      person.relevanceEvidence ||
      "",
  );
  const blob = `${title} ${why}`;
  const demandBlob = `${text(opportunity.proposedHotelUse)} ${text(opportunity.demandMechanism)} ${text(opportunity.segment)}`;

  if (person.verifiedHotelBuyer === true || /verified\s+hotel\s+buyer/i.test(why)) {
    return SECONDARY_CONTACT_CLASS.VERIFIED_HOTEL_BUYER;
  }
  if (
    BUYER_ROLE_RE.test(blob) ||
    (BUYER_ROLE_RE.test(demandBlob) &&
      /travel|meeting|event|procurement|sourcing|housing|lodging/i.test(title))
  ) {
    return SECONDARY_CONTACT_CLASS.POTENTIAL_BUYER;
  }
  if (OPS_ROLE_RE.test(blob)) {
    return SECONDARY_CONTACT_CLASS.LOCAL_OPERATIONS;
  }
  if (PROJECT_ROLE_RE.test(blob)) {
    return SECONDARY_CONTACT_CLASS.INFLUENCER;
  }
  if (SENIOR_EXEC_RE.test(title) && !BUYER_ROLE_RE.test(blob)) {
    // Senior exec without buyer/ops signal — weak; treat as referral only if explicitly marked
    return person.allowSeniorAsReferral === true
      ? SECONDARY_CONTACT_CLASS.REFERRAL_CONTACT
      : null;
  }
  if (text(person.name) && text(title)) {
    return SECONDARY_CONTACT_CLASS.REFERRAL_CONTACT;
  }
  return null;
}

function relevanceRank(classification) {
  switch (classification) {
    case SECONDARY_CONTACT_CLASS.VERIFIED_HOTEL_BUYER:
      return 100;
    case SECONDARY_CONTACT_CLASS.POTENTIAL_BUYER:
      return 85;
    case SECONDARY_CONTACT_CLASS.LOCAL_OPERATIONS:
      return 75;
    case SECONDARY_CONTACT_CLASS.INFLUENCER:
      return 60;
    case SECONDARY_CONTACT_CLASS.REFERRAL_CONTACT:
      return 45;
    default:
      return 0;
  }
}

/**
 * Normalize one customer-safe secondary contact object.
 * Never invents email/phone — only passes through verified fields.
 */
export function normalizeSecondaryNamedContact(person = {}, opportunity = {}, opts = {}) {
  const name = text(person.name || person.fullName || person.personName);
  if (!name || name === "UNKNOWN") return null;
  if (!looksLikePersonName(name)) return null;

  const title = text(person.title || person.role || person.currentTitle);
  if (!title) return null;

  // Classify from explicit evidence only — never from default "ask for travel/meetings" copy
  const explicitWhy = text(
    person.relationshipToOpportunity ||
      person.whyRelevant ||
      person.relevanceEvidence ||
      person.relevantResponsibility ||
      "",
  );
  const classification =
    person.contactClassification ||
    classifySecondaryNamedContact(
      { ...person, title, whyRelevant: explicitWhy || title },
      opportunity,
    );
  if (!classification) return null;

  const emailRaw = text(person.email || person.directEmail || person.businessEmail);
  const email =
    emailRaw && emailRaw.includes("@") && !looksGenericOrgEmail(emailRaw)
      ? emailRaw
      : null;

  const phoneRaw = text(person.phone || person.directPhone || person.businessPhone);
  // Do not treat company switchboard as person direct unless explicitly marked person_direct
  const phone =
    phoneRaw &&
    person.phoneOwnership !== "organization_switchboard" &&
    person.phoneType !== "company" &&
    person.phoneType !== "organization"
      ? phoneRaw
      : null;

  const orgRoute = text(
    person.organizationRoutingAlternative ||
      person.organizationRoute ||
      opportunity.functionalContactEmail ||
      opportunity.organizationContactEmail ||
      (looksGenericOrgEmail(opportunity.contactEmail) ? opportunity.contactEmail : "") ||
      "",
  );

  const companyPhone = text(
    person.companyPhone ||
      opportunity.companyPhone ||
      opportunity.contactPhone ||
      "",
  );

  const profileUrl = isSafeHttpUrl(person.profileUrl || person.linkedinUrl || person.linkedin)
    ? text(person.profileUrl || person.linkedinUrl || person.linkedin)
    : null;

  const evidenceUrls = [
    ...(Array.isArray(person.evidenceUrls) ? person.evidenceUrls : []),
    person.sourceUrl,
    person.evidenceUrl,
  ]
    .map((u) => text(u))
    .filter((u) => isSafeHttpUrl(u));

  const why =
    explicitWhy ||
    `Publicly listed ${title} at ${text(opportunity.organizationName) || "the organization"} — ask for referral to travel / meetings / workplace operations as appropriate.`;

  const orgRoutingAlternative = orgRoute.includes("@")
    ? orgRoute
    : companyPhone || null;
  // LinkedIn/profile alone is identity evidence — customer display still requires
  // email, phone, or organization routing (enforced at presentation sanitize).
  const hasUsableRoute = Boolean(
    email || phone || orgRoutingAlternative,
  );

  return {
    name,
    title,
    organization: text(person.organization || opportunity.organizationName || opportunity.company),
    relevantResponsibility: text(person.relevantResponsibility || title),
    relationshipToOpportunity: why,
    contactClassification: classification,
    contactClassificationLabel:
      SECONDARY_CONTACT_CLASS_LABEL[classification] || classification,
    email: email || null,
    phone: phone || null,
    organizationRoutingAlternative: orgRoutingAlternative,
    organizationRoutingLabel: orgRoute.includes("@")
      ? `Company email — ask for ${name}`
      : companyPhone
        ? `Company office — ask for ${name}`
        : null,
    companyPhone: companyPhone || null,
    profileUrl,
    evidenceUrls: [...new Set(evidenceUrls)].slice(0, 5),
    verificationDate:
      text(person.verificationDate || person.verifiedAt || opts.nowDate) ||
      new Date().toISOString().slice(0, 10),
    confidence: person.confidence != null ? Number(person.confidence) : null,
    hasUsableContactRoute: hasUsableRoute,
    secondaryNamedContactVersion: SECONDARY_NAMED_CONTACT_V1,
  };
}

function primaryRawName(opportunity = {}) {
  return text(
    opportunity.primaryContact?.name ||
      opportunity.contactName ||
      opportunity.primaryContactName ||
      "",
  );
}

function primaryPersonName(opportunity = {}) {
  const raw = primaryRawName(opportunity);
  return looksLikePersonName(raw) ? nameKey(raw) : "";
}

function primaryIsGeneric(opportunity = {}) {
  const named = primaryPersonName(opportunity);
  if (named) return false;
  const email = text(
    opportunity.primaryContact?.email ||
      opportunity.contactEmail ||
      opportunity.functionalContactEmail ||
      opportunity.organizationContactEmail ||
      "",
  );
  if (email && looksGenericOrgEmail(email)) return true;
  if (!named && (opportunity.contactUrl || opportunity.companyPhone || opportunity.contactPhone)) {
    return true;
  }
  // Org-as-name or empty primary counts as generic for secondary enrichment eligibility
  return !named;
}

function harvestCandidates(opportunity = {}, opts = {}) {
  const fromBuyerIntel = [];
  const bi = opportunity.buyerIntelligence || opportunity.buyerIntel || null;
  if (Array.isArray(bi)) {
    for (const row of bi) {
      if (row?.name || row?.personName) fromBuyerIntel.push(row);
      if (Array.isArray(row?.people)) fromBuyerIntel.push(...row.people);
      if (Array.isArray(row?.contacts)) fromBuyerIntel.push(...row.contacts);
    }
  } else if (bi && typeof bi === "object") {
    if (Array.isArray(bi.people)) fromBuyerIntel.push(...bi.people);
    if (Array.isArray(bi.contacts)) fromBuyerIntel.push(...bi.contacts);
    if (Array.isArray(bi.namedStakeholders)) {
      fromBuyerIntel.push(...bi.namedStakeholders);
    }
  }
  const raw = [
    ...(Array.isArray(opts.candidates) ? opts.candidates : []),
    ...(Array.isArray(opportunity.namedStakeholders) ? opportunity.namedStakeholders : []),
    ...(Array.isArray(opportunity.backupContacts) ? opportunity.backupContacts : []),
    ...(Array.isArray(opportunity.contacts) ? opportunity.contacts : []),
    ...(Array.isArray(opportunity.whoContacts) ? opportunity.whoContacts : []),
    ...fromBuyerIntel,
  ];
  return raw.filter((p) => p && !isFunctionalEntityCandidate(p));
}

/**
 * Pick the strongest distinct secondary named contact from candidates.
 */
export function selectSecondaryNamedContact(opportunity = {}, candidates = [], opts = {}) {
  const primary = primaryPersonName(opportunity);
  const ranked = [];

  for (const raw of candidates || []) {
    const normalized = normalizeSecondaryNamedContact(raw, opportunity, opts);
    if (!normalized) continue;
    if (primary && nameKey(normalized.name) === primary) continue; // never duplicate
    if (SENIOR_EXEC_RE.test(normalized.title) && !BUYER_ROLE_RE.test(normalized.title)) {
      // Skip pure senior execs unless classification already allowed as referral
      if (normalized.contactClassification !== SECONDARY_CONTACT_CLASS.REFERRAL_CONTACT) {
        continue;
      }
      // Still deprioritize
    }
    ranked.push({
      ...normalized,
      _rank: relevanceRank(normalized.contactClassification) + (normalized.email ? 5 : 0),
    });
  }

  ranked.sort((a, b) => b._rank - a._rank);
  if (!ranked.length) return null;
  const best = ranked[0];
  delete best._rank;
  delete best.confidence; // do not force confidence into customer UI by default
  return best;
}

/**
 * Evidence-backed CP Group (Waterstone) secondary contact example from public About page.
 * Not a verified hotel buyer — local operations / tenant-experience referral.
 * Direct email unknown; use Boca org route for routing.
 */
export const CP_GROUP_SECONDARY_CONTACT_EXAMPLE = Object.freeze({
  name: "Danielle Schwigk",
  title: "Tenant Experience Manager",
  organization: "CP Group",
  relevantResponsibility: "Tenant experience / workplace operations",
  relationshipToOpportunity:
    "Publicly listed Tenant Experience Manager on CP Group's About/Team page — appropriate referral for Boca tenant and visitor experience questions tied to BRiC / Boca portfolio activity. Not a verified hotel buyer.",
  contactClassification: SECONDARY_CONTACT_CLASS.LOCAL_OPERATIONS,
  email: null,
  phone: null,
  organizationRoutingAlternative: "bocaraton@cpgcre.com",
  companyPhone: "561.447.1800",
  profileUrl: null,
  evidenceUrls: ["https://cpgcre.com/about/"],
  verificationDate: "2026-10-26",
  sourceUrl: "https://cpgcre.com/about/",
  allowSeniorAsReferral: false,
});

/**
 * Saved / free-public evidence seeds (not paid enrichment).
 * Used only when live candidates are empty and primary is generic org routing.
 */
const SAVED_SECONDARY_EVIDENCE_BY_ORG = Object.freeze({
  "cp group": () => ({ ...CP_GROUP_SECONDARY_CONTACT_EXAMPLE }),
  cpgcre: () => ({ ...CP_GROUP_SECONDARY_CONTACT_EXAMPLE }),
});

function savedEvidenceCandidates(opportunity = {}) {
  const key = nameKey(opportunity.organizationName || opportunity.company || "");
  const factory = SAVED_SECONDARY_EVIDENCE_BY_ORG[key];
  if (!factory) return [];
  return [factory()];
}

/**
 * Attach secondaryContact onto opportunity without overwriting primary.
 * Sources: explicit secondaryContact, namedStakeholders, backupContacts,
 * optional saved public-evidence seeds (e.g. CP Group About page).
 * Never blocks publication when no secondary is found.
 * Segment-agnostic — safe for all 11 GDI demand segments.
 */
export function attachSecondaryNamedContact(opportunity = {}, opts = {}) {
  const o = opportunity && typeof opportunity === "object" ? opportunity : {};
  if (o.secondaryContact && o.secondaryContact.name && opts.force !== true) {
    const existing = normalizeSecondaryNamedContact(o.secondaryContact, o, opts);
    if (existing) {
      const { confidence: _c, ...customerSafe } = existing;
      return { ...o, secondaryContact: customerSafe };
    }
  }

  const candidates = harvestCandidates(o, opts);
  let selected = selectSecondaryNamedContact(o, candidates, opts);

  // Fallback: saved public evidence when primary is generic OR secondary still missing
  // (named primary does not block a distinct saved secondary when evidence exists).
  if (!selected && opts.allowSavedEvidence !== false) {
    selected = selectSecondaryNamedContact(o, savedEvidenceCandidates(o), opts);
  }

  if (!selected) {
    const next = { ...o };
    // Omit empty secondary rather than null clutter
    if (next.secondaryContact) delete next.secondaryContact;
    return next;
  }

  const { confidence: _conf, ...customerSafe } = selected;
  return {
    ...o,
    secondaryContact: customerSafe,
    secondaryNamedContactVersion: SECONDARY_NAMED_CONTACT_V1,
  };
}

/** Alias used by Full E2E / weekly / sales-packet universal choke points. */
export function enrichOpportunityWithSecondaryNamedContact(opportunity = {}, opts = {}) {
  return attachSecondaryNamedContact(opportunity, opts);
}

/**
 * Evaluate one opportunity for secondary enrichment (backfill / audit).
 * Does not mutate publication eligibility or invent contact channels.
 */
export function evaluateSecondaryContactOpportunity(opportunity = {}, opts = {}) {
  const o = opportunity && typeof opportunity === "object" ? opportunity : {};
  const primaryName = primaryRawName(o);
  const hasNamedPrimary = Boolean(primaryPersonName(o));
  const genericPrimary = primaryIsGeneric(o);
  const before = o.secondaryContact?.name
    ? normalizeSecondaryNamedContact(o.secondaryContact, o, opts)
    : null;

  const enriched = attachSecondaryNamedContact(
    { ...o, secondaryContact: opts.force === true ? undefined : o.secondaryContact },
    { ...opts, force: opts.force === true },
  );
  const after = enriched.secondaryContact || null;

  let disposition = "NO_APPROPRIATE_SECONDARY";
  if (before?.name && after?.name && nameKey(before.name) === nameKey(after.name)) {
    disposition = "ALREADY_HAS_SECONDARY";
  } else if (after?.name) {
    disposition =
      harvestCandidates(o, opts).length > 0 || savedEvidenceCandidates(o).length > 0
        ? "SECONDARY_VERIFIED_FROM_EXISTING_EVIDENCE"
        : "SECONDARY_VERIFIED_FROM_EXISTING_EVIDENCE";
  } else if (genericPrimary || hasNamedPrimary) {
    disposition = "NEEDS_ADDITIONAL_PUBLIC_RESEARCH";
  }

  // When primary is already a strong named buyer and no distinct alt exists, "no secondary" is OK
  if (
    disposition === "NEEDS_ADDITIONAL_PUBLIC_RESEARCH" &&
    hasNamedPrimary &&
    harvestCandidates(o, opts).filter((p) => nameKey(p.name) !== primaryPersonName(o))
      .length === 0 &&
    savedEvidenceCandidates(o).length === 0
  ) {
    disposition = "NO_APPROPRIATE_SECONDARY";
  }

  return {
    opportunityId: o.id || o.opportunityId || null,
    organizationName: o.organizationName || null,
    segment: o.segment || null,
    researchSegment: o.researchSegment || null,
    hasNamedPrimary,
    primaryName: primaryName || null,
    primaryIsGeneric: genericPrimary,
    eligibleForSecondaryResearch: genericPrimary || hasNamedPrimary,
    disposition,
    secondaryProposed: after
      ? {
          name: after.name,
          title: after.title,
          contactClassification: after.contactClassification,
          contactClassificationLabel: after.contactClassificationLabel,
          relationshipToOpportunity: after.relationshipToOpportunity,
          email: after.email,
          phone: after.phone,
          organizationRoutingAlternative: after.organizationRoutingAlternative,
          evidenceUrls: after.evidenceUrls || [],
          verificationDate: after.verificationDate || null,
        }
      : null,
    primaryPreserved: {
      name: o.primaryContact?.name || o.contactName || null,
      email: o.contactEmail || o.primaryContact?.email || o.functionalContactEmail || null,
      phone: o.contactPhone || o.companyPhone || o.primaryContact?.phone || null,
    },
    publicationEligibilityTouched: false,
    enrichedOpportunity: after ? enriched : o,
  };
}

/**
 * Plan a hotel-wide secondary-contact backfill from an opportunity inventory.
 * Dry-run by default — returns proposed patches; does not write Airtable.
 */
export function planHotelSecondaryContactBackfill(opportunities = [], opts = {}) {
  const rows = [];
  let alreadyNamedPrimary = 0;
  let eligible = 0;
  let verifiedFromEvidence = 0;
  let needsResearch = 0;
  let noAppropriate = 0;
  let alreadyHasSecondary = 0;

  for (const opp of opportunities || []) {
    const row = evaluateSecondaryContactOpportunity(opp, opts);
    rows.push(row);
    if (row.hasNamedPrimary) alreadyNamedPrimary += 1;
    if (row.eligibleForSecondaryResearch) eligible += 1;
    if (row.disposition === "SECONDARY_VERIFIED_FROM_EXISTING_EVIDENCE") {
      verifiedFromEvidence += 1;
    } else if (row.disposition === "NEEDS_ADDITIONAL_PUBLIC_RESEARCH") {
      needsResearch += 1;
    } else if (row.disposition === "ALREADY_HAS_SECONDARY") {
      alreadyHasSecondary += 1;
    } else {
      noAppropriate += 1;
    }
  }

  const proposedPatches = rows
    .filter((r) => r.disposition === "SECONDARY_VERIFIED_FROM_EXISTING_EVIDENCE" && r.secondaryProposed)
    .map((r) => ({
      opportunityId: r.opportunityId,
      organizationName: r.organizationName,
      secondaryContact: r.secondaryProposed,
      // Patch only secondaryContact — never primary / publication fields
      patch: { secondaryContact: r.enrichedOpportunity.secondaryContact },
    }));

  return {
    version: UNIVERSAL_SECONDARY_CONTACT_V1,
    hotelId: opts.hotelId || null,
    inspected: rows.length,
    alreadyNamedPrimary,
    eligibleForSecondaryResearch: eligible,
    secondaryVerifiedFromExistingEvidence: verifiedFromEvidence,
    alreadyHasSecondary,
    needsAdditionalPublicResearch: needsResearch,
    noAppropriateSecondary: noAppropriate,
    proposedPatchCount: proposedPatches.length,
    airtableWrites: false,
    paidResearch: false,
    rows,
    proposedPatches,
  };
}

export function buildCpGroupSecondaryContactExample(opportunity = {}) {
  return normalizeSecondaryNamedContact(CP_GROUP_SECONDARY_CONTACT_EXAMPLE, {
    organizationName: "CP Group",
    functionalContactEmail: "bocaraton@cpgcre.com",
    contactPhone: "561.447.1800",
    ...opportunity,
  });
}

export function primaryContactIsGeneric(opportunity = {}) {
  return primaryIsGeneric(opportunity);
}
