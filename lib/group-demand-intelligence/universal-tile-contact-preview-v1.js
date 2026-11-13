/**
 * Universal GDI tile contact preview — V2 display contract (V1-compatible exports).
 *
 * Tile shows at most ONE named person + ONE secondary description:
 *   1) Verified professional title
 *   2) Verified opportunity-specific role
 *   3) Role Not Yet Verified
 * No named person → Contact Research Required (no role line).
 *
 * displayRole is separate from buyerEligibility. Never invents titles.
 * Never auto-promotes sales readiness. Never calls Surfe.
 */
import { looksLikePersonName } from "./secondary-named-contact-v1.js";

export const UNIVERSAL_TILE_CONTACT_PREVIEW_V1 =
  "gdi-universal-tile-contact-preview-v1";
export const UNIVERSAL_TILE_CONTACT_PREVIEW_V2 =
  "gdi-universal-tile-contact-preview-v2";

/** @deprecated V1 eligibility-flavored labels — not for tile displayRole in V2. */
export const TILE_CONTACT_ROLE_LABEL = Object.freeze({
  DECISION_MAKER: "Decision Maker",
  EVENT_ORGANIZER: "Event Organizer",
  MEETING_PLANNER: "Meeting Planner",
  GROUP_TRAVEL: "Group Travel Contact",
  CORPORATE_TRAVEL: "Corporate Travel Contact",
  POTENTIAL_BUYER: "Potential Buyer",
  REFERRAL: "Referral Contact",
  UNDER_REVIEW: "Contact Under Review",
});

/** V2 customer-facing opportunity roles (second priority after professional title). */
export const TILE_OPPORTUNITY_ROLE_LABEL = Object.freeze({
  EVENT_ORGANIZER: "Event Organizer",
  MEETING_PLANNER: "Meeting Planner",
  GROUP_TRAVEL: "Group Travel Coordinator",
  CORPORATE_TRAVEL: "Corporate Travel Contact",
  REFERRAL: "Referral Contact",
});

/** Customer-facing gap copy — does not imply qualification is complete. */
export const TILE_NO_NAMED_CONTACT_LABEL = "Contact Research Required";
/** @deprecated Legacy copy retained for fixture/test recognition only. */
export const TILE_NO_NAMED_CONTACT_LABEL_LEGACY = "Buyer Not Yet Identified";
export const TILE_ROLE_NOT_YET_VERIFIED = "Role Not Yet Verified";

export const TILE_DISPLAY_ROLE_KIND = Object.freeze({
  PROFESSIONAL_TITLE: "professional_title",
  OPPORTUNITY_ROLE: "opportunity_role",
  UNVERIFIED: "unverified",
  NONE: "none",
});

/** Forbidden on tile footers (internal research / eligibility statuses). */
export const TILE_FORBIDDEN_DISPLAY_LABELS = Object.freeze([
  "Contact Under Review",
  "Relevant Contact",
  "Relevant contact",
  "CONTACT_RELEVANT",
  "INELIGIBLE_UNSPECIFIED",
  "Buyer Eligibility Pending",
  "Unverified Buyer",
  "Role Unverified",
  "Relevant Influencer",
  "Potential Buyer",
  "Decision Maker",
]);

export const APPROVED_TILE_ROLE_LABELS = Object.freeze(
  Object.values(TILE_CONTACT_ROLE_LABEL),
);

export const APPROVED_TILE_OPPORTUNITY_ROLE_LABELS = Object.freeze(
  Object.values(TILE_OPPORTUNITY_ROLE_LABEL),
);

function text(v) {
  return String(v || "")
    .replace(/\s+/g, " ")
    .trim();
}

function isForbiddenDisplayLabel(label) {
  const t = text(label);
  if (!t) return true;
  return TILE_FORBIDDEN_DISPLAY_LABELS.some(
    (f) => f.toLowerCase() === t.toLowerCase(),
  );
}

/**
 * Extract a verified workplace / professional title (not an eligibility enum).
 */
export function extractVerifiedProfessionalTitle(contact = {}) {
  const candidates = [
    contact.professionalTitle,
    contact.jobTitle,
    contact.title,
    contact.role,
    contact.workplaceTitle,
  ]
    .map(text)
    .filter(Boolean);

  for (const original of candidates) {
    // Prefer the first clause when research packed title + narrative (";", " — ", " - ")
    const segments = original
      .split(/\s*[;|]\s*|\s+[—–]\s+|\s+-\s+(?=[A-Z])/)
      .map(text)
      .filter(Boolean);
    const tryList = segments.length ? [segments[0], original] : [original];

    for (const raw of tryList) {
      if (isForbiddenDisplayLabel(raw)) continue;
      // Classification enums / opportunity roles are not professional titles
      if (
        /^(decision[-\s]?maker|event organizer|meeting planner|group travel|corporate travel|referral contact|hotel sourcing contact|potential buyer)$/i.test(
          raw,
        )
      ) {
        continue;
      }
      // Reject org-sounding shells
      if (
        /\b(association|conference|university|inc\.?|llc|corp\.?|desk|department|committee|housing bureau)\b/i.test(
          raw,
        ) &&
        !/\b(director|manager|president|vp|vice|head|coordinator|officer|lead)\b/i.test(
          raw,
        )
      ) {
        continue;
      }
      // Reject prose / why-relevant narratives (after clause split)
      if (
        raw.length > 72 ||
        /\b(confirm|outreach|until|based in|profile describes|public profile)\b/i.test(
          raw,
        )
      ) {
        continue;
      }
      // Reject speaker / attendee shells that are not workplace titles
      if (
        /\b(keynote|speaker|panelist|author|attendee|delegate|participant|moderator)\b/i.test(
          raw,
        ) &&
        !/\b(director|manager|president|vp|head|coordinator|officer)\b/i.test(raw)
      ) {
        continue;
      }
      // Prefer title-like tokens; allow short verified titles without keyword when clean
      const looksTitled =
        /\b(director|manager|president|vp|vice\s*president|head of|chief|coordinator|officer|lead|partner|founder|ceo|coo|cfo|cto|secretary|chair|planner|organizer|consultant|associate|specialist|executive|operations)\b/i.test(
          raw,
        ) ||
        (/^[A-ZÀ-ÖØ-öø-ÿ][\w'’&.\/,\- À-ÖØ-öø-ÿ]{2,70}$/.test(raw) &&
          raw.split(/\s+/).length <= 8);
      if (!looksTitled) continue;
      return shortDisplayTitle(raw);
    }
  }
  return null;
}

function shortDisplayTitle(raw) {
  let t = text(raw);
  if (t.length <= 56) return t;
  const cut = t.slice(0, 56);
  const sp = cut.lastIndexOf(" ");
  return `${(sp > 28 ? cut.slice(0, sp) : cut).trim()}…`;
}

function evidenceBlob(contact = {}, opportunity = {}) {
  return [
    contact.roleLabel,
    contact.contactClassificationLabel,
    contact.roleClass,
    contact.role,
    contact.title,
    contact.whyThisContact,
    contact.whyRelevantToHotelDemand,
    contact.whyContact,
    contact.commercialRelevance,
    contact.relationshipToOpportunity,
    contact.contactRelevanceLabel,
    opportunity.contactResponsibilityClass,
    opportunity.contactResponsibility,
    opportunity.buyerRole,
  ]
    .map(text)
    .filter(Boolean)
    .join(" ");
}

/**
 * Map evidence → opportunity-specific role for tile (V2 second priority).
 * Never returns Contact Under Review / Decision Maker / Potential Buyer.
 */
export function mapEvidenceToOpportunityRoleLabel(contact = {}, opportunity = {}) {
  const blob = evidenceBlob(contact, opportunity).toLowerCase();
  const explicit = text(
    contact.tileRoleLabel ||
      contact.contactClassificationLabel ||
      contact.roleClass ||
      "",
  );

  if (/^event organizer$/i.test(explicit)) {
    return TILE_OPPORTUNITY_ROLE_LABEL.EVENT_ORGANIZER;
  }
  if (/^meeting planner$/i.test(explicit)) {
    return TILE_OPPORTUNITY_ROLE_LABEL.MEETING_PLANNER;
  }
  if (/^hotel sourcing contact$|^group travel/i.test(explicit)) {
    return TILE_OPPORTUNITY_ROLE_LABEL.GROUP_TRAVEL;
  }
  if (/^corporate travel/i.test(explicit)) {
    return TILE_OPPORTUNITY_ROLE_LABEL.CORPORATE_TRAVEL;
  }
  if (/^referral contact$/i.test(explicit)) {
    return TILE_OPPORTUNITY_ROLE_LABEL.REFERRAL;
  }

  if (
    /\b(speaker|keynote|panelist|author|attendee|delegate|participant|journalist|reporter)\b/i.test(
      blob,
    ) &&
    !/\b(organizer|planner|housing|sourcing|meetings manager|events manager|pco|travel manager)\b/i.test(
      blob,
    )
  ) {
    return null;
  }

  if (
    /\b(event organizer|congress (director|chair)|event director)\b/i.test(blob) ||
    (/\borganizer\b/i.test(blob) &&
      /\b(event|congress|conference|exhibition|expo)\b/i.test(blob))
  ) {
    return TILE_OPPORTUNITY_ROLE_LABEL.EVENT_ORGANIZER;
  }
  if (
    /\b(meeting planner|congress manager|pco|destination manager|conference planner)\b/i.test(
      blob,
    )
  ) {
    return TILE_OPPORTUNITY_ROLE_LABEL.MEETING_PLANNER;
  }
  if (
    /\b(group travel|delegation travel|housing bureau|room.?block|hotel.?sourc|accommodation (manager|coordinator)|participant travel)\b/i.test(
      blob,
    )
  ) {
    return TILE_OPPORTUNITY_ROLE_LABEL.GROUP_TRAVEL;
  }
  if (
    /\b(corporate travel|travel manager|travel management|business travel|lodging (program|programme|manager)|tmc)\b/i.test(
      blob,
    )
  ) {
    return TILE_OPPORTUNITY_ROLE_LABEL.CORPORATE_TRAVEL;
  }
  if (
    /\b(referral|intermediary|introduction|route to|partner liaison|sponsor liaison)\b/i.test(
      blob,
    ) ||
    contact.contactResponsibilityClass === "REFERRAL_ENTRY" ||
    /REFERRAL/i.test(String(contact.contactRelevanceLabel || ""))
  ) {
    return TILE_OPPORTUNITY_ROLE_LABEL.REFERRAL;
  }
  return null;
}

/**
 * @deprecated Prefer resolveTileDisplayRole + buyerEligibility. Kept for V1 tests.
 */
export function mapEvidenceToTileRoleLabel(contact = {}, opportunity = {}) {
  const title = extractVerifiedProfessionalTitle(contact);
  if (title) return title;
  const oppRole = mapEvidenceToOpportunityRoleLabel(contact, opportunity);
  if (oppRole) return oppRole;
  // V1 returned Contact Under Review — V2 maps that path to Role Not Yet Verified
  // for display callers that still use this helper.
  if (text(contact.name)) return TILE_ROLE_NOT_YET_VERIFIED;
  return TILE_CONTACT_ROLE_LABEL.UNDER_REVIEW;
}

/**
 * Resolve customer-facing displayRole (title → opportunity role → unverified).
 */
export function resolveTileDisplayRole(contact = {}, opportunity = {}) {
  const professionalTitle = extractVerifiedProfessionalTitle(contact);
  if (professionalTitle) {
    return {
      displayRole: professionalTitle,
      displayRoleKind: TILE_DISPLAY_ROLE_KIND.PROFESSIONAL_TITLE,
      professionalTitle,
      opportunityRole: mapEvidenceToOpportunityRoleLabel(contact, opportunity),
    };
  }
  const opportunityRole = mapEvidenceToOpportunityRoleLabel(
    contact,
    opportunity,
  );
  if (opportunityRole) {
    return {
      displayRole: opportunityRole,
      displayRoleKind: TILE_DISPLAY_ROLE_KIND.OPPORTUNITY_ROLE,
      professionalTitle: null,
      opportunityRole,
    };
  }
  return {
    displayRole: TILE_ROLE_NOT_YET_VERIFIED,
    displayRoleKind: TILE_DISPLAY_ROLE_KIND.UNVERIFIED,
    professionalTitle: null,
    opportunityRole: null,
  };
}

/**
 * Buyer eligibility is independent of displayRole.
 */
export function assessTileBuyerEligibility(contact = {}, opportunity = {}) {
  const oppRole = mapEvidenceToOpportunityRoleLabel(contact, opportunity);
  const eligibleFlag =
    contact.buyerEligible === true ||
    contact.isNamedCommerciallyRelevant === true;
  const blob = evidenceBlob(contact, opportunity).toLowerCase();

  let status = "ineligible";
  let reason = "insufficient_buyer_evidence";

  if (
    /\b(decision.?maker|contracting authority|procurement (director|manager|lead))\b/i.test(
      blob,
    ) &&
    eligibleFlag
  ) {
    status = "eligible_decision_maker";
    reason = "decision_authority_evidenced";
  } else if (
    oppRole &&
    [
      TILE_OPPORTUNITY_ROLE_LABEL.EVENT_ORGANIZER,
      TILE_OPPORTUNITY_ROLE_LABEL.MEETING_PLANNER,
      TILE_OPPORTUNITY_ROLE_LABEL.GROUP_TRAVEL,
      TILE_OPPORTUNITY_ROLE_LABEL.CORPORATE_TRAVEL,
    ].includes(oppRole) &&
    eligibleFlag
  ) {
    status = "eligible_sourcing_role";
    reason = "verified_sourcing_or_planning_role";
  } else if (
    oppRole === TILE_OPPORTUNITY_ROLE_LABEL.REFERRAL &&
    eligibleFlag
  ) {
    status = "eligible_referral";
    reason = "verified_commercial_referral_pathway";
  } else if (eligibleFlag) {
    status = "eligible_potential_buyer";
    reason = "named_commercially_relevant";
  } else if (text(contact.name)) {
    status = "ineligible";
    reason = "named_buyer_gate_not_satisfied";
  }

  return {
    buyerEligible: status.startsWith("eligible"),
    buyerEligibleForGate: status.startsWith("eligible"),
    status,
    reason,
  };
}

function personFieldsMatchingName(opportunity = {}, name = "") {
  const want = text(name).toLowerCase();
  if (!want) return {};
  const pool = [
    opportunity.primaryContact,
    opportunity.secondaryContact,
    ...(Array.isArray(opportunity.namedContacts) ? opportunity.namedContacts : []),
  ].filter(Boolean);
  const match = pool.find((p) => text(p.name).toLowerCase() === want);
  if (!match) return {};
  return {
    contactClassificationLabel: text(match.contactClassificationLabel) || null,
    roleClass: text(match.roleClass) || null,
    contactRelevanceLabel: text(match.contactRelevanceLabel) || null,
    buyerEligible: match.buyerEligible === true,
    isNamedCommerciallyRelevant:
      match.isNamedCommerciallyRelevant === true || match.buyerEligible === true,
    title: text(match.title || match.role || match.jobTitle) || null,
    professionalTitle: text(match.professionalTitle || match.jobTitle) || null,
    whyThisContact: text(
      match.whyThisContact || match.whyRelevantToHotelDemand,
    ),
  };
}

function buildContactPayload(slotOrPerson, opportunity, source) {
  const fromPerson = personFieldsMatchingName(
    opportunity,
    slotOrPerson?.name,
  );
  return {
    name: text(slotOrPerson.name),
    title:
      text(slotOrPerson.title) ||
      text(slotOrPerson.role) ||
      fromPerson.title ||
      null,
    role:
      text(slotOrPerson.role) ||
      text(slotOrPerson.title) ||
      fromPerson.title ||
      null,
    professionalTitle:
      text(slotOrPerson.professionalTitle) ||
      fromPerson.professionalTitle ||
      null,
    jobTitle: text(slotOrPerson.jobTitle) || null,
    whyContact: text(slotOrPerson.whyContact) || fromPerson.whyThisContact || null,
    whyThisContact:
      text(slotOrPerson.whyContact || slotOrPerson.whyThisContact) ||
      fromPerson.whyThisContact ||
      null,
    contactClassificationLabel:
      text(slotOrPerson.contactClassificationLabel) ||
      fromPerson.contactClassificationLabel ||
      null,
    roleClass: text(slotOrPerson.roleClass) || fromPerson.roleClass || null,
    contactRelevanceLabel:
      text(slotOrPerson.contactRelevanceLabel) ||
      fromPerson.contactRelevanceLabel ||
      null,
    buyerEligible:
      slotOrPerson.buyerEligible === true || fromPerson.buyerEligible === true,
    isNamedCommerciallyRelevant:
      slotOrPerson.isNamedCommerciallyRelevant === true ||
      slotOrPerson.buyerEligible === true ||
      fromPerson.isNamedCommerciallyRelevant === true,
    profileUrl: text(slotOrPerson.profileUrl || slotOrPerson.linkedInUrl) || null,
    linkedInUrl: text(slotOrPerson.linkedInUrl) || null,
    email: text(slotOrPerson.email) || null,
    phone: text(slotOrPerson.phone) || null,
    organization: text(slotOrPerson.organization) || null,
    _source: source,
  };
}

function candidateFromSlot(slot, opportunity, source) {
  if (!slot || typeof slot !== "object") return null;
  const name = text(slot.name);
  if (!name || !looksLikePersonName(name)) return null;
  if (slot.customerFacingNamed === false) return null;
  const contact = buildContactPayload(slot, opportunity, source);
  const display = resolveTileDisplayRole(contact, opportunity);
  const eligibility = assessTileBuyerEligibility(contact, opportunity);
  const hasRoute = Boolean(
    contact.profileUrl ||
      contact.linkedInUrl ||
      contact.email ||
      contact.phone ||
      text(slot.officialContactUrl),
  );
  return {
    name,
    contact,
    display,
    eligibility,
    hasRoute,
    source,
    drawerSlot: source,
    // V1 compat
    roleLabel: display.displayRole,
    buyerEligibleForGate: eligibility.buyerEligibleForGate === true,
    rank:
      display.displayRoleKind === TILE_DISPLAY_ROLE_KIND.PROFESSIONAL_TITLE
        ? 1
        : display.displayRoleKind === TILE_DISPLAY_ROLE_KIND.OPPORTUNITY_ROLE
          ? 2
          : 3,
  };
}

function candidateFromPerson(person, opportunity, source) {
  if (!person || typeof person !== "object") return null;
  const name = text(person.name);
  if (!name || !looksLikePersonName(name)) return null;
  const contact = buildContactPayload(person, opportunity, source);
  const display = resolveTileDisplayRole(contact, opportunity);
  const eligibility = assessTileBuyerEligibility(contact, opportunity);
  const hasRoute = Boolean(
    contact.profileUrl || contact.linkedInUrl || contact.email || contact.phone,
  );
  return {
    name,
    contact,
    display,
    eligibility,
    hasRoute,
    source,
    drawerSlot: source,
    roleLabel: display.displayRole,
    buyerEligibleForGate: eligibility.buyerEligibleForGate === true,
    rank:
      display.displayRoleKind === TILE_DISPLAY_ROLE_KIND.PROFESSIONAL_TITLE
        ? 1
        : display.displayRoleKind === TILE_DISPLAY_ROLE_KIND.OPPORTUNITY_ROLE
          ? 2
          : 3,
  };
}

function pickBestCandidate(candidates = []) {
  const list = (candidates || []).filter(Boolean);
  if (!list.length) return null;
  // Selection is already primary-first; rank only used within single-candidate sets
  return list[0];
}

/**
 * Select the single tile contact preview from canonical Who-to-Contact data.
 */
export function selectUniversalTileContactPreview(opportunity = {}) {
  const o = opportunity && typeof opportunity === "object" ? opportunity : {};
  const two = o.scalingTwoContactPresentation || null;
  const primarySlot = two?.primary || null;
  const secondarySlot = two?.secondary || null;

  const candidates = [];

  // Named primary always wins the tile when present (drawer Section 7 consistency).
  const fromPrimarySlot = candidateFromSlot(primarySlot, o, "primary");
  if (fromPrimarySlot) {
    candidates.push(fromPrimarySlot);
  } else {
    const fromSecondarySlot = candidateFromSlot(
      secondarySlot,
      o,
      "secondary",
    );
    if (fromSecondarySlot) candidates.push(fromSecondarySlot);
  }

  if (!candidates.length) {
    const p = candidateFromPerson(o.primaryContact, o, "primaryContact");
    if (p) candidates.push(p);
  }
  if (!candidates.length) {
    const s = candidateFromPerson(o.secondaryContact, o, "secondaryContact");
    if (s) candidates.push(s);
  }
  if (
    !candidates.length &&
    text(o.contactName) &&
    looksLikePersonName(o.contactName)
  ) {
    candidates.push(
      candidateFromPerson(
        {
          name: o.contactName,
          title: o.contactRole,
          role: o.contactRole,
          whyThisContact: o.whoPrimaryReason,
          buyerEligible: o.primaryContact?.buyerEligible,
        },
        o,
        "contactName",
      ),
    );
  }

  const best = pickBestCandidate(candidates);
  if (!best) {
    return {
      version: UNIVERSAL_TILE_CONTACT_PREVIEW_V2,
      kind: "none",
      name: null,
      roleLabel: null,
      displayName: TILE_NO_NAMED_CONTACT_LABEL,
      displayRole: null,
      displayRoleKind: TILE_DISPLAY_ROLE_KIND.NONE,
      professionalTitle: null,
      opportunityRole: null,
      buyerEligibility: {
        buyerEligible: false,
        buyerEligibleForGate: false,
        status: "no_named_contact",
        reason: "no_verified_named_individual",
      },
      buyerEligibleForGate: false,
      drawerSlot: null,
      namedContactCountConsidered: 0,
    };
  }

  const displayRole = best.display.displayRole;
  if (isForbiddenDisplayLabel(displayRole)) {
    best.display.displayRole = TILE_ROLE_NOT_YET_VERIFIED;
    best.display.displayRoleKind = TILE_DISPLAY_ROLE_KIND.UNVERIFIED;
  }

  return {
    version: UNIVERSAL_TILE_CONTACT_PREVIEW_V2,
    kind: "named",
    name: best.name,
    // V1 compat: roleLabel mirrors displayRole (never forbidden statuses)
    roleLabel: best.display.displayRole,
    displayName: best.name,
    displayRole: best.display.displayRole,
    displayRoleKind: best.display.displayRoleKind,
    professionalTitle: best.display.professionalTitle,
    opportunityRole: best.display.opportunityRole,
    organization: best.contact.organization || null,
    buyerEligibility: best.eligibility,
    buyerEligibleForGate: best.eligibility.buyerEligibleForGate === true,
    drawerSlot: best.drawerSlot,
    namedContactCountConsidered: candidates.length,
    source: best.source,
  };
}

/**
 * Attach tileContactPreview onto opportunity after Who-to-Contact hydration.
 */
export function attachUniversalTileContactPreview(opportunity = {}) {
  const o =
    opportunity && typeof opportunity === "object" ? { ...opportunity } : {};
  const preview = selectUniversalTileContactPreview(o);
  o.tileContactPreview = preview;
  o.tileContactName =
    preview.kind === "named" ? preview.name : TILE_NO_NAMED_CONTACT_LABEL;
  o.tileContactRoleLabel =
    preview.kind === "named" ? preview.displayRole : null;
  return o;
}

export function assertApprovedTileRoleLabel(label) {
  const t = text(label);
  if (!t) return { ok: true, empty: true };
  if (t === TILE_ROLE_NOT_YET_VERIFIED) return { ok: true, label: t };
  if (APPROVED_TILE_OPPORTUNITY_ROLE_LABELS.includes(t)) {
    return { ok: true, label: t };
  }
  // Professional titles are free-form verified strings (not enum-bound)
  if (!isForbiddenDisplayLabel(t)) return { ok: true, label: t, freeform: true };
  return { ok: false, label: t };
}

export function tileContactPreviewViolatesOneNameRule(preview = {}) {
  const name = text(preview.displayName || preview.name);
  if (!name) return false;
  if (name === TILE_NO_NAMED_CONTACT_LABEL) return false;
  if (/\s+\/\s+|\s+&\s+|\sand\s|,.*,/i.test(name)) return true;
  return false;
}

export function tileDisplayRoleIsForbidden(preview = {}) {
  const role = text(preview.displayRole || preview.roleLabel);
  if (!role) return false;
  return isForbiddenDisplayLabel(role);
}
