/**
 * Account-quality taxonomy for GDI customer-ready opportunities.
 * Distinguishes real sales targets from venue/generator/contact shells.
 * Does not invent child accounts.
 */

import {
  BUYER_CONTACT_PATH_CLASS,
  classifyBuyerContactPath,
  classifyContactUrl,
  hasLodgingContactEvidence,
} from "./buyer-contact-path-taxonomy-v1.js";

export const ACCOUNT_QUALITY_CLASS = Object.freeze({
  TRUE_BUYER_ACCOUNT: "TRUE_BUYER_ACCOUNT",
  TRUE_PARTICIPATING_ACCOUNT: "TRUE_PARTICIPATING_ACCOUNT",
  TRUE_DELEGATION_ACCOUNT: "TRUE_DELEGATION_ACCOUNT",
  TRUE_VENDOR_CREW_ACCOUNT: "TRUE_VENDOR_CREW_ACCOUNT",
  TRUE_ORGANIZER_HOUSING_ACCOUNT: "TRUE_ORGANIZER_HOUSING_ACCOUNT",
  VENUE_OPERATOR_PLACEHOLDER: "VENUE_OPERATOR_PLACEHOLDER",
  GENERATOR_WRAPPER: "GENERATOR_WRAPPER",
  GENERIC_ORG_SHELL: "GENERIC_ORG_SHELL",
  CONTACT_PATH_SHELL: "CONTACT_PATH_SHELL",
  UNKNOWN: "UNKNOWN",
});

const TRUE_READY_CLASSES = new Set([
  ACCOUNT_QUALITY_CLASS.TRUE_BUYER_ACCOUNT,
  ACCOUNT_QUALITY_CLASS.TRUE_PARTICIPATING_ACCOUNT,
  ACCOUNT_QUALITY_CLASS.TRUE_DELEGATION_ACCOUNT,
  ACCOUNT_QUALITY_CLASS.TRUE_VENDOR_CREW_ACCOUNT,
  ACCOUNT_QUALITY_CLASS.TRUE_ORGANIZER_HOUSING_ACCOUNT,
]);

const BARE_PLACEHOLDER_ORG_RE =
  /^(partner|partners|delegations?|sponsors?|exhibitors?|consultanc(?:y|ies)|media\s*crews?|pr\s*teams?|crews?|teams?|cycle|humanitarian|ngo|university|production\s*vendor|palexpo\s*(listing\s*)?decompose|keep\s*generator|ampaign\s*visible|not\s*spectators|spectators)$/i;

const VENUE_ROLE_RE =
  /\b(VENUE_OPERATOR|VENUE|venue\s*operator)\b/i;

const ORGANIZER_ROLE_RE =
  /\b(ORGANIZER|EVENT_ORGANIZER|SECRETARIAT|PARENT_SOCIETY|FAIR\s*MANAGEMENT|MEETINGS)\b/i;

const DELEGATION_ROLE_RE =
  /\b(DELEGATION|GOVERNMENT|MINISTRY|MEMBER.?STATE|NGO|FOUNDATION)\b/i;

const VENDOR_ROLE_RE =
  /\b(VENDOR|CREW|PRODUCTION|HANDLER|INSTALLER|BROADCAST|AV\b|TECH|SECURITY|STAND\s*BUILDER)\b/i;

const PARTICIPANT_ROLE_RE =
  /\b(EXHIBITOR|SPONSOR|PARTICIPANT|GALLERY|UNIVERSITY_HOST|SESSION\s*PARTNER|NETWORKING\s*PARTNER)\b/i;

function blob(opp = {}) {
  return [
    opp.title,
    opp.organizationName,
    opp.participationRole,
    opp.childEntityType,
    opp.demandType,
    opp.segment,
    opp.buyerEntity,
    opp.primaryContactRole,
    opp.buyerRole,
  ]
    .map((x) => String(x || ""))
    .join(" | ");
}

function isPlaceholderOrgName(name = "") {
  const n = String(name || "").trim();
  if (!n || n.length < 3) return true;
  // Bare template labels only — not "University of Geneva …"
  if (BARE_PLACEHOLDER_ORG_RE.test(n)) return true;
  if (/^(unknown|tbd|n\/?a|various)\b/i.test(n)) return true;
  // Multi-role template residue: "partner delegation NGO university"
  if ((n.match(/\b(sponsor|exhibitor|speaker|partner|delegation|ngo|university|vendor)\b/gi) || []).length >= 3) {
    return true;
  }
  return false;
}

function hasHousingControlEvidence(opp = {}) {
  if (hasLodgingContactEvidence(opp)) return true;
  const housing = String(opp.housingStatus || "").toUpperCase();
  if (housing === "STRONG" || housing === "VERIFIED") return true;
  const room = String(opp.roomDemandStatus || "").toUpperCase();
  if (/VERIFIED_HOUSING|PUBLISHED_ROOM|CONFIRMED_BLOCK/.test(room)) return true;
  const role = `${opp.primaryContactRole || ""} ${opp.buyerRole || ""} ${opp.buyerEntity || ""}`;
  const contact = classifyBuyerContactPath(opp);
  // Meetings / exhibitor-services / housing / hospitality + relevant function URL
  // (not bare "secretariat" / "conference support" alone — those are often local HQ)
  if (
    contact.class === BUYER_CONTACT_PATH_CLASS.RELEVANT_FUNCTION_CONTACT &&
    /\b(housing|hotel|accommodation|meetings|exhibitor\s*services|hospitality|hotel\s*reservation)\b/i.test(
      role
    )
  ) {
    return true;
  }
  return false;
}

/**
 * Classify account quality for a GDI opportunity.
 */
export function classifyAccountQuality(opp = {}) {
  const org = String(opp.organizationName || opp.company || "").trim();
  const role = String(opp.participationRole || opp.childEntityType || opp.demandType || "").trim();
  const title = String(opp.title || "");
  const b = blob(opp);
  const contact = classifyBuyerContactPath(opp);
  const urlClass = classifyContactUrl(opp.publicContactPath || "", {
    officialSource: opp.officialSource,
    discoverySource: opp.discoverySource,
  });

  if (VENUE_ROLE_RE.test(role) || /\(VENUE OPERATOR\)/i.test(title) || /^Palexpo SA\b/i.test(org)) {
    // Palexpo / venue as account without housing control = placeholder
    if (!hasHousingControlEvidence(opp)) {
      return {
        class: ACCOUNT_QUALITY_CLASS.VENUE_OPERATOR_PLACEHOLDER,
        readyEligible: false,
        reason: "venue_without_housing_control",
        contactClass: contact.class,
      };
    }
  }

  if (isPlaceholderOrgName(org)) {
    return {
      class: ACCOUNT_QUALITY_CLASS.GENERATOR_WRAPPER,
      readyEligible: false,
      reason: "placeholder_or_template_org_name",
      contactClass: contact.class,
    };
  }

  // Contact-path shell: only homepage/source page and no stronger role evidence
  if (
    (contact.class === BUYER_CONTACT_PATH_CLASS.SOURCE_PAGE ||
      contact.class === BUYER_CONTACT_PATH_CLASS.GENERAL_ORG_CONTACT ||
      urlClass === BUYER_CONTACT_PATH_CLASS.SOURCE_PAGE ||
      urlClass === BUYER_CONTACT_PATH_CLASS.GENERAL_ORG_CONTACT) &&
    !contact.readyEligible
  ) {
    return {
      class: ACCOUNT_QUALITY_CLASS.CONTACT_PATH_SHELL,
      readyEligible: false,
      reason: contact.reason || "generic_contact_path",
      contactClass: contact.class,
    };
  }

  // Parent society alone is usually a wrapper when a regional organizer exists
  if (
    /PARENT_SOCIETY/i.test(role) ||
    /\(PARENT SOCIETY\)/i.test(title) ||
    /global meetings office/i.test(String(opp.buyerRole || opp.primaryContactRole || "")) ||
    (/society of environmental toxicology and chemistry/i.test(org) &&
      !/setac europe/i.test(org))
  ) {
    return {
      class: ACCOUNT_QUALITY_CLASS.GENERIC_ORG_SHELL,
      readyEligible: false,
      reason: "parent_society_shell",
      contactClass: contact.class,
    };
  }

  // University / institute host without lodging-control evidence
  if (
    /UNIVERSITY_HOST/i.test(role) ||
    /\(UNIVERSITY HOST\)/i.test(title) ||
    (/university of geneva|unige/i.test(org) &&
      /institute|global health|faculty/i.test(org))
  ) {
    if (!hasHousingControlEvidence(opp)) {
      return {
        class: ACCOUNT_QUALITY_CLASS.GENERIC_ORG_SHELL,
        readyEligible: false,
        reason: "university_host_without_travel_thesis",
        contactClass: contact.class,
      };
    }
  }

  // Organizer / secretariat
  if (ORGANIZER_ROLE_RE.test(role) || /\((ORGANIZER|SECRETARIAT|EVENT ORGANIZER)\)/i.test(title)) {
    if (hasHousingControlEvidence(opp)) {
      return {
        class: ACCOUNT_QUALITY_CLASS.TRUE_ORGANIZER_HOUSING_ACCOUNT,
        readyEligible: true,
        reason: "organizer_with_housing_or_meetings_path",
        contactClass: contact.class,
      };
    }
    // Named organizer without housing control = generator wrapper / org shell
    return {
      class: ACCOUNT_QUALITY_CLASS.GENERATOR_WRAPPER,
      readyEligible: false,
      reason: "organizer_without_housing_control",
      contactClass: contact.class,
    };
  }

  if (DELEGATION_ROLE_RE.test(role) || /\bdelegation\b/i.test(b)) {
    if (!isPlaceholderOrgName(org) && contact.readyEligible) {
      return {
        class: ACCOUNT_QUALITY_CLASS.TRUE_DELEGATION_ACCOUNT,
        readyEligible: true,
        reason: "named_delegation_with_path",
        contactClass: contact.class,
      };
    }
    return {
      class: ACCOUNT_QUALITY_CLASS.GENERIC_ORG_SHELL,
      readyEligible: false,
      reason: "delegation_shell_without_path",
      contactClass: contact.class,
    };
  }

  if (VENDOR_ROLE_RE.test(role)) {
    if (!isPlaceholderOrgName(org) && contact.readyEligible) {
      return {
        class: ACCOUNT_QUALITY_CLASS.TRUE_VENDOR_CREW_ACCOUNT,
        readyEligible: true,
        reason: "named_vendor_crew",
        contactClass: contact.class,
      };
    }
    return {
      class: ACCOUNT_QUALITY_CLASS.GENERATOR_WRAPPER,
      readyEligible: false,
      reason: "vendor_role_placeholder",
      contactClass: contact.class,
    };
  }

  if (PARTICIPANT_ROLE_RE.test(role) || /\((EXHIBITOR|SPONSOR|UNIVERSITY HOST|PARTNER)\)/i.test(title)) {
    if (!isPlaceholderOrgName(org) && contact.readyEligible) {
      // University host without travel/housing thesis → shell (role field or title tag)
      if (
        (/UNIVERSITY_HOST/i.test(role) || /\(UNIVERSITY HOST\)/i.test(title)) &&
        !hasHousingControlEvidence(opp)
      ) {
        return {
          class: ACCOUNT_QUALITY_CLASS.GENERIC_ORG_SHELL,
          readyEligible: false,
          reason: "university_host_without_travel_thesis",
          contactClass: contact.class,
        };
      }
      return {
        class: ACCOUNT_QUALITY_CLASS.TRUE_PARTICIPATING_ACCOUNT,
        readyEligible: true,
        reason: "named_participant",
        contactClass: contact.class,
      };
    }
    return {
      class: ACCOUNT_QUALITY_CLASS.GENERIC_ORG_SHELL,
      readyEligible: false,
      reason: "participant_shell",
      contactClass: contact.class,
    };
  }

  // Private-event / preferred-lodging venue partnerships (Bethesda PE control)
  if (
    org &&
    contact.readyEligible &&
    (opp.peVenueId ||
      /preferred lodging|private event|venue partnership/i.test(title + " " + b))
  ) {
    return {
      class: ACCOUNT_QUALITY_CLASS.TRUE_BUYER_ACCOUNT,
      readyEligible: true,
      reason: "private_event_or_preferred_lodging_account",
      contactClass: contact.class,
    };
  }

  // Named org + ready contact + buyer role → buyer account
  if (org && contact.readyEligible && (opp.buyerEntity || opp.primaryContactRole || opp.buyerRole)) {
    return {
      class: ACCOUNT_QUALITY_CLASS.TRUE_BUYER_ACCOUNT,
      readyEligible: true,
      reason: "named_org_buyer_path",
      contactClass: contact.class,
    };
  }

  // Named person contact + named org (Bethesda association meetings pattern)
  if (
    org &&
    contact.class === BUYER_CONTACT_PATH_CLASS.NAMED_BUYER_PERSON &&
    contact.readyEligible
  ) {
    return {
      class: ACCOUNT_QUALITY_CLASS.TRUE_BUYER_ACCOUNT,
      readyEligible: true,
      reason: "named_buyer_person_account",
      contactClass: contact.class,
    };
  }

  // Functional desk + named org + relevant role path (Bethesda meetings/housing desks)
  if (
    org &&
    contact.class === BUYER_CONTACT_PATH_CLASS.NAMED_BUYER_ROLE_PATH &&
    contact.readyEligible
  ) {
    return {
      class: ACCOUNT_QUALITY_CLASS.TRUE_BUYER_ACCOUNT,
      readyEligible: true,
      reason: "named_org_role_path_account",
      contactClass: contact.class,
    };
  }

  if (org && !contact.readyEligible) {
    return {
      class: ACCOUNT_QUALITY_CLASS.GENERIC_ORG_SHELL,
      readyEligible: false,
      reason: "named_org_without_buyer_path",
      contactClass: contact.class,
    };
  }

  return {
    class: ACCOUNT_QUALITY_CLASS.UNKNOWN,
    readyEligible: false,
    reason: "unclassified",
    contactClass: contact.class,
  };
}

export function meetsReadyAccountRequirement(opp = {}) {
  const c = classifyAccountQuality(opp);
  return {
    ok: c.readyEligible === true && TRUE_READY_CLASSES.has(c.class),
    class: c.class,
    reason: c.reason,
    detail: c,
  };
}

export function isTrueReadyAccountClass(cls = "") {
  return TRUE_READY_CLASSES.has(String(cls || ""));
}

/**
 * Customer-safe segment / meta (no CHILD ACCOUNT / generator jargon).
 */
export function customerSafeSegment(opp = {}) {
  const role = String(opp.participationRole || opp.childEntityType || "")
    .replace(/_/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (role && !/child account|demand campaign|generator|overflow/i.test(role)) {
    return role;
  }
  const buyerRole = String(opp.primaryContactRole || opp.buyerRole || "")
    .replace(/\s+/g, " ")
    .trim();
  if (buyerRole && !/overflow/i.test(buyerRole)) return buyerRole;
  return "";
}

function humanEventName(opp = {}) {
  const canon = String(opp.canonicalEventName || "").trim();
  if (canon && !/^[a-z0-9_]+$/i.test(canon) && !/_/.test(canon)) return canon;
  const fromTitle = String(opp.title || "")
    .replace(/^[^—–-]+[—–-]\s*/, "")
    .replace(/\s*\([^)]+\)\s*$/, "")
    .trim();
  if (fromTitle && !/^[a-z0-9_]+$/i.test(fromTitle) && fromTitle.length > 4) {
    // Collapse repeated segments from prior bad title merges
    const parts = fromTitle.split(/\s*[—–-]\s*/).filter(Boolean);
    const seen = new Set();
    const deduped = [];
    for (const p of parts) {
      const key = p.toLowerCase().replace(/[^a-z0-9]+/g, "");
      if (key.length < 4 || seen.has(key)) continue;
      seen.add(key);
      deduped.push(p.trim());
    }
    if (deduped.length) return deduped.join(" — ");
    return fromTitle;
  }
  // Prefer readable campaign-ish name from title start when needed
  const org = String(opp.organizationName || "").trim();
  if (fromTitle) return fromTitle;
  return org ? `${org} event` : "the event";
}

/**
 * Concise sales description (no duplicated title/org loops / research jargon).
 * Structure: account role · evidence · hotel relevance · next action hint.
 */
export function buildCustomerAccountDescription(opp = {}) {
  const org = String(opp.organizationName || "").trim() || "Organization";
  const event = humanEventName(opp);
  const aq = classifyAccountQuality(opp);
  let motion = "group travel / lodging coordination";
  if (aq.class === ACCOUNT_QUALITY_CLASS.TRUE_ORGANIZER_HOUSING_ACCOUNT) {
    motion = "meetings / exhibitor-services lodging coordination for traveling participants";
  } else if (aq.class === ACCOUNT_QUALITY_CLASS.TRUE_BUYER_ACCOUNT) {
    motion = "exhibitor-services / housing coordination for traveling teams";
  } else if (aq.class === ACCOUNT_QUALITY_CLASS.TRUE_DELEGATION_ACCOUNT) {
    motion = "delegation lodging";
  } else if (aq.class === ACCOUNT_QUALITY_CLASS.TRUE_VENDOR_CREW_ACCOUNT) {
    motion = "production / vendor crew stays";
  } else if (aq.class === ACCOUNT_QUALITY_CLASS.TRUE_PARTICIPATING_ACCOUNT) {
    motion = "participant / exhibitor team travel";
  }
  const role =
    String(opp.primaryContactRole || opp.buyerRole || "").trim() ||
    "the published events / hospitality function";
  const fit = String(opp.summaryWhyHotel || opp.fitExplanation || "")
    .replace(/\s+/g, " ")
    .trim();
  const fitClause = fit
    ? fit
    : "the property is a relevant overflow / corridor option for traveling teams when housing is open.";
  return `${org} is connected to ${event} via ${role}. Public evidence supports ${motion}. Hotel relevance: ${fitClause} Confirm lodging arrangements and preferred-hotel / overflow status with that function — do not invent room counts.`
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Customer-facing title: Account — motion/event (strip internal role tags).
 */
export function buildCustomerAccountTitle(opp = {}) {
  const org = String(opp.organizationName || "").trim() || "Organization";
  let event = humanEventName(opp)
    .replace(/\s*\((ORGANIZER|EVENT ORGANIZER|SECRETARIAT|PARENT SOCIETY|UNIVERSITY HOST|VENUE OPERATOR|PROGRAM HOST)\)\s*$/i, "")
    .trim();
  // Avoid "Art Genève — Art Genève"
  const orgNorm = org.toLowerCase().replace(/[^a-z0-9]+/g, "");
  const eventNorm = event.toLowerCase().replace(/[^a-z0-9]+/g, "");
  if (!event || eventNorm === orgNorm || eventNorm.startsWith(orgNorm)) {
    const role = String(opp.primaryContactRole || opp.buyerRole || opp.participationRole || "")
      .replace(/_/g, " ")
      .trim();
    if (/exhibitor|housing|meetings|hospitality|accommodation/i.test(role)) {
      event = `${humanEventName(opp).replace(/\s*\([^)]+\)\s*$/, "").trim() || "event"} — ${role}`;
    } else if (/organizer|secretariat|event/i.test(String(opp.participationRole || ""))) {
      event = `${humanEventName(opp).replace(/\s*\([^)]+\)\s*$/, "").trim() || "event"} organizer lodging`;
    }
  }
  event = event
    .replace(/\s*\((ORGANIZER|EVENT ORGANIZER|SECRETARIAT|PARENT SOCIETY|UNIVERSITY HOST|VENUE OPERATOR)\)\s*/gi, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (!event) event = "group lodging opportunity";
  // De-dupe "Org — Org …" and repeated org segments in event clause
  if (event.toLowerCase().startsWith(org.toLowerCase() + " ")) {
    event = event.slice(org.length).replace(/^[\s—–-]+/, "").trim();
  }
  const orgSeg = org.split(/\s*[—–-]\s*/)[0].trim();
  if (orgSeg && event.toLowerCase().startsWith(orgSeg.toLowerCase())) {
    event = event.slice(orgSeg.length).replace(/^[\s—–-]+/, "").trim();
  }
  if (!event) event = "group lodging opportunity";
  return `${org} — ${event}`;
}

/**
 * Hide homepage/general reservation from customer footer when not a sales path.
 */
export function customerSafeContactPath(opp = {}) {
  const contact = classifyBuyerContactPath(opp);
  if (
    contact.class === BUYER_CONTACT_PATH_CLASS.SOURCE_PAGE ||
    contact.class === BUYER_CONTACT_PATH_CLASS.GENERAL_ORG_CONTACT ||
    contact.class === BUYER_CONTACT_PATH_CLASS.NO_CONTACT
  ) {
    return null;
  }
  return contact.contactUrl || opp.publicContactPath || null;
}
