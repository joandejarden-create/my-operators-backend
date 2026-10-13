/**
 * Buyer / contact-path taxonomy for GDI commercial readiness.
 * Distinguishes evidence pages from sales-usable contact paths.
 * Does not invent buyers or people.
 */

import { isLikelyPersonName } from "./contact-resolution.js";

export const BUYER_CONTACT_PATH_CLASS = Object.freeze({
  SOURCE_PAGE: "SOURCE_PAGE",
  GENERAL_ORG_CONTACT: "GENERAL_ORG_CONTACT",
  RELEVANT_FUNCTION_CONTACT: "RELEVANT_FUNCTION_CONTACT",
  NAMED_BUYER_ROLE_PATH: "NAMED_BUYER_ROLE_PATH",
  NAMED_BUYER_PERSON: "NAMED_BUYER_PERSON",
  NO_CONTACT: "NO_CONTACT",
});

const RELEVANT_ROLE_RE =
  /\b(housing|hotel\s*reservation|accommodations?|exhibitor\s*services?|events?|meetings?|conference\s*services?|delegation|travel|group\s*sales|hospitality|procurement|sourcing|secretariat|fair\s*management|fair\s*operations?|fair\s*ops|field\s*marketing|field\s*operations?|field\s*ops|event\s*operations?|event\s*ops|production\s*management|activations?|corporate\s*events?|guest\s*services?|programme|program\s*admin|governing\s*bodies|secretar[ií]a(?:\s+t[eé]cnica)?|alojamiento|unterkunft|kongress|tagung|einkauf|contrataci[oó]n|pco|dmc|agencia\s+de\s+eventos|veranstaltungsagentur)\b/i;

/** Roles that assert a lodging desk — require housing path evidence, not homepage alone. */
const HOUSING_ROLE_RE =
  /\b(housing|hotel\s*reservation|accommodations?|room\s*block|hotel\s*desk|alojamiento|unterkunft|hotelkontingent|zimmerkontingent|bloque\s*de\s*habitaciones)\b/i;

const HOMEPAGE_RE =
  /^https?:\/\/[^/]+\/?(?:en\/|es\/|de\/|gl\/|fr\/)?(?:home\/?)?(?:index\.(?:html?|php|aspx))?$/i;

/** Functional path segments — EN + ES/DE/GL equivalents (same Ready standard, more proof forms). */
const FUNCTIONAL_PATH_RE =
  /\/(contact|contacts|housing|hotels?|accommodation|exhibitors?|services?|events?|meetings?|reservations?|delegates?|press|media|group|rfp|planner|when-where|discover-events|edition|alojamiento|hoteles|reserva|unterkunft|hotelbuchung|hotelkontingent|zimmerkontingent|partnerhotel|kontakt|anmeldung|inscripcion|inscripci[oó]n)/i;

function normalizeUrl(u = "") {
  try {
    const url = new URL(String(u).trim());
    return `${url.origin}${url.pathname}`.replace(/\/+$/, "") || url.origin;
  } catch {
    return String(u || "").trim().replace(/\/+$/, "");
  }
}

/**
 * Public lodging / housing-path evidence (not speculative desk stamps).
 */
export function hasLodgingContactEvidence(opp = {}) {
  if (opp.officialHousingUrl && /^https?:\/\//i.test(String(opp.officialHousingUrl))) {
    return true;
  }
  const blob = `${opp.lodgingEvidence || ""} ${opp.housingEvidence || ""} ${opp.roomDemandEvidence || ""}`;
  // Positive lodging evidence only — reject negated phrasing ("official housing not confirmed")
  if (
    !/\b(not|no|without|unconfirmed|unproven)\b.{0,40}\b(official housing|housing partner|room block)/i.test(
      blob
    ) &&
    !/\b(official housing|housing partner|room block).{0,40}\b(not confirmed|unconfirmed|unproven|not yet)/i.test(
      blob
    ) &&
    /official housing|hotel reservation page|housing partner|room block|housing list|passkey|onpeak|hotel programme|hotel program|hotel oficial|hoteles recomendados|offizielles hotel|partnerhotel|hotelkontingent|zimmerkontingent|bloque de habitaciones|alojamiento oficial|gestión de alojamiento/i.test(
      blob
    )
  ) {
    return true;
  }
  const housing = String(opp.housingStatus || "").toUpperCase();
  if (housing === "STRONG" || housing === "VERIFIED") return true;
  const room = String(opp.roomDemandStatus || "").toUpperCase();
  // Verified / published programs only — OVERFLOW_ONLY alone is not lodging-path evidence
  if (/VERIFIED_HOUSING|PUBLISHED_ROOM|CONFIRMED_BLOCK/.test(room)) return true;
  return false;
}

function isFunctionalDeskName(name = "") {
  const n = String(name || "").trim();
  if (!n || isLikelyPersonName(n)) return false;
  return /\b(staff|services|desk|office|secretariat|committee|information|housing|accommodations?|travel|venue|meetings?|liaison|secretar[ií]a|alojamiento|kongressbüro|teilnehmermanagement)\b/i.test(
    n
  );
}

/**
 * Classify a URL alone (page-level).
 * @param {string} url
 * @param {{ officialSource?: string, discoverySource?: string }} [ctx]
 */
export function classifyContactUrl(url = "", ctx = {}) {
  const u = String(url || "").trim();
  if (!u || !/^https?:\/\//i.test(u)) return BUYER_CONTACT_PATH_CLASS.NO_CONTACT;

  const evidenceUrls = [ctx.officialSource, ctx.discoverySource]
    .map(normalizeUrl)
    .filter(Boolean);
  const nu = normalizeUrl(u);

  if (evidenceUrls.includes(nu) && !FUNCTIONAL_PATH_RE.test(u)) {
    // Same as event/source landing — evidence, not sales contact
    if (HOMEPAGE_RE.test(u) || /\/(en\/)?home\/?$/i.test(u) || !FUNCTIONAL_PATH_RE.test(u)) {
      return BUYER_CONTACT_PATH_CLASS.SOURCE_PAGE;
    }
  }

  if (FUNCTIONAL_PATH_RE.test(u)) {
    return BUYER_CONTACT_PATH_CLASS.RELEVANT_FUNCTION_CONTACT;
  }
  if (HOMEPAGE_RE.test(u) || /\/(en\/)?home\/?$/i.test(u)) {
    return BUYER_CONTACT_PATH_CLASS.GENERAL_ORG_CONTACT;
  }
  // Bare org root / shallow path without functional segment
  const path = (() => {
    try {
      return new URL(u).pathname.replace(/\/+$/, "");
    } catch {
      return "";
    }
  })();
  if (!path || path === "" || path === "/en" || path === "/fr" || path === "/de") {
    return BUYER_CONTACT_PATH_CLASS.GENERAL_ORG_CONTACT;
  }
  return BUYER_CONTACT_PATH_CLASS.GENERAL_ORG_CONTACT;
}

/**
 * Commercial contact classification for an opportunity.
 * Combines URL class + buyer role/person evidence.
 */
export function classifyBuyerContactPath(opp = {}) {
  const person =
    opp.primaryContact?.name ||
    opp.primaryContact?.fullName ||
    opp.namedBuyer ||
    opp.contactName ||
    "";
  const role = String(
    opp.primaryContactRole ||
      opp.buyerRole ||
      opp.primaryContact?.role ||
      opp.preferredWhoRoles?.[0] ||
      ""
  ).trim();
  const buyerEntity = String(
    opp.buyerEntity || opp.organizer || opp.organizationName || opp.company || ""
  ).trim();
  const contactUrl =
    opp.publicContactPath ||
    opp.organizationContactUrl ||
    opp.officialContactPath ||
    opp.contactOfficialUrl ||
    opp.primaryContact?.sourceUrl ||
    "";
  const urlClass = classifyContactUrl(contactUrl, {
    officialSource: opp.officialSource,
    discoverySource: opp.discoverySource,
  });
  const lodgingEvidence = hasLodgingContactEvidence(opp);
  const housingRole = HOUSING_ROLE_RE.test(role) || HOUSING_ROLE_RE.test(buyerEntity);
  const weakUrlAlone =
    urlClass === BUYER_CONTACT_PATH_CLASS.SOURCE_PAGE ||
    urlClass === BUYER_CONTACT_PATH_CLASS.GENERAL_ORG_CONTACT ||
    urlClass === BUYER_CONTACT_PATH_CLASS.NO_CONTACT;
  const explicitHomepageOrSource =
    Boolean(contactUrl) &&
    (urlClass === BUYER_CONTACT_PATH_CLASS.SOURCE_PAGE ||
      urlClass === BUYER_CONTACT_PATH_CLASS.GENERAL_ORG_CONTACT);

  if (isLikelyPersonName(person)) {
    return {
      class: BUYER_CONTACT_PATH_CLASS.NAMED_BUYER_PERSON,
      urlClass,
      buyerEntity,
      buyerRole: role,
      contactUrl,
      readyEligible: true,
      reason: "named_buyer_person",
    };
  }

  // Functional URL is a sales path even without a named person
  if (urlClass === BUYER_CONTACT_PATH_CLASS.RELEVANT_FUNCTION_CONTACT) {
    return {
      class: BUYER_CONTACT_PATH_CLASS.RELEVANT_FUNCTION_CONTACT,
      urlClass,
      buyerEntity,
      buyerRole: role,
      contactUrl,
      readyEligible: true,
      reason: "relevant_function_url",
    };
  }

  const roleRelevant =
    RELEVANT_ROLE_RE.test(role) ||
    RELEVANT_ROLE_RE.test(buyerEntity) ||
    isFunctionalDeskName(person);
  if (buyerEntity && roleRelevant) {
    // Explicit venue/event homepage + Housing desk stamp without housing evidence → not READY
    if (housingRole && explicitHomepageOrSource && !lodgingEvidence) {
      return {
        class: BUYER_CONTACT_PATH_CLASS.GENERAL_ORG_CONTACT,
        urlClass,
        buyerEntity,
        buyerRole: role,
        contactUrl,
        readyEligible: false,
        reason: "housing_role_without_housing_path",
      };
    }
    return {
      class: BUYER_CONTACT_PATH_CLASS.NAMED_BUYER_ROLE_PATH,
      urlClass,
      buyerEntity,
      buyerRole: role || (isFunctionalDeskName(person) ? person : ""),
      contactUrl,
      readyEligible: true,
      reason: isFunctionalDeskName(person)
        ? "named_org_plus_functional_desk"
        : "named_org_plus_relevant_role",
      urlInsufficientAlone: weakUrlAlone,
    };
  }

  if (urlClass === BUYER_CONTACT_PATH_CLASS.SOURCE_PAGE) {
    return {
      class: BUYER_CONTACT_PATH_CLASS.SOURCE_PAGE,
      urlClass,
      buyerEntity,
      buyerRole: role,
      contactUrl,
      readyEligible: false,
      reason: "source_page_not_sales_contact",
    };
  }

  if (urlClass === BUYER_CONTACT_PATH_CLASS.GENERAL_ORG_CONTACT) {
    return {
      class: BUYER_CONTACT_PATH_CLASS.GENERAL_ORG_CONTACT,
      urlClass,
      buyerEntity,
      buyerRole: role,
      contactUrl,
      readyEligible: false,
      reason: "general_org_contact_alone_insufficient",
    };
  }

  return {
    class: BUYER_CONTACT_PATH_CLASS.NO_CONTACT,
    urlClass,
    buyerEntity,
    buyerRole: role,
    contactUrl,
    readyEligible: false,
    reason: "no_usable_contact_path",
  };
}

/**
 * READY contact requirement — does not lower thresholds.
 * Accepts RELEVANT_FUNCTION / NAMED_BUYER_ROLE_PATH / NAMED_BUYER_PERSON only.
 */
export function meetsReadyContactRequirement(opp = {}) {
  const c = classifyBuyerContactPath(opp);
  return {
    ok: c.readyEligible === true,
    class: c.class,
    reason: c.reason,
    detail: c,
  };
}

/**
 * Strip unsupported competitor-hotel claims from customer thesis text.
 */
export function sanitizeCustomerThesis(text = "") {
  let t = String(text || "");
  t = t.replace(/\s*Fit:PLAUSIBLE_FIT\s*/gi, " ");
  t = t.replace(
    /Public traces associate [^.]+ with competitor hotel\(s\):\s*UNKNOWN\.?/gi,
    ""
  );
  t = t.replace(
    /shows public competitor-hotel lodging\/event association[^.]*\./gi,
    "is a named venue / account under a published Geneva event cycle."
  );
  t = t.replace(/\s{2,}/g, " ").trim();
  return t;
}

/**
 * Detect sports/tournament template leakage on non-sports opportunities.
 */
export function hasTournamentActionLeak(opp = {}) {
  const action = String(opp.recommendedAction || opp.recommendedNextStep || "");
  const blob = [
    opp.title,
    opp.organizationName,
    opp.opportunityType,
    opp.demandType,
    opp.participationRole,
    opp.baseOfDemand,
  ]
    .map((x) => String(x || ""))
    .join(" ");
  const sports =
    /\b(CHI|hippique|tournament|sports?|rider|team hotel|football|hockey)\b/i.test(blob);
  if (sports) return false;
  return /\btournament\s+housing\b/i.test(action);
}

/**
 * Evidence-based recommended action by opportunity shape (no invented contacts).
 */
export function recommendedActionForCommercialShape(opp = {}) {
  const role = String(opp.primaryContactRole || opp.buyerRole || "").toLowerCase();
  const blob = `${opp.title || ""} ${opp.organizationName || ""} ${opp.participationRole || ""}`;
  if (/\bart\b|gallery|fair/i.test(blob)) {
    return "Verify whether the fair/venue publishes an accommodation or hotel-partnership path; contact exhibitor services / event operations if a public function path exists — do not invent lodging demand.";
  }
  if (/housing|hotel reservation|accommodation/i.test(role)) {
    return "Contact the published housing / hotel-reservation function (not only the event brand homepage), confirm whether overflow listings are open, and validate stay dates — do not invent room counts.";
  }
  if (/exhibitor/i.test(role) || /exhibitor/i.test(blob)) {
    return "Contact exhibitor services / field-marketing / travel function via a public path if available; confirm housing or hotel-list status before pursuit.";
  }
  if (/delegation|secretariat|ECOSOC|OCHA|WHO|UN\b/i.test(blob + role)) {
    return "Contact the secretariat / conference-support / delegation travel function via a public path if available; treat member-state lodging as separate unless named.";
  }
  if (/meeting|events|secretariat|programme|program/i.test(role)) {
    return "Contact the events / meetings / secretariat function via a public path; confirm lodging motion before treating as customer-ready.";
  }
  return "Validate the public buyer/housing path and lodging evidence before active pursuit; do not sell on organization existence alone.";
}
