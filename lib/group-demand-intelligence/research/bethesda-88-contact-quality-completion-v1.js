/**
 * Bethesda 88 — contact quality completion V1.
 *
 * Audit all 88 (59 canonical + 29 historical) against GDI contact eligibility.
 * Recover prior research, apply zero-paid public contact evidence to historical
 * research-stage bodies, and emit dry-run patches for canonical opportunities.json
 * (never write the store without explicit founder approval).
 *
 * No paid providers. No Sales Ready promotion from contact enrichment alone.
 * Never clobber a verified named person with a placeholder / empty value.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { looksLikePersonName } from "../secondary-named-contact-v1.js";
import {
  buildContactDisplaySurface,
  isGenericOrganizationEmail,
} from "../contact-display-surface-v1.js";
import { CONTACT_ELIGIBILITY_CLASS } from "../contact-eligibility-standard-v1.js";
import {
  BETHESDA_CANONICAL_HOTEL_ID,
  BETHESDA_CANONICAL_HOTEL_NAME,
} from "./bethesda-full-demand-database-reconciliation-v1.js";
import { BETHESDA_29_ATTESTED_IDS } from "./bethesda-historical-attestation-bag-v1.js";
import { mergeBethesdaInternalResearchOpportunities } from "./bethesda-internal-research-union-v1.js";
import {
  loadHotelOpportunityDoc,
  ACCEPTABLE_59_ROW_SHA256S,
  BETHESDA_PROTECTED_SCALING_IDS,
} from "./gdi-hotel-store-integrity-guard-v1.js";
import {
  persistResearchStageOpportunityBag,
  researchStageHotelDir,
  writeJsonAtomic,
} from "./gdi-research-stage-persistence-v1.js";
import { applyHistoricalCommercialCategoryProjection } from "./bethesda-historical-commercial-category-map-v1.js";

export const BETHESDA_88_CONTACT_QUALITY_COMPLETION_V1 =
  "gdi-bethesda-88-contact-quality-completion-v1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const REPORT_DIR_REL = "reports/gdi-bethesda-88-contact-quality-completion-v1";
const FIXTURE_REL = "fixtures/gdi/bethesda-29-historical-reconstruction-v1.json";

export const COVERAGE_BUCKET = Object.freeze({
  A: "A_NAMED_PLUS_DIRECT_ROUTE",
  B: "B_NAMED_PLUS_ORG_ROUTE",
  C: "C_NAMED_NO_ROUTE",
  D: "D_FUNCTIONAL_ORG_ONLY",
  E: "E_UNSUITABLE_OR_PLACEHOLDER",
  F: "F_NO_MEANINGFUL_CONTACT",
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function isPlaceholderName(name) {
  const n = text(name);
  if (!n) return true;
  if (/^unknown$/i.test(n)) return true;
  if (/buyer not yet identified/i.test(n)) return true;
  if (/organization contact path|official organization route|no secondary contact/i.test(n)) {
    return true;
  }
  if (
    /conference staff|tournaments? desk|hotel information|events? page|contact desk|organization contact|support branch|ric help|helpdesk|webmaster/i.test(
      n
    )
  ) {
    return true;
  }
  if (
    /\b(staff|desk|registration|housing|committee|department|team|office|bureau)\b/i.test(n) &&
    !/\b(director|manager|coordinator|planner|president|officer)\b/i.test(n)
  ) {
    // "NADO conference staff" / bare "Registration" — not a verified person.
    return true;
  }
  if (/^https?:\/\//i.test(n) || /\bwww\./i.test(n)) return true;
  return false;
}

function isVerifiedNamedPerson(name) {
  const n = text(name);
  if (!n || isPlaceholderName(n)) return false;
  if (!looksLikePersonName(n)) return false;
  // Reject org/venue strings that occasionally pass the heuristic.
  if (
    /\b(mansion|marriott|hotel|club|association|council|chamber|society|information|accommodations?)\b/i.test(
      n
    )
  ) {
    return false;
  }
  return true;
}

export function extractContactSnapshot(row = {}) {
  const surface = buildContactDisplaySurface(row);
  const p = row.primaryContact && typeof row.primaryContact === "object" ? row.primaryContact : {};
  const s =
    row.secondaryContact && typeof row.secondaryContact === "object"
      ? row.secondaryContact
      : {};
  const cr = row.contactRecovery && typeof row.contactRecovery === "object" ? row.contactRecovery : {};

  const name = text(
    surface.displayName ||
      p.name ||
      row.primaryContactName ||
      row.contactName ||
      (row.whoRecord && row.whoRecord.name)
  );
  const named =
    isVerifiedNamedPerson(name) ||
    (cr.namedPerson === true && isVerifiedNamedPerson(text(name) || text(cr.raw)));
  const title = text(
    surface.role ||
      p.title ||
      p.role ||
      row.primaryContactRole ||
      row.contactTitle ||
      row.contactRole
  );
  const ownershipOrg =
    text(p.emailOwnership).toLowerCase() === "organization_or_department" ||
    text(p.emailOwnership).toLowerCase() === "role_inbox" ||
    p.directPersonEmail === false;
  const rawPrimaryEmail = text(
    p.email || row.primaryContactEmail || row.contactEmail || surface.personEmail
  );
  const treatAsOrgEmail =
    ownershipOrg ||
    isGenericOrganizationEmail(rawPrimaryEmail) ||
    isGenericOrganizationEmail(p.email) ||
    isGenericOrganizationEmail(row.primaryContactEmail) ||
    isGenericOrganizationEmail(row.contactEmail);
  const email = text(
    !treatAsOrgEmail
      ? surface.personEmail ||
          (!isGenericOrganizationEmail(p.email) ? p.email : null) ||
          (!isGenericOrganizationEmail(row.primaryContactEmail)
            ? row.primaryContactEmail
            : null) ||
          (!isGenericOrganizationEmail(row.contactEmail) ? row.contactEmail : null)
      : null
  );
  let orgEmail = text(
    surface.organizationEmail ||
      row.functionalContactEmail ||
      row.organizationContactEmail ||
      (treatAsOrgEmail ? rawPrimaryEmail || p.email || row.contactEmail : null) ||
      (isGenericOrganizationEmail(p.email) ? p.email : null) ||
      (isGenericOrganizationEmail(cr.email) ? cr.email : null) ||
      (isGenericOrganizationEmail(row.contactEmail) ? row.contactEmail : null)
  );
  const linkedin = text(p.linkedin || p.linkedinUrl || row.primaryContactLinkedIn);
  const phone = text(
    surface.personPhone ||
      p.phone ||
      row.primaryContactPhone ||
      surface.companyPhone ||
      row.companyPhone ||
      row.contactPhone
  );
  let orgRoute = text(
    p.organizationContactRoute ||
      row.contactOfficialUrl ||
      row.contactUrl ||
      surface.contactUrl ||
      orgEmail
  );
  // Prefer email-shaped organization routes in orgEmail for coverage reporting.
  if (!orgEmail && orgRoute && orgRoute.includes("@")) {
    orgEmail = orgRoute;
  }
  if (!orgRoute && orgEmail) orgRoute = orgEmail;
  const contactClass = text(
    p.contactClass ||
      cr.contactClass ||
      row.contactEligibilityClass ||
      row.commercialContactPath
  );
  const secondaryName = text(s.name || row.secondaryContactName);
  const secondaryNamed = isVerifiedNamedPerson(secondaryName);
  const hasDirectEmail = Boolean(
    named &&
      email &&
      email.includes("@") &&
      !isGenericOrganizationEmail(email) &&
      !ownershipOrg
  );
  const hasOrgRoute =
    Boolean(orgRoute) ||
    Boolean(orgEmail) ||
    cr.usableRoute === true ||
    (ownershipOrg && Boolean(rawPrimaryEmail));
  const placeholder = isPlaceholderName(name) && Boolean(text(name));

  let coverage = COVERAGE_BUCKET.F;
  if (named && hasDirectEmail) coverage = COVERAGE_BUCKET.A;
  else if (named && hasOrgRoute) coverage = COVERAGE_BUCKET.B;
  else if (named) coverage = COVERAGE_BUCKET.C;
  else if (orgEmail || (hasOrgRoute && !named)) coverage = COVERAGE_BUCKET.D;
  else if (placeholder || (text(name) && !named)) coverage = COVERAGE_BUCKET.E;

  return {
    name: named ? name : null,
    displayName: text(name) || null,
    title: title || null,
    email: hasDirectEmail ? email : null,
    orgEmail: orgEmail || null,
    linkedin: linkedin || null,
    phone: phone || null,
    orgRoute: orgRoute || null,
    contactClass: contactClass || null,
    named,
    hasDirectEmail,
    hasOrgRoute,
    secondaryNamed,
    secondaryName: secondaryNamed ? secondaryName : null,
    secondaryEmail: text(s.email) || null,
    coverage,
    surfaceKind: surface.kind || null,
    whyThisContact: text(row.whyThisContact || p.whyRelevant) || null,
    sourceUrl: text(row.contactSourceUrl || p.sourceUrl) || null,
    verifiedAt: text(row.contactVerifiedAt || p.verifiedAt) || null,
  };
}

export function classifyCoverageBucket(snapshot) {
  return snapshot?.coverage || COVERAGE_BUCKET.F;
}

/**
 * Never replace a verified named person with empty / placeholder / weaker generic data.
 */
export function mergeContactAntiClobber(existing = {}, incoming = {}) {
  const before = extractContactSnapshot(existing);
  const nextPrimary = { ...(existing.primaryContact || {}) };
  const incPrimary = incoming.primaryContact || {};

  const incomingName = text(incPrimary.name);
  const incomingNamed = isVerifiedNamedPerson(incomingName);

  if (before.named) {
    // Preserve name/title unless incoming is a verified named person (upgrade allowed).
    if (incomingNamed) {
      nextPrimary.name = incomingName;
      if (text(incPrimary.title || incPrimary.role)) {
        nextPrimary.title = text(incPrimary.title || incPrimary.role);
      }
      if (text(incPrimary.contactClass)) nextPrimary.contactClass = text(incPrimary.contactClass);
    }
  } else if (incomingNamed) {
    nextPrimary.name = incomingName;
    nextPrimary.title = text(incPrimary.title || incPrimary.role) || nextPrimary.title || null;
    nextPrimary.contactClass =
      text(incPrimary.contactClass) ||
      CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT;
  }

  // Emails: add person email if missing; never overwrite person email with generic/role inbox.
  const incEmail = text(incPrimary.email);
  const incAsOrgRoute =
    text(incPrimary.emailOwnership).toLowerCase() === "organization_or_department" ||
    text(incPrimary.emailOwnership).toLowerCase() === "role_inbox" ||
    incPrimary.directPersonEmail === false ||
    text(incPrimary.contactClass) === "FUNCTIONAL_INBOX" ||
    isGenericOrganizationEmail(incEmail);
  // Always honor explicit ownership flags from verified evidence (even when email is null).
  if (text(incPrimary.emailOwnership)) {
    nextPrimary.emailOwnership = text(incPrimary.emailOwnership);
  }
  if (incPrimary.directPersonEmail === false) {
    nextPrimary.directPersonEmail = false;
  }
  if (incEmail && incEmail.includes("@")) {
    if (incAsOrgRoute) {
      if (!text(nextPrimary.organizationContactRoute)) {
        nextPrimary.organizationContactRoute = incEmail;
      }
      // Do not store role/org inboxes as person-direct email.
      if (
        !text(nextPrimary.email) ||
        isGenericOrganizationEmail(nextPrimary.email) ||
        text(nextPrimary.email).toLowerCase() === incEmail.toLowerCase()
      ) {
        nextPrimary.email = null;
      }
    } else {
      // Verified person-direct email: upgrade over empty OR leftover functional/org inboxes.
      const existingEmail = text(nextPrimary.email);
      const existingIsFunctional =
        !existingEmail ||
        isGenericOrganizationEmail(existingEmail) ||
        text(nextPrimary.organizationContactRoute).toLowerCase() ===
          existingEmail.toLowerCase() ||
        text(nextPrimary.emailOwnership).toLowerCase() === "organization_or_department" ||
        nextPrimary.directPersonEmail === false;
      if (existingIsFunctional) {
        if (
          existingEmail &&
          isGenericOrganizationEmail(existingEmail) &&
          !text(nextPrimary.organizationContactRoute)
        ) {
          nextPrimary.organizationContactRoute = existingEmail;
        }
        nextPrimary.email = incEmail;
        nextPrimary.directPersonEmail = true;
        if (
          text(nextPrimary.emailOwnership).toLowerCase() === "organization_or_department" ||
          text(nextPrimary.emailOwnership).toLowerCase() === "role_inbox"
        ) {
          delete nextPrimary.emailOwnership;
        }
      } else if (!existingEmail) {
        nextPrimary.email = incEmail;
        nextPrimary.directPersonEmail = true;
      }
      // Never overwrite a different verified person-direct email with another.
    }
  }
  if (text(incPrimary.organizationContactRoute)) {
    const incomingOrg = text(incPrimary.organizationContactRoute);
    if (!text(nextPrimary.organizationContactRoute)) {
      nextPrimary.organizationContactRoute = incomingOrg;
    } else if (
      isGenericOrganizationEmail(nextPrimary.organizationContactRoute) === false &&
      isGenericOrganizationEmail(incomingOrg)
    ) {
      // Prefer explicit functional org route when present.
      nextPrimary.organizationContactRoute = incomingOrg;
    }
    // If the existing person email is actually the org/role route, demote it.
    const orgRouteNow = text(nextPrimary.organizationContactRoute).toLowerCase();
    if (
      text(nextPrimary.email).toLowerCase() === orgRouteNow ||
      (isGenericOrganizationEmail(nextPrimary.email) &&
        text(incPrimary.email) &&
        !incAsOrgRoute)
    ) {
      if (isGenericOrganizationEmail(nextPrimary.email) && !orgRouteNow) {
        nextPrimary.organizationContactRoute = text(nextPrimary.email);
      }
      if (!incAsOrgRoute && text(incPrimary.email)) {
        nextPrimary.email = text(incPrimary.email);
        nextPrimary.directPersonEmail = true;
      } else if (text(nextPrimary.email).toLowerCase() === orgRouteNow) {
        nextPrimary.email = null;
      }
    }
  }
  if (text(incPrimary.title || incPrimary.role) && !incomingNamed && !before.named) {
    nextPrimary.title = text(incPrimary.title || incPrimary.role);
  }
  if (text(incPrimary.contactClass) && !before.named) {
    nextPrimary.contactClass = text(incPrimary.contactClass);
  }
  if (text(incPrimary.phone) && !text(nextPrimary.phone)) nextPrimary.phone = text(incPrimary.phone);
  if (text(incPrimary.linkedin) && !text(nextPrimary.linkedin)) {
    nextPrimary.linkedin = text(incPrimary.linkedin);
  }
  if (text(incPrimary.sourceUrl)) nextPrimary.sourceUrl = text(incPrimary.sourceUrl);
  if (text(incPrimary.verifiedAt)) nextPrimary.verifiedAt = text(incPrimary.verifiedAt);
  if (text(incPrimary.whyRelevant)) nextPrimary.whyRelevant = text(incPrimary.whyRelevant);

  const namedNow = isVerifiedNamedPerson(nextPrimary.name);
  const ownershipOrgNow =
    text(nextPrimary.emailOwnership).toLowerCase() === "organization_or_department" ||
    text(nextPrimary.emailOwnership).toLowerCase() === "role_inbox" ||
    nextPrimary.directPersonEmail === false ||
    text(nextPrimary.contactClass) === "FUNCTIONAL_INBOX";
  if (ownershipOrgNow && text(nextPrimary.email)) {
    // Keep role/org inboxes on organizationContactRoute only.
    if (!text(nextPrimary.organizationContactRoute)) {
      nextPrimary.organizationContactRoute = text(nextPrimary.email);
    }
    nextPrimary.email = null;
  }
  // If a verified person-direct email was previously parked only as org route,
  // promote it to primaryContact.email and drop the duplicate org copy.
  if (
    nextPrimary.directPersonEmail === true &&
    text(incPrimary.email) &&
    !isGenericOrganizationEmail(incPrimary.email) &&
    text(nextPrimary.organizationContactRoute).toLowerCase() ===
      text(incPrimary.email).toLowerCase()
  ) {
    nextPrimary.email = text(incPrimary.email);
    nextPrimary.organizationContactRoute = null;
    if (
      text(nextPrimary.emailOwnership).toLowerCase() === "organization_or_department" ||
      text(nextPrimary.emailOwnership).toLowerCase() === "role_inbox"
    ) {
      delete nextPrimary.emailOwnership;
    }
  }
  const personEmail =
    text(nextPrimary.email) &&
    !isGenericOrganizationEmail(nextPrimary.email) &&
    !ownershipOrgNow
      ? text(nextPrimary.email)
      : null;
  const orgRoute =
    text(nextPrimary.organizationContactRoute) ||
    (isGenericOrganizationEmail(nextPrimary.email) || ownershipOrgNow
      ? text(nextPrimary.email) || text(nextPrimary.organizationContactRoute)
      : null);

  let secondary = existing.secondaryContact || null;
  if (incoming.secondaryContact && isVerifiedNamedPerson(incoming.secondaryContact.name)) {
    if (!secondary || !isVerifiedNamedPerson(secondary.name)) {
      secondary = { ...incoming.secondaryContact };
    }
  }

  const contactRecovery = {
    ...(existing.contactRecovery || {}),
    namedPerson: namedNow,
    usableRoute: Boolean(personEmail || orgRoute),
    email: personEmail || orgRoute || (existing.contactRecovery && existing.contactRecovery.email) || null,
    contactClass:
      text(nextPrimary.contactClass) ||
      (namedNow
        ? CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT
        : orgRoute
          ? "FUNCTIONAL_INBOX"
          : existing.contactRecovery?.contactClass || null),
    raw: namedNow ? nextPrimary.name : personEmail || orgRoute || existing.contactRecovery?.raw || null,
    note: text(incoming.contactRecovery?.note) || existing.contactRecovery?.note || null,
  };

  return {
    ...existing,
    primaryContact: Object.keys(nextPrimary).length ? nextPrimary : existing.primaryContact || null,
    secondaryContact: secondary,
    contactRecovery,
    primaryContactName: namedNow ? nextPrimary.name : existing.primaryContactName || null,
    primaryContactRole: text(nextPrimary.title) || existing.primaryContactRole || null,
    primaryContactEmail: personEmail || existing.primaryContactEmail || null,
    primaryContactPhone: text(nextPrimary.phone) || existing.primaryContactPhone || null,
    functionalContactEmail: orgRoute && isGenericOrganizationEmail(orgRoute) ? orgRoute : existing.functionalContactEmail || null,
    organizationContactEmail:
      orgRoute && isGenericOrganizationEmail(orgRoute)
        ? orgRoute
        : existing.organizationContactEmail || null,
    contactSourceUrl: text(nextPrimary.sourceUrl) || existing.contactSourceUrl || null,
    contactVerifiedAt: text(nextPrimary.verifiedAt) || existing.contactVerifiedAt || null,
    whyThisContact: text(nextPrimary.whyRelevant) || existing.whyThisContact || null,
    salesReady: false,
    customerFacing: false,
    customerVisible: false,
    publicationApproved: false,
    publication: "NOT_AUTHORIZED",
  };
}

/**
 * Zero-paid public contact evidence gathered 2026-11-12 (Cursor WebSearch / official pages).
 * Only include verified public names/emails/phones — no invented patterns.
 */
export const ZERO_PAID_CONTACT_EVIDENCE_BY_ID = Object.freeze({
  gdi_e2d_bc0a94d7c6007f36: {
    inventory: "historical",
    primaryContact: {
      name: "Katherine Crosse",
      title: "Destination Sales Manager",
      email: "kcrosse@visitmontgomery.com",
      phone: "240-641-6748",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      whyRelevant:
        "Visit Montgomery destination sales owns group/hotel coordination for corporate, pharma, government and tour lodging — credible lodging partnership / referral entry point.",
      sourceUrl: "https://visitmontgomery.com/groups/",
      verifiedAt: "2026-11-12",
    },
    secondaryContact: {
      name: "Joe Doran",
      title: "Community Engagement & Partnership Manager",
      email: "jdoran@visitmontgomery.com",
      phone: "240-641-6752",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      sourceUrl: "https://visitmontgomery.com/about/meet-the-team/",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid public team directory + groups page 2026-11-12.",
    },
  },
  gdi_pep_live_af0711e711ba: {
    inventory: "historical",
    primaryContact: {
      name: "Kawania Wooten",
      title: "Principal / Event Planner — Howerton+Wooten Events",
      email: "kawania@hwevents.com",
      phone: "(202) 706-7953",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      organizationContactRoute: "sayhello@hwevents.com",
      whyRelevant:
        "Named principal of the planner agency; Destination DC listing publishes direct email for meetings/galas outreach.",
      sourceUrl: "https://washington.org/meetings/find-dc-listings/howertonwooten-events",
      verifiedAt: "2026-11-12",
    },
    secondaryContact: {
      name: null,
      title: "Agency intake",
      email: "sayhello@hwevents.com",
      contactClass: "FUNCTIONAL_INBOX",
      sourceUrl: "https://hwevents.com/contact",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid public planner contact 2026-11-12.",
    },
  },
  gdi_s4912_6ffc303bf55a: {
    inventory: "historical",
    primaryContact: {
      name: "Diane Kendrick",
      title: "Manager, Corporate Partnerships & Events",
      email: "diane.kendrick@nih.gov",
      phone: "301-451-6271",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      organizationContactRoute: "Events@childrensinn.org",
      whyRelevant:
        "Publicly listed corporate partnerships & events manager for An Evening for Hope gala sponsorship/venue planning.",
      sourceUrl:
        "https://resources.finalsite.net/images/v1775502373/childrensinnorg7/purfshtqjufvc1it32wb/Corporate-Partnership-Program-2025-2026.pdf",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid Children's Inn gala/sponsorship public contacts 2026-11-12. 2026 gala at JW Marriott; 2027 venue TBD.",
    },
  },
  gdi_s4912_6c8a62552bfa: {
    inventory: "historical",
    primaryContact: {
      name: "Amy Speck",
      title: "Event Manager — Maryland Tech Council",
      email: "amy@mdtechcouncil.com",
      phone: "(240) 831-4829",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      whyRelevant:
        "Listed event contact for MTC Industry Awards / ICON celebration hosted at Bethesda North Marriott — overflow lodging research entry point.",
      sourceUrl:
        "https://members.mdtechcouncil.com/calendar/Details/mtc-s-industry-awards-celebration-2027-1978918?sourceTypeId=Website",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid MTC event listing 2026-11-12.",
    },
  },
  gdi_plp_live_fa2063dc1195: {
    inventory: "historical",
    primaryContact: {
      name: "Katy Linder",
      title: "Rental Director — Rock Creek Mansion",
      email: "KatyRCMansion@gmail.com",
      phone: "301-452-1922",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      whyRelevant:
        "Named rental director for the Bethesda private event venue — partnership/referral path for guestroom overflow from mansion events.",
      sourceUrl: "https://rockcreekmansion.com/contact/",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid Rock Creek Mansion contact page 2026-11-12.",
    },
  },
  gdi_plp_live_4958569d826e: {
    inventory: "historical",
    primaryContact: {
      name: "Melanie Goff",
      title: "Corporate Events Manager — The Cordell",
      email: null,
      emailOwnership: "organization_or_department",
      directPersonEmail: false,
      phone: "(301) 500-1177",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      organizationContactRoute: "info@thecordell.com",
      linkedin: "https://linkedin.com/in/melanie-goff-3169z",
      whyRelevant:
        "Named Corporate Events Manager for The Cordell (downtown Bethesda venue) — primary commercial entry for corporate/nonprofit venue rentals and partnership/referral lodging adjacency. Individual email not published; use info@thecordell.com.",
      sourceUrl: "https://thecordell.com/contact-the-cordell-downtown-bethesda-event-venue",
      verifiedAt: "2026-11-12",
    },
    secondaryContact: {
      name: null,
      title: "The Cordell venue inquiry desk",
      email: "info@thecordell.com",
      phone: "(301) 500-1177",
      contactClass: "FUNCTIONAL_INBOX",
      sourceUrl: "https://thecordell.com/contact-the-cordell-downtown-bethesda-event-venue",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid Cordell contact page + LinkedIn role verification 2026-11-12. Named person recovered; direct email not published.",
    },
  },
  disp_recLuxvwwxID7U2B8_linder_global_events: {
    inventory: "historical",
    primaryContact: {
      name: "Heather Guay Yoo",
      title: "Senior Director of Business Development",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      organizationContactRoute: "contactus@linderglobal.com",
      phone: "(202) 298-6970",
      whyRelevant:
        "Named BD leader on LINDER Global Events public team page — commercial entry for hotel/DMC partnership outreach; individual email not published.",
      sourceUrl: "https://linderglobal.com/about-linder-global-events/",
      verifiedAt: "2026-11-12",
    },
    secondaryContact: {
      name: "Rebecca Linder",
      title: "Founder + Chief Visionary Officer",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      organizationContactRoute: "contactus@linderglobal.com",
      sourceUrl: "https://linderglobal.com/about-linder-global-events/",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid LINDER team page 2026-11-12 — named persons via organization inbox route.",
    },
  },
  gdi_s4912_732496c2f8aa: {
    inventory: "historical",
    primaryContact: {
      name: "Randi Jacobs",
      title: "Symposium / Program Manager — CASSS CGTP",
      email: "rjacobs@casss.org",
      phone: "510-428-0740",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      organizationContactRoute: "info@casss.org",
      whyRelevant:
        "Public symposium manager for CGTP 2027 at Bethesda North Marriott; owns program/venue logistics and government-rate room inquiries — overflow lodging research contact.",
      sourceUrl: "https://www.casss.org/cell-gene-therapy-products/hotel-information",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid CASSS hotel-information page 2026-11-12. Host hotel is Bethesda North — overflow watch only.",
    },
  },
  disp_recLuxvwwxID7U2B8_parenteral_drug_association_pda_: {
    inventory: "historical",
    // Preserve Laura Keeley (Exa); add functional training inbox as org route.
    primaryContact: {
      name: "Laura Keeley",
      title: "Head of Education - Training and Research Institute",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      organizationContactRoute: "training@pda.org",
      whyRelevant:
        "Named TRI education lead from authorized Exa research; training@pda.org is the public functional route for TRI lodging/attendee questions.",
      sourceUrl: "https://www.pda.org/",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Anti-clobber: keep Exa named person; attach prior zero-paid training@pda.org as organization route.",
    },
  },
  gdi_pep_live_c165c897d233: {
    inventory: "historical",
    primaryContact: {
      name: "Carrie Rice",
      title: "Principal — Potomac Meetings and Events",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      whyRelevant:
        "Named principal from authorized Exa batch; direct email still unverified in public sources — org-route research remains open.",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Preserve Exa named person; no public direct email verified in zero-paid pass.",
    },
  },
  // Canonical high-priority dry-run candidates (Woman's Club already founder-applied 2026-11-12)
  gdi_pe_781f12393f8117e7: {
    inventory: "canonical",
    applied: true,
    primaryContact: {
      name: "Diane Robertson",
      title: "Rental Coordinator — Woman's Club of Bethesda",
      email: "rentals@womansclubofbethesda.org",
      emailOwnership: "organization_or_department",
      directPersonEmail: false,
      phone: "301-530-1784",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      organizationContactRoute: "rentals@womansclubofbethesda.org",
      whyRelevant:
        "Named rental coordinator via organization role inbox — not a verified individual mailbox.",
      sourceUrl: "https://www.womansclubofbethesda.org/contact/",
      verifiedAt: "2026-11-12",
    },
    secondaryContact: {
      name: "Mary Sherman",
      title: "Club President",
      email: "president@womansclubofbethesda.org",
      emailOwnership: "organization_or_department",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      sourceUrl: "https://www.womansclubofbethesda.org/contact/",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Founder-approved and applied 2026-11-12. rentals@ = organization role inbox.",
    },
  },

  // --- Wave 2 zero-paid historical evidence (2026-11-12 continuation) ---
  disp_recLuxvwwxID7U2B8_nature_forward: {
    inventory: "historical",
    primaryContact: {
      name: "Beatriz Engel",
      title: "Director of Woodend Rental Events — Nature Forward",
      email: "beatriz.engel@natureforward.org",
      phone: "(301) 450-1407",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      whyRelevant:
        "Named CMP rentals director for Woodend events — relevant partnership / guestroom overflow entry for Nature Forward gatherings.",
      sourceUrl: "https://natureforward.org/beatriz-engel/",
      verifiedAt: "2026-11-12",
    },
    secondaryContact: {
      name: "Talissa Bavaresco",
      title: "Special Event and Sponsorship Manager",
      email: "talissa.bavaresco@natureforward.org",
      phone: "(301) 450-1381",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      sourceUrl: "https://natureforward.org/talissa-bavaresco/",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid Nature Forward staff directory 2026-11-12.",
    },
  },
  disp_recLuxvwwxID7U2B8_city_of_rockville: {
    inventory: "historical",
    primaryContact: {
      name: "Colleen McQuitty",
      title: "Special Events Manager — City of Rockville",
      email: "cmcquitty@rockvillemd.gov",
      phone: "240-314-8606",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      organizationContactRoute: "specialevents@rockvillemd.gov",
      whyRelevant:
        "Named Special Events Manager for City of Rockville festivals/permits — civic meetings & lodging-adjacent commercial entry.",
      sourceUrl: "https://www.rockvillemd.gov/services/sponsor-an-annual-event/",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid City of Rockville special-events pages 2026-11-12.",
    },
  },
  gdi_e2d_770e3b44ad97c1ae: {
    inventory: "historical",
    primaryContact: {
      name: "Clarissa Harris",
      title: "Vice President, Marketing Communications & Events — Greater Bethesda Chamber",
      email: "charris@greaterbethesdachamber.org",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      organizationContactRoute: "staff@greaterbethesdachamber.org",
      phone: "(301) 652-4900",
      whyRelevant:
        "Named VP of events/sponsorships for Greater Bethesda Chamber signature programs — local meetings lodging referral entry.",
      sourceUrl: "https://www.greaterbethesdachamber.org/annual-events.html",
      verifiedAt: "2026-11-12",
    },
    secondaryContact: {
      name: "Allie Williams",
      title: "President and CEO",
      email: "awilliams@greaterbethesdachamber.org",
      phone: "(301) 652-4900 x 203",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      sourceUrl: "https://www.greaterbethesdachamber.org/contact-us.html",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid Greater Bethesda Chamber team + annual-events pages 2026-11-12.",
    },
  },
  disp_recLuxvwwxID7U2B8_aledade: {
    inventory: "historical",
    primaryContact: {
      name: "Mary Kate Connor",
      title: "Events Coordinator — Aledade, Inc. (Bethesda HQ)",
      email: null,
      emailOwnership: "organization_or_department",
      directPersonEmail: false,
      phone: "(202) 803-7979",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      organizationContactRoute: "info@aledade.com",
      linkedin: "https://linkedin.com/in/mary-kate-connor",
      whyRelevant:
        "Named Events Coordinator at Aledade Bethesda HQ — relevant commercial entry for corporate meetings/training/travel lodging adjacency. Individual email not published; use info@aledade.com / (202) 803-7979.",
      sourceUrl: "https://aledade.com/more-information",
      verifiedAt: "2026-11-12",
    },
    secondaryContact: {
      name: "Lina Tavarez",
      title: "Events Coordinator — Aledade, Inc.",
      linkedin: "https://linkedin.com/in/linatavarez",
      organizationContactRoute: "info@aledade.com",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      sourceUrl: "https://jobs.lever.co/aledade?workplaceType=onsite",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid Aledade HQ contact page + LinkedIn Events Coordinator roles 2026-11-12. Named recovered; direct email not published.",
    },
  },
  gdi_s4912_59382d1e71a2: {
    inventory: "historical",
    primaryContact: {
      name: "Shinta Hernandez",
      title: "Dean of the Virtual Campus — Montgomery College (AI Skills Summit co-organizer)",
      email: "shinta.hernandez@montgomerycollege.edu",
      phone: "240-567-2374",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      whyRelevant:
        "Publicly credited co-organizer of MC AI Skills Summit; credible campus events / future-edition lodging research entry.",
      sourceUrl:
        "https://advocate.montgomerycollege.edu/_documents/offices/eass/mc-open-faculty-brochure.pdf",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid MC faculty brochure + LinkedIn organizer credits 2026-11-12. Michael Mills listed as retired — not used as primary.",
    },
  },
  gdi_s4912_d55ee8a8e9ca: {
    inventory: "historical",
    primaryContact: {
      name: "Allison Hurst",
      title: "Meeting Logistics — The Scientific Consulting Group (NIDDK workshop contractor)",
      email: "ahurst@scgcorp.com",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      whyRelevant:
        "Recurring public meeting-logistics contact for NIDDK workshops near Bethesda/Rockville — credible lodging coordination intermediary.",
      sourceUrl:
        "https://www.niddk.nih.gov/news/meetings-workshops/2026/k-awardees-2026",
      verifiedAt: "2026-11-12",
    },
    secondaryContact: {
      name: null,
      title: "NIDDK Meetings & Workshops portfolio",
      organizationContactRoute: "https://www.niddk.nih.gov/news/meetings-workshops",
      contactClass: "FUNCTIONAL_INBOX",
      sourceUrl: "https://www.niddk.nih.gov/news/meetings-workshops",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid NIDDK workshop pages list SCG logistics contacts 2026-11-12.",
    },
  },
  gdi_s4912_b0ea89e0716d: {
    inventory: "historical",
    primaryContact: {
      name: "Camila Sgrignoli Januario",
      title: "Senior Program Manager — Global Exposome Forum / JHU CAAT",
      email: "camilajanuario@jhu.edu",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      whyRelevant:
        "Named sponsorship/program contact for Global Exposome Summit at Bethesda North — overflow lodging research entry.",
      sourceUrl: "https://globalexposomesummit.org/sponsorships/",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid Global Exposome Summit sponsorships page 2026-11-12.",
    },
  },
  gdi_s4912_512deebd0963: {
    inventory: "historical",
    hotelFitHonesty: "OVERFLOW_WATCH_COMPETITOR_HOST",
    hotelFitNote:
      "2027 U.S. IFE Conference host is Bethesda North Marriott — Bethesda Marriott is overflow/residual only. Named organizer valid; do not promote Sales Ready from contact alone.",
    primaryContact: {
      name: "Liz Conrow",
      title: "Conference logistics contact — Laboratory for Laser Energetics (U.S. IFE / IFE-STAR)",
      email: "econ@lle.rochester.edu",
      directPersonEmail: true,
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      whyRelevant:
        "Named LLE conference logistics contact (econ@lle.rochester.edu) for U.S. IFE / IFE-STAR conference cycle; 2027 host is Bethesda North Marriott — overflow watch only for Bethesda Marriott.",
      sourceUrl: "https://ifestar2026.mailchimpsites.com/abstracts",
      verifiedAt: "2026-11-12",
    },
    secondaryContact: {
      name: "Dustin Froula",
      title: "Chair, U.S. IFE Conference Committee / PI IFE-COLoR — LLE",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      whyRelevant:
        "Publicly identified conference committee chair — program owner, not housing desk. Prefer Liz Conrow / econ@ for lodging logistics.",
      sourceUrl:
        "https://www.lle.rochester.edu/publications/lle-in-focus/us-ife-conference-fusion-energy/",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid IFE-STAR abstracts page names Liz Conrow at econ@; US IFE 2027 page confirms Bethesda North host + room block. OVERFLOW_WATCH preserved.",
    },
  },
  disp_recLuxvwwxID7U2B8_strathmore: {
    inventory: "historical",
    primaryContact: {
      name: "Kady Chambers",
      title: "Senior Director of Venue Rentals and Operations — Strathmore",
      email: null,
      emailOwnership: "organization_or_department",
      directPersonEmail: false,
      phone: "301-581-5199",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      organizationContactRoute: "rentals@strathmore.org",
      linkedin: "https://linkedin.com/in/kady-chambers",
      whyRelevant:
        "Named Senior Director of Venue Rentals and Operations on Rockville Chamber Strathmore listing — commercial entry for private/corporate venue rentals and lodging-adjacent partnership referrals. Individual email not published; use rentals@ / groupsales@.",
      sourceUrl: "https://business.rockvillechamber.org/list/Details/strathmore-4797014",
      verifiedAt: "2026-11-12",
    },
    secondaryContact: {
      name: "Faith N. Ring",
      title: "Group Sales and Audience Development Manager — Strathmore",
      email: null,
      emailOwnership: "organization_or_department",
      organizationContactRoute: "groupsales@strathmore.org",
      phone: "301-581-5199",
      linkedin: "https://linkedin.com/in/faithring",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      whyRelevant:
        "Named Group Sales manager (LinkedIn current) — group outing / ticketed-group lodging adjacency; public route remains groupsales@strathmore.org.",
      sourceUrl:
        "https://www.rockvillechamber.org/2026/01/30/plan-the-perfect-group-outing-at-strathmore/",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid Rockville Chamber Strathmore contacts + LinkedIn role verification 2026-11-12. Named recovered; direct emails not published. Preserve PARTNERSHIP_WATCH.",
    },
  },

  // Historical research-stage bodies (persist via protected merge)
  gdi_beth_v16_evt_3c8228774517: {
    inventory: "historical",
    hotelFitHonesty: "OVERFLOW_WATCH_COMPETITOR_HOST",
    hotelFitNote:
      "2027 U.S. IFE Conference host is Bethesda North Marriott Hotel & Conference Center with an official room block — Bethesda Marriott is overflow/residual only.",
    primaryContact: {
      name: "Liz Conrow",
      title: "Conference logistics contact — Laboratory for Laser Energetics (U.S. IFE / IFE-STAR)",
      email: "econ@lle.rochester.edu",
      directPersonEmail: true,
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      whyRelevant:
        "Named LLE conference logistics contact published for IFE-STAR/U.S. IFE questions (econ@lle.rochester.edu). 2027 conference host is Bethesda North Marriott — overflow watch for Bethesda Marriott; do not treat as primary Sales Ready demand.",
      sourceUrl: "https://ifestar2026.mailchimpsites.com/abstracts",
      verifiedAt: "2026-11-12",
    },
    secondaryContact: {
      name: "Dustin Froula",
      title: "Chair, U.S. IFE Conference Committee / PI IFE-COLoR — LLE",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      whyRelevant:
        "Publicly identified conference committee chair (program owner). Prefer Liz Conrow / econ@ for lodging logistics.",
      sourceUrl:
        "https://www.lle.rochester.edu/publications/lle-in-focus/us-ife-conference-fusion-energy/",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid IFE-STAR abstracts page names Liz Conrow at econ@; US IFE 2027 page confirms Bethesda North host + room block. OVERFLOW_WATCH preserved.",
    },
  },
  gdi_opp_series_request_a_conference_registration_website_2026: {
    inventory: "canonical",
    rejected: true,
    founderValidation: "FAIL",
    rejectionReason:
      "Carrie Wagerman appears only as historical backupContacts / Frederick staff index; 2027 RNA symposium pages publish CCR Conferences (ccrconferences@mail.nih.gov) only — symposium lodging role not independently confirmed. Do not promote to primary.",
    primaryContact: {
      name: null,
      title: "CCR Conferences — NCI RNA Biology Symposium",
      email: "ccrconferences@mail.nih.gov",
      contactClass: "FUNCTIONAL_INBOX",
      organizationContactRoute: "ccrconferences@mail.nih.gov",
      whyRelevant:
        "Official 2027 NCI RNA Biology Symposium conference desk; Bethesda Marriott listed on accommodations page. Named lodging owner not confirmed.",
      sourceUrl:
        "https://ncifrederick.cancer.gov/events/conferences/2027NCI_RNA_Symposium",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "REJECTED named Carrie Wagerman promotion 2026-11-12 — keep functional CCR route only.",
    },
  },
  gdi_opp_series_nci_rna_biology_symposium_ncif_conference_2027: {
    inventory: "canonical",
    rejected: true,
    founderValidation: "FAIL",
    rejectionReason:
      "Sibling of NCI RNA symposium family — same Carrie Wagerman lodging-role gap; official route remains CCR Conferences.",
    primaryContact: {
      name: null,
      title: "CCR Conferences — NCI RNA Biology Symposium",
      email: "ccrconferences@mail.nih.gov",
      contactClass: "FUNCTIONAL_INBOX",
      organizationContactRoute: "ccrconferences@mail.nih.gov",
      whyRelevant:
        "Official symposium conference desk for lodging-adjacent outreach; named buyer not confirmed.",
      sourceUrl:
        "https://ncifrederick.cancer.gov/events/conferences/2027NCI_RNA_Symposium",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "REJECTED named Carrie Wagerman promotion 2026-11-12.",
    },
  },
  gdi_opp_2027_nci_rna_biology_symposium_7: {
    inventory: "canonical",
    rejected: true,
    founderValidation: "FAIL",
    rejectionReason:
      "backupContacts Carrie Wagerman not confirmed as current symposium lodging coordinator on official 2027 pages.",
    primaryContact: {
      name: null,
      title: "CCR Conferences — NCI RNA Biology Symposium",
      email: "ccrconferences@mail.nih.gov",
      contactClass: "FUNCTIONAL_INBOX",
      organizationContactRoute: "ccrconferences@mail.nih.gov",
      whyRelevant:
        "Official symposium conference desk; accommodations list includes Bethesda Marriott.",
      sourceUrl:
        "https://ncifrederick.cancer.gov/events/conferences/2027NCI_RNA_Symposium/accommodations",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "REJECTED named Carrie Wagerman promotion 2026-11-12 — functional CCR only.",
    },
  },
  gdi_opp_amwa_2028_annual: {
    inventory: "canonical",
    hotelFitHonesty: "DESTINATION_UNCONFIRMED",
    hotelFitNote:
      "AMWA annual destination not confirmed for Bethesda — named Associate Director is a valid org entry point; keep speculative hotel-fit / Research Continuation until destination is evidenced.",
    primaryContact: {
      name: "Kelly McMahon",
      title: "Associate Director — AMWA",
      email: null,
      emailOwnership: "organization_or_department",
      directPersonEmail: false,
      organizationContactRoute: "associatedirector@amwa-doc.org",
      phone: "(518) 313-0122",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      whyRelevant:
        "Named AMWA Associate Director on public Our People staff roster; associatedirector@ is the verified headquarters meetings/ops route for annual meeting lodging outreach.",
      sourceUrl: "https://amwa-doc.org/about-amwa/our-people/",
      verifiedAt: "2026-11-12",
    },
    secondaryContact: {
      name: "Lisa M. Tate",
      title: "Director of Partnerships & Strategic Development — AMWA",
      organizationContactRoute: "associatedirector@amwa-doc.org",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      sourceUrl: "https://amwa-doc.org/about-amwa/our-people/",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid AMWA Our People staff roster 2026-11-12. Individual staff emails not published — org route labeled. CANONICAL dry-run.",
    },
  },
  gdi_opp_amwa_2027_annual: {
    inventory: "canonical",
    hotelFitHonesty: "DESTINATION_UNCONFIRMED",
    hotelFitNote:
      "AMWA 112th Annual destination not confirmed for Bethesda — contact valid; hotel-fit remains speculative.",
    primaryContact: {
      name: "Kelly McMahon",
      title: "Associate Director — AMWA",
      email: null,
      emailOwnership: "organization_or_department",
      directPersonEmail: false,
      organizationContactRoute: "associatedirector@amwa-doc.org",
      phone: "(518) 313-0122",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      whyRelevant:
        "Named AMWA Associate Director; headquarters meetings/ops route for 112th Annual Meeting lodging outreach.",
      sourceUrl: "https://amwa-doc.org/about-amwa/our-people/",
      verifiedAt: "2026-11-12",
    },
    secondaryContact: {
      name: "Lisa M. Tate",
      title: "Director of Partnerships & Strategic Development — AMWA",
      organizationContactRoute: "associatedirector@amwa-doc.org",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      sourceUrl: "https://amwa-doc.org/about-amwa/our-people/",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid AMWA staff roster 2026-11-12. CANONICAL dry-run.",
    },
  },
  gdi_opp_amwa_2027_interim: {
    inventory: "canonical",
    primaryContact: {
      name: "Kelly McMahon",
      title: "Associate Director — AMWA",
      email: null,
      emailOwnership: "organization_or_department",
      directPersonEmail: false,
      organizationContactRoute: "associatedirector@amwa-doc.org",
      phone: "(518) 313-0122",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      whyRelevant:
        "Named AMWA Associate Director for interim meeting logistics outreach.",
      sourceUrl: "https://amwa-doc.org/about-amwa/our-people/",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid AMWA staff roster 2026-11-12. CANONICAL dry-run.",
    },
  },
  gdi_opp_georgetown_homecoming_2027: {
    inventory: "canonical",
    primaryContact: {
      name: "Vanessa Otarola Kehoe",
      title: "Assistant Vice President, Class Programs and Reunion Campaigns — Georgetown Advancement",
      email: null,
      emailOwnership: "organization_or_department",
      directPersonEmail: false,
      organizationContactRoute: "advancementevents@georgetown.edu",
      phone: "202-687-2728",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      whyRelevant:
        "Named AVP for class programs/reunions; Advancement Events desk (202-687-2064 / advancementevents@) owns Homecoming Weekend lodging-adjacent alumni travel coordination. Homecoming 2027 save-the-date Oct 22–23.",
      sourceUrl: "https://contact.georgetown.edu/view/vbo/",
      verifiedAt: "2026-11-12",
    },
    secondaryContact: {
      name: null,
      title: "Georgetown Advancement Events hotline",
      email: "advancementevents@georgetown.edu",
      phone: "202-687-2064",
      contactClass: "FUNCTIONAL_INBOX",
      sourceUrl: "https://homecoming.georgetown.edu/",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid Georgetown directory + Homecoming site 2026-11-12. Individual email gated. CANONICAL dry-run.",
    },
  },
  gdi_opp_nih_sbpo_vos_pattern: {
    inventory: "canonical",
    primaryContact: {
      name: "Annette Owens-Scarboro",
      title: "NIH Small Business and Minority Serving Institution Program Manager — SBPO",
      email: "scarbora@od.nih.gov",
      directPersonEmail: true,
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      organizationContactRoute: "NIHSmallBusiness@mail.nih.gov",
      whyRelevant:
        "Named NIH SBPO Program Manager who owns vendor outreach sessions held on-site in Bethesda (Rockledge) — credible lodging-adjacent commercial entry for recurring VOS pattern. Persist scarbora@od.nih.gov as person email; NIHSmallBusiness@ as organization route only.",
      sourceUrl: "https://oamp.od.nih.gov/staff-directory/hhs-small-business-specialist",
      verifiedAt: "2026-11-12",
    },
    secondaryContact: {
      name: "Keondra Watts",
      title: "Program Analyst — NIH SBPO",
      email: "keondra.watts@nih.gov",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      sourceUrl: "https://oamp.od.nih.gov/staff-directory/hhs-small-business-specialist",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid NIH SBPO staff directory 2026-11-12. CANONICAL dry-run.",
    },
  },
  gdi_opp_series_ctn_annual_conference_2027: {
    inventory: "canonical",
    primaryContact: {
      name: "Quandra Blackeney",
      title: "Scientific Program Analyst — NIDA Center for Clinical Trials Network (CCTN)",
      email: "scudderq@nida.nih.gov",
      directPersonEmail: true,
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      whyRelevant:
        "Named CCTN Scientific Program Analyst on current NIDA staff list as Quandra Blackeney (scudderq@nida.nih.gov — former name Quandra Scudder). Logistics/registration entry for CTN Annual Scientific Meeting on NIH campus Bethesda; attendees arrange own lodging.",
      sourceUrl: "https://nida.nih.gov/about-nida/organization/cctn/staff-list",
      verifiedAt: "2026-11-12",
    },
    secondaryContact: {
      name: "Landhing Moran, Ph.D.",
      title: "Health Scientist Administrator — NIDA CCTN",
      email: "landhing.moran@nih.gov",
      phone: "(301) 827-7584",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      sourceUrl: "https://nida.nih.gov/about-nida/organization/cctn/staff-list",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid NIDA CCTN staff list + 2026 CTN registration page logistics contacts 2026-11-12. CANONICAL dry-run.",
    },
  },
  gdi_opp_series_new_in_the_library_august_2026: {
    inventory: "canonical",
    commercialEventFamilyId: "ctn_annual_conference_bethesda_2027",
    commercialEventFamilyRole: "LINKED_RESEARCH",
    primaryContact: {
      name: "Quandra Blackeney",
      title: "Scientific Program Analyst — NIDA CCTN",
      email: "scudderq@nida.nih.gov",
      directPersonEmail: true,
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      whyRelevant:
        "Linked research sibling of CTN Annual Conference 2027 (same Bethesda campus meeting). Quandra Blackeney / scudderq@nida.nih.gov remains the verified CCTN logistics entry — not a second independent demand.",
      sourceUrl: "https://nida.nih.gov/about-nida/organization/cctn/staff-list",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Sibling CTN conference recovery 2026-11-12. LINKED_RESEARCH — not a second qualified opportunity.",
    },
  },
  gdi_opp_series_march_15_17_bethesda_md_ctn_dissemination_2027: {
    inventory: "canonical",
    commercialEventFamilyId: "ctn_annual_conference_bethesda_2027",
    commercialEventFamilyRole: "LINKED_RESEARCH",
    primaryContact: {
      name: "Quandra Blackeney",
      title: "Scientific Program Analyst — NIDA CCTN",
      email: "scudderq@nida.nih.gov",
      directPersonEmail: true,
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      whyRelevant:
        "Same underlying CTN Annual / Dissemination March 15–17 2027 Bethesda campus event as the primary CTN Annual Conference card. Contact enrichment only — not independent demand.",
      sourceUrl: "https://ctnlibrary.org/2026/09/18/ctn-annual-conference-march-15-17-2027-bethesda-md/",
      verifiedAt: "2026-11-12",
    },
    secondaryContact: {
      name: "Landhing Moran, Ph.D.",
      title: "Health Scientist Administrator — NIDA CCTN",
      email: "landhing.moran@nih.gov",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      sourceUrl: "https://nida.nih.gov/about-nida/organization/cctn/staff-list",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid CTN 2027 Bethesda announcement + CCTN staff 2026-11-12. CANONICAL dry-run.",
    },
  },
  gdi_opp_arlington_spring_tournament_2027: {
    inventory: "canonical",
    primaryContact: {
      name: "Lizzy Stell",
      title: "Assistant Travel Director — Arlington Soccer Association",
      email: "lizzy@arlingtonsoccer.com",
      directPersonEmail: true,
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      organizationContactRoute: "tournaments@arlingtonsoccer.com",
      whyRelevant:
        "Named Assistant Travel Director on ASA staff roster — relevant stay-to-play / hotel coordination entry for ASIST Spring Tournament 2027 (fields near DC lodging corridor). Persist lizzy@ as person email; tournaments@ as organization route only.",
      sourceUrl: "https://www.arlingtonsoccer.com/staff",
      verifiedAt: "2026-11-12",
    },
    secondaryContact: {
      name: "Frank DeMarco",
      title: "Executive Director — Arlington Soccer Association",
      email: "frankdemarco@arlingtonsoccer.com",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      sourceUrl: "https://www.arlingtonsoccer.com/staff",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid ASA staff directory 2026-11-12. CANONICAL dry-run.",
    },
  },
  gdi_opp_umd_alumni_weekend_watch: {
    inventory: "canonical",
    primaryContact: {
      name: "Natalie Ross",
      title: "Senior Director of Alumni Events and Programs — UMD Alumni Association",
      email: "nross1@umd.edu",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      organizationContactRoute: "umdevents@umd.edu",
      whyRelevant:
        "Named senior director of alumni events/programs; owns alumni weekend lodging-adjacent demand when 2027 dates publish.",
      sourceUrl: "https://alumni.umd.edu/about/team",
      verifiedAt: "2026-11-12",
    },
    secondaryContact: {
      name: "Tricia Clarke",
      title: "Associate Director of Alumni Events and Programs — UMD Alumni Association",
      email: "tclarke3@umd.edu",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      sourceUrl: "https://alumni.umd.edu/about/team",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid UMD Alumni Association Meet the Team 2026-11-12. CANONICAL dry-run.",
    },
  },
  gdi_opp_ecs_251_2027_overflow: {
    inventory: "canonical",
    hotelFitHonesty: "OVERFLOW_WATCH_COMPETITOR_HOST",
    hotelFitNote:
      "Official housing is Marriott Marquis / Walter E. Washington Convention Center — Bethesda is overflow/residual only. Named contact valid; do not promote Sales Ready or Account Development from contact alone.",
    primaryContact: {
      name: "John Lewis",
      title: "Director of Meetings Operations — The Electrochemical Society",
      email: null,
      emailOwnership: "organization_or_department",
      directPersonEmail: false,
      organizationContactRoute: "meetings@electrochem.org",
      phone: "1.609.737.1902",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      whyRelevant:
        "Named ECS Director of Meetings Operations; official housing for 251st Meeting is Marriott Marquis/WEWCC — overflow watch only, contact via meetings@.",
      sourceUrl: "https://www.electrochem.org/contact-ecs",
      verifiedAt: "2026-11-12",
    },
    secondaryContact: {
      name: "Francesca Di Palo",
      title: "Meetings Manager — ECS",
      organizationContactRoute: "meetings@electrochem.org",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      sourceUrl: "https://www.electrochem.org/contact-ecs",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid ECS contact page 2026-11-12. Individual emails not published. CANONICAL dry-run. Remains OVERFLOW_WATCH.",
    },
  },
  gdi_opp_accp_annual_meeting_20261001: {
    inventory: "canonical",
    hotelFitHonesty: "OUT_OF_MARKET_HOST_RESEARCH_GATED",
    hotelFitNote:
      "2026 ACCP Annual Meeting is Salt Lake City — Bethesda Marriott demand remains speculative until a DC-area cycle is evidenced. Named planner contact is valid; contact completeness must not promote Sales Ready or Account Development.",
    primaryContact: {
      name: "Lisa C. Miller",
      title: "Meeting Planner, CMP — American College of Clinical Pharmacy",
      email: "lmiller@accp.com",
      directPersonEmail: true,
      phone: "(913) 271-1283",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      whyRelevant:
        "Named ACCP meeting planner for annual meeting space/lodging coordination. Note: 2026 ACCP AM is Salt Lake City — Bethesda hotel fit remains research-gated until a DC-area cycle is confirmed.",
      sourceUrl:
        "https://www.accp.com/docs/meetings/am26/AM26_Request_for_Meeting_or_Event_Space_Form.pdf",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid ACCP AM26 meeting-space form 2026-11-12. CANONICAL dry-run. Hotel-fit still RESEARCH_CONTINUATION.",
    },
  },
  disp_recLuxvwwxID7U2B8_montgomery_county_shrm: {
    inventory: "historical",
    primaryContact: {
      name: "Neha Agarwal",
      title: "Programs Hospitality Director — Montgomery County SHRM",
      email: null,
      emailOwnership: "organization_or_department",
      directPersonEmail: false,
      organizationContactRoute: "mcshrm@gmail.com",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      whyRelevant:
        "Named Programs Hospitality Director on MCSHRM board — direct commercial entry for chapter meetings/catering hospitality at local venues.",
      sourceUrl: "https://mcshrm.com/board",
      verifiedAt: "2026-11-12",
    },
    secondaryContact: {
      name: "Suzanne M. Motta, SHRM-CP",
      title: "Past President — Montgomery County SHRM",
      email: "smotta@signatureperformance.com",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      whyRelevant:
        "Public event logistics contact for MCSHRM chapter programs at Manor Country Club.",
      sourceUrl:
        "https://mcshrm.starchapter.com/meetinginfo.php?id=63&ts=1768345707",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid MCSHRM board + event pages 2026-11-12. Neha individual email not published.",
    },
  },

  // --- Wave 2 zero-paid contact completion (2026-11-12) — gaps after PR #189 ---
  gdi_opp_nado_ddaa_washcon_2028: {
    inventory: "canonical",
    hotelFitHonesty: "DESTINATION_UNCONFIRMED",
    hotelFitNote:
      "2028 WashCon hotel not yet named (Arlington VA cycle). Brittany Salazar is the NADO meetings owner; keep Research Continuation / overflow posture until Bethesda-eligible lodging is evidenced.",
    primaryContact: {
      name: "Brittany Salazar",
      title: "Associate Director of Meetings and Events — NADO",
      email: "bsalazar@nado.org",
      directPersonEmail: true,
      phone: "202-921-4451",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      whyRelevant:
        "Public NADO Associate Director of Meetings and Events; official hotel-reservation contact on NADO conference pages and sponsorship packets.",
      sourceUrl: "https://www.nado.org/2025atc/",
      verifiedAt: "2026-11-12",
    },
    secondaryContact: {
      name: "Kar'ron Grant",
      title: "Operations Specialist — NADO (conference registration)",
      email: "kgrant@nado.org",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      sourceUrl: "https://www.nado.org/2025atc/",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid NADO staff + 2025 ATC hotel contact page 2026-11-12. CANONICAL dry-run.",
    },
  },
  gdi_opp_nado_ddaa_washcon_2027_overflow: {
    inventory: "canonical",
    hotelFitHonesty: "OVERFLOW_WATCH_COMPETITOR_HOST",
    hotelFitNote:
      "2027 WashCon host is Crystal Gateway — Bethesda Marriott is overflow/residual only. Named meetings owner valid; do not promote Sales Ready from contact alone.",
    primaryContact: {
      name: "Brittany Salazar",
      title: "Associate Director of Meetings and Events — NADO",
      email: "bsalazar@nado.org",
      directPersonEmail: true,
      phone: "202-921-4451",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      whyRelevant:
        "Public NADO Associate Director of Meetings and Events for WashCon housing/meetings outreach.",
      sourceUrl: "https://www.nado.org/nado-staff/",
      verifiedAt: "2026-11-12",
    },
    secondaryContact: {
      name: "Kar'ron Grant",
      title: "Operations Specialist — NADO",
      email: "kgrant@nado.org",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      sourceUrl: "https://www.nado.org/2025atc/",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid NADO staff directory 2026-11-12. CANONICAL dry-run. Remains OVERFLOW_WATCH.",
    },
  },
  gdi_opp_series_high_risk_high_reward_research_symposium_2027: {
    inventory: "canonical",
    hotelFitHonesty: "NIH_CAMPUS_LODGING_ADJACENT",
    hotelFitNote:
      "2027 HRHR Symposium at NIH Natcher (Bethesda campus) — attendees arrange lodging; Bethesda Marriott corridor is credible guestroom demand, not a confirmed hotel block.",
    primaryContact: {
      name: "Jennifer Adona",
      title: "Symposium logistics contact — Rose Li and Associates (NIH Common Fund HRHR)",
      email: "jennifer.adona@roseliassociates.com",
      directPersonEmail: true,
      organizationContactRoute: "HRHR@od.nih.gov",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      whyRelevant:
        "Named Rose Li & Associates symposium logistics contact published for NIH Common Fund HRHR Symposium cycle; NIH Common Fund also publishes HRHR@od.nih.gov.",
      sourceUrl: "https://event.roseliassociates.com/hrhr2026/contact/",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid HRHR 2026 contractor contact + Common Fund 2027 symposium page 2026-11-12. CANONICAL dry-run.",
    },
  },
  gdi_opp_acvnu_renal_week_20270412: {
    inventory: "canonical",
    hotelFitHonesty: "OVERFLOW_WATCH_COMPETITOR_HOST",
    hotelFitNote:
      "Official 2027 Renal Week room block is The Bethesdan Hotel (Tapestry Collection) — Bethesda Marriott is overflow/residual only.",
    primaryContact: {
      name: "JD Foster",
      title: "Renal Week Chair / ACVNU President-Elect — Friendship Hospital for Animals",
      email: "jdfostervmd@gmail.com",
      directPersonEmail: true,
      organizationContactRoute: "info@acvnu.org",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      whyRelevant:
        "Public ACVNU Renal Week Chair and President-Elect; conference lodging outreach for overflow near The Bethesdan host block.",
      sourceUrl: "https://acvnurenalweek.org/Schedule/Speakers",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid ACVNU Renal Week speakers + founders contact 2026-11-12. CANONICAL dry-run. Remains OVERFLOW_WATCH.",
    },
  },
  gdi_opp_regulatory_information_conference_20260930: {
    inventory: "canonical",
    hotelFitHonesty: "OVERFLOW_WATCH_COMPETITOR_HOST",
    hotelFitNote:
      "RIC is hybrid at NRC HQ Rockville — Bethesda Marriott is lodging-adjacent overflow only, not the contracted host.",
    primaryContact: {
      name: "Lorna Kipfer",
      title: "RIC Conference Contact — U.S. Nuclear Regulatory Commission",
      email: "Lorna.Kipfer@nrc.gov",
      directPersonEmail: true,
      organizationContactRoute: "RICHelpDesk.Resource@nrc.gov",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      whyRelevant:
        "Named NRC RIC contact on official Conferences & Symposia listing for Regulatory Information Conference.",
      sourceUrl: "https://www.nrc.gov/public-involve/conferences",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid NRC conferences table 2026-11-12. CANONICAL dry-run. Remains OVERFLOW_WATCH.",
    },
  },
  disp_recLuxvwwxID7U2B8_iwlca_elite_tournaments_iwlca_champions_cup: {
    inventory: "historical",
    primaryContact: {
      name: "Dan Harris",
      title: "Director of Customer Experience — Halpern Travel (IWLCA Champions Cup official housing)",
      email: null,
      emailOwnership: "organization_or_department",
      directPersonEmail: false,
      organizationContactRoute: "reservations@halperntravel.com",
      phone: "888-640-6400",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      whyRelevant:
        "Named Halpern Travel Director of Customer Experience; Halpern is official IWLCA Champions Cup housing partner for stay-to-play lodging.",
      sourceUrl: "https://halperntravel.com/about-us/",
      verifiedAt: "2026-11-12",
    },
    secondaryContact: {
      name: "Anna Hofmann",
      title: "Tournament sponsor/vendor contact — Elite Tournaments",
      email: "ahofmann@elitetournaments.com",
      directPersonEmail: true,
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      whyRelevant:
        "Named Elite Tournaments commercial contact published on IWLCA Champions Cup page for sponsor/vendor outreach.",
      sourceUrl: "https://iwlcarecruiting.com/tournaments/iwlca-champions-cup-v2/",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid Halpern About + IWLCA Champions Cup housing page 2026-11-12. Dan Harris direct email not published.",
    },
  },
  gdi_s4912_66a1f89e5afb: {
    inventory: "historical",
    primaryContact: {
      name: "Laura Marler",
      title: "Director, Office of Postdoctoral Services — NIH OITE",
      email: null,
      emailOwnership: "organization_or_department",
      directPersonEmail: false,
      organizationContactRoute: "oite@nih.gov",
      phone: "301-496-2427",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      whyRelevant:
        "Named OITE Director of Postdoctoral Services; public contact for postdoc onboarding/relocation support (NIH does not provide dorm housing — lodging partnership/referral entry).",
      sourceUrl: "https://www.training.nih.gov/research-training/pd/",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid NIH OITE postdoc program page 2026-11-12. Individual email not published; oite@nih.gov is the verified route.",
    },
  },
  gdi_s4912_364713d41c67: {
    inventory: "historical",
    primaryContact: {
      name: "Candelario Zapata",
      title: "Director — NIH Division of International Services (ORS)",
      email: null,
      emailOwnership: "organization_or_department",
      directPersonEmail: false,
      organizationContactRoute: "DIS@mail.nih.gov",
      phone: "301-496-6166",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      whyRelevant:
        "Named DIS Director for visiting scientists arriving to NIH Bethesda; commercial entry for relocation/extended-stay partnership (NIH does not provide housing).",
      sourceUrl: "https://ors.od.nih.gov/pes/dis/AboutDIS/Pages/AboutTheStaff.aspx",
      verifiedAt: "2026-11-12",
    },
    secondaryContact: {
      name: "Anabelle Filbert",
      title: "Customer Service Team Leader — NIH DIS",
      organizationContactRoute: "DIS@mail.nih.gov",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      sourceUrl: "https://ors.od.nih.gov/pes/dis/AboutDIS/Pages/AboutTheStaff.aspx",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid NIH DIS About the Staff 2026-11-12. Individual emails not published.",
    },
  },
  gdi_beth_v16_evt_90a6ddcb10ca: {
    inventory: "historical",
    hotelFitHonesty: "OVERFLOW_WATCH_COMPETITOR_HOST",
    hotelFitNote:
      "2027 NatCon host is Bethesda North Marriott — Bethesda Marriott is overflow/residual only.",
    primaryContact: {
      name: "Dollie O'Neill",
      title: "National Executive Director — Kappa Kappa Psi & Tau Beta Sigma",
      email: "oneill@kkytbs.org",
      directPersonEmail: true,
      organizationContactRoute: "kkytbs@kkytbs.org",
      phone: "(405) 372-2333",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      whyRelevant:
        "Named National Executive Director; public staff bio and LinkedIn state she oversees District and National Convention event planning/scheduling.",
      sourceUrl: "https://www.kkytbs.org/expenseforms/",
      verifiedAt: "2026-11-12",
    },
    secondaryContact: {
      name: "Amber J. Breckenridge",
      title: "Business Operations Coordinator — KKΨ & ΤΒΣ National HQ (hospitality)",
      organizationContactRoute: "kkytbs@kkytbs.org",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      whyRelevant:
        "HQ Business Operations Coordinator with hospitality background (PCMA) — convention operations entry alongside Executive Director.",
      sourceUrl: "https://www.kkytbs.org/staff/",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid KKPsi/TBS HQ staff + expense-form published email 2026-11-12.",
    },
  },
  gdi_beth_v16_evt_0ab91e208405: {
    inventory: "historical",
    hotelFitHonesty: "OVERFLOW_WATCH_COMPETITOR_HOST",
    hotelFitNote:
      "IFPAC-2027 host is North Bethesda (Washington DC area) — Bethesda Marriott is overflow/residual lodging research only.",
    primaryContact: {
      name: "Robert Zutkis",
      title: "Director / President — InfoScience Services (IFPAC conference management)",
      email: null,
      emailOwnership: "organization_or_department",
      directPersonEmail: false,
      organizationContactRoute: "info@ifpacnet.org",
      phone: "847-543-6800",
      contactClass: CONTACT_ELIGIBILITY_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      whyRelevant:
        "Named IFPAC director / InfoScience Services president; firm provides IFPAC conference logistics, production and admin support.",
      sourceUrl: "https://ifpacglobal.org/",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid IFPAC site + InfoScience Services directory listing 2026-11-12. Individual email not published.",
    },
  },
});

/** Proposed paid batch — NOT AUTHORIZATION. Prior Exa PDA/Chamber/Potomac budget exhausted. */
export const PROPOSED_PAID_CONTACT_BATCH_V1 = Object.freeze({
  authorized: false,
  provider: "Exa Agent (existing GDI Full E2E contact deep research)",
  ceilingUsd: 12,
  batchMaxUsd: 12,
  expectedCalls: 3,
  opportunityIds: [
    "gdi_opp_2027_nci_rna_biology_symposium_7",
    "gdi_pep_live_c165c897d233",
    "gdi_opp_48th_iscb_annual_conference_2027_12",
  ],
  missingEvidence: {
    gdi_opp_2027_nci_rna_biology_symposium_7:
      "CCR Conferences org route only; Carrie Wagerman previously founder-rejected as lodging owner",
    gdi_pep_live_c165c897d233:
      "Carrie Rice named (Exa) but no verified direct email/LinkedIn/phone in zero-paid public sources",
    gdi_opp_48th_iscb_annual_conference_2027_12:
      "No public named ISCB 2027 local organizing / housing chair after zero-paid search",
  },
  commercialRationale: {
    gdi_opp_2027_nci_rna_biology_symposium_7:
      "Bethesda Marriott on accommodations list — confirming active lodging owner unlocks Sales Ready path when demand timing is current",
    gdi_pep_live_c165c897d233:
      "Planner partnership — direct route converts named principal into outreachable lead",
    gdi_opp_48th_iscb_annual_conference_2027_12:
      "Annual conference lodging demand if DC-area cycle and housing chair confirmed",
  },
  expectedImpact:
    "Up to +1 named lodging-owner confirmation (NCI RNA) and +1 bucket-A email upgrade (Carrie Rice). IWLCA/NADO/HRHR/ACVNU/OITE/DIS/KKPsi/IFPAC/NRC recovered zero-paid in wave 2.",
  note: "NOT AUTHORIZATION. Prior three-call Exa authorization exhausted — do not reuse. Stop for explicit founder approval before any paid request.",
});

function loadStaged({ root = ROOT } = {}) {
  const p = path.join(
    researchStageHotelDir(BETHESDA_CANONICAL_HOTEL_ID, { root }),
    "reconstructed-opportunities-v1.json"
  );
  if (fs.existsSync(p)) {
    return { path: p, doc: JSON.parse(fs.readFileSync(p, "utf8")) };
  }
  const fixture = path.join(root, FIXTURE_REL);
  if (fs.existsSync(fixture)) {
    return { path: fixture, doc: JSON.parse(fs.readFileSync(fixture, "utf8")) };
  }
  return { path: null, doc: null };
}

function auditOne(row, { historical = false } = {}) {
  const snap = extractContactSnapshot(row);
  const evidence = ZERO_PAID_CONTACT_EVIDENCE_BY_ID[row.id];
  const proposed =
    evidence && evidence.rejected !== true && evidence.founderValidation !== "FAIL"
      ? extractContactSnapshot(mergeContactAntiClobber(row, evidence))
      : null;
  return {
    id: row.id,
    historical,
    title: text(row.title).slice(0, 140) || null,
    organizationName: text(row.organizationName) || null,
    commercialStatus: text(row.commercialMaturity || row.priorityPillLabel || row.priority) || null,
    category: text(row.opportunityTypeLabel || row.segment) || null,
    before: snap,
    coverage: snap.coverage,
    proposedCoverage: proposed?.coverage || null,
    hasZeroPaidEvidence: Boolean(evidence),
    inventory: evidence?.inventory || (historical ? "historical" : "canonical"),
    researchPriority:
      snap.coverage === COVERAGE_BUCKET.A || snap.coverage === COVERAGE_BUCKET.B
        ? "LOW"
        : /HIGH|SALES|MEDIUM|ACCOUNT/i.test(
              text(row.priority || row.commercialMaturity || row.priorityPillLabel)
            )
          ? "HIGH"
          : historical
            ? "MEDIUM"
            : /DISQUALIFIED/i.test(text(row.priority || row.commercialMaturity))
              ? "DEFER"
              : "MEDIUM",
    missing: [
      !snap.named ? "VERIFIED_NAMED_PERSON" : null,
      !snap.hasDirectEmail && !snap.hasOrgRoute ? "OUTREACH_ROUTE" : null,
      snap.named && !snap.hasDirectEmail ? "PERSON_EMAIL" : null,
      !snap.linkedin ? "LINKEDIN" : null,
      !snap.secondaryNamed ? "SECONDARY_NAMED" : null,
    ].filter(Boolean),
  };
}

export function auditBethesda88Contacts({ root = ROOT } = {}) {
  const store = loadHotelOpportunityDoc(BETHESDA_CANONICAL_HOTEL_ID, { root });
  const union = mergeBethesdaInternalResearchOpportunities(
    BETHESDA_CANONICAL_HOTEL_ID,
    store.opportunities,
    { root }
  );
  const rows = [];
  for (const o of union.opportunities) {
    const historical = BETHESDA_29_ATTESTED_IDS.includes(o.id);
    rows.push(auditOne(o, { historical }));
  }
  const dist = {};
  const histDist = {};
  const canDist = {};
  for (const r of rows) {
    dist[r.coverage] = (dist[r.coverage] || 0) + 1;
    const d = r.historical ? histDist : canDist;
    d[r.coverage] = (d[r.coverage] || 0) + 1;
  }
  return {
    version: BETHESDA_88_CONTACT_QUALITY_COMPLETION_V1,
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    generatedAt: new Date().toISOString(),
    store: {
      count: store.count,
      sha256: store.sha256,
      acceptable: ACCEPTABLE_59_ROW_SHA256S.includes(store.sha256),
      scalingCompaniesIntact: BETHESDA_PROTECTED_SCALING_IDS.every((id) =>
        store.ids.includes(id)
      ),
    },
    union: union.counts,
    distribution: dist,
    historicalDistribution: histDist,
    canonicalDistribution: canDist,
    namedRelevantPrimary: rows.filter((r) => r.before.named).length,
    directOutreachRoutes: rows.filter((r) => r.before.hasDirectEmail).length,
    orgPathOnly: rows.filter((r) => r.coverage === COVERAGE_BUCKET.D).length,
    rows,
  };
}

export function applyHistoricalContactEnrichment({
  root = ROOT,
  dryRun = false,
} = {}) {
  const audit = auditBethesda88Contacts({ root });
  const { doc, path: stagedPath } = loadStaged({ root });
  if (!doc) return { ok: false, code: "RESEARCH_STAGE_ABSENT", audit };

  const beforeById = new Map((doc.opportunities || []).map((r) => [r.id, extractContactSnapshot(r)]));
  const applied = [];
  const updated = (doc.opportunities || []).map((row) => {
    const id = row.id || row.opportunityId;
    const evidence = ZERO_PAID_CONTACT_EVIDENCE_BY_ID[id];
    if (!evidence || evidence.inventory !== "historical") return row;
    if (!BETHESDA_29_ATTESTED_IDS.includes(id)) return row;
    const merged = mergeContactAntiClobber(row, evidence);
    const projected = applyHistoricalCommercialCategoryProjection(merged);
    // Force non-publication / non sales-ready.
    projected.salesReady = false;
    projected.customerFacing = false;
    projected.customerVisible = false;
    projected.publicationApproved = false;
    projected.publication = "NOT_AUTHORIZED";
    projected.contactQualityCompletion = {
      version: BETHESDA_88_CONTACT_QUALITY_COMPLETION_V1,
      appliedAt: new Date().toISOString(),
      source: "zero_paid_public_evidence",
      previous: beforeById.get(id) || null,
    };
    applied.push({
      id,
      before: beforeById.get(id),
      after: extractContactSnapshot(projected),
    });
    return projected;
  });

  const outDir = path.join(root, REPORT_DIR_REL);
  fs.mkdirSync(outDir, { recursive: true });
  writeJsonAtomic(path.join(outDir, "00-contact-coverage-audit.json"), audit);
  writeJsonAtomic(path.join(outDir, "01-historical-contact-apply-preview.json"), {
    dryRun,
    appliedCount: applied.length,
    applied,
  });

  if (dryRun) {
    return { ok: true, dryRun: true, audit, applied, writeApplied: false };
  }

  const persist = persistResearchStageOpportunityBag({
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    targetPath:
      stagedPath ||
      path.join(
        researchStageHotelDir(BETHESDA_CANONICAL_HOTEL_ID, { root }),
        "reconstructed-opportunities-v1.json"
      ),
    incomingDoc: {
      version: BETHESDA_88_CONTACT_QUALITY_COMPLETION_V1,
      hotelId: BETHESDA_CANONICAL_HOTEL_ID,
      hotelName: BETHESDA_CANONICAL_HOTEL_NAME,
      generatedAt: new Date().toISOString(),
      safety: {
        contactOnly: true,
        opportunitiesJsonWrites: false,
        customerFacing: false,
        salesReadyForced: false,
        paidResearch: false,
      },
      opportunities: updated,
    },
    protectedIds: [...BETHESDA_29_ATTESTED_IDS],
    root,
    label: "bethesda-88-contact-quality",
    mode: "merge",
  });

  const fixturePath = path.join(root, FIXTURE_REL);
  if (fs.existsSync(fixturePath)) {
    const prev = JSON.parse(fs.readFileSync(fixturePath, "utf8"));
    writeJsonAtomic(fixturePath, {
      ...prev,
      version: BETHESDA_88_CONTACT_QUALITY_COMPLETION_V1,
      generatedAt: new Date().toISOString(),
      opportunities: updated,
    });
  }

  writeJsonAtomic(path.join(outDir, "02-historical-apply-summary.json"), {
    dryRun: false,
    writeApplied: persist?.ok === true,
    appliedCount: applied.length,
    persistOk: persist?.ok === true,
  });

  return {
    ok: persist?.ok === true,
    dryRun: false,
    audit,
    applied,
    persist,
    writeApplied: persist?.ok === true,
  };
}

export function buildCanonicalContactDryRunPatches({ root = ROOT } = {}) {
  const store = loadHotelOpportunityDoc(BETHESDA_CANONICAL_HOTEL_ID, { root });
  const patches = [];
  for (const row of store.opportunities) {
    const evidence = ZERO_PAID_CONTACT_EVIDENCE_BY_ID[row.id];
    if (!evidence || evidence.inventory !== "canonical") continue;
    if (evidence.applied === true) continue; // already founder-applied (e.g. Woman's Club)
    if (evidence.rejected === true || evidence.founderValidation === "FAIL") continue;
    const before = extractContactSnapshot(row);
    const merged = mergeContactAntiClobber(row, evidence);
    const after = extractContactSnapshot(merged);
    if (
      before.coverage === after.coverage &&
      before.name === after.name &&
      (before.email || before.orgEmail) === (after.email || after.orgEmail)
    ) {
      continue;
    }
    patches.push({
      id: row.id,
      organizationName: row.organizationName,
      title: row.title,
      before,
      after,
      fieldDiff: {
        primaryContactName: {
          from: before.name,
          to: after.name,
        },
        // Direct person email only — never conflate with organization route.
        primaryContactEmail: {
          from: before.email || null,
          to: after.hasDirectEmail ? after.email : null,
        },
        organizationContactRoute: {
          from: before.orgEmail || before.orgRoute || null,
          to: after.orgEmail || after.orgRoute || null,
        },
        primaryContactRole: { from: before.title, to: after.title },
        coverage: { from: before.coverage, to: after.coverage },
        emailClassification: {
          from: before.hasDirectEmail
            ? "DIRECT_PERSON_EMAIL"
            : before.hasOrgRoute
              ? "ORGANIZATION_ROUTE"
              : "NONE",
          to: after.hasDirectEmail
            ? "DIRECT_PERSON_EMAIL"
            : after.hasOrgRoute
              ? "ORGANIZATION_ROUTE"
              : "NONE",
        },
      },
      requiresFounderApproval: true,
      opportunitiesJsonWriteAuthorized: false,
    });
  }
  const outDir = path.join(root, REPORT_DIR_REL);
  fs.mkdirSync(outDir, { recursive: true });
  writeJsonAtomic(path.join(outDir, "03-canonical-59-contact-dry-run-patches.json"), {
    version: BETHESDA_88_CONTACT_QUALITY_COMPLETION_V1,
    authorized: false,
    storeSha256: store.sha256,
    patchCount: patches.length,
    patches,
    note: "DRY-RUN ONLY. Do not write opportunities.json without explicit founder approval of these exact IDs/fields.",
  });
  writeJsonAtomic(path.join(outDir, "04-proposed-paid-contact-batch.json"), PROPOSED_PAID_CONTACT_BATCH_V1);
  return { ok: true, patches, storeSha256: store.sha256 };
}

export function runBethesda88ContactQualityCompletion({
  root = ROOT,
  dryRun = false,
} = {}) {
  const historical = applyHistoricalContactEnrichment({ root, dryRun });
  const canonical = buildCanonicalContactDryRunPatches({ root });
  const afterAudit = dryRun
    ? historical.audit
    : auditBethesda88Contacts({ root });
  return {
    ok: historical.ok !== false,
    dryRun,
    historical,
    canonical,
    afterAudit,
    proposedPaidBatch: PROPOSED_PAID_CONTACT_BATCH_V1,
  };
}
