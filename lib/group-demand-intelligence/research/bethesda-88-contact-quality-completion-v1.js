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
  if (/conference staff|tournaments? desk|hotel information|events? page/i.test(n)) {
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
  if (/\b(mansion|marriott|hotel|club|association|council|chamber|society)\b/i.test(n)) {
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
    incPrimary.directPersonEmail === false ||
    text(incPrimary.contactClass) === "FUNCTIONAL_INBOX" ||
    isGenericOrganizationEmail(incEmail);
  if (incEmail && incEmail.includes("@")) {
    if (incAsOrgRoute) {
      if (!text(nextPrimary.organizationContactRoute)) {
        nextPrimary.organizationContactRoute = incEmail;
      }
      if (incPrimary.emailOwnership) {
        nextPrimary.emailOwnership = text(incPrimary.emailOwnership);
      }
      if (incPrimary.directPersonEmail === false) {
        nextPrimary.directPersonEmail = false;
      }
      // Do not store role/org inboxes as person-direct email.
      if (
        isGenericOrganizationEmail(nextPrimary.email) ||
        text(nextPrimary.emailOwnership).toLowerCase() === "organization_or_department"
      ) {
        // keep existing ownership flags; person email stays unset/cleared below when org-only
      }
    } else if (!text(nextPrimary.email) || isGenericOrganizationEmail(nextPrimary.email)) {
      nextPrimary.email = incEmail;
    }
  }
  if (text(incPrimary.organizationContactRoute) && !text(nextPrimary.organizationContactRoute)) {
    nextPrimary.organizationContactRoute = text(incPrimary.organizationContactRoute);
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
    nextPrimary.directPersonEmail === false ||
    text(nextPrimary.contactClass) === "FUNCTIONAL_INBOX";
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
      name: null,
      title: "Venue inquiry desk — The Cordell",
      email: "info@thecordell.com",
      phone: "(301) 500-1177",
      contactClass: "FUNCTIONAL_INBOX",
      organizationContactRoute: "info@thecordell.com",
      whyRelevant:
        "Public venue inquiry inbox/phone only — named events manager not published; functional route for partnership watch.",
      sourceUrl: "https://thecordell.com/contact-the-cordell-downtown-bethesda-event-venue",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid Cordell contact page 2026-11-12 — no verified named person found.",
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
      name: null,
      title: "Aledade headquarters / corporate events desk (organization route)",
      email: "info@aledade.com",
      phone: "(202) 803-7979",
      contactClass: "FUNCTIONAL_INBOX",
      organizationContactRoute: "info@aledade.com",
      whyRelevant:
        "Public HQ inbox/phone for Bethesda-based corporate meetings — named Corporate Events Planner not published on current public pages.",
      sourceUrl: "https://aledade.com/more-information",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid Aledade contact page 2026-11-12 — no verified named buyer published.",
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
    primaryContact: {
      name: null,
      title: "U.S. IFE Conference organizing desk",
      email: "econ@lle.rochester.edu",
      contactClass: "FUNCTIONAL_INBOX",
      organizationContactRoute: "econ@lle.rochester.edu",
      whyRelevant:
        "Public conference questions inbox for 2027 U.S. IFE at Bethesda North — overflow lodging research route; named buyer not published.",
      sourceUrl: "https://usife.org/events/2027-u-s-ife-conference/",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid US IFE 2027 conference page 2026-11-12 — org route only.",
    },
  },
  disp_recLuxvwwxID7U2B8_strathmore: {
    inventory: "historical",
    applied: true,
    primaryContact: {
      name: null,
      title: "Strathmore Group Sales (organization route)",
      email: "groupsales@strathmore.org",
      emailOwnership: "organization_or_department",
      phone: "301-581-5199",
      contactClass: "FUNCTIONAL_INBOX",
      organizationContactRoute: "groupsales@strathmore.org",
      whyRelevant:
        "Founder-approved org routes — groupsales@ / rentals@ / 301-581-5199. No named buyer invented.",
      sourceUrl:
        "https://www.rockvillechamber.org/2026/01/30/plan-the-perfect-group-outing-at-strathmore/",
      verifiedAt: "2026-11-12",
    },
    secondaryContact: {
      name: null,
      title: "Strathmore venue rentals desk",
      email: "rentals@strathmore.org",
      emailOwnership: "organization_or_department",
      contactClass: "FUNCTIONAL_INBOX",
      sourceUrl: "https://directory.bizbash.com/venue/amp-by-strathmore/",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Founder-approved and applied 2026-11-12.",
    },
  },

  // Historical research-stage bodies (persist via protected merge)
  gdi_beth_v16_evt_0ab91e208405: {
    inventory: "historical",
    primaryContact: {
      name: null,
      title: "IFPAC conference management — InfoScience Services",
      email: "info@ifpacnet.org",
      phone: "847-543-6800",
      contactClass: "FUNCTIONAL_INBOX",
      organizationContactRoute: "info@ifpacnet.org",
      whyRelevant:
        "Public IFPAC-2027 organizer inbox (Bethesda North host) — overflow lodging research route; named buyer not published.",
      sourceUrl: "https://ifpacglobal.org/",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid IFPAC site 2026-11-12.",
    },
  },
  gdi_beth_v16_evt_3c8228774517: {
    inventory: "historical",
    primaryContact: {
      name: null,
      title: "U.S. IFE Conference organizing desk",
      email: "econ@lle.rochester.edu",
      contactClass: "FUNCTIONAL_INBOX",
      organizationContactRoute: "econ@lle.rochester.edu",
      whyRelevant:
        "Public IFE 2027 questions inbox — overflow lodging research; named buyer not published.",
      sourceUrl: "https://usife.org/events/2027-u-s-ife-conference/",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid US IFE page 2026-11-12 — org route only.",
    },
  },
  gdi_beth_v16_evt_90a6ddcb10ca: {
    inventory: "historical",
    primaryContact: {
      name: null,
      title: "ΚΚΨ & ΤΒΣ National Headquarters",
      email: "kkytbs@kkytbs.org",
      phone: "(405) 372-2333",
      contactClass: "FUNCTIONAL_INBOX",
      organizationContactRoute: "kkytbs@kkytbs.org",
      whyRelevant:
        "National HQ inbox for 2027 convention at Bethesda North — housing/logistics commercial entry; named housing manager not published.",
      sourceUrl: "https://natcon.kkytbsonline.com/contact-us/",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid KKPsi/TBS NatCon contact page 2026-11-12.",
    },
  },
  gdi_opp_series_request_a_conference_registration_website_2026: {
    inventory: "canonical",
    primaryContact: {
      name: null,
      title: "CCR Conferences — NCI RNA Biology Symposium",
      email: "ccrconferences@mail.nih.gov",
      contactClass: "FUNCTIONAL_INBOX",
      organizationContactRoute: "ccrconferences@mail.nih.gov",
      whyRelevant:
        "Public CCR conference inbox for 2027 NCI RNA Biology Symposium lodging list (includes Bethesda Marriott).",
      sourceUrl:
        "https://ncifrederick.cancer.gov/events/conferences/2027NCI_RNA_Symposium/accommodations",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid NCI RNA accommodations page 2026-11-12. CANONICAL — dry-run only.",
    },
  },
  gdi_opp_2027_nci_rna_biology_symposium_7: {
    inventory: "canonical",
    primaryContact: {
      name: null,
      title: "CCR Conferences — NCI RNA Biology Symposium",
      email: "ccrconferences@mail.nih.gov",
      contactClass: "FUNCTIONAL_INBOX",
      organizationContactRoute: "ccrconferences@mail.nih.gov",
      whyRelevant:
        "Public CCR conference inbox for 2027 NCI RNA Biology Symposium lodging list.",
      sourceUrl:
        "https://ncifrederick.cancer.gov/events/conferences/2027NCI_RNA_Symposium/accommodations",
      verifiedAt: "2026-11-12",
    },
    contactRecovery: {
      note: "Zero-paid NCI RNA accommodations page 2026-11-12. CANONICAL — dry-run only.",
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
    "disp_recLuxvwwxID7U2B8_montgomery_county_shrm",
    "disp_recLuxvwwxID7U2B8_iwlca_elite_tournaments_iwlca_champions_cup",
    "gdi_pep_live_c165c897d233",
  ],
  missingEvidence: {
    disp_recLuxvwwxID7U2B8_montgomery_county_shrm:
      "Only mcshrm@gmail.com functional inbox; no verified named chapter programs/events chair",
    disp_recLuxvwwxID7U2B8_iwlca_elite_tournaments_iwlca_champions_cup:
      "Only reservations@halperntravel.com; no named housing manager for Champions Cup 2027",
    gdi_pep_live_c165c897d233:
      "Carrie Rice named (Exa) but no verified direct email/LinkedIn/phone in zero-paid public sources",
  },
  commercialRationale: {
    disp_recLuxvwwxID7U2B8_montgomery_county_shrm:
      "Recurring local chapter meetings — named buyer unlocks actionable outreach",
    disp_recLuxvwwxID7U2B8_iwlca_elite_tournaments_iwlca_champions_cup:
      "Stay-to-play overflow lodging — named housing contact is the booking path",
    gdi_pep_live_c165c897d233:
      "Planner partnership — direct route converts named principal into outreachable lead",
  },
  expectedImpact:
    "Up to +3 named-with-direct-email (bucket A) or +3 named-with-org-route (bucket B); reduces high-priority historical gaps",
  note: "NOT AUTHORIZATION. Prior three-call Exa authorization exhausted — do not reuse remaining monetary allowance. Do not execute previously proposed $20 batch. Stop for explicit founder approval before any paid request. Strathmore removed (org routes already applied).",
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
  const proposed = evidence
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
        primaryContactEmail: {
          from: before.email || before.orgEmail,
          to: after.email || after.orgEmail,
        },
        primaryContactRole: { from: before.title, to: after.title },
        coverage: { from: before.coverage, to: after.coverage },
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
