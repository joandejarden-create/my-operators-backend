/**
 * Align PE commercial-path labels with usable-route + grade contract.
 * Homepage-only ≠ ORGANIZATION_PATH; email / phone / events inquiry URL can qualify.
 * Never claim "organization contact path" alongside Grade E.
 */

import { CONTACT_GRADE, CONTACT_GRADE_LABEL } from "../claim-types.js";
import { hasUsableContactRoute } from "../prospect-publication-eligibility-v1.js";
import { gradeContact } from "../contact-resolution.js";

const PATH_LABELS = Object.freeze({
  NAMED_DIRECT: "Named contact",
  FUNCTIONAL: "Functional contact (events / sales)",
  ORGANIZATION_PATH: "Venue / organization contact path",
  NO_CONTACT: "No contact path identified",
});

function text(v) {
  return String(v || "").trim();
}

export function resolvePeContactConsistency(opp = {}, venue = {}) {
  const email =
    text(venue.eventContactEmail) ||
    text(opp.contactEmail) ||
    text(opp.primaryContact?.email) ||
    "";
  const phone =
    text(venue.eventContactPhone) ||
    text(opp.contactPhone) ||
    text(opp.primaryContact?.phone) ||
    "";
  const contactUrl =
    text(opp.contactUrl) ||
    text(opp.contactOfficialUrl) ||
    text(venue.organizationPath) ||
    text(venue.website) ||
    "";
  const name =
    text(venue.eventContactName) ||
    text(opp.primaryContact?.name) ||
    text(opp.contactName) ||
    "";
  const role =
    text(venue.eventContactRole) ||
    text(opp.primaryContact?.role) ||
    text(opp.contactRole) ||
    "Venue events / rentals";

  const probe = {
    ...opp,
    contactEmail: email || opp.contactEmail,
    contactPhone: phone || opp.contactPhone,
    contactUrl: contactUrl || opp.contactUrl,
    contactMethod: email || phone || contactUrl || opp.contactMethod,
    primaryContact: {
      ...(opp.primaryContact || {}),
      name: name || opp.primaryContact?.name,
      email: email || null,
      phone: phone || null,
      role,
    },
  };
  const route = hasUsableContactRoute(probe);
  const graded = gradeContact(
    {
      name: name || (route.ok ? "Venue events team" : null),
      email: email || null,
      phone: phone || null,
      role,
      emailVerificationStatus: email ? "OFFICIAL_SOURCE_VERIFIED" : null,
      relationshipToEvent: role,
    },
    probe
  );

  let commercialContactPath = "NO_CONTACT";
  let contactGrade = CONTACT_GRADE.E;
  let contactGradeLabel = CONTACT_GRADE_LABEL.E;

  if (route.ok) {
    if (email || phone) {
      commercialContactPath = name ? "NAMED_DIRECT" : "FUNCTIONAL";
    } else {
      commercialContactPath = "ORGANIZATION_PATH";
    }
    if (graded.contactGrade === CONTACT_GRADE.E) {
      contactGrade = CONTACT_GRADE.D;
      contactGradeLabel = CONTACT_GRADE_LABEL.D;
    } else {
      contactGrade = graded.contactGrade;
      contactGradeLabel = graded.contactGradeLabel;
    }
  }

  return {
    commercialContactPath,
    commercialContactPathLabel:
      PATH_LABELS[commercialContactPath] || commercialContactPath,
    contactGrade,
    contactGradeLabel,
    contactQuality: route.ok
      ? email || phone
        ? "FUNCTIONAL"
        : "ORGANIZATION_PATH"
      : "NO_CONTACT",
    contactEmail: email || null,
    contactPhone: phone || null,
    contactUrl: route.ok ? contactUrl || null : null,
    contactOfficialUrl: route.ok ? contactUrl || null : null,
    primaryContact: {
      name: name || (route.ok ? venue.venueName || "Venue events team" : null),
      fullName: name || (route.ok ? venue.venueName || "Venue events team" : null),
      role,
      title: role,
      email: email || null,
      phone: phone || null,
      contactGrade,
      contactGradeLabel,
      contactQuality: route.ok ? "FUNCTIONAL" : "NO_CONTACT",
      source: contactUrl || venue.website || null,
      sourceUrl: contactUrl || venue.website || null,
      whyThisContact: route.ok
        ? `Usable commercial route (${route.reason}) for venue partnership / lodging referral discussion.`
        : "No usable email, phone, contact form, or events-specific inquiry URL established — homepage alone is not a commercial route.",
    },
    usableContactRoute: route,
  };
}
