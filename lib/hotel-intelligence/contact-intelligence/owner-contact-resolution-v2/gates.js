/**
 * False-positive / invalid-input gates for Contact Resolution V2.
 */

import { isRejectedOwnerDomain } from "./domain-resolver.js";
import { EMAIL_STATUS } from "./vocabulary.js";

export const GATES_V2 = "owner-contact-gates-v2";

const BRAND_EMPLOYEE_HOST =
  /marriott\.com|hilton\.com|hyatt\.com|ihg\.com|wyndham|choicehotels|accor\.com/i;

export function gateContactCandidate(candidate = {}, { owner_domain = null, brand_is_owner = false } = {}) {
  const rejected = [];
  const email = candidate.email || null;
  const domain = email ? String(email).split("@")[1] : null;

  if (email && domain && isRejectedOwnerDomain(domain)) {
    rejected.push("EMAIL_ON_REJECTED_DOMAIN");
  }
  if (email && domain && BRAND_EMPLOYEE_HOST.test(domain) && !brand_is_owner) {
    rejected.push("BRAND_EMPLOYEE_MISTAKEN_FOR_OWNER");
  }
  if (candidate.email_status === EMAIL_STATUS.INFERRED_UNVERIFIED) {
    rejected.push("INFERRED_UNVERIFIED_NOT_PUBLISHABLE");
  }
  if (/reservas?|reservations|frontdesk|concierge|booking/i.test(String(email || "").split("@")[0] || "")) {
    rejected.push("GENERIC_RESERVATIONS_EMAIL");
  }
  if (candidate.phone_type === "HOTEL_PHONE") {
    rejected.push("HOTEL_PHONE_AS_OWNER");
  }
  if (candidate.linkedin_url && owner_domain) {
    // Soft: LinkedIn alone without affiliation evidence
    if (!candidate.title && !candidate.full_name) {
      rejected.push("LINKEDIN_WITHOUT_IDENTITY");
    }
  }
  if (candidate.former_affiliation === true) {
    rejected.push("FORMER_EMPLOYEE_WITHOUT_CURRENT_EVIDENCE");
  }

  return {
    ok: rejected.length === 0,
    rejected,
    candidate: rejected.length ? null : candidate,
  };
}

export function retainRejected(candidate, reasons) {
  return {
    rejected: true,
    reasons,
    snapshot: {
      full_name: candidate.full_name || null,
      email: candidate.email || null,
      phone: candidate.phone || null,
      title: candidate.title || null,
      linkedin_url: candidate.linkedin_url || null,
    },
    retained_for_audit: true,
    at: new Date().toISOString(),
  };
}
