/**
 * Contact Intelligence V1 — negative screens (precision-first rejects).
 */

import { NEGATIVE_SCREEN, ROLE_CURRENCY, VERIFICATION_STATUS } from "./vocabulary.js";
import { isPersonalEmailDomain } from "./email-verification.js";
import { FRESHNESS } from "./vocabulary.js";

export const NEGATIVE_SCREENS_VERSION = "contact-negative-screens-v1";

/**
 * Evaluate person/contact candidate against negative screens.
 * Returns list of triggered screen codes (empty = pass).
 */
export function evaluateNegativeScreens(candidate = {}) {
  const hits = [];
  const name = String(candidate.display_name || candidate.name || "").toLowerCase();
  const org = String(candidate.organization_name || "").toLowerCase();
  const targetOrg = String(candidate.target_organization_name || "").toLowerCase();
  const email = String(candidate.email || "").toLowerCase();
  const title = String(candidate.title || "").toLowerCase();
  const affiliation = String(candidate.affiliation_status || "").toUpperCase();
  const roleCurrency = String(candidate.role_currency || "").toUpperCase();
  const verification = String(candidate.verification_status || "").toUpperCase();
  const freshness = String(candidate.freshness || "").toUpperCase();
  const sourceClass = String(candidate.source_class || "").toUpperCase();

  if (candidate.wrong_person === true) hits.push(NEGATIVE_SCREEN.WRONG_PERSON);

  if (candidate.same_name_collision === true || candidate.name_collision_unresolved === true) {
    hits.push(NEGATIVE_SCREEN.SAME_NAME_COLLISION);
  }

  if (
    candidate.former_affiliation === true ||
    affiliation === "FORMER" ||
    roleCurrency === ROLE_CURRENCY.STALE ||
    /former|ex-|previously|ya no/.test(title)
  ) {
    hits.push(NEGATIVE_SCREEN.FORMER_EMPLOYEE_NOT_CURRENT);
  }

  if (targetOrg && org && targetOrg !== org && candidate.org_match === false) {
    hits.push(NEGATIVE_SCREEN.WRONG_COMPANY);
  }

  if (candidate.adjacent_company === true) hits.push(NEGATIVE_SCREEN.ADJACENT_COMPANY);

  if (candidate.operator_side === true || /general manager|hotel manager|\bgm\b/.test(title)) {
    if (candidate.allow_operator_side !== true) {
      hits.push(NEGATIVE_SCREEN.OPERATOR_PERSON_NOT_OWNER_PERSON);
    }
  }

  if (candidate.brand_employee === true || /brand\b|franchise support/.test(title)) {
    hits.push(NEGATIVE_SCREEN.BRAND_PERSON_NOT_OWNER_PERSON);
  }

  if (
    /hr\b|human resources|marketing coordinator|receptionist|talent acquisition/.test(title) ||
    candidate.decision_relevant === false
  ) {
    hits.push(NEGATIVE_SCREEN.PERSON_NOT_DECISION_RELEVANT);
  }

  if (
    verification === VERIFICATION_STATUS.INFERRED ||
    verification === VERIFICATION_STATUS.DOMAIN_PATTERN_SUPPORTED
  ) {
    if (candidate.claimed_verified === true) {
      hits.push(NEGATIVE_SCREEN.EMAIL_PATTERN_UNVERIFIED);
    }
  }

  if (
    verification === VERIFICATION_STATUS.CATCH_ALL_PROBABLE &&
    candidate.claimed_verified === true
  ) {
    hits.push(NEGATIVE_SCREEN.CATCH_ALL_NOT_VERIFIED);
  }

  if (freshness === FRESHNESS.STALE || verification === VERIFICATION_STATUS.STALE) {
    hits.push(NEGATIVE_SCREEN.STALE_CONTACT);
  }

  if (email && isPersonalEmailDomain(email)) {
    hits.push(NEGATIVE_SCREEN.PERSONAL_EMAIL_NOT_BUSINESS_CONTACT);
  }

  if (candidate.personal_mobile_not_public_business === true) {
    hits.push(NEGATIVE_SCREEN.PERSONAL_PHONE_NOT_PUBLIC_BUSINESS_CONTACT);
  }

  if (sourceClass === "SNIPPET" || candidate.search_snippet_only === true) {
    hits.push(NEGATIVE_SCREEN.SEARCH_SNIPPET_NOT_DECISIVE_EVIDENCE);
  }

  // Soft name presence check for collision hints when two orgs share a common name
  if (candidate.ambiguous_identity === true && name) {
    if (!hits.includes(NEGATIVE_SCREEN.SAME_NAME_COLLISION)) {
      hits.push(NEGATIVE_SCREEN.SAME_NAME_COLLISION);
    }
  }

  return [...new Set(hits)];
}

export function passesNegativeScreens(candidate = {}) {
  return evaluateNegativeScreens(candidate).length === 0;
}
