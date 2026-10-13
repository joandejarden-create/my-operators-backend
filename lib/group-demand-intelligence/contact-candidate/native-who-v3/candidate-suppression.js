/**
 * Candidate suppression + confirmation caps for Native WHO V5.
 */

import { isStrictPersonName } from "./person-boundary.js";
import {
  sectionIsReject,
  sectionIsIdentityOnly,
  sectionAllowsWhoConfirmation,
  SECTION_PRIOR,
  classifySectionFromUrl,
} from "./section-semantics.js";

export const SUPPRESSION_CODE = Object.freeze({
  PERSON_BOUNDARY_ERROR: "PERSON_BOUNDARY_ERROR",
  WRONG_SECTION: "WRONG_SECTION",
  SPEAKER_NOT_ORGANIZER: "SPEAKER_NOT_ORGANIZER",
  BOARD_NOT_OPERATOR: "BOARD_NOT_OPERATOR",
  SPONSOR_NOT_ORGANIZER: "SPONSOR_NOT_ORGANIZER",
  PRESS_MENTION_ONLY: "PRESS_MENTION_ONLY",
  EXECUTIVE_WITHOUT_EVENT_ROLE: "EXECUTIVE_WITHOUT_EVENT_ROLE",
  WRONG_ORGANIZATION: "WRONG_ORGANIZATION",
  VENDOR_NOT_OWNER: "VENDOR_NOT_OWNER",
  TITLE_CONTEXT_BLEED: "TITLE_CONTEXT_BLEED",
  VOLUNTEER_NOT_OWNER: "VOLUNTEER_NOT_OWNER",
});

const MEETINGS_ROLE_RE =
  /\b(meetings?|events?|conference|convention|housing|registration|tournament|member services|partnerships?|audience engagement|director of programs|business development)\b/i;

/**
 * Suppress candidates before ranking/confirmation.
 */
export function suppressCandidate(candidate = {}, ctx = {}) {
  const name = String(candidate.name || "").trim();
  if (!isStrictPersonName(name)) {
    return { suppress: true, code: SUPPRESSION_CODE.PERSON_BOUNDARY_ERROR };
  }

  const section =
    candidate.sectionKind || classifySectionFromUrl(candidate.sourceUrl || "");
  if (sectionIsReject(section)) {
    if (section === "SPEAKERS") return { suppress: true, code: SUPPRESSION_CODE.SPEAKER_NOT_ORGANIZER };
    if (section === "BOARD") return { suppress: true, code: SUPPRESSION_CODE.BOARD_NOT_OPERATOR };
    if (section === "SPONSORS" || section === "EXHIBITORS") {
      return { suppress: true, code: SUPPRESSION_CODE.SPONSOR_NOT_ORGANIZER };
    }
    if (section === "VOLUNTEERS") {
      return { suppress: true, code: SUPPRESSION_CODE.VOLUNTEER_NOT_OWNER };
    }
    if (section === "VENDORS" || section === "PARTNERS") {
      return { suppress: true, code: SUPPRESSION_CODE.VENDOR_NOT_OWNER };
    }
    return { suppress: true, code: SUPPRESSION_CODE.WRONG_SECTION };
  }

  if (sectionIsIdentityOnly(section)) {
    return { suppress: true, code: SUPPRESSION_CODE.PRESS_MENTION_ONLY };
  }

  const role = String(candidate.role || "");
  if (
    /^(president|ceo|chairman)\b/i.test(role.trim()) &&
    !MEETINGS_ROLE_RE.test(role) &&
    !MEETINGS_ROLE_RE.test(candidate.evidenceQuote || "")
  ) {
    return { suppress: true, code: SUPPRESSION_CODE.EXECUTIVE_WITHOUT_EVENT_ROLE };
  }

  // Wrong org domain vs expected organization tokens
  const expected = String(ctx.organization || "").toLowerCase();
  const host = (() => {
    try {
      return new URL(candidate.sourceUrl || "").hostname.toLowerCase();
    } catch {
      return "";
    }
  })();
  if (expected && host) {
    const tokens = expected
      .replace(/[^a-z0-9\s]/g, " ")
      .split(/\s+/)
      .filter((t) => t.length >= 4)
      .slice(0, 3);
    const orgish = tokens.some((t) => host.includes(t));
    const official = Boolean(candidate.onOfficialDomain);
    if (
      !official &&
      !orgish &&
      /feeding|hungry|pan-florida|campresults|news|press/i.test(host + candidate.sourceUrl)
    ) {
      return { suppress: true, code: SUPPRESSION_CODE.WRONG_ORGANIZATION };
    }
  }

  if (!sectionAllowsWhoConfirmation(section) && SECTION_PRIOR[section] === "LOW") {
    // Allow through as CANDIDATE_FOUND only — caller decides; for confirm path suppress
    if (candidate.forConfirmation) {
      return { suppress: true, code: SUPPRESSION_CODE.WRONG_SECTION };
    }
  }

  return { suppress: false, code: null };
}

/**
 * Cap confirmed WHO: primary 1, secondary 1 (unless distributed responsibility).
 */
export function capConfirmedWho(rankedPeople = [], opts = {}) {
  const allowMulti = Boolean(opts.distributedResponsibility);
  const primaryMax = 1;
  const secondaryMax = allowMulti ? 2 : 1;

  const confirmed = rankedPeople.filter(
    (p) => p.researchState === "NAMED_PERSON_CONFIRMED" || p.gateOk
  );
  const validation = rankedPeople.filter(
    (p) => p.researchState === "NAMED_PERSON_NEEDS_VALIDATION"
  );

  const out = [];
  if (confirmed[0]) out.push({ ...confirmed[0], whoSlot: "PRIMARY" });
  for (let i = 1; i < confirmed.length && out.length < primaryMax + secondaryMax; i++) {
    out.push({ ...confirmed[i], whoSlot: "SECONDARY" });
  }

  // Do not promote validation-only into confirmed slots
  return {
    confirmed: out,
    candidatesFound: rankedPeople.length,
    suppressedValidation: validation.slice(0, 5),
  };
}

export function preferredRoleFamilies(eventFamily = "") {
  if (/SPORTS/i.test(eventFamily)) {
    return ["EVENT_OWNER", "HOUSING_OWNER", "CONFERENCE_DIRECTOR", "REGISTRATION_OWNER"];
  }
  return [
    "MEETINGS_OWNER",
    "CONFERENCE_DIRECTOR",
    "PARTNERSHIPS_OWNER",
    "EVENT_OWNER",
    "HOUSING_OWNER",
  ];
}
