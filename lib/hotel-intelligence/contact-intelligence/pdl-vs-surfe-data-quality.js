/**
 * Fair PDL-vs-Surfe data-quality comparison — offline utilities.
 * No network. No canonical writes. Evaluation staging artifacts only.
 *
 * Retention: Surfe central-retention/reuse conditions remain UNRESOLVED in-repo
 * (no committed Marta letter). Do not assume staging exempts retained Surfe data.
 * PDL: coded paraphrase of written embedded-product approval (2026-09); Amanda
 * not named in-repo — scope gaps flagged in data-rights matrix.
 */

export const PDL_VS_SURFE_DQ_VERSION = "pdl-vs-surfe-data-quality-v1";

/** Observation status for a contact channel on one provider. */
export const FIELD_STATUS = Object.freeze({
  NOT_REQUESTED: "NOT_REQUESTED",
  NOT_TESTED: "NOT_TESTED",
  NO_RESULT: "NO_RESULT",
  MASKED: "MASKED",
  FIELD_NOT_ENTITLED: "FIELD_NOT_ENTITLED",
  ERROR: "ERROR",
  RETURNED: "RETURNED",
});

export const EMAIL_AGREEMENT = Object.freeze({
  EXACT_SAME: "EXACT_SAME",
  SAME_TARGET_DOMAIN_DIFFERENT_LOCAL: "SAME_TARGET_DOMAIN_DIFFERENT_LOCAL",
  DIFFERENT_PROFESSIONAL_DOMAINS: "DIFFERENT_PROFESSIONAL_DOMAINS",
  PERSONAL_VS_PROFESSIONAL: "PERSONAL_VS_PROFESSIONAL",
  PDL_ONLY: "PDL_ONLY",
  SURFE_ONLY: "SURFE_ONLY",
  NEITHER: "NEITHER",
  NOT_COMPARABLE: "NOT_COMPARABLE",
});

export const PHONE_AGREEMENT = Object.freeze({
  OVERLAP_SAME_E164: "OVERLAP_SAME_E164",
  OVERLAP_PARTIAL: "OVERLAP_PARTIAL",
  DISJOINT: "DISJOINT",
  PDL_ONLY: "PDL_ONLY",
  SURFE_ONLY: "SURFE_ONLY",
  NEITHER: "NEITHER",
  NOT_COMPARABLE: "NOT_COMPARABLE",
});

export const REVIEW_LABEL = Object.freeze({
  PROVISIONAL_AI_REVIEW: "PROVISIONAL_AI_REVIEW",
  HUMAN_REVIEWED: "HUMAN_REVIEWED",
  UNRESOLVED: "UNRESOLVED",
});

const PERSONAL_HOSTS = new Set([
  "gmail.com",
  "googlemail.com",
  "hotmail.com",
  "hotmail.es",
  "hotmail.com.mx",
  "outlook.com",
  "outlook.es",
  "live.com",
  "yahoo.com",
  "yahoo.com.mx",
  "yahoo.es",
  "icloud.com",
  "me.com",
  "aol.com",
  "protonmail.com",
  "mail.com",
]);

export function hostOf(domainOrUrl) {
  const s = String(domainOrUrl || "")
    .trim()
    .toLowerCase()
    .replace(/^https?:\/\//, "")
    .replace(/^www\./, "")
    .split("/")[0]
    .split("?")[0];
  return s;
}

export function normalizeEmail(email) {
  const e = String(email || "")
    .trim()
    .toLowerCase()
    .replace(/^mailto:/, "");
  if (!e || !e.includes("@")) return null;
  const [local, domain] = e.split("@");
  if (!local || !domain) return null;
  return `${local}@${hostOf(domain)}`;
}

export function emailLocalPart(email) {
  const n = normalizeEmail(email);
  return n ? n.split("@")[0] : null;
}

export function emailDomain(email) {
  const n = normalizeEmail(email);
  return n ? n.split("@")[1] : null;
}

export function isPersonalEmailHost(domainOrEmail) {
  const d = domainOrEmail?.includes("@")
    ? emailDomain(domainOrEmail)
    : hostOf(domainOrEmail);
  return Boolean(d && PERSONAL_HOSTS.has(d));
}

export function domainsRelated(a, b) {
  const ha = hostOf(a);
  const hb = hostOf(b);
  if (!ha || !hb) return false;
  return ha === hb || ha.endsWith(`.${hb}`) || hb.endsWith(`.${ha}`);
}

/**
 * Conservative E.164 when digits + country evidence support it.
 * Preserves original + extension separately. Never invents country.
 */
export function normalizePhoneToE164(raw, { defaultCountry = null } = {}) {
  const original = raw == null ? null : String(raw).trim();
  if (!original) {
    return {
      original: null,
      e164: null,
      extension: null,
      status: "EMPTY",
      note: null,
    };
  }
  const extMatch = original.match(/(?:ext\.?|x|extension)\s*[:.]?\s*(\d+)/i);
  const extension = extMatch ? extMatch[1] : null;
  const withoutExt = extMatch
    ? original.slice(0, extMatch.index).trim()
    : original;
  let digits = withoutExt.replace(/[^\d+]/g, "");
  if (digits.startsWith("00")) digits = `+${digits.slice(2)}`;
  const hasPlus = digits.startsWith("+");
  const justDigits = digits.replace(/\D/g, "");

  if (hasPlus && justDigits.length >= 8 && justDigits.length <= 15) {
    return {
      original,
      e164: `+${justDigits}`,
      extension,
      status: "E164",
      note: null,
    };
  }

  const cc = String(defaultCountry || "").toUpperCase();
  const countryDial = {
    MX: "52",
    BR: "55",
    DO: "1",
    US: "1",
    CR: "506",
    PA: "507",
    CO: "57",
    JM: "1",
    BB: "1",
    BS: "1",
    TT: "1",
    PR: "1",
  };
  const dial = countryDial[cc] || null;
  if (dial && justDigits.length >= 8 && justDigits.length <= 12) {
    const body = justDigits.startsWith(dial) ? justDigits : `${dial}${justDigits}`;
    if (body.length <= 15) {
      return {
        original,
        e164: `+${body}`,
        extension,
        status: "E164_INFERRED_COUNTRY",
        note: `defaultCountry=${cc}`,
      };
    }
  }

  return {
    original,
    e164: null,
    extension,
    status: "UNNORMALIZED",
    note: "Insufficient country evidence for E.164",
  };
}

export function classifyEmailAgreement({
  pdlWorkEmail = null,
  pdlPersonalEmails = [],
  surfeEmail = null,
  targetDomain = null,
  pdlEmailStatus = null,
  surfeEmailStatus = null,
} = {}) {
  // Missing historical Surfe test ≠ Surfe failure — not a paired coverage cell.
  if (
    surfeEmailStatus === FIELD_STATUS.NOT_TESTED ||
    pdlEmailStatus === FIELD_STATUS.NOT_TESTED
  ) {
    return {
      cell: EMAIL_AGREEMENT.NOT_COMPARABLE,
      reason: "provider_not_tested",
      pdl: normalizeEmail(pdlWorkEmail),
      surfe: normalizeEmail(surfeEmail),
    };
  }

  const comparable =
    pdlEmailStatus === FIELD_STATUS.RETURNED ||
    surfeEmailStatus === FIELD_STATUS.RETURNED ||
    Boolean(pdlWorkEmail) ||
    Boolean(surfeEmail);
  if (
    !comparable &&
    [pdlEmailStatus, surfeEmailStatus].some((s) =>
      [
        FIELD_STATUS.NOT_REQUESTED,
        FIELD_STATUS.FIELD_NOT_ENTITLED,
        FIELD_STATUS.MASKED,
      ].includes(s)
    )
  ) {
    return { cell: EMAIL_AGREEMENT.NOT_COMPARABLE, reason: "channel_not_observable" };
  }

  const pdl = normalizeEmail(pdlWorkEmail);
  const surfe = normalizeEmail(surfeEmail);
  const target = hostOf(targetDomain);

  if (!pdl && !surfe) return { cell: EMAIL_AGREEMENT.NEITHER };
  if (pdl && !surfe) return { cell: EMAIL_AGREEMENT.PDL_ONLY, pdl, surfe: null };
  if (!pdl && surfe) return { cell: EMAIL_AGREEMENT.SURFE_ONLY, pdl: null, surfe };

  if (pdl === surfe) return { cell: EMAIL_AGREEMENT.EXACT_SAME, pdl, surfe };

  const pd = emailDomain(pdl);
  const sd = emailDomain(surfe);
  if (target && domainsRelated(pd, target) && domainsRelated(sd, target)) {
    return {
      cell: EMAIL_AGREEMENT.SAME_TARGET_DOMAIN_DIFFERENT_LOCAL,
      pdl,
      surfe,
    };
  }

  const pdlPersonal = (pdlPersonalEmails || [])
    .map(normalizeEmail)
    .filter(Boolean);
  if (
    (isPersonalEmailHost(pdl) && !isPersonalEmailHost(surfe)) ||
    (!isPersonalEmailHost(pdl) && isPersonalEmailHost(surfe)) ||
    pdlPersonal.includes(surfe) ||
    pdlPersonal.includes(pdl)
  ) {
    // Personal vs professional is about address class — not affiliation conflict.
    if (isPersonalEmailHost(pdl) !== isPersonalEmailHost(surfe)) {
      return { cell: EMAIL_AGREEMENT.PERSONAL_VS_PROFESSIONAL, pdl, surfe };
    }
  }

  return { cell: EMAIL_AGREEMENT.DIFFERENT_PROFESSIONAL_DOMAINS, pdl, surfe };
}

export function classifyPhoneAgreement({
  pdlPhones = [],
  surfePhones = [],
  pdlPhoneStatus = null,
  surfePhoneStatus = null,
  defaultCountry = null,
} = {}) {
  if (
    pdlPhoneStatus === FIELD_STATUS.NOT_REQUESTED ||
    surfePhoneStatus === FIELD_STATUS.NOT_REQUESTED ||
    pdlPhoneStatus === FIELD_STATUS.NOT_TESTED ||
    surfePhoneStatus === FIELD_STATUS.NOT_TESTED
  ) {
    const pdlObs =
      pdlPhoneStatus === FIELD_STATUS.RETURNED || (pdlPhones || []).length > 0;
    const surfeObs =
      surfePhoneStatus === FIELD_STATUS.RETURNED || (surfePhones || []).length > 0;
    if (!pdlObs || !surfeObs) {
      return { cell: PHONE_AGREEMENT.NOT_COMPARABLE, reason: "phone_channel_asymmetric" };
    }
  }

  const normSet = (arr) => {
    const out = [];
    for (const p of arr || []) {
      const raw = typeof p === "string" ? p : p?.number || p?.phone || null;
      const type =
        typeof p === "object"
          ? p.type || p.phone_type || p.provider_phone_field || "UNKNOWN"
          : "UNKNOWN";
      const n = normalizePhoneToE164(raw, { defaultCountry });
      if (n.e164 || n.original) {
        out.push({
          ...n,
          type: String(type),
          business_use:
            typeof p === "object" ? p.business_use || "UNKNOWN" : "UNKNOWN",
        });
      }
    }
    return out;
  };

  const pdlN = normSet(pdlPhones);
  const surfeN = normSet(surfePhones);
  const pdlKeys = new Set(pdlN.map((p) => p.e164 || p.original).filter(Boolean));
  const surfeKeys = new Set(
    surfeN.map((p) => p.e164 || p.original).filter(Boolean)
  );
  const overlap = [...pdlKeys].filter((k) => surfeKeys.has(k));

  if (!pdlKeys.size && !surfeKeys.size) return { cell: PHONE_AGREEMENT.NEITHER, pdlN, surfeN };
  if (pdlKeys.size && !surfeKeys.size)
    return { cell: PHONE_AGREEMENT.PDL_ONLY, pdlN, surfeN, overlap };
  if (!pdlKeys.size && surfeKeys.size)
    return { cell: PHONE_AGREEMENT.SURFE_ONLY, pdlN, surfeN, overlap };
  if (overlap.length && overlap.length === Math.min(pdlKeys.size, surfeKeys.size)) {
    return { cell: PHONE_AGREEMENT.OVERLAP_SAME_E164, pdlN, surfeN, overlap };
  }
  if (overlap.length) {
    return { cell: PHONE_AGREEMENT.OVERLAP_PARTIAL, pdlN, surfeN, overlap };
  }
  return { cell: PHONE_AGREEMENT.DISJOINT, pdlN, surfeN, overlap: [] };
}

/**
 * Corrected cost denominators: include reused paid calls in cohort cost when
 * their results sit in the coverage denominator; show new-run cost separately.
 */
export function correctedCostDenominators({
  newRunCredits = 0,
  newRunRequests = 0,
  reusedPaidCredits = 0,
  reusedPaidRequests = 0,
  attributableWorkEmailsInDenom = 0,
  emailAndPhoneInDenom = 0,
} = {}) {
  const cohortCredits = Number(newRunCredits) + Number(reusedPaidCredits);
  const cohortRequests = Number(newRunRequests) + Number(reusedPaidRequests);
  const safeDiv = (n, d) => (d > 0 ? n / d : null);
  return {
    new_run: {
      credits: newRunCredits,
      requests: newRunRequests,
    },
    reused_paid_included_in_denominator: {
      credits: reusedPaidCredits,
      requests: reusedPaidRequests,
    },
    cohort_total_when_reuse_in_denom: {
      credits: cohortCredits,
      requests: cohortRequests,
    },
    credits_per_attributable_work_email_new_run_only: safeDiv(
      newRunCredits,
      attributableWorkEmailsInDenom
    ),
    credits_per_attributable_work_email_cohort_including_reuse: safeDiv(
      cohortCredits,
      attributableWorkEmailsInDenom
    ),
    credits_per_email_and_phone_cohort_including_reuse: safeDiv(
      cohortCredits,
      emailAndPhoneInDenom
    ),
    note:
      "Include reused paid call credits in cohort cost when reused results are in the coverage denominator. Report new-run cost separately.",
  };
}

export function hardCapReservation({
  pdlPersonCap = 60,
  surfeEmailCap = 60,
  surfeMobileCap = 60,
  searchCap = 0,
} = {}) {
  return {
    pdl: {
      max_person_enrichments: pdlPersonCap,
      max_credits_worst_case: pdlPersonCap,
      retries: 0,
      purchases: false,
    },
    surfe: {
      max_email_credits: surfeEmailCap,
      max_mobile_credits: surfeMobileCap,
      max_search_credits: searchCap,
      retries: 0,
      purchases: false,
    },
    other_paid_providers: 0,
    note:
      "Ceilings are proposals only. Do not execute until billing balances are independently confirmed sufficient for worst-case reservations.",
  };
}

/**
 * Independent review scaffold. Automated judgments are PROVISIONAL_AI_REVIEW.
 * Does not claim deliverability or phone ownership verification.
 */
export function provisionalIndependentReview({
  subject = {},
  disagreement = false,
  evidenceSnippets = [],
  identityOk = null,
  affiliationOk = null,
  roleRelevant = null,
  contactAttributed = null,
} = {}) {
  const hasEvidence = (evidenceSnippets || []).length > 0;
  const dims = {
    correct_person: identityOk == null ? "UNRESOLVED" : identityOk ? "SUPPORTED" : "NOT_SUPPORTED",
    correct_target_company_affiliation:
      affiliationOk == null ? "UNRESOLVED" : affiliationOk ? "SUPPORTED" : "NOT_SUPPORTED",
    relevant_decision_maker_role:
      roleRelevant == null ? "UNRESOLVED" : roleRelevant ? "SUPPORTED" : "NOT_SUPPORTED",
    contact_attributed_to_person:
      contactAttributed == null ? "UNRESOLVED" : contactAttributed ? "SUPPORTED" : "NOT_SUPPORTED",
    provider_reported_deliverability_type: "PROVIDER_METADATA_ONLY",
    independently_verified_contact_status: "NOT_VERIFIED_NO_TEST_SEND",
  };

  let overall = REVIEW_LABEL.PROVISIONAL_AI_REVIEW;
  if (!hasEvidence && disagreement) overall = REVIEW_LABEL.UNRESOLVED;
  if (
    disagreement &&
    [dims.correct_person, dims.correct_target_company_affiliation, dims.contact_attributed_to_person]
      .filter((x) => x === "UNRESOLVED").length >= 2
  ) {
    overall = REVIEW_LABEL.UNRESOLVED;
  }

  return {
    label: overall,
    disagreement,
    subject_id: subject.id || subject.subject_id || null,
    dimensions: dims,
    evidence_refs: evidenceSnippets,
    human_review_required: overall !== REVIEW_LABEL.HUMAN_REVIEWED,
    note:
      "Provider agreement is corroboration, not independent verification. Disagreement ≠ automatic error.",
  };
}

export function assertNoCanonicalWrites(flags = {}) {
  const banned = [
    "canonical_writes",
    "customer_publication",
    "outreach",
    "airtable_write",
    "census_write",
  ];
  for (const k of banned) {
    if (flags[k] === true || flags[k] === "ENABLED") {
      throw new Error(`Canonical/product write flag enabled: ${k}`);
    }
  }
  return true;
}

export function enforceRequestCap({ made = 0, cap = 0 } = {}) {
  if (made > cap) {
    throw new Error(`Hard cap exceeded: made=${made} cap=${cap}`);
  }
  return made < cap;
}
