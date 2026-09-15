/**
 * Shared Contact Intelligence pre-submission gate for paid provider lookups.
 * Blocks rejected domains, conflicting profiles, and unsupported identifiers
 * before any FullEnrich / DataLayer / Prospeo (or similar) call.
 *
 * Name-token agreement can screen profiles but is not sufficient identity proof —
 * organization/role context must also support the profile. Prefer omitting an
 * uncertain LinkedIn over substituting another person's URL.
 */

export const PROVIDER_SUBMISSION_GATE_VERSION = "ci-provider-submission-gate-v1";

export const SUBMISSION_DECISION = Object.freeze({
  ALLOW: "ALLOW",
  REJECT: "REJECT",
  OMIT_IDENTIFIER: "OMIT_IDENTIFIER",
});

export const SUBMISSION_REJECT_REASON = Object.freeze({
  PERSON_IDENTITY_UNSUPPORTED: "PERSON_IDENTITY_UNSUPPORTED",
  TARGET_ORG_RELATIONSHIP_UNSUPPORTED: "TARGET_ORG_RELATIONSHIP_UNSUPPORTED",
  DOMAIN_REJECTED: "DOMAIN_REJECTED",
  DOMAIN_UNSUPPORTED: "DOMAIN_UNSUPPORTED",
  LINKEDIN_UNSUPPORTED: "LINKEDIN_UNSUPPORTED",
  LINKEDIN_WRONG_PERSON: "LINKEDIN_WRONG_PERSON",
  LINKEDIN_ORG_CONTEXT_MISSING: "LINKEDIN_ORG_CONTEXT_MISSING",
  NO_SUPPORTED_LOOKUP_IDENTIFIERS: "NO_SUPPORTED_LOOKUP_IDENTIFIERS",
  HOTEL_OR_OPERATOR_DOMAIN_AS_OWNER: "HOTEL_OR_OPERATOR_DOMAIN_AS_OWNER",
});

function norm(s) {
  return String(s || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .trim();
}

export function nameTokens(s) {
  return norm(s)
    .replace(/[^a-z0-9\s]/g, " ")
    .split(/\s+/)
    .filter((t) => t.length >= 2);
}

/** Slug tokens from linkedin.com/in/{slug} */
export function linkedInSlugTokens(url) {
  const m = String(url || "").match(/linkedin\.com\/in\/([^/?#]+)/i);
  if (!m) return [];
  return decodeURIComponent(m[1])
    .toLowerCase()
    .split(/[-_]+/)
    .filter((t) => t.length >= 2 && !/^\d+$/.test(t));
}

function tokenHitsSlugParts(token, slugParts, slugJoined) {
  if (!token) return false;
  return (
    slugParts.some((s) => s === token || s.startsWith(token) || token.startsWith(s)) ||
    // Concatenated slugs: /in/rolftweeten
    (slugJoined.includes(token) && token.length >= 4)
  );
}

/**
 * Name-token screen only — necessary but not sufficient for identity.
 */
export function linkedInNameTokenAgreement(personName, linkedinUrl) {
  const personTok = nameTokens(personName).filter((t) => t.length >= 3 || ["phil", "kot"].includes(t));
  const slugTok = linkedInSlugTokens(linkedinUrl);
  if (!personTok.length || !slugTok.length) {
    return { ok: false, reason: "missing_tokens", overlap: [] };
  }
  const slugJoined = slugTok.join("");
  const overlap = personTok.filter((t) => tokenHitsSlugParts(t, slugTok, slugJoined));
  const first = personTok[0];
  const firstOk = tokenHitsSlugParts(first, slugTok, slugJoined);
  const surnames = personTok.slice(1);
  const surnameOk =
    !surnames.length || surnames.some((t) => tokenHitsSlugParts(t, slugTok, slugJoined));
  // Require first + surname when both exist; reject unrelated slugs (e.g. roccobova ≠ Carlos Justo).
  return {
    ok: Boolean(firstOk && surnameOk && overlap.length >= Math.min(2, personTok.length)),
    overlap,
    firstOk,
    surnameOk,
  };
}

function hostOf(domainOrUrl) {
  const s = String(domainOrUrl || "").trim();
  if (!s) return "";
  try {
    const u = s.includes("://") ? new URL(s) : new URL(`https://${s}`);
    return u.hostname.replace(/^www\./, "").toLowerCase();
  } catch {
    return s
      .replace(/^www\./, "")
      .split("/")[0]
      .toLowerCase();
  }
}

function isRejectedDomain(domain, rejectedDomains = []) {
  const h = hostOf(domain);
  if (!h) return false;
  const list = (rejectedDomains || []).map((d) => hostOf(d));
  return list.some((r) => r && (h === r || h.endsWith(`.${r}`) || r.endsWith(`.${h}`)));
}

/**
 * Validate a single LinkedIn URL before it may be submitted.
 */
export function validateLinkedInIdentifier({
  personName,
  linkedinUrl,
  orgName = null,
  roleTitle = null,
  profileTitle = null,
  profileHeadline = null,
  profileCompany = null,
  independentlyConfirmed = false,
  evidenceNote = null,
} = {}) {
  if (!linkedinUrl) {
    return { decision: SUBMISSION_DECISION.OMIT_IDENTIFIER, reason: null, linkedin_url: null };
  }
  if (!/linkedin\.com\/in\//i.test(linkedinUrl)) {
    return {
      decision: SUBMISSION_DECISION.OMIT_IDENTIFIER,
      reason: SUBMISSION_REJECT_REASON.LINKEDIN_UNSUPPORTED,
      linkedin_url: null,
      detail: "not_a_person_profile_url",
    };
  }

  const nameScreen = linkedInNameTokenAgreement(personName, linkedinUrl);
  if (!nameScreen.ok) {
    return {
      decision: SUBMISSION_DECISION.OMIT_IDENTIFIER,
      reason: SUBMISSION_REJECT_REASON.LINKEDIN_WRONG_PERSON,
      linkedin_url: null,
      detail: "name_token_screen_failed",
      name_screen: nameScreen,
      submitted_url_rejected: linkedinUrl,
    };
  }

  const orgTok = nameTokens(orgName).filter((t) => t.length >= 4);
  const companyBlob = norm(
    `${profileCompany || ""} ${profileHeadline || ""} ${profileTitle || ""} ${evidenceNote || ""} ${roleTitle || ""}`
  );
  const orgInContext =
    independentlyConfirmed ||
    (orgTok.length > 0 && orgTok.some((t) => companyBlob.includes(t))) ||
    (/alliance|hospitality|hotel|inmobiliaria|hnf|owner|investor|chairman|ceo|director|development|buyer|principal/i.test(
      companyBlob
    ) &&
      orgTok.some((t) => companyBlob.includes(t)));

  if (!independentlyConfirmed && !orgInContext) {
    return {
      decision: SUBMISSION_DECISION.OMIT_IDENTIFIER,
      reason: SUBMISSION_REJECT_REASON.LINKEDIN_ORG_CONTEXT_MISSING,
      linkedin_url: null,
      detail: "name_tokens_ok_but_org_role_context_missing",
      name_screen: nameScreen,
      submitted_url_rejected: linkedinUrl,
    };
  }

  return {
    decision: SUBMISSION_DECISION.ALLOW,
    reason: null,
    linkedin_url: linkedinUrl,
    name_screen: nameScreen,
    org_context_ok: Boolean(orgInContext || independentlyConfirmed),
  };
}

/**
 * Full pre-submission gate for a provider person lookup.
 */
export function validateProviderSubmissionInput(input = {}) {
  const violations = [];
  const warnings = [];
  const person = input.person || {};
  const organization = input.organization || {};
  const ids = input.identifiers || {};
  const provider = String(input.provider || "fullenrich").toLowerCase();
  const allowLinkedInOnly =
    input.allow_linkedin_only != null
      ? Boolean(input.allow_linkedin_only)
      : provider === "fullenrich";

  const personName = person.display_name || person.full_name || "";
  if (!personName || person.identity_supported === false) {
    violations.push({
      code: SUBMISSION_REJECT_REASON.PERSON_IDENTITY_UNSUPPORTED,
      detail: "person display_name missing or identity_supported=false",
    });
  }
  if (person.deceased || person.former_affiliation) {
    violations.push({
      code: SUBMISSION_REJECT_REASON.PERSON_IDENTITY_UNSUPPORTED,
      detail: "deceased_or_former_not_current_contact_route",
    });
  }
  const pub = String(person.publication_label || person.provenance?.evidenced_or_inferred || "").toUpperCase();
  if (/HYPOTHESIS|SEED_ONLY|PENDING/.test(pub) && person.identity_supported !== true) {
    violations.push({
      code: SUBMISSION_REJECT_REASON.PERSON_IDENTITY_UNSUPPORTED,
      detail: "hypothesis_or_seed_only_without_identity_supported_flag",
    });
  }

  if (organization.relationship_supported === false || organization.rejected === true) {
    violations.push({
      code: SUBMISSION_REJECT_REASON.TARGET_ORG_RELATIONSHIP_UNSUPPORTED,
      detail: organization.reject_reason || "organization_relationship_not_supported",
    });
  }
  if (!organization.name && !organization.entity_id && organization.relationship_supported !== true) {
    violations.push({
      code: SUBMISSION_REJECT_REASON.TARGET_ORG_RELATIONSHIP_UNSUPPORTED,
      detail: "organization_name_or_entity_required",
    });
  }

  let domainOut = null;
  const domainRaw = ids.domain?.value || (typeof ids.domain === "string" ? ids.domain : null);
  const domainStatus = String(ids.domain?.status || (domainRaw ? "CANDIDATE" : "ABSENT")).toUpperCase();
  const rejectedList = [
    ...(input.rejected_domains || []),
    ...(domainStatus === "REJECTED" && domainRaw ? [domainRaw] : []),
  ];

  if (domainRaw) {
    if (domainStatus === "REJECTED" || isRejectedDomain(domainRaw, rejectedList)) {
      violations.push({
        code: SUBMISSION_REJECT_REASON.DOMAIN_REJECTED,
        detail: `domain_rejected:${hostOf(domainRaw)}`,
        domain: hostOf(domainRaw),
      });
    } else if (domainStatus === "UNSUPPORTED" || domainStatus === "DIRECTORY_ONLY") {
      violations.push({
        code: SUBMISSION_REJECT_REASON.DOMAIN_UNSUPPORTED,
        detail: `domain_status=${domainStatus}`,
        domain: hostOf(domainRaw),
      });
    } else {
      const h = hostOf(domainRaw);
      const forbidden = (input.forbidden_owner_domain_hosts || []).map((x) => hostOf(x));
      if (forbidden.some((f) => f && (h === f || h.endsWith(`.${f}`)))) {
        violations.push({
          code: SUBMISSION_REJECT_REASON.HOTEL_OR_OPERATOR_DOMAIN_AS_OWNER,
          detail: `host_forbidden_as_owner_domain:${h}`,
          domain: h,
        });
      } else if (
        domainStatus === "CONFIRMED" ||
        domainStatus === "CONFIRMED_FIRST_PARTY" ||
        ids.domain?.independently_supported === true
      ) {
        domainOut = h;
      } else {
        warnings.push({
          code: SUBMISSION_REJECT_REASON.DOMAIN_UNSUPPORTED,
          detail: "domain_not_independently_confirmed_omitted",
          domain: h,
        });
      }
    }
  }

  const liIn =
    ids.linkedin_url?.value || (typeof ids.linkedin_url === "string" ? ids.linkedin_url : null);
  const liCheck = validateLinkedInIdentifier({
    personName,
    linkedinUrl: liIn,
    orgName: organization.name,
    roleTitle: person.title,
    profileTitle: ids.linkedin_url?.profile_title,
    profileHeadline: ids.linkedin_url?.profile_headline,
    profileCompany: ids.linkedin_url?.profile_company,
    independentlyConfirmed: Boolean(ids.linkedin_url?.independently_confirmed),
    evidenceNote: ids.linkedin_url?.evidence_note || person.why_relevant,
  });

  let linkedinOut = null;
  if (liCheck.decision === SUBMISSION_DECISION.ALLOW) {
    linkedinOut = liCheck.linkedin_url;
  } else if (liIn) {
    warnings.push({
      code: liCheck.reason || SUBMISSION_REJECT_REASON.LINKEDIN_UNSUPPORTED,
      detail: liCheck.detail,
      submitted_url_rejected: liCheck.submitted_url_rejected || liIn,
    });
  }

  const hasDomain = Boolean(domainOut);
  const hasLinkedIn = Boolean(linkedinOut);
  if (!hasDomain && !hasLinkedIn) {
    violations.push({
      code: SUBMISSION_REJECT_REASON.NO_SUPPORTED_LOOKUP_IDENTIFIERS,
      detail: allowLinkedInOnly
        ? "need_confirmed_domain_or_independently_confirmed_linkedin"
        : "need_confirmed_domain",
    });
  } else if (!hasDomain && hasLinkedIn && !allowLinkedInOnly) {
    violations.push({
      code: SUBMISSION_REJECT_REASON.NO_SUPPORTED_LOOKUP_IDENTIFIERS,
      detail: `provider_${provider}_requires_domain`,
    });
  }

  const ok = violations.length === 0;
  const personTok = nameTokens(personName);
  return {
    ok,
    version: PROVIDER_SUBMISSION_GATE_VERSION,
    decision: ok ? SUBMISSION_DECISION.ALLOW : SUBMISSION_DECISION.REJECT,
    violations,
    warnings,
    sanitized_identifiers: {
      first_name: person.first_name || personTok[0] || null,
      last_name: person.last_name || personTok.slice(1).join(" ") || null,
      full_name: personName || null,
      domain: domainOut,
      company_name: organization.name || null,
      linkedin_url: linkedinOut,
      email: undefined,
    },
    alternative_chain_allowed: Boolean(
      ok && !domainOut && linkedinOut && allowLinkedInOnly && organization.relationship_supported !== false
    ),
  };
}

/**
 * Classify a prior evaluation attempt that should be excluded from coverage metrics.
 */
export function classifyInvalidInputEvaluation({
  subject_id,
  person,
  submitted_identifiers,
  rejection_reasons,
  raw_provider_response = null,
  submitted_at = null,
} = {}) {
  return {
    classification: "INVALID_INPUT_EVALUATION",
    exclude_from_provider_coverage_metrics: true,
    subject_id: subject_id || null,
    person: person || null,
    submitted_identifiers: submitted_identifiers || null,
    rejection_reasons: rejection_reasons || [],
    submitted_at: submitted_at || null,
    raw_provider_response_preserved: raw_provider_response != null,
    raw_provider_response,
    note: "Invalid identifiers were submitted; preserve raw provider payload but do not count toward coverage/precision.",
  };
}
