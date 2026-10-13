/**
 * PDL post-gate contact enrichment scoring + provider selection.
 * Does not discover owners. Does not write canonical contacts.
 */

export const PDL_CONTACT_EVAL_VERSION = "pdl-contact-enrichment-eval-v1";

export const CONTACT_PROVIDER = Object.freeze({
  SURFE: "surfe",
  PDL: "pdl",
  COMPARISON: "comparison",
});

/** Production default — Surfe until comparison is reviewed. */
export const DEFAULT_CONTACT_PROVIDER = CONTACT_PROVIDER.SURFE;

export const PDL_OUTCOME_BUCKET = Object.freeze({
  NEW_ATTRIBUTABLE_WORK_EMAIL: "NEW_ATTRIBUTABLE_WORK_EMAIL",
  KNOWN_EMAIL_CORROBORATED: "KNOWN_EMAIL_CORROBORATED",
  NEW_PROVIDER_REPORTED_PHONE: "NEW_PROVIDER_REPORTED_PHONE",
  KNOWN_PHONE_CORROBORATED: "KNOWN_PHONE_CORROBORATED",
  MATCH_WITHOUT_CONTACT: "MATCH_WITHOUT_CONTACT",
  IDENTITY_CONFLICT: "IDENTITY_CONFLICT",
  RELATED_DOMAIN_UNATTRIBUTED: "RELATED_DOMAIN_UNATTRIBUTED",
  NO_MATCH: "NO_MATCH",
  PROVIDER_ERROR: "PROVIDER_ERROR",
});

export function resolveContactProvider(opts = {}, env = process.env) {
  const raw = String(
    opts.contact_provider ||
      opts.provider ||
      env.CONTACT_INTELLIGENCE_CONTACT_PROVIDER ||
      DEFAULT_CONTACT_PROVIDER
  )
    .trim()
    .toLowerCase();
  if (raw === CONTACT_PROVIDER.PDL) return CONTACT_PROVIDER.PDL;
  if (raw === CONTACT_PROVIDER.COMPARISON) return CONTACT_PROVIDER.COMPARISON;
  return CONTACT_PROVIDER.SURFE;
}

function hostOf(domainOrUrl) {
  const s = String(domainOrUrl || "")
    .trim()
    .toLowerCase()
    .replace(/^https?:\/\//, "")
    .replace(/^www\./, "")
    .split("/")[0];
  return s;
}

function emailHost(email) {
  const m = String(email || "")
    .toLowerCase()
    .match(/@([^>\s]+)/);
  return m ? m[1].replace(/^www\./, "") : "";
}

function domainsMatch(a, b) {
  const ha = hostOf(a);
  const hb = hostOf(b);
  if (!ha || !hb) return false;
  return ha === hb || ha.endsWith(`.${hb}`) || hb.endsWith(`.${ha}`);
}

/**
 * Score one PDL normalized result against a frozen subject.
 * known_email / known_phone are scoring metadata only — never sent as PDL input.
 * Gate must already have run; this scorer does not re-open owner discovery.
 */
export function scorePdlEnrichmentResult({
  subject = {},
  normalized = {},
  http_status = null,
  latency_ms = null,
  cost_credits = null,
  transport_error = null,
} = {}) {
  const targetDomain = hostOf(subject.domain || subject.company_domain);
  const knownEmail = subject.known_email_for_scoring || subject.known_email || null;
  const knownPhone = subject.known_phone_for_scoring || subject.known_phone || null;
  const wantName = subject.full_name || `${subject.first_name || ""} ${subject.last_name || ""}`.trim();

  if (transport_error || (http_status && http_status >= 500)) {
    return baseScore({
      subject,
      bucket: PDL_OUTCOME_BUCKET.PROVIDER_ERROR,
      identity_match_accepted: false,
      affiliation_accepted: false,
      latency_ms,
      cost_credits,
      error: String(transport_error || `HTTP_${http_status}`),
    });
  }

  if (http_status === 429 || http_status === 402) {
    return baseScore({
      subject,
      bucket: PDL_OUTCOME_BUCKET.PROVIDER_ERROR,
      identity_match_accepted: false,
      affiliation_accepted: false,
      latency_ms,
      cost_credits,
      error: http_status === 429 ? "RATE_LIMITED" : "INSUFFICIENT_CREDITS",
    });
  }

  if (!normalized?.matched) {
    return baseScore({
      subject,
      bucket: PDL_OUTCOME_BUCKET.NO_MATCH,
      identity_match_accepted: false,
      affiliation_accepted: false,
      latency_ms,
      cost_credits,
      error: normalized?.error || "NO_MATCH",
      likelihood: normalized?.likelihood ?? null,
    });
  }

  const id = normalized.identity || {};
  const workEmail =
    normalized.work_email ||
    (normalized.emails || []).find((e) => e.type === "work")?.email ||
    (normalized.emails || [])[0]?.email ||
    null;

  // Identity: returned full_name must agree on first + a surname token when present.
  let identity_match_accepted = true;
  if (id.full_name && wantName) {
    const want = String(wantName)
      .toLowerCase()
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .split(/[^a-z0-9]+/)
      .filter((t) => t.length > 1);
    const got = String(id.full_name)
      .toLowerCase()
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .split(/[^a-z0-9]+/)
      .filter((t) => t.length > 1);
    const firstOk = want[0] && got.includes(want[0]);
    const surOk = want.length < 2 || want.slice(1).some((s) => got.includes(s));
    if (!firstOk || !surOk) identity_match_accepted = false;
  }

  let affiliation_accepted = true;
  const returnedCompanyHost = hostOf(id.job_company_website);
  if (workEmail) {
    const eh = emailHost(workEmail);
    if (targetDomain && eh && !domainsMatch(eh, targetDomain)) {
      affiliation_accepted = false;
    }
  } else if (targetDomain && returnedCompanyHost && !domainsMatch(returnedCompanyHost, targetDomain)) {
    // Match without email still fails affiliation when returned company website ≠ target owner domain
    affiliation_accepted = false;
  }

  const phones = normalized.phones || [];
  const primaryPhone = phones[0] || null;
  const phoneBusiness =
    primaryPhone?.type === "mobile"
      ? "PROVIDER_REPORTED_MOBILE_UNKNOWN_BUSINESS_USE"
      : primaryPhone
        ? "PROVIDER_REPORTED_UNKNOWN_BUSINESS_USE"
        : null;

  let bucket = PDL_OUTCOME_BUCKET.MATCH_WITHOUT_CONTACT;
  if (!identity_match_accepted) {
    bucket = PDL_OUTCOME_BUCKET.IDENTITY_CONFLICT;
  } else if (!affiliation_accepted) {
    bucket = workEmail
      ? PDL_OUTCOME_BUCKET.RELATED_DOMAIN_UNATTRIBUTED
      : PDL_OUTCOME_BUCKET.RELATED_DOMAIN_UNATTRIBUTED;
  } else if (workEmail && affiliation_accepted) {
    if (knownEmail && String(knownEmail).toLowerCase() === String(workEmail).toLowerCase()) {
      bucket = PDL_OUTCOME_BUCKET.KNOWN_EMAIL_CORROBORATED;
    } else {
      bucket = PDL_OUTCOME_BUCKET.NEW_ATTRIBUTABLE_WORK_EMAIL;
    }
  }

  let phone_bucket = null;
  if (primaryPhone && identity_match_accepted && affiliation_accepted !== false) {
    if (knownPhone && String(knownPhone) === String(primaryPhone.number)) {
      phone_bucket = PDL_OUTCOME_BUCKET.KNOWN_PHONE_CORROBORATED;
    } else {
      phone_bucket = PDL_OUTCOME_BUCKET.NEW_PROVIDER_REPORTED_PHONE;
    }
  }

  return {
    version: PDL_CONTACT_EVAL_VERSION,
    subject_id: subject.id,
    bucket,
    phone_bucket,
    identity_match_accepted,
    affiliation_accepted,
    work_email_returned: Boolean(workEmail),
    work_email: workEmail,
    email_provider_status: workEmail ? "PDL_REPORTED" : null,
    phone_returned: Boolean(primaryPhone),
    phone: primaryPhone?.number || null,
    phone_type: primaryPhone?.type || null,
    phone_business_use: phoneBusiness,
    likelihood: normalized.likelihood ?? null,
    matched_inputs: normalized.matched_inputs || null,
    identity: {
      full_name: id.full_name || null,
      job_title: id.job_title || null,
      job_company_name: id.job_company_name || null,
      job_company_website: id.job_company_website || null,
      linkedin_url: id.linkedin_url || null,
      job_last_verified: id.job_last_verified || null,
      pdl_id: id.pdl_id || null,
    },
    known_email_for_scoring: knownEmail || null,
    counts_as_new_attributable_email: bucket === PDL_OUTCOME_BUCKET.NEW_ATTRIBUTABLE_WORK_EMAIL,
    counts_as_new_provider_phone: phone_bucket === PDL_OUTCOME_BUCKET.NEW_PROVIDER_REPORTED_PHONE,
    latency_ms,
    cost_credits: cost_credits ?? (normalized.matched ? 1 : 0),
    error: null,
    canonical_writes: false,
    customer_publication: "BLOCKED",
  };
}

function baseScore(row) {
  return {
    version: PDL_CONTACT_EVAL_VERSION,
    subject_id: row.subject?.id || null,
    bucket: row.bucket,
    phone_bucket: null,
    identity_match_accepted: row.identity_match_accepted,
    affiliation_accepted: row.affiliation_accepted,
    work_email_returned: false,
    work_email: null,
    email_provider_status: null,
    phone_returned: false,
    phone: null,
    phone_type: null,
    phone_business_use: null,
    likelihood: row.likelihood ?? null,
    matched_inputs: null,
    identity: null,
    returned_gate: null,
    known_email_for_scoring: row.subject?.known_email_for_scoring || null,
    counts_as_new_attributable_email: false,
    counts_as_new_provider_phone: false,
    latency_ms: row.latency_ms ?? null,
    cost_credits: row.cost_credits ?? 0,
    error: row.error || null,
    canonical_writes: false,
    customer_publication: "BLOCKED",
  };
}

/**
 * Compare PDL scores to saved Surfe enrichment rows (no Surfe re-run).
 */
export function comparePdlToSavedSurfe({ pdlScores = [], surfeById = {} } = {}) {
  const rows = [];
  for (const p of pdlScores) {
    const s = surfeById[p.subject_id] || null;
    const surfeEmail = s?.surfe_email || s?.email || null;
    const surfePhone = s?.phone || null;
    const surfeAccepted =
      s?.identity_match === "MATCH" &&
      !/CONFLICT|AFFILIATION/i.test(String(s?.bucket || "")) &&
      Boolean(surfeEmail) &&
      domainsMatch(emailHost(surfeEmail), s?.domain || s?.returned_company_domain);

    rows.push({
      subject_id: p.subject_id,
      pdl_bucket: p.bucket,
      surfe_bucket: s?.bucket || (s ? "SAVED_RESULT" : "NO_SAVED_SURFE_RESULT"),
      pdl_email: p.work_email,
      surfe_email: surfeEmail,
      pdl_accepted_email: p.counts_as_new_attributable_email || p.bucket === PDL_OUTCOME_BUCKET.KNOWN_EMAIL_CORROBORATED,
      surfe_accepted_email: Boolean(surfeAccepted),
      pdl_phone: p.phone,
      surfe_phone: surfePhone,
      pdl_cost_credits: p.cost_credits,
      pdl_latency_ms: p.latency_ms,
      same_email:
        p.work_email && surfeEmail
          ? String(p.work_email).toLowerCase() === String(surfeEmail).toLowerCase()
          : false,
    });
  }

  const pdlAcceptedEmail = rows.filter((r) => r.pdl_accepted_email).length;
  const surfeAcceptedEmail = rows.filter((r) => r.surfe_accepted_email).length;
  const pdlNewEmail = pdlScores.filter((p) => p.counts_as_new_attributable_email).length;
  const pdlNewPhone = pdlScores.filter((p) => p.counts_as_new_provider_phone).length;
  const spend = pdlScores.reduce((a, p) => a + Number(p.cost_credits || 0), 0);

  return {
    version: `${PDL_CONTACT_EVAL_VERSION}-comparison`,
    n: rows.length,
    rows,
    coverage: {
      pdl_email_returned: pdlScores.filter((p) => p.work_email_returned).length,
      surfe_email_returned: rows.filter((r) => r.surfe_email).length,
      pdl_accepted_attributable_email: pdlAcceptedEmail,
      surfe_accepted_attributable_email: surfeAcceptedEmail,
      pdl_phone_returned: pdlScores.filter((p) => p.phone_returned).length,
      surfe_phone_returned: rows.filter((r) => r.surfe_phone).length,
      pdl_new_attributable_email: pdlNewEmail,
      pdl_new_provider_phone: pdlNewPhone,
      identity_conflicts: pdlScores.filter((p) => p.bucket === PDL_OUTCOME_BUCKET.IDENTITY_CONFLICT).length,
      related_domain: pdlScores.filter((p) => p.bucket === PDL_OUTCOME_BUCKET.RELATED_DOMAIN_UNATTRIBUTED)
        .length,
      no_match: pdlScores.filter((p) => p.bucket === PDL_OUTCOME_BUCKET.NO_MATCH).length,
      provider_error: pdlScores.filter((p) => p.bucket === PDL_OUTCOME_BUCKET.PROVIDER_ERROR).length,
    },
    cost: {
      pdl_credits_spent: spend,
      cost_per_new_usable_email: pdlNewEmail > 0 ? spend / pdlNewEmail : null,
      cost_per_new_usable_phone: pdlNewPhone > 0 ? spend / pdlNewPhone : null,
    },
    recommendation_hint:
      pdlAcceptedEmail > surfeAcceptedEmail
        ? "pdl_higher_accepted_email_coverage"
        : pdlAcceptedEmail < surfeAcceptedEmail
          ? "surfe_higher_accepted_email_coverage"
          : "tie_accepted_email_coverage",
  };
}
