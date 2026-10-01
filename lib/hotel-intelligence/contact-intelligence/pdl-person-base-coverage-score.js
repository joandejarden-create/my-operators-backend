/**
 * PDL Person Base coverage scoring — identity vs contact fields independent.
 * Personal email ≠ identity conflict / related-domain merely for hotmail/gmail.
 * Does not claim deliverability or verified business phone.
 */

import { PDL_CONTACT_EVAL_VERSION } from "./pdl-contact-enrichment-eval.js";

const FREE_MAIL =
  /^(gmail\.com|googlemail\.com|hotmail\.com|outlook\.com|live\.com|yahoo\.com|icloud\.com|me\.com|aol\.com|proton\.me|protonmail\.com)$/i;

function hostOf(domainOrUrl) {
  return String(domainOrUrl || "")
    .trim()
    .toLowerCase()
    .replace(/^https?:\/\//, "")
    .replace(/^www\./, "")
    .split("/")[0];
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

function isPersonalType(type, email) {
  const t = String(type || "").toLowerCase();
  if (t === "personal" || t.includes("personal")) return true;
  const h = emailHost(email);
  return Boolean(h && FREE_MAIL.test(h));
}

function isProfessionalType(type) {
  const t = String(type || "").toLowerCase();
  return (
    t === "work" ||
    t === "current_professional" ||
    t === "professional" ||
    t.includes("professional")
  );
}

/**
 * Score one PDL normalized Person Base result with separated contact dimensions.
 */
export function scorePdlPersonBaseCoverage({
  subject = {},
  normalized = {},
  http_status = null,
  latency_ms = null,
  cost_credits = null,
  transport_error = null,
  credit_headers = null,
  reused = false,
  reused_from = null,
} = {}) {
  const targetDomain = hostOf(subject.domain || subject.company_domain);
  const knownEmail = subject.known_email_for_scoring || subject.surfe_prior?.surfe_email || null;
  const knownPhone = subject.known_phone_for_scoring || subject.surfe_prior?.phone || null;
  const wantName = subject.full_name || `${subject.first_name || ""} ${subject.last_name || ""}`.trim();

  const base = {
    version: `${PDL_CONTACT_EVAL_VERSION}-person-base-coverage`,
    subject_id: subject.id,
    wave: subject.wave || null,
    reused,
    reused_from,
    latency_ms,
    cost_credits: cost_credits ?? 0,
    credit_headers,
    deliverability: "PROVIDER_REPORTED_NOT_INDEPENDENTLY_VERIFIED",
    phone_business_line: "NOT_VERIFIED_BUSINESS_LINE",
    likelihood_not_contact_accuracy: true,
    job_freshness_not_contact_accuracy: true,
    canonical_writes: false,
    customer_publication: "BLOCKED",
  };

  if (transport_error || (http_status && http_status >= 500)) {
    return {
      ...base,
      identity_match: false,
      affiliation: "UNKNOWN",
      outcome: "PROVIDER_ERROR",
      error: String(transport_error || `HTTP_${http_status}`),
    };
  }
  if (http_status === 429 || http_status === 402) {
    return {
      ...base,
      identity_match: false,
      affiliation: "UNKNOWN",
      outcome: http_status === 429 ? "RATE_LIMITED" : "INSUFFICIENT_CREDITS",
      error: String(http_status),
    };
  }
  if (!normalized?.matched) {
    return {
      ...base,
      identity_match: false,
      affiliation: "UNKNOWN",
      outcome: "NO_MATCH",
      likelihood: normalized?.likelihood ?? null,
      error: normalized?.error || "NO_MATCH",
    };
  }

  const id = normalized.identity || {};
  let identity_match = true;
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
    if (!firstOk || !surOk) identity_match = false;
  }

  const returnedCompanyHost = hostOf(id.job_company_website);
  let affiliation = "UNKNOWN";
  if (targetDomain && returnedCompanyHost) {
    affiliation = domainsMatch(returnedCompanyHost, targetDomain) ? "MATCH" : "AMBIGUOUS_OR_OTHER_COMPANY";
  } else if (targetDomain && !returnedCompanyHost) {
    affiliation = "JOB_COMPANY_WEBSITE_MISSING";
  }

  // Email classification
  const work_email_field = normalized.work_email || null;
  const fromEmails = [];
  for (const e of normalized.emails || []) {
    if (!e?.email) continue;
    fromEmails.push({
      email: e.email,
      type: e.type || "unknown",
      source: e.source || "emails",
      personal: isPersonalType(e.type, e.email),
      professional: isProfessionalType(e.type) || e.source === "work_email",
    });
  }
  for (const pe of normalized.personal_emails || []) {
    if (!fromEmails.some((x) => x.email === pe)) {
      fromEmails.push({ email: pe, type: "personal", source: "personal_emails", personal: true, professional: false });
    }
  }
  if (normalized.recommended_personal_email) {
    const r = normalized.recommended_personal_email;
    if (!fromEmails.some((x) => x.email === r)) {
      fromEmails.push({
        email: r,
        type: "personal",
        source: "recommended_personal_email",
        personal: true,
        professional: false,
      });
    }
  }

  const attributable_work_emails = [];
  const other_professional_emails = [];
  const personal_emails = [];

  if (work_email_field) {
    if (targetDomain && domainsMatch(emailHost(work_email_field), targetDomain)) {
      attributable_work_emails.push({ email: work_email_field, source: "work_email" });
    } else if (isPersonalType("personal", work_email_field)) {
      personal_emails.push({ email: work_email_field, source: "work_email_but_free_mail" });
    } else {
      other_professional_emails.push({
        email: work_email_field,
        source: "work_email",
        affiliation: targetDomain ? "NON_TARGET_DOMAIN" : "NO_TARGET_DOMAIN",
      });
    }
  }

  for (const e of fromEmails) {
    if (attributable_work_emails.some((x) => x.email === e.email)) continue;
    if (personal_emails.some((x) => x.email === e.email)) continue;
    if (other_professional_emails.some((x) => x.email === e.email)) continue;
    if (e.personal) {
      personal_emails.push({ email: e.email, source: e.source, type: e.type });
      continue;
    }
    if (e.professional || e.source === "work_email") {
      if (targetDomain && domainsMatch(emailHost(e.email), targetDomain)) {
        attributable_work_emails.push({ email: e.email, source: e.source, type: e.type });
      } else {
        other_professional_emails.push({
          email: e.email,
          source: e.source,
          type: e.type,
          affiliation: targetDomain ? "NON_TARGET_DOMAIN" : "NO_TARGET_DOMAIN",
        });
      }
      continue;
    }
    // unknown type — attribute by domain if possible
    if (targetDomain && domainsMatch(emailHost(e.email), targetDomain)) {
      attributable_work_emails.push({ email: e.email, source: e.source, type: e.type || "unknown" });
    } else if (isPersonalType(e.type, e.email)) {
      personal_emails.push({ email: e.email, source: e.source, type: e.type });
    } else {
      other_professional_emails.push({
        email: e.email,
        source: e.source,
        type: e.type || "unknown",
        affiliation: "UNASSESSED",
      });
    }
  }

  // Phones — dedupe mobile_phone + phone_numbers; type UNKNOWN unless mobile_phone
  const phones = [];
  for (const p of normalized.phones || []) {
    if (!p?.number) continue;
    if (phones.some((x) => x.number === p.number)) continue;
    phones.push({
      number: p.number,
      type: p.source === "mobile_phone" || p.type === "mobile" ? "mobile" : "UNKNOWN",
      source: p.source || "phone_numbers",
      verified_business_line: false,
    });
  }
  const mobile = phones.filter((p) => p.type === "mobile");
  const other_phones = phones.filter((p) => p.type !== "mobile");

  const primaryWork = attributable_work_emails[0]?.email || null;
  let work_email_vs_known = null;
  if (primaryWork && knownEmail) {
    work_email_vs_known =
      String(primaryWork).toLowerCase() === String(knownEmail).toLowerCase()
        ? "CORROBORATION"
        : "DIFFERENT_OR_NEW";
  } else if (primaryWork && !knownEmail) {
    work_email_vs_known = "NEW_NO_BASELINE";
  }

  let phone_vs_known = null;
  const primaryPhone = mobile[0]?.number || other_phones[0]?.number || null;
  if (primaryPhone && knownPhone) {
    phone_vs_known =
      String(primaryPhone) === String(knownPhone) ? "CORROBORATION" : "DIFFERENT_OR_NEW";
  } else if (primaryPhone && !knownPhone) {
    phone_vs_known = "NEW_NO_BASELINE";
  }

  let outcome = "MATCHED_WITHOUT_CONTACTS";
  if (!identity_match) outcome = "CONFLICTING_IDENTITY";
  else if (primaryWork && primaryPhone) outcome = "ATTRIBUTABLE_WORK_EMAIL_AND_PHONE";
  else if (primaryWork) outcome = "ATTRIBUTABLE_WORK_EMAIL";
  else if (primaryPhone) outcome = "PROVIDER_REPORTED_PHONE_ONLY";
  else if (personal_emails.length || other_professional_emails.length) outcome = "MATCHED_NON_TARGET_OR_PERSONAL_ONLY";

  return {
    ...base,
    identity_match,
    affiliation,
    outcome,
    likelihood: normalized.likelihood ?? null,
    matched_inputs: normalized.matched_inputs || null,
    identity: id,
    contacts: {
      attributable_target_work_email: primaryWork,
      attributable_work_emails,
      other_professional_emails,
      personal_emails,
      recommended_personal_email: normalized.recommended_personal_email || null,
      mobile,
      other_phones,
      phones_deduped: phones,
      has_attributable_work_email: Boolean(primaryWork),
      has_personal_email: personal_emails.length > 0,
      has_mobile: mobile.length > 0,
      has_other_phone: other_phones.length > 0,
      has_email_and_phone: Boolean(primaryWork && primaryPhone),
    },
    known_baseline: {
      email: knownEmail || null,
      phone: knownPhone || null,
      work_email_vs_known,
      phone_vs_known,
    },
    field_inventory: normalized.field_inventory || null,
    job_last_verified: id.job_last_verified || "UNKNOWN",
    job_last_changed: id.job_last_changed || "UNKNOWN",
    error: null,
  };
}

export function compareCoverageToSavedSurfe({ scores = [], people = [] } = {}) {
  const byId = Object.fromEntries((people || []).map((p) => [p.id, p]));
  const paired = [];
  const unpaired = [];

  for (const s of scores) {
    const p = byId[s.subject_id];
    const surfeEmail = p?.surfe_prior?.surfe_email || null;
    const surfePhone = p?.surfe_prior?.phone || null;
    const surfeHadEnrich =
      p?.surfe_prior?.outcome?.startsWith("EMAIL") ||
      p?.surfe_prior?.outcome === "NO_SURFE_RESULT" ||
      Boolean(surfeEmail);

    if (!surfeHadEnrich && !surfeEmail) {
      unpaired.push({
        subject_id: s.subject_id,
        note: "No historical Surfe test — not counted as Surfe failure",
        pdl_attributable_work_email: s.contacts?.attributable_target_work_email || null,
        pdl_phone: s.contacts?.phones_deduped?.[0]?.number || null,
      });
      continue;
    }

    const pdlEmail = s.contacts?.attributable_target_work_email || null;
    const surfeAccepted = Boolean(surfeEmail);
    const pdlAccepted = Boolean(pdlEmail);
    let cell = "NEITHER";
    if (pdlAccepted && surfeAccepted) cell = "BOTH";
    else if (pdlAccepted) cell = "PDL_ONLY";
    else if (surfeAccepted) cell = "SURFE_ONLY";

    paired.push({
      subject_id: s.subject_id,
      cell,
      pdl_email: pdlEmail,
      surfe_email: surfeEmail,
      same_email:
        pdlEmail && surfeEmail
          ? String(pdlEmail).toLowerCase() === String(surfeEmail).toLowerCase()
          : false,
      pdl_phone: s.contacts?.phones_deduped?.[0]?.number || null,
      pdl_phone_type: s.contacts?.phones_deduped?.[0]?.type || null,
      surfe_phone: surfePhone,
      pdl_personal_emails: (s.contacts?.personal_emails || []).map((x) => x.email),
      identity_match: s.identity_match,
      affiliation: s.affiliation,
      surfe_outcome: p?.surfe_prior?.outcome || null,
    });
  }

  const both = paired.filter((r) => r.cell === "BOTH").length;
  const pdlOnly = paired.filter((r) => r.cell === "PDL_ONLY").length;
  const surfeOnly = paired.filter((r) => r.cell === "SURFE_ONLY").length;
  const neither = paired.filter((r) => r.cell === "NEITHER").length;

  return {
    paired_n: paired.length,
    unpaired_n: unpaired.length,
    paired,
    unpaired,
    attributable_work_email: {
      both,
      pdl_only: pdlOnly,
      surfe_only: surfeOnly,
      neither,
      same_address: paired.filter((r) => r.same_email).length,
      different_address: paired.filter((r) => r.cell === "BOTH" && !r.same_email).length,
    },
    phone: {
      pdl_returned: paired.filter((r) => r.pdl_phone).length,
      surfe_returned: paired.filter((r) => r.surfe_phone).length,
      both: paired.filter((r) => r.pdl_phone && r.surfe_phone).length,
      overlap_same:
        paired.filter(
          (r) => r.pdl_phone && r.surfe_phone && String(r.pdl_phone) === String(r.surfe_phone)
        ).length,
    },
    retention_note:
      "Uses freeze/saved Surfe outcome fields already in evaluation cohort artifacts — no Surfe re-call; no copy into customer records",
  };
}
