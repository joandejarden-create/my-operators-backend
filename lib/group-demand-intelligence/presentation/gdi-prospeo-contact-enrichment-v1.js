/**
 * GDI Prospeo contact enrichment — email-only adapter.
 * Provider-derived emails are NOT independently verified public facts.
 */
import {
  PROSPEO_CLIENT_VERSION,
  PROSPEO_CREDIT_COSTS,
  describeProspeoKeyPresence,
  getProspeoAccountInformation,
  enrichProspeoPersonEmailOnly,
} from "../../prospeo/client.js";
import {
  classifyLinkedInProfileMatch,
  LINKEDIN_PROFILE_MATCH,
} from "./gdi-google-grounded-contact-discovery-v1.js";

export const GDI_PROSPEO_CONTACT_ENRICHMENT_V1 = "gdi-prospeo-contact-enrichment-v1";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function stripAccents(s) {
  return text(s)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
}

function splitName(fullName = "") {
  const parts = text(fullName).split(/\s+/).filter(Boolean);
  if (!parts.length) return { firstName: null, lastName: null };
  if (parts.length === 1) return { firstName: parts[0], lastName: null };
  return { firstName: parts[0], lastName: parts.slice(1).join(" ") };
}

function isGenericMailbox(email) {
  const local = String(email || "")
    .split("@")[0]
    ?.toLowerCase();
  return /^(info|contact|sales|support|admin|office|hello|enquiries|inquiry|marketing|press|media|training|hall|noreply|no-reply)$/i.test(
    local || "",
  );
}

function namesMatch(expected, got) {
  const a = stripAccents(expected);
  const b = stripAccents(got);
  if (!a || !b) return false;
  if (a === b) return true;
  const ap = a.split(/\s+/);
  const bp = b.split(/\s+/);
  if (ap[0] && bp[0] && ap[0] === bp[0]) {
    const al = ap[ap.length - 1];
    const bl = bp[bp.length - 1];
    if (al && bl && (al === bl || al.startsWith(bl.slice(0, 4)) || bl.startsWith(al.slice(0, 4)))) {
      return true;
    }
  }
  return false;
}

function orgMatch(expected, got) {
  const a = stripAccents(expected);
  const b = stripAccents(got);
  if (!a || !b) return null;
  if (a === b) return true;
  const tokens = a
    .split(/[^a-z0-9]+/)
    .filter((t) => t.length > 3 && !/^(the|and|association|international|group)$/.test(t))
    .slice(0, 4);
  return tokens.some((t) => b.includes(t));
}

/**
 * Verify Prospeo result against expected identity. Quarantine mismatches.
 */
export function verifyProspeoIdentityMatch({
  expectedName,
  expectedEmployer,
  expectedDomain,
  expectedLinkedIn,
  normalized = {},
} = {}) {
  const reasons = [];
  const nameOk = namesMatch(expectedName, normalized.full_name);
  if (!nameOk) reasons.push("NAME_MISMATCH");

  const empOk = orgMatch(expectedEmployer, normalized.company_name);
  if (empOk === false) reasons.push("EMPLOYER_MISMATCH");

  let domainOk = null;
  if (expectedDomain && normalized.company_website) {
    const a = text(expectedDomain).toLowerCase();
    const b = text(normalized.company_website)
      .toLowerCase()
      .replace(/^https?:\/\//, "")
      .replace(/^www\./, "")
      .split("/")[0];
    domainOk = a === b || a.endsWith(`.${b}`) || b.endsWith(`.${a}`);
    if (!domainOk) reasons.push("DOMAIN_MISMATCH");
  }

  let linkedInOk = null;
  if (normalized.linkedin_url) {
    const li = classifyLinkedInProfileMatch({
      url: normalized.linkedin_url,
      personName: expectedName,
      organization: expectedEmployer,
      evidenceText: `${normalized.company_name || ""} ${normalized.job_title || ""}`,
    });
    linkedInOk = li.status === LINKEDIN_PROFILE_MATCH.VERIFIED_LINKEDIN_PROFILE;
    if (
      li.status === LINKEDIN_PROFILE_MATCH.PROFILE_MATCH_UNCERTAIN ||
      li.status === LINKEDIN_PROFILE_MATCH.NO_PROFILE_FOUND
    ) {
      reasons.push("LINKEDIN_LOOKALIKE_OR_WEAK");
    }
    if (
      expectedLinkedIn &&
      text(expectedLinkedIn).replace(/\/$/, "").toLowerCase() !==
        text(normalized.linkedin_url).replace(/\/$/, "").toLowerCase()
    ) {
      // Different URL than our verified one — flag but not auto-reject if name+org ok
      reasons.push("LINKEDIN_URL_DIFFERS_FROM_BASELINE");
    }
  }

  const email = normalized.email;
  if (email && isGenericMailbox(email)) {
    reasons.push("GENERIC_MAILBOX");
  }

  const accepted =
    nameOk &&
    empOk !== false &&
    !reasons.includes("GENERIC_MAILBOX") &&
    !reasons.includes("NAME_MISMATCH");

  return {
    accepted,
    quarantined: !accepted,
    nameOk,
    employerOk: empOk,
    domainOk,
    linkedInOk,
    reasons,
    dataOrigin: "PROSPEO_PROVIDER_DERIVED",
    independentlyVerifiedPublic: false,
  };
}

export async function enrichGdiContactViaProspeo(args = {}) {
  const key = describeProspeoKeyPresence(args.env || process.env);
  if (!key.present) {
    return {
      ok: false,
      skipped: true,
      reason: "PROSPEO_KEY_MISSING",
      creditsConsumed: 0,
    };
  }
  if (args.authorizeLiveProspeo !== true) {
    return {
      ok: false,
      skipped: true,
      reason: "PROSPEO_NOT_AUTHORIZED",
      creditsConsumed: 0,
    };
  }
  if (args.authorizeMobileEnrichment === true) {
    return {
      ok: false,
      skipped: true,
      reason: "MOBILE_NOT_AUTHORIZED_BY_FOUNDER",
      creditsConsumed: 0,
    };
  }

  const ceiling = Number(args.creditCeiling ?? 10);
  const already = Number(args.creditsConsumedSoFar || 0);
  if (already >= ceiling) {
    return {
      ok: false,
      skipped: true,
      reason: "PROSPEO_CREDIT_CEILING",
      creditsConsumed: 0,
    };
  }

  const name = text(args.contact?.name || args.personName);
  const { firstName, lastName } = splitName(name);
  const employerName = text(
    args.contact?.employerName ||
      args.organization?.name ||
      args.employerName,
  );
  const domain = text(
    args.contact?.employerDomain ||
      args.organization?.domain ||
      args.employerDomain,
  )
    .toLowerCase()
    .replace(/^https?:\/\//, "")
    .replace(/^www\./, "")
    .split("/")[0];
  const linkedinUrl =
    args.contact?.linkedinTrustedForSurfe === true ||
    args.linkedinTrusted === true
      ? text(args.contact?.linkedinUrl || args.linkedinUrl)
      : text(args.contact?.linkedinUrl || args.linkedinUrl);

  // Never pay for already verified named email
  const existingEmail = text(args.contact?.email || args.existingEmail);
  if (existingEmail && existingEmail.includes("@") && !isGenericMailbox(existingEmail)) {
    return {
      ok: false,
      skipped: true,
      reason: "NAMED_EMAIL_ALREADY_VERIFIED",
      creditsConsumed: 0,
      existingEmail,
    };
  }

  const result = await enrichProspeoPersonEmailOnly(
    {
      firstName,
      lastName,
      fullName: name,
      companyName: employerName,
      companyDomain: domain || null,
      linkedinUrl:
        linkedinUrl && /linkedin\.com\/in\//i.test(linkedinUrl)
          ? linkedinUrl
          : null,
    },
    {
      authorizeLiveProspeo: true,
      onlyVerifiedEmail: args.onlyVerifiedEmail !== false,
      apiKey: args.apiKey,
    },
  );

  const verification = verifyProspeoIdentityMatch({
    expectedName: name,
    expectedEmployer: employerName,
    expectedDomain: domain,
    expectedLinkedIn: linkedinUrl,
    normalized: result.normalized || {},
  });

  const credits = Number(result.credits_consumed_estimate || 0);
  if (already + credits > ceiling) {
    return {
      ok: false,
      skipped: true,
      reason: "PROSPEO_CREDIT_CEILING_WOULD_EXCEED",
      note: "Result discarded for ceiling safety — still may have been charged by provider; check account.",
      creditsConsumed: credits,
      raw: result,
    };
  }

  return {
    ok: verification.accepted && Boolean(result.normalized?.email),
    miss: !result.normalized?.email,
    quarantined: verification.quarantined,
    version: GDI_PROSPEO_CONTACT_ENRICHMENT_V1,
    prospeoClientVersion: PROSPEO_CLIENT_VERSION,
    creditsConsumed: credits,
    creditCosts: PROSPEO_CREDIT_COSTS,
    verification,
    contactDetails: verification.accepted
      ? {
          email: result.normalized?.email || null,
          linkedinUrl: result.normalized?.linkedin_url || null,
          title: result.normalized?.job_title || null,
          companyName: result.normalized?.company_name || null,
          companyDomain: result.normalized?.company_website || null,
          dataOrigin: "PROSPEO_PROVIDER_DERIVED",
          independentlyVerifiedPublic: false,
        }
      : null,
    normalized: result.normalized,
    request_sanitized: result.request_sanitized,
    http_status: result.http_status,
    latency_ms: result.latency_ms,
    free_enrichment: result.normalized?.free_enrichment === true,
  };
}

export { getProspeoAccountInformation, describeProspeoKeyPresence };

export default {
  GDI_PROSPEO_CONTACT_ENRICHMENT_V1,
  enrichGdiContactViaProspeo,
  verifyProspeoIdentityMatch,
};
