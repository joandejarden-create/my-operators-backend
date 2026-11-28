/**
 * Permanent Stage 4 Findymail enrichment — missing-channel only (V3).
 *
 * Binding policy (FOUNDER GO 2026-11-27 + DYNAMIC_CONTACT_ENRICHMENT_V3):
 * - Findymail is a permanent Full E2E Stage 4 component (not optional manual).
 * - Recover Airtable / prior research first; dedupe by personKey + operation.
 * - Independently research email and telephone gaps via shared planner.
 * - Email: POST /search/name when needsEmail.
 * - Phone: POST /search/phone when needsPhone + verified LinkedIn (10 credits when found).
 * - Surfe remains explicit-click only and never durably persisted / never auto-run.
 * - Verified Findymail results may be persisted to Airtable when permitted.
 */
import {
  describeFindymailKeyPresence,
  findymailFindEmailFromName,
  findymailFindPhoneFromLinkedIn,
  appendFindymailUsageLedger,
  assertFindymailHotelHardCapAllowsDebit,
  readFindymailUsageLedger,
  sumFindymailCreditsDebitedForHotel,
  personAlreadyEnrichedInLedger,
  FINDYMAIL_CREDIT_COSTS,
  GDI_FINDYMAIL_CLIENT_V1,
} from "../providers/gdi-findymail-client-v1.js";
import {
  computeMissingContactChannels,
  planMissingChannelEnrichment,
  classifyPartialChannelResult,
  DEALALITY_DYNAMIC_CONTACT_ENRICHMENT_V3,
} from "../../shared-contact-intelligence/missing-channel-enrichment-v3.js";

/** Local constants avoid circular import with gdi-contact-research-stage-v1.js */
const CHANNEL_EMAIL = "PROFESSIONAL_EMAIL";
const CHANNEL_PHONE = "PROFESSIONAL_TELEPHONE";
const STATUS_UNVERIFIED = "UNVERIFIED";

export const GDI_FINDYMAIL_STAGE4_ENRICHMENT_V1 =
  "gdi-findymail-stage4-enrichment-v1";

export const FINDYMAIL_STAGE4_AUTH_TOKEN =
  "FOUNDER_GO_GDI_FINDYMAIL_STAGE4_PERMANENT_E2E_20261127";

const BAD_DOMAINS = new Set([
  "anywho.com",
  "yellowpages.com",
  "whitepages.com",
  "facebook.com",
  "linkedin.com",
  "twitter.com",
  "x.com",
  "instagram.com",
  "google.com",
  "wikipedia.org",
  "crunchbase.com",
  "prnewswire.com",
  "businesswire.com",
  "globenewswire.com",
  "newswire.com",
  "bloomberg.com",
  "reuters.com",
  "docs.sandiego.gov",
  "sandiego.gov",
  "sandiegobeer.news",
  "eventbrite.com",
  "meetup.com",
]);

/** Reject provider emails that clearly belong to a different person. */
export function emailLikelyMatchesPerson(email, fullName) {
  const local = String(email || "")
    .split("@")[0]
    .toLowerCase()
    .replace(/[^a-z]/g, "");
  const parts = text(fullName)
    .toLowerCase()
    .split(/\s+/)
    .map((p) => p.replace(/[^a-z]/g, ""))
    .filter((p) => p.length >= 2);
  if (!local || parts.length < 2) return true; // insufficient signal — keep for human review
  const first = parts[0];
  const last = parts[parts.length - 1];
  if (local.includes(last) || local.includes(first)) return true;
  if (local.startsWith(first[0] + last) || local.startsWith(first + last[0])) return true;
  return false;
}

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

export function normalizePersonKey(name, org) {
  return `${text(name).toLowerCase()}|${text(org).toLowerCase()}`;
}

/** Credentials accepted only after an explicit comma ("Jane Doe, MS"). */
const CREDENTIAL_TOKEN_RE =
  /^(ph\.?d\.?|m\.?d\.?|mba|m\.?s\.?|m\.?a\.?|b\.?s\.?|b\.?a\.?|cpa|cmp|cmm|cem|cae|sts|chme|cdme|dba|jd|j\.?d\.?|esq\.?|pmp|sphr|phr|rn|sr\.?|jr\.?|ii|iii|iv)$/i;
/** Unambiguous suffixes safe to strip without a comma (never surname-like tokens such as "Ma"). */
const UNAMBIGUOUS_SUFFIX_RE =
  /^(ph\.?d\.?|mba|cpa|cmp|cmm|cem|esq\.?|pmp|sphr|jr\.?|sr\.?)$/i;

/**
 * Normalize a display name for identity checks / Findymail "name" parameter.
 * Strips parenthetical nicknames and trailing credential suffixes ("…, PhD").
 * Never invents tokens; abbreviated surnames ("Lily Y.") stay abbreviated.
 * Does NOT mutate the display name — callers keep the original for UI/ledger keys.
 */
export function normalizePersonNameForEnrichment(name) {
  const original = text(name);
  const stripped = [];
  let n = original;
  // Parenthetical nicknames: "Alejandra (Alex) Gutierrez" → "Alejandra Gutierrez"
  n = n.replace(/\s*[(\[][^)\]]*[)\]]\s*/g, (m) => {
    stripped.push(text(m));
    return " ";
  });
  // Quoted nicknames: Robert "Bob" Smith
  n = n.replace(/\s*["“”][^"“”]*["“”]\s*/g, (m) => {
    stripped.push(text(m));
    return " ";
  });
  n = text(n);
  // Comma-separated trailing credential block: "Jane Doe, PhD, MBA"
  const commaParts = n.split(",").map((p) => text(p));
  if (commaParts.length > 1) {
    const tail = commaParts.slice(1);
    const tailIsCredentials = tail.every((seg) =>
      seg
        .split(/\s+/)
        .filter(Boolean)
        .every((tok) => CREDENTIAL_TOKEN_RE.test(tok)),
    );
    if (tailIsCredentials) {
      stripped.push(...tail);
      n = commaParts[0];
    }
  }
  // Trailing credential tokens without comma: "Jane Doe PhD"
  const toks = n.split(/\s+/).filter(Boolean);
  while (toks.length > 2 && UNAMBIGUOUS_SUFFIX_RE.test(toks[toks.length - 1])) {
    stripped.push(toks.pop());
  }
  n = toks.join(" ");
  return {
    original,
    normalized: n,
    changed: n !== original,
    stripped,
  };
}

export function looksLikeFullPersonName(name) {
  const n = normalizePersonNameForEnrichment(name).normalized;
  if (!n || n.length < 3) return false;
  if (/\b[A-Za-z]\.$/.test(n)) return false; // truncated surname "Lily Y."
  // Reject single-letter surname tokens without period: "Suzanne K"
  const parts = n.split(/\s+/).filter(Boolean);
  if (parts.length >= 2 && /^[A-Za-z]$/.test(parts[parts.length - 1])) {
    return false;
  }
  if (!/^[A-Za-z][A-Za-z .'\-]{1,100}$/.test(n)) return false;
  return parts.length >= 2;
}

function hostFromUrl(raw) {
  const u = text(raw);
  if (!u) return null;
  try {
    const host = new URL(u).hostname.replace(/^www\./i, "").toLowerCase();
    return host || null;
  } catch {
    return null;
  }
}

export function extractCompanyDomain(person = {}, opportunity = {}) {
  const email = text(person.email);
  const fromEmail = email.includes("@")
    ? email.split("@")[1].toLowerCase()
    : "";
  if (fromEmail && !BAD_DOMAINS.has(fromEmail)) return fromEmail;

  const candidates = [];
  // Prefer person-level official/source URLs (WHO/HOW often has org staff pages).
  for (const raw of [
    person.sourceUrl,
    person.personSourceUrl,
    person.officialContactUrl,
    person.organizationUrl,
    person.website,
  ]) {
    const host = hostFromUrl(raw);
    if (host) candidates.push(host);
  }
  for (const s of opportunity.sources || []) {
    const host = hostFromUrl(s.url || s.href || s);
    if (host) candidates.push(host);
  }
  // WHO/HOW source URLs (org staff pages, event pages) often carry the DNS domain.
  const wh = opportunity.whoHowResolution || {};
  for (const node of [
    wh.directBuyer,
    wh.buyerInfluencer,
    ...(Array.isArray(wh.entryPoints) ? wh.entryPoints : []),
  ]) {
    if (!node || typeof node !== "object") continue;
    for (const raw of [node.sourceUrl, node.personSourceUrl, node.organizationUrl]) {
      const host = hostFromUrl(raw);
      if (host) candidates.push(host);
    }
  }
  for (const host of candidates) {
    if (!BAD_DOMAINS.has(host) && !/linkedin\.com$/i.test(host)) return host;
  }
  const org = text(person.organization || opportunity.organizationName);
  // Last resort: org name as Findymail "domain" parameter (API accepts company name).
  return org || null;
}

/**
 * Eligible when named verified person is missing professional email and has
 * resolvable company domain/name. LinkedIn/phone presence does NOT block email research.
 */
export function assessFindymailEmailEligibility(person = {}, opportunity = {}, opts = {}) {
  const name = text(person.fullName || person.name);
  const org = text(person.organization || opportunity.organizationName);
  const email = text(person.email);
  const domain = extractCompanyDomain(person, opportunity);
  const personKey = normalizePersonKey(name, org);

  if (!looksLikeFullPersonName(name)) {
    return { eligible: false, reason: "IDENTITY_INCOMPLETE", personKey, name, org };
  }
  if (!org && !domain) {
    return { eligible: false, reason: "ORGANIZATION_UNRESOLVED", personKey, name, org };
  }
  if (email && email.includes("@")) {
    return { eligible: false, reason: "EMAIL_ALREADY_PRESENT", personKey, name, org, email };
  }
  if (!domain) {
    return { eligible: false, reason: "DOMAIN_UNRESOLVED", personKey, name, org };
  }
  if (BAD_DOMAINS.has(String(domain).toLowerCase())) {
    return { eligible: false, reason: "LOW_QUALITY_DOMAIN", personKey, name, org, domain };
  }
  if (opts.ledger && personAlreadyEnrichedInLedger(personKey, opts.ledger)) {
    return { eligible: false, reason: "ALREADY_IN_LEDGER", personKey, name, org, domain };
  }
  return {
    eligible: true,
    reason: "NAMED_PERSON_MISSING_PROFESSIONAL_EMAIL",
    personKey,
    name,
    // Provider query name: nicknames / credentials stripped; display name unchanged.
    enrichmentName: normalizePersonNameForEnrichment(name).normalized || name,
    org,
    domain,
  };
}

export function assertFindymailStage4Authorized(opts = {}) {
  if (opts.allowFindymail !== true) {
    const err = new Error(
      "Findymail Stage 4 requires allowFindymail=true (permanent E2E component gate)",
    );
    err.code = "FINDYMAIL_NOT_ALLOWED";
    throw err;
  }
  if (opts.authorizeSurfe === true) {
    const err = new Error("Surfe must never auto-run inside Findymail Stage 4");
    err.code = "SURFE_FORBIDDEN_IN_FINDYMAIL_STAGE";
    throw err;
  }
  const key = describeFindymailKeyPresence();
  if (!key.present && opts.dryRun !== true) {
    const err = new Error("FINDYMAIL_API_KEY missing for Live Findymail Stage 4");
    err.code = "MISSING_FINDYMAIL_API_KEY";
    throw err;
  }
  return key;
}

/**
 * Enrich one person's professional email via Findymail after public recovery.
 */
export async function enrichPersonEmailWithFindymail(person = {}, opportunity = {}, opts = {}) {
  assertFindymailStage4Authorized(opts);
  const ledger = opts.ledger || readFindymailUsageLedger(opts);
  const eligibility = assessFindymailEmailEligibility(person, opportunity, {
    ...opts,
    ledger,
  });
  if (!eligibility.eligible) {
    return {
      version: GDI_FINDYMAIL_STAGE4_ENRICHMENT_V1,
      outcome: "SKIPPED",
      eligibility,
      findymailUsed: false,
      person,
    };
  }

  // Cumulative hotel hard-cap (ledger + in-run) — refuse debit when exceeded.
  const emailBudgetGate = reserveBudgetOrSkip(opts, 1);
  if (!emailBudgetGate.ok) {
    return {
      version: GDI_FINDYMAIL_STAGE4_ENRICHMENT_V1,
      outcome: "SKIPPED",
      eligibility: {
        ...eligibility,
        eligible: false,
        reason: "CREDIT_HARD_CAP_REACHED",
        creditHardCap: emailBudgetGate.cap,
        creditsUsed: emailBudgetGate.used,
        ledgerCredits: emailBudgetGate.ledgerCredits || null,
      },
      findymailUsed: false,
      person,
      creditHardCapReached: true,
    };
  }

  // Fail soft: empty domain must not abort the Full E2E contact stage.
  // Org-name domains are allowed (Findymail accepts company name); provider 4xx
  // is handled below. Prefer DNS hosts from sourceUrl when extractCompanyDomain
  // resolved them.
  const domainRaw = String(eligibility.domain || "").trim();
  if (!domainRaw) {
    return {
      version: GDI_FINDYMAIL_STAGE4_ENRICHMENT_V1,
      outcome: "SKIPPED",
      eligibility: {
        ...eligibility,
        eligible: false,
        reason: "FINDYMAIL_DOMAIN_REQUIRED",
        domain: null,
      },
      findymailUsed: false,
      person,
      reason: "FINDYMAIL_DOMAIN_REQUIRED",
    };
  }

  let result;
  try {
    result = await findymailFindEmailFromName(
      {
        name: eligibility.enrichmentName || eligibility.name,
        domain: domainRaw,
      },
      { dryRun: opts.dryRun === true, apiKey: opts.apiKey },
    );
  } catch (err) {
    const code = err?.code || "FINDYMAIL_FIND_FAILED";
    const status = Number(err?.status) || null;
    // Provider 4xx (invalid domain / identity) → skip person, continue hotel run.
    if (
      code === "FINDYMAIL_INVALID_IDENTITY" ||
      code === "FINDYMAIL_FIND_FAILED" ||
      (status >= 400 && status < 500 && status !== 402)
    ) {
      return {
        version: GDI_FINDYMAIL_STAGE4_ENRICHMENT_V1,
        outcome: "SKIPPED",
        eligibility: {
          ...eligibility,
          eligible: false,
          reason: code,
          providerStatus: status,
        },
        findymailUsed: false,
        person,
        reason: code,
        errorMessage: String(err?.message || err).slice(0, 240),
      };
    }
    throw err;
  }

  const credits = Number(result.creditsDebitedEstimate || 0);
  if (budget && typeof budget === "object" && opts.dryRun !== true) {
    budget.used = Number(budget.used || 0) + credits;
  }
  if (opts.dryRun !== true) {
    appendFindymailUsageLedger(
      {
        authorizationId: opts.authorizationId || FINDYMAIL_STAGE4_AUTH_TOKEN,
        hotelId: opts.hotelId || opportunity.hotelId || null,
        opportunityId: opportunity.id || opportunity.opportunityId || null,
        personKey: eligibility.personKey,
        organizationName: eligibility.org,
        operation: "FIND_FROM_NAME",
        creditsDebited: credits,
        matchConfidence: result.email ? "PROVIDER_RETURNED" : "NO_MATCH",
        piiPersisted: false,
        resultDisposition: result.disposition,
        emailFound: Boolean(result.email),
      },
      opts,
    );
  }

  if (!result.email) {
    return {
      version: GDI_FINDYMAIL_STAGE4_ENRICHMENT_V1,
      outcome: "RESEARCH_COMPLETED_NOT_FOUND",
      eligibility,
      findymailUsed: opts.dryRun !== true,
      dryRun: opts.dryRun === true,
      result,
      person: {
        ...person,
        contactChannelResearch: {
          ...(person.contactChannelResearch || {}),
          professionalEmail: "RESEARCH_COMPLETED_NOT_FOUND",
          findymail: {
            attempted: true,
            disposition: result.disposition,
            at: new Date().toISOString(),
          },
        },
      },
    };
  }

  if (!emailLikelyMatchesPerson(result.email, eligibility.enrichmentName || eligibility.name)) {
    return {
      version: GDI_FINDYMAIL_STAGE4_ENRICHMENT_V1,
      outcome: "REJECTED_MISATTRIBUTED",
      eligibility,
      findymailUsed: opts.dryRun !== true,
      dryRun: opts.dryRun === true,
      result,
      reason: "EMAIL_LOCAL_PART_DOES_NOT_MATCH_PERSON",
      person: {
        ...person,
        contactChannelResearch: {
          ...(person.contactChannelResearch || {}),
          professionalEmail: "RESEARCH_COMPLETED_NOT_FOUND",
          findymail: {
            attempted: true,
            disposition: "REJECTED_MISATTRIBUTED",
            rejectedEmail: result.email,
            at: new Date().toISOString(),
          },
        },
      },
    };
  }

  const channel = {
    channelType: CHANNEL_EMAIL,
    value: result.email,
    source: "FINDYMAIL",
    sourceUrl: null,
    verificationStatus: STATUS_UNVERIFIED,
    verificationDate: new Date().toISOString(),
    personDirect: true,
    organizational: false,
    researchProvenance: {
      provider: "FINDYMAIL",
      operation: "FIND_FROM_NAME",
      clientVersion: GDI_FINDYMAIL_CLIENT_V1,
      domain: eligibility.domain,
    },
    personName: eligibility.name,
  };

  return {
    version: GDI_FINDYMAIL_STAGE4_ENRICHMENT_V1,
    outcome: "FOUND_VERIFICATION_REQUIRED",
    eligibility,
    findymailUsed: opts.dryRun !== true,
    dryRun: opts.dryRun === true,
    result,
    channel,
    person: {
      ...person,
      email: result.email,
      emailSource: "FINDYMAIL",
      emailOwnership: "person_specific",
      emailVerificationStatus: "FINDYMAIL_PROVIDER_RETURNED",
      emailCustomerVerified: false,
      channels: [...(person.channels || []), channel],
      contactChannelResearch: {
        ...(person.contactChannelResearch || {}),
        professionalEmail: "FOUND_VERIFICATION_REQUIRED",
        findymail: {
          attempted: true,
          disposition: result.disposition,
          at: new Date().toISOString(),
        },
      },
    },
    persistEligible: opts.persistFindymailResults === true,
  };
}

/**
 * Assess Findymail phone eligibility — LinkedIn required (provider contract).
 */
export function assessFindymailPhoneEligibility(person = {}, opportunity = {}, opts = {}) {
  const name = text(person.name || person.fullName);
  const org = text(
    person.organization ||
      person.organizationName ||
      person.company ||
      opportunity.organizationName ||
      "",
  );
  const personKey = normalizePersonKey(name, org);
  const linkedinUrl = text(
    person.linkedinUrl || person.linkedin || person.verifiedLinkedIn || "",
  ).replace(/\/$/, "");
  const phone = text(person.phone || person.mobile || person.directPhone);
  if (!name) {
    return { eligible: false, reason: "NAME_REQUIRED", personKey, name, org };
  }
  if (phone && phone.replace(/\D/g, "").length >= 7) {
    return {
      eligible: false,
      reason: "PHONE_ALREADY_PRESENT",
      personKey,
      name,
      org,
      phone,
    };
  }
  if (!linkedinUrl || !/linkedin\.com\/in\//i.test(linkedinUrl)) {
    return {
      eligible: false,
      reason: "LINKEDIN_REQUIRED_FOR_FINDYMAIL_PHONE",
      personKey,
      name,
      org,
    };
  }
  if (
    opts.ledger &&
    personAlreadyEnrichedInLedger(personKey, opts.ledger, {
      operation: "FIND_PHONE",
    })
  ) {
    return {
      eligible: false,
      reason: "ALREADY_IN_LEDGER_PHONE",
      personKey,
      name,
      org,
      linkedinUrl,
    };
  }
  return {
    eligible: true,
    reason: "NAMED_PERSON_MISSING_PHONE_WITH_LINKEDIN",
    personKey,
    name,
    org,
    linkedinUrl,
  };
}

function remainingBudgetCredits(opts = {}) {
  const hotelId = String(opts.hotelId || "").trim();
  const hardCap = Number(opts.creditHardCap || opts.findymailCreditBudget?.hardCap || 0);
  if (hotelId && hardCap > 0) {
    const gate = assertFindymailHotelHardCapAllowsDebit({
      ...opts,
      hotelId,
      creditHardCap: hardCap,
      estimatedCredits: 0,
    });
    return Math.max(0, hardCap - Number(gate.used || 0));
  }
  const budget = opts.findymailCreditBudget;
  if (!budget || typeof budget !== "object") {
    return hardCap > 0 ? hardCap : null;
  }
  const used = Number(budget.used || 0);
  const cap =
    Number(budget.hardCap) > 0
      ? Number(budget.hardCap)
      : hardCap > 0
        ? hardCap
        : 40;
  return Math.max(0, cap - used);
}

function reserveBudgetOrSkip(opts = {}, estimatedCredits = 1) {
  const hotelId = String(opts.hotelId || "").trim();
  const hardCap = Number(opts.creditHardCap || opts.findymailCreditBudget?.hardCap || 0);
  // Permanent: cumulative hotel ledger across auth IDs / resume / recovery.
  if (hotelId && hardCap > 0) {
    const gate = assertFindymailHotelHardCapAllowsDebit({
      ...opts,
      hotelId,
      creditHardCap: hardCap,
      estimatedCredits,
    });
    if (!gate.ok) {
      return {
        ok: false,
        cap: gate.hardCap,
        used: gate.used,
        estimatedCredits,
        reason: gate.reason,
        ledgerCredits: gate.ledgerCredits,
        byAuthorizationId: gate.byAuthorizationId,
      };
    }
    return {
      ok: true,
      cap: gate.hardCap,
      used: gate.used,
      ledgerCredits: gate.ledgerCredits,
    };
  }
  const budget = opts.findymailCreditBudget;
  if (!budget || typeof budget !== "object") return { ok: true };
  const used = Number(budget.used || 0);
  const cap =
    Number(budget.hardCap) > 0
      ? Number(budget.hardCap)
      : hardCap > 0
        ? hardCap
        : 40;
  if (used + Number(estimatedCredits || 0) > cap) {
    return {
      ok: false,
      cap,
      used,
      estimatedCredits,
      reason: "CREDIT_HARD_CAP_REACHED",
    };
  }
  return { ok: true, cap, used };
}

/**
 * Enrich professional telephone via Findymail LinkedIn phone search.
 * Credits: 10 when found, 0 when not found. Never auto-runs Surfe.
 */
export async function enrichPersonPhoneWithFindymail(
  person = {},
  opportunity = {},
  opts = {},
) {
  assertFindymailStage4Authorized(opts);
  const ledger = opts.ledger || readFindymailUsageLedger(opts);
  const eligibility = assessFindymailPhoneEligibility(person, opportunity, {
    ...opts,
    ledger,
  });
  if (!eligibility.eligible) {
    return {
      version: GDI_FINDYMAIL_STAGE4_ENRICHMENT_V1,
      outcome: "SKIPPED",
      channel: "phone",
      eligibility,
      findymailUsed: false,
      person,
    };
  }

  const phoneCost = FINDYMAIL_CREDIT_COSTS.find_phone_when_found;
  const budgetGate = reserveBudgetOrSkip(opts, phoneCost);
  if (!budgetGate.ok) {
    return {
      version: GDI_FINDYMAIL_STAGE4_ENRICHMENT_V1,
      outcome: "SKIPPED",
      channel: "phone",
      eligibility: {
        ...eligibility,
        eligible: false,
        reason: budgetGate.reason,
        creditHardCap: budgetGate.cap,
        creditsUsed: budgetGate.used,
        estimatedCredits: phoneCost,
      },
      findymailUsed: false,
      person,
      creditHardCapReached: true,
    };
  }

  let result;
  try {
    result = await findymailFindPhoneFromLinkedIn(
      { linkedinUrl: eligibility.linkedinUrl },
      { dryRun: opts.dryRun === true, apiKey: opts.apiKey },
    );
  } catch (err) {
    const code = err?.code || "FINDYMAIL_PHONE_FIND_FAILED";
    const status = Number(err?.status) || null;
    if (
      code === "FINDYMAIL_INVALID_LINKEDIN" ||
      code === "FINDYMAIL_PHONE_FIND_FAILED" ||
      (status >= 400 && status < 500 && status !== 402)
    ) {
      return {
        version: GDI_FINDYMAIL_STAGE4_ENRICHMENT_V1,
        outcome: "SKIPPED",
        channel: "phone",
        eligibility: {
          ...eligibility,
          eligible: false,
          reason: code,
          providerStatus: status,
        },
        findymailUsed: false,
        person,
        reason: code,
        errorMessage: String(err?.message || err).slice(0, 240),
      };
    }
    throw err;
  }

  const credits = Number(result.creditsDebitedEstimate || 0);
  const budget = opts.findymailCreditBudget;
  if (budget && typeof budget === "object" && opts.dryRun !== true) {
    budget.used = Number(budget.used || 0) + credits;
  }
  if (opts.dryRun !== true) {
    appendFindymailUsageLedger(
      {
        authorizationId: opts.authorizationId || FINDYMAIL_STAGE4_AUTH_TOKEN,
        hotelId: opts.hotelId || opportunity.hotelId || null,
        opportunityId: opportunity.id || opportunity.opportunityId || null,
        personKey: eligibility.personKey,
        organizationName: eligibility.org,
        operation: "FIND_PHONE",
        creditsDebited: credits,
        matchConfidence: result.phone ? "PROVIDER_RETURNED" : "NO_MATCH",
        piiPersisted: false,
        resultDisposition: result.disposition,
        phoneFound: Boolean(result.phone),
      },
      opts,
    );
  }

  if (!result.phone) {
    return {
      version: GDI_FINDYMAIL_STAGE4_ENRICHMENT_V1,
      outcome: "RESEARCH_COMPLETED_NOT_FOUND",
      channel: "phone",
      eligibility,
      findymailUsed: opts.dryRun !== true,
      dryRun: opts.dryRun === true,
      result,
      person: {
        ...person,
        contactChannelResearch: {
          ...(person.contactChannelResearch || {}),
          professionalTelephone: "RESEARCH_COMPLETED_NOT_FOUND",
          findymailPhone: {
            attempted: true,
            disposition: result.disposition,
            at: new Date().toISOString(),
          },
        },
      },
    };
  }

  const channel = {
    channelType: CHANNEL_PHONE,
    value: result.phone,
    source: "FINDYMAIL",
    sourceUrl: eligibility.linkedinUrl,
    verificationStatus: STATUS_UNVERIFIED,
    verificationDate: new Date().toISOString(),
    personDirect: true,
    organizational: false,
    researchProvenance: {
      provider: "FINDYMAIL",
      operation: "FIND_PHONE",
      clientVersion: GDI_FINDYMAIL_CLIENT_V1,
      linkedinUrl: eligibility.linkedinUrl,
    },
    personName: eligibility.name,
  };

  return {
    version: GDI_FINDYMAIL_STAGE4_ENRICHMENT_V1,
    outcome: "FOUND_VERIFICATION_REQUIRED",
    channel: "phone",
    eligibility,
    findymailUsed: opts.dryRun !== true,
    dryRun: opts.dryRun === true,
    result,
    phoneChannel: channel,
    person: {
      ...person,
      phone: result.phone,
      phoneSource: "FINDYMAIL",
      phoneOwnership: "person_specific",
      phoneVerificationStatus: "FINDYMAIL_PROVIDER_RETURNED",
      phoneCustomerVerified: false,
      channels: [...(person.channels || []), channel],
      contactChannelResearch: {
        ...(person.contactChannelResearch || {}),
        professionalTelephone: "FOUND_VERIFICATION_REQUIRED",
        findymailPhone: {
          attempted: true,
          disposition: result.disposition,
          at: new Date().toISOString(),
        },
      },
    },
    persistEligible: opts.persistFindymailResults === true,
  };
}

/**
 * Shared V3 Stage 4 entry — plan missing channels, then Findymail email and/or phone.
 * Surfe is never invoked. Finding email does not terminate phone research.
 */
export async function enrichPersonWithFindymailMissingChannels(
  person = {},
  opportunity = {},
  opts = {},
) {
  assertFindymailStage4Authorized(opts);
  const remaining = remainingBudgetCredits(opts);
  const plan = planMissingChannelEnrichment({
    person,
    provider: "findymail",
    remainingCredits: remaining,
    findymailEmailCreditCost: FINDYMAIL_CREDIT_COSTS.find_from_name_when_found,
    findymailPhoneCreditCost: FINDYMAIL_CREDIT_COSTS.find_phone_when_found,
    allowFindymailPhone: opts.allowFindymailPhone !== false,
  });

  let nextPerson = { ...person };
  const results = {
    email: null,
    phone: null,
  };
  let findymailUsed = false;

  const executable = plan.findymailExecutable || plan.findymailActions || [];
  const wantsEmail = executable.some(
    (s) => s.action === "FIND_EMAIL_FROM_NAME",
  );
  const wantsPhone = executable.some(
    (s) => s.action === "FIND_PHONE_FROM_LINKEDIN",
  );

  if (wantsEmail) {
    const emailRes = await enrichPersonEmailWithFindymail(
      nextPerson,
      opportunity,
      opts,
    );
    results.email = emailRes;
    nextPerson = emailRes.person || nextPerson;
    findymailUsed = findymailUsed || emailRes.findymailUsed === true;
  }

  // Recompute after email — durable email must not suppress independent phone research.
  const afterEmail = computeMissingContactChannels(nextPerson);
  const phoneStillNeeded =
    afterEmail.needsPhone && opts.allowFindymailPhone !== false;
  if (phoneStillNeeded && (wantsPhone || afterEmail.needsPhone)) {
    const phonePlan = planMissingChannelEnrichment({
      person: nextPerson,
      provider: "findymail",
      remainingCredits: remainingBudgetCredits(opts),
      findymailPhoneCreditCost: FINDYMAIL_CREDIT_COSTS.find_phone_when_found,
      allowFindymailPhone: true,
    });
    const phoneStep = (phonePlan.findymailExecutable || phonePlan.findymailActions || []).find(
      (s) => s.action === "FIND_PHONE_FROM_LINKEDIN",
    );
    if (phoneStep) {
      const phoneRes = await enrichPersonPhoneWithFindymail(
        nextPerson,
        opportunity,
        opts,
      );
      results.phone = phoneRes;
      nextPerson = phoneRes.person || nextPerson;
      findymailUsed = findymailUsed || phoneRes.findymailUsed === true;
    } else {
      results.phone = {
        outcome: "SKIPPED",
        channel: "phone",
        plan: phonePlan,
        findymailUsed: false,
        person: nextPerson,
      };
    }
  }

  const gotEmail = Boolean(nextPerson.email && String(nextPerson.email).includes("@"));
  const gotPhone = Boolean(
    nextPerson.phone && String(nextPerson.phone).replace(/\D/g, "").length >= 7,
  );
  const classification = classifyPartialChannelResult({
    requestedEmail: plan.missingChannelState.needsEmail,
    requestedPhone: plan.missingChannelState.needsPhone,
    gotEmail:
      gotEmail &&
      results.email &&
      results.email.outcome === "FOUND_VERIFICATION_REQUIRED",
    gotPhone:
      gotPhone &&
      results.phone &&
      results.phone.outcome === "FOUND_VERIFICATION_REQUIRED",
  });

  return {
    version: GDI_FINDYMAIL_STAGE4_ENRICHMENT_V1,
    contract: DEALALITY_DYNAMIC_CONTACT_ENRICHMENT_V3,
    plan,
    classification,
    outcome: classification,
    email: results.email,
    phone: results.phone,
    findymailUsed,
    person: nextPerson,
    surfeUsed: false,
    surfePersisted: false,
  };
}

/**
 * Build unique-person Findymail email enrichment manifest for a hotel package.
 */
export function buildFindymailEnrichmentManifest(opportunities = [], opts = {}) {
  const ledger = opts.ledger || readFindymailUsageLedger(opts);
  const byPerson = new Map();

  const pushPerson = (person, opportunity, role) => {
    if (!person || !text(person.name || person.fullName)) return;
    const elig = assessFindymailEmailEligibility(person, opportunity, {
      ledger,
    });
    const key = elig.personKey;
    const prev = byPerson.get(key) || {
      personKey: key,
      name: elig.name,
      organization: elig.org,
      domain: elig.domain,
      roles: [],
      opportunityIds: [],
      eligible: elig.eligible,
      reason: elig.reason,
      estimatedFinderCredits: elig.eligible ? 1 : 0,
    };
    prev.roles.push(role);
    prev.opportunityIds.push(opportunity.id || opportunity.opportunityId);
    if (elig.eligible) {
      prev.eligible = true;
      prev.reason = elig.reason;
      prev.domain = elig.domain || prev.domain;
      prev.estimatedFinderCredits = 1;
    }
    byPerson.set(key, prev);
  };

  for (const opp of opportunities) {
    pushPerson(opp.primaryContact, opp, "PRIMARY");
    pushPerson(opp.secondaryContact, opp, "SECONDARY");
    for (const alt of opp.alternativeEntryPoints ||
      opp.altEntryPoints ||
      []) {
      pushPerson(alt, opp, "ALT_ENTRY");
    }
  }

  const rows = [...byPerson.values()];
  const eligible = rows.filter((r) => r.eligible);
  const hardCap = Number(opts.creditHardCap || 40);
  const selected = eligible.slice(0, hardCap);
  return {
    version: GDI_FINDYMAIL_STAGE4_ENRICHMENT_V1,
    authorizationId: FINDYMAIL_STAGE4_AUTH_TOKEN,
    uniquePeople: rows.length,
    eligibleCount: eligible.length,
    selectedCount: selected.length,
    estimatedFinderCreditsMax: selected.length,
    creditHardCap: hardCap,
    keyPresence: describeFindymailKeyPresence(),
    selected,
    deferred: eligible.slice(hardCap),
    ineligible: rows.filter((r) => !r.eligible),
  };
}
