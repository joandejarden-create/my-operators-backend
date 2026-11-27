/**
 * Permanent Stage 4 Findymail enrichment — email channel only.
 *
 * Binding policy (FOUNDER GO 2026-11-27):
 * - Findymail is a permanent Full E2E Stage 4 component (not optional manual).
 * - Recover Airtable / prior research first; dedupe by personKey.
 * - Research all three channels; Findymail fills unresolved professional email.
 * - Surfe remains explicit-click only and never durably persisted.
 * - Verified Findymail emails may be persisted to Airtable when permitted.
 */
import {
  describeFindymailKeyPresence,
  findymailFindEmailFromName,
  appendFindymailUsageLedger,
  readFindymailUsageLedger,
  personAlreadyEnrichedInLedger,
  GDI_FINDYMAIL_CLIENT_V1,
} from "../providers/gdi-findymail-client-v1.js";

/** Local constants avoid circular import with gdi-contact-research-stage-v1.js */
const CHANNEL_EMAIL = "PROFESSIONAL_EMAIL";
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

export function looksLikeFullPersonName(name) {
  const n = text(name);
  if (!n || n.length < 3) return false;
  if (/\b(phd|mba|cpa)\b/i.test(n) && n.split(/\s+/).length < 2) return false;
  if (/\b[A-Za-z]\.$/.test(n)) return false; // truncated surname "Lily Y."
  if (!/^[A-Za-z][A-Za-z .'\-]{1,100}$/.test(n)) return false;
  return n.split(/\s+/).filter(Boolean).length >= 2;
}

export function extractCompanyDomain(person = {}, opportunity = {}) {
  const email = text(person.email);
  const fromEmail = email.includes("@")
    ? email.split("@")[1].toLowerCase()
    : "";
  if (fromEmail && !BAD_DOMAINS.has(fromEmail)) return fromEmail;

  const candidates = [];
  for (const s of opportunity.sources || []) {
    const u = text(s.url || s.href || s);
    try {
      const host = new URL(u).hostname.replace(/^www\./i, "").toLowerCase();
      if (host) candidates.push(host);
    } catch {
      /* ignore */
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

  const result = await findymailFindEmailFromName(
    { name: eligibility.name, domain: eligibility.domain },
    { dryRun: opts.dryRun === true, apiKey: opts.apiKey },
  );

  const credits = Number(result.creditsDebitedEstimate || 0);
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

  if (!emailLikelyMatchesPerson(result.email, eligibility.name)) {
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
