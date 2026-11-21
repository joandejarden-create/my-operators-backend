/**
 * Admin-only FullEnrich benchmark eligibility from genuine Surfe email misses.
 * Non-PII only — never reads/stores provider-returned emails or phones.
 *
 * Eligibility: user-authorized Surfe email miss + verified named GDI contact +
 * FE-ready identity inputs + no later work-email recovery + no prior FE attempt
 * for the same person/channel + valid hotel/opportunity. Fixtures excluded.
 */

import { createHash } from "node:crypto";
import {
  readSurfeLiveAccounting,
  contactRevealIdentityKey,
} from "./gdi-surfe-live-gate-v1.js";
import { classifyUsageClass } from "./gdi-surfe-usage-context-v1.js";
import { personIdentityKey as provenancePersonIdentityKey } from "./workflow/contact-field-provenance-v1.js";
import { listAllFullEnrichJobs } from "./gdi-fullenrich-spend-ledger-v1.js";
import { loadOpportunities } from "./repository.js";

export const GDI_FULLENRICH_BENCHMARK_ELIGIBILITY_V1 =
  "gdi-fullenrich-benchmark-eligibility-v1";

const ESTIMATED_FE_CREDIT_PER_EMAIL = 1;

function text(v) {
  return String(v == null ? "" : v).trim();
}

function normName(name) {
  return text(name).toLowerCase().replace(/\s+/g, " ");
}

/**
 * Cross-hotel person key: verified name + organization (reuse provenance hasher).
 * Requires both name and organization so same-name unrelated people do not merge.
 */
export function personIdentityKey({
  contactName,
  organizationName,
  name,
  organization,
  domain,
} = {}) {
  const n = text(contactName || name);
  const o = text(organizationName || organization || domain);
  if (!n || n.split(/\s+/).filter(Boolean).length < 2) return null;
  if (!o || o.length < 2) return null;
  return provenancePersonIdentityKey({ name: n, organization: o, domain });
}

export function contactNameHash(contactName) {
  const n = normName(contactName);
  if (!n) return null;
  return createHash("sha256").update(`gdi-contact-name|${n}`).digest("hex").slice(0, 16);
}

function providerOf(e) {
  return String(e?.contactProvider || "SURFE").toUpperCase() === "FULLENRICH"
    ? "FULLENRICH"
    : "SURFE";
}

function channelOf(e) {
  return String(e?.channelRequested || e?.channel || e?.lookupType || "")
    .toLowerCase()
    .trim();
}

function isEmailChannel(ch) {
  return !ch || ch === "email" || ch === "both";
}

function isFixture(e, env = process.env) {
  return classifyUsageClass(e, env) === "FIXTURE_TEST";
}

/** Genuine Surfe work-email miss (not fixture, not FullEnrich row). */
export function isGenuineSurfeWorkEmailMiss(entry, env = process.env) {
  if (!entry || typeof entry !== "object") return false;
  if (isFixture(entry, env)) return false;
  if (providerOf(entry) !== "SURFE") return false;
  if (entry.surfeAttempted === false) return false;
  if (!isEmailChannel(channelOf(entry))) return false;
  if (entry.emailReturned === true || entry.emailFound === true) return false;
  if (entry.miss === true) return true;
  if (String(entry.status || "").toLowerCase() === "no_match") return true;
  // Partial with phone but no email = work-email miss for FE email fallback.
  if (
    String(entry.status || "").toLowerCase() === "partial" &&
    (entry.phoneReturned === true || entry.phoneFound === true)
  ) {
    return true;
  }
  return false;
}

export function extractContactsFromOpportunity(opportunity = {}) {
  const out = [];
  const org =
    opportunity.organizationName ||
    opportunity.displayCompanyName ||
    opportunity.company ||
    null;
  const domain =
    opportunity.organizationDomain || opportunity.companyDomain || null;

  function push(c, slot) {
    if (!c) return;
    const name = text(c.name || c.fullName || "");
    if (!name) return;
    const email = text(c.email || c.contactEmail || "");
    out.push({
      slot,
      name,
      role: text(c.title || c.role) || null,
      organizationName: text(c.organization || org) || org,
      companyDomain: text(c.companyDomain || domain) || domain,
      linkedinUrl:
        text(c.linkedinUrl || c.profileUrl || c.sourceUrl || "") || null,
      hasPublicEmail: Boolean(email),
    });
  }

  push(opportunity.primaryContact, "primary");
  push(opportunity.secondaryContact, "secondary");
  for (const b of opportunity.buyerCandidates || []) {
    push(
      {
        name: b.fullName || b.name,
        title: b.title || b.role,
        organization: b.organization || org,
        linkedinUrl: b.sourceUrl || b.linkedinUrl,
        email: b.email,
        companyDomain: b.companyDomain || domain,
      },
      "buyer",
    );
  }
  if (opportunity.contactName || opportunity.primaryContactName) {
    push(
      {
        name: opportunity.contactName || opportunity.primaryContactName,
        title:
          opportunity.contactRole ||
          opportunity.contactTitle ||
          opportunity.primaryContactRole,
        email: opportunity.contactEmail || opportunity.primaryContactEmail,
        linkedinUrl: opportunity.contactLinkedinUrl,
      },
      "legacy",
    );
  }
  return out;
}

export function assessFullEnrichIdentityReadiness(contact = {}) {
  const name = text(contact.name);
  const parts = name.split(/\s+/).filter(Boolean);
  const hasFirstLast = parts.length >= 2;
  const hasDomain = Boolean(text(contact.companyDomain));
  const hasCompany = Boolean(text(contact.organizationName));
  const linkedin = text(contact.linkedinUrl);
  const hasLinkedIn = Boolean(linkedin && /linkedin\.com\/in\//i.test(linkedin));
  const ready = hasFirstLast && (hasLinkedIn || hasDomain || hasCompany);
  return {
    ready,
    hasFirstLast,
    hasCompanyName: hasCompany,
    hasCompanyDomain: hasDomain,
    hasLinkedInUrl: hasLinkedIn,
    reasons: ready
      ? []
      : [
          !hasFirstLast ? "NEED_FIRST_LAST_NAME" : null,
          !hasLinkedIn && !hasDomain && !hasCompany
            ? "NEED_COMPANY_OR_LINKEDIN"
            : null,
        ].filter(Boolean),
  };
}

function defaultLoadOpportunity(hotelId, opportunityId) {
  try {
    const opps = loadOpportunities(hotelId) || [];
    return (
      opps.find(
        (o) =>
          String(o.id || o.opportunityId || "") === String(opportunityId || ""),
      ) || null
    );
  } catch {
    return null;
  }
}

export function resolveContactForMiss(
  entry,
  { loadOpportunity = defaultLoadOpportunity } = {},
) {
  const hotelId = text(entry.hotelId);
  const opportunityId = text(entry.opportunityId);
  if (!hotelId || !opportunityId) {
    return { ok: false, reason: "MISSING_HOTEL_OR_OPPORTUNITY" };
  }
  const opportunity = loadOpportunity(hotelId, opportunityId);
  if (!opportunity) {
    return { ok: false, reason: "OPPORTUNITY_NOT_FOUND" };
  }
  const contacts = extractContactsFromOpportunity(opportunity);
  const channel = channelOf(entry) || "email";
  let matched = null;
  for (const c of contacts) {
    const key = contactRevealIdentityKey({
      hotelId,
      opportunityId,
      contactName: c.name,
      channel,
    });
    const keyBoth = contactRevealIdentityKey({
      hotelId,
      opportunityId,
      contactName: c.name,
      channel: "both",
    });
    if (
      entry.identityKey &&
      (entry.identityKey === key || entry.identityKey === keyBoth)
    ) {
      matched = c;
      break;
    }
  }
  if (!matched && entry.personIdentityKey) {
    matched =
      contacts.find(
        (c) =>
          personIdentityKey({
            contactName: c.name,
            organizationName: c.organizationName,
          }) === entry.personIdentityKey,
      ) || null;
  }
  if (!matched && entry.contactNameHash) {
    const hits = contacts.filter(
      (c) => contactNameHash(c.name) === entry.contactNameHash,
    );
    if (hits.length === 1) matched = hits[0];
    else if (hits.length > 1) {
      return { ok: false, reason: "AMBIGUOUS_NAME_HASH", contacts: hits.length };
    }
  }
  if (!matched) {
    return {
      ok: false,
      reason: "CONTACT_IDENTITY_UNRESOLVED",
      candidateCount: contacts.length,
      opportunityTitle: opportunity.title || opportunity.displayTitle || null,
      organizationName:
        opportunity.organizationName ||
        opportunity.displayCompanyName ||
        null,
    };
  }
  if (matched.hasPublicEmail) {
    return {
      ok: false,
      reason: "PUBLIC_EMAIL_ALREADY_PRESENT",
      contact: matched,
    };
  }
  return {
    ok: true,
    contact: matched,
    opportunity,
    opportunityTitle: opportunity.title || opportunity.displayTitle || null,
    organizationName: matched.organizationName,
  };
}

function feJobsForPerson(jobs, { personKey, identityKey, hotelId, opportunityId }) {
  return (jobs || []).filter((j) => {
    if (personKey && j.personIdentityKey === personKey) return true;
    if (identityKey && j.identityKey === identityKey) return true;
    if (
      hotelId &&
      opportunityId &&
      j.hotelId === hotelId &&
      j.opportunityId === opportunityId &&
      String(j.channel || "email").toLowerCase() === "email"
    ) {
      return true;
    }
    return false;
  });
}

function laterEmailRecovery(entries, missEntry, personKey) {
  const missAt = Date.parse(missEntry.at || "") || 0;
  return entries.some((e) => {
    if (!e || e === missEntry) return false;
    const at = Date.parse(e.at || "") || 0;
    if (at <= missAt) return false;
    const emailOk =
      e.emailReturned === true ||
      e.emailFound === true ||
      e.incrementalEmailFound === true;
    if (!emailOk) return false;
    if (missEntry.identityKey && e.identityKey === missEntry.identityKey) {
      return true;
    }
    if (personKey && e.personIdentityKey === personKey) return true;
    if (
      e.hotelId === missEntry.hotelId &&
      e.opportunityId === missEntry.opportunityId &&
      isEmailChannel(channelOf(e))
    ) {
      return true;
    }
    return false;
  });
}

/**
 * Build admin eligibility report from durable ledgers + GDI opportunity identity.
 */
export function buildFullEnrichBenchmarkEligibilityReport({
  env = process.env,
  estimatedCreditsPerContact = ESTIMATED_FE_CREDIT_PER_EMAIL,
  loadOpportunity = defaultLoadOpportunity,
} = {}) {
  const { doc } = readSurfeLiveAccounting(env);
  const entries = Array.isArray(doc.entries) ? doc.entries.slice() : [];
  let feJobs = [];
  try {
    feJobs = listAllFullEnrichJobs(env);
  } catch {
    feJobs = [];
  }

  const missRows = [];
  const excluded = {
    fixture: 0,
    notSurfe: 0,
    notEmailChannel: 0,
    hadEmail: 0,
    unresolvedIdentity: 0,
    publicEmailPresent: 0,
    laterRecovered: 0,
    priorFullEnrich: 0,
    identityNotReady: 0,
    other: 0,
  };

  for (const e of entries) {
    if (isFixture(e, env)) {
      if (
        providerOf(e) === "SURFE" &&
        isEmailChannel(channelOf(e)) &&
        e.emailReturned !== true &&
        (e.miss === true || String(e.status || "") === "no_match")
      ) {
        excluded.fixture += 1;
      }
      continue;
    }
    if (providerOf(e) !== "SURFE") {
      excluded.notSurfe += 1;
      continue;
    }
    if (!isEmailChannel(channelOf(e))) {
      excluded.notEmailChannel += 1;
      continue;
    }
    if (e.emailReturned === true || e.emailFound === true) {
      excluded.hadEmail += 1;
      continue;
    }
    if (!isGenuineSurfeWorkEmailMiss(e, env)) {
      excluded.other += 1;
      continue;
    }

    const resolved = resolveContactForMiss(e, { loadOpportunity });
    if (!resolved.ok) {
      if (resolved.reason === "PUBLIC_EMAIL_ALREADY_PRESENT") {
        excluded.publicEmailPresent += 1;
      } else {
        excluded.unresolvedIdentity += 1;
      }
      continue;
    }

    const personKey =
      e.personIdentityKey ||
      personIdentityKey({
        contactName: resolved.contact.name,
        organizationName: resolved.organizationName,
      });
    if (!personKey) {
      excluded.unresolvedIdentity += 1;
      continue;
    }

    if (laterEmailRecovery(entries, e, personKey)) {
      excluded.laterRecovered += 1;
      continue;
    }

    const priorFe = feJobsForPerson(feJobs, {
      personKey,
      identityKey: e.identityKey,
      hotelId: e.hotelId,
      opportunityId: e.opportunityId,
    });
    if (priorFe.length) {
      excluded.priorFullEnrich += 1;
      continue;
    }

    const readiness =
      e.fullEnrichIdentityInputs && typeof e.fullEnrichIdentityInputs === "object"
        ? {
            ready:
              e.fullEnrichIdentityInputs.ready === true ||
              (e.fullEnrichIdentityInputs.hasFirstLast === true &&
                (e.fullEnrichIdentityInputs.hasCompanyName === true ||
                  e.fullEnrichIdentityInputs.hasCompanyDomain === true ||
                  e.fullEnrichIdentityInputs.hasLinkedInUrl === true)),
            hasFirstLast: e.fullEnrichIdentityInputs.hasFirstLast === true,
            hasCompanyName: e.fullEnrichIdentityInputs.hasCompanyName === true,
            hasCompanyDomain: e.fullEnrichIdentityInputs.hasCompanyDomain === true,
            hasLinkedInUrl: e.fullEnrichIdentityInputs.hasLinkedInUrl === true,
            reasons: [],
          }
        : assessFullEnrichIdentityReadiness(resolved.contact);
    if (!readiness.ready) {
      excluded.identityNotReady += 1;
      continue;
    }

    missRows.push({
      lookupEventId: e.lookupEventId || null,
      at: e.at || null,
      hotelId: e.hotelId || null,
      hotelName: e.hotelName && e.hotelName !== "UNKNOWN" ? e.hotelName : null,
      hotelMarket: e.hotelMarket || null,
      hotelCountry: e.hotelCountry || null,
      opportunityId: e.opportunityId || null,
      opportunityTitle:
        e.opportunityTitle && e.opportunityTitle !== "UNKNOWN"
          ? e.opportunityTitle
          : resolved.opportunityTitle,
      opportunityOrganizationName:
        e.opportunityOrganizationName || resolved.organizationName || null,
      requestingOrganizationName: e.requestingOrganizationName || null,
      requestingPortfolioId: e.requestingPortfolioId || null,
      channel: channelOf(e) || "email",
      status: e.status || null,
      miss: e.miss === true,
      phoneReturned: e.phoneReturned === true || e.phoneFound === true,
      creditCertainty: e.creditCertainty || null,
      creditsCharged: e.creditsCharged ?? null,
      usageClass: classifyUsageClass(e, env),
      dataEnvironment: e.dataEnvironment || null,
      identityKey: e.identityKey || null,
      personIdentityKey: personKey,
      contactNameHash: contactNameHash(resolved.contact.name),
      contactSlot: resolved.contact.slot,
      contactRole: resolved.contact.role,
      contactDisplayName: resolved.contact.name,
      identityReadiness: readiness,
      surfeEnrichmentId: e.enrichmentId || null,
    });
  }

  const byPerson = new Map();
  for (const row of missRows) {
    const key = row.personIdentityKey;
    if (!byPerson.has(key)) {
      byPerson.set(key, {
        personIdentityKey: key,
        contactNameHash: row.contactNameHash,
        contactDisplayName: row.contactDisplayName,
        contactRole: row.contactRole,
        organizationName: row.opportunityOrganizationName,
        identityReadiness: row.identityReadiness,
        firstMissAt: row.at,
        latestMissAt: row.at,
        missCount: 0,
        opportunities: [],
        requestingOrganizations: new Set(),
        hotels: new Set(),
      });
    }
    const p = byPerson.get(key);
    p.missCount += 1;
    if (row.at && (!p.firstMissAt || row.at < p.firstMissAt)) p.firstMissAt = row.at;
    if (row.at && (!p.latestMissAt || row.at > p.latestMissAt)) {
      p.latestMissAt = row.at;
    }
    p.opportunities.push({
      hotelId: row.hotelId,
      hotelName: row.hotelName,
      hotelMarket: row.hotelMarket,
      hotelCountry: row.hotelCountry,
      opportunityId: row.opportunityId,
      opportunityTitle: row.opportunityTitle,
      lookupEventId: row.lookupEventId,
      at: row.at,
      requestingOrganizationName: row.requestingOrganizationName,
      channel: row.channel,
      status: row.status,
      phoneReturned: row.phoneReturned,
    });
    if (row.requestingOrganizationName) {
      p.requestingOrganizations.add(row.requestingOrganizationName);
    }
    if (row.hotelId) p.hotels.add(row.hotelId);
  }

  const distinctContacts = [...byPerson.values()].map((p) => ({
    personIdentityKey: p.personIdentityKey,
    contactNameHash: p.contactNameHash,
    contactDisplayName: p.contactDisplayName,
    contactRole: p.contactRole,
    organizationName: p.organizationName,
    identityReadiness: p.identityReadiness,
    firstMissAt: p.firstMissAt,
    latestMissAt: p.latestMissAt,
    missEventCount: p.missCount,
    hotelCount: p.hotels.size,
    opportunityCount: p.opportunities.length,
    hotels: [...p.hotels],
    requestingOrganizations: [...p.requestingOrganizations],
    opportunities: p.opportunities,
    estimatedCreditsIfBenchmarked: estimatedCreditsPerContact,
    eligible: true,
    priorFullEnrichAttempt: false,
  }));

  distinctContacts.sort((a, b) =>
    String(b.latestMissAt || "").localeCompare(String(a.latestMissAt || "")),
  );

  const estimatedBenchmarkCreditExposure =
    distinctContacts.length * estimatedCreditsPerContact;

  return {
    version: GDI_FULLENRICH_BENCHMARK_ELIGIBILITY_V1,
    generatedAt: new Date().toISOString(),
    summary: {
      genuineEligibleMissEvents: missRows.length,
      distinctVerifiedContacts: distinctContacts.length,
      associatedHotels: new Set(missRows.map((m) => m.hotelId).filter(Boolean))
        .size,
      associatedOpportunities: new Set(
        missRows.map((m) => m.opportunityId).filter(Boolean),
      ).size,
      missingWorkEmailCoverage: distinctContacts.length,
      fullEnrichIdentityReadyCount: distinctContacts.filter(
        (c) => c.identityReadiness?.ready,
      ).length,
      priorFullEnrichAttemptCount: excluded.priorFullEnrich,
      existingFullEnrichJobCount: feJobs.length,
      estimatedBenchmarkCreditExposure,
      estimatedCreditsPerContact,
      excluded,
    },
    shortlist: distinctContacts.slice(0, 25),
    missEvents: missRows.slice(0, 100),
    privacy: {
      includesProviderReturnedEmails: false,
      includesProviderReturnedPhones: false,
      includesRawProviderPayloads: false,
      contactNamesFrom: "public_gdi_opportunity_records",
      customerFacing: false,
    },
    controls: {
      surfePrimary: true,
      fullEnrichSpendFlagsRequired: true,
      automaticPaidEnrichment: false,
      customerDisplayBlocked: true,
      paidLookupsTriggered: false,
    },
  };
}
