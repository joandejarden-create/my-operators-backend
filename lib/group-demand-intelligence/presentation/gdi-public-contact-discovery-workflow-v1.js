/**
 * Permanent public-source contact discovery workflow (founder V1).
 *
 * Sequence:
 *  1) Verified named decision-maker required
 *  2) Reuse prior public research for same person (cross-hotel)
 *  3) Authoritative URL recovery (existing pages / Tier-1 candidates)
 *  4) Optional paid public search only when explicitly authorized
 *  5) Classify verification statuses
 *  6) Decide Surfe / FullEnrich offers — never auto-pay
 *
 * Reuses:
 *  - gdi-public-contact-discovery-v1 / channel-hierarchy
 *  - gdi-missing-channel-resolver-v1 (when present — Google-style branch)
 *  - contact-field-provenance personIdentityKey
 *
 * Does NOT call Surfe or FullEnrich.
 * Does NOT persist unless caller passes persistPublicChannels and founder auth.
 */

import { looksLikePersonName } from "../secondary-named-contact-v1.js";
import {
  recoverPublicContactChannels,
  mergePublicChannelsOntoOpportunity,
  buildContactChannelDisplayState,
  collectPublicContactSourceUrls,
  proposeAirtablePublicContactUpsert,
} from "./gdi-public-contact-channel-hierarchy-v1.js";
import {
  discoverPublicContactChannels,
  PUBLIC_CONTACT_RESEARCH_STATUS,
} from "./gdi-public-contact-discovery-v1.js";
import {
  assessPublicContactVerification,
  decideEnrichmentChannels,
  publicContactPersonKey,
  PUBLIC_CONTACT_VERIFICATION_STATUS,
  GDI_PUBLIC_CONTACT_VERIFICATION_V1,
} from "./gdi-public-contact-verification-v1.js";

export const GDI_PUBLIC_CONTACT_DISCOVERY_WORKFLOW_V1 =
  "gdi-public-contact-discovery-workflow-v1";

export const DEFAULT_RESEARCH_EFFORT = Object.freeze({
  minQueries: 0,
  maxQueries: 6,
  maxPages: 4,
  timeoutMs: 15000,
  stopWhenDirectEmailAndPhone: true,
  stopWhenDirectEmail: false,
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

/** In-memory cross-hotel reuse cache for a single process/eval run. */
const personResearchCache = new Map();

export function clearPublicContactResearchCacheForTests() {
  personResearchCache.clear();
}

export function getCachedPublicContactResearch(personKey) {
  if (!personKey) return null;
  return personResearchCache.get(personKey) || null;
}

export function putCachedPublicContactResearch(personKey, payload) {
  if (!personKey || !payload) return;
  personResearchCache.set(personKey, {
    ...payload,
    cachedAt: new Date().toISOString(),
  });
}

function effortConfig(opts = {}) {
  return {
    ...DEFAULT_RESEARCH_EFFORT,
    ...(opts.effort || {}),
    maxQueries: Math.max(
      0,
      Number(opts.maxQueries ?? opts.effort?.maxQueries ?? DEFAULT_RESEARCH_EFFORT.maxQueries),
    ),
    maxPages: Math.max(
      1,
      Number(opts.maxPages ?? opts.effort?.maxPages ?? DEFAULT_RESEARCH_EFFORT.maxPages),
    ),
  };
}

/**
 * Run bounded public-first discovery for one opportunity contact.
 *
 * @param {object} args
 * @param {object} args.opportunity
 * @param {string} [args.hotelId]
 * @param {boolean} [args.authorizePaidPublicSearch=false]
 * @param {boolean} [args.persist=false] — must stay false unless founder-approved
 * @param {Function} [args.persistPublicChannels]
 * @param {object} [args.searchProvider]
 * @param {boolean} [args.reuseCrossHotelCache=true]
 */
export async function runPublicContactDiscoveryWorkflow(args = {}) {
  const started = Date.now();
  const opportunity = { ...(args.opportunity || {}) };
  const hotelId = text(args.hotelId) || null;
  const authorizePaid = args.authorizePaidPublicSearch === true;
  const persist = args.persist === true;
  const effort = effortConfig(args);
  const methods = [];
  const metrics = {
    paidPublicSearchAuthorized: authorizePaid,
    paidPublicSearchUsed: false,
    surfeAttempted: false,
    fullEnrichAttempted: false,
    persistAttempted: false,
    crossHotelReuse: false,
    queriesUsed: 0,
    pagesFetched: 0,
    researchMs: 0,
    estimatedPaidSearchCostUsd: 0,
  };

  const name =
    text(opportunity.primaryContact?.name) ||
    text(opportunity.contactName) ||
    text(args.contact?.name);
  const org =
    text(opportunity.organizationName) ||
    text(opportunity.displayCompanyName) ||
    text(args.organization?.name);

  if (!name || !looksLikePersonName(name)) {
    return {
      ok: false,
      version: GDI_PUBLIC_CONTACT_DISCOVERY_WORKFLOW_V1,
      error: "NO_VERIFIED_NAMED_PERSON",
      metrics,
      verification: null,
      enrichmentDecision: null,
      display: buildContactChannelDisplayState(opportunity),
      opportunity,
      airtableUpsertProposal: null,
    };
  }

  const personKey = publicContactPersonKey({ name, organization: org });
  let working = opportunity;
  let before = assessPublicContactVerification(working);

  // Cross-hotel reuse
  if (args.reuseCrossHotelCache !== false && personKey) {
    const cached = getCachedPublicContactResearch(personKey);
    if (cached?.found && (cached.found.email || cached.found.phone)) {
      const merged = mergePublicChannelsOntoOpportunity(working, cached.found);
      working = merged.opportunity;
      methods.push({
        method: "cross_hotel_person_reuse",
        ok: true,
        fromHotelId: cached.hotelId || null,
        fromOpportunityId: cached.opportunityId || null,
      });
      metrics.crossHotelReuse = true;
      before = assessPublicContactVerification(working);
    }
  }

  // Already sufficient?
  if (
    before.hasVerifiedDirectEmail &&
    (before.hasVerifiedDirectPhone ||
      !effort.stopWhenDirectEmailAndPhone ||
      effort.stopWhenDirectEmail)
  ) {
    if (before.hasVerifiedDirectEmail && before.hasVerifiedDirectPhone) {
      methods.push({ method: "already_verified_public", ok: true });
      const decision = decideEnrichmentChannels(before);
      metrics.researchMs = Date.now() - started;
      return {
        ok: true,
        version: GDI_PUBLIC_CONTACT_DISCOVERY_WORKFLOW_V1,
        verificationVersion: GDI_PUBLIC_CONTACT_VERIFICATION_V1,
        researchStatus: PUBLIC_CONTACT_RESEARCH_STATUS.REUSED_EXISTING,
        personIdentityKey: personKey,
        hotelId,
        methods,
        metrics,
        before,
        after: before,
        verification: before,
        enrichmentDecision: decision,
        display: buildContactChannelDisplayState(working),
        opportunity: working,
        airtableUpsertProposal: proposeAirtablePublicContactUpsert(working),
        changed: false,
      };
    }
  }

  // Tier-1: recover from existing authoritative URLs (free)
  const urls = collectPublicContactSourceUrls(working);
  if (urls.length) {
    const recovered = await recoverPublicContactChannels(working, {
      maxUrls: Math.min(urls.length, effort.maxPages),
      timeoutMs: effort.timeoutMs,
      fetchImpl: args.fetchImpl,
      skipDns: args.skipDns === true,
      skipIfComplete: false,
    });
    methods.push({
      method: "authoritative_url_recovery",
      ok: recovered.ok === true,
      reason: recovered.reason || null,
      urlsConsidered: urls.length,
    });
    metrics.pagesFetched += Math.min(urls.length, effort.maxPages);
    if (recovered.ok && recovered.found) {
      const merged = mergePublicChannelsOntoOpportunity(
        recovered.opportunity || working,
        recovered.found,
      );
      working = merged.opportunity;
    }
  } else {
    methods.push({ method: "authoritative_url_recovery", ok: false, reason: "NO_SOURCE_URLS" });
  }

  let mid = assessPublicContactVerification(working);
  const sufficient =
    mid.hasVerifiedDirectEmail &&
    (effort.stopWhenDirectEmail || mid.hasVerifiedDirectPhone);

  // Optional paid public search (Exa/Google) — never default on
  if (!sufficient && authorizePaid && effort.maxQueries > 0) {
    metrics.paidPublicSearchUsed = true;
    const discovered = await discoverPublicContactChannels({
      opportunity: working,
      candidateUrls: args.candidateUrls || urls,
      searchProvider: args.searchProvider || null,
      authorizePaidSearch: true,
      maxQueries: effort.maxQueries,
      maxPages: effort.maxPages,
      timeoutMs: effort.timeoutMs,
      fetchImpl: args.fetchImpl,
      skipDns: args.skipDns === true,
      env: args.env,
    });
    methods.push({
      method: "paid_public_search",
      ok: discovered?.ok === true,
      researchStatus: discovered?.researchStatus || null,
      queries: discovered?.queriesUsed ?? discovered?.queryCount ?? null,
    });
    metrics.queriesUsed += Number(discovered?.queriesUsed || discovered?.queryCount || 0);
    metrics.estimatedPaidSearchCostUsd += Number(
      discovered?.estimatedCostUsd || discovered?.costUsd || 0,
    );
    if (discovered?.opportunity) working = discovered.opportunity;
    else if (discovered?.found) {
      const merged = mergePublicChannelsOntoOpportunity(working, discovered.found);
      working = merged.opportunity;
    }
  } else if (!sufficient && !authorizePaid) {
    methods.push({
      method: "paid_public_search",
      ok: false,
      skipped: true,
      reason: "PAID_PUBLIC_SEARCH_NOT_AUTHORIZED",
    });
  }

  const after = assessPublicContactVerification(working);
  const decision = decideEnrichmentChannels(after);

  // Optional persist — blocked unless explicitly requested
  if (persist && typeof args.persistPublicChannels === "function") {
    metrics.persistAttempted = true;
    try {
      const persisted = await args.persistPublicChannels(working);
      methods.push({ method: "persist_public_channels", ok: persisted?.ok !== false });
      if (persisted?.opportunity) working = persisted.opportunity;
    } catch (err) {
      methods.push({
        method: "persist_public_channels",
        ok: false,
        error: text(err?.message).slice(0, 120),
      });
    }
  } else {
    methods.push({
      method: "persist_public_channels",
      ok: false,
      skipped: true,
      reason: persist ? "NO_PERSIST_HOOK" : "PERSIST_NOT_AUTHORIZED",
    });
  }

  if (personKey && (after.hasVerifiedDirectEmail || after.hasVerifiedDirectPhone || after.hasVerifiedOrgEmailRoute)) {
    putCachedPublicContactResearch(personKey, {
      hotelId,
      opportunityId: working.id || working.opportunityId || null,
      found: {
        email: after.hasVerifiedDirectEmail ? after.email.value : null,
        phone: after.hasVerifiedDirectPhone ? after.phone.value : null,
        organizationEmail: after.hasVerifiedOrgEmailRoute ? after.email.value : null,
        organizationPhone: after.hasVerifiedOrgPhoneRoute ? after.phone.value : null,
        sourceUrl: after.email.sourceUrl || after.phone.sourceUrl || null,
      },
      verification: after,
    });
  }

  metrics.researchMs = Date.now() - started;
  const researchStatus =
    after.hasVerifiedDirectEmail || after.hasVerifiedDirectPhone
      ? PUBLIC_CONTACT_RESEARCH_STATUS.PUBLIC_CHANNELS_VERIFIED
      : after.hasVerifiedOrgEmailRoute || after.hasVerifiedOrgPhoneRoute
        ? PUBLIC_CONTACT_RESEARCH_STATUS.PARTIAL_PUBLIC_CHANNELS
        : authorizePaid
          ? PUBLIC_CONTACT_RESEARCH_STATUS.PUBLIC_SEARCH_EXHAUSTED
          : PUBLIC_CONTACT_RESEARCH_STATUS.PUBLIC_RESEARCH_INCOMPLETE;

  return {
    ok: true,
    version: GDI_PUBLIC_CONTACT_DISCOVERY_WORKFLOW_V1,
    verificationVersion: GDI_PUBLIC_CONTACT_VERIFICATION_V1,
    researchStatus,
    personIdentityKey: personKey,
    hotelId,
    methods,
    metrics,
    before,
    after,
    verification: after,
    enrichmentDecision: decision,
    display: buildContactChannelDisplayState(working),
    opportunity: working,
    airtableUpsertProposal: proposeAirtablePublicContactUpsert(working),
    changed:
      before.hasVerifiedDirectEmail !== after.hasVerifiedDirectEmail ||
      before.hasVerifiedDirectPhone !== after.hasVerifiedDirectPhone ||
      before.hasVerifiedOrgEmailRoute !== after.hasVerifiedOrgEmailRoute ||
      before.hasVerifiedOrgPhoneRoute !== after.hasVerifiedOrgPhoneRoute,
    statuses: PUBLIC_CONTACT_VERIFICATION_STATUS,
  };
}
