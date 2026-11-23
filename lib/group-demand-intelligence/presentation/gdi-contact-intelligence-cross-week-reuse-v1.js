/**
 * Cross-week / cross-hotel contact reuse (V2.2).
 *
 * Layers:
 *   1. PROCESS_LOCAL_CACHE — existing workflow Map (same process only)
 *   2. DURABLE_FIXTURE_STORE — file/fixture person index (cross-run tests)
 *
 * Live Airtable person reuse remains OFF. Provider payloads never enter durable store.
 */

import {
  getCachedPublicContactResearch,
  putCachedPublicContactResearch,
} from "./gdi-public-contact-discovery-workflow-v1.js";
import { publicContactPersonKey } from "./gdi-public-contact-verification-v1.js";
import {
  REUSE_LAYER,
  CHANNEL_KIND,
  createDurableContactStore,
} from "./gdi-durable-contact-intelligence-store-v1.js";
import { looksLikePersonName } from "../secondary-named-contact-v1.js";

export const GDI_CONTACT_INTELLIGENCE_CROSS_WEEK_REUSE_V1 =
  "gdi-contact-intelligence-cross-week-reuse-v1";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

/**
 * Resolve reusable contact intelligence for a named person.
 * Prefers process-local cache, then durable fixture store when provided.
 */
export function resolveReusableContactIntelligence(args = {}) {
  const name = text(args.name);
  const organization = text(args.organization);
  const personKey =
    text(args.personIdentityKey) ||
    publicContactPersonKey({ name, organization });

  const result = {
    version: GDI_CONTACT_INTELLIGENCE_CROSS_WEEK_REUSE_V1,
    personIdentityKey: personKey,
    layer: REUSE_LAYER.NONE,
    ok: false,
    reason: null,
    found: null,
    durableFresh: null,
    processLocalHit: false,
    durableHit: false,
  };

  if (!personKey || !looksLikePersonName(name)) {
    result.reason = "NO_VERIFIED_NAMED_PERSON";
    return result;
  }

  const cached = getCachedPublicContactResearch(personKey);
  if (cached?.found && (cached.found.email || cached.found.phone)) {
    result.ok = true;
    result.layer = REUSE_LAYER.PROCESS_LOCAL_CACHE;
    result.processLocalHit = true;
    result.found = cached.found;
    result.fromHotelId = cached.hotelId || null;
    return result;
  }

  const store = args.durableStore || null;
  if (store) {
    const person = store.getPerson(personKey);
    if (!person) {
      result.reason = "DURABLE_MISS";
      return result;
    }
    if (person.invalid) {
      result.reason = person.invalidReason || "DURABLE_INVALID";
      return result;
    }
    const fresh = store.isFresh(person, args.asOf || new Date());
    result.durableFresh = fresh;
    if (!fresh) {
      result.reason = "EVIDENCE_STALE";
      return result;
    }
    if (!person.rights?.permissionToRetain) {
      result.reason = "RETENTION_NOT_PERMITTED";
      return result;
    }
    const email =
      person.channels?.[CHANNEL_KIND.VERIFIED_DIRECT_WORK_EMAIL]?.value || null;
    const phone =
      person.channels?.[CHANNEL_KIND.VERIFIED_DIRECT_BUSINESS_PHONE]?.value ||
      null;
    if (!email && !phone) {
      result.reason = "DURABLE_NO_CHANNELS";
      return result;
    }
    result.ok = true;
    result.layer = REUSE_LAYER.DURABLE_FIXTURE_STORE;
    result.durableHit = true;
    result.found = {
      email,
      phone,
      sourceUrl: person.evidence?.sourceUrl || null,
      verificationTimestamp: person.evidence?.verifiedAt || null,
      organizationEmail:
        person.channels?.[CHANNEL_KIND.VERIFIED_ORGANIZATION_EMAIL_ROUTE]
          ?.value || null,
      organizationPhone:
        person.channels?.[CHANNEL_KIND.VERIFIED_ORGANIZATION_PHONE_ROUTE]
          ?.value || null,
    };
    // Optionally hydrate process cache for rest of this run
    if (args.hydrateProcessCache !== false) {
      putCachedPublicContactResearch(personKey, {
        hotelId: (person.associations?.hotelIds || [])[0] || null,
        opportunityId: (person.associations?.opportunityIds || [])[0] || null,
        found: result.found,
        reuseLayer: REUSE_LAYER.DURABLE_FIXTURE_STORE,
      });
    }
    return result;
  }

  result.reason = "NO_REUSE_SOURCE";
  return result;
}

/**
 * Persist verified public channels into durable fixture store (not Airtable).
 */
export function persistVerifiedContactToDurableStore(args = {}) {
  const store = args.durableStore;
  if (!store) return { ok: false, reason: "NO_DURABLE_STORE" };
  return store.upsertPerson({
    name: args.name,
    organization: args.organization,
    personIdentityKey: args.personIdentityKey,
    role: args.role,
    organizationDomain: args.organizationDomain,
    sourceUrl: args.sourceUrl,
    origin: args.origin || "PUBLIC_SOURCE",
    verifiedAt: args.verifiedAt || new Date().toISOString(),
    permissionToRetain: args.permissionToRetain === true,
    permissionToDisplayToHotelClients:
      args.permissionToDisplayToHotelClients === true,
    retentionPolicyDays: args.retentionPolicyDays || 365,
    revalidationRequiredAfterDays: args.revalidationRequiredAfterDays || 365,
    opportunityIds: args.opportunityIds || [],
    hotelIds: args.hotelIds || [],
    channels: args.channels || {},
  });
}

export function createCrossWeekReuseHarness(opts = {}) {
  const store =
    opts.durableStore ||
    createDurableContactStore({
      storePath: opts.storePath || null,
      persist: opts.persist !== false,
    });
  return {
    store,
    resolve: (args) =>
      resolveReusableContactIntelligence({ ...args, durableStore: store }),
    persistVerified: (args) =>
      persistVerifiedContactToDurableStore({ ...args, durableStore: store }),
  };
}

export default {
  GDI_CONTACT_INTELLIGENCE_CROSS_WEEK_REUSE_V1,
  resolveReusableContactIntelligence,
  persistVerifiedContactToDurableStore,
  createCrossWeekReuseHarness,
};
