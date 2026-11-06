/**
 * Shared canonical discovery candidate schema for Apify + Exa → one pool.
 */

import { buildDedupeKey } from "../research/apify/apify-source-normalize-v1.js";
import {
  dedupeApifyAgainstInventory,
  commercialRelevanceScreen,
} from "../research/apify/apify-cross-provider-dedup-v1.js";

export const UNIFIED_CANDIDATE_SCHEMA_V7 = "gdi-unified-discovery-candidate-v7";

/**
 * Normalize any provider discovery hit into the shared schema.
 * Missing fields stay null — never silently discard.
 */
export function toUnifiedDiscoveryCandidate(raw = {}, ctx = {}) {
  const provider = String(ctx.provider || raw.discoveryProvider || raw.provider || "unknown");
  const title = String(raw.title || raw.name || raw.eventName || "").trim();
  const rawOrg = raw.organizationName || raw.organizerName || raw.org || null;
  // Never promote String(object) leakage from Stage 1 Eventbrite organizer objects
  const organizationName =
    rawOrg && typeof rawOrg === "object"
      ? rawOrg.name || rawOrg.organizationName || null
      : rawOrg === "[object Object]"
        ? null
        : rawOrg;
  const startDate = raw.startDate || raw.start || raw.dateFrom || null;
  const endDate = raw.endDate || raw.end || raw.dateTo || null;
  const sourceUrl = raw.sourceUrl || raw.url || raw.link || null;
  const city = raw.city || ctx.hotel?.city || null;

  const candidate = {
    schemaVersion: UNIFIED_CANDIDATE_SCHEMA_V7,
    rawSourceId: raw.id ?? raw.rawRef?.index ?? null,
    provider,
    actorOrRequestId: raw.provenance?.actorId || raw.actorId || raw.exaRequestId || ctx.requestId || null,
    sourceUrl,
    sourceTitle: title || null,
    organizationName,
    organizerId: raw.organizerId || null,
    organizerUrl: raw.organizerUrl || null,
    eventOrProgrammeIdentity: raw.eventOrProgrammeIdentity || title || null,
    startDate,
    endDate,
    location: raw.venueName || raw.location || city,
    city,
    demandMechanism: raw.demandMechanism || raw.recordKind || null,
    researchTrack: raw.relationshipTrackOnly
      ? "ACCOUNT_RELATIONSHIP_DEMAND"
      : startDate
        ? "DATE_SPECIFIC_DEMAND"
        : "ACCOUNT_RELATIONSHIP_DEMAND",
    buyerCandidates: raw.buyerCandidates || [],
    evidence: raw.evidence || [],
    providerTimestamp: raw.provenance?.retrievedAt || new Date().toISOString(),
    researchVersion: ctx.researchVersion || UNIFIED_CANDIDATE_SCHEMA_V7,
    commercialConfidence: raw.commercialConfidence || null,
    accessibilityClass: null,
    commercialScreen: raw.commercialScreen || "PENDING_ORS",
    requiresOrs: true,
    timingStatus: raw.timingStatus || (startDate ? "CANDIDATE_DATED_UNVERIFIED" : "TIMING_UNCONFIRMED"),
    provenance: {
      provider,
      ...(raw.provenance || {}),
      supportingSources: raw.supportingSources || [],
    },
    rawArtifactRef: raw.rawRef || { preserved: true },
    dedupeKey:
      raw.dedupeKey ||
      buildDedupeKey({
        title,
        org: organizationName,
        startDate,
        url: sourceUrl,
        city,
      }),
  };
  return candidate;
}

/**
 * Merge Apify + Exa discoveries into one pool with inventory dedup.
 */
export function buildSharedCandidatePoolV7({
  apifyCandidates = [],
  exaCandidates = [],
  inventory = {},
} = {}) {
  const unifiedApify = apifyCandidates.map((c) =>
    toUnifiedDiscoveryCandidate(c, { provider: "apify" }),
  );
  const unifiedExa = exaCandidates.map((c) =>
    toUnifiedDiscoveryCandidate(c, { provider: "exa" }),
  );

  // First fold Exa into inventory-shaped pool, then Apify against combined
  const exaAsInventory = {
    exa: unifiedExa,
    gdi: inventory.gdi || [],
    priorApify: inventory.priorApify || [],
    continuation: inventory.continuation || [],
  };

  const apifyDedup = dedupeApifyAgainstInventory(unifiedApify, exaAsInventory);

  // Exa-only net-new (not matched by Apify corroboration keys in this batch)
  const apifyKeys = new Set(
    [...apifyDedup.uniqueCandidates, ...apifyDedup.mergedIntoExisting.map((m) => m.merged)].map(
      (c) => c.dedupeKey,
    ),
  );
  const exaOnly = unifiedExa.filter((e) => !apifyKeys.has(e.dedupeKey));

  const screened = [
    ...apifyDedup.uniqueCandidates.map(commercialRelevanceScreen),
    ...exaOnly.map((c) => commercialRelevanceScreen({ ...c, discoveryProvider: "exa" })),
  ];

  return {
    version: "gdi-unified-shared-candidate-pool-v7",
    pool: screened,
    metrics: {
      apifyRaw: unifiedApify.length,
      exaRaw: unifiedExa.length,
      apifyNetNew: apifyDedup.netNewCount,
      apifyCorroboration: apifyDedup.corroborationCount,
      exaOnlyCount: exaOnly.length,
      bothProviders: apifyDedup.corroborationCount,
      sharedPoolSize: screened.length,
    },
    dedup: apifyDedup,
  };
}
