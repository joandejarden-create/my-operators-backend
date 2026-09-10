/**
 * ADP_FULL_PROPERTY_UNIVERSE_DISCOVERY — resolve every governed Existing Hotel ADP property.
 * Authoritative order: published manifests ∩ fixture profiles (no manual cohort lists).
 */

import { existsSync, readFileSync } from "fs";
import { join } from "path";
import {
  listPublishedPropertyIds,
  loadPublishedManifest,
  loadPublishedReport,
} from "../published-snapshot.js";
import {
  loadPropertyProfile,
  loadPeriod,
  loadAllPeriods,
  listPropertyProfiles,
} from "../data-model.js";
import { resolveStandardScenarioMarket } from "../prompt-universe/scenario-registry.js";
import { getEntityRegistryForProperty } from "../metrics/adp-property-entity-registries.js";
import { EXECUTIVE_METRICS_VERSION } from "../metrics/executive-metrics-foundation.js";
import { CANONICAL_SUBJECT_RESOLVER_VERSION } from "../subject-presence/canonical-subject-presence-v1.js";
import { resolveCanonicalComparablePeriods } from "../metrics/canonical-comparable-period-resolver-v1.js";
import { discoverAdpPropertyIds } from "../multi-property-governed-audit-v2.js";

export const ADP_FULL_PROPERTY_UNIVERSE_DISCOVERY = "ADP_FULL_PROPERTY_UNIVERSE_DISCOVERY";
export const ADP_NO_SILENT_PROPERTY_EXCLUSION = "ADP_NO_SILENT_PROPERTY_EXCLUSION";

export const PROPERTY_AUDIT_STATUS = Object.freeze({
  AUDITED_PASS: "AUDITED_PASS",
  AUDITED_FAIL: "AUDITED_FAIL",
  BLOCKED_MISSING_ARTIFACT: "BLOCKED_MISSING_ARTIFACT",
  BLOCKED_INCOMPATIBLE_VERSION: "BLOCKED_INCOMPATIBLE_VERSION",
  NOT_APPLICABLE_WITH_REASON: "NOT_APPLICABLE_WITH_REASON",
});

export const PROPERTY_CLEANLINESS = Object.freeze({
  CLEAN: "CLEAN",
  BLOCKED: "BLOCKED",
});

const SHARE_REGISTRY_CANDIDATES = [
  join(process.cwd(), "config/client-share/adp-share-registry/active-tokens.json"),
  join(process.cwd(), "data/ai-demand-positioning/share-registry/active-tokens.json"),
];

function loadShareRegistry() {
  for (const p of SHARE_REGISTRY_CANDIDATES) {
    if (!existsSync(p)) continue;
    try {
      return JSON.parse(readFileSync(p, "utf8"));
    } catch {
      /* try next */
    }
  }
  return { tokens: {} };
}

function shareStatusForProperty(propertyId, registry) {
  const raw = registry?.tokens;
  const list = Array.isArray(raw)
    ? raw
    : raw && typeof raw === "object"
      ? Object.values(raw)
      : [];
  const tokens = list.filter((t) => t?.propertyId === propertyId);
  if (!tokens.length) {
    return { shareStatus: "NO_ACTIVE_TOKEN", activeTokenCount: 0, tokens: [] };
  }
  const active = tokens.filter((t) => String(t.status || "").toUpperCase() === "ACTIVE");
  return {
    shareStatus: active.length ? "ACTIVE_TOKEN_EXISTS" : "TOKENS_REVOKED_OR_EXPIRED",
    activeTokenCount: active.length,
    tokens: tokens.map((t) => ({
      tokenId: t.tokenId,
      status: t.status,
      reportScope: t.reportScope,
      issuedAt: t.issuedAt,
      expiresAt: t.expiresAt,
    })),
  };
}

function scenarioPackVersion(profile, period) {
  if (period?.scenarioUniverseVersion) return period.scenarioUniverseVersion;
  try {
    const market = resolveStandardScenarioMarket(profile);
    return `adp_scenario_universe_v1:${market || "unknown"}`;
  } catch {
    return "adp_scenario_universe_v1:unknown";
  }
}

function entityResolverVersion(propertyId) {
  try {
    const reg = getEntityRegistryForProperty?.(propertyId);
    if (reg?.version) return reg.version;
  } catch {
    /* fall through */
  }
  return CANONICAL_SUBJECT_RESOLVER_VERSION;
}

function previousComparablePeriod(propertyId, currentPeriodId) {
  try {
    const all = loadAllPeriods(propertyId);
    const resolution = resolveCanonicalComparablePeriods({
      allPeriods: all,
      currentPeriodId,
    });
    const prior =
      resolution?.priorComparablePeriodId ||
      resolution?.priorPeriodId ||
      resolution?.comparablePeriods?.[1]?.periodId ||
      null;
    return prior;
  } catch {
    return null;
  }
}

/**
 * Resolve one property row for the governed universe.
 */
export function resolveGovernedAdpPropertyEntryV1(propertyId) {
  const profile = loadPropertyProfile(propertyId);
  const manifest = loadPublishedManifest(propertyId);
  const report = loadPublishedReport(propertyId);
  const periodId = manifest?.latestPeriodId || null;
  const period = periodId ? loadPeriod(periodId) : null;
  const shareRegistry = loadShareRegistry();
  const share = shareStatusForProperty(propertyId, shareRegistry);

  const market =
    profile?.market ||
    manifest?.market ||
    (profile ? (() => {
      try {
        return resolveStandardScenarioMarket(profile);
      } catch {
        return null;
      }
    })() : null);

  const certified =
    Boolean(manifest?.certified) ||
    ["CERTIFIED", "CERTIFIED_WITH_DISCLOSURES"].includes(
      String(manifest?.certificationStatus || "")
    );

  const blockers = [];
  if (!profile) blockers.push("MISSING_PROFILE");
  if (!manifest) blockers.push("MISSING_PUBLISHED_MANIFEST");
  if (!report) blockers.push("MISSING_PUBLISHED_REPORT");
  if (!periodId) blockers.push("MISSING_PERIOD_ID");
  if (periodId && !period) blockers.push("MISSING_RUNTIME_PERIOD");

  return {
    propertyId,
    canonicalPropertyName: profile?.name || manifest?.propertyName || null,
    market,
    city: profile?.city || manifest?.city || null,
    state: profile?.state || manifest?.state || null,
    currentCertifiedPeriodId: periodId,
    previousComparablePeriodId: periodId
      ? previousComparablePeriod(propertyId, periodId)
      : null,
    customerPublishedStatus: manifest?.publishStatus || (manifest ? "UNKNOWN" : "NOT_PUBLISHED"),
    customerDropdownVisible: profile?.customerDropdownVisible !== false,
    certificationStatus: manifest?.certificationStatus || (manifest?.certified ? "CERTIFIED" : null),
    certified,
    ...share,
    measurementContractVersion:
      manifest?.measurementContractVersion ||
      period?.measurementContractVersion ||
      report?.measurementContractVersion ||
      null,
    scenarioPackVersion: profile ? scenarioPackVersion(profile, period) : null,
    entityResolverVersion: entityResolverVersion(propertyId),
    metricCalculatorVersion: EXECUTIVE_METRICS_VERSION,
    subjectPresenceResolverVersion: CANONICAL_SUBJECT_RESOLVER_VERSION,
    observationCount: period?.observations?.length || 0,
    artifactBlockers: blockers,
    discoverySources: {
      published: Boolean(manifest),
      profile: Boolean(profile),
      runtimePeriod: Boolean(period),
    },
  };
}

/**
 * Full governed Existing Hotel ADP universe.
 * Primary membership = published property directories (live product surface).
 * Discovery also records operational orphans (profile/runtime without publish).
 */
export function resolveGovernedAdpPropertyUniverseV1(options = {}) {
  const publishedIds = [...listPublishedPropertyIds()].filter((id) =>
    String(id).startsWith("adp_")
  );
  const profileIds = listPropertyProfiles()
    .map((p) => p.propertyId)
    .filter(Boolean);
  const discovered = discoverAdpPropertyIds();

  const primaryIds = [...new Set(publishedIds)].sort();
  const properties = primaryIds.map((id) => resolveGovernedAdpPropertyEntryV1(id));

  const orphanIds = discovered.filter((id) => !primaryIds.includes(id));
  const orphans = orphanIds.map((id) => ({
    ...resolveGovernedAdpPropertyEntryV1(id),
    orphanReason: "DISCOVERED_BUT_NOT_IN_PUBLISHED_UNIVERSE",
  }));

  const intendedCustomerFacing = properties.filter(
    (p) =>
      p.customerPublishedStatus === "Live" ||
      p.certified ||
      p.customerDropdownVisible
  );

  return {
    gate: ADP_FULL_PROPERTY_UNIVERSE_DISCOVERY,
    version: "adp_governed_property_universe_v1",
    discoveredAt: new Date().toISOString(),
    liveProviderCalls: 0,
    counts: {
      published: primaryIds.length,
      profiles: profileIds.length,
      discoveredUnion: discovered.length,
      intendedCustomerFacing: intendedCustomerFacing.length,
      orphans: orphans.length,
    },
    propertyIds: primaryIds,
    properties,
    intendedCustomerFacingPropertyIds: intendedCustomerFacing.map((p) => p.propertyId),
    orphans,
    noSilentExclusionGate: ADP_NO_SILENT_PROPERTY_EXCLUSION,
    note:
      "Primary universe = data/ai-demand-positioning/published/* ∩ fixture profiles. Cohort constants are not used as membership.",
  };
}
