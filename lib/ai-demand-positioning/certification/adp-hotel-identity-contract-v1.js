/**
 * Global AdpHotelIdentityContract — centrally governed identity for every ADP hotel.
 * Pre-flight + canary. No hotel-specific certification bypasses.
 */

import crypto from "crypto";
import { readdirSync, readFileSync, existsSync } from "fs";
import { join } from "path";
import { detectPropertyMention } from "../execution/response-parser.js";
import { loadPropertyProfile } from "../data-model.js";

export const ADP_HOTEL_IDENTITY_CONTRACT_VERSION = "adp_hotel_identity_contract_v1";

export const IDENTITY_PREFLIGHT_OUTCOMES = Object.freeze({
  IDENTITY_PASS: "IDENTITY_PASS",
  IDENTITY_REVIEW: "IDENTITY_REVIEW",
  IDENTITY_FAIL: "IDENTITY_FAIL",
});

function sha16(value) {
  return crypto.createHash("sha256").update(String(value || "")).digest("hex").slice(0, 16);
}

function listAllProfileFiles() {
  const fixturesDir = join(process.cwd(), "fixtures/ai-demand-positioning");
  if (!existsSync(fixturesDir)) return [];
  return readdirSync(fixturesDir).filter((f) => f.endsWith("-property-profile.json"));
}

export function listAllAdpPropertyProfiles() {
  const fixturesDir = join(process.cwd(), "fixtures/ai-demand-positioning");
  const out = [];
  for (const file of listAllProfileFiles()) {
    try {
      const data = JSON.parse(readFileSync(join(fixturesDir, file), "utf8"));
      if (data?.propertyId && data?.name) out.push(data);
    } catch {
      /* skip corrupt */
    }
  }
  return out.sort((a, b) => String(a.propertyId).localeCompare(String(b.propertyId)));
}

/**
 * Build canonical identity contract from a property profile (+ optional overrides).
 */
export function buildAdpHotelIdentityContract(propertyProfile, options = {}) {
  const p = propertyProfile || {};
  const aliases = [
    ...(p.identityAliases || []),
    ...(p.aliases || []),
    ...(options.extraAliases || []),
  ]
    .map((a) => String(a || "").trim())
    .filter(Boolean);
  const uniqueAliases = [...new Set(aliases)];
  const ownedDomains = [
    p.canonicalPropertyDomain,
    ...(p.ownedDomains || []),
    ...(p.additionalApprovedOwnedDomains || []),
  ]
    .map((d) =>
      String(d || "")
        .replace(/^https?:\/\//i, "")
        .replace(/^www\./i, "")
        .split("/")[0]
        .trim()
        .toLowerCase()
    )
    .filter(Boolean);

  const contract = {
    identityContractVersion: ADP_HOTEL_IDENTITY_CONTRACT_VERSION,
    propertyId: p.propertyId || null,
    subjectId: p.subjectId || p.censusRecordId || p.propertyId || null,
    canonicalName: p.name || null,
    brand: p.brand || p.affiliation || null,
    parentCompany: p.parentCompany || null,
    officialDomain: p.officialBrandDomain || ownedDomains[0] || null,
    officialPropertyUrl:
      p.officialPropertyPageUrl || p.website || p.officialWebsite || null,
    address: p.address || null,
    city: p.city || null,
    state: p.state || null,
    country: p.country || null,
    market: p.market || null,
    submarket: p.submarket || null,
    lat: p.lat ?? p.latitude ?? null,
    lng: p.lng ?? p.longitude ?? null,
    aliases: uniqueAliases,
    formerNames: [...(p.formerNames || [])],
    commonShorthand: [...(p.commonShorthand || [])],
    brandShorthand: [...(p.brandShorthand || [])],
    neighborhoodAliases: [...(p.neighborhoodAliases || [])],
    propertyEntityId: p.propertyEntityId || p.entityId || p.censusRecordId || null,
    portfolioLoyaltyIdentity: p.loyaltyProgram || p.portfolioLoyaltyIdentity || null,
    ownedDomains: [...new Set(ownedDomains)],
    confusableExclusions: [...(p.identityConfusableExclusions || [])],
  };

  contract.aliasSetHash = sha16(contract.aliases.slice().sort().join("|"));
  contract.ownedDomainSetHash = sha16(contract.ownedDomains.slice().sort().join("|"));
  return contract;
}

function normalizeAliasKey(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Detect alias collisions across the global ADP hotel universe.
 */
export function findGlobalAliasCollisions(propertyId, aliases, allProfiles = null) {
  const profiles = allProfiles || listAllAdpPropertyProfiles();
  const needles = new Set((aliases || []).map(normalizeAliasKey).filter((a) => a.length >= 4));
  const collisions = [];
  for (const other of profiles) {
    if (other.propertyId === propertyId) continue;
    const otherAliases = [
      other.name,
      ...(other.identityAliases || []),
      ...(other.aliases || []),
    ].map(normalizeAliasKey);
    for (const a of otherAliases) {
      if (needles.has(a)) {
        collisions.push({
          alias: a,
          otherPropertyId: other.propertyId,
          otherName: other.name,
        });
      }
    }
  }
  return collisions;
}

/**
 * Identity pre-flight before provider execution (Part 5).
 */
export function validateAdpHotelIdentityContract(propertyProfile, options = {}) {
  const contract = buildAdpHotelIdentityContract(propertyProfile, options);
  const hardFailures = [];
  const reviewFlags = [];
  const warnings = [];

  if (!contract.canonicalName) hardFailures.push({ code: "MISSING_CANONICAL_NAME" });
  if (!contract.aliases.length) hardFailures.push({ code: "NO_ALIASES" });
  if (!contract.officialDomain && !contract.ownedDomains.length) {
    hardFailures.push({ code: "NO_OFFICIAL_DOMAIN" });
  }
  if (!contract.officialPropertyUrl) {
    hardFailures.push({ code: "MISSING_CANONICAL_PROPERTY_URL" });
  }
  if (!contract.brand) reviewFlags.push({ code: "MISSING_BRAND" });
  if (!contract.market && !contract.city) reviewFlags.push({ code: "MISSING_MARKET" });

  const collisions = findGlobalAliasCollisions(
    contract.propertyId,
    contract.aliases,
    options.allProfiles || null
  );
  if (collisions.length) {
    hardFailures.push({ code: "ALIAS_COLLISION", details: collisions.slice(0, 8) });
  }

  // Entity ID collision
  if (contract.propertyEntityId && options.allProfiles) {
    const entityHits = options.allProfiles.filter(
      (p) =>
        p.propertyId !== contract.propertyId &&
        (p.propertyEntityId === contract.propertyEntityId ||
          p.censusRecordId === contract.propertyEntityId ||
          p.entityId === contract.propertyEntityId)
    );
    if (entityHits.length) {
      hardFailures.push({
        code: "ENTITY_ID_COLLISION",
        details: entityHits.map((p) => p.propertyId),
      });
    }
  }

  // Brand mismatch: officialBrandDomain vs brand registry expectation is soft.
  // Independents / property-owned domains (canonicalPropertyDomain / ownedDomains) are not brand hosts.
  const brandNorm = String(contract.brand || "").toLowerCase();
  const isIndependent =
    !brandNorm ||
    brandNorm === "independent" ||
    brandNorm === "indie" ||
    brandNorm.includes("independent");
  const propertyOwnedHosts = new Set(contract.ownedDomains || []);
  const officialIsPropertyDomain =
    contract.officialDomain && propertyOwnedHosts.has(String(contract.officialDomain).toLowerCase());
  if (
    contract.officialDomain &&
    contract.brand &&
    !isIndependent &&
    !officialIsPropertyDomain &&
    !String(contract.officialDomain).toLowerCase().includes(
      String(contract.brand).toLowerCase().split(/\s+/)[0].slice(0, 4)
    ) &&
    !String(contract.parentCompany || "")
      .toLowerCase()
      .includes(String(contract.officialDomain).split(".")[0])
  ) {
    // only flag when domain token and brand share no prefix — e.g. marriott.com + Hilton
    const domainToken = String(contract.officialDomain).split(".")[0];
    const brandBlob = `${contract.brand} ${contract.parentCompany || ""}`.toLowerCase();
    if (domainToken.length >= 4 && !brandBlob.includes(domainToken.slice(0, 4))) {
      reviewFlags.push({
        code: "BRAND_DOMAIN_MISMATCH_SUSPECT",
        detail: { officialDomain: contract.officialDomain, brand: contract.brand },
      });
    }
  }

  let outcome = IDENTITY_PREFLIGHT_OUTCOMES.IDENTITY_PASS;
  if (hardFailures.length) outcome = IDENTITY_PREFLIGHT_OUTCOMES.IDENTITY_FAIL;
  else if (reviewFlags.length) outcome = IDENTITY_PREFLIGHT_OUTCOMES.IDENTITY_REVIEW;

  return {
    outcome,
    contract,
    hardFailures,
    reviewFlags,
    warnings,
  };
}

/**
 * Default canary phrases for any hotel (Part 6).
 * Only phrases that are contractually expected valid forms — never invent
 * brand+location strings that are not on the identity contract (false hard-fails).
 */
export function buildIdentityCanaries(propertyProfile) {
  const name = String(propertyProfile?.name || "").trim();
  const aliases = [...(propertyProfile?.identityAliases || []), ...(propertyProfile?.aliases || [])]
    .map((a) => String(a || "").trim())
    .filter(Boolean);
  const commonShorthand = [...(propertyProfile?.commonShorthand || [])]
    .map((a) => String(a || "").trim())
    .filter(Boolean);
  const brandShorthand = [...(propertyProfile?.brandShorthand || [])]
    .map((a) => String(a || "").trim())
    .filter(Boolean);
  const neighborhoodAliases = [...(propertyProfile?.neighborhoodAliases || [])]
    .map((a) => String(a || "").trim())
    .filter(Boolean);
  const canaries = [];

  if (name) canaries.push({ kind: "CANONICAL_FULL_NAME", phrase: name });
  if (aliases[0]) canaries.push({ kind: "COMMON_SHORT_NAME", phrase: aliases[0] });
  if (aliases[1]) canaries.push({ kind: "KNOWN_ALTERNATE_NAME", phrase: aliases[1] });
  if (commonShorthand[0]) {
    canaries.push({ kind: "COMMON_SHORTHAND", phrase: commonShorthand[0] });
  }
  // Brand + location shorthand only when explicitly governed on the contract
  if (brandShorthand[0] && neighborhoodAliases[0]) {
    canaries.push({
      kind: "BRAND_LOCATION_SHORTHAND",
      phrase: `${brandShorthand[0]} ${neighborhoodAliases[0]}`.replace(/\s+/g, " ").trim(),
    });
  } else if (brandShorthand[0]) {
    canaries.push({ kind: "BRAND_SHORTHAND", phrase: brandShorthand[0] });
  }

  for (const c of propertyProfile?.identityCanaries || []) {
    canaries.push(typeof c === "string" ? { kind: "PROFILE_CANARY", phrase: c } : c);
  }

  // Deduplicate by normalized phrase
  const seen = new Set();
  return canaries.filter((c) => {
    const k = normalizeAliasKey(c.phrase);
    if (!k || k.length < 4 || seen.has(k)) return false;
    seen.add(k);
    return true;
  });
}

/**
 * Run identity canaries against detectPropertyMention.
 */
export function runIdentityCanaries(propertyProfile, options = {}) {
  const canaries = options.canaries || buildIdentityCanaries(propertyProfile);
  const results = [];
  for (const c of canaries) {
    const text = `Guests often choose ${c.phrase} for this stay.`;
    const hit = detectPropertyMention(text, propertyProfile);
    results.push({
      kind: c.kind,
      phrase: c.phrase,
      recognized: Boolean(hit.mentioned),
      matchedVariant: hit.matchedVariant || null,
    });
  }
  const failed = results.filter((r) => !r.recognized);
  return {
    pass: failed.length === 0,
    canaries: results,
    failed,
  };
}

/**
 * Synthetic regression: prove missing critical alias fails canary (Hilton lesson).
 * Does not mutate profiles.
 */
export function assertMissingAliasWouldFailCanary(propertyId, criticalAlias) {
  const profile = loadPropertyProfile(propertyId);
  if (!profile) {
    return { ok: false, reason: "profile_not_found", propertyId };
  }
  const stripped = {
    ...profile,
    identityAliases: (profile.identityAliases || []).filter(
      (a) => normalizeAliasKey(a) !== normalizeAliasKey(criticalAlias)
    ),
    aliases: (profile.aliases || []).filter(
      (a) => normalizeAliasKey(a) !== normalizeAliasKey(criticalAlias)
    ),
  };
  // Ensure name alone does not already equal the critical alias
  const text = `Guests often choose ${criticalAlias} for this stay.`;
  const withAlias = detectPropertyMention(text, profile);
  const withoutAlias = detectPropertyMention(text, stripped);
  return {
    ok: true,
    propertyId,
    criticalAlias,
    recognizedWithAlias: Boolean(withAlias.mentioned),
    recognizedWithoutAlias: Boolean(withoutAlias.mentioned),
    /** Framework catches the bug when removing the alias causes a miss. */
    missingAliasWouldFailCanary:
      Boolean(withAlias.mentioned) && !Boolean(withoutAlias.mentioned),
  };
}
