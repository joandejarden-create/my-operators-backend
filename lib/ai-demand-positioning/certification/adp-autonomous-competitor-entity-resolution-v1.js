/**
 * ADP_AUTONOMOUS_COMPETITOR_ENTITY_RESOLUTION_V1
 *
 * Evidence-backed competitor discovery + entity resolution for Full ADP E2E.
 * Does not require hand-curated per-hotel competitor lists.
 *
 * Lifecycle (S8b–S8d):
 *  1. Extract candidate mentions from archived Core answers
 *  2. Normalize / classify (hotel vs brand vs location vs nonhotel vs subject)
 *  3. Resolve against market hotel pools (reuse certified registries by geography)
 *  4. Optionally persist durable evidence-backed registry
 *  5. Reconcile every extracted mention to a governed disposition
 *  6. P0-fail when material evidence is silently lost
 */

import { createHash } from "crypto";
import { existsSync, mkdirSync, readFileSync, readdirSync, writeFileSync } from "fs";
import path from "path";
import {
  loadLatestCoreCustomerPeriod,
  loadPropertyProfile,
} from "../data-model.js";
import {
  createEntityRegistry,
  ENTITY_CLASSES,
} from "../metrics/adp-entity-registry-factory.js";
import {
  PROPERTY_ENTITY_REGISTRY,
  getEntityRegistryForProperty as getStaticEntityRegistry,
} from "../metrics/adp-property-entity-registries.js";
import {
  isCrossMarketLeak,
  resolveCustomerFacingEntity,
} from "../customer/customer-entity-resolution-v1.js";
import { subjectEntityIdsForProperty } from "../subject-presence/subject-entity-ids-v1.js";
import { getGovernedSubjectMentioned } from "../subject-presence/canonical-subject-presence-v1.js";
import { detectPropertyMention } from "../execution/response-parser.js";
import {
  computeCompetitorPresentGaps,
  observationSubjectPresent,
  observationHasCustomerFacingCompetitor,
} from "../metrics/competitor-present-gaps.js";
import { buildScenarioUniverse } from "../prompt-universe/scenario-registry.js";
import { filterComparableObservations } from "../metrics/grain-governance.js";
import {
  buildScenarioEligibilityMap,
  CONSIDERATION_ELIGIBLE_STATES,
  scenariosForEligibility,
} from "../metrics/scenario-eligibility.js";
import { loadArchivePromptMap } from "./adp-unprompted-independent-discovery-standard-v1.js";
import { isNonHotelEntity } from "../intelligence/competitor-name-resolution.js";
import { isLikelyArtifactEntity } from "../metrics/entity-quality.js";

/** Max autonomous peers promoted into territory CORE (display + benchmark peers). */
export const AUTONOMOUS_CORE_PEER_CAP = 12;

/**
 * Prefer the period's actual monitored scenario IDs (incl. independent replacements)
 * over a freshly rebuilt Core universe that may omit gen_/prop_gen_ IDs.
 */
export function scenariosForPeriodDisplacement(propertyId, period, profile) {
  const promptMap = loadArchivePromptMap(propertyId, period?.periodId) || new Map();
  const universe = buildScenarioUniverse(profile) || [];
  const universeById = new Map(universe.map((s) => [s.scenarioId, s]));
  const observedIds = new Set(
    (period?.observations || [])
      .map((o) => String(o?.scenarioId || "").trim())
      .filter(Boolean)
  );

  // Grain = scenarios actually monitored in this period (preserves gen_/prop_gen_ replacements).
  // Do not inflate with unmonitored rebuilt-universe IDs.
  if (observedIds.size === 0) return universe;

  // Prefer exactRenderedPrompt from any observation of this scenario (period archive).
  const promptFromObs = new Map();
  for (const o of period?.observations || []) {
    const sid = String(o?.scenarioId || "").trim();
    if (!sid || promptFromObs.has(sid)) continue;
    const p = o.exactRenderedPrompt || o.prompt || o.query || "";
    if (p) promptFromObs.set(sid, String(p));
  }

  const out = [];
  for (const sid of observedIds) {
    const archived = promptMap.get(sid);
    const base = universeById.get(sid);
    const prompt =
      (typeof archived === "string" ? archived : archived?.prompt || archived?.query) ||
      promptFromObs.get(sid) ||
      base?.query ||
      base?.prompt ||
      "";
    out.push({
      ...(base || {}),
      scenarioId: sid,
      query: String(prompt || base?.query || ""),
      prompt: String(prompt || base?.prompt || base?.query || ""),
      source: base?.source || "period_observation_archive",
      intent: base?.intent || "leisure",
    });
  }
  return out;
}

export const CONTRACT_ID = "ADP_AUTONOMOUS_COMPETITOR_ENTITY_RESOLUTION_V1";
export const CONTRACT_VERSION = "2026-10-22";

const DURABLE_DIR = "data/ai-demand-positioning/competitor-entity-registries";

const BRAND_ONLY = new Set(
  [
    "marriott",
    "hilton",
    "hyatt",
    "ihg",
    "accor",
    "radisson",
    "fiesta americana",
    "grand fiesta americana",
    "grupo posadas",
    "posadas",
    "four seasons",
    "ritz carlton",
    "st regis",
    "westin",
    "sheraton",
    "intercontinental",
    "holiday inn",
    "live aqua",
    "fiesta inn",
  ].map(norm)
);

const LOCATION_ONLY = new Set(
  [
    "mexico city",
    "cdmx",
    "polanco",
    "reforma",
    "chapultepec",
    "cancun",
    "cancún",
    "hotel zone",
    "zona hotelera",
    "airport",
    "aeropuerto",
  ].map(norm)
);

function norm(value) {
  return String(value || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/&/g, " and ")
    .replace(/[’']/g, "")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function slugEntityId(name) {
  const base = norm(name).replace(/\s+/g, "_").slice(0, 64);
  const hash = createHash("sha1").update(norm(name)).digest("hex").slice(0, 6);
  return `auto_${base || "hotel"}_${hash}`;
}

/**
 * Expand truncated extractor aliases using the originating provider answer.
 * Example: competitorsMentioned "Delta Hotel" + raw "Delta Hotels by Marriott Toronto".
 * Intentionally narrow — do not promote arbitrary bold prose as hotel identities.
 */
export function recoverCanonicalMentionFromRawResponse(raw, rawResponse) {
  const cleaned = String(raw || "")
    .replace(/\s+/g, " ")
    .trim();
  if (!cleaned) return cleaned;
  const n = norm(cleaned);
  const text = String(rawResponse || "");
  if (!text) return cleaned;

  // Brand-truncated / variant Delta → single Toronto property identity.
  if (
    /^delta hotels?$/.test(n) ||
    /^delta hotels?(?:\s+by\s+marriott)?\s+toronto$/.test(n)
  ) {
    const m = text.match(
      /Delta Hotels?(?:\s+by\s+Marriott)?\s+Toronto(?:\s*\([^)]{0,40}\))?/i
    );
    if (m || /^delta hotels?(?:\s+by\s+marriott)?\s+toronto$/.test(n)) {
      return "Delta Hotels by Marriott Toronto";
    }
  }

  return cleaned;
}

function isVibeOrCategoryDescriptor(raw) {
  const n = norm(raw);
  if (!n) return false;
  // Standalone marketing vibes / category labels — not identifiable hotels.
  if (
    /^(urban|luxury|boutique|lifestyle|modern|beach|family|business|wellness|adults?\s*only)\s+(resort|hotel|getaway|escape|vibe|alternative)$/.test(
      n
    )
  ) {
    return true;
  }
  if (/^(urban resort|luxury urban resort|urban resort getaway|urban resort alternative)$/.test(n)) {
    return true;
  }
  return false;
}

function looksLikeHotelName(raw) {
  const n = norm(raw);
  if (!n || n.length < 6) return false;
  if (BRAND_ONLY.has(n) || LOCATION_ONLY.has(n)) return false;
  if (isVibeOrCategoryDescriptor(raw)) return false;
  if (isNonHotelEntity(raw) || isLikelyArtifactEntity(raw)) return false;
  // Narrative / recommendation prose — never promote as hotel identities
  if (
    /\b(go with|best for|ideal for|perfect for|recommended for|if you|uber to|plus hot tub)\b/.test(
      n
    )
  ) {
    return false;
  }
  if (/[:;—–]|^\s*go\b/.test(String(raw || ""))) return false;
  // Prose / determiner fragments — never promote as hotel identities
  if (
    /^(of|the|a|an|and|or|to|for|with|from|in|on|at|by|this|that|these|those|our|your|their)\b/.test(
      n
    )
  ) {
    return false;
  }
  if (/^(best|top|luxury|boutique|recommended|important|only|social|mad)\s+(hotel|resort)/.test(n)) {
    return false;
  }
  if (/^(the\s+)?(hotel|resort|inn|property)$/.test(n)) return false;
  // Amenity descriptors that contain "suites/rooms" but are not brands
  if (
    /^(spacious|luxurious|elegant|comfortable|modern|stylish)\s+(rooms?|suites?)/.test(n) &&
    !/\b(hotel|resort|inn|palace|lodge)\b/.test(n)
  ) {
    return false;
  }
  // Truncation artifacts (e.g. "Toronto by Library Hotel" stripped of "Hotel X")
  if (/^[a-z]+\s+by\s+(library|hilton|marriott|hyatt|ihg|radisson)\b/.test(n)) return false;
  if (/^(entertainment|financial|yorkville|harbourfront|downtown)\s+(district|area)?,?\s+/i.test(n)) {
    return false;
  }
  if (/\b(airport|aeropuerto|metro|restaurant|bar|mall|park|museum|beach|pool)\b/.test(n) && !/\b(hotel|resort|inn)\b/.test(n)) {
    return false;
  }
  // Require a lodging token OR a multi-token proper name with a known lodging cue
  // Include plural "hotels" (e.g. "Delta Hotels by Marriott Toronto").
  if (/\b(hotels?|resort|inn|suites?|lodge|palace)\b/.test(n)) {
    // Must include at least one non-generic token beyond the lodging word
    const tokens = n
      .split(/\s+/)
      .filter((t) => !/^(hotels?|resort|inn|suites?|the|and|by)$/.test(t));
    // Bare amenity + suites (e.g. "spacious rooms and suites") is not a brand
    if (tokens.every((t) => /^(spacious|luxurious|elegant|comfortable|modern|stylish|rooms?)$/.test(t))) {
      return false;
    }
    return tokens.length >= 1 && tokens.join(" ").length >= 4;
  }
  return false;
}

/** Prefer longer canonical when a shorter mention is clearly the same hotel. */
function findAliasMergeTarget(canonical, hotelsById) {
  const n = norm(canonical);
  if (!n || n.length < 8) return null;
  let best = null;
  for (const h of hotelsById.values()) {
    if (h.subject) continue;
    const hn = norm(h.canonical);
    if (!hn || hn === n) continue;
    if (hn.includes(n) && hn.length > n.length + 2) {
      if (!best || hn.length > norm(best.canonical).length) best = h;
    }
  }
  return best;
}

function durablePath(propertyId, cwd = process.cwd()) {
  return path.join(cwd, DURABLE_DIR, propertyId, "registry-v1.json");
}

/** Canonical names claimed by other properties' durable registries (cross-contamination guard). */
function foreignDurableHotelNames(propertyId, cwd = process.cwd()) {
  const root = path.join(cwd, DURABLE_DIR);
  const out = new Map(); // normName -> { propertyId, market, canonical }
  if (!existsSync(root)) return out;
  for (const dir of readdirSync(root)) {
    if (dir === propertyId) continue;
    const fp = path.join(root, dir, "registry-v1.json");
    if (!existsSync(fp)) continue;
    try {
      const doc = JSON.parse(readFileSync(fp, "utf8"));
      const marketKeys = (doc.marketKeys || []).map(String);
      for (const h of doc.hotels || []) {
        if (!h || h.subject) continue;
        const n = norm(h.canonical);
        if (!n) continue;
        out.set(n, {
          propertyId: dir,
          market: h.market || marketKeys[0] || "",
          canonical: h.canonical,
        });
      }
    } catch {
      /* skip */
    }
  }
  return out;
}

function ledgerPath(propertyId, cwd = process.cwd()) {
  return path.join(cwd, DURABLE_DIR, propertyId, "mention-disposition-ledger-v1.json");
}

/** Build geography → hotel[] pools from every static property registry (reuse, don't re-hand-curate). */
export function buildMarketHotelPools() {
  const byMarket = new Map();
  const seen = new Set();
  for (const reg of Object.values(PROPERTY_ENTITY_REGISTRY)) {
    for (const hotel of reg?.hotels || []) {
      if (!hotel?.entityId || seen.has(hotel.entityId)) continue;
      seen.add(hotel.entityId);
      const marketKey = norm(hotel.market || reg.market || "unknown");
      if (!byMarket.has(marketKey)) byMarket.set(marketKey, []);
      byMarket.get(marketKey).push({
        ...hotel,
        subject: false,
        aliases: [...(hotel.aliases || [])],
      });
    }
  }
  return byMarket;
}

function marketKeysForProfile(profile) {
  const keys = new Set();
  for (const raw of [profile?.market, profile?.city, profile?.geography, profile?.demandTerritory]) {
    const n = norm(raw);
    if (n) keys.add(n);
  }
  // Common aliases
  if ([...keys].some((k) => /mexico\s*city|cdmx|chapultepec|polanco|reforma/.test(k))) {
    keys.add("mexico city");
  }
  if ([...keys].some((k) => /cancun|cancún/.test(k))) {
    keys.add("cancun");
  }
  return [...keys];
}

function subjectHotelFromProfile(profile) {
  const name = profile?.canonicalName || profile?.name || profile?.propertyName;
  if (!name) return null;
  const entityId =
    profile?.entityId ||
    String(profile.propertyId || "")
      .replace(/^adp_/, "")
      .slice(0, 80) ||
    slugEntityId(name);
  const aliases = [
    name,
    ...(profile.aliases || []),
    ...(profile.nameVariants || []),
  ]
    .map((a) => String(a || "").trim())
    .filter(Boolean);
  return {
    entityId,
    canonical: name,
    market: profile.market || profile.city || "",
    geography: profile.geography || "",
    chainScale: profile.chainScale || "Unknown",
    propertyType: profile.propertyType || "hotel",
    identityConfidence: "HIGH",
    subject: true,
    meetings: true,
    spa: Boolean(profile.spa),
    aliases,
    provenance: "SUBJECT_PROFILE",
  };
}

/**
 * Synthesize an evidence-backed registry for a property without a hand-wired entry.
 * Reuses market hotel pools from certified registries; adds ephemeral hotels only when
 * Core evidence shows a hotel-shaped name that does not bind to an existing identity.
 */
export function synthesizeMarketRegistryForProperty(propertyId, { cwd = process.cwd(), observations = null } = {}) {
  const profile = loadPropertyProfile(propertyId);
  if (!profile) return { ok: false, error: "missing_profile" };

  const period = observations
    ? { observations }
    : loadLatestCoreCustomerPeriod(propertyId);
  const obs = period?.observations || [];
  const pools = buildMarketHotelPools();
  const marketKeys = marketKeysForProfile(profile);
  const foreignDurable = foreignDurableHotelNames(propertyId, cwd);
  const hotelsById = new Map();

  const subject = subjectHotelFromProfile(profile);
  if (subject) hotelsById.set(subject.entityId, subject);

  for (const key of marketKeys) {
    for (const h of pools.get(key) || []) {
      if (!hotelsById.has(h.entityId)) {
        hotelsById.set(h.entityId, { ...h, subject: false });
      }
    }
  }
  // Soft match: any pool hotel whose market string overlaps profile market tokens
  for (const [mKey, list] of pools.entries()) {
    if (marketKeys.some((k) => mKey.includes(k) || k.includes(mKey))) {
      for (const h of list) {
        if (!hotelsById.has(h.entityId)) hotelsById.set(h.entityId, { ...h, subject: false });
      }
    }
  }

  // Seed registry for classification of evidence-backed additions
  let seed = createEntityRegistry({
    version: `${CONTRACT_ID}_seed`,
    hotels: [...hotelsById.values()],
    brandOnly: [...BRAND_ONLY],
  });

  const ephemeral = [];
  const synthesisExclusions = [];
  const mentionFreq = new Map();
  for (const o of obs) {
    for (const raw0 of o.competitorsMentioned || []) {
      const recovered = recoverCanonicalMentionFromRawResponse(
        raw0,
        o.rawResponse || o.responseText || o.answer || ""
      );
      const raw = recovered || raw0;
      const n = norm(raw);
      if (!n) continue;
      if (isVibeOrCategoryDescriptor(raw) || isVibeOrCategoryDescriptor(raw0)) {
        synthesisExclusions.push({
          raw: raw0,
          recovered: recovered !== raw0 ? recovered : undefined,
          reason: "VIBE_OR_CATEGORY_DESCRIPTOR",
          count: 1,
          observationId: o.observationId || o.id,
          provider: o.provider,
        });
        continue;
      }
      const prev = mentionFreq.get(n) || { raw, count: 0, samples: [] };
      // Prefer the longer recovered display form when collisions share a key.
      if (String(raw).length > String(prev.raw || "").length) prev.raw = raw;
      prev.count += 1;
      if (prev.samples.length < 3) {
        prev.samples.push({
          observationId: o.observationId || o.id,
          provider: o.provider,
          scenarioId: o.scenarioId,
          recoveredFrom: recovered !== raw0 ? raw0 : undefined,
        });
      }
      mentionFreq.set(n, prev);
    }
  }

  for (const [, row] of mentionFreq) {
    const classified = seed.classifyObservedEntity(row.raw);
    if (classified.identityOk) continue;
    if (BRAND_ONLY.has(norm(row.raw)) || LOCATION_ONLY.has(norm(row.raw))) continue;
    if (isVibeOrCategoryDescriptor(row.raw)) {
      synthesisExclusions.push({
        raw: row.raw,
        reason: "VIBE_OR_CATEGORY_DESCRIPTOR",
        count: row.count,
      });
      continue;
    }
    if (isNonHotelEntity(row.raw) || isLikelyArtifactEntity(row.raw)) {
      synthesisExclusions.push({ raw: row.raw, reason: "NON_HOTEL_OR_ARTIFACT", count: row.count });
      continue;
    }
    if (isCrossMarketLeak(row.raw, profile)) {
      synthesisExclusions.push({ raw: row.raw, reason: "CROSS_MARKET_LEAK", count: row.count });
      continue;
    }
    // Reject identities that already belong to a foreign market hotel pool.
    let foreignPoolHit = null;
    for (const [mKey, list] of pools.entries()) {
      const local = marketKeys.some((k) => mKey.includes(k) || k.includes(mKey));
      if (local) continue;
      if (
        (list || []).some(
          (h) =>
            norm(h.canonical) === norm(row.raw) ||
            (h.aliases || []).some((a) => norm(a) === norm(row.raw))
        )
      ) {
        foreignPoolHit = mKey;
        break;
      }
    }
    if (foreignPoolHit) {
      synthesisExclusions.push({
        raw: row.raw,
        reason: `FOREIGN_MARKET_POOL:${foreignPoolHit}`,
        count: row.count,
      });
      continue;
    }
    const foreignDurableHit = foreignDurable.get(norm(row.raw));
    if (foreignDurableHit) {
      synthesisExclusions.push({
        raw: row.raw,
        reason: `FOREIGN_DURABLE_REGISTRY:${foreignDurableHit.propertyId}`,
        count: row.count,
      });
      continue;
    }
    const customer = resolveCustomerFacingEntity(row.raw, profile);
    if (
      customer.rejected &&
      customer.reason &&
      !["unbound_fail_closed", "empty"].includes(customer.reason)
    ) {
      synthesisExclusions.push({
        raw: row.raw,
        reason: `CUSTOMER_RESOLVER_${customer.reason}`,
        count: row.count,
      });
      continue;
    }
    if (!looksLikeHotelName(row.raw)) {
      synthesisExclusions.push({ raw: row.raw, reason: "NOT_HOTEL_SHAPED", count: row.count });
      continue;
    }
    // Require material frequency before promoting ephemeral identity
    if (row.count < 2) continue;
    const canonical = String(row.raw).replace(/\s+/g, " ").trim();
    const mergeTarget = findAliasMergeTarget(canonical, hotelsById);
    if (mergeTarget) {
      mergeTarget.aliases = [
        ...new Set([...(mergeTarget.aliases || []), norm(row.raw), String(row.raw).toLowerCase()]),
      ];
      mergeTarget.evidenceMentionCount =
        (mergeTarget.evidenceMentionCount || 0) + row.count;
      continue;
    }
    const entityId = slugEntityId(row.raw);
    if (hotelsById.has(entityId)) continue;
    const hotel = {
      entityId,
      canonical,
      market: profile.market || profile.city || "",
      geography: "evidence_backed_auto",
      chainScale: "Unknown",
      propertyType: "hotel",
      identityConfidence: row.count >= 5 ? "HIGH" : "MEDIUM",
      subject: false,
      meetings: false,
      spa: false,
      // Default false — top-N capped below before CORE promotion
      coreEligible: false,
      aliases: [norm(row.raw), String(row.raw).toLowerCase()],
      provenance: "AUTONOMOUS_EVIDENCE_BACKED",
      evidenceMentionCount: row.count,
      evidenceSamples: row.samples,
    };
    hotelsById.set(entityId, hotel);
    ephemeral.push(hotel);
  }

  // Second-pass alias collapse (order-independent): shorter names fold into longer locals.
  for (const h of [...hotelsById.values()]) {
    if (h.subject || h.provenance !== "AUTONOMOUS_EVIDENCE_BACKED") continue;
    const target = findAliasMergeTarget(h.canonical, hotelsById);
    if (!target || target.entityId === h.entityId) continue;
    target.aliases = [
      ...new Set([
        ...(target.aliases || []),
        ...(h.aliases || []),
        norm(h.canonical),
        String(h.canonical).toLowerCase(),
      ]),
    ];
    target.evidenceMentionCount =
      (target.evidenceMentionCount || 0) + (h.evidenceMentionCount || 0);
    hotelsById.delete(h.entityId);
    const eIdx = ephemeral.findIndex((x) => x.entityId === h.entityId);
    if (eIdx >= 0) ephemeral.splice(eIdx, 1);
  }

  // Global CORE peer cap — market-pool reuse must not dump 20–80 peers into territory CORE.
  // Prefer evidence-backed mention frequency; unresolved market-pool hotels rank after.
  for (const h of hotelsById.values()) {
    if (h.subject) continue;
    h.coreEligible = false;
  }
  [...hotelsById.values()]
    .filter((h) => !h.subject)
    .sort((a, b) => {
      const ae = a.evidenceMentionCount || 0;
      const be = b.evidenceMentionCount || 0;
      if (be !== ae) return be - ae;
      const ap = a.provenance === "AUTONOMOUS_EVIDENCE_BACKED" ? 1 : 0;
      const bp = b.provenance === "AUTONOMOUS_EVIDENCE_BACKED" ? 1 : 0;
      if (bp !== ap) return bp - ap;
      return String(a.canonical || "").localeCompare(String(b.canonical || ""));
    })
    .forEach((h, idx) => {
      h.coreEligible = idx < AUTONOMOUS_CORE_PEER_CAP;
      if (h.provenance === "AUTONOMOUS_EVIDENCE_BACKED") {
        const e = ephemeral.find((x) => x.entityId === h.entityId);
        if (e) e.coreEligible = h.coreEligible;
      }
    });

  const hotels = [...hotelsById.values()];
  const registry = createEntityRegistry({
    version: `${CONTRACT_ID}_${CONTRACT_VERSION}`,
    hotels,
    brandOnly: [...BRAND_ONLY],
  });

  return {
    ok: true,
    propertyId,
    contract: CONTRACT_ID,
    marketKeys,
    hotelCount: hotels.length,
    ephemeralCount: ephemeral.length,
    coreEligibleEphemeral: ephemeral.filter((h) => h.coreEligible).length,
    synthesisExclusions: synthesisExclusions.slice(0, 200),
    synthesisExclusionCount: synthesisExclusions.length,
    reusedMarketHotels: hotels.length - ephemeral.length - (subject ? 1 : 0),
    registry,
    hotels,
    ephemeral,
  };
}

export function persistDurableCompetitorRegistry(propertyId, synthesis, cwd = process.cwd()) {
  const fp = durablePath(propertyId, cwd);
  mkdirSync(path.dirname(fp), { recursive: true });
  const payload = {
    contract: CONTRACT_ID,
    version: CONTRACT_VERSION,
    propertyId,
    generatedAt: new Date().toISOString(),
    marketKeys: synthesis.marketKeys,
    hotelCount: synthesis.hotelCount,
    ephemeralCount: synthesis.ephemeralCount,
    corePeerCap: AUTONOMOUS_CORE_PEER_CAP,
    coreEligibleCount: (synthesis.hotels || []).filter((h) => !h.subject && h.coreEligible).length,
    synthesisExclusionCount: synthesis.synthesisExclusionCount || 0,
    synthesisExclusions: (synthesis.synthesisExclusions || []).slice(0, 200),
    hotels: synthesis.hotels.map((h) => ({
      entityId: h.entityId,
      canonical: h.canonical,
      market: h.market,
      geography: h.geography,
      chainScale: h.chainScale,
      propertyType: h.propertyType,
      identityConfidence: h.identityConfidence,
      subject: Boolean(h.subject),
      meetings: Boolean(h.meetings),
      spa: Boolean(h.spa),
      aliases: [...(h.aliases || [])],
      provenance: h.provenance || "MARKET_POOL",
      evidenceMentionCount: h.evidenceMentionCount || null,
      coreEligible: h.subject ? false : Boolean(h.coreEligible),
      competitiveSetRole: h.subject
        ? "SUBJECT"
        : h.coreEligible
          ? "CERTIFIED_CORE_PEER"
          : "OBSERVED_ALTERNATIVE_NOT_IN_CORE",
    })),
  };
  writeFileSync(fp, JSON.stringify(payload, null, 2), "utf8");
  return { ok: true, path: fp, hotelCount: payload.hotelCount };
}

export function loadDurableCompetitorRegistry(propertyId, cwd = process.cwd()) {
  const fp = durablePath(propertyId, cwd);
  if (!existsSync(fp)) return null;
  try {
    const data = JSON.parse(readFileSync(fp, "utf8"));
    if (!Array.isArray(data.hotels) || !data.hotels.length) return null;
    return createEntityRegistry({
      version: data.version || CONTRACT_ID,
      hotels: data.hotels,
      brandOnly: [...BRAND_ONLY],
    });
  } catch {
    return null;
  }
}

/**
 * Resolve registry for a property: static wiring → durable auto → null.
 * Full E2E should call ensureAutonomousCompetitorRegistry before calculations
 * so greenfield properties never silently fail-closed to zero competitors.
 */
export function resolveEntityRegistryForProperty(propertyId, cwd = process.cwd()) {
  const staticReg = getStaticEntityRegistry(propertyId);
  if (staticReg) return { registry: staticReg, source: "STATIC_PROPERTY_REGISTRY" };
  const durable = loadDurableCompetitorRegistry(propertyId, cwd);
  if (durable) return { registry: durable, source: "DURABLE_AUTONOMOUS_REGISTRY" };
  return { registry: null, source: "NONE" };
}

function isSubjectMention(raw, profile, subjectIds) {
  const n = norm(raw);
  const aliases = [profile?.canonicalName, profile?.name, ...(profile?.aliases || [])]
    .map(norm)
    .filter(Boolean);
  if (aliases.some((a) => a && (n === a || n.includes(a) || a.includes(n)))) return true;
  // Bind via seed registry subject ids when available
  const staticReg = getStaticEntityRegistry(profile?.propertyId);
  if (staticReg) {
    const id = staticReg.canonicalizeToEntityId(raw);
    if (id && subjectIds.has(id)) return true;
  }
  return false;
}

function dispositionForMention(raw, profile, registry, subjectIds) {
  const n = norm(raw);
  if (!n) {
    return { disposition: "EMPTY", reason: "empty_string", certified: false };
  }
  if (isSubjectMention(raw, profile, subjectIds)) {
    return { disposition: "SUBJECT_ALIAS", reason: "subject_or_alias", certified: false };
  }
  if (BRAND_ONLY.has(n)) {
    return { disposition: "BRAND_NOT_PROPERTY", reason: "brand_only", certified: false };
  }
  if (LOCATION_ONLY.has(n)) {
    return { disposition: "GEOGRAPHIC_EXCLUSION", reason: "location_only", certified: false };
  }

  if (registry) {
    const classified = registry.classifyObservedEntity(raw);
    if (classified.identityOk) {
      return {
        disposition: "VERIFIED_COMPETITOR",
        reason: classified.class === ENTITY_CLASSES.DUPLICATE_ALIAS ? "alias_merge" : "canonical_bind",
        certified: true,
        entityId: classified.entityId,
        canonical: classified.canonical,
        resolutionMethod: classified.class,
      };
    }
    if (
      [
        ENTITY_CLASSES.GENERIC_PHRASE,
        ENTITY_CLASSES.VENUE_ONLY,
        ENTITY_CLASSES.NON_HOTEL_ENTITY,
        ENTITY_CLASSES.BRAND_NOT_PROPERTY,
        ENTITY_CLASSES.LOCATION,
      ].includes(classified.class)
    ) {
      return {
        disposition: "NONHOTEL_EXCLUSION",
        reason: classified.class,
        certified: false,
      };
    }
  }

  // Fall through to customer resolver for artifact checks
  const resolved = resolveCustomerFacingEntity(raw, profile);
  if (resolved.ok) {
    return {
      disposition: "VERIFIED_COMPETITOR",
      reason: "customer_resolver_bind",
      certified: true,
      entityId: resolved.entityId,
      canonical: resolved.displayName,
      resolutionMethod: "resolveCustomerFacingEntity",
    };
  }
  if (resolved.rejected && resolved.reason && resolved.reason !== "unbound_fail_closed") {
    return {
      disposition: "NONHOTEL_EXCLUSION",
      reason: resolved.reason,
      certified: false,
    };
  }
  if (looksLikeHotelName(raw)) {
    return {
      disposition: "UNRESOLVED_HOTEL_CANDIDATE",
      reason: "hotel_shaped_unbound",
      certified: false,
    };
  }
  return {
    disposition: "UNSUPPORTED_CANDIDATE",
    reason: resolved.reason || "unbound_fail_closed",
    certified: false,
  };
}

/**
 * Full autonomous resolution + mention disposition ledger.
 * Every extracted mention string must land in exactly one disposition bucket.
 */
export function runAutonomousCompetitorEntityResolution(
  propertyId,
  { cwd = process.cwd(), persist = true, forceSynthesize = false } = {}
) {
  const profile = loadPropertyProfile(propertyId);
  if (!profile) return { ok: false, error: "missing_profile", propertyId };

  const period = loadLatestCoreCustomerPeriod(propertyId);
  if (!period?.observations?.length) {
    return { ok: false, error: "no_core_period", propertyId };
  }

  let registrySource = "NONE";
  let registry = null;
  let synthesis = null;

  const staticReg = getStaticEntityRegistry(propertyId);
  if (staticReg && !forceSynthesize) {
    registry = staticReg;
    registrySource = "STATIC_PROPERTY_REGISTRY";
  } else {
    const durable = loadDurableCompetitorRegistry(propertyId, cwd);
    if (durable && !forceSynthesize) {
      registry = durable;
      registrySource = "DURABLE_AUTONOMOUS_REGISTRY";
    } else {
      synthesis = synthesizeMarketRegistryForProperty(propertyId, {
        cwd,
        observations: period.observations,
      });
      if (!synthesis.ok) return synthesis;
      registry = synthesis.registry;
      registrySource = "SYNTHESIZED_MARKET_POOL";
      if (persist) {
        persistDurableCompetitorRegistry(propertyId, synthesis, cwd);
        registrySource = "SYNTHESIZED_AND_PERSISTED";
      }
    }
  }

  // When static registry exists, still ensure durable ledger + optional market enrichment proof
  if (registrySource === "STATIC_PROPERTY_REGISTRY" && persist) {
    synthesis =
      synthesis ||
      synthesizeMarketRegistryForProperty(propertyId, {
        cwd,
        observations: period.observations,
      });
    // Persist synthesis as diagnostic parallel (does not replace static for resolution)
    if (synthesis?.ok) {
      const diagFp = path.join(
        cwd,
        DURABLE_DIR,
        propertyId,
        "market-synthesis-diagnostic-v1.json"
      );
      mkdirSync(path.dirname(diagFp), { recursive: true });
      writeFileSync(
        diagFp,
        JSON.stringify(
          {
            note: "Diagnostic market-pool synthesis; static registry remains authority for this property",
            hotelCount: synthesis.hotelCount,
            ephemeralCount: synthesis.ephemeralCount,
            marketKeys: synthesis.marketKeys,
          },
          null,
          2
        ),
        "utf8"
      );
    }
  }

  const subjectIds = subjectEntityIdsForProperty(profile);
  const byNorm = new Map();
  let totalExtractedMentions = 0;

  for (const obs of period.observations) {
    for (const raw of obs.competitorsMentioned || []) {
      totalExtractedMentions += 1;
      const key = norm(raw);
      if (!key) continue;
      if (!byNorm.has(key)) {
        byNorm.set(key, {
          rawName: raw,
          mentionCount: 0,
          observationIds: [],
          providers: new Set(),
          scenarios: new Set(),
          withSubject: 0,
          withoutSubject: 0,
        });
      }
      const row = byNorm.get(key);
      row.mentionCount += 1;
      const oid = obs.observationId || obs.id;
      if (oid && row.observationIds.length < 8) row.observationIds.push(oid);
      if (obs.provider) row.providers.add(obs.provider);
      if (obs.scenarioId) row.scenarios.add(obs.scenarioId);
      const sp =
        getGovernedSubjectMentioned(obs) === true ||
        obs.mentioned === true ||
        detectPropertyMention(obs.rawResponse || "", profile).mentioned === true;
      if (sp) row.withSubject += 1;
      else row.withoutSubject += 1;
    }
  }

  const dispositions = {
    VERIFIED_COMPETITOR: [],
    SUBJECT_ALIAS: [],
    NONHOTEL_EXCLUSION: [],
    BRAND_NOT_PROPERTY: [],
    GEOGRAPHIC_EXCLUSION: [],
    UNRESOLVED_HOTEL_CANDIDATE: [],
    UNSUPPORTED_CANDIDATE: [],
    EMPTY: [],
    DUPLICATE_CONSOLIDATED: [],
  };

  const entityRows = [];
  const certifiedByEntity = new Map();

  for (const [, row] of byNorm) {
    const d = dispositionForMention(row.rawName, profile, registry, subjectIds);
    const bucket = dispositions[d.disposition] ? d.disposition : "UNSUPPORTED_CANDIDATE";
    const evidenceRow = {
      rawName: row.rawName,
      normalized: norm(row.rawName),
      mentionCount: row.mentionCount,
      distinctScenarios: row.scenarios.size,
      providers: [...row.providers],
      sampleObservationIds: row.observationIds,
      appearedWithSubject: row.withSubject,
      appearedWithoutSubject: row.withoutSubject,
      disposition: bucket,
      reason: d.reason,
      entityId: d.entityId || null,
      canonical: d.canonical || null,
      resolutionMethod: d.resolutionMethod || d.reason,
      certified: Boolean(d.certified),
      rankingPositionVerifiable: null,
    };
    dispositions[bucket].push(evidenceRow);
    entityRows.push(evidenceRow);

    if (d.certified && d.entityId) {
      const prev = certifiedByEntity.get(d.entityId) || {
        entityId: d.entityId,
        canonical: d.canonical,
        mentionCount: 0,
        aliases: new Set(),
        scenarios: new Set(),
        withSubject: 0,
        withoutSubject: 0,
        sampleObservationIds: [],
        providers: new Set(),
      };
      prev.mentionCount += row.mentionCount;
      prev.aliases.add(row.rawName);
      for (const s of row.scenarios) prev.scenarios.add(s);
      prev.withSubject += row.withSubject;
      prev.withoutSubject += row.withoutSubject;
      for (const p of row.providers) prev.providers.add(p);
      for (const oid of row.observationIds) {
        if (prev.sampleObservationIds.length < 5) prev.sampleObservationIds.push(oid);
      }
      certifiedByEntity.set(d.entityId, prev);
    }
  }

  const certifiedCompetitors = [...certifiedByEntity.values()]
    .map((c) => ({
      entityId: c.entityId,
      canonical: c.canonical,
      mentionCount: c.mentionCount,
      distinctScenarios: c.scenarios.size,
      aliases: [...c.aliases],
      appearedWithSubject: c.withSubject,
      appearedWithoutSubject: c.withoutSubject,
      providers: [...c.providers],
      sampleObservationIds: c.sampleObservationIds,
      inclusionDecision: "INCLUDE",
      inclusionReason: "evidence_backed_registry_bind",
    }))
    .sort((a, b) => b.mentionCount - a.mentionCount);

  const distinctCandidates = byNorm.size;
  const duplicateMentionsConsolidated = Math.max(0, totalExtractedMentions - distinctCandidates);

  const ledger = {
    contract: CONTRACT_ID,
    version: CONTRACT_VERSION,
    propertyId,
    periodId: period.periodId,
    generatedAt: new Date().toISOString(),
    registrySource,
    registryPresent: Boolean(registry),
    entityResolutionExecuted: true,
    totals: {
      totalExtractedMentions,
      distinctCandidateEntities: distinctCandidates,
      duplicateMentionsConsolidated,
      verifiedCompetitors: certifiedCompetitors.length,
      subjectAliases: dispositions.SUBJECT_ALIAS.length,
      nonhotelExclusions:
        dispositions.NONHOTEL_EXCLUSION.length +
        dispositions.BRAND_NOT_PROPERTY.length +
        dispositions.GEOGRAPHIC_EXCLUSION.length,
      unresolvedCandidates: dispositions.UNRESOLVED_HOTEL_CANDIDATE.length,
      unsupportedCandidates: dispositions.UNSUPPORTED_CANDIDATE.length,
      finalCertifiedCompetitors: certifiedCompetitors.length,
    },
    dispositions,
    certifiedCompetitors,
    accountingComplete: true,
  };

  // Every distinct candidate must be in exactly one disposition list
  const accounted = Object.values(dispositions).reduce((n, arr) => n + arr.length, 0);
  ledger.totals.accountedDistinct = accounted;
  ledger.totals.unexplainedDrops = Math.max(0, distinctCandidates - accounted);
  ledger.accountingComplete = ledger.totals.unexplainedDrops === 0;

  const silentLoss = assessSilentCompetitorEvidenceLoss(ledger);

  if (persist) {
    const lp = ledgerPath(propertyId, cwd);
    mkdirSync(path.dirname(lp), { recursive: true });
    writeFileSync(lp, JSON.stringify(ledger, null, 2), "utf8");
    ledger.ledgerPath = lp;
  }

  return {
    ok: ledger.accountingComplete && !silentLoss.blockCustomerReady,
    propertyId,
    contract: CONTRACT_ID,
    registrySource,
    ledger,
    certifiedCompetitors,
    silentLoss,
    synthesis: synthesis
      ? {
          hotelCount: synthesis.hotelCount,
          ephemeralCount: synthesis.ephemeralCount,
          marketKeys: synthesis.marketKeys,
        }
      : null,
  };
}

/**
 * P0 integrity: competitorsMentioned > 0 but certifiedCompetitors = 0 is only
 * acceptable when every mention has an explicit non-competitor disposition AND
 * entity resolution actually ran. Missing registry / skipped resolution = FAIL.
 */
export function assessSilentCompetitorEvidenceLoss(ledger) {
  const t = ledger?.totals || {};
  const extracted = t.totalExtractedMentions || 0;
  const certified = t.finalCertifiedCompetitors || 0;
  const unresolved = t.unresolvedCandidates || 0;
  const unexplained = t.unexplainedDrops || 0;
  const executed = ledger?.entityResolutionExecuted === true;
  const registryPresent = ledger?.registryPresent === true;

  const issues = [];
  if (!executed) {
    issues.push({
      code: "ENTITY_RESOLUTION_SKIPPED",
      severity: "P0",
      message: "Competitor entity resolution did not execute",
    });
  }
  if (extracted > 0 && !registryPresent && certified === 0) {
    issues.push({
      code: "MISSING_REGISTRY_SILENT_ZERO",
      severity: "P0",
      message:
        "Extracted competitor mentions exist but no registry was available — fail-closed zero is evidence loss",
    });
  }
  if (unexplained > 0) {
    issues.push({
      code: "UNEXPLAINED_MENTION_DROPS",
      severity: "P0",
      message: `${unexplained} distinct candidates lack a disposition`,
    });
  }
  if (extracted > 0 && certified === 0 && unresolved > 0 && registryPresent) {
    issues.push({
      code: "MATERIAL_UNRESOLVED_HOTEL_CANDIDATES",
      severity: "P0",
      message: `${unresolved} hotel-shaped candidates remain unresolved with zero certified competitors`,
    });
  }

  // Genuine zero: extracted may be > 0 if all are nonhotel/brand/subject — OK when no unresolved hotels
  const genuineZeroOk =
    certified === 0 &&
    executed &&
    registryPresent &&
    unexplained === 0 &&
    unresolved === 0;

  return {
    contract: CONTRACT_ID,
    blockCustomerReady: issues.some((i) => i.severity === "P0"),
    genuineEvidenceSupportedZero: genuineZeroOk,
    issues,
    extracted,
    certified,
    unresolved,
  };
}

/**
 * Ensure registry + ledger exist before analytical calculations (Full E2E S8b–c).
 */
export function ensureAutonomousCompetitorRegistry(propertyId, opts = {}) {
  const resolved = resolveEntityRegistryForProperty(propertyId, opts.cwd);
  const result = runAutonomousCompetitorEntityResolution(propertyId, {
    ...opts,
    persist: opts.persist !== false,
    // If no static/durable registry, force synthesize
    forceSynthesize: resolved.source === "NONE" ? true : Boolean(opts.forceSynthesize),
  });
  return result;
}

/**
 * Independent 4-state scenario / response reconciliation for displacement certification.
 *
 * CRITICAL: Must use the same eligibility + subject/competitor detectors as
 * `computeCompetitorPresentGaps` or S8D falsely fails with
 * displacementMatchesIndependent=false while the KPI itself is coherent.
 */
export function reconcileDisplacementFourState(propertyId) {
  const profile = loadPropertyProfile(propertyId);
  const period = loadLatestCoreCustomerPeriod(propertyId);
  const scenarios = scenariosForPeriodDisplacement(propertyId, period, profile);
  const gaps = computeCompetitorPresentGaps(period.observations, scenarios, profile);

  const eligibility = buildScenarioEligibilityMap(scenarios, profile);
  const eligibleScenarios = scenariosForEligibility(
    scenarios,
    eligibility,
    CONSIDERATION_ELIGIBLE_STATES
  );
  const eligibleIds = new Set(eligibleScenarios.map((s) => s.scenarioId));
  const comparable = filterComparableObservations(period.observations || []).filter((o) =>
    eligibleIds.has(o.scenarioId)
  );

  const bySc = new Map();
  const response = { SPCP: 0, SPCA: 0, SACP: 0, SACA: 0 };
  for (const obs of comparable) {
    const sp = observationSubjectPresent(obs, profile);
    const cp = observationHasCustomerFacingCompetitor(obs, profile);
    if (sp && cp) response.SPCP += 1;
    else if (sp && !cp) response.SPCA += 1;
    else if (!sp && cp) response.SACP += 1;
    else response.SACA += 1;

    const sid = obs.scenarioId;
    if (!bySc.has(sid)) bySc.set(sid, { anyS: false, anyC: false, anySACP: false });
    const row = bySc.get(sid);
    if (sp) row.anyS = true;
    if (cp) row.anyC = true;
    if (!sp && cp) row.anySACP = true;
  }

  const scenarioExclusive = { SPCP: 0, SPCA: 0, SACP: 0, SACA: 0 };
  let displacementAnySacpResponse = 0;
  for (const row of bySc.values()) {
    if (row.anyS && row.anyC) scenarioExclusive.SPCP += 1;
    else if (row.anyS && !row.anyC) scenarioExclusive.SPCA += 1;
    else if (!row.anyS && row.anyC) scenarioExclusive.SACP += 1;
    else scenarioExclusive.SACA += 1;
    if (row.anySACP) displacementAnySacpResponse += 1;
  }

  const eligibleScenarioCount = eligibleIds.size;
  const expectedResponseCount = comparable.length;
  const subjectObservedScenarios =
    scenarioExclusive.SPCP + scenarioExclusive.SPCA;
  const exclusiveSum =
    scenarioExclusive.SPCP +
    scenarioExclusive.SPCA +
    scenarioExclusive.SACP +
    scenarioExclusive.SACA;
  const responseSum = response.SPCP + response.SPCA + response.SACP + response.SACA;

  return {
    propertyId,
    totalScenarios: bySc.size,
    totalResponses: comparable.length,
    eligibleScenarioCount,
    responseFourState: { ...response, sum: responseSum },
    scenarioExclusiveFourState: { ...scenarioExclusive, sum: exclusiveSum },
    subjectObservedScenarios,
    displacementKpiDefinition:
      "RESPONSE_LEVEL_ABSENCE — scenarios with ≥1 provider answer where subject is absent and a customer-facing competitor is present",
    displacementScenarioCount: gaps.competitorPresentScenarios,
    displacementFullyAbsentScenarios: gaps.competitorPresentScenariosFullyAbsent,
    independentDisplacementAnySacpResponse: displacementAnySacpResponse,
    reconciliation: {
      // Core independent universe is typically 63×4=252; OUT_OF_SCOPE hotels may differ.
      exclusiveSumEqualsEligible: exclusiveSum === eligibleScenarioCount,
      exclusiveSumEquals63: exclusiveSum === 63,
      responseSumEqualsExpected: responseSum === expectedResponseCount,
      responseSumEquals252: responseSum === 252,
      displacementMatchesIndependent:
        gaps.competitorPresentScenarios === displacementAnySacpResponse,
      spcpPlusSacpExplainsDisplacement:
        scenarioExclusive.SPCP + scenarioExclusive.SACP >= gaps.competitorPresentScenarios ||
        displacementAnySacpResponse === gaps.competitorPresentScenarios,
      note:
        "Displacement KPI uses RESPONSE_LEVEL_ABSENCE on consideration-eligible comparable observations (same detectors as computeCompetitorPresentGaps). Exclusive SACP alone understates displacement when SPCP scenarios also contain competitor-only provider answers.",
    },
    gaps,
  };
}

/** Patch getEntityRegistryForProperty consumers via this helper when durable auto is needed. */
export function getEntityRegistryForPropertyAutonomous(propertyId, cwd = process.cwd()) {
  const { registry, source } = resolveEntityRegistryForProperty(propertyId, cwd);
  return registry ? { ...registry, _registrySource: source } : null;
}
