/**
 * Brand Explorer — canonical Active/Live universe.
 *
 * Source of truth:
 *   Brand Basics · Brand Status ∈ {Active, Live}
 *   lib/brand-status-active.js → BRAND_STATUS_ACTIVE_FORMULA
 *   APIs: GET /api/brand-library/brands, GET /api/brand-explorer/brands
 *
 * Operational cohorts (PRIMARY_RELEASE_SLUGS, Lane 1/2, intentional restore,
 * LEGACY_SEED, prior 23 reconciliation, FACTORY_SUPPORTED) are NOT the
 * active universe. They are subsets/overlays used for release, restore, or build.
 *
 * Read-only. Never writes Airtable.
 */
import { BRAND_STATUS_ACTIVE_FORMULA, isBrandStatusActive } from "../brand-status-active.js";
import { LEGACY_SEED_BRANDS } from "./brand-explorer-legacy-approved-profile-reconciliation.js";
import { BUILT_BLOCKED_IDENTITIES } from "./brand-explorer-built-blocked-content.js";
import { FULL_BUILD_IDENTITIES } from "./brand-explorer-full-build-content.js";
import {
  listActiveProfileBrandSlugs,
  getActiveProfileBrandConfig,
} from "./brand-explorer-active-profile-brand-config.js";
import { ACTIVE_BRAND_AUDIT_TARGETS } from "./brand-explorer-portfolio-mix-context-normalization-writer.js";
import { slugifyBrandName } from "./brand-explorer-expansion-backlog-planner.js";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "./brand-explorer-factory-preview-candidates.js";
import {
  WAVE13_ACTIVE_IDENTITY_ANCHORS,
  canonicalWave13ActiveSlug,
} from "./brand-explorer-wave13-active-identity-anchors.js";

/** Extra Active/Live identity anchors (recordId → slug) outside seed/factory maps. */
const EXTRA_ACTIVE_IDENTITY_ANCHORS = Object.freeze([
  { slug: "radisson", recordId: "recywbx1YQSTCPqW1" },
  { slug: "radisson-blu", recordId: "recWPEvxBQxVVzSq3" },
  { slug: "radisson-red", recordId: "recmKqo7M7mLZgRqQ" },
  { slug: "quality-inn", recordId: "recd8o4k1JddhkRWW" },
  { slug: "bw-premier-collection", recordId: "recwXZ5gVZ8ZH8ekA" },
  { slug: "bw-signature-collection", recordId: "recdeh1NsP4gjrv80" },
  { slug: "preferred-hotels-and-resorts", recordId: "recwl5JOYxlChuCAr" },
  { slug: "ac-hotels-by-marriott", recordId: "rec9aZp7GHtzUEg0c" },
  { slug: "avid-hotels", recordId: "recoEarnE8T6sDjZq" },
  { slug: "bunkhouse-hotels", recordId: "recGv268Wda31PlSZ" },
  { slug: "canopy-by-hilton", recordId: "recsggfbKlJbjeRP9" },
  { slug: "city-express-by-marriott", recordId: "recucEzAS6724tOYA" },
  { slug: "courtyard-by-marriott", recordId: "rec6hye5H8zJmAGv3" },
  { slug: "even-hotels", recordId: "recvvmiyReHhiKdoK" },
  { slug: "holiday-inn-express", recordId: "recmGmiIqDtAsm01f" },
  { slug: "motto-by-hilton", recordId: "reclt44apoi8co0e6" },
  { slug: "moxy-hotels", recordId: "recahVIW4aCx0Ao84" },
  { slug: "tempo-by-hilton", recordId: "recqiHq3GHKMj8Meo" },
  { slug: "voco-hotels", recordId: "recwONQTqGU1jHCsM" },
  // Wave 13 Accor Active/Live — durable anchors (not factory-preview Wave 14 map)
  ...WAVE13_ACTIVE_IDENTITY_ANCHORS.map((a) => ({ slug: a.slug, recordId: a.recordId })),
  // Wave 14 Marriott Active/Live — durable anchors after factory-preview graduation
  // (Flex remains Under Review / held — intentionally omitted)
  { slug: "marriott-hotels", recordId: "recn59UtkyyoYwzSz" },
  { slug: "sheraton", recordId: "recg8HjT5Bky7NXeV" },
  { slug: "westin", recordId: "recIPuBC50fv13zRR" },
  { slug: "residence-inn-by-marriott", recordId: "rec9Ufbpa0GxJGzt8" },
  { slug: "springhill-suites-by-marriott", recordId: "recBzdGfkMUN9fYsv" },
  { slug: "towneplace-suites-by-marriott", recordId: "recUPiPDivkhNUogr" },
  { slug: "aloft-hotels", recordId: "recJ1GZQpttX7qHgw" },
  { slug: "studiores", recordId: "recDM0LAD8jVRA2x3" },
  // Wave 15 Hilton Active/Live — durable anchors after factory-preview graduation
  // (Four Points Flex remains Under Review / held outside this cohort — intentionally omitted)
  { slug: "hilton-hotels-and-resorts", recordId: "recWubG3rhiS1BaWi" },
  { slug: "homewood-suites-by-hilton", recordId: "recZjYI4nYflGHFNR" },
  { slug: "home2-suites-by-hilton", recordId: "reccZ4zV6wMav7a2i" },
  { slug: "tru-by-hilton", recordId: "recJLiMTv4W8VgO9L" },
  { slug: "doubletree-by-hilton", recordId: "rechVYWQ5ikRnr99B" },
  { slug: "hampton-by-hilton", recordId: "rectRvOWQPaL6FkzZ" },
  { slug: "hilton-garden-inn", recordId: "recrvdAjRlXxPvPPF" },
  { slug: "spark-by-hilton", recordId: "recfv66er4Ch2vJDO" },
]);

export const ACTIVE_UNIVERSE_VERSION = "active-universe-v1";

export const ACTIVE_UNIVERSE_SOURCE = Object.freeze({
  name: "Brand Basics Brand Status Active/Live",
  table: "Brand Setup - Brand Basics",
  file: "lib/brand-status-active.js",
  formula: BRAND_STATUS_ACTIVE_FORMULA,
  filterCriteria: "OR({Brand Status}='Active', {Brand Status}='Live')",
  apis: Object.freeze([
    "GET /api/brand-library/brands",
    "GET /api/brand-explorer/brands",
  ]),
  note:
    "Canonical Brand Explorer active universe. Code cohort lists are operational overlays, not this universe.",
});

/** Brands previously treated as active via code lists but currently not Active/Live. */
export const NON_ACTIVE_STATUS_CONFLICT_PROBES = Object.freeze([
  {
    slug: "radisson-collection",
    recordId: "rec2DDyPu38C6zDBC",
    name: "Radisson Collection",
  },
]);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function mockRes() {
  return {
    headers: {},
    setHeader() {},
    status(c) {
      this.statusCode = c;
      return this;
    },
    json(p) {
      this.payload = p;
      return this;
    },
  };
}

export function buildKnownSlugByRecordId() {
  const map = new Map();
  for (const s of LEGACY_SEED_BRANDS) {
    if (s.recordId) map.set(s.recordId, s.slug);
  }
  for (const [slug, meta] of Object.entries(BUILT_BLOCKED_IDENTITIES)) {
    if (meta?.recordId) map.set(meta.recordId, slug);
  }
  for (const [slug, meta] of Object.entries(FULL_BUILD_IDENTITIES)) {
    if (meta?.recordId) map.set(meta.recordId, slug);
  }
  for (const slug of listActiveProfileBrandSlugs()) {
    const cfg = getActiveProfileBrandConfig(slug);
    if (cfg?.recordId) map.set(cfg.recordId, slug);
  }
  for (const t of ACTIVE_BRAND_AUDIT_TARGETS) {
    if (t.recordId) map.set(t.recordId, t.slug);
  }
  for (const ext of EXTRA_ACTIVE_IDENTITY_ANCHORS) {
    if (ext.recordId) map.set(ext.recordId, ext.slug);
  }
  for (const [slug, meta] of Object.entries(FACTORY_PREVIEW_CANDIDATE_IDENTITIES)) {
    if (meta?.recordId) map.set(meta.recordId, slug);
  }
  return map;
}

export function resolveSlugForActiveBrand({ recordId, name, knownById = null } = {}) {
  const map = knownById || buildKnownSlugByRecordId();
  if (recordId && map.has(recordId)) {
    return { slug: map.get(recordId), slugSource: "known_identity_map" };
  }
  return { slug: slugifyBrandName(name), slugSource: "slugifyBrandName" };
}

/** Inverse of buildKnownSlugByRecordId — used by PVQL / audits that look up by slug. */
export function buildKnownRecordIdBySlug() {
  const map = new Map();
  for (const [recordId, slug] of buildKnownSlugByRecordId()) {
    if (slug && recordId && !map.has(slug)) map.set(slug, recordId);
  }
  // Short Fairmont / SO/ aliases must resolve to the same Airtable record ids.
  for (const anchor of WAVE13_ACTIVE_IDENTITY_ANCHORS) {
    if (!anchor.recordId || !anchor.slug) continue;
    if (!map.has(anchor.slug)) map.set(anchor.slug, anchor.recordId);
    for (const alias of anchor.slugAliases || []) {
      if (alias && !map.has(alias)) map.set(alias, anchor.recordId);
    }
  }
  return map;
}

/**
 * Resolve Airtable Brand Basics record id for a canonical active-universe slug.
 * Accepts raw `rec…` ids passthrough. Returns null when unknown.
 */
export function resolveActiveUniverseRecordId(slugOrId) {
  const raw = nz(slugOrId);
  if (!raw) return null;
  if (/^rec[a-zA-Z0-9]{10,}$/.test(raw)) return raw;
  const key = raw.toLowerCase();
  const bySlug = buildKnownRecordIdBySlug();
  return bySlug.get(key) || bySlug.get(canonicalWave13ActiveSlug(key)) || null;
}

/**
 * Load the canonical Active/Live Brand Explorer universe from Brand Library list API.
 * @returns {Promise<{ source: object, totalCount: number, brands: object[], slugSet: Set<string>, bySlug: Map, byRecordId: Map }>}
 */
export async function loadActiveUniverse({ includeDetails = false } = {}) {
  const { getBrandLibraryBrands, getBrandLibraryBrandById } = await import(
    "../../api/brand-library.js"
  );
  const listRes = mockRes();
  await getBrandLibraryBrands(
    { query: {}, headers: { "x-bypass-brand-list-cache": "1" } },
    listRes
  );
  if (listRes.statusCode >= 400 || !listRes.payload?.success) {
    throw new Error(
      `Active universe list failed: ${listRes.payload?.error || listRes.statusCode || "unknown"}`
    );
  }

  const knownById = buildKnownSlugByRecordId();
  const brands = [];

  for (const b of listRes.payload.brands || []) {
    const recordId = b.id;
    const name = nz(b.name) || "Unknown";
    const status = nz(b.status);
    const { slug, slugSource } = resolveSlugForActiveBrand({ recordId, name, knownById });

    const row = {
      recordId,
      name,
      status,
      slug,
      slugSource,
      isActiveLive: isBrandStatusActive(status),
    };

    if (includeDetails) {
      const detailRes = mockRes();
      try {
        await getBrandLibraryBrandById({ query: { brandId: recordId }, headers: {} }, detailRes);
        const brand = detailRes.payload?.brand || {};
        const blocks = brand.brandExplorer?.blocks || [];
        row.fetchOk = detailRes.statusCode === 200 && Boolean(brand);
        row.presentationRowCount = blocks.length;
        row.publicFull = brand.shouldRenderFullProfile === true;
        row.displayState = brand.brandExplorerDisplayState || null;
        row.readyForActiveProfile = brand.readyForActiveProfile === true;
        row.activeProfileApproved = brand.activeProfileApproved === true;
        row.founderVisualReviewPass = brand.founderVisualReviewPass === true;
        row.legacyHistoricalApproved = brand.legacyHistoricalApproved === true;
        row.brandApi = brand;
      } catch (err) {
        row.fetchOk = false;
        row.fetchError = err?.message || String(err);
        row.presentationRowCount = 0;
        row.publicFull = false;
        row.displayState = null;
        row.brandApi = null;
      }
    }

    brands.push(row);
  }

  brands.sort((a, b) => a.name.localeCompare(b.name, undefined, { sensitivity: "base" }));

  return {
    version: ACTIVE_UNIVERSE_VERSION,
    source: ACTIVE_UNIVERSE_SOURCE,
    totalCount: brands.length,
    brands,
    slugSet: new Set(brands.map((b) => b.slug)),
    bySlug: new Map(brands.map((b) => [b.slug, b])),
    byRecordId: new Map(brands.map((b) => [b.recordId, b])),
  };
}

/** Slug list for PVQL / audits that must cover the active universe. */
export async function listActiveUniverseSlugs() {
  const universe = await loadActiveUniverse({ includeDetails: false });
  return universe.brands.map((b) => b.slug).sort();
}

/**
 * Assert an operational cohort is interpreted relative to the active universe.
 * Does not mutate the cohort — returns membership analysis.
 */
export function analyzeCohortAgainstUniverse(cohortSlugs, slugSet) {
  const included = [...new Set((cohortSlugs || []).map((s) => nz(s).toLowerCase()).filter(Boolean))];
  return {
    included,
    inUniverse: included.filter((s) => slugSet.has(s)),
    outsideUniverse: included.filter((s) => !slugSet.has(s)),
    missingFromCohort: [...slugSet].filter((s) => !included.includes(s)),
  };
}

export function isOperationalCohortNotUniverse(cohortName) {
  return [
    "PRIMARY_RELEASE_SLUGS",
    "LEGACY_SEED_BRANDS",
    "VISIBILITY_RESTORED_RELEASE_SLUGS",
    "BUILT_BLOCKED_TARGETS",
    "FULL_BUILD_TRUE_INCOMPLETE_SLUGS",
    "PUBLIC_RESTORE_GOVERNANCE_TARGETS",
    "FACTORY_SUPPORTED_SLUGS",
    "PRIOR_23_RECONCILIATION_SLUGS",
    "discoverActiveBrandIdentities",
  ].includes(cohortName);
}
