/**
 * Brand Explorer — Factory Preview Mode candidates.
 *
 * Active factory cohort: Wave 16A Stage 2A LOW-risk Marriott brands.
 * Wave 15 Hilton identities remain available for historical/deep-link preview.
 * Separate from production Active/Live universe and the protected 62 public-full baseline.
 *
 * Production public-full remains: Brand Status Active/Live + release gates + PVQL.
 * Factory preview never writes Airtable.
 * AI-Assisted Profile footnote must render in preview mode (always-on enricher).
 */
import { isBrandStatusActive } from "../brand-status-active.js";
import { ACTIVE_UNIVERSE_SOURCE } from "./brand-explorer-active-universe.js";

export const FACTORY_PREVIEW_VERSION = "factory-preview-mode-v6-wave16a-stage2a";

/** Effective UI display state while factory preview is active (internal only). */
export const FACTORY_PREVIEW_DISPLAY_STATE = "factory_preview_internal";

/**
 * Wave 16A Stage 2A factory candidate cohort for controlled tab build preview.
 * Not a public release registry. Not the Active/Live universe.
 */
export const FACTORY_PREVIEW_CANDIDATE_SLUGS = Object.freeze([
  "fairfield-by-marriott",
  "four-points-by-sheraton",
  "delta-hotels-by-marriott",
]);

/** Known identity anchors for deep-link preview (record ID preferred over slug). */
export const FACTORY_PREVIEW_CANDIDATE_IDENTITIES = Object.freeze({
  "fairfield-by-marriott": Object.freeze({
    slug: "fairfield-by-marriott",
    name: "Fairfield by Marriott",
    recordId: "recpUTDtwt1wPMDPj",
    recommendedStatusWhileInFactory: "Under Review",
  }),
  "four-points-by-sheraton": Object.freeze({
    slug: "four-points-by-sheraton",
    name: "Four Points by Sheraton",
    recordId: "recH5ZF9V6ivz9p5h",
    recommendedStatusWhileInFactory: "Under Review",
  }),
  "delta-hotels-by-marriott": Object.freeze({
    slug: "delta-hotels-by-marriott",
    name: "Delta Hotels by Marriott",
    recordId: "rec50qkCj6yt9fMPg",
    recommendedStatusWhileInFactory: "Under Review",
  }),
  // Retained Wave 15 deep-link anchors (not current factory cohort slugs).
  "hilton-hotels-and-resorts": Object.freeze({
    slug: "hilton-hotels-and-resorts",
    name: "Hilton Hotels & Resorts",
    recordId: "recWubG3rhiS1BaWi",
    recommendedStatusWhileInFactory: "Under Review",
  }),
  "homewood-suites-by-hilton": Object.freeze({
    slug: "homewood-suites-by-hilton",
    name: "Homewood Suites by Hilton",
    recordId: "recZjYI4nYflGHFNR",
    recommendedStatusWhileInFactory: "Under Review",
  }),
  "home2-suites-by-hilton": Object.freeze({
    slug: "home2-suites-by-hilton",
    name: "Home2 Suites by Hilton",
    recordId: "reccZ4zV6wMav7a2i",
    recommendedStatusWhileInFactory: "Under Review",
  }),
  "tru-by-hilton": Object.freeze({
    slug: "tru-by-hilton",
    name: "Tru by Hilton",
    recordId: "recJLiMTv4W8VgO9L",
    recommendedStatusWhileInFactory: "Under Review",
  }),
  "doubletree-by-hilton": Object.freeze({
    slug: "doubletree-by-hilton",
    name: "DoubleTree by Hilton",
    recordId: "rechVYWQ5ikRnr99B",
    recommendedStatusWhileInFactory: "Under Review",
  }),
  "hampton-by-hilton": Object.freeze({
    slug: "hampton-by-hilton",
    name: "Hampton by Hilton",
    recordId: "rectRvOWQPaL6FkzZ",
    recommendedStatusWhileInFactory: "Under Review",
  }),
  "hilton-garden-inn": Object.freeze({
    slug: "hilton-garden-inn",
    name: "Hilton Garden Inn",
    recordId: "recrvdAjRlXxPvPPF",
    recommendedStatusWhileInFactory: "Under Review",
  }),
  "spark-by-hilton": Object.freeze({
    slug: "spark-by-hilton",
    name: "Spark by Hilton",
    recordId: "recfv66er4Ch2vJDO",
    recommendedStatusWhileInFactory: "Under Review",
  }),
});

export const FACTORY_PREVIEW_BANNER_TEXT =
  "Factory Preview — Not Public / Not Active Baseline";

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function normalizeSlug(slug) {
  return nz(slug).toLowerCase();
}

/**
 * Parse allowlist from env (comma-separated) or fall back to built-in candidates.
 * Env: BRAND_EXPLORER_FACTORY_PREVIEW_SLUGS=slug1,slug2
 */
export function resolveFactoryPreviewAllowlist({
  env = process.env,
  fallback = FACTORY_PREVIEW_CANDIDATE_SLUGS,
} = {}) {
  const raw = nz(env.BRAND_EXPLORER_FACTORY_PREVIEW_SLUGS);
  if (!raw) return [...fallback];
  return [
    ...new Set(
      raw
        .split(",")
        .map((s) => normalizeSlug(s))
        .filter(Boolean)
    ),
  ];
}

/**
 * Master enable for server-side factory-preview eligibility metadata.
 * - Explicit off: BRAND_EXPLORER_FACTORY_PREVIEW=0
 * - Explicit on: BRAND_EXPLORER_FACTORY_PREVIEW=1
 * - Allowlist env alone enables metadata for those slugs
 * - Default: on outside production; off in production unless explicitly enabled
 */
export function isFactoryPreviewModeEnabled({ env = process.env } = {}) {
  const v = nz(env.BRAND_EXPLORER_FACTORY_PREVIEW).toLowerCase();
  if (v === "0" || v === "false" || v === "off") return false;
  if (v === "1" || v === "true" || v === "on") return true;
  if (nz(env.BRAND_EXPLORER_FACTORY_PREVIEW_SLUGS)) return true;
  return nz(env.NODE_ENV).toLowerCase() !== "production";
}

export function isFactoryPreviewCandidate(slug, { env = process.env } = {}) {
  const s = normalizeSlug(slug);
  if (!s) return false;
  return resolveFactoryPreviewAllowlist({ env }).includes(s);
}

export function getFactoryPreviewIdentity(slugOrRecordId) {
  const key = normalizeSlug(slugOrRecordId);
  if (!key) return null;
  if (FACTORY_PREVIEW_CANDIDATE_IDENTITIES[key]) {
    return FACTORY_PREVIEW_CANDIDATE_IDENTITIES[key];
  }
  for (const identity of Object.values(FACTORY_PREVIEW_CANDIDATE_IDENTITIES)) {
    if (nz(identity.recordId) === nz(slugOrRecordId)) return identity;
  }
  return null;
}

export function resolveFactoryPreviewSlug(brand = {}, options = {}) {
  const direct = normalizeSlug(brand.slug || options.slug);
  if (direct && isFactoryPreviewCandidate(direct, { env: options.env || process.env })) {
    return direct;
  }
  const byId = getFactoryPreviewIdentity(brand.id || brand.recordId || options.recordId);
  if (byId?.slug) return byId.slug;
  if (direct) return direct;
  return "";
}

export function getFactoryPreviewDisplayState(slug, { env = process.env } = {}) {
  if (!isFactoryPreviewCandidate(slug, { env })) return null;
  return FACTORY_PREVIEW_DISPLAY_STATE;
}

/**
 * Query-string / request helpers (shared by Node tests + docs).
 * Client mirrors these with location.search.
 */
export function isFactoryPreviewQuery(search = "") {
  const q = String(search || "");
  return (
    /(?:\?|&)beInternalPreview=1(?:&|$)/.test(q) &&
    /(?:\?|&)factoryPreview=1(?:&|$)/.test(q)
  );
}

export function buildFactoryPreviewUrls({ recordId, slug } = {}) {
  const id = nz(recordId) || nz(slug);
  if (!id) return null;
  const q = `brandId=${encodeURIComponent(id)}&beInternalPreview=1&factoryPreview=1`;
  return {
    combined: `/brand-explorer-combined.html?${q}`,
    explorer: `/brand-explorer?brand=${encodeURIComponent(nz(slug) || id)}&beInternalPreview=1&factoryPreview=1`,
    api: `/api/brand-library/brand?brandId=${encodeURIComponent(id)}`,
  };
}

/**
 * Can this brand render full profile under factory preview (internal only)?
 * Does NOT imply public shouldRenderFullProfile / active_profile_ready.
 *
 * Options:
 * - factoryPreview: true — treat as query already approved (tests)
 * - search: location.search string requiring beInternalPreview=1&factoryPreview=1
 * - requirePresentationRows: default true
 * - hasPresentationRows: override when brand object lacks blocks
 */
export function canRenderFactoryPreview(brand = {}, options = {}) {
  const env = options.env || process.env;
  const slug = resolveFactoryPreviewSlug(brand, options);
  if (!isFactoryPreviewCandidate(slug, { env })) return false;

  const previewRequested =
    options.factoryPreview === true || isFactoryPreviewQuery(options.search || "");
  if (!previewRequested) return false;

  if (options.requireModeEnabled === true && !isFactoryPreviewModeEnabled({ env })) {
    return false;
  }

  const blocks = brand?.brandExplorer?.blocks;
  const hasRows =
    options.hasPresentationRows === true ||
    (Array.isArray(blocks) && blocks.length > 0);
  if (options.requirePresentationRows !== false && !hasRows) return false;
  return true;
}

/**
 * Attach factory-preview metadata for API responses.
 * Never mutates production shouldRenderFullProfile / brandExplorerDisplayState.
 */
export function buildFactoryPreviewApiMeta(brand = {}, { env = process.env } = {}) {
  const slug = resolveFactoryPreviewSlug(brand, { env });
  const identity = getFactoryPreviewIdentity(slug) || getFactoryPreviewIdentity(brand.id);
  const candidate = Boolean(slug && isFactoryPreviewCandidate(slug, { env }));
  const eligible = isFactoryPreviewModeEnabled({ env }) && candidate;
  const urls = buildFactoryPreviewUrls({
    recordId: brand.id || identity?.recordId,
    slug: slug || identity?.slug,
  });
  const blocks = brand?.brandExplorer?.blocks;
  const hasRows = Array.isArray(blocks) && blocks.length > 0;
  return {
    version: FACTORY_PREVIEW_VERSION,
    eligible,
    candidate,
    slug: slug || identity?.slug || null,
    factoryPreviewDisplayState: eligible ? FACTORY_PREVIEW_DISPLAY_STATE : null,
    canRenderFactoryPreview: eligible && hasRows,
    affectsActiveUniverse: false,
    affectsPvqlPublicFull: false,
    affectsProtectedBaseline: false,
    productionShouldRenderFullProfile: brand.shouldRenderFullProfile === true,
    productionDisplayState: brand.brandExplorerDisplayState || null,
    previewUrls: urls,
    bannerText: FACTORY_PREVIEW_BANNER_TEXT,
    activeUniverseSource: ACTIVE_UNIVERSE_SOURCE.name,
  };
}

/**
 * Hard invariant: factory preview config must not redefine Active/Live universe SoT.
 */
export function assertFactoryPreviewDoesNotAffectActiveUniverse() {
  const errors = [];
  if (!ACTIVE_UNIVERSE_SOURCE?.formula?.includes("Active")) {
    errors.push("active_universe_source_unexpected");
  }
  for (const slug of FACTORY_PREVIEW_CANDIDATE_SLUGS) {
    if (!slug || typeof slug !== "string") errors.push(`invalid_candidate:${slug}`);
  }
  // Candidates may temporarily be Active in Airtable (drift) — that is Brand Status truth,
  // not factory preview. Factory preview must never claim they are public-full via this module.
  if (FACTORY_PREVIEW_DISPLAY_STATE === "active_profile_ready") {
    errors.push("factory_preview_must_not_reuse_active_profile_ready");
  }
  if (FACTORY_PREVIEW_DISPLAY_STATE === "external_owner_ready") {
    errors.push("factory_preview_must_not_reuse_external_owner_ready");
  }
  return {
    ok: errors.length === 0,
    errors,
    activeUniverseSource: ACTIVE_UNIVERSE_SOURCE,
    candidateCount: FACTORY_PREVIEW_CANDIDATE_SLUGS.length,
  };
}

/**
 * Whether a Brand Status value places the brand in production Active/Live universe.
 */
export function factoryCandidateIsInActiveUniverseByStatus(brandStatus) {
  return isBrandStatusActive(brandStatus);
}

export default {
  FACTORY_PREVIEW_VERSION,
  FACTORY_PREVIEW_DISPLAY_STATE,
  FACTORY_PREVIEW_CANDIDATE_SLUGS,
  FACTORY_PREVIEW_CANDIDATE_IDENTITIES,
  FACTORY_PREVIEW_BANNER_TEXT,
  resolveFactoryPreviewAllowlist,
  isFactoryPreviewModeEnabled,
  isFactoryPreviewCandidate,
  getFactoryPreviewIdentity,
  resolveFactoryPreviewSlug,
  getFactoryPreviewDisplayState,
  isFactoryPreviewQuery,
  buildFactoryPreviewUrls,
  canRenderFactoryPreview,
  buildFactoryPreviewApiMeta,
  assertFactoryPreviewDoesNotAffectActiveUniverse,
  factoryCandidateIsInActiveUniverseByStatus,
};
