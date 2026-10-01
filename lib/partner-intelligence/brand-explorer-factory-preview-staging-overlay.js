/**
 * Brand Explorer — local / factory staging overlay (read-only).
 *
 * Rules:
 * 1. Factory Preview query (`beInternalPreview=1&factoryPreview=1`):
 *    prefer staged fixture fields over live Brand Basics.
 * 2. LOCAL app founder-review (`NODE_ENV !== production` or localhost Host,
 *    unless BRAND_EXPLORER_LOCAL_STAGING=0):
 *    same preference — Joan reviews staged content in the normal local app
 *    without appending factory-preview query flags.
 * 3. Production (`NODE_ENV=production` and non-localhost): never apply.
 * 4. Live Airtable remains the fallback when no staged fixture field exists.
 *
 * Does NOT write Airtable.
 * Cached live payloads stay pristine — overlays are applied on the response only.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  FACTORY_PREVIEW_CANDIDATE_IDENTITIES,
  isFactoryPreviewCandidate,
  isFactoryPreviewQuery,
  resolveFactoryPreviewSlug,
} from "./brand-explorer-factory-preview-candidates.js";
import {
  buildKnownSlugByRecordId,
  resolveSlugForActiveBrand,
} from "./brand-explorer-active-universe.js";
import {
  evaluateBrandWebsitePreference,
  FAIRFIELD_OFFICIAL_BRAND_WEBSITE,
} from "./brand-explorer-brand-website-preference.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

export const FACTORY_PREVIEW_STAGING_OVERLAY_VERSION =
  "factory-preview-staging-overlay-v2-local-app";

export const LOCAL_BRAND_EXPLORER_STAGING_VERSION =
  "local-brand-explorer-app-staging-v1";

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function fixturesDir(root = ROOT) {
  return path.join(root, "fixtures");
}

/**
 * Resolve staged full-presentation fixture path for a brand slug.
 */
export function resolveFactoryPreviewStagingFixturePath(slug, { root = ROOT } = {}) {
  const s = nz(slug).toLowerCase();
  if (!s) return null;
  const p = path.join(fixturesDir(root), `brand-explorer-presentation-${s}-full.json`);
  return fs.existsSync(p) ? p : null;
}

/**
 * Load staged overlay fields from the presentation fixture (if present).
 */
export function loadFactoryPreviewStagingOverlay(slug, { root = ROOT } = {}) {
  const s = nz(slug).toLowerCase();
  if (!s) return null;
  const fixturePath = resolveFactoryPreviewStagingFixturePath(s, { root });
  if (!fixturePath) {
    return {
      slug: s,
      fixturePath: null,
      brandWebsite: "",
      source: "no_fixture",
    };
  }
  try {
    const json = JSON.parse(fs.readFileSync(fixturePath, "utf8"));
    const brandWebsite = nz(json.brandWebsite);
    return {
      slug: s,
      fixturePath: path.relative(root, fixturePath).replace(/\\/g, "/"),
      brandWebsite,
      source: brandWebsite ? "presentation_fixture.brandWebsite" : "presentation_fixture_no_brandWebsite",
    };
  } catch (err) {
    return {
      slug: s,
      fixturePath: path.relative(root, fixturePath).replace(/\\/g, "/"),
      brandWebsite: "",
      source: `fixture_read_error:${err?.message || err}`,
    };
  }
}

/**
 * True when a staged presentation fixture exists and carries a brandWebsite.
 */
export function hasValidStagedBrandWebsiteFixture(slug, { root = ROOT } = {}) {
  const staging = loadFactoryPreviewStagingOverlay(slug, { root });
  return Boolean(nz(staging?.brandWebsite));
}

/**
 * Build a URLSearchParams-like search string from an Express/query object.
 */
export function queryObjectToSearch(query = {}) {
  const q = query || {};
  const parts = [];
  for (const [k, v] of Object.entries(q)) {
    if (v == null || v === "") continue;
    if (Array.isArray(v)) {
      for (const item of v) parts.push(`${encodeURIComponent(k)}=${encodeURIComponent(String(item))}`);
    } else {
      parts.push(`${encodeURIComponent(k)}=${encodeURIComponent(String(v))}`);
    }
  }
  return parts.length ? `?${parts.join("&")}` : "";
}

function hostLooksLocal(hostRaw) {
  const host = nz(hostRaw).toLowerCase().split(",")[0].trim();
  if (!host) return false;
  const hostname = host.replace(/:\d+$/, "");
  return (
    hostname === "localhost" ||
    hostname === "127.0.0.1" ||
    hostname === "::1" ||
    hostname === "[::1]"
  );
}

/**
 * LOCAL Brand Explorer founder-review staging.
 * Production NODE_ENV never enables this unless Host is explicitly localhost
 * (defense for mis-set NODE_ENV on a laptop) — and even then only when not
 * BRAND_EXPLORER_LOCAL_STAGING=0.
 */
export function isLocalBrandExplorerAppStagingEnabled({
  env = process.env,
  req = null,
  host = null,
} = {}) {
  const flag = nz(env.BRAND_EXPLORER_LOCAL_STAGING).toLowerCase();
  if (flag === "0" || flag === "false" || flag === "off") return false;
  if (flag === "1" || flag === "true" || flag === "on") return true;

  const hostValue =
    host ||
    req?.headers?.host ||
    req?.headers?.["x-forwarded-host"] ||
    "";
  const localHost = hostLooksLocal(hostValue);
  const nodeEnv = nz(env.NODE_ENV).toLowerCase();

  if (nodeEnv === "production") {
    // Hard rule: production API behavior unchanged unless someone is literally
    // hitting a production build via localhost (local smoke). Even then require
    // explicit opt-in via BRAND_EXPLORER_LOCAL_STAGING=1 (handled above).
    return false;
  }

  // Non-production: local founder-review default.
  if (localHost) return true;
  return true;
}

/**
 * Whether this request/options should apply factory-preview OR local-app staging overlays.
 */
export function shouldApplyFactoryPreviewStagingOverlay(options = {}) {
  if (options.factoryPreview === true) return true;
  if (options.localAppStaging === true) return true;
  if (isFactoryPreviewQuery(options.search || "")) return true;
  if (options.query && isFactoryPreviewQuery(queryObjectToSearch(options.query))) return true;
  if (
    isLocalBrandExplorerAppStagingEnabled({
      env: options.env || process.env,
      req: options.req || null,
      host: options.host || null,
    })
  ) {
    return true;
  }
  return false;
}

export function resolveStagingMode(options = {}) {
  if (options.factoryPreview === true || isFactoryPreviewQuery(options.search || "")) {
    return "factory_preview";
  }
  if (options.query && isFactoryPreviewQuery(queryObjectToSearch(options.query))) {
    return "factory_preview";
  }
  if (
    options.localAppStaging === true ||
    isLocalBrandExplorerAppStagingEnabled({
      env: options.env || process.env,
      req: options.req || null,
      host: options.host || null,
    })
  ) {
    return "local_app";
  }
  return null;
}

/**
 * Resolve preferred staged brandWebsite.
 * Fixture brandWebsite wins; else preference-preferred official when live is parent root.
 */
export function resolveFactoryPreviewStagedBrandWebsite({
  slug,
  liveBrandWebsite = "",
  root = ROOT,
} = {}) {
  const s = nz(slug).toLowerCase();
  const live = nz(liveBrandWebsite);
  const staging = loadFactoryPreviewStagingOverlay(s, { root });
  const fixtureWebsite = nz(staging?.brandWebsite);

  if (fixtureWebsite) {
    return {
      brandWebsite: fixtureWebsite,
      applied: true,
      reason: "fixture_brandWebsite",
      staging,
      preference: null,
    };
  }

  const preference = evaluateBrandWebsitePreference({
    brandSlug: s,
    brandWebsite: live,
  });
  if (preference.pass === false && nz(preference.preferredWebsite)) {
    return {
      brandWebsite: preference.preferredWebsite,
      applied: true,
      reason: "preference_over_parent_root",
      staging,
      preference,
    };
  }

  if (s === "fairfield-by-marriott" && !live) {
    return {
      brandWebsite: FAIRFIELD_OFFICIAL_BRAND_WEBSITE,
      applied: true,
      reason: "fairfield_official_fallback",
      staging,
      preference,
    };
  }

  return {
    brandWebsite: live,
    applied: false,
    reason: "live_unchanged",
    staging,
    preference,
  };
}

function resolveBrandSlugForStaging(brand = {}, options = {}) {
  const env = options.env || process.env;
  const fromFactory = resolveFactoryPreviewSlug(brand, options);
  if (fromFactory) return fromFactory;

  const knownById = options.knownById || buildKnownSlugByRecordId();
  const recordId = nz(brand.id || brand.recordId);
  const name = nz(brand.name || brand.brandName);
  const { slug } = resolveSlugForActiveBrand({
    recordId,
    name,
    knownById,
  });
  if (slug) return slug;

  const identity = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[nz(brand.slug).toLowerCase()];
  if (identity?.slug) return identity.slug;
  for (const entry of Object.values(FACTORY_PREVIEW_CANDIDATE_IDENTITIES)) {
    if (nz(entry.recordId) === recordId) return entry.slug;
  }
  return nz(brand.slug).toLowerCase();
}

/**
 * Apply staging overlay onto a brand API object (detail or list card).
 * Sets both brandWebsite and website so list cards + detail share the staged value.
 * Returns a shallow-cloned brand when fields change; otherwise the same reference
 * when no overlay applies at all (mode off).
 */
export function applyFactoryPreviewStagingOverlay(brand = {}, options = {}) {
  const mode = resolveStagingMode(options);
  if (!mode) return brand;

  const env = options.env || process.env;
  const slug = resolveBrandSlugForStaging(brand, options);
  if (!slug) return brand;

  // Factory-preview query still prefers allowlisted candidates when present;
  // local-app mode applies whenever a valid staged fixture exists for the slug.
  if (mode === "factory_preview" && !isFactoryPreviewCandidate(slug, { env })) {
    // Still allow if a valid staged fixture exists (e.g. graduated Wave 16A).
    if (!hasValidStagedBrandWebsiteFixture(slug, { root: options.root || ROOT })) {
      return brand;
    }
  }

  const liveWebsite = nz(brand.brandWebsite) || nz(brand.website);
  const resolved = resolveFactoryPreviewStagedBrandWebsite({
    slug,
    liveBrandWebsite: liveWebsite,
    root: options.root || ROOT,
  });

  if (!resolved.applied) {
    // No staged field — keep live Airtable values; attach meta only for factory preview.
    if (mode !== "factory_preview") return brand;
    return {
      ...brand,
      factoryPreviewStaging: {
        version: FACTORY_PREVIEW_STAGING_OVERLAY_VERSION,
        mode,
        applied: false,
        reason: resolved.reason,
        slug,
        fixturePath: resolved.staging?.fixturePath || null,
        airtableWrites: false,
      },
    };
  }

  const priorWebsite = liveWebsite;
  return {
    ...brand,
    brandWebsite: resolved.brandWebsite,
    website: resolved.brandWebsite,
    factoryPreviewStaging: {
      version: FACTORY_PREVIEW_STAGING_OVERLAY_VERSION,
      localStagingVersion: LOCAL_BRAND_EXPLORER_STAGING_VERSION,
      mode,
      applied: true,
      reason: resolved.reason,
      slug,
      fields: ["brandWebsite", "website"],
      priorBrandWebsite: priorWebsite || null,
      stagedBrandWebsite: resolved.brandWebsite,
      fixturePath: resolved.staging?.fixturePath || null,
      airtableWrites: false,
      productionUnaffected: true,
    },
  };
}

/**
 * Apply local/factory staging to every brand in a list payload (response-scoped).
 */
export function applyFactoryPreviewStagingOverlayToBrandList(brands = [], options = {}) {
  if (!Array.isArray(brands) || !brands.length) return brands;
  if (!shouldApplyFactoryPreviewStagingOverlay(options)) return brands;
  const knownById = buildKnownSlugByRecordId();
  let changed = false;
  const next = brands.map((b) => {
    const over = applyFactoryPreviewStagingOverlay(b, { ...options, knownById });
    if (over !== b) changed = true;
    return over;
  });
  return changed ? next : brands;
}

/** @deprecated alias — prefer applyFactoryPreviewStagingOverlay */
export const applyLocalBrandExplorerStagingOverlay = applyFactoryPreviewStagingOverlay;
