/**
 * Brand Explorer — Factory Preview staging overlay (read-only).
 *
 * When Factory Preview Mode is active (`beInternalPreview=1&factoryPreview=1`),
 * prefer staged presentation-fixture fields (e.g. brandWebsite) over live Brand
 * Basics so Joan can review approved local content before any Airtable write.
 *
 * Does NOT write Airtable.
 * Does NOT change production / non-factory responses.
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
  evaluateBrandWebsitePreference,
  FAIRFIELD_OFFICIAL_BRAND_WEBSITE,
} from "./brand-explorer-brand-website-preference.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

export const FACTORY_PREVIEW_STAGING_OVERLAY_VERSION =
  "factory-preview-staging-overlay-v1";

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function fixturesDir(root = ROOT) {
  return path.join(root, "fixtures");
}

/**
 * Resolve staged full-presentation fixture path for a factory candidate slug.
 */
export function resolveFactoryPreviewStagingFixturePath(slug, { root = ROOT } = {}) {
  const s = nz(slug).toLowerCase();
  if (!s) return null;
  const p = path.join(fixturesDir(root), `brand-explorer-presentation-${s}-full.json`);
  return fs.existsSync(p) ? p : null;
}

/**
 * Load staged overlay fields from the presentation fixture (if present).
 * @returns {{ slug: string, fixturePath: string|null, brandWebsite: string, source: string }|null}
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

/**
 * Whether this request/options should apply factory-preview staging overlays.
 */
export function shouldApplyFactoryPreviewStagingOverlay(options = {}) {
  if (options.factoryPreview === true) return true;
  if (isFactoryPreviewQuery(options.search || "")) return true;
  if (options.query && isFactoryPreviewQuery(queryObjectToSearch(options.query))) return true;
  return false;
}

/**
 * Resolve preferred staged brandWebsite for factory preview.
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

  // Fairfield hard fallback: official brand site when fixture lacks field but preference knows it.
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

/**
 * Apply staging overlay onto a brand API object for factory preview only.
 * Returns a shallow-cloned brand when fields change; otherwise the same reference.
 */
export function applyFactoryPreviewStagingOverlay(brand = {}, options = {}) {
  if (!shouldApplyFactoryPreviewStagingOverlay(options)) {
    return brand;
  }

  const env = options.env || process.env;
  const slug =
    resolveFactoryPreviewSlug(brand, options) ||
    nz(brand.slug).toLowerCase() ||
    nz(FACTORY_PREVIEW_CANDIDATE_IDENTITIES[nz(brand.id)]?.slug).toLowerCase();

  if (!slug || !isFactoryPreviewCandidate(slug, { env })) {
    return brand;
  }

  const resolved = resolveFactoryPreviewStagedBrandWebsite({
    slug,
    liveBrandWebsite: brand.brandWebsite,
    root: options.root || ROOT,
  });

  if (!resolved.applied) {
    return {
      ...brand,
      factoryPreviewStaging: {
        version: FACTORY_PREVIEW_STAGING_OVERLAY_VERSION,
        applied: false,
        reason: resolved.reason,
        slug,
        fixturePath: resolved.staging?.fixturePath || null,
        airtableWrites: false,
      },
    };
  }

  const priorWebsite = nz(brand.brandWebsite);
  return {
    ...brand,
    brandWebsite: resolved.brandWebsite,
    factoryPreviewStaging: {
      version: FACTORY_PREVIEW_STAGING_OVERLAY_VERSION,
      applied: true,
      reason: resolved.reason,
      slug,
      fields: ["brandWebsite"],
      priorBrandWebsite: priorWebsite || null,
      stagedBrandWebsite: resolved.brandWebsite,
      fixturePath: resolved.staging?.fixturePath || null,
      airtableWrites: false,
      productionUnaffected: true,
    },
  };
}
