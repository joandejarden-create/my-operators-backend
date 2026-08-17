/**
 * Brand Explorer Tab Factory — full build content index.
 *
 * Aggregates the five true-incomplete brand content packs (see
 * brand-explorer-built-blocked-content.js → BUILT_BLOCKED_TRUE_INCOMPLETE) into a
 * single lookup surface for the tab-factory build/remediation pipeline.
 *
 * Each pack module exports BRAND_FULL_BUILD_CONTENT as:
 *   { brandSlug, sourcePack, brandLens, presentation: [...] }
 *
 * This index does not itself write to Airtable — it is a read-only content
 * registry. Any writer/apply script consuming this index must still run through
 * the standard tab-factory validation gates (field-by-field, source provenance by
 * tab, image distinctiveness, golden benchmark comparison, founder visual review)
 * before active release, per docs/data-intelligence/brand-explorer-tab-factory-build-operation.md.
 */
import { BRAND_FULL_BUILD_CONTENT as AUTOGRAPH_COLLECTION_CONTENT } from "./brand-explorer-full-build-content-autograph-collection.js";
import { BRAND_FULL_BUILD_CONTENT as HANDWRITTEN_COLLECTION_CONTENT } from "./brand-explorer-full-build-content-handwritten-collection.js";
import { BRAND_FULL_BUILD_CONTENT as RADISSON_COLLECTION_CONTENT } from "./brand-explorer-full-build-content-radisson-collection.js";
import { BRAND_FULL_BUILD_CONTENT as TAPESTRY_COLLECTION_CONTENT } from "./brand-explorer-full-build-content-tapestry-collection.js";
import { BRAND_FULL_BUILD_CONTENT as VIGNETTE_COLLECTION_CONTENT } from "./brand-explorer-full-build-content-vignette-collection.js";
import { BRAND_FULL_BUILD_CONTENT as BW_PREMIER_COLLECTION_CONTENT } from "./brand-explorer-full-build-content-bw-premier-collection.js";
import { BRAND_FULL_BUILD_CONTENT as BW_SIGNATURE_COLLECTION_CONTENT } from "./brand-explorer-full-build-content-bw-signature-collection.js";
import { BRAND_FULL_BUILD_CONTENT as PREFERRED_HOTELS_CONTENT } from "./brand-explorer-full-build-content-preferred-hotels-and-resorts.js";
import { BRAND_FULL_BUILD_CONTENT as DAZZLER_BY_WYNDHAM_CONTENT } from "./brand-explorer-full-build-content-dazzler-by-wyndham.js";
import { BRAND_FULL_BUILD_CONTENT as TRADEMARK_COLLECTION_BY_WYNDHAM_CONTENT } from "./brand-explorer-full-build-content-trademark-collection-by-wyndham.js";

/**
 * True-incomplete brand slugs covered by this content build.
 * Matches BUILT_BLOCKED_TRUE_INCOMPLETE in brand-explorer-built-blocked-content.js.
 * Operational cohort — not the Active/Live universe.
 */
export const FULL_BUILD_TRUE_INCOMPLETE_SLUGS = Object.freeze([
  "autograph-collection",
  "handwritten-collection",
  "radisson-collection",
  "tapestry-collection-by-hilton",
  "vignette-collection",
]);

/**
 * Active/Live unconfigured brands receiving first-time Tab Factory full builds.
 * Not part of the legacy true-incomplete cohort; not Radisson Collection / Tapestry.
 *
 * NOTE: dazzler-by-wyndham and trademark-collection-by-wyndham are Under Review
 * Tab Factory candidates (see brand-explorer-wyndham-factory-build-queue.js), NOT
 * currently Active/Live. They are included here only so FULL_BUILD_ALLOWED_SLUGS
 * (brand-explorer-full-tab-factory-build.js, which merges this array with
 * FULL_BUILD_TRUE_INCOMPLETE_SLUGS) permits running full-build content generation
 * for them. Do not treat their presence in this array as an Active-status change —
 * promotion still requires the standard Tab Factory build/QA gates.
 */
export const UNCONFIGURED_ACTIVE_FULL_BUILD_SLUGS = Object.freeze([
  "bw-premier-collection",
  "bw-signature-collection",
  "preferred-hotels-and-resorts",
  "dazzler-by-wyndham",
  "trademark-collection-by-wyndham",
]);

/** Content pack lookup by brandSlug. */
export const FULL_BUILD_CONTENT_BY_SLUG = Object.freeze({
  "autograph-collection": AUTOGRAPH_COLLECTION_CONTENT,
  "handwritten-collection": HANDWRITTEN_COLLECTION_CONTENT,
  "radisson-collection": RADISSON_COLLECTION_CONTENT,
  "tapestry-collection-by-hilton": TAPESTRY_COLLECTION_CONTENT,
  "vignette-collection": VIGNETTE_COLLECTION_CONTENT,
  "bw-premier-collection": BW_PREMIER_COLLECTION_CONTENT,
  "bw-signature-collection": BW_SIGNATURE_COLLECTION_CONTENT,
  "preferred-hotels-and-resorts": PREFERRED_HOTELS_CONTENT,
  "dazzler-by-wyndham": DAZZLER_BY_WYNDHAM_CONTENT,
  "trademark-collection-by-wyndham": TRADEMARK_COLLECTION_BY_WYNDHAM_CONTENT,
});
/**
 * Identity registry (recordId / name / parentCompany / shortAlias) for the five
 * true-incomplete brands, sourced from the record IDs provided for this build.
 */
export const FULL_BUILD_IDENTITIES = Object.freeze({
  "autograph-collection": Object.freeze({
    recordId: "recEJCTDj1zrsjPM6",
    name: "Autograph Collection",
    parentCompany: "Marriott International, Inc.",
    shortAlias: "autograph",
    reportSlug: "autograph",
  }),
  "handwritten-collection": Object.freeze({
    recordId: "rec7hTXwMRC81EPqz",
    name: "Handwritten Collection",
    parentCompany: "Accor",
    shortAlias: "handwritten",
    reportSlug: "handwritten",
  }),
  "radisson-collection": Object.freeze({
    recordId: "rec2DDyPu38C6zDBC",
    name: "Radisson Collection",
    parentCompany: "Choice Hotels International, Inc. (Radisson Hotel Group brand family)",
    shortAlias: "radisson-collection",
    reportSlug: "radisson-collection",
  }),
  "tapestry-collection-by-hilton": Object.freeze({
    recordId: "reccXxMHEh7NNRhIE",
    name: "Tapestry Collection by Hilton",
    parentCompany: "Hilton Worldwide Holdings Inc.",
    shortAlias: "tapestry",
    reportSlug: "tapestry",
  }),
  "vignette-collection": Object.freeze({
    recordId: "recDwzv86TWnz2gGB",
    name: "Vignette Collection",
    parentCompany: "InterContinental Hotels Group",
    shortAlias: "vignette",
    reportSlug: "vignette",
  }),
  "bw-premier-collection": Object.freeze({
    recordId: "recwXZ5gVZ8ZH8ekA",
    name: "BW Premier Collection",
    parentCompany: "BWH Hotels",
    shortAlias: "bw-premier",
    reportSlug: "bw-premier-collection",
  }),
  "bw-signature-collection": Object.freeze({
    recordId: "recdeh1NsP4gjrv80",
    name: "BW Signature Collection",
    parentCompany: "BWH Hotels",
    shortAlias: "bw-signature",
    reportSlug: "bw-signature-collection",
  }),
  "preferred-hotels-and-resorts": Object.freeze({
    recordId: "recwl5JOYxlChuCAr",
    name: "Preferred Hotels & Resorts",
    parentCompany: "Preferred Hotels & Resorts",
    shortAlias: "preferred",
    reportSlug: "preferred-hotels-and-resorts",
  }),
  "dazzler-by-wyndham": Object.freeze({
    recordId: "rec5CNMM4ZUD7ZHlM",
    name: "Dazzler by Wyndham",
    parentCompany: "Wyndham Hotels & Resorts",
    loyaltyProgram: "Wyndham Rewards",
    shortAlias: "dazzler",
    reportSlug: "dazzler-by-wyndham",
  }),
  "trademark-collection-by-wyndham": Object.freeze({
    recordId: "recob7tgHRryRSbeO",
    name: "Trademark Collection by Wyndham",
    parentCompany: "Wyndham Hotels & Resorts",
    loyaltyProgram: "Wyndham Rewards",
    shortAlias: "trademark",
    reportSlug: "trademark-collection-by-wyndham",
  }),
});
export const FULL_BUILD_SLUG_ALIASES = Object.freeze({
  autograph: "autograph-collection",
  "autograph-collection": "autograph-collection",
  handwritten: "handwritten-collection",
  "handwritten-collection": "handwritten-collection",
  "radisson-collection": "radisson-collection",
  tapestry: "tapestry-collection-by-hilton",
  "tapestry-collection": "tapestry-collection-by-hilton",
  "tapestry-collection-by-hilton": "tapestry-collection-by-hilton",
  vignette: "vignette-collection",
  "vignette-collection": "vignette-collection",
  dazzler: "dazzler-by-wyndham",
  "dazzler-by-wyndham": "dazzler-by-wyndham",
  trademark: "trademark-collection-by-wyndham",
  "trademark-collection": "trademark-collection-by-wyndham",
  "trademark-collection-by-wyndham": "trademark-collection-by-wyndham",
});

export function resolveFullBuildSlug(raw) {
  const key = String(raw || "").trim().toLowerCase();
  return FULL_BUILD_SLUG_ALIASES[key] || key;
}

/**
 * @param {string} brandSlug
 * @returns {{ brandSlug: string, sourcePack: object, brandLens: object, presentation: Array<{slotKey:string,title:string,body:string,sortOrder:number}> } | null}
 */
export function getFullBuildContentPack(brandSlug) {
  return FULL_BUILD_CONTENT_BY_SLUG[resolveFullBuildSlug(brandSlug)] || null;
}

/** Alias used by full-tab-factory-build / remaining-brands orchestrator. */
export function getFullBuildContent(brandSlug) {
  return getFullBuildContentPack(brandSlug);
}

/**
 * @param {string} brandSlug
 * @returns {{ recordId: string, name: string, parentCompany: string, shortAlias: string, reportSlug?: string } | null}
 */
export function getFullBuildIdentity(brandSlug) {
  return FULL_BUILD_IDENTITIES[resolveFullBuildSlug(brandSlug)] || null;
}

export default {
  FULL_BUILD_TRUE_INCOMPLETE_SLUGS,
  FULL_BUILD_CONTENT_BY_SLUG,
  FULL_BUILD_IDENTITIES,
  FULL_BUILD_SLUG_ALIASES,
  resolveFullBuildSlug,
  getFullBuildContentPack,
  getFullBuildContent,
  getFullBuildIdentity,
};
