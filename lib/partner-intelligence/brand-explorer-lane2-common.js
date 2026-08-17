/**
 * Shared Lane 2 helpers — true-incomplete brands (post-draft integrity + image lanes).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  FULL_BUILD_IDENTITIES,
  FULL_BUILD_TRUE_INCOMPLETE_SLUGS,
  UNCONFIGURED_ACTIVE_FULL_BUILD_SLUGS,
  resolveFullBuildSlug,
  getFullBuildContent,
} from "./brand-explorer-full-build-content.js";
import { BUILT_BLOCKED_PROTECTED_PUBLIC_FULL } from "./brand-explorer-built-blocked-content.js";
import { getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "./brand-explorer-factory-preview-candidates.js";
import {
  isLogoImageUrl,
  isGenericBrandOrLifestyleImageUrl,
  isOfficialLifestylePropertyImageUrl,
} from "./brand-explorer-footprint-opening-image-governance.js";

export {
  FULL_BUILD_TRUE_INCOMPLETE_SLUGS as LANE2_BRAND_SLUGS,
  resolveFullBuildSlug,
  getFullBuildContent,
};

export const LANE2_VERSION = "lane2-v1";
export const MIN_CONTENT_PACK_ROWS = 70;
export const GALLERY_MIN = 6;
export const SCENARIO_MIN = 3;
export const PROPERTY_MIN = 3;

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const LANE2_ROOT = path.resolve(__dirname, "../..");

export const FIXTURE_POOL_BY_SLUG = Object.freeze({
  "autograph-collection": "lane2-autograph-collection-gallery-pool.json",
  "handwritten-collection": "lane2-handwritten-collection-gallery-pool.json",
  "radisson-collection": "lane2-radisson-collection-gallery-pool.json",
  "tapestry-collection-by-hilton": "lane2-tapestry-collection-by-hilton-gallery-pool.json",
  "vignette-collection": "lane2-vignette-collection-gallery-pool.json",
  "bw-premier-collection": "lane2-bw-premier-collection-gallery-pool.json",
  "bw-signature-collection": "lane2-bw-signature-collection-gallery-pool.json",
  "preferred-hotels-and-resorts": "lane2-preferred-hotels-and-resorts-gallery-pool.json",
  "dazzler-by-wyndham": "lane2-dazzler-by-wyndham-gallery-pool.json",
  "trademark-collection-by-wyndham": "lane2-trademark-collection-by-wyndham-gallery-pool.json",
});

export const EXPECTED_PARENT_COMPANY_RE = Object.freeze({
  "autograph-collection": /marriott/i,
  "handwritten-collection": /accor/i,
  "radisson-collection": /choice|radisson/i,
  "tapestry-collection-by-hilton": /hilton/i,
  "vignette-collection": /intercontinental|ihg/i,
  "bw-premier-collection": /best western|bwh/i,
  "bw-signature-collection": /best western|bwh/i,
  "preferred-hotels-and-resorts": /preferred/i,
  "dazzler-by-wyndham": /wyndham/i,
  "trademark-collection-by-wyndham": /wyndham/i,
});

export const GALLERY_ROLE_TITLES = Object.freeze([
  "Exterior / Arrival",
  "Guest Room / Suite",
  "Public Space",
  "F&B or Local Experience",
  "Design Detail",
  "Property Setting",
]);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function escapeFormulaValue(v) {
  return nz(v).replace(/'/g, "\\'");
}

export function resolveLane2BrandIdentity(slug) {
  const brandSlug = resolveFullBuildSlug(slug);
  const identity = FULL_BUILD_IDENTITIES[brandSlug];
  const cfg = getActiveProfileBrandConfig(brandSlug);
  const factory = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[brandSlug];
  return {
    brandSlug,
    recordId: cfg?.recordId || identity?.recordId || factory?.recordId || null,
    name: cfg?.name || identity?.name || factory?.name || brandSlug,
    reportSlug: identity?.reportSlug || brandSlug,
    parentCompany: cfg?.parentCompany || identity?.parentCompany || null,
  };
}

export function refuseProtectedOrOutOfCohort(slug) {
  const brandSlug = resolveFullBuildSlug(slug);
  if (BUILT_BLOCKED_PROTECTED_PUBLIC_FULL.includes(brandSlug)) {
    return { refused: true, reason: "protected_public_full_baseline" };
  }
  const allowed = new Set([
    ...FULL_BUILD_TRUE_INCOMPLETE_SLUGS,
    ...UNCONFIGURED_ACTIVE_FULL_BUILD_SLUGS,
  ]);
  if (!allowed.has(brandSlug)) {
    return { refused: true, reason: "not_in_lane2_cohort" };
  }
  return { refused: false, brandSlug };
}

export function loadLane2GalleryPool(brandSlug) {
  const slug = resolveFullBuildSlug(brandSlug);
  const fileName = FIXTURE_POOL_BY_SLUG[slug];
  if (!fileName) return [];
  const p = path.join(LANE2_ROOT, "fixtures", fileName);
  if (!fs.existsSync(p)) return [];
  try {
    const data = JSON.parse(fs.readFileSync(p, "utf8"));
    return Array.isArray(data) ? data : [];
  } catch (err) {
    if (process.env.NODE_ENV !== "production") {
      console.warn(`[lane2-common] failed to read ${p}: ${err.message}`);
    }
    return [];
  }
}

/** Reject logos, Radisson doorknob/app promo, generic IHG placeholders, non-official URLs. */
export function isLane2RejectedImageUrl(url, { brandSlug = "" } = {}) {
  const u = nz(url).toLowerCase();
  if (!u) return { rejected: true, reason: "missing_url" };
  if (isLogoImageUrl(u)) return { rejected: true, reason: "logo" };
  if (/doorknob\.jpg|statics\.radissonhotels\.com\/main\/img\/doorknob/i.test(u)) {
    return { rejected: true, reason: "radisson_doorknob_placeholder" };
  }
  if (/radisson-hotels-app\/promotional/i.test(u)) {
    return { rejected: true, reason: "radisson_app_promotional" };
  }
  if (/digital\.ihg\.com\/is\/image\/ihg\/stays\b/i.test(u)) {
    return { rejected: true, reason: "ihg_generic_stays_graphic" };
  }
  if (brandSlug === "vignette-collection") {
    if (!/vignette-collection-/i.test(u) && /digital\.ihg\.com\/is\/image\/ihg\/ihg-/i.test(u)) {
      return { rejected: true, reason: "generic_ihg_brand_hero" };
    }
  }
  if (!isOfficialLifestylePropertyImageUrl(u)) {
    return { rejected: true, reason: "not_official_property_cdn" };
  }
  if (isGenericBrandOrLifestyleImageUrl(u)) {
    return { rejected: true, reason: "generic_brand_lifestyle" };
  }
  return { rejected: false, reason: null };
}

export function normalizePoolAssets(rawPool = [], brandSlug) {
  const slug = resolveFullBuildSlug(brandSlug);
  const accepted = [];
  const rejections = [];
  for (const row of rawPool) {
    const imageUrl = nz(row.imageUrl);
    const gate = isLane2RejectedImageUrl(imageUrl, { brandSlug: slug });
    if (gate.rejected) {
      rejections.push({ imageUrl, reason: gate.reason, propertyKey: row.propertyKey });
      continue;
    }
    accepted.push({
      imageUrl,
      sourcePageUrl: nz(row.sourcePageUrl),
      propertyKey: nz(row.propertyKey),
      propertyName: nz(row.propertyName),
      marketCity: nz(row.marketCity),
      geographyLabel: nz(row.geographyLabel),
      label: nz(row.label) || "property",
      role: nz(row.role),
      caption: nz(row.caption),
      probedSuffix: nz(row.probedSuffix),
    });
  }
  return { accepted, rejections };
}

/** Lightweight Presentation fetch (same pattern as full-tab-factory-build). */
export async function listPresentationRowsLight(brandRecordId, brandName) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey || !brandRecordId) return { rows: [], skipped: "missing_airtable_credentials" };
  const formula = `{Brand Name}='${escapeFormulaValue(brandName)}'`;
  const rows = [];
  let offset = "";
  do {
    const params = new URLSearchParams({ pageSize: "100", filterByFormula: formula });
    if (offset) params.set("offset", offset);
    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}?${params}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) {
      throw new Error(json.error?.message || `Presentation list failed for ${brandName}: ${res.status}`);
    }
    for (const rec of json.records || []) {
      const f = rec.fields || {};
      const image = f.Image;
      const imageAtt = Array.isArray(image) && image[0] ? image[0] : null;
      const imageUrl = imageAtt?.url ? nz(imageAtt.url) : "";
      const imageFilename = nz(imageAtt?.filename);
      rows.push({
        recordId: rec.id,
        slotKey: nz(f["Slot Key"]),
        title: nz(f.Title),
        body: nz(f.Body),
        brandName: nz(f["Brand Name"]),
        active: f.Active !== false,
        externalDisplayStatus: nz(f["External Display Status"]),
        sortOrder: f["Sort Order"] ?? 0,
        caseSummaryOverview: nz(f["Case Summary Overview"]),
        caseSummaryBrandRelevance: nz(f["Case Summary Brand Relevance"]),
        caseSummaryOwnerObjective: nz(f["Case Summary Owner Objective"]),
        caseSummaryInterpretation: nz(f["Case Summary Interpretation"]),
        caseSummaryTags: nz(f["Case Summary Tags"]),
        imageUrl,
        imageFilename,
        filename: imageFilename,
      });
    }
    offset = json.offset || "";
  } while (offset);
  return { rows, skipped: null };
}

export function writeLane2Reports({ jsonPath, mdPath, json, mdLines }) {
  const dir = path.dirname(jsonPath);
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(jsonPath, `${JSON.stringify(json, null, 2)}\n`, "utf8");
  fs.writeFileSync(mdPath, `${(mdLines || []).join("\n")}\n`, "utf8");
  return { jsonPath, mdPath };
}
