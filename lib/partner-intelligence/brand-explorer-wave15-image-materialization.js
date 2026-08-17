/**
 * Wave 15 Stage 5 — image asset pack + Presentation Image materialization
 * for the eight Hilton brand-family cohort brands (Hilton Hotels & Resorts,
 * Homewood Suites, Home2 Suites, Tru by Hilton, DoubleTree by Hilton,
 * Hampton by Hilton, Hilton Garden Inn, Spark by Hilton).
 *
 * Allowed: target-brand Presentation Image / titles / captions / openings image refs.
 * Forbidden: Brand Status, release, CV, Source Library, Registry, protected 54,
 * Radisson Collection, Four Points Flex, House of Originals, Morgans, all
 * non-target brands. Includes a protected-54 identity preflight that fails
 * loudly if the active universe drifts (count !== 54, Marriott Hotels renamed
 * back to bare "Marriott", or Four Points Flex re-entered Active/Live).
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GALLERY_MIN,
  SCENARIO_MIN,
  PROPERTY_MIN,
  listPresentationRowsLight,
} from "./brand-explorer-lane2-common.js";
import {
  toAirtableFetchableImageUrl,
} from "./brand-explorer-lane2-image-materialization.js";
import {
  isOfficialLifestylePropertyImageUrl,
  isLogoImageUrl,
  isGenericBrandOrLifestyleImageUrl,
} from "./brand-explorer-footprint-opening-image-governance.js";
import {
  pickDistinctImageAssets,
  evaluateImageUniqueness,
  buildImageIdentity,
} from "./brand-explorer-image-uniqueness.js";
import {
  pickRoleMatchedGalleryAssets,
} from "./brand-explorer-gallery-selection.js";
import {
  getWave15SupplementalOpenings,
  isWave15PropertyHoldSlug,
  getWave15CuratedPoolSeed,
} from "./brand-explorer-wave15-image-supplemental.js";
import {
  evaluateBrandImageRoleMatch,
  detectVisualCategory,
  IMAGE_ROLES,
} from "./brand-explorer-image-role-match.js";
import {
  buildOpeningsPropertyCardTitle,
  buildOpeningsPropertyCardBody,
} from "./brand-explorer-openings-property-card-contract.js";
import { BUILT_BLOCKED_PROTECTED_PUBLIC_FULL } from "./brand-explorer-built-blocked-content.js";
import {
  FACTORY_PREVIEW_CANDIDATE_IDENTITIES,
} from "./brand-explorer-factory-preview-candidates.js";
import {
  WAVE15_VERSION,
  WAVE15_STAGE5_APPROVED_SLUGS,
  WAVE15_FORBIDDEN_WRITE_FIELDS,
  WAVE15_IMAGE_MATERIALIZATION_APPLY_FLAGS,
  WAVE15_PROTECTED_BASELINE_COUNT,
  isWave15Stage5Slug,
} from "./brand-explorer-wave15-factory-plan.js";
import { getWave15SourcePack } from "./brand-explorer-wave15-source-packs-content.js";
import { getWave15BrandContent } from "./brand-explorer-wave15-tab-factory-content.js";
import { EXPECTED_ACTIVE_COUNT_54 } from "./brand-explorer-54-active-public-full-baseline.js";
import { loadActiveUniverse } from "./brand-explorer-active-universe.js";

export const WAVE15_IMAGE_MATERIALIZATION_VERSION = "wave15-image-materialization-v1";

// Re-export apply flag list so callers can import from this module too.
export { WAVE15_IMAGE_MATERIALIZATION_APPLY_FLAGS };

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const OPENINGS_SLOT = "footprint.openings";

const FORBIDDEN_WRITE_FIELDS = new Set([
  ...WAVE15_FORBIDDEN_WRITE_FIELDS,
  "Company Validated",
  "Company Validation Date",
  "Founder Visual Review Pass",
  "Active Profile Approved",
  "Ready for Active Profile",
  "Active Profile Approved Date",
  "Brand Status",
]);

/**
 * Slugs that must never be written to during Wave 15 Stage 5. Covers:
 * - Marriott Hotels (protected 54, flagship — do not conflate with Hilton
 *   Hotels & Resorts even though names are similar)
 * - Four Points Flex by Sheraton (held / non-Active — Wave 14 exclusion)
 * - House of Originals / Morgans (retired collections — never rebuild)
 * - Radisson Collection (do not modify per governance)
 * - Every current protected 54 public-full brand identity anchor
 */
const FORBIDDEN_STAGE5_SLUGS = Object.freeze([
  "marriott-hotels",
  "four-points-flex-by-sheraton",
  "house-of-originals",
  "morgans-originals",
  "radisson-collection",
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const FIXTURES = path.join(ROOT, "fixtures");
const REPORTS_DIR = path.join(ROOT, "reports");
const DOCS_DIR = path.join(ROOT, "docs", "data-intelligence");

/**
 * Sibling / wrong-brand imagery rejection regex per Wave 15 slug. Each brand
 * rejects any URL / property key hinting at a sibling Hilton brand (or a
 * confusable non-Hilton brand for the flagship). Brand hint regexes below
 * allow same-brand URLs through even when a sibling token appears in a path.
 */
const SIBLING_RE_BY_SLUG = Object.freeze({
  // Flagship Hilton Hotels & Resorts — reject every non-flagship Hilton sub-brand.
  "hilton-hotels-and-resorts":
    /doubletree|curio|tapestry|signia|conrad|waldorf-astoria|hampton|homewood|home2|home-2|tru-by|tru_|spark-by|spark_|embassy|garden-inn|motto|canopy|tempo|lxr|graduate-by-hilton|nomad|autograph|element-by/i,
  "homewood-suites-by-hilton":
    /home2|home-2|hampton|spark-by|spark_|garden-inn|tru-by|tru_|embassy|doubletree|curio|tapestry|signia|conrad|waldorf-astoria|motto|canopy|tempo|lxr|graduate-by-hilton|hilton-hotels|hilton-orlando|hilton-panama|hilton-cancun|hilton-bogota|hilton-tulum|hilton-mexico|hilton-buenos|hilton-lima|hilton-santiago|hilton-nashville|hilton-miami|hilton-chicago|hilton-atlanta/i,
  "home2-suites-by-hilton":
    /homewood|hampton|spark-by|spark_|tru-by|tru_|embassy|doubletree|curio|tapestry|signia|conrad|waldorf-astoria|motto|canopy|tempo|lxr|graduate-by-hilton|garden-inn|hilton-hotels|hilton-orlando|hilton-panama|hilton-cancun|hilton-bogota|hilton-tulum|hilton-mexico|hilton-buenos|hilton-lima|hilton-santiago|hilton-nashville|hilton-miami|hilton-chicago|hilton-atlanta/i,
  "tru-by-hilton":
    /spark-by|spark_|hampton|home2|home-2|homewood|garden-inn|doubletree|curio|tapestry|signia|conrad|waldorf-astoria|motto|canopy|tempo|lxr|graduate-by-hilton|embassy|hilton-hotels|hilton-orlando|hilton-panama|hilton-cancun|hilton-bogota|hilton-tulum|hilton-mexico|hilton-buenos|hilton-lima|hilton-santiago|hilton-nashville|hilton-miami|hilton-chicago|hilton-atlanta/i,
  "doubletree-by-hilton":
    /curio|tapestry|embassy|signia|waldorf-astoria|conrad|motto|canopy|tempo|lxr|graduate-by-hilton|hilton-hotels|hilton-orlando|hilton-panama|hilton-cancun|hilton-bogota|hilton-tulum|hilton-mexico|hilton-buenos|hilton-lima|hilton-santiago|hilton-nashville|hilton-miami|hilton-chicago|hilton-atlanta|homewood|home2|home-2|hampton|garden-inn|tru-by|tru_|spark-by|spark_/i,
  "hampton-by-hilton":
    /tru-by|tru_|spark-by|spark_|garden-inn|home2|home-2|homewood|doubletree|curio|tapestry|signia|conrad|waldorf-astoria|motto|canopy|tempo|lxr|graduate-by-hilton|embassy|hilton-hotels|hilton-orlando|hilton-panama|hilton-cancun|hilton-bogota|hilton-tulum|hilton-mexico|hilton-buenos|hilton-lima|hilton-santiago|hilton-nashville|hilton-miami|hilton-chicago|hilton-atlanta/i,
  "hilton-garden-inn":
    /hampton|doubletree|homewood|home2|home-2|tru-by|tru_|spark-by|spark_|curio|tapestry|signia|conrad|waldorf-astoria|motto|canopy|tempo|lxr|graduate-by-hilton|embassy|hilton-hotels|hilton-orlando|hilton-panama|hilton-cancun|hilton-bogota|hilton-tulum|hilton-mexico|hilton-buenos|hilton-lima|hilton-santiago|hilton-nashville|hilton-miami|hilton-chicago|hilton-atlanta/i,
  "spark-by-hilton":
    /tru-by|tru_|hampton|home2|home-2|homewood|garden-inn|doubletree|curio|tapestry|signia|conrad|waldorf-astoria|motto|canopy|tempo|lxr|graduate-by-hilton|embassy|hilton-hotels|hilton-orlando|hilton-panama|hilton-cancun|hilton-bogota|hilton-tulum|hilton-mexico|hilton-buenos|hilton-lima|hilton-santiago|hilton-nashville|hilton-miami|hilton-chicago|hilton-atlanta/i,
});

/**
 * Per-slug brand hint regex. Hilton property URLs encode the brand via a
 * short suffix in the hotel code (e.g. `-hh` flagship Hilton, `-hx` Hampton,
 * `-hw` Homewood, `-ht` Home2, `-ru` Tru, `-dt` DoubleTree, `-gi` Garden Inn,
 * `-pe` Spark). CDN paths also embed the code (e.g. hilton.com/im/en/PTYHXHX/).
 * When a URL matches the sibling regex above but ALSO matches its own brand
 * hint, keep the asset (the code is authoritative, not the sibling substring).
 */
const BRAND_HINT_RE_BY_SLUG = Object.freeze({
  "hilton-hotels-and-resorts":
    /hilton\.com\/en\/hotels\/[a-z]{3,5}(?:hh|hf|fhh|chh|bchh)-|\/im\/en\/[a-z0-9]{5,8}(?:hh|hf|fhh|chh|bchh)\/|hilton-panama\b|hilton-cancun\b|hilton-bogota\b|hilton-tulum\b|hilton-mexico\b|hilton-buenos-aires\b|hilton-lima\b|hilton-santiago\b|hilton-orlando\b|hilton-nashville\b|hilton-miami\b|hilton-chicago\b|hilton-atlanta\b|endorsed_hilton|hilton-hotels-resorts|hilton-hotels-and-resorts/i,
  "homewood-suites-by-hilton":
    /hilton\.com\/en\/hotels\/[a-z]{3,5}hw-|\/im\/en\/[a-z0-9]{5,8}hw\/|homewood-suites|homewood-suites-by-hilton|endorsed_homewood/i,
  "home2-suites-by-hilton":
    /hilton\.com\/en\/hotels\/[a-z]{3,5}ht-|\/im\/en\/[a-z0-9]{5,8}ht\/|home2-suites|home-2-suites|home2-suites-by-hilton|endorsed_home2/i,
  "tru-by-hilton":
    /hilton\.com\/en\/hotels\/[a-z]{3,5}ru-|\/im\/en\/[a-z0-9]{5,8}ru\/|(?<!s)tru-by-hilton\b|tru-by-hilton-[a-z]+|endorsed_tru/i,
  "doubletree-by-hilton":
    /hilton\.com\/en\/hotels\/[a-z]{3,5}dt-|\/im\/en\/[a-z0-9]{5,8}dt\/|doubletree-by-hilton|doubletree-hilton|(^|[\/_-])doubletree([\/_-]|$)|endorsed_doubletree/i,
  "hampton-by-hilton":
    /hilton\.com\/en\/hotels\/[a-z]{3,5}hx-|\/im\/en\/[a-z0-9]{5,8}hx\/|hampton-by-hilton|hampton-inn|endorsed_hampton/i,
  "hilton-garden-inn":
    /hilton\.com\/en\/hotels\/[a-z]{3,5}gi-|\/im\/en\/[a-z0-9]{5,8}gi\/|hilton-garden-inn|garden-inn|endorsed_hgi/i,
  "spark-by-hilton":
    /hilton\.com\/en\/hotels\/[a-z]{3,5}pe-|\/im\/en\/[a-z0-9]{5,8}pe\/|spark-by-hilton\b|spark-by-hilton-[a-z]+|endorsed_spark/i,
});

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

export function parseWave15ImageMaterializationFlags(argv = []) {
  const missing = WAVE15_IMAGE_MATERIALIZATION_APPLY_FLAGS.filter((f) => !argv.includes(f));
  return {
    apply: argv.includes("--apply"),
    dryRun: !argv.includes("--apply") || argv.includes("--dry-run"),
    missing,
    ok: argv.includes("--apply") && missing.length === 0,
  };
}

export function resolveWave15ImageIdentity(slug) {
  const s = String(slug || "").trim().toLowerCase();
  if (!isWave15Stage5Slug(s)) return null;
  return FACTORY_PREVIEW_CANDIDATE_IDENTITIES[s] || null;
}

export function loadWave15GalleryPool(slug) {
  const s = String(slug || "").trim().toLowerCase();
  const p = path.join(FIXTURES, `wave15-${s}-gallery-pool.json`);
  let rows = [];
  if (fs.existsSync(p)) {
    try {
      const data = JSON.parse(fs.readFileSync(p, "utf8"));
      rows = Array.isArray(data) ? data : [];
    } catch (err) {
      if (process.env.NODE_ENV !== "production") {
        console.warn(`[wave15-image] failed to read ${p}: ${err.message}`);
      }
    }
  }
  const seed = getWave15CuratedPoolSeed(s);
  if (!seed.length) return rows;
  const seen = new Set(rows.map((r) => String(r.imageUrl || "").toLowerCase()));
  for (const row of seed) {
    const u = String(row.imageUrl || "").toLowerCase();
    if (!u || seen.has(u)) continue;
    seen.add(u);
    rows.push(row);
  }
  return rows;
}

export function isWave15RejectedImageUrl(url, { brandSlug = "" } = {}) {
  const u = nz(url);
  const lower = u.toLowerCase();
  if (!lower) return { rejected: true, reason: "missing_url" };
  if (isLogoImageUrl(lower)) return { rejected: true, reason: "logo" };
  if (/gettyimages|istock|shutterstock|family-at-the-beach|snorkeling|maldives|stays\b|chiclet|learning-hub|portal/i.test(lower)) {
    return { rejected: true, reason: "stock_or_generic_filler" };
  }
  // Reject generic destination / travel-lifestyle imagery that Hilton publishes
  // on shared Stories editor host — these are neither property photography nor
  // brand-hero shots and would fail role match.
  if (
    /(national-park|new-river-gorge|joshua-tree|yellowstone|zion|glacier|acadia|sequoia|yosemite|katrina|photo-credit|shutterstock|stock)/i.test(
      lower
    )
  ) {
    return { rejected: true, reason: "destination_or_stock_lifestyle" };
  }
  if (/digital\.ihg\.com\/is\/image\/ihg\/stays\b/i.test(lower)) {
    return { rejected: true, reason: "ihg_generic_stays_graphic" };
  }
  const sibling = SIBLING_RE_BY_SLUG[brandSlug];
  const hint = BRAND_HINT_RE_BY_SLUG[brandSlug];
  if (sibling && sibling.test(lower) && !(hint && hint.test(lower))) {
    return { rejected: true, reason: "sibling_or_wrong_brand" };
  }
  // Official Hilton brand-page / property-page hosts that carry
  // authoritative brand imagery even when not matched by the lifestyle regex.
  const isWave15BrandHostImage =
    /(?:assets\.)?hiltonstatic\.com\//i.test(lower) ||
    /(?:www\.)?hilton\.com\/im\//i.test(lower) ||
    /stories-editor\.hilton\.com\/wp-content\/uploads\//i.test(lower) ||
    /stories\.hilton\.com\/uploads\//i.test(lower) ||
    /cache\.hilton\.com\//i.test(lower);
  // Stories editor host is shared across all Hilton brands — require the
  // target brand hint to be present in the filename. Anything else on that
  // host is treated as wrong-brand or generic-brand imagery.
  const isStoriesHost = /stories(?:-editor)?\.hilton\.com\/(?:wp-content\/uploads|uploads)\//i.test(
    lower
  );
  if (isStoriesHost) {
    if (!hint || !hint.test(lower)) {
      return { rejected: true, reason: "stories_host_missing_brand_hint" };
    }
  }
  if (!isOfficialLifestylePropertyImageUrl(u) && !isWave15BrandHostImage) {
    return { rejected: true, reason: "not_official_property_cdn" };
  }
  if (isGenericBrandOrLifestyleImageUrl(u)) {
    return { rejected: true, reason: "generic_brand_lifestyle" };
  }
  return { rejected: false, reason: null };
}

function normalizeWave15Pool(rawPool, brandSlug) {
  const accepted = [];
  const rejections = [];
  for (const row of rawPool) {
    const imageUrl = nz(row.imageUrl);
    const gate = isWave15RejectedImageUrl(imageUrl, { brandSlug });
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
      geographyLabel: nz(row.geographyLabel) || "International Reference",
      label: nz(row.label) || "property",
      role: nz(row.role),
      caption: nz(row.caption),
    });
  }
  return { accepted, rejections };
}

function pickDiverseScenarioAssets(accepted, excludeGroupIds = [], minCount = SCENARIO_MIN) {
  const preferredRoles = [
    IMAGE_ROLES.exterior_arrival,
    IMAGE_ROLES.public_space_lobby,
    IMAGE_ROLES.guest_room_suite,
    IMAGE_ROLES.food_beverage_experience,
    IMAGE_ROLES.wellness_pool_spa,
    IMAGE_ROLES.property_setting,
  ];
  const used = [...excludeGroupIds];
  const picked = [];
  for (const role of preferredRoles) {
    if (picked.length >= minCount) break;
    const pool = accepted.filter((asset) => {
      const det = detectVisualCategory({
        imageUrl: asset.imageUrl,
        sourcePageUrl: asset.sourcePageUrl,
        title: asset.propertyName || "",
      });
      if (det.category !== role) return false;
      const id = buildImageIdentity(asset.imageUrl, { propertyName: asset.propertyName });
      return !used.includes(id.duplicateGroupId);
    });
    const d = pickDistinctImageAssets(pool, 1, { excludeGroupIds: used });
    if (!d.length) continue;
    used.push(d[0]._imageIdentity.duplicateGroupId);
    picked.push(d[0]);
  }
  while (picked.length < minCount) {
    const d = pickDistinctImageAssets(accepted, 1, { excludeGroupIds: used });
    if (!d.length) break;
    used.push(d[0]._imageIdentity.duplicateGroupId);
    picked.push(d[0]);
  }
  return picked.slice(0, minCount).map((a, i) => ({
    ...a,
    role: "scenario",
    slotKey: `overview.scenario.${i + 1}`,
    caption: `Owner scenario ${i + 1}`,
    title: `Scenario ${i + 1}${a.marketCity ? ` — ${a.marketCity}` : ""}`,
  }));
}

function propertyCatalogForSlug(slug) {
  // Prefer Wave 15 tab-factory content openings (Stage 4) as authoritative
  // named openings; fall back to source-pack propertyExamples; then merge
  // supplemental Americas / CALA openings.
  let contentOpenings = [];
  try {
    contentOpenings = getWave15BrandContent(slug)?.openings || [];
  } catch {
    contentOpenings = [];
  }
  let sourcePack = null;
  try {
    sourcePack = getWave15SourcePack(slug);
  } catch {
    sourcePack = null;
  }
  const supplemental = getWave15SupplementalOpenings(slug);
  const byKey = new Map();
  const push = (r) => {
    if (!r?.propertyName || !r?.url) return;
    if (!r.matchKey && /steward to match|pending|EMEA conversions/i.test(r.propertyName)) return;
    // Prefer real hilton.com property overview URLs; skip brand-site placeholders.
    const looksLikeHiltonProperty = /hilton\.com\/en\/hotels\/[a-z0-9-]+/i.test(r.url);
    if (!looksLikeHiltonProperty && !isWave15PropertyHoldSlug(slug)) {
      if (/hilton\.com\/en\/brands\//i.test(r.url) || /stories\.hilton\.com/i.test(r.url)) {
        return;
      }
    }
    const propertyKey = String(r.propertyName || r.matchKey || "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-|-$/g, "");
    if (!propertyKey || byKey.has(propertyKey)) return;
    byKey.set(propertyKey, {
      propertyKey,
      propertyName: r.propertyName,
      marketCity:
        r.marketCity ||
        (r.market || "").split(",")[0].trim() ||
        "",
      geographyLabel: r.geographyLabel || "International Reference",
      sourcePageUrl: r.url,
      teaser: r.teaser || r.note || "",
    });
  };
  for (const r of contentOpenings) push(r);
  for (const r of sourcePack?.propertyExamples || []) push(r);
  for (const r of supplemental) push({ ...r, matchKey: r.propertyName });
  // CALA-first ordering: CALA properties before International Reference.
  const rows = [...byKey.values()];
  return [
    ...rows.filter((c) => /^cala/i.test(nz(c.geographyLabel))),
    ...rows.filter((c) => !/^cala/i.test(nz(c.geographyLabel))),
  ];
}

function assignPropertyExampleAssets(rawAssets, catalog, brandName) {
  return rawAssets.map((a, i) => {
    const cat =
      catalog.find((c) => c.propertyKey === a.propertyKey) ||
      catalog.find((c) => nz(c.propertyName).toLowerCase() === nz(a.propertyName).toLowerCase()) ||
      null;
    const propertyName = cat?.propertyName || a.propertyName || `Property ${i + 1}`;
    const market = cat?.marketCity || a.marketCity || "";
    const geo = nz(cat?.geographyLabel || a.geographyLabel || "International Reference");
    const isIntl = !/^cala/i.test(geo);
    const chips = [
      isIntl ? "International Reference" : "CALA",
      market || "City",
      "Property example",
    ].join(", ");
    const locationLine = market ? `${market}${geo ? ` (${geo})` : ""}` : geo;
    const metaLine = isIntl
      ? `International Reference · ${geo}`
      : `CALA · ${geo}`;
    const scenarioLine = chips
      .split(",")
      .map((s) => s.trim().toUpperCase())
      .filter(Boolean)
      .join(" / ");
    const teaser =
      cat?.teaser ||
      a.caption ||
      `${propertyName} is an official ${isIntl ? "International Reference" : "CALA"} property example for ${brandName || "this brand"}. Confirm live affiliation criteria for the specific asset.`;
    const title = buildOpeningsPropertyCardTitle({
      propertyName,
      brandName,
      marketCity: market,
    });
    let body;
    try {
      body = buildOpeningsPropertyCardBody({
        chips,
        locationLine,
        metaLine,
        scenarioLine,
        teaser,
        sourceUrl: cat?.sourcePageUrl || a.sourcePageUrl || "",
      });
    } catch {
      body = `${chips}\n\n${locationLine}\n\n${metaLine}\n\n${scenarioLine}\n\n${teaser}`;
    }
    return {
      ...a,
      role: "property_example",
      slotKey: OPENINGS_SLOT,
      planSlotKey: `footprint.openings.${i + 1}`,
      caption: propertyName,
      title,
      body,
      geographyLabel: geo,
      internationalReference: isIntl,
      caseSummaryOverview: teaser,
      caseSummaryTags: chips,
      caseSummaryBrandRelevance: isIntl
        ? "Official International Reference property photography for Brand Explorer openings — not a CALA operating claim."
        : "Official CALA property photography used as a Brand Explorer property example for this brand.",
      caseSummaryOwnerObjective:
        "Use as a directional property reference when underwriting product fit, capital scope, and platform participation.",
      caseSummaryInterpretation:
        "Confirm live affiliation criteria and property-specific scope with the brand before underwriting.",
      teaser,
      marketCity: market,
      propertyName,
    };
  });
}

export function buildWave15ImageAssetPackForBrand(brandSlug) {
  const identity = resolveWave15ImageIdentity(brandSlug);
  if (!identity) {
    return {
      brandSlug,
      status: "refused",
      pass: false,
      blockers: ["not_wave15_target"],
    };
  }
  if (BUILT_BLOCKED_PROTECTED_PUBLIC_FULL.includes(identity.slug)) {
    return {
      brandSlug: identity.slug,
      status: "refused",
      pass: false,
      blockers: ["protected_public_full_brand"],
    };
  }
  if (FORBIDDEN_STAGE5_SLUGS.includes(identity.slug)) {
    return {
      brandSlug: identity.slug,
      status: "refused",
      pass: false,
      blockers: ["forbidden_stage5_target"],
    };
  }

  const slug = identity.slug;
  const rawPool = loadWave15GalleryPool(slug);
  const { accepted, rejections } = normalizeWave15Pool(rawPool, slug);
  const blockers = [];
  const propertyHeldSlug = isWave15PropertyHoldSlug(slug);
  const thinPool = accepted.length < GALLERY_MIN + SCENARIO_MIN + PROPERTY_MIN;

  const catalog = propertyCatalogForSlug(slug);
  const catalogOrdered = catalog; // already CALA-first from propertyCatalogForSlug

  let scenarioPack = [];
  let galleryPack = [];
  let galleryPick = { assets: [], inventedRoleCaptions: 0 };
  let propertyDistinct = [];

  if (thinPool || propertyHeldSlug) {
    scenarioPack = pickDiverseScenarioAssets(accepted, [], SCENARIO_MIN);
    const scenarioGroupIds = scenarioPack
      .map((a) => a._imageIdentity?.duplicateGroupId || buildImageIdentity(a.imageUrl).duplicateGroupId)
      .filter(Boolean);

    const propertyUsedGroups = [...scenarioGroupIds];
    if (!propertyHeldSlug) {
      for (const cat of catalogOrdered) {
        if (propertyDistinct.length >= PROPERTY_MIN) break;
        const pool = accepted.filter((a) => {
          const name = nz(a.propertyName).toLowerCase();
          const key = nz(a.propertyKey);
          return (
            (name && name === nz(cat.propertyName).toLowerCase()) ||
            (key && key === cat.propertyKey) ||
            (name && name.includes(nz(cat.propertyName).toLowerCase().slice(0, 18)))
          );
        });
        const d = pickDistinctImageAssets(pool, 1, { excludeGroupIds: propertyUsedGroups });
        if (!d.length) continue;
        propertyUsedGroups.push(d[0]._imageIdentity.duplicateGroupId);
        propertyDistinct.push({
          ...d[0],
          propertyName: cat.propertyName || d[0].propertyName,
          propertyKey: cat.propertyKey || d[0].propertyKey,
          marketCity: cat.marketCity || d[0].marketCity,
          geographyLabel: cat.geographyLabel || d[0].geographyLabel,
        });
      }
      if (propertyDistinct.length < PROPERTY_MIN) {
        const fill = pickDistinctImageAssets(accepted, PROPERTY_MIN, {
          excludeGroupIds: propertyUsedGroups,
        });
        for (const asset of fill) {
          if (propertyDistinct.length >= PROPERTY_MIN) break;
          propertyUsedGroups.push(asset._imageIdentity.duplicateGroupId);
          propertyDistinct.push(asset);
        }
      }
    }

    const excludeForGallery = [
      ...scenarioGroupIds,
      ...propertyDistinct.map((a) => a._imageIdentity?.duplicateGroupId).filter(Boolean),
    ];
    const galleryPool = accepted.filter((a) => {
      const id = buildImageIdentity(a.imageUrl, { propertyName: a.propertyName });
      return !excludeForGallery.includes(id.duplicateGroupId);
    });
    galleryPick = pickRoleMatchedGalleryAssets(galleryPool, GALLERY_MIN);
    galleryPack = galleryPick.assets;
  } else {
    galleryPick = pickRoleMatchedGalleryAssets(accepted, GALLERY_MIN);
    galleryPack = galleryPick.assets;
    const galleryGroupIds = galleryPack
      .map((a) => a._imageIdentity?.duplicateGroupId || buildImageIdentity(a.imageUrl).duplicateGroupId)
      .filter(Boolean);

    const propertyUsedGroups = [...galleryGroupIds];
    for (const cat of catalogOrdered) {
      if (propertyDistinct.length >= PROPERTY_MIN) break;
      const pool = accepted.filter((a) => {
        const name = nz(a.propertyName).toLowerCase();
        const key = nz(a.propertyKey);
        return (
          (name && name === nz(cat.propertyName).toLowerCase()) ||
          (key && key === cat.propertyKey) ||
          (name && name.includes(nz(cat.propertyName).toLowerCase().slice(0, 18)))
        );
      });
      const d = pickDistinctImageAssets(pool, 1, { excludeGroupIds: propertyUsedGroups });
      if (!d.length) continue;
      propertyUsedGroups.push(d[0]._imageIdentity.duplicateGroupId);
      propertyDistinct.push({
        ...d[0],
        propertyName: cat.propertyName || d[0].propertyName,
        propertyKey: cat.propertyKey || d[0].propertyKey,
        marketCity: cat.marketCity || d[0].marketCity,
        geographyLabel: cat.geographyLabel || d[0].geographyLabel,
      });
    }
    if (propertyDistinct.length < PROPERTY_MIN) {
      const fill = pickDistinctImageAssets(accepted, PROPERTY_MIN, {
        excludeGroupIds: propertyUsedGroups,
      });
      for (const asset of fill) {
        if (propertyDistinct.length >= PROPERTY_MIN) break;
        propertyUsedGroups.push(asset._imageIdentity.duplicateGroupId);
        propertyDistinct.push(asset);
      }
    }

    const usedGroups = [
      ...galleryGroupIds,
      ...propertyDistinct.map((a) => a._imageIdentity?.duplicateGroupId).filter(Boolean),
    ];
    scenarioPack = pickDiverseScenarioAssets(accepted, usedGroups, SCENARIO_MIN);
    if (scenarioPack.length < SCENARIO_MIN) {
      scenarioPack = pickDiverseScenarioAssets(accepted, galleryGroupIds, SCENARIO_MIN);
    }
  }

  const propertyPack = assignPropertyExampleAssets(
    propertyDistinct.slice(0, PROPERTY_MIN),
    catalog,
    identity.name
  );

  const galleryShortfallDocumented =
    propertyHeldSlug && galleryPack.length < GALLERY_MIN && scenarioPack.length >= SCENARIO_MIN;
  if (galleryPack.length < GALLERY_MIN && !galleryShortfallDocumented) {
    blockers.push(`gallery_distinct_${galleryPack.length}_lt_${GALLERY_MIN}`);
  }
  if (galleryPick.inventedRoleCaptions > 0) {
    blockers.push(
      `gallery_role_uncurable_without_metadata_${galleryPick.inventedRoleCaptions}`
    );
  }
  if (scenarioPack.length < SCENARIO_MIN) {
    blockers.push(`scenario_distinct_${scenarioPack.length}_lt_${SCENARIO_MIN}`);
  }
  const propertyHeld =
    propertyHeldSlug && propertyPack.length < PROPERTY_MIN;
  if (propertyPack.length < PROPERTY_MIN && !propertyHeld) {
    blockers.push(`property_distinct_${propertyPack.length}_lt_${PROPERTY_MIN}`);
  }

  const presentationRows = [
    ...galleryPack.map((a) => ({
      slotKey: a.slotKey,
      title: a.title,
      imageUrl: a.imageUrl,
      recordId: a.slotKey,
    })),
    ...scenarioPack.map((a) => ({
      slotKey: a.slotKey,
      title: a.title,
      imageUrl: a.imageUrl,
      recordId: a.slotKey,
    })),
    ...propertyPack.map((a) => ({
      slotKey: a.slotKey,
      title: a.title,
      imageUrl: a.imageUrl,
      propertyName: a.propertyName,
      recordId: a.planSlotKey,
    })),
  ];

  const uniqueness = evaluateImageUniqueness({ brandSlug: slug, presentationRows });
  const roleMatch = evaluateBrandImageRoleMatch({ brandSlug: slug, presentationRows });

  const uniquenessFindings = uniqueness?.findings || [];
  const allowedShortfallIds = new Set();
  if (galleryShortfallDocumented) {
    allowedShortfallIds.add("gallery_slot_count_short");
    allowedShortfallIds.add("gallery_distinct_short");
  }
  if (propertyHeld) {
    allowedShortfallIds.add("property_distinct_short");
  }
  const uniquenessHardFails = uniquenessFindings.filter(
    (f) => f.status === "fail" && !allowedShortfallIds.has(f.id)
  );
  const uniquenessPass =
    uniquenessHardFails.length === 0 &&
    (uniqueness?.pass === true ||
      (allowedShortfallIds.size > 0 &&
        uniquenessFindings.every(
          (f) => f.status !== "fail" || allowedShortfallIds.has(f.id)
        )));

  const roleMatchPass =
    roleMatch?.pass === true ||
    (galleryShortfallDocumented &&
      (roleMatch?.unresolvedRoleMismatchCount || 0) === 0 &&
      (roleMatch?.galleryCount || 0) === galleryPack.length &&
      galleryPack.length > 0);

  if (!uniquenessPass) blockers.push("image_uniqueness_failed");
  if (!roleMatchPass) blockers.push("image_role_match_failed");

  const pass = blockers.length === 0;
  return {
    brandSlug: slug,
    reportSlug: slug,
    brandName: identity.name,
    recordId: identity.recordId,
    status: pass ? "asset_pack_ready" : "blocked_missing_images",
    pass,
    blockers,
    poolStats: {
      fixtureRows: rawPool.length,
      acceptedRows: accepted.length,
      rejectedRows: rejections.length,
      rejectionSample: rejections.slice(0, 8),
    },
    counts: {
      gallery: galleryPack.length,
      property: propertyPack.length,
      scenario: scenarioPack.length,
    },
    propertyHeld: propertyHeld
      ? {
          held: true,
          reason:
            "No hold-designated Wave 15 slug should trigger this — investigate: named property overview URL not steward-matched (openings/property images cleanly unavailable).",
        }
      : null,
    galleryShortfall: galleryShortfallDocumented
      ? {
          held: true,
          count: galleryPack.length,
          required: GALLERY_MIN,
          reason:
            "Documented gallery shortfall (Wave 15 hold slug) — scenario images prioritized; remaining gallery slots cleanly unavailable.",
        }
      : null,
    calaFirstOpenings: {
      calaCount: propertyPack.filter((p) => !p.internationalReference).length,
      intlCount: propertyPack.filter((p) => p.internationalReference).length,
      labels: propertyPack.map((p) => ({
        propertyName: p.propertyName,
        geographyLabel: p.geographyLabel,
        internationalReference: !!p.internationalReference,
      })),
    },
    visualAssetPack: {
      galleryCandidates: galleryPack,
      propertyExampleCandidates: propertyPack,
      scenarioCandidates: scenarioPack,
    },
    uniqueness,
    roleMatch,
    eligibility: {
      asset_pack_ready: pass,
      materialization_allowed: pass,
    },
  };
}

/**
 * Protected 54 identity preflight — read-only. Fails loudly (with
 * stopRecommended) if the live Active/Live universe has drifted:
 *   - totalCount !== 54
 *   - marriott-hotels brand missing OR renamed back to bare "Marriott"
 *     (must match /Marriott Hotels/i to prove Stage 4.5 rename held)
 *   - four-points-flex-by-sheraton re-entered Active/Live (must remain held)
 */
export async function runWave15ProtectedFiftyFourIdentityPreflight() {
  const issues = [];
  let universe;
  try {
    universe = await loadActiveUniverse({ includeDetails: false });
  } catch (err) {
    return {
      pass: false,
      stopRecommended: true,
      generatedAt: new Date().toISOString(),
      expectedActiveCount: EXPECTED_ACTIVE_COUNT_54,
      universeLoadError: String(err?.message || err),
      issues: [`active_universe_load_failed:${String(err?.message || err)}`],
      marriottHotels: null,
      fourPointsFlexPresent: null,
      liveActiveCount: null,
    };
  }
  const brands = universe?.brands || [];
  const liveActiveCount = universe?.totalCount ?? brands.length;

  if (liveActiveCount !== EXPECTED_ACTIVE_COUNT_54) {
    issues.push(
      `active_universe_count_drift:got=${liveActiveCount}_expected=${EXPECTED_ACTIVE_COUNT_54}`
    );
  }

  const marriottHotelsAnchorRecordId = "recn59UtkyyoYwzSz";
  const marriottHotelsBrand =
    brands.find((b) => b.recordId === marriottHotelsAnchorRecordId) ||
    brands.find((b) => nz(b.slug).toLowerCase() === "marriott-hotels") ||
    null;
  const marriottHotelsNameOk =
    !!marriottHotelsBrand && /marriott\s+hotels/i.test(nz(marriottHotelsBrand.name));
  if (!marriottHotelsBrand) {
    issues.push("marriott_hotels_brand_missing_from_active_universe");
  } else if (!marriottHotelsNameOk) {
    issues.push(
      `marriott_hotels_name_drift:got="${nz(marriottHotelsBrand.name)}"_expected_matches_/Marriott Hotels/i`
    );
  }

  const fourPointsFlex = brands.find(
    (b) => nz(b.slug).toLowerCase() === "four-points-flex-by-sheraton"
  );
  if (fourPointsFlex) {
    issues.push(
      `four_points_flex_in_active_universe:recordId=${fourPointsFlex.recordId}_status=${nz(fourPointsFlex.status)}`
    );
  }

  const pass = issues.length === 0;
  return {
    pass,
    stopRecommended: !pass,
    generatedAt: new Date().toISOString(),
    expectedActiveCount: EXPECTED_ACTIVE_COUNT_54,
    liveActiveCount,
    marriottHotels: marriottHotelsBrand
      ? {
          recordId: marriottHotelsBrand.recordId,
          name: nz(marriottHotelsBrand.name),
          matchesMarriottHotelsName: marriottHotelsNameOk,
        }
      : null,
    fourPointsFlexPresent: !!fourPointsFlex,
    issues,
  };
}

function findPresentationRow(
  rows,
  { slotKey, title, planSlotKey, propertyName },
  { usedRecordIds = new Set() } = {}
) {
  const available = (rows || []).filter((r) => !usedRecordIds.has(r.recordId));
  if (slotKey === OPENINGS_SLOT) {
    const openingsAll = (rows || []).filter((r) => r.slotKey === OPENINGS_SLOT);
    const openings = openingsAll.filter((r) => !usedRecordIds.has(r.recordId));
    const prop = nz(propertyName) || nz(title).split("—")[0].trim();
    if (prop) {
      const propLower = prop.toLowerCase();
      const exact = openings.find((r) => nz(r.title).toLowerCase() === nz(title).toLowerCase());
      if (exact) return exact;
      const byProp = openings.find((r) => nz(r.title).toLowerCase().includes(propLower));
      if (byProp) return byProp;
      const tokens = propLower.split(/[^a-z0-9]+/).filter((t) => t.length > 3);
      const byTokens = openings.find((r) => {
        const t = nz(r.title).toLowerCase();
        return tokens.filter((tok) => t.includes(tok)).length >= Math.min(2, tokens.length);
      });
      if (byTokens) return byTokens;
    }
    const idx = Number(String(planSlotKey || "").match(/(\d+)$/)?.[1] || 0) - 1;
    if (idx >= 0) {
      for (let i = idx; i < openingsAll.length; i++) {
        if (!usedRecordIds.has(openingsAll[i].recordId)) return openingsAll[i];
      }
      for (const row of openingsAll) {
        if (!usedRecordIds.has(row.recordId)) return row;
      }
    }
    return openings[0] || null;
  }
  return available.find((r) => r.slotKey === slotKey) || null;
}

function buildImageOnlyPatch({ asset, row, brandName }) {
  const fields = {
    Image: [{ url: toAirtableFetchableImageUrl(asset.imageUrl) }],
  };
  if (asset.slotKey?.startsWith("materials.gallery.") && asset.title) {
    fields.Title = nz(asset.title);
    if (asset.caption) fields.Body = nz(asset.caption);
  } else if (!row?.recordId) {
    if (asset.title) fields.Title = nz(asset.title);
    if (asset.body) fields.Body = nz(asset.body);
    if (asset.caption && asset.slotKey?.startsWith("materials.gallery.")) {
      fields.Body = nz(asset.caption);
    }
  } else if (asset.slotKey === OPENINGS_SLOT) {
    if (asset.internationalReference && asset.title) fields.Title = nz(asset.title);
    if (asset.internationalReference && asset.body) fields.Body = nz(asset.body);
  } else if (asset.slotKey?.startsWith("overview.scenario.") && !nz(row?.title) && asset.title) {
    fields.Title = nz(asset.title);
  }

  if ((asset.slotKey || OPENINGS_SLOT) === OPENINGS_SLOT) {
    const overview =
      nz(asset.caseSummaryOverview) || nz(asset.teaser) || nz(asset.caption) || nz(asset.propertyName);
    const tags = nz(asset.caseSummaryTags) || nz(asset.marketCity) || "Property example";
    if (overview) fields["Case Summary Overview"] = overview;
    if (tags) fields["Case Summary Tags"] = tags;
    if (nz(asset.caseSummaryBrandRelevance)) {
      fields["Case Summary Brand Relevance"] = nz(asset.caseSummaryBrandRelevance);
    } else if (overview) {
      fields["Case Summary Brand Relevance"] =
        "Official property photography used as a Brand Explorer property example for this brand.";
    }
    if (nz(asset.caseSummaryOwnerObjective)) {
      fields["Case Summary Owner Objective"] = nz(asset.caseSummaryOwnerObjective);
    } else if (overview) {
      fields["Case Summary Owner Objective"] =
        "Use as a directional property reference when underwriting design intensity, capital scope, and platform fit.";
    }
    if (nz(asset.caseSummaryInterpretation)) {
      fields["Case Summary Interpretation"] = nz(asset.caseSummaryInterpretation);
    } else if (overview) {
      fields["Case Summary Interpretation"] =
        "Confirm live affiliation criteria and property-specific scope with the brand before underwriting.";
    }
  }

  for (const forbidden of FORBIDDEN_WRITE_FIELDS) {
    if (fields[forbidden] != null) delete fields[forbidden];
  }

  return {
    recordId: row?.recordId || null,
    slotKey: asset.slotKey || OPENINGS_SLOT,
    planSlotKey: asset.planSlotKey || asset.slotKey,
    createIfMissing: !row?.recordId,
    fields,
    imageUrl: nz(asset.imageUrl),
    sourcePageUrl: nz(asset.sourcePageUrl),
    propertyName: nz(asset.propertyName),
    brandName: nz(brandName),
  };
}

export async function planWave15BrandImageMaterialization(brandSlug, { assetPackBrand } = {}) {
  const identity = resolveWave15ImageIdentity(brandSlug);
  if (!identity) {
    return { brandSlug, blocked: true, blockers: ["not_wave15_target"], presentationPatches: [] };
  }
  if (BUILT_BLOCKED_PROTECTED_PUBLIC_FULL.includes(identity.slug)) {
    return {
      brandSlug: identity.slug,
      blocked: true,
      blockers: ["protected_public_full_brand"],
      presentationPatches: [],
    };
  }
  if (FORBIDDEN_STAGE5_SLUGS.includes(identity.slug)) {
    return {
      brandSlug: identity.slug,
      blocked: true,
      blockers: ["forbidden_stage5_target"],
      presentationPatches: [],
    };
  }

  const packRow = assetPackBrand || buildWave15ImageAssetPackForBrand(identity.slug);
  if (packRow.pass !== true || packRow.status !== "asset_pack_ready") {
    return {
      brandSlug: identity.slug,
      brandName: identity.name,
      recordId: identity.recordId,
      blocked: true,
      blockers: packRow.blockers || ["asset_pack_not_ready"],
      presentationPatches: [],
      assetPack: packRow,
    };
  }

  let presentationRows = [];
  try {
    const fetchRes = await listPresentationRowsLight(identity.recordId, identity.name);
    presentationRows = fetchRes.rows || [];
  } catch (err) {
    return {
      brandSlug: identity.slug,
      blocked: true,
      blockers: [`presentation_fetch_error:${err.message}`],
      presentationPatches: [],
    };
  }

  const visual = packRow.visualAssetPack || {};
  const assets = [
    ...(visual.galleryCandidates || []).map((a) => ({ ...a, kind: "gallery" })),
    ...(visual.scenarioCandidates || []).map((a) => ({ ...a, kind: "scenario" })),
    ...(visual.propertyExampleCandidates || []).map((a) => ({ ...a, kind: "property" })),
  ];

  const blockers = [];
  const patches = [];
  const usedRecordIds = new Set();

  for (const asset of assets) {
    const gate = isWave15RejectedImageUrl(asset.imageUrl, { brandSlug: identity.slug });
    if (gate.rejected) {
      blockers.push(`${asset.slotKey || asset.planSlotKey}:${gate.reason}`);
      continue;
    }
    const row = findPresentationRow(presentationRows, asset, { usedRecordIds });
    if (row?.recordId) usedRecordIds.add(row.recordId);
    const allowCreate =
      String(asset.slotKey || "").startsWith("materials.gallery.") ||
      asset.slotKey === OPENINGS_SLOT;
    if (!row?.recordId && !allowCreate) {
      blockers.push(`${asset.slotKey || asset.planSlotKey}:missing_presentation_row`);
      continue;
    }
    patches.push(buildImageOnlyPatch({ asset, row, brandName: identity.name }));
  }

  const galleryCount = patches.filter((p) => String(p.slotKey).startsWith("materials.gallery.")).length;
  const scenarioCount = patches.filter((p) => String(p.slotKey).startsWith("overview.scenario.")).length;
  const propertyCount = patches.filter((p) => p.slotKey === OPENINGS_SLOT).length;
  const propertyHeldOk = isWave15PropertyHoldSlug(identity.slug) && propertyCount === 0;
  const galleryShortfallOk =
    isWave15PropertyHoldSlug(identity.slug) &&
    galleryCount < GALLERY_MIN &&
    scenarioCount >= SCENARIO_MIN &&
    galleryCount >= 0;
  if (galleryCount < GALLERY_MIN && !galleryShortfallOk) {
    blockers.push(`gallery_patches_${galleryCount}_lt_${GALLERY_MIN}`);
  }
  if (scenarioCount < SCENARIO_MIN) blockers.push(`scenario_patches_${scenarioCount}_lt_${SCENARIO_MIN}`);
  if (propertyCount < PROPERTY_MIN && !propertyHeldOk) {
    blockers.push(`property_patches_${propertyCount}_lt_${PROPERTY_MIN}`);
  }

  return {
    brandSlug: identity.slug,
    brandName: identity.name,
    recordId: identity.recordId,
    blocked: blockers.length > 0,
    blockers,
    presentationPatches: patches,
    counts: { gallery: galleryCount, scenario: scenarioCount, property: propertyCount },
    assetPack: packRow,
    calaFirstOpenings: packRow.calaFirstOpenings,
    guardrails: {
      presentationImageOnly: true,
      registryWrites: false,
      sourceLibraryWrites: false,
      releaseFieldWrites: false,
      companyValidatedChanges: false,
      brandStatusChanges: false,
      protected54Changes: false,
      marriottHotelsWrites: false,
      fourPointsFlexWrites: false,
      houseOfOriginalsWrites: false,
      morgansOriginalsWrites: false,
      radissonCollectionChanges: false,
    },
  };
}

async function airtablePresentationWrite({ baseId, apiKey, recordId, fields, method }) {
  const url =
    method === "POST"
      ? `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}`
      : `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}/${recordId}`;
  const res = await fetch(url, {
    method,
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `${method} failed: ${res.status}`);
  return json;
}

function buildPresentationWriteFields(patch, identity) {
  const sortOrder =
    patch.slotKey === OPENINGS_SLOT
      ? 10
      : Number(String(patch.planSlotKey || patch.slotKey).match(/(\d+)$/)?.[1] || 0) || 0;
  return {
    "Slot Key": patch.slotKey,
    "Brand Name": identity.name,
    Brand: [identity.recordId],
    Active: true,
    "Sort Order": sortOrder,
    ...patch.fields,
  };
}

export async function applyWave15ImageMaterializationPlans({
  brandResults,
  apply = false,
  argv = [],
} = {}) {
  const flagCheck = parseWave15ImageMaterializationFlags(argv);
  if (!apply) {
    return { applied: false, reason: "dry_run_only", flagCheck };
  }
  if (!flagCheck.ok) {
    return { applied: false, reason: "missing_apply_flags", missing: flagCheck.missing };
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const resultsByBrand = {};
  for (const brand of brandResults) {
    if (!isWave15Stage5Slug(brand.brandSlug)) {
      throw new Error(`Refuse write to non-Wave15 brand ${brand.brandSlug}`);
    }
    if (FORBIDDEN_STAGE5_SLUGS.includes(brand.brandSlug)) {
      throw new Error(`Refuse write to forbidden Stage 5 target brand ${brand.brandSlug}`);
    }
    if (BUILT_BLOCKED_PROTECTED_PUBLIC_FULL.includes(brand.brandSlug)) {
      throw new Error(`Refuse write to protected public-full brand ${brand.brandSlug}`);
    }
    if (brand.blocked) {
      resultsByBrand[brand.brandSlug] = {
        applied: false,
        reason: "materialization_blocked",
        blockers: brand.blockers,
      };
      continue;
    }

    const identity = resolveWave15ImageIdentity(brand.brandSlug);
    if (!identity?.recordId || !identity?.name) {
      resultsByBrand[brand.brandSlug] = { applied: false, reason: "missing_brand_identity" };
      continue;
    }

    const results = { presentationCreated: [], presentationUpdated: [], errors: [] };
    for (const patch of brand.presentationPatches || []) {
      const fields = buildPresentationWriteFields(patch, identity);
      for (const forbidden of FORBIDDEN_WRITE_FIELDS) {
        if (fields[forbidden] != null) delete fields[forbidden];
      }
      try {
        let recordId = patch.recordId;
        if (recordId) {
          const meta = { ...fields };
          delete meta.Image;
          if (Object.keys(meta).length) {
            await airtablePresentationWrite({
              baseId,
              apiKey,
              recordId,
              fields: meta,
              method: "PATCH",
            });
            await sleep(280);
          }
          if (fields.Image) {
            await airtablePresentationWrite({
              baseId,
              apiKey,
              recordId,
              fields: { Image: fields.Image },
              method: "PATCH",
            });
          }
          results.presentationUpdated.push({
            recordId,
            slotKey: patch.slotKey,
            planSlotKey: patch.planSlotKey,
          });
        } else if (patch.createIfMissing) {
          const createFields = { ...fields };
          delete createFields.Image;
          const created = await airtablePresentationWrite({
            baseId,
            apiKey,
            fields: createFields,
            method: "POST",
          });
          recordId = created.id;
          await sleep(280);
          if (fields.Image) {
            await airtablePresentationWrite({
              baseId,
              apiKey,
              recordId,
              fields: { Image: fields.Image },
              method: "PATCH",
            });
          }
          results.presentationCreated.push({
            recordId,
            slotKey: patch.slotKey,
            planSlotKey: patch.planSlotKey,
          });
        } else {
          results.errors.push({
            slotKey: patch.slotKey,
            error: "missing_row_create_not_allowed",
          });
        }
        await sleep(280);
      } catch (err) {
        results.errors.push({
          slotKey: patch.slotKey,
          planSlotKey: patch.planSlotKey,
          error: String(err.message || err),
        });
      }
    }
    resultsByBrand[brand.brandSlug] = {
      applied: results.errors.length === 0,
      ...results,
    };
  }

  return { applied: true, flagCheck, resultsByBrand };
}

function writeReports(basename, json, md) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  const jsonPath = path.join(REPORTS_DIR, `${basename}.json`);
  const mdPath = path.join(REPORTS_DIR, `${basename}.md`);
  fs.writeFileSync(jsonPath, `${JSON.stringify(json, null, 2)}\n`, "utf8");
  fs.writeFileSync(mdPath, md.endsWith("\n") ? md : `${md}\n`, "utf8");
  return { jsonPath, mdPath };
}

function brandMd(plan) {
  const lines = [
    `# Wave 15 image materialization — ${plan.brandName || plan.brandSlug}`,
    ``,
    `- Status: **${plan.blocked ? "blocked" : "ready"}**`,
    `- Record: \`${plan.recordId || ""}\``,
    `- Patches: gallery ${plan.counts?.gallery ?? 0}/6 · scenario ${plan.counts?.scenario ?? 0}/3 · openings ${plan.counts?.property ?? 0}/3`,
    ``,
  ];
  if (plan.blockers?.length) {
    lines.push(`## Blockers`, ``, ...plan.blockers.map((b) => `- ${b}`), ``);
  }
  if (plan.calaFirstOpenings?.labels?.length) {
    lines.push(`## Openings geography`, ``);
    for (const l of plan.calaFirstOpenings.labels) {
      lines.push(
        `- ${l.propertyName}: ${l.geographyLabel}${l.internationalReference ? " (International Reference)" : ""}`
      );
    }
    lines.push(``);
  }
  lines.push(`## Patches`, ``);
  for (const p of plan.presentationPatches || []) {
    lines.push(
      `- \`${p.slotKey}\`${p.planSlotKey && p.planSlotKey !== p.slotKey ? ` (${p.planSlotKey})` : ""} → ${p.recordId ? "PATCH" : "CREATE"} · ${p.imageUrl}`
    );
  }
  return lines.join("\n");
}

export async function runWave15ImageMaterialization({ dryRun = true, argv = [], brands = null } = {}) {
  const flagCheck = parseWave15ImageMaterializationFlags(argv);
  const apply = argv.includes("--apply") && !dryRun;

  // Mandatory protected-54 identity preflight — stop on drift for both
  // dry-run and apply. Prevents Stage 5 activity if the frozen universe
  // shifts (count drift, Marriott Hotels name reverted, Flex re-added).
  const identityPreflight = await runWave15ProtectedFiftyFourIdentityPreflight();
  if (!identityPreflight.pass) {
    const summary = {
      version: WAVE15_IMAGE_MATERIALIZATION_VERSION,
      wave15Version: WAVE15_VERSION,
      stage: "image-materialization",
      generatedAt: new Date().toISOString(),
      dryRun: true,
      applyRequested: argv.includes("--apply"),
      applyBlocked: true,
      pass: false,
      stopRecommended: true,
      flagCheck,
      protectedBaselineCount: WAVE15_PROTECTED_BASELINE_COUNT,
      expectedActiveCount: EXPECTED_ACTIVE_COUNT_54,
      identityPreflight,
      counts: { brands: 0, ready: 0, blocked: 0, patches: 0 },
      brands: [],
      applyResult: null,
      readyStatement: "wave15_stage5_blocked_protected_54_identity_preflight_failed",
      guardrails: {
        noBrandStatus: true,
        noReleaseFields: true,
        noCompanyValidated: true,
        noSourceLibrary: true,
        noRegistry: true,
        noProtected54: true,
        targetBrandsOnly: true,
      },
    };
    writeReports("brand-explorer-wave15-image-materialization", summary,
      [
        `# Wave 15 Stage 5 — Image / Visual Materialization`,
        ``,
        `- Generated: ${summary.generatedAt}`,
        `- Mode: **BLOCKED (identity preflight)**`,
        `- Ready: **${summary.readyStatement}**`,
        ``,
        `## Identity preflight issues`,
        ``,
        ...identityPreflight.issues.map((i) => `- ${i}`),
        ``,
      ].join("\n")
    );
    return summary;
  }

  const brandPlans = [];
  const targetSlugs = (brands?.length ? brands : [...WAVE15_STAGE5_APPROVED_SLUGS])
    .map((s) => String(s || "").trim().toLowerCase())
    .filter((s) => WAVE15_STAGE5_APPROVED_SLUGS.includes(s) && !FORBIDDEN_STAGE5_SLUGS.includes(s));

  for (const slug of targetSlugs) {
    const pack = buildWave15ImageAssetPackForBrand(slug);
    const plan = await planWave15BrandImageMaterialization(slug, { assetPackBrand: pack });
    brandPlans.push(plan);
    writeReports(`brand-explorer-wave15-image-materialization-${slug}`, plan, brandMd(plan));
  }

  const applyResult = await applyWave15ImageMaterializationPlans({
    brandResults: brandPlans,
    apply,
    argv,
  });

  const ready = brandPlans.filter((b) => !b.blocked).length;
  const blocked = brandPlans.filter((b) => b.blocked).length;
  const summary = {
    version: WAVE15_IMAGE_MATERIALIZATION_VERSION,
    wave15Version: WAVE15_VERSION,
    stage: "image-materialization",
    generatedAt: new Date().toISOString(),
    dryRun: !apply,
    applyRequested: argv.includes("--apply"),
    flagCheck,
    identityPreflight,
    protectedBaselineCount: WAVE15_PROTECTED_BASELINE_COUNT,
    expectedActiveCount: EXPECTED_ACTIVE_COUNT_54,
    counts: {
      brands: brandPlans.length,
      ready,
      blocked,
      patches: brandPlans.reduce((n, b) => n + (b.presentationPatches?.length || 0), 0),
    },
    brands: brandPlans.map((b) => ({
      brandSlug: b.brandSlug,
      brandName: b.brandName,
      blocked: b.blocked,
      blockers: b.blockers,
      counts: b.counts,
      calaFirstOpenings: b.calaFirstOpenings,
      apply: applyResult.resultsByBrand?.[b.brandSlug] || null,
    })),
    applyResult,
    readyStatement: apply
      ? "wave15_image_materialization_ready_for_post_image_cleanup"
      : "wave15_stage5_image_materialization_dry_run_ready",
    guardrails: {
      noBrandStatus: true,
      noReleaseFields: true,
      noCompanyValidated: true,
      noSourceLibrary: true,
      noRegistry: true,
      noProtected54: true,
      noMarriottHotelsWrites: true,
      noFourPointsFlexWrites: true,
      noHouseOfOriginalsWrites: true,
      noMorgansOriginalsWrites: true,
      noRadissonCollectionChanges: true,
      targetBrandsOnly: true,
      imageUniquenessRequired: true,
      imageRoleMatchRequired: true,
      calaFirstOpenings: true,
      americasReferenceBeforeInternationalReference: true,
      hiltonBrandFamilySeparated: true,
    },
  };

  const md = [
    `# Wave 15 Stage 5 — Image / Visual Materialization`,
    ``,
    `- Generated: ${summary.generatedAt}`,
    `- Mode: **${apply ? "APPLY" : "DRY-RUN"}**`,
    `- Ready: **${ready}/${brandPlans.length}** · Blocked: **${blocked}**`,
    `- Patches planned: **${summary.counts.patches}**`,
    `- Protected 54 identity preflight: **${identityPreflight.pass ? "PASS" : "FAIL"}**`,
    ``,
    `## Brand results`,
    ``,
    ...brandPlans.map(
      (b) =>
        `- **${b.brandName || b.brandSlug}**: ${b.blocked ? `BLOCKED (${(b.blockers || []).join(", ")})` : `ready · g${b.counts?.gallery}/6 s${b.counts?.scenario}/3 o${b.counts?.property}/3`}`
    ),
    ``,
    `## Apply flags`,
    ``,
    ...WAVE15_IMAGE_MATERIALIZATION_APPLY_FLAGS.map((f) => `- \`${f}\``),
    ``,
  ].join("\n");

  writeReports("brand-explorer-wave15-image-materialization", summary, md);

  fs.mkdirSync(DOCS_DIR, { recursive: true });
  const docsPath = path.join(DOCS_DIR, "brand-explorer-wave15-image-materialization.md");
  fs.writeFileSync(
    docsPath,
    [
      `# Wave 15 — Image / Visual Materialization`,
      ``,
      `Stage 5 materializes gallery, scenario, and openings images for the eight Wave 15 Hilton factory-preview brands (Hilton Hotels & Resorts, Homewood Suites by Hilton, Home2 Suites by Hilton, Tru by Hilton, DoubleTree by Hilton, Hampton by Hilton, Hilton Garden Inn, Spark by Hilton).`,
      ``,
      `## Commands`,
      ``,
      "```bash",
      `npm run brand-explorer-wave15-factory -- --stage image-materialization --dry-run`,
      `npm run brand-explorer-wave15-factory -- --stage image-materialization --apply \\`,
      ...WAVE15_IMAGE_MATERIALIZATION_APPLY_FLAGS.map((f, i, arr) =>
        i === arr.length - 1 ? `  ${f}` : `  ${f} \\`
      ),
      "```",
      ``,
      `## Protected 54 identity preflight (mandatory)`,
      ``,
      `Runs before planning / applying. Fails loudly with \`stopRecommended\` if:`,
      ``,
      `- Live Active/Live universe count !== ${EXPECTED_ACTIVE_COUNT_54}`,
      `- Marriott Hotels brand (recordId \`recn59UtkyyoYwzSz\`) is missing OR its name has drifted back to bare "Marriott" (must match \`/Marriott Hotels/i\`)`,
      `- Four Points Flex by Sheraton re-entered Active/Live (must remain held)`,
      ``,
      `## Guardrails`,
      ``,
      `- Target brands only (eight Wave 15 Hilton family brands)`,
      `- No writes to Marriott Hotels, Four Points Flex by Sheraton, House of Originals, Morgans, Radisson Collection`,
      `- No protected 54 brand changes`,
      `- No Brand Status / release / CV / Source / Registry writes`,
      `- No content body rewrites`,
      `- Hilton Hotels & Resorts ≠ Hilton Worldwide (corporate)`,
      `- Homewood ≠ Home2; Tru ≠ Spark ≠ Hampton; DoubleTree ≠ Hilton flagship`,
      `- CALA-first openings where supported; Americas / International Reference otherwise`,
      `- Cleanly unavailable for unsupported property images`,
      ``,
      `## Fixtures`,
      ``,
      `- \`fixtures/wave15-{slug}-gallery-pool.json\` (from \`scripts/harvest-wave15-image-pools.mjs\`)`,
      ``,
      `## Reports`,
      ``,
      `- \`reports/brand-explorer-wave15-image-materialization.{json,md}\``,
      `- \`reports/brand-explorer-wave15-image-materialization-{slug}.md\``,
      ``,
      `Ready: \`wave15_image_materialization_ready_for_post_image_cleanup\` (apply) · \`wave15_stage5_image_materialization_dry_run_ready\` (dry-run)`,
      ``,
    ].join("\n")
  );

  return summary;
}
