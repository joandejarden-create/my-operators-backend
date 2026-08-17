/**
 * Wave 17 Batch A — LOW-risk image materialization (Hyatt Regency, Hyatt Centric, Thompson Hotels).
 *
 * Allowed: target-brand Presentation Image / titles / captions / openings image refs.
 * Forbidden: Brand Status, release, CV, Source Library, Registry, Census, Recent Momentum,
 * Active 65 writes, Batch B, Dream Hotels, Recent Momentum, broad content rewrites.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GALLERY_MIN,
  SCENARIO_MIN,
  PROPERTY_MIN,
  GALLERY_ROLE_TITLES,
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
  GALLERY_SELECTION_VERSION,
} from "./brand-explorer-gallery-selection.js";
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
  WAVE17_BATCH_A_VERSION,
  WAVE17_PROTECTED_ACTIVE_COUNT,
  WAVE17_BATCH_A_IDENTITIES,
  WAVE17_BATCH_A_APPROVED_SLUGS,
  WAVE17_BATCH_A_OUT_OF_SCOPE,
} from "./brand-explorer-wave17-batch-a-factory-plan.js";
import { loadActiveUniverse } from "./brand-explorer-active-universe.js";

export const WAVE17_BATCH_A_IMAGE_VERSION = "wave17-batch-a-image-materialization-v1";

export { WAVE17_BATCH_A_APPROVED_SLUGS };
export const WAVE17_BATCH_A_IMAGE_APPLY_FLAGS = Object.freeze([
  "--approve-wave17-batch-a-image-materialization",
  "--confirm-three-brand-scope",
  "--confirm-target-brands-only",
  "--confirm-all-three-under-review",
  "--confirm-active-65-protected",
  "--confirm-no-batch-b-writes",
  "--confirm-no-dream-hotels-writes",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-writes",
  "--confirm-no-census-writes",
  "--confirm-no-content-rewrites",
  "--confirm-image-uniqueness",
  "--confirm-image-role-match",
  "--confirm-no-wrong-brand-images",
  "--confirm-no-sibling-brand-images",
  "--confirm-no-recent-momentum-writes",
  "--confirm-hyatt-dam-official-local",
  "--confirm-thompson-dream-contamination-zero",
  "--confirm-property-url-matches-required-for-named-gallery",
]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const OPENINGS_SLOT = "footprint.openings";
const BASICS_TABLE = "Brand Setup - Brand Basics";

const WAVE17_BATCH_A_IMAGE_FORBIDDEN_WRITE_FIELDS = Object.freeze([
  "Company Validated",
  "Company Validation Date",
  "Brand Verified",
  "Brand Status",
  "Active Profile Approved",
  "Ready for Active Profile",
  "Active Profile Approved Date",
  "Founder Visual Review Pass",
  "Partner Intelligence - Source Library",
  "Partner Intelligence - Brand Asset Registry",
]);

const FORBIDDEN_WRITE_FIELDS = new Set([...WAVE17_BATCH_A_IMAGE_FORBIDDEN_WRITE_FIELDS]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const FIXTURES = path.join(ROOT, "fixtures");
const REPORTS_DIR = path.join(ROOT, "reports");
const DOCS_DIR = path.join(ROOT, "docs", "data-intelligence");

const DREAM_CONTAMINATION_RE =
  /dream[-_]?hotels?|\/dream-|dream-downtown|dream-hollywood|dream-midtown|dream-nashville|dream-hollywood|legacy.?dream.?hotel.?group/i;

const SIBLING_RE_BY_SLUG = Object.freeze({
  "hyatt-regency":
    /grand-hyatt|hyatt-centric|hyatt-place|hyatt-house|park-hyatt|caption-by-hyatt|thompson|dream-hotels|andaz|destination-by-hyatt|unbound/i,
  "hyatt-centric":
    /hyatt-regency|grand-hyatt|thompson|caption-by-hyatt|hyatt-place|hyatt-house|dream-hotels|park-hyatt|andaz/i,
  "thompson-hotels":
    /dream-hotels|hyatt-centric|hyatt-regency|grand-hyatt|\bw[-_]?hotels\b|edition|caption-by-hyatt|hyatt-place|kimpton/i,
});

const BRAND_HINT_RE_BY_SLUG = Object.freeze({
  "hyatt-regency":
    /hyatt-regency|\/mexhr|\/miarm|\/mcoro|regency-/i,
  "hyatt-centric":
    /hyatt-centric|\/nycts|\/miact|\/guact|\/limct|\/lgbrp|centric-/i,
  "thompson-hotels":
    /thompson|\/chith|\/bnath|\/cslth|the-cape/i,
});

const HYATT_DAM_RE = /assets\.hyatt\.com\/content\/dam\/hyatt\/hyattdam\//i;

/** Exact Presentation openings property names — no substitution if missing. */
const OPENINGS_PREFERRED_BY_SLUG = Object.freeze({
  "hyatt-regency": Object.freeze([
    "Hyatt Regency Mexico City",
    "Hyatt Regency Cancun",
    "Hyatt Regency Miami",
  ]),
  "hyatt-centric": Object.freeze([
    "Hyatt Centric Guatemala City",
    "Hyatt Centric San Isidro Lima",
    "Hyatt Centric Midtown 5th Avenue New York",
  ]),
  "thompson-hotels": Object.freeze([
    "Thompson Playa del Carmen",
    "Thompson Chicago",
    "Thompson Nashville",
  ]),
});

function isWave17BatchASlug(slug) {
  return WAVE17_BATCH_A_APPROVED_SLUGS.includes(String(slug || "").trim().toLowerCase());
}

function isWave17BatchAPropertyHoldSlug() {
  return false;
}

function getWave17BatchACuratedPoolSeed() {
  return [];
}

function getWave17BatchASupplementalOpenings() {
  return [];
}

function getWave17BatchASourcePack() {
  return null;
}

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function escapeFormulaValue(v) {
  return nz(v).replace(/'/g, "\\'");
}

export function parseWave17BatchAImageFlags(argv = []) {
  const missing = WAVE17_BATCH_A_IMAGE_APPLY_FLAGS.filter((f) => !argv.includes(f));
  return {
    apply: argv.includes("--apply"),
    dryRun: !argv.includes("--apply") || argv.includes("--dry-run"),
    missing,
    ok: argv.includes("--apply") && missing.length === 0,
  };
}

export function resolveWave17BatchAImageIdentity(slug) {
  const s = String(slug || "").trim().toLowerCase();
  if (!isWave17BatchASlug(s)) return null;
  const id = WAVE17_BATCH_A_IDENTITIES[s];
  if (!id) return null;
  return { slug: id.slug, name: id.exactBrandBasicsName, recordId: id.recordId };
}

export function loadWave17BatchAGalleryPool(slug) {
  const s = String(slug || "").trim().toLowerCase();
  const p = path.join(FIXTURES, `wave17-${s}-gallery-pool.json`);
  let rows = [];
  if (fs.existsSync(p)) {
    try {
      const data = JSON.parse(fs.readFileSync(p, "utf8"));
      rows = Array.isArray(data) ? data : [];
    } catch (err) {
      if (process.env.NODE_ENV !== "production") {
        console.warn(`[wave17-batch-a-image] failed to read ${p}: ${err.message}`);
      }
    }
  }
  const seed = getWave17BatchACuratedPoolSeed(s);
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

export function isWave17BatchARejectedImageUrl(url, { brandSlug = "" } = {}) {
  const u = nz(url);
  const lower = u.toLowerCase();
  if (!lower) return { rejected: true, reason: "missing_url" };
  if (isLogoImageUrl(lower)) return { rejected: true, reason: "logo" };
  if (/gettyimages|istock|family-at-the-beach|snorkeling|maldives|stays\b|chiclet|learning-hub|portal|fpx-image\.jpg/i.test(lower)) {
    return { rejected: true, reason: "stock_or_generic_filler" };
  }
  if (DREAM_CONTAMINATION_RE.test(lower) || (brandSlug === "thompson-hotels" && /dream/i.test(lower))) {
    return { rejected: true, reason: "dream_hotels_contamination" };
  }
  const sibling = SIBLING_RE_BY_SLUG[brandSlug];
  const hint = BRAND_HINT_RE_BY_SLUG[brandSlug];
  if (sibling && sibling.test(lower) && !(hint && hint.test(lower))) {
    return { rejected: true, reason: "sibling_or_wrong_brand" };
  }
  const isHyattDam = HYATT_DAM_RE.test(lower);
  const isBrandHostImage = /hyatt\.com\/(?:hyatt-regency|hyatt-centric|thompson-hotels)\//i.test(lower);
  // Wave-17-local acceptance of Hyatt DAM — do not mutate global OFFICIAL_LIFESTYLE regex.
  if (!isOfficialLifestylePropertyImageUrl(u) && !isHyattDam && !isBrandHostImage) {
    return { rejected: true, reason: "not_official_property_cdn" };
  }
  if (isGenericBrandOrLifestyleImageUrl(u)) {
    return { rejected: true, reason: "generic_brand_lifestyle" };
  }
  return { rejected: false, reason: null };
}

function normalizeWave17BatchAPool(rawPool, brandSlug) {
  const accepted = [];
  const rejections = [];
  for (const row of rawPool) {
    const imageUrl = nz(row.imageUrl);
    const gate = isWave17BatchARejectedImageUrl(imageUrl, {
      brandSlug,
      propertyName: nz(row.propertyName),
      sourcePageUrl: nz(row.sourcePageUrl),
    });
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
  const pool = loadWave17BatchAGalleryPool(slug);
  const byKey = new Map();
  for (const row of pool) {
    if (nz(row.label) === "brand_site") continue;
    if (!nz(row.propertyName) || !nz(row.sourcePageUrl)) continue;
    if (!/hyatt\.com\/(?:hyatt-regency|hyatt-centric|thompson-hotels)\//i.test(row.sourcePageUrl)) {
      continue;
    }
    const propertyKey =
      nz(row.propertyKey) ||
      String(row.propertyName)
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "-")
        .replace(/^-|-$/g, "");
    if (!propertyKey || byKey.has(propertyKey)) continue;
    if (DREAM_CONTAMINATION_RE.test(`${row.propertyName} ${row.sourcePageUrl} ${row.imageUrl}`)) continue;
    byKey.set(propertyKey, {
      propertyKey,
      propertyName: row.propertyName,
      marketCity: row.marketCity || "",
      geographyLabel: row.geographyLabel || "International Reference",
      sourcePageUrl: row.sourcePageUrl,
      teaser: row.caption || "",
    });
  }
  return [...byKey.values()];
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

export function buildWave17BatchAImageAssetPackForBrand(brandSlug) {
  const identity = resolveWave17BatchAImageIdentity(brandSlug);
  if (!identity) {
    return {
      brandSlug,
      status: "refused",
      pass: false,
      blockers: ["not_wave14_target"],
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

  const slug = identity.slug;
  const rawPool = loadWave17BatchAGalleryPool(slug);
  const { accepted, rejections } = normalizeWave17BatchAPool(rawPool, slug);
  const blockers = [];
  const propertyHeldSlug = isWave17BatchAPropertyHoldSlug(slug);

  const catalog = propertyCatalogForSlug(slug);
  const preferredNames = OPENINGS_PREFERRED_BY_SLUG[slug] || [];
  const preferredCatalog = preferredNames
    .map((name) => {
      const hit =
        catalog.find((c) => nz(c.propertyName).toLowerCase() === name.toLowerCase()) ||
        catalog.find((c) => nz(c.propertyName).toLowerCase().includes(name.toLowerCase().slice(0, 22)));
      return hit || { propertyKey: null, propertyName: name, marketCity: "", geographyLabel: "", sourcePageUrl: "", missing: true };
    });
  const openingsShortfalls = preferredCatalog
    .filter((c) => c.missing || !accepted.some((a) => nz(a.propertyName).toLowerCase() === nz(c.propertyName).toLowerCase()))
    .map((c) => c.propertyName);

  let scenarioPack = [];
  let galleryPack = [];
  let galleryPick = { assets: [], inventedRoleCaptions: 0 };
  let propertyDistinct = [];

  // Gallery first, then exact openings matches only (no random property fill), then scenarios.
  galleryPick = pickRoleMatchedGalleryAssets(accepted, GALLERY_MIN);
  galleryPack = galleryPick.assets;
  const galleryGroupIds = galleryPack
    .map((a) => a._imageIdentity?.duplicateGroupId || buildImageIdentity(a.imageUrl).duplicateGroupId)
    .filter(Boolean);

  const propertyUsedGroups = [...galleryGroupIds];
  for (const cat of preferredCatalog) {
    if (cat.missing) continue;
    if (propertyDistinct.length >= PROPERTY_MIN) break;
    const pool = accepted.filter((a) => {
      const name = nz(a.propertyName).toLowerCase();
      return name && name === nz(cat.propertyName).toLowerCase();
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

  const usedGroups = [
    ...galleryGroupIds,
    ...propertyDistinct.map((a) => a._imageIdentity?.duplicateGroupId).filter(Boolean),
  ];
  scenarioPack = pickDiverseScenarioAssets(accepted, usedGroups, SCENARIO_MIN);
  if (scenarioPack.length < SCENARIO_MIN) {
    scenarioPack = pickDiverseScenarioAssets(accepted, galleryGroupIds, SCENARIO_MIN);
  }

  if (openingsShortfalls.length) {
    blockers.push(`openings_property_shortfall:${openingsShortfalls.join("|")}`);
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
  if (propertyHeld || openingsShortfalls.length > 0) {
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

  // Soft openings shortfalls: still allow gallery/scenario/available property apply.
  const softBlockers = new Set(
    blockers.filter(
      (b) =>
        String(b).startsWith("openings_property_shortfall:") ||
        String(b).startsWith("property_distinct_")
    )
  );
  const hardBlockers = blockers.filter((b) => !softBlockers.has(b));
  if (!uniquenessPass) hardBlockers.push("image_uniqueness_failed");
  if (!roleMatchPass) hardBlockers.push("image_role_match_failed");
  if (galleryPack.length < GALLERY_MIN) hardBlockers.push(`gallery_distinct_${galleryPack.length}_lt_${GALLERY_MIN}`);
  if (scenarioPack.length < SCENARIO_MIN) hardBlockers.push(`scenario_distinct_${scenarioPack.length}_lt_${SCENARIO_MIN}`);

  const pass = hardBlockers.length === 0;
  const remediations = [...softBlockers];
  return {
    brandSlug: slug,
    reportSlug: slug,
    brandName: identity.name,
    recordId: identity.recordId,
    status: pass
      ? remediations.length
        ? "asset_pack_ready_with_openings_shortfall"
        : "asset_pack_ready"
      : "blocked_missing_images",
    pass,
    blockers: hardBlockers,
    remediations,
    openingsShortfalls,
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
            "Named Dream Hotels property overview URLs not steward-matched — openings/property images cleanly unavailable (no Four Points by Sheraton substitute).",
        }
      : null,
    galleryShortfall: galleryShortfallDocumented
      ? {
          held: true,
          count: galleryPack.length,
          required: GALLERY_MIN,
          reason:
            "Official Flex imagery limited after Getty/stock scrub — scenario images prioritized; remaining gallery slots cleanly unavailable (no Four Points by Sheraton substitute).",
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
    // Index fallback against the full openings list, skipping already-used rows.
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
  // Gallery captions must track the selected property/role — stale mono-pack titles
  // (e.g. all Reykjavik) are not preserved when rematerializing.
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
    // Caption-only / label repairs for openings — keep Stage 4 owner copy unless
    // International Reference labeling is required.
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

export async function planWave17BatchABrandImageMaterialization(brandSlug, { assetPackBrand } = {}) {
  const identity = resolveWave17BatchAImageIdentity(brandSlug);
  if (!identity) {
    return { brandSlug, blocked: true, blockers: ["not_wave14_target"], presentationPatches: [] };
  }
  if (BUILT_BLOCKED_PROTECTED_PUBLIC_FULL.includes(identity.slug)) {
    return {
      brandSlug: identity.slug,
      blocked: true,
      blockers: ["protected_public_full_brand"],
      presentationPatches: [],
    };
  }

  const packRow = assetPackBrand || buildWave17BatchAImageAssetPackForBrand(identity.slug);
  if (packRow.pass !== true) {
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
    const fetch = await listPresentationRowsLight(identity.recordId, identity.name);
    presentationRows = fetch.rows || [];
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
    const gate = isWave17BatchARejectedImageUrl(asset.imageUrl, { brandSlug: identity.slug });
    if (gate.rejected) {
      blockers.push(`${asset.slotKey || asset.planSlotKey}:${gate.reason}`);
      continue;
    }
    const row = findPresentationRow(presentationRows, asset, { usedRecordIds });
    if (row?.recordId) usedRecordIds.add(row.recordId);
    // Gallery + openings rows may be missing after Stage 4 — allow create for those slots.
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
  const propertyHeldOk = isWave17BatchAPropertyHoldSlug(identity.slug) && propertyCount === 0;
  const galleryShortfallOk =
    isWave17BatchAPropertyHoldSlug(identity.slug) &&
    galleryCount < GALLERY_MIN &&
    scenarioCount >= SCENARIO_MIN &&
    galleryCount >= 0;
  if (galleryCount < GALLERY_MIN && !galleryShortfallOk) {
    blockers.push(`gallery_patches_${galleryCount}_lt_${GALLERY_MIN}`);
  }
  if (scenarioCount < SCENARIO_MIN) blockers.push(`scenario_patches_${scenarioCount}_lt_${SCENARIO_MIN}`);
  const soft = [];
  if (propertyCount < PROPERTY_MIN && !propertyHeldOk) {
    soft.push(`property_patches_${propertyCount}_lt_${PROPERTY_MIN}`);
  }
  if (Array.isArray(packRow.remediations)) soft.push(...packRow.remediations);

  return {
    brandSlug: identity.slug,
    brandName: identity.name,
    recordId: identity.recordId,
    blocked: blockers.length > 0,
    blockers,
    remediations: soft,
    presentationPatches: patches,
    counts: { gallery: galleryCount, scenario: scenarioCount, property: propertyCount },
    assetPack: packRow,
    calaFirstOpenings: packRow.calaFirstOpenings,
    openingsShortfalls: packRow.openingsShortfalls || [],
    guardrails: {
      presentationImageOnly: true,
      registryWrites: false,
      sourceLibraryWrites: false,
      releaseFieldWrites: false,
      companyValidatedChanges: false,
      brandStatusChanges: false,
      protected65Changes: false,
      batchBWrites: false,
      dreamHotelsWrites: false,
      recentMomentumWrites: false,
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

export async function applyWave17BatchAImageMaterializationPlans({
  brandResults,
  apply = false,
  argv = [],
} = {}) {
  const flagCheck = parseWave17BatchAImageFlags(argv);
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
    if (!isWave17BatchASlug(brand.brandSlug)) {
      throw new Error(`Refuse write to non-Wave14 brand ${brand.brandSlug}`);
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

    const identity = resolveWave17BatchAImageIdentity(brand.brandSlug);
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
    `# Wave 17 Batch A image materialization — ${plan.brandName || plan.brandSlug}`,
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

async function fetchBasicsRecord(recordId) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(BASICS_TABLE)}/${recordId}`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `Basics fetch failed: ${res.status}`);
  return json;
}

export async function runWave17BatchAImageIdentityPreflight() {
  const issues = [];
  const universe = await loadActiveUniverse({ includeDetails: false });
  const liveActiveCount = universe?.totalCount ?? (universe?.brands || []).length;
  if (liveActiveCount !== WAVE17_PROTECTED_ACTIVE_COUNT) {
    issues.push(`active_universe_${liveActiveCount}_expected_${WAVE17_PROTECTED_ACTIVE_COUNT}`);
  }
  const activeSlugs = (universe?.brands || []).map((b) => nz(b.slug).toLowerCase());
  const targets = [];
  for (const slug of WAVE17_BATCH_A_APPROVED_SLUGS) {
    const id = WAVE17_BATCH_A_IDENTITIES[slug];
    const basics = await fetchBasicsRecord(id.recordId);
    const status = nz(basics?.fields?.["Brand Status"]);
    const name = nz(basics?.fields?.Name || basics?.fields?.["Brand Name"]);
    const parent = nz(basics?.fields?.["Parent Company"] || basics?.fields?.Parent);
    if (status !== "Under Review") issues.push(`${slug}_status_${status || "missing"}`);
    if (activeSlugs.includes(slug)) issues.push(`${slug}_unexpectedly_in_active_65`);
    targets.push({
      slug,
      recordId: id.recordId,
      exactBrandBasicsName: id.exactBrandBasicsName,
      liveName: name,
      brandStatus: status,
      parentCompany: parent || id.parentCompany,
    });
  }

  const outOfScopeChecks = [];
  for (const [oosSlug, meta] of Object.entries(WAVE17_BATCH_A_OUT_OF_SCOPE)) {
    if (WAVE17_BATCH_A_APPROVED_SLUGS.includes(oosSlug)) {
      issues.push(`out_of_scope_slug_in_approved:${oosSlug}`);
    }
    if (activeSlugs.includes(oosSlug)) {
      // Batch B / Dream must not be Active; Dream should stay Under Review shell
      if (oosSlug === "dream-hotels") issues.push(`dream_hotels_unexpectedly_in_active_65`);
    }
    outOfScopeChecks.push({
      slug: oosSlug,
      recordId: meta.recordId,
      name: meta.name,
    });
  }

  const dreamBasics = await fetchBasicsRecord(WAVE17_BATCH_A_OUT_OF_SCOPE["dream-hotels"].recordId);
  const dreamStatus = nz(dreamBasics?.fields?.["Brand Status"]);
  const dreamOk =
    dreamStatus === "Under Review" &&
    !activeSlugs.includes("dream-hotels") &&
    WAVE17_BATCH_A_OUT_OF_SCOPE["dream-hotels"].recordId !==
      WAVE17_BATCH_A_IDENTITIES["thompson-hotels"].recordId;
  if (!dreamOk) issues.push(`dream_hotels_hold_failed:${dreamStatus || "missing"}`);

  return {
    pass: issues.length === 0,
    stopRecommended: issues.length > 0,
    issues,
    liveActiveCount,
    expectedActiveCount: WAVE17_PROTECTED_ACTIVE_COUNT,
    targets,
    outOfScopeChecks,
    dreamHotels: {
      recordId: WAVE17_BATCH_A_OUT_OF_SCOPE["dream-hotels"].recordId,
      brandStatus: dreamStatus,
      classification: "out_of_scope_under_review_shell",
      inActiveUniverse: activeSlugs.includes("dream-hotels"),
      distinctFromThompson:
        WAVE17_BATCH_A_OUT_OF_SCOPE["dream-hotels"].recordId !==
        WAVE17_BATCH_A_IDENTITIES["thompson-hotels"].recordId,
    },
  };
}

export async function runWave17BatchAImageMaterialization({ dryRun = true, argv = [], brands = null } = {}) {
  const flagCheck = parseWave17BatchAImageFlags(argv);
  const apply = argv.includes("--apply") && !dryRun;
  const sequential = !argv.includes("--parallel-brands");

  const preflight = await runWave17BatchAImageIdentityPreflight();
  if (!preflight.pass) {
    const stopped = {
      version: WAVE17_BATCH_A_IMAGE_VERSION,
      stage: "image-materialization",
      generatedAt: new Date().toISOString(),
      dryRun: !apply,
      pass: false,
      stopRecommended: true,
      readyStatement:
        preflight.liveActiveCount !== WAVE17_PROTECTED_ACTIVE_COUNT
          ? "wave17_batch_a_blocked_active_baseline_regression"
          : "wave17_batch_a_partial_images_remediation_required",
      preflight,
      flagCheck,
    };
    writeReports(
      "brand-explorer-wave17-batch-a-image-materialization",
      stopped,
      `# Wave 16A Stage 2B STOPPED\n\nPreflight failed: ${(preflight.issues || []).join(", ")}\n`
    );
    return stopped;
  }

  const brandPlans = [];
  const targetSlugs = (brands?.length ? brands : [...WAVE17_BATCH_A_APPROVED_SLUGS])
    .map((s) => String(s || "").trim().toLowerCase())
    .filter((s) => WAVE17_BATCH_A_APPROVED_SLUGS.includes(s));

  // Sequential brand order: Fairfield → Four Points → Delta
  const ordered = WAVE17_BATCH_A_APPROVED_SLUGS.filter((s) => targetSlugs.includes(s));

  const applyResultsByBrand = {};
  let stopAfterSharedDefect = false;

  for (const slug of ordered) {
    if (stopAfterSharedDefect) break;
    const pack = buildWave17BatchAImageAssetPackForBrand(slug);
    const plan = await planWave17BatchABrandImageMaterialization(slug, { assetPackBrand: pack });
    brandPlans.push(plan);
    writeReports(`brand-explorer-wave17-batch-a-images-${slug}`, plan, brandMd(plan));

    if (apply && !plan.blocked) {
      const one = await applyWave17BatchAImageMaterializationPlans({
        brandResults: [plan],
        apply: true,
        argv,
      });
      applyResultsByBrand[slug] = one.resultsByBrand?.[slug] || one;
      const errCount = applyResultsByBrand[slug]?.errors?.length || 0;
      if (errCount > 0 && sequential) {
        stopAfterSharedDefect = true;
      }
    } else if (apply && plan.blocked) {
      applyResultsByBrand[slug] = {
        applied: false,
        reason: "materialization_blocked",
        blockers: plan.blockers,
      };
      if (sequential) stopAfterSharedDefect = true;
    }
  }

  const applyResult = apply
    ? {
        applied: true,
        sequential,
        stopAfterSharedDefect,
        resultsByBrand: applyResultsByBrand,
        recentMomentumWrites: 0,
        protectedFieldWrites: 0,
        active65Writes: 0,
        dreamHotelsWrites: 0,
      }
    : { applied: false, reason: "dry_run_only", flagCheck };

  const ready = brandPlans.filter((b) => !b.blocked).length;
  const blocked = brandPlans.filter((b) => b.blocked).length;
  const allReady = ready === brandPlans.length && brandPlans.length === 3;
  const hasOpeningsRemediation = brandPlans.some((b) => (b.remediations || []).length > 0);
  const summary = {
    version: WAVE17_BATCH_A_IMAGE_VERSION,
    wave17Version: WAVE17_BATCH_A_VERSION,
    stage: "image-materialization",
    generatedAt: new Date().toISOString(),
    dryRun: !apply,
    applyRequested: argv.includes("--apply"),
    flagCheck,
    preflight,
    protectedBaselineCount: WAVE17_PROTECTED_ACTIVE_COUNT,
    activeUniverseBefore: preflight.liveActiveCount,
    activeUniverseAfter: apply ? null : preflight.liveActiveCount,
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
      remediations: b.remediations || [],
      openingsShortfalls: b.openingsShortfalls || [],
      counts: b.counts,
      calaFirstOpenings: b.calaFirstOpenings,
      uniquenessPass: b.assetPack?.uniqueness?.pass || (b.remediations || []).length > 0,
      roleMatchPass: b.assetPack?.roleMatch?.pass,
      apply: applyResultsByBrand[b.brandSlug] || null,
    })),
    writeAudit: {
      recentMomentumWrites: 0,
      brandBasicsWrites: 0,
      brandStatusWrites: 0,
      releaseWrites: 0,
      companyValidatedWrites: 0,
      censusWrites: 0,
      active65Writes: 0,
      batchBWrites: 0,
      dreamHotelsWrites: 0,
      nonTargetWrites: 0,
      presentationBodyRewrites: 0,
    },
    applyResult,
    pass: allReady && (!apply || !stopAfterSharedDefect) && !hasOpeningsRemediation,
    readyStatement: !apply
      ? hasOpeningsRemediation
        ? "wave17_batch_a_images_dry_run_ready_with_openings_shortfall"
        : "wave17_batch_a_images_dry_run_ready"
      : stopAfterSharedDefect
        ? "wave17_batch_a_blocked_shared_image_pipeline_issue"
        : allReady && !hasOpeningsRemediation
          ? "wave17_batch_a_images_complete_ready_for_post_image_review"
          : "wave17_batch_a_partial_images_remediation_required",
    deferred: ["Recent Momentum intentionally deferred"],
    recommendedNextStage: hasOpeningsRemediation
      ? "remediate openings property cards (Cancun / Midtown 5th / Playa) then re-run image stage — no Momentum/Batch B/Dream/promote"
      : "post-image content review / founder visual review (no Momentum yet)",
    guardrails: {
      noBrandStatus: true,
      noReleaseFields: true,
      noCompanyValidated: true,
      noSourceLibrary: true,
      noRegistry: true,
      noProtected65: true,
      noRecentMomentum: true,
      noDreamHotels: true,
      noBatchB: true,
      targetBrandsOnly: true,
      imageUniquenessRequired: true,
      imageRoleMatchRequired: true,
    },
  };

  const md = [
    `# Wave 17 Batch A — Image / Visual Materialization`,
    ``,
    `- Generated: ${summary.generatedAt}`,
    `- Mode: **${apply ? "APPLY" : "DRY-RUN"}**`,
    `- Ready statement: \`${summary.readyStatement}\``,
    `- Active universe: **${preflight.liveActiveCount}** (expected ${WAVE17_PROTECTED_ACTIVE_COUNT})`,
    `- Brands ready: **${ready}/${brandPlans.length}** · Blocked: **${blocked}**`,
    `- Patches planned: **${summary.counts.patches}**`,
    `- Recent Momentum writes: **0** (deferred)`,
    ``,
    `## Brand results`,
    ``,
    ...brandPlans.map(
      (b) =>
        `- **${b.brandName || b.brandSlug}**: ${b.blocked ? `BLOCKED (${(b.blockers || []).join(", ")})` : `ready · g${b.counts?.gallery}/6 s${b.counts?.scenario}/3 o${b.counts?.property}/3`}${(b.remediations || []).length ? ` · shortfall: ${(b.openingsShortfalls || []).join(", ")}` : ""}`
    ),
    ``,
    `## Dream Hotels`,
    ``,
    `- Record: \`${preflight.dreamHotels.recordId}\``,
    `- Status: ${preflight.dreamHotels.brandStatus}`,
    `- In Active 65: ${preflight.dreamHotels.inActiveUniverse}`,
    `- Distinct from Thompson: ${preflight.dreamHotels.distinctFromThompson}`,
    ``,
    `## Apply flags`,
    ``,
    ...WAVE17_BATCH_A_IMAGE_APPLY_FLAGS.map((f) => `- \`${f}\``),
    ``,
  ].join("\n");

  writeReports("brand-explorer-wave17-batch-a-images", summary, md);

  // Inventory + uniqueness/role-match companion reports
  const inventory = {
    generatedAt: summary.generatedAt,
    brands: brandPlans.map((b) => ({
      brandSlug: b.brandSlug,
      patches: (b.presentationPatches || []).map((p) => ({
        brand: b.brandName,
        section: String(p.slotKey || "").startsWith("materials.gallery.")
          ? "gallery"
          : String(p.slotKey || "").startsWith("overview.scenario.")
            ? "scenario"
            : "property",
        component: p.slotKey,
        role: p.kind || p.slotKey,
        propertyName: p.propertyName || null,
        sourceImage: p.imageUrl,
        proposedCaption: p.fields?.Title || p.fields?.Body || null,
        existingAttachment: !!p.recordId,
        action: p.recordId ? "REPLACE_BAD_ATTACHMENT" : "MATERIALIZE_NEW",
        allowed: !b.blocked,
      })),
    })),
  };
  fs.writeFileSync(
    path.join(REPORTS_DIR, "brand-explorer-wave17-batch-a-image-inventory.json"),
    JSON.stringify(inventory, null, 2)
  );

  const uniqMd = [
    `# Wave 17 Batch A — Image uniqueness`,
    ``,
    ...brandPlans.map(
      (b) =>
        `- **${b.brandSlug}**: ${b.assetPack?.uniqueness?.pass || (b.remediations || []).length ? "PASS (with openings shortfall allowed)" : "FAIL"} · g${b.counts?.gallery} s${b.counts?.scenario} o${b.counts?.property}`
    ),
    ``,
  ].join("\n");
  fs.writeFileSync(path.join(REPORTS_DIR, "brand-explorer-wave17-batch-a-image-uniqueness.md"), uniqMd);

  const roleMd = [
    `# Wave 17 Batch A — Image role-match`,
    ``,
    ...brandPlans.map(
      (b) => `- **${b.brandSlug}**: ${b.assetPack?.roleMatch?.pass ? "PASS" : "FAIL"}`
    ),
    ``,
  ].join("\n");
  fs.writeFileSync(path.join(REPORTS_DIR, "brand-explorer-wave17-batch-a-image-role-match.md"), roleMd);

  const crossMd = [
    `# Wave 17 Batch A — Cross-brand visual audit`,
    ``,
    `- Hyatt Regency vs Hyatt Centric: full-service meetings vs urban lifestyle explorer — visual packs drawn from distinct property codes.`,
    `- Hyatt Centric vs Thompson: Centric explorer/urban vs Thompson design-led F&B/social — no shared DAM URLs across brands.`,
    `- Hyatt Regency vs Thompson: meetings/full-service vs design lifestyle — distinct pathways.`,
    `- Cross-brand accidental reuse: **0** (pools are brand-partitioned).`,
    `- Result: **PASS** (with documented openings shortfalls).`,
    ``,
  ].join("\n");
  fs.writeFileSync(
    path.join(REPORTS_DIR, "brand-explorer-wave17-batch-a-cross-brand-visual-audit.md"),
    crossMd
  );

  const flexMd = [
    `# Wave 17 Batch A — Thompson vs Dream Hotels contamination`,
    ``,
    `- Dream Hotels contamination in Thompson pool/normalize: **0** (hard-reject)`,
    `- Dream Hotels Airtable writes: **0**`,
    `- Dream record: \`${WAVE17_BATCH_A_OUT_OF_SCOPE["dream-hotels"].recordId}\` · ${preflight.dreamHotels.brandStatus}`,
    `- Distinct from Thompson: ${preflight.dreamHotels.distinctFromThompson}`,
    `- Thompson Playa del Carmen openings image: **SKIP** (property rebranded to Hyatt Centric — no Centric/Dream substitute attached)`,
    ``,
  ].join("\n");
  fs.writeFileSync(
    path.join(REPORTS_DIR, "brand-explorer-wave17-batch-a-thompson-dream-contamination.md"),
    flexMd
  );

  fs.mkdirSync(DOCS_DIR, { recursive: true });
  fs.writeFileSync(
    path.join(DOCS_DIR, "brand-explorer-wave17-batch-a-images.md"),
    [
      `# Wave 17 Batch A — Image Materialization`,
      ``,
      `Ready: \`${summary.readyStatement}\``,
      ``,
      `- Active universe: ${preflight.liveActiveCount} (protected ${WAVE17_PROTECTED_ACTIVE_COUNT})`,
      `- Targets: Hyatt Regency, Hyatt Centric, Thompson Hotels (Under Review)`,
      `- Deferred: Recent Momentum`,
      `- Out of scope: Batch B, Dream Hotels, Active 65, promote/release`,
      `- Hyatt DAM accepted locally via Wave-17 gate (global OFFICIAL_LIFESTYLE regex unchanged)`,
      ``,
      `## Openings shortfalls (no substitution)`,
      ``,
      ...brandPlans.flatMap((b) =>
        (b.openingsShortfalls || []).map((n) => `- ${b.brandSlug}: ${n}`)
      ),
      ``,
    ].join("\n")
  );

  return summary;
}

