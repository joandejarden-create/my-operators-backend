/**
 * Wave 13 Stage 5 — image asset pack + Presentation Image materialization.
 *
 * Allowed: target-brand Presentation Image / titles / captions / openings image refs.
 * Forbidden: Brand Status, release, CV, Source Library, Registry, protected 39,
 * Radisson Collection, non-target brands, broad content rewrites.
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
  FACTORY_PREVIEW_CANDIDATE_IDENTITIES,
} from "./brand-explorer-factory-preview-candidates.js";
import {
  WAVE13_VERSION,
  WAVE13_STAGE4_APPROVED_SLUGS,
  WAVE13_IMAGE_MATERIALIZATION_APPLY_FLAGS,
} from "./brand-explorer-wave13-factory-plan.js";
import { getWave13SourcePack } from "./brand-explorer-wave13-source-packs-content.js";
import { EXPECTED_ACTIVE_COUNT_39 } from "./brand-explorer-39-active-public-full-baseline.js";

export const WAVE13_IMAGE_MATERIALIZATION_VERSION = "wave13-image-materialization-v1";

export { WAVE13_IMAGE_MATERIALIZATION_APPLY_FLAGS };

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const OPENINGS_SLOT = "footprint.openings";

const WAVE13_FORBIDDEN_WRITE_FIELDS = Object.freeze([
  "Company Validated",
  "Company Validation Date",
  "Source Library Status",
  "Registry Approval",
  "Registry Status",
]);

const FORBIDDEN_WRITE_FIELDS = new Set([
  ...WAVE13_FORBIDDEN_WRITE_FIELDS,
  "Company Validated",
  "Company Validation Date",
  "Founder Visual Review Pass",
  "Active Profile Approved",
  "Ready for Active Profile",
  "Active Profile Approved Date",
  "Brand Status",
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const FIXTURES = path.join(ROOT, "fixtures");
const REPORTS_DIR = path.join(ROOT, "reports");
const DOCS_DIR = path.join(ROOT, "docs", "data-intelligence");

function isWave13Stage5Slug(slug) {
  return WAVE13_STAGE4_APPROVED_SLUGS.includes(String(slug || "").trim().toLowerCase());
}

const SIBLING_RE_BY_SLUG = Object.freeze({
  "mama-shelter": /moxy|bunkhouse|hotel-indigo|so\/|so-hotel|mondrian|hyde|delano|mgallery|sofitel|morgans/i,
  mercure: /grand.?mercure|ibis|novotel|pullman|fairmont|sofitel|mgallery/i,
  ibis: /ibis.?styles|ibis.?budget|mercure|novotel|pullman|fairmont/i,
  novotel: /mercure|pullman|ibis|fairmont|sofitel|mgallery/i,
  pullman: /novotel|fairmont|sofitel|mgallery|so\/|so-hotel|mama/i,
  "so-hotels-and-resorts": /fairmont|mgallery|mama.?shelter|morgans|hyde|mondrian|delano|sofitel|pullman/i,
  "fairmont-hotels-and-resorts": /so\/|so-hotel|raffles|sofitel|mgallery|pullman|mama/i,
});

const BRAND_HINT_RE_BY_SLUG = Object.freeze({
  "mama-shelter": /mama.?shelter|mamashelter/i,
  mercure: /mercure/i,
  ibis: /ibis(?!.?styles|.?budget)/i,
  novotel: /novotel/i,
  pullman: /pullman/i,
  "so-hotels-and-resorts": /\bso\/|\bso-hotel|\bsotogrande|\bdas.?stue/i,
  "fairmont-hotels-and-resorts": /fairmont/i,
});

/** Accor ALL hotel codes allowed per brand (wrong-code = wrong-brand). */
const ALLOWED_HOTEL_CODES_BY_SLUG = Object.freeze({
  "mama-shelter": new Set(["c4i1", "9921", "b5i0"]),
  mercure: new Set(["a535", "b215", "a247"]),
  ibis: new Set(["9011", "6971"]),
  novotel: new Set(["b552", "b904"]),
  pullman: new Set(["b464", "b8d7"]),
  "so-hotels-and-resorts": new Set(["a7l5", "b1y6", "b986", "6835", "b331", "b6v6", "9149", "6599"]),
  "fairmont-hotels-and-resorts": new Set(["a573", "b6v4", "a552"]),
});

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function escapeFormulaValue(v) {
  return nz(v).replace(/'/g, "\\'");
}

export function parseWave13ImageMaterializationFlags(argv = []) {
  const missing = WAVE13_IMAGE_MATERIALIZATION_APPLY_FLAGS.filter((f) => !argv.includes(f));
  return {
    apply: argv.includes("--apply"),
    dryRun: !argv.includes("--apply") || argv.includes("--dry-run"),
    missing,
    ok: argv.includes("--apply") && missing.length === 0,
  };
}

/** Supplemental ALL.accor property pages when source-pack URLs are consumer sites. */
const SUPPLEMENTAL_PROPERTY_CATALOG = Object.freeze({
  "fairmont-hotels-and-resorts": [
    {
      propertyName: "Fairmont Mayakoba",
      market: "Riviera Maya, Mexico",
      geographyLabel: "CALA",
      url: "https://all.accor.com/hotel/A573/index.en.shtml",
      teaser:
        "CALA luxury resort reference — Fairmont Mayakoba on Accor ALL; use for openings/property photography.",
    },
    {
      propertyName: "Fairmont El San Juan Hotel",
      market: "San Juan, Puerto Rico",
      geographyLabel: "CALA",
      url: "https://all.accor.com/hotel/B6V4/index.en.shtml",
      teaser:
        "CALA urban-resort Fairmont reference in San Juan — Accor ALL property photography.",
    },
    {
      propertyName: "Fairmont Royal Pavilion",
      market: "Barbados",
      geographyLabel: "CALA",
      url: "https://all.accor.com/hotel/A552/index.en.shtml",
      teaser:
        "CALA Caribbean Fairmont resort reference — Accor ALL property photography.",
    },
  ],
  "so-hotels-and-resorts": [
    {
      propertyName: "SO/ Paris",
      market: "Paris, France",
      geographyLabel: "International Reference",
      url: "https://all.accor.com/hotel/A7L5/index.en.shtml",
    },
    {
      propertyName: "SO/ Berlin Das Stue",
      market: "Berlin, Germany",
      geographyLabel: "International Reference",
      url: "https://all.accor.com/hotel/B1Y6/index.en.shtml",
    },
    {
      propertyName: "SO/ Maldives",
      market: "Maldives",
      geographyLabel: "International Reference",
      url: "https://all.accor.com/hotel/B986/index.en.shtml",
    },
  ],
  "mama-shelter": [
    {
      propertyName: "Mama Shelter Lisbon",
      market: "Lisbon, Portugal",
      geographyLabel: "International Reference",
      url: "https://all.accor.com/hotel/B5I0/index.en.shtml",
    },
  ],
});

export function resolveWave13ImageIdentity(slug) {
  const s = String(slug || "").trim().toLowerCase();
  if (!isWave13Stage5Slug(s)) return null;
  return FACTORY_PREVIEW_CANDIDATE_IDENTITIES[s] || null;
}

export function loadWave13GalleryPool(slug) {
  const s = String(slug || "").trim().toLowerCase();
  const p = path.join(FIXTURES, `wave13-${s}-gallery-pool.json`);
  if (!fs.existsSync(p)) return [];
  try {
    const data = JSON.parse(fs.readFileSync(p, "utf8"));
    return Array.isArray(data) ? data : [];
  } catch (err) {
    if (process.env.NODE_ENV !== "production") {
      console.warn(`[wave13-image] failed to read ${p}: ${err.message}`);
    }
    return [];
  }
}

function hotelCodeFromImageUrl(url) {
  const m = nz(url).match(/ahstatic\.com\/photos\/([a-z0-9]+)_/i);
  return m ? m[1].toLowerCase() : "";
}

/** Accor assets Airtable's attachment fetcher accepts then silently drops. */
const AIRTABLE_UNFETCHABLE_AHSTATIC = new Set(["a552_sm_00"]);

export function isWave13RejectedImageUrl(url, { brandSlug = "", hotelCode = "" } = {}) {
  const u = nz(url);
  const lower = u.toLowerCase();
  if (!lower) return { rejected: true, reason: "missing_url" };
  if (isLogoImageUrl(lower)) return { rejected: true, reason: "logo" };
  if (/gettyimages|istock|family-at-the-beach|snorkeling|chiclet|learning-hub|portal/i.test(lower)) {
    return { rejected: true, reason: "stock_or_generic_filler" };
  }
  if (/digital\.ihg\.com\/is\/image\/ihg\/stays\b/i.test(lower)) {
    return { rejected: true, reason: "ihg_generic_stays_graphic" };
  }
  const ah = lower.match(/ahstatic\.com\/photos\/([a-z0-9]+_[a-z0-9]+_\d+)/i);
  if (ah && AIRTABLE_UNFETCHABLE_AHSTATIC.has(ah[1].toLowerCase())) {
    return { rejected: true, reason: "airtable_unfetchable_ahstatic" };
  }
  const sibling = SIBLING_RE_BY_SLUG[brandSlug];
  const hint = BRAND_HINT_RE_BY_SLUG[brandSlug];
  if (sibling && sibling.test(lower) && !(hint && hint.test(lower))) {
    return { rejected: true, reason: "sibling_or_wrong_brand" };
  }
  const allowed = ALLOWED_HOTEL_CODES_BY_SLUG[brandSlug];
  const code = nz(hotelCode).toLowerCase() || hotelCodeFromImageUrl(u);
  if (allowed && code && !allowed.has(code)) {
    return { rejected: true, reason: `wrong_hotel_code:${code}` };
  }
  if (!isOfficialLifestylePropertyImageUrl(u)) {
    return { rejected: true, reason: "not_official_property_cdn" };
  }
  if (isGenericBrandOrLifestyleImageUrl(u)) {
    return { rejected: true, reason: "generic_brand_lifestyle" };
  }
  return { rejected: false, reason: null };
}

function normalizeWave13Pool(rawPool, brandSlug) {
  const accepted = [];
  const rejections = [];
  for (const row of rawPool) {
    const imageUrl = nz(row.imageUrl);
    const urlCode = hotelCodeFromImageUrl(imageUrl);
    const rowCode = nz(row.hotelCode).toLowerCase();
    // Accor ALL pages often embed sibling/nearby hotel thumbs — URL code is authoritative.
    if (rowCode && urlCode && rowCode !== urlCode) {
      rejections.push({
        imageUrl,
        reason: `hotel_code_mismatch:row_${rowCode}_url_${urlCode}`,
        propertyKey: row.propertyKey,
        hotelCode: urlCode,
      });
      continue;
    }
    const hotelCode = urlCode || rowCode;
    const gate = isWave13RejectedImageUrl(imageUrl, { brandSlug, hotelCode });
    if (gate.rejected) {
      rejections.push({ imageUrl, reason: gate.reason, propertyKey: row.propertyKey, hotelCode });
      continue;
    }
    const det = detectVisualCategory({ imageUrl, sourcePageUrl: nz(row.sourcePageUrl) });
    const role =
      nz(row.role) ||
      (det.category && det.category !== IMAGE_ROLES.unknown ? det.category : "");
    accepted.push({
      imageUrl,
      sourcePageUrl: nz(row.sourcePageUrl),
      propertyKey: nz(row.propertyKey),
      propertyName: nz(row.propertyName),
      marketCity: nz(row.marketCity),
      geographyLabel: nz(row.geographyLabel) || "International Reference",
      label: nz(row.label) || "property",
      role,
      caption: nz(row.caption),
      hotelCode,
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
      const cat = asset.role || det.category;
      if (cat !== role) return false;
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
    caption: a.propertyName
      ? `${a.propertyName}${a.marketCity ? ` — ${a.marketCity}` : ""}`
      : `Owner scenario ${i + 1}`,
    title: a.propertyName
      ? `${a.propertyName}${a.marketCity ? ` — ${a.marketCity}` : ""}`
      : `Scenario ${i + 1}${a.marketCity ? ` — ${a.marketCity}` : ""}`,
  }));
}

function propertyCatalogForSlug(slug) {
  const pack = getWave13SourcePack(slug);
  const supplemental = SUPPLEMENTAL_PROPERTY_CATALOG[slug] || [];
  const byKey = new Map();
  // Prefer Accor ALL supplemental pages (ahstatic harvest) over consumer-site examples.
  for (const r of [...supplemental, ...(pack?.propertyExamples || [])]) {
    const propertyName = r.propertyName;
    if (!propertyName) continue;
    const propertyKey = String(r.propertyName || r.matchKey || "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-|-$/g, "");
    if (byKey.has(propertyKey)) continue;
    byKey.set(propertyKey, {
      propertyKey,
      propertyName,
      marketCity: r.marketCity || (r.market || "").split(",")[0].trim(),
      geographyLabel: r.geographyLabel || "International Reference",
      sourcePageUrl: r.url,
      teaser: r.teaser || "",
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

export function buildWave13ImageAssetPackForBrand(brandSlug) {
  const identity = resolveWave13ImageIdentity(brandSlug);
  if (!identity) {
    return {
      brandSlug,
      status: "refused",
      pass: false,
      blockers: ["not_wave13_target"],
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
  const rawPool = loadWave13GalleryPool(slug);
  const { accepted, rejections } = normalizeWave13Pool(rawPool, slug);
  const blockers = [];

  const galleryPick = pickRoleMatchedGalleryAssets(accepted, GALLERY_MIN);
  const galleryPack = galleryPick.assets;
  const galleryGroupIds = galleryPack
    .map((a) => a._imageIdentity?.duplicateGroupId || buildImageIdentity(a.imageUrl).duplicateGroupId)
    .filter(Boolean);

  // Lane2 order: gallery → property (CALA-first, prefer one image per catalog property) → scenario.
  const catalog = propertyCatalogForSlug(slug);
  const catalogOrdered = [
    ...catalog.filter((c) => /^cala/i.test(nz(c.geographyLabel))),
    ...catalog.filter((c) => !/^cala/i.test(nz(c.geographyLabel))),
  ];
  const propertyDistinct = [];
  const propertyUsedGroups = [...galleryGroupIds];
  for (const cat of catalogOrdered) {
    if (propertyDistinct.length >= PROPERTY_MIN) break;
    const catHotelCode = (() => {
      const m = nz(cat.sourcePageUrl).match(/all\.accor\.com\/hotel\/([A-Za-z0-9]+)/i);
      return m ? m[1].toLowerCase() : "";
    })();
    const pool = accepted.filter((a) => {
      const name = nz(a.propertyName).toLowerCase();
      const key = nz(a.propertyKey);
      const exactName = name && name === nz(cat.propertyName).toLowerCase();
      const exactKey = key && key === cat.propertyKey;
      const codeMatch = catHotelCode && nz(a.hotelCode).toLowerCase() === catHotelCode;
      // Prefer exact property identity; hotel-code match is Accor-safe when names collide
      // (e.g. two Novotel Mexico City properties).
      return exactName || exactKey || codeMatch;
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
      hotelCode: catHotelCode || d[0].hotelCode,
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
  const propertyPack = assignPropertyExampleAssets(
    propertyDistinct.slice(0, PROPERTY_MIN),
    catalog,
    identity.name
  );

  const usedGroups = [
    ...galleryGroupIds,
    ...propertyDistinct.map((a) => a._imageIdentity?.duplicateGroupId).filter(Boolean),
  ];
  let scenarioPack = pickDiverseScenarioAssets(accepted, usedGroups, SCENARIO_MIN);
  if (scenarioPack.length < SCENARIO_MIN) {
    scenarioPack = pickDiverseScenarioAssets(accepted, galleryGroupIds, SCENARIO_MIN);
  }

  if (galleryPack.length < GALLERY_MIN) blockers.push(`gallery_distinct_${galleryPack.length}_lt_${GALLERY_MIN}`);
  if (galleryPick.inventedRoleCaptions > 0) {
    blockers.push(
      `gallery_role_uncurable_without_metadata_${galleryPick.inventedRoleCaptions}`
    );
  }
  if (scenarioPack.length < SCENARIO_MIN) {
    blockers.push(`scenario_distinct_${scenarioPack.length}_lt_${SCENARIO_MIN}`);
  }
  if (propertyPack.length < PROPERTY_MIN) {
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
  if (!uniqueness.pass) blockers.push("image_uniqueness_failed");
  if (roleMatch.pass === false) blockers.push("image_role_match_failed");

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
      // Specific property requested but no name match — do not index-fallback onto a
      // different property's openings row (e.g. El San Juan → San Francisco).
      return null;
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
  } else if (asset.slotKey === OPENINGS_SLOT) {
    // Image-first; International Reference title when labeling is required.
    // New openings rows need title/body so cards are not blank; existing Stage 4
    // Body / Case Summary fields are not rewritten.
    if (!row?.recordId) {
      if (asset.title) fields.Title = nz(asset.title);
      if (asset.body) fields.Body = nz(asset.body);
      if (nz(asset.caseSummaryOverview)) {
        fields["Case Summary Overview"] = nz(asset.caseSummaryOverview);
      }
      if (nz(asset.caseSummaryTags)) fields["Case Summary Tags"] = nz(asset.caseSummaryTags);
      if (nz(asset.caseSummaryBrandRelevance)) {
        fields["Case Summary Brand Relevance"] = nz(asset.caseSummaryBrandRelevance);
      }
      if (nz(asset.caseSummaryOwnerObjective)) {
        fields["Case Summary Owner Objective"] = nz(asset.caseSummaryOwnerObjective);
      }
      if (nz(asset.caseSummaryInterpretation)) {
        fields["Case Summary Interpretation"] = nz(asset.caseSummaryInterpretation);
      }
    } else if (asset.internationalReference && asset.title) {
      fields.Title = nz(asset.title);
    }
  } else if (asset.slotKey?.startsWith("overview.scenario.")) {
    // Caption/title only when empty — do not rewrite Stage 4 scenario body copy.
    if (!nz(row?.title) && asset.title) fields.Title = nz(asset.title);
  } else if (!row?.recordId) {
    if (asset.title) fields.Title = nz(asset.title);
    if (asset.body) fields.Body = nz(asset.body);
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

export async function planWave13BrandImageMaterialization(brandSlug, { assetPackBrand } = {}) {
  const identity = resolveWave13ImageIdentity(brandSlug);
  if (!identity) {
    return { brandSlug, blocked: true, blockers: ["not_wave13_target"], presentationPatches: [] };
  }
  if (BUILT_BLOCKED_PROTECTED_PUBLIC_FULL.includes(identity.slug)) {
    return {
      brandSlug: identity.slug,
      blocked: true,
      blockers: ["protected_public_full_brand"],
      presentationPatches: [],
    };
  }

  const packRow = assetPackBrand || buildWave13ImageAssetPackForBrand(identity.slug);
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
    const gate = isWave13RejectedImageUrl(asset.imageUrl, { brandSlug: identity.slug });
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
  if (galleryCount < GALLERY_MIN) blockers.push(`gallery_patches_${galleryCount}_lt_${GALLERY_MIN}`);
  if (scenarioCount < SCENARIO_MIN) blockers.push(`scenario_patches_${scenarioCount}_lt_${SCENARIO_MIN}`);
  if (propertyCount < PROPERTY_MIN) blockers.push(`property_patches_${propertyCount}_lt_${PROPERTY_MIN}`);

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
      protected39Changes: false,
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

function buildPresentationWriteFields(patch, identity, { isCreate = false } = {}) {
  const sortOrder =
    patch.slotKey === OPENINGS_SLOT
      ? 10
      : Number(String(patch.planSlotKey || patch.slotKey).match(/(\d+)$/)?.[1] || 0) || 0;
  const fields = {
    ...patch.fields,
  };
  if (isCreate) {
    fields["Slot Key"] = patch.slotKey;
    fields["Brand Name"] = identity.name;
    fields.Brand = [identity.recordId];
    fields.Active = true;
    fields["Sort Order"] = sortOrder;
  } else {
    // Updates: image/caption fields only — do not re-stamp Brand link/name on every patch.
    // (Stale Brand record IDs previously blocked SO/ image updates.)
  }
  return fields;
}

export async function applyWave13ImageMaterializationPlans({
  brandResults,
  apply = false,
  argv = [],
} = {}) {
  const flagCheck = parseWave13ImageMaterializationFlags(argv);
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
    if (!isWave13Stage5Slug(brand.brandSlug)) {
      throw new Error(`Refuse write to non-Wave13 Stage 5 brand ${brand.brandSlug}`);
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

    const identity = resolveWave13ImageIdentity(brand.brandSlug);
    if (!identity?.recordId || !identity?.name) {
      resultsByBrand[brand.brandSlug] = { applied: false, reason: "missing_brand_identity" };
      continue;
    }

    const results = { presentationCreated: [], presentationUpdated: [], errors: [] };
    for (const patch of brand.presentationPatches || []) {
      const isCreate = !patch.recordId && patch.createIfMissing;
      const fields = buildPresentationWriteFields(patch, identity, { isCreate });
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
    `# Wave 13 image materialization — ${plan.brandName || plan.brandSlug}`,
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

export async function runWave13ImageMaterialization({ dryRun = true, argv = [], brands = null } = {}) {
  const flagCheck = parseWave13ImageMaterializationFlags(argv);
  const apply = argv.includes("--apply") && !dryRun;
  const brandPlans = [];
  const brandsIdx = argv.indexOf("--brands");
  const brandsFromArgv =
    brandsIdx >= 0 && argv[brandsIdx + 1]
      ? String(argv[brandsIdx + 1])
          .split(",")
          .map((s) => s.trim().toLowerCase())
          .filter(Boolean)
      : null;
  const targetSlugs = (brands?.length ? brands : brandsFromArgv?.length ? brandsFromArgv : [...WAVE13_STAGE4_APPROVED_SLUGS])
    .map((s) => String(s || "").trim().toLowerCase())
    .filter((s) => WAVE13_STAGE4_APPROVED_SLUGS.includes(s));

  for (const slug of targetSlugs) {
    const pack = buildWave13ImageAssetPackForBrand(slug);
    const plan = await planWave13BrandImageMaterialization(slug, { assetPackBrand: pack });
    brandPlans.push(plan);
    writeReports(`brand-explorer-wave13-image-materialization-${slug}`, plan, brandMd(plan));
  }

  const applyResult = await applyWave13ImageMaterializationPlans({
    brandResults: brandPlans,
    apply,
    argv,
  });

  const readyCountPre = brandPlans.filter((b) => !b.blocked).length;
  const scenarioFailuresBefore = 21;
  const soStewardGapsIntentionallyNotFilled = [
    "snapshot.* (SO/ Brand Basics steward fields)",
    "footprint.primary_regions (SO/ Brand Basics steward field)",
  ];
  const intlLabels = brandPlans.flatMap((b) =>
    (b.calaFirstOpenings?.labels || [])
      .filter((l) => l.internationalReference)
      .map((l) => ({ brandSlug: b.brandSlug, ...l }))
  );
  const calaChoices = brandPlans.flatMap((b) =>
    (b.calaFirstOpenings?.labels || [])
      .filter((l) => !l.internationalReference)
      .map((l) => ({ brandSlug: b.brandSlug, ...l }))
  );
  const rejectedWrongBrand = brandPlans.flatMap((b) =>
    (b.assetPack?.poolStats?.rejectionSample || [])
      .filter((r) => /wrong|sibling/i.test(r.reason || ""))
      .map((r) => ({ brandSlug: b.brandSlug, ...r }))
  );
  const summary = {
    version: WAVE13_IMAGE_MATERIALIZATION_VERSION,
    wave13Version: WAVE13_VERSION,
    stage: "image-materialization",
    generatedAt: new Date().toISOString(),
    dryRun: !apply,
    applyRequested: argv.includes("--apply"),
    flagCheck,
    protectedBaselineCount: EXPECTED_ACTIVE_COUNT_39,
    stage5Scope: [...WAVE13_STAGE4_APPROVED_SLUGS],
    exclusions: [
      "the-house-of-originals",
      "morgans-originals",
      "radisson-collection",
      "protected-39",
      "non-target-brands",
    ],
    scenarioImageFailuresBeforeMaterialization: scenarioFailuresBefore,
    imageFieldsPatched: brandPlans.reduce((n, b) => n + (b.presentationPatches?.length || 0), 0),
    unavailableImageSlots: brandPlans.flatMap((b) => b.blockers || []),
    internationalReferenceLabels: intlLabels,
    calaFirstPropertyImageChoices: calaChoices,
    rejectedWrongOrSiblingBrandCandidates: rejectedWrongBrand,
    soStewardGapsIntentionallyNotFilled,
    readyStatement:
      readyCountPre === brandPlans.length && brandPlans.length > 0 && brandPlans.every((b) => !b.blocked)
        ? "wave13_image_materialization_ready_for_post_image_cleanup"
        : "wave13_image_materialization_blocked",
    counts: {
      brands: brandPlans.length,
      ready: brandPlans.filter((b) => !b.blocked).length,
      blocked: brandPlans.filter((b) => b.blocked).length,
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
    guardrails: {
      noBrandStatus: true,
      noReleaseFields: true,
      noCompanyValidated: true,
      noSourceLibrary: true,
      noRegistry: true,
      noProtected39: true,
      noHouseOfOriginals: true,
      noMorgansOriginals: true,
      noRadissonCollection: true,
      noSoStewardDataFills: true,
      noContentRewrites: true,
      targetBrandsOnly: true,
      imageUniquenessRequired: true,
      imageRoleMatchRequired: true,
      calaFirstOpenings: true,
      internationalReferenceLabels: true,
    },
  };

  const readyCount = summary.counts.ready;
  const blockedCount = summary.counts.blocked;

  const md = [
    `# Wave 13 Stage 5 — Image / Visual Materialization`,
    ``,
    `- Generated: ${summary.generatedAt}`,
    `- Mode: **${apply ? "APPLY" : "DRY-RUN"}**`,
    `- Ready: **${readyCount}/${brandPlans.length}** · Blocked: **${blockedCount}**`,
    `- Patches planned: **${summary.counts.patches}**`,
    `- Scenario image failures before materialization: **${scenarioFailuresBefore}**`,
    `- Ready statement: \`${summary.readyStatement}\``,
    ``,
    `## Brand results`,
    ``,
    ...brandPlans.map(
      (b) =>
        `- **${b.brandName || b.brandSlug}**: ${b.blocked ? `BLOCKED (${(b.blockers || []).join(", ")})` : `ready · g${b.counts?.gallery}/6 s${b.counts?.scenario}/3 o${b.counts?.property}/3`}`
    ),
    ``,
    `## SO/ steward-data gaps (intentionally not filled)`,
    ``,
    ...soStewardGapsIntentionallyNotFilled.map((g) => `- ${g}`),
    ``,
    `## Apply flags`,
    ``,
    ...WAVE13_IMAGE_MATERIALIZATION_APPLY_FLAGS.map((f) => `- \`${f}\``),
    ``,
  ].join("\n");

  writeReports("brand-explorer-wave13-image-materialization", summary, md);

  fs.mkdirSync(DOCS_DIR, { recursive: true });
  const docsPath = path.join(DOCS_DIR, "brand-explorer-wave13-image-materialization.md");
  fs.writeFileSync(
    docsPath,
    [
      `# Wave 13 — Image / Visual Materialization`,
      ``,
      `Stage 5 materializes gallery (6), scenario (3), and openings/property (3) images for the **seven** Wave 13 Stage 4–approved brands only.`,
      ``,
      `## Scope`,
      ``,
      ...WAVE13_STAGE4_APPROVED_SLUGS.map((s) => `- \`${s}\``),
      ``,
      `Excluded: The House of Originals, Morgans Originals, Radisson Collection, protected 39, non-targets.`,
      ``,
      `## Commands`,
      ``,
      "```bash",
      `npm run brand-explorer-wave13-factory -- --stage image-materialization --dry-run`,
      `npm run brand-explorer-wave13-factory -- --stage image-materialization --apply \\`,
      ...WAVE13_IMAGE_MATERIALIZATION_APPLY_FLAGS.map((f, i, arr) =>
        i === arr.length - 1 ? `  ${f}` : `  ${f} \\`
      ),
      "```",
      ``,
      `## Guardrails`,
      ``,
      `- Target brands only (seven Stage 5 brands)`,
      `- Image / caption fields only — no Presentation Body content rewrites`,
      `- No Brand Status / release / CV / Source Library / Registry writes`,
      `- No SO/ steward fills (\`snapshot.*\`, \`footprint.primary_regions\`)`,
      `- No protected 39 image or content writes`,
      `- CALA-first openings; International Reference labels when non-CALA`,
      `- Image uniqueness + role-match required (Scene7/filename-aware)`,
      ``,
      `## Fixtures`,
      ``,
      `- \`fixtures/wave13-{slug}-gallery-pool.json\` (from \`scripts/harvest-wave13-image-pools.mjs\`)`,
      ``,
      `## Reports`,
      ``,
      `- \`reports/brand-explorer-wave13-image-materialization.{json,md}\``,
      `- \`reports/brand-explorer-wave13-image-materialization-{slug}.md\``,
      ``,
      `## Ready statement`,
      ``,
      `\`wave13_image_materialization_ready_for_post_image_cleanup\``,
      ``,
    ].join("\n"),
    "utf8"
  );

  return summary;
}
