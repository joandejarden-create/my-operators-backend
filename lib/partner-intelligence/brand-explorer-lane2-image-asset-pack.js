/**
 * Lane 2 — curated image asset packs from fixtures + property catalogs.
 */
import fs from "fs";
import path from "path";
import {
  LANE2_VERSION,
  GALLERY_MIN,
  SCENARIO_MIN,
  PROPERTY_MIN,
  GALLERY_ROLE_TITLES,
  resolveFullBuildSlug,
  resolveLane2BrandIdentity,
  refuseProtectedOrOutOfCohort,
  loadLane2GalleryPool,
  normalizePoolAssets,
  writeLane2Reports,
  LANE2_ROOT,
} from "./brand-explorer-lane2-common.js";
import { LANE2_PROPERTY_CATALOG_BY_SLUG } from "./brand-explorer-lane2-property-catalog.js";
import {
  buildOpeningsPropertyCardTitle,
  buildOpeningsPropertyCardBody,
} from "./brand-explorer-openings-property-card-contract.js";
import { pickDistinctImageAssets, evaluateImageUniqueness, buildImageIdentity } from "./brand-explorer-image-uniqueness.js";
import {
  evaluateBrandImageRoleMatch,
  detectVisualCategory,
  IMAGE_ROLES,
} from "./brand-explorer-image-role-match.js";
import { pickRoleMatchedGalleryAssets } from "./brand-explorer-gallery-selection.js";

export const REPORT_JSON = "brand-explorer-lane2-image-asset-pack.json";
export const REPORT_MD = "brand-explorer-lane2-image-asset-pack.md";

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function assignGalleryCaptions(assets) {
  return assets.map((a, i) => ({
    ...a,
    role: "gallery",
    slotKey: a.slotKey || `materials.gallery.${i + 1}`,
    caption: a.caption || GALLERY_ROLE_TITLES[i] || `Gallery ${i + 1}`,
    title:
      a.title ||
      (a.propertyName
        ? `${GALLERY_ROLE_TITLES[i] || "Gallery"} — ${a.propertyName}`
        : GALLERY_ROLE_TITLES[i] || `Gallery ${i + 1}`),
  }));
}

function assignScenarioCaptions(assets) {
  return assets.map((a, i) => ({
    ...a,
    role: "scenario",
    slotKey: `overview.scenario.${i + 1}`,
    caption: `Owner scenario ${i + 1}`,
    title: `Scenario ${i + 1}${a.marketCity ? ` — ${a.marketCity}` : ""}`,
  }));
}

function assignPropertyCaptions(assets, catalog, brandName = "") {
  return assets.map((a, i) => {
    const cat =
      catalog.find((c) => c.propertyKey === a.propertyKey) ||
      catalog.find((c) => nz(c.propertyName).toLowerCase() === nz(a.propertyName).toLowerCase()) ||
      null;
    const propertyName = cat?.propertyName || a.propertyName || `Property ${i + 1}`;
    const market = cat?.marketCity || a.marketCity || "";
    const geo = nz(cat?.geographyLabel);
    const chips = [
      geo.split("/")[0]?.trim() || "Market",
      market || "City",
      geo.includes("Heritage") ? "Heritage" : "Urban",
      "Collection",
    ]
      .filter(Boolean)
      .join(", ");
    const locationLine = market
      ? `${market}${geo ? ` (${geo})` : ""}`
      : geo || "Market location";
    const metaLine = geo || market || "Property example";
    const scenarioLine = chips
      .split(",")
      .map((s) => s.trim().toUpperCase())
      .filter(Boolean)
      .join(" / ");
    const teaser =
      cat?.teaser ||
      a.caption ||
      `${propertyName} in ${market || "this market"} is a collection property reference for owners underwriting design narrative, capital intensity, and systems participation—confirm live affiliation criteria for the specific asset.`;
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
        sourceUrl: cat?.sourcePageUrl || "",
      });
    } catch {
      body = `${chips}\n\n${locationLine}\n\n${metaLine}\n\n${scenarioLine}\n\n${teaser}`;
    }
    return {
      ...a,
      role: "property_example",
      slotKey: "footprint.openings",
      planSlotKey: `footprint.openings.${i + 1}`,
      caption: propertyName,
      title,
      body,
      catalogEntry: cat,
      caseSummaryOverview: teaser,
      caseSummaryTags: chips,
      caseSummaryBrandRelevance:
        "Official property photography used as a Brand Explorer property example for this collection.",
      caseSummaryOwnerObjective:
        "Use as a directional property reference when underwriting design intensity, capital scope, and platform fit.",
      caseSummaryInterpretation:
        "Confirm live affiliation criteria and property-specific scope with the brand before underwriting.",
      teaser,
      marketCity: market,
      propertyName,
    };
  });
}

export function buildLane2ImageAssetPackForBrand(brandSlug) {
  const refuse = refuseProtectedOrOutOfCohort(brandSlug);
  if (refuse.refused) {
    return {
      brandSlug: resolveFullBuildSlug(brandSlug),
      status: "refused",
      pass: false,
      blockers: [refuse.reason],
    };
  }

  const slug = refuse.brandSlug;
  const identity = resolveLane2BrandIdentity(slug);
  const catalog = LANE2_PROPERTY_CATALOG_BY_SLUG[slug] || [];
  const rawPool = loadLane2GalleryPool(slug);
  const catalogByKey = new Map(catalog.map((c) => [c.propertyKey, c]));
  const enrichedPool = rawPool.map((row) => {
    const cat = catalogByKey.get(row.propertyKey) || null;
    return {
      ...row,
      geographyLabel: nz(row.geographyLabel) || nz(cat?.geographyLabel) || "",
      propertyName: nz(row.propertyName) || nz(cat?.propertyName) || "",
      marketCity: nz(row.marketCity) || nz(cat?.marketCity) || "",
    };
  });
  const { accepted, rejections } = normalizePoolAssets(enrichedPool, slug);

  const blockers = [];
  if (!rawPool.length) blockers.push("empty_fixture_pool");
  if (!accepted.length) blockers.push("no_accepted_official_images");

  const galleryPick = pickRoleMatchedGalleryAssets(accepted, GALLERY_MIN);
  const galleryDistinct = galleryPick.assets;
  const galleryGroupIds = galleryDistinct.map((a) => a._imageIdentity?.duplicateGroupId).filter(Boolean);

  // Prefer one image per distinct property for property examples.
  const propertyPool = accepted.filter((a) => a.propertyKey);
  const propertyByKey = new Map();
  for (const asset of accepted) {
    if (!asset.propertyKey) continue;
    if (propertyByKey.has(asset.propertyKey)) continue;
    const id = buildImageIdentity(asset.imageUrl, { propertyName: asset.propertyName });
    if (galleryGroupIds.includes(id.duplicateGroupId)) continue;
    propertyByKey.set(asset.propertyKey, { ...asset, _imageIdentity: id });
  }
  let propertyDistinct = [...propertyByKey.values()].slice(0, PROPERTY_MIN);
  if (propertyDistinct.length < PROPERTY_MIN) {
    propertyDistinct = pickDistinctImageAssets(propertyPool, PROPERTY_MIN, {
      excludeGroupIds: galleryGroupIds,
    });
  }
  const usedGroups = [
    ...galleryGroupIds,
    ...propertyDistinct.map((a) => a._imageIdentity?.duplicateGroupId).filter(Boolean),
  ];
  const scenarioPool = accepted.filter((a) => !usedGroups.includes(a._imageIdentity?.duplicateGroupId));
  let scenarioDistinct = pickDistinctImageAssets(scenarioPool, SCENARIO_MIN, {
    excludeGroupIds: usedGroups,
  });
  if (scenarioDistinct.length < SCENARIO_MIN) {
    scenarioDistinct = pickDistinctImageAssets(accepted, SCENARIO_MIN, {
      excludeGroupIds: galleryGroupIds,
    });
  }

  if (galleryDistinct.length < GALLERY_MIN) {
    blockers.push(`gallery_distinct_${galleryDistinct.length}_lt_${GALLERY_MIN}`);
  }
  if (galleryPick.inventedRoleCaptions > 0) {
    blockers.push(
      `gallery_role_uncurable_without_metadata_${galleryPick.inventedRoleCaptions}`
    );
  }
  if (propertyDistinct.length < PROPERTY_MIN) {
    blockers.push(`property_distinct_${propertyDistinct.length}_lt_${PROPERTY_MIN}`);
  }
  if (scenarioDistinct.length < SCENARIO_MIN) {
    blockers.push(`scenario_distinct_${scenarioDistinct.length}_lt_${SCENARIO_MIN}`);
  }

  const galleryPack = assignGalleryCaptions(galleryDistinct.slice(0, GALLERY_MIN));
  const propertyPack = assignPropertyCaptions(
    propertyDistinct.slice(0, PROPERTY_MIN),
    catalog,
    identity?.name || ""
  );
  const scenarioPack = assignScenarioCaptions(scenarioDistinct.slice(0, SCENARIO_MIN));

  const visualAssetPack = {
    galleryCandidates: galleryPack,
    propertyExampleCandidates: propertyPack,
    scenarioCandidates: scenarioPack,
  };

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
  const status = pass ? "asset_pack_ready" : "blocked_missing_images";

  return {
    brandSlug: slug,
    reportSlug: identity.reportSlug,
    brandName: identity.name,
    recordId: identity.recordId,
    status,
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
    visualAssetPack,
    uniqueness,
    roleMatch,
    eligibility: {
      asset_pack_ready: pass,
      materialization_allowed: pass,
    },
  };
}

export function writeLane2ImageAssetPackBrandReport(brandResult, { reportsDir }) {
  const slug = brandResult.reportSlug || brandResult.brandSlug;
  const mdPath = path.join(reportsDir, `brand-explorer-lane2-image-asset-pack-${slug}.md`);
  const md = [
    `# Lane 2 image asset pack — ${brandResult.brandName || brandResult.brandSlug}`,
    ``,
    `- Status: **${brandResult.status}**`,
    `- Pass: **${brandResult.pass}**`,
    `- Fixture rows: ${brandResult.poolStats?.fixtureRows ?? 0} (accepted ${brandResult.poolStats?.acceptedRows ?? 0})`,
    ``,
    brandResult.blockers?.length
      ? `## Blockers\n\n${brandResult.blockers.map((b) => `- ${b}`).join("\n")}\n`
      : "## Blockers\n\n_none_\n",
    ``,
    `## Counts`,
    ``,
    `- Gallery: ${brandResult.counts?.gallery ?? 0}/${GALLERY_MIN}`,
    `- Property: ${brandResult.counts?.property ?? 0}/${PROPERTY_MIN}`,
    `- Scenario: ${brandResult.counts?.scenario ?? 0}/${SCENARIO_MIN}`,
    ``,
  ];
  writeLane2Reports({
    jsonPath: mdPath.replace(/\.md$/, ".json"),
    mdPath,
    json: brandResult,
    mdLines: md,
  });
  return mdPath;
}

export function runLane2ImageAssetPack({ brands = [], reportsDir = path.join(LANE2_ROOT, "reports") } = {}) {
  const brandResults = brands.map((b) => buildLane2ImageAssetPackForBrand(b));
  for (const br of brandResults) {
    writeLane2ImageAssetPackBrandReport(br, { reportsDir });
  }

  const result = {
    version: LANE2_VERSION,
    lane: "image-asset-pack",
    generatedAt: new Date().toISOString(),
    brands: brandResults.map((b) => b.brandSlug),
    brandResults,
    summary: {
      brandCount: brandResults.length,
      readyCount: brandResults.filter((b) => b.status === "asset_pack_ready").length,
      blockedCount: brandResults.filter((b) => b.status === "blocked_missing_images").length,
      blockedSlugs: brandResults.filter((b) => !b.pass).map((b) => b.brandSlug),
    },
  };

  const md = [
    `# Lane 2 — Image asset pack`,
    ``,
    `- Generated: ${result.generatedAt}`,
    `- Ready: **${result.summary.readyCount}/${result.summary.brandCount}**`,
    `- Blocked: ${result.summary.blockedSlugs.join(", ") || "—"}`,
    ``,
    `## Per brand`,
    ``,
    ...brandResults.map(
      (b) =>
        `- **${b.brandSlug}**: \`${b.status}\`${b.blockers?.length ? ` — ${b.blockers.join("; ")}` : ""}`
    ),
    ``,
  ];

  writeLane2Reports({
    jsonPath: path.join(reportsDir, REPORT_JSON),
    mdPath: path.join(reportsDir, REPORT_MD),
    json: result,
    mdLines: md,
  });

  return result;
}

export function readLane2ImageAssetPackReport(reportsDir = path.join(LANE2_ROOT, "reports")) {
  const p = path.join(reportsDir, REPORT_JSON);
  if (!fs.existsSync(p)) {
    throw new Error(`Missing asset pack report: ${p}. Run image-asset-pack lane first.`);
  }
  return JSON.parse(fs.readFileSync(p, "utf8"));
}
