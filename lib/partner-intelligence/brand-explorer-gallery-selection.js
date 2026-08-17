/**
 * Brand Explorer gallery selection — CALA-first, role mix, property variety.
 *
 * Ideal state for materials.gallery.1–6:
 * 1. Prefer CALA / LATAM properties when the pool has them
 * 2. Show role variety: Exterior, Guest Room, Public Space, F&B (+ detail/setting)
 * 3. Prefer multiple properties (not a mono-property pack) when inventory allows
 * 4. Never reuse the same duplicateGroupId
 * 5. Fall back to International Reference only when no CALA inventory exists
 */
import {
  buildImageIdentity,
  pickDistinctImageAssets,
} from "./brand-explorer-image-uniqueness.js";
import {
  DEFAULT_GALLERY_ROLE_SEQUENCE,
  GALLERY_ROLE_CAPTIONS,
  IMAGE_ROLES,
  detectVisualCategory,
} from "./brand-explorer-image-role-match.js";

export const GALLERY_SELECTION_VERSION = "gallery-selection-cala-variety-v1";

export const GALLERY_MIN = 6;
export const GALLERY_CORE_ROLES = Object.freeze([
  IMAGE_ROLES.exterior_arrival,
  IMAGE_ROLES.guest_room_suite,
  IMAGE_ROLES.public_space_lobby,
  IMAGE_ROLES.food_beverage_experience,
]);

/** Soft cap: avoid stacking one hotel across the whole gallery when alternatives exist. */
export const GALLERY_MAX_PER_PROPERTY_WHEN_ALTERNATIVES = 2;
export const GALLERY_MIN_DISTINCT_PROPERTIES_WHEN_AVAILABLE = 3;

function nz(v) {
  return v == null ? "" : String(v).trim();
}

export function isCalaGeographyLabel(label) {
  const t = nz(label);
  if (!t) return false;
  if (/^international\s+reference\b/i.test(t)) return false;
  return /\bCALA\b/i.test(t) || /\bLATAM\b/i.test(t) || /\bLatin America\b/i.test(t);
}

export function propertyKeyOf(asset) {
  return (
    nz(asset?.propertyKey) ||
    nz(asset?.propertyName).toLowerCase() ||
    nz(asset?.matchKey).toLowerCase() ||
    ""
  );
}

export function resolveGalleryAssetRole(asset) {
  const fixtureRole = nz(asset?.role);
  if (fixtureRole && Object.values(IMAGE_ROLES).includes(fixtureRole)) return fixtureRole;
  const det = detectVisualCategory({
    imageUrl: asset?.imageUrl,
    sourcePageUrl: asset?.sourcePageUrl,
    title: nz(asset?.caption) || nz(asset?.title) || nz(asset?.label),
    altText: nz(asset?.caption),
    filename: nz(asset?.filename) || nz(asset?.imageFilename) || nz(asset?.probedSuffix),
  });
  if (det.category && det.category !== IMAGE_ROLES.unknown) return det.category;
  const blob = [asset?.imageUrl, asset?.probedSuffix].map(nz).join(" ").toLowerCase();
  if (/guest[_\s-]?room|guestroom|suite_\d|bedroom/i.test(blob)) return IMAGE_ROLES.guest_room_suite;
  if (/restaurant|bar_\d|_bar_/i.test(blob)) return IMAGE_ROLES.food_beverage_experience;
  if (/lobby|public_space|public_area/i.test(blob)) return IMAGE_ROLES.public_space_lobby;
  if (/exterior|arrival|facade/i.test(blob)) return IMAGE_ROLES.exterior_arrival;
  if (/terrace|aerial|pool|patio/i.test(blob)) return IMAGE_ROLES.property_setting;
  if (/detail|bathroom/i.test(blob)) return IMAGE_ROLES.design_detail;
  return null;
}

function sortPoolForPreference(assets, { preferCala = true } = {}) {
  const hasCala = preferCala && assets.some((a) => isCalaGeographyLabel(a.geographyLabel));
  return [...assets].sort((a, b) => {
    if (hasCala) {
      const aC = isCalaGeographyLabel(a.geographyLabel) ? 0 : 1;
      const bC = isCalaGeographyLabel(b.geographyLabel) ? 0 : 1;
      if (aC !== bC) return aC - bC;
    }
    return 0;
  });
}

function countByProperty(picked) {
  const map = new Map();
  for (const a of picked) {
    const k = propertyKeyOf(a) || "_unknown";
    map.set(k, (map.get(k) || 0) + 1);
  }
  return map;
}

function distinctPropertyCount(assets) {
  return new Set(assets.map(propertyKeyOf).filter(Boolean)).size;
}

/**
 * Pick up to minCount gallery assets with role mix + CALA-first + property variety.
 */
export function pickRoleMatchedGalleryAssets(accepted = [], minCount = GALLERY_MIN, opts = {}) {
  const preferCala = opts.preferCala !== false;
  const roleSequence = opts.roleSequence || DEFAULT_GALLERY_ROLE_SEQUENCE;
  const maxPerProperty = opts.maxPerProperty ?? GALLERY_MAX_PER_PROPERTY_WHEN_ALTERNATIVES;

  const pool = sortPoolForPreference(accepted, { preferCala });
  const availableProperties = distinctPropertyCount(pool);
  const enforcePropertyCap = availableProperties >= GALLERY_MIN_DISTINCT_PROPERTIES_WHEN_AVAILABLE;

  const usedGroupIds = [];
  const picked = [];
  let inventedRoleCaptions = 0;

  function identityOf(asset) {
    return buildImageIdentity(asset.imageUrl, {
      propertyName: asset.propertyName,
      filename: nz(asset.filename) || nz(asset.imageFilename) || nz(asset.probedSuffix) || undefined,
    });
  }

  function canUseProperty(asset) {
    if (!enforcePropertyCap) return true;
    const key = propertyKeyOf(asset) || "_unknown";
    const counts = countByProperty(picked);
    return (counts.get(key) || 0) < maxPerProperty;
  }

  function pickOne(rolePool) {
    const eligible = rolePool.filter((asset) => {
      const id = identityOf(asset);
      if (usedGroupIds.includes(id.duplicateGroupId)) return false;
      return canUseProperty(asset);
    });
    const usedProps = new Set(picked.map(propertyKeyOf).filter(Boolean));
    const ranked = [...eligible].sort((a, b) => {
      const aUsed = usedProps.has(propertyKeyOf(a)) ? 1 : 0;
      const bUsed = usedProps.has(propertyKeyOf(b)) ? 1 : 0;
      if (aUsed !== bUsed) return aUsed - bUsed;
      if (preferCala) {
        const aC = isCalaGeographyLabel(a.geographyLabel) ? 0 : 1;
        const bC = isCalaGeographyLabel(b.geographyLabel) ? 0 : 1;
        if (aC !== bC) return aC - bC;
      }
      return 0;
    });
    const distinct = pickDistinctImageAssets(ranked, 1, { excludeGroupIds: usedGroupIds });
    return distinct[0] || null;
  }

  for (const role of roleSequence) {
    if (picked.length >= minCount) break;
    const rolePool = pool.filter((asset) => resolveGalleryAssetRole(asset) === role);
    let chosen = pickOne(rolePool);
    if (!chosen && enforcePropertyCap) {
      const rolePoolAny = rolePool.filter((asset) => {
        const id = identityOf(asset);
        return !usedGroupIds.includes(id.duplicateGroupId);
      });
      const distinct = pickDistinctImageAssets(rolePoolAny, 1, { excludeGroupIds: usedGroupIds });
      chosen = distinct[0] || null;
    }
    if (!chosen) continue;
    usedGroupIds.push(chosen._imageIdentity.duplicateGroupId);
    const caption = GALLERY_ROLE_CAPTIONS[role] || "Gallery";
    picked.push({
      ...chosen,
      assignedRole: role,
      title: chosen.propertyName ? `${caption} — ${chosen.propertyName}` : caption,
    });
  }

  while (picked.length < minCount) {
    const usedRoles = new Set(picked.map((p) => p.assignedRole).filter(Boolean));
    // Prefer unused roles / non-exterior when filling remaining slots
    const fillRanked = [...pool].sort((a, b) => {
      const ra = resolveGalleryAssetRole(a);
      const rb = resolveGalleryAssetRole(b);
      const aUnused = ra && !usedRoles.has(ra) ? 0 : 1;
      const bUnused = rb && !usedRoles.has(rb) ? 0 : 1;
      if (aUnused !== bUnused) return aUnused - bUnused;
      const aExt = ra === IMAGE_ROLES.exterior_arrival && usedRoles.has(IMAGE_ROLES.exterior_arrival) ? 1 : 0;
      const bExt = rb === IMAGE_ROLES.exterior_arrival && usedRoles.has(IMAGE_ROLES.exterior_arrival) ? 1 : 0;
      if (aExt !== bExt) return aExt - bExt;
      return 0;
    });
    let chosen = pickOne(fillRanked);
    if (!chosen && enforcePropertyCap) {
      const any = pickDistinctImageAssets(fillRanked, 1, { excludeGroupIds: usedGroupIds });
      chosen = any[0] || null;
    }
    if (!chosen) break;
    usedGroupIds.push(chosen._imageIdentity.duplicateGroupId);
    const role = resolveGalleryAssetRole(chosen);
    if (role) {
      const caption = GALLERY_ROLE_CAPTIONS[role] || "Gallery";
      picked.push({
        ...chosen,
        assignedRole: role,
        title: chosen.propertyName ? `${caption} — ${chosen.propertyName}` : caption,
      });
    } else {
      inventedRoleCaptions += 1;
      picked.push({
        ...chosen,
        assignedRole: null,
        title: chosen.propertyName
          ? `Property photography — ${chosen.propertyName}`
          : "Property photography",
      });
    }
  }

  const assets = picked.slice(0, minCount).map((a, i) => ({
    ...a,
    role: "gallery",
    slotKey: `materials.gallery.${i + 1}`,
    caption: a.assignedRole
      ? GALLERY_ROLE_CAPTIONS[a.assignedRole]
      : a.title || `Gallery ${i + 1}`,
    title:
      a.title ||
      (a.propertyName ? `Property photography — ${a.propertyName}` : "Property photography"),
  }));

  return {
    assets,
    inventedRoleCaptions,
    diagnostics: evaluateGallerySelectionBar(assets, { pool }),
  };
}

/**
 * Evaluate a planned or live gallery set against the variety / CALA ideal.
 */
export function evaluateGallerySelectionBar(galleryAssets = [], { pool = [] } = {}) {
  const assets = (galleryAssets || []).filter((a) => nz(a.imageUrl));
  const failures = [];
  const warnings = [];

  const groupIds = assets.map((a) => buildImageIdentity(a.imageUrl).duplicateGroupId).filter(Boolean);
  if (new Set(groupIds).size < groupIds.length) {
    failures.push("duplicate_gallery_images");
  }

  const props = assets.map(propertyKeyOf).filter(Boolean);
  const distinctProps = new Set(props).size;
  const poolProps = distinctPropertyCount(pool.length ? pool : assets);
  const poolHasCala = (pool.length ? pool : assets).some((a) =>
    isCalaGeographyLabel(a.geographyLabel)
  );
  const galleryHasCala = assets.some((a) => isCalaGeographyLabel(a.geographyLabel));

  if (poolHasCala && !galleryHasCala) {
    failures.push("cala_inventory_available_but_unused");
  }

  if (poolProps >= GALLERY_MIN_DISTINCT_PROPERTIES_WHEN_AVAILABLE && distinctProps < 2) {
    failures.push(`mono_property_gallery:${props[0] || "unknown"}`);
  } else if (
    poolProps >= GALLERY_MIN_DISTINCT_PROPERTIES_WHEN_AVAILABLE &&
    distinctProps < Math.min(GALLERY_MIN_DISTINCT_PROPERTIES_WHEN_AVAILABLE, poolProps)
  ) {
    warnings.push(`low_property_variety:${distinctProps}_of_${poolProps}`);
  }

  const roles = assets.map((a) => a.assignedRole || resolveGalleryAssetRole(a)).filter(Boolean);
  const roleSet = new Set(roles);
  for (const core of GALLERY_CORE_ROLES) {
    const poolHasRole = (pool.length ? pool : assets).some(
      (a) => resolveGalleryAssetRole(a) === core
    );
    if (poolHasRole && !roleSet.has(core)) {
      failures.push(`missing_core_role:${core}`);
    }
  }

  const exteriorCount = roles.filter((r) => r === IMAGE_ROLES.exterior_arrival).length;
  if (exteriorCount >= 3 && roleSet.size < 3) {
    failures.push("exterior_over_concentration");
  }

  return {
    pass: failures.length === 0,
    failures,
    warnings,
    checks: {
      gallerySelectionVersion: GALLERY_SELECTION_VERSION,
      distinctProperties: distinctProps,
      poolDistinctProperties: poolProps,
      poolHasCala,
      galleryHasCala,
      distinctRoles: roleSet.size,
      roles: [...roleSet],
      imageCount: assets.length,
      duplicateGroups: groupIds.length - new Set(groupIds).size,
    },
  };
}

/**
 * Infer Canopy property names from Hilton Stories URL stems when harvest
 * incorrectly attributed everything to the first property page.
 */
export function inferHiltonCanopyPropertyFromUrl(imageUrl) {
  const u = decodeURIComponent(nz(imageUrl)).toLowerCase();
  if (/san-francisco-soma|san_francisco_soma|sf.?soma/.test(u)) {
    return {
      propertyName: "Canopy by Hilton San Francisco SoMa",
      marketCity: "San Francisco",
      geographyLabel: "International Reference",
      propertyKey: "canopy-sf-soma",
    };
  }
  if (/cape-town|longkloof/.test(u)) {
    return {
      propertyName: "Canopy by Hilton Cape Town Longkloof",
      marketCity: "Cape Town",
      geographyLabel: "International Reference",
      propertyKey: "canopy-cape-town",
    };
  }
  if (/nashville/.test(u)) {
    return {
      propertyName: "Canopy by Hilton Nashville Downtown The Gulch",
      marketCity: "Nashville",
      geographyLabel: "International Reference",
      propertyKey: "canopy-nashville",
    };
  }
  if (/cannes/.test(u)) {
    return {
      propertyName: "Canopy by Hilton Cannes",
      marketCity: "Cannes",
      geographyLabel: "International Reference",
      propertyKey: "canopy-cannes",
    };
  }
  if (/istanbul|taksim/.test(u)) {
    return {
      propertyName: "Canopy by Hilton Istanbul Taksim",
      marketCity: "Istanbul",
      geographyLabel: "International Reference",
      propertyKey: "canopy-istanbul",
    };
  }
  if (/portland|pearl/.test(u)) {
    return {
      propertyName: "Canopy by Hilton Portland Pearl District",
      marketCity: "Portland",
      geographyLabel: "International Reference",
      propertyKey: "canopy-portland",
    };
  }
  if (/wharf|washington|waswhcp/.test(u)) {
    return {
      propertyName: "Canopy by Hilton Washington DC The Wharf",
      marketCity: "Washington DC",
      geographyLabel: "International Reference",
      propertyKey: "canopy-dc-wharf",
    };
  }
  if (/reykjavik|rekcpcp/.test(u)) {
    return {
      propertyName: "Canopy by Hilton Reykjavik City Centre",
      marketCity: "Reykjavik",
      geographyLabel: "International Reference",
      propertyKey: "canopy-reykjavik",
    };
  }
  return null;
}

export function repairCanopyGalleryPoolPropertyAttribution(rows = []) {
  return (rows || []).map((row) => {
    const inferred = inferHiltonCanopyPropertyFromUrl(row.imageUrl);
    if (!inferred) return row;
    return {
      ...row,
      propertyName: inferred.propertyName,
      propertyKey: inferred.propertyKey || row.propertyKey,
      marketCity: inferred.marketCity || row.marketCity,
      geographyLabel: inferred.geographyLabel || row.geographyLabel || "International Reference",
    };
  });
}
