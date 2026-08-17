/**
 * Brand Explorer image uniqueness — distinct images, not populated slots.
 *
 * Distinctness is based on canonical URL + CDN/Scene7 asset identity.
 * Near-duplicates (crops / size variants of the same asset) share a group id.
 */
import { normalizeUrlKey } from "./brand-explorer-brand-asset-image-governance.js";

export const IMAGE_UNIQUENESS_VERSION = "image-uniqueness-v3";
export const GALLERY_DISTINCT_MIN = 6;
export const SCENARIO_DISTINCT_MIN = 3;
export const PROPERTY_DISTINCT_MIN = 3;

function nz(v) {
  return v == null ? "" : String(v).trim();
}

/**
 * Strip size / crop suffixes from filenames so variants collide.
 */
export function normalizeFilenameStem(name) {
  let stem = nz(name)
    .toLowerCase()
    .split("?")[0]
    .replace(/\.(jpe?g|png|webp|gif|avif)(\.jpe?g)?$/i, "");
  // Marriott Scene7 named crops: asset:Wide-Hor / :Classic-Hor / :Feature-Hor / :Pano-Hor / :Square
  stem = stem.replace(/:(?:wide|classic|feature|pano|square)(?:-hor)?$/i, "");
  // coral-reef-club-78484-xl-1 / -l-1 / -s-6 → coral-reef-club-78484
  stem = stem.replace(/-(?:xl|l|m|s|xs|thumb|hero|full|web)-\d+$/i, "");
  // Pixel / WordPress size variants: mhr_1189921-1024x576 / _2500x1875 / -scaled
  stem = stem.replace(/[-_]\d{2,5}x\d{2,5}$/i, "");
  stem = stem.replace(/[-_](?:scaled|original|fullsize|large|medium|small|thumb|preview)$/i, "");
  stem = stem.replace(/_\d{2,4}x\d{2,4}$/i, "");
  stem = stem.replace(/([:_-])(crop|resize|thumb|hero|detail|wid|hei|qlt)[_-]?\d*$/gi, "");
  stem = stem.replace(/[-_](sm|md|lg|xl|thumb|hero|crop)\d*$/i, "");
  // IHG / Marriott Scene7 aspect-ratio variants: …-10958437436-2x1 / -4x3 / -3x2 / -16x5
  stem = stem.replace(/-\d{1,2}x\d{1,2}$/i, "");
  // Marriott DAM numeric asset ids: mhr_1189921 / mhrs.1238136
  const marriottDam = stem.match(/\b(mhrs?\.\d+|mhr[_-]?\d{5,})\b/i);
  if (marriottDam) return marriottDam[1].toLowerCase();
  const dji = stem.match(/dji[_-]?\d+/i);
  if (dji) return `dji:${dji[0].toLowerCase()}`;
  return stem;
}

/**
 * Strip Scene7 / Akamai / CDN / size-variant suffixes so crops of the same asset collide.
 * Accor scenes keep the sequence (ho_00 vs ho_01) so different photographs stay distinct.
 */
export function extractSourceImageId(url, filename = "") {
  const rawIn = nz(url);
  let raw = rawIn;
  // Unwrap Airtable fetch proxies so Accor/Marriott asset IDs remain recoverable.
  try {
    if (/wsrv\.nl\/|images\.weserv\.nl\//i.test(raw)) {
      const u = new URL(raw);
      const nested = u.searchParams.get("url");
      if (nested) raw = nested;
    }
  } catch {
    /* keep raw */
  }
  let pathname = "";
  try {
    pathname = raw ? new URL(raw).pathname || "" : "";
  } catch {
    pathname = raw.split("?")[0];
  }
  const path = pathname.replace(/\/+$/, "").toLowerCase();

  // Accor ahstatic: keep hotel + type + sequence so ho_00 ≠ ho_01 (different photographs).
  // Type may be compound (roqec, bab001, rsr001) — not only exact 2-letter codes.
  let m = path.match(
    /\/photos\/([a-z0-9]+)_((?:ho|ro|su|ba|re|bar|br|bab|rsr|sp|sw|fi|me|sm|wd|fu|xx|lobby)[a-z0-9]*)_(\d+)/i
  );
  if (m) return `ahstatic:${m[1].toLowerCase()}_${m[2].toLowerCase()}_${m[3]}`;

  const fileStem = normalizeFilenameStem(filename);
  if (fileStem) {
    m = fileStem.match(
      /^([a-z0-9]+)_((?:ho|ro|su|ba|re|bar|br|bab|rsr|sp|sw|fi|me|sm|wd|fu|xx|lobby)[a-z0-9]*)_(\d+)/i
    );
    if (m) return `ahstatic:${m[1].toLowerCase()}_${m[2].toLowerCase()}_${m[3]}`;
    // Scene7-style attachment filenames (…-10958437436 / …-10958437436-2x1) must share
    // the same identity as live Scene7 CDN URLs so crop/aspect variants collide.
    if (/\d{7,}/.test(fileStem) && fileStem.length >= 6) {
      return `scene7:${fileStem}`;
    }
    if (fileStem.length >= 6) return `file:${fileStem}`;
  }

  if (!raw) return "";

  m = path.match(/\/is\/image\/(?:ihg|marriott|[^/]+)\/([^/:]+)/i);
  if (m) {
    // Collapse …-10958437436-2x1 and …-10958437436-4x3 to the same Scene7 asset.
    const scene7Stem = normalizeFilenameStem(m[1]);
    return `scene7:${scene7Stem || m[1].toLowerCase()}`;
  }

  const base = path.split("/").pop() || "";
  const stem = normalizeFilenameStem(base);
  if (stem.length >= 6 && !/airtableusercontent/i.test(raw)) {
    m = stem.match(
      /^([a-z0-9]+)_((?:ho|ro|su|ba|re|bar|br|bab|rsr|sp|sw|fi|me|sm|wd|fu|xx|lobby)[a-z0-9]*)_(\d+)/i
    );
    if (m) return `ahstatic:${m[1].toLowerCase()}_${m[2].toLowerCase()}_${m[3]}`;
    return `file:${stem}`;
  }

  if (/airtableusercontent\.com/i.test(raw) && stem.length >= 12) {
    return `airtable:${stem}`;
  }
  if (stem.length >= 6) return `file:${stem}`;
  return `path:${path}`;
}

export function canonicalizeImageUrl(url) {
  return normalizeUrlKey(url);
}

function extractPropertyFromTitle(title) {
  const t = nz(title);
  if (!t.includes(" - ")) return "";
  const parts = t
    .split(" - ")
    .map((p) => p.trim())
    .filter(Boolean);
  if (parts.length < 2) return "";
  return parts.slice(1).join(" - ").toLowerCase();
}

/**
 * @returns {object} identity fields for one image URL
 */
export function buildImageIdentity(url, meta = {}) {
  const imageUrl = nz(url);
  let filename = nz(meta.filename || meta.imageFilename);
  // Ignore proxy-host basenames (e.g. path.basename of https://wsrv.nl/?url=…)
  if (/^(wsrv\.nl|images\.weserv\.nl)$/i.test(filename) || /^https?:/i.test(filename)) {
    filename = "";
  }
  const canonicalImageUrl = canonicalizeImageUrl(imageUrl);
  const sourceImageId = extractSourceImageId(imageUrl, filename);
  const duplicateGroupId = sourceImageId || canonicalImageUrl || `empty:${meta.slotKey || "unknown"}`;
  const nearDuplicateGroupId = duplicateGroupId;
  let uniquenessStatus = "unique_candidate";
  if (!imageUrl) uniquenessStatus = "missing";
  else if (!canonicalImageUrl) uniquenessStatus = "invalid_url";

  return {
    imageUrl,
    canonicalImageUrl,
    sourceImageId,
    normalizedFilename: filename || sourceImageId,
    scene7AssetId: sourceImageId.startsWith("scene7:") ? sourceImageId.slice(7) : null,
    propertyName: nz(meta.propertyName) || extractPropertyFromTitle(meta.title),
    slotKey: nz(meta.slotKey),
    imageRole: nz(meta.imageRole) || inferImageRole(meta.slotKey, meta.title),
    title: nz(meta.title),
    dimensions: meta.dimensions || null,
    perceptualHash: null,
    duplicateGroupId,
    nearDuplicateGroupId,
    uniquenessStatus,
    recordId: meta.recordId || null,
    filename,
  };
}

export function inferImageRole(slotKey, title = "") {
  const sk = nz(slotKey).toLowerCase();
  if (/^materials\.gallery\./.test(sk)) return "gallery";
  if (sk === "footprint.openings") return "property_example";
  if (/^overview\.scenario\./.test(sk)) return "scenario";
  if (/exterior|arrival/i.test(title)) return "gallery_exterior";
  if (/guest room|suite/i.test(title)) return "gallery_room";
  if (/public space|lobby/i.test(title)) return "gallery_public";
  if (/f&b|local experience|restaurant|bar/i.test(title)) return "gallery_fb";
  if (/design detail/i.test(title)) return "gallery_detail";
  if (/property setting|setting/i.test(title)) return "gallery_setting";
  return "other";
}

function collectBlocks(brand, presentationRows) {
  if (Array.isArray(presentationRows) && presentationRows.length) return presentationRows;
  return Array.isArray(brand?.brandExplorer?.blocks) ? brand.brandExplorer.blocks : [];
}

function isVisuallyGatedRow(row = {}) {
  if (row.visible === false) return false;
  const ext = nz(row.externalDisplayStatus || row.external_display_status);
  if (/^do not display$/i.test(ext) || /^internal only$/i.test(ext)) return false;
  if (row.active === false) return false;
  return true;
}

function gallerySlots(blocks) {
  return blocks
    .filter((b) => isVisuallyGatedRow(b) && /^materials\.gallery\.\d+$/.test(nz(b.slotKey)))
    .sort((a, b) => {
      const na = Number(String(a.slotKey).match(/\.(\d+)$/)?.[1] || 0);
      const nb = Number(String(b.slotKey).match(/\.(\d+)$/)?.[1] || 0);
      return na - nb;
    });
}

function scenarioSlots(blocks) {
  return [1, 2, 3]
    .map((i) => blocks.find((b) => isVisuallyGatedRow(b) && nz(b.slotKey) === `overview.scenario.${i}`))
    .filter(Boolean);
}

function propertySlots(blocks) {
  return blocks.filter((b) => isVisuallyGatedRow(b) && nz(b.slotKey) === "footprint.openings");
}

function inventoryIdentities(rows, section) {
  return (rows || []).map((row) =>
    buildImageIdentity(row.imageUrl, {
      slotKey: row.slotKey,
      title: row.title,
      propertyName: row.propertyName || row.title,
      recordId: row.recordId || row.id || null,
      imageRole: section,
      filename: row.imageFilename || row.filename,
    })
  );
}

function groupByDuplicateId(identities) {
  const map = new Map();
  for (const id of identities) {
    if (!id.imageUrl) continue;
    const key = id.duplicateGroupId;
    if (!map.has(key)) map.set(key, []);
    map.get(key).push(id);
  }
  return map;
}

function markUniqueness(identities) {
  const groups = groupByDuplicateId(identities);
  for (const id of identities) {
    if (!id.imageUrl) {
      id.uniquenessStatus = "missing";
      continue;
    }
    const peers = groups.get(id.duplicateGroupId) || [];
    id.uniquenessStatus = peers.length > 1 ? "duplicate_or_near_duplicate" : "unique";
  }
  return identities;
}

/**
 * Pick first N assets with distinct duplicateGroupIds (for materializers).
 */
function filenameHintFromUrl(url) {
  try {
    const base = decodeURIComponent(new URL(url).pathname.split("/").pop() || "");
    return base.split("?")[0] || "";
  } catch {
    return nz(url).split("/").pop()?.split("?")[0] || "";
  }
}

export function pickDistinctImageAssets(assets = [], minCount = 1, { excludeGroupIds = [] } = {}) {
  const used = new Set((excludeGroupIds || []).map(String));
  const picked = [];
  for (const asset of assets || []) {
    const url = nz(asset.imageUrl || asset.sourceUrl);
    if (!url) continue;
    const filename = nz(asset.filename || asset.imageFilename) || filenameHintFromUrl(url);
    const id = buildImageIdentity(url, {
      slotKey: asset.slotKey,
      propertyName: asset.propertyName,
      title: asset.title,
      filename,
    });
    if (used.has(id.duplicateGroupId)) continue;
    used.add(id.duplicateGroupId);
    picked.push({ ...asset, imageUrl: url, filename, _imageIdentity: id });
    if (picked.length >= minCount) break;
  }
  return picked;
}

/**
 * Evaluate live presentation / brand blocks for image uniqueness.
 */
export function evaluateImageUniqueness({ brand = null, presentationRows = null, brandSlug = null } = {}) {
  const slug = brandSlug || brand?.slug || brand?.brandSlug || "";
  const blocks = collectBlocks(brand, presentationRows);
  const galleryRows = gallerySlots(blocks);
  const scenarioRows = scenarioSlots(blocks);
  const propertyRows = propertySlots(blocks);

  const gallery = markUniqueness(inventoryIdentities(galleryRows, "gallery"));
  const scenarios = markUniqueness(inventoryIdentities(scenarioRows, "scenario"));
  const properties = markUniqueness(inventoryIdentities(propertyRows, "property_example"));

  const galleryGroups = [...groupByDuplicateId(gallery).entries()].filter(([, rows]) => rows.length > 0);
  const scenarioGroups = [...groupByDuplicateId(scenarios).entries()].filter(([, rows]) => rows.length > 0);
  const propertyGroups = [...groupByDuplicateId(properties).entries()].filter(([, rows]) => rows.length > 0);

  const galleryDistinctCount = galleryGroups.length;
  const scenarioDistinctCount = scenarioGroups.length;
  const propertyExampleDistinctCount = propertyGroups.length;

  const gallerySlotCount = gallery.filter((g) => g.imageUrl).length;
  const scenarioSlotCount = scenarios.filter((g) => g.imageUrl).length;
  const propertySlotCount = properties.filter((g) => g.imageUrl).length;

  const duplicateGroups = [];
  for (const [groupId, rows] of galleryGroups) {
    if (rows.length > 1) {
      duplicateGroups.push({
        section: "gallery",
        duplicateGroupId: groupId,
        count: rows.length,
        slots: rows.map((r) => r.slotKey),
        titles: rows.map((r) => r.title),
        imageUrl: rows[0].imageUrl,
        canonicalImageUrl: rows[0].canonicalImageUrl,
        sourceImageId: rows[0].sourceImageId,
        status: "duplicate_or_near_duplicate",
        requiredFix: "reassign_distinct_gallery_image",
      });
    }
  }
  for (const [groupId, rows] of scenarioGroups) {
    if (rows.length > 1) {
      duplicateGroups.push({
        section: "scenario",
        duplicateGroupId: groupId,
        count: rows.length,
        slots: rows.map((r) => r.slotKey),
        titles: rows.map((r) => r.title),
        imageUrl: rows[0].imageUrl,
        status: "duplicate_or_near_duplicate",
        requiredFix: "reassign_distinct_scenario_image",
      });
    }
  }
  for (const [groupId, rows] of propertyGroups) {
    if (rows.length > 1) {
      duplicateGroups.push({
        section: "property_example",
        duplicateGroupId: groupId,
        count: rows.length,
        slots: rows.map((r) => `${r.slotKey}:${r.title || r.recordId}`),
        titles: rows.map((r) => r.title),
        imageUrl: rows[0].imageUrl,
        status: "duplicate_or_near_duplicate",
        requiredFix: "reassign_distinct_property_image",
      });
    }
  }

  const findings = [];
  if (gallerySlotCount < GALLERY_DISTINCT_MIN) {
    findings.push({
      id: "gallery_slot_count_short",
      status: "fail",
      detail: `gallery slots with imageUrl=${gallerySlotCount} < ${GALLERY_DISTINCT_MIN}`,
    });
  }
  if (galleryDistinctCount < GALLERY_DISTINCT_MIN) {
    findings.push({
      id: "gallery_distinct_short",
      status: "fail",
      detail: `galleryDistinctCount=${galleryDistinctCount} < ${GALLERY_DISTINCT_MIN} (slots=${gallerySlotCount})`,
    });
  }
  if (scenarioDistinctCount < SCENARIO_DISTINCT_MIN) {
    findings.push({
      id: "scenario_distinct_short",
      status: "fail",
      detail: `scenarioDistinctCount=${scenarioDistinctCount} < ${SCENARIO_DISTINCT_MIN}`,
    });
  }
  if (propertyExampleDistinctCount < PROPERTY_DISTINCT_MIN) {
    findings.push({
      id: "property_distinct_short",
      status: "fail",
      detail: `propertyExampleDistinctCount=${propertyExampleDistinctCount} < ${PROPERTY_DISTINCT_MIN}`,
    });
  }
  for (const g of duplicateGroups) {
    findings.push({
      id: `duplicate_${g.section}`,
      status: "fail",
      detail: `${g.section} duplicate group ${g.duplicateGroupId} spans ${g.slots.join(", ")}`,
      duplicateGroup: g,
    });
  }

  const pass =
    galleryDistinctCount >= GALLERY_DISTINCT_MIN &&
    scenarioDistinctCount >= SCENARIO_DISTINCT_MIN &&
    propertyExampleDistinctCount >= PROPERTY_DISTINCT_MIN &&
    duplicateGroups.length === 0;

  return {
    version: IMAGE_UNIQUENESS_VERSION,
    brandSlug: slug,
    brandName: brand?.name || slug,
    pass,
    gallerySlotCount,
    galleryDistinctCount,
    scenarioSlotCount,
    scenarioDistinctCount,
    propertySlotCount,
    propertyExampleDistinctCount,
    gallery: gallery,
    scenarios,
    properties,
    duplicateGroups,
    findings,
    requiredAction: pass ? "no_action" : "image_remediation",
    releaseQualityDecision: pass ? "image_unique" : "image_remediation_required",
  };
}

export function evaluateImageUniquenessForTest(result) {
  const failures = (result?.findings || [])
    .filter((f) => f.status === "fail")
    .map((f) => f.id + ":" + f.detail);
  return { pass: result?.pass === true && failures.length === 0, failures };
}
