/**
 * Footprint opening / property-example image governance (v33C-R2).
 *
 * Classifies property-card imagery, probes Choice hoteldam CDN for durable
 * hotel/property photography, and flags generic brand / logo / lifestyle assets.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

export const FOOTPRINT_OPENING_IMAGE_GOVERNANCE_VERSION = "33C-R2+33H";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const HOTELDAM_MAP_PATH = path.join(
  ROOT,
  "fixtures/choice-footprint-opening-hoteldam-map.json"
);

export const CHOICE_LOGO_IMAGE_RE =
  /choice[-_]?hotels?[-_]?social|choice[-_]?logo|logo[-_]?mark|image-choice-logo|ws-suites-logo/i;

export const GENERIC_BRAND_LIFESTYLE_IMAGE_RE =
  /woodspring\.com|www-media\.woodspring|pet[-_]?friendly|petfriendly|kitchen_dark|ws_webhero|ws_simply[-_]?clean|suite_dark|brand[-_]?platform|lifestyle|module_opt/i;

export const HOTELDAM_PROPERTY_IMAGE_RE = /\/hoteldam\/[a-z]{2}\/[a-z0-9]+\/images\//i;

export const OFFICIAL_LIFESTYLE_PROPERTY_IMAGE_RE =
  /designhotels\.com\/media\/|slh\.com\/-\/media\/slh\/hotels|lucidcm\.imgix\.net|digital\.ihg\.com\/is\/image\/ihg\/(?:hotel-indigo-[a-z0-9-]+|vignette-collection-[a-z0-9-]+|voco-[a-z0-9.-]+|avid-hotels-[a-z0-9-]+|holiday-inn-express-[a-z0-9-]+|even-hotels-[a-z0-9-]+)|development\.ihg\.com\/sites\/ihgplc\/files\/(?:IHG\/even-hotel\/|[^"'\\\s]*EVEN[_-])|ihgplc\.com\/(?:~\/)?media\/[^"'\\\s]*EVEN|cache\.marriott\.com\/(?:content\/dam\/marriott-renditions\/|is\/image\/marriotts7prod\/)|ahstatic\.com\/photos\/|(?:assets\.)?hiltonstatic\.com\/hilton-asset-cache\/image\/upload\/|hilton\.com\/im\/en\/[a-z0-9]+\/\d+\/|stories-editor\.hilton\.com\/wp-content\/uploads\/|stories\.hilton\.com\/uploads\/|login\.bunkhousehotels\.com\/|bunkhousehotels\.com\/wp-content\/uploads\/|media\.radissonhotels\.net\/image\/(?!radisson-hotels-app\/promotional)|radissonhotels\.iceportal\.com\/image\/(?!radisson-hotels-app\/promotional)|images\.bestwestern\.com\/|thewhitehallhotel\.com\/|hotelfinial\.com\/|librehotel\.com\/|nizuc(?:resort|cancun)\.com\/|ak-d\.tripcdn\.com\/images\/|media-cdn\.tripadvisor\.com\/media\/photo-|dynamic-media\.tacdn\.com\/|images\.trvl-media\.com\/|d25wybtmjgh8lz\.cloudfront\.net\/sites\/default\/files\/|wyndhamhotels\.com\/content\/dam\/property-images\//i;

export const PROPERTY_EXAMPLE_TITLE_RE = /property example/i;

const INTERIOR_FILENAME_PATTERNS = [
  (P) => `${P}GuestRoom1_1.jpg`,
  (P) => `${P}GuestRoom1.jpg`,
  (P) => `${P}Suite1_1.jpg`,
  (P) => `${P}Suite1.jpg`,
  (P) => `${P}Kitchen1_1.jpg`,
  (P) => `${P}Kitchen1.jpg`,
  (P) => `${P}Lobby1_1.jpg`,
  (P) => `${P}Lobby1.jpg`,
];

const EXTERIOR_FILENAME_PATTERNS = [
  (P) => `${P}ExteriorTemp01_1.jpg`,
  (P) => `${P}ExteriorTemp1.jpg`,
  (P) => `${P}Exterior01_1.jpg`,
  (P) => `${P}Exterior1_1.jpg`,
  (P) => `${P}Exterior1.jpg`,
  (P) => `${P}Exterior5_1.JPG`,
  (P) => `${P}AerialTemp1_1.jpg`,
  (P) => `Exterior1.JPG`,
];

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function normalizePageUrl(url) {
  try {
    const u = new URL(String(url || "").trim());
    u.hash = "";
    u.search = "";
    let s = u.toString();
    if (s.endsWith("/")) s = s.slice(0, -1);
    return s;
  } catch {
    return nz(url);
  }
}

function propertyIdFromPageUrl(url) {
  const parts = normalizePageUrl(url).split("/");
  return (parts[parts.length - 1] || "").toLowerCase();
}

function loadHoteldamMap() {
  try {
    if (fs.existsSync(HOTELDAM_MAP_PATH)) {
      return JSON.parse(fs.readFileSync(HOTELDAM_MAP_PATH, "utf8"));
    }
  } catch {
    // fixture optional at runtime
  }
  return {};
}

export function isPropertyExampleTitle(title) {
  return PROPERTY_EXAMPLE_TITLE_RE.test(nz(title));
}

export function isOfficialLifestylePropertyImageUrl(url) {
  return OFFICIAL_LIFESTYLE_PROPERTY_IMAGE_RE.test(nz(url));
}

export function isHoteldamPropertyImageUrl(url) {
  return HOTELDAM_PROPERTY_IMAGE_RE.test(nz(url));
}

export function isLogoImageUrl(url) {
  return CHOICE_LOGO_IMAGE_RE.test(nz(url));
}

export function isGenericBrandOrLifestyleImageUrl(url) {
  const u = nz(url).toLowerCase();
  if (!u) return false;
  if (isOfficialLifestylePropertyImageUrl(u)) return false;
  if (isLogoImageUrl(u)) return true;
  if (isHoteldamPropertyImageUrl(u)) return false;
  return GENERIC_BRAND_LIFESTYLE_IMAGE_RE.test(u);
}

export function classifyPropertyExampleImage(url, { registrySourceUrl = "", registryNotes = "" } = {}) {
  const combined = [url, registrySourceUrl, registryNotes].filter(Boolean).join("\n");
  const u = nz(url).toLowerCase();

  if (!u) {
    return {
      category: "missing",
      isPropertySpecific: false,
      isHotelPhotography: false,
      isGenericBrand: false,
      isLogo: false,
      isLifestyle: false,
      recommendation: "hide",
      reason: "missing_image",
    };
  }

  const isLogo = isLogoImageUrl(combined);
  const isHoteldam = isHoteldamPropertyImageUrl(combined);
  const isOfficialLifestyleProperty = isOfficialLifestylePropertyImageUrl(combined);
  const isGenericBrand =
    !isHoteldam && !isOfficialLifestyleProperty && isGenericBrandOrLifestyleImageUrl(combined);
  const isLifestyle = /pet[-_]?friendly|petfriendly|lifestyle|module_opt|dog/i.test(combined);
  const isHotelPhotography = (isHoteldam || isOfficialLifestyleProperty) && !isLogo;
  const isPropertySpecific = isHoteldam || isOfficialLifestyleProperty;

  let category = "unknown";
  let recommendation = "review";

  if (isLogo) {
    category = "logo";
    recommendation = "replace_or_hide";
  } else if (isLifestyle || isGenericBrand) {
    category = isLifestyle ? "lifestyle" : "generic_brand";
    recommendation = "replace_or_hide";
  } else if (isHoteldam) {
    category = "hoteldam_property";
    recommendation = "preserve";
  } else if (isOfficialLifestyleProperty) {
    category = "official_lifestyle_property";
    recommendation = "preserve";
  } else if (/\.(jpg|jpeg|png|webp)/i.test(u)) {
    category = "unverified_image";
    recommendation = "replace_or_hide";
  } else {
    category = "non_image";
    recommendation = "replace_or_hide";
  }

  return {
    category,
    isPropertySpecific,
    isHotelPhotography,
    isGenericBrand,
    isLogo,
    isLifestyle,
    recommendation,
    reason: category,
  };
}

async function probeImageUrl(url) {
  try {
    const res = await fetch(url, {
      method: "GET",
      redirect: "follow",
      signal: AbortSignal.timeout(12000),
    });
    if (!res.ok) return "";
    const ct = res.headers.get("content-type") || "";
    if (!ct.includes("image")) return "";
    return url;
  } catch {
    return "";
  }
}

function hoteldamCandidates(propertyId, kind = "exterior") {
  const pid = nz(propertyId).toLowerCase();
  const cc = pid.slice(0, 2);
  const P = pid.toUpperCase();
  const sizes = ["1280", "2048", "480"];
  const patterns =
    kind === "interior"
      ? INTERIOR_FILENAME_PATTERNS.map((fn) => fn(P))
      : EXTERIOR_FILENAME_PATTERNS.map((fn) => fn(P));
  const out = [];
  for (const size of sizes) {
    for (const name of patterns) {
      out.push(`https://www.choicehotels.com/hoteldam/${cc}/${pid}/images/${size}/${name}`);
    }
  }
  return out;
}

export async function probeHoteldamPropertyImage(propertyId, { kinds = ["exterior", "interior"] } = {}) {
  const pid = nz(propertyId).toLowerCase();
  if (!pid) return { ok: false, error: "missing_property_id" };

  const map = loadHoteldamMap();
  for (const pageUrl of Object.keys(map)) {
    if (propertyIdFromPageUrl(pageUrl) === pid && map[pageUrl]) {
      const cached = map[pageUrl];
      const cachedOk = await probeImageUrl(cached);
      if (cachedOk) {
        return {
          ok: true,
          imageUrl: cachedOk,
          imageKind: cachedOk.toLowerCase().includes("exterior") ? "exterior" : "property",
          imageSource: "hoteldam_fixture_map",
          propertyId: pid,
        };
      }
    }
  }

  for (const kind of kinds) {
    for (const url of hoteldamCandidates(pid, kind)) {
      const ok = await probeImageUrl(url);
      if (ok) {
        return {
          ok: true,
          imageUrl: ok,
          imageKind: kind,
          imageSource: "hoteldam_probe",
          propertyId: pid,
        };
      }
    }
  }

  return { ok: false, error: "no_hoteldam_property_image", propertyId: pid };
}

/** Gallery slot image kinds — WoodSpring hoteldam filename probes (v33H). */
export const GALLERY_HOTELDAM_KIND_PATTERNS = Object.freeze({
  exterior: EXTERIOR_FILENAME_PATTERNS,
  guest_room: [
    (P) => `${P}GuestRoom1_1.jpg`,
    (P) => `${P}GuestRoom1.jpg`,
    (P) => `${P}GuestRoom2_1.jpg`,
    (P) => `${P}GuestRoom2.jpg`,
  ],
  kitchen: [
    (P) => `${P}Kitchen1_1.jpg`,
    (P) => `${P}Kitchen1.jpg`,
    (P) => `${P}Kitchen2_1.jpg`,
  ],
  suite_work: [
    (P) => `${P}Suite1_1.jpg`,
    (P) => `${P}Suite1.jpg`,
    (P) => `${P}LivingRoom1_1.jpg`,
    (P) => `${P}BusinessCenter1_1.jpg`,
    (P) => `${P}Lobby1_1.jpg`,
  ],
  kitchen_detail: [
    (P) => `${P}Kitchen2_1.jpg`,
    (P) => `${P}Kitchen3_1.jpg`,
    (P) => `${P}Kitchen1_1.jpg`,
  ],
  extended_stay: [
    (P) => `${P}GuestRoom2_1.jpg`,
    (P) => `${P}Suite2_1.jpg`,
    (P) => `${P}Suite2.jpg`,
    (P) => `${P}GuestRoom1_1.jpg`,
  ],
});

function normalizeUrlKey(url) {
  return nz(url).replace(/\?.*$/, "").toLowerCase();
}

/** Same hoteldam filename at different CDN sizes counts as one image (filename case preserved). */
function normalizeHoteldamImageIdentity(url) {
  const raw = nz(url).replace(/\?.*$/, "");
  const m = raw.match(/\/hoteldam\/([a-z]{2}\/[a-z0-9]+\/images\/)\d+\/(.+)$/i);
  if (m) return `/hoteldam/${m[1]}${m[2]}`;
  return raw.toLowerCase();
}

function hoteldamUrlsFromPatterns(propertyId, patterns) {
  const pid = nz(propertyId).toLowerCase();
  const cc = pid.slice(0, 2);
  const P = pid.toUpperCase();
  const sizes = ["1280", "2048", "480"];
  const out = [];
  for (const size of sizes) {
    for (const fn of patterns) {
      out.push(`https://www.choicehotels.com/hoteldam/${cc}/${pid}/images/${size}/${fn(P)}`);
    }
  }
  return out;
}

export async function probeHoteldamGalleryKindImage(
  propertyId,
  kind,
  { excludeUrlKeys = [] } = {}
) {
  const pid = nz(propertyId).toLowerCase();
  if (!pid) return { ok: false, error: "missing_property_id" };

  const exclude = new Set((excludeUrlKeys || []).map(normalizeUrlKey));
  const patterns =
    GALLERY_HOTELDAM_KIND_PATTERNS[kind] ||
    GALLERY_HOTELDAM_KIND_PATTERNS.guest_room ||
    INTERIOR_FILENAME_PATTERNS;

  for (const url of hoteldamUrlsFromPatterns(pid, patterns)) {
    if (exclude.has(normalizeUrlKey(url))) continue;
    const ok = await probeImageUrl(url);
    if (ok) {
      return {
        ok: true,
        imageUrl: ok,
        imageKind: kind,
        imageSource: "hoteldam_gallery_probe",
        propertyId: pid,
      };
    }
  }

  const fallbackKinds =
    kind === "exterior"
      ? ["interior"]
      : kind === "guest_room" || kind === "extended_stay"
        ? ["interior", "exterior"]
        : ["interior"];
  for (const fb of fallbackKinds) {
    const fbResult = await probeHoteldamPropertyImage(pid, {
      kinds: fb === "exterior" ? ["exterior"] : ["interior"],
    });
    if (fbResult.ok && !exclude.has(normalizeUrlKey(fbResult.imageUrl))) {
      return {
        ok: true,
        imageUrl: fbResult.imageUrl,
        imageKind: kind,
        imageSource: fbResult.imageSource,
        propertyId: pid,
        fallbackFrom: fb,
      };
    }
  }

  return { ok: false, error: "no_hoteldam_gallery_image", propertyId: pid, kind };
}

export async function discoverDistinctHoteldamGalleryImages(assignments) {
  const usedKeys = new Set();
  const results = [];

  for (const assignment of assignments) {
    const { slotKey, propertyId, kind, sourcePageUrl, title } = assignment;
    let probe = await probeHoteldamGalleryKindImage(propertyId, kind, {
      excludeUrlKeys: [...usedKeys],
    });

    if (!probe.ok && assignment.fallbackPropertyIds?.length) {
      for (const fbPid of assignment.fallbackPropertyIds) {
        probe = await probeHoteldamGalleryKindImage(fbPid, kind, {
          excludeUrlKeys: [...usedKeys],
        });
        if (probe.ok) break;
      }
    }

    if (probe.ok) {
      usedKeys.add(normalizeUrlKey(probe.imageUrl));
    }

    results.push({
      slotKey,
      title,
      propertyId,
      kind,
      sourcePageUrl,
      ok: probe.ok,
      imageUrl: probe.imageUrl || null,
      imageKind: probe.imageKind || kind,
      imageSource: probe.imageSource || null,
      error: probe.error || null,
      fallbackFrom: probe.fallbackFrom || null,
    });
  }

  return {
    assignments: results,
    distinctCount: results.filter((r) => r.ok).length,
    allDistinct: results.every((r) => r.ok),
  };
}

const WOODSPRING_GALLERY_POOL_PATH = path.join(
  ROOT,
  "fixtures/choice-woodspring-gallery-hoteldam-pool.json"
);

const WOODSPRING_GALLERY_PROBE_NAMES = Object.freeze([
  (P, p) => `${P}Exterior01_1.jpg`,
  (P, p) => `${P}Exterior1_1.jpg`,
  (P, p) => `${P}Exterior1.jpg`,
  (P, p) => `${P}Exterior2_1.jpg`,
  (P, p) => `${P}Exterior2.jpg`,
  (P, p) => `${p}Exterior1_1.jpg`,
  (P, p) => `${P}GuestRoom1_1.jpg`,
  (P, p) => `${P}Kitchen1_1.jpg`,
  (P, p) => `${P}Suite1_1.jpg`,
  (P, p) => `${P}LivingRoom1_1.jpg`,
  (P, p) => `Exterior1.JPG`,
]);

function loadGalleryPoolFixture(fixturePath = WOODSPRING_GALLERY_POOL_PATH) {
  try {
    const resolved = path.isAbsolute(fixturePath)
      ? fixturePath
      : path.join(ROOT, fixturePath);
    if (fs.existsSync(resolved)) {
      const rows = JSON.parse(fs.readFileSync(resolved, "utf8"));
      return Array.isArray(rows) ? rows : [];
    }
  } catch {
    // optional fixture
  }
  return [];
}

function loadWoodspringGalleryPoolFixture() {
  return loadGalleryPoolFixture(WOODSPRING_GALLERY_POOL_PATH);
}

export async function probeBrandGalleryImagePool(propertyCatalog, { fixturePath } = {}) {
  const fixture = fixturePath
    ? loadGalleryPoolFixture(fixturePath)
    : loadWoodspringGalleryPoolFixture();
  return probeGalleryImagePoolFromFixture(propertyCatalog, fixture);
}

export async function probeWoodspringGalleryImagePool(propertyCatalog) {
  const fixture = loadWoodspringGalleryPoolFixture();
  return probeGalleryImagePoolFromFixture(propertyCatalog, fixture);
}

async function probeGalleryImagePoolFromFixture(propertyCatalog, fixture) {
  const seen = new Set();
  const pool = [];

  function addEntry(entry) {
    const key = normalizeHoteldamImageIdentity(entry.imageUrl);
    if (!key || seen.has(key)) return;
    if (!isHoteldamPropertyImageUrl(entry.imageUrl)) return;
    if (isLogoImageUrl(entry.imageUrl) || isGenericBrandOrLifestyleImageUrl(entry.imageUrl)) return;
    seen.add(key);
    pool.push(entry);
  }

  for (const row of fixture) {
    addEntry({
      propertyId: nz(row.propertyId).toLowerCase(),
      sourcePageUrl: normalizePageUrl(row.sourcePageUrl),
      imageUrl: nz(row.imageUrl),
      label: nz(row.label) || "exterior",
      imageSource: "woodspring_gallery_fixture_pool",
    });
  }

  for (const catalog of propertyCatalog || []) {
    const propertyId = propertyIdFromPageUrl(catalog.sourcePageUrl);
    const cc = propertyId.slice(0, 2);
    const P = propertyId.toUpperCase();
    const p = propertyId.toLowerCase();
    for (const fn of WOODSPRING_GALLERY_PROBE_NAMES) {
      for (const size of ["1280", "2048", "480"]) {
        const name = fn(P, p);
        const url = `https://www.choicehotels.com/hoteldam/${cc}/${propertyId}/images/${size}/${name}`;
        if (seen.has(normalizeHoteldamImageIdentity(url))) continue;
        const ok = await probeImageUrl(url);
        if (ok) {
          addEntry({
            propertyId,
            sourcePageUrl: normalizePageUrl(catalog.sourcePageUrl),
            imageUrl: ok,
            label: /exterior/i.test(name) ? "exterior" : "property",
            imageSource: "hoteldam_gallery_pool_probe",
          });
          break;
        }
      }
    }
  }

  return pool;
}

export async function assignBrandGalleryImagesFromPool(
  slotTargets,
  propertyCatalog,
  { fixturePath } = {}
) {
  const pool = await probeBrandGalleryImagePool(propertyCatalog, { fixturePath });
  return assignGalleryImagesFromPool(slotTargets, propertyCatalog, pool);
}

export async function assignWoodspringGalleryImagesFromPool(slotTargets, propertyCatalog) {
  const pool = await probeWoodspringGalleryImagePool(propertyCatalog);
  return assignGalleryImagesFromPool(slotTargets, propertyCatalog, pool);
}

function assignGalleryImagesFromPool(slotTargets, propertyCatalog, pool) {
  const usedKeys = new Set();
  const assignments = [];

  function pickFromPool({ preferredPropertyId = "", preferLabel = "" } = {}) {
    const pid = nz(preferredPropertyId).toLowerCase();
    const candidates = pool.filter(
      (c) => !usedKeys.has(normalizeHoteldamImageIdentity(c.imageUrl))
    );
    const scored = candidates.map((c) => {
      let score = 0;
      if (pid && c.propertyId === pid) score += 10;
      if (preferLabel && c.label === preferLabel) score += 5;
      if (c.label === "exterior") score += 1;
      return { c, score };
    });
    scored.sort((a, b) => b.score - a.score);
    return scored[0]?.c || candidates[0] || null;
  }

  for (const target of slotTargets) {
    const preferLabel =
      target.kind === "exterior"
        ? "exterior"
        : target.kind === "guest_room"
          ? "guest_room"
          : "property";
    let chosen = pickFromPool({
      preferredPropertyId: target.propertyKey,
      preferLabel,
    });

    if (!chosen) {
      assignments.push({
        slotKey: target.slotKey,
        title: target.title,
        propertyId: target.propertyKey,
        kind: target.kind,
        sourcePageUrl: catalogSourceForProperty(propertyCatalog, target.propertyKey),
        ok: false,
        imageUrl: null,
        imageKind: target.kind,
        imageSource: null,
        error: "no_distinct_hoteldam_image_in_pool",
        fallbackFrom: null,
      });
      continue;
    }

    usedKeys.add(normalizeHoteldamImageIdentity(chosen.imageUrl));
    assignments.push({
      slotKey: target.slotKey,
      title: target.title,
      propertyId: chosen.propertyId,
      kind: target.kind,
      sourcePageUrl: chosen.sourcePageUrl,
      ok: true,
      imageUrl: chosen.imageUrl,
      imageKind: target.kind,
      imageSource: chosen.imageSource,
      error: null,
      fallbackFrom: chosen.propertyId !== target.propertyKey ? "pool_property_fallback" : null,
    });
  }

  return {
    assignments,
    poolSize: pool.length,
    distinctCount: assignments.filter((a) => a.ok).length,
    allDistinct: assignments.every((a) => a.ok),
    imagePool: pool,
  };
}

export { loadGalleryPoolFixture };

function catalogSourceForProperty(propertyCatalog, propertyKey) {
  const suffix = `/${nz(propertyKey).toLowerCase()}`;
  const match = (propertyCatalog || []).find((c) =>
    nz(c.sourcePageUrl).toLowerCase().endsWith(suffix)
  );
  return match ? normalizePageUrl(match.sourcePageUrl) : "";
}

export function pickPropertyImageFromPool(pool, propertyKey) {
  const pid = nz(propertyKey).toLowerCase();
  const match = (pool || []).find((entry) => nz(entry.propertyId).toLowerCase() === pid);
  if (!match?.imageUrl) return null;
  if (isLogoImageUrl(match.imageUrl) || isGenericBrandOrLifestyleImageUrl(match.imageUrl)) {
    return null;
  }
  return {
    ok: true,
    imageUrl: match.imageUrl,
    imageKind: nz(match.label) || "exterior",
    imageSource: match.imageSource || "gallery_fixture_pool",
    imageSourcePageUrl: normalizePageUrl(match.sourcePageUrl || ""),
    propertyId: pid,
  };
}

export async function resolvePropertySpecificHotelImage(sourcePageUrl, { fixturePool = [] } = {}) {
  const propertyId = propertyIdFromPageUrl(sourcePageUrl);
  const exterior = await probeHoteldamPropertyImage(propertyId, { kinds: ["exterior"] });
  if (exterior.ok) {
    return {
      ok: true,
      imageUrl: exterior.imageUrl,
      imageKind: "exterior",
      imageSource: exterior.imageSource,
      imageSourcePageUrl: normalizePageUrl(sourcePageUrl),
      propertyId,
    };
  }

  const interior = await probeHoteldamPropertyImage(propertyId, { kinds: ["interior"] });
  if (interior.ok) {
    return {
      ok: true,
      imageUrl: interior.imageUrl,
      imageKind: interior.imageKind,
      imageSource: interior.imageSource,
      imageSourcePageUrl: normalizePageUrl(sourcePageUrl),
      propertyId,
    };
  }

  const fromPool =
    pickPropertyImageFromPool(fixturePool, propertyId) ||
    pickPropertyImageFromPool(
      fixturePool,
      propertyIdFromPageUrl(normalizePageUrl(sourcePageUrl))
    );
  if (fromPool) {
    return {
      ...fromPool,
      imageSourcePageUrl: normalizePageUrl(sourcePageUrl),
    };
  }

  return {
    ok: false,
    error: "no_property_specific_hotel_image",
    propertyId,
    imageSourcePageUrl: normalizePageUrl(sourcePageUrl),
  };
}

export function detectPropertyExampleImageDefects(row, registryAsset = null, brandConfig = {}) {
  if (!row || !/footprint\.openings/.test(nz(row.slotKey))) return [];
  if (!isPropertyExampleTitle(row.title)) return [];

  const classification = classifyPropertyExampleImage(row.imageUrl, {
    registrySourceUrl: registryAsset?.sourceUrl || "",
    registryNotes: [registryAsset?.sourceNotes, registryAsset?.reviewNotes, registryAsset?.validationNotes]
      .filter(Boolean)
      .join("\n"),
  });

  const defects = [];
  if (classification.isLogo) {
    defects.push({
      type: "property_example_logo_image",
      severity: "critical",
      category: "visual",
      surface: `presentation.${row.slotKey}`,
      recordId: row.recordId,
      slotKey: row.slotKey,
      message:
        "Property example card uses a logo image — replace with hotel/property photography or hide the card.",
      recommendedFixBatch: "v33C-R2_woodspring_property_specific_images",
    });
  } else if (classification.isGenericBrand || classification.isLifestyle) {
    defects.push({
      type: "property_example_generic_brand_image",
      severity: "high",
      category: "visual",
      surface: `presentation.${row.slotKey}`,
      recordId: row.recordId,
      slotKey: row.slotKey,
      message:
        "Property example card uses generic brand or lifestyle imagery — hotel/property photography required.",
      recommendedFixBatch: "v33C-R2_woodspring_property_specific_images",
    });
  } else if (!classification.isHotelPhotography && classification.category !== "missing") {
    defects.push({
      type: "property_example_non_hotel_image",
      severity: "high",
      category: "visual",
      surface: `presentation.${row.slotKey}`,
      recordId: row.recordId,
      slotKey: row.slotKey,
      message:
        "Property example card image is not verified hotel/property photography — replace or hide.",
      recommendedFixBatch: "v33C-R2_woodspring_property_specific_images",
    });
  }

  return defects;
}

export function detectGalleryNonHotelImageDefects(row, registryAsset = null) {
  if (!row || !/^materials\.gallery\.\d+$/.test(nz(row.slotKey))) return [];
  if (!row.imageUrl) return [];

  const classification = classifyPropertyExampleImage(row.imageUrl, {
    registrySourceUrl: registryAsset?.sourceUrl || "",
    registryNotes: [registryAsset?.sourceNotes, registryAsset?.reviewNotes].filter(Boolean).join("\n"),
  });

  if (classification.isLogo || classification.isGenericBrand || classification.isLifestyle) {
    return [
      {
        type: "gallery_non_hotel_image",
        severity: "medium",
        category: "visual",
        surface: `presentation.${row.slotKey}`,
        recordId: row.recordId,
        slotKey: row.slotKey,
        message:
          "Gallery slot uses logo, lifestyle, or generic brand graphic — replace with hotel/property photography or hide.",
        recommendedFixBatch: "v33C-R2_woodspring_property_specific_images",
      },
    ];
  }
  return [];
}
