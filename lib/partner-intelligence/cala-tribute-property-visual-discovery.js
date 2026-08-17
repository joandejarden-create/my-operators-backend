/**
 * CALA Tribute Property Visual Candidate Discovery v1/v2.
 *
 * v2 adds explicit cover-image resolution from official Marriott property
 * overview pages (og:image, __NEXT_DATA__, photos fallback).
 * markets from Marriott-controlled sources (country sitemaps + property pages).
 * Metadata-only — does NOT download images, attach files, overwrite Brand Setup,
 * or approve assets for Explorer use.
 *
 * @see docs/data-intelligence/cala-tribute-property-visual-discovery-v1.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  crawlMarriottCountrySitemaps,
  marshaFromMarriottWebsite,
  SITEMAP_SLUG_TO_CENSUS_COUNTRY_LABEL,
  MARRIOTT_FETCH_HEADERS,
} from "../marriott-brand-directory-extract.js";
import {
  fetchMarriottOverviewHtmlPlain,
  fetchMarriottOverviewHtmlPuppeteer,
} from "../marriott-hotel-content-fetch.js";
import { resolveMarriottCoverImageFromHtml } from "./marriott-property-cover-image.js";
import { extractImageUrlReferencesFromHtml } from "./brand-asset-pr-package-governance.js";
import {
  BRAND_ASSET_PILOT_CONFIG,
  MAP_BRAND_ASSET,
  buildRegistryDedupeKey,
  listRegistryAssetsForBrand,
  validateRegistryWritePayload,
} from "./brand-asset-registry-workflow.js";
import {
  MAP_VISUAL_SLOT,
  VISUAL_SLOT,
} from "./brand-explorer-visual-slot-requirements.js";
import {
  TRIBUTE_RECORD_ID,
  BRAND_NAME,
  PARENT_COMPANY,
} from "./tribute-portfolio-brand-package.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

export const DISCOVERY_VERSION = "2";
export const REPORT_JSON_NAME = "cala-tribute-property-visual-discovery.json";
export const REPORT_MD_NAME = "cala-tribute-property-visual-discovery.md";

export const SOURCE_BASIS = "Marriott-Controlled Source";

/** CALA / Caribbean / Latin America country sitemap slugs to crawl. */
export const CALA_SITEMAP_SLUGS = [
  "mexico",
  "puerto-rico",
  "dominican-republic",
  "jamaica",
  "panama",
  "costa-rica",
  "colombia",
  "peru",
  "brazil",
  "chile",
  "argentina",
  "ecuador",
  "guatemala",
  "honduras",
  "el-salvador",
  "bahamas",
  "barbados",
  "trinidad-and-tobago",
  "aruba",
  "turks-and-caicos-islands",
  "virgin-islands-us",
  "curacao",
  "saint-lucia",
  "belize",
  "antigua-and-barbuda",
  "cayman-islands",
  "suriname",
  "paraguay",
  "uruguay",
  "bolivia",
  "venezuela",
  "guyana",
  "grenada",
  "bermuda",
  "sint-maarten",
  "saint-kitts-and-nevis",
];

/** Seed properties from hotel census / overview import (names + MARSHA when known). */
export const SEED_TRIBUTE_CALA_PROPERTIES = [
  {
    propertyName: "Hotel Rumbao, a Tribute Portfolio Hotel",
    marsha: "SJUTX",
    countryRegion: "Puerto Rico",
    overviewUrl: "https://www.marriott.com/en-us/hotels/sjutx-hotel-rumbao-a-tribute-portfolio-hotel/overview/",
    calaRelevant: "Yes",
  },
  {
    propertyName: "Mystique Holbox by Royalton, A Tribute Portfolio Resort",
    marsha: "CUNMH",
    countryRegion: "Mexico",
    overviewUrl: "https://www.marriott.com/en-us/hotels/cunmh-mystique-holbox-by-royalton-a-tribute-portfolio-resort/overview/",
    calaRelevant: "Yes",
  },
  {
    propertyName: "Tulum, a Tribute Portfolio Hotel",
    marsha: null,
    countryRegion: "Mexico",
    overviewUrl: null,
    calaRelevant: "Yes",
  },
  {
    propertyName: "Ponce, a Tribute Portfolio Hotel",
    marsha: null,
    countryRegion: "Puerto Rico",
    overviewUrl: null,
    calaRelevant: "Yes",
  },
  {
    propertyName: "Mexico City, Alameda, a Tribute Portfolio Hotel",
    marsha: null,
    countryRegion: "Mexico",
    overviewUrl: null,
    calaRelevant: "Yes",
  },
  {
    propertyName: "Tribute Portfolio Merida Mexico",
    marsha: null,
    countryRegion: "Mexico",
    overviewUrl: null,
    calaRelevant: "Yes",
  },
  {
    propertyName: "Ermita Cartagena A Tribute Portfolio Hotel",
    marsha: null,
    countryRegion: "Colombia",
    overviewUrl: null,
    calaRelevant: "Yes",
  },
  {
    propertyName: "Los Tajibos Santa Cruz de la Sierra a Tribute Portfolio Hotel",
    marsha: null,
    countryRegion: "Bolivia",
    overviewUrl: null,
    calaRelevant: "Yes",
  },
  {
    propertyName: "Auberge du Vin a Tribute Portfolio Hotel Tupungato",
    marsha: null,
    countryRegion: "Argentina",
    overviewUrl: null,
    calaRelevant: "Yes",
  },
  {
    propertyName: "Arelauquen Lodge a Tribute Portfolio Hotel San Carlos de Bariloche",
    marsha: null,
    countryRegion: "Argentina",
    overviewUrl: null,
    calaRelevant: "Yes",
  },
];

const OTA_HOST_PATTERNS = [
  /booking\.com/i,
  /expedia\./i,
  /tripadvisor\./i,
  /hotels\.com/i,
  /agoda\./i,
  /kayak\./i,
  /trivago\./i,
  /googleusercontent/i,
  /gstatic\.com/i,
];

const GENERIC_IMAGE_PATTERNS = [
  /favicon|placeholder|1x1|pixel|spacer|icon|logo\.svg/i,
  /hotel-development\.marriott\.com\/resourcefiles/i,
  /trbcl\.1256725ws2880/i,
];

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function isTributePortfolioHotel(name, url) {
  const hay = `${name} ${url}`.toLowerCase();
  return /tribute\s*portfolio|a\s*tribute\s*portfolio/i.test(hay);
}

function isMarriottControlledUrl(url) {
  try {
    const host = new URL(url).hostname.toLowerCase();
    return host.endsWith("marriott.com");
  } catch {
    return false;
  }
}

function isRejectedImageUrl(url) {
  if (!url || !isMarriottControlledUrl(url)) return true;
  if (OTA_HOST_PATTERNS.some((re) => re.test(url))) return true;
  if (GENERIC_IMAGE_PATTERNS.some((re) => re.test(url))) return true;
  return false;
}

function overviewUrlFromHotel(row) {
  const url = nz(row.url || row.website);
  if (url && /\/hotels\//i.test(url)) {
    const base = url.replace(/\/+$/, "").replace(/\/(overview|photos|experiences|rooms)\/?$/i, "");
    return `${base}/overview/`;
  }
  const marsha = nz(row.marsha || row.marshaCode).toLowerCase();
  const slug = nz(row.slug);
  if (marsha && slug) {
    return `https://www.marriott.com/en-us/hotels/${marsha}-${slug}/overview/`;
  }
  return "";
}

function photosUrlFromOverview(overviewUrl) {
  return nz(overviewUrl).replace(/\/overview\/?$/i, "/photos/");
}

function inferPropertySetting(propertyName, description = "") {
  const hay = `${propertyName} ${description}`.toLowerCase();
  if (/resort|beach|holbox|island|cabos|riviera|caribbean/i.test(hay)) return "Resort";
  if (/lodge|wine|vineyard|auberge|boutique|design|tulum/i.test(hay)) return "Boutique / Lifestyle";
  if (/historic|heritage|ermita|cartagena|colonial|conversion|adaptive/i.test(hay)) {
    return "Conversion / Adaptive Reuse";
  }
  if (/mixed|alameda|downtown|urban|city/i.test(hay)) return "Urban";
  return "Urban";
}

function normalizePropertyKey(name) {
  return nz(name).toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
}

function mergeProperties(sitemapRows, seeds) {
  const map = new Map();

  for (const seed of seeds) {
    const key = normalizePropertyKey(seed.propertyName);
    map.set(key, {
      propertyName: seed.propertyName,
      marsha: seed.marsha || null,
      countryRegion: seed.countryRegion,
      overviewUrl: seed.overviewUrl || null,
      calaRelevant: seed.calaRelevant || "Yes",
      sourceType: "seed",
      brandConfirmed: true,
      propertyConfirmed: true,
    });
  }

  for (const row of sitemapRows) {
    if (!isTributePortfolioHotel(row.name, row.url)) continue;
    const key = normalizePropertyKey(row.name);
    const overviewUrl = overviewUrlFromHotel(row);
    const country =
      row.country ||
      SITEMAP_SLUG_TO_CENSUS_COUNTRY_LABEL[row.countryPage] ||
      row.countryPage ||
      "";
    const existing = map.get(key);
    map.set(key, {
      propertyName: row.name,
      marsha: row.marsha || row.marshaCode || marshaFromMarriottWebsite(row.url) || existing?.marsha || null,
      countryRegion: country || existing?.countryRegion || "",
      overviewUrl: overviewUrl || existing?.overviewUrl || null,
      propertyPageUrl: row.url || overviewUrl,
      calaRelevant: "Yes",
      sourceType: existing ? "seed+sitemap" : "sitemap",
      brandConfirmed: true,
      propertyConfirmed: true,
      sitemapUrl: row.url,
    });
  }

  return [...map.values()].sort((a, b) => a.propertyName.localeCompare(b.propertyName));
}

async function fetchPropertyPageHtml(pageUrl, { usePuppeteer = false } = {}) {
  const plain = await fetchMarriottOverviewHtmlPlain(pageUrl);
  if (!plain.accessDenied && plain.status !== 403 && plain.html) {
    return { html: plain.html, url: plain.url || pageUrl, fetchMethod: "plain" };
  }
  if (usePuppeteer) {
    try {
      const rendered = await fetchMarriottOverviewHtmlPuppeteer(pageUrl, { headless: true });
      if (!rendered.accessDenied && rendered.html) {
        return { html: rendered.html, url: rendered.url || pageUrl, fetchMethod: "puppeteer" };
      }
    } catch (err) {
      return { html: "", url: pageUrl, fetchMethod: "puppeteer_failed", error: err.message };
    }
  }
  return {
    html: plain.html || "",
    url: plain.url || pageUrl,
    fetchMethod: "plain",
    accessDenied: plain.accessDenied || plain.status === 403,
  };
}

async function probePropertyPages(property, { probePages = true, usePuppeteer = false } = {}) {
  const result = {
    ...property,
    sourcePagesProbed: [],
    imageCandidates: [],
    coverImage: null,
    pageContextConfirmed: false,
    probeErrors: [],
    descriptionSnippet: "",
  };

  if (!probePages) return result;

  const overviewUrl = property.overviewUrl;
  let coverFromOverview = null;

  if (overviewUrl) {
    try {
      const overview = await fetchPropertyPageHtml(overviewUrl, { usePuppeteer });
      const probe = {
        url: overview.url,
        pageType: "overview",
        status: overview.accessDenied ? 403 : 200,
        accessDenied: Boolean(overview.accessDenied),
        fetchMethod: overview.fetchMethod,
        coverResolved: false,
      };
      if (overview.html && !overview.accessDenied) {
        const descMatch = overview.html.match(
          /<meta[^>]+property="og:description"[^>]+content="([^"]+)"/i
        );
        if (descMatch) result.descriptionSnippet = descMatch[1].slice(0, 280);
        coverFromOverview = resolveMarriottCoverImageFromHtml(overview.html, overview.url);
        if (coverFromOverview) {
          probe.coverResolved = true;
          probe.coverSource = coverFromOverview.resolvedFrom;
          result.coverImage = {
            ...coverFromOverview,
            sourcePageUrl: overview.url,
            context: "cover",
          };
          result.imageCandidates.push({
            url: coverFromOverview.url,
            sourcePageUrl: overview.url,
            assetTypeGuess: "Hero Image",
            context: "cover",
            imageRole: "cover",
            resolvedFrom: coverFromOverview.resolvedFrom,
          });
          result.pageContextConfirmed = true;
        }
      } else if (overview.accessDenied) {
        probe.note = "access_denied on overview — try --use-puppeteer or photos-page fallback";
        result.probeErrors.push(`access_denied:${overviewUrl}`);
      }
      result.sourcePagesProbed.push(probe);
    } catch (err) {
      result.probeErrors.push(`${overviewUrl}: ${err.message}`);
    }
  }

  const photosUrl = photosUrlFromOverview(overviewUrl);
  if (photosUrl) {
    try {
      const photos = await fetchPropertyPageHtml(photosUrl, { usePuppeteer });
      const probe = {
        url: photos.url,
        pageType: "photos",
        status: photos.accessDenied ? 403 : 200,
        accessDenied: Boolean(photos.accessDenied),
        fetchMethod: photos.fetchMethod,
        imageCount: 0,
      };
      if (photos.html && !photos.accessDenied) {
        const refs = extractImageUrlReferencesFromHtml(photos.html, {
          label: property.propertyName,
          sourceBasis: SOURCE_BASIS,
        }).filter((r) => !isRejectedImageUrl(r.url));

        const coverUrl = result.coverImage?.url || "";
        const galleryRefs = refs.filter((r) => r.url !== coverUrl);

        if (!result.coverImage && refs.length) {
          const first = refs[0];
          result.coverImage = {
            url: first.url,
            source: "photos:first",
            imageRole: "cover",
            sourcePageUrl: photos.url,
            resolvedFrom: "photos:first",
            context: "cover",
          };
          result.imageCandidates.push({
            url: first.url,
            sourcePageUrl: photos.url,
            assetTypeGuess: first.assetTypeGuess,
            context: inferImageContext(first.url, property.propertyName),
            imageRole: "cover",
            resolvedFrom: "photos:first",
          });
        }

        probe.imageCount = galleryRefs.length;
        result.pageContextConfirmed = result.pageContextConfirmed || refs.length > 0;
        for (const ref of galleryRefs.slice(0, 7)) {
          result.imageCandidates.push({
            url: ref.url,
            sourcePageUrl: photos.url,
            assetTypeGuess: ref.assetTypeGuess,
            context: inferImageContext(ref.url, property.propertyName),
            imageRole: "gallery",
            resolvedFrom: "photos:gallery",
          });
        }
      } else if (photos.accessDenied) {
        probe.note = "access_denied on photos page";
        result.probeErrors.push(`access_denied:${photosUrl}`);
      }
      result.sourcePagesProbed.push(probe);
    } catch (err) {
      result.probeErrors.push(`${photosUrl}: ${err.message}`);
    }
  }

  result.propertySetting = inferPropertySetting(property.propertyName, result.descriptionSnippet);
  result.imageCandidates = dedupeImageCandidates(result.imageCandidates);
  return result;
}

function inferImageContext(url, propertyName) {
  const u = url.toLowerCase();
  if (/room|guest|suite|bed/i.test(u)) return "guestroom";
  if (/lobby|lounge|bar|restaurant|dining/i.test(u)) return "public space";
  if (/pool|beach|exterior|facade|aerial/i.test(u)) return "exterior";
  return "property";
}

function dedupeImageCandidates(candidates) {
  const seen = new Set();
  const out = [];
  for (const c of candidates) {
    const base = c.url.replace(/-\d+x\d+(?=\.\w+$)/, "");
    if (seen.has(base)) continue;
    seen.add(base);
    out.push(c);
  }
  return out;
}

function assessSlotSuitability(property) {
  const named = property.propertyConfirmed;
  const cala = property.calaRelevant === "Yes";
  const hasImages = property.imageCandidates.length > 0;
  const setting = property.propertySetting || "Urban";

  const hero =
    named &&
    property.brandConfirmed &&
    cala &&
    (hasImages || property.overviewUrl);

  const gallery = named && property.brandConfirmed && cala && hasImages;

  const recentOpenings = false;

  const valueDrivers = {
    Urban: setting === "Urban",
    Resort: setting === "Resort",
    "Conversion / Adaptive Reuse": setting === "Conversion / Adaptive Reuse",
    "Boutique / Lifestyle": setting === "Boutique / Lifestyle",
    "Mixed-Use": setting === "Mixed-Use",
  };

  return {
    heroImage: hero,
    imageGallery: gallery,
    recentOpenings,
    valueDrivers,
    gaps: [
      ...(!hasImages ? ["No Marriott-controlled image URLs extracted (page may be access-denied or JS-rendered)"] : []),
      ...(!property.overviewUrl ? ["Missing official property overview URL"] : []),
      ...(!recentOpenings ? ["No opening/PR date tied to this property in v1 discovery"] : []),
    ],
  };
}

function buildRegistryDedupeKeyDiscovery(record, brandRecordId) {
  return [
    brandRecordId,
    nz(record.sourceUrl),
    nz(record.recommendedExplorerSlot),
    nz(record.relatedPropertyName),
  ].join("|");
}

function mapImageToRegistryRecord({
  property,
  image,
  slot,
  explorerSection,
  slotPurpose,
  galleryIndex,
  brandRecordId,
  stagingRunId,
  isPrimary = false,
}) {
  const valueDriver =
    slot === VISUAL_SLOT.VALUE_DRIVER ? property.propertySetting || "None" : "None";
  const assetType =
    image.context === "guestroom"
      ? "Guestroom"
      : image.context === "public space"
        ? "Lobby / Public Space"
        : "Exterior / Property";

  const slotKey =
    slot === VISUAL_SLOT.GALLERY && galleryIndex != null
      ? `materials.gallery.${galleryIndex}`
      : slot === VISUAL_SLOT.HERO
        ? "Brand Setup — Explorer Hero"
        : slot === VISUAL_SLOT.VALUE_DRIVER
          ? "overview.why_value"
          : slot === VISUAL_SLOT.RECENT_OPENINGS
            ? "footprint.openings"
            : explorerSection;

  const assetName = `${property.propertyName} — ${slot}${galleryIndex ? ` ${galleryIndex}` : ""} (${image.imageRole || image.context || "property"})`;

  const fields = {
    [MAP_BRAND_ASSET.assetName]: assetName,
    [MAP_BRAND_ASSET.brand]: [brandRecordId],
    [MAP_BRAND_ASSET.brandRecordId]: brandRecordId,
    [MAP_BRAND_ASSET.parentCompany]: PARENT_COMPANY,
    [MAP_BRAND_ASSET.assetType]: slot === VISUAL_SLOT.HERO ? "Hero Image" : assetType,
    [MAP_BRAND_ASSET.assetStatus]: "Candidate",
    [MAP_BRAND_ASSET.sourceBasis]: SOURCE_BASIS,
    [MAP_BRAND_ASSET.sourceUrl]: image.url,
    [MAP_BRAND_ASSET.sourcePageUrl]: image.sourcePageUrl || property.overviewUrl,
    [MAP_BRAND_ASSET.usageReviewStatus]: "Pending Review",
    [MAP_BRAND_ASSET.explorerUsePermission]: "Candidate Only",
    [MAP_BRAND_ASSET.recommendedExplorerSlot]: slotKey,
    [MAP_BRAND_ASSET.isPrimaryCandidate]: isPrimary,
    [MAP_BRAND_ASSET.sourceNotes]: `CALA Tribute discovery v2 — ${image.imageRole === "cover" ? "official property cover image" : "property gallery image"} from Marriott-controlled source (${image.resolvedFrom || "overview/photos"}). Usage review required; not approved for Explorer.`,
    [MAP_BRAND_ASSET.reviewNotes]: "Metadata-only staging — no image download.",
    [MAP_BRAND_ASSET.stagingRunId]: stagingRunId,
    [MAP_BRAND_ASSET.companyValidated]: false,
    [MAP_VISUAL_SLOT.explorerSection]: slot,
    [MAP_VISUAL_SLOT.slotPurpose]:
      slot === VISUAL_SLOT.HERO
        ? "Primary brand-level property visual"
        : slot === VISUAL_SLOT.GALLERY
          ? "Gallery of different real Tribute Portfolio hotels"
          : slot === VISUAL_SLOT.VALUE_DRIVER
            ? "Visualize value driver"
            : slotPurpose || "",
    [MAP_VISUAL_SLOT.relatedValueDriver]: valueDriver,
    [MAP_VISUAL_SLOT.relatedPropertyName]: property.propertyName,
    [MAP_VISUAL_SLOT.countryRegion]: property.countryRegion,
    [MAP_VISUAL_SLOT.calaRelevant]: property.calaRelevant,
    [MAP_VISUAL_SLOT.propertyConfirmed]: "Yes",
    [MAP_VISUAL_SLOT.brandConfirmed]: "Yes",
    [MAP_VISUAL_SLOT.sourcePageConfirmsContext]: property.pageContextConfirmed ? "Yes" : "Unknown",
    [MAP_VISUAL_SLOT.useCaseMatch]: valueDriver,
    [MAP_VISUAL_SLOT.validationStatus]: "Needs Usage Review",
    [MAP_VISUAL_SLOT.validationNotes]:
      image.imageRole === "cover"
        ? `Official Marriott property cover image (${image.resolvedFrom || "overview"}) — named CALA Tribute property; usage review required before Explorer approval.`
        : "Named CALA Tribute property gallery candidate from official Marriott source — usage review required before Explorer approval.",
  };

  return {
    assetName,
    relatedPropertyName: property.propertyName,
    recommendedExplorerSlot: slotKey,
    sourceUrl: image.url,
    mappedVisualSlot: slot,
    fields,
  };
}

function pickGalleryImages(property) {
  const coverUrl = property.coverImage?.url || "";
  return property.imageCandidates.filter(
    (img) => img.imageRole === "gallery" && img.url !== coverUrl
  );
}

function pickHeroImage(property) {
  if (property.coverImage?.url) {
    const match = property.imageCandidates.find((i) => i.url === property.coverImage.url);
    return match || {
      url: property.coverImage.url,
      sourcePageUrl: property.coverImage.sourcePageUrl,
      context: "cover",
      imageRole: "cover",
      resolvedFrom: property.coverImage.resolvedFrom,
    };
  }
  return pickBestHeroImage(property.imageCandidates);
}

function buildRegistryProposals(properties, brandRecordId) {
  const stagingRunId = `cala-tribute-discovery-${Date.now()}`;
  const proposals = [];

  const heroCandidates = [];
  const galleryCandidates = [];
  const valueDriverCandidates = [];

  for (const property of properties) {
    const suit = property.slotSuitability;
    const heroImage = pickHeroImage(property);
    const galleryImages = pickGalleryImages(property);

    if (suit.heroImage && heroImage?.url) {
      heroCandidates.push({ property, image: heroImage });
    }
    if (suit.imageGallery) {
      for (const img of galleryImages.slice(0, 2)) {
        galleryCandidates.push({ property, image: img });
      }
      if (!galleryImages.length && heroImage?.url) {
        galleryCandidates.push({ property, image: { ...heroImage, imageRole: "gallery" } });
      }
    }
    if (suit.valueDrivers[property.propertySetting] && heroImage?.url) {
      valueDriverCandidates.push({ property, image: heroImage });
    }
  }

  let galleryIdx = 1;
  const usedGalleryProperties = new Set();
  for (const { property, image } of galleryCandidates) {
    if (usedGalleryProperties.has(property.propertyName) && galleryIdx > 1) continue;
    usedGalleryProperties.add(property.propertyName);
    proposals.push(
      mapImageToRegistryRecord({
        property,
        image,
        slot: VISUAL_SLOT.GALLERY,
        galleryIndex: galleryIdx,
        brandRecordId,
        stagingRunId,
        isPrimary: galleryIdx === 1,
      })
    );
    galleryIdx += 1;
    if (galleryIdx > 6) break;
  }

  const heroPick = heroCandidates.sort((a, b) => {
    const score = (p) => {
      let s = 0;
      if (p.property.coverImage?.resolvedFrom === "og:image") s += 4;
      else if (p.property.coverImage?.resolvedFrom?.startsWith("next_data")) s += 3;
      else if (p.property.coverImage) s += 2;
      if (p.property.pageContextConfirmed) s += 1;
      if (/rumbao|holbox|cartagena|ermita/i.test(p.property.propertyName)) s += 1;
      return s;
    };
    return score(b) - score(a);
  })[0];

  if (heroPick) {
    proposals.unshift(
      mapImageToRegistryRecord({
        property: heroPick.property,
        image: heroPick.image,
        slot: VISUAL_SLOT.HERO,
        brandRecordId,
        stagingRunId,
        isPrimary: true,
      })
    );
  }

  for (const { property, image } of valueDriverCandidates.slice(0, 5)) {
    proposals.push(
      mapImageToRegistryRecord({
        property,
        image,
        slot: VISUAL_SLOT.VALUE_DRIVER,
        brandRecordId,
        stagingRunId,
      })
    );
  }

  return { proposals, stagingRunId, heroPick: heroPick || null, galleryCount: galleryIdx - 1 };
}

function pickBestHeroImage(images) {
  const cover = images.find((i) => i.imageRole === "cover");
  if (cover) return cover;
  const scored = images.map((img) => {
    let score = 0;
    if (/exterior|facade|aerial|hero/i.test(img.url)) score += 3;
    if (/pool|beach|lobby/i.test(img.url)) score += 2;
    if (/room|guest/i.test(img.url)) score += 1;
    if (img.context === "exterior" || img.context === "cover") score += 2;
    return { img, score };
  });
  scored.sort((a, b) => b.score - a.score);
  return scored[0]?.img || images[0];
}

function buildExistingRegistryDedupeSet(existingRegistry, brandRecordId) {
  const keys = new Set();
  const sourceUrls = new Set();
  for (const r of existingRegistry) {
    if (nz(r.sourceUrl)) sourceUrls.add(nz(r.sourceUrl));
    keys.add(
      buildRegistryDedupeKeyDiscovery(
        {
          sourceUrl: r.sourceUrl,
          recommendedExplorerSlot: r.recommendedExplorerSlot,
          relatedPropertyName: r.assetName,
        },
        brandRecordId
      )
    );
    const propFromName = nz(r.assetName).split(" — ")[0];
    if (propFromName) {
      keys.add(
        buildRegistryDedupeKeyDiscovery(
          {
            sourceUrl: r.sourceUrl,
            recommendedExplorerSlot: r.recommendedExplorerSlot,
            relatedPropertyName: propFromName,
          },
          brandRecordId
        )
      );
    }
  }
  return { keys, sourceUrls };
}

async function registryDataFetch(url, apiKey, init = {}) {
  const res = await fetch(url, {
    ...init,
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
      ...(init.headers || {}),
    },
  });
  const json = await res.json().catch(() => ({}));
  return { res, json };
}

function registryTableName() {
  return (
    process.env.PARTNER_INTELLIGENCE_ASSET_REGISTRY_TABLE_ID ||
    process.env.PARTNER_INTELLIGENCE_ASSET_REGISTRY_TABLE ||
    "Partner Intelligence - Brand Asset Registry"
  );
}

async function createRegistryRecordsBatch(baseId, apiKey, recordsFields) {
  const table = encodeURIComponent(registryTableName());
  const url = `https://api.airtable.com/v0/${baseId}/${table}`;
  const { res, json } = await registryDataFetch(url, apiKey, {
    method: "POST",
    body: JSON.stringify({
      records: recordsFields.map((fields) => ({ fields })),
      typecast: true,
    }),
  });
  if (!res.ok) {
    throw new Error(json.error?.message || `Airtable create registry batch failed: ${res.status}`);
  }
  return json.records || [];
}

export async function buildCalaTributeDiscoveryReport({
  probePages = true,
  usePuppeteer = false,
  apply = false,
  applyApproved = false,
  crawlSitemaps = true,
} = {}) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) {
    return { error: "AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required." };
  }

  const mode = apply && applyApproved ? "candidates-apply" : "dry-run";
  let airtableModified = false;

  const filesRead = [
    "lib/partner-intelligence/cala-tribute-property-visual-discovery.js",
    "lib/partner-intelligence/marriott-property-cover-image.js",
    "lib/marriott-brand-directory-extract.js",
    "lib/marriott-hotel-content-fetch.js",
    "lib/partner-intelligence/brand-asset-pr-package-governance.js",
    "reports/marriott-overview-import-plan.json",
  ];

  let sitemapHotels = [];
  let sitemapErrors = [];
  if (crawlSitemaps) {
    try {
      const crawl = await crawlMarriottCountrySitemaps({
        countrySlugs: CALA_SITEMAP_SLUGS,
        delayMs: 120,
      });
      sitemapHotels = crawl.hotels || [];
    } catch (err) {
      sitemapErrors.push(err.message);
    }
  }

  const tributeSitemapRows = sitemapHotels.filter((h) => isTributePortfolioHotel(h.name, h.url));
  const properties = mergeProperties(tributeSitemapRows, SEED_TRIBUTE_CALA_PROPERTIES);

  const probed = [];
  for (const property of properties) {
    const p = await probePropertyPages(property, { probePages, usePuppeteer });
    p.slotSuitability = assessSlotSuitability(p);
    probed.push(p);
  }

  const existingRegistry = await listRegistryAssetsForBrand(TRIBUTE_RECORD_ID);
  const weakExisting = existingRegistry.filter((r) =>
    /property\/design image|hero — consumer property wide/i.test(r.assetName)
  );

  const { proposals, stagingRunId, heroPick, galleryCount } = buildRegistryProposals(
    probed,
    TRIBUTE_RECORD_ID
  );

  const { keys: existingDedupe, sourceUrls: existingSourceUrls } = buildExistingRegistryDedupeSet(
    existingRegistry,
    TRIBUTE_RECORD_ID
  );

  const proposed = [];
  const skippedDuplicates = [];
  for (const p of proposals) {
    const key = buildRegistryDedupeKeyDiscovery(p, TRIBUTE_RECORD_ID);
    if (existingSourceUrls.has(nz(p.sourceUrl)) || existingDedupe.has(key)) {
      skippedDuplicates.push({ assetName: p.assetName, dedupeKey: key, reason: "existing registry match" });
      continue;
    }
    const validation = validateRegistryWritePayload(p.fields);
    if (!validation.valid) {
      skippedDuplicates.push({ assetName: p.assetName, errors: validation.errors });
      continue;
    }
    proposed.push({ ...p, dedupeKey: key });
  }

  let created = [];
  if (apply && applyApproved && proposed.length) {
    const BATCH = 10;
    for (let i = 0; i < proposed.length; i += BATCH) {
      const batch = proposed.slice(i, i + BATCH);
      const records = await createRegistryRecordsBatch(
        baseId,
        apiKey,
        batch.map((p) => p.fields)
      );
      created.push(...records.map((r) => ({ recordId: r.id, assetName: r.fields?.[MAP_BRAND_ASSET.assetName] })));
    }
    airtableModified = created.length > 0;
  }

  const calaRelevant = probed.filter((p) => p.calaRelevant === "Yes");
  const namedConfirmed = probed.filter((p) => p.propertyConfirmed);
  const withImages = probed.filter((p) => p.imageCandidates.length > 0);

  const heroSuitable = probed.filter((p) => p.slotSuitability.heroImage);
  const gallerySuitable = probed.filter((p) => p.slotSuitability.imageGallery);
  const openingsSuitable = probed.filter((p) => p.slotSuitability.recentOpenings);

  const valueDriverCoverage = {
    Urban: probed.filter((p) => p.slotSuitability.valueDrivers.Urban && p.imageCandidates.length),
    Resort: probed.filter((p) => p.slotSuitability.valueDrivers.Resort && p.imageCandidates.length),
    "Conversion / Adaptive Reuse": probed.filter(
      (p) => p.slotSuitability.valueDrivers["Conversion / Adaptive Reuse"] && p.imageCandidates.length
    ),
    "Boutique / Lifestyle": probed.filter(
      (p) => p.slotSuitability.valueDrivers["Boutique / Lifestyle"] && p.imageCandidates.length
    ),
    "Mixed-Use": probed.filter((p) => p.slotSuitability.valueDrivers["Mixed-Use"] && p.imageCandidates.length),
  };

  const missingValueDrivers = Object.entries(valueDriverCoverage)
    .filter(([, list]) => list.length === 0)
    .map(([k]) => k);

  const missingSlots = [];
  if (!heroSuitable.some((p) => p.imageCandidates.length)) missingSlots.push("Hero Image");
  if (gallerySuitable.filter((p) => p.imageCandidates.length).length < 3) missingSlots.push("Image Gallery");
  if (!openingsSuitable.length) missingSlots.push("Recent Openings");
  if (missingValueDrivers.length) missingSlots.push("Where This Brand Creates the Most Value");

  const applyCommand =
    "npm run cala-tribute-property-visual-discovery -- --apply --approve-cala-tribute-visual-candidates";

  return {
    discoveryVersion: DISCOVERY_VERSION,
    generatedAt: new Date().toISOString(),
    mode,
    airtableModified,
    brandSetupMediaUntouched: true,
    textGovernancePlatformReady: true,
    filesRead,
    brand: {
      recordId: TRIBUTE_RECORD_ID,
      name: BRAND_NAME,
      parentCompany: PARENT_COMPANY,
    },
    sitemap: {
      calaSlugsCrawled: CALA_SITEMAP_SLUGS.length,
      totalHotelsInCala: sitemapHotels.length,
      tributePortfolioHotels: tributeSitemapRows.length,
      errors: sitemapErrors,
    },
    propertiesDiscovered: probed.length,
    properties: probed,
    marriottControlledSourcePages: probed
      .flatMap((p) => p.sourcePagesProbed.map((s) => s.url))
      .filter(Boolean),
    imageCandidatesFound: withImages.reduce((n, p) => n + p.imageCandidates.length, 0),
    calaRelevantCount: calaRelevant.length,
    namedPropertyConfirmedCount: namedConfirmed.length,
    heroSuitableProperties: heroSuitable.map((p) => p.propertyName),
    gallerySuitableProperties: gallerySuitable.map((p) => p.propertyName),
    recentOpeningSuitableProperties: openingsSuitable.map((p) => p.propertyName),
    valueDriverCoverage: Object.fromEntries(
      Object.entries(valueDriverCoverage).map(([k, v]) => [k, v.map((p) => p.propertyName)])
    ),
    missingValueDrivers,
    missingSlots,
    heroRecommendation: heroPick
      ? {
          propertyName: heroPick.property.propertyName,
          imageUrl: heroPick.image.url,
          countryRegion: heroPick.property.countryRegion,
          coverResolvedFrom: heroPick.property.coverImage?.resolvedFrom || heroPick.image.resolvedFrom || null,
          imageRole: heroPick.image.imageRole || "cover",
          note: "Primary CALA hero candidate from official property cover image — usage review required; not approved for Explorer.",
        }
      : null,
    coverImageStats: {
      withCoverImage: probed.filter((p) => p.coverImage?.url).length,
      ogImage: probed.filter((p) => p.coverImage?.resolvedFrom === "og:image").length,
      nextData: probed.filter((p) => String(p.coverImage?.resolvedFrom || "").startsWith("next_data")).length,
      photosFallback: probed.filter((p) => p.coverImage?.resolvedFrom === "photos:first").length,
      noCover: probed.filter((p) => !p.coverImage?.url).length,
    },
    galleryRecommendations: gallerySuitable
      .filter((p) => p.imageCandidates.length)
      .slice(0, 6)
      .map((p) => ({
        propertyName: p.propertyName,
        countryRegion: p.countryRegion,
        imageCount: p.imageCandidates.length,
        topImage: p.imageCandidates[0]?.url || null,
      })),
    weakExistingCandidates: weakExisting.map((r) => ({
      assetName: r.assetName,
      assetStatus: r.assetStatus,
      note: "Remains Not Enough Context — generic consumer-site crops without named property.",
    })),
    registry: {
      stagingRunId,
      proposedCount: proposed.length,
      skippedDuplicateCount: skippedDuplicates.length,
      createdCount: created.length,
      proposed,
      skippedDuplicates,
      created,
    },
    applyCommand: proposed.length ? applyCommand : null,
    nextCommand: "npm run cala-tribute-property-visual-discovery -- --dry-run",
    remainingWorkToVisualParity: [
      "Rendered page capture for Marriott property pages blocked by access_denied (image URL extraction gap).",
      "Recent Openings — capture specific opening/PR with property name + date.",
      "Complete value-driver imagery for any drivers still Missing.",
      "Human usage review on staged CALA property candidates before Explorer promotion.",
      "Future: governed hero/logo promotion writer (no Brand Setup overwrite until approved).",
    ],
    doesNotDo: [
      "Download images",
      "Attach image files",
      "Overwrite Brand Setup media fields",
      "Approve assets for Explorer use",
      "Replace Mock/Demo hero",
      "Delete existing asset records",
      "Use OTA / Google Images",
      "Set Company Validated or Company Validation Date",
    ],
  };
}

export function buildCalaTributeDiscoveryMarkdown(report) {
  if (report.error) {
    return `# CALA Tribute Property Visual Candidate Discovery v1\n\nError: ${report.error}\n`;
  }

  const lines = [
    "# CALA Tribute Property Visual Candidate Discovery v2",
    "",
    `Generated: ${report.generatedAt}`,
    `Mode: **${report.mode}** · Airtable modified: **${report.airtableModified ? "yes" : "no"}**`,
    `Brand: ${report.brand.name} \`${report.brand.recordId}\``,
    `Text/governance Platform Ready: **${report.textGovernancePlatformReady ? "yes" : "no"}**`,
    `Brand Setup media untouched: **${report.brandSetupMediaUntouched ? "yes" : "no"}**`,
    "",
    "## 1. Files read",
    "",
    ...report.filesRead.map((f) => `- \`${f}\``),
    "",
    "## 2. Properties discovered",
    "",
    `**${report.propertiesDiscovered}** named Tribute Portfolio properties (${report.sitemap.tributePortfolioHotels} from CALA sitemaps + seeds).`,
    "",
    "| Property | Country | MARSHA | Cover | Cover source | Gallery imgs | Hero | Setting |",
    "|----------|---------|--------|-------|--------------|--------------|------|---------|",
    ...report.properties.map(
      (p) =>
        `| ${p.propertyName} | ${p.countryRegion || "—"} | ${p.marsha || "—"} | ${p.coverImage?.url ? "yes" : "no"} | ${p.coverImage?.resolvedFrom || "—"} | ${p.imageCandidates.filter((i) => i.imageRole === "gallery").length} | ${p.slotSuitability.heroImage ? "yes" : "no"} | ${p.propertySetting || "—"} |`
    ),
    "",
    "## 3. Marriott-controlled source pages",
    "",
    ...(report.marriottControlledSourcePages.length
      ? report.marriottControlledSourcePages.map((u) => `- ${u}`)
      : ["- None probed successfully"]),
    "",
    "## 4. Image candidates",
    "",
    `**${report.imageCandidatesFound}** image URL references (metadata only; not downloaded).`,
    "",
    "## 5. CALA-relevant / named-property confirmed",
    "",
    `- CALA-relevant: **${report.calaRelevantCount}**`,
    `- Named property confirmed: **${report.namedPropertyConfirmedCount}**`,
    "",
    "## 6. Slot suitability",
    "",
    `- Hero suitable: ${report.heroSuitableProperties.join(", ") || "none"}`,
    `- Gallery suitable: ${report.gallerySuitableProperties.join(", ") || "none"}`,
    `- Recent openings: ${report.recentOpeningSuitableProperties.join(", ") || "none"}`,
    "",
    "### Value driver coverage",
    "",
    ...Object.entries(report.valueDriverCoverage).map(
      ([k, v]) => `- **${k}**: ${v.length ? v.join(", ") : "Missing"}`
    ),
    "",
    `**Missing value drivers:** ${report.missingValueDrivers.join(", ") || "none"}`,
    `**Missing slots:** ${report.missingSlots.join(", ") || "none"}`,
    "",
    "## 7. Hero recommendation (property cover image)",
    "",
    report.heroRecommendation
      ? `- **${report.heroRecommendation.propertyName}** (${report.heroRecommendation.countryRegion})\n  - Cover source: \`${report.heroRecommendation.coverResolvedFrom || "unknown"}\`\n  - ${report.heroRecommendation.imageUrl}\n  - ${report.heroRecommendation.note}`
      : "- No hero image candidate with extracted cover URL — property pages may require --use-puppeteer.",
    "",
    "### Cover image resolution stats",
    "",
    ...(report.coverImageStats
      ? [
          `- Properties with cover image: **${report.coverImageStats.withCoverImage}**`,
          `- Resolved via og:image: **${report.coverImageStats.ogImage}**`,
          `- Resolved via __NEXT_DATA__: **${report.coverImageStats.nextData}**`,
          `- Photos-page fallback: **${report.coverImageStats.photosFallback}**`,
          `- No cover extracted: **${report.coverImageStats.noCover}**`,
          "",
        ]
      : []),
    "",
    "## 8. Gallery recommendations",
    "",
    ...(report.galleryRecommendations.length
      ? report.galleryRecommendations.map(
          (g) => `- **${g.propertyName}** (${g.countryRegion}) — ${g.imageCount} image(s)`
        )
      : ["- None with extracted image URLs"]),
    "",
    "## 9. Weak existing candidates (unchanged)",
    "",
    ...report.weakExistingCandidates.map((w) => `- **${w.assetName}** — ${w.note}`),
    "",
    "## 10. Recommended registry records",
    "",
    `- Proposed: **${report.registry.proposedCount}** · Skipped duplicates: **${report.registry.skippedDuplicateCount}** · Created: **${report.registry.createdCount}**`,
    "",
    ...(report.registry.proposed.length
      ? report.registry.proposed.map(
          (p) => `- **${p.assetName}** → \`${p.recommendedExplorerSlot}\` · ${p.sourceUrl}`
        )
      : ["- None proposed"]),
    "",
    "## 11. Apply command",
    "",
    report.applyCommand ? `\`\`\`bash\n${report.applyCommand}\n\`\`\`` : "_No new registry records to create._",
    "",
    "## 12. Remaining gap to visual parity",
    "",
    ...report.remainingWorkToVisualParity.map((w) => `- ${w}`),
    "",
    "## Does not do",
    "",
    ...report.doesNotDo.map((d) => `- ${d}`),
  ];

  return `${lines.join("\n")}\n`;
}
