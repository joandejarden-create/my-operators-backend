/**
 * Brand Explorer Lifestyle / Affiliation Source Capture v35C.
 *
 * Design Hotels + SLH — Source Library seeding only.
 * No presentation, registry, image-field, or Company Validated changes.
 */
import {
  MAP_PARTNER_SOURCE,
  VAL_PARTNER_SOURCE_SELECTS,
} from "../../api/lib/partner-intelligence-field-map.js";
import {
  createPartnerSource,
  listPartnerSources,
  patchPartnerSource,
} from "./airtable-source.js";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import { getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";
import {
  extractOgImageFromHtml,
  isTemporaryAirtableUrl,
} from "./brand-explorer-radisson-individuals-durable-gallery-source-repair-writer.js";
import {
  classifyPropertyExampleImage,
  isGenericBrandOrLifestyleImageUrl,
  isLogoImageUrl,
} from "./brand-explorer-footprint-opening-image-governance.js";
import { downloadUrlWithFallback, estimateReadableTextLength } from "./choice-legacy-batch-url-capture.js";
import { isBlockedSourceUrl } from "./brand-explorer-choice-extended-stay-source-capture-writer.js";
import {
  DESIGN_HOTELS_PROPERTY_CATALOG,
  SLH_PROPERTY_CATALOG,
} from "./brand-explorer-lifestyle-affiliation-property-catalog.js";

export const V35C_VERSION = "v35C";
export { DESIGN_HOTELS_PROPERTY_CATALOG, SLH_PROPERTY_CATALOG };

export const APPLY_FLAG_APPROVE = "--approve-brand-explorer-v35C-lifestyle-affiliation-source-capture";
export const APPLY_FLAG_SOURCE_ONLY = "--confirm-source-library-only";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";
export const APPLY_FLAG_NO_PRESENTATION = "--confirm-no-presentation-row-changes";
export const APPLY_FLAG_NO_REGISTRY = "--confirm-no-registry-changes";
export const APPLY_FLAG_NO_IMAGE_FIELDS = "--confirm-no-image-field-changes";
export const APPLY_FLAG_NO_ACTIVE_APPROVAL = "--confirm-no-active-profile-approval";
export const APPLY_FLAG_DESIGN_SLH_ONLY = "--confirm-design-slh-only";

const ALLOWED_SOURCE_TYPES = Object.freeze([
  "Brand Page",
  "Development Page",
  "Portfolio Page",
  "Press Release",
  "Website Capture",
  "Case Study",
  "Other",
]);

const TARGET_BRANDS = Object.freeze([
  {
    slug: "design-hotels",
    recordId: "rec02zPClpWUTCyXM",
    name: "Design Hotels",
    modelType: "affiliation_curation_platform",
    copyGuidance: "Affiliation / curation platform — no franchise-flag language.",
  },
  {
    slug: "small-luxury-hotels-of-the-world",
    recordId: "recjjSnY2opb8P4DG",
    name: "Small Luxury Hotels of the World",
    modelType: "independent_luxury_consortium",
    copyGuidance: "Independent luxury consortium — no parent-brand or franchise language.",
  },
]);

const TRIBUTE_BENCHMARK = Object.freeze({
  slug: "tribute-portfolio",
  recordId: "recCvV0PuZOi8c3hC",
  readOnly: true,
});

export const DESIGN_HOTELS_SOURCE_CATALOG = Object.freeze([
  {
    role: "p0_consumer_brand",
    sourceTitle: "Design Hotels — official consumer collection site",
    sourceUrl: "https://www.designhotels.com/",
    sourceType: "Brand Page",
    evidenceUseCases: ["Affiliation Model", "Collection Positioning", "Distribution"],
    region: "Global",
    approveForExplorer: true,
  },
  {
    role: "p0_about_collection",
    sourceTitle: "Design Hotels — about / curation platform",
    sourceUrl: "https://www.designhotels.com/about/",
    sourceType: "Brand Page",
    evidenceUseCases: ["Affiliation Model", "Curation Standards", "Owner Identity"],
    region: "Global",
    approveForExplorer: true,
  },
  {
    role: "p0_member_hotelier",
    sourceTitle: "Design Hotels — become a member hotel",
    sourceUrl: "https://www.designhotels.com/about/become-a-member-hotel/",
    sourceType: "Development Page",
    evidenceUseCases: ["Owner Criteria", "Membership Benefits", "Distribution"],
    region: "Global",
    approveForExplorer: true,
  },
  {
    role: "p0_property_directory",
    sourceTitle: "Design Hotels — global hotel directory",
    sourceUrl: "https://www.designhotels.com/hotels/",
    sourceType: "Portfolio Page",
    evidenceUseCases: ["Property Directory", "Property Examples", "Gallery Images"],
    region: "Global",
    approveForExplorer: true,
  },
  {
    role: "p1_culture_directions",
    sourceTitle: "Design Hotels Directions — culture and destination editorial",
    sourceUrl: "https://www.designhotels.com/directions/",
    sourceType: "Brand Page",
    evidenceUseCases: ["Cultural Identity", "Design Storytelling", "Curation Expectations"],
    region: "Global",
    approveForExplorer: true,
  },
  {
    role: "p1_further_study",
    sourceTitle: "Design Hotels Further — cultural hospitality study hub",
    sourceUrl: "https://www.designhotels.com/further/",
    sourceType: "Case Study",
    evidenceUseCases: ["Cultural Identity", "Design Storytelling"],
    region: "Global",
    approveForExplorer: true,
  },
  {
    role: "p1_bonvoy_distribution",
    sourceTitle: "Marriott Bonvoy — loyalty / distribution context for Design Hotels affiliation",
    sourceUrl: "https://www.marriott.com/loyalty.mi",
    sourceType: "Brand Page",
    evidenceUseCases: ["Distribution", "Recognition Platform"],
    region: "Global",
    approveForExplorer: true,
    note: "Affiliation distribution context only — not franchise conversion language.",
  },
]);

export const SLH_SOURCE_CATALOG = Object.freeze([
  {
    role: "p0_consumer_brand",
    sourceTitle: "SLH — official consumer consortium site",
    sourceUrl: "https://www.slh.com/",
    sourceType: "Brand Page",
    evidenceUseCases: ["Consortium Model", "Affiliation Value", "Distribution"],
    region: "Global",
    approveForExplorer: true,
  },
  {
    role: "p0_about_consortium",
    sourceTitle: "SLH — about the independent luxury consortium",
    sourceUrl: "https://www.slh.com/about-slh",
    sourceType: "Brand Page",
    evidenceUseCases: ["Consortium Model", "Quality Expectations", "Owner Identity"],
    region: "Global",
    approveForExplorer: true,
  },
  {
    role: "p0_quality_inspectors",
    sourceTitle: "SLH — meet the inspectors / quality assurance",
    sourceUrl: "https://www.slh.com/about-slh/meet-the-inspectors",
    sourceType: "Brand Page",
    evidenceUseCases: ["Participation Standards", "Quality Expectations"],
    region: "Global",
    approveForExplorer: true,
  },
  {
    role: "p0_hotel_directory",
    sourceTitle: "SLH — hotel types / participating properties directory",
    sourceUrl: "https://slh.com/hotel-types",
    sourceType: "Portfolio Page",
    evidenceUseCases: ["Property Directory", "Property Examples", "Gallery Images"],
    region: "Global",
    approveForExplorer: true,
  },
  {
    role: "p1_hotel_types",
    sourceTitle: "SLH — hotel types / collection positioning",
    sourceUrl: "https://www.slh.com/hotel-types",
    sourceType: "Brand Page",
    evidenceUseCases: ["Boutique Fit", "Luxury Positioning"],
    region: "Global",
    approveForExplorer: true,
  },
  {
    role: "p1_swoon_editorial",
    sourceTitle: "SLH Swoon — editorial / destination storytelling",
    sourceUrl: "https://swoon.slh.com/",
    sourceType: "Press Release",
    evidenceUseCases: ["Guest Experience", "Destination Context"],
    region: "Global",
    approveForExplorer: true,
  },
  {
    role: "p1_swoon_quality_editorial",
    sourceTitle: "SLH Swoon — what makes a really good hotel (quality editorial)",
    sourceUrl: "https://swoon.slh.com/what-makes-a-really-good-hotel/",
    sourceType: "Case Study",
    evidenceUseCases: ["Quality Expectations", "Guest Experience", "Participation Standards"],
    region: "Global",
    approveForExplorer: true,
  },
]);

const GALLERY_SLOT_LABELS = Object.freeze([
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

function normalizeUrlKey(url) {
  return nz(url).toLowerCase().replace(/\/+$/, "").split("?")[0];
}

function validateSelect(field, value) {
  const allowed = VAL_PARTNER_SOURCE_SELECTS[field];
  if (!allowed) return true;
  return allowed.includes(value);
}

function companyValidatedSnapshot(brandBasics) {
  const fields = brandBasics?.fields || brandBasics || {};
  return {
    companyValidated: fields["Company Validated"] ?? null,
    companyValidationDate: fields["Company Validation Date"] ?? null,
  };
}

function buildGovernanceNote(candidate) {
  const cases = (candidate.evidenceUseCases || []).join(", ");
  return [
    `v35C evidenceUseCase: ${cases || "General"}`,
    `usagePermission: Platform Display Allowed (AI-assisted / official public source — not company validation)`,
    `confidenceLevel: ${candidate.confidenceLevel || "Medium"}`,
    `externalDisplayStatus: Internal Review`,
    candidate.note || "",
    candidate.copyGuidance || "",
  ]
    .filter(Boolean)
    .join(" | ");
}

export function buildSourceLibraryRowPlan(candidate, brandRecordId, brandMeta = {}) {
  const errors = [];
  if (!nz(candidate.sourceTitle)) errors.push("sourceTitle required");
  if (!ALLOWED_SOURCE_TYPES.includes(candidate.sourceType)) {
    errors.push(`invalid sourceType: ${candidate.sourceType}`);
  }
  if (!validateSelect("sourceOrigin", "Public Web")) errors.push("invalid sourceOrigin");
  if (!validateSelect("status", "Captured")) errors.push("invalid status");

  const urlCheck = isBlockedSourceUrl(candidate.sourceUrl);
  if (urlCheck.blocked) errors.push(`blocked_url:${urlCheck.reason}`);

  const captureDate = new Date().toISOString().slice(0, 10);
  const approvedExplorer = candidate.approveForExplorer === true ? "Yes" : "No";
  if (!validateSelect("approvedForExplorerUse", approvedExplorer)) {
    errors.push(`invalid approvedForExplorerUse: ${approvedExplorer}`);
  }

  const fields = {
    [MAP_PARTNER_SOURCE.sourceTitle]: candidate.sourceTitle,
    [MAP_PARTNER_SOURCE.profileType]: "Brand",
    [MAP_PARTNER_SOURCE.brand]: [brandRecordId],
    [MAP_PARTNER_SOURCE.sourceType]: candidate.sourceType,
    [MAP_PARTNER_SOURCE.sourceUrl]: candidate.sourceUrl,
    [MAP_PARTNER_SOURCE.sourceOrigin]: "Public Web",
    [MAP_PARTNER_SOURCE.sourceQuality]: candidate.sourceQuality || "High",
    [MAP_PARTNER_SOURCE.status]: "Captured",
    [MAP_PARTNER_SOURCE.visibility]: "Public",
    [MAP_PARTNER_SOURCE.verifiedSource]: "Yes",
    [MAP_PARTNER_SOURCE.approvedForExtraction]: "No",
    [MAP_PARTNER_SOURCE.approvedForExplorerUse]: approvedExplorer,
    [MAP_PARTNER_SOURCE.region]: candidate.region || "Global",
    [MAP_PARTNER_SOURCE.captureDate]: captureDate,
    [MAP_PARTNER_SOURCE.lastReviewed]: captureDate,
    [MAP_PARTNER_SOURCE.notes]: buildGovernanceNote({ ...candidate, copyGuidance: brandMeta.copyGuidance }),
    [MAP_PARTNER_SOURCE.permissionVisibilityNotes]:
      "AI-assisted / official public source stewardship — not company validation. Source Library only; no presentation or registry writes in v35C.",
  };

  return {
    ok: errors.length === 0,
    errors,
    fields,
    governance: {
      usagePermission: "Platform Display Allowed",
      confidenceLevel: candidate.confidenceLevel || "Medium",
      evidenceUseCases: candidate.evidenceUseCases || [],
      externalDisplayStatus: "Internal Review",
      companyValidated: false,
      companyValidationImplied: false,
    },
  };
}

export function resolveAbsoluteImageUrl(baseUrl, raw) {
  const value = nz(raw).replace(/&amp;/g, "&");
  if (!value) return null;
  if (value.startsWith("//")) return `https:${value}`;
  if (value.startsWith("http")) return value;
  if (value.startsWith("/")) {
    try {
      return new URL(value, baseUrl).toString();
    } catch {
      return null;
    }
  }
  return null;
}

export function extractDesignHotelsImages(html, pageUrl) {
  const og = extractOgImageFromHtml(html);
  const images = [];
  if (og) images.push(og);
  const mediaMatches = html.match(/https:\/\/www\.designhotels\.com\/media\/[^"'\s>]+\.(?:jpg|jpeg|webp)[^"'\s>]*/gi) || [];
  for (const raw of mediaMatches) {
    const url = resolveAbsoluteImageUrl(pageUrl, raw);
    if (url && !images.includes(url)) images.push(url);
  }
  return images.slice(0, 12);
}

export function extractSlhImages(html, pageUrl) {
  const images = [];
  const patterns = [
    /\/-\/media\/slh\/hotels\/[^"'\s>]+\.(?:jpg|jpeg|webp)[^"'\s>]*/gi,
    /\/\/lucidcm\.imgix\.net\/[^"'\s>]+\.(?:jpg|jpeg|webp)(?:\.jpg)?[^"'\s>]*/gi,
  ];
  for (const re of patterns) {
    for (const raw of html.match(re) || []) {
      const url = resolveAbsoluteImageUrl(pageUrl, raw);
      if (url && !images.includes(url)) images.push(url);
    }
  }
  return images.slice(0, 12);
}

/** Property-specific Hotel Indigo Scene7 assets (reject shared ihg-* heroes). */
export function extractHotelIndigoImages(html, pageUrl) {
  const images = [];
  for (const m of html.matchAll(/digital\.ihg\.com\/is\/image\/ihg\/(hotel-indigo-[a-z0-9-]+)/gi) || []) {
    const id = decodeURIComponent(m[1]).trim();
    if (/logo|sketch/i.test(id)) continue;
    const url = `https://digital.ihg.com/is/image/ihg/${id}`;
    if (!images.includes(url)) images.push(url);
  }
  return images.slice(0, 12);
}

/** Accor ahstatic property photography keyed by hotel code (e.g. b1r7_ho_00). */
export function extractAccorPropertyImages(html, pageUrl) {
  const images = [];
  for (const m of html.matchAll(/ahstatic\.com\/photos\/([a-z0-9]+_(?:ho|ro)[a-z0-9_]*_p_\d+x\d+\.jpe?g)/gi) ||
    []) {
    const url = `https://www.ahstatic.com/photos/${m[1].toLowerCase()}`;
    if (!images.includes(url)) images.push(url);
  }
  return images.slice(0, 12);
}

export function classifyImageCandidate(imageUrl, { propertyName = "" } = {}) {
  if (!imageUrl) return { ok: false, reason: "missing_image_url" };
  if (isTemporaryAirtableUrl(imageUrl)) return { ok: false, reason: "temporary_airtable_url" };
  if (isLogoImageUrl(imageUrl)) return { ok: false, reason: "logo_image" };
  if (isGenericBrandOrLifestyleImageUrl(imageUrl)) return { ok: false, reason: "generic_lifestyle" };
  const cls = classifyPropertyExampleImage(imageUrl);
  if (cls.isLogo || cls.isGenericBrand || cls.isLifestyle) {
    return { ok: false, reason: "governance_classified_generic" };
  }
  if (/share-about|share-hotel-membership/i.test(imageUrl) && !propertyName) {
    return { ok: false, reason: "platform_share_card" };
  }
  return { ok: true, reason: null, classification: cls };
}

export async function probeSourceUrl(candidate) {
  try {
    const dl = await downloadUrlWithFallback(candidate.sourceUrl);
    const ext = dl.ext || ".html";
    const readableTextLength = estimateReadableTextLength(dl.buf, dl.contentType, ext);
    const reachable = dl.httpStatus >= 200 && dl.httpStatus < 400;
    return {
      ...candidate,
      reachable,
      httpStatus: dl.httpStatus,
      readableTextLength,
      finalUrl: dl.finalUrl || candidate.sourceUrl,
      jsShellRisk:
        reachable && readableTextLength < 400
          ? "high"
          : reachable && readableTextLength < 1200
            ? "medium"
            : "low",
      registerRecommended: reachable,
    };
  } catch (err) {
    return {
      ...candidate,
      reachable: false,
      httpStatus: null,
      readableTextLength: 0,
      registerRecommended: false,
      error: err.message,
    };
  }
}

export async function probePropertyPage(property, brandSlug) {
  try {
    const res = await fetch(property.sourcePageUrl, {
      headers: { "User-Agent": "Dealality-BrandExplorer/1.0" },
      redirect: "follow",
    });
    const html = await res.text();
    const extractor =
      brandSlug === "design-hotels"
        ? extractDesignHotelsImages
        : brandSlug === "hotel-indigo"
          ? extractHotelIndigoImages
          : brandSlug === "mgallery-collection"
            ? extractAccorPropertyImages
            : extractSlhImages;
    const probeUrl = property.galleryPageUrl || property.sourcePageUrl;
    const probeRes =
      probeUrl === property.sourcePageUrl
        ? res
        : await fetch(probeUrl, {
            headers: { "User-Agent": "Dealality-BrandExplorer/1.0" },
            redirect: "follow",
          });
    const probeHtml = probeUrl === property.sourcePageUrl ? html : await probeRes.text();
    const rawImages = extractor(probeHtml, probeUrl);
    const imageCandidates = [];
    for (const imageUrl of rawImages) {
      const durable = resolveAbsoluteImageUrl(property.sourcePageUrl, imageUrl);
      const check = classifyImageCandidate(durable, { propertyName: property.propertyName });
      if (check.ok) {
        imageCandidates.push({
          imageUrl: durable,
          sourcePageUrl: property.sourcePageUrl,
          classification: check.classification || null,
        });
      }
    }
    return {
      ...property,
      reachable: res.ok,
      httpStatus: res.status,
      imageCandidates,
      primaryImage: imageCandidates[0] || null,
      galleryImages: imageCandidates.slice(0, 6),
    };
  } catch (err) {
    return {
      ...property,
      reachable: false,
      httpStatus: null,
      imageCandidates: [],
      primaryImage: null,
      galleryImages: [],
      error: err.message,
    };
  }
}

async function fetchAllBrandSources(brandRecordId) {
  const all = [];
  let offset = "";
  do {
    const page = await listPartnerSources({ brandId: brandRecordId, limit: 100, offset });
    all.push(...(page.sources || []));
    offset = page.offset || "";
  } while (offset);
  return all;
}

function findExistingByUrl(sources, url) {
  const key = normalizeUrlKey(url);
  return sources.find((s) => normalizeUrlKey(s.sourceUrl) === key) || null;
}

export function buildGalleryCandidates(propertyProbes) {
  const gallery = [];
  for (const property of propertyProbes) {
    for (const img of property.galleryImages || []) {
      gallery.push({
        propertyName: property.propertyName,
        geographyLabel: property.geographyLabel,
        sourcePageUrl: img.sourcePageUrl,
        imageUrl: img.imageUrl,
        intendedGalleryLabel: GALLERY_SLOT_LABELS[gallery.length % GALLERY_SLOT_LABELS.length],
      });
      if (gallery.length >= 6) return gallery;
    }
  }
  return gallery;
}

function projectAssetPackReadiness({
  sourceCreates,
  sourceUpdates,
  existingCount,
  propertyProbes,
  galleryCandidates,
}) {
  const approvedAfter = sourceCreates.filter(
    (c) => c.fields?.[MAP_PARTNER_SOURCE.approvedForExplorerUse] === "Yes"
  ).length;
  const propertyWithImage = propertyProbes.filter((p) => p.primaryImage).length;
  const galleryCount = galleryCandidates.length;

  const stages = {
    assetPackDryRun: "blocked_by_sources",
    draftBuildDryRun: "blocked_by_sources",
    copyGovernanceDryRun: "blocked_by_sources",
  };

  if (existingCount + sourceCreates.length >= 5 && approvedAfter >= 4) {
    stages.assetPackDryRun = galleryCount >= 6 && propertyWithImage >= 3 ? "source_ready" : "source_partial";
  } else if (existingCount + sourceCreates.length >= 3) {
    stages.assetPackDryRun = "source_partial";
  }

  if (stages.assetPackDryRun === "source_ready") {
    stages.draftBuildDryRun = "source_partial";
    stages.copyGovernanceDryRun = propertyWithImage >= 3 ? "source_partial" : "blocked_by_property_images";
  } else if (galleryCount < 6) {
    stages.assetPackDryRun = galleryCount > 0 ? "blocked_by_gallery_images" : stages.assetPackDryRun;
  }

  if (propertyWithImage < 3) {
    stages.draftBuildDryRun = "blocked_by_property_images";
  }

  return {
    classification: stages.assetPackDryRun,
    stages,
    metrics: {
      proposedSourceCreates: sourceCreates.length,
      proposedSourceUpdates: sourceUpdates.length,
      existingSources: existingCount,
      propertyExamplesWithImage: propertyWithImage,
      galleryCandidates: galleryCount,
      approvedExplorerAfterSeed: approvedAfter,
    },
  };
}

async function processBrand(brand, options = {}) {
  const brandConfig = getActiveProfileBrandConfig(brand.slug);
  const catalog =
    brand.slug === "design-hotels" ? DESIGN_HOTELS_SOURCE_CATALOG : SLH_SOURCE_CATALOG;
  const propertyCatalog =
    brand.slug === "design-hotels" ? DESIGN_HOTELS_PROPERTY_CATALOG : SLH_PROPERTY_CATALOG;
  const propertyExampleCatalog = propertyCatalog.filter((p) => (p.galleryPriority || 1) === 1);

  const brandBasics = await fetchBrandBasics(brand.recordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasics);
  const existingSources = await fetchAllBrandSources(brand.recordId);

  const probedSources = [];
  for (const entry of catalog) {
    probedSources.push(await probeSourceUrl(entry));
    await new Promise((r) => setTimeout(r, 150));
  }

  const propertyProbes = [];
  for (const property of propertyCatalog) {
    propertyProbes.push(await probePropertyPage(property, brand.slug));
    await new Promise((r) => setTimeout(r, 200));
  }

  const galleryCandidates = buildGalleryCandidates(propertyProbes);

  const proposedCreates = [];
  const proposedUpdates = [];
  const blockedProposals = [];

  for (const source of probedSources) {
    if (!source.registerRecommended) {
      blockedProposals.push({ role: source.role, reason: "unreachable_or_failed_probe", source });
      continue;
    }
    const existing = findExistingByUrl(existingSources, source.finalUrl || source.sourceUrl);
    const plan = buildSourceLibraryRowPlan(
      {
        ...source,
        sourceUrl: source.finalUrl || source.sourceUrl,
        confidenceLevel: source.jsShellRisk === "high" ? "Low" : "High",
      },
      brand.recordId,
      brand
    );
    if (!plan.ok) {
      blockedProposals.push({ role: source.role, reason: "validation_failed", errors: plan.errors });
      continue;
    }
    if (existing) {
      proposedUpdates.push({
        action: "patch_metadata",
        recordId: existing.id,
        role: source.role,
        sourceUrl: source.sourceUrl,
        fields: {
          [MAP_PARTNER_SOURCE.notes]: plan.fields[MAP_PARTNER_SOURCE.notes],
          [MAP_PARTNER_SOURCE.lastReviewed]: plan.fields[MAP_PARTNER_SOURCE.lastReviewed],
          [MAP_PARTNER_SOURCE.approvedForExplorerUse]: plan.fields[MAP_PARTNER_SOURCE.approvedForExplorerUse],
          [MAP_PARTNER_SOURCE.sourceQuality]: plan.fields[MAP_PARTNER_SOURCE.sourceQuality],
        },
        governance: plan.governance,
      });
    } else {
      proposedCreates.push({
        action: "create",
        role: source.role,
        sourceUrl: source.sourceUrl,
        fields: plan.fields,
        governance: plan.governance,
      });
    }
  }

  for (const property of propertyProbes) {
    if (!propertyExampleCatalog.some((e) => e.propertyKey === property.propertyKey)) continue;
    if (!property.reachable || !property.primaryImage) continue;
    const title = `${brand.name} — property source: ${property.propertyName}`;
    const existing = findExistingByUrl(existingSources, property.sourcePageUrl);
    const plan = buildSourceLibraryRowPlan(
      {
        role: `property_${property.propertyKey}`,
        sourceTitle: title,
        sourceUrl: property.sourcePageUrl,
        sourceType: "Website Capture",
        evidenceUseCases: ["Property Examples", "Gallery Images", "Owner Fit"],
        region: property.geographyLabel?.includes("CALA") ? "CALA" : "Global",
        approveForExplorer: true,
        confidenceLevel: "High",
        note: `Primary image candidate: ${property.primaryImage.imageUrl}`,
      },
      brand.recordId,
      brand
    );
    if (!plan.ok) continue;
    if (existing) {
      if (nz(existing.notes).includes(property.propertyKey)) continue;
      proposedUpdates.push({
        action: "patch_property_source",
        recordId: existing.id,
        role: `property_${property.propertyKey}`,
        fields: {
          [MAP_PARTNER_SOURCE.notes]: plan.fields[MAP_PARTNER_SOURCE.notes],
          [MAP_PARTNER_SOURCE.lastReviewed]: plan.fields[MAP_PARTNER_SOURCE.lastReviewed],
        },
      });
    } else {
      proposedCreates.push({
        action: "create_property_source",
        role: `property_${property.propertyKey}`,
        sourceUrl: property.sourcePageUrl,
        fields: plan.fields,
        governance: plan.governance,
      });
    }
  }

  const assetPackProjection = projectAssetPackReadiness({
    sourceCreates: proposedCreates,
    sourceUpdates: proposedUpdates,
    existingCount: existingSources.length,
    propertyProbes,
    galleryCandidates,
  });

  return {
    slug: brand.slug,
    name: brand.name,
    recordId: brand.recordId,
    modelType: brand.modelType,
    copyGuidance: brand.copyGuidance,
    brandConfigPresent: Boolean(brandConfig),
    companyValidatedBefore,
    existingSourceCount: existingSources.length,
    existingSources: existingSources.map((s) => ({
      id: s.id,
      sourceTitle: s.sourceTitle,
      sourceUrl: s.sourceUrl,
      approvedForExplorerUse: s.approvedForExplorerUse,
      status: s.status,
    })),
    sourceDiscovery: probedSources,
    propertyExampleCandidates: propertyProbes.filter((p) =>
      propertyExampleCatalog.some((e) => e.propertyKey === p.propertyKey)
    ),
    propertyGalleryProbes: propertyProbes,
    galleryImageCandidates: galleryCandidates,
    proposedCreates,
    proposedUpdates,
    blockedProposals,
    assetPackProjection,
  };
}

export function buildApplyCommand(brands = ["design-hotels", "small-luxury-hotels-of-the-world"]) {
  return [
    "npm run brand-explorer-lifestyle-affiliation-source-capture --",
    `--brands ${brands.join(",")}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_SOURCE_ONLY,
    APPLY_FLAG_NO_VALIDATION,
    APPLY_FLAG_NO_PRESENTATION,
    APPLY_FLAG_NO_REGISTRY,
    APPLY_FLAG_NO_IMAGE_FIELDS,
    APPLY_FLAG_NO_ACTIVE_APPROVAL,
    APPLY_FLAG_DESIGN_SLH_ONLY,
  ].join(" ");
}

function buildBrandMarkdown(brandResult) {
  const lines = [];
  lines.push(`## ${brandResult.name} (\`${brandResult.slug}\`)`);
  lines.push(`- Record: \`${brandResult.recordId}\``);
  lines.push(`- Model: ${brandResult.modelType}`);
  lines.push(`- Existing sources: ${brandResult.existingSourceCount}`);
  lines.push(`- Proposed creates: ${brandResult.proposedCreates.length}`);
  lines.push(`- Proposed updates: ${brandResult.proposedUpdates.length}`);
  lines.push(`- Asset-pack projection: **${brandResult.assetPackProjection.classification}**`);
  lines.push("");
  lines.push("### Source Library plan");
  for (const create of brandResult.proposedCreates) {
    lines.push(`- **CREATE** [${create.role}] ${create.fields[MAP_PARTNER_SOURCE.sourceTitle]}`);
    lines.push(`  - URL: ${create.sourceUrl}`);
    lines.push(`  - Approved for Explorer: ${create.fields[MAP_PARTNER_SOURCE.approvedForExplorerUse]}`);
    lines.push(`  - Evidence: ${(create.governance?.evidenceUseCases || []).join(", ")}`);
  }
  for (const update of brandResult.proposedUpdates) {
    lines.push(`- **UPDATE** [${update.role}] \`${update.recordId}\``);
  }
  lines.push("");
  lines.push("### Property examples");
  for (const p of brandResult.propertyExampleCandidates) {
    lines.push(
      `- **${p.propertyName}** (${p.geographyLabel}) — ${p.primaryImage ? "image OK" : "no image"} — ${p.sourcePageUrl}`
    );
  }
  lines.push("");
  lines.push("### Gallery candidates");
  for (const g of brandResult.galleryImageCandidates) {
    lines.push(`- ${g.intendedGalleryLabel}: ${g.propertyName} — ${g.imageUrl}`);
  }
  lines.push("");
  lines.push("### Stage projection");
  for (const [stage, status] of Object.entries(brandResult.assetPackProjection.stages)) {
    lines.push(`- ${stage}: **${status}**`);
  }
  return lines.join("\n");
}

export async function buildLifestyleAffiliationSourceCaptureV35CReport(options = {}) {
  const brandArg = nz(options.brands || "design-hotels,small-luxury-hotels-of-the-world");
  const slugs = brandArg.split(",").map((s) => s.trim()).filter(Boolean);
  const apply = Boolean(options.apply);

  const applyBlockers = [];
  if (apply && !options.approveBatch) applyBlockers.push(`missing_${APPLY_FLAG_APPROVE}`);
  if (apply && !options.sourceOnly) applyBlockers.push(`missing_${APPLY_FLAG_SOURCE_ONLY}`);
  if (apply && !options.noValidationClaim) applyBlockers.push(`missing_${APPLY_FLAG_NO_VALIDATION}`);
  if (apply && !options.noPresentation) applyBlockers.push(`missing_${APPLY_FLAG_NO_PRESENTATION}`);
  if (apply && !options.noRegistry) applyBlockers.push(`missing_${APPLY_FLAG_NO_REGISTRY}`);
  if (apply && !options.noImageFields) applyBlockers.push(`missing_${APPLY_FLAG_NO_IMAGE_FIELDS}`);
  if (apply && !options.noActiveApproval) applyBlockers.push(`missing_${APPLY_FLAG_NO_ACTIVE_APPROVAL}`);
  if (apply && !options.designSlhOnly) applyBlockers.push(`missing_${APPLY_FLAG_DESIGN_SLH_ONLY}`);

  const invalidSlugs = slugs.filter((s) => !TARGET_BRANDS.some((b) => b.slug === s));
  if (invalidSlugs.length) applyBlockers.push(`invalid_brand_slug:${invalidSlugs.join(",")}`);
  if (slugs.includes(TRIBUTE_BENCHMARK.slug)) applyBlockers.push("tribute_benchmark_read_only");

  const brands = TARGET_BRANDS.filter((b) => slugs.includes(b.slug));
  const brandResults = [];
  for (const brand of brands) {
    brandResults.push(await processBrand(brand, options));
  }

  for (const result of brandResults) {
    for (const create of result.proposedCreates) {
      const notes = nz(create.fields?.[MAP_PARTNER_SOURCE.notes]);
      if (/company validated|company-approved/i.test(notes)) {
        applyBlockers.push(`company_validation_language:${result.slug}:${create.role}`);
      }
      if (/franchise flag|fdd|item 19/i.test(notes) && result.slug === "design-hotels") {
        applyBlockers.push(`franchise_language_blocked:${result.slug}:${create.role}`);
      }
      if (/parent brand|franchise flag/i.test(notes) && result.slug === "small-luxury-hotels-of-the-world") {
        applyBlockers.push(`consortium_language_blocked:${result.slug}:${create.role}`);
      }
    }
  }

  const allCreates = brandResults.flatMap((b) => b.proposedCreates);
  const allUpdates = brandResults.flatMap((b) => b.proposedUpdates);
  const canApply = apply && applyBlockers.length === 0 && (allCreates.length > 0 || allUpdates.length > 0);

  let applyResults = { created: [], updated: [], errors: [] };
  let airtableModified = false;
  const companyValidatedSnapshots = [];

  if (canApply) {
    for (const result of brandResults) {
      const before = result.companyValidatedBefore;
      for (const create of result.proposedCreates) {
        try {
          const created = await createPartnerSource(create.fields);
          applyResults.created.push({
            slug: result.slug,
            recordId: created.id,
            role: create.role,
            sourceUrl: create.sourceUrl,
          });
          airtableModified = true;
          await new Promise((r) => setTimeout(r, 220));
        } catch (err) {
          applyResults.errors.push({ slug: result.slug, role: create.role, message: err.message });
        }
      }
      for (const update of result.proposedUpdates) {
        try {
          const patched = await patchPartnerSource(update.recordId, update.fields);
          applyResults.updated.push({
            slug: result.slug,
            recordId: patched.id,
            role: update.role,
          });
          airtableModified = true;
          await new Promise((r) => setTimeout(r, 220));
        } catch (err) {
          applyResults.errors.push({ slug: result.slug, role: update.role, message: err.message });
        }
      }
      const afterBasics = await fetchBrandBasics(result.recordId);
      const after = companyValidatedSnapshot(afterBasics);
      companyValidatedSnapshots.push({
        slug: result.slug,
        before,
        after,
        unchanged:
          before.companyValidated === after.companyValidated &&
          before.companyValidationDate === after.companyValidationDate,
      });
      if (!companyValidatedSnapshots.at(-1).unchanged) {
        applyBlockers.push(`company_validated_changed:${result.slug}`);
      }
    }
  } else {
    for (const result of brandResults) {
      companyValidatedSnapshots.push({
        slug: result.slug,
        before: result.companyValidatedBefore,
        after: result.companyValidatedBefore,
        unchanged: true,
      });
    }
  }

  const markdown = [
    `# Brand Explorer Lifestyle / Affiliation Source Capture ${V35C_VERSION}`,
    "",
    `Generated: ${new Date().toISOString()}`,
    `Mode: ${apply ? (canApply ? "apply" : "apply-blocked") : "dry-run"}`,
    "",
    "## Guardrails",
    "- Source Library rows only on apply",
    "- No presentation / registry / image-field / Company Validated changes",
    "- Tribute Portfolio read-only benchmark (not modified)",
    "",
    ...brandResults.flatMap((b) => [buildBrandMarkdown(b), ""]),
    "## Apply",
    applyBlockers.length ? `Blockers: ${applyBlockers.join("; ")}` : "No apply blockers.",
    "",
    "```bash",
    buildApplyCommand(slugs),
    "```",
  ].join("\n");

  return {
    version: V35C_VERSION,
    generatedAt: new Date().toISOString(),
    mode: apply ? (canApply ? "apply" : "apply-blocked") : "dry-run",
    guardrails: {
      sourceLibraryOnly: true,
      noPresentationChanges: true,
      noRegistryChanges: true,
      noImageFieldChanges: true,
      noCompanyValidatedChanges: true,
      noActiveProfileApproval: true,
      tributeReadOnly: true,
    },
    brands: brandResults,
    tributeBenchmark: TRIBUTE_BENCHMARK,
    applyBlockers,
    canApply,
    airtableModified,
    applyResults,
    companyValidatedSnapshots,
    companyValidatedUntouched: companyValidatedSnapshots.every((s) => s.unchanged),
    exactApplyCommand: buildApplyCommand(slugs),
    markdown,
  };
}
