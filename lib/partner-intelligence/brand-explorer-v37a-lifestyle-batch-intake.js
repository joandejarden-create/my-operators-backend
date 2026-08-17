/**
 * Brand Explorer v37A — Lifestyle Batch Intake (read-only).
 *
 * Batch orchestrator for new lifestyle/collection brands using v36 contracts.
 * No Airtable writes. No owner-facing presentation rows created.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import { listRegistryAssetsForBrand } from "./brand-asset-registry-workflow.js";
import { resolveBrandTarget, AmbiguousBrandResolutionError } from "./brand-explorer-brand-target-resolver.js";
import { getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";
import { loadBrandFactoryContext } from "./brand-explorer-active-profile-factory.js";
import { fetchAndResolveApprovedBrandSources, resolveApprovedBrandSources } from "./brand-source-auto-resolver.js";
import { buildBrandKnowledgePack } from "./brand-explorer-brand-knowledge-pack.js";
import {
  auditPresentationRowsAgainstContract,
  classifySlotKey,
} from "./brand-explorer-full-tab-content-contract.js";
import { extractOgImageFromHtml } from "./brand-explorer-radisson-individuals-durable-gallery-source-repair-writer.js";
import { validateBrandV36BContracts } from "./brand-explorer-v36b-contract-validation.js";
import { auditPresentationRowExternalOwner } from "./brand-explorer-external-owner-content-governance.js";

const TAB_READINESS_DEFS = Object.freeze([
  { name: "Overview", prefixes: ["overview.", "hero."] },
  { name: "Value to Owners", prefixes: ["valueOwners."] },
  { name: "Operating Model", prefixes: ["operations."] },
  { name: "Owner Considerations", prefixes: ["standards."] },
  { name: "Commercial Engine", prefixes: ["commercial."] },
  { name: "Economics & Obligations", prefixes: ["economics."] },
  { name: "Loyalty Program", prefixes: ["loyalty."] },
  { name: "Footprint & Growth", prefixes: ["footprint."] },
  { name: "Brand Materials", prefixes: ["materials."] },
  { name: "Dealality Insight", prefixes: ["insight.", "dealalityInsight."] },
]);

export const V37A_VERSION = "v37A";
export const REPORT_JSON = "brand-explorer-v37a-lifestyle-batch-intake.json";
export const REPORT_MD = "brand-explorer-v37a-lifestyle-batch-intake.md";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

const LIFESTYLE_GALLERY_LABELS = Object.freeze([
  "Exterior / Arrival",
  "Guest Room / Suite",
  "Public Space",
  "F&B or Local Experience",
  "Design Detail",
  "Property Setting",
]);

/** Exact-match intake targets — no fuzzy sibling-brand resolution. */
export const V37A_BRAND_TARGETS = Object.freeze([
  {
    slug: "hotel-indigo",
    exactNames: ["Hotel Indigo"],
    rejectNamePatterns: [
      /^intercontinental hotels/i,
      /^ihg hotels & resorts$/i,
      /^holiday inn/i,
      /^crowne plaza/i,
    ],
    workingParentHypothesis: "IHG Hotels & Resorts",
    officialSources: [
      {
        role: "consumer_brand_page",
        sourceTitle: "Hotel Indigo — official consumer brand site",
        sourceUrl: "https://www.ihg.com/hotelindigo/hotels/us/en/reservation",
        sourceType: "Brand Website",
        confidence: "high",
        intendedUse: "Brand positioning, guest promise, design narrative",
      },
      {
        role: "development_owner_page",
        sourceTitle: "IHG Development — Hotel Indigo brand page",
        sourceUrl: "https://development.ihg.com/brand/hotel-indigo",
        sourceType: "Development Brochure",
        confidence: "high",
        intendedUse: "Owner/development model, conversion context, brand standards overview",
      },
      {
        role: "parent_brand_family",
        sourceTitle: "IHG — brands and experiences overview",
        sourceUrl: "https://www.ihg.com/content/us/en/about/brands",
        sourceType: "Corporate Overview",
        confidence: "high",
        intendedUse: "Parent company official naming, portfolio ladder, IHG One Rewards context",
      },
      {
        role: "loyalty_distribution",
        sourceTitle: "IHG One Rewards — official loyalty program",
        sourceUrl: "https://www.ihg.com/onerewards/content/us/en/home",
        sourceType: "Loyalty Program",
        confidence: "high",
        intendedUse: "Distribution and loyalty mechanics for owner diligence",
      },
      {
        role: "hotel_directory",
        sourceTitle: "Hotel Indigo — hotel finder / directory",
        sourceUrl: "https://www.ihg.com/hotelindigo/hotels/us/en/reservation",
        sourceType: "Hotel Directory",
        confidence: "medium",
        intendedUse: "Property examples, footprint, CALA/U.S./global expansion screening",
      },
    ],
    propertyExampleCandidates: [
      {
        propertyName: "Hotel Indigo Guanajuato",
        propertyMarket: "Guanajuato",
        propertyRegion: "Mexico · CALA",
        sourcePageUrl: "https://www.ihg.com/hotelindigo/hotels/us/en/guanajuato/guan/hoteldetail",
        calaPriority: 1,
      },
      {
        propertyName: "Hotel Indigo Guadalajara Expo",
        propertyMarket: "Guadalajara",
        propertyRegion: "Mexico · CALA",
        sourcePageUrl: "https://www.ihg.com/hotelindigo/hotels/us/en/guadalajara/gdl/hoteldetail",
        calaPriority: 1,
      },
      {
        propertyName: "Hotel Indigo Lima Miraflores",
        propertyMarket: "Lima",
        propertyRegion: "Peru · CALA",
        sourcePageUrl: "https://www.ihg.com/hotelindigo/hotels/us/en/lima/lim/hoteldetail",
        calaPriority: 1,
      },
      {
        propertyName: "Hotel Indigo Nashville - The Countrypolitan",
        propertyMarket: "Nashville",
        propertyRegion: "United States",
        sourcePageUrl: "https://www.ihg.com/hotelindigo/hotels/us/en/nashville/bna/hoteldetail",
        calaPriority: 2,
      },
    ],
    modelHypothesis: "lifestyle_full_brand",
  },
  {
    slug: "mgallery-collection",
    exactNames: ["MGallery Collection", "MGallery"],
    rejectNamePatterns: [/^sofitel/i, /^pullman/i, /^handwritten collection/i, /^novotel/i],
    workingParentHypothesis: "Accor",
    officialSources: [
      {
        role: "consumer_brand_page",
        sourceTitle: "MGallery Collection — official consumer brand site",
        sourceUrl: "https://mgallery.accor.com/",
        sourceType: "Brand Website",
        confidence: "high",
        intendedUse: "Collection positioning, guest experience, design narrative",
      },
      {
        role: "development_owner_page",
        sourceTitle: "Accor Group — MGallery brand and experiences",
        sourceUrl: "https://group.accor.com/en/brands-and-experiences/mgallery",
        sourceType: "Corporate Overview",
        confidence: "high",
        intendedUse: "Owner-facing collection model, parent official naming",
      },
      {
        role: "parent_brand_family",
        sourceTitle: "Accor Group — brands and experiences hub",
        sourceUrl: "https://group.accor.com/en/brands-and-experiences",
        sourceType: "Corporate Overview",
        confidence: "high",
        intendedUse: "Accor portfolio context, ALL loyalty framing",
      },
      {
        role: "loyalty_distribution",
        sourceTitle: "ALL — Accor loyalty program",
        sourceUrl: "https://all.accor.com/loyalty-program/home/index.en.shtml",
        sourceType: "Loyalty Program",
        confidence: "high",
        intendedUse: "Distribution and loyalty participation for owner diligence",
      },
      {
        role: "hotel_directory",
        sourceTitle: "MGallery Collection — hotel directory",
        sourceUrl: "https://mgallery.accor.com/en/hotels.html",
        sourceType: "Hotel Directory",
        confidence: "high",
        intendedUse: "Property examples and footprint screening",
      },
    ],
    propertyExampleCandidates: [
      {
        propertyName: "Palladio Hotel Buenos Aires MGallery Collection",
        propertyMarket: "Buenos Aires",
        propertyRegion: "Argentina · CALA",
        sourcePageUrl: "https://all.accor.com/hotel/B1R7/index.en.shtml",
        calaPriority: 1,
      },
      {
        propertyName: "Hotel Costanero Montevideo - MGallery Collection",
        propertyMarket: "Montevideo",
        propertyRegion: "Uruguay · CALA",
        sourcePageUrl: "https://all.accor.com/hotel/B3G3/index.en.shtml",
        calaPriority: 1,
      },
      {
        propertyName: "Santa Teresa Hotel RJ - MGallery Collection",
        propertyMarket: "Rio de Janeiro",
        propertyRegion: "Brazil · CALA",
        sourcePageUrl: "https://all.accor.com/hotel/A1X5/index.en.shtml",
        calaPriority: 1,
      },
      {
        propertyName: "Armony Resort future MGallery",
        propertyMarket: "Comporta",
        propertyRegion: "Portugal · Global",
        sourcePageUrl: "https://all.accor.com/hotel/C2A2/index.en.shtml",
        calaPriority: 3,
        note: "Pipeline/global expansion reference — verify operating status on capture",
      },
    ],
    modelHypothesis: "soft_brand_collection",
  },
]);

const PLAN_SLOT_KEYS = Object.freeze([
  "overview.scenario.1",
  "overview.scenario.2",
  "overview.scenario.3",
  "overview.portfolio_context",
  "overview.why_value",
  "valueOwners.scenario.1",
  "operations.model.summary",
  "standards.requirement",
  "commercial.theme.1",
  "economics.fee.join",
  "loyalty.hero_title",
  "loyalty.owner_lens",
  "footprint.region.cala",
  "footprint.openings",
  "footprint.momentum",
  "materials.gallery.1",
  "materials.gallery.2",
  "materials.gallery.3",
  "materials.gallery.4",
  "materials.gallery.5",
  "materials.gallery.6",
  "insight.summary",
  "insight.similar.1",
  "insight.similar.2",
  "insight.similar.3",
]);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function fileKeyForSlug(slug) {
  return slug === "small-luxury-hotels-of-the-world" ? "slh" : slug;
}

function normalizeName(name) {
  return nz(name).toLowerCase().replace(/\s+/g, " ");
}

function isRejectedBrandName(name, target) {
  return (target.rejectNamePatterns || []).some((re) => re.test(nz(name)));
}

async function fetchHtml(url, timeoutMs = 12000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      signal: controller.signal,
      headers: {
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        Accept: "text/html,application/xhtml+xml",
      },
      redirect: "follow",
    });
    if (!res.ok) return { ok: false, status: res.status, html: "" };
    const html = await res.text();
    return { ok: true, status: res.status, html };
  } catch (err) {
    return { ok: false, status: 0, html: "", error: err.message };
  } finally {
    clearTimeout(timer);
  }
}

async function probePropertyImage(sourcePageUrl) {
  const fetchResult = await fetchHtml(sourcePageUrl);
  if (!fetchResult.ok) {
    return {
      imageUrl: "",
      renderReadinessProjection: "blocked_fetch_failed",
      registryReadinessProjection: "source_page_unreachable",
      probeError: fetchResult.error || `http_${fetchResult.status}`,
    };
  }
  const imageUrl = extractOgImageFromHtml(fetchResult.html) || "";
  const isLogo = /logo|icon|favicon|sprite/i.test(imageUrl);
  return {
    imageUrl: isLogo ? "" : imageUrl,
    renderReadinessProjection: imageUrl && !isLogo ? "imageUrl_candidate_ready" : "needs_manual_image_selection",
    registryReadinessProjection: imageUrl ? "registry_candidate_after_review" : "blocked_no_image",
    probeSource: imageUrl ? "og:image" : "none",
  };
}

function classifyImageCandidate({ imageUrl, propertyName, brandName, sourcePageUrl }) {
  const u = nz(imageUrl).toLowerCase();
  const isLogo = !u || /logo|icon|brandmark|sprite|favicon/.test(u);
  const isGenericGraphic = /placeholder|hero-banner|brand-platform|prototype-render/.test(u);
  const isLifestyleOnly = !propertyName && /lifestyle|stock|getty|shutterstock/.test(u);
  const wrongBrandRisk =
    /marriott|hilton|hyatt|choicehotels|wyndham/.test(u) ||
    (brandName && /accor|mgallery/.test(normalizeName(brandName)) && /ihg|hotelindigo/.test(u)) ||
    (brandName && /indigo|ihg/.test(normalizeName(brandName)) && /accor|mgallery/.test(u));
  const isHotelOrPropertyPhotography = Boolean(u) && !isLogo && !isGenericGraphic && !isLifestyleOnly;
  let imageClassification = "unknown";
  if (isLogo) imageClassification = "logo";
  else if (isGenericGraphic) imageClassification = "generic_graphic";
  else if (isLifestyleOnly) imageClassification = "lifestyle_only";
  else if (isHotelOrPropertyPhotography) imageClassification = "hotel_property_photography";
  return {
    imageClassification,
    isHotelOrPropertyPhotography,
    isLogo,
    isLifestyleOnly,
    isGenericGraphic,
    isWrongBrandRisk: wrongBrandRisk,
    sourcePageUrl,
  };
}

function selectPropertyExamples(candidates, minimum = 3) {
  const sorted = [...candidates].sort((a, b) => (a.calaPriority || 9) - (b.calaPriority || 9));
  const cala = sorted.filter((c) => (c.calaPriority || 9) === 1);
  const us = sorted.filter((c) => (c.calaPriority || 9) === 2);
  const global = sorted.filter((c) => (c.calaPriority || 9) >= 3);
  let selected = cala.slice(0, minimum);
  let sectionLabel = "Curated CALA examples · Not a full directory";
  if (selected.length < minimum) {
    selected = [...selected, ...us].slice(0, minimum);
    sectionLabel = "Curated CALA + U.S. examples · Not a full directory";
  }
  if (selected.length < minimum) {
    selected = [...selected, ...global].slice(0, minimum);
    sectionLabel = "Curated global examples · Not a full directory";
  }
  return { selected, sectionLabel, calaCount: cala.length, usCount: us.length, globalCount: global.length };
}

async function buildVisualAssetPack(target, brandName) {
  const { selected, sectionLabel, calaCount } = selectPropertyExamples(target.propertyExampleCandidates, 3);
  const propertyExamples = [];
  for (let i = 0; i < selected.length; i++) {
    const candidate = selected[i];
    const probe = await probePropertyImage(candidate.sourcePageUrl);
    const classification = classifyImageCandidate({
      imageUrl: probe.imageUrl,
      propertyName: candidate.propertyName,
      brandName,
      sourcePageUrl: candidate.sourcePageUrl,
    });
    propertyExamples.push({
      intendedSlot: "footprint.openings",
      propertyName: candidate.propertyName,
      propertyMarket: candidate.propertyMarket,
      propertyRegion: candidate.propertyRegion,
      sourcePageUrl: candidate.sourcePageUrl,
      imageUrl: probe.imageUrl,
      ...classification,
      renderReadinessProjection: probe.renderReadinessProjection,
      registryReadinessProjection: probe.registryReadinessProjection,
      note: candidate.note || "",
    });
    await new Promise((r) => setTimeout(r, 150));
  }

  const gallery = [];
  const galleryPool = [...propertyExamples, ...selected.map((c) => ({ ...c, imageUrl: "" }))];
  for (let i = 0; i < 6; i++) {
    const poolItem = galleryPool[i % galleryPool.length];
    let imageUrl = poolItem.imageUrl || "";
    if (!imageUrl && poolItem.sourcePageUrl) {
      const probe = await probePropertyImage(poolItem.sourcePageUrl);
      imageUrl = probe.imageUrl;
      await new Promise((r) => setTimeout(r, 120));
    }
    const classification = classifyImageCandidate({
      imageUrl,
      propertyName: poolItem.propertyName,
      brandName,
      sourcePageUrl: poolItem.sourcePageUrl,
    });
    gallery.push({
      intendedSlot: `materials.gallery.${i + 1}`,
      intendedGalleryLabel: LIFESTYLE_GALLERY_LABELS[i],
      propertyName: poolItem.propertyName,
      propertyMarket: poolItem.propertyMarket,
      propertyRegion: poolItem.propertyRegion,
      sourcePageUrl: poolItem.sourcePageUrl,
      imageUrl,
      ...classification,
      renderReadinessProjection: imageUrl ? "imageUrl_candidate_ready" : "needs_manual_image_selection",
      registryReadinessProjection: imageUrl ? "registry_candidate_after_review" : "blocked_no_image",
    });
  }

  const scenarios = propertyExamples.slice(0, 3).map((p, i) => ({
    intendedSlot: `overview.scenario.${i + 1}`,
    propertyName: p.propertyName,
    sourcePageUrl: p.sourcePageUrl,
    imageUrl: p.imageUrl,
    imageClassification: p.imageClassification,
    isHotelOrPropertyPhotography: p.isHotelOrPropertyPhotography,
    renderReadinessProjection: p.renderReadinessProjection,
    registryReadinessProjection: p.registryReadinessProjection,
  }));

  const galleryReady = gallery.filter((g) => g.imageUrl && g.isHotelOrPropertyPhotography).length;
  const propertyReady = propertyExamples.filter((p) => p.imageUrl && p.isHotelOrPropertyPhotography).length;
  const scenarioReady = scenarios.filter((s) => s.imageUrl).length;

  return {
    version: V37A_VERSION,
    brandSlug: target.slug,
    propertyExampleSectionLabel: sectionLabel,
    calaCandidateCount: calaCount,
    gallery,
    propertyExamples,
    scenarios,
    summary: {
      galleryCandidates: gallery.length,
      galleryImageUrlReady: galleryReady,
      propertyExampleCandidates: propertyExamples.length,
      propertyExamplesImageUrlReady: propertyReady,
      scenarioCandidates: scenarios.length,
      scenariosImageUrlReady: scenarioReady,
      renderReadyProjection: galleryReady >= 6 && propertyReady >= 3 && scenarioReady >= 3,
      registryReadyProjection: gallery.every((g) => g.registryReadinessProjection !== "blocked_no_image"),
    },
  };
}

function buildSourceLibraryPlans(target, discovery) {
  return target.officialSources.map((source) => ({
    brandSlug: target.slug,
    brandRecordId: discovery.brandRecordId || null,
    sourceTitle: source.sourceTitle,
    sourceUrl: source.sourceUrl,
    sourceType: source.sourceType,
    confidence: source.confidence,
    intendedUse: source.intendedUse,
    publicSourceYes: true,
    companyValidatedNo: true,
    approvedForExplorerUseProposal: source.confidence === "high" ? "Yes" : "No",
    notes: `v37A intake — ${source.role}. Official public source plan only; not company-validated.`,
    role: source.role,
  }));
}

function classifyBrandModel(target, sourcePlans, brandBasicsFields = {}) {
  const parentFromRecord = nz(brandBasicsFields["Parent Company"] || brandBasicsFields.parentCompany);
  const devUrl = sourcePlans.find((s) => s.role === "development_owner_page")?.sourceUrl || "";
  const consumerUrl = sourcePlans.find((s) => s.role === "consumer_brand_page")?.sourceUrl || "";

  let selected = target.modelHypothesis;
  let confidence = "medium";
  const evidence = [];

  if (/development\.ihg\.com\/brand\//i.test(devUrl)) {
    selected = "lifestyle_full_brand";
    confidence = "high";
    evidence.push(devUrl);
  }
  if (/group\.accor\.com.*mgallery/i.test(devUrl) || /mgallery\.accor\.com/i.test(consumerUrl)) {
    selected = "soft_brand_collection";
    confidence = "high";
    evidence.push(devUrl || consumerUrl);
  }

  const languageRules = [];
  const disallowedLanguage = ["Company Validated", "Sources:", "Item 19", "FDD", "LOI"];
  const ownerDiligenceImplications = [];

  if (selected === "lifestyle_full_brand") {
    languageRules.push(
      "Frame as IHG lifestyle full brand — neighborhood boutique positioning within IHG One Rewards",
      "Use development.ihg.com owner context for conversion/new-build fit",
      "Avoid generic boutique copy unrelated to IHG systems"
    );
    disallowedLanguage.push("affiliation curation platform", "independent consortium");
    ownerDiligenceImplications.push(
      "Confirm franchise/development agreement scope, PIP, and IHG One Rewards participation",
      "Compare fee stack and operating model vs voco, Kimpton, and Crowne Plaza tiers"
    );
  } else if (selected === "soft_brand_collection") {
    languageRules.push(
      "Frame as Accor soft collection — distinctive independent hotels with Accor commercial platform",
      "Preserve local identity language; avoid generic Accor boilerplate",
      "Use ALL loyalty context without unsupported performance claims"
    );
    disallowedLanguage.push("franchise prototype rinse-and-repeat", "standardized limited-service");
    ownerDiligenceImplications.push(
      "Confirm collection participation standards and conversion PIP scope",
      "Model ALL contribution versus independent operating baseline"
    );
  }

  return {
    selectedBrandModelType: selected,
    confidence,
    evidenceSources: evidence,
    parentCompanyOfficialName: parentFromRecord || target.workingParentHypothesis,
    parentNamingStatus: parentFromRecord ? "confirmed_from_brand_basics" : "hypothesis_pending_record_or_source",
    languageRules,
    disallowedLanguage,
    ownerDiligenceImplications,
  };
}

async function discoverBrandRecord(target) {
  const attempts = [];
  for (const exactName of target.exactNames) {
    try {
      const resolved = await resolveBrandTarget(exactName);
      const name = nz(resolved.name);
      if (isRejectedBrandName(name, target)) {
        attempts.push({
          input: exactName,
          status: "rejected_wrong_brand",
          resolvedName: name,
          recordId: resolved.recordId,
        });
        continue;
      }
      const exactMatch = target.exactNames.some((n) => normalizeName(n) === normalizeName(name));
      if (!exactMatch && !normalizeName(name).includes(normalizeName(target.exactNames[0]))) {
        attempts.push({
          input: exactName,
          status: "rejected_inexact_match",
          resolvedName: name,
          recordId: resolved.recordId,
        });
        continue;
      }
      return {
        found: true,
        brandRecordId: resolved.recordId,
        slug: resolved.slug || target.slug,
        brandName: name,
        parentCompany: resolved.resolution?.parentCompany || null,
        resolutionSource: resolved.resolution?.resolutionSource || "live_lookup",
        attempts,
      };
    } catch (err) {
      if (err instanceof AmbiguousBrandResolutionError) {
        attempts.push({
          input: exactName,
          status: "ambiguous",
          candidates: err.candidates?.map((c) => ({ name: c.name, recordId: c.recordId })),
        });
      } else {
        attempts.push({ input: exactName, status: "error", error: err.message });
      }
    }
  }
  return {
    found: false,
    brandRecordId: null,
    slug: target.slug,
    brandName: target.exactNames[0],
    blockedBy: "blocked_by_brand_record_missing",
    recommendedSetupFields: [
      "Brand Name (exact official brand name)",
      "Parent Company (from official parent page — do not assume)",
      "Brand Website",
      "Brand Architecture / Brand Family",
      "Hotel Chain Scale",
      "Brand Model Format",
      "Company Validated = No until human validation",
    ],
    attempts,
  };
}

async function loadLiveBrandState(discovery, target) {
  if (!discovery.found || !discovery.brandRecordId) {
    return {
      brandBasics: null,
      presentationRows: [],
      registryAssets: [],
      approvedSources: [],
      brandApi: null,
      factoryContext: null,
    };
  }
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  const [brandBasics, registryAssets, sourceResolution] = await Promise.all([
    fetchBrandBasics(discovery.brandRecordId).catch(() => null),
    listRegistryAssetsForBrand(discovery.brandRecordId).catch(() => []),
    fetchAndResolveApprovedBrandSources({ recordId: discovery.brandRecordId }).catch(() => ({ sources: [] })),
  ]);

  const brandConfig = getActiveProfileBrandConfig(target.slug);
  const approvedSources = resolveApprovedBrandSources(sourceResolution.sources || [], {
    recordId: discovery.brandRecordId,
    companyDomains: brandConfig?.officialSourceDomains || [],
  });

  let factoryContext = null;
  let presentationRows = [];
  let brandApi = null;
  if (brandConfig) {
    try {
      factoryContext = await loadBrandFactoryContext(target.slug);
      presentationRows = factoryContext.presentationRows || [];
      brandApi = factoryContext.brandApi || null;
    } catch (err) {
      factoryContext = { loadError: err.message };
    }
  }

  return {
    brandBasics,
    presentationRows,
    registryAssets,
    approvedSources,
    brandApi,
    factoryContext,
    brandConfig,
  };
}

function buildV37AKnowledgePack({
  target,
  discovery,
  modelClassification,
  sourcePlans,
  visualAssetPack,
  liveState,
  tabReadiness,
}) {
  const brandConfig = liveState.brandConfig;
  const brandName = discovery.brandName;
  const fields = liveState.brandBasics?.fields || {};

  let basePack = null;
  if (brandConfig && liveState.presentationRows) {
    try {
      basePack = buildBrandKnowledgePack({
        brandSlug: target.slug,
        brandConfig,
        presentationRows: liveState.presentationRows,
        brandApi: liveState.brandApi,
        approvedSources: liveState.approvedSources,
      });
    } catch {
      basePack = null;
    }
  }

  const unsupportedClaims = [];
  if (!discovery.found) unsupportedClaims.push("Brand Setup record missing — no live presentation audit");
  if ((liveState.approvedSources || []).length < 3) {
    unsupportedClaims.push("Fewer than 3 approved Source Library rows linked to brand");
  }
  if (!visualAssetPack.summary.renderReadyProjection) {
    unsupportedClaims.push("Visual asset pack not render-ready — gallery/property/scenario images incomplete");
  }

  return {
    knowledgePackVersion: V37A_VERSION,
    generatedAt: new Date().toISOString(),
    brandName,
    slug: target.slug,
    brandRecordId: discovery.brandRecordId,
    parentCompanyOfficialName: modelClassification.parentCompanyOfficialName,
    brandModelType: modelClassification.selectedBrandModelType,
    positioning: basePack?.positioning || `Official ${brandName} consumer + development sources required before owner copy`,
    ownerFit: basePack?.ownerFit || modelClassification.ownerDiligenceImplications.join(" "),
    assetFit: basePack?.assetFit || "Distinctive urban/lifestyle assets with design narrative and operating complexity",
    conversionOrNewBuildFit:
      modelClassification.selectedBrandModelType === "soft_brand_collection"
        ? "Conversion-weighted collection path — confirm PIP and identity preservation"
        : "IHG development path — confirm neighborhood boutique fit and IHG systems scope",
    operatingModel: basePack?.operatingModel || "Major-brand-system lifestyle operating model — source confirmation required",
    commercialModel: basePack?.commercialModel || "Parent-platform distribution + loyalty — no unsupported fee claims",
    distributionContext:
      modelClassification.selectedBrandModelType === "soft_brand_collection"
        ? "Accor commercial platform + ALL"
        : "IHG commercial platform + IHG One Rewards",
    loyaltyContext:
      modelClassification.selectedBrandModelType === "soft_brand_collection" ? "ALL loyalty program" : "IHG One Rewards",
    standardsOrParticipationContext:
      "Collection/participation standards from official development or brand pages — not yet materialized in Presentation",
    economicsAndObligationsContext:
      "Economics section deferred until owner-safe sources captured — no FDD/Item 19 in v37A intake",
    propertyExampleStrategy: visualAssetPack.propertyExampleSectionLabel,
    galleryStrategy: "Six official property photography URLs — space label + hotel name caption format",
    scenarioStrategy: "Three scenario cards mapped to owner-fit archetypes after property examples validated",
    footprintContext: "CALA-first property examples; expand U.S./global only when CALA count < 3",
    CALARelevance: `${visualAssetPack.calaCandidateCount} CALA property candidates identified in intake plan`,
    knownUnknowns: [
      ...(basePack?.knownUnknowns || []),
      modelClassification.parentNamingStatus === "hypothesis_pending_record_or_source"
        ? "Parent company official external naming not yet confirmed from Brand Basics"
        : null,
      "Owner-facing presentation rows intentionally not created in v37A",
      "Gallery image match requires founder visual review before registry approval",
    ].filter(Boolean),
    unsupportedClaims,
    sourceConfidenceBySection: {
      overview: sourcePlans.some((s) => s.role === "consumer_brand_page") ? "high" : "low",
      loyalty: sourcePlans.some((s) => s.role === "loyalty_distribution") ? "high" : "low",
      footprint: visualAssetPack.summary.propertyExamplesImageUrlReady >= 1 ? "medium" : "low",
      economics: "blocked_until_development_source_review",
      standards: "blocked_until_development_source_review",
    },
    founderReviewItems: [
      "Confirm brand model classification before copy generation",
      "Validate CALA property example selection and image match",
      "Confirm parent company official naming on Brand Setup",
      "Review development/franchise vs collection language before economics tab",
    ],
    externalOwnerReadinessRisks: [
      discovery.found ? null : "blocked_by_brand_record_missing",
      (liveState.approvedSources || []).length < 3 ? "blocked_by_sources" : null,
      !visualAssetPack.summary.renderReadyProjection ? "blocked_by_images" : null,
      "copy_not_started_v37a_intake_only",
    ].filter(Boolean),
    tabReadinessSummary: tabReadiness,
    sourcePlanCount: sourcePlans.length,
    modelClassification,
  };
}

function evaluateTabContractReadiness(presentationRows, brandApi, sourcePlans, visualAssetPack) {
  const audit = auditPresentationRowsAgainstContract(
    presentationRows,
    brandApi?.brandExplorer?.blocks || []
  );
  const visible = presentationRows.filter((r) => r.visible !== false);

  return TAB_READINESS_DEFS.map(({ name, prefixes }) => {
    const tabRows = visible.filter((r) => prefixes.some((p) => r.slotKey.startsWith(p)));
    const populated = tabRows.filter((r) => nz(r.title) || nz(r.body)).length;
    const withImages = tabRows.filter((r) => nz(r.imageUrl)).length;
    const sourceCoverage =
      sourcePlans.length >= 5 ? "official_sources_planned" : sourcePlans.length >= 3 ? "partial" : "insufficient";
    let imageCoverage = "n/a";
    if (name === "Brand Materials") {
      imageCoverage = `${visualAssetPack.summary.galleryImageUrlReady}/6 gallery candidates`;
    } else if (name === "Footprint & Growth") {
      imageCoverage = `${visualAssetPack.summary.propertyExamplesImageUrlReady}/3 property examples`;
    } else if (name === "Overview") {
      imageCoverage = `${visualAssetPack.summary.scenariosImageUrlReady}/3 scenario candidates`;
    }
    return {
      tab: name,
      requiredSlots: prefixes,
      contractAuditVisibleRows: audit.visibleRowCount,
      sourceCoverage,
      contentCoverage: tabRows.length ? `${populated}/${tabRows.length} rows populated` : "no_presentation_rows",
      imageCoverage,
      modelFitRisks: name === "Economics & Obligations" ? ["defer_fee_claims_until_sources"] : [],
      ownerFacingRisks: populated === 0 ? ["tab_would_render_empty_or_fallback"] : [],
      unsupportedClaims: [],
      draftBuildAllowedLater:
        sourcePlans.length >= 5 &&
        visualAssetPack.summary.galleryImageUrlReady >= 4 &&
        visualAssetPack.summary.propertyExamplesImageUrlReady >= 2,
    };
  });
}

function buildPresentationPlanRows({
  target,
  discovery,
  modelClassification,
  sourcePlans,
  visualAssetPack,
  liveState,
}) {
  const brandConfig = liveState.brandConfig || {};
  const disallowedTerms = brandConfig.disallowedCopyTerms || modelClassification.disallowedLanguage;

  return PLAN_SLOT_KEYS.map((slotKey, index) => {
    const tab = classifySlotKey(slotKey).tab || "Unknown";
    const section = slotKey.split(".").slice(0, 2).join(".") || slotKey;
    const isGallery = /^materials\.gallery\./.test(slotKey);
    const isOpening = slotKey === "footprint.openings";
    const isScenario = /^overview\.scenario\./.test(slotKey);

    let title = `[PLAN] ${slotKey}`;
    let externalBody =
      "Internal v37A intake plan row — owner-facing copy intentionally deferred until source capture and founder review complete.";
    let visualAssetIds = [];
    let founderReviewRequired = true;
    let reasonForFounderReview = "v37A intake — no owner copy yet";

    if (isGallery) {
      const gi = parseInt(slotKey.split(".").pop(), 10) - 1;
      const g = visualAssetPack.gallery[gi];
      if (g) {
        title = g.intendedGalleryLabel;
        externalBody = g.propertyName
          ? `${g.intendedGalleryLabel} - ${g.propertyName}`
          : g.intendedGalleryLabel;
        visualAssetIds = [g.sourcePageUrl].filter(Boolean);
        reasonForFounderReview = g.imageUrl ? "confirm_gallery_image_match" : "gallery_image_missing";
      }
    } else if (isOpening) {
      externalBody = visualAssetPack.propertyExamples
        .map((p) => `${p.propertyName} (${p.propertyRegion})`)
        .join("\n");
      reasonForFounderReview = "confirm_property_example_selection";
    } else if (isScenario) {
      const si = parseInt(slotKey.split(".").pop(), 10) - 1;
      const s = visualAssetPack.scenarios[si];
      if (s) {
        title = `Scenario ${si + 1}`;
        externalBody = `Property-backed scenario candidate: ${s.propertyName || "TBD"}`;
        visualAssetIds = [s.sourcePageUrl].filter(Boolean);
      }
    } else if (slotKey === "insight.summary") {
      externalBody = `Dealality editorial summary deferred — classify ${modelClassification.selectedBrandModelType} first.`;
      reasonForFounderReview = "strategic_positioning";
    } else if (/^insight\.similar/.test(slotKey)) {
      externalBody = "Competitive set peer cards deferred — requires founder-approved peer list.";
      reasonForFounderReview = "strategic_positioning";
    } else if (slotKey === "loyalty.hero_title") {
      externalBody =
        modelClassification.selectedBrandModelType === "soft_brand_collection"
          ? "ALL loyalty participation context — no performance claims"
          : "IHG One Rewards participation context — no performance claims";
    }

    const disallowedTermsCheck = disallowedTerms.every(
      (term) => !new RegExp(term.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i").test(externalBody)
    );
    const sourceTraceabilityCheck = sourcePlans.length > 0;
    const renderReadinessCheck = isGallery
      ? Boolean(visualAssetPack.gallery[parseInt(slotKey.split(".").pop(), 10) - 1]?.imageUrl)
      : !isOpening && !isScenario
        ? true
        : Boolean(
            visualAssetPack.propertyExamples.some((p) => p.imageUrl) ||
              visualAssetPack.scenarios.some((s) => s.imageUrl)
          );
    const brandModelFitCheck = !/\b(fdd|item\s*19|franchise disclosure)\b/i.test(externalBody);
    const isGenericFiller = externalBody.includes("Internal v37A intake plan row");
    const isExternalOwnerReady = !isGenericFiller && disallowedTermsCheck && brandModelFitCheck && renderReadinessCheck;

    return {
      brandSlug: target.slug,
      brandRecordId: discovery.brandRecordId,
      tab,
      section,
      slotKey,
      title,
      externalBody,
      internalEvidenceRefs: sourcePlans.slice(0, 3).map((s) => s.sourceTitle),
      sourceRecordIds: (liveState.approvedSources || []).map((s) => s.id || s.sourceId).filter(Boolean),
      visualAssetIds,
      sortOrder: index,
      externalDisplayStatus: "Internal Review",
      ownerReadinessStatus: isExternalOwnerReady ? "plan_ready_pending_copy" : "blocked_intake",
      founderReviewRequired,
      reasonForFounderReview,
      disallowedTermsCheck,
      sourceTraceabilityCheck,
      renderReadinessCheck,
      brandModelFitCheck,
      isGenericFiller,
      isExternalOwnerReady,
    };
  });
}

function buildV37ABatchQueueStatus({
  discovery,
  sourcePlans,
  knowledgePack,
  visualAssetPack,
  presentationPlan,
  liveState,
  modelClassification,
}) {
  const approvedCount = (liveState.approvedSources || []).length;
  const flags = {
    brand_record_found: discovery.found === true,
    source_ready: sourcePlans.length >= 5 && approvedCount >= 3,
    knowledge_pack_ready: Boolean(knowledgePack),
    visual_asset_pack_ready: Boolean(visualAssetPack),
    render_ready_projection: visualAssetPack?.summary?.renderReadyProjection === true,
    presentation_plan_ready: (presentationPlan?.rows || []).length > 0,
    copy_ready: false,
    external_owner_ready: false,
    draft_ready: false,
    founder_review_required: true,
    blocked_by_brand_record_missing: !discovery.found,
    blocked_by_sources: approvedCount < 3 || sourcePlans.length < 5,
    blocked_by_images: !visualAssetPack?.summary?.renderReadyProjection,
    blocked_by_claims: (knowledgePack?.unsupportedClaims || []).length > 0,
    blocked_by_model_ambiguity: modelClassification.confidence !== "high",
    blocked_by_renderer_mismatch: !discovery.found || (liveState.presentationRows || []).length === 0,
    ready_for_apply_draft: false,
    ready_for_active_approval: false,
  };
  flags.ready_for_apply_draft =
    flags.brand_record_found &&
    !flags.blocked_by_sources &&
    !flags.blocked_by_images &&
    flags.presentation_plan_ready &&
    modelClassification.confidence === "high";

  return { flags, activeFlags: Object.entries(flags).filter(([, v]) => v).map(([k]) => k) };
}

function buildV37AExceptionReview({ presentationPlan, modelClassification, visualAssetPack, discovery }) {
  const automated = [];
  const founderExceptions = [];

  for (const row of presentationPlan.rows || []) {
    const audit = auditPresentationRowExternalOwner({ slotKey: row.slotKey, title: row.title, body: row.externalBody });
    if (audit.hits.some((h) => h.patternId === "http_url")) {
      automated.push({ slotKey: row.slotKey, issue: "visible_url", action: "auto_block" });
    }
    if (/\b(fdd|item\s*19|loi)\b/i.test(row.externalBody)) {
      automated.push({ slotKey: row.slotKey, issue: "fdd_language", action: "auto_block" });
    }
    if (/company validated/i.test(row.externalBody)) {
      automated.push({ slotKey: row.slotKey, issue: "company_validated_wording", action: "auto_block" });
    }
  }

  for (const g of visualAssetPack.gallery || []) {
    if (g.isLogo) automated.push({ slot: g.intendedSlot, issue: "logo_only_image", action: "auto_reject" });
    if (g.isWrongBrandRisk) automated.push({ slot: g.intendedSlot, issue: "wrong_brand_risk", action: "founder_review" });
  }

  if (modelClassification.confidence !== "high") {
    founderExceptions.push({ type: "uncertain_brand_model", detail: modelClassification.selectedBrandModelType });
  }
  if (visualAssetPack.calaCandidateCount < 3) {
    founderExceptions.push({ type: "insufficient_cala_property_examples", detail: `${visualAssetPack.calaCandidateCount}/3` });
  }
  if (!discovery.found) {
    founderExceptions.push({ type: "brand_record_missing", detail: discovery.blockedBy });
  }
  founderExceptions.push({
    type: "strategic_positioning",
    detail: "Confirm lifestyle vs soft-collection framing before Dealality Insight and economics copy",
  });

  return {
    automatedCatchList: automated,
    founderReviewExceptions: founderExceptions,
    doNotAskFounderToCatch: [
      "empty bullets in plan rows",
      "visible URLs in owner copy",
      "logo-only gallery candidates",
      "FDD/LOI language",
      "Company Validated wording when false",
      "generic v37A plan filler markers",
    ],
  };
}

function buildNextActionRecommendation(batchQueue, discovery, target) {
  if (!discovery.found) {
    return {
      priority: 1,
      action: "create_or_verify_brand_setup_record",
      command: null,
      detail: `Create Brand Setup record for ${target.exactNames[0]} with Parent Company from official IHG/Accor pages — do not guess.`,
    };
  }
  if (batchQueue.flags.blocked_by_sources) {
    return {
      priority: 1,
      action: "apply_source_library_plans",
      command: `npm run brand-explorer-lifestyle-affiliation-source-capture -- --brands ${target.slug} --dry-run`,
      detail: "Review v37A source plans; apply Source Library only after founder approval",
    };
  }
  if (batchQueue.flags.blocked_by_images) {
    return {
      priority: 1,
      action: "complete_visual_asset_probe",
      detail: "Manual founder review of property pages; confirm six gallery + three property hotel photography URLs",
    };
  }
  if (batchQueue.flags.ready_for_apply_draft) {
    return {
      priority: 2,
      action: "v37B_draft_build_after_source_apply",
      command: `npm run brand-explorer-active-profile-preflight -- --brand ${target.slug} --dry-run`,
      detail: "Proceed to factory preflight after sources approved — still not active approval",
    };
  }
  return {
    priority: 3,
    action: "continue_v37a_batch_intake_review",
    detail: "Review knowledge pack, presentation plan, and exception list with founder",
  };
}

async function processBrandIntake(target, { dryRun = true } = {}) {
  const discovery = await discoverBrandRecord(target);
  const liveState = await loadLiveBrandState(discovery, target);
  const sourcePlans = buildSourceLibraryPlans(target, discovery);
  const modelClassification = classifyBrandModel(
    target,
    sourcePlans,
    liveState.brandBasics?.fields || {}
  );
  const visualAssetPack = await buildVisualAssetPack(target, discovery.brandName);
  const tabReadiness = evaluateTabContractReadiness(
    liveState.presentationRows,
    liveState.brandApi,
    sourcePlans,
    visualAssetPack
  );
  const knowledgePack = buildV37AKnowledgePack({
    target,
    discovery,
    modelClassification,
    sourcePlans,
    visualAssetPack,
    liveState,
    tabReadiness,
  });
  const presentationPlan = {
    version: V37A_VERSION,
    brandSlug: target.slug,
    mode: "read_only_plan",
    rows: buildPresentationPlanRows({
      target,
      discovery,
      modelClassification,
      sourcePlans,
      visualAssetPack,
      liveState,
    }),
    summary: {
      total: PLAN_SLOT_KEYS.length,
      externalOwnerReady: buildPresentationPlanRows({
        target,
        discovery,
        modelClassification,
        sourcePlans,
        visualAssetPack,
        liveState,
      }).filter((r) => r.isExternalOwnerReady).length,
    },
  };
  presentationPlan.summary.externalOwnerReady = presentationPlan.rows.filter((r) => r.isExternalOwnerReady).length;

  const batchQueue = buildV37ABatchQueueStatus({
    discovery,
    sourcePlans,
    knowledgePack,
    visualAssetPack,
    presentationPlan,
    liveState,
    modelClassification,
  });
  const exceptionReview = buildV37AExceptionReview({
    presentationPlan,
    modelClassification,
    visualAssetPack,
    discovery,
  });
  const nextAction = buildNextActionRecommendation(batchQueue, discovery, target);

  let v36bResult = null;
  if (liveState.brandConfig && discovery.found) {
    try {
      v36bResult = await validateBrandV36BContracts(target.slug, { dryRun });
    } catch (err) {
      v36bResult = { error: err.message };
    }
  }

  return {
    brandSlug: target.slug,
    brandName: discovery.brandName,
    dryRun,
    discovery,
    liveStateSummary: {
      presentationRowCount: liveState.presentationRows.length,
      registryAssetCount: liveState.registryAssets.length,
      approvedSourceCount: liveState.approvedSources.length,
      hasFactoryConfig: Boolean(liveState.brandConfig),
    },
    sourcePlans,
    modelClassification,
    knowledgePack,
    visualAssetPack,
    tabReadiness,
    presentationPlan,
    batchQueue,
    exceptionReview,
    nextAction,
    v36bCrossCheck: v36bResult
      ? {
          externalOwnerScore: v36bResult.externalOwnerScore?.numericScore,
          renderContractPass: v36bResult.renderContract?.pass,
        }
      : null,
  };
}

function buildBatchIntakeMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer v37A Lifestyle Batch Intake");
  lines.push("");
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push(`Mode: **${report.mode}** — read-only, no Airtable writes`);
  lines.push(`Brands: ${report.brands.join(", ")}`);
  lines.push("");
  lines.push("## Batch summary");
  lines.push("");
  for (const brand of report.brandResults) {
    lines.push(`### ${brand.brandName} (\`${brand.brandSlug}\`)`);
    lines.push(`- Record found: **${brand.discovery.found ? "yes" : "no"}** (${brand.discovery.brandRecordId || "—"})`);
    lines.push(`- Model: **${brand.modelClassification.selectedBrandModelType}** (${brand.modelClassification.confidence})`);
    lines.push(`- Source plans: ${brand.sourcePlans.length}`);
    lines.push(`- Gallery ready: ${brand.visualAssetPack.summary.galleryImageUrlReady}/6`);
    lines.push(`- Property examples ready: ${brand.visualAssetPack.summary.propertyExamplesImageUrlReady}/3`);
    lines.push(`- Batch flags: ${brand.batchQueue.activeFlags.join(", ") || "none"}`);
    lines.push(`- Next action: **${brand.nextAction.action}** — ${brand.nextAction.detail}`);
    lines.push("");
  }
  lines.push("## Guardrails");
  lines.push("");
  lines.push("- No Airtable writes in v37A");
  lines.push("- No owner-facing Presentation rows created");
  lines.push("- No Company Validated changes");
  lines.push("- No brand marked ready_for_active_approval");
  return lines.join("\n");
}

function buildExternalOwnerReadinessV37AMd(brandResult) {
  const lines = [];
  lines.push(`# External Owner Readiness — ${brandResult.brandName} (${V37A_VERSION})`);
  lines.push("");
  lines.push(`- Brand record: **${brandResult.discovery.found ? "found" : "missing"}**`);
  lines.push(`- Model: **${brandResult.modelClassification.selectedBrandModelType}**`);
  lines.push(`- Render projection: **${brandResult.visualAssetPack.summary.renderReadyProjection ? "yes" : "no"}**`);
  lines.push(`- External owner ready: **no** (v37A intake only)`);
  lines.push("");
  lines.push("## Blockers");
  for (const b of brandResult.knowledgePack.externalOwnerReadinessRisks || []) {
    lines.push(`- ${b}`);
  }
  lines.push("");
  lines.push("## Founder exceptions");
  for (const ex of brandResult.exceptionReview.founderReviewExceptions || []) {
    lines.push(`- **${ex.type}**: ${ex.detail}`);
  }
  lines.push("");
  lines.push("## Next action");
  lines.push(`- ${brandResult.nextAction.action}: ${brandResult.nextAction.detail}`);
  return lines.join("\n");
}

export async function runV37ALifestyleBatchIntake({ brands, dryRun = true } = {}) {
  const slugList = brands.map((b) => b.trim()).filter(Boolean);
  const targets = V37A_BRAND_TARGETS.filter((t) => slugList.includes(t.slug));
  if (!targets.length) {
    throw new Error(`No v37A targets matched: ${slugList.join(", ")}`);
  }

  const brandResults = [];
  for (const target of targets) {
    brandResults.push(await processBrandIntake(target, { dryRun }));
  }

  return {
    version: V37A_VERSION,
    generatedAt: new Date().toISOString(),
    mode: dryRun ? "dry-run" : "live-blocked",
    airtableModified: false,
    companyValidatedUntouched: true,
    brands: brandResults.map((b) => b.brandSlug),
    brandResults,
    workflow: [
      "source discovery",
      "source readiness",
      "brand knowledge pack",
      "visual asset pack",
      "presentation plan validation",
      "batch queue status",
      "next-action recommendation",
    ],
    markdown: "",
  };
}

export function writeV37AReports(report, rootDir = ROOT) {
  const reportsDir = path.join(rootDir, "reports");
  const docsDir = path.join(rootDir, "docs", "data-intelligence");
  fs.mkdirSync(reportsDir, { recursive: true });
  fs.mkdirSync(docsDir, { recursive: true });

  report.markdown = buildBatchIntakeMarkdown(report);
  const jsonPath = path.join(reportsDir, REPORT_JSON);
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`);

  const mdPath = path.join(reportsDir, REPORT_MD);
  fs.writeFileSync(mdPath, `${report.markdown}\n`);

  const docPath = path.join(docsDir, "brand-explorer-v37a-lifestyle-batch-intake.md");
  fs.writeFileSync(
    docPath,
    `# Brand Explorer v37A Lifestyle Batch Intake\n\nRead-only batch intake for lifestyle/collection brands.\n\nSee \`reports/${REPORT_MD}\`.\n`
  );

  const paths = { jsonPath, mdPath, docPath, perBrand: {} };

  for (const brandResult of report.brandResults) {
    const key = fileKeyForSlug(brandResult.brandSlug);
    const kpPath = path.join(reportsDir, `brand-knowledge-pack-${key}-v37a.json`);
    const vaPath = path.join(reportsDir, `visual-asset-pack-${key}-v37a.json`);
    const ppPath = path.join(reportsDir, `presentation-plan-${key}-v37a.json`);
    const eoPath = path.join(reportsDir, `external-owner-readiness-${key}-v37a.md`);

    fs.writeFileSync(kpPath, `${JSON.stringify(brandResult.knowledgePack, null, 2)}\n`);
    fs.writeFileSync(vaPath, `${JSON.stringify(brandResult.visualAssetPack, null, 2)}\n`);
    fs.writeFileSync(ppPath, `${JSON.stringify(brandResult.presentationPlan, null, 2)}\n`);
    fs.writeFileSync(eoPath, `${buildExternalOwnerReadinessV37AMd(brandResult)}\n`);

    paths.perBrand[brandResult.brandSlug] = { kpPath, vaPath, ppPath, eoPath };
  }

  return paths;
}

export const DEFAULT_V37A_BRANDS = V37A_BRAND_TARGETS.map((t) => t.slug);
