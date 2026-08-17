/**
 * Lifestyle / affiliation source capture plans v35B.
 *
 * Dry-run plans only — no Airtable writes.
 */
import { getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";
import { COMPANY_DOMAINS as TRIBUTE_COMPANY_DOMAINS, URL_CANDIDATES as TRIBUTE_URL_CANDIDATES } from "./tribute-portfolio-brand-package.js";

export const SOURCE_CAPTURE_PLAN_VERSION = "v35B";

const SOURCE_CATEGORIES = Object.freeze([
  { id: "official_brand_page", label: "Official brand / affiliation page", required: true },
  { id: "development_membership", label: "Development or membership page", required: true },
  { id: "owner_criteria", label: "Owner / hotelier-facing criteria", required: true },
  { id: "property_directory", label: "Property collection directory", required: true },
  { id: "gallery_images", label: "6 gallery / property images", required: true, count: 6 },
  { id: "property_examples", label: "3 property examples with hotel images", required: true, count: 3 },
  { id: "standards_quality", label: "Standards / participation / quality expectations", required: true },
  { id: "distribution_loyalty", label: "Distribution / loyalty / affiliation platform", required: true },
  { id: "press_resources", label: "Press or official brand resources", required: false },
]);

function planForDesignHotels(brandConfig) {
  return {
    brandSlug: brandConfig.slug,
    brandName: brandConfig.name,
    recordId: brandConfig.recordId,
    modelType: brandConfig.brandModelType,
    mode: "dry-run",
    copyGuidance: "Affiliation / curation platform — no franchise-flag language.",
    categories: [
      {
        ...SOURCE_CATEGORIES[0],
        status: "needed",
        candidates: [
          { url: "https://www.designhotels.com/", role: "consumer_page", priority: 1 },
          { url: "https://www.marriott.com/marriott-brands/design-hotels.mi", role: "affiliation_page", priority: 2 },
        ],
      },
      {
        ...SOURCE_CATEGORIES[1],
        status: "needed",
        candidates: [
          { url: "https://www.marriott.com/marriott-brands/design-hotels.mi", role: "development_affiliation", priority: 1 },
        ],
      },
      {
        ...SOURCE_CATEGORIES[2],
        status: "needed",
        candidates: [
          { url: "https://www.designhotels.com/about", role: "owner_criteria", priority: 1, note: "Verify live path during capture" },
        ],
      },
      {
        ...SOURCE_CATEGORIES[3],
        status: "needed",
        candidates: [{ url: "https://www.designhotels.com/hotels", role: "property_directory", priority: 1 }],
      },
      {
        ...SOURCE_CATEGORIES[4],
        status: "blocked",
        note: "Requires 6 hotel/property photography URLs from official property pages — no logos or lifestyle stock.",
        candidates: [],
      },
      {
        ...SOURCE_CATEGORIES[5],
        status: "blocked",
        note: "Select 3 curator-approved hotels from official directory with real property images.",
        candidates: [],
      },
      {
        ...SOURCE_CATEGORIES[6],
        status: "needed",
        candidates: [
          { url: "https://www.designhotels.com/about", role: "curation_standards", priority: 1 },
        ],
      },
      {
        ...SOURCE_CATEGORIES[7],
        status: "needed",
        candidates: [
          { url: "https://www.marriott.com/loyalty.mi", role: "bonvoy_distribution", priority: 1 },
        ],
      },
      {
        ...SOURCE_CATEGORIES[8],
        status: "optional",
        candidates: [{ url: "https://news.marriott.com/", role: "press", priority: 2 }],
      },
    ],
    recommendedSequence: [
      "source_capture_official_pages",
      "approve_source_library_rows",
      "select_three_property_examples_from_directory",
      "probe_six_gallery_images_from_property_pages",
      "register_brand_asset_registry_rows",
      "affiliation_copy_governance_setup",
      "asset_pack_dry_run",
    ],
    buildRecommendation: "source_capture_first",
  };
}

function planForSlh(brandConfig) {
  return {
    brandSlug: brandConfig.slug,
    brandName: brandConfig.name,
    recordId: brandConfig.recordId,
    modelType: brandConfig.brandModelType,
    mode: "dry-run",
    copyGuidance: "Independent luxury consortium — no parent-brand or franchise language.",
    categories: [
      {
        ...SOURCE_CATEGORIES[0],
        status: "needed",
        candidates: [{ url: "https://www.slh.com/", role: "consumer_page", priority: 1 }],
      },
      {
        ...SOURCE_CATEGORIES[1],
        status: "needed",
        candidates: [{ url: "https://www.slh.com/about-slh", role: "membership_page", priority: 1 }],
      },
      {
        ...SOURCE_CATEGORIES[2],
        status: "needed",
        candidates: [
          { url: "https://www.slh.com/about-slh", role: "owner_criteria", priority: 1 },
          { url: "https://www.slh.com/join-slh", role: "membership_criteria", priority: 2, note: "Verify live path" },
        ],
      },
      {
        ...SOURCE_CATEGORIES[3],
        status: "needed",
        candidates: [{ url: "https://www.slh.com/hotels", role: "hotel_finder", priority: 1 }],
      },
      {
        ...SOURCE_CATEGORIES[4],
        status: "blocked",
        note: "6 hotel photography URLs from official SLH property pages — no stock lifestyle imagery.",
        candidates: [],
      },
      {
        ...SOURCE_CATEGORIES[5],
        status: "blocked",
        note: "3 independent luxury property examples with verifiable hotel images.",
        candidates: [],
      },
      {
        ...SOURCE_CATEGORIES[6],
        status: "needed",
        candidates: [{ url: "https://www.slh.com/about-slh", role: "quality_standards", priority: 1 }],
      },
      {
        ...SOURCE_CATEGORIES[7],
        status: "needed",
        candidates: [
          { url: "https://www.slh.com/", role: "slh_club_distribution", priority: 1 },
        ],
      },
      {
        ...SOURCE_CATEGORIES[8],
        status: "optional",
        candidates: [],
      },
    ],
    recommendedSequence: [
      "source_capture_official_pages",
      "legal_review_consortium_sensitivity",
      "approve_source_library_rows",
      "select_three_luxury_property_examples",
      "probe_six_gallery_images",
      "affiliation_copy_governance_setup",
      "asset_pack_dry_run",
    ],
    buildRecommendation: "source_capture_first",
  };
}

function planForTributeBenchmark(brandConfig, liveState = {}) {
  const approvedSources = liveState.approvedSources || 0;
  const galleryApi = liveState.galleryApiWithImageUrl || 0;
  const propertyWithImage = liveState.propertyExamplesWithImage || 0;
  const galleryPass = liveState.galleryPass === true || galleryApi >= 6;
  const propertyPass = liveState.propertyPass === true || propertyWithImage >= 3;

  return {
    brandSlug: brandConfig.slug,
    brandName: brandConfig.name,
    recordId: brandConfig.recordId,
    modelType: brandConfig.brandModelType,
    mode: "dry-run",
    role: "technical_benchmark",
    copyGuidance: "Soft-brand collection — Marriott Tribute Portfolio lifestyle conversion framing.",
    liveSnapshot: {
      approvedSources,
      galleryApiWithImageUrl: galleryApi,
      propertyExamplesWithImage: propertyWithImage,
      presentationVisibleRows: liveState.visibleRows || 0,
    },
    categories: SOURCE_CATEGORIES.map((cat) => {
      if (cat.id === "official_brand_page") {
        return {
          ...cat,
          status: approvedSources > 0 ? "present" : "needed",
          candidates: TRIBUTE_URL_CANDIDATES.filter((c) => c.role === "consumer_page"),
        };
      }
      if (cat.id === "development_membership") {
        return {
          ...cat,
          status: approvedSources > 0 ? "present" : "needed",
          candidates: TRIBUTE_URL_CANDIDATES.filter((c) => c.role === "development_page"),
        };
      }
      if (cat.id === "gallery_images") {
        return {
          ...cat,
          status: galleryPass ? "present" : galleryApi > 0 ? "partial" : "needed",
          note: `${galleryApi}/6 gallery imageUrl in live API`,
        };
      }
      if (cat.id === "property_examples") {
        return {
          ...cat,
          status: propertyPass ? "present" : propertyWithImage > 0 ? "partial" : "needed",
          note: `${propertyWithImage}/3 property examples with images`,
        };
      }
      if (cat.id === "distribution_loyalty") {
        return { ...cat, status: "present", candidates: [{ url: "https://www.marriott.com/loyalty.mi", role: "bonvoy" }] };
      }
      return { ...cat, status: approvedSources > 0 ? "present" : "needed" };
    }),
    officialDomains: TRIBUTE_COMPANY_DOMAINS,
    recommendedSequence: [
      "copy_governance_pass",
      "registry_traceability_approval",
      "staged_apply_draft",
      "founder_visual_review",
      "active_approval",
    ],
    buildRecommendation: galleryPass && propertyPass ? "staged_apply_after_copy_governance" : "copy_governance_and_registry_first",
  };
}

export function buildSourceCapturePlan(brandSlug, liveState = {}) {
  const brandConfig = getActiveProfileBrandConfig(brandSlug);
  if (!brandConfig) return null;
  if (brandSlug === "design-hotels") return planForDesignHotels(brandConfig);
  if (brandSlug === "small-luxury-hotels-of-the-world") return planForSlh(brandConfig);
  if (brandSlug === "tribute-portfolio") return planForTributeBenchmark(brandConfig, liveState);
  return {
    brandSlug,
    brandName: brandConfig.name,
    mode: "dry-run",
    status: "deferred_until_source_capture",
    categories: SOURCE_CATEGORIES.map((c) => ({ ...c, status: "needed" })),
    buildRecommendation: "source_capture_first",
  };
}

export function buildSourceCapturePlanMarkdown(plan) {
  if (!plan) return "";
  const lines = [];
  lines.push(`# Source Capture Plan ${SOURCE_CAPTURE_PLAN_VERSION}`);
  lines.push("");
  lines.push(`- Brand: **${plan.brandName}** (\`${plan.brandSlug}\`)`);
  lines.push(`- Record: \`${plan.recordId}\``);
  lines.push(`- Model: ${plan.modelType}`);
  lines.push(`- Mode: **${plan.mode}** (no Airtable writes)`);
  if (plan.copyGuidance) lines.push(`- Copy guidance: ${plan.copyGuidance}`);
  if (plan.buildRecommendation) lines.push(`- Build recommendation: **${plan.buildRecommendation}**`);
  lines.push("");
  lines.push("## Source categories");
  for (const cat of plan.categories || []) {
    lines.push(`### ${cat.label} — **${cat.status}**`);
    if (cat.note) lines.push(`- ${cat.note}`);
    for (const c of cat.candidates || []) {
      lines.push(`- [${c.role}] ${c.url}${c.note ? ` — ${c.note}` : ""}`);
    }
    lines.push("");
  }
  if (plan.recommendedSequence?.length) {
    lines.push("## Recommended sequence");
    for (const step of plan.recommendedSequence) lines.push(`1. ${step}`);
  }
  return lines.join("\n");
}
