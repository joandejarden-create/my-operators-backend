/**
 * Brand-specific source validation for Brand Explorer setups.
 *
 * Hierarchy (strongest → weakest):
 * 1. Brand-specific official brand site
 * 2. Brand-specific official development page
 * 3. Brand-specific property pages
 * 4. Parent-company brand page
 * 5. Parent-company corporate page
 * 6. Third-party (supplementary only)
 *
 * Parent pages may support ownership / family / platform / portfolio context.
 * They may NOT be the only support for positioning, audience, design story,
 * property examples, images, scenarios, owner-facing fit, or differentiators.
 */
import { getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function hostnameOf(url) {
  try {
    return new URL(String(url)).hostname.replace(/^www\./i, "").toLowerCase();
  } catch {
    return "";
  }
}

function hostMatches(host, domain) {
  const h = nz(host).toLowerCase();
  const d = nz(domain).toLowerCase().replace(/^www\./, "");
  if (!h || !d) return false;
  return h === d || h.endsWith(`.${d}`);
}

/** Canonical brand-specific domains that must appear in the source set. */
export const CANONICAL_BRAND_SOURCE_RULES = Object.freeze({
  "hotel-indigo": {
    requiredBrandDomains: ["hotelindigo.com"],
    allowedParentDomains: ["ihg.com", "ihgplc.com", "development.ihg.com"],
    brandModelType: "lifestyle_full_brand",
  },
  "mgallery-collection": {
    requiredBrandDomains: ["mgallery.accor.com"],
    allowedParentDomains: ["accor.com", "group.accor.com", "all.accor.com"],
    brandModelType: "soft_brand_collection",
  },
  "small-luxury-hotels-of-the-world": {
    requiredBrandDomains: ["slh.com"],
    allowedParentDomains: [],
    brandModelType: "independent_luxury_consortium",
    forbidFranchiseLogic: true,
  },
});

/** Content that must not rely on parent-only sources. */
export const BRAND_SPECIFIC_CONTENT_SLOTS = Object.freeze([
  "overview.scenario.1",
  "overview.scenario.2",
  "overview.scenario.3",
  "overview.why_value",
  "overview.proof.1",
  "overview.proof.2",
  "overview.proof.3",
  "overview.proof.4",
  "overview.differentiators.identity",
  "overview.differentiators.commercial",
  "overview.featured_application",
  "footprint.openings",
  "materials.gallery.1",
  "materials.gallery.2",
  "materials.gallery.3",
  "materials.gallery.4",
  "materials.gallery.5",
  "materials.gallery.6",
]);

export const PARENT_ALLOWED_CONTEXT_FIELDS = Object.freeze([
  "parentCompany",
  "brandArchitecture",
  "brandFamily",
  "overview.portfolio_context",
  "overview.relative_positioning",
]);

function classifyHost(host, rule, brandConfig) {
  if (!host) return "unknown";
  const required = rule?.requiredBrandDomains || [];
  const parent = rule?.allowedParentDomains || [];
  const official = brandConfig?.officialSourceDomains || [];

  if (required.some((d) => hostMatches(host, d))) return "brand_specific";
  // Development / consumer hosts from config
  const consumerHost = hostnameOf(brandConfig?.consumerUrl);
  const developmentHost = hostnameOf(brandConfig?.developmentUrl);
  if (consumerHost && hostMatches(host, consumerHost) && !parent.some((d) => hostMatches(host, d))) {
    return "brand_specific";
  }
  if (developmentHost && hostMatches(host, developmentHost)) {
    // development.ihg.com is parent platform for Indigo; treat as parent unless also brand-required
    if (parent.some((d) => hostMatches(host, d))) return "parent";
    if (required.some((d) => hostMatches(host, d))) return "brand_specific";
    return "brand_development";
  }
  if (parent.some((d) => hostMatches(host, d))) return "parent";
  if (official.some((d) => hostMatches(host, d))) {
    // official list may mix brand + parent
    if (parent.some((d) => hostMatches(host, d))) return "parent";
    return "brand_specific";
  }
  return "third_party";
}

function collectSourceUrls({ brandConfig, registryAssets = [], presentationRows = [], brandApi = null }) {
  const urls = [];
  const push = (url, origin) => {
    const u = nz(url);
    if (!u || !/^https?:\/\//i.test(u)) return;
    urls.push({ url: u, host: hostnameOf(u), origin });
  };

  push(brandConfig?.consumerUrl, "brand_config.consumerUrl");
  push(brandConfig?.developmentUrl, "brand_config.developmentUrl");
  for (const u of brandConfig?.momentumSourceUrls || []) push(u, "brand_config.momentumSourceUrls");
  push(brandApi?.brandWebsite, "brandApi.brandWebsite");

  for (const a of registryAssets || []) {
    push(a.sourceUrl || a.sourceURL, "registry.sourceUrl");
    push(a.sourcePageUrl || a.sourcePageURL, "registry.sourcePageUrl");
  }

  for (const row of presentationRows || []) {
    push(row.summaryUrl, "presentation.summaryUrl");
    push(row.imageUrl, "presentation.imageUrl");
    // image URLs are asset hosts — skip airtableusercontent for classification of "sources"
  }

  return urls.filter((x) => !/airtableusercontent\.com/i.test(x.host));
}

/**
 * @returns {{
 *   pass: boolean,
 *   brandSlug: string,
 *   requiredBrandDomains: string[],
 *   missingRequiredBrandDomains: string[],
 *   classificationCounts: object,
 *   sources: object[],
 *   failures: string[],
 *   parentOnlyContentRisks: string[],
 * }}
 */
export function evaluateBrandSpecificSourceValidation({
  brandSlug,
  brandConfig = null,
  registryAssets = [],
  presentationRows = [],
  brandApi = null,
} = {}) {
  const config = brandConfig || getActiveProfileBrandConfig(brandSlug) || null;
  const rule = CANONICAL_BRAND_SOURCE_RULES[brandSlug] || null;

  const requiredBrandDomains =
    rule?.requiredBrandDomains ||
    (config?.consumerUrl ? [hostnameOf(config.consumerUrl)].filter(Boolean) : []);

  const allowedParentDomains = rule?.allowedParentDomains || [];

  const sources = collectSourceUrls({
    brandConfig: config,
    registryAssets,
    presentationRows,
    brandApi,
  }).map((s) => ({
    ...s,
    class: classifyHost(s.host, { requiredBrandDomains, allowedParentDomains }, config),
  }));

  const classificationCounts = sources.reduce((acc, s) => {
    acc[s.class] = (acc[s.class] || 0) + 1;
    return acc;
  }, {});

  const hosts = [...new Set(sources.map((s) => s.host).filter(Boolean))];
  const missingRequiredBrandDomains = requiredBrandDomains.filter(
    (d) => !hosts.some((h) => hostMatches(h, d))
  );

  const failures = [];
  if (requiredBrandDomains.length && missingRequiredBrandDomains.length) {
    failures.push(`missing_canonical_brand_domains:${missingRequiredBrandDomains.join(",")}`);
  }

  const brandSpecificCount =
    (classificationCounts.brand_specific || 0) + (classificationCounts.brand_development || 0);
  const parentCount = classificationCounts.parent || 0;
  const totalClassified = brandSpecificCount + parentCount + (classificationCounts.third_party || 0);

  if (totalClassified > 0 && brandSpecificCount === 0 && parentCount > 0) {
    failures.push("sources_are_parent_company_only");
  }
  // Parent share only fails when brand-specific evidence is thin (<3) despite parent volume.
  // Image/registry parent hosts often dominate URL counts even when brand domain is present.
  if (
    totalClassified >= 3 &&
    brandSpecificCount > 0 &&
    brandSpecificCount < 3 &&
    parentCount / totalClassified >= 0.7
  ) {
    failures.push("sources_mostly_parent_company_umbrella");
  }

  // Image / openings registry pages should not be parent-only when brand domains are required
  const parentOnlyContentRisks = [];
  if (requiredBrandDomains.length) {
    const visualRows = (presentationRows || []).filter((r) =>
      /^(materials\.gallery\.\d+|footprint\.openings|overview\.scenario\.\d+)$/.test(nz(r.slotKey))
    );
    const visualRegistry = (registryAssets || []).filter((a) => {
      const page = nz(a.sourcePageUrl || a.sourceUrl);
      return page && visualRows.length;
    });
    const visualClasses = visualRegistry.map((a) =>
      classifyHost(hostnameOf(a.sourcePageUrl || a.sourceUrl), { requiredBrandDomains, allowedParentDomains }, config)
    );
    if (
      visualClasses.length >= 3 &&
      visualClasses.every((c) => c === "parent") &&
      brandSpecificCount === 0
    ) {
      parentOnlyContentRisks.push("gallery_or_openings_parent_only");
      failures.push("visual_sources_parent_company_only");
    }
  }

  if (rule?.forbidFranchiseLogic) {
    const corpus = [
      nz(brandApi?.brandPositioning),
      nz(brandApi?.brandCustomerPromise),
      ...(presentationRows || []).map((r) => `${nz(r.title)}\n${nz(r.body)}`),
    ].join("\n");
    if (/\bfranchise agreement\b|\bFDD\b|\bItem\s*19\b/i.test(corpus)) {
      failures.push("slh_franchise_logic_forced");
    }
  }

  // Basics positioning must not be empty while claiming active readiness — soft check here
  if (!nz(brandApi?.brandPositioning) && !nz(brandApi?.guestPsychographics)) {
    // not a source failure
  }

  return {
    pass: failures.length === 0,
    brandSlug,
    requiredBrandDomains,
    allowedParentDomains,
    missingRequiredBrandDomains,
    classificationCounts,
    sources: sources.slice(0, 40),
    failures,
    parentOnlyContentRisks,
    brandSpecificContentSlots: BRAND_SPECIFIC_CONTENT_SLOTS,
    parentAllowedContextFields: PARENT_ALLOWED_CONTEXT_FIELDS,
  };
}
