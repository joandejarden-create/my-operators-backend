/**
 * Brand Explorer — Brand Website preference / validation.
 *
 * Parent-company root domains (e.g. https://marriott.com/) must not pass as the
 * Brand Website when an official brand-specific URL exists in the source pack.
 *
 * Does not invent URLs for brands without a sourced brand_page reference.
 * Does not rewrite unrelated brand fixtures without evidence.
 */
import { WAVE16A_PACKS_BY_SLUG } from "./brand-explorer-wave16a-stage1-source-content.js";

export const BRAND_WEBSITE_PREFERENCE_VERSION = "brand-website-preference-v1";

/** Parent-company marketing roots that are too broad for Brand Website. */
export const PARENT_COMPANY_ROOT_WEBSITE_PATTERNS = Object.freeze([
  /^https?:\/\/(www\.)?marriott\.com\/?$/i,
  /^https?:\/\/(www\.)?hilton\.com\/?$/i,
  /^https?:\/\/(www\.)?ihg\.com\/?$/i,
  /^https?:\/\/(www\.)?choicehotels\.com\/?$/i,
  /^https?:\/\/(www\.)?accor\.com\/?$/i,
  /^https?:\/\/(www\.)?hyatt\.com\/?$/i,
  /^https?:\/\/(www\.)?radissonhotels\.com\/?$/i,
]);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function normalizeWebsite(url) {
  const u = nz(url);
  if (!u) return "";
  return u.replace(/\/+$/, "").toLowerCase();
}

export function isParentCompanyRootWebsite(url) {
  const u = nz(url);
  if (!u) return false;
  return PARENT_COMPANY_ROOT_WEBSITE_PATTERNS.some((re) => re.test(u));
}

/**
 * Official brand-specific consumer URL from Wave 16A stage-1 source packs
 * (officialReferences type brand_page), when present and not a parent root.
 */
export function resolveOfficialBrandWebsiteFromSourcePack(brandSlug) {
  const pack = WAVE16A_PACKS_BY_SLUG?.[brandSlug];
  if (!pack) return "";
  const brandPage = (pack.officialReferences || []).find((r) => r.type === "brand_page");
  const url = nz(brandPage?.url);
  if (!url) return "";
  if (isParentCompanyRootWebsite(url)) return "";
  return url;
}

/**
 * Prefer an official brand-specific website over a parent-company root.
 *
 * @returns {{
 *   pass: boolean,
 *   brandSlug: string,
 *   currentWebsite: string,
 *   officialBrandWebsite: string,
 *   preferredWebsite: string,
 *   isParentRoot: boolean,
 *   code: string|null,
 *   message: string|null,
 * }}
 */
export function evaluateBrandWebsitePreference({
  brandSlug = "",
  brandWebsite = "",
  officialBrandWebsite = null,
} = {}) {
  const slug = nz(brandSlug);
  const current = nz(brandWebsite);
  const official =
    officialBrandWebsite != null
      ? nz(officialBrandWebsite)
      : resolveOfficialBrandWebsiteFromSourcePack(slug);

  const parentRoot = isParentCompanyRootWebsite(current);
  const officialIsBrandSpecific = Boolean(official) && !isParentCompanyRootWebsite(official);

  if (parentRoot && officialIsBrandSpecific) {
    return {
      version: BRAND_WEBSITE_PREFERENCE_VERSION,
      pass: false,
      brandSlug: slug,
      currentWebsite: current,
      officialBrandWebsite: official,
      preferredWebsite: official,
      isParentRoot: true,
      code: "parent_company_root_website",
      message: `Brand Website is parent-company root (${current}); prefer official brand-specific URL ${official}`,
    };
  }

  if (current && officialIsBrandSpecific) {
    const same = normalizeWebsite(current) === normalizeWebsite(official);
    if (!same && /marriott\.com|hilton\.com|ihg\.com|choicehotels\.com|accor\.com|hyatt\.com/i.test(current)) {
      // Same parent family but not the brand-specific host — soft preference only when current is clearly parent-ish path
      // Keep strict fail only for parent roots (already handled). Pass otherwise to avoid broad rewrites.
    }
  }

  return {
    version: BRAND_WEBSITE_PREFERENCE_VERSION,
    pass: true,
    brandSlug: slug,
    currentWebsite: current,
    officialBrandWebsite: official,
    preferredWebsite: officialIsBrandSpecific ? official : current,
    isParentRoot: parentRoot,
    code: null,
    message: null,
  };
}

/** Fairfield canonical Brand Website (sourced stage-1 brand_page). */
export const FAIRFIELD_OFFICIAL_BRAND_WEBSITE = "https://fairfield.marriott.com/";
