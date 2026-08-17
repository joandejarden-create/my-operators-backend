/**
 * Detect Official Property URLs that are multi-property brand portal roots
 * (not property-specific pages). Single-property domains at `/` are OK.
 */

import { websiteHost } from "./match-current-census.js";

/** Hosts that are multi-property brand portals — root ≠ Official Property URL. */
export const MULTI_PROPERTY_BRAND_PORTAL_HOSTS = Object.freeze(
  new Set([
    "riu.com",
    "barcelo.com",
    "melia.com",
    "marriott.com",
    "hilton.com",
    "ihg.com",
    "hyatt.com",
    "hyattinclusivecollection.com",
    "dreamsresorts.com",
    "secretsresorts.com",
    "breathlessresorts.com",
    "choicehotels.com",
    "bahia-principe.com",
    "cataloniahotels.com",
    "belivehotels.com",
    "clubmed.com",
    "breezes.com",
    "hardrock.com",
    "hardrockhotels.com",
    "wyndhamhotels.com",
    "iberostar.com",
    "hodelpa.com",
    "amhsamarina.com",
    "occidentalhotels.com",
    "superclubs.com",
    "vivaresortsbywyndham.com",
    "excellenceresorts.com",
    "slh.com",
    "sirenishotels.com",
    "accor.com",
    "all.accor.com",
    "radissonhotels.com",
  ])
);

/**
 * @param {string} url
 * @returns {{ isBrandHomepage: boolean, host: string, path: string, reason: string }}
 */
export function classifyOfficialPropertyUrl(url) {
  const raw = String(url || "").trim();
  if (!raw) {
    return {
      isBrandHomepage: false,
      host: "",
      path: "",
      reason: "missing_url",
    };
  }
  let host = "";
  let path = "/";
  try {
    const withProto = /^https?:\/\//i.test(raw) ? raw : `https://${raw}`;
    const u = new URL(withProto);
    host = (websiteHost(withProto) || u.hostname || "")
      .replace(/^www\./i, "")
      .toLowerCase();
    path = (u.pathname || "/").replace(/\/+$/, "") || "/";
  } catch {
    return {
      isBrandHomepage: false,
      host: "",
      path: "",
      reason: "invalid_url",
    };
  }

  const isPortal = MULTI_PROPERTY_BRAND_PORTAL_HOSTS.has(host);
  const localeOnly =
    path === "/" ||
    /^\/(en|es|fr|de|pt)(-[a-z]{2})?$/i.test(path) ||
    /^\/(en|es)(-[a-z]{2})?\/(hotels|resorts|destinations)?$/i.test(path);

  if (isPortal && localeOnly) {
    return {
      isBrandHomepage: true,
      host,
      path,
      reason: "multi_property_brand_homepage",
    };
  }
  return {
    isBrandHomepage: false,
    host,
    path,
    reason: "property_or_single_site_ok",
  };
}

export function isBrandHomepageOfficialUrl(url) {
  return classifyOfficialPropertyUrl(url).isBrandHomepage === true;
}
