#!/usr/bin/env node
/**
 * Unit tests for Brand Website preference (parent root vs brand-specific).
 */
import {
  evaluateBrandWebsitePreference,
  isParentCompanyRootWebsite,
  resolveOfficialBrandWebsiteFromSourcePack,
  FAIRFIELD_OFFICIAL_BRAND_WEBSITE,
} from "../lib/partner-intelligence/brand-explorer-brand-website-preference.js";

function assert(cond, msg) {
  if (!cond) throw new Error(msg);
}

assert(isParentCompanyRootWebsite("https://marriott.com/") === true, "marriott root");
assert(isParentCompanyRootWebsite("https://www.marriott.com") === true, "www marriott root");
assert(isParentCompanyRootWebsite("https://fairfield.marriott.com/") === false, "fairfield subdomain ok");
assert(isParentCompanyRootWebsite("https://www.marriott.com/en-us/hotels/x/overview/") === false, "property path not root");

const official = resolveOfficialBrandWebsiteFromSourcePack("fairfield-by-marriott");
assert(
  official === FAIRFIELD_OFFICIAL_BRAND_WEBSITE,
  `expected Fairfield official ${FAIRFIELD_OFFICIAL_BRAND_WEBSITE}, got ${official}`
);

const bad = evaluateBrandWebsitePreference({
  brandSlug: "fairfield-by-marriott",
  brandWebsite: "https://marriott.com/",
});
assert(bad.pass === false, "parent root must fail when brand-specific exists");
assert(bad.code === "parent_company_root_website", "expected parent_company_root_website");
assert(bad.preferredWebsite === FAIRFIELD_OFFICIAL_BRAND_WEBSITE, "preferred must be Fairfield brand site");

const good = evaluateBrandWebsitePreference({
  brandSlug: "fairfield-by-marriott",
  brandWebsite: FAIRFIELD_OFFICIAL_BRAND_WEBSITE,
});
assert(good.pass === true, "brand-specific website must pass");

const blankOk = evaluateBrandWebsitePreference({
  brandSlug: "fairfield-by-marriott",
  brandWebsite: "",
});
assert(blankOk.pass === true, "blank is not a parent-root defect (handled elsewhere)");
assert(blankOk.preferredWebsite === FAIRFIELD_OFFICIAL_BRAND_WEBSITE, "blank prefers official");

// Brand without sourced brand_page override: parent root alone does not invent a preferred URL
const noPack = evaluateBrandWebsitePreference({
  brandSlug: "unknown-brand-xyz",
  brandWebsite: "https://marriott.com/",
  officialBrandWebsite: "",
});
assert(noPack.pass === true, "do not fail parent root when no official brand URL is evidenced");

console.log("ok brand-explorer-brand-website-preference");
