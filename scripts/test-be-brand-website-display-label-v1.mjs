#!/usr/bin/env node
/**
 * Permanent: Brand Website labels must stay brand-specific (never bare parent host
 * when a brand path/subdomain exists).
 */
import assert from "assert";
import {
  brandWebsiteDisplayLabel,
  isParentCompanyHomepage,
  normalizeBrandWebsiteUrl,
} from "../lib/brand-explorer/active-brand-website-corrections.js";

assert.strictEqual(
  brandWebsiteDisplayLabel("https://marriott.com/en-us/marriott-hotels/travel"),
  "marriott.com/en-us/marriott-hotels/travel"
);
assert.strictEqual(
  brandWebsiteDisplayLabel("https://four-points.marriott.com/"),
  "four-points.marriott.com"
);
assert.strictEqual(brandWebsiteDisplayLabel("https://marriott.com/"), "marriott.com");
assert.strictEqual(
  brandWebsiteDisplayLabel("https://hyatt.com/en-US/brand/hyatt-regency"),
  "hyatt.com/en-US/brand/hyatt-regency"
);

assert.strictEqual(isParentCompanyHomepage("https://marriott.com", "Marriott", "Fairfield by Marriott"), true);
assert.strictEqual(
  isParentCompanyHomepage(
    "https://marriott.com/en-us/marriott-hotels/travel",
    "Marriott",
    "Marriott Hotels"
  ),
  false
);
assert.strictEqual(isParentCompanyHomepage("https://hyatt.com", "Hyatt", "Hyatt Regency"), true);
assert.strictEqual(isParentCompanyHomepage("https://hyatt.com", "Hyatt", "Hyatt"), false);

assert.ok(normalizeBrandWebsiteUrl("https://www.fairmont.com/").startsWith("https://fairmont.com"));

console.log("PASS be-brand-website-display-label-v1");
