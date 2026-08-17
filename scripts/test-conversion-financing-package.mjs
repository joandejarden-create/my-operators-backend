/**
 * Conversion Financing Package — acceptance tests (no live Airtable).
 * Run: node scripts/test-conversion-financing-package.mjs
 */
import {
  buildConversionFinancingPackage,
  sanitizeConversionFinancingInputs,
} from "../lib/conversion-financing-package-build.js";
import {
  buildHotelCapitalOpportunityView,
  isProviderVisibleSharingStatus,
} from "../lib/conversion-financing-package-access.js";
import { DEFAULT_CAPITAL_SHARING_STATUS } from "../lib/capital-setup/conversion-financing-package-options.js";

function assert(condition, message) {
  if (!condition) {
    console.error("FAIL:", message);
    process.exitCode = 1;
    return false;
  }
  console.log("PASS:", message);
  return true;
}

const partialInputs = sanitizeConversionFinancingInputs({
  capitalNeedType: "Brand Conversion / Reflagging",
  capitalAmountRange: "$8M – $12M",
  capitalCurrency: "USD",
  capitalUnlock: ["Brand Conversion", "Renovation Completion"],
  capitalAssetStatus: "Operating Hotel",
  capitalSharingStatus: "Draft",
});

const dealFields = {
  "Property Name": "Sample Bay Hotel",
  City: "Cartagena",
  Country: "Colombia",
  "Project Type": "Conversion",
  "Total Number of Rooms/Keys": 180,
  "Current Brand Affiliation": "Independent",
};

const result = buildConversionFinancingPackage(partialInputs, dealFields);
const s = result.snapshot;

console.log("\n=== Conversion Financing Package — partial inputs ===\n");

assert(s.opportunitySummary != null, "Opportunity summary section present");
assert(s.capitalRequest != null, "Capital request section present");
assert(s.conversionRepositioningThesis != null, "Conversion thesis section present");
assert(s.capitalProviderReviewLens.categories.length > 0, "Provider categories generated");
assert(
  !JSON.stringify(s.capitalProviderReviewLens).toLowerCase().includes("best lender"),
  "No broker-like lender recommendation language"
);
assert(
  !JSON.stringify(s.capitalProviderReviewLens).toLowerCase().includes("guaranteed"),
  "No guaranteed financing language"
);
assert(s.riskFlagsAndOpenQuestions.items.length > 0, "Partial inputs produce open questions");
assert(result.narrative.length > 100, "Narrative generated from partial inputs");

assert(
  DEFAULT_CAPITAL_SHARING_STATUS === "Draft",
  "Default sharing status is Draft (private)"
);
assert(!isProviderVisibleSharingStatus("Draft"), "Draft is not provider-visible");
assert(!isProviderVisibleSharingStatus("Internal Review"), "Internal Review is not provider-visible");
assert(isProviderVisibleSharingStatus("Shared Anonymously"), "Shared Anonymously is provider-visible");

const providerView = buildHotelCapitalOpportunityView({
  inputs: partialInputs,
  snapshot: s,
  sharingStatus: "Draft",
});
assert(providerView === null, "Provider view blocked when sharing is Draft");

const providerViewOk = buildHotelCapitalOpportunityView({
  inputs: partialInputs,
  snapshot: s,
  sharingStatus: "Shared Anonymously",
});
assert(providerViewOk != null, "Provider view available when sharing allows");
assert(
  !JSON.stringify(providerViewOk).includes("Sample Bay Hotel"),
  "Provider view does not expose property name"
);
assert(providerViewOk.objectType === "Hotel Capital Opportunity", "Provider object type label correct");

const sanitized = sanitizeConversionFinancingInputs({});
assert(sanitized.capitalSharingStatus === "Draft", "Sanitize defaults sharing status to Draft");

console.log("\nDone.\n");
