import assert from "node:assert/strict";
import {
  companyTypeFromProfileFields,
  normalizeCompanyTypeToFilterKey,
} from "../lib/company-type-normalize.js";

assert.equal(normalizeCompanyTypeToFilterKey("Hotel Brands (Franchise)"), "HOTEL BRANDS (FRANCHISE)");
assert.equal(normalizeCompanyTypeToFilterKey("Hotel Owner"), "HOTEL OWNERS");
assert.equal(normalizeCompanyTypeToFilterKey("Hotel Management Company"), "HOTEL MGMT. COMPANY");
assert.equal(normalizeCompanyTypeToFilterKey("Hospitality Consultants"), "HOSPITALITY CONSULTANTS");
assert.equal(normalizeCompanyTypeToFilterKey("Other"), "OTHER");
assert.equal(normalizeCompanyTypeToFilterKey("Hotel Owner - Operator"), "OWNER_OPERATOR");
assert.equal(normalizeCompanyTypeToFilterKey("Owner-Operator"), "OWNER_OPERATOR");
assert.equal(normalizeCompanyTypeToFilterKey("Owner Operator"), "OWNER_OPERATOR");
assert.equal(normalizeCompanyTypeToFilterKey("Hotel Owner Operator"), "OWNER_OPERATOR");
assert.notEqual(normalizeCompanyTypeToFilterKey("Hotel Owner - Operator"), "HOTEL MGMT. COMPANY");
assert.notEqual(normalizeCompanyTypeToFilterKey("Hotel Owner - Operator"), "HOTEL OWNERS");
assert.equal(
  companyTypeFromProfileFields({ "Company Type": "Hospitality Consultants" }),
  "HOSPITALITY CONSULTANTS"
);

console.log("test-company-type-normalize: ok");
