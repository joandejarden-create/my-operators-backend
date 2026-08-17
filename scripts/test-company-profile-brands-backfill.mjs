/**
 * Unit checks for company-profile-brands-backfill (no Airtable).
 */
import assert from "assert";
import {
  buildBrandBasicsIndex,
  buildCompanyProfileBrandsBackfillPlan,
  resolveBrandNamesToIds,
} from "../lib/company-profile-brands-backfill.js";

const brandRecords = [
  { id: "recBrand1", fields: { "Brand Name": "Hilton Garden Inn", "Parent Company": "Hilton" } },
  { id: "recBrand2", fields: { "Brand Name": "Hampton by Hilton", "Parent Company": "Hilton" } },
  { id: "recBrand3", fields: { "Brand Name": "Marriott Hotels", "Parent Company": "Marriott International" } },
];

const index = buildBrandBasicsIndex(brandRecords);
const { resolved, unresolved, ambiguous } = resolveBrandNamesToIds(
  ["Hilton Garden Inn", "Unknown Brand X"],
  index.nameToIds
);
assert.deepEqual(resolved, ["recBrand1"]);
assert.equal(unresolved.length, 1);
assert.equal(ambiguous.length, 0);

const dupIndex = buildBrandBasicsIndex([
  { id: "recA", fields: { "Brand Name": "Duplicate" } },
  { id: "recB", fields: { "Brand Name": "Duplicate" } },
]);
const dup = resolveBrandNamesToIds(["Duplicate"], dupIndex.nameToIds);
assert.equal(dup.resolved.length, 0);
assert.equal(dup.ambiguous.length, 1);

const plan = buildCompanyProfileBrandsBackfillPlan(
  {
    id: "recCo1",
    fields: {
      "Company Name": "Marriott International",
      "Company Type": "Hotel Brands (Franchise)",
      "Brand Name (from Brands You Operate / Support)": ["Marriott Hotels"],
    },
  },
  {
    ...index,
    operatorBasicsByCompanyKey: new Map(),
    operatorProfileByCompanyKey: new Map(),
  }
);
assert.ok(plan.mergedIds.includes("recBrand3"));
assert.ok(plan.hasChange);

console.log("company-profile-brands-backfill: OK");
