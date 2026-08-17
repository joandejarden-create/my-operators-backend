import assert from "node:assert/strict";
import { buildCompanyProfileOwnerOperatorBackfillPatch } from "../lib/company-profile-owner-operator-backfill.js";
import { MAP_CP_AIRTABLE } from "../lib/company-profile-owner-operator-fields.js";

const ownerOp = buildCompanyProfileOwnerOperatorBackfillPatch({
  "Company Name": "Test OO",
  "Company Type": "Hotel Owner - Operator",
});
assert.ok(ownerOp.patch[MAP_CP_AIRTABLE.workspaceAccess]);
assert.deepEqual(ownerOp.patch[MAP_CP_AIRTABLE.workspaceAccess], ["Owner", "Operator"]);

const mgmt = buildCompanyProfileOwnerOperatorBackfillPatch({
  "Company Name": "Test Mgmt",
  "Company Type": "Hotel Management Company",
});
assert.equal(mgmt.patch[MAP_CP_AIRTABLE.operatingModel], "Third-Party Management");
assert.equal(mgmt.patch[MAP_CP_AIRTABLE.thirdPartyManagementAvailability], "Yes");

const fixType = buildCompanyProfileOwnerOperatorBackfillPatch({
  "Company Type": "owner_operator",
});
assert.equal(fixType.patch[MAP_CP_AIRTABLE.companyType], "Hotel Owner - Operator");

const noOverwrite = buildCompanyProfileOwnerOperatorBackfillPatch({
  "Company Type": "Hotel Owner",
  [MAP_CP_AIRTABLE.thirdPartyManagementAvailability]: "No",
  [MAP_CP_AIRTABLE.workspaceAccess]: ["Owner"],
});
assert.equal(noOverwrite.patch[MAP_CP_AIRTABLE.thirdPartyManagementAvailability], undefined);
assert.equal(noOverwrite.patch[MAP_CP_AIRTABLE.workspaceAccess], undefined);

console.log("test-company-profile-owner-operator-backfill: ok");
