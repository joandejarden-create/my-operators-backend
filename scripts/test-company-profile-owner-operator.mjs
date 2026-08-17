import assert from "node:assert/strict";
import { formToAirtableFields } from "../api/company-profile.js";
import {
  airtableCompanyTypeToFormKey,
  applyProfileStatusDefaults,
  capabilitiesToTags,
  deriveCompanyClassificationFromTags,
  finalizeCompanyProfileFieldsForAirtableWrite,
  fromAirtableCompanyType,
  isInternalCompanyTypeKey,
  mergeOwnerOperatorExtensionFields,
  suggestThirdPartyManagementAvailability,
  toAirtableCompanyType,
  toAirtableOperatingModel,
  toAirtableThirdPartyManagement,
} from "../lib/company-profile-owner-operator-fields.js";
import { COMPANY_TYPE_OWNER_OPERATOR_AIRTABLE } from "../lib/company-type-normalize.js";

const CANONICAL = COMPANY_TYPE_OWNER_OPERATOR_AIRTABLE;

/** Simulates POST/PATCH: formToAirtableFields + pre-Airtable finalize (same as create/update). */
function simulateFullCompanyProfileSave(body) {
  const fields = formToAirtableFields(body);
  return finalizeCompanyProfileFieldsForAirtableWrite(
    { ...fields, companyType: body.companyType },
    { loud: false }
  );
}

assert.equal(toAirtableCompanyType("owner_operator"), CANONICAL);
assert.equal(toAirtableCompanyType(["owner_operator"]), CANONICAL);
assert.equal(toAirtableCompanyType("hotel_owner_operator"), CANONICAL);
assert.equal(toAirtableCompanyType("Owner-Operator"), CANONICAL);
assert.equal(toAirtableCompanyType("Hotel Owner Operator"), CANONICAL);
assert.equal(toAirtableCompanyType("Hotel Owner - Operator"), CANONICAL);
assert.equal(toAirtableCompanyType("Hotel Owner"), "Hotel Owner");
assert.notEqual(toAirtableCompanyType("owner_operator"), "owner_operator");
assert.equal(fromAirtableCompanyType("Hotel Owner - Operator"), "owner_operator");
assert.equal(isInternalCompanyTypeKey("owner_operator"), true);
assert.equal(isInternalCompanyTypeKey(CANONICAL), false);

function assertNeverInternalCompanyType(fields) {
  const ct = fields["Company Type"];
  assert.notEqual(ct, "owner_operator", "Company Type must not be owner_operator");
  assert.notEqual(ct, "hotel_owner_operator");
  assert.notEqual(ct, "OWNER_OPERATOR");
  if (ct) assert.equal(isInternalCompanyTypeKey(ct), false);
}

const fields1 = {};
mergeOwnerOperatorExtensionFields({ companyType: "owner_operator" }, fields1, {});
assert.equal(fields1["Company Type"], CANONICAL);
assertNeverInternalCompanyType(fields1);

const payloadTags = simulateFullCompanyProfileSave({
  companyType: "owner_operator",
  companyCapabilities: ["Owns Hotels", "Operates Third-Party Hotels"],
  deriveCompanyTypeFromCapabilities: "1",
  deriveWorkspaceFromCapabilities: "1",
});
assert.equal(payloadTags["Company Type"], CANONICAL);
assert.deepEqual(payloadTags["Workspace Access"], ["Owner", "Operator"]);
assert.equal(payloadTags["Third-Party Management Availability"], "Yes");
assertNeverInternalCompanyType(payloadTags);
assert.equal(Object.prototype.hasOwnProperty.call(payloadTags, "companyType"), false);

const payloadOwn = simulateFullCompanyProfileSave({
  companyType: "owner_operator",
  companyCapabilities: ["owns_assets", "operates_own"],
  deriveCompanyTypeFromCapabilities: "1",
  deriveWorkspaceFromCapabilities: "1",
});
assert.equal(payloadOwn["Company Type"], CANONICAL);
assert.equal(payloadOwn["Third-Party Management Availability"], "No");
assertNeverInternalCompanyType(payloadOwn);

const payloadMultipart = simulateFullCompanyProfileSave({
  companyType: ["owner_operator"],
  companyCapabilities: ["owns_assets", "operates_third_party"],
  deriveCompanyTypeFromCapabilities: "1",
  deriveWorkspaceFromCapabilities: "1",
});
assert.equal(payloadMultipart["Company Type"], CANONICAL);
assertNeverInternalCompanyType(payloadMultipart);

const payloadHotelOwnerOp = formToAirtableFields({
  companyType: "hotel_owner_operator",
});
finalizeCompanyProfileFieldsForAirtableWrite(payloadHotelOwnerOp, { loud: false });
assert.equal(payloadHotelOwnerOp["Company Type"], CANONICAL);
assertNeverInternalCompanyType(payloadHotelOwnerOp);

const leakAttempt = finalizeCompanyProfileFieldsForAirtableWrite(
  { "Company Type": "owner_operator", companyType: "owner_operator" },
  { loud: false }
);
assert.equal(leakAttempt["Company Type"], CANONICAL);
assert.equal(leakAttempt.companyType, undefined);
assertNeverInternalCompanyType(leakAttempt);

const tagsOwnerOp = ["Owns Hotels", "Operates Own Portfolio"];
assert.equal(
  suggestThirdPartyManagementAvailability(tagsOwnerOp, ""),
  "No"
);
assert.deepEqual(
  capabilitiesToTags(["Owns Hotels", "Operates Third-Party Hotels"]),
  ["Owns Hotels", "Operates Third-Party Hotels"]
);

const statuses = applyProfileStatusDefaults(CANONICAL, {
  ownerProfileStatus: "Complete",
  operatorProfileStatus: "",
});
assert.equal(statuses.ownerProfileStatus, "Complete");
assert.equal(statuses.operatorProfileStatus, "In Progress");

assert.equal(toAirtableOperatingModel("Own-and-operate only"), "Own-and-Operate Only");
assert.equal(toAirtableOperatingModel("own_and_operate_only"), "Own-and-Operate Only");
assert.equal(toAirtableThirdPartyManagement("case-by-case"), "Case-by-Case");
assert.notEqual(toAirtableThirdPartyManagement("case-by-case"), "Case-by-case");

const payloadExactTags = formToAirtableFields({
  companyCapabilities: ["owns_assets", "operates_third_party"],
  deriveCompanyTypeFromCapabilities: "1",
  deriveWorkspaceFromCapabilities: "1",
});
assert.deepEqual(payloadExactTags["Company Type Tags"], [
  "Owns Hotels",
  "Operates Third-Party Hotels",
]);
assert.equal(payloadExactTags["Third-Party Management Availability"], "Yes");
assert.equal(payloadExactTags["Operating Model"], "Mixed Owner/Operator Model");

const payloadOwnPortfolio = formToAirtableFields({
  companyCapabilities: ["owns_assets", "operates_own"],
  deriveCompanyTypeFromCapabilities: "1",
  deriveWorkspaceFromCapabilities: "1",
});
assert.equal(payloadOwnPortfolio["Third-Party Management Availability"], "No");

console.log("test-company-profile-owner-operator: ok");
