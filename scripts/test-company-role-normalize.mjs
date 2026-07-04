import assert from "node:assert/strict";
import {
  COMPANY_ROLE_AIRTABLE_FIELD,
  COMPANY_ROLE_FORM_TO_AIRTABLE,
  PARTNER_DIRECTORY_COMPANY_ROLE_FILTERS,
  companyRoleFilterLabel,
  companyRoleFromAirtableFields,
  companyRoleFromEcosystemField,
  getAirtableFieldValue,
  normalizeCompanyRoleToForm,
} from "../lib/company-role-normalize.js";

const fields = (value) => ({ [COMPANY_ROLE_AIRTABLE_FIELD]: value });

for (const [formKey, airtableVal] of Object.entries(COMPANY_ROLE_FORM_TO_AIRTABLE)) {
  assert.equal(normalizeCompanyRoleToForm(airtableVal), formKey);
  assert.equal(companyRoleFromEcosystemField(fields(airtableVal)), formKey);
  assert.equal(companyRoleFromAirtableFields(fields(airtableVal)), formKey);
}

assert.equal(
  normalizeCompanyRoleToForm("Both - We both represent a brand and operate hotels"),
  "Both"
);
assert.equal(
  normalizeCompanyRoleToForm("We both represent a brand and operate hotels"),
  "Both"
);

assert.equal(
  getAirtableFieldValue(
    { "Company\u2019s role in the hotel ecosystem": COMPANY_ROLE_FORM_TO_AIRTABLE.Brand },
    COMPANY_ROLE_AIRTABLE_FIELD
  ),
  COMPANY_ROLE_FORM_TO_AIRTABLE.Brand
);

assert.equal(companyRoleFilterLabel("Brand"), "Brand (Franchise / Licensing)");
assert.equal(companyRoleFilterLabel("Both"), "Brand & Operator (Both)");
assert.equal(companyRoleFilterLabel(""), "All Roles");
assert.equal(PARTNER_DIRECTORY_COMPANY_ROLE_FILTERS.length, 7);

console.log("test-company-role-normalize: ok");
