/**
 * Central field mapping for POST /api/intake/deal.
 * Uses Airtable table IDs and field IDs by default to preserve existing behavior;
 * optional env overrides for future flexibility.
 * See: docs/INTAKE-DEAL-REFACTOR-PLAN.md ("Why field IDs remain the default").
 */

// ---------------------------------------------------------------------------
// Users table (intake: find-or-create user)
// ---------------------------------------------------------------------------
export const INTAKE_USERS_TABLE = process.env.AIRTABLE_INTAKE_USERS_TABLE || "tbl6shiyz2wdUqE5F";

export const INTAKE_USERS_EMAIL = process.env.AIRTABLE_INTAKE_USERS_EMAIL_FIELD || "fldBl7IXEscwkMhnZ";
export const INTAKE_USERS_UNIQUE_WEBFLOW_ID = process.env.AIRTABLE_INTAKE_USERS_UNIQUE_WEBFLOW_ID_FIELD || "flddTfp7oLdcPwBIC";
export const INTAKE_USERS_FIRST_NAME = process.env.AIRTABLE_INTAKE_USERS_FIRST_NAME_FIELD || "fldG5nbAijQkUVSzr";
export const INTAKE_USERS_LAST_NAME = process.env.AIRTABLE_INTAKE_USERS_LAST_NAME_FIELD || "fldV0g50iRB8J46Hh";
export const INTAKE_USERS_COUNTRY = process.env.AIRTABLE_INTAKE_USERS_COUNTRY_FIELD || "fld2LWEer7PgkSCe9";

/** Map for building user record: API body key -> Airtable field ID */
export const INTAKE_USERS_FIELD_MAP = {
  email: INTAKE_USERS_EMAIL,
  uniqueWebflowId: INTAKE_USERS_UNIQUE_WEBFLOW_ID,
  firstName: INTAKE_USERS_FIRST_NAME,
  lastName: INTAKE_USERS_LAST_NAME,
  country: INTAKE_USERS_COUNTRY,
};

// ---------------------------------------------------------------------------
// Deals table (intake: create one deal linked to user)
// ---------------------------------------------------------------------------
export const INTAKE_DEALS_TABLE = process.env.AIRTABLE_INTAKE_DEALS_TABLE || "tblbvSxjiIhXzW6XW";

export const INTAKE_DEALS_NAME = process.env.AIRTABLE_INTAKE_DEALS_NAME_FIELD || "fldkKJzBOBoFCvbnx";
export const INTAKE_DEALS_USER_LINK = process.env.AIRTABLE_INTAKE_DEALS_USER_LINK_FIELD || "fldALlSB9UsnLhgvI";
export const INTAKE_DEALS_STATUS = process.env.AIRTABLE_INTAKE_DEALS_STATUS_FIELD || "fld4cvEAz0k3x8aaU";
export const INTAKE_DEALS_STAGE = process.env.AIRTABLE_INTAKE_DEALS_STAGE_FIELD || "flde0PSEQUhA9Jl5a";

/** Default values for new deal (must exist as options in Airtable). */
export const INTAKE_DEAL_STATUS_DEFAULT = process.env.AIRTABLE_INTAKE_DEAL_STATUS_DEFAULT || "Active";
export const INTAKE_DEAL_STAGE_DEFAULT = process.env.AIRTABLE_INTAKE_DEAL_STAGE_DEFAULT || "Concept";
