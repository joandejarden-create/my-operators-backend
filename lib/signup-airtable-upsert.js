import Airtable from "airtable";
import {
  AUTH_ROLE_HINT_FIELD_CANDIDATES,
  buildSignupUsersPatch,
  resolveSignupRoleHint,
  writeUsersRecordWithFieldFallback,
} from "./airtable-users-protected-patch.js";

export { resolveSignupRoleHint };

function getBase() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) {
    const err = new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");
    err.statusCode = 500;
    throw err;
  }
  return new Airtable({ apiKey }).base(baseId);
}

/** Users table — same ids as api/intake-user.js / api/signup.js */
export const USERS_SIGNUP = {
  table: "tbl6shiyz2wdUqE5F",
  uniqueWebflowId: "flddTfp7oLdcPwBIC",
  email: "fldBl7IXEscwkMhnZ",
  firstName: "fldG5nbAijQkUVSzr",
  lastName: "fldV0g50iRB8J46Hh",
};

/**
 * Create or update a Users row from the public signup form payload.
 * Does not overwrite Airtable authorization fields on re-signup.
 *
 * @param {object} body - firstName, lastName, companyName, title, email, phone, companyType|role, reasonToJoin, howDidYouHear, agreeWithTerms, termsVersion, termsAcceptedAt
 * @param {string} uniqueWebflowId - Memberstack mem_… id or placeholder (e.g. signup-temp, signup-pilot)
 * @param {{ statusOnWrite?: string }} [options] - Status on create when field exists; optional explicit status on write
 * @returns {Promise<{ record: import("airtable").Record, skipped?: string[] }>}
 */
export async function upsertSignupUserRecord(body, uniqueWebflowId, options = {}) {
  const {
    firstName,
    lastName,
    companyName,
    title,
    email,
    phone,
    reasonToJoin,
    howDidYouHear,
    agreeWithTerms,
    termsVersion,
    termsAcceptedAt,
  } = body || {};

  const normalizedEmail = typeof email === "string" ? email.trim().toLowerCase() : "";
  if (!normalizedEmail) {
    const err = new Error("Email is required");
    err.statusCode = 400;
    throw err;
  }

  const roleHint = resolveSignupRoleHint(body);
  const companyNameVal = typeof companyName === "string" ? companyName.trim() : "";
  const titleVal = typeof title === "string" ? title.trim() : "";
  const phoneVal = typeof phone === "string" ? phone.trim() : "";
  const reasonToJoinVal = typeof reasonToJoin === "string" ? reasonToJoin.trim() : "";
  const howDidYouHearVal = typeof howDidYouHear === "string" ? howDidYouHear.trim() : "";
  const webflowId = typeof uniqueWebflowId === "string" ? uniqueWebflowId.trim() : "signup-temp";
  const agreed =
    agreeWithTerms === true ||
    agreeWithTerms === "true" ||
    agreeWithTerms === "on" ||
    agreeWithTerms === "Yes" ||
    agreeWithTerms === 1;

  const coreFields = {
    [USERS_SIGNUP.email]: normalizedEmail,
    [USERS_SIGNUP.uniqueWebflowId]: webflowId,
    [USERS_SIGNUP.firstName]: typeof firstName === "string" ? firstName.trim() : "",
    [USERS_SIGNUP.lastName]: typeof lastName === "string" ? lastName.trim() : "",
  };

  const onboardingFields = {
    ...coreFields,
    "Company Name": companyNameVal,
    Title: titleVal,
    "Phone Number": phoneVal,
    "Reason to Join Platform": reasonToJoinVal,
    "How Did You Hear About Us": howDidYouHearVal,
  };

  if (agreed) {
    onboardingFields["Terms & Privacy Accepted"] = true;
    onboardingFields["Terms Accepted At"] =
      typeof termsAcceptedAt === "string" && termsAcceptedAt.trim()
        ? termsAcceptedAt.trim()
        : new Date().toISOString();
    onboardingFields["Terms Version Accepted"] =
      typeof termsVersion === "string" && termsVersion.trim()
        ? termsVersion.trim()
        : process.env.DEALALITY_TERMS_VERSION || "2026-07-16";
  }

  const statusFieldName = (process.env.SIGNUP_AIRTABLE_STATUS_FIELD || "").trim();
  const statusOnWrite = options.statusOnWrite || "";
  const setPendingOnCreate =
    !!statusFieldName &&
    process.env.SIGNUP_AIRTABLE_SET_PENDING_STATUS !== "false" &&
    !statusOnWrite;
  const pendingStatus = (process.env.SIGNUP_AIRTABLE_PENDING_STATUS || "Pending").trim();

  const escapedEmail = normalizedEmail.replace(/\\/g, "\\\\").replace(/'/g, "\\'");
  const base = getBase();
  const existing = await base(USERS_SIGNUP.table)
    .select({ filterByFormula: `{Email} = '${escapedEmail}'`, maxRecords: 1 })
    .firstPage();

  const isCreate = existing.length === 0;
  const existingFields = existing[0]?.fields || {};

  const { patch, skipped } = buildSignupUsersPatch(existingFields, onboardingFields, {
    isCreate,
    roleHint,
  });

  if (statusFieldName) {
    if (statusOnWrite) {
      patch[statusFieldName] = statusOnWrite;
    } else if (isCreate && setPendingOnCreate && pendingStatus) {
      patch[statusFieldName] = pendingStatus;
    }
  }

  Object.keys(patch).forEach((k) => {
    if (patch[k] === undefined || patch[k] === "") delete patch[k];
  });

  if (!Object.keys(patch).length && !isCreate) {
    return { record: existing[0], skipped, isCreate: false };
  }

  const record = await writeUsersRecordWithFieldFallback(base, USERS_SIGNUP.table, {
    isCreate,
    recordId: existing[0]?.id,
    fields: patch,
  });

  if (process.env.NODE_ENV !== "production" && skipped.length) {
    console.info("[signup-airtable-upsert] preserved fields:", skipped.join(", "));
  }

  return { record, skipped, isCreate };
}

export { AUTH_ROLE_HINT_FIELD_CANDIDATES, buildSignupUsersPatch };
