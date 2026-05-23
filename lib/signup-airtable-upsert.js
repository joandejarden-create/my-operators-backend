import Airtable from "airtable";

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);

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
 * @param {object} body - firstName, lastName, companyName, title, email, phone, companyType, reasonToJoin, howDidYouHear
 * @param {string} uniqueWebflowId - Memberstack mem_… id or placeholder (e.g. signup-temp, signup-pilot)
 * @param {{ statusOnWrite?: string }} [options] - e.g. Status on create/update when field exists in base
 * @returns {Promise<{ record: import("airtable").Record }>}
 */
export async function upsertSignupUserRecord(body, uniqueWebflowId, options = {}) {
  const {
    firstName,
    lastName,
    companyName,
    title,
    email,
    phone,
    companyType,
    reasonToJoin,
    howDidYouHear,
  } = body || {};

  const normalizedEmail = typeof email === "string" ? email.trim().toLowerCase() : "";
  if (!normalizedEmail) {
    const err = new Error("Email is required");
    err.statusCode = 400;
    throw err;
  }

  const companyNameVal = typeof companyName === "string" ? companyName.trim() : "";
  const titleVal = typeof title === "string" ? title.trim() : "";
  const phoneVal = typeof phone === "string" ? phone.trim() : "";
  const companyTypeVal = typeof companyType === "string" ? companyType.trim() : "";
  const reasonToJoinVal = typeof reasonToJoin === "string" ? reasonToJoin.trim() : "";
  const howDidYouHearVal = typeof howDidYouHear === "string" ? howDidYouHear.trim() : "";

  const coreFields = {
    [USERS_SIGNUP.email]: normalizedEmail,
    [USERS_SIGNUP.uniqueWebflowId]: uniqueWebflowId,
    [USERS_SIGNUP.firstName]: typeof firstName === "string" ? firstName.trim() : "",
    [USERS_SIGNUP.lastName]: typeof lastName === "string" ? lastName.trim() : "",
  };

  const extendedFields = {
    ...coreFields,
    "Company Name": companyNameVal,
    Title: titleVal,
    "Phone Number": phoneVal,
    "User Type": companyTypeVal,
    "Reason to Join Platform": reasonToJoinVal,
    "How Did You Hear About Us": howDidYouHearVal,
  };

  const statusFieldName = (process.env.SIGNUP_AIRTABLE_STATUS_FIELD || "").trim();
  const statusOnWrite = options.statusOnWrite || "";
  const setPendingOnCreate =
    !!statusFieldName &&
    process.env.SIGNUP_AIRTABLE_SET_PENDING_STATUS !== "false" &&
    !statusOnWrite;
  const pendingStatus = (process.env.SIGNUP_AIRTABLE_PENDING_STATUS || "Pending").trim();

  const escapedEmail = normalizedEmail.replace(/\\/g, "\\\\").replace(/'/g, "\\'");
  const existing = await base(USERS_SIGNUP.table)
    .select({ filterByFormula: `{Email} = '${escapedEmail}'`, maxRecords: 1 })
    .firstPage();

  const tryWrite = async (fieldsToUse, isCreate, { includeStatus = true } = {}) => {
    const payload = { ...fieldsToUse };
    if (includeStatus && statusFieldName) {
      if (statusOnWrite) {
        payload[statusFieldName] = statusOnWrite;
      } else if (isCreate && setPendingOnCreate && pendingStatus) {
        payload[statusFieldName] = pendingStatus;
      }
    }
    if (existing.length > 0) {
      return await base(USERS_SIGNUP.table).update(existing[0].id, payload, { typecast: true });
    }
    return await base(USERS_SIGNUP.table).create(payload, { typecast: true });
  };

  const isCreate = existing.length === 0;
  const isFieldError = (err) =>
    err.statusCode === 422 &&
    err.message &&
    (err.message.includes("Unknown field") || err.message.includes("invalid"));

  try {
    const record = await tryWrite(extendedFields, isCreate);
    return { record };
  } catch (err) {
    if (!isFieldError(err)) throw err;
    const statusRejected = statusFieldName && String(err.message).includes(statusFieldName);
    try {
      const record = await tryWrite(extendedFields, isCreate, { includeStatus: !statusRejected });
      return { record };
    } catch (err2) {
      if (!isFieldError(err2)) throw err2;
      const record = await tryWrite(coreFields, isCreate, { includeStatus: false });
      return { record };
    }
  }
}
