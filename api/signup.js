import Airtable from "airtable";
import { sendWelcomeEmail } from "../lib/email.js";

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);

// Users table — only fields that exist in intake-user / intake-deal (avoids 422)
const USERS = {
  table: "tbl6shiyz2wdUqE5F",
  uniqueWebflowId: "flddTfp7oLdcPwBIC",
  email: "fldBl7IXEscwkMhnZ",
  firstName: "fldG5nbAijQkUVSzr",
  lastName: "fldV0g50iRB8J46Hh",
};

/**
 * POST /api/signup — create/update User in Airtable and send standardized welcome email.
 * Used by /signup and /signup-temp. Body: firstName, lastName, companyName, title, email, phone, companyType, reasonToJoin, howDidYouHear
 */
export default async function signup(req, res) {
  try {
    if (req.method !== "POST") {
      return res.status(405).json({ error: "Method not allowed" });
    }

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
    } = req.body || {};

    const normalizedEmail = typeof email === "string" ? email.trim().toLowerCase() : "";
    if (!normalizedEmail) {
      return res.status(400).json({ error: "Email is required" });
    }

    const companyNameVal = typeof companyName === "string" ? companyName.trim() : "";
    const titleVal = typeof title === "string" ? title.trim() : "";
    const phoneVal = typeof phone === "string" ? phone.trim() : "";
    const companyTypeVal = typeof companyType === "string" ? companyType.trim() : "";
    const reasonToJoinVal = typeof reasonToJoin === "string" ? reasonToJoin.trim() : "";
    const howDidYouHearVal = typeof howDidYouHear === "string" ? howDidYouHear.trim() : "";

    const coreFields = {
      [USERS.email]: normalizedEmail,
      [USERS.uniqueWebflowId]: "signup-temp",
      [USERS.firstName]: typeof firstName === "string" ? firstName.trim() : "",
      [USERS.lastName]: typeof lastName === "string" ? lastName.trim() : "",
    };

    // Signup fields — use exact Airtable column names from your Users table
    const extendedFields = {
      ...coreFields,
      "Company Name": companyNameVal,
      "Title": titleVal,
      "Phone Number": phoneVal,
      "User Type": companyTypeVal,
      "Reason to Join Platform": reasonToJoinVal,
      "How Did You Hear About Us": howDidYouHearVal,
    };

    // If user already exists by email, update; otherwise create
    const escapedEmail = normalizedEmail.replace(/\\/g, "\\\\").replace(/'/g, "\\'");
    const existing = await base(USERS.table)
      .select({ filterByFormula: `{Email} = '${escapedEmail}'`, maxRecords: 1 })
      .firstPage();

    const tryWrite = async (fieldsToUse) => {
      if (existing.length > 0) {
        return await base(USERS.table).update(existing[0].id, fieldsToUse, { typecast: true });
      }
      return await base(USERS.table).create(fieldsToUse, { typecast: true });
    };

    let record;
    try {
      record = await tryWrite(extendedFields);
    } catch (err) {
      if (err.statusCode === 422 && err.message && (err.message.includes("Unknown field") || err.message.includes("invalid"))) {
        record = await tryWrite(coreFields);
      } else {
        throw err;
      }
    }

    // Send welcome / onboarding email (fire-and-forget; don't block response)
    const firstNameForEmail = typeof firstName === "string" ? firstName.trim() : "";
    sendWelcomeEmail(normalizedEmail, { firstName: firstNameForEmail })
      .then((r) => { if (!r.sent && r.error) console.error("Signup welcome email:", r.error); })
      .catch((e) => console.error("Signup welcome email error:", e));

    return res.status(200).json({ id: record.id, ok: true });
  } catch (err) {
    console.error("Error in signup:", err);
    const message = err.message || "Internal Server Error";
    const status = err.statusCode === 422 ? 422 : 500;
    return res.status(status).json({ error: "Signup failed", details: message });
  }
}
