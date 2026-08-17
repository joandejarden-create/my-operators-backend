import { sendWelcomeEmail } from "../lib/email.js";
import { upsertSignupUserRecord } from "../lib/signup-airtable-upsert.js";
import {
  CURRENT_TERMS_VERSION,
  recordSignupTermsAcceptance,
} from "../lib/signup-terms-acceptance.js";

/**
 * POST /api/signup — create/update User in Airtable and send standardized welcome email.
 * Used by /signup and /signup-temp. Body: firstName, lastName, companyName, title, email, phone, companyType|role, reasonToJoin, howDidYouHear, agreeWithTerms, termsVersion, termsAcceptedAt, memberstackId (optional)
 */
export default async function signup(req, res) {
  try {
    if (req.method !== "POST") {
      return res.status(405).json({ error: "Method not allowed" });
    }

    const body = req.body || {};
    const normalizedEmail = typeof body.email === "string" ? body.email.trim().toLowerCase() : "";
    if (!normalizedEmail) {
      return res.status(400).json({ error: "Email is required" });
    }

    const agreed =
      body.agreeWithTerms === true ||
      body.agreeWithTerms === "true" ||
      body.agreeWithTerms === "on" ||
      body.agreeWithTerms === "Yes" ||
      body.agreeWithTerms === 1;
    if (!agreed) {
      return res.status(400).json({
        error: "You must agree to the Terms of Service and Privacy Policy",
      });
    }

    const termsAcceptedAt =
      typeof body.termsAcceptedAt === "string" && body.termsAcceptedAt.trim()
        ? body.termsAcceptedAt.trim()
        : new Date().toISOString();
    const termsVersion =
      typeof body.termsVersion === "string" && body.termsVersion.trim()
        ? body.termsVersion.trim()
        : CURRENT_TERMS_VERSION;

    const memberstackId =
      typeof body.memberstackId === "string" && body.memberstackId.trim()
        ? body.memberstackId.trim()
        : "signup-temp";

    let record;
    try {
      ({ record } = await upsertSignupUserRecord(
        {
          ...body,
          email: normalizedEmail,
          agreeWithTerms: true,
          termsAcceptedAt,
          termsVersion,
        },
        memberstackId
      ));
    } catch (err) {
      if (err.statusCode === 400) {
        return res.status(400).json({ error: err.message || "Bad request" });
      }
      throw err;
    }

    // Best-effort Commercial Acceptances row (do not fail signup if this errors)
    let termsAcceptanceId = null;
    try {
      const acceptance = await recordSignupTermsAcceptance({
        email: normalizedEmail,
        firstName: body.firstName,
        lastName: body.lastName,
        companyName: body.companyName,
        memberTypeHint: body.companyType || body.role,
        acceptedAtIso: termsAcceptedAt,
        termsVersion,
        usersRecordId: record?.id,
      });
      termsAcceptanceId = acceptance?.id || null;
    } catch (err) {
      console.error("Signup terms acceptance record failed:", err.message || err);
    }

    const firstNameForEmail = typeof body.firstName === "string" ? body.firstName.trim() : "";
    sendWelcomeEmail(normalizedEmail, { firstName: firstNameForEmail })
      .then((r) => {
        if (!r.sent && r.error) console.error("Signup welcome email:", r.error);
      })
      .catch((e) => console.error("Signup welcome email error:", e));

    return res.status(200).json({
      id: record.id,
      ok: true,
      termsAccepted: true,
      termsVersion,
      termsAcceptanceId,
    });
  } catch (err) {
    console.error("Error in signup:", err);
    const message = err.message || "Internal Server Error";
    const status = err.statusCode === 422 ? 422 : 500;
    return res.status(status).json({ error: "Signup failed", details: message });
  }
}
