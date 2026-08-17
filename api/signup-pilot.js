import { sendWelcomeEmail } from "../lib/email.js";
import {
  buildSignupCustomFields,
  patchMemberstackAfterAirtable,
  provisionMemberstackForSignup,
} from "../lib/memberstack/signup-member.js";
import { upsertSignupUserRecord } from "../lib/signup-airtable-upsert.js";
import {
  CURRENT_TERMS_VERSION,
  recordSignupTermsAcceptance,
} from "../lib/signup-terms-acceptance.js";

/**
 * POST /api/signup-pilot — same Airtable + welcome email as /api/signup, plus optional Memberstack id resolution.
 * Used only by GET /signup-pilot for testing the Railway path while Webflow + Zapier remain unchanged.
 *
 * Response includes `pilot: true` and a small `memberstack` summary (no secrets).
 */
export default async function signupPilot(req, res) {
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
      agreeWithTerms,
      termsVersion,
      termsAcceptedAt,
    } = req.body || {};

    const normalizedEmail = typeof email === "string" ? email.trim().toLowerCase() : "";
    if (!normalizedEmail) {
      return res.status(400).json({ error: "Email is required" });
    }

    const agreed =
      agreeWithTerms === true ||
      agreeWithTerms === "true" ||
      agreeWithTerms === "on" ||
      agreeWithTerms === "Yes" ||
      agreeWithTerms === 1;
    if (!agreed) {
      return res.status(400).json({
        error: "You must agree to the Terms of Service and Privacy Policy",
      });
    }

    const acceptedAt =
      typeof termsAcceptedAt === "string" && termsAcceptedAt.trim()
        ? termsAcceptedAt.trim()
        : new Date().toISOString();
    const version =
      typeof termsVersion === "string" && termsVersion.trim()
        ? termsVersion.trim()
        : CURRENT_TERMS_VERSION;

    const body = {
      firstName,
      lastName,
      companyName,
      title,
      email: normalizedEmail,
      phone,
      companyType,
      reasonToJoin,
      howDidYouHear,
      agreeWithTerms: true,
      termsAcceptedAt: acceptedAt,
      termsVersion: version,
    };

    const customFields = buildSignupCustomFields(body);
    const pilotMode = (process.env.SIGNUP_PILOT_MEMBERSTACK_MODE || "lookup").trim();
    const ms = await provisionMemberstackForSignup({
      email: normalizedEmail,
      firstName,
      lastName,
      customFields,
      password: typeof req.body?.password === "string" ? req.body.password : undefined,
      mode: pilotMode,
    });
    const uniqueWebflowId = ms.memberstackId || "signup-pilot";

    let record;
    try {
      ({ record } = await upsertSignupUserRecord(body, uniqueWebflowId));
    } catch (err) {
      if (err.statusCode === 400) {
        return res.status(400).json({ error: err.message || "Bad request" });
      }
      throw err;
    }

    try {
      await recordSignupTermsAcceptance({
        email: normalizedEmail,
        firstName,
        lastName,
        companyName,
        memberTypeHint: companyType,
        acceptedAtIso: acceptedAt,
        termsVersion: version,
        usersRecordId: record?.id,
      });
    } catch (err) {
      console.error("Signup-pilot terms acceptance record failed:", err.message || err);
    }

    if (ms.memberstackId) {
      await patchMemberstackAfterAirtable(ms.memberstackId, {
        airtableRecordId: record.id,
        body,
      });
    }

    const sendWelcome = process.env.SIGNUP_PILOT_SEND_WELCOME_EMAIL === "true";
    if (sendWelcome) {
      const firstNameForEmail = typeof firstName === "string" ? firstName.trim() : "";
      sendWelcomeEmail(normalizedEmail, { firstName: firstNameForEmail })
        .then((r) => {
          if (!r.sent && r.error) console.error("Signup pilot welcome email:", r.error);
        })
        .catch((e) => console.error("Signup pilot welcome email error:", e));
    }

    return res.status(200).json({
      id: record.id,
      ok: true,
      pilot: true,
      memberstack: {
        linked: Boolean(ms.memberstackId),
        note: ms.memberstackNote,
      },
    });
  } catch (err) {
    console.error("Error in signup-pilot:", err);
    const message = err.message || "Internal Server Error";
    const status = err.statusCode === 422 ? 422 : 500;
    return res.status(status).json({ error: "Signup pilot failed", details: message });
  }
}
