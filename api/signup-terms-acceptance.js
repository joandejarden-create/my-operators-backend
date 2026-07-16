import Airtable from "airtable";
import {
  CURRENT_TERMS_VERSION,
  recordSignupTermsAcceptance,
} from "../lib/signup-terms-acceptance.js";
import { USERS_SIGNUP } from "../lib/signup-airtable-upsert.js";

/**
 * POST /api/signup-terms-acceptance
 * Lightweight Terms checkbox tracking for Webflow Memberstack signup.
 * Body: email (required), agreeWithTerms, termsVersion, termsAcceptedAt,
 * firstName, lastName, companyName, companyType, memberstackId
 */
export default async function signupTermsAcceptance(req, res) {
  try {
    if (req.method !== "POST") {
      return res.status(405).json({ error: "Method not allowed" });
    }

    const body = req.body || {};
    const email =
      typeof body.email === "string" ? body.email.trim().toLowerCase() : "";
    if (!email) {
      return res.status(400).json({
        ok: false,
        validation: { pass: false, failed: ["email required"] },
        error: "Email is required",
      });
    }

    const agreed =
      body.agreeWithTerms === true ||
      body.agreeWithTerms === "true" ||
      body.agreeWithTerms === "on" ||
      body.agreeWithTerms === "Yes" ||
      body.agreeWithTerms === 1 ||
      body["Agree with Terms"] === true ||
      body["Agree with Terms"] === "true" ||
      body["Agree with Terms"] === "on" ||
      body["Agree with Terms"] === "Yes";

    if (!agreed) {
      return res.status(400).json({
        ok: false,
        validation: { pass: false, failed: ["agreeWithTerms required"] },
        error: "You must agree to the Terms of Service and Privacy Policy",
        fieldMapping: {
          agreeWithTerms: "Terms & Privacy Accepted",
          termsAcceptedAt: "Terms Accepted At",
          termsVersion: "Terms Version Accepted",
        },
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

    const apiKey = process.env.AIRTABLE_API_KEY;
    const baseId = process.env.AIRTABLE_BASE_ID;
    if (!apiKey || !baseId) {
      return res.status(500).json({ error: "Airtable not configured" });
    }

    const base = new Airtable({ apiKey }).base(baseId);
    const escapedEmail = email.replace(/\\/g, "\\\\").replace(/'/g, "\\'");
    const existing = await base(USERS_SIGNUP.table)
      .select({
        maxRecords: 1,
        filterByFormula: `{Email} = '${escapedEmail}'`,
      })
      .firstPage();

    const userFields = {
      "Terms & Privacy Accepted": true,
      "Terms Accepted At": termsAcceptedAt,
      "Terms Version Accepted": termsVersion,
    };

    let usersRecordId = null;
    if (existing[0]) {
      await base(USERS_SIGNUP.table).update([
        { id: existing[0].id, fields: userFields },
      ]);
      usersRecordId = existing[0].id;
    } else {
      const memberstackId =
        typeof body.memberstackId === "string" && body.memberstackId.trim()
          ? body.memberstackId.trim()
          : "webflow-signup-terms";
      const created = await base(USERS_SIGNUP.table).create(
        [
          {
            fields: {
              [USERS_SIGNUP.email]: email,
              [USERS_SIGNUP.uniqueWebflowId]: memberstackId,
              [USERS_SIGNUP.firstName]:
                typeof body.firstName === "string" ? body.firstName.trim() : "",
              [USERS_SIGNUP.lastName]:
                typeof body.lastName === "string" ? body.lastName.trim() : "",
              ...userFields,
            },
          },
        ],
        { typecast: true }
      );
      usersRecordId = created[0]?.id || null;
    }

    let termsAcceptanceId = null;
    try {
      const acceptance = await recordSignupTermsAcceptance({
        email,
        firstName: body.firstName,
        lastName: body.lastName,
        companyName: body.companyName,
        memberTypeHint: body.companyType || body.role,
        acceptedAtIso: termsAcceptedAt,
        termsVersion,
        usersRecordId,
      });
      termsAcceptanceId = acceptance?.id || null;
    } catch (err) {
      console.error(
        "[signup-terms-acceptance] Commercial Acceptances write failed:",
        err.message || err
      );
    }

    if (process.env.NODE_ENV !== "production") {
      console.info("[signup-terms-acceptance]", {
        email,
        termsVersion,
        usersRecordId,
        termsAcceptanceId,
        fieldMapping: {
          agreeWithTerms: "Terms & Privacy Accepted",
          termsAcceptedAt: "Terms Accepted At",
          termsVersion: "Terms Version Accepted",
        },
      });
    }

    return res.status(200).json({
      ok: true,
      validation: { pass: true, failed: [] },
      termsAccepted: true,
      termsVersion,
      termsAcceptedAt,
      usersRecordId,
      termsAcceptanceId,
      fieldMapping: {
        agreeWithTerms: "Terms & Privacy Accepted",
        termsAcceptedAt: "Terms Accepted At",
        termsVersion: "Terms Version Accepted",
      },
      sanitizedPreview: {
        email,
        termsVersion,
        termsAcceptedAt,
        agreeWithTerms: true,
      },
    });
  } catch (err) {
    console.error("Error in signup-terms-acceptance:", err);
    return res.status(500).json({
      ok: false,
      error: "Terms acceptance tracking failed",
      details: err.message || "Internal Server Error",
    });
  }
}
