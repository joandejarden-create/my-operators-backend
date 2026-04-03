/**
 * POST /api/intake/deal – create or update user and create deal.
 * Uses central mapping (intake-deal-fields.js) and validation (intake-deal-validate.js).
 */

import Airtable from "airtable";
import {
  INTAKE_USERS_TABLE,
  INTAKE_USERS_EMAIL,
  INTAKE_USERS_UNIQUE_WEBFLOW_ID,
  INTAKE_USERS_FIRST_NAME,
  INTAKE_USERS_LAST_NAME,
  INTAKE_USERS_COUNTRY,
  INTAKE_DEALS_TABLE,
  INTAKE_DEALS_NAME,
  INTAKE_DEALS_USER_LINK,
  INTAKE_DEALS_STATUS,
  INTAKE_DEALS_STAGE,
  INTAKE_DEAL_STATUS_DEFAULT,
  INTAKE_DEAL_STAGE_DEFAULT,
} from "./schemas/intake-deal-fields.js";
import { validateIntakeDealPayload, intakePayloadPreview } from "./intake-deal-validate.js";

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);

const DEV = process.env.NODE_ENV !== "production" || process.env.DEBUG_DEAL_SETUP === "true" || process.env.DEBUG_INTAKE_DEAL === "true";

/**
 * Escape a value for safe use inside an Airtable formula string (single-quoted).
 * Escapes backslashes first, then single quotes, so quotes/special chars in user input cannot break the formula.
 */
function escapeAirtableFormulaValue(value) {
  const s = String(value ?? "");
  return s.replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

export default async function dealIntake(req, res) {
  try {
    if (req.method !== "POST") {
      return res.status(405).json({ error: "Method not allowed" });
    }

    const secret = req.headers["x-intake-secret"];
    if (!secret || secret !== process.env.INTAKE_SHARED_SECRET) {
      return res.status(401).json({ error: "Unauthorized" });
    }

    const validation = validateIntakeDealPayload(req.body || {});
    if (!validation.valid) {
      const msg = validation.errors.length
        ? validation.errors.map((e) => (e.field !== "_" ? `${e.field}: ${e.message}` : e.message)).join("; ")
        : "Missing required fields";
      if (DEV) {
        console.warn("[intake-deal] validation failed:", validation.errors);
      }
      return res.status(400).json({ error: msg });
    }

    const { projectName, email, memberstackId, country, firstName, lastName } = validation.payload;

    if (DEV) {
      console.log("[intake-deal] validation ok; field mapping used:", validation.fieldMappingUsed ?? "(none)");
      console.log("[intake-deal] sanitized payload preview:", JSON.stringify(intakePayloadPreview(validation.payload)));
    }

    const filter = `OR({${INTAKE_USERS_UNIQUE_WEBFLOW_ID}}='${escapeAirtableFormulaValue(
      memberstackId || ""
    )}', {${INTAKE_USERS_EMAIL}}='${escapeAirtableFormulaValue(email)}')`;
    const hits = await base(INTAKE_USERS_TABLE).select({ filterByFormula: filter, maxRecords: 1 }).firstPage();

    const userFields = {
      [INTAKE_USERS_EMAIL]: email,
      [INTAKE_USERS_UNIQUE_WEBFLOW_ID]: memberstackId || "",
      ...(firstName ? { [INTAKE_USERS_FIRST_NAME]: firstName } : {}),
      ...(lastName ? { [INTAKE_USERS_LAST_NAME]: lastName } : {}),
      ...(country ? { [INTAKE_USERS_COUNTRY]: country } : {}),
    };

    let userRecord;
    if (hits.length) {
      userRecord = await base(INTAKE_USERS_TABLE).update(hits[0].id, userFields, { typecast: true });
    } else {
      userRecord = await base(INTAKE_USERS_TABLE).create(userFields, { typecast: true });
    }

    const deal = await base(INTAKE_DEALS_TABLE).create(
      {
        [INTAKE_DEALS_NAME]: projectName,
        [INTAKE_DEALS_USER_LINK]: [userRecord.id],
        [INTAKE_DEALS_STATUS]: INTAKE_DEAL_STATUS_DEFAULT,
        [INTAKE_DEALS_STAGE]: INTAKE_DEAL_STAGE_DEFAULT,
      },
      { typecast: true }
    );

    return res.json({ id: deal.id });
  } catch (err) {
    console.error("Error in intake-deal:", err);
    if (DEV && err && err.message) {
      console.error("[intake-deal] module: intake-deal, error:", err.message);
    }
    return res.status(500).json({ error: "Internal Server Error", details: err.message });
  }
}
