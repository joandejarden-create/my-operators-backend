/**
 * Ensure the authenticated user may access the deal in req.params.recordId.
 */

import { DEALS_TABLE } from "../api/schemas/deal-setup-fields.js";
import { dealRecordAllowedForUser } from "../lib/dealality/deal-record-access.js";

export async function requireDealRecordAccess(req, res, next) {
  try {
    const recordId = req.params.recordId;
    if (!recordId || !recordId.startsWith("rec")) {
      return res.status(400).json({
        success: false,
        error: "Valid record ID is required",
      });
    }

    const u = req.dealalityUser;
    if (!u) {
      return res.status(500).json({
        ok: false,
        success: false,
        error: "server_error",
        message: "User context missing.",
      });
    }

    if (u.isAdmin) {
      return next();
    }

    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) {
      return res.status(500).json({
        success: false,
        error: "Airtable credentials not configured",
      });
    }

    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(DEALS_TABLE)}/${encodeURIComponent(recordId)}`;
    const airtableRes = await fetch(url, {
      headers: { Authorization: "Bearer " + apiKey },
    });

    if (airtableRes.status === 404) {
      return res.status(404).json({ success: false, error: "Deal not found" });
    }
    if (!airtableRes.ok) {
      return res.status(502).json({
        success: false,
        error: "Failed to load deal for access check",
      });
    }

    const data = await airtableRes.json();
    if (!dealRecordAllowedForUser(data.fields, u)) {
      return res.status(403).json({
        ok: false,
        success: false,
        error: "forbidden",
        message: "You do not have access to this deal.",
      });
    }

    req.dealRecordFields = data.fields;
    req.dealRecordId = recordId;
    return next();
  } catch (err) {
    console.error("[requireDealRecordAccess]", err);
    return res.status(500).json({
      success: false,
      error: err.message || "Internal Server Error",
    });
  }
}
