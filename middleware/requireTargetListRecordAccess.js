/**
 * Ensure target list record belongs to a deal the owner may access.
 */

import { TARGET_LIST_TABLE } from "../api/schemas/target-list-fields.js";
import { assertOwnerDealAccess } from "../lib/dealality/owner-deal-id-access.js";

function dealIdFromTargetFields(fields) {
  const raw = fields?.Deal_ID;
  if (Array.isArray(raw) && raw[0]) return String(raw[0]).trim();
  return null;
}

export async function requireTargetListRecordAccess(req, res, next) {
  try {
    const targetId = req.params.targetId;
    if (!targetId || !targetId.startsWith("rec")) {
      return res.status(400).json({ success: false, error: "Valid target record ID is required" });
    }

    const u = req.dealalityUser;
    if (!u) {
      return res.status(500).json({ success: false, error: "User context missing." });
    }

    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) {
      return res.status(500).json({ success: false, error: "Airtable credentials not configured" });
    }

    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(TARGET_LIST_TABLE)}/${encodeURIComponent(targetId)}`;
    const airtableRes = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
    if (airtableRes.status === 404) {
      return res.status(404).json({ success: false, error: "Target not found" });
    }
    if (!airtableRes.ok) {
      return res.status(502).json({ success: false, error: "Failed to load target for access check" });
    }

    const data = await airtableRes.json();
    const dealId = dealIdFromTargetFields(data.fields);
    if (!dealId) {
      return res.status(404).json({ success: false, error: "Target has no linked deal" });
    }

    const check = await assertOwnerDealAccess(u, dealId, { baseId, apiKey });
    if (!check.ok) {
      return res.status(check.status).json({
        success: false,
        error: check.error || "forbidden",
        message: check.message || "You do not have access to this target.",
      });
    }

    req.targetListRecordFields = data.fields;
    req.targetListDealId = dealId;
    return next();
  } catch (err) {
    console.error("[requireTargetListRecordAccess]", err);
    return res.status(500).json({ success: false, error: err.message || "Internal Server Error" });
  }
}
