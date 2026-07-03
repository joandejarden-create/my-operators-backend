/**
 * Owner workspace: BDR row must link to a deal the user may access.
 * Brand workspace: pass-through (brand BDR security deferred to Batch 2B).
 */

import { BDR_TABLE } from "../api/schemas/brand-deal-request-fields.js";
import { firstLinkedDealIdFromBdrFields } from "../lib/dealality/bdr-record-access.js";
import { assertOwnerDealAccess, ownerMustEnforceDealAccess } from "../lib/dealality/owner-deal-id-access.js";

export async function requireOwnerBdrRecordAccess(req, res, next) {
  try {
    const requestId = req.params.requestId;
    if (!requestId || !requestId.startsWith("rec")) {
      return res.status(400).json({ success: false, error: "Valid requestId required" });
    }

    const u = req.dealalityUser;
    if (!u) {
      return res.status(500).json({ success: false, error: "User context missing." });
    }

    if (!ownerMustEnforceDealAccess(u)) {
      return next();
    }

    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) {
      return res.status(500).json({ success: false, error: "Airtable credentials not configured" });
    }

    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(BDR_TABLE)}/${encodeURIComponent(requestId)}`;
    const airtableRes = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
    if (airtableRes.status === 404) {
      return res.status(404).json({ success: false, error: "Brand Deal Request not found" });
    }
    if (!airtableRes.ok) {
      return res.status(502).json({ success: false, error: "Failed to load request for access check" });
    }

    const data = await airtableRes.json();
    const dealId = firstLinkedDealIdFromBdrFields(data.fields);
    if (!dealId) {
      return res.status(404).json({ success: false, error: "Deal not found for request" });
    }

    const check = await assertOwnerDealAccess(u, dealId, { baseId, apiKey });
    if (!check.ok) {
      return res.status(check.status).json({
        success: false,
        error: check.error || "forbidden",
        message: check.message || "You do not have access to this deal request.",
      });
    }

    req.bdrRecordFields = data.fields;
    req.bdrDealId = dealId;
    return next();
  } catch (err) {
    console.error("[requireOwnerBdrRecordAccess]", err);
    return res.status(500).json({ success: false, error: err.message || "Internal Server Error" });
  }
}
