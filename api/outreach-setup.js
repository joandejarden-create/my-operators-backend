/**
 * Outreach Setup API – get/update outreach preferences for a deal.
 * Uses Airtable table "Outreach Setup" with User_ID (per-user default) and optional Deal link.
 */

import {
  OUTREACH_TABLE,
  OUTREACH_USER_FIELD,
  OUTREACH_USER_WEBFLOW_FIELD,
  OUTREACH_DEAL_FIELD,
  map_outreachUiFieldsToAirtable,
  map_outreachAirtableToUiFields,
} from "./schemas/outreach-setup-fields.js";

const DEALS_TABLE = process.env.AIRTABLE_TABLE_DEALS || "Deals";

function hasPerDealLinkField() {
  return Boolean(OUTREACH_DEAL_FIELD);
}

function dealalityUserFromReq(req) {
  return req.dealalityUser || null;
}

/**
 * Fetch all Outreach Setup records and return a Set of linked deal record IDs.
 * Used by getMyDeals to add hasOutreachSetup to each deal.
 */
export async function getAllOutreachDealIds(baseId, apiKey, options = {}) {
  const set = new Set();
  if (!hasPerDealLinkField()) return set;

  let offset = null;
  const tableIdOrName = encodeURIComponent(OUTREACH_TABLE);
  do {
    if (options.beforeRequest) await options.beforeRequest();
    let url = `https://api.airtable.com/v0/${baseId}/${tableIdOrName}?pageSize=100`;
    if (offset) url += "&offset=" + encodeURIComponent(offset);
    const res = await fetch(url, { headers: { Authorization: "Bearer " + apiKey } });
    const data = await res.json();
    if (data.error) return set;
    const records = data.records || [];
    for (const rec of records) {
      const raw = (rec.fields || {})[OUTREACH_DEAL_FIELD];
      if (Array.isArray(raw)) {
        for (const item of raw) {
          const id = typeof item === "string" ? item : item?.id;
          if (id && typeof id === "string" && id.startsWith("rec")) set.add(id);
        }
      }
    }
    offset = data.offset || null;
  } while (offset);
  return set;
}

async function listOutreachRecords(baseId, apiKey, { filterFormula, maxRecords } = {}) {
  const tableIdOrName = encodeURIComponent(OUTREACH_TABLE);
  let url = `https://api.airtable.com/v0/${baseId}/${tableIdOrName}?pageSize=100`;
  if (filterFormula) url += `&filterByFormula=${encodeURIComponent(filterFormula)}`;
  if (maxRecords) url += `&maxRecords=${maxRecords}`;
  const res = await fetch(url, { headers: { Authorization: "Bearer " + apiKey } });
  const data = await res.json();
  if (data.error) {
    const err = new Error(data.error.message || "Airtable API error");
    err.airtable = data.error;
    throw err;
  }
  return data.records || [];
}

function userFilterFormula(userRecordId) {
  const lit = String(userRecordId).replace(/"/g, '\\"');
  return `FIND("${lit}", ARRAYJOIN({${OUTREACH_USER_FIELD}}))`;
}

function dealFilterFormula(dealRecordId) {
  const lit = String(dealRecordId).replace(/"/g, '\\"');
  return `FIND("${lit}", ARRAYJOIN({${OUTREACH_DEAL_FIELD}}))`;
}

/**
 * User-scoped default outreach (no per-deal Deal link, or Deal link empty).
 */
async function findUserDefaultOutreachRecord(baseId, apiKey, userRecordId) {
  if (!userRecordId) return null;
  const records = await listOutreachRecords(baseId, apiKey, {
    filterFormula: userFilterFormula(userRecordId),
    maxRecords: 50,
  });
  if (!hasPerDealLinkField()) {
    return records[0] ? { record: records[0] } : null;
  }
  for (const rec of records) {
    const raw = (rec.fields || {})[OUTREACH_DEAL_FIELD];
    if (!Array.isArray(raw) || raw.length === 0) return { record: rec };
  }
  return null;
}

async function findDealOutreachRecord(baseId, apiKey, dealRecordId, userRecordId) {
  if (!hasPerDealLinkField() || !dealRecordId) return null;
  const records = await listOutreachRecords(baseId, apiKey, {
    filterFormula: dealFilterFormula(dealRecordId),
    maxRecords: 1,
  });
  const record = records[0];
  if (!record) return null;
  if (userRecordId) {
    const linkedUsers = record.fields?.[OUTREACH_USER_FIELD];
    if (Array.isArray(linkedUsers) && linkedUsers.length && !linkedUsers.includes(userRecordId)) {
      return null;
    }
  }
  return { record };
}

function identityFieldsForUser(dealalityUser) {
  const patch = {};
  if (dealalityUser?.userRecordId) {
    patch[OUTREACH_USER_FIELD] = [dealalityUser.userRecordId];
  }
  if (dealalityUser?.memberstackId) {
    patch[OUTREACH_USER_WEBFLOW_FIELD] = String(dealalityUser.memberstackId).trim();
  }
  return patch;
}

async function writeOutreachRecord(baseId, apiKey, { existingId, fields }) {
  const tableIdOrName = encodeURIComponent(OUTREACH_TABLE);
  if (existingId) {
    const patchUrl = `https://api.airtable.com/v0/${baseId}/${tableIdOrName}/${encodeURIComponent(existingId)}`;
    const patchRes = await fetch(patchUrl, {
      method: "PATCH",
      headers: {
        Authorization: "Bearer " + apiKey,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ fields }),
    });
    return patchRes.json();
  }
  const createUrl = `https://api.airtable.com/v0/${baseId}/${tableIdOrName}`;
  const createRes = await fetch(createUrl, {
    method: "POST",
    headers: {
      Authorization: "Bearer " + apiKey,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields, typecast: true }),
  });
  return createRes.json();
}

/**
 * GET /api/my-deals/outreach-default
 */
export async function getOutreachDefault(req, res) {
  try {
    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) {
      return res.status(500).json({ success: false, error: "Airtable credentials not configured" });
    }
    const user = dealalityUserFromReq(req);
    const found = await findUserDefaultOutreachRecord(baseId, apiKey, user?.userRecordId);
    const fields = found ? map_outreachAirtableToUiFields(found.record.fields) : {};
    res.json({ success: true, fields });
  } catch (err) {
    console.error("Error in getOutreachDefault:", err);
    res.status(500).json({ success: false, error: err.message || "Internal Server Error" });
  }
}

/**
 * PATCH /api/my-deals/outreach-default
 * Body: { fields: { "Confidentiality": value, ... } } — UI keys from my-deals.html
 */
export async function updateOutreachDefault(req, res) {
  try {
    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) {
      return res.status(500).json({ success: false, error: "Airtable credentials not configured" });
    }
    const user = dealalityUserFromReq(req);
    if (!user?.userRecordId) {
      return res.status(403).json({ success: false, error: "User record not found for outreach save." });
    }

    const body = req.body && typeof req.body === "object" ? req.body : {};
    const uiFields = body.fields && typeof body.fields === "object" ? body.fields : {};
    const toWrite = {
      ...map_outreachUiFieldsToAirtable(uiFields),
      ...identityFieldsForUser(user),
    };

    const found = await findUserDefaultOutreachRecord(baseId, apiKey, user.userRecordId);
    const result = await writeOutreachRecord(baseId, apiKey, {
      existingId: found?.record?.id,
      fields: toWrite,
    });

    if (result.error) {
      return res.status(400).json({ success: false, error: result.error.message || "Airtable API error" });
    }
    res.json({ success: true, record: result });
  } catch (err) {
    console.error("Error in updateOutreachDefault:", err);
    res.status(500).json({ success: false, error: err.message || "Internal Server Error" });
  }
}

/**
 * GET /api/my-deals/:recordId/outreach-setup
 */
export async function getOutreachSetup(req, res) {
  try {
    const dealRecordId = req.params.recordId;
    if (!dealRecordId || !dealRecordId.startsWith("rec")) {
      return res.status(400).json({ success: false, error: "Valid deal record ID is required" });
    }
    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) {
      return res.status(500).json({ success: false, error: "Airtable credentials not configured" });
    }
    const user = dealalityUserFromReq(req);

    if (hasPerDealLinkField()) {
      const dealFound = await findDealOutreachRecord(baseId, apiKey, dealRecordId, user?.userRecordId);
      if (dealFound?.record) {
        return res.json({
          success: true,
          recordId: dealFound.record.id,
          fields: map_outreachAirtableToUiFields(dealFound.record.fields),
          useDefault: false,
        });
      }
    }

    const defaultFound = await findUserDefaultOutreachRecord(baseId, apiKey, user?.userRecordId);
    const defaultFields = defaultFound ? map_outreachAirtableToUiFields(defaultFound.record.fields) : {};
    return res.json({
      success: true,
      recordId: null,
      fields: defaultFields,
      useDefault: true,
      perDealOverridesAvailable: hasPerDealLinkField(),
    });
  } catch (err) {
    console.error("Error in getOutreachSetup:", err);
    res.status(500).json({ success: false, error: err.message || "Internal Server Error" });
  }
}

/**
 * PATCH /api/my-deals/:recordId/outreach-setup
 */
export async function updateOutreachSetup(req, res) {
  try {
    const dealRecordId = req.params.recordId;
    if (!dealRecordId || !dealRecordId.startsWith("rec")) {
      return res.status(400).json({ success: false, error: "Valid deal record ID is required" });
    }
    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) {
      return res.status(500).json({ success: false, error: "Airtable credentials not configured" });
    }
    const user = dealalityUserFromReq(req);
    if (!user?.userRecordId) {
      return res.status(403).json({ success: false, error: "User record not found for outreach save." });
    }

    const body = req.body && typeof req.body === "object" ? req.body : {};
    const uiFields = body.fields && typeof body.fields === "object" ? body.fields : {};
    const toWrite = {
      ...map_outreachUiFieldsToAirtable(uiFields),
      ...identityFieldsForUser(user),
    };

    let existingId = null;
    if (hasPerDealLinkField()) {
      toWrite[OUTREACH_DEAL_FIELD] = [dealRecordId];
      const dealFound = await findDealOutreachRecord(baseId, apiKey, dealRecordId, user.userRecordId);
      existingId = dealFound?.record?.id || null;
    } else {
      const defaultFound = await findUserDefaultOutreachRecord(baseId, apiKey, user.userRecordId);
      existingId = defaultFound?.record?.id || null;
      if (process.env.NODE_ENV !== "production") {
        console.warn(
          "[outreach-setup] Per-deal save without AIRTABLE_OUTREACH_SETUP_DEAL_FIELD — updating user default record.",
          { dealRecordId, userRecordId: user.userRecordId }
        );
      }
    }

    const result = await writeOutreachRecord(baseId, apiKey, { existingId, fields: toWrite });
    if (result.error) {
      return res.status(400).json({ success: false, error: result.error.message || "Airtable API error" });
    }
    res.json({
      success: true,
      record: result,
      perDealOverridesAvailable: hasPerDealLinkField(),
    });
  } catch (err) {
    console.error("Error in updateOutreachSetup:", err);
    res.status(500).json({ success: false, error: err.message || "Internal Server Error" });
  }
}

/**
 * DELETE /api/my-deals/:recordId/outreach-setup
 */
export async function deleteOutreachSetup(req, res) {
  try {
    const dealRecordId = req.params.recordId;
    if (!dealRecordId || !dealRecordId.startsWith("rec")) {
      return res.status(400).json({ success: false, error: "Valid deal record ID is required" });
    }
    if (!hasPerDealLinkField()) {
      return res.json({ success: true, reverted: true, note: "Per-deal overrides not configured in Airtable." });
    }

    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) {
      return res.status(500).json({ success: false, error: "Airtable credentials not configured" });
    }
    const user = dealalityUserFromReq(req);
    const dealFound = await findDealOutreachRecord(baseId, apiKey, dealRecordId, user?.userRecordId);
    const existing = dealFound?.record;
    if (!existing?.id) {
      return res.json({ success: true, reverted: true });
    }

    const tableIdOrName = encodeURIComponent(OUTREACH_TABLE);
    const deleteUrl = `https://api.airtable.com/v0/${baseId}/${tableIdOrName}/${encodeURIComponent(existing.id)}`;
    const deleteRes = await fetch(deleteUrl, {
      method: "DELETE",
      headers: { Authorization: "Bearer " + apiKey },
    });
    if (!deleteRes.ok) {
      const errData = await deleteRes.json().catch(() => ({}));
      return res.status(400).json({ success: false, error: errData.error?.message || "Delete failed" });
    }
    res.json({ success: true, reverted: true });
  } catch (err) {
    console.error("Error in deleteOutreachSetup:", err);
    res.status(500).json({ success: false, error: err.message || "Internal Server Error" });
  }
}
