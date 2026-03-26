/**
 * Outreach Setup API – get/update outreach preferences for a deal.
 * Uses Airtable table "Outreach Setup" (or AIRTABLE_TABLE_OUTREACH_SETUP) with a "Deal" link to Deals.
 * One record per deal; creates on first save if missing.
 */

const OUTREACH_TABLE = process.env.AIRTABLE_TABLE_OUTREACH_SETUP || "Outreach Setup";
const DEALS_TABLE = process.env.AIRTABLE_TABLE_DEALS || "Deals";
/** Link field to Deals table. Must match Airtable column name exactly (e.g. "Deal"). Override with AIRTABLE_OUTREACH_SETUP_DEAL_FIELD if your column has a different name. */
const OUTREACH_DEAL_FIELD = process.env.AIRTABLE_OUTREACH_SETUP_DEAL_FIELD || "Deal";

const OUTREACH_FIELD_NAMES = [
  "Confidentiality",
  "Identity Disclosure",
  "Exclude Brands",
  "Exclude Brands List",
  "Prioritize Companies",
  "Prioritize Companies List",
  "Outreach From",
  "Messaging Involvement",
  "Approve Each Message",
  "Preferred Tone",
  "When to Begin Outreach",
  "Outreach Start Date",
  "Follow-up Frequency",
  "Notify on Open or Respond",
  "Attachments to Include",
  "Attachments Gated",
  "Allow Forward or Share",
  "Custom First Message",
];

function valueToStr(v) {
  if (v == null) return "";
  if (typeof v === "string") return v.trim();
  if (typeof v === "object" && v !== null && typeof v.name === "string") return v.name.trim();
  if (Array.isArray(v) && v[0]) return valueToStr(v[0]);
  return "";
}

function fieldsFromRecord(record) {
  if (!record || !record.fields) return {};
  const out = {};
  OUTREACH_FIELD_NAMES.forEach((name) => {
    const v = record.fields[name];
    if (v != null) out[name] = valueToStr(v);
  });
  return out;
}

/**
 * Fetch all Outreach Setup records and return a Set of linked deal record IDs.
 * Used by getMyDeals to add hasOutreachSetup to each deal.
 * options.beforeRequest: optional async fn to call before each Airtable request (e.g. throttle when called from getMyDeals).
 */
export async function getAllOutreachDealIds(baseId, apiKey, options = {}) {
  const set = new Set();
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

/**
 * Find the single Outreach Setup record that has no Deal link (the "default" record).
 * Returns { record } or null.
 */
async function findDefaultOutreachRecord(baseId, apiKey) {
  const tableIdOrName = encodeURIComponent(OUTREACH_TABLE);
  let offset = null;
  do {
    let url = `https://api.airtable.com/v0/${baseId}/${tableIdOrName}?pageSize=100`;
    if (offset) url += "&offset=" + encodeURIComponent(offset);
    const res = await fetch(url, { headers: { Authorization: "Bearer " + apiKey } });
    const data = await res.json();
    if (data.error) return null;
    const records = data.records || [];
    for (const rec of records) {
      const raw = (rec.fields || {})[OUTREACH_DEAL_FIELD];
      if (!Array.isArray(raw) || raw.length === 0) return { record: rec };
    }
    offset = data.offset || null;
  } while (offset);
  return null;
}

/**
 * GET /api/my-deals/outreach-default
 * Returns the default outreach settings (record with no Deal link). Used for "applies to all deals."
 */
export async function getOutreachDefault(req, res) {
  try {
    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) {
      return res.status(500).json({ success: false, error: "Airtable credentials not configured" });
    }
    const found = await findDefaultOutreachRecord(baseId, apiKey);
    const fields = found ? fieldsFromRecord(found.record) : {};
    res.json({ success: true, fields });
  } catch (err) {
    console.error("Error in getOutreachDefault:", err);
    res.status(500).json({ success: false, error: err.message || "Internal Server Error" });
  }
}

/**
 * PATCH /api/my-deals/outreach-default
 * Body: { fields: { "Field Name": value, ... } }
 * Creates or updates the single default Outreach record (no Deal link).
 */
export async function updateOutreachDefault(req, res) {
  try {
    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) {
      return res.status(500).json({ success: false, error: "Airtable credentials not configured" });
    }
    const body = req.body && typeof req.body === "object" ? req.body : {};
    const fields = body.fields && typeof body.fields === "object" ? { ...body.fields } : {};
    const toWrite = {};
    OUTREACH_FIELD_NAMES.forEach((name) => {
      if (fields[name] !== undefined && fields[name] !== null) {
        const v = fields[name];
        toWrite[name] = typeof v === "string" ? v.trim() : v;
      }
    });
    const tableIdOrName = encodeURIComponent(OUTREACH_TABLE);
    const found = await findDefaultOutreachRecord(baseId, apiKey);
    let result;
    if (found && found.record.id) {
      const patchUrl = `https://api.airtable.com/v0/${baseId}/${tableIdOrName}/${encodeURIComponent(found.record.id)}`;
      const patchRes = await fetch(patchUrl, {
        method: "PATCH",
        headers: {
          Authorization: "Bearer " + apiKey,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ fields: toWrite }),
      });
      result = await patchRes.json();
    } else {
      const createUrl = `https://api.airtable.com/v0/${baseId}/${tableIdOrName}`;
      const createRes = await fetch(createUrl, {
        method: "POST",
        headers: {
          Authorization: "Bearer " + apiKey,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ fields: toWrite, typecast: true }),
      });
      result = await createRes.json();
    }
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
 * Returns outreach setup fields for the given deal. If the deal has no record, returns default fields and useDefault: true.
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

    const tableIdOrName = encodeURIComponent(OUTREACH_TABLE);
    // Linked record filter: FIND(recordId, ARRAYJOIN({Field})) works for "Link to another record" fields. Field name must match table exactly.
    const formula = `FIND("${dealRecordId.replace(/"/g, '\\"')}", ARRAYJOIN({${OUTREACH_DEAL_FIELD}}))`;
    const url = `https://api.airtable.com/v0/${baseId}/${tableIdOrName}?filterByFormula=${encodeURIComponent(formula)}&maxRecords=1`;
    const getRes = await fetch(url, {
      headers: { Authorization: "Bearer " + apiKey },
    });
    const data = await getRes.json();
    if (data.error) {
      return res.status(400).json({ success: false, error: data.error.message || "Airtable API error" });
    }
    const records = data.records || [];
    const record = records[0];
    if (!record || !record.fields) {
      const defaultFound = await findDefaultOutreachRecord(baseId, apiKey);
      const defaultFields = defaultFound ? fieldsFromRecord(defaultFound.record) : {};
      return res.json({ success: true, recordId: null, fields: defaultFields, useDefault: true });
    }
    res.json({ success: true, recordId: record.id, fields: fieldsFromRecord(record), useDefault: false });
  } catch (err) {
    console.error("Error in getOutreachSetup:", err);
    res.status(500).json({ success: false, error: err.message || "Internal Server Error" });
  }
}

/**
 * PATCH /api/my-deals/:recordId/outreach-setup
 * Body: { fields: { "Field Name": value, ... } }
 * Creates an Outreach Setup record linked to the deal if none exists; otherwise updates.
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

    const body = req.body && typeof req.body === "object" ? req.body : {};
    const fields = body.fields && typeof body.fields === "object" ? { ...body.fields } : {};
    const toWrite = {};
    OUTREACH_FIELD_NAMES.forEach((name) => {
      if (fields[name] !== undefined && fields[name] !== null) {
        const v = fields[name];
        toWrite[name] = typeof v === "string" ? v.trim() : v;
      }
    });
    toWrite[OUTREACH_DEAL_FIELD] = [dealRecordId];

    const tableIdOrName = encodeURIComponent(OUTREACH_TABLE);
    const formula = `FIND("${dealRecordId.replace(/"/g, '\\"')}", ARRAYJOIN({${OUTREACH_DEAL_FIELD}}))`;
    const listUrl = `https://api.airtable.com/v0/${baseId}/${tableIdOrName}?filterByFormula=${encodeURIComponent(formula)}&maxRecords=1`;
    const listRes = await fetch(listUrl, {
      headers: { Authorization: "Bearer " + apiKey },
    });
    const listData = await listRes.json();
    if (listData.error) {
      return res.status(400).json({ success: false, error: listData.error.message || "Airtable API error" });
    }
    const records = listData.records || [];
    const existing = records[0];

    let result;
    if (existing && existing.id) {
      const patchUrl = `https://api.airtable.com/v0/${baseId}/${tableIdOrName}/${encodeURIComponent(existing.id)}`;
      const patchRes = await fetch(patchUrl, {
        method: "PATCH",
        headers: {
          Authorization: "Bearer " + apiKey,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ fields: toWrite }),
      });
      result = await patchRes.json();
    } else {
      const createUrl = `https://api.airtable.com/v0/${baseId}/${tableIdOrName}`;
      const createRes = await fetch(createUrl, {
        method: "POST",
        headers: {
          Authorization: "Bearer " + apiKey,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ fields: toWrite, typecast: true }),
      });
      result = await createRes.json();
    }
    if (result.error) {
      return res.status(400).json({ success: false, error: result.error.message || "Airtable API error" });
    }
    res.json({ success: true, record: result });
  } catch (err) {
    console.error("Error in updateOutreachSetup:", err);
    res.status(500).json({ success: false, error: err.message || "Internal Server Error" });
  }
}

/**
 * DELETE /api/my-deals/:recordId/outreach-setup
 * Removes this deal's custom outreach record so the deal uses the default again.
 */
export async function deleteOutreachSetup(req, res) {
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
    const tableIdOrName = encodeURIComponent(OUTREACH_TABLE);
    const formula = `FIND("${dealRecordId.replace(/"/g, '\\"')}", ARRAYJOIN({${OUTREACH_DEAL_FIELD}}))`;
    const listUrl = `https://api.airtable.com/v0/${baseId}/${tableIdOrName}?filterByFormula=${encodeURIComponent(formula)}&maxRecords=1`;
    const listRes = await fetch(listUrl, {
      headers: { Authorization: "Bearer " + apiKey },
    });
    const listData = await listRes.json();
    if (listData.error) {
      return res.status(400).json({ success: false, error: listData.error.message || "Airtable API error" });
    }
    const records = listData.records || [];
    const existing = records[0];
    if (!existing || !existing.id) {
      return res.json({ success: true, reverted: true });
    }
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
