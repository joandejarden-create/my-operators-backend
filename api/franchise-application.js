/**
 * Franchise Application API – get/update franchise application for a deal.
 * Uses Airtable table "Franchise Applications" (AIRTABLE_TABLE_FRANCHISE_APPLICATIONS)
 * with a "Deal" link to Deals. One record per deal; creates on first save if missing.
 * Full form data is stored as JSON in "Form Data" (Long text).
 */

const FRANCHISE_TABLE = process.env.AIRTABLE_TABLE_FRANCHISE_APPLICATIONS || "Franchise Applications";
const DEAL_LINK_FIELD = process.env.AIRTABLE_FRANCHISE_APPLICATION_DEAL_FIELD || "Deal";
const FORM_DATA_FIELD = process.env.AIRTABLE_FRANCHISE_APPLICATION_FORM_DATA_FIELD || "Form Data";

async function findFranchiseApplicationByDealId(baseId, apiKey, dealRecordId) {
  const formula = `FIND("${dealRecordId.replace(/"/g, '\\"')}", ARRAYJOIN({${DEAL_LINK_FIELD}}))`;
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(FRANCHISE_TABLE)}?filterByFormula=${encodeURIComponent(formula)}&maxRecords=1`;
  const res = await fetch(url, { headers: { Authorization: "Bearer " + apiKey } });
  const data = await res.json();
  if (data.error) return null;
  return (data.records && data.records[0]) || null;
}

/**
 * GET /api/franchise-application/:dealId
 * Returns saved franchise application form data for the deal. Merges with deal prefills on client.
 */
export async function getFranchiseApplication(req, res) {
  try {
    const dealId = req.params.dealId;
    if (!dealId || !dealId.startsWith("rec")) {
      return res.status(400).json({ success: false, error: "Valid deal record ID is required" });
    }
    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) {
      return res.status(500).json({ success: false, error: "Airtable credentials not configured" });
    }

    const record = await findFranchiseApplicationByDealId(baseId, apiKey, dealId);
    let fields = {};
    if (record && record.fields && record.fields[FORM_DATA_FIELD]) {
      try {
        const parsed = JSON.parse(record.fields[FORM_DATA_FIELD]);
        if (parsed && typeof parsed === "object") {
          delete parsed._dealId;
          delete parsed._savedAt;
          fields = parsed;
        }
      } catch (_) {}
    }
    res.json({ success: true, recordId: record ? record.id : null, fields });
  } catch (err) {
    console.error("Error in getFranchiseApplication:", err);
    res.status(500).json({ success: false, error: err.message || "Internal Server Error" });
  }
}

/**
 * PATCH /api/franchise-application/:dealId
 * Body: full form fields object (from getFieldValues). Creates record if none exists.
 */
export async function updateFranchiseApplication(req, res) {
  try {
    const dealId = req.params.dealId;
    if (!dealId || !dealId.startsWith("rec")) {
      return res.status(400).json({ success: false, error: "Valid deal record ID is required" });
    }
    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) {
      return res.status(500).json({ success: false, error: "Airtable credentials not configured" });
    }

    const body = req.body && typeof req.body === "object" ? req.body : {};
    const toStore = { ...body };
    delete toStore._dealId;
    delete toStore._savedAt;
    const formDataJson = JSON.stringify(toStore);

    const record = await findFranchiseApplicationByDealId(baseId, apiKey, dealId);
    const tableIdOrName = encodeURIComponent(FRANCHISE_TABLE);

    if (record && record.id) {
      const patchUrl = `https://api.airtable.com/v0/${baseId}/${tableIdOrName}/${encodeURIComponent(record.id)}`;
      const patchRes = await fetch(patchUrl, {
        method: "PATCH",
        headers: {
          Authorization: "Bearer " + apiKey,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ fields: { [FORM_DATA_FIELD]: formDataJson } }),
      });
      const result = await patchRes.json();
      if (result.error) {
        return res.status(400).json({ success: false, error: result.error.message || "Airtable API error" });
      }
      return res.json({ success: true, recordId: record.id });
    }

    const createUrl = `https://api.airtable.com/v0/${baseId}/${tableIdOrName}`;
    const createRes = await fetch(createUrl, {
      method: "POST",
      headers: {
        Authorization: "Bearer " + apiKey,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        fields: {
          [DEAL_LINK_FIELD]: [dealId],
          [FORM_DATA_FIELD]: formDataJson,
        },
        typecast: true,
      }),
    });
    const result = await createRes.json();
    if (result.error) {
      return res.status(400).json({ success: false, error: result.error.message || "Airtable API error" });
    }
    res.json({ success: true, recordId: result.id });
  } catch (err) {
    console.error("Error in updateFranchiseApplication:", err);
    res.status(500).json({ success: false, error: err.message || "Internal Server Error" });
  }
}
