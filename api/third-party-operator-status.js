const TABLE_NAME = process.env.AIRTABLE_THIRD_PARTY_OPERATORS_TABLE || "3rd Party Operator - Basics";

export default async function updateThirdPartyOperatorStatus(req, res) {
  try {
    const recordId = String((req.params && req.params.recordId) || "").trim();
    if (!recordId) return res.status(400).json({ success: false, error: "Missing recordId" });
    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    if (!baseId || !apiKey) {
      return res.status(503).json({ success: false, error: "Airtable not configured" });
    }

    const body = (req.body && typeof req.body === "object") ? req.body : {};
    const newStatus = (body.dealStatus || body.operatorStatus || body.status || "").toString().trim();
    if (!newStatus) return res.status(400).json({ success: false, error: "dealStatus is required" });

    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(TABLE_NAME)}/${encodeURIComponent(recordId)}`;
    const fieldCandidates = ["Operator Status", "Deal Status", "Status"];
    let lastErr = null;

    for (const fieldName of fieldCandidates) {
      const patchRes = await fetch(url, {
        method: "PATCH",
        headers: { Authorization: `Bearer ${apiKey}`, "Content-Type": "application/json" },
        body: JSON.stringify({
          fields: { [fieldName]: newStatus },
          typecast: true,
        }),
      });

      const data = await patchRes.json().catch(() => ({}));
      if (patchRes.ok && !data.error) {
        return res.json({ success: true, recordId, operatorStatus: newStatus, updatedField: fieldName });
      }

      lastErr =
        (data && data.error && (data.error.message || data.error.type)) ||
        patchRes.statusText ||
        "Update failed for field: " + fieldName;

      // If a field name doesn't exist, Airtable rejects the request; try the next candidate.
      // If it fails for other reasons (auth, rate limit), the next attempt will also fail.
    }

    return res.status(400).json({ success: false, error: lastErr || "Update failed" });
  } catch (err) {
    return res.status(500).json({ success: false, error: err && err.message ? err.message : "Update failed" });
  }
}

