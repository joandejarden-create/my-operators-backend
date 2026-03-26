/**
 * Target List API - Manage brand shortlist and deleted brands for each deal
 * Owners can add brands to their target list from:
 * - Matched Brands tab (preferred brand)
 * - Alternative Brand Suggestions modal
 * Each target has a status (Considering → Ready to Contact → Contacted → Won/Lost → Deleted)
 * 
 * Status Options:
 * - "Considering" - Default for new target list items
 * - "Ready to Contact" - Brand is ready for outreach
 * - "In Discussion" - Actively discussing with brand
 * - "Won" - Deal won with this brand
 * - "Deleted" - Brand was removed/deleted from consideration
 */

import Airtable from "airtable";

const TARGET_LIST_TABLE = process.env.AIRTABLE_TABLE_TARGET_LIST || "Target List";
const NOTES_FIELD = process.env.AIRTABLE_TARGET_LIST_NOTES_FIELD || "Notes";
const DEALS_TABLE = process.env.AIRTABLE_TABLE_DEALS || "Deals";

function getAirtableBase() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) {
    throw new Error("AIRTABLE_API_KEY or AIRTABLE_BASE_ID not configured");
  }
  return new Airtable({ apiKey }).base(baseId);
}

/** Fetch targets for a deal (for use by add-recommended-brand limit check). Returns [{ brandName, status }]. */
export async function fetchTargetsForDeal(dealId) {
  const base = getAirtableBase();
  const records = await base(TARGET_LIST_TABLE).select({ sort: [{ field: "Added Date", direction: "desc" }] }).all();
  return records
    .filter((r) => {
      const dealIds = r.fields.Deal_ID;
      return dealIds && Array.isArray(dealIds) && dealIds.includes(dealId);
    })
    .map((r) => ({ brandName: r.fields["Brand Name"] || "", status: r.fields["Status"] || "Considering" }));
}

/**
 * GET /api/target-list/:dealId
 * Fetch all targets for a deal
 * Query params:
 * - status: Filter by status (e.g., "Considering", "Deleted")
 * - excludeDeleted: "true" to exclude deleted items (default: false)
 */
export async function getTargetList(req, res) {
  const { dealId } = req.params;
  const { status, excludeDeleted } = req.query;

  try {
    const base = getAirtableBase();
    console.log('[target-list] GET for dealId:', dealId, 'status:', status, 'excludeDeleted:', excludeDeleted);
    
    // Fetch ALL records and filter in JavaScript (more reliable than Airtable formulas)
    const records = await base(TARGET_LIST_TABLE)
      .select({
        sort: [{ field: "Added Date", direction: "desc" }],
      })
      .all();

    console.log('[target-list] Fetched', records.length, 'total target records from Airtable');

    // Filter in JavaScript to find targets for this deal
    const matchingRecords = records.filter(r => {
      const dealIds = r.fields.Deal_ID;
      if (!dealIds || !Array.isArray(dealIds)) return false;
      if (!dealIds.includes(dealId)) return false;
      
      // Filter by status if provided
      const recordStatus = r.fields["Status"] || "Considering";
      if (status && recordStatus !== status) return false;
      
      // Exclude deleted items if requested
      if (excludeDeleted === "true" && recordStatus === "Deleted") return false;
      
      return true;
    });

    console.log('[target-list] Found', matchingRecords.length, 'targets for deal', dealId);

    const targets = matchingRecords.map((r) => ({
      id: r.id,
      dealId: r.fields.Deal_ID?.[0] || dealId,
      brandName: r.fields["Brand Name"] || "",
      matchScore: r.fields["Match Score"] || null,
      status: r.fields["Status"] || "Considering",
      notes: r.fields[NOTES_FIELD] || "",
      addedDate: r.fields["Added Date"] || new Date().toISOString(),
      lastUpdated: r.fields["Last Updated"] || new Date().toISOString(),
      breakdown: r.fields["Score Breakdown"] || null,
    }));

    console.log('[target-list] Returning targets:', targets.map(t => `${t.brandName} (${t.status})`));

    res.json({ success: true, targets });
  } catch (err) {
    console.error("[target-list] GET error:", err.message);
    res.status(500).json({ success: false, error: err.message });
  }
}

/**
 * POST /api/target-list
 * Add a brand to the target list
 * Body: { dealId, brandName, matchScore?, breakdown?, notes?, status? }
 */
export async function addToTargetList(req, res) {
  const { dealId, brandName, matchScore, breakdown, notes, status = "Considering" } = req.body;

  if (!dealId || !brandName) {
    return res.status(400).json({ success: false, error: "dealId and brandName required" });
  }

  try {
    const base = getAirtableBase();
    
    console.log('[target-list] POST - Adding brand:', brandName, 'to deal:', dealId, 'status:', status);
    
    // Check if brand already in target list (regardless of status)
    const existing = await base(TARGET_LIST_TABLE)
      .select({
        filterByFormula: `AND(FIND('${dealId}', ARRAYJOIN({Deal_ID})), {Brand Name} = '${brandName}')`,
        maxRecords: 1,
      })
      .firstPage();

    if (existing.length > 0) {
      console.log('[target-list] POST - Brand already exists');
      return res.json({ success: true, alreadyExists: true, targetId: existing[0].id });
    }

    // Get Deal record to link (verify it exists)
    const dealRecords = await base(DEALS_TABLE)
      .select({ filterByFormula: `RECORD_ID() = '${dealId}'`, maxRecords: 1 })
      .firstPage();

    if (dealRecords.length === 0) {
      console.log('[target-list] POST - Deal not found:', dealId);
      return res.status(404).json({ success: false, error: "Deal not found" });
    }

    // Create new target
    const now = new Date().toISOString();
    const fields = {
      "Deal_ID": [dealId],
      "Brand Name": brandName,
      "Status": status,
      "Added Date": now,
      "Last Updated": now,
    };

    if (matchScore != null) fields["Match Score"] = matchScore;
    if (breakdown) fields["Score Breakdown"] = typeof breakdown === "string" ? breakdown : JSON.stringify(breakdown);
    if (notes) fields[NOTES_FIELD] = notes;

    console.log('[target-list] POST - Creating record with fields:', JSON.stringify(fields, null, 2));
    const record = await base(TARGET_LIST_TABLE).create([{ fields }]);
    console.log('[target-list] POST - Record created with ID:', record[0].id);

    res.json({ success: true, targetId: record[0].id });
  } catch (err) {
    console.error("[target-list] POST error:", err.message);
    console.error("[target-list] POST error stack:", err.stack);
    res.status(500).json({ success: false, error: err.message });
  }
}

/**
 * PATCH /api/target-list/:targetId
 * Update a target (status, notes)
 * Body: { status?, notes? }
 */
export async function updateTarget(req, res) {
  const { targetId } = req.params;
  const { status, notes } = req.body;

  try {
    const base = getAirtableBase();
    const fields = { "Last Updated": new Date().toISOString() };
    if (status) fields["Status"] = status;
    if (notes !== undefined) fields[NOTES_FIELD] = notes;

    await base(TARGET_LIST_TABLE).update([{ id: targetId, fields }]);

    res.json({ success: true });
  } catch (err) {
    console.error("[target-list] PATCH error:", err.message);
    res.status(500).json({ success: false, error: err.message });
  }
}

/**
 * DELETE /api/target-list/:targetId
 * Remove a brand from the target list (permanently delete record)
 */
export async function removeFromTargetList(req, res) {
  const { targetId } = req.params;

  try {
    const base = getAirtableBase();
    console.log('[target-list] DELETE - Removing target with ID:', targetId);
    await base(TARGET_LIST_TABLE).destroy([targetId]);
    console.log('[target-list] DELETE - Successfully removed target:', targetId);
    res.json({ success: true });
  } catch (err) {
    console.error("[target-list] DELETE error:", err.message);
    console.error("[target-list] DELETE error stack:", err.stack);
    res.status(500).json({ success: false, error: err.message });
  }
}

/**
 * POST /api/target-list/mark-deleted
 * Mark a brand as deleted by changing its status to "Deleted"
 * Body: { dealId, brandName, matchScore?, breakdown?, notes? }
 */
export async function markAsDeleted(req, res) {
  const { dealId, brandName, matchScore, breakdown, notes } = req.body;

  if (!dealId || !brandName) {
    return res.status(400).json({ success: false, error: "dealId and brandName required" });
  }

  try {
    const base = getAirtableBase();
    const notesVal = notes != null && String(notes).trim() ? String(notes).trim() : null;
    console.log('[target-list] MARK DELETED - Brand:', brandName, 'Deal:', dealId, 'notes:', JSON.stringify(notesVal));

    // Check if brand already exists in target list
    const records = await base(TARGET_LIST_TABLE)
      .select({
        sort: [{ field: "Added Date", direction: "desc" }],
      })
      .all();

    const existing = records.filter(r => {
      const dealIds = r.fields.Deal_ID;
      if (!dealIds || !Array.isArray(dealIds)) return false;
      if (!dealIds.includes(dealId)) return false;
      return r.fields["Brand Name"] === brandName;
    });

    if (existing.length > 0) {
      // Update all matching records to Deleted status (there may be duplicates for same brand+deal)
      const updateFields = {
        "Status": "Deleted",
        "Last Updated": new Date().toISOString()
      };
      if (notesVal) updateFields[NOTES_FIELD] = notesVal;
      const updates = existing.map((r) => ({ id: r.id, fields: updateFields }));
      await base(TARGET_LIST_TABLE).update(updates);
      console.log('[target-list] MARK DELETED - Updated', existing.length, 'record(s), notes:', notesVal || '(none)');
      return res.json({ success: true, targetId: existing[0].id, updated: true });
    }

    // Create new record with Deleted status
    const now = new Date().toISOString();
    const fields = {
      "Deal_ID": [dealId],
      "Brand Name": brandName,
      "Status": "Deleted",
      "Added Date": now,
      "Last Updated": now,
    };

    if (matchScore != null) fields["Match Score"] = matchScore;
    if (breakdown) fields["Score Breakdown"] = typeof breakdown === "string" ? breakdown : JSON.stringify(breakdown);
    if (notesVal) fields[NOTES_FIELD] = notesVal;

    const record = await base(TARGET_LIST_TABLE).create([{ fields }]);
    console.log('[target-list] MARK DELETED - Record created with ID:', record[0].id);

    res.json({ success: true, targetId: record[0].id, created: true });
  } catch (err) {
    const msg = err.error || err.message || String(err);
    console.error("[target-list] MARK DELETED error:", msg);
    if (err.statusCode) console.error("[target-list] MARK DELETED statusCode:", err.statusCode);
    res.status(500).json({ success: false, error: msg });
  }
}

/**
 * POST /api/target-list/restore
 * Restore a deleted brand by changing its status from "Deleted" to "Considering"
 * Body: { dealId, brandName }
 */
export async function restoreFromDeleted(req, res) {
  const { dealId, brandName } = req.body;

  if (!dealId || !brandName) {
    return res.status(400).json({ success: false, error: "dealId and brandName required" });
  }

  try {
    const base = getAirtableBase();
    console.log("[target-list] RESTORE - Brand:", brandName, "Deal:", dealId);

    const records = await base(TARGET_LIST_TABLE)
      .select({ sort: [{ field: "Added Date", direction: "desc" }] })
      .all();

    const existing = records.filter((r) => {
      const dealIds = r.fields.Deal_ID;
      if (!dealIds || !Array.isArray(dealIds)) return false;
      if (!dealIds.includes(dealId)) return false;
      if (r.fields["Brand Name"] !== brandName) return false;
      return (r.fields["Status"] || "") === "Deleted";
    });

    if (existing.length === 0) {
      return res.status(404).json({ success: false, error: "No deleted target found for this brand" });
    }

    const targetId = existing[0].id;
    await base(TARGET_LIST_TABLE).update([
      {
        id: targetId,
        fields: {
          Status: "Considering",
          "Last Updated": new Date().toISOString(),
        },
      },
    ]);

    console.log("[target-list] RESTORE - Restored record:", targetId);
    res.json({ success: true, targetId });
  } catch (err) {
    const msg = err.error || err.message || String(err);
    console.error("[target-list] RESTORE error:", msg);
    res.status(500).json({ success: false, error: msg });
  }
}

/**
 * POST /api/target-list/batch-delete
 * Remove multiple brands from the target list at once
 */
export async function batchRemoveFromTargetList(req, res) {
  const { targetIds } = req.body;

  if (!Array.isArray(targetIds) || targetIds.length === 0) {
    return res.status(400).json({ success: false, error: 'targetIds array is required' });
  }

  try {
    const base = getAirtableBase();
    console.log('[target-list] BATCH DELETE - Removing', targetIds.length, 'targets:', targetIds);
    
    // Airtable allows up to 10 records per batch delete
    const chunks = [];
    for (let i = 0; i < targetIds.length; i += 10) {
      chunks.push(targetIds.slice(i, i + 10));
    }
    
    for (const chunk of chunks) {
      await base(TARGET_LIST_TABLE).destroy(chunk);
    }
    
    console.log('[target-list] BATCH DELETE - Successfully removed', targetIds.length, 'targets');
    res.json({ success: true, deletedCount: targetIds.length });
  } catch (err) {
    console.error("[target-list] BATCH DELETE error:", err.message);
    console.error("[target-list] BATCH DELETE error stack:", err.stack);
    res.status(500).json({ success: false, error: err.message });
  }
}
