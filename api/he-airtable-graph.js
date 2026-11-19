/**
 * Hotel Explorer — Airtable HI graph read API (tabs 2/4/5 aggregation input).
 */
import { readHiGraphForHotel } from "../lib/hotel-intelligence/research-engine-v1/he-airtable-graph-reader-v1.js";

export async function getHeAirtableGraphHotel(req, res) {
  try {
    const recordId = String(req.params.recordId || "").trim();
    if (!recordId.startsWith("rec")) {
      return res.status(400).json({ success: false, error: "invalid_airtable_record_id" });
    }
    const payload = await readHiGraphForHotel(recordId);
    return res.status(payload.ok ? 200 : 503).json({ success: payload.ok, ...payload });
  } catch (err) {
    console.error("[he-airtable-graph] hotel", err);
    return res.status(500).json({ success: false, error: "he_airtable_graph_failed" });
  }
}
