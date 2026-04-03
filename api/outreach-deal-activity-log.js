import Airtable from "airtable";

const DEALS_TABLE = process.env.AIRTABLE_TABLE_DEALS || "Deals";
const ACTIVITY_LOG_TABLE = process.env.AIRTABLE_TABLE_DEAL_ACTIVITY_LOG || "Deal Activity Log";

function getAirtableBase() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) throw new Error("AIRTABLE_API_KEY or AIRTABLE_BASE_ID not configured");
  return new Airtable({ apiKey }).base(baseId);
}

function str(v) {
  if (v == null) return "";
  if (typeof v === "string") return v.trim();
  if (typeof v === "number" && Number.isFinite(v)) return String(v);
  if (Array.isArray(v) && v.length > 0) return str(v[0]);
  if (typeof v === "object" && v && typeof v.name === "string") return String(v.name).trim();
  return "";
}

function getDealName(fields) {
  return (
    str(fields["Project Name"]) ||
    str(fields["Deal Name"]) ||
    str(fields["Project Opportunity"]) ||
    str(fields["Deal Title"]) ||
    str(fields["Property Name"]) ||
    str(fields["Hotel Name"]) ||
    str(fields["Hotel"]) ||
    str(fields["Name"]) ||
    "Untitled Deal"
  );
}

function isAirtableRecordId(v) {
  return /^rec[a-zA-Z0-9]{10,}$/.test(str(v));
}

function collectRecordIds(value) {
  const out = [];
  const pushMaybe = (v) => {
    const s = str(v);
    if (!s) return;
    if (isAirtableRecordId(s)) out.push(s);
  };
  if (Array.isArray(value)) {
    value.forEach((v) => pushMaybe(v));
    return out;
  }
  const s = str(value);
  if (!s) return out;
  if (isAirtableRecordId(s)) {
    out.push(s);
    return out;
  }
  s.split(/[,\s]+/g).forEach((token) => pushMaybe(token));
  return out;
}

function extractDealIds(fields) {
  const candidates = [
    fields["Deal"],
    fields["Deals"],
    fields["Deal ID"],
    fields["DealId"],
    fields["Deal Id"],
    fields["Project"],
    fields["Project Deal"],
    fields["Project Opportunity"],
    fields["Project Opportunity (from Deal)"]
  ];
  const ids = [];
  for (const c of candidates) {
    collectRecordIds(c).forEach((id) => {
      if (!ids.includes(id)) ids.push(id);
    });
  }
  return ids;
}

function timeAgo(dateStr) {
  if (!dateStr) return "";
  const d = new Date(dateStr);
  if (Number.isNaN(d.getTime())) return "";
  const diff = Date.now() - d.getTime();
  if (diff < 60000) return "Just now";
  if (diff < 3600000) return `${Math.floor(diff / 60000)}m ago`;
  if (diff < 86400000) return `${Math.floor(diff / 3600000)}h ago`;
  if (diff < 172800000) return "Yesterday";
  return `${Math.floor(diff / 86400000)}d ago`;
}

function inferStakeholder(fields) {
  const explicit = str(fields["Stakeholder"]);
  if (explicit) return explicit;
  const action = str(fields["Action"]).toLowerCase();
  const details = str(fields["Details"]).toLowerCase();
  const brandName = str(fields["Brand Name"]).toLowerCase();
  if (brandName === "operator") return "Operator";
  if (
    action.includes("brand viewed") ||
    action.includes("accepted") ||
    action.includes("declined") ||
    action.includes("proposal submitted") ||
    action.includes("proposal updated")
  ) return "Brand";
  if (details.includes("operator")) return "Operator";
  return "Owner";
}

export async function getOutreachDealActivityLog(req, res) {
  try {
    const base = getAirtableBase();
    const limitRaw = parseInt(String(req.query?.limit || "50"), 10);
    const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(limitRaw, 200)) : 50;

    // Get user's deals (for now: all deals visible in this environment).
    const dealRecords = await base(DEALS_TABLE)
      .select({ pageSize: 100 })
      .all();

    const dealNameById = new Map();
    for (const rec of dealRecords) {
      const fields = rec.fields || {};
      const resolved = getDealName(fields);
      dealNameById.set(rec.id, resolved);
      // Support bases that persist record id in a text field.
      const possibleIdText = str(fields["Record ID"]) || str(fields["Deal ID"]) || str(fields["Airtable Record ID"]);
      if (isAirtableRecordId(possibleIdText)) dealNameById.set(possibleIdText, resolved);
    }
    const dealIds = new Set([...dealNameById.keys()]);
    if (dealIds.size === 0) return res.json({ success: true, entries: [] });

    const logRecords = await base(ACTIVITY_LOG_TABLE)
      .select({
        sort: [{ field: "Created At", direction: "desc" }],
        maxRecords: 500,
        pageSize: 100
      })
      .all();

    const entries = [];
    for (const rec of logRecords) {
      const fields = rec.fields || {};
      const extractedDealIds = extractDealIds(fields);
      const dealId = extractedDealIds.find((id) => dealIds.has(id)) || null;

      const createdAt = str(fields["Created At"]) || str(rec.createdTime);
      const action = str(fields["Action"]) || "Activity Updated";
      const details = str(fields["Details"]);
      const stakeholder = inferStakeholder(fields);
      const lookupDealName =
        str(fields["Deal Name"]) ||
        str(fields["Deal Name (from Deal)"]) ||
        str(fields["Project Name (from Deal)"]) ||
        str(fields["Project Opportunity (from Deal)"]) ||
        str(fields["Deal Title (from Deal)"]) ||
        str(fields["Project Name"]) ||
        str(fields["Project Opportunity"]) ||
        str(fields["Deal Title"]) ||
        str(fields["Property Name"]);
      const resolvedDealName =
        (dealId ? (dealNameById.get(dealId) || "") : "") ||
        (extractedDealIds.length ? (dealNameById.get(extractedDealIds[0]) || "") : "") ||
        (isAirtableRecordId(lookupDealName) ? "" : lookupDealName) ||
        "Untitled Deal";
      entries.push({
        id: rec.id,
        dealId,
        dealName: resolvedDealName,
        stakeholder,
        action,
        details,
        createdAt,
        timeAgo: timeAgo(createdAt),
        type: "deal",
        title: action,
        contextLabel: resolvedDealName,
        badgeLabel: "Deal Activity",
        badgeType: "info",
        ctaHref: "/outreach-deal-activity-log"
      });
      if (entries.length >= limit) break;
    }

    return res.json({ success: true, entries });
  } catch (error) {
    console.error("[outreach-deal-activity-log] error:", error.message);
    return res.status(500).json({ success: false, error: error.message, entries: [] });
  }
}

