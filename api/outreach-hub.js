/**
 * Outreach Hub API – read (and optional write) for OutreachPlans, PlanTargets,
 * Threads, Messages, Templates, Sequences, SequenceSteps.
 * Uses AIRTABLE_BASE_ID and AIRTABLE_API_KEY. Table names from env or schema defaults.
 */

const TABLE_NAMES = {
  plans: process.env.AIRTABLE_TABLE_OUTREACH_PLANS || "OutreachPlans",
  "plan-targets": process.env.AIRTABLE_TABLE_PLAN_TARGETS || "PlanTargets",
  threads: process.env.AIRTABLE_TABLE_THREADS || "Threads",
  messages: process.env.AIRTABLE_TABLE_MESSAGES || "Messages",
  templates: process.env.AIRTABLE_TABLE_TEMPLATES || "Templates",
  "template-packs": process.env.AIRTABLE_TABLE_TEMPLATE_PACKS || "TemplatePacks",
  sequences: process.env.AIRTABLE_TABLE_SEQUENCES || "Sequences",
  "sequence-steps": process.env.AIRTABLE_TABLE_SEQUENCE_STEPS || "SequenceSteps",
  companies: process.env.AIRTABLE_TABLE_COMPANIES || "Companies",
  contacts: process.env.AIRTABLE_TABLE_CONTACTS || "Contacts",
};

function getTableName(slug) {
  const name = TABLE_NAMES[slug];
  if (!name) return null;
  return name;
}

function getBaseAndKey() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  return { baseId, apiKey };
}

async function listAirtable(tableName, baseId, apiKey, opts = {}) {
  const params = new URLSearchParams();
  if (opts.pageSize != null) params.set("pageSize", String(Math.min(100, Math.max(1, opts.pageSize))));
  else params.set("pageSize", "100");
  if (opts.offset) params.set("offset", opts.offset);
  if (opts.filterByFormula) params.set("filterByFormula", opts.filterByFormula);
  if (opts.maxRecords != null) params.set("maxRecords", String(opts.maxRecords));
  if (opts.sort && Array.isArray(opts.sort) && opts.sort.length) {
    params.set("sort", JSON.stringify(opts.sort.slice(0, 3)));
  }
  if (opts.view) params.set("view", opts.view);

  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}?${params}`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
  const data = await res.json();
  if (data.error) throw new Error(data.error.message || "Airtable API error");
  return data;
}

/**
 * GET /api/outreach-hub/:table
 * Query: filterByFormula, maxRecords, pageSize, offset, view, sort (JSON array)
 */
export async function list(req, res) {
  try {
    const tableSlug = req.params.table;
    const tableName = getTableName(tableSlug);
    if (!tableName) {
      return res.status(400).json({
        success: false,
        error: "Invalid table. Use: plans, plan-targets, threads, messages, templates, template-packs, sequences, sequence-steps, companies, contacts",
      });
    }
    const { baseId, apiKey } = getBaseAndKey();
    if (!baseId || !apiKey) {
      return res.status(500).json({ success: false, error: "Airtable credentials not configured" });
    }

    let sort;
    if (req.query.sort) {
      try {
        sort = JSON.parse(req.query.sort);
        if (!Array.isArray(sort)) sort = undefined;
      } catch (_) {}
    }

    const opts = {
      filterByFormula: req.query.filterByFormula || undefined,
      maxRecords: req.query.maxRecords != null ? parseInt(req.query.maxRecords, 10) : undefined,
      pageSize: req.query.pageSize != null ? parseInt(req.query.pageSize, 10) : undefined,
      offset: req.query.offset || undefined,
      view: req.query.view || undefined,
      sort,
    };

    const data = await listAirtable(tableName, baseId, apiKey, opts);
    res.json({
      success: true,
      records: data.records || [],
      offset: data.offset || null,
    });
  } catch (err) {
    console.error("Outreach Hub list error:", err);
    res.status(500).json({ success: false, error: err.message || "Internal Server Error" });
  }
}

/**
 * GET /api/outreach-hub/:table/:recordId
 */
export async function get(req, res) {
  try {
    const tableSlug = req.params.table;
    const recordId = req.params.recordId;
    const tableName = getTableName(tableSlug);
    if (!tableName) {
      return res.status(400).json({
        success: false,
        error: "Invalid table. Use: plans, plan-targets, threads, messages, templates, template-packs, sequences, sequence-steps, companies, contacts",
      });
    }
    if (!recordId || !recordId.startsWith("rec")) {
      return res.status(400).json({ success: false, error: "Valid record ID (rec...) required" });
    }
    const { baseId, apiKey } = getBaseAndKey();
    if (!baseId || !apiKey) {
      return res.status(500).json({ success: false, error: "Airtable credentials not configured" });
    }

    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}/${encodeURIComponent(recordId)}`;
    const fetchRes = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
    const data = await fetchRes.json();
    if (data.error) {
      if (fetchRes.status === 404) return res.status(404).json({ success: false, error: data.error.message });
      return res.status(400).json({ success: false, error: data.error.message });
    }
    res.json({ success: true, record: data });
  } catch (err) {
    console.error("Outreach Hub get error:", err);
    res.status(500).json({ success: false, error: err.message || "Internal Server Error" });
  }
}

/**
 * POST /api/outreach-hub/:table
 * Body: { fields: { ... } }
 */
export async function create(req, res) {
  try {
    const tableSlug = req.params.table;
    const tableName = getTableName(tableSlug);
    if (!tableName) {
      return res.status(400).json({
        success: false,
        error: "Invalid table. Use: plans, plan-targets, threads, messages, templates, template-packs, sequences, sequence-steps, companies, contacts",
      });
    }
    const { baseId, apiKey } = getBaseAndKey();
    if (!baseId || !apiKey) {
      return res.status(500).json({ success: false, error: "Airtable credentials not configured" });
    }

    const fields = req.body?.fields && typeof req.body.fields === "object" ? req.body.fields : {};
    if (Object.keys(fields).length === 0) {
      return res.status(400).json({ success: false, error: "Body must include { fields: { ... } }" });
    }

    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}`;
    const fetchRes = await fetch(url, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ fields, typecast: true }),
    });
    const data = await fetchRes.json();
    if (data.error) {
      return res.status(400).json({ success: false, error: data.error.message });
    }
    res.status(201).json({ success: true, record: data });
  } catch (err) {
    console.error("Outreach Hub create error:", err);
    res.status(500).json({ success: false, error: err.message || "Internal Server Error" });
  }
}

/**
 * PATCH /api/outreach-hub/:table/:recordId
 * Body: { fields: { ... } }
 */
export async function update(req, res) {
  try {
    const tableSlug = req.params.table;
    const recordId = req.params.recordId;
    const tableName = getTableName(tableSlug);
    if (!tableName) {
      return res.status(400).json({
        success: false,
        error: "Invalid table. Use: plans, plan-targets, threads, messages, templates, template-packs, sequences, sequence-steps, companies, contacts",
      });
    }
    if (!recordId || !recordId.startsWith("rec")) {
      return res.status(400).json({ success: false, error: "Valid record ID (rec...) required" });
    }
    const { baseId, apiKey } = getBaseAndKey();
    if (!baseId || !apiKey) {
      return res.status(500).json({ success: false, error: "Airtable credentials not configured" });
    }

    const fields = req.body?.fields && typeof req.body.fields === "object" ? req.body.fields : {};
    if (Object.keys(fields).length === 0) {
      return res.status(400).json({ success: false, error: "Body must include { fields: { ... } }" });
    }

    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}/${encodeURIComponent(recordId)}`;
    const fetchRes = await fetch(url, {
      method: "PATCH",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ fields, typecast: true }),
    });
    const data = await fetchRes.json();
    if (data.error) {
      return res.status(400).json({ success: false, error: data.error.message });
    }
    res.json({ success: true, record: data });
  } catch (err) {
    console.error("Outreach Hub update error:", err);
    res.status(500).json({ success: false, error: err.message || "Internal Server Error" });
  }
}

/**
 * DELETE /api/outreach-hub/:table/:recordId
 */
export async function remove(req, res) {
  try {
    const tableSlug = req.params.table;
    const recordId = req.params.recordId;
    const tableName = getTableName(tableSlug);
    if (!tableName) {
      return res.status(400).json({
        success: false,
        error: "Invalid table. Use: plans, plan-targets, threads, messages, templates, template-packs, sequences, sequence-steps, companies, contacts",
      });
    }
    if (!recordId || !recordId.startsWith("rec")) {
      return res.status(400).json({ success: false, error: "Valid record ID (rec...) required" });
    }
    const { baseId, apiKey } = getBaseAndKey();
    if (!baseId || !apiKey) {
      return res.status(500).json({ success: false, error: "Airtable credentials not configured" });
    }

    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}/${encodeURIComponent(recordId)}`;
    const fetchRes = await fetch(url, {
      method: "DELETE",
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    if (!fetchRes.ok) {
      const errData = await fetchRes.json().catch(() => ({}));
      return res.status(400).json({ success: false, error: errData.error?.message || "Delete failed" });
    }
    res.json({ success: true, deleted: true });
  } catch (err) {
    console.error("Outreach Hub delete error:", err);
    res.status(500).json({ success: false, error: err.message || "Internal Server Error" });
  }
}
