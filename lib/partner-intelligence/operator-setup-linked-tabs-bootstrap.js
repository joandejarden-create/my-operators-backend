/**
 * Bootstrap Operator Setup linked tabs for a Master record.
 * Creates missing 1:1 child rows linked via Operator → Master.
 * Does not invent select options; only creates link scaffold (+ optional safe text fields).
 *
 * Tables (from operator-setup-new-base-writer ONE_TO_ONE_TABLES):
 * - Operator Setup - Profile & Positioning
 * - Operator Setup - Platform & Markets
 * - Operator Setup - Commercial Fit & Terms
 * - Operator Setup - Governance, Delivery & Diligence
 *
 * Platform multi-row tables (Operating Platform, Brand Relationships, etc.) are populated
 * by website-content apply via replace* writers — not empty stub rows here.
 */
import Airtable from "airtable";
import {
  upsertOperatorOneToOneTable,
} from "../../api/lib/operator-setup-new-base-writer.js";

export const OPERATOR_SETUP_LINKED_BOOTSTRAP_VERSION = "operator-setup-linked-tabs-bootstrap-v1";

export const OPERATOR_SETUP_ONE_TO_ONE_TABLES = Object.freeze([
  "Operator Setup - Profile & Positioning",
  "Operator Setup - Platform & Markets",
  "Operator Setup - Commercial Fit & Terms",
  "Operator Setup - Governance, Delivery & Diligence",
]);

const MASTER_TABLE = process.env.AIRTABLE_OPERATOR_SETUP_MASTER_TABLE || "Operator Setup - Master";

function getBase() {
  const key = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!key || !baseId) throw new Error("Missing AIRTABLE_API_KEY/PAT or AIRTABLE_BASE_ID");
  return new Airtable({ apiKey: key }).base(baseId);
}

async function findLinked(base, tableName, masterId) {
  // Match Operator link IDs in JS — ARRAYJOIN({Operator}) returns primary
  // display names, not rec IDs, so FIND formulas false-negative.
  try {
    const rows = await base(tableName).select({ fields: ["Operator"], pageSize: 100 }).all();
    return rows
      .filter((r) => Array.isArray(r.fields?.Operator) && r.fields.Operator.includes(masterId))
      .map((r) => r.id);
  } catch (err) {
    return { error: String(err?.message || err).slice(0, 200) };
  }
}

/**
 * @param {{
 *   masters: Array<{ recordId: string, companyName?: string, website?: string }>,
 *   apply?: boolean,
 *   approveBootstrap?: boolean
 * }} opts
 */
export async function runOperatorSetupLinkedTabsBootstrap(opts = {}) {
  const apply = opts.apply === true;
  if (apply && !opts.approveBootstrap) {
    throw new Error("Apply requires --approve-operator-setup-linked-tabs-bootstrap");
  }
  const masters = opts.masters || [];
  if (!masters.length) throw new Error("masters[] required");

  const base = getBase();
  const results = [];

  for (const m of masters) {
    const masterId = String(m.recordId || "").trim();
    if (!/^rec[a-zA-Z0-9]+$/.test(masterId)) {
      results.push({ recordId: masterId, error: "invalid_master_id" });
      continue;
    }

    // Confirm Master exists
    let masterName = m.companyName || null;
    try {
      const master = await base(MASTER_TABLE).find(masterId);
      masterName = master.get("company_name") || masterName;
    } catch (err) {
      results.push({
        recordId: masterId,
        error: `master_not_found:${err?.message || err}`,
      });
      continue;
    }

    const tableResults = [];
    for (const tableName of OPERATOR_SETUP_ONE_TO_ONE_TABLES) {
      const existing = await findLinked(base, tableName, masterId);
      if (existing?.error) {
        tableResults.push({
          tableName,
          status: "error",
          error: existing.error,
        });
        continue;
      }
      if (Array.isArray(existing) && existing.length > 0) {
        tableResults.push({
          tableName,
          status: "exists",
          recordIds: existing,
        });
        continue;
      }

      const payload = {};
      // Safe identity mirrors used across Setup (build sheet / writer)
      if (tableName === "Operator Setup - Profile & Positioning") {
        if (masterName) payload.company_name = masterName;
        if (m.website) payload.website = m.website;
      }

      const preview = {
        ...payload,
        Operator: [masterId],
      };

      if (apply) {
        const res = await upsertOperatorOneToOneTable(
          tableName,
          masterId,
          payload,
          `bootstrap-${masterId}`
        );
        tableResults.push({
          tableName,
          status: res.created ? "created" : "updated",
          recordId: res.recordId,
          sanitizedPayloadPreview: preview,
          exactFieldMapping: Object.keys(preview).map((k) => ({
            airtableField: k,
            valuePreview: k === "Operator" ? masterId : String(preview[k]).slice(0, 80),
          })),
        });
      } else {
        tableResults.push({
          tableName,
          status: "would_create",
          sanitizedPayloadPreview: preview,
          exactFieldMapping: Object.keys(preview).map((k) => ({
            airtableField: k,
            valuePreview: k === "Operator" ? masterId : String(preview[k]).slice(0, 80),
          })),
        });
      }
    }

    results.push({
      recordId: masterId,
      companyName: masterName,
      tables: tableResults,
      createdCount: tableResults.filter((t) => t.status === "created" || t.status === "would_create").length,
      existingCount: tableResults.filter((t) => t.status === "exists").length,
    });
  }

  return {
    version: OPERATOR_SETUP_LINKED_BOOTSTRAP_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: !apply,
    applyPerformed: apply,
    airtableWrites: apply,
    writeKind: apply ? "operator_setup_one_to_one_create" : "none",
    validation: { pass: true, checksFailed: [] },
    errorHandling: {
      validationError: "Do not create; fix Master id / company_name",
      apiError: "Surface Airtable message; re-check link field Operator",
      networkError: "Retry once; re-search before duplicate create",
      userFacing: "Could not bootstrap Operator Setup linked tabs.",
    },
    results,
    summary: {
      masters: results.length,
      tablesWouldCreateOrCreated: results.reduce((n, r) => n + (r.createdCount || 0), 0),
      tablesAlreadyLinked: results.reduce((n, r) => n + (r.existingCount || 0), 0),
    },
  };
}
