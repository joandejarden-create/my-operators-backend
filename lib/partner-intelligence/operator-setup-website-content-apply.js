/**
 * Apply website-sourced content into Operator Setup linked 1:1 tables.
 * Requires linked tabs (run bootstrap first). Uses upsertOperatorOneToOneTable.
 * Default dry-run. Does not touch Company Validated / governance approval fields.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { upsertOperatorOneToOneTable } from "../../api/lib/operator-setup-new-base-writer.js";
import buildSheet from "../../api/lib/operator-setup-new-base-build-sheet-rows.json" with { type: "json" };
import { runOperatorSetupLinkedTabsBootstrap } from "./operator-setup-linked-tabs-bootstrap.js";
import {
  getWebsiteContentPack,
  listWebsiteContentPackSlugs,
  resolvePackMasterMeta,
} from "./operator-setup-website-content-packs.js";

export const OPERATOR_SETUP_WEBSITE_CONTENT_APPLY_VERSION =
  "operator-setup-website-content-apply-v2";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

const TABLE = Object.freeze({
  profile: "Operator Setup - Profile & Positioning",
  platform: "Operator Setup - Platform & Markets",
  commercial: "Operator Setup - Commercial Fit & Terms",
  governance: "Operator Setup - Governance, Delivery & Diligence",
});

/** Prefill/form aliases → real Airtable field on the linked Setup table. */
const FIELD_ALIASES = Object.freeze({
  regions: "specificMarkets",
});

function allowedFieldsByTable() {
  const oneToOne = new Set(Object.values(TABLE));
  /** @type {Record<string, Set<string>>} */
  const map = {};
  for (const row of buildSheet.rows || []) {
    const t = row.table_name;
    if (!t || !oneToOne.has(t)) continue;
    if (!map[t]) map[t] = new Set();
    if (row.airtable_field_name) map[t].add(row.airtable_field_name);
  }
  return map;
}

const ALLOWED_BY_TABLE = allowedFieldsByTable();

function nonEmptyFields(obj = {}) {
  const out = {};
  for (const [k, v] of Object.entries(obj)) {
    if (v == null || v === "") continue;
    if (Array.isArray(v) && v.length === 0) continue;
    out[k] = v;
  }
  return out;
}

/**
 * Map aliases + drop fields not on the build sheet (prevents Airtable
 * "Unknown field name" rejecting the entire payload).
 * @param {string} tableName
 * @param {Record<string, unknown>} fields
 */
function sanitizeToAirtableFields(tableName, fields) {
  const allowed = ALLOWED_BY_TABLE[tableName] || new Set();
  const out = {};
  const dropped = [];
  for (const [rawKey, value] of Object.entries(fields)) {
    const key = FIELD_ALIASES[rawKey] || rawKey;
    if (!allowed.has(key)) {
      dropped.push(rawKey === key ? key : `${rawKey}->${key}`);
      continue;
    }
    if (key === "specificMarkets" && out[key] && typeof out[key] === "string" && typeof value === "string") {
      out[key] = `${out[key]}; ${value}`;
    } else {
      out[key] = value;
    }
  }
  return { fields: out, dropped };
}

/**
 * @param {{
 *   operators?: string[],
 *   apply?: boolean,
 *   approveApply?: boolean,
 *   bootstrapIfMissing?: boolean,
 *   approveBootstrap?: boolean
 * }} [opts]
 */
export async function runOperatorSetupWebsiteContentApply(opts = {}) {
  const apply = opts.apply === true;
  if (apply && !opts.approveApply) {
    throw new Error("Apply requires --approve-operator-setup-website-content-apply");
  }

  const operators = opts.operators?.length
    ? opts.operators
    : listWebsiteContentPackSlugs();

  let bootstrapReport = null;
  if (opts.bootstrapIfMissing) {
    const masters = operators
      .map((slug) => resolvePackMasterMeta(slug))
      .filter(Boolean)
      .map((m) => ({
        recordId: m.recordId,
        companyName: m.companyName,
        website: m.pack.profile?.website,
      }));
    bootstrapReport = await runOperatorSetupLinkedTabsBootstrap({
      masters,
      apply,
      approveBootstrap: opts.approveBootstrap === true || (apply && opts.approveApply === true),
    });
  }

  const results = [];
  for (const slug of operators) {
    const meta = resolvePackMasterMeta(slug);
    const pack = getWebsiteContentPack(slug);
    if (!meta || !pack) {
      results.push({ operatorSlug: slug, error: "unknown_pack_or_queue_entry" });
      continue;
    }

    const writes = [
      { tableName: TABLE.profile, raw: nonEmptyFields(pack.profile) },
      { tableName: TABLE.platform, raw: nonEmptyFields(pack.platformMarkets) },
      { tableName: TABLE.commercial, raw: nonEmptyFields(pack.commercial) },
      { tableName: TABLE.governance, raw: nonEmptyFields(pack.governance) },
    ]
      .map((w) => {
        const { fields, dropped } = sanitizeToAirtableFields(w.tableName, w.raw);
        return { tableName: w.tableName, fields, dropped };
      })
      .filter((w) => Object.keys(w.fields).length > 0);

    const tableResults = [];
    for (const w of writes) {
      const mapping = Object.entries(w.fields).map(([airtableField, value]) => ({
        airtableField,
        valuePreview: typeof value === "string" ? value.slice(0, 120) : value,
      }));
      if (apply) {
        try {
          const res = await upsertOperatorOneToOneTable(
            w.tableName,
            meta.recordId,
            w.fields,
            `web-content-${slug}`
          );
          tableResults.push({
            tableName: w.tableName,
            status: res.created ? "created_with_content" : "updated",
            recordId: res.recordId,
            exactFieldMapping: mapping,
            sanitizedPayloadPreview: w.fields,
            droppedUnknownFields: w.dropped,
          });
        } catch (err) {
          tableResults.push({
            tableName: w.tableName,
            status: "error",
            error: String(err?.message || err).slice(0, 300),
            exactFieldMapping: mapping,
            sanitizedPayloadPreview: w.fields,
            droppedUnknownFields: w.dropped,
          });
        }
      } else {
        tableResults.push({
          tableName: w.tableName,
          status: "would_upsert",
          exactFieldMapping: mapping,
          sanitizedPayloadPreview: w.fields,
          droppedUnknownFields: w.dropped,
        });
      }
    }

    results.push({
      operatorSlug: slug,
      recordId: meta.recordId,
      companyName: meta.companyName,
      sources: pack.sources,
      tables: tableResults,
      validation: {
        pass: tableResults.every((t) => t.status !== "error"),
        checksFailed: tableResults.filter((t) => t.status === "error").map((t) => t.tableName),
      },
    });
  }

  return {
    version: OPERATOR_SETUP_WEBSITE_CONTENT_APPLY_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: !apply,
    applyPerformed: apply,
    airtableWrites: apply,
    writeKind: apply ? "operator_setup_one_to_one_content_upsert" : "none",
    bootstrapReport,
    operators,
    results,
    summary: {
      operators: results.length,
      tablesTouched: results.reduce((n, r) => n + (r.tables?.length || 0), 0),
      errors: results.reduce(
        (n, r) => n + (r.tables || []).filter((t) => t.status === "error").length,
        0
      ),
    },
    errorHandling: {
      validationError: "Skip table; fix field types / select options",
      apiError: "Logged per table; other tables may still apply",
      networkError: "Retry once",
      userFacing: "Website content apply failed for one or more Setup tables.",
    },
  };
}

export function writeOperatorSetupWebsiteContentApplyReports(
  report,
  reportsDir = path.join(ROOT, "reports")
) {
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, "operator-setup-website-content-apply.json");
  const mdPath = path.join(reportsDir, "operator-setup-website-content-apply.md");
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));
  const lines = [
    "# Operator Setup website content apply",
    "",
    `dryRun: **${report.dryRun}** · tablesTouched: **${report.summary.tablesTouched}** · errors: **${report.summary.errors}**`,
    "",
  ];
  for (const r of report.results) {
    lines.push(`## ${r.companyName || r.operatorSlug}`, "");
    lines.push(`- Master: \`${r.recordId}\``);
    for (const s of r.sources || []) lines.push(`- Source: [${s.title}](${s.url})`);
    for (const t of r.tables || []) {
      lines.push(
        `- **${t.tableName}**: ${t.status} · fields: ${(t.exactFieldMapping || []).map((m) => m.airtableField).join(", ")}`
      );
      if (t.error) lines.push(`  - error: ${t.error}`);
    }
    lines.push("");
  }
  fs.writeFileSync(mdPath, lines.join("\n"));
  return { jsonPath, mdPath };
}
