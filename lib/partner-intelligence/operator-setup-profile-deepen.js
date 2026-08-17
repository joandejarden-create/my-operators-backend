/**
 * Deepen Operator Setup - Profile & Positioning to Arbor/HE-like field coverage.
 * Allowlist = build-sheet Profile fields ∪ fields present on Arbor baseline row
 * (so overview_* / brand JSON are included; logos & brand links excluded).
 *
 * Default dry-run. Gated apply flag required for writes.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { upsertOperatorOneToOneTable } from "../../api/lib/operator-setup-new-base-writer.js";
import buildSheet from "../../api/lib/operator-setup-new-base-build-sheet-rows.json" with { type: "json" };
import {
  getProfileDeepPack,
  listProfileDeepPackSlugs,
  resolveProfileDeepMasterMeta,
} from "./operator-setup-profile-deep-packs.js";

export const OPERATOR_SETUP_PROFILE_DEEPEN_VERSION = "operator-setup-profile-deepen-v1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

const PROFILE_TABLE = "Operator Setup - Profile & Positioning";
const ARBOR_MASTER = "recF5Z87OAqFgndoq";

const EXCLUDE_FIELDS = new Set([
  "Operator",
  "companyLogo",
  "brands", // linked Brand Basics — do not invent rec IDs
  "readyForInvestorPublication", // founder gate only
]);

function getBase() {
  const key = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!key || !baseId) throw new Error("Missing AIRTABLE_API_KEY/PAT or AIRTABLE_BASE_ID");
  return new Airtable({ apiKey: key }).base(baseId);
}

async function buildProfileAllowlist(base) {
  const fromSheet = new Set(
    (buildSheet.rows || [])
      .filter((r) => r.table_name === PROFILE_TABLE && r.airtable_field_name)
      .map((r) => r.airtable_field_name)
  );
  const rows = await base(PROFILE_TABLE).select({ pageSize: 100 }).all();
  const arbor = rows.find(
    (r) => Array.isArray(r.fields?.Operator) && r.fields.Operator.includes(ARBOR_MASTER)
  );
  if (arbor?.fields) {
    for (const k of Object.keys(arbor.fields)) fromSheet.add(k);
  }
  for (const k of EXCLUDE_FIELDS) fromSheet.delete(k);
  return fromSheet;
}

function sanitizeProfileFields(pack, allowlist) {
  const fields = {};
  const dropped = [];
  for (const [k, v] of Object.entries(pack || {})) {
    if (v == null || v === "") {
      dropped.push(`${k}:empty`);
      continue;
    }
    if (!allowlist.has(k)) {
      dropped.push(k);
      continue;
    }
    fields[k] = v;
  }
  return { fields, dropped };
}

/**
 * @param {{ operators?: string[], apply?: boolean, approveApply?: boolean }} [opts]
 */
export async function runOperatorSetupProfileDeepen(opts = {}) {
  const apply = opts.apply === true;
  if (apply && !opts.approveApply) {
    throw new Error("Apply requires --approve-operator-setup-profile-deepen");
  }

  const operators = opts.operators?.length ? opts.operators : listProfileDeepPackSlugs();
  const base = getBase();
  const allowlist = await buildProfileAllowlist(base);

  const results = [];
  for (const slug of operators) {
    const meta = resolveProfileDeepMasterMeta(slug);
    const pack = getProfileDeepPack(slug);
    if (!meta || !pack) {
      results.push({ operatorSlug: slug, error: "unknown_pack_or_queue_entry" });
      continue;
    }

    const { fields, dropped } = sanitizeProfileFields(pack, allowlist);
    const mapping = Object.entries(fields).map(([airtableField, value]) => ({
      airtableField,
      valuePreview:
        typeof value === "string"
          ? value.slice(0, 100)
          : Array.isArray(value)
            ? value.slice(0, 8)
            : value,
    }));

    if (!Object.keys(fields).length) {
      results.push({
        operatorSlug: slug,
        recordId: meta.recordId,
        status: "skipped_empty",
        droppedUnknownFields: dropped,
      });
      continue;
    }

    if (apply) {
      try {
        const res = await upsertOperatorOneToOneTable(
          PROFILE_TABLE,
          meta.recordId,
          fields,
          `profile-deepen-${slug}`
        );
        results.push({
          operatorSlug: slug,
          recordId: meta.recordId,
          companyName: meta.companyName,
          status: res.created ? "created" : "updated",
          profileRecordId: res.recordId,
          fieldCount: Object.keys(fields).length,
          exactFieldMapping: mapping,
          sanitizedPayloadPreview: fields,
          droppedUnknownFields: dropped,
          validation: { pass: true, checksFailed: [] },
        });
      } catch (err) {
        results.push({
          operatorSlug: slug,
          recordId: meta.recordId,
          companyName: meta.companyName,
          status: "error",
          error: String(err?.message || err).slice(0, 400),
          fieldCount: Object.keys(fields).length,
          exactFieldMapping: mapping,
          sanitizedPayloadPreview: fields,
          droppedUnknownFields: dropped,
          validation: { pass: false, checksFailed: ["airtable_write"] },
        });
      }
    } else {
      results.push({
        operatorSlug: slug,
        recordId: meta.recordId,
        companyName: meta.companyName,
        status: "would_upsert",
        fieldCount: Object.keys(fields).length,
        exactFieldMapping: mapping,
        sanitizedPayloadPreview: fields,
        droppedUnknownFields: dropped,
        validation: { pass: true, checksFailed: [] },
      });
    }
  }

  return {
    version: OPERATOR_SETUP_PROFILE_DEEPEN_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: !apply,
    applyPerformed: apply,
    airtableWrites: apply,
    writeKind: apply ? "operator_setup_profile_deepen_upsert" : "none",
    tableName: PROFILE_TABLE,
    allowlistSize: allowlist.size,
    operators,
    results,
    summary: {
      operators: results.length,
      wouldOrDidWrite: results.filter((r) =>
        ["would_upsert", "updated", "created"].includes(r.status)
      ).length,
      errors: results.filter((r) => r.status === "error").length,
      avgFieldCount: Math.round(
        results.reduce((n, r) => n + (r.fieldCount || 0), 0) /
          Math.max(1, results.filter((r) => r.fieldCount).length)
      ),
    },
    errorHandling: {
      validationError: "Drop unknown/empty fields; fix select options to observed baseline set",
      apiError: "Logged per operator; others may still apply",
      networkError: "Retry once",
      userFacing: "Profile deepen failed for one or more operators.",
    },
  };
}

export function writeOperatorSetupProfileDeepenReports(
  report,
  reportsDir = path.join(ROOT, "reports")
) {
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, "operator-setup-profile-deepen.json");
  const mdPath = path.join(reportsDir, "operator-setup-profile-deepen.md");
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));
  const lines = [
    "# Operator Setup Profile deepen",
    "",
    `dryRun: **${report.dryRun}** · avg fields: **${report.summary.avgFieldCount}** · errors: **${report.summary.errors}**`,
    "",
  ];
  for (const r of report.results) {
    lines.push(`## ${r.companyName || r.operatorSlug}`);
    lines.push(`- Master: \`${r.recordId}\``);
    lines.push(`- Status: **${r.status}** · fields: ${r.fieldCount ?? 0}`);
    if (r.error) lines.push(`- Error: ${r.error}`);
    if (r.exactFieldMapping?.length) {
      lines.push(
        `- Fields: ${r.exactFieldMapping.map((m) => m.airtableField).join(", ")}`
      );
    }
    lines.push("");
  }
  fs.writeFileSync(mdPath, lines.join("\n"));
  return { jsonPath, mdPath };
}
