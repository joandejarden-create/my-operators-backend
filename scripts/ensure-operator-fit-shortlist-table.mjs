#!/usr/bin/env node
/**
 * Ensure "Operator Fit - Shortlist" Airtable table (Meta API).
 * Dry-run by default; apply with --apply.
 *
 *   node scripts/ensure-operator-fit-shortlist-table.mjs --dry-run
 *   node scripts/ensure-operator-fit-shortlist-table.mjs --apply --approve-shortlist-table
 */
import "../load-env.js";
import { writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  OPERATOR_SHORTLIST_TABLE,
  map_operatorShortlistFields as F,
  SHORTLIST_STATUS,
} from "../lib/operator-fit/shortlist.js";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const APPLY = process.argv.includes("--apply");
const APPROVED = process.argv.includes("--approve-shortlist-table");
const DRY = !APPLY;

async function metaFetch(baseId, token, path, init = {}) {
  const url = `https://api.airtable.com/v0/meta/bases/${baseId}${path}`;
  const res = await fetch(url, {
    ...init,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      ...(init.headers || {}),
    },
  });
  const text = await res.text();
  let json;
  try {
    json = text ? JSON.parse(text) : {};
  } catch {
    json = { raw: text };
  }
  return { res, json };
}

function fieldDefs(masterTableId) {
  return [
    { name: F.shortlistId, type: "singleLineText" },
    { name: F.deal, type: "singleLineText" },
    { name: F.dealLabel, type: "singleLineText" },
    ...(masterTableId
      ? [
          {
            name: F.operator,
            type: "multipleRecordLinks",
            options: { linkedTableId: masterTableId },
          },
        ]
      : [{ name: F.operatorName, type: "singleLineText" }]),
    { name: F.operatorName, type: "singleLineText" },
    { name: F.brand, type: "singleLineText" },
    {
      name: F.candidateType,
      type: "singleSelect",
      options: {
        choices: [
          { name: "Third-party operator" },
          { name: "Brand-managed" },
          { name: "Hybrid" },
          { name: "Research Stage" },
        ],
      },
    },
    { name: F.operatingStructure, type: "singleLineText" },
    {
      name: F.status,
      type: "singleSelect",
      options: {
        choices: Object.values(SHORTLIST_STATUS).map((name) => ({ name })),
      },
    },
    {
      name: F.shortlistedDate,
      type: "date",
      options: { dateFormat: { name: "iso" } },
    },
    { name: F.shortlistedBy, type: "singleLineText" },
    { name: F.alignmentAtShortlist, type: "number", options: { precision: 1 } },
    { name: F.confidenceAtShortlist, type: "singleLineText" },
    { name: F.coverageAtShortlist, type: "number", options: { precision: 1 } },
    { name: F.eligibilityAtShortlist, type: "singleLineText" },
    { name: F.readinessAtShortlist, type: "singleLineText" },
    { name: F.lifecycleAtShortlist, type: "singleLineText" },
    { name: F.engineVersion, type: "singleLineText" },
    { name: F.reasonsAtShortlist, type: "multilineText" },
    { name: F.concernsAtShortlist, type: "multilineText" },
    { name: F.unknownsAtShortlist, type: "multilineText" },
    { name: F.advisorNote, type: "multilineText" },
    {
      name: F.removedDate,
      type: "date",
      options: { dateFormat: { name: "iso" } },
    },
    { name: F.removedBy, type: "singleLineText" },
    { name: F.removalReason, type: "multilineText" },
    { name: F.outreachStatus, type: "singleLineText" },
    { name: F.snapshotJson, type: "multilineText" },
  ];
}

async function main() {
  if (APPLY && !APPROVED) {
    throw new Error("Refusing --apply without --approve-shortlist-table");
  }
  const token = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!token || !baseId) throw new Error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID required");

  const { res: listRes, json: listJson } = await metaFetch(baseId, token, "/tables");
  if (!listRes.ok) throw new Error(`List tables failed ${listRes.status}`);

  const tables = listJson.tables || [];
  const existing = tables.find((t) => t.name === OPERATOR_SHORTLIST_TABLE);
  const master = tables.find((t) => t.name === "Operator Setup - Master");
  const report = {
    generatedAt: new Date().toISOString(),
    mode: DRY ? "dry-run" : "apply",
    tableName: OPERATOR_SHORTLIST_TABLE,
    exists: Boolean(existing),
    tableId: existing?.id || null,
    notOdr: true,
    odrTableUntouched: true,
    fieldsPlanned: fieldDefs(master?.id).map((f) => f.name),
  };

  if (existing) {
    report.action = "already_exists";
  } else if (DRY) {
    report.action = "would_create";
  } else {
    const primary = { name: F.shortlistId, type: "singleLineText" };
    const rest = fieldDefs(master?.id).filter((f) => f.name !== F.shortlistId);
    // Deduplicate operatorName if operator link present
    const seen = new Set([primary.name]);
    const uniqueRest = [];
    for (const f of rest) {
      if (seen.has(f.name)) continue;
      seen.add(f.name);
      uniqueRest.push(f);
    }
    const { res, json } = await metaFetch(baseId, token, "/tables", {
      method: "POST",
      body: JSON.stringify({
        name: OPERATOR_SHORTLIST_TABLE,
        description:
          "Operator Fit dedicated shortlist with immutable decision snapshots. Not Operator Deal Requests.",
        fields: [primary],
      }),
    });
    if (!res.ok) throw new Error(`Create table failed ${res.status}: ${JSON.stringify(json)}`);
    report.tableId = json.id;
    report.action = "created";
    for (const field of uniqueRest) {
      const r = await metaFetch(baseId, token, `/tables/${json.id}/fields`, {
        method: "POST",
        body: JSON.stringify(field),
      });
      if (!r.res.ok) {
        report.fieldErrors = report.fieldErrors || [];
        report.fieldErrors.push({ field: field.name, status: r.res.status, body: r.json });
      }
    }
  }

  writeFileSync(
    join(ROOT, "reports", "operator-fit-shortlist-schema-ensure.json"),
    JSON.stringify(report, null, 2)
  );
  writeFileSync(
    join(ROOT, "reports", "operator-fit-shortlist-schema-ensure.md"),
    [
      "# Operator Fit Shortlist — Schema Ensure",
      "",
      `Mode: **${report.mode}** · Action: **${report.action}**`,
      "",
      `- Table: \`${OPERATOR_SHORTLIST_TABLE}\``,
      `- Exists: ${report.exists}`,
      `- Table ID: ${report.tableId || "—"}`,
      `- ODR untouched: **yes**`,
      "",
      "## Fields",
      "",
      ...report.fieldsPlanned.map((n) => `- ${n}`),
      "",
    ].join("\n")
  );
  console.log(JSON.stringify(report, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
