/**
 * Build Hotel Property Census key-field completion matrix (read-only).
 * Usage: node scripts/census-autopilot-key-field-completion-matrix.mjs
 */
import "dotenv/config";
import {
  runKeyFieldCompletionQueue,
  KEY_FIELD_COMPLETION_STATUS,
  KEY_FIELD_MATRIX,
} from "../lib/research-engine-v2/census-autopilot-key-field-completion.js";
import { evaluateProviderReadiness } from "../lib/research-engine-v2/production-census-description-extraction.js";
import {
  resolvePat,
  resolveTargetBase,
} from "../lib/research-engine-v2/production-census-schema-create.js";
import { TABLE_IDS } from "../lib/research-engine-v2/production-census-write.js";
import { MAP_FIRST_PASS } from "../lib/research-engine-v2/production-census-first-pass-enrichment.js";

const CENSUS_TABLE_ID = TABLE_IDS["Hotel Property Census"] || "tbl9aY5ijiuIzzWam";
const READ_FIELDS = [
  ...new Set([
    MAP_FIRST_PASS.identityKey,
    MAP_FIRST_PASS.officialUrl,
    ...KEY_FIELD_MATRIX.map((f) => f.airtable),
  ]),
];

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function listCensusRecords(baseId, token, tableId, fields) {
  const out = [];
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of fields) params.append("fields[]", f);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(`census list ${res.status}: ${JSON.stringify(json.error || json)}`);
    out.push(...(json.records || []));
    offset = json.offset;
    await sleep(120);
  } while (offset);
  return out;
}

async function main() {
  const token = resolvePat();
  const bases = resolveTargetBase();
  if (!token || !bases?.target_base_id) {
    console.error("[key-field-completion] AIRTABLE credentials missing — empty matrix");
    const report = runKeyFieldCompletionQueue({
      censusRecords: [],
      writeReports: true,
      env: process.env,
    });
    console.log(JSON.stringify({ ok: report.ok, status: report.status, records: 0 }, null, 2));
    process.exitCode = report.status === KEY_FIELD_COMPLETION_STATUS.BLOCKED ? 1 : 0;
    return;
  }

  const rows = await listCensusRecords(bases.target_base_id, token, CENSUS_TABLE_ID, READ_FIELDS);
  const provider = evaluateProviderReadiness(process.env);
  const report = runKeyFieldCompletionQueue({
    censusRecords: rows,
    writeReports: true,
    env: process.env,
    providerReady: Boolean(provider.approved_for_geocode_apply),
  });

  console.log(
    JSON.stringify(
      {
        ok: report.ok,
        status: report.status,
        records: rows.length,
        high_proposals: report.high_proposals,
        provider_ready: provider.approved_for_geocode_apply,
        autofill: report.matrix_summary?.autofill_opportunities,
        provider_blocked_coords: report.matrix_summary?.provider_blocked_coordinate_records,
        recommended: report.recommended_next_production_cycle_action,
        reports: [
          "reports/research-engine-v2/production-census-key-field-completion-matrix.md",
          "reports/research-engine-v2/production-census-key-field-completion-matrix.json",
          "docs/data-intelligence/production-census-key-field-completion.md",
        ],
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
