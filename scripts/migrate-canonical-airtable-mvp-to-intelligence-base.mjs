#!/usr/bin/env node
/**
 * Migrate Decisions + Decision Events + Group Demand Opportunities
 * from legacy Deal Capture MVP → canonical intelligence base.
 *
 * SOURCE (legacy): appvtnDurnMSjINP6
 * TARGET: appa2cE7FTRmIbB32 (AIRTABLE_INTELLIGENCE_BASE_ID / AIRTABLE_DECISION_OUTCOME_BASE_ID)
 *
 * Does NOT delete source rows (LEGACY_MIGRATED_SOURCE retained).
 *
 * Usage:
 *   node scripts/migrate-canonical-airtable-mvp-to-intelligence-base.mjs
 *   node scripts/migrate-canonical-airtable-mvp-to-intelligence-base.mjs --apply
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import {
  LEGACY_DEAL_CAPTURE_MVP_BASE_ID,
  CANONICAL_INTELLIGENCE_BASE_ID,
  getDecisionOutcomesAirtableBaseId,
  getGdiOpportunitiesAirtableBaseId,
} from "../lib/decision-outcomes/airtable-base.js";
import {
  DECISIONS_TABLE_NAME,
  DECISION_EVENTS_TABLE_NAME,
  MAP_DECISION as D,
  MAP_DECISION_EVENT as E,
} from "../lib/decision-outcomes/field-map.js";
import {
  GDI_OPPORTUNITIES_TABLE_NAME,
  MAP_GDI_OPPORTUNITY as F,
} from "../lib/group-demand-intelligence/opportunity-field-map.js";
import { escapeAirtableFormulaValue } from "../lib/airtable-utils.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");
const DELAY_MS = Number(process.env.CANONICAL_AT_MIGRATE_DELAY_MS || 120);

const SOURCE_BASE =
  process.env.CANONICAL_MIGRATE_SOURCE_BASE_ID || LEGACY_DEAL_CAPTURE_MVP_BASE_ID;
const TARGET_BASE =
  process.env.AIRTABLE_INTELLIGENCE_BASE_ID ||
  process.env.AIRTABLE_DECISION_OUTCOME_BASE_ID ||
  process.env.DECISION_OUTCOMES_AIRTABLE_BASE_ID ||
  process.env.ADP_AIRTABLE_BASE_ID ||
  CANONICAL_INTELLIGENCE_BASE_ID;

const REPORT_JSON = path.join(
  ROOT,
  "reports",
  "decision-outcomes",
  "mvp-to-intelligence-base-migration.json"
);
const REPORT_MD = path.join(
  ROOT,
  "reports",
  "decision-outcomes",
  "mvp-to-intelligence-base-migration.md"
);

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function token() {
  const t = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  if (!t) throw new Error("AIRTABLE_API_KEY or AIRTABLE_PAT required");
  return t;
}

function base(id) {
  return new Airtable({ apiKey: token() }).base(id);
}

async function selectAll(table) {
  const out = [];
  await table.select({ pageSize: 100 }).eachPage((rows, next) => {
    out.push(...rows);
    next();
  });
  return out;
}

async function findByField(table, fieldName, value) {
  const formula = `{${fieldName}}='${escapeAirtableFormulaValue(value)}'`;
  const rows = await table
    .select({ filterByFormula: formula, maxRecords: 1 })
    .firstPage();
  return rows[0] || null;
}

function stripReadonly(fields) {
  const out = { ...fields };
  // Airtable formula/computed shouldn't appear; pass through as-is
  return out;
}

async function migrateTable({
  label,
  tableName,
  idField,
  stats,
}) {
  const sourceTable = base(SOURCE_BASE)(tableName);
  const targetTable = base(TARGET_BASE)(tableName);
  let sourceRows = [];
  try {
    sourceRows = await selectAll(sourceTable);
  } catch (err) {
    stats.failures.push({
      label,
      error: `source_read_failed: ${err.message}`,
    });
    return;
  }

  stats.found = sourceRows.length;
  for (const row of sourceRows) {
    const fields = stripReadonly(row.fields || {});
    const stableId = fields[idField];
    if (!stableId) {
      stats.skipped.push({ reason: "missing_stable_id", airtableId: row.id });
      continue;
    }
    let existing = null;
    try {
      existing = await findByField(targetTable, idField, stableId);
    } catch (err) {
      stats.failures.push({
        id: stableId,
        error: `target_lookup_failed: ${err.message}`,
      });
      continue;
    }

    if (existing) {
      stats.wouldSkip += 1;
      stats.skipped.push({ id: stableId, reason: "already_on_target" });
      if (APPLY) {
        // Optionally refresh fields (preserve id) — update for mirror freshness
        try {
          await targetTable.update(existing.id, fields);
          stats.updated += 1;
          await sleep(DELAY_MS);
        } catch (err) {
          stats.failures.push({ id: stableId, error: err.message });
        }
      }
      continue;
    }

    stats.wouldCreate += 1;
    if (!APPLY) {
      stats.createdPreview.push(stableId);
      continue;
    }
    try {
      await targetTable.create(fields);
      stats.created += 1;
      await sleep(DELAY_MS);
    } catch (err) {
      stats.failures.push({ id: stableId, error: err.message });
    }
  }
}

async function main() {
  if (SOURCE_BASE === TARGET_BASE) {
    throw new Error("source_and_target_base_identical");
  }
  if (TARGET_BASE === LEGACY_DEAL_CAPTURE_MVP_BASE_ID) {
    throw new Error("target_must_not_be_mvp");
  }

  const report = {
    mode: APPLY ? "apply" : "dry-run",
    generatedAt: new Date().toISOString(),
    sourceBase: SOURCE_BASE,
    targetBase: TARGET_BASE,
    resolvedDecisionBase: getDecisionOutcomesAirtableBaseId(),
    resolvedGdiBase: getGdiOpportunitiesAirtableBaseId(),
    note: "Source rows retained as LEGACY_MIGRATED_SOURCE — do not delete yet.",
    decisions: {
      found: 0,
      wouldCreate: 0,
      wouldSkip: 0,
      created: 0,
      updated: 0,
      skipped: [],
      createdPreview: [],
      failures: [],
    },
    events: {
      found: 0,
      wouldCreate: 0,
      wouldSkip: 0,
      created: 0,
      updated: 0,
      skipped: [],
      createdPreview: [],
      failures: [],
    },
    opportunities: {
      found: 0,
      wouldCreate: 0,
      wouldSkip: 0,
      created: 0,
      updated: 0,
      skipped: [],
      createdPreview: [],
      failures: [],
    },
  };

  console.log(`Mode: ${report.mode}`);
  console.log(`SOURCE: ${SOURCE_BASE}`);
  console.log(`TARGET: ${TARGET_BASE}`);

  await migrateTable({
    label: "Decisions",
    tableName: DECISIONS_TABLE_NAME,
    idField: D.decisionId,
    stats: report.decisions,
  });
  await migrateTable({
    label: "Decision Events",
    tableName: DECISION_EVENTS_TABLE_NAME,
    idField: E.eventId,
    stats: report.events,
  });
  await migrateTable({
    label: "Group Demand Opportunities",
    tableName: GDI_OPPORTUNITIES_TABLE_NAME,
    idField: F.opportunityId,
    stats: report.opportunities,
  });

  fs.mkdirSync(path.dirname(REPORT_JSON), { recursive: true });
  fs.writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2) + "\n");

  const md = `# MVP → Intelligence base migration

- Mode: **${report.mode}**
- Source (legacy MVP): \`${report.sourceBase}\`
- Target (canonical): \`${report.targetBase}\`
- Generated: ${report.generatedAt}

| Table | Found | Would create | Would skip | Created | Updated | Failures |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Decisions | ${report.decisions.found} | ${report.decisions.wouldCreate} | ${report.decisions.wouldSkip} | ${report.decisions.created} | ${report.decisions.updated} | ${report.decisions.failures.length} |
| Decision Events | ${report.events.found} | ${report.events.wouldCreate} | ${report.events.wouldSkip} | ${report.events.created} | ${report.events.updated} | ${report.events.failures.length} |
| Group Demand Opportunities | ${report.opportunities.found} | ${report.opportunities.wouldCreate} | ${report.opportunities.wouldSkip} | ${report.opportunities.created} | ${report.opportunities.updated} | ${report.opportunities.failures.length} |

Source rows **not deleted** (LEGACY_MIGRATED_SOURCE).
`;
  fs.writeFileSync(REPORT_MD, md);

  console.log("\n--- Summary ---");
  console.log(
    "Decisions:",
    report.decisions.found,
    "create",
    report.decisions.created || report.decisions.wouldCreate,
    "fail",
    report.decisions.failures.length
  );
  console.log(
    "Events:",
    report.events.found,
    "create",
    report.events.created || report.events.wouldCreate,
    "fail",
    report.events.failures.length
  );
  console.log(
    "Opportunities:",
    report.opportunities.found,
    "create",
    report.opportunities.created || report.opportunities.wouldCreate,
    "fail",
    report.opportunities.failures.length
  );
  console.log("Report:", REPORT_JSON);

  const fails =
    report.decisions.failures.length +
    report.events.failures.length +
    report.opportunities.failures.length;
  if (fails) process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
