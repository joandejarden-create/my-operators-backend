/**
 * Shared runner for P1 profile governance field setup (schema only).
 */
import { writeFileSync, mkdirSync } from "fs";
import { join } from "path";
import {
  buildP1ProfileGovernanceFieldDefs,
  resolveExistingGovernanceField,
  P1_EXCLUDED_FROM_SETUP_ROOTS,
} from "./p1-profile-governance-field-specs.js";

export async function metaFetch(baseId, token, path, init = {}) {
  const url = `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}${path}`;
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

async function ensureField(baseId, token, tableId, fieldDef, existingNames, dryRun, report) {
  const resolved = resolveExistingGovernanceField(fieldDef.name, existingNames);

  if (resolved.status === "exact") {
    report.present.push({ field: fieldDef.name, table: report._currentTable, live: resolved.liveName });
    console.log(`  exists: "${fieldDef.name}"`);
    return { field: fieldDef.name, status: "present", live: resolved.liveName };
  }

  if (resolved.status === "alias") {
    const skip = resolved.aliasConfig?.skipCreate;
    const entry = {
      field: fieldDef.name,
      table: report._currentTable,
      live: resolved.liveName,
      reason: resolved.aliasConfig?.reason || "alias match",
      skippedCreate: skip,
    };
    if (skip) {
      report.skippedAlias.push(entry);
      console.log(`  skip (alias): "${fieldDef.name}" → live "${resolved.liveName}"`);
      return { field: fieldDef.name, status: "skipped_alias", live: resolved.liveName, ...entry };
    }
    report.aliasPresent.push(entry);
    console.log(`  alias present: "${fieldDef.name}" → live "${resolved.liveName}" (no create)`);
    return { field: fieldDef.name, status: "alias_present", live: resolved.liveName, ...entry };
  }

  const payload = {
    name: fieldDef.name,
    type: fieldDef.type,
    ...(fieldDef.description ? { description: fieldDef.description } : {}),
    ...(fieldDef.options ? { options: fieldDef.options } : {}),
  };

  if (dryRun) {
    report.wouldCreate.push({ field: fieldDef.name, table: report._currentTable, type: fieldDef.type });
    console.log(`  [dry-run] would create: "${fieldDef.name}" (${fieldDef.type})`);
    return { field: fieldDef.name, status: "would_create", type: fieldDef.type };
  }

  const { res, json } = await metaFetch(baseId, token, `/tables/${tableId}/fields`, {
    method: "POST",
    body: JSON.stringify(payload),
  });

  if (!res.ok) {
    const err = json?.error?.message || JSON.stringify(json);
    report.failed.push({ field: fieldDef.name, table: report._currentTable, error: err, status: res.status });
    console.error(`  FAIL "${fieldDef.name}": ${res.status} ${err}`);
    return { field: fieldDef.name, status: "failed", error: err };
  }

  report.created.push({
    field: fieldDef.name,
    table: report._currentTable,
    id: json.id,
    type: fieldDef.type,
  });
  existingNames.add(fieldDef.name);
  console.log(`  created: "${fieldDef.name}" (${json.id})`);
  await new Promise((r) => setTimeout(r, 220));
  return { field: fieldDef.name, status: "created", id: json.id };
}

function buildMarkdownReport(report) {
  const lines = [
    `# ${report.title}`,
    "",
    `Generated: ${report.generatedAt}`,
    `Mode: ${report.mode}`,
    `Base: \`${report.baseId}\``,
    "",
    "> Schema-only. No records read or modified.",
    "",
    `**P1 scope:** ${report.tableSpecs.map((t) => t.tableName).join(", ")}`,
    "",
    `**Excluded from Setup roots (Partner Intelligence SSOT):** ${P1_EXCLUDED_FROM_SETUP_ROOTS.join(", ")}`,
    "",
    "## Summary",
    "",
    "| Metric | Count |",
    "|--------|-------|",
    `| Fields already present (exact) | ${report.present.length} |`,
    `| Fields satisfied by alias (no create) | ${report.aliasPresent.length + report.skippedAlias.length} |`,
    `| Fields would create / created | ${report.wouldCreate.length + report.created.length} |`,
    `| Fields failed | ${report.failed.length} |`,
    "",
  ];

  if (report.skippedAlias.length) {
    lines.push("## Skipped — alias equivalent (no duplicate column)", "");
    lines.push("| Table | Expected | Live column | Reason |");
    lines.push("|-------|----------|-------------|--------|");
    for (const row of report.skippedAlias) {
      lines.push(`| ${row.table} | \`${row.field}\` | \`${row.live}\` | ${row.reason} |`);
    }
    lines.push("");
  }

  if (report.aliasPresent.length) {
    lines.push("## Alias present (no create needed)", "");
    for (const row of report.aliasPresent) {
      lines.push(`- **${row.table}** — \`${row.field}\` → \`${row.live}\`: ${row.reason}`);
    }
    lines.push("");
  }

  if (report.wouldCreate.length) {
    lines.push("## Would create (dry-run)", "");
    lines.push("| Table | Field | Type |");
    lines.push("|-------|-------|------|");
    for (const row of report.wouldCreate) {
      lines.push(`| ${row.table} | \`${row.field}\` | ${row.type} |`);
    }
    lines.push("");
  }

  if (report.created.length) {
    lines.push("## Created", "");
    for (const row of report.created) {
      lines.push(`- **${row.table}** — \`${row.field}\` (${row.type}, ${row.id})`);
    }
    lines.push("");
  }

  if (report.present.length) {
    lines.push("## Already present (exact name)", "");
    for (const row of report.present) {
      lines.push(`- **${row.table}** — \`${row.field}\``);
    }
    lines.push("");
  }

  if (report.failed.length) {
    lines.push("## Failed", "");
    for (const row of report.failed) {
      lines.push(`- **${row.table}** — \`${row.field}\`: ${row.error}`);
    }
    lines.push("");
  }

  if (report.warnings.length) {
    lines.push("## Warnings", "", ...report.warnings.map((w) => `- ${w}`), "");
  }

  lines.push("## Per-table detail", "");
  for (const [key, tableReport] of Object.entries(report.tables)) {
    lines.push(`### ${tableReport.tableName}`, "");
    if (!tableReport.found) {
      lines.push("**Table not found.**", "");
      continue;
    }
    lines.push(`| Field | Status | Live / notes |`);
    lines.push(`|-------|--------|--------------|`);
    for (const r of tableReport.results) {
      const live = r.live ? `\`${r.live}\`` : r.type || r.error || "—";
      lines.push(`| \`${r.field}\` | ${r.status} | ${live} |`);
    }
    lines.push("");
  }

  return lines.join("\n");
}

/**
 * @param {object} opts
 * @param {string} opts.scriptName
 * @param {string} opts.title
 * @param {Array<{ tableKey: string, tableName: string }>} opts.tableSpecs
 * @param {string} opts.reportJsonPath
 * @param {string} opts.reportMdPath
 * @param {string} opts.root
 * @param {boolean} opts.dryRun
 */
export async function runP1GovernanceSetup(opts) {
  const token =
    process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || process.env.AIRTABLE_TOKEN;
  const baseId = process.env.AIRTABLE_BASE_ID;

  if (!token) throw new Error("Missing AIRTABLE_API_KEY, AIRTABLE_PAT, or AIRTABLE_TOKEN");
  if (!baseId) throw new Error("Missing AIRTABLE_BASE_ID");

  console.log(`[${opts.scriptName}] mode=${opts.dryRun ? "dry-run" : "apply"}`);

  const { res, json } = await metaFetch(baseId, token, "/tables");
  if (!res.ok) {
    throw new Error(`Meta API list tables failed: ${res.status} ${json.error?.message || JSON.stringify(json)}`);
  }

  const tables = json.tables || [];
  const fieldDefs = buildP1ProfileGovernanceFieldDefs();

  const report = {
    title: opts.title,
    generatedAt: new Date().toISOString(),
    mode: opts.dryRun ? "dry-run" : "apply",
    baseId,
    scriptName: opts.scriptName,
    tableSpecs: opts.tableSpecs,
    excludedFromSetupRoots: P1_EXCLUDED_FROM_SETUP_ROOTS,
    fieldDefCount: fieldDefs.length,
    tables: {},
    present: [],
    aliasPresent: [],
    skippedAlias: [],
    created: [],
    wouldCreate: [],
    failed: [],
    warnings: [],
    _currentTable: "",
  };

  for (const spec of opts.tableSpecs) {
    console.log(`\n${spec.tableName}`);
    report._currentTable = spec.tableName;
    const table = tables.find((t) => t.name === spec.tableName);
    if (!table) {
      const msg = `Table not found: ${spec.tableName}`;
      report.warnings.push(msg);
      console.warn(`  WARN: ${msg}`);
      report.tables[spec.tableKey] = { found: false, tableName: spec.tableName, results: [] };
      continue;
    }

    const existingNames = new Set((table.fields || []).map((f) => f.name));
    const results = [];
    for (const fieldDef of fieldDefs) {
      const result = await ensureField(baseId, token, table.id, fieldDef, existingNames, opts.dryRun, report);
      results.push(result);
    }
    report.tables[spec.tableKey] = {
      found: true,
      tableName: spec.tableName,
      tableId: table.id,
      results,
    };
  }

  delete report._currentTable;

  mkdirSync(join(opts.root, "reports"), { recursive: true });
  writeFileSync(opts.reportJsonPath, JSON.stringify(report, null, 2));
  writeFileSync(opts.reportMdPath, buildMarkdownReport(report));

  console.log("\nWrote", opts.reportJsonPath);
  console.log("Wrote", opts.reportMdPath);
  console.log(
    `Summary: present=${report.present.length} alias/skipped=${report.aliasPresent.length + report.skippedAlias.length} ` +
      `wouldCreate=${report.wouldCreate.length} created=${report.created.length} failed=${report.failed.length}`
  );

  if (opts.dryRun && report.wouldCreate.length) {
    console.log("\nRe-run with --apply to create missing fields (founder approval required).");
  }

  return report;
}
