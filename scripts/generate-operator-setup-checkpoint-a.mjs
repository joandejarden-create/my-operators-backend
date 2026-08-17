#!/usr/bin/env node
import fs from "fs";
import path from "path";
import "dotenv/config";

const ROOT = process.cwd();
const REPORTS = path.join(ROOT, "reports");

function read(p) {
  return fs.readFileSync(path.join(ROOT, p), "utf8");
}
function exists(p) {
  return fs.existsSync(path.join(ROOT, p));
}
function csvParse(text) {
  const lines = String(text || "").split(/\r?\n/).filter(Boolean);
  if (!lines.length) return [];
  const parseLine = (line) => {
    const out = [];
    let cur = "";
    let q = false;
    for (let i = 0; i < line.length; i++) {
      const ch = line[i];
      if (ch === '"') {
        if (q && line[i + 1] === '"') {
          cur += '"';
          i++;
        } else q = !q;
      } else if (ch === "," && !q) {
        out.push(cur);
        cur = "";
      } else cur += ch;
    }
    out.push(cur);
    return out;
  };
  const header = parseLine(lines[0]);
  return lines.slice(1).map((line) => {
    const vals = parseLine(line);
    const row = {};
    header.forEach((h, i) => (row[h] = vals[i] ?? ""));
    return row;
  });
}
function csvEscape(v) {
  const s = String(v ?? "");
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}
function writeCsv(file, rows, headers) {
  const lines = [headers.join(",")];
  for (const r of rows) lines.push(headers.map((h) => csvEscape(r[h])).join(","));
  fs.writeFileSync(path.join(REPORTS, file), lines.join("\n"));
}
function writeJson(file, obj) {
  fs.writeFileSync(path.join(REPORTS, file), JSON.stringify(obj, null, 2));
}
function writeMd(file, text) {
  fs.writeFileSync(path.join(REPORTS, file), text);
}

async function fetchLiveSchema(baseId, apiKey) {
  if (!baseId || !apiKey) return { ok: false, reason: "Missing AIRTABLE_BASE_ID/API_KEY" };
  try {
    const res = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    if (!res.ok) return { ok: false, reason: `Meta API ${res.status}` };
    const data = await res.json();
    return { ok: true, tables: data.tables || [] };
  } catch (e) {
    return { ok: false, reason: e.message || String(e) };
  }
}

function pickOperatorSetupTables(tables) {
  return (tables || []).filter((t) => /^Operator Setup - /.test(t.name || ""));
}

function mainEvidenceFiles() {
  const files = [
    "api/lib/operator-setup-new-base-writer.js",
    "api/lib/operator-setup-new-base-read.js",
    "api/third-party-operator-intake.js",
    "api/third-party-operator-detail.js",
    "public/third-party-operator-setup-new-two.html",
    "public/js/operator-explorer-gold-mock-data.js",
    "public/js/operator-dna-view-model.js",
    "api/operator-capability-snapshot.js",
    "api/operator-alignment-snapshot.js",
    "public/js/operator-strategy-my-deals.js",
  ];
  const blobs = {};
  for (const f of files) {
    blobs[f] = exists(f) ? read(f) : "";
  }
  return blobs;
}

function includesField(blobs, field) {
  if (!field) return false;
  const needle = String(field);
  return Object.values(blobs).some((txt) => txt.includes(needle));
}

function usageFlags(blobs, field) {
  const f = String(field || "");
  const inFile = (name) => (blobs[name] || "").includes(f);
  return {
    explorer: inFile("public/js/operator-explorer-gold-mock-data.js") || inFile("api/operator-explorer.js"),
    capabilitySnapshot: inFile("api/operator-capability-snapshot.js"),
    alignmentSnapshot: inFile("api/operator-alignment-snapshot.js"),
    scoreBreakdown:
      inFile("api/operator-alignment-snapshot.js") ||
      inFile("public/js/operator-dna-view-model.js"),
    strategy: inFile("public/js/operator-strategy-my-deals.js"),
  };
}

async function run() {
  fs.mkdirSync(REPORTS, { recursive: true });
  const priorMapCsv = exists("reports/operator-setup-to-explorer-field-mapping-audit.csv")
    ? csvParse(read("reports/operator-setup-to-explorer-field-mapping-audit.csv"))
    : [];
  const uiRegistryCsv = exists("reports/operator-explorer-dna-ui-field-registry.csv")
    ? csvParse(read("reports/operator-explorer-dna-ui-field-registry.csv"))
    : [];
  const coverageDiffCsv = exists("reports/operator-setup-field-coverage-diff.csv")
    ? csvParse(read("reports/operator-setup-field-coverage-diff.csv"))
    : [];
  const buildSheet = exists("api/lib/operator-setup-new-base-build-sheet-rows.json")
    ? JSON.parse(read("api/lib/operator-setup-new-base-build-sheet-rows.json")).rows || []
    : [];
  const schemaBackup = exists("reports/operator-alignment-5b-schema-backup-2026-05-25.json")
    ? JSON.parse(read("reports/operator-alignment-5b-schema-backup-2026-05-25.json"))
    : null;

  const liveSchema = await fetchLiveSchema(process.env.AIRTABLE_BASE_ID, process.env.AIRTABLE_API_KEY);
  const liveSetupTables = liveSchema.ok ? pickOperatorSetupTables(liveSchema.tables) : [];
  const setupFromBackup = schemaBackup
    ? Object.values(schemaBackup.tables || {}).filter((t) => /^Operator Setup - /.test(t.name || ""))
    : [];
  const evidence = mainEvidenceFiles();

  // Stage 1 inventory
  const tableByName = new Map();
  for (const t of liveSetupTables) tableByName.set(t.name, t);
  for (const t of setupFromBackup) if (!tableByName.has(t.name)) tableByName.set(t.name, t);

  const invRows = [];
  const priorIndex = new Map(
    priorMapCsv.map((r) => [`${r["Airtable Table"]}::${r["Airtable Field"]}`, r])
  );
  for (const [tableName, t] of tableByName.entries()) {
    for (const f of t.fields || []) {
      const key = `${tableName}::${f.name}`;
      const prior = priorIndex.get(key);
      const inBuild = buildSheet.some(
        (b) => b.table_name === tableName && b.airtable_field_name === f.name
      );
      const inCurrentCode = includesField(evidence, f.name);
      const flags = usageFlags(evidence, f.name);
      const priorRead = prior ? prior["Displayed in Operator Explorer?"] : "";
      const conflict =
        prior && ((priorRead === "Yes" && !flags.explorer) || (priorRead === "No" && flags.explorer))
          ? "Possible prior-vs-current conflict"
          : "";
      invRows.push({
        airtable_table_name: tableName,
        airtable_table_id: t.id || "",
        airtable_field_name: f.name,
        airtable_field_id: f.id || "",
        airtable_field_type: f.type || "",
        select_options: "",
        read_by_code_current: inCurrentCode ? "Yes" : "No/Unclear",
        written_by_code_current: inBuild || inCurrentCode ? "Yes/Partial" : "No/Unclear",
        appears_in_ui_current: includesField({ "ui": evidence["public/third-party-operator-setup-new-two.html"] }, f.name)
          ? "Yes"
          : "No/Unclear",
        appears_in_snapshot_current:
          flags.capabilitySnapshot || flags.alignmentSnapshot ? "Yes/Partial" : "No/Unclear",
        appears_in_scoring_current: flags.scoreBreakdown ? "Yes/Partial" : "No/Unclear",
        classification:
          /^Operator$|created|updated|operator_id|submission_status/i.test(f.name)
            ? "System/link/derived"
            : "Needs Review",
        evidence_current_source:
          liveSchema.ok
            ? "Live Airtable meta + current repo scan"
            : `Current repo scan only (live schema unavailable: ${liveSchema.reason})`,
        evidence_prior_report: prior ? "operator-setup-to-explorer-field-mapping-audit.csv" : "",
        conflict_prior_vs_current: conflict,
        confirmation_status:
          liveSchema.ok ? "Partially Confirmed (schema + code scans)" : "Needs Review",
      });
    }
  }

  // Stage 2: UI -> Airtable map
  const buildIndex = new Map(buildSheet.map((b) => [b.form_name, b]));
  const stage2 = uiRegistryCsv.map((r) => {
    const formKey = r.formKey || "";
    const b = buildIndex.get(formKey);
    const inCurrentUi = formKey && evidence["public/third-party-operator-setup-new-two.html"].includes(formKey);
    const inCurrentWriter = formKey && evidence["api/lib/operator-setup-new-base-writer.js"].includes(formKey);
    return {
      page_tab_section: `${r.tab || ""} / ${r.subsection || ""}`,
      ui_label: r.uiKey || "",
      frontend_field_key_name_id: formKey,
      expected_airtable_table: b ? b.table_name : (r.airtableField ? "Needs Review" : ""),
      expected_airtable_field: b ? b.airtable_field_name : (r.airtableField || ""),
      api_route_used_to_save: "/api/third-party-operator-intake",
      request_payload_key: formKey,
      response_payload_key: "recordId, fields.companyName, fields.email",
      save_currently_implemented:
        b || inCurrentWriter ? "Yes/Partial" : "No/Unclear",
      persists_after_refresh: "Needs runtime validation",
      loads_existing_values: "Needs runtime validation",
      validation_present: inCurrentWriter ? "Yes/Partial" : "No/Unclear",
      select_option_consistency: "Needs Review",
      disconnected_or_mock_risk: !b && !inCurrentWriter ? "High" : "Low/Medium",
      mismatch_issue: !b && inCurrentUi ? "UI key not in new-base build-sheet mapping" : "",
      evidence_current_source: "Current UI + writer/build-sheet scan",
      evidence_prior_report: "operator-explorer-dna-ui-field-registry.csv",
      conflict_prior_vs_current:
        r.linkStatus && /Linked/.test(r.linkStatus) && !b && !inCurrentWriter
          ? "Prior report says linked; current mapping not found"
          : "",
      confirmation_status: b ? "Partially Confirmed" : "Needs Review",
    };
  });

  // Stage 3: downstream usage matrix (seed from coverage diff + inventory)
  const uniq = new Map();
  for (const r of coverageDiffCsv) {
    uniq.set(`${r["Airtable Table"]}::${r["Airtable Field"]}`, {
      table: r["Airtable Table"],
      field: r["Airtable Field"],
      type: r["Airtable Field Type"],
    });
  }
  for (const r of invRows) {
    uniq.set(`${r.airtable_table_name}::${r.airtable_field_name}`, {
      table: r.airtable_table_name,
      field: r.airtable_field_name,
      type: r.airtable_field_type,
    });
  }
  const stage3 = [];
  for (const item of uniq.values()) {
    const flags = usageFlags(evidence, item.field);
    const writerHit =
      evidence["api/lib/operator-setup-new-base-writer.js"].includes(item.field) ||
      evidence["api/third-party-operator-intake.js"].includes(item.field);
    const readerHit =
      evidence["api/lib/operator-setup-new-base-read.js"].includes(item.field) ||
      evidence["api/third-party-operator-detail.js"].includes(item.field);
    let cls = "Needs Review";
    if (writerHit && (flags.explorer || flags.capabilitySnapshot || flags.alignmentSnapshot)) cls = "Actively used";
    else if (writerHit && !flags.explorer && !flags.capabilitySnapshot && !flags.alignmentSnapshot) cls = "Written but not displayed";
    else if (!writerHit && (flags.explorer || flags.capabilitySnapshot || flags.alignmentSnapshot)) cls = "Displayed but not written";
    stage3.push({
      airtable_table: item.table,
      airtable_field: item.field,
      airtable_type: item.type || "",
      used_by_writer_current: writerHit ? "Yes" : "No/Unclear",
      used_by_reader_current: readerHit ? "Yes" : "No/Unclear",
      used_in_operator_explorer_current: flags.explorer ? "Yes" : "No/Unclear",
      used_in_capability_snapshot_current: flags.capabilitySnapshot ? "Yes/Partial" : "No/Unclear",
      used_in_alignment_snapshot_current: flags.alignmentSnapshot ? "Yes/Partial" : "No/Unclear",
      used_in_score_breakdown_current: flags.scoreBreakdown ? "Yes/Partial" : "No/Unclear",
      usage_classification: cls,
      missing_source_of_truth_mapping_risk:
        !writerHit && (flags.explorer || flags.capabilitySnapshot || flags.alignmentSnapshot)
          ? "High"
          : "Low/Medium",
      evidence_current_source: "Current repo scan",
      evidence_prior_report: "operator-setup-field-coverage-diff.csv",
      conflict_prior_vs_current: "",
      confirmation_status: "Needs Review",
    });
  }

  // Write outputs
  writeJson("operator-setup-field-inventory.json", {
    generatedAt: new Date().toISOString(),
    checkpoint: "A",
    stage: 1,
    liveSchema: liveSchema.ok ? "ok" : "unavailable",
    liveSchemaReason: liveSchema.ok ? "" : liveSchema.reason,
    rows: invRows,
  });
  writeCsv(
    "operator-setup-field-inventory.csv",
    invRows,
    Object.keys(invRows[0] || { note: "" })
  );
  writeMd(
    "operator-setup-field-inventory.md",
    `# Operator Setup Field Inventory (Checkpoint A)\n\n- Generated: ${new Date().toISOString()}\n- Live Airtable schema: ${liveSchema.ok ? "Available" : `Unavailable (${liveSchema.reason})`}\n- Rows: ${invRows.length}\n\nThis report distinguishes:\n1. current source evidence (repo and live schema if available)\n2. prior report evidence\n3. conflicts\n4. unconfirmed areas.\n\nSee CSV/JSON for full row-level details.\n`
  );

  writeJson("my-operator-input-to-airtable-map.json", {
    generatedAt: new Date().toISOString(),
    checkpoint: "A",
    stage: 2,
    rows: stage2,
  });
  writeCsv(
    "my-operator-input-to-airtable-map.csv",
    stage2,
    Object.keys(stage2[0] || { note: "" })
  );
  writeMd(
    "my-operator-input-to-airtable-map.md",
    `# My Operator Input -> Airtable Map (Checkpoint A)\n\n- Generated: ${new Date().toISOString()}\n- Rows: ${stage2.length}\n- Save route audited: \`/api/third-party-operator-intake\`\n\nThis is based on current UI/build-sheet/writer scans and prior registry references.\n`
  );

  writeJson("operator-field-usage-matrix.json", {
    generatedAt: new Date().toISOString(),
    checkpoint: "A",
    stage: 3,
    rows: stage3,
  });
  writeCsv(
    "operator-field-usage-matrix.csv",
    stage3,
    Object.keys(stage3[0] || { note: "" })
  );
  writeMd(
    "operator-field-usage-matrix.md",
    `# Operator Field Usage Matrix (Checkpoint A)\n\n- Generated: ${new Date().toISOString()}\n- Rows: ${stage3.length}\n\nCurrent-code usage flags are derived from current repo scans across writer/readers and downstream pages/routes.\n`
  );

  console.log(
    JSON.stringify(
      {
        ok: true,
        checkpoint: "A",
        files: [
          "reports/operator-setup-field-inventory.md",
          "reports/operator-setup-field-inventory.csv",
          "reports/operator-setup-field-inventory.json",
          "reports/my-operator-input-to-airtable-map.md",
          "reports/my-operator-input-to-airtable-map.csv",
          "reports/my-operator-input-to-airtable-map.json",
          "reports/operator-field-usage-matrix.md",
          "reports/operator-field-usage-matrix.csv",
          "reports/operator-field-usage-matrix.json",
        ],
        counts: { stage1: invRows.length, stage2: stage2.length, stage3: stage3.length },
      },
      null,
      2
    )
  );
}

run().catch((e) => {
  console.error(e);
  process.exit(1);
});

