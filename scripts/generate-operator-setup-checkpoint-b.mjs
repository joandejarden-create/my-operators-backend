#!/usr/bin/env node
import fs from "fs";
import path from "path";
import "dotenv/config";

const ROOT = process.cwd();
const REPORTS = path.join(ROOT, "reports");

const readJson = (p) => JSON.parse(fs.readFileSync(path.join(ROOT, p), "utf8"));
const writeJson = (p, v) => fs.writeFileSync(path.join(ROOT, p), JSON.stringify(v, null, 2));
const writeMd = (p, v) => fs.writeFileSync(path.join(ROOT, p), v);
const esc = (s) => {
  const v = String(s ?? "");
  return /[",\n]/.test(v) ? `"${v.replace(/"/g, '""')}"` : v;
};
const writeCsv = (p, rows) => {
  const headers = Object.keys(rows[0] || {});
  const lines = [headers.join(",")];
  for (const r of rows) lines.push(headers.map((h) => esc(r[h])).join(","));
  fs.writeFileSync(path.join(ROOT, p), lines.join("\n"));
};

async function fetchOperatorSetupMeta() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) return { ok: false, reason: "Missing AIRTABLE_BASE_ID/API_KEY", tables: [] };
  try {
    const res = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    if (!res.ok) return { ok: false, reason: `Meta API ${res.status}`, tables: [] };
    const data = await res.json();
    const setup = (data.tables || []).filter((t) => /^Operator Setup - /.test(t.name || ""));
    return { ok: true, reason: "", tables: setup };
  } catch (e) {
    return { ok: false, reason: e.message || String(e), tables: [] };
  }
}

function makeIssue({
  id,
  priority,
  stage,
  issue,
  table = "",
  field = "",
  page = "",
  file = "",
  currentBehavior = "",
  intendedBehavior = "",
  risk = "Medium",
  fix = "",
  schemaChange = "No",
  dataMigration = "No",
  codeChange = "Yes",
  uiChange = "Maybe",
  scoringSnapshotChange = "Maybe",
  test = "",
  evidenceCurrent = "",
  evidencePrior = "",
  conflict = "",
  needsReview = false,
}) {
  return {
    id,
    priority,
    stage,
    issue,
    affected_table: table,
    affected_field: field,
    affected_page_function: page,
    affected_file_or_api_route: file,
    current_behavior: currentBehavior,
    intended_behavior: intendedBehavior,
    risk_level: risk,
    recommended_fix: fix,
    airtable_schema_change_needed: schemaChange,
    data_migration_needed: dataMigration,
    code_change_needed: codeChange,
    ui_change_needed: uiChange,
    scoring_or_snapshot_change_needed: scoringSnapshotChange,
    recommended_test: test,
    evidence_current_source: evidenceCurrent,
    evidence_prior_report: evidencePrior,
    conflict_prior_vs_current: conflict,
    needs_review: needsReview ? "Yes" : "No",
  };
}

async function run() {
  fs.mkdirSync(REPORTS, { recursive: true });
  const inv = readJson("reports/operator-setup-field-inventory.json");
  const inMap = readJson("reports/my-operator-input-to-airtable-map.json");
  const usage = readJson("reports/operator-field-usage-matrix.json");
  const meta = await fetchOperatorSetupMeta();

  // Stage 4 - Missing field recommendations
  const missing = [];
  let id = 1;
  const s2 = inMap.rows || [];
  for (const r of s2) {
    if (r.expected_airtable_table === "Needs Review" && r.frontend_field_key_name_id && r.frontend_field_key_name_id !== "—") {
      missing.push({
        id: `MF-${id++}`,
        proposed_airtable_table: "Needs Review (likely Operator Setup split table)",
        proposed_field_name: r.frontend_field_key_name_id,
        proposed_field_type: "Needs Review",
        proposed_options: "",
        reason_needed: "UI input key appears present but mapping to Operator Setup table/field is unconfirmed.",
        dependent_pages_functions: r.page_tab_section,
        go_live_priority: "P0 if owner-facing; otherwise P1",
        derived_vs_manual: "Manual input (from My Operator UI)",
        recommended_sample_data: "Add representative operator values for QA once mapping is confirmed.",
        evidence_current_source: r.evidence_current_source,
        evidence_prior_report: r.evidence_prior_report,
        conflict_prior_vs_current: r.conflict_prior_vs_current,
        confirmation_status: "Needs Review",
      });
    }
  }

  // Stage 5 - Legacy/unused fields
  const legacy = [];
  for (const r of usage.rows || []) {
    const unused =
      r.used_by_writer_current === "No/Unclear" &&
      r.used_by_reader_current === "No/Unclear" &&
      r.used_in_operator_explorer_current === "No/Unclear" &&
      r.used_in_capability_snapshot_current === "No/Unclear" &&
      r.used_in_alignment_snapshot_current === "No/Unclear" &&
      r.used_in_score_breakdown_current === "No/Unclear";
    if (!unused) continue;
    legacy.push({
      airtable_table: r.airtable_table,
      field_name: r.airtable_field,
      field_type: r.airtable_type,
      possible_duplicate_field: "",
      read_anywhere_current: r.used_by_reader_current,
      write_anywhere_current: r.used_by_writer_current,
      appears_in_api_payloads: "No/Unclear",
      appears_in_snapshots_or_docs: "No/Unclear",
      appears_in_scoring_logic: r.used_in_score_breakdown_current,
      contains_data_check: "Not checked in this stage (non-destructive)",
      removal_risk: /^Operator$|created|updated|operator_id|submission_status/i.test(r.airtable_field)
        ? "High"
        : "Unknown",
      recommendation: /^Operator$|created|updated|operator_id|submission_status/i.test(r.airtable_field)
        ? "Keep (system/link field)"
        : "Needs business review (candidate legacy/unused)",
      evidence_current_source: r.evidence_current_source,
      evidence_prior_report: r.evidence_prior_report,
      conflict_prior_vs_current: r.conflict_prior_vs_current || "",
      confirmation_status: "Needs Review",
    });
  }

  // Stage 6 - option normalization from live meta
  const norm = [];
  const optionFields = [];
  for (const t of meta.tables || []) {
    for (const f of t.fields || []) {
      if (!["singleSelect", "multipleSelects"].includes(f.type)) continue;
      const opts = (f.options && f.options.choices ? f.options.choices.map((c) => c.name) : []) || [];
      optionFields.push({ table: t.name, field: f.name, type: f.type, opts });
    }
  }
  const groups = [
    { key: "markets", re: /(market|country|region|geo|cala|gateway)/i },
    { key: "asset_types", re: /(asset|hotel type|property type|chain scale|mix)/i },
    { key: "service_model", re: /(service|model|operating|management structure|agreement)/i },
    { key: "fit_risk_confidence", re: /(fit|risk|confidence|status|strength|maturity|readiness)/i },
  ];
  for (const g of groups) {
    const fsIn = optionFields.filter((x) => g.re.test(x.field));
    const sig = new Set(fsIn.map((x) => x.opts.join("|")));
    if (fsIn.length && sig.size > 1) {
      norm.push({
        field_group: g.key,
        current_options_found: fsIn.map((x) => `${x.table}::${x.field} => [${x.opts.join(" | ")}]`).join(" || "),
        recommended_standard_options: "Needs product decision; align option taxonomy per field semantics.",
        fields_to_apply_standard: fsIn.map((x) => `${x.table}::${x.field}`).join("; "),
        fields_to_exclude: "",
        data_migration_notes: "Map legacy labels to standard labels via migration script; do not delete old values until verified.",
        code_changes_needed: "Update validation maps and form option lists.",
        scoring_impact: g.key === "fit_risk_confidence" ? "Likely" : "Possible",
        ui_filter_impact: "Likely",
        airtable_form_view_impact: "Likely",
        evidence_current_source: meta.ok ? "Live Airtable meta option lists" : `Meta unavailable: ${meta.reason}`,
        evidence_prior_report: "operator-alignment-live-airtable-options.json (reference only)",
        conflict_prior_vs_current: "",
        confirmation_status: meta.ok ? "Partially Confirmed" : "Needs Review",
      });
    }
  }

  // Stage 7 - source-of-truth gaps (manual, evidence-backed)
  const sot = [
    {
      file: "api/operator-explorer.js",
      route_page_component: "/api/operator-explorer/operator",
      current_data_source: "MOCK_OPERATORS fallback when id is not rec* and OPERATOR_EXPLORER_ALLOW_MOCKS=true",
      expected_source_of_truth: "Operator Setup tables via /api/third-party-operator-detail",
      risk: "High",
      recommended_fix: "Disable mocks in prod and block non-rec IDs with explicit error.",
      safe_now_or_wait: "Safe now (config + guard)",
      evidence_current_source: "Current code",
      evidence_prior_report: "",
      conflict_prior_vs_current: "",
      confirmation_status: "Confirmed",
    },
    {
      file: "api/third-party-operator-intake.js",
      route_page_component: "/api/third-party-operator-intake",
      current_data_source: "Dual path: new-base writer (flag) + legacy Basics/split writes + optional shadow write",
      expected_source_of_truth: "Single new-base write path",
      risk: "High",
      recommended_fix: "Define one authoritative write mode for go-live and lock flags.",
      safe_now_or_wait: "Before go-live",
      evidence_current_source: "Current code",
      evidence_prior_report: "Prior audits assumed migration complete",
      conflict_prior_vs_current: "Migration appears partial in current code",
      confirmation_status: "Confirmed",
    },
    {
      file: "api/third-party-operator-detail.js",
      route_page_component: "/api/intake/third-party-operators/:recordId",
      current_data_source: "new-base read with legacy fallback path",
      expected_source_of_truth: "new-base only for Operator Setup records",
      risk: "Medium-High",
      recommended_fix: "Add read-path telemetry and explicit contract for record types; phase out legacy fallback.",
      safe_now_or_wait: "Before external demos",
      evidence_current_source: "Current code",
      evidence_prior_report: "",
      conflict_prior_vs_current: "",
      confirmation_status: "Confirmed",
    },
  ];

  // Stage 8 test plan
  const checklist = [
    "Create/update operator in My Operator (all tabs).",
    "Save and refresh; values reload correctly.",
    "Verify writes in Operator Setup tables (Master + 1:1 + child rows).",
    "Verify Operator Explorer uses same values (no fallback/mock).",
    "Verify Operator Capability Snapshot uses same values.",
    "Verify Operator Alignment Snapshot uses same values.",
    "Verify score breakdown consumes mapped operator fields.",
    "Verify select/multi-select options validate and persist.",
    "Verify blank fields degrade gracefully in downstream UI.",
    "Verify one realistic profile supports end-to-end lifecycle.",
  ];
  const testPlanJson = {
    generatedAt: new Date().toISOString(),
    checkpoint: "B",
    stagesCovered: [8],
    checklist: checklist.map((c, i) => ({ id: `E2E-${i + 1}`, step: c, status: "todo" })),
  };

  // Stage 9+10 prioritized fix plan
  const fixes = [];
  let fx = 1;
  fixes.push(
    makeIssue({
      id: `FIX-${fx++}`,
      priority: "P0",
      stage: "7/9",
      issue: "Operator Explorer can serve mock data path.",
      table: "Operator Setup (all)",
      field: "N/A",
      page: "Operator Explorer detail",
      file: "api/operator-explorer.js",
      currentBehavior: "Fallback to MOCK_OPERATORS when non-rec ID and allow-mocks flag enabled.",
      intendedBehavior: "Owner-facing explorer should resolve only source-of-truth Operator Setup records.",
      risk: "High",
      fix: "Disable mock fallback for prod and rec-id gate all detail lookups.",
      schemaChange: "No",
      dataMigration: "No",
      codeChange: "Yes",
      uiChange: "No",
      scoringSnapshotChange: "No",
      test: "Request explorer detail with non-rec id; must return explicit not-found.",
      evidenceCurrent: "api/operator-explorer.js",
    })
  );
  fixes.push(
    makeIssue({
      id: `FIX-${fx++}`,
      priority: "P0",
      stage: "7/9",
      issue: "Dual-write architecture can create source-of-truth drift.",
      table: "Operator Setup + legacy 3rd Party Operator tables",
      field: "Multiple",
      page: "My Operator save flow",
      file: "api/third-party-operator-intake.js",
      currentBehavior: "Flag-driven new-base/legacy/shadow combinations.",
      intendedBehavior: "Deterministic write contract to one canonical store.",
      risk: "High",
      fix: "Freeze go-live write mode to new-base primary and instrument failures.",
      schemaChange: "No",
      dataMigration: "Maybe",
      codeChange: "Yes",
      uiChange: "No",
      scoringSnapshotChange: "No",
      test: "Save representative payload and verify only canonical destination used.",
      evidenceCurrent: "api/third-party-operator-intake.js",
      needsReview: false,
    })
  );
  fixes.push(
    makeIssue({
      id: `FIX-${fx++}`,
      priority: "P1",
      stage: "4/9",
      issue: "My Operator fields with unresolved current mapping need explicit contract.",
      table: "Needs Review",
      field: "Multiple form keys",
      page: "My Operator tabs",
      file: "reports/my-operator-input-to-airtable-map.json",
      currentBehavior: "Rows flagged high risk/no clear mapping.",
      intendedBehavior: "Each UI key maps to exact table/field + readback path.",
      risk: "Medium-High",
      fix: "Resolve unresolved keys and update build-sheet bindings and/or UI names.",
      schemaChange: "Maybe",
      dataMigration: "Maybe",
      codeChange: "Yes",
      uiChange: "Yes",
      scoringSnapshotChange: "Possible",
      test: "Per-field save-refresh-verify in Airtable and Explorer.",
      evidenceCurrent: "Checkpoint A Stage 2 output",
      evidencePrior: "operator-explorer-dna-ui-field-registry.csv",
      needsReview: true,
    })
  );
  fixes.push(
    makeIssue({
      id: `FIX-${fx++}`,
      priority: "P1",
      stage: "6/9",
      issue: "Select/multi-select option sets diverge across semantically related fields.",
      table: "Operator Setup (multiple tables)",
      field: "Market/asset/service/fit-status option fields",
      page: "My Operator forms + filters + snapshots/scoring",
      file: "Live meta option scan + validation maps",
      currentBehavior: "Inconsistent option taxonomies increase save/filter/scoring mismatch risk.",
      intendedBehavior: "Normalized option families with controlled aliases.",
      risk: "Medium",
      fix: "Define canonical option sets and map old values non-destructively.",
      schemaChange: "Maybe",
      dataMigration: "Yes",
      codeChange: "Yes",
      uiChange: "Yes",
      scoringSnapshotChange: "Likely",
      test: "Option round-trip and scoring classification stability test.",
      evidenceCurrent: meta.ok ? "Live Airtable meta" : `Meta unavailable: ${meta.reason}`,
      needsReview: true,
    })
  );

  // Add P2 from legacy candidates
  let p2count = 0;
  for (const l of legacy.slice(0, 12)) {
    fixes.push(
      makeIssue({
        id: `FIX-${fx++}`,
        priority: "P2",
        stage: "5/9",
        issue: "Potential legacy/unused field candidate (deprecate-review only).",
        table: l.airtable_table,
        field: l.field_name,
        page: "N/A",
        file: "Stage 5 analysis",
        currentBehavior: "No clear read/write/display usage in current scan.",
        intendedBehavior: "Field lifecycle status documented and reviewed before any cleanup.",
        risk: l.removal_risk || "Unknown",
        fix: l.recommendation,
        schemaChange: "No (not yet)",
        dataMigration: "No",
        codeChange: "No",
        uiChange: "No",
        scoringSnapshotChange: "No",
        test: "Repo-wide grep + Airtable data presence check before deprecation.",
        evidenceCurrent: l.evidence_current_source,
        evidencePrior: l.evidence_prior_report,
        needsReview: true,
      })
    );
    p2count++;
  }

  // Stage 10 patch plan (proposal only)
  const patchPlan = [
    {
      order: 1,
      scope: "Guard rails",
      changes: [
        "Disable mock fallback in operator explorer API for production.",
        "Enforce rec-id-only detail lookup contract.",
      ],
      risk: "Low",
      touching: ["api/operator-explorer.js"],
    },
    {
      order: 2,
      scope: "Write path hardening",
      changes: [
        "Lock intake to canonical new-base path for go-live mode.",
        "Keep legacy shadow writes only in non-prod diagnostics mode.",
      ],
      risk: "Medium",
      touching: ["api/third-party-operator-intake.js", "env/feature-flag docs"],
    },
    {
      order: 3,
      scope: "Mapping completeness",
      changes: [
        "Resolve high-risk unmapped My Operator keys from Stage 2.",
        "Update build-sheet JSON/CSV and add validation assertions.",
      ],
      risk: "Medium-High",
      touching: ["api/lib/operator-setup-new-base-build-sheet-rows.json", "mapping scripts", "UI form names"],
    },
    {
      order: 4,
      scope: "Option normalization",
      changes: [
        "Define canonical option sets and alias maps (non-destructive migration).",
        "Update validators + UI option sources + scoring normalizers.",
      ],
      risk: "Medium-High",
      touching: ["field maps", "form option providers", "scoring normalizers"],
    },
  ];

  // Write Stage 4
  writeJson("reports/operator-missing-fields-recommendations.json", {
    generatedAt: new Date().toISOString(),
    checkpoint: "B",
    stage: 4,
    rows: missing,
  });
  writeCsv("reports/operator-missing-fields-recommendations.csv", missing.length ? missing : [{ note: "No rows" }]);
  writeMd(
    "reports/operator-missing-fields-recommendations.md",
    `# Stage 4 — Missing Field Recommendations\n\nGenerated: ${new Date().toISOString()}\n\nRows: ${missing.length}\n\nEvidence model: current repo + live schema first; prior reports reference-only.\n`
  );

  // Stage 5
  writeJson("reports/operator-legacy-unused-fields.json", {
    generatedAt: new Date().toISOString(),
    checkpoint: "B",
    stage: 5,
    rows: legacy,
  });
  writeCsv("reports/operator-legacy-unused-fields.csv", legacy.length ? legacy : [{ note: "No rows" }]);
  writeMd(
    "reports/operator-legacy-unused-fields.md",
    `# Stage 5 — Legacy / Duplicate / Unused Fields\n\nGenerated: ${new Date().toISOString()}\n\nRows: ${legacy.length}\n\nNo deletions recommended in this stage.\n`
  );

  // Stage 6
  writeJson("reports/operator-select-option-normalization.json", {
    generatedAt: new Date().toISOString(),
    checkpoint: "B",
    stage: 6,
    liveMeta: meta.ok ? "ok" : `unavailable: ${meta.reason}`,
    rows: norm,
  });
  writeCsv("reports/operator-select-option-normalization.csv", norm.length ? norm : [{ note: "No inconsistent groups detected or needs manual grouping" }]);
  writeMd(
    "reports/operator-select-option-normalization.md",
    `# Stage 6 — Select Option Normalization\n\nGenerated: ${new Date().toISOString()}\n- Live meta: ${meta.ok ? "Available" : `Unavailable (${meta.reason})`}\n- Rows: ${norm.length}\n`
  );

  // Stage 7
  writeJson("reports/operator-source-of-truth-gaps.json", {
    generatedAt: new Date().toISOString(),
    checkpoint: "B",
    stage: 7,
    rows: sot,
  });
  writeCsv("reports/operator-source-of-truth-gaps.csv", sot);
  writeMd(
    "reports/operator-source-of-truth-gaps.md",
    `# Stage 7 — Source-of-Truth Gaps\n\nGenerated: ${new Date().toISOString()}\n\nRows: ${sot.length}\n`
  );

  // Stage 8
  writeJson("reports/operator-setup-end-to-end-test-plan.json", testPlanJson);
  writeMd(
    "reports/operator-setup-end-to-end-test-plan.md",
    `# Stage 8 — End-to-End Validation Plan\n\n${checklist.map((x, i) => `${i + 1}. ${x}`).join("\n")}\n`
  );

  // Stage 9
  writeJson("reports/operator-setup-reconciliation-fix-plan.json", {
    generatedAt: new Date().toISOString(),
    checkpoint: "B",
    stage: 9,
    issues: fixes,
  });
  writeMd(
    "reports/operator-setup-reconciliation-fix-plan.md",
    `# Stage 9 — Prioritized Reconciliation Fix Plan\n\nGenerated: ${new Date().toISOString()}\n\n- P0: ${fixes.filter((x) => x.priority === "P0").length}\n- P1: ${fixes.filter((x) => x.priority === "P1").length}\n- P2: ${fixes.filter((x) => x.priority === "P2").length}\n\nSee JSON for detailed issue objects.\n`
  );

  // Stage 10 proposal-only
  writeJson("reports/operator-setup-proposed-patch-plan.json", {
    generatedAt: new Date().toISOString(),
    checkpoint: "B",
    stage: 10,
    proposalOnly: true,
    batches: patchPlan,
  });
  writeMd(
    "reports/operator-setup-proposed-patch-plan.md",
    `# Stage 10 — Proposed Safe Patch Plan (No Implementation)\n\nGenerated: ${new Date().toISOString()}\n\n${patchPlan
      .map(
        (b) =>
          `## Batch ${b.order}: ${b.scope}\n- Risk: ${b.risk}\n- Files/areas: ${b.touching.join(", ")}\n- Changes:\n${b.changes.map((c) => `  - ${c}`).join("\n")}\n`
      )
      .join("\n")}`
  );

  // Executive summary
  const p0 = fixes.filter((x) => x.priority === "P0");
  const p1 = fixes.filter((x) => x.priority === "P1");
  const p2 = fixes.filter((x) => x.priority === "P2");
  const doNotTouch = [
    "System/link fields in Operator Setup tables (Operator, operator_id, created_*, updated_*, submission_status).",
    "Legacy fallback columns still referenced by read/write alias logic until migration is complete.",
    "Scoring category fields before option normalization + regression validation.",
  ];
  writeMd(
    "reports/operator-setup-audit-summary.md",
    `# Operator Setup Audit Summary (Checkpoint B)\n\n1. Source of truth readiness: **Partially** — new-base architecture exists, but dual-path/mocks create drift risk.\n2. My Operator readiness: **Not fully** — unresolved field mappings remain (Needs Review set).\n3. Explorer source usage: **Partial** — live path is correct for rec IDs; mock fallback exists.\n4. Capability/Alignment snapshots: **Partially aligned** — consume deal+operator contexts but field lineage requires tightening for all UI keys.\n5. Score breakdown consistency: **Needs Review** for unmapped/alias-driven fields.\n6. P0 blockers: ${p0.map((x) => x.issue).join(" | ")}\n7. P1 before demos: ${p1.map((x) => x.issue).join(" | ")}\n8. Safe to wait (P2): ${p2.length} legacy/cleanup items.\n9. Fields not to touch yet: ${doNotTouch.join(" ")}\n10. Recommended first fix batch: Batch 1 (mock/path guard rails) then Batch 2 (write-mode hardening).\n`
  );

  console.log(
    JSON.stringify(
      {
        ok: true,
        checkpoint: "B",
        files: [
          "reports/operator-missing-fields-recommendations.md",
          "reports/operator-missing-fields-recommendations.csv",
          "reports/operator-missing-fields-recommendations.json",
          "reports/operator-legacy-unused-fields.md",
          "reports/operator-legacy-unused-fields.csv",
          "reports/operator-legacy-unused-fields.json",
          "reports/operator-select-option-normalization.md",
          "reports/operator-select-option-normalization.csv",
          "reports/operator-select-option-normalization.json",
          "reports/operator-source-of-truth-gaps.md",
          "reports/operator-source-of-truth-gaps.csv",
          "reports/operator-source-of-truth-gaps.json",
          "reports/operator-setup-end-to-end-test-plan.md",
          "reports/operator-setup-end-to-end-test-plan.json",
          "reports/operator-setup-reconciliation-fix-plan.md",
          "reports/operator-setup-reconciliation-fix-plan.json",
          "reports/operator-setup-proposed-patch-plan.md",
          "reports/operator-setup-proposed-patch-plan.json",
          "reports/operator-setup-audit-summary.md",
        ],
        counts: { stage4: missing.length, stage5: legacy.length, stage6: norm.length, stage7: sot.length, p0: p0.length, p1: p1.length, p2: p2.length },
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

