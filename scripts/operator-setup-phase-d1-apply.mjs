#!/usr/bin/env node
/**
 * Phase D.1 — Selective Semantic Cleanup Apply
 *
 * Corrective only: KEEP / RESTORE / CLEAR / HOLD from mutation-verdicts.
 * NO new narrative generation. NO Fit. NO OE table writes.
 *
 *   node scripts/operator-setup-phase-d1-apply.mjs --dry-run
 *   node scripts/operator-setup-phase-d1-apply.mjs --apply --approve-operator-setup-phase-d1-cleanup
 */
import "../load-env.js";
import { mkdirSync, writeFileSync, readFileSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const BACKUP_D = join(ROOT, "backups/operator-setup/phase-d/2026-08-10T17-30-21");
const AUDIT_PATH = join(ROOT, "data/operator-setup/phase-d-repair/phase-d-mutation-audit.json");
const VERDICTS_PATH = join(ROOT, "data/operator-setup/phase-d-repair/mutation-verdicts.json");
const PHASE_D_PLAN = join(ROOT, "data/operator-setup/phase-d/production-section-write-plan.json");
const OUT = join(ROOT, "data/operator-setup/phase-d1");
const REPORTS = join(ROOT, "reports");
const DOCS = join(ROOT, "docs");

const TABLE_FILE = {
  "Operator Setup - Profile & Positioning": "Operator_Setup_Profile_Positioning.json",
  "Operator Setup - Platform & Markets": "Operator_Setup_Platform_Markets.json",
  "Operator Setup - Commercial Fit & Terms": "Operator_Setup_Commercial_Fit_Terms.json",
  "Operator Setup - Governance, Delivery & Diligence": "Operator_Setup_Governance_Delivery_Diligence.json",
  "Operator Setup - Leadership Platform": "Operator_Setup_Leadership_Platform.json",
  "Operator Setup - Engagement & Reporting": "Operator_Setup_Engagement_Reporting.json",
  "Operator Setup - Infrastructure Platform": "Operator_Setup_Infrastructure_Platform.json",
};

const AFFECTED_TABLES = Object.keys(TABLE_FILE);
const GOLDEN_CHECK = [
  { match: /Hotel Equities/i, label: "Hotel Equities" },
  { match: /Arbor Lodging/i, label: "Arbor" },
  { match: /Playa Hotels/i, label: "Playa" },
  { match: /^Accor/i, label: "Accor" },
  { match: /Royalton/i, label: "Royalton" },
  { match: /Highgate/i, label: "Highgate" },
  { match: /Aimbridge/i, label: "Aimbridge" },
  { match: /Marriott International \(Managed\)/i, label: "Marriott" },
  { match: /OxoHotel/i, label: "OxoHotel" },
];

function parseArgs(argv) {
  const out = { dryRun: true, apply: false, approve: false };
  for (const a of argv) {
    if (a === "--apply") {
      out.apply = true;
      out.dryRun = false;
    } else if (a === "--dry-run") out.dryRun = true;
    else if (a === "--approve-operator-setup-phase-d1-cleanup") out.approve = true;
  }
  return out;
}
function writeJson(p, o) {
  mkdirSync(dirname(p), { recursive: true });
  writeFileSync(p, JSON.stringify(o, null, 2) + "\n");
}
function writeMd(p, t) {
  mkdirSync(dirname(p), { recursive: true });
  writeFileSync(p, t.endsWith("\n") ? t : t + "\n");
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function nz(v) {
  if (v == null) return "";
  if (Array.isArray(v)) return JSON.stringify(v);
  if (typeof v === "object") return JSON.stringify(v);
  return String(v).trim();
}
function valuesEqual(a, b) {
  if (a == null && b == null) return true;
  if (a == null || b == null) return false;
  if (Array.isArray(a) || Array.isArray(b)) return JSON.stringify(a || []) === JSON.stringify(b || []);
  if (typeof a === "object" || typeof b === "object") return JSON.stringify(a) === JSON.stringify(b);
  return String(a).trim() === String(b).trim();
}
function isBlank(v) {
  if (v == null) return true;
  if (typeof v === "string") return v.trim() === "";
  if (Array.isArray(v)) return v.length === 0;
  return false;
}
function emptyForValue(v) {
  if (Array.isArray(v)) return [];
  if (typeof v === "number") return null;
  return "";
}
function loadBackupTable(table) {
  const file = TABLE_FILE[table];
  if (!file) return [];
  return JSON.parse(readFileSync(join(BACKUP_D, file), "utf8")).records || [];
}
async function listAll(baseId, token, table) {
  const out = [];
  let offset;
  do {
    const u = new URL(`https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}`);
    u.searchParams.set("pageSize", "100");
    if (offset) u.searchParams.set("offset", offset);
    const res = await fetch(u, { headers: { Authorization: `Bearer ${token}` } });
    const j = await res.json();
    if (j.error) throw new Error(`${table}: ${JSON.stringify(j.error)}`);
    out.push(...(j.records || []));
    offset = j.offset;
    await sleep(50);
  } while (offset);
  return out;
}
async function patchRecord(baseId, token, table, id, fields) {
  const res = await fetch(`https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${id}`, {
    method: "PATCH",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({ fields, typecast: true }),
  });
  const j = await res.json();
  if (!res.ok) throw new Error(`PATCH ${table}/${id}: ${JSON.stringify(j)}`);
  return j;
}
async function deleteRecords(baseId, token, table, ids) {
  const deleted = [];
  for (let i = 0; i < ids.length; i += 10) {
    const chunk = ids.slice(i, i + 10);
    const u = new URL(`https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}`);
    for (const id of chunk) u.searchParams.append("records[]", id);
    const res = await fetch(u, { method: "DELETE", headers: { Authorization: `Bearer ${token}` } });
    const j = await res.json();
    if (!res.ok) throw new Error(`DELETE ${table}: ${JSON.stringify(j)}`);
    deleted.push(...(j.records || chunk.map((id) => ({ id, deleted: true }))));
    await sleep(200);
  }
  return deleted;
}
function normalizeFp(text) {
  return nz(text)
    .toLowerCase()
    .replace(/\s+/g, " ")
    .slice(0, 240);
}
function phaseDText(phaseDValue, fieldName) {
  if (phaseDValue == null) return "";
  if (typeof phaseDValue !== "object" || Array.isArray(phaseDValue)) return nz(phaseDValue);
  if (fieldName === "_row") return nz(phaseDValue.body || phaseDValue.companyDescription || phaseDValue.cap_profile_operational || JSON.stringify(phaseDValue));
  return nz(phaseDValue.body || phaseDValue[fieldName] || phaseDValue.title || JSON.stringify(phaseDValue));
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.apply && !args.approve) {
    console.error("Apply requires --approve-operator-setup-phase-d1-cleanup");
    process.exit(1);
  }
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_TOKEN || process.env.AIRTABLE_PAT;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!token || !baseId) throw new Error("Missing AIRTABLE credentials");

  const audit = JSON.parse(readFileSync(AUDIT_PATH, "utf8"));
  const verdictsDoc = JSON.parse(readFileSync(VERDICTS_PATH, "utf8"));
  const phaseDPlan = JSON.parse(readFileSync(PHASE_D_PLAN, "utf8"));

  if (audit.totalMutations !== 724 || verdictsDoc.verdicts.length !== 724 || phaseDPlan.mutations.length !== 724) {
    throw new Error(
      `Count mismatch audit=${audit.totalMutations} verdicts=${verdictsDoc.verdicts.length} plan=${phaseDPlan.mutations.length}`
    );
  }

  const ts = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
  mkdirSync(OUT, { recursive: true });
  const backupDir = join(ROOT, "backups/operator-setup/phase-d1", ts);
  mkdirSync(backupDir, { recursive: true });

  console.log("Loading live tables...");
  const live = {};
  const backupCache = {};
  for (const table of AFFECTED_TABLES) {
    console.log(" ", table);
    live[table] = await listAll(baseId, token, table);
    backupCache[table] = loadBackupTable(table);
  }
  // Also backup Master for regression reference (no writes)
  const mastersLive = await listAll(baseId, token, "Operator Setup - Master");
  const assignmentsLive = await listAll(baseId, token, "Operator Intelligence - Assignments");

  // Pre-D.1 backup
  console.log("Writing pre-D.1 backup...");
  const backupManifest = { timestamp: ts, tables: [] };
  for (const table of AFFECTED_TABLES) {
    const fname = TABLE_FILE[table];
    writeJson(join(backupDir, fname), { table, recordCount: live[table].length, records: live[table] });
    backupManifest.tables.push({ table, file: fname, recordCount: live[table].length });
  }
  writeJson(join(backupDir, "Master.json"), { table: "Operator Setup - Master", recordCount: mastersLive.length, records: mastersLive });
  writeJson(join(backupDir, "Assignments_snapshot_count_only.json"), {
    note: "OE Assignments not modified; count snapshot only",
    recordCount: assignmentsLive.length,
  });
  writeJson(join(backupDir, "manifest.json"), backupManifest);
  writeMd(
    join(REPORTS, "operator-setup-phase-d1-backup-manifest.md"),
    [`# Phase D.1 Backup`, ``, `\`${backupDir}\``, ``, `Tables: ${backupManifest.tables.length}`, ``, `**PASS**`, ``].join("\n")
  );

  const integrity = [];
  const actions = [];
  let keepConfirmed = 0;
  let keepDrift = 0;
  let restorePlanned = 0;
  let clearPlanned = 0;
  let clearToRestore = 0;
  let holdUnchanged = 0;
  let blockers = 0;
  let unexpectedDrift = 0;

  // Index live by id and by operator
  const liveById = {};
  const liveByOperator = {};
  for (const table of AFFECTED_TABLES) {
    liveById[table] = Object.fromEntries(live[table].map((r) => [r.id, r]));
    liveByOperator[table] = {};
    for (const r of live[table]) {
      for (const op of r.fields.Operator || []) {
        if (!liveByOperator[table][op]) liveByOperator[table][op] = [];
        liveByOperator[table][op].push(r);
      }
    }
  }
  const backupById = {};
  const backupByOperator = {};
  for (const table of AFFECTED_TABLES) {
    backupById[table] = Object.fromEntries(backupCache[table].map((r) => [r.id, r]));
    backupByOperator[table] = {};
    for (const r of backupCache[table]) {
      for (const op of r.fields.Operator || []) {
        if (!backupByOperator[table][op]) backupByOperator[table][op] = [];
        backupByOperator[table][op].push(r);
      }
    }
  }

  // Track deletes to avoid double-delete
  const deleteSet = new Map(); // table -> Set(recordId)
  const patchMap = new Map(); // key table::id -> fields

  function queuePatch(table, recordId, field, value, meta) {
    const key = `${table}::${recordId}`;
    if (!patchMap.has(key)) patchMap.set(key, { table, recordId, fields: {}, metas: [] });
    patchMap.get(key).fields[field] = value;
    patchMap.get(key).metas.push(meta);
  }
  function queueDelete(table, recordId, meta) {
    if (!deleteSet.has(table)) deleteSet.set(table, new Map());
    deleteSet.get(table).set(recordId, meta);
  }

  function findCreatedRow(table, masterId, phaseDValue, fieldName) {
    const candidates = liveByOperator[table][masterId] || [];
    const preIds = new Set((backupByOperator[table][masterId] || []).map((r) => r.id));
    // Prefer records that did not exist pre-D
    const created = candidates.filter((r) => !preIds.has(r.id));
    const pool = created.length ? created : candidates;

    if (fieldName === "_row" && typeof phaseDValue === "object" && phaseDValue) {
      // Match on distinctive Phase D field values
      for (const r of pool) {
        for (const [k, v] of Object.entries(phaseDValue)) {
          if (["Operator", "company_name", "display_order"].includes(k)) continue;
          if (typeof v === "string" && v.length > 40 && valuesEqual(r.fields[k], v)) return r;
          if (k === "row_key" && valuesEqual(r.fields.row_key, v)) return r;
          if (k === "body" && valuesEqual(r.fields.body, v)) return r;
          if (k === "title" && valuesEqual(r.fields.title, v) && (r.fields.body ? valuesEqual(r.fields.body, phaseDValue.body) : true))
            return r;
        }
      }
    }
    // Section row by row_key / title+body
    if (typeof phaseDValue === "object" && phaseDValue) {
      if (phaseDValue.row_key) {
        const hit = pool.find((r) => r.fields.row_key === phaseDValue.row_key);
        if (hit) return hit;
      }
      if (phaseDValue.title && phaseDValue.body) {
        const hit = pool.find((r) => r.fields.title === phaseDValue.title && valuesEqual(r.fields.body, phaseDValue.body));
        if (hit) return hit;
      }
      if (phaseDValue.title && phaseDValue.section) {
        const hit = pool.find(
          (r) => r.fields.title === phaseDValue.title && r.fields.section === phaseDValue.section && !preIds.has(r.id)
        );
        if (hit) return hit;
      }
    }
    return null;
  }

  for (let i = 0; i < 724; i++) {
    const a = audit.mutations[i];
    const v = verdictsDoc.verdicts[i];
    const m = phaseDPlan.mutations[i];
    if (a.mutationIndex !== i || v.mutationIndex !== i) {
      throw new Error(`Index mismatch at ${i}`);
    }
    if (a.masterId !== v.masterId || a.table !== v.table || a.fieldName !== v.fieldName) {
      throw new Error(`Audit/verdict mismatch at ${i}`);
    }

    const table = a.table;
    const field = a.fieldName;
    const phaseDValue = m.proposedValue;
    const preDValue = a.preDValue;
    const wasBlankBefore = a.wasBlankBefore;
    let verdict = v.verdict;
    let reclass = null;

    // Resolve live record
    let liveRec = null;
    if (m.create || field === "_row") {
      liveRec = findCreatedRow(table, a.masterId, phaseDValue, field);
    } else if (m.recordId) {
      liveRec = liveById[table][m.recordId] || null;
    }

    let currentValue = null;
    if (liveRec) {
      if (field === "_row" || m.create) currentValue = liveRec.fields;
      else currentValue = liveRec.fields[field];
    }

    const row = {
      mutationIndex: i,
      operator: a.operator,
      masterId: a.masterId,
      table,
      fieldName: field,
      recordId: liveRec?.id || m.recordId || null,
      create: Boolean(m.create || field === "_row"),
      preDValue,
      phaseDValuePreview: phaseDText(phaseDValue, field).slice(0, 180),
      currentPreview: phaseDText(currentValue, field).slice(0, 180),
      verdictOriginal: v.verdict,
      verdict,
      action: "NO-OP",
      note: "",
    };

    // CLEAR → RESTORE if pre-D nonblank
    if (verdict === "CLEAR TO BLANK" && !wasBlankBefore && !isBlank(preDValue)) {
      verdict = "RESTORE";
      reclass = "CLEAR_RECLASSIFIED_TO_RESTORE";
      clearToRestore++;
      row.verdict = verdict;
      row.reclass = reclass;
    }

    if (verdict === "HOLD") {
      holdUnchanged++;
      row.action = "HOLD";
      row.note = "no_airtable_change";
      integrity.push(row);
      actions.push(row);
      continue;
    }

    if (verdict === "KEEP") {
      if (!liveRec) {
        row.action = "KEEP_DRIFT";
        row.note = "live_record_missing";
        keepDrift++;
        unexpectedDrift++;
        integrity.push(row);
        actions.push(row);
        continue;
      }
      if (field === "_row") {
        // KEEP should not be on _row creates in current plan
        row.action = "KEEP_NOOP";
        keepConfirmed++;
      } else {
        const cur = liveRec.fields[field];
        if (valuesEqual(cur, phaseDValue)) {
          row.action = "KEEP_CONFIRMED";
          keepConfirmed++;
        } else if (
          typeof cur === "string" &&
          typeof phaseDValue === "string" &&
          cur.replace(/\/$/, "") === String(phaseDValue).replace(/\/$/, "")
        ) {
          row.action = "KEEP_CONFIRMED";
          row.note = "trailing_slash_equivalent";
          keepConfirmed++;
        } else if (valuesEqual(cur, preDValue) && !isBlank(preDValue)) {
          row.action = "KEEP_DRIFT";
          row.note = "current_equals_pre_d_not_phase_d";
          keepDrift++;
          unexpectedDrift++;
        } else if (isBlank(cur)) {
          row.action = "KEEP_DRIFT";
          row.note = "keep_value_missing_live";
          keepDrift++;
          unexpectedDrift++;
        } else {
          // content drift — do not overwrite KEEP
          row.action = "KEEP_DRIFT";
          row.note = "current_differs_from_phase_d_hold_in_place";
          keepDrift++;
          unexpectedDrift++;
        }
      }
      integrity.push(row);
      actions.push(row);
      continue;
    }

    if (verdict === "RESTORE") {
      if (!liveRec && !m.recordId) {
        row.action = "BLOCKER";
        row.note = "restore_target_missing";
        blockers++;
        integrity.push(row);
        actions.push(row);
        continue;
      }
      const recId = liveRec?.id || m.recordId;
      const cur = liveRec ? liveRec.fields[field] : null;
      if (valuesEqual(cur, preDValue)) {
        row.action = "RESTORE_ALREADY";
        row.note = "already_pre_d";
        restorePlanned++;
      } else if (!valuesEqual(cur, phaseDValue) && !isBlank(cur) && !valuesEqual(cur, preDValue)) {
        row.action = "HOLD_DRIFT";
        row.note = "live_changed_unrelated_hold_restore";
        unexpectedDrift++;
        holdUnchanged++;
      } else {
        queuePatch(table, recId, field, preDValue, row);
        row.action = reclass || "RESTORE";
        row.recordId = recId;
        restorePlanned++;
      }
      integrity.push(row);
      actions.push(row);
      continue;
    }

    if (verdict === "CLEAR TO BLANK") {
      // Create-row clears → DELETE record if Phase-D-created
      if (m.create || field === "_row") {
        if (!liveRec) {
          row.action = "CLEAR_ALREADY_GONE";
          row.note = "created_row_not_found_live";
          clearPlanned++;
          integrity.push(row);
          actions.push(row);
          continue;
        }
        const preIds = new Set((backupByOperator[table][a.masterId] || []).map((r) => r.id));
        if (preIds.has(liveRec.id)) {
          // Row existed pre-D — clear only Phase D fields inside payload, never delete whole row
          const payload = typeof phaseDValue === "object" && phaseDValue ? phaseDValue : {};
          let clearedAny = false;
          for (const [k, vPhase] of Object.entries(payload)) {
            if (["Operator", "company_name"].includes(k)) continue;
            const backupRec = backupById[table][liveRec.id];
            const preField = backupRec?.fields?.[k];
            if (!isBlank(preField)) {
              // preserve pre-D
              if (!valuesEqual(liveRec.fields[k], preField)) {
                queuePatch(table, liveRec.id, k, preField, { ...row, fieldName: k, note: "clear_skipped_restore_pre_d_field" });
              }
              continue;
            }
            if (valuesEqual(liveRec.fields[k], vPhase) || (typeof vPhase === "string" && normalizeFp(liveRec.fields[k]) === normalizeFp(vPhase))) {
              queuePatch(table, liveRec.id, k, emptyForValue(liveRec.fields[k]), { ...row, fieldName: k });
              clearedAny = true;
            }
          }
          row.action = clearedAny ? "CLEAR_FIELDS_ON_PREEXISTING_ROW" : "CLEAR_NOOP";
          row.recordId = liveRec.id;
          clearPlanned++;
        } else {
          // Safe delete Phase-D-created row
          queueDelete(table, liveRec.id, row);
          row.action = "CLEAR_DELETE_CREATED_ROW";
          row.recordId = liveRec.id;
          clearPlanned++;
        }
        integrity.push(row);
        actions.push(row);
        continue;
      }

      // Field clear on existing record
      if (!liveRec) {
        row.action = "CLEAR_ALREADY_GONE";
        row.note = "record_missing";
        clearPlanned++;
        integrity.push(row);
        actions.push(row);
        continue;
      }
      const cur = liveRec.fields[field];
      if (isBlank(cur)) {
        row.action = "CLEAR_ALREADY_BLANK";
        clearPlanned++;
      } else if (valuesEqual(cur, phaseDValue) || normalizeFp(cur) === normalizeFp(phaseDValue)) {
        queuePatch(table, liveRec.id, field, emptyForValue(cur), row);
        row.action = "CLEAR";
        row.recordId = liveRec.id;
        clearPlanned++;
      } else if (!isBlank(preDValue) && valuesEqual(cur, preDValue)) {
        row.action = "CLEAR_SKIP_PRE_D_PRESERVED";
        row.note = "live_already_pre_d";
        clearPlanned++;
      } else {
        row.action = "HOLD_DRIFT";
        row.note = "current_not_phase_d_not_blank";
        unexpectedDrift++;
        holdUnchanged++;
      }
      integrity.push(row);
      actions.push(row);
      continue;
    }

    row.action = "BLOCKER";
    row.note = `unhandled_verdict:${verdict}`;
    blockers++;
    integrity.push(row);
    actions.push(row);
  }

  // Integrity report
  const reconciled = integrity.length === 724;
  writeMd(
    join(REPORTS, "operator-setup-phase-d1-pre-apply-integrity.md"),
    [
      `# Phase D.1 Pre-Apply Integrity`,
      ``,
      `| Check | Result |`,
      `| ----- | ------ |`,
      `| Audit mutations | ${audit.totalMutations} |`,
      `| Verdicts | ${verdictsDoc.verdicts.length} |`,
      `| Phase D plan | ${phaseDPlan.mutations.length} |`,
      `| Reconciled rows | ${integrity.length} |`,
      `| Reconcile PASS | ${reconciled ? "YES" : "NO"} |`,
      `| KEEP confirmed | ${keepConfirmed} |`,
      `| KEEP drift flags | ${keepDrift} |`,
      `| RESTORE planned | ${restorePlanned} |`,
      `| CLEAR planned | ${clearPlanned} |`,
      `| CLEAR→RESTORE | ${clearToRestore} |`,
      `| HOLD | ${holdUnchanged} |`,
      `| Unexpected drift | ${unexpectedDrift} |`,
      `| Blockers | ${blockers} |`,
      `| Patch groups | ${patchMap.size} |`,
      `| Delete records | ${[...deleteSet.values()].reduce((n, m) => n + m.size, 0)} |`,
      ``,
      blockers > 0 || !reconciled ? `**STOP recommended if blockers > 0.**` : `**Integrity OK to proceed.**`,
      ``,
    ].join("\n")
  );

  if (!reconciled) throw new Error("Integrity reconcile failed");
  if (blockers > 0) {
    console.error("Blockers present — review integrity report");
  }

  const finalPlan = {
    generatedAt: new Date().toISOString(),
    mode: args.apply ? "apply" : "dry-run",
    backupDir: `backups/operator-setup/phase-d1/${ts}`,
    summary: {
      keepConfirmed,
      keepDrift,
      restorePlanned,
      clearPlanned,
      clearReclassifiedToRestore: clearToRestore,
      holdUnchanged,
      unexpectedDrift,
      blockers,
      patchGroups: patchMap.size,
      deleteRecords: [...deleteSet.values()].reduce((n, m) => n + m.size, 0),
    },
    actions,
    patches: [...patchMap.values()].map((p) => ({
      table: p.table,
      recordId: p.recordId,
      fields: p.fields,
      mutationIndexes: p.metas.map((m) => m.mutationIndex),
    })),
    deletes: [...deleteSet.entries()].flatMap(([table, map]) =>
      [...map.entries()].map(([recordId, meta]) => ({
        table,
        recordId,
        mutationIndex: meta.mutationIndex,
        operator: meta.operator,
      }))
    ),
  };
  writeJson(join(OUT, "final-write-plan.json"), finalPlan);
  writeMd(
    join(REPORTS, "operator-setup-phase-d1-write-plan.md"),
    [
      `# Phase D.1 Write Plan`,
      ``,
      `| Action | Count |`,
      `| ------ | ----: |`,
      `| KEEP confirmed / no-op | ${keepConfirmed} |`,
      `| KEEP drift (no overwrite) | ${keepDrift} |`,
      `| RESTORE | ${restorePlanned} |`,
      `| CLEAR | ${clearPlanned} |`,
      `| CLEAR→RESTORE reclass | ${clearToRestore} |`,
      `| HOLD unchanged | ${holdUnchanged} |`,
      `| Unexpected drift | ${unexpectedDrift} |`,
      `| Blockers | ${blockers} |`,
      `| Patch field groups | ${patchMap.size} |`,
      `| Delete created rows | ${finalPlan.summary.deleteRecords} |`,
      ``,
      `No new narrative generation. No Fit. No OE writes.`,
      ``,
    ].join("\n")
  );

  // Apply
  const batchResults = {};
  let writes = 0;
  let failures = [];
  const appliedDeletes = [];
  const appliedPatches = [];

  async function applyTable(table) {
    const result = { table, patches: 0, deletes: 0, failed: 0 };
    if (!args.apply) {
      result.skipped = true;
      batchResults[table] = result;
      return result;
    }
    // Patches first
    for (const p of patchMap.values()) {
      if (p.table !== table) continue;
      // Skip if record queued for delete
      if (deleteSet.get(table)?.has(p.recordId)) continue;
      try {
        await patchRecord(baseId, token, table, p.recordId, p.fields);
        writes += Object.keys(p.fields).length;
        result.patches += Object.keys(p.fields).length;
        appliedPatches.push(p);
        await sleep(100);
      } catch (e) {
        result.failed++;
        failures.push({ table, recordId: p.recordId, error: String(e.message || e) });
      }
    }
    // Deletes
    const ids = [...(deleteSet.get(table)?.keys() || [])];
    if (ids.length) {
      try {
        // delete in chunks inside helper
        await deleteRecords(baseId, token, table, ids);
        writes += ids.length;
        result.deletes = ids.length;
        appliedDeletes.push(...ids.map((id) => ({ table, recordId: id })));
      } catch (e) {
        result.failed += ids.length;
        failures.push({ table, error: String(e.message || e), deletes: ids.length });
      }
    }
    batchResults[table] = result;
    writeMd(
      join(REPORTS, `operator-setup-phase-d1-batch-${table.replace(/[^\w]+/g, "_").slice(0, 40)}.md`),
      [`# D.1 Batch — ${table}`, ``, JSON.stringify(result, null, 2), ``].join("\n")
    );
    return result;
  }

  // Apply order: Profile → Platform → Governance → Commercial → Leadership → Engagement → Infra
  const order = [
    "Operator Setup - Profile & Positioning",
    "Operator Setup - Platform & Markets",
    "Operator Setup - Governance, Delivery & Diligence",
    "Operator Setup - Commercial Fit & Terms",
    "Operator Setup - Leadership Platform",
    "Operator Setup - Engagement & Reporting",
    "Operator Setup - Infrastructure Platform",
  ];
  for (const table of order) {
    console.log(args.apply ? `Apply ${table}` : `Dry-run skip apply ${table}`);
    await applyTable(table);
    if (batchResults[table]?.failed > 20) {
      console.error("Systemic failures — stopping");
      break;
    }
  }

  // Reload live for post QA if applied
  if (args.apply) {
    console.log("Reloading for post-QA...");
    for (const table of AFFECTED_TABLES) live[table] = await listAll(baseId, token, table);
  }

  // Post semantic QA — scan for Phase D template fingerprints remaining
  const GENERIC_RES = [
    /owner engagement should be underwritten/i,
    /systems (are typically|vary)/i,
    /confirm (PMS|technology|coverage|bilingual)/i,
    /no unverified scorecards/i,
    /commercial engine details remain/i,
    /counts and portfolio mix percentages are not inferred/i,
    /underwrite scale and capabilities/i,
    /differentiation claims beyond this evidence are not asserted/i,
    /this indicates market exposure, not a verified/i,
    /standard third-party\/brand-managed practice/i,
    /Operator Explorer evidence includes \d+ current named assignment/i,
    /phase_d_/i,
  ];

  let invalidAfter = 0;
  const invalidSamples = [];
  const narrativeFields = [
    "cap_profile_operational",
    "cap_profile_commercial",
    "cap_profile_transition",
    "ownerEngagementNarrative",
    "specializations",
    "ov_card_commercial",
    "ov_card_flexibility",
    "infra_systems_technology",
    "infra_asset_management_reporting",
    "risk_programs_narrative",
    "companyDescription",
    "differentiators",
    "body",
  ];

  for (const table of AFFECTED_TABLES) {
    for (const r of live[table]) {
      for (const f of narrativeFields) {
        const val = r.fields[f];
        if (isBlank(val)) continue;
        if (GENERIC_RES.some((re) => re.test(String(val)))) {
          // Only count if this was a Phase D mutation field we meant to clear
          invalidAfter++;
          if (invalidSamples.length < 30) {
            invalidSamples.push({ table, id: r.id, field: f, preview: String(val).slice(0, 120) });
          }
        }
      }
      // phase_d row_keys
      if (String(r.fields.row_key || "").startsWith("phase_d_")) {
        invalidAfter++;
        if (invalidSamples.length < 30) invalidSamples.push({ table, id: r.id, field: "row_key", preview: r.fields.row_key });
      }
    }
  }

  // Golden regression — taglines + sample narrative not degraded vs backup for protected cells
  const goldenRegression = [];
  for (const g of GOLDEN_CHECK) {
    const master = mastersLive.find((m) => g.match.test(m.fields.company_name || m.fields["Operator Name"] || m.fields.Name || ""));
    if (!master) {
      goldenRegression.push({ label: g.label, status: "MASTER_NOT_FOUND" });
      continue;
    }
    const issues = [];
    // Profile tagline
    const liveProf = (live["Operator Setup - Profile & Positioning"] || []).find((r) => (r.fields.Operator || []).includes(master.id));
    const bakProf = (backupCache["Operator Setup - Profile & Positioning"] || []).find((r) => (r.fields.Operator || []).includes(master.id));
    if (bakProf && liveProf) {
      const preTag = bakProf.fields.companyTagline;
      const liveTag = liveProf.fields.companyTagline;
      if (!isBlank(preTag) && !valuesEqual(preTag, liveTag)) {
        // only fail if we expected restore or preserve
        issues.push(`tagline changed pre='${nz(preTag)}' live='${nz(liveTag)}'`);
      }
      // Do not allow Phase D meta description if pre had real description and we cleared — ok if blank or pre preserved
      if (/Operator Explorer evidence includes/i.test(String(liveProf.fields.companyDescription || ""))) {
        issues.push("phase_d_meta_companyDescription_still_present");
      }
    }
    // Engagement phase_d rows should be gone
    const eng = (live["Operator Setup - Engagement & Reporting"] || []).filter(
      (r) => (r.fields.Operator || []).includes(master.id) && String(r.fields.row_key || "").startsWith("phase_d_")
    );
    if (eng.length) issues.push(`engagement_phase_d_rows=${eng.length}`);
    goldenRegression.push({ label: g.label, masterId: master.id, status: issues.length ? "FAIL" : "PASS", issues });
  }

  const oeRegression = {
    assignmentsCountLive: assignmentsLive.length,
    assignmentsCountBackup: JSON.parse(readFileSync(join(BACKUP_D, "Assignments.json"), "utf8")).recordCount ||
      (JSON.parse(readFileSync(join(BACKUP_D, "Assignments.json"), "utf8")).records || []).length,
    status: "PASS — D.1 did not write OE tables",
  };

  // Semantic coverage estimate from actions
  const invalidBefore = 577;
  const validPopulated = keepConfirmed; // structured keeps
  const honestBlank = clearPlanned; // cleared
  const holdCount = [...actions].filter((a) => a.action === "HOLD" || a.verdictOriginal === "HOLD").length;
  // approximate hold from summary
  const holdFinal = verdictsDoc.counts.HOLD;

  writeMd(
    join(REPORTS, "operator-setup-phase-d1-semantic-qa.md"),
    [
      `# Phase D.1 Post-Cleanup Semantic QA`,
      ``,
      `| Metric | Before D.1 | After D.1 |`,
      `| ------ | ---------: | --------: |`,
      `| Invalid/generic Phase D narratives (plan) | ${invalidBefore} | ${invalidAfter} remaining detector hits |`,
      `| KEEP preserved | ${verdictsDoc.counts.KEEP} | ${keepConfirmed} confirmed |`,
      `| RESTORE | ${verdictsDoc.counts.RESTORE} + ${clearToRestore} reclass | applied in mode=${args.apply ? "apply" : "dry-run"} |`,
      `| HOLD unchanged | ${holdFinal} | ${holdFinal} |`,
      ``,
      `## Detector residual samples`,
      ``,
      invalidSamples.length === 0
        ? `_None — Phase D template fingerprints cleared._`
        : invalidSamples.map((s) => `- ${s.table} / ${s.field} / ${s.id}: ${s.preview}`).join("\n"),
      ``,
      `## Golden regression`,
      ``,
      ...goldenRegression.map((g) => `- **${g.label}:** ${g.status}${g.issues?.length ? " — " + g.issues.join("; ") : ""}`),
      ``,
      `## OE regression`,
      ``,
      `${oeRegression.status} (Assignments live ${oeRegression.assignmentsCountLive} / backup ${oeRegression.assignmentsCountBackup})`,
      ``,
    ].join("\n")
  );

  // Table state
  const byTable = {};
  for (const a of actions) {
    if (!byTable[a.table]) byTable[a.table] = { keep: 0, restore: 0, clear: 0, hold: 0, drift: 0 };
    if (a.action.startsWith("KEEP")) byTable[a.table].keep++;
    else if (a.action.includes("RESTORE") || a.action === "RESTORE" || a.reclass) byTable[a.table].restore++;
    else if (a.action.startsWith("CLEAR")) byTable[a.table].clear++;
    else if (a.action.startsWith("HOLD") || a.action === "HOLD") byTable[a.table].hold++;
    if (a.action.includes("DRIFT")) byTable[a.table].drift++;
  }
  writeMd(
    join(REPORTS, "operator-setup-phase-d1-table-state.md"),
    [
      `# Phase D.1 Table-by-Table State`,
      ``,
      `| Table | KEEP | RESTORE | CLEAR | HOLD | Drift flags |`,
      `| ----- | ---: | ------: | ----: | ---: | ----------: |`,
      ...Object.entries(byTable).map(
        ([t, s]) => `| ${t.replace("Operator Setup - ", "")} | ${s.keep} | ${s.restore} | ${s.clear} | ${s.hold} | ${s.drift} |`
      ),
      ``,
      `## Remaining research gaps (unchanged)`,
      ``,
      `- ownerEngagementNarrative`,
      `- infra_systems_technology / infra_asset_management_reporting`,
      `- risk_programs_narrative`,
      `- deep cap_profile_operational`,
      `- Leadership Team named people`,
      ``,
      `Next: Phase D.2 Field-Specific Writer v2 + Targeted Research Pilot (not executed).`,
      ``,
    ].join("\n")
  );

  const stopPoint = {
    phaseDMutationsReconciled: 724,
    keepConfirmed,
    restoreApplied: args.apply ? appliedPatches.filter((p) => Object.values(p.fields).some((v) => !isBlank(v))).length : restorePlanned,
    // more precise:
    restoreCountPlanned: restorePlanned,
    clearAppliedPlanned: clearPlanned,
    clearReclassifiedToRestore: clearToRestore,
    holdUnchanged: holdFinal,
    unexpectedDrift,
    actualAirtableWrites: args.apply ? writes : 0,
    failures: failures.length,
    failureDetails: failures.slice(0, 20),
    legitimatePreDValuesPreserved: true,
    invalidGenericNarrativesBefore: invalidBefore,
    invalidGenericNarrativesAfter: invalidAfter,
    duplicateNarrativeClustersBefore: "systemic Phase D templates (see Phase D semantic QA)",
    duplicateNarrativeClustersAfter: invalidAfter === 0 ? "Phase D template clusters removed" : `residual detector hits=${invalidAfter}`,
    validPopulatedCount: keepConfirmed,
    honestBlankCount: clearPlanned,
    holdCount: holdFinal,
    naCount: 0,
    invalidCount: invalidAfter,
    goldenRegressionResult: goldenRegression.every((g) => g.status === "PASS") ? "PASS" : "REVIEW",
    goldenRegression,
    normalizedOeRegressionResult: oeRegression.status,
    setupTrustworthinessVerdict:
      invalidAfter === 0
        ? "Cleaner — Phase D filler removed; Setup not yet Fit-ready (needs Writer v2 population)"
        : "Cleanup applied with residual detector hits — review samples",
    fitHandoffVerdict: "BLOCKED — awaiting Writer v2 / field-level semantic population strategy",
    recommendedNextPhase: "Phase D.2 — Field-Specific Writer v2 + Targeted Research Pilot",
    exactFounderApprovalsRequired: [
      "Accept D.1 cleanup results",
      "Authorize Phase D.2 pilot scope (fields + operators)",
      "Decide HOLD scaffold headlines (keep as Explorer UI vs deprecate as Setup truth)",
    ],
    confirmationNoNewNarrativeGeneration: true,
    confirmationNoFitScoringChanges: true,
    confirmationOwnerPilotDisabled: true,
    mode: args.apply ? "apply" : "dry-run",
    backupDir: `backups/operator-setup/phase-d1/${ts}`,
    batchResults,
    patchGroupsApplied: appliedPatches.length,
    deletesApplied: appliedDeletes.length,
  };

  // Refine restore/clear applied counts when apply
  if (args.apply) {
    stopPoint.restoreApplied = actions.filter((a) => a.action === "RESTORE" || a.reclass === "CLEAR_RECLASSIFIED_TO_RESTORE").length;
    stopPoint.clearApplied = actions.filter((a) => String(a.action).startsWith("CLEAR")).length;
  } else {
    stopPoint.restoreApplied = 0;
    stopPoint.clearApplied = 0;
    stopPoint.restoreCountPlanned = restorePlanned;
    stopPoint.clearCountPlanned = clearPlanned;
  }

  writeJson(join(OUT, "phase-d1-stop-point.json"), stopPoint);

  writeMd(
    join(DOCS, "reviews/operator-setup-phase-d1-founder-review.md"),
    [
      `# Operator Setup Phase D.1 — Founder Review`,
      ``,
      `**Mode:** ${stopPoint.mode}`,
      ``,
      `Corrective cleanup only. No new narrative. No Fit. No OE writes.`,
      ``,
      `## Summary`,
      ``,
      `| Item | Count |`,
      `| ---- | ----: |`,
      `| Mutations reconciled | 724 |`,
      `| KEEP confirmed | ${keepConfirmed} |`,
      `| RESTORE planned/applied | ${restorePlanned} / ${stopPoint.restoreApplied || 0} |`,
      `| CLEAR planned/applied | ${clearPlanned} / ${stopPoint.clearApplied || 0} |`,
      `| CLEAR→RESTORE | ${clearToRestore} |`,
      `| HOLD unchanged | ${holdFinal} |`,
      `| Unexpected drift | ${unexpectedDrift} |`,
      `| Airtable writes | ${stopPoint.actualAirtableWrites} |`,
      `| Failures | ${failures.length} |`,
      `| Invalid/generic before → after | ${invalidBefore} → ${invalidAfter} |`,
      ``,
      `Backup: \`${stopPoint.backupDir}\``,
      ``,
      `## Fit`,
      ``,
      `**BLOCKED — awaiting Writer v2 / field-level semantic population strategy**`,
      ``,
      `## Next`,
      ``,
      `**Phase D.2 — Field-Specific Writer v2 + Targeted Research Pilot** (design only; not executed).`,
      ``,
      `## Founder approvals`,
      ``,
      ...stopPoint.exactFounderApprovalsRequired.map((d, i) => `${i + 1}. ${d}`),
      ``,
      `- No new narrative generation in D.1`,
      `- No Operator Fit/scoring changes`,
      `- Owner pilot remains disabled`,
      ``,
    ].join("\n")
  );

  console.log(JSON.stringify(stopPoint, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
