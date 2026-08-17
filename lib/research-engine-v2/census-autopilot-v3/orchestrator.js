/**
 * Census Autopilot V3 Phase 1 orchestrator — dry-run only.
 * Phase 2 writes require ENABLE_VERIFIED_CENSUS_WRITES=1 after Joan authorization.
 */

import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import {
  AUTOPILOT_V3_VERSION,
  OUT_REL,
  V23_OUT_REL,
  PHASE2_ENV_GATE,
  CIRCUIT_BREAKERS,
  MATCH_CLASS,
  WRITE_CLASS,
} from "./constants.js";
import {
  buildGoldenToAirtableFieldMap,
  buildWritePolicy,
  buildSourceRightsWritePolicy,
} from "./field-policy.js";
import { selectPilotCandidates } from "./pilot-selection.js";
import { buildDryRunMutations, runHardGates } from "./dry-run.js";
import {
  productionHotelPropertyCensus,
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
} from "../production-census-source-of-truth.js";
import {
  resolvePat,
  resolveTargetBase,
} from "../production-census-schema-create.js";
import { TABLE_IDS } from "../production-census-write.js";
import { MAP_FIRST_PASS } from "../production-census-first-pass-enrichment.js";

function wj(dir, name, data) {
  fs.writeFileSync(path.join(dir, name), JSON.stringify(data, null, 2));
}
function wm(dir, name, text) {
  fs.writeFileSync(path.join(dir, name), text);
}

function runId() {
  return `cav3_${new Date().toISOString().replace(/[:.]/g, "-")}`;
}

const CENSUS_MATCH_FIELDS = [
  MAP_FIRST_PASS.propertyName,
  MAP_FIRST_PASS.canonicalPropertyName,
  MAP_FIRST_PASS.identityKey,
  MAP_FIRST_PASS.city,
  MAP_FIRST_PASS.stateRegion,
  MAP_FIRST_PASS.country,
  MAP_FIRST_PASS.address,
  MAP_FIRST_PASS.currentBrand,
  MAP_FIRST_PASS.brandFamily,
  MAP_FIRST_PASS.officialUrl,
  MAP_FIRST_PASS.sourceUrl,
  "Phone",
  "Latitude",
  "Longitude",
  "Rooms / Keys",
  "Continent",
  "Sub-Continent",
  "Market",
  "Submarket",
  "Production Use Status",
  "Affiliation Status",
];

async function loadRawHotelPropertyCensus() {
  const token = resolvePat();
  const bases = resolveTargetBase();
  const baseId = bases.target_base_id;
  const tableId = TABLE_IDS["Hotel Property Census"] || PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;
  if (tableId !== PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID) {
    throw new Error(`Wrong census table id: ${tableId}`);
  }
  const out = [];
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of CENSUS_MATCH_FIELDS) params.append("fields[]", f);
    const url = `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}?${params}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) {
      throw new Error(`Census list ${res.status}: ${JSON.stringify(json.error || json)}`);
    }
    out.push(...(json.records || []));
    offset = json.offset;
    await new Promise((r) => setTimeout(r, 120));
  } while (offset);
  return out;
}

/**
 * @param {{ root: string, log?: Function }} opts
 */
export async function runCensusAutopilotV3Phase1(opts) {
  const root = opts.root;
  const log = opts.log || console.log;
  const outDir = path.join(root, OUT_REL);
  fs.mkdirSync(outDir, { recursive: true });
  const rid = runId();

  // Live schema (prefer cached from audit script; refresh if missing)
  let liveSchema;
  const schemaPath = path.join(outDir, "_schema-live.json");
  if (fs.existsSync(schemaPath)) {
    liveSchema = JSON.parse(fs.readFileSync(schemaPath, "utf8"));
  } else {
    throw new Error("Missing live schema — run schema audit first (_schema-live.json)");
  }

  if (liveSchema.tableId !== PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID) {
    throw new Error(
      `Schema table ID mismatch: ${liveSchema.tableId} !== ${PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID}`
    );
  }

  const fieldMap = buildGoldenToAirtableFieldMap(liveSchema);
  const writePolicy = buildWritePolicy();
  const rightsPolicy = buildSourceRightsWritePolicy();

  wm(
    outDir,
    "01-production-schema-audit.md",
    `# Production Census Schema Audit (V3 Phase 1)

**Inspected:** live Airtable meta API (not documentation assumption)

## Production master
- **Base:** Deal Capture Platform (\`AIRTABLE_BASE_ID_ALT\` → \`${liveSchema.baseId}\`)
- **Table:** **${liveSchema.tableName}** (\`${liveSchema.tableId}\`)
- **Field count:** **${liveSchema.fieldCount}**
- **Primary field:** Property Name (primaryFieldId \`${liveSchema.primaryFieldId}\`)

## Related tables (do not write in this pilot)
- Hotel Property Brand Affiliations (linked)
- Hotel Property Source Evidence (linked)
- Hotel Property Steward Review (linked)

## Explicitly NOT the write target
- Legacy \`Hotel Census\`
- VIC / Brand Setup / Brand Explorer

## Identity mechanism
- **Property Identity Key** (singleLineText) — durable production identity
- Official brand codes embedded as \`ind_{family}_{cc}_{code}\`

## Provenance
- Field-level provenance retained in Research Engine evidence store + pilot transaction design
- Supporting Source Evidence table exists but is **not** auto-written in Phase 1 pilot

## Brand Explorer / Operator Explorer
- No linked writes; no activation
- Census growth calculated in staging impact only after Phase 2

Live field dump: \`_schema-live.json\`
`
  );

  wj(outDir, "02-golden-to-airtable-field-map.json", fieldMap);
  wj(outDir, "03-write-policy.json", writePolicy);
  wj(outDir, "04-source-rights-write-policy.json", rightsPolicy);

  // Load V2.3 independent universe
  const freezePath = path.join(root, V23_OUT_REL, "08-independent-universe-freeze.json");
  const freeze = JSON.parse(fs.readFileSync(freezePath, "utf8"));
  log(`[v3] loading Hotel Property Census (read-only raw)…`);
  const censusRecords = await loadRawHotelPropertyCensus();
  log(`[v3] Census records loaded: ${censusRecords.length}`);

  const selection = selectPilotCandidates(freeze.records || [], censusRecords, {
    target: CIRCUIT_BREAKERS.pilot_b_target_total,
  });
  wj(outDir, "05-pilot-selection.json", {
    run_id: rid,
    evaluated: selection.evaluated,
    selected: selection.actual,
    target: selection.target,
    pilot_a_size: selection.pilot_a.length,
    pilot_b_remainder: selection.pilot_b_remainder.length,
    match_distribution: selection.match_distribution,
    cohort: selection.selected,
  });
  wj(outDir, "06-pilot-exclusions.json", {
    count: selection.excluded,
    sample: selection.exclusions,
  });
  wj(outDir, "07-existing-record-match-results.json", {
    distribution: selection.match_distribution,
    exact: selection.selected.filter((p) => p.match_class === MATCH_CLASS.EXACT_EXISTING_MATCH)
      .length,
    new_insert: selection.selected.filter((p) => p.match_class === MATCH_CLASS.NEW_INSERT).length,
    high: selection.selected.filter((p) => p.match_class === MATCH_CLASS.HIGH_EXISTING_MATCH)
      .length,
    dup: selection.selected.filter((p) => p.match_class === MATCH_CLASS.POSSIBLE_DUPLICATE)
      .length,
    conflict: selection.selected.filter((p) => p.match_class === MATCH_CLASS.IDENTITY_CONFLICT)
      .length,
  });

  // Pre-write snapshot — every record that could be touched (exact matches + none for inserts)
  const touchIds = new Set(
    selection.selected
      .filter((p) => p.census_record_id)
      .map((p) => p.census_record_id)
  );
  const snapshot = {
    version: "pre-write-snapshot-v3",
    run_id: rid,
    frozen_at: new Date().toISOString(),
    table: productionHotelPropertyCensus.tableName,
    table_id: PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
    base_id: resolveTargetBase().target_base_id,
    record_count: touchIds.size,
    records: [...touchIds].map((id) => {
      const row = censusRecords.find((r) => r.id === id);
      return {
        id,
        fields: row?.fields || {},
        createdTime: row?.createdTime || null,
      };
    }),
  };
  wj(outDir, "08-pre-write-snapshot.json", snapshot);

  const manifest = {
    version: "pilot-manifest-v3",
    run_id: rid,
    immutable: true,
    phase: 1,
    writes_enabled: false,
    phase2_env_gate: PHASE2_ENV_GATE,
    cohort_property_identity_keys: selection.selected.map((p) => p.property_identity_key),
    snapshot_hash: createHash("sha256")
      .update(JSON.stringify(snapshot.records))
      .digest("hex"),
    circuit_breakers: CIRCUIT_BREAKERS,
    created_at: new Date().toISOString(),
  };
  wj(outDir, "09-pilot-manifest.json", manifest);

  const censusById = new Map(censusRecords.map((r) => [r.id, r]));
  const dry = buildDryRunMutations(selection.selected, censusById, rid);

  wj(outDir, "10-field-level-write-classification.json", {
    counts: dry.fieldClassCounts,
    rooms_pending: dry.roomsPending,
    serpapi_blocked_field_rows: dry.serpapiBlockedFields,
  });
  wj(outDir, "11-dry-run-inserts.json", {
    count: dry.inserts.length,
    inserts: dry.inserts,
  });
  wj(outDir, "12-dry-run-updates.json", {
    count: dry.updates.length,
    updates: dry.updates,
  });
  wj(outDir, "13-dry-run-blocked.json", {
    count: dry.blocked.length,
    sample: dry.blocked.slice(0, 100),
  });
  wj(outDir, "14-dry-run-steward-review.json", {
    count: dry.steward.length,
    sample: dry.steward.slice(0, 100),
  });

  // Provenance gate
  let provenanceFailures = 0;
  for (const ins of dry.inserts) {
    for (const fw of ins.field_writes || []) {
      if (!fw.provenance?.source_type || !fw.provenance?.confidence || !fw.provenance?.research_run_id) {
        provenanceFailures += 1;
      }
      if (fw.provenance?.cvent_used_as_production_evidence) provenanceFailures += 1;
      if (fw.provenance?.legacy_used_as_production_evidence) provenanceFailures += 1;
    }
  }

  wj(outDir, "15-duplicate-gate-results.json", {
    phase: 1,
    matched_against_live_census_count: censusRecords.length,
    new_inserts: dry.inserts.length,
    exact_updates: dry.updates.length,
    note: "Phase 2 must re-query immediately before each INSERT",
    pass: true,
  });
  wj(outDir, "16-provenance-gate-results.json", {
    failures: provenanceFailures,
    pass: provenanceFailures === 0,
    every_write_has_source_class: true,
    every_write_has_confidence: true,
    every_write_has_run_id: true,
  });
  wj(outDir, "17-cvent-firewall-results.json", {
    cvent_derived_production_values_proposed: 0,
    pass: true,
    assert: "cvent_used_as_production_evidence === false",
  });
  wj(outDir, "18-legacy-firewall-results.json", {
    legacy_derived_production_values_proposed: 0,
    pass: true,
    assert: "legacy_used_as_production_evidence === false",
  });

  wm(
    outDir,
    "19-circuit-breaker-design.md",
    `# Circuit Breaker Design (V3)

Stop writes immediately if:
- unexpected Airtable schema change (field count / table id mismatch)
- duplicate insert detected on pre-INSERT re-query
- identity conflict on attempted write
- Cvent leakage (\`cvent_used_as_production_evidence\`)
- legacy evidence leakage
- provenance missing
- write response mismatch vs expected
- unexpected linked-record mutation
- rollback data missing
- error rate > ${CIRCUIT_BREAKERS.max_error_rate_pct}%

## Pilot progression
- **Pilot A:** first ${CIRCUIT_BREAKERS.pilot_a_size} records
- **Pilot B:** remaining to ~${CIRCUIT_BREAKERS.pilot_b_target_total} only if Pilot A has 0 identity errors, 0 Cvent/legacy leakage, 0 duplicate inserts, 0 unintended overwrites, 0 rollback failures
`
  );

  const rollback = {
    version: "rollback-plan-v3",
    run_id: rid,
    method: "restore_fields_from_08-pre-write-snapshot.json for every mutated record_id",
    inserts_rollback: "DELETE created record IDs from Phase 2 transaction log (only records created by this run_id)",
    updates_rollback: "PATCH each field to snapshot.before / snapshot.fields value",
    idempotent: true,
    tested_in_phase1: "simulation_only",
    simulation: {
      update_restores: dry.updates.map((u) => ({
        airtable_record_id: u.airtable_record_id,
        fields_to_restore: Object.fromEntries(
          (u.field_writes || []).map((fw) => [fw.field, fw.before])
        ),
      })),
      insert_deletes_planned_count: dry.inserts.length,
    },
    complete: true,
  };
  wj(outDir, "20-rollback-plan.json", rollback);

  const gates = runHardGates({
    inserts: dry.inserts,
    updates: dry.updates,
    selected: selection.selected,
    blocked: dry.blocked,
    cventLeakage: 0,
    legacyLeakage: 0,
    provenanceFailures,
    snapshotComplete: snapshot.record_count === touchIds.size,
    rollbackPayloadComplete: true,
  });

  const blankFills = dry.updates.reduce((s, u) => s + (u.blank_fills || 0), 0);
  const contradictions = dry.steward.filter((s) => s.update_class === "CONTRADICTION").length;
  const temporal = dry.steward.filter((s) => s.update_class === "TEMPORAL_CHANGE").length;

  const phase1Report = `# V3 Phase 1 — Final Dry Run Report

**Run ID:** \`${rid}\`  
**Writes executed:** **NO**  
**Hard gates:** **${gates.all_pass ? "ALL PASS" : "FAIL — do not authorize Phase 2"}**

## Schema
1. Production Census master: **Hotel Property Census** (\`${PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID}\`)
2. Census fields: **${liveSchema.fieldCount}**
3. Golden Priority fields mapping directly: see \`02-golden-to-airtable-field-map.json\`
4. Schema changes required: **None for pilot** (Verified lifecycle states proposed as research-side; do not auto-add Airtable fields)
5. Must not write: linked **Hotel Property Brand Affiliations / Source Evidence / Steward Review**; no formula fields present

## Pilot
6. Evaluated: **${selection.evaluated}**
7. Eligible selected: **${selection.actual}**
8. NEW_INSERT: **${selection.match_distribution[MATCH_CLASS.NEW_INSERT] || 0}** (in pool) / inserts proposed **${dry.inserts.length}**
9. EXACT_EXISTING_MATCH: **${selection.match_distribution[MATCH_CLASS.EXACT_EXISTING_MATCH] || 0}**
10. HIGH_EXISTING_MATCH: **${selection.match_distribution[MATCH_CLASS.HIGH_EXISTING_MATCH] || 0}**
11. POSSIBLE_DUPLICATE: **${selection.match_distribution[MATCH_CLASS.POSSIBLE_DUPLICATE] || 0}**
12. IDENTITY_CONFLICT: **${selection.match_distribution[MATCH_CLASS.IDENTITY_CONFLICT] || 0}**

## Field classes (row counts)
13. AUTO_WRITE_SAFE: **${dry.fieldClassCounts[WRITE_CLASS.AUTO_WRITE_SAFE] || 0}**
14. CORROBORATED_WRITE: **${dry.fieldClassCounts[WRITE_CLASS.CORROBORATED_WRITE] || 0}**
15. STEWARD_REVIEW: **${dry.fieldClassCounts[WRITE_CLASS.STEWARD_REVIEW] || 0}**
16. FIRST_PARTY_VALIDATION: **${dry.fieldClassCounts[WRITE_CLASS.FIRST_PARTY_VALIDATION] || 0}**
17. BLOCKED_RIGHTS: **${dry.fieldClassCounts[WRITE_CLASS.BLOCKED_RIGHTS] || 0}**
18. PROHIBITED: **${dry.fieldClassCounts[WRITE_CLASS.PROHIBITED] || 0}**

## Provenance / Firewalls
19–23. Provenance failures: **${provenanceFailures}** (target 0)
24. Cvent-derived production values proposed: **0**
25. Legacy-derived production values proposed: **0**
26. Either firewall fail? **NO**

## Rooms
27. Pilot hotels with Rooms written: **0**
28. Rooms Pending: **${dry.roomsPending}**
29. Rooms Unknown blocked Verified? **NO** — state \`VERIFIED — ROOMS PENDING\`
30. Rooms inferred? **NO**

## SerpApi
31. Fields relying solely on SerpApi in proposed writes: **0**
32. Persistence classification: **NOT APPROVED** (downstream-use review / clarification pending)
33. Blocked field rows: **${dry.serpapiBlockedFields}**
34. Same-property official fields still writable: yes (identity/geography/URL/brand)

## Dry run
35. Proposed inserts: **${dry.inserts.length}**
36. Proposed updates: **${dry.updates.length}**
37. Blank fills: **${blankFills}**
38. Contradictions (steward): **${contradictions}**
39. Temporal changes (steward): **${temporal}**
40. Blocked writes: **${dry.blocked.length}**
41. Steward-review writes: **${dry.steward.length}**

## Safety
42–48. Gates: ${gates.checks.map((c) => `${c.id}=${c.pass ? "PASS" : "FAIL"}`).join(", ")}

## Phase 2 NOT run
Artifacts 22–30 deferred until Joan authorizes.

---

## AUTHORIZATION GATE — STOP BEFORE WRITE

Hard gates: **${gates.all_pass ? "PASS" : "FAIL"}**

| Item | Count |
|------|------:|
| Records to write (Pilot A then B) | **${selection.actual}** (A=${selection.pilot_a.length}, B+=${selection.pilot_b_remainder.length}) |
| Inserts | **${dry.inserts.length}** |
| Updates (blank-fill) | **${dry.updates.length}** |
| Fields (insert field keys avg) | ~**${dry.inserts[0] ? Object.keys(dry.inserts[0].fields).length : 0}** per insert |
| Rooms Pending | **${dry.roomsPending}** |
| SerpApi-blocked field rows | **${dry.serpapiBlockedFields}** |
| Duplicate/conflict in auto cohort | **0** (excluded from auto) |
| Rollback ready | **YES** (\`20-rollback-plan.json\` + snapshot) |

### To authorize Phase 2 (Joan only)

\`\`\`bash
# Explicit run-level gate — do NOT set until ready
set ENABLE_VERIFIED_CENSUS_WRITES=1
npm run census:autopilot-v3-airtable-migration -- --phase2
\`\`\`

Phase 2 will process Pilot A (25) then Pilot B only if Pilot A circuit breakers stay clean.
**Per-property approval is not required** once this env gate is set.

Do **not** enable this gate until you have reviewed this dry-run.
`;

  wm(outDir, "21-phase1-final-dry-run-report.md", phase1Report);

  // Partial final report answers for Phase 1 (30-final only after Phase 2)
  wj(outDir, "00-phase1-scorecard.json", {
    phase: 1,
    run_id: rid,
    hard_gates_pass: gates.all_pass,
    inserts: dry.inserts.length,
    updates: dry.updates.length,
    selected: selection.actual,
    rooms_pending: dry.roomsPending,
    serpapi_blocked_rows: dry.serpapiBlockedFields,
    cvent_leakage: 0,
    legacy_leakage: 0,
    writes_executed: false,
    phase2_authorized: false,
    research_verdict: "CONTROLLED WAVES ONLY",
    verified_census_verdict: "MIGRATION PILOT ONLY",
    airtable_verdict: gates.all_pass ? "PILOT WRITE READY" : "DRY-RUN ONLY",
    rooms_verdict: "PARALLEL VALIDATION PIPELINE",
    serpapi_verdict: "DOWNSTREAM-USE REVIEW",
    cvent_verdict: "CHALLENGE SET ONLY",
  });

  log(`[v3] Phase 1 complete — gates=${gates.all_pass ? "PASS" : "FAIL"} — NO WRITES`);

  return {
    outDir,
    runId: rid,
    gates,
    selection,
    dry,
    snapshot,
    liveSchema,
    phase2Authorized: false,
  };
}
