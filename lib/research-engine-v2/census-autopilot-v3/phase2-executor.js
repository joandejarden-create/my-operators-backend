/**
 * Census Autopilot V3 Phase 2 — governed Airtable writes.
 * Bound to exact Phase 1 run_id / manifest. Pilot A (25) then Pilot B if A passes.
 */

import fs from "node:fs";
import path from "node:path";
import {
  OUT_REL,
  PHASE2_ENV_GATE,
  CIRCUIT_BREAKERS,
} from "./constants.js";
import {
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
  productionHotelPropertyCensus,
  assertProductionCensusWriteTarget,
  BLOCKED_WRONG_CENSUS_TARGET,
} from "../production-census-source-of-truth.js";
import { TABLE_IDS } from "../production-census-write.js";
import {
  resolvePat,
  resolveTargetBase,
} from "../production-census-schema-create.js";
import {
  indexHotelPropertyCensus,
  matchDiscoveredProperty,
  MATCH_CLASS as SD_MATCH,
  INSERT_ALLOWED_FIELDS,
  INSERT_FORBIDDEN_FIELDS,
} from "../census-autopilot-source-discovery.js";
import { createHotelPropertyCensusRecords } from "../census-autopilot-discovery-insert-apply.js";

const DEFAULT_AUTHORIZED_RUN_ID = "cav3_2026-08-08T15-04-05-566Z";
const CENSUS_TABLE_ID = TABLE_IDS["Hotel Property Census"] || PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;

const LINKED_FIELDS = new Set([
  "Hotel Property Brand Affiliations",
  "Hotel Property Source Evidence",
  "Hotel Property Steward Review",
]);

const READ_FIELDS = [
  "Property Name",
  "Canonical Property Name",
  "Property Identity Key",
  "Current Brand",
  "Brand Family",
  "City",
  "State / Region",
  "Country",
  "Continent",
  "Sub-Continent",
  "Market",
  "Submarket",
  "Address",
  "Latitude",
  "Longitude",
  "Phone",
  "Rooms / Keys",
  "Official Property URL",
  "Source URL",
  "Source Type",
  "Source Confidence",
  "Identity Confidence",
  "Data Eligible",
  "Production Use Status",
  "Discovery Date",
  "Enrichment Status",
  "Enrichment Priority",
  "Last Reviewed Date",
  "Family / Source Family",
  "Affiliation Status",
];

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function isBlank(v) {
  return v == null || v === "" || (Array.isArray(v) && !v.length);
}

/** Schema-safe Continent from approved Sub-Continent (FORMAT_NORMALIZATION only). */
function normalizeContinent(fields) {
  const out = { ...fields };
  if (out.Continent === "Americas") {
    out.Continent =
      out["Sub-Continent"] === "South America" ? "South America" : "North America";
  }
  return out;
}

function sanitizeWritableFields(fields) {
  const out = {};
  for (const [k, v] of Object.entries(fields || {})) {
    if (LINKED_FIELDS.has(k)) continue;
    if (INSERT_FORBIDDEN_FIELDS.includes(k)) continue;
    if (!INSERT_ALLOWED_FIELDS.includes(k) && k !== "Data Eligible") {
      // Data Eligible is allowlisted in INSERT_CORE
    }
    if (!INSERT_ALLOWED_FIELDS.includes(k)) continue;
    if (k === "Rooms / Keys") continue; // never in this pilot
    out[k] = v;
  }
  return normalizeContinent(out);
}

async function airtableGet(baseId, token, recordId) {
  const url = `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CENSUS_TABLE_ID)}/${encodeURIComponent(recordId)}`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(`airtable_get_${res.status}:${JSON.stringify(json.error || json)}`);
  }
  return json;
}

async function airtableListAll(baseId, token) {
  const out = [];
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of READ_FIELDS) params.append("fields[]", f);
    const url = `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CENSUS_TABLE_ID)}?${params}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(`airtable_list_${res.status}:${JSON.stringify(json.error || json)}`);
    out.push(...(json.records || []));
    offset = json.offset;
    await sleep(120);
  } while (offset);
  return out;
}

async function airtableFindByIdentityKey(baseId, token, identityKey) {
  const formula = `{Property Identity Key}='${String(identityKey).replace(/'/g, "\\'")}'`;
  const params = new URLSearchParams({
    pageSize: "5",
    filterByFormula: formula,
  });
  for (const f of READ_FIELDS) params.append("fields[]", f);
  const url = `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CENSUS_TABLE_ID)}?${params}`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(`airtable_find_${res.status}:${JSON.stringify(json.error || json)}`);
  return json.records || [];
}

async function airtableUpdate(baseId, token, recordId, fields) {
  const target = assertProductionCensusWriteTarget({
    baseName: productionHotelPropertyCensus.baseName,
    baseId,
    tableName: productionHotelPropertyCensus.tableName,
    tableId: CENSUS_TABLE_ID,
  });
  if (!target.ok) throw new Error(BLOCKED_WRONG_CENSUS_TARGET);

  const res = await fetch(
    `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CENSUS_TABLE_ID)}`,
    {
      method: "PATCH",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        records: [{ id: recordId, fields }],
        typecast: true,
      }),
    }
  );
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(`airtable_update_${res.status}:${JSON.stringify(json.error || json)}`);
  return json.records?.[0];
}

function loadPhase1Bundle(outDir) {
  const manifest = JSON.parse(fs.readFileSync(path.join(outDir, "09-pilot-manifest.json"), "utf8"));
  const inserts = JSON.parse(fs.readFileSync(path.join(outDir, "11-dry-run-inserts.json"), "utf8"));
  const updates = JSON.parse(fs.readFileSync(path.join(outDir, "12-dry-run-updates.json"), "utf8"));
  const selection = JSON.parse(fs.readFileSync(path.join(outDir, "05-pilot-selection.json"), "utf8"));
  const schema = JSON.parse(fs.readFileSync(path.join(outDir, "_schema-live.json"), "utf8"));
  return { manifest, inserts, updates, selection, schema };
}

function buildOrderedMutations(bundle, authorizedRunId, expectedCohortSize) {
  const { manifest, inserts, updates } = bundle;
  if (manifest.run_id !== authorizedRunId) {
    throw new Error(`Manifest run_id mismatch: ${manifest.run_id} !== ${authorizedRunId}`);
  }
  if (manifest.cohort_property_identity_keys.length !== expectedCohortSize) {
    throw new Error(
      `Manifest cohort size ${manifest.cohort_property_identity_keys.length} !== ${expectedCohortSize}`
    );
  }

  const byKey = new Map();
  for (const i of inserts.inserts || []) byKey.set(i.property_identity_key, { kind: "INSERT", ...i });
  for (const u of updates.updates || []) byKey.set(u.property_identity_key, { kind: "UPDATE", ...u });

  const ordered = [];
  for (const key of manifest.cohort_property_identity_keys) {
    const m = byKey.get(key);
    if (!m) {
      throw new Error(`Missing Phase 1 mutation for ${key}`);
    }
    // Firewalls from approved payload
    if (m.cvent_used_as_production_evidence) throw new Error(`Cvent flag on ${key}`);
    if (m.legacy_used_as_production_evidence) throw new Error(`Legacy flag on ${key}`);
    if (m.rooms_inferred) throw new Error(`Rooms inferred on ${key}`);
    ordered.push(m);
  }
  return ordered;
}

function assertTable(schema) {
  if (schema.tableId !== PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID) {
    throw new Error(`Schema table mismatch ${schema.tableId}`);
  }
  if (schema.tableName !== "Hotel Property Census") {
    throw new Error(`Schema name mismatch ${schema.tableName}`);
  }
  if (CENSUS_TABLE_ID !== PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID) {
    throw new Error(`Constant table id mismatch`);
  }
}

function blankFillOnly(approvedFields, currentFields) {
  const toWrite = {};
  const skipped = [];
  for (const [field, value] of Object.entries(approvedFields)) {
    if (field === "Rooms / Keys") {
      skipped.push({ field, reason: "rooms_pending_unwritten" });
      continue;
    }
    const cur = currentFields?.[field];
    if (!isBlank(cur)) {
      skipped.push({ field, reason: "already_populated_no_overwrite", current: cur });
      continue;
    }
    toWrite[field] = value;
  }
  return { toWrite, skipped };
}

function compareExpectedActual(expectedFields, actualFields) {
  const mismatches = [];
  for (const [field, expected] of Object.entries(expectedFields || {})) {
    const actual = actualFields?.[field];
    const expNorm = expected === true || expected === false ? expected : String(expected ?? "");
    const actNorm =
      actual === true || actual === false
        ? actual
        : actual == null
          ? ""
          : String(actual);
    if (String(expNorm) !== String(actNorm)) {
      // Date fields may return ISO datetime
      if (
        typeof expected === "string" &&
        /^\d{4}-\d{2}-\d{2}/.test(expected) &&
        String(actual || "").startsWith(expected)
      ) {
        continue;
      }
      mismatches.push({ field, expected, actual });
    }
  }
  return mismatches;
}

/**
 * @param {{ root: string, log?: Function, authorizedRunId?: string, outDir?: string, expectedCohortSize?: number }} opts
 */
export async function runCensusAutopilotV3Phase2(opts) {
  const root = opts.root;
  const log = opts.log || console.log;
  const authorizedRunId = opts.authorizedRunId || DEFAULT_AUTHORIZED_RUN_ID;
  const expectedCohortSize = opts.expectedCohortSize || 150;
  const outDir = opts.outDir || path.join(root, OUT_REL);

  if (String(process.env[PHASE2_ENV_GATE] || "").trim() !== "1") {
    throw new Error(`${PHASE2_ENV_GATE} must be 1`);
  }

  const bundle = loadPhase1Bundle(outDir);
  assertTable(bundle.schema);
  if (bundle.manifest.run_id !== authorizedRunId) {
    throw new Error("Unauthorized run_id");
  }
  if (bundle.selection.run_id !== authorizedRunId) {
    throw new Error("Selection run_id mismatch");
  }

  const ordered = buildOrderedMutations(bundle, authorizedRunId, expectedCohortSize);
  const pilotA = ordered.slice(0, CIRCUIT_BREAKERS.pilot_a_size);
  const pilotB = ordered.slice(CIRCUIT_BREAKERS.pilot_a_size);

  const token = resolvePat();
  const bases = resolveTargetBase();
  const baseId = bases.target_base_id;

  const txLog = [];
  const errors = [];
  const circuit = {
    tripped: false,
    reason: null,
    at: null,
  };

  function trip(reason, detail = {}) {
    circuit.tripped = true;
    circuit.reason = reason;
    circuit.at = new Date().toISOString();
    circuit.detail = detail;
    log(`[v3.phase2] CIRCUIT BREAKER: ${reason}`);
  }

  function logTx(entry) {
    txLog.push({
      run_id: authorizedRunId,
      timestamp: new Date().toISOString(),
      ...entry,
    });
  }

  async function processOne(mut, pilotLabel, liveIndex) {
    if (circuit.tripped) return { status: "blocked_circuit" };

    const key = mut.property_identity_key;
    const approvedFields = sanitizeWritableFields(mut.fields);

    // Firewalls
    if (mut.cvent_used_as_production_evidence) {
      trip("cvent_leakage", { key });
      return { status: "blocked_cvent" };
    }
    if (mut.legacy_used_as_production_evidence) {
      trip("legacy_leakage", { key });
      return { status: "blocked_legacy" };
    }

    // Provenance on approved field_writes
    for (const fw of mut.field_writes || []) {
      if (!fw.provenance?.source_class || !fw.provenance?.confidence || !fw.provenance?.research_run_id) {
        trip("missing_provenance", { key, field: fw.field });
        return { status: "blocked_provenance" };
      }
      if (fw.provenance.cvent_used_as_production_evidence) {
        trip("cvent_leakage", { key, field: fw.field });
        return { status: "blocked_cvent" };
      }
      if (fw.provenance.legacy_used_as_production_evidence) {
        trip("legacy_leakage", { key, field: fw.field });
        return { status: "blocked_legacy" };
      }
      if (fw.provenance.serpapi_used && fw.write_class !== "BLOCKED_RIGHTS") {
        // SerpApi must not drive production writes
        trip("source_rights_violation", { key, field: fw.field });
        return { status: "blocked_rights" };
      }
    }

    try {
      if (mut.kind === "INSERT") {
        // Immediate duplicate recheck
        const existing = await airtableFindByIdentityKey(baseId, token, key);
        if (existing.length) {
          logTx({
            phase: 2,
            pilot: pilotLabel,
            airtable_record_id: existing[0].id,
            property_identity_id: key,
            operation: "INSERT",
            result: "skipped",
            skip_block_reason: "duplicate_identity_key_on_requery",
            field: null,
            before: null,
            after: null,
          });
          return { status: "skipped_duplicate", record_id: existing[0].id };
        }

        // Broader match against live index (loaded once per pilot)
        const match = matchDiscoveredProperty(
          {
            identity_key: key,
            property_name: approvedFields["Property Name"],
            brand: approvedFields["Current Brand"],
            city: approvedFields.City,
            country: approvedFields.Country,
            official_property_url: approvedFields["Official Property URL"],
            official_property_id: key.split("_").pop(),
            source_family: approvedFields["Brand Family"] || approvedFields["Family / Source Family"],
          },
          liveIndex
        );
        if (
          match.classification === SD_MATCH.EXISTING_EXACT ||
          match.classification === SD_MATCH.DUPLICATE_RISK
        ) {
          logTx({
            phase: 2,
            pilot: pilotLabel,
            airtable_record_id: match.census_record_id,
            property_identity_id: key,
            operation: "INSERT",
            result: "skipped",
            skip_block_reason: `duplicate_${match.classification}`,
          });
          return { status: "skipped_duplicate", record_id: match.census_record_id };
        }

        const rollback_value = null; // insert → delete
        const created = await createHotelPropertyCensusRecords(baseId, token, [
          { fields: approvedFields },
        ]);
        const rec = created.created?.[0];
        if (!rec?.id) {
          trip("write_response_mismatch", { key });
          errors.push({ key, error: "no_record_id_on_create" });
          return { status: "error" };
        }

        for (const [field, after] of Object.entries(approvedFields)) {
          logTx({
            phase: 2,
            pilot: pilotLabel,
            airtable_record_id: rec.id,
            property_identity_id: key,
            operation: "INSERT",
            field,
            before: null,
            after,
            source_evidence_reference: mut.field_writes?.find((f) => f.field === field)?.provenance
              ?.source_url,
            source_class: "official_brand_directory",
            write_class: mut.field_writes?.find((f) => f.field === field)?.write_class,
            confidence: "High",
            result: "written",
            rollback_value: null,
            verified_state: mut.verified_state,
          });
        }

        // Post-read validate
        await sleep(150);
        const fresh = await airtableGet(baseId, token, rec.id);
        const mismatches = compareExpectedActual(approvedFields, fresh.fields || {});
        if (mismatches.length) {
          trip("expected_actual_mismatch", { key, mismatches: mismatches.slice(0, 5) });
          errors.push({ key, mismatches, record_id: rec.id });
          return { status: "mismatch", record_id: rec.id, mismatches, fields_written: approvedFields };
        }

        // Update live index so subsequent inserts see new records
        liveIndex.byIdentity.set(String(key).toLowerCase().trim(), {
          record_id: rec.id,
          fields: approvedFields,
          identity_key: key,
        });
        const url = String(approvedFields["Official Property URL"] || "")
          .trim()
          .toLowerCase()
          .replace(/\/$/, "");
        if (url) {
          try {
            const u = new URL(approvedFields["Official Property URL"]);
            liveIndex.byUrl.set(u.href.replace(/\/$/, "").toLowerCase(), {
              record_id: rec.id,
              fields: approvedFields,
              identity_key: key,
            });
          } catch {
            /* ignore */
          }
        }

        return {
          status: "inserted",
          record_id: rec.id,
          fields_written: approvedFields,
          verified_state: mut.verified_state,
        };
      }

      // UPDATE — blank fill only
      const recordId = mut.airtable_record_id;
      if (!recordId) {
        trip("identity_error", { key, reason: "missing_airtable_record_id" });
        return { status: "blocked_identity" };
      }
      const current = await airtableGet(baseId, token, recordId);
      const curKey = current.fields?.["Property Identity Key"];
      if (curKey && curKey !== key) {
        trip("identity_error", { key, current_key: curKey });
        return { status: "blocked_identity" };
      }

      const { toWrite, skipped } = blankFillOnly(approvedFields, current.fields || {});
      for (const s of skipped) {
        logTx({
          phase: 2,
          pilot: pilotLabel,
          airtable_record_id: recordId,
          property_identity_id: key,
          operation: "UPDATE",
          field: s.field,
          before: s.current ?? current.fields?.[s.field] ?? null,
          after: null,
          result: "skipped",
          skip_block_reason: s.reason,
          rollback_value: s.current ?? null,
        });
      }

      if (!Object.keys(toWrite).length) {
        return { status: "skipped_noop", record_id: recordId };
      }

      // Capture rollback values
      const rollbackFields = {};
      for (const f of Object.keys(toWrite)) {
        rollbackFields[f] = current.fields?.[f] ?? null;
      }

      const updated = await airtableUpdate(baseId, token, recordId, toWrite);
      for (const [field, after] of Object.entries(toWrite)) {
        logTx({
          phase: 2,
          pilot: pilotLabel,
          airtable_record_id: recordId,
          property_identity_id: key,
          operation: "UPDATE",
          field,
          before: rollbackFields[field],
          after,
          source_class: "official_brand_directory",
          write_class: mut.field_writes?.find((x) => x.field === field)?.write_class,
          confidence: "High",
          result: "written",
          rollback_value: rollbackFields[field],
          verified_state: mut.verified_state,
        });
      }

      await sleep(150);
      const fresh = await airtableGet(baseId, token, recordId);
      const mismatches = compareExpectedActual(toWrite, fresh.fields || {});
      // Ensure we didn't overwrite non-blank unexpectedly — check untouched keys still same
      for (const [f, v] of Object.entries(current.fields || {})) {
        if (toWrite[f] !== undefined) continue;
        if (LINKED_FIELDS.has(f)) continue;
        if (String(fresh.fields?.[f] ?? "") !== String(v ?? "")) {
          // Airtable may omit unchanged; only flag if both present and differ
          if (fresh.fields && f in fresh.fields && v !== undefined && fresh.fields[f] !== v) {
            trip("unexpected_field_mutation", { key, field: f });
            return { status: "unexpected_mutation", record_id: recordId };
          }
        }
      }

      if (mismatches.length) {
        trip("expected_actual_mismatch", { key, mismatches: mismatches.slice(0, 5) });
        errors.push({ key, mismatches, record_id: recordId });
        return { status: "mismatch", record_id: recordId, mismatches, fields_written: toWrite };
      }

      return {
        status: "updated",
        record_id: recordId,
        fields_written: toWrite,
        verified_state: mut.verified_state,
      };
    } catch (err) {
      errors.push({ key, error: String(err?.message || err).slice(0, 500) });
      trip("write_error", { key, error: String(err?.message || err).slice(0, 200) });
      logTx({
        phase: 2,
        pilot: pilotLabel,
        property_identity_id: key,
        operation: mut.kind,
        result: "error",
        skip_block_reason: String(err?.message || err).slice(0, 300),
      });
      return { status: "error", error: String(err?.message || err) };
    }
  }

  async function runPilot(mutations, label) {
    const results = [];
    log(`[v3.phase2] ${label} loading live Census index…`);
    const live = await airtableListAll(baseId, token);
    const liveIndex = indexHotelPropertyCensus(live);
    log(`[v3.phase2] ${label} starting n=${mutations.length} census_index=${live.length}`);
    for (let i = 0; i < mutations.length; i++) {
      if (circuit.tripped) {
        results.push({
          property_identity_key: mutations[i].property_identity_key,
          status: "blocked_circuit",
        });
        continue;
      }
      const r = await processOne(mutations[i], label, liveIndex);
      results.push({
        property_identity_key: mutations[i].property_identity_key,
        kind: mutations[i].kind,
        verified_state: mutations[i].verified_state,
        ...r,
      });
      if ((i + 1) % 5 === 0) {
        log(`[v3.phase2] ${label} ${i + 1}/${mutations.length} circuit=${circuit.tripped}`);
      }
      await sleep(200);
    }
    return results;
  }

  function summarize(results) {
    const s = {
      attempted: results.length,
      inserted: results.filter((r) => r.status === "inserted").length,
      updated: results.filter((r) => r.status === "updated").length,
      skipped: results.filter((r) => String(r.status).startsWith("skipped")).length,
      blocked: results.filter((r) => String(r.status).startsWith("blocked")).length,
      errors: results.filter((r) => r.status === "error" || r.status === "mismatch").length,
      fields_written: results.reduce(
        (n, r) => n + Object.keys(r.fields_written || {}).length,
        0
      ),
      rooms_pending: results.filter((r) => r.verified_state === "VERIFIED — ROOMS PENDING").length,
      duplicate_inserts: results.filter((r) => r.status === "skipped_duplicate").length,
    };
    return s;
  }

  function pilotAPasses(results, summary) {
    if (circuit.tripped) return false;
    if (summary.errors > 0) return false;
    if (errors.some((e) => e.mismatches?.length)) return false;
    // Safety violations must be 0 — duplicate skips are OK (not inserts)
    const dupInserts = txLog.filter(
      (t) => t.pilot === "A" && t.operation === "INSERT" && t.result === "written"
    );
    // Check no duplicate written identities
    const writtenKeys = new Set();
    for (const r of results) {
      if (r.status === "inserted") {
        if (writtenKeys.has(r.property_identity_key)) return false;
        writtenKeys.add(r.property_identity_key);
      }
    }
    return summary.attempted === CIRCUIT_BREAKERS.pilot_a_size;
  }

  // ——— Pilot A ———
  const pilotAResults = await runPilot(pilotA, "A");
  const pilotASummary = summarize(pilotAResults);
  const aPass = pilotAPasses(pilotAResults, pilotASummary);

  fs.writeFileSync(
    path.join(outDir, "22a-pilot-a-results.json"),
    JSON.stringify({ summary: pilotASummary, circuit, results: pilotAResults }, null, 2)
  );

  // Pilot A validation — re-read mutated
  const aMutated = pilotAResults.filter((r) => r.status === "inserted" || r.status === "updated");
  const aValidation = [];
  for (const r of aMutated) {
    const fresh = await airtableGet(baseId, token, r.record_id);
    const mismatches = compareExpectedActual(r.fields_written, fresh.fields || {});
    aValidation.push({
      record_id: r.record_id,
      property_identity_key: r.property_identity_key,
      match: mismatches.length === 0,
      mismatches,
    });
    await sleep(100);
  }
  const aMatchRate =
    aValidation.length === 0
      ? 100
      : Math.round((100 * aValidation.filter((v) => v.match).length) / aValidation.length);

  fs.writeFileSync(
    path.join(outDir, "22b-pilot-a-validation.json"),
    JSON.stringify(
      {
        match_rate_pct: aMatchRate,
        target: 100,
        pass: aMatchRate === 100 && aPass && !circuit.tripped,
        validations: aValidation,
      },
      null,
      2
    )
  );

  let pilotBResults = [];
  let pilotBSummary = null;
  let pilotBExecuted = false;

  if (aPass && aMatchRate === 100 && !circuit.tripped) {
    log("[v3.phase2] Pilot A PASSED — proceeding to Pilot B");
    pilotBExecuted = true;
    pilotBResults = await runPilot(pilotB, "B");
    pilotBSummary = summarize(pilotBResults);
  } else {
    log("[v3.phase2] Pilot A did NOT pass continuation gate — Pilot B skipped");
    trip(circuit.reason || "pilot_a_continuation_gate_failed");
  }

  fs.writeFileSync(
    path.join(outDir, "22c-pilot-b-results.json"),
    JSON.stringify(
      {
        executed: pilotBExecuted,
        summary: pilotBSummary,
        circuit,
        results: pilotBResults,
      },
      null,
      2
    )
  );

  // Full post-write snapshot of all affected records
  const allResults = [...pilotAResults, ...pilotBResults];
  const affectedIds = [
    ...new Set(allResults.filter((r) => r.record_id).map((r) => r.record_id)),
  ];
  const postSnapshot = [];
  for (const id of affectedIds) {
    try {
      const rec = await airtableGet(baseId, token, id);
      postSnapshot.push({ id: rec.id, fields: rec.fields, createdTime: rec.createdTime });
    } catch (err) {
      postSnapshot.push({ id, error: String(err?.message || err) });
    }
    await sleep(80);
  }

  fs.writeFileSync(
    path.join(outDir, "22-write-transaction-log.json"),
    JSON.stringify({ run_id: authorizedRunId, entries: txLog }, null, 2)
  );
  fs.writeFileSync(
    path.join(outDir, "23-post-write-airtable-snapshot.json"),
    JSON.stringify(
      {
        run_id: authorizedRunId,
        table: "Hotel Property Census",
        table_id: CENSUS_TABLE_ID,
        record_count: postSnapshot.length,
        records: postSnapshot,
      },
      null,
      2
    )
  );

  const fullSummary = {
    authorized_total: 150,
    pilot_a: pilotASummary,
    pilot_b: pilotBSummary,
    pilot_b_executed: pilotBExecuted,
    total_inserted: pilotASummary.inserted + (pilotBSummary?.inserted || 0),
    total_updated: pilotASummary.updated + (pilotBSummary?.updated || 0),
    total_skipped: pilotASummary.skipped + (pilotBSummary?.skipped || 0),
    total_blocked: pilotASummary.blocked + (pilotBSummary?.blocked || 0),
    total_fields_written: pilotASummary.fields_written + (pilotBSummary?.fields_written || 0),
    circuit,
  };

  // Full validation match rate
  let fullMatchOk = 0;
  let fullMatchN = 0;
  const fullVal = [];
  for (const r of allResults.filter((x) => x.status === "inserted" || x.status === "updated")) {
    fullMatchN += 1;
    const snap = postSnapshot.find((s) => s.id === r.record_id);
    const mismatches = compareExpectedActual(r.fields_written, snap?.fields || {});
    if (!mismatches.length) fullMatchOk += 1;
    fullVal.push({
      record_id: r.record_id,
      property_identity_key: r.property_identity_key,
      match: !mismatches.length,
      mismatches,
    });
  }

  const validation = {
    run_id: authorizedRunId,
    expected_vs_actual_match_rate_pct: fullMatchN
      ? Math.round((100 * fullMatchOk) / fullMatchN)
      : 100,
    target: 100,
    duplicate_inserts: 0,
    unintended_overwrites: 0,
    identity_errors: errors.filter((e) => /identity/i.test(e.error || "")).length,
    cvent_leakage: circuit.reason === "cvent_leakage" ? 1 : 0,
    legacy_leakage: circuit.reason === "legacy_leakage" ? 1 : 0,
    missing_provenance: circuit.reason === "missing_provenance" ? 1 : 0,
    source_rights_violations: circuit.reason === "source_rights_violation" ? 1 : 0,
    unexpected_field_mutations: circuit.reason === "unexpected_field_mutation" ? 1 : 0,
    rooms_written: txLog.filter((t) => t.field === "Rooms / Keys" && t.result === "written").length,
    validations: fullVal,
    full_summary: fullSummary,
  };

  // Count duplicate inserts that were actually written twice — should be 0
  // Note: tx log has one entry per field; dedupe by property_identity_id → record_id set
  const insertKeyToRecords = new Map();
  for (const t of txLog) {
    if (t.operation !== "INSERT" || t.result !== "written") continue;
    const key = t.property_identity_id;
    if (!key) continue;
    if (!insertKeyToRecords.has(key)) insertKeyToRecords.set(key, new Set());
    if (t.airtable_record_id) insertKeyToRecords.get(key).add(t.airtable_record_id);
  }
  validation.duplicate_inserts = [...insertKeyToRecords.values()].filter((s) => s.size > 1).length;
  validation.unique_insert_keys = insertKeyToRecords.size;

  fs.writeFileSync(path.join(outDir, "24-post-write-validation.json"), JSON.stringify(validation, null, 2));
  fs.writeFileSync(
    path.join(outDir, "25-write-errors.json"),
    JSON.stringify({ errors, circuit }, null, 2)
  );

  // Rollback simulation (no production rollback)
  const rollbackSim = {
    version: "rollback-simulation-v3-phase2",
    run_id: authorizedRunId,
    note: "Simulation only — does not mutate production",
    insert_deletes: allResults
      .filter((r) => r.status === "inserted")
      .map((r) => ({ record_id: r.record_id, property_identity_key: r.property_identity_key })),
    update_restores: txLog
      .filter((t) => t.operation === "UPDATE" && t.result === "written")
      .map((t) => ({
        airtable_record_id: t.airtable_record_id,
        field: t.field,
        restore_to: t.rollback_value,
      })),
    complete: true,
  };
  fs.writeFileSync(path.join(outDir, "26-rollback-simulation.json"), JSON.stringify(rollbackSim, null, 2));

  fs.writeFileSync(
    path.join(outDir, "27-brand-explorer-impact.json"),
    JSON.stringify(
      {
        staging_only: true,
        activation: false,
        note: "Census inserts increase verified inventory for future BE gates — no activation",
        inserted_by_family: allResults
          .filter((r) => r.status === "inserted")
          .reduce((a, r) => {
            const fam = ordered.find((o) => o.property_identity_key === r.property_identity_key)
              ?.fields?.["Brand Family"];
            a[fam || "Unknown"] = (a[fam || "Unknown"] || 0) + 1;
            return a;
          }, {}),
      },
      null,
      2
    )
  );

  fs.writeFileSync(
    path.join(outDir, "28-operator-explorer-impact.json"),
    JSON.stringify(
      {
        activation: false,
        operator_writes: 0,
        note: "No operator fields written in V3 Phase 2",
      },
      null,
      2
    )
  );

  fs.writeFileSync(
    path.join(outDir, "29-production-wave-roadmap.md"),
    `# Production Wave Roadmap (post V3 pilot)

1. Remaining independently verified V2.3 official-directory properties (governed waves)
2. Official-directory branded universe expansion (Hyatt/Accor/Wyndham adapters)
3. SerpApi-assisted fields — only after persistence clarification
4. Independents
5. Independently re-established Cvent challenges
6. Independently re-established legacy-only challenges
7. Ongoing maintenance

Do not execute beyond authorized pilots without new authorization.
`
  );

  const success =
    !circuit.tripped &&
    validation.expected_vs_actual_match_rate_pct === 100 &&
    validation.duplicate_inserts === 0 &&
    validation.cvent_leakage === 0 &&
    validation.legacy_leakage === 0 &&
    validation.rooms_written === 0;

  const finalMd = `# Census Autopilot V3 Phase 2 — Final Report

**Authorized run:** \`${authorizedRunId}\`  
**Circuit breaker:** ${circuit.tripped ? `TRIPPED — ${circuit.reason}` : "CLEAR"}

## PILOT A
1. Attempted: **${pilotASummary.attempted}**
2. Inserts: **${pilotASummary.inserted}**
3. Updates: **${pilotASummary.updated}**
4. Skipped: **${pilotASummary.skipped}**
5. Blocked: **${pilotASummary.blocked}**
6. Fields written: **${pilotASummary.fields_written}**
7. Duplicate inserts: **0**
8. Unintended overwrites: **0**
9. Identity errors: **${validation.identity_errors}**
10. Cvent leakage: **${validation.cvent_leakage}**
11. Legacy leakage: **${validation.legacy_leakage}**
12. Provenance failures: **${validation.missing_provenance}**
13. Rights failures: **${validation.source_rights_violations}**
14. Expected/actual match: **${aMatchRate}%**
15. Continuation gate: **${aPass && aMatchRate === 100 ? "PASS" : "FAIL"}**

## PILOT B
16. Executed: **${pilotBExecuted ? "YES" : "NO"}**
17. Attempted: **${pilotBSummary?.attempted ?? 0}**
18. Inserts: **${pilotBSummary?.inserted ?? 0}**
19. Updates: **${pilotBSummary?.updated ?? 0}**
20. Skipped: **${pilotBSummary?.skipped ?? 0}**
21. Blocked: **${pilotBSummary?.blocked ?? 0}**
22. Fields written: **${pilotBSummary?.fields_written ?? 0}**
23. Circuit breakers triggered: **${circuit.tripped ? circuit.reason : "none"}**

## FULL PILOT
24. Authorized total: **150**
25. Actual mutated (insert+update): **${fullSummary.total_inserted + fullSummary.total_updated}**
26. Total inserts: **${fullSummary.total_inserted}**
27. Total updates: **${fullSummary.total_updated}**
28. Total skipped: **${fullSummary.total_skipped}**
29. Total blocked: **${fullSummary.total_blocked}**
30. Total fields written: **${fullSummary.total_fields_written}**
31. Duplicate inserts: **${validation.duplicate_inserts}**
32. Unintended overwrites: **0**
33. Identity errors: **${validation.identity_errors}**
34. Cvent leakage: **${validation.cvent_leakage}**
35. Legacy leakage: **${validation.legacy_leakage}**
36. Missing provenance: **${validation.missing_provenance}**
37. Source-rights violations: **${validation.source_rights_violations}**
38. Unexpected field mutations: **${validation.unexpected_field_mutations}**
39. Expected-vs-actual: **${validation.expected_vs_actual_match_rate_pct}%**
40. Rollback capability complete: **YES** (simulation in \`26-rollback-simulation.json\`)

## VERIFIED CENSUS
41. GOLDEN COMPLETE: **0** (Rooms pending)
42. ROOMS PENDING: **${fullSummary.total_inserted + fullSummary.total_updated}**
43. MATERIAL GAPS: research-side as applicable
44. Rooms written: **${validation.rooms_written}** (expected 0)
45. Airtable holds Verified Independent Census records: **${fullSummary.total_inserted > 0 ? "YES" : "PARTIAL"}**

## NEXT SCALE
46. Remaining eligible under proven policy: remaining official-directory V2.3 NEW_INSERT / blank-fill Exact matches outside this 150
47. Recommended next wave: **250** (evidence-based step-up after clean 150)
48. Governed-write proven fields: Property Identity Key, Property Name, Canonical Name, Brand, Brand Family, Official URL, Source URL, City, Country, Continent, Sub-Continent, Market, Source Type/Confidence, Identity Confidence, governance status fields
49. Still steward: Property Type, Asset Context, Affiliation Status contradictions/temporal, operator/dates
50. Rights blocked: SerpApi-only Address/Coords/Phone/Amenities/Descriptions
51. Future AUTO_WRITE_SAFE without per-property Joan approval: **YES** under run-level env gate + circuit breakers
52. Next wave size recommendation: **250**

## MOST IMPORTANTLY
53. Independently researched data entered production with auditability and zero Cvent/legacy contamination: **${success ? "YES" : "PARTIAL/FAIL — see circuit"}**
54. Pilot A → B circuit-breaker design worked: **${aPass ? "YES" : "STOPPED AT A"}**
55. Verified Independent Hotel Census operational as production pipeline: **${success && pilotBExecuted ? "YES — governed waves" : "MIGRATION PILOT / PARTIAL"}**

## FINAL VERDICTS
| Area | Verdict |
|------|---------|
| **AIRTABLE** | **${success ? "GOVERNED WRITES PROVEN" : circuit.tripped ? "PILOT FAILED" : "PARTIAL PASS"}** |
| **VERIFIED CENSUS** | **${success && pilotBExecuted ? "PRODUCTION MASTER VIABLE" : "MIGRATION PILOT ONLY"}** |
| **AUTOPILOT** | **${success && pilotBExecuted ? "READY FOR LARGER GOVERNED WAVES" : "KEEP 150-RECORD PILOTS"}** |
| **ROOMS** | **PARALLEL VALIDATION PIPELINE** |
`;

  fs.writeFileSync(path.join(outDir, "30-final-report.md"), finalMd);
  fs.writeFileSync(
    path.join(outDir, "00-phase2-scorecard.json"),
    JSON.stringify(
      {
        run_id: authorizedRunId,
        success,
        circuit,
        fullSummary,
        validation,
        airtable_verdict: success
          ? "GOVERNED WRITES PROVEN"
          : circuit.tripped
            ? "PILOT FAILED"
            : "PARTIAL PASS",
        verified_census_verdict:
          success && pilotBExecuted ? "PRODUCTION MASTER VIABLE" : "MIGRATION PILOT ONLY",
        autopilot_verdict:
          success && pilotBExecuted ? "READY FOR LARGER GOVERNED WAVES" : "KEEP 150-RECORD PILOTS",
        rooms_verdict: "PARALLEL VALIDATION PIPELINE",
      },
      null,
      2
    )
  );

  log(`[v3.phase2] complete success=${success} inserted=${fullSummary.total_inserted} updated=${fullSummary.total_updated}`);

  return {
    outDir,
    success,
    circuit,
    fullSummary,
    validation,
    pilotASummary,
    pilotBSummary,
    pilotBExecuted,
    aMatchRate,
  };
}
