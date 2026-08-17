/**
 * V3.0.1 official coordinate corrective backfill — blank-fill only.
 * Bound to diagnostic dry-run 10-corrective-backfill-dry-run.json (max 60).
 */

import fs from "node:fs";
import path from "node:path";
import {
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
  productionHotelPropertyCensus,
  assertProductionCensusWriteTarget,
  BLOCKED_WRONG_CENSUS_TARGET,
} from "../production-census-source-of-truth.js";
import { TABLE_IDS } from "../production-census-write.js";
import { resolvePat, resolveTargetBase } from "../production-census-schema-create.js";

const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] || PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;
const AUTHORIZED_RUN = "cav3_2026-08-08T15-04-05-566Z";
const PILOT_A = 10;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function isBlank(v) {
  return v == null || v === "" || (Array.isArray(v) && !v.length);
}

async function airtableGet(baseId, token, recordId) {
  const url = `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CENSUS_TABLE_ID)}/${encodeURIComponent(recordId)}`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(`airtable_get_${res.status}:${JSON.stringify(json.error || json)}`);
  return json;
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
      body: JSON.stringify({ records: [{ id: recordId, fields }], typecast: true }),
    }
  );
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(`airtable_update_${res.status}:${JSON.stringify(json.error || json)}`);
  return json.records?.[0];
}

function loadManifest(root) {
  const p = path.join(
    root,
    "data/research-engine-v2/census-autopilot-v3-airtable-migration/31-field-gap-diagnostic/10-corrective-backfill-dry-run.json"
  );
  const dry = JSON.parse(fs.readFileSync(p, "utf8"));
  if (dry.run_id !== AUTHORIZED_RUN) throw new Error(`dry-run run_id mismatch ${dry.run_id}`);
  if (!Array.isArray(dry.proposed_mutations) || dry.proposed_mutations.length !== 60) {
    throw new Error(`expected 60 mutations, got ${dry.proposed_mutations?.length}`);
  }
  return dry;
}

function validateMutation(m) {
  if (m.operation !== "UPDATE_BLANK_FILL") throw new Error("bad_operation");
  if (!m.airtable_record_id || !m.property_identity_key) throw new Error("missing_ids");
  if (m.fields.Latitude == null || m.fields.Longitude == null) throw new Error("missing_coords");
  if (m.provenance?.serpapi_used) throw new Error("serpapi_not_allowed");
  if (!/official/i.test(m.provenance?.source_class || m.provenance?.source_type || "")) {
    throw new Error("non_official_source");
  }
  if (Object.keys(m.fields).some((f) => !["Latitude", "Longitude"].includes(f))) {
    throw new Error("unexpected_fields");
  }
}

/**
 * @param {{ root: string, log?: Function, enableWrites?: boolean }} opts
 */
export async function runOfficialCoordinateBackfill(opts) {
  const root = opts.root;
  const log = opts.log || console.log;
  const enableWrites = opts.enableWrites === true;
  const dry = loadManifest(root);
  const mutations = dry.proposed_mutations;
  const pilotA = mutations.slice(0, PILOT_A);
  const pilotB = mutations.slice(PILOT_A);

  const token = resolvePat();
  const bases = resolveTargetBase();
  const baseId = bases.target_base_id;

  const circuit = { tripped: false, reason: null, at: null, detail: null };
  const trip = (reason, detail = {}) => {
    circuit.tripped = true;
    circuit.reason = reason;
    circuit.at = new Date().toISOString();
    circuit.detail = detail;
    log(`[v3.0.1.coords] CIRCUIT: ${reason}`);
  };

  const preSnapshot = [];
  for (const m of mutations) {
    validateMutation(m);
    const rec = await airtableGet(baseId, token, m.airtable_record_id);
    preSnapshot.push({
      id: rec.id,
      property_identity_key: rec.fields?.["Property Identity Key"] || null,
      Latitude: rec.fields?.Latitude ?? null,
      Longitude: rec.fields?.Longitude ?? null,
    });
    await sleep(80);
  }

  async function processOne(m, pilot) {
    if (circuit.tripped) return { status: "blocked_circuit" };
    validateMutation(m);
    const current = await airtableGet(baseId, token, m.airtable_record_id);
    const key = current.fields?.["Property Identity Key"];
    if (key && key !== m.property_identity_key) {
      trip("identity_error", { expected: m.property_identity_key, actual: key });
      return { status: "blocked_identity" };
    }
    if (!isBlank(current.fields?.Latitude) || !isBlank(current.fields?.Longitude)) {
      // blank-fill only — if already matches authorized values, treat as success/skip
      const latOk =
        !isBlank(current.fields?.Latitude) &&
        Number(current.fields.Latitude) === Number(m.fields.Latitude);
      const lngOk =
        !isBlank(current.fields?.Longitude) &&
        Number(current.fields.Longitude) === Number(m.fields.Longitude);
      if (latOk && lngOk) {
        return {
          status: "skipped_already_populated_matching",
          record_id: m.airtable_record_id,
          fields_written: {},
          already_matched: true,
        };
      }
      if (!isBlank(current.fields?.Latitude) && !isBlank(current.fields?.Longitude)) {
        return { status: "skipped_already_populated", record_id: m.airtable_record_id };
      }
    }
    const toWrite = {};
    if (isBlank(current.fields?.Latitude)) toWrite.Latitude = m.fields.Latitude;
    if (isBlank(current.fields?.Longitude)) toWrite.Longitude = m.fields.Longitude;
    if (!Object.keys(toWrite).length) {
      return { status: "skipped_already_populated", record_id: m.airtable_record_id };
    }
    // Never overwrite nonblank
    for (const f of Object.keys(toWrite)) {
      if (!isBlank(current.fields?.[f])) {
        trip("unintended_overwrite", { field: f, key: m.property_identity_key });
        return { status: "blocked_overwrite" };
      }
    }
    if (m.provenance?.cvent_used_as_production_evidence) {
      trip("cvent_leakage", { key: m.property_identity_key });
      return { status: "blocked_cvent" };
    }
    if (m.provenance?.legacy_used_as_production_evidence) {
      trip("legacy_leakage", { key: m.property_identity_key });
      return { status: "blocked_legacy" };
    }
    if (m.provenance?.serpapi_used) {
      trip("source_rights_violation", { key: m.property_identity_key });
      return { status: "blocked_rights" };
    }
    if (!enableWrites) {
      return {
        status: "dry_run_would_update",
        record_id: m.airtable_record_id,
        fields_written: toWrite,
      };
    }
    await airtableUpdate(baseId, token, m.airtable_record_id, toWrite);
    await sleep(150);
    const fresh = await airtableGet(baseId, token, m.airtable_record_id);
    const mismatches = [];
    for (const [f, v] of Object.entries(toWrite)) {
      if (Number(fresh.fields?.[f]) !== Number(v)) {
        mismatches.push({ field: f, expected: v, actual: fresh.fields?.[f] });
      }
    }
    if (mismatches.length) {
      trip("expected_actual_mismatch", { key: m.property_identity_key, mismatches });
      return { status: "mismatch", record_id: m.airtable_record_id, mismatches, fields_written: toWrite };
    }
    return {
      status: "updated",
      record_id: m.airtable_record_id,
      fields_written: toWrite,
      pilot,
      property_identity_key: m.property_identity_key,
      provenance: m.provenance,
    };
  }

  async function runPilot(list, label) {
    const results = [];
    log(`[v3.0.1.coords] Pilot ${label} n=${list.length} writes=${enableWrites}`);
    for (let i = 0; i < list.length; i++) {
      const r = await processOne(list[i], label);
      results.push({
        property_identity_key: list[i].property_identity_key,
        airtable_record_id: list[i].airtable_record_id,
        family: list[i].family,
        ...r,
      });
      if ((i + 1) % 5 === 0) log(`[v3.0.1.coords] Pilot ${label} ${i + 1}/${list.length}`);
      await sleep(120);
    }
    return results;
  }

  const aResults = await runPilot(pilotA, "A");
  const aUpdated = aResults.filter((r) => r.status === "updated" || r.status === "dry_run_would_update");
  const aFail = aResults.some((r) =>
    ["mismatch", "blocked_identity", "blocked_overwrite", "blocked_cvent", "blocked_legacy", "blocked_rights", "error"].includes(
      r.status
    )
  );
  const aPass = !circuit.tripped && !aFail && aResults.length === PILOT_A;

  let bResults = [];
  let bExecuted = false;
  if (aPass && enableWrites) {
    // post-validate A expected/actual already inline
    const aMatch =
      aResults.filter((r) => r.status === "updated").every((r) => !r.mismatches?.length) ||
      aResults.every((r) => r.status === "skipped_already_populated");
    // Require all A updated or skipped with 100% match on updated
    const updatedA = aResults.filter((r) => r.status === "updated");
    const aOk =
      updatedA.length > 0
        ? updatedA.every((r) => !r.mismatches?.length)
        : aResults.every((r) => r.status === "skipped_already_populated");
    if (aOk && !circuit.tripped) {
      log("[v3.0.1.coords] Pilot A PASSED — Pilot B");
      bExecuted = true;
      bResults = await runPilot(pilotB, "B");
    }
  } else if (aPass && !enableWrites) {
    bExecuted = true;
    bResults = await runPilot(pilotB, "B");
  } else {
    log("[v3.0.1.coords] Pilot A failed — Pilot B skipped");
  }

  const all = [...aResults, ...bResults];
  const postSnapshot = [];
  for (const r of all.filter((x) => x.record_id)) {
    try {
      const rec = await airtableGet(baseId, token, r.record_id);
      postSnapshot.push({
        id: rec.id,
        property_identity_key: rec.fields?.["Property Identity Key"],
        Latitude: rec.fields?.Latitude ?? null,
        Longitude: rec.fields?.Longitude ?? null,
      });
    } catch (err) {
      postSnapshot.push({ id: r.record_id, error: String(err?.message || err) });
    }
    await sleep(80);
  }

  const summary = {
    authorized: 60,
    pilot_a_attempted: aResults.length,
    pilot_a_updated: aResults.filter((r) => r.status === "updated").length,
    pilot_a_skipped: aResults.filter((r) => String(r.status).startsWith("skipped")).length,
    pilot_a_pass: aPass && !circuit.tripped,
    pilot_b_executed: bExecuted,
    pilot_b_updated: bResults.filter((r) => r.status === "updated").length,
    total_updated: all.filter((r) => r.status === "updated").length,
    fields_written: all.reduce((n, r) => n + Object.keys(r.fields_written || {}).length, 0),
    circuit,
    enableWrites,
  };

  return {
    dry,
    mutations,
    preSnapshot,
    aResults,
    bResults,
    bExecuted,
    aPass: summary.pilot_a_pass,
    postSnapshot,
    summary,
    circuit,
  };
}
