/**
 * V3.0.3 — Apply authorized Backfill 2 official blank fills only.
 * Manifest: 34-serpapi-gap-closure-and-backfill/17-backfill2-official-dry-run.json
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
const PILOT_A = 15;
const ALLOWED_FIELDS = new Set([
  "State / Region",
  "Address",
  "Submarket",
  "Latitude",
  "Longitude",
  "Phone",
  "Market",
]);

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

/**
 * @param {{ root: string, log?: Function, enableWrites?: boolean, manifestPath?: string }} opts
 */
export async function runV303Backfill2(opts) {
  const root = opts.root;
  const log = opts.log || console.log;
  const enableWrites = opts.enableWrites === true;
  const manifestPath =
    opts.manifestPath ||
    path.join(
      root,
      "data/research-engine-v2/census-autopilot-v3-airtable-migration/34-serpapi-gap-closure-and-backfill/17-backfill2-official-dry-run.json"
    );
  const dry = JSON.parse(fs.readFileSync(manifestPath, "utf8"));
  const mutations = (dry.mutations || []).filter((m) => {
    if (m.operation !== "UPDATE_BLANK_FILL") return false;
    if (m.serpapi_used) return false;
    if (m.cvent_used_as_production_evidence) return false;
    if (m.legacy_used_as_production_evidence) return false;
    return true;
  });
  if (!mutations.length) throw new Error("empty_bf2_official_manifest");

  const token = resolvePat();
  const baseId = resolveTargetBase().target_base_id;
  const circuit = { tripped: false, reason: null, at: null, detail: null };
  const trip = (reason, detail = {}) => {
    circuit.tripped = true;
    circuit.reason = reason;
    circuit.at = new Date().toISOString();
    circuit.detail = detail;
    log(`[v3.0.3.bf2] CIRCUIT: ${reason}`);
  };

  const preWrite = [];
  for (const m of mutations) {
    const rec = await airtableGet(baseId, token, m.airtable_record_id);
    preWrite.push({
      id: rec.id,
      property_identity_key: rec.fields?.["Property Identity Key"] || null,
      fields: Object.fromEntries([...ALLOWED_FIELDS].map((f) => [f, rec.fields?.[f] ?? null])),
    });
    await sleep(50);
  }

  async function processOne(m, pilot) {
    if (circuit.tripped) return { status: "blocked_circuit" };
    if (m.operation !== "UPDATE_BLANK_FILL") {
      trip("bad_operation", { key: m.property_identity_key });
      return { status: "blocked_operation" };
    }
    if (m.serpapi_used) {
      trip("source_rights_violation", { key: m.property_identity_key });
      return { status: "blocked_rights" };
    }
    if (m.cvent_used_as_production_evidence) {
      trip("cvent_leakage", { key: m.property_identity_key });
      return { status: "blocked_cvent" };
    }
    if (m.legacy_used_as_production_evidence) {
      trip("legacy_leakage", { key: m.property_identity_key });
      return { status: "blocked_legacy" };
    }
    for (const f of Object.keys(m.fields || {})) {
      if (!ALLOWED_FIELDS.has(f)) {
        trip("unexpected_field_mutation", { field: f, key: m.property_identity_key });
        return { status: "blocked_unexpected_field" };
      }
    }

    const current = await airtableGet(baseId, token, m.airtable_record_id);
    const key = current.fields?.["Property Identity Key"];
    if (key && key !== m.property_identity_key) {
      trip("identity_error", { expected: m.property_identity_key, actual: key });
      return { status: "blocked_identity" };
    }

    const toWrite = {};
    const rollback = {};
    for (const [f, v] of Object.entries(m.fields || {})) {
      const cur = current.fields?.[f];
      if (!isBlank(cur)) continue;
      toWrite[f] = v;
      rollback[f] = cur ?? null;
    }
    if (!Object.keys(toWrite).length) {
      return { status: "skipped_already_populated", record_id: m.airtable_record_id };
    }

    if (!enableWrites) {
      return {
        status: "dry_run_would_update",
        record_id: m.airtable_record_id,
        fields_written: toWrite,
        rollback,
      };
    }

    await airtableUpdate(baseId, token, m.airtable_record_id, toWrite);
    await sleep(120);
    const fresh = await airtableGet(baseId, token, m.airtable_record_id);
    const mismatches = [];
    for (const [f, v] of Object.entries(toWrite)) {
      const act = fresh.fields?.[f];
      if (typeof v === "number") {
        if (Number(act) !== Number(v)) mismatches.push({ field: f, expected: v, actual: act });
      } else if (String(act ?? "") !== String(v ?? "")) {
        mismatches.push({ field: f, expected: v, actual: act });
      }
    }
    if (mismatches.length) {
      trip("expected_actual_mismatch", { key: m.property_identity_key, mismatches });
      return { status: "mismatch", record_id: m.airtable_record_id, mismatches, fields_written: toWrite };
    }
    return {
      status: "updated",
      record_id: m.airtable_record_id,
      property_identity_key: m.property_identity_key,
      fields_written: toWrite,
      rollback,
      pilot,
    };
  }

  async function runPilot(list, label) {
    const results = [];
    log(`[v3.0.3.bf2] Pilot ${label} n=${list.length} writes=${enableWrites}`);
    for (let i = 0; i < list.length; i++) {
      const r = await processOne(list[i], label);
      results.push({
        property_identity_key: list[i].property_identity_key,
        airtable_record_id: list[i].airtable_record_id,
        ...r,
      });
      if ((i + 1) % 5 === 0) log(`[v3.0.3.bf2] Pilot ${label} ${i + 1}/${list.length}`);
      await sleep(60);
    }
    return results;
  }

  const pilotA = mutations.slice(0, PILOT_A);
  const rest = mutations.slice(PILOT_A);
  const aResults = await runPilot(pilotA, "A");
  const aFail = aResults.some((r) =>
    ["mismatch", "blocked_identity", "blocked_cvent", "blocked_legacy", "blocked_rights", "blocked_unexpected_field"].includes(
      r.status
    )
  );
  const aPass = !circuit.tripped && !aFail && aResults.length === Math.min(PILOT_A, mutations.length);

  let bResults = [];
  let bExecuted = false;
  if (aPass) {
    log("[v3.0.3.bf2] Pilot A PASSED — applying remainder");
    bExecuted = true;
    bResults = await runPilot(rest, "B");
  } else {
    log("[v3.0.3.bf2] Pilot A FAILED — remainder skipped");
  }

  const all = [...aResults, ...bResults];
  const post = [];
  for (const r of all.filter((x) => x.record_id)) {
    try {
      const rec = await airtableGet(baseId, token, r.record_id);
      post.push({
        id: rec.id,
        property_identity_key: rec.fields?.["Property Identity Key"],
        fields: Object.fromEntries([...ALLOWED_FIELDS].map((f) => [f, rec.fields?.[f] ?? null])),
      });
    } catch (err) {
      post.push({ id: r.record_id, error: String(err?.message || err) });
    }
    await sleep(40);
  }

  const summary = {
    authorized_records: mutations.length,
    pilot_a_attempted: aResults.length,
    pilot_a_pass: aPass,
    remainder_applied: bExecuted,
    updated: all.filter((r) => r.status === "updated").length,
    skipped: all.filter((r) => String(r.status).startsWith("skipped")).length,
    fields_written: all.reduce((n, r) => n + Object.keys(r.fields_written || {}).length, 0),
    circuit,
    enableWrites,
    run_id: AUTHORIZED_RUN,
  };

  return {
    dry,
    mutations,
    preWrite,
    aResults,
    bResults,
    bExecuted,
    aPass,
    post,
    summary,
    circuit,
  };
}
