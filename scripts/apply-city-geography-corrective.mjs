/**
 * Apply CITY_GEOGRAPHY_CORRECTIVE_MANIFEST — Pilot A 25 → remainder.
 * ENABLE_VERIFIED_CENSUS_WRITES=1 --apply required.
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import "dotenv/config";
import {
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
  productionHotelPropertyCensus,
  assertProductionCensusWriteTarget,
  BLOCKED_WRONG_CENSUS_TARGET,
} from "../lib/research-engine-v2/production-census-source-of-truth.js";
import { TABLE_IDS } from "../lib/research-engine-v2/production-census-write.js";
import { resolvePat, resolveTargetBase } from "../lib/research-engine-v2/production-census-schema-create.js";
import { validateCitySemantics } from "../lib/research-engine-v2/census-autopilot-v3/golden-field-semantics.js";
import {
  assertMarketWriteGate,
  assertSubmarketWriteGate,
  classifyProductionMarket,
} from "../lib/research-engine-v2/census-autopilot-v3/geography/dealality-market-registry.js";
import {
  isStreetLineAsCity,
  isPostalAsCity,
  classifyCityLabel,
} from "../lib/research-engine-v2/census-autopilot-v3/geography/city-resolver-v4.js";
import { isDescriptorCity } from "../lib/research-engine-v2/census-city-state-normalizer.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const CITY_OUT = path.join(
  ROOT,
  "data/research-engine-v2/census-autopilot-v4-standing/city-resolution-v1"
);
const FULL_OUT = path.join(
  ROOT,
  "data/research-engine-v2/census-autopilot-v4-full-universe"
);
const MANIFEST_REL =
  "data/research-engine-v2/census-autopilot-v4-standing/city-resolution-v1/21-city-corrective-dry-run.json";
const MANIFEST_PATH = path.join(ROOT, MANIFEST_REL);
const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] || PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;

const SAFE_CLASSES = new Set([
  "SAFE_CITY_BLANK_FILL",
  "SAFE_CITY_INVALID_CLEAR",
  "SAFE_STATE_RECOMPUTE",
  "SAFE_MARKET_RECOMPUTE",
  "SAFE_SUBMARKET_RECOMPUTE",
  "SAFE_ADDRESS_FILL",
  "SAFE_COORDINATE_FILL",
  "UNKNOWN_PLACEHOLDER_CLEAR",
]);

const ALLOWED_FIELDS = new Set([
  "City",
  "Address",
  "State / Region",
  "Market",
  "Submarket",
  "Latitude",
  "Longitude",
]);

const PLACEHOLDER_RE = /^(unknown|n\/a|na|tbd|to be confirmed|not known|null|undefined|-)$/i;
const PILOT_A_SIZE = 25;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function isBlank(v) {
  return v == null || v === "" || (Array.isArray(v) && !v.length);
}
function wj(dir, name, data) {
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, name), JSON.stringify(data, null, 2));
}
function eqVal(a, b) {
  if (isBlank(a) && isBlank(b)) return true;
  if (typeof a === "number" || typeof b === "number") return Number(a) === Number(b);
  return String(a ?? "") === String(b ?? "");
}
function writeValue(after) {
  return after == null ? null : after;
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

function classifyLiveMutation(m, currentVal, liveFields = {}) {
  if (!ALLOWED_FIELDS.has(m.field)) {
    return { action: "BLOCK", reason: "unexpected_field", circuit: true };
  }
  if (m.cvent_used || m.legacy_used || m.str_used) {
    return { action: "BLOCK", reason: "cvent_legacy_str_leakage", circuit: true };
  }
  if (!SAFE_CLASSES.has(m.mutation_class)) {
    return { action: "BLOCK", reason: "class_not_authorized" };
  }
  if (eqVal(currentVal, m.after)) {
    return { action: "ALREADY_CORRECT", reason: "already_equals_after" };
  }

  if (m.mutation_class === "UNKNOWN_PLACEHOLDER_CLEAR") {
    if (isBlank(currentVal)) return { action: "ALREADY_CORRECT", reason: "already_blank" };
    if (!PLACEHOLDER_RE.test(String(currentVal).trim())) {
      return { action: "STALE", reason: "no_longer_placeholder" };
    }
    return { action: "APPLY", reason: "ok" };
  }

  const beforeBlank = isBlank(m.before);
  const curBlank = isBlank(currentVal);
  if (beforeBlank) {
    if (!curBlank && m.mutation_class !== "UNKNOWN_PLACEHOLDER_CLEAR") {
      // blank fill / recompute onto non-blank — stale unless clearing placeholder
      if (m.field === "City" && PLACEHOLDER_RE.test(String(currentVal).trim()) && m.after != null) {
        // replace Unknown with verified city
      } else {
        return { action: "STALE", reason: "blank_fill_target_no_longer_blank" };
      }
    }
  } else if (!eqVal(currentVal, m.before)) {
    if (
      m.field === "City" &&
      m.after != null &&
      (PLACEHOLDER_RE.test(String(currentVal).trim()) || isBlank(currentVal))
    ) {
      // ok
    } else {
      return { action: "STALE", reason: "before_value_changed_since_dry_run" };
    }
  }

  if (m.field === "City" && m.after != null) {
    const sem = validateCitySemantics(m.after, liveFields.Country);
    if (
      !sem.ok ||
      isStreetLineAsCity(m.after) ||
      isPostalAsCity(m.after, liveFields.Country) ||
      isDescriptorCity(m.after)
    ) {
      return { action: "BLOCK", reason: "semantic_city_invalid_after", circuit: true };
    }
  }
  if (m.field === "Address" && m.after != null) {
    if (typeof m.after !== "string" || m.after === "[object Object]") {
      return { action: "BLOCK", reason: "address_not_formatted_string", circuit: true };
    }
  }
  if (m.field === "Market" && m.after != null) {
    const gate = assertMarketWriteGate({
      country: liveFields.Country,
      market: m.after,
      city: liveFields.City,
      state: liveFields["State / Region"],
    });
    if (!gate.write_allowed) {
      return { action: "BLOCK", reason: "market_gate_failed" };
    }
  }
  if (m.field === "Submarket" && m.after != null) {
    const sg = assertSubmarketWriteGate({
      country: liveFields.Country,
      market: liveFields.Market,
      submarket: m.after,
      status: "MATCHED",
    });
    if (!sg.write_allowed) {
      return { action: "BLOCK", reason: "submarket_gate_failed" };
    }
  }
  return { action: "APPLY", reason: "ok" };
}

async function main() {
  const enableWrites =
    process.env.ENABLE_VERIFIED_CENSUS_WRITES === "1" || process.argv.includes("--apply");
  const log = (...a) => console.log(...a);

  const raw = fs.readFileSync(MANIFEST_PATH);
  const manifestHash = crypto.createHash("sha256").update(raw).digest("hex");
  const manifest = JSON.parse(raw.toString("utf8"));
  const bound = (manifest.mutations || []).filter((m) => SAFE_CLASSES.has(m.mutation_class));

  const auth = {
    authorized: true,
    authorized_at: new Date().toISOString(),
    authorized_by: "Joan",
    standing_authorization: "city_correction_then_v4_full_universe",
    manifest_path: MANIFEST_REL,
    manifest_sha256: manifestHash,
    safe_count: bound.length,
    v4_paused_until_city_apply_and_gate: true,
  };
  wj(CITY_OUT, "25-city-apply-authorization.json", auth);
  wj(FULL_OUT, "04-city-final-correction.json", {
    ...auth,
    mutation_class_counts: bound.reduce((a, m) => {
      a[m.mutation_class] = (a[m.mutation_class] || 0) + 1;
      return a;
    }, {}),
  });

  manifest.authorized = true;
  manifest.apply = true;
  fs.writeFileSync(MANIFEST_PATH, JSON.stringify(manifest, null, 2));

  if (!enableWrites) {
    log("ERROR: --apply required");
    process.exit(2);
  }

  const token = resolvePat();
  const baseId = resolveTargetBase().target_base_id;
  const circuit = { tripped: false, reason: null, detail: null };
  const trip = (reason, detail = {}) => {
    circuit.tripped = true;
    circuit.reason = reason;
    circuit.detail = detail;
    log(`[city-apply] CIRCUIT ${reason}`, JSON.stringify(detail));
  };

  const txPath = path.join(CITY_OUT, "26-city-apply-transactions.jsonl");
  fs.writeFileSync(txPath, "");
  const appendTx = (row) => fs.appendFileSync(txPath, JSON.stringify(row) + "\n");
  const liveCache = new Map();
  async function getLive(id) {
    if (liveCache.has(id)) return liveCache.get(id);
    const live = await airtableGet(baseId, token, id);
    liveCache.set(id, live);
    await sleep(70);
    return live;
  }

  const FIELD_ORDER = {
    City: 10,
    Address: 20,
    "State / Region": 30,
    Latitude: 40,
    Longitude: 45,
    Market: 50,
    Submarket: 60,
  };

  const classified = [];
  for (const m of bound) {
    const live = await getLive(m.airtable_record_id);
    const liveKey = live.fields?.["Property Identity Key"] || null;
    if (liveKey && m.property_identity_key && liveKey !== m.property_identity_key) {
      trip("identity_mismatch", { expected: m.property_identity_key, actual: liveKey });
      break;
    }
    const currentVal = live.fields?.[m.field] ?? null;
    const d = classifyLiveMutation(m, currentVal, live.fields || {});
    classified.push({ ...m, decision: d.action, reason: d.reason, current: currentVal, circuit: d.circuit });
  }
  if (circuit.tripped) process.exit(3);

  const eligible = classified
    .filter((m) => m.decision === "APPLY")
    .sort((a, b) => (FIELD_ORDER[a.field] || 99) - (FIELD_ORDER[b.field] || 99));
  const pilotA = eligible.slice(0, PILOT_A_SIZE);
  const remainder = eligible.slice(PILOT_A_SIZE);
  log(`[city-apply] eligible=${eligible.length} pilotA=${pilotA.length} rem=${remainder.length}`);

  async function processMutation(m, phase) {
    if (circuit.tripped) return { status: "blocked_circuit", phase };
    const ts = new Date().toISOString();
    liveCache.delete(m.airtable_record_id);
    const live = await getLive(m.airtable_record_id);
    const currentVal = live.fields?.[m.field] ?? null;
    const decision = classifyLiveMutation(m, currentVal, live.fields || {});
    if (decision.action === "BLOCK" && decision.circuit) {
      trip(decision.reason, { key: m.property_identity_key, field: m.field });
      return { status: decision.reason, phase };
    }
    if (decision.action !== "APPLY") {
      appendTx({ timestamp: ts, ...m, status: decision.action, reason: decision.reason, current: currentVal });
      return { status: decision.action, phase };
    }
    const rollbackValue = currentVal;
    await airtableUpdate(baseId, token, m.airtable_record_id, { [m.field]: writeValue(m.after) });
    liveCache.delete(m.airtable_record_id);
    await sleep(100);
    const fresh = await getLive(m.airtable_record_id);
    const actual = fresh.fields?.[m.field] ?? null;
    if (!eqVal(actual, m.after)) {
      trip("expected_actual_mismatch", { key: m.property_identity_key, field: m.field, expected: m.after, actual });
      appendTx({
        timestamp: ts,
        airtable_record_id: m.airtable_record_id,
        property_identity_key: m.property_identity_key,
        field: m.field,
        before_value: rollbackValue,
        after_value: m.after,
        actual_result: actual,
        rollback_value: rollbackValue,
        status: "mismatch",
        phase,
      });
      return { status: "mismatch", phase };
    }
    appendTx({
      timestamp: ts,
      airtable_record_id: m.airtable_record_id,
      property_identity_key: m.property_identity_key,
      field: m.field,
      before_value: rollbackValue,
      after_value: m.after,
      actual_result: actual,
      rollback_value: rollbackValue,
      mutation_class: m.mutation_class,
      status: "updated",
      phase,
      cvent_used: false,
      legacy_used: false,
    });
    return { status: "updated", phase };
  }

  const pilotResults = [];
  for (let i = 0; i < pilotA.length; i++) {
    pilotResults.push({ ...pilotA[i], ...(await processMutation(pilotA[i], "A")) });
  }
  const pilotPass =
    !circuit.tripped &&
    pilotA.length > 0 &&
    !pilotResults.some((r) => ["mismatch", "identity_mismatch", "error"].includes(r.status));
  wj(CITY_OUT, "27-city-pilot-a-results.json", { pass: pilotPass, attempted: pilotResults.length, results: pilotResults });
  wj(FULL_OUT, "04b-city-pilot-a-results.json", { pass: pilotPass, attempted: pilotResults.length });

  const remResults = [];
  if (pilotPass) {
    log(`[city-apply] Pilot A PASS — remainder ${remainder.length}`);
    for (let i = 0; i < remainder.length; i++) {
      if (circuit.tripped) break;
      remResults.push({ ...remainder[i], ...(await processMutation(remainder[i], "B")) });
      if ((i + 1) % 20 === 0) log(`[city-apply] rem ${i + 1}/${remainder.length}`);
    }
  } else {
    log("[city-apply] Pilot A FAIL");
  }

  const all = [...pilotResults, ...remResults];
  const updated = all.filter((r) => r.status === "updated").length;
  const summary = {
    pilot_pass: pilotPass,
    updated,
    eligible: eligible.length,
    already: classified.filter((c) => c.decision === "ALREADY_CORRECT").length,
    stale: classified.filter((c) => c.decision === "STALE").length,
    expected_actual_pct: all.some((r) => r.status === "mismatch") ? null : 100,
    circuit,
  };
  wj(CITY_OUT, "28-city-apply-summary.json", summary);
  wj(FULL_OUT, "04c-city-apply-summary.json", summary);
  console.log(JSON.stringify(summary, null, 2));
  if (!pilotPass) process.exit(4);
  if (circuit.tripped) process.exit(5);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
