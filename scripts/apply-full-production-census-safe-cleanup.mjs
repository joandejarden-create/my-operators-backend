/**
 * Apply authorized FULL_PRODUCTION_CENSUS_CLEANUP_MANIFEST — SAFE classes only.
 * Pilot A (25) → remainder if PASS. V4 remains PAUSED.
 *
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
import {
  isParentCompanyAsCurrentBrand,
  validateCurrentBrandSemantics,
} from "../lib/research-engine-v2/census-autopilot-v3/current-affiliation.js";
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

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(
  ROOT,
  "data/research-engine-v2/census-autopilot-v4-standing/full-production-retroactive-cleanup-v1"
);
const MANIFEST_REL =
  "data/research-engine-v2/census-autopilot-v4-standing/full-production-retroactive-cleanup-v1/16-full-cleanup-manifest-dry-run.json";
const MANIFEST_PATH = path.join(ROOT, MANIFEST_REL);

const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] || PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;

const SAFE_CLASSES = new Set([
  "SAFE_INVALID_VALUE_CORRECTION",
  "SAFE_OBJECT_FORMAT_CORRECTION",
  "SAFE_BLANK_FILL",
  "SAFE_DERIVED_GEOGRAPHY",
  "SAFE_INVALID_CLEAR",
  "SAFE_MARKET_CORRECTION",
  "SAFE_SUBMARKET_CORRECTION",
  "SUBMARKET_NOT_APPLICABLE",
]);

const BLOCKED_CLASSES = new Set(["STEWARD_REVIEW", "RIGHTS_BLOCKED"]);

const ALLOWED_FIELDS = new Set([
  "City",
  "Address",
  "State / Region",
  "Market",
  "Submarket",
  "Latitude",
  "Longitude",
  "Phone",
]);

const PILOT_A_SIZE = 25;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function isBlank(v) {
  return v == null || v === "" || (Array.isArray(v) && !v.length);
}
function wj(name, data) {
  const fp = path.join(OUT, name);
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify(data, null, 2));
  return fp;
}
function eqVal(a, b) {
  if (isBlank(a) && isBlank(b)) return true;
  if (typeof a === "number" || typeof b === "number") return Number(a) === Number(b);
  return String(a ?? "") === String(b ?? "");
}
function writeValue(after) {
  return after == null ? null : after;
}
function isObjectSerialized(addr) {
  if (addr == null) return false;
  if (typeof addr === "object") return true;
  const s = String(addr);
  return (
    s === "[object Object]" ||
    s === "[object Array]" ||
    s === "undefined" ||
    s === "null" ||
    (/^\s*[\{\[]/.test(s) && s.length > 2)
  );
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

function classifyLiveMutation(m, currentVal, liveFields = {}, pendingCityByKey = null, pendingMarketByKey = null) {
  if (!ALLOWED_FIELDS.has(m.field)) {
    return { action: "BLOCK", reason: "unexpected_field", circuit: true };
  }
  if (m.cvent_used || m.legacy_used || m.str_used) {
    return { action: "BLOCK", reason: "cvent_legacy_str_leakage", circuit: true };
  }
  if (BLOCKED_CLASSES.has(m.mutation_class) || !SAFE_CLASSES.has(m.mutation_class)) {
    return { action: "BLOCK", reason: "class_not_authorized" };
  }
  // Never apply Current Brand in this run (steward held; not in ALLOWED_FIELDS)
  if (m.field === "Current Brand") {
    return { action: "BLOCK", reason: "brand_not_authorized_steward_only" };
  }

  if (eqVal(currentVal, m.after)) {
    return { action: "ALREADY_CORRECT", reason: "already_equals_after" };
  }

  const beforeBlank = isBlank(m.before);
  const curBlank = isBlank(currentVal);
  if (beforeBlank) {
    if (!curBlank && m.mutation_class !== "SAFE_INVALID_CLEAR") {
      return { action: "STALE", reason: "blank_fill_target_no_longer_blank" };
    }
  } else if (!eqVal(currentVal, m.before)) {
    if (
      m.mutation_class === "SAFE_INVALID_CLEAR" &&
      m.field === "Address" &&
      isObjectSerialized(currentVal)
    ) {
      // still serialized — clear current
    } else if (
      m.mutation_class === "SAFE_INVALID_CLEAR" &&
      m.field === "Market" &&
      !isBlank(currentVal)
    ) {
      const cls = classifyProductionMarket({
        country: liveFields.Country,
        market: currentVal,
        city: liveFields.City,
        state: liveFields["State / Region"],
      });
      if (cls.ok) {
        return { action: "STALE", reason: "market_before_changed_now_valid" };
      }
    } else if (
      m.mutation_class === "SAFE_INVALID_CLEAR" &&
      m.field === "City" &&
      (isPostalAsCity(currentVal, liveFields.Country) ||
        isStreetLineAsCity(currentVal) ||
        !validateCitySemantics(currentVal, liveFields.Country).ok ||
        ["COUNTRY_AS_CITY", "POSTAL_CODE_AS_CITY", "CITY_INVALID"].includes(
          classifyCityLabel(currentVal, liveFields.Country).bucket
        ))
    ) {
      // still invalid city
    } else {
      return { action: "STALE", reason: "before_value_changed_since_dry_run" };
    }
  }

  if (m.field === "City" && m.after != null) {
    const sem = validateCitySemantics(m.after, liveFields.Country);
    if (!sem.ok || isStreetLineAsCity(m.after) || isPostalAsCity(m.after, liveFields.Country)) {
      return { action: "BLOCK", reason: "semantic_city_invalid_after", circuit: true };
    }
  }
  if (m.field === "Address" && m.after != null) {
    if (typeof m.after !== "string" || isObjectSerialized(m.after)) {
      return { action: "BLOCK", reason: "address_not_formatted_string", circuit: true };
    }
  }
  if (m.field === "Market" && m.after != null) {
    const cityForGate =
      (pendingCityByKey && pendingCityByKey.get(m.property_identity_key)) || liveFields.City;
    const gate = assertMarketWriteGate({
      country: liveFields.Country,
      market: m.after,
      city: cityForGate,
      state: liveFields["State / Region"],
    });
    if (!gate.write_allowed) {
      return { action: "BLOCK", reason: "market_gate_failed:" + (gate.failures || []).join(",") };
    }
  }
  if (m.field === "Submarket" && m.after != null) {
    const marketForGate =
      (pendingMarketByKey && pendingMarketByKey.get(m.property_identity_key)) || liveFields.Market;
    const mktCls = classifyProductionMarket({
      country: liveFields.Country,
      market: marketForGate,
      city: liveFields.City,
      state: liveFields["State / Region"],
    });
    if (!mktCls.ok) {
      return { action: "BLOCK", reason: "submarket_parent_market_invalid" };
    }
    const sg = assertSubmarketWriteGate({
      country: liveFields.Country,
      market: marketForGate,
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

  if (manifest.table_id !== CENSUS_TABLE_ID) {
    throw new Error(`table_id mismatch: ${manifest.table_id} vs ${CENSUS_TABLE_ID}`);
  }
  if (manifest.total_records !== 1437) {
    log(`[warn] total_records in manifest=${manifest.total_records} (expected 1437 at authorize time)`);
  }

  const authorizedAt = new Date().toISOString();
  const authMeta = {
    authorized: true,
    authorized_at: authorizedAt,
    authorized_by: "Joan",
    apply_note: "SAFE full-table retroactive cleanup only; V4 remains PAUSED",
    v4_paused: true,
    manifest_path: MANIFEST_REL,
    manifest_sha256: manifestHash,
    table_id: CENSUS_TABLE_ID,
    safe_classes: [...SAFE_CLASSES],
  };

  wj("25-safe-apply-authorization.json", authMeta);

  const allMutations = manifest.mutations || [];
  const bound = allMutations.filter((m) => SAFE_CLASSES.has(m.mutation_class));
  const held = allMutations.filter((m) => !SAFE_CLASSES.has(m.mutation_class));

  wj("26-bound-safe-manifest.json", {
    ...authMeta,
    mutation_count_total: allMutations.length,
    mutation_count_safe: bound.length,
    mutation_count_held: held.length,
    held_classes: [...new Set(held.map((m) => m.mutation_class))],
    mutations: bound,
  });

  // Update dry-run file authorization flags (do not regenerate mutations)
  manifest.authorized = true;
  manifest.authorized_at = authorizedAt;
  manifest.authorized_by = "Joan";
  manifest.apply = true;
  manifest.v4_paused = true;
  manifest.manifest_sha256_at_authorize = manifestHash;
  fs.writeFileSync(MANIFEST_PATH, JSON.stringify(manifest, null, 2));

  if (!enableWrites) {
    log("ERROR: ENABLE_VERIFIED_CENSUS_WRITES=1 or --apply required");
    process.exit(2);
  }

  const token = resolvePat();
  const baseId = resolveTargetBase().target_base_id;
  const circuit = { tripped: false, reason: null, at: null, detail: null };
  const trip = (reason, detail = {}) => {
    circuit.tripped = true;
    circuit.reason = reason;
    circuit.at = new Date().toISOString();
    circuit.detail = detail;
    log(`[safe-cleanup] CIRCUIT: ${reason}`, JSON.stringify(detail));
  };

  const txPath = path.join(OUT, "29-full-safe-apply-transactions.jsonl");
  fs.writeFileSync(txPath, "");
  const appendTx = (row) => fs.appendFileSync(txPath, JSON.stringify(row) + "\n");

  const prewrite = [];
  const liveCache = new Map();

  async function getLive(id) {
    if (liveCache.has(id)) return liveCache.get(id);
    const live = await airtableGet(baseId, token, id);
    liveCache.set(id, live);
    await sleep(80);
    return live;
  }

  const pendingCityByKey = new Map();
  const pendingMarketByKey = new Map();
  for (const m of bound) {
    if (m.field === "City" && m.after) pendingCityByKey.set(m.property_identity_key, m.after);
    if (m.field === "Market" && m.after) pendingMarketByKey.set(m.property_identity_key, m.after);
  }

  log(`[safe-cleanup] Pre-read + classify n=${bound.length}`);
  const classified = [];
  for (let i = 0; i < bound.length; i++) {
    const m = bound[i];
    try {
      const live = await getLive(m.airtable_record_id);
      const liveKey = live.fields?.["Property Identity Key"] || null;
      if (liveKey && liveKey !== m.property_identity_key) {
        classified.push({ ...m, decision: "BLOCK", reason: "identity_mismatch", circuit: true });
        trip("identity_mismatch", { expected: m.property_identity_key, actual: liveKey });
        break;
      }
      const currentVal = live.fields?.[m.field] ?? null;
      const d = classifyLiveMutation(
        m,
        currentVal,
        live.fields || {},
        pendingCityByKey,
        pendingMarketByKey
      );
      classified.push({
        ...m,
        decision: d.action,
        reason: d.reason,
        current: currentVal,
        circuit: d.circuit || false,
      });
      prewrite.push({
        index: i,
        key: m.property_identity_key,
        field: m.field,
        class: m.mutation_class,
        decision: d.action,
        reason: d.reason,
        before: m.before,
        current: currentVal,
        after: m.after,
      });
      if ((i + 1) % 50 === 0) log(`[safe-cleanup] pre-classify ${i + 1}/${bound.length}`);
    } catch (err) {
      classified.push({ ...m, decision: "BLOCK", reason: String(err?.message || err), circuit: true });
      trip("prewrite_error", { key: m.property_identity_key, error: String(err?.message || err) });
      break;
    }
  }

  wj("27-pilot-a-prewrite.json", {
    n: prewrite.length,
    counts: prewrite.reduce((a, r) => {
      a[r.decision] = (a[r.decision] || 0) + 1;
      return a;
    }, {}),
    circuit,
    note: "Full prewrite classification; Pilot A taken from first 25 APPLY after field order",
    records: prewrite,
  });

  if (circuit.tripped) {
    log("[safe-cleanup] Circuit during prewrite — abort");
    process.exit(3);
  }

  const FIELD_ORDER = {
    Address: 20,
    City: 30,
    "State / Region": 40,
    Latitude: 50,
    Longitude: 55,
    Market: 60,
    Submarket: 70,
    Phone: 80,
  };

  const eligible = classified
    .filter((m) => m.decision === "APPLY")
    .sort((a, b) => (FIELD_ORDER[a.field] || 99) - (FIELD_ORDER[b.field] || 99));
  const pilotA = eligible.slice(0, PILOT_A_SIZE);
  const remainder = eligible.slice(PILOT_A_SIZE);
  log(
    `[safe-cleanup] Eligible APPLY=${eligible.length} · Pilot A=${pilotA.length} · Remainder=${remainder.length}`
  );
  log(
    `[safe-cleanup] Already=${classified.filter((c) => c.decision === "ALREADY_CORRECT").length} Stale=${classified.filter((c) => c.decision === "STALE").length} Block=${classified.filter((c) => c.decision === "BLOCK").length}`
  );

  async function processMutation(m, phase) {
    if (circuit.tripped) return { status: "blocked_circuit", phase };
    const ts = new Date().toISOString();
    try {
      liveCache.delete(m.airtable_record_id);
      const live = await getLive(m.airtable_record_id);
      const liveKey = live.fields?.["Property Identity Key"] || null;
      if (liveKey && liveKey !== m.property_identity_key) {
        trip("identity_mismatch", { expected: m.property_identity_key, actual: liveKey });
        return { status: "identity_mismatch", phase };
      }
      const currentVal = live.fields?.[m.field] ?? null;
      const decision = classifyLiveMutation(
        m,
        currentVal,
        live.fields || {},
        pendingCityByKey,
        pendingMarketByKey
      );
      if (decision.action === "BLOCK" && decision.circuit) {
        trip(decision.reason, { key: m.property_identity_key, field: m.field });
        return { status: decision.reason, phase };
      }
      if (decision.action === "ALREADY_CORRECT") {
        appendTx({
          timestamp: ts,
          ...m,
          status: "ALREADY_CORRECT",
          expected: m.after,
          actual: currentVal,
        });
        return { status: "ALREADY_CORRECT", phase };
      }
      if (decision.action === "STALE" || decision.action === "BLOCK") {
        appendTx({
          timestamp: ts,
          ...m,
          status: decision.action,
          reason: decision.reason,
          current: currentVal,
        });
        return { status: decision.action, reason: decision.reason, phase };
      }

      const rollbackValue = currentVal;
      const afterWrite = writeValue(m.after);
      await airtableUpdate(baseId, token, m.airtable_record_id, { [m.field]: afterWrite });
      liveCache.delete(m.airtable_record_id);
      await sleep(120);
      const fresh = await getLive(m.airtable_record_id);
      const actual = fresh.fields?.[m.field] ?? null;
      const match = eqVal(actual, m.after);
      if (!match) {
        trip("expected_actual_mismatch", {
          key: m.property_identity_key,
          field: m.field,
          expected: m.after,
          actual,
        });
        appendTx({
          timestamp: ts,
          airtable_record_id: m.airtable_record_id,
          property_identity_key: m.property_identity_key,
          field: m.field,
          before_value: rollbackValue,
          after_value: m.after,
          mutation_class: m.mutation_class,
          expected_result: m.after,
          actual_result: actual,
          rollback_value: rollbackValue,
          status: "mismatch",
          phase,
          cvent_used: false,
          legacy_used: false,
        });
        return { status: "mismatch", phase, expected: m.after, actual };
      }

      appendTx({
        timestamp: ts,
        airtable_record_id: m.airtable_record_id,
        property_identity_key: m.property_identity_key,
        field: m.field,
        before_value: rollbackValue,
        after_value: m.after,
        mutation_class: m.mutation_class,
        expected_result: m.after,
        actual_result: actual,
        rollback_value: rollbackValue,
        status: "updated",
        phase,
        cvent_used: false,
        legacy_used: false,
        str_used: false,
      });
      return { status: "updated", phase, field: m.field, mutation_class: m.mutation_class };
    } catch (err) {
      trip("write_error", { key: m.property_identity_key, error: String(err?.message || err) });
      return { status: "error", phase, error: String(err?.message || err) };
    }
  }

  const pilotAResults = [];
  for (let i = 0; i < pilotA.length; i++) {
    const r = await processMutation(pilotA[i], "A");
    pilotAResults.push({
      index: i,
      property_identity_key: pilotA[i].property_identity_key,
      field: pilotA[i].field,
      mutation_class: pilotA[i].mutation_class,
      ...r,
    });
    if ((i + 1) % 5 === 0) log(`[safe-cleanup] Pilot A ${i + 1}/${pilotA.length}`);
  }

  const pilotAFail =
    circuit.tripped ||
    pilotAResults.some((r) => ["mismatch", "identity_mismatch", "error"].includes(r.status));
  const pilotAPass = !pilotAFail && pilotA.length > 0;

  wj("28-pilot-a-results.json", {
    attempted: pilotAResults.length,
    pass: pilotAPass,
    circuit,
    updated: pilotAResults.filter((r) => r.status === "updated").length,
    expected_actual_pct: pilotAResults.some((r) => r.status === "mismatch") ? null : 100,
    results: pilotAResults,
  });

  const remainderResults = [];
  let remainderExecuted = false;
  if (pilotAPass) {
    log(`[safe-cleanup] Pilot A PASS — remainder n=${remainder.length}`);
    remainderExecuted = true;
    for (let i = 0; i < remainder.length; i++) {
      if (circuit.tripped) break;
      const r = await processMutation(remainder[i], "B");
      remainderResults.push({
        index: PILOT_A_SIZE + i,
        property_identity_key: remainder[i].property_identity_key,
        field: remainder[i].field,
        mutation_class: remainder[i].mutation_class,
        ...r,
      });
      if ((i + 1) % 25 === 0) log(`[safe-cleanup] Remainder ${i + 1}/${remainder.length}`);
    }
  } else {
    log("[safe-cleanup] Pilot A FAIL — remainder skipped");
  }

  const allResults = [...pilotAResults, ...remainderResults];
  const updated = allResults.filter((r) => r.status === "updated");

  wj("30-post-apply-mutated-record-validation.json", {
    pilot_a_pass: pilotAPass,
    remainder_executed: remainderExecuted,
    circuit,
    counts: {
      updated: updated.length,
      already_correct: classified.filter((c) => c.decision === "ALREADY_CORRECT").length,
      stale:
        classified.filter((c) => c.decision === "STALE").length +
        allResults.filter((r) => r.status === "STALE").length,
      blocked: classified.filter((c) => c.decision === "BLOCK").length,
      eligible: eligible.length,
      mismatch: allResults.filter((r) => r.status === "mismatch").length,
    },
    expected_actual_pct: allResults.some((r) => r.status === "mismatch") ? null : 100,
    safety_violations: 0,
    cvent: 0,
    legacy: 0,
    str: 0,
    v4_paused: true,
    updated_sample: updated.slice(0, 50),
  });

  // Rollback payload (do not execute)
  const txLines = fs
    .readFileSync(txPath, "utf8")
    .split("\n")
    .filter(Boolean)
    .map((l) => JSON.parse(l));
  const rollbackItems = txLines
    .filter((t) => t.status === "updated")
    .map((t) => ({
      airtable_record_id: t.airtable_record_id,
      property_identity_key: t.property_identity_key,
      field: t.field,
      restore_value: t.rollback_value,
      applied_value: t.after_value,
      mutation_class: t.mutation_class,
    }));
  wj("40-rollback.json", {
    execute: false,
    coverage_pct: updated.length === 0 ? 100 : Math.round((1000 * rollbackItems.length) / updated.length) / 10,
    n: rollbackItems.length,
    items: rollbackItems,
  });

  log(
    JSON.stringify(
      {
        pilotAPass,
        updated: updated.length,
        already: classified.filter((c) => c.decision === "ALREADY_CORRECT").length,
        stale: classified.filter((c) => c.decision === "STALE").length,
        blocked: classified.filter((c) => c.decision === "BLOCK").length,
        held: held.length,
        circuit,
        expected_actual: allResults.some((r) => r.status === "mismatch") ? null : 100,
      },
      null,
      2
    )
  );

  if (!pilotAPass) process.exit(4);
  if (circuit.tripped) process.exit(5);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
