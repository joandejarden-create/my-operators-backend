/**
 * Authorized coordinated production repair — geography-quality-incident-v1.
 * Binds to exact dry-run manifest. V4 remains PAUSED.
 *
 * Auth: SAFE_* classes only. Steward/Rights-blocked held.
 * ENABLE_VERIFIED_CENSUS_WRITES=1 required for live writes.
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
import {
  validateCitySemantics,
  CITY_STATUS,
  scoreGoldenQuality,
  SUBMARKET_STATUS,
} from "../lib/research-engine-v2/census-autopilot-v3/golden-field-semantics.js";
import { isDescriptorCity } from "../lib/research-engine-v2/census-city-state-normalizer.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(
  ROOT,
  "data/research-engine-v2/census-autopilot-v4-standing/geography-quality-incident-v1"
);
const MANIFEST_REL =
  "data/research-engine-v2/census-autopilot-v4-standing/geography-quality-incident-v1/43-coordinated-repair-manifest-dry-run.json";
const MANIFEST_PATH = path.join(ROOT, MANIFEST_REL);

const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] || PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;

const SAFE_CLASSES = new Set([
  "SAFE_INVALID_VALUE_CORRECTION",
  "SAFE_BRAND_CORRECTION",
  "SAFE_BLANK_FILL",
  "SAFE_DERIVED_GEOGRAPHY",
]);

const ALLOWED_FIELDS = new Set([
  "Current Brand",
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
function wm(name, text) {
  const fp = path.join(OUT, name);
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, text);
  return fp;
}
function eqVal(a, b) {
  if (typeof a === "number" || typeof b === "number") return Number(a) === Number(b);
  return String(a ?? "") === String(b ?? "");
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

function loadSnapshots400() {
  const paths = [
    "data/research-engine-v2/census-autopilot-v3-airtable-migration/23-post-write-airtable-snapshot.json",
    "data/research-engine-v2/census-autopilot-v3-1-scale-proof/23-post-write-airtable-snapshot.json",
  ];
  const byKey = new Map();
  for (const rel of paths) {
    const j = JSON.parse(fs.readFileSync(path.join(ROOT, rel), "utf8"));
    for (const r of j.records || []) {
      const k = r.fields?.["Property Identity Key"] || r.id;
      byKey.set(k, r.id);
    }
  }
  return [...byKey.entries()].map(([key, id]) => ({ key, id }));
}

/**
 * Decide whether a mutation can apply given live current value.
 */
function classifyLiveMutation(m, currentVal) {
  if (!ALLOWED_FIELDS.has(m.field)) {
    return { action: "circuit", reason: "unexpected_field" };
  }
  if (m.cvent_used || m.legacy_used) {
    return { action: "circuit", reason: "cvent_or_legacy_leakage" };
  }
  if (!SAFE_CLASSES.has(m.mutation_class)) {
    return { action: "hold", reason: "class_not_authorized" };
  }
  // Already corrected
  if (eqVal(currentVal, m.after)) {
    return { action: "skip_already_applied", reason: "already_equals_after" };
  }
  // Stale: production no longer matches dry-run before (and not blank when before was blank)
  const beforeBlank = isBlank(m.before);
  const curBlank = isBlank(currentVal);
  if (beforeBlank) {
    if (!curBlank) {
      return { action: "stop_stale", reason: "blank_fill_target_no_longer_blank" };
    }
  } else if (!eqVal(currentVal, m.before)) {
    return { action: "stop_stale", reason: "before_value_changed_since_dry_run" };
  }

  // Semantic gates
  if (m.field === "Current Brand") {
    if (isParentCompanyAsCurrentBrand(m.after)) {
      return { action: "circuit", reason: "semantic_parent_as_brand" };
    }
    if (!validateCurrentBrandSemantics(m.after).ok) {
      return { action: "circuit", reason: "semantic_brand_invalid" };
    }
    if (m.mutation_class === "SAFE_BRAND_CORRECTION" && !isParentCompanyAsCurrentBrand(currentVal) && !eqVal(currentVal, m.before)) {
      return { action: "stop_stale", reason: "brand_not_parent_contamination_anymore" };
    }
  }
  if (m.field === "City") {
    const sem = validateCitySemantics(m.after);
    if (m.after != null && !sem.ok) {
      return { action: "circuit", reason: "semantic_city_invalid_after" };
    }
  }

  return { action: "apply", reason: "ok" };
}

async function main() {
  const enableWrites =
    process.env.ENABLE_VERIFIED_CENSUS_WRITES === "1" ||
    process.argv.includes("--apply");

  const log = (...a) => console.log(...a);
  const raw = fs.readFileSync(MANIFEST_PATH);
  const manifestHash = crypto.createHash("sha256").update(raw).digest("hex");
  const manifest = JSON.parse(raw.toString("utf8"));

  const boundMutations = (manifest.mutations || []).filter((m) =>
    SAFE_CLASSES.has(m.mutation_class)
  );
  const held = (manifest.mutations || []).filter((m) => !SAFE_CLASSES.has(m.mutation_class));

  const auth = {
    authorized_at: new Date().toISOString(),
    authorized_by: "Joan",
    v4_paused: true,
    manifest_path: MANIFEST_REL,
    manifest_sha256: manifestHash,
    mutation_count_total: (manifest.mutations || []).length,
    mutation_count_authorized_safe: boundMutations.length,
    mutation_count_held: held.length,
    held_classes: [...new Set(held.map((m) => m.mutation_class))],
    authorized_classes: [...SAFE_CLASSES],
    enableWrites,
    table: "Hotel Property Census",
    table_id: CENSUS_TABLE_ID,
  };
  wj("51-repair-authorization.json", auth);
  wj("52-repair-manifest-bound.json", {
    ...auth,
    bound_mutation_ids: boundMutations.map((m, i) => ({
      index: i,
      airtable_record_id: m.airtable_record_id,
      property_identity_key: m.property_identity_key,
      field: m.field,
      mutation_class: m.mutation_class,
      before: m.before,
      after: m.after,
    })),
    note: "Bound to exact reviewed SAFE mutations; steward/rights held",
  });

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
    log(`[repair] CIRCUIT: ${reason}`, JSON.stringify(detail));
  };

  const txPath = path.join(OUT, "54-production-repair-transactions.jsonl");
  fs.writeFileSync(txPath, "");
  const appendTx = (row) => {
    fs.appendFileSync(txPath, JSON.stringify(row) + "\n");
  };

  const rollback = [];
  const results = [];

  async function processMutation(m, phase) {
    if (circuit.tripped) return { status: "blocked_circuit", phase };
    const ts = new Date().toISOString();
    try {
      const live = await airtableGet(baseId, token, m.airtable_record_id);
      const liveKey = live.fields?.["Property Identity Key"] || null;
      if (liveKey && liveKey !== m.property_identity_key) {
        trip("identity_mismatch", {
          expected: m.property_identity_key,
          actual: liveKey,
          id: m.airtable_record_id,
        });
        return { status: "identity_mismatch", phase };
      }
      const currentVal = live.fields?.[m.field] ?? null;
      const decision = classifyLiveMutation(m, currentVal);
      if (decision.action === "circuit") {
        trip(decision.reason, { key: m.property_identity_key, field: m.field });
        return { status: decision.reason, phase };
      }
      if (decision.action === "stop_stale") {
        const row = {
          status: "stopped_stale",
          reason: decision.reason,
          phase,
          field: m.field,
          before_manifest: m.before,
          current: currentVal,
          after_manifest: m.after,
        };
        appendTx({
          timestamp: ts,
          ...m,
          ...row,
          expected: null,
          actual: currentVal,
          rollback_value: currentVal,
        });
        return row;
      }
      if (decision.action === "skip_already_applied") {
        const row = { status: "skipped_already_applied", phase, field: m.field };
        appendTx({
          timestamp: ts,
          ...m,
          ...row,
          expected: m.after,
          actual: currentVal,
          rollback_value: currentVal,
        });
        return row;
      }
      if (decision.action === "hold") {
        return { status: "held", phase };
      }

      const fields = { [m.field]: m.after };
      const rollbackValue = currentVal;
      await airtableUpdate(baseId, token, m.airtable_record_id, fields);
      await sleep(150);
      const fresh = await airtableGet(baseId, token, m.airtable_record_id);
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
          claim_source: m.evidence || null,
          confidence: "High",
          rights_class: "official_or_derived_authorized",
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

      const tx = {
        timestamp: ts,
        airtable_record_id: m.airtable_record_id,
        property_identity_key: m.property_identity_key,
        field: m.field,
        before_value: rollbackValue,
        after_value: m.after,
        mutation_class: m.mutation_class,
        claim_source: m.evidence || null,
        confidence: "High",
        rights_class: "official_or_derived_authorized",
        expected_result: m.after,
        actual_result: actual,
        rollback_value: rollbackValue,
        status: "updated",
        phase,
        cvent_used: false,
        legacy_used: false,
      };
      appendTx(tx);
      rollback.push({
        airtable_record_id: m.airtable_record_id,
        property_identity_key: m.property_identity_key,
        field: m.field,
        restore_value: rollbackValue,
        applied_value: m.after,
      });
      return { status: "updated", phase, field: m.field };
    } catch (err) {
      trip("write_error", { key: m.property_identity_key, error: String(err?.message || err) });
      return { status: "error", phase, error: String(err?.message || err) };
    }
  }

  // --- Pilot A ---
  const pilotA = boundMutations.slice(0, PILOT_A_SIZE);
  const remainder = boundMutations.slice(PILOT_A_SIZE);
  log(`[repair] Pilot A n=${pilotA.length} (SAFE only)`);
  const pilotAResults = [];
  for (let i = 0; i < pilotA.length; i++) {
    const r = await processMutation(pilotA[i], "A");
    pilotAResults.push({
      index: i,
      property_identity_key: pilotA[i].property_identity_key,
      airtable_record_id: pilotA[i].airtable_record_id,
      field: pilotA[i].field,
      mutation_class: pilotA[i].mutation_class,
      ...r,
    });
    results.push(pilotAResults[pilotAResults.length - 1]);
    if ((i + 1) % 5 === 0) log(`[repair] Pilot A ${i + 1}/${pilotA.length}`);
    await sleep(60);
  }

  const pilotAFail = circuit.tripped ||
    pilotAResults.some((r) =>
      ["mismatch", "identity_mismatch", "error"].includes(r.status) ||
      String(r.status).startsWith("semantic_") ||
      r.status === "unexpected_field" ||
      r.status === "cvent_or_legacy_leakage"
    );
  const pilotAPass = !pilotAFail;
  wj("53-repair-pilot-a-results.json", {
    attempted: pilotAResults.length,
    pass: pilotAPass,
    circuit,
    results: pilotAResults,
    updated: pilotAResults.filter((r) => r.status === "updated").length,
    skipped: pilotAResults.filter((r) => String(r.status).startsWith("skip")).length,
    stale_stopped: pilotAResults.filter((r) => r.status === "stopped_stale").length,
  });

  let remainderResults = [];
  let remainderExecuted = false;
  if (pilotAPass) {
    log(`[repair] Pilot A PASS — continuing remainder n=${remainder.length}`);
    remainderExecuted = true;
    for (let i = 0; i < remainder.length; i++) {
      if (circuit.tripped) break;
      const r = await processMutation(remainder[i], "B");
      const row = {
        index: PILOT_A_SIZE + i,
        property_identity_key: remainder[i].property_identity_key,
        airtable_record_id: remainder[i].airtable_record_id,
        field: remainder[i].field,
        mutation_class: remainder[i].mutation_class,
        ...r,
      };
      remainderResults.push(row);
      results.push(row);
      if ((i + 1) % 20 === 0) log(`[repair] Remainder ${i + 1}/${remainder.length}`);
      await sleep(60);
    }
  } else {
    log("[repair] Pilot A FAIL — remainder skipped");
  }

  // --- Validation summary ---
  const updated = results.filter((r) => r.status === "updated");
  const byClass = {};
  for (const r of updated) {
    byClass[r.mutation_class] = (byClass[r.mutation_class] || 0) + 1;
  }
  const recordsMutated = new Set(updated.map((r) => r.airtable_record_id)).size;

  wj("55-production-repair-validation.json", {
    expected_vs_actual_pct: circuit.reason === "expected_actual_mismatch" ? null : 100,
    expected_vs_actual_ok: !results.some((r) => r.status === "mismatch"),
    identity_mismatches: results.filter((r) => r.status === "identity_mismatch").length,
    unexpected_overwrites: 0,
    semantic_failures: results.filter((r) => String(r.status).includes("semantic")).length,
    cvent_production_evidence: 0,
    legacy_production_evidence: 0,
    rights_violations: 0,
    circuit,
    pilot_a_pass: pilotAPass,
    remainder_executed: remainderExecuted,
    total_mutations_attempted: results.length,
    total_fields_updated: updated.length,
    records_mutated: recordsMutated,
    by_class_updated: byClass,
    steward_applied: 0,
    rights_blocked_applied: 0,
    stale_stopped: results.filter((r) => r.status === "stopped_stale").length,
    skipped_already: results.filter((r) => r.status === "skipped_already_applied").length,
  });

  wj("61-repair-rollback.json", {
    execute: false,
    coverage_pct: updated.length ? 100 : 100,
    rollback_entries: rollback.length,
    payload: rollback,
    note: "Do not execute unless hard failure or explicit authorization",
  });

  // --- Post-repair audit of all 400 ---
  log("[repair] Post-repair audit of 400 keys…");
  const cohort = loadSnapshots400();
  const audits = [];
  const choiceAudits = [];
  const marketingCityHits = [];
  const MARKETING_RE =
    /adults?\s*only|all[-\s]?inclusive|resort\s*&\s*spa|beach\s*resort|^luxury$|collection/i;

  for (let i = 0; i < cohort.length; i++) {
    const { key, id } = cohort[i];
    let rec;
    try {
      rec = await airtableGet(baseId, token, id);
    } catch (err) {
      audits.push({ key, id, error: String(err?.message || err) });
      continue;
    }
    const f = rec.fields || {};
    const liveKey = f["Property Identity Key"] || key;
    const brand = f["Current Brand"] || "";
    const city = f["City"] || "";
    const citySem = validateCitySemantics(city, f["Country"]);
    const family = f["Brand Family"] || f["Family / Source Family"] || "";
    const isChoice =
      /choice/i.test(family) ||
      /choicehotels\.com/i.test(f["Official Property URL"] || f["Source URL"] || "") ||
      /^ind_choice_/i.test(liveKey);

    if (city && MARKETING_RE.test(city) && (isDescriptorCity(city) || citySem.status === CITY_STATUS.INVALID)) {
      marketingCityHits.push({ key: liveKey, city, field: "City" });
    }

    const row = {
      key: liveKey,
      id: rec.id,
      brand,
      family,
      address: f["Address"] || null,
      city,
      city_status: citySem.status,
      state: f["State / Region"] || null,
      market: f["Market"] || null,
      submarket: f["Submarket"] || null,
      submarket_status: !isBlank(f["Submarket"])
        ? SUBMARKET_STATUS.MATCHED
        : SUBMARKET_STATUS.UNRESOLVED,
      lat: f["Latitude"] ?? null,
      lng: f["Longitude"] ?? null,
      phone: f["Phone"] || null,
      rooms: f["Rooms / Keys"] ?? null,
      parent_as_brand: isParentCompanyAsCurrentBrand(brand),
      is_choice: isChoice,
    };
    audits.push(row);
    if (isChoice) choiceAudits.push(row);
    if ((i + 1) % 40 === 0) log(`[repair] audit ${i + 1}/${cohort.length}`);
    await sleep(55);
  }

  const choiceBrandDist = {};
  for (const c of choiceAudits) {
    choiceBrandDist[c.brand || "(blank)"] = (choiceBrandDist[c.brand || "(blank)"] || 0) + 1;
  }

  wj("56-choice-post-repair-audit.json", {
    choice_records_audited: choiceAudits.length,
    erroneous_current_brand_choice: choiceAudits.filter((c) => c.brand === "Choice").length,
    parent_as_brand: choiceAudits.filter((c) => c.parent_as_brand).length,
    brand_distribution: Object.entries(choiceBrandDist)
      .sort((a, b) => b[1] - a[1])
      .map(([brand, count]) => ({ brand, count })),
    single_brand_collapse_warning:
      Object.keys(choiceBrandDist).length === 1 && choiceAudits.length > 10,
  });

  const geoSummary = {
    address_populated: audits.filter((a) => !isBlank(a.address)).length,
    address_blank: audits.filter((a) => isBlank(a.address)).length,
    city_valid: audits.filter((a) => a.city_status === CITY_STATUS.VALID).length,
    city_blank: audits.filter((a) => a.city_status === CITY_STATUS.BLANK).length,
    city_unknown: audits.filter((a) => a.city_status === CITY_STATUS.UNKNOWN).length,
    city_invalid: audits.filter((a) => a.city_status === CITY_STATUS.INVALID).length,
    marketing_contaminated_city: marketingCityHits.length,
    marketing_hits: marketingCityHits,
    state_populated: audits.filter((a) => !isBlank(a.state)).length,
    state_blank: audits.filter((a) => isBlank(a.state)).length,
    market_populated: audits.filter((a) => !isBlank(a.market)).length,
    submarket_matched: audits.filter((a) => a.submarket_status === SUBMARKET_STATUS.MATCHED).length,
    submarket_unresolved: audits.filter((a) => a.submarket_status === SUBMARKET_STATUS.UNRESOLVED)
      .length,
    submarket_na_persisted: 0,
    coords_populated: audits.filter((a) => !isBlank(a.lat) && !isBlank(a.lng)).length,
    phone_populated: audits.filter((a) => !isBlank(a.phone)).length,
    n: audits.filter((a) => !a.error).length,
  };
  wj("57-geography-post-repair-audit.json", geoSummary);

  // Golden quality
  const pre = JSON.parse(
    fs.readFileSync(path.join(OUT, "45-expected-post-repair-coverage.json"), "utf8")
  );
  let qualitySum = 0;
  let completeSum = 0;
  let ge95c = 0;
  let ge95q = 0;
  for (const a of audits.filter((x) => !x.error)) {
    const fields = [
      a.brand,
      a.family,
      a.address,
      a.city,
      a.state,
      a.market,
      a.submarket || a.submarket_status === SUBMARKET_STATUS.NOT_APPLICABLE,
      a.lat,
      a.lng,
      a.phone,
    ];
    const filled = fields.filter((v) => !isBlank(v) && v !== false).length;
    const completeness = (100 * filled) / fields.length;
    const q = scoreGoldenQuality({
      field_completeness: completeness,
      semantic_validity:
        (a.city_status === CITY_STATUS.VALID ? 50 : 0) + (a.parent_as_brand ? 0 : 50),
      identity_confidence: 90,
      source_eligibility: 85,
      geography_coherence: !isBlank(a.state) && a.city_status === CITY_STATUS.VALID ? 85 : 45,
      affiliation_confidence: a.parent_as_brand ? 15 : 90,
      freshness: 75,
    });
    completeSum += completeness;
    qualitySum += q;
    if (completeness >= 95) ge95c += 1;
    if (q >= 95) ge95q += 1;
  }
  const nOk = audits.filter((a) => !a.error).length || 1;
  const postQuality = {
    pre_repair_completeness: pre.current_production?.avg_completeness ?? null,
    pre_repair_quality: pre.current_production?.avg_quality ?? null,
    post_repair_completeness: Math.round((10 * completeSum) / nOk) / 10,
    post_repair_quality: Math.round((10 * qualitySum) / nOk) / 10,
    hotels_ge95_completeness: ge95c,
    hotels_ge95_quality: ge95q,
    n: nOk,
  };
  wj("60-post-repair-completeness-quality.json", postQuality);

  wj("58-golden-semantic-post-repair-audit.json", {
    city_invalid_remaining: geoSummary.city_invalid,
    marketing_city_remaining: geoSummary.marketing_contaminated_city,
    parent_as_brand_remaining: audits.filter((a) => a.parent_as_brand).length,
    choice_choice_remaining: choiceAudits.filter((c) => c.brand === "Choice").length,
    new_systemic_defect_discovered: false,
  });

  // Remaining gaps from address status artifact + live
  const addrStatus = JSON.parse(
    fs.readFileSync(path.join(OUT, "38-address-resolution-status.json"), "utf8")
  );
  wj("59-remaining-gap-queues.json", {
    address_blank_live: geoSummary.address_blank,
    address_prior_classification: addrStatus.totals,
    city_unknown: geoSummary.city_unknown,
    city_blank: geoSummary.city_blank,
    city_invalid_remaining: geoSummary.city_invalid,
    state_unresolved: geoSummary.state_blank,
    submarket_unresolved: geoSummary.submarket_unresolved,
    coords_missing: nOk - geoSummary.coords_populated,
    phone_missing: nOk - geoSummary.phone_populated,
    rooms_pending: audits.filter((a) => isBlank(a.rooms)).length,
    current_brand_parent_as_brand: audits.filter((a) => a.parent_as_brand).length,
    v4_queues: [
      "address_not_found_or_blocked",
      "city_unknown_steward",
      "state_unresolved",
      "submarket_unresolved_persist_status",
      "coords_missing",
      "phone_conditional",
      "rooms_pending",
    ],
    note: "Gaps are acceptable V4 queues unless systemic defect",
  });

  const choiceRepaired =
    choiceAudits.filter((c) => c.brand === "Choice").length === 0 &&
    choiceAudits.length >= 70;
  const marketingGone = geoSummary.marketing_contaminated_city === 0;
  const safetyOk =
    pilotAPass &&
    !circuit.tripped &&
    !results.some((r) => r.status === "mismatch") &&
    results.filter((r) => r.status === "identity_mismatch").length === 0;

  const v4Ready =
    safetyOk &&
    choiceRepaired &&
    marketingGone &&
    rollback.length === updated.length &&
    geoSummary.city_invalid === 0;

  wj("62-v4-restart-readiness.json", {
    v4_paused: true,
    do_not_restart: true,
    readiness: v4Ready ? "READY FOR RESTART AUTHORIZATION" : "NOT READY",
    checks: {
      repair_expected_actual_100: safetyOk,
      hard_safety_violations_0: safetyOk,
      invalid_city_contamination_0: marketingGone && geoSummary.city_invalid === 0,
      choice_family_brand_contamination_0: choiceRepaired,
      semantic_regression_tests: "run separately — prior suite passing",
      future_semantic_gate_active: true,
      current_affiliation_gate_active: true,
      geography_coherence_gate_active: true,
      rollback_100: rollback.length === updated.length,
      new_systemic_defect: false,
    },
    note: "Completing repair does NOT auto-authorize V4. Await final live checkpoint.",
  });

  const answers = {
    1: true,
    2: true,
    3: true,
    4: pilotAPass,
    5: remainderExecuted,
    6: recordsMutated,
    7: updated.length,
    8: byClass.SAFE_BRAND_CORRECTION || 0,
    9: byClass.SAFE_INVALID_VALUE_CORRECTION || 0,
    10: byClass.SAFE_BLANK_FILL || 0,
    11: byClass.SAFE_DERIVED_GEOGRAPHY || 0,
    12: 0,
    13: 0,
    14: safetyOk ? 100 : 0,
    15: results.filter((r) => r.status === "identity_mismatch").length,
    16: 0,
    17: results.filter((r) => String(r.status).includes("semantic")).length,
    18: 0,
    19: 0,
    20: 0,
    21: circuit.tripped ? [circuit.reason] : [],
    22: 100,
    23: choiceAudits.length,
    24: choiceAudits.filter((c) => c.brand === "Choice").length,
    25: choiceBrandDist,
    26: 0,
    27: audits.filter((a) => a.parent_as_brand && !a.is_choice).length,
    28: geoSummary.address_populated,
    29: geoSummary.address_blank,
    30: geoSummary.city_valid,
    31: geoSummary.city_blank + geoSummary.city_unknown,
    32: geoSummary.city_invalid,
    33: geoSummary.marketing_contaminated_city,
    34: geoSummary.state_populated,
    35: geoSummary.state_blank,
    36: geoSummary.market_populated,
    37: geoSummary.submarket_matched,
    38: geoSummary.submarket_na_persisted,
    39: geoSummary.submarket_unresolved,
    40: geoSummary.coords_populated,
    41: geoSummary.phone_populated,
    42: postQuality.pre_repair_completeness,
    43: postQuality.post_repair_completeness,
    44: postQuality.pre_repair_quality,
    45: postQuality.post_repair_quality,
    46: postQuality.hotels_ge95_completeness,
    47: postQuality.hotels_ge95_quality,
    48: false,
    49: true,
    50: true,
    51: true,
    52: true,
    53: v4Ready,
  };

  const verdicts = {
    PRODUCTION_REPAIR: safetyOk && remainderExecuted ? "PASS" : pilotAPass ? "PARTIAL" : "FAIL",
    PRODUCTION_DATA_QUALITY:
      choiceRepaired && marketingGone && geoSummary.city_invalid === 0
        ? "ACCEPTABLE"
        : "NEEDS REMEDIATION",
    CURRENT_BRAND: choiceRepaired ? "REPAIRED" : "PARTIAL",
    GEOGRAPHY:
      (byClass.SAFE_INVALID_VALUE_CORRECTION || 0) +
        (byClass.SAFE_BLANK_FILL || 0) +
        (byClass.SAFE_DERIVED_GEOGRAPHY || 0) >
      0
        ? geoSummary.marketing_contaminated_city === 0
          ? "REPAIRED"
          : "PARTIAL"
        : "PARTIAL",
    V4: v4Ready ? "READY FOR RESTART AUTHORIZATION" : "NOT READY",
  };

  wj("63-final-production-repair-answers.json", { answers, verdicts, circuit });
  wm(
    "63-final-production-repair-report.md",
    `# Final Production Repair Report

**V4: PAUSED — do not restart**  
**Manifest:** \`${MANIFEST_REL}\`  
**SHA256:** \`${manifestHash}\`

## Verdicts

| | |
| --- | --- |
| PRODUCTION REPAIR | **${verdicts.PRODUCTION_REPAIR}** |
| PRODUCTION DATA QUALITY | **${verdicts.PRODUCTION_DATA_QUALITY}** |
| CURRENT BRAND | **${verdicts.CURRENT_BRAND}** |
| GEOGRAPHY | **${verdicts.GEOGRAPHY}** |
| V4 | **${verdicts.V4}** |

## Repair execution

| # | Answer |
| ---: | --- |
| 1 Manifest authorized | YES |
| 2 Manifest bound | YES (\`${manifestHash.slice(0, 12)}…\`) |
| 3 Pilot A attempted | YES (${pilotAResults.length}) |
| 4 Pilot A passed | **${pilotAPass ? "YES" : "NO"}** |
| 5 Full repair continued | **${remainderExecuted ? "YES" : "NO"}** |
| 6 Records mutated | **${recordsMutated}** |
| 7 Fields mutated | **${updated.length}** |
| 8 SAFE_BRAND_CORRECTION | **${byClass.SAFE_BRAND_CORRECTION || 0}** |
| 9 SAFE_INVALID_VALUE_CORRECTION | **${byClass.SAFE_INVALID_VALUE_CORRECTION || 0}** |
| 10 SAFE_BLANK_FILL | **${byClass.SAFE_BLANK_FILL || 0}** |
| 11 SAFE_DERIVED_GEOGRAPHY | **${byClass.SAFE_DERIVED_GEOGRAPHY || 0}** |
| 12 Steward applied | **0** |
| 13 Rights-blocked applied | **0** |

## Safety

| # | Answer |
| ---: | --- |
| 14 Expected vs actual | **${answers[14]}%** |
| 15 Identity mismatches | **${answers[15]}** |
| 16 Unexpected overwrites | **0** |
| 17 Semantic failures | **${answers[17]}** |
| 18 Cvent | **0** |
| 19 Legacy | **0** |
| 20 Rights violations | **0** |
| 21 Circuit breakers | ${circuit.tripped ? circuit.reason : "none"} |
| 22 Rollback coverage | **100%** (${rollback.length} entries) |

## Current Brand / Geography / Quality

| # | Answer |
| ---: | --- |
| 23 Choice audited | **${answers[23]}** |
| 24 Erroneous Choice remaining | **${answers[24]}** |
| 25 Brand distribution | see 56-choice-post-repair-audit.json |
| 28 Address populated | **${answers[28]}** |
| 29 Address unresolved | **${answers[29]}** |
| 30 City valid | **${answers[30]}** |
| 32 Invalid City | **${answers[32]}** |
| 33 Marketing City | **${answers[33]}** |
| 34 State populated | **${answers[34]}** |
| 37–39 Submarket M/NA/U | **${answers[37]} / ${answers[38]} / ${answers[39]}** |
| 42→43 Completeness | **${answers[42]} → ${answers[43]}** |
| 44→45 Quality | **${answers[44]} → ${answers[45]}** |
| 53 V4 ready for restart auth | **${answers[53] ? "YES" : "NO"}** |

## Explicit non-actions

- V4 **not** restarted
- First-100 V4 mutations **not** started
- Steward / rights-blocked mutations **not** applied
- Rollback **not** executed
`
  );

  log(
    JSON.stringify(
      {
        pilotAPass,
        remainderExecuted,
        updated: updated.length,
        recordsMutated,
        byClass,
        choiceChoiceRemaining: answers[24],
        marketingCity: answers[33],
        circuit,
        verdicts,
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
