/**
 * Full CALA shell orchestrator v1 — automated SAFE shell inserts until exhausted.
 *
 * Modes: dry-run | run | resume
 * AUTO_APPLY safe records · AUTO_HOLD weak · STOP_FOR_FOUNDER_REVIEW only on policy/safety faults.
 *
 * Never writes Brand Explorer / Brand Setup / VIC / old Census.
 * Never writes Rooms/Keys, coords, media, owner/operator/dates, Current Brand, Brand Family.
 */
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  resolvePat,
  resolveTargetBase,
} from "./production-census-schema-create.js";
import {
  assertProductionCensusWriteTarget,
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
} from "./production-census-source-of-truth.js";
import { isForbiddenAutopilotField } from "./census-autopilot-field-allowlist.js";
import { normName } from "./census-autopilot-v2/identity-dedupe.js";

import {
  CENSUS_TABLE_ID,
  FORBIDDEN_SHELL,
  MATCH,
  SHELL_PREFLIGHT_CLASS,
  classifyAgainstCensus,
  classifyShellPreflightQuality,
  mexicoBatchSourcePriority,
  loadMasterUniverseCandidates,
  loadHbxCandidates,
  mergeCandidateUniverses,
  listCensusIndex,
  buildShellFields,
  insertBatch,
  COLOMBIA_BATCH_2_PREFLIGHT_FINGERPRINT,
  buildColombiaBatch2SafeAllowlist,
} from "./full-cala-15k-census-shell-insert-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const ORCHESTRATOR_VERSION = "full-cala-15k-shell-orchestrator-v1";
export const ORCHESTRATOR_OBJECTIVE = "full-cala-15k-shell-orchestrator-v1";

export const ORCHESTRATOR_STATUS = Object.freeze({
  RUNNING: "production_census_full_cala_shell_orchestrator_running",
  EXHAUSTED:
    "production_census_full_cala_shell_universe_exhausted_pending_enrichment",
  FOUNDER_STOP:
    "production_census_full_cala_shell_orchestrator_stop_for_founder_review",
  FAILED: "production_census_full_cala_shell_orchestrator_failed",
  DRY_RUN_READY:
    "production_census_full_cala_shell_orchestrator_dry_run_ready",
});

const STATE_DIR = path.join(
  ROOT,
  "data/research-engine-v2/full-cala-15k-shell-orchestrator"
);
const STATE_FILE = path.join(STATE_DIR, "orchestrator-state.json");
const HOLDS_FILE = path.join(STATE_DIR, "holds-ledger.json");
const APPLIED_FILE = path.join(STATE_DIR, "applied-index.json");

const MAX_BATCH = 500;
const LOCKED_TABLE_ID = PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;

const MAJOR_CALA = new Set([
  "Colombia",
  "Costa Rica",
  "Panama",
  "Dominican Republic",
  "Mexico",
  "Puerto Rico",
  "Jamaica",
  "Guatemala",
  "Honduras",
  "Nicaragua",
  "El Salvador",
  "Belize",
  "Cuba",
  "Bahamas",
  "Trinidad and Tobago",
  "Barbados",
  "Aruba",
  "Curaçao",
  "Cayman Islands",
  "Peru",
  "Ecuador",
  "Chile",
  "Argentina",
  "Uruguay",
  "Paraguay",
  "Bolivia",
  "Venezuela",
  "Brazil",
]);

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function writeJson(fp, data) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}
function writeMd(fp, md) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, md.endsWith("\n") ? md : `${md}\n`, "utf8");
}
function readJson(fp, fallback = null) {
  if (!fs.existsSync(fp)) return fallback;
  return JSON.parse(fs.readFileSync(fp, "utf8"));
}

/**
 * Re-list Census until count matches expected, or fail.
 * Allows a short read-after-write lag (documented safe timing window).
 */
async function validateCensusDelta({
  baseId,
  token,
  censusBefore,
  insertsApplied,
  log,
}) {
  const expectedAfter = censusBefore + insertsApplied;
  const maxAttempts = 4;
  let lastCount = null;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    if (attempt > 1) {
      const waitMs = 1500 * attempt;
      log?.(
        `[orchestrator] census delta re-list attempt ${attempt}/${maxAttempts} wait=${waitMs}ms`
      );
      await sleep(waitMs);
    }
    const indexAfter = await listCensusIndex(baseId, token, CENSUS_TABLE_ID);
    lastCount = indexAfter.count;
    if (lastCount === expectedAfter) {
      return { ok: true, censusAfter: lastCount, expectedAfter, attempts: attempt };
    }
  }
  return {
    ok: false,
    censusAfter: lastCount,
    expectedAfter,
    attempts: maxAttempts,
    reason: `census_delta_mismatch:before=${censusBefore}:inserted=${insertsApplied}:after=${lastCount}:expected=${expectedAfter}`,
  };
}
function evidenceFingerprint(c) {
  return crypto
    .createHash("sha1")
    .update(
      [
        c.city || "",
        c.address || "",
        c.website || "",
        c.phone || "",
        c.external_ids?.hbx_code || "",
      ].join("|")
    )
    .digest("hex")
    .slice(0, 16);
}

export function assertLockedCensusTable(tableId) {
  if (String(tableId) !== LOCKED_TABLE_ID) {
    throw new Error(
      `target_table_id_mismatch: got=${tableId} expected=${LOCKED_TABLE_ID}`
    );
  }
}

export function assertNoProtectedShellFields(fields) {
  const hits = [];
  for (const k of Object.keys(fields || {})) {
    if (FORBIDDEN_SHELL.has(k) || isForbiddenAutopilotField(k)) hits.push(k);
    if (
      k === "Current Brand" ||
      k === "Brand Family" ||
      k === "Family / Source Family" ||
      k === "Brand Verified" ||
      k === "Brand Status" ||
      k === "Rooms / Keys"
    ) {
      hits.push(k);
    }
  }
  if (hits.length) {
    throw new Error(`protected_field_proposed:${[...new Set(hits)].join(",")}`);
  }
}

export function buildSafeAllowlistForCountry(
  countryCandidates,
  { maxInserts = MAX_BATCH } = {}
) {
  const sorted = [...countryCandidates].sort((a, b) => {
    const ap = mexicoBatchSourcePriority(a);
    const bp = mexicoBatchSourcePriority(b);
    if (ap !== bp) return ap - bp;
    const aHigh = a.match_class === MATCH.NEW_HIGH ? 0 : 1;
    const bHigh = b.match_class === MATCH.NEW_HIGH ? 0 : 1;
    if (aHigh !== bHigh) return aHigh - bHigh;
    return String(a.property_name).localeCompare(String(b.property_name));
  });

  const seenHbx = new Set();
  const seenName = new Set();
  const planned = [];
  let skippedHbx = 0;
  let skippedName = 0;
  let heldWeak = 0;
  let heldReview = 0;
  let heldOther = 0;

  for (const c of sorted) {
    const pf = classifyShellPreflightQuality(c, { cventOnlyQualityGate: true });
    if (pf.class === SHELL_PREFLIGHT_CLASS.WEAK) {
      heldWeak += 1;
      continue;
    }
    if (pf.class === SHELL_PREFLIGHT_CLASS.REVIEW) {
      heldReview += 1;
      continue;
    }
    if (pf.class !== SHELL_PREFLIGHT_CLASS.SAFE) {
      heldOther += 1;
      continue;
    }
    const hbxCode =
      c.external_ids?.hbx_code != null ? Number(c.external_ids.hbx_code) : null;
    if (hbxCode == null) {
      heldOther += 1;
      continue;
    }
    if (seenHbx.has(hbxCode)) {
      skippedHbx += 1;
      continue;
    }
    const ncKey = `${c.normalized_property_name}|${normName(c.country)}`;
    if (seenName.has(ncKey)) {
      skippedName += 1;
      continue;
    }
    seenHbx.add(hbxCode);
    seenName.add(ncKey);
    planned.push({
      candidate: c,
      candidate_id: c.candidate_id,
      property_name: c.property_name,
      country: c.country,
      city: c.city || null,
      match_class: c.match_class,
      source_type: c.source_type,
      hbx_hotel_code: hbxCode,
      is_cvent:
        c.source_type === "cvent_candidate" ||
        (c.merged_sources || []).includes("cvent_candidate"),
      is_hbx: true,
      has_candidate_brand: Boolean(c.brand_text || c.chain_text),
      preflight_class: pf.class,
      preflight_reason: pf.reason,
      source_priority: mexicoBatchSourcePriority(c),
      evidence_fingerprint: evidenceFingerprint(c),
    });
    if (planned.length >= maxInserts) break;
  }

  return {
    planned,
    skippedHbx,
    skippedName,
    heldWeak,
    heldReview,
    heldOther,
    pool_reviewed: sorted.length,
  };
}

function fingerprintAllowlist(planned) {
  const ids = planned.map((p) => p.candidate_id).join("|");
  return {
    count: planned.length,
    hbx_only: planned.filter((p) => p.is_hbx && !p.is_cvent).length,
    cvent_plus_hbx: planned.filter((p) => p.is_hbx && p.is_cvent).length,
    cvent_only: planned.filter((p) => p.is_cvent && !p.is_hbx).length,
    first_candidate_id: planned[0]?.candidate_id || null,
    sha1: crypto.createHash("sha1").update(ids).digest("hex").slice(0, 20),
    all_have_hbx: planned.every((p) => p.hbx_hotel_code != null),
    all_safe: planned.every(
      (p) => p.preflight_class === SHELL_PREFLIGHT_CLASS.SAFE
    ),
  };
}

function loadState() {
  return (
    readJson(STATE_FILE, null) || {
      version: ORCHESTRATOR_VERSION,
      run_id: null,
      status: null,
      country_batch_numbers: {},
      batches_completed: [],
      shells_added: 0,
      founder_stop_reason: null,
      updated_at: null,
    }
  );
}

function loadHolds() {
  return readJson(HOLDS_FILE, { version: 1, by_candidate_id: {} });
}

function loadApplied() {
  return readJson(APPLIED_FILE, {
    version: 1,
    hbx_codes: [],
    candidate_ids: [],
  });
}

function persistHolds(holds) {
  writeJson(HOLDS_FILE, holds);
}
function persistApplied(applied) {
  writeJson(APPLIED_FILE, applied);
}
function persistState(state) {
  state.updated_at = new Date().toISOString();
  writeJson(STATE_FILE, state);
}

/** Unit-testable Colombia Batch 2 allowlist regression. */
export function regressionValidateColombiaBatch2Allowlist(colombiaPotentialNew) {
  const built = buildColombiaBatch2SafeAllowlist(colombiaPotentialNew);
  const planned = built.planned.slice(
    0,
    COLOMBIA_BATCH_2_PREFLIGHT_FINGERPRINT.expected_insert_count
  );
  const fp = fingerprintAllowlist(planned);
  return {
    ok:
      fp.count === COLOMBIA_BATCH_2_PREFLIGHT_FINGERPRINT.expected_insert_count &&
      fp.hbx_only === COLOMBIA_BATCH_2_PREFLIGHT_FINGERPRINT.hbx_only &&
      fp.cvent_plus_hbx === COLOMBIA_BATCH_2_PREFLIGHT_FINGERPRINT.cvent_plus_hbx &&
      fp.cvent_only === 0 &&
      fp.first_candidate_id ===
        COLOMBIA_BATCH_2_PREFLIGHT_FINGERPRINT.first_candidate_id,
    fingerprint: fp,
    expected: COLOMBIA_BATCH_2_PREFLIGHT_FINGERPRINT,
  };
}

export async function runFullCala15kShellOrchestratorV1(opts = {}) {
  const env = opts.env || process.env;
  const log = opts.log || (() => {});
  const mode = String(opts.mode || "dry-run").toLowerCase();
  const enableWrites = Boolean(
    opts.enableProductionWrites && (mode === "run" || mode === "resume")
  );
  const maxBatches =
    opts.maxBatches != null ? Number(opts.maxBatches) : Infinity;
  const focusCountry =
    opts.focusCountry != null && String(opts.focusCountry).trim()
      ? String(opts.focusCountry).trim()
      : null;
  /** When true, SAFE preflight may auto-apply without HBX code (Core Identity gate). */
  const coreIdentityMode = Boolean(opts.coreIdentityMode);
  const deprioritizeCountries = new Set(
    (opts.deprioritizeCountries || []).map((c) => String(c).trim()).filter(Boolean)
  );
  const preferCountries = new Set(
    (opts.preferCountries || []).map((c) => String(c).trim()).filter(Boolean)
  );
  const generated_at = new Date().toISOString();

  let token;
  let baseId;
  try {
    token = resolvePat();
    const base = resolveTargetBase();
    baseId = base?.target_base_id || base?.baseId || env.AIRTABLE_BASE_ID_ALT;
    assertProductionCensusWriteTarget({
      tableId: CENSUS_TABLE_ID,
      tableName: "Hotel Property Census",
    });
    assertLockedCensusTable(CENSUS_TABLE_ID);
  } catch (err) {
    return founderStop({
      reason: String(err?.message || err).slice(0, 400),
      generated_at,
      enableWrites,
    });
  }

  const state = loadState();
  const holds = loadHolds();
  const applied = loadApplied();
  const appliedHbx = new Set((applied.hbx_codes || []).map(Number));
  const appliedCandidates = new Set(applied.candidate_ids || []);

  if (
    mode === "resume" &&
    state.status === ORCHESTRATOR_STATUS.FOUNDER_STOP &&
    !opts.acknowledgeFounderStop
  ) {
    return founderStop({
      reason:
        state.founder_stop_reason ||
        "prior_founder_stop_requires_acknowledge_flag",
      generated_at,
      enableWrites,
      state,
      shellsAddedThisRun: state.shells_added_this_run || 0,
      batchesCompletedThisRun: (state.batches_this_run || []).length,
      lastSuccessfulBatch: (state.batches_this_run || []).slice(-1)[0] || null,
      lastCensusCount: state.last_census_count || null,
    });
  }

  if (mode === "run" || (mode === "resume" && !state.run_id)) {
    state.run_id = `orch_${new Date().toISOString().replace(/[:.]/g, "-")}`;
    state.status = ORCHESTRATOR_STATUS.RUNNING;
    state.shells_added_this_run = 0;
    state.batches_this_run = [];
    state.founder_stop_reason = null;
  } else if (mode === "resume") {
    state.status = ORCHESTRATOR_STATUS.RUNNING;
    state.shells_added_this_run = state.shells_added_this_run || 0;
    state.batches_this_run = state.batches_this_run || [];
  } else {
    state.run_id = `dry_${new Date().toISOString().replace(/[:.]/g, "-")}`;
    state.shells_added_this_run = 0;
    state.batches_this_run = [];
  }

  // Seed country batch numbers from known completed production
  state.country_batch_numbers = {
    "Dominican Republic": Math.max(state.country_batch_numbers?.["Dominican Republic"] || 0, 1),
    "Costa Rica": Math.max(state.country_batch_numbers?.["Costa Rica"] || 0, 1),
    Panama: Math.max(state.country_batch_numbers?.Panama || 0, 1),
    Colombia: Math.max(state.country_batch_numbers?.Colombia || 0, 2),
    Mexico: Math.max(state.country_batch_numbers?.Mexico || 0, 3),
    ...(state.country_batch_numbers || {}),
  };

  const runDir = path.join(
    ROOT,
    "reports/research-engine-v2/full-cala-shell-orchestrator",
    state.run_id
  );
  fs.mkdirSync(runDir, { recursive: true });
  persistState(state);

  let batchesCompletedThisRun = 0;
  let shellsAddedThisRun = 0;
  let holdsCreatedThisRun = 0;
  let lastSuccessfulBatch = null;
  let stopReason = null;
  let orchestratorStatus = ORCHESTRATOR_STATUS.RUNNING;
  const countriesProcessed = new Set();
  const errors = [];
  let lastCensusCount = null;

  log(`[orchestrator] loading candidates…`);
  const universe = loadMasterUniverseCandidates();
  const hbx = loadHbxCandidates();
  const { merged } = mergeCandidateUniverses(universe, hbx);

  while (batchesCompletedThisRun < maxBatches) {
    log(`[orchestrator] indexing Census (read)…`);
    let index;
    try {
      assertLockedCensusTable(CENSUS_TABLE_ID);
      index = await listCensusIndex(baseId, token, CENSUS_TABLE_ID);
    } catch (err) {
      return founderStop({
        reason: `census_index_failed:${String(err?.message || err).slice(0, 300)}`,
        generated_at,
        enableWrites,
        state,
        runDir,
        shellsAddedThisRun,
        batchesCompletedThisRun,
        lastSuccessfulBatch,
        countriesProcessed,
        errors,
        lastCensusCount,
      });
    }
    lastCensusCount = index.count;
    for (const code of index.byHbx?.keys?.() || []) appliedHbx.add(Number(code));

    const classified = [];
    for (const c of merged) {
      c.merged_sources = c.merged_sources || [c.source_type];
      const cls = classifyAgainstCensus(c, index);
      classified.push({ ...c, ...cls });
    }

    const insertableClasses = new Set([MATCH.NEW_HIGH, MATCH.NEW_MEDIUM]);
    const potentialNew = classified.filter((c) =>
      insertableClasses.has(c.match_class)
    );

    for (const c of potentialNew) {
      const pf = classifyShellPreflightQuality(c, { cventOnlyQualityGate: true });
      const ev = evidenceFingerprint(c);
      const prior = holds.by_candidate_id[c.candidate_id];
      if (
        pf.class === SHELL_PREFLIGHT_CLASS.WEAK ||
        pf.class === SHELL_PREFLIGHT_CLASS.REVIEW ||
        pf.class === SHELL_PREFLIGHT_CLASS.NON_HOTEL ||
        pf.class === SHELL_PREFLIGHT_CLASS.INSUFFICIENT
      ) {
        if (!prior || prior.evidence_fingerprint !== ev) {
          holds.by_candidate_id[c.candidate_id] = {
            country: c.country,
            class: pf.class,
            reason: pf.reason,
            evidence_fingerprint: ev,
            held_at: new Date().toISOString(),
          };
          holdsCreatedThisRun += 1;
        }
      }
    }
    persistHolds(holds);

    const eligible = potentialNew.filter((c) => {
      if (appliedCandidates.has(c.candidate_id)) return false;
      const hbxCode =
        c.external_ids?.hbx_code != null ? Number(c.external_ids.hbx_code) : null;
      if (hbxCode != null && appliedHbx.has(hbxCode)) return false;
      if (hbxCode != null && index.byHbx?.has?.(hbxCode)) return false;
      const pf = classifyShellPreflightQuality(c, { cventOnlyQualityGate: true });
      if (pf.class !== SHELL_PREFLIGHT_CLASS.SAFE) return false;
      // Default bar: SAFE + HBX. Core Identity mode also allows strong Cvent-only SAFE.
      if (hbxCode != null) return true;
      if (!coreIdentityMode) return false;
      return (
        pf.reason === "cvent_only_hotel_like_strong_identity" ||
        pf.reason === "multi_or_independent_source" ||
        pf.reason === "hbx_backed_with_city"
      );
    });

    const byCountry = {};
    for (const c of eligible) {
      if (focusCountry && String(c.country || "").trim() !== focusCountry) {
        continue;
      }
      const k = c.country || "UNK";
      byCountry[k] = byCountry[k] || [];
      byCountry[k].push(c);
    }
    const ranked = Object.entries(byCountry)
      .filter(([country, rows]) => {
        if (coreIdentityMode) return rows.length > 0;
        if (!MAJOR_CALA.has(country) && rows.length < 25) return false;
        return rows.length > 0;
      })
      .map(([country, rows]) => ({
        country,
        safe_count: rows.length,
        hbx_backed: rows.filter((r) => r.external_ids?.hbx_code).length,
        next_batch_number: (state.country_batch_numbers[country] || 0) + 1,
      }))
      .sort((a, b) => {
        const aPref = preferCountries.has(a.country) ? 1 : 0;
        const bPref = preferCountries.has(b.country) ? 1 : 0;
        if (bPref !== aPref) return bPref - aPref;
        const aDep = deprioritizeCountries.has(a.country) ? 1 : 0;
        const bDep = deprioritizeCountries.has(b.country) ? 1 : 0;
        if (aDep !== bDep) return aDep - bDep;
        if (b.safe_count !== a.safe_count) return b.safe_count - a.safe_count;
        return b.hbx_backed - a.hbx_backed;
      });

    state.last_ranked_pools = ranked.slice(0, 20);
    state.last_census_count = lastCensusCount;
    persistState(state);

    if (!ranked.length) {
      orchestratorStatus = ORCHESTRATOR_STATUS.EXHAUSTED;
      stopReason = "no_safe_hbx_backed_candidates_remaining";
      break;
    }

    const selected = ranked[0];
    const country = selected.country;
    const batchNumber = (state.country_batch_numbers[country] || 0) + 1;
    const batchLabel = `${country} Batch ${batchNumber}`;
    const batchId = `${state.run_id}__${country.replace(/\s+/g, "_")}__${batchNumber}`;

    if ((state.batches_completed || []).some((b) => b.batch_id === batchId)) {
      state.country_batch_numbers[country] = batchNumber;
      persistState(state);
      continue;
    }

    const built = buildSafeAllowlistForCountry(byCountry[country], {
      maxInserts: MAX_BATCH,
    });
    const allowlist = built.planned;
    if (!allowlist.length) {
      state.country_batch_numbers[country] = batchNumber;
      persistState(state);
      if (ranked.length <= 1) {
        orchestratorStatus = ORCHESTRATOR_STATUS.EXHAUSTED;
        stopReason = "allowlists_empty_after_safe_filter";
        break;
      }
      continue;
    }

    const fp = fingerprintAllowlist(allowlist);
    const hbxInvariantOk = fp.all_have_hbx && fp.all_safe && fp.cvent_only === 0;
    const coreIdentityInvariantOk =
      coreIdentityMode &&
      fp.all_safe &&
      allowlist.every(
        (p) =>
          p.hbx_hotel_code != null ||
          p.candidate?.city ||
          p.candidate?.property_name
      );
    if (!hbxInvariantOk && !coreIdentityInvariantOk) {
      return founderStop({
        reason: "allowlist_failed_safe_hbx_invariants",
        generated_at,
        enableWrites,
        state,
        runDir,
        shellsAddedThisRun,
        batchesCompletedThisRun,
        lastSuccessfulBatch,
        countriesProcessed,
        errors,
        lastCensusCount,
        extra: { fingerprint: fp, country, batchLabel, coreIdentityMode },
      });
    }

    const prepared = [];
    for (const row of allowlist) {
      try {
        const fieldsBuilt = buildShellFields(row.candidate, [], {
          countryBatchLabel: batchLabel,
        });
        if (!fieldsBuilt.validation.pass) {
          errors.push({
            candidate_id: row.candidate_id,
            error: "validation_failed",
          });
          continue;
        }
        assertNoProtectedShellFields(fieldsBuilt.fields);
        if (
          fieldsBuilt.fields["Current Brand"] != null ||
          fieldsBuilt.fields["Brand Family"] != null
        ) {
          return founderStop({
            reason: "current_brand_or_brand_family_proposed",
            generated_at,
            enableWrites,
            state,
            runDir,
            shellsAddedThisRun,
            batchesCompletedThisRun,
            lastSuccessfulBatch,
            countriesProcessed,
            errors,
            lastCensusCount,
          });
        }
        if (!fieldsBuilt.fields["HBX Hotel Code"]) {
          if (!coreIdentityMode) {
            errors.push({
              candidate_id: row.candidate_id,
              error: "missing_hbx_field",
            });
            continue;
          }
        } else if (index.byHbx.has(Number(row.hbx_hotel_code))) {
          appliedHbx.add(Number(row.hbx_hotel_code));
          continue;
        }
        prepared.push({ ...row, fields: fieldsBuilt.fields });
      } catch (err) {
        const msg = String(err?.message || err);
        if (/protected_field|target_table|Current Brand|Brand Family/i.test(msg)) {
          return founderStop({
            reason: msg.slice(0, 400),
            generated_at,
            enableWrites,
            state,
            runDir,
            shellsAddedThisRun,
            batchesCompletedThisRun,
            lastSuccessfulBatch,
            countriesProcessed,
            errors,
            lastCensusCount,
          });
        }
        errors.push({
          candidate_id: row.candidate_id,
          error: msg.slice(0, 200),
        });
      }
    }

    if (!prepared.length) {
      state.country_batch_numbers[country] = batchNumber;
      persistState(state);
      continue;
    }

    const censusBefore = index.count;
    const manifest = {
      run_id: state.run_id,
      batch_id: batchId,
      country,
      batch_number: batchNumber,
      batch_label: batchLabel,
      production_table_id: CENSUS_TABLE_ID,
      mode,
      enable_writes: enableWrites,
      allowlist_fingerprint: fingerprintAllowlist(prepared),
      candidates_considered: built.pool_reviewed,
      approved_count: prepared.length,
      source_mix: {
        hbx_only: prepared.filter((p) => p.is_hbx && !p.is_cvent).length,
        cvent_plus_hbx: prepared.filter((p) => p.is_hbx && p.is_cvent).length,
        cvent_only: prepared.filter((p) => p.is_cvent && !p.is_hbx).length,
      },
      held_weak_in_pool: built.heldWeak,
      held_review_in_pool: built.heldReview,
      census_before: censusBefore,
      generated_at: new Date().toISOString(),
    };

    writeJson(path.join(runDir, `${batchId}-allowlist.json`), {
      ...manifest,
      records: prepared.map((p) => ({
        candidate_id: p.candidate_id,
        property_name: p.property_name,
        hbx_hotel_code: p.hbx_hotel_code,
        source_type: p.source_type,
        city: p.city,
      })),
    });

    let inserts_applied = 0;
    let insert_errors = [];
    let created_ids = [];

    if (enableWrites) {
      log(
        `[orchestrator] APPLY ${batchLabel} size=${prepared.length} census_before=${censusBefore}`
      );
      const batchSize = 100;
      for (let i = 0; i < prepared.length; i += batchSize) {
        const chunk = prepared.slice(i, i + batchSize);
        const result = await insertBatch(chunk, {
          baseId,
          token,
          tableId: CENSUS_TABLE_ID,
          log,
        });
        inserts_applied += result.inserted;
        insert_errors.push(...result.errors);
        created_ids.push(...result.createdIds.filter(Boolean));
        if (result.errors.length >= 10) {
          return founderStop({
            reason: "significant_production_api_write_failure",
            generated_at,
            enableWrites,
            state,
            runDir,
            shellsAddedThisRun,
            batchesCompletedThisRun,
            lastSuccessfulBatch,
            countriesProcessed,
            errors: [...errors, ...insert_errors],
            lastCensusCount,
            extra: { batchId, inserts_applied },
          });
        }
      }

      const delta = await validateCensusDelta({
        baseId,
        token,
        censusBefore,
        insertsApplied: inserts_applied,
        log,
      });
      if (!delta.ok) {
        return founderStop({
          reason: delta.reason,
          generated_at,
          enableWrites,
          state,
          runDir,
          shellsAddedThisRun: shellsAddedThisRun + inserts_applied,
          batchesCompletedThisRun,
          lastSuccessfulBatch,
          countriesProcessed,
          errors: [...errors, ...insert_errors],
          lastCensusCount: delta.censusAfter,
          extra: { batchId, delta },
        });
      }
      const censusAfter = delta.censusAfter;
      lastCensusCount = censusAfter;
      manifest.census_after = censusAfter;
      manifest.inserts_applied = inserts_applied;
      manifest.insert_errors = insert_errors.slice(0, 20);
      manifest.created_ids_sample = created_ids.slice(0, 20);
      manifest.current_brand_writes = 0;
      manifest.brand_family_writes = 0;
      manifest.forbidden_field_writes = 0;
      manifest.validation = "pass";

      for (const p of prepared) {
        appliedHbx.add(Number(p.hbx_hotel_code));
        appliedCandidates.add(p.candidate_id);
      }
      applied.hbx_codes = [...appliedHbx];
      applied.candidate_ids = [...appliedCandidates];
      persistApplied(applied);

      shellsAddedThisRun += inserts_applied;
      batchesCompletedThisRun += 1;
      countriesProcessed.add(country);
      lastSuccessfulBatch = {
        batch_id: batchId,
        country,
        batch_label: batchLabel,
        inserts_applied,
        census_before: censusBefore,
        census_after: censusAfter,
      };
      state.country_batch_numbers[country] = batchNumber;
      state.batches_completed = [
        ...(state.batches_completed || []),
        lastSuccessfulBatch,
      ];
      state.batches_this_run = [
        ...(state.batches_this_run || []),
        lastSuccessfulBatch,
      ];
      state.shells_added = (state.shells_added || 0) + inserts_applied;
      state.shells_added_this_run = shellsAddedThisRun;
      persistState(state);
    } else {
      manifest.inserts_applied = 0;
      manifest.dry_run_would_insert = prepared.length;
      manifest.validation = "dry_run";
      lastSuccessfulBatch = {
        batch_id: batchId,
        country,
        batch_label: batchLabel,
        would_insert: prepared.length,
        census_before: censusBefore,
        fingerprint: fp,
        source_mix: manifest.source_mix,
      };
      batchesCompletedThisRun += 1;
      countriesProcessed.add(country);
      lastCensusCount = censusBefore;
      writeJson(path.join(runDir, `${batchId}-manifest.json`), manifest);
      orchestratorStatus = ORCHESTRATOR_STATUS.DRY_RUN_READY;
      stopReason = "dry_run_first_batch_planned";
      break;
    }

    writeJson(path.join(runDir, `${batchId}-manifest.json`), manifest);
    log(
      `[orchestrator] completed ${batchLabel} inserted=${inserts_applied} census=${lastCensusCount}`
    );
  }

  if (!stopReason && orchestratorStatus === ORCHESTRATOR_STATUS.RUNNING) {
    orchestratorStatus = ORCHESTRATOR_STATUS.EXHAUSTED;
    stopReason = "loop_complete_no_more_safe_batches";
  }

  state.status = orchestratorStatus;
  state.founder_stop_reason =
    orchestratorStatus === ORCHESTRATOR_STATUS.FOUNDER_STOP ? stopReason : null;
  persistState(state);

  const finalReport = buildFinalReport({
    orchestratorStatus,
    stopReason,
    founderDecisionRequired: false,
    founderDecision: null,
    generated_at,
    enableWrites,
    mode,
    state,
    lastCensusCount,
    shellsAddedThisRun,
    batchesCompletedThisRun,
    countriesProcessed,
    holdsCreatedThisRun,
    holds,
    errors,
    lastSuccessfulBatch,
    runDir,
  });
  writeJson(path.join(runDir, "orchestrator-final.json"), finalReport);
  writeJson(
    path.join(
      ROOT,
      "reports/research-engine-v2/full-cala-15k-shell-orchestrator-final.json"
    ),
    finalReport
  );
  writeMd(
    path.join(
      ROOT,
      "reports/research-engine-v2/full-cala-15k-shell-orchestrator-final.md"
    ),
    renderOrchestratorFinalMd(finalReport)
  );
  if (orchestratorStatus === ORCHESTRATOR_STATUS.EXHAUSTED) {
    writeMd(
      path.join(
        ROOT,
        "docs/data-intelligence/full-cala-15k-shell-universe-exhausted.md"
      ),
      renderOrchestratorFinalMd(finalReport)
    );
  }

  log(
    `[orchestrator] STATUS=${orchestratorStatus} STOP=${stopReason} shells_added=${shellsAddedThisRun} batches=${batchesCompletedThisRun}`
  );
  return finalReport;
}

function founderStop(ctx) {
  const state = ctx.state || loadState();
  state.status = ORCHESTRATOR_STATUS.FOUNDER_STOP;
  state.founder_stop_reason = ctx.reason;
  persistState(state);
  const report = buildFinalReport({
    orchestratorStatus: ORCHESTRATOR_STATUS.FOUNDER_STOP,
    stopReason: ctx.reason,
    founderDecisionRequired: true,
    founderDecision: ctx.reason,
    generated_at: ctx.generated_at,
    enableWrites: ctx.enableWrites,
    mode: "run",
    state,
    lastCensusCount: ctx.lastCensusCount,
    shellsAddedThisRun: ctx.shellsAddedThisRun || 0,
    batchesCompletedThisRun: ctx.batchesCompletedThisRun || 0,
    countriesProcessed: ctx.countriesProcessed || new Set(),
    holdsCreatedThisRun: 0,
    holds: loadHolds(),
    errors: ctx.errors || [],
    lastSuccessfulBatch: ctx.lastSuccessfulBatch,
    runDir: ctx.runDir,
    extra: ctx.extra,
  });
  const out =
    ctx.runDir ||
    path.join(ROOT, "reports/research-engine-v2/full-cala-shell-orchestrator");
  fs.mkdirSync(out, { recursive: true });
  writeJson(path.join(out, "orchestrator-final.json"), report);
  writeJson(
    path.join(
      ROOT,
      "reports/research-engine-v2/full-cala-15k-shell-orchestrator-final.json"
    ),
    report
  );
  return report;
}

function buildFinalReport(ctx) {
  const holdsByCountry = {};
  for (const h of Object.values(ctx.holds?.by_candidate_id || {})) {
    const c = h.country || "UNK";
    holdsByCountry[c] = holdsByCountry[c] || {
      weak: 0,
      review: 0,
      other: 0,
      total: 0,
    };
    holdsByCountry[c].total += 1;
    if (h.class === SHELL_PREFLIGHT_CLASS.WEAK) holdsByCountry[c].weak += 1;
    else if (h.class === SHELL_PREFLIGHT_CLASS.REVIEW)
      holdsByCountry[c].review += 1;
    else holdsByCountry[c].other += 1;
  }
  const nextUnresolved =
    Object.entries(holdsByCountry).sort((a, b) => b[1].total - a[1].total)[0]?.[0] ||
    null;

  return {
    ok: ctx.orchestratorStatus !== ORCHESTRATOR_STATUS.FAILED,
    ORCHESTRATOR_STATUS: ctx.orchestratorStatus,
    STOP_REASON: ctx.stopReason,
    FINAL_CENSUS_COUNT: ctx.lastCensusCount,
    SHELLS_ADDED_THIS_RUN: ctx.shellsAddedThisRun,
    BATCHES_COMPLETED_THIS_RUN: ctx.batchesCompletedThisRun,
    COUNTRIES_PROCESSED: [...(ctx.countriesProcessed || [])],
    HOLDS_CREATED: ctx.holdsCreatedThisRun,
    HOLDS_BY_COUNTRY: holdsByCountry,
    ERRORS: (ctx.errors || []).slice(0, 50),
    LAST_SUCCESSFUL_BATCH: ctx.lastSuccessfulBatch,
    NEXT_SAFE_POOL: (ctx.state?.last_ranked_pools || [])[0] || null,
    RANKED_SAFE_POOLS: ctx.state?.last_ranked_pools || [],
    NEXT_UNRESOLVED_POOL: nextUnresolved,
    FOUNDER_DECISION_REQUIRED: ctx.founderDecisionRequired ? "YES" : "NO",
    FOUNDER_DECISION: ctx.founderDecision || null,
    mode: ctx.mode,
    production_writes: Boolean(ctx.enableWrites),
    production_table_id: CENSUS_TABLE_ID,
    run_id: ctx.state?.run_id,
    checkpoint_path:
      "data/research-engine-v2/full-cala-15k-shell-orchestrator/",
    run_dir: ctx.runDir
      ? path.relative(ROOT, ctx.runDir).replace(/\\/g, "/")
      : null,
    production_command:
      "ALLOW_CENSUS_AUTOPILOT_APPLY=1 CONFIRM_WRITE_TO_PRODUCTION_CENSUS=1 CONFIRM_NO_BRAND_EXPLORER_WRITES=1 CONFIRM_NO_BRAND_SETUP_WRITES=1 ENABLE_FULL_CALA_15K_CENSUS_SHELL=1 ENABLE_CENSUS_SHELL_INSERTS=1 ENABLE_CURRENT_BRAND_WRITES=0 ENABLE_BRAND_FAMILY_WRITES=0 ENABLE_ROOMS_WRITES=0 npm run census:full-cala-15k-shell-orchestrator -- --mode run --enable-production-writes",
    resume_command:
      "ALLOW_CENSUS_AUTOPILOT_APPLY=1 CONFIRM_WRITE_TO_PRODUCTION_CENSUS=1 CONFIRM_NO_BRAND_EXPLORER_WRITES=1 CONFIRM_NO_BRAND_SETUP_WRITES=1 ENABLE_FULL_CALA_15K_CENSUS_SHELL=1 ENABLE_CENSUS_SHELL_INSERTS=1 ENABLE_CURRENT_BRAND_WRITES=0 ENABLE_BRAND_FAMILY_WRITES=0 ENABLE_ROOMS_WRITES=0 npm run census:full-cala-15k-shell-orchestrator -- --mode resume --enable-production-writes",
    dry_run_command:
      "npm run census:full-cala-15k-shell-orchestrator -- --mode dry-run",
    generated_at: ctx.generated_at,
    extra: ctx.extra || null,
  };
}

function renderOrchestratorFinalMd(r) {
  return `# Full CALA Shell Orchestrator — Final

**ORCHESTRATOR_STATUS:** \`${r.ORCHESTRATOR_STATUS}\`  
**STOP_REASON:** \`${r.STOP_REASON}\`  
**FOUNDER_DECISION_REQUIRED:** **${r.FOUNDER_DECISION_REQUIRED}**  
${r.FOUNDER_DECISION ? `**Decision:** ${r.FOUNDER_DECISION}` : ""}

## Results
- Final Census count: **${r.FINAL_CENSUS_COUNT}**
- Shells added this run: **${r.SHELLS_ADDED_THIS_RUN}**
- Batches completed: **${r.BATCHES_COMPLETED_THIS_RUN}**
- Countries processed: ${(r.COUNTRIES_PROCESSED || []).join(", ") || "—"}
- Holds created/updated: **${r.HOLDS_CREATED}**
- Production table: \`${r.production_table_id}\`
- Run ID: \`${r.run_id}\`

## Last successful batch
\`\`\`json
${JSON.stringify(r.LAST_SUCCESSFUL_BATCH || {}, null, 2)}
\`\`\`

## Next unresolved pool
**${r.NEXT_UNRESOLVED_POOL || "—"}**

## Checkpoint
\`${r.checkpoint_path}\`
`;
}
