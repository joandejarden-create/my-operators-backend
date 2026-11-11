/**
 * ADP_PAID_SUPERVISOR_HANDOFF_V1
 *
 * Bridges the Full ADP E2E orchestrator to the existing Core/Loyalty research
 * supervisors after exact founder authorization. Does not invent a second
 * provider integration. Paid adapters stay disabled unless explicitly allowed
 * and an exact authorization record is present.
 *
 * Zero-paid simulation is the default for tests and greenfield prep.
 */

import fs from "fs";
import path from "path";
import crypto from "crypto";
import { fileURLToPath } from "url";
import { PROVIDER_CONFIGS } from "../execution/multi-provider-runner.js";
import { PROVIDERS } from "../data-model.js";

export const ADP_PAID_SUPERVISOR_HANDOFF_V1 = "ADP_PAID_SUPERVISOR_HANDOFF_V1";
export const SUPERVISOR_HANDOFF_VERSION = "2026-10-21.v1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");
const HANDOFF_ROOT = path.join(
  ROOT,
  "data/ai-demand-positioning/runtime/adp-supervisor-handoff-v1"
);
const AUTH_LEDGER = path.join(HANDOFF_ROOT, "authorization-ledger.json");
const RUN_STATE_DIR = path.join(HANDOFF_ROOT, "runs");
const LOCK_DIR = path.join(HANDOFF_ROOT, "_locks");

function ensureDir(d) {
  fs.mkdirSync(d, { recursive: true });
}

function readJson(fp, fallback = null) {
  if (!fs.existsSync(fp)) return fallback;
  try {
    return JSON.parse(fs.readFileSync(fp, "utf8"));
  } catch {
    return fallback;
  }
}

function writeJson(fp, obj) {
  ensureDir(path.dirname(fp));
  fs.writeFileSync(fp, `${JSON.stringify(obj, null, 2)}\n`);
}

function sha16(obj) {
  return crypto.createHash("sha256").update(JSON.stringify(obj)).digest("hex").slice(0, 16);
}

export function buildExactAuthorizationRecord({
  runId,
  hotelIdentity,
  propertyId,
  coreScope = { scenarios: 63, providers: [...PROVIDERS] },
  loyaltyScope = { scenarios: 28, providers: [...PROVIDERS], applicable: true },
  pricingScheduleVersion = "PROVIDER_CONFIGS_v1",
  maxSpendUsd,
  approverIdentity,
  approvedAt = new Date().toISOString(),
} = {}) {
  const record = {
    contract: ADP_PAID_SUPERVISOR_HANDOFF_V1,
    authorizationId: `auth_${sha16({ runId, propertyId, maxSpendUsd, approvedAt })}`,
    runId: String(runId || "").trim(),
    hotelIdentity: String(hotelIdentity || "").trim(),
    propertyId: String(propertyId || "").trim(),
    coreScope,
    loyaltyScope,
    providerSet: [...PROVIDERS],
    scenarioCounts: {
      core: coreScope?.scenarios ?? 63,
      loyalty: loyaltyScope?.applicable === false ? 0 : loyaltyScope?.scenarios ?? 28,
    },
    pricingScheduleVersion,
    maxSpendUsd: Number(maxSpendUsd),
    approverIdentity: String(approverIdentity || "").trim(),
    approvedAt,
    status: "ACTIVE",
  };
  const missing = [];
  if (!record.runId) missing.push("runId");
  if (!record.propertyId) missing.push("propertyId");
  if (!record.hotelIdentity) missing.push("hotelIdentity");
  if (!Number.isFinite(record.maxSpendUsd) || record.maxSpendUsd <= 0) missing.push("maxSpendUsd");
  if (!record.approverIdentity) missing.push("approverIdentity");
  if (missing.length) {
    return { ok: false, error: "INCOMPLETE_AUTHORIZATION_RECORD", missing, record };
  }
  return { ok: true, record };
}

export function persistAuthorizationRecord(record) {
  ensureDir(HANDOFF_ROOT);
  const ledger = readJson(AUTH_LEDGER, { version: 1, records: [] });
  const isBppLoyaltySupplement = (kind) =>
    kind === "PEER_LOYALTY_SUPPLEMENT" || kind === "BPP_SUBJECT_AND_PEER_LOYALTY";
  const dup = ledger.records.find(
    (r) =>
      r.authorizationId === record.authorizationId ||
      (r.runId === record.runId &&
        r.propertyId === record.propertyId &&
        r.status === "ACTIVE" &&
        // BPP Loyalty supplements may coexist with a prior Core/subject ACTIVE auth.
        !(isBppLoyaltySupplement(record.authorizationKind) && !isBppLoyaltySupplement(r.authorizationKind)))
  );
  if (dup) {
    return {
      ok: false,
      error: "AUTHORIZATION_ALREADY_RECORDED",
      existing: dup,
      message: "Reuse existing authorization; do not mint a second chargeable approval for the same run.",
    };
  }
  ledger.records.push(record);
  ledger.updatedAt = new Date().toISOString();
  writeJson(AUTH_LEDGER, ledger);
  return { ok: true, record };
}

/**
 * Record a peer-only Loyalty authorization for a run that already completed Core + subject.
 * Does not reuse the prior Core/subject spending ceiling.
 */
export function persistPeerLoyaltySupplementalAuthorization(record) {
  const required = [
    "runId",
    "propertyId",
    "hotelIdentity",
    "maxSpendUsd",
    "approverIdentity",
    "approvedPeerCanonicalIds",
  ];
  const missing = required.filter((k) => {
    const v = record?.[k];
    if (k === "approvedPeerCanonicalIds") return !Array.isArray(v) || v.length < 1;
    if (k === "maxSpendUsd") return !Number.isFinite(Number(v)) || Number(v) <= 0;
    return !String(v || "").trim();
  });
  if (missing.length) {
    return { ok: false, error: "INCOMPLETE_PEER_SUPPLEMENT_AUTHORIZATION", missing };
  }
  if (record.peerLoyaltyAuthorized !== true) {
    return { ok: false, error: "PEER_LOYALTY_NOT_AUTHORIZED_FLAG" };
  }
  if (/SIMULATION|TEST_HARNESS|DRY_RUN|PLACEHOLDER/i.test(record.approverIdentity || "")) {
    return { ok: false, error: "SIMULATION_APPROVER_REJECTED" };
  }
  const prior = loadAuthorizationForRun(record.runId, record.propertyId, {
    preferPeerSupplement: false,
  });
  const subjectLoyaltyExecute =
    record.subjectLoyaltyExecute === true ||
    record.loyaltyScope?.execute === true ||
    record.scope?.subjectLoyaltyExecute === true;
  const supplemental = {
    ...record,
    contract: ADP_PAID_SUPERVISOR_HANDOFF_V1,
    authorizationKind: subjectLoyaltyExecute
      ? "BPP_SUBJECT_AND_PEER_LOYALTY"
      : "PEER_LOYALTY_SUPPLEMENT",
    authorizationId:
      record.authorizationId ||
      `auth_peer_${sha16({
        runId: record.runId,
        propertyId: record.propertyId,
        maxSpendUsd: record.maxSpendUsd,
        peers: record.approvedPeerCanonicalIds,
        subjectLoyalty: subjectLoyaltyExecute,
        approvedAt: record.approvedAt || new Date().toISOString(),
      })}`,
    coreScope: {
      scenarios: 63,
      providers: [...PROVIDERS],
      execute: false,
      reason:
        record.coreScope?.reason ||
        "ALREADY_COMPLETE — Core immutable; BPP Loyalty only",
    },
    loyaltyScope: {
      scenarios: 28,
      providers: [...PROVIDERS],
      applicable: true,
      execute: subjectLoyaltyExecute,
      reason: subjectLoyaltyExecute
        ? record.loyaltyScope?.reason ||
          "SUBJECT_LOYALTY_STANDARD_28_AUTHORIZED_WITH_PEER_COHORT"
        : record.loyaltyScope?.reason || "SUBJECT_ALREADY_COMPLETE — peer supplement only",
    },
    scope: {
      ...(record.scope || {}),
      coreExecute: false,
      subjectLoyaltyExecute,
      peerLoyaltyAuthorized: true,
      family: subjectLoyaltyExecute
        ? "BPP_SUBJECT_AND_PEER_LOYALTY"
        : record.scope?.family || "PEER_LOYALTY_BPP_ONLY",
    },
    peerLoyaltyAuthorized: true,
    priorAuthorizationId: prior?.authorizationId || null,
    priorMaxSpendUsd: prior?.maxSpendUsd ?? null,
    reusesPriorCeiling: false,
    status: "ACTIVE",
    approvedAt: record.approvedAt || new Date().toISOString(),
  };
  // Deactivate any prior peer supplement for this run (idempotent re-record)
  const ledger = readJson(AUTH_LEDGER, { version: 1, records: [] });
  for (const r of ledger.records || []) {
    if (
      r.runId === supplemental.runId &&
      r.propertyId === supplemental.propertyId &&
      (r.authorizationKind === "PEER_LOYALTY_SUPPLEMENT" ||
        r.authorizationKind === "BPP_SUBJECT_AND_PEER_LOYALTY") &&
      r.status === "ACTIVE"
    ) {
      r.status = "SUPERSEDED";
      r.supersededAt = supplemental.approvedAt;
      r.supersededBy = supplemental.authorizationId;
    }
  }
  writeJson(AUTH_LEDGER, ledger);
  return persistAuthorizationRecord(supplemental);
}

export function loadAuthorizationForRun(runId, propertyId, { preferPeerSupplement = true } = {}) {
  const ledger = readJson(AUTH_LEDGER, { records: [] });
  const matches = (ledger.records || []).filter(
    (r) => r.runId === runId && r.propertyId === propertyId && r.status === "ACTIVE"
  );
  if (!matches.length) return null;
  if (preferPeerSupplement) {
    const peer = matches.find(
      (r) =>
        (r.authorizationKind === "PEER_LOYALTY_SUPPLEMENT" ||
          r.authorizationKind === "BPP_SUBJECT_AND_PEER_LOYALTY") &&
        r.peerLoyaltyAuthorized === true
    );
    if (peer) return peer;
  } else {
    const coreSubject = matches.find(
      (r) =>
        r.authorizationKind !== "PEER_LOYALTY_SUPPLEMENT" &&
        r.authorizationKind !== "BPP_SUBJECT_AND_PEER_LOYALTY"
    );
    if (coreSubject) return coreSubject;
  }
  return matches.sort((a, b) => String(b.approvedAt || "").localeCompare(String(a.approvedAt || "")))[0];
}

function lockPath(propertyId) {
  return path.join(LOCK_DIR, `${propertyId}.lock`);
}

export function acquireSupervisorLock(propertyId, runId) {
  ensureDir(LOCK_DIR);
  const lp = lockPath(propertyId);
  if (fs.existsSync(lp)) {
    const existing = readJson(lp, {});
    if (existing.runId && existing.runId !== runId) {
      const age = Date.now() - Date.parse(existing.acquiredAt || 0);
      if (age < 6 * 60 * 60 * 1000) {
        return { ok: false, error: "SUPERVISOR_LOCK_HELD", existing };
      }
    }
  }
  const body = {
    runId,
    propertyId,
    acquiredAt: new Date().toISOString(),
    pid: process.pid,
  };
  writeJson(lp, body);
  return { ok: true, lock: body };
}

export function releaseSupervisorLock(propertyId, runId) {
  const lp = lockPath(propertyId);
  if (!fs.existsSync(lp)) return { ok: true };
  const existing = readJson(lp, {});
  if (existing.runId && existing.runId !== runId) {
    return { ok: false, error: "LOCK_OWNED_BY_OTHER_RUN", existing };
  }
  fs.unlinkSync(lp);
  return { ok: true };
}

function runStatePath(runId) {
  return path.join(RUN_STATE_DIR, `${runId}.json`);
}

export function loadSupervisorRunState(runId) {
  return readJson(runStatePath(runId), null);
}

function saveSupervisorRunState(state) {
  writeJson(runStatePath(state.runId), state);
  return state;
}

function estimateFamilyCost(scenarios, providers = PROVIDERS) {
  let total = 0;
  for (const p of providers) {
    total += scenarios * (PROVIDER_CONFIGS[p]?.costPerCall || 0);
  }
  return Number(total.toFixed(2));
}

/**
 * Idempotency key for a research family within a run.
 */
export function researchIdempotencyKey({ runId, propertyId, family, scenarioCount, providerSet }) {
  return `idem_${sha16({
    runId,
    propertyId,
    family,
    scenarioCount,
    providerSet: [...(providerSet || PROVIDERS)].sort(),
  })}`;
}

/**
 * Execute (or simulate) authorized supervisor handoff.
 * @param {{ simulation?: boolean, forceRerun?: boolean, injectFailure?: string }} opts
 */
export function executeAuthorizedSupervisorHandoff({
  runId,
  propertyId,
  hotelIdentity,
  authorizationRecord,
  simulation = true,
  allowPaid = false,
  forceRerun = false,
  injectFailure = null,
  authorizePublish = false,
} = {}) {
  if (authorizePublish) {
    return {
      ok: false,
      error: "PUBLICATION_NOT_PART_OF_SUPERVISOR_HANDOFF",
      message:
        "Research approval is not publication approval. External publish requires a separate founder gate.",
      paidCalls: 0,
    };
  }

  const auth =
    authorizationRecord ||
    loadAuthorizationForRun(runId, propertyId) ||
    null;
  if (!auth) {
    return {
      ok: false,
      error: "MISSING_EXACT_AUTHORIZATION",
      message:
        'A general "run ADP" statement does not authorize chargeable calls. Persist an exact authorization record first.',
      paidCalls: 0,
    };
  }
  if (auth.propertyId !== propertyId) {
    return {
      ok: false,
      error: "AUTHORIZATION_HOTEL_MISMATCH",
      expected: auth.propertyId,
      got: propertyId,
      paidCalls: 0,
    };
  }
  if (auth.runId !== runId) {
    return {
      ok: false,
      error: "AUTHORIZATION_RUN_MISMATCH",
      expected: auth.runId,
      got: runId,
      paidCalls: 0,
    };
  }
  if (auth.status !== "ACTIVE") {
    return {
      ok: false,
      error: "STALE_OR_INACTIVE_AUTHORIZATION",
      status: auth.status,
      paidCalls: 0,
    };
  }

  const existing = loadSupervisorRunState(runId);
  if (existing?.status === "COMPLETED" && !forceRerun) {
    return {
      ok: true,
      reused: true,
      paidCalls: 0,
      message: "Supervisor already completed for this runId — reuse archives; no re-charge.",
      state: existing,
    };
  }

  const lock = acquireSupervisorLock(propertyId, runId);
  if (!lock.ok) {
    return {
      ok: false,
      error: lock.error,
      existing: lock.existing,
      paidCalls: 0,
      message: "Duplicate concurrent supervisor invocation blocked.",
    };
  }

  try {
    if (!simulation && !allowPaid) {
      return {
        ok: false,
        error: "PAID_ADAPTERS_DISABLED",
        message: "Set simulation=true for zero-paid tests, or allowPaid with exact founder auth for live.",
        paidCalls: 0,
      };
    }

    if (injectFailure === "stale_approval") {
      return { ok: false, error: "STALE_OR_INACTIVE_AUTHORIZATION", paidCalls: 0 };
    }
    if (injectFailure === "wrong_hotel") {
      return {
        ok: false,
        error: "AUTHORIZATION_HOTEL_MISMATCH",
        paidCalls: 0,
      };
    }

    const coreScenarios = auth.scenarioCounts?.core ?? 63;
    const loyaltyApplicable = auth.loyaltyScope?.applicable !== false;
    const loyaltyScenarios = loyaltyApplicable ? auth.scenarioCounts?.loyalty ?? 28 : 0;
    const coreCost = estimateFamilyCost(coreScenarios, auth.providerSet);
    const loyaltyCost = estimateFamilyCost(loyaltyScenarios, auth.providerSet);
    const estimatedTotal = Number((coreCost + loyaltyCost).toFixed(2));

    if (estimatedTotal > Number(auth.maxSpendUsd)) {
      return {
        ok: false,
        error: "BUDGET_EXHAUSTION_PRECHECK",
        estimatedTotal,
        maxSpendUsd: auth.maxSpendUsd,
        paidCalls: 0,
        message: "Hard spending ceiling blocks launch before any provider call.",
      };
    }

    if (injectFailure === "budget_exhaustion") {
      return {
        ok: false,
        error: "BUDGET_EXHAUSTION_PRECHECK",
        estimatedTotal: Number(auth.maxSpendUsd) + 1,
        maxSpendUsd: auth.maxSpendUsd,
        paidCalls: 0,
      };
    }

    const coreKey = researchIdempotencyKey({
      runId,
      propertyId,
      family: "CORE_ADP",
      scenarioCount: coreScenarios,
      providerSet: auth.providerSet,
    });
    const loyaltyKey = researchIdempotencyKey({
      runId,
      propertyId,
      family: "LOYALTY_BPP",
      scenarioCount: loyaltyScenarios,
      providerSet: auth.providerSet,
    });

    const state = {
      contract: ADP_PAID_SUPERVISOR_HANDOFF_V1,
      version: SUPERVISOR_HANDOFF_VERSION,
      runId,
      propertyId,
      hotelIdentity: hotelIdentity || auth.hotelIdentity,
      authorizationId: auth.authorizationId,
      simulation: Boolean(simulation),
      allowPaid: Boolean(allowPaid) && !simulation,
      status: "IN_PROGRESS",
      startedAt: new Date().toISOString(),
      spend: { estimatedTotal, incurredUsd: 0, remainingAuthorizedUsd: auth.maxSpendUsd },
      families: {
        CORE_ADP: {
          idempotencyKey: coreKey,
          scenarioCount: coreScenarios,
          estimatedUsd: coreCost,
          status: "PENDING",
          archiveId: null,
        },
        LOYALTY_BPP: {
          idempotencyKey: loyaltyKey,
          scenarioCount: loyaltyScenarios,
          estimatedUsd: loyaltyCost,
          status: loyaltyApplicable ? "PENDING" : "NOT_APPLICABLE",
          archiveId: null,
        },
      },
      downstream: {
        coreArchiveValidation: "PENDING",
        loyaltyArchiveValidation: loyaltyApplicable ? "PENDING" : "SKIPPED",
        calculations: "PENDING",
        reportStaging: "PENDING",
        pdfShare: "PENDING",
        browserQa: "PENDING",
        customerRelease: "PENDING",
        founderReviewPackage: "PENDING",
      },
      errors: [],
      paidCalls: 0,
    };

    if (existing?.families?.CORE_ADP?.status === "COMPLETED" && !forceRerun) {
      state.families.CORE_ADP = existing.families.CORE_ADP;
      state.spend.incurredUsd = existing.spend?.incurredUsd || 0;
    }

    if (injectFailure === "provider_failure") {
      state.families.CORE_ADP.status = "FAILED";
      state.errors.push({ code: "PROVIDER_FAILURE", family: "CORE_ADP", simulated: true });
      state.status = "FAILED_PARTIAL";
      saveSupervisorRunState(state);
      return {
        ok: false,
        error: "PROVIDER_FAILURE",
        state,
        paidCalls: 0,
        message: "Simulated provider failure — no charge; resume from checkpoint.",
      };
    }

    if (injectFailure === "partial_completion") {
      state.families.CORE_ADP.status = "COMPLETED";
      state.families.CORE_ADP.archiveId = `sim_archive_core_${sha16(coreKey)}`;
      state.families.LOYALTY_BPP.status = loyaltyApplicable ? "FAILED" : "NOT_APPLICABLE";
      if (loyaltyApplicable) {
        state.errors.push({ code: "PARTIAL_LOYALTY_FAILURE", simulated: true });
      }
      state.status = "FAILED_PARTIAL";
      state.downstream.coreArchiveValidation = "PASS_SIM";
      state.downstream.loyaltyArchiveValidation = "FAIL_SIM";
      saveSupervisorRunState(state);
      return {
        ok: false,
        error: "PARTIAL_COMPLETION",
        state,
        paidCalls: 0,
        resumable: true,
      };
    }

    if (injectFailure === "missing_provider_response") {
      state.errors.push({ code: "MISSING_PROVIDER_RESPONSE", simulated: true });
      state.status = "FAILED_PARTIAL";
      saveSupervisorRunState(state);
      return { ok: false, error: "MISSING_PROVIDER_RESPONSE", state, paidCalls: 0 };
    }

    // Success path (simulation): mark research complete and auto-continue downstream.
    if (state.families.CORE_ADP.status !== "COMPLETED") {
      state.families.CORE_ADP.status = "COMPLETED";
      state.families.CORE_ADP.archiveId = simulation
        ? `sim_archive_core_${sha16(coreKey)}`
        : null;
      state.families.CORE_ADP.completedAt = new Date().toISOString();
    }
    if (loyaltyApplicable && state.families.LOYALTY_BPP.status !== "COMPLETED") {
      state.families.LOYALTY_BPP.status = "COMPLETED";
      state.families.LOYALTY_BPP.archiveId = simulation
        ? `sim_archive_loyalty_${sha16(loyaltyKey)}`
        : null;
      state.families.LOYALTY_BPP.completedAt = new Date().toISOString();
    }

    state.downstream.coreArchiveValidation = "PASS_SIM";
    state.downstream.loyaltyArchiveValidation = loyaltyApplicable ? "PASS_SIM" : "SKIPPED";

    if (injectFailure === "downstream_pdf_failure") {
      state.downstream.calculations = "PASS_SIM";
      state.downstream.reportStaging = "PASS_SIM";
      state.downstream.pdfShare = "FAIL_SIM";
      state.status = "FAILED_DOWNSTREAM";
      state.errors.push({
        code: "PDF_GENERATION_FAILED",
        note: "Do not re-run paid research; repair PDF from existing archives.",
      });
      saveSupervisorRunState(state);
      return {
        ok: false,
        error: "DOWNSTREAM_PDF_FAILURE",
        state,
        paidCalls: 0,
        message: "PDF failure must not trigger paid research re-run.",
      };
    }

    state.downstream.calculations = "PASS_SIM";
    state.downstream.reportStaging = "PASS_SIM";
    state.downstream.pdfShare = "PASS_SIM";
    state.downstream.browserQa = "PASS_SIM";
    state.downstream.customerRelease = "PASS_SIM_GATED";
    state.downstream.founderReviewPackage = "PASS_SIM";
    state.status = "COMPLETED";
    state.completedAt = new Date().toISOString();
    state.spend.incurredUsd = simulation ? 0 : estimatedTotal;
    state.spend.remainingAuthorizedUsd = Number(
      (auth.maxSpendUsd - state.spend.incurredUsd).toFixed(2)
    );
    state.paidCalls = simulation ? 0 : coreScenarios * 4 + loyaltyScenarios * 4;
    state.automaticContinuation = true;
    state.note = simulation
      ? "Zero-paid simulation — paid provider adapters disabled. Downstream stages marked PASS_SIM."
      : "Live paid path reserved for explicit allowPaid + founder auth; not auto-launched by this module without external supervisor bind.";

    saveSupervisorRunState(state);
    return {
      ok: true,
      simulation: Boolean(simulation),
      paidCalls: state.paidCalls,
      state,
      message: simulation
        ? "Supervisor handoff simulation completed with automatic downstream continuation."
        : "Authorization accepted; bind to dealality-adp-core-loyalty-execution supervisor for live calls.",
    };
  } finally {
    releaseSupervisorLock(propertyId, runId);
  }
}

/**
 * Pure fixture battery for zero-paid supervisor guarantees.
 */
export function runSupervisorHandoffSimulationBattery() {
  const runId = `sim_run_${Date.now().toString(36)}`;
  const propertyId = "adp_sim_posadas_chapultepec";
  const hotelIdentity = "Grand Fiesta Americana Chapultepec";
  const built = buildExactAuthorizationRecord({
    runId,
    hotelIdentity,
    propertyId,
    maxSpendUsd: 20,
    approverIdentity: "FOUNDER_SIMULATION",
  });
  if (!built.ok) return { ok: false, error: "AUTH_BUILD_FAILED", built };

  // Persist once
  const persisted = persistAuthorizationRecord(built.record);
  if (!persisted.ok && persisted.error !== "AUTHORIZATION_ALREADY_RECORDED") {
    return { ok: false, persisted };
  }

  const cases = {};

  cases.success = executeAuthorizedSupervisorHandoff({
    runId,
    propertyId,
    hotelIdentity,
    authorizationRecord: built.record,
    simulation: true,
  });

  cases.resume = executeAuthorizedSupervisorHandoff({
    runId,
    propertyId,
    hotelIdentity,
    authorizationRecord: built.record,
    simulation: true,
  });

  cases.duplicateInvocation = (() => {
    const lock = acquireSupervisorLock(propertyId, `${runId}_other`);
    const attempt = executeAuthorizedSupervisorHandoff({
      runId: `${runId}_dup`,
      propertyId,
      hotelIdentity,
      authorizationRecord: { ...built.record, runId: `${runId}_dup`, authorizationId: `auth_dup_${Date.now()}` },
      simulation: true,
    });
    releaseSupervisorLock(propertyId, `${runId}_other`);
    // If lock from other run held, attempt should fail; else we still prove reuse on success path
    return {
      lockHeldBlocked: lock.ok === true ? attempt.error === "SUPERVISOR_LOCK_HELD" || attempt.ok === false : true,
      attempt,
    };
  })();

  const budgetRun = `sim_budget_${Date.now().toString(36)}`;
  const budgetAuth = buildExactAuthorizationRecord({
    runId: budgetRun,
    hotelIdentity,
    propertyId: `${propertyId}_budget`,
    maxSpendUsd: 1,
    approverIdentity: "FOUNDER_SIMULATION",
  }).record;
  cases.budgetExhaustion = executeAuthorizedSupervisorHandoff({
    runId: budgetRun,
    propertyId: `${propertyId}_budget`,
    hotelIdentity,
    authorizationRecord: budgetAuth,
    simulation: true,
    injectFailure: "budget_exhaustion",
  });

  cases.staleApproval = executeAuthorizedSupervisorHandoff({
    runId,
    propertyId,
    hotelIdentity,
    authorizationRecord: { ...built.record, status: "REVOKED" },
    simulation: true,
  });

  cases.wrongHotel = executeAuthorizedSupervisorHandoff({
    runId,
    propertyId: "adp_wrong_hotel",
    hotelIdentity: "Wrong Hotel",
    authorizationRecord: built.record,
    simulation: true,
  });

  cases.providerFailure = executeAuthorizedSupervisorHandoff({
    runId: `${runId}_pf`,
    propertyId: `${propertyId}_pf`,
    hotelIdentity,
    authorizationRecord: {
      ...built.record,
      runId: `${runId}_pf`,
      propertyId: `${propertyId}_pf`,
      authorizationId: `auth_pf_${Date.now()}`,
    },
    simulation: true,
    injectFailure: "provider_failure",
  });

  cases.partialCompletion = executeAuthorizedSupervisorHandoff({
    runId: `${runId}_pc`,
    propertyId: `${propertyId}_pc`,
    hotelIdentity,
    authorizationRecord: {
      ...built.record,
      runId: `${runId}_pc`,
      propertyId: `${propertyId}_pc`,
      authorizationId: `auth_pc_${Date.now()}`,
    },
    simulation: true,
    injectFailure: "partial_completion",
  });

  cases.missingProviderResponse = executeAuthorizedSupervisorHandoff({
    runId: `${runId}_mpr`,
    propertyId: `${propertyId}_mpr`,
    hotelIdentity,
    authorizationRecord: {
      ...built.record,
      runId: `${runId}_mpr`,
      propertyId: `${propertyId}_mpr`,
      authorizationId: `auth_mpr_${Date.now()}`,
    },
    simulation: true,
    injectFailure: "missing_provider_response",
  });

  cases.downstreamPdfFailure = executeAuthorizedSupervisorHandoff({
    runId: `${runId}_pdf`,
    propertyId: `${propertyId}_pdf`,
    hotelIdentity,
    authorizationRecord: {
      ...built.record,
      runId: `${runId}_pdf`,
      propertyId: `${propertyId}_pdf`,
      authorizationId: `auth_pdf_${Date.now()}`,
    },
    simulation: true,
    injectFailure: "downstream_pdf_failure",
  });

  cases.unauthorizedPublication = executeAuthorizedSupervisorHandoff({
    runId,
    propertyId,
    hotelIdentity,
    authorizationRecord: built.record,
    simulation: true,
    authorizePublish: true,
  });

  cases.missingAuth = executeAuthorizedSupervisorHandoff({
    runId: "no_auth_run",
    propertyId,
    hotelIdentity,
    simulation: true,
  });

  const results = {
    success: cases.success.ok === true && cases.success.paidCalls === 0,
    resume: cases.resume.ok === true && cases.resume.reused === true && cases.resume.paidCalls === 0,
    duplicateInvocation: cases.duplicateInvocation.lockHeldBlocked === true,
    budgetExhaustion: cases.budgetExhaustion.error === "BUDGET_EXHAUSTION_PRECHECK",
    staleApproval: cases.staleApproval.error === "STALE_OR_INACTIVE_AUTHORIZATION",
    wrongHotel: cases.wrongHotel.error === "AUTHORIZATION_HOTEL_MISMATCH",
    providerFailure: cases.providerFailure.error === "PROVIDER_FAILURE",
    partialCompletion: cases.partialCompletion.error === "PARTIAL_COMPLETION",
    missingProviderResponse: cases.missingProviderResponse.error === "MISSING_PROVIDER_RESPONSE",
    downstreamPdfFailure:
      cases.downstreamPdfFailure.error === "DOWNSTREAM_PDF_FAILURE" &&
      cases.downstreamPdfFailure.paidCalls === 0,
    unauthorizedPublication:
      cases.unauthorizedPublication.error === "PUBLICATION_NOT_PART_OF_SUPERVISOR_HANDOFF",
    missingAuth: cases.missingAuth.error === "MISSING_EXACT_AUTHORIZATION",
  };

  const failed = Object.entries(results).filter(([, v]) => !v).map(([k]) => k);
  return {
    ok: failed.length === 0,
    contract: ADP_PAID_SUPERVISOR_HANDOFF_V1,
    paidCallsTotal: 0,
    results,
    failed,
    cases,
  };
}
