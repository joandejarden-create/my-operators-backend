/**
 * Packet 2.6C-R3 — Orphan provider-run recovery + lifecycle health.
 * Reattaches existing provider results without starting a new paid run.
 */

import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { createResearchRepository } from "./repository.js";
import { createWebhoundProvider, PROVIDER_IDS } from "./providers.js";
import { getTemplate } from "./templates.js";
import { normalizeResearchArtifact } from "./addendum-normalize.js";
import { buildResearchAddendumDossier } from "./addendum-builder.js";
import { buildHotelIdentitySeed } from "./hotel-seed.js";
import { assertCompletableResearchResult } from "./archive-integrity.js";
import { appendResearchAuditEvent } from "./audit-log.js";
import { recordExternalSpend } from "./spend-guard.js";
import { isTerminalStatus } from "./statuses.js";

function nowIso() {
  return new Date().toISOString();
}

/**
 * Find provider_run_id from RAW_RESEARCH_START artifacts for a request.
 */
export function findStartArtifactSession(repo, hotelId, requestId) {
  const req = repo.getRequest?.(hotelId, requestId);
  if (req?.raw_artifact_id) {
    const art = repo.getArtifact(hotelId, req.raw_artifact_id);
    if (art?.kind === "RAW_RESEARCH_START") {
      const sid = art.provider_run_id || art.payload?.session_id;
      if (sid) {
        return {
          artifact_id: art.artifact_id,
          provider_run_id: String(sid),
          written_at: art.written_at || null,
        };
      }
    }
  }
  // Known start artifact for founder C&O orphan (also scan all arts)
  const candidates = ["art_d7d34cab"];
  for (const id of candidates) {
    const art = repo.getArtifact?.(hotelId, id);
    if (!art || art.kind !== "RAW_RESEARCH_START") continue;
    if (art.payload?.request_id && art.payload.request_id !== requestId) continue;
    const sid = art.provider_run_id || art.payload?.session_id;
    if (sid) {
      return { artifact_id: art.artifact_id, provider_run_id: String(sid), written_at: art.written_at || null };
    }
  }
  const root = repo.root;
  if (!root) return null;
  const dir = path.join(root, "hotels", String(hotelId).replace(/[^a-zA-Z0-9_-]/g, "_"), "artifacts");
  if (!fs.existsSync(dir)) return null;
  for (const name of fs.readdirSync(dir)) {
    if (!name.endsWith(".json")) continue;
    try {
      const art = JSON.parse(fs.readFileSync(path.join(dir, name), "utf8"));
      if (art.kind !== "RAW_RESEARCH_START") continue;
      const payload = art.payload || {};
      if (payload.request_id && payload.request_id !== requestId) continue;
      const sid = art.provider_run_id || payload.session_id || payload.provider_run_id;
      if (sid) {
        return {
          artifact_id: art.artifact_id,
          provider_run_id: String(sid),
          written_at: art.written_at || null,
        };
      }
    } catch {
      /* skip */
    }
  }
  return null;
}

/**
 * Detect provider-complete / report-missing / archive-missing orphans.
 */
export function detectResearchOrphans(hotelId, opts = {}) {
  const repo = opts.repo || createResearchRepository({ env: opts.env });
  const index = repo.listHotelResearch(hotelId);
  const orphanProviderRuns = [];
  const orphanReports = [];
  const orphanArchive = [];

  for (const req of index.requests || []) {
    if (req.simulated || req.is_simulation) continue;
    const start = findStartArtifactSession(repo, hotelId, req.request_id);
    const sessionId = req.provider_run_id || start?.provider_run_id || null;
    const terminalOk =
      isTerminalStatus(req.status) &&
      ["COMPLETED", "COMPLETED_WITH_OPEN_QUESTIONS", "PARTIAL"].includes(req.status);

    if (sessionId && !terminalOk) {
      orphanProviderRuns.push({
        kind: "ORPHAN_PROVIDER_RUN",
        hotel_id: hotelId,
        request_id: req.request_id,
        run_id: (index.runs || []).find((r) => r.request_id === req.request_id)?.run_id || null,
        provider_run_id: sessionId,
        status: req.status,
        start_artifact_id: start?.artifact_id || null,
        report_id: req.report_id || null,
      });
    }

    if (req.report_id && !terminalOk) {
      orphanReports.push({
        kind: "ORPHAN_REPORT",
        hotel_id: hotelId,
        request_id: req.request_id,
        report_id: req.report_id,
        status: req.status,
      });
    }
  }

  return { orphanProviderRuns, orphanReports, orphanArchive };
}

/**
 * Recover a provider-completed orphan into report + archive without new provider spend.
 */
export async function recoverOrphanProviderRun(opts = {}) {
  const env = opts.env || process.env;
  const hotelId = String(opts.hotel_id || "").trim();
  const requestId = String(opts.request_id || "").trim();
  if (!hotelId || !requestId) throw new Error("hotel_id_and_request_id_required");

  const repo = opts.repo || createResearchRepository({ env });
  let request = repo.getRequest(hotelId, requestId);
  if (!request) throw new Error("request_not_found");

  const start = findStartArtifactSession(repo, hotelId, requestId);
  const sessionId = String(
    opts.provider_run_id || request.provider_run_id || start?.provider_run_id || ""
  ).trim();
  if (!sessionId) {
    const err = new Error("orphan_missing_provider_run_id");
    err.code = "orphan_missing_provider_run_id";
    throw err;
  }

  // Already terminal with report — idempotent no-op (no provider, no duplicate report).
  if (
    isTerminalStatus(request.status) &&
    ["COMPLETED", "COMPLETED_WITH_OPEN_QUESTIONS", "PARTIAL"].includes(request.status) &&
    request.report_id &&
    (request.provider_run_id === sessionId || !request.provider_run_id)
  ) {
    const runs0 = repo.listHotelResearch(hotelId).runs.filter((r) => r.request_id === requestId);
    return {
      ok: true,
      recovered_legacy_orphan: Boolean(request.recovered_legacy_orphan),
      new_provider_runs: 0,
      paid_cost_usd: 0,
      historical_provider_cost_usd: request.provider_actual_cost_usd,
      request,
      run: runs0[runs0.length - 1] || null,
      report_id: request.report_id,
      pdf_id: request.pdf_id || request.report_id,
      raw_artifact_id: request.raw_artifact_id,
      normalized_research_id: request.normalized_research_id,
      source_count: request.source_count,
      finding_count: request.finding_count,
      open_question_count: request.open_question_count,
      provider_run_id: sessionId,
      idempotent_replay: true,
    };
  }

  // Reopen FAILED → PROVIDER_COMPLETED for pipeline resume
  if (request.status === "FAILED" || request.status === "CANCELLED") {
    request = repo.updateRequest(hotelId, requestId, {
      allow_orphan_recovery: true,
      status: "PROVIDER_COMPLETED",
      provider_run_id: sessionId,
      raw_artifact_id: start?.artifact_id || request.raw_artifact_id,
      error_code: null,
      error_message: null,
      customer_safe_error: null,
      completed_at: null,
      recovered_legacy_orphan: true,
    });
  } else if (!request.provider_run_id) {
    request = repo.updateRequest(hotelId, requestId, {
      provider_run_id: sessionId,
      status: request.status === "QUEUED" ? "PROVIDER_COMPLETED" : request.status,
    });
  }

  const runs = repo.listHotelResearch(hotelId).runs.filter((r) => r.request_id === requestId);
  let run = runs[runs.length - 1];
  if (run) {
    run = repo.updateRun(hotelId, run.run_id, {
      provider_run_id: sessionId,
      provider_status: "PROVIDER_COMPLETED",
      completed_at: null,
      error: null,
    });
  }

  appendResearchAuditEvent(
    {
      event: "orphan_provider_recovery_started",
      hotel_id: hotelId,
      template_id: request.template_id,
      request_id: requestId,
      run_id: run?.run_id,
      provider: "WEBHOUND",
      provider_run_id: sessionId,
      status: "PROVIDER_COMPLETED",
      recovered_legacy_orphan: true,
      new_provider_runs: 0,
    },
    env
  );

  const template = getTemplate(request.template_id);
  const provider = createWebhoundProvider({ env, ...opts.providerOpts });

  // Collect EXISTING session — never start
  const collected = await provider.collectCompleted(sessionId);
  if (!collected?.watch?.done && collected?.status !== "COMPLETED") {
    const err = new Error("provider_session_not_done");
    err.code = "provider_session_not_done";
    err.watch = collected?.watch || null;
    throw err;
  }

  // Prefer historical provider completion time — never invent a fresh "run now" stamp.
  const historicalCompletedAt =
    collected.watch?.status_snapshot?.updated_at ||
    collected.watch?.updated_at ||
    collected.raw_artifact?.evidence_pack?.session?.updated_at ||
    collected.raw_artifact?.output?.updated_at ||
    collected.raw_artifact?.completed_at ||
    start?.written_at ||
    request.created_at ||
    nowIso();
  const historicalStartedAt =
    request.started_at ||
    start?.written_at ||
    request.created_at ||
    historicalCompletedAt;

  // Idempotent resume: reuse existing immutable RAW_RESEARCH if already saved for this session.
  let rawArt = null;
  const existingArts = [];
  try {
    const dir = path.join(
      repo.root,
      "hotels",
      String(hotelId).replace(/[^a-zA-Z0-9_-]/g, "_"),
      "artifacts"
    );
    if (fs.existsSync(dir)) {
      for (const name of fs.readdirSync(dir)) {
        if (!name.endsWith(".json")) continue;
        try {
          existingArts.push(JSON.parse(fs.readFileSync(path.join(dir, name), "utf8")));
        } catch {
          /* skip */
        }
      }
    }
  } catch {
    /* scan best-effort */
  }
  rawArt =
    existingArts.find(
      (a) =>
        a.kind === "RAW_RESEARCH" &&
        (a.provider_run_id === sessionId || a.payload?.session_id === sessionId) &&
        a.immutable === true
    ) || null;

  if (!rawArt) {
    repo.updateRequest(hotelId, requestId, { status: "RAW_ARTIFACT_SAVED" });
    rawArt = repo.writeArtifact(hotelId, {
      artifact_id: `art_${crypto.randomBytes(4).toString("hex")}`,
      kind: "RAW_RESEARCH",
      provider: PROVIDER_IDS.WEBHOUND,
      provider_run_id: sessionId,
      simulated: false,
      immutable: true,
      payload: collected.raw_artifact,
    });
  } else {
    repo.updateRequest(hotelId, requestId, {
      status: "RAW_ARTIFACT_SAVED",
      raw_artifact_id: rawArt.artifact_id,
    });
  }

  repo.updateRequest(hotelId, requestId, { status: "NORMALIZING" });
  const normalized = normalizeResearchArtifact({
    raw: rawArt.payload || collected.raw_artifact,
    template,
    request: { ...request, provider_run_id: sessionId },
    hotel_seed: buildHotelIdentitySeed({
      hotel_id: hotelId,
      hotel_name: request.hotel_name,
    }),
  });

  let normArt =
    existingArts.find(
      (a) =>
        a.kind === "NORMALIZED_RESEARCH" &&
        (a.payload?.provider_run_id === sessionId ||
          a.payload?.session_id === sessionId ||
          request.normalized_research_id === a.artifact_id)
    ) || null;
  if (!normArt) {
    normArt = repo.writeArtifact(hotelId, {
      artifact_id: normalized.normalized_research_id || `norm_${crypto.randomBytes(4).toString("hex")}`,
      kind: "NORMALIZED_RESEARCH",
      simulated: false,
      immutable: true,
      payload: { ...normalized, provider_run_id: sessionId, session_id: sessionId },
    });
  }

  repo.updateRequest(hotelId, requestId, { status: "GENERATING_REPORT" });
  const actual = collected.provider_cost_usd ?? request.provider_actual_cost_usd ?? 5;
  try {
    recordExternalSpend(
      {
        kind: "settle",
        amount_usd: actual,
        hotel_id: hotelId,
        request_id: requestId,
        template_id: request.template_id,
        note: "orphan_recovery_historical_cost",
      },
      env
    );
  } catch {
    /* ledger settle is best-effort for recovery */
  }

  // Reuse existing report if already materialized for this request (idempotent).
  const dossierId =
    request.report_id && String(request.report_id).startsWith("addendum_")
      ? request.report_id
      : undefined;

  const dossier = buildResearchAddendumDossier({
    dossier_id: dossierId,
    template,
    request: {
      ...request,
      provider_run_id: sessionId,
      provider_actual_cost_usd: actual,
      raw_artifact_id: rawArt.artifact_id,
      recovered_legacy_orphan: true,
      started_at: historicalStartedAt,
      completed_at: historicalCompletedAt,
    },
    normalized: { ...normalized, raw_ref: rawArt.artifact_id },
    run_id: run?.run_id,
    completed_at: historicalCompletedAt,
    env,
    persist: true,
  });

  assertCompletableResearchResult({
    simulated: false,
    report_id: dossier.dossier_id,
    normalized_research_id: normArt.artifact_id,
    source_count: dossier.source_count,
    finding_count: dossier.finding_count,
    open_question_count: dossier.open_question_count,
    explicit_no_evidence_validated: normalized.explicit_no_evidence_validated === true,
    immutable_historical: false,
  });

  repo.updateRequest(hotelId, requestId, { status: "ARCHIVING" });
  const terminalStatus =
    (dossier.open_question_count || 0) > 0
      ? "COMPLETED_WITH_OPEN_QUESTIONS"
      : dossier.source_count === 0 && normalized.explicit_no_evidence_validated
        ? "PARTIAL"
        : "COMPLETED";

  request = repo.updateRequest(hotelId, requestId, {
    status: terminalStatus,
    started_at: historicalStartedAt,
    completed_at: historicalCompletedAt,
    provider_run_id: sessionId,
    raw_artifact_id: rawArt.artifact_id,
    normalized_research_id: normArt.artifact_id,
    report_id: dossier.dossier_id,
    pdf_id: dossier.dossier_id,
    source_count: dossier.source_count,
    finding_count: dossier.finding_count,
    open_question_count: dossier.open_question_count,
    provider_actual_cost_usd: actual,
    total_internal_cost_usd: actual,
    simulated: false,
    is_simulation: false,
    recovered_legacy_orphan: true,
    claim_handoff: { auto_promote: false },
  });

  if (run) {
    run = repo.updateRun(hotelId, run.run_id, {
      started_at: historicalStartedAt,
      completed_at: historicalCompletedAt,
      provider_status: terminalStatus,
      actual_cost: actual,
      raw_artifact_id: rawArt.artifact_id,
      report_version: template.version,
      simulated: false,
      recovered_legacy_orphan: true,
    });
  }

  appendResearchAuditEvent(
    {
      event: "orphan_provider_recovery_completed",
      hotel_id: hotelId,
      template_id: request.template_id,
      request_id: requestId,
      run_id: run?.run_id,
      provider: "WEBHOUND",
      provider_run_id: sessionId,
      budget_usd: request.provider_budget_usd,
      actual_cost_usd: actual,
      status: terminalStatus,
      report_id: dossier.dossier_id,
      recovered_legacy_orphan: true,
      new_provider_runs: 0,
    },
    env
  );

  return {
    ok: true,
    recovered_legacy_orphan: true,
    new_provider_runs: 0,
    paid_cost_usd: 0,
    historical_provider_cost_usd: actual,
    request,
    run,
    report_id: dossier.dossier_id,
    pdf_id: dossier.dossier_id,
    raw_artifact_id: rawArt.artifact_id,
    normalized_research_id: normArt.artifact_id,
    source_count: dossier.source_count,
    finding_count: dossier.finding_count,
    open_question_count: dossier.open_question_count,
    provider_run_id: sessionId,
  };
}

/**
 * Scan hotel for orphans and recover provider-completed sessions (no new starts).
 */
export async function resumeIncompleteResearchPipelines(hotelId, opts = {}) {
  const detected = detectResearchOrphans(hotelId, opts);
  const results = [];
  for (const orphan of detected.orphanProviderRuns) {
    if (opts.onlyRequestId && orphan.request_id !== opts.onlyRequestId) continue;
    try {
      const recovered = await recoverOrphanProviderRun({
        hotel_id: hotelId,
        request_id: orphan.request_id,
        provider_run_id: orphan.provider_run_id,
        env: opts.env,
        repo: opts.repo,
        providerOpts: opts.providerOpts,
      });
      results.push({ orphan, recovered });
    } catch (err) {
      results.push({
        orphan,
        error: { code: err.code || "recovery_failed", message: String(err.message || err).slice(0, 300) },
      });
    }
  }
  return { detected, results };
}
