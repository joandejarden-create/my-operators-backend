/**
 * Packet 2.6C-R2 — ResearchOrchestrator (simulation + controlled live Webhound).
 * REQUEST → RUN → RAW → NORMALIZED → ADDENDUM → PDF interface → ARCHIVE.
 * claim_handoff.auto_promote always false. No automatic paid retry.
 */

import crypto from "node:crypto";
import { getTemplate } from "./templates.js";
import { createResearchRepository } from "./repository.js";
import { resolveProvider, PROVIDER_IDS, createWebhoundProvider } from "./providers.js";
import {
  requireProviderBudget,
  isExternalResearchEnabled,
  getPilotMaxProviderBudgetUsd,
  isFounderInternalDebug,
  PILOT_MAX_PROVIDER_BUDGET_USD,
} from "./policy.js";
import {
  isActiveStatus,
  isTerminalStatus,
  nextSimulationStatus,
  customerStageLabel,
} from "./statuses.js";
import { resolveReportExportLinks, REPORT_KINDS } from "./report-export-contract.js";
import { ensureKgpvRun1Backfill, KGPV_HOTEL_ID } from "./backfill-kgpv.js";
import { assertCompletableResearchResult } from "./archive-integrity.js";
import { buildHotelIdentitySeed } from "./hotel-seed.js";
import { normalizeResearchArtifact } from "./addendum-normalize.js";
import { buildResearchAddendumDossier } from "./addendum-builder.js";
import { assertMayStartPaidWebhoundRun, recordExternalSpend } from "./spend-guard.js";
import { appendResearchAuditEvent } from "./audit-log.js";

function nowIso() {
  return new Date().toISOString();
}

/** In-process live tick schedulers (one chain per request). */
const liveTickTimers = new Map();

function clearLiveTick(requestId) {
  const t = liveTickTimers.get(requestId);
  if (t) clearTimeout(t);
  liveTickTimers.delete(requestId);
}

export function createResearchOrchestrator(opts = {}) {
  const repo = opts.repository || createResearchRepository(opts);
  const env = opts.env || process.env;
  const providerOpts = {
    env,
    clientFactory: opts.webhoundClientFactory,
    fetchImpl: opts.fetchImpl,
    webhound_key: opts.webhound_key,
  };

  function ensureHotelSeed(input) {
    const hotelId = String(input.hotel_id || input.hotelId || "").trim();
    if (!hotelId) {
      const err = new Error("hotel_id_required");
      err.code = "hotel_id_required";
      throw err;
    }
    return buildHotelIdentitySeed({
      hotel_id: hotelId,
      hotel_name: input.hotel_name || input.hotelName || null,
      ...input.hotel_seed,
    });
  }

  function normalizeSimulatedArtifact(raw, template, request) {
    return {
      normalized_research_id: `norm_${crypto.randomBytes(4).toString("hex")}`,
      simulated: true,
      is_simulation: true,
      template_id: template.template_id,
      template_version: template.version,
      report_id: null,
      report_type: request.report_type,
      title: `${template.display_name} — Research Addendum (SIMULATED)`,
      source_count: 0,
      finding_count: 0,
      open_question_count: 0,
      findings: [],
      sources: [],
      open_questions: [],
      claim_handoff: { auto_promote: false },
      raw_ref: raw?.provider_run_id || null,
      note: "SIMULATED — excluded from live archive.",
      change_opportunity_schema:
        template.template_id === "CHANGE_OPPORTUNITY"
          ? {
              executive_answer: {
                why_now: null,
                what_could_derail: null,
                what_looks_stable: null,
                what_needs_verification: null,
              },
              opportunity_thesis: null,
              asset_product_risk: {
                assessment: "INSUFFICIENT_EVIDENCE",
                themes: [],
                product_investment: null,
                deferred_capex: null,
              },
              operating_quality: {
                assessment: "INSUFFICIENT_EVIDENCE",
                themes: [],
                management_response: null,
              },
              operator_stability: { assessment: "INSUFFICIENT_EVIDENCE", signals: [] },
              deal_risk_flags: [],
              what_to_verify_next: [],
            }
          : undefined,
    };
  }

  function scheduleLiveTick(hotelId, requestId) {
    clearLiveTick(requestId);
    const delayMs = Number(env.HOTEL_INTELLIGENCE_LIVE_TICK_MS) || 20000;
    const timer = setTimeout(() => {
      liveTickTimers.delete(requestId);
      advanceLiveRequest(hotelId, requestId).catch((err) => {
        console.error("[hi-research] live tick", requestId, err.code || err.message);
      });
    }, delayMs);
    if (typeof timer.unref === "function") timer.unref();
    liveTickTimers.set(requestId, timer);
  }

  async function createResearchRequest(input = {}) {
    const seed = ensureHotelSeed(input);
    if (seed.hotel_id === KGPV_HOTEL_ID) ensureKgpvRun1Backfill(repo);

    const template = getTemplate(input.template_id);
    if (!template) {
      const err = new Error("unknown_template");
      err.code = "unknown_template";
      err.customer_safe = "That investigation is not available.";
      throw err;
    }

    // KGPV Full Investigation is historical — never rerun from Research Center.
    if (
      seed.hotel_id === KGPV_HOTEL_ID &&
      template.template_id === "FULL_HOTEL_INTELLIGENCE" &&
      !input.allow_kgpv_full_rerun
    ) {
      const err = new Error("kgpv_full_investigation_immutable");
      err.code = "kgpv_full_investigation_immutable";
      err.customer_safe =
        "The Full Hotel Intelligence Investigation for this hotel already exists and cannot be rerun from Research Center.";
      throw err;
    }

    if (input.custom_question && !input.allow_custom_question) {
      const err = new Error("custom_questions_not_enabled");
      err.code = "custom_questions_not_enabled";
      err.customer_safe = "Custom research questions are not available yet.";
      throw err;
    }

    const wantLive =
      input.run_live === true ||
      input.confirm_spend === true ||
      (input.authorized === true &&
        String(input.provider_strategy || "").toUpperCase() === "WEBHOUND");

    const externalOn = isExternalResearchEnabled(env);
    const forceSim = input.force_simulation === true || (!externalOn && !wantLive);

    let liveMode = false;
    if (wantLive && !forceSim) {
      if (!externalOn) {
        const err = new Error("external_research_disabled");
        err.code = "external_research_disabled";
        err.customer_safe = "Live research is disabled on this server.";
        throw err;
      }
      if (input.confirm_spend !== true && input.explicit_user_action !== true) {
        const err = new Error("explicit_run_research_required");
        err.code = "explicit_run_research_required";
        err.customer_safe = "Confirm Run Research to start live investigation.";
        throw err;
      }
      liveMode = true;
    }

    const hardCap = getPilotMaxProviderBudgetUsd(env);
    const requestedBudget =
      input.provider_budget_usd != null
        ? Number(input.provider_budget_usd)
        : Number(template.max_provider_budget_usd ?? template.default_provider_budget_usd ?? hardCap);
    const budget = requireProviderBudget(Math.min(requestedBudget, hardCap), env);

    if (liveMode) {
      assertMayStartPaidWebhoundRun({
        env,
        repository: repo,
        budget_usd: budget,
      });
    }

    // Duplicate-run guard: same hotel+template already processing → join existing.
    {
      const hotelIndex = repo.listHotelResearch(seed.hotel_id);
      const activeSame = (hotelIndex.requests || []).find(
        (r) =>
          r.template_id === template.template_id &&
          !r.simulated &&
          !r.is_simulation &&
          isActiveStatus(r.status)
      );
      if (activeSame) {
        const existingRun = (hotelIndex.runs || [])
          .filter((r) => r.request_id === activeSame.request_id)
          .pop();
        return {
          request: activeSame,
          run: existingRun || null,
          export_links: resolveReportExportLinks(activeSame),
          mode: "LIVE",
          joined_existing_active_run: true,
          provider_run_id: activeSame.provider_run_id || null,
        };
      }
    }

    const parentReportId =
      input.parent_report_id ||
      (seed.hotel_id === KGPV_HOTEL_ID ? "dossier_kgpv_full_hi_v1" : null);

    const request = repo.createRequest({
      hotel_id: seed.hotel_id,
      hotel_name: seed.hotel_name,
      requested_by: input.requested_by || "ui",
      template_id: template.template_id,
      template_version: template.version,
      question: template.customer_question,
      scope: template.scope_labels || template.research_lanes,
      status: "QUEUED",
      provider_strategy: liveMode ? "WEBHOUND" : "SIMULATION",
      provider_budget_usd: budget,
      max_provider_budget_usd: hardCap,
      estimated_research_units: template.estimated_research_units,
      pricing_tier_key: template.pricing_tier_key,
      future_credit_cost: template.future_credit_cost,
      simulated: !liveMode,
      is_simulation: !liveMode,
      idempotency_token: input.idempotency_token || null,
      parent_report_id: parentReportId,
      report_type:
        template.report_template === "FULL_INVESTIGATION"
          ? REPORT_KINDS.FULL_INVESTIGATION
          : REPORT_KINDS.RESEARCH_ADDENDUM,
      claim_handoff: { auto_promote: false },
      known_facts: input.known_facts || null,
      known_gaps: input.known_gaps || null,
    });

    // Idempotent replay: one user action ⇒ at most one paid/sim run.
    {
      const existingRuns = repo
        .listHotelResearch(seed.hotel_id)
        .runs.filter((r) => r.request_id === request.request_id);
      if (
        existingRuns.length > 0 ||
        request.provider_run_id ||
        (request.status && request.status !== "QUEUED")
      ) {
        return {
          request,
          run: existingRuns[existingRuns.length - 1] || null,
          export_links: resolveReportExportLinks(request),
          idempotent_replay: true,
          mode: request.simulated ? "SIMULATION" : "LIVE",
        };
      }
    }

    const run = repo.createRun({
      hotel_id: seed.hotel_id,
      request_id: request.request_id,
      provider: liveMode ? PROVIDER_IDS.WEBHOUND : PROVIDER_IDS.SIMULATION,
      provider_version: liveMode ? "2.0.0" : "1.0.0",
      budget,
      simulated: !liveMode,
      is_simulation: !liveMode,
      template_id: template.template_id,
      template_version: template.version,
      provider_status: "QUEUED",
    });

    appendResearchAuditEvent(
      {
        event: liveMode ? "live_research_requested" : "simulation_research_requested",
        triggered_by: input.requested_by || "ui",
        hotel_id: seed.hotel_id,
        template_id: template.template_id,
        template_version: template.version,
        request_id: request.request_id,
        run_id: run.run_id,
        provider: liveMode ? "WEBHOUND" : "SIMULATION",
        budget_usd: budget,
        status: "QUEUED",
      },
      env
    );

    if (liveMode) {
      // Durable state BEFORE provider invoke: mark PROVIDER_STARTING so concurrent
      // Research Center ticks never treat a mid-start request as missing_provider_run_id.
      repo.updateRequest(seed.hotel_id, request.request_id, {
        status: "PROVIDER_STARTING",
        started_at: nowIso(),
      });
      repo.updateRun(seed.hotel_id, run.run_id, {
        provider_status: "PROVIDER_STARTING",
      });
      recordExternalSpend(
        {
          kind: "reserve",
          amount_usd: budget,
          hotel_id: seed.hotel_id,
          request_id: request.request_id,
          template_id: template.template_id,
        },
        env
      );
      const started = await startLiveProvider({
        hotel_id: seed.hotel_id,
        request_id: request.request_id,
        run_id: run.run_id,
        template,
        seed,
        budget,
        authorized: true,
        confirm_spend: true,
        known_facts: input.known_facts,
        known_gaps: input.known_gaps,
        requested_by: input.requested_by || "ui",
      });
      if (input.execute !== false) {
        scheduleLiveTick(seed.hotel_id, request.request_id);
      }
      return {
        request: repo.getRequest(seed.hotel_id, request.request_id),
        run: repo.listHotelResearch(seed.hotel_id).runs.filter((r) => r.request_id === request.request_id).pop(),
        export_links: resolveReportExportLinks(repo.getRequest(seed.hotel_id, request.request_id)),
        mode: "LIVE",
        provider_run_id: started.provider_run_id,
      };
    }

    // Simulation path
    if (input.execute !== false) {
      return advanceSimulation(seed.hotel_id, request.request_id, {
        to_completion: input.to_completion !== false,
      });
    }
    return {
      request: repo.getRequest(seed.hotel_id, request.request_id),
      run,
      export_links: resolveReportExportLinks(request),
      mode: "SIMULATION",
    };
  }

  async function startLiveProvider(ctx) {
    const provider = createWebhoundProvider(providerOpts);
    try {
      const result = await provider.start({
        hotel_seed: ctx.seed,
        template: ctx.template,
        budget_usd: ctx.budget,
        request_id: ctx.request_id,
        run_id: ctx.run_id,
        authorized: true,
        confirm_spend: true,
        explicit_user_action: true,
        known_facts: ctx.known_facts,
        known_gaps: ctx.known_gaps,
      });
      const artifact = repo.writeArtifact(ctx.hotel_id, {
        artifact_id: `art_${crypto.randomBytes(4).toString("hex")}`,
        kind: "RAW_RESEARCH_START",
        provider: PROVIDER_IDS.WEBHOUND,
        provider_run_id: result.provider_run_id,
        simulated: false,
        payload: result.raw_artifact,
      });
      repo.updateRequest(ctx.hotel_id, ctx.request_id, {
        status: "RUNNING",
        started_at: nowIso(),
        provider_run_id: result.provider_run_id,
        raw_artifact_id: artifact.artifact_id,
        simulated: false,
        is_simulation: false,
      });
      repo.updateRun(ctx.hotel_id, ctx.run_id, {
        provider_run_id: result.provider_run_id,
        provider_status: "RUNNING",
        raw_artifact_id: artifact.artifact_id,
        simulated: false,
      });
      appendResearchAuditEvent(
        {
          event: "webhound_started",
          triggered_by: ctx.requested_by,
          hotel_id: ctx.hotel_id,
          template_id: ctx.template.template_id,
          request_id: ctx.request_id,
          run_id: ctx.run_id,
          provider: "WEBHOUND",
          provider_run_id: result.provider_run_id,
          budget_usd: ctx.budget,
          status: "RUNNING",
        },
        env
      );
      return result;
    } catch (err) {
      recordExternalSpend(
        {
          kind: "release_reserve",
          amount_usd: ctx.budget,
          hotel_id: ctx.hotel_id,
          request_id: ctx.request_id,
          template_id: ctx.template.template_id,
        },
        env
      );
      repo.updateRequest(ctx.hotel_id, ctx.request_id, {
        status: "FAILED",
        completed_at: nowIso(),
        error_code: err.code || "provider_start_failed",
        error_message: String(err.message || err).slice(0, 200),
        customer_safe_error: err.customer_safe || "Research could not be started.",
      });
      repo.updateRun(ctx.hotel_id, ctx.run_id, {
        completed_at: nowIso(),
        provider_status: "FAILED",
        error: { code: err.code || "provider_start_failed" },
      });
      appendResearchAuditEvent(
        {
          event: "webhound_start_failed",
          hotel_id: ctx.hotel_id,
          template_id: ctx.template.template_id,
          request_id: ctx.request_id,
          run_id: ctx.run_id,
          provider: "WEBHOUND",
          budget_usd: ctx.budget,
          status: "FAILED",
          error_code: err.code || "provider_start_failed",
        },
        env
      );
      throw err;
    }
  }

  async function advanceLiveRequest(hotelId, requestId) {
    let request = repo.getRequest(hotelId, requestId);
    if (!request) {
      const err = new Error("request_not_found");
      err.code = "request_not_found";
      throw err;
    }
    if (request.simulated || request.is_simulation) {
      return advanceSimulation(hotelId, requestId, { to_completion: false });
    }
    if (request.immutable_historical || isTerminalStatus(request.status)) {
      clearLiveTick(requestId);
      return {
        request,
        run: repo.listHotelResearch(hotelId).runs.filter((r) => r.request_id === requestId).pop(),
        export_links: resolveReportExportLinks(request),
        customer_stage: customerStageLabel(request.status),
      };
    }

    const template = getTemplate(request.template_id);
    const runs = repo.listHotelResearch(hotelId).runs.filter((r) => r.request_id === requestId);
    let run = runs[runs.length - 1];
    const provider = createWebhoundProvider(providerOpts);
    let sessionId = request.provider_run_id;

    // Recover provider_run_id from start artifact if race left it null
    if (!sessionId) {
      try {
        const { findStartArtifactSession } = await import("./orphan-recovery.js");
        const start = findStartArtifactSession(repo, hotelId, requestId);
        if (start?.provider_run_id) {
          sessionId = start.provider_run_id;
          request = repo.updateRequest(hotelId, requestId, {
            provider_run_id: sessionId,
            raw_artifact_id: start.artifact_id || request.raw_artifact_id,
            status: request.status === "QUEUED" || request.status === "PROVIDER_STARTING" ? "RUNNING" : request.status,
          });
          if (run) {
            run = repo.updateRun(hotelId, run.run_id, {
              provider_run_id: sessionId,
              provider_status: "RUNNING",
            });
          }
        }
      } catch (err) {
        console.error("[hi-research] start-artifact recovery", err);
      }
    }

    if (!sessionId) {
      // Mid-start grace: never fail QUEUED/PROVIDER_STARTING while provider invoke is in flight.
      const ageMs = Date.now() - new Date(request.created_at || request.started_at || 0).getTime();
      const starting =
        request.status === "QUEUED" ||
        request.status === "PROVIDER_STARTING" ||
        (request.status === "RUNNING" && ageMs < 15 * 60 * 1000);
      if (starting && Number.isFinite(ageMs) && ageMs < 15 * 60 * 1000) {
        scheduleLiveTick(hotelId, requestId);
        return {
          request,
          run,
          export_links: resolveReportExportLinks(request),
          customer_stage: customerStageLabel(request.status || "PROVIDER_STARTING"),
          waiting_for_provider_run_id: true,
        };
      }
      request = repo.updateRequest(hotelId, requestId, {
        status: "FAILED",
        completed_at: nowIso(),
        error_code: "missing_provider_run_id",
        customer_safe_error: "Research session was lost.",
      });
      clearLiveTick(requestId);
      return { request, run, export_links: resolveReportExportLinks(request) };
    }

    try {
      const watch = await provider.watch(sessionId);
      const done = watch.done === true;
      const failed =
        String(watch.completion_reason || "").toLowerCase().includes("fail") ||
        (Array.isArray(watch.alerts) &&
          watch.alerts.some((a) => /credit_exhausted|empty_output|failed/i.test(a.type || a.code || "")));

      if (!done) {
        // Still researching — keep RUNNING / Discovering Sources
        repo.updateRequest(hotelId, requestId, {
          status: "RUNNING",
          provider_watch_snapshot: {
            done: false,
            completion_reason: watch.completion_reason || null,
            updated_at: nowIso(),
          },
        });
        scheduleLiveTick(hotelId, requestId);
        return {
          request: repo.getRequest(hotelId, requestId),
          run,
          export_links: resolveReportExportLinks(request),
          customer_stage: customerStageLabel("RUNNING"),
          watch,
        };
      }

      if (failed && !watch.output_ready) {
        const collected = await provider.collectCompleted(sessionId).catch(() => null);
        const actual = collected?.provider_cost_usd ?? null;
        settleSpend(request, actual);
        request = repo.updateRequest(hotelId, requestId, {
          status: "FAILED",
          completed_at: nowIso(),
          provider_actual_cost_usd: actual,
          total_internal_cost_usd: actual,
          error_code: "provider_failed_or_empty",
          customer_safe_error: "Research could not be completed.",
          raw_final_artifact_id: collected
            ? repo.writeArtifact(hotelId, {
                artifact_id: `art_${crypto.randomBytes(4).toString("hex")}`,
                kind: "RAW_RESEARCH",
                provider: PROVIDER_IDS.WEBHOUND,
                provider_run_id: sessionId,
                payload: collected.raw_artifact,
              }).artifact_id
            : request.raw_artifact_id,
        });
        clearLiveTick(requestId);
        appendResearchAuditEvent(
          {
            event: "webhound_failed",
            hotel_id: hotelId,
            template_id: request.template_id,
            request_id: requestId,
            provider: "WEBHOUND",
            provider_run_id: sessionId,
            budget_usd: request.provider_budget_usd,
            actual_cost_usd: actual,
            status: "FAILED",
          },
          env
        );
        return { request, run, export_links: resolveReportExportLinks(request) };
      }

      // Complete collection
      repo.updateRequest(hotelId, requestId, { status: "NORMALIZING" });
      const collected = await provider.collectCompleted(sessionId);
      const rawArt = repo.writeArtifact(hotelId, {
        artifact_id: `art_${crypto.randomBytes(4).toString("hex")}`,
        kind: "RAW_RESEARCH",
        provider: PROVIDER_IDS.WEBHOUND,
        provider_run_id: sessionId,
        simulated: false,
        payload: collected.raw_artifact,
      });

      const normalized = normalizeResearchArtifact({
        raw: collected.raw_artifact,
        template,
        request: { ...request, provider_run_id: sessionId },
        hotel_seed: buildHotelIdentitySeed({
          hotel_id: hotelId,
          hotel_name: request.hotel_name,
        }),
      });

      repo.updateRequest(hotelId, requestId, { status: "VALIDATING" });
      const normArt = repo.writeArtifact(hotelId, {
        artifact_id: normalized.normalized_research_id,
        kind: "NORMALIZED_RESEARCH",
        simulated: false,
        payload: normalized,
      });

      repo.updateRequest(hotelId, requestId, { status: "GENERATING_REPORT" });
      const actual = collected.provider_cost_usd;
      settleSpend(request, actual);

      const dossier = buildResearchAddendumDossier({
        template,
        request: {
          ...request,
          provider_run_id: sessionId,
          provider_actual_cost_usd: actual,
          raw_artifact_id: rawArt.artifact_id,
        },
        normalized: { ...normalized, raw_ref: rawArt.artifact_id },
        run_id: run?.run_id,
        env,
        persist: true,
      });

      // Completion integrity
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

      repo.updateRequest(hotelId, requestId, { status: "GENERATING_PDF" });
      // PDF is generated on-demand via R9 endpoint; mark pdf_id = dossier_id.
      const terminalStatus =
        (dossier.open_question_count || 0) > 0
          ? "COMPLETED_WITH_OPEN_QUESTIONS"
          : dossier.source_count === 0 && normalized.explicit_no_evidence_validated
            ? "PARTIAL"
            : "COMPLETED";

      request = repo.updateRequest(hotelId, requestId, {
        status: terminalStatus,
        completed_at: nowIso(),
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
        claim_handoff: { auto_promote: false },
      });

      if (run) {
        run = repo.updateRun(hotelId, run.run_id, {
          completed_at: nowIso(),
          provider_status: terminalStatus,
          actual_cost: actual,
          raw_artifact_id: rawArt.artifact_id,
          report_version: template.version,
          simulated: false,
        });
      }

      // Open-question resolution lineage (data support)
      linkOpenQuestionResolutions(hotelId, request, dossier);

      clearLiveTick(requestId);
      appendResearchAuditEvent(
        {
          event: "webhound_completed",
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
        },
        env
      );

      return {
        request,
        run,
        export_links: resolveReportExportLinks(request),
        customer_stage: customerStageLabel(terminalStatus),
        dossier_id: dossier.dossier_id,
      };
    } catch (err) {
      console.error("[hi-research] advanceLiveRequest", err);
      // Do not auto-retry paid run. Mark failed unless still running mid-flight.
      if (!isTerminalStatus(request.status)) {
        request = repo.updateRequest(hotelId, requestId, {
          status: "PARTIAL",
          completed_at: nowIso(),
          error_code: err.code || "live_advance_error",
          error_message: String(err.message || err).slice(0, 200),
          customer_safe_error: err.customer_safe || "Research finished with errors.",
        });
      }
      clearLiveTick(requestId);
      return {
        request,
        run,
        export_links: resolveReportExportLinks(request),
        customer_stage: customerStageLabel(request.status),
      };
    }
  }

  function settleSpend(request, actualCost) {
    const reserved = Number(request.provider_budget_usd || PILOT_MAX_PROVIDER_BUDGET_USD);
    const spent = actualCost != null && Number.isFinite(Number(actualCost)) ? Number(actualCost) : null;
    if (spent != null) {
      recordExternalSpend(
        {
          kind: "settle_reserve",
          amount_usd: spent,
          reserved_usd: reserved,
          hotel_id: request.hotel_id,
          request_id: request.request_id,
          template_id: request.template_id,
          provider_run_id: request.provider_run_id,
        },
        env
      );
    } else {
      // Cost unknown — release reserve without inventing spend
      recordExternalSpend(
        {
          kind: "release_reserve",
          amount_usd: reserved,
          hotel_id: request.hotel_id,
          request_id: request.request_id,
          template_id: request.template_id,
        },
        env
      );
    }
  }

  function linkOpenQuestionResolutions(hotelId, request, dossier) {
    try {
      const idx = repo.readIndex ? repo.readIndex(hotelId) : null;
      // repository may not expose readIndex — use listHotelResearch
      const data = repo.listHotelResearch(hotelId);
      const lineage = data.open_question_lineage || [];
      const parent = data.requests.find((r) => r.request_id === "req_kgpv_full_hi_run1");
      if (!parent) return;
      // Store resolution stubs for any matching open questions by keyword
      const resolutions = (dossier.open_questions || []).map((q) => ({
        open_question_id: q.id,
        resolved_by_run_id: request.provider_run_id,
        resolved_by_request_id: request.request_id,
        report_id: dossier.dossier_id,
        at: nowIso(),
      }));
      if (typeof repo.writeIndex === "function") {
        const full = repo.listHotelResearch(hotelId);
        // Soft-append via updateRequest metadata on the new request
        repo.updateRequest(hotelId, request.request_id, {
          resolves_open_questions: resolutions,
          open_question_lineage_note: "Data support for Resolved-by Addendum UX",
        });
      }
      void lineage;
    } catch (err) {
      console.warn("[hi-research] open_question_lineage", err.message);
    }
  }

  async function advanceSimulation(hotelId, requestId, optsAdvance = {}) {
    let request = repo.getRequest(hotelId, requestId);
    if (!request) {
      const err = new Error("request_not_found");
      err.code = "request_not_found";
      throw err;
    }
    if (request.immutable_historical) {
      return { request, run: null, export_links: resolveReportExportLinks(request) };
    }
    if (isTerminalStatus(request.status) && !optsAdvance.force) {
      const runs = repo.listHotelResearch(hotelId).runs.filter((r) => r.request_id === requestId);
      return {
        request,
        run: runs[runs.length - 1] || null,
        export_links: resolveReportExportLinks(request),
      };
    }

    const template = getTemplate(request.template_id);
    const runs = repo.listHotelResearch(hotelId).runs.filter((r) => r.request_id === requestId);
    let run = runs[runs.length - 1];
    const toCompletion = optsAdvance.to_completion !== false;
    let guard = 0;
    while (isActiveStatus(request.status) && guard < 12) {
      guard += 1;
      const next = nextSimulationStatus(request.status);
      const patch = { status: next };
      if (next === "RUNNING" && !request.started_at) patch.started_at = nowIso();

      if (next === "RUNNING" || (request.status === "QUEUED" && next === "RUNNING")) {
        const provider = resolveProvider("SIMULATION", { env });
        try {
          const result = await provider.execute({
            hotel_seed: { hotel_id: hotelId, hotel_name: request.hotel_name, id: hotelId },
            template,
            budget_usd: request.provider_budget_usd,
            request_id: request.request_id,
            run_id: run?.run_id,
            authorized: false,
          });
          const artifact = repo.writeArtifact(hotelId, {
            artifact_id: `art_${crypto.randomBytes(4).toString("hex")}`,
            kind: "RAW_RESEARCH",
            provider: provider.id,
            provider_run_id: result.provider_run_id,
            simulated: true,
            payload: result.raw_artifact,
          });
          if (run) {
            run = repo.updateRun(hotelId, run.run_id, {
              provider_run_id: result.provider_run_id,
              provider_status: result.status,
              actual_cost: 0,
              raw_artifact_id: artifact.artifact_id,
            });
          }
          patch.provider_run_id = result.provider_run_id;
          patch.raw_artifact_id = artifact.artifact_id;
          patch.provider_actual_cost_usd = 0;
          patch.total_internal_cost_usd = 0;
          patch._raw = result.raw_artifact;
        } catch (err) {
          request = repo.updateRequest(hotelId, requestId, {
            status: "FAILED",
            completed_at: nowIso(),
            error_code: err.code || "provider_failed",
            customer_safe_error: err.customer_safe || "Research could not be completed.",
          });
          return {
            request,
            run: repo.listHotelResearch(hotelId).runs.filter((r) => r.request_id === requestId).pop(),
            export_links: resolveReportExportLinks(request),
          };
        }
      }

      if (next === "GENERATING_REPORT" || next === "SIMULATED" || next === "COMPLETED") {
        const normalized = normalizeSimulatedArtifact(patch._raw || null, template, request);
        const artNorm = repo.writeArtifact(hotelId, {
          artifact_id: normalized.normalized_research_id,
          kind: "NORMALIZED_RESEARCH",
          simulated: true,
          is_simulation: true,
          payload: normalized,
        });
        patch.normalized_research_id = artNorm.artifact_id;
        patch.report_id = null;
        patch.pdf_id = null;
        patch.source_count = 0;
        patch.finding_count = 0;
        patch.open_question_count = 0;
        patch.simulated = true;
        patch.is_simulation = true;
        patch.completed_at = nowIso();
        patch.status = "SIMULATED";
        if (run) {
          run = repo.updateRun(hotelId, run.run_id, {
            completed_at: nowIso(),
            provider_status: "SIMULATED",
            simulated: true,
            is_simulation: true,
          });
        }
      }

      delete patch._raw;
      request = repo.updateRequest(hotelId, requestId, patch);
      if (!toCompletion) break;
      if (isTerminalStatus(request.status)) break;
    }

    return {
      request,
      run: repo.listHotelResearch(hotelId).runs.filter((r) => r.request_id === requestId).pop() || null,
      export_links: resolveReportExportLinks(request),
      customer_stage: customerStageLabel(request.status),
      mode: "SIMULATION",
    };
  }

  async function advanceRequest(hotelId, requestId, optsAdvance = {}) {
    const request = repo.getRequest(hotelId, requestId);
    if (!request) {
      const err = new Error("request_not_found");
      err.code = "request_not_found";
      throw err;
    }
    if (!request.simulated && !request.is_simulation && request.provider_strategy === "WEBHOUND") {
      return advanceLiveRequest(hotelId, requestId);
    }
    return advanceSimulation(hotelId, requestId, optsAdvance);
  }

  async function tickActiveSimulations(hotelId) {
    const active = repo.listActiveRequests(hotelId);
    const out = [];
    for (const r of active) {
      if (r.simulated || r.is_simulation) {
        out.push(await advanceSimulation(hotelId, r.request_id, { to_completion: false }));
      } else {
        out.push(await advanceLiveRequest(hotelId, r.request_id));
      }
    }
    return out;
  }

  return {
    repository: repo,
    createResearchRequest,
    advanceRequest,
    advanceLiveRequest,
    tickActiveSimulations,
    ensureKgpvRun1Backfill: () => ensureKgpvRun1Backfill(repo),
    isLiveEnabled: () => isExternalResearchEnabled(env),
    internalDebug: () => isFounderInternalDebug(env),
  };
}
