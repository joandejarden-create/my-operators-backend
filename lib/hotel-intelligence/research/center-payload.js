/**
 * Packet 2.6C — Research Center customer payload builder.
 * Packet 2.6C-R1: live archive excludes simulations; actions require real reports.
 */

import { getDossierForHotelRecord } from "../dossier/index.js";
import { listTemplates, getTemplate, FOLLOW_UP_TEMPLATE_IDS, customerTemplateView } from "./templates.js";
import { recommendFollowUps } from "./recommend.js";
import { resolveReportExportLinks, reportTypeLabel } from "./report-export-contract.js";
import { isActiveStatus, customerStageLabel } from "./statuses.js";
import { ensureKgpvRun1Backfill, KGPV_HOTEL_ID } from "./backfill-kgpv.js";
import {
  ensureMexicoExplorerRun1Backfill,
  isMexicoExplorerFullHiHotel,
} from "./backfill-mexico-explorer.js";
import { ensureCompletedDossierArchiveBackfill } from "./backfill-completed-dossiers.js";
import { createResearchRepository } from "./repository.js";
import { createResearchOrchestrator } from "./orchestrator.js";
import {
  canExposeDownloadPdf,
  canExposeViewReport,
  filterLiveActiveRequests,
  filterLiveArchiveRequests,
  isSimulationRequest,
} from "./archive-integrity.js";
import { materializeResearchArchive } from "./archive-materializer.js";
import {
  isExternalResearchEnabled,
  isFounderInternalDebug,
  getPilotMaxProviderBudgetUsd,
  getDailyExternalBudgetUsd,
  getMaxConcurrentWebhoundRuns,
} from "./policy.js";

function formatDisplayDate(iso) {
  if (!iso) return null;
  try {
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return null;
    return d.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
  } catch {
    return null;
  }
}

function stripInternalCost(request, { includeAdmin = false, omitRequestId = false } = {}) {
  const exportLinks = resolveReportExportLinks(request);
  const template = getTemplate(request.template_id);
  const labels = reportTypeLabel(request.report_type, template?.display_name || request.display_name);
  const viewOk = canExposeViewReport(request);
  const pdfOk = canExposeDownloadPdf(request);
  const base = {
    request_id: omitRequestId ? undefined : request.request_id,
    template_id: request.template_id,
    template_version: request.template_version,
    display_name:
      template?.display_name || request.display_name || request.template_id,
    question: request.question,
    scope: request.scope,
    scope_groups: template?.scope_groups || null,
    status: request.status,
    status_label: customerStageLabel(request.status),
    created_at: request.created_at,
    started_at: request.started_at,
    completed_at: request.completed_at,
    completed_display: formatDisplayDate(request.completed_at),
    source_count: request.source_count,
    finding_count: request.finding_count,
    open_question_count: request.open_question_count,
    report_id: viewOk ? request.report_id : null,
    pdf_id: pdfOk ? request.pdf_id || request.report_id : null,
    report_type: request.report_type,
    report_labels: labels,
    parent_report_id: request.parent_report_id,
    parent_report_label: request.parent_report_id
      ? "Full Hotel Intelligence Investigation"
      : null,
    parent_completed_display: request.parent_report_id === "dossier_kgpv_full_hi_v1" ? "Sep 4, 2026" : null,
    simulated: Boolean(request.simulated || request.is_simulation),
    is_simulation: isSimulationRequest(request),
    immutable_historical: Boolean(request.immutable_historical),
    export: exportLinks,
    actions: {
      view_report: viewOk,
      download_pdf: pdfOk,
    },
    customer_safe_error: request.customer_safe_error || null,
    is_active: isActiveStatus(request.status),
    is_terminal: !isActiveStatus(request.status),
    resolves_open_questions: request.resolves_open_questions || null,
  };
  if (omitRequestId) {
    delete base.request_id;
  }
  if (includeAdmin) {
    base.admin = {
      provider_strategy: request.provider_strategy,
      provider_run_id: request.provider_run_id,
      provider_budget_usd: request.provider_budget_usd,
      provider_actual_cost_usd: request.provider_actual_cost_usd,
      total_internal_cost_usd: request.total_internal_cost_usd,
      raw_artifact_id: request.raw_artifact_id,
      normalized_research_id: request.normalized_research_id,
      error_code: request.error_code,
      lineage: request.lineage,
      estimated_research_units: request.estimated_research_units,
      pricing_tier_key: request.pricing_tier_key,
      is_simulation: isSimulationRequest(request),
    };
  }
  return base;
}

/** Strip internal identifiers / cost leftovers from a card for share mode. */
function toShareSafeCard(card) {
  if (!card) return null;
  const {
    request_id: _rid,
    admin: _admin,
    simulated: _sim,
    is_simulation: _isim,
    customer_safe_error: _err,
    export: exportLinks,
    ...rest
  } = card;
  const safeExport = exportLinks
    ? {
        contract_version: exportLinks.contract_version,
        report_kind: exportLinks.report_kind,
        report_id: exportLinks.report_id,
        hotel_id: exportLinks.hotel_id,
        parent_report_id: exportLinks.parent_report_id,
        view_report: exportLinks.view_report,
        download_pdf: exportLinks.download_pdf,
        pdf_endpoint: exportLinks.pdf_endpoint,
      }
    : null;
  return {
    ...rest,
    export: safeExport,
  };
}

function projectClientSafeSharePayload(payload) {
  const archive = (payload.archive || []).map(toShareSafeCard).filter(Boolean);
  const latest = toShareSafeCard(payload.latest_investigation);
  const completed = (payload.completed_reports || []).map(toShareSafeCard).filter(Boolean);
  return {
    ok: true,
    product: "Dealality Research Reports",
    hotel: payload.hotel,
    header: {
      title: "Research Reports",
      subheading: "Completed Reports",
      status_line: {
        completed_investigations: payload.header?.status_line?.completed_investigations ?? archive.length,
        active: 0,
        last_researched: payload.header?.status_line?.last_researched || null,
        last_researched_display: payload.header?.status_line?.last_researched_display || null,
      },
    },
    mode: payload.mode,
    mode_flags: { share_readonly: true },
    first_use: null,
    latest_investigation: latest,
    active_requests: [],
    active_runs: [],
    completed_reports: completed,
    failed_or_partial_runs: [],
    latest_research_at: payload.latest_research_at || null,
    recommended_follow_up: [],
    research_more: [],
    archive,
    templates: [],
    badge: payload.badge,
    governance: {
      claim_handoff_auto_promote: false,
      external_research_enabled: false,
      external_research_default: false,
      customer_pricing_exposed: false,
      simulations_excluded_from_live_archive: true,
      live_execution_requires_explicit_click: true,
      show_internal_research_costs: false,
      share_readonly: true,
    },
    // Omitted: execution_mode, internal_research_controls (share must not expose SIMULATION)
    integrity: {
      live_archive_count: archive.length,
      simulation_runs_visible: 0,
      broken_report_links: archive.filter((a) => a.actions?.view_report && !a.report_id).length,
    },
  };
}

export function buildResearchCenterPayload(input = {}) {
  const hotelId = String(input.hotel_id || input.hotelId || "").trim();
  const hotelName = input.hotel_name || input.hotelName || null;
  const env = input.env || process.env;
  const clientSafeShare = Boolean(input.clientSafeShare);
  const includeAdmin = clientSafeShare ? false : Boolean(input.includeAdmin);
  const showInternalCosts = includeAdmin || isFounderInternalDebug(env);
  const includeSimulations = Boolean(input.includeSimulations);
  const repo = input.repository || createResearchRepository(input);

  if (hotelId === KGPV_HOTEL_ID) {
    ensureKgpvRun1Backfill(repo);
  }
  if (isMexicoExplorerFullHiHotel(hotelId)) {
    ensureMexicoExplorerRun1Backfill(repo, hotelId);
  }
  // Global path: any completed client-visible dossier → archive (Cambridge + future hotels).
  if (hotelId) {
    ensureCompletedDossierArchiveBackfill(repo, hotelId);
  }

  const research = repo.listHotelResearch(hotelId);
  const dossier = getDossierForHotelRecord(hotelId);
  const requests = research.requests.slice().sort((a, b) => {
    const ta = String(b.completed_at || b.created_at || "");
    const tb = String(a.completed_at || a.created_at || "");
    return ta.localeCompare(tb);
  });

  const materialized = materializeResearchArchive(hotelId, {
    repo,
    env,
    includeSimulations,
  });
  const liveArchive = filterLiveArchiveRequests(requests, { includeSimulations });
  const active = filterLiveActiveRequests(requests, { includeSimulations });
  // Prefer materializer count for header consistency (deduped projection).
  const completedCount = materialized.count;
  const activeCount = active.length;

  const completedFull = liveArchive.find((r) => r.template_id === "FULL_HOTEL_INTELLIGENCE");
  const hasFull = Boolean(completedFull) || Boolean(dossier);

  const openQuestions = dossier?.open_questions || [];
  const completedTemplateIds = liveArchive.map((r) => r.template_id);
  const activeTemplateIds = active.map((r) => r.template_id);

  const rec = recommendFollowUps({
    open_questions: openQuestions,
    completed_template_ids: completedTemplateIds,
    active_template_ids: activeTemplateIds,
    has_full_investigation: hasFull,
    origin_report_id: completedFull?.report_id || dossier?.dossier_id || null,
  });

  if (rec.recommendations.length) {
    const lineage = [];
    for (const card of rec.recommendations) {
      for (const q of card.matched_open_questions || []) {
        lineage.push({
          open_question_id: q.open_question_id,
          origin_report_id: q.origin_report_id,
          origin_finding_id: q.origin_finding_id,
          recommended_template_id: card.template?.template_id || null,
          resolved_by_run_id: null,
        });
      }
    }
    repo.upsertOpenQuestionLineage(hotelId, lineage);
  }

  const stripOpts = { includeAdmin, omitRequestId: clientSafeShare };
  const latest = completedFull
    ? stripInternalCost(completedFull, stripOpts)
    : liveArchive[0]
      ? stripInternalCost(liveArchive[0], stripOpts)
      : null;

  if (latest && dossier && latest.template_id === "FULL_HOTEL_INTELLIGENCE") {
    latest.metrics = {
      source_count: dossier.source_count ?? latest.source_count,
      finding_count: dossier.finding_count ?? latest.finding_count,
      open_question_count: dossier.open_question_count ?? latest.open_question_count,
    };
    latest.scope = latest.scope || [
      "Ownership",
      "Corporate Structure",
      "Brand & Operator",
      "People",
      "Development Intelligence",
    ];
    latest.actions = {
      view_report: true,
      download_pdf: true,
    };
    latest.report_id = dossier.dossier_id;
    latest.pdf_id = dossier.dossier_id;
  }

  const lastResearched =
    liveArchive
      .map((r) => r.completed_at)
      .filter(Boolean)
      .sort()
      .reverse()[0] || null;

  const archive = liveArchive.map((r) => stripInternalCost(r, stripOpts));

  const researchMore = FOLLOW_UP_TEMPLATE_IDS.map((id) => customerTemplateView(getTemplate(id)));

  const payload = {
    ok: true,
    product: "Dealality Deep Research",
    hotel: {
      hotel_id: hotelId,
      hotel_name: hotelName || completedFull?.hotel_name || dossier?.hotel_name || null,
    },
    header: {
      title: "Deep Research",
      subheading: "Research Center",
      status_line: {
        completed_investigations: completedCount,
        active: activeCount,
        last_researched: lastResearched,
        last_researched_display: formatDisplayDate(lastResearched),
      },
    },
    mode: hasFull ? "AFTER_FULL_INVESTIGATION" : "FIRST_USE_FULL_INVESTIGATION",
    mode_flags: { share_readonly: false },
    first_use: !hasFull
      ? {
          title: "Start with a Full Hotel Intelligence Investigation",
          description:
            "Conduct a comprehensive evidence-backed investigation covering ownership, corporate structure, operator and brand relationships, history, key people, portfolio connections, material changes and development implications.",
          template_id: "FULL_HOTEL_INTELLIGENCE",
          action: "run_full_investigation",
        }
      : null,
    latest_investigation: latest,
    active_requests: active.map((r) => ({
      ...stripInternalCost(r, stripOpts),
      stage_label: customerStageLabel(r.status),
    })),
    active_runs: active.map((r) => ({
      ...stripInternalCost(r, stripOpts),
      stage_label: customerStageLabel(r.status),
    })),
    completed_reports: liveArchive.map((r) => stripInternalCost(r, stripOpts)),
    failed_or_partial_runs: requests
      .filter(
        (r) =>
          !isSimulationRequest(r) &&
          (r.status === "FAILED" || r.status === "PARTIAL" || r.status === "CANCELLED")
      )
      .map((r) => stripInternalCost(r, { includeAdmin: true })),
    latest_research_at: lastResearched,
    recommended_follow_up: rec.recommendations,
    research_more: researchMore,
    archive,
    templates: listTemplates({ includeInternal: includeAdmin }),
    badge: hasFull
      ? {
          label: liveArchive.length === 1 ? "1 Report" : `${liveArchive.length} Reports`,
          state: "Completed",
        }
      : null,
    governance: {
      claim_handoff_auto_promote: false,
      external_research_enabled: isExternalResearchEnabled(env),
      external_research_default: false,
      customer_pricing_exposed: false,
      simulations_excluded_from_live_archive: !includeSimulations,
      live_execution_requires_explicit_click: true,
      show_internal_research_costs: showInternalCosts,
      // Customer payloads never include provider dollar amounts.
      ...(showInternalCosts
        ? {
            max_provider_budget_usd: getPilotMaxProviderBudgetUsd(env),
            daily_external_budget_usd: getDailyExternalBudgetUsd(env),
            max_concurrent_webhound_runs: getMaxConcurrentWebhoundRuns(env),
            automatic_paid_retries: false,
          }
        : {}),
    },
    execution_mode: {
      live: isExternalResearchEnabled(env),
      label: isExternalResearchEnabled(env)
        ? "LIVE RESEARCH ENABLED"
        : "SIMULATION",
      internal_budget_hint: showInternalCosts && isExternalResearchEnabled(env)
        ? `Maximum provider budget: $${getPilotMaxProviderBudgetUsd(env).toFixed(2)}`
        : null,
    },
    internal_research_controls: showInternalCosts
      ? {
          visible: true,
          provider_maximum_usd: getPilotMaxProviderBudgetUsd(env),
          daily_external_budget_usd: getDailyExternalBudgetUsd(env),
          automatic_paid_retries: false,
          max_concurrent_runs: getMaxConcurrentWebhoundRuns(env),
        }
      : { visible: false },
    integrity: {
      live_archive_count: liveArchive.length,
      simulation_runs_visible: includeSimulations
        ? requests.filter((r) => isSimulationRequest(r)).length
        : 0,
      broken_report_links: archive.filter((a) => a.actions?.view_report && !a.report_id).length,
    },
  };

  if (clientSafeShare) {
    return projectClientSafeSharePayload(payload);
  }
  return payload;
}

export async function buildResearchCenterPayloadAsync(input = {}) {
  const hotelId = String(input.hotel_id || input.hotelId || "").trim();
  const repo = input.repository || createResearchRepository(input);
  const orchestrator = createResearchOrchestrator({ repository: repo, env: input.env });
  if (hotelId === KGPV_HOTEL_ID) ensureKgpvRun1Backfill(repo);
  if (isMexicoExplorerFullHiHotel(hotelId)) ensureMexicoExplorerRun1Backfill(repo, hotelId);
  if (hotelId) ensureCompletedDossierArchiveBackfill(repo, hotelId);
  if (input.tick !== false) {
    await orchestrator.tickActiveSimulations(hotelId);
  }
  // Resume incomplete post-provider pipelines (never starts a new paid provider run).
  if (input.resume_orphans !== false && hotelId) {
    try {
      const { resumeIncompleteResearchPipelines } = await import("./orphan-recovery.js");
      await resumeIncompleteResearchPipelines(hotelId, {
        repo,
        env: input.env || process.env,
      });
    } catch (err) {
      console.error("[hi-research] orphan_resume", err?.code || err?.message || err);
    }
  }
  return buildResearchCenterPayload({ ...input, repository: repo, tick: false });
}
