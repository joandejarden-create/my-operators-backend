/**
 * Weekly refresh orchestrator — coverage backlog + refresh queue.
 * Integrates segment plans, feedback, change detection, and publication policy.
 * Does not enable unattended paid spend. Auto-publish OFF by default.
 */

import { createHash, randomBytes } from "node:crypto";
import {
  buildHotelSegmentResearchPlan,
  loadHotelConfig,
  RESEARCH_COVERAGE,
  RESEARCH_SEGMENT,
  APPLICABILITY,
} from "./hotel-segment-research-plan-v1.js";
import { loadFeedback } from "../repository.js";
import { listPursuits } from "../pursuit/pursuit-store-v1.js";
import {
  WEEKLY_REFRESH_CONTRACT_VERSION,
  RUN_MODE,
  RUN_TRIGGER,
  RUN_STATUS,
  CHANGE_CLASS,
  QUEUE_KIND,
  PUBLICATION_POLICY,
  classifyRefreshChange,
  resolveCadenceDays,
  isCheckDue,
  mayAutoPublish,
  feedbackToResearchTask,
} from "./weekly-refresh-contract-v1.js";
import {
  loadWeeklyRefreshState,
  saveWeeklyRefreshState,
  acquireWeeklyRefreshLock,
  releaseWeeklyRefreshLock,
  persistRunJournal,
  advanceFreshness,
  recordFailedCheck,
  markRunStatus,
} from "./weekly-refresh-state-v1.js";
import {
  createEmptyScorecardRow,
  finalizeScorecardEfficiency,
  writeScorecardArtifacts,
  EXECUTION_MODE,
  RESEARCH_SCORECARD_VERSION,
  PRIMARY_DISPOSITION,
} from "./research-scorecard-v1.js";
import {
  assessHotelFitForOpportunity,
  applyHotelFitAssessmentToOpportunity,
  HOTEL_FIT_ASSESSMENT_VERSION,
} from "./hotel-fit-assessment-v1.js";
import {
  buildContactDisplaySurface,
  applyContactDisplaySurfaceToOpportunity,
} from "../contact-display-surface-v1.js";
import { buildBuyerContactEnrichmentQueue } from "./buyer-contact-enrichment-v1.js";
import {
  buildHotelSegmentResearchBriefs,
  selectContinuationPriorityBriefs,
} from "./hotel-segment-research-brief-v1.js";
import { adaptPeCandidatesBatch } from "./pe-provider-to-canonical-adapter-v1.js";
import {
  packagesFromDispositions,
  persistCorporatePackageDocument,
} from "./corporate-package-persist-v1.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __wrDirname = path.dirname(fileURLToPath(import.meta.url));
const WR_REPO_ROOT = path.resolve(__wrDirname, "../../..");

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function weeklyArtifactsDir(hotelId, runId, env = process.env) {
  if (env.GDI_WEEKLY_REFRESH_ARTIFACTS_DIR) {
    return path.join(
      path.resolve(env.GDI_WEEKLY_REFRESH_ARTIFACTS_DIR),
      hotelId,
      runId || "run"
    );
  }
  const root = env.GDI_DATA_ROOT
    ? path.resolve(env.GDI_DATA_ROOT)
    : path.join(WR_REPO_ROOT, "data/group-demand-intelligence");
  return path.join(
    root,
    "hotels",
    hotelId,
    "weekly-refresh",
    "runs",
    runId || "run"
  );
}

function accountKey(opp = {}) {
  const org = text(opp.organizationName || opp.company).toLowerCase();
  const domain = text(opp.officialDomain || opp.website)
    .toLowerCase()
    .replace(/^https?:\/\//, "")
    .replace(/^www\./, "")
    .split("/")[0];
  return domain ? `d:${domain}` : `n:${org}`;
}

function opportunityIdentity(opp = {}) {
  if (opp.id || opp.opportunityId) return String(opp.id || opp.opportunityId);
  const seed = `${opp.hotelId || ""}|${accountKey(opp)}|${opp.eventStartDate || "standing"}|${opp.researchSegment || opp.opportunityType || ""}`;
  return `oid_${createHash("sha256").update(seed).digest("hex").slice(0, 16)}`;
}

/**
 * Build dual queues from segment coverage + freshness + feedback.
 */
export function buildWeeklyWorkQueues({
  hotelId,
  hotelConfig = null,
  opportunities = [],
  state = null,
  now = new Date(),
  maxCoverageTasks = 6,
  maxRefreshTasks = 8,
  maxFeedbackTasks = 4,
} = {}) {
  const cfg = hotelConfig || loadHotelConfig(hotelId) || { hotelId };
  const st = state || loadWeeklyRefreshState(hotelId);
  const plan = buildHotelSegmentResearchPlan({
    hotelId,
    hotelConfig: cfg,
    opportunities,
    artifacts: {
      paidBudgetApproved: false,
      coverageStatusBySegment: st.segmentScopeComplete || {},
    },
  });

  const coverageBacklog = [];
  const refreshQueue = [];
  const feedbackQueue = [];
  const deferred = [];

  for (const seg of plan.segments || []) {
    if (seg.applicability !== APPLICABILITY.APPLICABLE) continue;

    const incomplete =
      seg.researchCoverage === RESEARCH_COVERAGE.NOT_YET_RESEARCHED ||
      seg.researchCoverage === RESEARCH_COVERAGE.PARTIALLY_RESEARCHED ||
      seg.researchCoverage === RESEARCH_COVERAGE.PARTIAL ||
      seg.researchCoverage === RESEARCH_COVERAGE.NOT_STARTED;

    if (incomplete) {
      const task = {
        queueKind: QUEUE_KIND.COVERAGE_BACKLOG,
        segmentId: seg.segmentId,
        label: seg.label,
        researchCoverage: seg.researchCoverage,
        rationale: `Initial coverage incomplete: ${seg.coverageRationale || seg.researchCoverage}`,
        priority:
          seg.segmentId === RESEARCH_SEGMENT.SCALING_COMPANIES ||
          seg.segmentId === RESEARCH_SEGMENT.LOCAL_MEETINGS_CATERING ||
          seg.segmentId === RESEARCH_SEGMENT.WEDDINGS_SOCIAL_PRIVATE_EVENTS
            ? "HIGH"
            : "MEDIUM",
        cadenceKind: "coverage_gap",
        selectedReason: "applicable_segment_not_adequately_researched",
        ...(seg.segmentId === RESEARCH_SEGMENT.LOCAL_MEETINGS_CATERING
          ? {
              lmcSubsegmentNote:
                "Refresh checks new programs, dates, contact changes, and material buying signals across applicable LMC subsegments. Do not re-label standing accounts as new discoveries. Preferred lodging with outside venues stays on Private Events.",
              applicableLmcSubsegmentIds:
                seg.applicableLmcSubsegmentIds || [],
            }
          : {}),
      };
      coverageBacklog.push(task);
    } else {
      const fresh = st.segmentFreshness?.[seg.segmentId];
      const cadenceDays = resolveCadenceDays(
        { cadenceKind: "defaultSegmentRefresh", queueKind: QUEUE_KIND.REFRESH },
        cfg
      );
      if (
        isCheckDue({
          lastSuccessfulCheckAt: fresh?.lastSuccessfulCheckAt,
          cadenceDays,
          now,
        })
      ) {
        refreshQueue.push({
          queueKind: QUEUE_KIND.REFRESH,
          segmentId: seg.segmentId,
          label: seg.label,
          rationale: `Segment refresh due (cadence ${cadenceDays}d)`,
          priority: "MEDIUM",
          cadenceDays,
          selectedReason: "segment_refresh_due",
        });
      } else {
        deferred.push({
          segmentId: seg.segmentId,
          reason: "not_due_yet",
          nextEligibleAt: fresh?.lastSuccessfulCheckAt
            ? new Date(
                new Date(fresh.lastSuccessfulCheckAt).getTime() +
                  cadenceDays * 86400000
              ).toISOString()
            : null,
        });
      }
    }
  }

  // Account-level refresh for existing opportunities
  for (const opp of opportunities || []) {
    const key = accountKey(opp);
    const fresh = st.accountFreshness?.[key];
    const cadenceDays = resolveCadenceDays(
      {
        cadenceKind: opp.standingOpportunity
          ? "standingAccountRefresh"
          : opp.eventStartDate
            ? "upcomingDatedOccasion"
            : "defaultSegmentRefresh",
        hasUpcomingDatedOccasion: Boolean(opp.eventStartDate),
      },
      cfg
    );
    if (
      !isCheckDue({
        lastSuccessfulCheckAt: fresh?.lastSuccessfulCheckAt,
        cadenceDays,
        now,
      })
    ) {
      continue;
    }
    // Prefer contact enrichment when contact weak OR named-buyer research incomplete
    // (functional route alone must not mark named research complete)
    const weakContact =
      !opp.contactEmail && !opp.contactPhone && !opp.primaryContactEmail;
    const namedPending =
      !opp.primaryContact?.name &&
      (opp.contactEmail ||
        opp.functionalContactEmail ||
        opp.primaryContactEmail ||
        opp.contactPhone);
    const needsContactEnrichment = weakContact || namedPending;
    refreshQueue.push({
      queueKind: needsContactEnrichment
        ? QUEUE_KIND.CONTACT_ENRICHMENT
        : QUEUE_KIND.REFRESH,
      opportunityId: opportunityIdentity(opp),
      accountKey: key,
      organizationName: opp.organizationName || opp.company || null,
      segmentId: opp.researchSegment || null,
      rationale: weakContact
        ? "Existing prospect missing usable contact — enrichment due"
        : namedPending
          ? "Usable functional route present; named-buyer research not yet established"
          : `Account refresh due (cadence ${cadenceDays}d)`,
      priority: weakContact ? "HIGH" : namedPending ? "HIGH" : "MEDIUM",
      cadenceDays,
      selectedReason: weakContact
        ? "contact_enrichment_due"
        : namedPending
          ? "named_buyer_enrichment_pending"
          : "account_refresh_due",
    });
  }

  // Feedback-driven tasks
  const feedbackDoc = loadFeedback(hotelId) || {};
  const items = []
    .concat(feedbackDoc.items || [])
    .concat(feedbackDoc.feedback || [])
    .concat(Array.isArray(feedbackDoc) ? feedbackDoc : []);
  for (const item of items) {
    const task = feedbackToResearchTask(item);
    if (!task) continue;
    feedbackQueue.push({
      ...task,
      opportunityId: item.opportunityId || item.id || null,
      organizationName: item.organizationName || null,
      feedbackSource: "hotel_feedback_store",
      selectedReason: task.rationale,
    });
  }

  // Pursuit preservation signals
  let pursuits = [];
  try {
    pursuits = listPursuits(hotelId) || [];
  } catch {
    pursuits = [];
  }
  for (const p of pursuits) {
    if (/BOOKED|WON|ACTIVE|PURSUING/i.test(String(p.pursuitStatus || ""))) {
      feedbackQueue.push({
        queueKind: QUEUE_KIND.FEEDBACK_FOLLOWUP,
        opportunityId: p.opportunityId || null,
        preservePursuitState: true,
        suppressAutomatedRecommendations: true,
        priority: "LOW",
        rationale: "Active pursuit — preserve hotel state",
        selectedReason: "active_pursuit_preserve",
      });
    }
  }

  // Pending retries from prior failures
  for (const r of st.pendingRetries || []) {
    refreshQueue.push({
      queueKind: QUEUE_KIND.RETRY,
      segmentId: r.segmentId,
      accountKey: r.accountKey,
      rationale: `Retry prior failed check: ${r.reason}`,
      priority: "HIGH",
      selectedReason: "pending_retry",
      lastRetrievalFailed: true,
    });
  }

  // Sort: do not spend entire budget on associations alone
  const segmentPriority = (id) => {
    const order = [
      RESEARCH_SEGMENT.LOCAL_MEETINGS_CATERING,
      RESEARCH_SEGMENT.SCALING_COMPANIES,
      RESEARCH_SEGMENT.WEDDINGS_SOCIAL_PRIVATE_EVENTS,
      RESEARCH_SEGMENT.CORPORATE_MEETINGS_TRAINING,
      RESEARCH_SEGMENT.ASSOCIATIONS_GOVERNMENT,
      RESEARCH_SEGMENT.SPORTS,
    ];
    const i = order.indexOf(id);
    return i === -1 ? 50 : i;
  };
  coverageBacklog.sort(
    (a, b) => segmentPriority(a.segmentId) - segmentPriority(b.segmentId)
  );

  const selectedCoverage = coverageBacklog.slice(0, maxCoverageTasks);
  const selectedRefresh = refreshQueue.slice(0, maxRefreshTasks);
  const selectedFeedback = feedbackQueue.slice(0, maxFeedbackTasks);

  for (const t of coverageBacklog.slice(maxCoverageTasks)) {
    deferred.push({ ...t, reason: "deferred_coverage_cap" });
  }
  for (const t of refreshQueue.slice(maxRefreshTasks)) {
    deferred.push({ ...t, reason: "deferred_refresh_cap" });
  }

  const runMode =
    selectedCoverage.length && selectedRefresh.length
      ? RUN_MODE.COMBINED
      : selectedCoverage.length
        ? RUN_MODE.INITIAL_COVERAGE
        : RUN_MODE.REFRESH;

  return {
    version: WEEKLY_REFRESH_CONTRACT_VERSION,
    hotelId,
    generatedAt: new Date(now).toISOString(),
    runMode,
    segmentPlanSummary: plan.summary,
    coverageBacklog: selectedCoverage,
    refreshQueue: selectedRefresh,
    feedbackQueue: selectedFeedback,
    deferred,
    counts: {
      coverageSelected: selectedCoverage.length,
      refreshSelected: selectedRefresh.length,
      feedbackSelected: selectedFeedback.length,
      deferred: deferred.length,
      opportunitiesInScope: (opportunities || []).length,
    },
    invariantNotes: [
      "Unfinished initial coverage remains unfinished even if a refresh runs.",
      "Do not spend entire budget on associations while catering/scaling/PE gaps remain.",
      "Auto-publish remains OFF until forceAutoPublish is explicitly set.",
    ],
  };
}

/**
 * Apply change detection across candidate vs baseline maps.
 */
export function detectRefreshChanges({
  baselineOpportunities = [],
  candidates = [],
  assessedInternalIds = new Set(),
} = {}) {
  const byId = new Map();
  const byAccount = new Map();
  for (const o of baselineOpportunities) {
    const id = opportunityIdentity(o);
    byId.set(id, o);
    const ak = accountKey(o);
    if (!byAccount.has(ak)) byAccount.set(ak, []);
    byAccount.get(ak).push(o);
  }

  const results = [];
  for (const cand of candidates) {
    const id = opportunityIdentity(cand);
    let prior = byId.get(id) || null;
    if (!prior && cand.organizationName) {
      const matches = byAccount.get(accountKey(cand)) || [];
      // Standing merge: same account, no distinct dated occasion
      prior =
        matches.find(
          (m) =>
            !m.eventStartDate &&
            !cand.eventStartDate &&
            (m.standingOpportunity || cand.standingOpportunity)
        ) || null;
      // Distinct dated occasion stays separate
      if (
        !prior &&
        cand.eventStartDate &&
        matches.some((m) => m.eventStartDate === cand.eventStartDate)
      ) {
        prior = matches.find((m) => m.eventStartDate === cand.eventStartDate);
      }
    }

    const change = classifyRefreshChange(prior, cand, {
      previouslyAssessedInternal: !prior && assessedInternalIds.has(id),
      researchBlocked: cand.researchBlocked === true,
      blockReasons: cand.blockReasons,
      occasionConcluded: cand.occasionConcluded === true,
      conflictingEvidence: cand.conflictingEvidence === true,
    });

    results.push({
      opportunityId: id,
      organizationName: cand.organizationName || prior?.organizationName || null,
      priorId: prior ? opportunityIdentity(prior) : null,
      ...change,
      publishGate: mayAutoPublish(change, {
        forceAutoPublish: false,
        qualificationPassed: cand.qualificationPassed === true,
      }),
    });
  }

  return {
    version: WEEKLY_REFRESH_CONTRACT_VERSION,
    results,
    summary: {
      newProspect: results.filter((r) => r.changeClass === CHANGE_CLASS.NEW_PROSPECT)
        .length,
      materialUpdate: results.filter(
        (r) => r.changeClass === CHANGE_CLASS.MATERIAL_UPDATE
      ).length,
      contactEnriched: results.filter(
        (r) => r.changeClass === CHANGE_CLASS.CONTACT_ENRICHED
      ).length,
      noMaterialChange: results.filter(
        (r) => r.changeClass === CHANGE_CLASS.NO_MATERIAL_CHANGE
      ).length,
      needsReview: results.filter((r) => r.changeClass === CHANGE_CLASS.NEEDS_REVIEW)
        .length,
      researchBlocked: results.filter(
        (r) => r.changeClass === CHANGE_CLASS.RESEARCH_BLOCKED
      ).length,
      occasionStatusChanged: results.filter(
        (r) =>
          r.changeClass === CHANGE_CLASS.OCCASION_CONCLUDED_OR_STATUS_CHANGED
      ).length,
    },
  };
}

/**
 * Invoke the same downstream adapters the coordinator uses after weekly
 * selection/change classification — PE schema adapt, corporate packages,
 * contact display surface, hotel-fit reassessment, optional dry-run persist.
 *
 * Does NOT enable paid research or unattended publication. Returns a chain
 * trace proving weekly → same modules as manual coordinator stages.
 */
export async function invokeWeeklyDownstreamWorkflow({
  hotelId,
  opportunities = [],
  changeResults = [],
  dispositions = [],
  peCandidates = [],
  pePackagePath = null,
  authorizePublish = false,
  dryRun = true,
  authorizeSurfe = false,
  executeBuyerContactEnrichment = true,
  workflowRunId = null,
  buyerEnrichmentOutDir = null,
} = {}) {
  const chain = [];
  const cfg = loadHotelConfig(hotelId) || { hotelId };

  chain.push({
    step: "weekly_task_selection_complete",
    at: new Date().toISOString(),
    opportunityCount: opportunities.length,
    changeResultCount: changeResults.length,
  });

  // Source check / candidate classification already done by caller — record
  chain.push({
    step: "candidate_change_classification",
    module: "weekly_refresh_contract_v1.classifyRefreshChange",
    changeClasses: Object.fromEntries(
      [...new Set(changeResults.map((r) => r.changeClass))].map((c) => [
        c,
        changeResults.filter((r) => r.changeClass === c).length,
      ])
    ),
  });

  // Contact display surface on facing opportunities (same as list DTO path)
  let contactSurfacesApplied = 0;
  const contactSample = [];
  for (const opp of opportunities.slice(0, 40)) {
    const surface = buildContactDisplaySurface(opp);
    if (surface?.primaryRoute || surface?.companyPhone || surface?.resolvedName) {
      contactSurfacesApplied += 1;
      if (contactSample.length < 3) {
        contactSample.push({
          opportunityId: opp.id || opp.opportunityId,
          resolvedName: surface.resolvedName || null,
          hasPersonEmail: Boolean(surface.personEmail),
          hasCompanyPhone: Boolean(surface.companyPhone),
        });
      }
    }
  }
  chain.push({
    step: "contact_drawer_assessment",
    module: "contact_display_surface_v1",
    contactSurfacesApplied,
    sample: contactSample,
  });

  // Same buyer-contact enrichment path as coordinator contact-depth stage.
  // Weekly always runs public/canonical evaluation for queued items.
  // Surfe / paid provider calls stay off unless authorizeSurfe (never default).
  const buyerQueue = buildBuyerContactEnrichmentQueue({
    hotelId,
    opportunities,
    maxItems: 12,
  });
  let buyerPass = null;
  if (executeBuyerContactEnrichment !== false && buyerQueue.items?.length) {
    const { runBuyerContactEnrichmentPass } = await import(
      "./buyer-contact-enrichment-v1.js"
    );
    buyerPass = await runBuyerContactEnrichmentPass({
      hotelId,
      opportunities,
      queueItems: buyerQueue,
      authorizeSurfe: authorizeSurfe === true,
      dryRun: dryRun === true,
      workflowRunId: workflowRunId || null,
      outDir: buyerEnrichmentOutDir || null,
      maxItems: 12,
      persistCanonical: dryRun !== true,
    });
  }
  chain.push({
    step: "buyer_contact_enrichment",
    module: "buyer_contact_enrichment_v1",
    queueCount: buyerQueue.items?.length || 0,
    mode:
      buyerPass != null
        ? dryRun === true
          ? "dry_run_public_contact_depth"
          : authorizeSurfe === true
            ? "execute_with_optional_surfe"
            : "execute_public_contact_depth"
        : "queue_only",
    dryRun: dryRun === true,
    authorizeSurfe: authorizeSurfe === true,
    resultCount: buyerPass?.results?.length || 0,
    materialImprovements:
      buyerPass?.results?.filter((r) => r.materialImprovement).length || 0,
    note:
      buyerPass != null
        ? dryRun === true
          ? "Weekly dry-run executed public contact-depth only — Surfe and production writes blocked even if authorizeSurfe/authorizePublish were requested."
          : "Weekly refresh executed shared contact-depth evaluator (Surfe only when authorizeSurfe). Unattended paid publishing remains off."
        : "Buyer queue built; execution skipped (executeBuyerContactEnrichment=false).",
  });

  // Hotel-fit reassessment for material changes (same as weekly cycle)
  const fit = reassessHotelFitForRefreshChanges({
    hotelId,
    hotelConfig: cfg,
    baselineOpportunities: opportunities,
    candidates: opportunities,
    changeResults:
      changeResults.length > 0
        ? changeResults
        : opportunities.slice(0, 3).map((o) => ({
            opportunityId: o.id || o.opportunityId,
            changeClass: CHANGE_CLASS.NO_MATERIAL_CHANGE,
          })),
  });
  chain.push({
    step: "fit_assessment",
    module: "hotel_fit_assessment_v1",
    summary: fit.summary,
  });

  // Corporate package path from dispositions (same as apply_corporate_packages)
  let corporate = null;
  if (dispositions.length) {
    const pkgs = packagesFromDispositions(dispositions);
    corporate = await persistCorporatePackageDocument(pkgs, {
      dryRun: true,
      hotelIds: [hotelId],
    });
    chain.push({
      step: "corporate_package_persist_dry_run",
      module: "corporate_package_persist_v1",
      count: corporate.count,
      published: corporate.published,
      failed: corporate.failed,
    });
  } else {
    chain.push({
      step: "corporate_package_persist_dry_run",
      module: "corporate_package_persist_v1",
      skipped: true,
      reason: "no_dispositions_in_weekly_inputs",
    });
  }

  // PE adapter path (same as apply_pe_canonical load → adapt → persist)
  let peAdapted = null;
  let peCandidatesIn = peCandidates;
  if ((!peCandidatesIn || !peCandidatesIn.length) && pePackagePath) {
    try {
      const abs = path.isAbsolute(pePackagePath)
        ? pePackagePath
        : path.join(WR_REPO_ROOT, pePackagePath);
      if (fs.existsSync(abs)) {
        const doc = JSON.parse(fs.readFileSync(abs, "utf8"));
        peCandidatesIn = (Array.isArray(doc) ? doc : doc.packages || []).filter(
          (p) =>
            (p.hotel?.hotelId || p.hotel?.id || p.hotelId) === hotelId ||
            !hotelId
        );
      }
    } catch (err) {
      chain.push({
        step: "pe_provider_adapt",
        error: err?.message || String(err),
      });
    }
  }
  if (peCandidatesIn?.length) {
    peAdapted = adaptPeCandidatesBatch(peCandidatesIn, {
      hotelId,
      hotelConfig: cfg,
    });
    chain.push({
      step: "pe_provider_adapt",
      module: "pe_provider_to_canonical_adapter_v1",
      packageCount: peAdapted.packageCount,
      reviewCount: peAdapted.reviewCount,
      sharedDomainVenues: peAdapted.sharedDomainVenues,
    });
  } else {
    chain.push({
      step: "pe_provider_adapt",
      module: "pe_provider_to_canonical_adapter_v1",
      skipped: true,
      reason: "no_pe_candidates_for_hotel",
    });
  }

  // Quality / publication decision — auto-publish remains OFF
  const publicationDecision = {
    authorizePublish: Boolean(authorizePublish),
    autoPublishEnabled: false,
    dryRun: dryRun || !authorizePublish,
    policy:
      authorizePublish && !dryRun
        ? "WOULD_APPLY_CANONICAL_WHEN_AUTHORIZED"
        : "PREPARE_ONLY_AUTO_PUBLISH_OFF",
  };
  chain.push({
    step: "quality_publication_decision",
    ...publicationDecision,
  });

  // Canonical apply only when explicitly authorized (weekly default: never)
  chain.push({
    step: "canonical_apply",
    applied: false,
    reason: authorizePublish
      ? "authorize_publish_set_but_weekly_keeps_apply_in_coordinator_stages"
      : "unattended_publication_off",
    note: "Weekly uses the same adapters; live Airtable apply remains in coordinator stages apply_corporate_packages / apply_pe_canonical with authorize flags.",
  });

  const blockers = [];
  if (!dispositions.length && !(peAdapted?.packageCount > 0)) {
    // Still publication-ready if facing opps have fit+contact surfaces
    if (contactSurfacesApplied === 0 && fit.summary.reassessed === 0) {
      blockers.push("no_downstream_packages_and_no_surface_work");
    }
  }

  return {
    version: "gdi-weekly-downstream-workflow-v1",
    hotelId,
    generatedAt: new Date().toISOString(),
    chain,
    publicationDecision,
    corporate,
    peAdapted: peAdapted
      ? {
          packageCount: peAdapted.packageCount,
          reviewCount: peAdapted.reviewCount,
          sharedDomainVenues: peAdapted.sharedDomainVenues,
          reviewQueue: peAdapted.reviewQueue,
        }
      : null,
    hotelFitReassessments: fit.summary,
    completePublicationReadyPackage: blockers.length === 0,
    blockers,
    note: "Weekly entry invokes the same corporate/PE/fit/contact adapters as manual coordinator stages; paid + unattended publish remain OFF.",
  };
}

/**
 * Execute one weekly refresh cycle (research selection + change detect + state).
 * Does not perform paid provider calls. Publication preparation only unless
 * opts.forceAutoPublish (still defaults OFF).
 */
export async function runWeeklyRefreshCycle({
  hotelId,
  opportunities = [],
  candidates = [],
  assessedInternalIds = new Set(),
  trigger = RUN_TRIGGER.MANUAL,
  now = new Date(),
  dryRun = true,
  env = process.env,
  clockLabel = null,
  dispositions = [],
  pePackagePath = null,
  invokeDownstream = true,
} = {}) {
  const cfg = loadHotelConfig(hotelId) || { hotelId };
  let state = loadWeeklyRefreshState(hotelId, env);

  if (state.killSwitch) {
    return {
      ok: false,
      status: RUN_STATUS.BLOCKED,
      reason: "kill_switch_enabled",
      hotelId,
    };
  }

  const runId = `gdi_weekly_${hotelId.slice(0, 8)}_${new Date(now)
    .toISOString()
    .replace(/[:.]/g, "-")}_${randomBytes(3).toString("hex")}`;

  const lock = acquireWeeklyRefreshLock(hotelId, { runId, env });
  if (!lock.ok) {
    return {
      ok: false,
      status: RUN_STATUS.BLOCKED,
      reason: lock.reason,
      holderRunId: lock.holderRunId,
      hotelId,
    };
  }

  let runRecord = {
    version: WEEKLY_REFRESH_CONTRACT_VERSION,
    runId,
    hotelId,
    trigger,
    clockLabel,
    dryRun,
    autoPublishEnabled: false,
    startedAt: new Date(now).toISOString(),
    endedAt: null,
    status: RUN_STATUS.RUNNING,
    budget: {
      estimatedSpendUsd: 0,
      actualBilledSpendUsd: 0,
      note: "No paid spend authorized in weekly refresh V1 pilot",
    },
  };

  try {
    const queues = buildWeeklyWorkQueues({
      hotelId,
      hotelConfig: cfg,
      opportunities,
      state,
      now,
    });

    // Simulate / apply checks for selected work without paid calls
    const checkResults = [];
    for (const task of [
      ...queues.coverageBacklog,
      ...queues.refreshQueue,
      ...queues.feedbackQueue,
    ]) {
      const ok = task.simulateFailure !== true;
      checkResults.push({
        ...task,
        checkStatus: ok ? "success" : "failed",
        checkedAt: new Date(now).toISOString(),
      });
      if (ok) {
        state = advanceFreshness(state, {
          segmentId: task.segmentId,
          accountKey: task.accountKey,
          at: new Date(now).toISOString(),
        });
      } else {
        state = recordFailedCheck(state, {
          segmentId: task.segmentId,
          accountKey: task.accountKey,
          reason: "simulated_or_retrieval_failure",
          at: new Date(now).toISOString(),
        });
      }
    }

    // Clear retries that succeeded
    const succeededKeys = new Set(
      checkResults
        .filter((c) => c.checkStatus === "success" && c.accountKey)
        .map((c) => c.accountKey)
    );
    state.pendingRetries = (state.pendingRetries || []).filter(
      (r) => !r.accountKey || !succeededKeys.has(r.accountKey)
    );

    const candidatePool =
      candidates.length > 0
        ? candidates
        : opportunities.map((o) => ({ ...o })); // self-compare → NO_MATERIAL_CHANGE

    const changes = detectRefreshChanges({
      baselineOpportunities: opportunities,
      candidates: candidatePool,
      assessedInternalIds,
    });

    // Reassess hotel-fit only when material commercial / contact evidence changed
    // (or new prospect). Do not rewrite scores solely because a week passed.
    const fitReassessments = reassessHotelFitForRefreshChanges({
      hotelId,
      hotelConfig: cfg,
      baselineOpportunities: opportunities,
      candidates: candidatePool,
      changeResults: changes.results,
    });

    const publicationPlan = changes.results.map((r) => ({
      opportunityId: r.opportunityId,
      changeClass: r.changeClass,
      publicationPolicy: r.publicationPolicy,
      autoPublish: r.publishGate,
      action:
        r.publishGate.allowed === true
          ? "WOULD_PUBLISH"
          : r.publicationPolicy === PUBLICATION_POLICY.NON_MATERIAL_VERIFICATION
            ? "UPDATE_LAST_CHECKED_ONLY"
            : r.publicationPolicy === PUBLICATION_POLICY.INTERNAL_REVIEW
              ? "HOLD_INTERNAL_REVIEW"
              : "PREPARE_ONLY_AUTO_PUBLISH_OFF",
    }));

    const summary = buildHotelWeeklySummary({
      hotelId,
      queues,
      checkResults,
      changes,
      publicationPlan,
      now,
    });

    // Hotel × segment briefs for next coverage/refresh selection (no paid submit)
    const briefPack = buildHotelSegmentResearchBriefs({
      hotelId,
      hotelConfig: cfg,
      opportunities,
      segmentIds: null,
    });
    const priorityBriefs = selectContinuationPriorityBriefs(briefPack, {
      avoidPeExpansion: true,
    });

    // Scorecard: weekly run is plan/check — plan-only is not completed research
    const scorecardTasks = (queues.coverageBacklog || [])
      .concat(queues.refreshQueue || [])
      .map((task) => {
        const row = createEmptyScorecardRow({
          runId,
          taskId: task.taskId || `${task.kind}:${task.segmentId}`,
          hotelId,
          segmentId: task.segmentId,
          mode: EXECUTION_MODE.PLAN_ONLY,
          objective: task.rationale || task.reason || "weekly_queue_item",
        });
        row.scope.status = "plan_only_check";
        row.scope.stoppingReason =
          "weekly_refresh_v1_no_paid_research; plan-only is not completed research";
        row.scope.startedAt = runRecord.startedAt;
        row.scope.endedAt = new Date(now).toISOString();
        row.cost.estimatedIncurredUsd = 0;
        row.cost.actualBilledUsd = 0;
        row.cost.remainingBalanceUsd = null;
        row.cost.costBasis = "observed";
        if (changes?.summary?.newProspect) {
          row.outcomes.byPrimaryDisposition[
            PRIMARY_DISPOSITION.NET_NEW_QUALIFIED
          ] = 0;
        }
        return finalizeScorecardEfficiency(row);
      });

    const scorecardDoc = {
      version: RESEARCH_SCORECARD_VERSION,
      generatedAt: new Date(now).toISOString(),
      source: "weekly_refresh_orchestrator_v1",
      hotelId,
      runId,
      note: "Weekly scorecard instruments queue selection; unpaid checks ≠ completed discovery research",
      spend: summary.spend,
      tasks: scorecardTasks,
      nextRecommendedBriefs: (priorityBriefs.selectedBriefs || []).map((b) => ({
        briefId: b.briefId,
        segmentId: b.segment?.segmentId,
        objective: b.objective,
      })),
    };

    // Downstream workflow: same adapters as coordinator stages (dry-run)
    let downstream = null;
    if (invokeDownstream) {
      const defaultPePath =
        pePackagePath ||
        path.join(
          WR_REPO_ROOT,
          "reports/gdi-paid-multisegment-validation/pe-venue-packages.json"
        );
      downstream = await invokeWeeklyDownstreamWorkflow({
        hotelId,
        opportunities,
        changeResults: changes.results || [],
        dispositions,
        pePackagePath: fs.existsSync(defaultPePath) ? defaultPePath : null,
        authorizePublish: false,
        dryRun: true,
      });
    }

    runRecord = markRunStatus(runRecord, RUN_STATUS.COMPLETED, {
      runMode: queues.runMode,
      queues,
      checkResults,
      changes,
      hotelFitReassessments: fitReassessments.summary,
      publicationPlan,
      summary,
      downstreamWorkflow: downstream
        ? {
            completePublicationReadyPackage:
              downstream.completePublicationReadyPackage,
            chainSteps: (downstream.chain || []).map((c) => c.step),
            pePackageCount: downstream.peAdapted?.packageCount ?? null,
            blockers: downstream.blockers,
          }
        : null,
      scorecard: {
        taskCount: scorecardTasks.length,
        nextRecommendedBriefCount:
          scorecardDoc.nextRecommendedBriefs?.length || 0,
      },
      researchBriefs: {
        applicableCount: briefPack.applicableSegmentCount,
        priorityCount: priorityBriefs.selectedBriefs?.length || 0,
      },
      providerJobIds: [],
      sourcesChecked: checkResults.length,
      ruleVersions: {
        contract: WEEKLY_REFRESH_CONTRACT_VERSION,
        segmentPlan: planVersionSafe(),
        scorecard: RESEARCH_SCORECARD_VERSION,
        hotelFitAssessment: HOTEL_FIT_ASSESSMENT_VERSION,
        peAdapter: "gdi-pe-provider-to-canonical-adapter-v1",
      },
    });

    // Persist reassessed opportunities when apply mode (not dry-run)
    if (!dryRun && fitReassessments.updatedOpportunities.length > 0) {
      try {
        const { saveOpportunitiesCanonical } = await import(
          "../opportunity-persistence.js"
        );
        const byId = new Map(
          opportunities.map((o) => [opportunityIdentity(o), o])
        );
        for (const updated of fitReassessments.updatedOpportunities) {
          byId.set(opportunityIdentity(updated), updated);
        }
        await saveOpportunitiesCanonical(hotelId, {
          hotelId,
          opportunities: [...byId.values()],
          researchVersion: HOTEL_FIT_ASSESSMENT_VERSION,
          runId,
        });
      } catch (err) {
        console.warn(
          "[weekly-refresh] hotel-fit reassessment persist:",
          err?.message || err
        );
      }
    }

    // Persist scorecard + briefs beside weekly state when env root available
    try {
      const wrArtifactsDir = weeklyArtifactsDir(hotelId, runId, env);
      if (wrArtifactsDir) {
        fs.mkdirSync(wrArtifactsDir, { recursive: true });
        writeScorecardArtifacts(scorecardDoc, path.join(wrArtifactsDir, "scorecard"));
        fs.writeFileSync(
          path.join(wrArtifactsDir, "hotel-segment-research-briefs.json"),
          JSON.stringify(
            {
              ...briefPack,
              prioritySelection: priorityBriefs.selectedBriefs?.map((b) => b.briefId),
            },
            null,
            2
          ) + "\n"
        );
      }
    } catch (err) {
      console.warn(
        "[weekly-refresh] scorecard/brief artifact write:",
        err?.message || err
      );
    }

    // Initial coverage remains incomplete unless segment scope explicitly complete
    state.pendingGaps = queues.deferred
      .concat(
        queues.coverageBacklog.map((t) => ({
          ...t,
          reason: "still_in_coverage_backlog_after_run",
        }))
      )
      .slice(0, 50);
    state.lastSuccessfulRunId = runId;
    state.lastSuccessfulRunAt = new Date(now).toISOString();
    // Do NOT set lastSuccessfulPublicationAt — auto publish off
    state.nextEligibleAt = new Date(
      new Date(now).getTime() + 7 * 86400000
    ).toISOString();
    state.schedulingRationale =
      "Default weekly cadence; auto-publish OFF; paid spend not authorized";

    if (!dryRun) {
      saveWeeklyRefreshState(hotelId, state, env);
    } else {
      // Still persist journal for proof; state write optional in dry-run —
      // for two-cycle proof we persist state even in dry-run when env allows
      if (env.GDI_WEEKLY_REFRESH_PERSIST_STATE_ON_DRY_RUN === "1") {
        saveWeeklyRefreshState(hotelId, state, env);
      }
    }

    const journalPath = persistRunJournal(hotelId, runRecord, env);

    // Never write dry-run / test-clock completions into production customer history.
    // Journals remain on disk for audit; customer aggregation filters dryRun/clockLabel.
    if (!dryRun && !String(clockLabel || "").trim()) {
      try {
        const { appendHotelActivityEvent, ACTIVITY_KIND } = await import(
          "../../hotel-activity-history-v1.js"
        );
        appendHotelActivityEvent(hotelId, {
          kind: ACTIVITY_KIND.WEEKLY_REFRESH,
          status: RUN_STATUS.COMPLETED,
          at: state.lastSuccessfulRunAt || new Date().toISOString(),
          runId,
          source: "weekly-refresh-orchestrator",
          provenance: "live_write",
          dryRun: false,
          clockLabel: null,
          runMode: queues.runMode || null,
        }, env);
      } catch (actErr) {
        console.warn(
          "[weekly-refresh] activity ledger:",
          actErr?.message || actErr
        );
      }
    }

    return {
      ok: true,
      status: RUN_STATUS.COMPLETED,
      runId,
      hotelId,
      dryRun,
      runMode: queues.runMode,
      summary,
      queues,
      changes,
      hotelFitReassessments: fitReassessments,
      publicationPlan,
      downstreamWorkflow: downstream,
      scorecard: scorecardDoc,
      researchBriefs: {
        applicableCount: briefPack.applicableSegmentCount,
        priorityBriefIds: (priorityBriefs.selectedBriefs || []).map(
          (b) => b.briefId
        ),
      },
      journalPath,
      stateSnapshot: {
        pendingGaps: state.pendingGaps?.length || 0,
        pendingRetries: state.pendingRetries?.length || 0,
        nextEligibleAt: state.nextEligibleAt,
        lastSuccessfulRunAt: state.lastSuccessfulRunAt,
        lastSuccessfulPublicationAt: state.lastSuccessfulPublicationAt,
      },
    };
  } catch (err) {
    runRecord = markRunStatus(runRecord, RUN_STATUS.FAILED, {
      error: err?.message || String(err),
    });
    persistRunJournal(hotelId, runRecord, env);
    if (!dryRun && !String(clockLabel || "").trim()) {
      try {
        const { appendHotelActivityEvent, ACTIVITY_KIND } = await import(
          "../../hotel-activity-history-v1.js"
        );
        appendHotelActivityEvent(hotelId, {
          kind: ACTIVITY_KIND.WEEKLY_REFRESH,
          status: RUN_STATUS.FAILED,
          at: new Date().toISOString(),
          runId: runRecord?.id || runId,
          source: "weekly-refresh-orchestrator",
          provenance: "live_write",
          dryRun: false,
          clockLabel: null,
          note: err?.message || String(err),
        }, env);
      } catch {
        // activity ledger is best-effort
      }
    }
    return {
      ok: false,
      status: RUN_STATUS.FAILED,
      runId,
      hotelId,
      error: err?.message || String(err),
    };
  } finally {
    releaseWeeklyRefreshLock(hotelId, lock.claimToken, env);
  }
}

function planVersionSafe() {
  try {
    return "gdi-hotel-segment-research-plan-v2";
  } catch {
    return null;
  }
}

/** Evidence fields that justify a hotel-fit reassessment on refresh. */
const FIT_REASSESS_CHANGE_CLASSES = new Set([
  CHANGE_CLASS.NEW_PROSPECT,
  CHANGE_CLASS.MATERIAL_UPDATE,
  CHANGE_CLASS.CONTACT_ENRICHED,
  CHANGE_CLASS.OCCASION_CONCLUDED_OR_STATUS_CHANGED,
]);

/**
 * Re-run shared hotel-fit assessment when refresh detects evidence change.
 * Skips NO_MATERIAL_CHANGE / RESEARCH_BLOCKED. Does not invent weekly churn.
 */
export function reassessHotelFitForRefreshChanges({
  hotelId,
  hotelConfig = null,
  baselineOpportunities = [],
  candidates = [],
  changeResults = [],
} = {}) {
  const baselineById = new Map(
    baselineOpportunities.map((o) => [opportunityIdentity(o), o])
  );
  const candById = new Map(
    candidates.map((o) => [opportunityIdentity(o), o])
  );
  const updatedOpportunities = [];
  const rows = [];

  for (const change of changeResults) {
    if (!FIT_REASSESS_CHANGE_CLASSES.has(change.changeClass)) {
      rows.push({
        opportunityId: change.opportunityId,
        changeClass: change.changeClass,
        reassessed: false,
        reason: "no_evidence_delta_for_fit",
      });
      continue;
    }
    const cand =
      candById.get(change.opportunityId) ||
      baselineById.get(change.opportunityId) ||
      null;
    if (!cand) {
      rows.push({
        opportunityId: change.opportunityId,
        changeClass: change.changeClass,
        reassessed: false,
        reason: "candidate_missing",
      });
      continue;
    }
    const prior = baselineById.get(change.priorId || change.opportunityId);
    const priorVersion = prior?.hotelFitAssessmentVersion || null;
    const assessment = assessHotelFitForOpportunity(cand, {
      hotelId,
      hotelConfig,
    });
    let next = applyHotelFitAssessmentToOpportunity(
      { ...cand, _reassessHotelFit: true },
      assessment
    );
    next = applyContactDisplaySurfaceToOpportunity(
      next,
      buildContactDisplaySurface(next)
    );
    next.hotelFitReassessReason = {
      changeClass: change.changeClass,
      materialFieldsChanged: change.materialFieldsChanged || [],
      contactFieldsChanged: change.contactFieldsChanged || [],
      priorAssessmentVersion: priorVersion,
      priorHotelFitScore: prior?.hotelFitScore ?? null,
      at: assessment.assessedAt,
    };
    updatedOpportunities.push(next);
    rows.push({
      opportunityId: change.opportunityId,
      changeClass: change.changeClass,
      reassessed: true,
      priorScore: prior?.hotelFitScore ?? null,
      nextScore: next.hotelFitScore ?? null,
      overallStatus: next.hotelFitOverallStatus || null,
      useCaseProfile: next.hotelFitUseCaseProfile || null,
      version: HOTEL_FIT_ASSESSMENT_VERSION,
    });
  }

  return {
    version: HOTEL_FIT_ASSESSMENT_VERSION,
    rows,
    updatedOpportunities,
    summary: {
      considered: changeResults.length,
      reassessed: updatedOpportunities.length,
      skipped: changeResults.length - updatedOpportunities.length,
    },
  };
}

export function buildHotelWeeklySummary({
  hotelId,
  queues,
  checkResults = [],
  changes,
  publicationPlan = [],
  now = new Date(),
} = {}) {
  const failed = checkResults.filter((c) => c.checkStatus === "failed");
  return {
    hotelId,
    generatedAt: new Date(now).toISOString(),
    runMode: queues?.runMode,
    newQualifiedProspects: changes?.summary?.newProspect || 0,
    materiallyUpdatedProspects: changes?.summary?.materialUpdate || 0,
    contactsResolved: changes?.summary?.contactEnriched || 0,
    statusChanges: changes?.summary?.occasionStatusChanged || 0,
    unchangedRecordsChecked: changes?.summary?.noMaterialChange || 0,
    needsReview: changes?.summary?.needsReview || 0,
    researchBlocked: changes?.summary?.researchBlocked || 0,
    internalAwaitingEvidence: (queues?.deferred || []).filter((d) =>
      /enrichment|contact|gap/i.test(String(d.reason || d.rationale || ""))
    ).length,
    segmentCoverageGaps: (queues?.coverageBacklog || []).map((t) => t.segmentId),
    failedOrBlockedChecks: failed.length,
    spend: {
      estimatedUsd: 0,
      actualUsd: 0,
      note: "Paid research not authorized for weekly refresh pilot",
    },
    nextPlannedWork: (queues?.deferred || [])
      .slice(0, 8)
      .map((d) => d.segmentId || d.organizationName || d.reason),
    distinctOpportunitiesTouched: new Set(
      publicationPlan.map((p) => p.opportunityId).filter(Boolean)
    ).size,
    fieldChangeTotal:
      (changes?.summary?.materialUpdate || 0) +
      (changes?.summary?.contactEnriched || 0),
    note: "Zero new prospects is valid when real research ran and found no NEW_PROSPECT.",
  };
}

export { opportunityIdentity, accountKey, RUN_TRIGGER, RUN_STATUS, RUN_MODE };
