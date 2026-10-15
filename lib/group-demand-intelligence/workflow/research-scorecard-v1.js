/**
 * Standard GDI research scorecard V1 — hotel × segment × task.
 *
 * Distinguishes observed / provider-reported / estimated / UNKNOWN metrics.
 * Zero yield is valid (show spend + zero without division errors).
 * Shared costs counted once; per-hotel figures labeled as allocations.
 * Plan-only is not completed research.
 */

import fs from "node:fs";
import path from "node:path";

export const RESEARCH_SCORECARD_VERSION = "gdi-research-scorecard-v1";

export const METRIC_BASIS = Object.freeze({
  OBSERVED: "observed",
  PROVIDER_REPORTED: "provider_reported",
  ESTIMATED: "estimated",
  UNKNOWN: "UNKNOWN",
});

export const PRIMARY_DISPOSITION = Object.freeze({
  NET_NEW_QUALIFIED: "net_new_qualified",
  EXISTING_ENRICHED: "existing_prospect_enriched",
  DUPLICATE_REUSED: "duplicate_reused",
  REJECTED: "rejected",
  INTERNAL_ENRICHMENT: "internal_enrichment",
  NEEDS_REVIEW: "needs_review",
  QUARANTINED: "quarantined",
});

export const PUBLICATION_STATUS = Object.freeze({
  NOT_ELIGIBLE: "not_eligible",
  READY: "ready",
  PENDING: "pending",
  PERSISTED: "persisted",
  PRODUCTION_VERIFIED: "production_verified",
});

export const EXECUTION_MODE = Object.freeze({
  REUSE: "reuse",
  FRESH_RESEARCH: "fresh_research",
  REPLAY: "replay",
  PLAN_ONLY: "plan_only",
  REPROCESS_STORED: "reprocess_stored",
});

/** Task execution status — orthogonal to segment coverage. */
export const TASK_EXECUTION_STATUS = Object.freeze({
  PLANNED: "planned",
  RUNNING: "running",
  COMPLETED: "completed",
  PARTIAL: "partial",
  FAILED: "failed",
  BLOCKED: "blocked",
});

/**
 * Segment coverage — not triggered by producing one or more cards.
 * completed_for_declared_scope must cite boundedScope + remainingExclusions.
 */
export const SEGMENT_COVERAGE_STATUS = Object.freeze({
  NOT_STARTED: "not_started",
  PARTIAL: "partial",
  COMPLETED_FOR_DECLARED_SCOPE: "completed_for_declared_scope",
  NOT_APPLICABLE: "not_applicable",
});

const UNKNOWN = null;

function numOrUnknown(v) {
  if (v == null || v === "" || v === "UNKNOWN") return UNKNOWN;
  const n = Number(v);
  return Number.isFinite(n) ? n : UNKNOWN;
}

function safeRate(num, den) {
  if (num == null || den == null || den === 0) return UNKNOWN;
  return Number((num / den).toFixed(4));
}

/**
 * Empty scorecard shell for one hotel × segment × task.
 */
export function createEmptyScorecardRow({
  runId = null,
  taskId = null,
  hotelId = null,
  segmentId = null,
  useCases = [],
  objective = null,
  declaredGeography = null,
  briefVersion = null,
  provider = null,
  mode = EXECUTION_MODE.PLAN_ONLY,
} = {}) {
  return {
    version: RESEARCH_SCORECARD_VERSION,
    // A. Scope and execution
    scope: {
      runId,
      taskId,
      hotelId,
      segmentId,
      useCases: useCases || [],
      objective,
      declaredGeography,
      briefOrQuery: null,
      briefVersion,
      provider,
      mode,
      startedAt: null,
      endedAt: null,
      /** @deprecated prefer taskExecutionStatus — kept for back-compat */
      status: "not_started",
      taskExecutionStatus: TASK_EXECUTION_STATUS.PLANNED,
      /** Segment coverage — completed task ≠ completed coverage */
      segmentCoverageStatus: SEGMENT_COVERAGE_STATUS.NOT_STARTED,
      declaredScope: null,
      remainingExclusions: [],
      stoppingReason: null,
      coverageGaps: [],
      /** LMC only — per-subsegment coverage under LOCAL_MEETINGS_CATERING */
      lmcSubsegmentCoverage: null,
      note:
        mode === EXECUTION_MODE.PLAN_ONLY
          ? "Plan-only is not completed research. Completed task does not imply completed_for_declared_scope."
          : "Completed task does not imply completed segment coverage.",
    },
    // B. Research depth — unavailable counts stay UNKNOWN
    depth: {
      searchesExecuted: UNKNOWN,
      searchesBasis: METRIC_BASIS.UNKNOWN,
      pagesAttempted: UNKNOWN,
      pagesRetrieved: UNKNOWN,
      pagesBasis: METRIC_BASIS.UNKNOWN,
      uniqueSourcesOrDomains: UNKNOWN,
      officialSources: UNKNOWN,
      candidatesDiscovered: UNKNOWN,
      candidatesAssessed: UNKNOWN,
      claimsCorroborated: UNKNOWN,
      buyerSearches: UNKNOWN,
      channelsFound: UNKNOWN,
      channelsVerified: UNKNOWN,
      unresolvedEvidenceGaps: [],
    },
    // C. Candidate outcomes
    outcomes: {
      byPrimaryDisposition: {
        [PRIMARY_DISPOSITION.NET_NEW_QUALIFIED]: 0,
        [PRIMARY_DISPOSITION.EXISTING_ENRICHED]: 0,
        [PRIMARY_DISPOSITION.DUPLICATE_REUSED]: 0,
        [PRIMARY_DISPOSITION.REJECTED]: 0,
        [PRIMARY_DISPOSITION.INTERNAL_ENRICHMENT]: 0,
        [PRIMARY_DISPOSITION.NEEDS_REVIEW]: 0,
        [PRIMARY_DISPOSITION.QUARANTINED]: 0,
      },
      publicationByStatus: {
        [PUBLICATION_STATUS.NOT_ELIGIBLE]: 0,
        [PUBLICATION_STATUS.READY]: 0,
        [PUBLICATION_STATUS.PENDING]: 0,
        [PUBLICATION_STATUS.PERSISTED]: 0,
        [PUBLICATION_STATUS.PRODUCTION_VERIFIED]: 0,
      },
      contactImprovements: 0,
      uniqueOrganizations: 0,
      uniqueVenues: 0,
      uniqueOpportunities: 0,
      restorationsReapplicationsNotCountedAsDiscovery: 0,
      bottleneck:
        null /* discovery|evidence|fit|contact|drawer|publication */,
    },
    // D. Cost
    cost: {
      allocatedAuthorizationUsd: UNKNOWN,
      estimatedIncurredUsd: UNKNOWN,
      outstandingReservationsUsd: 0,
      actualBilledUsd: UNKNOWN,
      creditsConsumed: UNKNOWN,
      failedOrNoResultChargesUsd: UNKNOWN,
      remainingBalanceUsd: UNKNOWN,
      costBasis: METRIC_BASIS.ESTIMATED,
      sharedResearchCountedOnce: true,
      perHotelAllocationLabel: null,
    },
    // E. Time
    time: {
      wallClockMs: UNKNOWN,
      providerWaitMs: UNKNOWN,
      processingMs: UNKNOWN,
      retryMs: UNKNOWN,
      reviewPublicationMs: UNKNOWN,
      manualInterventions: [],
      humanEffortLogged: false,
      note: "Do not sum concurrent durations as elapsed time",
    },
    // F. Efficiency
    efficiency: {
      qualifiedPerAssessed: UNKNOWN,
      netNewPerAssessed: UNKNOWN,
      contactResolutionsPerAttempted: UNKNOWN,
      wrongMarketRate: UNKNOWN,
      duplicateRate: UNKNOWN,
      drawerPassRate: UNKNOWN,
      publicationSuccessAmongEligible: UNKNOWN,
      costPerNetNewQualifiedUsd: UNKNOWN,
      costPerEnrichedExistingUsd: UNKNOWN,
      timeToPublicationReadyMs: UNKNOWN,
      zeroYieldWithSpend: false,
    },
  };
}

/**
 * Finalize efficiency metrics after outcomes/cost/depth are filled.
 */
export function finalizeScorecardEfficiency(row = {}) {
  const assessed = numOrUnknown(row.depth?.candidatesAssessed) ?? 0;
  const discovered = numOrUnknown(row.depth?.candidatesDiscovered);
  const disp = row.outcomes?.byPrimaryDisposition || {};
  const netNew = disp[PRIMARY_DISPOSITION.NET_NEW_QUALIFIED] || 0;
  const enriched = disp[PRIMARY_DISPOSITION.EXISTING_ENRICHED] || 0;
  const qualified = netNew + enriched;
  const quarantined = disp[PRIMARY_DISPOSITION.QUARANTINED] || 0;
  const dupes = disp[PRIMARY_DISPOSITION.DUPLICATE_REUSED] || 0;
  const est = numOrUnknown(row.cost?.estimatedIncurredUsd);
  const wall = numOrUnknown(row.time?.wallClockMs);

  const denom = assessed || discovered || 0;
  row.efficiency = {
    ...row.efficiency,
    qualifiedPerAssessed: safeRate(qualified, denom),
    netNewPerAssessed: safeRate(netNew, denom),
    contactResolutionsPerAttempted: safeRate(
      row.outcomes?.contactImprovements,
      row.depth?.buyerSearches
    ),
    wrongMarketRate: safeRate(quarantined, denom),
    duplicateRate: safeRate(dupes, denom),
    drawerPassRate: row.efficiency?.drawerPassRate ?? UNKNOWN,
    publicationSuccessAmongEligible: safeRate(
      row.outcomes?.publicationByStatus?.[PUBLICATION_STATUS.PERSISTED] ||
        row.outcomes?.publicationByStatus?.[
          PUBLICATION_STATUS.PRODUCTION_VERIFIED
        ] ||
        0,
      (row.outcomes?.publicationByStatus?.[PUBLICATION_STATUS.READY] || 0) +
        (row.outcomes?.publicationByStatus?.[PUBLICATION_STATUS.PENDING] || 0) +
        (row.outcomes?.publicationByStatus?.[PUBLICATION_STATUS.PERSISTED] ||
          0) +
        (row.outcomes?.publicationByStatus?.[
          PUBLICATION_STATUS.PRODUCTION_VERIFIED
        ] || 0)
    ),
    costPerNetNewQualifiedUsd:
      netNew > 0 && est != null ? Number((est / netNew).toFixed(4)) : UNKNOWN,
    costPerEnrichedExistingUsd:
      enriched > 0 && est != null
        ? Number((est / enriched).toFixed(4))
        : UNKNOWN,
    timeToPublicationReadyMs: wall,
    zeroYieldWithSpend: netNew === 0 && enriched === 0 && (est || 0) > 0,
  };
  return row;
}

/**
 * Map disposition strings from paid-MS / buyer enrichment into primary classes.
 */
export function mapDispositionToPrimary(disposition) {
  const d = String(disposition || "").toLowerCase();
  if (/quarantine/.test(d)) return PRIMARY_DISPOSITION.QUARANTINED;
  if (/qualified_publishable|net.?new/.test(d)) {
    return PRIMARY_DISPOSITION.NET_NEW_QUALIFIED;
  }
  if (/contact_resolved|existing.?enrich|material.?improv/.test(d)) {
    return PRIMARY_DISPOSITION.EXISTING_ENRICHED;
  }
  if (/duplicate|already_assessed/.test(d)) {
    return PRIMARY_DISPOSITION.DUPLICATE_REUSED;
  }
  if (/internal_enrichment/.test(d)) {
    return PRIMARY_DISPOSITION.INTERNAL_ENRICHMENT;
  }
  if (/needs_review/.test(d)) return PRIMARY_DISPOSITION.NEEDS_REVIEW;
  if (/reject/.test(d)) return PRIMARY_DISPOSITION.REJECTED;
  return PRIMARY_DISPOSITION.NEEDS_REVIEW;
}

/**
 * Attach LMC subsegment coverage onto a scorecard row (LOCAL_MEETINGS_CATERING only).
 * Does not invent yield — pass observed outcomesBySubsegment from the run.
 */
export function attachLmcSubsegmentCoverage(row = {}, coverageSnapshot = null) {
  if (!row?.scope) return row;
  if (!coverageSnapshot) {
    row.scope.lmcSubsegmentCoverage = null;
    return row;
  }
  row.scope.lmcSubsegmentCoverage = coverageSnapshot;
  const gaps = [];
  for (const s of coverageSnapshot.subsegments || []) {
    if (s.applicable && !s.researched) {
      gaps.push(`lmc_subsegment_not_researched:${s.subsegmentId}`);
    }
    for (const g of s.remainingGaps || []) gaps.push(g);
  }
  row.scope.coverageGaps = [
    ...new Set([...(row.scope.coverageGaps || []), ...gaps]),
  ];
  return row;
}

/**
 * Build a scorecard row from a completed paid Parallel task result.
 */
export function scorecardFromPaidTaskResult(taskResult = {}, opts = {}) {
  const row = createEmptyScorecardRow({
    runId: opts.runId || null,
    taskId: taskResult.taskId,
    hotelId: taskResult.hotelId || opts.hotelId,
    segmentId: taskResult.segment || opts.segmentId,
    provider: "parallel",
    mode:
      taskResult.status === "completed_resume"
        ? EXECUTION_MODE.REUSE
        : taskResult.status === "completed"
          ? EXECUTION_MODE.FRESH_RESEARCH
          : EXECUTION_MODE.PLAN_ONLY,
    briefVersion: opts.briefVersion || null,
    objective: opts.objective || null,
    declaredGeography: opts.declaredGeography || null,
  });

  const exec =
    taskResult.status === "completed" || taskResult.status === "completed_resume"
      ? TASK_EXECUTION_STATUS.COMPLETED
      : taskResult.status === "failed"
        ? TASK_EXECUTION_STATUS.FAILED
        : taskResult.status === "blocked"
          ? TASK_EXECUTION_STATUS.BLOCKED
          : TASK_EXECUTION_STATUS.PARTIAL;
  row.scope.status = exec; // back-compat
  row.scope.taskExecutionStatus = exec;
  // Producing candidates never flips coverage to completed_for_declared_scope
  row.scope.segmentCoverageStatus =
    opts.segmentCoverageStatus || SEGMENT_COVERAGE_STATUS.PARTIAL;
  row.scope.declaredScope = opts.declaredScope || null;
  row.scope.remainingExclusions = opts.remainingExclusions || [];
  row.scope.stoppingReason =
    taskResult.reason ||
    taskResult.note ||
    (exec === TASK_EXECUTION_STATUS.COMPLETED
      ? "task_execution_completed_coverage_may_remain_partial"
      : null);
  row.scope.briefOrQuery = opts.promptFocus || null;
  if (opts.lmcSubsegmentCoverage) {
    attachLmcSubsegmentCoverage(row, opts.lmcSubsegmentCoverage);
  }

  const dispositions = taskResult.dispositions || opts.dispositions || [];
  row.depth.candidatesDiscovered =
    Array.isArray(taskResult.candidates)
      ? taskResult.candidates.length
      : dispositions.length || UNKNOWN;
  row.depth.candidatesAssessed = dispositions.length || UNKNOWN;
  row.depth.searchesBasis = METRIC_BASIS.UNKNOWN;
  row.depth.pagesBasis = METRIC_BASIS.UNKNOWN;

  for (const d of dispositions) {
    const primary = mapDispositionToPrimary(d.disposition);
    row.outcomes.byPrimaryDisposition[primary] =
      (row.outcomes.byPrimaryDisposition[primary] || 0) + 1;
  }
  row.outcomes.uniqueOrganizations = new Set(
    dispositions.map((d) => d.organizationName).filter(Boolean)
  ).size;
  row.outcomes.uniqueOpportunities = dispositions.length;

  const pub = dispositions.filter((d) =>
    /qualified_publishable/.test(String(d.disposition || ""))
  ).length;
  row.outcomes.publicationByStatus[PUBLICATION_STATUS.READY] = pub;

  row.cost.estimatedIncurredUsd = numOrUnknown(taskResult.estimatedUsd) ?? 0;
  row.cost.actualBilledUsd = numOrUnknown(taskResult.actualBilledUsd);
  row.cost.costBasis =
    taskResult.actualBilledUsd != null
      ? METRIC_BASIS.PROVIDER_REPORTED
      : METRIC_BASIS.ESTIMATED;
  row.cost.allocatedAuthorizationUsd = opts.authorizedUsd ?? UNKNOWN;
  row.cost.remainingBalanceUsd = opts.remainingUsd ?? UNKNOWN;

  if (opts.startedAt && opts.endedAt) {
    row.scope.startedAt = opts.startedAt;
    row.scope.endedAt = opts.endedAt;
    row.time.wallClockMs =
      Date.parse(opts.endedAt) - Date.parse(opts.startedAt);
  }

  // Bottleneck heuristic
  const disp = row.outcomes.byPrimaryDisposition;
  if ((disp[PRIMARY_DISPOSITION.QUARANTINED] || 0) > pub) {
    row.outcomes.bottleneck = "evidence";
  } else if (pub === 0 && (disp[PRIMARY_DISPOSITION.INTERNAL_ENRICHMENT] || 0) > 0) {
    row.outcomes.bottleneck = "contact";
  } else if (pub === 0) {
    row.outcomes.bottleneck = "discovery";
  } else {
    row.outcomes.bottleneck = "publication";
  }

  return finalizeScorecardEfficiency(row);
}

/**
 * Build scorecard from buyer-contact enrichment result.
 */
export function scorecardFromBuyerEnrichment(result = {}, opts = {}) {
  const row = createEmptyScorecardRow({
    runId: opts.runId,
    taskId: `buyer_contact:${result.opportunityId}`,
    hotelId: result.hotelId || opts.hotelId,
    segmentId: opts.segmentId || "contact_enrichment",
    provider: result.surfe?.status === "accepted" ? "surfe" : "unpaid_public",
    mode: EXECUTION_MODE.FRESH_RESEARCH,
    objective: "Named-buyer / channel enrichment",
  });
  row.scope.status = "completed";
  row.depth.buyerSearches = 1;
  row.depth.candidatesAssessed = 1;
  row.depth.channelsFound = result.after?.customerContact?.email ||
    result.after?.customerContact?.phone
    ? 1
    : 0;
  if (result.materialImprovement) {
    row.outcomes.byPrimaryDisposition[
      PRIMARY_DISPOSITION.EXISTING_ENRICHED
    ] = 1;
    row.outcomes.contactImprovements = 1;
    row.outcomes.publicationByStatus[PUBLICATION_STATUS.PERSISTED] =
      opts.applied ? 1 : 0;
  } else {
    row.outcomes.byPrimaryDisposition[
      PRIMARY_DISPOSITION.INTERNAL_ENRICHMENT
    ] = 1;
  }
  row.cost.estimatedIncurredUsd =
    numOrUnknown(result.surfe?.estimatedCostUsd) ?? 0;
  row.cost.creditsConsumed =
    result.surfe?.creditsConsumed === undefined
      ? UNKNOWN
      : result.surfe.creditsConsumed;
  row.cost.costBasis = METRIC_BASIS.ESTIMATED;
  row.outcomes.bottleneck =
    result.surfe?.reason?.includes?.("no_named")
      ? "contact"
      : result.materialImprovement
        ? "publication"
        : "contact";
  return finalizeScorecardEfficiency(row);
}

/**
 * Aggregate hotel × segment comparison table rows.
 */
/**
 * Build hotel × segment comparison from scorecards and/or authoritative change ledger.
 * When ledgerSummary is provided, unique new/enriched/restored counts come from the ledger
 * (not raw disposition sums) so enrichment and restores do not inflate net-new.
 */
export function buildHotelSegmentComparisonTable(
  scorecards = [],
  { ledgerSummary = null } = {}
) {
  const byKey = new Map();
  for (const sc of scorecards) {
    const hotelId = sc.scope?.hotelId || "unknown";
    const segmentId = sc.scope?.segmentId || "unknown";
    const key = `${hotelId}::${segmentId}`;
    if (!byKey.has(key)) {
      byKey.set(key, {
        hotelId,
        segmentId,
        taskExecutionStatus:
          sc.scope?.taskExecutionStatus || sc.scope?.status,
        segmentCoverageStatus:
          sc.scope?.segmentCoverageStatus || SEGMENT_COVERAGE_STATUS.PARTIAL,
        depthCandidatesAssessed: 0,
        assessed: 0,
        netNewQualified: 0,
        enriched: 0,
        restored: 0,
        published: 0,
        estimatedCostUsd: 0,
        actualBilledUsd: null,
        wallClockMs: 0,
        bottlenecks: [],
        nextStep: null,
        modes: new Set(),
        taskIds: [],
      });
    }
    const agg = byKey.get(key);
    const disp = sc.outcomes?.byPrimaryDisposition || {};
    agg.assessed += sc.depth?.candidatesAssessed || 0;
    agg.depthCandidatesAssessed += sc.depth?.candidatesAssessed || 0;
    // Disposition-based provisional counts — overwritten by ledger when present
    agg.netNewQualified += disp[PRIMARY_DISPOSITION.NET_NEW_QUALIFIED] || 0;
    agg.enriched += disp[PRIMARY_DISPOSITION.EXISTING_ENRICHED] || 0;
    agg.published +=
      (sc.outcomes?.publicationByStatus?.[PUBLICATION_STATUS.PERSISTED] || 0) +
      (sc.outcomes?.publicationByStatus?.[
        PUBLICATION_STATUS.PRODUCTION_VERIFIED
      ] || 0) +
      (sc.outcomes?.publicationByStatus?.[PUBLICATION_STATUS.READY] || 0);
    agg.estimatedCostUsd += sc.cost?.estimatedIncurredUsd || 0;
    if (sc.cost?.actualBilledUsd != null) {
      agg.actualBilledUsd =
        (agg.actualBilledUsd || 0) + sc.cost.actualBilledUsd;
    }
    // Wall clock: take max of concurrent tasks, do not sum
    agg.wallClockMs = Math.max(agg.wallClockMs, sc.time?.wallClockMs || 0);
    if (sc.outcomes?.bottleneck) agg.bottlenecks.push(sc.outcomes.bottleneck);
    if (sc.scope?.mode) agg.modes.add(sc.scope.mode);
    if (sc.scope?.taskId) agg.taskIds.push(sc.scope.taskId);
    agg.taskExecutionStatus =
      sc.scope?.taskExecutionStatus ||
      sc.scope?.status ||
      agg.taskExecutionStatus;
    if (sc.scope?.segmentCoverageStatus) {
      agg.segmentCoverageStatus = sc.scope.segmentCoverageStatus;
    }
  }

  // Authoritative unique counts from record-level ledger
  if (ledgerSummary?.byHotelSegment) {
    for (const lr of ledgerSummary.byHotelSegment) {
      const key = `${lr.hotelId}::${lr.segmentId}`;
      if (!byKey.has(key)) {
        byKey.set(key, {
          hotelId: lr.hotelId,
          segmentId: lr.segmentId,
          taskExecutionStatus: TASK_EXECUTION_STATUS.COMPLETED,
          segmentCoverageStatus: SEGMENT_COVERAGE_STATUS.PARTIAL,
          assessed: lr.uniqueRecords,
          depthCandidatesAssessed: lr.uniqueRecords,
          netNewQualified: 0,
          enriched: 0,
          restored: 0,
          published: 0,
          estimatedCostUsd: 0,
          actualBilledUsd: null,
          wallClockMs: 0,
          bottlenecks: [],
          modes: new Set(["ledger"]),
          taskIds: [],
        });
      }
      const agg = byKey.get(key);
      agg.netNewQualified =
        (lr.newProspectFirstPublished || 0) +
        (lr.existingInternalPromoted || 0);
      // Enrichment count = unique records with enrichment change type.
      // contactEnrichmentActivities is an activity flag on those rows — do not add again.
      agg.enriched = lr.existingMateriallyEnriched || 0;
      agg.restored = lr.restoredReapplied || 0;
      agg.published = agg.netNewQualified; // facing publishes among unique new/promoted
      agg.countsSource = "opportunity_change_ledger";
    }
  }

  return [...byKey.values()].map((a) => {
    const bottleneck =
      a.bottlenecks.sort(
        (x, y) =>
          a.bottlenecks.filter((b) => b === y).length -
          a.bottlenecks.filter((b) => b === x).length
      )[0] || null;
    let nextStep = "continue_unpaid_gap_fill";
    if (bottleneck === "contact") nextStep = "named_buyer_research_queue";
    if (bottleneck === "evidence") nextStep = "verify_local_presence_or_sources";
    if (bottleneck === "discovery" && a.netNewQualified === 0) {
      nextStep = "bounded_fresh_discovery_if_budget";
    }
    if (a.netNewQualified > 0 && a.published === 0) {
      nextStep = "publish_qualified_pending";
    }
    if (
      a.segmentCoverageStatus === SEGMENT_COVERAGE_STATUS.PARTIAL &&
      a.taskExecutionStatus === TASK_EXECUTION_STATUS.COMPLETED
    ) {
      nextStep =
        nextStep === "continue_unpaid_gap_fill"
          ? "task_done_coverage_still_partial"
          : nextStep;
    }
    return {
      hotelId: a.hotelId,
      segmentId: a.segmentId,
      taskExecutionStatus: a.taskExecutionStatus,
      segmentCoverageStatus: a.segmentCoverageStatus,
      scopeStatus: `${a.taskExecutionStatus}/${a.segmentCoverageStatus}`,
      modes: [...a.modes],
      depthAssessed: a.assessed,
      netNewQualified: a.netNewQualified,
      enriched: a.enriched,
      restored: a.restored || 0,
      published: a.published,
      countsSource: a.countsSource || "disposition_provisional",
      costBasis: a.actualBilledUsd != null ? "mixed" : "estimated",
      estimatedCostUsd: Number(a.estimatedCostUsd.toFixed(4)),
      actualBilledUsd: a.actualBilledUsd,
      elapsedWallClockMs: a.wallClockMs || null,
      mainBottleneck: bottleneck,
      nextStep,
      taskIds: a.taskIds,
    };
  });
}

export function scorecardComparisonToMarkdown(rows = []) {
  const lines = [
    "| Hotel | Segment | Task exec | Coverage | Depth | Net-new | Enriched | Restored | Published | Cost (est.) | Elapsed ms | Bottleneck | Next step |",
    "|-------|---------|-----------|----------|------:|--------:|---------:|---------:|----------:|------------:|-----------:|------------|-----------|",
  ];
  for (const r of rows) {
    lines.push(
      `| ${r.hotelId} | ${r.segmentId} | ${r.taskExecutionStatus || r.scopeStatus} | ${r.segmentCoverageStatus || "partial"} | ${r.depthAssessed} | ${r.netNewQualified} | ${r.enriched} | ${r.restored ?? 0} | ${r.published} | $${r.estimatedCostUsd} | ${r.elapsedWallClockMs ?? "UNKNOWN"} | ${r.mainBottleneck || "—"} | ${r.nextStep} |`
    );
  }
  return lines.join("\n") + "\n";
}

export function writeScorecardArtifacts(doc, outDir) {
  fs.mkdirSync(outDir, { recursive: true });
  fs.writeFileSync(
    path.join(outDir, "research-scorecard.json"),
    JSON.stringify(doc, null, 2) + "\n"
  );
  const table = buildHotelSegmentComparisonTable(doc.tasks || []);
  fs.writeFileSync(
    path.join(outDir, "hotel-segment-comparison.json"),
    JSON.stringify({ generatedAt: doc.generatedAt, rows: table }, null, 2) +
      "\n"
  );
  fs.writeFileSync(
    path.join(outDir, "hotel-segment-comparison.md"),
    "# Hotel × segment research scorecard\n\n" +
      scorecardComparisonToMarkdown(table)
  );
  // CSV
  const csvHeader =
    "hotelId,segmentId,scopeStatus,depthAssessed,netNewQualified,enriched,published,estimatedCostUsd,elapsedWallClockMs,mainBottleneck,nextStep\n";
  const csvBody = table
    .map(
      (r) =>
        [
          r.hotelId,
          r.segmentId,
          r.scopeStatus,
          r.depthAssessed,
          r.netNewQualified,
          r.enriched,
          r.published,
          r.estimatedCostUsd,
          r.elapsedWallClockMs ?? "",
          r.mainBottleneck || "",
          r.nextStep,
        ].join(",")
    )
    .join("\n");
  fs.writeFileSync(
    path.join(outDir, "hotel-segment-comparison.csv"),
    csvHeader + csvBody + "\n"
  );
  return { outDir, table };
}
