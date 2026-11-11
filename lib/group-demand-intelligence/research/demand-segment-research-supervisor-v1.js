/**
 * Demand Segment Research Supervisor v1
 *
 * Independent durable ownership per demand segment for a hotel.
 * Does not replace Hotel Research Orchestrator or Opportunity Research Supervisor —
 * coordinates segment state, rediscovery rounds, exhaustion reopen audits,
 * and hotel-wide fair scheduling across segments.
 *
 * Queue drained ≠ segment complete.
 * Progress in one segment must not silently satisfy another.
 */

import fs from "node:fs";
import path from "node:path";
import {
  SUPERVISOR_EXPECTED_SEGMENTS,
  SEGMENT_RUN_STATUS,
  SEGMENT_TERMINAL_OK,
  RESEARCH_TERMINAL,
  STOP_REASONS,
  HARD_TERMINAL_STOP_REASONS,
  RESUMABLE_STOP_REASONS,
  canonicalizeSegmentId,
  ORCHESTRATOR_STOP_REASONS,
} from "./research-constants-v1.js";
import {
  assessSegmentResearchSufficiency,
  classifySegmentRunStatus,
  classifyWorkstreamResearchPosture,
} from "./segment-research-sufficiency-v1.js";
import { segmentCoverageGate } from "./segment-coverage-controller-v1.js";
import {
  remainingPublicStrategiesForBlocker,
  publicStrategiesForBlocker,
  PARALLEL_STRATEGY,
} from "./research-planner-v1.js";
import {
  buildOpportunityResearchDecisionRecord,
  mapCommercialDisposition,
  COMMERCIAL_DISPOSITION,
} from "./opportunity-research-decision-record-v1.js";
import {
  evaluateOpportunity,
  isActionableContactRoute,
} from "./opportunity-research-supervisor-v1.js";

export const SEGMENT_SUPERVISOR_VERSION =
  "gdi-demand-segment-research-supervisor-v1";

/** Expected source families per segment (coverage breadth targets). */
export const SEGMENT_SOURCE_FAMILIES_EXPECTED = Object.freeze({
  associations_government: [
    "official",
    "association_calendar",
    "cvb",
    "press",
  ],
  sports: ["official", "league_calendar", "venue", "press"],
  corporate_meetings_training: [
    "official",
    "corporate_events",
    "press",
    "cvb",
  ],
  entertainment_production: ["official", "production_call", "venue", "press"],
  medical_academic: [
    "official",
    "association_calendar",
    "academic_program",
    "press",
  ],
  tours_educational_specialist: ["official", "operator_programme", "press"],
  weddings_social_private_events: ["official", "venue", "planner", "press"],
  professional_planners_dmc: ["official", "dmc_profile", "press"],
  scaling_companies: ["official", "company_growth", "press"],
  local_meetings_catering: ["official", "local_business", "venue"],
  exhibitor_sponsor_teams: [
    "official",
    "exhibitor_list",
    "sponsor_prospectus",
    "press",
  ],
});

/** Demand mechanisms typically investigated per segment. */
export const SEGMENT_DEMAND_MECHANISMS_EXPECTED = Object.freeze({
  associations_government: [
    "annual_meeting",
    "committee_offsite",
    "delegates_housing",
  ],
  sports: ["team_travel", "event_staff", "fan_group"],
  corporate_meetings_training: [
    "training_offsite",
    "project_team",
    "board_retreat",
  ],
  entertainment_production: ["crew_housing", "talent_block"],
  medical_academic: ["conference_housing", "faculty_offsite", "exam_cohort"],
  tours_educational_specialist: ["group_tour_block", "study_tour"],
  weddings_social_private_events: ["wedding_block", "private_celebration"],
  professional_planners_dmc: ["inbound_programme", "incentive_group"],
  scaling_companies: ["growth_offsite", "hiring_cohort"],
  local_meetings_catering: ["day_meeting", "catered_reception"],
  exhibitor_sponsor_teams: ["exhibitor_team", "sponsor_hospitality"],
});

function now() {
  return new Date().toISOString();
}

function oppId(o) {
  return o?.id || o?.opportunityId || null;
}

function primarySegmentOf(o) {
  return canonicalizeSegmentId(
    o?.primarySegment || o?.segment || o?.researchSegment || null
  );
}

function secondarySegmentsOf(o) {
  const secs = [
    ...(Array.isArray(o?.secondarySegments) ? o.secondarySegments : []),
    ...(Array.isArray(o?.segments) ? o.segments : []),
  ]
    .map(canonicalizeSegmentId)
    .filter(Boolean);
  const primary = primarySegmentOf(o);
  return [...new Set(secs.filter((s) => s && s !== primary))];
}

export function defaultSegmentStateDir(hotelId, root = process.cwd()) {
  return path.join(
    root,
    "data",
    "group-demand-intelligence",
    "hotels",
    hotelId,
    "segment-research-state"
  );
}

/**
 * Create empty durable state for one segment.
 */
export function createEmptySegmentResearchState({
  hotelId,
  segment,
  applicable = true,
  applicabilityReason = null,
  researchPlan = null,
} = {}) {
  const seg = canonicalizeSegmentId(segment);
  return {
    version: SEGMENT_SUPERVISOR_VERSION,
    hotelId,
    segmentId: seg,
    applicable,
    applicabilityReason,
    researchPlan: researchPlan || {
      sourceFamiliesExpected:
        SEGMENT_SOURCE_FAMILIES_EXPECTED[seg] || ["official", "press", "web"],
      demandMechanismsExpected:
        SEGMENT_DEMAND_MECHANISMS_EXPECTED[seg] || ["future_program"],
      discoveryStrategies: [
        "event_to_demand",
        "paid_multisegment",
        "orchestrator_rediscovery",
      ],
    },
    sourceFamiliesExpected:
      SEGMENT_SOURCE_FAMILIES_EXPECTED[seg] || ["official", "press", "web"],
    sourceFamiliesResearched: [],
    demandMechanismsExpected:
      SEGMENT_DEMAND_MECHANISMS_EXPECTED[seg] || ["future_program"],
    demandMechanismsInvestigated: [],
    candidateIds: [],
    unresolvedCandidateIds: [],
    completedCandidateIds: [],
    secondaryAssociationIds: [],
    researchActions: [],
    evidenceRefs: [],
    providerUsage: { attempts: 0, costUsd: 0, byProvider: {} },
    coverageGaps: [],
    nextPlannedResearchActions: [],
    discoveryRounds: 0,
    lastDiscoveryAt: null,
    lastDiscoveryInserted: 0,
    completionDecision: null,
    completionRationale: null,
    status: SEGMENT_RUN_STATUS.PENDING,
    createdAt: now(),
    updatedAt: now(),
  };
}

export function loadSegmentResearchState(dir, segmentId) {
  const seg = canonicalizeSegmentId(segmentId);
  const file = path.join(dir, `${seg}.json`);
  if (!fs.existsSync(file)) return null;
  return JSON.parse(fs.readFileSync(file, "utf8"));
}

export function saveSegmentResearchState(dir, state) {
  fs.mkdirSync(dir, { recursive: true });
  const seg = canonicalizeSegmentId(state.segmentId);
  state.updatedAt = now();
  const file = path.join(dir, `${seg}.json`);
  // Atomic write — never leave a truncated segment state after interrupt.
  const tmp = `${file}.${process.pid}.${Date.now()}.tmp`;
  const body = `${JSON.stringify(state, null, 2)}\n`;
  fs.writeFileSync(tmp, body);
  try {
    fs.renameSync(tmp, file);
  } catch (err) {
    if (err?.code === "EPERM" || err?.code === "EEXIST" || err?.code === "EACCES") {
      try {
        fs.writeFileSync(file, body);
      } finally {
        try {
          fs.rmSync(tmp, { force: true });
        } catch {
          /* ignore */
        }
      }
    } else {
      try {
        fs.rmSync(tmp, { force: true });
      } catch {
        /* ignore */
      }
      throw err;
    }
  }
  return file;
}

export function loadAllSegmentResearchStates(
  dir,
  expectedSegments = SUPERVISOR_EXPECTED_SEGMENTS
) {
  return expectedSegments.map((seg) => {
    const existing = loadSegmentResearchState(dir, seg);
    return (
      existing ||
      createEmptySegmentResearchState({
        hotelId: path.basename(path.dirname(dir)),
        segment: seg,
      })
    );
  });
}

/**
 * Global opportunity identity key for cross-segment dedupe.
 * Prefer org/domain/title so distinct ids of the same commercial entity collide.
 * Fall back to record id only when org identity is empty.
 */
export function opportunityGlobalKey(o = {}) {
  const org = String(o.organizationName || o.name || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
  const domain = String(o.officialDomain || o.officialUrl || "")
    .toLowerCase()
    .replace(/^https?:\/\//, "")
    .replace(/\/$/, "");
  const title = String(o.title || o.eventName || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
  if (org || domain || title) {
    return `org:${org}|domain:${domain}|title:${title}`;
  }
  const id = oppId(o);
  return id ? `id:${id}` : `anon:${Math.random()}`;
}

/**
 * Assign candidates to segments with primary ownership + secondary associations.
 * Global dedupe by opportunityGlobalKey — first seen wins primary; later segments
 * get secondary credit only.
 */
export function assignCandidatesToSegments(
  opportunities = [],
  expectedSegments = SUPERVISOR_EXPECTED_SEGMENTS
) {
  const expected = expectedSegments.map(canonicalizeSegmentId);
  const bySeg = Object.fromEntries(
    expected.map((s) => [
      s,
      { primaryIds: [], secondaryIds: [], opportunities: [] },
    ])
  );
  const globalSeen = new Map(); // key → { id, primarySegment }
  const duplicates = [];

  for (const o of opportunities) {
    const id = oppId(o);
    if (!id) continue;
    const primary = primarySegmentOf(o);
    const key = opportunityGlobalKey(o);
    const prior = globalSeen.get(key);

    if (prior && prior.id !== id) {
      duplicates.push({
        opportunityId: id,
        duplicateOf: prior.id,
        key,
        primarySegment: primary,
      });
      // Still allow secondary association for coverage credit
      if (primary && bySeg[primary]) {
        if (!bySeg[primary].secondaryIds.includes(id)) {
          bySeg[primary].secondaryIds.push(id);
        }
      }
      continue;
    }

    if (!prior) {
      globalSeen.set(key, { id, primarySegment: primary });
    }

    if (primary && bySeg[primary]) {
      if (!bySeg[primary].primaryIds.includes(id)) {
        bySeg[primary].primaryIds.push(id);
        bySeg[primary].opportunities.push(o);
      }
    }

    for (const sec of secondarySegmentsOf(o)) {
      if (bySeg[sec] && !bySeg[sec].secondaryIds.includes(id)) {
        bySeg[sec].secondaryIds.push(id);
      }
    }
  }

  return { bySeg, globalSeen, duplicates };
}

function collectSourceFamilies(opportunities = []) {
  const families = new Set();
  for (const o of opportunities) {
    for (const e of o.evidence || []) {
      if (e.sourceFamily) families.add(String(e.sourceFamily));
    }
    for (const a of o.researchAttempts || []) {
      for (const f of a.sourceFamilies || []) families.add(String(f));
    }
    // Infer durable families from already-persisted customer / disposition rows
    // so paid MS + E2D evidence is not lost when attempt arrays were never written.
    if (o.officialUrl || o.officialDomain || o.officialSource) {
      families.add("official");
    }
    if ((o.sources || []).length > 0 || o.discoverySource) {
      families.add("web");
    }
    if (
      /paid_multisegment|disposition/i.test(String(o.discoverySource || "")) ||
      String(o.id || "").startsWith("gdi_disp_") ||
      String(o.id || "").startsWith("disp_")
    ) {
      families.add("paid_multisegment");
      families.add("press");
    }
    if (/event_to_demand/i.test(String(o.discoverySource || ""))) {
      families.add("event_calendar");
      families.add("official");
    }
    if (/private.?event|pe_/i.test(String(o.id || o.discoverySource || ""))) {
      families.add("venue");
      families.add("planner");
    }
    if (/local|chamber|catering|lmc_/i.test(String(o.id || o.segment || ""))) {
      families.add("local_business");
    }
    if (/dmc|planner|cohera/i.test(String(o.organizationName || o.id || ""))) {
      families.add("dmc_profile");
    }
  }
  return [...families];
}

function collectDemandMechanisms(opportunities = []) {
  const mechs = new Set();
  for (const o of opportunities) {
    const m =
      o.demandMechanism ||
      o.eventToDemand?.mechanism ||
      o.proposedHotelUseMechanism ||
      null;
    if (m) mechs.add(String(m));
    if (o.futureDemand?.supported) mechs.add("future_program");
    if (o.discoverySource === "event_to_demand_v1") mechs.add("event_to_demand");
  }
  return [...mechs];
}

function isTerminalOpportunity(o) {
  const st = String(o.researchStatus || "").toUpperCase();
  const disp = String(
    o.disposition ||
      o.commercialDisposition ||
      o.researchLayerDisposition ||
      o.terminalKind ||
      ""
  ).toUpperCase();
  const commercialTerminal =
    /MARKET_INTELLIGENCE|REJECTED|DUPLICATE|PARTNERSHIP|NOT_HOTEL_FACING|QUARANTINE|VALID_COMPLETE|RESEARCH_EXHAUSTED|QUALIFIED_PUBLISHABLE|EXPLORATORY|ENRICHMENT_COMPLETE|VENUE_ONLY|SUPPLIER_ONLY/.test(
      disp
    );
  return (
    HARD_TERMINAL_STOP_REASONS.includes(o.stopReason) ||
    o.researchStatus === RESEARCH_TERMINAL.PUBLISHABLE_PROSPECT ||
    o.researchStatus === RESEARCH_TERMINAL.EXPLORATORY_PROSPECT ||
    o.customerVisible === true ||
    // Bethesda (and peers) often persist salesperson-facing rows as QUALIFIED
    // without supervisor PUBLISHABLE/EXPLORATORY enums — still a commercial
    // research disposition, not an open maturation queue item.
    st === "QUALIFIED" ||
    st === "QUALIFIED_PROSPECT" ||
    st === "STOPPED_WITH_LEGITIMATE_REASON" ||
    o.prospectHotelFacing === true ||
    commercialTerminal ||
    // Staged Ready / HF packets are commercially dispositioned even when not published
    (o.ready === true &&
      /HOTEL_FACING|QUALIFIED/i.test(String(o.eligibility || ""))) ||
    (o.hotelFacing === true &&
      o.ready === true &&
      Boolean(o.stopReason || o.researchStatus || o.eligibility))
  );
}

function isUnresolvedOpportunity(o, { paidBlocked = true } = {}) {
  // When paid research is not authorized for this hotel wave, PUBLIC_PATH_EXHAUSTED
  // is a durable commercial/research disposition — not an open maturation queue item.
  // (Leaving it "unresolved forever" made every Bethesda segment impossible to close.)
  if (
    paidBlocked &&
    (o.stopReason === STOP_REASONS.PUBLIC_PATH_EXHAUSTED ||
      o.stopReason === STOP_REASONS.BUDGET_BLOCKED ||
      o.stopReason === STOP_REASONS.BUDGET_LIMIT_REACHED)
  ) {
    return false;
  }
  if (isTerminalOpportunity(o) && !RESUMABLE_STOP_REASONS.includes(o.stopReason)) {
    // Terminal hard stops are resolved; resumable are not
    if (RESUMABLE_STOP_REASONS.includes(o.stopReason)) return true;
    return false;
  }
  if (RESUMABLE_STOP_REASONS.includes(o.stopReason)) return true;
  if (o.researchStatus === RESEARCH_TERMINAL.IN_PROGRESS) return true;
  if (!o.researchStatus && !o.stopReason) return true;
  return false;
}

/**
 * Sync durable segment state from live opportunity corpus.
 * Secondary associations get coverage credit but do not count as primary resolved.
 */
export function syncSegmentStateFromCorpus(
  state,
  opportunities = [],
  { assignment = null, paidBlocked = true } = {}
) {
  const seg = canonicalizeSegmentId(state.segmentId);
  const assign =
    assignment ||
    assignCandidatesToSegments(opportunities, [seg]);
  const bucket = assign.bySeg[seg] || {
    primaryIds: [],
    secondaryIds: [],
    opportunities: [],
  };
  const byId = new Map(opportunities.map((o) => [oppId(o), o]));

  // ALWAYS union prior research-stage candidate IDs with FS corpus assignment.
  // Prior bug (Bethesda wipe): when FS corpus had a non-empty partial set,
  // primaryIds was replaced wholesale and staged gdi_disp_*/gdi_e2d_*/pe_* /
  // shadow IDs not present in opportunities.json were silently erased.
  const priorCandidateIds = [...(state.candidateIds || [])].filter(Boolean);
  const priorCompletedIds = [...(state.completedCandidateIds || [])].filter(Boolean);
  const priorUnresolvedIds = [...(state.unresolvedCandidateIds || [])].filter(Boolean);
  const corpusPrimaryIds = [...bucket.primaryIds].filter(Boolean);
  const secondaryIds = [...bucket.secondaryIds];

  const primaryIds = [
    ...new Set([
      ...corpusPrimaryIds,
      ...priorCandidateIds,
      ...priorUnresolvedIds,
      ...priorCompletedIds,
    ]),
  ];
  const preservedFromPrior = priorCandidateIds.filter(
    (id) => !corpusPrimaryIds.includes(id)
  );

  const primaryOpps = primaryIds.map((id) => byId.get(id)).filter(Boolean);
  const missingFromCorpus = primaryIds.filter((id) => !byId.has(id));

  const unresolved = [];
  const completed = [];
  for (const id of primaryIds) {
    const o = byId.get(id);
    if (!o) {
      // Previously tracked but absent from this corpus pass — keep durable.
      // Prefer prior completed membership; otherwise remain unresolved so
      // research-stage IDs are not dropped by FS-only sync.
      if (priorCompletedIds.includes(id)) completed.push(id);
      else unresolved.push(id);
      continue;
    }
    if (isUnresolvedOpportunity(o, { paidBlocked })) unresolved.push(id);
    else if (isTerminalOpportunity(o)) completed.push(id);
    else unresolved.push(id);
  }

  const families = collectSourceFamilies(primaryOpps);
  const mechanisms = collectDemandMechanisms(primaryOpps);

  let costUsd = Number(state.providerUsage?.costUsd || 0);
  const byProvider = { ...(state.providerUsage?.byProvider || {}) };
  let attempts = Number(state.providerUsage?.attempts || 0);
  for (const o of primaryOpps) {
    for (const a of o.researchAttempts || []) {
      attempts += 1;
      const c = Number(a.costUsd ?? a.actualCostUsd ?? 0);
      costUsd += c;
      const p = a.provider || "unknown";
      byProvider[p] = (byProvider[p] || 0) + c;
    }
  }

  const next = {
    ...state,
    candidateIds: primaryIds,
    secondaryAssociationIds: [
      ...new Set([...(state.secondaryAssociationIds || []), ...secondaryIds]),
    ],
    unresolvedCandidateIds: unresolved,
    completedCandidateIds: completed,
    sourceFamiliesResearched: [
      ...new Set([...(state.sourceFamiliesResearched || []), ...families]),
    ],
    demandMechanismsInvestigated: [
      ...new Set([
        ...(state.demandMechanismsInvestigated || []),
        ...mechanisms,
      ]),
    ],
    providerUsage: {
      attempts,
      costUsd,
      byProvider,
    },
    corpusSyncMeta: {
      at: now(),
      primaryFromAssignment: corpusPrimaryIds.length,
      preservedPriorIds: preservedFromPrior.length,
      preservedPriorIdSample: preservedFromPrior.slice(0, 12),
      missingFromCorpusCount: missingFromCorpus.length,
      syncMode: "UNION_PRIOR_AND_CORPUS",
    },
    updatedAt: now(),
  };

  // Coverage gaps (segment-local — never borrow from other segments).
  // Missing expected families remain informational once multi-family research
  // already exists — they do not auto-force infinite rediscovery alone.
  const expectedFam = next.sourceFamiliesExpected || [];
  const missingFam = expectedFam.filter(
    (f) => !next.sourceFamiliesResearched.includes(f)
  );
  const expectedMech = next.demandMechanismsExpected || [];
  const missingMech = expectedMech.filter(
    (f) => !next.demandMechanismsInvestigated.includes(f)
  );
  const researchedBreadth = (next.sourceFamiliesResearched || []).length >= 2;
  const gapFamilies =
    researchedBreadth && unresolved.length === 0 ? [] : missingFam;
  const gapMechs =
    researchedBreadth &&
    (next.demandMechanismsInvestigated || []).length >= 1 &&
    unresolved.length === 0
      ? []
      : missingMech;
  next.coverageGaps = [
    ...gapFamilies.map((f) => ({ kind: "source_family", id: f })),
    ...gapMechs.map((m) => ({ kind: "demand_mechanism", id: m })),
    ...(unresolved.length
      ? [{ kind: "unresolved_candidates", count: unresolved.length }]
      : []),
  ];

  return next;
}

/**
 * Credit a completed paid multisegment (or equivalent) discovery pass onto
 * durable segment state without re-spending.
 */
export function creditPaidMultisegmentEvidence(state, credit = {}) {
  const families = [
    ...(state.sourceFamiliesResearched || []),
    ...(credit.sourceFamilies || ["paid_multisegment", "web", "press"]),
  ];
  const mechanisms = [
    ...(state.demandMechanismsInvestigated || []),
    ...(credit.demandMechanisms || []),
  ];
  const rounds = Math.max(
    Number(state.discoveryRounds || 0),
    Number(credit.discoveryRounds || 1)
  );
  const actions = [
    ...(state.researchActions || []),
    {
      at: now(),
      kind: "paid_multisegment",
      strategy: "paid_multisegment",
      providerRunId: credit.providerRunId || null,
      dispositionCount: credit.dispositionCount ?? null,
      note: credit.note || "credited_prior_paid_ms_without_respend",
    },
  ];
  return {
    ...state,
    paidMultisegmentCredited: true,
    discoveryRounds: rounds,
    lastDiscoveryAt: state.lastDiscoveryAt || now(),
    sourceFamiliesResearched: [...new Set(families)],
    demandMechanismsInvestigated: [...new Set(mechanisms)],
    researchActions: actions,
    evidenceRefs: [
      ...new Set([
        ...(state.evidenceRefs || []),
        ...(credit.providerRunId ? [`providerRunId:${credit.providerRunId}`] : []),
        ...(credit.evidenceRefs || []),
      ]),
    ],
    updatedAt: now(),
  };
}

/**
 * Derive furtherResearchUnlikely from durable evidence — never require a prior
 * SUFFICIENT completionDecision (that circular gate made SUFFICIENT unreachable).
 */
export function deriveFurtherResearchUnlikely(state = {}) {
  const decision = String(state.completionDecision || "");
  if (decision === "SUFFICIENT" || decision === "SUFFICIENT_ZERO_YIELD") {
    return true;
  }
  if (decision === "BLOCKED_DEFENSIBLE") return true;

  const unresolved = (state.unresolvedCandidateIds || []).length;
  const candidates = (state.candidateIds || []).length;
  const completed = (state.completedCandidateIds || []).length;
  const families = (state.sourceFamiliesResearched || []).length;
  const rounds = Number(state.discoveryRounds || 0);
  const actions = state.researchActions || [];
  const discoveryLike = actions.filter((a) =>
    /discovery|paid_multisegment|rediscovery|event_to_demand/i.test(
      `${a.kind || ""} ${a.strategy || ""} ${a.reason || ""}`
    )
  ).length;
  const opportunityPasses = actions.filter((a) =>
    /opportunity_research|mature|reopen|disposition/i.test(
      `${a.kind || ""} ${a.strategy || ""}`
    )
  ).length;
  const rationale = String(state.completionRationale || "");
  if (
    /REDISCOVERY_ZERO_YIELD|PUBLIC_PATH_EXHAUSTED|EVIDENCE_SUPPORTED_STOP|RESEARCH_EXHAUSTED|HONEST_ZERO|PAID_MS_CREDITED_WITH_DISPOSITIONS/i.test(
      rationale
    )
  ) {
    return true;
  }

  const allDisposed =
    candidates > 0 && unresolved === 0 && completed === candidates;
  const multiFamily = families >= 2;
  const paidOrDiscoveryDepth = rounds >= 1 || discoveryLike >= 1;

  // Disposed corpus + multi-family coverage + at least one discovery/maturation pass
  if (
    allDisposed &&
    multiFamily &&
    (paidOrDiscoveryDepth || opportunityPasses >= 1 || rounds >= 1)
  ) {
    return true;
  }

  // Honest zero after documented multi-family discovery depth
  if (
    candidates === 0 &&
    multiFamily &&
    (rounds >= 2 || discoveryLike >= 2 || (rounds >= 1 && discoveryLike >= 1))
  ) {
    return true;
  }

  // Rediscovery capped at zero yield with no open queue
  if (
    unresolved === 0 &&
    rounds >= 2 &&
    Number(state.lastDiscoveryInserted || 0) === 0 &&
    multiFamily
  ) {
    return true;
  }

  return false;
}

/**
 * Build sufficiency run object from durable segment state (not borrowed corpus).
 */
export function segmentStateToSufficiencyRun(state) {
  const discovered = (state.candidateIds || []).length;
  const terminated = (state.completedCandidateIds || []).length;
  const discoveryRounds = Number(state.discoveryRounds || 0);
  const actions = state.researchActions || [];
  const discoveryActionCount = actions.filter((a) =>
    /discovery|paid_multisegment|rediscovery|event_to_demand/i.test(
      `${a.kind || ""} ${a.strategy || ""}`
    )
  ).length;
  const strategySet = new Set(
    actions
      .map((a) => a.strategy || a.targetSourceFamily || a.kind)
      .filter(Boolean)
  );
  const familyCount = (state.sourceFamiliesResearched || []).length;
  // Prior paid MS + staged dispositions often leave discoveryRounds=0 even though
  // independent research already happened — credit breadth + completed inventory.
  const independentTasks = Math.max(
    discoveryRounds,
    discoveryActionCount,
    discovered > 0 && discoveryRounds === 0 ? 1 : 0,
    discovered > 0 && terminated === discovered && familyCount >= 2 ? 2 : 0,
    familyCount >= 2 && discoveryRounds >= 1 ? 2 : 0,
    state.paidMultisegmentCredited ? 1 : 0
  );

  const furtherUnlikely = deriveFurtherResearchUnlikely(state);
  const stoppingReason =
    state.completionRationale ||
    (furtherUnlikely
      ? "EVIDENCE_SUPPORTED_STOP_AFTER_DISPOSITIONS_AND_COVERAGE"
      : null);

  return {
    segment: state.segmentId,
    applicable: state.applicable !== false,
    applicabilityReason: state.applicabilityReason,
    blocked: state.status === SEGMENT_RUN_STATUS.BLOCKED,
    blockedReason: state.completionRationale,
    independentTasks,
    sourceFamilies: state.sourceFamiliesResearched || [],
    candidatesDiscovered: discovered,
    candidatesTerminated: terminated,
    followUpPasses: Math.max(
      actions.filter((a) =>
        /opportunity_research|reopen|mature|disposition|coverage_assessment/i.test(
          String(a.kind || "")
        )
      ).length > 1
        ? 1
        : 0,
      actions.filter(
        (a) => a.kind === "opportunity_research" || a.kind === "reopen"
      ).length,
      terminated > 0 && discovered === terminated ? 1 : 0
    ),
    strategiesTried: Math.max(strategySet.size, discoveryRounds > 0 ? 1 : 0),
    unresolvedBlockers: (state.coverageGaps || [])
      .filter((g) => g.kind === "unresolved_candidates")
      .map((g) => `unresolved:${g.count}`),
    furtherResearchUnlikely: furtherUnlikely,
    stoppingReason,
  };
}

/**
 * Independently assess whether a segment is sufficiently researched.
 */
export function assessSegmentCoverageFromState(state, opportunities = []) {
  if (state.applicable === false) {
    return {
      status: SEGMENT_RUN_STATUS.NOT_APPLICABLE,
      reason: state.applicabilityReason || "DECLARED_NOT_APPLICABLE",
      sufficient: true,
      posture: "NOT_APPLICABLE",
    };
  }

  const run = segmentStateToSufficiencyRun(state);
  const classified = classifySegmentRunStatus(run);
  const posture = classifyWorkstreamResearchPosture(state, opportunities);

  // Queue drained alone is never complete
  if (
    (state.unresolvedCandidateIds || []).length === 0 &&
    (state.candidateIds || []).length > 0 &&
    !assessSegmentResearchSufficiency(run).sufficient
  ) {
    return {
      status: SEGMENT_RUN_STATUS.INSUFFICIENT_RESEARCH_DEPTH,
      reason: "QUEUE_DRAINED_BUT_COVERAGE_INSUFFICIENT",
      sufficient: false,
      posture,
      classified,
      run,
    };
  }

  if (classified.status === SEGMENT_RUN_STATUS.COMPLETE && posture !== "DEMAND_VERIFIED") {
    return {
      status: SEGMENT_RUN_STATUS.INSUFFICIENT_RESEARCH_DEPTH,
      reason: "BUYERS_DISCOVERED_DEMAND_NOT_VERIFIED",
      sufficient: false,
      posture,
      classified,
      run,
    };
  }

  return {
    status: classified.status,
    reason: classified.reason,
    sufficient: SEGMENT_TERMINAL_OK.includes(classified.status),
    posture,
    classified,
    run,
  };
}

/**
 * Audit previously stopped opportunities for remaining justified research paths.
 * Does NOT reopen everything — only when material public strategies remain.
 */
export function auditExhaustionForReopen(opportunity = {}, opts = {}) {
  const stop = opportunity.stopReason;
  const status = opportunity.researchStatus;

  // Never reopen Ready / publishable / exploratory hotel-facing without explicit force
  const stUpper = String(status || "").toUpperCase();
  if (
    !opts.force &&
    (status === RESEARCH_TERMINAL.PUBLISHABLE_PROSPECT ||
      status === RESEARCH_TERMINAL.EXPLORATORY_PROSPECT ||
      opportunity.customerVisible === true ||
      stUpper === "QUALIFIED" ||
      stUpper === "QUALIFIED_PROSPECT" ||
      (opportunity.ready === true &&
        /HOTEL_FACING|QUALIFIED/i.test(String(opportunity.eligibility || ""))) ||
      /VENUE_ONLY|MARKET_INTELLIGENCE|PARTNERSHIP|LEGITIMATELY_REJECTED|BELOW_COMMERCIAL/i.test(
        String(
          opportunity.disposition ||
            opportunity.commercialDisposition ||
            opportunity.researchLayerDisposition ||
            ""
        )
      ))
  ) {
    return {
      reopen: false,
      reason: "PRESERVE_HOTEL_FACING",
      remainingStrategies: [],
    };
  }

  // Paid-blocked hotel waves: public-path / research exhaustion is terminal
  // for segment coverage (no authorized Parallel follow-up this wave).
  if (
    opts.paidBlocked !== false &&
    (stop === STOP_REASONS.PUBLIC_PATH_EXHAUSTED ||
      stop === STOP_REASONS.RESEARCH_EXHAUSTED ||
      stop === STOP_REASONS.BUDGET_BLOCKED ||
      stop === STOP_REASONS.BUDGET_LIMIT_REACHED) &&
    !opts.force
  ) {
    return {
      reopen: false,
      reason: "EXHAUSTED_PAID_BLOCKED",
      remainingStrategies: [],
    };
  }

  const eligibleStops = new Set([
    STOP_REASONS.NO_FUTURE_DEMAND_EVIDENCE,
    STOP_REASONS.RESEARCH_EXHAUSTED,
    STOP_REASONS.PUBLIC_PATH_EXHAUSTED,
    STOP_REASONS.PAID_FOLLOWUP_AVAILABLE,
    STOP_REASONS.PROVIDER_BLOCKED,
  ]);
  if (!eligibleStops.has(stop) && !opts.force) {
    return {
      reopen: false,
      reason: "STOP_NOT_ELIGIBLE_FOR_REOPEN",
      remainingStrategies: [],
    };
  }

  const evalResult = evaluateOpportunity(opportunity, opts);
  const blockerKey =
    evalResult.blockers?.[0]?.key ||
    (opportunity.researchAttempts || []).slice(-1)[0]?.blocker ||
    "future_demand";

  const remaining = remainingPublicStrategiesForBlocker(
    opportunity,
    blockerKey
  );

  // Prefer remaining public over re-running Parallel
  if (remaining.length > 0) {
    return {
      reopen: true,
      reason: "UNTRIED_PUBLIC_STRATEGIES_REMAIN",
      blockerKey,
      remainingStrategies: remaining,
      nextStrategy: remaining[0],
      avoidParallelUntilPublicDone: true,
    };
  }

  // All public done; Parallel available and not tried → paid follow-up
  const publics = publicStrategiesForBlocker(blockerKey);
  const parallelTried = (opportunity.researchAttempts || []).some(
    (a) =>
      a.strategy === PARALLEL_STRATEGY || a.provider === "parallel"
  );
  if (
    publics.length > 0 &&
    remaining.length === 0 &&
    !parallelTried &&
    opts.paidBlocked !== true
  ) {
    return {
      reopen: true,
      reason: "PAID_FOLLOWUP_STILL_AVAILABLE",
      blockerKey,
      remainingStrategies: [PARALLEL_STRATEGY],
      nextStrategy: PARALLEL_STRATEGY,
      avoidParallelUntilPublicDone: false,
    };
  }

  return {
    reopen: false,
    reason: "NO_JUSTIFIED_PATH_REMAINING",
    blockerKey,
    remainingStrategies: [],
    researchedStrategies: (opportunity.researchAttempts || [])
      .map((a) => a.strategy)
      .filter(Boolean),
  };
}

/**
 * Apply reopen: clear hard stop, seed reopenStrategies, mark IN_PROGRESS.
 */
export function applyJustifiedReopen(opportunity, audit) {
  if (!audit?.reopen) return opportunity;
  return {
    ...opportunity,
    stopReason: null,
    researchStatus: RESEARCH_TERMINAL.IN_PROGRESS,
    reopenStrategies: [
      ...new Set([
        ...(opportunity.reopenStrategies || []),
        ...(audit.remainingStrategies || []),
      ]),
    ],
    notes: [
      ...(opportunity.notes || []),
      `segment_supervisor_reopen:${audit.reason}:${audit.nextStrategy}`,
    ],
    statusHistory: [
      ...(opportunity.statusHistory || []),
      {
        at: now(),
        from: opportunity.stopReason,
        to: "REOPENED",
        reason: audit.reason,
        nextStrategy: audit.nextStrategy,
      },
    ],
  };
}

/**
 * Decide next discovery action for an under-covered segment.
 */
export function planSegmentRediscovery(state, coverageAssessment) {
  if (state.applicable === false) {
    return { action: null, reason: "NOT_APPLICABLE" };
  }
  if (coverageAssessment?.sufficient) {
    return { action: null, reason: "SEGMENT_SUFFICIENT" };
  }

  const gaps = state.coverageGaps || [];
  const missingFamily = gaps.find((g) => g.kind === "source_family");
  const missingMech = gaps.find((g) => g.kind === "demand_mechanism");
  const rounds = Number(state.discoveryRounds || 0);
  const unresolved = state.unresolvedCandidateIds || [];

  // Cap rediscovery rounds to prevent infinite loops
  if (rounds >= 4 && (state.lastDiscoveryInserted || 0) === 0) {
    return {
      action: "MARK_BLOCKED_DEFENSIBLE",
      reason: "REDISCOVERY_ZERO_YIELD_AFTER_MAX_ROUNDS",
      strategy: null,
    };
  }

  // Prefer resolving open candidates (incl. justified reopens) before rediscovery
  if (unresolved.length > 0) {
    return {
      action: "MATURE_UNRESOLVED",
      reason: "UNRESOLVED_CANDIDATES_REMAIN",
      candidateIds: unresolved,
      segmentId: state.segmentId,
    };
  }

  if ((state.candidateIds || []).length === 0) {
    return {
      action: "DISCOVERY",
      reason: "ZERO_CANDIDATES",
      strategy: "event_to_demand",
      segmentId: state.segmentId,
    };
  }

  if (missingFamily) {
    return {
      action: "DISCOVERY",
      reason: `MISSING_SOURCE_FAMILY:${missingFamily.id}`,
      strategy: "event_to_demand",
      targetSourceFamily: missingFamily.id,
      segmentId: state.segmentId,
    };
  }
  if (missingMech) {
    return {
      action: "DISCOVERY",
      reason: `MISSING_DEMAND_MECHANISM:${missingMech.id}`,
      strategy: "event_to_demand",
      targetMechanism: missingMech.id,
      segmentId: state.segmentId,
    };
  }

  return {
    action: "DISCOVERY",
    reason: "COVERAGE_INSUFFICIENT_AFTER_QUEUE_DRAIN",
    strategy: "event_to_demand",
    segmentId: state.segmentId,
  };
}

/**
 * Record a research/discovery action on segment state.
 */
export function recordSegmentResearchAction(state, action) {
  const entry = {
    at: now(),
    ...action,
  };
  return {
    ...state,
    researchActions: [...(state.researchActions || []), entry],
    nextPlannedResearchActions: action.nextActions || [],
    updatedAt: now(),
  };
}

/**
 * Mark segment completion with explicit rationale.
 */
export function finalizeSegmentCompletion(state, assessment) {
  if (assessment.sufficient) {
    const zero = (state.candidateIds || []).length === 0;
    return {
      ...state,
      status: assessment.status,
      completionDecision: zero
        ? "SUFFICIENT_ZERO_YIELD"
        : "SUFFICIENT",
      completionRationale: assessment.reason,
      updatedAt: now(),
    };
  }
  if (assessment.status === SEGMENT_RUN_STATUS.BLOCKED) {
    return {
      ...state,
      status: SEGMENT_RUN_STATUS.BLOCKED,
      completionDecision: "BLOCKED_DEFENSIBLE",
      completionRationale: assessment.reason,
      updatedAt: now(),
    };
  }
  return {
    ...state,
    status: assessment.status || SEGMENT_RUN_STATUS.INSUFFICIENT_RESEARCH_DEPTH,
    completionDecision: null,
    completionRationale: assessment.reason,
    updatedAt: now(),
  };
}

/**
 * Hotel-level: enumerate segments, sync state, schedule next actions fairly.
 * Prevents one segment from consuming entire budget without EV justification.
 */
export function coordinateHotelSegmentSupervision({
  hotelId,
  opportunities = [],
  expectedSegments = SUPERVISOR_EXPECTED_SEGMENTS,
  stateDir,
  remainingBudgetUsd = Infinity,
  maxSegmentBudgetShare = 0.35,
  paidBlocked = true,
} = {}) {
  const dir = stateDir || defaultSegmentStateDir(hotelId);
  fs.mkdirSync(dir, { recursive: true });

  const assignment = assignCandidatesToSegments(
    opportunities,
    expectedSegments
  );
  const states = [];
  const decisions = [];
  const reopenPlan = [];

  for (const seg of expectedSegments) {
    let state =
      loadSegmentResearchState(dir, seg) ||
      createEmptySegmentResearchState({ hotelId, segment: seg });
    state = syncSegmentStateFromCorpus(state, opportunities, {
      assignment,
      paidBlocked,
    });
    const assessment = assessSegmentCoverageFromState(state, opportunities);
    state = finalizeSegmentCompletion(state, assessment);

    // Reopen audit for primary candidates in this segment.
    // Include unresolved IDs: PUBLIC_PATH_EXHAUSTED / IN_PROGRESS often sit in
    // unresolvedCandidateIds waiting for justified Parallel follow-up — auditing
    // only completedCandidateIds silently dropped those (false "reopenPlan: 0").
    const byId = new Map(opportunities.map((o) => [oppId(o), o]));
    const reopenAuditIds = [
      ...new Set([
        ...(state.completedCandidateIds || []),
        ...(state.unresolvedCandidateIds || []),
      ]),
    ];
    let reopenMutated = false;
    for (const id of reopenAuditIds) {
      const o = byId.get(id);
      if (!o) continue;
      const audit = auditExhaustionForReopen(o, { paidBlocked });
      if (audit.reopen) {
        reopenPlan.push({
          opportunityId: id,
          segmentId: seg,
          ...audit,
        });
        if (!state.unresolvedCandidateIds.includes(id)) {
          state.unresolvedCandidateIds = [
            ...state.unresolvedCandidateIds,
            id,
          ];
          state.completedCandidateIds = state.completedCandidateIds.filter(
            (x) => x !== id
          );
          reopenMutated = true;
        }
      }
    }

    // Reopen can invalidate a premature SUFFICIENT finalize — reassess after mutation.
    let finalAssessment = assessment;
    if (reopenMutated) {
      finalAssessment = assessSegmentCoverageFromState(state, opportunities);
      state = finalizeSegmentCompletion(state, finalAssessment);
    }

    const plan = planSegmentRediscovery(state, finalAssessment);
    state.nextPlannedResearchActions = plan.action
      ? [plan]
      : [];
    state = recordSegmentResearchAction(state, {
      kind: "coverage_assessment",
      status: finalAssessment.status,
      reason: finalAssessment.reason,
      nextActions: state.nextPlannedResearchActions,
    });

    saveSegmentResearchState(dir, state);
    states.push(state);

    for (const id of state.candidateIds || []) {
      const o = byId.get(id);
      if (o) decisions.push(buildOpportunityResearchDecisionRecord(o));
    }
  }

  // Fair schedule: prefer segments with planned actions, lowest spend share
  const totalSpend = states.reduce(
    (n, s) => n + Number(s.providerUsage?.costUsd || 0),
    0
  );
  const actionable = states
    .filter((s) => (s.nextPlannedResearchActions || []).length > 0)
    .map((s) => {
      const spend = Number(s.providerUsage?.costUsd || 0);
      const share = totalSpend > 0 ? spend / totalSpend : 0;
      const budgetBlocked =
        Number.isFinite(remainingBudgetUsd) &&
        remainingBudgetUsd <= 0;
      const overShare =
        share > maxSegmentBudgetShare &&
        states.filter((x) => (x.nextPlannedResearchActions || []).length > 0)
          .length > 1;
      return {
        segmentId: s.segmentId,
        action: s.nextPlannedResearchActions[0],
        spendUsd: spend,
        share,
        deferred: budgetBlocked || overShare,
        deferReason: budgetBlocked
          ? "HOTEL_BUDGET_EXHAUSTED"
          : overShare
            ? "SEGMENT_BUDGET_SHARE_CAP"
            : null,
      };
    })
    .sort((a, b) => {
      if (a.deferred !== b.deferred) return a.deferred ? 1 : -1;
      return a.spendUsd - b.spendUsd;
    });

  const coverageRows = states.map((s) => {
    const a = assessSegmentCoverageFromState(s, opportunities);
    return {
      segment: s.segmentId,
      coverage: a.status,
      candidates: (s.candidateIds || []).length,
      resolved: (s.completedCandidateIds || []).length,
      unresolved: (s.unresolvedCandidateIds || []).length,
      newDiscovery: s.lastDiscoveryInserted || 0,
      nextAction: s.nextPlannedResearchActions?.[0]?.action || null,
      status: a.status,
      reason: a.reason,
    };
  });

  const coverage = segmentCoverageGate({
    expectedSegments,
    segmentRuns: states.map(segmentStateToSufficiencyRun),
  });

  const hotelStop = coverage.pass
    ? ORCHESTRATOR_STOP_REASONS.NO_JUSTIFIED_WORK_REMAINING
    : ORCHESTRATOR_STOP_REASONS.DISCOVERY_INCOMPLETE;

  return {
    version: SEGMENT_SUPERVISOR_VERSION,
    hotelId,
    stateDir: dir,
    states,
    coverage,
    coverageRows,
    schedule: actionable,
    reopenPlan,
    decisions,
    duplicates: assignment.duplicates,
    hotelAssessment: {
      stopReason: hotelStop,
      segmentsExpected: expectedSegments.length,
      segmentsSufficient: coverageRows.filter((r) =>
        SEGMENT_TERMINAL_OK.includes(r.status)
      ).length,
      segmentsBlocked: coverageRows.filter(
        (r) => r.status === SEGMENT_RUN_STATUS.BLOCKED
      ).length,
      segmentsInsufficient: coverageRows.filter(
        (r) =>
          r.status === SEGMENT_RUN_STATUS.INSUFFICIENT_RESEARCH_DEPTH ||
          r.status === SEGMENT_RUN_STATUS.PENDING ||
          r.status === SEGMENT_RUN_STATUS.IN_PROGRESS
      ).length,
      remainingBudgetUsd,
      reopenCandidates: reopenPlan.length,
    },
    commercialDispositionCounts: decisions.reduce((m, d) => {
      const k = d.commercialDisposition || "UNKNOWN";
      m[k] = (m[k] || 0) + 1;
      return m;
    }, {}),
  };
}

/**
 * After a discovery round inserts candidates into a segment.
 */
export function applyDiscoveryRoundToSegment(
  state,
  { insertedIds = [], mode = null, note = null } = {}
) {
  const ids = insertedIds.filter(Boolean);
  return {
    ...recordSegmentResearchAction(state, {
      kind: "discovery",
      strategy: mode || "event_to_demand",
      inserted: ids.length,
      insertedIds: ids,
      note,
    }),
    discoveryRounds: Number(state.discoveryRounds || 0) + 1,
    lastDiscoveryAt: now(),
    lastDiscoveryInserted: ids.length,
    candidateIds: [...new Set([...(state.candidateIds || []), ...ids])],
    unresolvedCandidateIds: [
      ...new Set([...(state.unresolvedCandidateIds || []), ...ids]),
    ],
    status: SEGMENT_RUN_STATUS.IN_PROGRESS,
    completionDecision: null,
  };
}

/**
 * Build Full E2E segmentCoverage axes from durable segment states.
 */
export function segmentStatesToFullE2eCoverageAxes(states = [], opportunities = []) {
  return states.map((s) => {
    const assessment = assessSegmentCoverageFromState(s, opportunities);
    const complete = SEGMENT_TERMINAL_OK.includes(assessment.status);
    return {
      segmentId: s.segmentId,
      applicable: s.applicable !== false,
      researched: (s.candidateIds || []).length > 0 || Number(s.discoveryRounds || 0) > 0,
      complete,
      researchedZeroYield:
        complete &&
        (s.candidateIds || []).length === 0 &&
        Number(s.discoveryRounds || 0) > 0,
      coverageStatus: assessment.status,
      coverageReason: assessment.reason,
      independentFromOtherSegments: true,
      discoveryRounds: s.discoveryRounds || 0,
      primaryCandidates: (s.candidateIds || []).length,
      secondaryAssociations: (s.secondaryAssociationIds || []).length,
    };
  });
}

export { COMMERCIAL_DISPOSITION, mapCommercialDisposition };
