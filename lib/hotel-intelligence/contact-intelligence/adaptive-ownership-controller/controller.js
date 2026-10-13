import {
  ADAPTIVE_CONTROLLER_VERSION,
  ALL_PLAYBOOK_IDS,
  CANDIDATE_VERIFICATION_STATUS,
  OWNER_CANDIDATE_TYPE,
  PLAYBOOK_ID,
  PLAYBOOK_OUTCOME,
  STRUCTURED_REJECTION_REASON,
} from "./constants.js";
import {
  createOwnerCandidate,
  mineCandidatesFromExistingState,
  rejectOwnerCandidate,
  supportOwnerCandidate,
  upsertOwnerCandidate,
} from "./candidate-store.js";
import {
  collectEvidenceSignals,
  inferStructuredRejectionFromState,
  selectNextPlaybook,
} from "./playbook-router.js";
import { buildPlaybookGoals } from "./playbooks.js";
import { lookupOwnerGraphIntelligence } from "./owner-graph-reuse.js";
import {
  evaluateAllActionableCandidates,
  selectPlaybookFromCandidateVerification,
} from "./evaluate-candidates.js";
import {
  PROPERTY_ENTITY_MATCH,
  OWNERSHIP_CONCLUSION_STATE,
} from "../ownership-research-strategy/constants.js";

function emptyTelemetry() {
  return {
    playbook_transitions: 0,
    raw_hypotheses_mined: 0,
    quality_gate_rejected: 0,
    candidates_after_normalization: 0,
    candidate_merges: 0,
    candidates_generated: 0,
    candidates_rejected: 0,
    candidates_supported: 0,
    candidates_insufficient: 0,
    candidates_pending: 0,
    supported_non_owner_entities: 0,
    verification_attempts: 0,
    owner_graph_reuse_events: 0,
  };
}

/**
 * Create empty adaptive controller state (durable on resume).
 */
export function createAdaptiveControllerState() {
  return {
    version: ADAPTIVE_CONTROLLER_VERSION,
    enabled: true,
    attempted_playbooks: [],
    playbook_order: [],
    current_playbook: null,
    triggering_rejection_reason: null,
    playbook_runs: [],
    candidates: [],
    owner_graph_lookups: [],
    transitions: [],
    verification_runs: [],
    research_path_exhausted: false,
    telemetry: emptyTelemetry(),
  };
}

/**
 * Hydrate adaptive state from resume payload.
 */
export function hydrateAdaptiveState(resumeAdaptive) {
  const base = createAdaptiveControllerState();
  if (!resumeAdaptive || typeof resumeAdaptive !== "object") return base;
  return {
    ...base,
    ...resumeAdaptive,
    attempted_playbooks: [...(resumeAdaptive.attempted_playbooks || [])],
    playbook_order: [...(resumeAdaptive.playbook_order || [])],
    playbook_runs: [...(resumeAdaptive.playbook_runs || [])],
    candidates: [...(resumeAdaptive.candidates || [])],
    owner_graph_lookups: [...(resumeAdaptive.owner_graph_lookups || [])],
    transitions: [...(resumeAdaptive.transitions || [])],
    verification_runs: [...(resumeAdaptive.verification_runs || [])],
    telemetry: { ...base.telemetry, ...(resumeAdaptive.telemetry || {}) },
  };
}

/**
 * Reconcile telemetry counters to candidate store + verification runs.
 */
export function reconcileAdaptiveTelemetry(adaptive) {
  const a = adaptive || createAdaptiveControllerState();
  const t = { ...emptyTelemetry(), ...(a.telemetry || {}) };
  const cands = a.candidates || [];
  t.candidates_generated = cands.length;
  t.candidates_supported = cands.filter(
    (c) => c.verification_status === CANDIDATE_VERIFICATION_STATUS.SUPPORTED
  ).length;
  t.candidates_rejected = cands.filter(
    (c) => c.verification_status === CANDIDATE_VERIFICATION_STATUS.REJECTED
  ).length;
  t.candidates_insufficient = cands.filter(
    (c) => c.verification_status === CANDIDATE_VERIFICATION_STATUS.INSUFFICIENT
  ).length;
  t.candidates_pending = cands.filter(
    (c) =>
      !c.verification_status ||
      c.verification_status === CANDIDATE_VERIFICATION_STATUS.PENDING
  ).length;
  t.supported_non_owner_entities = cands.filter(
    (c) => c.supported_non_owner_entity === true
  ).length;
  t.candidates_after_normalization = cands.length;
  a.telemetry = t;
  return a;
}

/**
 * Mine + qualify + evaluate candidates from existing research state.
 * Primary V2.1 integration point — must run in the live Full Research path.
 */
export function refreshCandidatesAndVerify(adaptive, state = {}, hotel = {}) {
  let a = hydrateAdaptiveState(adaptive);
  const mine = mineCandidatesFromExistingState(
    { ...state, adaptive: a },
    hotel,
    a.current_playbook,
    a.telemetry
  );
  a.candidates = mine.list;
  a.telemetry = { ...a.telemetry, ...mine.telemetry };

  const evalResult = evaluateAllActionableCandidates(
    a,
    {
      ...state,
      ownership_conclusion: state.ownership_conclusion,
      legal_entity_leads: state.legal_entity_leads || [],
      claims: state.claims || [],
    },
    hotel
  );
  a = evalResult.adaptive;
  a = reconcileAdaptiveTelemetry(a);
  return { adaptive: a, routing: evalResult.routing };
}

/**
 * Start or continue adaptive routing — returns next goals or exhaustion.
 *
 * NO_USEFUL_NEW_EVIDENCE is playbook-scoped: if an untried applicable playbook
 * remains, continue rather than terminating the case.
 *
 * Skips playbooks whose goals are entirely already in queries_attempted /
 * completed_work (retrieval diversity — do not re-run exhausted Brazil CNPJ set
 * just because operating-entity signal prefers CORPORATE).
 */
export function planAdaptiveContinuation(state = {}, hotel = {}, opts = {}) {
  let adaptive = state.adaptive || createAdaptiveControllerState();
  const maxPlaybooks = Number(opts.max_playbooks || ALL_PLAYBOOK_IDS.length);
  const attemptedQueries = new Set(
    (state.queries_attempted || []).map((q) =>
      String(q || "")
        .toLowerCase()
        .replace(/\s+/g, " ")
        .trim()
    )
  );

  function goalsUnattempted(goals) {
    return (goals || []).filter((g) => {
      const k = String(g.query || "")
        .toLowerCase()
        .replace(/\s+/g, " ")
        .trim();
      return k && !attemptedQueries.has(k);
    });
  }

  // V2.1: mine + qualify + verify from existing evidence BEFORE routing
  const refreshed = refreshCandidatesAndVerify(adaptive, state, hotel);
  adaptive = refreshed.adaptive;

  let rejection =
    opts.rejection_reason ||
    adaptive.triggering_rejection_reason ||
    null;

  // Prefer verification-driven rejection when available
  if (refreshed.routing?.from_verification && refreshed.routing.rejection_reason) {
    rejection = refreshed.routing.rejection_reason;
  }
  if (!rejection) {
    rejection = inferStructuredRejectionFromState({ ...state, adaptive });
  }

  adaptive.triggering_rejection_reason = rejection;
  const signals = collectEvidenceSignals({ ...state, adaptive });

  const trySelect = (preferredIds, routeReason) => {
    for (const playbookId of preferredIds) {
      if ((adaptive.attempted_playbooks || []).includes(playbookId)) continue;
      const rawGoals = buildPlaybookGoals(playbookId, hotel, state);
      const goals = goalsUnattempted(rawGoals);
      if (!goals.length) {
        if (!adaptive.attempted_playbooks.includes(playbookId)) {
          adaptive.attempted_playbooks.push(playbookId);
          adaptive.playbook_order.push(playbookId);
          adaptive.playbook_runs.push({
            playbook_id: playbookId,
            triggering_rejection_reason: rejection,
            started_at: new Date().toISOString(),
            finished_at: new Date().toISOString(),
            playbook_outcome: PLAYBOOK_OUTCOME.NO_USEFUL_NEW_EVIDENCE,
            new_candidates: 0,
            provider_operations: 0,
            provider_spend: 0,
            note: "skipped_all_goals_already_attempted",
          });
        }
        continue;
      }
      return { playbookId, goals, routeReason: `${routeReason}_TO_${playbookId}` };
    }
    return null;
  };

  let picked = null;

  // Verification outcomes drive playbook preference when present
  const fromVerify = selectPlaybookFromCandidateVerification(adaptive);
  if (fromVerify?.playbook_id && !adaptive.attempted_playbooks.includes(fromVerify.playbook_id)) {
    picked = trySelect([fromVerify.playbook_id], fromVerify.reason);
  }

  if (!adaptive.current_playbook && !picked) {
    if (signals.fii_or_fund) {
      picked = trySelect(
        [PLAYBOOK_ID.FINANCING_PUBLIC_RECORD, PLAYBOOK_ID.GENERAL_OWNERSHIP_DISCOVERY],
        "BOOTSTRAP_FII_SIGNAL"
      );
    } else if (signals.historical_owner) {
      picked = trySelect(
        [PLAYBOOK_ID.TRANSACTION_CURRENT_OWNER, PLAYBOOK_ID.GENERAL_OWNERSHIP_DISCOVERY],
        "BOOTSTRAP_HISTORICAL"
      );
    } else if (signals.operating_entity_only) {
      picked = trySelect(
        [
          PLAYBOOK_ID.TRANSACTION_CURRENT_OWNER,
          PLAYBOOK_ID.FINANCING_PUBLIC_RECORD,
          PLAYBOOK_ID.GENERAL_OWNERSHIP_DISCOVERY,
          PLAYBOOK_ID.CORPORATE_ENTITY_RELATIONSHIP,
        ],
        "BOOTSTRAP_OPERATING_ENTITY"
      );
    } else {
      picked = trySelect([PLAYBOOK_ID.GENERAL_OWNERSHIP_DISCOVERY, ...ALL_PLAYBOOK_IDS], "BOOTSTRAP_GENERAL");
    }
  }

  if (!picked) {
    const selection = selectNextPlaybook({
      rejection_reason: rejection,
      attempted_playbooks: adaptive.attempted_playbooks,
      evidence_signals: signals,
    });
    if (selection.exhausted || !selection.playbook_id) {
      adaptive.research_path_exhausted = true;
      adaptive = reconcileAdaptiveTelemetry(adaptive);
      return {
        allowed: false,
        reason: STRUCTURED_REJECTION_REASON.RESEARCH_PATH_EXHAUSTED,
        goals: [],
        adaptive,
        structured_rejection: STRUCTURED_REJECTION_REASON.RESEARCH_PATH_EXHAUSTED,
      };
    }
    const order = [
      selection.playbook_id,
      ...ALL_PLAYBOOK_IDS.filter((id) => id !== selection.playbook_id),
    ];
    picked = trySelect(order, selection.reason || `ROUTE_${rejection}`);
  }

  if (!picked) {
    adaptive.research_path_exhausted = true;
    adaptive = reconcileAdaptiveTelemetry(adaptive);
    return {
      allowed: false,
      reason: STRUCTURED_REJECTION_REASON.RESEARCH_PATH_EXHAUSTED,
      goals: [],
      adaptive,
      structured_rejection: STRUCTURED_REJECTION_REASON.RESEARCH_PATH_EXHAUSTED,
    };
  }

  if (adaptive.attempted_playbooks.length >= maxPlaybooks && !picked) {
    adaptive.research_path_exhausted = true;
    adaptive = reconcileAdaptiveTelemetry(adaptive);
    return {
      allowed: false,
      reason: STRUCTURED_REJECTION_REASON.RESEARCH_PATH_EXHAUSTED,
      goals: [],
      adaptive,
      structured_rejection: STRUCTURED_REJECTION_REASON.RESEARCH_PATH_EXHAUSTED,
    };
  }

  const playbookId = picked.playbookId;
  const goals = picked.goals;

  if (adaptive.current_playbook && adaptive.current_playbook !== playbookId) {
    adaptive.transitions.push({
      from: adaptive.current_playbook,
      to: playbookId,
      trigger: rejection,
      at: new Date().toISOString(),
      route_reason: picked.routeReason,
      from_candidate_verification: Boolean(refreshed.routing?.from_verification),
    });
    adaptive.telemetry.playbook_transitions += 1;
  }

  if (!adaptive.attempted_playbooks.includes(playbookId)) {
    adaptive.attempted_playbooks.push(playbookId);
    adaptive.playbook_order.push(playbookId);
  }
  adaptive.current_playbook = playbookId;
  adaptive.playbook_runs.push({
    playbook_id: playbookId,
    triggering_rejection_reason: rejection,
    started_at: new Date().toISOString(),
    finished_at: null,
    playbook_outcome: null,
    new_candidates: 0,
    provider_operations: 0,
    provider_spend: 0,
  });

  adaptive = reconcileAdaptiveTelemetry(adaptive);

  return {
    allowed: goals.length > 0,
    reason: picked.routeReason,
    goals,
    adaptive,
    structured_rejection: rejection,
    playbook_id: playbookId,
  };
}

/**
 * Mark current playbook finished with playbook-scoped NO_USEFUL_NEW_EVIDENCE semantics.
 */
export function finishCurrentPlaybook(adaptive, outcome, extras = {}) {
  const a = adaptive || createAdaptiveControllerState();
  const run = a.playbook_runs[a.playbook_runs.length - 1];
  if (run && !run.finished_at) {
    run.finished_at = new Date().toISOString();
    run.playbook_outcome = outcome || PLAYBOOK_OUTCOME.NO_USEFUL_NEW_EVIDENCE;
    run.new_candidates = extras.new_candidates ?? run.new_candidates;
    run.provider_operations = extras.provider_operations ?? run.provider_operations;
    run.provider_spend = extras.provider_spend ?? run.provider_spend;
  }
  return a;
}

/**
 * Apply structured verification to candidates from staging conclusions.
 * Never auto-publishes SUPPORTED_PROPERTY_OWNER as a Dealality fact.
 * Prefer evaluateAllActionableCandidates for bulk existing-evidence evaluation.
 */
export function applyVerificationToCandidates(adaptive, verification = {}) {
  let a = { ...(adaptive || createAdaptiveControllerState()) };
  let candidates = [...(a.candidates || [])];

  if (verification.support_candidate_id) {
    candidates = supportOwnerCandidate(
      candidates,
      verification.support_candidate_id,
      verification.evidence || []
    );
    a.telemetry.candidates_supported += 1;

    const supported = candidates.find((c) => c.candidate_id === verification.support_candidate_id);
    if (supported) {
      const lookup = lookupOwnerGraphIntelligence({
        hotel_id: supported.hotel_id,
        entity_name: supported.entity_name,
        owner_entity_id: verification.owner_entity_id,
      });
      a.owner_graph_lookups.push(lookup);
      if (lookup.owner_graph_reuse) a.telemetry.owner_graph_reuse_events += 1;
    }
  }

  if (verification.reject_candidate_id && verification.rejection_reason) {
    candidates = rejectOwnerCandidate(
      candidates,
      verification.reject_candidate_id,
      verification.rejection_reason,
      verification.evidence || []
    );
    a.telemetry.candidates_rejected += 1;
  }

  if (verification.identity_match === PROPERTY_ENTITY_MATCH.WRONG_PROPERTY) {
    const pending = candidates.filter((c) => c.verification_status === CANDIDATE_VERIFICATION_STATUS.PENDING);
    for (const c of pending) {
      candidates = rejectOwnerCandidate(
        candidates,
        c.candidate_id,
        STRUCTURED_REJECTION_REASON.ENTITY_LOCATION_CONFLICT,
        [{ note: "wrong_property_identity_match" }]
      );
      a.telemetry.candidates_rejected += 1;
    }
  }

  if (
    verification.staging_conclusion === OWNERSHIP_CONCLUSION_STATE.SUPPORTED_CURRENT_OPERATING_ENTITY
  ) {
    for (const c of candidates) {
      if (
        c.candidate_type === OWNER_CANDIDATE_TYPE.OPERATING_ENTITY ||
        c.candidate_type === OWNER_CANDIDATE_TYPE.HOTEL_OPERATOR ||
        c.candidate_type === OWNER_CANDIDATE_TYPE.MANAGEMENT_COMPANY
      ) {
        if (c.verification_status === CANDIDATE_VERIFICATION_STATUS.PENDING) {
          candidates = rejectOwnerCandidate(
            candidates,
            c.candidate_id,
            STRUCTURED_REJECTION_REASON.OPERATING_ENTITY_NOT_OWNER,
            [{ note: "operator_not_auto_owner" }]
          );
          candidates = candidates.map((x) =>
            x.candidate_id === c.candidate_id
              ? {
                  ...x,
                  candidate_type: OWNER_CANDIDATE_TYPE.OPERATING_ENTITY,
                  verification_status: CANDIDATE_VERIFICATION_STATUS.INSUFFICIENT,
                  rejection_reason: STRUCTURED_REJECTION_REASON.OPERATING_ENTITY_NOT_OWNER,
                  verification_reason: STRUCTURED_REJECTION_REASON.OPERATING_ENTITY_NOT_OWNER,
                  supported_non_owner_entity: true,
                  verified_relationship: "OPERATING_ENTITY",
                }
              : x
          );
        }
      }
    }
  }

  a.candidates = candidates;
  return reconcileAdaptiveTelemetry(a);
}

/**
 * After playbook yields SERP/staging, upsert candidate hypotheses from text names.
 */
export function ingestPlaybookCandidateNames(adaptive, hotel, names = [], playbookId) {
  let a = { ...(adaptive || createAdaptiveControllerState()) };
  let list = [...(a.candidates || [])];
  const before = list.length;
  let qualityRejected = 0;
  let merges = 0;
  for (const n of names) {
    if (!n || String(n).trim().length < 3) continue;
    a.telemetry.raw_hypotheses_mined = Number(a.telemetry.raw_hypotheses_mined || 0) + 1;
    const cand = createOwnerCandidate({
      hotel_id: hotel.hotel_id,
      entity_name: n,
      candidate_type: OWNER_CANDIDATE_TYPE.POSSIBLE_OWNER,
      why_generated: "playbook_serp_or_extract",
      generating_playbook: playbookId || a.current_playbook,
    });
    if (!cand) {
      qualityRejected += 1;
      continue;
    }
    const res = upsertOwnerCandidate(list, cand);
    list = res.list;
    if (res.merged) merges += 1;
  }
  a.candidates = list;
  a.telemetry.quality_gate_rejected = Number(a.telemetry.quality_gate_rejected || 0) + qualityRejected;
  a.telemetry.candidate_merges = Number(a.telemetry.candidate_merges || 0) + merges;
  a = reconcileAdaptiveTelemetry(a);
  return { adaptive: a, new_count: Math.max(0, list.length - before) };
}

/**
 * Whether case-level NO_USEFUL_NEW_EVIDENCE should be deferred because
 * another playbook remains justified.
 */
export function shouldDeferCaseTerminalForPlaybook(adaptive) {
  const a = adaptive || {};
  if (a.research_path_exhausted) return false;
  const attempted = new Set(a.attempted_playbooks || []);
  return ALL_PLAYBOOK_IDS.some((id) => !attempted.has(id));
}

export function serializeAdaptiveForResume(adaptive) {
  if (!adaptive) return null;
  return {
    ...adaptive,
    attempted_playbooks: [...(adaptive.attempted_playbooks || [])],
    playbook_order: [...(adaptive.playbook_order || [])],
    playbook_runs: [...(adaptive.playbook_runs || [])],
    candidates: [...(adaptive.candidates || [])],
    owner_graph_lookups: [...(adaptive.owner_graph_lookups || [])],
    transitions: [...(adaptive.transitions || [])],
    verification_runs: [...(adaptive.verification_runs || [])],
  };
}
