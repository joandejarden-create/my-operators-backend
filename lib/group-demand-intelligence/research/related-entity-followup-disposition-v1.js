/**
 * Related-entity follow-up disposition V1 (run-049).
 *
 * RELATED_ENTITY_FOLLOWUPS_PENDING must never silently disappear, and must never
 * remain "pending" after the session has already exhausted its authorized follow-up
 * budget on heuristic path guesses. Every commercially material follow-up ends in
 * a truthful status.
 */

import { STOP_REASONS } from "./research-constants-v1.js";
import { STOPPING_TRIGGER } from "./commercial-source-selection-v1.js";
import { selectNextAuthorizedLeadFollowUp } from "./autonomous-lead-following-v2.js";
import { buildLeadPursueQueueFromState } from "./commercial-spine-restoration-v41.js";

export const RELATED_ENTITY_FOLLOWUP_DISPOSITION_V1 =
  "gdi-related-entity-followup-disposition-v1";

export const FOLLOWUP_STATUS = Object.freeze({
  RESOLVED_WITH_EVIDENCE: "RESOLVED_WITH_EVIDENCE",
  INVESTIGATED_UNRESOLVED: "INVESTIGATED_UNRESOLVED",
  NOT_WORTH_PURSUING: "NOT_WORTH_PURSUING",
  SCHEDULED_FOR_RESUME: "SCHEDULED_FOR_RESUME",
  BLOCKED_EXTERNAL: "BLOCKED_EXTERNAL",
  PENDING_EXECUTABLE: "PENDING_EXECUTABLE",
});

const HEURISTIC_PATH_RE =
  /\/(?:about|about-us|our-team|team|people|leadership|who-we-are|staff|contact|contact-us|news|programs|events)(?:\/|$|\?)/i;

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function hostOf(url) {
  try {
    return new URL(String(url)).hostname.replace(/^www\./i, "").toLowerCase();
  } catch {
    return "";
  }
}

export function isHeuristicOrgPathUrl(url) {
  return HEURISTIC_PATH_RE.test(String(url || ""));
}

/**
 * Count related_entity_followup attempts already recorded on the opportunity.
 */
export function countRelatedEntityFollowUpAttempts(state = {}) {
  return (state.researchAttempts || []).filter(
    (a) => a?.strategy === "related_entity_followup",
  ).length;
}

/**
 * Hosts where heuristic org paths were already attempted.
 */
export function heuristicHostsAlreadyProbed(state = {}) {
  const hosts = new Set();
  for (const u of [
    ...(state.investigatedLeadUrls || []),
    ...(state.investigatedUrls || []),
    ...((state.researchAttempts || [])
      .filter((a) => a?.strategy === "related_entity_followup")
      .flatMap((a) => a.queries || a.evidenceIds || [])),
  ]) {
    if (isHeuristicOrgPathUrl(u)) {
      const h = hostOf(u);
      if (h) hosts.add(h);
    }
  }
  return hosts;
}

/**
 * Filter alternative / pending discovery candidates so exhausted heuristic hosts
 * are not endlessly re-queued as RELATED_ENTITY_FOLLOWUPS_PENDING.
 */
export function filterExhaustedHeuristicCandidates(candidates = [], state = {}, { minProbesPerHost = 2 } = {}) {
  const probed = heuristicHostsAlreadyProbed(state);
  const probeCountByHost = new Map();
  for (const u of state.investigatedLeadUrls || []) {
    if (!isHeuristicOrgPathUrl(u)) continue;
    const h = hostOf(u);
    if (!h) continue;
    probeCountByHost.set(h, (probeCountByHost.get(h) || 0) + 1);
  }
  return (candidates || []).filter((c) => {
    const url = c?.suggestedUrl || c?.url || (typeof c === "string" ? c : "");
    if (!url || !isHeuristicOrgPathUrl(url)) return true;
    const h = hostOf(url);
    if (!h) return true;
    if ((probeCountByHost.get(h) || 0) >= minProbesPerHost || probed.has(h)) {
      return false;
    }
    return true;
  });
}

/**
 * Build per-lead disposition records for audit / drawer honesty.
 */
function asNoteList(notes) {
  if (Array.isArray(notes)) {
    // Flatten accidental multi-line blobs stored as a single array element.
    return notes.flatMap((n) => String(n || "").split(/\n+/)).map((n) => n.trim()).filter(Boolean);
  }
  if (notes == null || notes === "") return [];
  return String(notes).split(/\n+/).map((n) => n.trim()).filter(Boolean);
}

export function buildFollowUpDispositionRecords(state = {}, { remainingPursue = [], maxFollowUps = 3 } = {}) {
  const followed = countRelatedEntityFollowUpAttempts(state);
  const executedNotes = asNoteList(state.notes).filter((n) =>
    /^ors_executed_related_entity_followup:/i.test(String(n)),
  );
  const records = [];
  const seenExec = new Set();

  for (const note of executedNotes) {
    const name = String(note)
      .replace(/^ors_executed_related_entity_followup:/i, "")
      .trim()
      .slice(0, 120);
    if (!name || seenExec.has(name)) continue;
    seenExec.add(name);
    const material = (state.researchAttempts || []).some(
      (a) =>
        a.strategy === "related_entity_followup" &&
        a.newMaterialEvidence === true &&
        String(a.notes || "").includes(name.slice(0, 40)),
    );
    records.push({
      entityName: name || "related_entity",
      status: material
        ? FOLLOWUP_STATUS.RESOLVED_WITH_EVIDENCE
        : FOLLOWUP_STATUS.INVESTIGATED_UNRESOLVED,
      reason: material
        ? "followup_returned_material_evidence"
        : "followup_executed_without_material_commercial_advance",
    });
  }

  for (const lead of remainingPursue || []) {
    const url = lead?.suggestedUrl || lead?.url || null;
    const heuristicExhausted =
      isHeuristicOrgPathUrl(url) &&
      heuristicHostsAlreadyProbed(state).has(hostOf(url));
    let status = FOLLOWUP_STATUS.PENDING_EXECUTABLE;
    let reason = "authorized_lead_not_yet_retrieved";
    if (followed >= maxFollowUps) {
      status = FOLLOWUP_STATUS.SCHEDULED_FOR_RESUME;
      reason = "session_followup_cap_reached_preserve_for_resume";
    } else if (heuristicExhausted) {
      status = FOLLOWUP_STATUS.NOT_WORTH_PURSUING;
      reason = "heuristic_org_paths_already_probed_on_host";
    } else if (lead?.pursueRecommended === false) {
      status = FOLLOWUP_STATUS.NOT_WORTH_PURSUING;
      reason = "voi_deferred";
    }
    records.push({
      entityName: lead?.entityName || "pending_lead",
      suggestedUrl: url,
      status,
      reason,
      researchQuestion: lead?.researchQuestion || null,
    });
  }

  return records;
}

/**
 * Decide whether ORS may still claim RELATED_ENTITY_FOLLOWUPS_PENDING, or must
 * finalize with a truthful investigated/resumable disposition.
 *
 * @returns {{
 *   stopReason: string|null,
 *   commercialStoppingTrigger: string|null,
 *   nextResearchStep: object|null,
 *   relatedEntityFollowUpDispositions: object[],
 *   pendingSilentlyDropped: false,
 *   note: string,
 * }}
 */
export function resolveRelatedEntityFollowUpStop({
  state = {},
  pendingLead = null,
  maxFollowUps = 3,
  remainingBudgetUsd = Infinity,
  paidBlocked = false,
} = {}) {
  const followed = countRelatedEntityFollowUpAttempts(state);
  const queuePack = buildLeadPursueQueueFromState(state, {
    targetHotel: state.targetHotel || state.hotelName || "",
    remainingBudgetUsd,
  });
  const filteredPursue = filterExhaustedHeuristicCandidates(queuePack.pursue || [], state);
  const pick =
    pendingLead ||
    selectNextAuthorizedLeadFollowUp({
      pursueQueue: filteredPursue,
      investigatedUrls: [
        ...(state.evidence || []).map((e) => e.url).filter(Boolean),
        ...(state.investigatedLeadUrls || []),
      ],
      alreadyFollowedCount: followed,
      maxFollowUps,
      remainingBudgetUsd,
      commerciallyUsefulInternal:
        state.supervisorV2?.qualification?.commerciallyUsefulInternal === true ||
        state.internalAccountDevelopment === true,
    }).lead;

  const dispositions = buildFollowUpDispositionRecords(state, {
    remainingPursue: filteredPursue,
    maxFollowUps,
  });

  // Cap reached with leftover leads → truthful resume checkpoint, not infinite PENDING.
  if (pick && followed >= maxFollowUps) {
    return {
      stopReason: STOP_REASONS.ITERATION_CEILING_HIT,
      commercialStoppingTrigger: STOPPING_TRIGGER.ITERATION_CEILING_HIT,
      nextResearchStep: {
        action: "RELATED_ENTITY_FOLLOWUP_RESUME",
        question: pick.researchQuestion || null,
        evidenceSought: pick.entityName,
        url: pick.suggestedUrl,
        executable: true,
        paidCalls: 0,
        publicationEligibilityChanged: false,
        stoppingCriteria:
          "Session follow-up cap reached; remaining leads preserved for resume — not claimed resolved.",
        followUpStatus: FOLLOWUP_STATUS.SCHEDULED_FOR_RESUME,
      },
      relatedEntityFollowUpDispositions: dispositions.map((d) =>
        d.status === FOLLOWUP_STATUS.PENDING_EXECUTABLE
          ? { ...d, status: FOLLOWUP_STATUS.SCHEDULED_FOR_RESUME }
          : d,
      ),
      pendingSilentlyDropped: false,
      note: "related_entity_followups_cap_reached_scheduled_for_resume",
    };
  }

  // Heuristic-only leftovers after probes → investigated unresolved, not PENDING.
  if (
    !pick &&
    followed > 0 &&
    (queuePack.pursue || []).length > 0 &&
    filteredPursue.length === 0
  ) {
    return {
      stopReason: STOP_REASONS.CONTACT_RESEARCH_PENDING,
      commercialStoppingTrigger: STOPPING_TRIGGER.DEAD_END_LEAD,
      nextResearchStep: {
        action: "ORGANIZER_ROUTE_OR_INDEPENDENT_SOURCE",
        question:
          "Heuristic /about|/team paths exhausted without buyer evidence — prefer official contact route or independent press, not more path guesses.",
        evidenceSought: "role-relevant buyer or functional organizer route",
        url: null,
        executable: false,
        paidCalls: 0,
        publicationEligibilityChanged: false,
        stoppingCriteria: "Do not re-queue exhausted heuristic org paths as PENDING.",
        followUpStatus: FOLLOWUP_STATUS.INVESTIGATED_UNRESOLVED,
      },
      relatedEntityFollowUpDispositions: dispositions.map((d) =>
        d.status === FOLLOWUP_STATUS.PENDING_EXECUTABLE
          ? { ...d, status: FOLLOWUP_STATUS.INVESTIGATED_UNRESOLVED, reason: "heuristic_paths_exhausted" }
          : d,
      ),
      pendingSilentlyDropped: false,
      note: "related_entity_heuristic_paths_exhausted_investigated_unresolved",
    };
  }

  if (pick) {
    return {
      stopReason: STOP_REASONS.RELATED_ENTITY_FOLLOWUPS_PENDING,
      commercialStoppingTrigger: STOPPING_TRIGGER.RELATED_ENTITY_FOLLOWUPS_PENDING,
      nextResearchStep: {
        action: "RELATED_ENTITY_FOLLOWUP",
        question: pick.researchQuestion || null,
        evidenceSought: pick.entityName,
        url: pick.suggestedUrl,
        executable: true,
        paidCalls: 0,
        publicationEligibilityChanged: false,
        stoppingCriteria:
          "Execute bounded related-entity follow-up; do not claim PUBLIC_PATH_EXHAUSTED; do not drop pending silently.",
        followUpStatus: FOLLOWUP_STATUS.PENDING_EXECUTABLE,
      },
      relatedEntityFollowUpDispositions: dispositions,
      pendingSilentlyDropped: false,
      note: "related_entity_followups_pending_executable",
    };
  }

  if (followed > 0) {
    return {
      stopReason: null,
      commercialStoppingTrigger: STOPPING_TRIGGER.DEAD_END_LEAD,
      nextResearchStep: {
        action: "NONE",
        question: null,
        evidenceSought: null,
        url: null,
        executable: false,
        paidCalls: 0,
        publicationEligibilityChanged: false,
        stoppingCriteria: "All authorized related-entity follow-ups investigated.",
        followUpStatus: FOLLOWUP_STATUS.INVESTIGATED_UNRESOLVED,
      },
      relatedEntityFollowUpDispositions: dispositions.length
        ? dispositions
        : [
            {
              entityName: "related_entity_batch",
              status: FOLLOWUP_STATUS.INVESTIGATED_UNRESOLVED,
              reason: "followups_executed_queue_empty",
            },
          ],
      pendingSilentlyDropped: false,
      note: "related_entity_followups_investigated_queue_empty",
    };
  }

  return {
    stopReason: null,
    commercialStoppingTrigger: null,
    nextResearchStep: null,
    relatedEntityFollowUpDispositions: dispositions,
    pendingSilentlyDropped: false,
    note: paidBlocked
      ? "no_related_entity_followups_provider_may_still_restrict"
      : "no_related_entity_followups_authorized",
  };
}

/**
 * Apply disposition onto opportunity state (mutates copy).
 */
export function applyRelatedEntityFollowUpDisposition(state = {}, resolution = {}) {
  const next = { ...state };
  if (resolution.stopReason != null) next.stopReason = resolution.stopReason;
  if (resolution.commercialStoppingTrigger != null) {
    next.commercialStoppingTrigger = resolution.commercialStoppingTrigger;
  }
  if (resolution.nextResearchStep !== undefined) {
    next.nextResearchStep = resolution.nextResearchStep;
  }
  next.relatedEntityFollowUpDispositions = resolution.relatedEntityFollowUpDispositions || [];
  next.relatedEntityFollowUpDispositionVersion = RELATED_ENTITY_FOLLOWUP_DISPOSITION_V1;
  next.notes = [
    ...asNoteList(next.notes),
    resolution.note || "related_entity_followup_disposition_applied",
  ].filter(Boolean);
  // Hard invariant: PENDING requires executable next step; otherwise strip PENDING.
  if (
    next.stopReason === STOP_REASONS.RELATED_ENTITY_FOLLOWUPS_PENDING &&
    !(next.nextResearchStep?.executable === true && text(next.nextResearchStep?.url || next.nextResearchStep?.evidenceSought))
  ) {
    next.stopReason = STOP_REASONS.CONTACT_RESEARCH_PENDING;
    next.commercialStoppingTrigger = STOPPING_TRIGGER.DEAD_END_LEAD;
    next.notes = [
      ...asNoteList(next.notes),
      "related_entity_pending_stripped_missing_executable_step",
    ];
  }
  return next;
}
