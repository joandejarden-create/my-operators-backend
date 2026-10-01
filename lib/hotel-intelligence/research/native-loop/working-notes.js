/**
 * Working notes — research trail (facts/observations only, not chain-of-thought).
 * Iteration 2: topic depth (hypothesis / support / contradiction / next path).
 */

import crypto from "node:crypto";

export const WORKING_NOTES_VERSION = "native-working-notes-v2";

export function addWorkingNote(state, note) {
  const row = {
    note_id: note.note_id || `note_${crypto.randomBytes(3).toString("hex")}`,
    topic: note.topic || "general",
    observation: note.observation,
    supporting_evidence_ids: note.supporting_evidence_ids || [],
    contradicting_evidence_ids: note.contradicting_evidence_ids || [],
    status: note.status || "OBSERVED",
    open_question: note.open_question || null,
    current_hypothesis: note.current_hypothesis || null,
    strongest_support: note.strongest_support || null,
    strongest_contradiction: note.strongest_contradiction || null,
    next_research_path: note.next_research_path || null,
    at: new Date().toISOString(),
    iteration: state.iteration,
  };
  state.working_notes.push(row);
  return row;
}

/**
 * Maintain one evolving topic note per major research area.
 */
export function upsertTopicNote(state, note) {
  const topic = note.topic || "general";
  let row = state.working_notes.find((n) => n.topic === topic && n.kind === "topic_depth");
  if (!row) {
    row = {
      note_id: `topic_${topic}`,
      kind: "topic_depth",
      topic,
      observation: note.observation || note.current_hypothesis || "",
      supporting_evidence_ids: [],
      contradicting_evidence_ids: [],
      status: note.status || "OBSERVED",
      open_question: null,
      current_hypothesis: null,
      strongest_support: null,
      strongest_contradiction: null,
      next_research_path: null,
      at: new Date().toISOString(),
      iteration: state.iteration,
    };
    state.working_notes.push(row);
  }
  if (note.current_hypothesis) row.current_hypothesis = note.current_hypothesis;
  if (note.strongest_support) row.strongest_support = note.strongest_support;
  if (note.strongest_contradiction) row.strongest_contradiction = note.strongest_contradiction;
  if (note.unresolved_question) row.open_question = note.unresolved_question;
  if (note.open_question) row.open_question = note.open_question;
  if (note.next_research_path) row.next_research_path = note.next_research_path;
  if (note.status) row.status = note.status;
  if (note.observation) row.observation = note.observation;
  if (note.supporting_evidence_ids?.length) {
    row.supporting_evidence_ids = [
      ...new Set([...(row.supporting_evidence_ids || []), ...note.supporting_evidence_ids]),
    ];
  }
  if (note.contradicting_evidence_ids?.length) {
    row.contradicting_evidence_ids = [
      ...new Set([...(row.contradicting_evidence_ids || []), ...note.contradicting_evidence_ids]),
    ];
  }
  row.updated_at = new Date().toISOString();
  row.iteration = state.iteration;
  return row;
}

export const WORKING_TOPICS = Object.freeze([
  "identity",
  "ownership",
  "operator",
  "management",
  "transaction_history",
  "portfolio",
  "contacts",
  "contradictions",
  "unresolved",
]);
