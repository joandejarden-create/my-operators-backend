/**
 * Advisor scorecards — separate from Operator Fit algorithm scores.
 * Ratings never modify alignment/eligibility/readiness.
 */

import { readFileSync, writeFileSync, existsSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..", "..");
const DEFAULT_PATH = join(ROOT, "data", "operator-fit", "advisor-scorecards.json");

export function getAdvisorScorecardPath() {
  return process.env.OPERATOR_FIT_ADVISOR_SCORECARD_PATH || DEFAULT_PATH;
}

function emptyStore() {
  return {
    version: 1,
    note: "Advisor ratings do not modify Operator Fit algorithm scores",
    updatedAt: null,
    scorecards: [],
  };
}

export function loadAdvisorScorecards(path = getAdvisorScorecardPath()) {
  if (!existsSync(path)) return emptyStore();
  try {
    return { ...emptyStore(), ...JSON.parse(readFileSync(path, "utf8")) };
  } catch (err) {
    console.error("[advisor-scorecards] load failed", err?.message || err);
    return emptyStore();
  }
}

export function saveAdvisorScorecards(store, path = getAdvisorScorecardPath()) {
  mkdirSync(dirname(path), { recursive: true });
  const next = { ...store, updatedAt: new Date().toISOString(), note: emptyStore().note };
  writeFileSync(path, JSON.stringify(next, null, 2));
  return next;
}

/**
 * @param {object} input — must include dealId, overallDecision, sections A–F
 */
export function upsertAdvisorScorecard(input = {}, path = getAdvisorScorecardPath()) {
  if (!input.dealId) throw new Error("dealId required");
  if (!input.overallDecision) throw new Error("overallDecision required");
  const allowed = [
    "Strong enough for owner pilot",
    "Useful internally but needs improvement",
    "Material problems remain",
  ];
  if (!allowed.includes(input.overallDecision)) {
    throw new Error(`overallDecision must be one of: ${allowed.join(" | ")}`);
  }

  const store = loadAdvisorScorecards(path);
  const card = {
    id: input.id || `asc_${input.dealId}_${Date.now()}`,
    dealId: input.dealId,
    dealLabel: input.dealLabel || input.dealId,
    advisorRole: input.advisorRole || "internal_advisor",
    completedAt: new Date().toISOString(),
    rankingCredibility: input.rankingCredibility || {},
    differentiation: input.differentiation || {},
    explanationQuality: input.explanationQuality || {},
    evidenceTrust: input.evidenceTrust || {},
    workflowValue: input.workflowValue || {},
    overallDecision: input.overallDecision,
    rationale: input.rationale || "",
    /** Explicit firewall */
    mutatesAlgorithmScores: false,
  };

  const idx = store.scorecards.findIndex((c) => c.dealId === card.dealId && !input.forceNew);
  if (idx >= 0) store.scorecards[idx] = { ...store.scorecards[idx], ...card, id: store.scorecards[idx].id };
  else store.scorecards.push(card);

  saveAdvisorScorecards(store, path);
  return card;
}

export function aggregateAdvisorScorecards(path = getAdvisorScorecardPath()) {
  const store = loadAdvisorScorecards(path);
  const cards = store.scorecards || [];
  const overall = {
    strong: cards.filter((c) => /strong enough/i.test(c.overallDecision)).length,
    useful: cards.filter((c) => /useful internally/i.test(c.overallDecision)).length,
    material: cards.filter((c) => /material problems/i.test(c.overallDecision)).length,
  };
  return {
    count: cards.length,
    overall,
    dealIds: cards.map((c) => c.dealId),
    mutatesAlgorithmScores: false,
  };
}
