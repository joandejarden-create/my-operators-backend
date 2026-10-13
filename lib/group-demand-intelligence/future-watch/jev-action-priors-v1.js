/**
 * Soft Jev action priors by market archetype × blocker × source family.
 * Observation ledger only — no auto training / no override of hard exclusions.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { JEV_NEXT_ACTION } from "../evidence-gap/evidence-gap-model-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_LEDGER = path.resolve(
  __dirname,
  "../../../reports/group-demand-intelligence/future-watch-trigger-engine-v1/JEV_ACTION_PRIORS.json"
);

function keyOf({ archetype, blocker, sourceFamily, action }) {
  return [archetype || "OTHER", blocker || "UNKNOWN", sourceFamily || "UNKNOWN", action || "UNKNOWN"].join(
    "|"
  );
}

export function emptyPriorRow(meta = {}) {
  return {
    archetype: meta.archetype || null,
    blocker: meta.blocker || null,
    sourceFamily: meta.sourceFamily || null,
    action: meta.action || null,
    attempts: 0,
    resolutions: 0,
    stateChanges: 0,
    fetches: 0,
    wrongRoutes: 0,
    resolutionRate: 0,
    fetchesPerResolution: null,
  };
}

export function loadActionPriors(filePath = DEFAULT_LEDGER) {
  try {
    if (fs.existsSync(filePath)) {
      return JSON.parse(fs.readFileSync(filePath, "utf8"));
    }
  } catch {
    /* ignore */
  }
  return { version: "gdi_jev_action_priors_v1", updatedAt: null, rows: {} };
}

export function saveActionPriors(ledger, filePath = DEFAULT_LEDGER) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  ledger.updatedAt = new Date().toISOString();
  fs.writeFileSync(filePath, JSON.stringify(ledger, null, 2), "utf8");
  return ledger;
}

export function recordActionObservation(ledger, obs = {}) {
  const k = keyOf(obs);
  const row = ledger.rows[k] || emptyPriorRow(obs);
  row.attempts += 1;
  row.fetches += Number(obs.fetches) || 0;
  if (obs.resolved) row.resolutions += 1;
  if (obs.stateChanged) row.stateChanges += 1;
  if (obs.wrongRoute) row.wrongRoutes += 1;
  row.resolutionRate = row.attempts ? row.resolutions / row.attempts : 0;
  row.fetchesPerResolution =
    row.resolutions > 0 ? Number((row.fetches / row.resolutions).toFixed(2)) : null;
  ledger.rows[k] = row;
  return ledger;
}

/**
 * Seed priors from prior Jev evidence-gap run results (observation only).
 */
export function seedPriorsFromCandidateResults(results = [], { archetypeByHotel = {} } = {}) {
  const ledger = { version: "gdi_jev_action_priors_v1", updatedAt: null, rows: {} };
  for (const r of results) {
    const arch = archetypeByHotel[r.hotelShort] || "OTHER";
    for (const a of r.jevActions || []) {
      recordActionObservation(ledger, {
        archetype: arch,
        blocker: r.primaryBlocker,
        sourceFamily: r.sourceFamily,
        action: a.appliedAction || a.jevAction,
        fetches: a.fetches,
        resolved: a.blockerResolved === true,
        stateChanged: true,
        wrongRoute: a.valueClass === "WRONG_ROUTE",
      });
    }
  }
  return ledger;
}

/**
 * Soft prior suggestion — never invents actions outside enum; never overrides exclusions.
 */
export function suggestPriorAction({
  ledger,
  archetype,
  blocker,
  sourceFamily,
  preferredActions = [],
  excludedActions = [],
  fallback,
} = {}) {
  const candidates = (preferredActions.length ? preferredActions : Object.values(JEV_NEXT_ACTION)).filter(
    (a) => !excludedActions.includes(a)
  );
  let best = fallback || candidates[0] || JEV_NEXT_ACTION.WAIT_FOR_TRIGGER;
  let bestScore = -1;
  for (const action of candidates) {
    const k = keyOf({ archetype, blocker, sourceFamily, action });
    const row = ledger?.rows?.[k];
    if (!row || row.attempts < 1) continue;
    const score = row.resolutionRate * 10 - (row.wrongRoutes || 0) * 2;
    if (score > bestScore) {
      bestScore = score;
      best = action;
    }
  }
  return { action: best, fromPrior: bestScore >= 0, score: bestScore };
}
