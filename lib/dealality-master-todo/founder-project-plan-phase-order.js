/**
 * Roadmap phase index + within-phase step ordering for Founder Project Plan.
 *
 * Sort in Airtable: Roadmap Sort (asc) — or Phase Number (asc) → Step Number (asc).
 */
import { MAP_MASTER_TODO } from "./master-todo-field-map.js";
import { parseSeedId } from "./dealality-airtable-field-fill.js";

const F = MAP_MASTER_TODO;
const PHASE_NUM_FIELD = "Phase Number";
const STEP_NUM_FIELD = "Step Number";

/** Overall roadmap sequence (Phase Number). GTM/pilot phases placed before platform build. */
export const ROADMAP_PHASE_INDEX = {
  "Strategy & Foundations": 1,
  "Product Definition": 2,
  "Strategy & Design": 3,
  "Resources / Collateral": 4,
  "GTM / Outreach": 5,
  "Pilot Conversion": 6,
  "Pilot Delivery": 7,
  "Product / Access": 8,
  "Platform Design": 9,
  "Platform Build": 10,
  "Content & GTM": 11,
  "Testing & Pilot": 12,
  "Launch & Operations": 13,
  "Scale & Optimize": 14,
  Later: 15,
};

export const MAP_FPP_PHASE_ORDER = {
  phaseNumber: PHASE_NUM_FIELD,
  stepNumber: STEP_NUM_FIELD,
  roadmapSort: "Roadmap Sort",
  roadmapPhaseIndex: ROADMAP_PHASE_INDEX,
};

/** Explicit master-to-do step order within key operational phases (overrides mt-XX numeric id). */
export const PHASE_SEED_STEP_ORDER = {
  "GTM / Outreach": ["mt-07", "mt-04", "mt-05", "mt-09", "mt-19", "mt-21", "mt-18"],
  "Pilot Conversion": ["mt-24", "mt-06", "mt-08", "mt-20", "mt-23", "mt-26", "mt-27"],
  "Pilot Delivery": ["mt-10", "mt-11", "mt-22", "mt-28", "mt-29", "mt-30", "mt-12", "mt-13"],
  "Product / Access": ["mt-14", "mt-15", "mt-31"],
  "Resources / Collateral": ["mt-01", "mt-02", "mt-03", "mt-16", "mt-17"],
};

function isPhaseRollup(task) {
  return String(task || "").trim().startsWith("[Phase rollup]");
}

function isFounderPmoRow(fields) {
  const ws = String(fields?.[F.workstream] || "").trim().toLowerCase();
  const task = String(fields?.[F.task] || "").trim().toLowerCase();
  if (ws === "founder / pmo" || ws.includes("founder / pmo")) return true;
  if (/^track .+ phase completion/.test(task)) return true;
  return false;
}

function seedSortKey(fields) {
  const seedId = parseSeedId(fields);
  if (!seedId) return null;
  const n = Number.parseInt(seedId.replace("mt-", ""), 10);
  return Number.isFinite(n) ? n : null;
}

function seedRankInPhase(phaseName, fields) {
  const seedId = parseSeedId(fields);
  if (!seedId) return null;
  const order = PHASE_SEED_STEP_ORDER[phaseName];
  if (!order) return null;
  const idx = order.indexOf(seedId);
  if (idx >= 0) return idx;
  const fallback = seedSortKey(fields);
  return fallback != null ? 1000 + fallback : null;
}

/**
 * Rank for within-phase logical order (status does not affect position).
 * 1. Founder / PMO tracker
 * 2. Phase rollup summary row
 * 3. Master to-do seed order (phase-specific when configured)
 * 4. Start → End → Workstream → Task
 */
function withinPhaseRank(rec, phaseName) {
  const fields = rec.fields || {};
  const task = fields[F.task] || "";

  if (isFounderPmoRow(fields)) {
    return [0, 0, task];
  }
  if (isPhaseRollup(task)) {
    return [0, 1, task];
  }

  const phaseSeedIdx = seedRankInPhase(phaseName, fields);
  if (phaseSeedIdx != null) return [1, phaseSeedIdx, task];

  const seed = seedSortKey(fields);
  if (seed != null) return [1, seed, task];

  const start = fields[F.startDate] || "9999-12-31";
  const end = fields[F.dueDate] || start;
  return [2, start, end, fields[F.workstream] || "", task];
}

function compareRank(a, b) {
  for (let i = 0; i < Math.max(a.length, b.length); i += 1) {
    const av = a[i] ?? "";
    const bv = b[i] ?? "";
    if (av < bv) return -1;
    if (av > bv) return 1;
  }
  return 0;
}

/**
 * @param {Array<{ id: string, fields: object }>} records
 * @returns {Map<string, { phaseNumber: number, stepNumber: number }>}
 */
export function computePhaseAndStepNumbers(records) {
  const byPhase = new Map();

  for (const rec of records) {
    const phase = rec.fields?.[F.phase];
    if (!phase || phase === "PHASE" || phase === "__") continue;
    if (!byPhase.has(phase)) byPhase.set(phase, []);
    byPhase.get(phase).push(rec);
  }

  const result = new Map();

  for (const [phaseName, phaseRecords] of byPhase) {
    const phaseNumber = ROADMAP_PHASE_INDEX[phaseName] ?? 99;
    const ordered = [...phaseRecords].sort((a, b) => {
      const ra = withinPhaseRank(a, phaseName);
      const rb = withinPhaseRank(b, phaseName);
      return compareRank(ra, rb);
    });

    ordered.forEach((rec, index) => {
      result.set(rec.id, {
        phaseNumber,
        stepNumber: index + 1,
      });
    });
  }

  return result;
}

export function phaseOrderNeedsUpdate(fields, proposed) {
  const curPhase = fields?.[PHASE_NUM_FIELD];
  const curStep = fields?.[STEP_NUM_FIELD];
  return curPhase !== proposed.phaseNumber || curStep !== proposed.stepNumber;
}

export { PHASE_NUM_FIELD, STEP_NUM_FIELD };
