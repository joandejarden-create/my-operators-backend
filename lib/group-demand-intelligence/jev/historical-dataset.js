/**
 * Expand core historical Jev fixtures to ≥minCount bounded decision cases.
 * V1.1: also loads adjudicated live disagreements when present.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const FIXTURE_DIR = path.join(
  __dirname,
  "../../../fixtures/group-demand-intelligence/jev"
);
const CORE = path.join(FIXTURE_DIR, "historical-decisions-core.json");
const ADJUDICATED = path.join(FIXTURE_DIR, "adjudicated-disagreements-v1.json");
const EXPANDED_SEED = path.join(FIXTURE_DIR, "historical-decisions-expanded-v1.1.json");

const ARCHETYPES = ["urban_full_service", "resort", "airport_adjacent", "suburban_select"];
const PRIORITIES = ["HIGH", "MEDIUM", "LOW"];

function readJsonSafe(p) {
  try {
    if (!fs.existsSync(p)) return [];
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return [];
  }
}

export function loadCoreHistoricalJevDecisions() {
  return readJsonSafe(CORE);
}

export function loadAdjudicatedDisagreements() {
  return readJsonSafe(ADJUDICATED);
}

export function loadExpandedSeedDecisions() {
  return readJsonSafe(EXPANDED_SEED);
}

/**
 * @param {{ minCount?: number }} opts
 */
export function loadHistoricalJevDecisions({ minCount = 200 } = {}) {
  const core = loadCoreHistoricalJevDecisions().map((r) => ({
    ...r,
    source: r.source || "real_core",
  }));
  const expanded = loadExpandedSeedDecisions().map((r) => ({
    ...r,
    source: r.source || "expanded_seed",
  }));
  const adjudicated = loadAdjudicatedDisagreements().map((row) => ({
    id: row.id || `adj_${row.decisionId || Math.random().toString(16).slice(2, 8)}`,
    decisionType: row.decisionType,
    expected: row.adjudicatedExpected || row.existingGdiDecision,
    existingDecision: row.existingGdiDecision,
    policyContext: row.policyContext || {},
    context: row.context || {},
    riskClass: row.riskClass || "LIVE_ADJUDICATED",
    adjudication: row.adjudication,
    source: "live_adjudicated",
  }));

  const out = [...core, ...expanded, ...adjudicated];
  let i = 0;
  while (out.length < minCount && core.length) {
    const base = core[i % core.length];
    const n = Math.floor(i / core.length) + 1;
    out.push({
      ...base,
      id: `${base.id}_v${n}`,
      context: {
        ...base.context,
        archetype: ARCHETYPES[i % ARCHETYPES.length],
        priority: base.context?.priority || PRIORITIES[i % PRIORITIES.length],
        queriesAlreadyRun:
          base.context?.queriesAlreadyRun != null
            ? base.context.queriesAlreadyRun
            : n % 4,
      },
      variantOf: base.id,
      source: "synthetic_variant",
    });
    i += 1;
  }
  return out;
}

export function countHistoricalSources(rows = []) {
  const counts = {
    real_core: 0,
    expanded_seed: 0,
    live_adjudicated: 0,
    synthetic: 0,
  };
  for (const r of rows) {
    if (r.source === "live_adjudicated") counts.live_adjudicated += 1;
    else if (r.source === "synthetic_variant" || r.variantOf) counts.synthetic += 1;
    else if (r.source === "expanded_seed" || String(r.id || "").startsWith("exp_")) {
      counts.expanded_seed += 1;
    } else counts.real_core += 1;
  }
  return counts;
}
