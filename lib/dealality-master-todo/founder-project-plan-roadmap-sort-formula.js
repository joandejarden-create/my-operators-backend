/**
 * Roadmap Sort formula — single-column numeric sort key from Phase Number + Step Number.
 * Exported for schema scripts and docs.
 */
import { PHASE_NUM_FIELD, STEP_NUM_FIELD } from "./founder-project-plan-phase-order.js";

/** Max ~99 steps per phase; divides step into decimal fraction for correct numeric sort. */
export const ROADMAP_SORT_FIELD = "Roadmap Sort";
export const ROADMAP_SORT_STEP_DIVISOR = 100;

/**
 * Airtable formula: phase 1 step 1 → 1.01, phase 1 step 12 → 1.12, phase 5 step 3 → 5.03
 * ROUND avoids float noise (e.g. 1.1400000000000001).
 */
export const ROADMAP_SORT_FORMULA = `IF(
  AND({${PHASE_NUM_FIELD}}, {${STEP_NUM_FIELD}}),
  ROUND({${PHASE_NUM_FIELD}} + ({${STEP_NUM_FIELD}} / ${ROADMAP_SORT_STEP_DIVISOR}), 2),
  BLANK()
)`;

/** One-line variant for Meta API create/patch. */
export const ROADMAP_SORT_FORMULA_ONELINE = `IF(AND({${PHASE_NUM_FIELD}}, {${STEP_NUM_FIELD}}), ROUND({${PHASE_NUM_FIELD}} + ({${STEP_NUM_FIELD}} / ${ROADMAP_SORT_STEP_DIVISOR}), 2), BLANK())`;
