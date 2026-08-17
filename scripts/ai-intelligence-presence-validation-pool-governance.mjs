#!/usr/bin/env node
/**
 * Apply Presence validation pool governance to existing candidates.
 * Does NOT regenerate provider responses. Does NOT score Holdout v2.
 *
 *   node scripts/ai-intelligence-presence-validation-pool-governance.mjs
 */
import "../load-env.js";
import { applyPresenceValidationPoolGovernance } from "../lib/ai-visibility/validation/presence-validation-pool-governance.js";

const result = applyPresenceValidationPoolGovernance();
if (!result.ok) {
  console.error(JSON.stringify(result, null, 2));
  process.exit(2);
}
console.log(JSON.stringify(result.report, null, 2));
console.log(`\nWrote ${result.reportPath}`);
console.log("NEXT_ACTION: CONTINUE_HUMAN_PRESENCE_REVIEW");
