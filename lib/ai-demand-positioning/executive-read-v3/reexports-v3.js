/** Barrel re-exports for compose-executive-read-v3 consumers. */
export { buildExecutiveReadInputV3 } from "./input-contract-v3.js";
export { generateExecutiveInsightCandidatesV3 } from "./insight-candidates-v3.js";
export { selectPrimaryInsightV3 } from "./insight-priority-v3.js";
export { selectNumericAnchorsV3 } from "./numeric-anchors-v3.js";
export { composeExecutiveReadSectionsV3 } from "./section-composer-v3.js";
export { evaluateExecutiveReadV3QualityGates } from "./quality-gates-v3.js";
export {
  resolveExecutiveReadCompositionWritePolicyV3,
  attachInactiveExecutiveReadV3,
} from "./historical-immutability-v3.js";
