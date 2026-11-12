/**
 * Shared hunter execution surface (re-exports harness for PowerShell / Node entry).
 * Specialist modules must not import one another — only this shared layer + contract.
 */

export {
  parseHunterCliArgs,
  assertHunterRunSafe,
  screenHunterCandidate,
  materializeHunterDiscoveries,
  runSpecialistHunter,
} from "../hunter-base-v1.js";

export { resolveHunterConfig, listEnabledHunterIds } from "./hunter-config-registry-v1.js";
export { buildQualificationHandoffPacket } from "./qualification-handoff-v1.js";
export {
  DISCOVERY_EVIDENCE_CLASS,
  resolveDiscoveryEvidenceClass,
  stampDiscoveryEvidenceClass,
} from "./discovery-evidence-class-v1.js";
