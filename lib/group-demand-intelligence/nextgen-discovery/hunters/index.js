import { SPECIALIST_HUNTER, FIRST_WAVE_HUNTERS } from "../specialist-hunter-contract-v1.js";
import { governmentProcurementHunter } from "./government-procurement-hunter-v1.js";
import { associationRotationHunter } from "./association-rotation-hunter-v1.js";
import { corporateMeetingTriggersHunter } from "./corporate-meeting-triggers-hunter-v1.js";
import { clinicalTrialsMedicalHunter } from "./clinical-trials-medical-hunter-v1.js";
import { projectWorkforceHunter } from "./project-workforce-hunter-v1.js";
import { sportsHousingHunter } from "./sports-housing-hunter-v1.js";
import { universityDemandHunter } from "./university-demand-hunter-v1.js";
import { legalRegulatoryHunter } from "./legal-regulatory-hunter-v1.js";
import { emergencyDisplacementHunter } from "./emergency-displacement-hunter-v1.js";
import { crewProductionHunter } from "./crew-production-hunter-v1.js";

/**
 * All ten specialist runners — independently executable.
 * Specialists must not import one another; shared harness owns auth/budget/ORS.
 */
export const ALL_HUNTER_RUNNERS = Object.freeze({
  [SPECIALIST_HUNTER.GOVERNMENT_PROCUREMENT]: governmentProcurementHunter,
  [SPECIALIST_HUNTER.ASSOCIATION_ROTATION]: associationRotationHunter,
  [SPECIALIST_HUNTER.CORPORATE_MEETING_TRIGGERS]: corporateMeetingTriggersHunter,
  [SPECIALIST_HUNTER.CLINICAL_TRIALS_MEDICAL]: clinicalTrialsMedicalHunter,
  [SPECIALIST_HUNTER.PROJECT_WORKFORCE]: projectWorkforceHunter,
  [SPECIALIST_HUNTER.SPORTS_HOUSING]: sportsHousingHunter,
  [SPECIALIST_HUNTER.UNIVERSITY_DEMAND]: universityDemandHunter,
  [SPECIALIST_HUNTER.LEGAL_REGULATORY]: legalRegulatoryHunter,
  [SPECIALIST_HUNTER.EMERGENCY_DISPLACEMENT]: emergencyDisplacementHunter,
  [SPECIALIST_HUNTER.CREW_PRODUCTION]: crewProductionHunter,
});

/** @deprecated Prefer ALL_HUNTER_RUNNERS; retained for first-wave pilot callers. */
export const FIRST_WAVE_HUNTER_RUNNERS = Object.freeze(
  Object.fromEntries(
    FIRST_WAVE_HUNTERS.map((id) => [id, ALL_HUNTER_RUNNERS[id]]).filter(([, v]) => v)
  )
);

export function getHunterRunner(hunterId) {
  return ALL_HUNTER_RUNNERS[hunterId] || null;
}

export function listImplementedHunterIds() {
  return Object.keys(ALL_HUNTER_RUNNERS);
}

export function listFirstWaveHunterIds() {
  return FIRST_WAVE_HUNTERS.slice();
}

export { FIRST_WAVE_HUNTERS };
