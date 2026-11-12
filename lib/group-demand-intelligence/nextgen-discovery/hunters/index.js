import { SPECIALIST_HUNTER, FIRST_WAVE_HUNTERS } from "../specialist-hunter-contract-v1.js";
import { governmentProcurementHunter } from "./government-procurement-hunter-v1.js";
import { projectWorkforceHunter } from "./project-workforce-hunter-v1.js";
import { sportsHousingHunter } from "./sports-housing-hunter-v1.js";
import { corporateMeetingTriggersHunter } from "./corporate-meeting-triggers-hunter-v1.js";
import { universityDemandHunter } from "./university-demand-hunter-v1.js";

export const FIRST_WAVE_HUNTER_RUNNERS = Object.freeze({
  [SPECIALIST_HUNTER.GOVERNMENT_PROCUREMENT]: governmentProcurementHunter,
  [SPECIALIST_HUNTER.PROJECT_WORKFORCE]: projectWorkforceHunter,
  [SPECIALIST_HUNTER.SPORTS_HOUSING]: sportsHousingHunter,
  [SPECIALIST_HUNTER.CORPORATE_MEETING_TRIGGERS]: corporateMeetingTriggersHunter,
  [SPECIALIST_HUNTER.UNIVERSITY_DEMAND]: universityDemandHunter,
});

export function getHunterRunner(hunterId) {
  return FIRST_WAVE_HUNTER_RUNNERS[hunterId] || null;
}

export function listImplementedHunterIds() {
  return Object.keys(FIRST_WAVE_HUNTER_RUNNERS);
}

export { FIRST_WAVE_HUNTERS };
