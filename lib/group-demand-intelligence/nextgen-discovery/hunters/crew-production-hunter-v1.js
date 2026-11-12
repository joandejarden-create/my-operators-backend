import { SPECIALIST_HUNTER } from "../specialist-hunter-contract-v1.js";
import { createCacheBackedHunter } from "./create-cache-hunter-v1.js";

/** Independently executable Crew / Production housing hunter. */
export const crewProductionHunter = createCacheBackedHunter(
  SPECIALIST_HUNTER.CREW_PRODUCTION
);

export const runCrewProductionHunter = crewProductionHunter.run;
export const discoverCrewProduction = crewProductionHunter.discoverFn;
