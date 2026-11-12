import { SPECIALIST_HUNTER } from "../specialist-hunter-contract-v1.js";
import { createCacheBackedHunter } from "./create-cache-hunter-v1.js";

/** Independently executable Emergency / Displacement hunter. */
export const emergencyDisplacementHunter = createCacheBackedHunter(
  SPECIALIST_HUNTER.EMERGENCY_DISPLACEMENT
);

export const runEmergencyDisplacementHunter = emergencyDisplacementHunter.run;
export const discoverEmergencyDisplacement = emergencyDisplacementHunter.discoverFn;
