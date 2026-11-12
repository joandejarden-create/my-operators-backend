import { SPECIALIST_HUNTER } from "../specialist-hunter-contract-v1.js";
import { createCacheBackedHunter } from "./create-cache-hunter-v1.js";

export const universityDemandHunter = createCacheBackedHunter(
  SPECIALIST_HUNTER.UNIVERSITY_DEMAND
);

export const runUniversityDemandHunter = universityDemandHunter.run;
export const discoverUniversityDemand = universityDemandHunter.discoverFn;
