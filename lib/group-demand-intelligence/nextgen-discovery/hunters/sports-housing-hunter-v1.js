import { SPECIALIST_HUNTER } from "../specialist-hunter-contract-v1.js";
import { createCacheBackedHunter } from "./create-cache-hunter-v1.js";

export const sportsHousingHunter = createCacheBackedHunter(
  SPECIALIST_HUNTER.SPORTS_HOUSING
);

export const runSportsHousingHunter = sportsHousingHunter.run;
export const discoverSportsHousing = sportsHousingHunter.discoverFn;
