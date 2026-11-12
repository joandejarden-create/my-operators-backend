import { SPECIALIST_HUNTER } from "../specialist-hunter-contract-v1.js";
import { createCacheBackedHunter } from "./create-cache-hunter-v1.js";

export const corporateMeetingTriggersHunter = createCacheBackedHunter(
  SPECIALIST_HUNTER.CORPORATE_MEETING_TRIGGERS
);

export const runCorporateMeetingTriggersHunter =
  corporateMeetingTriggersHunter.run;
export const discoverCorporateMeetingTriggers =
  corporateMeetingTriggersHunter.discoverFn;
