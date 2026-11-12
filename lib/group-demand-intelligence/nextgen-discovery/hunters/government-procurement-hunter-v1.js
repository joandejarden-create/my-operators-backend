import { SPECIALIST_HUNTER } from "../specialist-hunter-contract-v1.js";
import { createCacheBackedHunter } from "./create-cache-hunter-v1.js";

export const governmentProcurementHunter = createCacheBackedHunter(
  SPECIALIST_HUNTER.GOVERNMENT_PROCUREMENT
);

export const runGovernmentProcurementHunter = governmentProcurementHunter.run;
export const discoverGovernmentProcurement =
  governmentProcurementHunter.discoverFn;
