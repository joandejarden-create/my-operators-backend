import { SPECIALIST_HUNTER } from "../specialist-hunter-contract-v1.js";
import { createCacheBackedHunter } from "./create-cache-hunter-v1.js";

export const projectWorkforceHunter = createCacheBackedHunter(
  SPECIALIST_HUNTER.PROJECT_WORKFORCE
);

export const runProjectWorkforceHunter = projectWorkforceHunter.run;
export const discoverProjectWorkforce = projectWorkforceHunter.discoverFn;
