import { SPECIALIST_HUNTER } from "../specialist-hunter-contract-v1.js";
import { createCacheBackedHunter } from "./create-cache-hunter-v1.js";

/** Independently executable Clinical Trials / Medical Meetings hunter. */
export const clinicalTrialsMedicalHunter = createCacheBackedHunter(
  SPECIALIST_HUNTER.CLINICAL_TRIALS_MEDICAL
);

export const runClinicalTrialsMedicalHunter = clinicalTrialsMedicalHunter.run;
export const discoverClinicalTrialsMedical = clinicalTrialsMedicalHunter.discoverFn;
