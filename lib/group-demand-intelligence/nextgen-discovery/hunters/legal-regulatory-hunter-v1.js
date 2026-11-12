import { SPECIALIST_HUNTER } from "../specialist-hunter-contract-v1.js";
import { createCacheBackedHunter } from "./create-cache-hunter-v1.js";

/** Independently executable Legal / Regulatory Events hunter. */
export const legalRegulatoryHunter = createCacheBackedHunter(
  SPECIALIST_HUNTER.LEGAL_REGULATORY
);

export const runLegalRegulatoryHunter = legalRegulatoryHunter.run;
export const discoverLegalRegulatory = legalRegulatoryHunter.discoverFn;
