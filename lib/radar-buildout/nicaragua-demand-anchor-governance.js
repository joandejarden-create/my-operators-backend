/**
 * Governance defaults for Nicaragua countrywide demand anchor candidates.
 */

import { createCentralAmericaGovernance } from "./central-america-country-shared.js";

export const NICARAGUA_SUBMARKETS = [
  "Managua",
  "Granada",
  "San Juan del Sur",
  "León",
  "Ometepe",
  "Other",
];

export const applyNicaraguaGovernanceDefaults = createCentralAmericaGovernance("Nicaragua");
