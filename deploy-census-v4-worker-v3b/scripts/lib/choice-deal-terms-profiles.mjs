/**
 * Deal Terms batch: brand list + FDD file map + typical PIP $/room for conversions (folder-informed; confirm in FDD).
 * Reuses Choice CHI brand keys from choice-fee-structure-profiles.mjs.
 */
export {
  CHOICE_FEE_TARGET_BRANDS as CHOICE_DEAL_TERMS_BRANDS,
  CHOICE_FEE_FDD_FILE as CHOICE_DEAL_TERMS_FDD_FILE,
} from "./choice-fee-structure-profiles.mjs";

/** Typical mandatory PIP for conversions ($/room) — tiered estimate; replace with FDD Item 7 when extracted. */
export const CHOICE_DEAL_PIP_CONVERSION_USD = {
  "Ascend Hotel Collection": 9000,
  "Cambria Hotels": 15000,
  "Clarion": 6500,
  "Comfort Inn & Suites": 7500,
  "MainStay Suites": 6500,
  "Quality Inn": 6000,
  "Sleep Inn": 6500,
  "Econo Lodge": 3500,
  "Rodeway Inn": 3500,
  "Suburban Studios": 5000,
  "WoodSpring Suites": 5000,
  "Radisson (Choice)": 9000,
  "Radisson RED  (Choice)": 9000,
  "Radisson Collection  (Choice)": 14000,
  "Park Plaza (Choice)": 14000,
  "Park Inn by Radisson (Choice)": 7500,
  "Country Inn & Suites by Radisson (Choice)": 7500,
  "Radisson Blu (Choice)": 14000,
  "Radisson Individual (Choice)": 8000,
  "Clarion Pointe": 6000,
  "Radisson Inn & Suites": 7000,
  "Everhome Suites": 6500,
};
