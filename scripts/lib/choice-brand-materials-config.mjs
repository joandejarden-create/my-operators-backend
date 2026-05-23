/**
 * Choice Hotels Canada — Brand Positioning & Development Materials (shared drive).
 */
export const CHOICE_MATERIALS_ROOT =
  process.env.CHOICE_BRAND_MATERIALS_ROOT ||
  "G:\\Shared drives\\Choice Hotels\\Choice Hotels (Canada)\\Data Request Files\\Brand Positioning & Development Materials";

/** Folder name on shared drive → Airtable Brand Name (Alpha Brand Studios rows). */
export const FOLDER_TO_AIRTABLE_NAME = {
  Radisson: "Radisson (Choice)",
  "Radisson Collection": "Radisson Collection",
  "Radisson Inn & Suites": "Radisson Inn & Suites",
  "Park Inn by Radisson": "Park Inn by Radisson (Choice)",
  "Country Inn & Suites": "Country Inn & Suites by Radisson (Choice)",
  Cambria: "Cambria Hotels",
  "Sleep Inn": "Sleep Inn",
  "Ascend Collection": "Ascend Hotel Collection",
  "Everhome Suites": "Everhome Suites",
  "Woodspring Suites": "WoodSpring Suites",
  MainStay: "MainStay Suites",
};

/** Prefer these filename patterns (first match wins). */
export const PDF_PRIORITY = [
  /one\s*pager/i,
  /one\s*sheet/i,
  /fact\s*sheet/i,
  /pitch\s*deck/i,
  /brand\s*book/i,
  /entitlement/i,
  /development\s*presentation/i,
  /prototype\s*overview/i,
  /messaging/i,
  /\.pdf$/i,
];

export const ROOT_PDF_BY_BRAND = {
  "MainStay Suites": ["Choice_ExtendedStay_MainStay_Development.pdf"],
};
