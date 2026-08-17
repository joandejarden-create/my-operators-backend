/**
 * Universal record source router — choose official source strategy per record.
 */

import {
  familyFromIdentity,
  extractChoicePropertyId,
  extractMarshaCode,
  extractHiltonCtyhocn,
  extractAccorPropertyId,
} from "./census-autopilot-family-directory-adapters.js";

export const UNIVERSAL_SOURCE_ROUTER_VERSION = "universal-record-source-router-v1";

export const SOURCE_STRATEGY = Object.freeze({
  CHOICE_PROPERTY_ID: "choice_property_id",
  MARRIOTT_MARSHA_OFFICIAL: "marriott_marsha_official",
  HILTON_DIRECTORY_PROPERTY: "hilton_directory_property",
  IHG_PROPERTY_PAGE: "ihg_property_page",
  ACCOR_CATALOG: "accor_catalog",
  WYNDHAM_PROPERTY_PAGE: "wyndham_property_page",
  PREFERRED_COLLECTION_PAGE: "preferred_collection_page",
  OFFICIAL_PROPERTY_URL: "official_property_url",
  GEOGRAPHY_MAPS_ONLY: "geography_maps_only",
  UNKNOWN: "unknown",
});

/**
 * Route one record to a primary + fallback strategies.
 * @param {object} fields
 * @param {{ propertyCode?: string, identityKey?: string }} [opts]
 */
export function routeHotelRecordSources(fields = {}, opts = {}) {
  const identityKey = opts.identityKey || fields["Property Identity Key"] || "";
  const family = familyFromIdentity(fields, identityKey);
  const officialUrl = String(
    fields["Official Property URL"] || fields["Source URL"] || ""
  ).trim();
  const choiceId =
    extractChoicePropertyId(
      {
        ...fields,
        "Brand Property Code":
          opts.propertyCode || fields["Brand Property Code"] || fields["Property Code"],
      },
      identityKey
    ) ||
    (opts.propertyCode && /^[A-Z]{2}\d{2,4}$/i.test(opts.propertyCode)
      ? String(opts.propertyCode).toUpperCase()
      : null);
  const marsha = extractMarshaCode(fields, identityKey);
  const hilton = extractHiltonCtyhocn(fields, identityKey);
  const accorId = extractAccorPropertyId(fields, identityKey);

  /** @type {string[]} */
  const strategies = [];
  /** @type {string[]} */
  const signals = [];

  if (family === "Choice" || choiceId) {
    strategies.push(SOURCE_STRATEGY.CHOICE_PROPERTY_ID);
    if (choiceId) signals.push(`choice_id:${choiceId}`);
  }
  if (family === "Marriott" || marsha || /marriott\.com/i.test(officialUrl)) {
    strategies.push(SOURCE_STRATEGY.MARRIOTT_MARSHA_OFFICIAL);
    if (marsha) signals.push(`marsha:${marsha}`);
  }
  if (family === "Hilton" || hilton || /hilton\.com/i.test(officialUrl)) {
    strategies.push(SOURCE_STRATEGY.HILTON_DIRECTORY_PROPERTY);
    if (hilton) signals.push(`hilton:${hilton}`);
  }
  if (family === "IHG" || /ihg\.com|holidayinn|kimpton/i.test(officialUrl)) {
    strategies.push(SOURCE_STRATEGY.IHG_PROPERTY_PAGE);
  }
  if (family === "Accor" || accorId || /all\.accor\.com/i.test(officialUrl)) {
    strategies.push(SOURCE_STRATEGY.ACCOR_CATALOG);
    if (accorId) signals.push(`accor:${accorId}`);
  }
  if (family === "Wyndham" || /wyndhamhotels\.com/i.test(officialUrl)) {
    strategies.push(SOURCE_STRATEGY.WYNDHAM_PROPERTY_PAGE);
  }
  if (family === "Preferred" || /preferredhotels\.com/i.test(officialUrl)) {
    strategies.push(SOURCE_STRATEGY.PREFERRED_COLLECTION_PAGE);
  }
  if (officialUrl) {
    strategies.push(SOURCE_STRATEGY.OFFICIAL_PROPERTY_URL);
    signals.push("has_official_url");
  }
  strategies.push(SOURCE_STRATEGY.GEOGRAPHY_MAPS_ONLY);

  const unique = [...new Set(strategies)];
  return {
    version: UNIVERSAL_SOURCE_ROUTER_VERSION,
    family: family || "Other",
    primary: unique[0] || SOURCE_STRATEGY.UNKNOWN,
    strategies: unique,
    signals,
    codes: {
      choice_property_id: choiceId || null,
      marsha: marsha || null,
      hilton_ctyhocn: hilton || null,
      accor_property_id: accorId || null,
      property_code_override: opts.propertyCode || null,
    },
    official_url: officialUrl || null,
  };
}
