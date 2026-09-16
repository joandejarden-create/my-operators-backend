/**
 * Legacy Hotel Census write guard — READ-ONLY migration archive by default.
 *
 * Emergency recovery only when BOTH are set:
 *   LEGACY_HOTEL_CENSUS_WRITES_ENABLED=1
 *   LEGACY_CENSUS_WRITE_EMERGENCY_CONFIRM=1
 *
 * Not a legal determination — technical governance only.
 */

import { HOTEL_CENSUS_TABLE } from "./fields.js";

const WRITE_METHODS = new Set([
  "create",
  "update",
  "destroy",
  "replace",
]);

export function legacyHotelCensusWritesAllowed() {
  return (
    process.env.LEGACY_HOTEL_CENSUS_WRITES_ENABLED === "1" &&
    process.env.LEGACY_CENSUS_WRITE_EMERGENCY_CONFIRM === "1"
  );
}

export function assertLegacyCensusWriteAllowed(context = "unspecified") {
  if (legacyHotelCensusWritesAllowed()) {
    console.warn(
      `[legacy-census-write-guard] EMERGENCY write allowed for context=${context}`
    );
    return;
  }
  const err = new Error(
    `Legacy Hotel Census is READ-ONLY migration archive (P8.1). ` +
      `Writes blocked (context=${context}). ` +
      `Emergency only: LEGACY_HOTEL_CENSUS_WRITES_ENABLED=1 and LEGACY_CENSUS_WRITE_EMERGENCY_CONFIRM=1.`
  );
  err.code = "LEGACY_CENSUS_WRITE_BLOCKED";
  throw err;
}

/**
 * Wrap an Airtable base() so Hotel Census mutating calls fail closed.
 * @param {Function} baseFn Airtable base function
 */
export function guardLegacyCensusBase(baseFn) {
  if (typeof baseFn !== "function") return baseFn;

  return new Proxy(baseFn, {
    apply(target, thisArg, argArray) {
      const tableName = argArray?.[0];
      const table = Reflect.apply(target, thisArg, argArray);
      if (tableName !== HOTEL_CENSUS_TABLE) return table;

      return new Proxy(table, {
        get(t, prop, receiver) {
          const value = Reflect.get(t, prop, receiver);
          if (typeof prop === "string" && WRITE_METHODS.has(prop) && typeof value === "function") {
            return (...args) => {
              assertLegacyCensusWriteAllowed(`base(${HOTEL_CENSUS_TABLE}).${prop}`);
              return value.apply(t, args);
            };
          }
          return value;
        },
      });
    },
  });
}
