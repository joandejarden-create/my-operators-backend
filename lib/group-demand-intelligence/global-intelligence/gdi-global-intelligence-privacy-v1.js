/**
 * Privacy boundaries for global intelligence retention.
 * Hotel-private commercial feedback must never enter the global store.
 */

export const GDI_GLOBAL_INTELLIGENCE_PRIVACY_V1 =
  "gdi-global-intelligence-privacy-v1";

/** Fields that are hotel-confidential and must be stripped from global records. */
export const HOTEL_PRIVATE_FIELD_DENYLIST = Object.freeze([
  "hotelSalesFeedback",
  "salesFeedback",
  "crmNotes",
  "outreachHistory",
  "negotiationNotes",
  "negotiatedRate",
  "rates",
  "rateNotes",
  "businessWon",
  "businessLost",
  "accountStatus",
  "existingRelationshipNotes",
  "privateHotelFeedback",
  "hotelSalesActivity",
  "memberstackUserId",
  "shareToken",
  "customerShareToken",
  "internalOwnerEmail",
  "salesOwnerEmail",
]);

function isPlainObject(v) {
  return v != null && typeof v === "object" && !Array.isArray(v);
}

/**
 * Deep-strip denylisted keys from an object destined for global persistence.
 */
export function stripHotelPrivateFields(input, { denylist = HOTEL_PRIVATE_FIELD_DENYLIST } = {}) {
  const deny = new Set(denylist);
  function walk(value) {
    if (Array.isArray(value)) return value.map(walk);
    if (!isPlainObject(value)) return value;
    const out = {};
    for (const [k, v] of Object.entries(value)) {
      if (deny.has(k)) continue;
      if (/salesFeedback|crmNote|negotiat|outreachHistory|shareToken/i.test(k)) {
        continue;
      }
      out[k] = walk(v);
    }
    return out;
  }
  return walk(input);
}

/**
 * Assert a global record does not contain hotel-private payload.
 * Returns { ok, violations[] }.
 */
export function assertNoHotelPrivateLeak(record = {}) {
  const blob = JSON.stringify(record);
  const violations = [];
  for (const key of HOTEL_PRIVATE_FIELD_DENYLIST) {
    if (Object.prototype.hasOwnProperty.call(record, key) && record[key] != null) {
      violations.push({ code: "PRIVATE_FIELD_PRESENT", field: key });
    }
  }
  if (/\"negotiatedRate\"|\"crmNotes\"|\"outreachHistory\"|\"shareToken\"/i.test(blob)) {
    violations.push({ code: "PRIVATE_PATTERN_IN_PAYLOAD" });
  }
  // Hotel A must not embed Hotel B private feedback bags
  if (record.hotelSalesActivity != null || record.privateHotelFeedback != null) {
    violations.push({ code: "HOTEL_SALES_ACTIVITY_ON_GLOBAL_RECORD" });
  }
  return {
    version: GDI_GLOBAL_INTELLIGENCE_PRIVACY_V1,
    ok: violations.length === 0,
    violations,
  };
}

/**
 * Tenant isolation: Hotel B match must not expose Hotel A private fields.
 */
export function assertTenantIsolation({
  hotelAPrivate = {},
  hotelBMatch = {},
  globalRecord = {},
} = {}) {
  const stripped = stripHotelPrivateFields({
    ...hotelAPrivate,
    ...globalRecord,
  });
  const privacy = assertNoHotelPrivateLeak(stripped);
  const bBlob = JSON.stringify(hotelBMatch);
  const leaks = [];
  for (const [k, v] of Object.entries(hotelAPrivate || {})) {
    if (v == null || v === "") continue;
    if (HOTEL_PRIVATE_FIELD_DENYLIST.includes(k) && bBlob.includes(String(v))) {
      leaks.push({ code: "CROSS_HOTEL_PRIVATE_LEAK", field: k });
    }
  }
  return {
    version: GDI_GLOBAL_INTELLIGENCE_PRIVACY_V1,
    ok: privacy.ok && leaks.length === 0,
    privacy,
    leaks,
  };
}

export default {
  GDI_GLOBAL_INTELLIGENCE_PRIVACY_V1,
  HOTEL_PRIVATE_FIELD_DENYLIST,
  stripHotelPrivateFields,
  assertNoHotelPrivateLeak,
  assertTenantIsolation,
};
