/**
 * Hotel Explorer Identity Integrity Gate V1
 * Permanent regression prevention: one canonical HPC ID across Radar → HE → APIs → render.
 *
 * Independent of paid research, Surfe/Findymail spend, and contactability completeness.
 */

import { HE50_CANONICAL_HPC_BY_PILOT_KEY } from "./he-5-0-full-e2e/contract.js";

export const HE_IDENTITY_INTEGRITY_GATES = Object.freeze({
  HE_CANONICAL_HOTEL_ID_VALIDATED: "HE_CANONICAL_HOTEL_ID_VALIDATED",
  HE_IDENTITY_PROPAGATION_VALIDATED: "HE_IDENTITY_PROPAGATION_VALIDATED",
  HE_API_HOTEL_ID_PARITY: "HE_API_HOTEL_ID_PARITY",
  HE_API_RESPONSE_HOTEL_PARITY: "HE_API_RESPONSE_HOTEL_PARITY",
  HE_RENDERED_HOTEL_IDENTITY_VALIDATED: "HE_RENDERED_HOTEL_IDENTITY_VALIDATED",
  HE_CROSS_HOTEL_CACHE_ISOLATION: "HE_CROSS_HOTEL_CACHE_ISOLATION",
  HE_NO_BLANK_HOTEL_REQUESTS: "HE_NO_BLANK_HOTEL_REQUESTS",
  HE_ALL_TABS_IDENTITY_VALIDATED: "HE_ALL_TABS_IDENTITY_VALIDATED",
});

/** Eleven active HE tabs (Guest Sentiment disabled). */
export const HE_IDENTITY_ACTIVE_TABS = Object.freeze([
  { id: "hotel", label: "Hotel Overview", hotelSpecificApi: "ownership|workspace" },
  { id: "rooms", label: "Guest Rooms & Meeting Space", hotelSpecificApi: "workspace" },
  { id: "ownership", label: "Ownership Intelligence", hotelSpecificApi: "ownership" },
  { id: "contacts", label: "Contact Intelligence", hotelSpecificApi: "contact-intelligence" },
  { id: "organization", label: "Organization & Portfolio", hotelSpecificApi: "ownership|org" },
  { id: "relationships", label: "Relationships & Structure", hotelSpecificApi: "ownership|org" },
  { id: "submarket", label: "Submarket Snapshot", hotelSpecificApi: "workspace" },
  { id: "area", label: "Area Hotels", hotelSpecificApi: "workspace" },
  { id: "demand", label: "Demand Drivers", hotelSpecificApi: "workspace" },
  { id: "access", label: "Access & Connectivity", hotelSpecificApi: "workspace" },
  { id: "sources", label: "Sources & Evidence", hotelSpecificApi: "workspace" },
]);

export const HE_IDENTITY_SWITCH_PAIR = Object.freeze({
  primary: {
    name: "Bethesda Marriott",
    hpcId: HE50_CANONICAL_HPC_BY_PILOT_KEY.bethesda_marriott,
  },
  secondary: {
    name: "AC Hotel Newtown Square",
    hpcId: HE50_CANONICAL_HPC_BY_PILOT_KEY.ac_hotel_newtown_square,
  },
});

export const HE50_IDENTITY_HOTELS = Object.freeze(
  Object.entries(HE50_CANONICAL_HPC_BY_PILOT_KEY).map(([pilotKey, hpcId]) => ({
    pilotKey,
    hpcId,
  })),
);

const HPC_RE = /^rec[A-Za-z0-9]{10,}$/;

export function isCanonicalHpcId(value) {
  return typeof value === "string" && HPC_RE.test(value.trim());
}

/**
 * Detect malformed hotel-specific URLs that omit the HPC id segment.
 * Examples:
 *   /api/contact-intelligence/hotels/?audience=internal
 *   /api/golden-demo/ownership/hotel/
 */
export function isBlankHotelApiUrl(url) {
  const u = String(url || "");
  if (/\/api\/contact-intelligence\/hotels\/(?:\?|$)/i.test(u)) return true;
  if (/\/api\/golden-demo\/ownership\/hotel\/(?:\?|$)/i.test(u)) return true;
  if (/\/api\/[^?\s]+\/hotels\/(?:\?|$)/i.test(u) && /hotel/i.test(u)) {
    // Generic hotels/? with empty id — keep narrow to known HE surfaces above.
  }
  return false;
}

export function extractHotelIdFromApiUrl(url) {
  const u = String(url || "");
  let m = u.match(/\/api\/contact-intelligence\/hotels\/(rec[A-Za-z0-9]{10,})(?:\/|\?|$)/i);
  if (m) return m[1];
  m = u.match(/\/api\/golden-demo\/ownership\/hotel\/(rec[A-Za-z0-9]{10,})(?:\/|\?|$)/i);
  if (m) return m[1];
  m = u.match(/[?&]hotel(?:Id|ID|_id)=(rec[A-Za-z0-9]{10,})/i);
  if (m) return m[1];
  return "";
}

export function responseHotelId(payload) {
  if (!payload || typeof payload !== "object") return "";
  const candidates = [
    payload.hotel_id,
    payload.hotelId,
    payload.hpcId,
    payload.hpc_id,
    payload.recordId,
    payload.package && payload.package.hotel_id,
    payload.hotel && (payload.hotel.id || payload.hotel.recordId || payload.hotel.airtable_record_id),
  ];
  for (const c of candidates) {
    if (isCanonicalHpcId(c)) return String(c).trim();
  }
  return "";
}

export function assertUrlHotelParity(url, expectedHotelId) {
  if (isBlankHotelApiUrl(url)) {
    return { ok: false, reason: "BLANK_HOTEL_ID", url };
  }
  const got = extractHotelIdFromApiUrl(url);
  if (!got) {
    return { ok: true, reason: "NOT_HOTEL_SCOPED", url };
  }
  if (got !== expectedHotelId) {
    return { ok: false, reason: "HOTEL_ID_MISMATCH", url, got, expected: expectedHotelId };
  }
  return { ok: true, reason: "PARITY", url, got };
}

export function gateStatus(ok, { tested = true, applicable = true } = {}) {
  if (!applicable) return "NOT_APPLICABLE";
  if (!tested) return "NOT_TESTED";
  return ok ? "PASS" : "FAIL";
}

export function summarizeIdentityGates(results = {}) {
  const out = {};
  for (const id of Object.values(HE_IDENTITY_INTEGRITY_GATES)) {
    const row = results[id] || { status: "NOT_TESTED" };
    out[id] = row.status || "NOT_TESTED";
  }
  const mandatory = Object.values(HE_IDENTITY_INTEGRITY_GATES);
  const anyFail = mandatory.some((id) => out[id] === "FAIL");
  const anyNotTested = mandatory.some((id) => out[id] === "NOT_TESTED");
  return {
    gates: out,
    anyFail,
    anyNotTested,
    // Identity failure must keep Full E2E NO; identity alone never upgrades Full E2E to YES.
    FULL_HE_E2E_VALIDATED_BLOCKED_BY_IDENTITY: anyFail === true,
  };
}

export default {
  HE_IDENTITY_INTEGRITY_GATES,
  HE_IDENTITY_ACTIVE_TABS,
  HE_IDENTITY_SWITCH_PAIR,
  HE50_IDENTITY_HOTELS,
  isCanonicalHpcId,
  isBlankHotelApiUrl,
  extractHotelIdFromApiUrl,
  responseHotelId,
  assertUrlHotelParity,
  gateStatus,
  summarizeIdentityGates,
};
