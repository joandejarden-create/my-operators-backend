/**
 * Map Brand Standards profile → Airtable fields with Meta select validation.
 * Does not include Additional Amenities (populated separately).
 */
import { MAP_BRAND_STANDARDS } from "./brand-standards-profiles.mjs";

export const BRAND_STANDARDS_SELECT_COLS = Object.freeze([
  "F&B Outlets Required",
  "Typical F&B Program Type",
  "F&B Outlet Size Unit",
  "Meeting Space Required",
  "Condo Residences Allowed",
  "Hotel Rental Program",
  "Parking Required",
  "Parking Program",
  "Sustainability Features",
  "Compliance & Safety",
]);

export const BRAND_STANDARDS_MULTI_SELECT_COLS = Object.freeze([
  "Typical F&B Program Type",
  "Parking Program",
  "Sustainability Features",
  "Compliance & Safety",
]);

/** @type {ReadonlyArray<string>} */
export const WRITABLE_BRAND_STANDARDS_COLS = Object.freeze(Object.values(MAP_BRAND_STANDARDS));

/**
 * @param {string[]|undefined} choices
 * @param {string} preferred
 * @param {Array} proposals
 * @param {string} brandName
 * @param {string} column
 */
export function pickStdSelect(choices, preferred, proposals, brandName, column) {
  if (preferred == null || preferred === "") return null;
  if (!Array.isArray(choices) || !choices.length) return preferred;
  const exact = choices.find(
    (c) => String(c).trim().toLowerCase() === String(preferred).trim().toLowerCase()
  );
  if (exact) return exact;
  if (Array.isArray(proposals)) {
    proposals.push({
      brandName,
      column,
      preferred,
      note: "Preferred select not in Meta; field omitted.",
    });
  }
  return null;
}

/**
 * @param {string[]|undefined} choices
 * @param {string[]} preferred
 * @param {Array} proposals
 * @param {string} brandName
 * @param {string} column
 */
export function filterMultiSelect(choices, preferred, proposals, brandName, column) {
  const list = Array.isArray(preferred) ? preferred : preferred ? [preferred] : [];
  if (!list.length) return [];
  if (!Array.isArray(choices) || !choices.length) return list.map(String);
  const allow = new Set(choices.map((c) => String(c).trim()));
  const out = [];
  const seen = new Set();
  for (const raw of list) {
    const v = String(raw || "").trim();
    if (!v || seen.has(v)) continue;
    if (!allow.has(v)) {
      const ci = [...allow].find((a) => a.toLowerCase() === v.toLowerCase());
      if (ci) {
        seen.add(ci);
        out.push(ci);
        continue;
      }
      if (Array.isArray(proposals)) {
        proposals.push({
          brandName,
          column,
          preferred: v,
          note: "Multi-select option not in Meta; dropped.",
        });
      }
      continue;
    }
    seen.add(v);
    out.push(v);
  }
  return out;
}

function emptyVal(v) {
  return v === undefined || v === null || v === "" || (Array.isArray(v) && !v.length);
}

function sameValue(a, b) {
  if (Array.isArray(a) || Array.isArray(b)) {
    const aa = [...(Array.isArray(a) ? a : a != null && a !== "" ? [a] : [])].map(String).sort();
    const bb = [...(Array.isArray(b) ? b : b != null && b !== "" ? [b] : [])].map(String).sort();
    return aa.length === bb.length && aa.every((x, i) => x === bb[i]);
  }
  if (typeof a === "number" || typeof b === "number") {
    const na = a === "" || a == null ? null : Number(a);
    const nb = b === "" || b == null ? null : Number(b);
    if (Number.isNaN(na) && Number.isNaN(nb)) return true;
    return na === nb;
  }
  return String(a ?? "").trim() === String(b ?? "").trim();
}

/**
 * @param {object} profile
 * @param {Record<string, string[]>} metaChoices
 * @param {Array} [proposals]
 * @param {string} [brandName]
 * @returns {{ fields: Record<string, unknown>, resolved: object }}
 */
export function buildBrandStandardsFieldsFromProfile(
  profile,
  metaChoices,
  proposals = [],
  brandName = ""
) {
  const sel = (col, want) => pickStdSelect(metaChoices[col], want, proposals, brandName, col);
  const multi = (col, want) =>
    filterMultiSelect(metaChoices[col], want || [], proposals, brandName, col);

  const fields = {};
  const M = MAP_BRAND_STANDARDS;

  const fbReq = sel(M.fbOutletsRequired, profile.fbOutletsRequired);
  if (fbReq != null) fields[M.fbOutletsRequired] = fbReq;

  if (profile.typicalFbOutlets != null && profile.typicalFbOutlets !== "") {
    fields[M.typicalFbOutlets] = Number(profile.typicalFbOutlets);
  }

  const fbProg = multi(M.fbProgramType, profile.fbProgramType);
  if (fbProg.length) fields[M.fbProgramType] = fbProg;

  if (profile.outletConcepts != null && String(profile.outletConcepts).trim() !== "") {
    fields[M.outletConcepts] = String(profile.outletConcepts).trim();
  }

  if (profile.fbOutletSize != null && profile.fbOutletSize !== "") {
    fields[M.fbOutletSize] = Number(profile.fbOutletSize);
  }

  const sizeUnit = sel(M.fbOutletSizeUnit, profile.fbOutletSizeUnit);
  if (sizeUnit != null) fields[M.fbOutletSizeUnit] = sizeUnit;

  const meetReq = sel(M.meetingSpaceRequired, profile.meetingSpaceRequired);
  if (meetReq != null) fields[M.meetingSpaceRequired] = meetReq;

  if (profile.typicalMeetingRooms != null && profile.typicalMeetingRooms !== "") {
    fields[M.typicalMeetingRooms] = Number(profile.typicalMeetingRooms);
  }

  if (profile.meetingSpaceSize != null && String(profile.meetingSpaceSize).trim() !== "") {
    fields[M.meetingSpaceSize] = String(profile.meetingSpaceSize).trim();
  }

  const condo = sel(M.condoResidencesAllowed, profile.condoResidencesAllowed);
  if (condo != null) fields[M.condoResidencesAllowed] = condo;

  const rental = sel(M.hotelRentalProgram, profile.hotelRentalProgram);
  if (rental != null) fields[M.hotelRentalProgram] = rental;

  const parkReq = sel(M.parkingRequired, profile.parkingRequired);
  if (parkReq != null) fields[M.parkingRequired] = parkReq;

  if (profile.typicalParkingSpaces != null && profile.typicalParkingSpaces !== "") {
    fields[M.typicalParkingSpaces] = Number(profile.typicalParkingSpaces);
  }

  const parkProg = multi(M.parkingProgram, profile.parkingProgram);
  if (parkProg.length) fields[M.parkingProgram] = parkProg;

  const sust = multi(M.sustainabilityFeatures, profile.sustainabilityFeatures);
  if (sust.length) fields[M.sustainabilityFeatures] = sust;

  if (profile.otherSustainabilityText != null && String(profile.otherSustainabilityText).trim()) {
    fields[M.otherSustainabilityText] = String(profile.otherSustainabilityText).trim();
  }

  if (profile.otherAmenitiesText != null && String(profile.otherAmenitiesText).trim()) {
    fields[M.otherAmenitiesText] = String(profile.otherAmenitiesText).trim();
  }

  const comp = multi(M.complianceSafety, profile.complianceSafety);
  if (comp.length) fields[M.complianceSafety] = comp;

  if (profile.otherComplianceText != null && String(profile.otherComplianceText).trim()) {
    fields[M.otherComplianceText] = String(profile.otherComplianceText).trim();
  }

  if (profile.qaExpectations != null && String(profile.qaExpectations).trim()) {
    fields[M.qaExpectations] = String(profile.qaExpectations).trim();
  }

  if (profile.standardsNotes != null && String(profile.standardsNotes).trim()) {
    fields[M.standardsNotes] = String(profile.standardsNotes).trim();
  }

  return {
    fields,
    resolved: {
      segment: profile.segment,
      sourceTier: profile.sourceTier,
      fieldCount: Object.keys(fields).length,
    },
  };
}

/**
 * @param {Record<string, unknown>} expected
 * @param {Record<string, unknown>} existing
 * @returns {{ column: string, from: unknown, to: unknown }[]}
 */
export function diffBrandStandardsFields(expected, existing) {
  const mismatches = [];
  for (const [col, want] of Object.entries(expected)) {
    const cur = existing[col];
    if (emptyVal(cur)) continue;
    if (!sameValue(cur, want)) {
      mismatches.push({ column: col, from: cur, to: want });
    }
  }
  return mismatches;
}

export { emptyVal as isBrandStandardsFieldEmpty, sameValue as sameBrandStandardsValue };
