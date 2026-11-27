/**
 * Canonical Hotel Explorer physical-product adapter.
 * Reads Guest Rooms & Meeting Space from HPC (durable SoT).
 * Staged packets are optional proposed overlay only — never primary Live source
 * once HPC fields are populated.
 */
import {
  BETHESDA_PHYSICAL_PRODUCT_VALUES,
  HE_PHYSICAL_PRODUCT_BASE_ID,
  HE_PHYSICAL_PRODUCT_HPC_TABLE_ID,
} from "./he-physical-product-schema-v1.js";
import { loadGuestRoomsMeetingSpaceInventory } from "./he-guest-rooms-meeting-space-inventory-v1.js";

export const HE_PHYSICAL_PRODUCT_ADAPTER_V1 = "he-physical-product-adapter-v1";

const FIELD = Object.freeze({
  rooms: "Rooms / Keys",
  meetingSqFt: "Total Meeting Space (sq ft)",
  meetingRooms: "Meeting Rooms Count",
  largestName: "Largest Meeting Room Name",
  largestSqFt: "Largest Meeting Room (sq ft)",
  single: "Guestroom Single Count",
  double: "Guestroom Double Count",
  suites: "Guestroom Suites Count",
  mixConfidence: "Guestroom Mix Confidence",
  meetingSourceUrl: "Meeting Space Source URL",
});

function num(v) {
  if (v == null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function str(v) {
  if (v == null) return null;
  const s = String(v).trim();
  return s || null;
}

/**
 * Build guestRoomsMeetingSpace inventory shape from HPC fields.
 * @param {object} fields Airtable HPC fields
 * @param {{ hpcId?: string, hotelName?: string }} meta
 */
export function inventoryFromHpcFields(fields, meta = {}) {
  const f = fields || {};
  const total = num(f[FIELD.rooms]);
  const meetingSqFt = num(f[FIELD.meetingSqFt]);
  const meetingRooms = num(f[FIELD.meetingRooms]);
  const largestName = str(f[FIELD.largestName]);
  const largestSqFt = num(f[FIELD.largestSqFt]);
  const single = num(f[FIELD.single]);
  const dbl = num(f[FIELD.double]);
  const suites = num(f[FIELD.suites]);
  const mixConfidence = str(f[FIELD.mixConfidence]);
  const sourceUrl = str(f[FIELD.meetingSourceUrl]);

  const hasCanonicalMeeting =
    meetingSqFt != null || meetingRooms != null || largestName != null || largestSqFt != null;
  const hasCanonicalMix = single != null || dbl != null || suites != null;

  if (!hasCanonicalMeeting && !hasCanonicalMix && total == null) return null;

  const mixStatus =
    mixConfidence === "Brand Materials Reference"
      ? "BRAND_MATERIALS_REFERENCE"
      : mixConfidence === "High"
        ? "VERIFIED"
        : mixConfidence === "Medium"
          ? "MEDIUM"
          : hasCanonicalMix
            ? "VERIFIED"
            : "NOT_YET_VERIFIED";

  return {
    hpcId: meta.hpcId || null,
    hotelName: meta.hotelName || null,
    persisted: true,
    writeOnGet: false,
    sourceOfTruth: "HOTEL_PROPERTY_CENSUS",
    adapter: HE_PHYSICAL_PRODUCT_ADAPTER_V1,
    guestRooms: {
      totalGuestrooms: {
        value: total,
        verificationStatus: total != null ? "VERIFIED" : "NOT_YET_VERIFIED",
        source: "Hotel Property Census",
        note: "Canonical rooms count",
      },
      singleBedRooms: {
        value: single,
        verificationStatus: single != null ? mixStatus : "NOT_YET_VERIFIED",
        source: "Hotel Property Census",
        note:
          mixStatus === "BRAND_MATERIALS_REFERENCE"
            ? "Brand materials reference persisted on HPC"
            : "Guestroom configuration",
      },
      doubleBedRooms: {
        value: dbl,
        verificationStatus: dbl != null ? mixStatus : "NOT_YET_VERIFIED",
        source: "Hotel Property Census",
        note:
          mixStatus === "BRAND_MATERIALS_REFERENCE"
            ? "Brand materials reference persisted on HPC"
            : "Guestroom configuration",
      },
      suites: {
        value: suites,
        verificationStatus: suites != null ? mixStatus : "NOT_YET_VERIFIED",
        source: "Hotel Property Census",
        note: "Suite count is a classification within Rooms / Keys — not additive",
      },
      aggregationRule:
        "TOTAL_IS_CANONICAL; ROOM_TYPE_COUNTS_ARE_PARTITION_OF_TOTAL; SUITES_NOT_ADDITIVE",
    },
    meetingSpaceSummary: {
      totalMeetingSpaceSqFt: {
        value: meetingSqFt,
        verificationStatus: meetingSqFt != null ? "VERIFIED" : "NOT_YET_VERIFIED",
        sourceUrl: sourceUrl,
        source: "Hotel Property Census",
        note: "Capacity-chart aggregate; marketing private-space figures must not be stored here",
      },
      meetingRoomCount: {
        value: meetingRooms,
        verificationStatus: meetingRooms != null ? "VERIFIED" : "NOT_YET_VERIFIED",
        sourceUrl: sourceUrl,
        source: "Hotel Property Census",
      },
      largestRoom: largestName
        ? {
            name: largestName,
            areaSqFt: largestSqFt,
            verificationStatus: "VERIFIED",
            sourceUrl: sourceUrl,
          }
        : null,
      privateSpaceMarketingSqFt: {
        value: null,
        verificationStatus: "NOT_APPLICABLE",
        note: "Marketing figures are not persisted on HPC physical-product fields",
      },
      floorPlanUrl: sourceUrl
        ? { value: sourceUrl, verificationStatus: "VERIFIED", label: "Meeting space source" }
        : null,
    },
    meetingRooms: [],
    sources: sourceUrl
      ? [
          {
            title: "Hotel Property Census — meeting space source",
            url: sourceUrl,
            supports: ["meeting_space_aggregate", "meeting_rooms_count", "largest_room"],
            verificationStatus: "VERIFIED",
          },
        ]
      : [],
  };
}

/**
 * Resolve Live inventory: HPC canonical first; staged only as proposed overlay.
 * @param {string} hpcId
 * @param {object|null} hpcFields
 * @param {{ preferProposedOverlay?: boolean, hotelName?: string }} opts
 */
export function resolvePhysicalProductInventory(hpcId, hpcFields, opts = {}) {
  const fromHpc = inventoryFromHpcFields(hpcFields, {
    hpcId,
    hotelName: opts.hotelName,
  });
  const staged = loadGuestRoomsMeetingSpaceInventory(hpcId);

  const hpcHasMeeting =
    fromHpc &&
    fromHpc.meetingSpaceSummary &&
    fromHpc.meetingSpaceSummary.totalMeetingSpaceSqFt &&
    fromHpc.meetingSpaceSummary.totalMeetingSpaceSqFt.value != null;
  const hpcHasMix =
    fromHpc &&
    fromHpc.guestRooms &&
    fromHpc.guestRooms.singleBedRooms &&
    fromHpc.guestRooms.singleBedRooms.value != null;

  if (hpcHasMeeting || hpcHasMix) {
    // Merge staged meeting-room inventory table for UI detail when HPC lacks row grain
    if (staged && Array.isArray(staged.meetingRooms) && staged.meetingRooms.length) {
      return {
        ...fromHpc,
        meetingRooms: staged.meetingRooms,
        privateSpaceMarketingFromStaged:
          staged.meetingSpaceSummary?.privateSpaceMarketingSqFt || null,
        meetingSpaceSummary: {
          ...fromHpc.meetingSpaceSummary,
          privateSpaceMarketingSqFt:
            staged.meetingSpaceSummary?.privateSpaceMarketingSqFt ||
            fromHpc.meetingSpaceSummary.privateSpaceMarketingSqFt,
        },
        stagedDetailAttached: true,
        stagedDetailRole: "MEETING_ROOM_ROW_INVENTORY_AND_MARKETING_FIGURE_ONLY",
      };
    }
    return fromHpc;
  }

  if (opts.preferProposedOverlay && staged) {
    return {
      ...staged,
      persisted: false,
      sourceOfTruth: "STAGED_PROPOSED_OVERLAY",
      adapter: HE_PHYSICAL_PRODUCT_ADAPTER_V1,
      preview_state: "PROPOSED_STAGED",
    };
  }

  return fromHpc || staged || null;
}

/**
 * Fetch HPC fields for physical-product hydration (server-side).
 */
export async function fetchHpcPhysicalProductFields(hpcId, opts = {}) {
  const token = opts.token || process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  if (!token || !hpcId) return null;
  const baseId = opts.baseId || HE_PHYSICAL_PRODUCT_BASE_ID;
  const tableId = opts.tableId || HE_PHYSICAL_PRODUCT_HPC_TABLE_ID;
  const res = await fetch(`https://api.airtable.com/v0/${baseId}/${tableId}/${hpcId}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  if (!res.ok) return null;
  const j = await res.json();
  return j.fields || null;
}

export async function loadCanonicalPhysicalProductInventory(hpcId, opts = {}) {
  const fields = await fetchHpcPhysicalProductFields(hpcId, opts);
  return resolvePhysicalProductInventory(hpcId, fields, {
    hotelName: opts.hotelName,
    preferProposedOverlay: opts.preferProposedOverlay === true,
  });
}

export { BETHESDA_PHYSICAL_PRODUCT_VALUES, FIELD as HE_PHYSICAL_PRODUCT_FIELD_MAP };
