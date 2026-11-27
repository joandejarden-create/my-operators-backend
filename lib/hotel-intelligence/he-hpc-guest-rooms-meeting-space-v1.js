/**
 * Map canonical Hotel Property Census fields → Hotel Explorer
 * guestRoomsMeetingSpace DTO (Live API hydration).
 * Prefer Airtable-persisted scalars; never invent inventory rows.
 */

export const HE_HPC_GUEST_ROOMS_MEETING_SPACE_V1 = "he-hpc-guest-rooms-meeting-space-v1";

function num(v) {
  if (v == null || v === "") return null;
  const n = typeof v === "number" ? v : Number(String(v).replace(/,/g, ""));
  return Number.isFinite(n) ? n : null;
}

function str(v) {
  const s = String(v == null ? "" : v).trim();
  return s || null;
}

function mixStatus(confidence) {
  const c = String(confidence || "").toLowerCase();
  if (c.includes("brand")) return "BRAND_MATERIALS_REFERENCE";
  if (c.includes("high") || c.includes("verified")) return "VERIFIED";
  return "VERIFIED";
}

/**
 * @param {Record<string, unknown>|null|undefined} fields HPC record fields
 * @param {{ hpcId?: string, hotelName?: string }} [meta]
 * @returns {object|null} guestRoomsMeetingSpace payload or null when no product fields present
 */
export function buildGuestRoomsMeetingSpaceFromHpc(fields, meta = {}) {
  if (!fields || typeof fields !== "object") return null;

  const total = num(fields["Rooms / Keys"] ?? fields.rooms);
  const single = num(fields["Guestroom Single Count"]);
  const double = num(fields["Guestroom Double Count"]);
  const suites = num(fields["Guestroom Suites Count"]);
  const mixConfidence = str(fields["Guestroom Mix Confidence"]);
  const meetingTotal = num(
    fields["Total Meeting Space (sq ft)"] ??
      fields["Total Meeting Space Sq Ft"] ??
      fields["Total Meeting Space"],
  );
  const meetingCount = num(fields["Meeting Rooms Count"]);
  const largestArea = num(fields["Largest Meeting Room (sq ft)"]);
  const largestName = str(fields["Largest Meeting Room Name"]);
  const meetingSourceUrl = str(fields["Meeting Space Source URL"]);
  const meetingFlag = fields["Meeting Space Flag"] === true;

  const hasProduct =
    total != null ||
    meetingTotal != null ||
    meetingCount != null ||
    single != null ||
    double != null ||
    suites != null ||
    largestArea != null;

  if (!hasProduct) return null;

  const guestMixStatus = mixStatus(mixConfidence);

  const guestRooms = {
    totalGuestrooms:
      total != null
        ? {
            value: total,
            verificationStatus: "VERIFIED",
            source: "Hotel Property Census",
            note: "Canonical rooms count",
          }
        : undefined,
    singleBedRooms:
      single != null
        ? {
            value: single,
            verificationStatus: guestMixStatus,
            source: "Hotel Property Census",
            note: mixConfidence || "Persisted guestroom mix",
          }
        : undefined,
    doubleBedRooms:
      double != null
        ? {
            value: double,
            verificationStatus: guestMixStatus,
            source: "Hotel Property Census",
            note: mixConfidence || "Persisted guestroom mix",
          }
        : undefined,
    suites:
      suites != null
        ? {
            value: suites,
            verificationStatus: guestMixStatus,
            source: "Hotel Property Census",
            note: "Suite count is a subset classification — do not add to total keys",
          }
        : undefined,
    aggregationRule:
      "TOTAL_IS_CANONICAL; ROOM_TYPE_COUNTS_ARE_PARTITION_OF_TOTAL; SUITES_NOT_ADDITIVE",
  };

  const meetingSpaceSummary = {
    totalMeetingSpaceSqFt:
      meetingTotal != null
        ? {
            value: meetingTotal,
            verificationStatus: "VERIFIED",
            sourceUrl: meetingSourceUrl,
            source: "Hotel Property Census",
            note: meetingFlag
              ? "Meeting Space Flag true on HPC"
              : "Persisted total meeting space",
          }
        : undefined,
    meetingRoomCount:
      meetingCount != null
        ? {
            value: meetingCount,
            verificationStatus: "VERIFIED",
            sourceUrl: meetingSourceUrl,
            source: "Hotel Property Census",
          }
        : undefined,
    largestRoom:
      largestArea != null || largestName
        ? {
            name: largestName || "Largest meeting room",
            areaSqFt: largestArea,
            verificationStatus: "VERIFIED",
            sourceUrl: meetingSourceUrl,
            source: "Hotel Property Census",
          }
        : undefined,
    floorPlanUrl: meetingSourceUrl
      ? {
          value: meetingSourceUrl,
          verificationStatus: "VERIFIED",
          label: "Official meeting & events materials",
        }
      : undefined,
    outdoorEventSpace: {
      value: null,
      verificationStatus: "NOT_YET_VERIFIED",
    },
  };

  const sources = [];
  if (meetingSourceUrl) {
    sources.push({
      title: "Meeting space source (HPC)",
      url: meetingSourceUrl,
      supports: ["meeting_space_aggregate", "meeting_room_count", "largest_room"],
      verificationStatus: "VERIFIED",
      publisher: "Hotel Property Census",
    });
  }
  if (total != null) {
    sources.push({
      title: "Hotel Property Census — Rooms / Keys",
      url: null,
      supports: ["total_guestrooms"],
      verificationStatus: "VERIFIED",
      publisher: "Hotel Property Census",
    });
  }

  return {
    version: HE_HPC_GUEST_ROOMS_MEETING_SPACE_V1,
    hpcId: meta.hpcId || null,
    hotelName: meta.hotelName || str(fields["Property Name"]) || null,
    persisted: true,
    writeOnGet: false,
    source: "HOTEL_PROPERTY_CENSUS",
    guestRooms,
    meetingSpaceSummary,
    // Per-room inventory is not stored as HPC scalars; leave empty rather than invent.
    meetingRooms: [],
    sources,
    limitations: [
      "Meeting room inventory rows require a durable room-level store; HPC supplies summary metrics only.",
    ],
  };
}

export default {
  HE_HPC_GUEST_ROOMS_MEETING_SPACE_V1,
  buildGuestRoomsMeetingSpaceFromHpc,
};
