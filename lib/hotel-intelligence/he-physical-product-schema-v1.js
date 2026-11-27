/**
 * HE 5.0 physical-product HPC schema contract (founder-authorized 2026-11-27).
 * Additive fields only on Hotel Property Census. No legacy Hotel Census.
 */
export const HE_PHYSICAL_PRODUCT_SCHEMA_V1 = "he-physical-product-schema-v1";

export const HE_PHYSICAL_PRODUCT_BASE_ID = "appCCUsuGsE1ifoLk";
export const HE_PHYSICAL_PRODUCT_HPC_TABLE_ID = "tbl9aY5ijiuIzzWam";

/** Existing fields — never recreate. */
export const HE_PHYSICAL_PRODUCT_EXISTING_FIELDS = Object.freeze([
  "Rooms / Keys",
  "Meeting Space Flag",
  "Rooms Notes",
  "Rooms Source URL",
  "Rooms Confidence",
]);

/**
 * Create-only field plan aligned to BETH_MEETING_SPACE_SCHEMA_PROPOSAL_V1.json
 * and founder authorization for meeting + guestroom configuration.
 */
export const HE_PHYSICAL_PRODUCT_FIELDS_TO_CREATE = Object.freeze([
  {
    name: "Total Meeting Space (sq ft)",
    type: "number",
    description:
      "Published capacity-chart aggregate meeting space in square feet. Never store marketing private-space figures here.",
    options: { precision: 0 },
  },
  {
    name: "Meeting Rooms Count",
    type: "number",
    description: "Published meeting-room count from official capacity materials.",
    options: { precision: 0 },
  },
  {
    name: "Largest Meeting Room Name",
    type: "singleLineText",
    description: "Name of the largest function room (e.g. Grand Ballroom).",
  },
  {
    name: "Largest Meeting Room (sq ft)",
    type: "number",
    description: "Area of the largest function room in square feet.",
    options: { precision: 0 },
  },
  {
    name: "Guestroom Single Count",
    type: "number",
    description:
      "Classified single-bed guestrooms. Part of Rooms / Keys total; must not exceed Rooms / Keys.",
    options: { precision: 0 },
  },
  {
    name: "Guestroom Double Count",
    type: "number",
    description:
      "Classified double-bed guestrooms. Part of Rooms / Keys total; Single+Double should equal Rooms / Keys when both known.",
    options: { precision: 0 },
  },
  {
    name: "Guestroom Suites Count",
    type: "number",
    description:
      "Suite classification WITHIN Rooms / Keys. Never additive to the rooms total.",
    options: { precision: 0 },
  },
  {
    name: "Guestroom Mix Confidence",
    type: "singleSelect",
    description: "Confidence for guestroom configuration / mix classification.",
    options: {
      choices: [
        { name: "High" },
        { name: "Medium" },
        { name: "Brand Materials Reference" },
        { name: "Insufficient" },
      ],
    },
  },
  {
    name: "Meeting Space Source URL",
    type: "url",
    description: "Official capacity-chart / meeting events source URL.",
  },
]);

export const BETHESDA_PHYSICAL_PRODUCT_VALUES = Object.freeze({
  hpcId: "recLuxvwwxID7U2B8",
  hotelName: "Bethesda Marriott",
  fields: Object.freeze({
    "Rooms / Keys": 407,
    "Total Meeting Space (sq ft)": 18719,
    "Meeting Rooms Count": 27,
    "Largest Meeting Room Name": "Grand Ballroom",
    "Largest Meeting Room (sq ft)": 4592,
    "Guestroom Single Count": 171,
    "Guestroom Double Count": 236,
    "Guestroom Suites Count": 8,
    "Guestroom Mix Confidence": "Brand Materials Reference",
    "Meeting Space Source URL":
      "https://www.marriott.com/en-us/hotels/wasbt-bethesda-marriott/events/",
    "Meeting Space Flag": true,
  }),
  invariants: Object.freeze({
    singlePlusDoubleEqualsTotal: true,
    suitesNotAdditive: true,
    marketingPrivateSpaceSqFtExcluded: 19000,
  }),
  evidence: Object.freeze({
    capacityChartUrl:
      "https://www.marriott.com/en-us/hotels/wasbt-bethesda-marriott/events/",
    stagedPacket:
      "reports/hotel-explorer/phase4-quality/HE-BETH-GUEST-ROOMS-MEETING-SPACE-STAGED-V1.json",
  }),
});

export function assertBethesdaPhysicalProductInvariants(fields) {
  const total = Number(fields["Rooms / Keys"]);
  const single = Number(fields["Guestroom Single Count"]);
  const dbl = Number(fields["Guestroom Double Count"]);
  const suites = Number(fields["Guestroom Suites Count"]);
  const meeting = Number(fields["Total Meeting Space (sq ft)"]);
  if (total !== 407) throw new Error("Rooms / Keys must be 407");
  if (single !== 171 || dbl !== 236) throw new Error("guestroom mix must be 171/236");
  if (single + dbl !== total) throw new Error("171+236 must equal 407");
  if (suites !== 8) throw new Error("suites must be 8 (subset)");
  if (meeting !== 18719) throw new Error("meeting space must be 18719 not 19000");
  if (meeting === 19000) throw new Error("marketing figure prohibited");
  return true;
}
