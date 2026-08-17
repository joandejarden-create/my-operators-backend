/**
 * Golden Census Priority Schema — Autopilot V1.2
 * Priority Completeness denominator = applicable Priority fields only.
 */

export const GOLDEN_SCHEMA_VERSION = "census-autopilot-v1.2-golden-schema";

/** @typedef {'REQUIRED'|'CONDITIONAL'|'OPTIONAL'} Applicability */
/** @typedef {'PRIORITY'|'LIFECYCLE'|'OWNERSHIP_OPERATION'|'IMAGE'|'GOVERNANCE'} Track */
/** @typedef {'CRITICAL'|'HIGH'|'MEDIUM'|'LOW'} WeightBand */

/**
 * @param {object} p
 */
function f(p) {
  return Object.freeze({
    field: p.field,
    group: p.group,
    track: p.track,
    applicability: p.applicability,
    weight_band: p.weight_band,
    weight: p.weight,
    counts_toward_priority: p.track === "PRIORITY",
    notes: p.notes || "",
  });
}

const W = Object.freeze({ CRITICAL: 4, HIGH: 3, MEDIUM: 2, LOW: 1 });

/** Canonical Golden Census field registry. */
export const GOLDEN_FIELD_REGISTRY = Object.freeze([
  // —— PRIORITY GROUP 1 — Identity & Geography
  f({
    field: "Property Name",
    group: "G1_Identity_Geography",
    track: "PRIORITY",
    applicability: "REQUIRED",
    weight_band: "CRITICAL",
    weight: W.CRITICAL,
  }),
  f({
    field: "Current Brand",
    group: "G1_Identity_Geography",
    track: "PRIORITY",
    applicability: "REQUIRED",
    weight_band: "CRITICAL",
    weight: W.CRITICAL,
  }),
  f({
    field: "Brand Family",
    group: "G1_Identity_Geography",
    track: "PRIORITY",
    applicability: "REQUIRED",
    weight_band: "CRITICAL",
    weight: W.CRITICAL,
    notes: "Parent company / family (IHG, Hilton, Choice)",
  }),
  f({
    field: "Official Property ID",
    group: "G1_Identity_Geography",
    track: "PRIORITY",
    applicability: "CONDITIONAL",
    weight_band: "HIGH",
    weight: W.HIGH,
    notes: "Applicable when brand directory issues property codes",
  }),
  f({
    field: "Official Property URL",
    group: "G1_Identity_Geography",
    track: "PRIORITY",
    applicability: "REQUIRED",
    weight_band: "CRITICAL",
    weight: W.CRITICAL,
  }),
  f({
    field: "Address",
    group: "G1_Identity_Geography",
    track: "PRIORITY",
    applicability: "REQUIRED",
    weight_band: "CRITICAL",
    weight: W.CRITICAL,
  }),
  f({
    field: "City",
    group: "G1_Identity_Geography",
    track: "PRIORITY",
    applicability: "REQUIRED",
    weight_band: "CRITICAL",
    weight: W.CRITICAL,
  }),
  f({
    field: "State / Region",
    group: "G1_Identity_Geography",
    track: "PRIORITY",
    applicability: "REQUIRED",
    weight_band: "HIGH",
    weight: W.HIGH,
  }),
  f({
    field: "Country",
    group: "G1_Identity_Geography",
    track: "PRIORITY",
    applicability: "REQUIRED",
    weight_band: "CRITICAL",
    weight: W.CRITICAL,
  }),
  f({
    field: "Continent",
    group: "G1_Identity_Geography",
    track: "PRIORITY",
    applicability: "REQUIRED",
    weight_band: "HIGH",
    weight: W.HIGH,
    notes: "Dealality: Americas for Mexico/CALA",
  }),
  f({
    field: "Sub-Continent",
    group: "G1_Identity_Geography",
    track: "PRIORITY",
    applicability: "REQUIRED",
    weight_band: "HIGH",
    weight: W.HIGH,
    notes: "Mexico → North America (Dealality CALA subdivision)",
  }),
  f({
    field: "Market",
    group: "G1_Identity_Geography",
    track: "PRIORITY",
    applicability: "REQUIRED",
    weight_band: "CRITICAL",
    weight: W.CRITICAL,
    notes: "Dealality commercial market — not STR",
  }),
  f({
    field: "Submarket",
    group: "G1_Identity_Geography",
    track: "PRIORITY",
    applicability: "REQUIRED",
    weight_band: "CRITICAL",
    weight: W.CRITICAL,
    notes: "Dealality corridor — not STR",
  }),
  f({
    field: "Postal Code",
    group: "G1_Identity_Geography",
    track: "PRIORITY",
    applicability: "OPTIONAL",
    weight_band: "LOW",
    weight: W.LOW,
  }),
  f({
    field: "Latitude",
    group: "G1_Identity_Geography",
    track: "PRIORITY",
    applicability: "REQUIRED",
    weight_band: "HIGH",
    weight: W.HIGH,
  }),
  f({
    field: "Longitude",
    group: "G1_Identity_Geography",
    track: "PRIORITY",
    applicability: "REQUIRED",
    weight_band: "HIGH",
    weight: W.HIGH,
  }),
  f({
    field: "Phone",
    group: "G1_Identity_Geography",
    track: "PRIORITY",
    applicability: "REQUIRED",
    weight_band: "HIGH",
    weight: W.HIGH,
  }),

  // —— PRIORITY GROUP 2 — Physical Profile
  f({
    field: "Rooms / Keys",
    group: "G2_Physical_Profile",
    track: "PRIORITY",
    applicability: "REQUIRED",
    weight_band: "CRITICAL",
    weight: W.CRITICAL,
  }),
  f({
    field: "Suites",
    group: "G2_Physical_Profile",
    track: "PRIORITY",
    applicability: "CONDITIONAL",
    weight_band: "MEDIUM",
    weight: W.MEDIUM,
    notes: "N/A when source ecosystem does not disclose suite counts",
  }),
  f({
    field: "Floors",
    group: "G2_Physical_Profile",
    track: "PRIORITY",
    applicability: "OPTIONAL",
    weight_band: "LOW",
    weight: W.LOW,
  }),
  f({
    field: "Property Type",
    group: "G2_Physical_Profile",
    track: "PRIORITY",
    applicability: "REQUIRED",
    weight_band: "HIGH",
    weight: W.HIGH,
  }),
  f({
    field: "Asset Context",
    group: "G2_Physical_Profile",
    track: "PRIORITY",
    applicability: "CONDITIONAL",
    weight_band: "MEDIUM",
    weight: W.MEDIUM,
  }),
  f({
    field: "Resort / Urban",
    group: "G2_Physical_Profile",
    track: "PRIORITY",
    applicability: "REQUIRED",
    weight_band: "MEDIUM",
    weight: W.MEDIUM,
  }),
  f({
    field: "Boutique Flag",
    group: "G2_Physical_Profile",
    track: "PRIORITY",
    applicability: "OPTIONAL",
    weight_band: "LOW",
    weight: W.LOW,
  }),
  f({
    field: "All-Suite Flag",
    group: "G2_Physical_Profile",
    track: "PRIORITY",
    applicability: "CONDITIONAL",
    weight_band: "LOW",
    weight: W.LOW,
  }),
  f({
    field: "Mixed-Use Flag",
    group: "G2_Physical_Profile",
    track: "PRIORITY",
    applicability: "OPTIONAL",
    weight_band: "LOW",
    weight: W.LOW,
  }),
  f({
    field: "Branded Residences Flag",
    group: "G2_Physical_Profile",
    track: "PRIORITY",
    applicability: "OPTIONAL",
    weight_band: "LOW",
    weight: W.LOW,
  }),

  // —— PRIORITY GROUP 3 — Amenities (boolean-like)
  f({
    field: "Amenities - Source Text",
    group: "G3_Amenities",
    track: "PRIORITY",
    applicability: "REQUIRED",
    weight_band: "HIGH",
    weight: W.HIGH,
  }),
  f({
    field: "Amenities - Structured Tags",
    group: "G3_Amenities",
    track: "PRIORITY",
    applicability: "REQUIRED",
    weight_band: "MEDIUM",
    weight: W.MEDIUM,
  }),
  f({
    field: "Pool",
    group: "G3_Amenities",
    track: "PRIORITY",
    applicability: "OPTIONAL",
    weight_band: "MEDIUM",
    weight: W.MEDIUM,
    notes: "Yes | Unknown — never infer No from absence; OPTIONAL for Priority denominator (Source Text + Tags are REQUIRED)",
  }),
  f({
    field: "Spa",
    group: "G3_Amenities",
    track: "PRIORITY",
    applicability: "OPTIONAL",
    weight_band: "MEDIUM",
    weight: W.MEDIUM,
  }),
  f({
    field: "Fitness",
    group: "G3_Amenities",
    track: "PRIORITY",
    applicability: "OPTIONAL",
    weight_band: "MEDIUM",
    weight: W.MEDIUM,
  }),
  f({
    field: "Golf",
    group: "G3_Amenities",
    track: "PRIORITY",
    applicability: "OPTIONAL",
    weight_band: "LOW",
    weight: W.LOW,
  }),
  f({
    field: "Beach / Beachfront",
    group: "G3_Amenities",
    track: "PRIORITY",
    applicability: "CONDITIONAL",
    weight_band: "MEDIUM",
    weight: W.MEDIUM,
  }),
  f({
    field: "Beach Club",
    group: "G3_Amenities",
    track: "PRIORITY",
    applicability: "OPTIONAL",
    weight_band: "LOW",
    weight: W.LOW,
  }),
  f({
    field: "Casino",
    group: "G3_Amenities",
    track: "PRIORITY",
    applicability: "OPTIONAL",
    weight_band: "LOW",
    weight: W.LOW,
  }),
  f({
    field: "Kids Club",
    group: "G3_Amenities",
    track: "PRIORITY",
    applicability: "OPTIONAL",
    weight_band: "LOW",
    weight: W.LOW,
  }),
  f({
    field: "Club Lounge",
    group: "G3_Amenities",
    track: "PRIORITY",
    applicability: "OPTIONAL",
    weight_band: "LOW",
    weight: W.LOW,
  }),
  f({
    field: "All-Inclusive",
    group: "G3_Amenities",
    track: "PRIORITY",
    applicability: "OPTIONAL",
    weight_band: "MEDIUM",
    weight: W.MEDIUM,
    notes: "Researched when coastal/AI-brand; OPTIONAL for Priority denominator — Unknown must not fail select-service coastal hotels",
  }),
  f({
    field: "Parking",
    group: "G3_Amenities",
    track: "PRIORITY",
    applicability: "OPTIONAL",
    weight_band: "LOW",
    weight: W.LOW,
  }),
  f({
    field: "Airport Shuttle",
    group: "G3_Amenities",
    track: "PRIORITY",
    applicability: "OPTIONAL",
    weight_band: "LOW",
    weight: W.LOW,
  }),
  f({
    field: "Ski",
    group: "G3_Amenities",
    track: "PRIORITY",
    applicability: "CONDITIONAL",
    weight_band: "LOW",
    weight: W.LOW,
    notes: "N/A for most Mexico coastal/urban properties",
  }),
  f({
    field: "Residences Amenity",
    group: "G3_Amenities",
    track: "PRIORITY",
    applicability: "OPTIONAL",
    weight_band: "LOW",
    weight: W.LOW,
  }),

  // —— PRIORITY GROUP 4 — F&B
  f({
    field: "Restaurant Count",
    group: "G4_FB",
    track: "PRIORITY",
    applicability: "CONDITIONAL",
    weight_band: "MEDIUM",
    weight: W.MEDIUM,
    notes: "Only when defensible count exists",
  }),
  f({
    field: "Restaurant Names",
    group: "G4_FB",
    track: "PRIORITY",
    applicability: "OPTIONAL",
    weight_band: "LOW",
    weight: W.LOW,
  }),
  f({
    field: "Bars / Lounges",
    group: "G4_FB",
    track: "PRIORITY",
    applicability: "OPTIONAL",
    weight_band: "LOW",
    weight: W.LOW,
  }),
  f({
    field: "Rooftop F&B",
    group: "G4_FB",
    track: "PRIORITY",
    applicability: "OPTIONAL",
    weight_band: "LOW",
    weight: W.LOW,
  }),
  f({
    field: "Signature / Third-Party Restaurant",
    group: "G4_FB",
    track: "PRIORITY",
    applicability: "OPTIONAL",
    weight_band: "LOW",
    weight: W.LOW,
  }),
  f({
    field: "Room Service",
    group: "G4_FB",
    track: "PRIORITY",
    applicability: "OPTIONAL",
    weight_band: "LOW",
    weight: W.LOW,
  }),
  f({
    field: "F&B Flag",
    group: "G4_FB",
    track: "PRIORITY",
    applicability: "REQUIRED",
    weight_band: "MEDIUM",
    weight: W.MEDIUM,
  }),

  // —— PRIORITY GROUP 5 — Meetings
  f({
    field: "Meeting / Event Space",
    group: "G5_Meetings",
    track: "PRIORITY",
    applicability: "REQUIRED",
    weight_band: "MEDIUM",
    weight: W.MEDIUM,
  }),
  f({
    field: "Total Meeting Space",
    group: "G5_Meetings",
    track: "PRIORITY",
    applicability: "CONDITIONAL",
    weight_band: "MEDIUM",
    weight: W.MEDIUM,
  }),
  f({
    field: "Largest Meeting Room / Ballroom",
    group: "G5_Meetings",
    track: "PRIORITY",
    applicability: "CONDITIONAL",
    weight_band: "LOW",
    weight: W.LOW,
  }),
  f({
    field: "Number of Meeting Rooms",
    group: "G5_Meetings",
    track: "PRIORITY",
    applicability: "CONDITIONAL",
    weight_band: "MEDIUM",
    weight: W.MEDIUM,
    notes: "Never map to Rooms / Keys",
  }),
  f({
    field: "Ballroom",
    group: "G5_Meetings",
    track: "PRIORITY",
    applicability: "OPTIONAL",
    weight_band: "LOW",
    weight: W.LOW,
  }),
  f({
    field: "Convention Hotel",
    group: "G5_Meetings",
    track: "PRIORITY",
    applicability: "OPTIONAL",
    weight_band: "LOW",
    weight: W.LOW,
  }),

  // —— PRIORITY GROUP 6 — Dealality Classification
  f({
    field: "Dealality Segment / Positioning",
    group: "G6_Dealality_Classification",
    track: "PRIORITY",
    applicability: "REQUIRED",
    weight_band: "MEDIUM",
    weight: W.MEDIUM,
    notes: "Derived Dealality taxonomy — not STR Chain Scale",
  }),

  // —— PRIORITY GROUP 7 — Content
  f({
    field: "Hotel Description - Source Text",
    group: "G7_Content",
    track: "PRIORITY",
    applicability: "REQUIRED",
    weight_band: "MEDIUM",
    weight: W.MEDIUM,
  }),
  f({
    field: "Hotel Description - AI Summary",
    group: "G7_Content",
    track: "PRIORITY",
    applicability: "CONDITIONAL",
    weight_band: "LOW",
    weight: W.LOW,
    notes: "Dealality-original from verified facts — conditional until source text exists",
  }),

  // —— IMAGE (separate score)
  f({
    field: "Hero Image",
    group: "Image",
    track: "IMAGE",
    applicability: "OPTIONAL",
    weight_band: "MEDIUM",
    weight: W.MEDIUM,
  }),
  f({
    field: "Image Rights Status",
    group: "Image",
    track: "IMAGE",
    applicability: "REQUIRED",
    weight_band: "HIGH",
    weight: W.HIGH,
  }),

  // —— LIFECYCLE (excluded from Priority 95%)
  f({
    field: "Affiliation Status",
    group: "Lifecycle",
    track: "LIFECYCLE",
    applicability: "REQUIRED",
    weight_band: "CRITICAL",
    weight: W.CRITICAL,
  }),
  f({
    field: "Opening Date",
    group: "Lifecycle",
    track: "LIFECYCLE",
    applicability: "OPTIONAL",
    weight_band: "HIGH",
    weight: W.HIGH,
  }),
  f({
    field: "Expected Opening Date",
    group: "Lifecycle",
    track: "LIFECYCLE",
    applicability: "CONDITIONAL",
    weight_band: "MEDIUM",
    weight: W.MEDIUM,
  }),
  f({
    field: "Renovation Date",
    group: "Lifecycle",
    track: "LIFECYCLE",
    applicability: "OPTIONAL",
    weight_band: "MEDIUM",
    weight: W.MEDIUM,
  }),
  f({
    field: "Conversion / Reflag Date",
    group: "Lifecycle",
    track: "LIFECYCLE",
    applicability: "OPTIONAL",
    weight_band: "MEDIUM",
    weight: W.MEDIUM,
  }),

  // —— OWNERSHIP / OPERATION (excluded)
  f({
    field: "Owner Name",
    group: "Ownership_Operation",
    track: "OWNERSHIP_OPERATION",
    applicability: "OPTIONAL",
    weight_band: "HIGH",
    weight: W.HIGH,
  }),
  f({
    field: "Developer Name",
    group: "Ownership_Operation",
    track: "OWNERSHIP_OPERATION",
    applicability: "OPTIONAL",
    weight_band: "MEDIUM",
    weight: W.MEDIUM,
  }),
  f({
    field: "Operator / Management Company",
    group: "Ownership_Operation",
    track: "OWNERSHIP_OPERATION",
    applicability: "OPTIONAL",
    weight_band: "HIGH",
    weight: W.HIGH,
  }),
  f({
    field: "Operation Type",
    group: "Ownership_Operation",
    track: "OWNERSHIP_OPERATION",
    applicability: "OPTIONAL",
    weight_band: "MEDIUM",
    weight: W.MEDIUM,
  }),

  // —— GOVERNANCE
  f({
    field: "Source URL",
    group: "Governance",
    track: "GOVERNANCE",
    applicability: "REQUIRED",
    weight_band: "CRITICAL",
    weight: W.CRITICAL,
  }),
  f({
    field: "Source Type",
    group: "Governance",
    track: "GOVERNANCE",
    applicability: "REQUIRED",
    weight_band: "HIGH",
    weight: W.HIGH,
  }),
  f({
    field: "Data Confidence Tier",
    group: "Governance",
    track: "GOVERNANCE",
    applicability: "REQUIRED",
    weight_band: "HIGH",
    weight: W.HIGH,
  }),
  f({
    field: "Production Use Status",
    group: "Governance",
    track: "GOVERNANCE",
    applicability: "REQUIRED",
    weight_band: "CRITICAL",
    weight: W.CRITICAL,
  }),
  f({
    field: "Last Verified",
    group: "Governance",
    track: "GOVERNANCE",
    applicability: "REQUIRED",
    weight_band: "HIGH",
    weight: W.HIGH,
  }),
]);

export function priorityFields() {
  return GOLDEN_FIELD_REGISTRY.filter((x) => x.counts_toward_priority);
}

export function fieldsByTrack(track) {
  return GOLDEN_FIELD_REGISTRY.filter((x) => x.track === track);
}

/**
 * Completeness-bearing priority fields: REQUIRED + CONDITIONAL (when applicable) + OPTIONAL that are designated completeness-bearing.
 * OPTIONAL low-weight fields count only when applicability resolves to applicable (default: OPTIONAL counts if not N/A).
 */
export function isCompletenessBearing(entry) {
  if (!entry.counts_toward_priority) return false;
  // OPTIONAL amenity booleans still count when applicable (Unknown = incomplete)
  return true;
}

export function buildApplicabilityMap() {
  const required = [];
  const conditional = [];
  const optional = [];
  for (const e of priorityFields()) {
    if (e.applicability === "REQUIRED") required.push(e.field);
    else if (e.applicability === "CONDITIONAL") conditional.push(e.field);
    else optional.push(e.field);
  }
  return { required, conditional, optional };
}
