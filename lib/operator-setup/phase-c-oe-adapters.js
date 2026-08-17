/**
 * Phase C — OE → Operator Setup adapters (researched-summary / section rows).
 * Downstream of Assignments / Market Presence / Brand Relationships / Master.
 * Never invents portfolio % or Fit-specific prefs.
 */
import { isAggregateAssignmentName } from "../operator-explorer/readiness.js";

export const PHASE_C_ADAPTER_VERSION = "operator-setup-phase-c-oe-adapters-v1";

const HELD_MASTERS = new Set(["recJ6NPSYveCTo3At"]); // Tafer Coral Beach hold

export const OM_MA_GAP_IDS = [
  "rec6UB6RpMKSs2tAo", // Remington
  "recJ6NPSYveCTo3At", // Tafer
  "recJtFkhjaO57rSDC", // Grupo Presidente
  "recOc5kpsg4Muip9Y", // Royalton
  "receHCdI6CEsJqdG4", // Brittain
  "reck6gjQd3wdeugmZ", // Arriva
  "rectsHzacZDFTH1Ze", // OxoHotel
  "recuEDrp6oeJIEuRX", // Grupo Marta
];

function nz(v) {
  return v != null && String(v).trim() !== "" ? String(v).trim() : "";
}

function namedCurrentAssignments(assignments, masterId) {
  return (assignments || []).filter(
    (r) =>
      (r.fields?.Operator || []).includes(masterId) &&
      !isAggregateAssignmentName(r.fields?.["Property Name"]) &&
      String(r.fields?.["Assignment Status"] || "") === "Current" &&
      !/Various/i.test(String(r.fields?.["Property Name"] || ""))
  );
}

/**
 * Propose OM/MA from unanimous/strong Assignment structure evidence only.
 * Does not infer MA from OM alone — MA requires third-party management evidence.
 */
export function proposeOmMaFromEvidence({ masterId, canonicalName, assignments, packHint }) {
  const named = namedCurrentAssignments(assignments, masterId);
  const structs = {};
  for (const r of named) {
    const s = nz(r.fields?.["Operating / Management Structure"]) || "(none)";
    structs[s] = (structs[s] || 0) + 1;
  }
  const entries = Object.entries(structs).sort((a, b) => b[1] - a[1]);
  const total = named.length;
  const tpm = (structs["Third-Party Management"] || 0) + (structs["Third-party management"] || 0);
  const ownerOp =
    (structs["Owner-Operated"] || 0) +
    (structs["Owner-operated"] || 0) +
    (structs["Owner-operated / managed"] || 0);
  const franchiseOp = structs["Franchise + Operator"] || 0;

  const base = {
    masterId,
    canonicalName,
    assignmentCount: total,
    structures: structs,
    packHint: packHint || null,
    held: HELD_MASTERS.has(masterId),
  };

  if (HELD_MASTERS.has(masterId)) {
    return {
      ...base,
      operatingModel: { status: "REMAIN UNKNOWN", value: null, reason: "operator_hold_tafer" },
      managementAvailability: { status: "REMAIN UNKNOWN", value: null, reason: "operator_hold_tafer" },
    };
  }

  // Unanimous third-party management
  if (total >= 3 && tpm === total) {
    return {
      ...base,
      operatingModel: {
        status: "SAFE WRITE",
        value: "Third-Party",
        confidence: "high",
        source: "Assignments.Operating / Management Structure (unanimous Third-Party Management)",
        reason: `${tpm}/${total} current named assignments are Third-Party Management`,
      },
      managementAvailability: {
        status: "SAFE WRITE",
        value: "Confirmed Direct Management",
        confidence: "high",
        source: "Assignments TPM + company operates as third-party manager",
        reason: "All evidenced assignments are third-party management contracts",
      },
    };
  }

  // Unanimous franchise + operator
  if (total >= 2 && franchiseOp === total) {
    return {
      ...base,
      operatingModel: {
        status: "SAFE WRITE",
        value: "Hybrid",
        confidence: "medium",
        source: "Assignments Franchise + Operator",
        reason: `${franchiseOp}/${total} Franchise + Operator`,
      },
      managementAvailability: {
        status: "SAFE WRITE",
        value: "Conditional / Scoped",
        confidence: "medium",
        source: "Franchise + Operator path implies scoped availability",
        reason: "Not pure third-party open availability",
      },
    };
  }

  // Mixed or owner-operated dominant without brand-managed clarity
  if (ownerOp > 0 && tpm > 0) {
    return {
      ...base,
      operatingModel: {
        status: "SAFE WRITE",
        value: "Hybrid",
        confidence: "medium",
        source: "Mixed Assignment structures",
        reason: `Owner-Operated ${ownerOp} + TPM ${tpm}`,
      },
      managementAvailability: {
        status: "SAFE WRITE",
        value: "Conditional / Scoped",
        confidence: "medium",
        source: "Mixed owner-operated and third-party assignments",
        reason: "Availability is scoped, not universal",
      },
    };
  }

  if (total >= 2 && ownerOp === total) {
    return {
      ...base,
      operatingModel: {
        status: "SAFE WRITE",
        value: "Owner-Operator",
        confidence: "medium",
        source: "Assignments Owner-Operated (unanimous)",
        reason: `${ownerOp}/${total} Owner-Operated`,
      },
      managementAvailability: {
        status: "REMAIN UNKNOWN",
        value: null,
        reason: "Owner-operated evidence does not prove third-party management availability",
      },
    };
  }

  return {
    ...base,
    operatingModel: {
      status: "REMAIN UNKNOWN",
      value: null,
      reason: `Insufficient unanimous structure evidence (${JSON.stringify(structs)})`,
    },
    managementAvailability: {
      status: "REMAIN UNKNOWN",
      value: null,
      reason: "No validated MA evidence without inference from OM alone",
    },
  };
}

/**
 * Build Setup Brand Relationships section rows from Intel Brand Relationships + Assignments.
 * Thin, evidence-backed portfolio mix — not golden-depth narrative clone.
 */
export function buildBrandRelationshipSectionRows({ masterId, brandRelationships, assignments }) {
  const br = (brandRelationships || []).filter((r) => (r.fields?.Operator || []).includes(masterId));
  const named = namedCurrentAssignments(assignments, masterId);
  const brandCounts = new Map();
  for (const r of [...br, ...named]) {
    const b = nz(r.fields?.Brand);
    if (!b) continue;
    brandCounts.set(b, (brandCounts.get(b) || 0) + 1);
  }
  const rows = [];
  let order = 10;
  if (!brandCounts.size) return rows;

  rows.push({
    section: "Brand Snapshot",
    row_key: "oe_adapter_brand_snapshot",
    display_order: order++,
    title: "Documented Brand Relationships",
    body: `Normalized Operator Explorer evidence shows ${brandCounts.size} distinct brand name(s) across Brand Relationships and current named Assignments.`,
    extra: "OE adapter — evidence summary",
  });

  for (const [brand, count] of [...brandCounts.entries()].sort((a, b) => b[1] - a[1]).slice(0, 12)) {
    const intel = br.filter((r) => nz(r.fields?.Brand) === brand);
    const relType = intel.map((r) => nz(r.fields?.["Relationship Type"])).filter(Boolean)[0] || "Assignment-evidenced";
    rows.push({
      section: "Portfolio Mix",
      row_key: `oe_adapter_portfolio_${brand.toLowerCase().replace(/[^a-z0-9]+/g, "_").slice(0, 40)}`,
      display_order: order++,
      title: brand,
      subtitle: relType,
      body: `Evidence count across Intel BR + current named Assignments: ${count}.`,
      extra: intel.length ? "Brand Relationship record(s) present" : "Assignment-only evidence",
    });
  }
  return rows;
}

/**
 * Thin Operating Platform capability rows from Assignments — only when evidence supports.
 */
export function buildOperatingPlatformSectionRows({ masterId, assignments, marketPresence }) {
  const named = namedCurrentAssignments(assignments, masterId);
  if (named.length < 2) return [];

  const countries = [...new Set(named.map((r) => nz(r.fields?.Country)).filter(Boolean))];
  const brands = [...new Set(named.map((r) => nz(r.fields?.Brand)).filter(Boolean))];
  const hotelTypes = named.map((r) => nz(r.fields?.["Hotel Type"])).filter(Boolean);
  const resortN = hotelTypes.filter((t) => /resort|all-inclusive/i.test(t)).length;
  const urbanN = hotelTypes.filter((t) => /urban|city|airport|select|full-service/i.test(t) && !/resort/i.test(t)).length;
  const aiN = named.filter((r) => r.fields?.["All-Inclusive"]).length;
  const convN = named.filter((r) =>
    /conversion|reflag|repositioning|flag conversion/i.test(String(r.fields?.["Development Context"] || ""))
  ).length;
  const newBuildN = named.filter((r) => /new build/i.test(String(r.fields?.["Development Context"] || ""))).length;
  const structures = [
    ...new Set(named.map((r) => nz(r.fields?.["Operating / Management Structure"])).filter(Boolean)),
  ];

  const mpCurrent = (marketPresence || []).filter(
    (r) =>
      (r.fields?.Operator || []).includes(masterId) &&
      ["Current Managed Property", "Current Operating Portfolio"].includes(
        String(r.fields?.["Market Presence Type"] || "")
      )
  );

  const rows = [];
  let order = 10;

  rows.push({
    section: "Platform Snapshot",
    row_type: "Capability",
    row_key: "oe_adapter_platform_snapshot",
    display_order: order++,
    title: "Current named operating evidence",
    body: `${named.length} current named assignment(s) across ${countries.length} country(ies)${
      brands.length ? ` and ${brands.length} brand name(s)` : ""
    }. Market Presence current rows: ${mpCurrent.length}.`,
  });

  if (countries.length) {
    rows.push({
      section: "Portfolio & Multi-Property Management",
      row_type: "Capability",
      row_key: "oe_adapter_multi_property",
      display_order: order++,
      title: "Multi-market operating footprint",
      body: `Current assignment countries: ${countries.sort().join("; ")}.`,
    });
  }

  if (structures.length) {
    rows.push({
      section: "Operational Execution & Labor",
      row_type: "Capability",
      row_key: "oe_adapter_structures",
      display_order: order++,
      title: "Documented operating structures",
      body: `Assignment structures observed: ${structures.join("; ")}.`,
    });
  }

  if (convN > 0 || newBuildN > 0) {
    rows.push({
      section: "Conversion & Repositioning",
      row_type: "Capability",
      row_key: "oe_adapter_development",
      display_order: order++,
      title: "Development context evidence",
      body: `Current named assignments with conversion/reflag/repositioning context: ${convN}; new-build context: ${newBuildN}.`,
    });
  }

  if (resortN > 0 || aiN > 0 || urbanN > 0) {
    rows.push({
      section: "F&B, Lifestyle & Resort",
      row_type: "Capability",
      row_key: "oe_adapter_hotel_types",
      display_order: order++,
      title: "Hotel-type evidence (assignments)",
      body: `Among typed current assignments — resort-like: ${resortN}; urban/select/full-service-like: ${urbanN}; all-inclusive flags: ${aiN}. Not a portfolio percentage.`,
    });
  }

  return rows;
}

/** Fields that must never be written by Phase C researched-summary packs */
export const PHASE_C_BLOCKED_FIELDS = new Set([
  "bf_fit",
  "bf_operating_situations",
  "bf_not_ideal_for",
  "bf_selected_deal_structures",
  "idealProject",
  "marketsToAvoid",
  "priorityMarkets",
  "feeExpectation",
  "knownRedFlags",
  "readyForInvestorPublication",
  "locationTypeUrban",
  "locationTypeSuburban",
  "locationTypeSmallMetro",
  "locationTypeInterstate",
  "locationTypeTotal",
  "locationTypeResort",
  "locationTypeAirport",
  "conversionExperience",
  "newBuildExperience",
  "turnaroundExperience",
  "preOpeningExperience",
  "renovationExperience",
  "Active Countries", // Phase B derived SoT
]);

export function isBlockedPhaseCField(name) {
  if (PHASE_C_BLOCKED_FIELDS.has(name)) return true;
  if (/^bf_/i.test(name)) return true;
  if (/^locationType/i.test(name)) return true;
  if (/Experience$/i.test(name) && !/company|brand|narrative/i.test(name)) return true;
  return false;
}

export { namedCurrentAssignments, HELD_MASTERS };
