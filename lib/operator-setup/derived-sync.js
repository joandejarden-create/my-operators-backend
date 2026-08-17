/**
 * Operator Setup — deterministic DERIVED summary sync (Phase B).
 * Dry-run by default. Does not invent OM/MA or portfolio %.
 *
 * Active Countries: Market Presence (current) ∪ Assignments (Current, named) — exclude Strategic Interest.
 * Location/conversion number fields are portfolio-% semantics — only sync when explicitly enabled
 * and evidence thresholds are met (default: off for Phase A+B safety).
 */
import { isAggregateAssignmentName } from "../operator-explorer/readiness.js";

export const DERIVED_SYNC_VERSION = "operator-setup-derived-sync-v1";

export const CURRENT_PRESENCE_TYPES = new Set([
  "Current Managed Property",
  "Current Operating Portfolio",
]);

export const EXCLUDED_PRESENCE_TYPES = new Set([
  "Strategic Interest",
  "Claimed Capability",
  "Historical Presence",
  "Historical",
  "Historical Only",
  "Pipeline",
  "Announced",
  "Regional Office or Team",
  "Active Development",
  "Unknown",
]);

const HELD_MASTER_IDS = new Set([
  "recJ6NPSYveCTo3At", // Tafer — Coral Beach hold
]);

function isPopulated(v) {
  if (v === null || v === undefined) return false;
  if (typeof v === "boolean") return true;
  if (typeof v === "number") return !Number.isNaN(v);
  if (typeof v === "string") return v.trim().length > 0;
  if (Array.isArray(v)) return v.length > 0;
  if (typeof v === "object") return Object.keys(v).length > 0;
  return Boolean(v);
}

function sameMultiSelect(a, b) {
  const aa = [...new Set((a || []).map(String))].sort();
  const bb = [...new Set((b || []).map(String))].sort();
  return aa.length === bb.length && aa.every((x, i) => x === bb[i]);
}

/**
 * @param {object} opts
 * @param {Array} opts.assignments
 * @param {Array} opts.marketPresence
 * @param {Array} opts.brandRelationships
 * @param {string} opts.masterId
 * @param {Set<string>} [opts.activeCountryOptions]
 */
export function deriveOperatorSummaries(opts) {
  const {
    assignments = [],
    marketPresence = [],
    brandRelationships = [],
    masterId,
    activeCountryOptions = null,
  } = opts;

  const asg = assignments.filter((r) => (r.fields?.Operator || []).includes(masterId));
  const namedCurrent = asg.filter(
    (r) =>
      !isAggregateAssignmentName(r.fields?.["Property Name"]) &&
      String(r.fields?.["Assignment Status"] || "") === "Current" &&
      !/Various/i.test(String(r.fields?.["Property Name"] || ""))
  );

  const countries = new Set();
  const countrySources = [];

  for (const r of marketPresence.filter((x) => (x.fields?.Operator || []).includes(masterId))) {
    const ptype = String(r.fields?.["Market Presence Type"] || r.fields?.["Presence Type"] || "");
    if (EXCLUDED_PRESENCE_TYPES.has(ptype)) continue;
    if (!CURRENT_PRESENCE_TYPES.has(ptype)) continue;
    const c = r.fields?.Country;
    if (c) {
      countries.add(String(c));
      countrySources.push({ kind: "market_presence", recordId: r.id, country: c, presenceType: ptype || null });
    }
  }

  for (const r of namedCurrent) {
    const c = r.fields?.Country;
    if (c) {
      countries.add(String(c));
      countrySources.push({ kind: "assignment", recordId: r.id, country: c, property: r.fields?.["Property Name"] });
    }
  }

  let activeCountries = [...countries].sort();
  const taxonomyExcluded = [];
  if (activeCountryOptions && activeCountryOptions.size) {
    const kept = [];
    for (const c of activeCountries) {
      if (activeCountryOptions.has(c)) kept.push(c);
      else taxonomyExcluded.push(c);
    }
    activeCountries = kept;
  }

  const brands = [
    ...new Set(
      [
        ...brandRelationships
          .filter((r) => (r.fields?.Operator || []).includes(masterId))
          .map((r) => r.fields?.Brand),
        ...namedCurrent.map((r) => r.fields?.Brand),
      ].filter(Boolean)
    ),
  ].sort();

  const developmentContexts = [
    ...new Set(namedCurrent.map((r) => r.fields?.["Development Context"]).filter(Boolean)),
  ];

  const hotelTypes = namedCurrent.map((r) => r.fields?.["Hotel Type"]).filter(Boolean);
  const resortCount = hotelTypes.filter((t) => /resort|all-inclusive/i.test(String(t))).length;
  const urbanCount = hotelTypes.filter((t) => /urban|city|airport|select-service|full-service/i.test(String(t)) && !/resort/i.test(String(t))).length;
  const aiCount = namedCurrent.filter((r) => r.fields?.["All-Inclusive"]).length;
  const esCount = namedCurrent.filter((r) => r.fields?.["Extended Stay"]).length;
  const conversionCount = namedCurrent.filter((r) =>
    /conversion|reflag|flag conversion|repositioning/i.test(String(r.fields?.["Development Context"] || ""))
  ).length;
  const newBuildCount = namedCurrent.filter((r) => /new build/i.test(String(r.fields?.["Development Context"] || ""))).length;

  return {
    masterId,
    activeCountries,
    taxonomyExcludedCountries: taxonomyExcluded,
    countrySources,
    brands,
    developmentContexts,
    namedCurrentCount: namedCurrent.length,
    hotelTypeEvidenceCount: hotelTypes.length,
    counts: {
      resort: resortCount,
      urban: urbanCount,
      allInclusive: aiCount,
      extendedStay: esCount,
      conversion: conversionCount,
      newBuild: newBuildCount,
    },
    held: HELD_MASTER_IDS.has(masterId),
  };
}

/**
 * Build Phase B mutations for one operator (Active Countries focus).
 */
export function buildDerivedMutationsForOperator({
  masterId,
  masterName,
  recordPurpose,
  platformRecord,
  profileRecord,
  derived,
  enablePortfolioPercents = false,
}) {
  const mutations = [];
  const provenance = [];
  const held = [];

  if (recordPurpose === "Test Fixture") return { mutations, provenance, held: [{ masterId, reason: "test_fixture" }] };
  if (derived.held) {
    held.push({ masterId, masterName, reason: "operator_hold_tafer_coral_beach" });
    // Still allow Active Countries if evidence exists — hold is assignment-specific; countries from other evidence OK
  }

  if (!platformRecord) {
    held.push({ masterId, masterName, field: "Active Countries", reason: "missing_platform_row" });
  } else if (derived.activeCountries.length) {
    const cur = platformRecord.fields?.["Active Countries"];
    if (!isPopulated(cur)) {
      mutations.push({
        table: "Operator Setup - Platform & Markets",
        recordId: platformRecord.id,
        masterId,
        masterName,
        field: "Active Countries",
        currentValue: null,
        proposedValue: derived.activeCountries,
        source: "Market Presence (current) + Assignments (Current, named)",
        treatment: "DERIVED",
        confidence: "high",
        conflictStatus: derived.taxonomyExcludedCountries.length
          ? `Excluded taxonomy: ${derived.taxonomyExcludedCountries.join("; ")}`
          : "None",
        whySafe: "Strategic Interest excluded; options filtered; named current assignments only",
      });
      provenance.push({
        masterId,
        field: "Active Countries",
        rule: "active_countries_v1",
        generatedAt: new Date().toISOString(),
        proposedValue: derived.activeCountries,
        sources: derived.countrySources,
        excludedCountries: derived.taxonomyExcludedCountries,
        evidenceThreshold: "≥1 current presence or current named assignment",
      });
    } else if (!sameMultiSelect(cur, derived.activeCountries)) {
      // Do not overwrite curated lists in Phase B — hold as conflict for founder
      held.push({
        masterId,
        masterName,
        field: "Active Countries",
        reason: "conflict_existing_vs_derived",
        currentValue: cur,
        proposedValue: derived.activeCountries,
      });
    }
  }

  // Portfolio % fields — off by default (CALA assignment sample ≠ global mix)
  if (enablePortfolioPercents && derived.hotelTypeEvidenceCount >= 5 && profileRecord) {
    const total = derived.hotelTypeEvidenceCount;
    const resortPct = Math.round((derived.counts.resort / total) * 100);
    if (!isPopulated(profileRecord.fields?.locationTypeResort) && resortPct > 0) {
      mutations.push({
        table: "Operator Setup - Profile & Positioning",
        recordId: profileRecord.id,
        masterId,
        masterName,
        field: "locationTypeResort",
        currentValue: null,
        proposedValue: resortPct,
        source: "Assignments.Hotel Type (Current named)",
        treatment: "DERIVED",
        confidence: "medium",
        conflictStatus: "Sample-based % — not global portfolio census",
        whySafe: "Blank only; ≥5 typed current assignments",
      });
    }
  }

  return { mutations, provenance, held };
}

export function isHeldMaster(masterId) {
  return HELD_MASTER_IDS.has(masterId);
}

export { isPopulated, sameMultiSelect, HELD_MASTER_IDS };
