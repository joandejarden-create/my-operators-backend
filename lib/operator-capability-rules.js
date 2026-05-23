/**
 * Operator Capability Snapshot — deal-only capability signals (no operator matching).
 */

import {
  DEALS_FIELDS,
  LOCATION_FIELDS,
  SI_FIELDS,
  isOperatorInScopeFromFields,
  strVal,
  listVal,
} from "./operator-capability-inputs.js";
import { detectOperatingModelConflicts } from "./operator-capability-backfill.js";
import {
  resolveProjectTypeKind,
  isOtherToBeConfirmedProjectType,
} from "./project-type.js";

const CAPABILITY_BY_ID = {
  full_management: { label: "Full hotel management" },
  pre_opening: { label: "Pre-opening / opening support" },
  conversion_pip: { label: "Conversion & PIP execution" },
  revenue_distribution: { label: "Revenue management & distribution" },
  procurement: { label: "Procurement & cost control" },
  accounting_reporting: { label: "Accounting & owner reporting" },
  fb: { label: "F&B / culinary operations" },
  sales_marketing: { label: "Sales & marketing" },
  hr_training: { label: "HR & training" },
  technology: { label: "Technology & systems" },
  design_renovation: { label: "Design / renovation PM" },
  asset_capex: { label: "Asset management / capex planning" },
  cala_local: { label: "Local market / CALA execution" },
  lifestyle: { label: "Lifestyle / experience programming" },
  crisis: { label: "Crisis / business continuity" },
  brand_standards: { label: "Brand standards alignment" },
  operator_transition: { label: "Operator transition / handover" },
  commercial_repositioning: { label: "Commercial repositioning" },
  operating_while_renovating: { label: "Operating-while-renovating coordination" },
  development_complexity: { label: "Development complexity / permitting" },
  governance: { label: "Owner–operator–brand governance" },
  amenity_complexity: { label: "F&B / amenity complexity" },
};

/** @param {string} id @param {string} source @param {"stated"|"inferred"|"needs_validation"} strength */
function cap(id, source, strength = "inferred") {
  const def = CAPABILITY_BY_ID[id];
  if (!def) return null;
  return { id, label: def.label, sources: [source], strength };
}

/**
 * Capability IDs suggested by canonical project type (before generic text rules).
 * @param {import("./project-type.js").ProjectTypeKind} kind
 * @returns {{ id: string, strength: "inferred"|"needs_validation" }[]}
 */
export function capabilityIdsForProjectTypeKind(kind) {
  switch (kind) {
    case "new_build":
      return [
        { id: "pre_opening", strength: "inferred" },
        { id: "design_renovation", strength: "inferred" },
        { id: "development_complexity", strength: "inferred" },
      ];
    case "conversion_reflag":
      return [
        { id: "conversion_pip", strength: "inferred" },
        { id: "brand_standards", strength: "inferred" },
        { id: "operator_transition", strength: "inferred" },
        { id: "pre_opening", strength: "inferred" },
      ];
    case "renovation_repositioning":
      return [
        { id: "conversion_pip", strength: "inferred" },
        { id: "asset_capex", strength: "inferred" },
        { id: "commercial_repositioning", strength: "inferred" },
        { id: "operating_while_renovating", strength: "inferred" },
        { id: "revenue_distribution", strength: "inferred" },
      ];
    case "existing_operating":
      return [
        { id: "full_management", strength: "inferred" },
        { id: "accounting_reporting", strength: "inferred" },
        { id: "revenue_distribution", strength: "inferred" },
      ];
    case "adaptive_reuse":
      return [
        { id: "development_complexity", strength: "inferred" },
        { id: "conversion_pip", strength: "inferred" },
        { id: "pre_opening", strength: "inferred" },
      ];
    case "mixed_use":
      return [
        { id: "governance", strength: "inferred" },
        { id: "amenity_complexity", strength: "inferred" },
        { id: "fb", strength: "inferred" },
        { id: "full_management", strength: "inferred" },
      ];
    case "other_tbc":
      return [];
    default:
      return [];
  }
}

const GENERIC_MATCH_DEFS = [
  { id: "full_management", match: /full hotel management|third.party management only|brand \+ third/i },
  { id: "pre_opening", match: /pre-opening|soft opening|pre-opening ramp|construction/i },
  { id: "revenue_distribution", match: /revenue management|distribution/i },
  { id: "accounting_reporting", match: /accounting|owner reporting|reporting package|weekly financial|monthly p/i },
  { id: "cala_local", match: /cala|local market/i },
];

/**
 * @param {Record<string, unknown>} fields
 * @returns {{ id: string, label: string, sources: string[], strength: "stated" | "inferred" | "needs_validation" }[]}
 */
export function deriveCapabilityAreas(fields) {
  const f = fields || {};
  const stated = new Set(listVal(f[SI_FIELDS.operatorCapabilityPriorities]));
  const out = [];
  const seen = new Set();

  for (const [id, def] of Object.entries(CAPABILITY_BY_ID)) {
    if (stated.has(def.label)) {
      const row = cap(id, "Operator Capability Priorities", "stated");
      if (row) {
        out.push(row);
        seen.add(id);
      }
    }
  }

  const kind = resolveProjectTypeKind(f[DEALS_FIELDS.projectType]);
  for (const { id, strength } of capabilityIdsForProjectTypeKind(kind)) {
    if (seen.has(id)) continue;
    const row = cap(id, `Project Type (${strVal(f[DEALS_FIELDS.projectType]) || kind})`, strength);
    if (row) {
      out.push(row);
      seen.add(id);
    }
  }

  const textBlob = [
    strVal(f[DEALS_FIELDS.openingTransitionPhase]),
    strVal(f[SI_FIELDS.planSelfManage]),
    strVal(f["Preferred Deal Structure"]),
    strVal(f[SI_FIELDS.servicesRequired]),
    strVal(f[LOCATION_FIELDS.primaryMarketRegion]),
    strVal(f["Stage of Development"]),
    strVal(f["PIP / CapEx Status"]),
  ]
    .filter(Boolean)
    .join(" | ");

  for (const def of GENERIC_MATCH_DEFS) {
    if (seen.has(def.id)) continue;
    if (def.match.test(textBlob)) {
      const row = cap(def.id, "Deal context inference", "inferred");
      if (row) {
        out.push(row);
        seen.add(def.id);
      }
    }
  }

  return out.slice(0, 12);
}

/**
 * @param {Record<string, unknown>} fields
 */
export function buildOperatingContext(fields) {
  const f = fields || {};
  const rawPt = strVal(f[DEALS_FIELDS.projectType]);
  return {
    currentOperatingModel: strVal(f[DEALS_FIELDS.currentOperatingModel]) || "—",
    preferredFutureOperatingModel:
      strVal(f[SI_FIELDS.preferredFutureOperatingModel]) ||
      strVal(f[SI_FIELDS.planSelfManage]) ||
      "—",
    operatorStrategyStatus: strVal(f[SI_FIELDS.operatorStrategyStatus]) || "—",
    openingTransitionPhase: strVal(f[DEALS_FIELDS.openingTransitionPhase]) || "—",
    primaryMarketRegion: strVal(f[LOCATION_FIELDS.primaryMarketRegion]) || "—",
    projectType: rawPt || "—",
    projectTypeKind: resolveProjectTypeKind(rawPt),
    stage: strVal(f["Stage of Development"]) || "—",
    operatorInScope: isOperatorInScopeFromFields(f),
  };
}

/**
 * @param {Record<string, unknown>} fields
 * @returns {string[]}
 */
export function buildClarifications(fields) {
  const f = fields || {};
  const items = [];
  const inScope = isOperatorInScopeFromFields(f);
  const kind = resolveProjectTypeKind(f[DEALS_FIELDS.projectType]);

  if (!inScope) {
    items.push(
      "Third-party operator capabilities may be out of scope for this deal based on bid audience and operating model selections."
    );
    return items;
  }

  if (!strVal(f[DEALS_FIELDS.currentOperatingModel])) {
    items.push("Confirm current operating model (branded/managed structure today).");
  }
  if (!strVal(f[SI_FIELDS.preferredFutureOperatingModel])) {
    items.push("Confirm preferred future operating model after the deal.");
  }
  if (!listVal(f[SI_FIELDS.operatorCapabilityPriorities]).length) {
    items.push("Select operator capability priorities that matter for advisor review.");
  }

  if (isOtherToBeConfirmedProjectType(kind)) {
    items.push("Confirm project type — capability themes need validation until Project Type is set.");
  } else if (
    (kind === "conversion_reflag" || kind === "renovation_repositioning") &&
    !strVal(f[DEALS_FIELDS.openingTransitionPhase])
  ) {
    items.push("Specify opening / transition phase for conversion, rebranding, or renovation.");
  }

  if (!strVal(f[LOCATION_FIELDS.primaryMarketRegion])) {
    items.push("Confirm primary market region for operator execution context.");
  }
  if (
    /third.party|brand \+ third/i.test(strVal(f[SI_FIELDS.preferredFutureOperatingModel])) &&
    !strVal(f[SI_FIELDS.ownerReportingFrequency])
  ) {
    items.push("Confirm owner reporting frequency expected from an operator.");
  }

  for (const c of detectOperatingModelConflicts(f)) {
    items.push(`Review operating model consistency: ${c}`);
  }

  if (strVal(f[DEALS_FIELDS.currentOperatingModel]) === "Needs Review") {
    items.push("Current operating model flagged Needs Review — validate before outreach.");
  }
  if (strVal(f[DEALS_FIELDS.openingTransitionPhase]) === "Needs Review") {
    items.push("Opening / Transition Phase flagged Needs Review — validate before outreach.");
  }
  if (strVal(f[SI_FIELDS.preferredFutureOperatingModel]) === "Needs Review") {
    items.push("Preferred future operating model flagged Needs Review — validate before outreach.");
  }

  return items.slice(0, 8);
}

/**
 * @param {Record<string, unknown>} fields
 */
export function buildReportingSummary(fields) {
  const f = fields || {};
  const freq =
    strVal(f[SI_FIELDS.ownerReportingFrequency]) ||
    strVal(f[SI_FIELDS.preferredReportingFrequency]) ||
    "—";
  const pkg = listVal(f[SI_FIELDS.ownerReportingPackage]);
  return {
    ownerReportingFrequency: freq,
    ownerReportingPackage: pkg.length ? pkg.join(", ") : "—",
    legacyPreferredReportingFrequency: strVal(f[SI_FIELDS.preferredReportingFrequency]) || "—",
  };
}
