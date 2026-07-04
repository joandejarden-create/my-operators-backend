/**
 * Phase 5B — Proposed operator-alignment deal field values (SI + Deals).
 * Used by scripts/backfill-deal-operator-alignment-fields.mjs
 */

import fs from "fs";
import path from "path";
import { validateWriteValue } from "./operator-alignment-airtable-option-normalize.js";
import {
  OAS_DEAL_SI_FIELD_NAMES as SI,
  OAS_DEAL_DEALS_FIELD_NAMES as DEALS,
  OAS_OPERATOR_SERVICE_OPTIONS,
  OAS_OPERATOR_REVIEW_STATUS_OPTIONS,
  OAS_PREFERRED_MANAGEMENT_STRUCTURE_OPTIONS,
  OAS_MARKET_PRESENCE_REQUIREMENT_OPTIONS,
  OAS_PREOPENING_SUPPORT_NEEDED_OPTIONS,
  OAS_OWNER_REPORTING_EXPECTATIONS_OPTIONS,
  OAS_BRAND_OPERATOR_SPLIT_OPTIONS,
  OAS_OWNER_CONTROL_PREFERENCE_OPTIONS,
  OAS_COMMERCIAL_PRIORITY_OPTIONS,
  OAS_YES_NO_NA_OPTIONS,
  OAS_OWNER_INTERNAL_OPS_OPTIONS,
  OAS_OPENING_TIMELINE_OPTIONS,
  OAS_BRAND_AGREEMENT_STRUCTURE_OPTIONS,
  OAS_DEAL_OPERATING_MODEL_OPTIONS,
  OAS_OPERATOR_SCOPE_OPTIONS,
  OAS_FB_CAPABILITY_OPTIONS,
} from "./operator-alignment-field-options.js";

/** @typedef {{ value: unknown, source: string, confidence: string, note?: string }} FieldProposal */
/** @typedef {{ dealId?: string, slug?: string, projectName?: string, skip?: boolean, skipReason?: string, fields: Record<string, FieldProposal>, notes?: string[] }} DealBackfillPlan */

export const OAS_SI_BACKFILL_COLUMNS = Object.values(SI);
export const OAS_DEALS_BACKFILL_COLUMNS = Object.values(DEALS);

const SERVICE = {
  fullMgmt: "Full hotel management",
  preOpenPlan: "Pre-opening planning",
  openTrans: "Opening / transition support",
  rm: "Revenue management",
  sales: "Sales",
  dist: "Distribution / channel management",
  digital: "Digital marketing",
  acct: "Accounting / finance",
  hr: "HR / staffing",
  proc: "Procurement",
  fb: "F&B operations",
  brandComp: "Brand compliance support",
  ownerRep: "Owner reporting",
  assetMgmt: "Asset management support",
  tech: "Technical services coordination",
};

function norm(s) {
  return String(s || "").trim().toLowerCase();
}

function list(v) {
  if (v == null) return [];
  if (Array.isArray(v)) return v.map((x) => String(x).trim()).filter(Boolean);
  const s = String(v).trim();
  return s ? [s] : [];
}

function hasToken(hay, ...tokens) {
  const h = norm(hay);
  return tokens.some((t) => h.includes(norm(t)));
}

export function isOperatorPathInScope(merged) {
  const bids = merged["Who should receive bids for this project?"] || "";
  const future = merged["Preferred Future Operating Model"] || "";
  const plan = merged["Plan to Self-Manage or Hire Third Party?"] || "";
  if (hasToken(bids, "third-party operators only", "both brands and third")) return true;
  if (hasToken(future, "third-party", "third party")) return true;
  if (hasToken(plan, "third-party", "third party")) return false;
  if (hasToken(bids, "hotel brands only")) return false;
  return hasToken(future, "management");
}

function inferBrandAgreementStructure(merged) {
  const pds = list(merged["Preferred Deal Structure"]).join(" ") || String(merged["Preferred Deal Structure"] || "");
  if (hasToken(pds, "franchise")) return { value: "Franchise", source: "inferred", confidence: "High", note: "From Preferred Deal Structure (MP)" };
  if (hasToken(pds, "brand-managed", "brand managed")) return { value: "Brand-managed", source: "inferred", confidence: "High" };
  if (hasToken(pds, "management")) return { value: "Management", source: "inferred", confidence: "Medium" };
  if (hasToken(merged["Soft vs Hard Brand Preference"], "soft")) return { value: "Soft brand / collection affiliation", source: "inferred", confidence: "Medium" };
  return { value: "Undecided", source: "inferred", confidence: "Low" };
}

function inferOperatingModel(merged) {
  const future = merged["Preferred Future Operating Model"] || "";
  const current = merged["Current Operating Model"] || "";
  if (hasToken(future, "third-party", "third party")) return { value: "Third-party managed", source: "inferred", confidence: "High", note: "From Preferred Future Operating Model" };
  if (hasToken(future, "owner-operated", "self-manage")) return { value: "Owner-operated", source: "inferred", confidence: "High" };
  if (hasToken(future, "brand-managed")) return { value: "Brand-managed", source: "inferred", confidence: "High" };
  if (hasToken(current, "third-party")) return { value: "Third-party managed", source: "inferred", confidence: "Medium", note: "From Current Operating Model" };
  return { value: "Undecided", source: "inferred", confidence: "Low" };
}

function inferPreferredManagementStructure(merged) {
  const out = [];
  const future = merged["Preferred Future Operating Model"] || "";
  const pds = list(merged["Preferred Deal Structure"]).join(" ") || String(merged["Preferred Deal Structure"] || "");
  if (hasToken(future, "third-party", "third party")) out.push("Full third-party management");
  if (hasToken(pds, "franchise") && out.length) out.push("Franchise with third-party operator");
  else if (hasToken(pds, "franchise")) out.push("Franchise with third-party operator");
  if (hasToken(pds, "brand-managed")) out.push("Brand-managed");
  if (hasToken(future, "commercial")) out.push("Commercial-only support");
  if (!out.length) out.push("Undecided");
  return {
    value: [...new Set(out)],
    source: "inferred",
    confidence: out.includes("Undecided") ? "Low" : "High",
    note: "Resolves franchise vs third-party dimension split",
  };
}

function inferOperatorReviewStatus(merged, demoPrimary = false) {
  const st = merged["Operator Strategy Status"] || "";
  if (demoPrimary || hasToken(st, "ready for structured", "shortlist")) {
    return { value: "Ready for operator shortlist", source: "inferred", confidence: "High", note: "Demo/OAS + Operator Strategy Status" };
  }
  if (hasToken(st, "exploring")) return { value: "Exploring operator options", source: "inferred", confidence: "High" };
  if (hasToken(st, "not seeking")) return { value: "Not applicable", source: "existing", confidence: "High" };
  return { value: "Operator review in scope", source: "inferred", confidence: "Medium" };
}

function baseRequiredServices(merged) {
  const req = [
    SERVICE.fullMgmt,
    SERVICE.preOpenPlan,
    SERVICE.openTrans,
    SERVICE.rm,
    SERVICE.sales,
    SERVICE.dist,
    SERVICE.brandComp,
    SERVICE.ownerRep,
    SERVICE.hr,
    SERVICE.proc,
    SERVICE.tech,
  ];
  const pt = merged["Project Type"] || "";
  if (hasToken(pt, "conversion", "reflag", "reposition", "renovation")) {
    return req.filter((s) => s !== SERVICE.preOpenPlan || true);
  }
  return req;
}

function inferMustHaveServices(merged) {
  const must = [
    SERVICE.fullMgmt,
    SERVICE.preOpenPlan,
    SERVICE.rm,
    SERVICE.sales,
    SERVICE.brandComp,
    SERVICE.ownerRep,
  ];
  const legacy = list(merged["Must-haves From Brand or Operator"] || merged["Must-Haves From Brand/Operator"]);
  if (legacy.some((x) => hasToken(x, "distribution", "marketing"))) {
    if (!must.includes(SERVICE.dist)) must.push(SERVICE.dist);
  }
  return { value: must, source: "inferred", confidence: "High", note: "Fixture priorities + legacy must-haves mapping" };
}

function inferNiceToHave() {
  return {
    value: [SERVICE.proc, SERVICE.digital, SERVICE.tech, SERVICE.fb],
    source: "manual_sample_assumption",
    confidence: "Medium",
    note: "CALA demo default nice-to-haves",
  };
}

function inferMarketPresence(merged) {
  const country = merged.Country || "";
  if (norm(country) === "mexico" || hasToken(merged["Primary Market Region"], "cala")) {
    return { value: "Active country operations required", source: "inferred", confidence: "High", note: "Mexico/CALA deal" };
  }
  return { value: "Regional experience acceptable", source: "inferred", confidence: "Medium" };
}

function inferPreOpening(merged) {
  const pt = merged["Project Type"] || "";
  const stage = merged["Stage of Development"] || "";
  const phase = merged["Opening / Transition Phase"] || "";
  if (hasToken(pt, "new build") || hasToken(stage, "land under", "under construction", "planning")) {
    return { value: "Yes", source: "inferred", confidence: "High" };
  }
  if (hasToken(phase, "planning", "construction", "pre-opening", "transition")) {
    return { value: "Yes", source: "inferred", confidence: "Medium" };
  }
  return { value: "Not applicable", source: "inferred", confidence: "Medium" };
}

function inferOwnerReporting(merged) {
  const freq = merged["Owner Reporting Frequency"] || merged["Preferred Reporting Frequency"] || "";
  if (hasToken(freq, "weekly")) return { value: "Basic owner reporting", source: "existing", confidence: "High" };
  return { value: "Monthly operating review", source: "existing", confidence: "High", note: "Owner Reporting Frequency Monthly" };
}

function inferBrandOperatorSplit(merged) {
  const om = inferOperatingModel(merged).value;
  const ba = inferBrandAgreementStructure(merged).value;
  if (om === "Third-party managed" && (ba === "Franchise" || hasToken(ba, "franchise"))) {
    return { value: "Brand standards with third-party operator", source: "inferred", confidence: "High" };
  }
  if (om === "Owner-operated") return { value: "Owner-operated with brand support", source: "inferred", confidence: "Medium" };
  return { value: "Brand standards with third-party operator", source: "inferred", confidence: "Medium" };
}

function inferOwnerControl(merged) {
  const inv = list(merged["Owner Control Priorities"]).join(" ");
  if (hasToken(inv, "budget", "capex")) return { value: "Shared control", source: "inferred", confidence: "High", note: "Owner Control Priorities" };
  return { value: "Shared control", source: "manual_sample_assumption", confidence: "Medium" };
}

function inferFbComplexity(merged) {
  const fb = merged["F&B Outlets?"] || "";
  const prog = list(merged["F&B Program Type"]).join(" ");
  const sm = merged["Hotel Service Model"] || "";
  if (fb === "No" || hasToken(sm, "rooms-only")) return { value: "None / rooms-only", source: "existing", confidence: "High" };
  if (hasToken(prog, "lifestyle", "experiential", "significant")) return { value: "Significant F&B", source: "inferred", confidence: "Medium" };
  if (hasToken(sm, "full-service", "resort")) return { value: "Moderate F&B", source: "inferred", confidence: "Medium" };
  return { value: "Limited F&B", source: "inferred", confidence: "High", note: "Select-service / limited outlets" };
}

function inferCommercialPriority(merged) {
  const out = [];
  if (Number(merged["Revenue / Yield Management Importance"]) >= 3) out.push("Revenue management");
  if (Number(merged["Marketing & Distribution Importance"]) >= 3) {
    out.push("Sales");
    out.push("Distribution");
  }
  if (Number(merged["Loyalty Program Importance"]) >= 3) out.push("Loyalty / brand channels");
  if (!out.length) out.push("None specified");
  return { value: [...new Set(out)], source: "inferred", confidence: "High", note: "SI importance fields" };
}

function inferLaborProcurement(merged) {
  const pri = list(merged["Operator Capability Priorities"]).join(" ");
  const yes = hasToken(pri, "hr", "staffing", "procurement") ? "Yes" : "Unknown";
  return {
    labor: { value: yes, source: "inferred", confidence: "Medium" },
    proc: { value: hasToken(pri, "procurement") ? "Yes" : yes, source: "inferred", confidence: "Medium" },
  };
}

function inferOwnerInternalOps(merged) {
  const cur = merged["Current Operating Model"] || "";
  if (hasToken(cur, "owner-operated", "unbranded")) {
    return { value: "Limited internal capability", source: "inferred", confidence: "High", note: "Owner-operated today → third-party path" };
  }
  return { value: "Partial internal capability", source: "inferred", confidence: "Medium" };
}

function inferOpeningTimeline(merged) {
  const stage = merged["Stage of Development"] || "";
  if (hasToken(stage, "land under", "pre-development", "entitlement")) return { value: "Pre-development", source: "existing", confidence: "High" };
  if (hasToken(stage, "under construction")) return { value: "Under construction", source: "existing", confidence: "High" };
  const open = merged["Expected Opening or Rebranding Date"] || "";
  if (open) return { value: "12–24 months", source: "inferred", confidence: "Medium", note: "From Expected Opening date" };
  return { value: "Unknown", source: "inferred", confidence: "Low" };
}

function inferOperatorScope() {
  return {
    value: [
      "Full management",
      "Pre-opening support",
      "Brand compliance support",
      "Owner reporting",
      "Technical services coordination",
    ],
    source: "inferred",
    confidence: "High",
  };
}

/** Explicit Aeropuerto Cancún plan (fixture + user spec). */
export function planAeropuertoCancun() {
  return {
    slug: "aeropuerto-cancun-select-service",
    dealId: "recIeGRZP21udmTnt",
    projectName: "Aeropuerto Cancún Select-Service Hotel",
    fields: {
      [SI.operatorReviewStatus]: { value: "Ready for operator shortlist", source: "manual_sample_assumption", confidence: "High", note: "OAS demo primary" },
      [SI.preferredManagementStructure]: {
        value: ["Franchise with third-party operator", "Full third-party management"],
        source: "manual_sample_assumption",
        confidence: "High",
        note: "Resolves MP Franchise Only vs third-party path",
      },
      [SI.requiredOperatorServices]: {
        value: baseRequiredServices({ "Project Type": "New Build" }),
        source: "manual_sample_assumption",
        confidence: "High",
      },
      [SI.mustHaveOperatorServices]: {
        value: [
          SERVICE.fullMgmt,
          SERVICE.preOpenPlan,
          SERVICE.rm,
          SERVICE.sales,
          SERVICE.brandComp,
          SERVICE.ownerRep,
        ],
        source: "manual_sample_assumption",
        confidence: "High",
      },
      [SI.niceToHaveOperatorServices]: inferNiceToHave(),
      [SI.marketPresenceRequirement]: { value: "Active country operations required", source: "manual_sample_assumption", confidence: "High" },
      [SI.preOpeningSupportNeeded]: { value: "Yes", source: "fixture", confidence: "High" },
      [SI.ownerReportingExpectations]: { value: "Monthly operating review", source: "existing", confidence: "High", note: "Owner Reporting Frequency Monthly" },
      [SI.brandOperatorResponsibilitySplit]: { value: "Brand standards with third-party operator", source: "manual_sample_assumption", confidence: "High" },
      [SI.ownerControlPreference]: { value: "Shared control", source: "manual_sample_assumption", confidence: "High" },
      [DEALS.fbComplexity]: { value: "Limited F&B", source: "inferred", confidence: "High", note: "Select-service with outlets" },
      [SI.commercialPriority]: {
        value: ["Revenue management", "Sales", "Distribution", "Digital marketing", "Loyalty / brand channels"],
        source: "manual_sample_assumption",
        confidence: "High",
      },
      [SI.localLaborHrSupportNeeded]: { value: "Yes", source: "inferred", confidence: "Medium" },
      [SI.procurementSupportNeeded]: { value: "Yes", source: "inferred", confidence: "Medium" },
      [SI.ownerInternalOpsCapability]: { value: "Limited internal capability", source: "inferred", confidence: "High" },
      [DEALS.openingTimeline]: { value: "Pre-development", source: "existing", confidence: "High", note: "Stage Land Under Control Only" },
      [SI.brandAgreementStructure]: { value: "Franchise", source: "inferred", confidence: "High", note: "Preferred Deal Structure Franchise Only (legacy MP)" },
      [SI.dealOperatingModel]: { value: "Third-party managed", source: "existing", confidence: "High", note: "Preferred Future Operating Model" },
      [SI.operatorScope]: {
        value: ["Full management", "Pre-opening support", "Brand compliance support", "Owner reporting", "Technical services coordination"],
        source: "manual_sample_assumption",
        confidence: "High",
      },
    },
    notes: [
      "Legacy MP Preferred Deal Structure left as Franchise Only (not overwritten).",
      "Franchise (brand) and Third-party managed (operations) are separate dimensions.",
    ],
  };
}

/**
 * Build plan from merged fixture/deal fields.
 * @param {object} merged
 * @param {{ slug?: string, dealId?: string, projectName?: string, demoPrimary?: boolean }} meta
 * @returns {DealBackfillPlan}
 */
export function buildBackfillPlanFromMerged(merged, meta = {}) {
  if (!isOperatorPathInScope(merged)) {
    return {
      slug: meta.slug,
      dealId: meta.dealId,
      projectName: meta.projectName,
      skip: true,
      skipReason: "Operator path not in scope (bids/future model)",
      fields: {},
    };
  }
  const laborProc = inferLaborProcurement(merged);
  return {
    slug: meta.slug,
    dealId: meta.dealId,
    projectName: meta.projectName || merged["Property Name"] || merged["Project Name"],
    fields: {
      [SI.operatorReviewStatus]: inferOperatorReviewStatus(merged, meta.demoPrimary),
      [SI.preferredManagementStructure]: inferPreferredManagementStructure(merged),
      [SI.requiredOperatorServices]: { value: baseRequiredServices(merged), source: "inferred", confidence: "Medium" },
      [SI.mustHaveOperatorServices]: inferMustHaveServices(merged),
      [SI.niceToHaveOperatorServices]: inferNiceToHave(),
      [SI.marketPresenceRequirement]: inferMarketPresence(merged),
      [SI.preOpeningSupportNeeded]: inferPreOpening(merged),
      [SI.ownerReportingExpectations]: inferOwnerReporting(merged),
      [SI.brandOperatorResponsibilitySplit]: inferBrandOperatorSplit(merged),
      [SI.ownerControlPreference]: inferOwnerControl(merged),
      [DEALS.fbComplexity]: inferFbComplexity(merged),
      [SI.commercialPriority]: inferCommercialPriority(merged),
      [SI.localLaborHrSupportNeeded]: laborProc.labor,
      [SI.procurementSupportNeeded]: laborProc.proc,
      [SI.ownerInternalOpsCapability]: inferOwnerInternalOps(merged),
      [DEALS.openingTimeline]: inferOpeningTimeline(merged),
      [SI.brandAgreementStructure]: inferBrandAgreementStructure(merged),
      [SI.dealOperatingModel]: inferOperatingModel(merged),
      [SI.operatorScope]: inferOperatorScope(),
    },
  };
}

export function loadFixtureBySlug(slug, repoRoot) {
  const p = path.join(repoRoot, "fixtures", "sample-deals", `${slug}.example.json`);
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

export function mergeFixtureFields(fixture) {
  const ref = fixture?.referenceProperty?.fields || {};
  const fic = fixture?.fictionalDeal?.fields || {};
  return { ...ref, ...fic };
}

/** @param {string} repoRoot @returns {Map<string, DealBackfillPlan>} */
export function loadAllSamplePlans(repoRoot) {
  const resultsPath = path.join(repoRoot, "data", "cala-sample-import-results.json");
  const { deals } = JSON.parse(fs.readFileSync(resultsPath, "utf8"));
  const map = new Map();

  map.set("recIeGRZP21udmTnt", planAeropuertoCancun());

  for (const row of deals || []) {
    if (row.dealId === "recIeGRZP21udmTnt") continue;
    try {
      const fixture = loadFixtureBySlug(row.slug, repoRoot);
      const merged = mergeFixtureFields(fixture);
      const plan = buildBackfillPlanFromMerged(merged, {
        slug: row.slug,
        dealId: row.dealId,
        projectName: row.projectName,
        demoPrimary: false,
      });
      map.set(row.dealId, plan);
    } catch (e) {
      map.set(row.dealId, {
        dealId: row.dealId,
        slug: row.slug,
        skip: true,
        skipReason: "Fixture load failed: " + e.message,
        fields: {},
      });
    }
  }
  return map;
}

/** Airtable column title → registry tableKey */
export const FIELD_TO_TABLE_KEY = {
  [SI.operatorReviewStatus]: "si",
  [SI.preferredManagementStructure]: "si",
  [SI.requiredOperatorServices]: "si",
  [SI.mustHaveOperatorServices]: "si",
  [SI.niceToHaveOperatorServices]: "si",
  [SI.marketPresenceRequirement]: "si",
  [SI.preOpeningSupportNeeded]: "si",
  [SI.ownerReportingExpectations]: "si",
  [SI.brandOperatorResponsibilitySplit]: "si",
  [SI.ownerControlPreference]: "si",
  [SI.commercialPriority]: "si",
  [SI.localLaborHrSupportNeeded]: "si",
  [SI.procurementSupportNeeded]: "si",
  [SI.ownerInternalOpsCapability]: "si",
  [SI.brandAgreementStructure]: "si",
  [SI.dealOperatingModel]: "si",
  [SI.operatorScope]: "si",
  [DEALS.fbComplexity]: "deals",
  [DEALS.openingTimeline]: "deals",
};

/**
 * Validate against live Airtable options when liveIndex provided; else planned OPTION_SETS.
 * @param {string} fieldName
 * @param {unknown} value
 * @param {object|null} [liveIndex]
 */
export function validateProposalValue(fieldName, value, liveIndex = null) {
  if (liveIndex) {
    const tableKey = FIELD_TO_TABLE_KEY[fieldName];
    if (!tableKey) return { ok: true, source: "unmapped" };
    const v = validateWriteValue(liveIndex, tableKey, fieldName, value);
    return {
      ok: v.ok,
      bad: v.warnings || (v.ok ? [] : [value]),
      normalized: v.value,
      source: "live_airtable",
    };
  }
  const allowed = OPTION_SETS[fieldName];
  if (!allowed) return { ok: true, source: "unmapped" };
  const check = (v) => allowed.includes(v);
  if (Array.isArray(value)) {
    const bad = value.filter((v) => !check(v));
    return bad.length ? { ok: false, bad, source: "planned" } : { ok: true, source: "planned" };
  }
  return check(value) ? { ok: true, source: "planned" } : { ok: false, bad: [value], source: "planned" };
}

/** Normalize proposal values to exact live Airtable labels for PATCH. */
export function normalizeProposalForWrite(fieldName, value, liveIndex) {
  if (!liveIndex) return value;
  const v = validateProposalValue(fieldName, value, liveIndex);
  if (v.normalized !== undefined) return v.normalized;
  return value;
}

export const OPTION_SETS = {
  // Planned reference; live Airtable is authoritative when liveIndex passed to validateProposalValue
  [SI.operatorReviewStatus]: OAS_OPERATOR_REVIEW_STATUS_OPTIONS,
  [SI.preferredManagementStructure]: OAS_PREFERRED_MANAGEMENT_STRUCTURE_OPTIONS,
  [SI.requiredOperatorServices]: OAS_OPERATOR_SERVICE_OPTIONS,
  [SI.mustHaveOperatorServices]: OAS_OPERATOR_SERVICE_OPTIONS,
  [SI.niceToHaveOperatorServices]: OAS_OPERATOR_SERVICE_OPTIONS,
  [SI.marketPresenceRequirement]: OAS_MARKET_PRESENCE_REQUIREMENT_OPTIONS,
  [SI.preOpeningSupportNeeded]: OAS_PREOPENING_SUPPORT_NEEDED_OPTIONS,
  [SI.ownerReportingExpectations]: OAS_OWNER_REPORTING_EXPECTATIONS_OPTIONS,
  [SI.brandOperatorResponsibilitySplit]: OAS_BRAND_OPERATOR_SPLIT_OPTIONS,
  [SI.ownerControlPreference]: OAS_OWNER_CONTROL_PREFERENCE_OPTIONS,
  [DEALS.fbComplexity]: OAS_FB_CAPABILITY_OPTIONS,
  [SI.commercialPriority]: OAS_COMMERCIAL_PRIORITY_OPTIONS,
  [SI.localLaborHrSupportNeeded]: OAS_YES_NO_NA_OPTIONS,
  [SI.procurementSupportNeeded]: OAS_YES_NO_NA_OPTIONS,
  [SI.ownerInternalOpsCapability]: OAS_OWNER_INTERNAL_OPS_OPTIONS,
  [DEALS.openingTimeline]: OAS_OPENING_TIMELINE_OPTIONS,
  [SI.brandAgreementStructure]: OAS_BRAND_AGREEMENT_STRUCTURE_OPTIONS,
  [SI.dealOperatingModel]: OAS_DEAL_OPERATING_MODEL_OPTIONS,
  [SI.operatorScope]: OAS_OPERATOR_SCOPE_OPTIONS,
};
