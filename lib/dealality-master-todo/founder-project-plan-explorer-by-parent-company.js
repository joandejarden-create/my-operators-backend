/**
 * Pilot Delivery — Brand Explorer tasks grouped by parent company.
 * Supersedes chain-scale segment tasks (FPP steps 6–13).
 */
import {
  MAP_MASTER_TODO,
  MASTER_TODO_SOURCE_VALUE,
  mapPriorityForWrite,
} from "./master-todo-field-map.js";
import {
  summarizeChoiceBrandCoverage,
  NON_CHI_ENRICHED_BRANDS,
} from "./founder-project-plan-explorer-coverage-status.js";

export const EXPLORER_WORKSTREAM = "Brand & Operator Explorers";
export const EXPLORER_PHASE = "Pilot Delivery";
export const EXPLORER_PHASE_NUMBER = 7;

/** Segment-based brand tasks superseded by parent-company tasks. */
export const SEGMENT_BRAND_TASK_RECORD_IDS = [
  "recyByGp3IMbKekKw", // 6 Luxury
  "recvgqK2MgcMz2FeE", // 7 Soft Brand
  "recogqpllM7cKkqCG", // 8 Upper-Upscale
  "recdqXb3Zo3q2KtJr", // 9 Resort
  "recsMTzJEXYsXN2hQ", // 10 Extended-Stay
  "recUTjIKhqzLunNgS", // 11 Upscale/Lifestyle
  "recohyfsjEmUASe5E", // 12 Upper-Midscale
  "recyCmdpANFWF1C1B", // 13 Midscale/Economy
];

const DEFERRED_NEXT_ACTION =
  "Superseded — brand work tracked by parent company (steps 6–8: Choice Hotels International, Hilton, IHG). See reports/brand-operator-explorer-coverage-tracker.md.";

/**
 * @typedef {{
 *   seedId: string,
 *   parentCompany: string,
 *   taskName: string,
 *   stepNumber: number,
 *   roadmapSort: number,
 *   durationDays: number,
 *   brands: string[],
 *   successMetric: string,
 * }} ParentCompanyBrandSeed
 */

/** @type {ParentCompanyBrandSeed[]} */
export const PARENT_COMPANY_BRAND_SEEDS = [
  {
    seedId: "explorer-parent-chi",
    parentCompany: "Choice Hotels International",
    taskName: "Complete Brand Explorer profiles — Choice Hotels International",
    stepNumber: 6,
    roadmapSort: 7.06,
    durationDays: 14,
    brands: [], // filled from manifest at runtime
    successMetric:
      "All Choice Hotels International pilot Brand Explorer profiles are L1+ with Blu-parity L2 on priority Radisson-family brands.",
  },
  {
    seedId: "explorer-parent-hilton",
    parentCompany: "Hilton",
    taskName: "Complete Brand Explorer profiles — Hilton",
    stepNumber: 7,
    roadmapSort: 7.065,
    durationDays: 5,
    brands: ["Curio Collection by Hilton"],
    successMetric:
      "Hilton pilot Brand Explorer profiles (Curio Collection) are L2-enriched and demo-ready.",
  },
  {
    seedId: "explorer-parent-ihg",
    parentCompany: "IHG Hotels & Resorts",
    taskName: "Complete Brand Explorer profiles — IHG Hotels & Resorts",
    stepNumber: 8,
    roadmapSort: 7.07,
    durationDays: 5,
    brands: ["Kimpton"],
    successMetric:
      "IHG pilot Brand Explorer profiles (Kimpton) are L2-enriched and demo-ready.",
  },
];

function pct(n) {
  const v = Math.max(0, Math.min(100, Math.round(n)));
  return `${v}%`;
}

function chiTarget() {
  const s = summarizeChoiceBrandCoverage();
  const l2 = s.l2Complete.length;
  const l1 = s.l1Generated.length;
  const needs = s.needsEnrichment.length;
  const total = s.total || 22;
  const score = (l2 * 100 + l1 * 75 + needs * 45) / total;
  const progress = pct(score);
  const status = score >= 70 ? "Needs Review" : "In Progress";
  const nextAction = [
    `CHI portfolio: ${total} brands — L2 complete (${l2}): ${s.l2Complete.join(", ") || "—"}.`,
    `L1 slot-complete (${l1}): ${s.l1Generated.slice(0, 6).join(", ")}${l1 > 6 ? ` +${l1 - 6} more` : ""}.`,
    `L2 queue (${needs}): ${s.needsEnrichment.join(", ") || "—"}.`,
    "Next: Blu-style L2 for Radisson Collection + Radisson Individual (see choice-brand-explorer-completion-runbook P1).",
  ].join(" ");
  return { status, progress, nextAction, summary: s };
}

function singleBrandEnrichedTarget(brandName, parentLabel) {
  const enriched = NON_CHI_ENRICHED_BRANDS.includes(brandName);
  return {
    status: enriched ? "Needs Review" : "In Progress",
    progress: enriched ? "90%" : "25%",
    nextAction: enriched
      ? `${brandName} (${parentLabel}): L2 split fixtures in repo. QA tabs in Brand Explorer UI → mark Completed after Joan sign-off.`
      : `Build L2 split fixtures for ${brandName} (${parentLabel}).`,
  };
}

/**
 * Live status targets for parent-company brand tasks (keyed by seedId).
 */
export function buildParentCompanyBrandTargets() {
  const chi = chiTarget();
  const hilton = singleBrandEnrichedTarget("Curio Collection by Hilton", "Hilton");
  const ihg = singleBrandEnrichedTarget("Kimpton", "IHG");

  return {
    "explorer-parent-chi": {
      ...chi,
      blocker: "None",
    },
    "explorer-parent-hilton": {
      ...hilton,
      blocker: "None",
    },
    "explorer-parent-ihg": {
      ...ihg,
      blocker: "None",
    },
  };
}

function buildChiBrandListDescription() {
  const s = summarizeChoiceBrandCoverage();
  const all = [...s.l2Complete, ...s.l1Generated, ...s.needsEnrichment];
  return all.length ? all.join(", ") : "22 CHI brands (see choice-brand-explorer:manifest)";
}

/**
 * @param {ParentCompanyBrandSeed} seed
 */
export function parentCompanySeedToCreateFields(seed) {
  const F = MAP_MASTER_TODO;
  const brandList =
    seed.parentCompany === "Choice Hotels International"
      ? buildChiBrandListDescription()
      : seed.brands.join(", ");

  const description = [
    "Purpose:",
    `- Complete and verify Brand Explorer profiles for all pilot-relevant brands under ${seed.parentCompany}.`,
    "",
    "Scope:",
    `- Parent company: ${seed.parentCompany}`,
    `- Brands: ${brandList}`,
    "- For each profile: affiliation model, standards, economics, footprint, case studies, pilot relevance.",
    "- Flag unknown fields rather than guessing.",
    "",
    "Completion standard:",
    "- Every listed brand is L1 slot-complete minimum; priority brands at L2 (Radisson Blu parity).",
    "- Profiles usable in pilot demos and owner/advisor conversations.",
  ].join("\n");

  return {
    [F.task]: seed.taskName,
    [F.workstream]: EXPLORER_WORKSTREAM,
    [F.phase]: EXPLORER_PHASE,
    "Phase Number": EXPLORER_PHASE_NUMBER,
    [F.status]: "Not Started",
    [F.priority]: mapPriorityForWrite("P1"),
    [F.owner]: "Joan D.",
    "Sprint / Wave": "Pilot Wave 1",
    [F.source]: MASTER_TODO_SOURCE_VALUE,
    [F.relatedArea]: "Dealality Platform",
    [F.relatedTable]: "Founder Project Plan",
    [F.stepNumber]: seed.stepNumber,
    [F.startDate]: "2026-07-07",
    [F.dueDate]: "2026-07-21",
    [F.description]: description,
    [F.successMetric]: `Seed ID: ${seed.seedId} — ${seed.successMetric}`,
    [F.dependency]: "Master Brand and Operator Explorer completion tracker",
    Deliverables: `Completed ${seed.parentCompany} Brand Explorer checklist and profile updates`,
    [F.blocker]: "None",
  };
}

/**
 * Patch for superseded segment tasks.
 */
export function buildSegmentDeferPatch(existingFields) {
  const F = MAP_MASTER_TODO;
  const patch = {};
  if (existingFields?.[F.status] !== "Deferred") patch[F.status] = "Deferred";
  if (existingFields?.[F.nextAction] !== DEFERRED_NEXT_ACTION) {
    patch[F.nextAction] = DEFERRED_NEXT_ACTION;
  }
  if (existingFields?.[F.stepNumber] != null) {
    patch[F.stepNumber] = null;
  }
  if (existingFields?.[F.blocker] !== "None" && existingFields?.[F.blocker]) {
    patch[F.blocker] = "None";
  }
  return Object.keys(patch).length ? patch : null;
}

/**
 * @param {string} seedId
 * @param {object} existingFields
 * @param {object} target
 */
export function buildParentCompanyUpdatePatch(seedId, existingFields, target) {
  const F = MAP_MASTER_TODO;
  const patch = {};
  if (existingFields?.[F.status] !== target.status) patch[F.status] = target.status;
  if (existingFields?.[F.progress] !== target.progress) patch[F.progress] = target.progress;
  if (existingFields?.[F.nextAction] !== target.nextAction) patch[F.nextAction] = target.nextAction;
  if (target.blocker !== undefined && existingFields?.[F.blocker] !== target.blocker) {
    patch[F.blocker] = target.blocker;
  }
  return Object.keys(patch).length ? patch : null;
}

export function seedIdFromRecord(fields) {
  const metric = String(fields?.[MAP_MASTER_TODO.successMetric] || "");
  const m = metric.match(/Seed ID:\s*(\S+)/);
  return m ? m[1] : "";
}
