/**
 * Chain-scale × parent-company Brand Explorer FPP sync.
 */
import { listChoiceBrandManifest } from "../../scripts/lib/choice-brand-explorer-manifest.mjs";
import {
  MAP_MASTER_TODO,
  MASTER_TODO_SOURCE_VALUE,
  mapPriorityForWrite,
} from "./master-todo-field-map.js";
import {
  CHAIN_SCALE_BRAND_SEGMENTS,
  BRAND_INVENTORY_TO_REPO,
  normalizeBrandKey,
  expandChainScaleParentTasks,
  buildParentTaskScopeDescription,
} from "./founder-project-plan-explorer-brand-inventory.js";
import {
  OPERATOR_EXPLORER_SEGMENTS,
  buildOperatorScopeDescription,
} from "./founder-project-plan-explorer-operator-inventory.js";

export const EXPLORER_WORKSTREAM = "Brand & Operator Explorers";
export const EXPLORER_PHASE = "Pilot Delivery";
export const EXPLORER_PHASE_NUMBER = 7;
export const MASTER_TRACKER_RECORD_ID = "recHls6zriLafoqJT";
export const BRAND_PARENT_TASK_STEP_START = 6;

export const PARENT_COMPANY_TASK_RECORD_IDS = [
  "recj0oEOoDgQfo3v4",
  "recKGqZjWYo4TBsn2",
  "rec1DLuQ0IDTt8cMg",
];

const SUPERSEDED_NEXT_ACTION =
  "Superseded — work tracked by chain-scale × parent-company tasks. See reports/brand-operator-explorer-coverage-tracker.md.";

const NON_CHI_ENRICHED = new Map([
  ["Kimpton", "l2"],
  ["Curio Collection by Hilton", "l2"],
]);

/** @typedef {'l2'|'l1'|'needs-l2'|'missing'} BrandCoverageLevel */

function pct(n) {
  return `${Math.max(0, Math.min(100, Math.round(n)))}%`;
}

function scoreLevel(level) {
  if (level === "l2") return 100;
  if (level === "l1") return 75;
  if (level === "needs-l2") return 45;
  return 0;
}

function buildRepoCoverageIndex() {
  /** @type {Map<string, { profileName: string, level: BrandCoverageLevel }>} */
  const index = new Map();
  for (const row of listChoiceBrandManifest()) {
    const level =
      row.parity === "complete" ? "l2" : row.parity === "needs-enrichment" ? "needs-l2" : "l1";
    const keys = [normalizeBrandKey(row.profileName), normalizeBrandKey(row.airtableName)];
    for (const k of keys) {
      if (k) index.set(k, { profileName: row.profileName, level });
    }
  }
  for (const [name, level] of NON_CHI_ENRICHED) {
    index.set(normalizeBrandKey(name), { profileName: name, level });
  }
  return index;
}

export function resolveBrandCoverage(inventoryBrand, repoIndex) {
  const key = normalizeBrandKey(inventoryBrand);
  const repoKey = BRAND_INVENTORY_TO_REPO[key];
  if (repoKey) {
    const hit = repoIndex.get(normalizeBrandKey(repoKey));
    if (hit) return hit;
  }
  const direct = repoIndex.get(key);
  if (direct) return direct;
  for (const [k, v] of repoIndex) {
    if (k.includes(key) || key.includes(k)) return v;
  }
  return { profileName: null, level: "missing" };
}

/**
 * @param {string[]} brands
 * @param {string} parent
 */
export function scoreParentGroupCoverage(brands, parent) {
  const repoIndex = buildRepoCoverageIndex();
  const rows = brands.map((brand) => {
    const cov = resolveBrandCoverage(brand, repoIndex);
    return { brand, parent, ...cov };
  });

  const total = rows.length;
  const inRepo = rows.filter((r) => r.level !== "missing");
  const l2 = rows.filter((r) => r.level === "l2");
  const l1 = rows.filter((r) => r.level === "l1");
  const needs = rows.filter((r) => r.level === "needs-l2");
  const missing = rows.filter((r) => r.level === "missing");

  const score = rows.reduce((sum, r) => sum + scoreLevel(r.level), 0) / Math.max(total, 1);
  const progress = pct(score);

  let status = "Not Started";
  if (total > 0 && score >= 85 && missing.length === 0) status = "Needs Review";
  else if (score >= 25 || inRepo.length > 0) status = "In Progress";

  const brandStatus = rows
    .map((r) => {
      const tag =
        r.level === "l2" ? "L2" : r.level === "l1" ? "L1" : r.level === "needs-l2" ? "L2 queue" : "not started";
      return `${r.brand} (${tag})`;
    })
    .join("; ");

  const nextAction = [
    `${parent} (${brands.length} brands): ${inRepo.length}/${total} in repo.`,
    `Per brand: ${brandStatus}.`,
    needs.length ? `L2 queue: ${needs.map((r) => r.profileName || r.brand).join(", ")}.` : "",
    "Tracker: reports/brand-operator-explorer-coverage-tracker.md.",
  ]
    .filter(Boolean)
    .join(" ");

  return {
    total,
    inRepo: inRepo.length,
    l2,
    l1,
    needs,
    missing,
    rows,
    progress,
    status,
    nextAction,
    score,
  };
}

export function scoreAllParentTasks() {
  const tasks = expandChainScaleParentTasks(BRAND_PARENT_TASK_STEP_START);
  return tasks.map((task) => ({
    task,
    coverage: scoreParentGroupCoverage(task.brands, task.parent),
  }));
}

export function operatorStepNumber(brandParentTaskCount) {
  return BRAND_PARENT_TASK_STEP_START + brandParentTaskCount;
}

/**
 * @param {import('./founder-project-plan-explorer-brand-inventory.js').ChainScaleParentTask} task
 * @param {ReturnType<typeof scoreParentGroupCoverage>} coverage
 */
export function buildParentTaskPatch(task, coverage) {
  const F = MAP_MASTER_TODO;
  return {
    [F.task]: task.taskName,
    [F.stepNumber]: task.stepNumber,
    [F.status]: coverage.status,
    [F.progress]: coverage.progress,
    [F.nextAction]: coverage.nextAction,
    [F.description]: buildParentTaskScopeDescription(task),
    [F.successMetric]: `Seed ID: ${task.seedId} — ${coverage.inRepo}/${coverage.total} brands in repo`,
    Deliverables: task.deliverables,
    [F.blocker]: "None",
  };
}

export function buildParentTaskCreateFields(task, coverage) {
  const F = MAP_MASTER_TODO;
  return {
    ...buildParentTaskPatch(task, coverage),
    [F.workstream]: EXPLORER_WORKSTREAM,
    [F.phase]: EXPLORER_PHASE,
    "Phase Number": EXPLORER_PHASE_NUMBER,
    [F.priority]: mapPriorityForWrite("P1"),
    [F.owner]: "Joan D.",
    "Sprint / Wave": "Pilot Wave 1",
    [F.source]: MASTER_TODO_SOURCE_VALUE,
    [F.relatedArea]: "Dealality Platform",
    [F.relatedTable]: "Founder Project Plan",
    [F.startDate]: "2026-07-07",
    [F.dueDate]: "2026-07-28",
    [F.dependency]: "Master Brand and Operator Explorer completion tracker",
  };
}

export function scoreOperatorSegment(segment) {
  const total = segment.operators.length;
  return {
    total,
    progress: "15%",
    status: "In Progress",
    nextAction: [
      `Operator scope: ${total} operators listed.`,
      `Priority batch: ${segment.operators.slice(0, 4).join(", ")}${total > 4 ? "…" : ""}.`,
      "Hydrate Operator Setup → Explorer profiles; track each operator individually in master tracker.",
    ].join(" "),
  };
}

export function buildOperatorSegmentPatch(segment, stepNumber) {
  const cov = scoreOperatorSegment(segment);
  const F = MAP_MASTER_TODO;
  return {
    [F.task]: segment.taskName,
    [F.stepNumber]: stepNumber,
    [F.status]: cov.status,
    [F.progress]: cov.progress,
    [F.nextAction]: cov.nextAction,
    [F.description]: buildOperatorScopeDescription(segment),
    [F.successMetric]: `Seed ID: ${segment.seedId} — ${cov.total} operators in scope`,
    Deliverables: segment.deliverables,
    [F.blocker]: "None",
  };
}

export function buildSupersededDeferPatch(existingFields) {
  const F = MAP_MASTER_TODO;
  const patch = {};
  if (existingFields?.[F.status] !== "Deferred") patch[F.status] = "Deferred";
  if (existingFields?.[F.nextAction] !== SUPERSEDED_NEXT_ACTION) {
    patch[F.nextAction] = SUPERSEDED_NEXT_ACTION;
  }
  if (existingFields?.[F.stepNumber] != null) patch[F.stepNumber] = null;
  return Object.keys(patch).length ? patch : null;
}

/** Aggregate chain-scale tasks (8 rows) superseded by 60 parent×scale rows. */
export function isAggregateChainScaleRecord(fields) {
  const metric = String(fields?.[MAP_MASTER_TODO.successMetric] || "");
  const seedMatch = metric.match(/Seed ID:\s*(\S+)/);
  if (seedMatch) {
    const seed = seedMatch[1];
    if (/^explorer-scale-\d{2}-/.test(seed) && !seed.includes("--")) return true;
  }
  const task = String(fields?.Task || "");
  if (task.startsWith("Complete Brand Explorer profiles —") && task.endsWith(" brands")) {
    return task.split(" — ").length === 2;
  }
  return false;
}

export function seedIdFromFields(fields) {
  const metric = String(fields?.[MAP_MASTER_TODO.successMetric] || "");
  const m = metric.match(/Seed ID:\s*(\S+)/);
  return m ? m[1] : "";
}

export function buildMasterTrackerPatch(scoredParents) {
  const uniqueBrands = new Set();
  let inRepoCount = 0;
  let l2Count = 0;
  for (const { task, coverage } of scoredParents) {
    for (const row of coverage.rows) {
      const key = normalizeBrandKey(row.brand);
      if (uniqueBrands.has(key)) continue;
      uniqueBrands.add(key);
      if (row.level !== "missing") inRepoCount += 1;
      if (row.level === "l2") l2Count += 1;
    }
  }
  const totalUnique = uniqueBrands.size;

  return {
    Status: "Needs Review",
    Progress: pct((inRepoCount / Math.max(totalUnique, 1)) * 100),
    "Next Action": [
      `Master tracker: ${inRepoCount}/${totalUnique} unique taxonomy brands in repo (${l2Count} L2).`,
      `${scoredParents.length} chain-scale × parent-company brand tasks (steps ${BRAND_PARENT_TASK_STEP_START}–${BRAND_PARENT_TASK_STEP_START + scoredParents.length - 1}) + 4 operator tasks.`,
      "Report: reports/brand-operator-explorer-coverage-tracker.md.",
      "Joan: sign off per-brand checklist → Completed.",
    ].join(" "),
  };
}

export function renderParentTrackerMarkdown(scoredParents) {
  const byScale = new Map();
  for (const entry of scoredParents) {
    const key = entry.task.scaleLabel;
    if (!byScale.has(key)) byScale.set(key, []);
    byScale.get(key).push(entry);
  }

  const lines = [
    "## Chain-scale × parent-company brand tasks",
    "",
    `**${scoredParents.length} rows** - one FPP task per parent company within each chain-scale segment.`,
    "",
  ];

  for (const [scaleLabel, entries] of byScale) {
    lines.push(`### ${scaleLabel}`);
    lines.push("");
    lines.push("| Step | Parent | Brands | In repo | L2 | Progress | Status |");
    lines.push("|------|--------|--------|---------|-----|----------|--------|");
    for (const { task, coverage } of entries) {
      lines.push(
        `| ${task.stepNumber} | ${task.parent} | ${coverage.total} | ${coverage.inRepo} | ${coverage.l2.length} | ${coverage.progress} | ${coverage.status} |`
      );
    }
    lines.push("");
  }

  return lines.join("\n");
}

export { CHAIN_SCALE_BRAND_SEGMENTS, OPERATOR_EXPLORER_SEGMENTS, expandChainScaleParentTasks };
