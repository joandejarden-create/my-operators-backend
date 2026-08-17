/**
 * Chain-scale Brand Explorer FPP sync — coverage scoring + Airtable patch builders.
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
  flattenSegmentBrands,
  buildSegmentScopeDescription,
} from "./founder-project-plan-explorer-brand-inventory.js";
import {
  OPERATOR_EXPLORER_SEGMENTS,
  buildOperatorScopeDescription,
} from "./founder-project-plan-explorer-operator-inventory.js";

export const EXPLORER_WORKSTREAM = "Brand & Operator Explorers";
export const EXPLORER_PHASE = "Pilot Delivery";
export const EXPLORER_PHASE_NUMBER = 7;
export const MASTER_TRACKER_RECORD_ID = "recHls6zriLafoqJT";

/** Parent-company tasks superseded by chain-scale taxonomy. */
export const PARENT_COMPANY_TASK_RECORD_IDS = [
  "recj0oEOoDgQfo3v4",
  "recKGqZjWYo4TBsn2",
  "rec1DLuQ0IDTt8cMg",
];

const PARENT_DEFERRED_NEXT_ACTION =
  "Superseded — brand work tracked by chain-scale segment tasks (steps 6–13). See reports/brand-operator-explorer-coverage-tracker.md.";

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

/**
 * @param {string} inventoryBrand
 * @param {Map<string, { profileName: string, level: BrandCoverageLevel }>} repoIndex
 * @returns {{ profileName: string|null, level: BrandCoverageLevel }}
 */
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
 * @param {import('./founder-project-plan-explorer-brand-inventory.js').ChainScaleSegment} segment
 */
export function scoreSegmentCoverage(segment) {
  const repoIndex = buildRepoCoverageIndex();
  const brands = flattenSegmentBrands(segment);
  const rows = brands.map((b) => {
    const cov = resolveBrandCoverage(b.brand, repoIndex);
    return { ...b, ...cov };
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
  if (score >= 85 && missing.length === 0) status = "Needs Review";
  else if (score >= 25 || inRepo.length > 0) status = "In Progress";
  else status = "Not Started";

  const inRepoSummary = inRepo
    .slice(0, 8)
    .map((r) => `${r.brand} (${r.level === "l2" ? "L2" : r.level === "l1" ? "L1" : "L2 queue"})`)
    .join(", ");

  const missingSample = missing
    .slice(0, 6)
    .map((r) => r.brand)
    .join(", ");

  const nextAction = [
    `Brand coverage: ${inRepo.length}/${total} in repo (${l2.length} L2, ${l1.length} L1, ${needs.length} L2 queue, ${missing.length} not started).`,
    inRepo.length ? `In repo: ${inRepoSummary}${inRepo.length > 8 ? "…" : ""}.` : "",
    needs.length
      ? `L2 queue: ${needs.map((r) => r.profileName || r.brand).join(", ")}.`
      : "",
    missing.length
      ? `Not started (${missing.length}): ${missingSample}${missing.length > 6 ? "…" : ""}.`
      : "",
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

export function scoreAllBrandSegments() {
  return CHAIN_SCALE_BRAND_SEGMENTS.map((seg) => ({
    segment: seg,
    coverage: scoreSegmentCoverage(seg),
  }));
}

/**
 * @param {import('./founder-project-plan-explorer-brand-inventory.js').ChainScaleSegment} segment
 * @param {ReturnType<typeof scoreSegmentCoverage>} coverage
 */
export function buildBrandSegmentPatch(segment, coverage) {
  const F = MAP_MASTER_TODO;
  return {
    [F.task]: segment.taskName,
    [F.stepNumber]: segment.stepNumber,
    [F.status]: coverage.status,
    [F.progress]: coverage.progress,
    [F.nextAction]: coverage.nextAction,
    [F.description]: buildSegmentScopeDescription(segment),
    [F.successMetric]: `Seed ID: ${segment.seedId} — ${coverage.inRepo}/${coverage.total} brands in repo`,
    Deliverables: segment.deliverables,
    [F.blocker]: "None",
  };
}

export function buildBrandSegmentCreateFields(segment, coverage) {
  const F = MAP_MASTER_TODO;
  return {
    ...buildBrandSegmentPatch(segment, coverage),
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

/**
 * Operator segments — scope in description; progress from placeholder until operator manifest exists.
 */
export function scoreOperatorSegment(segment) {
  const total = segment.operators.length;
  const progress = "15%";
  const status = "In Progress";
  const nextAction = [
    `Operator scope: ${total} operators listed.`,
    `Priority batch: ${segment.operators.slice(0, 4).join(", ")}${total > 4 ? "…" : ""}.`,
    "Hydrate Operator Setup → Explorer profiles; track each operator individually in master tracker.",
  ].join(" ");
  return { total, progress, status, nextAction };
}

export function buildOperatorSegmentPatch(segment) {
  const cov = scoreOperatorSegment(segment);
  const F = MAP_MASTER_TODO;
  return {
    [F.task]: segment.taskName,
    [F.stepNumber]: segment.stepNumber,
    [F.status]: cov.status,
    [F.progress]: cov.progress,
    [F.nextAction]: cov.nextAction,
    [F.description]: buildOperatorScopeDescription(segment),
    [F.successMetric]: `Seed ID: ${segment.seedId} — ${cov.total} operators in scope`,
    Deliverables: segment.deliverables,
    [F.blocker]: "None",
  };
}

export function buildParentCompanyDeferPatch(existingFields) {
  const F = MAP_MASTER_TODO;
  const patch = {};
  if (existingFields?.[F.status] !== "Deferred") patch[F.status] = "Deferred";
  if (existingFields?.[F.nextAction] !== PARENT_DEFERRED_NEXT_ACTION) {
    patch[F.nextAction] = PARENT_DEFERRED_NEXT_ACTION;
  }
  if (existingFields?.[F.stepNumber] != null) patch[F.stepNumber] = null;
  return Object.keys(patch).length ? patch : null;
}

export function buildMasterTrackerPatch(coverageSummary) {
  const totalBrands = coverageSummary.reduce((s, x) => s + x.coverage.total, 0);
  const inRepo = coverageSummary.reduce((s, x) => s + x.coverage.inRepo, 0);
  const l2 = coverageSummary.reduce((s, x) => s + x.coverage.l2.length, 0);

  return {
    Status: "Needs Review",
    Progress: pct((inRepo / Math.max(totalBrands, 1)) * 100),
    "Next Action": [
      `Master tracker: ${inRepo}/${totalBrands} taxonomy brands in repo (${l2} L2).`,
      "8 chain-scale brand tasks (steps 6–13) + 4 operator-type tasks (steps 14–17).",
      "Report: reports/brand-operator-explorer-coverage-tracker.md.",
      "Joan: sign off per-brand checklist → Completed.",
    ].join(" "),
  };
}

/** Generate markdown tracker section for brand segments. */
export function renderBrandTrackerMarkdown(scored) {
  const lines = [
    "## Chain-scale brand segments",
    "",
    "| Step | Segment | Brands | In repo | L2 | Progress | Status |",
    "|------|---------|--------|---------|-----|----------|--------|",
  ];
  for (const { segment, coverage } of scored) {
    lines.push(
      `| ${segment.stepNumber} | ${segment.taskName.replace("Complete Brand Explorer profiles — ", "")} | ${coverage.total} | ${coverage.inRepo} | ${coverage.l2.length} | ${coverage.progress} | ${coverage.status} |`
    );
  }
  lines.push("");
  lines.push("### In-repo brands by segment");
  lines.push("");
  for (const { segment, coverage } of scored) {
    const inRepo = coverage.rows.filter((r) => r.level !== "missing");
    if (!inRepo.length) continue;
    lines.push(`**${segment.taskName.replace("Complete Brand Explorer profiles — ", "")}:**`);
    lines.push(
      inRepo.map((r) => `- ${r.brand} (${r.parent}) — ${r.level === "l2" ? "L2" : r.level === "l1" ? "L1" : "L2 queue"}`).join("\n")
    );
    lines.push("");
  }
  return lines.join("\n");
}

export {
  CHAIN_SCALE_BRAND_SEGMENTS,
  OPERATOR_EXPLORER_SEGMENTS,
};
