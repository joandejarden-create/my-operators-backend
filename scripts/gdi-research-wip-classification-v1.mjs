#!/usr/bin/env node
/**
 * Classify unmerged GDI research WIP (commit 72df16986 vs isolation staged vs main).
 * Read-only. Writes reports under reports/gdi-research-consolidation-and-full-e2e-persistence-v1/.
 */
import fs from "node:fs";
import path from "node:path";
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const ISO = path.resolve(ROOT, "../wt-gdi-isolation-post-365");
const COMMIT = "72df16986";
const OUT = path.join(
  ROOT,
  "reports/gdi-research-consolidation-and-full-e2e-persistence-v1",
);

function sh(args, cwd = ROOT) {
  return execFileSync("git", args, {
    cwd,
    encoding: "utf8",
    maxBuffer: 64 * 1024 * 1024,
    stdio: ["ignore", "pipe", "pipe"],
  }).trim();
}

function trySh(args, cwd = ROOT) {
  try {
    return sh(args, cwd);
  } catch {
    return null;
  }
}

function lines(text) {
  return String(text || "")
    .split(/\r?\n/)
    .map((s) => s.trim())
    .filter(Boolean);
}

function classify(file) {
  const cats = [];
  const base = path.basename(file);
  if (/\/full-e2e-fourteen-segment-state\//.test(file)) {
    cats.push("FULL_E2E_SEGMENT_STATE");
  }
  if (/\/opportunity-research-state\//.test(file)) {
    cats.push("OPPORTUNITY_RESEARCH_STATE");
  }
  if (/\/pursuits\.json$/.test(file)) cats.push("PURSUITS");
  if (/^data\/group-demand-intelligence\/hotels\//.test(file)) {
    cats.push("HOTEL_CANONICAL_DATA_TREE");
  }
  if (/^reports\//.test(file)) cats.push("RUN_REPORT_ARTIFACT");
  if (/hist_/.test(base)) cats.push("HISTORICAL_EVIDENCE");
  if (/disc_/.test(base)) cats.push("DISCOVERY_STATE");
  if (/my-operators-backend-ava-v4-reconcile/.test(file)) {
    cats.push("UNRELATED_NESTED_WORKTREE");
  }
  if (/\.(log|tmp)$/i.test(base)) cats.push("TEMPORARY");
  if (!cats.length) cats.push("OTHER");
  return cats;
}

function hotelIdFrom(file) {
  const m = file.match(/hotels\/(rec[A-Za-z0-9]+)\//);
  return m ? m[1] : null;
}

function opportunityHint(file) {
  const b = path.basename(file, path.extname(file));
  return /^(hist_|disc_)/.test(b) ? b : null;
}

function primaryDisposition(categories) {
  if (categories.includes("UNRELATED_NESTED_WORKTREE")) {
    return "EXCLUDE_UNRELATED";
  }
  if (categories.includes("TEMPORARY")) return "TEMPORARY";
  if (categories.includes("RUN_REPORT_ARTIFACT")) {
    return "OFF_MAIN_EVIDENCE_ARCHIVE";
  }
  if (categories.includes("FULL_E2E_SEGMENT_STATE")) {
    return "HOTEL_EXECUTION_STATE_PRESERVE";
  }
  if (categories.includes("OPPORTUNITY_RESEARCH_STATE")) {
    return "HOTEL_RESEARCH_CONTINUATION_PRESERVE";
  }
  if (categories.includes("PURSUITS")) {
    return "REVIEW_PURSUITS_DELTA";
  }
  return "REVIEW";
}

const commitFiles = lines(sh(["show", "--name-only", "--pretty=format:", COMMIT]));
const isolationStaged = fs.existsSync(ISO)
  ? lines(sh(["diff", "--cached", "--name-only"], ISO))
  : [];
const commitSet = new Set(commitFiles);
const isoSet = new Set(isolationStaged);

const mainPaths = new Set(
  lines(
    trySh([
      "ls-tree",
      "-r",
      "--name-only",
      "main",
      "--",
      "data/group-demand-intelligence",
      "reports",
    ]),
  ),
);

const differCommitVsIso = new Set(
  fs.existsSync(ISO)
    ? lines(trySh(["diff", "--cached", "--name-only", COMMIT], ISO) || "")
    : [],
);

const all = [...new Set([...commitFiles, ...isolationStaged])].sort();
const rows = [];
const opportunityHints = new Set();
const byCategory = {};
const byHotel = {};
const byDisposition = {};

for (const file of all) {
  const inCommit = commitSet.has(file);
  const inIso = isoSet.has(file);
  const categories = classify(file);
  const disposition = primaryDisposition(categories);
  const hotelId = hotelIdFrom(file);
  const hint = opportunityHint(file);
  if (hint) opportunityHints.add(hint);
  for (const c of categories) byCategory[c] = (byCategory[c] || 0) + 1;
  if (hotelId) byHotel[hotelId] = (byHotel[hotelId] || 0) + 1;
  byDisposition[disposition] = (byDisposition[disposition] || 0) + 1;

  let contentRelation = "UNKNOWN";
  if (inCommit && inIso) {
    contentRelation = differCommitVsIso.has(file)
      ? "COMMIT_DIFFERS_ISOLATION"
      : "COMMIT_EQ_ISOLATION";
  } else if (inCommit && !inIso) contentRelation = "COMMIT_ONLY";
  else if (!inCommit && inIso) contentRelation = "ISOLATION_ONLY";

  const onMain = mainPaths.has(file);
  rows.push({
    file,
    inCommit,
    inIsolationStaged: inIso,
    onMain,
    vsMain: onMain ? "PATH_EXISTS_ON_MAIN" : "NOT_ON_MAIN",
    contentRelation,
    categories,
    disposition,
    hotelId,
    opportunityHint: hint,
  });
}

// Extract opportunity ids from WIP research-state filenames and compare to canonical opportunities.json
function loadCanonicalOpportunityIds() {
  const hotelsDir = path.join(ROOT, "data/group-demand-intelligence/hotels");
  const byHotelIds = {};
  const allIds = new Set();
  if (!fs.existsSync(hotelsDir)) return { byHotelIds, allIds };
  for (const hotelId of fs.readdirSync(hotelsDir)) {
    const p = path.join(hotelsDir, hotelId, "opportunities.json");
    if (!fs.existsSync(p)) continue;
    try {
      const raw = JSON.parse(fs.readFileSync(p, "utf8"));
      const rowsOpp = Array.isArray(raw)
        ? raw
        : Array.isArray(raw?.opportunities)
          ? raw.opportunities
          : Array.isArray(raw?.rows)
            ? raw.rows
            : [];
      const ids = new Set();
      for (const row of rowsOpp) {
        const id = String(row?.opportunityId || row?.id || "").trim();
        if (!id) continue;
        ids.add(id);
        allIds.add(`${hotelId}::${id}`);
      }
      byHotelIds[hotelId] = [...ids].sort();
    } catch {
      byHotelIds[hotelId] = [];
    }
  }
  return { byHotelIds, allIds };
}

const canonical = loadCanonicalOpportunityIds();
const wipOppKeys = new Set();
for (const r of rows) {
  if (!r.opportunityHint || !r.hotelId) continue;
  // hist_/disc_ filenames are research-state keys, not always opportunityIds
  wipOppKeys.add(`${r.hotelId}::${r.opportunityHint}`);
}

const summary = {
  generatedAt: new Date().toISOString(),
  commit: COMMIT,
  isolationWorktree: ISO,
  isolationPresent: fs.existsSync(ISO),
  commitFileCount: commitFiles.length,
  isolationStagedCount: isolationStaged.length,
  unionCount: all.length,
  intersectionCount: commitFiles.filter((f) => isoSet.has(f)).length,
  commitOnlyCount: commitFiles.filter((f) => !isoSet.has(f)).length,
  isolationOnlyCount: isolationStaged.filter((f) => !commitSet.has(f)).length,
  commitOnly: commitFiles.filter((f) => !isoSet.has(f)),
  isolationOnly: isolationStaged.filter((f) => !commitSet.has(f)),
  contentEqualCount: rows.filter((r) => r.contentRelation === "COMMIT_EQ_ISOLATION")
    .length,
  contentDifferCount: rows.filter(
    (r) => r.contentRelation === "COMMIT_DIFFERS_ISOLATION",
  ).length,
  notOnMainCount: rows.filter((r) => r.vsMain === "NOT_ON_MAIN").length,
  pathExistsOnMainCount: rows.filter((r) => r.vsMain === "PATH_EXISTS_ON_MAIN")
    .length,
  byCategory,
  byHotel,
  byDisposition,
  uniqueResearchStateHints: [...opportunityHints].sort(),
  uniqueResearchStateHintCount: opportunityHints.size,
  canonicalHotelOpportunityFiles: Object.keys(canonical.byHotelIds).length,
  canonicalCompositeKeys: canonical.allIds.size,
  note:
    "822 WIP files are overwhelmingly Full E2E execution/report artifacts for a few hotels — not 822 new Airtable opportunities.",
};

fs.mkdirSync(OUT, { recursive: true });
fs.writeFileSync(
  path.join(OUT, "_phase1_classification_raw.json"),
  `${JSON.stringify({ summary, rows }, null, 2)}\n`,
);
fs.writeFileSync(
  path.join(OUT, "PHASE1_SUMMARY.json"),
  `${JSON.stringify(summary, null, 2)}\n`,
);

console.log(
  JSON.stringify(
    {
      commitFileCount: summary.commitFileCount,
      isolationStagedCount: summary.isolationStagedCount,
      unionCount: summary.unionCount,
      intersectionCount: summary.intersectionCount,
      contentEqualCount: summary.contentEqualCount,
      contentDifferCount: summary.contentDifferCount,
      byDisposition: summary.byDisposition,
      byHotel: summary.byHotel,
      uniqueResearchStateHintCount: summary.uniqueResearchStateHintCount,
      canonicalCompositeKeys: summary.canonicalCompositeKeys,
      commitOnly: summary.commitOnly,
      isolationOnly: summary.isolationOnly,
    },
    null,
    2,
  ),
);
