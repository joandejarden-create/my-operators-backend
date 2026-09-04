#!/usr/bin/env node
/**
 * Emit Wave 3 full 19-brand + 4-parent reconciliation matrix (no provider calls).
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { buildBaiWave3FullCohortReconciliationV1 } from "../lib/ai-visibility/brand-longitudinal/bai-wave3-longitudinal-intelligence-v1.js";
import { BAI_VIEW_MODE } from "../lib/ai-visibility/brand-longitudinal/resolve-bai-prior-comparable-period-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const outDir = path.join(ROOT, "reports", "bai-wave3-longitudinal-qa");
fs.mkdirSync(outDir, { recursive: true });

const full = buildBaiWave3FullCohortReconciliationV1({
  viewMode: BAI_VIEW_MODE.INTERNAL_CANDIDATE_LONGITUDINAL_QA,
});

const jsonPath = path.join(outDir, "full-19-brand-reconciliation.json");
fs.writeFileSync(jsonPath, JSON.stringify(full, null, 2));

function fmtPct(n) {
  return n == null || !Number.isFinite(Number(n)) ? "—" : Number(n).toFixed(1) + "%";
}
function fmtDelta(n, display) {
  if (display) return display;
  if (n == null || !Number.isFinite(Number(n))) return "—";
  const v = Number(n);
  return (v > 0 ? "+" : "") + v.toFixed(1) + " pp";
}

const lines = [];
lines.push("# BAI Wave 3 — Full 19-brand cohort reconciliation");
lines.push("");
lines.push(`- Period 2 publication state: **${full.PERIOD_2_PUBLICATION_STATE}**`);
lines.push(`- LIVE_PROVIDER_CALLS: **${full.LIVE_PROVIDER_CALLS}**`);
lines.push(`- Peer mean Δ: **${fmtDelta(full.peerMeanDeltaPp)}**`);
lines.push(
  `- Owner-intent cohort state: **${full.ownerIntentCohortState || "—"}**`
);
lines.push(`- Coverage gates: \`${JSON.stringify(full.gates)}\``);
lines.push("");
lines.push("## Parent-company summaries");
lines.push("");
lines.push(
  "| Parent | Brands | Prior → Current | Δ | Improving | Declining | Stable | Strongest gain | Largest loss | Abs / Rel |"
);
lines.push("|---|---:|---|---|---:|---:|---:|---|---|---|");
for (const p of full.parentSummaries || []) {
  lines.push(
    `| ${p.parentCompanyName} | ${p.brandCount} | ${fmtPct(p.priorPresence)} → ${fmtPct(p.currentPresence)} | ${p.portfolioDeltaDisplay || "—"} | ${p.brandsImproving} | ${p.brandsDeclining} | ${p.brandsStable} | ${p.strongestPositiveMover?.brandName || "—"} (${p.strongestPositiveMover?.deltaDisplay || "—"}) | ${p.largestVisibilityLoss?.brandName || "—"} (${p.largestVisibilityLoss?.deltaDisplay || "—"}) | ${p.absoluteRelative?.absolutePerformance}/${p.absoluteRelative?.relativePerformance} |`
  );
}
lines.push("");
lines.push("## 19-brand matrix");
lines.push("");
lines.push(
  "| Parent | Brand | Brand ID | Current | Prior | Δ | Rank | Abs | Rel | Membership | ID match | History explained | Intent state |"
);
lines.push("|---|---|---|---:|---:|---|---|---|---|---|---|---|---|");
for (const m of full.matrix || []) {
  lines.push(
    `| ${m.parentCompanyName} | ${m.brandName} | \`${m.brandId}\` | ${fmtPct(m.currentPresence)} | ${fmtPct(m.priorPresence)} | ${m.deltaDisplay || fmtDelta(m.deltaPp)} | ${m.rankDisplay || "—"} | ${m.absolutePerformance} | ${m.relativePerformance} | ${m.membershipState} | ${m.canonicalBrandIdMatch ? "YES" : "NO"} | ${m.historyExplained ? "YES" : "NO"} | ${m.ownerIntent?.intentComparabilityState || "—"} (${m.ownerIntent?.comparableIntentCount ?? 0}/${m.ownerIntent?.intentCount ?? 0}) |`
  );
}
lines.push("");
lines.push("## Notes");
lines.push("");
lines.push("- Computation scope = full governed showcase monitoring cohort (19).");
lines.push("- Marriott remains the primary parent QA surface; not the computation scope.");
lines.push("- No Period 2 promotion. No provider reruns.");

const mdPath = path.join(outDir, "full-19-brand-reconciliation.md");
fs.writeFileSync(mdPath, lines.join("\n"));

console.log(
  JSON.stringify(
    {
      ok: full.ok,
      gates: full.gates,
      jsonPath,
      mdPath,
      parentCount: full.parentSummaries?.length,
      brandCount: full.matrix?.length,
    },
    null,
    2
  )
);
process.exit(full.ok ? 0 : 1);
