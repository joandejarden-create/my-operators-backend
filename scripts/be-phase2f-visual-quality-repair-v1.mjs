#!/usr/bin/env node
/**
 * Phase 2F — repair Curio/Ascend quality candidates + write scorecards (no Airtable writes).
 */
import { mkdirSync, writeFileSync } from "fs";
import { join } from "path";
import { repairBrandPairFromDisk } from "../lib/brand-explorer/be-phase2f-presentation-repair-v1.js";
import { evaluateCandidateOverlay } from "../lib/brand-explorer/be-phase2f-visual-quality-gates-v1.js";
import { readFileSync } from "fs";

const root = process.cwd();
const outDir = join(root, "reports/brand-explorer/phase2f-visual-quality");
mkdirSync(outDir, { recursive: true });
mkdirSync(join(root, "public/brand-explorer-phase2e-preview"), { recursive: true });

const beforeCurio = JSON.parse(
  readFileSync(join(root, "public/brand-explorer-phase2e-preview/curio-collection-quality-candidate.json"), "utf8"),
);
const beforeAscend = JSON.parse(
  readFileSync(join(root, "public/brand-explorer-phase2e-preview/ascend-quality-candidate.json"), "utf8"),
);

const beforeScores = {
  curio: evaluateCandidateOverlay(beforeCurio, { brandName: "Curio Collection by Hilton" }),
  ascend: evaluateCandidateOverlay(beforeAscend, { brandName: "Ascend Hotel Collection" }),
};

const { curio, ascend } = repairBrandPairFromDisk();

writeFileSync(
  join(root, "public/brand-explorer-phase2e-preview/curio-collection-phase2f-candidate.json"),
  JSON.stringify(curio.repaired, null, 2),
);
writeFileSync(
  join(root, "public/brand-explorer-phase2e-preview/ascend-phase2f-candidate.json"),
  JSON.stringify(ascend.repaired, null, 2),
);
writeFileSync(
  join(root, "public/brand-explorer-phase2e-preview/curio-collection-phase2f-scorecard.json"),
  JSON.stringify(curio.scorecard, null, 2),
);
writeFileSync(
  join(root, "public/brand-explorer-phase2e-preview/ascend-phase2f-scorecard.json"),
  JSON.stringify(ascend.scorecard, null, 2),
);

const summary = {
  phase: "2F",
  writes: 0,
  paidResearch: 0,
  founderApproved: false,
  before: {
    curio: beforeScores.curio.metrics,
    ascend: beforeScores.ascend.metrics,
  },
  after: {
    curio: curio.scorecard.metrics,
    ascend: ascend.scorecard.metrics,
  },
  failureRegisterPaths: {
    curio: "reports/brand-explorer/phase2f-visual-quality/07-curio-failure-register.json",
    ascend: "reports/brand-explorer/phase2f-visual-quality/07-ascend-failure-register.json",
  },
};

writeFileSync(join(outDir, "00-before-after-scorecard.json"), JSON.stringify(summary, null, 2));
writeFileSync(join(outDir, "07-curio-failure-register.json"), JSON.stringify(curio.scorecard.failureRegister, null, 2));
writeFileSync(join(outDir, "07-ascend-failure-register.json"), JSON.stringify(ascend.scorecard.failureRegister, null, 2));
writeFileSync(
  join(outDir, "00-before-after-scorecard.md"),
  `# Phase 2F visual quality — before/after

**Founder approval:** NOT claimed (STOP for founder review)

| Brand | Before visual pass | After visual pass | Blocked failures |
|---|---:|---:|---:|
| Curio | ${beforeScores.curio.metrics.visualQualityPassRate} | ${curio.scorecard.metrics.visualQualityPassRate} | ${curio.scorecard.failureRegister.length} |
| Ascend | ${beforeScores.ascend.metrics.visualQualityPassRate} | ${ascend.scorecard.metrics.visualQualityPassRate} | ${ascend.scorecard.failureRegister.length} |

Airtable writes: 0 · Paid research: 0
`,
);

console.log(JSON.stringify(summary, null, 2));
