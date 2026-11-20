#!/usr/bin/env node
/**
 * Phase 2G — sanitize Curio/Ascend candidates + write parity scorecards (no Airtable writes).
 */
import { mkdirSync, writeFileSync, readFileSync } from "fs";
import { join } from "path";
import { sanitizeBrandPairFromDisk } from "../lib/brand-explorer/be-phase2g-presentation-sanitize-v1.js";
import { evaluateAntiPadding } from "../lib/brand-explorer/be-phase2g-anti-padding-v1.js";
import { groupBlocksBySubsection } from "../lib/brand-explorer/be-phase2g-subsection-contract-v1.js";

const root = process.cwd();
const outDir = join(root, "reports/brand-explorer/phase2g-parity");
mkdirSync(outDir, { recursive: true });

const beforeCurio = JSON.parse(
  readFileSync(join(root, "public/brand-explorer-phase2e-preview/curio-collection-phase2f-candidate.json"), "utf8"),
);
const beforeAscend = JSON.parse(
  readFileSync(join(root, "public/brand-explorer-phase2e-preview/ascend-phase2f-candidate.json"), "utf8"),
);

const beforePad = {
  curio: evaluateAntiPadding(beforeCurio.brandExplorer?.blocks || [], { brandName: "Curio Collection by Hilton" }),
  ascend: evaluateAntiPadding(beforeAscend.brandExplorer?.blocks || [], { brandName: "Ascend Hotel Collection" }),
};

const { curio, ascend } = sanitizeBrandPairFromDisk();

function writePair(slug, result) {
  writeFileSync(
    join(root, `public/brand-explorer-phase2e-preview/${slug}-phase2g-candidate.json`),
    JSON.stringify(result.sanitized, null, 2),
  );
  writeFileSync(
    join(root, `public/brand-explorer-phase2e-preview/${slug}-phase2g-scorecard.json`),
    JSON.stringify(result.metrics, null, 2),
  );
}

writePair("curio-collection", curio);
writePair("ascend", ascend);

writeFileSync(join(outDir, "07-curio-failure-register.json"), JSON.stringify(curio.antiPadding.failureRegister, null, 2));
writeFileSync(join(outDir, "07-ascend-failure-register.json"), JSON.stringify(ascend.antiPadding.failureRegister, null, 2));
writeFileSync(join(outDir, "08-curio-research-gaps.json"), JSON.stringify(curio.researchGaps, null, 2));
writeFileSync(join(outDir, "08-ascend-research-gaps.json"), JSON.stringify(ascend.researchGaps, null, 2));

const curioGrouped = groupBlocksBySubsection(curio.sanitized.brandExplorer.blocks);
const subsectionAudit = curioGrouped.order.map((sub) => ({
  id: sub.id,
  tabName: sub.tabName,
  label: sub.label,
  itemCount: (curioGrouped.byId.get(sub.id)?.items || []).length,
}));

writeFileSync(join(outDir, "05-curio-subsection-inventory.json"), JSON.stringify(subsectionAudit, null, 2));

const summary = {
  phase: "2G",
  writes: 0,
  paidResearch: 0,
  founderApproved: false,
  pr453Status: "OPEN — founder-rejected; do not merge until Phase 2G acceptance",
  beforeAntiPadding: {
    curio: beforePad.curio.metrics,
    ascend: beforePad.ascend.metrics,
  },
  after: {
    curio: curio.metrics,
    ascend: ascend.metrics,
  },
};

writeFileSync(join(outDir, "00-before-after-parity.json"), JSON.stringify(summary, null, 2));
writeFileSync(
  join(outDir, "00-before-after-parity.md"),
  `# Phase 2G parity — before/after

**Founder approval:** NOT claimed (STOP for founder review)

| Brand | Phase 2F anti-padding failures | Phase 2G anti-padding failures | Research gaps |
|---|---:|---:|---:|
| Curio | ${beforePad.curio.metrics.paddingFailures} | ${curio.antiPadding.metrics.paddingFailures} | ${curio.researchGaps.length} |
| Ascend | ${beforePad.ascend.metrics.paddingFailures} | ${ascend.antiPadding.metrics.paddingFailures} | ${ascend.researchGaps.length} |

Airtable writes: 0 · Paid research: 0
`,
);

console.log(JSON.stringify(summary, null, 2));
