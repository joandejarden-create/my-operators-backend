#!/usr/bin/env node
/**
 * Assemble founder certification package for Three-Layer contract + PR #599.
 * Read-only / local artifacts only — no Airtable mutation.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import {
  GDI_THREE_LAYER_INTELLIGENCE_CONTRACT_V1,
  FOUNDER_PRINCIPLES,
  EXISTING_ARCHITECTURE_MAPPING,
  FIELD_OWNERSHIP_TABLE,
} from "../lib/group-demand-intelligence/workflow/gdi-three-layer-intelligence-contract-v1.js";
import { GDI_RESEARCH_SOT_FIELD_OWNERSHIP_V1 } from "../lib/group-demand-intelligence/workflow/gdi-research-sot-field-ownership-v1.js";
import { GDI_RESEARCH_SOT_LOSSLESS_MERGE_V1 } from "../lib/group-demand-intelligence/workflow/gdi-research-sot-lossless-merge-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-three-layer-intelligence-certification-v1",
);
fs.mkdirSync(OUT, { recursive: true });

function writeJson(name, data) {
  fs.writeFileSync(path.join(OUT, name), `${JSON.stringify(data, null, 2)}\n`);
}

function runTest(scriptName) {
  const r = spawnSync(
    process.execPath,
    [path.join(ROOT, "scripts", scriptName)],
    { encoding: "utf8", cwd: ROOT },
  );
  return {
    script: scriptName,
    exitCode: r.status,
    ok: r.status === 0,
    stdoutTail: String(r.stdout || "").slice(-800),
    stderrTail: String(r.stderr || "").slice(-400),
  };
}

const testResults = [
  runTest("test-gdi-three-layer-intelligence-contract-v1.mjs"),
  runTest("test-gdi-research-sot-lossless-merge-v1.mjs"),
  runTest("test-gdi-research-inventory-lossless-merge-v1.mjs"),
  runTest("test-gdi-validated-fqo-write-set-policy-v1.mjs"),
  runTest("test-gdi-full-e2e-research-persistence-bridge-v1.mjs"),
];

const niceAuditPath = path.join(
  ROOT,
  "reports/gdi-nice-sot-preservation-audit-v1/00-NICE_PRESERVATION_AUDIT_SUMMARY.json",
);
const manifestPath = path.join(
  ROOT,
  "reports/gdi-bethesda-68-lossless-write-manifest-v1/00-MANIFEST_SUMMARY.json",
);
const canaryPath = path.join(
  ROOT,
  "reports/gdi-nice-canary-reissue-lossless-v1/00-CANARY_REISSUE_SUMMARY.json",
);

const niceAudit = fs.existsSync(niceAuditPath)
  ? JSON.parse(fs.readFileSync(niceAuditPath, "utf8"))
  : null;
const manifest = fs.existsSync(manifestPath)
  ? JSON.parse(fs.readFileSync(manifestPath, "utf8"))
  : null;
const canary = fs.existsSync(canaryPath)
  ? JSON.parse(fs.readFileSync(canaryPath, "utf8"))
  : null;

const allTestsPass = testResults.every((t) => t.ok);
const niceOk =
  niceAudit?.verdict === "NICE_LOSSLESS_MERGE_VERIFIED_DRY" ||
  niceAudit?.zeroUnexplainedLoss === true;
const canaryProposed =
  canary?.status === "PROPOSED_NOT_EXECUTED" &&
  canary?.mutationExecuted === false;

const remainingBlockers = [];
if (!allTestsPass) remainingBlockers.push("REGRESSION_TESTS_FAILING");
if (!niceOk) remainingBlockers.push("NICE_PRESERVATION_AUDIT_INCOMPLETE");
if (!canaryProposed) remainingBlockers.push("CANARY_PROPOSAL_MISSING");
remainingBlockers.push(
  "AWAITING_FOUNDER_REVIEW_BEFORE_MERGE",
  "AWAITING_SEPARATE_FOUNDER_AUTH_FOR_LIVE_CANARY",
  "CI_MUST_PASS_ON_PR_HEAD",
);

// Verdict: remediation is technically ready for founder GO decision on canary,
// but merge/canary execute remain blocked until founder approval.
const verdict =
  allTestsPass && niceOk && canaryProposed
    ? "READY_FOR_FOUNDER_GO"
    : "BLOCKED";

const package_ = {
  contract: GDI_THREE_LAYER_INTELLIGENCE_CONTRACT_V1,
  ownership: GDI_RESEARCH_SOT_FIELD_OWNERSHIP_V1,
  merge: GDI_RESEARCH_SOT_LOSSLESS_MERGE_V1,
  pr: 599,
  prUrl:
    "https://github.com/joandejarden-create/my-operators-backend/pull/599",
  mutationExecuted: false,
  mergeAuthorized: false,
  canaryExecuted: false,
  founderPrinciples: FOUNDER_PRINCIPLES,
  architectureMapping: EXISTING_ARCHITECTURE_MAPPING,
  fieldOwnershipTable: FIELD_OWNERSHIP_TABLE,
  regressionTests: testResults,
  allTestsPass,
  nicePreservation: niceAudit
    ? {
        verdict: niceAudit.verdict,
        liveChars: niceAudit.sizes?.liveChars,
        rejectedSlimChars: niceAudit.sizes?.rejectedSlimChars,
        losslessMergedChars: niceAudit.sizes?.losslessMergedChars,
        zeroUnexplainedLoss: niceAudit.zeroUnexplainedLoss,
      }
    : null,
  bethesda68Manifest: manifest
    ? {
        bucketCounts: manifest.bucketCounts,
        necessaryUpdateCount: manifest.necessaryUpdateCount,
        historicallyCompatibleFqoCount:
          manifest.historicallyCompatibleFqoCount,
        mutationAuthorized: manifest.mutationAuthorized,
      }
    : null,
  canaryProposal: canary,
  remainingBlockers,
  verdict,
  note:
    verdict === "READY_FOR_FOUNDER_GO"
      ? "Technical remediation complete. STOP — Integration Controller merge and live canary require separate founder authorization."
      : "Technical blockers remain — see remainingBlockers.",
};

writeJson("00-CERTIFICATION_PACKAGE_SUMMARY.json", package_);

const md = `# Three-Layer Intelligence Contract — Founder Certification Package

**PR:** [#599](https://github.com/joandejarden-create/my-operators-backend/pull/599)  
**Contract:** \`${GDI_THREE_LAYER_INTELLIGENCE_CONTRACT_V1}\`  
**Verdict:** \`${verdict}\`

## Deliverables checklist

1. Three-Layer Intelligence Contract specification — \`docs/ai-build-system/GDI_THREE_LAYER_INTELLIGENCE_CONTRACT_V1.md\`
2. Existing architecture mapping — included above / \`EXISTING_ARCHITECTURE_MAPPING\`
3. Machine-enforced field ownership — \`${GDI_RESEARCH_SOT_FIELD_OWNERSHIP_V1}\` + contract module
4. Lossless semantic merge — \`${GDI_RESEARCH_SOT_LOSSLESS_MERGE_V1}\`
5. NICE before/after preservation audit — \`reports/gdi-nice-sot-preservation-audit-v1/\`
6. Permanent regression tests — exit ${allTestsPass ? "PASS" : "FAIL"} (${testResults.filter((t) => t.ok).length}/${testResults.length})
7. Revised Bethesda 68 write manifest — \`reports/gdi-bethesda-68-lossless-write-manifest-v1/\` (necessary updates: ${manifest?.necessaryUpdateCount ?? "n/a"})
8. Research-preservation + idempotency — covered by three-layer + lossless merge suites
9. Proposed one-record canary — \`reports/gdi-nice-canary-reissue-lossless-v1/\` (**NOT EXECUTED**)
10. CI status — see GitHub PR checks on latest head
11. Integration Controller — **AWAITING** founder approval before merge
12. Remaining blockers — ${remainingBlockers.join(", ")}
13. Final verdict — **\`${verdict}\`**

## NICE sizes

| Live | Rejected slim | Lossless |
|---|---|---|
| ${niceAudit?.sizes?.liveChars ?? "—"} | ${niceAudit?.sizes?.rejectedSlimChars ?? "—"} | ${niceAudit?.sizes?.losslessMergedChars ?? "—"} |

## Production posture

- No live Airtable mutation in this package
- No GDO / STAGED / CURRENT changes
- \`VALIDATED_FQO_ONLY\` retained
- STOP before merge or canary execute

**THREE DISTINCT INTELLIGENCE LAYERS. ONE COHERENT ARCHITECTURE. ZERO SILENT INFORMATION LOSS.**
`;

fs.writeFileSync(path.join(OUT, "FOUNDER_CERTIFICATION_PACKAGE.md"), md);
console.log(JSON.stringify({ out: OUT, verdict, allTestsPass }, null, 2));
process.exit(allTestsPass ? 0 : 1);
