#!/usr/bin/env node
/**
 * Permanent gate: BAI_PROMOTED_PERIOD_STORE_DEPLOYMENT_COMPLETENESS
 *
 * Back-tests live Period 2 by default. NO publication mutation. NO provider calls.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  BAI_PERIOD_2_CANDIDATE_ID,
  BAI_CUSTOMER_PUBLISHED_PERIOD_ID,
} from "../lib/ai-visibility/brand-longitudinal/resolve-bai-prior-comparable-period-v1.js";
import {
  BAI_PROMOTED_PERIOD_STORE_DEPLOYMENT_COMPLETENESS,
  evaluateBaiPromotedPeriodStoreDeploymentCompletenessV1,
  isPathIgnoredByIgnoreFile,
  matchIgnorePattern,
} from "../lib/ai-visibility/brand-longitudinal/bai-promoted-period-store-deployment-completeness-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const outDir = path.join(
  ROOT,
  "reports/bai-p2-promotion-readiness/deployment-completeness"
);
fs.mkdirSync(outDir, { recursive: true });

let passed = 0;
let failed = 0;
function test(name, fn) {
  try {
    fn();
    passed += 1;
    console.log("  PASS", name);
  } catch (err) {
    failed += 1;
    console.log("  FAIL", name + ":", err.message);
  }
}

const periodId = process.env.BAI_DEPLOY_GATE_PERIOD_ID || BAI_PERIOD_2_CANDIDATE_ID;

test("ignore_matcher_data_star_and_negation", () => {
  const sample = [
    "data/*",
    "!data/ai-visibility/runtime/brand-longitudinal/",
    `!data/ai-visibility/runtime/brand-longitudinal/${periodId}/`,
    `!data/ai-visibility/runtime/brand-longitudinal/${periodId}/**`,
    `data/ai-visibility/runtime/brand-longitudinal/${periodId}/raw/`,
    `data/ai-visibility/runtime/brand-longitudinal/${periodId}/raw/**`,
  ].join("\n");
  const cvp = `data/ai-visibility/runtime/brand-longitudinal/${periodId}/current-vs-prior.json`;
  const ev = `data/ai-visibility/runtime/brand-longitudinal/${periodId}/evidence/ev_x.json`;
  const raw = `data/ai-visibility/runtime/brand-longitudinal/${periodId}/raw/x.json`;
  if (isPathIgnoredByIgnoreFile(cvp, sample)) {
    throw new Error("current-vs-prior should be allowed by negation");
  }
  if (isPathIgnoredByIgnoreFile(ev, sample)) {
    throw new Error("evidence should be allowed by negation");
  }
  if (!isPathIgnoredByIgnoreFile(raw, sample)) {
    throw new Error("raw/ should remain excluded");
  }
  if (!matchIgnorePattern("data/ai-visibility", "data/*")) {
    throw new Error("data/* should match data/ai-visibility");
  }
});

const result = evaluateBaiPromotedPeriodStoreDeploymentCompletenessV1({
  periodId,
  repoRoot: ROOT,
});

test(BAI_PROMOTED_PERIOD_STORE_DEPLOYMENT_COMPLETENESS, () => {
  if (!result.ok) {
    const detail = result.failures
      .map((f) => `${f.name}:${JSON.stringify(f.detail)}`)
      .join("; ");
    throw new Error(detail || "gate failed");
  }
  if (result.periodId !== periodId) {
    throw new Error(`period mismatch ${result.periodId}`);
  }
  if (result.LIVE_MUTATION !== false) {
    throw new Error("LIVE_MUTATION must be false");
  }
  if (result.trackedCounts.evidence < 1 || result.trackedCounts.mentions < 1) {
    throw new Error("missing evidence/mentions in git index");
  }
});

test("live_p2_pointer_alignment_when_published", () => {
  if (BAI_CUSTOMER_PUBLISHED_PERIOD_ID === BAI_PERIOD_2_CANDIDATE_ID) {
    if (periodId === BAI_PERIOD_2_CANDIDATE_ID && !result.pointerIsThisPeriod) {
      throw new Error("expected pointerIsThisPeriod for live P2");
    }
  }
});

fs.writeFileSync(
  path.join(outDir, "deployment-completeness-summary.json"),
  JSON.stringify(
    {
      gate: BAI_PROMOTED_PERIOD_STORE_DEPLOYMENT_COMPLETENESS,
      result: result.ok ? "PASS" : "FAIL",
      periodId,
      publishedPointerPeriodId: result.publishedPointerPeriodId,
      trackedCounts: result.trackedCounts,
      requiredRootFiles: result.requiredRootFiles,
      requiredDirs: result.requiredDirs,
      failureCount: result.failures.length,
      failures: result.failures,
      checks: result.checks,
      LIVE_MUTATION: false,
      LIVE_PROVIDER_CALLS: 0,
    },
    null,
    2
  )
);

console.log(
  `\n${BAI_PROMOTED_PERIOD_STORE_DEPLOYMENT_COMPLETENESS}: ${passed} passed, ${failed} failed (period=${periodId})`
);
process.exit(failed ? 1 : 0);
