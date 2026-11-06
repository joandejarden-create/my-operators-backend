/**
 * ADP Full E2E fail-closed stage enforcement.
 * Missing handlers, empty placeholders, or failed quality gates cannot green-certify.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { ADP_OS_STAGES } from "./adp-full-e2e-workflow-v1.js";
import { EXECUTION_STATE } from "./execution-states.js";

export const ADP_FAIL_CLOSED_STAGE_CONTRACT_V1 = "ADP_FAIL_CLOSED_STAGE_CONTRACT_V1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const RUNNER_PATH = path.join(__dirname, "autonomous-stage-runner-v1.js");

/**
 * Audit mandatory stages vs autonomous runner handlers.
 */
export function auditMandatoryStageHandlers({
  runnerSource = null,
  stages = ADP_OS_STAGES,
} = {}) {
  const src =
    runnerSource ||
    (fs.existsSync(RUNNER_PATH) ? fs.readFileSync(RUNNER_PATH, "utf8") : "");
  const rows = [];
  for (const stage of stages) {
    const hasHandler = src.includes(`async ${stage.id}(`);
    rows.push({
      stageId: stage.id,
      required: stage.required !== false,
      hasHandler,
      failClosed: stage.failClosed === true,
      description: stage.description || null,
      status: hasHandler
        ? "HANDLER_PRESENT"
        : stage.required === false
          ? "OPTIONAL_NO_HANDLER"
          : "MISSING_HANDLER_BLOCKER",
    });
  }
  const missingRequired = rows.filter((r) => r.status === "MISSING_HANDLER_BLOCKER");
  return {
    contractVersion: ADP_FAIL_CLOSED_STAGE_CONTRACT_V1,
    stageCount: rows.length,
    missingRequiredHandlers: missingRequired.map((r) => r.stageId),
    allMandatoryHandlersPresent: missingRequired.length === 0,
    rows,
    auditedAt: new Date().toISOString(),
  };
}

/**
 * Assert before autonomous run — throw or return blocker.
 */
export function assertMandatoryStageHandlersOrBlock() {
  const audit = auditMandatoryStageHandlers();
  if (!audit.allMandatoryHandlersPresent) {
    return {
      ok: false,
      state: EXECUTION_STATE.BLOCKED_TECHNICAL,
      blockers: [
        {
          code: "MISSING_MANDATORY_STAGE_HANDLERS",
          stages: audit.missingRequiredHandlers,
          detail:
            "ADP Full E2E cannot start — mandatory stages lack autonomous handlers (fail-closed)",
        },
      ],
      audit,
    };
  }
  return { ok: true, audit };
}

/**
 * Classify whether a stage result may count toward founder-review readiness.
 */
export function classifyStageCertificationEligibility(stageResult) {
  if (!stageResult) {
    return {
      eligible: false,
      reason: "NO_STAGE_RESULT",
    };
  }
  if (stageResult.state === EXECUTION_STATE.COMPLETE) {
    if (stageResult.qualityGate?.passed === false) {
      return { eligible: false, reason: "COMPLETE_BUT_QUALITY_GATE_FAILED" };
    }
    if ((stageResult.blockers || []).length > 0) {
      return { eligible: false, reason: "COMPLETE_WITH_BLOCKERS" };
    }
    return { eligible: true, reason: "COMPLETE_CLEAN" };
  }
  if (stageResult.state === EXECUTION_STATE.WAITING_AUTHORIZATION) {
    return {
      eligible: false,
      reason: "WAITING_AUTHORIZATION",
      nextAction: stageResult.nextAction || stageResult.blockers?.[0] || null,
    };
  }
  return {
    eligible: false,
    reason: stageResult.state || "NON_COMPLETE",
  };
}

export function buildFailClosedContractDoc() {
  return {
    contractVersion: ADP_FAIL_CLOSED_STAGE_CONTRACT_V1,
    rules: [
      "Every required ADP_OS_STAGES entry must have an autonomous handler",
      "Missing handler → BLOCKED_TECHNICAL (never skip to next as success)",
      "qualityGate.passed===false cannot remain COMPLETE (demoted to BLOCKED_EVIDENCE)",
      "Required inputs absent → explicit blocker, not empty placeholder success",
      "Not applicable → governed NOT_APPLICABLE / STATUS_ONLY reason recorded",
      "Prior certifications that depended on skipped stages are invalidated until reassessment",
      "Reality Gap MEASURED requires observation corpus + governed attributes",
      "BPP READY_RANK requires certified peer set + metrics — never invented ranks",
      "Founder-review readiness requires independent second-pass verification",
    ],
  };
}
