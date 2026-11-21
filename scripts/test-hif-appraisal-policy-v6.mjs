#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  APPRAISAL_ADAPTER_DECISION,
  assertAppraisalPolicyLocked,
  buildSourcePriorityPointerQueues,
  routeAppraisalPilotDocToQueues,
} from "../lib/hotel-intelligence/sec-edgar/hif-appraisal-policy-v6.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const errors = [];

const lock = assertAppraisalPolicyLocked();
if (!lock.ok) errors.push(...lock.errors);
if (APPRAISAL_ADAPTER_DECISION.buildStandaloneAppraisalAdapter !== false) {
  errors.push("ADAPTER_FLAG");
}
if (APPRAISAL_ADAPTER_DECISION.authorizePaidAppraisalResearch !== false) {
  errors.push("PAID_FLAG");
}

const assumption = routeAppraisalPilotDocToQueues({
  docId: "x",
  score: { assumptionOnly: true, pointerOnly: false },
  signals: { factClasses: ["APPRAISAL_ASSUMPTION"], appraisalAssumptions: ["CONTINUED_FRANCHISE"] },
});
if (assumption.maySatisfyContractualVerificationGate !== false) errors.push("ASSUMPTION_GATE");
if (assumption.queues.length !== 0) errors.push("ASSUMPTION_MUST_NOT_QUEUE");

const pointer = routeAppraisalPilotDocToQueues({
  docId: "p",
  score: { pointerOnly: true },
  signals: { factClasses: ["SOURCE_PRIORITY_POINTER"] },
  issuerHint: "CMBS due-diligence source map",
});
if (!pointer.queues.includes("FRANCHISE_AGREEMENT_DISCOVERY")) errors.push("POINTER_FRANCHISE");
if (!pointer.queues.includes("MANAGEMENT_CONTRACT_DISCOVERY")) errors.push("POINTER_MGMT");

const pilotPath = path.join(ROOT, "reports/sec-edgar-appraisal-uw-source-pilot-v1/PILOT_SUMMARY.json");
if (fs.existsSync(pilotPath)) {
  const pilot = JSON.parse(fs.readFileSync(pilotPath, "utf8"));
  const q = buildSourcePriorityPointerQueues(pilot);
  if (q.pointerCount !== 22) errors.push(`POINTER_COUNT_${q.pointerCount}`);
  if (!q.integrity.ok) errors.push(q.integrity.error);
  if (q.franchiseQueueCount < 1 || q.managementQueueCount < 1) errors.push("QUEUES_EMPTY");
}

if (errors.length) {
  console.error("FAIL", errors);
  process.exit(1);
}
console.log(
  JSON.stringify(
    {
      ok: true,
      decision: APPRAISAL_ADAPTER_DECISION.decision,
      pointers: 22,
    },
    null,
    2,
  ),
);
