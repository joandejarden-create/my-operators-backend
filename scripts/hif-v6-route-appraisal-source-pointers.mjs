#!/usr/bin/env node
/**
 * Materialize the 22 appraisal/UW source-priority pointers into
 * Franchise + Management discovery queues (founder-locked policy).
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  APPRAISAL_ADAPTER_DECISION,
  assertAppraisalPolicyLocked,
  buildSourcePriorityPointerQueues,
} from "../lib/hotel-intelligence/sec-edgar/hif-appraisal-policy-v6.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const PILOT = path.join(ROOT, "reports/sec-edgar-appraisal-uw-source-pilot-v1/PILOT_SUMMARY.json");
const OUT = path.join(ROOT, "reports/sec-edgar-hif-v6-appraisal-policy");

const lock = assertAppraisalPolicyLocked();
if (!lock.ok) {
  console.error(lock);
  process.exit(1);
}

const pilot = JSON.parse(fs.readFileSync(PILOT, "utf8"));
const queues = buildSourcePriorityPointerQueues(pilot);
if (!queues.integrity.ok) {
  console.error(queues.integrity);
  process.exit(1);
}

fs.mkdirSync(OUT, { recursive: true });
fs.writeFileSync(path.join(OUT, "FOUNDER_DECISION_LOCK.json"), JSON.stringify(APPRAISAL_ADAPTER_DECISION, null, 2));
fs.writeFileSync(path.join(OUT, "SOURCE_PRIORITY_POINTER_QUEUES.json"), JSON.stringify(queues, null, 2));
fs.writeFileSync(
  path.join(OUT, "FRANCHISE_AGREEMENT_DISCOVERY_QUEUE.json"),
  JSON.stringify(
    {
      queue: "FRANCHISE_AGREEMENT_DISCOVERY",
      count: queues.franchiseQueue.length,
      items: queues.franchiseQueue,
    },
    null,
    2,
  ),
);
fs.writeFileSync(
  path.join(OUT, "MANAGEMENT_CONTRACT_DISCOVERY_QUEUE.json"),
  JSON.stringify(
    {
      queue: "MANAGEMENT_CONTRACT_DISCOVERY",
      count: queues.managementQueue.length,
      items: queues.managementQueue,
    },
    null,
    2,
  ),
);

const md = [
  "# HIF V6 — Appraisal / UW founder decision lock",
  "",
  `**Decision:** \`${APPRAISAL_ADAPTER_DECISION.decision}\``,
  "",
  "- 50 documents evaluated · 0 verified new contractual dates · 0 new prospects · **22** source-priority pointers · $0 spend",
  "- No standalone appraisal adapter in V6",
  "- No paid appraisal research authorized from this pilot",
  `- Pointers routed: Franchise queue **${queues.franchiseQueueCount}** · Management queue **${queues.managementQueueCount}**`,
  "- Continue CMBS/EX-10/regulatory/recorded-MA/court/ownership paths",
  "- Preserve incidental appraisal evidence; never treat valuation assumptions as contractual terms",
  "- CUSTOMER_BLOCKED · no Meta schema · no HPC · no customer publication",
  "",
  "**Success criterion:** More independently verified Franchise and Management Contract opportunities—not more appraisal documents.",
].join("\n");
fs.writeFileSync(path.join(OUT, "FOUNDER_DECISION_LOCK.md"), md);

console.log(
  JSON.stringify(
    {
      ok: true,
      decision: APPRAISAL_ADAPTER_DECISION.decision,
      pointers: queues.pointerCount,
      franchiseQueue: queues.franchiseQueueCount,
      managementQueue: queues.managementQueueCount,
      out: OUT,
    },
    null,
    2,
  ),
);
