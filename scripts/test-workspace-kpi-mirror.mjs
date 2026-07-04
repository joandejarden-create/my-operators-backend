/**
 * Verifies owner/brand workspace KPI mirror rules on sample BDR-shaped rows.
 * Run: node scripts/test-workspace-kpi-mirror.mjs
 */
import {
  auditWorkspaceKpiMirror,
  computeWorkspaceKpiSnapshot,
  enrichWorkspaceRow,
} from "../lib/deal-workspace-pipeline.js";

const samples = [
  {
    id: "recNew",
    status: "New",
    requestSentAt: new Date().toISOString(),
  },
  {
    id: "recAccepted",
    status: "Accepted",
    nextFollowupDate: "2020-01-01",
    lastUpdated: "2020-01-01",
  },
  {
    id: "recTerms",
    status: "Pre-LOI",
    proposalStatus: "Draft",
    lastUpdated: new Date().toISOString(),
  },
  {
    id: "recPassed",
    status: "Declined",
  },
];

const audit = auditWorkspaceKpiMirror(samples);
const owner = computeWorkspaceKpiSnapshot(samples, "owner");
const brand = computeWorkspaceKpiSnapshot(samples, "brand");

console.log("Row count:", audit.rowCount);
console.log("Owner awaiting brand:", owner.awaitingCounterparty, "| Brand needs action:", brand.needsAction);
console.log("Owner action:", owner.needsAction, "| Brand awaiting owner:", brand.awaitingCounterparty);
console.log("Stuck/at risk:", owner.atRisk, brand.atRisk);
console.log("Pipeline partition:", audit.violations.find((v) => v.code === "pipeline-partition") || "ok");
console.log("Mirror OK:", audit.ok);
if (!audit.ok) {
  console.error(audit.violations);
  process.exit(1);
}

const e = enrichWorkspaceRow(samples[1]);
console.log("Accepted bucket:", e.workspaceBucket, "owner action:", e.ownerNextAction, "brand action:", e.brandNextAction);
console.log("All mirror checks passed.");
