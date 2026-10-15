#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-priority-action-window-parity");
const sim = JSON.parse(
  fs.readFileSync(path.join(OUT, "simulation-report.json"), "utf8")
);

function esc(v) {
  const s = String(v ?? "");
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

const rows = [];
for (const [key, h] of Object.entries(sim.hotels)) {
  for (const r of h.rowsAfter) {
    const before =
      h.rowsBefore.find((b) => b.opportunityId === r.opportunityId) || {};
    rows.push({
      hotel: key,
      opportunityId: r.opportunityId,
      researchOrigin: r.researchOrigin,
      displayedPriorityBefore: before.displayedPriority,
      displayedPriorityAfter: r.displayedPriority,
      displayedActionBefore: before.displayedActionWindow,
      displayedActionAfter: r.displayedActionWindow,
      commercialClass: r.commercialClass,
      sourcingStatus: r.sourcingStatus,
      occasionDates: r.occasionDates,
      contactUsable: r.contactUsable,
      fitEvidence: r.fitEvidence,
      assignmentRuleVersion: r.assignmentRuleVersion,
      priorityReason: r.priorityReason,
      bookingWindowReason: r.bookingWindowReason,
    });
  }
}
const headers = Object.keys(rows[0]);
const csv = [headers.join(",")]
  .concat(rows.map((r) => headers.map((h) => esc(r[h])).join(",")))
  .join("\n");
fs.writeFileSync(path.join(OUT, "reconciliation-rows.csv"), csv + "\n");
console.log("rows", rows.length);
