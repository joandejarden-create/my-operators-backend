#!/usr/bin/env node
/**
 * Bethesda Marriott — next factory actions from HE50 field-gap matrix.
 * Read-only. No Airtable writes. No paid research.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { classifyGapAction } from "../lib/hotel-intelligence/he-explorer-publication-gates-v1.js";
import { HE50_BETHESDA_HPC_ID } from "../lib/hotel-intelligence/he-5-0-full-e2e/contract.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const matrixPath = path.join(
  ROOT,
  "reports/hotel-explorer/he-5-0-pilot/HE50-FIELD-GAP-MATRIX.json",
);
const outPath = path.join(
  ROOT,
  "reports/hotel-explorer/he50-completion-v1/BETHESDA_GAP_ACTION_QUEUE_V1.json",
);

const matrix = JSON.parse(fs.readFileSync(matrixPath, "utf8"));
const rows = (matrix.rows || []).map((row) => {
  const plan = classifyGapAction(row);
  return {
    hotelId: HE50_BETHESDA_HPC_ID,
    tabId: row.tabId,
    fieldKey: row.fieldKey,
    label: row.label,
    classification: row.classification,
    uiConnected: row.uiConnected,
    notes: row.notes || "",
    ...plan,
  };
});

const priority = [
  "CONNECT_PERSIST_READBACK",
  "HUMAN_RECONCILE",
  "FREE_FIRST_THEN_PAID_GATE",
  "ACCEPT_GAP_OR_ESCALATE",
  "DISABLED_BY_FOUNDER",
  "SKIP_REUSE",
  "REVIEW",
];

const queue = rows
  .filter((r) => r.action !== "SKIP_REUSE")
  .sort(
    (a, b) =>
      priority.indexOf(a.action) - priority.indexOf(b.action) ||
      String(a.tabId).localeCompare(b.tabId) ||
      String(a.fieldKey).localeCompare(b.fieldKey),
  );

const report = {
  version: "he50-bethesda-gap-action-queue-v1",
  at: new Date().toISOString(),
  hotelId: HE50_BETHESDA_HPC_ID,
  hotelName: "Bethesda Marriott",
  sourceMatrix: "reports/hotel-explorer/he-5-0-pilot/HE50-FIELD-GAP-MATRIX.json",
  totals: {
    rows: rows.length,
    open: queue.length,
    connectPersist: queue.filter((q) => q.action === "CONNECT_PERSIST_READBACK").length,
    researchRequired: queue.filter((q) => q.action === "FREE_FIRST_THEN_PAID_GATE").length,
    conflicts: queue.filter((q) => q.action === "HUMAN_RECONCILE").length,
    disabledByFounder: queue.filter((q) => q.action === "DISABLED_BY_FOUNDER").length,
  },
  paidResearchSpendUsdThisRun: 0,
  applyAuthorized: false,
  queue,
};

fs.mkdirSync(path.dirname(outPath), { recursive: true });
fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
console.log(JSON.stringify({ ok: true, out: outPath, totals: report.totals }, null, 2));
