#!/usr/bin/env node
import { runActiveUniverseBackscan } from "../lib/brand-explorer/canonical/backscan.js";

const report = await runActiveUniverseBackscan();
const summary = {
  ok: report.ok,
  writes: report.writes,
  activeCount: report.activeCount,
  byStatus: report.byStatus,
  cleanCount: report.cleanCount,
  failingCount: report.failingCount,
  defectCounts: report.defectCounts,
  imageEvidenceDistribution: report.imageEvidenceDistribution,
  probes: report.probes,
  reason: report.reason || null,
  note: report.note || null,
};
console.log(JSON.stringify(summary, null, 2));
process.exit(report.ok ? 0 : 1);
