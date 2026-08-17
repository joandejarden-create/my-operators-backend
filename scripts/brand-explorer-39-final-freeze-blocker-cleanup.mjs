#!/usr/bin/env node
import dotenv from "dotenv";
dotenv.config();
import { run39FinalFreezeBlockerCleanup } from "../lib/partner-intelligence/brand-explorer-39-final-freeze-blocker-cleanup.js";

const argv = process.argv.slice(2);
if (argv.includes("--dry-run") && argv.includes("--apply")) {
  console.error("Pass only one of --dry-run or --apply");
  process.exit(1);
}
const apply = argv.includes("--apply");

const report = await run39FinalFreezeBlockerCleanup({ apply, argv });

console.log(
  JSON.stringify(
    {
      version: report.version,
      applyPerformed: report.applyPerformed,
      writePerformed: report.writePerformed,
      plannedPatchCount: report.plannedPatchCount,
      readyStatement: report.readyStatement,
      brands: Object.fromEntries(
        Object.entries(report.brands || {}).map(([k, v]) => [
          k,
          {
            lane: v.lane,
            planned: v.plannedCount,
            applied: v.appliedCount,
            blocked: v.blockedCount,
            uniquenessBefore: v.uniquenessBefore || null,
          },
        ])
      ),
    },
    null,
    2
  )
);
