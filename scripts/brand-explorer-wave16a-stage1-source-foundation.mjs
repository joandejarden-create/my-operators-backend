#!/usr/bin/env node
/**
 * Wave 16A Stage 1 — source foundation (read-only local artifacts).
 * Zero Airtable writes.
 */
import "../load-env.js";
import { runWave16aStage1SourceFoundation } from "../lib/partner-intelligence/brand-explorer-wave16a-stage1-source-foundation.js";

const argv = process.argv.slice(2);
const report = await runWave16aStage1SourceFoundation({ argv });
const code =
  report.readyStatement === "wave16a_stage1_blocked"
    ? 3
    : report.readyStatement === "wave16a_stage1_partial_ready_subset_identified"
      ? 0
      : 0;
process.exit(code);
