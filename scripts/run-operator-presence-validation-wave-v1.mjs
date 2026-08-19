#!/usr/bin/env node
/**
 * Operator AI Presence Validation Wave V1
 * --plan-only   preflight only (default)
 * --execute     live 84-call provider wave (requires AI_VISIBILITY_ENABLED + LIVE_TEST)
 * --analyze     re-score saved wave (--wave-id required unless latest)
 * --allow-duplicate  bypass duplicate-wave guard
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  preflightOperatorPresenceValidation,
  executeOperatorPresenceValidationWave,
  analyzeSavedOperatorWave,
  OPERATOR_RUNTIME_ROOT,
} from "../lib/ai-visibility/operator-intelligence/presence-validation-wave.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

const EXECUTE = process.argv.includes("--execute");
const ANALYZE = process.argv.includes("--analyze");
const ALLOW_DUP = process.argv.includes("--allow-duplicate");
const waveArg = process.argv.find((a) => a.startsWith("--wave-id="));
const waveIdArg = waveArg ? waveArg.split("=")[1] : null;

async function main() {
  if (ANALYZE) {
    const waveId =
      waveIdArg ||
      JSON.parse(
        fs.readFileSync(
          path.join(OPERATOR_RUNTIME_ROOT, "operator-presence-validation-completed.json"),
          "utf8"
        )
      ).waveId;
    const report = analyzeSavedOperatorWave(waveId);
    console.log(JSON.stringify(report, null, 2));
    return;
  }

  if (EXECUTE) {
    process.env.AI_VISIBILITY_ENABLED = process.env.AI_VISIBILITY_ENABLED || "true";
    process.env.AI_VISIBILITY_LIVE_TEST = process.env.AI_VISIBILITY_LIVE_TEST || "true";
  }

  const preflight = preflightOperatorPresenceValidation({
    requireEnv: EXECUTE,
    allowDuplicate: ALLOW_DUP,
  });
  console.log(JSON.stringify({ phase: "PREFLIGHT", ...preflight }, null, 2));

  if (!EXECUTE) {
    console.log("\nPass --execute to run the live 84-call validation wave.");
    process.exit(preflight.ok ? 0 : 2);
  }

  if (!preflight.ok) {
    console.error("\nPreflight STOP — wave not executed.");
    process.exit(2);
  }

  console.log("\nStarting live operator presence validation wave (84 calls)...\n");
  const result = await executeOperatorPresenceValidationWave({
    execute: true,
    resume: true,
    allowDuplicate: ALLOW_DUP,
  });

  if (!result.ok) {
    console.error(JSON.stringify(result, null, 2));
    process.exit(result.phase === "DUPLICATE_SKIPPED" ? 0 : 2);
  }

  console.log(JSON.stringify(result.report, null, 2));
  console.log(`\nReport: reports/ai-visibility/operator-presence-validation-wave-v1.json`);
  console.log(`Runtime: data/ai-visibility/runtime/operator/${result.waveId}/`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
