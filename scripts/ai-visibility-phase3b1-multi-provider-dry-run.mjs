#!/usr/bin/env node
/**
 * Phase 3B.1 — Multi-provider dry-run report (no live calls).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { buildWave1ShowcaseDryRunPlan } from "../lib/ai-visibility/wave1-showcase-plan.js";
import {
  buildMultiProviderDryRunReport,
  auditSemanticPromptParity,
  validateFingerprintProviderIsolation,
  findSampleExecution,
} from "../lib/ai-visibility/providers/multi-provider-dry-run.js";
import { buildControlledValidationPlan } from "../lib/ai-visibility/providers/validation-plan.js";
import { auditRawTextRepair } from "../lib/ai-visibility/providers/raw-text-repair.js";
import { PROVIDER_CAPABILITY_MATRIX } from "../lib/ai-visibility/providers/provider-capabilities.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");
const outPath = path.join(root, "data/ai-visibility/phase3b1-multi-provider-dry-run.json");

const plan = buildWave1ShowcaseDryRunPlan();
const dryRun = buildMultiProviderDryRunReport();
const parity = auditSemanticPromptParity(plan);
const sampleExec = findSampleExecution(plan, {
  country: "MEXICO",
  language: "es",
  intent: "Conversion",
  promptFamily: "showcase_conversion_existing_asset_reposition",
});
const fingerprintIsolation = validateFingerprintProviderIsolation(sampleExec);
const validationPlan = buildControlledValidationPlan();
const rawTextAudit = auditRawTextRepair();

const report = {
  phase: "BRAND_AI_VISIBILITY_PHASE_3B1_MULTI_PROVIDER_ADAPTER_FOUNDATION",
  generatedAt: new Date().toISOString(),
  BUILD_STATUS: dryRun.TOTAL_BUILDABLE === 252 ? "PASS" : "BLOCKED",
  LIVE_PROVIDER_CALLS: 0,
  dryRun,
  semanticPromptParity: parity,
  fingerprintIsolation: {
    samplePromptId: sampleExec.promptId,
    ...fingerprintIsolation,
  },
  providerCapabilityMatrix: PROVIDER_CAPABILITY_MATRIX,
  controlledValidationPlan: validationPlan,
  rawTextRepair: rawTextAudit,
};

fs.mkdirSync(path.dirname(outPath), { recursive: true });
fs.writeFileSync(outPath, JSON.stringify(report, null, 2));

console.log(JSON.stringify({
  BUILD_STATUS: report.BUILD_STATUS,
  GEMINI_BUILDABLE: dryRun.GEMINI_BUILDABLE,
  PERPLEXITY_BUILDABLE: dryRun.PERPLEXITY_BUILDABLE,
  CLAUDE_BUILDABLE: dryRun.CLAUDE_BUILDABLE,
  TOTAL_BUILDABLE: dryRun.TOTAL_BUILDABLE,
  SEMANTIC_PROMPT_PARITY: parity.SEMANTIC_PROMPT_PARITY,
  FINGERPRINT_ISOLATION: fingerprintIsolation.valid,
  RAW_TEXT_REPAIR_SAFE: rawTextAudit.RAW_TEXT_REPAIR_SAFE,
  output: outPath,
}, null, 2));

if (report.BUILD_STATUS !== "PASS") process.exit(1);
