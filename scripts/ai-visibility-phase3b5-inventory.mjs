#!/usr/bin/env node
/** Phase 3B.5 preflight inventory only — no live calls. */
import { auditMissingBaselineFingerprints } from "../lib/ai-visibility/baseline-missing-fingerprints.js";
import { preflightProviderCredentials } from "../lib/ai-visibility/provider-credentials.js";

const inventory = auditMissingBaselineFingerprints();
const cred = preflightProviderCredentials();

console.log(
  JSON.stringify(
    {
      phase: "3B.5_INVENTORY",
      inventory: {
        OPENAI: { successful: inventory.OPENAI.successful, missing: inventory.OPENAI.missingCount },
        PERPLEXITY: {
          successful: inventory.PERPLEXITY.successful,
          missing: inventory.PERPLEXITY.missingCount,
        },
        GEMINI: {
          successful: inventory.GEMINI.successful,
          missing: inventory.GEMINI.missingCount,
          fingerprints: inventory.GEMINI.missing?.map((m) => m.fingerprint),
        },
        CLAUDE: {
          successful: inventory.CLAUDE.successful,
          missing: inventory.CLAUDE.missingCount,
        },
        TOTAL_SUCCESSFUL: inventory.TOTAL_SUCCESSFUL,
        TOTAL_MISSING: inventory.TOTAL_MISSING,
        RECONCILES_TO_12: inventory.RECONCILES_TO_12,
        inventoryValid: inventory.inventoryValid,
      },
      GEMINI: cred.GEMINI_CREDENTIAL,
      CLAUDE: cred.CLAUDE_CREDENTIAL,
    },
    null,
    2
  )
);

process.exit(inventory.inventoryValid ? 0 : 1);
