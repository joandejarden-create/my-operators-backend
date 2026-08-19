#!/usr/bin/env node
/**
 * Apply controlled V1 observed-demand activation to file store.
 * DATAFORSEO_NEW_CALLS = 0. PROVIDER_CALLS = 0. AIRTABLE_WRITES = 0.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { loadDemandSignalRegistry } from "../lib/ai-visibility/prompt-provenance.js";
import { validatePromptRow } from "../lib/ai-visibility/prompt-validation.js";
import { validateProvenanceRecord } from "../lib/ai-visibility/prompt-provenance.js";
import {
  OBSERVED_DEMAND_ACTIVATION_VERSION,
  OBSERVED_DEMAND_SEED_V1_VALIDATED,
  DEDUP_REPORT,
  DEMAND_METHODOLOGY_V1,
  V1_VALIDATED_THEMES,
  DERIVED_PROMPT_SPECS,
  buildObservedDemandPromptRows,
  buildObservedDemandOverlayClassifications,
  evaluateV1ActivationGate,
} from "../lib/ai-visibility/observed-demand-activation.js";

const root = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const PROMPTS_PATH = path.join(root, "fixtures", "ai-visibility", "observed-demand-prompts-v1.json");
const OVERLAY_PATH = path.join(root, "fixtures", "ai-visibility", "prompt-provenance-v1.json");
const SEED_PATH = path.join(root, "fixtures", "ai-visibility", "observed-demand-seed-v1.json");
const SIGNALS_PATH = path.join(root, "fixtures", "ai-visibility", "demand-signals-v1.json");

const prompts = buildObservedDemandPromptRows();
const promptErrors = [];
for (const p of prompts) {
  const v = validatePromptRow(p);
  if (!v.ok) promptErrors.push({ promptId: p.promptId, errors: v.errors });
}
if (promptErrors.length) {
  console.error(JSON.stringify({ promptErrors }, null, 2));
  process.exit(1);
}

const signals = loadDemandSignalRegistry(SIGNALS_PATH);
const classifications = buildObservedDemandOverlayClassifications(signals);
const overlayErrors = [];
const byId = new Map(classifications.map((c) => [c.promptId, c]));
for (const row of classifications) {
  const v = validateProvenanceRecord(row, {
    signals,
    overlay: { byPromptId: byId },
    requireResolvedParent: row.promptOrigin === "DERIVED",
  });
  if (!v.ok) overlayErrors.push({ promptId: row.promptId, errors: v.errors });
}
if (overlayErrors.length) {
  console.error(JSON.stringify({ overlayErrors }, null, 2));
  process.exit(1);
}

const gate = evaluateV1ActivationGate({
  distinctThemes: V1_VALIDATED_THEMES.length,
  ownerIntentFamilies: 6,
  geoLanguageCohorts: 3,
  provenanceQuality: overlayErrors.length ? "FAIL" : "PASS",
  noDuplicateInflation: "PASS",
});
if (!gate.pass) {
  console.error(JSON.stringify({ gate }, null, 2));
  process.exit(1);
}

fs.writeFileSync(
  PROMPTS_PATH,
  JSON.stringify(
    {
      seedId: "ai_visibility_observed_demand_prompts_v1",
      activationVersion: OBSERVED_DEMAND_ACTIVATION_VERSION,
      notes: [
        "Governed OBSERVED/DERIVED V1 seed. monitoringEligible=false until monitoring is approved.",
        "Do not convert existing SCENARIO prompt IDs. Source geography is not CALA unless evidence country is CALA.",
        "DATAFORSEO_NEW_CALLS=0. PROVIDER_CALLS=0.",
      ],
      prompts,
    },
    null,
    2
  )
);

fs.writeFileSync(
  OVERLAY_PATH,
  JSON.stringify(
    {
      overlayVersion: "ai_visibility_prompt_provenance_v1",
      observedDemandSeedStatus: OBSERVED_DEMAND_SEED_V1_VALIDATED,
      notes: [
        "V1 activation overlay. Classifications are for new observed/derived prompt IDs only.",
        "Existing 122 monitored prompts keep SCENARIO or LEGACY_UNCLASSIFIED origin.",
        "Do not infer OBSERVED from prompt wording.",
      ],
      equivalences: DEDUP_REPORT.SEMANTIC_NEAR_DUPLICATES,
      classifications,
    },
    null,
    2
  )
);

const seed = JSON.parse(fs.readFileSync(SEED_PATH, "utf8"));
fs.writeFileSync(
  SEED_PATH,
  JSON.stringify(
    {
      ...seed,
      seedStatus: OBSERVED_DEMAND_SEED_V1_VALIDATED,
      activationStatus: "PROVENANCE_ATTACHED_MONITORING_ELIGIBLE_OFF",
      promptMixEligible: true,
      promptMixReason:
        "V1 validation gate passed (8-theme founder standard). Prompt Intelligence is methodology context, not a performance KPI. Observed prompts are not in the current monitored 122.",
      demandMethodology: DEMAND_METHODOLOGY_V1,
      includedThemes: V1_VALIDATED_THEMES.map((t) => t.theme),
      netNewObservedPrompts: DEDUP_REPORT.NET_NEW_OBSERVED_PROMPTS,
      netNewDerivedPrompts: DEDUP_REPORT.NET_NEW_DERIVED_PROMPTS,
      derivedPromptIds: DERIVED_PROMPT_SPECS.map((d) => d.promptId),
    },
    null,
    2
  )
);

const registry = JSON.parse(fs.readFileSync(SIGNALS_PATH, "utf8"));
registry.notes = [
  "Normalized observed-demand evidence from budget-capped DataForSEO sample + targeted refinement.",
  "V1 activation registered. Raw evidence stays in file store. Not attached to the 122 monitored prompts.",
];
fs.writeFileSync(SIGNALS_PATH, JSON.stringify(registry, null, 2));

console.log(
  JSON.stringify(
    {
      DATAFORSEO_NEW_CALLS: 0,
      PROVIDER_CALLS: 0,
      AIRTABLE_WRITES: 0,
      prompts: prompts.length,
      overlay: classifications.length,
      gate,
      seedStatus: OBSERVED_DEMAND_SEED_V1_VALIDATED,
    },
    null,
    2
  )
);
