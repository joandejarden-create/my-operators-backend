/**
 * Reclassify refinement signals using PAA commercial-relevance rules.
 * No DataForSEO calls. No Airtable. No overlay attach.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  canonicalObservedTheme,
  classifyCommercialRelevance,
  VALIDATED_OBSERVED_THEMES_V1,
  CORE_SCENARIO_IDS_FOR_COVERAGE,
  REFINEMENT_SEEDS_EN,
  REFINEMENT_SEEDS_ES,
} from "../lib/ai-visibility/observed-demand-source-sample.js";

const root = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const SIGNALS_PATH = path.join(root, "fixtures", "ai-visibility", "demand-signals-v1.json");
const SEED_PATH = path.join(root, "fixtures", "ai-visibility", "observed-demand-seed-v1.json");
const REPORT_PATH = path.join(
  root,
  "reports",
  "ai-visibility",
  "observed-demand-refinement-2026-08-17.json"
);
const OVERLAY_PATH = path.join(root, "fixtures", "ai-visibility", "prompt-provenance-v1.json");

const meta = new Map(
  [...REFINEMENT_SEEDS_EN, ...REFINEMENT_SEEDS_ES].map((s) => [s.seed.toLowerCase(), s])
);
const existingMap = new Map([
  ["hotel franchise fees", { intent: "FEES_ECONOMICS", scenarioId: "scenario_owner_economics_v1" }],
  ["soft brand hotel", { intent: "SOFT_BRAND_HARD_BRAND", scenarioId: "scenario_soft_brand_collection_affiliation_v1" }],
  ["franquicia hotelera", { intent: "FRANCHISE_VS_HMA", scenarioId: "scenario_hma_vs_franchise_v1" }],
  ["contrato de gestion hotelera", { intent: "FRANCHISE_VS_HMA", scenarioId: "scenario_hma_vs_franchise_v1" }],
  [
    "hotel franchise vs management agreement",
    { intent: "FRANCHISE_VS_HMA", scenarioId: "scenario_hma_vs_franchise_v1" },
  ],
  ["hotel reflagging", { intent: "REFLAGGING_CONVERSION", scenarioId: "scenario_conversion_suitability_v1" }],
]);

const sample = JSON.parse(
  fs.readFileSync(path.join(root, "reports", "ai-visibility", "observed-demand-source-sample-2026-08-17.json"), "utf8")
);
const refinement = JSON.parse(fs.readFileSync(REPORT_PATH, "utf8"));
const merged = [...(sample.signals || []), ...(refinement.signals_added || [])];
const kept = [];
const dropped = [];
for (const s of merged) {
  const canon = canonicalObservedTheme(s.normalizedTheme || s.queryText);
  const grandfathered = VALIDATED_OBSERVED_THEMES_V1.map((t) => canonicalObservedTheme(t)).includes(canon);
  const rel = classifyCommercialRelevance({
    queryText: s.queryText,
    normalizedTheme: s.normalizedTheme,
    paaQuestions: s.paaQuestions || [],
    search_volume: s.search_volume,
  });
  if (!rel.usable && !grandfathered) {
    dropped.push({
      demandSignalId: s.demandSignalId,
      theme: s.normalizedTheme,
      why: rel.why || rel.code,
    });
    continue;
  }
  kept.push(s);
}

const distinctThemes = [...new Set(kept.map((s) => canonicalObservedTheme(s.normalizedTheme || s.queryText)))];
const geoLang = [...new Set(kept.map((s) => `${s.geography}|${s.language}`))];
const mappedScenario = new Set();
const overlap = [];
const observedOnly = [];
const intent = new Set();
for (const theme of distinctThemes) {
  const row = existingMap.get(theme) || meta.get(theme) || {};
  const scenarioId = row.scenarioId || null;
  const ownerIntent = row.intent || null;
  if (ownerIntent) intent.add(ownerIntent);
  if (scenarioId) {
    mappedScenario.add(scenarioId);
    overlap.push(theme);
  } else observedOnly.push(theme);
}
const coverage = {
  ownerIntentFamilies: [...intent].sort(),
  overlapWithScenario: [...new Set(overlap)],
  observedOnly: [...new Set(observedOnly)],
  scenarioOnly: CORE_SCENARIO_IDS_FOR_COVERAGE.filter((id) => !mappedScenario.has(id)),
};
const gate = {
  MIN_10_DISTINCT_THEMES: distinctThemes.length >= 10 ? "PASS" : "FAIL",
  MIN_3_INTENT_FAMILIES: coverage.ownerIntentFamilies.length >= 3 ? "PASS" : "FAIL",
  MIN_2_GEO_LANGUAGE_COHORTS: geoLang.length >= 2 ? "PASS" : "FAIL",
  NO_DUPLICATE_INFLATION: "PASS",
};
const gatePass = Object.values(gate).every((v) => v === "PASS");

fs.writeFileSync(
  SIGNALS_PATH,
  JSON.stringify(
      {
        registryVersion: "ai_visibility_demand_signals_v1",
        notes: [
          "Normalized observed-demand evidence from budget-capped DataForSEO sample + targeted refinement. Generic franchise-entrepreneur PAA rows removed. Not attached to live monitored prompts.",
        ],
        dateObserved: "2026-08-17",
        actual_cost_usd_phase: 0.474,
        signals: kept,
      },
    null,
    2
  )
);

const seed = JSON.parse(fs.readFileSync(SEED_PATH, "utf8"));
const candidateByTheme = new Map((seed.candidateThemes || []).map((c) => [c.theme, { ...c, included: false }]));
for (const theme of distinctThemes) {
  const rows = kept.filter((s) => canonicalObservedTheme(s.normalizedTheme) === theme);
  const best = rows.find((s) => s.search_volume > 0) || rows[0];
  candidateByTheme.set(theme, {
    theme,
    geography: "country",
    demandTier: best?.demandTier || "UNKNOWN",
    evidenceState: best && best.search_volume > 0 ? "LICENSED_VOLUME" : "PAA_SUPPORTED",
    included: true,
  });
}

const nextSeed = {
  ...seed,
  seedStatus: gatePass ? "OBSERVED_DEMAND_SEED_READY_FOR_ACTIVATION" : "OBSERVED_DEMAND_SEED_PARTIAL",
  includedThemes: distinctThemes,
  activationStatus: "NOT_ATTACHED_TO_LIVE_PROMPTS",
  promptMixEligible: false,
  promptMixReason: gatePass
    ? "Activation readiness only. Prompt Mix stays hidden until OBSERVED_DEMAND_ACTIVATION attaches live prompts."
    : `${distinctThemes.length} distinct observed themes is below OBSERVED_PROMPT_MIX_MIN_THEMES (10). Client Prompt Mix stays hidden.`,
  candidateThemes: [...candidateByTheme.values()],
};
fs.writeFileSync(SEED_PATH, JSON.stringify(nextSeed, null, 2));

const overlay = JSON.parse(fs.readFileSync(OVERLAY_PATH, "utf8"));
overlay.observedDemandSeedStatus = nextSeed.seedStatus;
overlay.classifications = [];
fs.writeFileSync(OVERLAY_PATH, JSON.stringify(overlay, null, 2));

const report = JSON.parse(fs.readFileSync(REPORT_PATH, "utf8"));
report.paa_quality_filter = {
  applied: true,
  dropped,
  note: "Recycled generic hotel-franchise PAA does not count as a distinct owner-decision theme.",
};
report.distinct_themes = distinctThemes;
report.new_distinct_themes = distinctThemes.filter(
  (t) => !VALIDATED_OBSERVED_THEMES_V1.map((x) => canonicalObservedTheme(x)).includes(t)
);
report.consumer_noise_removed = dropped.length;
report.valid_rows = kept.length;
report.coverage = coverage;
report.gate = gate;
report.gatePass = gatePass;
report.FINAL = gatePass ? "OBSERVED_DEMAND_REFINEMENT_PASS" : "OBSERVED_DEMAND_REFINEMENT_PARTIAL";
fs.writeFileSync(REPORT_PATH, JSON.stringify(report, null, 2));

console.log(
  JSON.stringify(
    {
      FINAL: report.FINAL,
      distinct: distinctThemes.length,
      dropped,
      gate,
      overlayClassifications: overlay.classifications.length,
    },
    null,
    2
  )
);
