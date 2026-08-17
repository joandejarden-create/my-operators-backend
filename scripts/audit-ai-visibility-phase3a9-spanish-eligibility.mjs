#!/usr/bin/env node
/**
 * Phase 3A.9 — Spanish QA + eligibility mapping reports (no provider / no Airtable writes).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { listEligibilityByTerritory } from "../lib/ai-visibility/brand-decision-eligibility.js";
import { validateSemanticPairMembers } from "../lib/ai-visibility/semantic-pair.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const seed = JSON.parse(
  fs.readFileSync(path.join(ROOT, "fixtures/ai-visibility/phase3a9-showcase-prompt-seed.json"), "utf8")
);

const FAIL_PATTERNS = [
  { id: "mixed_use_leak", re: /\bmixed[\s-]?use\b|\buso mixto\b/i },
  { id: "new_build_leak", re: /\bnew[\s-]?build\b|\bnueva construcción\b|\bnuevo desarrollo hotelero\b/i },
  { id: "eligibility_in_prompt", re: /\beligib|\bsuitabilit/i },
  { id: "leading_brand", re: /\b(?:marriott|hilton|choice|autograph|curio|ascend|kimpton)\b/i },
  { id: "traveler_frame", re: /\b(?:best hotel to stay|where should i book|vacation package)\b/i },
  { id: "spain_only", re: /\b(?:vosotros|península ibérica|comunidad autónoma)\b/i },
];

const esPrompts = seed.prompts.filter((p) => p.language === "es");
const byId = Object.fromEntries(seed.prompts.map((p) => [p.promptId, p]));

const spanishQa = esPrompts.map((p) => {
  const fails = FAIL_PATTERNS.filter((f) => f.re.test(p.promptText)).map((f) => f.id);
  const pair = seed.semanticPairs.find((x) => x.esPromptId === p.promptId);
  const en = pair ? byId[pair.enPromptId] : null;
  const pairOk = en ? validateSemanticPairMembers(en, p).ok : false;
  return {
    PROMPT_ID: p.promptId,
    INTENT: p.intentTerritory,
    GEOGRAPHY: p.country || p.commercialRegion,
    SEMANTIC_PAIR_ID: p.semanticPairId,
    PAIR_OK: pairOk,
    NATURAL_CALA_CHECK: !fails.includes("spain_only"),
    FAIL_CODES: fails,
    RESULT: fails.length === 0 && pairOk ? "PASS" : "FAIL",
  };
});

const intents = [...new Set(seed.prompts.map((p) => p.intentTerritory))];
const eligibilityMap = intents.map((intent) => {
  const rows = listEligibilityByTerritory(intent);
  return {
    INTENT: intent,
    ELIGIBLE: rows.ELIGIBLE.map((r) => r.brandName),
    NOT_ELIGIBLE: rows.NOT_ELIGIBLE.map((r) => r.brandName),
    UNKNOWN: rows.UNKNOWN.map((r) => r.brandName),
  };
});

const outDir = path.join(ROOT, "data/ai-visibility");
fs.writeFileSync(
  path.join(outDir, "phase3a9-spanish-qa.json"),
  JSON.stringify(
    {
      generatedAt: new Date().toISOString(),
      TOTAL: spanishQa.length,
      PASS: spanishQa.filter((r) => r.RESULT === "PASS").length,
      FAIL: spanishQa.filter((r) => r.RESULT === "FAIL").length,
      rows: spanishQa,
    },
    null,
    2
  ) + "\n"
);
fs.writeFileSync(
  path.join(outDir, "phase3a9-eligibility-mapping.json"),
  JSON.stringify({ generatedAt: new Date().toISOString(), byIntent: eligibilityMap }, null, 2) + "\n"
);

console.log(
  JSON.stringify(
    {
      spanishPass: spanishQa.filter((r) => r.RESULT === "PASS").length,
      spanishFail: spanishQa.filter((r) => r.RESULT === "FAIL").length,
      intents: eligibilityMap.map((i) => ({
        intent: i.INTENT,
        E: i.ELIGIBLE.length,
        N: i.NOT_ELIGIBLE.length,
        U: i.UNKNOWN.length,
      })),
    },
    null,
    2
  )
);
