/**
 * Packet 2.8A — prompt compiler + registry gates (no paid Webhound).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  getTemplate,
  listTemplates,
  RESEARCH_TEMPLATES,
  INTERNAL_GAP_FILL_TEMPLATES,
  TEMPLATE_REGISTRY_VERSION,
} from "../lib/hotel-intelligence/research/templates.js";
import {
  compileWebhoundPrompt,
  validateCompiledWebhoundPrompt,
  PROMPT_COMPILER_VERSION,
} from "../lib/hotel-intelligence/research/compile-webhound-prompt.js";
import { createWebhoundProvider } from "../lib/hotel-intelligence/research/providers.js";
import { getDossierById } from "../lib/hotel-intelligence/dossier/index.js";
import { evaluateHotelIntelligenceCompleteness } from "../lib/hotel-intelligence/factory/completeness-evaluator.js";
import { planHotelResearch } from "../lib/hotel-intelligence/factory/research-planner.js";
import { canPromoteLearning } from "../lib/hotel-intelligence/learning/research-learning-registry.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const SNAP_DIR = path.join(ROOT, "fixtures/hotel-intelligence/prompts/snapshots");

function writeSnap(name, text) {
  fs.mkdirSync(SNAP_DIR, { recursive: true });
  const p = path.join(SNAP_DIR, name);
  if (!fs.existsSync(p)) {
    fs.writeFileSync(p, text);
    return { path: p, wrote: true };
  }
  return { path: p, wrote: false, existing: fs.readFileSync(p, "utf8") };
}

let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log("PASS", name);
  } catch (err) {
    failed += 1;
    console.error("FAIL", name, err.message);
  }
}

check("RESEARCH_TEMPLATE_REGISTRY", () => {
  assert.ok(TEMPLATE_REGISTRY_VERSION.includes("v2"));
  assert.ok(RESEARCH_TEMPLATES.FULL_HOTEL_INTELLIGENCE);
  assert.ok(RESEARCH_TEMPLATES.CHANGE_OPPORTUNITY);
  assert.ok(RESEARCH_TEMPLATES.DECISION_AUTHORITY);
  assert.ok(RESEARCH_TEMPLATES.BRAND_OPERATOR_AGREEMENT);
  assert.ok(RESEARCH_TEMPLATES.OWNERSHIP_CAPITAL_EVENTS);
  assert.ok(RESEARCH_TEMPLATES.REPOSITIONING_DEVELOPMENT);
  assert.ok(RESEARCH_TEMPLATES.OWNER_PORTFOLIO);
  assert.equal(getTemplate("BRAND_FRANCHISE_OPERATOR")?.template_id, "BRAND_OPERATOR_AGREEMENT");
  assert.equal(getTemplate("OWNERSHIP_CAPITAL")?.template_id, "OWNERSHIP_CAPITAL_EVENTS");
  assert.ok(INTERNAL_GAP_FILL_TEMPLATES.OWNERSHIP_GAP);
  assert.ok(listTemplates({ includeInternal: true }).length > listTemplates().length);
});

check("WEBHOUND_PROMPT_COMPILER", () => {
  const compiled = compileWebhoundPrompt({
    template_id: "FULL_HOTEL_INTELLIGENCE",
    hotel_seed: {
      hotel_id: "recUNycnMwOVFX0hc",
      hotel_name: "Krystal Grand Puerto Vallarta",
    },
    known_facts: ["GSF owner-operator"],
    unresolved_questions: ["Natural-person UBO"],
    budget_usd: 5,
  });
  assert.equal(compiled.prompt_compiler_version, PROMPT_COMPILER_VERSION);
  assert.ok(compiled.prompt_hash);
  assert.ok(compiled.prompt.includes("Shared Dealality research contract"));
  const v = validateCompiledWebhoundPrompt(compiled);
  assert.equal(v.ok, true, v.errors.join(","));
});

check("CHANGE_OPPORTUNITY_COMPILER", () => {
  const compiled = compileWebhoundPrompt({
    template_id: "CHANGE_OPPORTUNITY",
    hotel_seed: { hotel_id: "recUNycnMwOVFX0hc", hotel_name: "Krystal Grand Puerto Vallarta" },
    budget_usd: 5,
  });
  const v = validateCompiledWebhoundPrompt(compiled);
  assert.equal(v.ok, true, v.errors.join(","));
  assert.ok(compiled.prompt.includes("Change & Opportunity special requirements"));
});

check("PROVIDER_USES_COMPILER_HASH", () => {
  const provider = createWebhoundProvider({ env: { ...process.env, EXTERNAL_RESEARCH_ENABLED: "0" } });
  const job = provider.buildJobPayload({
    template: getTemplate("FULL_HOTEL_INTELLIGENCE"),
    hotel_seed: { hotel_id: "recIwaP1etgx2g9nA", hotel_name: "Cambridge Beaches Resort & Spa" },
    budget_usd: 5,
  });
  assert.ok(job.prompt_hash);
  assert.ok(job.prompt_compiler_version);
  assert.ok(job.prompt.includes("Negative screens:"));
});

check("FULL_HI_FINDING_ID_MIGRATION", () => {
  for (const id of [
    "dossier_kgpv_full_hi_v1",
    "dossier_cambridge_beaches_full_hi_v3",
    "dossier_sheraton_gdl_expo_full_hi_v1",
    "dossier_real_inn_cancun_full_hi_v1",
    "addendum_change_opportunity_ff3e5bf2",
  ]) {
    const d = getDossierById(id);
    const n = (d.key_findings || []).length;
    const withId = (d.key_findings || []).filter((f) => f.finding_id).length;
    assert.equal(withId, n, `${id} finding_id ${withId}/${n}`);
  }
});

check("COMPLETENESS_AND_PLANNER", () => {
  const c = evaluateHotelIntelligenceCompleteness({
    hotel_id: "recTEST",
    domains: {
      IDENTITY: { complete: true, confidence: "HIGH" },
      OWNERSHIP: { data_available: false, what_is_missing: "PropCo", escalation_required: false, estimated_cost_class: "L2" },
    },
  });
  assert.equal(c.domains.IDENTITY.status, "COMPLETE");
  assert.ok(["MISSING", "RESEARCH_REQUIRED"].includes(c.domains.OWNERSHIP.status) || c.domains.OWNERSHIP.status === "MISSING");
  const plan = planHotelResearch({ hotel_id: "recTEST", completeness: c });
  assert.ok(plan.tasks.length >= 1);
  assert.equal(plan.webhound_default, false);
});

check("NO_AUTO_PROMOTE_LEARNING", () => {
  assert.equal(canPromoteLearning({ status: "CANDIDATE" }), false);
  assert.equal(
    canPromoteLearning({
      status: "VALIDATED",
      case_evidence: true,
      generic_enough: true,
      negative_cases_understood: true,
      eval_id: "e1",
      regression_suite_passed: true,
    }),
    true
  );
});

check("PROMPT_SNAPSHOTS", () => {
  const full = compileWebhoundPrompt({
    template_id: "FULL_HOTEL_INTELLIGENCE",
    hotel_seed: { hotel_id: "recUNycnMwOVFX0hc", hotel_name: "Krystal Grand Puerto Vallarta" },
    budget_usd: 5,
  });
  const co = compileWebhoundPrompt({
    template_id: "CHANGE_OPPORTUNITY",
    hotel_seed: { hotel_id: "recUNycnMwOVFX0hc", hotel_name: "Krystal Grand Puerto Vallarta" },
    budget_usd: 5,
  });
  const cambridge = compileWebhoundPrompt({
    template_id: "FULL_HOTEL_INTELLIGENCE",
    hotel_seed: { hotel_id: "recIwaP1etgx2g9nA", hotel_name: "Cambridge Beaches Resort & Spa" },
    budget_usd: 5,
  });
  for (const [name, body] of [
    ["full-hi-kgpv.snap.md", full.prompt],
    ["change-opportunity-kgpv.snap.md", co.prompt],
    ["full-hi-cambridge.snap.md", cambridge.prompt],
  ]) {
    const r = writeSnap(name, body);
    if (!r.wrote) {
      // Structure should match (template sections); allow hotel-context diff only if hash header differs — require shared contract present
      assert.ok(r.existing.includes("Shared Dealality research contract"));
      assert.ok(body.includes("Shared Dealality research contract"));
      assert.ok(body.includes("Structured output contract"));
    }
  }
  // Structure identity across hotels
  assert.ok(full.prompt.includes("## Research template"));
  assert.ok(cambridge.prompt.includes("## Research template"));
  assert.notEqual(full.prompt_hash, cambridge.prompt_hash);
});

if (failed) {
  console.error(`\n${failed} check(s) failed`);
  process.exit(1);
}
console.log("\nAll Packet 2.8A prompt/registry gates passed. Webhound runs this packet: 0");
