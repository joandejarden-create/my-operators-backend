#!/usr/bin/env node
/**
 * Phase 2D tests — schema gates, prompt validation, loader, cohorts, peer sets.
 * No paid provider calls. No Airtable writes in unit tests.
 */
import assert from "node:assert/strict";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  validatePromptRow,
  validatePromptSeedSet,
  resolvePromptUpsertAction,
  suggestNextPromptVersion,
  loadGovernedAiVisibilityPromptsFromFixture,
  buildPromptCohort,
  resolveCommercialRegionForCountry,
  validateCountryRegionPair,
  auditCommercialGeography,
  resolvePeerSetMembership,
  loadPeerSetConfig,
  HEADLINE_REGION_METRIC_COHORT_RULE,
  AI_VISIBILITY_PROMPTS_TABLE,
  AI_VISIBILITY_OPPORTUNITIES_TABLE,
} from "../lib/ai-visibility/index.js";
import {
  getPromptCoreFieldSpecs,
  getOpportunityCoreFieldSpecs,
  classifyFieldEnsureAction,
} from "../lib/ai-visibility/airtable-schema-proposal.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const FIXTURES = path.join(__dirname, "..", "fixtures", "ai-visibility");

let passed = 0;
let failed = 0;

function test(name, fn) {
  try {
    fn();
    passed += 1;
    console.log(`  PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`  FAIL ${name}: ${err.message}`);
  }
}

console.log("AI Visibility Phase 2D tests\n");

console.log("Schema proposal");
test("approved tables only", () => {
  assert.equal(AI_VISIBILITY_PROMPTS_TABLE, "AI Visibility - Prompts");
  assert.equal(AI_VISIBILITY_OPPORTUNITIES_TABLE, "AI Visibility - Opportunities");
});
test("prompt core fields include Commercial Region + Monitoring Eligible", () => {
  const names = getPromptCoreFieldSpecs().map((f) => f.name);
  assert.ok(names.includes("Commercial Region"));
  assert.ok(names.includes("Monitoring Eligible"));
  assert.ok(names.includes("Prompt Family"));
  assert.ok(names.includes("Geography Scope"));
});
test("opportunity fields include geography + interpretation status", () => {
  const names = getOpportunityCoreFieldSpecs().map((f) => f.name);
  assert.ok(names.includes("Commercial Region"));
  assert.ok(names.includes("Interpretation Status"));
  assert.ok(names.includes("Peer Set ID"));
});
test("classifyFieldEnsureAction skip/conflict/create", () => {
  assert.equal(classifyFieldEnsureAction(null, { name: "X", type: "singleLineText" }).action, "create");
  assert.equal(
    classifyFieldEnsureAction({ type: "singleLineText" }, { name: "X", type: "singleLineText" }).action,
    "skip"
  );
  assert.equal(
    classifyFieldEnsureAction({ type: "number" }, { name: "X", type: "singleLineText" }).action,
    "conflict"
  );
});
test("apply gate env contract", () => {
  // Gate: --apply requires AI_VISIBILITY_SCHEMA_APPLY=true (ensure script exits 2 otherwise).
  const gateOpen = (v) => String(v || "").toLowerCase() === "true";
  assert.equal(gateOpen(""), false);
  assert.equal(gateOpen("false"), false);
  assert.equal(gateOpen("true"), true);
});

console.log("\nCommercial geography");
test("Mexico → CALA", () => {
  const r = resolveCommercialRegionForCountry("Mexico");
  assert.equal(r.commercialRegion, "CALA");
  assert.equal(r.known, true);
});
test("Spain → Europe", () => {
  assert.equal(resolveCommercialRegionForCountry("Spain").commercialRegion, "Europe");
});
test("United States → North America; Mexico not NA", () => {
  assert.equal(resolveCommercialRegionForCountry("United States").commercialRegion, "North America");
  assert.equal(resolveCommercialRegionForCountry("Mexico").commercialRegion, "CALA");
});
test("unknown country stays unknown", () => {
  const r = resolveCommercialRegionForCountry("Atlantis");
  assert.equal(r.known, false);
  assert.equal(r.commercialRegion, null);
});
test("country-region mismatch detected", () => {
  const v = validateCountryRegionPair("Spain", "CALA");
  assert.equal(v.ok, false);
});
test("headline region rule is region-scope only", () => {
  assert.equal(HEADLINE_REGION_METRIC_COHORT_RULE, "region_scope_prompts_only_no_country_rollup");
  const audit = auditCommercialGeography();
  assert.ok(audit.EUROPE.countries.includes("Spain"));
});

console.log("\nPrompt validation");
const seed = JSON.parse(
  fs.readFileSync(path.join(FIXTURES, "phase2d-prompt-seed.json"), "utf8")
);
test("seed size 30–50", () => {
  assert.ok(seed.prompts.length >= 30 && seed.prompts.length <= 50);
});
test("seed validates", () => {
  const v = validatePromptSeedSet(seed.prompts);
  assert.equal(v.ok, true, v.errors.slice(0, 5).join("; "));
});
test("missing ID fails", () => {
  const v = validatePromptRow({ ...seed.prompts[0], promptId: "" });
  assert.equal(v.ok, false);
  assert.ok(v.errors.includes("missing_prompt_id"));
});
test("invalid geography fails", () => {
  const v = validatePromptRow({ ...seed.prompts[0], geographyScope: "Planet" });
  assert.equal(v.ok, false);
});
test("country-region mismatch fails", () => {
  const v = validatePromptRow({
    ...seed.prompts.find((p) => p.promptId === "p_mx_uu_conversion_brand_v1"),
    commercialRegion: "Europe",
  });
  assert.equal(v.ok, false);
});
test("non-neutral wording fails", () => {
  const v = validatePromptRow({
    ...seed.prompts[0],
    promptText: "Why is Curio Collection the best brand for a hotel conversion in Mexico?",
  });
  assert.equal(v.ok, false);
});
test("duplicate id+version fails seed set", () => {
  const dup = [...seed.prompts, seed.prompts[0]];
  const v = validatePromptSeedSet(dup);
  assert.equal(v.ok, false);
});

console.log("\nVersioning");
test("new version creates; identical matches; different text skip", () => {
  const map = new Map();
  const row = seed.prompts[0];
  assert.equal(resolvePromptUpsertAction(row, map).action, "create");
  map.set(`${row.promptId}::${row.version}`, { promptText: row.promptText });
  assert.equal(resolvePromptUpsertAction(row, map).action, "match");
  assert.equal(
    resolvePromptUpsertAction({ ...row, promptText: row.promptText + " (revised)" }, map).action,
    "skip_conflict"
  );
  assert.equal(suggestNextPromptVersion("1"), "2");
});

console.log("\nLoader + cohorts");
const loaded = loadGovernedAiVisibilityPromptsFromFixture({ activeOnly: true });
test("fixture loader returns valid prompts", () => {
  assert.ok(loaded.prompts.length >= 30);
  assert.equal(loaded.malformed.length, 0);
});
test("monitoring eligible filter", () => {
  const all = loadGovernedAiVisibilityPromptsFromFixture({ monitoringEligible: true });
  assert.ok(all.prompts.every((p) => p.monitoringEligible));
  const off = loadGovernedAiVisibilityPromptsFromFixture({ monitoringEligible: false });
  assert.ok(off.prompts.every((p) => !p.monitoringEligible));
});

const prompts = loaded.prompts;
const globalBrand = buildPromptCohort({
  prompts,
  geographyScope: "Global",
  entityScope: "Brand",
  monitoringEligible: true,
});
const calaBrand = buildPromptCohort({
  prompts,
  geographyScope: "Region",
  region: "CALA",
  entityScope: "Brand",
});
const europeBrand = buildPromptCohort({
  prompts,
  geographyScope: "Region",
  region: "Europe",
  entityScope: "Brand",
});
const naBrand = buildPromptCohort({
  prompts,
  geographyScope: "Region",
  region: "North America",
  entityScope: "Brand",
});
const mxBrand = buildPromptCohort({
  prompts,
  geographyScope: "Country",
  country: "Mexico",
  entityScope: "Brand",
});
const calaOp = buildPromptCohort({
  prompts,
  geographyScope: "Region",
  region: "CALA",
  entityScope: "Operator",
});
const europeOp = buildPromptCohort({
  prompts,
  geographyScope: "Region",
  region: "Europe",
  entityScope: "Operator",
});

test("Global cohort is Global-only", () => {
  assert.ok(globalBrand.count >= 1);
  assert.ok(globalBrand.members.every((m) => m.geographyScope === "Global"));
});
test("CALA headline excludes Mexico country rows", () => {
  assert.ok(calaBrand.members.every((m) => m.geographyScope === "Region"));
  assert.ok(calaBrand.members.every((m) => m.commercialRegion === "CALA"));
  assert.ok(!calaBrand.promptIds.some((id) => id.includes("_mx_")));
});
test("Europe / NA / CALA isolation", () => {
  const euIds = new Set(europeBrand.promptIds);
  const calaIds = new Set(calaBrand.promptIds);
  const naIds = new Set(naBrand.promptIds);
  for (const id of euIds) assert.ok(!calaIds.has(id) && !naIds.has(id));
  for (const id of calaIds) assert.ok(!naIds.has(id));
});
test("Mexico country isolation", () => {
  assert.ok(mxBrand.count >= 1);
  assert.ok(mxBrand.members.every((m) => m.country === "Mexico"));
});
test("operator cohorts exist", () => {
  assert.ok(calaOp.count >= 1);
  assert.ok(europeOp.count >= 1);
});
test("cohort fingerprint stable", () => {
  const a = buildPromptCohort({
    prompts,
    geographyScope: "Region",
    region: "CALA",
    entityScope: "Brand",
  });
  const b = buildPromptCohort({
    prompts,
    geographyScope: "Region",
    region: "CALA",
    entityScope: "Brand",
  });
  assert.equal(a.fingerprint, b.fingerprint);
});
test("country rollup opt-in differs from headline", () => {
  const pure = buildPromptCohort({
    prompts,
    geographyScope: "Region",
    region: "CALA",
    entityScope: "Brand",
    includeCountryRollup: false,
  });
  const rolled = buildPromptCohort({
    prompts,
    geographyScope: "Region",
    region: "CALA",
    entityScope: "Brand",
    includeCountryRollup: true,
  });
  assert.ok(rolled.count >= pure.count);
});

console.log("\nPeer sets");
test("global + regional override shape", () => {
  const cfg = loadPeerSetConfig();
  const base = resolvePeerSetMembership(
    { peerSetId: "peers_cala_operators_global_v1", commercialRegion: "Europe" },
    cfg
  );
  assert.equal(base.ok, true);
  assert.ok(base.overrideApplied === "Europe");
  assert.ok(!base.entityIds.includes("reciI2tYQBfMoMK9G"));
});

console.log("\nCohort sample counts");
console.log(
  JSON.stringify(
    {
      globalBrand: { count: globalBrand.count, fingerprint: globalBrand.fingerprint },
      calaBrand: { count: calaBrand.count, fingerprint: calaBrand.fingerprint },
      europeBrand: { count: europeBrand.count, fingerprint: europeBrand.fingerprint },
      naBrand: { count: naBrand.count, fingerprint: naBrand.fingerprint },
      mxBrand: { count: mxBrand.count, fingerprint: mxBrand.fingerprint },
      calaOp: { count: calaOp.count, fingerprint: calaOp.fingerprint },
      europeOp: { count: europeOp.count, fingerprint: europeOp.fingerprint },
    },
    null,
    2
  )
);

console.log(`\nResults: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
