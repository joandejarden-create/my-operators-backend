#!/usr/bin/env node
/**
 * Validate operator profile archetypes fixture and profile alignment utility.
 *   node scripts/validate-operator-profile-archetypes.mjs
 */
import { readFileSync } from "fs";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import {
  loadOperatorProfileArchetypes,
  evaluateOperatorProfilesForReview,
  buildOperatorAlignmentProfileSnapshot,
  ALIGNMENT_BANDS,
  ARCHETYPES_PATH,
} from "../lib/operator-alignment-profile-utils.js";
import { containsForbiddenOperatorLanguage } from "../lib/operator-capability-narrative.js";

const REQUIRED_KEYS = [
  "regional_cala_full_service",
  "international_third_party_market_presence",
  "lifestyle_boutique_operator",
  "owner_operated_commercial_support",
  "brand_managed_structure",
];

const REQUIRED_ARCHETYPE_FIELDS = [
  "key",
  "displayLabel",
  "shortLabel",
  "description",
  "bestUseCase",
  "legacyProfileOptions",
  "requiredDealSignals",
  "positiveDealSignals",
  "negativeOrConditionalDealSignals",
  "alignmentSignals",
  "reviewConsiderations",
  "questionsToClarify",
  "dataGaps",
  "suggestedWorkflowActions",
  "defaultAlignmentBand",
  "sortPriority",
];

const ADVISORY_PATTERNS = [
  /\bdealality recommends\b/i,
  /\bbest operator\b/i,
  /\brecommended operator\b/i,
  /\bshould select\b/i,
  /\bwe advise\b/i,
  /\bthe owner should\b/i,
  /\bstrongest path\b/i,
  /\brecommended path\b/i,
  /\bbest fit\b/i,
  /\bbest match\b/i,
  /\bweak fit\b/i,
];

let failed = 0;

function assert(cond, msg) {
  if (!cond) {
    console.error("FAIL:", msg);
    failed += 1;
  } else {
    console.log("ok:", msg);
  }
}

function collectStrings(obj, out = []) {
  if (obj == null) return out;
  if (typeof obj === "string") {
    out.push(obj);
    return out;
  }
  if (Array.isArray(obj)) {
    for (const x of obj) collectStrings(x, out);
    return out;
  }
  if (typeof obj === "object") {
    for (const v of Object.values(obj)) collectStrings(v, out);
  }
  return out;
}

function testFixtureStructure() {
  const fixture = loadOperatorProfileArchetypes();
  assert(fixture.archetypes && fixture.archetypes.length === 5, "fixture has five archetypes");

  const keys = new Set(fixture.archetypes.map((a) => a.key));
  for (const k of REQUIRED_KEYS) {
    assert(keys.has(k), "required profile key present: " + k);
  }

  for (const arch of fixture.archetypes) {
    for (const f of REQUIRED_ARCHETYPE_FIELDS) {
      assert(arch[f] != null && arch[f] !== "", "archetype " + arch.key + " has field " + f);
    }
    assert(
      ALIGNMENT_BANDS.includes(arch.defaultAlignmentBand),
      "defaultAlignmentBand valid for " + arch.key
    );
  }

  const raw = readFileSync(ARCHETYPES_PATH, "utf8");
  assert(raw.includes("operator-profile-archetypes-v1"), "fixture version marker present");
}

function testNoAdvisoryLanguageInFixture() {
  const fixture = loadOperatorProfileArchetypes();
  const strings = collectStrings(fixture);
  for (const s of strings) {
    if (containsForbiddenOperatorLanguage(s)) {
      assert(false, "OCS forbidden phrase in fixture: " + s.slice(0, 80));
    }
    for (const re of ADVISORY_PATTERNS) {
      if (re.test(s)) {
        assert(false, "advisory pattern in fixture: " + re + " → " + s.slice(0, 80));
      }
    }
  }
  assert(true, "no advisory language in fixture strings");
}

function loadCalaSampleMergedFields() {
  const dir = dirname(fileURLToPath(import.meta.url));
  const path = join(dir, "..", "fixtures", "sample-deals", "aeropuerto-cancun-select-service.example.json");
  const sample = JSON.parse(readFileSync(path, "utf8"));
  const ref = sample.referenceProperty?.fields || {};
  const deal = sample.fictionalDeal?.fields || {};
  return { ...ref, ...deal };
}

function testSampleDealEvaluation() {
  const merged = loadCalaSampleMergedFields();
  const snapshot = buildOperatorAlignmentProfileSnapshot("recSAMPLE_CALA_CANCUN", merged);

  assert(snapshot.featureName === "Operator Alignment Snapshot", "featureName set");
  assert(snapshot.mode === "profile", "mode profile");
  assert(snapshot.profilesForReview.length === 5, "five profiles for review");
  assert(
    ["High", "Medium", "Low", "Insufficient Data"].includes(snapshot.operatorReviewSignal.level),
    "operator review signal level valid"
  );
  assert(snapshot.dealContext.country === "Mexico", "sample deal country Mexico");

  const cala = snapshot.profilesForReview.find((p) => p.profileKey === "regional_cala_full_service");
  assert(cala != null, "CALA profile present");
  assert(
    cala.alignmentBand !== "Insufficient Data" || cala.missingDealSignals.length > 0,
    "CALA profile band explainable"
  );

  console.log(
    "sample:",
    snapshot.operatorReviewSignal.level,
    "| bands:",
    snapshot.profilesForReview.map((p) => p.shortLabel + ":" + p.alignmentBand).join("; ")
  );
}

function testUtilityBands() {
  const profiles = evaluateOperatorProfilesForReview({});
  assert(profiles.length === 5, "empty deal still returns five profile shells");
  for (const p of profiles) {
    assert(ALIGNMENT_BANDS.includes(p.alignmentBand), "band valid: " + p.profileKey);
    assert(p.alignmentScoreOptional === null, "Phase 1 leaves score null for " + p.profileKey);
  }
}

testFixtureStructure();
testNoAdvisoryLanguageInFixture();
testUtilityBands();
testSampleDealEvaluation();

if (failed > 0) {
  console.error("\n" + failed + " validation failure(s)");
  process.exit(1);
}
console.log("\nAll operator profile archetype validations passed.");
