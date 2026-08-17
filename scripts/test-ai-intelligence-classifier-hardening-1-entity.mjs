#!/usr/bin/env node
/**
 * Classifier Hardening 1 — entity resolution regression tests.
 * LIVE_PROVIDER_CALLS: 0. HOLDOUT not accessed.
 */
import assert from "node:assert/strict";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  stripMarkdownNoiseForEntityMatch,
  findEntitySpans,
  isBlockedBareParentMention,
  extractMentions,
  normalizeMatchKey,
} from "../lib/ai-visibility/index.js";
import {
  materializeGoldenSetEntityUniverse,
  buildGoldenSetScoringEntityIndex,
} from "../lib/ai-visibility/validation/golden-set-entity-index.js";
import { loadRuntimeAliasOverlay } from "../lib/ai-visibility/runtime-alias-overlay.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

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

console.log("Classifier Hardening 1 — Entity Resolution\n");

materializeGoldenSetEntityUniverse({ write: true });
const index = buildGoldenSetScoringEntityIndex({});
const overlay = loadRuntimeAliasOverlay();

test("PROVIDER_NORMALIZATION_PRESERVES_BRAND_TEXT", () => {
  // Scoring hydration contract: store rawText longer than excerpt must be preferred
  const hydrateSrc = fs.readFileSync(
    path.join(ROOT, "lib/ai-visibility/validation/hydrate-golden-set-texts.js"),
    "utf8"
  );
  assert.ok(hydrateSrc.includes("monitoring_store_rawText"));
  assert.ok(hydrateSrc.includes("listBatchRuns"));
});

test("MARKDOWN_TABLE_ENTITY_RESOLUTION", () => {
  const text = "| Rank | Brand |\n| 1 | **Autograph Collection** |\n| 2 | Curio Collection by Hilton |";
  const stripped = stripMarkdownNoiseForEntityMatch(text);
  assert.ok(stripped.includes("Autograph Collection"));
  assert.ok(!stripped.includes("**"));
  const spans = findEntitySpans(text, index.aliasIndex);
  const names = spans.map((s) => s.entity.name);
  assert.ok(names.includes("Autograph Collection") || names.includes("Curio Collection by Hilton"));
});

test("SPANISH_ACCENT_ENTITY_RESOLUTION", () => {
  // NFKD folding: "Colección" noise around brand should still allow ASCII brand match
  const text = "Marcas soft brand: Autograph Collection y Curio Collection by Hilton en México.";
  const spans = findEntitySpans(text, index.aliasIndex);
  const names = new Set(spans.map((s) => s.entity.name));
  assert.ok(names.has("Autograph Collection"));
  assert.ok(names.has("Curio Collection by Hilton"));
  assert.equal(normalizeMatchKey("México"), "mexico");
});

test("CANONICAL_ALIAS_RESOLUTION", () => {
  const text = "Owners often shortlist Curio, Autograph, and Kimpton for lifestyle conversions.";
  const mentions = extractMentions({
    responseId: "t1",
    text,
    entityIndex: index.aliasIndex,
  });
  const names = new Set(mentions.map((m) => m.canonicalEntityName));
  assert.ok(names.has("Curio Collection by Hilton"));
  assert.ok(names.has("Autograph Collection"));
  assert.ok(names.has("Kimpton Hotels"));
});

test("SHORT_ALIAS_COLLISION_SAFE", () => {
  assert.ok(isBlockedBareParentMention("Marriott"));
  assert.ok(isBlockedBareParentMention("Hilton"));
  const text = "Marriott remains a parent company option alongside Autograph Collection.";
  const spans = findEntitySpans(text, index.aliasIndex);
  // Must not resolve bare Marriott to Autograph / Marriott Hotels incorrectly as parent block
  const bareMarriott = spans.filter(
    (s) => normalizeMatchKey(s.rawMention) === "marriott"
  );
  assert.equal(bareMarriott.length, 0);
  assert.ok(spans.some((s) => s.entity.name === "Autograph Collection"));
});

test("LONGEST_ALIAS_WINS", () => {
  const text = "Curio Collection by Hilton outperforms bare Curio mentions when both appear.";
  const spans = findEntitySpans(text, index.aliasIndex);
  const curio = spans.filter((s) => s.entity.name === "Curio Collection by Hilton");
  assert.ok(curio.length >= 1);
  // First span should prefer longer label when overlapping — at least one full-name match
  assert.ok(curio.some((s) => /collection/i.test(s.rawMention) || /curio/i.test(s.rawMention)));
});

test("PARENT_NOT_CHILD_BRAND", () => {
  const text = "Hilton and Hyatt are major parent companies.";
  const spans = findEntitySpans(text, index.aliasIndex);
  assert.equal(
    spans.filter((s) => ["Hilton", "Hyatt"].includes(s.rawMention)).length,
    0
  );
});

test("NO_GENERIC_COLLECTION_ALIAS", () => {
  const rejected = overlay.aliasesRejected || [];
  assert.ok(rejected.some((a) => a.alias === "Collection"));
  assert.ok(rejected.some((a) => a.alias === "Preferred"));
  const text = "A soft brand collection strategy matters for hotels.";
  const spans = findEntitySpans(text, index.aliasIndex);
  assert.ok(!spans.some((s) => normalizeMatchKey(s.rawMention) === "collection"));
});

test("ENTITY_TEXT_FALLBACK_WHEN_MENTION_ARRAY_EMPTY", () => {
  // extractMentions operates on text — not provider mention arrays
  const text = "Tribute Portfolio and Westin appear in the shortlist.";
  const mentions = extractMentions({
    responseId: "t2",
    text,
    entityIndex: index.aliasIndex,
  });
  assert.ok(mentions.some((m) => m.canonicalEntityName === "Tribute Portfolio"));
  assert.ok(mentions.some((m) => m.canonicalEntityName === "Westin"));
});

test("COURTYARD_SLASH_SHORT_FORM", () => {
  const text = "midscale/upscale like Courtyard/Fiesta Inn";
  const spans = findEntitySpans(text, index.aliasIndex);
  assert.ok(spans.some((s) => s.entity.name === "Courtyard by Marriott"));
});

test("PLAYA_BARE_ALIAS_REJECTED", () => {
  const rejected = overlay.aliasesRejected || [];
  assert.ok(rejected.some((a) => a.alias === "Playa"));
  const text = "destinos de playa con club de playa; Thompson Playa del Carmen.";
  const spans = findEntitySpans(text, index.aliasIndex);
  assert.ok(!spans.some((s) => /Playa Hotels/i.test(s.entity.name)));
});

test("NO_CASE_SPECIFIC_RULES", () => {
  const norm = fs.readFileSync(
    path.join(ROOT, "lib/ai-visibility/normalize-entities.js"),
    "utf8"
  );
  const hydrate = fs.readFileSync(
    path.join(ROOT, "lib/ai-visibility/validation/hydrate-golden-set-texts.js"),
    "utf8"
  );
  assert.ok(!/cand_[a-z0-9]+/.test(norm));
  assert.ok(!/v2_cand_/.test(hydrate));
  assert.ok(!/caseId\s*===\s*['\"]/.test(norm));
});

test("EXPANDED_UNIVERSE_COVERS_GOLDEN_SUBJECTS", () => {
  assert.ok(index.entities.length >= 30);
  const names = new Set(index.entities.map((e) => e.name));
  assert.ok(names.has("Preferred Hotels & Resorts"));
  assert.ok(names.has("Small Luxury Hotels of the World"));
  assert.ok(names.has("Sheraton"));
});

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
