#!/usr/bin/env node
/**
 * Subject name-matching regressions + OpenAI false-negative guardrails.
 *   npm run test:adp-subject-name-matching-v1
 */
import assert from "assert";
import { readFileSync } from "fs";
import { join } from "path";
import {
  detectPropertyMention,
  buildNameVariants,
  normalizeSubjectHaystack,
} from "../lib/ai-demand-positioning/execution/response-parser.js";
import { loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";

const UI = join(process.cwd(), "public/js/ai-demand-positioning/ai-demand-positioning.js");
const CSS = join(process.cwd(), "public/js/ai-demand-positioning/ai-demand-positioning.css");
const HTML = join(process.cwd(), "public/owner-ai-demand.html");

function main() {
  const phillips = loadPropertyProfile("adp_hotel_phillips_kansas_city");
  assert.ok(phillips);

  // Exact / approved aliases from OpenAI false-negative audit
  assert.equal(detectPropertyMention("Stay at Hotel Phillips for art deco charm.", phillips).mentioned, true);
  assert.equal(
    detectPropertyMention("4. **The Phillips Hotel**: This hotel originally opened in 1931", phillips).mentioned,
    true
  );
  assert.equal(
    detectPropertyMention(
      "5. **The Phillips Kansas City, Curio Collection by Hilton**: Originally opened in 1931",
      phillips
    ).mentioned,
    true
  );
  assert.equal(detectPropertyMention("Consider Phillips Kansas City downtown.", phillips).mentioned, true);

  // Punctuation / & normalization
  const waterstone = loadPropertyProfile("adp_waterstone_boca_raton");
  assert.equal(
    detectPropertyMention("Waterstone Resort and Marina is a strong waterfront pick.", waterstone).mentioned,
    true
  );
  assert.equal(
    detectPropertyMention("Waterstone Resort & Marina sits on the Intracoastal.", waterstone).mentioned,
    true
  );

  // City suffix / brand
  const renaissance = loadPropertyProfile("adp_renaissance_times_square");
  assert.equal(
    detectPropertyMention("Renaissance New York Times Square Hotel near Broadway.", renaissance).mentioned,
    true
  );
  // Ambiguous bare brand must NOT auto-match Midtown-only text without Times Square context
  // (short "Renaissance" alone is a weak variant — collision risk). Guard: Midtown hotel text without subject.
  const midtownOnly =
    "1. Renaissance New York Midtown Hotel\n2. The Westin New York at Times Square";
  assert.equal(
    detectPropertyMention(midtownOnly, renaissance).mentioned,
    false,
    "Renaissance Midtown must not count as Times Square subject"
  );

  // False-positive: unrelated KC hotels must not match Phillips
  assert.equal(
    detectPropertyMention("Book Hotel Kansas City or Loews Kansas City Hotel.", phillips).mentioned,
    false
  );

  const variants = buildNameVariants(phillips);
  assert.ok(variants.includes("The Phillips Hotel"));
  assert.ok(normalizeSubjectHaystack("A & B") === "a and b");

  // Structural: CORE meta on Competitive Overview title row; fixed table layout
  const ui = readFileSync(UI, "utf8");
  const css = readFileSync(CSS, "utf8");
  const html = readFileSync(HTML, "utf8");
  assert.ok(html.includes("adpCompCoreMeta"), "CORE meta slot on overview title row");
  assert.ok(html.includes("adp-comp-overview-title-row"), "title row structure");
  assert.ok(html.includes("adp-comp-table-fixed"), "fixed layout table class");
  assert.ok(!html.includes('id="adpCompCount"'), "CORE note removed from under Demand Territory filter");
  assert.ok(ui.includes("adpCompCoreMeta"), "JS renders into CORE meta");
  assert.ok(css.includes("table-layout: fixed"), "fixed table layout CSS");
  assert.ok(css.includes("adp-comp-overview-title-row"), "title row CSS");

  console.log("test:adp-subject-name-matching-v1 OK");
}

main();
