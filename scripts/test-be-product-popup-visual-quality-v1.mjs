#!/usr/bin/env node
/**
 * Independent product visual-quality gate — fails when BOTH preview and product
 * share the same defect (parity alone is insufficient).
 *
 * Covers:
 * - Royal Palm Airtable CDN masthead regression (no -tw filename)
 * - Long uppercase scenario prose forbidden
 * - Single-card grid contract (data-card-count + CSS)
 * - Product popup must honor Proposed Update mount path
 * - Founder lesson BE-QL-PRODUCT-POPUP-VISUAL-001 registered
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  applyPropertyImageIntegrityToBlocks,
  findVerifiedPropertyImageReplacement,
} from "../lib/brand-explorer/be-property-image-integrity-v1.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

function read(rel) {
  return fs.readFileSync(path.join(root, rel), "utf8");
}

const HILTON_AERIAL =
  "Royal-Palm-Galapagos-Curio-Collection-by-Hilton_Aerial_Courtesy-of-Royal-Palm-Galapagos-Curio-Collection-by-Hilton.jpg";

// --- Image identity: CDN without -tw must still swap ---
const cdnBlock = {
  slotKey: "footprint.openings",
  title: "Royal Palm Galapagos Curio Collection by Hilton — Santa Cruz Island",
  imageUrl: "https://v5.airtableusercontent.com/v3/u/59/59/x/y/z/opaqueToken",
  imageFilename: "",
  body: "https://www.travelweekly.com/story\nhttps://www.hilton.com/en/hotels/gpsrpqq-royal-palm-galapagos/",
};
const repaired = applyPropertyImageIntegrityToBlocks([cdnBlock]);
assert.equal(repaired.changedCount, 1);
assert.match(String(repaired.blocks[0].imageUrl), /stories-editor\.hilton\.com/);
assert.match(String(repaired.blocks[0].imageUrl), new RegExp(HILTON_AERIAL.replace(/\./g, "\\.")));
assert.ok(findVerifiedPropertyImageReplacement(cdnBlock));

// --- Browser mirror + product popup wiring ---
const browserMirror = read("public/js/brand-explorer-property-image-integrity.js");
assert.match(browserMirror, /resolvePropertyDisplayImage/);
assert.match(browserMirror, /airtableusercontent/);
assert.match(browserMirror, /Always return verified identity photo/);

const atelier = read("public/js/brand-explorer-atelier-from-api.js");
assert.match(atelier, /isShortPropertyScenarioAccent/);
assert.match(atelier, /resolvePropertyDisplayImage/);
assert.match(atelier, /beCombinedPopupAtelierRoot/);
assert.match(atelier, /data-card-count/);

const combined = read("public/brand-explorer-combined.html");
assert.match(combined, /buildCumulativeProposedBrandAsync/);
assert.match(combined, /wantProposed/);
assert.match(combined, /paintPopupAtelier/);

const css = read("public/css/brand-explorer-atelier-oe-panels.css");
assert.match(css, /property-example-grid\[data-card-count="1"\]/);
assert.match(
  css,
  /\.be-atelier-oe \.property-example-card__scenario\s*\{[\s\S]*?text-transform:\s*none/,
);
assert.doesNotMatch(
  css.match(
    /\.be-atelier-oe \.property-example-card__scenario\s*\{[^}]+\}/,
  )?.[0] || "",
  /text-transform:\s*uppercase/,
);

// --- Lesson registry ---
const lessons = JSON.parse(
  read("configs/brand-explorer-founder-quality-lessons-registry-v1.json"),
);
assert.ok(
  lessons.lessons.some((l) => l.lessonId === "BE-QL-PRODUCT-POPUP-VISUAL-001"),
  "founder lesson BE-QL-PRODUCT-POPUP-VISUAL-001 must be registered",
);
assert.ok(lessons.lessons.some((l) => l.lessonId === "BE-QL-PROPERTY-IMAGE-001"));

console.log(
  JSON.stringify(
    {
      ok: true,
      contract: "BE_PRODUCT_POPUP_VISUAL_QUALITY_V1",
      checks: [
        "cdn_royal_palm_verified_swap",
        "no_uppercase_scenario_css",
        "single_card_grid_contract",
        "popup_proposed_mount_path",
        "founder_lesson_registered",
      ],
    },
    null,
    2,
  ),
);
