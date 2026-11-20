#!/usr/bin/env node
/**
 * Why Value insight structure: never replace CURRENT colon-led bullets with thin dumps;
 * atelier formats Bold Title Case Insight: sentence.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildCumulativeProposedBrand,
  countColonInsightLeads,
  decideSlotProvenance,
} from "../lib/brand-explorer/be-cumulative-proposal-assembler-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

const currentWhy = {
  slotKey: "overview.why_value",
  title: "Why Value Is Strongest in These Scenarios",
  body:
    "Keep your independent spirit—just add Hilton: retain hotel individuality and local character while accessing Hilton Honors.\n" +
    "Remarkable characters: hand-picked upper-upscale hotels in sought-after urban and resort destinations.\n" +
    "Culinary-forward positioning: world-class F&B from beachfront cocktails to chef-driven tasting menus.\n" +
    "Conversion-ready economics: commercial agreement materials support conversion and adaptive reuse.\n" +
    "Fit: owners and operators who can run full-service experiential hospitality with design QA.",
};

const thinCandidateWhy = {
  slotKey: "overview.why_value",
  title: "Why Value Is Strongest in These Scenarios",
  body:
    "Retain independent hotel character while participating in Hilton distribution and Hilton Honors. Owners should confirm conversion scope.\n" +
    "Each property retains unique identity\n" +
    "Hilton loyalty reach\n" +
    "soft-collection flexibility for distinctive upper-upscale hotels.\n" +
    "Hilton Honors drives member demand to Curio Collection by Hilton properties through earn/redeem interoperability across the Hilton Worldwide portfolio.\n" +
    "Curio Collection provides iconic dining experiences for guests and culinary enthusiasts.\n" +
    "Curio Collection by Hilton brand pillars should show up in conversion design.\n" +
    "Demand lenses span Corporate / Business; Leisure; Bleisure; Group / MICE; Family.",
};

assert.ok(countColonInsightLeads(currentWhy.body) >= 4, "CURRENT must count as colon insights");
assert.equal(countColonInsightLeads(thinCandidateWhy.body), 0, "thin dump has no colon leads");

const decision = decideSlotProvenance(
  "overview.why_value",
  [currentWhy],
  [thinCandidateWhy]
);
assert.equal(decision.applyCandidate, false, "thin why_value must not replace CURRENT");
assert.match(String(decision.reason || ""), /prose|regression|insight/i);

const overlay = JSON.parse(
  fs.readFileSync(
    path.join(ROOT, "public/brand-explorer-phase2e-preview/curio-collection-phase2h-candidate.json"),
    "utf8"
  )
);
const current = {
  id: "receQkxgjlezsc1xg",
  name: "Curio Collection by Hilton",
  brandExplorer: {
    version: 1,
    blocks: [
      {
        slotKey: "overview.scenario.2",
        title: "Commercial Platform And Lobby Experience",
        body: "Urban soft-brand conversions with lobby social programs.",
      },
      currentWhy,
    ],
  },
};
const { brand } = buildCumulativeProposedBrand(current, overlay);
const why = (brand.brandExplorer.blocks || []).find((b) => b.slotKey === "overview.why_value");
assert.ok(why, "why_value present");
assert.ok(
  countColonInsightLeads(why.body) >= 4,
  "assembled proposed why_value must retain CURRENT colon insights"
);
assert.doesNotMatch(
  why.body,
  /^Each property retains unique identity$/m,
  "thin fragment dump must not win"
);

const atelier = fs.readFileSync(
  path.join(ROOT, "public/js/brand-explorer-atelier-from-api.js"),
  "utf8"
);
assert.match(atelier, /formatWhyValueBulletHtml/);
assert.match(atelier, /<strong>' \+ escapeHtml\(lead\) \+ ':<\/strong>/);

const browserAsm = fs.readFileSync(
  path.join(ROOT, "public/js/brand-explorer-cumulative-proposal-assembler.js"),
  "utf8"
);
assert.match(browserAsm, /isWhyValueInsightRegression/);

console.log(
  JSON.stringify(
    {
      ok: true,
      currentLeads: countColonInsightLeads(currentWhy.body),
      thinLeads: countColonInsightLeads(thinCandidateWhy.body),
      assembledLeads: countColonInsightLeads(why.body),
      decision: decision.reason,
    },
    null,
    2
  )
);
