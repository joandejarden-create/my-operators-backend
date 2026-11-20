#!/usr/bin/env node
/**
 * Phase 2G regressions — founder screenshot failure classes + paired comparison assets.
 */
import assert from "assert";
import { readFileSync, existsSync } from "fs";
import { join } from "path";
import { evaluateAntiPadding, PADDING_FAIL } from "../lib/brand-explorer/be-phase2g-anti-padding-v1.js";
import { sanitizePresentationCandidate } from "../lib/brand-explorer/be-phase2g-presentation-sanitize-v1.js";
import { matchContentItems, loadSubsectionParityContract } from "../lib/brand-explorer/be-phase2g-subsection-contract-v1.js";

const root = process.cwd();

assert.ok(existsSync(join(root, "configs/brand-explorer-subsection-parity-contract-v1.json")));
const contract = loadSubsectionParityContract();
assert.equal(contract.version, "BE_SUBSECTION_PARITY_CONTRACT_V1");
assert.equal(contract.forbidDualFullPageScrollAsDefault, true);

// Defective: repeated negotiation paragraphs + generic proof titles + meta language
const defective = {
  brandName: "Fixture",
  brandExplorer: {
    blocks: [
      {
        slotKey: "economics.negotiable_items",
        title: "Negotiable items",
        body: "Confirm fee abatements in the current disclosure. Confirm fee abatements in the current disclosure.",
      },
      {
        slotKey: "economics.rarely_negotiable",
        title: "Rarely negotiable",
        body: "Confirm fee abatements in the current disclosure. Owners should diligence net contribution after fees.",
      },
      {
        slotKey: "overview.proof.1",
        title: "Proof 1",
        body: "Evidence-grounded reconstruction for overview.proof.1: Focus this card on proof timing and documentation only.",
      },
      {
        slotKey: "overview.proof.2",
        title: "Proof 2",
        body: "Generic interchangeable soft-brand diligence language without a brand-specific claim or example.",
      },
    ],
  },
};

const bad = evaluateAntiPadding(defective.brandExplorer.blocks, { brandName: "Fixture" });
assert.ok(bad.metrics.founderApprovalBlocked, "defective must block");
assert.ok(bad.failureRegister.some((f) => f.code === PADDING_FAIL.REPEATED_SENTENCE_WITHIN_FIELD));
assert.ok(bad.failureRegister.some((f) => f.code === PADDING_FAIL.GENERIC_PROOF_TITLE));
assert.ok(bad.failureRegister.some((f) => f.code === PADDING_FAIL.INTERNAL_META_LANGUAGE));

const cleaned = sanitizePresentationCandidate(defective, {
  brandName: "Fixture Brand",
  parentCompany: "Parent",
  runPhase2fRepair: false,
});
assert.ok(
  !/Evidence-grounded|Focus this card|overview\.proof/i.test(
    cleaned.sanitized.brandExplorer.blocks.map((b) => b.body).join(" "),
  ),
  "sanitize must strip internal meta",
);

// Collection matching must not be index-only
const pairs = matchContentItems(
  [
    { slotKey: "commercial.demand", title: "Bleisure demand", body: "A" },
    { slotKey: "commercial.demand", title: "Group demand", body: "B" },
  ],
  [
    { slotKey: "commercial.demand", title: "Group demand", body: "B2" },
    { slotKey: "commercial.demand", title: "Bleisure demand", body: "A2" },
  ],
);
assert.equal(pairs[0].right.title, "Bleisure demand");
assert.equal(pairs[1].right.title, "Group demand");

const html = readFileSync(join(root, "public/brand-explorer-compare.html"), "utf8");
assert.ok(html.includes("becsPairedDoc"));
assert.ok(!html.includes("becsLeftAtelier"), "dual atelier mounts removed from default compare page");

const js = readFileSync(join(root, "public/js/brand-explorer-compare-studio.js"), "utf8");
assert.ok(js.includes("becs-pair-cols"));
assert.ok(js.includes("matchItems"));
assert.ok(js.includes("phase2g-parity"));

const css = readFileSync(join(root, "public/css/brand-explorer-compare-studio.css"), "utf8");
assert.ok(css.includes("align-items: start"));
assert.ok(css.includes("becs-pair-cols"));

// If candidates exist, anti-padding must improve vs Phase 2F
const curio2g = join(root, "public/brand-explorer-phase2e-preview/curio-collection-phase2g-candidate.json");
const curio2f = join(root, "public/brand-explorer-phase2e-preview/curio-collection-phase2f-candidate.json");
if (existsSync(curio2g) && existsSync(curio2f)) {
  const a = evaluateAntiPadding(JSON.parse(readFileSync(curio2f, "utf8")).brandExplorer.blocks, {
    brandName: "Curio",
  });
  const b = evaluateAntiPadding(JSON.parse(readFileSync(curio2g, "utf8")).brandExplorer.blocks, {
    brandName: "Curio",
  });
  assert.ok(
    b.metrics.paddingFailures < a.metrics.paddingFailures,
    "Phase 2G Curio must reduce anti-padding failures vs Phase 2F",
  );
}

console.log(
  JSON.stringify(
    {
      ok: true,
      defectiveFailures: bad.failureRegister.length,
      airtableWrites: 0,
      founderApproved: false,
      comparisonArchitecture: "paired_subsection_document",
    },
    null,
    2,
  ),
);
