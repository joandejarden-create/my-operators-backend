#!/usr/bin/env node
/**
 * No repeated card headings — title must not echo at the start of body.
 */
import assert from "assert";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  BE_NO_REPEATED_CARD_HEADING_V1,
  stripRepeatedCardHeading,
  bodyRepeatsCardHeading,
  applyNoRepeatedCardHeadingToBlocks,
  validateNoRepeatedCardHeading,
  customerFacingCardBody,
} from "../lib/brand-explorer/be-no-repeated-card-heading-v1.js";
import { buildCumulativeProposedBrand } from "../lib/brand-explorer/be-cumulative-proposal-assembler-v1.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const fixturePath = path.join(
  root,
  "fixtures/brand-explorer-no-repeated-card-heading/founder-title-echo-examples.json",
);
const preview = path.join(root, "public", "brand-explorer-phase2e-preview");

assert.equal(BE_NO_REPEATED_CARD_HEADING_V1, "BE_NO_REPEATED_CARD_HEADING_V1");

assert.equal(
  stripRepeatedCardHeading(
    "Collection Scale Context",
    "Collection Scale Context: Choice Hotels positions Ascend as an upscale soft collection.",
  ),
  "Choice Hotels positions Ascend as an upscale soft collection.",
);
assert.equal(
  stripRepeatedCardHeading(
    "Direct Booking Emphasis",
    "direct booking emphasis — Owners should model member mix.",
  ),
  "Owners should model member mix.",
);
assert.equal(
  bodyRepeatsCardHeading(
    "Collection Scale Context",
    "Collection Scale Context: Choice Hotels positions Ascend.",
  ),
  true,
);

// Later mention must be preserved
const later =
  "The asset keeps local character while gaining collection distribution — local character preserved is mentioned later only.";
assert.equal(stripRepeatedCardHeading("Local Character Preserved", later), later);
assert.equal(customerFacingCardBody("Local Character Preserved", later), later);

const fixture = JSON.parse(fs.readFileSync(fixturePath, "utf8"));
const before = validateNoRepeatedCardHeading(fixture.blocks);
assert.equal(before.pass, false);
assert.ok(before.failureCount >= 2);

const repaired = applyNoRepeatedCardHeadingToBlocks(fixture.blocks);
assert.equal(validateNoRepeatedCardHeading(repaired.blocks).pass, true);
fixture.expectedBodies.forEach((expected, i) => {
  assert.equal(repaired.blocks[i].body, expected, `block ${i}`);
});

// Proposed Curio path still passes (no new echoes introduced)
const current = JSON.parse(
  fs.readFileSync(path.join(preview, "curio-gate1-renderer-parity-payload.json"), "utf8"),
);
const candidate = JSON.parse(
  fs.readFileSync(path.join(preview, "curio-collection-phase2h-candidate.json"), "utf8"),
);
const { brand, assembly } = buildCumulativeProposedBrand(current, candidate);
assert.ok(assembly.noRepeatedCardHeading);
const rightValidation = validateNoRepeatedCardHeading(brand.brandExplorer.blocks || []);
assert.equal(
  rightValidation.pass,
  true,
  JSON.stringify(rightValidation.failures.slice(0, 10), null, 2),
);

// Synthetic Ascend founder case on proposed
const ascendCurrent = {
  id: "reclkgOzvAcBheUSo",
  name: "Ascend Hotel Collection",
  brandExplorer: {
    version: 1,
    blocks: [
      {
        slotKey: "overview.proof.1",
        title: "Collection Scale Context",
        body: "Collection Scale Context: Choice Hotels positions Ascend as an upscale soft collection for distinctive conversions.",
      },
    ],
  },
};
const ascendBuilt = buildCumulativeProposedBrand(ascendCurrent, { brandExplorer: { blocks: [] } });
const proof1 = (ascendBuilt.brand.brandExplorer.blocks || []).find(
  (b) => b.slotKey === "overview.proof.1",
);
assert.ok(proof1);
assert.equal(proof1.title, "Collection Scale Context");
assert.equal(
  proof1.body,
  "Choice Hotels positions Ascend as an upscale soft collection for distinctive conversions.",
);
assert.ok(!/^Collection Scale Context\s*:/i.test(proof1.body));

const contract = JSON.parse(
  fs.readFileSync(
    path.join(root, "configs/brand-explorer-permanent-content-standards-contract-v1.json"),
    "utf8",
  ),
);
assert.equal(
  contract.typographyAndWriting.cardHeadings.forbidRepeatedTitleInBody,
  true,
);

console.log(
  JSON.stringify(
    {
      ok: true,
      contract: BE_NO_REPEATED_CARD_HEADING_V1,
      fixtureFailuresBefore: before.failureCount,
      headingEchoChangedOnProposed: assembly.noRepeatedCardHeading.changedCount,
      ascendProofBody: proof1.body,
      airtableWrites: 0,
      founderApproved: false,
    },
    null,
    2,
  ),
);
