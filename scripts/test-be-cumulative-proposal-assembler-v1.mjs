#!/usr/bin/env node
import assert from "assert";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  assembleCumulativeProposal,
  buildCumulativeProposedBrand,
  PROVENANCE,
} from "../lib/brand-explorer/be-cumulative-proposal-assembler-v1.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const preview = path.join(root, "public", "brand-explorer-phase2e-preview");

const current = JSON.parse(
  fs.readFileSync(path.join(preview, "curio-gate1-renderer-parity-payload.json"), "utf8"),
);
const candidate = JSON.parse(
  fs.readFileSync(path.join(preview, "curio-collection-phase2h-candidate.json"), "utf8"),
);

const { brand, assembly } = buildCumulativeProposedBrand(current, candidate);
const blocks = brand.brandExplorer.blocks;
const proofs = blocks.filter((b) => b.slotKey === "loyalty.proof");
const identity = blocks.find((b) => b.slotKey === "overview.differentiators.identity");
const commercial = blocks.find((b) => b.slotKey === "overview.differentiators.commercial");

assert.strictEqual(proofs.length, 6, "loyalty.proof must retain 6 cards");
assert.ok(
  proofs.every((p) => String(p.body || "").trim().length > 0),
  "no empty loyalty proof bodies",
);
assert.ok(
  !proofs.some((p) => /loyalty proof points/i.test(p.title || "")),
  "no generic Loyalty proof points placeholder",
);
assert.ok(
  String(identity.body).split(/\n+/).filter(Boolean).length >= 4,
  "identity differentiators need 4 bullets",
);
assert.ok(
  String(commercial.body).split(/\n+/).filter(Boolean).length >= 4,
  "commercial differentiators need 4 bullets",
);
assert.ok(
  assembly.provenanceBySlot["loyalty.proof"].provenance === PROVENANCE.RETAINED_FROM_CURRENT,
  "loyalty.proof publication provenance retained",
);
assert.ok(
  assembly.incompleteDrafts.some((d) => d.slotKey === "loyalty.proof"),
  "loyalty.proof incomplete candidate exposed separately",
);

// Synthetic: weaker candidate must not win
const weak = assembleCumulativeProposal(
  [{ slotKey: "loyalty.proof", title: "A", body: "Substantive benefit card body one here." },
   { slotKey: "loyalty.proof", title: "B", body: "Substantive benefit card body two here." },
   { slotKey: "loyalty.proof", title: "C", body: "Substantive benefit card body three here." },
   { slotKey: "loyalty.proof", title: "D", body: "Substantive benefit card body four here." },
   { slotKey: "loyalty.proof", title: "E", body: "Substantive benefit card body five here." },
   { slotKey: "loyalty.proof", title: "F", body: "Substantive benefit card body six here." }],
  [{ slotKey: "loyalty.proof", title: "Loyalty proof points", body: "" }],
);
assert.strictEqual(weak.blocks.length, 6);
assert.strictEqual(weak.provenanceBySlot["loyalty.proof"].provenance, PROVENANCE.RETAINED_FROM_CURRENT);

console.log(
  JSON.stringify(
    {
      ok: true,
      loyaltyCards: proofs.length,
      identityBullets: String(identity.body).split(/\n+/).filter(Boolean).length,
      commercialBullets: String(commercial.body).split(/\n+/).filter(Boolean).length,
      provenanceCounts: assembly.provenanceCounts,
    },
    null,
    2,
  ),
);
