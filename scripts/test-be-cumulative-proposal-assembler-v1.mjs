#!/usr/bin/env node
import assert from "assert";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  assembleCumulativeProposal,
  buildCumulativeProposedBrand,
  rehydrateEphemeralImageUrls,
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

// Empty CURRENT + single Proof Point stub must NOT publish (protects static six-card UX).
const emptyCurrentProof = assembleCumulativeProposal(
  [],
  [
    {
      slotKey: "loyalty.proof",
      title: "Proof Point",
      body: "Marriott discloses Autograph Collection as a Premium soft brand with 362 properties.",
    },
  ]
);
assert.strictEqual(
  emptyCurrentProof.provenanceBySlot["loyalty.proof"].applyCandidate,
  false,
  "single loyalty.proof stub must not apply over empty CURRENT"
);
assert.strictEqual(
  emptyCurrentProof.provenanceBySlot["loyalty.proof"].reason,
  "loyalty_proof_requires_exact_six_cards"
);
assert.ok(
  !emptyCurrentProof.blocks.some((b) => b.slotKey === "loyalty.proof"),
  "incomplete loyalty.proof must stay out of publication blocks"
);

// Display-only: ephemeral Airtable CDN proposed URLs must yield to CURRENT delivery.
const hydrated = rehydrateEphemeralImageUrls(
  [
    {
      slotKey: "overview.scenario.1",
      recordId: "recFROZEN1",
      imageUrl:
        "https://v5.airtableusercontent.com/v3/u/59/59/expired/signature/photo.jpg",
    },
    {
      slotKey: "overview.scenario.2",
      recordId: "recFROZEN2",
      imageUrl: "https://cache.marriott.com/is/image/marriottsitespecials/ac-ok",
    },
  ],
  [
    {
      slotKey: "overview.scenario.1",
      recordId: "recCURRENT1",
      imageUrl: "/api/brand-library/presentation-image/recCURRENT1",
      imageDeliveryUrl: "/api/brand-library/presentation-image/recCURRENT1",
      imageAttachmentId: "attABC",
    },
    {
      slotKey: "overview.scenario.2",
      recordId: "recCURRENT2",
      imageUrl: "/api/brand-library/presentation-image/recCURRENT2",
      imageDeliveryUrl: "/api/brand-library/presentation-image/recCURRENT2",
    },
  ]
);
assert.strictEqual(
  hydrated[0].imageUrl,
  "/api/brand-library/presentation-image/recCURRENT1",
  "expired CDN must rehydrate to CURRENT delivery"
);
assert.strictEqual(hydrated[0].recordId, "recCURRENT1");
assert.ok(hydrated[0].imageRehydratedFromCurrent);
assert.strictEqual(
  hydrated[1].imageUrl,
  "https://cache.marriott.com/is/image/marriottsitespecials/ac-ok",
  "durable non-Airtable proposed URLs must be preserved"
);

const frozenBuilt = buildCumulativeProposedBrand(
  {
    id: "recBrand",
    name: "AC Hotels by Marriott",
    brandExplorer: {
      version: 1,
      blocks: [
        {
          slotKey: "overview.scenario.1",
          recordId: "recSFmj7OjAMaNpRR",
          title: "Urban Lifestyle",
          body: "Scenario body retained.",
          imageUrl: "/api/brand-library/presentation-image/recSFmj7OjAMaNpRR",
          imageDeliveryUrl: "/api/brand-library/presentation-image/recSFmj7OjAMaNpRR",
        },
      ],
    },
  },
  {
    contract: "FROZEN_APPROVAL_PAYLOAD_V1",
    approvalContentHash: "frozen-hash-keep",
    brandExplorer: {
      blocks: [
        {
          slotKey: "overview.scenario.1",
          recordId: "recOLD",
          title: "Urban Lifestyle",
          body: "Scenario body retained.",
          imageUrl:
            "https://v5.airtableusercontent.com/v3/u/59/59/expired/signature/scenario1.jpg",
        },
      ],
    },
  }
);
assert.strictEqual(
  frozenBuilt.brand.brandExplorer.blocks[0].imageUrl,
  "/api/brand-library/presentation-image/recSFmj7OjAMaNpRR"
);
assert.strictEqual(
  frozenBuilt.brand.beCandidateOverlayMeta.contentHash,
  "frozen-hash-keep",
  "frozen approval hash must remain immutable after image rehydration"
);

console.log(
  JSON.stringify(
    {
      ok: true,
      loyaltyCards: proofs.length,
      identityBullets: String(identity.body).split(/\n+/).filter(Boolean).length,
      commercialBullets: String(commercial.body).split(/\n+/).filter(Boolean).length,
      provenanceCounts: assembly.provenanceCounts,
      rehydrateEphemeralImageUrls: true,
      frozenImageRehydrate: true,
    },
    null,
    2,
  ),
);
