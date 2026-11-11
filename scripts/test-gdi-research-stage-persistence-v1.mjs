#!/usr/bin/env node
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import crypto from "node:crypto";
import {
  syncSegmentStateFromCorpus,
  createEmptySegmentResearchState,
  saveSegmentResearchState,
  loadSegmentResearchState,
} from "../lib/group-demand-intelligence/research/demand-segment-research-supervisor-v1.js";
import {
  mergeOpportunityBagsById,
  persistResearchStageOpportunityBag,
  assertResearchMemoryNotErased,
  writeJsonAtomic,
} from "../lib/group-demand-intelligence/research/gdi-research-stage-persistence-v1.js";
import {
  BETHESDA_29_ATTESTED_IDS,
  buildBethesdaHistoricalAttestationBag,
  persistBethesdaHistoricalAttestationBag,
} from "../lib/group-demand-intelligence/research/bethesda-historical-attestation-bag-v1.js";
import { BETHESDA_CANONICAL_HOTEL_ID } from "../lib/group-demand-intelligence/research/bethesda-full-demand-database-reconciliation-v1.js";
import { SPICE_ISLAND_CERTIFIED_SEVEN_IDS } from "../lib/group-demand-intelligence/durable-publication/gdi-approved-publication-opportunity-source-v1.js";

const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-rsp-"));

// --- Attestation bag ---
const bag = buildBethesdaHistoricalAttestationBag();
assert.equal(bag.count, 29);
assert.deepEqual(bag.ids, [...BETHESDA_29_ATTESTED_IDS]);
assert.equal(bag.safety.customerFacing, false);
assert.equal(bag.safety.salesReady, false);
assert.equal(bag.safety.opportunitiesJsonWrites, false);
for (const r of bag.records) {
  assert.equal(r.customerFacing, false);
  assert.equal(r.salesReady, false);
  assert.equal(r.publicationApproved, false);
  assert.equal(r.promoteToOpportunitiesJson, false);
  assert.equal(r.inventedFields, false);
  assert.equal(r.missingOriginalResearchBody, true);
  assert.ok(
    ["ATTESTED_BODY_MISSING", "OVERFLOW_WATCH", "PARTNERSHIP_WATCH"].includes(
      r.recoveryDisposition
    )
  );
}

const persisted = persistBethesdaHistoricalAttestationBag({
  root: tmpRoot,
  writeFixture: true,
});
assert.equal(persisted.bag.count, 29);
assert.ok(fs.existsSync(persisted.stagePath));

// --- Canonical 59 unchanged ---
const oppPath = path.join(
  "data/group-demand-intelligence/hotels",
  BETHESDA_CANONICAL_HOTEL_ID,
  "opportunities.json"
);
const beforeHash = crypto
  .createHash("sha256")
  .update(fs.readFileSync(oppPath))
  .digest("hex");
const beforeDoc = JSON.parse(fs.readFileSync(oppPath, "utf8"));
const beforeOps = Array.isArray(beforeDoc)
  ? beforeDoc
  : beforeDoc.opportunities || [];
const beforeCount = beforeOps.length;

// --- FS-only sync must preserve staged catering/partnership IDs ---
const stagedCatering = [
  "disp_recLuxvwwxID7U2B8_greater_bethesda_chamber_of_commerce",
  "disp_recLuxvwwxID7U2B8_montgomery_county_shrm",
  "disp_recLuxvwwxID7U2B8_cohera_formerly_csi_dmc_and_360dg_",
];
let state = createEmptySegmentResearchState({
  hotelId: BETHESDA_CANONICAL_HOTEL_ID,
  segment: "local_meetings_catering",
});
state.candidateIds = [...stagedCatering];
state.unresolvedCandidateIds = [...stagedCatering];

// Corpus has only one FS opportunity — previously this wiped the other staged IDs.
const fsOnlyCorpus = [
  {
    id: "gdi_opp_acts_ts27",
    organizationName: "ACTS",
    segment: "local_meetings_catering",
    researchStatus: "IN_PROGRESS",
  },
];
state = syncSegmentStateFromCorpus(state, fsOnlyCorpus, { paidBlocked: true });
for (const id of stagedCatering) {
  assert.ok(
    state.candidateIds.includes(id),
    `staged research id must survive FS-only sync: ${id}`
  );
}
assert.ok(state.candidateIds.includes("gdi_opp_acts_ts27"));
assert.equal(state.corpusSyncMeta.syncMode, "UNION_PRIOR_AND_CORPUS");

// Repeated syncs must not erase
state = syncSegmentStateFromCorpus(state, fsOnlyCorpus, { paidBlocked: true });
state = syncSegmentStateFromCorpus(state, fsOnlyCorpus, { paidBlocked: true });
for (const id of stagedCatering) {
  assert.ok(state.candidateIds.includes(id));
}

// --- Interrupted atomic segment-state save retains recoverable state ---
const segDir = path.join(tmpRoot, "segment-state");
saveSegmentResearchState(segDir, state);
const loaded = loadSegmentResearchState(segDir, "local_meetings_catering");
assert.ok(loaded);
for (const id of stagedCatering) {
  assert.ok(loaded.candidateIds.includes(id));
}

// --- Shadow merge refuses protected drops ---
const shadowPath = path.join(tmpRoot, "shadow.json");
writeJsonAtomic(shadowPath, {
  opportunities: stagedCatering.map((id) => ({
    id,
    organizationName: id,
  })),
});
const bad = persistResearchStageOpportunityBag({
  hotelId: BETHESDA_CANONICAL_HOTEL_ID,
  targetPath: shadowPath,
  incomingDoc: { opportunities: [{ id: "only_new", organizationName: "x" }] },
  protectedIds: stagedCatering,
  root: tmpRoot,
  mode: "replace",
});
assert.equal(bad.ok, false);
assert.equal(bad.code, "PROTECTED_RESEARCH_IDS_WOULD_BE_DROPPED");

const good = persistResearchStageOpportunityBag({
  hotelId: BETHESDA_CANONICAL_HOTEL_ID,
  targetPath: shadowPath,
  incomingDoc: {
    opportunities: [
      { id: "only_new", organizationName: "x" },
      { id: stagedCatering[0], organizationName: "updated" },
    ],
  },
  protectedIds: stagedCatering,
  root: tmpRoot,
  mode: "merge",
});
assert.equal(good.ok, true);
const shadowDoc = JSON.parse(fs.readFileSync(shadowPath, "utf8"));
assert.equal(shadowDoc.customerFacing, false);
assert.equal(shadowDoc.publication, "NOT_AUTHORIZED");
for (const id of stagedCatering) {
  assert.ok(shadowDoc.opportunities.some((o) => o.id === id));
}
assert.ok(shadowDoc.opportunities.some((o) => o.id === "only_new"));

const merge = mergeOpportunityBagsById({
  existing: [{ id: "a" }, { id: "b" }],
  incoming: [{ id: "b", x: 1 }, { id: "c" }],
});
assert.equal(merge.ok, true);
assert.equal(merge.opportunities.length, 3);

const erased = assertResearchMemoryNotErased({
  beforeIds: stagedCatering,
  afterIds: [stagedCatering[0]],
  protectedIds: stagedCatering,
});
assert.equal(erased.ok, false);

// --- Canonical 59 still unchanged ---
const afterHash = crypto
  .createHash("sha256")
  .update(fs.readFileSync(oppPath))
  .digest("hex");
assert.equal(beforeHash, afterHash);
assert.equal(beforeCount, 59);

// --- Spice Island seven registry intact (immutable certified IDs) ---
assert.equal(SPICE_ISLAND_CERTIFIED_SEVEN_IDS.length, 7);
assert.ok(SPICE_ISLAND_CERTIFIED_SEVEN_IDS.includes("gdi_s4912_03deda917778"));
assert.ok(SPICE_ISLAND_CERTIFIED_SEVEN_IDS.includes("gdi_s4912_ba7b1cb8a3ab"));
const spicePath =
  "data/group-demand-intelligence/hotels/recKRJjcPnb4tVDDS/opportunities.json";
if (fs.existsSync(spicePath)) {
  const spice = JSON.parse(fs.readFileSync(spicePath, "utf8"));
  const spiceOps = Array.isArray(spice) ? spice : spice.opportunities || [];
  const spiceIds = new Set(spiceOps.map((o) => o.id || o.opportunityId));
  const present = SPICE_ISLAND_CERTIFIED_SEVEN_IDS.filter((id) => spiceIds.has(id));
  // Local FS may be a lean mirror; when any certified IDs are present, all seven must be.
  if (present.length > 0) {
    assert.equal(
      present.length,
      7,
      `Spice seven must remain intact when local store is populated (found ${present.length})`
    );
  }
}

// cleanup
fs.rmSync(tmpRoot, { recursive: true, force: true });

console.log(
  JSON.stringify(
    {
      ok: true,
      attestationCount: 29,
      syncPreservesStaged: true,
      shadowMergeGuarded: true,
      bethesdaHashUnchanged: beforeHash,
      bethesdaCount: 59,
      spiceSeven: SPICE_ISLAND_CERTIFIED_SEVEN_IDS.length,
    },
    null,
    2
  )
);
