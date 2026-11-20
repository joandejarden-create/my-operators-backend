#!/usr/bin/env node
/**
 * Gate: Phase 3 evidence is planned, deduped, and projectable to Sources tab citations.
 */
import assert from "node:assert/strict";
import {
  planGraphUpserts,
  emptyGraphIndex,
} from "../lib/hotel-intelligence/research-engine-v1/airtable-graph-upsert-planner.js";
import {
  evidenceDedupeKey,
  orgDedupeKey,
} from "../lib/hotel-intelligence/research-engine-v1/entity-keys.js";
import { projectEvidenceToSourcesCitations } from "../lib/hotel-intelligence/research-engine-v1/he-airtable-graph-reader-v1.js";
import { HI_GRAPH_TABLES } from "../lib/hotel-intelligence/research-engine-v1/airtable-graph-contract-v1.js";
import { createSyntheticGraphStore } from "../lib/hotel-intelligence/research-engine-v1/airtable-graph-store.js";

const HPC = "recLuxvwwxID7U2B8";
const URL_A = "https://example.com/sdat-owner";
const URL_B = "https://example.com/mrec-portfolio";
const marbethKey = orgDedupeKey({
  legalName: "Marbeth Acquisition LLC",
  jurisdiction: "US-PA",
});

const staged = {
  hpc_id: HPC,
  case_id: "test_beth_sources",
  organizations: [
    {
      legal_name: "Marbeth Acquisition LLC",
      jurisdiction: "US-PA",
      confidence: "HIGH",
    },
  ],
  relationships: [
    {
      hpc_id: HPC,
      relationship_type: "Legal Owner",
      org_dedupe_key: marbethKey,
      confidence: "HIGH",
    },
  ],
  evidence: [
    {
      hpc_id: HPC,
      source_name: "SDAT",
      source_url: URL_A,
      source_type: "Government Record",
      field_name: "property_company",
      value_observed: "Marbeth Acquisition LLC",
      claims_supported: ["property_company", "Legal Owner"],
      current: true,
      confidence: "HIGH",
    },
    {
      hpc_id: HPC,
      source_name: "MREC Portfolio",
      source_url: URL_B,
      source_type: "Company Website",
      field_name: "economic_owner",
      value_observed: "Mullen Real Estate Capital",
      claims_supported: ["economic_owner sponsor association"],
      current: true,
      confidence: "MEDIUM",
      notes: "UBO unresolved",
    },
    // Same URL again with additional claim — must merge, not duplicate.
    {
      hpc_id: HPC,
      source_name: "SDAT",
      source_url: URL_A,
      source_type: "Government Record",
      field_name: "property_history",
      value_observed: "2019 transfer",
      claims_supported: ["property_history 2019 transfer"],
      current: true,
      confidence: "HIGH",
    },
  ],
};

const plan = planGraphUpserts(staged, emptyGraphIndex(), { hpcId: HPC });
assert.equal(plan.ok, true);
const evidenceOps = plan.operations.filter((o) => o.table === HI_GRAPH_TABLES.graphEvidence);
assert.equal(evidenceOps.length, 2, "one evidence op per unique URL");
assert.ok(evidenceOps.every((o) => o.op === "CREATE"));

const sdat = evidenceOps.find((o) => o.record.source_url === URL_A);
assert.ok(sdat.record.claims_supported.includes("property_company"));
assert.ok(sdat.record.claims_supported.includes("property_history 2019 transfer"));

const key = evidenceDedupeKey({ hpcId: HPC, sourceUrl: URL_A });
assert.equal(sdat.dedupe_key, key);

const idx = emptyGraphIndex();
for (const op of evidenceOps) idx.evidenceByDedupe.set(op.dedupe_key, op.record);
const plan2 = planGraphUpserts(staged, idx, { hpcId: HPC, runId: plan.run_id });
const evidenceOps2 = plan2.operations.filter((o) => o.table === HI_GRAPH_TABLES.graphEvidence);
assert.ok(evidenceOps2.every((o) => o.op === "UPDATE"), "idempotent UPDATE on replay");

const citations = projectEvidenceToSourcesCitations(
  evidenceOps.map((o) => ({
    sourceUrl: o.record.source_url,
    sourceName: o.record.source_name,
    sourceType: o.record.source_type,
    fieldName: o.record.field_name,
    valueObserved: o.record.value_observed,
    evidenceSnippet: o.record.evidence_snippet,
    notes: o.record.notes,
    current: o.record.current,
  })),
);
assert.equal(citations.length, 2, "Sources tab shows one row per URL");
assert.ok(citations.every((c) => c.url && c.title && c.claim));

const store = createSyntheticGraphStore({ allowSynthetic: true });
const applied = store.persistStaged(staged, {
  apply_mode: "APPLY",
  founder_auth: true,
  allow_synthetic: true,
});
assert.equal(applied.ok, true);
assert.ok(applied.writes >= evidenceOps.length);

console.log(
  JSON.stringify(
    {
      ok: true,
      test: "test-he-beth-phase3-sources-evidence-v1",
      evidenceCreate: evidenceOps.length,
      sourcesTabCitations: citations.length,
      idempotentUpdates: evidenceOps2.length,
    },
    null,
    2,
  ),
);
console.log("OK — Phase 3 Sources/Evidence gate");
