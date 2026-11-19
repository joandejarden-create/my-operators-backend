#!/usr/bin/env node
/**
 * HE-INTEL-001 P2 — Airtable intelligence graph tests (synthetic / offline).
 * No paid providers. No live Airtable data writes in this harness.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  GRAPH_CONTRACT_ID,
  GRAPH_CONTRACT_VERSION,
  GRAPH_BASE_ID_CANONICAL,
  GRAPH_BASE_ID_FORBIDDEN,
  HPC_TABLE_ID,
  HI_GRAPH_TABLES,
  P2_POLICY,
  PUBLICATION_LAYER,
  EVIDENCE_ENTITY_TYPE_EXTENSIONS,
  assertCanonicalGraphBase,
  resolveGraphBaseId,
  writeGuaranteesP2,
} from "../lib/hotel-intelligence/research-engine-v1/airtable-graph-contract-v1.js";
import {
  planGraphUpserts,
  resolveGraphDataApply,
} from "../lib/hotel-intelligence/research-engine-v1/airtable-graph-upsert-planner.js";
import {
  createSyntheticGraphStore,
  assertNoCustomerPublication,
} from "../lib/hotel-intelligence/research-engine-v1/airtable-graph-store.js";
import {
  validateHotelOrgRelationshipRefs,
} from "../lib/hotel-intelligence/research-engine-v1/airtable-graph-referential-integrity.js";
import { MAP_HI_HOTEL_ORG_RELATIONSHIP, HPC_RECIPROCAL_RELATIONSHIP_FIELD } from "../lib/hotel-intelligence/research-engine-v1/airtable-graph-contract-v1.js";
import {
  normalizeOrganization,
  normalizeRelationship,
  normalizePerson,
  normalizeChannel,
  emptyStagedPackage,
} from "../lib/hotel-intelligence/research-engine-v1/output-schemas.js";
import { loadPilotManifest, listCanonicalPilotHpcIds } from "../lib/hotel-intelligence/research-engine-v1/pilot-manifest.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

let passed = 0;
function ok(name) {
  passed += 1;
  console.log(`PASS ${name}`);
}

function syntheticStaged(hpcId, orgName = "Synthetic Owner LLC") {
  const org = normalizeOrganization({
    legal_name: orgName,
    jurisdiction: "US-DE",
    confidence: "HIGH",
  });
  const rel = normalizeRelationship({
    hpc_id: hpcId,
    relationship_type: "Legal Owner",
    org_dedupe_key: org.dedupe_key,
    confidence: "HIGH",
  });
  const person = normalizePerson({
    display_name: "Alex Example",
    title: "Managing Partner",
    org_dedupe_key: org.dedupe_key,
    confidence: "MEDIUM",
  });
  const channel = normalizeChannel({
    subject_id: "pending_person",
    subject_type: "Person",
    kind: "Email",
    value: "alex@example-owner.test",
    classification: "Inferred",
  });
  const staged = emptyStagedPackage({ hpcId, caseId: `case_${hpcId}` });
  staged.organizations = [org];
  staged.relationships = [rel];
  staged.persons = [person];
  staged.channels = [channel];
  staged.execution_mode = "OFFLINE_DETERMINISTIC_STAGING";
  staged.research_complete = false;
  return staged;
}

// 1. Contract / policy + canonical HPC base gate
{
  assert.equal(GRAPH_CONTRACT_ID, "HOTEL_INTELLIGENCE_AIRTABLE_GRAPH_V1");
  assert.equal(GRAPH_BASE_ID_CANONICAL, "appCCUsuGsE1ifoLk");
  assert.equal(GRAPH_BASE_ID_FORBIDDEN, "appa2cE7FTRmIbB32");
  assert.equal(HPC_TABLE_ID, "tbl9aY5ijiuIzzWam");
  assert.equal(assertCanonicalGraphBase(GRAPH_BASE_ID_CANONICAL).ok, true);
  assert.equal(assertCanonicalGraphBase(GRAPH_BASE_ID_FORBIDDEN).ok, false);
  assert.equal(assertCanonicalGraphBase(GRAPH_BASE_ID_FORBIDDEN).error, "GRAPH_BASE_FORBIDDEN");
  assert.equal(resolveGraphBaseId({}), GRAPH_BASE_ID_CANONICAL);
  assert.equal(
    resolveGraphBaseId({ AIRTABLE_INTELLIGENCE_BASE_ID: GRAPH_BASE_ID_FORBIDDEN }),
    GRAPH_BASE_ID_CANONICAL
  );
  assert.equal(P2_POLICY.bulk_research_data_writes, false);
  assert.equal(P2_POLICY.paid_providers_enabled, false);
  assert.equal(P2_POLICY.customer_publication, "BLOCKED");
  assert.equal(P2_POLICY.hpc_owner_operator_overwrite, false);
  assert.equal(P2_POLICY.name_only_auto_merge, false);
  assert.ok(EVIDENCE_ENTITY_TYPE_EXTENSIONS.includes("Organization"));
  assert.ok(HI_GRAPH_TABLES.graphEvidence);
  const g = writeGuaranteesP2();
  assert.equal(g.customer_publication, "BLOCKED");
  assert.equal(g.graph_base_id, GRAPH_BASE_ID_CANONICAL);
  assert.equal(g.forbidden_base_id, GRAPH_BASE_ID_FORBIDDEN);
  ok("p2_contract_and_policy");
}

// 2. Apply refuses without auth / bulk disabled
{
  const refused = resolveGraphDataApply("APPLY", { founderAuth: false });
  assert.equal(refused.ok, false);
  const bulk = resolveGraphDataApply("APPLY", { founderAuth: true, allowSynthetic: false });
  assert.equal(bulk.ok, false);
  assert.match(bulk.message, /bulk_research_data_writes/);
  const dry = resolveGraphDataApply("DRY_RUN");
  assert.equal(dry.ok, true);
  assert.equal(dry.mode, "DRY_RUN");
  ok("apply_gates_refuse_unauthorized");
}

// 3. Dry-run produces no writes
{
  const store = createSyntheticGraphStore({ allowSynthetic: true });
  const staged = syntheticStaged("recLuxvwwxID7U2B8");
  const r = store.persistStaged(staged, { apply_mode: "DRY_RUN" });
  assert.equal(r.ok, true);
  assert.equal(r.writes, 0);
  assert.equal(store.countRows(), 0);
  assert.ok(r.planned > 0);
  ok("dry_run_no_writes");
}

// 4. Idempotent create + update + no duplicate orgs
{
  const store = createSyntheticGraphStore({ allowSynthetic: true });
  const staged = syntheticStaged("recLuxvwwxID7U2B8");
  const a1 = store.persistStaged(staged, {
    apply_mode: "APPLY",
    founder_auth: true,
    allow_synthetic: true,
  });
  assert.equal(a1.ok, true);
  assert.ok(a1.writes > 0);
  const orgCount1 = store.snapshot()[HI_GRAPH_TABLES.organizations].length;

  const a2 = store.persistStaged(staged, {
    apply_mode: "APPLY",
    founder_auth: true,
    allow_synthetic: true,
  });
  assert.equal(a2.ok, true);
  const orgCount2 = store.snapshot()[HI_GRAPH_TABLES.organizations].length;
  assert.equal(orgCount1, 1);
  assert.equal(orgCount2, 1);
  const orgOps = a2.applied.filter((x) => x.table === HI_GRAPH_TABLES.organizations);
  assert.ok(orgOps.every((x) => x.op === "UPDATE"));
  ok("idempotent_upsert_no_duplicate_orgs");
}

// 5. Cross-hotel organization reuse
{
  const store = createSyntheticGraphStore({ allowSynthetic: true });
  const s1 = syntheticStaged("recLuxvwwxID7U2B8", "Shared Operator Inc");
  const s2 = syntheticStaged("rec0eTEnCqlsGJJt9", "Shared Operator Inc");
  store.persistStaged(s1, { apply_mode: "APPLY", founder_auth: true, allow_synthetic: true });
  store.persistStaged(s2, { apply_mode: "APPLY", founder_auth: true, allow_synthetic: true });
  const orgs = store.snapshot()[HI_GRAPH_TABLES.organizations];
  assert.equal(orgs.length, 1);
  const rels = store.snapshot()[HI_GRAPH_TABLES.hotelOrgRelationships];
  assert.equal(rels.length, 2);
  assert.equal(rels[0].org_id, rels[1].org_id);
  ok("cross_hotel_organization_reuse");
}

// 6. Conflicting ownership held for review
{
  const staged = syntheticStaged("recLuxvwwxID7U2B8");
  const plan1 = planGraphUpserts(staged);
  // Seed index with existing HIGH confidence different name same key impossible —
  // instead same dedupe with equal confidence different type on relationship
  const idx = {
    orgsByDedupe: new Map([
      [
        staged.organizations[0].dedupe_key,
        { ...staged.organizations[0], org_id: "dle_EXISTING", confidence: "HIGH", legal_name: "Other Legal Name LLC" },
      ],
    ]),
    relsByDedupe: new Map(),
    personsByAffiliation: new Map(),
    channelsByDedupe: new Map(),
    runsById: new Map(),
  };
  const plan2 = planGraphUpserts(staged, idx);
  assert.ok(plan2.held_for_review.some((h) => h.reason === "CONFLICT_HELD_FOR_REVIEW"));
  ok("conflicting_ownership_held_for_review");
}

// 7. Name-only org without jurisdiction/dedupe key held
{
  const staged = emptyStagedPackage({ hpcId: "recLuxvwwxID7U2B8" });
  staged.organizations = [{ legal_name: "", confidence: "LOW" }];
  const plan = planGraphUpserts(staged);
  assert.ok(plan.held_for_review.some((h) => h.reason === "MISSING_DEDUPE_KEY"));
  ok("name_only_merge_forbidden");
}

// 8. Evidence provenance preserved on plan records
{
  const staged = syntheticStaged("recLuxvwwxID7U2B8");
  staged.organizations[0].evidence_refs = ["doe_TESTEVIDENCE000000000001"];
  const plan = planGraphUpserts(staged);
  const orgOp = plan.operations.find((o) => o.table === HI_GRAPH_TABLES.organizations);
  assert.deepEqual(orgOp.record.evidence_refs, ["doe_TESTEVIDENCE000000000001"]);
  ok("evidence_provenance_preserved");
}

// 9. Read-after-write validation
{
  const store = createSyntheticGraphStore({ allowSynthetic: true });
  const staged = syntheticStaged("recLuxvwwxID7U2B8");
  const r = store.persistStaged(staged, {
    apply_mode: "APPLY",
    founder_auth: true,
    allow_synthetic: true,
  });
  assert.equal(r.reconcile.ok, true);
  assert.equal(r.reconcile.missing.length, 0);
  ok("read_after_write_validation");
}

// 10. Safe resume after interruption
{
  const store = createSyntheticGraphStore({ allowSynthetic: true });
  const staged = syntheticStaged("recLuxvwwxID7U2B8");
  const interrupted = store.persistStaged(staged, {
    apply_mode: "APPLY",
    founder_auth: true,
    allow_synthetic: true,
    interrupt_after: 1,
  });
  assert.equal(interrupted.ok, false);
  assert.equal(interrupted.error, "INTERRUPTED");
  assert.ok(interrupted.resume_token);
  const resumed = store.resumePersist(staged, interrupted.resume_token, {
    founder_auth: true,
  });
  assert.equal(resumed.ok, true);
  assert.equal(store.snapshot()[HI_GRAPH_TABLES.organizations].length, 1);
  ok("safe_resume_after_interruption");
}

// 11. No unauthorized customer publication
{
  const store = createSyntheticGraphStore({ allowSynthetic: true });
  const staged = syntheticStaged("recLuxvwwxID7U2B8");
  const r = store.persistStaged(staged, {
    apply_mode: "APPLY",
    founder_auth: true,
    allow_synthetic: true,
  });
  const pub = assertNoCustomerPublication(r);
  assert.equal(pub.ok, true);
  assert.equal(r.customer_published, false);
  assert.equal(PUBLICATION_LAYER.CUSTOMER_FACING_PUBLICATION, "CUSTOMER_FACING_PUBLICATION");
  ok("no_unauthorized_customer_publication");
}

// 12. Inferred channel cannot be planned as Verified Direct
{
  const staged = syntheticStaged("recLuxvwwxID7U2B8");
  staged.channels[0].classification = "Inferred";
  staged.channels[0].verification_status = "Verified Direct";
  const plan = planGraphUpserts(staged);
  assert.ok(
    plan.held_for_review.some((h) => h.reason === "INFERRED_CANNOT_BE_VERIFIED_DIRECT")
  );
  ok("inferred_email_not_verified_direct");
}

// 13. Pilot fixture identity only — not live research claim
{
  const m = loadPilotManifest();
  assert.equal(listCanonicalPilotHpcIds(m).length, 15);
  const store = createSyntheticGraphStore({ allowSynthetic: true });
  for (const hpcId of listCanonicalPilotHpcIds(m).slice(0, 3)) {
    const staged = syntheticStaged(hpcId);
    assert.equal(staged.research_complete, false);
    store.persistStaged(staged, {
      apply_mode: "DRY_RUN",
    });
  }
  assert.equal(store.countRows(), 0);
  ok("pilot_fixture_identity_not_live_research");
}

// 14. Schema modules / ensure script present
{
  assert.equal(
    fs.existsSync(
      path.join(ROOT, "scripts/ensure-he-intel-001-p2-airtable-graph-schema.mjs")
    ),
    true
  );
  assert.equal(
    fs.existsSync(
      path.join(
        ROOT,
        "lib/hotel-intelligence/research-engine-v1/airtable-graph-contract-v1.js"
      )
    ),
    true
  );
  assert.equal(MAP_HI_HOTEL_ORG_RELATIONSHIP.hpcHotelLink, "HPC Hotel");
  assert.equal(HPC_RECIPROCAL_RELATIONSHIP_FIELD, "HI Organization Relationships");
  ok("schema_ensure_script_present");
}

// 15. HPC native-link referential integrity / cardinality
{
  const hpcId = "recnPB3Q02NO8Ma1o";
  const okRef = validateHotelOrgRelationshipRefs(
    {
      hpcHotelId: hpcId,
      hpcHotelLinkIds: [hpcId],
      organizationId: "dle_SYNTH",
      organizationLinkIds: ["recOrgAirtable0001"],
    },
    {
      hpcRecord: { id: hpcId },
      orgRecord: { id: "recOrgAirtable0001" },
      orgAirtableRecId: "recOrgAirtable0001",
    }
  );
  assert.equal(okRef.ok, true);

  const multi = validateHotelOrgRelationshipRefs(
    {
      hpcHotelId: hpcId,
      hpcHotelLinkIds: [hpcId, "recOTHER000000001"],
      organizationId: "dle_SYNTH",
      organizationLinkIds: ["recOrgAirtable0001"],
    },
    {
      hpcRecord: { id: hpcId },
      orgRecord: { id: "recOrgAirtable0001" },
      orgAirtableRecId: "recOrgAirtable0001",
    }
  );
  assert.equal(multi.ok, false);
  assert.equal(multi.error, "HPC_CARDINALITY_VIOLATION");

  const mismatch = validateHotelOrgRelationshipRefs(
    {
      hpcHotelId: hpcId,
      hpcHotelLinkIds: ["recOTHER000000001"],
      organizationId: "dle_SYNTH",
      organizationLinkIds: ["recOrgAirtable0001"],
    },
    {
      hpcRecord: { id: hpcId },
      orgRecord: { id: "recOrgAirtable0001" },
      orgAirtableRecId: "recOrgAirtable0001",
    }
  );
  assert.equal(mismatch.ok, false);
  assert.equal(mismatch.error, "HPC_ID_LINK_MISMATCH");

  const orphan = validateHotelOrgRelationshipRefs(
    { hpcHotelId: hpcId, hpcHotelLinkIds: [hpcId] },
    { hpcRecord: null, orgRecord: null }
  );
  assert.equal(orphan.ok, false);
  ok("hpc_native_link_referential_integrity");
}

console.log(`\nOK he-intel-001-p2-airtable-graph-v1 (${passed} assertions)`);
console.log(`graph ${GRAPH_CONTRACT_ID}@${GRAPH_CONTRACT_VERSION}`);
