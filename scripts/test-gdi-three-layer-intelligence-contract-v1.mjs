#!/usr/bin/env node
/**
 * Permanent Three-Layer Intelligence Contract regression suite.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GDI_THREE_LAYER_INTELLIGENCE_CONTRACT_V1,
  INTELLIGENCE_LAYER,
  FOUNDER_PRINCIPLES,
  EXISTING_ARCHITECTURE_MAPPING,
  FIELD_OWNERSHIP_TABLE,
  classifyIntelligenceLayer,
  assertPresentationCannotOverwriteResearch,
  assertMultiHotelFqoIndependence,
  interpretCommercialEligibilityStatus,
  classifyWeeklyChangeMetadata,
  evaluatePersistenceBoundaryGuard,
  THREE_LAYER_WRITE_SET_POLICY,
} from "../lib/group-demand-intelligence/workflow/gdi-three-layer-intelligence-contract-v1.js";
import {
  mergeResearchSotLossless,
} from "../lib/group-demand-intelligence/workflow/gdi-research-sot-lossless-merge-v1.js";
import {
  mergeProposedResearchInventoryFields,
  fingerprintResearchFields,
  diffMaterialResearchFields,
} from "../lib/group-demand-intelligence/workflow/gdi-hotel-research-inventory-store-v1.js";
import { MAP_GDI_HOTEL_RESEARCH_OPPORTUNITY as F } from "../lib/group-demand-intelligence/hotel-research-opportunity-field-map.js";
import {
  filterValidatedFqoPersistenceWriteSet,
  RESEARCH_PERSISTENCE_WRITE_SET_POLICY,
} from "../lib/group-demand-intelligence/workflow/gdi-full-e2e-research-persistence-bridge-v1.js";
import { evaluateFinalizedQualifiedOpportunity } from "../lib/group-demand-intelligence/workflow/gdi-finalized-qualified-opportunity-contract-v1.js";
import { buildSlimResearchPayload } from "../lib/group-demand-intelligence/workflow/gdi-research-inventory-slim-payload-v1.js";

const results = [];
function pass(name) {
  results.push({ name, ok: true });
}
function check(name, fn) {
  fn();
  pass(name);
}

check("contract_id", () => {
  assert.equal(
    GDI_THREE_LAYER_INTELLIGENCE_CONTRACT_V1,
    "gdi-three-layer-intelligence-contract-v1",
  );
  assert.ok(FOUNDER_PRINCIPLES.cumulativeIntelligence);
  assert.ok(EXISTING_ARCHITECTURE_MAPPING.layer1Surfaces.length >= 4);
  assert.ok(FIELD_OWNERSHIP_TABLE.length >= 8);
});

check("layer_classification", () => {
  assert.equal(
    classifyIntelligenceLayer("meetingHistory"),
    INTELLIGENCE_LAYER.RESEARCH_SOT,
  );
  assert.equal(
    classifyIntelligenceLayer("hotelSpecificQualification"),
    INTELLIGENCE_LAYER.HOTEL_QUALIFICATION,
  );
  assert.equal(
    classifyIntelligenceLayer("tenSectionDrawer"),
    INTELLIGENCE_LAYER.COMMERCIAL_PRESENTATION,
  );
  assert.equal(
    classifyIntelligenceLayer("unknownHistoricalKeep"),
    INTELLIGENCE_LAYER.RESEARCH_SOT,
  );
});

const fat = {
  opportunityId: "opp_tl",
  hotelId: "recHotelA",
  organizationName: "Org",
  commercialDemandType: "BOTH",
  revenueStreamClass: "COMBINED_GUESTROOMS_EVENT_FNB",
  whatIsHappening: "Accurate multi-sentence research description of the conference.",
  summaryWhat: "Accurate summary of the verified conference.",
  meetingHistory: [
    { year: 2026, venue: "Marriott Downtown", claimKind: "FACT" },
    { year: 2025, venue: "UNKNOWN", claimKind: "FACT" },
  ],
  competitors: [
    { name: "Hyatt Regency Bethesda", class: "STR_COMP_SET" },
    { name: "Bethesda North Marriott", class: "RELEVANT_GROUP_DEMAND_ALTERNATIVE" },
  ],
  evidence: [
    {
      id: "ev1",
      field: "event_dates",
      value: "June 7–9, 2027",
      claimKind: "FACT",
      confidence: 80,
      sourceUrl: "https://example.gov/a",
    },
    {
      id: "ev2",
      field: "venue",
      value: "TBA",
      claimKind: "FACT",
      confidence: 80,
      sourceUrl: "https://example.gov/a",
    },
    {
      id: "ev3",
      field: "contact",
      value: "a@example.gov",
      claimKind: "FACT",
      confidence: 75,
      sourceUrl: "https://example.gov/staff",
    },
  ],
  evidenceConfidence: 89,
  primaryContact: {
    name: "Ada Contact",
    email: "ada@example.gov",
    contactGrade: "A",
  },
  backupContacts: [{ name: "Bob Backup", email: "bob@example.gov", contactGrade: "A" }],
  contacts: [
    { name: "Ada Contact", email: "ada@example.gov", contactGrade: "A" },
    { name: "Bob Backup", email: "bob@example.gov", contactGrade: "A" },
  ],
  contactResearchAudit: {
    pass: "gdi_contact_resolution_v1",
    gradeBefore: "A",
    gradeAfter: "A",
  },
  historicalKeep: { nested: true },
};

const slim = {
  opportunityId: "opp_tl",
  hotelId: "recHotelA",
  commercialDemandType: "GUESTROOMS",
  revenueStreamClass: "GUESTROOMS_ONLY",
  whatIsHappening:
    "Exhibitors and attendees create guestroom and networking demand for the annual cycle.",
  summaryWhat: "Annual conference.",
  meetingHistory: [],
  competitors: [],
  evidence: [{ url: "https://example.gov/a" }],
  evidenceConfidence: 99,
  primaryContact: { name: "Ada Contact", email: "ada@example.gov" },
  contacts: [{ name: "Ada Contact", email: "ada@example.gov" }],
  hotelSpecificQualification: {
    finalizedQualified: true,
    status: "PASS",
    hotelId: "recHotelA",
  },
  finalizedQualified: true,
  tenSectionDrawer: { section1: "short" },
  drawerHeaderTitle: "Short title",
};

// --- Research preservation ---
check("fat_sot_not_overwritten_by_slim", () => {
  const m = mergeResearchSotLossless({
    livePayload: fat,
    proposedPayload: slim,
  });
  assert.equal(m.ok, true, m.code);
  assert.equal(m.payload.revenueStreamClass, "COMBINED_GUESTROOMS_EVENT_FNB");
  assert.equal(m.payload.meetingHistory.length, 2);
  assert.equal(m.payload.competitors.length, 2);
  assert.ok(m.payload.evidence.some((e) => e.id === "ev1" && e.claimKind === "FACT"));
  assert.deepEqual(m.payload.historicalKeep, { nested: true });
  assert.ok(m.length > JSON.stringify(slim).length);
});

check("unknown_fields_preserved", () => {
  const m = mergeResearchSotLossless({ livePayload: fat, proposedPayload: slim });
  assert.deepEqual(m.payload.historicalKeep, { nested: true });
});

check("structured_evidence_preserved", () => {
  const m = mergeResearchSotLossless({ livePayload: fat, proposedPayload: slim });
  assert.ok(m.payload.evidence.filter((e) => e.claimKind === "FACT").length >= 3);
});

check("meeting_history_preserved", () => {
  const m = mergeResearchSotLossless({ livePayload: fat, proposedPayload: slim });
  assert.equal(m.payload.meetingHistory.length, 2);
});

check("competitor_relationships_preserved", () => {
  const m = mergeResearchSotLossless({ livePayload: fat, proposedPayload: slim });
  assert.ok(m.payload.competitors.some((c) => c.name.includes("Hyatt")));
});

check("contact_audit_preserved", () => {
  const m = mergeResearchSotLossless({ livePayload: fat, proposedPayload: slim });
  assert.equal(m.payload.contactResearchAudit.pass, "gdi_contact_resolution_v1");
  assert.equal(m.payload.primaryContact.contactGrade, "A");
  assert.equal(m.payload.backupContacts.length, 1);
});

// --- Commercial classification ---
check("combined_revenue_not_silently_guestrooms_only", () => {
  const m = mergeResearchSotLossless({ livePayload: fat, proposedPayload: slim });
  assert.equal(m.payload.revenueStreamClass, "COMBINED_GUESTROOMS_EVENT_FNB");
  assert.equal(m.payload.commercialDemandType, "BOTH");
});

check("verified_description_not_exhibitor_template", () => {
  const m = mergeResearchSotLossless({ livePayload: fat, proposedPayload: slim });
  assert.ok(!String(m.payload.whatIsHappening || "").includes("Exhibitors and attendees"));
  assert.ok(String(m.payload.summaryWhat).includes("Accurate"));
});

check("evidence_confidence_not_silently_upgraded", () => {
  // Slim proposed 99; live 89 — keep live unless independently supported (keep live on conflict path)
  const m = mergeResearchSotLossless({ livePayload: fat, proposedPayload: slim });
  // evidenceConfidence is authoritative research — not in certification overlay; live preserved via base merge
  assert.equal(m.payload.evidenceConfidence, 89);
});

check("presentation_guard_flags_thinning", () => {
  const g = assertPresentationCannotOverwriteResearch({
    livePayload: fat,
    presentationPayload: slim,
  });
  assert.equal(g.ok, false);
  assert.ok(g.violations.some((v) => v.code === "PRESENTATION_REVENUE_DOWNGRADE"));
  assert.ok(g.violations.some((v) => v.code === "PRESENTATION_ARRAY_SHRINK"));
});

// --- Qualification ---
check("strict_fqo_independent", () => {
  const incomplete = {
    opportunityId: "opp_no_contact",
    title: "X",
    hotelId: "recHotelA",
  };
  const fqo = evaluateFinalizedQualifiedOpportunity(incomplete, {
    historicalApprovedInventory: true,
  });
  assert.notEqual(fqo.finalizedQualified, true);
});

check("nonqualified_cannot_enter_validated_write_set", () => {
  const filtered = filterValidatedFqoPersistenceWriteSet(
    [
      {
        opportunityId: "opp_fail",
        finalizedQualified: false,
      },
      {
        opportunityId: "opp_pass",
        ...fat,
        primaryContact: fat.primaryContact,
        hotelOpportunityThesis: "thesis",
        evidence: fat.evidence,
        recommendedNextStep: "Call Ada",
        eventStartDate: "2027-06-07",
        hotelSpecificQualification: { finalizedQualified: true },
        finalizedQualified: true,
        fqoEvaluation: { finalizedQualified: true, status: "PASS" },
      },
    ],
    { allowHistoricalSchemaCompat: true },
  );
  assert.equal(
    filtered.policy,
    RESEARCH_PERSISTENCE_WRITE_SET_POLICY.VALIDATED_FQO_ONLY,
  );
  assert.ok(filtered.deferred.some((d) => d.opportunityId === "opp_fail"));
});

check("multi_hotel_fqo_does_not_transfer", () => {
  const r = assertMultiHotelFqoIndependence({
    hotelAId: "recHotelA",
    hotelBId: "recHotelB",
    hotelAQualification: { finalizedQualified: true },
    hotelBQualification: null,
  });
  assert.equal(r.aApprovesB, false);
  assert.equal(r.hotelAQualified, true);
  assert.equal(r.hotelBQualified, false);
});

check("global_org_only_is_not_hotel_fqo_failure", () => {
  const i = interpretCommercialEligibilityStatus("GLOBAL_ORG_ONLY");
  assert.equal(i.isHotelFqoFailure, false);
});

// --- Presentation cannot mutate research via store merge ---
check("presentation_generation_cannot_mutate_authoritative_research", () => {
  const slimBuilt = buildSlimResearchPayload(slim);
  assert.equal(slimBuilt.ok, true);
  const store = mergeProposedResearchInventoryFields(
    {
      [F.opportunityId]: "opp_tl",
      [F.hotelId]: "recHotelA",
      [F.researchPayloadJson]: slimBuilt.json,
    },
    {
      [F.opportunityId]: "opp_tl",
      [F.hotelId]: "recHotelA",
      [F.researchPayloadJson]: JSON.stringify(fat),
    },
  );
  assert.equal(store.ok, true, store.code);
  const after = JSON.parse(store.fields[F.researchPayloadJson]);
  assert.equal(after.revenueStreamClass, "COMBINED_GUESTROOMS_EVENT_FNB");
  assert.equal(after.meetingHistory.length, 2);
  assert.ok(after.hotelSpecificQualification?.finalizedQualified === true);
  assert.ok(store.weeklyChangeMetadata);
  assert.equal(store.threeLayerContract, GDI_THREE_LAYER_INTELLIGENCE_CONTRACT_V1);
});

// --- Persistence ---
check("idempotent_merge_second_pass", () => {
  const first = mergeResearchSotLossless({
    livePayload: fat,
    proposedPayload: slim,
  });
  assert.equal(first.ok, true);
  const second = mergeResearchSotLossless({
    livePayload: first.payload,
    proposedPayload: slim,
  });
  assert.equal(second.ok, true);
  assert.equal(second.payload.revenueStreamClass, "COMBINED_GUESTROOMS_EVENT_FNB");
  // Stable identity
  assert.equal(second.payload.opportunityId, "opp_tl");
});

check("material_change_not_timestamp_alone", () => {
  const fields = {
    [F.opportunityId]: "opp_tl",
    [F.hotelId]: "recHotelA",
    [F.researchPayloadJson]: JSON.stringify(fat),
  };
  const stamped = {
    ...fields,
    [F.lastMaterialChangeAt]: "2099-01-01T00:00:00.000Z",
  };
  const diff = diffMaterialResearchFields(stamped, fields);
  assert.equal(diff.materialChanged, false);
  assert.equal(
    fingerprintResearchFields(fields),
    fingerprintResearchFields({
      ...fields,
      [F.lastResearchedAt]: "2099-01-01T00:00:00.000Z",
    }),
  );
});

check("weekly_change_metadata_categories", () => {
  const m = mergeResearchSotLossless({ livePayload: fat, proposedPayload: slim });
  const w = classifyWeeklyChangeMetadata({
    beforePayload: fat,
    afterPayload: m.payload,
  });
  assert.ok(w.stableIdentities.opportunityId === "opp_tl");
  assert.ok(Array.isArray(w.researchChanges));
  assert.ok(Array.isArray(w.qualificationChanges));
  assert.ok(Array.isArray(w.commercialChanges));
});

check("persistence_boundary_guard_fail_closed_on_bad_policy", () => {
  const g = evaluatePersistenceBoundaryGuard({
    livePayload: fat,
    proposedPayload: slim,
    writeSetPolicy: "WRITE_EVERYTHING",
  });
  assert.equal(g.ok, false);
  assert.ok(
    g.blockers.some((b) => b.code === "WRITE_SET_POLICY_NOT_VALIDATED_FQO_ONLY"),
  );
});

check("persistence_boundary_guard_ok_under_validated_fqo", () => {
  const g = evaluatePersistenceBoundaryGuard({
    livePayload: fat,
    proposedPayload: slim,
    writeSetPolicy: THREE_LAYER_WRITE_SET_POLICY.VALIDATED_FQO_ONLY,
  });
  assert.equal(g.ok, true, JSON.stringify(g.blockers));
  assert.equal(g.writeSetPolicy, "VALIDATED_FQO_ONLY");
});

// --- NICE fixture if present ---
const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const niceLive = path.join(
  ROOT,
  "reports/gdi-validated-airtable-canary-prewrite-safety-v1/04b-ORIGINAL_researchPayloadJson.json",
);
const niceProp = path.join(
  ROOT,
  "reports/gdi-validated-airtable-canary-prewrite-safety-v1/05b-PROPOSED_researchPayloadJson.json",
);
if (fs.existsSync(niceLive) && fs.existsSync(niceProp)) {
  check("nice_fixture_zero_unexplained_loss", () => {
    const live = JSON.parse(fs.readFileSync(niceLive, "utf8"));
    const prop = JSON.parse(fs.readFileSync(niceProp, "utf8"));
    const g = evaluatePersistenceBoundaryGuard({
      livePayload: live,
      proposedPayload: prop,
    });
    assert.equal(g.ok, true, JSON.stringify(g.blockers));
    assert.equal(g.merge.payload.commercialDemandType, "BOTH");
    assert.equal(
      g.merge.payload.revenueStreamClass,
      "COMBINED_GUESTROOMS_EVENT_FNB",
    );
    assert.ok(g.merge.length >= JSON.stringify(live).length * 0.9);
  });
}

console.log(
  JSON.stringify(
    {
      ok: true,
      contract: GDI_THREE_LAYER_INTELLIGENCE_CONTRACT_V1,
      passed: results.length,
      tests: results.map((r) => r.name),
    },
    null,
    2,
  ),
);
