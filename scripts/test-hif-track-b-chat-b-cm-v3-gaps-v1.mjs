#!/usr/bin/env node
import assert from "node:assert/strict";
import { contractPersistAllowed } from "../lib/hotel-intelligence/sec-edgar/hif-track-b-global-historical-waves-v1.js";
import { isTrackAReservedId } from "../lib/hotel-intelligence/sec-edgar/hif-track-b-historical-foundation-v1.js";
import {
  ASSESSMENT,
  GAP_FINDINGS,
  buildChatBGapDossiers,
  summarizeGapAssessments,
} from "../lib/hotel-intelligence/sec-edgar/hif-track-b-chat-b-cm-v3-gaps-v1.js";

assert.equal(GAP_FINDINGS.length, 7);
const d = buildChatBGapDossiers();
assert.equal(d.length, 7);

const by = summarizeGapAssessments();
assert.equal(by[ASSESSMENT.QUALIFIED_RETAINED_STRENGTHENED], 3);
assert.equal(by[ASSESSMENT.EMERGING_RETAINED], 2);
assert.equal(by[ASSESSMENT.AMBIGUOUS_REVIEW], 1);
assert.equal(by[ASSESSMENT.UNDER_RESEARCH], 1);

const oromana = d.find((x) => x.findingId.includes("oromana"));
assert.equal(oromana.chatBAssessment, ASSESSMENT.QUALIFIED_RETAINED_STRENGTHENED);
assert.equal(oromana.milestoneDate, "2026-11-27");
assert.equal(oromana.agreementType, "MANAGEMENT");
assert.equal(contractPersistAllowed(oromana), true);
assert.ok(oromana.contractId);

const agueda = d.find((x) => x.findingId.includes("agueda"));
assert.equal(agueda.milestoneDate, "2027-11-06");
assert.equal(agueda.persistEligible, true);

const areia = d.find((x) => x.findingId.includes("areia"));
assert.equal(areia.milestoneDate, "2027-03-17");

const cap = d.find((x) => x.findingId.includes("cap_cana"));
assert.equal(cap.chatBAssessment, ASSESSMENT.EMERGING_RETAINED);
assert.equal(cap.contractEligible, false);
assert.equal(contractPersistAllowed(cap), false);

const finn = d.find((x) => x.findingId.includes("finn"));
assert.equal(finn.persistEligible, false);
assert.equal(finn.chatBAssessment, ASSESSMENT.UNDER_RESEARCH);

const berg = d.find((x) => x.findingId.includes("bergantin"));
assert.equal(berg.chatBAssessment, ASSESSMENT.EMERGING_RETAINED);

const uriz = d.find((x) => x.findingId.includes("torre_uriz"));
assert.equal(uriz.chatBAssessment, ASSESSMENT.AMBIGUOUS_REVIEW);
assert.equal(uriz.milestoneDate, "2026-11-19");
assert.equal(uriz.trackAPayload.competingPathway, false);

const ids = new Set(d.map((x) => x.findingId));
assert.equal(ids.size, d.length);
assert.ok(d.every((x) => !isTrackAReservedId(x.discoveryPropertyId)));
assert.ok(d.every((x) => x.publicationState === "CUSTOMER_BLOCKED"));
assert.equal(d.filter((x) => x.persistEligible).length, 6);

// No silent promotion of Emerging → Qualified
assert.ok(!GAP_FINDINGS.some((g) => g.priorTrackAClass?.includes("EMERGING") && g.chatBAssessment === ASSESSMENT.QUALIFIED_RETAINED_STRENGTHENED));
// No downgrade of Qualified
assert.ok(
  GAP_FINDINGS.filter((g) => g.priorTrackAClass === "QUALIFIED").every(
    (g) => g.chatBAssessment === ASSESSMENT.QUALIFIED_RETAINED_STRENGTHENED,
  ),
);

console.log(
  JSON.stringify(
    {
      ok: true,
      dossiers: d.length,
      persistEligible: d.filter((x) => x.persistEligible).length,
      contractPersistAllowed: d.filter((x) => contractPersistAllowed(x)).length,
      byAssessment: by,
    },
    null,
    2,
  ),
);
