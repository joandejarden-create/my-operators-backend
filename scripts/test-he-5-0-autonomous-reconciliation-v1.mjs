/**
 * Gates for HE 5.0 autonomous reconciliation supervisor.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  RESOLUTION_CLASS,
  detectPilotIssues,
  prioritizeGap,
  adjudicateIssue,
  runAutonomousReconciliation,
  AUTONOMOUS_FREE_PASS_EVIDENCE_V1,
  buildHe50DryRunMutationPackage,
  APPLY_GATE,
  SEVEN_HOTEL_FREE_IDENTITY_PACKETS,
} from "../lib/hotel-intelligence/he-5-0-full-e2e/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");

// Resolution vocabulary
for (const k of [
  "RESOLVED_VERIFIED",
  "RESOLVED_WITH_HISTORICAL_CONTEXT",
  "UNRESOLVED_INSUFFICIENT_EVIDENCE",
  "UNRESOLVED_PROVIDER_RESTRICTED",
  "REQUIRES_FOUNDER_DECISION",
]) {
  assert.ok(RESOLUTION_CLASS[k]);
}

// Priority: identity conflicts rank above cosmetic
const identityGap = {
  commercialValue: 5,
  identityImpact: 5,
  heCompleteness: 5,
  reusability: 3,
  resolveLikelihood: 5,
  evidenceWeakness: 2,
};
const amenityGap = {
  commercialValue: 1,
  identityImpact: 1,
  heCompleteness: 2,
  reusability: 1,
  resolveLikelihood: 3,
  evidenceWeakness: 1,
};
assert.ok(prioritizeGap(identityGap) > prioritizeGap(amenityGap));

const issues = detectPilotIssues();
assert.ok(issues.length >= 10);
assert.ok(issues[0].priority >= issues[issues.length - 1].priority);

// AC rooms resolve without founder decision
const ac = adjudicateIssue(issues.find((i) => i.issueId === "ac_rooms_140_vs_146"));
assert.equal(ac.resolutionClass, RESOLUTION_CLASS.RESOLVED_VERIFIED);
assert.equal(ac.resolvedValue, 140);
assert.equal(ac.requiresFounderDecision, false);

// Renovation dual events
const reno = adjudicateIssue(issues.find((i) => i.issueId === "beth_renovation_2015_vs_2017"));
assert.equal(reno.resolutionClass, RESOLUTION_CLASS.RESOLVED_WITH_HISTORICAL_CONTEXT);

// Cap Cana identity
const capId = adjudicateIssue(issues.find((i) => i.issueId === "cap_cana_vs_dreams_identity"));
assert.equal(capId.resolutionClass, RESOLUTION_CLASS.RESOLVED_VERIFIED);
assert.equal(capId.resolvedValue.distinct, true);

// Cap Cana rooms from free-pass evidence
const capRooms = adjudicateIssue(
  issues.find((i) => i.issueId === "cap_cana_rooms"),
  AUTONOMOUS_FREE_PASS_EVIDENCE_V1
);
assert.equal(capRooms.resolutionClass, RESOLUTION_CLASS.RESOLVED_VERIFIED);
assert.equal(capRooms.resolvedValue, 457);

// HGI rooms
const hgi = adjudicateIssue(
  issues.find((i) => i.issueId === "hgi_rooms_confidence"),
  AUTONOMOUS_FREE_PASS_EVIDENCE_V1
);
assert.equal(hgi.resolvedValue, 138);

// Current lender: unresolved insufficient — NOT founder decision
const lender = adjudicateIssue(issues.find((i) => i.issueId === "beth_current_lender"));
assert.equal(lender.resolutionClass, RESOLUTION_CLASS.UNRESOLVED_INSUFFICIENT_EVIDENCE);
assert.equal(lender.requiresFounderDecision, false);

// Sentiment: provider restricted — not founder decision
const sent = adjudicateIssue(issues.find((i) => i.issueId === "guest_sentiment_licensing"));
assert.equal(sent.resolutionClass, RESOLUTION_CLASS.UNRESOLVED_PROVIDER_RESTRICTED);

// Full autonomous run
const result = runAutonomousReconciliation({ enablePaid: false });
assert.ok(result.issuesInvestigated >= 10);
assert.ok(result.resolvedCount >= 8);
assert.equal(result.founderDecisionCount, 0);
assert.equal(result.budget.confirmedActualUsd, 0);
assert.equal(result.budget.remainingAuthorizedUsd, 50);
assert.match(result.applyGate.stopMessage, /SEPARATE FOUNDER AUTHORIZATION/);
assert.equal(result.stopReason, "apply_gate_only");

// Dry-run includes Cap Cana rooms READY; no production apply
const dry = buildHe50DryRunMutationPackage();
assert.equal(dry.applyGate.airtableWritesAllowed, true);
assert.equal(dry.applyGate.standingAuthorization, true);
const capRoomsMut = dry.mutations.find((m) => m.id === "M-UPD-secrets_cap_cana-rooms");
assert.ok(capRoomsMut);
assert.equal(capRoomsMut.proposedValue, 457);
assert.equal(capRoomsMut.founderRecommendation, "READY");

const hgiPkt = SEVEN_HOTEL_FREE_IDENTITY_PACKETS.find(
  (p) => p.pilotKey === "hgi_newtown_square_radnor"
);
assert.equal(hgiPkt.rooms.confidence, "HIGH");

const capPkt = SEVEN_HOTEL_FREE_IDENTITY_PACKETS.find((p) => p.pilotKey === "secrets_cap_cana");
assert.equal(capPkt.rooms.value, 457);

// Docs / standard mention autonomous behavior
const standard = fs.readFileSync(
  path.join(root, "docs/ai-build-system/HOTEL_EXPLORER_QUALITY_STANDARD.md"),
  "utf8"
);
assert.ok(standard.includes("HE 5.0") || standard.includes("HE-QL-039"));

const mod = fs.readFileSync(
  path.join(root, "lib/hotel-intelligence/he-5-0-full-e2e/autonomous-reconciliation-v1.js"),
  "utf8"
);
assert.ok(mod.includes("REQUIRES_FOUNDER_DECISION"));
assert.ok(mod.includes("diminishing"));
assert.ok(!/Airtable\.update|\.create\(|--apply/.test(mod));

assert.equal(APPLY_GATE.standingAuthorization, true);
assert.equal(APPLY_GATE.surfeAutomatic, false);

console.log("test:he-5-0-autonomous-reconciliation-v1 PASS");
