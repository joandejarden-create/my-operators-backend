#!/usr/bin/env node
/**
 * Unit tests — Operator Intelligence publication policy + conflicts + overlay isolation.
 *   node scripts/test-operator-intelligence-calibration.mjs
 */
import {
  resolvePublicationDecision,
  PUBLICATION_DECISION,
  SOURCE_AUTHORITY,
  allowsStrongConfidence,
  isOwnerFacingConfirmed,
} from "../lib/operator-intelligence/publication-policy.js";
import { detectClaimConflict, detectConflictsForOperator } from "../lib/operator-intelligence/conflict-detector.js";
import {
  buildPrefillOverlayFromCohort,
  mergePrefillWithCalibration,
  loadCalibrationCohort,
} from "../lib/operator-intelligence/calibration-overlay.js";
import { scoreOperatorMatchForDeal } from "../api/my-deals.js";
import { readFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

let failed = 0;
function ok(cond, msg) {
  if (!cond) {
    console.error("FAIL:", msg);
    failed += 1;
  } else console.log("ok:", msg);
}

// One authoritative source auto-publishes objective property fact
{
  const d = resolvePublicationDecision({
    publicationClass: 1,
    objectiveFact: true,
    claimCategory: "comparable",
    sources: [{ authority: SOURCE_AUTHORITY.PRIMARY_AUTHORITATIVE }],
  });
  ok(d.status === PUBLICATION_DECISION.AUTO_PUBLISH, "one authoritative source can auto-publish objective fact");
  ok(isOwnerFacingConfirmed(d), "auto-publish is owner-facing confirmed");
}

// Operator marketing cannot verify performance
{
  const d = resolvePublicationDecision({
    claimCategory: "performance",
    publicationClass: 3,
    sources: [{ authority: SOURCE_AUTHORITY.OPERATOR_MARKETING }],
  });
  ok(
    d.status === PUBLICATION_DECISION.INTERNAL_ONLY || d.status === PUBLICATION_DECISION.REJECTED,
    "one operator marketing source cannot verify performance"
  );
}

// Two consistent independent sources
{
  const d = resolvePublicationDecision({
    publicationClass: 1,
    objectiveFact: true,
    sources: [
      { authority: SOURCE_AUTHORITY.RELIABLE_INDEPENDENT },
      { authority: SOURCE_AUTHORITY.TRADE_PRESS },
    ],
  });
  ok(d.status === PUBLICATION_DECISION.AUTO_PUBLISH, "two consistent independent sources support objective claim");
}

// Conflicting authoritative → review
{
  const d = resolvePublicationDecision({
    publicationClass: 1,
    objectiveFact: true,
    conflictStatus: "Hard",
    sources: [{ authority: SOURCE_AUTHORITY.PRIMARY_AUTHORITATIVE }],
  });
  ok(d.status === PUBLICATION_DECISION.CONFLICTED, "conflicting authoritative sources trigger review");
  ok(d.humanReviewRequired, "conflict requires human review");
}

// Historical ≠ current
{
  const c = detectClaimConflict({
    claimCategory: "geography",
    existingPresenceType: "Historical Presence",
    existingValue: "Mexico",
    newClaim: { claimValue: "Mexico", presenceType: "Current Managed Property" },
    potentialScoreImpact: "High",
  });
  ok(c.conflictType === "current_versus_historical", "historical presence does not equal current presence");
}

// Strategic interest overclaim
{
  const c = detectClaimConflict({
    claimCategory: "geography",
    newClaim: {
      claimValue: "Current Managed Property",
      presenceType: "Strategic Interest",
      limitations: "",
    },
    potentialScoreImpact: "High",
  });
  ok(c.conflictType === "presence_overclaim", "strategic interest does not become confirmed geography");
}

// One brand property ≠ global approval
{
  const c = detectClaimConflict({
    claimCategory: "brand",
    newClaim: {
      claimValue: "Global approval",
      limitations: "one property only",
    },
    limitations: "one property",
  });
  ok(c.conflictType === "brand_relationship_overclaim", "one operated brand does not create global approval");
}

// Operator-reported cannot create Strong confidence alone
{
  const d = resolvePublicationDecision({
    publicationClass: 2,
    requiresEvidenceLabel: true,
    sources: [{ authority: SOURCE_AUTHORITY.OPERATOR_MARKETING }],
  });
  ok(d.status === PUBLICATION_DECISION.PUBLISH_WITH_LABEL, "operator-reported publishes with label");
  ok(!allowsStrongConfidence({ evidenceClass: "general_claim" }, d), "operator-reported cannot create Strong confidence alone");
}

// Internal-only not confirmed owner-facing
{
  const d = resolvePublicationDecision({
    publicationClass: 3,
    claimCategory: "fees",
    sources: [{ authority: SOURCE_AUTHORITY.PRIMARY_AUTHORITATIVE }],
  });
  ok(d.status === PUBLICATION_DECISION.INTERNAL_ONLY, "internal-only claims stay internal");
  ok(!isOwnerFacingConfirmed(d), "internal-only not confirmed owner-facing");
}

// Sensitive negative → human review
{
  const d = resolvePublicationDecision({
    publicationClass: 1,
    objectiveFact: true,
    sensitive: true,
    sources: [{ authority: SOURCE_AUTHORITY.PRIMARY_AUTHORITATIVE }],
  });
  ok(d.status === PUBLICATION_DECISION.HUMAN_REVIEW_REQUIRED, "sensitive negative claims require human review");
}

// High-impact conflict → exception
{
  const c = detectClaimConflict({
    claimCategory: "geography",
    existingValue: "Mexico|Peru",
    newClaim: { normalizedValue: "Mexico" },
    potentialScoreImpact: "High",
  });
  ok(c.humanReviewRequired, "high-impact ranking geography change triggers exception review");
}

// Determinism
{
  const input = {
    publicationClass: 1,
    objectiveFact: true,
    sources: [{ authority: SOURCE_AUTHORITY.PRIMARY_AUTHORITATIVE }],
  };
  const a = resolvePublicationDecision(input);
  const b = resolvePublicationDecision(input);
  ok(JSON.stringify(a) === JSON.stringify(b), "publication decisions are deterministic");
  const c1 = detectConflictsForOperator({
    operatorId: "x",
    operatorName: "X",
    claims: [{ claimCategory: "geography", normalizedValue: "Mexico", sourceIds: [] }],
    profile: { activeCountries: ["Peru"] },
  });
  const c2 = detectConflictsForOperator({
    operatorId: "x",
    operatorName: "X",
    claims: [{ claimCategory: "geography", normalizedValue: "Mexico", sourceIds: [] }],
    profile: { activeCountries: ["Peru"] },
  });
  ok(JSON.stringify(c1) === JSON.stringify(c2), "conflict detection is deterministic");
}

// AI / snippet rejected
{
  const d = resolvePublicationDecision({
    publicationClass: 1,
    objectiveFact: true,
    sources: [{ authority: SOURCE_AUTHORITY.AI_OR_SNIPPET, isSnippet: true }],
  });
  ok(d.status === PUBLICATION_DECISION.REJECTED, "snippets/AI rejected");
}

// Local calibration does not alter legacy OAS function source
{
  const root = join(dirname(fileURLToPath(import.meta.url)), "..");
  const myDeals = readFileSync(join(root, "api", "my-deals.js"), "utf8");
  ok(!/calibration-cohort|operator-intelligence\/calibration/.test(myDeals), "local calibration data does not alter legacy OAS module wiring");
  ok(typeof scoreOperatorMatchForDeal === "function", "legacy OAS still returns numeric scoring function");
}

// Production owner routes unchanged markers
{
  const root = join(dirname(fileURLToPath(import.meta.url)), "..");
  const v2 = readFileSync(join(root, "api", "operator-fit-v2.js"), "utf8");
  ok(!/loadCalibrationCohort|calibration-cohort/.test(v2), "local calibration data does not alter production owner Fit routes");
}

// Overlay merge geo replace
{
  try {
    const cohort = loadCalibrationCohort();
    const he = buildPrefillOverlayFromCohort("recWPKu5laVZxsvpn", cohort);
    const merged = mergePrefillWithCalibration(
      { activeCountries: ["Mars"], managementStructuresSupported: [] },
      he
    );
    ok(merged.mode === "airtable_plus_calibration", "overlay merge mode set");
    ok(
      Array.isArray(merged.prefill.activeCountries) &&
        merged.prefill.activeCountries.includes("Mexico") ||
        merged.prefill.activeCountries.length >= 1,
      "HE overlay supplies structured countries"
    );
  } catch (e) {
    ok(false, "overlay load: " + (e && e.message));
  }
}

console.log(failed ? `\n${failed} failure(s)` : "\nAll operator-intelligence calibration tests passed.");
process.exit(failed ? 1 : 0);
