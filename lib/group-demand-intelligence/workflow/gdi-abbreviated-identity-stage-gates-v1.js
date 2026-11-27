/**
 * Permanent WHO/HOW + Stage 4 abbreviated-identity gates.
 *
 * A partial / abbreviated name must not:
 * - pass as a fully verified professional identity
 * - be submitted for ambiguous Findymail enrichment
 *
 * Reuses presentation classifier + Findymail eligibility.
 */
import {
  auditAbbreviatedContactIdentities,
  classifyPersonIdentityCompleteness,
  isFullyVerifiedPersonName,
  PERSON_IDENTITY_COMPLETENESS,
  GDI_ABBREVIATED_CONTACT_IDENTITY_V1,
} from "../presentation/gdi-abbreviated-contact-identity-v1.js";
import { assessFindymailEmailEligibility } from "./gdi-findymail-stage4-enrichment-v1.js";

export const GDI_ABBREVIATED_IDENTITY_STAGE_GATES_V1 =
  "gdi-abbreviated-identity-stage-gates-v1";

/**
 * WHO/HOW person gate — abbreviated surnames cannot be identityVerified=true.
 */
export function evaluateWhoHowAbbreviatedIdentityGate(person = {}) {
  const name = String(person.fullName || person.name || "").trim();
  const completeness = classifyPersonIdentityCompleteness(name);
  const full = completeness === PERSON_IDENTITY_COMPLETENESS.FULL;
  const claimedVerified = person.identityVerified === true;
  return {
    ok: !claimedVerified || full,
    name: name || null,
    completeness,
    identityVerifiedAllowed: full,
    blockReason: full
      ? null
      : claimedVerified
        ? "ABBREVIATED_NAME_CANNOT_BE_IDENTITY_VERIFIED"
        : completeness === PERSON_IDENTITY_COMPLETENESS.EMPTY
          ? "NAME_EMPTY"
          : "PARTIAL_IDENTITY_ABBREVIATED_OR_INCOMPLETE",
  };
}

/**
 * Stage 4 Findymail gate — incomplete surnames are never eligible.
 */
export function evaluateStage4AbbreviatedIdentityGate(person = {}, opportunity = {}) {
  const name = String(person.fullName || person.name || "").trim();
  const full = isFullyVerifiedPersonName(name);
  const eligibility = assessFindymailEmailEligibility(person, opportunity);
  const blockedByAbbrev =
    !full && eligibility.reason === "IDENTITY_INCOMPLETE";
  return {
    ok: full || !eligibility.eligible,
    name: name || null,
    findymailEligible: eligibility.eligible === true,
    findymailReason: eligibility.reason,
    blockedByAbbreviatedIdentity: blockedByAbbrev || (!full && eligibility.eligible),
    completeness: classifyPersonIdentityCompleteness(name),
  };
}

/**
 * Hotel inventory gate used by Stage 4 / WHO-HOW completion audits.
 */
export function evaluateHotelAbbreviatedIdentityGates(opportunities = []) {
  const audit = auditAbbreviatedContactIdentities(opportunities);
  const whoHow = [];
  const stage4 = [];
  for (const o of opportunities) {
    const person = o.primaryContact || {};
    whoHow.push({
      opportunityId: o.id || o.opportunityId,
      ...evaluateWhoHowAbbreviatedIdentityGate(person),
    });
    stage4.push({
      opportunityId: o.id || o.opportunityId,
      ...evaluateStage4AbbreviatedIdentityGate(person, o),
    });
  }
  const whoHowFailures = whoHow.filter((r) => !r.ok);
  const stage4Failures = stage4.filter((r) => !r.ok);
  const presentationDrift = audit.rows.filter((r) => r.presentationDrift);
  return {
    version: GDI_ABBREVIATED_IDENTITY_STAGE_GATES_V1,
    classifier: GDI_ABBREVIATED_CONTACT_IDENTITY_V1,
    checked: opportunities.length,
    audit,
    whoHowFailures,
    stage4Failures,
    presentationDrift,
    ok:
      whoHowFailures.length === 0 &&
      stage4Failures.length === 0 &&
      presentationDrift.length === 0,
  };
}
