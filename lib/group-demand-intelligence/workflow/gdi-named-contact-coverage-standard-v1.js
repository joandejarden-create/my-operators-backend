/**
 * Permanent GDI named-contact coverage standard V1.
 *
 * Customer-facing finalization requires ≥1 verified named person.
 * Two verified relevant people is the desired coverage whenever reasonably possible.
 * Zero named people ⇒ RESEARCH_CONTINUATION (not finalized).
 *
 * Org-route-only / homepage / desk labels never satisfy this gate for STAGED/CURRENT.
 */

import {
  isPlausibleBuyerPersonName,
  personQualifiesForWhoFinalization,
} from "./gdi-original-full-e2e-execution-restore-v1.js";

export const GDI_NAMED_CONTACT_COVERAGE_STANDARD_V1 =
  "gdi-named-contact-coverage-standard-v1";

export const NAMED_CONTACT_COVERAGE = Object.freeze({
  TWO_VERIFIED: "TWO_VERIFIED_NAMED",
  ONE_VERIFIED: "ONE_VERIFIED_NAMED",
  ZERO: "ZERO_VERIFIED_NAMED",
});

export const SECONDARY_CONTACT_RESEARCH_STATUS = Object.freeze({
  SECOND_VERIFIED: "SECOND_VERIFIED",
  ATTEMPTED_NO_SECOND_VERIFIED: "ATTEMPTED_NO_SECOND_VERIFIED",
  NOT_ATTEMPTED: "NOT_ATTEMPTED",
});

export const FINALIZATION_CONTACT_DISPOSITION = Object.freeze({
  FINALIZED_TWO_NAMED: "FINALIZED_TWO_NAMED",
  FINALIZED_ONE_NAMED: "FINALIZED_ONE_NAMED",
  RESEARCH_CONTINUATION: "RESEARCH_CONTINUATION",
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function gapLabel(name) {
  return /^(contact research required|buyer not yet identified|contact not yet identified|role not yet verified)$/i.test(
    text(name),
  );
}

function nameKey(name) {
  return text(name)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

/**
 * True when two contact objects refer to the same verified person identity.
 * Compares normalized names (and optional personIdentityKey when both present).
 */
export function isSameVerifiedPerson(a = {}, b = {}) {
  const ka = text(a?.personIdentityKey);
  const kb = text(b?.personIdentityKey);
  if (ka && kb && ka === kb) return true;
  const na = nameKey(a?.name || a?.fullName || a?.personName);
  const nb = nameKey(b?.name || b?.fullName || b?.personName);
  if (!na || !nb) return false;
  return na === nb;
}

/**
 * Enforce distinct primary vs secondary identities on an opportunity.
 * Clears duplicate secondary rather than inventing a replacement.
 *
 * @returns {{ opportunity: object, clearedDuplicateSecondary: boolean }}
 */
export function enforceDistinctPrimarySecondaryContacts(opportunity = {}) {
  const o = opportunity && typeof opportunity === "object" ? { ...opportunity } : {};
  const primary = o.primaryContact || null;
  const secondary = o.secondaryContact || null;
  if (
    primary &&
    secondary &&
    text(secondary.name) &&
    isSameVerifiedPerson(primary, secondary)
  ) {
    delete o.secondaryContact;
    if (o.secondaryContactName) delete o.secondaryContactName;
    return { opportunity: o, clearedDuplicateSecondary: true };
  }
  return { opportunity: o, clearedDuplicateSecondary: false };
}

/**
 * @param {object} opportunity
 * @param {{ looksLikePersonName?: (n: string) => boolean }} [opts]
 */
export function extractVerifiedNamedContacts(opportunity = {}, opts = {}) {
  const looksLike =
    typeof opts.looksLikePersonName === "function"
      ? opts.looksLikePersonName
      : isPlausibleBuyerPersonName;

  const primaryRaw = text(
    opportunity.primaryContact?.name ||
      opportunity.contactName ||
      opportunity.namedStakeholders?.[0]?.name,
  );
  const primaryPerson = {
    ...(opportunity.primaryContact || {}),
    name: primaryRaw,
    title:
      opportunity.primaryContact?.title ||
      opportunity.primaryContact?.role ||
      opportunity.primaryContact?.professionalTitle,
    organization:
      opportunity.primaryContact?.organization || opportunity.organizationName,
  };
  const primaryClass = text(
    primaryPerson.buyerClassification ||
      primaryPerson.classification ||
      primaryPerson.personKind ||
      "",
  );
  const primaryBlockedClass =
    /RELEVANT_ORGANIZATIONAL|ORGANIZATIONAL_ROUTE|GENERAL_BUSINESS|AUTHORITY_UNVERIFIED|UNVERIFIED|REJECTED|JUNK/i.test(
      primaryClass,
    );
  // Named-coverage floor: plausible person + role/org, but never junk / RELEVANT_ORG.
  // Commercial WHO finalization uses personQualifiesForWhoFinalization separately.
  const primaryNamedOk =
    primaryRaw &&
    !gapLabel(primaryRaw) &&
    looksLike(primaryRaw) &&
    !primaryBlockedClass &&
    (personQualifiesForWhoFinalization(primaryPerson) ||
      Boolean(text(primaryPerson.organization)) ||
      (text(primaryPerson.title || primaryPerson.role) &&
        text(primaryPerson.organization)));
  const primary = primaryNamedOk
      ? {
          name: primaryRaw,
          role: text(
            opportunity.primaryContact?.role ||
              opportunity.primaryContact?.title ||
              opportunity.primaryContact?.professionalTitle,
          ) || null,
          organization: text(
            opportunity.primaryContact?.organization ||
              opportunity.organizationName,
          ) || null,
          sourceUrl: opportunity.primaryContact?.sourceUrl || null,
          verificationStatus:
            opportunity.primaryContact?.verificationStatus || null,
          personIdentityKey:
            opportunity.primaryContact?.personIdentityKey || null,
        }
      : null;

  const secondaryRaw = text(opportunity.secondaryContact?.name);
  const secondaryPerson = {
    ...(opportunity.secondaryContact || {}),
    name: secondaryRaw,
    title:
      opportunity.secondaryContact?.title ||
      opportunity.secondaryContact?.role,
    organization:
      opportunity.secondaryContact?.organization ||
      opportunity.organizationName,
    buyerClassification:
      opportunity.secondaryContact?.buyerClassification ||
      opportunity.secondaryContact?.classification ||
      "SECONDARY_CONTACT",
  };
  const secondaryCandidate =
    secondaryRaw &&
    !gapLabel(secondaryRaw) &&
    looksLike(secondaryRaw) &&
    personQualifiesForWhoFinalization(secondaryPerson)
      ? {
          name: secondaryRaw,
          role: text(
            opportunity.secondaryContact?.role ||
              opportunity.secondaryContact?.title,
          ) || null,
          organization: text(
            opportunity.secondaryContact?.organization ||
              opportunity.organizationName,
          ) || null,
          sourceUrl: opportunity.secondaryContact?.sourceUrl || null,
          verificationStatus:
            opportunity.secondaryContact?.verificationStatus || null,
          personIdentityKey:
            opportunity.secondaryContact?.personIdentityKey || null,
        }
      : null;
  const secondary =
    secondaryCandidate &&
    (!primary || !isSameVerifiedPerson(primary, secondaryCandidate))
      ? secondaryCandidate
      : null;

  return { primary, secondary };
}

/**
 * Canonical coverage evaluation for finalization / STAGED / CURRENT eligibility.
 */
export function evaluateNamedContactCoverageStandard(opportunity = {}, opts = {}) {
  const { primary, secondary } = extractVerifiedNamedContacts(opportunity, opts);
  const namedCount = (primary ? 1 : 0) + (secondary ? 1 : 0);

  let coverage = NAMED_CONTACT_COVERAGE.ZERO;
  if (namedCount >= 2) coverage = NAMED_CONTACT_COVERAGE.TWO_VERIFIED;
  else if (namedCount === 1) coverage = NAMED_CONTACT_COVERAGE.ONE_VERIFIED;

  const explicitSecondaryStatus = text(
    opportunity.secondaryContactResearchStatus ||
      opportunity.contactCoverage?.secondaryResearchStatus ||
      "",
  );
  let secondaryResearchStatus = SECONDARY_CONTACT_RESEARCH_STATUS.NOT_ATTEMPTED;
  if (secondary) {
    secondaryResearchStatus = SECONDARY_CONTACT_RESEARCH_STATUS.SECOND_VERIFIED;
  } else if (
    opportunity.secondContactAttempted === true ||
    opportunity.contactInvestigation?.secondContactAttempted === true ||
    explicitSecondaryStatus ===
      SECONDARY_CONTACT_RESEARCH_STATUS.ATTEMPTED_NO_SECOND_VERIFIED ||
    (/NO_SECOND|NEEDS_ADDITIONAL|NOT_AVAILABLE/i.test(explicitSecondaryStatus) &&
      !/^NOT_ATTEMPTED$/i.test(explicitSecondaryStatus))
  ) {
    secondaryResearchStatus =
      SECONDARY_CONTACT_RESEARCH_STATUS.ATTEMPTED_NO_SECOND_VERIFIED;
  }

  let disposition = FINALIZATION_CONTACT_DISPOSITION.RESEARCH_CONTINUATION;
  if (namedCount >= 2) {
    disposition = FINALIZATION_CONTACT_DISPOSITION.FINALIZED_TWO_NAMED;
  } else if (namedCount === 1) {
    disposition = FINALIZATION_CONTACT_DISPOSITION.FINALIZED_ONE_NAMED;
  }

  // Named coverage finalization requires a primary person.
  // Secondary alone is not enough. RELEVANT_ORG / junk never become primary above.
  // Commercial DIRECT_BUYER / VIABLE WHO is enforced by personQualifiesForWhoFinalization
  // at Agent certification / Sales Ready — not by collapsing named coverage to zero.
  const finalizeEligible = Boolean(primary);
  const reason =
    !primary
      ? secondary
        ? "Secondary named contact only — primary commercial WHO required for finalization"
        : "No verified named person — RESEARCH_CONTINUATION; refuse customer finalization"
      : namedCount === 1
        ? secondaryResearchStatus ===
          SECONDARY_CONTACT_RESEARCH_STATUS.ATTEMPTED_NO_SECOND_VERIFIED
          ? "One verified named person; secondary researched without second verified contact"
          : "One verified named person; secondary research status must be recorded"
        : "Two verified named contacts";

  return {
    version: GDI_NAMED_CONTACT_COVERAGE_STANDARD_V1,
    coverage,
    namedCount,
    finalizeEligible,
    disposition,
    primary,
    secondary,
    secondaryResearchStatus,
    reason,
  };
}

/**
 * Attach coverage metadata onto an opportunity after contact investigation.
 */
export function attachNamedContactCoverageMetadata(
  opportunity = {},
  investigationPass = null,
) {
  // Never fabricate secondary attempts — require explicit evidence that secondary
  // research ran (investigation pass flag or prior documented status).
  const distinct = enforceDistinctPrimarySecondaryContacts(opportunity);
  const base = distinct.opportunity;
  const attemptedExplicitly =
    investigationPass?.secondContactAttempted === true ||
    base.secondContactAttempted === true ||
    text(base.secondaryContactResearchStatus).length > 0;
  const withFlags = {
    ...base,
    secondContactAttempted: attemptedExplicitly,
  };
  if (text(withFlags.secondaryContact?.name)) {
    withFlags.secondaryContactResearchStatus =
      SECONDARY_CONTACT_RESEARCH_STATUS.SECOND_VERIFIED;
    withFlags.secondContactAttempted = true;
  } else if (attemptedExplicitly) {
    withFlags.secondaryContactResearchStatus =
      SECONDARY_CONTACT_RESEARCH_STATUS.ATTEMPTED_NO_SECOND_VERIFIED;
    withFlags.secondaryContactResearchReason =
      investigationPass?.secondContactDisposition ||
      (investigationPass?.contactGaps || []).find((g) =>
        /second/i.test(String(g)),
      ) ||
      "Second relevant named contact not verified from available public evidence";
  } else {
    // Honest incomplete state — do not fabricate an attempt
    withFlags.secondaryContactResearchStatus =
      SECONDARY_CONTACT_RESEARCH_STATUS.NOT_ATTEMPTED;
    withFlags.secondaryResearchScheduled = true;
    withFlags.secondaryContactResearchReason =
      "Secondary-contact research not yet attempted; scheduled for continuation";
  }

  const evaled = evaluateNamedContactCoverageStandard(withFlags);
  return {
    ...withFlags,
    contactCoverage: {
      version: GDI_NAMED_CONTACT_COVERAGE_STANDARD_V1,
      coverage: evaled.coverage,
      namedCount: evaled.namedCount,
      disposition: evaled.disposition,
      secondaryResearchStatus: evaled.secondaryResearchStatus,
      reason: evaled.reason,
      at: new Date().toISOString(),
    },
    secondaryContactResearchStatus: evaled.secondaryResearchStatus,
  };
}

/**
 * Hard gate for STAGED / CURRENT package membership.
 * Returns ok:false when any opportunity has zero verified named contacts.
 */
export function assertFinalizedPackageNamedContactCoverage(
  opportunities = [],
  opts = {},
) {
  const blockers = [];
  const summary = {
    total: 0,
    oneNamed: 0,
    twoNamed: 0,
    zeroNamed: 0,
    secondaryAttemptDocumented: 0,
  };

  for (const o of opportunities || []) {
    summary.total += 1;
    const ev = evaluateNamedContactCoverageStandard(o, opts);
    if (ev.namedCount === 0) {
      summary.zeroNamed += 1;
      blockers.push({
        opportunityId: text(o.id || o.opportunityId) || null,
        organizationName: text(o.organizationName) || null,
        code: "ZERO_VERIFIED_NAMED_CONTACTS",
        disposition: ev.disposition,
        reason: ev.reason,
      });
    } else if (ev.namedCount === 1) {
      summary.oneNamed += 1;
      if (
        ev.secondaryResearchStatus ===
        SECONDARY_CONTACT_RESEARCH_STATUS.ATTEMPTED_NO_SECOND_VERIFIED
      ) {
        summary.secondaryAttemptDocumented += 1;
      } else if (opts.requireSecondaryAttemptDocumentation !== false) {
        blockers.push({
          opportunityId: text(o.id || o.opportunityId) || null,
          organizationName: text(o.organizationName) || null,
          code: "SECONDARY_CONTACT_RESEARCH_UNDOCUMENTED",
          disposition: ev.disposition,
          reason:
            "One named contact present but secondary-contact research attempt not documented",
        });
      }
    } else {
      summary.twoNamed += 1;
      summary.secondaryAttemptDocumented += 1;
    }
  }

  return {
    ok: blockers.length === 0,
    version: GDI_NAMED_CONTACT_COVERAGE_STANDARD_V1,
    summary,
    blockers,
  };
}

export default {
  GDI_NAMED_CONTACT_COVERAGE_STANDARD_V1,
  NAMED_CONTACT_COVERAGE,
  SECONDARY_CONTACT_RESEARCH_STATUS,
  FINALIZATION_CONTACT_DISPOSITION,
  isSameVerifiedPerson,
  enforceDistinctPrimarySecondaryContacts,
  extractVerifiedNamedContacts,
  evaluateNamedContactCoverageStandard,
  attachNamedContactCoverageMetadata,
  assertFinalizedPackageNamedContactCoverage,
};
