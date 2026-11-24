/**
 * Permanent GDI named-contact coverage standard V1.
 *
 * Customer-facing finalization requires ≥1 verified named person.
 * Two verified relevant people is the desired coverage whenever reasonably possible.
 * Zero named people ⇒ RESEARCH_CONTINUATION (not finalized).
 *
 * Org-route-only / homepage / desk labels never satisfy this gate for STAGED/CURRENT.
 */

import { isPlausibleBuyerPersonName } from "./gdi-original-full-e2e-execution-restore-v1.js";

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
  const primary =
    primaryRaw && !gapLabel(primaryRaw) && looksLike(primaryRaw)
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
        }
      : null;

  const secondaryRaw = text(opportunity.secondaryContact?.name);
  const secondary =
    secondaryRaw &&
    !gapLabel(secondaryRaw) &&
    looksLike(secondaryRaw) &&
    (!primary || secondaryRaw.toLowerCase() !== primary.name.toLowerCase())
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
        }
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
    /ATTEMPTED|NO_SECOND|NEEDS_ADDITIONAL|NOT_AVAILABLE/i.test(
      explicitSecondaryStatus,
    ) ||
    explicitSecondaryStatus ===
      SECONDARY_CONTACT_RESEARCH_STATUS.ATTEMPTED_NO_SECOND_VERIFIED
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

  const finalizeEligible = namedCount >= 1;
  const reason =
    namedCount === 0
      ? "No verified named person — RESEARCH_CONTINUATION; refuse customer finalization"
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
  const withFlags = {
    ...opportunity,
    secondContactAttempted:
      investigationPass?.secondContactAttempted === true ||
      opportunity.secondContactAttempted === true ||
      true,
  };
  if (
    investigationPass?.secondContactFound !== true &&
    !text(withFlags.secondaryContact?.name)
  ) {
    withFlags.secondaryContactResearchStatus =
      SECONDARY_CONTACT_RESEARCH_STATUS.ATTEMPTED_NO_SECOND_VERIFIED;
    withFlags.secondaryContactResearchReason =
      investigationPass?.secondContactDisposition ||
      (investigationPass?.contactGaps || []).find((g) =>
        /second/i.test(String(g)),
      ) ||
      "Second relevant named contact not verified from available public evidence";
  } else if (text(withFlags.secondaryContact?.name)) {
    withFlags.secondaryContactResearchStatus =
      SECONDARY_CONTACT_RESEARCH_STATUS.SECOND_VERIFIED;
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
  extractVerifiedNamedContacts,
  evaluateNamedContactCoverageStandard,
  attachNamedContactCoverageMetadata,
  assertFinalizedPackageNamedContactCoverage,
};
