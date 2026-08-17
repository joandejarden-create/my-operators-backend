/**
 * Canonical Operator Explorer readiness (shared dry-run + Airtable).
 * Policy: docs/product/operator-explorer-readiness-canonical-policy.md
 *
 * Named assignments only — callers must exclude aggregate/representative rows.
 */

export const EXPLORER_USEFULNESS = Object.freeze({
  STRONG: "Strong Profile",
  USEFUL: "Useful Profile",
  THIN: "Thin Profile",
  NOT_PUBLISHABLE: "Not Publishable",
});

/**
 * @param {object} input
 * @param {number} input.namedAssignmentCount
 * @param {number} input.distinctCountryCount — from Market Presence and/or Assignments
 * @param {number} input.distinctBrandNameCount — Brand Relationships and/or Assignments
 * @param {number} [input.track] — 1 or 2; Track 2 requires BMC for content-complete Useful
 * @param {boolean} [input.hasBrandManagedCapability]
 * @param {string|null} [input.recordPurpose] — Production | Research | Test Fixture
 */
export function classifyExplorerReadiness(input) {
  const namedAssignmentCount = Number(input.namedAssignmentCount || 0);
  const distinctCountryCount = Number(input.distinctCountryCount || 0);
  const distinctBrandNameCount = Number(input.distinctBrandNameCount || 0);
  const track = Number(input.track || 0);
  const hasBmc = input.hasBrandManagedCapability === true;
  const recordPurpose = input.recordPurpose || null;

  const researchCompleteEnough =
    namedAssignmentCount >= 1 || distinctCountryCount >= 1 || distinctBrandNameCount >= 1;

  const contentThin =
    namedAssignmentCount < 2 ||
    distinctCountryCount === 0 ||
    (track === 2 && !hasBmc);

  const contentStrong =
    namedAssignmentCount >= 5 && distinctCountryCount >= 2 && distinctBrandNameCount >= 2;

  let contentClass = EXPLORER_USEFULNESS.USEFUL;
  if (contentStrong) contentClass = EXPLORER_USEFULNESS.STRONG;
  else if (contentThin) {
    contentClass =
      namedAssignmentCount === 0
        ? EXPLORER_USEFULNESS.NOT_PUBLISHABLE
        : EXPLORER_USEFULNESS.THIN;
  }

  const contentComplete =
    contentClass === EXPLORER_USEFULNESS.STRONG || contentClass === EXPLORER_USEFULNESS.USEFUL;

  const isTestFixture = recordPurpose === "Test Fixture";
  const isResearch = recordPurpose === "Research";
  const isProduction = recordPurpose === "Production";

  const explorerPublishable = isProduction && contentComplete && !isTestFixture;
  const strongExplorerProfile = explorerPublishable && contentClass === EXPLORER_USEFULNESS.STRONG;
  const contentCompleteButLifecycleGated = isResearch && contentComplete;

  let usefulness = contentClass;
  if (isTestFixture) usefulness = EXPLORER_USEFULNESS.NOT_PUBLISHABLE;
  else if (contentCompleteButLifecycleGated) usefulness = EXPLORER_USEFULNESS.THIN;
  else if (explorerPublishable && contentClass === EXPLORER_USEFULNESS.STRONG) {
    usefulness = EXPLORER_USEFULNESS.STRONG;
  } else if (explorerPublishable) usefulness = EXPLORER_USEFULNESS.USEFUL;
  else if (!contentComplete && namedAssignmentCount === 0) {
    usefulness = EXPLORER_USEFULNESS.NOT_PUBLISHABLE;
  } else if (!contentComplete) usefulness = EXPLORER_USEFULNESS.THIN;

  return {
    researchCompleteEnough,
    contentClass,
    contentComplete,
    contentCompleteButLifecycleGated,
    explorerPublishable,
    strongExplorerProfile,
    usefulness,
    gates: {
      namedAssignmentCount,
      distinctCountryCount,
      distinctBrandNameCount,
      track,
      hasBrandManagedCapability: hasBmc,
      recordPurpose,
    },
  };
}

/** Fit diagnostic only — not Fit production eligibility. */
export function classifyFitDataReadinessDiagnostic(input) {
  const asg = Number(input.namedAssignmentCount || 0);
  const mp = Number(input.marketPresenceRowCount || input.distinctCountryCount || 0);
  const br = Number(input.brandRelationshipCount || input.distinctBrandNameCount || 0);
  if (asg >= 6 && mp >= 3 && br >= 2) return "Fit Data Ready";
  if (asg >= 3 || (br >= 1 && mp >= 1)) return "Conditional";
  return "Research Required";
}

export function isAggregateAssignmentName(name) {
  return /representative|examples\b|enterprise\b|various\b/i.test(String(name || ""));
}
