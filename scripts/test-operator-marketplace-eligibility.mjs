import assert from "node:assert/strict";
import {
  evaluateOperatorMarketplaceEligibility,
  enrichOperatorListRowWithEligibility,
} from "../lib/company-workspace-access.js";
import {
  userCanAccessOwnerWorkspace,
  userCanAccessOperatorWorkspace,
} from "../lib/dealality/user-workspace-gates.js";

const activeRow = { dealStatus: "Active", companyName: "Test Op Co" };

assert.equal(
  evaluateOperatorMarketplaceEligibility(
    { "Company Type": "Hotel Management Company" },
    { isActiveSetup: true }
  ).operatorExplorerEligible,
  true
);

const ooYes = evaluateOperatorMarketplaceEligibility(
  {
    "Company Type": "Hotel Owner - Operator",
    "Third-Party Management Availability": "Yes",
  },
  { isActiveSetup: true }
);
assert.equal(ooYes.operatorExplorerEligible, true);
assert.equal(ooYes.reviewBeforeOutreach, false);
assert.equal(ooYes.isOwnerOperator, true);

const ooSelective = evaluateOperatorMarketplaceEligibility(
  {
    "Company Type": "Hotel Owner - Operator",
    "Third-Party Management Availability": "Selectively",
  },
  { isActiveSetup: true }
);
assert.equal(ooSelective.operatorExplorerEligible, true);
assert.equal(ooSelective.reviewBeforeOutreach, true);

const ooCaseByCase = evaluateOperatorMarketplaceEligibility(
  {
    "Company Type": "Hotel Owner - Operator",
    "Third-Party Management Availability": "Case-by-Case",
  },
  { isActiveSetup: true }
);
assert.equal(ooCaseByCase.operatorExplorerEligible, true);
assert.equal(ooCaseByCase.reviewBeforeOutreach, true);

const ooLegacyCase = evaluateOperatorMarketplaceEligibility(
  {
    "Company Type": "Hotel Owner - Operator",
    "Third-Party Management Availability": "case-by-case",
  },
  { isActiveSetup: true }
);
assert.equal(ooLegacyCase.operatorExplorerEligible, true);
assert.equal(ooLegacyCase.reviewBeforeOutreach, true);

const ooNo = evaluateOperatorMarketplaceEligibility(
  {
    "Company Type": "Hotel Owner - Operator",
    "Third-Party Management Availability": "No",
  },
  { isActiveSetup: true }
);
assert.equal(ooNo.operatorExplorerEligible, false);
assert.equal(ooNo.eligibilitySource, "third-party-unavailable");

const legacyMgmt = enrichOperatorListRowWithEligibility(activeRow, {
  "Company Type": "Hotel Management Company",
});
assert.equal(legacyMgmt.operatorExplorerEligible, true);
assert.equal(legacyMgmt.normalizedCompanyType, "HOTEL MGMT. COMPANY");

const ooLegacy = enrichOperatorListRowWithEligibility(activeRow, {
  "Company Type": "Hotel Owner - Operator",
});
assert.equal(ooLegacy.operatorExplorerEligible, true);
assert.equal(ooLegacy.normalizedCompanyType, "OWNER_OPERATOR");

const inactive = enrichOperatorListRowWithEligibility(
  { dealStatus: "Draft", companyName: "X" },
  { "Company Type": "Hotel Management Company" }
);
assert.equal(inactive.operatorExplorerEligible, false);

assert.equal(userCanAccessOwnerWorkspace({ isAdmin: false, flags: { isOwner: true } }), true);
assert.equal(
  userCanAccessOwnerWorkspace({
    companyType: "Hotel Owner - Operator",
    flags: { isOwner: true, isOperator: true },
    workspaceAccess: ["Owner", "Operator"],
  }),
  true
);
assert.equal(userCanAccessOwnerWorkspace({ flags: { isOperator: true } }), false);

assert.equal(userCanAccessOperatorWorkspace({ flags: { isOperator: true } }), true);
assert.equal(
  userCanAccessOperatorWorkspace({
    flags: { isOwner: true, isOperator: true },
    workspaceAccess: ["Owner", "Operator"],
  }),
  true
);
assert.equal(userCanAccessOperatorWorkspace({ flags: { isOwner: true } }), false);

assert.equal(
  userCanAccessOwnerWorkspace({
    isDemo: true,
    flags: { isDemo: true },
    workspaceAccess: ["Demo"],
  }),
  false
);
assert.equal(
  userCanAccessOperatorWorkspace({
    isDemo: true,
    flags: { isDemo: true },
    workspaceAccess: ["Demo"],
  }),
  false
);

console.log("test-operator-marketplace-eligibility: ok");
