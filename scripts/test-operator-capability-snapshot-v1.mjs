#!/usr/bin/env node
/**
 * Operator Capability Snapshot v1 tests.
 *   node scripts/test-operator-capability-snapshot-v1.mjs
 */
import "dotenv/config";
import {
  buildOperatorCapabilitySnapshotV1,
  assessSnapshotStatus,
  SNAPSHOT_STATUS,
} from "../lib/operator-capability-snapshot-build.js";
import { fetchDealWithMergedLinkedRecords } from "../api/my-deals.js";
import { getOperatorCapabilitySnapshot } from "../api/operator-capability-snapshot.js";
import {
  PROJECT_TYPE_CANONICAL_OPTIONS,
  normalizeProjectTypeLabel,
} from "../lib/project-type.js";
import {
  DEALS_FIELDS,
  SI_FIELDS,
  NEEDS_REVIEW,
  strVal,
} from "../lib/operator-capability-inputs.js";
import { deriveCapabilityAreas } from "../lib/operator-capability-rules.js";
import {
  auditOperatorCapabilitySnapshotCopy,
  containsForbiddenOperatorLanguage,
  isBrandManagedPreferred,
} from "../lib/operator-capability-narrative.js";

let failed = 0;

function assert(cond, msg) {
  if (!cond) {
    console.error("FAIL:", msg);
    failed += 1;
  } else {
    console.log("ok:", msg);
  }
}

function mockRes() {
  const out = { statusCode: 200, body: null };
  return {
    status(c) {
      out.statusCode = c;
      return this;
    },
    json(b) {
      out.body = b;
      return this;
    },
    out,
  };
}

function testBlockedNoGuessedInference() {
  const fields = {
    "Project Type": "",
    "Who should receive bids for this project?": "Third-Party Operators Only (Management)",
    [SI_FIELDS.operatorCapabilityPriorities]: ["Full hotel management"],
  };
  const snap = buildOperatorCapabilitySnapshotV1(fields, "recTESTblocked");
  assert(snap.snapshotStatus === SNAPSHOT_STATUS.BLOCKED, "empty project type → blocked");
  const inferred = snap.capabilityAreas.filter((c) => c.strength === "inferred");
  assert(inferred.length === 0, "blocked snapshot has no inferred capabilities");
  assert(snap.capabilityAreas.length <= 1, "blocked only allows stated caps at most");
}

function testAcquisitionNotProjectType() {
  const fields = {
    "Project Type": "Acquisition of operating hotel",
    "Who should receive bids for this project?": "Third-Party Operators Only (Management)",
  };
  const snap = buildOperatorCapabilitySnapshotV1(fields, "recTESTacq");
  assert(snap.snapshotStatus === SNAPSHOT_STATUS.BLOCKED, "acquisition in project type → blocked");
  assert(/acquisition/i.test((snap.snapshotAccessReasons || []).join(" ")), "blocked reason mentions acquisition");
}

function testAllowedShape() {
  const fields = {
    "Project Type": "New Build",
    "Current Operating Model": "Third-party managed (branded)",
    "Opening / Transition Phase": "Construction",
    "Primary Market Region": "CALA",
    "Preferred Future Operating Model": "Third-party management only",
    "Operator Capability Priorities": ["Full hotel management"],
    "Owner Reporting Frequency": "Weekly",
    "Who should receive bids for this project?": "Third-Party Operators Only (Management)",
    "Plan to Self-Manage or Hire Third Party?": "Third-party Managed",
  };
  const snap = buildOperatorCapabilitySnapshotV1(fields, "recTESTallowed");
  assert(snap.snapshotStatus === SNAPSHOT_STATUS.ALLOWED, "complete new build → allowed (got " + snap.snapshotStatus + ")");
  assert(
    snap.reviewLabel.includes("Ready") || snap.reviewLabel.includes("owner/advisor"),
    "allowed review label"
  );
  assert(snap.disclaimer.includes("does not recommend"), "v1 disclaimer present");
  assert(Array.isArray(snap.ruleTriggers) && snap.ruleTriggers.length > 0, "rule triggers populated");
  assert(snap.capabilityAreas.length > 0, "allowed has capability areas");
}

function testLimitedHasClarifications() {
  const fields = {
    "Project Type": "Conversion / Reflag",
    "Current Operating Model": "Owner-operated (unbranded)",
    "Opening / Transition Phase": "Rebranding in place",
    "Primary Market Region": "CALA",
    "Preferred Future Operating Model": "Third-party management only",
    "Operator Capability Priorities": ["Full hotel management"],
    "Who should receive bids for this project?": "Third-Party Operators Only (Management)",
  };
  const snap = buildOperatorCapabilitySnapshotV1(fields, "recTESTlimited");
  assert(snap.snapshotStatus === SNAPSHOT_STATUS.LIMITED, "model tension → limited");
  assert(snap.clarifications.length > 0, "limited has clarifications");
  assert(snap.capabilityAreas.some((c) => c.strength === "inferred"), "limited still shows inferred areas");
}

function testOperatingModelTransitionNotConflict() {
  const fields = {
    "Project Type": "Conversion / Reflag",
    "Current Operating Model": "Owner-operated (unbranded)",
    "Opening / Transition Phase": "Rebranding in place",
    "Primary Market Region": "CALA",
    "Preferred Future Operating Model": "Third-party management only",
    "Operator Capability Priorities": ["Full hotel management"],
    "Who should receive bids for this project?": "Third-Party Operators Only (Management)",
  };
  const snap = buildOperatorCapabilitySnapshotV1(fields, "recTESTtransition");
  const transitions = snap.operatingModelTransitionsToValidate || [];
  assert(
    transitions.some((t) => /transition to validate/i.test(t)),
    "transition to validate label present"
  );
  assert(
    transitions.some((t) => /owner-operated while the preferred future model is third-party/i.test(t)),
    "neutral owner-operated → third-party transition copy"
  );
  const blob = JSON.stringify(snap);
  assert(!/operating model conflict/i.test(blob), "no operating model conflict wording in snapshot");
  assert(
    !(snap.clarifications || []).some((c) => /Review operating model consistency/i.test(c)),
    "clarifications use neutral transition language"
  );
  const pathway = (snap.operatingPathways || []).find((p) => p.id === "third_party");
  assert(pathway && /pathway to validate|structured review/i.test(pathway.relevance), "pathways are validation paths");
}

function testAmsterdamBrandManagedNarrative() {
  const fields = {
    "Project Type": "New Build",
    "Property Name": "Courtyard by Marriott Amsterdam Airport",
    "Current Operating Model": "Owner-operated (unbranded)",
    "Opening / Transition Phase": "Planning / entitlement",
    "Primary Market Region": "CALA",
    "Preferred Future Operating Model": "Brand-managed",
    "Operator Capability Priorities": ["Full hotel management"],
    "Owner Reporting Frequency": "Monthly",
    "Who should receive bids for this project?": "Brands Only",
    "Plan to Self-Manage or Hire Third Party?": "Brand-Managed",
    "Stage of Development": "Fully Entitled",
  };
  const snap = buildOperatorCapabilitySnapshotV1(fields, "recTESTamsterdam");
  assert(snap.snapshotStatus === SNAPSHOT_STATUS.LIMITED, "Amsterdam brand-managed → limited");
  assert(isBrandManagedPreferred(snap.operatingContext.preferredFutureOperatingModel), "brand-managed preferred");
  assert(
    (snap.brandManagedGuidance || []).some((l) => /appears in current inputs/i.test(l)),
    "brand-managed framed as input to validate"
  );
  assert(
    (snap.whyOperatorStrategyMatters || []).some((l) => /pathway to validate/i.test(l)),
    "pathway-to-validate language for brand-managed"
  );
  assert(
    (snap.operatingPathways || []).length >= 4,
    "operating pathways section populated"
  );
  assert(
    (snap.diligenceQuestions || []).length >= 5,
    "Amsterdam has 5+ diligence questions (got " + (snap.diligenceQuestions || []).length + ")"
  );
  assert(
    snap.diligenceQuestions.some((q) => /target brand manage the hotel directly/i.test(q)),
    "Amsterdam asks brand direct management"
  );
  assert(
    snap.diligenceQuestions.some((q) => /third-party manager/i.test(q)),
    "Amsterdam asks third-party fallback"
  );
  assert(
    snap.diligenceQuestions.some((q) => /pre-opening planning/i.test(q)),
    "Amsterdam asks pre-opening ownership"
  );
  assert((snap.newBuildGuidance || []).length >= 6, "new build guidance themes");
  const cap = snap.capabilityAreas[0];
  assert(cap && cap.whyItMayMatter && cap.whatToValidate && cap.relevance, "enriched capability card fields");
  assert(!auditOperatorCapabilitySnapshotCopy(snap), "Amsterdam snapshot passes copy audit");
}

function testExecutiveSummaryReadable() {
  const fields = {
    "Project Type": "Conversion / Reflag",
    "Current Operating Model": "Owner-operated (unbranded)",
    "Opening / Transition Phase": "Rebranding in place",
    "Primary Market Region": "CALA",
    "Preferred Future Operating Model": "Third-party management only",
    "Operator Capability Priorities": ["Conversion & PIP execution"],
    "Who should receive bids for this project?": "Third-Party Operators Only (Management)",
  };
  const snap = buildOperatorCapabilitySnapshotV1(fields, "recTESTexec");
  const summary = (snap.executiveSummary || []).join(" ");
  assert(/opportunity in CALA/i.test(summary), "executive summary names market");
  assert(/may need to be reviewed/i.test(summary), "executive summary uses neutral review language");
  assert(!/summarizes operating capability themes for/i.test(summary), "old database-style opener removed");
  assert((snap.ownerAdvisorReviewTakeaway || []).length >= 2, "owner/advisor takeaway present");
}

function testOutOfScopeClarificationNotDiligenceQuestion() {
  const fields = {
    "Project Type": "New Build",
    "Current Operating Model": "Owner-operated (unbranded)",
    "Opening / Transition Phase": "Construction",
    "Primary Market Region": "CALA",
    "Preferred Future Operating Model": "Undecided / exploring",
    "Operator Capability Priorities": ["Revenue management & distribution"],
    "Who should receive bids for this project?": "Brands Only",
  };
  const snap = buildOperatorCapabilitySnapshotV1(fields, "recTESTmedellin");
  const bad = (snap.diligenceQuestions || []).some((q) =>
    /third-party operator capabilities may be out of scope/i.test(q)
  );
  assert(!bad, "out-of-scope clarification is not turned into a diligence question");
}

function testLimitedHasExpandedNarrative() {
  const fields = {
    "Project Type": "Conversion / Reflag",
    "Current Operating Model": "Owner-operated (unbranded)",
    "Opening / Transition Phase": "Rebranding in place",
    "Primary Market Region": "CALA",
    "Preferred Future Operating Model": "Third-party management only",
    "Operator Capability Priorities": ["Full hotel management"],
    "Who should receive bids for this project?": "Third-Party Operators Only (Management)",
  };
  const snap = buildOperatorCapabilitySnapshotV1(fields, "recTESTnarrative");
  assert(snap.snapshotStatus === SNAPSHOT_STATUS.LIMITED, "limited deal for narrative test");
  assert((snap.executiveSummary || []).length >= 2, "expanded executive summary");
  assert((snap.whyOperatorStrategyMatters || []).length >= 1, "why operator strategy matters");
  assert((snap.capabilityImplications || []).length >= 1, "capability implications");
  assert((snap.decisionPointsBeforeOutreach || []).length >= 1, "decision points");
  assert((snap.knownGapsClarifications || []).length >= 1, "known gaps");
}

function testForbiddenPhraseGuard() {
  assert(containsForbiddenOperatorLanguage("Dealality recommends a third-party manager"), "catches Dealality recommends");
  assert(containsForbiddenOperatorLanguage("the owner should choose brand-managed"), "catches owner should");
  assert(!containsForbiddenOperatorLanguage("does not recommend, rank, endorse, or select operators"), "allows disclaimer phrase");
}

function testNoForbiddenOperatorRecommendationCopy() {
  const samples = [
    {
      "Project Type": "New Build",
      "Preferred Future Operating Model": "Brand-managed",
      "Who should receive bids for this project?": "Brands Only",
    },
    {
      "Project Type": "New Build",
      "Preferred Future Operating Model": "Third-party management only",
      "Operator Capability Priorities": ["Full hotel management"],
      "Who should receive bids for this project?": "Third-Party Operators Only (Management)",
      "Current Operating Model": "Third-party managed (branded)",
      "Opening / Transition Phase": "Construction",
      "Primary Market Region": "CALA",
      "Owner Reporting Frequency": "Weekly",
    },
  ];
  for (const fields of samples) {
    const snap = buildOperatorCapabilitySnapshotV1(fields, "recTESTcopy");
    assert(!auditOperatorCapabilitySnapshotCopy(snap), "full snapshot passes copy audit");
    assert(snap.disclaimer.includes("does not recommend"), "disclaimer preserved");
  }
}

function testXavierNeedsReview() {
  const fields = {
    "Project Type": "Renovation / Repositioning",
    [DEALS_FIELDS.currentOperatingModel]: NEEDS_REVIEW,
    [DEALS_FIELDS.openingTransitionPhase]: NEEDS_REVIEW,
    "Primary Market Region": "CALA",
    "Preferred Future Operating Model": "Undecided / exploring",
    "Operator Capability Priorities": ["Full hotel management", "HR & training"],
    "Owner Reporting Frequency": "Monthly",
    "Who should receive bids for this project?": "Third-Party Operators Only (Management)",
  };
  const snap = buildOperatorCapabilitySnapshotV1(fields, "recTESTxavier");
  assert(snap.snapshotStatus === SNAPSHOT_STATUS.LIMITED, "Xavier → limited");
  assert(snap.requiresManualReview === true, "Xavier requiresManualReview");
  assert(snap.reviewLabel === "Manual Review Required", "Xavier reviewLabel");
  const clar = snap.clarifications.join(" ");
  assert(/Current operating model flagged Needs Review/i.test(clar), "Xavier current OM clarification");
  assert(/Opening \/ Transition Phase flagged Needs Review/i.test(clar), "Xavier opening clarification");
}

function testCanonicalProjectTypeOnly() {
  for (const pt of PROJECT_TYPE_CANONICAL_OPTIONS) {
    const norm = normalizeProjectTypeLabel(pt);
    assert(norm === pt, "canonical PT normalizes to self: " + pt);
  }
}

async function testLiveDeals() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) {
    console.warn("skip live: no AIRTABLE credentials");
    return;
  }

  const Airtable = (await import("airtable")).default;
  const base = new Airtable({ apiKey }).base(baseId);
  const rows = await base(process.env.AIRTABLE_TABLE_DEALS || "Deals")
    .select({ pageSize: 100 })
    .all();

  let allowedCount = 0;
  let limitedCount = 0;
  let xavier = null;

  for (const row of rows) {
    const full = await fetchDealWithMergedLinkedRecords(baseId, apiKey, row.id);
    if (!full?.deal?.fields) continue;
    const snap = buildOperatorCapabilitySnapshotV1(full.deal.fields || {}, row.id);
    const rawPt = strVal(full.deal.fields?.["Project Type"]);
    if (rawPt && !PROJECT_TYPE_CANONICAL_OPTIONS.includes(rawPt)) {
      assert(false, "non-canonical stored PT: " + rawPt + " on " + snap.dealName);
    }
    if (/acquisition\s+of\s+operating/i.test(rawPt)) {
      assert(false, "acquisition in PT on " + snap.dealName);
    }
    if (snap.dealName && /xavier/i.test(snap.dealName)) xavier = snap;
    if (snap.snapshotStatus === SNAPSHOT_STATUS.ALLOWED) allowedCount += 1;
    if (snap.snapshotStatus === SNAPSHOT_STATUS.LIMITED) limitedCount += 1;

    const res = mockRes();
    await getOperatorCapabilitySnapshot(
      { params: { dealId: row.id, recordId: row.id } },
      res
    );
    assert(res.out.body?.success === true, "GET API for " + snap.dealName);
    assert(res.out.body?.snapshotStatus === snap.snapshotStatus, "GET matches builder status for " + snap.dealName);
  }

  if (xavier) {
    assert(xavier.snapshotStatus === SNAPSHOT_STATUS.LIMITED, "live Xavier limited");
    assert(xavier.requiresManualReview, "live Xavier manual review");
  } else {
    console.warn("warn: Xavier v2.0 not found in first 20 deals");
  }

  assert(xavier != null, "live Xavier v2.0 deal found");
  console.log(`live summary: ${allowedCount} allowed, ${limitedCount} limited (n=${rows.length})`);
}

async function main() {
  testCanonicalProjectTypeOnly();
  testBlockedNoGuessedInference();
  testAcquisitionNotProjectType();
  testAllowedShape();
  testLimitedHasClarifications();
  testOperatingModelTransitionNotConflict();
  testAmsterdamBrandManagedNarrative();
  testExecutiveSummaryReadable();
  testOutOfScopeClarificationNotDiligenceQuestion();
  testLimitedHasExpandedNarrative();
  testForbiddenPhraseGuard();
  testNoForbiddenOperatorRecommendationCopy();
  testXavierNeedsReview();
  if (!process.env.SKIP_OCS_LIVE) {
    await testLiveDeals();
  } else {
    console.log("skip live: SKIP_OCS_LIVE set");
  }

  if (failed) {
    console.error("\n" + failed + " test(s) failed");
    process.exit(1);
  }
  console.log("\nAll Operator Capability Snapshot v1 tests passed.");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
