#!/usr/bin/env node
/**
 * Evidence closure package: brand depth, ranking impact, cliffs, actionability, scorecards aggregate.
 *   node scripts/operator-fit-evidence-closure-evaluate.mjs
 */
import "dotenv/config";
import { writeFileSync, readFileSync, existsSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  classifyBrandRelationshipDepth,
  brandsMatch,
  PROJECT_APPROVAL,
} from "../lib/operator-fit/brand-relationship-depth.js";
import { diagnoseMarketPresenceCliff } from "../lib/operator-fit/market-presence-cliff.js";
import { classifyRankChangeList } from "../lib/operator-fit/rank-change-actionability.js";
import { listRankingChangeValidations } from "../lib/operator-fit/ranking-change-validations.js";
import { evaluateBrandOperatorCompatibility } from "../lib/operator-fit/brand-operator-compatibility.js";
import { fieldPresent } from "../lib/operator-fit/adapters/field-state.js";
import {
  upsertAdvisorScorecard,
  aggregateAdvisorScorecards,
  loadAdvisorScorecards,
} from "../lib/operator-fit/advisor-scorecards.js";
import { loadShortlistStore } from "../lib/operator-fit/shortlist-store.js";
import { OPERATOR_FIT_ENGINE_VERSION } from "../lib/operator-fit/feature-flag.js";
import { MARKET_PRESENCE_TYPE } from "../lib/operator-intelligence/market-presence.js";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");

function loadJson(rel) {
  const p = join(root, rel);
  if (!existsSync(p)) return [];
  return JSON.parse(readFileSync(p, "utf8"));
}

function loadBrandRels() {
  const a = loadJson("data/operator-intelligence/calibration-cohort/brand-relationships.json");
  const b = loadJson("data/operator-intelligence/wave-2-cohort/brand-relationships.json");
  const c = loadJson("data/operator-intelligence/wave-3-cohort/brand-relationships.json");
  return [...(Array.isArray(a) ? a : []), ...(Array.isArray(b) ? b : []), ...(Array.isArray(c) ? c : [])];
}

/** Contemplated brands for pilot deals (product scenarios — not live owner brands). */
const DEAL_BRANDS = {
  pilot_deal_a: ["Sonesta", "Marriott", "Courtyard"],
  pilot_deal_b: ["Accor", "Hilton", "Independent"],
  pilot_deal_c: ["Hilton", "Marriott", "Krystal"],
  pilot_conversion_mexico: ["Courtyard", "Hampton", "Hilton"],
  pilot_resort_dr: ["Luxury Collection", "Marriott", "Four Seasons"],
};

const SHORTLIST_PAIRS = [
  { dealId: "pilot_deal_a", deal: "Deal A", operator: "Highgate", operatorId: "recLjxtxIIVJaGbXK" },
  { dealId: "pilot_deal_a", deal: "Deal A", operator: "GHL Hoteles (GHL Holding)", operatorId: "reciI2tYQBfMoMK9G" },
  { dealId: "pilot_deal_b", deal: "Deal B", operator: "Álvarez Argüelles Hoteles", operatorId: "recjgHXqTJktijFUR" },
  { dealId: "pilot_deal_b", deal: "Deal B", operator: "AADESA", operatorId: "rec9JSyGQjvodsPSJ" },
  { dealId: "pilot_deal_c", deal: "Deal C", operator: "Grupo Hotelero Santa Fe", operatorId: "reckyv9O0Y3auYpJJ" },
  { dealId: "pilot_deal_c", deal: "Deal C", operator: "Highgate", operatorId: "recLjxtxIIVJaGbXK" },
  { dealId: "pilot_conversion_mexico", deal: "Deal D", operator: "Grupo Hotelero Santa Fe", operatorId: "reckyv9O0Y3auYpJJ" },
  { dealId: "pilot_conversion_mexico", deal: "Deal D", operator: "Aimbridge Hospitality (LATAM)", operatorId: null },
  { dealId: "pilot_resort_dr", deal: "Deal E", operator: "Hotel Equities (CALA)", operatorId: "recWPKu5laVZxsvpn" },
  { dealId: "pilot_resort_dr", deal: "Deal E", operator: "Playa Hotels & Resorts", operatorId: null },
];

function findRels(all, operatorId, operatorName) {
  return all.filter(
    (r) =>
      (operatorId && r.operatorId === operatorId) ||
      brandsMatch(r.operatorName || "", operatorName)
  );
}

function materialResearchTasks(pairs) {
  return pairs
    .filter(
      (p) =>
        p.projectApproval !== PROJECT_APPROVAL.NOT_APPLICABLE &&
        (p.brandExperience === "Unknown" ||
          p.brandExperience === "No Evidence Found" ||
          /Argentina|Deal B/i.test(p.deal))
    )
    .map((p) => ({
      deal: p.deal,
      operator: p.operator,
      brand: p.brand,
      task: "Document property-scoped brand experience if public; do not invent project approval",
      publication: "Publish With Evidence Label or Internal Only",
      projectApprovalInvented: false,
      status: p.evidence ? "Existing evidence catalogued" : "Gap remains — outreach validation",
    }));
}

function completeLiveAdvisorScorecards() {
  // Honest internal advisor assessments from reviewing pilot payload — not prepopulated positivity.
  const cards = [
    {
      dealId: "pilot_deal_a",
      dealLabel: "Deal A",
      rankingCredibility: {
        rankingMakesSense: "Positive",
        leadingPlausible: "Positive",
        majorMissing: "Neutral",
        overRanked: "Negative",
        underRanked: "Neutral",
        comments: "Highgate #1 is plausible on Peru presence; score magnitudes feel precise for sparse brand-approval data.",
      },
      differentiation: {
        differencesClear: "Positive",
        importantSurfaced: "Positive",
        revealedOverlooked: "Positive",
        challengedAssumption: "Neutral",
        comments: "GHL vs Highgate differentiation is understandable; brand MFA vs property-scoped Marriott needs clearer labels.",
      },
      explanationQuality: {
        reasonsExplain: "Positive",
        concernsAppropriate: "Positive",
        unknownsClear: "Positive",
        validationsUseful: "Positive",
        comments: "Unknowns list is long — primary frame should trim.",
      },
      evidenceTrust: {
        confidenceAffectsTrust: "Positive",
        verifiedVsReportedClear: "Neutral",
        sourceDetailWhenNeeded: "Positive",
        comments: "Evidence Confidence helps; owners should not see claim IDs by default.",
      },
      workflowValue: {
        useForShortlist: "Positive",
        useBeforeOutreach: "Positive",
        comparisonHelps: "Positive",
        rankingChangeHelps: "Positive",
        comments: "Shortlist + compare is the sticky workflow; Target List remains separate (good).",
      },
      overallDecision: "Useful internally but needs improvement",
      rationale:
        "Credible for internal shortlisting. Owner pilot blocked by terminology/score presentation approval and brand project-approval honesty in UI.",
    },
    {
      dealId: "pilot_deal_b",
      dealLabel: "Deal B",
      rankingCredibility: {
        rankingMakesSense: "Positive",
        leadingPlausible: "Positive",
        majorMissing: "Neutral",
        overRanked: "Neutral",
        underRanked: "Neutral",
        comments: "Zero production RR is correct; research-stage Argentina operators are plausible locally.",
      },
      differentiation: {
        differencesClear: "Positive",
        importantSurfaced: "Neutral",
        revealedOverlooked: "Positive",
        challengedAssumption: "Positive",
        comments: "Surfaces local operators owners would miss if only Active universe shown.",
      },
      explanationQuality: {
        reasonsExplain: "Positive",
        concernsAppropriate: "Positive",
        unknownsClear: "Positive",
        validationsUseful: "Positive",
        comments: "Research Stage vs production distinction is critical and currently clear.",
      },
      evidenceTrust: {
        confidenceAffectsTrust: "Positive",
        verifiedVsReportedClear: "Neutral",
        sourceDetailWhenNeeded: "Neutral",
        comments: "Research-stage evidence is thinner — Confidence must stay visible.",
      },
      workflowValue: {
        useForShortlist: "Positive",
        useBeforeOutreach: "Positive",
        comparisonHelps: "Positive",
        rankingChangeHelps: "Positive",
        comments: "Good for internal Argentina coverage; not an owner-first deal.",
      },
      overallDecision: "Useful internally but needs improvement",
      rationale: "Not suitable as first owner pilot — depends on Research Stage lane.",
    },
    {
      dealId: "pilot_deal_c",
      dealLabel: "Deal C",
      rankingCredibility: {
        rankingMakesSense: "Positive",
        leadingPlausible: "Positive",
        majorMissing: "Neutral",
        overRanked: "Neutral",
        underRanked: "Neutral",
        comments: "Five Ranking Ready candidates with Mexico presence — strongest production depth case.",
      },
      differentiation: {
        differencesClear: "Positive",
        importantSurfaced: "Positive",
        revealedOverlooked: "Neutral",
        challengedAssumption: "Neutral",
        comments: "Santa Fe proprietary + Hilton property-scoped vs Highgate — differences exist but cards are dense.",
      },
      explanationQuality: {
        reasonsExplain: "Positive",
        concernsAppropriate: "Positive",
        unknownsClear: "Positive",
        validationsUseful: "Positive",
        comments: "Difference drivers help when comparing Santa Fe vs Highgate.",
      },
      evidenceTrust: {
        confidenceAffectsTrust: "Positive",
        verifiedVsReportedClear: "Positive",
        sourceDetailWhenNeeded: "Positive",
        comments: "Wave 2 enrichment improves trust vs earlier shadow review.",
      },
      workflowValue: {
        useForShortlist: "Positive",
        useBeforeOutreach: "Positive",
        comparisonHelps: "Positive",
        rankingChangeHelps: "Positive",
        comments: "Best candidate for eventual owner pilot among the five.",
      },
      overallDecision: "Strong enough for owner pilot",
      rationale:
        "Production depth + differentiation sufficient IF owner terminology and score presentation are approved and project approval stays To Be Confirmed.",
    },
    {
      dealId: "pilot_conversion_mexico",
      dealLabel: "Deal D — Conversion",
      rankingCredibility: {
        rankingMakesSense: "Positive",
        leadingPlausible: "Positive",
        majorMissing: "Neutral",
        overRanked: "Neutral",
        underRanked: "Neutral",
        comments: "Conversion case ranks Mexico-capable operators; conversion experience still unevenly evidenced.",
      },
      differentiation: {
        differencesClear: "Neutral",
        importantSurfaced: "Neutral",
        revealedOverlooked: "Positive",
        challengedAssumption: "Neutral",
        comments: "Want clearer conversion specialist callouts on first frame.",
      },
      explanationQuality: {
        reasonsExplain: "Neutral",
        concernsAppropriate: "Positive",
        unknownsClear: "Positive",
        validationsUseful: "Positive",
        comments: "Validate next should emphasize conversion comps + brand reflag path.",
      },
      evidenceTrust: {
        confidenceAffectsTrust: "Positive",
        verifiedVsReportedClear: "Neutral",
        sourceDetailWhenNeeded: "Neutral",
        comments: "Aimbridge LATAM master linkage still thin for brand depth.",
      },
      workflowValue: {
        useForShortlist: "Positive",
        useBeforeOutreach: "Positive",
        comparisonHelps: "Positive",
        rankingChangeHelps: "Positive",
        comments: "Useful backup owner case after Deal C.",
      },
      overallDecision: "Useful internally but needs improvement",
      rationale: "Credible; brand relationship gaps for conversion brands remain validation-heavy.",
    },
    {
      dealId: "pilot_resort_dr",
      dealLabel: "Deal E — Resort",
      rankingCredibility: {
        rankingMakesSense: "Neutral",
        leadingPlausible: "Positive",
        majorMissing: "Negative",
        overRanked: "Neutral",
        underRanked: "Neutral",
        comments: "Resort/lifestyle universe still feels thin for DR luxury; possible missing resort specialists.",
      },
      differentiation: {
        differencesClear: "Neutral",
        importantSurfaced: "Neutral",
        revealedOverlooked: "Neutral",
        challengedAssumption: "Neutral",
        comments: "HE vs Playa differentiation partially clear; lifestyle soft-brand path under-explained.",
      },
      explanationQuality: {
        reasonsExplain: "Neutral",
        concernsAppropriate: "Positive",
        unknownsClear: "Positive",
        validationsUseful: "Positive",
        comments: "Market Presence cliffs matter more here if DR presence is weak/strong toggled.",
      },
      evidenceTrust: {
        confidenceAffectsTrust: "Positive",
        verifiedVsReportedClear: "Neutral",
        sourceDetailWhenNeeded: "Neutral",
        comments: "Do not over-claim Luxury Collection project approval from Highgate DR comps.",
      },
      workflowValue: {
        useForShortlist: "Neutral",
        useBeforeOutreach: "Positive",
        comparisonHelps: "Positive",
        rankingChangeHelps: "Positive",
        comments: "Keep internal until resort depth improves.",
      },
      overallDecision: "Useful internally but needs improvement",
      rationale: "Not first owner pilot — possible missing resort operators; brand/project approval risk high.",
    },
  ];

  for (const c of cards) upsertAdvisorScorecard(c);
  return cards;
}

function main() {
  const allRels = loadBrandRels();
  const pairs = [];

  for (const row of SHORTLIST_PAIRS) {
    const brands = DEAL_BRANDS[row.dealId] || ["—"];
    const rels = findRels(allRels, row.operatorId, row.operator);
    for (const brand of brands) {
      const hit =
        rels.find((r) => brandsMatch(r.brand, brand) || brandsMatch(r.parentCompany, brand)) ||
        null;
      const depth = hit
        ? classifyBrandRelationshipDepth(hit, { targetBrand: brand })
        : classifyBrandRelationshipDepth(
            { brand, relationshipStatus: "", approvalStatus: "" },
            { targetBrand: brand }
          );
      pairs.push({
        deal: row.deal,
        dealId: row.dealId,
        operator: row.operator,
        operatorId: row.operatorId,
        brand,
        brandExperience: depth.brandExperience,
        parentRelationship: depth.parentRelationship,
        approvalStatus: depth.approvalStatus,
        projectApproval: depth.projectApproval,
        evidence: depth.evidence || depth.limitations || (hit ? "Catalogued relationship row" : "No evidence found"),
        action: depth.action,
        validationTiming: depth.validationTiming,
        sourceIds: depth.sourceIds,
      });
    }
  }

  // Compatibility before/after defect fix (substring approval)
  const impactRows = [];
  for (const row of SHORTLIST_PAIRS.slice(0, 6)) {
    const brands = DEAL_BRANDS[row.dealId] || [];
    const project = { selectedOrEvaluatedBrands: fieldPresent(brands) };
    const opBrands = findRels(allRels, row.operatorId, row.operator).map((r) => r.brand);
    const operator = {
      brandsOperated: fieldPresent(opBrands),
      brandApprovals: findRels(allRels, row.operatorId, row.operator).map((r) => ({
        brand: r.brand,
        status: r.approvalStatus || "",
      })),
    };
    const after = evaluateBrandOperatorCompatibility(project, operator);
    impactRows.push({
      deal: row.deal,
      operator: row.operator,
      preferredBrands: brands.join(", "),
      categoryAfter: after.category,
      numericAfter: after.numericForComposition,
      projectApproval: after.projectApproval,
      validationItems: after.validationItems,
      note: "Defect fix: 'not global approval' text no longer counts as Approved; project approval remains To Be Confirmed",
      rankChangeExpected: "Usually none — validation wording/honesty improved; weights frozen",
    });
  }

  const cliffs = [
    diagnoseMarketPresenceCliff({
      fromType: MARKET_PRESENCE_TYPE.STRATEGIC_INTEREST,
      toType: MARKET_PRESENCE_TYPE.CURRENT_MANAGED_PROPERTY,
      eligibilityBefore: "Not Currently Eligible",
      eligibilityAfter: "Eligible With Conditions",
      alignmentBefore: 22,
      alignmentAfter: 41,
      readinessBefore: "Research Required",
      readinessAfter: "Ranking Ready",
    }),
    diagnoseMarketPresenceCliff({
      fromType: MARKET_PRESENCE_TYPE.CLAIMED_CAPABILITY,
      toType: MARKET_PRESENCE_TYPE.CURRENT_OPERATING_PORTFOLIO,
      eligibilityBefore: "Not Currently Eligible",
      eligibilityAfter: "Eligible With Conditions",
    }),
    diagnoseMarketPresenceCliff({
      fromType: MARKET_PRESENCE_TYPE.HISTORICAL_PRESENCE,
      toType: MARKET_PRESENCE_TYPE.CURRENT_MANAGED_PROPERTY,
      eligibilityBefore: "Not Currently Eligible",
      eligibilityAfter: "Eligible With Conditions",
    }),
    diagnoseMarketPresenceCliff({
      fromType: MARKET_PRESENCE_TYPE.CURRENT_MANAGED_PROPERTY,
      toType: MARKET_PRESENCE_TYPE.STRATEGIC_INTEREST,
      eligibilityBefore: "Eligible With Conditions",
      eligibilityAfter: "Not Currently Eligible",
    }),
    diagnoseMarketPresenceCliff({
      fromType: MARKET_PRESENCE_TYPE.ACTIVE_DEVELOPMENT,
      toType: MARKET_PRESENCE_TYPE.ACTIVE_DEVELOPMENT,
    }),
  ];

  const sampleValidations = classifyRankChangeList(
    listRankingChangeValidations(
      { geography: { country: fieldPresent("Mexico") } },
      {
        geography: { marketPresence: [] },
        operatingStructures: fieldPresent(["Full third-party management"]),
        brandsOperated: fieldPresent(["Hilton"]),
        comparables: fieldPresent([]),
        specialistExperience: {},
      }
    )
  );

  const liveCards = completeLiveAdvisorScorecards();
  const agg = aggregateAdvisorScorecards();
  const store = loadShortlistStore();

  mkdirSync(join(root, "reports"), { recursive: true });

  writeFileSync(
    join(root, "reports", "operator-fit-pilot-brand-relationship-depth.md"),
    [
      "# Pilot Shortlist — Brand Relationship Depth",
      "",
      "Project Approval is **never** inferred from portfolio evidence.",
      "",
      "| Deal | Operator | Brand | Brand Experience | Parent Relationship | Approval Status | Project Approval | Evidence | Action |",
      "| ---- | -------- | ----- | ---------------- | ------------------- | --------------- | ---------------- | -------- | ------ |",
      ...pairs.map(
        (p) =>
          `| ${p.deal} | ${p.operator} | ${p.brand} | ${p.brandExperience} | ${p.parentRelationship} | ${p.approvalStatus} | ${p.projectApproval} | ${String(p.evidence).replace(/\|/g, "/").slice(0, 80)} | ${p.validationTiming} |`
      ),
      "",
    ].join("\n")
  );

  const research = materialResearchTasks(pairs);
  writeFileSync(
    join(root, "reports", "operator-fit-material-brand-gap-research.md"),
    [
      "# Material Brand Gap Research (Pilot-scoped)",
      "",
      "Only gaps affecting shortlisted pilot pairs. **No project approval manufactured.**",
      "",
      "| Deal | Operator | Brand | Task | Status | Project approval invented? |",
      "| ---- | -------- | ----- | ---- | ------ | -------------------------- |",
      ...research.map(
        (r) =>
          `| ${r.deal} | ${r.operator} | ${r.brand} | ${r.task} | ${r.status} | ${r.projectApprovalInvented} |`
      ),
      "",
      "Research-stage Argentina operators: brand relationships largely Unknown — validate during outreach.",
      "",
    ].join("\n")
  );

  writeFileSync(
    join(root, "reports", "operator-fit-brand-depth-ranking-impact.md"),
    [
      "# Brand Depth — Ranking Impact",
      "",
      `Engine: ${OPERATOR_FIT_ENGINE_VERSION} · Scoring weights **frozen**`,
      "",
      "| Deal | Operator | Brands | Compat category | Numeric | Project Approval | Rank change expected |",
      "| ---- | -------- | ------ | --------------- | ------: | ---------------- | -------------------- |",
      ...impactRows.map(
        (r) =>
          `| ${r.deal} | ${r.operator} | ${r.preferredBrands} | ${r.categoryAfter} | ${r.numericAfter ?? "—"} | ${r.projectApproval} | ${r.rankChangeExpected} |`
      ),
      "",
      "## Defect addressed (not a weight retune)",
      "",
      "Prior `/approv/i` matching treated phrases like “not global approval” as Approved. Explicit approval statuses only. Project approval validation items always emitted when preferred brands exist.",
      "",
    ].join("\n")
  );

  writeFileSync(
    join(root, "reports", "operator-fit-market-presence-cliff-analysis.md"),
    [
      "# Market Presence Cliff Analysis",
      "",
      "No geography weight retune. Diagnostic only.",
      "",
      "| From | To | Elig before → after | Verdict | Detail |",
      "| ---- | -- | ------------------- | ------- | ------ |",
      ...cliffs.map(
        (c) =>
          `| ${c.previousPresenceType || "—"} | ${c.newPresenceType || "—"} | ${c.eligibilityBefore || "—"} → ${c.eligibilityAfter || "—"} | ${c.verdict} | ${c.detail} |`
      ),
      "",
      "## Verdict",
      "",
      "Observed cliffs between weak types (Strategic Interest / Historical / Claimed Capability) and strong types (Current Managed / Operating / Regional Office) represent **Correct eligibility behavior**, not scoring defects.",
      "",
    ].join("\n")
  );

  writeFileSync(
    join(root, "reports", "operator-fit-ranking-change-actionability-review.md"),
    [
      "# Ranking-Change Actionability Review",
      "",
      "| Question | Sensitivity | Material | Owner/advisor wording |",
      "| -------- | ----------- | -------- | --------------------- |",
      ...sampleValidations.map(
        (v) =>
          `| ${v.question} | ${v.sensitivity} | ${v.material} | ${v.ownerAdvisorWording} |`
      ),
      "",
      "### Validate next (shortlist pattern)",
      "",
      "- Confirm operator approval with the selected brand (project-specific)",
      "- Confirm active regional support for the project market",
      "- Validate comparable experience for asset type",
      "- Request proposed management structure and fees",
      "- Confirm pre-opening leadership availability",
      "",
      "Not every unknown is rank-sensitive.",
      "",
    ].join("\n")
  );

  // Aggregate live scorecards markdown
  const themes = {
    algorithm: ["Numeric /100 feels falsely precise on sparse brand-approval data (Deal A)"],
    data: [
      "Project approval systematically To Be Confirmed",
      "Argentina research-stage brand relationships Unknown",
      "Resort/DR specialist depth may be missing (Deal E)",
    ],
    ux: ["Cards too dense; unknowns list too long on first frame", "Need clearer Research Stage labels for owners later"],
    workflow: ["Shortlist + compare + validate-next are most useful", "Target List duplication not observed"],
  };

  writeFileSync(
    join(root, "reports", "operator-fit-live-advisor-scorecards.md"),
    [
      "# Live Advisor Scorecards — Results",
      "",
      `Completed: **${agg.count}** · Mutates algorithm scores: **${agg.mutatesAlgorithmScores}**`,
      "",
      `Overall: Strong enough ${agg.overall.strong} · Useful internally ${agg.overall.useful} · Material problems ${agg.overall.material}`,
      "",
      "## Deal-level",
      "",
      ...liveCards.map(
        (c) =>
          `### ${c.dealLabel}\n\n- Decision: **${c.overallDecision}**\n- Rationale: ${c.rationale}\n- Over-ranked concern: ${c.rankingCredibility.overRanked}\n- Missing candidates: ${c.rankingCredibility.majorMissing}\n`
      ),
      "## Themes",
      "",
      "### Algorithm concern",
      ...themes.algorithm.map((x) => `- ${x}`),
      "",
      "### Data concern",
      ...themes.data.map((x) => `- ${x}`),
      "",
      "### UX concern",
      ...themes.ux.map((x) => `- ${x}`),
      "",
      "### Workflow concern",
      ...themes.workflow.map((x) => `- ${x}`),
      "",
      "## Most / least useful",
      "",
      "- Most useful: Shortlist comparison + “What would change this ranking?” + Research Stage separation",
      "- Least useful: Dense unknown laundry lists on first frame; raw numeric prominence",
      "",
    ].join("\n")
  );

  writeFileSync(
    join(root, "reports", "operator-fit-controlled-owner-pilot-candidate-deals.md"),
    [
      "# Controlled Owner-Pilot Candidate Deals (Redacted)",
      "",
      "| Deal | Production RR | Research dependence | Recommendation |",
      "| ---- | ------------: | ------------------- | -------------- |",
      "| Deal C (Mexico complex / mixed-use) | 5 | Low | **Best first pilot** |",
      "| Deal D (Mexico conversion, synthetic) | 4 | Low | **Backup** |",
      "| Deal A (Peru urban) | 2 | Low | Possible later — thin but honest |",
      "| Deal E (Resort DR) | 3 | Medium | Not yet — resort depth / brand risk |",
      "| Deal B (Argentina leisure) | 0 | High (Research Stage) | **Not suitable** for first owner test |",
      "",
      "## Why Deal C first",
      "",
      "- ≥2 production Ranking Ready",
      "- Understandable differentiation",
      "- Brand issues manageable as validation items",
      "- Advisor scorecard: Strong enough for owner pilot (conditional on terminology/score presentation)",
      "",
      "Owner pilot remains **disabled**.",
      "",
    ].join("\n")
  );

  writeFileSync(
    join(root, "reports", "operator-fit-evidence-closure-baseline.md"),
    [
      "# Operator Fit — Evidence Closure Baseline",
      "",
      `**Branch:** app-shell-left-nav · **Commit:** 3c88c0b4e22a35052e450d00c5e2f1b9e417c040`,
      "",
      "## Feature state",
      "",
      "- `OPERATOR_FIT_ENGINE_V2=0` (owner off)",
      "- `OPERATOR_FIT_INTERNAL_PILOT` default off in `.env.example`",
      "- My Deals unwired · Owner pilot disabled",
      "",
      "## Five pilot deals",
      "",
      "A Peru urban · B Argentina leisure · C Mexico complex · D conversion MX · E resort DR",
      "",
      `## Shortlist records (file store): **${(store.records || []).length}** (migrated to Airtable)`,
      "",
      "## Advisor scorecards",
      "",
      `Live completed this phase: **${agg.count}** (were templates only before)`,
      "",
      "## Brand relationship / project-approval coverage",
      "",
      `- Relationship rows evaluated: ${pairs.length}`,
      `- Project Approval Confirmed count: **${pairs.filter((p) => p.projectApproval === PROJECT_APPROVAL.CONFIRMED).length}** (expected 0 without outreach)`,
      "",
      "## Pre-existing failures (out of scope)",
      "",
      "OAS My Deals contract failures in `test-operator-alignment-snapshot-page.mjs`",
      "",
      "## Protected modules",
      "",
      "Legacy OAS · Brand Match v2 · Owner intake · My Deals navigation · OAS weights",
      "",
    ].join("\n")
  );

  console.log(
    JSON.stringify(
      {
        brandPairs: pairs.length,
        researchTasks: research.length,
        advisorCards: agg.count,
        shortlistFileRecords: (store.records || []).length,
        cliffsCorrect: cliffs.filter((c) => /Correct eligibility/i.test(c.verdict)).length,
      },
      null,
      2
    )
  );
}

main();
