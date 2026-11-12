#!/usr/bin/env node
/**
 * Zero-paid Bethesda recovery reassessment under Proactive Commercial Intelligence Standard.
 * No paid research. No customer publication. No Airtable / destructive inventory writes.
 */
import fs from "node:fs";
import path from "node:path";
import { execSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { classifyCommercialDemandThesis } from "../lib/group-demand-intelligence/commercial-demand-thesis-standard-v1.js";
import {
  applyProactiveOutreachThesisInChain,
  evaluateFivePartCommercialThesis,
  evaluateProactiveAccountDevelopment,
} from "../lib/group-demand-intelligence/gdi-proactive-commercial-intelligence-standard-v1.js";
import {
  attachCommercialDemandTypeInChain,
  evaluatePublicationReviewReadiness,
  reconcileUnresolvedQuestionsAfterContact,
  reconstructProviderRunIdFromOpportunity,
} from "../lib/group-demand-intelligence/workflow/gdi-watchlist-finalization-handoff-v1.js";
import { evaluateAccountDevelopmentEligibility } from "../lib/group-demand-intelligence/account-development-eligibility-v1.js";
import { orgDeepKey } from "../lib/group-demand-intelligence/research/gdi-paid-deep-two-stage-dedupe-v1.js";
import { BETHESDA_HOTEL_ID } from "../lib/group-demand-intelligence/research/gdi-hotel-store-integrity-guard-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL_ID = BETHESDA_HOTEL_ID;
const OUT = "reports/gdi-bethesda-proactive-ci-reassessment-v1";
const ORS_DIR = path.join(
  ROOT,
  "data/group-demand-intelligence/hotels",
  HOTEL_ID,
  "opportunity-research-state",
);
const PRIOR =
  "reports/gdi-bethesda-fourteen-segment-offline-replay-postmortem-v1/07-watchlist-value-recovery.json";

const FOCUS_ORGS = [
  "knowbe4",
  "astrazeneca",
  "ameresco",
  "x-energy",
  "x energy",
  "workday",
  "true anomaly",
  "menlo digital",
  "hidden level",
  "energy bar",
  "rock creek mansion",
];

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function writeJson(rel, obj) {
  const p = path.join(ROOT, OUT, rel);
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, `${JSON.stringify(obj, null, 2)}\n`);
  return p;
}

function writeText(rel, body) {
  const p = path.join(ROOT, OUT, rel);
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, body.endsWith("\n") ? body : `${body}\n`);
  return p;
}

function loadOrs(id) {
  const p = path.join(ORS_DIR, `${id}.json`);
  const doc = JSON.parse(fs.readFileSync(p, "utf8"));
  return doc.opportunity || doc;
}

function matchesFocus(orgName) {
  const k = text(orgName).toLowerCase();
  return FOCUS_ORGS.some((f) => k.includes(f));
}

function classifyFinal(row) {
  if (row.publicationReviewReady) return "PUBLICATION_REVIEW_READY";
  if (row.accountDevelopment) return "ACCOUNT_DEVELOPMENT";
  if (row.demandConfidence === "RELIABLE_INFERRED_DEMAND" || row.demandConfidence === "CONFIRMED_DEMAND") {
    return "RESEARCH_CONTINUATION_WITH_CREDIBLE_TRIGGER";
  }
  if (row.crossSegmentDuplicateOf) return "DUPLICATE_OR_EXCLUDED";
  return "RESEARCH_CONTINUATION";
}

const prior = JSON.parse(fs.readFileSync(path.join(ROOT, PRIOR), "utf8"));
const gitSha = (() => {
  try {
    return execSync("git rev-parse HEAD", { cwd: ROOT, encoding: "utf8" }).trim();
  } catch {
    return null;
  }
})();

const byOrg = new Map();
const rows = [];

for (const rec of prior.recoveries || []) {
  const id = rec.opportunityId || rec.id;
  if (!id) continue;
  let opp;
  try {
    opp = loadOrs(id);
  } catch {
    continue;
  }
  if (!matchesFocus(opp.organizationName || rec.organizationName)) continue;

  const orgKey = orgDeepKey(opp);
  const priorForOrg = byOrg.get(orgKey);
  let crossSegmentDuplicateOf = null;
  if (priorForOrg && priorForOrg.segment !== (opp.researchSegment || rec.segment)) {
    crossSegmentDuplicateOf = priorForOrg.opportunityId;
  }

  let working = { ...opp };
  working = reconcileUnresolvedQuestionsAfterContact(working).opportunity;
  working = applyProactiveOutreachThesisInChain(working).opportunity;
  working = attachCommercialDemandTypeInChain(working);

  const demand = classifyCommercialDemandThesis(working);
  const five = evaluateFivePartCommercialThesis(working);
  const baseAd = evaluateAccountDevelopmentEligibility(working);
  const proactiveAd = evaluateProactiveAccountDevelopment(working);
  const pub = evaluatePublicationReviewReadiness(working);
  const provider = reconstructProviderRunIdFromOpportunity(working);

  const row = {
    opportunityId: id,
    organizationName: working.organizationName,
    orgKey,
    segment: working.researchSegment || rec.segment || null,
    title: working.title || working.programmeName || null,
    demandConfidence: demand.thesisStandard,
    demandReason: demand.reason,
    commercialDemandType: working.commercialDemandType || null,
    fivePartComplete: five.complete === true,
    fivePartGaps: five.gaps,
    verifiedTrigger: five.trigger?.primaryTrigger || null,
    namedEntryPoint: five.parts?.VERIFIED_NAMED_COMMERCIAL_ENTRY_POINT?.detail || null,
    whyReachOutNow: five.whyReachOutNow || null,
    accountDevelopment: proactiveAd.accountDevelopment === true,
    accountDevelopmentStatus: proactiveAd.status,
    accountDevelopmentGaps: proactiveAd.gaps,
    baseAdStatus: baseAd.status,
    publicationReviewReady: pub.publicationReviewReady === true,
    customerPublicationReady: false,
    publicationBlockers: pub.blockers || [],
    providerRunId: provider.providerRunId,
    providerRunIdSource: provider.source,
    crossSegmentDuplicateOf,
    remainingBlockers: [
      ...(five.gaps || []),
      ...(proactiveAd.accountDevelopment ? [] : proactiveAd.gaps || []),
    ].filter((v, i, a) => a.indexOf(v) === i),
  };
  row.finalClassification = classifyFinal(row);
  rows.push(row);
  if (!byOrg.has(orgKey)) {
    byOrg.set(orgKey, { opportunityId: id, segment: row.segment });
  }
}

const newlyEligibleAd = rows.filter((r) => r.accountDevelopment);
const pubReady = rows.filter((r) => r.publicationReviewReady);
const inventoryPath = path.join(
  ROOT,
  "data/group-demand-intelligence/hotels",
  HOTEL_ID,
  "opportunities.json",
);
const inventory = JSON.parse(fs.readFileSync(inventoryPath, "utf8"));
const inventoryIds = new Set(
  (Array.isArray(inventory) ? inventory : inventory.opportunities || []).map(
    (o) => o.id || o.opportunityId,
  ),
);

const summary = {
  version: "gdi-bethesda-proactive-ci-reassessment-v1",
  at: new Date().toISOString(),
  gitSha,
  hotelId: HOTEL_ID,
  uniqueCandidatesReviewed: rows.length,
  uniqueOrganizations: byOrg.size,
  reliableOrConfirmed: rows.filter((r) =>
    /CONFIRMED_DEMAND|RELIABLE_INFERRED_DEMAND/.test(r.demandConfidence),
  ).length,
  speculative: rows.filter((r) => r.demandConfidence === "SPECULATIVE_DEMAND").length,
  accountDevelopmentNewlyEligible: newlyEligibleAd.length,
  publicationReviewReadyBodies: pubReady.length,
  customerPublication: false,
  paidCalls: 0,
  inventoryProtected: {
    canonicalOpportunityCount: inventoryIds.size,
    note: "No destructive writes; 88 research IDs / existing cards untouched",
  },
  recommendation:
    newlyEligibleAd.length > 0
      ? "GO_FOR_FOUNDER_REVIEW_OF_PROACTIVE_AD_BODIES__NO_GO_BROAD_PAID"
      : "NO_GO_BROAD_PAID__CONTINUE_ZERO_PAID_RESEARCH_GAPS",
};

writeJson("00-summary.json", summary);
writeJson("01-candidate-reassessments.json", { candidates: rows });
writeJson("02-publication-review-ready.json", {
  count: pubReady.length,
  bodies: pubReady.map((r) => ({
    opportunityId: r.opportunityId,
    organizationName: r.organizationName,
    demandConfidence: r.demandConfidence,
    commercialDemandType: r.commercialDemandType,
    whyReachOutNow: r.whyReachOutNow,
    namedEntryPoint: r.namedEntryPoint,
    note: "Staged for founder review only — customerPublicationReady remains false",
  })),
});

const md = `# Bethesda Proactive Commercial Intelligence Reassessment

Generated: ${summary.at}
Git SHA: \`${gitSha || "unknown"}\`
Hotel: Bethesda Marriott (${HOTEL_ID})

## Policy applied

- CONFIRMED_DEMAND / RELIABLE_INFERRED_DEMAND may qualify; SPECULATIVE stays research continuation
- Confirmed hotel booking / RFP / room-night volume are **not** universal requirements
- Five-part thesis required for Account Development + publication-review staging
- \`publicationReviewReady\` ≠ \`customerPublicationReady\`

## Results

| Metric | Count |
|---|---|
| Unique candidates reviewed | ${summary.uniqueCandidatesReviewed} |
| Unique organizations | ${summary.uniqueOrganizations} |
| CONFIRMED or RELIABLE_INFERRED | ${summary.reliableOrConfirmed} |
| SPECULATIVE | ${summary.speculative} |
| Account Development eligible | ${summary.accountDevelopmentNewlyEligible} |
| Publication-review-ready bodies | ${summary.publicationReviewReadyBodies} |

## Candidate table

| Org | Segment | Demand | Maturity | Pub-review | Remaining blockers |
|---|---|---|---|---|---|
${rows
  .map(
    (r) =>
      `| ${r.organizationName} | ${r.segment || "—"} | ${r.demandConfidence} | ${r.finalClassification} | ${r.publicationReviewReady ? "YES" : "no"} | ${(r.remainingBlockers || []).slice(0, 3).join("; ") || "—"} |`,
  )
  .join("\n")}

## Genuine Account Development / publication-review-ready

${
  pubReady.length
    ? pubReady
        .map(
          (r) =>
            `- **${r.organizationName}** (${r.opportunityId}) — ${r.demandConfidence}; entry point: ${typeof r.namedEntryPoint === "object" ? r.namedEntryPoint?.name : r.namedEntryPoint}; outreach grounded in verified trigger.`,
        )
        .join("\n")
    : "_None — historical evidence still insufficient for five-part completion._"
}

## Safety

- Paid calls: **0**
- Customer publication: **false**
- Inventory destructive writes: **none**
- Recommendation: **${summary.recommendation}**
`;

writeText("FOUNDER-PROACTIVE-CI-REASSESSMENT.md", md);
writeText("README.md", "Bethesda proactive CI reassessment (offline, $0).\n");

console.log(
  JSON.stringify(
    {
      out: OUT,
      ...summary,
      orgs: rows.map((r) => ({
        org: r.organizationName,
        demand: r.demandConfidence,
        ad: r.accountDevelopment,
        pub: r.publicationReviewReady,
      })),
    },
    null,
    2,
  ),
);
