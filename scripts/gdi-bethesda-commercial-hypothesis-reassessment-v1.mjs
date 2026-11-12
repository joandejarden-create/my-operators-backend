#!/usr/bin/env node
/**
 * Zero-paid KnowBe4 + Ameresco reassessment under Commercial Hypothesis Development.
 * No paid research. No customer publication. No destructive inventory writes.
 */
import fs from "node:fs";
import path from "node:path";
import { execSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import {
  applyCommercialHypothesisDevelopmentInChain,
  developCommercialHypotheses,
} from "../lib/group-demand-intelligence/gdi-commercial-hypothesis-development-v1.js";
import {
  evaluateProactiveAccountDevelopment,
  evaluateProactivePublicationReviewReadiness,
} from "../lib/group-demand-intelligence/gdi-proactive-commercial-intelligence-standard-v1.js";
import { evaluateSharedCommercialEligibility } from "../lib/group-demand-intelligence/commercial-eligibility-contract-v1.js";
import { BETHESDA_HOTEL_ID } from "../lib/group-demand-intelligence/research/gdi-hotel-store-integrity-guard-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = "reports/gdi-bethesda-commercial-hypothesis-reassessment-v1";
const ORS = path.join(
  ROOT,
  "data/group-demand-intelligence/hotels",
  BETHESDA_HOTEL_ID,
  "opportunity-research-state",
);

function writeJson(rel, obj) {
  const p = path.join(ROOT, OUT, rel);
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, `${JSON.stringify(obj, null, 2)}\n`);
}
function writeText(rel, body) {
  const p = path.join(ROOT, OUT, rel);
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, body.endsWith("\n") ? body : `${body}\n`);
}
function load(id) {
  const doc = JSON.parse(fs.readFileSync(path.join(ORS, `${id}.json`), "utf8"));
  return doc.opportunity || doc;
}

const gitSha = (() => {
  try {
    return execSync("git rev-parse HEAD", { cwd: ROOT, encoding: "utf8" }).trim();
  } catch {
    return null;
  }
})();

const targets = [
  { id: "disc_5976f7572131", label: "KnowBe4" },
  { id: "disc_2fc664271289", label: "Ameresco" },
];

const beforeAfter = [];
for (const t of targets) {
  const raw = load(t.id);
  const before = {
    commercialDemandType: raw.commercialDemandType || null,
    whyThisHotel: String(raw.whyThisHotel || "").slice(0, 220),
    recommendedNextStep: String(raw.recommendedNextStep || "").slice(0, 220),
    publicationReviewReadyPrior: null,
  };
  const applied = applyCommercialHypothesisDevelopmentInChain(raw);
  const opp = applied.opportunity;
  const hyp = applied.hypothesis;
  const elig = evaluateSharedCommercialEligibility(opp, {
    hotel: { hotelId: BETHESDA_HOTEL_ID, displayName: "Bethesda Marriott" },
  });
  const ad = evaluateProactiveAccountDevelopment(opp);
  const pub = evaluateProactivePublicationReviewReadiness(opp, {
    eligibilityResult: elig,
  });
  beforeAfter.push({
    opportunityId: t.id,
    organizationName: opp.organizationName,
    before,
    after: {
      proposedRevenueType: hyp.proposedRevenueType,
      commercialDemandType: opp.commercialDemandType,
      confirmedCustomerRequirement: hyp.confirmedCustomerRequirement,
      demandConfidence: hyp.demandConfidence,
      primaryHypothesis: hyp.primaryHypothesis,
      secondaryHypothesis: hyp.secondaryHypothesis,
      geography: hyp.geography,
      salesRecommendation: hyp.salesRecommendation,
      accountDevelopment: ad.accountDevelopment,
      salesReady: elig.salesReady === true,
      publicationReviewReady: pub.publicationReviewReady === true,
      customerPublicationReady: false,
      blockers: pub.blockers,
      targetedResearchQuestions: hyp.targetedResearchQuestions,
    },
  });
}

const summary = {
  version: "gdi-bethesda-commercial-hypothesis-reassessment-v1",
  at: new Date().toISOString(),
  gitSha,
  hotelId: BETHESDA_HOTEL_ID,
  paidCalls: 0,
  customerPublication: false,
  publicationReviewReadyCount: beforeAfter.filter((r) => r.after.publicationReviewReady)
    .length,
  accountDevelopmentCount: beforeAfter.filter((r) => r.after.accountDevelopment).length,
  recommendation:
    "NO_GO_CUSTOMER_PUBLICATION__CONTINUE_TARGETED_GEOGRAPHY_RESEARCH_BEFORE_AD_PROMOTION",
};

writeJson("00-summary.json", summary);
writeJson("01-knowbe4-ameresco-before-after.json", { candidates: beforeAfter });

const md = `# Bethesda Commercial Hypothesis Reassessment

Generated: ${summary.at}
Git SHA: \`${gitSha || "unknown"}\`

## Verdict

KnowBe4 and Ameresco retain **credible primary Guest Rooms hypotheses** from verified triggers, with \`confirmedCustomerRequirement = UNKNOWN\`.

Neither is publication-review-ready: Bethesda Marriott hotel-specific geography is **CONTESTED_METRO** under the corrected standard (Arlington office / NRL D.C. campus + generic “same commercial market” copy is insufficient).

## Before → After

${beforeAfter
  .map(
    (r) => `### ${r.organizationName}

- **Before demand type:** ${r.before.commercialDemandType || "UNDETERMINED / unset"}
- **After proposed revenue:** ${r.after.proposedRevenueType}
- **Primary hypothesis:** ${r.after.primaryHypothesis?.revenueType} (${(r.after.primaryHypothesis?.demandMechanism || []).join(", ")})
- **Secondary:** ${r.after.secondaryHypothesis?.revenueType || "none"}
- **Geography:** ${r.after.geography?.tier} — ${r.after.geography?.reason}
- **AD / Sales Ready / Pub-review:** ${r.after.accountDevelopment} / ${r.after.salesReady} / ${r.after.publicationReviewReady}
- **What to verify:** ${(r.after.targetedResearchQuestions || []).slice(0, 3).join("; ")}
`,
  )
  .join("\n")}

## Safety

Paid calls: 0 · Customer publication: false · Inventory destructive writes: none
`;

writeText("FOUNDER-COMMERCIAL-HYPOTHESIS-REASSESSMENT.md", md);
writeText("README.md", "KnowBe4 + Ameresco CHD reassessment (offline, $0).\n");
console.log(JSON.stringify({ out: OUT, ...summary }, null, 2));
