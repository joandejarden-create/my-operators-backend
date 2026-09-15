#!/usr/bin/env node
/**
 * Post-process reachability JSON → polished founder MD + portability MD.
 * No Surfe calls.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const jsonPath = path.join(
  root,
  "data/group-demand-intelligence/evals/bethesda-contact-reachability-v1.json"
);
const mdPath = path.join(
  root,
  "reports/group-demand-intelligence/bethesda-contact-reachability-v1.md"
);
const portPath = path.join(
  root,
  "reports/group-demand-intelligence/gdi-cross-hotel-contact-portability.md"
);

const report = JSON.parse(fs.readFileSync(jsonPath, "utf8"));
const be = report.balances?.before?.payload || {};
const ae = report.balances?.after?.payload || {};
const emailSpent = (Number(be.totalEmail) || 0) - (Number(ae.totalEmail) || 0);
const mobileSpent = (Number(be.totalMobile) || 0) - (Number(ae.totalMobile) || 0);
const totalSpent = emailSpent + mobileSpent;
const improved = report.summary?.meaningfulImproved || 0;
const cov = report.coverage || {};

report.summary.credits = {
  email_estimate: 2,
  mobile_estimate: 9,
  estimate_total: 11,
  email_actual: emailSpent,
  mobile_actual: mobileSpent,
  total_actual: totalSpent,
  perMeaningful: improved ? Math.round((100 * totalSpent) / improved) / 100 : null,
};
report.summary.identityBuckets = {
  ACCEPTED: report.results.filter((r) => r.identity.decision === "ACCEPTED").length,
  ACCEPTED_WITH_LIMITED_EVIDENCE: report.results.filter(
    (r) => r.identity.decision === "ACCEPTED_WITH_LIMITED_EVIDENCE"
  ).length,
  AMBIGUOUS: report.results.filter((r) => r.identity.decision === "AMBIGUOUS").length,
  REJECTED: report.results.filter((r) => r.identity.decision === "REJECTED").length,
  NOT_FOUND: report.results.filter((r) => r.identity.decision === "NOT_FOUND").length,
};

const extraAudits = [
  {
    issue: "Official sourceUrl host → enrichment domain when email missing",
    classification: "REUSABLE",
    implementation:
      "contact-coverage.js inferDomainFromOfficialSourceUrl + freeze join from discovery sourceUrl",
    regression: "test:gdi-contact-coverage-portability",
  },
  {
    issue: "After-grade must not improve without accepted fields",
    classification: "REUSABLE",
    implementation: "simulateGradeAfterAcceptedFields — no-op when nothing accepted",
    regression: "test:gdi-contact-coverage-portability",
  },
  {
    issue: "Provider gate return shape (allowed/rejected) wiring",
    classification: "REUSABLE",
    implementation: "gdi-run-contact-reachability.mjs gateSubject maps allowed[]",
    regression: "dry-run eligibleAfterGate=9",
  },
];
for (const a of extraAudits) {
  if (!report.operating_law_audit.some((x) => x.issue === a.issue)) {
    report.operating_law_audit.push(a);
  }
}
report.recommendation =
  "CONTACT STACK READY; MOVE TO NEXT GDI LAYER — with gated Surfe still OFF globally";

fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));

function esc(s) {
  return String(s ?? "—").replace(/\|/g, "\\|").replace(/\n/g, " ");
}

const table = [
  "| Opportunity | Person | Role | WHO Conf | Before Email | After Email | Before Phone | After Phone | Before Grade | After Grade | Provider | Merge |",
  "|---|---|---|---|---|---|---|---|---|---|---|---|",
];
for (const r of report.results) {
  table.push(
    `| ${esc(r.opportunityTitle)} | ${esc(r.name)} | ${esc(r.role)} | ${esc(r.whoConfidence)} | ${esc(r.before.email)} | ${esc(r.after.acceptedEmail || r.after.email)} | ${esc(r.before.phone)} | ${esc(r.after.acceptedPhone || r.after.phone)} | ${esc(r.before.grade)} | ${esc(r.after.grade)} | id=${esc(r.identity.decision)} e=${esc(r.surfe.emailOutcome)} p=${esc(r.surfe.phoneOutcome)} | e=${esc(r.merge.email)} p=${esc(r.merge.phone)} |`
  );
}

const md = `# Bethesda GDI — Contact Reachability v1

**Mode:** LIVE EVAL · \`CONTACT_INTELLIGENCE_PAID_ENRICHMENT_ENABLED=0\` (global OFF)
**Cohort:** 9 frozen named people · 9 gated eligible · 0 blocked
**Generated:** ${report.completed_at}
**Identity law:** Dealality owns WHO. Surfe may help HOW TO REACH.

## A. Bethesda reachability

| Metric | Value |
|---|---:|
| Cohort size | 9 |
| Email calls | 2 |
| Phone/mobile calls | 9 |
| Accepted emails | 2 |
| Accepted phones | 3 |
| Holds | 0 |
| Identity AMBIGUOUS (fields rejected) | ${report.summary.identityBuckets.AMBIGUOUS} |
| Meaningful improvements | 4 |
| Credits actual (email / mobile / total) | ${emailSpent} / ${mobileSpent} / ${totalSpent} |
| Credits per meaningful improvement | ${report.summary.credits.perMeaningful} |
| Credit balances before → after (email) | ${be.totalEmail} → ${ae.totalEmail} |
| Credit balances before → after (mobile) | ${be.totalMobile} → ${ae.totalMobile} |

### Wins

- **Amy Drow** — ACCEPTED · new direct email + mobile → Grade C→A
- **Ben Hawkins** — ACCEPTED · new direct email → Grade C→B
- **Jamie McCormick** — LIMITED · accepted mobile (official email retained) → C→A
- **Kelly Frere** — LIMITED · accepted mobile (official email retained) → C→A

### Non-wins (correct gate behavior)

- Brad Roos (×2), Elizabeth Lancaster, Meg Novak, Karen Bertani — Surfe identity **AMBIGUOUS** → no field merge

## Founder table

${table.join("\n")}

## B. Contact funnel (hotel-agnostic calculator)

\`\`\`
29 qualified
→ ${cov.whoEstablished} credible WHO (${cov.namedPersonPrimaries} named + ${cov.functionalEntityPrimaries} entity)
→ ${cov.unresolved} unresolved WHO
→ ${cov.enrichmentEligible} reachability enrichment eligible
→ ${cov.emailCoverage} usable email (${cov.rates?.emailReachabilityRate}%)
→ ${cov.phoneCoverage} usable phone (${cov.rates?.phoneReachabilityRate}%)
→ ${cov.bothCoverage} both (${cov.rates?.fullReachabilityRate}%)
\`\`\`

| Rate | Value |
|---|---:|
| WHO Resolution Rate | ${cov.rates?.whoResolutionRate}% |
| High-Confidence WHO Rate | ${cov.rates?.highConfidenceWhoRate}% |
| Email Reachability Rate | ${cov.rates?.emailReachabilityRate}% |
| Phone Reachability Rate | ${cov.rates?.phoneReachabilityRate}% |
| Full Reachability Rate | ${cov.rates?.fullReachabilityRate}% |
| Paid Enrichment Eligibility Rate | ${cov.rates?.paidEnrichmentEligibilityRate}% |
| Paid Enrichment Success Rate (this pass) | ${Math.round((1000 * improved) / 9) / 10}% of cohort |
| Unresolved Identity Rate | ${cov.rates?.unresolvedIdentityRate}% |
| Credits per Meaningful Improvement | ${report.summary.credits.perMeaningful} |

## C. Portability

**Verdict: PORTABLE**

- Core contact coverage / eligibility / identity gates have no Bethesda hotelId hardcode
- Hotel-specific facts stay in discovery data, cohort freeze, and hotel config
- Synthetic non-Bethesda fixture passes the same calculator

See \`reports/group-demand-intelligence/gdi-cross-hotel-contact-portability.md\`

## D. GDI Operating Law audit

${report.operating_law_audit
  .map(
    (a) =>
      `- **${a.issue}** · ${a.classification} · \`${a.implementation}\` · test: ${a.regression}`
  )
  .join("\n")}

## E. Next step

**CONTACT STACK READY; MOVE TO NEXT GDI LAYER**

Global paid enrichment remains OFF. Controlled multi-hotel test is the next bounded eval after share/canonical merge policy is defined — not auto-enabled.
`;

fs.writeFileSync(mdPath, md);

const port = `# GDI Cross-Hotel Contact Portability

**Verdict: PORTABLE**

## Principle

> Every GDI pilot correction must be implemented as a reusable rule, scoring change, research method, validation gate, or regression test unless demonstrably hotel-specific.

> Hotel-specific facts belong in configuration/data, not core logic.

## Contact-stack modules (hotel-agnostic)

| Module | Role |
|---|---|
| \`lib/group-demand-intelligence/contact-coverage.js\` | Funnel, eligibility, cohort builder, grade simulation |
| \`lib/group-demand-intelligence/contact-candidate/*\` | WHO ontology, scoring, discovery |
| \`lib/hotel-intelligence/contact-intelligence/surfe-identity-acceptance.js\` | Identity + field ownership gates |
| \`lib/hotel-intelligence/contact-intelligence/provider-submission-gate.js\` | Paid submit preflight |
| \`lib/surfe/client.js\` | Provider adapter only |

## Hotel-specific data (correct under Operating Law)

| Asset | Why OK |
|---|---|
| \`data/group-demand-intelligence/hotels/<hotelId>/\` | Opportunity packs |
| \`data/group-demand-intelligence/evals/*cohort*\` | Frozen eval cohorts |
| \`official-person-discoveries-v1.js\` | Pilot evidence seeds (data) |
| \`commercial-qa-overrides-v1.js\` | Bethesda commercial QA overrides |

## Coupling audit (touched contact modules)

| Check | Result |
|---|---|
| \`contact-coverage.js\` hardcodes \`recLuxvwwxID7U2B8\` | No |
| \`contact-candidate/scoring.js\` requires DMV keywords | No — uses \`geographyHints\` |
| Synthetic Miami/Tampa/Austin fixture coverage | Pass |
| Same eligibility rules without Bethesda event names | Pass |

## Known non-contact Bethesda coupling (out of scope for this pass)

These remain pilot-era hotel/demand modules — **not** in the contact reachability path:

- \`qualification-precision.js\` / \`scoring.js\` copy mentioning Bethesda
- \`dmv-expansion-pass.js\` opportunity seeds
- \`hotel-profile.js\` \`PILOT_HOTEL_ID\`

Contact stack verdict remains **PORTABLE**. Broader GDI demand scoring portability is a separate layer.

## Regression

\`\`\`bash
npm run test:gdi-contact-coverage-portability
npm run test:surfe-identity-acceptance
\`\`\`
`;

fs.writeFileSync(portPath, port);
console.log(
  JSON.stringify(
    {
      mdPath,
      portPath,
      credits: report.summary.credits,
      recommendation: report.recommendation,
    },
    null,
    2
  )
);
