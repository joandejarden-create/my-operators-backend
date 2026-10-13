#!/usr/bin/env node
/**
 * Bethesda canonical contact merge — DRY RUN only.
 * Reads reachability eval; proposes merges; never writes production packages.
 *
 * Usage: npm run gdi:canonical-contact-merge-dry-run
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  createCanonicalPersonRegistry,
  simulateReachabilityRowMerge,
  summarizeMergeDecisions,
  extendCoverageWithMergeMetrics,
  MERGE_WRITE_MODE,
  FIELD_MERGE_ACTION,
} from "../lib/hotel-intelligence/contact-intelligence/canonical-merge-policy.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "..");

const REACH_PATH = path.join(
  root,
  "data/group-demand-intelligence/evals/bethesda-contact-reachability-v1.json"
);
const OUT_JSON = path.join(
  root,
  "data/group-demand-intelligence/evals/bethesda-canonical-contact-merge-dry-run.json"
);
const OUT_MD = path.join(
  root,
  "reports/group-demand-intelligence/bethesda-canonical-contact-merge-dry-run.md"
);
const POLICY_MD = path.join(
  root,
  "reports/contact-intelligence/canonical-contact-merge-policy.md"
);

const reach = JSON.parse(fs.readFileSync(REACH_PATH, "utf8"));
const registry = createCanonicalPersonRegistry();
const hotelId = "recLuxvwwxID7U2B8"; // hotel-specific DATA in report only

const allDecisions = [];
const rowSummaries = [];

for (const row of reach.results || []) {
  const { person, decisions } = simulateReachabilityRowMerge(registry, row, {
    writeMode: MERGE_WRITE_MODE.DRY_RUN,
    hotelId,
  });
  allDecisions.push(...decisions);
  rowSummaries.push({
    name: row.name,
    organization: row.organization,
    opportunityId: row.opportunityId,
    identity: row.identity?.decision,
    decisions: decisions.map((d) => ({
      field: d.field,
      action: d.action,
      reasonCode: d.reasonCode,
      safetyTier: d.safetyTier,
      previousValue: d.previousField?.value ?? null,
      proposedValue: d.proposedField?.value ?? null,
      explanation: d.explanation,
    })),
    proposedEmail: person?.fields?.EMAIL?.value || null,
    proposedMobile: person?.fields?.MOBILE?.value || null,
    proposedPhone: person?.fields?.PHONE?.value || null,
  });
}

const mergeSummary = summarizeMergeDecisions(allDecisions);
const snap = registry.snapshot();

// Projected coverage among reachability cohort people (proposed dry-run fields)
const people = snap.people;
const withEmail = people.filter((p) => p.fields.EMAIL?.value || p.fields.ROLE_EMAIL?.value).length;
const withPhone = people.filter((p) => p.fields.PHONE?.value || p.fields.MOBILE?.value).length;
const withBoth = people.filter(
  (p) =>
    (p.fields.EMAIL?.value || p.fields.ROLE_EMAIL?.value) &&
    (p.fields.PHONE?.value || p.fields.MOBILE?.value)
).length;

const beforeFunnel = reach.funnel || reach.coverage?.funnel || {};
const coverage = extendCoverageWithMergeMetrics(reach.coverage || {}, {
  mergeSummary,
  reuseAttempts: 0,
  reuseHits: 0,
  providerCallsAvoided: 0,
});

const report = {
  version: "bethesda_canonical_contact_merge_dry_run_v1",
  write_mode: MERGE_WRITE_MODE.DRY_RUN,
  production_writes: "PROHIBITED",
  auto_accept_safe: "DISABLED",
  paid_enrichment_global: "NOT_ENABLED",
  policy_module: "lib/hotel-intelligence/contact-intelligence/canonical-merge-policy.js",
  source_reachability: REACH_PATH,
  hotelId,
  generated_at: new Date().toISOString(),
  mergeSummary,
  projected_cohort_people: {
    people: people.length,
    withEmail,
    withPhone,
    withBoth,
  },
  before_funnel: beforeFunnel,
  coverage_rates: coverage.rates,
  rowSummaries,
  audit_sample: snap.auditLog.slice(0, 40),
  audit_count: snap.auditLog.length,
  mutated_production: snap.auditLog.some((a) => a.mutated === true),
  relationships: snap.relationships,
  operating_law_audit: [
    {
      issue: "Canonical field merge precedence + safety tiers",
      classification: "REUSABLE_PRODUCT_LOGIC",
      implementation: "canonical-merge-policy.js",
      regression: "test:canonical-contact-merge-policy",
    },
    {
      issue: "Person vs opportunity relationship separation",
      classification: "REUSABLE_PRODUCT_LOGIC",
      implementation: "createCanonicalPerson + createOpportunityRelationship",
      regression: "test:canonical-contact-merge-policy #12-13",
    },
    {
      issue: "Bethesda hotelId / reachability eval input",
      classification: "HOTEL_SPECIFIC_DATA",
      implementation: "evals/bethesda-*-v1.json",
      regression: "N/A — data",
    },
  ],
  recommendation: "READY FOR CONTROLLED CANONICAL MERGE PILOT",
};

fs.mkdirSync(path.dirname(OUT_JSON), { recursive: true });
fs.writeFileSync(OUT_JSON, JSON.stringify(report, null, 2));

function esc(s) {
  return String(s ?? "—").replace(/\|/g, "\\|");
}

const md = `# Bethesda Canonical Contact Merge — Dry Run

**Mode:** \`${MERGE_WRITE_MODE.DRY_RUN}\` · Production writes: **PROHIBITED** · AUTO_ACCEPT_SAFE: **DISABLED**
**Source:** \`bethesda-contact-reachability-v1.json\`
**Generated:** ${report.generated_at}

## A. Canonical merge decisions

| Action | Count |
|---|---:|
| ACCEPT_NEW_FIELD | ${mergeSummary.ACCEPT_NEW_FIELD} |
| REPLACE_WEAKER_FIELD | ${mergeSummary.REPLACE_WEAKER_FIELD} |
| CORROBORATE_EXISTING | ${mergeSummary.CORROBORATE_EXISTING} |
| HOLD_FOR_REVIEW | ${mergeSummary.HOLD_FOR_REVIEW} |
| REJECT_FIELD | ${mergeSummary.REJECT_FIELD} |
| NO_INCREMENTAL_VALUE | ${mergeSummary.NO_INCREMENTAL_VALUE} |

Audit entries: **${snap.auditLog.length}** · Any production mutation: **${report.mutated_production}**

## B. Bethesda projected reachability (cohort people)

| | Before (funnel) | After proposed merge (cohort people) |
|---|---:|---:|
| Usable email | ${beforeFunnel.usableEmail ?? "—"} | ${withEmail} |
| Usable phone | ${beforeFunnel.usablePhone ?? "—"} | ${withPhone} |
| Both | ${beforeFunnel.bothEmailAndPhone ?? "—"} | ${withBoth} |

> Funnel before = hotel opportunity grain (29). After = distinct people in reachability cohort with proposed fields (dry-run).

## Per-person proposals

| Person | Identity | Decisions | Proposed email | Proposed phone/mobile |
|---|---|---|---|---|
${rowSummaries
  .map(
    (r) =>
      `| ${esc(r.name)} | ${esc(r.identity)} | ${esc(r.decisions.map((d) => `${d.field}:${d.action}`).join("; "))} | ${esc(r.proposedEmail)} | ${esc(r.proposedMobile || r.proposedPhone)} |`
  )
  .join("\n")}

## C. Reuse

This dry-run seeds from reachability only (no prior canonical store). Cross-opportunity/hotel reuse is validated in the multi-hotel suite.

## D. Verdict

**${report.recommendation}**

Next controlled step: REVIEW_REQUIRED apply path for TIER_1_SAFE fields only — still not global Surfe enablement.
`;

fs.mkdirSync(path.dirname(OUT_MD), { recursive: true });
fs.writeFileSync(OUT_MD, md);

const policyMd = `# Canonical Contact Merge Policy

**Version:** canonical-contact-merge-policy-v1  
**Module:** \`lib/hotel-intelligence/contact-intelligence/canonical-merge-policy.js\`  
**Default write mode:** \`REVIEW_REQUIRED\` (code supports \`DRY_RUN\` / \`AUTO_ACCEPT_SAFE\` disabled for production)

## Principles

1. **Dealality owns WHO.** Surfe/providers may only contribute HOW TO REACH.
2. **Identity gate first** — never merge AMBIGUOUS / REJECTED / NOT_FOUND.
3. **Field ownership second** — accepted identity ≠ every field is theirs.
4. **Precedence** — never overwrite stronger evidence with weaker.
5. **Person ≠ opportunity** — event roles stay on relationship records.
6. **GDI Operating Law** — reusable logic in this module; hotel facts in data/config.

## Source class precedence (high → low)

HOTEL/USER VALIDATED → OFFICIAL DIRECT → CANONICAL INTERNAL → PROVIDER VERIFIED → PROVIDER ACCEPTED → OFFICIAL FUNCTIONAL → PROVIDER CORROBORATION → INFERRED

## Merge actions

| Action | Meaning |
|---|---|
| ACCEPT_NEW_FIELD | Fill missing field |
| REPLACE_WEAKER_FIELD | Stronger source replaces weaker (often TIER_2) |
| CORROBORATE_EXISTING | Same value; provenance only |
| HOLD_FOR_REVIEW | Equal-strength conflict / limited evidence |
| REJECT_FIELD | Identity/ownership/feedback block |
| NO_INCREMENTAL_VALUE | Weaker or duplicate main line |

## Safety tiers

| Tier | Examples | Behavior |
|---|---|---|
| TIER_1_SAFE | Missing email + ACCEPTED identity; exact corroboration | Eligible for future AUTO_ACCEPT_SAFE |
| TIER_2_REVIEW | Role→direct upgrade; limited evidence | REVIEW_REQUIRED |
| TIER_3_BLOCK | Ambiguous identity; phone collision; former employee | Never write |

## Write modes

| Mode | Effect |
|---|---|
| DRY_RUN | Propose + audit; in-memory PROPOSED_DRY_RUN only |
| REVIEW_REQUIRED | Default — human approval before write |
| AUTO_ACCEPT_SAFE | Code path exists; **disabled** until founder approval |

## Hotel / user feedback

States: CONFIRMED_CORRECT, CONFIRMED_WRONG, OUTDATED, LEFT_ORGANIZATION, WRONG_ROLE, WRONG_PERSON, VALID_BUT_NOT_DECISION_MAKER

Wrong / left-org → \`reuseBlocked\` (history retained).

## Regression

\`\`\`bash
npm run test:canonical-contact-merge-policy
\`\`\`
`;

fs.mkdirSync(path.dirname(POLICY_MD), { recursive: true });
fs.writeFileSync(POLICY_MD, policyMd);

console.log(
  JSON.stringify(
    {
      outJson: OUT_JSON,
      outMd: OUT_MD,
      policyMd: POLICY_MD,
      mergeSummary,
      projected: report.projected_cohort_people,
      mutated_production: report.mutated_production,
      recommendation: report.recommendation,
    },
    null,
    2
  )
);
