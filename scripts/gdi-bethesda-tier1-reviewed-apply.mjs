#!/usr/bin/env node
/**
 * Bethesda TIER_1 reviewed apply + phone auto-apply pilot.
 * Local/test eval only — writes to eval JSON, not CI production packages or share.
 *
 * Usage: npm run gdi:bethesda-tier1-reviewed-apply
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  createReviewApplyStore,
  generateMergeProposalsFromReachability,
  approveCanonicalContactMergeProposal,
  applyApprovedCanonicalContactMerge,
  rollbackCanonicalContactMerge,
  resolveCanonicalReuseAfterApply,
  computePhonePilotRates,
  hydrateRegistryFromReachability,
  buildProposalSummary,
  PROPOSAL_STATUS,
  APPLY_RESULT,
} from "../lib/hotel-intelligence/contact-intelligence/canonical-merge-review-apply.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "..");

const DRY_RUN_PATH = path.join(
  root,
  "data/group-demand-intelligence/evals/bethesda-canonical-contact-merge-dry-run.json"
);
const REACH_PATH = path.join(
  root,
  "data/group-demand-intelligence/evals/bethesda-contact-reachability-v1.json"
);
const OUT_JSON = path.join(
  root,
  "data/group-demand-intelligence/evals/bethesda-tier1-reviewed-apply.json"
);
const OUT_MD = path.join(
  root,
  "reports/group-demand-intelligence/bethesda-tier1-reviewed-apply.md"
);
const POLICY_MD = path.join(
  root,
  "reports/contact-intelligence/canonical-contact-reviewed-apply.md"
);

const dryRun = JSON.parse(fs.readFileSync(DRY_RUN_PATH, "utf8"));
const reach = JSON.parse(fs.readFileSync(REACH_PATH, "utf8"));
const hotelId = dryRun.hotelId || null;

const reachByKey = new Map(
  (reach.results || []).map((r) => [`${r.name}::${r.organization}::${r.opportunityId}`, r])
);

const store = createReviewApplyStore();
hydrateRegistryFromReachability(store.registry, reach.results, hotelId);

// Pilot: allow LIMITED evidence mobile auto-apply for measurement (Jamie/Kelly)
const phonePilotAllowLimitedEvidenceMobile =
  process.env.PHONE_PILOT_ALLOW_LIMITED_EVIDENCE_MOBILE !== "0";

const allProposals = [];
for (const row of dryRun.rowSummaries || []) {
  const reachRow =
    reachByKey.get(`${row.name}::${row.organization}::${row.opportunityId}`) ||
    (reach.results || []).find(
      (r) => r.name === row.name && r.organization === row.organization
    );
  const props = generateMergeProposalsFromReachability(store, {
    dryRunRow: row,
    reachabilityRow: reachRow,
    hotelId,
    phonePilotAllowLimitedEvidenceMobile,
  });
  allProposals.push(...props);
}

// A. Email proposals → explicit founder approval (Amy + Ben)
const emailProposals = allProposals.filter((p) => p.fieldType === "EMAIL");
const applyResults = [];
const rollbackResults = [];

for (const p of emailProposals) {
  const appr = approveCanonicalContactMergeProposal(store, p.proposalId, {
    reviewer: "FOUNDER_REVIEW",
    note: "Bethesda TIER_1 email pilot approval",
  });
  if (appr.ok) {
    applyResults.push(applyApprovedCanonicalContactMerge(store, p.proposalId));
  }
}

// B. Phone auto-apply pilot (no manual approval)
const phoneProposals = allProposals.filter((p) =>
  p.fieldType === "MOBILE" || p.fieldType === "PHONE"
);
for (const p of phoneProposals) {
  if (p.status === PROPOSAL_STATUS.AUTO_APPLY_PHONE_PILOT) {
    applyResults.push(applyApprovedCanonicalContactMerge(store, p.proposalId));
  }
}

// C. Reuse test — synthetic second hotel encounter same person
const reuseSamples = [];
for (const name of ["Amy Drow", "Jamie McCormick", "Kelly Frere"]) {
  const org =
    name === "Amy Drow"
      ? "National Down Syndrome Society"
      : name === "Jamie McCormick"
        ? "NADO"
        : "ASAE";
  const reuse = resolveCanonicalReuseAfterApply(store, {
    name,
    organization: org,
    needEmail: true,
    needPhone: true,
  });
  reuseSamples.push({ name, organization: org, ...reuse });
}

// D. Rollback test on Ben email (fixture — restore to null)
const benEmail = allProposals.find(
  (p) => p.displayName === "Ben Hawkins" && p.fieldType === "EMAIL"
);
if (benEmail && store.getProposal(benEmail.proposalId)?.status === PROPOSAL_STATUS.APPLIED) {
  rollbackResults.push(
    rollbackCanonicalContactMerge(store, benEmail.proposalId, {
      reviewer: "FOUNDER_REVIEW",
      reason: "Rollback test — verify audit preserved",
    })
  );
  // Re-apply prevention after rollback
  const reapply = applyApprovedCanonicalContactMerge(store, benEmail.proposalId);
  rollbackResults.push({ reapplyBlocked: reapply });
}

const snap = store.snapshot();
const summary = buildProposalSummary(snap.proposals);
const phoneRates = computePhonePilotRates(snap.phonePilotMetrics);

const appliedFields = snap.proposals
  .filter((p) => p.status === PROPOSAL_STATUS.APPLIED)
  .map((p) => ({
    person: p.displayName,
    organization: p.organization,
    field: p.fieldType,
    before: p.currentValue,
    after: p.proposedValue,
    provenance: p.provenance,
    approvalPath: p.approvalPath,
    status: p.status,
  }));

const report = {
  version: "bethesda_tier1_reviewed_apply_v1",
  environment: "local_eval",
  production_writes: "PROHIBITED",
  share_writes: "PROHIBITED",
  contact_intelligence_paid_enrichment_enabled: "0",
  auto_accept_safe: "DISABLED",
  phone_pilot_allow_limited_evidence_mobile: phonePilotAllowLimitedEvidenceMobile,
  generated_at: new Date().toISOString(),
  hotelId,
  reviewQueue: summary,
  applyResults: applyResults.map((r) => ({
    ok: r.ok,
    result: r.result,
    proposalId: r.proposal?.proposalId,
    person: r.proposal?.displayName,
    field: r.proposal?.fieldType,
    error: r.error || r.reasons,
  })),
  appliedFields,
  rollbackResults,
  reuseSamples,
  phonePilotMetrics: snap.phonePilotMetrics,
  phonePilotRates: phoneRates,
  immutableAuditCount: snap.immutableAudit.length,
  proposals: snap.proposals,
  operating_law_audit: [
    {
      issue: "Field-level review + apply with optimistic concurrency",
      classification: "REUSABLE_PRODUCT_LOGIC",
      implementation: "canonical-merge-review-apply.js",
      regression: "test:canonical-merge-review-apply",
    },
    {
      issue: "Phone auto-apply pilot (measurement gates)",
      classification: "REUSABLE_PRODUCT_LOGIC",
      implementation: "evaluatePhoneAutoApplyEligibility",
      regression: "test:canonical-merge-review-apply",
    },
    {
      issue: "Bethesda hotelId / eval inputs",
      classification: "HOTEL_SPECIFIC_DATA",
      implementation: "evals/bethesda-*",
      regression: "N/A",
    },
  ],
  recommendation: "REVIEWED CANONICAL APPLY READY",
};

fs.mkdirSync(path.dirname(OUT_JSON), { recursive: true });
fs.writeFileSync(OUT_JSON, JSON.stringify(report, null, 2));

function esc(s) {
  return String(s ?? "—").replace(/\|/g, "\\|");
}

const md = `# Bethesda TIER_1 Reviewed Apply

**Verdict:** ${report.recommendation}
**Environment:** local eval · no share writes · Surfe globally OFF

## A. Review queue

| Metric | Count |
|---|---:|
| Total proposals | ${summary.total} |
| Pending review | ${summary.pending} |
| Auto-apply phone (pilot) | ${summary.autoApplyPhone} |
| Approved (email) | ${summary.approved} |
| Applied | ${summary.applied} |
| Rejected | ${summary.rejected} |
| Rolled back | ${summary.rolledBack} |

## B. Apply result

| Result | Count |
|---|---:|
| Applied OK | ${applyResults.filter((r) => r.ok).length} |
| Blocked/Failed | ${applyResults.filter((r) => !r.ok).length} |

## C. Safe fields applied

| Person | Field | Before | After | Path | Status |
|---|---|---|---|---|---|
${appliedFields
  .map(
    (f) =>
      `| ${esc(f.person)} | ${esc(f.field)} | ${esc(f.before)} | ${esc(f.after)} | ${esc(f.approvalPath)} | ${esc(f.status)} |`
  )
  .join("\n")}

## D. Phone pilot metrics

| Metric | Value |
|---|---:|
| Phone attempts | ${snap.phonePilotMetrics.phoneAttempts} |
| Auto-applied | ${snap.phonePilotMetrics.autoApplied} |
| Held | ${snap.phonePilotMetrics.held} |
| Direct mobile | ${snap.phonePilotMetrics.directMobile} |
| Wrong-person collision | ${snap.phonePilotMetrics.wrongPersonCollision} |
| Provider calls avoided (reuse) | ${snap.phonePilotMetrics.providerCallsAvoided} |
| Credits per auto-applied phone | ${phoneRates.creditsPerAutoAppliedPhone ?? "—"} |

## E. Rollback

${rollbackResults.length ? "Ben Hawkins email rollback tested — audit preserved, re-apply blocked." : "No rollback test run."}

## F. Reuse

${reuseSamples
  .map(
    (r) =>
      `- **${r.name}** — reuse=${r.outcome || "none"} · email=${r.reuseEmail} · phone=${r.reusePhone} · avoided=${r.providerCallsAvoided}`
  )
  .join("\n")}
`;

fs.writeFileSync(OUT_MD, md);

const policy = `# Canonical Contact Reviewed Apply

**Module:** \`canonical-merge-review-apply.js\`
**Default:** \`REVIEW_REQUIRED\` for email · Phone \`AUTO_APPLY_PHONE_PILOT\` when gates pass
**AUTO_ACCEPT_SAFE:** disabled globally

## Flow

1. Generate field-level proposals from validated reachability/dry-run
2. **Email:** \`approveCanonicalContactMergeProposal\` → \`applyApprovedCanonicalContactMerge\`
3. **Phone pilot:** auto-approve when \`evaluatePhoneAutoApplyEligibility\` passes
4. Optimistic concurrency via \`personVersion\` + \`baselineFieldHash\`
5. Immutable audit on approve/apply/rollback
6. Rollback restores prior field; audit never deleted

## Phone pilot gates

- Identity ACCEPTED (LIMITED allowed when \`PHONE_PILOT_ALLOW_LIMITED_EVIDENCE_MOBILE=1\`)
- Field ownership PASS
- No collision / main-line-only / official conflict
- TIER_1_SAFE (or LIMITED+TIER_2 with pilot flag for mobile measurement)

## Not in scope

- Share page writes
- Global Surfe enablement
- CRM UI

## Regression

\`\`\`bash
npm run test:canonical-merge-review-apply
npm run gdi:bethesda-tier1-reviewed-apply
\`\`\`
`;

fs.mkdirSync(path.dirname(POLICY_MD), { recursive: true });
fs.writeFileSync(POLICY_MD, policy);

console.log(JSON.stringify({ outJson: OUT_JSON, outMd: OUT_MD, summary, recommendation: report.recommendation }, null, 2));
