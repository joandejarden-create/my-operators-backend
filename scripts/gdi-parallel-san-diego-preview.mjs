#!/usr/bin/env node
/**
 * San Diego Parallel integration — dry-run publication preview.
 * Reads authoritative baseline (no writes). Replays fixtures. No paid calls.
 *
 *   node scripts/gdi-parallel-san-diego-preview.mjs
 *   node scripts/gdi-parallel-san-diego-preview.mjs --out-dir reports/gdi-parallel-integration
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { reconcileSanDiegoReviewSet } from "../lib/group-demand-intelligence/parallel/san-diego-reconcile-v1.js";
import {
  runParallelDiscoveryStage,
  writeDiscoveryStageArtifacts,
  DISCOVERY_MODE,
} from "../lib/group-demand-intelligence/workflow/parallel-discovery-adapter-v1.js";
import { buildTargetedContactPlan } from "../lib/group-demand-intelligence/workflow/targeted-contact-plan-v1.js";
import { getGdiParallelFlagState } from "../lib/group-demand-intelligence/parallel/feature-flag.js";
import { PARALLEL_PRICE_ESTIMATES_USD } from "../lib/group-demand-intelligence/parallel/budget-ledger-v1.js";
import { GDI_PARALLEL_SAN_DIEGO_HOTEL_ID } from "../lib/group-demand-intelligence/parallel/feature-flag.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function arg(name, fallback = null) {
  const i = process.argv.indexOf(name);
  if (i < 0) return fallback;
  return process.argv[i + 1] || fallback;
}

const outDir = path.resolve(
  arg("--out-dir", path.join(ROOT, "reports/gdi-parallel-integration"))
);
fs.mkdirSync(outDir, { recursive: true });

const hotelId = GDI_PARALLEL_SAN_DIEGO_HOTEL_ID;

const flags = getGdiParallelFlagState(process.env);
const discovery = await runParallelDiscoveryStage(hotelId, {
  runDiscovery: true,
  mode: DISCOVERY_MODE.REPLAY,
});
writeDiscoveryStageArtifacts(discovery, path.join(outDir, "discovery-replay"));

const reconcile = await reconcileSanDiegoReviewSet({
  hotelId,
  env: { ...process.env, GDI_PARALLEL_ENABLED: "1" },
});
fs.writeFileSync(
  path.join(outDir, "san-diego-reconciliation.json"),
  JSON.stringify(reconcile, null, 2) + "\n"
);

const contactPlan = buildTargetedContactPlan({
  hotelId,
  opportunities: (discovery.candidates || []).map((c) => ({
    ...c,
    id: c.candidateId,
    primaryContact: c.primaryContact,
  })),
  authorizeSurfe: false,
  env: process.env,
});
fs.writeFileSync(
  path.join(outDir, "targeted-contact-plan.json"),
  JSON.stringify(contactPlan, null, 2) + "\n"
);

const recoveryJobs = (reconcile.localRecovery || []).map((r) => ({
  orgKey: r.orgKey,
  plan: r.plan,
  surfeEligible: r.surfeEligible,
  estimatedSurfeEmailCredits: r.estimatedSurfeEmailCredits,
  estimatedParallelUsdCap: 0,
  note: "Local-first recovery — Surfe queued only when eligible; no paid execution in this preview",
}));

const preview = {
  kind: "gdi_parallel_san_diego_publication_preview",
  label: "INTERNAL REVIEW — dry-run only",
  asOf: new Date().toISOString(),
  hotelId,
  displayName: "Wyndham San Diego Bayside",
  flags,
  pricing: PARALLEL_PRICE_ESTIMATES_USD,
  discoveryReplay: {
    status: discovery.status,
    paidCalls: discovery.paidCalls,
    freshDiscoveryClaimed: discovery.freshDiscoveryClaimed,
    candidateCount: (discovery.candidates || []).length,
    fixtureSource: discovery.fixtureSource,
    baselineNoveltyLab: discovery.reviewPackage?.baselineNovelty,
  },
  reconciliationSummary: reconcile.reconciliation,
  baseline: reconcile.baseline,
  proposedPublicationCounts: {
    customerVisibleProposed: reconcile.customerVisibleProposed.length,
    internalOnlyResearch: reconcile.internalOnly.length,
    existingMatches: reconcile.existingMatches.length,
    proposedNew: reconcile.proposedNew.length,
    note: "Zero customer-visible cards in this dry-run — explicit approval required to publish",
  },
  contactBuckets: reconcile.reconciliation.contactBuckets,
  unsupportedQuarantined: reconcile.unsupportedQuarantined,
  sharedHousingPathways: reconcile.sharedHousingPathways,
  localRecoveryJobs: recoveryJobs,
  surfeFallbackQueue: reconcile.surfeFallbackQueue,
  confirmedOpenDemand: 0,
  gdiClassifierNote: reconcile.gdiClassifierNote,
  activationBlockedUntil: [
    "Explicit deployment approval",
    "GDI_PARALLEL_ENABLED=1 on target env",
    "Hotel allowlist includes San Diego",
    "Baseline novelty reconciled if previously UNKNOWN",
    "Explicit --authorize-publish for customer cards",
  ],
};

fs.writeFileSync(
  path.join(outDir, "publication-preview.json"),
  JSON.stringify(preview, null, 2) + "\n"
);

const md = `# San Diego Parallel publication preview (dry-run)

**Hotel:** Wyndham San Diego Bayside (\`${hotelId}\`)  
**As-of:** ${preview.asOf}  
**Paid Parallel/Surfe calls this run:** 0

## Counts

| Metric | Value |
| --- | ---: |
| Fixture / replay candidates | ${preview.discoveryReplay.candidateCount} |
| After account+trip dedupe | ${reconcile.reconciliation.afterAccountTripDedupe} |
| Existing baseline matches | ${reconcile.reconciliation.existingMatches} |
| Proposed new (internal) | ${reconcile.reconciliation.proposedNew} |
| Customer-visible proposed | **${reconcile.reconciliation.customerVisibleProposed}** |
| Confirmed open demand | **0** |
| Email or phone usable | ${reconcile.reconciliation.contactBuckets.email_or_phone} |
| Routing-only | ${reconcile.reconciliation.contactBuckets.routing_only} |
| Neither / quarantined | ${reconcile.reconciliation.contactBuckets.neither_or_quarantined} |

## Baseline

- Readable: **${reconcile.baseline.readable}**
- Opportunity rows: ${reconcile.baseline.opportunityCount}
- Novelty policy: ${reconcile.baseline.noveltyPolicy}
- Auto-publication blocked: ${reconcile.baseline.autoPublicationBlocked}

## Local recovery (Sol-Ark / SPARGO / Esri)

${recoveryJobs.map((j) => `- **${j.orgKey}**: ${j.plan} (Surfe eligible=${j.surfeEligible}, est. email credits=${j.estimatedSurfeEmailCredits})`).join("\n")}

## Flags (this process)

\`\`\`json
${JSON.stringify(flags, null, 2)}
\`\`\`

## Next (explicit approval)

1. Review \`san-diego-reconciliation.json\` and card-level gaps.
2. Deploy code with \`GDI_PARALLEL_ENABLED\` still **OFF**.
3. Activate San Diego only after approval; do not publish customer cards without \`--authorize-publish\`.

Artifacts: \`publication-preview.json\`, \`san-diego-reconciliation.json\`, \`discovery-replay/\`, \`targeted-contact-plan.json\`.
`;

fs.writeFileSync(path.join(outDir, "PUBLICATION-PREVIEW.md"), md);
console.log(JSON.stringify({ ok: true, outDir, summary: preview.proposedPublicationCounts, contactBuckets: preview.contactBuckets, recoveryJobs }, null, 2));
