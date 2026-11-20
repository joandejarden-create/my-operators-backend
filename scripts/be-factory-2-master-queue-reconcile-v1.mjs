#!/usr/bin/env node
/**
 * Reconcile Factory 2.0 master queue across all authorized batches + staged work.
 * Usage: npm run be-factory-2-master-queue-reconcile
 */
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { writeFileSync } from "fs";
import { reconcileMasterQueue } from "../lib/brand-explorer/factory-2/be-factory-2-master-queue-reconcile-v1.js";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");

function main() {
  const { queue, dashboard, audit } = reconcileMasterQueue({ repoRoot: ROOT });
  const md = [
    `# Factory 2.0 — Consolidated Portfolio Dashboard`,
    ``,
    `**Contract:** \`BE_FACTORY_2_0_MASTER_QUEUE_RECONCILE_V1\``,
    `**Generated:** ${audit.generatedAt}`,
    ``,
    `## Reconciliation`,
    ``,
    `| Metric | Count |`,
    `|---|---:|`,
    `| Authorized batches | ${queue.reconciliation.authorizedBatches} |`,
    `| Authorized brand slots | ${queue.reconciliation.authorizedBrandSlots} |`,
    `| Prior Active/Live queue | ${queue.reconciliation.priorQueueCount} |`,
    `| Staged brand folders | ${queue.reconciliation.stagedFolderCount} |`,
    `| **Master queue total** | **${queue.reconciliation.masterQueueCount}** |`,
    ``,
    `## Portfolio status`,
    ``,
    `| State | Count |`,
    `|---|---:|`,
    `| Total brands in master queue | ${dashboard.summary.totalBrandsInMasterQueue} |`,
    `| Existing brands still QUEUED | ${dashboard.summary.existingBrandsQueued} |`,
    `| EXISTING_CANONICAL in queue | ${dashboard.summary.existingCanonicalInQueue} |`,
    `| New brands proposed | ${dashboard.summary.newBrandsProposed} |`,
    `| Researching | ${dashboard.summary.brandsResearching} |`,
    `| Certified (ready for review) | ${dashboard.summary.brandsCertified} |`,
    `| Awaiting approval | ${dashboard.summary.brandsAwaitingApproval} |`,
    `| Needs remediation | ${dashboard.summary.brandsNeedsRemediation} |`,
    `| Persisted verified | ${dashboard.summary.brandsPersisted} |`,
    `| Published | ${dashboard.summary.brandsPublished} |`,
    `| Blocked | ${dashboard.summary.brandsBlocked} |`,
    `| Protected (skip) | ${dashboard.summary.skippedProtected} |`,
    `| Research spend USD | ${dashboard.summary.researchSpendUsdTotal} |`,
    `| Expected publication writes | ${dashboard.summary.expectedPublicationWritesTotal} |`,
    ``,
    `## Authorized batches accounted for`,
    ``,
    ...(queue.authorizedBatches || []).map(
      (b) =>
        `- **${b.batchId}** (${b.researchClass}) — ${b.brandCount} brands — ${b.label}`
    ),
    ``,
    `## Next 10 scheduled`,
    ``,
    `| # | Brand | Status | Batch |`,
    `|---:|---|---|---|`,
    ...(dashboard.nextTenScheduled || []).map(
      (r) =>
        `| ${r.priority} | ${r.brandName} (\`${r.brandId}\`) | ${r.factoryStatus} | ${r.batchId || "—"} |`
    ),
    ``,
    `## Gates preserved`,
    ``,
    `- Exact-candidate approval required for writeback authorization`,
    `- No production Presentation overwrite without founder exact-hash approval`,
    `- No new Brand Basics creates without identity + creation approval`,
    `- Paid research budget remains $0 unless explicitly raised`,
    `- Curio / Ascend remain SKIPPED_PROTECTED`,
    ``,
  ].join("\n");

  writeFileSync(
    join(ROOT, "reports/brand-explorer/factory-2-0/70-master-queue-reconcile.md"),
    md
  );
  writeFileSync(
    join(ROOT, "reports/brand-explorer/factory-2-0/05-founder-batch-dashboard.md"),
    md
  );

  console.log(JSON.stringify({
    ok: true,
    masterQueueCount: queue.brands.length,
    summary: dashboard.summary,
    nextTen: audit.nextTen,
    report: "reports/brand-explorer/factory-2-0/70-master-queue-reconcile.md",
  }, null, 2));
}

main();
