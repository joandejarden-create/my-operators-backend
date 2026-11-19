#!/usr/bin/env node
/**
 * Full ADP + GDI E2E recovery reconciler.
 *
 * Default: dry-run inventory + classification + founder tables.
 * Optional: --apply-gdi-pdf-rebuilds rebuilds MISSING GDI PDFs from existing
 *           eligible opportunities only (never mints shares / paid research).
 *
 * STOP before merge / new customer publication — this CLI does not publish.
 */
import fs from "node:fs";
import path from "node:path";
import {
  buildFullE2eRecoveryPlan,
  listGdiPdfRebuildCandidates,
  probeActiveAdpShareHealth,
  renderFounderMarkdownTables,
  DEALALITY_ADP_GDI_FULL_E2E_RECOVERY_RECONCILE_V1,
} from "../lib/admin/dealality-adp-gdi-full-e2e-recovery-reconcile-v1.js";

function parseArgs(argv) {
  const out = {
    dryRun: true,
    applyGdiPdfRebuilds: false,
    shareHealth: false,
    outDir: path.join(
      process.cwd(),
      "reports/adp-gdi-full-e2e-recovery-v1"
    ),
  };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--apply-gdi-pdf-rebuilds") {
      out.applyGdiPdfRebuilds = true;
      out.dryRun = false;
    } else if (a === "--share-health") out.shareHealth = true;
    else if (a === "--dry-run") out.dryRun = true;
    else if (a === "--out" && argv[i + 1]) out.outDir = path.resolve(argv[++i]);
  }
  return out;
}

async function maybeRebuildGdiPdfs(plan, { apply }) {
  const candidates = listGdiPdfRebuildCandidates(plan);
  const results = [];
  if (!candidates.length) {
    return { attempted: 0, rebuilt: 0, failed: 0, results };
  }
  if (!apply) {
    return {
      attempted: 0,
      rebuilt: 0,
      failed: 0,
      planned: candidates.map((c) => ({
        hotelId: c.hotelId,
        hotel: c.hotel,
        eligibleOps: c.eligibleOps,
      })),
      results,
    };
  }
  const { generateGdiReportPdfV1 } = await import(
    "../lib/group-demand-intelligence/reports/generate-gdi-report-pdf-v1.mjs"
  );
  for (const c of candidates) {
    process.stdout.write(`[gdi-pdf-rebuild] ${c.hotelId}… `);
    try {
      const result = await generateGdiReportPdfV1({
        hotelId: c.hotelId,
        persist: true,
      });
      if (!result?.ok) {
        console.log(`FAIL ${result?.reason || "unavailable"}`);
        results.push({
          hotelId: c.hotelId,
          ok: false,
          reason: result?.reason || "unavailable",
        });
        continue;
      }
      console.log(`OK ${result.byteLength || 0}b`);
      results.push({
        hotelId: c.hotelId,
        ok: true,
        byteLength: result.byteLength || null,
        hotelName: result.hotelName || c.hotel,
      });
    } catch (err) {
      console.log(`ERROR ${err?.message || err}`);
      results.push({
        hotelId: c.hotelId,
        ok: false,
        reason: err?.message || String(err),
      });
    }
  }
  return {
    attempted: results.length,
    rebuilt: results.filter((r) => r.ok).length,
    failed: results.filter((r) => !r.ok).length,
    results,
  };
}

async function main() {
  const args = parseArgs(process.argv);
  console.log(
    `[${DEALALITY_ADP_GDI_FULL_E2E_RECOVERY_RECONCILE_V1}] dryRun=${args.dryRun} applyGdiPdf=${args.applyGdiPdfRebuilds}`
  );

  const plan = await buildFullE2eRecoveryPlan({ dryRun: args.dryRun });
  const tables = renderFounderMarkdownTables(plan);
  const rebuild = await maybeRebuildGdiPdfs(plan, {
    apply: args.applyGdiPdfRebuilds,
  });

  // Re-classify GDI after rebuilds so tables reflect post-repair state.
  let finalPlan = plan;
  if (args.applyGdiPdfRebuilds && rebuild.rebuilt > 0) {
    finalPlan = await buildFullE2eRecoveryPlan({ dryRun: false });
  }
  const finalTables = renderFounderMarkdownTables(finalPlan);

  fs.mkdirSync(args.outDir, { recursive: true });
  const jsonPath = path.join(args.outDir, "RECOVERY_PLAN.json");
  const mdPath = path.join(args.outDir, "FOUNDER_RECONCILIATION_TABLES.md");
  const summaryPath = path.join(args.outDir, "SUMMARY.json");
  const shareHealthPath = path.join(args.outDir, "ADP_ACTIVE_SHARE_HEALTH.json");

  let shareHealth = null;
  if (args.shareHealth) {
    console.log("[share-health] probing ACTIVE ADP sealed shares on production…");
    shareHealth = await probeActiveAdpShareHealth({});
    fs.writeFileSync(shareHealthPath, JSON.stringify(shareHealth, null, 2), "utf8");
    console.log(
      `[share-health] ok=${shareHealth.okCount}/${shareHealth.probed} fail401=${shareHealth.fail401} fail403=${shareHealth.fail403}`
    );
  }

  const summary = {
    gate: DEALALITY_ADP_GDI_FULL_E2E_RECOVERY_RECONCILE_V1,
    generatedAt: finalPlan.generatedAt,
    dryRun: args.dryRun,
    applyGdiPdfRebuilds: args.applyGdiPdfRebuilds,
    shareHealthProbed: Boolean(shareHealth),
    adpDistinctHotels: finalPlan.adp.universe.distinct,
    adpComplete: finalPlan.adp.complete,
    adpCoverageCount: finalPlan.adp.coveragePropertyCount,
    adpAdminInventory: finalPlan.adp.adminInventoryCount,
    adpAuthorizedLinks: finalPlan.adp.authorizedExternalLinks,
    adpRequiresFounderApproval: finalPlan.adp.requiresFounderApproval,
    adpEvidenceMissing: finalPlan.adp.evidenceMissing,
    adpPdfRebuildable: finalPlan.adp.pdfRebuildable,
    adpReviewRebuildable: finalPlan.adp.reviewRebuildable,
    gdiDistinctHotels: finalPlan.gdi.universe.distinct,
    gdiSelectable: finalPlan.gdi.universe.selectable,
    gdiComplete: finalPlan.gdi.complete,
    gdiPdfReady: finalPlan.gdi.pdfReady,
    gdiPdfMissing: finalPlan.gdi.pdfMissing,
    gdiAuthorizedLinks: finalPlan.gdi.authorizedExternalLinks,
    gdiRequiresFounderApproval: finalPlan.gdi.requiresFounderApproval,
    gdiEvidenceMissing: finalPlan.gdi.evidenceMissing,
    gdiPdfRebuildable: finalPlan.gdi.pdfRebuildable,
    gdiWatchOnly: finalPlan.gdi.watchOnly,
    gdiAwaitingReportPolicy: finalPlan.gdi.awaitingReportPolicy,
    gdiWatchOnlyPdfNotApplicable: finalPlan.gdi.watchOnlyPdfNotApplicable,
    gdiPdfRebuild: rebuild,
    shareHealth: shareHealth
      ? {
          probed: shareHealth.probed,
          okCount: shareHealth.okCount,
          fail401: shareHealth.fail401,
          fail403: shareHealth.fail403,
          failingPropertyIds: (shareHealth.failing || []).map((r) => r.propertyId),
        }
      : null,
    mintShares: false,
    paidResearch: false,
    stopBeforeMerge: true,
  };

  fs.writeFileSync(
    jsonPath,
    JSON.stringify({ ...finalPlan, gdiPdfRebuild: rebuild }, null, 2),
    "utf8"
  );
  fs.writeFileSync(
    summaryPath,
    JSON.stringify(summary, null, 2),
    "utf8"
  );
  fs.writeFileSync(
    mdPath,
    [
      "# Dealality — ADP + GDI Full E2E Recovery Reconciliation",
      "",
      `Generated: ${finalPlan.generatedAt}`,
      "",
      "## ADP",
      "",
      finalTables.adpMarkdown,
      "",
      "## GDI",
      "",
      finalTables.gdiMarkdown,
      "",
      "## Inventory deltas",
      "",
      ...finalPlan.inventoryDeltas.explanation.map((e) => `- ${e}`),
      "",
      `ADP distinct vs prior Reviews baseline (39): **${finalPlan.inventoryDeltas.adpDistinctVsPriorReviewsBaseline39}**`,
      "",
      `GDI distinct vs prior catalog baseline (22): **${finalPlan.inventoryDeltas.gdiDistinctVsPriorCatalogBaseline22}**`,
      "",
      "## Controls",
      "",
      "- No paid research",
      "- No new customer share minting",
      "- STOP before merge / new customer publication",
      "",
    ].join("\n"),
    "utf8"
  );

  console.log(JSON.stringify(summary, null, 2));
  console.log(`wrote ${jsonPath}`);
  console.log(`wrote ${mdPath}`);
  console.log(`wrote ${summaryPath}`);
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
