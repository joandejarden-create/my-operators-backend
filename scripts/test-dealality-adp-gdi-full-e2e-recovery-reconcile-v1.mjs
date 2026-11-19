#!/usr/bin/env node
/**
 * Contract test: full ADP+GDI recovery reconciler discovers universes and classifies.
 */
import assert from "node:assert/strict";
import {
  DEALALITY_ADP_GDI_FULL_E2E_RECOVERY_RECONCILE_V1,
  discoverAdpUniverse,
  discoverGdiUniverse,
  listGdiPdfRebuildCandidates,
  buildFullE2eRecoveryPlan,
  renderFounderMarkdownTables,
} from "../lib/admin/dealality-adp-gdi-full-e2e-recovery-reconcile-v1.js";

assert.equal(
  DEALALITY_ADP_GDI_FULL_E2E_RECOVERY_RECONCILE_V1,
  "DEALALITY_ADP_GDI_FULL_E2E_RECOVERY_RECONCILE_V1"
);

const adp = discoverAdpUniverse();
assert.ok(adp.hotels.length >= 31, `expected >=31 ADP hotels, got ${adp.hotels.length}`);
assert.ok(adp.hotels.some((h) => h.propertyId === "adp_bethesda_marriott"));

const gdi = discoverGdiUniverse();
assert.ok(gdi.hotels.length >= 22, `expected >=22 GDI hotels, got ${gdi.hotels.length}`);
assert.ok(gdi.hotels.some((h) => h.hotelId === "recLuxvwwxID7U2B8"));

const plan = await buildFullE2eRecoveryPlan({ dryRun: true });
assert.equal(plan.mintShares, false);
assert.equal(plan.paidResearch, false);
assert.ok(plan.adp.rows.length === adp.hotels.length);
assert.ok(plan.gdi.rows.length === gdi.hotels.length);

const tables = renderFounderMarkdownTables(plan);
assert.match(tables.adpMarkdown, /\| Hotel \| Evidence \|/);
assert.match(tables.gdiMarkdown, /\| Hotel \| Eligible ops \|/);

const candidates = listGdiPdfRebuildCandidates(plan);
assert.ok(Array.isArray(candidates));
assert.ok(
  candidates.every((c) => Number(c.eligibleOps || 0) > 0),
  "GDI PDF rebuild candidates must have eligible commercial opportunities"
);
assert.ok(
  !candidates.some((c) =>
    String(c.result || "").includes("WATCH_ONLY") ||
    String(c.result || "") === "AWAITING_REPORT_POLICY"
  ),
  "WATCH_ONLY / AWAITING_REPORT_POLICY must not be auto PDF rebuild candidates"
);

const watchClassified = plan.gdi.rows.filter((r) =>
  [
    "WATCH_ONLY",
    "WATCH_ONLY_PDF_NOT_APPLICABLE",
    "AWAITING_REPORT_POLICY",
  ].includes(r.result)
);
assert.ok(
  plan.gdi.awaitingReportPolicy >= 0 && plan.gdi.watchOnly >= 0,
  "plan must expose watch-only policy counters"
);

console.log(
  JSON.stringify(
    {
      ok: true,
      adpDistinct: adp.hotels.length,
      gdiDistinct: gdi.hotels.length,
      gdiPdfRebuildCandidates: candidates.length,
      adpComplete: plan.adp.complete,
      gdiComplete: plan.gdi.complete,
      gdiWatchClassified: watchClassified.length,
      gdiAwaitingReportPolicy: plan.gdi.awaitingReportPolicy,
      adpAdminInventory: plan.adp.adminInventoryCount,
    },
    null,
    2
  )
);
