#!/usr/bin/env node
/**
 * Audit Bethesda local commercial list vs customer-facing publication filter.
 * Read-only. Zero paid.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { applyApprovedBethesdaCustomerProjection } from "../lib/group-demand-intelligence/workflow/gdi-bethesda-publication-v1.js";
import {
  filterOpportunitiesByCommercialSurface,
  GDI_LIST_SURFACE_VIEW,
} from "../lib/group-demand-intelligence/gdi-commercial-surface-disposition-v1.js";
import { mergeBethesdaInternalResearchOpportunities } from "../lib/group-demand-intelligence/research/bethesda-internal-research-union-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const hotelId = "recLuxvwwxID7U2B8";
const doc = await loadOpportunitiesCanonical(hotelId);
const union = mergeBethesdaInternalResearchOpportunities(
  hotelId,
  doc.opportunities || []
);
const commercial = filterOpportunitiesByCommercialSurface(
  union.opportunities,
  GDI_LIST_SURFACE_VIEW.COMMERCIAL
);
const facing = applyApprovedBethesdaCustomerProjection(
  filterCustomerFacingOpportunities(commercial, { hotelId })
);

function summarize(rows) {
  const byPriority = {};
  for (const o of rows) {
    const p = String(o.priority || "UNKNOWN").toUpperCase();
    byPriority[p] = (byPriority[p] || 0) + 1;
  }
  return {
    count: rows.length,
    byPriority,
    customerVisibleFalse: rows.filter((o) => o.customerVisible === false).length,
    watchlistIds: rows
      .filter((o) => String(o.priority).toUpperCase() === "WATCHLIST")
      .map((o) => o.id),
  };
}

const out = {
  hotelId,
  storeCount: (doc.opportunities || []).length,
  unionCount: union.opportunities.length,
  localAuthCommercialLike: summarize(commercial),
  customerFacingLikeShare: summarize(facing),
  facingWatchlistDetail: facing
    .filter((o) => String(o.priority).toUpperCase() === "WATCHLIST")
    .map((o) => ({
      id: o.id,
      title: o.title,
      priority: o.priority,
      commercialMaturity: o.commercialMaturity || null,
      gdiMaturityState: o.gdiMaturityState || null,
      commercialInventoryClass: o.commercialInventoryClass || null,
      prospectPublicationBasis: o.prospectPublicationBasis || null,
      outreachReadiness: o.outreachReadiness || null,
      gdiPublishedInventoryRegistry: o.gdiPublishedInventoryRegistry === true,
      customerVisible: o.customerVisible,
    })),
  facingIds: facing.map((o) => o.id).sort(),
};

const outDir = path.join(
  ROOT,
  "reports/gdi-bethesda-local-published-parity-zero-paid-v1"
);
fs.mkdirSync(outDir, { recursive: true });
fs.writeFileSync(
  path.join(outDir, "01-parity-audit.json"),
  JSON.stringify(out, null, 2) + "\n"
);
console.log(JSON.stringify(out, null, 2));
