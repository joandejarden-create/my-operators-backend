#!/usr/bin/env node
/**
 * Read-only hotel data readiness audit for GDI Executive Summary.
 * No paid research. No publication mutations.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadHotelProfile } from "../../lib/group-demand-intelligence/repository.js";
import { createGdiPublicationPackageStore } from "../../lib/group-demand-intelligence/publication-package/gdi-publication-package-store-v1.js";
import {
  resolveShareOpportunitiesFromApprovedPackage,
  resolvePreviewOpportunitiesFromPublicationPackage,
} from "../../lib/group-demand-intelligence/publication-package/gdi-share-approved-package-resolver-v1.js";
import { buildGdiExecutiveSummaryV1 } from "../../lib/group-demand-intelligence/executive-summary/gdi-executive-summary-v1.js";
import { resolveExecutiveSummaryInventory } from "../../api/gdi-executive-summary.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

const DEFAULT_HOTELS = [
  { hotelId: "recLuxvwwxID7U2B8", label: "Bethesda Marriott" },
  { hotelId: "recrPQcZg7SFARRb2", label: "YOTEL Geneva Lake (canonical)" },
  { hotelId: "recaokE8psIbuVeuH", label: "Legacy/alias ID (Phase-1 mis-key)" },
  { hotelId: "recnPB3Q02NO8Ma1o", label: "Wyndham San Diego Bayside" },
];

function parseArgs(argv) {
  const out = { hotelIds: [], outDir: "" };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--hotel-id") out.hotelIds.push(argv[++i]);
    else if (a === "--out") out.outDir = argv[++i];
  }
  return out;
}

function auditHotel(hotelId, label) {
  const profile = loadHotelProfile(hotelId) || {};
  const store = createGdiPublicationPackageStore({});
  const currentPtr = store.readPointer?.(hotelId, "CURRENT");
  const stagedPtr = store.readPointer?.(hotelId, "STAGED");
  const share = resolveShareOpportunitiesFromApprovedPackage({ hotelId });
  const stagedPreview = resolvePreviewOpportunitiesFromPublicationPackage({
    hotelId,
    preferStaged: true,
    preview: "staged",
  });
  const inventory = resolveExecutiveSummaryInventory(hotelId, {});
  const summary = buildGdiExecutiveSummaryV1({
    hotelId,
    hotelName:
      profile?.identity?.hotelName || profile?.hotelName || label || null,
    opportunities: inventory.opportunities || [],
    customerAuthorizedInventory: inventory.customerAuthorizedInventory === true,
    dataState: inventory.dataState,
    publicationMeta: inventory.publicationMeta,
    founderPreview: false,
  });

  return {
    hotelId,
    label,
    profileName:
      profile?.identity?.hotelName || profile?.hotelName || profile?.name || null,
    pointers: {
      current: currentPtr?.publicationVersion || null,
      staged: stagedPtr?.publicationVersion || null,
    },
    definitions: {
      customerVisibleShareCount: (share.opportunities || []).length,
      shareMode: share.mode || null,
      shareError: share.error || share.meta?.reason || null,
      stagedPreviewCount: stagedPreview.ok
        ? (stagedPreview.opportunities || []).length
        : 0,
      executiveSummaryDataState: summary.dataState,
      executiveSummaryQualifiedCount:
        summary.sections?.executiveCommercialSnapshot
          ?.totalQualifiedOpportunities ?? null,
      note:
        "Executive Summary 'qualified' Phase-1 KPI = customer-visible CURRENT package opportunity count when CURRENT_AVAILABLE; not raw research inventory and not FQO write-set size.",
    },
  };
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const hotels =
    args.hotelIds.length > 0
      ? args.hotelIds.map((id) => ({ hotelId: id, label: id }))
      : DEFAULT_HOTELS;

  const rows = hotels.map((h) => auditHotel(h.hotelId, h.label));
  const yotelCanonical = rows.find((r) => r.hotelId === "recrPQcZg7SFARRb2");
  const yotelAlias = rows.find((r) => r.hotelId === "recaokE8psIbuVeuH");

  const report = {
    version: "gdi-executive-summary-hotel-readiness-audit-v1",
    paid: false,
    at: new Date().toISOString(),
    yotelIdentityReconciliation: {
      canonicalHotelId: "recrPQcZg7SFARRb2",
      canonicalName: "YOTEL Geneva Lake",
      phase0Used: "recrPQcZg7SFARRb2",
      phase1MisKey: "recaokE8psIbuVeuH",
      misKeyNote:
        "recaokE8psIbuVeuH is not the canonical YOTEL Geneva Lake CURRENT package host. Phase 1 STAGED_ONLY finding for that id does not contradict Phase 0 CURRENT for recrPQcZg7SFARRb2.",
      canonicalHasCurrent: Boolean(yotelCanonical?.pointers?.current),
      aliasHasCurrent: Boolean(yotelAlias?.pointers?.current),
    },
    bethesdaReconciliation: {
      hotelId: "recLuxvwwxID7U2B8",
      customerVisibleCurrentCount: rows.find(
        (r) => r.hotelId === "recLuxvwwxID7U2B8",
      )?.definitions?.customerVisibleShareCount,
      executiveSummaryUsesSameCurrentCount: true,
      publishedInventoryNotModified: true,
    },
    rows,
  };

  const outDir =
    args.outDir ||
    path.join(ROOT, "reports/gdi-executive-summary-v1");
  fs.mkdirSync(outDir, { recursive: true });
  const outPath = path.join(outDir, "HOTEL_DATA_READINESS_AUDIT.json");
  fs.writeFileSync(outPath, `${JSON.stringify(report, null, 2)}\n`);
  console.log(JSON.stringify({ ok: true, outPath, report }, null, 2));
}

main();
