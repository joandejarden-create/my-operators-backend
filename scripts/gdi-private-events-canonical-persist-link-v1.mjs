#!/usr/bin/env node
/**
 * Canonical PE persist + GDI link for San Diego / Mayflower (+ Woman's Club contact fix).
 *
 *   node scripts/gdi-private-events-canonical-persist-link-v1.mjs
 *   node scripts/gdi-private-events-canonical-persist-link-v1.mjs --apply
 *   node scripts/gdi-private-events-canonical-persist-link-v1.mjs --apply --hotels=san_diego,mayflower
 *   node scripts/gdi-private-events-canonical-persist-link-v1.mjs --apply --packages=path/to/packages.json
 */

import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  loadVenuePartnershipPackagesFromFile,
  persistVenuePartnershipPackages,
} from "../lib/group-demand-intelligence/private-events/canonical-persist-and-link-v1.js";
import { enrichPrivateEventOpportunityDetail } from "../lib/group-demand-intelligence/private-events/customer-detail-enrichment.js";
import {
  findOpportunityRecordById,
  airtableRecordToOpportunity,
} from "../lib/group-demand-intelligence/airtable-opportunity-store.js";
import { invalidateGdiHotelReadCache } from "../lib/group-demand-intelligence/read-cache.js";
import { isPeAirtableConfigured } from "../lib/group-demand-intelligence/private-events/airtable-client.js";
import { listVenuesFromAirtable } from "../lib/group-demand-intelligence/private-events/airtable-venue-store.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");
const RERUN = process.argv.includes("--rerun");
const OUT = path.join(
  ROOT,
  "reports/gdi-operational-baseline/pe-canonical-persist"
);
const DEFAULT_PACKAGE_PATH = path.join(OUT, "venue-partnership-packages.json");

function resolvePackagePath() {
  const arg = process.argv.find((a) => a.startsWith("--packages="));
  if (!arg) return DEFAULT_PACKAGE_PATH;
  const raw = arg.slice("--packages=".length).trim();
  return path.isAbsolute(raw) ? raw : path.resolve(ROOT, raw);
}

const PACKAGE_PATH = resolvePackagePath();

const HOTEL_ALIASES = {
  san_diego: "recnPB3Q02NO8Ma1o",
  sd: "recnPB3Q02NO8Ma1o",
  mayflower: "rec0eTEnCqlsGJJt9",
  may: "rec0eTEnCqlsGJJt9",
  bethesda: "recLuxvwwxID7U2B8",
  beth: "recLuxvwwxID7U2B8",
};

function parseHotelFilter() {
  const arg = process.argv.find((a) => a.startsWith("--hotels="));
  if (!arg) return null;
  return new Set(
    arg
      .slice("--hotels=".length)
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean)
      .map((s) => HOTEL_ALIASES[s.toLowerCase()] || s)
  );
}

async function verifyDetail(opportunityId, hotelId) {
  invalidateGdiHotelReadCache(hotelId);
  const rec = await findOpportunityRecordById(opportunityId);
  if (!rec) return { ok: false, reason: "missing_opportunity" };
  const raw = airtableRecordToOpportunity(rec);
  const { opportunity: enriched, peEnriched, venueLoaded, fitLoaded } =
    await enrichPrivateEventOpportunityDetail(raw);
  return {
    ok:
      peEnriched &&
      enriched.detailLayout === "VENUE_PARTNERSHIP" &&
      Boolean(enriched.peVenueId) &&
      Boolean(enriched.hotelVenueFitId),
    peEnriched,
    venueLoaded,
    fitLoaded,
    detailLayout: enriched.detailLayout,
    peVenueId: enriched.peVenueId,
    hotelVenueFitId: enriched.hotelVenueFitId,
    contactGrade: enriched.contactGrade,
    contactGradeLabel: enriched.contactGradeLabel,
    commercialContactPath: enriched.commercialContactPath,
    commercialContactPathLabel: enriched.commercialContactPathLabel,
    partnerStatusLabel: enriched.partnerStatusLabel,
    contradiction:
      /organization contact path/i.test(
        String(enriched.commercialContactPathLabel || "")
      ) && /Grade E/i.test(String(enriched.contactGradeLabel || "")),
  };
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  if (!isPeAirtableConfigured()) throw new Error("pe_airtable_not_configured");

  const hotelFilter = parseHotelFilter();
  let packages = loadVenuePartnershipPackagesFromFile(PACKAGE_PATH);
  if (hotelFilter) {
    packages = packages.filter((p) =>
      hotelFilter.has(p.hotel?.hotelId || p.hotel?.id)
    );
  }

  console.log(
    JSON.stringify(
      {
        apply: APPLY,
        packagePath: PACKAGE_PATH,
        packageCount: packages.length,
        hotels: [...new Set(packages.map((p) => p.hotel?.hotelId))],
      },
      null,
      2
    )
  );

  const summary = await persistVenuePartnershipPackages(packages, {
    dryRun: !APPLY,
    outDir: OUT,
  });

  if (APPLY) {
    const verification = [];
    for (const r of summary.results) {
      if (!r.opportunityId) continue;
      verification.push({
        hotelId: r.hotelId,
        opportunityId: r.opportunityId,
        venueId: r.venueId,
        ...(await verifyDetail(r.opportunityId, r.hotelId)),
      });
    }
    summary.verification = verification;

    if (RERUN || APPLY) {
      const rerun = await persistVenuePartnershipPackages(packages, {
        dryRun: false,
        outDir: null,
      });
      summary.rerun = {
        results: rerun.results.map((r) => ({
          venueName: r.venueName,
          venueAction: r.steps?.venue?.action,
          fitAction: r.steps?.fit?.action,
          oppMode: r.steps?.opportunityMatch?.mode,
          oppWrite: r.steps?.opportunityWrite,
          signals: r.steps?.signals,
        })),
      };
    }

    const byHotel = {};
    for (const r of summary.results) {
      const id = r.hotelId;
      if (!byHotel[id]) {
        byHotel[id] = {
          hotelId: id,
          venues: [],
          signals: [],
          fits: [],
          opportunities: [],
          published: 0,
          internal: 0,
        };
      }
      byHotel[id].venues.push({
        venueId: r.venueId,
        name: r.venueName,
        action: r.steps?.venue?.action,
        recordId: r.steps?.venue?.recordId,
      });
      byHotel[id].fits.push({
        fitId: r.steps?.fit?.fitId,
        action: r.steps?.fit?.action,
        recordId: r.steps?.fit?.recordId,
      });
      for (const s of r.steps?.signals || []) byHotel[id].signals.push(s);
      byHotel[id].opportunities.push({
        opportunityId: r.opportunityId,
        mode: r.steps?.opportunityMatch?.mode,
        published: r.published,
      });
      if (r.published) byHotel[id].published += 1;
      else byHotel[id].internal += 1;
    }
    summary.byHotel = byHotel;

    const venuesAfter = await listVenuesFromAirtable({ maxRecords: 200 });
    summary.venueInventoryAfter = {
      total: venuesAfter.length,
      names: venuesAfter.map((v) => ({
        id: v.venueId,
        name: v.venueName,
        city: v.city,
      })),
    };
  }

  const reportPath = path.join(
    OUT,
    APPLY ? "FOUNDER_APPLY_REPORT.json" : "FOUNDER_DRY_RUN.json"
  );
  fs.writeFileSync(reportPath, JSON.stringify(summary, null, 2) + "\n");
  console.log(
    JSON.stringify(
      {
        verdict: APPLY ? "APPLIED" : "DRY_RUN",
        byVerdict: summary.byVerdict,
        reportPath,
        verification: summary.verification || null,
        byHotel: summary.byHotel || null,
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
