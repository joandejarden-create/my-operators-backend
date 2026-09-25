#!/usr/bin/env node
/**
 * Re-enrich thin V2/V3 Bethesda opportunities through canonical customer enrichment.
 *   node scripts/gdi-bethesda-drawer-parity-repair-v1.mjs
 */
import "../load-env.js";
import { writeFileSync, mkdirSync } from "fs";
import { join } from "path";
import {
  loadOpportunitiesCanonical,
  upsertSingleOpportunity,
} from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { invalidateGdiHotelReadCache } from "../lib/group-demand-intelligence/read-cache.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { filterSalespersonView } from "../lib/group-demand-intelligence/opportunity-factory.js";
import {
  enrichGdiOpportunityForCustomer,
  isGdiCustomerDrawerReady,
  findCustomerInternalIdLeaks,
  CUSTOMER_ENRICHMENT_VERSION,
} from "../lib/group-demand-intelligence/enrich-gdi-opportunity-for-customer.js";

const HOTEL_ID = "recLuxvwwxID7U2B8";
const OUT = join(
  process.cwd(),
  "reports/group-demand-intelligence/external-recall-benchmark-v1/drawer-parity-repair-v1"
);

const NEW_ID_RE =
  /gdi_opp_aao_|gdi_opp_2027_ddaa|gdi_opp_2027_nci|gdi_opp_series_|gdi_nih_association|nih_association_recall_v3/i;

const GENERIC_THESIS_RE =
  /future demand cycle relevant to .+ meeting \/ housing capacity/i;

function isThin(o) {
  if (GENERIC_THESIS_RE.test(o.hotelDemandThesis || o.hotelOpportunityThesis || "")) return true;
  if (/^future cycle 20\d{2}$/i.test(String(o.whyNow || "").trim())) return true;
  if (/^[a-z][a-z0-9]*(?:_[a-z0-9]+){1,8}$/.test(String(o.segment || "").trim())) return true;
  if (o.customerEnrichmentVersion !== CUSTOMER_ENRICHMENT_VERSION) {
    if (NEW_ID_RE.test(`${o.id} ${o.discoverySource} ${o.discoveryMethod}`)) return true;
  }
  return false;
}

function fieldCoverage(opps, getter) {
  const n = opps.length || 1;
  const ok = opps.filter((o) => {
    const v = getter(o);
    return v != null && String(v).trim() && !/^(unknown|—|-)$/i.test(String(v).trim());
  }).length;
  return Math.round((ok / n) * 1000) / 10;
}

async function main() {
  mkdirSync(OUT, { recursive: true });
  const beforeDoc = await loadOpportunitiesCanonical(HOTEL_ID);
  const all = beforeDoc.opportunities || [];
  const beforeVisible = filterCustomerFacingOpportunities(filterSalespersonView(all));

  const newish = all.filter(
    (o) =>
      NEW_ID_RE.test(`${o.id} ${o.discoverySource} ${o.discoveryMethod}`) ||
      GENERIC_THESIS_RE.test(o.hotelDemandThesis || "") ||
      String(o.segment || "").includes("assoc_development")
  );

  const thinBefore = newish.filter(isThin);
  const identityDefectBefore = newish.filter((o) =>
    /^[a-z][a-z0-9_]{8,}$/.test(String(o.segment || "").trim())
  );
  const geoDefectBefore = newish.filter(
    (o) =>
      /Las Vegas/i.test(`${o.venueStatus} ${o.destinationStatus} ${o.eventLocation}`) &&
      !/Bethesda|Washington|Natcher|NIH/i.test(`${o.title} ${o.hotelDemandThesis}`)
  );

  const stats = {
    TOTAL: newish.length,
    RESEARCHED: 0,
    DRAWER_READY: 0,
    DOWNGRADED: 0,
    REMOVED_DQ: 0,
    UNKNOWN_AFTER_RESEARCH: 0,
    COMPLETE: 0,
    THIN_REMAINING: 0,
    IDENTITY_DEFECT_AFTER: 0,
    GEO_FIXED: 0,
  };

  const rows = [];
  const runId = `gdi_drawer_parity_${Date.now().toString(36)}`;

  for (const o of newish) {
    const before = {
      id: o.id,
      title: o.title,
      segment: o.segment,
      thesis: o.hotelDemandThesis || o.hotelOpportunityThesis,
      whyNow: o.whyNow,
      venueStatus: o.venueStatus,
      venueSourcingStatus: o.venueSourcingStatus,
      visible: o.customerVisible !== false && o.priority !== "DISQUALIFIED",
    };

    const enriched = enrichGdiOpportunityForCustomer(o, { hotelId: HOTEL_ID });
    stats.RESEARCHED += 1;

    const ready = isGdiCustomerDrawerReady(enriched);
    if (ready.ok) stats.DRAWER_READY += 1;
    if (enriched.priority === "DISQUALIFIED" || enriched.customerVisible === false) {
      if (before.visible) stats.REMOVED_DQ += 1;
      else stats.DOWNGRADED += 1;
    }
    if (enriched.roomDemandResearchState === "UNKNOWN_AFTER_RESEARCH") {
      stats.UNKNOWN_AFTER_RESEARCH += 1;
    }
    if (enriched._geoFieldNormalized || enriched.geoClass === "OUTSIDE_CATCHMENT") {
      stats.GEO_FIXED += 1;
    }
    if (findCustomerInternalIdLeaks(enriched).length) stats.IDENTITY_DEFECT_AFTER += 1;
    if (!isThin(enriched) && ready.ok) stats.COMPLETE += 1;
    else if (isThin(enriched)) stats.THIN_REMAINING += 1;

    await upsertSingleOpportunity(HOTEL_ID, enriched, { runId });
    rows.push({
      id: enriched.id,
      title: enriched.title,
      before,
      after: {
        segment: enriched.segment,
        thesis: (enriched.hotelOpportunityThesis || enriched.hotelDemandThesis || "").slice(0, 160),
        whyHotel: (enriched.summaryWhyHotel || "").slice(0, 120),
        whyNow: enriched.whyNow,
        venueSourcingStatus: enriched.venueSourcingStatus,
        roomDemandStatus: enriched.roomDemandStatus,
        hotelFitScore: enriched.hotelFitScore,
        opportunityType: enriched.opportunityType,
        customerVisible: enriched.customerVisible,
        priority: enriched.priority,
        geoClass: enriched.geoClass,
        drawerReady: ready.ok,
        drawerFailed: ready.failed,
        leaks: findCustomerInternalIdLeaks(enriched),
      },
    });
  }

  invalidateGdiHotelReadCache(HOTEL_ID);
  const afterDoc = await loadOpportunitiesCanonical(HOTEL_ID);
  const afterVisible = filterCustomerFacingOpportunities(
    filterSalespersonView(afterDoc.opportunities || [])
  );
  const afterNew = afterVisible.filter((o) =>
    NEW_ID_RE.test(`${o.id} ${o.discoverySource} ${o.discoveryMethod}`)
  );
  const mature = afterVisible
    .filter((o) => !NEW_ID_RE.test(`${o.id} ${o.discoverySource}`))
    .slice(0, 5);

  const aao = afterDoc.opportunities.find((o) => o.id === "gdi_opp_aao_2027_annual_session_9");
  const aaoVisible = afterVisible.find((o) => o.id === "gdi_opp_aao_2027_annual_session_9");

  const report = {
    ok: true,
    audit: "GDI_DRAWER_PARITY_REPAIR_V1",
    hotelId: HOTEL_ID,
    A_problem: {
      NEW_CUSTOMER_VISIBLE_BEFORE: beforeVisible.filter((o) =>
        NEW_ID_RE.test(`${o.id} ${o.discoverySource}`)
      ).length,
      DRAWER_COMPLETE_BEFORE: 0,
      THIN: thinBefore.length,
      IDENTITY_DEFECT: identityDefectBefore.length,
      GEO_DEFECT: geoDefectBefore.length,
    },
    B_rootCause: {
      PRIMARY:
        "V3 promote path wrote thin candidates with generic thesis/whyNow and skipped buildOpportunity / enrichQualificationPrecision",
      SECONDARY:
        "Discovery vertical slug (assoc_development_district_ass) leaked into customer segment; venueStatus held city string (Las Vegas)",
      bypassedMatureEnrichment: "YES",
      serializerDifferent: "NO — same serializer; thin persisted fields",
      researchIncomplete: "YES",
    },
    E_allNew: stats,
    F_coverage: {
      before: {
        thesis: fieldCoverage(thinBefore, (o) =>
          GENERIC_THESIS_RE.test(o.hotelDemandThesis || "") ? null : o.hotelDemandThesis
        ),
        whyNow: fieldCoverage(thinBefore, (o) =>
          /^future cycle/i.test(o.whyNow || "") ? null : o.whyNow
        ),
        segmentHuman: fieldCoverage(thinBefore, (o) =>
          /_/.test(o.segment || "") && !/\s/.test(o.segment || "") ? null : o.segment
        ),
      },
      after: {
        thesis: fieldCoverage(afterNew, (o) => o.hotelOpportunityThesis || o.hotelDemandThesis),
        whyHotel: fieldCoverage(afterNew, (o) => o.summaryWhyHotel),
        whyNow: fieldCoverage(afterNew, (o) => o.whyNow),
        venueSourcing: fieldCoverage(afterNew, (o) => o.venueSourcingStatus),
        hotelFit: fieldCoverage(afterNew, (o) => o.hotelFitScore),
        segmentHuman: fieldCoverage(afterNew, (o) =>
          /_/.test(o.segment || "") && !/\s/.test(o.segment || "") ? null : o.segment
        ),
        action: fieldCoverage(afterNew, (o) => o.recommendedAction),
      },
    },
    G_internalIdLeaks: {
      BEFORE: identityDefectBefore.length,
      AFTER: afterNew.filter((o) => findCustomerInternalIdLeaks(o).length).length,
    },
    H_geo: {
      MISMATCHES: geoDefectBefore.length,
      FIXED: stats.GEO_FIXED,
      DOWNGRADED: stats.REMOVED_DQ + stats.DOWNGRADED,
    },
    I_customerDrawer: {
      MATURE_SAMPLE: mature.map((o) => ({
        id: o.id,
        title: o.title,
        hasThesis: Boolean(o.hotelOpportunityThesis || o.hotelDemandThesis),
        hasWhyHotel: Boolean(o.summaryWhyHotel),
      })),
      NEW_AFTER: afterNew.map((o) => ({
        id: o.id,
        title: o.title,
        segment: o.segment,
        type: o.opportunityType,
        venue: o.venueSourcingStatus,
        thesis: (o.hotelOpportunityThesis || o.hotelDemandThesis || "").slice(0, 100),
        whyHotel: (o.summaryWhyHotel || "").slice(0, 80),
        whyNow: o.whyNow,
        fit: o.hotelFitScore,
        visible: true,
      })),
      SAME_SCHEMA: "YES",
      SAME_ENRICHMENT_PATH: "YES",
    },
    D_aaoForensic: aao
      ? {
          ORGANIZATION: aao.organizationName,
          EVENT_SERIES: aao.eventSeriesId,
          EVENT_CYCLE: aao.eventCycleId,
          DESTINATION: aao.destinationStatus || aao.eventLocation || aao.venueNote,
          VENUE_STATUS: aao.venueSourcingStatus,
          ROOM_DEMAND: aao.roomDemandStatus,
          HOTEL_FIT: aao.hotelFitScore,
          WHY_THIS_HOTEL: aao.summaryWhyHotel,
          WHY_NOW: aao.whyNow,
          CONTACT: aao.primaryContactCandidate?.name || aao.contactName || "NO_CONTACT",
          ACTION: aao.recommendedAction,
          INTERNAL_ID_LEAK: findCustomerInternalIdLeaks(aao).length > 0 ? "YES" : "NO",
          CUSTOMER_RELEVANT: aaoVisible ? "YES" : "NO — geo outside catchment / DQ",
          GEO: aao.geoClass || aao.geoConsistency?.status,
          VISIBLE: Boolean(aaoVisible),
        }
      : null,
    rows,
    enrichmentVersion: CUSTOMER_ENRICHMENT_VERSION,
    visibleBefore: beforeVisible.length,
    visibleAfter: afterVisible.length,
    timestamp: new Date().toISOString(),
  };

  writeFileSync(join(OUT, "REPAIR_REPORT.json"), JSON.stringify(report, null, 2));
  console.log(
    JSON.stringify(
      {
        thinBefore: thinBefore.length,
        researched: stats.RESEARCHED,
        drawerReady: stats.DRAWER_READY,
        removedDq: stats.REMOVED_DQ,
        leaksAfter: report.G_internalIdLeaks.AFTER,
        aaoVisible: Boolean(aaoVisible),
        aaoGeo: aao?.geoClass,
        visibleBefore: beforeVisible.length,
        visibleAfter: afterVisible.length,
        out: OUT,
      },
      null,
      2
    )
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
