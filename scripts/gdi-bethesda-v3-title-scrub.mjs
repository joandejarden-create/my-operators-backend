#!/usr/bin/env node
import "../load-env.js";
import {
  loadOpportunitiesCanonical,
  upsertSingleOpportunity,
} from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { invalidateGdiHotelReadCache } from "../lib/group-demand-intelligence/read-cache.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { filterSalespersonView } from "../lib/group-demand-intelligence/opportunity-factory.js";
import { upsertLocalResearchTarget } from "../lib/group-demand-intelligence/local-research-targets.js";
import {
  buildResearchTarget,
  TARGET_TYPE,
  TARGET_PRIORITY,
  RESEARCH_CADENCE,
  suggestNextResearchAt,
} from "../lib/group-demand-intelligence/research-coverage/entities.js";

const HOTEL = "recLuxvwwxID7U2B8";
const doc = await loadOpportunitiesCanonical(HOTEL);
const scrubIds = [];
const fixes = [];

for (const o of doc.opportunities || []) {
  const blob = `${o.title || ""} ${o.officialSource || ""}`;
  if (/passcode|meeting id|skip to main content|dissemination library browse/i.test(o.title || "")) {
    o.customerVisible = false;
    o.isTestData = true;
    o.priority = "DISQUALIFIED";
    o.opportunityQualification = "CLOSED";
    scrubIds.push(o.id);
    await upsertSingleOpportunity(HOTEL, o, { runId: "gdi_v3_title_scrub" });
    continue;
  }
  if (/ctn-annual-conference|ctn annual conference/i.test(blob)) {
    Object.assign(o, {
      title: "CTN Annual Conference 2027 — Bethesda, MD (housing pending)",
      organizationName: "NIDA Clinical Trials Network",
      opportunityType: "FIXED_VENUE_OPEN_HOUSING",
      salesPartitionV11: "HOUSING",
      customerFacingState: "WATCH",
      priority: "WATCHLIST",
      eventStartDate: "2027-03-15",
      eventYear: 2027,
      lodgingEvidence: "Official CTN library: accommodations TBD; Bethesda venue",
      housingEvidence: "FIXED_VENUE_OPEN_HOUSING",
      hotelDemandThesis:
        "CTN Annual Conference at Bethesda — room block / overflow / VIP housing motion",
      recommendedAction: "Pursue housing around Bethesda/NIH campus for March 2027",
      customerVisible: true,
      isTestData: false,
      opportunityQualification: "MODERATE",
      officialSource:
        o.officialSource?.includes("ctn-annual")
          ? o.officialSource
          : "https://ctnlibrary.org/2026/09/18/ctn-annual-conference-march-15-17-2027-bethesda-md/",
    });
    fixes.push(o.id);
    await upsertSingleOpportunity(HOTEL, o, { runId: "gdi_v3_title_scrub" });
    upsertLocalResearchTarget(
      HOTEL,
      buildResearchTarget({
        hotelId: HOTEL,
        hotelName: "Bethesda Marriott",
        targetType: TARGET_TYPE.EVENT_SERIES,
        entityKey: "series:ctn_annual_conference",
        seriesId: "series:ctn_annual_conference",
        canonicalName: "CTN Annual Conference",
        officialDomain: "ctnlibrary.org",
        primarySourceUrl: o.officialSource,
        priority: TARGET_PRIORITY.HIGH,
        researchCadence: RESEARCH_CADENCE.MONTHLY,
        nextResearchAt: suggestNextResearchAt({
          priority: TARGET_PRIORITY.HIGH,
          researchCadence: RESEARCH_CADENCE.MONTHLY,
        }),
      })
    );
    continue;
  }
  if (/nci rna biology/i.test(o.title || "") || /2027NCI_RNA/i.test(o.officialSource || "")) {
    Object.assign(o, {
      title: "2027 NCI RNA Biology Symposium — Hotel & Travel open",
      organizationName: "NCI RNA Biology Initiative",
      opportunityType: "FIXED_VENUE_OPEN_HOUSING",
      salesPartitionV11: "HOUSING",
      customerFacingState: "WATCH",
      priority: "WATCHLIST",
      eventStartDate: "2027-04-15",
      eventYear: 2027,
      officialSource:
        "https://ncifrederick.cancer.gov/events/conferences/2027NCI_RNA_Symposium/",
      lodgingEvidence: "Official conference page includes Hotel & Travel section",
      housingEvidence: "HOUSING_PENDING",
      customerVisible: true,
      isTestData: false,
      opportunityQualification: "MODERATE",
    });
    fixes.push(o.id);
    await upsertSingleOpportunity(HOTEL, o, { runId: "gdi_v3_title_scrub" });
  }
}

invalidateGdiHotelReadCache(HOTEL);
const after = await loadOpportunitiesCanonical(HOTEL);
const visible = filterCustomerFacingOpportunities(
  filterSalespersonView(after.opportunities || [])
);
const sample = visible.filter((o) =>
  /ctn annual|high-risk|nci rna|ddaa|aao 2027 annual/i.test(o.title || "")
);
console.log(
  JSON.stringify(
    {
      scrubIds,
      fixes,
      apiVisible: visible.length,
      sample: sample.map((o) => ({
        id: o.id,
        title: o.title,
        state: o.customerFacingState || o.salesPartitionV11,
        type: o.opportunityType,
        src: o.officialSource,
      })),
    },
    null,
    2
  )
);
