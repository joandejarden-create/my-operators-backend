#!/usr/bin/env node
/**
 * Drawer parity / customer enrichment unit tests.
 *   node scripts/test-gdi-drawer-parity-repair-v1.mjs
 */
import assert from "assert";
import {
  enrichGdiOpportunityForCustomer,
  sanitizeCustomerSegment,
  normalizeMisplacedGeoFields,
  clearGenericBoilerplate,
  assessGeoConsistencyForHotel,
  isGdiCustomerDrawerReady,
  findCustomerInternalIdLeaks,
  CUSTOMER_ENRICHMENT_VERSION,
} from "../lib/group-demand-intelligence/enrich-gdi-opportunity-for-customer.js";

assert.strictEqual(
  sanitizeCustomerSegment("assoc_development_district_ass", {
    title: "AAO 2027 Annual Session",
    organizationName: "American Academy of Ophthalmology",
  }),
  "Association"
);

const geoNorm = normalizeMisplacedGeoFields({
  venueStatus: "Las Vegas, NV",
  title: "AAO 2027",
});
assert.ok(geoNorm.destinationStatus === "Las Vegas, NV");
assert.ok(geoNorm.venueStatus === "UNKNOWN" || geoNorm.venueStatus !== "Las Vegas, NV");

const cleared = clearGenericBoilerplate({
  hotelDemandThesis:
    "Future demand cycle relevant to Bethesda Marriott meeting / housing capacity",
  whyNow: "Future cycle 2027",
});
assert.ok(!cleared.hotelDemandThesis);
assert.ok(!cleared.whyNow);

const geo = assessGeoConsistencyForHotel({
  destinationStatus: "Las Vegas, NV",
  title: "AAO 2027 Annual Session",
  opportunityType: "PRIMARY_PURSUIT",
});
assert.strictEqual(geo.status, "OUTSIDE_CATCHMENT");

const thin = {
  id: "gdi_opp_aao_2027_annual_session_9",
  hotelId: "recLuxvwwxID7U2B8",
  title: "AAO 2027 Annual Session",
  organizationName: "American Academy of Ophthalmology",
  segment: "assoc_development_district_ass",
  venueStatus: "Las Vegas, NV",
  venueSourcingStatus: "UNKNOWN",
  roomDemandStatus: "UNKNOWN",
  hotelDemandThesis:
    "Future demand cycle relevant to Bethesda Marriott meeting / housing capacity",
  whyNow: "Future cycle 2027",
  opportunityQualification: "MODERATE",
  opportunityType: "PRIMARY_PURSUIT",
  eventStartDate: "2027-06-01",
  eventYear: 2027,
  officialSource:
    "https://www.aao.org/young-ophthalmologists/yo-info/article/aao-2026-events-just-early-career-ophthalmologists",
  sources: [
    {
      url: "https://www.aao.org/young-ophthalmologists/yo-info/article/aao-2026-events-just-early-career-ophthalmologists",
    },
  ],
  customerVisible: true,
  priority: "WATCHLIST",
};

const enriched = enrichGdiOpportunityForCustomer(thin, {
  hotelId: "recLuxvwwxID7U2B8",
});

assert.ok(!/assoc_development/.test(enriched.segment || ""));
assert.ok(!/Future demand cycle relevant/.test(enriched.hotelOpportunityThesis || ""));
assert.ok(!/^Future cycle 2027$/i.test(enriched.whyNow || ""));
assert.ok(enriched.customerEnrichmentVersion === CUSTOMER_ENRICHMENT_VERSION);
assert.ok(
  enriched.geoConsistency?.status === "OUTSIDE_CATCHMENT" ||
    enriched.geoClass === "OUTSIDE_CATCHMENT" ||
    enriched.customerVisible === false ||
    enriched.priority === "DISQUALIFIED",
  "AAO Las Vegas should geo-downgrade"
);
assert.strictEqual(findCustomerInternalIdLeaks(enriched).length, 0);

// Bethesda-relevant housing opportunity should stay visible and get hotel-specific why
const ctn = enrichGdiOpportunityForCustomer(
  {
    id: "gdi_opp_series_ctn_annual_conference_2027",
    hotelId: "recLuxvwwxID7U2B8",
    title: "CTN Annual Conference 2027 — Bethesda, MD (housing pending)",
    organizationName: "NIDA Clinical Trials Network",
    opportunityType: "FIXED_VENUE_OPEN_HOUSING",
    opportunityQualification: "MODERATE",
    eventStartDate: "2027-03-15",
    eventYear: 2027,
    destinationStatus: "Bethesda, MD",
    roomDemandStatus: "HOUSING_PENDING",
    lodgingEvidence: "Accommodations TBD",
    housingEvidence: "FIXED_VENUE_OPEN_HOUSING",
    officialSource:
      "https://ctnlibrary.org/2026/09/18/ctn-annual-conference-march-15-17-2027-bethesda-md/",
    sources: [
      {
        url: "https://ctnlibrary.org/2026/09/18/ctn-annual-conference-march-15-17-2027-bethesda-md/",
      },
    ],
    hotelDemandThesis:
      "Future demand cycle relevant to Bethesda Marriott meeting / housing capacity",
    whyNow: "Future cycle 2027",
    customerVisible: true,
    priority: "WATCHLIST",
    cqState: "HOUSING",
    salesPartitionV11: "HOUSING",
  },
  { hotelId: "recLuxvwwxID7U2B8" }
);

assert.ok(ctn.customerVisible !== false);
assert.ok(ctn.summaryWhyHotel && ctn.summaryWhyHotel.length > 20);
assert.ok(ctn.whyNow && !/^Future cycle 2027$/i.test(ctn.whyNow));
assert.ok(ctn.hotelFitScore != null);
const ready = isGdiCustomerDrawerReady(ctn);
assert.ok(ready.ok, JSON.stringify(ready.failed));

console.log(
  JSON.stringify(
    {
      ok: true,
      suite: "gdi_drawer_parity_repair_v1",
      aaoHiddenOrDq: enriched.customerVisible === false || enriched.priority === "DISQUALIFIED",
      ctnReady: ready.ok,
      ctnSegment: ctn.segment,
    },
    null,
    2
  )
);
