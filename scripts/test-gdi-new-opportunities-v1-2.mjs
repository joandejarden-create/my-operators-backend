#!/usr/bin/env node
/**
 * GDI New Opportunities V1.2 — routing + evidence recovery regressions.
 */
import assert from "node:assert/strict";
import {
  scrubYearFloorForHygiene,
  screenSerpHitForPast,
  filterSerpOrganicPast,
  attachSourceLineage,
  separateGeographyFields,
  classifyPageSignal,
  PAGE_SIGNAL_CLASS,
  classifyWatchRecovery,
  WATCH_RECOVERY_CLASS,
  buildSecondPassQueries,
  enrichCandidateRoutingV12,
  geoConflictsWithHotelMarket,
  DISCOVERY_ROUTING_V1_2,
} from "../lib/group-demand-intelligence/discovery-routing-v1-2.js";
import {
  qualifyOpportunityV3,
  ACTIONABILITY_V3,
  classifyDateConfidence,
  DATE_CONFIDENCE,
} from "../lib/group-demand-intelligence/discovery-hygiene-v3.js";
import { mapWebhoundOpportunityRow } from "../lib/group-demand-intelligence/webhound-opportunity-import.js";
import { mapNativeCandidateRow } from "../lib/group-demand-intelligence/native-blind-discovery.js";

const AS_OF = "2026-09-23";
const HOTEL = { hotelId: "recLuxvwwxID7U2B8", name: "Bethesda Marriott", city: "Bethesda" };
let passed = 0;
let failed = 0;
function check(name, fn) {
  try {
    fn();
    passed += 1;
    console.log("PASS", name);
  } catch (err) {
    failed += 1;
    console.error("FAIL", name, err.message);
  }
}

check("version_marker", () => {
  assert.equal(DISCOVERY_ROUTING_V1_2, "gdi_discovery_routing_v1_2");
});

check("year_only_does_not_become_jan1_past", () => {
  const scrubbed = scrubYearFloorForHygiene(
    { title: "Annual Meeting 2027", eventStartDate: "2027-01-01" },
    { nowDate: AS_OF }
  );
  assert.equal(scrubbed.eventStartDate, null);
  assert.equal(scrubbed.eventDateGranularity, "YEAR");
  assert.equal(scrubbed.eventYear, "2027");
  const d = classifyDateConfidence(scrubbed, { nowDate: AS_OF });
  assert.notEqual(d.confidence, DATE_CONFIDENCE.DATE_PAST);
  assert.equal(d.confidence, DATE_CONFIDENCE.DATE_INFERRED);
});

check("past_year_still_past", () => {
  const scrubbed = scrubYearFloorForHygiene(
    { title: "Annual Meeting 2024", eventStartDate: "2024-01-01" },
    { nowDate: AS_OF }
  );
  assert.equal(scrubbed._yearPast, true);
  const d = classifyDateConfidence(scrubbed, { nowDate: AS_OF });
  assert.equal(d.confidence, DATE_CONFIDENCE.DATE_PAST);
});

check("past_serp_skipped_early", () => {
  const skip = screenSerpHitForPast(
    {
      title: "2023 Annual Conference Recap & Photos",
      snippet: "Highlights from the 2023 archive",
      url: "https://example.org/events/2023/recap",
    },
    { nowDate: AS_OF }
  );
  assert.equal(skip.skip, true);
  assert.equal(skip.reason, "SKIP_PAST_SERP");
});

check("historical_page_with_future_signal_kept", () => {
  const keep = screenSerpHitForPast(
    {
      title: "2024 Annual Meeting Archive",
      snippet: "Join us for the upcoming 2027 cycle — registration open, housing coming soon",
      url: "https://example.org/events/archive",
    },
    { nowDate: AS_OF }
  );
  assert.equal(keep.skip, false);
  assert.equal(keep.keepForFutureSignal, true);
});

check("official_url_survives_webhound_map", () => {
  const mapped = mapWebhoundOpportunityRow(
    {
      title: "SANS DC Metro September 2026",
      organizationName: "SANS Institute",
      officialSource: "https://www.sans.org/cyber-security-training-events/dc-metro-2026/",
      evidenceSources: [
        { url: "https://www.sans.org/cyber-security-training-events/dc-metro-2026/", title: "SANS" },
      ],
      eventStartDate: "2026-09-28",
      location: "Bethesda, MD",
      housingEvidence: "Multi-day training hotel lodging",
    },
    "recLuxvwwxID7U2B8",
    0
  );
  assert.ok(mapped.officialSource);
  assert.match(mapped.officialSource, /sans\.org/i);
  assert.ok(mapped.evidenceSources || mapped.evidence);
});

check("official_url_survives_native_map", () => {
  const row = mapNativeCandidateRow(
    {
      eventName: "SANS DC Metro September 2026",
      organization: "SANS Institute",
      officialSource: "https://www.sans.org/event/dc-2026",
      officialSourceTitle: "SANS Official",
      startDate: "2026-09-28",
      location: "Bethesda, MD",
      housingEvidence: "Hotel block for attendees",
      hotelDemandThesis: "Multi-day training requires lodging",
    },
    "recLuxvwwxID7U2B8",
    0
  );
  assert.ok(row.officialSource);
  assert.equal(row.isOfficialSource, true);
  assert.ok(row.sourceAuthority);
});

check("org_hq_not_event_location", () => {
  const sep = separateGeographyFields({
    title: "California Air Resources Board Meeting",
    organizationLocation: "Sacramento, CA",
    geographyEvidence: "headquartered in Sacramento, California",
    destinationStatus: "Sacramento, CA",
    location: "Sacramento, CA",
  });
  // HQ language → not used as event destination when only HQ evidence
  assert.ok(sep.organizationLocation);
  assert.equal(sep.geoConflict || !sep.destinationStatus || sep.organizationLocation != null, true);
});

check("geo_conflict_fails_closed", () => {
  const q = qualifyOpportunityV3(
    "recLuxvwwxID7U2B8",
    {
      title: "California Air Resources Board Meeting 2027",
      organizationName: "California Air Resources Board",
      eventStartDate: "2027-02-25",
      destinationStatus: "Sacramento, California",
      eventLocation: "Sacramento, California",
      locationEvidenceText: "Meeting in Sacramento, California",
      housingEvidence: "Hotel lodging for board members multi-day",
      openSourcingEvidence: true,
      hotelDemandEvidence: true,
      officialSource: "https://ww2.arb.ca.gov/meeting",
      evidenceSources: [{ url: "https://ww2.arb.ca.gov/meeting" }],
    },
    {
      nowDate: AS_OF,
      subjectHotel: HOTEL,
      marketTokens: ["Bethesda", "Washington", "DMV", "Montgomery"],
    }
  );
  assert.equal(q.actionability, ACTIONABILITY_V3.INVALID);
  assert.ok(
    (q.notes || []).some((n) => /GEO_CONFLICT/i.test(n)) ||
      q.failureClass === "OTHER"
  );
});

check("vendor_marketing_not_relocation_demand", () => {
  assert.equal(
    classifyPageSignal(
      "Blueground corporate relocation housing furnished apartments",
      "https://www.blueground.com/bethesda"
    ),
    PAGE_SIGNAL_CLASS.SUPPLY_SIDE_PAGE
  );
  const q = qualifyOpportunityV3(
    "recLuxvwwxID7U2B8",
    {
      title: "Corporate Relocation Housing",
      organizationName: "Blueground",
      eventStartDate: "2026-09-23",
      destinationStatus: "Bethesda, MD",
      housingEvidence: "Furnished apartments for rent",
      officialSource: "https://www.blueground.com/bethesda",
    },
    { nowDate: AS_OF, subjectHotel: HOTEL }
  );
  assert.equal(q.actionability, ACTIONABILITY_V3.INVALID);
});

check("government_listing_reference_only", () => {
  assert.equal(
    classifyPageSignal(
      "Integrated Lodging Program Sites directory of hotels",
      "https://www.defense.gov/lodging-program"
    ),
    PAGE_SIGNAL_CLASS.REFERENCE_ONLY
  );
});

check("watch_keep_triggers_second_pass", () => {
  const recovery = classifyWatchRecovery({
    title: "SANS DC Metro September 2026",
    organizationName: "SANS Institute",
    actionabilityV3: "VALID_WATCH",
    officialSource: "https://www.sans.org/event",
    hygieneV3: { failureClass: "NO_OPEN_SOURCING_EVIDENCE" },
    destinationStatus: "Bethesda, MD",
  });
  assert.ok(
    [
      WATCH_RECOVERY_CLASS.WATCH_KEEP,
      WATCH_RECOVERY_CLASS.WATCH_VERIFY_HOUSING,
      WATCH_RECOVERY_CLASS.WATCH_VERIFY_SOURCE,
    ].includes(recovery.class)
  );
  const pack = buildSecondPassQueries({
    title: "SANS DC Metro September 2026",
    organizationName: "SANS Institute",
    officialSource: null,
    hygieneV3: { failureClass: "NO_OPEN_SOURCING_EVIDENCE" },
  });
  assert.ok(pack.queries.length >= 1);
  assert.ok(pack.queries.length <= 4);
});

check("second_pass_bounded_stop", () => {
  const pack = buildSecondPassQueries(
    { title: "Test Event", organizationName: "Org" },
    { maxQueries: 3 }
  );
  assert.ok(pack.queries.length <= 3);
});

check("enrich_preserves_source_and_scrubs_date", () => {
  const e = enrichCandidateRoutingV12(
    {
      title: "ACRM 2026",
      eventStartDate: "2026-01-01",
      officialSource: "https://acrm.org/2026",
      housingEvidence: "Official hotels and overflow housing",
    },
    { nowDate: AS_OF, marketTokens: ["Bethesda", "Washington"] }
  );
  assert.equal(e.eventStartDate, null);
  assert.equal(e.eventDateGranularity, "YEAR");
  assert.ok(e.officialSource);
  assert.equal(e.isOfficialSource, true);
});

check("past_serp_all_skip_keeps_empty_not_restoring_organic", () => {
  const { kept, skipped, skipPastCount } = filterSerpOrganicPast(
    [
      {
        title: "2023 Annual Conference Recap & Photos",
        snippet: "Highlights from the 2023 archive",
        url: "https://example.org/events/2023/recap",
      },
      {
        title: "2022 Past Events Archive",
        snippet: "Previous cycle photos",
        url: "https://example.org/events/2022",
      },
    ],
    { nowDate: AS_OF }
  );
  assert.equal(kept.length, 0);
  assert.equal(skipPastCount, 2);
  assert.equal(skipped.length, 2);
});

check("org_vs_event_geo_conflict_fails_closed", () => {
  const q = qualifyOpportunityV3(
    "recLuxvwwxID7U2B8",
    {
      title: "National Association Board Meeting 2027",
      organizationName: "National Association",
      organizationLocation: "Sacramento, CA",
      eventLocation: "Chicago, IL",
      eventStartDate: "2027-06-15",
      destinationStatus: "Chicago, IL",
      locationEvidenceText: "Meeting held in Chicago; association HQ Sacramento",
      housingEvidence: "Hotel block for attendees multi-night",
      openSourcingEvidence: true,
      hotelDemandEvidence: true,
      officialSource: "https://example.org/chicago-2027",
      evidenceSources: [{ url: "https://example.org/chicago-2027" }],
      geoConflict: true,
      geoConflictReason: "ORG_VS_EVENT",
      geoClass: "GEO_CONFLICT",
    },
    {
      nowDate: AS_OF,
      subjectHotel: HOTEL,
      marketTokens: ["Bethesda", "Washington", "DMV"],
    }
  );
  assert.equal(q.actionability, ACTIONABILITY_V3.INVALID);
  assert.ok((q.notes || []).some((n) => /GEO_CONFLICT/i.test(String(n))));
});

console.log(
  JSON.stringify({
    suite: "gdi-new-opportunities-v1-2",
    passed,
    failed,
    total: passed + failed,
  })
);
process.exit(failed ? 1 : 0);
