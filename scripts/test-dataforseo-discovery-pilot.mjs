/**
 * Unit tests for DataForSEO discovery pilot v2 (no live API / no Census writes).
 */
import assert from "node:assert/strict";
import {
  assertDataForSeoCandidateOnlyMode,
  scoreIncompletePriority,
  scoreParentPriority,
  buildDiscoveryQueriesForRecord,
  resolveBrandSiteDomain,
  DATAFORSEO_DISCOVERY_PILOT_OBJECTIVE,
} from "../lib/research-engine-v2/dataforseo-discovery-pilot.js";
import {
  classifySerpOrMapsItem,
  summarizeClassifiedCandidates,
  estimateOfficialUrlPrecision,
  SOURCE_TIER,
} from "../lib/research-engine-v2/dataforseo-candidate-classifier.js";
import { resolveDataForSeoLocationName } from "../lib/research-engine-v2/dataforseo-client.js";
import { isForbiddenAutopilotField } from "../lib/research-engine-v2/census-autopilot-field-allowlist.js";
import { resolveMissionObjective } from "../lib/research-engine-v2/census-autopilot-mission.js";

function test(name, fn) {
  try {
    fn();
    console.log(`[PASS] ${name}`);
  } catch (err) {
    console.error(`[FAIL] ${name}`);
    throw err;
  }
}

test("candidate-only gate requires flags", () => {
  assert.equal(
    assertDataForSeoCandidateOnlyMode({
      DATAFORSEO_ENABLED: "1",
      DATAFORSEO_WRITE_CANDIDATES_ONLY: "1",
      ENABLE_DATAFORSEO_VALIDATED_WRITES: "0",
    }).ok,
    true
  );
  assert.equal(
    assertDataForSeoCandidateOnlyMode({
      DATAFORSEO_ENABLED: "1",
      DATAFORSEO_WRITE_CANDIDATES_ONLY: "0",
      ENABLE_DATAFORSEO_VALIDATED_WRITES: "0",
    }).ok,
    false
  );
  assert.equal(
    assertDataForSeoCandidateOnlyMode({
      DATAFORSEO_ENABLED: "1",
      DATAFORSEO_WRITE_CANDIDATES_ONLY: "1",
      ENABLE_DATAFORSEO_VALIDATED_WRITES: "1",
    }).ok,
    false
  );
  assert.equal(
    assertDataForSeoCandidateOnlyMode({
      DATAFORSEO_ENABLED: "1",
      DATAFORSEO_WRITE_CANDIDATES_ONLY: "1",
      ENABLE_DATAFORSEO_VALIDATED_WRITES: "0",
    }).dataforseo_is_source_of_truth,
    false
  );
});

test("priority prefers missing URL then rooms then address then phone", () => {
  const url = scoreIncompletePriority({
    "Official Property URL": "",
    "Rooms / Keys": 100,
    Phone: "1",
    Address: "x",
    Latitude: 1,
    Longitude: 1,
  });
  assert.equal(url.priority_band, 1);
  const rooms = scoreIncompletePriority({
    "Official Property URL": "https://example.com",
    "Rooms / Keys": null,
    Phone: "",
    Address: "x",
    Latitude: 1,
    Longitude: 1,
  });
  assert.equal(rooms.priority_band, 2);
  const address = scoreIncompletePriority({
    "Official Property URL": "https://example.com",
    "Rooms / Keys": 10,
    Phone: "",
    Address: "",
    Latitude: 1,
    Longitude: 1,
  });
  assert.equal(address.priority_band, 3);
  const phone = scoreIncompletePriority({
    "Official Property URL": "https://example.com",
    "Rooms / Keys": 10,
    Phone: "",
    Address: "1 Main",
    Latitude: 1,
    Longitude: 1,
  });
  assert.equal(phone.priority_band, 4);
});

test("parent priority Choice before Marriott before IHG", () => {
  assert.equal(scoreParentPriority({ "Brand Family": "Choice" }).parent_band, 1);
  assert.equal(
    scoreParentPriority({ "Brand Family": "Marriott" }).parent_band,
    2
  );
  assert.equal(scoreParentPriority({ "Brand Family": "IHG" }).parent_band, 3);
  assert.equal(scoreParentPriority({ "Brand Family": "Accor" }).parent_band, 4);
});

test("classifier rejects affiliates and accepts brand official", () => {
  const ota = classifySerpOrMapsItem({
    title: "Hotel Foo",
    url: "https://www.booking.com/hotel/mx/foo.html",
    description: "Book now",
  });
  assert.equal(ota.status, "rejected");

  const affiliate = classifySerpOrMapsItem(
    {
      title: "Staybridge Suites Puebla",
      url: "https://www.hoteles.com/ho536163/staybridge-suites-puebla/",
      description: "Hotel",
    },
    { hotelName: "Staybridge Suites Puebla" }
  );
  assert.equal(affiliate.status, "rejected");
  assert.equal(affiliate.reason, "rejected_affiliate_mirror");

  const mirror = classifySerpOrMapsItem(
    {
      title: "Staybridge Suites Puebla",
      url: "https://staybridge-suites.puebla-hotels.com/es/",
      description: "Hotel",
    },
    { hotelName: "Staybridge Suites Puebla" }
  );
  assert.equal(mirror.status, "rejected");

  const roomsAero = classifySerpOrMapsItem(
    {
      title: "La Purificadora rooms",
      url: "https://rooms.aero/marriott/hotel/abc",
      description: "rooms",
    },
    { hotelName: "La Purificadora" }
  );
  assert.equal(roomsAero.status, "rejected");

  const official = classifySerpOrMapsItem(
    {
      title: "Courtyard Mexico City",
      url: "https://www.marriott.com/en-us/hotels/mexcy-courtyard/overview/",
      description: "Official hotel site",
    },
    { hotelName: "Courtyard Mexico City" }
  );
  assert.equal(official.status, "useful");
  assert.equal(official.source_tier, SOURCE_TIER.brand_official);
  assert.ok(official.categories.includes("official_hotel_url_candidate"));
  assert.equal(official.write_eligible, false);

  const travelWeekly = classifySerpOrMapsItem(
    {
      title: "City Express Puebla",
      url: "https://www.travelweekly.com/Hotels/Puebla-Mexico/City-Express-p59092916",
      description: "180 rooms",
    },
    { hotelName: "City Express Puebla" }
  );
  assert.equal(travelWeekly.status, "secondary");
  assert.ok(
    travelWeekly.categories.includes("trusted_secondary_verification_candidate")
  );
  assert.equal(travelWeekly.write_eligible, false);

  const rooms = classifySerpOrMapsItem(
    {
      title: "Hilton Test — Fact sheet 180 guest rooms",
      url: "https://www.hilton.com/en/hotels/xyz/fact-sheet/",
      description: "180 guest rooms",
    },
    { hotelName: "Hilton Test" }
  );
  assert.equal(rooms.status, "useful");
  assert.ok(rooms.categories.includes("rooms_evidence_page_candidate"));

  const maps = classifySerpOrMapsItem({
    title: "Hotel Test",
    address: "1 Main St",
    phone: "+52 55 1234",
    latitude: 19.4,
    longitude: -99.1,
    place_id: "ChIJtest",
  });
  assert.ok(maps.categories.includes("google_maps_local_candidate"));
  assert.ok(maps.categories.includes("phone_candidate"));
  assert.equal(maps.storage_policy_flag, "candidate_only_no_census_write");
});

test("query builder multilingual + max 6 + maps for contact gaps", () => {
  const { queries } = buildDiscoveryQueriesForRecord(
    {
      "Canonical Property Name": "Test Hotel",
      City: "Cancún",
      Country: "Mexico",
      "Brand Family": "Marriott",
      "Official Property URL": "",
      "Rooms / Keys": null,
      Phone: "",
      Address: "",
    },
    { maxQueries: 6, enableSerp: true, enableMaps: true }
  );
  assert.ok(queries.length <= 6);
  assert.ok(queries.some((q) => q.kind === "serp_organic"));
  assert.ok(queries.some((q) => q.kind === "google_maps"));
  assert.ok(
    queries.some(
      (q) =>
        /sitio oficial|página oficial|official hotel|site:marriott\.com/i.test(
          q.keyword
        )
    )
  );
  assert.equal(
    resolveBrandSiteDomain({ "Brand Family": "Marriott" }),
    "marriott.com"
  );
});

test("location helper and forbidden fields", () => {
  assert.ok(
    resolveDataForSeoLocationName({
      city: "Bogotá",
      country: "Colombia",
    }).includes("Colombia")
  );
  assert.equal(isForbiddenAutopilotField("Owner Name"), true);
  assert.equal(isForbiddenAutopilotField("Brand Status"), true);
});

test("summarize + precision + objective wiring", () => {
  const s = summarizeClassifiedCandidates([
    {
      status: "useful",
      categories: ["official_hotel_url_candidate"],
      url: "https://marriott.com/x",
      source_tier: SOURCE_TIER.brand_official,
    },
    { status: "rejected", reason: "rejected_affiliate_mirror" },
    {
      status: "secondary",
      categories: ["trusted_secondary_verification_candidate"],
      source_tier: SOURCE_TIER.hospitality_trade_secondary,
    },
  ]);
  assert.equal(s.useful_count, 1);
  assert.equal(s.secondary_count, 1);
  assert.equal(s.official_hotel_urls.length, 1);

  const p = estimateOfficialUrlPrecision([
    { source_tier: SOURCE_TIER.brand_official },
    { source_tier: SOURCE_TIER.hotel_official },
  ]);
  assert.ok(p.precision_estimate > 0.7);

  assert.equal(
    resolveMissionObjective("dataforseo-discovery-pilot-v2"),
    DATAFORSEO_DISCOVERY_PILOT_OBJECTIVE
  );
});

test("validated-write policy gates + no SERP-snippet / Maps / Travel Weekly writes", async () => {
  const {
    resolveDataForSeoValidatedWriteGates,
    classifyCandidateForValidatedWrite,
    validateHotelOfficialStrict,
    isPropertySpecificBrandUrl,
    isRejectedDiscoveryHost,
    extractValidatedRoomsFromHtml,
  } = await import("../lib/research-engine-v2/dataforseo-validated-write-policy.js");
  const { SOURCE_TIER: T } = await import(
    "../lib/research-engine-v2/dataforseo-candidate-classifier.js"
  );

  assert.equal(
    resolveDataForSeoValidatedWriteGates({
      DATAFORSEO_ENABLED: "1",
      DATAFORSEO_WRITE_CANDIDATES_ONLY: "0",
      ENABLE_DATAFORSEO_VALIDATED_WRITES: "1",
      ENABLE_DATAFORSEO_URL_WRITES: "1",
      ENABLE_DATAFORSEO_ROOMS_WRITES: "1",
      ENABLE_DATAFORSEO_ADDRESS_WRITES: "0",
      ENABLE_DATAFORSEO_PHONE_WRITES: "0",
      ENABLE_DATAFORSEO_COORDINATE_WRITES: "0",
    }).ok,
    true
  );
  assert.equal(
    resolveDataForSeoValidatedWriteGates({
      DATAFORSEO_ENABLED: "1",
      DATAFORSEO_WRITE_CANDIDATES_ONLY: "0",
      ENABLE_DATAFORSEO_VALIDATED_WRITES: "1",
      ENABLE_DATAFORSEO_URL_WRITES: "1",
      ENABLE_DATAFORSEO_ROOMS_WRITES: "1",
      ENABLE_DATAFORSEO_ADDRESS_WRITES: "1",
    }).ok,
    false
  );
  const gates = resolveDataForSeoValidatedWriteGates({
    DATAFORSEO_ENABLED: "1",
    DATAFORSEO_WRITE_CANDIDATES_ONLY: "0",
    ENABLE_DATAFORSEO_VALIDATED_WRITES: "1",
    ENABLE_DATAFORSEO_URL_WRITES: "1",
    ENABLE_DATAFORSEO_ROOMS_WRITES: "1",
    ENABLE_DATAFORSEO_ADDRESS_WRITES: "0",
    ENABLE_DATAFORSEO_PHONE_WRITES: "0",
    ENABLE_DATAFORSEO_COORDINATE_WRITES: "0",
  });
  assert.equal(gates.dataforseo_is_source_of_truth, false);
  assert.equal(gates.serp_snippet_writes_allowed, false);
  assert.equal(gates.maps_writes, false);
  assert.equal(gates.travel_weekly_direct_writes, false);

  const ota = classifyCandidateForValidatedWrite({
    url: "https://www.booking.com/hotel/mx/x.html",
    source_tier: T.hotel_official,
    categories: ["official_hotel_url_candidate"],
  });
  assert.equal(ota.action, "reject");

  const tw = classifyCandidateForValidatedWrite({
    url: "https://www.travelweekly.com/Hotels/x",
    source_tier: T.hospitality_trade_secondary,
    categories: ["trusted_secondary_verification_candidate"],
  });
  assert.equal(tw.action, "hold");

  const maps = classifyCandidateForValidatedWrite({
    url: "",
    categories: ["google_maps_local_candidate", "address_candidate"],
    address: "1 Main",
  });
  assert.ok(maps.action === "skip" || maps.action === "hold");

  assert.equal(
    isPropertySpecificBrandUrl(
      "https://www.choicehotels.com/mexico/puebla/comfort-inn-hotels/mx224"
    ),
    true
  );
  assert.equal(
    isRejectedDiscoveryHost("hotels-oaxaca.com"),
    true
  );
  assert.equal(isRejectedDiscoveryHost("cvent.com"), true);

  const strictFail = validateHotelOfficialStrict({
    url: "https://random-hotel-mirror.example/x",
    html: "<html><title>Unrelated</title><body>Book now affiliate deals</body></html>",
    hotelName: "Courtyard Mexico City",
    city: "Mexico City",
    country: "Mexico",
  });
  assert.equal(strictFail.ok, false);

  const roomsFromSnippetAlone = extractValidatedRoomsFromHtml(
    "",
    "https://www.marriott.com/en-us/hotels/mexcy-courtyard/overview/"
  );
  assert.equal(roomsFromSnippetAlone.ok, false);

  const roomsHtml = extractValidatedRoomsFromHtml(
    `<html><title>Courtyard Mexico City</title><body>The hotel features 245 guest rooms in Mexico City.</body></html>`,
    "https://www.marriott.com/en-us/hotels/mexcy-courtyard/overview/",
    { hotelName: "Courtyard Mexico City", page_validated: true }
  );
  assert.equal(roomsHtml.ok, true);
  assert.equal(roomsHtml.rooms, 245);
  assert.equal(roomsHtml.confidence, "High");

  const {
    validateBrandOfficialUrlBotBlocked,
  } = await import("../lib/research-engine-v2/dataforseo-validated-write-policy.js");
  const botOk = validateBrandOfficialUrlBotBlocked({
    url: "https://www.marriott.com/en-us/hotels/mexcy-courtyard-mexico-city/overview/",
    hotelName: "Courtyard Mexico City",
    city: "Mexico City",
    title: "Courtyard Mexico City",
  });
  assert.equal(botOk.ok, true);
  assert.equal(botOk.bot_blocked, true);

  assert.equal(
    resolveMissionObjective("dataforseo-validated-write-policy-v1"),
    "dataforseo-validated-write-policy-v1"
  );
});

test("local-business enrichment candidate-only gates + match classes", async () => {
  const {
    assertDataForSeoLocalCandidateOnly,
    scoreIncompleteLocalPriority,
    DATAFORSEO_LOCAL_PILOT_MARKETS,
  } = await import(
    "../lib/research-engine-v2/dataforseo-local-business-enrichment-v1.js"
  );
  const {
    scoreLocalBusinessToCensus,
    matchDiscoveryItemToCensus,
    classifyLodgingType,
    MATCH_CLASS,
    LODGING_CLASS,
  } = await import("../lib/research-engine-v2/dataforseo-local-match.js");
  const { isForbiddenAutopilotField } = await import(
    "../lib/research-engine-v2/census-autopilot-field-allowlist.js"
  );
  const { assertNoInsertInFieldCompletionMode, CENSUS_MODE } = await import(
    "../lib/research-engine-v2/census-autopilot-full-latam-v3.js"
  );

  assert.equal(
    assertDataForSeoLocalCandidateOnly({
      DATAFORSEO_ENABLED: "1",
      DATAFORSEO_WRITE_CANDIDATES_ONLY: "1",
      ENABLE_DATAFORSEO_VALIDATED_WRITES: "0",
      DATAFORSEO_ENABLE_GOOGLE_MAPS: "1",
      DATAFORSEO_ENABLE_BUSINESS_LISTINGS: "1",
      ENABLE_DATAFORSEO_LOCAL_ADDRESS_WRITES: "0",
      ENABLE_DATAFORSEO_LOCAL_PHONE_WRITES: "0",
      ENABLE_DATAFORSEO_LOCAL_WEBSITE_WRITES: "0",
      ENABLE_DATAFORSEO_LOCAL_COORDINATE_WRITES: "0",
    }).ok,
    true
  );
  assert.equal(
    assertDataForSeoLocalCandidateOnly({
      DATAFORSEO_ENABLED: "1",
      DATAFORSEO_WRITE_CANDIDATES_ONLY: "1",
      ENABLE_DATAFORSEO_VALIDATED_WRITES: "0",
      DATAFORSEO_ENABLE_GOOGLE_MAPS: "1",
      ENABLE_DATAFORSEO_LOCAL_PHONE_WRITES: "1",
    }).ok,
    false
  );
  assert.equal(
    assertDataForSeoLocalCandidateOnly({
      DATAFORSEO_ENABLED: "1",
      DATAFORSEO_WRITE_CANDIDATES_ONLY: "1",
      ENABLE_DATAFORSEO_VALIDATED_WRITES: "0",
      DATAFORSEO_ENABLE_GOOGLE_MAPS: "1",
    }).census_writes_allowed,
    false
  );
  assert.equal(
    assertDataForSeoLocalCandidateOnly({
      DATAFORSEO_ENABLED: "1",
      DATAFORSEO_WRITE_CANDIDATES_ONLY: "1",
      ENABLE_DATAFORSEO_VALIDATED_WRITES: "0",
      DATAFORSEO_ENABLE_GOOGLE_MAPS: "1",
    }).rooms_from_maps_allowed,
    false
  );

  assert.equal(DATAFORSEO_LOCAL_PILOT_MARKETS.length, 9);
  assert.equal(
    scoreIncompleteLocalPriority({
      Address: "",
      Phone: "1",
      "Official Property URL": "https://x.com",
      Latitude: 1,
      Longitude: 1,
    }).priority_band,
    1
  );

  const high = scoreLocalBusinessToCensus(
    {
      title: "Courtyard Mexico City Airport",
      address: "Mexico City, Mexico",
      url: "https://www.marriott.com/en-us/hotels/mexca-courtyard/overview/",
      latitude: 19.43,
      longitude: -99.08,
      category: "Hotel",
    },
    {
      "Canonical Property Name": "Courtyard Mexico City Airport",
      City: "Mexico City",
      Country: "Mexico",
      "Brand Family": "Marriott",
      "Official Property URL":
        "https://www.marriott.com/en-us/hotels/mexca-courtyard/overview/",
      Latitude: 19.43,
      Longitude: -99.08,
    },
    { recordId: "recTest" }
  );
  assert.equal(high.match_class, MATCH_CLASS.MATCH_HIGH);
  assert.equal(high.write_eligible_future, true);
  assert.equal(high.storage_policy_flag, "candidate_only_no_census_write");

  assert.equal(
    classifyLodgingType({ title: "Downtown Hostel", category: "Hostel" }),
    LODGING_CLASS.HOSTEL
  );
  assert.equal(
    classifyLodgingType({ title: "Taco Restaurant", category: "Restaurant" }),
    LODGING_CLASS.NON_HOTEL
  );

  const discovery = matchDiscoveryItemToCensus(
    {
      title: "Brand New Boutique Hotel XYZ Unique",
      address: "Cancún, Mexico",
      category: "Hotel",
      latitude: 21.16,
      longitude: -86.85,
    },
    [
      {
        id: "recOther",
        fields: {
          "Canonical Property Name": "Totally Different Resort",
          City: "Cancún",
          Country: "Mexico",
        },
      },
    ]
  );
  assert.equal(discovery.discovery_class, "new_hotel_candidate");

  assert.equal(
    assertNoInsertInFieldCompletionMode(CENSUS_MODE.FIELD_COMPLETION_ONLY, 1)
      .ok,
    false
  );
  assert.equal(
    assertNoInsertInFieldCompletionMode(CENSUS_MODE.FIELD_COMPLETION_ONLY, 0)
      .ok,
    true
  );
  assert.equal(isForbiddenAutopilotField("Owner Name"), true);
  assert.equal(isForbiddenAutopilotField("Brand Status"), true);

  assert.equal(
    resolveMissionObjective("dataforseo-local-business-enrichment-v1"),
    "dataforseo-local-business-enrichment-v1"
  );
});

test("local-business validated write gates + match_high website/address rules", async () => {
  const {
    resolveDataForSeoLocalValidatedWriteGates,
    evaluateLocalWebsiteWrite,
    evaluateLocalAddressWrite,
    DATAFORSEO_LOCAL_VALIDATED_WRITE_OBJECTIVE,
    DATAFORSEO_LOCAL_VALIDATED_WRITE_STATUS,
  } = await import(
    "../lib/research-engine-v2/dataforseo-local-business-validated-write-v1.js"
  );
  const { MATCH_CLASS } = await import(
    "../lib/research-engine-v2/dataforseo-local-match.js"
  );
  const { isForbiddenAutopilotField } = await import(
    "../lib/research-engine-v2/census-autopilot-field-allowlist.js"
  );

  assert.equal(
    DATAFORSEO_LOCAL_VALIDATED_WRITE_OBJECTIVE,
    "dataforseo-local-business-validated-write-v1"
  );
  assert.ok(
    DATAFORSEO_LOCAL_VALIDATED_WRITE_STATUS.COMPLETE.includes(
      "validated_write_v1_complete"
    )
  );

  assert.equal(
    resolveDataForSeoLocalValidatedWriteGates({
      DATAFORSEO_ENABLED: "1",
      DATAFORSEO_WRITE_CANDIDATES_ONLY: "0",
      ENABLE_DATAFORSEO_VALIDATED_WRITES: "1",
      ENABLE_DATAFORSEO_LOCAL_WEBSITE_WRITES: "1",
      ENABLE_DATAFORSEO_LOCAL_ADDRESS_WRITES: "1",
      ENABLE_DATAFORSEO_LOCAL_PHONE_WRITES: "0",
      ENABLE_DATAFORSEO_LOCAL_COORDINATE_WRITES: "0",
      ENABLE_DATAFORSEO_LOCAL_INSERTS: "0",
    }).ok,
    true
  );
  assert.equal(
    resolveDataForSeoLocalValidatedWriteGates({
      DATAFORSEO_ENABLED: "1",
      DATAFORSEO_WRITE_CANDIDATES_ONLY: "0",
      ENABLE_DATAFORSEO_VALIDATED_WRITES: "1",
      ENABLE_DATAFORSEO_LOCAL_WEBSITE_WRITES: "1",
      ENABLE_DATAFORSEO_LOCAL_ADDRESS_WRITES: "1",
      ENABLE_DATAFORSEO_LOCAL_PHONE_WRITES: "1",
      ENABLE_DATAFORSEO_LOCAL_COORDINATE_WRITES: "0",
      ENABLE_DATAFORSEO_LOCAL_INSERTS: "0",
    }).ok,
    false
  );
  assert.equal(
    resolveDataForSeoLocalValidatedWriteGates({
      DATAFORSEO_ENABLED: "1",
      DATAFORSEO_WRITE_CANDIDATES_ONLY: "1",
      ENABLE_DATAFORSEO_VALIDATED_WRITES: "1",
      ENABLE_DATAFORSEO_LOCAL_WEBSITE_WRITES: "1",
      ENABLE_DATAFORSEO_LOCAL_ADDRESS_WRITES: "1",
    }).ok,
    false
  );

  const bookingReject = evaluateLocalWebsiteWrite(
    {
      match_class: MATCH_CLASS.MATCH_HIGH,
      raw: { website: "https://www.booking.com/hotel/mx/test.html" },
    },
    {}
  );
  assert.equal(bookingReject.ok, false);
  assert.equal(bookingReject.reason, "rejected_ota_affiliate_or_directory_host");

  const preserveBrand = evaluateLocalWebsiteWrite(
    {
      match_class: MATCH_CLASS.MATCH_HIGH,
      raw: {
        website:
          "https://www.ihg.com/staybridge/hotels/us/en/puebla/pueaa/hoteldetail",
      },
    },
    {
      "Official Property URL":
        "https://www.ihg.com/staybridge/hotels/us/en/puebla/pueaa/hoteldetail",
    }
  );
  assert.equal(preserveBrand.ok, false);
  assert.ok(
    preserveBrand.reason === "existing_brand_official_url_preserved" ||
      preserveBrand.reason === "website_already_same_host"
  );

  const blankUrlWrite = evaluateLocalWebsiteWrite(
    {
      match_class: MATCH_CLASS.MATCH_HIGH,
      raw: {
        website: "https://www.marriott.com/en-us/hotels/mexca-courtyard/overview/",
      },
    },
    { "Official Property URL": "" }
  );
  assert.equal(blankUrlWrite.ok, true);
  assert.ok(blankUrlWrite.patch["Official Property URL"]);

  const mediumSkip = evaluateLocalAddressWrite(
    {
      match_class: "match_medium",
      raw: {
        address: "Blvd. Hermanos Serdán 810, El Riego Sur, 72160 Puebla",
        title: "Staybridge Suites Puebla",
      },
      hotel_name: "Staybridge Suites Puebla",
    },
    { Address: "" }
  );
  assert.equal(mediumSkip.ok, false);
  assert.equal(mediumSkip.reason, "not_match_high");

  const streetOk = evaluateLocalAddressWrite(
    {
      match_class: MATCH_CLASS.MATCH_HIGH,
      raw: {
        address: "Blvd. Hermanos Serdán 810, El Riego Sur, 72160 Puebla",
        title: "Staybridge Suites Puebla",
        website: "https://www.ihg.com/staybridge/hotels/us/en/puebla/pueaa/hoteldetail",
      },
      hotel_name: "Staybridge Suites Puebla",
    },
    { Address: "", City: "Puebla", Country: "Mexico" }
  );
  assert.equal(streetOk.ok, true);
  assert.equal(streetOk.patch.Address.includes("Hermanos"), true);
  assert.equal(streetOk.patch["Address Confidence"], "Medium");
  assert.equal(streetOk.patch.Phone, undefined);
  assert.equal(streetOk.patch.Latitude, undefined);

  const conflict = evaluateLocalAddressWrite(
    {
      match_class: MATCH_CLASS.MATCH_HIGH,
      raw: {
        address: "Different St 1, Puebla",
        title: "Staybridge Suites Puebla",
      },
      hotel_name: "Staybridge Suites Puebla",
    },
    { Address: "Existing St 99, Puebla" }
  );
  assert.equal(conflict.ok, false);
  assert.equal(conflict.reason, "address_conflict");
  assert.equal(conflict.conflict, true);

  assert.equal(isForbiddenAutopilotField("Phone"), false);
  assert.equal(isForbiddenAutopilotField("Owner Name"), true);
  assert.equal(isForbiddenAutopilotField("Brand Status"), true);

  assert.equal(
    resolveMissionObjective("dataforseo-local-business-validated-write-v1"),
    "dataforseo-local-business-validated-write-v1"
  );
});


test("local-address scale gates + Mapbox eligibility classification", async () => {
  const {
    resolveDataForSeoLocalAddressScaleGates,
    classifyMapboxEligibilityAfterLocalAddress,
    DATAFORSEO_LOCAL_ADDRESS_SCALE_OBJECTIVE,
    DATAFORSEO_LOCAL_ADDRESS_SCALE_STATUS,
    ADDRESS_SCALE_SCHEMA_NOTES,
  } = await import(
    "../lib/research-engine-v2/dataforseo-local-address-scale-v1.js"
  );
  const { isForbiddenAutopilotField } = await import(
    "../lib/research-engine-v2/census-autopilot-field-allowlist.js"
  );

  assert.equal(
    DATAFORSEO_LOCAL_ADDRESS_SCALE_OBJECTIVE,
    "dataforseo-local-address-scale-v1"
  );
  assert.ok(
    DATAFORSEO_LOCAL_ADDRESS_SCALE_STATUS.COMPLETE.includes(
      "address_scale_v1_complete"
    )
  );
  assert.equal(ADDRESS_SCALE_SCHEMA_NOTES.address_source_type_field_exists, false);

  assert.equal(
    resolveDataForSeoLocalAddressScaleGates({
      DATAFORSEO_ENABLED: "1",
      DATAFORSEO_WRITE_CANDIDATES_ONLY: "0",
      ENABLE_DATAFORSEO_VALIDATED_WRITES: "1",
      ENABLE_DATAFORSEO_LOCAL_ADDRESS_WRITES: "1",
      ENABLE_DATAFORSEO_LOCAL_WEBSITE_WRITES: "0",
      ENABLE_DATAFORSEO_LOCAL_PHONE_WRITES: "0",
      ENABLE_DATAFORSEO_LOCAL_COORDINATE_WRITES: "0",
      ENABLE_DATAFORSEO_LOCAL_INSERTS: "0",
    }).ok,
    true
  );
  assert.equal(
    resolveDataForSeoLocalAddressScaleGates({
      DATAFORSEO_ENABLED: "1",
      DATAFORSEO_WRITE_CANDIDATES_ONLY: "0",
      ENABLE_DATAFORSEO_VALIDATED_WRITES: "1",
      ENABLE_DATAFORSEO_LOCAL_ADDRESS_WRITES: "1",
      ENABLE_DATAFORSEO_LOCAL_WEBSITE_WRITES: "1",
      ENABLE_DATAFORSEO_LOCAL_PHONE_WRITES: "0",
      ENABLE_DATAFORSEO_LOCAL_COORDINATE_WRITES: "0",
      ENABLE_DATAFORSEO_LOCAL_INSERTS: "0",
    }).ok,
    true
  );
  assert.equal(
    resolveDataForSeoLocalAddressScaleGates({
      DATAFORSEO_ENABLED: "1",
      DATAFORSEO_WRITE_CANDIDATES_ONLY: "0",
      ENABLE_DATAFORSEO_VALIDATED_WRITES: "1",
      ENABLE_DATAFORSEO_LOCAL_ADDRESS_WRITES: "1",
      ENABLE_DATAFORSEO_LOCAL_WEBSITE_WRITES: "1",
      ENABLE_DATAFORSEO_LOCAL_PHONE_WRITES: "1",
      ENABLE_DATAFORSEO_LOCAL_COORDINATE_WRITES: "0",
      ENABLE_DATAFORSEO_LOCAL_INSERTS: "0",
    }).ok,
    false
  );

  const pending = classifyMapboxEligibilityAfterLocalAddress({
    fields: {
      Address: "Blvd. Hermanos Serdan 810, Puebla",
      "Address Confidence": "Medium",
      "Address Source URL":
        "https://www.ihg.com/staybridge/hotels/us/en/puebla/pueaa/hoteldetail",
    },
    clean_core_pass: true,
    env: { ENABLE_MAPBOX_AFTER_MEDIUM_MATCH_HIGH_ADDRESS: "0" },
  });
  assert.equal(pending.eligible, false);
  assert.equal(pending.status, "mapbox_pending_address_confidence");

  const mediumOk = classifyMapboxEligibilityAfterLocalAddress({
    fields: {
      Address: "Blvd. Hermanos Serdan 810, Puebla",
      "Address Confidence": "Medium",
      "Address Source URL":
        "https://www.ihg.com/staybridge/hotels/us/en/puebla/pueaa/hoteldetail",
    },
    clean_core_pass: true,
    env: { ENABLE_MAPBOX_AFTER_MEDIUM_MATCH_HIGH_ADDRESS: "1" },
  });
  assert.equal(mediumOk.eligible, true);
  assert.equal(mediumOk.status, "mapbox_eligible_medium_match_high");

  const eligibleHigh = classifyMapboxEligibilityAfterLocalAddress({
    fields: {
      Address: "Blvd. Hermanos Serdan 810, Puebla",
      "Address Confidence": "High",
      "Address Source URL":
        "https://www.ihg.com/staybridge/hotels/us/en/puebla/pueaa/hoteldetail",
    },
    clean_core_pass: true,
  });
  assert.equal(eligibleHigh.eligible, true);
  assert.equal(eligibleHigh.status, "mapbox_eligible");

  const mediumApproved = classifyMapboxEligibilityAfterLocalAddress({
    fields: {
      Address: "Blvd. Hermanos Serdan 810, Puebla",
      "Address Confidence": "Medium",
      "Address Source URL":
        "https://www.ihg.com/staybridge/hotels/us/en/puebla/pueaa/hoteldetail",
    },
    clean_core_pass: true,
    geocode_approved_status: "approved",
  });
  assert.equal(mediumApproved.eligible, true);

  assert.equal(isForbiddenAutopilotField("Latitude"), false);
  assert.equal(isForbiddenAutopilotField("Owner Name"), true);
  assert.equal(
    resolveMissionObjective("dataforseo-local-address-scale-v1"),
    "dataforseo-local-address-scale-v1"
  );
});


test("local-address scale query attempts strip accents + country fallback", async () => {
  const { resolveDataForSeoLocationName, stripDiacritics } = await import(
    "../lib/research-engine-v2/dataforseo-client.js"
  );
  const { buildLocalAddressScaleQueryAttempts } = await import(
    "../lib/research-engine-v2/dataforseo-local-address-scale-v1.js"
  );

  assert.equal(stripDiacritics("Bogotá"), "Bogota");
  assert.equal(
    resolveDataForSeoLocationName({ city: "Bogotá", country: "Colombia" }),
    "Bogota,Colombia"
  );
  const attempts = buildLocalAddressScaleQueryAttempts({
    "Canonical Property Name": "JW Marriott Hotel Bogota",
    City: "Bogotá",
    Country: "Colombia",
    "State / Region": "Cundinamarca",
  });
  assert.ok(attempts.length >= 3);
  assert.equal(attempts[0].location_name, "Bogota,Colombia");
  assert.ok(attempts.some((a) => a.attempt === "country_only_fallback"));
});


test("policy controller gates + insert policy + no founder gates", async () => {
  const {
    resolveCensusAutopilotPolicyGates,
    assertHighConfidenceInsertPolicy,
    classifyPhoneUnderAutopilotPolicy,
    classifyDirectLocalCoordinatesUnderPolicy,
    NEVER_WRITE_FIELDS,
  } = await import(
    "../lib/research-engine-v2/census-autopilot-approved-policy.js"
  );
  const { resolveMissionObjective } = await import(
    "../lib/research-engine-v2/census-autopilot-mission.js"
  );
  const { evaluateCoordinateCompletionEligibility } = await import(
    "../lib/research-engine-v2/census-coordinate-completion.js"
  );
  const { CENSUS_MODE, assertNoInsertInFieldCompletionMode } = await import(
    "../lib/research-engine-v2/census-autopilot-full-latam-v3.js"
  );
  const { isForbiddenAutopilotField } = await import(
    "../lib/research-engine-v2/census-autopilot-field-allowlist.js"
  );

  assert.equal(
    resolveMissionObjective("census-autopilot-policy-controller-v1"),
    "census-autopilot-policy-controller-v1"
  );

  const gates = resolveCensusAutopilotPolicyGates({
    ENABLE_CENSUS_POLICY_CONTROLLER: "1",
    ENABLE_DATAFORSEO_LOCAL_ADDRESS_WRITES: "1",
    ENABLE_DATAFORSEO_LOCAL_WEBSITE_WRITES: "1",
    ENABLE_DATAFORSEO_LOCAL_PHONE_WRITES: "0",
    ENABLE_DATAFORSEO_LOCAL_COORDINATE_WRITES: "0",
    ENABLE_MAPBOX_AFTER_VALIDATED_ADDRESS: "1",
    ENABLE_SECONDARY_ROOMS_SOURCES: "1",
    ENABLE_SECONDARY_PHONE_SOURCES: "0",
    ENABLE_DATAFORSEO_LOCAL_INSERTS: "0",
    ENABLE_HIGH_CONFIDENCE_INSERTS: "0",
  });
  assert.equal(gates.ok, true);
  assert.equal(gates.founder_gate_between_passes, false);
  assert.equal(gates.phone_writes, false);
  assert.equal(gates.inserts_enabled, false);

  assert.equal(
    assertHighConfidenceInsertPolicy({
      censusMode: "field-completion-only",
      gates,
    }).allowed,
    false
  );
  assert.equal(
    assertHighConfidenceInsertPolicy({
      censusMode: "growth",
      gates: { ...gates, inserts_enabled: false },
    }).reason,
    "high_confidence_inserts_require_explicit_flags"
  );
  assert.equal(
    assertNoInsertInFieldCompletionMode(CENSUS_MODE.FIELD_COMPLETION_ONLY, 1)
      .ok,
    false
  );

  assert.equal(classifyPhoneUnderAutopilotPolicy({}).write, false);
  assert.equal(classifyDirectLocalCoordinatesUnderPolicy().write, false);

  const mediumMapbox = evaluateCoordinateCompletionEligibility(
    {
      id: "recX",
      fields: {
        Address: "Blvd. Hermanos Serdan 810, Puebla",
        "Address Confidence": "Medium",
        "Address Source URL": "https://www.ihg.com/x",
        "Official Property URL": "https://www.ihg.com/x",
        "Source URL": "https://www.ihg.com/x",
        City: "Puebla",
        Country: "Mexico",
        "Canonical Property Name": "Staybridge Suites Puebla",
        "Property Name": "Staybridge Suites Puebla",
        "Current Brand": "Staybridge Suites",
        "Property Identity Key": "test-key",
      },
    },
    {
      allowMediumAddressWithProvenance: true,
      mediumMatchHighPathway: true,
      env: { ENABLE_MAPBOX_AFTER_MEDIUM_MATCH_HIGH_ADDRESS: "1" },
    }
  );
  assert.notEqual(mediumMapbox.reason, "address_confidence_not_high");
  assert.equal(mediumMapbox.eligible, true);
  assert.equal(mediumMapbox.from_medium_address, true);

  assert.equal(isForbiddenAutopilotField("Owner Name"), true);
  assert.equal(isForbiddenAutopilotField("Brand Status"), true);
  assert.ok(NEVER_WRITE_FIELDS.includes("Company Validated"));
});

console.log("All dataforseo-discovery-pilot unit tests passed.");