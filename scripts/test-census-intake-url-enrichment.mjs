/**
 * Unit tests — Google Places hotel URL lookup + known-chain enrichment (mocked).
 */
import test from "node:test";
import assert from "node:assert/strict";
import {
  classifyGoogleWebsiteProposal,
  lookupHotelOfficialUrlWithGoogle,
  normalizeHotelPlaceFromSearch,
} from "../lib/independent-census/google-places-hotel-url-lookup.js";
import {
  enrichKnownChainOfficialUrl,
  resolveBrandOfficialHosts,
  runKnownChainOfficialUrlEnrichmentBatch,
  simulateIntakeGateWithProposedUrl,
} from "../lib/independent-census/known-chain-official-url-enrichment.js";

test("classifyGoogleWebsiteProposal denylists google maps / booking", () => {
  const conf = { matchConfidence: "High", verificationStatus: "Verified" };
  assert.equal(
    classifyGoogleWebsiteProposal("https://maps.google.com/?cid=1", conf)
      .propose_as_official_url,
    false
  );
  assert.equal(
    classifyGoogleWebsiteProposal("https://www.booking.com/hotel/x.html", conf)
      .propose_as_official_url,
    false
  );
  assert.equal(
    classifyGoogleWebsiteProposal("https://www.riu.com/en/hotel/do/punta-cana", conf)
      .propose_as_official_url,
    true
  );
});

test("lookupHotelOfficialUrlWithGoogle uses mocked search", async () => {
  const place = normalizeHotelPlaceFromSearch({
    id: "places/ChIJ_test",
    displayName: { text: "RIU Palace Punta Cana" },
    formattedAddress: "Punta Cana, Dominican Republic",
    location: { latitude: 18.68, longitude: -68.41 },
    websiteUri: "https://www.riu.com/en/hotel/do/riu-palace-punta-cana",
    types: ["lodging", "hotel"],
    businessStatus: "OPERATIONAL",
    googleMapsUri: "https://maps.google.com/?cid=1",
  });
  const result = await lookupHotelOfficialUrlWithGoogle(
    {
      source_record_id: "node/1",
      property_name: "RIU Palace Punta Cana",
      current_brand: "RIU",
      city: "Punta Cana",
      payload: {
        Country: "Dominican Republic",
        Latitude: 18.68,
        Longitude: -68.41,
      },
    },
    {
      searchTextFn: async () => [place],
    }
  );
  assert.equal(result.status, "matched");
  assert.ok(result.suggested_official_property_url.includes("riu.com"));
  assert.equal(result.website_proposal.propose_as_official_url, true);
});

test("resolveBrandOfficialHosts maps RIU / Be Live / Autograph", () => {
  assert.ok(resolveBrandOfficialHosts("RIU").includes("riu.com"));
  assert.ok(resolveBrandOfficialHosts("Be Live").includes("belivehotels.com"));
  assert.ok(
    resolveBrandOfficialHosts("Autograph Collection").includes("marriott.com")
  );
});

test("sanitizeOfficialUrlCandidate strips utm params", async () => {
  const { sanitizeOfficialUrlCandidate } = await import(
    "../lib/independent-census/known-chain-official-url-enrichment.js"
  );
  assert.equal(
    sanitizeOfficialUrlCandidate(
      "https://www.riu.com/en/hotel/x?utm_source=google&utm_medium=organic&keep=1"
    ),
    "https://www.riu.com/en/hotel/x?keep=1"
  );
});

test("enrichKnownChainOfficialUrl prefers brand-domain Google website", () => {
  const row = {
    source_record_id: "node/2",
    property_name: "Catalonia Punta Cana",
    current_brand: "Catalonia",
    city: "Unknown",
    decision: "steward_hold",
    intake_class: "known_chain_census_backlog_not_active_setup",
    lane: "known_brand_census_intake",
    hpc_recommended_action: "likely_new_candidate",
    quality_score: 55,
    payload: {
      "Property Name": "Catalonia Punta Cana",
      Country: "Dominican Republic",
      City: "Unknown",
      "Official Property URL": "",
      "Current Brand": "Catalonia",
      "Affiliation Status": "Branded",
    },
  };
  const google = {
    status: "matched",
    match_confidence: "High",
    suggested_official_property_url:
      "https://www.cataloniahotels.com/en/hotel/catalonia-punta-cana",
    place: {
      google_place_id: "places/x",
      google_website_uri:
        "https://www.cataloniahotels.com/en/hotel/catalonia-punta-cana",
    },
    website_proposal: {
      propose_as_official_url: true,
      requires_steward: false,
      host: "www.cataloniahotels.com",
    },
  };
  const e = enrichKnownChainOfficialUrl(row, google);
  assert.equal(e.apply_as_official_url_candidate, true);
  assert.equal(e.proposal_source, "google_places_website_on_brand_domain");
  assert.equal(e.proposal_confidence, "high");

  const sim = simulateIntakeGateWithProposedUrl(row, e);
  assert.equal(sim.simulated, true);
  assert.equal(sim.new_decision, "auto_insert");
});

test("name corroboration lifts Low Google match on brand-domain property URL", async () => {
  const { propertyUrlNameCorroborates, sanitizeOfficialUrlCandidate } =
    await import("../lib/independent-census/known-chain-official-url-enrichment.js");
  assert.equal(
    propertyUrlNameCorroborates(
      "Grand Bahia Principe Turquesa",
      "https://www.bahia-principe.com/en/resorts-in-dominican-republic/resort-turquesa/",
      "Unknown"
    ),
    true
  );
  assert.equal(
    propertyUrlNameCorroborates(
      "Barceló Puerto Plata",
      "https://www.barcelo.com/en-us/barcelo-santo-domingo/",
      "Unknown"
    ),
    false
  );
  assert.equal(
    propertyUrlNameCorroborates(
      "Sensimar Punta Cana Villas & Suites",
      "https://www.riu.com/en/hotel/dominican-republic/punta-cana/hotel-riu-palace-punta-cana",
      "Unknown"
    ),
    false
  );
  assert.equal(
    propertyUrlNameCorroborates(
      "Dreams Punta Cana Resort & Spa",
      "https://www.hyattinclusivecollection.com/en/resorts-hotels/dreams/dominican-republic/cap-cana-resort-spa/",
      "Unknown"
    ),
    false
  );
  assert.ok(
    sanitizeOfficialUrlCandidate(
      "https://a2.adform.net/C/?bn=1;cpdir=https://www.excellenceresorts.com/punta-cana/excellence-punta-cana/?utm_source=x"
    ).includes("excellenceresorts.com/punta-cana")
  );

  const row = {
    source_record_id: "way/471926026",
    property_name: "Grand Bahia Principe Turquesa",
    current_brand: "Bahía Príncipe",
    city: "Unknown",
    decision: "steward_hold",
    intake_class: "known_chain_census_backlog_not_active_setup",
    lane: "known_brand_census_intake",
    hpc_recommended_action: "likely_new_candidate",
    quality_score: 60,
    payload: {
      "Property Name": "Grand Bahia Principe Turquesa",
      Country: "Dominican Republic",
      City: "Unknown",
      "Official Property URL": "",
      "Current Brand": "Bahía Príncipe",
      "Affiliation Status": "Branded",
    },
  };
  const google = {
    status: "matched",
    match_confidence: "Low",
    place: {
      google_website_uri:
        "https://www.bahia-principe.com/en/resorts-in-dominican-republic/resort-turquesa/?utm_source=google",
    },
    website_proposal: {
      propose_as_official_url: false,
      reason: "match_confidence_too_low",
    },
  };
  const e = enrichKnownChainOfficialUrl(row, google);
  assert.equal(e.proposal_confidence, "high");
  assert.equal(e.apply_as_official_url_candidate, true);
  assert.match(e.proposed_official_property_url, /resort-turquesa/);
});

test("batch enrichment counts lift", () => {
  const rows = [
    {
      source_record_id: "node/3",
      property_name: "Barceló Bávaro Palace",
      current_brand: "Barceló",
      decision: "steward_hold",
      intake_class: "known_chain_census_backlog_not_active_setup",
      lane: "known_brand_census_intake",
      hpc_recommended_action: "likely_new_candidate",
      quality_score: 70,
      payload: {
        "Property Name": "Barceló Bávaro Palace",
        Country: "Dominican Republic",
        City: "Punta Cana",
        "Official Property URL": "",
        "Current Brand": "Barceló",
        "Affiliation Status": "Branded",
      },
    },
  ];
  const googleById = new Map([
    [
      "node/3",
      {
        status: "matched",
        match_confidence: "High",
        suggested_official_property_url:
          "https://www.barcelo.com/en-us/hotels/punta-cana/barcelo-bavaro-palace/",
        place: {
          google_website_uri:
            "https://www.barcelo.com/en-us/hotels/punta-cana/barcelo-bavaro-palace/",
        },
        website_proposal: {
          propose_as_official_url: true,
          requires_steward: false,
        },
      },
    ],
  ]);
  const batch = runKnownChainOfficialUrlEnrichmentBatch(rows, googleById);
  assert.equal(batch.proposed_official_url_count, 1);
  assert.ok(batch.simulated_auto_insert_lift >= 1);
});
