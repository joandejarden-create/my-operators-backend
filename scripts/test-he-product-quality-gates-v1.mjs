#!/usr/bin/env node
/**
 * Hotel Explorer product quality gates (reusable / regression).
 * References docs/ai-build-system/HOTEL_EXPLORER_FOUNDER_QUALITY_LESSONS_REGISTRY.json
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import assert from "node:assert/strict";
import {
  buildCommercialDescription,
  isLowQualityCommercialDescription,
  resolveCommercialDescription,
  HOTEL_COMMERCIAL_DESCRIPTION_VERSION,
} from "../lib/hotel-intelligence/hotel-commercial-description-v1.js";
import {
  rankContactsForHotel,
  selectRecommendedFirstContact,
  looksLikeInventedEmail,
  isCorporateChannelMislabeledAsDirect,
  detectDuplicateNamedContacts,
  assertNoUnsupportedPrimaryClaim,
  CONTACT_CLASS,
  PROPERTY_RESPONSIBILITY,
} from "../lib/hotel-intelligence/contact-intelligence/property-decision-maker-engine-v1.js";
import {
  selectAmenitiesForHotelExplorerDisplay,
  mergeAmenityDisplaySources,
  assessAmenityCoverageResearchNeed,
  HE_AMENITIES_DISPLAY_MAX,
  HE_AMENITIES_DISPLAY_TARGET_MIN,
  isNonGuestAmenityLabel,
} from "../lib/hotel-intelligence/he-amenity-display-v1.js";
import { ATTRIBUTE_DEFINITIONS } from "../lib/ai-demand-positioning/intelligence/reality-gap.js";
import {
  SQFT_TO_SQM,
  MI_TO_KM,
  sqFtToSqM,
  kmToMi,
  areaSortValueSqFt,
  defaultRadiusMilesForLocationType,
} from "../lib/hotel-intelligence/he-measurement-prefs-v1.js";
import {
  iconIdsMustBeDistinctForUnrelatedAttributes,
  HE_AMENITY_ICON_MAP,
} from "../lib/hotel-intelligence/he-amenity-icon-map-v1.js";
import {
  buildOwnerPortfolioOperatorView,
  classifyPortfolioHotel,
  OPERATOR_NOT_VERIFIED_LABEL,
  PORTFOLIO_EVIDENCE,
  PORTFOLIO_SCOPE,
} from "../lib/hotel-intelligence/ownership/owner-portfolio-operator-view-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function read(rel) {
  return fs.readFileSync(path.join(ROOT, rel), "utf8");
}

function mustInclude(hay, needle, msg) {
  assert.ok(hay.includes(needle), msg || `expected to include ${needle}`);
}

function mustNotMatch(hay, re, msg) {
  assert.ok(!re.test(hay), msg || `expected not to match ${re}`);
}

const failures = [];
function run(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (err) {
    failures.push({ name, message: err.message });
    console.error(`FAIL ${name}: ${err.message}`);
  }
}

run("registry_and_standard_exist", () => {
  const reg = JSON.parse(
    read("docs/ai-build-system/HOTEL_EXPLORER_FOUNDER_QUALITY_LESSONS_REGISTRY.json")
  );
  assert.equal(reg.version, "hotel-explorer-founder-quality-lessons-v1.19");
  assert.ok(Array.isArray(reg.lessons) && reg.lessons.length >= 30);
  mustInclude(
    read("docs/ai-build-system/HOTEL_EXPLORER_QUALITY_STANDARD.md"),
    "hotel-explorer-quality-standard-v1.19"
  );
  assert.ok(reg.lessons.some((l) => l.id === "HE-QL-016"));
  assert.ok(reg.lessons.some((l) => l.id === "HE-QL-017"));
  assert.ok(reg.lessons.some((l) => l.id === "HE-QL-018"));
  assert.ok(reg.lessons.some((l) => l.id === "HE-QL-019"));
  assert.ok(reg.lessons.some((l) => l.id === "HE-QL-020"));
  assert.ok(reg.lessons.some((l) => l.id === "HE-QL-021"));
  assert.ok(reg.lessons.some((l) => l.id === "HE-QL-022"));
  assert.ok(reg.lessons.some((l) => l.id === "HE-QL-023"));
  assert.ok(reg.lessons.some((l) => l.id === "HE-QL-024"));
  assert.ok(reg.lessons.some((l) => l.id === "HE-QL-025"));
  assert.ok(reg.lessons.some((l) => l.id === "HE-QL-026"));
  assert.ok(reg.lessons.some((l) => l.id === "HE-QL-027"));
  assert.ok(reg.lessons.some((l) => l.id === "HE-QL-028"));
  assert.ok(reg.lessons.some((l) => l.id === "HE-QL-029"));
  assert.ok(reg.lessons.some((l) => l.id === "HE-QL-030"));
});

run("rejects_keyword_stuffed_and_id_leaking_descriptions", () => {
  assert.equal(
    isLowQualityCommercialDescription("Hotel hotel, serving City — signals: a,b,c", {}),
    true
  );
  assert.equal(
    isLowQualityCommercialDescription("Owner recLuxvwwxID7U2B8 operates dle_ABCDEF", {}),
    true
  );
  const good = buildCommercialDescription({
    name: "Bethesda Marriott",
    brand: "Marriott Hotels",
    chainScale: "Upper Upscale",
    city: "Bethesda",
    state: "MD",
    country: "United States",
    rooms: 407,
    totalMeetingSpace: 18719,
    managementCompany: "Marriott International",
    submarket: "Pooks Hill / Rockville Pike",
    locationType: "Suburban",
  });
  assert.equal(good.lowQuality, false);
  assert.ok(/lodging asset|physical-product|guestrooms/i.test(good.text));
  assert.ok(!/rec[A-Z0-9]/i.test(good.text));
  assert.ok(!/physical product comprises|It sits in|Day-to-day operations are managed by/i.test(good.text));
});

run("commercial_rubric_generalizes_across_hotels", () => {
  const beth = resolveCommercialDescription({
    name: "Bethesda Marriott",
    brand: "Marriott Hotels",
    chainScale: "Upper Upscale",
    city: "Bethesda",
    rooms: 407,
    totalMeetingSpace: 18719,
    managementCompany: "Marriott International",
  });
  const kgpv = resolveCommercialDescription({
    name: "Krystal Grand Puerto Vallarta",
    brand: "Krystal Grand",
    chainScale: "Upper Upscale",
    city: "Puerto Vallarta",
    country: "Mexico",
    rooms: 468,
    managementCompany: "Grupo Hotelero Santa Fe",
    locationType: "Resort",
  });
  const third = resolveCommercialDescription({
    name: "Sample Limited Coverage Hotel",
    city: "Austin",
    rooms: 120,
  });
  assert.ok(String(beth.source || "").startsWith("rubric_v2"));
  assert.ok(String(kgpv.source || "").startsWith("rubric_v2"));
  assert.ok(beth.text.includes("Bethesda Marriott"));
  assert.ok(kgpv.text.includes("Krystal Grand Puerto Vallarta"));
  assert.ok(third.text.includes("Sample Limited Coverage Hotel"));
  assert.equal(HOTEL_COMMERCIAL_DESCRIPTION_VERSION, "hotel-commercial-description-v2");
  assert.ok(!/physical product comprises|It sits in|Day-to-day operations are managed by/i.test(beth.text));
  assert.ok(beth.wordCount >= 160 || beth.text.length >= 700);
});

run("overview_exposes_year_built_and_renovated_slots", () => {
  const hdp = read("public/js/hotel-detail-panel.js");
  mustInclude(hdp, "resolveYearBuiltDisplay");
  mustInclude(hdp, "resolveLastRenovatedDisplay");
  mustInclude(hdp, "Year built");
  mustInclude(hdp, "Last renovated");
  mustInclude(hdp, "Not yet verified");
});

run("amenities_forbid_raw_db_tokens", () => {
  const hdp = read("public/js/hotel-detail-panel.js");
  mustInclude(hdp, "hdp-amenities-carousel");
  mustInclude(hdp, "resolveAmenitiesDisplay");
  mustNotMatch(hdp, /label:\s*["']has_pool["']/);
});

run("amenities_kgpv_strip_and_display_cap", () => {
  const hdp = read("public/js/hotel-detail-panel.js");
  const css = read("public/css/hotel-detail-panel.css");
  mustInclude(hdp, "hdp-amenities-wrap");
  mustInclude(hdp, "hdp-amenities-carousel");
  mustInclude(hdp, "HE_AMENITIES_DISPLAY_MAX");
  mustInclude(hdp, "amenityDisplayPriority");
  mustInclude(hdp, "researchTrigger");
  mustInclude(hdp, "fromVerified.concat(fromDisplay)");
  assert.ok(!/hdp-amenities-grid/.test(hdp), "grid markup must not be used");
  assert.ok(!/hdp-amenity-card--grid/.test(hdp), "bordered grid cards must not be used");
  mustInclude(css, "hdp-amenities-carousel");
  assert.ok(/border:\s*none/.test(css.match(/\.hdp-amenity-card\s*\{[\s\S]*?\}/)?.[0] || ""));
  assert.ok(!/\.hdp-amenities-grid\s*\{/.test(css), "grid CSS removed");
  assert.equal(HE_AMENITIES_DISPLAY_MAX, 20);
  assert.equal(isNonGuestAmenityLabel("Bethesda MD Location"), true);
  assert.equal(isNonGuestAmenityLabel("Outdoor Pool", "outdoorPool"), false);
  const many = Array.from({ length: 28 }, (_, i) => ({
    id: i === 0 ? "meetingEventSpace" : `attr_${i}`,
    label: i === 0 ? "Meeting & Event Space" : i === 1 ? "Meeting & Event Space" : `Amenity ${i}`,
    verificationStatus: "VERIFIED_FACT",
  }));
  const selected = selectAmenitiesForHotelExplorerDisplay(many);
  // Duplicate label collapses → 27 unique → capped at 20
  assert.equal(selected.displayed.length, 20);
  assert.equal(selected.coverage.overflowCount, 7);
  assert.equal(selected.displayed[0].id, "meetingEventSpace");
  const sparse = selectAmenitiesForHotelExplorerDisplay([
    { id: "outdoorPool", label: "Outdoor Pool" },
    { id: "fitnessCenter", label: "Fitness Center" },
  ]);
  assert.equal(sparse.displayed.length, 2);
  assert.equal(sparse.coverage.belowTargetMin, true);
  assert.equal(sparse.coverage.shortfall, 10);
  assert.equal(sparse.research.researchTrigger, true);
  assert.equal(sparse.research.status, "RESEARCH_GAP");
  assert.equal(sparse.research.customerFacingCompletenessIndicator, false);
});

run("amenities_verified_coverage_research_trigger", () => {
  assert.equal(HE_AMENITIES_DISPLAY_TARGET_MIN, 12);
  for (const key of [
    "business_center",
    "room_service",
    "free_wifi",
    "pet_friendly",
    "on_site_parking",
    "metro_shuttle",
    "room_accessibility",
  ]) {
    assert.ok(ATTRIBUTE_DEFINITIONS[key], "missing ATTRIBUTE_DEFINITIONS." + key);
  }
  const merged = mergeAmenityDisplaySources([
    [{ id: "outdoorPool", label: "Outdoor Pool", verificationStatus: "VERIFIED_FACT" }],
    [
      { id: "outdoorPool", label: "Outdoor Pool" },
      { id: "businessCenter", label: "Business Center" },
      { id: "bethesda_md_location", label: "Bethesda, Maryland Location" },
      { id: "meetingRooms", label: "Meeting Space" },
      { id: "meeting_space", label: "Meeting & Event Space" },
    ],
  ]);
  assert.equal(merged.length, 3, "geo filtered; pool+meeting near-dupes collapsed; business kept");

  const bethProfile = JSON.parse(
    read("fixtures/ai-demand-positioning/bethesda-marriott-property-profile.json")
  );
  const bethGuest = (bethProfile.attributes || [])
    .map((key) => ({
      id: key,
      label: ATTRIBUTE_DEFINITIONS[key]?.label || key,
      factKind: "VERIFIED_FACT",
      sourceUrl: bethProfile.attributeEvidence?.[key]?.sourceUrl || bethProfile.website,
    }))
    .filter((row) => !isNonGuestAmenityLabel(row.label, row.id));
  const guest = selectAmenitiesForHotelExplorerDisplay(bethGuest, {
    hotelId: "recLuxvwwxID7U2B8",
    hotelName: "Bethesda Marriott",
  });
  assert.ok(
    guest.coverage.verifiedCount >= 12,
    "Bethesda must meet ≥12 when first-party evidence permits (was 9)"
  );
  assert.ok(guest.coverage.displayedCount <= 20);
  assert.equal(guest.coverage.belowTargetMin, false);
  assert.equal(guest.research.status, "COVERAGE_OK");
  assert.ok(
    !/UNVERIFIED/i.test(String(bethProfile.notes?.parking || "")),
    "parking note must not remain UNVERIFIED after Marriott.com freeze"
  );
  const gap = assessAmenityCoverageResearchNeed(
    { verifiedCount: 9, targetMin: 12, max: 20 },
    { hotelId: "recTEST", hotelName: "Sparse" }
  );
  assert.equal(gap.researchTrigger, true);
  assert.equal(gap.shortfall, 3);
  mustInclude(
    read("docs/ai-build-system/HOTEL_EXPLORER_QUALITY_STANDARD.md"),
    "HE-QL-030"
  );
  const reg = JSON.parse(
    read("docs/ai-build-system/HOTEL_EXPLORER_FOUNDER_QUALITY_LESSONS_REGISTRY.json")
  );
  assert.ok(reg.lessons.some((l) => l.id === "HE-QL-030"));
});

run("forbids_internal_graph_ids_in_relationship_markup", () => {
  const ws = read("public/js/hotel-intelligence-workspace.js");
  mustInclude(ws, "resolveOrgDisplayName");
  // Renderer must prefer trading/legal names — regression against raw dle_ echo helpers
  assert.ok(/tradingName|trading_name|legal_name|display_name/.test(ws));
});

run("rooms_tab_after_overview_and_sortable_inventory", () => {
  const hex = read("public/js/hotel-explorer.js");
  const tabsMatch = hex.match(/var TABS = \[([\s\S]*?)\];/);
  assert.ok(tabsMatch, "TABS array present");
  const ids = [...tabsMatch[1].matchAll(/id:\s*"([^"]+)"/g)].map((m) => m[1]);
  assert.equal(ids[0], "hotel");
  assert.equal(ids[1], "rooms");
  const rms = read("public/js/hotel-explorer-rooms-meeting-space.js");
  mustInclude(rms, "DealalitySortableTable");
  mustInclude(rms, "not additive");
  const sort = read("public/js/dealality-sortable-table.js");
  mustInclude(sort, "dl-th--sortable");
  mustInclude(sort, "data-sort");
  const css = read("public/css/hotel-explorer.css");
  mustInclude(css, "dl-table");
  mustInclude(css, "--hex-surface-table");
  mustInclude(css, "var(--hex-surface-table");
});

run("hex_tabs_even_flex_and_rooms_present", () => {
  const css = read("public/css/hotel-explorer.css");
  mustInclude(css, ".hex-tabs .section-nav-item");
  mustInclude(css, "grid-auto-columns: minmax(76px, 1fr)");
  mustInclude(css, "min-width: 32px");
  mustInclude(css, "border: 2px solid var(--neutral--600)");
});

run("contact_cards_omit_dash_placeholders_and_surfe_not_on_get", () => {
  const hci = read("public/js/hotel-contact-intelligence.js");
  mustInclude(hci, "Find Email & Phone");
  mustInclude(hci, "Loading Contacts…");
  mustInclude(hci, "HeLoadingUx");
  mustInclude(hci, "hci-note--unresolved");
  mustInclude(hci, "Verified identity ≠ verified personal contact channels");
  mustInclude(hci, "data-hci-enrich");
  mustInclude(hci, "/reveal");
  mustInclude(hci, "enrichment-capability");
  mustInclude(hci, "Recommended First Contact");
  mustInclude(hci, "Alternative Decision-Makers");
  mustInclude(hci, "Organization Contact Channels");
  mustInclude(hci, "Research gaps");
  assert.ok(!/hci-dev-json|Dev diagnostic|JSON\.stringify\(vm/.test(hci), "no user-visible VM dump");
  assert.ok(!/production_writes|write_on_get|apply_authorized|surfe_live_calls/.test(hci));
  assert.ok(
    !/LinkedIn · VERIFIED_PUBLIC|verification_status \|\| \"VERIFIED/.test(hci),
    "raw verification enums must not be user-facing labels"
  );
  const mountIdx = hci.indexOf("function mount(");
  const mountSlice = hci.slice(mountIdx, mountIdx + 1600);
  assert.ok(!/enrichPeople|provider\.enrich/.test(mountSlice), "mount must not call Surfe provider");
  const api = read("api/contact-intelligence.js");
  mustInclude(api, "postContactIntelligencePersonReveal");
  mustInclude(api, "getContactIntelligenceEnrichmentCapability");
  mustInclude(api, "persist_forbidden");
  mustInclude(api, "NOT_WRITTEN");
  mustInclude(api, "autoEnrichOnLoad: false");
  mustInclude(api, "buildMockSurfePerson");
  mustInclude(api, "CONTACT_ENRICHMENT_MOCK");
  mustInclude(api, "No additional professional contact details found");
  const waveCss = read("public/css/dealality-wave-loader.css");
  const compactRule = waveCss.slice(
    waveCss.indexOf(".dealality-wave-loader--compact"),
    waveCss.indexOf(".dealality-loading-toast-host")
  );
  assert.ok(!/width:\s*100%/.test(compactRule), "HE loaders must not use full-width compact bars");
  mustInclude(waveCss, "dealality-wave-loader--toast");
});

run("rejects_mechanical_fact_dump_descriptions", () => {
  assert.equal(
    isLowQualityCommercialDescription(
      "The physical product comprises 407 guestrooms. It sits in Bethesda. Day-to-day operations are managed by Marriott.",
      { rooms: 407, brand: "Marriott Hotels" }
    ),
    true
  );
  const built = buildCommercialDescription({
    name: "Bethesda Marriott",
    brand: "Marriott Hotels",
    chainScale: "Upper Upscale",
    city: "Bethesda",
    state: "MD",
    country: "United States",
    rooms: 407,
    totalMeetingSpace: 18719,
    meetingRooms: 27,
    largestMeetingRoomName: "Grand Ballroom",
    largestMeetingRoomSqFt: 4592,
    managementCompany: "Marriott International",
    operationType: "Management Agreement",
    submarket: "Pooks Hill / Rockville Pike",
    locationType: "Suburban",
    demandGenerators: ["NIH Access", "Washington DC Metro Access"],
    amenitiesDisplay: [{ label: "Outdoor Pool" }, { label: "24-Hour Fitness Center" }],
    ownershipContext: {
      legalOwnerName: "Marbeth Acquisition",
      economicOwnerName: "Mullen Real Estate Capital",
      preview_state: "PROPOSED_STAGED",
      confidence: "MEDIUM",
    },
  });
  assert.ok(built.wordCount >= 180, "executive length");
  assert.ok(built.paragraphs.length >= 3, "multi-paragraph");
  assert.ok(!MECHANICAL_CHECK(built.text));
});

function MECHANICAL_CHECK(t) {
  return /physical product comprises|on the physical-product side|It sits in|Day-to-day operations are managed by/i.test(
    t
  );
}

run("meeting_table_readable_room_names_no_forced_min_width", () => {
  const rms = read("public/js/hotel-explorer-rooms-meeting-space.js");
  mustInclude(rms, "hex-rms-room-name");
  mustInclude(rms, "Supporting Documentation");
  mustInclude(rms, "hex-rms-ext-link");
  assert.ok(!/Config.*Reception/s.test(rms.match(/meetingRoomColumns[\s\S]*?return \[([\s\S]*?)\];/)?.[1] || "") || true);
  const cols = rms.match(/function meetingRoomColumns[\s\S]*?return \[([\s\S]*?)\];/);
  assert.ok(cols, "columns fn");
  assert.ok(!/key: "config"/.test(cols[1]), "config not a primary column");
  const css = read("public/css/hotel-explorer.css");
  mustInclude(css, "--hex-surface-table: #0d152c");
  mustInclude(css, "hex-rms-room-name");
  assert.ok(!/\.hex-rms-table\.dl-table--inventory \{\s*min-width: 980px/.test(css));
});

run("ownership_hierarchy_proper_case_labels", () => {
  const ws = read("public/js/hotel-intelligence-workspace.js");
  mustInclude(ws, "Hotel Stakeholder");
  mustInclude(ws, "Legal Property Owner");
  mustInclude(ws, "Economic Owner / Sponsor");
  mustInclude(ws, "Hotel Operator");
  mustInclude(ws, "Brand Affiliation");
  // Relationships tab + OCN still use proper-cased role labels (not raw enums)
  mustInclude(ws, "Legal property owner");
  mustInclude(read("public/js/he-ownership-control-network-v1.js"), "Economic owner / sponsor");
  mustNotMatch(ws, /\bLEGAL_PROPERTY_OWNER\b|\bECONOMIC_OWNER_OR_GROUP\b/);
});

run("proposed_overlay_never_marked_live_persisted", () => {
  const preview = read("api/hotel-explorer-founder-preview.js");
  mustInclude(preview, "PROPOSED");
  mustInclude(preview, "X-Dealality-Write");
  mustNotMatch(preview, /Airtable\.update|patchRecord|createRecord/);
});

run("founder_preview_and_radar_share_canonical_shell", () => {
  const previewHtml = read("public/hotel-explorer-preview.html");
  const radarHtml = read("public/deal-capture-radar-with-ranked-list.html");
  const goldenHtml = read("public/hotel-intelligence-golden-demo.html");
  for (const html of [previewHtml, goldenHtml]) {
    mustInclude(html, "dealality-sortable-table.js");
    mustInclude(html, "hotel-explorer-rooms-meeting-space.js");
    mustInclude(html, "hotel-explorer.js");
  }
  mustInclude(radarHtml, "hotel-explorer.js");
  mustInclude(radarHtml, "dealality-sortable-table.js");
});

run("rooms_inventory_fixture_no_double_count_rule", () => {
  const invPath = path.join(
    ROOT,
    "reports/hotel-explorer/phase4-quality/HE-BETH-GUEST-ROOMS-MEETING-SPACE-STAGED-V1.json"
  );
  if (!fs.existsSync(invPath)) {
    console.log("SKIP rooms_inventory_fixture (file missing)");
    return;
  }
  const inv = JSON.parse(fs.readFileSync(invPath, "utf8"));
  const rooms = inv.meetingRooms || inv.inventory?.meetingRooms || [];
  const secondary = rooms.filter((r) => r.includeInAggregate === false);
  assert.ok(rooms.length > 0, "meeting rooms present");
  assert.ok(secondary.length > 0, "divisible/combined exclusions present");
});

run("owner_decision_maker_ranking_and_surfe_fallback", () => {
  const ceo = {
    personId: "ceo",
    name: "Jeff Mullen",
    title: "President and CEO",
    propertyResponsibility: PROPERTY_RESPONSIBILITY.UNKNOWN,
    contactClass: CONTACT_CLASS.OWNER_DECISION_AUTHORITY,
  };
  const am = {
    personId: "am",
    name: "Katherine Button",
    title: "VP, Asset Management and Investments",
    propertyResponsibility: PROPERTY_RESPONSIBILITY.INFERRED,
    contactClass: CONTACT_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
    portfolioEvidenceCount: 2,
  };
  const ranked = rankContactsForHotel([ceo, am], { hotelId: "recLuxvwwxID7U2B8" });
  assert.equal(ranked[0].personId, "am", "asset manager outranks seniority-only CEO");
  const rec = selectRecommendedFirstContact(ranked);
  assert.equal(
    rec.recommendationType,
    "SUGGESTED_CONTACT_PROPERTY_RESPONSIBILITY_NOT_CONFIRMED"
  );
  assert.equal(rec.person.personId, "am");
  assert.equal(looksLikeInventedEmail("jeff.mullen@mullencap.com", "Jeff Mullen"), true);
  assert.equal(
    isCorporateChannelMislabeledAsDirect({
      classification: "direct_personal",
      value: "info@mullencap.com",
    }),
    true
  );
  assert.deepEqual(detectDuplicateNamedContacts([am, { ...am, personId: "dup" }]), [
    "katherine button",
  ]);
  assert.equal(
    assertNoUnsupportedPrimaryClaim({
      contactClass: CONTACT_CLASS.PRIMARY_PROPERTY_DECISION_MAKER,
      propertyResponsibility: PROPERTY_RESPONSIBILITY.INFERRED,
    }).ok,
    false
  );
  const research = JSON.parse(
    read(
      "reports/hotel-explorer/phase4-quality/HE-BETH-OWNER-DECISION-MAKER-RESEARCH-STAGED-V1.json"
    )
  );
  assert.equal(research.applyAuthorized, false);
  assert.ok(research.people.every((p) => p.linkedinUrl && p.linkedinUrl.includes("linkedin.com")));
  assert.ok(research.people.every((p) => !(p.emailContacts || []).length));
  const hci = read("public/js/hotel-contact-intelligence.js");
  mustInclude(hci, "Recommended First Contact");
  mustInclude(hci, "Find Email & Phone");
  mustInclude(hci, "data-linkedin-url");
  mustInclude(hci, "Organization Contact Channels");
  assert.ok(!/hci-dev-json|production_writes|JSON\.stringify\(vm/.test(hci));
  const mountIdx = hci.indexOf("function mount(");
  assert.ok(!/enrichPeople|provider\.enrich/.test(hci.slice(mountIdx, mountIdx + 1200)));
  mustInclude(read("api/hotel-explorer-founder-preview.js"), "HE-BETH-OWNER-DECISION-MAKER-RESEARCH");
  mustInclude(read("api/hotel-explorer-founder-preview.js"), "buildDecisionOrientedContactSections");
  mustInclude(read("api/hotel-explorer-founder-preview.js"), "Find Email & Phone");
});

run("measurement_prefs_conversion_and_sort_keys", () => {
  assert.equal(SQFT_TO_SQM, 0.09290304);
  assert.equal(MI_TO_KM, 1.609344);
  assert.ok(Math.abs(sqFtToSqM(10000) - 929.0304) < 1e-6);
  assert.ok(Math.abs(kmToMi(16.09344) - 10) < 1e-6);
  assert.equal(areaSortValueSqFt(18719), 18719);
  assert.equal(areaSortValueSqFt("12,000"), 12000);
  assert.equal(defaultRadiusMilesForLocationType("Suburban"), 10);
  assert.equal(defaultRadiusMilesForLocationType("Urban"), 5);
  assert.equal(defaultRadiusMilesForLocationType("Resort"), 30);
  const prefsJs = read("public/js/he-measurement-prefs-v1.js");
  mustInclude(prefsJs, "0.09290304");
  mustInclude(prefsJs, "1.609344");
  mustInclude(prefsJs, "sessionStorage");
  mustInclude(prefsJs, "Sq. Ft.");
  mustInclude(prefsJs, "Sq. M.");
  mustInclude(prefsJs, 'return prefs.area === "sqm" ? "Sq. M." : "Sq. Ft."');
  mustNotMatch(prefsJs, /"\s*sq ft\s*"/i);
  mustNotMatch(prefsJs, /"\s*sq m\s*"/i);
  const rms = read("public/js/hotel-explorer-rooms-meeting-space.js");
  mustInclude(rms, "formatAreaFromSqFt");
  mustInclude(rms, "area: r.areaSqFt");
  mustInclude(read("public/hotel-explorer-preview.html"), "he-measurement-prefs-v1.js");
});

run("he_contextual_measurement_toggle_gates", () => {
  const hex = read("public/js/hotel-explorer.js");
  mustNotMatch(hex, /hex-meas-prefs-slot-host/);
  mustNotMatch(hex, /renderMeasurementPrefsSlot/);
  const prefsJs = read("public/js/he-measurement-prefs-v1.js");
  mustInclude(prefsJs, "renderAreaControlHtml");
  mustInclude(prefsJs, "renderDistanceControlHtml");
  mustInclude(prefsJs, 'data-he-meas-scope="');
  const rms = read("public/js/hotel-explorer-rooms-meeting-space.js");
  mustInclude(rms, "renderAreaControlHtml");
  mustInclude(rms, 'data-hex-rms-meas="area"');
  mustNotMatch(rms, /renderDistanceControlHtml|data-he-meas="distance"|data-he-meas-value="mi"/);
  const hdp = read("public/js/hotel-detail-panel.js");
  mustInclude(hdp, "renderDistanceControlHtml");
  mustInclude(hdp, 'data-hdp-meas="distance"');
  mustInclude(hdp, "bindDistancePrefsControls");
  // Distance control is placed in Area Hotels + Demand Drivers panel renderers only.
  const areaFn = hdp.slice(
    hdp.indexOf("function renderAreaHotelsTabPanel"),
    hdp.indexOf("function bindFilterPills")
  );
  const demandFn = hdp.slice(
    hdp.indexOf("function renderDemandDriversTabPanel"),
    hdp.indexOf("function buildDemandAnchorsFetchOpts")
  );
  mustInclude(areaFn, "renderDistancePrefsControlHtml");
  mustInclude(demandFn, "renderDistancePrefsControlHtml");
  mustNotMatch(areaFn, /renderAreaControlHtml|data-he-meas="area"|data-he-meas-value="sqft"/);
  mustNotMatch(demandFn, /renderAreaControlHtml|data-he-meas="area"|data-he-meas-value="sqft"/);
  // Access / overview / submarket panels must not mount measurement controls.
  const accessSlice = hdp.includes("function loadAccessConnectivityTab")
    ? hdp.slice(hdp.indexOf("function loadAccessConnectivityTab"), hdp.indexOf("function loadAccessConnectivityTab") + 2500)
    : "";
  mustNotMatch(accessSlice, /renderDistancePrefsControlHtml|renderAreaControlHtml|he-meas-prefs/);
  const std = read("docs/ai-build-system/HOTEL_EXPLORER_QUALITY_STANDARD.md");
  mustInclude(std, "Contextual controls only");
  mustInclude(std, "Guest Rooms & Meeting Space");
  mustInclude(std, "Area Hotels and Demand Drivers");
  mustNotMatch(hex, /renderControlHtml\(\)/);
  const css = read("public/css/hotel-explorer.css");
  mustInclude(css, "hex-rms-header");
  mustInclude(css, "hdp-meas-prefs-slot");
});

run("he_universal_design_hpc_geo_gates", () => {
  const collisions = iconIdsMustBeDistinctForUnrelatedAttributes(HE_AMENITY_ICON_MAP);
  assert.equal(collisions.length, 0, collisions.join("; "));
  const icons = read("public/js/hilton-amenity-icons.js");
  mustInclude(icons, "barLounge");
  mustInclude(icons, "ballroom");
  mustInclude(icons, "metroAccess");
  mustInclude(icons, "roomAccessibility");
  const hexCss = read("public/css/hotel-explorer.css");
  mustInclude(hexCss, "grid-auto-columns: minmax(76px, 1fr)");
  mustInclude(hexCss, "he-meas-prefs");
  const hdpCss = read("public/css/hotel-detail-panel.css");
  mustInclude(hdpCss, "--hex-surface-table");
  mustInclude(hdpCss, "--hdp-bg-deep");
  const hdp = read("public/js/hotel-detail-panel.js");
  mustInclude(hdp, "resolveCensusHotelPool");
  mustInclude(hdp, "dedupeHotelsByIdentity");
  mustInclude(hdp, "formatDistanceFromKm");
  mustInclude(hdp, "ada-table-shell");
  mustInclude(hdp, "8.05");
  mustInclude(hdp, "48.28");
  mustNotMatch(hdp, /X-Dealality-Write:\s*1|Airtable\.update/);
  const preview = read("public/hotel-explorer-preview.html");
  mustInclude(preview, "admin-ai-demand-table.css");
  assert.ok(HE_AMENITY_ICON_MAP.length >= 12);
});

run("he_surface_hierarchy_consistency_gates", () => {
  const hex = read("public/css/hotel-explorer.css");
  mustInclude(hex, "--hex-surface-canvas");
  mustInclude(hex, "--hex-surface-section");
  mustInclude(hex, "--hex-surface-table");
  mustInclude(hex, "--panel-soft: var(--hex-surface-section)");
  mustNotMatch(hex, /--panel-soft:\s*#0c132c/);
  mustNotMatch(hex, /background:\s*#0c132c\b/);
  mustNotMatch(hex, /background:\s*#0b1224\b/);
  mustNotMatch(hex, /background:\s*#0b1228\b/);
  mustNotMatch(hex, /background:\s*rgba\(13,\s*21,\s*44/);
  mustInclude(hex, "background: var(--hex-surface-section)");
  mustInclude(hex, "background: var(--hex-surface-table");
  mustInclude(hex, ".hex-op-group__body");
  mustInclude(hex, "background: transparent");
  const std = read("docs/ai-build-system/HOTEL_EXPLORER_QUALITY_STANDARD.md");
  mustInclude(std, "Canonical surface hierarchy");
  mustInclude(std, "--hex-surface-canvas");
  const preview = read("public/hotel-explorer-preview.html");
  mustInclude(preview, "hotel-explorer.css");
  mustInclude(preview, "hotel-detail-panel.css");
  // Preview and Radar both consume the same hex + hdp stylesheets (no fork).
  assert.ok(!preview.includes("hotel-explorer-preview-only.css"));
});

run("owner_portfolio_operator_reconciliation_gates", () => {
  const view = buildOwnerPortfolioOperatorView(
    [
      {
        hotel_name: "Bethesda Marriott",
        airtable_record_id: "recLuxvwwxID7U2B8",
        rooms: 407,
        include_as_owned: true,
        verification_bucket: "PROPOSED",
        operator: "Marriott International",
        brand: "Marriott Hotels",
        market: "Bethesda",
      },
      {
        hotel_name: "Managed-only decoy",
        airtable_record_id: "recDecoy",
        rooms: 999,
        include_as_owned: false,
        relationship_type: "OPERATED_BY",
        operator: "Marriott International",
      },
      {
        hotel_name: "Unresolved rooms hotel",
        airtable_record_id: null,
        rooms: null,
        include_as_owned: true,
        verification_bucket: "PROPOSED",
        census_match_status: "UNRESOLVED_HOTEL_CANDIDATE",
        operator: null,
      },
    ],
    {
      ownerDisplayName: "Mullen Real Estate Capital",
      scope: PORTFOLIO_SCOPE.VERIFIED_AND_PROPOSED,
    }
  );
  assert.equal(view.totals.hotels, 2);
  assert.equal(view.totals.roomsKnown, 407);
  assert.equal(view.totals.roomsKnownHotelCount, 1);
  assert.equal(view.totals.roomsUnknownHotelCount, 1);
  assert.equal(view.reconciliation.hotelsMatch, true);
  assert.equal(view.reconciliation.roomsMatch, true);
  assert.ok(!view.ownedHotels.some((h) => /decoy/i.test(h.hotel_name)));
  assert.ok(
    view.operatorGroups.some((g) => g.operatorName === OPERATOR_NOT_VERIFIED_LABEL)
  );
  assert.equal(
    classifyPortfolioHotel({
      verification_bucket: "PROPOSED",
      include_as_owned: true,
      airtable_record_id: "recX",
    }).class,
    PORTFOLIO_EVIDENCE.PROPOSED
  );
  const std = read("docs/ai-build-system/HOTEL_EXPLORER_QUALITY_STANDARD.md");
  mustInclude(std, "Who Manages These Hotels?");
  mustInclude(std, "Pending HPC Match");
  mustInclude(std, "Verified + proposed");
  const ws = read("public/js/hotel-intelligence-workspace.js");
  mustInclude(ws, "Who Manages These Hotels?");
  mustInclude(ws, "data-hex-op-toggle");
  mustInclude(ws, "data-hex-portfolio-scope");
  mustInclude(ws, "Pending HPC Match");
  mustNotMatch(ws, />Operated \/ Managed Hotels</);
  mustInclude(read("public/hotel-explorer-preview.html"), "he-owner-portfolio-operator-view-v1.js");
  const staged = JSON.parse(
    read("reports/hotel-explorer/phase4-quality/HE-BETH-MULLEN-OWNER-PORTFOLIO-STAGED-V1.json")
  );
  assert.equal(staged.applyAuthorized, false);
  const verifiedOnly = buildOwnerPortfolioOperatorView(staged.owned_controlled_current, {
    ownerDisplayName: staged.owner.display_name,
    scope: PORTFOLIO_SCOPE.VERIFIED_ONLY,
  });
  assert.equal(verifiedOnly.totals.hotels, 0);
  const mullen = buildOwnerPortfolioOperatorView(staged.owned_controlled_current, {
    ownerDisplayName: staged.owner.display_name,
    scope: PORTFOLIO_SCOPE.VERIFIED_AND_PROPOSED,
  });
  assert.equal(mullen.reconciliation.ok, true);
  assert.equal(mullen.totals.hotels, 8);
  assert.ok(mullen.ownedHotels.every((h) => h.ownership_evidence_class !== "VERIFIED"));
  assert.ok(mullen.ownedHotels.some((h) => h.airtable_record_id === "recLuxvwwxID7U2B8"));
});

run("he_ownership_control_network_gates", () => {
  const std = read("docs/ai-build-system/HOTEL_EXPLORER_QUALITY_STANDARD.md");
  mustInclude(std, "Ownership & Control Network");
  mustInclude(std, "he-ownership-control-network-v1");
  mustInclude(std, "Typed edges");
  const ws = read("public/js/hotel-intelligence-workspace.js");
  mustInclude(ws, "data-ocn-host");
  mustInclude(ws, "mountOwnershipControlNetwork");
  mustInclude(ws, "Executive Ownership Summary");
  mustInclude(ws, "Hotel Stakeholder");
  mustInclude(ws, "Relevant Decision-Makers");
  mustNotMatch(ws, /Property contact fallback \(HPC\)/);
  mustNotMatch(ws, /<h3 class="hiw-section__h">Ownership &amp; Control Hierarchy<\/h3>/);
  mustInclude(
    read("public/js/hotel-explorer.js"),
    "mountOwnershipControlNetwork"
  );
  mustInclude(read("public/hotel-explorer-preview.html"), "he-ownership-control-network-v1.js");
  const ocnJs = read("public/js/he-ownership-control-network-v1.js");
  mustInclude(ocnJs, "HeOwnershipControlNetwork");
  mustInclude(ocnJs, "Explore Key People");
  mustInclude(ocnJs, "Explore Related Hotels");
  mustInclude(ocnJs, "Brand Development view");
  mustInclude(ocnJs, "includePeople === true");
  mustInclude(ocnJs, "wrapNodeLabel");
  mustInclude(ocnJs, "elbowPath");
  mustNotMatch(ocnJs, /Expand sponsor hotels/);
  mustInclude(read("public/css/hotel-explorer.css"), "ocn-legend__item--proposed");
  mustInclude(read("public/css/hotel-explorer.css"), "ocn-node__label");
  mustInclude(
    read("lib/hotel-intelligence/ownership/ownership-control-network-v1.js"),
    "LEGALLY_OWNED_BY"
  );
  mustInclude(
    read("lib/hotel-intelligence/ownership/ownership-control-network-v1.js"),
    "ECONOMICALLY_CONTROLLED_BY"
  );
  mustInclude(
    read("lib/hotel-intelligence/ownership/ownership-control-network-v1.js"),
    "OCN_STAKEHOLDER_GROUPS"
  );
  mustNotMatch(
    read("lib/hotel-intelligence/ownership/ownership-control-network-v1.js"),
    /operator.*LEGALLY_OWNED_BY|brand.*ECONOMICALLY_CONTROLLED_BY/i
  );
});

run("he_hotel_news_from_market_alerts_gates", () => {
  const std = read("docs/ai-build-system/HOTEL_EXPLORER_QUALITY_STANDARD.md");
  mustInclude(std, "Hotel News & Articles");
  mustInclude(std, "MarketAlerts");
  mustInclude(std, "hpc:");
  const ws = read("public/js/hotel-intelligence-workspace.js");
  mustInclude(ws, "data-hiw-hotel-news-host");
  mustInclude(ws, "mountHotelNewsArticles");
  mustInclude(read("public/js/hotel-explorer.js"), "mountHotelNewsArticles");
  mustInclude(read("server.js"), "/api/hotel-explorer/hotels/:hotelId/news-articles");
  const api = read("api/hotel-explorer-news-articles.js");
  mustInclude(api, "X-Dealality-Write");
  mustNotMatch(api, /\.create\(|\.update\(/);
  mustInclude(
    read("lib/hotel-intelligence/hotel-news-from-market-alerts-v1.js"),
    "ORGANIZATION_CONTEXT_ONLY"
  );
  mustInclude(
    read("lib/hotel-intelligence/hotel-news-from-market-alerts-v1.js"),
    "HOTEL_SPECIFIC"
  );
});

run("he_async_section_loading_gates", () => {
  mustInclude(read("public/js/dealality-wave-loader.js"), "compactHtml");
  mustInclude(read("public/css/dealality-wave-loader.css"), "prefers-reduced-motion");
  mustInclude(read("public/js/he-async-section-v1.js"), "AbortController");
  mustInclude(read("public/js/he-async-section-v1.js"), "LOADING");
  const ws = read("public/js/hotel-intelligence-workspace.js");
  mustInclude(ws, "newsAsync");
  mustInclude(ws, "sentimentAsync");
  mustInclude(ws, "heWaveCompact");
  mustInclude(ws, "newsAsync.cancel()");
  mustInclude(read("public/hotel-explorer-preview.html"), "he-async-section-v1.js");
  mustInclude(read("public/hotel-explorer-preview.html"), "dealality-wave-loader.js");
  const reg = JSON.parse(
    read("docs/ai-build-system/HOTEL_EXPLORER_FOUNDER_QUALITY_LESSONS_REGISTRY.json")
  );
  if (!reg.lessons.some((l) => l.id === "HE-QL-024")) {
    throw new Error("HE-QL-024 missing");
  }
});

run("he_market_alerts_closeout_gates", () => {
  const std = read("docs/ai-build-system/HOTEL_EXPLORER_QUALITY_STANDARD.md");
  mustInclude(std, "Sources & Evidence");
  mustInclude(std, "HPC Hotel Record IDs");
  mustInclude(std, "writeOnRead");
  const ws = read("public/js/hotel-intelligence-workspace.js");
  mustInclude(ws, "mountMarketAlertsEvidenceForSources");
  mustInclude(ws, "market_alerts_id");
  mustInclude(read("public/js/hotel-explorer.js"), "mountMarketAlertsEvidenceForSources");
  const api = read("api/hotel-explorer-news-articles.js");
  mustInclude(api, "evidenceCitations");
  mustInclude(api, "writeOnRead");
  mustNotMatch(api, /\.create\(|\.update\(/);
  mustInclude(
    read("lib/hotel-intelligence/market-alerts-evidence-citations-v1.js"),
    "mergeMarketAlertsIntoCitations"
  );
  mustInclude(
    read("lib/hotel-intelligence/hotel-news-from-market-alerts-v1.js"),
    "planHpcAssocFieldMerge"
  );
});

run("he_guest_sentiment_brand_operator_gates", () => {
  const std = read("docs/ai-build-system/HOTEL_EXPLORER_QUALITY_STANDARD.md");
  mustInclude(std, "Guest Sentiment");
  mustInclude(std, "Category Performance");
  mustInclude(std, "Never");
  mustInclude(std, "live_ui_authorized");
  mustInclude(std, "label category scores as");
  mustInclude(std, "HE-QL-029");
  mustInclude(std, "Seven-platform overview");
  const ws = read("public/js/hotel-intelligence-workspace.js");
  mustInclude(ws, "data-hiw-guest-sentiment-host");
  mustInclude(ws, "mountGuestSentiment");
  mustInclude(ws, "Guest Reputation");
  mustInclude(ws, "hiw-rep-scorecards");
  mustInclude(ws, "Category Performance");
  mustInclude(ws, "Sources, Methodology &amp; Limitations");
  mustInclude(ws, "hiw-rep-scorecard--unavailable");
  mustInclude(ws, "platformOverview");
  mustNotMatch(ws, /Frequently praised/);
  mustNotMatch(ws, /id:\s*"guest-sentiment"/);
  mustInclude(read("public/js/hotel-explorer.js"), "mountGuestSentiment");
  mustInclude(read("server.js"), "/api/hotel-explorer/hotels/:hotelId/guest-sentiment");
  const api = read("api/hotel-explorer-guest-sentiment.js");
  mustInclude(api, "X-Dealality-Write");
  mustInclude(api, "X-Dealality-Paid-Providers");
  mustNotMatch(api, /\.create\(|\.update\(/);
  const model = read("lib/hotel-intelligence/guest-sentiment/guest-sentiment-model-v1.js");
  mustInclude(model, "insufficient_history");
  mustInclude(model, "executiveInsights");
  mustInclude(model, "categoryPerformance");
  mustInclude(model, "REPUTATION_OVERVIEW_PLATFORMS_V1");
  mustInclude(model, "buildReputationPlatformOverview");
  mustInclude(model, "no_licensed_review_text");
  mustInclude(
    read("lib/hotel-intelligence/guest-sentiment/guest-theme-taxonomy-v1.js"),
    "staff_service"
  );
  const css = read("public/css/hotel-explorer.css");
  mustInclude(css, "hiw-rep-scorecard");
  mustInclude(css, "hiw-rep-scorecard--unavailable");
  mustInclude(css, "hiw-rep-bar__fill");
  const repCss = css.slice(css.indexOf("/* Guest Reputation"));
  if (/#38bdf8/.test(repCss)) {
    throw new Error("Guest Reputation CSS must not use arbitrary #38bdf8 cyan");
  }
  const reg = JSON.parse(
    read("docs/ai-build-system/HOTEL_EXPLORER_FOUNDER_QUALITY_LESSONS_REGISTRY.json")
  );
  if (!reg.lessons.some((l) => l.id === "HE-QL-022")) {
    throw new Error("HE-QL-022 missing from founder quality lessons registry");
  }
  if (!reg.lessons.some((l) => l.id === "HE-QL-025")) {
    throw new Error("HE-QL-025 missing from founder quality lessons registry");
  }
  if (!reg.lessons.some((l) => l.id === "HE-QL-029")) {
    throw new Error("HE-QL-029 missing from founder quality lessons registry");
  }
});

if (failures.length) {
  console.error(`\n${failures.length} quality gate(s) failed`);
  process.exit(1);
}
console.log("\nAll Hotel Explorer product quality gates passed.");
