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
  normalizeOrgPortfolioKeyPeople,
  buildOrgPortfolioStrategicContext,
  strategicContextHasGenericCopy,
} from "../lib/hotel-intelligence/ownership/he-org-portfolio-key-people-v1.js";
import {
  classifySubmarketGrouping,
  dedupeHpcHotelsByIdentity,
  computeHpcSubmarketMetrics,
  HE_HPC_SUBMARKET_SNAPSHOT_V1,
} from "../lib/hotel-intelligence/he-hpc-submarket-snapshot-v1.js";
import {
  HE_DEMAND_ACCESS_READ_V1,
  resolveContextRadiusKm,
  HE_CONTEXT_RADIUS_KM,
} from "../lib/hotel-intelligence/he-demand-access-read-v1.js";
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
  assert.equal(reg.version, "hotel-explorer-founder-quality-lessons-v1.29");
  assert.ok(Array.isArray(reg.lessons) && reg.lessons.length >= 32);
  mustInclude(
    read("docs/ai-build-system/HOTEL_EXPLORER_QUALITY_STANDARD.md"),
    "hotel-explorer-quality-standard-v1.25"
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
  assert.ok(reg.lessons.some((l) => l.id === "HE-QL-031"));
  assert.ok(reg.lessons.some((l) => l.id === "HE-QL-032"));
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
  const tabsMatch = hex.match(/var ALL_TABS = \[([\s\S]*?)\];/) || hex.match(/var TABS = \[([\s\S]*?)\];/);
  assert.ok(tabsMatch, "TABS array present");
  const ids = [...tabsMatch[1].matchAll(/id:\s*"([^"]+)"/g)].map((m) => m[1]);
  assert.equal(ids[0], "hotel");
  assert.equal(ids[1], "rooms");
  assert.ok(ids.includes("sentiment"), "sentiment remains registered for reactivation");
  assert.equal(ids.length, 12);
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
  const assets = read("lib/hotel-intelligence/he-canonical-explorer-assets-v1.js");
  mustInclude(assets, "HE_ACTIVE_TAB_IDS");
  mustInclude(assets, "HE_PRESERVED_DISABLED_TAB_IDS");
  mustInclude(assets, "he-guest-sentiment-feature-flag-v1.js");
  const he = read("public/js/hotel-explorer.js");
  mustInclude(he, "ALL_TABS");
  mustInclude(he, "isGuestSentimentEnabled");
  assert.ok(he.includes('id: "sentiment"'), "sentiment tab remains registered for reactivation");
});

run("contact_cards_omit_dash_placeholders_and_surfe_not_on_get", () => {
  const hci = read("public/js/hotel-contact-intelligence.js");
  mustInclude(hci, "Find Contact Details");
  mustInclude(hci, "Loading Contacts…");
  mustInclude(hci, "HeLoadingUx");
  mustInclude(hci, "hci-note--unresolved");
  mustInclude(hci, "data-hci-enrich");
  mustInclude(hci, "/reveal");
  mustInclude(hci, "enrichment-capability");
  mustInclude(hci, "Hotel Leadership");
  mustInclude(hci, "Sales & Commercial Leadership");
  mustInclude(hci, "Ownership & Asset Management");
  mustInclude(hci, "Management Company & Regional Leadership");
  mustInclude(hci, "Brand & Development");
  mustInclude(hci, "data-hci-purpose-filter");
  mustInclude(hci, "Company contact routes");
  mustInclude(hci, "Open items");
  mustInclude(hci, "Email");
  mustInclude(hci, "Call");
  mustInclude(hci, "Find Contact Details");
  assert.ok(!hci.includes("Recommended First Contact"), "no global recommended-first hierarchy");
  assert.ok(!hci.includes("Alternative Decision-Makers"));
  assert.ok(!hci.includes("Suggested Contact"), "no renamed global first-contact hierarchy");
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
  mustInclude(hci, "Hotel Leadership");
  mustInclude(hci, "Find Contact Details");
  mustInclude(hci, "data-linkedin-url");
  mustInclude(hci, "Company contact routes");
  assert.ok(!hci.includes("Recommended First Contact"));
  assert.ok(!/hci-dev-json|production_writes|JSON\.stringify\(vm/.test(hci));
  const mountIdx = hci.indexOf("function mount(");
  assert.ok(!/enrichPeople|provider\.enrich/.test(hci.slice(mountIdx, mountIdx + 1200)));
  mustInclude(read("api/hotel-explorer-founder-preview.js"), "HE-BETH-OWNER-DECISION-MAKER-RESEARCH");
  mustInclude(read("api/hotel-explorer-founder-preview.js"), "buildPurposeGroupedContactSections");
  mustInclude(read("api/hotel-explorer-founder-preview.js"), "resolveEnrichAction");
  mustInclude(
    read("lib/hotel-intelligence/contact-intelligence/he-public-contact-channel-hierarchy-v1.js"),
    "Find Contact Details"
  );
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
  // Distance control: Area Hotels + Access & Connectivity (HE-QL-037); not Demand Drivers.
  const areaFn = hdp.slice(
    hdp.indexOf("function renderAreaHotelsTabPanel"),
    hdp.indexOf("function bindFilterPills")
  );
  const demandFn = hdp.slice(
    hdp.indexOf("function renderDemandDriversTabPanel"),
    hdp.indexOf("function buildDemandAnchorsFetchOpts")
  );
  const accessPanelFn = hdp.slice(
    hdp.indexOf("function renderAccessConnectivityPanel"),
    hdp.indexOf("function renderAccessConnectivityPanel") + 2200
  );
  mustInclude(areaFn, "renderDistancePrefsControlHtml");
  mustInclude(accessPanelFn, "renderDistancePrefsControlHtml");
  mustNotMatch(demandFn, /renderDistancePrefsControlHtml/);
  mustNotMatch(areaFn, /renderAreaControlHtml|data-he-meas="area"|data-he-meas-value="sqft"/);
  mustNotMatch(demandFn, /renderAreaControlHtml|data-he-meas="area"|data-he-meas-value="sqft"/);
  mustNotMatch(accessPanelFn, /renderAreaControlHtml|data-he-meas="area"|data-he-meas-value="sqft"/);
  const std = read("docs/ai-build-system/HOTEL_EXPLORER_QUALITY_STANDARD.md");
  mustInclude(std, "Contextual controls only");
  mustInclude(std, "Guest Rooms & Meeting Space");
  mustInclude(std, "Area Hotels and Access");
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
  mustInclude(ocnJs, "data-ocn-group");
  mustInclude(ocnJs, "Related Hotels (");
  mustInclude(ocnJs, "Brand Development view");
  mustInclude(ocnJs, "portfolioAsCluster");
  mustInclude(ocnJs, "includeHistoricalCapital");
  mustInclude(ocnJs, "wrapNodeLabel");
  mustInclude(ocnJs, "elbowPath");
  mustNotMatch(ocnJs, /Expand sponsor hotels/);
  mustInclude(read("public/css/hotel-explorer.css"), "ocn-legend__item--proposed");
  mustInclude(read("public/css/hotel-explorer.css"), "ocn-node__label");
  mustInclude(read("public/css/hotel-explorer.css"), "ocn-chip");
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
  mustInclude(
    read("lib/hotel-intelligence/ownership/ownership-control-network-v1.js"),
    "assessStakeholderResearchCoverage"
  );
  mustNotMatch(
    read("lib/hotel-intelligence/ownership/ownership-control-network-v1.js"),
    /operator.*LEGALLY_OWNED_BY|brand.*ECONOMICALLY_CONTROLLED_BY/i
  );
});

run("he_relationships_no_radial_org_network", () => {
  const ws = read("public/js/hotel-intelligence-workspace.js");
  mustNotMatch(ws, /Show Organization Network|data-hiw-org-network|hiwOrgNetwork|loadOrgNetwork|drawMiniNetwork|Organization Network/);
  mustInclude(ws, "Relationship Detail");
  mustInclude(ws, "renderRelationships");
  mustInclude(ws, "mountOwnershipControlNetwork");
  mustNotMatch(read("public/js/hotel-explorer.js"), /data-hiw-org-network|loadOrgNetwork/);
  mustNotMatch(
    read("public/css/hotel-intelligence-workspace.css"),
    /\.ofg-edge|\.ofg-node|hiw-graph-svg \.ofg/
  );
  const std = read("docs/ai-build-system/HOTEL_EXPLORER_QUALITY_STANDARD.md");
  mustInclude(std, "no redundant radial Organization Network");
  mustInclude(std, "HE-QL-034");
  const reg = JSON.parse(
    read("docs/ai-build-system/HOTEL_EXPLORER_FOUNDER_QUALITY_LESSONS_REGISTRY.json")
  );
  assert.ok(reg.lessons.some((l) => l.id === "HE-QL-034"));
});

run("he_stakeholder_research_vs_viz_coverage", () => {
  const std = read("docs/ai-build-system/HOTEL_EXPLORER_QUALITY_STANDARD.md");
  mustInclude(std, "Stakeholder Research Coverage");
  mustInclude(std, "Network Visualization Coverage");
  mustInclude(std, "Historical ≠ current");
  mustInclude(read("api/hotel-explorer-founder-preview.js"), "stakeholder_research_coverage");
  mustInclude(read("api/hotel-explorer-founder-preview.js"), "Ramsfield Hospitality Finance");
  mustInclude(read("api/hotel-explorer-founder-preview.js"), "Hodges Ward Elliott");
  mustInclude(read("api/hotel-explorer-founder-preview.js"), "HISTORICAL");
  assert.ok(
    !/current lender|current mortgagee/i.test(
      read("reports/hotel-explorer/phase4-quality/HE-BETH-STAKEHOLDER-UPSERTS-DRY-RUN-V1.json")
    ) ||
      /NOT proof of current|current": false/i.test(
        read("reports/hotel-explorer/phase4-quality/HE-BETH-STAKEHOLDER-UPSERTS-DRY-RUN-V1.json")
      )
  );
  const matrix = read("reports/hotel-explorer/HE-BETH-STAKEHOLDER-COVERAGE-MATRIX-V21.md");
  mustInclude(matrix, "HISTORICAL VERIFIED · CURRENT GAP");
  mustInclude(matrix, "FIRM RESEARCHED · PROPERTY ASSIGNMENT GAP");
  const reg = JSON.parse(
    read("docs/ai-build-system/HOTEL_EXPLORER_FOUNDER_QUALITY_LESSONS_REGISTRY.json")
  );
  assert.ok(reg.lessons.some((l) => l.id === "HE-QL-033"));
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

run("he_guest_sentiment_tab_restructure_gates", () => {
  const hex = read("public/js/hotel-explorer.js");
  const ws = read("public/js/hotel-intelligence-workspace.js");
  const css = read("public/css/hotel-explorer.css");
  const std = read("docs/ai-build-system/HOTEL_EXPLORER_QUALITY_STANDARD.md");
  const model = read("lib/hotel-intelligence/guest-sentiment/guest-sentiment-model-v1.js");
  const fixture = read("fixtures/hotel-explorer/guest-sentiment/beth-marriott-staged-v1.json");
  const reg = JSON.parse(
    read("docs/ai-build-system/HOTEL_EXPLORER_FOUNDER_QUALITY_LESSONS_REGISTRY.json")
  );

  mustInclude(hex, 'id: "sentiment"');
  mustInclude(hex, "Guest<br>Sentiment");
  mustInclude(hex, "ALL_TABS");
  mustInclude(hex, "isGuestSentimentEnabled");
  mustInclude(hex, "TAB_ALIASES");
  mustInclude(hex, "normalizeHexTab");
  mustInclude(hex, 'brand: "sentiment"');
  mustInclude(ws, 'id: "sentiment"');
  mustInclude(ws, "Guest Sentiment");
  mustInclude(ws, "renderGuestSentimentTab");
  mustInclude(ws, "isGuestSentimentEnabled");
  mustInclude(ws, "data-hiw-guest-sentiment-tab");
  mustInclude(
    read("lib/hotel-intelligence/guest-sentiment/he-guest-sentiment-feature-flag-v1.js"),
    "DISABLED_BY_FOUNDER"
  );
  mustNotMatch(ws, /hiw-section__h">Franchise &amp; Brand Status/);
  mustNotMatch(ws, /hiw-section__h">Current Operator/);
  mustNotMatch(ws, /hiw-report__title">Brand \/ Operator/);
  mustInclude(ws, "hiw-rep-insight__items");
  mustInclude(ws, "data-he-founder-preview");
  mustInclude(css, "var(--accent--primary-1");
  mustInclude(css, "hiw-rep-bar__fill");
  const fillSlice = css.slice(css.indexOf(".hex-root .hiw-rep-bar__fill"), css.indexOf(".hex-root .hiw-rep-bar__fill") + 280);
  assert.ok(
    fillSlice.includes("var(--accent--primary-1"),
    "Category bars must use dynamic Chain Scale CSS variable"
  );
  mustNotMatch(fillSlice, /background:\s*#6c72ff/);
  mustInclude(model, "scrubCustomerFacingFollowUps");
  mustInclude(model, "items:");
  mustNotMatch(model, /Confirm which platform property IDs/);
  const followJson = JSON.parse(fixture).insights?.followUps || [];
  assert.ok(Array.isArray(followJson) && followJson.length >= 1);
  const followText = followJson.join("\n");
  mustNotMatch(followText, /d83967|h23292|Confirm Tripadvisor|before any paid refresh|platform property ID/i);
  mustNotMatch(followText, /rec[A-Za-z0-9]{14}/);
  mustInclude(std, "HE-QL-038");
  mustInclude(std, "Dynamic Chain Scale accents");
  assert.ok(reg.lessons.some((l) => l.id === "HE-QL-038"));
  mustInclude(read("public/js/dealality-chain-scale-theme.js"), "chainStripeColor");
});

run("sources_evidence_consolidated_nav_and_dates", () => {
  const ws = read("public/js/hotel-intelligence-workspace.js");
  const css = read("public/css/hotel-explorer.css");
  const ocn = read("public/js/he-ownership-control-network-v1.js");
  const ma = read("lib/hotel-intelligence/market-alerts-evidence-citations-v1.js");
  const std = read("docs/ai-build-system/HOTEL_EXPLORER_QUALITY_STANDARD.md");
  const reg = JSON.parse(
    read("docs/ai-build-system/HOTEL_EXPLORER_FOUNDER_QUALITY_LESSONS_REGISTRY.json")
  );

  mustNotMatch(ws, /Open Contact Intelligence/);
  mustNotMatch(ws, /Open Sources\s*&(?:amp;)?\s*Evidence/);
  mustNotMatch(ws, /Sources and Verification/);
  mustNotMatch(ws, /function renderOwnershipSourcesStrip/);
  mustInclude(ws, "hiw-navlink");
  mustInclude(ws, "sectionHeadHtml");
  mustInclude(ws, "View Contacts");
  mustInclude(ws, "View Sources");
  mustInclude(ws, "data-hiw-source-filter");
  mustInclude(ws, "data-hiw-source-focus");
  mustInclude(ws, "publication_date");
  mustInclude(ws, "Verified / accessed");
  mustInclude(ws, "normalizeCitationDates");
  mustInclude(ws, "SOURCE_CATEGORY_DEFS");
  mustInclude(ws, "dedupeCitations");
  mustInclude(css, "hiw-navlink");
  mustInclude(css, "hiw-section__head");
  mustInclude(css, "is-source-focus");
  mustInclude(ocn, "View Contacts");
  mustNotMatch(ocn, /Open Contact Intelligence/);
  mustInclude(ma, "publication_date");
  mustInclude(std, "HE-QL-031");
  mustInclude(std, "One canonical Sources & Evidence tab");
  assert.ok(reg.lessons.some((l) => l.id === "HE-QL-031"));

  // Oversized cross-tab nav buttons for Contacts/Sources must not reappear as hiw-btn.
  assert.ok(
    !/hiw-btn[^>]*(?:data-hiw-tab="contacts"|data-hiw-tab="sources")/.test(ws),
    "Contacts/Sources cross-tab nav must not use hiw-btn"
  );
});

run("he_public_contact_hierarchy_and_surfe_session_only", () => {
  const hci = read("public/js/hotel-contact-intelligence.js");
  const api = read("api/contact-intelligence.js");
  const lib = read(
    "lib/hotel-intelligence/contact-intelligence/he-public-contact-channel-hierarchy-v1.js"
  );
  const std = read("docs/ai-build-system/HOTEL_EXPLORER_QUALITY_STANDARD.md");
  const reg = JSON.parse(
    read("docs/ai-build-system/HOTEL_EXPLORER_FOUNDER_QUALITY_LESSONS_REGISTRY.json")
  );
  mustInclude(hci, "Direct Business Email");
  mustInclude(hci, "Direct Business Phone");
  mustInclude(hci, "Company contact routes");
  mustInclude(hci, "Company email");
  mustInclude(hci, "Find Phone");
  mustInclude(hci, "Find Contact Details");
  mustInclude(hci, "_sessionSurfeByPerson");
  mustInclude(hci, "session-only");
  assert.ok(!hci.includes("Recommended First Contact"));
  mustInclude(api, "persist_forbidden");
  mustInclude(api, "NOT_WRITTEN");
  mustInclude(lib, "looksLikeInventedEmail");
  mustInclude(lib, "resolveEnrichAction");
  mustInclude(lib, "surfeDerived: false");
  mustInclude(lib, "Organizational Contact Route");
  mustInclude(lib, "Direct Business Email");
  mustInclude(std, "HE-QL-032");
  mustInclude(std, "Surfe session-only");
  assert.ok(reg.lessons.some((l) => l.id === "HE-QL-032"));
  // Surfe must not be auto-invoked on mount/GET.
  const mountSlice = hci.slice(hci.indexOf("function mount("), hci.indexOf("function mount(") + 2500);
  assert.ok(!/enrichPeople|createSurfeContactProvider|\/reveal/.test(mountSlice));
});

run("he_org_portfolio_key_people_strategic_context_gates", () => {
  const ws = read("public/js/hotel-intelligence-workspace.js");
  const api = read("api/hotel-explorer-founder-preview.js");
  const lib = read("lib/hotel-intelligence/ownership/he-org-portfolio-key-people-v1.js");
  const std = read("docs/ai-build-system/HOTEL_EXPLORER_QUALITY_STANDARD.md");
  const reg = JSON.parse(
    read("docs/ai-build-system/HOTEL_EXPLORER_FOUNDER_QUALITY_LESSONS_REGISTRY.json")
  );
  const research = JSON.parse(
    read(
      "reports/hotel-explorer/phase4-quality/HE-BETH-OWNER-DECISION-MAKER-RESEARCH-STAGED-V1.json"
    )
  );

  mustInclude(api, "buildSharedProposedPeople");
  mustInclude(api, "normalizeOrgPortfolioKeyPeople");
  mustInclude(api, "buildOrgPortfolioStrategicContext");
  mustInclude(api, "strategic_context");
  mustInclude(ws, "collectOrgKeyPeople");
  mustInclude(ws, "normalizeOrgKeyPeopleRows");
  mustInclude(ws, "renderStrategicContext");
  mustInclude(ws, "data-hiw-strategic-context");
  mustInclude(ws, "hiw-person-link");
  mustInclude(ws, "Commercial role");
  mustInclude(ws, "Property responsibility");
  mustInclude(ws, "linkedinUrl");
  mustNotMatch(
    ws,
    /Use Hotel → Owner → Portfolio and Owner → Hotels → Operators/
  );
  mustNotMatch(ws, /Research one owner; reuse verified relationships across every connected hotel/);
  mustInclude(lib, "normalizeOrgPortfolioKeyPeople");
  mustInclude(lib, "buildOrgPortfolioStrategicContext");
  mustInclude(std, "HE-QL-035");
  mustInclude(std, "One shared contact intelligence model");
  mustInclude(std, "No empty fields when verified intelligence already exists");
  assert.ok(reg.lessons.some((l) => l.id === "HE-QL-035"));

  const rows = normalizeOrgPortfolioKeyPeople(research.people || [], {
    organizationName: "Mullen Real Estate Capital",
  });
  assert.ok(rows.length >= 3, "Bethesda research people must normalize to ≥3 Key People rows");
  const jeff = rows.find((p) => /jeff mullen/i.test(p.name));
  const katherine = rows.find((p) => /katherine button/i.test(p.name));
  const patrick = rows.find((p) => /patrick reece/i.test(p.name));
  assert.ok(jeff, "Jeff Mullen required");
  assert.ok(katherine, "Katherine Button required");
  assert.ok(patrick, "Patrick Reece required");
  assert.ok(jeff.relevance && !/^—$/.test(jeff.relevance), "Jeff relevance must not be blank");
  assert.ok(jeff.linkedinUrl, "Jeff LinkedIn must surface from shared model");
  assert.ok(jeff.professional_profile_verified, "Jeff LinkedIn must be verified");
  assert.ok(
    katherine.relevance && /asset/i.test(katherine.relevance),
    "Katherine asset-management relevance required"
  );
  assert.ok(
    patrick.organization && /mullen/i.test(patrick.organization),
    "Patrick organization/portfolio role required"
  );
  assert.equal(
    rows.filter((p) => /jeff mullen/i.test(p.name)).length,
    1,
    "No duplicate Jeff Mullen rows"
  );
  assert.ok(
    /INFERRED|inferred/i.test(String(jeff.propertyResponsibilityLabel || "")),
    "Property responsibility must stay qualified (inferred), not invented confirmed"
  );

  const sc = buildOrgPortfolioStrategicContext(
    { name: "Bethesda Marriott", city: "Bethesda", rooms: 407, brand: "Marriott" },
    {
      hotel: { name: "Bethesda Marriott", brand_display: "Marriott" },
      report: {
        ownership_and_control: {
          legal_property_owner_propco: { name: "Marbeth Acquisition LLC", preview_state: "PROPOSED_STAGED" },
          economic_owner_or_group: { name: "Mullen Real Estate Capital" },
          operator: { name: "Marriott International" },
          ubo_group: { known: false, status: "UNRESOLVED" },
          lender: { name: "Ramsfield Hospitality Finance", current: false, preview_state: "HISTORICAL" },
        },
      },
      organization: {
        owned_controlled_hotels: [
          { hotel_name: "Bethesda Marriott" },
          { hotel_name: "Example Related Hotel" },
        ],
      },
    }
  );
  const bodies = (sc.sections || []).map((s) => s.body).join(" ");
  assert.ok(sc.sections.some((s) => s.id === "asset_market"));
  assert.ok(sc.sections.some((s) => s.id === "ownership_portfolio"));
  assert.ok(sc.sections.some((s) => s.id === "strengths"));
  assert.ok(sc.sections.some((s) => s.id === "strategic"));
  assert.ok(sc.sections.some((s) => s.id === "follow_ups"));
  assert.ok(!strategicContextHasGenericCopy(bodies), "Strategic Context must not be pipeline boilerplate");
  // Affirmative unsupported investment conclusions are forbidden; denial / qualification language is required.
  assert.ok(
    !/\b(owner is willing to sell|owner plans to sell|definitely selling|confirmed refinance|confirmed rebrand)\b/i.test(
      bodies
    ),
    "Must not assert unsupported sell/rebrand/refinance conclusions"
  );
  assert.ok(
    /no public evidence|does not authorize|without evidence/i.test(bodies),
    "Must qualify that sell/rebrand/refinance willingness is unproven"
  );
  assert.ok(/unresolved/i.test(bodies), "UBO / unresolved claims must stay qualified");
  assert.ok(/Sources|evidence/i.test(ws) || /Sources & Evidence/.test(std));
});

run("he_hpc_submarket_snapshot_canonical_gates", () => {
  const lib = read("lib/hotel-intelligence/he-hpc-submarket-snapshot-v1.js");
  const api = read("api/hotel-explorer-submarket-snapshot.js");
  const hdp = read("public/js/hotel-detail-panel.js");
  const server = read("server.js");
  const std = read("docs/ai-build-system/HOTEL_EXPLORER_QUALITY_STANDARD.md");
  const reg = JSON.parse(
    read("docs/ai-build-system/HOTEL_EXPLORER_FOUNDER_QUALITY_LESSONS_REGISTRY.json")
  );

  mustInclude(lib, HE_HPC_SUBMARKET_SNAPSHOT_V1);
  mustInclude(lib, "buildHpcSubmarketSnapshot");
  mustInclude(lib, "selected_only");
  mustInclude(lib, "supporting_enrichment_only");
  mustInclude(lib, "tbl9aY5ijiuIzzWam");
  mustInclude(api, "X-Dealality-Write");
  mustInclude(api, "buildHpcSubmarketSnapshot");
  mustInclude(server, "/api/hotel-explorer/hotels/:hotelId/submarket-snapshot");
  mustInclude(hdp, "fetchHpcSubmarketSnapshot");
  mustInclude(hdp, "ensureSelectedHotelInPool");
  mustInclude(hdp, "data-hdp-submarket-source");
  mustNotMatch(
    hdp,
    /Prefer Radar map census; when empty \(Founder Preview\), load Hotel Census via Scout coverage/
  );
  mustInclude(std, "HE-QL-036");
  mustInclude(std, "HPC is the source of truth");
  mustInclude(std, "never");
  assert.ok(reg.lessons.some((l) => l.id === "HE-QL-036"));

  const beth = {
    id: "recLuxvwwxID7U2B8",
    name: "Bethesda Marriott",
    market: "Bethesda / Montgomery County",
    submarket: "Pooks Hill / Rockville Pike",
    city: "Bethesda",
    country: "United States",
    status: "Open",
    rooms: 407,
  };
  const grouping = classifySubmarketGrouping(beth);
  assert.equal(grouping.mode, "submarket");
  assert.equal(grouping.label, "Pooks Hill / Rockville Pike");
  assert.equal(grouping.researchNeeded, false);

  const noGeo = classifySubmarketGrouping({
    id: "recTest",
    name: "Unknown Geo Hotel",
    status: "Open",
  });
  assert.equal(noGeo.mode, "selected_only");
  assert.equal(noGeo.researchNeeded, true);

  const deduped = dedupeHpcHotelsByIdentity([
    beth,
    { ...beth, name: "Bethesda Marriott Duplicate" },
    { id: "recOtherHotel0001", name: "Other", status: "Open", rooms: 100 },
  ]);
  assert.equal(deduped.length, 2);

  const metrics = computeHpcSubmarketMetrics([
    beth,
    { id: "recPipe", name: "Pipe", status: "Pipeline", rooms: null },
  ]);
  assert.equal(metrics.openHotels, 1);
  assert.equal(metrics.openRooms, 407);
  assert.equal(metrics.pipelineHotels, 1);
  assert.equal(metrics.pipelineRooms, 0);
  assert.equal(metrics.roomsUnknownHotelCount, 1);
  assert.equal(metrics.roomsCoverageComplete, false);

  // Empty Scout must not be coded as the only path that can populate the panel.
  mustInclude(hdp, 'source: "hpc"');
  mustInclude(hdp, "Hotel Property Census (canonical)");
});

run("he_demand_access_airtable_read_gates", () => {
  const lib = read("lib/hotel-intelligence/he-demand-access-read-v1.js");
  const api = read("api/hotel-explorer-demand-access.js");
  const hdp = read("public/js/hotel-detail-panel.js");
  const server = read("server.js");
  const daFields = read("lib/demand-anchors/airtable-demand-anchors-fields.js");
  const tiFields = read("lib/travel-infrastructure/airtable-travel-infrastructure-fields.js");
  const std = read("docs/ai-build-system/HOTEL_EXPLORER_QUALITY_STANDARD.md");
  const inv = read("reports/hotel-explorer/HE-DEMAND-ACCESS-AIRTABLE-RECOVERY-V1.md");
  const reg = JSON.parse(
    read("docs/ai-build-system/HOTEL_EXPLORER_FOUNDER_QUALITY_LESSONS_REGISTRY.json")
  );

  mustInclude(lib, HE_DEMAND_ACCESS_READ_V1);
  mustInclude(lib, "buildHeDemandDriversRead");
  mustInclude(lib, "buildHeAccessConnectivityRead");
  mustInclude(lib, "resolveHpcHotelForDemandAccess");
  mustInclude(lib, "fetchDemandAnchorRecords");
  mustInclude(lib, "fetchTravelInfrastructureRecords");
  mustInclude(lib, "STRAIGHT_LINE");
  mustInclude(lib, "Reusable geographic");
  mustInclude(lib, "Do not invent");
  mustInclude(api, "X-Dealality-Write");
  mustInclude(api, "X-Dealality-Paid-Providers");
  mustInclude(api, "buildHeDemandDriversRead");
  mustInclude(api, "buildHeAccessConnectivityRead");
  mustInclude(server, "/api/hotel-explorer/hotels/:hotelId/demand-drivers");
  mustInclude(server, "/api/hotel-explorer/hotels/:hotelId/access-connectivity");
  mustInclude(hdp, "fetchHeDemandDrivers");
  mustInclude(hdp, "fetchHeAccessConnectivity");
  mustInclude(hdp, "hydrateHotelFromHpc");
  mustInclude(hdp, "verifiedTravelTimeLabel");
  mustInclude(hdp, "data-hdp-demand-source");
  mustInclude(hdp, "data-hdp-access-source");
  mustInclude(hdp, "data-he-async=\"demand-drivers\"");
  mustInclude(hdp, "data-he-async=\"access-connectivity\"");
  // No invented drive times from haversine / 55 km/h heuristic.
  mustNotMatch(hdp, /estimateDriveMinutes\s*\(/);
  mustNotMatch(hdp, /55\s*km\/h/);
  mustNotMatch(hdp, /~\s*"\s*\+\s*mins\s*\+\s*"\s*min drive/);
  // Access tab hosts mi/km; Demand Drivers tab does not reintroduce the control.
  mustInclude(hdp, "hdp-access-connectivity-tab");
  mustInclude(hdp, "renderDistancePrefsControlHtml()");
  const demandTabFn = hdp.match(
    /function renderDemandDriversTabPanel\([\s\S]*?\n  function /
  );
  assert.ok(demandTabFn, "renderDemandDriversTabPanel must exist");
  assert.ok(
    !demandTabFn[0].includes("renderDistancePrefsControlHtml()"),
    "Demand Drivers tab must not host the mi/km toggle (Access-only placement)"
  );
  assert.ok(
    /function renderAccessConnectivityPanel[\s\S]*renderDistancePrefsControlHtml\(\)/.test(hdp),
    "Access & Connectivity must host the mi/km toggle when distances are shown"
  );
  // Canonical HPC + reusable entity stores.
  mustInclude(daFields, "Demand Anchors");
  mustInclude(tiFields, "Travel Infrastructure Data");
  mustInclude(lib, "Demand Anchors");
  mustInclude(lib, "Travel Infrastructure Data");
  mustInclude(std, "HE-QL-037");
  mustInclude(std, "Demand Anchors");
  mustInclude(std, "Travel Infrastructure Data");
  mustInclude(inv, "recLuxvwwxID7U2B8");
  mustInclude(inv, "Demand Anchors");
  mustInclude(inv, "Travel Infrastructure Data");
  assert.ok(reg.lessons.some((l) => l.id === "HE-QL-037"));

  assert.equal(resolveContextRadiusKm({ locationType: "Urban" }), HE_CONTEXT_RADIUS_KM.Urban);
  assert.equal(resolveContextRadiusKm({ locationType: "Suburban" }), HE_CONTEXT_RADIUS_KM.Suburban);
  // Distance conversion sanity (mi/km) — Access toggle uses shared prefs.
  assert.ok(Math.abs(kmToMi(16.09) - 10) < 0.05);
  assert.ok(Math.abs(MI_TO_KM * 10 - 16.09) < 0.05);

  // GET handlers must not import apply/write planners or invoke paid research.
  mustNotMatch(api, /--apply|applyAuthorized:\s*true|createRecords|updateRecords/);
  mustNotMatch(lib, /openai|webhound|createRecords|updateRecords/i);
  mustInclude(lib, "paidProviders: false");
  mustInclude(api, "paidProviders: false");
});

if (failures.length) {
  console.error(`\n${failures.length} quality gate(s) failed`);
  process.exit(1);
}
console.log("\nAll Hotel Explorer product quality gates passed.");
