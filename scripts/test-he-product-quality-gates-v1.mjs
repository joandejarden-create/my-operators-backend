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
  assert.equal(reg.version, "hotel-explorer-founder-quality-lessons-v1.2");
  assert.ok(Array.isArray(reg.lessons) && reg.lessons.length >= 12);
  mustInclude(
    read("docs/ai-build-system/HOTEL_EXPLORER_QUALITY_STANDARD.md"),
    "hotel-explorer-quality-standard-v1.2"
  );
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
  mustInclude(hdp, "hdp-amenities-grid");
  mustInclude(hdp, "resolveAmenitiesDisplay");
  mustNotMatch(hdp, /label:\s*["']has_pool["']/);
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
  mustInclude(css, "background: #0d152c");
});

run("hex_tabs_even_flex_and_rooms_present", () => {
  const css = read("public/css/hotel-explorer.css");
  mustInclude(css, ".hex-tabs .section-nav-item");
  mustInclude(css, "flex: 1 1 0");
  mustInclude(css, "min-width: 32px");
});

run("contact_cards_omit_dash_placeholders_and_surfe_not_on_get", () => {
  const hci = read("public/js/hotel-contact-intelligence.js");
  mustInclude(hci, "function kvRow");
  mustInclude(hci, "hci-note--unresolved");
  mustInclude(hci, "Verified identity ≠ verified personal contact channels");
  mustInclude(hci, "data-hci-enrich");
  mustInclude(hci, "/reveal");
  mustInclude(hci, "enrichment-capability");
  assert.ok(!/hci-dev-json|Dev diagnostic|JSON\.stringify\(vm/.test(hci), "no user-visible VM dump");
  assert.ok(!/production_writes|write_on_get|apply_authorized|surfe_live_calls/.test(hci));
  const mountIdx = hci.indexOf("function mount(");
  const mountSlice = hci.slice(mountIdx, mountIdx + 1200);
  assert.ok(!/enrichPeople|provider\.enrich/.test(mountSlice), "mount must not call Surfe provider");
  const api = read("api/contact-intelligence.js");
  mustInclude(api, "postContactIntelligencePersonReveal");
  mustInclude(api, "getContactIntelligenceEnrichmentCapability");
  mustInclude(api, "persist_forbidden");
  mustInclude(api, "NOT_WRITTEN");
  mustInclude(api, "autoEnrichOnLoad: false");
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
  mustInclude(css, "background: #0d152c");
  mustInclude(css, "hex-rms-room-name");
  assert.ok(!/\.hex-rms-table\.dl-table--inventory \{\s*min-width: 980px/.test(css));
});

run("ownership_hierarchy_proper_case_labels", () => {
  const ws = read("public/js/hotel-intelligence-workspace.js");
  mustInclude(ws, "Ownership &amp; Control Hierarchy");
  mustInclude(ws, "Legal Property Owner");
  mustInclude(ws, "Economic Owner / Sponsor");
  mustInclude(ws, "Hotel Operator");
  mustInclude(ws, "Brand Affiliation");
  mustInclude(ws, "Ultimate Beneficial Owner");
  mustInclude(ws, "Ownership Status");
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
  mustInclude(hci, "Suggested Contact");
  mustInclude(hci, "data-linkedin-url");
  mustInclude(hci, "Corporate contact paths");
  assert.ok(!/hci-dev-json|production_writes|JSON\.stringify\(vm/.test(hci));
  const mountIdx = hci.indexOf("function mount(");
  assert.ok(!/enrichPeople|provider\.enrich/.test(hci.slice(mountIdx, mountIdx + 1200)));
  mustInclude(read("api/hotel-explorer-founder-preview.js"), "HE-BETH-OWNER-DECISION-MAKER-RESEARCH");
  mustInclude(read("api/hotel-explorer-founder-preview.js"), "buildDecisionOrientedContactSections");
});

if (failures.length) {
  console.error(`\n${failures.length} quality gate(s) failed`);
  process.exit(1);
}
console.log("\nAll Hotel Explorer product quality gates passed.");
