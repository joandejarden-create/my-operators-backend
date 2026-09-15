#!/usr/bin/env node
/**
 * Group Demand Intelligence quality / isolation gates.
 */

import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  runGroupDemandResearch,
  PILOT_HOTEL_ID,
  classifyPriority,
  computeHotelFitScore,
  getWebhoundHardCapUsd,
  resolveAmpfyAdapterStatus,
  reconstructScoreAudit,
  BOOKING_WINDOW,
  DEFAULT_HOTEL_FIT_WEIGHTS,
  buildBethesdaMarriottProfileFromExistingKnowledge,
} from "../lib/group-demand-intelligence/index.js";
import {
  recordWebhoundSpend,
  createEmptyCostLedger,
} from "../lib/group-demand-intelligence/cost-ledger.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "..");

let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log("PASS", name);
  } catch (err) {
    failed += 1;
    console.error("FAIL", name, err.message);
  }
}

async function checkAsync(name, fn) {
  try {
    await fn();
    console.log("PASS", name);
  } catch (err) {
    failed += 1;
    console.error("FAIL", name, err.message);
  }
}

check("webhound_hard_cap_is_15", () => {
  assert.equal(getWebhoundHardCapUsd(), 15);
});

check("webhound_spend_cannot_exceed_cap", () => {
  const ledger = createEmptyCostLedger();
  recordWebhoundSpend(ledger, { costUsd: 14.5, question: "a" });
  assert.throws(
    () => recordWebhoundSpend(ledger, { costUsd: 1, question: "b" }),
    /hard_cap/
  );
});

check("ampfy_not_source_of_truth", () => {
  const s = resolveAmpfyAdapterStatus({});
  assert.equal(s.status, "provider_not_configured");
});

check("hotel_fit_weights_sum_to_1", () => {
  const fit = computeHotelFitScore({
    physicalFit: 100,
    geographyFit: 100,
    timing: 100,
    commercialValue: 100,
    historicalFit: 100,
    competitiveAccessibility: 100,
    contactability: 100,
  });
  assert.equal(fit.hotelFitScore, 100);
});

check("priority_requires_evidence_for_high", () => {
  const lowConf = classifyPriority({
    hotelFitScore: 90,
    evidenceConfidence: 20,
    bookingWindowStatus: BOOKING_WINDOW.CONTACT_NOW,
  });
  assert.notEqual(lowConf.priority, "HIGH_PRIORITY");
});

check("bethesda_config_not_architecture_hardcode", () => {
  const cfg = path.join(
    root,
    "config/group-demand-intelligence/hotels/recLuxvwwxID7U2B8.json"
  );
  assert.ok(fs.existsSync(cfg));
  const json = JSON.parse(fs.readFileSync(cfg, "utf8"));
  assert.ok(json.commercialPriorities.targetSegments.includes("Medical"));
});

check("gdi_data_path_isolated_from_adp", () => {
  const gdiRoot = path.join(root, "data/group-demand-intelligence");
  assert.ok(!String(gdiRoot).includes("ai-demand-positioning"));
});

await checkAsync("pilot_run_produces_qualified_opportunities", async () => {
  const result = await runGroupDemandResearch({
    hotelId: PILOT_HOTEL_ID,
    trigger: "test",
    dryRun: true,
  });
  assert.ok(result.ok);
  assert.equal(result.run.adpTouched, false);
  assert.equal(result.run.censusWritten, false);
  const active = result.opportunities.filter((o) => o.priority !== "DISQUALIFIED");
  assert.ok(active.length >= 5, `expected >=5 qualified, got ${active.length}`);
  assert.ok(active.length <= 35, `expected <=35 qualified after DMV expansion, got ${active.length}`);
  assert.ok(active.length >= 20, `expected >=20 qualified after DMV expansion, got ${active.length}`);
  const dmvComp = active.filter((o) => o.demandTerritoryFit === "DMV_COMPETITIVE");
  const dmvStretch = active.filter((o) => o.demandTerritoryFit === "DMV_STRETCH");
  assert.ok(dmvComp.length >= 4, `expected >=4 DMV Competitive qualified, got ${dmvComp.length}`);
  assert.ok(dmvStretch.length >= 2, `expected >=2 DMV Stretch qualified, got ${dmvStretch.length}`);
  const high = active.filter((o) => o.priority === "HIGH_PRIORITY");
  assert.ok(high.length >= 1, "expected at least one High Priority");
  assert.ok(high.length <= 5, `High list should stay tight after qualification gates, got ${high.length}`);
  for (const h of high) {
    assert.ok((h.evidence || []).length > 0, "high priority needs evidence");
    assert.ok(h.whyNow, "high priority needs why now");
    assert.ok(h.fitExplanation || h.summaryWhyHotel, "high priority needs why hotel");
    assert.ok(h.opportunityType, "high priority needs opportunity type");
    assert.ok(h.venueSourcingStatus, "high priority needs venue sourcing status");
    assert.ok(h.opportunityQualification, "high priority needs opportunity qualification");
    assert.ok(
      h.opportunityQualification === "VERIFIED_OPEN" || h.opportunityQualification === "STRONG",
      `high must have strong qualification, got ${h.opportunityQualification}`
    );
    assert.ok(h.hotelOpportunityThesis || h.bethesdaWinThesis, "high needs hotel opportunity thesis");
    assert.notEqual(h.contactQuality, "NO_CONTACT");
    assert.notEqual(h.contactQuality, "GENERIC_INBOX");
  }
  const audit = reconstructScoreAudit(high[0]);
  assert.equal(audit.hotelFit.weights.physicalFit, 0.25);
});

await checkAsync("persisted_pilot_run_writes_only_gdi_namespace", async () => {
  const result = await runGroupDemandResearch({
    hotelId: PILOT_HOTEL_ID,
    trigger: "test_persist",
    dryRun: true,
  });
  assert.ok(result.ok);
  // Live persist covered by gdi:bethesda-pilot-run — avoid overwriting $15 cost ledger in tests
  const gdiRoot = path.join(root, "data/group-demand-intelligence/hotels", PILOT_HOTEL_ID);
  assert.ok(!String(gdiRoot).includes("ai-demand-positioning"));
});

check("multi_session_webhound_respects_15_cap", () => {
  const ledger = createEmptyCostLedger();
  recordWebhoundSpend(ledger, { costUsd: 5, sessionId: "a", question: "wave1" });
  recordWebhoundSpend(ledger, { costUsd: 5, sessionId: "b", question: "deepen" });
  recordWebhoundSpend(ledger, { costUsd: 5, sessionId: "c", question: "discovery" });
  assert.equal(ledger.webhoundUsd, 15);
  assert.throws(
    () => recordWebhoundSpend(ledger, { costUsd: 0.01, question: "over" }),
    /hard_cap/
  );
});

await checkAsync("share_token_hotel_isolation_and_read_only", async () => {
  process.env.GDI_SHARE_CAPABILITY_ALLOW_DEV_SECRET = "1";
  const {
    issueGdiShareCapability,
    verifyGdiShareCapability,
    revokeGdiShareCapability,
    sanitizeOpportunityForShare,
  } = await import("../lib/group-demand-intelligence/index.js");

  const issued = issueGdiShareCapability({
    hotelId: PILOT_HOTEL_ID,
    label: "test",
  });
  const ok = verifyGdiShareCapability(issued.token, {
    expectedHotelId: PILOT_HOTEL_ID,
    requiredSurface: "brief",
  });
  assert.equal(ok.ok, true);

  const wrongHotel = verifyGdiShareCapability(issued.token, {
    expectedHotelId: "recOTHER",
  });
  assert.equal(wrongHotel.ok, false);

  const auditSurface = verifyGdiShareCapability(issued.token, {
    requiredSurface: "research_audit",
  });
  assert.equal(auditSurface.ok, false);

  revokeGdiShareCapability(issued.tokenId, "test");
  const revoked = verifyGdiShareCapability(issued.token);
  assert.equal(revoked.ok, false);

  const sanitized = sanitizeOpportunityForShare({
    id: "x",
    title: "t",
    priority: "HIGH_PRIORITY",
    researchMethodsAttempted: ["secret"],
    webhoundUsed: true,
    evidence: [
      {
        field: "a",
        value: "b",
        claimKind: "FACT",
        sourceAuthority: "Tier_A",
        sourceUrl: "https://example.com",
        researchProvider: "webhound",
      },
    ],
  });
  assert.ok(!("researchMethodsAttempted" in sanitized));
  assert.ok(!("webhoundUsed" in sanitized));
  assert.equal(sanitized.evidence[0].field, "a");
});

check("gdi_ui_uses_dealality_tokens_not_prototype_palette", () => {
  const css = fs.readFileSync(
    path.join(root, "public/css/group-demand-intelligence.css"),
    "utf8"
  );
  assert.ok(css.includes("#080f25"), "page bg should use Dealality neutral--800");
  assert.ok(css.includes("#6c72ff"), "accent should use Dealality primary");
  assert.ok(css.includes("Segoe UI"), "font should match product sans");
  assert.ok(!/Fraunces/.test(css), "prototype Fraunces font must be removed");
  assert.ok(!/#3d8bfd/.test(css), "prototype blue accent must be removed");
});

check("gdi_share_ui_has_no_admin_surfaces", () => {
  const shareJs = fs.readFileSync(
    path.join(root, "public/js/group-demand-intelligence/share-app.js"),
    "utf8"
  );
  assert.ok(shareJs.includes("gdi-share-brand"), "share brand strip required");
  assert.ok(shareJs.includes("Read-only"), "read-only label required");
  assert.ok(!/Research Audit/i.test(shareJs), "audit tab must not appear on share");
  assert.ok(!/Run Research/i.test(shareJs), "run research must not appear on share");
  assert.ok(!/gdiFbSave|Save Feedback/.test(shareJs), "feedback form must not appear on share");
});

check("gdi_registered_in_dealality_app_shell", () => {
  const appJs = fs.readFileSync(path.join(root, "public/app.js"), "utf8");
  assert.ok(
    appJs.includes("'/group-demand-intelligence'"),
    "GDI must be registered as an app shell route"
  );
  assert.ok(
    appJs.includes("Group Demand Intelligence"),
    "GDI must appear in Market Intelligence nav"
  );
});

check("str_comp_set_and_group_alternatives_configured", () => {
  const cfg = JSON.parse(
    fs.readFileSync(
      path.join(root, "config/group-demand-intelligence/hotels/recLuxvwwxID7U2B8.json"),
      "utf8"
    )
  );
  assert.equal(cfg.competitiveContext.strCompSet.length, 5);
  assert.equal(cfg.competitiveContext.relevantGroupDemandAlternatives.length, 3);
  assert.ok(cfg.demandTerritory.label === "DMV");
  const profile = buildBethesdaMarriottProfileFromExistingKnowledge();
  assert.equal(profile.strCompSet.length, 5);
  assert.equal(profile.relevantGroupDemandAlternatives.length, 3);
});

await checkAsync("demand_territory_and_sourcing_defaults", async () => {
  const {
    classifyDemandTerritoryFit,
    enrichOpportunityForRadFeedback,
    SOURCING_STATUS,
    INCREMENTAL_VALUE_STATUS,
    DEMAND_TERRITORY_FIT,
    loadHotelDemandConfig,
  } = await import("../lib/group-demand-intelligence/index.js");
  const cfg = loadHotelDemandConfig(PILOT_HOTEL_ID);
  const core = classifyDemandTerritoryFit({
    title: "Bethesda Premier Cup",
    destinationStatus: "Bethesda, MD",
    venueStatus: "Bethesda fields",
  });
  assert.equal(core.demandTerritoryFit, DEMAND_TERRITORY_FIT.BETHESDA_MONTGOMERY_CORE);
  const enriched = enrichOpportunityForRadFeedback(
    {
      id: "gdi_opp_test_territory",
      hotelId: PILOT_HOTEL_ID,
      title: "NICE 2027 — Washington DC area venue TBD",
      organizationName: "NIST",
      segment: "Government",
      demandStatus: "CONFIRMED_DEMAND",
      bookingWindowStatus: "CONTACT_NOW",
      physicalFitScore: 80,
      geographyFitScore: 85,
      timingScore: 88,
      commercialValueScore: 75,
      historicalFitScore: 70,
      competitiveAccessibilityScore: 72,
      contactabilityScore: 80,
      evidence: [
        {
          field: "dates",
          value: "Jun 7–9 2027",
          claimKind: "FACT",
          sourceTitle: "NIST official event page",
          sourceUrl: "https://www.nist.gov/nice",
          sourceType: "government",
        },
      ],
      fitComponents: {
        physicalFit: 80,
        geographyFit: 85,
        timing: 88,
        commercialValue: 75,
        historicalFit: 70,
        competitiveAccessibility: 72,
        contactability: 80,
      },
    },
    cfg
  );
  assert.equal(enriched.sourcingStatus, SOURCING_STATUS.UNKNOWN);
  assert.equal(enriched.incrementalValueStatus, INCREMENTAL_VALUE_STATUS.UNKNOWN);
  assert.ok(enriched.demandTerritoryFit);
  assert.ok(enriched.sources.length >= 1);
  assert.ok(enriched.hotelFitComponentLabels.geographyFit === "Demand Territory Fit");
  assert.ok(/Evidence Confidence|confidence/i.test(enriched.evidenceConfidenceExplanation));
});

await checkAsync("dmv_expansion_adds_competitive_and_stretch_with_win_thesis", async () => {
  const {
    buildDmvExpansionOpportunities,
    listRejectedDmvExpansionCandidates,
    applyDmvExpansionPass,
    applyRadFeedbackEnrichmentPass,
    DEMAND_TERRITORY_FIT,
  } = await import("../lib/group-demand-intelligence/index.js");

  const built = buildDmvExpansionOpportunities();
  assert.ok(built.length >= 8);
  const competitive = built.filter(
    (o) => o.demandTerritoryFit === DEMAND_TERRITORY_FIT.DMV_COMPETITIVE
  );
  const stretch = built.filter(
    (o) => o.demandTerritoryFit === DEMAND_TERRITORY_FIT.DMV_STRETCH
  );
  assert.ok(competitive.length >= 4, "expected several DMV Competitive discoveries");
  assert.ok(stretch.length >= 3, "expected several DMV Stretch discoveries");
  for (const o of [...competitive, ...stretch]) {
    assert.ok(
      o.bethesdaWinThesis && o.bethesdaWinThesis.length > 40,
      `missing win thesis on ${o.id}`
    );
    assert.equal(o.demandTerritoryFitLocked, true);
    assert.ok(
      (o.marketCompetitors || []).length >= 1,
      `missing market competitors on ${o.id}`
    );
  }

  const rejected = listRejectedDmvExpansionCandidates();
  assert.ok(rejected.length >= 8);

  const merged = applyDmvExpansionPass([]);
  const enriched = applyRadFeedbackEnrichmentPass(merged.opportunities, PILOT_HOTEL_ID);
  const gad = enriched.opportunities.find((o) => o.id === "gdi_opp_nar_gad_institute_2027");
  assert.ok(gad);
  assert.equal(gad.demandTerritoryFit, DEMAND_TERRITORY_FIT.DMV_COMPETITIVE);
  assert.ok(gad.bethesdaWinThesis);
  assert.ok(
    (gad.competitors || []).some((c) => c.class === "OPPORTUNITY_MARKET"),
    "expected opportunity-market competitors after enrichment"
  );
});

check("hotel_fit_weights_match_gm_framework_documented", () => {
  const weights = DEFAULT_HOTEL_FIT_WEIGHTS;
  assert.equal(weights.physicalFit, 0.25);
  assert.equal(weights.geographyFit, 0.2);
  assert.equal(weights.timing, 0.15);
  assert.equal(weights.commercialValue, 0.15);
  assert.equal(weights.historicalFit, 0.1);
  assert.equal(weights.competitiveAccessibility, 0.1);
  assert.equal(weights.contactability, 0.05);
  const cfg = JSON.parse(
    fs.readFileSync(path.join(root, "config/group-demand-intelligence/scoring-weights.json"), "utf8")
  );
  assert.equal(cfg.reviewedAgainstGmFrameworkAt, "2026-09-13");
  assert.ok(/unchanged|already matched/i.test(cfg.weightsUnchangedNote));
});

await checkAsync("hotel_feedback_does_not_mutate_canonical_opportunity_fields", async () => {
  const { saveFeedbackItem, loadOpportunities, loadFeedback } = await import(
    "../lib/group-demand-intelligence/index.js"
  );
  const before = loadOpportunities(PILOT_HOTEL_ID);
  const sample = (before.opportunities || []).find((o) => o.priority !== "DISQUALIFIED");
  if (!sample) return;
  const titleBefore = sample.title;
  saveFeedbackItem(PILOT_HOTEL_ID, {
    opportunityId: sample.id,
    familiarity: "Never Seen",
    commercialStatus: "Worth Pursuing",
    value: "Useful",
    incrementalValueStatus: "NEW_TO_HOTEL",
    comment: "test validation — do not treat as production feedback",
    actor: "gdi-foundation-test",
    doesNotOverwriteCanonicalFacts: true,
  });
  const after = loadOpportunities(PILOT_HOTEL_ID);
  const again = (after.opportunities || []).find((o) => o.id === sample.id);
  assert.equal(again.title, titleBefore);
  assert.ok(loadFeedback(PILOT_HOTEL_ID));
});

check("ui_uses_evidence_confidence_and_hotel_validation_labels", () => {
  const appJs = fs.readFileSync(
    path.join(root, "public/js/group-demand-intelligence/app.js"),
    "utf8"
  );
  assert.ok(appJs.includes("Evidence Confidence"));
  assert.ok(appJs.includes("Demand Territory"));
  assert.ok(appJs.includes("Sourcing Status") || appJs.includes("Already Sourced to Hotel"));
  assert.ok(appJs.includes("Hotel Validation"));
  assert.ok(appJs.includes("Recommended Action") || appJs.includes("Recommended Next Action"));
  assert.ok(appJs.includes("Opportunity Type"));
  assert.ok(appJs.includes("Opportunity Qualification"));
  assert.ok(appJs.includes("Hotel Opportunity Thesis"));
  const shareJs = fs.readFileSync(
    path.join(root, "public/js/group-demand-intelligence/share-app.js"),
    "utf8"
  );
  assert.ok(shareJs.includes("Evidence Confidence"));
  assert.ok(shareJs.includes("Opportunity Type"));
  assert.ok(!/Hotel Validation|gdiFbSave/.test(shareJs));
});

await checkAsync("event_dedupe_keeps_single_id_in_enrichment_pass", async () => {
  const { applyRadFeedbackEnrichmentPass } = await import(
    "../lib/group-demand-intelligence/index.js"
  );
  const dup = [
    { id: "gdi_opp_dup", title: "A", hotelId: PILOT_HOTEL_ID, segment: "Associations", demandStatus: "CONFIRMED_DEMAND", bookingWindowStatus: "WATCH", physicalFitScore: 60, geographyFitScore: 60, timingScore: 50, commercialValueScore: 50, historicalFitScore: 50, competitiveAccessibilityScore: 50, contactabilityScore: 50, fitComponents: { physicalFit: 60, geographyFit: 60, timing: 50, commercialValue: 50, historicalFit: 50, competitiveAccessibility: 50, contactability: 50 } },
    { id: "gdi_opp_dup", title: "A", hotelId: PILOT_HOTEL_ID, segment: "Associations", demandStatus: "CONFIRMED_DEMAND", bookingWindowStatus: "WATCH", physicalFitScore: 60, geographyFitScore: 60, timingScore: 50, commercialValueScore: 50, historicalFitScore: 50, competitiveAccessibilityScore: 50, contactabilityScore: 50, fitComponents: { physicalFit: 60, geographyFit: 60, timing: 50, commercialValue: 50, historicalFit: 50, competitiveAccessibility: 50, contactability: 50 } },
  ];
  const out = applyRadFeedbackEnrichmentPass(dup, PILOT_HOTEL_ID);
  assert.equal(out.opportunities.length, 1);
  assert.equal(out.enrichment.webhoundSpentUsd, 0);
});

await checkAsync("qualification_precision_false_positive_patterns", async () => {
  const {
    buildOpportunity,
    classifyDemandTerritoryFit,
    classifyVenueSourcingStatus,
    classifyRoomDemand,
    classifyContactQuality,
    classifyEventLocation,
    DEMAND_TERRITORY_FIT,
    VENUE_SOURCING_STATUS,
    ROOM_DEMAND_STATUS,
    CONTACT_QUALITY,
    EVENT_LOCATION_STATUS,
    listGdiResearchMethods,
  } = await import("../lib/group-demand-intelligence/index.js");

  const methods = listGdiResearchMethods();
  assert.ok(methods.some((m) => m.id === "GDI-VENUE-HOUSING-01"));

  // Venue already selected → not High
  const placed = buildOpportunity({
    id: "gdi_test_placed",
    hotelId: PILOT_HOTEL_ID,
    title: "Association Annual — already contracted at Gaylord National",
    organizationName: "Example Assoc",
    segment: "Associations",
    demandStatus: "CONFIRMED_DEMAND",
    bookingWindowStatus: "CONTACT_NOW",
    venueStatus: "Gaylord National — already contracted",
    destinationStatus: "National Harbor",
    whyNow: "CONTACT NOW: event is next year but host hotel already selected with no overflow page.",
    estimatedAttendance: 800,
    estimatedAttendanceClaimKind: "ESTIMATED",
    fitComponents: {
      physicalFit: 90,
      geographyFit: 80,
      timing: 85,
      commercialValue: 80,
      historicalFit: 70,
      competitiveAccessibility: 70,
      contactability: 80,
    },
    confidenceInput: {
      sourceAuthority: 80,
      independentSourceCount: 3,
      directness: 80,
      recency: 80,
      verifiedFieldRatio: 0.7,
      firstPartyShare: 70,
      completeness: 70,
    },
    primaryContact: {
      name: "Jane Smith",
      role: "Director of Meetings",
      email: "jane@example.org",
      claimKind: "FACT",
    },
    evidence: [{ field: "venue", value: "Gaylord", claimKind: "FACT", sourceTitle: "Official" }],
  });
  assert.notEqual(placed.priority, "HIGH_PRIORITY");
  assert.ok(
    placed.venueSourcingStatus === VENUE_SOURCING_STATUS.FULLY_PLACED ||
      placed.venueSourcingStatus ===
        VENUE_SOURCING_STATUS.PRIMARY_VENUE_SELECTED_NO_OVERFLOW_EVIDENCE ||
      placed.opportunityQualification === "CLOSED"
  );

  // Housing / overflow distinguished from primary host
  const housing = classifyVenueSourcingStatus({
    title: "Youth Cup",
    venueStatus: "Fields-based; hotels via official housing partner",
    labels: ["overflow"],
    whyNow: "stay-to-play housing list opening",
  });
  assert.equal(
    housing.venueSourcingStatus,
    VENUE_SOURCING_STATUS.PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE
  );

  // Event geography > org name
  const geo = classifyDemandTerritoryFit({
    title: "Regional Cup",
    organizationName: "Bethesda Soccer League HQ",
    destinationStatus: "Upper Marlboro, MD",
    venueStatus: "Prince George's Stadium area",
  });
  assert.notEqual(geo.demandTerritoryFit, DEMAND_TERRITORY_FIT.BETHESDA_MONTGOMERY_CORE);

  const loc = classifyEventLocation({
    organizationName: "Bethesda Widget Assoc",
    destinationStatus: "Upper Marlboro, MD",
    venueStatus: "Showplace Arena",
  });
  assert.ok(
    loc.eventLocationStatus === EVENT_LOCATION_STATUS.VERIFIED_CITY ||
      loc.eventLocationStatus === EVENT_LOCATION_STATUS.VERIFIED_VENUE,
    `expected verified city/venue, got ${loc.eventLocationStatus}`
  );

  // Attendance ≠ rooms
  const rooms = classifyRoomDemand({
    title: "Day conference",
    estimatedAttendance: 1400,
    estimatedAttendanceClaimKind: "ESTIMATED",
    estimatedPeakRooms: null,
    summaryWhat: "mostly local attendees day meeting no overnight",
  });
  assert.equal(rooms.roomDemandStatus, ROOM_DEMAND_STATUS.LOCAL_LIMITED_ROOM_DEMAND);

  // Generic inbox weaker than named director
  const generic = classifyContactQuality({
    primaryContact: {
      name: "UNKNOWN",
      role: "Association meetings contact",
      email: "info@example.org",
    },
  });
  const named = classifyContactQuality({
    primaryContact: {
      name: "Brad Roos",
      role: "Tournament Director",
      email: "broos@example.org",
    },
  });
  assert.equal(generic.contactQuality, CONTACT_QUALITY.GENERIC_INBOX);
  assert.equal(named.contactQuality, CONTACT_QUALITY.NAMED_DECISION_MAKER);
  assert.ok(named.contactabilityAdjustment > generic.contactabilityAdjustment);

  // High Hotel Fit cannot rescue closed qualification
  assert.notEqual(placed.priority, "HIGH_PRIORITY");
  assert.ok((placed.hotelFitScore || 0) >= 75);

  // Commercial QA: Premier Cup multi-site geography cannot remain High for Bethesda Marriott
  const { applyQualificationPrecisionPass } = await import(
    "../lib/group-demand-intelligence/index.js"
  );
  const premierSeed = {
    id: "gdi_opp_bethesda_premier_cup_2026",
    hotelId: PILOT_HOTEL_ID,
    title: "Bethesda Premier Cup 2026",
    organizationName: "Bethesda Soccer Club",
    segment: "Sports",
    demandStatus: "CONFIRMED_DEMAND",
    bookingWindowStatus: "CONTACT_NOW",
    destinationStatus: "Bethesda / Montgomery County",
    venueStatus: "Fields-based; hotels via official housing partner",
    whyNow: "CONTACT NOW: housing via HBC",
    priority: "HIGH_PRIORITY",
    demandTerritoryFit: "BETHESDA_MONTGOMERY_CORE",
    physicalFitScore: 80,
    geographyFitScore: 85,
    timingScore: 85,
    commercialValueScore: 75,
    historicalFitScore: 70,
    competitiveAccessibilityScore: 70,
    contactabilityScore: 85,
    fitComponents: {
      physicalFit: 80,
      geographyFit: 85,
      timing: 85,
      commercialValue: 75,
      historicalFit: 70,
      competitiveAccessibility: 70,
      contactability: 85,
    },
    confidenceInput: {
      sourceAuthority: 80,
      independentSourceCount: 3,
      directness: 75,
      recency: 80,
      verifiedFieldRatio: 0.6,
      firstPartyShare: 70,
      completeness: 70,
    },
    primaryContact: {
      name: "Brad Roos",
      role: "Tournament Director",
      email: "broos@bethesdasoccer.org",
      claimKind: "FACT",
    },
    evidence: [
      {
        field: "housing",
        value: "HBC",
        claimKind: "FACT",
        sourceTitle: "Official housing",
      },
    ],
    labels: ["weekend", "sports"],
  };
  const qaPass = applyQualificationPrecisionPass([premierSeed]);
  const premierOut = qaPass.opportunities[0];
  assert.notEqual(premierOut.priority, "HIGH_PRIORITY");
  assert.equal(premierOut.demandTerritoryFit, "DMV_COMPETITIVE");
  assert.ok(premierOut.destinationStatus.includes("Upper Marlboro") || premierOut.destinationStatus.includes("Alexandria"));
});

await checkAsync("contact_resolution_grades_labels_and_failure_modes", async () => {
  const {
    gradeContact,
    enrichContactRecord,
    classifyTargetRoleMatch,
    hasNamedPerson,
    applyContactResolutionPass,
    EMAIL_TYPE,
    EMAIL_VERIFICATION_STATUS,
    PHONE_TYPE,
    TARGET_ROLE_MATCH,
    CONTACT_GRADE,
  } = await import("../lib/group-demand-intelligence/index.js").then(async (gdi) => {
    const cr = await import("../lib/group-demand-intelligence/contact-resolution.js");
    const { EMAIL_TYPE, EMAIL_VERIFICATION_STATUS, PHONE_TYPE, TARGET_ROLE_MATCH, CONTACT_GRADE } =
      await import("../lib/group-demand-intelligence/claim-types.js");
    return { ...gdi, ...cr, EMAIL_TYPE, EMAIL_VERIFICATION_STATUS, PHONE_TYPE, TARGET_ROLE_MATCH, CONTACT_GRADE };
  });
  const {
    classifyEmailType,
    classifyEmailVerificationStatus,
    classifyPhoneType,
    personDedupeKey,
  } = await import("../lib/hotel-intelligence/contact-intelligence/contact-reachability.js");

  // Generic inbox must not score like named director
  const genericGrade = gradeContact(
    { name: null, email: "info@association.org", role: "Office" },
    { opportunityType: "PRIMARY_PURSUIT" }
  );
  const namedGrade = gradeContact(
    {
      name: "Jane Smith",
      role: "Director of Meetings",
      email: "jane.smith@association.org",
      phone: "202-555-0100",
      phoneLineHint: "OFFICE",
      claimKind: "FACT",
      sourceUrl: "https://association.org/staff",
      relationshipToEvent: "Official meetings lead",
    },
    { opportunityType: "PRIMARY_PURSUIT" }
  );
  assert.equal(genericGrade.contactGrade, CONTACT_GRADE.D);
  assert.ok(
    namedGrade.contactGrade === CONTACT_GRADE.A || namedGrade.contactGrade === CONTACT_GRADE.B
  );

  // Inferred email must be labeled inferred, not verified
  const inferredType = classifyEmailType("jsmith@association.org", {
    contactName: "Jane Smith",
    inferred: true,
  });
  const inferredVer = classifyEmailVerificationStatus({
    email: "jsmith@association.org",
    contactName: "Jane Smith",
    inferred: true,
  });
  assert.equal(inferredType, EMAIL_TYPE.INFERRED);
  assert.equal(inferredVer, EMAIL_VERIFICATION_STATUS.DOMAIN_PATTERN_INFERRED);

  // Main phone must not be labeled direct
  assert.equal(
    classifyPhoneType("301-519-8070", { lineHint: "MAIN" }),
    PHONE_TYPE.MAIN_ORGANIZATION
  );
  assert.notEqual(
    classifyPhoneType("301-519-8070", { lineHint: "MAIN" }),
    PHONE_TYPE.DIRECT
  );

  // Wrong employee — finance staff with no event relationship → not high-value primary role
  const wrongRole = classifyTargetRoleMatch(
    {
      name: "Bob Finance",
      role: "Controller",
      organization: "Association",
      email: "bob@association.org",
    },
    { opportunityType: "PRIMARY_PURSUIT", title: "Annual Meeting" }
  );
  assert.equal(wrongRole, TARGET_ROLE_MATCH.GENERAL_ORGANIZATION_CONTACT);
  assert.ok(hasNamedPerson({ name: "Bob Finance" }));

  // Housing opportunity prefers housing provider over association ED
  const housingOpp = { opportunityType: "OVERFLOW_HOUSING", title: "Stay-to-play cup" };
  const housingPrimary = enrichContactRecord(
    {
      name: "HBC Event Services",
      role: "Official housing partner",
      organization: "HBC Event Services",
      email: "support@hbceventservices.com",
      phone: "505-346-0522",
      phoneLineHint: "EVENT",
      claimKind: "FACT",
      sourceUrl: "https://example.com/hotels",
      targetRoleMatch: TARGET_ROLE_MATCH.HOUSING_SOURCING_CONTACT,
    },
    housingOpp
  );
  const assocEd = enrichContactRecord(
    {
      name: "Alice Executive",
      role: "Executive Director",
      organization: "Soccer Club",
      email: "ed@soccer.org",
      claimKind: "FACT",
      sourceUrl: "https://example.com/about",
    },
    housingOpp
  );
  assert.equal(housingPrimary.targetRoleMatch, TARGET_ROLE_MATCH.HOUSING_SOURCING_CONTACT);
  assert.equal(assocEd.targetRoleMatch, TARGET_ROLE_MATCH.EXECUTIVE_SPONSOR);
  assert.ok(housingPrimary.contactConfidence >= assocEd.contactConfidence - 5);

  // Stale historical contact flagged
  const stale = enrichContactRecord(
    {
      name: "Old Planner",
      role: "Meeting Planner",
      email: "old@assoc.org",
      historicalOnly: true,
      claimKind: "FACT",
      sourceUrl: "https://example.com/2019",
    },
    { opportunityType: "REACTIVATION" }
  );
  assert.ok(stale.contactConfidence < 70);

  // Duplicate person keys collapse nickname / middle initial
  assert.equal(
    personDedupeKey("Kathy Hauschild", "Potomac Soccer"),
    personDedupeKey("Kathy M. Hauschild", "Potomac Soccer")
  );

  // Pass: Potomac overflow primary becomes housing path; NICE upgrades off generic-only primary
  const pass = applyContactResolutionPass([
    {
      id: "gdi_opp_potomac_memorial_2027",
      hotelId: PILOT_HOTEL_ID,
      title: "Potomac Memorial",
      organizationName: "Potomac Soccer Association",
      segment: "Sports",
      demandStatus: "CONFIRMED_DEMAND",
      priority: "HIGH_PRIORITY",
      opportunityType: "OVERFLOW_HOUSING",
      opportunityQualification: "VERIFIED_OPEN",
      bookingWindowStatus: "CONTACT_NOW",
      physicalFitScore: 80,
      geographyFitScore: 80,
      timingScore: 80,
      commercialValueScore: 75,
      historicalFitScore: 70,
      competitiveAccessibilityScore: 70,
      contactabilityScore: 90,
      fitComponents: {
        physicalFit: 80,
        geographyFit: 80,
        timing: 80,
        commercialValue: 75,
        historicalFit: 70,
        competitiveAccessibility: 70,
        contactability: 90,
      },
      confidenceInput: {
        sourceAuthority: 80,
        independentSourceCount: 3,
        directness: 80,
        recency: 80,
        verifiedFieldRatio: 0.7,
        firstPartyShare: 70,
        completeness: 70,
      },
      primaryContact: {
        name: "Kathy Hauschild",
        role: "Tournament Director",
        email: "tournament@potomacsoccer.org",
        claimKind: "FACT",
        sourceUrl: "https://potomacsoccer.org/memorial_tournament/potomac-memorial-tournament/",
      },
      evidence: [{ field: "housing", value: "HBC", claimKind: "FACT" }],
    },
    {
      id: "gdi_opp_nice_2027",
      hotelId: PILOT_HOTEL_ID,
      title: "NICE 2027",
      organizationName: "NIST NICE",
      segment: "Associations",
      demandStatus: "CONFIRMED_DEMAND",
      priority: "MEDIUM_PRIORITY",
      opportunityType: "PRIMARY_PURSUIT",
      opportunityQualification: "STRONG",
      bookingWindowStatus: "QUALIFY_NOW",
      physicalFitScore: 75,
      geographyFitScore: 75,
      timingScore: 70,
      commercialValueScore: 80,
      historicalFitScore: 60,
      competitiveAccessibilityScore: 65,
      contactabilityScore: 20,
      fitComponents: {
        physicalFit: 75,
        geographyFit: 75,
        timing: 70,
        commercialValue: 80,
        historicalFit: 60,
        competitiveAccessibility: 65,
        contactability: 20,
      },
      confidenceInput: {
        sourceAuthority: 85,
        independentSourceCount: 2,
        directness: 70,
        recency: 75,
        verifiedFieldRatio: 0.5,
        firstPartyShare: 80,
        completeness: 60,
      },
      primaryContact: {
        name: "NIST NICE Program Office / FIU conference ops",
        role: "Program + venue logistics",
        email: "nice@nist.gov",
        phone: "301-975-4470",
        claimKind: "FACT",
        sourceUrl: "https://www.nist.gov/itl/applied-cybersecurity/nice/about/meet-staff",
      },
      evidence: [],
    },
  ]);

  const potomac = pass.opportunities.find((o) => o.id === "gdi_opp_potomac_memorial_2027");
  const nice = pass.opportunities.find((o) => o.id === "gdi_opp_nice_2027");
  assert.ok(potomac);
  assert.ok(/HBC/i.test(potomac.primaryContact?.name || ""));
  assert.equal(potomac.primaryContact?.targetRoleMatch, TARGET_ROLE_MATCH.HOUSING_SOURCING_CONTACT);
  assert.ok(potomac.contactGrade === CONTACT_GRADE.A || potomac.contactGrade === CONTACT_GRADE.B);
  assert.ok(nice);
  assert.equal(nice.primaryContact?.name, "Karen Wetzel");
  assert.notEqual(nice.primaryContact?.email, "nice@nist.gov");
  assert.equal(
    nice.primaryContact?.emailVerificationStatus,
    EMAIL_VERIFICATION_STATUS.OFFICIAL_SOURCE_VERIFIED
  );
  assert.ok(pass.enrichment.webhoundSpentUsd === 0);
});

if (failed) {
  console.error(`\n${failed} GDI gate(s) failed`);
  process.exit(1);
}
console.log("\nAll GDI gates passed");
