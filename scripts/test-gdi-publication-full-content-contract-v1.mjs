#!/usr/bin/env node
/**
 * Publication full-content contract gate tests.
 * Covers: new publication reject lean snap, enriched snap accept,
 * hydrate+share sanitize retention, registry reload simulation.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  buildCustomerFacingPublishedSnapshot,
  validateCustomerFacingPublishedSnapshot,
  hydrateOpportunityFromPublishedCommercialFields,
} from "../lib/group-demand-intelligence/durable-publication/gdi-customer-facing-published-snapshot-v1.js";
import { createGdiPublishedInventoryRegistry } from "../lib/group-demand-intelligence/durable-publication/gdi-published-inventory-registry-v1.js";
import { sanitizeOpportunityForShare } from "../lib/group-demand-intelligence/share/gdi-signed-share-capability-v1.js";
import { sanitizeOpportunityForCustomerPresentation } from "../lib/group-demand-intelligence/customer-presentation-sanitize-v1.js";

const HOTEL = "recTESTFULLCONTENT01";
const OPP = "gdi_test_full_content_001";

function sampleSource() {
  return {
    id: OPP,
    hotelId: HOTEL,
    title: "Destination weddings and private celebrations",
    organizationName: "Hangers Weddings & Events",
    segment: "Social Groups & Special Events",
    researchSegment: "social_special_events",
    whatIsHappening:
      "Active Grenada full-service planner for bespoke weddings and private events.",
    whyThisHotel:
      "Qualified planner-channel prospect for intimate luxury destination weddings.",
    whyNow: "No time-critical sourcing trigger is established. Monitor until dates confirm.",
    establishedFacts: [
      "Tourism authority lists the planner.",
      "Official about page names the founder.",
    ],
    uncertainties: ["No future programme date verified."],
    qualificationQuestions: [
      "Which future weddings still need guest accommodation?",
    ],
    recommendedNextStep:
      "Send a concise B2B partnership enquiry to the organizational inbox.",
    primaryContact: {
      name: "Khaleen Johnson",
      title: "Founder and principal event planner",
      organization: "Hangers Weddings & Events",
      whyRelevant: "Verified owner/principal planner.",
    },
    secondaryContact: {
      name: "Aviann Murray",
      title: "Event Confirmation / Routing Contact",
      organization: "Hangers Weddings and Events",
      whyRelevant: "Administrative/event coordination routing candidate.",
    },
    sources: [
      {
        url: "https://www.hangersgrenada.com/",
        title: "Planner service scope",
      },
    ],
    commercialIntelligenceBrief: {
      whatIsHappening:
        "Active Grenada full-service planner for bespoke weddings and private events.",
      potentialBusinessForThisHotel:
        "Intimate weddings, couples stays, honeymoon extensions and private celebrations.",
      whyThisHotel:
        "Qualified planner-channel prospect for intimate luxury destination weddings.",
      whyActNow:
        "No time-critical sourcing trigger is established. Monitor until dates confirm.",
      establishedFactsAndUncertainties: {
        facts: [
          "Tourism authority lists the planner.",
          "Official about page names the founder.",
        ],
        uncertainties: ["No future programme date verified."],
      },
      qualificationQuestions: [
        "Which future weddings still need guest accommodation?",
      ],
      whoToContact: {
        primary: {
          name: "Khaleen Johnson",
          title: "Founder",
          whyRelevant: "Owner",
        },
        secondary: {
          name: "Aviann Murray",
          title: "Coordinator",
          whyRelevant: "Routing",
        },
      },
      recommendedNextStep:
        "Send a concise B2B partnership enquiry to the organizational inbox.",
    },
  };
}

function run() {
  const leanGate = validateCustomerFacingPublishedSnapshot({
    id: OPP,
    title: "Hangers Weddings & Events",
    whatIsHappening: "Hangers Weddings & Events",
    whyThisHotel: "fit text",
    recommendedNextStep: "call them",
    primaryContact: { name: "Khaleen" },
    sources: [{ url: "https://example.com" }],
  });
  assert.equal(leanGate.ok, false, "lean snap must fail gate");
  assert.ok(
    leanGate.fails.some((f) => f.includes("whatIsHappening")),
    "must reject title-as-what",
  );

  const source = sampleSource();
  const snap = buildCustomerFacingPublishedSnapshot(source, {
    hotelId: HOTEL,
    canonicalTitle: "Hangers Weddings & Events",
    classification: "ACCOUNT_DEVELOPMENT",
  });
  const gate = validateCustomerFacingPublishedSnapshot(snap);
  assert.equal(gate.ok, true, `enriched must pass: ${gate.fails.join(",")}`);
  assert.ok(snap.potentialHotelBusiness);
  assert.ok(snap.establishedFacts.length >= 2);
  assert.ok(snap.secondaryContact?.name);
  assert.ok(snap.qualificationQuestions.length >= 1);
  assert.ok(snap.commercialIntelligenceBrief);

  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-pub-full-"));
  const registry = createGdiPublishedInventoryRegistry({
    root: tmp,
    env: { GDI_PUBLISHED_INVENTORY_ROOT: tmp },
  });

  // New publication
  const pub = registry.publishOpportunity({
    hotelId: HOTEL,
    opportunityId: OPP,
    classification: "ACCOUNT_DEVELOPMENT",
    snapshot: snap,
    approval: {
      authorized: true,
      authorizedBy: "test",
      authorizedAt: new Date().toISOString(),
      note: "unit_test",
    },
  });
  assert.equal(pub.ok, true);

  // Lean publish must throw
  let leanBlocked = false;
  try {
    registry.publishOpportunity({
      hotelId: HOTEL,
      opportunityId: "gdi_test_lean_blocked",
      classification: "ACCOUNT_DEVELOPMENT",
      snapshot: {
        id: "gdi_test_lean_blocked",
        title: "Acme",
        whatIsHappening: "Acme",
        whyThisHotel: "x",
        recommendedNextStep: "y",
        primaryContact: { name: "A" },
        sources: [{ url: "https://example.com" }],
      },
      approval: {
        authorized: true,
        authorizedBy: "test",
        authorizedAt: new Date().toISOString(),
        note: "should_fail_gate",
      },
    });
  } catch (err) {
    leanBlocked = err.code === "PUBLICATION_CONTENT_GATE_FAILED";
  }
  assert.equal(leanBlocked, true, "lean publish must be blocked by gate");

  // Reload registry (process restart simulation)
  const registry2 = createGdiPublishedInventoryRegistry({
    root: tmp,
    env: { GDI_PUBLISHED_INVENTORY_ROOT: tmp },
  });
  const loaded = registry2.loadSnapshot(HOTEL, OPP, pub.row.publishedVersion);
  const loadedSnap = loaded.snapshot || loaded;
  assert.ok(loadedSnap.whatIsHappening.includes("full-service planner"));
  assert.ok(loadedSnap.potentialHotelBusiness);

  // Incremental update
  const nextSnap = {
    ...snap,
    whyActNow: "Relationship window remains open; no deadline urgency.",
  };
  const upd = registry2.updatePublishedOpportunity({
    hotelId: HOTEL,
    opportunityId: OPP,
    classification: "ACCOUNT_DEVELOPMENT",
    snapshot: nextSnap,
    approval: {
      authorized: true,
      authorizedBy: "test",
      authorizedAt: new Date().toISOString(),
      note: "incremental",
    },
  });
  assert.equal(upd.ok, true);

  // Share sanitize retains commercial content + events drawer
  const hydrated = hydrateOpportunityFromPublishedCommercialFields(loadedSnap);
  const presented = sanitizeOpportunityForCustomerPresentation({
    ...hydrated,
    hotelId: HOTEL,
    customerFacing: true,
  });
  const share = sanitizeOpportunityForShare(presented);
  assert.ok(share.whatIsHappening && !/^Hangers Weddings/i.test(share.whatIsHappening));
  assert.ok(share.potentialHotelBusiness || share.salesBrief?.potentialBusinessForThisHotel);
  assert.ok((share.establishedFacts || []).length >= 1);
  const drawerSections =
    share.eventsDrawerModel?.sections ||
    share.scalingDrawerModel?.sections ||
    share.sharedTenSectionDrawer?.sections ||
    null;
  assert.ok(drawerSections, "share must retain a customer drawer model");
  assert.ok(drawerSections.what_is_happening?.body);
  assert.ok((drawerSections.facts_and_uncertainties?.facts || []).length >= 1);
  assert.ok(
    (
      share.qualificationQuestions ||
      drawerSections.recommended_next_step?.qualificationQuestions ||
      []
    ).length >= 1,
  );
  assert.equal(share.secondaryContact?.name, "Aviann Murray");

  console.log(
    JSON.stringify(
      {
        ok: true,
        tests: [
          "lean_gate_reject",
          "enriched_gate_accept",
          "publish",
          "lean_publish_blocked",
          "registry_reload",
          "incremental_update",
          "share_sanitize_retention",
        ],
      },
      null,
      2,
    ),
  );
}

run();
