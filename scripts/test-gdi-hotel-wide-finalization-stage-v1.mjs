#!/usr/bin/env node
/**
 * Permanent regressions — hotel-wide Full E2E finalization stage.
 * $0 only. No pointer mutation. Agent OFF.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  collectHotelWideCanonicalInventory,
  assignHotelWideDisposition,
  runHotelWideFinalizationStage,
  HOTEL_WIDE_DISPOSITION,
  HOTEL_WIDE_COMPLETION_OUTCOME,
  GDI_HOTEL_WIDE_FINALIZATION_STAGE_V1,
} from "../lib/group-demand-intelligence/workflow/gdi-hotel-wide-finalization-stage-v1.js";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const HILTON = "rec35fExUxCClpOP6";
const BETHESDA = "recLuxvwwxID7U2B8";

async function check(name, fn) {
  try {
    await fn();
    console.log(`  PASS ${name}`);
  } catch (e) {
    console.error(`  FAIL ${name}`);
    throw e;
  }
}

function baseOpp(over = {}) {
  return {
    id: "opp_hw_1",
    title: "Annual Association Conference",
    organizationName: "Example Association",
    hotelOpportunityThesis:
      "Credible Midtown housing fit for association attendees with evidenced future cycle demand.",
    recommendedNextStep:
      "Outreach to the named meetings contact for the next housing cycle.",
    sourceUrls: ["https://example.org/events/2027"],
    eventStartDate: "2027-06-01",
    futureDemandPathway: true,
    primaryContact: {
      name: "Jordan Lee",
      role: "Director of Meetings",
      organization: "Example Association",
      email: "jordan.lee@example.org",
      emailType: "DIRECT_WORK",
    },
    secondContactAttempted: true,
    secondaryContactResearchStatus: "ATTEMPTED_NO_SECOND_VERIFIED",
    ...over,
  };
}

await check("contract_id", () => {
  assert.equal(
    GDI_HOTEL_WIDE_FINALIZATION_STAGE_V1,
    "gdi-hotel-wide-finalization-stage-v1",
  );
  assert.ok(HOTEL_WIDE_COMPLETION_OUTCOME.FULL_E2E_COMPLETE);
  assert.ok(HOTEL_WIDE_COMPLETION_OUTCOME.RESEARCH_GAPS_REMAIN);
});

await check("entire_hotel_inventory_coverage_hilton", () => {
  const inv = collectHotelWideCanonicalInventory({ hotelId: HILTON, root: ROOT });
  assert.ok(inv.opportunities.length >= 40, "expected Hilton research inventory");
  const ids = inv.opportunities.map((o) => o.id);
  assert.equal(new Set(ids).size, ids.filter(Boolean).length);
  assert.ok(
    (inv.candidatePackageVersions || []).some((v) =>
      /paid_research/i.test(v),
    ),
    "paid-research candidate package must be ingested",
  );
});

await check("old_and_hold_included_in_review", () => {
  const inv = collectHotelWideCanonicalInventory({ hotelId: HILTON, root: ROOT });
  const plus = inv.opportunities.find((o) => o.id === "gdi_opp_5f3bb06d74145fdc");
  const scbwi = inv.opportunities.find((o) => o.id === "gdi_opp_5282e7a3223f2741");
  assert.ok(plus, "PLUS prior HOLD must be in inventory");
  assert.ok(scbwi, "SCBWI must be in inventory");
});

await check("latest_verified_contact_wins_over_stale_research", () => {
  const inv = collectHotelWideCanonicalInventory({ hotelId: HILTON, root: ROOT });
  const scbwi = inv.opportunities.find((o) => o.id === "gdi_opp_5282e7a3223f2741");
  assert.ok(scbwi, "SCBWI present");
  assert.match(
    String(scbwi.primaryContact?.linkedinUrl || ""),
    /linkedin\.com\/in\//i,
    "SCBWI LinkedIn from paid package must win over stale research",
  );
  const sssp = inv.opportunities.find((o) => o.id === "gdi_opp_267d801f2fa39f9e");
  assert.ok(sssp, "SSSP present");
  assert.equal(String(sssp.marketScope || "").toUpperCase(), "COMPETITIVE");
  assert.match(
    String(sssp.competitiveContext?.incumbentHotel || sssp.incumbentHotel || ""),
    /Westin/i,
  );
});

await check("disposition_pass_hold_reject_historical", () => {
  const pass = assignHotelWideDisposition(baseOpp(), { sources: ["RESEARCH_STORE"] });
  assert.equal(pass.disposition, HOTEL_WIDE_DISPOSITION.FINALIZATION_PASS);

  const hold = assignHotelWideDisposition(
    baseOpp({
      primaryContact: { name: "Jordan Lee", role: "Director of Meetings" },
    }),
    { sources: ["RESEARCH_STORE"] },
  );
  assert.equal(hold.disposition, HOTEL_WIDE_DISPOSITION.HOLD_RESEARCH);

  const hist = assignHotelWideDisposition(
    baseOpp({
      pastClosed: true,
      historicalOnlyCustomerExclusion: true,
      futureDemandPathway: false,
      eventStartDate: "2018-01-01",
      hotelOpportunityThesis: "Past-only event with no future pathway documented here.",
    }),
    { sources: ["RESEARCH_STORE"] },
  );
  assert.equal(hist.disposition, HOTEL_WIDE_DISPOSITION.HISTORICAL);

  const rej = assignHotelWideDisposition(
    baseOpp({ disqualified: true, status: "DISQUALIFIED" }),
    { sources: ["RESEARCH_STORE"] },
  );
  assert.equal(rej.disposition, HOTEL_WIDE_DISPOSITION.REJECT);

  const exhibitor = assignHotelWideDisposition(
    baseOpp({
      id: "gdi_opp_exhibitor_block_x",
      title: "Interiors from Spain — EXHIBITOR BLOCK",
      primaryContact: { name: "", role: "" },
    }),
    { sources: ["RESEARCH_STORE"] },
  );
  assert.equal(exhibitor.disposition, HOTEL_WIDE_DISPOSITION.REJECT);
});

await check("rejected_records_not_resurrected", () => {
  const row = assignHotelWideDisposition(
    baseOpp({
      disqualified: true,
      status: "DISQUALIFIED",
      primaryContact: {
        name: "Jordan Lee",
        role: "Director of Meetings",
        email: "jordan.lee@example.org",
        emailType: "DIRECT_WORK",
      },
    }),
    { sources: ["CANDIDATE_PACKAGE:x", "RESEARCH_STORE"] },
  );
  assert.equal(row.disposition, HOTEL_WIDE_DISPOSITION.REJECT);
});

await check("previously_approved_review_when_eligibility_changes", () => {
  const row = assignHotelWideDisposition(
    baseOpp({
      primaryContact: { name: "Jordan Lee", role: "Director of Meetings" },
    }),
    { sources: ["STAGED_PACKAGE"] },
  );
  assert.equal(
    row.disposition,
    HOTEL_WIDE_DISPOSITION.PREVIOUSLY_APPROVED_REVIEW,
  );
});

await check("missing_named_primary_and_how", () => {
  const noName = assignHotelWideDisposition(
    baseOpp({ primaryContact: { name: "", role: "" } }),
    { sources: ["RESEARCH_STORE"] },
  );
  assert.ok(
    noName.reasonCodes.some((r) => /NAMED_COMMERCIAL|PERSON_SPECIFIC/.test(r)),
  );
  const noHow = assignHotelWideDisposition(
    baseOpp({
      primaryContact: {
        name: "Jordan Lee",
        role: "Director of Meetings",
        email: "info@example.org",
        emailType: "GENERIC_ORGANIZATION",
      },
    }),
    { sources: ["RESEARCH_STORE"] },
  );
  assert.ok(noHow.reasonCodes.includes("PERSON_SPECIFIC_CHANNEL_REQUIRED"));
});

await check("valid_linkedin_alternative_pathway", () => {
  const row = assignHotelWideDisposition(
    baseOpp({
      primaryContact: {
        name: "Alex Morgan",
        role: "Events Manager",
        linkedinUrl: "https://www.linkedin.com/in/alex-morgan-events",
      },
      salesReady: false,
    }),
    { sources: ["RESEARCH_STORE"] },
  );
  assert.equal(row.disposition, HOTEL_WIDE_DISPOSITION.FINALIZATION_PASS);
  assert.equal(row.fqo.salesReadyRequired, false);
});

await check("competitive_incumbent_required", () => {
  const bad = assignHotelWideDisposition(
    baseOpp({
      marketScope: "COMPETITIVE",
      competitiveContext: { label: "COMPETITOR_HOSTED" },
    }),
    { sources: ["RESEARCH_STORE"] },
  );
  assert.notEqual(bad.disposition, HOTEL_WIDE_DISPOSITION.FINALIZATION_PASS);
  const good = assignHotelWideDisposition(
    baseOpp({
      marketScope: "COMPETITIVE",
      competitiveContext: {
        incumbentHotel: "New York Marriott Marquis",
        pathway: "OVERFLOW_AND_FUTURE_CYCLE",
      },
    }),
    { sources: ["RESEARCH_STORE"] },
  );
  assert.equal(good.disposition, HOTEL_WIDE_DISPOSITION.FINALIZATION_PASS);
});

await check("no_automatic_paid_research_and_agent_off", async () => {
  process.env.GDI_EXA_AGENT_LIVE = "1";
  const result = await runHotelWideFinalizationStage({
    hotelId: HILTON,
    runId: "test_hw_agent_off",
    writeCandidatePackage: false,
    agentAuthorized: false,
    outDir: path.join(
      ROOT,
      "reports/gdi-hotel-wide-finalization-stage-v1/_test_agent_off",
    ),
  });
  assert.equal(result.paidSpendUsd, 0);
  assert.equal(result.agentOff, true);
  assert.ok(!process.env.GDI_EXA_AGENT_LIVE);
  assert.ok(result.manifest?.researchAuthorization?.hotelWideReviewDoesNotAuthorizePaidResearch);
});

await check("hilton_manifest_reconciliation_complete", async () => {
  const result = await runHotelWideFinalizationStage({
    hotelId: HILTON,
    hotelName: "Hilton New York Times Square",
    runId: "test_hw_hilton_manifest",
    writeCandidatePackage: false,
    outDir: path.join(
      ROOT,
      "reports/gdi-hotel-wide-finalization-stage-v1/_test_hilton",
    ),
  });
  assert.equal(result.ok, true);
  assert.equal(result.manifest.reconciliation.everyOpportunityReviewedExactlyOnce, true);
  assert.ok(result.manifest.inventory.totalCanonicalReviewed >= 40);
  assert.ok(result.hotelInventoryReconciled === true);
  assert.ok(
    [
      HOTEL_WIDE_COMPLETION_OUTCOME.HOTEL_INVENTORY_RECONCILED,
      HOTEL_WIDE_COMPLETION_OUTCOME.RESEARCH_GAPS_REMAIN,
      HOTEL_WIDE_COMPLETION_OUTCOME.QUALIFIED_CANDIDATE_CERTIFIED,
      HOTEL_WIDE_COMPLETION_OUTCOME.FULL_E2E_COMPLETE,
    ].includes(result.completionOutcome),
  );
  assert.ok(fs.existsSync(result.manifestPath));
});

await check("hilton_six_vs_nine_paid_package_authority", async () => {
  const result = await runHotelWideFinalizationStage({
    hotelId: HILTON,
    hotelName: "Hilton New York Times Square",
    runId: "test_hw_hilton_6v9",
    writeCandidatePackage: true,
    outDir: path.join(
      ROOT,
      "reports/gdi-hotel-wide-finalization-stage-v1/_test_hilton_6v9",
    ),
  });
  assert.equal(result.pointersUnchanged, true);
  assert.equal(result.manifest.stagedCandidate.pointerActivated, false);
  const passIds = result.manifest.rows
    .filter((r) => r.disposition === HOTEL_WIDE_DISPOSITION.FINALIZATION_PASS)
    .map((r) => r.opportunityId)
    .sort();
  const required = [
    "gdi_opp_8ae344c41d87f94b",
    "gdi_opp_f07d86ab80b5f956",
    "gdi_opp_3b2b6d838d977415",
    "gdi_opp_84fc450962c52470",
    "gdi_opp_267d801f2fa39f9e",
    "gdi_opp_5282e7a3223f2741",
  ];
  for (const id of required) {
    assert.ok(passIds.includes(id), `expected PASS for ${id}`);
  }
  assert.equal(
    passIds.length,
    required.length,
    "Hilton candidate must equal paid-6 commercially validated set (not funnel-9 soft passes)",
  );
  const sssp = result.manifest.rows.find(
    (r) => r.opportunityId === "gdi_opp_267d801f2fa39f9e",
  );
  assert.equal(sssp.marketScope, "COMPETITIVE");
  const scbwi = result.manifest.rows.find(
    (r) => r.opportunityId === "gdi_opp_5282e7a3223f2741",
  );
  assert.equal(scbwi.disposition, HOTEL_WIDE_DISPOSITION.FINALIZATION_PASS);
  assert.equal(scbwi.secondaryPresent, true);
  assert.match(
    String(scbwi.contactMergePreferredSource || ""),
    /paid_research/i,
  );
  const olympics = result.manifest.rows.find((r) =>
    /olympics/i.test(r.opportunityId || ""),
  );
  if (olympics) {
    assert.ok(
      olympics.disposition === HOTEL_WIDE_DISPOSITION.HISTORICAL ||
        olympics.disposition === HOTEL_WIDE_DISPOSITION.REJECT ||
        olympics.disposition === HOTEL_WIDE_DISPOSITION.HOLD_RESEARCH,
      "Olympics must not be a silent FQO PASS without future pathway",
    );
  }
  const sior = result.manifest.rows.find(
    (r) => r.opportunityId === "gdi_opp_790a7cea8c321ada",
  );
  if (sior) {
    assert.notEqual(sior.disposition, HOTEL_WIDE_DISPOSITION.FINALIZATION_PASS);
  }
  assert.ok(result.qualifiedCandidateCertified === true);
  assert.equal(result.manifest.stagedCandidate.pointerActivated, false);
});

await check("auto_candidate_default_and_zero_qualified_path", async () => {
  const result = await runHotelWideFinalizationStage({
    hotelId: "recTEST_NO_SUCH_HOTEL_ZERO",
    runId: "test_hw_zero",
    seedOpportunities: [
      baseOpp({
        id: "opp_hold_only",
        primaryContact: { name: "Jordan Lee", role: "Director" },
      }),
    ],
    outDir: path.join(
      ROOT,
      "reports/gdi-hotel-wide-finalization-stage-v1/_test_zero",
    ),
  });
  assert.equal(result.manifest.dispositions.FINALIZATION_PASS, 0);
  assert.equal(result.manifest.stagedCandidate.zeroQualifiedCertified, true);
  assert.equal(result.manifest.stagedCandidate.pointerActivated, false);
  assert.ok(result.researchGapsRemain === true);
});

await check("no_unintended_staged_current_changes", async () => {
  const store = createGdiPublicationPackageStore();
  const beforeS = store.readPointer(BETHESDA, "STAGED")?.publicationVersion;
  const beforeC = store.readPointer(BETHESDA, "CURRENT")?.publicationVersion;
  await runHotelWideFinalizationStage({
    hotelId: HILTON,
    runId: "test_hw_pointers",
    writeCandidatePackage: false,
    outDir: path.join(
      ROOT,
      "reports/gdi-hotel-wide-finalization-stage-v1/_test_pointers",
    ),
  });
  const afterS = store.readPointer(BETHESDA, "STAGED")?.publicationVersion;
  const afterC = store.readPointer(BETHESDA, "CURRENT")?.publicationVersion;
  assert.equal(afterS, beforeS);
  assert.equal(afterC, beforeC);
  assert.equal(store.readPointer(HILTON, "STAGED"), null);
});

await check("bethesda_existing_staged_preserved_on_review", async () => {
  const store = createGdiPublicationPackageStore();
  const before = store.readPointer(BETHESDA, "STAGED")?.publicationVersion;
  const result = await runHotelWideFinalizationStage({
    hotelId: BETHESDA,
    runId: "test_hw_bethesda",
    writeCandidatePackage: false,
    outDir: path.join(
      ROOT,
      "reports/gdi-hotel-wide-finalization-stage-v1/_test_bethesda",
    ),
  });
  assert.equal(store.readPointer(BETHESDA, "STAGED")?.publicationVersion, before);
  assert.equal(result.pointersUnchanged, true);
  assert.ok(result.hotelInventoryReconciled === true);
});

await check("idempotent_repeat_review", async () => {
  const a = await runHotelWideFinalizationStage({
    hotelId: HILTON,
    runId: "test_hw_idem_a",
    writeCandidatePackage: false,
    outDir: path.join(ROOT, "reports/gdi-hotel-wide-finalization-stage-v1/_test_idem_a"),
  });
  const b = await runHotelWideFinalizationStage({
    hotelId: HILTON,
    runId: "test_hw_idem_b",
    writeCandidatePackage: false,
    outDir: path.join(ROOT, "reports/gdi-hotel-wide-finalization-stage-v1/_test_idem_b"),
  });
  assert.equal(
    a.manifest.inventory.totalCanonicalReviewed,
    b.manifest.inventory.totalCanonicalReviewed,
  );
  assert.deepEqual(a.manifest.dispositions, b.manifest.dispositions);
});

await check("dedupe_new_discovery_with_historical", () => {
  const seed = [
    {
      id: "gdi_opp_f07d86ab80b5f956",
      title: "NYSSBA duplicate seed",
      organizationName: "NYSSBA",
    },
  ];
  const inv = collectHotelWideCanonicalInventory({
    hotelId: HILTON,
    root: ROOT,
    seedOpportunities: seed,
  });
  const matches = inv.opportunities.filter(
    (o) => o.id === "gdi_opp_f07d86ab80b5f956",
  );
  assert.equal(matches.length, 1);
  assert.ok(
    inv.duplicateMappings.some(
      (d) =>
        d.opportunityId === "gdi_opp_f07d86ab80b5f956" &&
        /MERGED/.test(d.resolution || ""),
    ),
  );
});

console.log("\nOK test-gdi-hotel-wide-finalization-stage-v1");