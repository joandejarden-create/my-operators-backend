#!/usr/bin/env node
import assert from "node:assert/strict";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { reconcileCommercialDispositionV1 } from "../lib/group-demand-intelligence/workflow/gdi-commercial-disposition-reconcile-v1.js";
import { buildResearchEconomicsLedger } from "../lib/group-demand-intelligence/workflow/gdi-research-economics-ledger-v1.js";
import { qualifyCommercialProspectV3 } from "../lib/group-demand-intelligence/research/commercial-qualification-v3.js";
import { mapDossierEvidenceToOpportunityFields } from "../lib/group-demand-intelligence/research/exa-agent/exa-agent-source-evidence-v1.js";
import {
  GDI_PORTFOLIO_CERT_V1_2,
  FORENSIC_CLASS,
  reclassifyFrozenOpportunity,
  assessDeepResearchImprovement,
  runOfflineConversionRecovery,
  V1_1_EIGHT_OPPORTUNITY_IDS,
} from "../lib/group-demand-intelligence/workflow/gdi-portfolio-cert-v1-2-commercial-conversion-recovery-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "..");

// 1) Reconcile: RESEARCH_CONTINUATION must not keep EXCLUDE
{
  const r = reconcileCommercialDispositionV1({
    eligibilityResult: {
      status: "RESEARCH_CONTINUATION",
      researchContinuation: true,
      preserveOpportunity: true,
      doNotTreatAsExhausted: true,
      salesReady: false,
    },
    v41Qualification: { internalDisposition: "EXCLUDE" },
  });
  assert.equal(r.finalDisposition, "WATCHLIST_RESEARCH");
  assert.equal(r.conflict, true);
  assert.equal(r.promotionBlocked, true);
}

// 2) Economics: EXCLUDE + commercialResearchComplete must not count actionable
{
  const ledger = buildResearchEconomicsLedger({
    processed: [
      {
        primaryContactName: "Frank Yang",
        salesReadyContacts: 1,
        commercialResearchComplete: true,
        disposition: "EXCLUDE",
      },
      {
        disposition: "WATCHLIST_RESEARCH",
        commercialResearchComplete: true,
      },
    ],
    selectionRegister: { selectedCount: 2, deferredCount: 0, register: [] },
    retainedCandidates: [{}, {}],
    spentUsd: 0.8,
  });
  assert.equal(ledger.verifiedCommercialContactsGained, 1);
  assert.equal(ledger.opportunitiesBecomingActionable, 1);
  assert.equal(ledger.canonicalSalesReadyCount, 0);
  assert.ok(
    ledger.honestyNotes.some((n) => /EXCLUDE disposition/i.test(n)),
  );
}

// 3) V3: association with group activity and no email → WATCHLIST not EXCLUDE
{
  const q = qualifyCommercialProspectV3({
    opportunity: {
      organizationName: "American Society for Gravitational and Space Research",
      title: "ASGSR 2026 Annual Meeting",
      summaryWhyHotel: "Annual multi-day conference drawing out-of-town attendees.",
    },
    acceptedClaims: [
      {
        statement:
          "ASGSR 2026 Annual Meeting multi-day conference with registered attendees traveling to the region.",
        url: "https://asgsr.org/asgsr-conference-2026-important-information/",
      },
    ],
    pageTexts: [
      "ASGSR 2026 Annual Meeting is a multi-day scientific conference. Hotel travel information for attendees.",
    ],
    targetHotel: "Bethesda Marriott",
    usableContactRoute: null,
  });
  assert.notEqual(q.internalDisposition, "EXCLUDE");
  assert.match(q.internalDisposition, /WATCHLIST|ACCOUNT|PRE_SOURCING|ACTIVE/);
}

// 4) investigatedUrls propagation from dossier sources
{
  const mapped = mapDossierEvidenceToOpportunityFields(
    { seedUrls: ["https://example.com/seed"], investigatedUrls: [] },
    {
      supportingSources: [
        {
          url: "https://example.com/deep-source",
          title: "Deep",
          relevance: "confirms lodging",
        },
      ],
      claimToSourceMap: [
        {
          claim: "Hotel block may be needed",
          sourceUrls: ["https://example.com/deep-source"],
        },
      ],
    },
  );
  assert.ok(
    mapped.opportunity.investigatedUrls.includes("https://example.com/deep-source"),
  );
}

// 5) Offline recovery on frozen V1.1 ORS (if present)
{
  const result = runOfflineConversionRecovery(root, { replayV3: true });
  assert.equal(result.version, GDI_PORTFOLIO_CERT_V1_2);
  assert.equal(result.paidResearch, false);
  assert.equal(result.salesReadyPromotions, 0);
  assert.equal(result.rows.length, V1_1_EIGHT_OPPORTUNITY_IDS.length);
  assert.ok(result.benchmark.falsePositiveExcludeRemediated >= 1);
  assert.equal(result.benchmark.salesReadyOpportunities, 0);
  assert.equal(result.benchmark.falseNegativeSalesReadyForced, 0);
  for (const row of result.rows) {
    assert.notEqual(row.afterDisposition, "SALES_READY");
    assert.ok(row.forensicClasses.length >= 1);
    if (/EXCLUDE/i.test(row.beforeDisposition || "")) {
      assert.equal(row.afterDisposition, "WATCHLIST_RESEARCH");
    }
  }
  const deep = assessDeepResearchImprovement({
    seedUrls: ["https://a.example"],
    investigatedUrls: [],
    exaRunId: "agent_run_x",
    evidence: [{ provenance: "discovery_evidence" }],
  });
  assert.equal(deep.verdict, "PAID_CALL_WITHOUT_EVIDENTIARY_IMPROVEMENT");
  assert.ok(
    Object.values(FORENSIC_CLASS).every((v) => typeof v === "string"),
  );
}

// 6) reclassify unit: preserve + EXCLUDE → WATCHLIST
{
  const row = reclassifyFrozenOpportunity({
    opportunityId: "disc_test",
    organizationName: "Test Org",
    commercialEligibilityStatus: "RESEARCH_CONTINUATION",
    commercialEligibilityContractV1: {
      status: "RESEARCH_CONTINUATION",
      researchContinuation: true,
      preserveOpportunity: true,
      doNotTreatAsExhausted: true,
      salesReady: false,
      gaps: ["MISSING_NAMED_RELEVANT_BUYER"],
      gates: {
        credibleOrganizationActivity: true,
        realisticHotelDemand: false,
        hotelFitDefensibleForSalesReady: false,
        namedRelevantBuyer: false,
        usableContactRoute: false,
        sourcesTraceable: true,
      },
    },
    commercialQualificationV41: { internalDisposition: "EXCLUDE" },
    historicalInternalDisposition: "EXCLUDE",
    internalDisposition: "EXCLUDE",
    dispositionReconcile: { finalDisposition: "EXCLUDE", v41Disposition: "EXCLUDE" },
    investigatedUrls: [],
    seedUrls: ["https://example.com"],
    exaRunId: "agent_run_test",
    namedStakeholders: [],
  });
  assert.equal(row.afterDisposition, "WATCHLIST_RESEARCH");
  assert.equal(row.changed, true);
  assert.ok(
    row.forensicClasses.includes(
      FORENSIC_CLASS.E_INCORRECT_QUALIFICATION_OR_PROPAGATION,
    ),
  );
}

console.log("test-gdi-portfolio-cert-v1-2-commercial-conversion-recovery: PASS");
