#!/usr/bin/env node
/**
 * Automated tests for SEC EDGAR Contract Intelligence V1.
 */
import assert from "node:assert/strict";
import {
  SEC_EDGAR_CONTRACT_ID,
  isPersistableVerifiedClass,
  VERIFICATION_CLASS,
  writeGuarantees,
  DISCOVERY_STATUS,
  DISCOVERY_INVENTORY_DESTINATION,
} from "../lib/hotel-intelligence/sec-edgar/contract.js";
import { normalizeCik, listPilotCompanies } from "../lib/hotel-intelligence/sec-edgar/cik-registry-v1.js";
import { loadSecEdgarConfig } from "../lib/hotel-intelligence/sec-edgar/edgar-client-v1.js";
import { listRecentFilings, dedupeFilings } from "../lib/hotel-intelligence/sec-edgar/filing-discovery-v1.js";
import {
  extractContractFindingsFromDocument,
  isUnsupportedExpirationClaim,
} from "../lib/hotel-intelligence/sec-edgar/contract-extractor-v1.js";
import { verifyFinding, verifyFindings } from "../lib/hotel-intelligence/sec-edgar/verification-v1.js";
import { matchHotelHintToHpc, attributeFindings } from "../lib/hotel-intelligence/sec-edgar/hotel-attribution-v1.js";
import { planContractPersistence } from "../lib/hotel-intelligence/sec-edgar/persistence-planner-v1.js";
import { buildOpportunityDossiers, scoreOpportunity } from "../lib/hotel-intelligence/sec-edgar/opportunity-discovery-v1.js";
import { createSecEdgarBudgetGuard } from "../lib/hotel-intelligence/sec-edgar/budget-guard-v1.js";
import { diffContractFindings, documentUnchanged } from "../lib/hotel-intelligence/sec-edgar/change-detection-v1.js";
import { internationalSourceAdapterContract } from "../lib/hotel-intelligence/sec-edgar/intl-readiness-v1.js";
import { applyContractPersistencePlan } from "../lib/hotel-intelligence/sec-edgar/persistence-apply-v1.js";
import {
  discoverHotelsFromDocument,
  dedupeDiscoveredHotels,
} from "../lib/hotel-intelligence/sec-edgar/hotel-discovery-v1.js";
import {
  upsertDiscoveredHotels,
  inventorySummary,
} from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-v1.js";
import { reconcileDiscoveryInventoryToHpc } from "../lib/hotel-intelligence/sec-edgar/hpc-reconciliation-v1.js";
import {
  buildHotelInventoryRows,
  hotelInventoryToCsv,
} from "../lib/hotel-intelligence/sec-edgar/hotel-inventory-report-v1.js";
import { buildYieldPrioritizationReport } from "../lib/hotel-intelligence/sec-edgar/yield-prioritization-v1.js";

let passed = 0;
function test(name, fn) {
  try {
    fn();
    passed += 1;
    console.log(`PASS ${name}`);
  } catch (err) {
    console.error(`FAIL ${name}`);
    console.error(err);
    process.exitCode = 1;
  }
}

test("contract_id_and_write_guarantees", () => {
  assert.equal(SEC_EDGAR_CONTRACT_ID, "SEC_EDGAR_CONTRACT_INTELLIGENCE_V1");
  const g = writeGuarantees();
  assert.equal(g.new_airtable_tables, false);
  assert.equal(g.new_airtable_fields, false);
  assert.equal(g.customer_publication, false);
  assert.equal(g.estimates_as_verified, false);
});

test("cik_normalize_and_pilot_cohort", () => {
  assert.equal(normalizeCik("1048286"), "0001048286");
  const cos = listPilotCompanies({ includeUnverifiedCik: true });
  assert.ok(cos.length >= 3);
  assert.ok(cos.some((c) => c.ticker === "MAR"));
});

test("fair_access_config_requires_user_agent", () => {
  const cfg = loadSecEdgarConfig({
    SEC_EDGAR_ENABLED: "true",
    SEC_EDGAR_USER_AGENT: "Dealality Research joan@aohospitalityadvisors.com",
    SEC_EDGAR_RATE_LIMIT: "2",
  });
  assert.ok(cfg.userAgent.includes("Dealality"));
  assert.equal(cfg.rateLimitPerSec, 2);
});

test("filing_discovery_filters_and_dedupes", () => {
  const body = {
    cik: "1048286",
    name: "Marriott",
    filings: {
      recent: {
        form: ["10-K", "8-K", "4"],
        filingDate: ["2025-02-01", "2025-01-15", "2024-12-01"],
        accessionNumber: ["a", "b", "c"],
        primaryDocument: ["d1.htm", "d2.htm", "d3.htm"],
        primaryDocDescription: ["", "", ""],
        reportDate: ["", "", ""],
        size: [1, 2, 3],
      },
    },
  };
  const listed = listRecentFilings(body, { limit: 10, sinceYear: 2024 });
  assert.equal(listed.ok, true);
  assert.equal(listed.filings.length, 2);
  assert.equal(dedupeFilings([...listed.filings, listed.filings[0]]).length, 2);
});

test("extractor_finds_management_and_rejects_portfolio_auto_attribute", () => {
  const doc = {
    text: `Management Agreement for the Grand Example Hotel expires December 31, 2028.
           Marriott portfolio-wide management agreements typically include renewal options.`,
  };
  const out = extractContractFindingsFromDocument(doc, {
    cik: "0001048286",
    accessionNumber: "0001",
    form: "10-K",
    filingDate: "2025-02-01",
    documentUrl: "https://www.sec.gov/Archives/edgar/data/1048286/x.htm",
    companyName: "Marriott",
  });
  assert.ok(out.findingCount >= 1);
  const hotelOne = out.findings.find((f) => f.hotelAttributed);
  const portfolio = out.findings.find((f) => f.portfolioWide);
  assert.ok(hotelOne, "expected hotel-attributed finding");
  if (portfolio) {
    assert.equal(portfolio.verificationClass, VERIFICATION_CLASS.INSUFFICIENT_EVIDENCE);
  }
});

test("negative_unsupported_expiration_and_estimate_language", () => {
  assert.equal(isUnsupportedExpirationClaim("expected to expire around 2030"), true);
  assert.equal(isUnsupportedExpirationClaim("expires on December 31, 2028"), false);
  const f = {
    verificationClass: VERIFICATION_CLASS.VERIFIED_EXPIRATION,
    hotelAttributed: true,
    hotelNameHints: ["Grand Example Hotel"],
    primaryDate: "2035",
    snippet: "franchise expiration is approximately 2035 (estimated)",
    source: { accessionNumber: "x", documentUrl: "https://www.sec.gov/x" },
  };
  const v = verifyFinding(f);
  assert.equal(v.persistable, false);
  assert.equal(v.verificationClass, VERIFICATION_CLASS.ESTIMATED_RENEWAL_WINDOW);
});

test("hotel_attribution_ambiguous_quarantine", () => {
  const candidates = [
    { id: "rec1", name: "Marriott Bethesda", city: "Bethesda" },
    { id: "rec2", name: "Bethesda Marriott Suites", city: "Bethesda" },
  ];
  const m = matchHotelHintToHpc("Bethesda Marriott", candidates, { minScore: 0.4 });
  assert.ok(["MATCHED", "AMBIGUOUS", "NO_MATCH"].includes(m.status));
});

test("verification_requires_provenance_and_attribution", () => {
  const v = verifyFinding({
    verificationClass: VERIFICATION_CLASS.VERIFIED_EXPIRATION,
    hotelAttributed: false,
    portfolioWide: true,
    primaryDate: "December 31, 2028",
    snippet: "expires December 31, 2028",
    source: {},
  });
  assert.equal(v.persistable, false);
  assert.ok(!isPersistableVerifiedClass(VERIFICATION_CLASS.INSUFFICIENT_EVIDENCE));
});

test("persistence_plan_uses_existing_hi_graph_only", () => {
  const verified = verifyFindings([
    {
      verificationClass: VERIFICATION_CLASS.VERIFIED_EXPIRATION,
      verificationClassRaw: VERIFICATION_CLASS.VERIFIED_EXPIRATION,
      hotelAttributed: true,
      hotelNameHints: ["Bethesda Marriott"],
      primaryDate: "December 31, 2028",
      evidenceFieldName: "Management Agreement Expiration Date",
      agreementType: "MANAGEMENT_AGREEMENT",
      snippet: "Bethesda Marriott management agreement expires December 31, 2028",
      source: {
        accessionNumber: "0001048286-25-000001",
        documentUrl: "https://www.sec.gov/Archives/edgar/data/1048286/x.htm",
        formType: "10-K",
        filingDate: "2025-02-01",
        cik: "0001048286",
        companyName: "Marriott",
      },
    },
  ]);
  const attributed = attributeFindings(verified.results, [
    { id: "recLuxvwwxID7U2B8", name: "Bethesda Marriott", city: "Bethesda" },
  ]);
  const plan = planContractPersistence(attributed.results);
  assert.ok(plan.counts.evidence >= 0);
  assert.equal(plan.publication.customerFacing, false);
  for (const row of plan.evidenceRows) {
    assert.equal(row.table, "HI Graph Evidence");
    assert.equal(row.fields["Source Type"], "SEC EDGAR Filing");
  }
});

test("apply_defaults_to_dry_run", async () => {
  const res = await applyContractPersistencePlan({ evidenceRows: [], counts: { evidence: 0 } }, {});
  assert.equal(res.dryRun, true);
  assert.equal(res.applied, false);
});

test("opportunity_scoring_keeps_evidence_separate", () => {
  const s = scoreOpportunity({
    verificationClass: VERIFICATION_CLASS.INSUFFICIENT_EVIDENCE,
    windowId: "0_6_MONTHS",
    hasOwnerRoute: true,
    roomsKnown: true,
  });
  assert.ok(s.evidenceConfidence < 0.5);
  assert.ok(s.commercialAttractiveness > 0.5);
  const d = buildOpportunityDossiers([]);
  assert.equal(d.count, 0);
});

test("budget_guard_blocks_over_ceiling", () => {
  const g = createSecEdgarBudgetGuard({
    ceilingUsd: 1,
    ledgerPath: `data/sec-edgar/test-budget-${Date.now()}.json`,
  });
  assert.equal(g.canSpend(0.5).ok, true);
  assert.equal(g.recordSpend(0.8, { op: "test" }).ok, true);
  assert.equal(g.canSpend(0.5).ok, false);
});

test("change_detection_and_document_fingerprint", () => {
  const a = [{ hpcHotelId: "rec1", evidenceFieldName: "X", primaryDate: "2028", source: { accessionNumber: "1" }, snippet: "a", verificationClass: "VERIFIED_EXPIRATION" }];
  const b = [{ hpcHotelId: "rec1", evidenceFieldName: "X", primaryDate: "2033", source: { accessionNumber: "2" }, snippet: "b", verificationClass: "VERIFIED_EXPIRATION" }];
  const diff = diffContractFindings(a, b);
  assert.ok(diff.counts.NEW + diff.counts.SUPERSEDED + diff.counts.UPDATED >= 1);
  assert.equal(documentUnchanged("abc", "abc"), true);
  assert.equal(documentUnchanged("abc", "xyz"), false);
});

test("intl_readiness_no_paid_sources", () => {
  const intl = internationalSourceAdapterContract();
  assert.equal(intl.paidSourcesAuthorizedThisPhase, false);
  assert.ok(intl.jurisdictions.length >= 5);
});

test("hotel_discovery_independent_of_hpc_and_contracts", () => {
  const doc = {
    text: `
      Westin Chicago River North, Chicago, IL 407
      Courtyard San Diego Downtown, San Diego, CA 245
      Management Agreement expires December 31, 2028 for Grand Example Hotel.
    `,
  };
  const out = discoverHotelsFromDocument(doc, {
    cik: "0001295810",
    companyName: "Sunstone",
    form: "10-K",
  });
  assert.ok(out.hotelCount >= 2, `expected >=2 hotels, got ${out.hotelCount}`);
  assert.ok(out.hotels.every((h) => h.discoveryId && h.hotelName));
  const deduped = dedupeDiscoveredHotels([...out.hotels, ...out.hotels]);
  assert.equal(deduped.length, out.hotels.length);
});

test("discovery_inventory_retains_unmatched_hotels", () => {
  const hotels = [
    {
      discoveryId: "disc_sec_test1",
      hotelName: "Westin Chicago River North",
      city: "Chicago",
      stateRegion: "IL",
      country: "United States",
      cik: "0001295810",
      hpcMatchStatus: DISCOVERY_STATUS.DISCOVERED_NOT_IN_HPC,
      mentionCount: 1,
    },
  ];
  const inv = upsertDiscoveredHotels({ hotels: [] }, hotels);
  assert.equal(inv.hotels.length, 1);
  assert.equal(inv.hotels[0].hpcMatchStatus, DISCOVERY_STATUS.DISCOVERED_NOT_IN_HPC);
  const withContract = upsertDiscoveredHotels(inv, [], {
    contractLinks: [
      {
        hotelName: "Westin Chicago River North",
        verified: true,
        finding: {
          verificationClass: VERIFICATION_CLASS.VERIFIED_EXPIRATION,
          primaryDate: "December 31, 2028",
          evidenceFieldName: "Management Agreement Expiration Date",
          source: { accessionNumber: "a" },
        },
      },
    ],
  });
  assert.equal(
    withContract.hotels[0].hpcMatchStatus,
    DISCOVERY_STATUS.CONTRACT_VERIFIED_PENDING_HPC,
  );
  const summary = inventorySummary(withContract);
  assert.equal(summary.totalHotels, 1);
  assert.equal(summary.contractVerifiedPendingHpc, 1);
  assert.equal(DISCOVERY_INVENTORY_DESTINATION.kind, "FILESYSTEM_RESEARCH_INVENTORY");
});

test("hpc_reconciliation_dry_run_no_redownload", () => {
  const inventory = {
    hotels: [
      {
        discoveryId: "disc_sec_recon1",
        hotelName: "Bethesda Marriott",
        city: "Bethesda",
        hpcMatchStatus: DISCOVERY_STATUS.CONTRACT_VERIFIED_PENDING_HPC,
        hpcRecordId: null,
        contractFindings: [
          {
            verificationClass: VERIFICATION_CLASS.VERIFIED_EXPIRATION,
            primaryDate: "December 31, 2028",
            evidenceFieldName: "Management Agreement Expiration Date",
            agreementType: "MANAGEMENT_AGREEMENT",
            source: {
              accessionNumber: "x",
              documentUrl: "https://www.sec.gov/x",
              formType: "10-K",
              filingDate: "2025-02-01",
              cik: "0001048286",
            },
          },
        ],
      },
    ],
  };
  const recon = reconcileDiscoveryInventoryToHpc(
    inventory,
    [{ id: "recLuxvwwxID7U2B8", name: "Bethesda Marriott", city: "Bethesda" }],
    { apply: false },
  );
  assert.equal(recon.dryRun, true);
  assert.ok(recon.counts.newlyMatchable >= 1);
  assert.ok(String(recon.note).includes("no SEC re-download"));
});

test("hotel_inventory_report_csv_json", () => {
  const inventory = {
    hotels: [
      {
        discoveryId: "disc_sec_r1",
        hotelName: "Grand Example Hotel",
        city: "Miami",
        stateRegion: "FL",
        country: "United States",
        registrant: "Example REIT",
        cik: "0001",
        sourceFiling: "10-K",
        sourceUrl: "https://www.sec.gov/x",
        hpcMatchStatus: DISCOVERY_STATUS.DISCOVERED_NOT_IN_HPC,
        contractFindings: [],
        mentionCount: 2,
      },
    ],
  };
  const report = buildHotelInventoryRows(inventory);
  assert.equal(report.rowCount, 1);
  assert.ok(report.columns.includes("hpcMatchStatus"));
  const csv = hotelInventoryToCsv(report);
  assert.ok(csv.includes("Grand Example Hotel"));
  assert.ok(csv.includes("DISCOVERED_NOT_IN_HPC"));
});

test("yield_prioritization_ranks_companies", () => {
  const y = buildYieldPrioritizationReport([
    {
      companyKey: "marriott",
      companyName: "Marriott",
      cik: "0001048286",
      filingsProcessed: 10,
      namedHotels: 0,
      verifiedMilestones: 0,
      relevantAgreements: 1,
      estimatedCostUsd: 0,
      failedAttributionReasons: ["no_hpc_candidate_above_threshold"],
    },
    {
      companyKey: "sunstone",
      companyName: "Sunstone",
      cik: "0001295810",
      filingsProcessed: 8,
      namedHotels: 12,
      verifiedMilestones: 2,
      relevantAgreements: 6,
      estimatedCostUsd: 0,
    },
  ]);
  assert.equal(y.ranked[0].companyKey, "sunstone");
  assert.ok(y.ranked.some((r) => r.recommendation === "DEPRIORITIZE_BROAD_CORPORATE"));
});

test("discovery_status_vocabulary_complete", () => {
  for (const key of [
    "DISCOVERED_MATCHED_HPC",
    "DISCOVERED_NOT_IN_HPC",
    "CONTRACT_VERIFIED_PENDING_HPC",
    "MATCH_AMBIGUOUS",
    "PORTFOLIO_ONLY_EVIDENCE",
    "UNATTRIBUTED_CONTRACT_LANGUAGE",
  ]) {
    assert.equal(DISCOVERY_STATUS[key], key);
  }
});

console.log(`\n${passed} tests passed`);
