#!/usr/bin/env node
/**
 * SEC EDGAR Historical Intelligence V2 — Marriott & Hilton contract discovery.
 *
 * Extends PR #492 connector. Resumable, cached, fair-access.
 *
 *   node scripts/sec-edgar-marriott-hilton-historical-v2.mjs
 *   node scripts/sec-edgar-marriott-hilton-historical-v2.mjs --offline-fixture
 *   node scripts/sec-edgar-marriott-hilton-historical-v2.mjs --batch-size 8
 *   node scripts/sec-edgar-marriott-hilton-historical-v2.mjs --companies diamondrock-hospitality,hilton-worldwide
 *   node scripts/sec-edgar-marriott-hilton-historical-v2.mjs --since-year 2000 --max-priority 2
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";

import {
  SEC_EDGAR_CONTRACT_ID,
  SEC_EDGAR_HISTORICAL_V2,
  writeGuarantees,
  SCHEMA_GAPS_REQUIRING_FOUNDER_APPROVAL,
  DISCOVERY_STATUS,
  DISCOVERY_INVENTORY_DESTINATION,
  HISTORICAL_APPLICABILITY,
} from "../lib/hotel-intelligence/sec-edgar/contract.js";
import {
  listHistoricalResearchCompanies,
  normalizeCik,
} from "../lib/hotel-intelligence/sec-edgar/cik-registry-v2.js";
import { createSecEdgarClient } from "../lib/hotel-intelligence/sec-edgar/edgar-client-v1.js";
import {
  listHistoricalFilings,
  loadAllSubmissionBodies,
  dedupeFilings,
} from "../lib/hotel-intelligence/sec-edgar/filing-discovery-v2.js";
import { extractContractFindingsFromDocument } from "../lib/hotel-intelligence/sec-edgar/contract-extractor-v1.js";
import { extractFranchiseExpirationSchedule } from "../lib/hotel-intelligence/sec-edgar/franchise-schedule-extractor-v2.js";
import { discoverHotelsFromDocument, dedupeDiscoveredHotels } from "../lib/hotel-intelligence/sec-edgar/hotel-discovery-v1.js";
import { applyHistoricalApplicability } from "../lib/hotel-intelligence/sec-edgar/historical-verification-v2.js";
import { detectBrandFamily, brandFamilySummary } from "../lib/hotel-intelligence/sec-edgar/brand-families-v2.js";
import {
  loadDiscoveryInventory,
  saveDiscoveryInventory,
  upsertDiscoveredHotels,
  inventorySummary,
  dedupeInventoryByIdentity,
  defaultInventoryRoot,
} from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-v1.js";
import { reconcileDiscoveryInventoryToHpc } from "../lib/hotel-intelligence/sec-edgar/hpc-reconciliation-v1.js";
import {
  buildMarriottHiltonInventory,
  marriottHiltonInventoryToCsv,
} from "../lib/hotel-intelligence/sec-edgar/marriott-hilton-inventory-v2.js";
import {
  loadCheckpoint,
  saveCheckpoint,
  isDocumentProcessed,
  markDocumentProcessed,
  markCompanyCoverage,
  defaultCheckpointPath,
} from "../lib/hotel-intelligence/sec-edgar/research-checkpoint-v2.js";
import { createSecEdgarBudgetGuard } from "../lib/hotel-intelligence/sec-edgar/budget-guard-v1.js";
import { matchHotelHintToHpc } from "../lib/hotel-intelligence/sec-edgar/hotel-attribution-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) {
    dotenv.config({ path: p, override: false });
    break;
  }
}

function argValue(flag, fallback = null) {
  const i = process.argv.indexOf(flag);
  if (i >= 0 && process.argv[i + 1] && !process.argv[i + 1].startsWith("--")) {
    return process.argv[i + 1];
  }
  return fallback;
}

const OFFLINE = process.argv.includes("--offline-fixture");
const BATCH_SIZE = Math.max(1, Number(argValue("--batch-size", "12")) || 12);
const SINCE_YEAR = Number(argValue("--since-year", "1994")) || 1994;
// Universal company research: registry includes REITs/owners/managers beyond Marriott/Hilton.
// MH inventory export remains available as a brand-family view, not a research restriction.
const MAX_PRIORITY = Number(argValue("--max-priority", "3")) || 3;
const COMPANY_FILTER = argValue("--companies", "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);
const OUT_DIR = path.join(ROOT, "reports", "sec-edgar-marriott-hilton-historical-v2");
fs.mkdirSync(OUT_DIR, { recursive: true });

function sha1(s) {
  return crypto.createHash("sha1").update(String(s)).digest("hex");
}

function diamondRockFixtureDoc() {
  return {
    text: `
      DiamondRock Hospitality Company
      Form 10-K — Hotel Franchise Expiration Schedule (illustrative historical table)

      Property Franchise Expiration Schedule
      Courtyard Denver Downtown Denver CO Franchise 2028
      Westin San Diego San Diego, CA Franchise 2030
      Hilton Garden Inn Boston Downtown Boston MA Franchise 2029
      Residence Inn Atlanta Midtown Atlanta, GA Franchise 2027
      Embassy Suites Chicago Downtown Chicago IL Franchise 2031
      Marriott Chicago Downtown Magnificent Mile Chicago, IL Management 2032
      Hampton Inn Seattle Downtown Seattle WA Franchise 2026
      Autograph Collection Hotel Indigo-style excluded row should not match
      Courtyard Boston Downtown | Boston | MA | Franchise | 2033
      DoubleTree by Hilton Bethesda Bethesda MD Franchise 2025

      EXHIBIT NOTES: Expiration years above are as disclosed in the filing year and are
      historically verified disclosures, not proof the agreement remains active today.
    `,
  };
}

function loadHpcSeed() {
  const p = path.join(
    ROOT,
    "reports",
    "sec-edgar-contract-intelligence-v1",
    "HPC_CANDIDATES_SEED.json",
  );
  if (fs.existsSync(p)) {
    try {
      return JSON.parse(fs.readFileSync(p, "utf8")).hotels || [];
    } catch {
      /* ignore */
    }
  }
  return [
    { id: "recLuxvwwxID7U2B8", name: "Bethesda Marriott", city: "Bethesda", brand: "Marriott Hotels" },
  ];
}

async function resolveCompanies(client) {
  let companies = listHistoricalResearchCompanies({
    maxPriority: MAX_PRIORITY,
    slugs: COMPANY_FILTER.length ? COMPANY_FILTER : null,
  });
  // Always include DiamondRock for the V2 extraction test case when not filtered out
  if (!COMPANY_FILTER.length && !companies.some((c) => c.slug === "diamondrock-hospitality")) {
    companies = [
      ...listHistoricalResearchCompanies({ slugs: ["diamondrock-hospitality"] }),
      ...companies,
    ];
  }
  const resolved = [];
  for (const co of companies) {
    let cik = normalizeCik(co.cik);
    let cikSource = "registry";
    if (!OFFLINE && co.ticker && client) {
      const live = await client.resolveCikByTicker(co.ticker);
      if (live?.cik) {
        cik = live.cik;
        cikSource = "sec_company_tickers";
      }
    }
    resolved.push({ ...co, cik, cikSource });
  }
  return resolved;
}

function stage3Match(hotels, hpcCandidates) {
  return hotels.map((h) => {
    const hint = [h.hotelName, h.city].filter(Boolean).join(" ");
    const match = matchHotelHintToHpc(hint, hpcCandidates, { minScore: 0.58 });
    if (match.status === "MATCHED") {
      return {
        ...h,
        hpcMatchStatus: DISCOVERY_STATUS.DISCOVERED_MATCHED_HPC,
        hpcRecordId: match.hpcId,
      };
    }
    if (match.status === "AMBIGUOUS") {
      return {
        ...h,
        hpcMatchStatus: DISCOVERY_STATUS.MATCH_AMBIGUOUS,
        hpcRecordId: null,
      };
    }
    return {
      ...h,
      hpcMatchStatus: DISCOVERY_STATUS.DISCOVERED_NOT_IN_HPC,
      hpcRecordId: null,
    };
  });
}

async function researchCompany(client, company, checkpoint, budget, metrics) {
  const report = {
    slug: company.slug,
    legalName: company.legalName,
    ticker: company.ticker,
    cik: company.cik,
    filingsEligible: 0,
    filingsProcessed: 0,
    filingsRemaining: 0,
    documentsProcessed: 0,
    documentsSkippedCached: 0,
    hotels: [],
    findings: [],
    scheduleHotels: 0,
    errors: [],
    coverage: null,
  };

  if (OFFLINE) {
    const meta = {
      cik: company.cik,
      accessionNumber: "0001298946-23-000012",
      form: "10-K",
      filingDate: "2023-02-28",
      documentUrl: "https://www.sec.gov/Archives/edgar/data/fixture/drh-10k.htm",
      companyName: company.legalName,
    };
    const schedule = extractFranchiseExpirationSchedule(diamondRockFixtureDoc(), meta);
    const discovery = discoverHotelsFromDocument(diamondRockFixtureDoc(), meta);
    const contracts = extractContractFindingsFromDocument(diamondRockFixtureDoc(), meta);
    report.filingsEligible = 1;
    report.filingsProcessed = 1;
    report.documentsProcessed = 1;
    report.scheduleHotels = schedule.hotelCount;
    report.hotels.push(...schedule.hotels, ...discovery.hotels);
    report.findings.push(...schedule.findings, ...contracts.findings);
    report.coverage = {
      eligibleFound: 1,
      returned: 1,
      remainingInQueue: 0,
      sinceYear: SINCE_YEAR,
    };
    return report;
  }

  const bodies = await loadAllSubmissionBodies(client, company.cik);
  if (!bodies.ok) {
    report.errors.push({ stage: "submissions", error: bodies.error });
    return report;
  }

  const listed = listHistoricalFilings(bodies.primary, {
    additionalBodies: bodies.additional,
    sinceYear: SINCE_YEAR,
    limit: BATCH_SIZE,
    prioritizeHighYield: true,
  });
  report.filingsEligible = listed.eligibleCount;
  report.filingsRemaining = listed.coverage.remainingInQueue;
  report.coverage = listed.coverage;

  const filings = dedupeFilings(listed.filings || []);
  let processedThisBatch = 0;

  for (const filing of filings) {
    if (isDocumentProcessed(checkpoint, company.cik, filing.accessionNumber, filing.primaryDocument)) {
      report.documentsSkippedCached += 1;
      continue;
    }
    if (processedThisBatch >= BATCH_SIZE) break;

    const doc = await client.getFilingDocument(
      company.cik,
      filing.accessionNumber,
      filing.primaryDocument,
    );
    if (!doc.ok || !doc.text) {
      report.errors.push({
        stage: "document",
        accession: filing.accessionNumber,
        error: doc.error || doc.status,
      });
      markDocumentProcessed(checkpoint, company.cik, filing.accessionNumber, filing.primaryDocument, {
        status: "error",
        error: doc.error || doc.status,
      });
      continue;
    }

    const fingerprint = sha1(doc.text.slice(0, 50000));
    const meta = {
      cik: company.cik,
      accessionNumber: filing.accessionNumber,
      form: filing.form,
      filingDate: filing.filingDate,
      documentUrl: client.filingDocumentUrl(
        company.cik,
        filing.accessionNumber,
        filing.primaryDocument,
      ),
      primaryDocument: filing.primaryDocument,
      companyName: company.legalName,
      documentFingerprint: fingerprint,
      maxFindings: 80,
    };

    // Prefer schedule extraction (high yield) then general discovery + contracts
    const schedule = extractFranchiseExpirationSchedule(doc, meta);
    const discovery = discoverHotelsFromDocument(doc, meta);
    const contracts = extractContractFindingsFromDocument(doc, meta);

    report.scheduleHotels += schedule.hotelCount;
    report.hotels.push(...schedule.hotels, ...discovery.hotels);
    report.findings.push(...schedule.findings, ...contracts.findings);
    report.documentsProcessed += 1;
    report.filingsProcessed += 1;
    processedThisBatch += 1;
    metrics.documentsProcessed += 1;

    markDocumentProcessed(checkpoint, company.cik, filing.accessionNumber, filing.primaryDocument, {
      status: "ok",
      form: filing.form,
      filingDate: filing.filingDate,
      hotels: schedule.hotelCount + discovery.hotelCount,
      findings: schedule.findings.length + contracts.findings.length,
      fingerprint,
      fromCache: Boolean(doc.fromCache),
    });

    // Paid AI remains gated
    if (String(process.env.SEC_EDGAR_ALLOW_PAID_AI || "") === "1") {
      budget.canSpend(0.5);
    }
  }

  markCompanyCoverage(checkpoint, company.slug, {
    cik: company.cik,
    eligible: report.filingsEligible,
    processedTotal: Object.keys(checkpoint.processedDocuments || {}).filter((k) =>
      k.startsWith(company.cik),
    ).length,
    remaining: report.filingsRemaining,
    scheduleHotels: report.scheduleHotels,
  });

  report.hotels = dedupeDiscoveredHotels(report.hotels);
  return report;
}

function buildMetrics(companyReports, inventory, mhReport, checkpoint, budget, clientStats) {
  const hotels = inventory.hotels || [];
  const mhHotels = mhReport.rows || [];
  const findings = hotels.flatMap((h) => h.contractFindings || []);
  const histExp = findings.filter(
    (f) =>
      f.historicalApplicability === HISTORICAL_APPLICABILITY.HISTORICALLY_VERIFIED &&
      (f.initialExpiration || String(f.verificationClass || "").includes("EXPIRATION")),
  );
  const currExp = findings.filter(
    (f) => f.historicalApplicability === HISTORICAL_APPLICABILITY.CURRENTLY_VERIFIED,
  );

  const eligible = companyReports.reduce((n, c) => n + (c.filingsEligible || 0), 0);
  const processed = companyReports.reduce((n, c) => n + (c.filingsProcessed || 0), 0);
  const remaining = companyReports.reduce((n, c) => n + (c.filingsRemaining || 0), 0);
  const years = companyReports
    .flatMap((c) => (c.coverage?.sinceYear != null ? [c.coverage.sinceYear] : []))
    .concat([SINCE_YEAR]);

  return {
    historicalPeriodCovered: `${Math.min(...years)}–present`,
    companiesResearched: companyReports.length,
    filingInventoriesSearched: companyReports.length,
    relevantFilingsEligible: eligible,
    relevantFilingsProcessedThisRun: processed,
    relevantFilingsRemainingInQueue: remaining,
    documentsAndExhibitsProcessed: companyReports.reduce((n, c) => n + c.documentsProcessed, 0),
    uniqueHotelsDiscovered: hotels.length,
    marriottAffiliatedHotels: mhReport.summaries?.marriott?.hotels || 0,
    hiltonAffiliatedHotels: mhReport.summaries?.hilton?.hotels || 0,
    hotelsWithVerifiedHistoricalSigningDates: findings.filter((f) =>
      String(f.verificationClass || "").includes("SIGNING"),
    ).length,
    hotelsWithVerifiedHistoricalExpirationDates: histExp.length,
    hotelsWithCurrentlyVerifiedExpirationDates: currExp.length,
    hotelsWithVerifiedRenewals: findings.filter((f) =>
      String(f.verificationClass || "").includes("RENEWAL"),
    ).length,
    hotelsWithVerifiedTerminations: findings.filter((f) =>
      String(f.verificationClass || "").includes("TERMINATION"),
    ).length,
    hotelsWithOwnershipOrManagementTransitions: findings.filter((f) =>
      /transition|replacement|conversion/i.test(f.snippet || ""),
    ).length,
    hotelsCurrentlyInHpc: hotels.filter(
      (h) => h.hpcMatchStatus === DISCOVERY_STATUS.DISCOVERED_MATCHED_HPC,
    ).length,
    hotelsMissingFromHpc: hotels.filter(
      (h) => h.hpcMatchStatus === DISCOVERY_STATUS.DISCOVERED_NOT_IN_HPC,
    ).length,
    findingsDurablyPersisted: hotels.length, // filesystem inventory
    readBackSuccessRate: null, // no Airtable apply this run
    ambiguousMatches: hotels.filter((h) => h.hpcMatchStatus === DISCOVERY_STATUS.MATCH_AMBIGUOUS)
      .length,
    researchExtractionErrors: companyReports.reduce((n, c) => n + (c.errors?.length || 0), 0),
    totalPaidResearchCostUsd: budget.snapshot().spentUsd,
    checkpointDocuments: Object.keys(checkpoint.processedDocuments || {}).length,
    clientStats,
    marriottHiltonInventoryRows: mhHotels.length,
    coverageDenominatorNote:
      "eligible = filings matching form/year filters; processed = this run; remaining = still queued. Batch finish ≠ research complete.",
  };
}

async function main() {
  const budget = createSecEdgarBudgetGuard();
  const client = OFFLINE ? null : createSecEdgarClient();
  const checkpoint = loadCheckpoint(defaultCheckpointPath(ROOT));
  const companies = await resolveCompanies(client);
  const metricsAcc = { documentsProcessed: 0 };
  const companyReports = [];

  for (const co of companies) {
    console.error(`[v2] researching ${co.slug} (${co.cik})…`);
    companyReports.push(await researchCompany(client, co, checkpoint, budget, metricsAcc));
    saveCheckpoint(checkpoint, defaultCheckpointPath(ROOT));
  }

  const allHotels = dedupeDiscoveredHotels(companyReports.flatMap((c) => c.hotels || []));
  const allFindings = companyReports.flatMap((c) => c.findings || []);
  const hist = applyHistoricalApplicability(allFindings);

  // Attach classified findings back onto hotels
  const findingsByHotel = new Map();
  for (const f of hist.findings) {
    const name = f.attributedHotelName || f.hotelNameHints?.[0];
    if (!name) continue;
    const key = String(name).toLowerCase();
    if (!findingsByHotel.has(key)) findingsByHotel.set(key, []);
    findingsByHotel.get(key).push(f);
  }

  const enriched = allHotels.map((h) => {
    const brand = detectBrandFamily(h.hotelName);
    const linked = findingsByHotel.get(String(h.hotelName).toLowerCase()) || h.contractFindings || [];
    return {
      ...h,
      brandFamily: h.brandFamily || brand.family,
      historicalBrand: h.historicalBrand || brand.brandLabel,
      contractFindings: linked.length ? linked : h.contractFindings || [],
    };
  });

  const hpcCandidates = loadHpcSeed();
  const matched = stage3Match(enriched, hpcCandidates);

  let inventory = loadDiscoveryInventory(defaultInventoryRoot(ROOT));
  inventory = upsertDiscoveredHotels(inventory, matched);
  inventory = dedupeInventoryByIdentity(inventory);
  const savedInventory = saveDiscoveryInventory(inventory, defaultInventoryRoot(ROOT));
  const invSummary = inventorySummary(savedInventory);

  // Brand-family view for MH reporting; full inventory retains all families.
  const mhOnly = !process.argv.includes("--all-brands");
  const mhReport = buildMarriottHiltonInventory(savedInventory.hotels, {
    marriottHiltonOnly: mhOnly,
  });
  if (mhOnly) {
    const universalReport = buildMarriottHiltonInventory(savedInventory.hotels, {
      marriottHiltonOnly: false,
    });
    fs.writeFileSync(
      path.join(OUT_DIR, "UNIVERSAL_HOTEL_INTELLIGENCE_INVENTORY.json"),
      JSON.stringify(
        {
          version: "sec-edgar-universal-hotel-inventory-v2",
          generatedAt: new Date().toISOString(),
          rowCount: universalReport.rowCount,
          summaries: universalReport.summaries,
          note: "Includes non-Marriott/Hilton brands when present; quality gate still required before persistence",
        },
        null,
        2,
      ),
      "utf8",
    );
  }
  fs.writeFileSync(
    path.join(OUT_DIR, "MARRIOTT_HILTON_HISTORICAL_CONTRACT_INVENTORY.json"),
    JSON.stringify(mhReport, null, 2),
    "utf8",
  );
  fs.writeFileSync(
    path.join(OUT_DIR, "MARRIOTT_HILTON_HISTORICAL_CONTRACT_INVENTORY.csv"),
    marriottHiltonInventoryToCsv(mhReport),
    "utf8",
  );
  fs.writeFileSync(
    path.join(OUT_DIR, "MARRIOTT_SUMMARY.json"),
    JSON.stringify({ summary: mhReport.summaries.marriott, rows: mhReport.rows.filter((r) => r.brandFamily === "MARRIOTT") }, null, 2),
    "utf8",
  );
  fs.writeFileSync(
    path.join(OUT_DIR, "HILTON_SUMMARY.json"),
    JSON.stringify({ summary: mhReport.summaries.hilton, rows: mhReport.rows.filter((r) => r.brandFamily === "HILTON") }, null, 2),
    "utf8",
  );

  const recon = reconcileDiscoveryInventoryToHpc(savedInventory, hpcCandidates, { apply: false });
  fs.writeFileSync(
    path.join(OUT_DIR, "HPC_RECONCILIATION_DRY_RUN.json"),
    JSON.stringify({ counts: recon.counts, newlyMatchable: recon.newlyMatchable, note: recon.note }, null, 2),
    "utf8",
  );

  const metrics = buildMetrics(
    companyReports,
    savedInventory,
    mhReport,
    checkpoint,
    budget,
    client ? client.stats() : { offline: true },
  );

  const report = {
    contract: SEC_EDGAR_CONTRACT_ID,
    historicalProgram: SEC_EDGAR_HISTORICAL_V2,
    generatedAt: new Date().toISOString(),
    mode: OFFLINE ? "OFFLINE_FIXTURE" : "DRY_RUN_BATCH",
    writeGuarantees: writeGuarantees(),
    schemaGaps: SCHEMA_GAPS_REQUIRING_FOUNDER_APPROVAL,
    discoveryInventory: {
      destination: DISCOVERY_INVENTORY_DESTINATION,
      summary: invSummary,
    },
    brandFamilySummary: brandFamilySummary(matched),
    batch: {
      batchSize: BATCH_SIZE,
      sinceYear: SINCE_YEAR,
      maxPriority: MAX_PRIORITY,
      companies: companies.map((c) => c.slug),
    },
    companyReports: companyReports.map((c) => ({
      ...c,
      hotels: (c.hotels || []).length,
      findings: (c.findings || []).length,
      sampleHotels: (c.hotels || []).slice(0, 10).map((h) => ({
        hotelName: h.hotelName,
        city: h.city,
        brandFamily: h.brandFamily || detectBrandFamily(h.hotelName).family,
        discoveryMethod: h.discoveryMethod,
      })),
    })),
    marriottHiltonSummaries: mhReport.summaries,
    reconciliation: { counts: recon.counts },
    metrics,
    expansionPlan: {
      next: "Continue checkpointed batches with --batch-size until remainingInQueue approaches 0 for priority≤2 REITs",
      prioritize: [
        "DiamondRock / Apple / RLJ / Park property schedules",
        "EX-10 property-specific agreements",
        "S-11 / 424B IPO & follow-on property portfolios",
        "Then broader Marriott/Hilton corporate disclosures",
      ],
      paidAi: "Still gated at $25 ceiling; deterministic schedule parsing preferred",
    },
    airtablePersistenceGate: "PERSISTENCE_DESIGNED",
    classificationNote:
      "Schedule expirations are HISTORICALLY_VERIFIED. Never promoted to CURRENTLY_VERIFIED without later evidence. Unmatched hotels retained in filesystem inventory.",
  };

  fs.writeFileSync(path.join(OUT_DIR, "RESEARCH_REPORT.json"), JSON.stringify(report, null, 2), "utf8");
  fs.writeFileSync(
    path.join(OUT_DIR, "METRICS.md"),
    `# Marriott & Hilton Historical Contract Intelligence V2

**Mode:** ${report.mode}  
**Generated:** ${report.generatedAt}

| Metric | Value |
|---|---|
${Object.entries(metrics)
  .map(([k, v]) => `| ${k} | ${typeof v === "object" ? JSON.stringify(v) : v} |`)
  .join("\n")}

## Coverage
Eligible filings found ≠ research complete. Remaining queue: **${metrics.relevantFilingsRemainingInQueue}**.
Resume with the same command; checkpoints skip completed documents.
`,
    "utf8",
  );

  console.log(
    JSON.stringify(
      {
        ok: true,
        outDir: OUT_DIR,
        mode: report.mode,
        metrics,
        marriott: mhReport.summaries.marriott,
        hilton: mhReport.summaries.hilton,
        inventorySummary: invSummary,
      },
      null,
      2,
    ),
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
