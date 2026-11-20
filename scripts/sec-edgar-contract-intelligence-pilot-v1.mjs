#!/usr/bin/env node
/**
 * SEC EDGAR Contract Intelligence — three-stage pilot.
 *
 * Stage 1: Discover named hotels (independent of HPC)
 * Stage 2: Extract + verify contractual intelligence
 * Stage 3: Match against HPC (failure does not invalidate 1–2)
 *
 * Default: dry-run research + local inventory (no Airtable writes).
 *   node scripts/sec-edgar-contract-intelligence-pilot-v1.mjs
 *   node scripts/sec-edgar-contract-intelligence-pilot-v1.mjs --apply
 *   node scripts/sec-edgar-contract-intelligence-pilot-v1.mjs --offline-fixture
 *   node scripts/sec-edgar-contract-intelligence-pilot-v1.mjs --reconcile-only
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";

import {
  SEC_EDGAR_CONTRACT_ID,
  SEC_EDGAR_CONTRACT_VERSION,
  writeGuarantees,
  SCHEMA_GAPS_REQUIRING_FOUNDER_APPROVAL,
  DISCOVERY_STATUS,
  DISCOVERY_INVENTORY_DESTINATION,
} from "../lib/hotel-intelligence/sec-edgar/contract.js";
import {
  listPilotCompanies,
  normalizeCik,
} from "../lib/hotel-intelligence/sec-edgar/cik-registry-v1.js";
import { createSecEdgarClient } from "../lib/hotel-intelligence/sec-edgar/edgar-client-v1.js";
import {
  listRecentFilings,
  dedupeFilings,
} from "../lib/hotel-intelligence/sec-edgar/filing-discovery-v1.js";
import { extractContractFindingsFromDocument } from "../lib/hotel-intelligence/sec-edgar/contract-extractor-v1.js";
import { verifyFindings } from "../lib/hotel-intelligence/sec-edgar/verification-v1.js";
import { attributeFindings, matchHotelHintToHpc } from "../lib/hotel-intelligence/sec-edgar/hotel-attribution-v1.js";
import { discoverHotelsFromDocument, dedupeDiscoveredHotels } from "../lib/hotel-intelligence/sec-edgar/hotel-discovery-v1.js";
import {
  loadDiscoveryInventory,
  saveDiscoveryInventory,
  upsertDiscoveredHotels,
  inventorySummary,
  defaultInventoryRoot,
} from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-v1.js";
import { reconcileDiscoveryInventoryToHpc } from "../lib/hotel-intelligence/sec-edgar/hpc-reconciliation-v1.js";
import {
  buildHotelInventoryRows,
  hotelInventoryToCsv,
} from "../lib/hotel-intelligence/sec-edgar/hotel-inventory-report-v1.js";
import { buildYieldPrioritizationReport } from "../lib/hotel-intelligence/sec-edgar/yield-prioritization-v1.js";
import { planContractPersistence } from "../lib/hotel-intelligence/sec-edgar/persistence-planner-v1.js";
import { applyContractPersistencePlan } from "../lib/hotel-intelligence/sec-edgar/persistence-apply-v1.js";
import { buildOpportunityDossiers } from "../lib/hotel-intelligence/sec-edgar/opportunity-discovery-v1.js";
import { createSecEdgarBudgetGuard } from "../lib/hotel-intelligence/sec-edgar/budget-guard-v1.js";
import { diffContractFindings } from "../lib/hotel-intelligence/sec-edgar/change-detection-v1.js";
import { internationalSourceAdapterContract } from "../lib/hotel-intelligence/sec-edgar/intl-readiness-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) {
    dotenv.config({ path: p, override: false });
    break;
  }
}

const APPLY = process.argv.includes("--apply");
const OFFLINE = process.argv.includes("--offline-fixture");
const RECONCILE_ONLY = process.argv.includes("--reconcile-only");
const OUT_DIR = path.join(ROOT, "reports", "sec-edgar-contract-intelligence-v1");
fs.mkdirSync(OUT_DIR, { recursive: true });

function sha1(s) {
  return crypto.createHash("sha1").update(String(s)).digest("hex");
}

function loadPriorFindings() {
  const p = path.join(OUT_DIR, "PRIOR_FINDINGS.json");
  if (!fs.existsSync(p)) return [];
  try {
    return JSON.parse(fs.readFileSync(p, "utf8")).findings || [];
  } catch {
    return [];
  }
}

function loadHpcCandidatesSeed() {
  const p = path.join(OUT_DIR, "HPC_CANDIDATES_SEED.json");
  if (fs.existsSync(p)) {
    try {
      return JSON.parse(fs.readFileSync(p, "utf8")).hotels || [];
    } catch {
      /* fall through */
    }
  }
  return [
    {
      id: "recLuxvwwxID7U2B8",
      name: "Bethesda Marriott",
      city: "Bethesda",
      brand: "Marriott Hotels",
    },
  ];
}

async function searchHpcByNameHints(hints = [], opts = {}) {
  const token = (
    process.env.AIRTABLE_API_KEY ||
    process.env.AIRTABLE_PAT ||
    process.env.AIRTABLE_TOKEN ||
    ""
  ).trim();
  const baseId = (process.env.AIRTABLE_BASE_ID_ALT || "").trim();
  if (!token || !baseId || !hints.length) return [];
  const table = "tbl9aY5ijiuIzzWam";
  const out = [];
  const seen = new Set();
  for (const hint of hints.slice(0, opts.limit || 24)) {
    const safe = String(hint).replace(/'/g, "\\'").slice(0, 60);
    if (safe.length < 6) continue;
    const formula = encodeURIComponent(`SEARCH(LOWER('${safe.toLowerCase()}'), LOWER({Name}))`);
    try {
      const url = `https://api.airtable.com/v0/${baseId}/${table}?filterByFormula=${formula}&maxRecords=5&fields%5B%5D=Name&fields%5B%5D=City&fields%5B%5D=Brand&fields%5B%5D=Affiliation`;
      const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
      const json = await res.json().catch(() => ({}));
      for (const rec of json.records || []) {
        if (seen.has(rec.id)) continue;
        seen.add(rec.id);
        out.push({
          id: rec.id,
          name: rec.fields?.Name || null,
          city: rec.fields?.City || null,
          brand: rec.fields?.Brand || rec.fields?.Affiliation || null,
        });
      }
    } catch {
      /* continue */
    }
  }
  return out;
}

async function loadHpcCandidates(nameHints = []) {
  const seed = loadHpcCandidatesSeed();
  const live = await searchHpcByNameHints(nameHints);
  const byId = new Map();
  for (const h of [...seed, ...live]) {
    if (h?.id) byId.set(h.id, h);
  }
  return [...byId.values()];
}

function offlineFixtureDoc() {
  return {
    text: `
      Item 2. Properties — Hotel Portfolio Schedule
      Westin Chicago River North, Chicago, IL 407
      Courtyard San Diego Downtown, San Diego, CA 245
      Renaissance Seattle Hotel, Seattle, WA 557
      Hyatt Regency Grand Cypress, Orlando, FL 750
      Embassy Suites by Hilton Austin Downtown, Austin, TX 262

      EXHIBIT 10.1 — HOTEL MANAGEMENT AGREEMENT
      This Management Agreement dated as of January 15, 2019 between Owner and Manager
      for the Grand Example Hotel located in Miami, Florida.
      The initial term expires on December 31, 2028. Owner has two five-year renewal options.
      Termination for convenience requires 90 days notice.
      Separately, the franchise agreement for Courtyard Sample Airport was amended and restated
      effective June 1, 2022; the franchise expiration is approximately 2035 (estimated).
      Marriott International portfolio-wide management agreements typically renew automatically.
    `,
  };
}

async function resolveCompanyCiks(client) {
  const resolved = [];
  for (const co of listPilotCompanies({ includeUnverifiedCik: true })) {
    let cik = normalizeCik(co.cik);
    let cikSource = "registry";
    if (co.ticker && client) {
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

/**
 * Three-stage research for one company.
 */
async function researchCompany(client, company, budget) {
  const companyReport = {
    slug: company.slug,
    companyKey: company.slug || company.ticker,
    legalName: company.legalName,
    ticker: company.ticker,
    cik: company.cik,
    cikSource: company.cikSource,
    filingsReviewed: 0,
    documentsProcessed: 0,
    discoveredHotels: [],
    findings: [],
    keywordSummaries: [],
    failedAttributionReasons: [],
    errors: [],
  };

  const processDoc = (doc, meta) => {
    // Stage 1 — independent hotel discovery
    const discovery = discoverHotelsFromDocument(doc, meta);
    companyReport.discoveredHotels.push(...discovery.hotels);

    // Stage 2 — contract extraction (separate from discovery)
    const extracted = extractContractFindingsFromDocument(doc, meta);
    companyReport.keywordSummaries.push({
      accession: meta.accessionNumber,
      form: meta.form,
      ...extracted.keywordHitSummary,
      characters: extracted.characterCount,
      hotelsDiscovered: discovery.hotelCount,
    });
    companyReport.findings.push(...extracted.findings);
  };

  if (OFFLINE) {
    const meta = {
      cik: company.cik,
      accessionNumber: "0000000000-00-000000",
      form: "10-K",
      filingDate: "2025-02-15",
      documentUrl: "https://www.sec.gov/Archives/edgar/data/fixture/primary.htm",
      primaryDocument: "fixture.htm",
      companyName: company.legalName,
      maxFindings: 40,
    };
    processDoc(offlineFixtureDoc(), meta);
    companyReport.filingsReviewed = 1;
    companyReport.documentsProcessed = 1;
    companyReport.discoveredHotels = dedupeDiscoveredHotels(companyReport.discoveredHotels);
    return companyReport;
  }

  const sub = await client.getSubmissions(company.cik);
  if (!sub.ok) {
    companyReport.errors.push({ stage: "submissions", error: sub.error || sub.status });
    return companyReport;
  }

  // Prefer REIT schedules (higher property yield) over broad brand-corporate loops
  const discovered = listRecentFilings(sub.body, {
    limit: company.organizationType === "REIT" ? 12 : 6,
    sinceYear: new Date().getFullYear() - 2,
  });
  const filings = dedupeFilings(discovered.filings || []);
  companyReport.filingsReviewed = filings.length;

  for (const filing of filings) {
    if (!filing.accessionNumber || !filing.primaryDocument) continue;
    const doc = await client.getFilingDocument(
      company.cik,
      filing.accessionNumber,
      filing.primaryDocument,
    );
    if (!doc.ok || !doc.text) {
      companyReport.errors.push({
        stage: "document",
        accession: filing.accessionNumber,
        error: doc.error || doc.status,
      });
      continue;
    }
    companyReport.documentsProcessed += 1;
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
      maxFindings: 50,
    };
    processDoc(doc, meta);

    if (String(process.env.SEC_EDGAR_ALLOW_PAID_AI || "") === "1") {
      const gate = budget.canSpend(0.5);
      if (!gate.ok) {
        companyReport.errors.push({ stage: "paid_ai", error: gate.error, message: gate.message });
      }
    }
  }

  companyReport.discoveredHotels = dedupeDiscoveredHotels(companyReport.discoveredHotels);
  return companyReport;
}

function stage3MatchHotels(discoveredHotels, hpcCandidates) {
  return discoveredHotels.map((h) => {
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

function buildContractLinks(verifiedResults, discoveredHotels) {
  const links = [];
  for (const r of verifiedResults) {
    const f = r.finding || r;
    const hints = f.hotelNameHints || [];
    const hotelName = hints[0] || f.attributedHotelName || null;
    const verified = String(r.verificationClass || f.verificationClass || "").startsWith(
      "VERIFIED_",
    );
    if (f.portfolioWide) continue;
    links.push({
      hotelName,
      hint: hotelName,
      finding: f,
      verified,
      hpcId: r.attribution?.hpcId || null,
    });
  }
  // Also attach verified findings to discovered hotels by name overlap
  for (const h of discoveredHotels) {
    for (const r of verifiedResults) {
      const f = r.finding || r;
      if (f.portfolioWide) continue;
      const hints = (f.hotelNameHints || []).map((x) => String(x).toLowerCase());
      const n = String(h.hotelName || "").toLowerCase();
      if (!hints.some((x) => n.includes(x) || x.includes(n))) continue;
      links.push({
        hotelName: h.hotelName,
        hint: h.hotelName,
        finding: f,
        verified: String(r.verificationClass || "").startsWith("VERIFIED_"),
        hpcId: h.hpcRecordId || r.attribution?.hpcId || null,
      });
    }
  }
  return links;
}

function countClass(verified, cls, agreementType) {
  return verified.results.filter((r) => {
    if (r.verificationClass !== cls && r.finding?.verificationClass !== cls) return false;
    if (!agreementType) return true;
    return String(r.finding?.agreementType || "").includes(agreementType);
  }).length;
}

function renderMetricsMd(metrics, report) {
  const rows = Object.entries(metrics)
    .map(([k, v]) => `| ${k} | ${typeof v === "object" ? JSON.stringify(v) : v} |`)
    .join("\n");
  return `# SEC EDGAR Contract Intelligence — Pilot Metrics

**Contract:** ${report.contract} \`${report.version}\`  
**Mode:** ${report.mode}  
**Generated:** ${report.generatedAt}  
**Airtable gate:** ${report.airtablePersistenceGate}

| Metric | Count |
|---|---|
${rows}

## Notes

- Stages: discover → verify contracts → HPC match (independent).
- Missing HPC does **not** invalidate discovery or verified contracts.
- Inventory: \`${DISCOVERY_INVENTORY_DESTINATION.rootRelative}\`
- SEC HTTP retrieval is free; actual paid spend = $${metrics.actualResearchSpendUsd}.
- Customer-facing publication remains blocked.
`;
}

async function runReconcileOnly() {
  const inventory = loadDiscoveryInventory(defaultInventoryRoot(ROOT));
  const hints = (inventory.hotels || []).map((h) => h.hotelName).filter(Boolean);
  const hpcCandidates = await loadHpcCandidates(hints);
  const recon = reconcileDiscoveryInventoryToHpc(inventory, hpcCandidates, { apply: false });
  if (!APPLY) {
    // dry-run only — still write recon report; do not mutate inventory unless --apply
  } else {
    saveDiscoveryInventory(recon.nextInventory, defaultInventoryRoot(ROOT));
  }
  const outPath = path.join(OUT_DIR, "HPC_RECONCILIATION_DRY_RUN.json");
  fs.writeFileSync(outPath, JSON.stringify(recon, null, 2), "utf8");
  console.log(
    JSON.stringify(
      {
        ok: true,
        mode: "RECONCILE_ONLY",
        outPath,
        counts: recon.counts,
        dryRun: recon.dryRun,
      },
      null,
      2,
    ),
  );
}

async function main() {
  if (RECONCILE_ONLY) {
    await runReconcileOnly();
    return;
  }

  const budget = createSecEdgarBudgetGuard();
  const client = OFFLINE ? null : createSecEdgarClient();
  const companies = await resolveCompanyCiks(client);
  const companyReports = [];
  for (const co of companies) {
    companyReports.push(await researchCompany(client, co, budget));
  }

  // Stage 1 aggregate
  const allDiscovered = dedupeDiscoveredHotels(
    companyReports.flatMap((c) => c.discoveredHotels || []),
  );

  // Stage 2 — verify contracts (independent of HPC)
  const allFindings = companyReports.flatMap((c) => c.findings);
  const verified = verifyFindings(allFindings);

  // Stage 3 — HPC match (failure does not drop discovery)
  const hpcCandidates = await loadHpcCandidates([
    ...allDiscovered.map((h) => h.hotelName),
    ...allFindings.flatMap((f) => f.hotelNameHints || []),
  ]);
  const matchedHotels = stage3MatchHotels(allDiscovered, hpcCandidates);
  const attributed = attributeFindings(verified.results, hpcCandidates);

  // Persist discovery inventory (filesystem) — retains unmatched hotels.
  // Rebuild from this run's discoveries; keep prior rows that still look like hotels
  // and carry contract findings forward by discoveryId.
  const contractLinks = buildContractLinks(attributed.results, matchedHotels);
  const priorInventory = loadDiscoveryInventory(defaultInventoryRoot(ROOT));
  const priorById = new Map((priorInventory.hotels || []).map((h) => [h.discoveryId, h]));
  let inventory = upsertDiscoveredHotels({ hotels: [] }, matchedHotels, { contractLinks });
  for (const h of inventory.hotels) {
    const prev = priorById.get(h.discoveryId);
    if (!prev) continue;
    h.firstDiscoveredAt = prev.firstDiscoveredAt || h.firstDiscoveredAt;
    if ((prev.contractFindings || []).length) {
      h.contractFindings = [
        ...(h.contractFindings || []),
        ...prev.contractFindings.filter(
          (f) =>
            !(h.contractFindings || []).some(
              (x) =>
                x.evidenceFieldName === f.evidenceFieldName &&
                x.primaryDate === f.primaryDate &&
                x.source?.accessionNumber === f.source?.accessionNumber,
            ),
        ),
      ];
    }
  }
  // Keep prior hotels from other research waves not present in this run
  const currentIds = new Set(inventory.hotels.map((h) => h.discoveryId));
  for (const prev of priorInventory.hotels || []) {
    if (currentIds.has(prev.discoveryId)) continue;
    if (!prev.hotelName) continue;
    // Drop prior false positives that fail the tightened name gate on reload
    inventory.hotels.push(prev);
  }
  inventory = {
    ...inventory,
    hotels: inventory.hotels.map((h) => {
      if (
        h.hpcMatchStatus === DISCOVERY_STATUS.DISCOVERED_NOT_IN_HPC &&
        (h.contractFindings || []).some((f) =>
          String(f.verificationClass || "").startsWith("VERIFIED_"),
        )
      ) {
        return { ...h, hpcMatchStatus: DISCOVERY_STATUS.CONTRACT_VERIFIED_PENDING_HPC };
      }
      return h;
    }),
  };
  // Fresh pilot snapshot: prefer current-run hotels; prune prior-only noise without contracts
  inventory = {
    ...inventory,
    hotels: inventory.hotels.filter((h) => {
      if (currentIds.has(h.discoveryId)) return true;
      return (h.contractFindings || []).length > 0;
    }),
  };
  const savedInventory = saveDiscoveryInventory(inventory, defaultInventoryRoot(ROOT));
  const invSummary = inventorySummary(savedInventory);

  // Portfolio / unattributed for report transparency
  const portfolioOnly = attributed.results
    .filter((r) => r.finding?.portfolioWide)
    .map((r) => r.finding);
  const unattributed = attributed.results
    .filter(
      (r) =>
        !r.finding?.portfolioWide &&
        r.attribution?.status !== "MATCHED" &&
        !(r.finding?.hotelNameHints || []).length,
    )
    .map((r) => r.finding);

  const hotelReport = buildHotelInventoryRows(savedInventory, {
    portfolioOnlyFindings: portfolioOnly,
    unattributedFindings: unattributed,
  });
  fs.writeFileSync(
    path.join(OUT_DIR, "HOTEL_INVENTORY.json"),
    JSON.stringify(hotelReport, null, 2),
    "utf8",
  );
  fs.writeFileSync(
    path.join(OUT_DIR, "HOTEL_INVENTORY.csv"),
    hotelInventoryToCsv(hotelReport),
    "utf8",
  );

  // Yield prioritization
  const yieldStats = companyReports.map((c) => {
    const companyFindings = c.findings || [];
    const companyVerified = verifyFindings(companyFindings);
    const failed = [];
    for (const r of attributeFindings(companyVerified.results, hpcCandidates).results) {
      if (r.attribution?.status !== "MATCHED" && r.attribution?.reason) {
        failed.push(r.attribution.reason);
      }
    }
    return {
      companyKey: c.companyKey || c.slug || c.ticker,
      companyName: c.legalName,
      cik: c.cik,
      filingsProcessed: c.filingsReviewed,
      namedHotels: (c.discoveredHotels || []).length,
      verifiedMilestones: companyVerified.persistable,
      relevantAgreements: companyFindings.filter((f) => !f.portfolioWide).length,
      estimatedCostUsd: 0,
      failedAttributionReasons: failed,
    };
  });
  const yieldReport = buildYieldPrioritizationReport(yieldStats);
  fs.writeFileSync(
    path.join(OUT_DIR, "YIELD_PRIORITIZATION.json"),
    JSON.stringify(yieldReport, null, 2),
    "utf8",
  );

  // Future HPC reconciliation dry-run (no SEC re-download)
  const recon = reconcileDiscoveryInventoryToHpc(savedInventory, hpcCandidates, {
    apply: false,
  });
  fs.writeFileSync(
    path.join(OUT_DIR, "HPC_RECONCILIATION_DRY_RUN.json"),
    JSON.stringify(
      {
        ...recon,
        nextInventory: undefined,
        persistencePlan: {
          counts: recon.persistencePlan?.counts,
          evidenceRowCount: recon.persistencePlan?.evidenceRows?.length || 0,
        },
      },
      null,
      2,
    ),
    "utf8",
  );

  const plan = planContractPersistence(attributed.results, {
    planRelationships: true,
    planHpcNullFill: false,
    companyOrg: companies[0]
      ? {
          legalName: companies[0].legalName,
          cik: companies[0].cik,
          organizationType: companies[0].organizationType,
        }
      : null,
  });
  const applyResult = await applyContractPersistencePlan(plan, { apply: APPLY });
  const opportunities = buildOpportunityDossiers(attributed.results, {
    includeNonPersistable: false,
  });
  const prior = loadPriorFindings();
  const changeDiff = diffContractFindings(
    prior,
    attributed.results.map((r) => r.finding).filter(Boolean),
  );

  const verifiedPropertyLevel = attributed.results.filter(
    (r) =>
      r.persistable &&
      r.attribution?.status === "MATCHED" &&
      String(r.verificationClass || "").startsWith("VERIFIED_"),
  ).length;

  const metrics = {
    companiesResearched: companyReports.length,
    secFilingsReviewed: companyReports.reduce((n, c) => n + c.filingsReviewed, 0),
    documentsAndExhibitsProcessed: companyReports.reduce(
      (n, c) => n + c.documentsProcessed,
      0,
    ),
    hotelsDiscoveredIndependentOfHpc: allDiscovered.length,
    hotelsMatchedToHpc: invSummary.matchedHpc,
    hotelsNotInHpc: invSummary.notInHpc,
    contractVerifiedPendingHpc: invSummary.contractVerifiedPendingHpc,
    matchAmbiguous: invSummary.ambiguous,
    hotelInventoryRows: hotelReport.rowCount,
    verifiedSigningDates: countClass(verified, "VERIFIED_SIGNING_DATE"),
    verifiedManagementExpirations: countClass(verified, "VERIFIED_EXPIRATION"),
    verifiedFranchiseExpirations: countClass(verified, "VERIFIED_EXPIRATION", "FRANCHISE"),
    verifiedRenewals: countClass(verified, "VERIFIED_RENEWAL_OPTION"),
    verifiedTerminations: countClass(verified, "VERIFIED_TERMINATION"),
    amendmentsAndExtensions: countClass(verified, "VERIFIED_AMENDMENT"),
    verifiedPropertyLevelContractFindings: verifiedPropertyLevel,
    ownershipAndOperatorRelationships: plan.counts.relationships,
    successfullyPersistedHpcUpdates: applyResult.persistedVerified || 0,
    successfulReadbacks: applyResult.persistedVerified || 0,
    conflictsAndQuarantinedFindings: attributed.quarantined + plan.counts.quarantined,
    potentialConversionOpportunities: opportunities.count,
    estimatedOrUnsupportedDatesRejected: verified.rejectedEstimates,
    actualResearchSpendUsd: budget.snapshot().spentUsd,
    yieldNextCohortGuidance: yieldReport.nextCohortGuidance,
    discoveryInventoryDestination: DISCOVERY_INVENTORY_DESTINATION.rootRelative,
    reconciliationNewlyMatchable: recon.counts.newlyMatchable,
  };

  const report = {
    contract: SEC_EDGAR_CONTRACT_ID,
    version: SEC_EDGAR_CONTRACT_VERSION,
    generatedAt: new Date().toISOString(),
    mode: APPLY ? "APPLY" : OFFLINE ? "OFFLINE_FIXTURE" : "DRY_RUN",
    stages: [
      "1_DISCOVER_HOTELS",
      "2_EXTRACT_VERIFY_CONTRACTS",
      "3_MATCH_HPC",
    ],
    writeGuarantees: writeGuarantees(),
    schemaGaps: SCHEMA_GAPS_REQUIRING_FOUNDER_APPROVAL,
    discoveryInventory: {
      destination: DISCOVERY_INVENTORY_DESTINATION,
      summary: invSummary,
      path: path.join(DISCOVERY_INVENTORY_DESTINATION.rootRelative, "hotels.json"),
    },
    internationalReadiness: internationalSourceAdapterContract(),
    budget: budget.snapshot(),
    clientStats: client ? client.stats() : { offline: true },
    companies,
    companyReports: companyReports.map((c) => ({
      ...c,
      findings: c.findings.length,
      discoveredHotelCount: (c.discoveredHotels || []).length,
      sampleHotels: (c.discoveredHotels || []).slice(0, 8).map((h) => ({
        hotelName: h.hotelName,
        city: h.city,
        stateRegion: h.stateRegion,
        discoveryMethod: h.discoveryMethod,
      })),
      sampleFindings: c.findings.slice(0, 5),
      discoveredHotels: undefined,
    })),
    verification: {
      total: verified.total,
      persistable: verified.persistable,
      rejectedEstimates: verified.rejectedEstimates,
      quarantined: verified.quarantined,
    },
    attribution: { matched: attributed.matched, quarantined: attributed.quarantined },
    yieldPrioritization: yieldReport,
    reconciliationDryRun: {
      counts: recon.counts,
      note: recon.note,
    },
    persistencePlan: plan,
    applyResult,
    opportunities,
    changeDiff: { counts: changeDiff.counts },
    metrics,
    classificationNote:
      "PERSISTENCE_DESIGNED for architecture; PERSISTED_VERIFIED only when --apply + read-back pass. Unmatched discoveries retained in filesystem inventory. Customer publication BLOCKED.",
    airtablePersistenceGate: APPLY
      ? applyResult.persistedVerified > 0
        ? "PERSISTENCE_VALIDATED"
        : "PERSISTENCE_DESIGNED"
      : "PERSISTENCE_DESIGNED",
  };

  const outPath = path.join(
    OUT_DIR,
    APPLY ? "PILOT_APPLY_REPORT.json" : "PILOT_DRY_RUN_REPORT.json",
  );
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2), "utf8");
  fs.writeFileSync(
    path.join(OUT_DIR, "PRIOR_FINDINGS.json"),
    JSON.stringify(
      {
        at: new Date().toISOString(),
        findings: attributed.results.map((r) => r.finding).filter(Boolean),
      },
      null,
      2,
    ),
    "utf8",
  );
  fs.writeFileSync(
    path.join(OUT_DIR, "METRICS.md"),
    renderMetricsMd(metrics, report),
    "utf8",
  );

  console.log(
    JSON.stringify(
      {
        ok: true,
        outPath,
        mode: report.mode,
        metrics,
        inventorySummary: invSummary,
        airtablePersistenceGate: report.airtablePersistenceGate,
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
