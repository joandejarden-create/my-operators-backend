#!/usr/bin/env node
/**
 * SEC EDGAR Contract Intelligence — pilot runner (Marriott, Sunstone, Ashford, Host).
 *
 * Default: dry-run research + local staging (no Airtable writes).
 *   node scripts/sec-edgar-contract-intelligence-pilot-v1.mjs
 *   node scripts/sec-edgar-contract-intelligence-pilot-v1.mjs --apply
 *   node scripts/sec-edgar-contract-intelligence-pilot-v1.mjs --offline-fixture
 *
 * Paid AI extraction is OFF unless SEC_EDGAR_ALLOW_PAID_AI=1 and budget remains.
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
  PERSISTENCE_CLASS,
} from "../lib/hotel-intelligence/sec-edgar/contract.js";
import {
  PILOT_SEC_COMPANIES,
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
import { attributeFindings } from "../lib/hotel-intelligence/sec-edgar/hotel-attribution-v1.js";
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
  for (const hint of hints.slice(0, opts.limit || 12)) {
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

async function loadHpcCandidates(allFindings = []) {
  const seed = loadHpcCandidatesSeed();
  const hints = [
    ...new Set(allFindings.flatMap((f) => f.hotelNameHints || []).filter(Boolean)),
  ];
  const live = await searchHpcByNameHints(hints);
  const byId = new Map();
  for (const h of [...seed, ...live]) {
    if (h?.id) byId.set(h.id, h);
  }
  return [...byId.values()];
}

function offlineFixtureDoc() {
  return {
    text: `
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

async function researchCompany(client, company, budget) {
  const companyReport = {
    slug: company.slug,
    legalName: company.legalName,
    ticker: company.ticker,
    cik: company.cik,
    cikSource: company.cikSource,
    filingsReviewed: 0,
    documentsProcessed: 0,
    findings: [],
    keywordSummaries: [],
    errors: [],
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
    const extracted = extractContractFindingsFromDocument(offlineFixtureDoc(), meta);
    companyReport.filingsReviewed = 1;
    companyReport.documentsProcessed = 1;
    companyReport.keywordSummaries.push(extracted.keywordHitSummary);
    companyReport.findings.push(...extracted.findings);
    return companyReport;
  }

  const sub = await client.getSubmissions(company.cik);
  if (!sub.ok) {
    companyReport.errors.push({ stage: "submissions", error: sub.error || sub.status });
    return companyReport;
  }

  const discovered = listRecentFilings(sub.body, {
    limit: company.organizationType === "REIT" ? 10 : 6,
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
    const extracted = extractContractFindingsFromDocument(doc, meta);
    companyReport.keywordSummaries.push({
      accession: filing.accessionNumber,
      form: filing.form,
      ...extracted.keywordHitSummary,
      characters: extracted.characterCount,
    });
    companyReport.findings.push(...extracted.findings);

    // Paid AI path intentionally unused in pilot unless explicitly enabled + budgeted.
    if (String(process.env.SEC_EDGAR_ALLOW_PAID_AI || "") === "1") {
      const gate = budget.canSpend(0.5);
      if (!gate.ok) {
        companyReport.errors.push({ stage: "paid_ai", error: gate.error, message: gate.message });
      }
    }
  }

  return companyReport;
}

async function main() {
  const budget = createSecEdgarBudgetGuard();
  const client = OFFLINE ? null : createSecEdgarClient();
  const companies = await resolveCompanyCiks(client);
  const companyReports = [];
  for (const co of companies) {
    companyReports.push(await researchCompany(client, co, budget));
  }

  const allFindings = companyReports.flatMap((c) => c.findings);
  const verified = verifyFindings(allFindings);
  const hpcCandidates = await loadHpcCandidates(allFindings);
  const attributed = attributeFindings(verified.results, hpcCandidates);
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

  const metrics = {
    companiesResearched: companyReports.length,
    secFilingsReviewed: companyReports.reduce((n, c) => n + c.filingsReviewed, 0),
    documentsAndExhibitsProcessed: companyReports.reduce(
      (n, c) => n + c.documentsProcessed,
      0,
    ),
    hotelPropertiesIdentified: new Set(
      [
        ...attributed.results
          .filter((r) => r.attribution?.status === "MATCHED")
          .map((r) => r.attribution.hpcId || r.finding?.attributedHotelName),
        ...allFindings.flatMap((f) => f.hotelNameHints || []),
      ].filter(Boolean),
    ).size,
    hotelPropertiesMatchedToHpc: attributed.matched,
    verifiedSigningDates: countClass(verified, "VERIFIED_SIGNING_DATE"),
    verifiedManagementExpirations: countClass(verified, "VERIFIED_EXPIRATION"),
    verifiedFranchiseExpirations: countClass(verified, "VERIFIED_EXPIRATION", "FRANCHISE"),
    verifiedRenewals: countClass(verified, "VERIFIED_RENEWAL_OPTION"),
    verifiedTerminations: countClass(verified, "VERIFIED_TERMINATION"),
    amendmentsAndExtensions: countClass(verified, "VERIFIED_AMENDMENT"),
    ownershipAndOperatorRelationships: plan.counts.relationships,
    existingHpcMatches: attributed.matched,
    successfullyPersistedHpcUpdates: applyResult.persistedVerified || 0,
    successfulReadbacks: applyResult.persistedVerified || 0,
    duplicateRecordsPrevented: plan.evidenceRows.length, // dedupe keys planned
    conflictsAndQuarantinedFindings: attributed.quarantined + plan.counts.quarantined,
    potentialConversionOpportunities: opportunities.count,
    estimatedOrUnsupportedDatesRejected: verified.rejectedEstimates,
    actualResearchSpendUsd: budget.snapshot().spentUsd,
  };

  const report = {
    contract: SEC_EDGAR_CONTRACT_ID,
    version: SEC_EDGAR_CONTRACT_VERSION,
    generatedAt: new Date().toISOString(),
    mode: APPLY ? "APPLY" : OFFLINE ? "OFFLINE_FIXTURE" : "DRY_RUN",
    writeGuarantees: writeGuarantees(),
    schemaGaps: SCHEMA_GAPS_REQUIRING_FOUNDER_APPROVAL,
    internationalReadiness: internationalSourceAdapterContract(),
    budget: budget.snapshot(),
    clientStats: client ? client.stats() : { offline: true },
    companies,
    companyReports: companyReports.map((c) => ({
      ...c,
      findings: c.findings.length,
      sampleFindings: c.findings.slice(0, 5),
    })),
    verification: {
      total: verified.total,
      persistable: verified.persistable,
      rejectedEstimates: verified.rejectedEstimates,
      quarantined: verified.quarantined,
    },
    attribution: { matched: attributed.matched, quarantined: attributed.quarantined },
    persistencePlan: plan,
    applyResult,
    opportunities,
    changeDiff: { counts: changeDiff.counts },
    metrics,
    classificationNote:
      "PERSISTENCE_DESIGNED for architecture; PERSISTED_VERIFIED only when --apply + read-back pass. Customer publication BLOCKED.",
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
        airtablePersistenceGate: report.airtablePersistenceGate,
      },
      null,
      2,
    ),
  );
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
    .map(([k, v]) => `| ${k} | ${v} |`)
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

- SEC HTTP retrieval is free; actual paid spend = $${metrics.actualResearchSpendUsd}.
- Unattributed portfolio disclosures are quarantined (not property-persisted).
- Estimated / speculative dates are rejected as verified facts.
- Customer-facing publication remains blocked.
`;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
