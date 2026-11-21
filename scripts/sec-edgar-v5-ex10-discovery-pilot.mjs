#!/usr/bin/env node
/**
 * V5 EX-10 discovery pilot — brand-neutral, ≤$25, cache-first EDGAR.
 *
 *   node scripts/sec-edgar-v5-ex10-discovery-pilot.mjs --run --limit=80
 *
 * No HPC. CUSTOMER_BLOCKED. No Meta schema apply.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import { createContractTimingV2Budget } from "../lib/hotel-intelligence/sec-edgar/contract-timing-v2-budget-v1.js";
import { createSecApiIoClient } from "../lib/hotel-intelligence/sec-edgar/sec-api-io-client-v1.js";
import { createExaContractTimingClient } from "../lib/hotel-intelligence/sec-edgar/exa-contract-timing-client-v1.js";
import { createSecEdgarClient } from "../lib/hotel-intelligence/sec-edgar/edgar-client-v1.js";
import {
  buildEx10DiscoveryQueries,
  harvestExhibitDocument,
  mineSecApiCacheForEx10Candidates,
  enrichCandidateFromEdgarUrl,
  AGREEMENT_INSTRUMENT_CLASS,
  SCOPE_CLASS,
} from "../lib/hotel-intelligence/sec-edgar/ex10-contract-harvest-v1.js";
import { classifyRenewalOutcome } from "../lib/hotel-intelligence/sec-edgar/contract-terms-renewal-classifier-v1.js";
import { listUniversalCompaniesV24 } from "../lib/hotel-intelligence/sec-edgar/cik-registry-v24.js";
import {
  appendCoverageRows,
  defaultCoverageLedgerPath,
  summarizeCoverageLedger,
  loadCoverageLedger,
  RETRIEVAL_STATUS,
  EXTRACTION_STATUS,
} from "../lib/hotel-intelligence/sec-edgar/sec-discovery-coverage-ledger-v3.js";
import { VERIFICATION_GATE } from "../lib/hotel-intelligence/sec-edgar/contract-intelligence-identity-layers-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of ["C:/Dev/dealality/my-operators-backend/.env", path.join(ROOT, ".env")]) {
  if (fs.existsSync(p)) dotenv.config({ path: p, override: false });
}

const OUT = path.join(ROOT, "reports/sec-edgar-factory-v5");
const LIMIT = Math.min(
  100,
  Math.max(50, Number((process.argv.find((a) => a.startsWith("--limit=")) || "").split("=")[1]) || 80),
);
const MAX_PAID = 25;

function ensureDir(d) {
  fs.mkdirSync(d, { recursive: true });
}
function writeJson(fp, obj) {
  ensureDir(path.dirname(fp));
  fs.writeFileSync(fp, JSON.stringify(obj, null, 2), "utf8");
}

function normalizeHit(h, channel) {
  return {
    channel,
    title: h.companyName || h.companyNameLong || h.title || null,
    cik: h.cik != null ? String(h.cik).padStart(10, "0") : null,
    accession: h.accessionNo || h.accessionNumber || h.accession || null,
    form: h.formType || h.form || null,
    filedAt: h.filedAt || h.filingDate || h.publishedDate || null,
    url: h.filingUrl || h.linkToHtml || h.linkToFilingDetails || h.url || null,
  };
}

async function discoverCandidates(budget, limit) {
  const queries = buildEx10DiscoveryQueries();
  const secApi = createSecApiIoClient({ budget });
  const exa = createExaContractTimingClient({ budget });
  const byKey = new Map();

  // Free: mine prior SEC-API full-text caches for EX-10 hotel exhibits
  const cacheDir = path.join(ROOT, "data/sec-edgar/sec-api-io-cache");
  for (const c of mineSecApiCacheForEx10Candidates(cacheDir, { limit: Math.max(limit * 2, 120) })) {
    const n = enrichCandidateFromEdgarUrl(c);
    if (n.url && !byKey.has(n.url)) byKey.set(n.url, n);
  }

  for (const q of queries) {
    if (byKey.size >= limit * 3) break;
    try {
      if (q.provider === "SEC_API_IO" && typeof secApi.fullTextSearch === "function") {
        const res = await secApi.fullTextSearch({
          query: q.query,
          formTypes: q.formTypes,
          page: "1",
        });
        if (res?.ok === false && res?.error) {
          // Network/subscription failure is non-fatal; cache + Exa continue
          continue;
        }
        const hits = res?.filings || res?.body?.filings || [];
        for (const h of hits) {
          const n = enrichCandidateFromEdgarUrl(normalizeHit(h, `SEC_API_IO:${q.id}`));
          const key = n.url || `${n.accession}|${n.title}`;
          if (key && !byKey.has(key)) byKey.set(key, n);
        }
      } else if (q.provider === "EXA") {
        const gate = budget.canSpend(0.02);
        if (!gate.ok) continue;
        const res = await exa.search({ query: q.query, numResults: q.numResults || 5, type: "lite" });
        const hits = res?.results || res?.result?.results || [];
        for (const h of hits) {
          const n = enrichCandidateFromEdgarUrl(normalizeHit(h, `EXA:${q.id}`));
          const key = n.url || n.title;
          if (key && !byKey.has(key) && /sec\.gov/i.test(n.url || "")) byKey.set(key, n);
        }
      }
    } catch (err) {
      byKey.set(`error:${q.id}`, { channel: q.id, error: String(err?.message || err) });
    }
  }

  // Prefer true exhibit document URLs, then diversify by CIK
  const ranked = [...byKey.values()]
    .filter((c) => c.url && !c.error)
    .sort((a, b) => {
      const ae = /ex[-_]?10|dex10|exv10/i.test(a.url) ? 1 : 0;
      const be = /ex[-_]?10|dex10|exv10/i.test(b.url) ? 1 : 0;
      return be - ae;
    });

  const selected = [];
  const cikCounts = new Map();
  for (const c of ranked) {
    if (selected.length >= limit) break;
    const cik = c.cik || "UNKNOWN";
    const n = cikCounts.get(cik) || 0;
    if (n >= 8 && cik !== "UNKNOWN") continue; // diversify issuers
    cikCounts.set(cik, n + 1);
    selected.push(c);
  }
  return selected;
}

async function laterDocumentProbe(exa, budget, harvest) {
  if (!harvest.issuerName && !harvest.sourceUrl) return { skipped: true };
  const gate = budget.canSpend(0.01);
  if (!gate.ok) return { skipped: "BUDGET" };
  const q = `"${harvest.issuerName || "hotel"}" (amendment OR renewed OR renewal OR terminated OR "management agreement") SEC`;
  try {
    const res = await exa.search({ query: q, numResults: 3, type: "lite" });
    const results = res?.results || [];
    const laterText = results.map((r) => `${r.title || ""} ${r.url || ""}`).join("\n");
    const classification = classifyRenewalOutcome({
      originalText: JSON.stringify(harvest.extracted || {}),
      laterText,
      scheduledExpiration: null,
    });
    return {
      query: q,
      hits: results.map((r) => ({ title: r.title, url: r.url })),
      renewalStatusClass: classification.renewalStatusClass,
      currentStatusVerified:
        classification.renewalStatusClass === "RENEWAL_EXECUTED_VERIFIED" ||
        classification.renewalStatusClass === "EXTENSION_EXECUTED_VERIFIED" ||
        classification.renewalStatusClass === "TERMINATED_VERIFIED",
    };
  } catch (err) {
    return { error: String(err?.message || err) };
  }
}

async function main() {
  ensureDir(OUT);
  const registry = listUniversalCompaniesV24();
  const registryCiks = new Set(registry.map((c) => String(c.cik || "").padStart(10, "0")));

  const budget = createContractTimingV2Budget({
    ceilingUsd: MAX_PAID,
    ledgerPath: path.join(ROOT, "data/sec-edgar/v5-ex10-budget-ledger.json"),
    cwd: ROOT,
  });
  const edgar = createSecEdgarClient();
  const exa = createExaContractTimingClient({ budget });

  const candidates = await discoverCandidates(budget, LIMIT);
  writeJson(path.join(OUT, "EX10_CANDIDATES.json"), { count: candidates.length, candidates });

  const harvested = [];
  const coverageRows = [];
  let retrieved = 0;
  const beyondRegistryCiks = new Set();

  for (const c of candidates) {
    if (budget.remaining() <= 0 && budget.canSpend(0.001).error === "BUDGET_STOPPED") break;
    let text = "";
    let fromCache = false;
    let retrievalError = null;
    try {
      const res = await edgar.request(c.url, {
        accept: "text/html,*/*",
        maxAgeMs: 30 * 24 * 60 * 60 * 1000,
      });
      if (res.ok) {
        text = res.text || "";
        fromCache = Boolean(res.fromCache);
        retrieved += 1;
      } else retrievalError = res.error || `HTTP_${res.status}`;
    } catch (err) {
      retrievalError = String(err?.message || err);
    }

    const h = harvestExhibitDocument(
      { html: text },
      {
        sourceUrl: c.url,
        accession: c.accession,
        cik: c.cik,
        issuerName: c.title,
      },
    );
    h.discoveryChannel = c.channel;
    h.filedAt = c.filedAt;
    h.retrievalError = retrievalError;
    h.fromCache = fromCache;

    const cikPad = c.cik ? String(c.cik).padStart(10, "0") : null;
    if (cikPad && !registryCiks.has(cikPad)) beyondRegistryCiks.add(cikPad);
    h.cik = cikPad || h.cik;
    h.issuerBeyondRegistry = Boolean(cikPad && !registryCiks.has(cikPad));

    const later = await laterDocumentProbe(exa, budget, h);
    h.laterDocument = later;
    if (later?.currentStatusVerified) {
      h.gates[VERIFICATION_GATE.CURRENT_STATUS_VERIFIED] = true;
    }

    harvested.push(h);
    coverageRows.push({
      cik: c.cik,
      issuerName: c.title,
      accession: c.accession,
      filingType: c.form || "EX-10",
      filingDate: c.filedAt,
      documentId: path.basename(c.url || "PRIMARY"),
      sourceUrl: c.url,
      retrievalStatus: retrievalError
        ? RETRIEVAL_STATUS.FAILED
        : fromCache
          ? RETRIEVAL_STATUS.CACHED
          : RETRIEVAL_STATUS.RETRIEVED,
      extractionStatus:
        h.disclosedFieldCount > 0 ? EXTRACTION_STATUS.PARTIAL : EXTRACTION_STATUS.NO_HOTEL_SIGNAL,
      hotelIdentitiesDiscovered: h.hotelAttributionAllowed ? ["NAMED_HOTEL_SCOPE"] : [],
      contractualIntelligenceExtracted: {
        expirationDates: 0,
        termProvisions: h.extracted?.originalTermYears != null ? 1 : 0,
        renewalProvisions: h.extracted?.renewalOptionsCount != null ? 1 : 0,
        terminationRights: h.extracted?.terminationForCause || h.extracted?.terminationWithoutCause ? 1 : 0,
        financialTerms: h.extracted?.managementFeeMention || h.extracted?.royaltyMention ? 1 : 0,
        managementAgreements: h.agreementType === "MANAGEMENT" ? 1 : 0,
        franchiseAgreements: h.agreementType === "FRANCHISE" ? 1 : 0,
      },
      discoveryChannel: c.channel,
      lastResearchDate: new Date().toISOString().slice(0, 10),
      followUpRequirements: h.gates[VERIFICATION_GATE.CURRENT_STATUS_VERIFIED]
        ? ["NONE"]
        : ["LATER_AMENDMENT_SEARCH"],
      costUsd: 0,
    });
  }

  const ledgerPath = defaultCoverageLedgerPath(ROOT);
  appendCoverageRows(coverageRows, ledgerPath);
  const ledgerSummary = summarizeCoverageLedger(loadCoverageLedger(ledgerPath));

  const byType = {};
  const byInstrument = {};
  const byScope = {};
  let existence = 0;
  let terms = 0;
  let current = 0;
  let withTermYears = 0;
  let withRenewal = 0;
  let withNotice = 0;
  let withFees = 0;

  for (const h of harvested) {
    byType[h.agreementType] = (byType[h.agreementType] || 0) + 1;
    byInstrument[h.instrumentClass] = (byInstrument[h.instrumentClass] || 0) + 1;
    byScope[h.scopeClass] = (byScope[h.scopeClass] || 0) + 1;
    if (h.gates[VERIFICATION_GATE.AGREEMENT_EXISTENCE_VERIFIED]) existence += 1;
    if (h.gates[VERIFICATION_GATE.CONTRACT_TERMS_VERIFIED]) terms += 1;
    if (h.gates[VERIFICATION_GATE.CURRENT_STATUS_VERIFIED]) current += 1;
    if (h.extracted?.originalTermYears != null) withTermYears += 1;
    if (h.extracted?.renewalOptionsCount != null || h.extracted?.renewalMode) withRenewal += 1;
    if (h.extracted?.renewalNoticeDays != null) withNotice += 1;
    if (h.extracted?.managementFeeMention || h.extracted?.royaltyMention) withFees += 1;
  }

  const spend = budget.snapshot();
  const metrics = {
    candidatesEnumerated: candidates.length,
    exhibitsRetrieved: retrieved,
    harvested: harvested.length,
    issuersBeyondRegistry: beyondRegistryCiks.size,
    issuersBeyondRegistryCiks: [...beyondRegistryCiks],
    registrySizeCompared: registry.length,
    byAgreementType: byType,
    byInstrumentClass: byInstrument,
    byScopeClass: byScope,
    agreementExistenceVerified: existence,
    contractTermsVerified: terms,
    currentStatusVerified: current,
    initialTermExtracted: withTermYears,
    renewalProvisionsFound: withRenewal,
    noticeDeadlinesFound: withNotice,
    financialTermsFound: withFees,
    formOfAgreements: byInstrument[AGREEMENT_INSTRUMENT_CLASS.FORM_OF_AGREEMENT] || 0,
    executedAgreements: byInstrument[AGREEMENT_INSTRUMENT_CLASS.EXECUTED_AGREEMENT] || 0,
    namedHotelScope: byScope[SCOPE_CLASS.NAMED_HOTEL] || 0,
    portfolioScope: byScope[SCOPE_CLASS.PORTFOLIO_LEVEL] || 0,
    spentUsd: spend.spentUsd,
    ceilingUsd: MAX_PAID,
    ledgerSummary,
    publicationState: "CUSTOMER_BLOCKED",
    hpcCreates: 0,
  };

  writeJson(path.join(OUT, "EX10_HARVEST_RESULTS.json"), { harvested });
  writeJson(path.join(OUT, "EX10_PILOT_METRICS.json"), metrics);
  writeJson(path.join(OUT, "EX10_PILOT_SPEND.json"), spend);

  console.log(JSON.stringify({ ok: true, metrics }, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
