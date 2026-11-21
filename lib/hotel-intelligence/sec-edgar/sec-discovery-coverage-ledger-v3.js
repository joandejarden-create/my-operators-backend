/**
 * Document-grain SEC discovery coverage ledger (V3).
 * Durable tracking for incremental backfill + ongoing discovery.
 * Does NOT claim exhaustive hotel-SEC coverage when the ledger is incomplete.
 */

import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";

export const SEC_DISCOVERY_COVERAGE_LEDGER_V3 = "sec-discovery-coverage-ledger-v3";

export const RETRIEVAL_STATUS = Object.freeze({
  NOT_QUEUED: "NOT_QUEUED",
  QUEUED: "QUEUED",
  RETRIEVED: "RETRIEVED",
  CACHED: "CACHED",
  FAILED: "FAILED",
  SKIPPED_BUDGET: "SKIPPED_BUDGET",
  SKIPPED_DEDUPE: "SKIPPED_DEDUPE",
});

export const EXTRACTION_STATUS = Object.freeze({
  NOT_STARTED: "NOT_STARTED",
  PARTIAL: "PARTIAL",
  COMPLETE: "COMPLETE",
  NO_HOTEL_SIGNAL: "NO_HOTEL_SIGNAL",
  FAILED: "FAILED",
  DEFERRED: "DEFERRED",
});

export const FOLLOW_UP = Object.freeze({
  NONE: "NONE",
  ENUMERATE_EXHIBITS: "ENUMERATE_EXHIBITS",
  FETCH_IBR: "FETCH_INCORPORATED_BY_REFERENCE",
  ENRICH_CONTRACT_TERMS: "ENRICH_CONTRACT_TERMS",
  LATER_AMENDMENT_SEARCH: "LATER_AMENDMENT_SEARCH",
  HPC_RECONCILE_AMERICAS_EUROPE: "HPC_RECONCILE_AMERICAS_EUROPE",
  INTL_SOURCE_EXPAND: "INTL_SOURCE_EXPAND",
  MANUAL_REVIEW: "MANUAL_REVIEW",
});

export function defaultCoverageLedgerPath(cwd = process.cwd()) {
  return path.join(cwd, "data", "sec-edgar", "coverage-ledger-v3", "ledger.jsonl");
}

export function coverageUnitId({ cik, accession, documentId }) {
  const raw = [
    String(cik || "").replace(/\D/g, "").padStart(10, "0"),
    String(accession || "").replace(/-/g, ""),
    String(documentId || "PRIMARY"),
  ].join("|");
  return `cov_${crypto.createHash("sha1").update(raw).digest("hex").slice(0, 20)}`;
}

/**
 * Canonical row shape for the durable ledger.
 */
export function buildCoverageRow(input = {}) {
  const cik = String(input.cik || "").replace(/\D/g, "").padStart(10, "0");
  const accession = input.accession || null;
  const documentId = input.documentId || input.primaryDocument || "PRIMARY";
  return {
    version: SEC_DISCOVERY_COVERAGE_LEDGER_V3,
    coverageUnitId: coverageUnitId({ cik, accession, documentId }),
    cik: cik === "0000000000" ? null : cik,
    issuerSlug: input.issuerSlug || null,
    issuerName: input.issuerName || null,
    accession,
    filingType: input.filingType || input.formType || null,
    filingDate: input.filingDate || null,
    documentId,
    exhibitType: input.exhibitType || null,
    sourceUrl: input.sourceUrl || null,
    retrievalStatus: input.retrievalStatus || RETRIEVAL_STATUS.NOT_QUEUED,
    extractionStatus: input.extractionStatus || EXTRACTION_STATUS.NOT_STARTED,
    hotelIdentitiesDiscovered: input.hotelIdentitiesDiscovered || [],
    contractualIntelligenceExtracted: input.contractualIntelligenceExtracted || {
      expirationDates: 0,
      termProvisions: 0,
      renewalProvisions: 0,
      terminationRights: 0,
      financialTerms: 0,
      managementAgreements: 0,
      franchiseAgreements: 0,
    },
    geographyHints: input.geographyHints || [],
    discoveryChannel: input.discoveryChannel || null,
    queryFamilyId: input.queryFamilyId || null,
    lastResearchDate: input.lastResearchDate || null,
    followUpRequirements: input.followUpRequirements || [FOLLOW_UP.NONE],
    costUsd: Number(input.costUsd) || 0,
    notes: input.notes || null,
    updatedAt: new Date().toISOString(),
  };
}

export function appendCoverageRows(rows, filePath = defaultCoverageLedgerPath()) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  const lines = (rows || []).map((r) => JSON.stringify(buildCoverageRow(r)));
  fs.appendFileSync(filePath, `${lines.join("\n")}\n`, "utf8");
  return { ok: true, appended: lines.length, filePath };
}

export function loadCoverageLedger(filePath = defaultCoverageLedgerPath()) {
  if (!fs.existsSync(filePath)) {
    return { version: SEC_DISCOVERY_COVERAGE_LEDGER_V3, rows: [], path: filePath, empty: true };
  }
  const rows = fs
    .readFileSync(filePath, "utf8")
    .split(/\n/)
    .map((l) => l.trim())
    .filter(Boolean)
    .map((l) => {
      try {
        return JSON.parse(l);
      } catch {
        return null;
      }
    })
    .filter(Boolean);
  return { version: SEC_DISCOVERY_COVERAGE_LEDGER_V3, rows, path: filePath, empty: false };
}

/**
 * Aggregate measurable coverage — never claims "all SEC hotel intelligence discovered".
 */
export function summarizeCoverageLedger(ledgerOrRows) {
  const rows = Array.isArray(ledgerOrRows) ? ledgerOrRows : ledgerOrRows?.rows || [];
  const byRetrieval = {};
  const byExtraction = {};
  const byForm = {};
  const ciks = new Set();
  const accessions = new Set();
  let hotels = 0;
  let costUsd = 0;
  const followUps = {};

  for (const r of rows) {
    byRetrieval[r.retrievalStatus] = (byRetrieval[r.retrievalStatus] || 0) + 1;
    byExtraction[r.extractionStatus] = (byExtraction[r.extractionStatus] || 0) + 1;
    byForm[r.filingType || "UNKNOWN"] = (byForm[r.filingType || "UNKNOWN"] || 0) + 1;
    if (r.cik) ciks.add(r.cik);
    if (r.accession) accessions.add(r.accession);
    hotels += (r.hotelIdentitiesDiscovered || []).length;
    costUsd += Number(r.costUsd) || 0;
    for (const f of r.followUpRequirements || []) {
      followUps[f] = (followUps[f] || 0) + 1;
    }
  }

  return {
    version: SEC_DISCOVERY_COVERAGE_LEDGER_V3,
    unitCount: rows.length,
    uniqueCiks: ciks.size,
    uniqueAccessions: accessions.size,
    hotelIdentityMentions: hotels,
    byRetrieval,
    byExtraction,
    byForm,
    followUps,
    costUsd,
    exhaustiveClaimForbidden: true,
    note: "Counts measure ledger completeness only — not total SEC hotel contractual universe.",
  };
}

export default {
  buildCoverageRow,
  appendCoverageRows,
  loadCoverageLedger,
  summarizeCoverageLedger,
  coverageUnitId,
  RETRIEVAL_STATUS,
  EXTRACTION_STATUS,
  FOLLOW_UP,
};
