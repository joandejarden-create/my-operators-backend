/**
 * Build and persist the complete eligible filing inventory BEFORE downloads.
 * Distinguishes form types from exhibit document types.
 */

import fs from "node:fs";
import path from "node:path";
import {
  listHistoricalFilings,
  loadAllSubmissionBodies,
  dedupeFilings,
  HISTORICAL_FORM_TYPES,
} from "./filing-discovery-v2.js";

export const ELIGIBLE_FILING_INVENTORY_V2 = "sec-edgar-eligible-filing-inventory-v2";

export function defaultEligibleInventoryDir(cwd = process.cwd()) {
  return path.join(cwd, "data", "sec-edgar", "eligible-filings");
}

/**
 * Classify a submissions row as FORM vs possible EXHIBIT-labelled form string.
 * True EX-10 exhibits usually live inside a filing package index — noted as gap.
 */
export function classifyFilingEntry(filing) {
  const form = String(filing.form || "").toUpperCase();
  if (/^EX-10/.test(form)) {
    return {
      entryKind: "EXHIBIT_AS_FORM_ROW",
      formType: form,
      note: "Submissions listed an EX-10* form string; still verify via filing index when available.",
    };
  }
  return {
    entryKind: "FORM",
    formType: form,
    note: "Primary form filing; exhibits may exist under this accession via index.json.",
  };
}

/**
 * Build complete eligible inventory for one CIK (no document downloads).
 */
export async function buildEligibleFilingInventory(client, company, opts = {}) {
  const sinceYear = opts.sinceYear != null ? Number(opts.sinceYear) : 1994;
  const bodies = await loadAllSubmissionBodies(client, company.cik);
  if (!bodies.ok) {
    return {
      ok: false,
      error: bodies.error,
      company,
      filings: [],
      coverage: { eligibleFound: 0, remainingInQueue: 0 },
    };
  }

  const listed = listHistoricalFilings(bodies.primary, {
    additionalBodies: bodies.additional,
    sinceYear,
    formTypes: opts.formTypes || HISTORICAL_FORM_TYPES,
    limit: null, // complete inventory — no truncate
    prioritizeHighYield: true,
  });

  const filings = dedupeFilings(listed.filings || []).map((f) => ({
    ...f,
    ...classifyFilingEntry(f),
    cik: company.cik,
    companySlug: company.slug,
    companyName: company.legalName,
  }));

  return {
    ok: true,
    version: ELIGIBLE_FILING_INVENTORY_V2,
    company: {
      slug: company.slug,
      legalName: company.legalName,
      ticker: company.ticker,
      cik: company.cik,
    },
    additionalShardsLoaded: bodies.loadedAdditional,
    additionalShardsListed: bodies.additionalFileCount,
    coverage: {
      sinceYear,
      eligibleFound: filings.length,
      formEntries: filings.filter((f) => f.entryKind === "FORM").length,
      exhibitAsFormRows: filings.filter((f) => f.entryKind === "EXHIBIT_AS_FORM_ROW").length,
      processed: 0,
      remainingInQueue: filings.length,
      yearSpan: yearSpan(filings),
      formTypeCounts: countBy(filings, (f) => f.formType),
    },
    filings,
    exhibitGapNote:
      "EX-10 contractual exhibits are typically nested under a parent 10-K/8-K accession. Enumerate via Archives .../index.json in a follow-on pass; do not assume EX-10 is a standalone submissions form.",
  };
}

function yearSpan(filings) {
  const years = filings
    .map((f) => Number(String(f.filingDate || "").slice(0, 4)))
    .filter((y) => Number.isFinite(y));
  if (!years.length) return null;
  return { min: Math.min(...years), max: Math.max(...years) };
}

function countBy(rows, fn) {
  const out = {};
  for (const r of rows) {
    const k = fn(r) || "UNKNOWN";
    out[k] = (out[k] || 0) + 1;
  }
  return out;
}

export function saveEligibleFilingInventory(inventory, dir = defaultEligibleInventoryDir()) {
  fs.mkdirSync(dir, { recursive: true });
  const slug = inventory.company?.slug || inventory.company?.cik || "unknown";
  const filePath = path.join(dir, `${slug}.json`);
  const payload = {
    ...inventory,
    savedAt: new Date().toISOString(),
  };
  fs.writeFileSync(filePath, JSON.stringify(payload, null, 2), "utf8");
  // Slim index without full filing list for dashboards
  const indexPath = path.join(dir, `_index.json`);
  let index = { version: ELIGIBLE_FILING_INVENTORY_V2, companies: {} };
  if (fs.existsSync(indexPath)) {
    try {
      index = JSON.parse(fs.readFileSync(indexPath, "utf8"));
    } catch {
      /* keep default */
    }
  }
  index.companies[slug] = {
    cik: inventory.company?.cik,
    coverage: inventory.coverage,
    path: `${slug}.json`,
    savedAt: payload.savedAt,
  };
  index.updatedAt = payload.savedAt;
  fs.writeFileSync(indexPath, JSON.stringify(index, null, 2), "utf8");
  return { filePath, indexPath };
}

/**
 * Select next batch from a persisted eligible inventory, skipping processed keys.
 */
export function selectBatchFromEligibleInventory(inventory, opts = {}) {
  const batchSize = Math.max(1, Number(opts.batchSize || 12) || 12);
  const isProcessed = opts.isProcessed || (() => false);
  const selected = [];
  let skipped = 0;
  for (const f of inventory.filings || []) {
    if (isProcessed(f.cik, f.accessionNumber, f.primaryDocument)) {
      skipped += 1;
      continue;
    }
    selected.push(f);
    if (selected.length >= batchSize) break;
  }
  return {
    selected,
    skippedProcessed: skipped,
    remainingAfterBatch: Math.max(
      0,
      (inventory.filings || []).length - skipped - selected.length,
    ),
  };
}
