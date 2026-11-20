/**
 * Reconcile eligible-filing inventories, checkpoints, and exhibit enumeration.
 * Prevents double-counting and separates company cohorts.
 */

import fs from "node:fs";
import path from "node:path";

export const COVERAGE_LEDGER_V2 = "sec-edgar-coverage-ledger-v2";

export function defaultLedgerPaths(cwd = process.cwd()) {
  return {
    eligibleDir: path.join(cwd, "data", "sec-edgar", "eligible-filings"),
    checkpointPath: path.join(
      cwd,
      "data",
      "sec-edgar",
      "checkpoints",
      "marriott-hilton-historical-v2.json",
    ),
    exhibitLedgerPath: path.join(cwd, "data", "sec-edgar", "exhibit-ledger", "exhibits-v2.json"),
  };
}

/**
 * @param {object} opts
 * @param {string[]} [opts.coreCohort] — e.g. six-company 3863 universe
 * @param {string[]} [opts.extendedCohort] — Apple, RLJ, etc.
 */
export function buildCoverageLedger(opts = {}) {
  const paths = { ...defaultLedgerPaths(opts.cwd || process.cwd()), ...(opts.paths || {}) };
  const coreCohort = opts.coreCohort || [
    "marriott-international",
    "hilton-worldwide",
    "sunstone-hotel-investors",
    "host-hotels-resorts",
    "ashford-hospitality-trust",
    "diamondrock-hospitality",
  ];
  const extendedCohort = opts.extendedCohort || [
    "apple-hospitality-reit",
    "rlj-lodging-trust",
    "park-hotels-resorts",
  ];
  // Known CIK → slug for checkpoint rows without eligible inventory yet
  const cikToSlug = {
    "0001511337": "rlj-lodging-trust",
    "0001418121": "apple-hospitality-reit",
    ...(opts.cikToSlug || {}),
  };

  const indexPath = path.join(paths.eligibleDir, "_index.json");
  let eligibleIndex = { companies: {} };
  if (fs.existsSync(indexPath)) {
    try {
      eligibleIndex = JSON.parse(fs.readFileSync(indexPath, "utf8"));
    } catch {
      /* empty */
    }
  }

  let checkpoint = { processedDocuments: {}, companies: {} };
  if (fs.existsSync(paths.checkpointPath)) {
    try {
      checkpoint = JSON.parse(fs.readFileSync(paths.checkpointPath, "utf8"));
    } catch {
      /* empty */
    }
  }

  let exhibitLedger = { exhibitsEnumerated: 0, exhibitsProcessed: 0, byParent: {} };
  if (fs.existsSync(paths.exhibitLedgerPath)) {
    try {
      exhibitLedger = JSON.parse(fs.readFileSync(paths.exhibitLedgerPath, "utf8"));
    } catch {
      /* empty */
    }
  }

  const processedKeys = Object.keys(checkpoint.processedDocuments || {});
  const processedByCik = {};
  for (const key of processedKeys) {
    const cik = key.split("|")[0];
    processedByCik[cik] = (processedByCik[cik] || 0) + 1;
  }

  function companyRow(slug) {
    const meta = eligibleIndex.companies?.[slug] || {};
    const coverage = meta.coverage || {};
    const eligible = Number(coverage.eligibleFound || 0);
    // Match processed docs whose key cik matches company file if available
    let cik = meta.cik || null;
    if (!cik && fs.existsSync(path.join(paths.eligibleDir, `${slug}.json`))) {
      try {
        cik = JSON.parse(fs.readFileSync(path.join(paths.eligibleDir, `${slug}.json`), "utf8"))
          .company?.cik;
      } catch {
        /* ignore */
      }
    }
    // Prefer registry CIK; also accept reverse map from checkpoint CIKs
    if (!cik) {
      for (const [mappedCik, mappedSlug] of Object.entries(cikToSlug)) {
        if (mappedSlug === slug) {
          cik = mappedCik;
          break;
        }
      }
    }
    const downloadedProcessed = cik ? processedByCik[cik] || 0 : 0;
    const failed = Object.values(checkpoint.processedDocuments || {}).filter(
      (d) => d.status === "error" && String(d).includes?.(cik),
    ).length;
    // Better failed count: scan keys for cik
    let failedForCik = 0;
    let skippedForCik = 0;
    for (const [k, v] of Object.entries(checkpoint.processedDocuments || {})) {
      if (!k.startsWith(String(cik || "___"))) continue;
      if (v.status === "error") failedForCik += 1;
      if (v.status === "skipped") skippedForCik += 1;
    }
    const pending = Math.max(0, eligible - downloadedProcessed);
    const exhibitsEnumerated = exhibitLedger.byParent?.[slug]?.enumerated || 0;
    const exhibitsProcessed = exhibitLedger.byParent?.[slug]?.processed || 0;

    return {
      slug,
      cik,
      eligibleFilingAccessions: eligible,
      filingDocumentsEnumerated: eligible, // submissions list = accessions with primaryDocument
      exhibitsEnumerated,
      exhibitsProcessed,
      documentsDownloadedOrCached: downloadedProcessed,
      documentsProcessed: downloadedProcessed - failedForCik,
      documentsSkippedWithReason: skippedForCik,
      documentsPending: pending,
      documentsFailed: failedForCik,
      documentsRequiringFurtherResearch: Math.max(0, exhibitsEnumerated - exhibitsProcessed),
      yearSpan: coverage.yearSpan || null,
      formTypeCounts: coverage.formTypeCounts || null,
      cohort: coreCohort.includes(slug)
        ? "CORE_SIX"
        : extendedCohort.includes(slug)
          ? "EXTENDED"
          : "OTHER",
    };
  }

  const allSlugs = [
    ...new Set([
      ...coreCohort,
      ...extendedCohort,
      ...Object.keys(eligibleIndex.companies || {}),
    ]),
  ];
  const companies = allSlugs.map(companyRow);

  const sum = (cohort, field) =>
    companies.filter((c) => c.cohort === cohort).reduce((n, c) => n + (c[field] || 0), 0);

  return {
    version: COVERAGE_LEDGER_V2,
    generatedAt: new Date().toISOString(),
    paths,
    companies,
    cohorts: {
      CORE_SIX: {
        note: "Six-company eligible universe (coverage eval baseline)",
        eligibleFilings: sum("CORE_SIX", "eligibleFilingAccessions"),
        processed: sum("CORE_SIX", "documentsProcessed"),
        pending: sum("CORE_SIX", "documentsPending"),
        exhibitsEnumerated: sum("CORE_SIX", "exhibitsEnumerated"),
        exhibitsProcessed: sum("CORE_SIX", "exhibitsProcessed"),
      },
      EXTENDED: {
        note: "Additional cohorts (e.g. Apple, RLJ, Park) — do not merge into CORE_SIX denominator",
        eligibleFilings: sum("EXTENDED", "eligibleFilingAccessions"),
        processed: sum("EXTENDED", "documentsProcessed"),
        pending: sum("EXTENDED", "documentsPending"),
      },
    },
    checkpointDocumentCount: processedKeys.length,
    checkpointByCik: processedByCik,
    reconciliation: {
      coreSixEligibleUniverse: sum("CORE_SIX", "eligibleFilingAccessions"),
      coreSixCheckpointProcessed: sum("CORE_SIX", "documentsProcessed"),
      extendedCheckpointProcessed: sum("EXTENDED", "documentsProcessed"),
      totalCheckpointDocuments: processedKeys.length,
      note:
        "Do not merge EXTENDED (Apple/RLJ/…) into the CORE_SIX 3,863 eligible denominator. Checkpoint totals may exceed CORE_SIX processed when extended CIKs were researched.",
    },
    guarantees: [
      "Eligible filing counts and checkpoint processing are not double-counted across cohorts",
      "Enumerated exhibits are not claimed as searched unless exhibitsProcessed increases",
      "Parent accession ≠ exhibit document",
      "CORE_SIX eligible (≈3863) is distinct from EXTENDED cohort checkpoint processing",
    ],
  };
}

export function saveExhibitLedgerEntry(ledgerPath, slug, patch = {}) {
  fs.mkdirSync(path.dirname(ledgerPath), { recursive: true });
  let ledger = {
    version: COVERAGE_LEDGER_V2,
    exhibitsEnumerated: 0,
    exhibitsProcessed: 0,
    byParent: {},
  };
  if (fs.existsSync(ledgerPath)) {
    try {
      ledger = JSON.parse(fs.readFileSync(ledgerPath, "utf8"));
    } catch {
      /* keep default */
    }
  }
  const prev = ledger.byParent[slug] || { enumerated: 0, processed: 0, skipped: 0 };
  ledger.byParent[slug] = {
    enumerated: prev.enumerated + (patch.enumeratedDelta || 0),
    processed: prev.processed + (patch.processedDelta || 0),
    skipped: prev.skipped + (patch.skippedDelta || 0),
    lastAt: new Date().toISOString(),
  };
  ledger.exhibitsEnumerated = Object.values(ledger.byParent).reduce(
    (n, x) => n + (x.enumerated || 0),
    0,
  );
  ledger.exhibitsProcessed = Object.values(ledger.byParent).reduce(
    (n, x) => n + (x.processed || 0),
    0,
  );
  ledger.updatedAt = new Date().toISOString();
  fs.writeFileSync(ledgerPath, JSON.stringify(ledger, null, 2), "utf8");
  return ledger;
}

export function coverageLedgerToMarkdown(ledger) {
  const lines = [
    `# SEC EDGAR Coverage Ledger V2`,
    ``,
    `**Generated:** ${ledger.generatedAt}`,
    ``,
    `## Cohorts`,
    ``,
    `| Cohort | Eligible | Processed | Pending | Exhibits enumerated |`,
    `|---|---|---|---|---|`,
    `| CORE_SIX | ${ledger.cohorts.CORE_SIX.eligibleFilings} | ${ledger.cohorts.CORE_SIX.processed} | ${ledger.cohorts.CORE_SIX.pending} | ${ledger.cohorts.CORE_SIX.exhibitsEnumerated} |`,
    `| EXTENDED | ${ledger.cohorts.EXTENDED.eligibleFilings} | ${ledger.cohorts.EXTENDED.processed} | ${ledger.cohorts.EXTENDED.pending} | — |`,
    ``,
    `## Per registrant`,
    ``,
    `| Slug | Cohort | Eligible | Processed | Pending | Failed | Exhibits enum |`,
    `|---|---|---|---|---|---|---|`,
    ...ledger.companies.map(
      (c) =>
        `| ${c.slug} | ${c.cohort} | ${c.eligibleFilingAccessions} | ${c.documentsProcessed} | ${c.documentsPending} | ${c.documentsFailed} | ${c.exhibitsEnumerated} |`,
    ),
    ``,
    `## Guarantees`,
    ...ledger.guarantees.map((g) => `- ${g}`),
    ``,
  ];
  return lines.join("\n");
}
