#!/usr/bin/env node
/**
 * SEC EDGAR V2.2 — bounded live evidence validation.
 * DiamondRock schedules + real filing-package EX-10 enumeration.
 *
 *   node scripts/sec-edgar-v22-live-evidence-eval.mjs
 *   node scripts/sec-edgar-v22-live-evidence-eval.mjs --max-filings 5 --max-exhibits 8
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";

import { createSecEdgarClient } from "../lib/hotel-intelligence/sec-edgar/edgar-client-v1.js";
import { normalizeCik } from "../lib/hotel-intelligence/sec-edgar/cik-registry-v2.js";
import {
  buildEligibleFilingInventory,
  selectBatchFromEligibleInventory,
} from "../lib/hotel-intelligence/sec-edgar/eligible-filing-inventory-v2.js";
import {
  discoverExhibitsForFiling,
  dedupeExhibits,
} from "../lib/hotel-intelligence/sec-edgar/filing-package-exhibits-v2.js";
import { extractFranchiseExpirationSchedule } from "../lib/hotel-intelligence/sec-edgar/franchise-schedule-extractor-v2.js";
import { extractRelationshipsFromDocument } from "../lib/hotel-intelligence/sec-edgar/relationship-extractor-v2.js";
import { classifyHotelIdentity, IDENTITY_CLASS, evaluateVerifiedPrecision } from "../lib/hotel-intelligence/sec-edgar/hotel-identity-quality-v2.js";
import { buildEntityAttribution } from "../lib/hotel-intelligence/sec-edgar/entity-attribution-v2.js";
import { buildCoverageLedger, coverageLedgerToMarkdown, saveExhibitLedgerEntry, defaultLedgerPaths } from "../lib/hotel-intelligence/sec-edgar/coverage-ledger-v2.js";
import { loadDiscoveryInventory, defaultInventoryRoot } from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-v1.js";
import { isDocumentProcessed, loadCheckpoint, defaultCheckpointPath } from "../lib/hotel-intelligence/sec-edgar/research-checkpoint-v2.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) {
    dotenv.config({ path: p, override: false });
    break;
  }
}

function argNum(flag, fallback) {
  const i = process.argv.indexOf(flag);
  if (i >= 0 && process.argv[i + 1]) return Number(process.argv[i + 1]) || fallback;
  return fallback;
}

const MAX_FILINGS = argNum("--max-filings", 5);
const MAX_EXHIBITS = argNum("--max-exhibits", 8);
const OUT = path.join(ROOT, "reports", "sec-edgar-v22-live-evidence");
fs.mkdirSync(OUT, { recursive: true });

const DRH = {
  slug: "diamondrock-hospitality",
  legalName: "DiamondRock Hospitality Company",
  ticker: "DRH",
  cik: "0001298946",
};

async function main() {
  const client = createSecEdgarClient();
  const liveCik = await client.resolveCikByTicker("DRH");
  const company = {
    ...DRH,
    cik: liveCik?.cik || normalizeCik(DRH.cik),
  };

  // Prefer persisted eligible inventory if present
  const eligiblePath = path.join(ROOT, "data", "sec-edgar", "eligible-filings", `${company.slug}.json`);
  let eligible;
  if (fs.existsSync(eligiblePath)) {
    eligible = JSON.parse(fs.readFileSync(eligiblePath, "utf8"));
  } else {
    eligible = await buildEligibleFilingInventory(client, company, { sinceYear: 2004 });
  }

  const checkpoint = loadCheckpoint(defaultCheckpointPath(ROOT));
  const batch = selectBatchFromEligibleInventory(eligible, {
    batchSize: MAX_FILINGS,
    isProcessed: (cik, acc, doc) => isDocumentProcessed(checkpoint, cik, acc, doc),
  });

  // Prefer known high-yield DiamondRock 10-Ks (HTML property tables + EX-21)
  // plus recent 8-Ks that carry EX-10 packages — not arbitrary unprocessed leftovers.
  const HIGH_YIELD_ACCESSIONS = new Set([
    "0001298946-24-000011", // 2023 YE 10-K — portfolio table
    "0001298946-25-000015", // 2024 YE 10-K
    "0001298946-23-000015", // 2022 YE 10-K
    "0001298946-24-000084", // 8-K with EX-10.1
    "0001298946-24-000086", // 8-K with EX-10.1
  ]);
  const allEligible = eligible.filings || [];
  const preferred = allEligible.filter((f) => HIGH_YIELD_ACCESSIONS.has(f.accessionNumber));
  const tenKFallback = allEligible.filter((f) => /^10-K/i.test(f.form));
  const filings = [
    ...preferred,
    ...tenKFallback.filter((f) => !HIGH_YIELD_ACCESSIONS.has(f.accessionNumber)),
    ...(batch.selected || []).filter(
      (f) => !HIGH_YIELD_ACCESSIONS.has(f.accessionNumber) && !/^10-K/i.test(f.form),
    ),
  ].slice(0, MAX_FILINGS);
  if (!filings.length) {
    filings.push(...allEligible.slice(0, MAX_FILINGS));
  }

  const scheduleHotels = [];
  const scheduleFindings = [];
  const relationshipFindings = [];
  const exhibitResults = [];
  const falsePositives = [];
  const errors = [];

  for (const filing of filings) {
    console.error(`[v22] filing ${filing.form} ${filing.filingDate} ${filing.accessionNumber}`);
    const meta = {
      cik: company.cik,
      accessionNumber: filing.accessionNumber,
      form: filing.form,
      filingDate: filing.filingDate,
      companyName: company.legalName,
      documentUrl: client.filingDocumentUrl(
        company.cik,
        filing.accessionNumber,
        filing.primaryDocument,
      ),
    };

    const doc = await client.getFilingDocument(
      company.cik,
      filing.accessionNumber,
      filing.primaryDocument,
    );
    if (!doc.ok || !doc.text) {
      errors.push({ stage: "primary", accession: filing.accessionNumber, error: doc.error || doc.status });
      continue;
    }

    const schedule = extractFranchiseExpirationSchedule(doc, meta);
    for (const h of schedule.hotels || []) {
      const cls = classifyHotelIdentity({
        ...h,
        discoveryMethod: h.discoveryMethod || "HTML_PROPERTY_TABLE",
      });
      const row = { ...h, ...cls, entityAttribution: buildEntityAttribution(h) };
      if (cls.identityClass === IDENTITY_CLASS.VERIFIED_PROPERTY) scheduleHotels.push(row);
      else if (
        cls.identityClass === IDENTITY_CLASS.EXTRACTION_ARTIFACT ||
        cls.identityClass === IDENTITY_CLASS.GENERIC_BRAND_OR_COMPANY
      ) {
        falsePositives.push(row);
      } else {
        scheduleHotels.push(row); // keep probable for review — never silent-promote
      }
    }
    scheduleFindings.push(...(schedule.findings || []));
    relationshipFindings.push(...(schedule.managerRelationships || []));

    const rel = extractRelationshipsFromDocument(doc, meta);
    relationshipFindings.push(...rel.findings);

    // Exhibit package discovery (parent ≠ exhibit)
    const exh = await discoverExhibitsForFiling(client, {
      ...meta,
      primaryDocument: filing.primaryDocument,
    });
    if (!exh.ok) {
      errors.push({ stage: "index", accession: filing.accessionNumber, error: exh.error });
      continue;
    }
    const deduped = dedupeExhibits(exh.exhibits);
    saveExhibitLedgerEntry(defaultLedgerPaths(ROOT).exhibitLedgerPath, company.slug, {
      enumeratedDelta: deduped.uniqueCount,
      skippedDelta: deduped.skippedCount,
    });

    let processedExhibits = 0;
    for (const ex of deduped.exhibits.slice(0, MAX_EXHIBITS)) {
      // Process EX-10* and EX-21* (and issuer-named exhibit files); skip graphics
      if (/\.(jpg|jpeg|png|gif|pdf)$/i.test(ex.fileName)) continue;
      if (
        !/EX-10|EX-21/i.test(ex.exhibitType) &&
        !/exhibit[-_]?10|exhibit[-_]?21|ex[-_]?10|ex[-_]?21/i.test(ex.fileName)
      ) {
        continue;
      }
      const exDoc = await client.getFilingDocument(company.cik, filing.accessionNumber, ex.fileName);
      if (!exDoc.ok || !exDoc.text) {
        errors.push({ stage: "exhibit", file: ex.fileName, error: exDoc.error || exDoc.status });
        continue;
      }
      const exMeta = {
        ...meta,
        documentUrl: ex.sourceUrl,
        exhibitFileName: ex.fileName,
      };
      const exRel = extractRelationshipsFromDocument(exDoc, exMeta);
      const exSched = extractFranchiseExpirationSchedule(exDoc, exMeta);
      relationshipFindings.push(...exRel.findings);
      scheduleFindings.push(...(exSched.findings || []));
      processedExhibits += 1;
      exhibitResults.push({
        fileName: ex.fileName,
        exhibitType: ex.exhibitType,
        sourceUrl: ex.sourceUrl,
        parentAccession: ex.parentAccession,
        relationshipFindings: exRel.findingCount,
        scheduleHotels: exSched.hotelCount,
        characterCount: exDoc.text.length,
      });
    }
    saveExhibitLedgerEntry(defaultLedgerPaths(ROOT).exhibitLedgerPath, company.slug, {
      processedDelta: processedExhibits,
    });
  }

  // Precision sample on schedule-backed verified (dedupe by normalized identity)
  const verifiedRaw = scheduleHotels.filter((h) => h.identityClass === IDENTITY_CLASS.VERIFIED_PROPERTY);
  const byNorm = new Map();
  for (const h of verifiedRaw) {
    const key = String(h.normalizedName || h.hotelName || "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, " ")
      .trim();
    if (!key) continue;
    if (!byNorm.has(key)) byNorm.set(key, h);
  }
  const verified = [...byNorm.values()];
  const precisionSample = verified.map((h) => ({
    hotelName: h.normalizedName || h.hotelName,
    city: h.city,
    stateRegion: h.stateRegion,
    correct: Boolean(
      h.city &&
        h.stateRegion &&
        (h.normalizedName || h.hotelName) &&
        !/\band\b(?!\s+spa\b)/i.test(h.hotelName || ""),
    ),
  }));
  const precision = evaluateVerifiedPrecision(precisionSample);

  // Recall vs known DiamondRock-style names if any found
  const expectedHints = [
    "Courtyard",
    "Westin",
    "Hilton",
    "Residence Inn",
    "Embassy",
    "Marriott",
    "Hampton",
  ];
  const recalled = expectedHints.filter((hint) =>
    verified.some((h) => String(h.hotelName).includes(hint)),
  );
  const recallApprox = recalled.length / expectedHints.length;

  // HPC reconciliation dry-run on durable inventory
  const inventory = loadDiscoveryInventory(defaultInventoryRoot(ROOT));
  const hpcSeed = [
    { id: "recLuxvwwxID7U2B8", name: "Bethesda Marriott", city: "Bethesda", brand: "Marriott Hotels" },
  ];
  // Use v1 reconciliation module path — check import
  let recon;
  try {
    const mod = await import("../lib/hotel-intelligence/sec-edgar/hpc-reconciliation-v1.js");
    recon = mod.reconcileDiscoveryInventoryToHpc(inventory, hpcSeed, { apply: false });
  } catch (err) {
    recon = { error: String(err.message || err) };
  }

  const ledger = buildCoverageLedger({ cwd: ROOT });
  fs.writeFileSync(path.join(OUT, "COVERAGE_LEDGER.json"), JSON.stringify(ledger, null, 2));
  fs.writeFileSync(path.join(OUT, "COVERAGE_LEDGER.md"), coverageLedgerToMarkdown(ledger));

  const owners = relationshipFindings.filter((f) => f.role === "LEGAL_OWNER");
  const managers = relationshipFindings.filter((f) => f.role === "MANAGEMENT_COMPANY");
  const registrantOnly = relationshipFindings.filter((f) => f.role === "FILING_REGISTRANT");

  const report = {
    version: "sec-edgar-v22-live-evidence-eval",
    generatedAt: new Date().toISOString(),
    company,
    filingsReviewed: filings.map((f) => ({
      form: f.form,
      filingDate: f.filingDate,
      accessionNumber: f.accessionNumber,
    })),
    demonstrations: {
      correctHotelIdentification: verified.slice(0, 15).map((h) => ({
        hotelName: h.hotelName,
        city: h.city,
        stateRegion: h.stateRegion,
        identityClass: h.identityClass,
      })),
      propertyLevelContractDates: scheduleFindings
        .filter((f) => f.primaryDate)
        .slice(0, 20)
        .map((f) => ({
          hotel: f.attributedHotelName,
          date: f.primaryDate,
          historicalApplicability: f.historicalApplicability,
          agreementType: f.agreementType,
        })),
      ownerOperatorAttribution: {
        explicitOwners: owners.length,
        explicitManagers: managers.length,
        registrantRecords: registrantOnly.length,
        sample: [...owners, ...managers].slice(0, 10),
        guarantee: "Registrant is not treated as legal owner without explicit ownership language",
      },
      historicalVsCurrent: {
        historicallyVerifiedDates: scheduleFindings.filter(
          (f) => f.historicalApplicability === "HISTORICALLY_VERIFIED",
        ).length,
        currentlyVerifiedDates: scheduleFindings.filter(
          (f) => f.historicalApplicability === "CURRENTLY_VERIFIED",
        ).length,
      },
      duplicateElimination: {
        scheduleHotelsRaw: scheduleHotels.length,
        verifiedUnique: verified.length,
        falsePositivesRejected: falsePositives.length,
      },
      sourceEvidence: exhibitResults.slice(0, 10),
      hpcMatchingDryRun: recon.counts || recon,
      unmatchedRetention: {
        durableInventoryHotels: (inventory.hotels || []).length,
        note: "Unmatched verified/probable hotels retained; no auto HPC create",
      },
    },
    metrics: {
      verifiedProperties: verified.length,
      probableOrOtherScheduleRows: scheduleHotels.length - verified.length,
      falsePositives: falsePositives.length,
      scheduleFindings: scheduleFindings.length,
      relationshipFindings: relationshipFindings.length,
      exhibitsProcessed: exhibitResults.length,
      precision,
      recallApproxBrandHints: recallApprox,
      recallNote: "Approximate brand-hint recall against expected DiamondRock brand families — not gold-label property list",
      errors: errors.length,
      paidSpendUsd: 0,
    },
    errors,
  };

  fs.writeFileSync(path.join(OUT, "LIVE_EVIDENCE_REPORT.json"), JSON.stringify(report, null, 2));
  fs.writeFileSync(
    path.join(OUT, "VERIFIED_PROPERTIES_SAMPLE.json"),
    JSON.stringify({ count: verified.length, rows: verified }, null, 2),
  );

  console.log(
    JSON.stringify(
      {
        ok: true,
        outDir: OUT,
        filings: filings.length,
        verifiedProperties: verified.length,
        scheduleFindings: scheduleFindings.length,
        exhibitsProcessed: exhibitResults.length,
        explicitOwners: owners.length,
        explicitManagers: managers.length,
        precision,
        recallApprox,
        coverageCoreEligible: ledger.cohorts.CORE_SIX.eligibleFilings,
        coverageCoreProcessed: ledger.cohorts.CORE_SIX.processed,
        paidSpendUsd: 0,
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
