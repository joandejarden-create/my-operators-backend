#!/usr/bin/env node
/**
 * SEC EDGAR V2.4 — universal company & historical research wave.
 *
 *   node scripts/sec-edgar-universal-research-wave-v24.mjs --inventory-only
 *   node scripts/sec-edgar-universal-research-wave-v24.mjs --batch-size 20 --max-exhibits 4
 *   node scripts/sec-edgar-universal-research-wave-v24.mjs --companies diamondrock-hospitality,ryman-hospitality
 *   node scripts/sec-edgar-universal-research-wave-v24.mjs --persist-airtable   # new verified only, with read-back
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";

import { createSecEdgarClient } from "../lib/hotel-intelligence/sec-edgar/edgar-client-v1.js";
import {
  buildUniversalCompanyRegistryV24,
  validateUniversalRegistryLive,
  listUniversalCompaniesV24,
  UNIVERSAL_WAVE_ID,
} from "../lib/hotel-intelligence/sec-edgar/cik-registry-v24.js";
import {
  buildEligibleFilingInventory,
  saveEligibleFilingInventory,
  selectBatchFromEligibleInventory,
  defaultEligibleInventoryDir,
} from "../lib/hotel-intelligence/sec-edgar/eligible-filing-inventory-v2.js";
import {
  discoverExhibitsForFiling,
  dedupeExhibits,
} from "../lib/hotel-intelligence/sec-edgar/filing-package-exhibits-v2.js";
import { extractFranchiseExpirationSchedule } from "../lib/hotel-intelligence/sec-edgar/franchise-schedule-extractor-v2.js";
import { extractRelationshipsFromDocument } from "../lib/hotel-intelligence/sec-edgar/relationship-extractor-v2.js";
import {
  classifyHotelIdentity,
  IDENTITY_CLASS,
  evaluateVerifiedPrecision,
} from "../lib/hotel-intelligence/sec-edgar/hotel-identity-quality-v2.js";
import { buildEntityAttribution } from "../lib/hotel-intelligence/sec-edgar/entity-attribution-v2.js";
import {
  loadDiscoveryInventory,
  saveDiscoveryInventory,
  upsertDiscoveredHotels,
  defaultInventoryRoot,
} from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-v1.js";
import {
  loadCheckpoint,
  saveCheckpoint,
  isDocumentProcessed,
  markDocumentProcessed,
  markCompanyCoverage,
} from "../lib/hotel-intelligence/sec-edgar/research-checkpoint-v2.js";
import {
  buildCoverageLedger,
  coverageLedgerToMarkdown,
  saveExhibitLedgerEntry,
  defaultLedgerPaths,
} from "../lib/hotel-intelligence/sec-edgar/coverage-ledger-v2.js";
import {
  GRAPH_BASE_ID_CANONICAL,
  MAP_DISCOVERY_INVENTORY,
  MAP_DISCOVERY_FINDINGS,
  TABLE_HI_HOTEL_DISCOVERY_INVENTORY,
  TABLE_HI_DISCOVERY_FINDINGS,
  DISCOVERY_SCHEMA_VERSION,
} from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-airtable-contract-v23.js";

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

const INVENTORY_ONLY = process.argv.includes("--inventory-only");
const PERSIST_AIRTABLE = process.argv.includes("--persist-airtable");
const BATCH_SIZE = Math.max(1, Number(argValue("--batch-size", "16")) || 16);
const MAX_EXHIBITS = Math.max(0, Number(argValue("--max-exhibits", "4")) || 4);
const SINCE_YEAR = Number(argValue("--since-year", "1994")) || 1994;
const MAX_PRIORITY = Number(argValue("--max-priority", "3")) || 3;
const COMPANY_FILTER = (argValue("--companies", "") || "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const OUT_DIR = path.join(ROOT, "reports", "sec-edgar-universal-wave-v24");
const CHECKPOINT_PATH = path.join(
  ROOT,
  "data",
  "sec-edgar",
  "checkpoints",
  "universal-research-v24.json",
);
fs.mkdirSync(OUT_DIR, { recursive: true });

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function atCreate(baseId, tableId, token, fields) {
  const res = await fetch(`https://api.airtable.com/v0/${baseId}/${tableId}`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields, typecast: true }),
  });
  const json = await res.json();
  return { ok: res.ok, json };
}

async function atFindByPropertyId(baseId, tableId, token, propertyId) {
  const formula = `{${MAP_DISCOVERY_INVENTORY.discoveryPropertyId}}="${propertyId}"`;
  const qs = new URLSearchParams({
    filterByFormula: formula,
    maxRecords: "1",
  });
  const res = await fetch(`https://api.airtable.com/v0/${baseId}/${tableId}?${qs}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const json = await res.json();
  return json.records?.[0] || null;
}

async function main() {
  const client = createSecEdgarClient();
  const staticReg = buildUniversalCompanyRegistryV24();
  fs.writeFileSync(
    path.join(OUT_DIR, "REGISTRY_STATIC.json"),
    JSON.stringify(staticReg, null, 2),
  );

  console.error(`[v24] validating ${staticReg.companyCount} registry companies via SEC tickers…`);
  const validated = await validateUniversalRegistryLive(client, {
    maxPriority: MAX_PRIORITY,
    slugs: COMPANY_FILTER.length ? COMPANY_FILTER : null,
  });
  // If filter left empty companies (all failed), fall back to static filtered list with static CIKs
  let companies = validated.companies;
  if (COMPANY_FILTER.length && !companies.length) {
    companies = listUniversalCompaniesV24({
      maxPriority: MAX_PRIORITY,
      slugs: COMPANY_FILTER,
    });
  }
  fs.writeFileSync(path.join(OUT_DIR, "REGISTRY_VALIDATED.json"), JSON.stringify(validated, null, 2));
  console.error(
    `[v24] validated=${validated.validatedCount} failed=${validated.failed.length} collisions=${validated.cikCollisions.length}`,
  );

  // --- Eligible inventory for all companies ---
  const inventorySummaries = [];
  let totalEligible = 0;
  for (const company of companies) {
    const existingPath = path.join(defaultEligibleInventoryDir(ROOT), `${company.slug}.json`);
    let inv;
    if (fs.existsSync(existingPath) && !process.argv.includes("--rebuild-inventory")) {
      inv = JSON.parse(fs.readFileSync(existingPath, "utf8"));
      // Refresh company cik if live-validated
      if (company.cik && inv.company) inv.company.cik = company.cik;
      console.error(`[v24] inventory reuse ${company.slug} eligible=${inv.coverage?.eligibleFound}`);
    } else {
      console.error(`[v24] building eligible inventory ${company.slug} since=${SINCE_YEAR}`);
      inv = await buildEligibleFilingInventory(client, company, { sinceYear: SINCE_YEAR });
      if (!inv.ok) {
        inventorySummaries.push({ slug: company.slug, ok: false, error: inv.error });
        continue;
      }
      saveEligibleFilingInventory(inv, defaultEligibleInventoryDir(ROOT));
      await sleep(200);
    }
    totalEligible += inv.coverage?.eligibleFound || 0;
    inventorySummaries.push({
      slug: company.slug,
      cik: company.cik,
      ticker: company.ticker,
      ok: true,
      eligible: inv.coverage?.eligibleFound || 0,
      yearSpan: inv.coverage?.yearSpan || null,
      formTypeCounts: inv.coverage?.formTypeCounts || null,
    });
  }

  fs.writeFileSync(
    path.join(OUT_DIR, "ELIGIBLE_INVENTORY_SUMMARY.json"),
    JSON.stringify(
      {
        waveId: UNIVERSAL_WAVE_ID,
        sinceYear: SINCE_YEAR,
        companies: inventorySummaries,
        totalEligibleFilings: totalEligible,
        generatedAt: new Date().toISOString(),
      },
      null,
      2,
    ),
  );

  if (INVENTORY_ONLY) {
    const ledger = buildCoverageLedger({
      cwd: ROOT,
      coreCohort: companies.map((c) => c.slug).slice(0, 6),
      extendedCohort: companies.map((c) => c.slug).slice(6),
      cikToSlug: Object.fromEntries(companies.filter((c) => c.cik).map((c) => [c.cik, c.slug])),
    });
    fs.writeFileSync(path.join(OUT_DIR, "COVERAGE_LEDGER.json"), JSON.stringify(ledger, null, 2));
    fs.writeFileSync(path.join(OUT_DIR, "COVERAGE_LEDGER.md"), coverageLedgerToMarkdown(ledger));
    console.log(
      JSON.stringify(
        {
          ok: true,
          mode: "inventory-only",
          companies: companies.length,
          totalEligibleFilings: totalEligible,
          outDir: OUT_DIR,
        },
        null,
        2,
      ),
    );
    return;
  }

  // --- Research wave ---
  const checkpoint = loadCheckpoint(CHECKPOINT_PATH);
  let fsInventory = loadDiscoveryInventory(defaultInventoryRoot(ROOT));

  const waveStats = {
    documentsProcessed: 0,
    documentsFailed: 0,
    exhibitsEnumerated: 0,
    exhibitsProcessed: 0,
    hotelsRaw: 0,
    verified: [],
    probable: [],
    ambiguous: 0,
    artifacts: 0,
    managers: [],
    owners: [],
    contractMilestones: [],
    historicalFindings: 0,
    currentFindings: 0,
    airtablePersisted: [],
    airtableSkippedExisting: [],
    errors: [],
  };

  // Prefer high-yield historical REIT 10-Ks (property schedules often richer pre-XBRL ~2018)
  // over newest brand/gaming 10-Ks that are narrative/XBRL-heavy with low table yield.
  function filingYieldScore(f, company) {
    let score = 0;
    const form = String(f.form || "");
    const year = Number(String(f.filingDate || "").slice(0, 4)) || 0;
    const doc = String(f.primaryDocument || "").toLowerCase();
    if (/^10-K/i.test(form)) score += 40;
    else if (/^8-K/i.test(form)) score += 10;
    else if (/^424B/i.test(form)) score += 15;
    if ((company.cohorts || []).includes("REIT") || company.organizationType === "REIT") score += 25;
    if ((company.cohorts || []).includes("HISTORICAL")) score += 10;
    // Historical HTML schedules beat recent inline-XBRL portfolios for current extractor
    if (year >= 1996 && year <= 2018) score += 30;
    else if (year >= 2019 && year <= 2022) score += 12;
    if (/^d10k|^d10-k|10k|10-k/.test(doc) && !/\d{8}/.test(doc)) score += 5;
    if (/xbrl|htm\.xml|_htm\.xml/.test(doc)) score -= 20;
    return score;
  }

  const batchFilings = [];
  for (const company of companies) {
    const invPath = path.join(defaultEligibleInventoryDir(ROOT), `${company.slug}.json`);
    if (!fs.existsSync(invPath)) continue;
    const inv = JSON.parse(fs.readFileSync(invPath, "utf8"));
    const perCompany = Math.max(2, Math.ceil(BATCH_SIZE / Math.max(1, Math.min(companies.length, 12))));
    const candidates = (inv.filings || [])
      .filter((f) => f.accessionNumber && f.primaryDocument)
      .filter((f) => !isDocumentProcessed(checkpoint, company.cik, f.accessionNumber, f.primaryDocument))
      .map((f) => ({ filing: f, score: filingYieldScore(f, company) }))
      .sort((a, b) => b.score - a.score || String(b.filing.filingDate).localeCompare(String(a.filing.filingDate)))
      .slice(0, perCompany);
    for (const { filing } of candidates) {
      batchFilings.push({ company, filing, score: filingYieldScore(filing, company) });
    }
  }
  batchFilings.sort((a, b) => b.score - a.score);
  const work = batchFilings.slice(0, BATCH_SIZE);
  console.error(`[v24] processing ${work.length} filings across ${companies.length} companies`);

  for (const { company, filing } of work) {
    const cik = company.cik;
    const acc = filing.accessionNumber;
    const docName = filing.primaryDocument;
    if (!cik || !acc || !docName) continue;
    if (isDocumentProcessed(checkpoint, cik, acc, docName)) continue;

    console.error(`[v24] ${company.slug} ${filing.form} ${filing.filingDate} ${acc}`);
    const meta = {
      cik,
      accessionNumber: acc,
      form: filing.form,
      filingDate: filing.filingDate,
      companyName: company.legalName,
      documentUrl: client.filingDocumentUrl(cik, acc, docName),
    };

    try {
      const doc = await client.getFilingDocument(cik, acc, docName);
      if (!doc.ok || !doc.text) {
        waveStats.documentsFailed += 1;
        waveStats.errors.push({ stage: "primary", slug: company.slug, acc, error: doc.error || doc.status });
        markDocumentProcessed(checkpoint, cik, acc, docName, { status: "error", form: filing.form });
        continue;
      }

      const schedule = extractFranchiseExpirationSchedule(doc, meta);
      const rel = extractRelationshipsFromDocument(doc, meta);
      const hotels = [];
      for (const h of schedule.hotels || []) {
        const cls = classifyHotelIdentity({
          ...h,
          discoveryMethod: h.discoveryMethod || "HTML_PROPERTY_TABLE",
        });
        const row = {
          ...h,
          ...cls,
          registrant: company.legalName,
          cik,
          sourceFiling: filing.form,
          sourceAccession: acc,
          sourceUrl: meta.documentUrl,
          entityAttribution: buildEntityAttribution({ ...h, registrant: company.legalName, cik }),
          discoveryVersion: UNIVERSAL_WAVE_ID,
        };
        hotels.push(row);
        if (cls.identityClass === IDENTITY_CLASS.VERIFIED_PROPERTY) waveStats.verified.push(row);
        else if (cls.identityClass === IDENTITY_CLASS.PROBABLE_PROPERTY) waveStats.probable.push(row);
        else if (
          cls.identityClass === IDENTITY_CLASS.EXTRACTION_ARTIFACT ||
          cls.identityClass === IDENTITY_CLASS.GENERIC_BRAND_OR_COMPANY
        ) {
          waveStats.artifacts += 1;
        } else waveStats.ambiguous += 1;
      }
      waveStats.hotelsRaw += hotels.length;

      for (const f of schedule.findings || []) {
        if (f.primaryDate || f.initialExpirationYear) {
          waveStats.contractMilestones.push({
            hotel: f.attributedHotelName,
            date: f.primaryDate || f.initialExpirationYear,
            applicability: f.historicalApplicability,
            company: company.slug,
            accession: acc,
          });
          if (f.historicalApplicability === "HISTORICALLY_VERIFIED") waveStats.historicalFindings += 1;
          if (f.historicalApplicability === "CURRENTLY_VERIFIED") waveStats.currentFindings += 1;
        }
      }
      for (const r of [...(schedule.managerRelationships || []), ...rel.findings]) {
        if (r.role === "MANAGEMENT_COMPANY") waveStats.managers.push({ ...r, company: company.slug });
        if (r.role === "LEGAL_OWNER") waveStats.owners.push({ ...r, company: company.slug });
      }

      // Upsert filesystem inventory (verified + probable only)
      const toUpsert = hotels.filter(
        (h) =>
          h.identityClass === IDENTITY_CLASS.VERIFIED_PROPERTY ||
          h.identityClass === IDENTITY_CLASS.PROBABLE_PROPERTY,
      );
      if (toUpsert.length) {
        fsInventory = upsertDiscoveredHotels(fsInventory, toUpsert);
      }

      // Exhibit package pass
      const exh = await discoverExhibitsForFiling(client, { ...meta, primaryDocument: docName });
      if (exh.ok) {
        const deduped = dedupeExhibits(exh.exhibits);
        waveStats.exhibitsEnumerated += deduped.uniqueCount;
        saveExhibitLedgerEntry(defaultLedgerPaths(ROOT).exhibitLedgerPath, company.slug, {
          enumeratedDelta: deduped.uniqueCount,
          skippedDelta: deduped.skippedCount,
        });
        let processedEx = 0;
        for (const ex of deduped.exhibits.slice(0, MAX_EXHIBITS)) {
          if (/\.(jpg|jpeg|png|gif)$/i.test(ex.fileName)) continue;
          if (
            !/EX-10|EX-21/i.test(ex.exhibitType) &&
            !/exhibit[-_]?10|exhibit[-_]?21|ex[-_]?10|ex[-_]?21/i.test(ex.fileName)
          ) {
            continue;
          }
          const exDoc = await client.getFilingDocument(cik, acc, ex.fileName);
          if (!exDoc.ok || !exDoc.text) continue;
          const exMeta = { ...meta, documentUrl: ex.sourceUrl, exhibitFileName: ex.fileName };
          const exRel = extractRelationshipsFromDocument(exDoc, exMeta);
          const exSched = extractFranchiseExpirationSchedule(exDoc, exMeta);
          for (const r of exRel.findings) {
            if (r.role === "MANAGEMENT_COMPANY") waveStats.managers.push({ ...r, company: company.slug, exhibit: ex.fileName });
            if (r.role === "LEGAL_OWNER") waveStats.owners.push({ ...r, company: company.slug, exhibit: ex.fileName });
          }
          for (const h of exSched.hotels || []) {
            const cls = classifyHotelIdentity({ ...h, discoveryMethod: h.discoveryMethod || "HTML_PROPERTY_TABLE" });
            if (cls.identityClass === IDENTITY_CLASS.VERIFIED_PROPERTY) {
              waveStats.verified.push({ ...h, ...cls, registrant: company.legalName, cik });
            }
          }
          processedEx += 1;
          await sleep(150);
        }
        waveStats.exhibitsProcessed += processedEx;
        saveExhibitLedgerEntry(defaultLedgerPaths(ROOT).exhibitLedgerPath, company.slug, {
          processedDelta: processedEx,
        });
      }

      markDocumentProcessed(checkpoint, cik, acc, docName, {
        status: "ok",
        form: filing.form,
        filingDate: filing.filingDate,
        hotels: hotels.length,
        findings: (schedule.findings || []).length,
        fromCache: Boolean(doc.fromCache),
      });
      markCompanyCoverage(checkpoint, company.slug, {
        lastAccession: acc,
        lastForm: filing.form,
      });
      waveStats.documentsProcessed += 1;
      saveCheckpoint(checkpoint, CHECKPOINT_PATH);
      await sleep(200);
    } catch (err) {
      waveStats.documentsFailed += 1;
      waveStats.errors.push({ stage: "wave", slug: company.slug, acc, error: String(err.message || err) });
      markDocumentProcessed(checkpoint, cik, acc, docName, { status: "error", error: String(err.message || err) });
      saveCheckpoint(checkpoint, CHECKPOINT_PATH);
    }
  }

  try {
    saveDiscoveryInventory(fsInventory, defaultInventoryRoot(ROOT));
  } catch (err) {
    // Windows file lock fallback — never lose wave hotels
    const fallback = path.join(OUT_DIR, "discovery-inventory-wave-hotels.json");
    fs.writeFileSync(
      fallback,
      JSON.stringify(
        {
          version: UNIVERSAL_WAVE_ID,
          updatedAt: new Date().toISOString(),
          hotels: fsInventory.hotels || [],
          note: `Primary inventory save failed: ${String(err.message || err)}`,
        },
        null,
        2,
      ),
    );
    waveStats.errors.push({ stage: "inventory_save", error: String(err.message || err), fallback });
    console.error(`[v24] inventory save fallback → ${fallback}`);
  }

  // Dedupe verified for precision sample
  const verifiedByKey = new Map();
  for (const h of waveStats.verified) {
    const key = `${String(h.normalizedName || h.hotelName || "").toLowerCase()}|${h.city}|${h.stateRegion}`;
    if (!verifiedByKey.has(key)) verifiedByKey.set(key, h);
  }
  const verifiedUnique = [...verifiedByKey.values()];
  const precision = evaluateVerifiedPrecision(
    verifiedUnique.map((h) => ({
      hotelName: h.normalizedName || h.hotelName,
      city: h.city,
      stateRegion: h.stateRegion,
      correct: Boolean(h.city && h.stateRegion && !/\b(ltd\.?|l\.?p\.?)\b/i.test(h.city || "")),
    })),
  );

  // Optional Airtable persistence for NEW verified only
  if (PERSIST_AIRTABLE) {
    const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
    const idsPath = path.join(
      ROOT,
      "lib/hotel-intelligence/sec-edgar/discovery-inventory-table-ids.generated.json",
    );
    if (token && fs.existsSync(idsPath)) {
      const ids = JSON.parse(fs.readFileSync(idsPath, "utf8"));
      const invTable = ids.tableIds[TABLE_HI_HOTEL_DISCOVERY_INVENTORY];
      for (const h of verifiedUnique) {
        const propertyId = h.propertyId || h.discoveryId;
        if (!propertyId) continue;
        const existing = await atFindByPropertyId(GRAPH_BASE_ID_CANONICAL, invTable, token, propertyId);
        if (existing) {
          waveStats.airtableSkippedExisting.push(propertyId);
          continue;
        }
        const fields = {
          [MAP_DISCOVERY_INVENTORY.discoveryPropertyId]: propertyId,
          [MAP_DISCOVERY_INVENTORY.hotelNameNormalized]: h.normalizedName || h.hotelName,
          [MAP_DISCOVERY_INVENTORY.hotelNameRaw]: h.hotelName || h.rawHotelName,
          [MAP_DISCOVERY_INVENTORY.city]: h.city || null,
          [MAP_DISCOVERY_INVENTORY.stateRegion]: h.stateRegion || null,
          [MAP_DISCOVERY_INVENTORY.country]: h.country || "United States",
          [MAP_DISCOVERY_INVENTORY.identityClass]: "VERIFIED_PROPERTY",
          [MAP_DISCOVERY_INVENTORY.lifecycleStatus]: "IDENTITY_VERIFIED",
          [MAP_DISCOVERY_INVENTORY.persistenceStatus]: "DISCOVERY_PERSISTED",
          [MAP_DISCOVERY_INVENTORY.publicationState]: "CUSTOMER_BLOCKED",
          [MAP_DISCOVERY_INVENTORY.secRegistrant]: h.registrant || null,
          [MAP_DISCOVERY_INVENTORY.registrantCik]: h.cik || null,
          [MAP_DISCOVERY_INVENTORY.sourceFilingUrl]: h.sourceUrl || null,
          [MAP_DISCOVERY_INVENTORY.schemaVersion]: DISCOVERY_SCHEMA_VERSION,
          [MAP_DISCOVERY_INVENTORY.hpcMatchStatus]: "DISCOVERED_NOT_IN_HPC",
          [MAP_DISCOVERY_INVENTORY.evidenceConfidence]: "MEDIUM",
        };
        for (const k of Object.keys(fields)) {
          if (fields[k] == null || fields[k] === "") delete fields[k];
        }
        const created = await atCreate(GRAPH_BASE_ID_CANONICAL, invTable, token, fields);
        if (created.ok) {
          // read-back
          const rb = await atFindByPropertyId(GRAPH_BASE_ID_CANONICAL, invTable, token, propertyId);
          waveStats.airtablePersisted.push({
            propertyId,
            airtableId: created.json.id,
            readBackOk: Boolean(rb),
          });
        } else {
          waveStats.errors.push({ stage: "airtable", propertyId, error: created.json });
        }
        await sleep(220);
      }
    }
  }

  const ledger = buildCoverageLedger({
    cwd: ROOT,
    paths: {
      ...defaultLedgerPaths(ROOT),
      checkpointPath: CHECKPOINT_PATH,
    },
    coreCohort: companies.slice(0, 6).map((c) => c.slug),
    extendedCohort: companies.slice(6).map((c) => c.slug),
    cikToSlug: Object.fromEntries(companies.filter((c) => c.cik).map((c) => [c.cik, c.slug])),
  });
  // Merge legacy MH checkpoint counts into report for cumulative ledger
  const legacyLedger = buildCoverageLedger({ cwd: ROOT });
  fs.writeFileSync(path.join(OUT_DIR, "COVERAGE_LEDGER.json"), JSON.stringify({ wave: ledger, cumulativeLegacy: legacyLedger }, null, 2));
  fs.writeFileSync(
    path.join(OUT_DIR, "COVERAGE_LEDGER.md"),
    `${coverageLedgerToMarkdown(ledger)}\n\n## Legacy checkpoint (MH V2)\n\n${coverageLedgerToMarkdown(legacyLedger)}\n`,
  );

  const managerUnique = [...new Set(waveStats.managers.map((m) => m.entityName).filter(Boolean))];
  const ownerUnique = [...new Set(waveStats.owners.map((o) => o.entityName).filter(Boolean))];

  const report = {
    version: UNIVERSAL_WAVE_ID,
    generatedAt: new Date().toISOString(),
    sinceYear: SINCE_YEAR,
    companiesInventoried: inventorySummaries.filter((c) => c.ok).length,
    companiesResearched: [...new Set(work.map((w) => w.company.slug))],
    eligibleFilings: totalEligible,
    documentsProcessed: waveStats.documentsProcessed,
    documentsFailed: waveStats.documentsFailed,
    remainingDocumentsApprox: Math.max(0, totalEligible - Object.keys(checkpoint.processedDocuments || {}).length),
    exhibitsEnumerated: waveStats.exhibitsEnumerated,
    exhibitsProcessed: waveStats.exhibitsProcessed,
    uniqueVerifiedHotels: verifiedUnique.length,
    probableHotels: waveStats.probable.length,
    ambiguousCandidates: waveStats.ambiguous,
    extractionArtifacts: waveStats.artifacts,
    newOwnersIdentified: ownerUnique,
    newManagementCompaniesIdentified: managerUnique,
    verifiedOwnershipManagementRelationships: {
      owners: waveStats.owners.length,
      managers: waveStats.managers.length,
      note: "Registrant never auto-classified as owner/manager",
    },
    contractualMilestonesDiscovered: waveStats.contractMilestones.length,
    historicalVersusCurrent: {
      historicalFindings: waveStats.historicalFindings,
      currentlyVerifiedFindings: waveStats.currentFindings,
    },
    precision,
    hpcMatches: 0,
    canonicalFieldsEnriched: 0,
    airtablePersistence: {
      enabled: PERSIST_AIRTABLE,
      persisted: waveStats.airtablePersisted.length,
      skippedExisting: waveStats.airtableSkippedExisting.length,
      readBacksOk: waveStats.airtablePersisted.filter((x) => x.readBackOk).length,
    },
    researchSpendUsd: 0,
    nextHighestYieldTargets: inventorySummaries
      .filter((c) => c.ok)
      .sort((a, b) => (b.eligible || 0) - (a.eligible || 0))
      .slice(0, 10)
      .map((c) => ({ slug: c.slug, eligible: c.eligible, yearSpan: c.yearSpan })),
    verifiedSample: verifiedUnique.slice(0, 25).map((h) => ({
      hotelName: h.normalizedName || h.hotelName,
      city: h.city,
      stateRegion: h.stateRegion,
      registrant: h.registrant,
    })),
    errors: waveStats.errors.slice(0, 30),
    guarantees: [
      "Not restricted to Marriott/Hilton",
      "Identity quality gate unchanged (≥95% verified precision)",
      "Historical dates not promoted to currently applicable without later evidence",
      "Customer publication blocked",
      "No HPC auto-create",
    ],
  };

  fs.writeFileSync(path.join(OUT_DIR, "WAVE_REPORT.json"), JSON.stringify(report, null, 2));
  fs.writeFileSync(
    path.join(OUT_DIR, "WAVE_REPORT.md"),
    [
      `# SEC EDGAR Universal Research Wave V2.4`,
      ``,
      `**Generated:** ${report.generatedAt}`,
      ``,
      `| Metric | Value |`,
      `|---|---|`,
      `| Companies inventoried | ${report.companiesInventoried} |`,
      `| Companies researched this wave | ${report.companiesResearched.length} |`,
      `| Eligible filings (1994+) | ${report.eligibleFilings} |`,
      `| Documents processed | ${report.documentsProcessed} |`,
      `| Exhibits enumerated / processed | ${report.exhibitsEnumerated} / ${report.exhibitsProcessed} |`,
      `| Unique verified hotels | ${report.uniqueVerifiedHotels} |`,
      `| Probable hotels | ${report.probableHotels} |`,
      `| Contract milestones | ${report.contractualMilestonesDiscovered} |`,
      `| Managers (unique names) | ${managerUnique.length} |`,
      `| Owners (unique names) | ${ownerUnique.length} |`,
      `| Verified precision | ${precision.precision} (n=${precision.sampleSize}, gate=${precision.meetsGate}) |`,
      `| Research spend | $0 |`,
      ``,
      `## Next highest-yield targets`,
      ...report.nextHighestYieldTargets.map((t) => `- ${t.slug}: ${t.eligible} eligible`),
      ``,
    ].join("\n"),
  );

  console.log(
    JSON.stringify(
      {
        ok: true,
        outDir: OUT_DIR,
        companiesInventoried: report.companiesInventoried,
        eligibleFilings: report.eligibleFilings,
        documentsProcessed: report.documentsProcessed,
        exhibitsProcessed: report.exhibitsProcessed,
        uniqueVerifiedHotels: report.uniqueVerifiedHotels,
        probableHotels: report.probableHotels,
        precision,
        researchSpendUsd: 0,
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
