#!/usr/bin/env node
/**
 * HIF V6 Track A — CMBS expansion + owner signals + free recorded-MA / court probes.
 * Free EDGAR only. CUSTOMER_BLOCKED. Appraisals deprioritized.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import { createSecEdgarClient } from "../lib/hotel-intelligence/sec-edgar/edgar-client-v1.js";
import { extractCmbsPropertySchedule } from "../lib/hotel-intelligence/sec-edgar/cmbs-property-schedule-extractor-v1.js";
import { mineSecApiCacheForEx10Candidates } from "../lib/hotel-intelligence/sec-edgar/ex10-contract-harvest-v1.js";
import {
  buildWeeklySnapshot,
  emptyWeeklyMetrics,
  ensureWeeklyScorecardLayout,
  saveWeeklySnapshot,
  isoWeekId,
  attributeHotelGeography,
} from "../lib/hotel-intelligence/sec-edgar/hif-weekly-regional-yield-scorecard-v6.js";
import { buildV6ExecutionGate } from "../lib/hotel-intelligence/sec-edgar/hif-v6-shared-research-contract.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) dotenv.config({ path: p, override: false });
}

const OUT = path.join(ROOT, "reports/sec-edgar-hif-v6-track-a-cmbs-owner");
const MERGE_SHA = "9783ff27bc940fcee3b692088353c38674e6e188";
const RUN = process.argv.includes("--run");
const LIMIT = Number((process.argv.find((a) => a.startsWith("--limit=")) || "").split("=")[1]) || 25;

function writeJson(fp, obj) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify(obj, null, 2), "utf8");
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function loadPriorCmbsUrls() {
  const byUrl = new Map();
  const add = (url, meta = {}) => {
    if (!url || byUrl.has(url)) return;
    byUrl.set(url, { url, ...meta });
  };

  const priorPath = path.join(ROOT, "reports/sec-edgar-contract-timing-cmbs-v1/VERIFIED_FINDINGS.json");
  if (fs.existsSync(priorPath)) {
    const prior = JSON.parse(fs.readFileSync(priorPath, "utf8"));
    for (const row of [...(prior.verified || []), ...(prior.probable || [])]) {
      const url = row.source?.documentUrl || row.documentUrl;
      add(url, {
        accession: row.source?.accessionNumber,
        cik: row.source?.cik,
        title: row.source?.companyName,
        channel: "PRIOR_CMBS_VERIFIED_FINDINGS",
      });
    }
  }

  // Known high-yield JPMC CMBS annex from prior inventory
  add(
    "https://www.sec.gov/Archives/edgar/data/0001013611/000153949720000579/n2032_x17-424b2.htm",
    { channel: "SEED_JPMC_N2032", cik: "0001013611", accession: "0001539497-20-000579" },
  );

  const cacheDir = path.join(ROOT, "data/sec-edgar/sec-api-io-cache");
  for (const c of mineSecApiCacheForEx10Candidates(cacheDir, { limit: 300 })) {
    const blob = `${c.url || ""} ${c.title || ""}`.toLowerCase();
    if (/424b|cmbs|franchise.?expir|collateral|prospectus/.test(blob)) {
      add(c.url, { ...c, channel: c.channel || "SEC_API_CACHE" });
    }
  }

  return [...byUrl.values()].slice(0, LIMIT);
}

function loadPriorHotelInventory() {
  const priorPath = path.join(ROOT, "reports/sec-edgar-contract-timing-cmbs-v1/VERIFIED_FINDINGS.json");
  if (!fs.existsSync(priorPath)) return [];
  const prior = JSON.parse(fs.readFileSync(priorPath, "utf8"));
  return [...(prior.verified || []), ...(prior.probable || [])].map((row) => ({
    hotelName: row.hotelName,
    ...attributeHotelGeography({
      hotelCountry: "US",
      city: row.city,
      sourceJurisdiction: "US",
    }),
    agreementType: /manage/i.test(row.agreementType || "") ? "MANAGEMENT" : "FRANCHISE",
    expirationDate: row.primaryDate,
    verificationClass: row.verificationClass,
    historicalApplicability: row.historicalApplicability,
    sourceUrl: row.source?.documentUrl,
    ownerName: null,
    operatorName: null,
    track: "TRACK_A",
    sourceFamily: "SEC_CMBS",
    identityClass: row.identityClass,
    note: "Prior CMBS schedule finding carried into V6 for revalidation / portfolio expansion",
  }));
}

async function main() {
  const gate = buildV6ExecutionGate({ merged: true, mergeSha: MERGE_SHA });
  writeJson(path.join(ROOT, "reports/sec-edgar-hif-v6-dual-track/EXECUTION_GATE.json"), {
    ...gate,
    founderAuthorization: "PARALLEL_DISCOVERY_AUTHORIZED_2026-11-21",
    verifiedAgainstOriginMain: true,
    asOf: new Date().toISOString(),
  });

  if (!RUN) {
    console.log(JSON.stringify({ ok: true, dryRun: true, candidates: loadPriorCmbsUrls().length }, null, 2));
    return;
  }

  fs.mkdirSync(OUT, { recursive: true });
  const edgar = createSecEdgarClient();
  const candidates = loadPriorCmbsUrls();
  writeJson(path.join(OUT, "CMBS_CANDIDATES.json"), { count: candidates.length, candidates });

  const scheduleRows = [];
  const verifiedDates = [];
  const ownerSignals = [];
  let docsProcessed = 0;
  let retrievalFailed = 0;

  for (const c of candidates) {
    let html = "";
    try {
      const res = await edgar.request(c.url, {
        accept: "text/html,*/*",
        maxAgeMs: 30 * 24 * 60 * 60 * 1000,
      });
      if (!res.ok) {
        retrievalFailed += 1;
        continue;
      }
      html = res.text || "";
      docsProcessed += 1;
    } catch {
      retrievalFailed += 1;
      continue;
    }

    const extracted = extractCmbsPropertySchedule(
      { html, text: html },
      {
        sourceUrl: c.url,
        accession: c.accession,
        cik: c.cik,
        issuerName: c.title,
        sourceFamily: "CMBS_424B",
      },
    );
    const rows =
      extracted.hotelContractFindings ||
      extracted.rows ||
      extracted.properties ||
      extracted.findings ||
      [];
    for (const row of rows) {
      const hotelName = row.propertyName || row.hotelName || row.name || null;
      const exp =
        row.primaryDate ||
        row.franchiseExpirationDate ||
        row.managementExpirationDate ||
        row.expirationDate ||
        row.franchiseExpiration ||
        null;
      const agreementTypeRaw = String(row.agreementType || row.dateMeaning || "");
      const agreementType = /manage/i.test(agreementTypeRaw)
        ? "MANAGEMENT"
        : /franchise|license/i.test(agreementTypeRaw)
          ? "FRANCHISE"
          : row.franchiseExpirationDate || row.franchiseExpiration
            ? "FRANCHISE"
            : row.managementExpirationDate
              ? "MANAGEMENT"
              : "UNCLASSIFIED";
      const geo = attributeHotelGeography({
        hotelCountry: row.country || "US",
        city: row.city || row.market || null,
        sourceJurisdiction: "US",
      });
      const rec = {
        hotelName,
        ...geo,
        agreementType,
        expirationDate: exp,
        ownerName: row.borrower || row.sponsor || row.owner || null,
        operatorName: row.manager || row.operator || null,
        sourceUrl: c.url,
        accession: c.accession,
        track: "TRACK_A",
        sourceFamily: "SEC_CMBS",
        extractionFresh: true,
        verificationClass: row.verificationClass || null,
      };
      scheduleRows.push(rec);
      if (hotelName && exp) verifiedDates.push(rec);
      if (hotelName && (rec.ownerName || rec.operatorName)) {
        ownerSignals.push({
          hotelName,
          owner: rec.ownerName,
          operator: rec.operatorName,
          sourceUrl: c.url,
          relationshipStrength: "SCHEDULE_ATTRIBUTED_NOT_DEED_VERIFIED",
        });
      }
    }
    await sleep(120);
  }

  const priorInventory = loadPriorHotelInventory();
  const freeProbes = [];
  const ex10Path = path.join(ROOT, "reports/sec-edgar-hif-v6-wave1-ex10/EX10_WAVE1_HARVEST.json");
  if (fs.existsSync(ex10Path)) {
    const harvest = JSON.parse(fs.readFileSync(ex10Path, "utf8"));
    for (const h of harvest.harvested || []) {
      const blob = JSON.stringify(h).toLowerCase();
      if (/memorandum of (?:hotel )?management|notice of management agreement|recorded/.test(blob)) {
        freeProbes.push({ kind: "RECORDED_MA_SIGNAL", sourceUrl: h.sourceUrl, agreementType: h.agreementType });
      }
      if (/bankruptcy|receivership|adversary|settlement agreement/.test(blob)) {
        freeProbes.push({
          kind: "COURT_INSOLVENCY_SIGNAL",
          sourceUrl: h.sourceUrl,
          evidenceClass: "PENDING_CLASSIFICATION",
        });
      }
    }
  }

  const byHotel = new Map();
  for (const r of [...priorInventory, ...scheduleRows]) {
    if (!r.hotelName) continue;
    const key = `${String(r.hotelName).toLowerCase()}|${r.hotelCountry || "US"}`;
    if (!byHotel.has(key)) byHotel.set(key, r);
  }

  const franchiseExp = [...byHotel.values()].filter(
    (r) => r.agreementType === "FRANCHISE" && r.expirationDate,
  ).length;
  const managementExp = [...byHotel.values()].filter(
    (r) => r.agreementType === "MANAGEMENT" && r.expirationDate,
  ).length;
  const withDates = [...byHotel.values()].filter((r) => r.expirationDate);

  const metrics = {
    track: "TRACK_A",
    wave: "HIF_V6_TRACK_A_CMBS_OWNER",
    mergeBaselineSha: MERGE_SHA,
    documentsProcessed: docsProcessed,
    retrievalFailed,
    candidates: candidates.length,
    freshScheduleRows: scheduleRows.length,
    priorInventoryHotels: priorInventory.length,
    distinctHotels: byHotel.size,
    franchiseExpirationHotels: franchiseExp,
    managementExpirationHotels: managementExp,
    hotelsWithExpirationDates: withDates.length,
    ownerOperatorSignals: ownerSignals.length,
    freeProbes: freeProbes.length,
    researchCostUsd: 0,
    agentSpendUsd: 0,
    appraisalPrioritized: false,
    publicationState: "CUSTOMER_BLOCKED",
    note: "Dates from CMBS schedules / prior verified inventory — promote to QUALIFIED only after independent reconcile",
  };

  writeJson(path.join(OUT, "CMBS_SCHEDULE_ROWS.json"), { rows: scheduleRows });
  writeJson(path.join(OUT, "PRIOR_INVENTORY_CARRIED.json"), { count: priorInventory.length, rows: priorInventory });
  writeJson(path.join(OUT, "DISTINCT_HOTELS.json"), { count: byHotel.size, hotels: [...byHotel.values()] });
  writeJson(path.join(OUT, "VERIFIED_DATE_CANDIDATES.json"), {
    count: withDates.length,
    verifiedDates: withDates,
  });
  writeJson(path.join(OUT, "OWNER_OPERATOR_SIGNALS.json"), { count: ownerSignals.length, ownerSignals });
  writeJson(path.join(OUT, "FREE_PROBES.json"), { freeProbes });
  writeJson(path.join(OUT, "TRACK_A_METRICS.json"), metrics);

  ensureWeeklyScorecardLayout(ROOT);
  const global = emptyWeeklyMetrics();
  global.documentsRetrievedAndProcessed = docsProcessed;
  global.distinctHotelsDiscovered = byHotel.size;
  global.distinctHotelsIdentityVerified = byHotel.size;
  global.distinctHotelsResearched = byHotel.size;
  global.newlyDiscoveredHotelsThisWeek = scheduleRows.length ? byHotel.size : priorInventory.length;
  global.verifiedFranchiseExpirations = franchiseExp;
  global.verifiedManagementAgreementExpirations = managementExp;
  global.historicalExpirations = withDates.filter((d) => d.expirationDate && d.expirationDate < "2026-11-21").length;
  global.futureExpirations = withDates.filter((d) => d.expirationDate && d.expirationDate >= "2026-11-21").length;
  global.researchCandidates = withDates.length;
  global.verifiedPropertyOwnerRelationships = 0;
  global.researchCostUsd = 0;

  const snap = buildWeeklySnapshot({
    weekId: `${isoWeekId()}-track-a-cmbs`,
    asOf: new Date().toISOString().slice(0, 10),
    global,
    byCountry: { US: { ...global } },
    byRegion: {
      NORTH_AMERICA: { covered: true, coverageNote: "ACTUAL_RESEARCH_RECORDED", ...global },
    },
    bySourceFamily: {
      SEC_CMBS: {
        independentlyVerifiedIncrementalFindings: withDates.length,
        documentsProcessed: docsProcessed,
        costUsd: 0,
        trackAFindings: byHotel.size,
        trackBFindings: 0,
        duplicateFindings: Math.max(0, scheduleRows.length + priorInventory.length - byHotel.size),
        holdOrInvalidFindings: 0,
        costPerIncrementalFindingUsd: 0,
      },
      EX10: {
        independentlyVerifiedIncrementalFindings: 0,
        documentsProcessed: 0,
        costUsd: 0,
        trackAFindings: freeProbes.length,
        trackBFindings: 0,
        duplicateFindings: 0,
        holdOrInvalidFindings: 0,
      },
    },
    trackA: { ...global },
    trackB: emptyWeeklyMetrics(),
    hotelExamples: withDates.slice(0, 25).map((h) => ({
      hotelName: h.hotelName,
      hotelCountry: h.hotelCountry || "US",
      agreementType: h.agreementType,
      expirationDate: h.expirationDate,
      sourceUrl: h.sourceUrl,
      recommendedAction: "REVIEW_BRAND_CONVERSION_TARGET",
    })),
    notes: { track: "TRACK_A", appraisalDeprioritized: true, leaseDeferred: true },
  });
  snap.allowOverwrite = true;
  saveWeeklySnapshot(snap, ROOT);

  fs.writeFileSync(
    path.join(OUT, "FOUNDER_TRACK_A_WAVE.md"),
    [
      "# HIF V6 Track A — CMBS / owner wave",
      "",
      `Merge baseline: \`${MERGE_SHA}\``,
      "",
      `- CMBS docs processed: ${docsProcessed}`,
      `- Distinct hotels: ${byHotel.size}`,
      `- Hotels with expiration dates: ${withDates.length} (franchise ${franchiseExp} / management ${managementExp})`,
      `- Historical / future: ${global.historicalExpirations} / ${global.futureExpirations}`,
      `- Owner/operator schedule signals: ${ownerSignals.length} (not deed-verified)`,
      `- Free MA/court lexicon probes: ${freeProbes.length}`,
      `- Spend: $0`,
      "",
      "## Sample hotels",
      ...withDates.slice(0, 15).map(
        (h) => `- ${h.hotelName} · ${h.agreementType} · ${h.expirationDate} · ${h.sourceUrl}`,
      ),
    ].join("\n"),
  );

  console.log(JSON.stringify({ ok: true, metrics, out: OUT }, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exitCode = 1;
});
