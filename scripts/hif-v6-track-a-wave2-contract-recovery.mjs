#!/usr/bin/env node
/**
 * HIF V6 Track A Wave 2 — expand CMBS schedules + recover management source-priority pointers.
 * Free EDGAR only. Deduplicate against prior inventory. CUSTOMER_BLOCKED.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import { createSecEdgarClient } from "../lib/hotel-intelligence/sec-edgar/edgar-client-v1.js";
import { extractCmbsPropertySchedule } from "../lib/hotel-intelligence/sec-edgar/cmbs-property-schedule-extractor-v1.js";
import { harvestExhibitDocument } from "../lib/hotel-intelligence/sec-edgar/ex10-contract-harvest-v1.js";
import {
  buildWeeklySnapshot,
  emptyWeeklyMetrics,
  ensureWeeklyScorecardLayout,
  saveWeeklySnapshot,
  isoWeekId,
  attributeHotelGeography,
} from "../lib/hotel-intelligence/sec-edgar/hif-weekly-regional-yield-scorecard-v6.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) dotenv.config({ path: p, override: false });
}

const OUT = path.join(ROOT, "reports/sec-edgar-hif-v6-track-a-wave2");
const RUN = process.argv.includes("--run");
const CMBS_LIMIT = Number((process.argv.find((a) => a.startsWith("--cmbs-limit=")) || "").split("=")[1]) || 12;
const MGMT_LIMIT = Number((process.argv.find((a) => a.startsWith("--mgmt-limit=")) || "").split("=")[1]) || 21;

const writeJson = (fp, obj) => {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify(obj, null, 2), "utf8");
};
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function loadPriorHotelKeys() {
  const keys = new Set();
  const paths = [
    "reports/sec-edgar-contract-timing-cmbs-v1/VERIFIED_FINDINGS.json",
    "reports/sec-edgar-hif-v6-track-a-cmbs-owner/DISTINCT_HOTELS.json",
  ];
  for (const rel of paths) {
    const fp = path.join(ROOT, rel);
    if (!fs.existsSync(fp)) continue;
    const j = JSON.parse(fs.readFileSync(fp, "utf8"));
    const rows = [...(j.verified || []), ...(j.probable || []), ...(j.hotels || []), ...(j.rows || [])];
    for (const r of rows) {
      const name = r.hotelName || r.propertyName;
      if (name) keys.add(`${String(name).toLowerCase()}|US`);
    }
  }
  return keys;
}

function collectCmbsUrls() {
  const byUrl = new Map();
  const add = (url, meta = {}) => {
    if (!url || byUrl.has(url)) return;
    byUrl.set(url, { url, ...meta });
  };
  const prior = path.join(ROOT, "reports/sec-edgar-contract-timing-cmbs-v1/VERIFIED_FINDINGS.json");
  if (fs.existsSync(prior)) {
    const j = JSON.parse(fs.readFileSync(prior, "utf8"));
    for (const row of [...(j.verified || []), ...(j.probable || [])]) {
      add(row.source?.documentUrl, {
        cik: row.source?.cik,
        accession: row.source?.accessionNumber,
        channel: "PRIOR_FINDINGS",
      });
    }
  }
  // Additional known hotel CMBS annex seeds (free EDGAR) beyond the single n2032 deal when available
  const seeds = [
    "https://www.sec.gov/Archives/edgar/data/1013611/000153949720000579/n2032_x17-424b2.htm",
    "https://www.sec.gov/Archives/edgar/data/1013611/000153949720000850/n2234_x6-424h.htm",
    "https://www.sec.gov/Archives/edgar/data/1005007/000153949720000828/n2231-x5_424h.htm",
  ];
  for (const url of seeds) add(url, { channel: "WAVE2_SEED" });

  // Management pointer queue URLs (appraisal routing) — often UW annexes with agreement maps
  const mgmtQ = path.join(
    ROOT,
    "reports/sec-edgar-hif-v6-appraisal-policy/MANAGEMENT_CONTRACT_DISCOVERY_QUEUE.json",
  );
  if (fs.existsSync(mgmtQ)) {
    const q = JSON.parse(fs.readFileSync(mgmtQ, "utf8"));
    for (const item of q.items || []) add(item.url, { channel: "MGMT_POINTER_QUEUE", docId: item.docId });
  }
  return [...byUrl.values()].slice(0, CMBS_LIMIT);
}

function loadMgmtPointers() {
  const fp = path.join(
    ROOT,
    "reports/sec-edgar-hif-v6-appraisal-policy/MANAGEMENT_CONTRACT_DISCOVERY_QUEUE.json",
  );
  if (!fs.existsSync(fp)) return [];
  const q = JSON.parse(fs.readFileSync(fp, "utf8"));
  return (q.items || []).slice(0, MGMT_LIMIT);
}

function normalizeFinding(row, sourceUrl, accession) {
  const hotelName = row.hotelName || row.propertyName || row.name || null;
  const exp = row.primaryDate || row.expirationDate || row.franchiseExpirationDate || null;
  const agreementTypeRaw = String(row.agreementType || row.dateMeaning || "");
  const agreementType = /manage/i.test(agreementTypeRaw)
    ? "MANAGEMENT"
    : /franchise|license/i.test(agreementTypeRaw)
      ? "FRANCHISE"
      : "UNCLASSIFIED";
  return {
    hotelName,
    ...attributeHotelGeography({ hotelCountry: "US", city: row.city, sourceJurisdiction: "US" }),
    agreementType,
    expirationDate: exp,
    verificationClass: row.verificationClass || "SCHEDULE_EXTRACTED",
    sourceUrl,
    accession,
    track: "TRACK_A",
    wave: "WAVE2",
  };
}

async function main() {
  if (!RUN) {
    console.log(
      JSON.stringify(
        {
          ok: true,
          dryRun: true,
          cmbsUrls: collectCmbsUrls().length,
          mgmtPointers: loadMgmtPointers().length,
          priorHotels: loadPriorHotelKeys().size,
        },
        null,
        2,
      ),
    );
    return;
  }

  fs.mkdirSync(OUT, { recursive: true });
  const edgar = createSecEdgarClient();
  const priorKeys = loadPriorHotelKeys();
  const cmbsUrls = collectCmbsUrls();
  const mgmtPointers = loadMgmtPointers();
  writeJson(path.join(OUT, "WAVE2_CMBS_URLS.json"), { count: cmbsUrls.length, cmbsUrls });
  writeJson(path.join(OUT, "WAVE2_MGMT_POINTERS.json"), { count: mgmtPointers.length, mgmtPointers });

  const freshRows = [];
  let docsOk = 0;
  let docsFail = 0;
  for (const c of cmbsUrls) {
    try {
      const res = await edgar.request(c.url, {
        accept: "text/html,*/*",
        maxAgeMs: 30 * 24 * 60 * 60 * 1000,
      });
      if (!res.ok) {
        docsFail += 1;
        continue;
      }
      docsOk += 1;
      const extracted = extractCmbsPropertySchedule(
        { html: res.text, text: res.text },
        { sourceUrl: c.url, accession: c.accession, cik: c.cik, sourceFamily: "CMBS_424B" },
      );
      for (const row of extracted.hotelContractFindings || []) {
        freshRows.push(normalizeFinding(row, c.url, c.accession));
      }
    } catch {
      docsFail += 1;
    }
    await sleep(120);
  }

  // Management pointer recovery — fetch underlying docs; harvest exhibit terms
  const mgmtRecoveries = [];
  for (const item of mgmtPointers) {
    const rec = {
      docId: item.docId,
      url: item.url,
      issuerHint: item.issuerHint,
      pointerOnly: true,
      retrieved: false,
      hotelCandidates: [],
      agreementType: null,
      expirationDate: null,
      status: "PENDING",
    };
    try {
      const res = await edgar.request(item.url, {
        accept: "text/html,text/plain,*/*",
        maxAgeMs: 30 * 24 * 60 * 60 * 1000,
      });
      if (!res.ok) {
        rec.status = `RETRIEVAL_FAILED_${res.status || res.error}`;
        mgmtRecoveries.push(rec);
        continue;
      }
      rec.retrieved = true;
      const h = harvestExhibitDocument(
        { html: res.text },
        { sourceUrl: item.url, accession: null, issuerName: item.issuerHint },
      );
      rec.agreementType = h.agreementType;
      rec.instrumentClass = h.instrumentClass;
      rec.scopeClass = h.scopeClass;
      rec.disclosedFieldCount = h.disclosedFieldCount;
      rec.extracted = h.extracted;
      // Also run CMBS extractor in case annex tables exist
      const sched = extractCmbsPropertySchedule(
        { html: res.text, text: res.text },
        { sourceUrl: item.url, sourceFamily: "UW_OR_CMBS" },
      );
      for (const row of sched.hotelContractFindings || []) {
        const n = normalizeFinding(row, item.url, null);
        if (/MANAGE/i.test(n.agreementType) || /manage/i.test(String(row.dateMeaning || ""))) {
          n.agreementType = "MANAGEMENT";
        }
        freshRows.push(n);
        rec.hotelCandidates.push(n);
      }
      rec.status =
        rec.hotelCandidates.length > 0
          ? "HOTEL_SCHEDULE_ROWS_FOUND"
          : h.persistResearchEligible
            ? "EXHIBIT_TERMS_PARTIAL_NO_HOTEL_DATE"
            : "RETRIEVED_NO_VERIFIED_CONTRACT_DATE";
    } catch (err) {
      rec.status = `ERROR_${String(err?.message || err).slice(0, 80)}`;
    }
    mgmtRecoveries.push(rec);
    await sleep(120);
  }

  // Deduplicate: net-new vs prior inventory
  const byHotel = new Map();
  const netNew = [];
  const reprocessed = [];
  for (const r of freshRows) {
    if (!r.hotelName || !r.expirationDate) continue;
    const key = `${String(r.hotelName).toLowerCase()}|${r.hotelCountry || "US"}`;
    if (!byHotel.has(key)) byHotel.set(key, r);
    if (priorKeys.has(key)) reprocessed.push(r);
    else netNew.push(r);
  }
  // unique net-new
  const netNewDistinct = [];
  const seenNew = new Set();
  for (const r of netNew) {
    const key = `${String(r.hotelName).toLowerCase()}|${r.hotelCountry || "US"}`;
    if (seenNew.has(key)) continue;
    seenNew.add(key);
    netNewDistinct.push(r);
  }

  const franchiseNet = netNewDistinct.filter((r) => r.agreementType === "FRANCHISE");
  const managementNet = netNewDistinct.filter((r) => r.agreementType === "MANAGEMENT");

  const metrics = {
    track: "TRACK_A",
    wave: "WAVE2",
    documentsRetrievedOk: docsOk,
    documentsFailed: docsFail,
    cmbsUrlsAttempted: cmbsUrls.length,
    mgmtPointersAttempted: mgmtPointers.length,
    mgmtPointersRetrieved: mgmtRecoveries.filter((r) => r.retrieved).length,
    scheduleRowsExtracted: freshRows.filter((r) => r.expirationDate).length,
    distinctHotelsInWaveExtract: byHotel.size,
    reprocessedPriorHotels: reprocessed.length,
    netNewHotelsWithDates: netNewDistinct.length,
    netNewFranchiseExpirations: franchiseNet.length,
    netNewManagementExpirations: managementNet.length,
    recordedMaFound: 0,
    courtExhibitsFound: 0,
    propertyVerifiedOwners: 0,
    researchCostUsd: 0,
    publicationState: "CUSTOMER_BLOCKED",
    note: "Net-new counts exclude hotels already in prior CMBS/V6 inventory keys",
  };

  writeJson(path.join(OUT, "MGMT_POINTER_RECOVERIES.json"), { recoveries: mgmtRecoveries });
  writeJson(path.join(OUT, "NET_NEW_HOTEL_DATES.json"), {
    count: netNewDistinct.length,
    hotels: netNewDistinct,
  });
  writeJson(path.join(OUT, "REPROCESSED_PRIOR_HOTELS.json"), {
    count: reprocessed.length,
    note: "Not counted as newly discovered",
  });
  writeJson(path.join(OUT, "TRACK_A_WAVE2_METRICS.json"), metrics);

  ensureWeeklyScorecardLayout(ROOT);
  const global = emptyWeeklyMetrics();
  global.documentsRetrievedAndProcessed = docsOk + mgmtRecoveries.filter((r) => r.retrieved).length;
  global.distinctHotelsResearched = byHotel.size;
  global.newlyDiscoveredHotelsThisWeek = netNewDistinct.length;
  global.verifiedFranchiseExpirations = franchiseNet.length;
  global.verifiedManagementAgreementExpirations = managementNet.length;
  global.historicalExpirations = netNewDistinct.filter((d) => d.expirationDate < "2026-11-21").length;
  global.futureExpirations = netNewDistinct.filter((d) => d.expirationDate >= "2026-11-21").length;
  global.researchCandidates = mgmtRecoveries.filter((r) =>
    ["EXHIBIT_TERMS_PARTIAL_NO_HOTEL_DATE", "HOTEL_SCHEDULE_ROWS_FOUND"].includes(r.status),
  ).length;
  global.researchCostUsd = 0;

  const snap = buildWeeklySnapshot({
    weekId: `${isoWeekId()}-track-a-wave2`,
    asOf: new Date().toISOString().slice(0, 10),
    global,
    byCountry: { US: { ...global } },
    byRegion: {
      NORTH_AMERICA: { covered: true, coverageNote: "ACTUAL_RESEARCH_RECORDED", ...global },
    },
    bySourceFamily: {
      SEC_CMBS: {
        independentlyVerifiedIncrementalFindings: netNewDistinct.length,
        documentsProcessed: docsOk,
        costUsd: 0,
        trackAFindings: byHotel.size,
        trackBFindings: 0,
        duplicateFindings: reprocessed.length,
        holdOrInvalidFindings: 0,
        costPerIncrementalFindingUsd: 0,
      },
    },
    trackA: { ...global },
    trackB: emptyWeeklyMetrics(),
    hotelExamples: netNewDistinct.slice(0, 20).map((h) => ({
      hotelName: h.hotelName,
      hotelCountry: h.hotelCountry,
      agreementType: h.agreementType,
      expirationDate: h.expirationDate,
      sourceUrl: h.sourceUrl,
      recommendedAction:
        h.agreementType === "MANAGEMENT"
          ? "REVIEW_MANAGEMENT_CONTRACT_TARGET"
          : "REVIEW_BRAND_CONVERSION_TARGET",
    })),
    notes: {
      wave: "WAVE2",
      reprocessedNotCountedAsNew: true,
      appraisalDeprioritized: true,
    },
  });
  snap.allowOverwrite = true;
  saveWeeklySnapshot(snap, ROOT);

  fs.writeFileSync(
    path.join(OUT, "FOUNDER_TRACK_A_WAVE2.md"),
    [
      "# HIF V6 Track A — Wave 2",
      "",
      `- CMBS/UW docs retrieved: ${docsOk}`,
      `- Management pointers attempted/retrieved: ${mgmtPointers.length}/${metrics.mgmtPointersRetrieved}`,
      `- Distinct hotels in extract: ${byHotel.size}`,
      `- Reprocessed prior (not new): ${reprocessed.length}`,
      `- **Net-new hotels with dates: ${netNewDistinct.length}** (franchise ${franchiseNet.length} / management ${managementNet.length})`,
      `- Recorded MA / court: 0 / 0 this wave`,
      `- Property-verified owners: 0`,
      `- Spend: $0`,
      "",
      "## Net-new examples",
      ...(netNewDistinct.length
        ? netNewDistinct
            .slice(0, 15)
            .map((h) => `- ${h.hotelName} · ${h.agreementType} · ${h.expirationDate} · ${h.sourceUrl}`)
        : ["_No net-new hotel dates beyond prior inventory in this controlled batch._"]),
    ].join("\n"),
  );

  console.log(JSON.stringify({ ok: true, metrics, out: OUT }, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exitCode = 1;
});
