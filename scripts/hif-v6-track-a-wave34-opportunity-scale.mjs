#!/usr/bin/env node
/**
 * HIF V6 Track A Waves 3–4 — opportunities-first CMBS/EX-10 scale discovery.
 * Free EDGAR EFTS + archive retrieval. Dedupes against prior inventory.
 * Ownership enrichment deferred. CUSTOMER_BLOCKED. $0.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import { createSecEdgarClient } from "../lib/hotel-intelligence/sec-edgar/edgar-client-v1.js";
import { extractCmbsPropertySchedule } from "../lib/hotel-intelligence/sec-edgar/cmbs-property-schedule-extractor-v1.js";
import {
  buildWeeklySnapshot,
  emptyWeeklyMetrics,
  ensureWeeklyScorecardLayout,
  saveWeeklySnapshot,
  isoWeekId,
  attributeHotelGeography,
} from "../lib/hotel-intelligence/sec-edgar/hif-weekly-regional-yield-scorecard-v6.js";
import {
  OPPORTUNITY_MILESTONE_V6,
  progressTowardOpportunityMilestone,
} from "../lib/hotel-intelligence/sec-edgar/hif-v6-opportunities-first-priority-v6.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) dotenv.config({ path: p, override: false });
}

const OUT = path.join(ROOT, "reports/sec-edgar-hif-v6-track-a-wave34");
const RUN = process.argv.includes("--run");
const WAVE = (process.argv.find((a) => a.startsWith("--wave=")) || "--wave=3").split("=")[1];
const DOC_LIMIT =
  Number((process.argv.find((a) => a.startsWith("--doc-limit=")) || "").split("=")[1]) ||
  (WAVE === "4" ? 40 : 30);
const UA = "Dealality Research joan@aohospitalityadvisors.com";

const writeJson = (fp, obj) => {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify(obj, null, 2), "utf8");
};
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function hotelKey(name, country = "US") {
  return `${String(name || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim()}|${String(country || "US").toUpperCase()}`;
}

function loadPriorHotelKeys() {
  const keys = new Set();
  const paths = [
    "reports/sec-edgar-contract-timing-cmbs-v1/VERIFIED_FINDINGS.json",
    "reports/sec-edgar-hif-v6-track-a-cmbs-owner/DISTINCT_HOTELS.json",
    "reports/sec-edgar-hif-v6-track-a-wave2/NET_NEW_HOTEL_DATES.json",
    "reports/sec-edgar-hif-v6-track-a-wave34/OPPORTUNITY_INVENTORY.json",
  ];
  for (const rel of paths) {
    const fp = path.join(ROOT, rel);
    if (!fs.existsSync(fp)) continue;
    const j = JSON.parse(fs.readFileSync(fp, "utf8"));
    const rows = [
      ...(j.verified || []),
      ...(j.probable || []),
      ...(j.hotels || []),
      ...(j.rows || []),
      ...(j.opportunities || []),
    ];
    for (const r of rows) {
      const name = r.hotelName || r.propertyName;
      if (name) keys.add(hotelKey(name, r.hotelCountry || "US"));
    }
  }
  return keys;
}

function loadBaselineOpportunityCount() {
  // V5/V6 baseline commercial targets + wave2 net-new (historical tracked separately)
  const baselinePath = path.join(
    ROOT,
    "reports/sec-edgar-hif-v6-industrial-scale/V5_BASELINE_SCORECARD_V6_TAXONOMY.json",
  );
  let baseline = 139;
  if (fs.existsSync(baselinePath)) {
    const j = JSON.parse(fs.readFileSync(baselinePath, "utf8"));
    baseline = Number(j?.metrics?.QUALIFIED_COMMERCIAL_TARGETS || baseline);
  }
  const wave2 = path.join(ROOT, "reports/sec-edgar-hif-v6-track-a-wave2/NET_NEW_HOTEL_DATES.json");
  let wave2Future = 0;
  let wave2Historical = 0;
  if (fs.existsSync(wave2)) {
    const j = JSON.parse(fs.readFileSync(wave2, "utf8"));
    const asOf = new Date().toISOString().slice(0, 10);
    for (const h of j.hotels || []) {
      if (!h.expirationDate) continue;
      if (String(h.expirationDate) < asOf) wave2Historical += 1;
      else wave2Future += 1;
    }
  }
  return { baselineQualified: baseline, wave2Future, wave2Historical };
}

async function eftsSearch(query, forms, from = 0, size = 80) {
  const params = new URLSearchParams({
    q: query,
    dateRange: "custom",
    startdt: "2010-01-01",
    enddt: "2026-11-21",
    from: String(from),
    size: String(size),
  });
  if (forms) params.set("forms", forms);
  const url = `https://efts.sec.gov/LATEST/search-index?${params}`;
  const res = await fetch(url, {
    headers: { "User-Agent": UA, Accept: "application/json" },
  });
  const text = await res.text();
  let json;
  try {
    json = JSON.parse(text);
  } catch {
    return { ok: false, status: res.status, error: text.slice(0, 200), hits: [], total: 0 };
  }
  const hits = (json.hits?.hits || []).map((h) => {
    const src = h._source || {};
    const adshDashed = String(src.adsh || "");
    const adsh = adshDashed.replace(/-/g, "");
    const cik = String((src.ciks || [])[0] || "").replace(/^0+/, "") || "0";
    const fileName = String(h._id || "").split(":").pop() || "";
    const archiveUrl =
      cik && adsh && fileName
        ? `https://www.sec.gov/Archives/edgar/data/${cik}/${adsh}/${fileName}`
        : null;
    return {
      id: h._id,
      form: src.form || null,
      fileDate: src.file_date || null,
      display: (src.display_names || [])[0] || null,
      adsh: adshDashed,
      ciks: src.ciks || [],
      archiveUrl,
      score: h._score,
      query,
    };
  });
  return {
    ok: res.ok,
    status: res.status,
    total: json.hits?.total?.value ?? json.hits?.total ?? 0,
    hits,
  };
}

function classifyAgreement(row) {
  const blob = `${row.agreementType || ""} ${row.dateMeaning || ""} ${row.snippet || ""}`;
  if (/manage/i.test(blob)) return "MANAGEMENT";
  if (/franchise|license/i.test(blob)) return "FRANCHISE";
  return "UNCLASSIFIED";
}

function commercialThesis(agreementType, expirationDate, asOf) {
  const historical = expirationDate && String(expirationDate) < asOf;
  if (agreementType === "FRANCHISE") {
    return historical
      ? {
          list: "BRAND_CONVERSION_TARGETS",
          bucket: "HISTORICAL_CLOSED_SUPERSEDED",
          thesis: "Historical franchise expiration on CMBS schedule — research status before treating as active BCT",
          countsAsActiveOpportunity: false,
        }
      : {
          list: "BRAND_CONVERSION_TARGETS",
          bucket: "QUALIFIED_ACTIONABLE_OPPORTUNITIES",
          thesis: "Hotel-specific franchise expiration disclosed on CMBS property schedule — Brand Conversion Target research opportunity",
          countsAsActiveOpportunity: true,
        };
  }
  if (agreementType === "MANAGEMENT") {
    return historical
      ? {
          list: "MANAGEMENT_CONTRACT_TARGETS",
          bucket: "HISTORICAL_CLOSED_SUPERSEDED",
          thesis: "Historical management expiration — not automatically active MCT",
          countsAsActiveOpportunity: false,
        }
      : {
          list: "MANAGEMENT_CONTRACT_TARGETS",
          bucket: "QUALIFIED_ACTIONABLE_OPPORTUNITIES",
          thesis: "Hotel-specific management agreement expiration on schedule — Management Contract Target research opportunity",
          countsAsActiveOpportunity: true,
        };
  }
  return {
    list: null,
    bucket: "RESEARCH_CANDIDATES",
    thesis: "Hotel schedule row without clear franchise/management classification",
    countsAsActiveOpportunity: false,
  };
}

async function listFilingAnnexes(archiveUrl) {
  // Convert file URL → directory URL and list sibling HTM annexes.
  const m = String(archiveUrl || "").match(
    /^(https:\/\/www\.sec\.gov\/Archives\/edgar\/data\/\d+\/\d+)\//i,
  );
  if (!m) return [];
  const dirUrl = `${m[1]}/`;
  try {
    const res = await fetch(dirUrl, {
      headers: { "User-Agent": UA, Accept: "text/html" },
    });
    if (!res.ok) return [];
    const html = await res.text();
    const files = [...html.matchAll(/href="([^"]+\.htm[l]?)"/gi)].map((x) => x[1]);
    const out = [];
    for (const f of files) {
      const name = f.split("/").pop();
      if (!name || /index\.htm/i.test(name)) continue;
      // Prefer annex / exhibit / property schedule style filenames
      if (!/(x\d{1,2}|ex-?\d|annex|sched|424b)/i.test(name)) continue;
      const url = f.startsWith("http")
        ? f
        : f.startsWith("/")
          ? `https://www.sec.gov${f}`
          : `${dirUrl}${name}`;
      out.push({ fileName: name, archiveUrl: url, fromDirectory: dirUrl });
    }
    return out;
  } catch {
    return [];
  }
}

async function discoverDocumentUrls(wave) {
  const queries =
    wave === "4"
      ? [
          // Prefer schedule-bearing docs (header co-occurrence), not prose-only prospectus hits
          ['"Franchise Expiration Date" "Property Name"', null],
          ['"Franchise Expiration Date" Courtyard OR Hilton OR Marriott OR Hampton', null],
          ['"Management Agreement Expiration" "Property Name"', null],
          ['"Remaining Franchise Term" "Property Name"', null],
          ['"Franchise Expiration Date" Annex', null],
        ]
      : [
          ['"Franchise Expiration Date" "Property Name"', null],
          ['"Franchise Expiration Date"', "424B2"],
          ['"Franchise Expiration Date"', null],
          ['"Franchise Expiration"', "424B2"],
        ];

  const byUrl = new Map();
  const queryStats = [];
  for (const [q, forms] of queries) {
    const res = await eftsSearch(q, forms, 0, 100);
    queryStats.push({
      query: q,
      forms,
      total: res.total,
      hitsReturned: res.hits.length,
      ok: res.ok,
    });
    for (const h of res.hits) {
      if (!h.archiveUrl || byUrl.has(h.archiveUrl)) continue;
      byUrl.set(h.archiveUrl, h);
    }
    await sleep(300);
  }

  // Expand each EFTS hit into sibling annex files (prospectus wrappers rarely hold schedules).
  const expanded = new Map();
  const seedHits = [...byUrl.values()].slice(0, Math.min(60, byUrl.size));
  for (const hit of seedHits) {
    expanded.set(hit.archiveUrl, hit);
    const annexes = await listFilingAnnexes(hit.archiveUrl);
    for (const a of annexes) {
      if (expanded.has(a.archiveUrl)) continue;
      // Skip giant primary prospectus filenames when annex siblings exist
      expanded.set(a.archiveUrl, {
        ...hit,
        id: `${hit.adsh || "filing"}:${a.fileName}`,
        archiveUrl: a.archiveUrl,
        expandedFrom: hit.archiveUrl,
        fileName: a.fileName,
      });
    }
    await sleep(120);
  }

  const ranked = [...expanded.values()].sort((a, b) => {
    const score = (d) => {
      const f = String(d.fileName || d.id || d.archiveUrl || "");
      let s = Number(d.score) || 0;
      if (/x\d{1,2}[-_]|-x\d{1,2}\./i.test(f)) s += 100;
      if (/ex-?\d/i.test(f)) s += 40;
      // Deprioritize multi-MB primary prospectus wrappers
      if (/^n\d+_424b2\.htm$/i.test(f) || /prospectus/i.test(f)) s -= 50;
      if (d.expandedFrom) s += 20;
      return s;
    };
    return score(b) - score(a);
  });
  return { docs: ranked.slice(0, DOC_LIMIT), queryStats, expandedCount: expanded.size };
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  if (!RUN) {
    console.log(JSON.stringify({ ok: true, dryRun: true, wave: WAVE, docLimit: DOC_LIMIT }, null, 2));
    return;
  }

  const asOf = new Date().toISOString().slice(0, 10);
  const priorKeys = loadPriorHotelKeys();
  const baseline = loadBaselineOpportunityCount();
  const { docs, queryStats } = await discoverDocumentUrls(WAVE);
  writeJson(path.join(OUT, `WAVE${WAVE}_EFTS_QUERIES.json`), { queryStats, docCount: docs.length });
  writeJson(path.join(OUT, `WAVE${WAVE}_DOC_URLS.json`), { count: docs.length, docs });

  const edgar = createSecEdgarClient();
  const docResults = [];
  const netNew = [];
  const reprocessed = [];
  const historical = [];
  const activeOpps = [];
  const signalsNoDate = [];
  let scheduleRows = 0;
  let franchiseNew = 0;
  let managementNew = 0;

  for (const doc of docs) {
    const row = {
      url: doc.archiveUrl,
      id: doc.id,
      query: doc.query,
      ok: false,
      hotelsExtracted: 0,
      netNew: 0,
      error: null,
    };
    try {
      const res = await edgar.request(doc.archiveUrl, {
        accept: "text/html,*/*",
        maxAgeMs: 30 * 24 * 60 * 60 * 1000,
      });
      if (!res.ok) {
        row.error = `HTTP_${res.status || res.error}`;
        docResults.push(row);
        await sleep(150);
        continue;
      }
      row.ok = true;
      const extracted = extractCmbsPropertySchedule(
        { html: res.text, text: res.text },
        { sourceUrl: doc.archiveUrl, sourceFamily: "CMBS_424B" },
      );
      const findings = extracted.hotelContractFindings || [];
      scheduleRows += findings.length;
      for (const f of findings) {
        const hotelName = f.hotelName || f.propertyName;
        const exp = f.primaryDate || f.expirationDate;
        if (!hotelName) continue;
        row.hotelsExtracted += 1;
        const agreementType = classifyAgreement(f);
        const geo = attributeHotelGeography({
          hotelCountry: "US",
          city: f.city,
          sourceJurisdiction: "US",
        });
        const key = hotelKey(hotelName, "US");
        const thesis = commercialThesis(agreementType, exp, asOf);
        const opp = {
          hotelName,
          ...geo,
          agreementType,
          expirationDate: exp || null,
          verificationClass: f.verificationClass || (exp ? "SCHEDULE_EXTRACTED" : "AGREEMENT_SIGNAL"),
          dateMeaning: f.dateMeaning || null,
          sourceUrl: doc.archiveUrl,
          sourceFamily: "SEC_CMBS",
          track: "TRACK_A",
          wave: `WAVE${WAVE}`,
          commercialList: thesis.list,
          trackingBucket: thesis.bucket,
          commercialThesis: thesis.thesis,
          countsAsActiveOpportunity: thesis.countsAsActiveOpportunity,
          ownershipRequired: false,
          ownershipStatus: "DEFERRED",
        };
        if (!exp) {
          signalsNoDate.push(opp);
          continue;
        }
        if (priorKeys.has(key)) {
          reprocessed.push({ hotelName, expirationDate: exp, sourceUrl: doc.archiveUrl });
          continue;
        }
        priorKeys.add(key);
        netNew.push(opp);
        row.netNew += 1;
        if (thesis.countsAsActiveOpportunity) activeOpps.push(opp);
        else historical.push(opp);
        if (agreementType === "FRANCHISE") franchiseNew += 1;
        else if (agreementType === "MANAGEMENT") managementNew += 1;
      }
    } catch (err) {
      row.error = String(err?.message || err).slice(0, 120);
    }
    docResults.push(row);
    await sleep(150);
  }

  // Cumulative inventory estimate (baseline + prior wave2 future + this wave active)
  const cumulativeActiveEstimate =
    baseline.baselineQualified + baseline.wave2Future + activeOpps.length;
  // Note: baseline already includes many hotels; wave2 future may partially overlap baseline.
  // Prefer strict inventory file merge for founder report.
  const inventoryPath = path.join(OUT, "OPPORTUNITY_INVENTORY.json");
  let inventory = { opportunities: [], historical: [], signalsWithoutDates: [] };
  if (fs.existsSync(inventoryPath)) {
    inventory = JSON.parse(fs.readFileSync(inventoryPath, "utf8"));
  }
  const invKeys = new Set(
    [...(inventory.opportunities || []), ...(inventory.historical || [])].map((o) =>
      hotelKey(o.hotelName, o.hotelCountry || "US"),
    ),
  );
  for (const o of activeOpps) {
    const k = hotelKey(o.hotelName, o.hotelCountry || "US");
    if (invKeys.has(k)) continue;
    invKeys.add(k);
    inventory.opportunities.push(o);
  }
  for (const o of historical) {
    const k = hotelKey(o.hotelName, o.hotelCountry || "US");
    if (invKeys.has(k)) continue;
    invKeys.add(k);
    inventory.historical.push(o);
  }
  inventory.signalsWithoutDates = [
    ...(inventory.signalsWithoutDates || []),
    ...signalsNoDate,
  ].slice(0, 500);

  const distinctInventory = inventory.opportunities.length;
  // Combine with baseline for progress — use max of baseline+wave increments vs inventory-only
  // Strict founder progress: baselineQualified already counted; add only net-new active from waves after baseline cut.
  const progressCurrent = baseline.baselineQualified + baseline.wave2Future + distinctInventory;
  // Deduplicate overlap risk: if inventory already includes wave2, wave2Future double-counts.
  // Safer progress = baseline + inventory opportunities only (wave2 was not in baseline QUALIFIED count as net-new).
  const progressSafe = baseline.baselineQualified + distinctInventory;
  const progress = progressTowardOpportunityMilestone({
    distinctEvidenceBackedOpportunities: progressSafe,
  });

  const metrics = {
    track: "TRACK_A",
    waves: `WAVE${WAVE}`,
    priority: "OPPORTUNITIES_FIRST",
    publicationState: "CUSTOMER_BLOCKED",
    researchCostUsd: 0,
    documentsDiscovered: docs.length,
    documentsRetrievedOk: docResults.filter((d) => d.ok).length,
    documentsFailed: docResults.filter((d) => !d.ok).length,
    scheduleRowsExtracted: scheduleRows,
    reprocessedPriorHotels: reprocessed.length,
    netNewHotelsWithDates: netNew.length,
    netNewFranchiseExpirations: franchiseNew,
    netNewManagementExpirations: managementNew,
    netNewActiveOpportunities: activeOpps.length,
    netNewHistoricalExpirations: historical.length,
    evidenceBackedSignalsWithoutDates: signalsNoDate.length,
    inventoryActiveOpportunitiesThisBranch: distinctInventory,
    baselineQualifiedCommercialTargets: baseline.baselineQualified,
    progressToward1000: progress,
    progressNote:
      "progressSafe = baseline QUALIFIED_COMMERCIAL_TARGETS + net-new active opportunities accumulated in this branch inventory (historical excluded)",
    ownershipEnrichment: "DEFERRED",
  };

  writeJson(path.join(OUT, `WAVE${WAVE}_DOC_RESULTS.json`), { docs: docResults });
  writeJson(path.join(OUT, `WAVE${WAVE}_NET_NEW.json`), {
    count: netNew.length,
    active: activeOpps.length,
    historical: historical.length,
    hotels: netNew,
  });
  writeJson(path.join(OUT, `WAVE${WAVE}_REPROCESSED.json`), {
    count: reprocessed.length,
    sample: reprocessed.slice(0, 50),
  });
  writeJson(inventoryPath, {
    ...inventory,
    countActive: inventory.opportunities.length,
    countHistorical: inventory.historical.length,
    updatedAt: new Date().toISOString(),
    milestone: OPPORTUNITY_MILESTONE_V6,
  });
  writeJson(path.join(OUT, `WAVE${WAVE}_METRICS.json`), metrics);

  ensureWeeklyScorecardLayout(ROOT);
  const global = emptyWeeklyMetrics();
  global.distinctHotelsDiscovered = netNew.length;
  global.distinctHotelsIdentityVerified = netNew.length;
  global.newlyDiscoveredHotelsThisWeek = netNew.length;
  global.verifiedFranchiseExpirations = franchiseNew;
  global.verifiedManagementAgreementExpirations = managementNew;
  global.historicalExpirations = historical.length;
  global.futureExpirations = activeOpps.length;
  global.newBrandConversionTargets = activeOpps.filter(
    (o) => o.commercialList === "BRAND_CONVERSION_TARGETS",
  ).length;
  global.newManagementContractTargets = activeOpps.filter(
    (o) => o.commercialList === "MANAGEMENT_CONTRACT_TARGETS",
  ).length;
  global.actionableCommercialOpportunities = activeOpps.length;
  global.evidenceBackedOpportunityCandidates = activeOpps.length;
  global.newlyQualifiedCommercialOpportunities = activeOpps.length;
  global.progressToward1000Opportunities = progress.current;
  global.documentsRetrievedAndProcessed = docResults.filter((d) => d.ok).length;
  global.researchCostUsd = 0;
  const snap = buildWeeklySnapshot({
    weekId: `${isoWeekId()}-track-a-wave${WAVE}`,
    asOf,
    global,
    byCountry: { US: { ...global } },
    byRegion: {
      NORTH_AMERICA: { covered: true, coverageNote: "ACTUAL_RESEARCH_RECORDED", ...global },
    },
    bySourceFamily: {
      SEC_CMBS: {
        independentlyVerifiedIncrementalFindings: activeOpps.length,
        documentsProcessed: docResults.filter((d) => d.ok).length,
        costUsd: 0,
        trackAFindings: activeOpps.length,
        trackBFindings: 0,
        duplicateFindings: reprocessed.length,
      },
    },
    trackA: global,
    notes: {
      priority: "OPPORTUNITIES_FIRST",
      ownership: "DEFERRED",
      progressToward1000: `${progress.current}/${progress.target}`,
    },
    allowOverwrite: true,
  });
  saveWeeklySnapshot(snap, ROOT);

  console.log(JSON.stringify(metrics, null, 2));
}

main().catch((e) => {
  console.error(String(e?.stack || e));
  process.exit(1);
});
