#!/usr/bin/env node
/**
 * Commercial target discovery scale-up pilot — ≤$25, 100–200 candidate records.
 * Optimizes verified hotel end-dates + notice when available (not legal-term depth).
 *
 *   node scripts/sec-edgar-commercial-target-scale-pilot.mjs --run --limit=180
 *
 * No HPC. CUSTOMER_BLOCKED. No Meta schema apply. No customer publication.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import { createContractTimingV2Budget } from "../lib/hotel-intelligence/sec-edgar/contract-timing-v2-budget-v1.js";
import { createExaContractTimingClient } from "../lib/hotel-intelligence/sec-edgar/exa-contract-timing-client-v1.js";
import { createSecEdgarClient } from "../lib/hotel-intelligence/sec-edgar/edgar-client-v1.js";
import { mineSecApiCacheForEx10Candidates, enrichCandidateFromEdgarUrl } from "../lib/hotel-intelligence/sec-edgar/ex10-contract-harvest-v1.js";
import { extractCommercialTimingObservations } from "../lib/hotel-intelligence/sec-edgar/commercial-expiration-notice-extract-v1.js";
import {
  classifyCommercialOpportunity,
  projectDualProspectLists,
  summarizeCommercialInventory,
  OPPORTUNITY_TIER,
} from "../lib/hotel-intelligence/sec-edgar/commercial-target-discovery-v1.js";
import { listUniversalCompaniesV24 } from "../lib/hotel-intelligence/sec-edgar/cik-registry-v24.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of ["C:/Dev/dealality/my-operators-backend/.env", path.join(ROOT, ".env")]) {
  if (fs.existsSync(p)) dotenv.config({ path: p, override: false });
}

const OUT = path.join(ROOT, "reports/sec-edgar-commercial-targets-v1");
const LIMIT = Math.min(
  200,
  Math.max(100, Number((process.argv.find((a) => a.startsWith("--limit=")) || "").split("=")[1]) || 160),
);
const MAX_PAID = 25;

function ensureDir(d) {
  fs.mkdirSync(d, { recursive: true });
}
function writeJson(fp, obj) {
  ensureDir(path.dirname(fp));
  fs.writeFileSync(fp, JSON.stringify(obj, null, 2), "utf8");
}

function mineCmbsCacheCandidates(cacheDir, limit) {
  if (!fs.existsSync(cacheDir)) return [];
  const byUrl = new Map();
  for (const name of fs.readdirSync(cacheDir)) {
    if (!name.startsWith("full_text") || !name.endsWith(".json")) continue;
    let raw;
    try {
      raw = JSON.parse(fs.readFileSync(path.join(cacheDir, name), "utf8"));
    } catch {
      continue;
    }
    for (const x of raw?.result?.filings || []) {
      const url = String(x.filingUrl || x.linkToHtml || "").trim();
      if (!url || !/sec\.gov/i.test(url)) continue;
      const blob = `${url} ${x.description || ""} ${x.formType || ""} ${x.companyName || ""}`.toLowerCase();
      const isCmbs =
        /424b|424h|cmbs|pros.?supp|free.?writing/i.test(blob) ||
        /franchise expiration|management agreement expiration|property schedule/i.test(blob);
      const isEx10Hotel =
        /ex-?10|dex10|exv10/i.test(url) &&
        /hotel|management|franchise|lodging|hospitality/i.test(blob);
      if (!isCmbs && !isEx10Hotel) continue;
      if (byUrl.has(url)) continue;
      byUrl.set(url, enrichCandidateFromEdgarUrl({
        channel: isCmbs ? "SEC_API_CACHE_CMBS" : "SEC_API_CACHE_EX10",
        title: x.companyNameLong || x.companyName || null,
        cik: x.cik != null ? String(x.cik).padStart(10, "0") : null,
        accession: x.accessionNo || x.accessionNumber || null,
        form: x.formType || null,
        filedAt: x.filedAt || null,
        url,
        priority: isCmbs ? 2 : 1,
      }));
      if (byUrl.size >= limit * 2) break;
    }
    if (byUrl.size >= limit * 2) break;
  }
  return [...byUrl.values()].sort((a, b) => (b.priority || 0) - (a.priority || 0));
}

function loadPriorHighYieldUrls() {
  const paths = [
    "reports/sec-edgar-contract-timing-v2/V2_FINDINGS_INCREMENTAL.json",
    "reports/sec-edgar-contract-timing-v2/V2_PILOT_REPORT.json",
    "reports/sec-edgar-contract-timing-cmbs-v1/PILOT_REPORT.json",
    "reports/sec-edgar-identity-quality-v2/VERIFIED_HOTEL_INTELLIGENCE_INVENTORY.json",
  ];
  const byUrl = new Map();
  for (const rel of paths) {
    const fp = path.join(ROOT, rel);
    if (!fs.existsSync(fp)) continue;
    let raw;
    try {
      raw = JSON.parse(fs.readFileSync(fp, "utf8"));
    } catch {
      continue;
    }
    const findings = [
      ...(raw.secApiIo || []),
      ...(raw.findings || []),
      ...(raw.hotelContractFindings || []),
      ...(raw.verifiedHotels || []),
      ...(Array.isArray(raw) ? raw : []),
    ];
    for (const f of findings) {
      const url = f.source?.documentUrl || f.documentUrl || f.sourceUrl || null;
      if (!url || !/sec\.gov/i.test(url)) continue;
      if (!byUrl.has(url)) {
        byUrl.set(url, enrichCandidateFromEdgarUrl({
          channel: "PRIOR_HIGH_YIELD",
          title: f.source?.companyName || f.companyName || null,
          cik: f.source?.cik || f.cik || null,
          accession: f.source?.accessionNumber || f.accession || null,
          url,
          priority: 5,
        }));
      }
    }
  }
  return [...byUrl.values()];
}

function normalizePriorFinding(f, sourceFamily) {
  if (!f?.hotelName || !f.primaryDate || !f.hotelAttributed) return null;
  const notes = (f.notes || []).join(" ");
  // Only add header provenance when the extractor recorded an explicit Franchise Expiration column.
  // Never invent franchise-expiration language for year-opened / ambiguous prior labels.
  const headerCtx = /HTML_TABLE_FRANCHISE_EXPIRATION_COLUMN/i.test(notes)
    ? "Franchise Expiration Date column | "
    : /HTML_TABLE_MANAGEMENT_EXPIRATION/i.test(notes)
      ? "Management Agreement Expiration column | "
      : "";
  return {
    hotelName: f.hotelName,
    hotelVerified: true,
    hotelAttributed: Boolean(f.hotelAttributed),
    location: [f.city, f.stateRegion].filter(Boolean).join(", ") || null,
    brand: f.brandLabel || null,
    owner: f.borrower || f.owner || null,
    managementCompany: f.manager || null,
    agreementType: /MANAGEMENT/i.test(f.agreementType || "")
      ? "MANAGEMENT"
      : /LICENSE/i.test(f.agreementType || "")
        ? "LICENSE"
        : "FRANCHISE",
    primaryDate: f.primaryDate,
    dateMeaning: f.dateMeaning,
    evidenceSnippet: `${headerCtx}${f.snippet || ""}`.slice(0, 500),
    sourceUrl: f.source?.documentUrl || f.sourceUrl || null,
    accession: f.source?.accessionNumber || f.accession || null,
    cik: f.source?.cik || f.cik || null,
    issuerName: f.source?.companyName || f.registrant || null,
    sourceFamily,
    expirationStillCited: true,
  };
}

function loadPriorFindingsAsObservations() {
  const files = [
    ["reports/sec-edgar-contract-timing-v2/V2_FINDINGS_INCREMENTAL.json", "PRIOR_V2"],
    ["reports/sec-edgar-contract-timing-v3/V3_FINDINGS_NEW.json", "PRIOR_V3"],
    ["reports/sec-edgar-contract-timing-cmbs-v1/VERIFIED_FINDINGS.json", "PRIOR_CMBS_V1"],
  ];
  const out = [];
  for (const [rel, fam] of files) {
    const fp = path.join(ROOT, rel);
    if (!fs.existsSync(fp)) continue;
    let raw;
    try {
      raw = JSON.parse(fs.readFileSync(fp, "utf8"));
    } catch {
      continue;
    }
    const findings = [
      ...(raw.secApiIo || []),
      ...(raw.exa || []),
      ...(raw.edgar || []),
      ...(raw.findings || []),
      ...(raw.verified || []),
    ];
    for (const f of findings) {
      const n = normalizePriorFinding(f, fam);
      if (n) out.push(n);
    }
  }

  // Identity inventory → Tier C research candidates (no invented expirations)
  const invPath = path.join(
    ROOT,
    "reports/sec-edgar-identity-quality-v2/VERIFIED_HOTEL_INTELLIGENCE_INVENTORY.json",
  );
  if (fs.existsSync(invPath)) {
    const inv = JSON.parse(fs.readFileSync(invPath, "utf8"));
    for (const row of [...(inv.rows || []), ...(inv.probableRows || [])].slice(0, 200)) {
      if (!row.hotelName) continue;
      // Skip rows that already have contract findings (covered above)
      if ((row.contractFindings || []).some((c) => c.primaryDate)) continue;
      out.push({
        hotelName: row.hotelName,
        hotelVerified: true,
        hotelAttributed: true,
        location: [row.city, row.stateRegion].filter(Boolean).join(", ") || null,
        brand: row.brandHint || row.brandLabel || row.historicalBrand || null,
        owner: null,
        managementCompany: null,
        agreementType: "UNKNOWN",
        primaryDate: null,
        evidenceSnippet: row.sourceLine || null,
        sourceUrl: row.sourceUrl || null,
        accession: row.sourceAccession || null,
        cik: row.cik || null,
        issuerName: row.registrant || null,
        sourceFamily: "IDENTITY_INVENTORY_TIER_C",
        conversionSignals: { brand: Boolean(row.brandHint || row.historicalBrand), management: false },
        listHints: ["BRAND_CONVERSION"],
      });
    }
  }
  return out;
}

async function discoverCandidates(budget, limit) {
  const cacheDir = path.join(ROOT, "data/sec-edgar/sec-api-io-cache");
  const byUrl = new Map();

  // Highest priority: URLs that previously produced hotel franchise expiration rows
  for (const c of loadPriorHighYieldUrls()) {
    if (c.url && !byUrl.has(c.url)) byUrl.set(c.url, c);
  }
  for (const c of mineCmbsCacheCandidates(cacheDir, limit)) {
    if (c.url && !byUrl.has(c.url)) byUrl.set(c.url, c);
  }
  for (const c of mineSecApiCacheForEx10Candidates(cacheDir, { limit: limit })) {
    const n = enrichCandidateFromEdgarUrl(c);
    if (n.url && !byUrl.has(n.url)) byUrl.set(n.url, { ...n, priority: 1 });
  }

  // Light Exa complement for franchise expiration schedules (budgeted)
  const exa = createExaContractTimingClient({ budget });
  const queries = [
    'site:sec.gov "Franchise Expiration" hotel OR lodging 424B',
    'site:sec.gov "management agreement" expires hotel EX-10',
    'site:sec.gov "Franchise Expiration Date" Courtyard OR Hilton OR Marriott',
  ];
  for (const q of queries) {
    if (byUrl.size >= limit * 2) break;
    const gate = budget.canSpend(0.02);
    if (!gate.ok) break;
    try {
      const res = await exa.search({ query: q, numResults: 8, type: "lite" });
      for (const h of res?.results || []) {
        const url = h.url || "";
        if (!/sec\.gov/i.test(url) || byUrl.has(url)) continue;
        byUrl.set(url, enrichCandidateFromEdgarUrl({
          channel: "EXA_COMMERCIAL",
          title: h.title || null,
          url,
          priority: 3,
        }));
      }
    } catch {
      /* non-fatal */
    }
  }

  // Prefer CMBS schedules + explicit franchise expiration URLs
  return [...byUrl.values()]
    .sort((a, b) => {
      const ae = /franchise.?expir|424b|management.?expir/i.test(a.url + (a.title || "")) ? 1 : 0;
      const be = /franchise.?expir|424b|management.?expir/i.test(b.url + (b.title || "")) ? 1 : 0;
      return be + (b.priority || 0) - (ae + (a.priority || 0));
    })
    .slice(0, limit);
}

function dedupeObservations(rows) {
  const byKey = new Map();
  let dupes = 0;
  for (const r of rows) {
    const key = [
      String(r.hotelName || "").toLowerCase().replace(/\s+/g, " ").trim(),
      r.agreementType || "",
      r.primaryDate || "nodate",
    ].join("|");
    if (byKey.has(key)) {
      dupes += 1;
      continue;
    }
    byKey.set(key, r);
  }
  return { unique: [...byKey.values()], duplicatesRejected: dupes };
}

async function main() {
  ensureDir(OUT);
  const registry = listUniversalCompaniesV24();
  const registryCiks = new Set(registry.map((c) => String(c.cik || "").padStart(10, "0")));

  const budget = createContractTimingV2Budget({
    ceilingUsd: MAX_PAID,
    ledgerPath: path.join(ROOT, "data/sec-edgar/v5-commercial-target-budget-ledger.json"),
    cwd: ROOT,
  });
  const edgar = createSecEdgarClient();

  const candidates = await discoverCandidates(budget, LIMIT);
  writeJson(path.join(OUT, "SOURCE_CANDIDATES.json"), { count: candidates.length, candidates });

  const allObs = [];
  const allRejected = [];
  let documentsExamined = 0;
  let docsWithText = 0;
  const beyondRegistry = new Set();

  // Seed with prior findings — re-audited through date-semantic gate (never trust old VERIFIED blindly)
  allObs.push(...loadPriorFindingsAsObservations());

  // Fixture regression seed (prove extractor path; not inflated live yield)
  for (const rel of [
    "fixtures/sec-edgar/contract-timing-cmbs-schedule-sample.txt",
    "fixtures/sec-edgar/diamondrock-franchise-expiration-schedule-v2.txt",
  ]) {
    const fp = path.join(ROOT, rel);
    if (!fs.existsSync(fp)) continue;
    const extracted = extractCommercialTimingObservations(
      { text: fs.readFileSync(fp, "utf8") },
      { sourceUrl: `fixture://${rel}`, sourceFamily: "FIXTURE" },
    );
    documentsExamined += 1;
    docsWithText += 1;
    allObs.push(...extracted.observations.map((o) => ({ ...o, fromFixture: true })));
    allRejected.push(...(extracted.rejected || []));
  }

  for (const c of candidates) {
    let text = "";
    try {
      const res = await edgar.request(c.url, {
        accept: "text/html,*/*",
        maxAgeMs: 30 * 24 * 60 * 60 * 1000,
      });
      documentsExamined += 1;
      if (res.ok) {
        text = res.text || "";
        docsWithText += 1;
      }
    } catch {
      documentsExamined += 1;
    }
    if (!text) continue;

    // Franchise Expiration headers often appear deep in CMBS annexes — search full doc
    const feIdx = text.search(/franchise\s+expiration|management\s+agreement\s+expiration/i);
    if (text.length > 4_000_000 && feIdx < 0) {
      allRejected.push({
        hotelName: null,
        reason: "skipped_large_doc_without_franchise_or_management_expiration_header",
        sourceUrl: c.url,
      });
      continue;
    }

    // Cap HTML parse cost: window around expiration schedule headers
    let parseText = text;
    if (text.length > 1_500_000) {
      if (feIdx >= 0) {
        parseText = text.slice(Math.max(0, feIdx - 80_000), feIdx + 1_200_000);
      } else {
        parseText = text.slice(0, 1_500_000);
      }
    }

    const extracted = extractCommercialTimingObservations(
      { html: parseText },
      {
        sourceUrl: c.url,
        accession: c.accession,
        cik: c.cik,
        issuerName: c.title,
        sourceFamily: c.channel,
      },
    );
    for (const o of extracted.observations) {
      allObs.push(o);
      if (o.cik && !registryCiks.has(String(o.cik).padStart(10, "0"))) {
        beyondRegistry.add(String(o.cik).padStart(10, "0"));
      }
    }
    allRejected.push(...(extracted.rejected || []));
  }

  // Re-extract from prior V5 EX-10 harvest URLs already retrieved (cache-first) — no invented hotel names
  const priorHarvestPath = path.join(ROOT, "reports/sec-edgar-factory-v5/EX10_HARVEST_RESULTS.json");
  if (fs.existsSync(priorHarvestPath)) {
    const prior = JSON.parse(fs.readFileSync(priorHarvestPath, "utf8"));
    const seenUrl = new Set(candidates.map((c) => c.url));
    for (const h of (prior.harvested || []).slice(0, 40)) {
      if (!h.sourceUrl || seenUrl.has(h.sourceUrl)) continue;
      seenUrl.add(h.sourceUrl);
      let text = "";
      try {
        const res = await edgar.request(h.sourceUrl, {
          accept: "text/html,*/*",
          maxAgeMs: 30 * 24 * 60 * 60 * 1000,
        });
        documentsExamined += 1;
        if (res.ok) {
          text = res.text || "";
          docsWithText += 1;
        }
      } catch {
        documentsExamined += 1;
      }
      if (!text) continue;
      const extracted = extractCommercialTimingObservations(
        { html: text },
        {
          sourceUrl: h.sourceUrl,
          accession: h.accession,
          cik: h.cik,
          issuerName: h.issuerName,
          agreementType: h.agreementType,
          namedHotel: h.hotelAttributionAllowed,
          sourceFamily: "PRIOR_EX10_HARVEST",
        },
      );
      allObs.push(...extracted.observations);
      allRejected.push(...(extracted.rejected || []));
    }
  }

  // Live research inventory excludes fixtures (fixtures prove extractor only)
  const liveObs = allObs.filter((o) => !o.fromFixture && !String(o.sourceUrl || "").startsWith("fixture://"));
  const fixtureObs = allObs.filter((o) => o.fromFixture || String(o.sourceUrl || "").startsWith("fixture://"));
  const { unique, duplicatesRejected } = dedupeObservations(liveObs);
  const fixtureExtracted = dedupeObservations(fixtureObs).unique.length;

  for (const o of unique) {
    if (o.cik && !registryCiks.has(String(o.cik).padStart(10, "0"))) {
      beyondRegistry.add(String(o.cik).padStart(10, "0"));
    }
  }

  const opportunities = unique.map((o) => classifyCommercialOpportunity(o, { horizonMonths: 36 }));
  const projections = projectDualProspectLists(opportunities);
  const summary = summarizeCommercialInventory(opportunities, projections);

  const spend = budget.snapshot();
  const verifiedTargets =
    (summary.byTier[OPPORTUNITY_TIER.TIER_A_VERIFIED_DECISION_WINDOW] || 0) +
    (summary.byTier[OPPORTUNITY_TIER.TIER_B_VERIFIED_CONTRACT_EXPIRATION] || 0);
  const costPerVerified =
    verifiedTargets > 0 ? Number((spend.spentUsd / verifiedTargets).toFixed(4)) : null;

  // Owner/operator coverage (from extracted fields only — no paid people enrichment)
  let withOwner = 0;
  let withMgmt = 0;
  let withBrand = 0;
  for (const o of opportunities) {
    if (o.owner) withOwner += 1;
    if (o.managementCompany) withMgmt += 1;
    if (o.brand) withBrand += 1;
  }

  const sampleTargets = opportunities
    .filter(
      (o) =>
        o.tier === OPPORTUNITY_TIER.TIER_A_VERIFIED_DECISION_WINDOW ||
        o.tier === OPPORTUNITY_TIER.TIER_B_VERIFIED_CONTRACT_EXPIRATION,
    )
    .slice(0, 15)
    .map((o) => ({
      hotel: o.hotelName,
      location: o.location,
      brand: o.brand,
      owner: o.owner,
      agreementType: o.agreementType,
      verifiedEndDate: o.verifiedEndDate,
      noticeStatus: o.noticeStatus,
      noticeDeadline: o.noticeDeadline,
      monthsToExpiration: o.monthsToExpiration,
      tier: o.tier,
      sourceUrl: o.sourceUrl,
      confidence: o.confidence,
    }));

  const sourceYield = {};
  for (const o of opportunities) {
    if (o.tier === OPPORTUNITY_TIER.HOLD_QUARANTINE) continue;
    const fam = o.dateAudit ? (o.sourceUrl || "").includes("424") ? "CMBS_424B" : "OTHER" : "OTHER";
    // better: use observation sourceFamily via researchId lookup
  }
  for (const o of unique) {
    const fam = o.sourceFamily || "UNKNOWN";
    sourceYield[fam] = sourceYield[fam] || { observations: 0, verifiedTierAB: 0 };
    sourceYield[fam].observations += 1;
  }
  for (const o of opportunities) {
    if (
      o.tier === OPPORTUNITY_TIER.TIER_A_VERIFIED_DECISION_WINDOW ||
      o.tier === OPPORTUNITY_TIER.TIER_B_VERIFIED_CONTRACT_EXPIRATION
    ) {
      // approximate family from URL
      const fam = /424b|cmbs/i.test(o.sourceUrl || "")
        ? "CMBS_OR_SCHEDULE"
        : /ex-?10|dex10/i.test(o.sourceUrl || "")
          ? "NARRATIVE_EXHIBIT"
          : "OTHER";
      sourceYield[fam] = sourceYield[fam] || { observations: 0, verifiedTierAB: 0 };
      sourceYield[fam].verifiedTierAB += 1;
    }
  }

  const metrics = {
    documentsExamined,
    documentsWithText: docsWithText,
    candidatesEnumerated: candidates.length,
    rawObservations: allObs.length,
    distinctHotelObservations: unique.length,
    fixtureObservationsExtracted: fixtureExtracted,
    duplicatesRejected,
    misclassifiedOrRejected: allRejected.length + (summary.rejectedFalseExpirations || 0),
    issuersBeyondRegistry: beyondRegistry.size,
    issuersBeyondRegistryCiks: [...beyondRegistry],
    registrySizeCompared: registry.length,
    ...summary,
    ownerCoverage: withOwner,
    managementCoverage: withMgmt,
    brandCoverage: withBrand,
    sampleHighQualityTargets: sampleTargets,
    sourceFamilyYield: sourceYield,
    spentUsd: spend.spentUsd,
    ceilingUsd: MAX_PAID,
    incrementalCostPerVerifiedTarget: costPerVerified,
    publicationState: "CUSTOMER_BLOCKED",
    hpcCreates: 0,
  };

  writeJson(path.join(OUT, "OPPORTUNITIES.json"), { opportunities });
  writeJson(path.join(OUT, "BRAND_CONVERSION_TARGETS.json"), {
    count: projections.brandConversionTargets.length,
    targets: projections.brandConversionTargets,
  });
  writeJson(path.join(OUT, "MANAGEMENT_CONTRACT_TARGETS.json"), {
    count: projections.managementContractTargets.length,
    targets: projections.managementContractTargets,
  });
  writeJson(path.join(OUT, "REJECTED_FALSE_EXPIRATIONS.json"), {
    scheduleRejected: allRejected.slice(0, 500),
    summaryRejected: summary.rejectedFalseExpirationSamples,
  });
  writeJson(path.join(OUT, "PILOT_METRICS.json"), metrics);
  writeJson(path.join(OUT, "PILOT_SPEND.json"), spend);

  console.log(JSON.stringify({ ok: true, metrics }, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
