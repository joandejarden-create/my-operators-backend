#!/usr/bin/env node
/**
 * HIF V6 Track A — Five-country concentration opportunity discovery.
 * Free EDGAR EFTS + CMBS extract + commercial-signal harvest.
 * Ownership deferred. CUSTOMER_BLOCKED. $0.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import { createSecEdgarClient } from "../lib/hotel-intelligence/sec-edgar/edgar-client-v1.js";
import { extractCmbsPropertySchedule } from "../lib/hotel-intelligence/sec-edgar/cmbs-property-schedule-extractor-v1.js";
import { harvestExhibitDocument } from "../lib/hotel-intelligence/sec-edgar/ex10-contract-harvest-v1.js";
import {
  attributeHotelGeography,
  buildWeeklySnapshot,
  emptyWeeklyMetrics,
  ensureWeeklyScorecardLayout,
  isoWeekId,
  saveWeeklySnapshot,
} from "../lib/hotel-intelligence/sec-edgar/hif-weekly-regional-yield-scorecard-v6.js";
import {
  COUNTRY_PROSPECT_AMBITIONS_V6,
  PRIORITY_COUNTRIES_V6,
  computeYieldPer1000,
  emptyCountryScorecardRow,
  progressTowardAmbition,
} from "../lib/hotel-intelligence/sec-edgar/hif-v6-country-concentration-v6.js";
import { progressTowardOpportunityMilestone } from "../lib/hotel-intelligence/sec-edgar/hif-v6-opportunities-first-priority-v6.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) dotenv.config({ path: p, override: false });
}

const OUT = path.join(ROOT, "reports/sec-edgar-hif-v6-country-concentration");
const RUN = process.argv.includes("--run");
const UA = "Dealality Research joan@aohospitalityadvisors.com";
const writeJson = (fp, obj) => {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify(obj, null, 2), "utf8");
};
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function hotelKey(name, country) {
  return `${String(name || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim()}|${String(country || "").toUpperCase()}`;
}

function isNoisyName(name) {
  const n = String(name || "").trim();
  if (n.length < 10 || n.length > 70) return true;
  return /^(the\s+)?(particular|per|interim|borrower|loan agreement|english translation|holdings introduction|development services|form of|each|any|such)\b/i.test(
    n,
  ) || /\b(agreement|amendment|exhibit|introduction|translation|borrower|loan)\b/i.test(n);
}

function extractHotelNames(text, limit = 10) {
  const names = [];
  const re =
    /\b((?:The\s+)?[A-Z][A-Za-z0-9&'.\-]{1,35}(?:\s+[A-Z][A-Za-z0-9&'.\-]{1,25}){0,4}\s+(?:Hotel|Resort|Inn|Suites))\b/g;
  let m;
  while ((m = re.exec(text)) && names.length < limit) {
    const n = m[1].replace(/\s+/g, " ").trim();
    if (!isNoisyName(n)) names.push(n);
  }
  return [...new Set(names)];
}

async function efts(q, forms, size = 30) {
  const p = new URLSearchParams({
    q,
    dateRange: "custom",
    startdt: "2014-01-01",
    enddt: "2026-11-21",
    from: "0",
    size: String(size),
  });
  if (forms) p.set("forms", forms);
  const r = await fetch(`https://efts.sec.gov/LATEST/search-index?${p}`, {
    headers: { "User-Agent": UA, Accept: "application/json" },
  });
  const j = await r.json();
  return {
    total: j.hits?.total?.value ?? j.hits?.total ?? 0,
    hits: (j.hits?.hits || []).map((h) => {
      const src = h._source || {};
      const adsh = String(src.adsh || "").replace(/-/g, "");
      const cik = String((src.ciks || [])[0] || "").replace(/^0+/, "");
      const fileName = String(h._id || "").split(":").pop();
      return {
        id: h._id,
        url: `https://www.sec.gov/Archives/edgar/data/${cik}/${adsh}/${fileName}`,
        fileDate: src.file_date,
        fileType: src.file_type,
        form: src.form,
        q,
      };
    }),
  };
}

function loadPriorKeys() {
  const keys = new Set();
  const paths = [
    "reports/sec-edgar-contract-timing-cmbs-v1/VERIFIED_FINDINGS.json",
    "reports/sec-edgar-hif-v6-track-a-wave2/NET_NEW_HOTEL_DATES.json",
    "reports/sec-edgar-hif-v6-track-a-wave34/FINALIZED_OPPORTUNITY_INVENTORY.json",
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
      ...(j.opportunities || []),
      ...(j.datedActiveOpportunities || []),
      ...(j.historicalExpirations || []),
      ...(j.evidenceBackedSignalsWithoutDates || []),
    ];
    for (const r of rows) {
      const n = r.hotelName || r.propertyName;
      if (n) keys.add(hotelKey(n, r.hotelCountry || "US"));
    }
  }
  return keys;
}

/** Country-specific discovery query packs (Track A commercial/contract). */
const COUNTRY_QUERIES = {
  US: [
    { q: '"Franchise Expiration Date" "Property Name"', forms: null, kind: "CMBS_SCHEDULE" },
    { q: '"Management Agreement Expiration" hotel', forms: null, kind: "CMBS_OR_DISCLOSURE" },
    { q: "hotel brand conversion OR reflag OR reflagged", forms: "8-K", kind: "CONVERSION_SIGNAL" },
    { q: '"franchise agreement" termination hotel OR inn', forms: "8-K", kind: "TERMINATION_SIGNAL" },
  ],
  MX: [
    { q: "Mexico hotel franchise OR management agreement", forms: null, kind: "MX_DISCLOSURE" },
    { q: '"Mexico" "hotel" ("management agreement" OR franchise OR reflag)', forms: "8-K", kind: "MX_SIGNAL" },
    { q: "Cancun OR Cabo OR Guadalajara hotel franchise OR management", forms: null, kind: "MX_MARKET" },
    { q: "Grupo Hotelero OR Fibra Hotel OR Fibra Inn", forms: null, kind: "MX_ISSUER" },
  ],
  BR: [
    { q: "Brazil hotel management OR franchise OR condo-hotel OR flat", forms: null, kind: "BR_DISCLOSURE" },
    { q: '"Brazil" hotel ("management agreement" OR franchise OR operator)', forms: "6-K", kind: "BR_6K" },
    { q: "Sao Paulo OR Rio hotel franchise OR management agreement", forms: null, kind: "BR_MARKET" },
    { q: "Atlantis Condos OR Hotelaria OR Accor Brazil", forms: null, kind: "BR_ISSUER" },
  ],
  GB: [
    { q: '"United Kingdom" OR London hotel ("management agreement" OR franchise)', forms: null, kind: "UK_DISCLOSURE" },
    { q: "London hotel franchise OR management agreement OR reflag", forms: "8-K", kind: "UK_SIGNAL" },
    { q: "IHG OR InterContinental hotel management agreement London OR UK", forms: null, kind: "UK_IHG" },
    { q: "Premier Inn OR Whitbread hotel franchise OR management", forms: null, kind: "UK_OPERATOR" },
  ],
  CA: [
    { q: "Canada hotel franchise OR management agreement OR reflag", forms: null, kind: "CA_DISCLOSURE" },
    { q: '"Canada" hotel ("management agreement" OR franchise)', forms: "8-K", kind: "CA_SIGNAL" },
    { q: "Toronto OR Vancouver OR Montreal hotel franchise OR management", forms: null, kind: "CA_MARKET" },
    { q: "American Hotel Income Properties OR InnVest OR Dream Hotel", forms: null, kind: "CA_REIT" },
  ],
};

function inferCountryFromQueryAndText(countryHint, text, hotelName) {
  const blob = `${hotelName || ""} ${String(text || "").slice(0, 5000)}`.toLowerCase();
  if (countryHint === "US") {
    if (/\b(mexico|cancun|guadalajara|brazil|sao paulo|london|toronto|vancouver|montreal|united kingdom)\b/i.test(hotelName || "")) {
      // may be non-US
    } else return "US";
  }
  if (countryHint === "MX" || /\b(mexico|cancún|cancun|cabo|guadalajara|cdmx|ciudad de méxico)\b/i.test(blob)) {
    return "MX";
  }
  if (countryHint === "BR" || /\b(brazil|brasil|são paulo|sao paulo|rio de janeiro)\b/i.test(blob)) {
    return "BR";
  }
  if (countryHint === "GB" || /\b(united kingdom|london|manchester|edinburgh|scotland|wales|england)\b/i.test(blob)) {
    return "GB";
  }
  if (countryHint === "CA" || /\b(canada|toronto|vancouver|montreal|ontario|quebec|british columbia)\b/i.test(blob)) {
    return "CA";
  }
  return countryHint;
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  if (!RUN) {
    console.log(JSON.stringify({ ok: true, dryRun: true, countries: PRIORITY_COUNTRIES_V6 }, null, 2));
    return;
  }

  const asOf = new Date().toISOString().slice(0, 10);
  const priorKeys = loadPriorKeys();
  const edgar = createSecEdgarClient();
  const byCountry = Object.fromEntries(PRIORITY_COUNTRIES_V6.map((c) => [c, emptyCountryScorecardRow(c)]));
  const queryStats = [];
  const docResults = [];
  const datedActive = [];
  const historical = [];
  const undatedSignals = [];
  const conversions = [];
  const researchedHotels = new Set();
  let duplicatesRemoved = 0;

  for (const country of PRIORITY_COUNTRIES_V6) {
    const packs = COUNTRY_QUERIES[country] || [];
    const urls = new Map();
    for (const pack of packs) {
      const res = await efts(pack.q, pack.forms, country === "US" ? 35 : 20);
      queryStats.push({
        country,
        ...pack,
        total: res.total,
        returned: res.hits.length,
      });
      for (const h of res.hits) {
        if (!urls.has(h.url)) urls.set(h.url, { ...h, countryHint: country, kind: pack.kind });
      }
      await sleep(250);
    }

    const docs = [...urls.values()].slice(0, country === "US" ? 28 : 14);
    for (const doc of docs) {
      const row = {
        countryHint: country,
        url: doc.url,
        kind: doc.kind,
        ok: false,
        error: null,
        findings: 0,
        netNew: 0,
      };
      try {
        const res = await edgar.request(doc.url, {
          accept: "text/html,*/*",
          maxAgeMs: 14 * 24 * 3600 * 1000,
        });
        if (!res.ok) {
          row.error = `HTTP_${res.status || res.error}`;
          docResults.push(row);
          await sleep(120);
          continue;
        }
        row.ok = true;
        const text = res.text || "";

        if (doc.kind === "CMBS_SCHEDULE" || /Franchise Expiration Date/i.test(text)) {
          const extracted = extractCmbsPropertySchedule(
            { html: text, text },
            { sourceUrl: doc.url, sourceFamily: "CMBS_424B" },
          );
          for (const f of extracted.hotelContractFindings || []) {
            const hotelName = f.hotelName || f.propertyName;
            const exp = f.primaryDate || f.expirationDate;
            if (!hotelName || !exp) continue;
            const cc = inferCountryFromQueryAndText(country, text, hotelName);
            if (!PRIORITY_COUNTRIES_V6.includes(cc) && country === "US") {
              // keep US default for CMBS US schedules
            }
            const hotelCountry = country === "US" ? "US" : cc;
            researchedHotels.add(hotelKey(hotelName, hotelCountry));
            byCountry[hotelCountry] = byCountry[hotelCountry] || emptyCountryScorecardRow(hotelCountry);
            byCountry[hotelCountry].distinctHotelsResearched += 1;
            row.findings += 1;
            const key = hotelKey(hotelName, hotelCountry);
            if (priorKeys.has(key)) {
              duplicatesRemoved += 1;
              byCountry[hotelCountry].duplicatesRemoved += 1;
              continue;
            }
            priorKeys.add(key);
            const agType = /manage/i.test(String(f.agreementType || f.dateMeaning || ""))
              ? "MANAGEMENT"
              : "FRANCHISE";
            const hist = String(exp) < asOf;
            const opp = {
              hotelName,
              ...attributeHotelGeography({
                hotelCountry,
                city: f.city,
                sourceJurisdiction: "US",
              }),
              agreementType: agType,
              expirationDate: exp,
              verificationClass: f.verificationClass || "SCHEDULE_EXTRACTED",
              sourceUrl: doc.url,
              sourceFamily: "SEC_CMBS",
              track: "TRACK_A",
              wave: "COUNTRY_CONCENTRATION",
              commercialList:
                agType === "MANAGEMENT" ? "MANAGEMENT_CONTRACT_TARGETS" : "BRAND_CONVERSION_TARGETS",
              qualificationLevel: hist
                ? "HISTORICAL_CLOSED_SUPERSEDED"
                : "VERIFIED_CONTRACTUAL_RESEARCH_OPPORTUNITY",
              countsAsActiveOpportunity: !hist,
              ownershipRequired: false,
              ownershipStatus: "DEFERRED_OR_CAPTURED_IF_PRESENT",
            };
            row.netNew += 1;
            if (hist) {
              historical.push(opp);
              byCountry[hotelCountry].historicalClosedSuperseded += 1;
              byCountry[hotelCountry].newlyVerifiedFranchiseExpirations +=
                agType === "FRANCHISE" ? 1 : 0;
              byCountry[hotelCountry].newlyVerifiedManagementExpirations +=
                agType === "MANAGEMENT" ? 1 : 0;
            } else {
              datedActive.push(opp);
              byCountry[hotelCountry].verifiedContractualResearchOpportunities += 1;
              byCountry[hotelCountry].distinctEvidenceBackedProspects += 1;
              byCountry[hotelCountry].newlyVerifiedFranchiseExpirations +=
                agType === "FRANCHISE" ? 1 : 0;
              byCountry[hotelCountry].newlyVerifiedManagementExpirations +=
                agType === "MANAGEMENT" ? 1 : 0;
              byCountry[hotelCountry].trackAContribution += 1;
            }
            byCountry[hotelCountry].sourceFamilies.SEC_CMBS =
              (byCountry[hotelCountry].sourceFamilies.SEC_CMBS || 0) + 1;
          }
        }

        // Commercial conversion / operator-change / country disclosure signals
        if (/CONVERSION|TERMINATION|SIGNAL|MX_|BR_|UK_|CA_|DISCLOSURE|ISSUER|MARKET|OPERATOR|REIT/i.test(doc.kind)) {
          const harvest = harvestExhibitDocument(
            { html: text, text },
            { sourceUrl: doc.url },
          );
          const names = extractHotelNames(text, 8);
          const signalBlob = text.slice(0, 60000);
          const isConversion =
            /reflag|re-flag|brand conversion|franchise terminat|non-?renew|operator replace|management compan(?:y|ies) (?:changed|replaced)|new manager|appointed.*manager/i.test(
              signalBlob,
            );
          const agType = /manage/i.test(String(harvest.agreementType || doc.kind))
            ? "MANAGEMENT"
            : "FRANCHISE";
          for (const hotelName of names) {
            const hotelCountry = inferCountryFromQueryAndText(country, text, hotelName);
            if (!PRIORITY_COUNTRIES_V6.includes(hotelCountry) && hotelCountry !== country) {
              // still attribute to hint if unclear
            }
            const cc = PRIORITY_COUNTRIES_V6.includes(hotelCountry) ? hotelCountry : country;
            researchedHotels.add(hotelKey(hotelName, cc));
            byCountry[cc].distinctHotelsResearched += 1;
            const key = hotelKey(hotelName, cc);
            if (priorKeys.has(key)) {
              duplicatesRemoved += 1;
              byCountry[cc].duplicatesRemoved += 1;
              continue;
            }
            priorKeys.add(key);
            const opp = {
              hotelName,
              ...attributeHotelGeography({ hotelCountry: cc, sourceJurisdiction: "US" }),
              agreementType: agType,
              expirationDate: null,
              verificationClass: isConversion
                ? "COMMERCIAL_EVENT_SIGNAL"
                : "HOTEL_NAMED_IN_COUNTRY_DISCLOSURE",
              sourceUrl: doc.url,
              sourceFamily: "EX10_OR_8K_DISCLOSURE",
              track: "TRACK_A",
              wave: "COUNTRY_CONCENTRATION",
              commercialList:
                agType === "MANAGEMENT" ? "MANAGEMENT_CONTRACT_TARGETS" : "BRAND_CONVERSION_TARGETS",
              qualificationLevel: "EVIDENCE_BACKED_COMMERCIAL_PROSPECT",
              countsAsActiveOpportunity: false,
              countsAsEvidenceBackedProspect: true,
              commercialThesis: isConversion
                ? "Hotel named in brand-conversion / operator-change / termination disclosure — evidence-backed prospect; current status requires confirmation"
                : "Hotel named in priority-country hotel franchise/management disclosure — evidence-backed research prospect without verified expiration",
              ownershipRequired: false,
              ownershipStatus: "DEFERRED",
              instrumentClass: harvest.instrumentClass,
              scopeClass: harvest.scopeClass,
            };
            undatedSignals.push(opp);
            byCountry[cc].evidenceBackedCommercialProspectsUndated += 1;
            byCountry[cc].distinctEvidenceBackedProspects += 1;
            byCountry[cc].trackAContribution += 1;
            if (isConversion) {
              conversions.push(opp);
              byCountry[cc].brandConversionAndOperatorChangeSignals += 1;
            }
            byCountry[cc].sourceFamilies.EX10_OR_8K_DISCLOSURE =
              (byCountry[cc].sourceFamilies.EX10_OR_8K_DISCLOSURE || 0) + 1;
            row.netNew += 1;
          }
        }
      } catch (e) {
        row.error = String(e?.message || e).slice(0, 140);
      }
      docResults.push(row);
      await sleep(120);
    }
  }

  // Finalize country metrics
  for (const cc of PRIORITY_COUNTRIES_V6) {
    const row = byCountry[cc];
    // distinct hotels researched was incremented per finding — approximate unique via set filter
    row.yieldPer1000HotelsResearched = computeYieldPer1000(
      row.distinctEvidenceBackedProspects,
      Math.max(row.distinctHotelsResearched, 1),
    );
    row.ambitionProgress = progressTowardAmbition(row.distinctEvidenceBackedProspects, cc);
    row.progressTowardAmbitionMinPct = row.ambitionProgress?.pctOfMin ?? null;
    row.researchCostUsd = 0;
  }

  // Unique hotel researched recount from set
  const hotelsByCountry = Object.fromEntries(PRIORITY_COUNTRIES_V6.map((c) => [c, 0]));
  for (const k of researchedHotels) {
    const cc = k.split("|")[1];
    if (hotelsByCountry[cc] != null) hotelsByCountry[cc] += 1;
  }
  for (const cc of PRIORITY_COUNTRIES_V6) {
    byCountry[cc].distinctHotelsResearched = hotelsByCountry[cc] || 0;
    byCountry[cc].yieldPer1000HotelsResearched = computeYieldPer1000(
      byCountry[cc].distinctEvidenceBackedProspects,
      byCountry[cc].distinctHotelsResearched,
    );
  }

  const baselinePath = path.join(
    ROOT,
    "reports/sec-edgar-hif-v6-industrial-scale/V5_BASELINE_SCORECARD_V6_TAXONOMY.json",
  );
  const wave34Path = path.join(
    ROOT,
    "reports/sec-edgar-hif-v6-track-a-wave34/WAVE34_COMBINED_METRICS.json",
  );
  let priorToward1000 = 194;
  if (fs.existsSync(wave34Path)) {
    priorToward1000 = Number(
      JSON.parse(fs.readFileSync(wave34Path, "utf8"))?.progressToward1000?.current || 194,
    );
  } else if (fs.existsSync(baselinePath)) {
    priorToward1000 = Number(
      JSON.parse(fs.readFileSync(baselinePath, "utf8"))?.metrics?.QUALIFIED_COMMERCIAL_TARGETS || 139,
    );
  }

  const netNewProspects =
    datedActive.length + undatedSignals.filter((s) => s.countsAsEvidenceBackedProspect).length;
  const progress = progressTowardOpportunityMilestone({
    distinctEvidenceBackedOpportunities: priorToward1000 + netNewProspects,
  });

  const metrics = {
    track: "TRACK_A",
    wave: "COUNTRY_CONCENTRATION",
    publicationState: "CUSTOMER_BLOCKED",
    researchCostUsd: 0,
    priorityCountries: PRIORITY_COUNTRIES_V6,
    ambitions: COUNTRY_PROSPECT_AMBITIONS_V6,
    documentsAttempted: docResults.length,
    documentsOk: docResults.filter((d) => d.ok).length,
    netNewVerifiedFranchiseExpirations: datedActive.filter((d) => d.agreementType === "FRANCHISE")
      .length + historical.filter((d) => d.agreementType === "FRANCHISE").length,
    netNewVerifiedManagementExpirations: datedActive.filter((d) => d.agreementType === "MANAGEMENT")
      .length + historical.filter((d) => d.agreementType === "MANAGEMENT").length,
    netNewActiveDatedOpportunities: datedActive.length,
    netNewHistoricalExpirations: historical.length,
    netNewEvidenceBackedUndatedProspects: undatedSignals.length,
    brandConversionOperatorChangeSignals: conversions.length,
    duplicatesRemoved,
    priorToward1000,
    progressToward1000: progress,
    byCountry,
    ownershipEnrichment: "DEFERRED_EXCEPT_INLINE_CAPTURE",
  };

  writeJson(path.join(OUT, "TRACK_A_QUERY_STATS.json"), { queryStats });
  writeJson(path.join(OUT, "TRACK_A_DOC_RESULTS.json"), { docs: docResults });
  writeJson(path.join(OUT, "TRACK_A_DATED_ACTIVE.json"), { count: datedActive.length, hotels: datedActive });
  writeJson(path.join(OUT, "TRACK_A_HISTORICAL.json"), { count: historical.length, hotels: historical });
  writeJson(path.join(OUT, "TRACK_A_UNDATED_PROSPECTS.json"), {
    count: undatedSignals.length,
    hotels: undatedSignals,
  });
  writeJson(path.join(OUT, "TRACK_A_CONVERSION_SIGNALS.json"), {
    count: conversions.length,
    hotels: conversions,
  });
  writeJson(path.join(OUT, "TRACK_A_COUNTRY_SCORECARD.json"), { asOf, byCountry });
  writeJson(path.join(OUT, "TRACK_A_METRICS.json"), metrics);

  ensureWeeklyScorecardLayout(ROOT);
  const global = emptyWeeklyMetrics();
  global.distinctHotelsDiscovered = researchedHotels.size;
  global.newlyDiscoveredHotelsThisWeek = datedActive.length + undatedSignals.length;
  global.verifiedFranchiseExpirations = metrics.netNewVerifiedFranchiseExpirations;
  global.verifiedManagementAgreementExpirations = metrics.netNewVerifiedManagementExpirations;
  global.historicalExpirations = historical.length;
  global.futureExpirations = datedActive.length;
  global.newBrandConversionTargets = [...datedActive, ...undatedSignals].filter(
    (o) => o.commercialList === "BRAND_CONVERSION_TARGETS",
  ).length;
  global.newManagementContractTargets = [...datedActive, ...undatedSignals].filter(
    (o) => o.commercialList === "MANAGEMENT_CONTRACT_TARGETS",
  ).length;
  global.evidenceBackedOpportunityCandidates = netNewProspects;
  global.newlyQualifiedCommercialOpportunities = datedActive.length;
  global.progressToward1000Opportunities = progress.current;
  global.actionableCommercialOpportunities = datedActive.length;
  global.documentsRetrievedAndProcessed = docResults.filter((d) => d.ok).length;
  global.researchCostUsd = 0;
  const snap = buildWeeklySnapshot({
    weekId: `${isoWeekId()}-track-a-country-concentration`,
    asOf,
    global,
    byCountry: Object.fromEntries(
      PRIORITY_COUNTRIES_V6.map((c) => [
        c,
        {
          ...global,
          distinctEvidenceBackedProspects: byCountry[c].distinctEvidenceBackedProspects,
        },
      ]),
    ),
    trackA: global,
    notes: {
      priority: "COUNTRY_CONCENTRATION_OPPORTUNITIES_FIRST",
      countries: PRIORITY_COUNTRIES_V6.join(","),
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
