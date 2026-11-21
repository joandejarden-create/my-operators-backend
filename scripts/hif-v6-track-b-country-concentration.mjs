#!/usr/bin/env node
/**
 * HIF V6 Track B — Five-country regulatory / official-source opportunity research.
 * Free public sources. CH/HMLR supporting only. CUSTOMER_BLOCKED. $0.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import {
  attributeHotelGeography,
  buildWeeklySnapshot,
  emptyWeeklyMetrics,
  ensureWeeklyScorecardLayout,
  isoWeekId,
  saveWeeklySnapshot,
} from "../lib/hotel-intelligence/sec-edgar/hif-weekly-regional-yield-scorecard-v6.js";
import {
  PRIORITY_COUNTRIES_V6,
  emptyCountryScorecardRow,
} from "../lib/hotel-intelligence/sec-edgar/hif-v6-country-concentration-v6.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) dotenv.config({ path: p, override: false });
}

const OUT = path.join(ROOT, "reports/sec-edgar-hif-v6-country-concentration");
const RUN = process.argv.includes("--run");
const UA =
  process.env.SEC_EDGAR_USER_AGENT ||
  "Dealality HIF Research joan@aohospitalityadvisors.com";
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
  return /^(the\s+)?(particular|per|interim|borrower|form of|each|any|such)\b/i.test(n);
}

function extractHotelNames(text, limit = 12) {
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

function classifySignal(text) {
  const t = String(text || "").toLowerCase();
  if (/reflag|brand conversion|franchise terminat|non-?renew/.test(t)) {
    return { agreementType: "FRANCHISE", list: "BRAND_CONVERSION_TARGETS", conversion: true };
  }
  if (/management agreement|hotel management|operator (?:appoint|replace|change)/.test(t)) {
    return { agreementType: "MANAGEMENT", list: "MANAGEMENT_CONTRACT_TARGETS", conversion: false };
  }
  if (/franchise|license agreement/.test(t)) {
    return { agreementType: "FRANCHISE", list: "BRAND_CONVERSION_TARGETS", conversion: false };
  }
  return { agreementType: "UNCLASSIFIED", list: null, conversion: false };
}

async function fetchText(url, timeoutMs = 35000) {
  const started = Date.now();
  try {
    const res = await fetch(url, {
      headers: {
        "User-Agent": UA,
        Accept: "text/html,application/json,application/pdf;q=0.9,*/*;q=0.8",
      },
      redirect: "follow",
      signal: AbortSignal.timeout(timeoutMs),
    });
    const text = await res.text();
    return { ok: res.ok, status: res.status, text, ms: Date.now() - started, url: res.url };
  } catch (e) {
    return {
      ok: false,
      status: null,
      text: "",
      ms: Date.now() - started,
      error: String(e?.message || e),
      errorClass: "NETWORK_OR_TIMEOUT",
    };
  }
}

/** Official / regulatory targets by priority country — document-level where possible. */
function countryTargets() {
  return [
    // US
    {
      country: "US",
      id: "SEC_EDGAR_COMPANY_SEARCH_SHELL",
      sourceFamily: "SEC_EDGAR",
      url: "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0000945394&type=EX-10&dateb=&owner=include&count=20",
      note: "Service Properties Trust EX-10 lodging agreements",
    },
    {
      country: "US",
      id: "SEC_HOST_HOTELS_EX10",
      sourceFamily: "SEC_EDGAR",
      url: "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0001070752&type=EX-10&dateb=&owner=include&count=15",
    },
    // Mexico
    {
      country: "MX",
      id: "BMV_PORTAL",
      sourceFamily: "BMV_EMISNET",
      url: "https://www.bmv.com.mx/es/Grupo_BMV/Informacion_de_emisora",
    },
    {
      country: "MX",
      id: "SEC_FIBRA_INN_CROSSBORDER",
      sourceFamily: "SEC_CROSSBORDER",
      url: "https://efts.sec.gov/LATEST/search-index?q=%22Fibra%20Inn%22%20hotel&dateRange=custom&startdt=2016-01-01&enddt=2026-11-21&from=0&size=10",
    },
    {
      country: "MX",
      id: "SEC_MEXICO_HOTEL_MGMT",
      sourceFamily: "SEC_CROSSBORDER",
      url: "https://efts.sec.gov/LATEST/search-index?q=Mexico%20%22management%20agreement%22%20hotel&forms=6-K&dateRange=custom&startdt=2016-01-01&enddt=2026-11-21&from=0&size=15",
    },
    // Brazil
    {
      country: "BR",
      id: "CVM_ENET",
      sourceFamily: "CVM",
      url: "https://www.rad.cvm.gov.br/ENET/frmConsultaExternaCVM.aspx",
    },
    {
      country: "BR",
      id: "CVM_DADOS_ABERTOS",
      sourceFamily: "CVM",
      url: "https://dados.cvm.gov.br/dataset",
    },
    {
      country: "BR",
      id: "SEC_BRAZIL_HOTEL_6K",
      sourceFamily: "SEC_CROSSBORDER",
      url: "https://efts.sec.gov/LATEST/search-index?q=Brazil%20hotel%20%22management%20agreement%22%20OR%20franchise&forms=6-K&dateRange=custom&startdt=2016-01-01&enddt=2026-11-21&from=0&size=15",
    },
    // UK
    {
      country: "GB",
      id: "COMPANIES_HOUSE_SEARCH_UI",
      sourceFamily: "COMPANIES_HOUSE",
      url: "https://find-and-update.company-information.service.gov.uk/search?q=hotel+management",
    },
    {
      country: "GB",
      id: "SEC_UK_HOTEL_MGMT",
      sourceFamily: "SEC_CROSSBORDER",
      url: "https://efts.sec.gov/LATEST/search-index?q=%22United%20Kingdom%22%20OR%20London%20%22hotel%20management%20agreement%22&dateRange=custom&startdt=2016-01-01&enddt=2026-11-21&from=0&size=15",
    },
    {
      country: "GB",
      id: "ULPD_DATASETS_META",
      sourceFamily: "HM_LAND_REGISTRY_ULPD",
      url: "https://use-land-property-data.service.gov.uk/api/v1/datasets",
      authEnv: "HM_LAND_REGISTRY_KEY",
    },
    // Canada
    {
      country: "CA",
      id: "SEDAR_PLUS",
      sourceFamily: "SEDAR_PLUS",
      url: "https://www.sedarplus.ca/",
    },
    {
      country: "CA",
      id: "CORPORATIONS_CANADA_API_META",
      sourceFamily: "CORPORATIONS_CANADA",
      url: "https://ised-isde.canada.ca/cc/lgcy/fdrlCrpSrch.html",
    },
    {
      country: "CA",
      id: "SEC_CA_HOTEL_REIT",
      sourceFamily: "SEC_CROSSBORDER",
      url: "https://efts.sec.gov/LATEST/search-index?q=Canada%20hotel%20%22management%20agreement%22%20OR%20franchise%20OR%20reflag&dateRange=custom&startdt=2016-01-01&enddt=2026-11-21&from=0&size=15",
    },
    {
      country: "CA",
      id: "QUEBEC_TOURISM_ESTABLISHMENTS",
      sourceFamily: "QC_HOTEL_REGISTER",
      url: "https://www.quebec.ca/tourisme-et-loisirs/services-informations-touristes/hebergement",
    },
  ];
}

function loadEnvKey(name) {
  const merged = {
    ...dotenv.config({ path: "C:/Dev/dealality/my-operators-backend/.env", override: false }).parsed,
    ...dotenv.config({ path: path.join(ROOT, ".env"), override: false }).parsed,
  };
  return process.env[name] || merged?.[name] || "";
}

async function deepenEftsHits(eftsJsonText, country, limit = 6) {
  let j;
  try {
    j = JSON.parse(eftsJsonText);
  } catch {
    return [];
  }
  const hits = (j.hits?.hits || []).slice(0, limit);
  const out = [];
  for (const h of hits) {
    const src = h._source || {};
    const adsh = String(src.adsh || "").replace(/-/g, "");
    const cik = String((src.ciks || [])[0] || "").replace(/^0+/, "");
    const fileName = String(h._id || "").split(":").pop();
    if (!cik || !adsh || !fileName) continue;
    const url = `https://www.sec.gov/Archives/edgar/data/${cik}/${adsh}/${fileName}`;
    const doc = await fetchText(url);
    out.push({
      country,
      url,
      ok: doc.ok,
      status: doc.status,
      hotelNames: doc.ok ? extractHotelNames(doc.text, 8) : [],
      signal: doc.ok ? classifySignal(doc.text.slice(0, 80000)) : null,
      bytes: doc.text.length,
    });
    await sleep(150);
  }
  return out;
}

async function deepenSecBrowse(indexUrl, country, limit = 5) {
  const index = await fetchText(indexUrl);
  if (!index.ok) return { index, exhibits: [] };
  const links = [...index.text.matchAll(/href="([^"]+)"/gi)]
    .map((m) => m[1].replace(/&amp;/g, "&"))
    .filter((h) => /Archives\/edgar\/data/i.test(h) || /index\.htm/i.test(h))
    .slice(0, 8);
  const exhibits = [];
  for (const href of links) {
    const url = href.startsWith("http") ? href : `https://www.sec.gov${href}`;
    const page = await fetchText(url);
    if (!page.ok) continue;
    // if index page, dive one more level for ex-10
    const exLinks = [...page.text.matchAll(/href="([^"]+)"/gi)]
      .map((m) => m[1].replace(/&amp;/g, "&"))
      .filter((h) => /ex-?10|dex10|\.htm/i.test(h) && !/index\.htm/i.test(h))
      .slice(0, 4);
    for (const eh of exLinks) {
      const eurl = eh.startsWith("http") ? eh : `https://www.sec.gov${eh}`;
      const doc = await fetchText(eurl);
      exhibits.push({
        country,
        url: eurl,
        ok: doc.ok,
        status: doc.status,
        hotelNames: doc.ok ? extractHotelNames(doc.text, 8) : [],
        signal: doc.ok ? classifySignal(doc.text.slice(0, 80000)) : null,
        bytes: doc.text?.length || 0,
      });
      await sleep(150);
      if (exhibits.length >= limit) break;
    }
    if (exhibits.length >= limit) break;
    await sleep(120);
  }
  return { index, exhibits };
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const chKey = Boolean(loadEnvKey("COMPANIES_HOUSE_API_KEY"));
  const hmlrKey = loadEnvKey("HM_LAND_REGISTRY_KEY");
  if (!RUN) {
    console.log(
      JSON.stringify(
        {
          ok: true,
          dryRun: true,
          countries: PRIORITY_COUNTRIES_V6,
          chKeyPresent: chKey,
          hmlrKeyPresent: Boolean(hmlrKey),
        },
        null,
        2,
      ),
    );
    return;
  }

  const byCountry = Object.fromEntries(PRIORITY_COUNTRIES_V6.map((c) => [c, emptyCountryScorecardRow(c)]));
  const probes = [];
  const prospects = [];
  const candidates = [];
  const seen = new Set();
  let verifiedFranchise = 0;
  let verifiedManagement = 0;

  for (const t of countryTargets()) {
    const probe = {
      ...t,
      retrieved: false,
      status: null,
      error: null,
      deepenCount: 0,
      prospectsExtracted: 0,
      authOutcome: null,
    };
    try {
      if (t.authEnv === "HM_LAND_REGISTRY_KEY") {
        if (!hmlrKey) {
          probe.authOutcome = "KEY_ABSENT";
          probe.error = "HM_LAND_REGISTRY_KEY missing in this process";
          probes.push(probe);
          continue;
        }
        const res = await fetch(t.url, {
          headers: { Authorization: hmlrKey, Accept: "application/json", "User-Agent": UA },
        });
        const text = await res.text();
        probe.retrieved = res.ok;
        probe.status = res.status;
        probe.authOutcome = res.ok ? "AUTHENTICATED_OK" : `HTTP_${res.status}`;
        // Supporting only — do not invent hotel ownership prospects from dataset list
        byCountry[t.country].filingsOrDatasetsProbed += 1;
        if (res.ok) byCountry[t.country].portalsRetrievedOk += 1;
        probes.push(probe);
        continue;
      }

      const res = await fetchText(t.url);
      probe.retrieved = res.ok;
      probe.status = res.status;
      probe.error = res.error || null;
      byCountry[t.country].filingsOrDatasetsProbed += 1;
      if (res.ok) byCountry[t.country].portalsRetrievedOk += 1;

      let deepDocs = [];
      if (res.ok && /efts\.sec\.gov/i.test(t.url)) {
        deepDocs = await deepenEftsHits(res.text, t.country, 6);
      } else if (res.ok && /browse-edgar/i.test(t.url)) {
        const deep = await deepenSecBrowse(t.url, t.country, 5);
        deepDocs = deep.exhibits || [];
      } else if (res.ok) {
        // Portal page lexicon — research candidates only unless hotel+signal strong
        const names = extractHotelNames(res.text, 6);
        const signal = classifySignal(res.text.slice(0, 50000));
        for (const name of names) {
          const key = hotelKey(name, t.country);
          if (seen.has(key)) continue;
          seen.add(key);
          candidates.push({
            hotelName: name,
            ...attributeHotelGeography({
              hotelCountry: t.country,
              sourceJurisdiction: t.country,
            }),
            agreementType: signal.agreementType,
            expirationDate: null,
            verificationClass: "PORTAL_LEXICON_CANDIDATE",
            sourceUrl: t.url,
            sourceFamily: t.sourceFamily,
            track: "TRACK_B",
            wave: "COUNTRY_CONCENTRATION",
            commercialList: signal.list,
            qualificationLevel: "HOLD_RESEARCH_CANDIDATE",
            countsAsEvidenceBackedProspect: false,
            commercialThesis:
              "Hotel-name lexicon on regulatory/portal page — not yet a verified contractual opportunity",
            ownershipRequired: false,
            ownershipStatus: "DEFERRED",
          });
          byCountry[t.country].holdResearchCandidates += 1;
        }
      }

      probe.deepenCount = deepDocs.length;
      for (const d of deepDocs) {
        if (!d.ok) continue;
        const signal = d.signal || classifySignal("");
        for (const name of d.hotelNames || []) {
          const key = hotelKey(name, t.country);
          if (seen.has(key)) {
            byCountry[t.country].duplicatesRemoved += 1;
            continue;
          }
          seen.add(key);
          const isProspect = Boolean(signal.list);
          const row = {
            hotelName: name,
            ...attributeHotelGeography({
              hotelCountry: t.country,
              sourceJurisdiction: "US",
            }),
            agreementType: signal.agreementType,
            expirationDate: null,
            verificationClass: "HOTEL_NAMED_IN_REGULATORY_OR_CROSSBORDER_DISCLOSURE",
            sourceUrl: d.url,
            sourceFamily: t.sourceFamily,
            track: "TRACK_B",
            wave: "COUNTRY_CONCENTRATION",
            commercialList: signal.list,
            qualificationLevel: isProspect
              ? "EVIDENCE_BACKED_COMMERCIAL_PROSPECT"
              : "HOLD_RESEARCH_CANDIDATE",
            countsAsEvidenceBackedProspect: isProspect,
            commercialThesis: isProspect
              ? "Hotel named in priority-country regulatory/cross-border franchise or management disclosure — undated evidence-backed prospect"
              : "Hotel named without clear franchise/management classification",
            ownershipRequired: false,
            ownershipStatus: "DEFERRED",
          };
          if (isProspect) {
            prospects.push(row);
            probe.prospectsExtracted += 1;
            byCountry[t.country].evidenceBackedCommercialProspectsUndated += 1;
            byCountry[t.country].distinctEvidenceBackedProspects += 1;
            byCountry[t.country].trackBContribution += 1;
            byCountry[t.country].distinctHotelsResearched += 1;
            if (signal.conversion) {
              byCountry[t.country].brandConversionAndOperatorChangeSignals += 1;
            }
            byCountry[t.country].sourceFamilies[t.sourceFamily] =
              (byCountry[t.country].sourceFamilies[t.sourceFamily] || 0) + 1;
          } else {
            candidates.push(row);
            byCountry[t.country].holdResearchCandidates += 1;
          }
        }
      }
    } catch (e) {
      probe.error = String(e?.message || e).slice(0, 160);
    }
    probes.push(probe);
    await sleep(200);
  }

  const metrics = {
    track: "TRACK_B",
    wave: "COUNTRY_CONCENTRATION",
    publicationState: "CUSTOMER_BLOCKED",
    researchCostUsd: 0,
    companiesHouseApiKeyPresent: chKey,
    hmLandRegistryKeyPresent: Boolean(hmlrKey),
    companiesHouseRole: "SUPPORTING_VERIFICATION_ONLY",
    hmLandRegistryRole: "SUPPORTING_VERIFICATION_ONLY_NO_PAID_TITLES",
    portalsOrDatasetsProbed: probes.length,
    portalsRetrievedOk: probes.filter((p) => p.retrieved).length,
    netNewVerifiedFranchiseExpirations: verifiedFranchise,
    netNewVerifiedManagementExpirations: verifiedManagement,
    evidenceBackedUndatedProspects: prospects.length,
    holdResearchCandidates: candidates.length,
    byCountry,
    ownershipEnrichment: "DEFERRED",
  };

  writeJson(path.join(OUT, "TRACK_B_PROBES.json"), { probes });
  writeJson(path.join(OUT, "TRACK_B_PROSPECTS.json"), { count: prospects.length, hotels: prospects });
  writeJson(path.join(OUT, "TRACK_B_CANDIDATES.json"), {
    count: candidates.length,
    hotels: candidates.slice(0, 200),
  });
  writeJson(path.join(OUT, "TRACK_B_COUNTRY_SCORECARD.json"), {
    asOf: new Date().toISOString().slice(0, 10),
    byCountry,
  });
  writeJson(path.join(OUT, "TRACK_B_METRICS.json"), metrics);

  ensureWeeklyScorecardLayout(ROOT);
  const global = emptyWeeklyMetrics();
  global.distinctHotelsDiscovered = prospects.length;
  global.evidenceBackedOpportunityCandidates = prospects.length;
  global.researchCandidates = candidates.length;
  global.documentsRetrievedAndProcessed = probes.reduce(
    (n, p) => n + (p.deepenCount || (p.retrieved ? 1 : 0)),
    0,
  );
  global.researchCostUsd = 0;
  const snap = buildWeeklySnapshot({
    weekId: `${isoWeekId()}-track-b-country-concentration`,
    asOf: new Date().toISOString().slice(0, 10),
    global,
    trackB: global,
    notes: { priority: "COUNTRY_CONCENTRATION", chHmlr: "SUPPORTING_ONLY" },
    allowOverwrite: true,
  });
  saveWeeklySnapshot(snap, ROOT);

  console.log(JSON.stringify(metrics, null, 2));
}

main().catch((e) => {
  console.error(String(e?.stack || e));
  process.exit(1);
});
