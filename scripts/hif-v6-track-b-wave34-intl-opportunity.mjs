#!/usr/bin/env node
/**
 * HIF V6 Track B Waves 3–4 — international hotel contractual opportunity discovery.
 * Prioritize CVM/EDINET/SGX/HKEX/SEDAR+ disclosures over Companies House enumeration.
 * CH/HMLR supporting only. CUSTOMER_BLOCKED. $0. No paid access.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
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
for (const p of [
  path.join(ROOT, ".env"),
  "C:/Dev/dealality/my-operators-backend/.env",
]) {
  if (fs.existsSync(p)) dotenv.config({ path: p, override: false });
}

const OUT = path.join(ROOT, "reports/sec-edgar-hif-v6-track-b-wave34");
const RUN = process.argv.includes("--run");
const WAVE = (process.argv.find((a) => a.startsWith("--wave=")) || "--wave=3").split("=")[1];
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

async function fetchText(url, timeoutMs = 35000) {
  const started = Date.now();
  try {
    const res = await fetch(url, {
      headers: {
        "User-Agent": UA,
        Accept: "text/html,application/xhtml+xml,application/pdf,application/json;q=0.9,*/*;q=0.8",
      },
      redirect: "follow",
      signal: AbortSignal.timeout(timeoutMs),
    });
    const text = await res.text();
    return {
      ok: res.ok,
      status: res.status,
      text,
      ms: Date.now() - started,
      finalUrl: res.url,
    };
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

function parseIsoDatesNearContract(text) {
  const out = [];
  const patterns = [
    /(?:expir(?:y|es|ation)|terminat(?:e|ion)|renewal)\s[^.]{0,80}?((?:19|20)\d{2}[-/]\d{1,2}[-/]\d{1,2}|\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+(?:19|20)\d{2}|(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+(?:19|20)\d{2})/gi,
    /(?:franchise|management)\s+agreement[^.]{0,120}?((?:19|20)\d{2})/gi,
  ];
  for (const re of patterns) {
    let m;
    while ((m = re.exec(text))) {
      out.push({ raw: m[1], context: m[0].slice(0, 180) });
      if (out.length >= 20) return out;
    }
  }
  return out;
}

function extractHotelNames(text) {
  const names = new Set();
  const re =
    /\b((?:The\s+)?[A-Z][A-Za-z0-9&'.\-]{1,40}(?:\s+[A-Z][A-Za-z0-9&'.\-]{1,30}){0,4}\s+(?:Hotel|Resort|Inn|Suites))\b/g;
  let m;
  while ((m = re.exec(text))) {
    const n = m[1].replace(/\s+/g, " ").trim();
    if (n.length >= 8 && n.length <= 80 && !/Annual Report|Press Release/i.test(n)) {
      names.add(n);
    }
    if (names.size >= 30) break;
  }
  return [...names];
}

function classifySignal(blob) {
  const t = String(blob || "").toLowerCase();
  if (/management agreement|hotel management|operator agreement|hma\b/.test(t)) {
    return { agreementType: "MANAGEMENT", list: "MANAGEMENT_CONTRACT_TARGETS" };
  }
  if (/franchise|reflag|re-flag|brand conversion|license agreement/.test(t)) {
    return { agreementType: "FRANCHISE", list: "BRAND_CONVERSION_TARGETS" };
  }
  if (/terminat|non-?renew|operator replace|management compan(?:y|ies) replaced/.test(t)) {
    return { agreementType: "OPERATOR_CHANGE", list: "MANAGEMENT_CONTRACT_TARGETS" };
  }
  return { agreementType: "UNCLASSIFIED", list: null };
}

/** Free public filing/search endpoints — hotel-lexicon targeted, not portal homepage pings. */
function portalTargets(wave) {
  const wave3 = [
    {
      id: "SEDAR_PLUS_HOTEL_FRANCHISE",
      country: "CA",
      sourceJurisdiction: "CA",
      sourceFamily: "INTERNATIONAL_SECURITIES_REGULATORS",
      url: "https://www.sedarplus.ca/csa-party/records/search.html?search=franchise%20hotel%20agreement",
      note: "SEDAR+ public search UI — may require browser; capture reachability + lexicon",
    },
    {
      id: "SEDAR_LEGACY_HOST_HOTELS",
      country: "CA",
      sourceJurisdiction: "CA",
      sourceFamily: "INTERNATIONAL_SECURITIES_REGULATORS",
      url: "https://www.sedar.com/search/search_form_pc_en.htm",
    },
    {
      id: "SGX_HOTEL_ANNOUNCEMENTS",
      country: "SG",
      sourceJurisdiction: "SG",
      sourceFamily: "INTERNATIONAL_SECURITIES_REGULATORS",
      url: "https://www.sgx.com/securities/company-announcements",
    },
    {
      id: "HKEX_NEWS_HOTEL",
      country: "HK",
      sourceJurisdiction: "HK",
      sourceFamily: "INTERNATIONAL_SECURITIES_REGULATORS",
      url: "https://www1.hkexnews.hk/search/titlesearch.xhtml?lang=en",
    },
    {
      id: "CVM_EMPRESA_NET",
      country: "BR",
      sourceJurisdiction: "BR",
      sourceFamily: "INTERNATIONAL_SECURITIES_REGULATORS",
      url: "https://www.rad.cvm.gov.br/ENET/frmConsultaExternaCVM.aspx",
    },
    {
      id: "EDINET_SEARCH",
      country: "JP",
      sourceJurisdiction: "JP",
      sourceFamily: "INTERNATIONAL_SECURITIES_REGULATORS",
      url: "https://disclosure.edinet-fsa.go.jp/",
    },
    // Known free Canadian hotel REIT / lodging issuer pages with contractual language often in MD&A
    {
      id: "HOST_HOTELS_SEDAR_PROXY_SEC",
      country: "US",
      physicalHotelGeoNote: "CA_AND_US_PORTFOLIO — attribute physical hotels when named",
      sourceJurisdiction: "US",
      sourceFamily: "SEC_CMBS",
      url: "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0001070752&type=10-K&dateb=&owner=include&count=5",
    },
    {
      id: "AHIP_REIT_SEDARISH_SEC",
      country: "US",
      sourceJurisdiction: "US",
      sourceFamily: "EX10",
      url: "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0001658566&type=EX-10&dateb=&owner=include&count=20",
    },
  ];
  const wave4 = [
    {
      id: "PARK_HOTELS_EX10",
      country: "US",
      sourceJurisdiction: "US",
      sourceFamily: "EX10",
      url: "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0001613092&type=EX-10&dateb=&owner=include&count=20",
    },
    {
      id: "SUNSTONE_EX10",
      country: "US",
      sourceJurisdiction: "US",
      sourceFamily: "EX10",
      url: "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0001288942&type=EX-10&dateb=&owner=include&count=20",
    },
    {
      id: "RLJ_EX10",
      country: "US",
      sourceJurisdiction: "US",
      sourceFamily: "EX10",
      url: "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0001511337&type=EX-10&dateb=&owner=include&count=20",
    },
    {
      id: "APPLE_HOSPITALITY_EX10",
      country: "US",
      sourceJurisdiction: "US",
      sourceFamily: "EX10",
      url: "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0001418121&type=EX-10&dateb=&owner=include&count=20",
    },
    {
      id: "HKEX_SHANGRI_LA",
      country: "HK",
      sourceJurisdiction: "HK",
      sourceFamily: "INTERNATIONAL_SECURITIES_REGULATORS",
      url: "https://www1.hkexnews.hk/listedco/listconews/sehk/2024/0321/2024032100465.pdf",
    },
    {
      id: "SGX_CDL_HOSPITALITY",
      country: "SG",
      sourceJurisdiction: "SG",
      sourceFamily: "INTERNATIONAL_SECURITIES_REGULATORS",
      url: "https://links.sgx.com/1.0.0/corporate-announcements/",
    },
  ];
  return wave === "4" ? wave4 : wave3;
}

function extractExhibitLinks(html) {
  const links = [];
  const re = /href="([^"]+)"/gi;
  let m;
  while ((m = re.exec(html))) {
    const href = m[1].replace(/&amp;/g, "&");
    if (/javascript:|#|/i.test(href) && href.length < 3) continue;
    // Company filing index rows + documents
    const isDoc =
      /\/Archives\/edgar\/data\/\d+\//i.test(href) ||
      /\.htm[l]?$/i.test(href) ||
      /ex[-_]?10/i.test(href) ||
      /dex10/i.test(href);
    if (!isDoc) continue;
    let url = href;
    if (href.startsWith("/")) url = `https://www.sec.gov${href}`;
    else if (href.startsWith("Archives/")) url = `https://www.sec.gov/${href}`;
    else if (!/^https?:/i.test(href)) continue;
    // Prefer document index or exhibit files
    if (/index\.htm|ex-?10|dex10|\.htm/i.test(url)) links.push(url.split("#")[0]);
    if (links.length >= 40) break;
  }
  return [...new Set(links)];
}

async function resolveExhibitDocuments(indexUrl, limit = 8) {
  const index = await fetchText(indexUrl);
  if (!index.ok) return { index, exhibits: [] };
  // First hop: filing indexes from company browse page
  const filingIndexes = extractExhibitLinks(index.text)
    .filter((u) => /index\.htm/i.test(u) || /\/\d{18}\//.test(u))
    .slice(0, 6);
  const exhibitUrls = new Set();
  for (const fi of filingIndexes) {
    const page = await fetchText(fi);
    if (!page.ok) continue;
    for (const u of extractExhibitLinks(page.text)) {
      if (/ex-?10|dex10|exhibit/i.test(u) || /\.htm[l]?$/i.test(u)) {
        if (!/index\.htm/i.test(u)) exhibitUrls.add(u);
      }
    }
    await sleep(150);
    if (exhibitUrls.size >= limit) break;
  }
  // Fallback: direct links already on browse page
  if (exhibitUrls.size === 0) {
    for (const u of extractExhibitLinks(index.text)) {
      if (/ex-?10|dex10/i.test(u)) exhibitUrls.add(u);
    }
  }
  const exhibits = [];
  for (const url of [...exhibitUrls].slice(0, limit)) {
    const doc = await fetchText(url);
    exhibits.push({
      url,
      ok: doc.ok,
      status: doc.status,
      bytes: doc.text.length,
      hotelNames: doc.ok ? extractHotelNames(doc.text).slice(0, 15) : [],
      dateHints: doc.ok ? parseIsoDatesNearContract(doc.text).slice(0, 10) : [],
      signal: doc.ok ? classifySignal(doc.text.slice(0, 50000)) : null,
      hasFranchiseLexicon: doc.ok && /franchise/i.test(doc.text),
      hasManagementLexicon: doc.ok && /management agreement|hotel management/i.test(doc.text),
    });
    await sleep(200);
  }
  return { index, exhibits };
}

async function deepenSecExhibits(indexUrl, limit = 6) {
  const index = await fetchText(indexUrl);
  if (!index.ok) return { index, exhibits: [] };
  const links = extractExhibitLinks(index.text).slice(0, limit);
  const exhibits = [];
  for (const url of links) {
    const doc = await fetchText(url);
    exhibits.push({
      url,
      ok: doc.ok,
      status: doc.status,
      bytes: doc.text.length,
      hotelNames: doc.ok ? extractHotelNames(doc.text).slice(0, 15) : [],
      dateHints: doc.ok ? parseIsoDatesNearContract(doc.text).slice(0, 10) : [],
      signal: doc.ok ? classifySignal(doc.text.slice(0, 50000)) : null,
      hasFranchiseLexicon: doc.ok && /franchise/i.test(doc.text),
      hasManagementLexicon: doc.ok && /management agreement|hotel management/i.test(doc.text),
    });
    await sleep(200);
  }
  return { index, exhibits };
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  if (!RUN) {
    console.log(
      JSON.stringify(
        {
          ok: true,
          dryRun: true,
          wave: WAVE,
          targets: portalTargets(WAVE).length,
          chKeyPresent: Boolean(process.env.COMPANIES_HOUSE_API_KEY),
        },
        null,
        2,
      ),
    );
    return;
  }

  const asOf = new Date().toISOString().slice(0, 10);
  const targets = portalTargets(WAVE);
  const probes = [];
  const opportunities = [];
  const candidates = [];
  const seen = new Set();

  for (const t of targets) {
    const probe = {
      ...t,
      retrieved: false,
      status: null,
      error: null,
      hotelNamesFound: [],
      dateHints: [],
      exhibitDeepens: 0,
      opportunitiesExtracted: 0,
    };
    try {
      if (/sec\.gov\/cgi-bin\/browse-edgar/i.test(t.url)) {
        const deep = await resolveExhibitDocuments(t.url, WAVE === "4" ? 10 : 6);
        probe.retrieved = deep.index.ok;
        probe.status = deep.index.status;
        probe.exhibitDeepens = deep.exhibits.length;
        for (const ex of deep.exhibits) {
          if (!ex.ok) continue;
          for (const name of ex.hotelNames) {
            const key = hotelKey(name, t.country || "US");
            if (seen.has(key)) continue;
            seen.add(key);
            const hasDate = (ex.dateHints || []).length > 0;
            const signal = ex.signal || classifySignal(`${ex.hasFranchiseLexicon} ${ex.hasManagementLexicon}`);
            // Hotel name in EX-10 without property-level expiration is a research candidate / signal, not verified expiration
            const row = {
              hotelName: name,
              ...attributeHotelGeography({
                hotelCountry: t.country || "US",
                sourceJurisdiction: t.sourceJurisdiction,
              }),
              agreementType: signal.agreementType,
              expirationDate: null,
              verificationClass: hasDate
                ? "DATE_HINT_UNVERIFIED"
                : "HOTEL_NAMED_IN_CONTRACT_DISCLOSURE",
              sourceUrl: ex.url,
              sourceFamily: t.sourceFamily,
              track: "TRACK_B",
              wave: `WAVE${WAVE}`,
              commercialList: signal.list,
              trackingBucket: hasDate
                ? "RESEARCH_CANDIDATES"
                : "EVIDENCE_BACKED_COMMERCIAL_SIGNALS_WITHOUT_DATES",
              commercialThesis: hasDate
                ? "Hotel named near contractual date language — requires schedule/term verification before counting as verified expiration"
                : "Hotel named in franchise/management-related exhibit — evidence-backed signal, date not yet verified",
              countsAsActiveOpportunity: false,
              countsAsEvidenceBackedSignal: Boolean(signal.list),
              ownershipRequired: false,
              ownershipStatus: "DEFERRED",
              dateHints: ex.dateHints.slice(0, 3),
            };
            if (signal.list) {
              candidates.push(row);
              probe.opportunitiesExtracted += 1;
            }
          }
        }
      } else {
        const res = await fetchText(t.url);
        probe.retrieved = res.ok;
        probe.status = res.status;
        probe.error = res.error || null;
        if (res.ok) {
          probe.hotelNamesFound = extractHotelNames(res.text).slice(0, 20);
          probe.dateHints = parseIsoDatesNearContract(res.text).slice(0, 10);
          const signal = classifySignal(res.text.slice(0, 80000));
          for (const name of probe.hotelNamesFound.slice(0, 10)) {
            const key = hotelKey(name, t.country);
            if (seen.has(key)) continue;
            seen.add(key);
            candidates.push({
              hotelName: name,
              ...attributeHotelGeography({
                hotelCountry: t.country,
                sourceJurisdiction: t.sourceJurisdiction,
              }),
              agreementType: signal.agreementType,
              expirationDate: null,
              verificationClass: "PORTAL_LEXICON_CANDIDATE",
              sourceUrl: t.url,
              sourceFamily: t.sourceFamily,
              track: "TRACK_B",
              wave: `WAVE${WAVE}`,
              commercialList: signal.list,
              trackingBucket: "RESEARCH_CANDIDATES",
              commercialThesis:
                "Hotel-name lexicon hit on international/regulatory portal page — not yet a verified contractual opportunity",
              countsAsActiveOpportunity: false,
              countsAsEvidenceBackedSignal: false,
              ownershipRequired: false,
              ownershipStatus: "DEFERRED",
            });
            probe.opportunitiesExtracted += 1;
          }
        }
      }
    } catch (e) {
      probe.error = String(e?.message || e).slice(0, 160);
    }
    probes.push(probe);
    await sleep(250);
  }

  // Only EX-10 hotel+franchise/management named disclosures count as evidence-backed signals (not portal homepage lexicon)
  const evidenceSignals = candidates.filter((c) => c.countsAsEvidenceBackedSignal);
  const verifiedExpirations = opportunities.filter((o) => o.expirationDate && o.countsAsActiveOpportunity);

  const inventoryPath = path.join(OUT, "OPPORTUNITY_INVENTORY.json");
  let inventory = { activeOpportunities: [], evidenceSignals: [], researchCandidates: [] };
  if (fs.existsSync(inventoryPath)) {
    inventory = JSON.parse(fs.readFileSync(inventoryPath, "utf8"));
  }
  const invKeys = new Set(
    [
      ...(inventory.activeOpportunities || []),
      ...(inventory.evidenceSignals || []),
    ].map((o) => hotelKey(o.hotelName, o.hotelCountry)),
  );
  for (const s of evidenceSignals) {
    const k = hotelKey(s.hotelName, s.hotelCountry);
    if (invKeys.has(k)) continue;
    invKeys.add(k);
    inventory.evidenceSignals.push(s);
  }
  inventory.researchCandidates = [
    ...(inventory.researchCandidates || []),
    ...candidates.filter((c) => !c.countsAsEvidenceBackedSignal),
  ].slice(0, 300);

  const metrics = {
    track: "TRACK_B",
    waves: `WAVE${WAVE}`,
    priority: "OPPORTUNITIES_FIRST_INTL_DISCLOSURES",
    publicationState: "CUSTOMER_BLOCKED",
    researchCostUsd: 0,
    companiesHouseKeyPresent: Boolean(process.env.COMPANIES_HOUSE_API_KEY),
    companiesHouseRole: "SUPPORTING_VERIFICATION_ONLY_NOT_VOLUME_TARGET",
    hmlrRole: "SUPPORTING_VERIFICATION_ONLY_NOT_VOLUME_TARGET",
    portalsOrIndexesProbed: probes.length,
    portalsRetrievedOk: probes.filter((p) => p.retrieved).length,
    exhibitDocumentsReviewed: probes.reduce((n, p) => n + (p.exhibitDeepens || 0), 0),
    netNewVerifiedFranchiseExpirations: 0,
    netNewVerifiedManagementExpirations: 0,
    evidenceBackedConversionOrOperatorSignals: evidenceSignals.length,
    researchCandidatesNotCountedAsOpportunities: candidates.filter((c) => !c.countsAsEvidenceBackedSignal)
      .length,
    inventoryEvidenceSignals: (inventory.evidenceSignals || []).length,
    inventoryActiveOpportunities: (inventory.activeOpportunities || []).length,
    progressToward1000Note:
      "Track B contributes verified expirations only when hotel-specific contract dates are independently verified; named-hotel EX-10 signals tracked separately",
    ownershipEnrichment: "DEFERRED",
    milestone: OPPORTUNITY_MILESTONE_V6,
  };

  writeJson(path.join(OUT, `WAVE${WAVE}_PROBES.json`), { probes });
  writeJson(path.join(OUT, `WAVE${WAVE}_CANDIDATES.json`), {
    evidenceSignals,
    researchCandidates: candidates.filter((c) => !c.countsAsEvidenceBackedSignal),
  });
  writeJson(inventoryPath, {
    ...inventory,
    updatedAt: new Date().toISOString(),
  });
  writeJson(path.join(OUT, `WAVE${WAVE}_METRICS.json`), metrics);

  ensureWeeklyScorecardLayout(ROOT);
  const global = emptyWeeklyMetrics();
  global.distinctHotelsDiscovered = evidenceSignals.length;
  global.researchCandidates = candidates.length;
  global.evidenceBackedOpportunityCandidates = evidenceSignals.length;
  global.documentsRetrievedAndProcessed = probes.reduce(
    (n, p) => n + (p.exhibitDeepens || (p.retrieved ? 1 : 0)),
    0,
  );
  global.researchCostUsd = 0;
  const snap = buildWeeklySnapshot({
    weekId: `${isoWeekId()}-track-b-wave${WAVE}`,
    asOf,
    global,
    trackB: global,
    bySourceFamily: {
      INTERNATIONAL_SECURITIES_REGULATORS: {
        independentlyVerifiedIncrementalFindings: 0,
        documentsProcessed: global.documentsRetrievedAndProcessed,
        costUsd: 0,
        trackAFindings: 0,
        trackBFindings: evidenceSignals.length,
        duplicateFindings: 0,
        holdOrInvalidFindings: candidates.filter((c) => !c.countsAsEvidenceBackedSignal).length,
      },
      EX10: {
        independentlyVerifiedIncrementalFindings: 0,
        documentsProcessed: probes
          .filter((p) => /EX10|sec\.gov/i.test(p.sourceFamily || p.id || ""))
          .reduce((n, p) => n + (p.exhibitDeepens || 0), 0),
        costUsd: 0,
        trackBFindings: evidenceSignals.length,
      },
    },
    notes: {
      priority: "OPPORTUNITIES_FIRST_INTL",
      chHmlr: "SUPPORTING_ONLY",
      wave: WAVE,
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
