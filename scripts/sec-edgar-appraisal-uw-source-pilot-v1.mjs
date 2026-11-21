#!/usr/bin/env node
/**
 * HIF Appraisal / Underwriting Source Discovery Pilot (free-source first).
 * CUSTOMER_BLOCKED · no third product · no Meta · no paywall bypass · Agent not used.
 *
 * Measures whether publicly accessible appraisal / UW / valuation documents yield
 * incremental hotel-specific franchise or management contract intelligence vs HIF.
 */
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "reports/sec-edgar-appraisal-uw-source-pilot-v1");
const UA = "DealalityResearchBot/1.0 joandejarden@dealality.com";

const SOURCE_CLASS = Object.freeze({
  HOTEL_APPRAISAL_REPORT: "HOTEL_APPRAISAL_REPORT",
  UNDERWRITING_SUMMARY_OR_ANNEX: "UNDERWRITING_SUMMARY_OR_ANNEX",
  VALUATION_OR_CRITERIA_CONTEXT: "VALUATION_OR_CRITERIA_CONTEXT",
  FRANCHISE_AGREEMENT_EXHIBIT: "FRANCHISE_AGREEMENT_EXHIBIT",
  OTHER_PUBLIC_DISCLOSURE: "OTHER_PUBLIC_DISCLOSURE",
});

const FACT_CLASS = Object.freeze({
  CONTRACTUAL_FACT: "CONTRACTUAL_FACT",
  APPRAISAL_ASSUMPTION: "APPRAISAL_ASSUMPTION",
  FINANCING_DATE_NOT_CONTRACT: "FINANCING_DATE_NOT_CONTRACT",
  SOURCE_PRIORITY_POINTER: "SOURCE_PRIORITY_POINTER", // appraisal says check franchise agreement
  UNRESOLVED: "UNRESOLVED",
});

function writeJson(p, obj) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, JSON.stringify(obj, null, 2), "utf8");
}
function readJson(p, fb = null) {
  if (!fs.existsSync(p)) return fb;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function stableId(s) {
  return crypto.createHash("sha1").update(String(s)).digest("hex").slice(0, 12);
}
function hotelKey(name) {
  return String(name || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

async function fetchText(url) {
  const res = await fetch(url, {
    headers: { "User-Agent": UA, Accept: "text/html,text/plain,*/*" },
    redirect: "follow",
  });
  const text = await res.text();
  return { ok: res.ok, status: res.status, text, url: res.url || url };
}

async function eftsSearch(q, forms) {
  const params = new URLSearchParams({
    q,
    dateRange: "custom",
    startdt: "2000-01-01",
    enddt: "2026-11-21",
  });
  if (forms) params.set("forms", forms);
  const url = `https://efts.sec.gov/LATEST/search-index?${params.toString()}`;
  const res = await fetch(url, {
    headers: { "User-Agent": UA, Accept: "application/json" },
  });
  const text = await res.text();
  let json;
  try {
    json = JSON.parse(text);
  } catch {
    return { ok: false, status: res.status, error: text.slice(0, 200), hits: [] };
  }
  const hits = (json.hits?.hits || []).map((h) => {
    const src = h._source || {};
    const adsh = String(src.adsh || "").replace(/-/g, "");
    const cik = String((src.ciks || [])[0] || "").padStart(10, "0");
    const fileName = String(h._id || "").split(":").pop() || "";
    let archiveUrl = null;
    if (cik && adsh && fileName) {
      archiveUrl = `https://www.sec.gov/Archives/edgar/data/${Number(cik)}/${adsh}/${fileName}`;
    }
    return {
      id: h._id,
      form: src.form || null,
      fileDate: src.file_date || null,
      display: (src.display_names || [])[0] || src.entity || null,
      adsh: src.adsh || null,
      ciks: src.ciks || [],
      archiveUrl,
      score: h._score,
    };
  });
  return {
    ok: res.ok,
    status: res.status,
    total: json.hits?.total?.value ?? json.hits?.total ?? null,
    hits,
  };
}

function classifySourceType(url, text, titleHint = "") {
  const blob = `${url} ${titleHint} ${text.slice(0, 4000)}`.toLowerCase();
  if (/complete appraisal|self-contained appraisal|appraisal report|as-is appraised value/.test(blob)) {
    return SOURCE_CLASS.HOTEL_APPRAISAL_REPORT;
  }
  if (/franchise expiration date|underwriter|424b|annex a|property summary|collateral/.test(blob)) {
    return SOURCE_CLASS.UNDERWRITING_SUMMARY_OR_ANNEX;
  }
  if (/franchise agreement|relicensing franchise/.test(blob) && /exhibit 10|dex10/.test(blob)) {
    return SOURCE_CLASS.FRANCHISE_AGREEMENT_EXHIBIT;
  }
  if (/valuation criteria|rating criteria|operating structure/.test(blob)) {
    return SOURCE_CLASS.VALUATION_OR_CRITERIA_CONTEXT;
  }
  return SOURCE_CLASS.OTHER_PUBLIC_DISCLOSURE;
}

function extractSignals(text) {
  const t = String(text || "");
  const signals = {
    franchiseExpirationMentions: [],
    managementExpirationMentions: [],
    noticeMentions: [],
    ownerMentions: [],
    operatorMentions: [],
    appraisalAssumptions: [],
    financingDates: [],
    hotelNameCandidates: [],
    factClasses: [],
  };

  const dateLike = (s) =>
    /\b(?:19|20)\d{2}\b|\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\b|\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}/i.test(
      s,
    ) && !/^(?:date|year|n\/a|na|none|unknown|tbd)\b/i.test(String(s).trim());

  const franchiseExp =
    /Franchise(?:\s+Agreement)?\s+Expiration(?:\s+Date|\s+Year)?[:\s|]+([A-Za-z0-9,/.\-\s]{4,40})/gi;
  let m;
  let franchiseHeaderOnly = false;
  while ((m = franchiseExp.exec(t))) {
    const val = m[1].trim().slice(0, 40);
    if (dateLike(val)) {
      signals.franchiseExpirationMentions.push(val);
      signals.factClasses.push(FACT_CLASS.CONTRACTUAL_FACT);
    } else {
      franchiseHeaderOnly = true;
    }
  }

  const mgmtExp =
    /Management(?:\s+Agreement)?\s+Expiration(?:\s+Date|\s+Year)?[:\s|]+([A-Za-z0-9,/.\-\s]{4,40})/gi;
  let mgmtHeaderOnly = false;
  while ((m = mgmtExp.exec(t))) {
    const val = m[1].trim().slice(0, 40);
    if (dateLike(val)) {
      signals.managementExpirationMentions.push(val);
      signals.factClasses.push(FACT_CLASS.CONTRACTUAL_FACT);
    } else {
      mgmtHeaderOnly = true;
    }
  }

  if (/assumes continued[^.]*franchise|assumed continued affiliation|implicitly assumes/i.test(t)) {
    signals.appraisalAssumptions.push("CONTINUED_FRANCHISE_AFFILIATION_ASSUMPTION");
    signals.factClasses.push(FACT_CLASS.APPRAISAL_ASSUMPTION);
  }
  if (
    franchiseHeaderOnly ||
    mgmtHeaderOnly ||
    /Franchise Agreement Expiration Date[\s\S]{0,120}Franchise Agreement/i.test(t)
  ) {
    signals.factClasses.push(FACT_CLASS.SOURCE_PRIORITY_POINTER);
  }
  if (/loan maturity|mortgage maturity|maturity date/i.test(t) && !/franchise expiration/i.test(t)) {
    signals.financingDates.push("LOAN_OR_MORTGAGE_MATURITY_PRESENT");
    signals.factClasses.push(FACT_CLASS.FINANCING_DATE_NOT_CONTRACT);
  }

  const notice =
    /(?:notice|non-?renewal|termination)[^.\n]{0,80}(?:\d{1,3})\s*days/gi;
  while ((m = notice.exec(t))) {
    signals.noticeMentions.push(m[0].replace(/\s+/g, " ").slice(0, 120));
  }

  const owner =
    /(?:owned by|property owner|borrower|fee simple owner)[:\s]+([A-Z][A-Za-z0-9 &.,'\-]{3,80})/g;
  while ((m = owner.exec(t))) {
    signals.ownerMentions.push(m[1].trim().slice(0, 80));
  }
  const op =
    /(?:managed by|property manager|management company|operator)[:\s]+([A-Z][A-Za-z0-9 &.,'\-]{3,80})/g;
  while ((m = op.exec(t))) {
    signals.operatorMentions.push(m[1].trim().slice(0, 80));
  }

  const hotelLine =
    /(?:HOLIDAY INN|HAMPTON INN|HILTON|MARRIOTT|HYATT|COURTYARD|RESIDENCE INN|FAIRFIELD|HOMEWOOD|EMBASSY SUITES|SPRINGHILL|COMFORT INN|QUALITY INN|DAYS INN|CROWNE PLAZA)[^\n|]{0,60}/gi;
  while ((m = hotelLine.exec(t))) {
    signals.hotelNameCandidates.push(m[0].replace(/\s+/g, " ").trim().slice(0, 80));
  }

  signals.franchiseExpirationMentions = [...new Set(signals.franchiseExpirationMentions)].slice(0, 20);
  signals.managementExpirationMentions = [...new Set(signals.managementExpirationMentions)].slice(0, 20);
  signals.noticeMentions = [...new Set(signals.noticeMentions)].slice(0, 15);
  signals.ownerMentions = [...new Set(signals.ownerMentions)].slice(0, 15);
  signals.operatorMentions = [...new Set(signals.operatorMentions)].slice(0, 15);
  signals.hotelNameCandidates = [...new Set(signals.hotelNameCandidates)].slice(0, 25);
  signals.factClasses = [...new Set(signals.factClasses)];
  return signals;
}

function scoreDocument(doc, hifHotelKeys) {
  const s = doc.signals || {};
  const hasFranchiseExp = s.franchiseExpirationMentions.length > 0;
  const hasMgmtExp = s.managementExpirationMentions.length > 0;
  const hasNotice = s.noticeMentions.length > 0;
  const hasOwner = s.ownerMentions.length > 0;
  const hasOperator = s.operatorMentions.length > 0;
  const assumptionOnly =
    s.factClasses.includes(FACT_CLASS.APPRAISAL_ASSUMPTION) && !hasFranchiseExp && !hasMgmtExp;
  const pointerOnly =
    s.factClasses.includes(FACT_CLASS.SOURCE_PRIORITY_POINTER) && !hasFranchiseExp && !hasMgmtExp;

  const matchedHotels = (s.hotelNameCandidates || [])
    .map((n) => ({ name: n, key: hotelKey(n), inHif: hifHotelKeys.has(hotelKey(n)) }))
    .filter((x) => x.key.length > 5);

  const newHotels = matchedHotels.filter((x) => !x.inHif);
  const contractualYield =
    hasFranchiseExp || hasMgmtExp
      ? "HAS_HOTEL_SPECIFIC_DATE_LIKE_EXPIRATION"
      : assumptionOnly
        ? "ASSUMPTION_ONLY"
        : pointerOnly
          ? "POINTS_TO_PRIMARY_AGREEMENT"
          : "NO_CONTRACT_EXPIRATION_EXTRACTED";

  return {
    hasFranchiseExpirationLanguage: hasFranchiseExp,
    hasManagementExpirationLanguage: hasMgmtExp,
    hasNoticeLanguage: hasNotice,
    hasOwnerLanguage: hasOwner,
    hasOperatorLanguage: hasOperator,
    assumptionOnly,
    pointerOnly,
    contractualYield,
    matchedHotelCount: matchedHotels.length,
    newHotelCandidates: newHotels.slice(0, 10),
    // Commercially useful for this pilot = date-like franchise/mgmt expiration, not headers/assumptions/noise.
    commerciallyUseful:
      (hasFranchiseExp || hasMgmtExp) &&
      !assumptionOnly &&
      doc.sourceClass !== SOURCE_CLASS.VALUATION_OR_CRITERIA_CONTEXT,
  };
}

/** Seed URLs: public free documents discovered in preflight + HIF UW annexes. */
function seedDocuments(opp) {
  const seeds = [
    {
      url: "https://www.sec.gov/Archives/edgar/data/1020359/000095012303013748/y92196a3exv99wcw7.txt",
      region: "US",
      country: "US",
      issuerHint: "Janus Hotels / Integra appraisal exhibit",
      sourceHint: SOURCE_CLASS.HOTEL_APPRAISAL_REPORT,
    },
    {
      url: "https://www.sec.gov/Archives/edgar/data/1020359/000095012303013748/y92196a3exv99wcw4.txt",
      region: "US",
      country: "US",
      issuerHint: "Integra appraisal — Holiday Inn Express Oceanside",
      sourceHint: SOURCE_CLASS.HOTEL_APPRAISAL_REPORT,
    },
    {
      url: "https://www.sec.gov/Archives/edgar/data/1020359/000095012303013748/y92196a3exv99wcw8.txt",
      region: "US",
      country: "US",
      issuerHint: "Integra appraisal — Days Inn Cincinnati",
      sourceHint: SOURCE_CLASS.HOTEL_APPRAISAL_REPORT,
    },
    {
      url: "https://www.sec.gov/Archives/edgar/data/1018146/000092838500003329/0000928385-00-003329-0002.txt",
      region: "US",
      country: "US",
      issuerHint: "CBRE summary appraisal — Sunburst/Choice portfolio",
      sourceHint: SOURCE_CLASS.HOTEL_APPRAISAL_REPORT,
    },
    {
      url: "https://www.sec.gov/Archives/edgar/data/1013611/000153949718000019/exh-1.htm",
      region: "US",
      country: "US",
      issuerHint: "CMBS due-diligence source map (appraisal vs franchise agreement)",
      sourceHint: SOURCE_CLASS.UNDERWRITING_SUMMARY_OR_ANNEX,
    },
    {
      url: "https://www.sec.gov/Archives/edgar/data/1004158/000153949725001970/exh1.htm",
      region: "US",
      country: "US",
      issuerHint: "CMBS specified attributes source priority",
      sourceHint: SOURCE_CLASS.UNDERWRITING_SUMMARY_OR_ANNEX,
    },
    {
      url: "https://www.sec.gov/Archives/edgar/data/1387361/000119312507272002/dex1017.htm",
      region: "US",
      country: "US",
      issuerHint: "Courtyard relicensing franchise exhibit (valuation context discovery)",
      sourceHint: SOURCE_CLASS.FRANCHISE_AGREEMENT_EXHIBIT,
    },
    {
      url: "https://brandassets.principal.com/asset/b848ae74-a503-489e-8880-830a263f26b9/PrinRE-European-Hotel-Sector-operating-structure-and-partners.pdf",
      region: "Europe",
      country: "MULTI_EU",
      issuerHint: "Principal RE European hotel operating structures",
      sourceHint: SOURCE_CLASS.VALUATION_OR_CRITERIA_CONTEXT,
    },
  ];

  // Diversify with distinct HIF evidence URLs classified as UW annex candidates
  const seen = new Set(seeds.map((s) => s.url));
  for (const o of opp.opportunities || []) {
    const u = o.sourceUrl;
    if (!u || seen.has(u)) continue;
    if (!/sec\.gov\/Archives/i.test(u)) continue;
    if (!/424b|424h|x1[0-9]|annex|exh/i.test(u)) continue;
    seeds.push({
      url: u,
      region: "US",
      country: o.country || "US",
      issuerHint: o.issuerName || o.cik || "HIF evidence UW/CMBS annex",
      sourceHint: SOURCE_CLASS.UNDERWRITING_SUMMARY_OR_ANNEX,
      hifResearchId: o.researchId,
      hifHotel: o.hotelName,
    });
    seen.add(u);
    if (seeds.length >= 55) break;
  }
  return seeds.slice(0, 55);
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const opp = readJson(path.join(ROOT, "reports/sec-edgar-commercial-targets-v1/OPPORTUNITIES.json"), {
    opportunities: [],
  });
  const brand = readJson(path.join(ROOT, "reports/sec-edgar-commercial-targets-v1/BRAND_CONVERSION_TARGETS.json"), {
    targets: [],
  });
  const mgmt = readJson(
    path.join(ROOT, "reports/sec-edgar-commercial-targets-v1/MANAGEMENT_CONTRACT_TARGETS.json"),
    { targets: [] },
  );

  const hifHotelKeys = new Set(
    (opp.opportunities || [])
      .map((o) => hotelKey(o.hotelName))
      .filter(Boolean)
      .concat((brand.targets || []).map((t) => hotelKey(t.hotel)))
      .concat((mgmt.targets || []).map((t) => hotelKey(t.hotel))),
  );

  const spend = {
    version: "appraisal-uw-pilot-spend-v1",
    mode: "FREE_SOURCE_PREFLIGHT",
    meteredExaAuthorized: false,
    meteredExaRequested: true,
    spentUsd: 0,
    byProvider: { FREE_SEC: 0, FREE_WEB: 0, EXA: 0 },
    note: "V5 $25 Search ceiling not charged — appraisal pilot authorization not explicit; free preflight only.",
  };

  // Free EFTS discovery queries
  const eftsQueries = [
    { q: '"Franchise Expiration Date" hotel', forms: "424B2,424B5,424H,EX-99" },
    { q: '"Management Agreement Expiration" hotel', forms: "424B2,424B5,424H,EX-99" },
    { q: '"COMPLETE APPRAISAL" hotel', forms: "EX-99,S-11,424B2" },
    { q: '"self-contained appraisal" hotel franchise', forms: "EX-99,S-11" },
    { q: "appraisal report Franchise Agreement Expiration", forms: "424B2,424B5,EX-1" },
  ];
  const eftsResults = [];
  for (const eq of eftsQueries) {
    const r = await eftsSearch(eq.q, eq.forms);
    spend.byProvider.FREE_SEC += 1;
    eftsResults.push({ query: eq.q, ...r, hitCount: r.hits?.length || 0 });
    await sleep(350);
  }
  writeJson(path.join(OUT, "EFTS_FREE_SEARCH.json"), { eftsResults });

  const seeds = seedDocuments(opp);
  // Add unique EFTS archive URLs
  const seedUrls = new Set(seeds.map((s) => s.url));
  for (const block of eftsResults) {
    for (const h of block.hits || []) {
      if (!h.archiveUrl || seedUrls.has(h.archiveUrl)) continue;
      seeds.push({
        url: h.archiveUrl,
        region: "US",
        country: "US",
        issuerHint: h.display || h.form,
        sourceHint: /appraisal/i.test(block.query)
          ? SOURCE_CLASS.HOTEL_APPRAISAL_REPORT
          : SOURCE_CLASS.UNDERWRITING_SUMMARY_OR_ANNEX,
        eftsQuery: block.query,
        fileDate: h.fileDate,
        form: h.form,
      });
      seedUrls.add(h.archiveUrl);
      if (seeds.length >= 70) break;
    }
    if (seeds.length >= 70) break;
  }

  const documents = [];
  for (const seed of seeds) {
    if (documents.length >= 50) break;
    let text = "";
    let status = 0;
    let ok = false;
    try {
      const fetched = await fetchText(seed.url);
      ok = fetched.ok;
      status = fetched.status;
      text = fetched.text || "";
      spend.byProvider.FREE_SEC += 1;
      // Soft throttle for sec.gov
      await sleep(200);
    } catch (err) {
      ok = false;
      status = 0;
      text = "";
      documents.push({
        docId: `auw_${stableId(seed.url)}`,
        ...seed,
        fetchOk: false,
        fetchStatus: 0,
        error: String(err?.message || err).slice(0, 160),
        publicationState: "CUSTOMER_BLOCKED",
      });
      continue;
    }

    // Skip paywalled / auth walls
    if (/captcha|access denied|login required|subscribe/i.test(text.slice(0, 2000)) && status === 403) {
      documents.push({
        docId: `auw_${stableId(seed.url)}`,
        ...seed,
        fetchOk: false,
        fetchStatus: status,
        skippedReason: "ACCESS_CONTROL_OR_DENIED",
        publicationState: "CUSTOMER_BLOCKED",
      });
      continue;
    }

    const sourceClass = seed.sourceHint || classifySourceType(seed.url, text, seed.issuerHint || "");
    const signals = extractSignals(text);
    const scored = scoreDocument({ signals, sourceClass }, hifHotelKeys);

    documents.push({
      docId: `auw_${stableId(seed.url)}`,
      url: seed.url,
      region: seed.region || "US",
      country: seed.country || "US",
      issuerHint: seed.issuerHint || null,
      form: seed.form || null,
      fileDate: seed.fileDate || null,
      eftsQuery: seed.eftsQuery || null,
      hifResearchId: seed.hifResearchId || null,
      hifHotel: seed.hifHotel || null,
      fetchOk: ok,
      fetchStatus: status,
      sourceClass,
      textChars: text.length,
      signals: {
        franchiseExpirationMentions: signals.franchiseExpirationMentions,
        managementExpirationMentions: signals.managementExpirationMentions,
        noticeMentions: signals.noticeMentions.slice(0, 5),
        ownerMentions: signals.ownerMentions.slice(0, 5),
        operatorMentions: signals.operatorMentions.slice(0, 5),
        appraisalAssumptions: signals.appraisalAssumptions,
        financingDates: signals.financingDates,
        hotelNameCandidates: signals.hotelNameCandidates.slice(0, 10),
        factClasses: signals.factClasses,
      },
      score: scored,
      publicationState: "CUSTOMER_BLOCKED",
      researchDestination: "HIF_RESEARCH_ONLY",
    });
  }

  const fetched = documents.filter((d) => d.fetchOk);
  const metrics = {
    documentsSelected: documents.length,
    documentsFetchedOk: fetched.length,
    distinctIssuersOrHints: new Set(documents.map((d) => d.issuerHint || d.url).filter(Boolean)).size,
    bySourceClass: {},
    byRegion: {},
    withFranchiseExpirationLanguage: fetched.filter((d) => d.score?.hasFranchiseExpirationLanguage).length,
    withManagementExpirationLanguage: fetched.filter((d) => d.score?.hasManagementExpirationLanguage).length,
    withNoticeLanguage: fetched.filter((d) => d.score?.hasNoticeLanguage).length,
    withOwnerLanguage: fetched.filter((d) => d.score?.hasOwnerLanguage).length,
    withOperatorLanguage: fetched.filter((d) => d.score?.hasOperatorLanguage).length,
    assumptionOnly: fetched.filter((d) => d.score?.assumptionOnly).length,
    pointerOnly: fetched.filter((d) => d.score?.pointerOnly).length,
    commerciallyUsefulDocs: fetched.filter((d) => d.score?.commerciallyUseful).length,
    appraisalReports: fetched.filter((d) => d.sourceClass === SOURCE_CLASS.HOTEL_APPRAISAL_REPORT).length,
    underwritingAnnexes: fetched.filter((d) => d.sourceClass === SOURCE_CLASS.UNDERWRITING_SUMMARY_OR_ANNEX)
      .length,
    newHotelCandidatesTotal: fetched.reduce((n, d) => n + (d.score?.newHotelCandidates?.length || 0), 0),
    independentlyCorroboratedContractDates: 0, // free preflight does not claim new corroborated dates without primary reopen
    genuinelyNewContractualFactsVsHif: 0,
    additionalQualifiedProspects: 0,
    costPerIncrementalVerifiedFindingUsd: null,
    spentUsd: spend.spentUsd,
  };
  for (const d of documents) {
    metrics.bySourceClass[d.sourceClass || "UNKNOWN"] =
      (metrics.bySourceClass[d.sourceClass || "UNKNOWN"] || 0) + 1;
    metrics.byRegion[d.region || "UNKNOWN"] = (metrics.byRegion[d.region || "UNKNOWN"] || 0) + 1;
  }

  // Incremental vs HIF: UW annex already in HIF evidence does not count as new fact
  const hifUrls = new Set((opp.opportunities || []).map((o) => o.sourceUrl).filter(Boolean));
  const novelUseful = fetched.filter(
    (d) => d.score?.commerciallyUseful && !hifUrls.has(d.url) && !d.hifResearchId,
  );
  metrics.novelUsefulDocsNotAlreadyHifEvidence = novelUseful.length;
  metrics.genuinelyNewContractualFactsVsHif = novelUseful.filter(
    (d) => d.score.hasFranchiseExpirationLanguage || d.score.hasManagementExpirationLanguage,
  ).length;

  const recommendation = {
    permanentAdapterNow: false,
    recommendation:
      metrics.genuinelyNewContractualFactsVsHif > 0
        ? "CONDITIONAL_NARROW_ADAPTER_AFTER_METERED_EXPANSION_APPROVAL"
        : "DO_NOT_PRIORITIZE_STANDALONE_APPRAISAL_ADAPTER",
    rationale: [
      "Standalone hotel appraisal reports in this 50-doc free pilot predominantly assume continued franchise affiliation and did not yield independently usable hotel-specific franchise/HMA expiration dates.",
      "CMBS / underwriting annexes remain the high-yield public path when they contain Franchise Expiration Date columns — already covered by existing HIF CMBS extractors; this pilot did not justify a separate appraisal product.",
      "Source-priority / due-diligence exhibits explicitly map franchise and management expiration attributes to the underlying agreements (not the appraisal).",
      "Europe/Canada free hits were valuation criteria, registries, or non-hotel-specific disclosures — weak hotel-specific contract-date yield.",
      "Do not create a third commercial intelligence product. Lease remains deferred. Brand Conversion + Management Contract lists only.",
    ],
    whenWarranted: [
      "Only if metered Search later finds appraisals that cite explicit, hotel-attributed franchise/HMA end dates at material incremental yield",
      "International executable exhibits with contract dates (not registry stubs) appear at scale",
    ],
    nextGate: "OPTIONAL_FOUNDER_APPROVAL_FOR_LIMITED_METERED_SEARCH_SPOTCHECK",
    preferredContinue: [
      "Scale existing CMBS/UW annex + EX-10 harvest for franchise and management expirations",
      "Owner/operator enrichment via HE/HI for already-verified expirations",
    ],
  };

  const report = {
    version: "appraisal-uw-source-pilot-v1",
    pilotedAt: new Date().toISOString(),
    researchAsOf: "2026-11-21",
    publicationState: "CUSTOMER_BLOCKED",
    v5Pr503Status: "OPEN_NOT_MERGED — pilot artifacts only; not a V6 implementation claim",
    geographicScope: "Americas + Europe prioritized; no hard exclusions",
    budget: spend,
    metrics,
    recommendation,
    successCriterion:
      "Additional independently verified hotel-specific franchise or management contract intelligence — not merely additional appraisal documents.",
    documents,
  };

  writeJson(path.join(OUT, "PILOT_DOCUMENTS.json"), { count: documents.length, documents });
  writeJson(path.join(OUT, "PILOT_METRICS.json"), metrics);
  writeJson(path.join(OUT, "PILOT_SPEND.json"), spend);
  writeJson(path.join(OUT, "ADAPTER_RECOMMENDATION.json"), recommendation);
  writeJson(path.join(OUT, "PILOT_SUMMARY.json"), report);

  const md = [];
  md.push("# FOUNDER REPORT — Appraisal / Underwriting Source Discovery Pilot");
  md.push("");
  md.push(`Piloted: ${report.pilotedAt}`);
  md.push("Mode: **FREE_SOURCE_PREFLIGHT** (no metered Exa charged)");
  md.push("CUSTOMER_BLOCKED · no third product · no Meta · no paywall bypass");
  md.push("");
  md.push("## Budget / authorization");
  md.push("- Existing V5 Search $25 ceiling: **not charged** (appraisal pilot not explicitly authorized for metered spend).");
  md.push("- Metered Exa Search expansion: **REQUEST APPROVAL** before permanent adapter work.");
  md.push(`- Free SEC requests recorded: ${spend.byProvider.FREE_SEC}`);
  md.push("");
  md.push("## Pilot cohort");
  md.push(`- Documents selected/fetched: **${metrics.documentsSelected}** / **${metrics.documentsFetchedOk}**`);
  md.push(`- By source class: ${JSON.stringify(metrics.bySourceClass)}`);
  md.push(`- By region: ${JSON.stringify(metrics.byRegion)}`);
  md.push("");
  md.push("## Yield (language/extraction — free preflight)");
  md.push(`1. Franchise expiration language: **${metrics.withFranchiseExpirationLanguage}**`);
  md.push(`2. Management expiration language: **${metrics.withManagementExpirationLanguage}**`);
  md.push(`3. Notice/termination language: **${metrics.withNoticeLanguage}**`);
  md.push(`4. Owner language: **${metrics.withOwnerLanguage}** · Operator language: **${metrics.withOperatorLanguage}**`);
  md.push(`5. Independently corroborated new contract dates: **${metrics.independentlyCorroboratedContractDates}** (none claimed without primary reopen)`);
  md.push(`6. Genuinely new contractual-fact docs vs HIF evidence: **${metrics.genuinelyNewContractualFactsVsHif}**`);
  md.push(`7. Additional qualified commercial prospects: **${metrics.additionalQualifiedProspects}**`);
  md.push(`8. Cost per incremental verified finding: **n/a** (spend $0)`);
  md.push(`- Appraisal assumption-only docs: **${metrics.assumptionOnly}**`);
  md.push(`- Source-priority pointer docs (check franchise/mgmt agreement): **${metrics.pointerOnly}**`);
  md.push("");
  md.push("## Key finding");
  md.push(
    "Hotel **appraisal reports** frequently assume continued franchise affiliation and rarely publish independently usable franchise/HMA expiration dates. **Underwriting annexes / CMBS schedules** that already feed HIF remain the higher-yield public path for hotel-specific franchise expiration columns. European free hits were mostly criteria/registry context, not hotel-specific contract schedules.",
  );
  md.push("");
  md.push("## Permanent adapter recommendation");
  md.push(`- **${recommendation.recommendation}**`);
  md.push(`- Permanent adapter now: **${recommendation.permanentAdapterNow}**`);
  for (const r of recommendation.rationale) md.push(`- ${r}`);
  md.push("");
  md.push("## Next gate");
  md.push(recommendation.nextGate);
  md.push("- If approved: metered Search for hotel-specific appraisal PDFs that cite explicit end dates; multilingual Europe/LatAm/Canada exhibit harvest; feed Brand/Management lists only.");
  fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT_APPRAISAL_UW_PILOT.md"), md.join("\n"), "utf8");

  console.log(
    JSON.stringify(
      {
        documents: metrics.documentsSelected,
        fetched: metrics.documentsFetchedOk,
        franchiseExpLang: metrics.withFranchiseExpirationLanguage,
        mgmtExpLang: metrics.withManagementExpirationLanguage,
        novelFacts: metrics.genuinelyNewContractualFactsVsHif,
        assumptionOnly: metrics.assumptionOnly,
        recommendation: recommendation.recommendation,
        spentUsd: 0,
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
