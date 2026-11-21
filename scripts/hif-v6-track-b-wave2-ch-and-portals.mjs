#!/usr/bin/env node
/**
 * HIF V6 Track B Wave 2 — Companies House candidate deep research + portal filing probes.
 * Uses COMPANIES_HOUSE_API_KEY when present; otherwise public HTML only.
 * Never logs or writes the API key. CUSTOMER_BLOCKED.
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

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env", "C:/Dev/dealality/ws-hotel-explorer/.env"]) {
  if (fs.existsSync(p)) dotenv.config({ path: p, override: false });
}

const OUT = path.join(ROOT, "reports/sec-edgar-hif-v6-track-b-wave2");
const RUN = process.argv.includes("--run");
const LIMIT = Number((process.argv.find((a) => a.startsWith("--limit=")) || "").split("=")[1]) || 25;
const UA =
  process.env.SEC_EDGAR_USER_AGENT ||
  process.env.EDGAR_USER_AGENT ||
  "DealalityHIFResearch/6.0 (research@dealality.com)";

const writeJson = (fp, obj) => {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify(obj, null, 2), "utf8");
};
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function getChKey() {
  const k = String(process.env.COMPANIES_HOUSE_API_KEY || "").trim();
  return k || null;
}

function chAuthHeader(key) {
  // Basic: key as username, empty password — built at runtime; never logged
  return `Basic ${Buffer.from(`${key}:`, "utf8").toString("base64")}`;
}

async function fetchText(url, opts = {}) {
  const started = Date.now();
  try {
    const res = await fetch(url, {
      headers: {
        "User-Agent": UA,
        Accept: opts.accept || "text/html,application/json;q=0.9,*/*;q=0.8",
        ...(opts.headers || {}),
      },
      redirect: "follow",
      signal: AbortSignal.timeout(opts.timeoutMs || 30000),
    });
    const text = await res.text();
    return {
      ok: res.ok,
      status: res.status,
      text,
      ms: Date.now() - started,
      errorClass: classifyHttp(res.status, text, opts.expectAuth),
    };
  } catch (err) {
    return {
      ok: false,
      status: null,
      text: "",
      ms: Date.now() - started,
      errorClass: "NETWORK_OR_TIMEOUT",
      error: String(err?.message || err),
    };
  }
}

function classifyHttp(status, text, expectAuth) {
  if (status === 401 || status === 403) return "AUTHENTICATION_ERROR";
  if (status === 429) return "RATE_LIMIT";
  if (status >= 500) return "SERVER_ERROR";
  if (status >= 400) return "CLIENT_OR_SCHEMA_ERROR";
  if (status === 200 && (!text || text.length < 20)) return "EMPTY_RESULT";
  return null;
}

function parseCompanyNumber(label) {
  const m = String(label || "").match(/\[([A-Z0-9]{6,8})\]\s*$/);
  return m ? m[1] : null;
}

function loadCandidates() {
  const fp = path.join(ROOT, "reports/sec-edgar-hif-v6-track-b-regulatory/ENTITY_CANDIDATES.json");
  if (!fs.existsSync(fp)) return [];
  const j = JSON.parse(fs.readFileSync(fp, "utf8"));
  return (j.entities || [])
    .map((e) => ({
      ...e,
      companyNumber: parseCompanyNumber(e.legalNameOrLabel),
      legalName: String(e.legalNameOrLabel || "").replace(/\s*\[[A-Z0-9]+\]\s*$/, "").trim(),
    }))
    .filter((e) => e.companyNumber);
}

function parsePublicCompanyPage(html, companyNumber) {
  const text = String(html || "");
  const statusMatch = text.match(/Company status<\/dt>\s*<dd[^>]*>\s*([^<]+)/i) ||
    text.match(/status[^<]{0,40}(Active|Dissolved|Liquidation|Administration)/i);
  const status = statusMatch ? String(statusMatch[1] || statusMatch[0]).replace(/\s+/g, " ").trim() : null;
  const sic = [...text.matchAll(/SIC[\s\S]{0,40}?(\d{5})\s*[-–]\s*([^<\n]+)/gi)].map((m) => ({
    code: m[1],
    label: m[2].replace(/\s+/g, " ").trim(),
  }));
  const hasHotelsSic = sic.some((s) => s.code === "55100");
  const nameMatch = text.match(/<p class="heading-xlarge">\s*([^<]+)/i) || text.match(/<h1[^>]*>\s*([^<]+)/i);
  return {
    companyNumber,
    companyName: nameMatch ? nameMatch[1].replace(/\s+/g, " ").trim() : null,
    companyStatus: status,
    sic,
    hasHotelsSic,
    registeredOfficeSnippet: (text.match(/Registered office address[\s\S]{0,200}/i) || [null])[0],
  };
}

async function main() {
  const key = getChKey();
  const authStatus = {
    envVar: "COMPANIES_HOUSE_API_KEY",
    keyPresent: Boolean(key),
    outcome: key ? "KEY_PRESENT_WILL_TEST" : "KEY_ABSENT_PUBLIC_HTML_ONLY",
    blocker: key
      ? null
      : "COMPANIES_HOUSE_API_KEY missing in environment / local .env — continue public-source research",
  };

  if (!RUN) {
    console.log(
      JSON.stringify(
        { ok: true, dryRun: true, authStatus, candidates: loadCandidates().length },
        null,
        2,
      ),
    );
    return;
  }

  fs.mkdirSync(OUT, { recursive: true });
  // Never write the key
  writeJson(path.join(OUT, "CH_API_AUTH_STATUS.json"), {
    ...authStatus,
    testedAt: new Date().toISOString(),
    publicationState: "CUSTOMER_BLOCKED",
  });

  let apiLiveTest = null;
  if (key) {
    const testUrl = "https://api.company-information.service.gov.uk/company/01051132";
    const res = await fetchText(testUrl, {
      accept: "application/json",
      headers: { Authorization: chAuthHeader(key) },
      expectAuth: true,
    });
    let profile = null;
    if (res.ok) {
      try {
        profile = JSON.parse(res.text);
      } catch {
        profile = null;
      }
    }
    apiLiveTest = {
      url: testUrl,
      ok: res.ok,
      status: res.status,
      errorClass: res.errorClass,
      companyName: profile?.company_name || null,
      companyStatus: profile?.company_status || null,
      // do not embed raw body with potential PII dumps beyond name/status
    };
    authStatus.outcome = res.ok
      ? "AUTHENTICATED_OK"
      : res.errorClass || "AUTH_TEST_FAILED";
    writeJson(path.join(OUT, "CH_API_AUTH_STATUS.json"), {
      ...authStatus,
      apiLiveTest,
      testedAt: new Date().toISOString(),
      keyPresent: true,
      publicationState: "CUSTOMER_BLOCKED",
    });
  }

  const candidates = loadCandidates().slice(0, LIMIT);
  const investigated = [];
  let legalVerified = 0;
  let activeHospitality = 0;
  let propertyLinked = 0;
  let ownersVerified = 0;
  let operatorsVerified = 0;
  let contractExpirations = 0;

  for (const c of candidates) {
    const row = {
      companyNumber: c.companyNumber,
      legalNameFromWave1: c.legalName,
      sourceUrl: c.sourceUrl,
      apiUsed: false,
      publicHtmlUsed: false,
      companyStatus: null,
      hasHotelsSic: false,
      propertyLinkStatus: "NOT_VERIFIED",
      ownerVerified: false,
      operatorVerified: false,
      contractualExpiration: null,
      evidence: [],
    };

    if (key) {
      const apiUrl = `https://api.company-information.service.gov.uk/company/${c.companyNumber}`;
      const res = await fetchText(apiUrl, {
        accept: "application/json",
        headers: { Authorization: chAuthHeader(key) },
      });
      row.apiUsed = true;
      if (res.ok) {
        try {
          const profile = JSON.parse(res.text);
          row.companyName = profile.company_name;
          row.companyStatus = profile.company_status;
          row.sicCodes = profile.sic_codes || [];
          row.hasHotelsSic = (profile.sic_codes || []).includes("55100");
          row.evidence.push({ kind: "CH_API_COMPANY_PROFILE", url: apiUrl });
          legalVerified += 1;
          if (row.companyStatus === "active" && row.hasHotelsSic) activeHospitality += 1;
        } catch {
          row.apiParseError = true;
        }
      } else {
        row.apiErrorClass = res.errorClass || `HTTP_${res.status}`;
      }
    } else {
      const htmlUrl = `https://find-and-update.company-information.service.gov.uk/company/${c.companyNumber}`;
      const res = await fetchText(htmlUrl, { accept: "text/html" });
      row.publicHtmlUsed = true;
      if (res.ok) {
        const parsed = parsePublicCompanyPage(res.text, c.companyNumber);
        row.companyName = parsed.companyName || c.legalName;
        row.companyStatus = parsed.companyStatus;
        row.sic = parsed.sic;
        row.hasHotelsSic = parsed.hasHotelsSic;
        row.evidence.push({ kind: "CH_PUBLIC_HTML_PROFILE", url: htmlUrl });
        if (parsed.companyName || parsed.companyStatus) legalVerified += 1;
        if (/active/i.test(String(parsed.companyStatus || "")) && parsed.hasHotelsSic) {
          activeHospitality += 1;
        }
      } else {
        row.htmlErrorClass = res.errorClass || `HTTP_${res.status}`;
      }
    }

    // Property link: require stronger evidence than SIC/name — mark research candidate only
    if (row.hasHotelsSic && /active/i.test(String(row.companyStatus || ""))) {
      row.propertyLinkStatus = "RESEARCH_CANDIDATE_SIC_ACTIVE_ONLY";
      row.note =
        "SIC 55100 + Active is not property ownership proof; requires title/accounts/contract evidence";
    }

    investigated.push(row);
    await sleep(150);
  }

  // International portals — retrieve actual search/listing pages for hotel issuers (not reachability-only)
  const portalProbes = [];
  const portals = [
    {
      id: "BR_CVM",
      country: "BR",
      urls: ["https://dados.cvm.gov.br/dataset?q=hotel", "https://dados.cvm.gov.br/dataset?q=hospedagem"],
    },
    {
      id: "JP_EDINET",
      country: "JP",
      urls: ["https://disclosure2.edinet-fsa.go.jp/week0010.aspx"],
    },
    {
      id: "SG_SGX",
      country: "SG",
      urls: ["https://www.sgx.com/securities/company-announcements"],
    },
    {
      id: "HK_HKEX",
      country: "HK",
      urls: ["https://www1.hkexnews.hk/search/titlesearch.xhtml?lang=en"],
    },
    {
      id: "CA_SEDAR_PLUS",
      country: "CA",
      urls: ["https://www.sedarplus.ca/csa-party/records/search.html"],
    },
  ];
  for (const p of portals) {
    const probes = [];
    for (const url of p.urls) {
      const res = await fetchText(url, { accept: "text/html" });
      const hotelHits = (res.text.match(/\bhotels?\b|\bresorts?\b|\bhospitality\b|\bhospedagem\b/gi) || [])
        .length;
      probes.push({
        url,
        ok: res.ok,
        status: res.status,
        errorClass: res.errorClass,
        bytes: res.text.length,
        hotelLexiconHits: hotelHits,
        documentsInvestigated: res.ok ? 1 : 0,
      });
    }
    const anyDoc = probes.some((x) => x.ok && x.bytes > 500);
    portalProbes.push({
      ...p,
      accessPlusContent: anyDoc,
      meaningfulCoverage: false, // require property-level extract before true
      probes,
      note: anyDoc
        ? "Content retrieved; property-level hotel/agreement extract still pending"
        : "No usable content body",
    });
  }

  const metrics = {
    track: "TRACK_B",
    wave: "WAVE2",
    chApiAuthOutcome: authStatus.outcome,
    chApiKeyPresent: Boolean(key),
    candidatesInvestigated: investigated.length,
    legalEntitiesVerified: legalVerified,
    activeHospitalitySicCandidates: activeHospitality,
    physicalHotelsLinked: propertyLinked,
    propertyOwnersVerified: ownersVerified,
    operatorsVerified: operatorsVerified,
    newContractualExpirations: contractExpirations,
    portalsContentRetrieved: portalProbes.filter((p) => p.accessPlusContent).length,
    researchCostUsd: 0,
    publicationState: "CUSTOMER_BLOCKED",
  };

  writeJson(path.join(OUT, "CH_CANDIDATES_INVESTIGATED.json"), {
    count: investigated.length,
    investigated,
  });
  writeJson(path.join(OUT, "PORTAL_FILING_PROBES.json"), { portalProbes });
  writeJson(path.join(OUT, "TRACK_B_WAVE2_METRICS.json"), metrics);

  ensureWeeklyScorecardLayout(ROOT);
  const global = emptyWeeklyMetrics();
  global.documentsRetrievedAndProcessed =
    investigated.filter((i) => i.apiUsed || i.publicHtmlUsed).length +
    portalProbes.reduce((n, p) => n + p.probes.filter((x) => x.ok).length, 0);
  global.researchCandidates = activeHospitality;
  global.distinctHotelsIdentityVerified = 0;
  global.verifiedPropertyOwnerRelationships = 0;
  global.researchCostUsd = 0;

  const snap = buildWeeklySnapshot({
    weekId: `${isoWeekId()}-track-b-wave2`,
    asOf: new Date().toISOString().slice(0, 10),
    global,
    byCountry: {
      GB: {
        ...emptyWeeklyMetrics(),
        documentsRetrievedAndProcessed: investigated.length,
        researchCandidates: activeHospitality,
      },
    },
    byRegion: {
      WESTERN_EUROPE: {
        covered: investigated.length > 0,
        coverageNote: "ACTUAL_COMPANY_RESEARCH_NOT_PORTAL_REACHABILITY",
        ...emptyWeeklyMetrics(),
        documentsRetrievedAndProcessed: investigated.length,
        researchCandidates: activeHospitality,
      },
    },
    bySourceFamily: {
      CORPORATE_REGISTRIES: {
        independentlyVerifiedIncrementalFindings: 0,
        documentsProcessed: investigated.length,
        costUsd: 0,
        trackBFindings: legalVerified,
        trackAFindings: 0,
        duplicateFindings: 0,
        holdOrInvalidFindings: 0,
      },
      INTERNATIONAL_SECURITIES_REGULATORS: {
        independentlyVerifiedIncrementalFindings: 0,
        documentsProcessed: portalProbes.filter((p) => p.accessPlusContent).length,
        costUsd: 0,
        trackBFindings: portalProbes.filter((p) => p.accessPlusContent).length,
        trackAFindings: 0,
        duplicateFindings: 0,
        holdOrInvalidFindings: 0,
      },
    },
    trackA: emptyWeeklyMetrics(),
    trackB: { ...global },
    hotelExamples: [],
    notes: {
      wave: "WAVE2",
      chApi: authStatus.outcome,
      propertyLinksRequireStrongerEvidence: true,
    },
  });
  snap.allowOverwrite = true;
  saveWeeklySnapshot(snap, ROOT);

  fs.writeFileSync(
    path.join(OUT, "FOUNDER_TRACK_B_WAVE2.md"),
    [
      "# HIF V6 Track B — Wave 2",
      "",
      `- Companies House API: **${authStatus.outcome}**${key ? "" : " (key absent — public HTML path)"}`,
      `- Candidates investigated: ${investigated.length}`,
      `- Legal identities verified: ${legalVerified}`,
      `- Active + SIC 55100 candidates: ${activeHospitality}`,
      `- Physical hotels linked: ${propertyLinked}`,
      `- Property owners / operators verified: ${ownersVerified} / ${operatorsVerified}`,
      `- New contractual expirations: ${contractExpirations}`,
      `- Portal content retrieved (not reachability-only): ${metrics.portalsContentRetrieved}/${portalProbes.length}`,
      `- Spend: $0`,
      "",
      "## Sample legal entities",
      ...investigated
        .filter((i) => i.companyStatus)
        .slice(0, 12)
        .map(
          (i) =>
            `- ${i.companyName || i.legalNameFromWave1} [${i.companyNumber}] · ${i.companyStatus} · SIC55100=${i.hasHotelsSic} · property=${i.propertyLinkStatus}`,
        ),
    ].join("\n"),
  );

  console.log(
    JSON.stringify(
      {
        ok: true,
        metrics,
        authStatus: { ...authStatus, keyPresent: Boolean(key) },
        out: OUT,
      },
      null,
      2,
    ),
  );
}

main().catch((e) => {
  console.error(e);
  process.exitCode = 1;
});
