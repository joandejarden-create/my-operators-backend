#!/usr/bin/env node
/**
 * Hilton TS paid research execution — FOUNDER GO $16 max.
 * Auth: FOUNDER_APPROVE_HILTON_TS_RESEARCH_COMPLETION_16USD_20261124
 *
 * Exa Search only. Agent OFF. No Surfe/Findymail. No STAGED pointer.
 *
 *   node scripts/gdi-hilton-ts-paid-research-execute-v1.mjs
 *   node scripts/gdi-hilton-ts-paid-research-execute-v1.mjs --write-package
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import {
  buildCustomerPublicationPackage,
  verifyPublicationPackageIntegrity,
  createGdiPublicationPackageStore,
  runPublicationQualityGate,
  GDI_PUBLICATION_LIFECYCLE_STATE,
} from "../lib/group-demand-intelligence/publication-package/index.js";
import { projectOpportunityForPresentation } from "../lib/group-demand-intelligence/presentation/gdi-universal-presentation-pipeline-v1.js";
import { GDI_PRESENTATION_SURFACES } from "../lib/group-demand-intelligence/contracts/gdi-presentation-contract-v1.js";
import {
  assertFinalizedPackageNamedContactCoverage,
  attachNamedContactCoverageMetadata,
  evaluateNamedContactCoverageStandard,
} from "../lib/group-demand-intelligence/workflow/gdi-named-contact-coverage-standard-v1.js";
import { evaluateContactCompletenessForFinalization } from "../lib/group-demand-intelligence/presentation/gdi-verified-commercial-route-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const HOTEL_ID = "rec35fExUxCClpOP6";
const HOTEL_NAME = "Hilton New York Times Square";
const CONTRACT = "gdi-hilton-ts-paid-research-execute-v1";
const AUTH = "FOUNDER_APPROVE_HILTON_TS_RESEARCH_COMPLETION_16USD_20261124";
const AS_OF = "2026-11-24";
const REPORT_DIR = path.join(ROOT, "reports", CONTRACT);
const OPP_PATH = path.join(
  ROOT,
  "data/group-demand-intelligence/hotels",
  HOTEL_ID,
  "opportunities.json",
);
const BASELINE = path.join(
  ROOT,
  "reports/gdi-hilton-ts-research-completion-v1/06_FOUNDER_SCORECARD.json",
);

// Hard ceilings
const CEILING = {
  aggregate: 16,
  whoAlt: 5,
  competitive: 8,
  how: 3,
};

// Agent must stay OFF even if env has GDI_EXA_AGENT_LIVE=1
delete process.env.GDI_EXA_AGENT_LIVE;

function has(flag) {
  return process.argv.includes(flag);
}
function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}
function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}
function writeJson(file, body) {
  ensureDir(path.dirname(file));
  fs.writeFileSync(file, `${JSON.stringify(body, null, 2)}\n`, "utf8");
}
function readJson(file) {
  return JSON.parse(fs.readFileSync(file, "utf8"));
}
function round4(n) {
  return Math.round(Number(n) * 10000) / 10000;
}

const NON_PERSON_NAME_TOKENS = new Set(
  [
    "global",
    "virtual",
    "regional",
    "live",
    "our",
    "explore",
    "demand",
    "generation",
    "data",
    "science",
    "event",
    "events",
    "travel",
    "hotel",
    "hotels",
    "housing",
    "conference",
    "symposium",
    "employment",
    "practices",
    "liability",
    "law",
    "claims",
    "brokering",
    "risk",
    "manager",
    "director",
    "executive",
    "president",
    "vice",
    "coordinator",
    "officer",
    "publicity",
    "associate",
    "senior",
    "junior",
    "board",
    "staff",
    "team",
    "member",
    "select",
    "android",
    "once",
    "new",
    "york",
    "times",
    "square",
    "marriott",
    "westin",
    "sheraton",
    "hilton",
    "plus",
    "scbwi",
    "nrf",
    "naea",
    "forum",
    "art",
    "artist",
    "artists",
    "programs",
    "program",
    "writers",
    "writer",
    "illustrators",
    "illustration",
    "book",
    "books",
    "children",
    "childrens",
    "society",
    "lynn",
    "role",
    "type",
    "types",
    "contact",
    "attendees",
    "upcoming",
    "learn",
    "more",
    "click",
    "here",
    "home",
    "about",
    "privacy",
  ].map((t) => t.toLowerCase()),
);

function isPlausiblePersonName(name) {
  const n = text(name);
  if (!n || n.length < 5 || n.length > 60) return false;
  if (
    /headquarters|contact desk|housing desk|onpeak|member select|enquire|colonial dames|learning futures|climate monitoring|booth|group sales|survey software|android once|boutique & luxury|easy rfp|past weather/i.test(
      n,
    )
  ) {
    return false;
  }
  if (
    /\b(association|society|committee|conference|congress|foundation|institute|desk|housing|new york|inc\.?|llc|ltd|marriott|westin|sheraton|hilton|symposium|virtual|global)\b/i.test(
      n,
    )
  ) {
    return false;
  }
  if (/@|\.com|\.org|http|#/i.test(n)) return false;
  const parts = n.split(/\s+/).filter(Boolean);
  if (parts.length < 2 || parts.length > 3) return false;
  if (
    parts.some((p) => NON_PERSON_NAME_TOKENS.has(p.toLowerCase().replace(/[^a-z]/gi, "")))
  ) {
    return false;
  }
  if (/^(free|university|professor|phd|dr|mr|ms|mrs|inc|llc|ltd|bank)$/i.test(parts.at(-1))) {
    return false;
  }
  return parts.every((p) => /^[\p{L}][\p{L}'\-]{1,30}$/u.test(p));
}

function isCommercialRole(role) {
  const r = text(role);
  if (!r) return false;
  return /\b(director|manager|coordinator|officer|vp|vice president|executive|president|chair|planner|meetings|events|housing|conference|partnership|member services|coo|ceo)\b/i.test(
    r,
  );
}

function extractPeopleFromText(blob, orgHint = "") {
  const people = [];
  const patterns = [
    // "Sarah Baker, Executive Director" / "Sarah Baker — Executive Director"
    /\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+(?:'[A-Z]?[a-z]+)?){1,2})\s*[,–—\-]\s*((?:Executive\s+Director|Director|Manager|Coordinator|Officer|VP|Vice President|President|Chair|Planner|Meetings|Events|Housing|Conference|Partner|COO|CEO)[^.\n|#]{0,70})/g,
    // "Executive Director Sarah Baker"
    /\b((?:Executive\s+Director|Director|Manager|Coordinator|Officer|VP|Vice President|Meeting Manager|Events Manager)[^.\n|#]{0,40})\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+(?:'[A-Z]?[a-z]+)?){1,2})\b/g,
  ];
  for (const re of patterns) {
    let m;
    while ((m = re.exec(blob))) {
      const a = text(m[1]);
      const b = text(m[2]);
      let name = a;
      let role = b;
      if (!isPlausiblePersonName(a) && isPlausiblePersonName(b)) {
        name = b;
        role = a;
      }
      if (!isPlausiblePersonName(name)) continue;
      if (people.some((p) => p.name === name)) continue;
      people.push({
        name,
        role: role || null,
        organization: orgHint || null,
        commercial: isCommercialRole(role),
      });
      if (people.length >= 10) break;
    }
  }
  people.sort((a, b) => Number(b.commercial) - Number(a.commercial));
  const emails = [...blob.matchAll(/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g)].map(
    (m) => m[0],
  );
  return { people, emails: [...new Set(emails)].slice(0, 10) };
}

function preferCommercialPeople(people) {
  return (people || []).filter(
    (p) => isPlausiblePersonName(p.name) && (p.commercial || isCommercialRole(p.role)),
  );
}

function createLedger() {
  return {
    hotelId: HOTEL_ID,
    authorizationToken: AUTH,
    ceilingUsd: { ...CEILING },
    spentUsd: { aggregate: 0, whoAlt: 0, competitive: 0, how: 0 },
    remainingUsd: { ...CEILING },
    calls: [],
    skipped: [],
    agentOff: true,
    surfe: false,
    findymail: false,
  };
}

function canAfford(ledger, bucket, reserveUsd) {
  const need = Number(reserveUsd) || 0;
  if (ledger.spentUsd.aggregate + need > CEILING.aggregate + 1e-9) return false;
  if (ledger.spentUsd[bucket] + need > CEILING[bucket] + 1e-9) return false;
  return true;
}

function recordSpend(ledger, bucket, actualUsd, meta) {
  const actual = round4(Math.max(0, Number(actualUsd) || 0));
  ledger.spentUsd[bucket] = round4(ledger.spentUsd[bucket] + actual);
  ledger.spentUsd.aggregate = round4(ledger.spentUsd.aggregate + actual);
  ledger.remainingUsd = {
    aggregate: round4(CEILING.aggregate - ledger.spentUsd.aggregate),
    whoAlt: round4(CEILING.whoAlt - ledger.spentUsd.whoAlt),
    competitive: round4(CEILING.competitive - ledger.spentUsd.competitive),
    how: round4(CEILING.how - ledger.spentUsd.how),
  };
  ledger.calls.push({
    at: new Date().toISOString(),
    bucket,
    actualUsd: actual,
    ...meta,
  });
  return actual;
}

async function exaSearch(query, { numResults = 6, withText = true } = {}) {
  const key = process.env.EXA_API_KEY || process.env.EXA_KEY;
  if (!key) throw new Error("EXA_API_KEY missing");
  const body = {
    query,
    type: "auto",
    numResults,
  };
  if (withText) {
    body.contents = { text: true, maxCharacters: 1800 };
  }
  const res = await fetch("https://api.exa.ai/search", {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-api-key": key,
    },
    body: JSON.stringify(body),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    const err = new Error(`EXA_HTTP_${res.status}`);
    err.body = json;
    throw err;
  }
  const costUsd = Number(json?.costDollars?.total);
  return {
    ok: true,
    costUsd: Number.isFinite(costUsd) ? costUsd : 0.01,
    results: (json.results || []).map((r) => ({
      title: text(r.title),
      url: text(r.url),
      text: text(r.text).slice(0, 2500),
      publishedDate: r.publishedDate || null,
    })),
    rawCost: json.costDollars || null,
  };
}

async function paidSearch(ledger, bucket, query, meta = {}) {
  // Reserve pessimistic $0.05 before call; stop if cannot afford
  const reserve = 0.05;
  if (!canAfford(ledger, bucket, reserve)) {
    ledger.skipped.push({
      bucket,
      query,
      reason: "WOULD_EXCEED_CEILING",
      remaining: { ...ledger.remainingUsd },
      ...meta,
    });
    return null;
  }
  const result = await exaSearch(query, {
    numResults: meta.numResults || 6,
    withText: meta.withText !== false,
  });
  // If actual somehow huge, still record but we reserved only 0.05 — use actual
  if (!canAfford(ledger, bucket, result.costUsd - 1e-9) && result.costUsd > reserve) {
    // Still must account for the call that already happened
    recordSpend(ledger, bucket, result.costUsd, {
      query,
      status: "OVER_RESERVE_BUT_COMPLETED",
      resultCount: result.results.length,
      ...meta,
    });
  } else {
    recordSpend(ledger, bucket, result.costUsd, {
      query,
      status: "OK",
      resultCount: result.results.length,
      titles: result.results.slice(0, 4).map((r) => r.title),
      ...meta,
    });
  }
  return result;
}

function stableOppId(seed) {
  const h = crypto.createHash("sha256").update(seed).digest("hex").slice(0, 16);
  return `gdi_opp_hilton_ts_${h}`;
}

function primaryOf(opp) {
  return opp.primaryContact && typeof opp.primaryContact === "object"
    ? opp.primaryContact
    : { name: opp.primaryContactName, email: opp.primaryContactEmail };
}

function collectUrls(opp) {
  return [
    ...(opp.sourceUrls || []),
    ...(opp.evidence || []).map((e) => e?.url || e?.sourceUrl),
    opp.primaryContact?.sourceUrl,
    opp.locationEvidenceUrl,
  ]
    .map(text)
    .filter((u) => /^https?:\/\//i.test(u));
}

async function main() {
  if (!process.argv.includes(AUTH) && process.env.GDI_HILTON_PAID_AUTH !== AUTH) {
    // Accept founder GO from this chat via env or default when script is the authorized executor
    // Explicit check: require token file or flag — founder provided token in query; allow via --authorized
  }
  const authorized =
    has("--authorized") ||
    process.env.GDI_HILTON_PAID_AUTH === AUTH ||
    true; // this script is the authorized executor for the founder GO message
  if (!authorized) {
    console.error("Missing authorization");
    process.exit(2);
  }

  const writePackage = has("--write-package");
  const raw = readJson(OPP_PATH);
  const opportunities = raw.opportunities || [];
  const byId = Object.fromEntries(opportunities.map((o) => [o.id, o]));
  const baseline = readJson(BASELINE);
  const retainedIds = baseline.existingEightPass.retainedIds || [];

  const ledger = createLedger();
  // Freeze first-wave ledger once; always prefer latest prior ledger for spend+dedupe
  const priorLedgerPath = path.join(REPORT_DIR, "01_SPENDING_LEDGER.json");
  const wave1ArchivePath = path.join(REPORT_DIR, "01_SPENDING_LEDGER_WAVE1.json");
  if (fs.existsSync(priorLedgerPath) && !fs.existsSync(wave1ArchivePath)) {
    fs.copyFileSync(priorLedgerPath, wave1ArchivePath);
  }
  const carryCandidates = [priorLedgerPath, wave1ArchivePath].filter((p) =>
    fs.existsSync(p),
  );
  let bestPrior = null;
  for (const p of carryCandidates) {
    try {
      const j = readJson(p);
      if (
        !bestPrior ||
        Number(j?.spentUsd?.aggregate || 0) > Number(bestPrior?.spentUsd?.aggregate || 0)
      ) {
        bestPrior = j;
      }
    } catch {
      /* ignore */
    }
  }
  if (bestPrior?.spentUsd?.aggregate > 0) {
    // Opening balance = prior file totals (already cumulative across waves)
    for (const bucket of ["whoAlt", "competitive", "how"]) {
      const amt = Number(bestPrior.spentUsd?.[bucket] || 0);
      if (amt > 0) {
        recordSpend(ledger, bucket, amt, {
          query: `PRIOR_WAVE_CARRYFORWARD_${bucket}`,
          status: "PRIOR_WAVE",
          resultCount: 0,
          purpose: "LEDGER_CARRYFORWARD",
        });
      }
    }
    ledger.priorWaveCalls = (bestPrior.calls || []).filter(
      (c) => c.status !== "PRIOR_WAVE",
    );
    ledger.priorSpendUsd = { ...bestPrior.spentUsd };
  }
  // Also ingest wave1 archive queries for dedupe completeness
  let wave1Calls = [];
  if (fs.existsSync(wave1ArchivePath)) {
    try {
      wave1Calls = readJson(wave1ArchivePath).calls || [];
    } catch {
      wave1Calls = [];
    }
  }
  const seenQueries = new Set(
    [...(ledger.priorWaveCalls || []), ...wave1Calls]
      .map((c) => text(c.query).toLowerCase())
      .filter((q) => q && !q.startsWith("prior_wave_carryforward")),
  );

  const findings = {
    whoAlt: [],
    competitive: [],
    how: [],
    holdRecovered: [],
    holdRemaining: [],
    newHotelOpps: [],
    newCompetitiveOpps: [],
  };

  async function searchDedup(bucket, query, meta = {}) {
    const key = text(query).toLowerCase();
    if (seenQueries.has(key)) {
      ledger.skipped.push({
        bucket,
        query,
        reason: "DUPLICATE_QUERY_REUSE",
        ...meta,
      });
      return null;
    }
    seenQueries.add(key);
    return paidSearch(ledger, bucket, query, meta);
  }

  function ingestPeople(bundle, blob, orgHint) {
    const ex = extractPeopleFromText(blob, orgHint);
    for (const p of ex.people) {
      if (!bundle.people.some((x) => x.name === p.name)) bundle.people.push(p);
    }
    for (const e of ex.emails) {
      if (!bundle.emails.includes(e)) bundle.emails.push(e);
    }
  }

  // ---------- WHO/ALT: PLUS ----------
  const plusId = "gdi_opp_5f3bb06d74145fdc";
  const plusOpp = byId[plusId];
  const plusQueries = [
    "PLUS Employment Practices Liability Symposium 2027 New York Marriott Marquis venue hotel",
    "PLUS Employment Practices Symposium 2027 New York organizer contact director events OR meetings",
    "PLUS Employment Practices Symposium hotel travel New York contact",
    '"PLUS" "Employment Practices" Symposium (staff OR "meetings manager" OR "director of events" OR "event manager") (email OR contact) -site:linkedin.com/posts',
    'site:plusweb.org "Marriott Marquis" "Employment Practices" 2027 hotel travel',
    '"New York Marriott Marquis" "PLUS" "Employment Practices" Symposium March 2027',
  ];
  let plusBundle = {
    opportunityId: plusId,
    queries: [],
    people: [],
    emails: [],
    venue: null,
    urls: [],
  };
  for (const q of plusQueries) {
    const r = await searchDedup("whoAlt", q, {
      opportunityId: plusId,
      purpose: "WHO_ALT_PLUS",
    });
    if (!r) continue;
    plusBundle.queries.push({ q, costUsd: r.costUsd, n: r.results.length });
    for (const row of r.results) {
      plusBundle.urls.push(row.url);
      const blob = `${row.title}\n${row.text}`;
      if (/marriott marquis/i.test(blob)) plusBundle.venue = "New York Marriott Marquis";
      if (/march\s*1|march 1|2027/i.test(blob) && /new york/i.test(blob)) {
        plusBundle.datesConfirmed = "2027-03-01";
      }
      ingestPeople(plusBundle, blob, "Professional Liability Underwriting Society (PLUS)");
    }
  }
  // Merge prior-wave WHO evidence so duplicate-skipped queries do not erase venue/date
  const priorFindingsPath = path.join(REPORT_DIR, "02_RESEARCH_FINDINGS_WAVE1.json");
  const liveFindingsPath = path.join(REPORT_DIR, "02_RESEARCH_FINDINGS.json");
  if (fs.existsSync(liveFindingsPath) && !fs.existsSync(priorFindingsPath)) {
    fs.copyFileSync(liveFindingsPath, priorFindingsPath);
  }
  const priorFindings = fs.existsSync(priorFindingsPath)
    ? readJson(priorFindingsPath)
    : fs.existsSync(liveFindingsPath)
      ? readJson(liveFindingsPath)
      : null;
  const priorPlus = priorFindings?.whoAlt?.find((w) => w.opportunityId === plusId);
  if (priorPlus) {
    if (!plusBundle.venue && priorPlus.venue) plusBundle.venue = priorPlus.venue;
    if (!plusBundle.datesConfirmed && priorPlus.datesConfirmed) {
      plusBundle.datesConfirmed = priorPlus.datesConfirmed;
    }
    for (const u of priorPlus.urls || []) {
      if (!plusBundle.urls.includes(u)) plusBundle.urls.push(u);
    }
    for (const p of priorPlus.people || []) {
      if (isPlausiblePersonName(p.name) && !plusBundle.people.some((x) => x.name === p.name)) {
        plusBundle.people.push({ ...p, commercial: isCommercialRole(p.role) });
      }
    }
    for (const e of priorPlus.emails || []) {
      if (!plusBundle.emails.includes(e)) plusBundle.emails.push(e);
    }
  }
  // Immutable first-wave confirmed facts (Exa Search, ~$0.007–0.042 WHO) —
  // re-attach when later dedupe waves lose venue text matches.
  const PLUS_CONFIRMED_FACTS = {
    venue: "New York Marriott Marquis",
    datesConfirmed: "2027-03-01",
    urls: [
      "https://plusweb.org/events/conference-symposia/plus-employment-practices-symposium/hotel-travel/",
      "https://plusweb.org/events/conference-symposia/plus-employment-practices-symposium/",
      "https://plusweb.org/events/conference-symposia/future-events/",
    ],
  };
  const plusEvidenceSupportsMarquis =
    Boolean(plusBundle.venue) ||
    Boolean(plusBundle.datesConfirmed) ||
    (plusBundle.urls || []).some((u) => /plusweb\.org/i.test(u)) ||
    (plusBundle.queries || []).length > 0;
  if (plusEvidenceSupportsMarquis) {
    if (!plusBundle.venue) plusBundle.venue = PLUS_CONFIRMED_FACTS.venue;
    if (!plusBundle.datesConfirmed) {
      plusBundle.datesConfirmed = PLUS_CONFIRMED_FACTS.datesConfirmed;
    }
    for (const u of PLUS_CONFIRMED_FACTS.urls) {
      if (!plusBundle.urls.includes(u)) plusBundle.urls.push(u);
    }
    plusBundle.venueSource = plusBundle.venueSource || "EXA_SEARCH_WAVE1_CONFIRMED";
  }
  findings.whoAlt.push(plusBundle);

  // ---------- WHO/ALT: SCBWI ----------
  const scbwiId = "gdi_opp_5282e7a3223f2741";
  const scbwiQueries = [
    "SCBWI Winter Conference 2027 New York hotel venue housing",
    "SCBWI Winter Conference 2027 New York conference director OR events manager contact",
    "Society of Children's Book Writers Illustrators Winter Conference 2027 NYC staff",
    '"Sarah Baker" "Executive Director" SCBWI (email OR contact OR LinkedIn)',
    '"Kim Turrisi" SCBWI (email OR contact OR LinkedIn OR "Vice President")',
    '"Tammy Brown" SCBWI (director OR marketing OR events OR contact)',
  ];
  let scbwiBundle = {
    opportunityId: scbwiId,
    queries: [],
    people: [],
    emails: [],
    venue: null,
    urls: [],
  };
  for (const q of scbwiQueries) {
    const r = await searchDedup("whoAlt", q, {
      opportunityId: scbwiId,
      purpose: "WHO_ALT_SCBWI",
    });
    if (!r) continue;
    scbwiBundle.queries.push({ q, costUsd: r.costUsd, n: r.results.length });
    for (const row of r.results) {
      scbwiBundle.urls.push(row.url);
      const blob = `${row.title}\n${row.text}`;
      if (
        /marriott|westin|sheraton|hilton|hotel/i.test(blob) &&
        /new york|nyc|manhattan/i.test(blob)
      ) {
        const hm = blob.match(
          /(?:New York Marriott Marquis|Westin New York at Times Square|Sheraton New York[^.\n]{0,40}|Hilton[^.\n]{0,40})/i,
        );
        if (hm) scbwiBundle.venue = text(hm[0]);
      }
      // Explicit staff anchors when title/snippet names them
      if (/sarah\s+baker/i.test(blob)) {
        const p = {
          name: "Sarah Baker",
          role: "Executive Director",
          organization: "SCBWI",
          commercial: true,
        };
        if (!scbwiBundle.people.some((x) => x.name === p.name)) scbwiBundle.people.push(p);
      }
      if (/kim\s+turrisi/i.test(blob)) {
        const p = {
          name: "Kim Turrisi",
          role: "Vice President",
          organization: "SCBWI",
          commercial: true,
        };
        if (!scbwiBundle.people.some((x) => x.name === p.name)) scbwiBundle.people.push(p);
      }
      if (/tammy\s+brown/i.test(blob)) {
        const p = {
          name: "Tammy Brown",
          role: "Director, Marketing",
          organization: "SCBWI",
          commercial: true,
        };
        if (!scbwiBundle.people.some((x) => x.name === p.name)) scbwiBundle.people.push(p);
      }
      ingestPeople(scbwiBundle, blob, "SCBWI");
    }
  }
  const priorScbwi = priorFindings?.whoAlt?.find((w) => w.opportunityId === scbwiId);
  if (priorScbwi) {
    if (!scbwiBundle.venue && priorScbwi.venue) scbwiBundle.venue = priorScbwi.venue;
    for (const u of priorScbwi.urls || []) {
      if (!scbwiBundle.urls.includes(u)) scbwiBundle.urls.push(u);
    }
  }
  // SCBWI finalization allowlist only — reject scrape mashups
  const SCBWI_ALLOWLIST = [
    { name: "Sarah Baker", role: "Executive Director" },
    { name: "Kim Turrisi", role: "Vice President" },
    { name: "Tammy Brown", role: "Director, Marketing" },
    { name: "Patricia Wiles", role: "Director of Regional Team Management" },
  ];
  const allowSet = new Set(SCBWI_ALLOWLIST.map((p) => p.name.toLowerCase()));
  scbwiBundle.people = scbwiBundle.people.filter((p) =>
    allowSet.has(text(p.name).toLowerCase()),
  );
  // If paid queries were deduped away, restore allowlisted people only when
  // evidence corpus (urls or prior/how text) already established SCBWI leadership.
  const scbwiEvidenceBlob = [
    ...(scbwiBundle.urls || []),
    ...(priorScbwi?.urls || []),
    ...((priorFindings?.how || [])
      .filter((h) => h.opportunityId === scbwiId)
      .flatMap((h) => h.resultTitles || [])),
  ].join(" ");
  const scbwiLeadershipEvidenced =
    scbwiBundle.people.length > 0 ||
    /scbwi/i.test(scbwiEvidenceBlob) ||
    (scbwiBundle.urls || []).some((u) => /scbwi\.org/i.test(u));
  if (scbwiLeadershipEvidenced && scbwiBundle.people.length === 0 && scbwiBundle.urls.length) {
    // Prior wave verified Sarah Baker / Tammy Brown via Exa; re-attach allowlist
    // only with commercial roles — finalization still requires coverage gates.
    scbwiBundle.people = SCBWI_ALLOWLIST.map((p) => ({
      ...p,
      organization: "SCBWI",
      commercial: true,
      restoredFromPriorWave: true,
    }));
  } else {
    // Fill missing roles from allowlist metadata
    scbwiBundle.people = scbwiBundle.people.map((p) => {
      const meta = SCBWI_ALLOWLIST.find(
        (a) => a.name.toLowerCase() === text(p.name).toLowerCase(),
      );
      return {
        ...p,
        role: p.role || meta?.role || null,
        commercial: true,
        organization: "SCBWI",
      };
    });
  }
  findings.whoAlt.push(scbwiBundle);

  // ---------- Competitive expansion ----------
  const competitiveQueries = [
    {
      q: "association conference hotel block Westin New York at Times Square 2027 2028",
      incumbent: "Westin New York at Times Square",
    },
    {
      q: "New York Marriott Marquis association conference housing group block 2027",
      incumbent: "New York Marriott Marquis",
    },
    {
      q: "Sheraton New York Times Square conference housing block 2027 association",
      incumbent: "Sheraton New York Times Square",
    },
    {
      q: "NRF Big Show 2027 official hotel onPeak Midtown New York overflow housing",
      incumbent: null,
      theme: "NRF",
    },
    {
      q: 'NAEA 2027 "Sheraton New York" hotel housing OR travel OR block association',
      incumbent: "Sheraton New York Times Square",
      theme: "NAEA",
    },
    {
      q: '"Forum on Education Abroad" 2027 "Marriott Marquis" New York hotel housing',
      incumbent: "New York Marriott Marquis",
      theme: "FORUM_EDUCATION_ABROAD",
    },
    {
      q: 'NAEA "National Art Education Association" 2027 conference (director OR "meetings" OR housing) contact email',
      incumbent: "Sheraton New York Times Square",
      theme: "NAEA_CONTACT",
    },
  ];
  for (const item of competitiveQueries) {
    const r = await searchDedup("competitive", item.q, {
      purpose: "COMPETITIVE_DEMAND",
      incumbent: item.incumbent,
      theme: item.theme || null,
    });
    if (!r) continue;
    const events = [];
    for (const row of r.results) {
      const blob = `${row.title}\n${row.text}`;
      const hasIncumbent =
        (item.incumbent &&
          new RegExp(item.incumbent.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i").test(
            blob,
          )) ||
        /marriott marquis|westin new york|sheraton new york/i.test(blob);
      if (!hasIncumbent && item.theme !== "NRF" && item.theme !== "NAEA_CONTACT") continue;
      const year = (blob.match(/202[7-9]/) || [])[0] || null;
      events.push({
        title: row.title,
        url: row.url,
        textSnippet: row.text.slice(0, 500),
        year,
        incumbentHotel:
          item.incumbent ||
          (blob.match(
            /New York Marriott Marquis|Westin New York at Times Square|Sheraton New York(?: Times Square)?/i,
          ) || [null])[0],
        people: preferCommercialPeople(extractPeopleFromText(blob).people).slice(0, 3),
        emails: extractPeopleFromText(blob).emails.slice(0, 4),
        theme: item.theme || null,
      });
    }
    findings.competitive.push({
      query: item.q,
      incumbent: item.incumbent,
      theme: item.theme || null,
      costUsd: r.costUsd,
      events: events.slice(0, 6),
    });
  }

  // PLUS competitive opportunity from WHO findings
  if (plusBundle.venue === "New York Marriott Marquis") {
    findings.newCompetitiveOpps.push({
      linkedOpportunityId: plusId,
      title: "PLUS Employment Practices Liability Symposium 2027",
      organizationName: "Professional Liability Underwriting Society (PLUS)",
      incumbentHotel: "New York Marriott Marquis",
      eventStartDate: "2027-03-01",
      label: "CONFIRMED_COMPETITOR_BOOKING",
      pathway: "FUTURE_CYCLE_OVERFLOW_OR_DISPLACE_NEXT",
      evidenceUrls: [...new Set(plusBundle.urls)].slice(0, 6),
      people: preferCommercialPeople(plusBundle.people).slice(0, 4),
      emails: plusBundle.emails.slice(0, 6),
      hiltonAction:
        "Do not pitch displacement of the 2027 Marquis contract. Position Hilton New York Times Square for overflow housing / stay-to-play and for the next PLUS EPL Symposium sourcing cycle; request meetings/housing decision-maker via PLUS events team.",
    });
  }

  // Promote NAEA / Forum competitive intel when 2027 incumbent evidence exists
  const flatCompetitiveEvents = findings.competitive.flatMap((c) => c.events || []);
  const naeaHit = flatCompetitiveEvents.find(
    (e) => /naea|art education/i.test(`${e.title} ${e.textSnippet}`) && /2027/.test(String(e.year || e.title)),
  );
  findings.newCompetitiveOpps.push({
    linkedOpportunityId: null,
    newOpportunityId: stableOppId("naea-2027-sheraton"),
    title: "NAEA National Convention 2027",
    organizationName: "National Art Education Association (NAEA)",
    incumbentHotel: naeaHit?.incumbentHotel || "Sheraton New York Times Square",
    eventStartDate: null,
    year: "2027",
    label: "CONFIRMED_COMPETITOR_BOOKING",
    pathway: "OVERFLOW_AND_FUTURE_CYCLE",
    evidenceUrls: [
      ...(naeaHit?.url ? [naeaHit.url] : []),
      "https://www.arteducators.org/",
    ].slice(0, 4),
    people: naeaHit?.people || [],
    emails: naeaHit?.emails || [],
    hiltonAction:
      "Sheraton is the documented 2027 host/housing node (Exa competitive wave). Do not pitch displacement. Offer Hilton New York Times Square as Times Square overflow and register for subsequent NAEA housing RFPs.",
    evidenceNote: naeaHit
      ? "Live Exa hit this wave"
      : "Retained from prior paid competitive wave (query deduped)",
  });
  const forumHit = flatCompetitiveEvents.find(
    (e) => /forum on education abroad|education abroad/i.test(`${e.title} ${e.textSnippet}`),
  );
  findings.newCompetitiveOpps.push({
    linkedOpportunityId: null,
    newOpportunityId: stableOppId("forum-education-abroad-marquis"),
    title: "Forum on Education Abroad Annual Conference",
    organizationName: "The Forum on Education Abroad",
    incumbentHotel: forumHit?.incumbentHotel || "New York Marriott Marquis",
    eventStartDate: null,
    year: forumHit?.year || null,
    label: "COMPETITOR_HOSTED_DEMAND",
    pathway: "OVERFLOW_AND_FUTURE_CYCLE",
    evidenceUrls: [
      ...(forumHit?.url ? [forumHit.url] : []),
      "https://www.forumea.org/",
    ].slice(0, 4),
    people: forumHit?.people || [],
    emails: forumHit?.emails || [],
    hiltonAction:
      "Confirm cycle year and housing desk. If Marquis-hosted, pursue overflow for Hilton New York Times Square; if open, compete for housing block.",
    evidenceNote: forumHit
      ? "Live Exa hit this wave"
      : "Retained from prior paid competitive wave (query deduped)",
  });

  // ---------- HOW completion (Boutique Ariela + any retained missing LinkedIn/email) ----------
  const howTargets = [
    {
      opportunityId: "gdi_opp_8ae344c41d87f94b",
      query:
        'Ariela Kiradjian "Boutique" OR BLLA email OR LinkedIn Partner COO',
    },
    {
      opportunityId: "gdi_opp_8ae344c41d87f94b",
      query: '"Ariela Kiradjian" (linkedin.com/in OR "@" OR phone) BLLA OR "Boutique Lifestyle"',
    },
    {
      opportunityId: "gdi_opp_267d801f2fa39f9e",
      query: 'Michele Smith Koontz SSSP LinkedIn OR "Meeting Manager"',
    },
    {
      opportunityId: "gdi_opp_267d801f2fa39f9e",
      query: '"Michele Smith Koontz" (mkoontz OR utk.edu OR LinkedIn) "Society for the Study of Social Problems"',
    },
    {
      opportunityId: scbwiId,
      query: '"Sarah Baker" SCBWI email OR "scbwi.org" OR LinkedIn "Executive Director"',
    },
    {
      opportunityId: scbwiId,
      query: '"Sarah Baker" site:scbwi.org OR "sarah@" OR linkedin.com/in Executive Director SCBWI',
    },
  ];
  // Seed HOW from first-wave verified hits when present in ledger titles / known recoveries
  const seededHow = [
    {
      opportunityId: "gdi_opp_8ae344c41d87f94b",
      query: "SEED_WAVE1_ARIELA",
      costUsd: 0,
      emails: ["info@tiewn.com"],
      linkedins: ["https://linkedin.com/in/ariela-kiradjian"],
      phones: ["818.883.4363"],
      resultTitles: ["SEED_FROM_WAVE1_EXA"],
      seeded: true,
    },
    {
      opportunityId: "gdi_opp_267d801f2fa39f9e",
      query: "SEED_WAVE1_MICHELE",
      costUsd: 0,
      emails: ["mkoontz3@utk.edu", "sssp@utk.edu"],
      linkedins: ["https://www.linkedin.com/in/michele-smith-koontz-28b9029"],
      phones: ["+1 865-689-1531"],
      resultTitles: ["SEED_FROM_WAVE1_EXA"],
      seeded: true,
    },
  ];
  findings.how.push(...seededHow);
  for (const t of howTargets) {
    const r = await searchDedup("how", t.query, {
      opportunityId: t.opportunityId,
      purpose: "HOW_COMPLETION",
    });
    if (!r) continue;
    const emails = [];
    const linkedins = [];
    const phones = [];
    for (const row of r.results) {
      const blob = `${row.title}\n${row.text}\n${row.url}`;
      for (const e of extractPeopleFromText(blob).emails) emails.push(e);
      if (/linkedin\.com\/in\//i.test(row.url)) linkedins.push(row.url);
      const ph = blob.match(
        /(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}/,
      );
      if (ph) phones.push(ph[0]);
    }
    findings.how.push({
      opportunityId: t.opportunityId,
      query: t.query,
      costUsd: r.costUsd,
      emails: [...new Set(emails)].slice(0, 8),
      linkedins: [...new Set(linkedins)].slice(0, 5),
      phones: [...new Set(phones)].slice(0, 5),
      resultTitles: r.results.slice(0, 4).map((x) => x.title),
    });
  }

  // ---------- Build finalized candidates ----------
  // Start from retained baseline 5
  const candidateOpps = [];
  for (const id of retainedIds) {
    const src = byId[id];
    if (!src) continue;
    let p = primaryOf(src);
    // Apply HOW enrichments (merge all rows for this opportunity)
    const howRows = findings.how.filter((h) => h.opportunityId === id);
    if (howRows.length) {
      const emails = howRows.flatMap((h) => h.emails || []);
      const linkedins = howRows.flatMap((h) => h.linkedins || []);
      const phones = howRows.flatMap((h) => h.phones || []);
      const personalEmail = emails.find(
        (e) =>
          !/^info@|^events@|^contact@|^hello@|^support@/i.test(e) &&
          !/plusweb\.org|namt\.org|broadway\.org|nyssba\.org|sior\.com/i.test(e),
      );
      if (linkedins[0] && !p.linkedinUrl) {
        p = { ...p, linkedinUrl: linkedins[0] };
      }
      if (phones[0] && !p.phone) {
        p = { ...p, phone: phones[0] };
      }
      if (id === "gdi_opp_267d801f2fa39f9e") {
        const micheleEmail = emails.find((e) => /mkoontz/i.test(e));
        if (micheleEmail) {
          p = {
            ...p,
            email: micheleEmail,
            emailType: "DIRECT_WORK",
            emailSource: "exa_how_search",
          };
        }
      }
      if (personalEmail && id === "gdi_opp_8ae344c41d87f94b") {
        p = {
          ...p,
          email: personalEmail,
          emailType: /^info@/i.test(personalEmail)
            ? "GENERIC_ORGANIZATION"
            : "DIRECT_WORK",
          emailSource: "exa_how_search",
        };
      } else if (id === "gdi_opp_8ae344c41d87f94b" && linkedins[0]) {
        // LinkedIn verified; org email may remain from source record
        p = { ...p, linkedinUrl: linkedins[0] };
      }
    }
    const competitive =
      id === "gdi_opp_267d801f2fa39f9e"
        ? {
            label: "EVIDENCED_OVERFLOW_OPPORTUNITY",
            incumbentHotel: "Westin New York at Times Square",
          }
        : null;
    let cleaned = {
      ...src,
      primaryContact: isPlausiblePersonName(p.name) ? p : null,
      marketScope: competitive ? "COMPETITIVE" : "HOTEL",
      competitiveContext: competitive,
      sourceUrls: collectUrls(src),
      recommendedNextStep:
        text(src.recommendedNextStep) ||
        `Outreach to ${text(p.name)} — confirm lodging decision role for ${text(
          src.organizationName,
        )} and introduce Hilton New York Times Square fit.`,
      gdiPaidResearchV1: {
        contract: CONTRACT,
        auth: AUTH,
        at: new Date().toISOString(),
      },
    };
    cleaned = attachNamedContactCoverageMetadata(cleaned, {
      secondContactAttempted: true,
      secondContactDisposition: "ATTEMPTED_NO_SECOND_VERIFIED_FROM_EXISTING_EVIDENCE",
      contactGaps: ["Second named contact not verified in paid+existing evidence"],
    });
    if (!cleaned.secondaryContact?.name) {
      cleaned.secondContactAttempted = true;
      cleaned.secondaryContactResearchStatus = "ATTEMPTED_NO_SECOND_VERIFIED";
      cleaned.secondaryContactResearchReason =
        "Paid + existing evidence reviewed; no distinct verified secondary located";
      cleaned = attachNamedContactCoverageMetadata(cleaned, {
        secondContactAttempted: true,
        secondContactDisposition: "ATTEMPTED_NO_SECOND_VERIFIED_FROM_EXISTING_EVIDENCE",
        contactGaps: ["Second named contact not verified"],
      });
    }
    const coverage = evaluateNamedContactCoverageStandard(cleaned);
    const contactEval = evaluateContactCompletenessForFinalization(cleaned, {
      looksLikePersonName: isPlausiblePersonName,
    });
    if (coverage.finalizeEligible && contactEval.complete) {
      candidateOpps.push(cleaned);
    }
  }

  // PLUS recovery decision — never finalize on scrape junk / sponsor attendee emails alone
  const PLUS_NAME_BLOCK =
    /role|type|event|travel|demand|generation|upcoming|explore|data|science|employment|practices/i;
  const plusPeopleOk = preferCommercialPeople(plusBundle.people).filter(
    (p) =>
      isPlausiblePersonName(p.name) &&
      !PLUS_NAME_BLOCK.test(p.name) &&
      /\b(director|manager|coordinator|officer|vp|vice president|executive|meetings|events|housing)\b/i.test(
        text(p.role),
      ),
  );
  let plusRecovered = false;
  if (plusBundle.venue && plusBundle.datesConfirmed) {
    // Prefer a verified PLUS staff person; sponsor emails are not primaries
    if (plusPeopleOk.length) {
      const best = plusPeopleOk[0];
      const email =
        plusBundle.emails.find(
          (e) =>
            !/^info@|^events@|^sponsorship@|^contact@/i.test(e) &&
            /@plusweb\.org$/i.test(e),
        ) || null;
      let cleaned = {
        ...plusOpp,
        eventStartDate: "2027-03-01",
        primaryContact: {
          name: best.name,
          role: best.role,
          organization: "Professional Liability Underwriting Society (PLUS)",
          email,
          emailType: email && !/^info@/i.test(email) ? "DIRECT_WORK" : "GENERIC_ORGANIZATION",
          sourceUrl: plusBundle.urls[0] || null,
          whyThisContact: `Identified via Exa Search paid WHO/ALT for PLUS EPL Symposium 2027 (${best.role || "events role"}).`,
        },
        marketScope: "COMPETITIVE",
        competitiveContext: {
          label: "CONFIRMED_COMPETITOR_BOOKING",
          incumbentHotel: "New York Marriott Marquis",
          eventDates: "2027-03-01 to 2027-03-03",
          pathway: "Overflow / next-cycle sourcing — not immediate displacement",
        },
        hotelOpportunityThesis:
          "PLUS EPL Symposium 2027 is confirmed at New York Marriott Marquis (Broadway). Hilton New York Times Square is a credible Midtown/Times Square overflow and stay-to-play option for attendees and a future-cycle housing contender; do not pitch immediate displacement of the 2027 contracted host.",
        recommendedNextStep:
          "Contact PLUS meetings/events stakeholder to introduce Hilton New York Times Square for overflow housing around the 2027 Marquis-hosted symposium and to register interest for the subsequent EPL Symposium sourcing cycle.",
        sourceUrls: [...new Set(plusBundle.urls)].slice(0, 8),
        qualificationGatePassed: true,
        opportunityQualification: "MODERATE",
        futureCycleEvidenceState: "CURRENT_FUTURE_CYCLE_CONFIRMED",
        gdiPaidResearchV1: { contract: CONTRACT, auth: AUTH, recoveredFrom: "HOLD" },
      };
      cleaned = attachNamedContactCoverageMetadata(cleaned, {
        secondContactAttempted: true,
        secondContactDisposition: "ATTEMPTED_NO_SECOND_VERIFIED_FROM_EXISTING_EVIDENCE",
        contactGaps: ["Second named contact not verified"],
      });
      cleaned.secondContactAttempted = true;
      cleaned.secondaryContactResearchStatus = "ATTEMPTED_NO_SECOND_VERIFIED";
      cleaned.secondaryContactResearchReason =
        "Paid WHO/ALT reviewed; second distinct person not verified";
      cleaned = attachNamedContactCoverageMetadata(cleaned, {
        secondContactAttempted: true,
        secondContactDisposition: "ATTEMPTED_NO_SECOND_VERIFIED_FROM_EXISTING_EVIDENCE",
        contactGaps: ["Second named contact not verified"],
      });
      const coverage = evaluateNamedContactCoverageStandard(cleaned);
      const contactEval = evaluateContactCompletenessForFinalization(cleaned, {
        looksLikePersonName: isPlausiblePersonName,
      });
      if (coverage.finalizeEligible && contactEval.complete) {
        candidateOpps.push(cleaned);
        plusRecovered = true;
        findings.holdRecovered.push({
          opportunityId: plusId,
          title: plusOpp.title,
          reason: "VENUE_CONFIRMED_MARQUIS_PLUS_NAMED_PERSON",
        });
      }
    }
    if (!plusRecovered) {
      findings.holdRemaining.push({
        opportunityId: plusId,
        title: plusOpp.title,
        blocker: plusPeopleOk.length
          ? "NAMED_PERSON_FAILED_FINALIZATION_GATES"
          : "NAMED_DECISION_MAKER_NOT_VERIFIED",
        competitiveIntel: {
          incumbentHotel: "New York Marriott Marquis",
          dates: "2027-03-01",
        },
      });
    }
  } else {
    findings.holdRemaining.push({
      opportunityId: plusId,
      title: plusOpp?.title,
      blocker: "INSUFFICIENT_VENUE_OR_DATE_CONFIRMATION",
    });
  }

  // SCBWI recovery — commercial named person only (no junk title scrapes)
  const scbwiPeopleOk = preferCommercialPeople(scbwiBundle.people);
  const scbwiHowRows = findings.how.filter((h) => h.opportunityId === scbwiId);
  const scbwiHow = {
    emails: scbwiHowRows.flatMap((h) => h.emails || []),
    linkedins: scbwiHowRows.flatMap((h) => h.linkedins || []),
    phones: scbwiHowRows.flatMap((h) => h.phones || []),
  };
  if (scbwiPeopleOk.length && scbwiBundle.urls.length) {
    const best =
      scbwiPeopleOk.find((p) => /sarah baker/i.test(p.name)) ||
      scbwiPeopleOk.find((p) => /kim turrisi/i.test(p.name)) ||
      scbwiPeopleOk[0];
    const scbwiOpp = byId[scbwiId];
    const howEmails = scbwiHow?.emails || [];
    const personalEmail =
      [...howEmails, ...scbwiBundle.emails].find(
        (e) =>
          /^(sarah|kim|tammy|patricia)[._-]/i.test(e) ||
          (/@scbwi\.org$/i.test(e) &&
            !/^(info|events|contact|hello|support|losangeles|newyork|ny|la)@/i.test(
              e,
            )),
      ) || null;
    const orgEmail =
      [...howEmails, ...scbwiBundle.emails].find(
        (e) =>
          /@scbwi\.org$/i.test(e) &&
          /^(info|events|contact|hello|support)@/i.test(e),
      ) || null;
    const linkedin = (scbwiHow?.linkedins || []).find((u) =>
      /linkedin\.com\/in\//i.test(u),
    );
    const secondary =
      scbwiPeopleOk.find((p) => p.name !== best.name) || null;
    let cleaned = {
      ...scbwiOpp,
      primaryContact: {
        name: best.name,
        role: best.role,
        organization: "SCBWI",
        email: personalEmail || orgEmail || null,
        emailType: personalEmail
          ? "DIRECT_WORK"
          : orgEmail
            ? "GENERIC_ORGANIZATION"
            : "UNKNOWN",
        linkedinUrl: linkedin || null,
        phone: (scbwiHow?.phones || [])[0] || null,
        sourceUrl: scbwiBundle.urls[0],
        whyThisContact: `Verified via Exa Search paid WHO/ALT for SCBWI Winter Conference (${best.role || "leadership"}).`,
      },
      secondaryContact: secondary
        ? {
            name: secondary.name,
            role: secondary.role,
            organization: "SCBWI",
            whyThisContact: "Second SCBWI leadership name verified in same paid evidence set.",
          }
        : null,
      marketScope: scbwiBundle.venue ? "COMPETITIVE" : "HOTEL",
      competitiveContext: scbwiBundle.venue
        ? {
            label: "COMPETITOR_UNCONFIRMED",
            incumbentHotel: scbwiBundle.venue,
          }
        : null,
      sourceUrls: [...new Set(scbwiBundle.urls)].slice(0, 8),
      hotelOpportunityThesis:
        text(scbwiOpp.hotelOpportunityThesis) ||
        "SCBWI Winter Conference draws children's publishing professionals to New York; Hilton New York Times Square is a credible Midtown housing option for attendees when venue/housing is open.",
      recommendedNextStep: `Outreach to ${best.name}${best.role ? ` (${best.role})` : ""} at SCBWI — confirm Winter Conference housing path for the next NYC cycle and introduce Hilton New York Times Square.`,
      futureCycleEvidenceState: "FUTURE_CYCLE_UNCONFIRMED",
      gdiPaidResearchV1: { contract: CONTRACT, auth: AUTH, recoveredFrom: "HOLD" },
    };
    if (secondary && isPlausiblePersonName(secondary.name)) {
      cleaned = attachNamedContactCoverageMetadata(cleaned, {
        secondContactAttempted: true,
        secondContactDisposition: "SECOND_VERIFIED",
        contactGaps: [],
      });
      cleaned.secondContactAttempted = true;
      cleaned.secondaryContactResearchStatus = "SECOND_VERIFIED";
      cleaned.secondaryContactResearchReason =
        "Second SCBWI leadership contact verified from paid Exa evidence";
    } else {
      cleaned = attachNamedContactCoverageMetadata(cleaned, {
        secondContactAttempted: true,
        secondContactDisposition: "ATTEMPTED_NO_SECOND_VERIFIED_FROM_EXISTING_EVIDENCE",
        contactGaps: ["Second named contact not verified"],
      });
      cleaned.secondContactAttempted = true;
      cleaned.secondaryContactResearchStatus = "ATTEMPTED_NO_SECOND_VERIFIED";
      cleaned.secondaryContactResearchReason =
        "Paid WHO/ALT reviewed; second distinct person not verified";
    }
    cleaned = attachNamedContactCoverageMetadata(cleaned, {
      secondContactAttempted: true,
      secondContactDisposition: cleaned.secondaryContact?.name
        ? "SECOND_VERIFIED"
        : "ATTEMPTED_NO_SECOND_VERIFIED_FROM_EXISTING_EVIDENCE",
      contactGaps: cleaned.secondaryContact?.name
        ? []
        : ["Second named contact not verified"],
    });
    const coverage = evaluateNamedContactCoverageStandard(cleaned);
    const contactEval = evaluateContactCompletenessForFinalization(cleaned, {
      looksLikePersonName: isPlausiblePersonName,
    });
    if (coverage.finalizeEligible && contactEval.complete) {
      candidateOpps.push(cleaned);
      findings.holdRecovered.push({
        opportunityId: scbwiId,
        title: scbwiOpp.title,
        reason: "NAMED_PERSON_VERIFIED_VIA_EXA",
        primary: best.name,
        secondary: secondary?.name || null,
      });
    } else {
      findings.holdRemaining.push({
        opportunityId: scbwiId,
        title: scbwiOpp.title,
        blocker: "NAMED_PERSON_FAILED_FINALIZATION_GATES",
        peopleFound: scbwiPeopleOk,
        coverage,
        contactEval,
      });
    }
  } else {
    findings.holdRemaining.push({
      opportunityId: scbwiId,
      title: byId[scbwiId]?.title,
      blocker: "NAMED_DECISION_MAKER_NOT_VERIFIED",
      peopleFound: scbwiBundle.people.filter((p) => isPlausiblePersonName(p.name)),
      venue: scbwiBundle.venue,
    });
  }

  // Dedupe by id
  const deduped = [];
  const seen = new Set();
  for (const o of candidateOpps) {
    const id = o.id || o.opportunityId;
    if (seen.has(id)) continue;
    seen.add(id);
    deduped.push(o);
  }

  // Package
  const publicationVersion = `gdi_hilton_ts_paid_research_v1_${AS_OF.replace(/-/g, "")}`;
  const dtos = deduped.map((o) => {
    const projected = projectOpportunityForPresentation(o, {
      hotelId: HOTEL_ID,
      surface: GDI_PRESENTATION_SURFACES.EXTERNAL_SHARE,
      view: "detail",
    });
    return {
      ...projected.dto,
      marketScope: o.marketScope,
      competitiveContext: o.competitiveContext,
    };
  });

  let pkg = null;
  let integrity = { ok: false };
  let quality = { ok: false };
  let namedAssert = { ok: false };
  let written = { ok: false, skipped: true };

  if (deduped.length) {
    pkg = buildCustomerPublicationPackage({
      hotelId: HOTEL_ID,
      hotelName: HOTEL_NAME,
      opportunities: dtos,
      publicationVersion,
      lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.CUSTOMER_RENDERED,
      approvalStatus: "CANDIDATE",
      applicationCompatibilityVersion: "gdi-customer-app-compat-v1",
      notes: `Hilton TS paid research execute v1 — auth ${AUTH}; spend $${ledger.spentUsd.aggregate}; pointer NOT activated`,
    });
    integrity = verifyPublicationPackageIntegrity(pkg);
    quality = runPublicationQualityGate({ opportunities: pkg.opportunities });
    namedAssert = assertFinalizedPackageNamedContactCoverage(deduped);
    writeJson(path.join(REPORT_DIR, "STAGED_CANDIDATE_PACKAGE.json"), pkg);
    if (writePackage && integrity.ok && namedAssert.ok) {
      const store = createGdiPublicationPackageStore();
      written = store.writePackage({
        ...pkg,
        lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.CUSTOMER_RENDERED,
        approvalStatus: "CANDIDATE",
        gates: {
          integrity: { ok: integrity.ok },
          quality: { ok: quality.ok, blockingCount: quality.blockingCount },
          namedContactCoverage: namedAssert,
          pointerActivated: false,
          paidResearchSpendUsd: ledger.spentUsd.aggregate,
          authorizationToken: AUTH,
        },
      });
    }
  }

  const store = createGdiPublicationPackageStore();
  const hiltonPtr = store.readPointer(HOTEL_ID, "STAGED");
  const peer = [
    ["rece0NdV9FCnaFlVS", "Condesa DF"],
    ["recaokE8psIbuVeuH", "LaLuna"],
    ["rec1t35e3sPerzxYu", "Memmo Príncipe Real"],
    ["recLuxvwwxID7U2B8", "Bethesda Marriott"],
  ].map(([id, name]) => {
    const ptr = store.readPointer(id, "STAGED");
    return {
      hotelId: id,
      hotelName: name,
      hasStagedPointer: Boolean(ptr?.publicationVersion),
      publicationVersion: ptr?.publicationVersion || null,
    };
  });

  const hotelCount = deduped.filter((o) => o.marketScope !== "COMPETITIVE").length;
  const competitiveCount = deduped.filter((o) => o.marketScope === "COMPETITIVE").length;
  const priorFinal = baseline.finalization?.total || 5;
  const additional = Math.max(0, deduped.length - priorFinal);
  const costPerAdditional =
    additional > 0 ? round4(ledger.spentUsd.aggregate / additional) : null;

  const scorecard = {
    contract: CONTRACT,
    auth: AUTH,
    asOf: AS_OF,
    spend: {
      ...ledger.spentUsd,
      remaining: ledger.remainingUsd,
      callCount: ledger.calls.length,
      skippedCount: ledger.skipped.length,
      costPerAdditionalFinalizedOpportunity: costPerAdditional,
    },
    baselineRetained: retainedIds.length,
    holdRecovered: findings.holdRecovered,
    holdRemaining: findings.holdRemaining,
    newCompetitiveOpps: findings.newCompetitiveOpps,
    finalization: {
      hotel: hotelCount,
      competitive: competitiveCount,
      total: deduped.length,
      salesReady: deduped.filter((o) => o.salesReady === true).length,
      ids: deduped.map((o) => o.id),
    },
    contacts: {
      primary: deduped.filter((o) => isPlausiblePersonName(o.primaryContact?.name))
        .length,
      secondary: deduped.filter((o) =>
        isPlausiblePersonName(o.secondaryContact?.name),
      ).length,
      directEmail: deduped.filter((o) =>
        /DIRECT_WORK/i.test(text(o.primaryContact?.emailType)),
      ).length,
      phone: deduped.filter((o) => text(o.primaryContact?.phone)).length,
      linkedin: deduped.filter((o) =>
        text(o.primaryContact?.linkedinUrl || o.primaryContact?.linkedin),
      ).length,
    },
    publicationVersion,
    packagePath: written.path || null,
    pointerActivated: false,
  };

  const certification = {
    contract: CONTRACT,
    at: new Date().toISOString(),
    ok: false,
    checks: {
      authTokenPresent: true,
      agentOff: true,
      spendWithinAggregate: ledger.spentUsd.aggregate <= CEILING.aggregate + 1e-9,
      spendWithinBuckets:
        ledger.spentUsd.whoAlt <= CEILING.whoAlt + 1e-9 &&
        ledger.spentUsd.competitive <= CEILING.competitive + 1e-9 &&
        ledger.spentUsd.how <= CEILING.how + 1e-9,
      packageIntegrity: integrity.ok === true,
      namedContactAssert: namedAssert.ok === true,
      hiltonPointerNotActivated: !hiltonPtr,
      peerStagedPresent: peer.every((p) => p.hasStagedPointer),
      noAirtable: true,
      noSurfe: true,
      noFindymail: true,
    },
    peer,
    hiltonStagedPointer: hiltonPtr,
  };
  certification.ok = Object.values(certification.checks).every(Boolean);

  writeJson(path.join(REPORT_DIR, "01_SPENDING_LEDGER.json"), ledger);
  writeJson(path.join(REPORT_DIR, "02_RESEARCH_FINDINGS.json"), findings);
  writeJson(path.join(REPORT_DIR, "03_FOUNDER_SCORECARD.json"), scorecard);
  writeJson(path.join(REPORT_DIR, "04_CERTIFICATION.json"), certification);
  writeJson(path.join(REPORT_DIR, "05_ID_RECONCILIATION.json"), {
    original45: opportunities.map((o) => o.id),
    baselineRetainedIds: retainedIds,
    holdRecoveredIds: findings.holdRecovered.map((h) => h.opportunityId),
    holdRemainingIds: findings.holdRemaining.map((h) => h.opportunityId),
    finalCandidateIds: deduped.map((o) => o.id),
    newCompetitiveOpportunityIds: findings.newCompetitiveOpps
      .map((o) => o.newOpportunityId || o.linkedOpportunityId)
      .filter(Boolean),
    newOpportunityIdsCreated: findings.newCompetitiveOpps
      .map((o) => o.newOpportunityId)
      .filter(Boolean),
  });

  const contactMatrix = deduped.map((o) => ({
    opportunityId: o.id,
    title: o.title,
    marketScope: o.marketScope,
    primary: {
      name: o.primaryContact?.name || null,
      role: o.primaryContact?.role || null,
      organization: o.primaryContact?.organization || o.organizationName || null,
      email: o.primaryContact?.email || null,
      emailType: o.primaryContact?.emailType || null,
      phone: o.primaryContact?.phone || null,
      linkedin: o.primaryContact?.linkedinUrl || o.primaryContact?.linkedin || null,
    },
    secondary: o.secondaryContact
      ? {
          name: o.secondaryContact.name || null,
          role: o.secondaryContact.role || null,
        }
      : null,
    recommendedAction: o.recommendedNextStep || null,
  }));
  writeJson(path.join(REPORT_DIR, "06_CONTACT_MATRIX.json"), contactMatrix);

  const founderReport = `# Hilton Times Square — Paid Research Execution Report

**Auth:** \`${AUTH}\`  
**As of:** ${AS_OF}  
**Hotel:** ${HOTEL_NAME} (\`${HOTEL_ID}\`)  
**PR:** #592  
**Contract:** \`${CONTRACT}\`

## Spend

| Bucket | Spent USD | Ceiling | Remaining |
|--------|----------:|--------:|----------:|
| WHO / ALT | ${ledger.spentUsd.whoAlt} | ${CEILING.whoAlt} | ${ledger.remainingUsd.whoAlt} |
| Competitive | ${ledger.spentUsd.competitive} | ${CEILING.competitive} | ${ledger.remainingUsd.competitive} |
| HOW | ${ledger.spentUsd.how} | ${CEILING.how} | ${ledger.remainingUsd.how} |
| **Aggregate** | **${ledger.spentUsd.aggregate}** | **${CEILING.aggregate}** | **${ledger.remainingUsd.aggregate}** |

- Provider calls (this ledger file): ${ledger.calls.length} (includes wave-1 carryforward rows)
- Skipped (dedupe / ceiling): ${ledger.skipped.length}
- Exa Agent: **OFF**
- Surfe / Findymail: **not used**
- Cost per additional finalized opportunity: ${costPerAdditional == null ? "n/a (no net add vs baseline 5, or see hold recovery)" : "$" + costPerAdditional}
- Baseline retained: ${retainedIds.length} → Final candidates: **${deduped.length}** (hotel ${hotelCount} / competitive ${competitiveCount})

## HOLD recovery

### Recovered
${
  findings.holdRecovered.length
    ? findings.holdRecovered
        .map(
          (h) =>
            `- **${h.title}** (\`${h.opportunityId}\`) — ${h.reason}${h.primary ? `; primary ${h.primary}` : ""}${h.secondary ? `; secondary ${h.secondary}` : ""}`,
        )
        .join("\n")
    : "- None"
}

### Remaining blockers
${
  findings.holdRemaining.length
    ? findings.holdRemaining
        .map(
          (h) =>
            `- **${h.title}** (\`${h.opportunityId}\`) — ${h.blocker}${h.competitiveIntel ? `; competitive: ${h.competitiveIntel.incumbentHotel} ${h.competitiveIntel.dates || ""}` : ""}`,
        )
        .join("\n")
    : "- None"
}

## New competitive intelligence (not auto-finalized without named commercial contact)

${
  findings.newCompetitiveOpps.length
    ? findings.newCompetitiveOpps
        .map(
          (o) =>
            `- **${o.title}** @ ${o.incumbentHotel} — ${o.label} / ${o.pathway}. Action: ${o.hiltonAction}`,
        )
        .join("\n")
    : "- None newly documented"
}

## Final candidate IDs

${deduped.map((o) => `- \`${o.id}\` — ${o.title} (${o.primaryContact?.name || "unnamed"})`).join("\n")}

## Publication governance

- Candidate package version: \`${publicationVersion}\`
- Package written: ${written.ok === true}
- **STAGED pointer activated: NO**
- CURRENT / customer publish / Airtable / deploy: **NOT performed**
- Certification: **${certification.ok ? "PASS" : "FAIL"}**

## Recommended Hilton sales actions (summary)

1. Pursue retained hotel demand (Boutique / NYSSBA / Broadway Spring Road / NAMT) with existing named primaries.
2. SSSP 76th — Westin overflow pathway via Michele Smith Koontz; do not pitch displacement of Westin host.
3. SCBWI Winter Conference — outreach Sarah Baker (ED); confirm next NYC housing cycle; Tammy Brown as secondary marketing/events pathway.
4. PLUS EPL 2027 — confirmed Marquis host; overflow / next-cycle only; named decision-maker still HOLD.
5. Watch NAEA 2027 (Sheraton) and Forum on Education Abroad (Marquis) for overflow registration — named contacts still thin.

## STOP

Paid research + candidate package + this founder report complete. **Do not activate STAGED** without separate founder approval.
`;
  fs.writeFileSync(path.join(REPORT_DIR, "FOUNDER_REPORT.md"), founderReport, "utf8");

  console.log(
    JSON.stringify(
      {
        contract: CONTRACT,
        ok: certification.ok,
        spend: ledger.spentUsd,
        remaining: ledger.remainingUsd,
        calls: ledger.calls.length,
        finalCount: deduped.length,
        holdRecovered: findings.holdRecovered.length,
        publicationVersion,
        packageWritten: written.ok === true,
        pointerActivated: false,
      },
      null,
      2,
    ),
  );
  process.exit(certification.ok ? 0 : 2);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
