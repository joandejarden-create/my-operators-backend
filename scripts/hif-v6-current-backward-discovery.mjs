#!/usr/bin/env node
/**
 * HIF V6 — CURRENT_BACKWARD discovery pass.
 * Starts from contemporary commercial signals (8-K / management discovery corpus),
 * then classifies toward franchise/management opportunity without inventing dates.
 * CUSTOMER_BLOCKED. $0 free SEC sources.
 *
 * Usage: node scripts/hif-v6-current-backward-discovery.mjs --run [--max-docs=15]
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import { createSecEdgarClient } from "../lib/hotel-intelligence/sec-edgar/edgar-client-v1.js";
import { annotateResearchFreshness, RESEARCH_DIRECTION } from "../lib/hotel-intelligence/sec-edgar/hif-v6-research-direction-freshness-v6.js";
import { classifyPhysicalGeography } from "../lib/hotel-intelligence/sec-edgar/hif-v6-geographic-confidence-v6.js";
import { deriveNextCommercialDecisionWindow } from "../lib/hotel-intelligence/sec-edgar/hif-v6-commercial-timing-model-v6.js";
import { classifyCommercialActionability } from "../lib/hotel-intelligence/sec-edgar/hif-v6-commercial-actionability-v6.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) dotenv.config({ path: p, override: false });
}

const OUT = path.join(ROOT, "reports/sec-edgar-hif-v6-current-backward-discovery");
const RUN = process.argv.includes("--run");
const MAX_DOCS = Number((process.argv.find((a) => a.startsWith("--max-docs=")) || "").split("=")[1]) || 15;
const asOf = new Date().toISOString().slice(0, 10);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const writeJson = (fp, obj) => {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify(obj, null, 2), "utf8");
};
const load = (rel) => {
  const fp = path.join(ROOT, rel);
  if (!fs.existsSync(fp)) return null;
  return JSON.parse(fs.readFileSync(fp, "utf8"));
};

const SIGNAL_PATTERNS = [
  { id: "BRAND_EXIT", re: /terminat(?:ed|ion|es)?\s+(?:the\s+)?(?:franchise|brand)|ceased\s+(?:to\s+)?operat(?:e|ing)\s+as|reflag(?:ged|ging)|brand\s+conversion|disaffiliat/i },
  { id: "FRANCHISE_NONRENEWAL", re: /non[- ]?renew(?:al|ed)|will\s+not\s+renew|elected\s+not\s+to\s+renew/i },
  { id: "MGMT_DEPARTURE", re: /manag(?:ement|er)\s+(?:agreement\s+)?(?:terminat|expir)|replaced\s+(?:as\s+)?(?:hotel\s+)?manager|new\s+(?:hotel\s+)?manager/i },
  { id: "OPERATOR_SEARCH", re: /seeking\s+(?:a\s+)?(?:new\s+)?(?:operator|manager)|operator\s+selection|RFP\s+for\s+(?:hotel\s+)?manag/i },
  { id: "OWNERSHIP_CHANGE", re: /acquired\s+the\s+(?:hotel|property)|sale\s+of\s+the\s+(?:hotel|property)|disposed\s+of\s+the\s+(?:hotel|property)/i },
  { id: "RESTRUCTURING", re: /foreclosure|receivership|debtor[- ]in[- ]possession|restructuring/i },
];

function decode(html) {
  return String(html || "")
    .replace(/&nbsp;/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function isNoisyHotelHint(name) {
  return /agreement|between|operator|international\b|portfolio|franchise agreement|relicensing|maturity|related mortgage|exhibit|schedule/i.test(
    name,
  );
}

function extractHotelHints(plain) {
  const names = new Set();
  const re =
    /\b((?:Hampton Inn|Holiday Inn(?: Express)?|Courtyard by Marriott|Residence Inn|Hilton Garden Inn|Homewood Suites|Home2 Suites|SpringHill Suites|TownePlace Suites|Candlewood Suites|Comfort Inn|Quality Inn|Best Western|La Quinta|Embassy Suites|DoubleTree|Crowne Plaza|Staybridge Suites|Fairfield Inn)[^,.]{0,50})/gi;
  let m;
  while ((m = re.exec(plain)) && names.size < 40) {
    const n = m[1].replace(/\s+/g, " ").trim();
    if (n.length >= 10 && n.length <= 70 && !isNoisyHotelHint(n)) names.add(n);
  }
  return [...names];
}

function classifySignals(plain) {
  const hits = [];
  for (const p of SIGNAL_PATTERNS) {
    if (p.re.test(plain)) hits.push(p.id);
  }
  return hits;
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });

  // Seed from prior management discovery docs + fresh 8-K full-text search
  const priorDocs =
    load("reports/sec-edgar-hif-v6-management-discovery/DOC_RESULTS.json")?.docs ||
    load("reports/sec-edgar-hif-v6-management-discovery/DOC_RESULTS.json")?.results ||
    [];
  const priorUrls = [
    ...new Set(
      priorDocs
        .map((d) => d.sourceUrl || d.url || d.documentUrl)
        .filter(Boolean),
    ),
  ].slice(0, Math.max(4, Math.floor(MAX_DOCS / 2)));

  if (!RUN) {
    console.log(JSON.stringify({ dryRun: true, priorUrls: priorUrls.length, maxDocs: MAX_DOCS }, null, 2));
    return;
  }

  const edgar = createSecEdgarClient();
  const UA = "Dealality Research joan@aohospitalityadvisors.com";
  async function efts(q, forms = "8-K", size = 8) {
    const p = new URLSearchParams({
      q,
      dateRange: "custom",
      startdt: "2024-01-01",
      enddt: asOf,
      from: "0",
      size: String(size),
      forms,
    });
    const r = await fetch(`https://efts.sec.gov/LATEST/search-index?${p}`, {
      headers: { "User-Agent": UA, Accept: "application/json" },
    });
    if (!r.ok) return [];
    const j = await r.json();
    return (j.hits?.hits || []).map((h) => {
      const src = h._source || {};
      const adsh = String(src.adsh || "").replace(/-/g, "");
      const fileName = String(h._id || "").split(":").pop();
      const cik = String((src.ciks || [])[0] || "").replace(/^0+/, "");
      if (!cik || !adsh || !fileName) return null;
      return {
        url: `https://www.sec.gov/Archives/edgar/data/${cik}/${adsh}/${fileName}`,
        fileDate: src.file_date,
        query: q,
      };
    }).filter(Boolean);
  }

  const queries = [
    `"hotel" "franchise" "non-renewal"`,
    `"hotel" "management agreement" terminated`,
    `hotel reflagged OR "brand conversion" hotel`,
  ];
  const searchHits = [];
  for (const q of queries) {
    try {
      searchHits.push(...(await efts(q)));
    } catch {
      // continue with prior corpus
    }
    await sleep(200);
  }

  const urls = [...new Set([...priorUrls, ...searchHits.map((s) => s.url).filter(Boolean)])].slice(0, MAX_DOCS);
  const events = [];
  const docResults = [];

  for (const url of urls) {
    const res = await edgar.request(url, { accept: "text/html,text/plain,*/*", maxAgeMs: 7 * 24 * 3600 * 1000 });
    const plain = res.ok ? decode(res.text || "").slice(0, 120000) : "";
    const signals = plain ? classifySignals(plain) : [];
    const hotels = plain ? extractHotelHints(plain) : [];
    docResults.push({
      sourceUrl: url,
      ok: res.ok,
      signals,
      hotelHints: hotels.length,
      researchDirection: RESEARCH_DIRECTION.CURRENT_BACKWARD,
    });

    if (!signals.length) {
      await sleep(100);
      continue;
    }

    // A sale/foreclosure/renovation alone does not create opportunity — require brand/mgmt signal or hold
    const commercialRelevant = signals.some((s) =>
      ["BRAND_EXIT", "FRANCHISE_NONRENEWAL", "MGMT_DEPARTURE", "OPERATOR_SEARCH"].includes(s),
    );

    for (const hotelName of hotels.slice(0, 8)) {
      const row = {
        hotelName,
        hotelCountry: "US",
        sourceUrl: url,
        sourceFamily: "SEC_8K",
        wave: "CURRENT_BACKWARD",
        discoveryMethod: "CURRENT_BACKWARD",
        commercialSignals: signals,
        // Keyword hits are signals only — do not set activeCommercialTrigger
        // (that would invent EVIDENCE_INFERRED NOW windows without dates).
        activeCommercialTrigger: false,
        agreementType: "UNKNOWN",
        agreementTypeResolved: false,
        commercialList: "CONTRACT_DATE_AGREEMENT_TYPE_UNRESOLVED",
        signalFamilyHint: commercialRelevant
          ? signals.includes("MGMT_DEPARTURE") || signals.includes("OPERATOR_SEARCH")
            ? "MANAGEMENT"
            : "FRANCHISE"
          : null,
        fileDate: asOf,
        expirationDate: null,
        noticeDeadline: null,
      };
      const geo = classifyPhysicalGeography(row);
      // Undated contemporary signals stay TIMING_UNKNOWN — do not invent notice deadlines.
      const timing = deriveNextCommercialDecisionWindow(row, asOf);
      const act = classifyCommercialActionability(row, timing, geo);
      const fresh = annotateResearchFreshness(row, asOf);
      events.push({
        ...row,
        ...geo,
        ...timing,
        ...act,
        ...fresh,
        researchDirection: RESEARCH_DIRECTION.CURRENT_BACKWARD,
        qualificationNote: commercialRelevant
          ? "Signal-backed candidate — ownership/sale-only signals excluded from auto-qualify"
          : "Non-qualifying ownership/restructuring context only — HOLD",
      });
    }
    await sleep(150);
  }

  // Also fold prior management-discovery incremental as CURRENT_BACKWARD provenance
  const priorJ = load("reports/sec-edgar-hif-v6-management-discovery/INCREMENTAL.json");
  const priorInc = priorJ?.events || priorJ?.rows || priorJ?.hotels || [];
  for (const ev of priorInc.slice(0, 200)) {
    if (!ev?.hotelName || isNoisyHotelHint(ev.hotelName)) continue;
    events.push({
      ...ev,
      researchDirection: RESEARCH_DIRECTION.CURRENT_BACKWARD,
      discoveryMethod: "CURRENT_BACKWARD",
      wave: ev.wave || "MGMT_DISCOVERY_FOLDED",
      ...annotateResearchFreshness({ ...ev, wave: "MGMT_DISCOVERY", discoveryMethod: "CURRENT_BACKWARD" }, asOf),
    });
  }

  const metrics = {
    wave: "CURRENT_BACKWARD",
    asOf,
    publicationState: "CUSTOMER_BLOCKED",
    researchCostUsd: 0,
    documentsAttempted: urls.length,
    documentsOk: docResults.filter((d) => d.ok).length,
    documentsWithSignals: docResults.filter((d) => d.signals?.length).length,
    incrementalEvents: events.length,
    promotedPriorityA: events.filter((e) => e.commercialPriority === "PRIORITY_A").length,
    priorityB: events.filter((e) => e.commercialPriority === "PRIORITY_B").length,
    hold: events.filter((e) => e.commercialPriority === "HOLD").length,
    note: "Sale/foreclosure/renovation alone does not establish franchise or management opportunity",
  };

  writeJson(path.join(OUT, "DOC_RESULTS.json"), { count: docResults.length, results: docResults });
  writeJson(path.join(OUT, "INCREMENTAL.json"), { count: events.length, events });
  writeJson(path.join(OUT, "METRICS.json"), metrics);
  console.log(JSON.stringify(metrics, null, 2));
}

main().catch((e) => {
  console.error(String(e?.stack || e));
  process.exit(1);
});
