#!/usr/bin/env node
/**
 * HIF V6 — Deep audit of Priority B (≤24m) candidates.
 * Revalidate identity/geo, seek notice/type evidence, disposition each record.
 * Does NOT auto-promote expiration-only to Priority A. CUSTOMER_BLOCKED. $0.
 *
 * Usage: node scripts/hif-v6-priority-b-recovery-audit.mjs --run [--max=92]
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import { createSecEdgarClient } from "../lib/hotel-intelligence/sec-edgar/edgar-client-v1.js";
import { deriveNextCommercialDecisionWindow } from "../lib/hotel-intelligence/sec-edgar/hif-v6-commercial-timing-model-v6.js";
import { classifyPhysicalGeography, GEO_CLASS } from "../lib/hotel-intelligence/sec-edgar/hif-v6-geographic-confidence-v6.js";
import { COMMERCIAL_PRIORITY } from "../lib/hotel-intelligence/sec-edgar/hif-v6-commercial-actionability-v6.js";
import { annotateResearchFreshness } from "../lib/hotel-intelligence/sec-edgar/hif-v6-research-direction-freshness-v6.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) dotenv.config({ path: p, override: false });
}

const OUT = path.join(ROOT, "reports/sec-edgar-hif-v6-priority-b-recovery");
const RUN = process.argv.includes("--run");
const MAX = Number((process.argv.find((a) => a.startsWith("--max=")) || "").split("=")[1]) || 92;
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

function isNoisyName(name) {
  const n = String(name || "");
  return /maturity|turity date|related mortgage|loan number|cut-?off|appraised/i.test(n) || n.split(/\s+/).length > 14;
}

function decode(html) {
  return String(html || "")
    .replace(/&nbsp;/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Document-level probes for notice / franchisor / manager near hotel name.
 */
function probeDocument(text, hotelName) {
  const plain = decode(text);
  const needle = String(hotelName || "").replace(/\s+/g, " ").trim();
  const idx = needle.length >= 6 ? plain.toLowerCase().indexOf(needle.toLowerCase()) : -1;
  const window =
    idx >= 0
      ? plain.slice(Math.max(0, idx - 200), Math.min(plain.length, idx + needle.length + 600))
      : plain.slice(0, 4000);

  const hasNotice =
    /notice\s+(?:of\s+)?(?:non[- ]?renewal|termination)|non[- ]?renewal\s+notice|termination\s+notice|days['’]?\s+prior\s+(?:written\s+)?notice/i.test(
      window,
    ) || /notice\s+(?:of\s+)?(?:non[- ]?renewal|termination)|non[- ]?renewal\s+notice/i.test(plain.slice(0, 8000));
  const hasFranchisor = /franchisor|franchise brand|\bflag\b|brand affiliation/i.test(window);
  const hasManager = /\bmanager\b|management company|\boperator\b|\bhma\b/i.test(window);
  const renewal = /automatic renewal|renewal option|extension option/i.test(window);
  const termination = /terminat(?:e|ion|es|ed)/i.test(window);
  const combinedColumn = /related license\/\s*franchise|operating agreement or management/i.test(plain.slice(0, 5000));

  let agreementHint = "UNRESOLVED";
  if (hasFranchisor && hasManager) agreementHint = "MULTIPLE";
  else if (hasFranchisor && !hasManager) agreementHint = "FRANCHISE";
  else if (hasManager && !hasFranchisor) agreementHint = "MANAGEMENT";
  else if (combinedColumn) agreementHint = "UNRESOLVED";

  // Explicit notice deadline date near hotel (rare)
  let noticeDeadline = null;
  const noticeDate = window.match(
    /notice[^.]{0,80}?((?:\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})|(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+20\d{2})/i,
  );
  if (noticeDate) {
    const raw = noticeDate[1];
    const mdy = raw.match(/(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})/);
    if (mdy) {
      let y = Number(mdy[3]);
      if (y < 100) y += y >= 70 ? 1900 : 2000;
      noticeDeadline = `${y}-${String(mdy[1]).padStart(2, "0")}-${String(mdy[2]).padStart(2, "0")}`;
    }
  }

  return {
    hotelFoundInDoc: idx >= 0,
    hasNoticeLanguage: hasNotice,
    noticeDeadlineExtracted: noticeDeadline,
    agreementHint,
    renewalLanguage: renewal,
    terminationLanguage: termination,
    combinedColumnLikely: combinedColumn,
    windowSnippet: window.slice(0, 240),
  };
}

function disposition(candidate, geo, probe, freshness) {
  if (isNoisyName(candidate.hotelName)) {
    return {
      disposition: "REJECTED_OR_INVALID",
      explanation: "Hotel identity fails noise/loan-maturity parse quality gate",
      promotedPriorityA: false,
    };
  }
  if (geo.geographicClass === GEO_CLASS.UNRESOLVED) {
    return {
      disposition: "HOLD_INSUFFICIENT_EVIDENCE",
      explanation: "Physical country unresolved",
      promotedPriorityA: false,
    };
  }
  if (probe.noticeDeadlineExtracted) {
    return {
      disposition: "PRIORITY_A_CONFIRMED",
      explanation: "Verified/extracted notice-related deadline near property schedule — confirmed decision window",
      promotedPriorityA: true,
      noticeDeadline: probe.noticeDeadlineExtracted,
    };
  }
  // Expiration-only with unresolved type stays B or HOLD
  if (candidate.decisionWindowKind === "EXPIRATION_ONLY_PROXY" && probe.agreementHint === "UNRESOLVED") {
    if (freshness.evidenceFreshness === "SUPERSESSION_RISK_HIGH") {
      return {
        disposition: "HOLD_INSUFFICIENT_EVIDENCE",
        explanation: "Expiration-only proxy + unresolved type + aged source supersession risk — current status not corroborated",
        promotedPriorityA: false,
      };
    }
    return {
      disposition: "PRIORITY_B_RESEARCH",
      explanation: "Near-term expiration-only proxy remains Priority B — no confirmed decision window; not auto Priority A",
      promotedPriorityA: false,
      agreementHint: probe.agreementHint,
    };
  }
  if (probe.agreementHint === "FRANCHISE" || probe.agreementHint === "MANAGEMENT") {
    return {
      disposition: "PRIORITY_B_RESEARCH",
      explanation: `Document-level ${probe.agreementHint} cues found but no confirmed notice deadline — remains research candidate`,
      promotedPriorityA: false,
      agreementHint: probe.agreementHint,
    };
  }
  if (candidate.timingHorizon === "HISTORICAL_SUPERSEDED" || (candidate.monthsToDecision !== null && candidate.monthsToDecision < 0)) {
    return {
      disposition: "HISTORICAL_OR_SUPERSEDED",
      explanation: "Decision/expiration date is in the past relative to audit asOf",
      promotedPriorityA: false,
    };
  }
  return {
    disposition: "PRIORITY_B_RESEARCH",
    explanation: "Insufficient new evidence to promote; retain as near-term research candidate",
    promotedPriorityA: false,
  };
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const priB = load("reports/sec-edgar-hif-v6-commercial-timing-geo-audit/PRIORITY_B.json");
  const candidates = (priB?.hotels || []).slice(0, MAX);
  if (!RUN) {
    console.log(JSON.stringify({ dryRun: true, candidates: candidates.length, max: MAX }, null, 2));
    return;
  }

  const edgar = createSecEdgarClient();
  const byUrl = new Map();
  for (const c of candidates) {
    if (!c.sourceUrl) continue;
    if (!byUrl.has(c.sourceUrl)) byUrl.set(c.sourceUrl, []);
    byUrl.get(c.sourceUrl).push(c);
  }

  const results = [];
  const tallies = {
    reviewed: 0,
    geographyVerified: 0,
    geographyReview: 0,
    agreementTypeVerified: 0,
    currentStatusCorroborated: 0,
    noticeDeadlineFound: 0,
    promotedToPriorityA: 0,
    remainingPriorityB: 0,
    strategicWatchlist: 0,
    longTerm: 0,
    historicalOrSuperseded: 0,
    rejectedOrInvalid: 0,
    hold: 0,
  };

  for (const [url, hotels] of byUrl) {
    const res = await edgar.request(url, { accept: "text/html,*/*", maxAgeMs: 14 * 24 * 3600 * 1000 });
    const text = res.ok ? res.text || "" : "";
    for (const c of hotels) {
      tallies.reviewed += 1;
      const geo = classifyPhysicalGeography(c);
      if (geo.geographicClass === GEO_CLASS.VERIFIED) tallies.geographyVerified += 1;
      if (geo.geographicClass === GEO_CLASS.REVIEW_REQUIRED) tallies.geographyReview += 1;

      const probe = text.length > 2000 ? probeDocument(text, c.hotelName) : { hotelFoundInDoc: false, agreementHint: "UNRESOLVED", hasNoticeLanguage: false, noticeDeadlineExtracted: null };
      const freshness = annotateResearchFreshness({ ...c, sourceUrl: url }, asOf);
      const timing = deriveNextCommercialDecisionWindow(
        { ...c, noticeDeadline: probe.noticeDeadlineExtracted || c.noticeDeadline },
        asOf,
      );
      const disp = disposition({ ...c, ...timing }, geo, probe, freshness);

      if (probe.agreementHint === "FRANCHISE" || probe.agreementHint === "MANAGEMENT") tallies.agreementTypeVerified += 1;
      if (probe.noticeDeadlineExtracted) tallies.noticeDeadlineFound += 1;
      if (disp.promotedPriorityA) tallies.promotedToPriorityA += 1;
      if (disp.disposition === "PRIORITY_B_RESEARCH") tallies.remainingPriorityB += 1;
      if (disp.disposition === "STRATEGIC_WATCHLIST") tallies.strategicWatchlist += 1;
      if (disp.disposition === "LONG_TERM_INTELLIGENCE") tallies.longTerm += 1;
      if (disp.disposition === "HISTORICAL_OR_SUPERSEDED") tallies.historicalOrSuperseded += 1;
      if (disp.disposition === "REJECTED_OR_INVALID") tallies.rejectedOrInvalid += 1;
      if (disp.disposition === "HOLD_INSUFFICIENT_EVIDENCE") tallies.hold += 1;
      if (freshness.evidenceFreshness === "CURRENT_STATUS_CORROBORATED") tallies.currentStatusCorroborated += 1;

      results.push({
        hotelName: c.hotelName,
        key: c.key,
        sourceUrl: url,
        priorCommercialPriority: c.commercialPriority || COMMERCIAL_PRIORITY.B_NEAR_TERM_RESEARCH,
        expirationDate: c.expirationDate || c.nextCommercialDecisionDate,
        ...geo,
        ...freshness,
        ...timing,
        probe,
        ...disp,
        wave: "PRIORITY_B_RECOVERY",
        asOf,
      });
    }
    await sleep(120);
  }

  // Candidates without sourceUrl
  for (const c of candidates.filter((x) => !x.sourceUrl)) {
    tallies.reviewed += 1;
    tallies.hold += 1;
    results.push({
      hotelName: c.hotelName,
      disposition: "HOLD_INSUFFICIENT_EVIDENCE",
      explanation: "No sourceUrl for document re-probe",
      promotedPriorityA: false,
    });
  }

  const metrics = {
    wave: "PRIORITY_B_RECOVERY",
    asOf,
    publicationState: "CUSTOMER_BLOCKED",
    researchCostUsd: 0,
    cohortSize: candidates.length,
    documentsFetched: byUrl.size,
    ...tallies,
    note: "Expiration-only near-term records are never auto-promoted to Priority A",
  };

  writeJson(path.join(OUT, "RECOVERY_RESULTS.json"), { count: results.length, results });
  writeJson(path.join(OUT, "RECOVERY_METRICS.json"), metrics);
  writeJson(path.join(OUT, "PROMOTED_PRIORITY_A.json"), {
    hotels: results.filter((r) => r.promotedPriorityA),
  });
  writeJson(path.join(OUT, "REMAINING_PRIORITY_B.json"), {
    hotels: results.filter((r) => r.disposition === "PRIORITY_B_RESEARCH"),
  });
  console.log(JSON.stringify(metrics, null, 2));
}

main().catch((e) => {
  console.error(String(e?.stack || e));
  process.exit(1);
});
