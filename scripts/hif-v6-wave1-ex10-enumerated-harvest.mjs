#!/usr/bin/env node
/**
 * HIF V6 Wave 1 — process the enumerated 62 EX-10 exhibits (HIGH_TEXT first).
 * Free EDGAR retrieval. CUSTOMER_BLOCKED. No HPC. No Agent/Jev spend.
 *
 *   node scripts/hif-v6-wave1-ex10-enumerated-harvest.mjs --run
 *   node scripts/hif-v6-wave1-ex10-enumerated-harvest.mjs --run --high-text-only --limit=46
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import { createSecEdgarClient } from "../lib/hotel-intelligence/sec-edgar/edgar-client-v1.js";
import {
  harvestExhibitDocument,
  AGREEMENT_INSTRUMENT_CLASS,
  SCOPE_CLASS,
} from "../lib/hotel-intelligence/sec-edgar/ex10-contract-harvest-v1.js";
import { extractCommercialTimingObservations } from "../lib/hotel-intelligence/sec-edgar/commercial-expiration-notice-extract-v1.js";
import { VERIFICATION_GATE } from "../lib/hotel-intelligence/sec-edgar/contract-intelligence-identity-layers-v1.js";
import {
  buildWeeklySnapshot,
  emptyWeeklyMetrics,
  ensureWeeklyScorecardLayout,
  saveWeeklySnapshot,
  isoWeekId,
  attributeHotelGeography,
} from "../lib/hotel-intelligence/sec-edgar/hif-weekly-regional-yield-scorecard-v6.js";
import { buildV6ExecutionGate } from "../lib/hotel-intelligence/sec-edgar/hif-v6-shared-research-contract.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of ["C:/Dev/dealality/my-operators-backend/.env", path.join(ROOT, ".env")]) {
  if (fs.existsSync(p)) dotenv.config({ path: p, override: false });
}

const OUT = path.join(ROOT, "reports/sec-edgar-hif-v6-wave1-ex10");
const PLAN_PATH = path.join(
  ROOT,
  "reports/sec-edgar-prospect-qualification-v5/QUALIFICATION_SUMMARY.json",
);

const args = process.argv.slice(2);
const RUN = args.includes("--run");
const HIGH_TEXT_ONLY = args.includes("--high-text-only") || !args.includes("--include-image");
const LIMIT = Number((args.find((a) => a.startsWith("--limit=")) || "").split("=")[1]) || 62;

function ensureDir(d) {
  fs.mkdirSync(d, { recursive: true });
}
function writeJson(fp, obj) {
  ensureDir(path.dirname(fp));
  fs.writeFileSync(fp, JSON.stringify(obj, null, 2), "utf8");
}

function edgarExhibitUrl(ex) {
  const cikNum = String(ex.cik || "").replace(/^0+/, "") || "0";
  const acc = String(ex.accession || "").replace(/-/g, "");
  const file = ex.fileName || "primary.htm";
  return `https://www.sec.gov/Archives/edgar/data/${cikNum}/${acc}/${file}`;
}

function loadPlan() {
  if (!fs.existsSync(PLAN_PATH)) {
    throw new Error(`Missing exhibit plan: ${PLAN_PATH}`);
  }
  const plan = JSON.parse(fs.readFileSync(PLAN_PATH, "utf8"));
  const exhibits = plan.ex10NextWave?.exhibits || [];
  let list = exhibits.slice();
  if (HIGH_TEXT_ONLY) {
    list = list.filter((e) => e.retrievalPriority === "HIGH_TEXT" || !e.likelyImageOnly);
  }
  list.sort((a, b) => {
    const pa = a.retrievalPriority === "HIGH_TEXT" ? 0 : 1;
    const pb = b.retrievalPriority === "HIGH_TEXT" ? 0 : 1;
    return pa - pb;
  });
  return { planMeta: plan.ex10NextWave, exhibits: list.slice(0, LIMIT) };
}

async function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function main() {
  const gate = buildV6ExecutionGate({
    merged: true,
    mergeSha: process.env.HIF_V5_MERGE_SHA || "9783ff27bc940fcee3b692088353c38674e6e188",
  });
  if (!RUN) {
    const { planMeta, exhibits } = loadPlan();
    console.log(
      JSON.stringify(
        {
          ok: true,
          dryRun: true,
          gate,
          plan: planMeta,
          wouldProcess: exhibits.length,
          highTextOnly: HIGH_TEXT_ONLY,
          hint: "Pass --run to execute free EDGAR harvest",
        },
        null,
        2,
      ),
    );
    return;
  }

  ensureDir(OUT);
  const { planMeta, exhibits } = loadPlan();
  writeJson(path.join(OUT, "WAVE1_EXHIBIT_QUEUE.json"), {
    enumerated: planMeta?.exhibitsEnumerated,
    queued: exhibits.length,
    highTextOnly: HIGH_TEXT_ONLY,
    exhibits,
  });

  const edgar = createSecEdgarClient();
  const harvested = [];
  const hotelExamples = [];
  let retrieved = 0;
  let failed = 0;
  let existence = 0;
  let terms = 0;
  let franchiseExp = 0;
  let managementExp = 0;
  let namedHotel = 0;
  let costUsd = 0;

  for (let i = 0; i < exhibits.length; i++) {
    const ex = exhibits[i];
    const url = edgarExhibitUrl(ex);
    let text = "";
    let retrievalError = null;
    let fromCache = false;
    try {
      const res = await edgar.request(url, {
        accept: "text/html,image/*,*/*",
        maxAgeMs: 30 * 24 * 60 * 60 * 1000,
      });
      if (res.ok) {
        text = res.text || "";
        fromCache = Boolean(res.fromCache);
        retrieved += 1;
      } else {
        retrievalError = res.error || `HTTP_${res.status}`;
        failed += 1;
      }
    } catch (err) {
      retrievalError = String(err?.message || err);
      failed += 1;
    }

    const h = harvestExhibitDocument(
      { html: text },
      {
        sourceUrl: url,
        accession: ex.accession,
        cik: ex.cik,
        issuerName: ex.registrant,
      },
    );
    h.retrievalPriority = ex.retrievalPriority;
    h.fileName = ex.fileName;
    h.filingDate = ex.filingDate;
    h.form = ex.form;
    h.retrievalError = retrievalError;
    h.fromCache = fromCache;
    h.wave = "HIF_V6_WAVE1_ENUMERATED_EX10";

    let commercial = null;
    if (text && text.length > 200) {
      try {
        commercial = extractCommercialTimingObservations(
          { text, html: text },
          { sourceUrl: url, accession: ex.accession },
        );
      } catch {
        commercial = null;
      }
    }
    h.commercialExtract = commercial
      ? {
          findings: (commercial.observations || commercial.findings || commercial.rows || []).slice(
            0,
            20,
          ),
          count: (commercial.observations || commercial.findings || commercial.rows || []).length,
        }
      : null;

    if (h.gates?.[VERIFICATION_GATE.AGREEMENT_EXISTENCE_VERIFIED]) existence += 1;
    if (h.gates?.[VERIFICATION_GATE.CONTRACT_TERMS_VERIFIED]) terms += 1;
    if (h.scopeClass === SCOPE_CLASS.NAMED_HOTEL) namedHotel += 1;
    if (h.agreementType === "FRANCHISE" && h.disclosedFieldCount > 0) franchiseExp += 1;
    if (h.agreementType === "MANAGEMENT" && h.disclosedFieldCount > 0) managementExp += 1;

    const endDate =
      h.extracted?.scheduledEndDate ||
      h.extracted?.expirationDate ||
      commercial?.observations?.[0]?.expirationDate ||
      commercial?.findings?.[0]?.expirationDate ||
      null;
    if (h.hotelAttributionAllowed && (endDate || h.persistResearchEligible)) {
      hotelExamples.push({
        hotelName: h.extracted?.propertyName || h.issuerName || "Named hotel scope",
        hotelCountry: "US",
        agreementType: h.agreementType,
        expirationDate: endDate,
        instrumentClass: h.instrumentClass,
        sourceUrl: url,
        recommendedAction:
          h.agreementType === "MANAGEMENT"
            ? "REVIEW_MANAGEMENT_CONTRACT_TARGET"
            : "REVIEW_BRAND_CONVERSION_TARGET",
      });
    }

    harvested.push(h);
    // polite pacing for SEC
    if (!fromCache) await sleep(120);
    if ((i + 1) % 10 === 0) {
      console.error(`progress ${i + 1}/${exhibits.length} retrieved=${retrieved} failed=${failed}`);
    }
  }

  const metrics = {
    wave: "HIF_V6_WAVE1_ENUMERATED_EX10",
    mergeBaselineSha: gate.mergeSha,
    exhibitsQueued: exhibits.length,
    exhibitsRetrieved: retrieved,
    retrievalFailed: failed,
    harvested: harvested.length,
    agreementExistenceVerified: existence,
    contractTermsVerified: terms,
    namedHotelScope: namedHotel,
    franchiseSignalRows: franchiseExp,
    managementSignalRows: managementExp,
    formOfAgreements: harvested.filter(
      (h) => h.instrumentClass === AGREEMENT_INSTRUMENT_CLASS.FORM_OF_AGREEMENT,
    ).length,
    executedAgreements: harvested.filter(
      (h) => h.instrumentClass === AGREEMENT_INSTRUMENT_CLASS.EXECUTED_AGREEMENT,
    ).length,
    researchCostUsd: costUsd,
    publicationState: "CUSTOMER_BLOCKED",
    hpcCreates: 0,
    agentSpendUsd: 0,
    jevApply: false,
  };

  writeJson(path.join(OUT, "EX10_WAVE1_HARVEST.json"), { harvested });
  writeJson(path.join(OUT, "EX10_WAVE1_METRICS.json"), metrics);
  writeJson(path.join(OUT, "EX10_WAVE1_HOTEL_EXAMPLES.json"), {
    count: hotelExamples.length,
    examples: hotelExamples.slice(0, 30),
  });

  // Weekly scorecard increment note (new week id; do not overwrite immutable W47)
  ensureWeeklyScorecardLayout(ROOT);
  const weekId = isoWeekId();
  const global = emptyWeeklyMetrics();
  global.documentsRetrievedAndProcessed = retrieved;
  global.distinctHotelsResearched = namedHotel;
  global.distinctHotelsDiscovered = namedHotel;
  global.newlyDiscoveredHotelsThisWeek = namedHotel;
  global.agreementIdentifiedHint = existence;
  global.verifiedFranchiseExpirations = 0; // only promote after independent date verification
  global.verifiedManagementAgreementExpirations = 0;
  global.researchCostUsd = costUsd;
  global.researchCandidates = harvested.filter((h) => h.persistResearchEligible).length;
  global.holdQuarantinedRecords = harvested.filter(
    (h) =>
      h.instrumentClass === AGREEMENT_INSTRUMENT_CLASS.FORM_OF_AGREEMENT ||
      h.instrumentClass === AGREEMENT_INSTRUMENT_CLASS.DRAFT_OR_PROPOSED,
  ).length;

  const snap = buildWeeklySnapshot({
    weekId: `${weekId}-wave1-ex10`,
    asOf: new Date().toISOString().slice(0, 10),
    global,
    byCountry: {
      US: { ...global },
    },
    byRegion: {
      NORTH_AMERICA: { covered: true, coverageNote: "ACTUAL_RESEARCH_RECORDED", ...global },
    },
    bySourceFamily: {
      EX10: {
        independentlyVerifiedIncrementalFindings: terms,
        documentsProcessed: retrieved,
        costUsd: 0,
        trackAFindings: harvested.length,
        trackBFindings: 0,
        duplicateFindings: 0,
        holdOrInvalidFindings: global.holdQuarantinedRecords,
      },
    },
    trackA: { ...global },
    trackB: emptyWeeklyMetrics(),
    hotelExamples: hotelExamples.slice(0, 20).map((h) => ({
      ...h,
      ...attributeHotelGeography({ hotelCountry: h.hotelCountry, sourceJurisdiction: "US" }),
    })),
    notes: {
      wave: "HIF_V6_WAVE1_ENUMERATED_EX10",
      evidenceRule: "Do not promote expiration counts without hotel-specific verified dates",
      leaseDeferred: true,
    },
  });
  snap.allowOverwrite = true; // wave artifact week id is unique suffix
  const saved = saveWeeklySnapshot(snap, ROOT);

  writeJson(path.join(OUT, "FOUNDER_WAVE1_SUMMARY.md".replace(/\.md$/, ".json")), {
    ...metrics,
    weeklySnapshot: saved.path,
    hotelExampleCount: hotelExamples.length,
    gate,
  });

  const md = [
    "# HIF V6 Wave 1 — Enumerated EX-10 harvest",
    "",
    `**Merge baseline:** \`${gate.mergeSha}\``,
    `**CUSTOMER_BLOCKED** · Agent spend $0 · Jev apply false · Lease deferred`,
    "",
    "## Results",
    `- Exhibits queued: ${exhibits.length}`,
    `- Retrieved: ${retrieved}`,
    `- Retrieval failed: ${failed}`,
    `- Agreement existence verified: ${existence}`,
    `- Contract terms verified: ${terms}`,
    `- Named-hotel scope: ${namedHotel}`,
    `- Research-eligible exhibits: ${global.researchCandidates}`,
    `- Form-of / draft HOLD-like: ${global.holdQuarantinedRecords}`,
    "",
    "## Note",
    "Verified franchise/management **expiration counts** are not inflated from this wave until hotel-specific dates pass independent verification gates.",
    "",
    "## Examples (capped)",
    ...hotelExamples.slice(0, 15).map(
      (h) =>
        `- ${h.hotelName} (${h.hotelCountry}) · ${h.agreementType} · ${h.expirationDate || "date TBD"} · ${h.sourceUrl}`,
    ),
  ].join("\n");
  fs.writeFileSync(path.join(OUT, "FOUNDER_WAVE1_SUMMARY.md"), md, "utf8");

  console.log(JSON.stringify({ ok: true, metrics, weeklySnapshot: saved, out: OUT }, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
