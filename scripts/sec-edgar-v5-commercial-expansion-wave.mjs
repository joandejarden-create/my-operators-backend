#!/usr/bin/env node
/**
 * V5 commercial expansion wave (≤$25 remaining budget):
 *  - Later-filing probes for amendment/renewal/termination instruments
 *  - Free EDGAR exhibit-index crawl for newly discovered CIKs
 *  - Dual prospect lists with classes A/B/C/D
 *  - Free owner/operator hints from existing commercial + identity inventories
 *
 *   node scripts/sec-edgar-v5-commercial-expansion-wave.mjs --run
 *
 * No HPC. CUSTOMER_BLOCKED. No Meta apply.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import { createContractTimingV2Budget } from "../lib/hotel-intelligence/sec-edgar/contract-timing-v2-budget-v1.js";
import { createExaContractTimingClient } from "../lib/hotel-intelligence/sec-edgar/exa-contract-timing-client-v1.js";
import { createSecEdgarClient } from "../lib/hotel-intelligence/sec-edgar/edgar-client-v1.js";
import { classifyRenewalOutcome } from "../lib/hotel-intelligence/sec-edgar/contract-terms-renewal-classifier-v1.js";
import { discoverExhibitsForFiling } from "../lib/hotel-intelligence/sec-edgar/filing-package-exhibits-v2.js";
import { listHistoricalFilings } from "../lib/hotel-intelligence/sec-edgar/filing-discovery-v2.js";
import { listUniversalCompaniesV24 } from "../lib/hotel-intelligence/sec-edgar/cik-registry-v24.js";
import { PROSPECT_CLASS } from "../lib/hotel-intelligence/sec-edgar/ex10-document-execution-class-v1.js";


const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of ["C:/Dev/dealality/my-operators-backend/.env", path.join(ROOT, ".env")]) {
  if (fs.existsSync(p)) dotenv.config({ path: p, override: false });
}

const OUT = path.join(ROOT, "reports/sec-edgar-factory-v5");
const COMM = path.join(ROOT, "reports/sec-edgar-commercial-targets-v1");
const MAX_PAID = 25;

function ensureDir(d) {
  fs.mkdirSync(d, { recursive: true });
}
function writeJson(fp, obj) {
  ensureDir(path.dirname(fp));
  fs.writeFileSync(fp, JSON.stringify(obj, null, 2), "utf8");
}
function ymd() {
  return new Date().toISOString().slice(0, 10);
}
function monthsBetween(fromIso, toIso) {
  const a = new Date(fromIso);
  const b = new Date(toIso);
  if (Number.isNaN(a.getTime()) || Number.isNaN(b.getTime())) return null;
  return (b.getUTCFullYear() - a.getUTCFullYear()) * 12 + (b.getUTCMonth() - a.getUTCMonth());
}

function mapProspectClass(row, later) {
  if (
    later?.renewalStatusClass === "RENEWAL_EXECUTED_VERIFIED" ||
    later?.renewalStatusClass === "EXTENSION_EXECUTED_VERIFIED" ||
    later?.renewalStatusClass === "REPLACED_OR_REFLAGGED_VERIFIED" ||
    later?.renewalStatusClass === "TERMINATED_VERIFIED"
  ) {
    return PROSPECT_CLASS.CLOSED_SUPERSEDED;
  }
  if (row.verifiedEndDate && later?.currentStatusVerified) {
    return PROSPECT_CLASS.A_VERIFIED_DECISION_WINDOW;
  }
  if (row.verifiedEndDate) {
    return PROSPECT_CLASS.B_VERIFIED_END_DATE_STATUS_UNCONFIRMED;
  }
  if (row.hotel && (row.brand || row.owner || row.managementCompany || row.agreementType)) {
    return PROSPECT_CLASS.C_VERIFIED_COMMERCIAL_SIGNAL;
  }
  return PROSPECT_CLASS.D_RESEARCH_REQUIRED;
}

async function laterFilingProbe(exa, budget, row) {
  const gate = budget.canSpend(0.01);
  if (!gate.ok) return { skipped: "BUDGET" };
  const hotel = row.hotelName || row.hotel || "";
  const q = `"${hotel || row.issuerName || "hotel"}" (amendment OR renewed OR renewal OR terminated OR "management agreement" OR franchise) SEC`;
  try {
    const res = await exa.search({ query: q, numResults: 3, type: "lite" });
    const results = res?.results || [];
    const laterText = results.map((r) => `${r.title || ""} ${r.url || ""}`).join("\n");
    const classification = classifyRenewalOutcome({
      originalText: JSON.stringify(row.extracted || row),
      laterText,
      scheduledExpiration: row.scheduledEndDate || row.verifiedEndDate || null,
    });
    return {
      query: q,
      hits: results.map((r) => ({ title: r.title, url: r.url })),
      renewalStatusClass: classification.renewalStatusClass,
      currentStatusVerified:
        classification.renewalStatusClass === "RENEWAL_EXECUTED_VERIFIED" ||
        classification.renewalStatusClass === "EXTENSION_EXECUTED_VERIFIED" ||
        classification.renewalStatusClass === "TERMINATED_VERIFIED" ||
        classification.renewalStatusClass === "REPLACED_OR_REFLAGGED_VERIFIED" ||
        classification.renewalStatusClass === "CURRENT_TERMS_CORROBORATED",
      reasons: classification.reasons,
    };
  } catch (err) {
    return { error: String(err?.message || err) };
  }
}

async function crawlIssuerExhibits(edgar, cik, limit = 8) {
  const padded = String(cik).padStart(10, "0");
  const sub = await edgar.getSubmissions(padded);
  if (!sub.ok) return { cik: padded, error: sub.error || sub.status, exhibits: [] };
  const listed = listHistoricalFilings(sub.body, {
    formTypes: ["10-K", "10-Q", "8-K", "S-1", "S-11", "424B2", "424B5"],
    sinceYear: 2010,
    limit: 12,
  });
  const filings = listed.filings || [];
  const exhibits = [];
  for (const f of filings.slice(0, 4)) {
    try {
      const pack = await discoverExhibitsForFiling(edgar, {
        cik: padded,
        accessionNumber: f.accessionNumber,
        primaryDocument: f.primaryDocument,
        form: f.form,
        filingDate: f.filingDate,
      });
      for (const ex of pack.exhibits || []) {
        if (!/EX-10|exhibit.?10/i.test(`${ex.exhibitType || ""} ${ex.fileName || ""} ${ex.description || ""}`)) {
          continue;
        }
        exhibits.push({
          cik: padded,
          accession: f.accessionNumber,
          form: f.form,
          filingDate: f.filingDate,
          fileName: ex.fileName,
          description: ex.description || null,
          exhibitType: ex.exhibitType,
          url: ex.url,
        });
        if (exhibits.length >= limit) break;
      }
    } catch {
      /* continue */
    }
    if (exhibits.length >= limit) break;
  }
  return {
    cik: padded,
    registrant: sub.body?.name || null,
    exhibits,
  };
}

async function main() {
  ensureDir(OUT);
  const budget = createContractTimingV2Budget({
    ceilingUsd: MAX_PAID,
    ledgerPath: path.join(ROOT, "data/sec-edgar/v5-commercial-expansion-budget.json"),
    cwd: ROOT,
  });
  // Seed spent from prior V5 pilots if present
  const priorSpendPaths = [
    path.join(ROOT, "data/sec-edgar/v5-ex10-budget-ledger.json"),
    path.join(ROOT, "data/sec-edgar/v5-commercial-target-budget-ledger.json"),
  ];
  let priorSpent = 0;
  for (const p of priorSpendPaths) {
    if (!fs.existsSync(p)) continue;
    try {
      const j = JSON.parse(fs.readFileSync(p, "utf8"));
      priorSpent += Number(j.spentUsd || j.totalSpentUsd || 0) || 0;
    } catch {
      /* ignore */
    }
  }

  const edgar = createSecEdgarClient();
  const exa = createExaContractTimingClient({ budget });
  const registry = listUniversalCompaniesV24();
  const registryCiks = new Set(registry.map((c) => String(c.cik).padStart(10, "0")));

  // --- Phase 3: later-filing verification on persist-classified instruments ---
  const persistPath = path.join(OUT, "EX10_PERSIST_CLASSIFIED.json");
  const classified = fs.existsSync(persistPath)
    ? JSON.parse(fs.readFileSync(persistPath, "utf8")).classified || []
    : [];
  const instruments = classified.filter((r) =>
    /AMENDMENT|RENEWAL|TERMINATION|EXTENSION/i.test(r.instrumentClass || r.executionClass || ""),
  );
  // Deduplicate by accession+instrument
  const byKey = new Map();
  for (const r of instruments) {
    const k = `${r.accession}|${r.instrumentClass}|${r.hotelName || ""}`;
    if (!byKey.has(k)) byKey.set(k, r);
  }
  const laterResults = [];
  for (const row of [...byKey.values()].slice(0, 30)) {
    const later = await laterFilingProbe(exa, budget, row);
    laterResults.push({
      exhibitId: row.exhibitId,
      hotelName: row.hotelName,
      instrumentClass: row.instrumentClass,
      agreementType: row.agreementType,
      scheduledEndDate: row.scheduledEndDate,
      noticePeriodDays: row.noticePeriodDays,
      later,
    });
  }
  writeJson(path.join(OUT, "EX10_LATER_FILING_VERIFICATION.json"), {
    probed: laterResults.length,
    confirmedRenewalOrTermination: laterResults.filter((r) => r.later?.currentStatusVerified).length,
    results: laterResults,
  });

  // --- Phase 4: EDGAR crawl for newly discovered CIKs ---
  const metricsPath = path.join(ROOT, "reports/sec-edgar-factory-v5/EX10_PILOT_METRICS.json");
  const pilotMetrics = fs.existsSync(metricsPath)
    ? JSON.parse(fs.readFileSync(metricsPath, "utf8"))
    : {};
  const newCiks = (pilotMetrics.issuersBeyondRegistryCiks || []).filter(
    (c) => c && !registryCiks.has(String(c).padStart(10, "0")),
  );
  const crawl = [];
  for (const cik of newCiks.slice(0, 29)) {
    const row = await crawlIssuerExhibits(edgar, cik, 6);
    crawl.push(row);
  }
  const allExhibits = crawl.flatMap((c) => c.exhibits || []);
  writeJson(path.join(OUT, "EX10_NEW_ISSUER_EXHIBIT_CRAWL.json"), {
    issuersCrawled: crawl.length,
    exhibitsEnumerated: allExhibits.length,
    crawl,
  });

  // --- Phase 5: dual prospect lists with A/B/C/D ---
  const oppPath = path.join(COMM, "OPPORTUNITIES.json");
  const opportunities = fs.existsSync(oppPath)
    ? JSON.parse(fs.readFileSync(oppPath, "utf8")).opportunities || []
    : [];
  const persistRows = fs.existsSync(path.join(OUT, "EX10_PERSIST_RESULTS.json"))
    ? JSON.parse(fs.readFileSync(path.join(OUT, "EX10_PERSIST_RESULTS.json"), "utf8")).results || []
    : [];

  const laterByHotel = new Map();
  for (const r of laterResults) {
    if (r.hotelName) laterByHotel.set(r.hotelName, r.later);
  }

  const asOf = ymd();
  const prospectSeeds = [];

  for (const o of opportunities) {
    if (!o.hotelName && !o.hotel) continue;
    const hotel = o.hotelName || o.hotel;
    const later = laterByHotel.get(hotel) || null;
    const verifiedEndDate = o.verifiedEndDate || null;
    const noticePeriodDays = o.noticePeriodDays ?? null;
    const row = {
      researchId: o.researchId || `ct_${hotel}`,
      hotelName: hotel,
      hotel: hotel,
      location: o.location || null,
      brand: o.brand || null,
      owner: o.owner || null,
      managementCompany: o.managementCompany || null,
      agreementType: o.agreementType || "UNKNOWN",
      verifiedEndDate,
      noticePeriodDays,
      noticeStatus: o.noticeStatus || (noticePeriodDays != null ? "VERIFIED" : "NOTICE_NOT_DISCLOSED"),
      noticeDeadline: o.noticeDeadline || null,
      renewalStatusClass: later?.renewalStatusClass || o.renewalStatusClass || "STATUS_UNCONFIRMED",
      monthsToExpiration: verifiedEndDate ? monthsBetween(asOf, verifiedEndDate) : null,
      sourceUrl: o.sourceUrl || null,
      confidence: o.confidence || "MEDIUM",
      verificationDate: asOf,
      publicationState: "CUSTOMER_BLOCKED",
    };
    row.prospectClass = mapProspectClass(row, later);
    row.activeTargeting =
      row.prospectClass === PROSPECT_CLASS.A_VERIFIED_DECISION_WINDOW ||
      (row.prospectClass === PROSPECT_CLASS.B_VERIFIED_END_DATE_STATUS_UNCONFIRMED &&
        row.monthsToExpiration != null &&
        row.monthsToExpiration >= 0 &&
        row.monthsToExpiration <= 36);
    row.recommendedNextAction =
      row.prospectClass === PROSPECT_CLASS.CLOSED_SUPERSEDED
        ? "REMOVE_FROM_ACTIVE_TARGETING"
        : row.prospectClass === PROSPECT_CLASS.A_VERIFIED_DECISION_WINDOW
          ? "OWNER_ASSET_MANAGER_OUTREACH_RESEARCH"
          : row.prospectClass === PROSPECT_CLASS.B_VERIFIED_END_DATE_STATUS_UNCONFIRMED
            ? "VERIFY_CURRENT_APPLICABILITY_THEN_OWNER_ENRICH"
            : "CONFIRM_CONTRACT_END_DATE_OR_TRANSITION_SIGNAL";
    prospectSeeds.push(row);
  }

  // Add persist-named hotels as management/franchise signals
  for (const p of persistRows) {
    if (!p.hotelName) continue;
    if (prospectSeeds.some((s) => s.hotelName === p.hotelName && s.agreementType === p.agreementType)) {
      continue;
    }
    const later = laterByHotel.get(p.hotelName) || null;
    const row = {
      researchId: p.agreementId || p.documentId,
      hotelName: p.hotelName,
      hotel: p.hotelName,
      location: null,
      brand: null,
      owner: null,
      managementCompany: null,
      agreementType: p.agreementType === "UNKNOWN_AGREEMENT" ? "OTHER" : p.agreementType,
      verifiedEndDate: p.scheduledEndDate || null,
      noticePeriodDays: p.noticePeriodDays,
      noticeStatus: p.noticePeriodDays != null ? "VERIFIED" : "NOTICE_NOT_DISCLOSED",
      noticeDeadline: p.noticeDeadline || null,
      renewalStatusClass: later?.renewalStatusClass || "STATUS_UNCONFIRMED",
      monthsToExpiration: p.scheduledEndDate ? monthsBetween(asOf, p.scheduledEndDate) : null,
      sourceUrl: p.sourceUrl,
      confidence: "MEDIUM",
      verificationDate: asOf,
      publicationState: "CUSTOMER_BLOCKED",
      listHints:
        p.agreementType === "MANAGEMENT"
          ? ["MANAGEMENT_CONTRACT"]
          : p.agreementType === "FRANCHISE"
            ? ["BRAND_CONVERSION"]
            : ["MANAGEMENT_CONTRACT", "BRAND_CONVERSION"],
    };
    row.prospectClass = mapProspectClass(row, later);
    row.activeTargeting =
      row.prospectClass === PROSPECT_CLASS.A_VERIFIED_DECISION_WINDOW ||
      (row.prospectClass === PROSPECT_CLASS.B_VERIFIED_END_DATE_STATUS_UNCONFIRMED &&
        row.monthsToExpiration != null &&
        row.monthsToExpiration >= 0 &&
        row.monthsToExpiration <= 36);
    row.recommendedNextAction = "VERIFY_OWNER_OPERATOR_AND_CURRENT_STATUS";
    prospectSeeds.push(row);
  }

  // Shape for projectDualProspectLists (uses tier field historically) — map class → list rows directly
  const brandConversion = [];
  const managementContract = [];
  for (const row of prospectSeeds) {
    if (row.prospectClass === PROSPECT_CLASS.CLOSED_SUPERSEDED) continue;
    const base = {
      researchId: row.researchId,
      hotel: row.hotelName,
      location: row.location,
      brand: row.brand,
      owner: row.owner,
      managementCompany: row.managementCompany,
      agreementType: row.agreementType,
      verifiedEndDate: row.verifiedEndDate,
      noticePeriodDays: row.noticePeriodDays,
      noticeStatus: row.noticeStatus,
      noticeDeadline: row.noticeDeadline,
      renewalStatus: row.renewalStatusClass,
      monthsToDecisionWindow: row.monthsToExpiration,
      prospectClass: row.prospectClass,
      confidence: row.confidence,
      evidenceUrl: row.sourceUrl,
      verificationDate: row.verificationDate,
      recommendedNextAction: row.recommendedNextAction,
      publicationState: "CUSTOMER_BLOCKED",
      activeTargeting: Boolean(row.activeTargeting),
    };
    const isFranchise = /FRANCHISE|LICENSE/i.test(row.agreementType || "");
    const isMgmt = /MANAGEMENT/i.test(row.agreementType || "");
    if (isFranchise || row.listHints?.includes("BRAND_CONVERSION") || (!isMgmt && row.brand)) {
      brandConversion.push({ ...base, prospectList: "BRAND_CONVERSION", decisionMakerFocus: "owner_and_brand" });
    }
    if (isMgmt || row.listHints?.includes("MANAGEMENT_CONTRACT") || (!isFranchise && /MANAGEMENT|OTHER|UNKNOWN/i.test(row.agreementType || ""))) {
      managementContract.push({
        ...base,
        prospectList: "MANAGEMENT_CONTRACT",
        decisionMakerFocus: "owner_and_asset_management",
      });
    }
  }

  function byClass(list) {
    const out = {};
    for (const c of Object.values(PROSPECT_CLASS)) out[c] = 0;
    for (const r of list) out[r.prospectClass] = (out[r.prospectClass] || 0) + 1;
    return out;
  }

  // Free owner/operator coverage from identity inventory
  let ownerCoverage = 0;
  let brandCoverage = 0;
  let mgmtCoverage = 0;
  const invPath = path.join(
    ROOT,
    "reports/sec-edgar-identity-quality-v2/VERIFIED_HOTEL_INTELLIGENCE_INVENTORY.json",
  );
  if (fs.existsSync(invPath)) {
    const inv = JSON.parse(fs.readFileSync(invPath, "utf8"));
    const byName = new Map();
    for (const r of [...(inv.rows || []), ...(inv.probableRows || [])]) {
      if (r.hotelName) byName.set(String(r.hotelName).toLowerCase(), r);
    }
    for (const list of [brandConversion, managementContract]) {
      for (const row of list) {
        const hit = byName.get(String(row.hotel || "").toLowerCase());
        if (hit) {
          if (!row.brand && (hit.brandHint || hit.brandLabel || hit.historicalBrand)) {
            row.brand = hit.brandHint || hit.brandLabel || hit.historicalBrand;
          }
          if (!row.owner && hit.registrant) row.owner = hit.registrant; // registrant ≠ owner; mark as SEC registrant hint
          row.secRegistrantHint = hit.registrant || null;
          row.ownerNote = hit.registrant
            ? "SEC_REGISTRANT_HINT_NOT_VERIFIED_PROPERTY_OWNER"
            : null;
        }
        if (row.brand) brandCoverage += 1;
        if (row.owner || row.secRegistrantHint) ownerCoverage += 1;
        if (row.managementCompany) mgmtCoverage += 1;
      }
    }
  }

  writeJson(path.join(COMM, "BRAND_CONVERSION_TARGETS_V5.json"), {
    count: brandConversion.length,
    byClass: byClass(brandConversion),
    targets: brandConversion,
    publicationState: "CUSTOMER_BLOCKED",
  });
  writeJson(path.join(COMM, "MANAGEMENT_CONTRACT_TARGETS_V5.json"), {
    count: managementContract.length,
    byClass: byClass(managementContract),
    targets: managementContract,
    publicationState: "CUSTOMER_BLOCKED",
  });

  const spend = budget.snapshot();
  const verifiedTargets =
    brandConversion.filter((r) => r.prospectClass?.startsWith("A_") || r.prospectClass?.startsWith("B_")).length +
    managementContract.filter((r) => r.prospectClass?.startsWith("A_") || r.prospectClass?.startsWith("B_")).length;

  const summary = {
    laterFilingProbed: laterResults.length,
    laterConfirmed: laterResults.filter((r) => r.later?.currentStatusVerified).length,
    issuersCrawled: crawl.length,
    exhibitsEnumerated: allExhibits.length,
    brandConversionCount: brandConversion.length,
    managementContractCount: managementContract.length,
    brandByClass: byClass(brandConversion),
    managementByClass: byClass(managementContract),
    ownerOrRegistrantHints: ownerCoverage,
    brandCoverage,
    managementCompanyCoverage: mgmtCoverage,
    spentUsdThisWave: spend.spentUsd,
    priorSpentUsdApprox: priorSpent,
    ceilingUsd: MAX_PAID,
    remainingBudgetApprox: Math.max(0, MAX_PAID - priorSpent - spend.spentUsd),
    costPerVerifiedTargetApprox:
      verifiedTargets > 0 ? Number(((priorSpent + spend.spentUsd) / verifiedTargets).toFixed(4)) : null,
    publicationState: "CUSTOMER_BLOCKED",
    hpcCreates: 0,
  };
  writeJson(path.join(OUT, "COMMERCIAL_EXPANSION_WAVE_SUMMARY.json"), summary);
  console.log(JSON.stringify({ ok: true, summary }, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
