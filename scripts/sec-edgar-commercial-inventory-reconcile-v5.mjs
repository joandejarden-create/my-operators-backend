#!/usr/bin/env node
/**
 * Reconcile HIF commercial inventory:
 * - Upgrade opportunities: no research-horizon gate; preserve historical + future dates
 * - Merge EX-10 named MANAGEMENT agreements into dual prospect lists
 * - Explain Tier B/C vs Brand Conversion vs Management Contract counts
 * - Write dual lists + founder reconciliation report
 *
 * CUSTOMER_BLOCKED · no HPC · no Meta schema · no merge
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  classifyCommercialOpportunity,
  projectDualProspectLists,
  filterProspectList,
  summarizeCommercialInventory,
  OPPORTUNITY_TIER,
  APP_DATE_FILTER,
  PROSPECT_LIST,
} from "../lib/hotel-intelligence/sec-edgar/commercial-target-discovery-v1.js";
import { AUDITED_DATE_MEANING, EVIDENCE_CONFIDENCE, DECISION_WINDOW_ELIGIBILITY } from "../lib/hotel-intelligence/sec-edgar/contract-date-meaning-audit-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const COMM = path.join(ROOT, "reports/sec-edgar-commercial-targets-v1");
const OUT = path.join(ROOT, "reports/sec-edgar-commercial-inventory-reconcile-v5");

function readJson(p, fb = null) {
  if (!fs.existsSync(p)) return fb;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}
function writeJson(p, obj) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, JSON.stringify(obj, null, 2), "utf8");
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

function upgradeExistingOpportunity(o, asOf) {
  const months =
    o.monthsToExpiration != null
      ? o.monthsToExpiration
      : o.verifiedEndDate
        ? monthsBetween(asOf, o.verifiedEndDate)
        : null;
  const historical = months != null && months < 0;
  const future = months != null && months >= 0;
  const hints = [APP_DATE_FILTER.ALL];
  if (!o.verifiedEndDate) hints.push(APP_DATE_FILTER.UNKNOWN_EXPIRATION);
  else {
    if (historical) hints.push(APP_DATE_FILTER.HISTORICAL_EXPIRATIONS);
    if (future) hints.push(APP_DATE_FILTER.FUTURE_EXPIRATIONS);
    if (future && months <= 12) hints.push(APP_DATE_FILTER.NEXT_12_MONTHS);
    if (future && months <= 24) hints.push(APP_DATE_FILTER.NEXT_24_MONTHS);
    if (future && months <= 36) hints.push(APP_DATE_FILTER.NEXT_36_MONTHS);
    if (future && months <= 60) hints.push(APP_DATE_FILTER.NEXT_60_MONTHS);
  }

  let tier = o.tier;
  if (tier === OPPORTUNITY_TIER.HOLD_QUARANTINE && o.changeClass === "CLOSED") {
    tier = OPPORTUNITY_TIER.CLOSED_SUPERSEDED;
  }

  const researchEligible =
    tier === OPPORTUNITY_TIER.TIER_A_VERIFIED_DECISION_WINDOW ||
    tier === OPPORTUNITY_TIER.TIER_B_VERIFIED_CONTRACT_EXPIRATION ||
    tier === OPPORTUNITY_TIER.TIER_C_CONVERSION_RESEARCH_CANDIDATE ||
    tier === OPPORTUNITY_TIER.TIER_D_FURTHER_RESEARCH_REQUIRED ||
    tier === OPPORTUNITY_TIER.CLOSED_SUPERSEDED;

  const nearTerm =
    researchEligible &&
    Boolean(o.within36Months) &&
    (tier === OPPORTUNITY_TIER.TIER_A_VERIFIED_DECISION_WINDOW ||
      (tier === OPPORTUNITY_TIER.TIER_B_VERIFIED_CONTRACT_EXPIRATION && !historical));

  return {
    ...o,
    tier,
    monthsToExpiration: months,
    historicalExpirationDate: historical ? o.verifiedEndDate : o.historicalExpirationDate || null,
    futureExpirationDate: future ? o.verifiedEndDate : o.futureExpirationDate || null,
    latestCorroboratedExpirationDate: o.verifiedEndDate || null,
    appDateFilterHints: hints,
    researchEligible,
    nearTermOutreachPriority: nearTerm,
    activeTargeting: nearTerm,
    country: o.country || null,
    region: o.region || null,
    contractDateClass:
      o.contractDateClass ||
      (historical
        ? "HISTORICAL_EXPIRATION_STATUS_UNKNOWN"
        : o.verifiedEndDate
          ? "EXPLICIT_CONTRACT_EXPIRATION"
          : "UNRESOLVED_DATE_MEANING"),
  };
}

function ex10ToObservation(row) {
  const hotelName = row.hotelName;
  if (!hotelName) return null;
  const agreementType = String(row.agreementType || "MANAGEMENT").toUpperCase();
  const primaryDate = row.scheduledEndDate || null;
  const noticeDays = row.noticePeriodDays != null ? Number(row.noticePeriodDays) : null;
  const isMgmt = agreementType === "MANAGEMENT";
  const meaning = isMgmt
    ? AUDITED_DATE_MEANING.MANAGEMENT_AGREEMENT_EXPIRATION
    : AUDITED_DATE_MEANING.FRANCHISE_AGREEMENT_EXPIRATION;

  return {
    hotelName,
    hotelVerified: true,
    hotelAttributed: row.scopeClass === "NAMED_HOTEL",
    portfolioLevelOnly: row.scopeClass === "PORTFOLIO_LEVEL",
    agreementType: isMgmt ? "MANAGEMENT" : agreementType === "FRANCHISE" ? "FRANCHISE" : "UNKNOWN",
    primaryDate,
    renewalNoticeDays: noticeDays,
    brand: null,
    owner: null,
    managementCompany: null,
    evidenceSnippet: `${agreementType} ${row.instrumentClass || ""} ${hotelName} end=${primaryDate || "undisclosed"} notice=${noticeDays ?? "n/a"}`,
    sourceUrl: row.sourceUrl,
    accession: row.accession,
    cik: row.cik,
    listHints: isMgmt ? [PROSPECT_LIST.MANAGEMENT_CONTRACT] : [PROSPECT_LIST.BRAND_CONVERSION],
    conversionSignals: isMgmt ? { management: true } : { brand: true },
    dateAudit: primaryDate
      ? {
          auditedMeaning: meaning,
          confidence: EVIDENCE_CONFIDENCE.MEDIUM,
          decisionWindowEligibility: DECISION_WINDOW_ELIGIBILITY.ELIGIBLE,
        }
      : null,
    country: null,
    region: null,
    fromEx10Persist: true,
    findingId: row.findingId,
    agreementId: row.agreementId,
  };
}

function hotelKey(name) {
  return String(name || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function main() {
  const asOf = ymd();
  fs.mkdirSync(OUT, { recursive: true });

  const prior = readJson(path.join(COMM, "OPPORTUNITIES.json"), { opportunities: [] });
  const priorOps = prior.opportunities || [];
  const metricsPrior = readJson(path.join(COMM, "PILOT_METRICS.json"), {});
  const brandPrior = readJson(path.join(COMM, "BRAND_CONVERSION_TARGETS.json"), { count: 0, targets: [] });
  const mgmtPrior = readJson(path.join(COMM, "MANAGEMENT_CONTRACT_TARGETS.json"), { count: 0, targets: [] });
  const ex10 = readJson(path.join(ROOT, "reports/sec-edgar-factory-v5/EX10_PERSIST_CLASSIFIED.json"), {
    classified: [],
  });

  const upgraded = priorOps.map((o) => upgradeExistingOpportunity(o, asOf));

  // Merge EX-10 named hotels (esp. MANAGEMENT) — dedupe by hotel+agreementType+end
  const seen = new Set(
    upgraded.map((o) => `${hotelKey(o.hotelName)}|${o.agreementType}|${o.verifiedEndDate || ""}`),
  );
  const ex10Added = [];
  for (const row of ex10.classified || []) {
    if (row.scopeClass && row.scopeClass !== "NAMED_HOTEL") continue;
    if (row.executionClass === "FORM_AGREEMENT" || row.executionClass === "DRAFT_OR_PROPOSED_AGREEMENT") continue;
    const obs = ex10ToObservation(row);
    if (!obs || !obs.hotelAttributed) continue;
    const k = `${hotelKey(obs.hotelName)}|${obs.agreementType}|${obs.primaryDate || ""}`;
    if (seen.has(k)) continue;
    seen.add(k);
    const classified = classifyCommercialOpportunity(obs, { asOf });
    // Force management list hint for MANAGEMENT even when Tier C (no end date)
    if (obs.agreementType === "MANAGEMENT") {
      classified.listHints = [PROSPECT_LIST.MANAGEMENT_CONTRACT];
      classified.conversionSignals = { management: true };
      if (!classified.agreementType) classified.agreementType = "MANAGEMENT";
    }
    upgraded.push(classified);
    ex10Added.push({
      hotel: obs.hotelName,
      agreementType: obs.agreementType,
      tier: classified.tier,
      end: classified.verifiedEndDate,
      notice: classified.noticePeriodDays,
      sourceUrl: obs.sourceUrl,
    });
  }

  const projections = projectDualProspectLists(upgraded);
  const summary = summarizeCommercialInventory(upgraded, projections);

  // Deduplicate prospect lists by hotel key within each list (keep best tier)
  function dedupeList(list) {
    const tierRank = {
      [OPPORTUNITY_TIER.TIER_A_VERIFIED_DECISION_WINDOW]: 1,
      [OPPORTUNITY_TIER.TIER_B_VERIFIED_CONTRACT_EXPIRATION]: 2,
      [OPPORTUNITY_TIER.TIER_C_CONVERSION_RESEARCH_CANDIDATE]: 3,
      [OPPORTUNITY_TIER.TIER_D_FURTHER_RESEARCH_REQUIRED]: 4,
    };
    const byHotel = new Map();
    for (const row of list) {
      const k = hotelKey(row.hotel);
      if (!k) continue;
      const prev = byHotel.get(k);
      if (!prev) {
        byHotel.set(k, row);
        continue;
      }
      const pr = tierRank[prev.opportunityTier] || 9;
      const nr = tierRank[row.opportunityTier] || 9;
      if (nr < pr) byHotel.set(k, row);
      else if (nr === pr && row.verifiedEndDate && !prev.verifiedEndDate) byHotel.set(k, row);
    }
    return [...byHotel.values()];
  }

  const brandDeduped = dedupeList(projections.brandConversionTargets);
  const mgmtDeduped = dedupeList(projections.managementContractTargets);

  const tierB = upgraded.filter((o) => o.tier === OPPORTUNITY_TIER.TIER_B_VERIFIED_CONTRACT_EXPIRATION);
  const tierC = upgraded.filter((o) => o.tier === OPPORTUNITY_TIER.TIER_C_CONVERSION_RESEARCH_CANDIDATE);
  const reconciliation = {
    version: "commercial-inventory-reconcile-v5",
    asOf,
    prNote: "PR #503 — no research horizon; dual lists; EX-10 management merge",
    priorMetricsSnapshot: {
      distinctHotelObservations: metricsPrior.distinctHotelObservations,
      tierB: metricsPrior.byTier?.TIER_B_VERIFIED_CONTRACT_EXPIRATION,
      tierC: metricsPrior.byTier?.TIER_C_CONVERSION_RESEARCH_CANDIDATE,
      hold: metricsPrior.byTier?.HOLD_QUARANTINE,
      brandList: brandPrior.count ?? metricsPrior.brandConversionListCount,
      managementList: mgmtPrior.count ?? metricsPrior.managementContractListCount,
    },
    explanation: {
      whyBrandListWas136Not76Plus71:
        "projectDualProspectLists includes (a) all FRANCHISE Tier A/B rows and (b) Tier C UNKNOWN rows only when brand/conversionSignals.brand are present. Of 71 Tier C, only those with brand signals joined Brand Conversion — yielding 76 Franchise Tier B + ~60 branded Tier C ≈ 136. Management list was 0 because OPPORTUNITIES had 0 MANAGEMENT agreementType rows (all CMBS franchise-like).",
      tierBVsBrandList:
        "Tier B (76) = verified hotel-specific contractual expirations (all FRANCHISE in prior inventory). Brand Conversion list (136) = Tier B franchise rows + Tier C brand-signal research candidates. Not the same grain.",
      historicalVsFutureTierB: {
        futureDated: tierB.filter((o) => o.monthsToExpiration != null && o.monthsToExpiration >= 0).length,
        historicallyExpired: tierB.filter((o) => o.monthsToExpiration != null && o.monthsToExpiration < 0).length,
        note: "All 76 remain research-eligible. Near-term outreach priority is optional app metadata only.",
      },
      managementGapRootCause:
        "Scale pilot source family was CMBS franchise expiration schedules; EX-10 named MANAGEMENT agreements were not projected into MANAGEMENT_CONTRACT_TARGETS.json (count 0).",
      duplicatesVsUniqueHotels:
        "Prior 446 observations ≈ 442 distinct hotel names; researchId is hotel|type|date|source — multiple agreements/filings can share a hotel.",
    },
    after: {
      opportunityCount: upgraded.length,
      summary,
      brandListRows: brandDeduped.length,
      managementListRows: mgmtDeduped.length,
      ex10ManagementMerged: ex10Added.length,
      ex10AddedSamples: ex10Added.slice(0, 20),
    },
    appFilterExamples: {
      allBrand: brandDeduped.length,
      historicalBrand: filterProspectList(brandDeduped, { dateFilter: APP_DATE_FILTER.HISTORICAL_EXPIRATIONS }).length,
      futureBrand: filterProspectList(brandDeduped, { dateFilter: APP_DATE_FILTER.FUTURE_EXPIRATIONS }).length,
      next36Brand: filterProspectList(brandDeduped, { dateFilter: APP_DATE_FILTER.NEXT_36_MONTHS }).length,
      nearTermBrand: filterProspectList(brandDeduped, { nearTermOnly: true }).length,
      allMgmt: mgmtDeduped.length,
    },
    globalCoverage: {
      geographicResearchRestriction: "NONE",
      researchHorizonFilter: "NONE",
      note: "Country/region filters are application-only; inventory retains worldwide eligibility.",
    },
  };

  // Persist upgraded dual lists (canonical commercial-targets-v1 paths + reconcile pack)
  writeJson(path.join(COMM, "OPPORTUNITIES.json"), { opportunities: upgraded, asOf, researchHorizonFilter: "NONE" });
  writeJson(path.join(COMM, "BRAND_CONVERSION_TARGETS.json"), {
    count: brandDeduped.length,
    publicationState: "CUSTOMER_BLOCKED",
    researchHorizonFilter: "NONE",
    targets: brandDeduped,
  });
  writeJson(path.join(COMM, "MANAGEMENT_CONTRACT_TARGETS.json"), {
    count: mgmtDeduped.length,
    publicationState: "CUSTOMER_BLOCKED",
    researchHorizonFilter: "NONE",
    targets: mgmtDeduped,
  });
  writeJson(path.join(COMM, "PILOT_METRICS.json"), {
    ...metricsPrior,
    ...summary,
    reconciledAt: asOf,
    brandConversionListCount: brandDeduped.length,
    managementContractListCount: mgmtDeduped.length,
    ex10ManagementMerged: ex10Added.length,
  });

  writeJson(path.join(OUT, "RECONCILIATION.json"), reconciliation);
  writeJson(path.join(OUT, "EX10_MANAGEMENT_MERGED.json"), { count: ex10Added.length, rows: ex10Added });
  writeJson(path.join(OUT, "TIER_B_ALL_76.json"), {
    count: tierB.length,
    future: tierB.filter((o) => o.monthsToExpiration != null && o.monthsToExpiration >= 0),
    historical: tierB.filter((o) => o.monthsToExpiration != null && o.monthsToExpiration < 0),
  });

  const founder = [];
  founder.push("# FOUNDER REPORT — Commercial Inventory Reconciliation (No Research Horizon)");
  founder.push("");
  founder.push(`As of: ${asOf}`);
  founder.push(`PR: #503 · researchHorizonFilter: **NONE** · geographicRestriction: **NONE**`);
  founder.push("");
  founder.push("## Why Brand Conversion (136) ≠ Tier B (76) + Tier C (71)");
  founder.push(reconciliation.explanation.whyBrandListWas136Not76Plus71);
  founder.push("");
  founder.push(reconciliation.explanation.tierBVsBrandList);
  founder.push("");
  founder.push("## Management Contract list was 0");
  founder.push(reconciliation.explanation.managementGapRootCause);
  founder.push(`After EX-10 named MANAGEMENT merge: **${mgmtDeduped.length}** distinct management-contract prospect hotels.`);
  founder.push("");
  founder.push("## Tier B (76) — all remain in research scope");
  founder.push(
    `- Future-dated: **${reconciliation.explanation.historicalVsFutureTierB.futureDated}**`,
  );
  founder.push(
    `- Historically expired (status follow-up): **${reconciliation.explanation.historicalVsFutureTierB.historicallyExpired}**`,
  );
  founder.push("- No 36-month eligibility cutoff. `within36Months` / near-term flags are **application filter metadata only**.");
  founder.push("");
  founder.push("## Dual lists after reconcile");
  founder.push(`- Brand Conversion Targets (deduped hotels): **${brandDeduped.length}**`);
  founder.push(`- Management Contract Targets (deduped hotels): **${mgmtDeduped.length}**`);
  founder.push(`- Research-eligible opportunities: **${summary.researchEligibleCount}**`);
  founder.push(`- Verified expirations: **${summary.verifiedExpirations}** (historical ${summary.historicalContractExpirations} / future ${summary.futureContractExpirations})`);
  founder.push(`- HOLD/quarantine: **${summary.holdQuarantine}** (false opening/loan dates remain excluded from prospect lists)`);
  founder.push("");
  founder.push("## App filter examples (do not delete inventory)");
  for (const [k, v] of Object.entries(reconciliation.appFilterExamples)) {
    founder.push(`- ${k}: ${v}`);
  }
  founder.push("");
  founder.push("## Sample management-contract additions");
  for (const s of ex10Added.slice(0, 8)) {
    founder.push(
      `- ${s.hotel} · ${s.agreementType} · end=${s.end || "undisclosed"} · notice=${s.notice ?? "n/a"} · tier=${s.tier}`,
    );
  }
  founder.push("");
  founder.push("## Governance");
  founder.push("- CUSTOMER_BLOCKED · no HPC · no unauthorized Meta schema · IC owns merge");
  fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT_INVENTORY_RECONCILE.md"), founder.join("\n"), "utf8");

  console.log(
    JSON.stringify(
      {
        brand: brandDeduped.length,
        management: mgmtDeduped.length,
        tierB: tierB.length,
        ex10Added: ex10Added.length,
        researchEligible: summary.researchEligibleCount,
        historicalExp: summary.historicalContractExpirations,
        futureExp: summary.futureContractExpirations,
      },
      null,
      2,
    ),
  );
}

main();
