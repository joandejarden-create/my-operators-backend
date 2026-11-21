#!/usr/bin/env node
/**
 * Recalculate V5 baseline into V6 industrial-scale metric taxonomy.
 * Offline — no paid calls. Does not claim V6 discovery waves started.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  HOTEL_INTELLIGENCE_METRIC,
  SCALE_AMBITIONS_V6,
  emptyHotelIntelligenceScorecard,
  assertMetricTaxonomyIntegrity,
  RESEARCH_SEQUENCING_AFTER_V5_MERGE,
  DISCOVERY_CONSTRAINTS_V6,
} from "../lib/hotel-intelligence/sec-edgar/hif-industrial-scale-operating-contract-v6.js";
import {
  buildEmptyRegionalRegister,
  recordYieldObservation,
  saveRegionalRegister,
  summarizeCoverage,
  mapCountryToIntensiveRegion,
} from "../lib/hotel-intelligence/sec-edgar/hif-regional-coverage-yield-register-v6.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "reports/sec-edgar-hif-v6-industrial-scale");

function readJson(p, fb = null) {
  if (!fs.existsSync(p)) return fb;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}
function writeJson(p, obj) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, JSON.stringify(obj, null, 2), "utf8");
}
function hotelKey(n) {
  return String(n || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const AS_OF = "2026-11-21";
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
  const recon = readJson(
    path.join(ROOT, "reports/sec-edgar-commercial-inventory-reconcile-v5/RECONCILIATION.json"),
    {},
  );

  const rows = opp.opportunities || [];
  const discoveredKeys = new Set(rows.map((r) => hotelKey(r.hotelName)).filter(Boolean));
  const identityVerified = rows.filter(
    (r) => r.hotelName && r.tier !== "HOLD_QUARANTINE" && String(r.hotelName).length > 3,
  );
  const identityKeys = new Set(identityVerified.map((r) => hotelKey(r.hotelName)));
  const ownerLinked = rows.filter((r) => r.owner && !/REGISTRANT_HINT/i.test(r.ownerNote || ""));
  // V5 qualification: owner fields were largely registrant hints — property-verified owners ≈ 0
  const ownerLinkedStrict = rows.filter(
    (r) => r.owner && r.ownerVerification === "FIELD_PRESENT_UNVERIFIED_DEPTH",
  );
  const agreementIdentified = rows.filter(
    (r) =>
      r.agreementType &&
      r.agreementType !== "UNKNOWN" &&
      r.tier !== "HOLD_QUARANTINE",
  );
  const verifiedExpiration = rows.filter(
    (r) =>
      r.verifiedEndDate &&
      (r.tier === "TIER_A_VERIFIED_DECISION_WINDOW" ||
        r.tier === "TIER_B_VERIFIED_CONTRACT_EXPIRATION"),
  );
  const verifiedNotice = rows.filter(
    (r) =>
      r.noticePeriodDays != null &&
      r.tier !== "HOLD_QUARANTINE" &&
      (r.noticeStatus === "VERIFIED" || r.noticeStatus === "CALCULATED"),
  );
  // Strict: filing-term corroboration (CURRENT_TERMS / HISTORICAL_APPLICABILITY labels) ≠ current-status verified.
  const currentStatusVerified = rows.filter(
    (r) =>
      r.currentStatusVerified === true ||
      r.renewalStatusClass === "CURRENT_STATUS_VERIFIED" ||
      (r.gates && r.gates.CURRENT_STATUS_VERIFIED === true),
  );

  const brandDistinct = new Set((brand.targets || []).map((t) => hotelKey(t.hotel)).filter(Boolean));
  const mgmtDistinct = new Set((mgmt.targets || []).map((t) => hotelKey(t.hotel)).filter(Boolean));
  const qualifiedKeys = new Set([...brandDistinct, ...mgmtDistinct]);

  const scorecard = emptyHotelIntelligenceScorecard(AS_OF);
  scorecard.metrics[HOTEL_INTELLIGENCE_METRIC.DISCOVERED_HOTELS] =
    recon?.after?.summary?.distinctHotels ?? discoveredKeys.size;
  scorecard.metrics[HOTEL_INTELLIGENCE_METRIC.IDENTITY_VERIFIED_HOTELS] = identityKeys.size;
  scorecard.metrics[HOTEL_INTELLIGENCE_METRIC.OWNER_LINKED_HOTELS] = ownerLinkedStrict.length
    ? new Set(ownerLinkedStrict.map((r) => hotelKey(r.hotelName))).size
    : 0;
  scorecard.metrics[HOTEL_INTELLIGENCE_METRIC.AGREEMENT_IDENTIFIED_HOTELS] = new Set(
    agreementIdentified.map((r) => hotelKey(r.hotelName)),
  ).size;
  scorecard.metrics[HOTEL_INTELLIGENCE_METRIC.VERIFIED_EXPIRATION_HOTELS] = new Set(
    verifiedExpiration.map((r) => hotelKey(r.hotelName)),
  ).size;
  scorecard.metrics[HOTEL_INTELLIGENCE_METRIC.VERIFIED_NOTICE_HOTELS] = new Set(
    verifiedNotice.map((r) => hotelKey(r.hotelName)),
  ).size;
  scorecard.metrics[HOTEL_INTELLIGENCE_METRIC.CURRENT_STATUS_VERIFIED_HOTELS] = new Set(
    currentStatusVerified.map((r) => hotelKey(r.hotelName)),
  ).size;
  scorecard.metrics[HOTEL_INTELLIGENCE_METRIC.QUALIFIED_COMMERCIAL_TARGETS] = qualifiedKeys.size;

  scorecard.listBreakdown = {
    brandConversionDistinctHotels: brandDistinct.size,
    managementContractDistinctHotels: mgmtDistinct.size,
    brandRows: brand.targets?.length ?? brand.count,
    managementRows: mgmt.targets?.length ?? mgmt.count,
  };
  scorecard.v5Reference = {
    distinctHotelsReconciled: 446,
    verifiedExpirations: 79,
    historicalExpirations: 60,
    futureExpirations: 19,
    brandStrict: 136,
    managementStrict: 4,
    holdQuarantine: 299,
    propertyVerifiedOwnersInQualificationSnapshot: 0,
  };
  scorecard.integrity = assertMetricTaxonomyIntegrity(scorecard);
  scorecard.ambitionGap = {
    researchedHotels:
      SCALE_AMBITIONS_V6.aspirationalMilestones.distinctResearchedHotelsInitialScale -
      scorecard.metrics.DISCOVERED_HOTELS,
    ownerLinks:
      SCALE_AMBITIONS_V6.aspirationalMilestones.verifiedOwnerPropertyRelationships -
      scorecard.metrics.OWNER_LINKED_HOTELS,
    verifiedExpirations:
      SCALE_AMBITIONS_V6.aspirationalMilestones.hotelSpecificVerifiedFranchiseOrManagementExpirations -
      scorecard.metrics.VERIFIED_EXPIRATION_HOTELS,
    commercialProspects:
      SCALE_AMBITIONS_V6.aspirationalMilestones.evidenceBackedCommercialResearchProspects -
      scorecard.metrics.QUALIFIED_COMMERCIAL_TARGETS,
  };
  scorecard.executionGate = {
    v5Pr503Merged: false,
    v6WaveExecutionAuthorized: false,
    blocker: "AWAITING_INTEGRATION_CONTROLLER — PR #503 OPEN / BEHIND main",
    sequencingAfterMerge: RESEARCH_SEQUENCING_AFTER_V5_MERGE,
    constraints: DISCOVERY_CONSTRAINTS_V6,
  };

  // Regional register — V5 inventory is US-SEC dominant / geography often UNKNOWN
  let register = buildEmptyRegionalRegister(AS_OF);
  const byCountry = {};
  for (const r of rows) {
    const c = String(r.country || "UNKNOWN").toUpperCase();
    byCountry[c] = byCountry[c] || { hotels: new Set(), exp: 0, notice: 0, docs: new Set() };
    if (r.hotelName) byCountry[c].hotels.add(hotelKey(r.hotelName));
    if (r.verifiedEndDate && (r.tier || "").includes("TIER_")) byCountry[c].exp += 1;
    if (r.noticePeriodDays != null) byCountry[c].notice += 1;
    if (r.sourceUrl) byCountry[c].docs.add(r.sourceUrl);
  }
  for (const [country, agg] of Object.entries(byCountry)) {
    const secImpliedUs =
      country === "UNKNOWN" &&
      [...agg.docs].some((u) => /sec\.gov/i.test(u));
    const region = secImpliedUs
      ? "NORTH_AMERICA"
      : country === "UNKNOWN"
        ? "UNATTRIBUTED"
        : mapCountryToIntensiveRegion(country);
    register = recordYieldObservation(register, {
      country: secImpliedUs ? "US_SEC_IMPLIED" : country,
      region,
      hotelsDiscovered: agg.hotels.size,
      hotelsIdentityVerified: agg.hotels.size,
      contractDatesVerified: agg.exp,
      noticePeriodsVerified: agg.notice,
      documentsExamined: agg.docs.size,
      sourceType: "SEC_EDGAR_CMBS_EX10_MIXED",
      agreementFamily: "FRANCHISE_OR_MANAGEMENT_OR_UNKNOWN",
      lastResearchedDate: AS_OF,
      sourceAvailability: secImpliedUs
        ? "US_SEC_RESEARCH_GEOGRAPHY_IMPLIED_NOT_FIELD_VERIFIED"
        : "PARTIAL",
      costUsd: 0,
    });
  }
  // Mark intensive non-US regions still eligible-not-covered
  for (const r of SCALE_AMBITIONS_V6.initialIntensiveRegions) {
    if (r === "NORTH_AMERICA") continue;
    if (!register.byRegion[r].covered) {
      register.byRegion[r].coverageNote =
        "ELIGIBLE_NOT_COVERED — no verified non-US research wave completed yet";
    }
  }

  const coverageSummary = summarizeCoverage(register);
  writeJson(path.join(OUT, "V5_BASELINE_SCORECARD_V6_TAXONOMY.json"), scorecard);
  saveRegionalRegister(register, path.join(OUT, "REGIONAL_COVERAGE_YIELD_REGISTER.json"));
  writeJson(path.join(OUT, "COVERAGE_SUMMARY.json"), coverageSummary);
  writeJson(path.join(OUT, "SCALE_AMBITIONS.json"), SCALE_AMBITIONS_V6);

  console.log(
    JSON.stringify(
      {
        integrity: scorecard.integrity,
        metrics: scorecard.metrics,
        ambitionGap: scorecard.ambitionGap,
        coverageSummary,
        gate: scorecard.executionGate.blocker,
      },
      null,
      2,
    ),
  );
}

main();
