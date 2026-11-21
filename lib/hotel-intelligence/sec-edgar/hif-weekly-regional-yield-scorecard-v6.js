/**
 * HIF V6 — Permanent Weekly Regional Coverage & Research Yield Scorecard.
 *
 * Measures actual geographic research coverage, independently verified
 * contractual intelligence, and commercially valuable prospect creation.
 * Does NOT equate document volume or generic hotel discovery with success.
 *
 * Geography: physical hotel location (source jurisdiction preserved separately).
 * Reuses regional register + industrial-scale metric taxonomy + dual-track contract.
 * CUSTOMER_BLOCKED; no Airtable schema apply without founder-approved additive proposal.
 */

import fs from "node:fs";
import path from "node:path";
import {
  HOTEL_INTELLIGENCE_METRIC,
  SCALE_AMBITIONS_V6,
} from "./hif-industrial-scale-operating-contract-v6.js";
import {
  buildEmptyRegionalRegister,
  mapCountryToIntensiveRegion,
  summarizeCoverage,
} from "./hif-regional-coverage-yield-register-v6.js";
import { DISCOVERY_TRACK } from "./hif-v6-shared-research-contract.js";
import {
  WORLDWIDE_ELIGIBILITY_POLICY_V6,
  buildDualGeographyViews,
} from "./hif-v6-worldwide-eligibility-v6.js";

export const HIF_WEEKLY_REGIONAL_YIELD_SCORECARD_V6 =
  "hif-weekly-regional-yield-scorecard-v6";

/** ISO week id YYYY-Www (UTC Monday start). */
export function isoWeekId(dateInput = new Date()) {
  const d = new Date(dateInput);
  const utc = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
  const day = utc.getUTCDay() || 7;
  utc.setUTCDate(utc.getUTCDate() + 4 - day);
  const yearStart = new Date(Date.UTC(utc.getUTCFullYear(), 0, 1));
  const weekNo = Math.ceil(((utc - yearStart) / 86400000 + 1) / 7);
  return `${utc.getUTCFullYear()}-W${String(weekNo).padStart(2, "0")}`;
}

export const WEEKLY_METRIC_KEYS = Object.freeze([
  "distinctHotelsDiscovered",
  "distinctHotelsIdentityVerified",
  "distinctHotelsResearched",
  "newlyDiscoveredHotelsThisWeek",
  "verifiedPropertyOwnerRelationships",
  "verifiedManagementCompanyRelationships",
  "verifiedFranchiseExpirations",
  "verifiedManagementAgreementExpirations",
  "historicalExpirations",
  "futureExpirations",
  "verifiedApplicableNoticePeriods",
  "verifiedNoticeDeadlines",
  "verifiedContractualStatusChanges",
  "newBrandConversionTargets",
  "newManagementContractTargets",
  "actionableCommercialOpportunities",
  "evidenceBackedOpportunityCandidates",
  "newlyQualifiedCommercialOpportunities",
  "progressToward1000Opportunities",
  "researchCandidates",
  "holdQuarantinedRecords",
  "documentsRetrievedAndProcessed",
  "researchCostUsd",
]);

/** Coverage vs qualification ladder (independent measures). */
export const COVERAGE_QUALIFICATION_LADDER = Object.freeze({
  DISCOVERED_HOTEL_IDENTITIES: "DISCOVERED_HOTEL_IDENTITIES",
  VERIFIED_HOTEL_IDENTITIES: "VERIFIED_HOTEL_IDENTITIES",
  PROPERTY_VERIFIED_OWNERS: "PROPERTY_VERIFIED_OWNERS",
  VERIFIED_AGREEMENT_EXISTENCE: "VERIFIED_AGREEMENT_EXISTENCE",
  VERIFIED_CONTRACTUAL_EXPIRATION_DATES: "VERIFIED_CONTRACTUAL_EXPIRATION_DATES",
  VERIFIED_APPLICABLE_NOTICE_DEADLINES: "VERIFIED_APPLICABLE_NOTICE_DEADLINES",
  VERIFIED_CURRENT_CONTRACTUAL_STATUS: "VERIFIED_CURRENT_CONTRACTUAL_STATUS",
  EVIDENCE_BACKED_COMMERCIAL_PROSPECTS: "EVIDENCE_BACKED_COMMERCIAL_PROSPECTS",
  QUALIFIED_ACTIONABLE_OPPORTUNITIES: "QUALIFIED_ACTIONABLE_OPPORTUNITIES",
});

export const SOURCE_FAMILY_YIELD = Object.freeze([
  "SEC_CMBS",
  "EX10",
  "INTERNATIONAL_SECURITIES_REGULATORS",
  "GOVERNMENT_APIS",
  "CORPORATE_REGISTRIES",
  "RECORDED_MANAGEMENT_AGREEMENTS",
  "COURT_INSOLVENCY",
  "OWNERSHIP_PROPERTY_RECORDS",
  "HOTEL_LICENSING_REGISTERS",
  "BRAND_OPERATOR_ANNOUNCEMENTS",
  "OTHER_APPROVED_COMMERCIAL",
]);

/** Week-over-week change taxonomy — reclassification ≠ newly discovered. */
export const WEEKLY_CHANGE_KIND = Object.freeze({
  NET_CHANGE: "NET_CHANGE",
  NEWLY_DISCOVERED: "NEWLY_DISCOVERED",
  IMPROVED_EXISTING: "IMPROVED_EXISTING",
  CLOSED_OR_SUPERSEDED: "CLOSED_OR_SUPERSEDED",
  DOWNGRADED_OR_INVALIDATED: "DOWNGRADED_OR_INVALIDATED",
  RECLASSIFIED_NOT_NEW: "RECLASSIFIED_NOT_NEW",
});

export function emptyWeeklyMetrics() {
  const m = {};
  for (const k of WEEKLY_METRIC_KEYS) m[k] = 0;
  return m;
}

export function emptyLadder() {
  const m = {};
  for (const k of Object.values(COVERAGE_QUALIFICATION_LADDER)) m[k] = 0;
  return m;
}

export function emptySourceYieldRow() {
  return {
    independentlyVerifiedIncrementalFindings: 0,
    documentsProcessed: 0,
    costUsd: 0,
    duplicateFindings: 0,
    holdOrInvalidFindings: 0,
    trackAFindings: 0,
    trackBFindings: 0,
    overlappingFindingsDeduped: 0,
    costPerIncrementalFindingUsd: null,
  };
}

/**
 * Attribute geography by physical hotel location; preserve source jurisdiction separately.
 */
export function attributeHotelGeography(hotel = {}) {
  const hotelCountry =
    hotel.hotelCountry || hotel.country || hotel.physicalCountry || hotel.locationCountry || null;
  const hotelMarket = hotel.market || hotel.city || hotel.hotelMarket || null;
  const region = mapCountryToIntensiveRegion(hotelCountry);
  return {
    hotelCountry: hotelCountry ? String(hotelCountry).toUpperCase() : "UNATTRIBUTED",
    hotelMarket: hotelMarket || null,
    intensiveRegion: region,
    sourceJurisdiction: hotel.sourceJurisdiction || hotel.filingJurisdiction || null,
    geographyRule:
      "PHYSICAL_HOTEL_LOCATION_FOR_COVERAGE; SOURCE_JURISDICTION_PRESERVED_SEPARATELY",
  };
}

export function defaultWeeklyScorecardRoot(cwd = process.cwd()) {
  return path.join(cwd, "reports", "sec-edgar-hif-v6-weekly-scorecard");
}

export function weeklySnapshotPath(weekId, cwd = process.cwd()) {
  return path.join(defaultWeeklyScorecardRoot(cwd), "snapshots", `${weekId}.json`);
}

export function ensureWeeklyScorecardLayout(cwd = process.cwd()) {
  const root = defaultWeeklyScorecardRoot(cwd);
  for (const sub of ["snapshots", "deltas", "exports", "ledgers"]) {
    fs.mkdirSync(path.join(root, sub), { recursive: true });
  }
  return root;
}

/**
 * Build an immutable weekly snapshot envelope.
 * Callers must supply already-deduplicated combined totals (Track A∪B).
 */
export function buildWeeklySnapshot(input = {}) {
  const weekId = input.weekId || isoWeekId(input.asOf || new Date());
  const asOf = input.asOf || new Date().toISOString().slice(0, 10);
  const global = { ...emptyWeeklyMetrics(), ...(input.global || {}) };
  const ladder = { ...emptyLadder(), ...(input.coverageQualificationLadder || {}) };
  const byRegion = input.byRegion || {};
  const byCountry = input.byCountry || {};
  const bySourceFamily = {};
  for (const fam of SOURCE_FAMILY_YIELD) {
    const row = { ...emptySourceYieldRow(), ...(input.bySourceFamily?.[fam] || {}) };
    if (row.independentlyVerifiedIncrementalFindings > 0) {
      row.costPerIncrementalFindingUsd =
        Math.round((Number(row.costUsd) / row.independentlyVerifiedIncrementalFindings) * 100) /
        100;
    }
    bySourceFamily[fam] = row;
  }

  const trackComparison = {
    trackA: input.trackA || emptyWeeklyMetrics(),
    trackB: input.trackB || emptyWeeklyMetrics(),
    overlappingHotelIdsDeduped: Number(input.overlappingHotelIdsDeduped) || 0,
    combinedDeduplicated: global,
    rule: "Avoid double-counting discoveries identified by both tracks",
  };

  const dualGeography = buildDualGeographyViews(byCountry);

  return {
    version: HIF_WEEKLY_REGIONAL_YIELD_SCORECARD_V6,
    weekId,
    asOf,
    immutable: true,
    customerBlocked: true,
    publicationRestricted: true,
    geographicExclusions: "NONE",
    researchHorizonFilter: "NONE",
    geographyAttribution: "PHYSICAL_HOTEL_LOCATION",
    worldwideEligibility: WORLDWIDE_ELIGIBILITY_POLICY_V6,
    /** Required dual views: global inventory + priority-market concentration. */
    globalOpportunityInventory: dualGeography.globalOpportunityInventory,
    priorityMarketPerformance: dualGeography.priorityMarketPerformance,
    discoveryTracks: [
      DISCOVERY_TRACK.TRACK_A_COMMERCIAL_CONTRACT_PROPERTY,
      DISCOVERY_TRACK.TRACK_B_GOVERNMENT_REGULATORY_PUBLIC,
    ],
    metricTaxonomyAligned: HOTEL_INTELLIGENCE_METRIC,
    global,
    coverageQualificationLadder: ladder,
    byRegion,
    byCountry,
    byMarket: input.byMarket || {},
    bySourceFamily,
    trackComparison,
    hotelExamples: Array.isArray(input.hotelExamples) ? input.hotelExamples.slice(0, 50) : [],
    notes: {
      ...(input.notes || {}),
      geographyPolicy:
        "Priority markets (US/MX/BR/GB/CA) control research prioritization only — never eligibility, rejection, or ceilings",
    },
    scaleAmbitions: SCALE_AMBITIONS_V6.aspirationalMilestones,
    progressMilestonesNotCeilings: WORLDWIDE_ELIGIBILITY_POLICY_V6.progressMilestones,
  };
}

export function saveWeeklySnapshot(snapshot, cwd = process.cwd()) {
  ensureWeeklyScorecardLayout(cwd);
  const p = weeklySnapshotPath(snapshot.weekId, cwd);
  if (fs.existsSync(p) && !snapshot.allowOverwrite) {
    const existing = JSON.parse(fs.readFileSync(p, "utf8"));
    if (existing.immutable) {
      return { ok: false, error: "IMMUTABLE_SNAPSHOT_EXISTS", path: p };
    }
  }
  const toWrite = { ...snapshot };
  delete toWrite.allowOverwrite;
  fs.writeFileSync(p, JSON.stringify(toWrite, null, 2), "utf8");
  return { ok: true, path: p };
}

export function loadWeeklySnapshot(weekId, cwd = process.cwd()) {
  const p = weeklySnapshotPath(weekId, cwd);
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

export function listWeeklySnapshots(cwd = process.cwd()) {
  const dir = path.join(defaultWeeklyScorecardRoot(cwd), "snapshots");
  if (!fs.existsSync(dir)) return [];
  return fs
    .readdirSync(dir)
    .filter((f) => f.endsWith(".json"))
    .map((f) => f.replace(/\.json$/, ""))
    .sort();
}

/**
 * Week-over-week delta. Reclassification must use RECLASSIFIED_NOT_NEW counts,
 * never inflate newlyDiscovered.
 */
export function computeWeeklyDelta(previous, current, changeDetail = {}) {
  const prev = previous?.global || emptyWeeklyMetrics();
  const curr = current?.global || emptyWeeklyMetrics();
  const metrics = {};
  for (const k of WEEKLY_METRIC_KEYS) {
    const previousTotal = Number(prev[k]) || 0;
    const currentTotal = Number(curr[k]) || 0;
    const detail = changeDetail[k] || {};
    metrics[k] = {
      previousWeekTotal: previousTotal,
      currentWeekTotal: currentTotal,
      netChange: currentTotal - previousTotal,
      newlyDiscoveredRecords: Number(detail.newlyDiscoveredRecords) || 0,
      improvedExistingRecords: Number(detail.improvedExistingRecords) || 0,
      closedOrSupersededOpportunities: Number(detail.closedOrSupersededOpportunities) || 0,
      downgradedOrInvalidatedFindings: Number(detail.downgradedOrInvalidatedFindings) || 0,
      reclassifiedNotNew: Number(detail.reclassifiedNotNew) || 0,
    };
  }
  return {
    version: HIF_WEEKLY_REGIONAL_YIELD_SCORECARD_V6,
    previousWeekId: previous?.weekId || null,
    currentWeekId: current?.weekId || null,
    rule: "Do not misrepresent a reclassification as a newly discovered opportunity",
    metrics,
    changeKinds: WEEKLY_CHANGE_KIND,
  };
}

export function saveWeeklyDelta(delta, cwd = process.cwd()) {
  ensureWeeklyScorecardLayout(cwd);
  const id = `${delta.previousWeekId || "NONE"}_to_${delta.currentWeekId}`;
  const p = path.join(defaultWeeklyScorecardRoot(cwd), "deltas", `${id}.json`);
  fs.writeFileSync(p, JSON.stringify(delta, null, 2), "utf8");
  return p;
}

/**
 * Regional research gaps — zero findings ≠ no opportunities exist.
 */
export function identifyRegionalResearchGaps(snapshot, regionalRegister = null) {
  const register = regionalRegister || buildEmptyRegionalRegister(snapshot.asOf);
  const coverage = summarizeCoverage(register);
  const intensive = SCALE_AMBITIONS_V6.initialIntensiveRegions;

  const countriesNoProductiveSource = [];
  const hotelsNoAgreementIntel = [];
  const agreementNoOwnership = [];
  const duplicateHeavySources = [];
  const highQuarantineSources = [];
  const regionsNeedingAdapters = [];

  for (const region of intensive) {
    const regBucket = register.byRegion?.[region];
    if (!regBucket || !regBucket.covered) {
      regionsNeedingAdapters.push({
        region,
        reason: "NO_ACTUAL_RESEARCH_RECORDED",
        note: "Zero findings is not proof that no opportunities exist",
      });
    }
  }

  for (const [country, cell] of Object.entries(snapshot.byCountry || {})) {
    const discovered = Number(cell.distinctHotelsDiscovered || cell.hotelsDiscovered) || 0;
    const agreements =
      Number(cell.verifiedFranchiseExpirations || 0) +
      Number(cell.verifiedManagementAgreementExpirations || 0) +
      Number(cell.agreementsIdentified || 0);
    const owners = Number(cell.verifiedPropertyOwnerRelationships || cell.ownersVerified) || 0;
    if (discovered > 0 && agreements === 0) {
      hotelsNoAgreementIntel.push({ country, discovered });
    }
    if (agreements > 0 && owners === 0) {
      agreementNoOwnership.push({ country, agreements });
    }
  }

  for (const [fam, row] of Object.entries(snapshot.bySourceFamily || {})) {
    const incr = Number(row.independentlyVerifiedIncrementalFindings) || 0;
    const dup = Number(row.duplicateFindings) || 0;
    const hold = Number(row.holdOrInvalidFindings) || 0;
    if (incr > 0 && dup / (incr + dup) >= 0.5) {
      duplicateHeavySources.push({ sourceFamily: fam, duplicateFindings: dup, incremental: incr });
    }
    if (hold > 0 && (incr === 0 || hold / (hold + incr) >= 0.4)) {
      highQuarantineSources.push({ sourceFamily: fam, holdOrInvalidFindings: hold, incremental: incr });
    }
  }

  for (const [country, region] of Object.entries(GAP_SCAN_COUNTRY_REGION_PAIRS)) {
    const cell = snapshot.byCountry?.[country];
    const productive =
      cell &&
      ((Number(cell.documentsRetrievedAndProcessed) || 0) > 0 ||
        (Number(cell.distinctHotelsDiscovered) || 0) > 0 ||
        (Number(cell.verifiedFranchiseExpirations) || 0) > 0);
    if (!productive) {
      countriesNoProductiveSource.push({ country, region });
    }
  }

  return {
    version: HIF_WEEKLY_REGIONAL_YIELD_SCORECARD_V6,
    weekId: snapshot.weekId,
    rule: "Do not interpret zero findings as proof that no opportunities exist",
    coverageSummary: coverage,
    countriesWithNoProductiveSourceCoverage: countriesNoProductiveSource.slice(0, 200),
    countriesWithVerifiedHotelsButNoAgreementIntelligence: hotelsNoAgreementIntel,
    countriesWithAgreementEvidenceButNoVerifiedOwnership: agreementNoOwnership,
    sourcesProducingMainlyDuplicates: duplicateHeavySources,
    sourcesProducingSignificantInvalidOrQuarantinedFindings: highQuarantineSources,
    regionsRequiringNewSourceAdaptersOrStrategies: regionsNeedingAdapters,
  };
}

/** Starter country set for gap scans (intensive Americas/Europe + eligible APAC pilots). */
const GAP_SCAN_COUNTRY_REGION_PAIRS = Object.freeze({
  US: "NORTH_AMERICA",
  CA: "NORTH_AMERICA",
  MX: "NORTH_AMERICA",
  GT: "CENTRAL_AMERICA",
  CR: "CENTRAL_AMERICA",
  PA: "CENTRAL_AMERICA",
  DO: "CARIBBEAN",
  PR: "CARIBBEAN",
  JM: "CARIBBEAN",
  BR: "SOUTH_AMERICA",
  AR: "SOUTH_AMERICA",
  CL: "SOUTH_AMERICA",
  CO: "SOUTH_AMERICA",
  GB: "WESTERN_EUROPE",
  FR: "WESTERN_EUROPE",
  DE: "CENTRAL_EUROPE",
  PL: "CENTRAL_EUROPE",
  SE: "NORTHERN_EUROPE",
  ES: "SOUTHERN_EUROPE",
  IT: "SOUTHERN_EUROPE",
  PT: "SOUTHERN_EUROPE",
  RO: "EASTERN_EUROPE",
  SG: "OTHER_ELIGIBLE",
  JP: "OTHER_ELIGIBLE",
  HK: "OTHER_ELIGIBLE",
});

/**
 * Investment recommendations grounded in verification yield, coverage, and cost.
 */
export function buildResearchInvestmentRecommendations(snapshot, gaps = null) {
  const sources = Object.entries(snapshot.bySourceFamily || {}).map(([id, row]) => ({
    id,
    incremental: Number(row.independentlyVerifiedIncrementalFindings) || 0,
    cost: Number(row.costUsd) || 0,
    costPer: row.costPerIncrementalFindingUsd,
    hold: Number(row.holdOrInvalidFindings) || 0,
    dup: Number(row.duplicateFindings) || 0,
  }));

  const highYield = sources
    .filter((s) => s.incremental > 0)
    .sort((a, b) => b.incremental - a.incremental || (a.costPer ?? 1e9) - (b.costPer ?? 1e9))
    .slice(0, 5);

  const scaleWorth = highYield.filter((s) => (s.costPer == null || s.costPer < 50) && s.hold <= s.incremental);

  const deprioritize = sources
    .filter((s) => s.dup > s.incremental || (s.hold > 0 && s.incremental === 0) || (s.cost > 0 && s.incremental === 0))
    .map((s) => ({
      sourceFamily: s.id,
      reason:
        s.cost > 0 && s.incremental === 0
          ? "COST_WITHOUT_VERIFIED_INCREMENTAL_YIELD"
          : s.dup > s.incremental
            ? "MAINLY_DUPLICATES"
            : "HIGH_QUARANTINE_LOW_YIELD",
    }));

  const countries = Object.entries(snapshot.byCountry || {}).map(([country, cell]) => ({
    country,
    verifiedExpirations:
      (Number(cell.verifiedFranchiseExpirations) || 0) +
      (Number(cell.verifiedManagementAgreementExpirations) || 0),
    owners: Number(cell.verifiedPropertyOwnerRelationships) || 0,
    prospects:
      (Number(cell.newBrandConversionTargets) || 0) +
      (Number(cell.newManagementContractTargets) || 0) +
      (Number(cell.actionableCommercialOpportunities) || 0),
    cost: Number(cell.researchCostUsd) || 0,
  }));

  const highestYieldCountries = countries
    .filter((c) => c.verifiedExpirations + c.owners + c.prospects > 0)
    .sort(
      (a, b) =>
        b.verifiedExpirations + b.owners + b.prospects - (a.verifiedExpirations + a.owners + a.prospects),
    )
    .slice(0, 10);

  const gapList = gaps || identifyRegionalResearchGaps(snapshot);
  const researchGapsWorthInvestigating = (
    gapList.regionsRequiringNewSourceAdaptersOrStrategies || []
  ).slice(0, 9);

  return {
    version: HIF_WEEKLY_REGIONAL_YIELD_SCORECARD_V6,
    weekId: snapshot.weekId,
    grounding: "Observed verification yield, source coverage, and cost — not document counts alone",
    highestYieldSources: highYield,
    highestYieldCountries,
    sourcesWorthScaling: scaleWorth,
    researchGapsWorthInvestigating,
    lowYieldPathsToDeprioritize: deprioritize,
    incrementalResearchSpendUsd: Number(snapshot.global?.researchCostUsd) || 0,
    recommendedNextDiscoveryAllocations: [
      ...scaleWorth.slice(0, 3).map((s) => ({
        action: "SCALE_SOURCE",
        sourceFamily: s.id,
        rationale: "Highest verified incremental yield at acceptable cost",
      })),
      ...researchGapsWorthInvestigating.slice(0, 3).map((g) => ({
        action: "OPEN_ADAPTER_OR_STRATEGY",
        region: g.region,
        rationale: g.reason,
      })),
      {
        action: "COMPLETE_PENDING_EX10_WAVE",
        rationale: "Controlled V6 sequencing after PR #503 merge",
      },
    ],
  };
}

/**
 * Founder executive overview export.
 */
export function buildFounderExecutiveOverview(snapshot, delta = null, gaps = null, recommendations = null) {
  const g = snapshot.global || emptyWeeklyMetrics();
  const rec = recommendations || buildResearchInvestmentRecommendations(snapshot, gaps);
  const gap = gaps || identifyRegionalResearchGaps(snapshot);
  return {
    version: HIF_WEEKLY_REGIONAL_YIELD_SCORECARD_V6,
    title: "Weekly Regional Coverage & Research Yield — Founder Overview",
    weekId: snapshot.weekId,
    asOf: snapshot.asOf,
    customerBlocked: true,
    globalResearchProgress: {
      hotelsDiscovered: g.distinctHotelsDiscovered,
      hotelsIdentityVerified: g.distinctHotelsIdentityVerified,
      hotelsResearched: g.distinctHotelsResearched,
      newlyDiscoveredThisWeek: g.newlyDiscoveredHotelsThisWeek,
      ladder: snapshot.coverageQualificationLadder,
      weekOverWeek: delta?.metrics
        ? {
            discoveredNet: delta.metrics.distinctHotelsDiscovered?.netChange,
            verifiedExpirationsNet:
              (delta.metrics.verifiedFranchiseExpirations?.netChange || 0) +
              (delta.metrics.verifiedManagementAgreementExpirations?.netChange || 0),
            brandTargetsNet: delta.metrics.newBrandConversionTargets?.netChange,
            managementTargetsNet: delta.metrics.newManagementContractTargets?.netChange,
            costNet: delta.metrics.researchCostUsd?.netChange,
          }
        : null,
    },
    regionalCoverage: {
      byRegion: snapshot.byRegion,
      intensiveRegionsNeedingAdapters: gap.regionsRequiringNewSourceAdaptersOrStrategies,
    },
    franchiseVersusManagement: {
      verifiedFranchiseExpirations: g.verifiedFranchiseExpirations,
      verifiedManagementAgreementExpirations: g.verifiedManagementAgreementExpirations,
      newBrandConversionTargets: g.newBrandConversionTargets,
      newManagementContractTargets: g.newManagementContractTargets,
      historicalExpirations: g.historicalExpirations,
      futureExpirations: g.futureExpirations,
    },
    newlyVerifiedContractualExpirations:
      (Number(g.verifiedFranchiseExpirations) || 0) +
      (Number(g.verifiedManagementAgreementExpirations) || 0),
    newOwnerOperatorRelationships: {
      owners: g.verifiedPropertyOwnerRelationships,
      managementCompanies: g.verifiedManagementCompanyRelationships,
    },
    newCommerciallyQualifiedOpportunities: g.actionableCommercialOpportunities,
    weeklyResearchCostsUsd: g.researchCostUsd,
    bestPerformingSources: rec.highestYieldSources,
    highestPriorityGeographicGaps: gap.regionsRequiringNewSourceAdaptersOrStrategies,
    recommendedNextResearchWave: rec.recommendedNextDiscoveryAllocations,
    countryLevelBreakdown: snapshot.byCountry,
    evidenceBackedHotelExamples: snapshot.hotelExamples || [],
    trackComparison: snapshot.trackComparison,
  };
}

export function exportFounderOverviewMarkdown(overview) {
  const lines = [];
  lines.push(`# ${overview.title}`);
  lines.push("");
  lines.push(`**Week:** ${overview.weekId} · **As of:** ${overview.asOf} · **CUSTOMER_BLOCKED**`);
  lines.push("");
  lines.push("## Global research progress");
  const g = overview.globalResearchProgress || {};
  lines.push(`- Hotels discovered: ${g.hotelsDiscovered ?? 0}`);
  lines.push(`- Identity verified: ${g.hotelsIdentityVerified ?? 0}`);
  lines.push(`- Researched: ${g.hotelsResearched ?? 0}`);
  lines.push(`- Newly discovered this week: ${g.newlyDiscoveredThisWeek ?? 0}`);
  lines.push("");
  lines.push("## Franchise vs management");
  const fm = overview.franchiseVersusManagement || {};
  lines.push(`- Verified franchise expirations: ${fm.verifiedFranchiseExpirations ?? 0}`);
  lines.push(`- Verified management expirations: ${fm.verifiedManagementAgreementExpirations ?? 0}`);
  lines.push(`- New Brand Conversion Targets: ${fm.newBrandConversionTargets ?? 0}`);
  lines.push(`- New Management Contract Targets: ${fm.newManagementContractTargets ?? 0}`);
  lines.push(`- Historical / future expirations: ${fm.historicalExpirations ?? 0} / ${fm.futureExpirations ?? 0}`);
  lines.push("");
  lines.push("## Owners / operators");
  lines.push(`- Property-verified owners: ${overview.newOwnerOperatorRelationships?.owners ?? 0}`);
  lines.push(
    `- Verified management companies: ${overview.newOwnerOperatorRelationships?.managementCompanies ?? 0}`,
  );
  lines.push("");
  lines.push(`## Weekly research cost: $${overview.weeklyResearchCostsUsd ?? 0}`);
  lines.push("");
  lines.push("## Best-performing sources");
  for (const s of overview.bestPerformingSources || []) {
    lines.push(
      `- ${s.id}: +${s.incremental} verified incremental · $${s.cost} · $${s.costPer ?? "n/a"}/finding`,
    );
  }
  lines.push("");
  lines.push("## Highest-priority geographic gaps");
  for (const g2 of overview.highestPriorityGeographicGaps || []) {
    lines.push(`- ${g2.region}: ${g2.reason} (zero ≠ none exist)`);
  }
  lines.push("");
  lines.push("## Recommended next research wave");
  for (const a of overview.recommendedNextResearchWave || []) {
    lines.push(`- ${a.action}${a.sourceFamily ? ` · ${a.sourceFamily}` : ""}${a.region ? ` · ${a.region}` : ""} — ${a.rationale}`);
  }
  lines.push("");
  lines.push("## Country-level (non-zero excerpt)");
  for (const [country, cell] of Object.entries(overview.countryLevelBreakdown || {})) {
    const exp =
      (Number(cell.verifiedFranchiseExpirations) || 0) +
      (Number(cell.verifiedManagementAgreementExpirations) || 0);
    const disc = Number(cell.distinctHotelsDiscovered) || 0;
    if (disc || exp) lines.push(`- ${country}: discovered ${disc} · verified expirations ${exp}`);
  }
  lines.push("");
  lines.push("## Evidence-backed hotel examples");
  const examples = overview.evidenceBackedHotelExamples || [];
  if (!examples.length) {
    lines.push("_No hotel examples in this snapshot (do not invent)._");
  } else {
    for (const h of examples) {
      lines.push(
        `- ${h.hotelName || h.name || "Hotel"} (${h.hotelCountry || h.country || "?"}) · ${h.agreementType || "?"} · ${h.expirationDate || "n/a"} · ${h.sourceUrl || ""}`,
      );
    }
  }
  lines.push("");
  return lines.join("\n");
}

/**
 * Seed baseline weekly snapshot from V5 industrial-scale scorecard numbers.
 * Marks US / NORTH_AMERICA as the only region with actual research.
 */
export function buildV5BaselineWeeklySnapshot(opts = {}) {
  const weekId = opts.weekId || "2026-W47";
  const asOf = opts.asOf || "2026-11-21";
  const global = emptyWeeklyMetrics();
  global.distinctHotelsDiscovered = 446;
  global.distinctHotelsIdentityVerified = 150;
  global.distinctHotelsResearched = 446;
  global.newlyDiscoveredHotelsThisWeek = 0; // baseline seed — not a discovery week claim
  global.verifiedPropertyOwnerRelationships = 0;
  global.verifiedManagementCompanyRelationships = 0;
  global.verifiedFranchiseExpirations = 79; // CMBS/franchise-heavy V5 inventory
  global.verifiedManagementAgreementExpirations = 4; // management list strict (aligned to prospect seed)
  global.historicalExpirations = 60;
  global.futureExpirations = 19;
  global.verifiedApplicableNoticePeriods = 2;
  global.verifiedNoticeDeadlines = 2;
  global.verifiedContractualStatusChanges = 0;
  global.newBrandConversionTargets = 136;
  global.newManagementContractTargets = 4;
  global.actionableCommercialOpportunities = 139;
  global.researchCandidates = 0;
  global.holdQuarantinedRecords = 299;
  global.documentsRetrievedAndProcessed = 0; // not claimed without ledger join
  global.researchCostUsd = 0;

  const ladder = emptyLadder();
  ladder.DISCOVERED_HOTEL_IDENTITIES = 446;
  ladder.VERIFIED_HOTEL_IDENTITIES = 150;
  ladder.PROPERTY_VERIFIED_OWNERS = 0;
  ladder.VERIFIED_AGREEMENT_EXISTENCE = 81;
  ladder.VERIFIED_CONTRACTUAL_EXPIRATION_DATES = 79;
  ladder.VERIFIED_APPLICABLE_NOTICE_DEADLINES = 2;
  ladder.VERIFIED_CURRENT_CONTRACTUAL_STATUS = 0;
  ladder.EVIDENCE_BACKED_COMMERCIAL_PROSPECTS = 139;
  ladder.QUALIFIED_ACTIONABLE_OPPORTUNITIES = 139;

  const byCountry = {
    US: {
      ...emptyWeeklyMetrics(),
      distinctHotelsDiscovered: 446,
      distinctHotelsIdentityVerified: 150,
      distinctHotelsResearched: 446,
      verifiedFranchiseExpirations: 79,
      verifiedManagementAgreementExpirations: 4,
      historicalExpirations: 60,
      futureExpirations: 19,
      verifiedApplicableNoticePeriods: 2,
      verifiedNoticeDeadlines: 2,
      newBrandConversionTargets: 136,
      newManagementContractTargets: 4,
      actionableCommercialOpportunities: 139,
      holdQuarantinedRecords: 299,
    },
  };

  const byRegion = {
    NORTH_AMERICA: {
      covered: true,
      coverageNote: "ACTUAL_RESEARCH_RECORDED",
      ...byCountry.US,
    },
  };

  const bySourceFamily = {
    SEC_CMBS: {
      ...emptySourceYieldRow(),
      independentlyVerifiedIncrementalFindings: 75,
      trackAFindings: 75,
    },
    EX10: {
      ...emptySourceYieldRow(),
      independentlyVerifiedIncrementalFindings: 4,
      trackAFindings: 4,
      notes: "62-exhibit harvest pending post-merge wave",
    },
  };

  return buildWeeklySnapshot({
    weekId,
    asOf,
    global,
    coverageQualificationLadder: ladder,
    byRegion,
    byCountry,
    bySourceFamily,
    trackA: { ...global },
    trackB: emptyWeeklyMetrics(),
    overlappingHotelIdsDeduped: 0,
    hotelExamples: opts.hotelExamples || [],
    notes: {
      seed: "V5_BASELINE_UNDER_V6_TAXONOMY",
      rule: "Baseline totals are cumulative inventory — newlyDiscoveredHotelsThisWeek=0",
      gate: "Weekly production refreshes begin after PR #503 merge + research waves",
    },
  });
}

export default {
  HIF_WEEKLY_REGIONAL_YIELD_SCORECARD_V6,
  isoWeekId,
  WEEKLY_METRIC_KEYS,
  COVERAGE_QUALIFICATION_LADDER,
  SOURCE_FAMILY_YIELD,
  WEEKLY_CHANGE_KIND,
  attributeHotelGeography,
  buildWeeklySnapshot,
  saveWeeklySnapshot,
  loadWeeklySnapshot,
  listWeeklySnapshots,
  computeWeeklyDelta,
  saveWeeklyDelta,
  identifyRegionalResearchGaps,
  buildResearchInvestmentRecommendations,
  buildFounderExecutiveOverview,
  exportFounderOverviewMarkdown,
  buildV5BaselineWeeklySnapshot,
  ensureWeeklyScorecardLayout,
};
