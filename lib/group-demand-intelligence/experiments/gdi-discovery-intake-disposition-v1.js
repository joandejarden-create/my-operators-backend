/**
 * Offline validation harness for gdi-discovery-intake-disposition-v1.
 * Frozen Bethesda (+ optional YOTEL) inventories. $0. No Airtable / publish.
 * Does not claim improved live discovery — classifier metrics only.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  INTAKE_DISPOSITION_VERSION,
  PHASE2B_EXCLUDED_OPPORTUNITY_IDS,
  DISCOVERY_YIELD_LABEL,
  COMMERCIAL_DISPOSITION,
  assessDiscoveryIntake,
  labelDiscoveryYield,
  classifyCommercialDisposition,
  assessOpportunityDrawerReadiness,
  resolveSharedOpportunityIdentity,
  applyIntakeSalesReadyGate,
  buildKnownIndexFromOpportunities,
} from "../research/gdi-discovery-intake-disposition-v1.js";
import { evaluateSharedCommercialEligibility } from "../commercial-eligibility-contract-v1.js";

export const EXPERIMENT_ID = "gdi_discovery_intake_disposition_v1";
export const EXPERIMENT_VERSION = "2026-11-19.1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const GROUND_TRUTH_REL =
  "fixtures/gdi-experiments/gdi-discovery-intake-disposition-v1/ground-truth.json";
const REPORT_DIR_REL = "reports/gdi-discovery-intake-disposition-v1";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function loadJson(abs) {
  return JSON.parse(fs.readFileSync(abs, "utf8"));
}

function loadHotelOpportunities(hotelId) {
  const abs = path.join(
    ROOT,
    "data/group-demand-intelligence/hotels",
    hotelId,
    "opportunities.json",
  );
  if (!fs.existsSync(abs)) return { abs, opportunities: [], missing: true };
  const raw = loadJson(abs);
  const opportunities = Array.isArray(raw)
    ? raw
    : raw.opportunities || raw.items || [];
  return { abs, opportunities, missing: false };
}

function oppId(row) {
  return text(row.id || row.opportunityId) || null;
}

function confusionMetrics(rows, positiveLabel) {
  let tp = 0;
  let fp = 0;
  let tn = 0;
  let fn = 0;
  for (const row of rows) {
    const expectedPositive = row.expected === positiveLabel;
    const predictedPositive = row.predicted === positiveLabel;
    if (expectedPositive && predictedPositive) tp += 1;
    else if (!expectedPositive && predictedPositive) fp += 1;
    else if (!expectedPositive && !predictedPositive) tn += 1;
    else fn += 1;
  }
  const precision = tp + fp === 0 ? null : tp / (tp + fp);
  const recall = tp + fn === 0 ? null : tp / (tp + fn);
  const accuracy =
    rows.length === 0 ? null : (tp + tn) / rows.length;
  return {
    label: positiveLabel,
    tp,
    fp,
    tn,
    fn,
    precision,
    recall,
    accuracy,
    falsePositives: rows.filter((r) => r.predicted === positiveLabel && r.expected !== positiveLabel),
    falseNegatives: rows.filter((r) => r.expected === positiveLabel && r.predicted !== positiveLabel),
  };
}

export function loadGroundTruth() {
  const abs = path.join(ROOT, GROUND_TRUTH_REL);
  return { abs, data: loadJson(abs) };
}

export function runYieldProbes(groundTruth) {
  const probes = groundTruth.yieldProbeCases || [];
  const results = [];
  for (const probe of probes) {
    const labeled = labelDiscoveryYield({
      candidate: {
        organizationName: probe.organizationName || "Probe Org",
        title: probe.title || probe.id,
        fixture: probe.fixture === true,
        historical: probe.historical === true,
        fixtureOnly: probe.fixture === true,
      },
      retrievalMode: probe.retrievalMode,
      isLive: probe.isLive === true,
      freshRetrievals: Number(probe.freshRetrievals || 0),
      inventoryReopen: probe.inventoryReopen === true,
    });
    results.push({
      id: probe.id,
      expected: probe.expectedYield,
      predicted: labeled.discoveryYieldLabel,
      ok: labeled.discoveryYieldLabel === probe.expectedYield,
      claimsGenuineNewDiscovery: labeled.claimsGenuineNewDiscovery,
      novelty: labeled.discoveryNoveltyKinds,
    });
  }
  const liveNewOnNonLive = results.filter(
    (r) =>
      r.predicted === DISCOVERY_YIELD_LABEL.LIVE_NEW &&
      (r.id.includes("fixture") ||
        r.id.includes("historical") ||
        r.id.includes("inventory") ||
        r.id.includes("false_live")),
  );
  return {
    results,
    passCount: results.filter((r) => r.ok).length,
    total: results.length,
    zeroHistoricalOrFixtureLabeledLiveNew: liveNewOnNonLive.length === 0,
    illegalLiveNew: liveNewOnNonLive,
  };
}

export function runDispositionCohort(groundTruth, hotelCaches) {
  const excluded = new Set([
    ...(groundTruth.excludedOpportunityIds || []),
    ...PHASE2B_EXCLUDED_OPPORTUNITY_IDS,
  ]);
  const rows = [];
  const missing = [];
  for (const gt of groundTruth.rows || []) {
    if (excluded.has(gt.opportunityId)) continue;
    const cache = hotelCaches.get(gt.hotelId);
    if (!cache || cache.missing) {
      missing.push({ opportunityId: gt.opportunityId, hotelId: gt.hotelId, reason: "HOTEL_INVENTORY_MISSING" });
      continue;
    }
    const opp = cache.byId.get(gt.opportunityId);
    if (!opp) {
      missing.push({ opportunityId: gt.opportunityId, hotelId: gt.hotelId, reason: "OPPORTUNITY_NOT_FOUND" });
      continue;
    }
    const hotelName =
      (groundTruth.hotels || []).find((h) => h.hotelId === gt.hotelId)?.hotelName ||
      opp.hotelName ||
      "";
    const assessment = assessDiscoveryIntake(opp, {
      targetHotel: hotelName,
      hotelId: gt.hotelId,
      inventoryReopen: true,
      retrievalMode: "INVENTORY_REOPEN",
      isLive: false,
      freshRetrievals: 0,
      knownOrgKeys: cache.known.knownOrgKeys,
      knownProgramKeys: cache.known.knownProgramKeys,
      knownOpportunityIds: cache.known.knownOpportunityIds,
    });
    const predicted = assessment.commercialDisposition.commercialDisposition;
    rows.push({
      opportunityId: gt.opportunityId,
      hotelId: gt.hotelId,
      organizationName: opp.organizationName || opp.company || null,
      expected: gt.expectedDisposition,
      predicted,
      ok: predicted === gt.expectedDisposition,
      labelRationale: gt.labelRationale || null,
      reasons: assessment.commercialDisposition.reasons,
      displacementOpportunity: assessment.commercialDisposition.displacementOpportunity,
      watch: assessment.commercialDisposition.watch,
      discoveryYieldLabel: assessment.discoveryYield.discoveryYieldLabel,
      claimsGenuineNewDiscovery: assessment.discoveryYield.claimsGenuineNewDiscovery,
      sharedDemandKey: assessment.sharedIdentity.sharedDemandKey,
      drawerReady: assessment.drawerReadiness.ready,
      drawerGaps: assessment.drawerReadiness.gaps,
      salesReadyBlockedByIntake: assessment.salesReadyBlockedByIntake,
    });
  }
  const competitor = confusionMetrics(rows, COMMERCIAL_DISPOSITION.COMPETITOR_PLACED);
  const accuracy =
    rows.length === 0 ? null : rows.filter((r) => r.ok).length / rows.length;
  return {
    rows,
    missing,
    scored: rows.length,
    correct: rows.filter((r) => r.ok).length,
    accuracy,
    competitorPlacement: competitor,
    meetsAccuracyTarget: accuracy != null && accuracy >= 0.8,
  };
}

export function runDuplicateDetection(hotelCaches) {
  const keyToHotels = new Map();
  for (const [hotelId, cache] of hotelCaches.entries()) {
    if (cache.missing) continue;
    for (const opp of cache.opportunities) {
      const id = oppId(opp);
      if (!id || PHASE2B_EXCLUDED_OPPORTUNITY_IDS.includes(id)) continue;
      const shared = resolveSharedOpportunityIdentity(opp);
      const key = shared.sharedDemandKey;
      if (!key) continue;
      if (!keyToHotels.has(key)) keyToHotels.set(key, []);
      keyToHotels.get(key).push({
        hotelId,
        opportunityId: id,
        organizationKey: shared.organizationKey,
        programKey: shared.programKey,
      });
    }
  }
  const crossHotelReusable = [...keyToHotels.entries()]
    .map(([key, refs]) => {
      const hotels = [...new Set(refs.map((r) => r.hotelId))];
      return { sharedDemandKey: key, hotels, refs, crossHotel: hotels.length > 1 };
    })
    .filter((x) => x.crossHotel);

  // Within a single hotel, duplicate canonical keys with distinct opportunity IDs
  const withinHotelDupes = [];
  for (const [hotelId, cache] of hotelCaches.entries()) {
    if (cache.missing) continue;
    const byKey = new Map();
    for (const opp of cache.opportunities) {
      const id = oppId(opp);
      if (!id || PHASE2B_EXCLUDED_OPPORTUNITY_IDS.includes(id)) continue;
      const shared = resolveSharedOpportunityIdentity(opp);
      const key = shared.programKey || shared.sharedDemandKey;
      if (!key) continue;
      if (!byKey.has(key)) byKey.set(key, []);
      byKey.get(key).push(id);
    }
    for (const [key, ids] of byKey.entries()) {
      const unique = [...new Set(ids)];
      if (unique.length > 1) {
        withinHotelDupes.push({ hotelId, sharedDemandKey: key, opportunityIds: unique });
      }
    }
  }

  return {
    sharedKeys: keyToHotels.size,
    crossHotelReusableCount: crossHotelReusable.length,
    crossHotelReusable: crossHotelReusable.slice(0, 25),
    withinHotelProgramKeyCollisions: withinHotelDupes.slice(0, 25),
    withinHotelCollisionCount: withinHotelDupes.length,
    // Harness does not invent new canonical opportunities — read-only.
    newCanonicalOpportunitiesIntroduced: 0,
  };
}

export function runDrawerAudit(dispositionRows, hotelCaches) {
  const audits = [];
  for (const row of dispositionRows) {
    const cache = hotelCaches.get(row.hotelId);
    const opp = cache?.byId?.get(row.opportunityId);
    if (!opp) continue;
    const drawer = assessOpportunityDrawerReadiness(opp, { hotelId: row.hotelId });
    audits.push({
      opportunityId: row.opportunityId,
      hotelId: row.hotelId,
      ready: drawer.ready,
      gaps: drawer.gaps,
      fields: drawer.fields,
      certifiedOk: drawer.certifiedDrawer.ok,
    });
  }
  return {
    audited: audits.length,
    readyCount: audits.filter((a) => a.ready).length,
    gapFrequency: audits.reduce((acc, a) => {
      for (const g of a.gaps) acc[g] = (acc[g] || 0) + 1;
      return acc;
    }, {}),
    rows: audits,
  };
}

export function runSalesReadyRegression(dispositionRows, hotelCaches) {
  const checks = [];
  for (const row of dispositionRows) {
    const cache = hotelCaches.get(row.hotelId);
    const opp = cache?.byId?.get(row.opportunityId);
    if (!opp) continue;
    const shared = evaluateSharedCommercialEligibility(opp);
    const gated = applyIntakeSalesReadyGate(shared, {
      commercialDisposition: row.predicted,
    });
    const speculativePromoted =
      row.predicted === COMMERCIAL_DISPOSITION.SPECULATIVE &&
      gated.status === "SALES_READY";
    const competitorPromoted =
      row.predicted === COMMERCIAL_DISPOSITION.COMPETITOR_PLACED &&
      gated.status === "SALES_READY";
    checks.push({
      opportunityId: row.opportunityId,
      disposition: row.predicted,
      eligibilityBefore: shared.status,
      eligibilityAfterGate: gated.status,
      intakeBlockedSalesReady: gated.intakeBlockedSalesReady === true,
      speculativePromoted,
      competitorPromoted,
    });
  }
  return {
    checks,
    speculativePromotedToSalesReady: checks.filter((c) => c.speculativePromoted).length,
    competitorPlacedPromotedToSalesReady: checks.filter((c) => c.competitorPromoted).length,
    noSpeculativeSalesReady: checks.every((c) => !c.speculativePromoted),
  };
}

export function runOfflineValidation(opts = {}) {
  const startedAt = new Date().toISOString();
  const { data: groundTruth } = loadGroundTruth();
  const hotelCaches = new Map();
  for (const hotel of groundTruth.hotels || []) {
    const loaded = loadHotelOpportunities(hotel.hotelId);
    const byId = new Map();
    for (const opp of loaded.opportunities) {
      const id = oppId(opp);
      if (id) byId.set(id, opp);
    }
    hotelCaches.set(hotel.hotelId, {
      ...loaded,
      byId,
      known: buildKnownIndexFromOpportunities(loaded.opportunities),
      hotelName: hotel.hotelName,
    });
  }

  const yieldProbes = runYieldProbes(groundTruth);
  const disposition = runDispositionCohort(groundTruth, hotelCaches);
  const duplicates = runDuplicateDetection(hotelCaches);
  const drawer = runDrawerAudit(disposition.rows, hotelCaches);
  const salesReady = runSalesReadyRegression(disposition.rows, hotelCaches);

  const illegalLiveNewOnInventory = disposition.rows.filter(
    (r) => r.claimsGenuineNewDiscovery === true,
  );

  const acceptance = {
    zeroHistoricalFixtureLabeledLiveNew: yieldProbes.zeroHistoricalOrFixtureLabeledLiveNew,
    zeroInventoryReopenLabeledLiveNew: illegalLiveNewOnInventory.length === 0,
    competitorPlacementPrecision: disposition.competitorPlacement.precision,
    competitorPlacementRecall: disposition.competitorPlacement.recall,
    dispositionAccuracy: disposition.accuracy,
    dispositionAccuracyTargetMet: disposition.meetsAccuracyTarget,
    noSpeculativePromotedToSalesReady: salesReady.noSpeculativeSalesReady,
    noNewCanonicalOpportunities: duplicates.newCanonicalOpportunitiesIntroduced === 0,
    paidUsd: 0,
    phase2bCasesExcluded: true,
    claimsImprovedLiveDiscovery: false,
  };

  const acceptancePass =
    acceptance.zeroHistoricalFixtureLabeledLiveNew &&
    acceptance.zeroInventoryReopenLabeledLiveNew &&
    acceptance.dispositionAccuracyTargetMet &&
    acceptance.noSpeculativePromotedToSalesReady &&
    acceptance.noNewCanonicalOpportunities &&
    acceptance.paidUsd === 0;

  const remainingBlockersToLiveDiscovery = [
    "Paid Exa / live providers require separate founder authorization",
    "Most segment executors remain fixture/historical/blocked without authorizePaidDiscovery",
    "Offline classifier does not create new LIVE_NEW organizations or opportunities",
    "Demand-mechanism mission packs (slice 2+) not yet implemented",
  ];

  const recommendedNextSlice = {
    id: "gdi-discovery-mission-packs-scaling-projects-v1",
    focus:
      "Catchment-local mission packs for corporate_accounts_scaling + projects_infrastructure with $0 query diffs; optional bounded Exa only after founder budget auth",
    dependsOn: EXPERIMENT_ID,
  };

  const report = {
    schema: EXPERIMENT_ID,
    version: EXPERIMENT_VERSION,
    intakeModuleVersion: INTAKE_DISPOSITION_VERSION,
    startedAt,
    finishedAt: new Date().toISOString(),
    paidUsd: 0,
    airtableWrites: 0,
    customerPublication: false,
    phase2bExcludedOpportunityIds: [...PHASE2B_EXCLUDED_OPPORTUNITY_IDS],
    hotels: (groundTruth.hotels || []).map((h) => ({
      hotelId: h.hotelId,
      hotelName: h.hotelName,
      role: h.role,
      opportunityCount: hotelCaches.get(h.hotelId)?.opportunities?.length || 0,
      inventoryMissing: hotelCaches.get(h.hotelId)?.missing === true,
    })),
    yieldProbes,
    disposition,
    duplicates,
    drawer,
    salesReady,
    acceptance,
    acceptancePass,
    claimsImprovedLiveDiscovery: false,
    remainingBlockersToLiveDiscovery,
    recommendedNextSlice,
    disclaimer:
      "Offline classifier experiment only. Do not claim improved live opportunity discovery from these results alone.",
  };

  if (opts.writeReports !== false) {
    const outDir = path.join(ROOT, REPORT_DIR_REL);
    fs.mkdirSync(outDir, { recursive: true });
    fs.writeFileSync(
      path.join(outDir, "RESULTS.json"),
      `${JSON.stringify(report, null, 2)}\n`,
      "utf8",
    );
    const md = renderMarkdown(report);
    fs.writeFileSync(path.join(outDir, "VALIDATION_REPORT.md"), md, "utf8");
    report.reportDir = REPORT_DIR_REL;
  }

  return report;
}

function pct(n) {
  if (n == null || Number.isNaN(n)) return "n/a";
  return `${(n * 100).toFixed(1)}%`;
}

function renderMarkdown(report) {
  const c = report.disposition.competitorPlacement;
  return `# GDI Discovery Intake Disposition V1 — Offline Validation

**Status:** ${report.acceptancePass ? "ACCEPTANCE PASS" : "ACCEPTANCE GAPS"}  
**Paid USD:** ${report.paidUsd}  
**Phase 2B excluded:** ${report.phase2bExcludedOpportunityIds.join(", ")}

> ${report.disclaimer}

## Discovery provenance

- Yield probes passed: ${report.yieldProbes.passCount}/${report.yieldProbes.total}
- Zero historical/fixture labeled \`LIVE_NEW\`: ${report.yieldProbes.zeroHistoricalOrFixtureLabeledLiveNew}
- Zero inventory reopen claiming genuine new discovery: ${report.acceptance.zeroInventoryReopenLabeledLiveNew}

## Commercial disposition (frozen cohort)

- Scored rows: ${report.disposition.scored}
- Accuracy: ${pct(report.disposition.accuracy)} (target ≥80%)
- Competitor-placement precision: ${pct(c.precision)}
- Competitor-placement recall: ${pct(c.recall)}
- False positives: ${(c.falsePositives || []).map((r) => r.opportunityId).join(", ") || "none"}
- False negatives: ${(c.falseNegatives || []).map((r) => r.opportunityId).join(", ") || "none"}

## Duplicates / shared intelligence

- Shared demand keys indexed: ${report.duplicates.sharedKeys}
- Cross-hotel reusable keys: ${report.duplicates.crossHotelReusableCount}
- Within-hotel program-key collisions (informational): ${report.duplicates.withinHotelCollisionCount}
- New canonical opportunities introduced by harness: ${report.duplicates.newCanonicalOpportunitiesIntroduced}

## Drawer readiness

- Audited: ${report.drawer.audited}
- Fully ready: ${report.drawer.readyCount}
- Top gaps: ${Object.entries(report.drawer.gapFrequency || {})
    .sort((a, b) => b[1] - a[1])
    .slice(0, 8)
    .map(([k, v]) => `${k} (${v})`)
    .join("; ") || "none"}

## Qualification / publication gates

- Speculative promoted to SALES_READY: ${report.salesReady.speculativePromotedToSalesReady}
- Competitor-placed promoted to SALES_READY after intake gate: ${report.salesReady.competitorPlacedPromotedToSalesReady}

## Remaining blockers to genuine live discovery

${report.remainingBlockersToLiveDiscovery.map((b) => `- ${b}`).join("\n")}

## Recommended next slice

- **${report.recommendedNextSlice.id}** — ${report.recommendedNextSlice.focus}
`;
}

export default runOfflineValidation;
