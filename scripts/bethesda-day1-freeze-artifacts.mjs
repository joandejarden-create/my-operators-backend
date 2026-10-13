#!/usr/bin/env node
/**
 * Bethesda Pilot 001 Day-1 freeze artifacts (read-only freeze manifests).
 * Does not mutate opportunity/ADP run payloads. Does not rotate tokens.
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";

const ROOT = process.cwd();
const OUT = path.join(ROOT, "reports/bethesda-pilot/day1/2026-10-01");
fs.mkdirSync(OUT, { recursive: true });

const HOTEL_ID = "recLuxvwwxID7U2B8";
const ADP_PROPERTY = "adp_bethesda_marriott";
const PERIOD_ID = "adp_period_adp_bethesda_marriott_20260909091016_9f3a60";
const TOKEN_ID = "gdisht_47c25d74c79216021fb36150";
const DAY1 = "2026-10-01";

function readJson(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function sha256File(p) {
  const buf = fs.readFileSync(p);
  return crypto.createHash("sha256").update(buf).digest("hex");
}

const corpusPath = path.join(
  ROOT,
  "reports/group-demand-intelligence/bethesda-completion-v1/CURRENT_BETHESDA_CORPUS.json"
);
const oppPath = path.join(
  ROOT,
  `data/group-demand-intelligence/hotels/${HOTEL_ID}/opportunities.json`
);
const feedbackPath = path.join(
  ROOT,
  `data/group-demand-intelligence/hotels/${HOTEL_ID}/feedback.json`
);
const adpManifestPath = path.join(
  ROOT,
  `data/ai-demand-positioning/published/${ADP_PROPERTY}/manifest.json`
);
const adpRuntimePath = path.join(
  ROOT,
  `data/ai-demand-positioning/runtime/${PERIOD_ID}.json`
);
const contractPath = path.join(
  ROOT,
  "config/client-share/production-share-contract-tokens.json"
);

const corpus = readJson(corpusPath);
const oppsWrap = readJson(oppPath);
const feedback = fs.existsSync(feedbackPath) ? readJson(feedbackPath) : { items: [] };
const adpManifest = readJson(adpManifestPath);
const adpRuntime = fs.existsSync(adpRuntimePath) ? readJson(adpRuntimePath) : {};
const contract = readJson(contractPath);

const opps = Array.isArray(oppsWrap.opportunities)
  ? oppsWrap.opportunities
  : Array.isArray(corpus)
    ? corpus
    : [];

function countWhere(arr, fn) {
  return arr.filter(fn).length;
}

const gdiSnapshot = {
  snapshotId: "BETHESDA_GDI_DAY1_SNAPSHOT_2026_10_01",
  snapshotDate: DAY1,
  hotelId: HOTEL_ID,
  hotelName: "Bethesda Marriott",
  sourceCorpusPath: path.relative(ROOT, corpusPath).replace(/\\/g, "/"),
  sourceOpportunitiesPath: path.relative(ROOT, oppPath).replace(/\\/g, "/"),
  sourceSha256: {
    corpus: sha256File(corpusPath),
    opportunities: sha256File(oppPath),
  },
  counts: {
    total: opps.length,
    strictReady: countWhere(
      corpus,
      (r) => r.strictReady === true || r.classLabel === "STRICT_READY"
    ),
    visible: countWhere(corpus, (r) => r.visible === true),
    high: countWhere(corpus, (r) => r.priority === "HIGH_PRIORITY"),
    medium: countWhere(corpus, (r) => r.priority === "MEDIUM_PRIORITY"),
    futureWatchStatus: countWhere(corpus, (r) => r.status === "FUTURE_WATCH"),
    futureWatchClass: countWhere(corpus, (r) => r.classLabel === "FUTURE_WATCH"),
    disqualified: countWhere(corpus, (r) => r.priority === "DISQUALIFIED"),
  },
  opportunityIds: corpus.map((r) => r.opportunityId).sort(),
  rows: corpus.map((r) => ({
    opportunityId: r.opportunityId,
    priority: r.priority || null,
    status: r.status || null,
    whoState: r.whoState || null,
    strictReady: !!r.strictReady,
    visible: !!r.visible,
    classLabel: r.classLabel || null,
    commercialStatus: r.commercialStatus || null,
    alreadyKnownState: "UNKNOWN",
    hotelFeedbackState: "UNKNOWN",
    discoveryDate: r.dates || null,
  })),
  alreadyKnownPolicy:
    "Until hotel responds: UNKNOWN. Do not infer net-new from Dealality discovery time.",
  feedbackItemsPresent: Array.isArray(feedback.items) ? feedback.items.length : 0,
  note: "Measurement reference freeze only — opportunity records are not permanently frozen.",
  frozenAt: new Date().toISOString(),
};

const bethToken = (contract.tokens || []).find((t) => t.tokenId === TOKEN_ID);

const controlQuerySet = {
  controlQuerySetId: "BETHESDA_ADP_OCTOBER_CONTROL_QUERY_SET_V1",
  propertyId: ADP_PROPERTY,
  hotelId: HOTEL_ID,
  lockedDate: DAY1,
  locked: true,
  scenarioUniverseVersion:
    adpRuntime.scenarioUniverseVersion || "adp_scenario_universe_v1",
  measurementContractVersion:
    adpManifest.measurementContractVersion || "ADP_MEASUREMENT_CONTRACT_V1",
  measurementContractHash: adpRuntime.measurementContractHash || null,
  queryCount: adpRuntime.scenarioCount || 63,
  providerCount: adpManifest.providerCount || 4,
  modelsObserved: adpRuntime.modelsUsed || [
    "gpt-4o",
    "gemini-3.6-flash",
    "sonar",
    "claude-sonnet-4-6",
  ],
  note: "Control set = production scenario universe used by baseline period. Experimental expansions must be versioned separately.",
};

const adpBaseline = {
  baselineId: "BETHESDA_ADP_BASELINE_V1",
  baselineVersion: "V1",
  baselineDate: DAY1,
  assignmentMode:
    "DESIGNATE_EXISTING_CERTIFIED_PERIOD_AS_OCTOBER_CONTROL_BASELINE",
  hotelId: HOTEL_ID,
  propertyId: ADP_PROPERTY,
  periodId: PERIOD_ID,
  baselineMarker: "ADP_BETHESDA_MARRIOTT_BASELINE_PERIOD_001",
  runTimestamp: adpRuntime.startedAt || "2026-09-09T09:10:16.123Z",
  completedAt: adpRuntime.completedAt || "2026-09-09T09:45:24.154Z",
  publishedAt: adpManifest.latestPublishedAt,
  methodologyVersion: adpManifest.measurementContractVersion,
  measurementContractHash: adpRuntime.measurementContractHash || null,
  scenarioUniverseVersion: adpRuntime.scenarioUniverseVersion || null,
  entityResolutionVersion: adpRuntime.entityResolutionVersion || null,
  promptVersion: adpRuntime.promptVersion || adpRuntime.scenarioUniverseVersion || null,
  modelProviders: controlQuerySet.modelsObserved,
  queryCount: controlQuerySet.queryCount,
  demandCaptureRate: adpManifest.demandCaptureRate,
  certifiedPublished: adpManifest.certified === true,
  certificationStatus: adpManifest.certificationStatus,
  forensicClientReadyStatus: adpManifest.forensicClientReadyStatus || null,
  externalDistributionHold: adpManifest.externalDistributionHold || null,
  immutable: true,
  overwritePolicy: "NEVER_OVERWRITE — November remeasurement creates a new current period compared against this baseline",
  sourcePaths: {
    manifest: path.relative(ROOT, adpManifestPath).replace(/\\/g, "/"),
    runtime: path.relative(ROOT, adpRuntimePath).replace(/\\/g, "/"),
    report: `data/ai-demand-positioning/published/${ADP_PROPERTY}/${adpManifest.reportFile}`,
    evidence: `data/ai-demand-positioning/published/${ADP_PROPERTY}/${adpManifest.evidenceFile}`,
  },
  hashes: {
    manifest: sha256File(adpManifestPath),
    runtime: fs.existsSync(adpRuntimePath) ? sha256File(adpRuntimePath) : null,
  },
  controlQuerySetId: controlQuerySet.controlQuerySetId,
  designatedAt: new Date().toISOString(),
};

const kpiBaseline = {
  pilotId: "BETHESDA_MARRIOTT_001",
  baselineDate: DAY1,
  adp: {
    aiRecommendationRate: "BASELINE_FROM_PERIOD",
    demandCaptureRate: adpManifest.demandCaptureRate,
    top3AppearanceRate: "NOT_YET_MEASURED_AS_PILOT_KPI",
    competitorShareOfRecommendations: "NOT_YET_MEASURED_AS_PILOT_KPI",
    actionsCompleted: "NOT_YET_MEASURED",
    movementAfterIntervention: "NOT_YET_MEASURED",
  },
  gdi: {
    qualifiedOpportunitiesDiscovered: gdiSnapshot.counts.strictReady,
    genuinelyNetNewPercent: "NOT_YET_MEASURED — requires hotel Already Known responses",
    opportunitiesPursued: "NOT_YET_MEASURED",
    contactsInitiated: "NOT_YET_MEASURED",
    rfpsGenerated: "NOT_YET_MEASURED",
    tentatives: "NOT_YET_MEASURED",
    wins: "NOT_YET_MEASURED",
    losses: "NOT_YET_MEASURED",
    roomNights: "NOT_YET_MEASURED",
    revenue: "NOT_YET_MEASURED",
  },
  quality: {
    hotelAcceptanceRate: "NOT_YET_MEASURED",
    rejectionRate: "NOT_YET_MEASURED",
    alreadyKnownRate: "NOT_YET_MEASURED",
    evidenceConfidence: "BASELINE_CORPUS_PRESENT",
  },
  commercial: {
    incrementalRevenueInfluenced: "NOT_YET_MEASURED",
    dealalityCost: "NOT_YET_MEASURED",
    roi: "NOT_YET_MEASURED",
  },
};

const pilotRecord = {
  pilot_id: "BETHESDA_MARRIOTT_001",
  pilot_name: "Bethesda Marriott Founding Pilot",
  property: "Bethesda Marriott",
  hotelId: HOTEL_ID,
  products: ["ADP", "GDI"],
  start_date: DAY1,
  initial_term_months: 6,
  planned_end_date: "2027-03-31",
  pilot_type: "FOUNDING_PARTNER",
  founding_hotel_partner_number: 1,
  status: "PENDING_PRODUCTION_DEPLOY_AND_SMOKE",
  adp: {
    baseline_date: DAY1,
    baseline_version: "V1",
    baseline_id: adpBaseline.baselineId,
    period_id: PERIOD_ID,
    remeasurement: "monthly",
    first_remeasurement_target: "2026-11-01",
  },
  gdi: {
    active: true,
    delivery: "weekly",
    day1_snapshot_id: gdiSnapshot.snapshotId,
    share_token_id: TOKEN_ID,
    share_token_must_not_change: true,
  },
  cron_future_watch: "HELD",
  createdAt: new Date().toISOString(),
};

const shareTokenCheck = {
  expectedTokenId: TOKEN_ID,
  presentInContract: !!bethToken,
  tokenIdUnchanged: bethToken?.tokenId === TOKEN_ID,
  hotelId: bethToken?.hotelId || null,
};

fs.writeFileSync(
  path.join(OUT, "GDI_DAY1_SNAPSHOT.json"),
  JSON.stringify(gdiSnapshot, null, 2)
);
fs.writeFileSync(
  path.join(OUT, "ADP_BASELINE_MANIFEST.json"),
  JSON.stringify(adpBaseline, null, 2)
);
fs.writeFileSync(
  path.join(OUT, "ADP_CONTROL_QUERY_SET.json"),
  JSON.stringify(controlQuerySet, null, 2)
);
fs.writeFileSync(
  path.join(OUT, "PILOT_KPI_BASELINE.json"),
  JSON.stringify(kpiBaseline, null, 2)
);
fs.writeFileSync(
  path.join(OUT, "PILOT_MASTER_RECORD.json"),
  JSON.stringify(pilotRecord, null, 2)
);
fs.writeFileSync(
  path.join(OUT, "SHARE_TOKEN_CHECK.json"),
  JSON.stringify(shareTokenCheck, null, 2)
);

// also write durable pilot registry under data/
const pilotDir = path.join(ROOT, "data/pilots/bethesda-marriott-001");
fs.mkdirSync(pilotDir, { recursive: true });
fs.writeFileSync(
  path.join(pilotDir, "pilot-master.json"),
  JSON.stringify(pilotRecord, null, 2)
);
fs.writeFileSync(
  path.join(pilotDir, "adp-october-baseline-v1.json"),
  JSON.stringify(adpBaseline, null, 2)
);
fs.writeFileSync(
  path.join(pilotDir, "gdi-day1-snapshot-2026-10-01.json"),
  JSON.stringify(gdiSnapshot, null, 2)
);
fs.writeFileSync(
  path.join(pilotDir, "adp-october-control-query-set-v1.json"),
  JSON.stringify(controlQuerySet, null, 2)
);

console.log(
  JSON.stringify(
    {
      ok: true,
      gdiReady: gdiSnapshot.counts.strictReady,
      gdiVisible: gdiSnapshot.counts.visible,
      gdiHigh: gdiSnapshot.counts.high,
      gdiMedium: gdiSnapshot.counts.medium,
      gdiFutureWatch: gdiSnapshot.counts.futureWatchStatus,
      adpBaselineId: adpBaseline.baselineId,
      periodId: PERIOD_ID,
      tokenOk: shareTokenCheck.tokenIdUnchanged,
      out: OUT,
    },
    null,
    2
  )
);
