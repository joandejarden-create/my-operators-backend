#!/usr/bin/env node
/**
 * Existing Hotel ADP recovery republish (audited periods only).
 *
 *   node scripts/run-adp-existing-hotel-recovery-republish-v1.mjs --dry-run
 *   node scripts/run-adp-existing-hotel-recovery-republish-v1.mjs --apply
 *
 * - No new LLM calls / periods
 * - No Airtable writes
 * - Preserves OLD snapshots under reports/.../backup/
 * - Stops a property if core KPIs change materially
 */
import "../load-env.js";
import {
  copyFileSync,
  cpSync,
  existsSync,
  mkdirSync,
  readFileSync,
  writeFileSync,
  readdirSync,
} from "fs";
import { join } from "path";
import { loadAllPeriods, loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import {
  selectLatestCertifiedOfficialPeriod,
  isOfficialProductionPeriod,
} from "../lib/ai-demand-positioning/period-eligibility-v1.js";
import { OFFICIAL_BASELINE_PERIOD_MARKER } from "../lib/ai-demand-positioning/contracts/adp-measurement-contract-v1.js";
import {
  buildPublishedSnapshotBundle,
  savePublishedSnapshotBundle,
  loadPublishedReport,
  loadPublishedManifest,
} from "../lib/ai-demand-positioning/published-snapshot.js";
import { resolveCensusRecordIdForPublish } from "../lib/ai-demand-positioning/census-link-registry.js";

const APPLY = process.argv.includes("--apply");
const DRY = !APPLY;

const PORTFOLIO_FOUR = [
  "adp_waterstone_boca_raton",
  "adp_renaissance_times_square",
  "adp_cambridge_beaches_bermuda",
  "adp_now_now_noho",
];
const STANDALONE = ["adp_hotel_phillips_kansas_city"];
const ALL = [...PORTFOLIO_FOUR, ...STANDALONE];

const KPI_MATERIAL_PP = 0.6; // stop if |Δ| >= this for core rates

const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
const BACKUP_ROOT = join(
  process.cwd(),
  "reports/ai-demand-positioning/recovery-republish-backup",
  stamp
);
const DIFF_OUT = join(
  process.cwd(),
  "reports/ai-demand-positioning",
  `adp-existing-hotel-recovery-republish-diff-${stamp}.json`
);

function round1(n) {
  return n == null || Number.isNaN(Number(n)) ? null : Math.round(Number(n) * 10) / 10;
}

function pickPeriod(propertyId, manifest) {
  const periods = loadAllPeriods(propertyId);
  if (manifest?.latestPeriodId) {
    const fromManifest = periods.find((p) => p.periodId === manifest.latestPeriodId);
    if (fromManifest) return fromManifest;
  }
  return selectLatestCertifiedOfficialPeriod(periods) || null;
}

function extractKpis(payload) {
  if (!payload) return {};
  const indexByIntent = {};
  for (const [intent, row] of Object.entries(payload.intentPresenceIndex || {})) {
    indexByIntent[intent] = {
      index: row?.index ?? null,
      subject: row?.myRate ?? row?.subjectRatePct ?? null,
      core: row?.coreBenchmarkRatePct ?? row?.avgCompRate ?? null,
      status: row?.status ?? null,
    };
  }
  return {
    demandCapture: payload.demandCapture?.overallRate ?? null,
    consideration: payload.executiveMetrics?.considerationRate?.rate ?? null,
    scenarioPresence: payload.executiveMetrics?.scenarioPresence?.rate ?? null,
    top1: payload.executiveMetrics?.rankMetrics?.numberOneAppearanceRate ?? null,
    top3: payload.executiveMetrics?.rankMetrics?.topThreeAppearanceRate ?? null,
    indexByIntent,
    trendsLen: Array.isArray(payload.trends) ? payload.trends.length : 0,
    trend0: payload.trends?.[0]
      ? {
          date: payload.trends[0].date,
          considerationRate: payload.trends[0].considerationRate ?? null,
          scenarioPresenceRate: payload.trends[0].scenarioPresenceRate ?? null,
          demandCaptureRate: payload.trends[0].demandCaptureRate ?? null,
        }
      : null,
    competitiveObserved: (payload.competitiveSet?.observed || []).map((x) => x.name || x),
    displacementTop: (payload.lostDemand?.displacement || []).slice(0, 5).map((x) => ({
      name: x.name,
      count: x.displacementCount,
    })),
    topAlternative: payload.lostDemand?.displacement?.[0]?.name || null,
    actionImpacts: (payload.actions || []).map((a) => a.expectedImpact),
    actionDescriptions: (payload.actions || []).map((a) => a.description || ""),
    openaiPresence: (payload.evidence?.providers || []).find((p) => p.provider === "openai")?.presence ?? null,
    providerPresence: Object.fromEntries(
      (payload.evidence?.providers || []).map((p) => [p.provider, p.presence])
    ),
    baselineMarker: payload.period?.baselineMarker || payload.period?.officialBaselineMarker || null,
    officialPeriod: payload.period?.officialPeriod ?? null,
    providerCount: payload.period?.providerCount ?? null,
  };
}

function kpiMaterialDelta(oldK, newK) {
  const keys = ["demandCapture", "consideration", "scenarioPresence", "top1", "top3"];
  const hits = [];
  for (const k of keys) {
    const a = round1(oldK[k]);
    const b = round1(newK[k]);
    if (a == null && b == null) continue;
    if (a == null || b == null) {
      hits.push({ metric: k, old: a, new: b, delta: null, reason: "null_mismatch" });
      continue;
    }
    const d = Math.abs(a - b);
    if (d >= KPI_MATERIAL_PP) hits.push({ metric: k, old: a, new: b, delta: round1(b - a) });
  }
  // Presence Index per intent
  const intents = new Set([
    ...Object.keys(oldK.indexByIntent || {}),
    ...Object.keys(newK.indexByIntent || {}),
  ]);
  for (const intent of intents) {
    const a = round1(oldK.indexByIntent?.[intent]?.index);
    const b = round1(newK.indexByIntent?.[intent]?.index);
    if (a == null && b == null) continue;
    if (a == null || b == null) {
      hits.push({ metric: `index.${intent}`, old: a, new: b, delta: null, reason: "null_mismatch" });
      continue;
    }
    // Index can shift slightly from entity cleanup in ranking overlays; gate on >= 2 points
    if (Math.abs(a - b) >= 2) {
      hits.push({ metric: `index.${intent}`, old: a, new: b, delta: round1(b - a) });
    }
  }
  return hits;
}

function backupProperty(propertyId) {
  const src = join(process.cwd(), "data/ai-demand-positioning/published", propertyId);
  if (!existsSync(src)) return null;
  const dest = join(BACKUP_ROOT, "data-published", propertyId);
  mkdirSync(dest, { recursive: true });
  cpSync(src, dest, { recursive: true });
  const seedSrc = join(process.cwd(), "fixtures/ai-demand-positioning/published", propertyId);
  if (existsSync(seedSrc)) {
    const seedDest = join(BACKUP_ROOT, "fixtures-published", propertyId);
    mkdirSync(seedDest, { recursive: true });
    cpSync(seedSrc, seedDest, { recursive: true });
  }
  return dest;
}

function classifyProperty(propertyId, period) {
  if (STANDALONE.includes(propertyId)) {
    const marker = period?.baselineMarker || "";
    if (marker === OFFICIAL_BASELINE_PERIOD_MARKER) {
      return { ok: false, classification: "INVALID", error: "Phillips must not use OFFICIAL_BASELINE_PERIOD_001" };
    }
    return { ok: true, classification: "CERTIFIED_STANDALONE" };
  }
  if (period?.baselineMarker !== OFFICIAL_BASELINE_PERIOD_MARKER && period?.officialPeriod !== true) {
    return {
      ok: false,
      classification: "INVALID",
      error: `Expected official baseline marker ${OFFICIAL_BASELINE_PERIOD_MARKER}`,
    };
  }
  return { ok: true, classification: "ACTIVE_OFFICIAL_BASELINE" };
}

const results = [];

mkdirSync(BACKUP_ROOT, { recursive: true });

for (const propertyId of ALL) {
  const profile = loadPropertyProfile(propertyId);
  const manifest = loadPublishedManifest(propertyId);
  const oldPayload = loadPublishedReport(propertyId);
  const period = pickPeriod(propertyId, manifest);
  const row = {
    propertyId,
    status: "PENDING",
    periodId: period?.periodId || null,
    backup: null,
    classification: null,
    materialKpiChanges: [],
    areas: [],
  };

  if (!profile || !period) {
    row.status = "FAIL";
    row.error = !profile ? "NO_PROFILE" : "NO_PERIOD";
    results.push(row);
    continue;
  }

  const cls = classifyProperty(propertyId, period);
  row.classification = cls.classification;
  if (!cls.ok) {
    row.status = "FAIL";
    row.error = cls.error;
    results.push(row);
    continue;
  }

  row.backup = backupProperty(propertyId);
  const censusRecordId = resolveCensusRecordIdForPublish(propertyId, null);
  const bundle = buildPublishedSnapshotBundle({ period, profile, censusRecordId });
  if (!bundle?.ok) {
    row.status = "FAIL";
    row.error = bundle?.message || bundle?.error || "BUILD_FAILED";
    results.push(row);
    continue;
  }

  const newPayload = bundle.report.payload;
  const oldK = extractKpis(oldPayload);
  const newK = extractKpis(newPayload);
  row.materialKpiChanges = kpiMaterialDelta(oldK, newK);

  row.areas = [
    {
      area: "Consideration",
      old: oldK.consideration,
      new: newK.consideration,
      reason: "rebuild from audited period",
    },
    {
      area: "Scenario Presence",
      old: oldK.scenarioPresence,
      new: newK.scenarioPresence,
      reason: "rebuild from audited period",
    },
    {
      area: "Demand Capture",
      old: oldK.demandCapture,
      new: newK.demandCapture,
      reason: "rebuild from audited period",
    },
    {
      area: "Presence Index (sample)",
      old: oldK.indexByIntent,
      new: newK.indexByIntent,
      reason: "governed index rebuild; formula unchanged",
    },
    {
      area: "Top Alternative",
      old: oldK.topAlternative,
      new: newK.topAlternative,
      reason: "customer entity resolution",
    },
    {
      area: "Competitor entities",
      old: oldK.competitiveObserved,
      new: newK.competitiveObserved,
      reason: "customer entity resolution / junk suppression",
    },
    {
      area: "Displacement entities",
      old: oldK.displacementTop,
      new: newK.displacementTop,
      reason: "customer entity resolution",
    },
    {
      area: "Action expectedImpact",
      old: oldK.actionImpacts,
      new: newK.actionImpacts,
      reason: "unsupported numeric impact neutralized",
    },
    {
      area: "Action descriptions",
      old: oldK.actionDescriptions,
      new: newK.actionDescriptions,
      reason: "remove unsupported causal uplift language",
    },
    {
      area: "Provider presence (OpenAI)",
      old: oldK.openaiPresence,
      new: newK.openaiPresence,
      reason: "subject matching on existing raw observations (no new LLM)",
    },
    {
      area: "Provider presence (all)",
      old: oldK.providerPresence,
      new: newK.providerPresence,
      reason: "provider differences preserved; not equalized",
    },
    {
      area: "Trend payload",
      old: { len: oldK.trendsLen, sample: oldK.trend0 },
      new: { len: newK.trendsLen, sample: newK.trend0 },
      reason: "single baseline point with consideration/scenario rates",
    },
    {
      area: "Baseline classification",
      old: {
        marker: oldPayload?.period?.baselineMarker || null,
        official: oldPayload?.period?.officialPeriod ?? null,
      },
      new: {
        marker: newPayload?.period?.baselineMarker || period.baselineMarker || null,
        official: newPayload?.period?.officialPeriod ?? period.officialPeriod ?? null,
      },
      reason: STANDALONE.includes(propertyId)
        ? "CERTIFIED_STANDALONE — not portfolio official baseline"
        : "ADP_OFFICIAL_BASELINE_PERIOD_001",
    },
  ];

  if (row.materialKpiChanges.length) {
    row.status = "STOP_KPI_MATERIAL_CHANGE";
    results.push(row);
    console.error(`STOP ${propertyId}: material KPI change`, row.materialKpiChanges);
    continue;
  }

  if (DRY) {
    row.status = "DRY_RUN_PASS";
    results.push(row);
    continue;
  }

  // Apply to runtime published + seed fixtures (Railway lean deploy fallback)
  savePublishedSnapshotBundle(bundle, { seed: false });
  savePublishedSnapshotBundle(bundle, { seed: true });
  row.status = "PASS";
  row.applied = true;
  results.push(row);
}

const report = {
  generatedAt: new Date().toISOString(),
  mode: DRY ? "DRY_RUN" : "APPLY",
  backupRoot: BACKUP_ROOT,
  kpiMaterialThresholdPp: KPI_MATERIAL_PP,
  portfolioOfficialBaseline: PORTFOLIO_FOUR,
  certifiedStandalone: STANDALONE,
  officialBaselineMarker: OFFICIAL_BASELINE_PERIOD_MARKER,
  airtable: "SKIPPED_BY_FOUNDER — file SoT only this step",
  results,
  summary: {
    pass: results.filter((r) => r.status === "PASS" || r.status === "DRY_RUN_PASS").length,
    stop: results.filter((r) => r.status === "STOP_KPI_MATERIAL_CHANGE").length,
    fail: results.filter((r) => r.status === "FAIL").length,
  },
};

writeFileSync(DIFF_OUT, JSON.stringify(report, null, 2));
console.log(JSON.stringify({ out: DIFF_OUT, backupRoot: BACKUP_ROOT, summary: report.summary }, null, 2));
for (const r of results) {
  console.log(
    r.propertyId,
    r.status,
    r.periodId?.slice(-20),
    r.classification,
    r.materialKpiChanges?.length ? `KPI_STOP ${JSON.stringify(r.materialKpiChanges)}` : "kpi_ok"
  );
}

if (results.some((r) => r.status === "STOP_KPI_MATERIAL_CHANGE" || r.status === "FAIL")) {
  process.exitCode = 1;
}
