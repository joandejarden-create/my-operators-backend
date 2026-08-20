/**
 * ADP Existing Hotel production recovery audit (read-only).
 * Reconciles published ↔ runtime ↔ contract expectations.
 * No Airtable mutations. 0 LLM spend.
 */
import { readdirSync, readFileSync, existsSync, writeFileSync, mkdirSync } from "fs";
import { join } from "path";
import { loadAllPeriods, loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import { loadPublishedReport, loadPublishedManifest, loadPublishedEvidenceIndex } from "../lib/ai-demand-positioning/published-snapshot.js";
import { buildOwnerPayload } from "../lib/ai-demand-positioning/customer/owner-payload.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { buildGovernedIntentPresenceIndex } from "../lib/ai-demand-positioning/metrics/governed-customer-presence-index.js";
import {
  isOfficialProductionPeriod,
  isCustomerTrendEligible,
  isCustomerCurrentEligible,
  classifyPreBaselinePeriod,
} from "../lib/ai-demand-positioning/period-eligibility-v1.js";
import { OFFICIAL_BASELINE_PERIOD_MARKER as MARKER } from "../lib/ai-demand-positioning/contracts/adp-measurement-contract-v1.js";
import { queryEvidenceIndex } from "../lib/ai-demand-positioning/customer/evidence-index.js";

const OUT_DIR = join(process.cwd(), "reports/ai-demand-positioning");
const PUBLISHED = join(process.cwd(), "data/ai-demand-positioning/published");
const RUNTIME = join(process.cwd(), "data/ai-demand-positioning/runtime");

const JUNK_ENTITY_PATTERNS = [
  /^the\s+$/i,
  /\b(is|are|was|were|and|or|with|for|near|located)\b$/i,
  /^[^a-zA-Z0-9]*$/,
  /^.{1,2}$/,
  /\b(hotel|resort|inn)\s+(is|are|offers|features)\b/i,
];

function listPublishedProperties() {
  if (!existsSync(PUBLISHED)) return [];
  return readdirSync(PUBLISHED, { withFileTypes: true })
    .filter((d) => d.isDirectory())
    .map((d) => d.name);
}

function providerCompleteness(period) {
  const obs = period?.observations || [];
  const byProvider = {};
  for (const o of obs) {
    const p = o.provider || o.modelProvider || "unknown";
    if (!byProvider[p]) byProvider[p] = { total: 0, ok: 0, fail: 0, missing: 0, dryRun: 0 };
    byProvider[p].total += 1;
    if (o.dryRun) byProvider[p].dryRun += 1;
    else if (o.error || o.providerError || o.status === "FAILED" || (o.httpStatus && o.httpStatus >= 400)) {
      byProvider[p].fail += 1;
    } else if (o.parsed || o.rawResponse) {
      byProvider[p].ok += 1;
    } else {
      byProvider[p].missing += 1;
    }
  }
  const totals = Object.values(byProvider).reduce(
    (a, b) => ({
      total: a.total + b.total,
      ok: a.ok + b.ok,
      fail: a.fail + b.fail,
      missing: a.missing + b.missing,
      dryRun: a.dryRun + b.dryRun,
    }),
    { total: 0, ok: 0, fail: 0, missing: 0, dryRun: 0 }
  );
  return { byProvider, totals };
}

function extractEntitiesFromPayload(payload) {
  const names = new Set();
  const add = (n) => {
    const s = String(n || "").trim();
    if (s) names.add(s);
  };
  const comp = payload?.competitiveSet || payload?.competitiveRanking || [];
  const list = Array.isArray(comp) ? comp : comp?.competitors || comp?.rows || [];
  for (const c of list) {
    add(c?.name || c?.hotelName || c?.displayName || c?.canonicalName);
  }
  const territories = payload?.intentPresence || payload?.demandTerritories || payload?.aiPresenceByDemandTerritory || [];
  const tList = Array.isArray(territories) ? territories : territories?.rows || [];
  for (const t of tList) {
    add(t?.topAlternative || t?.topObservedAlternative?.name);
    for (const a of t?.alternatives || t?.competitors || []) add(a?.name || a);
  }
  const exec = payload?.executiveRead || payload?.executiveMetrics || {};
  add(exec?.topObservedAlternative || exec?.topAlternative);
  const actions = payload?.priorityActions || payload?.reviewOpportunities || [];
  for (const a of actions || []) {
    const m = String(a?.expectedImpact || a?.title || a?.body || "");
    // not entities
  }
  return [...names];
}

function classifyEntity(name) {
  for (const re of JUNK_ENTITY_PATTERNS) {
    if (re.test(name)) return "JUNK_FRAGMENT";
  }
  if (/eau\s+palm/i.test(name) && !/eau palm beach/i.test(name)) return "ALIAS_CANDIDATE";
  if (/^boca raton$/i.test(name) || /boca raton resort/i.test(name)) return "ALIAS_CANDIDATE";
  return "OK_OR_UNKNOWN";
}

function reconcileProperty(propertyId) {
  const manifest = loadPublishedManifest(propertyId);
  const published = loadPublishedReport(propertyId);
  const evidence = loadPublishedEvidenceIndex(propertyId);
  const profile = loadPropertyProfile(propertyId);
  const periods = loadAllPeriods(propertyId) || [];
  const latestId = manifest?.latestPeriodId;
  const runtimePeriod = periods.find((p) => p.periodId === latestId) || null;

  const periodClasses = periods.map((p) => {
    const cls = classifyPreBaselinePeriod(p);
    return {
      periodId: p.periodId,
      executionDate: p.executionDate || p.createdAt || null,
      status: p.status,
      certified: p.certified === true,
      officialPeriod: p.officialPeriod === true,
      baselineMarker: p.baselineMarker || null,
      baselinePeriod: p.baselinePeriod === true,
      customerTrendEligible: isCustomerTrendEligible(p),
      customerCurrentEligible: isCustomerCurrentEligible(p),
      archiveClass: cls?.archiveClass || null,
      measurementPhase: cls?.measurementPhase || p.measurementPhase || null,
      observationCount: (p.observations || []).length,
      providerCompleteness: providerCompleteness(p).totals,
    };
  });

  const officialBaselines = periodClasses.filter(
    (p) => p.baselineMarker === MARKER || p.baselineMarker === "ADP_OFFICIAL_BASELINE_PERIOD_001" || p.baselinePeriod
  );
  const phillipsBaselines = periodClasses.filter((p) =>
    String(p.baselineMarker || "").includes("PHILLIPS")
  );

  let recomputed = null;
  let metricReconcile = [];
  let indexSample = [];
  let entityIssues = [];
  let evidenceAudit = [];
  let crossMetric = [];

  if (runtimePeriod && profile && published?.payload) {
    const scenarios = buildScenarioUniverse(profile);
    recomputed = buildOwnerPayload(runtimePeriod, scenarios, profile);
    const pub = published.payload;

    const pairs = [
      ["demandCapture.overallRate", recomputed?.demandCapture?.overallRate, pub?.demandCapture?.overallRate],
      ["consideration.rate", recomputed?.consideration?.rate, pub?.consideration?.rate],
      ["executiveMetrics.considerationRate", recomputed?.executiveMetrics?.considerationRate ?? recomputed?.consideration?.rate, pub?.executiveMetrics?.considerationRate],
      ["executiveMetrics.scenarioPresence", recomputed?.executiveMetrics?.scenarioPresence ?? recomputed?.demandCapture?.overallRate, pub?.executiveMetrics?.scenarioPresence],
    ];

    for (const [key, exp, got] of pairs) {
      metricReconcile.push(statusRow(propertyId, key, exp, got, pub?.[key.split(".")[0]]));
    }

    // Presence index sample
    try {
      const obs = (runtimePeriod.observations || []).filter((o) => o.parsed);
      const idx = buildGovernedIntentPresenceIndex(obs, scenarios, profile);
      const pubIdx = pub.intentPresenceIndex || {};
      const keys = new Set([...Object.keys(idx || {}), ...Object.keys(pubIdx || {})]);
      for (const k of [...keys].slice(0, 12)) {
        const e = idx?.[k];
        const g = pubIdx?.[k];
        const expVal = e?.index ?? e?.presenceIndex ?? null;
        const gotVal = g?.index ?? g?.presenceIndex ?? null;
        const thin = (e?.corePeerCount != null && e.corePeerCount < 3) || (e?.coreRate != null && e.coreRate < 0.05);
        let status = approxMatch(expVal, gotVal);
        if (status === "MATCH" && thin && expVal != null && Math.abs(expVal) > 250) {
          status = "CORRECT_BUT_PRESENTATION_REVIEW_PENDING";
        }
        indexSample.push({
          propertyId,
          territory: k,
          subjectRate: e?.subjectRate ?? e?.yourRate ?? null,
          coreRate: e?.coreRate ?? null,
          index: expVal,
          publishedIndex: gotVal,
          status,
        });
      }
    } catch (err) {
      indexSample.push({ propertyId, error: err.message });
    }

    // Cross-metric
    const cons = recomputed?.consideration;
    const dc = recomputed?.demandCapture;
    if (cons?.rate != null && dc?.overallRate != null) {
      crossMetric.push({
        check: "scenarioPresence_vs_consideration_same_scale_not_required",
        note: "Different grains — no equality required",
        status: "NOT_APPLICABLE",
      });
    }
    const top1 = recomputed?.executiveMetrics?.appearanceRate1 ?? recomputed?.rankMetrics?.rate1;
    const top3 = recomputed?.executiveMetrics?.appearanceRateTop3 ?? recomputed?.rankMetrics?.rateTop3;
    if (top1 != null && top3 != null) {
      crossMetric.push({
        check: "top1_lte_top3",
        expected: "top1 <= top3",
        top1,
        top3,
        status: top1 <= top3 + 1e-9 ? "MATCH" : "MISMATCH",
      });
    }

    // Entities
    const names = extractEntitiesFromPayload(recomputed.ok ? recomputed : pub);
    for (const n of names) {
      const c = classifyEntity(n);
      if (c !== "OK_OR_UNKNOWN") entityIssues.push({ propertyId, name: n, classification: c });
    }
    // Duplicate case-insensitive
    const lower = new Map();
    for (const n of names) {
      const k = n.toLowerCase().replace(/\s+/g, " ").trim();
      if (!lower.has(k)) lower.set(k, []);
      lower.get(k).push(n);
    }
    for (const [, arr] of lower) {
      if (new Set(arr).size > 1) {
        entityIssues.push({ propertyId, name: arr.join(" | "), classification: "DUPLICATE_VARIANT" });
      }
    }

    // Evidence by intent
    const intents = Object.keys(pub.intentPresenceIndex || recomputed.intentPresenceIndex || {});
    const evIndex = evidence || null;
    for (const intent of intents.slice(0, 20)) {
      try {
        const q = queryEvidenceIndex
          ? queryEvidenceIndex(evIndex || { ok: true, byIntent: {} }, { intent, type: "missing" })
          : null;
        const count = Array.isArray(q?.evidence) ? q.evidence.length : q?.length || 0;
        let status = "PASS";
        if (!evIndex) status = "MISSING_EVIDENCE";
        else if (count === 0) status = "PARTIAL";
        evidenceAudit.push({
          propertyId,
          demandIntent: intent,
          evidenceRecords: count,
          status,
        });
      } catch (err) {
        evidenceAudit.push({ propertyId, demandIntent: intent, status: "BROKEN_LINK", error: err.message });
      }
    }
  }

  // Trends in published
  const trends = published?.payload?.trends || [];
  const trendEligiblePeriods = periodClasses.filter((p) => p.customerTrendEligible);

  // Impact claims
  const actions = published?.payload?.priorityActions || [];
  const impactClaims = (actions || [])
    .map((a) => a?.expectedImpact)
    .filter((x) => x && /scenario/i.test(String(x)));

  return {
    propertyId,
    propertyName: manifest?.propertyName || profile?.propertyName || propertyId,
    classification: classifyPropertyRole(manifest, periodClasses),
    manifest: manifest
      ? {
          latestPeriodId: manifest.latestPeriodId,
          publishStatus: manifest.publishStatus,
          certified: manifest.certified,
          baselineMarker: manifest.baselineMarker,
          officialPeriod: manifest.officialPeriod,
          measurementContractHash: manifest.measurementContractHash,
          demandCaptureRate: manifest.demandCaptureRate,
        }
      : null,
    publishedPresent: !!published,
    evidencePresent: !!evidence,
    runtimePeriodPresent: !!runtimePeriod,
    periodClasses,
    officialBaselines,
    phillipsBaselines,
    trendEligibleCount: trendEligiblePeriods.length,
    publishedTrendPoints: trends.length,
    trendDefect:
      trendEligiblePeriods.length === 1 && trends.length === 0
        ? "SINGLE_BASELINE_BUT_TRENDS_EMPTY"
        : trendEligiblePeriods.length === 1 && trends.length === 1
          ? "SINGLE_BASELINE_TREND_POINT_PRESENT"
          : trendEligiblePeriods.length === 0
            ? "NO_TREND_ELIGIBLE_PERIOD"
            : "MULTI_OR_OTHER",
    providerCompleteness: runtimePeriod ? providerCompleteness(runtimePeriod) : null,
    metricReconcile,
    indexSample,
    crossMetric,
    entityIssues,
    evidenceAudit,
    impactClaims,
    recomputedOk: recomputed?.ok === true,
  };
}

function classifyPropertyRole(manifest, periodClasses) {
  if (!manifest) return "UNKNOWN";
  if (String(manifest.baselineMarker || "").includes("PHILLIPS")) return "CERTIFIED_STANDALONE";
  if (manifest.baselineMarker === MARKER || manifest.baselineMarker === "ADP_OFFICIAL_BASELINE_PERIOD_001") {
    return "ACTIVE_OFFICIAL_BASELINE_PROPERTY";
  }
  if (manifest.certified && manifest.officialPeriod) return "CERTIFIED_OFFICIAL";
  if (manifest.publishStatus === "Live") return "LIVE_PUBLISHED";
  return "OTHER";
}

function approxMatch(exp, got) {
  if (exp == null && got == null) return "MATCH";
  if (exp == null || got == null) return "MISSING";
  if (typeof exp === "number" && typeof got === "number") {
    if (Math.abs(exp - got) < 0.05) return "MATCH";
    if (Math.abs(exp - got) < 0.6) return "ROUNDING_ONLY";
    return "MISMATCH";
  }
  if (String(exp) === String(got)) return "MATCH";
  return "MISMATCH";
}

function statusRow(propertyId, metric, expected, published) {
  return {
    propertyId,
    metric,
    expected: expected ?? null,
    published: published ?? null,
    ui: "NOT_CHECKED_THIS_PASS",
    status: approxMatch(expected, published),
  };
}

async function tryAirtableInventory() {
  const enabled =
    process.env.ADP_AIRTABLE_READ_LIVE === "1" || process.env.ADP_PUBLISHED_READ_SOURCE === "airtable";
  const apiKey = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  const baseId = process.env.ADP_AIRTABLE_BASE_ID || process.env.AIRTABLE_BASE_ID;
  const result = {
    airtableReadLiveEnv: enabled,
    credentialsPresent: !!(apiKey && baseId),
    roleFromCode:
      "Optional mirror/read overlay. Production default is local published snapshots unless ADP_AIRTABLE_READ_LIVE=1 or ADP_PUBLISHED_READ_SOURCE=airtable.",
    records: null,
    error: null,
  };
  if (!apiKey || !baseId) {
    result.error = "NO_CREDENTIALS_OR_BASE";
    return result;
  }
  try {
    const { listPublishedReportsFromAirtable } = await import(
      "../lib/ai-demand-positioning/airtable-published-report.js"
    ).catch(() => ({}));
    // Fallback: direct Airtable select via existing get helpers if list not exported
    const Airtable = (await import("airtable")).default;
    const base = new Airtable({ apiKey }).base(baseId);
    const table = "AI Demand Positioning - Published Reports";
    const rows = [];
    await base(table)
      .select({ pageSize: 100 })
      .eachPage((records, next) => {
        for (const r of records) {
          rows.push({
            id: r.id,
            propertyId: r.get("ADP Property ID"),
            periodId: r.get("Period ID"),
            publishStatus: r.get("Publish Status"),
            propertyName: r.get("Property Name"),
            publishedAt: r.get("Published At"),
            demandCaptureRate: r.get("Demand Capture Rate"),
            hasPayload: !!(r.get("Payload JSON") || r.get("Payload Store Ref")),
            hasEvidence: !!(r.get("Evidence Index JSON") || r.get("Payload Store Ref")),
          });
        }
        next();
      });
    result.records = rows;
    result.table = table;
  } catch (err) {
    result.error = String(err.message || err);
  }
  return result;
}

async function main() {
  const properties = listPublishedProperties();
  const propertyReports = properties.map(reconcileProperty);

  // Baseline inventory across all
  const baselineInventory = [];
  for (const pr of propertyReports) {
    for (const p of pr.periodClasses) {
      let classification = "OTHER";
      if (p.baselineMarker === MARKER || p.baselineMarker === "ADP_OFFICIAL_BASELINE_PERIOD_001") {
        classification = p.customerTrendEligible ? "ACTIVE_OFFICIAL_BASELINE" : "ARCHIVED_OR_INACTIVE_OFFICIAL_MARKER";
      } else if (String(p.baselineMarker || "").includes("PHILLIPS")) {
        classification = "CERTIFIED_STANDALONE";
      } else if (p.archiveClass === "FULL_PROPERTY_PRE_BASELINE") {
        classification = "ARCHIVED_BASELINE";
      } else if (p.archiveClass === "DEVELOPMENT_ONLY") {
        classification = "TEST";
      } else if (p.archiveClass === "TARGETED_RESEARCH") {
        classification = "TEST";
      } else if (p.certified && p.officialPeriod) {
        classification = "ACTIVE_OFFICIAL";
      }
      baselineInventory.push({
        propertyId: pr.propertyId,
        periodId: p.periodId,
        executionDate: p.executionDate,
        baselineMarker: p.baselineMarker,
        classification,
        customerTrendEligible: p.customerTrendEligible,
        certified: p.certified,
      });
    }
  }

  const airtable = await tryAirtableInventory();

  const summary = {
    title: "ADP_EXISTING_HOTEL_PRODUCTION_RECOVERY_AUDIT_V1",
    generatedAt: new Date().toISOString(),
    mode: "FINISH_RECOVER_READ_ONLY",
    visualStructure: "FROZEN",
    formulaChanges: "NONE",
    propertiesAudited: properties,
    officialBaselineMarker: MARKER,
    activeOfficialBaselinePeriods: baselineInventory.filter((b) => b.classification === "ACTIVE_OFFICIAL_BASELINE"),
    airtable,
    propertyReports,
    baselineInventory,
    globalFindings: {
      unsupportedImpactClaims: propertyReports.flatMap((p) =>
        (p.impactClaims || []).map((c) => ({ propertyId: p.propertyId, claim: c }))
      ),
      entityIssues: propertyReports.flatMap((p) => p.entityIssues || []),
      metricMismatches: propertyReports.flatMap((p) =>
        (p.metricReconcile || []).filter((m) => m.status === "MISMATCH" || m.status === "MISSING")
      ),
      extremeIndexes: propertyReports.flatMap((p) =>
        (p.indexSample || []).filter((i) => i.status === "CORRECT_BUT_PRESENTATION_REVIEW_PENDING")
      ),
      trendDefects: propertyReports.map((p) => ({
        propertyId: p.propertyId,
        trendDefect: p.trendDefect,
        trendEligibleCount: p.trendEligibleCount,
        publishedTrendPoints: p.publishedTrendPoints,
      })),
    },
  };

  mkdirSync(OUT_DIR, { recursive: true });
  const outPath = join(OUT_DIR, "adp-existing-hotel-production-recovery-audit-v1.json");
  writeFileSync(outPath, JSON.stringify(summary, null, 2));
  console.log(JSON.stringify({
    outPath,
    properties: properties.length,
    activeOfficialBaselines: summary.activeOfficialBaselinePeriods.length,
    metricMismatches: summary.globalFindings.metricMismatches.length,
    entityIssues: summary.globalFindings.entityIssues.length,
    impactClaims: summary.globalFindings.unsupportedImpactClaims.length,
    trendDefects: summary.globalFindings.trendDefects,
    airtable: {
      readLiveEnv: airtable.airtableReadLiveEnv,
      credentialsPresent: airtable.credentialsPresent,
      recordCount: airtable.records?.length ?? null,
      error: airtable.error,
    },
  }, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
