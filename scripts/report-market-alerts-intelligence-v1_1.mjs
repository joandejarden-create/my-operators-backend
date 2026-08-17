#!/usr/bin/env node
/**
 * Before/after report for V1.1 on the same 200 enriched records.
 * Usage: node scripts/report-market-alerts-intelligence-v1_1.mjs [beforeSnapshot.json]
 */
import fs from "fs";
import path from "path";
import "../load-env.js";
import { computeMarketAlertIntelligence } from "../lib/market-alerts-intelligence.js";
import Airtable from "airtable";
import { MAP_ALERT } from "../api/lib/market-alerts-rss-airtable.js";
import { MAP_INTEL } from "../api/lib/market-alerts-intelligence-map.js";

const BEFORE_BASELINE = {
  review: 36,
  standard: 164,
  ignore: 0,
  worthOwner: 26,
  worthBrand: 16,
  worthOperator: 23,
};

function rowSnapshot(fields) {
  return {
    treatment: fields[MAP_INTEL.intelligenceTreatment] || null,
    eventType: fields[MAP_INTEL.eventType] || null,
    worthOwner: !!fields[MAP_INTEL.worthReviewingOwner],
    worthBrand: !!fields[MAP_INTEL.worthReviewingBrand],
    worthOperator: !!fields[MAP_INTEL.worthReviewingOperator],
    signalOwner: fields[MAP_INTEL.signalTypeOwner] || null,
    signalBrand: fields[MAP_INTEL.signalTypeBrand] || null,
    signalOperator: fields[MAP_INTEL.signalTypeOperator] || null,
    stageOwner: fields[MAP_INTEL.decisionStageOwner] || null,
    stageBrand: fields[MAP_INTEL.decisionStageBrand] || null,
    stageOperator: fields[MAP_INTEL.decisionStageOperator] || null,
    hotelProject: fields[MAP_INTEL.hotelProject] || null,
    whyOwner: fields[MAP_INTEL.whyItMattersOwner] || null,
    whyBrand: fields[MAP_INTEL.whyItMattersBrand] || null,
    whyOperator: fields[MAP_INTEL.whyItMattersOperator] || null,
    actionOwner: fields[MAP_INTEL.recommendedActionOwner] || null,
    actionBrand: fields[MAP_INTEL.recommendedActionBrand] || null,
    actionOperator: fields[MAP_INTEL.recommendedActionOperator] || null,
  };
}

async function loadTop200(base, tableName) {
  const intelFields = Object.values(MAP_INTEL);
  const records = await base(tableName)
    .select({
      fields: [MAP_ALERT.title, MAP_ALERT.summary, MAP_ALERT.publishedAt, ...intelFields],
      sort: [{ field: MAP_ALERT.publishedAt, direction: "desc" }],
      maxRecords: 200,
    })
    .all();
  return records.map((r) => ({
    id: r.id,
    title: r.fields[MAP_ALERT.title] || "",
    summary: r.fields[MAP_ALERT.summary] || "",
    ...rowSnapshot(r.fields),
  }));
}

function countAfter(rows) {
  const c = {
    review: 0,
    standard: 0,
    ignore: 0,
    worthOwner: 0,
    worthBrand: 0,
    worthOperator: 0,
    eventTypes: {},
  };
  for (const r of rows) {
    if (r.treatment === "REVIEW") c.review += 1;
    else if (r.treatment === "STANDARD") c.standard += 1;
    else if (r.treatment === "IGNORE") c.ignore += 1;
    if (r.worthOwner) c.worthOwner += 1;
    if (r.worthBrand) c.worthBrand += 1;
    if (r.worthOperator) c.worthOperator += 1;
    if (r.eventType) c.eventTypes[r.eventType] = (c.eventTypes[r.eventType] || 0) + 1;
  }
  return c;
}

function diffRows(beforeRows, afterRows) {
  const beforeById = new Map(beforeRows.map((r) => [r.id, r]));
  let reviewToStandard = 0;
  let standardToReview = 0;
  let eventTypeChanged = 0;
  let signalChanged = 0;
  let stageChanged = 0;
  const downgraded = [];

  for (const after of afterRows) {
    const before = beforeById.get(after.id);
    if (!before) continue;
    if (before.treatment === "REVIEW" && after.treatment === "STANDARD") {
      reviewToStandard += 1;
      downgraded.push({ before, after });
    }
    if (before.treatment === "STANDARD" && after.treatment === "REVIEW") standardToReview += 1;
    if (before.eventType !== after.eventType) eventTypeChanged += 1;
    if (
      before.signalOwner !== after.signalOwner ||
      before.signalBrand !== after.signalBrand ||
      before.signalOperator !== after.signalOperator
    ) {
      signalChanged += 1;
    }
    if (
      before.stageOwner !== after.stageOwner ||
      before.stageBrand !== after.stageBrand ||
      before.stageOperator !== after.stageOperator
    ) {
      stageChanged += 1;
    }
  }

  return {
    reviewToStandard,
    standardToReview,
    eventTypeChanged,
    signalChanged,
    stageChanged,
    downgraded,
  };
}

function aggregateGateStats(rows) {
  const stats = {
    negationRejections: 0,
    assetTypeRejections: 0,
    partialAssetRejections: 0,
    entityQualityRejections: 0,
    closedDecisionDowngrades: 0,
    audienceQualificationDowngrades: 0,
  };
  for (const r of rows) {
    const m = computeMarketAlertIntelligence({
      title: r.title,
      summary: r.summary || "",
      alertId: r.id,
    });
    const g = m.meta.gate || {};
    stats.negationRejections += g.negationRejections || 0;
    stats.assetTypeRejections += g.assetTypeRejections || 0;
    stats.partialAssetRejections += g.partialAssetRejections || 0;
    stats.entityQualityRejections += g.entityQualityRejections || 0;
    stats.closedDecisionDowngrades += g.closedDecisionDowngrades || 0;
    stats.audienceQualificationDowngrades += g.audienceQualificationDowngrades || 0;
  }
  return stats;
}

function primaryAudience(row) {
  if (row.worthOwner) return "owner";
  if (row.worthBrand) return "brand";
  if (row.worthOperator) return "operator";
  return null;
}

function audienceDetail(row, aud) {
  if (aud === "owner") {
    return {
      signalType: row.signalOwner,
      stage: row.stageOwner,
      why: row.whyOwner,
      action: row.actionOwner,
    };
  }
  if (aud === "brand") {
    return {
      signalType: row.signalBrand,
      stage: row.stageBrand,
      why: row.whyBrand,
      action: row.actionBrand,
    };
  }
  return {
    signalType: row.signalOperator,
    stage: row.stageOperator,
    why: row.whyOperator,
    action: row.actionOperator,
  };
}

async function main() {
  const token = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  const tableName = process.env.AIRTABLE_TABLE_MARKET_ALERTS || "MarketAlerts";
  const beforePath =
    process.argv[2] ||
    path.join(process.cwd(), "data", "market-alerts-intelligence-v1-before-200.json");

  const base = new Airtable({ apiKey: token }).base(baseId);
  const afterRows = await loadTop200(base, tableName);
  const afterCounts = countAfter(afterRows);

  let beforeRows = null;
  if (fs.existsSync(beforePath)) {
    beforeRows = JSON.parse(fs.readFileSync(beforePath, "utf8"));
  }

  const diff = beforeRows ? diffRows(beforeRows, afterRows) : null;
  const gateStats = beforeRows ? aggregateGateStats(beforeRows) : null;

  const worthExamples = afterRows
    .filter((r) => r.worthOwner || r.worthBrand || r.worthOperator)
    .slice(0, 15)
    .map((r) => {
      const aud = primaryAudience(r);
      const d = audienceDetail(r, aud);
      return {
        title: r.title,
        eventType: r.eventType,
        audience: aud,
        signalType: d.signalType,
        stage: d.stage,
        whyItMatters: d.why,
        recommendedAction: d.action,
        whyQualified: `${r.treatment} · ${aud} · ${d.signalType || "signal"}`,
      };
    });

  const downgradedExamples = (diff?.downgraded || [])
    .slice(0, 10)
    .map(({ before, after }) => ({
      title: before.title,
      beforeTreatment: before.treatment,
      afterTreatment: after.treatment,
      beforeEvent: before.eventType,
      afterEvent: after.eventType,
      reason:
        before.eventType !== after.eventType
          ? "event_reclassified_or_cleared"
          : "audience_qualification_tightened",
    }));

  const report = {
    generatedAt: new Date().toISOString(),
    rowsTested: 200,
    before: BEFORE_BASELINE,
    after: {
      review: afterCounts.review,
      standard: afterCounts.standard,
      ignore: afterCounts.ignore,
      worthOwner: afterCounts.worthOwner,
      worthBrand: afterCounts.worthBrand,
      worthOperator: afterCounts.worthOperator,
    },
    diff,
    gateStats,
    eventTypeCounts: afterCounts.eventTypes,
    worthExamples,
    downgradedExamples,
  };

  const outPath = path.join(process.cwd(), "data", "market-alerts-intelligence-v1_1-report.json");
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report, null, 2));
  console.log(`\nWrote ${outPath}`);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
