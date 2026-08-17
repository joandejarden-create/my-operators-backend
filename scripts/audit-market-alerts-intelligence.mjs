#!/usr/bin/env node
/**
 * Audit Market Alerts intelligence coverage.
 *
 *   npm run market-alerts:intelligence:audit
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import Airtable from "airtable";
import { MAP_ALERT } from "../api/lib/market-alerts-rss-airtable.js";
import { MAP_INTEL } from "../api/lib/market-alerts-intelligence-map.js";

async function main() {
  const token = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  const tableName = process.env.AIRTABLE_TABLE_MARKET_ALERTS || "MarketAlerts";
  if (!token || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");

  const base = new Airtable({ apiKey: token }).base(baseId);
  const counts = {
    total: 0,
    ready: 0,
    pending: 0,
    error: 0,
    skipped: 0,
    blankStatus: 0,
    treatment: { STANDARD: 0, REVIEW: 0, IGNORE: 0, blank: 0 },
    worthOwner: 0,
    worthBrand: 0,
    worthOperator: 0,
    withEventType: 0,
    withEntityKey: 0,
    earlySignals: {
      total: 0,
      insertedLast24h: 0,
      insertedLast7d: 0,
      review: 0,
      standard: 0,
      rejected: 0,
      byFamily: { Planning: 0, "Early Development": 0, "Mixed Use": 0, "Adaptive Reuse": 0 },
      offTopicRejections: 0,
      entityQualityRejections: 0,
      staleRejections: 0,
    },
  };
  const eventTypeCounts = {};
  const samples = { review: [], forSale: [], errors: [] };

  const now = Date.now();
  const dayMs = 24 * 60 * 60 * 1000;

  await base(tableName)
    .select({
      fields: [
        MAP_ALERT.title,
        MAP_ALERT.publishedAt,
        MAP_ALERT.tags,
        MAP_INTEL.intelligenceStatus,
        MAP_INTEL.intelligenceTreatment,
        MAP_INTEL.eventType,
        MAP_INTEL.entityKey,
        MAP_INTEL.worthReviewingOwner,
        MAP_INTEL.worthReviewingBrand,
        MAP_INTEL.worthReviewingOperator,
      ],
      pageSize: 100,
    })
    .eachPage((records, next) => {
      for (const r of records) {
        counts.total += 1;
        const f = r.fields || {};
        const status = f[MAP_INTEL.intelligenceStatus] || "";
        const treatment = f[MAP_INTEL.intelligenceTreatment] || "";
        const eventType = f[MAP_INTEL.eventType] || "";
        const tags = Array.isArray(f[MAP_ALERT.tags]) ? f[MAP_ALERT.tags] : [];
        const isEarlySignal = tags.includes("EARLY_SIGNAL");

        if (status === "Ready") counts.ready += 1;
        else if (status === "Pending") counts.pending += 1;
        else if (status === "Error") counts.error += 1;
        else if (status === "Skipped") counts.skipped += 1;
        else counts.blankStatus += 1;

        if (treatment === "STANDARD" || treatment === "REVIEW" || treatment === "IGNORE") {
          counts.treatment[treatment] += 1;
        } else counts.treatment.blank += 1;

        if (f[MAP_INTEL.worthReviewingOwner]) counts.worthOwner += 1;
        if (f[MAP_INTEL.worthReviewingBrand]) counts.worthBrand += 1;
        if (f[MAP_INTEL.worthReviewingOperator]) counts.worthOperator += 1;
        if (eventType) {
          counts.withEventType += 1;
          eventTypeCounts[eventType] = (eventTypeCounts[eventType] || 0) + 1;
        }
        if (f[MAP_INTEL.entityKey]) counts.withEntityKey += 1;

        if (isEarlySignal) {
          counts.earlySignals.total += 1;
          const pub = f[MAP_ALERT.publishedAt];
          if (pub) {
            const age = now - new Date(pub).getTime();
            if (Number.isFinite(age)) {
              if (age <= dayMs) counts.earlySignals.insertedLast24h += 1;
              if (age <= 7 * dayMs) counts.earlySignals.insertedLast7d += 1;
            }
          }
          if (treatment === "REVIEW") counts.earlySignals.review += 1;
          else if (treatment === "STANDARD") counts.earlySignals.standard += 1;
          else if (treatment === "IGNORE") counts.earlySignals.rejected += 1;

          if (tags.includes("EARLY_SIGNAL_PLANNING")) counts.earlySignals.byFamily.Planning += 1;
          if (tags.includes("EARLY_SIGNAL_DEVELOPMENT")) {
            counts.earlySignals.byFamily["Early Development"] += 1;
          }
          if (tags.includes("EARLY_SIGNAL_MIXED_USE")) counts.earlySignals.byFamily["Mixed Use"] += 1;
          if (tags.includes("EARLY_SIGNAL_ADAPTIVE_REUSE")) {
            counts.earlySignals.byFamily["Adaptive Reuse"] += 1;
          }
        }

        const title = String(f[MAP_ALERT.title] || "").slice(0, 90);
        if (treatment === "REVIEW" && samples.review.length < 8) {
          samples.review.push({ id: r.id, title, eventType });
        }
        if (eventType === "Hotel For Sale" && samples.forSale.length < 5) {
          samples.forSale.push({
            id: r.id,
            title,
            worthOwner: !!f[MAP_INTEL.worthReviewingOwner],
            worthBrand: !!f[MAP_INTEL.worthReviewingBrand],
            worthOperator: !!f[MAP_INTEL.worthReviewingOperator],
          });
        }
        if (status === "Error" && samples.errors.length < 5) {
          samples.errors.push({ id: r.id, title });
        }
      }
      next();
    });

  const report = {
    generatedAt: new Date().toISOString(),
    table: tableName,
    counts,
    eventTypeCounts,
    samples,
  };

  const outDir = path.join(process.cwd(), "data");
  fs.mkdirSync(outDir, { recursive: true });
  const outPath = path.join(outDir, "market-alerts-intelligence-audit.json");
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));

  console.log(JSON.stringify(report, null, 2));
  console.log(`\nWrote ${outPath}`);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
