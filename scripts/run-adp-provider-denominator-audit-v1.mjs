#!/usr/bin/env node
/**
 * Provider denominator audit — Existing Hotel ADP (5 × 4).
 * Ledger truth vs published math vs UI contract fields.
 */
import { writeFileSync, mkdirSync, existsSync, readFileSync, readdirSync } from "fs";
import { join } from "path";
import { loadAllPeriods, loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import { loadPublishedReport, loadPublishedManifest } from "../lib/ai-demand-positioning/published-snapshot.js";
import { isComparableObservation } from "../lib/ai-demand-positioning/metrics/grain-governance.js";
import { getPublishedOwnerReport } from "../lib/ai-demand-positioning/published-read-service.js";
import { PROVIDERS } from "../lib/ai-demand-positioning/data-model.js";
import {
  MEASUREMENT_CONTRACT_VERSION,
  buildMeasurementContractCanonicalBody,
} from "../lib/ai-demand-positioning/contracts/adp-measurement-contract-v1.js";

const PROPERTIES = [
  "adp_waterstone_boca_raton",
  "adp_renaissance_times_square",
  "adp_cambridge_beaches_bermuda",
  "adp_now_now_noho",
  "adp_hotel_phillips_kansas_city",
];

const LABELS = {
  adp_waterstone_boca_raton: "Waterstone",
  adp_renaissance_times_square: "Renaissance Times Square",
  adp_cambridge_beaches_bermuda: "Cambridge Beaches",
  adp_now_now_noho: "NOW NOW NOHO",
  adp_hotel_phillips_kansas_city: "Hotel Phillips",
};

function round1(n) {
  return n == null || !Number.isFinite(Number(n)) ? null : Math.round(Number(n) * 10) / 10;
}

function pickPeriod(propertyId) {
  const man = loadPublishedManifest(propertyId);
  const periods = loadAllPeriods(propertyId);
  if (man?.latestPeriodId) {
    const hit = periods.find((p) => p.periodId === man.latestPeriodId);
    if (hit) return hit;
  }
  return periods.sort((a, b) => String(b.executionDate || "").localeCompare(String(a.executionDate || "")))[0];
}

function ledgerByProvider(period) {
  const by = {};
  for (const provider of PROVIDERS) {
    by[provider] = {
      scheduled: 0,
      attempted: 0,
      successful: 0,
      failed: 0,
      missing: 0,
      comparable: 0,
      mentionedComparable: 0,
      mentionedAllParsed: 0,
    };
  }
  for (const obs of period?.observations || []) {
    const p = obs.provider;
    if (!by[p]) continue;
    by[p].scheduled++;
    by[p].attempted++;
    const failed = Boolean(obs.error || obs.providerError || obs.status === "FAILED");
    const comparable = isComparableObservation(obs);
    if (failed) by[p].failed++;
    if (!comparable) {
      if (!failed) by[p].missing++;
    } else {
      by[p].successful++;
      by[p].comparable++;
      if (obs.mentioned) by[p].mentionedComparable++;
    }
    if (obs.parsed && obs.mentioned) by[p].mentionedAllParsed++;
  }
  return by;
}

function oldRateIfScheduledDenom(mentioned, scheduled) {
  if (!scheduled) return null;
  return round1((mentioned / scheduled) * 100);
}

function statusRow(ledger, publishedRow) {
  const mathDenom = ledger.comparable;
  const displayDenom = publishedRow?.comparable ?? publishedRow?.total ?? null;
  const mathRate =
    mathDenom > 0 ? round1((ledger.mentionedComparable / mathDenom) * 100) : null;
  const displayRate = publishedRow?.presence != null ? round1(publishedRow.presence) : null;

  const calcOk =
    mathDenom === 0
      ? displayRate == null || (displayRate === 0 && mathDenom === 0) // flag zero-success separately
      : displayRate === mathRate && (publishedRow?.total === mathDenom || publishedRow?.comparable === mathDenom);

  const displayOk = displayDenom === mathDenom;

  let status = "PASS";
  if (!calcOk && !displayOk) status = "BOTH";
  else if (!calcOk) status = "CALCULATION_MISMATCH";
  else if (!displayOk) status = "DISPLAY_MISMATCH";

  // Zero success: contract says missing != zero — presence should be null, not 0
  if (mathDenom === 0) {
    if (displayRate === 0) status = status === "PASS" ? "CALCULATION_MISMATCH" : status;
    else if (displayRate == null && displayDenom === 0) status = "PASS";
  }

  return { mathDenom, displayDenom, mathRate, displayRate, status };
}

async function main() {
  const contract = buildMeasurementContractCanonicalBody();
  const rows = [];
  const phillipsDetail = {};

  for (const propertyId of PROPERTIES) {
    const period = pickPeriod(propertyId);
    const published = loadPublishedReport(propertyId);
    const read = await getPublishedOwnerReport(propertyId);
    const evidenceProviders = read.payload?.evidence?.providers || published?.evidence?.providers || [];
    const ledger = ledgerByProvider(period);

    for (const provider of PROVIDERS) {
      const L = ledger[provider];
      const pub = evidenceProviders.find((p) => p.provider === provider);
      const { mathDenom, displayDenom, mathRate, displayRate, status } = statusRow(L, pub);
      const oldScheduledRate = oldRateIfScheduledDenom(L.mentionedComparable, L.scheduled);

      const row = {
        propertyId,
        property: LABELS[propertyId],
        provider,
        periodId: period?.periodId,
        scheduled: L.scheduled,
        attempted: L.attempted,
        successful: L.successful,
        failed: L.failed,
        missing: L.missing,
        failedOrMissing: L.failed + L.missing,
        mathDenominator: mathDenom,
        displayDenominator: displayDenom,
        includedInMath: L.comparable,
        mentionedComparable: L.mentionedComparable,
        mathRate,
        displayRate,
        oldScheduledDenomRate: oldScheduledRate,
        rateChangedVsScheduledDenom: oldScheduledRate != null && mathRate != null && oldScheduledRate !== mathRate,
        publishedFields: pub
          ? {
              scheduled: pub.scheduled ?? pub.scenariosScheduled ?? null,
              comparable: pub.comparable ?? null,
              total: pub.total ?? null,
              mentioned: pub.mentioned ?? null,
              excludedFromMetric: pub.excludedFromMetric ?? null,
              presence: pub.presence ?? null,
              denominatorGrain: pub.denominatorGrain ?? null,
            }
          : null,
        status,
      };
      rows.push(row);

      if (propertyId === "adp_hotel_phillips_kansas_city" && provider === "gemini") {
        Object.assign(phillipsDetail, {
          ...row,
          note:
            "Consideration uses comparable observations (failed Gemini omitted). Provider Gemini must use the same comparable grain, not scheduled 63.",
        });
      }
    }
  }

  const summary = {
    PASS: rows.filter((r) => r.status === "PASS").length,
    DISPLAY_MISMATCH: rows.filter((r) => r.status === "DISPLAY_MISMATCH").length,
    CALCULATION_MISMATCH: rows.filter((r) => r.status === "CALCULATION_MISMATCH").length,
    BOTH: rows.filter((r) => r.status === "BOTH").length,
  };

  const outDir = join(process.cwd(), "reports/ai-demand-positioning");
  mkdirSync(outDir, { recursive: true });
  const out = {
    title: "ADP_PROVIDER_DENOMINATOR_AUDIT_V1",
    finished: new Date().toISOString(),
    contractVersion: MEASUREMENT_CONTRACT_VERSION,
    canonicalRules: {
      COMPARABLE_OBSERVATION_RULE: contract.COMPARABLE_OBSERVATION_RULE,
      CONSIDERATION_RATE_FORMULA: contract.CONSIDERATION_RATE_FORMULA,
      PROVIDER_PRESENCE_RATE_FORMULA:
        "Provider Presence Rate = subject-present comparable observations for provider / comparable observations for provider. Failed/missing provider calls are omitted (Missing != measured zero).",
      PROVIDER_DENOMINATOR_GRAIN: "comparable_observations",
    },
    summary,
    phillipsGemini: phillipsDetail,
    rows,
  };

  const path = join(outDir, "adp-provider-denominator-audit-v1.json");
  writeFileSync(path, JSON.stringify(out, null, 2) + "\n");

  // Markdown table
  let md = `# ADP Provider Denominator Audit V1\n\n`;
  md += `Contract: \`${MEASUREMENT_CONTRACT_VERSION}\`\n\n`;
  md += `## Canonical rule\n\n`;
  md += `${out.canonicalRules.PROVIDER_PRESENCE_RATE_FORMULA}\n\n`;
  md += `Comparable rule: ${contract.COMPARABLE_OBSERVATION_RULE}\n\n`;
  md += `| Property | Provider | Scheduled | Successful | Failed/Missing | Math Denominator | Display Denominator | Status |\n`;
  md += `|---|---|---:|---:|---:|---:|---:|---|\n`;
  for (const r of rows) {
    md += `| ${r.property} | ${r.provider} | ${r.scheduled} | ${r.successful} | ${r.failedOrMissing} | ${r.mathDenominator} | ${r.displayDenominator} | **${r.status}** |\n`;
  }
  md += `\n## Summary\n\n\`\`\`json\n${JSON.stringify(summary, null, 2)}\n\`\`\`\n`;
  md += `\n## Phillips Gemini\n\n\`\`\`json\n${JSON.stringify(phillipsDetail, null, 2)}\n\`\`\`\n`;

  const mdPath = join(outDir, "ADP_PROVIDER_DENOMINATOR_AUDIT_V1.md");
  writeFileSync(mdPath, md);
  console.log(JSON.stringify({ path, mdPath, summary, phillipsGemini: phillipsDetail }, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
