/**
 * Brand-decision contact enrichment work queue — owners ranked by intent with contact gaps.
 *
 * Usage:
 *   node scripts/report-gtm-brand-decision-enrichment-queue.mjs
 *   node scripts/report-gtm-brand-decision-enrichment-queue.mjs --country=Mexico
 *   node scripts/report-gtm-brand-decision-enrichment-queue.mjs --country=Mexico --needs-enrichment-only
 *   node scripts/report-gtm-brand-decision-enrichment-queue.mjs --limit=50
 *   node scripts/report-gtm-brand-decision-enrichment-queue.mjs --country=Mexico --needs-enrichment-only --limit=20 --suffix=p1-sprint
 *
 * Writes:
 *   reports/gtm-brand-decision-enrichment-queue.json
 *   reports/gtm-brand-decision-enrichment-queue.csv
 *   reports/gtm-brand-decision-enrichment-queue.md
 *
 * With --country=Mexico also writes:
 *   reports/gtm-brand-decision-enrichment-queue-mx-mexico.*
 */
import "../load-env.js";
import { writeFileSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { assertGtmBaseConfigured, assertNotProductBase } from "../lib/gtm-owner-target/platform-base.js";
import {
  fetchBrandingDecisionTargetRows,
  toEnrichmentQueueItem,
  deriveEnrichmentPriority,
} from "../lib/gtm-owner-target/branding-decision-target-rows.js";
import { MAP_BRANDING_DECISION_CONFIG } from "../lib/gtm-owner-target/branding-decision-signals.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const OUT_BASE = join(ROOT, "reports", "gtm-brand-decision-enrichment-queue");

const countryArg = process.argv.find((a) => a.startsWith("--country="));
const COUNTRY = countryArg ? countryArg.split("=")[1].replace(/^"|"$/g, "") : null;
const NEEDS_ENRICHMENT_ONLY = process.argv.includes("--needs-enrichment-only");
const minScoreArg = process.argv.find((a) => a.startsWith("--min-score="));
const MIN_SCORE = minScoreArg ? Number(minScoreArg.split("=")[1]) : 25;
const limitArg = process.argv.find((a) => a.startsWith("--limit="));
const LIMIT = limitArg ? Number(limitArg.split("=")[1]) : null;
const suffixArg = process.argv.find((a) => a.startsWith("--suffix="));
const OUTPUT_SUFFIX = suffixArg ? suffixArg.split("=")[1].replace(/^"|"$/g, "") : "";

function csvEscape(value) {
  const s = String(value ?? "");
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function priorityRank(p) {
  const order = ["P0_outreach_ready", "P1_high_intent_tier_a", "P1b_linkedin_ready_tier_a", "P2_tier_a", "P3_high_intent", "P4_medium_intent", "P5_backlog"];
  const i = order.indexOf(p);
  return i >= 0 ? i : 99;
}

function buildMarkdown(items, summary, filters) {
  const lines = [
    "# Brand-Decision Contact Enrichment Queue",
    "",
    `Generated: ${new Date().toISOString()}`,
    "",
    "Working list for **named-email / LinkedIn / verified phone** research on brand-decision-eligible owners.",
    "",
    `- Country filter: **${filters.country || "CALA (all)"}**`,
    `- Min intent score: **${filters.minScore}**`,
    `- Needs enrichment only: **${filters.needsEnrichmentOnly}**`,
    `- Total in queue: **${summary.total}**`,
    `- Outreach-ready (skip enrichment): **${summary.outreachReady}**`,
    `- Needs contact work: **${summary.needsEnrichment}**`,
    "",
    "## Priority breakdown",
    "",
  ];

  for (const [priority, count] of Object.entries(summary.byEnrichmentPriority || {}).sort(
    (a, b) => priorityRank(a[0]) - priorityRank(b[0])
  )) {
    lines.push(`- **${priority}:** ${count}`);
  }

  lines.push("", "## P1 — Tier A high intent (enrich first)", "");
  for (const item of items.filter((i) => i.enrichmentPriority.startsWith("P1")).slice(0, 30)) {
    lines.push(
      `- **${item.ownerName}** (${item.intentScore}) — ${item.contactGaps || "gaps unknown"} → ${item.suggestedAction}`
    );
  }

  lines.push("", "## P2 — Tier A (enrich next)", "");
  for (const item of items.filter((i) => i.enrichmentPriority === "P2_tier_a").slice(0, 30)) {
    lines.push(
      `- **${item.ownerName}** (${item.intentScore}) — ${item.contactGaps} → ${item.suggestedAction}`
    );
  }

  lines.push("", "## Outreach-ready (P0 — send now)", "");
  for (const item of items.filter((i) => i.outreachReady).slice(0, 25)) {
    lines.push(
      `- **${item.ownerName}** — ${item.contactName} <${item.contactEmail || item.contactLinkedIn}>`
    );
  }

  lines.push("", "## Commands", "", "```bash", "node scripts/report-gtm-brand-decision-enrichment-queue.mjs --country=Mexico", "node scripts/report-gtm-brand-decision-enrichment-queue.mjs --country=Mexico --needs-enrichment-only", "node scripts/report-gtm-brand-decision-enrichment-queue.mjs --country=Mexico --limit=100", "```");

  return lines.join("\n");
}

async function main() {
  assertGtmBaseConfigured();
  assertNotProductBase();

  let rows = await fetchBrandingDecisionTargetRows({
    country: COUNTRY,
    minScore: MIN_SCORE,
    brandDecisionOnly: true,
    needsEnrichmentOnly: NEEDS_ENRICHMENT_ONLY,
  });

  let items = rows.map(toEnrichmentQueueItem);
  items.sort(
    (a, b) =>
      priorityRank(a.enrichmentPriority) - priorityRank(b.enrichmentPriority) ||
      b.intentScore - a.intentScore ||
      String(a.priorityTier).localeCompare(String(b.priorityTier))
  );

  if (LIMIT != null && LIMIT > 0) {
    items = items.slice(0, LIMIT);
  }

  const summary = {
    total: items.length,
    outreachReady: items.filter((i) => i.outreachReady).length,
    needsEnrichment: items.filter((i) => i.needsEnrichment).length,
    byEnrichmentPriority: {},
    byOutreachTrack: {},
    byContactGap: {},
  };

  for (const item of items) {
    summary.byEnrichmentPriority[item.enrichmentPriority] =
      (summary.byEnrichmentPriority[item.enrichmentPriority] || 0) + 1;
    summary.byOutreachTrack[item.outreachTrack] =
      (summary.byOutreachTrack[item.outreachTrack] || 0) + 1;
    for (const gap of item.contactGaps.split("|").filter(Boolean)) {
      summary.byContactGap[gap] = (summary.byContactGap[gap] || 0) + 1;
    }
  }

  const suffix = [
    COUNTRY ? `-mx-${COUNTRY.toLowerCase().replace(/[^a-z0-9]+/g, "-")}` : "",
    OUTPUT_SUFFIX ? `-${OUTPUT_SUFFIX.replace(/[^a-z0-9-]+/gi, "-").replace(/^-|-$/g, "")}` : "",
  ].join("");
  const outJson = `${OUT_BASE}${suffix}.json`;
  const outCsv = `${OUT_BASE}${suffix}.csv`;
  const outMd = `${OUT_BASE}${suffix}.md`;

  mkdirSync(dirname(outJson), { recursive: true });

  const filters = {
    country: COUNTRY,
    minScore: MIN_SCORE,
    needsEnrichmentOnly: NEEDS_ENRICHMENT_ONLY,
    limit: LIMIT,
    brandDecisionOnly: true,
  };

  writeFileSync(
    outJson,
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        filters,
        config: MAP_BRANDING_DECISION_CONFIG,
        summary,
        items,
      },
      null,
      2
    )
  );

  const csvHeaders = [
    "enrichmentPriority",
    "ownerName",
    "ownerTargetId",
    "priorityTier",
    "outreachReady",
    "needsEnrichment",
    "outreachTrack",
    "intentScore",
    "outreachScore",
    "primaryDealTrigger",
    "brandDecisionEligiblePropertyCount",
    "contactName",
    "contactEmail",
    "contactPhone",
    "contactLinkedIn",
    "hasVerifiedPersonEmail",
    "contactGaps",
    "suggestedAction",
    "mxCorporateSeedSlug",
    "companyWebsite",
    "topLeadAsset",
    "topLeadBrand",
    "topLeadCity",
    "calaPropertyCount",
    "linkedContactCount",
    "pitchAngle",
  ];

  const csvLines = [csvHeaders.join(",")];
  for (const item of items) {
    csvLines.push(
      csvHeaders
        .map((h) => csvEscape(item[h]))
        .join(",")
    );
  }
  writeFileSync(outCsv, csvLines.join("\n"));
  writeFileSync(outMd, buildMarkdown(items, summary, filters));

  console.log(`Enrichment queue: ${items.length} owners`);
  console.log(`  Outreach-ready (P0): ${summary.outreachReady}`);
  console.log(`  Needs enrichment: ${summary.needsEnrichment}`);
  console.log(`  P1 high-intent Tier A: ${summary.byEnrichmentPriority.P1_high_intent_tier_a || 0}`);
  console.log("Wrote", outJson);
  console.log("Wrote", outCsv);
  console.log("Wrote", outMd);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
