/**
 * Owner portfolio confidence audit — verify CoStar hotel rollups before outreach.
 *
 *   node scripts/report-gtm-owner-portfolio-audit.mjs
 *   node scripts/report-gtm-owner-portfolio-audit.mjs --p0-only
 *   node scripts/report-gtm-owner-portfolio-audit.mjs --owner="Grupo Questro"
 *   node scripts/report-gtm-owner-portfolio-audit.mjs --unsafe-only
 *
 * Writes:
 *   reports/gtm-owner-portfolio-audit.json
 *   reports/gtm-owner-portfolio-audit.csv
 *   reports/gtm-owner-portfolio-audit.md
 */
import "../load-env.js";
import { writeFileSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { assertGtmBaseConfigured, assertNotProductBase } from "../lib/gtm-owner-target/platform-base.js";
import {
  fetchBrandingDecisionTargetRows,
  deriveEnrichmentPriority,
} from "../lib/gtm-owner-target/branding-decision-target-rows.js";
import { normalizeOwnerKey } from "../lib/gtm-owner-target/normalize.js";
import {
  fetchAllGtmProperties,
  groupAirtablePropertiesByOwner,
} from "../lib/gtm-owner-target/properties-read.js";
import { isCalaCountry } from "../lib/gtm-owner-target/cala-footprint.js";
import {
  buildOwnerPortfolioAudit,
  summarizePortfolioAudits,
  MANUAL_PORTFOLIO_AUDIT,
} from "../lib/gtm-owner-target/owner-portfolio-audit.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const OUT_JSON = join(ROOT, "reports", "gtm-owner-portfolio-audit.json");
const OUT_CSV = join(ROOT, "reports", "gtm-owner-portfolio-audit.csv");
const OUT_MD = join(ROOT, "reports", "gtm-owner-portfolio-audit.md");

const P0_ONLY = process.argv.includes("--p0-only");
const UNSAFE_ONLY = process.argv.includes("--unsafe-only");
const ownerArg = process.argv.find((a) => a.startsWith("--owner="));
const OWNER_FILTER = ownerArg ? ownerArg.split("=").slice(1).join("=").replace(/^"|"$/g, "") : null;
const minScoreArg = process.argv.find((a) => a.startsWith("--min-score="));
const MIN_SCORE = minScoreArg ? Number(minScoreArg.split("=")[1]) : 25;

function csvEscape(value) {
  const s = String(value ?? "");
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function confidenceRank(level) {
  const order = { blocked: 0, low: 1, medium: 2, high: 3 };
  return order[level] ?? 9;
}

function buildMarkdown(audits, summary) {
  const lines = [
    "# GTM Owner Portfolio Audit",
    "",
    `Generated: ${new Date().toISOString()}`,
    "",
    "CoStar **True Owner** rollups with operator-alignment checks. Use before outreach to confirm which hotels to reference.",
    "",
    `- Owners audited: **${summary.total}**`,
    `- Outreach-safe (high, or medium without entity block): **${summary.outreachSafe}**`,
    `- Needs review: **${summary.needsAudit}**`,
    "",
    "## Confidence breakdown",
    "",
    `- **high:** ${summary.byConfidence.high || 0}`,
    `- **medium:** ${summary.byConfidence.medium || 0}`,
    `- **low:** ${summary.byConfidence.low || 0}`,
    `- **blocked:** ${summary.byConfidence.blocked || 0}`,
    "",
    "## Blocked / audit first (do not pitch full portfolio)",
    "",
  ];

  const blocked = audits.filter((a) => a.portfolioConfidence === "blocked" || !a.outreachSafe);
  if (!blocked.length) {
    lines.push("_None in this run._");
  } else {
    for (const a of blocked.slice(0, 25)) {
      lines.push(
        `- **${a.ownerName}** [${a.portfolioConfidence}] — ${a.calaPropertyCount} hotels, ${a.pitchEligibleCount} pitch-eligible — ${a.flags.join("|")}`
      );
      lines.push(`  - ${a.outreachGuidance}`);
      if (a.leadPitchAsset) lines.push(`  - Lead asset: ${a.leadPitchAsset}`);
    }
  }

  lines.push("", "## Outreach-safe P0 owners", "");
  const safe = audits.filter((a) => a.outreachSafe && a.outreachReady);
  if (!safe.length) lines.push("_None._");
  else {
    for (const a of safe.slice(0, 40)) {
      lines.push(
        `- **${a.ownerName}** — ${a.contactName || "no contact"} — ${a.pitchEligibleCount}/${a.calaPropertyCount} pitch-eligible — lead: ${a.leadPitchAsset || "—"}`
      );
    }
  }

  lines.push("", "## Commands", "", "```bash", "node scripts/report-gtm-owner-portfolio-audit.mjs --p0-only", "node scripts/report-gtm-owner-portfolio-audit.mjs --unsafe-only", "```");
  return lines.join("\n");
}

async function main() {
  assertGtmBaseConfigured();
  assertNotProductBase(process.env.AIRTABLE_GTM_BASE_ID);

  const rows = await fetchBrandingDecisionTargetRows({ minScore: MIN_SCORE });
  const { records } = await fetchAllGtmProperties();
  const groups = groupAirtablePropertiesByOwner(records);
  const propsByKey = new Map(groups.map((g) => [g.ownerKey, g.properties.filter((p) => isCalaCountry(p.country))]));

  /** @type {string[]} */
  const priorityKeys = Object.keys(MANUAL_PORTFOLIO_AUDIT);

  let targets = rows.map((row) => ({
    ...row,
    enrichmentPriority: deriveEnrichmentPriority(row),
  }));

  if (P0_ONLY) {
    targets = targets.filter((t) => t.outreachReady);
  }

  if (OWNER_FILTER) {
    const needle = normalizeOwnerKey(OWNER_FILTER);
    targets = targets.filter((t) => normalizeOwnerKey(t.ownerName).includes(needle));
  }

  if (!OWNER_FILTER) {
    const seen = new Set(targets.map((t) => normalizeOwnerKey(t.ownerName)));
    for (const row of rows) {
      const key = normalizeOwnerKey(row.ownerName);
      if (priorityKeys.includes(key) && !seen.has(key)) {
        targets.push({ ...row, enrichmentPriority: deriveEnrichmentPriority(row) });
        seen.add(key);
      }
    }
  }

  /** @type {ReturnType<buildOwnerPortfolioAudit>[]} */
  const audits = [];

  for (const row of targets) {
    const key = normalizeOwnerKey(row.ownerName);
    const properties = propsByKey.get(key) || [];
    audits.push(
      buildOwnerPortfolioAudit(
        {
          ...row,
          ownerTargetId: row.ownerTargetId,
        },
        properties
      )
    );
  }

  audits.sort(
    (a, b) =>
      confidenceRank(a.portfolioConfidence) - confidenceRank(b.portfolioConfidence) ||
      (b.intentScore || 0) - (a.intentScore || 0) ||
      String(a.ownerName).localeCompare(String(b.ownerName))
  );

  let filtered = audits;
  if (UNSAFE_ONLY) {
    filtered = audits.filter((a) => !a.outreachSafe);
  }

  const summary = summarizePortfolioAudits(filtered);

  const csvHeader = [
    "portfolioConfidence",
    "outreachSafe",
    "ownerName",
    "ownerTargetId",
    "outreachReady",
    "contactName",
    "calaPropertyCount",
    "pitchEligibleCount",
    "operatorMatchRate",
    "flags",
    "leadPitchAsset",
    "outreachGuidance",
    "operatorsSummary",
    "countriesSummary",
  ];
  const csvLines = [
    csvHeader.join(","),
    ...filtered.map((a) => csvHeader.map((h) => csvEscape(a[h])).join(",")),
  ];

  mkdirSync(dirname(OUT_JSON), { recursive: true });
  writeFileSync(
    OUT_JSON,
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        filters: { p0Only: P0_ONLY, unsafeOnly: UNSAFE_ONLY, owner: OWNER_FILTER, minScore: MIN_SCORE },
        summary,
        items: filtered,
      },
      null,
      2
    )
  );
  writeFileSync(OUT_CSV, csvLines.join("\n"));
  writeFileSync(OUT_MD, buildMarkdown(filtered, summary));

  console.log(`Portfolio audit: ${filtered.length} owners`);
  console.log(`  Outreach-safe: ${summary.outreachSafe}`);
  console.log(`  Needs review: ${summary.needsAudit}`);
  console.log(`  Confidence:`, summary.byConfidence);
  console.log("Wrote", OUT_JSON);
  console.log("Wrote", OUT_CSV);
  console.log("Wrote", OUT_MD);
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
