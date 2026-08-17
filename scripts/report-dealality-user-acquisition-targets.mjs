/**
 * Report Dealality customer acquisition targets (platform users, not just CoStar owners).
 *
 *   node scripts/report-dealality-user-acquisition-targets.mjs
 *   node scripts/report-dealality-user-acquisition-targets.mjs --country="Dominican Republic"
 *   node scripts/report-dealality-user-acquisition-targets.mjs --priority=P1 --outreach-ready-only
 *
 * Writes:
 *   reports/dealality-user-acquisition-targets.json
 *   reports/dealality-user-acquisition-targets.csv
 *   reports/dealality-user-acquisition-targets.md
 */
import { writeFileSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildDealalityUserAcquisitionTargets } from "../lib/gtm-owner-target/build-dealality-user-acquisition-targets.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const OUT_JSON = join(ROOT, "reports", "dealality-user-acquisition-targets.json");
const OUT_CSV = join(ROOT, "reports", "dealality-user-acquisition-targets.csv");
const OUT_MD = join(ROOT, "reports", "dealality-user-acquisition-targets.md");

const countryArg = process.argv.find((a) => a.startsWith("--country="));
const COUNTRY_FILTER = countryArg
  ? countryArg.split("=")[1].replace(/^"|"$/g, "")
  : null;
const priorityArg = process.argv.find((a) => a.startsWith("--priority="));
const PRIORITY_FILTER = priorityArg ? priorityArg.split("=")[1] : null;
const OUTREACH_READY_ONLY = process.argv.includes("--outreach-ready-only");
const ALIS_ONLY = process.argv.includes("--alis-only");
const PROSPECT_ONLY = process.argv.includes("--prospect-only");

function csvEscape(value) {
  const s = String(value ?? "");
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function filterItems(items) {
  return items.filter((item) => {
    if (COUNTRY_FILTER) {
      const hay = `${item.country || ""} ${item.countriesSummary || ""}`.toLowerCase();
      if (!hay.includes(COUNTRY_FILTER.toLowerCase())) return false;
    }
    if (PRIORITY_FILTER && item.acquisitionPriority !== PRIORITY_FILTER) return false;
    if (OUTREACH_READY_ONLY && !item.outreachReady) return false;
    if (ALIS_ONLY && !item.alisCala2026Attendee) return false;
    if (PROSPECT_ONLY && !item.prospectOnly) return false;
    return true;
  });
}

function toCsv(items) {
  const headers = [
    "acquisitionPriority",
    "acquisitionScore",
    "acquisitionSegment",
    "acquisitionStage",
    "contactName",
    "contactTitle",
    "company",
    "ownerName",
    "ownerTargetId",
    "country",
    "verificationTier",
    "contactEmail",
    "contactLinkedIn",
    "outreachReady",
    "alisCala2026Attendee",
    "strikeListMember",
    "prospectOnly",
    "sourceTracks",
    "pitchAngle",
  ];
  const lines = [headers.join(",")];
  for (const item of items) {
    lines.push(
      headers
        .map((h) => {
          if (h === "sourceTracks") return csvEscape((item.sourceTracks || []).join("|"));
          return csvEscape(item[h]);
        })
        .join(",")
    );
  }
  return lines.join("\n");
}

function toMd(report, items) {
  const lines = [
    "# Dealality customer acquisition targets",
    "",
    `Generated: ${report.generatedAt}`,
    "",
    "## Summary",
    "",
    `- **Total:** ${items.length} (filtered from ${report.summary.totalTargets})`,
    `- **P1:** ${items.filter((i) => i.acquisitionPriority === "P1").length}`,
    `- **Outreach-ready:** ${items.filter((i) => i.outreachReady).length}`,
    `- **ALIS CALA 2026:** ${items.filter((i) => i.alisCala2026Attendee).length}`,
    `- **Prospect-only (no CoStar row):** ${items.filter((i) => i.prospectOnly).length}`,
    "",
    "## Top P1 targets",
    "",
  ];

  for (const item of items.filter((i) => i.acquisitionPriority === "P1").slice(0, 40)) {
    const who = item.contactName
      ? `**${item.contactName}** (${item.contactTitle || "—"})`
      : "—";
    lines.push(
      `- ${who} — ${item.company || item.ownerName} [${item.acquisitionSegment}] score ${item.acquisitionScore}${item.alisCala2026Attendee ? " · ALIS" : ""}${item.strikeListMember ? " · strike" : ""}`
    );
    if (item.pitchAngle) lines.push(`  - ${item.pitchAngle}`);
  }

  lines.push("", "## DR ALIS batch (Jul 2026)", "");
  for (const item of items.filter(
    (i) =>
      i.alisCala2026Attendee &&
      String(i.country || i.countriesSummary || "").toLowerCase().includes("dominican")
  )) {
    lines.push(
      `- **${item.contactName || "—"}** — ${item.company} (${item.acquisitionStage}, ${item.verificationTier || "—"})`
    );
  }

  return lines.join("\n");
}

function main() {
  mkdirSync(join(ROOT, "reports"), { recursive: true });
  const report = buildDealalityUserAcquisitionTargets({ root: ROOT });
  const items = filterItems(report.items);
  const filtered = { ...report, items, summary: { ...report.summary, filteredCount: items.length } };

  writeFileSync(OUT_JSON, JSON.stringify(filtered, null, 2));
  writeFileSync(OUT_CSV, toCsv(items));
  writeFileSync(OUT_MD, toMd(report, items));

  console.log(`Dealality acquisition targets: ${items.length} (of ${report.summary.totalTargets})`);
  console.log(`  P1: ${items.filter((i) => i.acquisitionPriority === "P1").length}`);
  console.log(`  outreach-ready: ${items.filter((i) => i.outreachReady).length}`);
  console.log(`  wrote ${OUT_JSON}`);
}

main();
