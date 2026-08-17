/**
 * Extract Wave 15 Stage 6 post-image cleanup failures (read-only).
 * Primary: snapshot.typical_keys Portfolio blanks + stale PF mismatches.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import "../load-env.js";
import Airtable from "airtable";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "../lib/partner-intelligence/brand-explorer-factory-preview-candidates.js";
import {
  WAVE15_SLUGS,
  WAVE15_BRAND_PLAN,
} from "../lib/partner-intelligence/brand-explorer-wave15-factory-plan.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const REPORTS = path.join(ROOT, "reports");
const PF_TABLE = "Brand Setup - Project Fit";
const PP_TABLE = "Brand Setup - Portfolio & Performance";

function canonicalColumnName(s) {
  return String(s).trim().replace(/\u00A0/g, " ").replace(/\u2013|\u2014/g, "-");
}

async function resolvePortfolioRoomFieldNames(base) {
  const rows = await base(PP_TABLE).select({ maxRecords: 80 }).all();
  const keys = [...new Set(rows.flatMap((r) => Object.keys(r.fields)))];
  const pick = (re) => keys.find((k) => re.test(canonicalColumnName(k))) || null;
  const minKey = pick(/^minimum property size \(rooms\)$/i);
  const maxKey = pick(/^maximum property size \(rooms\)$/i);
  if (!minKey || !maxKey) {
    throw new Error(`Could not resolve Portfolio room columns`);
  }
  return { minKey, maxKey };
}

async function findByName(base, table, brandName) {
  const esc = brandName.replace(/"/g, '\\"');
  const rows = await base(table)
    .select({ filterByFormula: `{Brand Name} = "${esc}"`, maxRecords: 3 })
    .all();
  return rows[0] || null;
}

function renderedKeys(min, max) {
  if (min != null && max != null) return `${min}–${max} rooms`;
  if (min != null) return `${min}+ rooms (minimum)`;
  if (max != null) return `Up to ${max} rooms`;
  return "";
}

async function main() {
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID
  );
  const { minKey, maxKey } = await resolvePortfolioRoomFieldNames(base);
  const failures = [];

  for (const slug of WAVE15_SLUGS) {
    const plan = WAVE15_BRAND_PLAN[slug];
    const identity = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[slug];
    const pf = await findByName(base, PF_TABLE, plan.name);
    const pp = await findByName(base, PP_TABLE, plan.name);
    const pfMin = pf?.get("Min - Room Count") ?? null;
    const pfMax = pf?.get("Max - Room Count") ?? null;
    const ppMin = pp?.get(minKey) ?? null;
    const ppMax = pp?.get(maxKey) ?? null;
    const current = renderedKeys(ppMin, ppMax);
    const proposed =
      pfMin != null || pfMax != null
        ? renderedKeys(pfMin, pfMax)
        : "Typical key range not shown in current public materials.";

    if (ppMin == null && ppMax == null) {
      failures.push({
        brand: plan.name,
        slug,
        recordId: identity?.recordId || null,
        portfolioRecordId: pp?.id || null,
        projectFitRecordId: pf?.id || null,
        section: "Brand Snapshot",
        field: "snapshot.typical_keys",
        failureType: "typical_keys_blank_cleanly_unavailable",
        currentValue: "(blank)",
        proposedFix: `Copy Project Fit Min/Max Room Count → Portfolio ${minKey} / ${maxKey} → render "${proposed}"`,
        sourceSupport:
          "Brand Setup - Project Fit Min/Max Room Count (segment/brand steward apply; Choice-style Portfolio sync)",
        stewardRequired: false,
        writeRequired: true,
        proposedMin: pfMin,
        proposedMax: pfMax,
      });
    } else if (
      pfMin != null &&
      pfMax != null &&
      (Number(ppMin) !== Number(pfMin) || Number(ppMax) !== Number(pfMax))
    ) {
      failures.push({
        brand: plan.name,
        slug,
        recordId: identity?.recordId || null,
        portfolioRecordId: pp?.id || null,
        projectFitRecordId: pf?.id || null,
        section: "Brand Snapshot",
        field: "snapshot.typical_keys",
        failureType: "typical_keys_stale_portfolio_mismatch",
        currentValue: current,
        proposedFix: `Reconcile Portfolio to Project Fit → "${proposed}" (replace stale ${current})`,
        sourceSupport:
          "Brand Setup - Project Fit Min/Max Room Count (segment/brand steward apply; Choice-style Portfolio sync)",
        stewardRequired: false,
        writeRequired: true,
        proposedMin: pfMin,
        proposedMax: pfMax,
      });
    }
  }

  const report = {
    version: "wave15-post-image-cleanup-failures-v1",
    generatedAt: new Date().toISOString(),
    protectedBaseline: "frozen_54_active_public_full_baseline_semantic_clean_flex_held",
    stage5Ready: "wave15_image_materialization_ready_for_post_image_cleanup",
    notes: [
      "Stage 5 left overview.scenario images cleared; uniqueness/role-match/no-empty/golden PASS.",
      "Primary Stage 6 blocker: snapshot.typical_keys derived from Portfolio min/max rooms (not Presentation).",
      "No Presentation caption/momentum/openings residual fails in Stage 5 post-validate beyond typical_keys.",
      "Owner-facing unavailable phrasing cannot pass rendered-completeness — numeric Portfolio fields required.",
    ],
    counts: {
      total: failures.length,
      actionable: failures.filter((f) => f.writeRequired).length,
      blank: failures.filter((f) => f.failureType === "typical_keys_blank_cleanly_unavailable")
        .length,
      staleMismatch: failures.filter((f) => f.failureType === "typical_keys_stale_portfolio_mismatch")
        .length,
    },
    failures,
  };

  fs.mkdirSync(REPORTS, { recursive: true });
  const jsonPath = path.join(REPORTS, "brand-explorer-wave15-post-image-cleanup-failures.json");
  const mdPath = path.join(REPORTS, "brand-explorer-wave15-post-image-cleanup-failures.md");
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`);

  const md = [
    `# Wave 15 Stage 6 — Post-Image Cleanup Failures`,
    ``,
    `Generated: ${report.generatedAt}`,
    ``,
    `## Summary`,
    ``,
    `- Total rows: **${report.counts.total}**`,
    `- Actionable: **${report.counts.actionable}**`,
    `- Blank typical_keys: **${report.counts.blank}**`,
    `- Stale Portfolio≠Project Fit: **${report.counts.staleMismatch}**`,
    `- Primary theme: **snapshot.typical_keys Portfolio sync from Project Fit**`,
    ``,
    `## Notes`,
    ``,
    ...report.notes.map((n) => `- ${n}`),
    ``,
    `## Failure table`,
    ``,
    `| Brand | Slug | Record ID | Section | Field | Failure Type | Current Value | Proposed Fix | Source Support | Steward? |`,
    `| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |`,
    ...failures.map(
      (f) =>
        `| ${f.brand} | \`${f.slug}\` | \`${f.recordId}\` | ${f.section} | ${f.field} | ${f.failureType} | ${f.currentValue} | ${f.proposedFix.replace(/\|/g, "/")} | ${f.sourceSupport} | ${f.stewardRequired ? "yes" : "no"} |`
    ),
    ``,
  ];
  fs.writeFileSync(mdPath, md.join("\n"));
  console.log(`Wrote ${jsonPath}`);
  console.log(`Wrote ${mdPath}`);
  console.log(`failures=${failures.length} actionable=${report.counts.actionable}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
