#!/usr/bin/env node
/**
 * Quarantine contaminated Curio PI Extracted Facts (Mexico FDD batch).
 * Dry-run by default. Does NOT approve sources/facts or touch Setup governance.
 *
 * Usage:
 *   npm run quarantine-curio-pi-facts -- --dry-run
 *   npm run quarantine-curio-pi-facts -- --apply --approve-curio-quarantine
 *   npm run quarantine-curio-pi-facts -- --dry-run --include-secondary
 *
 * Requires: AIRTABLE_API_KEY, AIRTABLE_BASE_ID
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import { listPartnerFacts, patchPartnerFact } from "../lib/partner-intelligence/airtable-facts.js";
import { getPartnerSourceById } from "../lib/partner-intelligence/airtable-source.js";
import {
  CURIO_BRAND_ID,
  PRIMARY_CONTAMINATED_SOURCE_ID,
  SECONDARY_REPORT_SOURCE_IDS,
  DEFAULT_QUARANTINE_NOTE,
  assessFactContamination,
  buildQuarantinePatch,
  factContaminationSnapshot,
  isEligibleForQuarantineApply,
  resolveQuarantineReviewStatus,
} from "../lib/partner-intelligence/curio-fact-contamination.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", "curio-pi-contaminated-facts-quarantine.json");
const REPORT_MD = join(ROOT, "reports", "curio-pi-contaminated-facts-quarantine.md");

const APPLY = process.argv.includes("--apply");
const DRY_RUN = process.argv.includes("--dry-run") || !APPLY;
const APPROVE = process.argv.includes("--approve-curio-quarantine");
const INCLUDE_SECONDARY = process.argv.includes("--include-secondary");

function argValue(flag) {
  const idx = process.argv.indexOf(flag);
  if (idx === -1) return "";
  return String(process.argv[idx + 1] || "").trim();
}

const SOURCE_ID = argValue("--source-id") || PRIMARY_CONTAMINATED_SOURCE_ID;
const CUSTOM_REASON = argValue("--reason") || DEFAULT_QUARANTINE_NOTE;

async function fetchCurioFacts() {
  const all = [];
  let offset = null;
  do {
    const page = await listPartnerFacts({ brandId: CURIO_BRAND_ID, limit: 100, offset });
    all.push(...(page.facts || []));
    offset = page.offset;
  } while (offset);
  return all;
}

function filterFactsByScope(facts) {
  const sourceIds = new Set([SOURCE_ID]);
  if (INCLUDE_SECONDARY) {
    sourceIds.add(SECONDARY_REPORT_SOURCE_IDS.pointsGuide);
    sourceIds.add(SECONDARY_REPORT_SOURCE_IDS.usFddBadFootprint);
  }
  return facts.filter((f) => f.brandId === CURIO_BRAND_ID && sourceIds.has(f.sourceRecordId));
}

function buildMarkdown(report) {
  const lines = [
    "# Curio PI Contaminated Facts Quarantine",
    "",
    `Generated: ${report.generatedAt}`,
    `Mode: **${report.mode}**`,
    `Brand: Curio Collection by Hilton — \`${report.brandId}\``,
    `Primary source: \`${report.primarySourceId}\` — ${report.primarySourceTitle || "(title unknown)"}`,
    `Scope source filter: \`${report.scopeSourceId}\``,
    `Include secondary sources: **${report.includeSecondary}**`,
    "",
    "## Summary",
    "",
    `| Metric | Count |`,
    `|--------|-------|`,
    `| Facts in scope | ${report.summary.factsInScope} |`,
    `| Facts from primary source | ${report.summary.factsFromPrimarySource} |`,
    `| Contaminated (quarantine) | ${report.summary.contaminatedQuarantine} |`,
    `| Report-only (secondary/manual) | ${report.summary.contaminatedReportOnly} |`,
    `| Identity facts in scope | ${report.summary.identityFactsInScope} |`,
    `| Identity facts contaminated | ${report.summary.identityFactsContaminated} |`,
    `| Would quarantine / quarantined | ${report.summary.wouldQuarantine} |`,
    `| Primary source facts (no rule match) | ${report.summary.factsFromPrimarySource - report.summary.contaminatedQuarantine} |`,
    "",
    `**Proposed review status:** \`${report.proposedReviewStatus}\``,
    "",
  ];

  if (report.applyResult) {
    lines.push("## Apply Result", "");
    lines.push(`- Patched: ${report.applyResult.patched}`);
    lines.push(`- Skipped: ${report.applyResult.skipped.length}`);
    lines.push("");
  }

  lines.push("## Contaminated Facts (quarantine scope)", "");
  for (const row of report.quarantineRows) {
    lines.push(`### \`${row.id}\` — ${row.fieldName}`, "");
    lines.push(`- Human Review Status: ${row.humanReviewStatus || "—"}`);
    lines.push(`- Reasons: ${row.contaminationReasons.join("; ")}`);
    lines.push(`- Extracted: ${JSON.stringify((row.extractedValue || "").slice(0, 120))}`);
    if (row.approvedValue) lines.push(`- Approved: ${JSON.stringify(row.approvedValue.slice(0, 120))}`);
    lines.push(`- Proposed: ${row.proposedAction}`);
    if (row.applyPlan) {
      lines.push(
        `- Apply plan: ${row.applyPlan.wouldApply ? "would patch" : "skip"} — ${(row.applyPlan.skipped || []).join("; ") || "ok"}`
      );
    }
    lines.push("");
  }

  if (report.reportOnlyRows?.length) {
    lines.push("## Report-Only (manual review)", "");
    for (const row of report.reportOnlyRows) {
      lines.push(
        `- \`${row.id}\` — ${row.fieldName} — ${row.contaminationReasons.join("; ")} — ${JSON.stringify((row.extractedValue || "").slice(0, 80))}`
      );
    }
    lines.push("");
  }

  lines.push("## Never Updated By This Script", "");
  for (const n of report.neverUpdate) lines.push(`- ${n}`);
  lines.push("");

  if (report.applyCommandPreview) {
    lines.push("## Apply Command (founder approval required)", "");
    lines.push("```bash", report.applyCommandPreview, "```", "");
  }

  return lines.join("\n");
}

async function main() {
  if (APPLY && !APPROVE) {
    console.error("Apply requires both --apply and --approve-curio-quarantine.");
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const proposedStatus = resolveQuarantineReviewStatus();
  if (!proposedStatus) {
    console.error("No safe Human Review Status option (Rejected / Needs More Source) in field map.");
    process.exit(1);
  }

  console.log(
    `[quarantine-curio-pi] mode=${DRY_RUN ? "dry-run" : "apply"} brand=${CURIO_BRAND_ID} source=${SOURCE_ID}`
  );

  const primarySource = await getPartnerSourceById(PRIMARY_CONTAMINATED_SOURCE_ID).catch(() => null);
  const allFacts = await fetchCurioFacts();
  const scopedFacts = filterFactsByScope(allFacts);
  const primaryFacts = scopedFacts.filter((f) => f.sourceRecordId === PRIMARY_CONTAMINATED_SOURCE_ID);

  const quarantineRows = [];
  const reportOnlyRows = [];
  let identityInScope = 0;
  let identityContaminated = 0;

  for (const fact of scopedFacts) {
    const assessment = assessFactContamination(fact, {
      includeSecondary: INCLUDE_SECONDARY,
      secondaryQuarantine: INCLUDE_SECONDARY,
    });
    if (String(fact.fieldName || "").startsWith("be.identity.")) {
      identityInScope += 1;
      if (assessment.contaminated) identityContaminated += 1;
    }

    const snap = factContaminationSnapshot(fact, assessment);
    const eligibility = isEligibleForQuarantineApply(fact, assessment, {
      includeSecondary: INCLUDE_SECONDARY,
    });
    const patchResult = buildQuarantinePatch(fact, { reason: CUSTOM_REASON });

    const row = {
      ...snap,
      applyPlan: {
        wouldApply: eligibility.ok && Boolean(patchResult.patch),
        skipped: eligibility.ok ? patchResult.skipped : eligibility.reasons,
        preview: eligibility.ok ? patchResult.patch : null,
      },
      _fact: fact,
      _patch: patchResult,
      _eligibility: eligibility,
    };

    if (assessment.severity === "quarantine" && assessment.contaminated) {
      quarantineRows.push(row);
    } else if (assessment.contaminated) {
      reportOnlyRows.push(snap);
    }
  }

  const applyResult = { patched: 0, skipped: [], changes: [] };

  if (APPLY && APPROVE) {
    for (const row of quarantineRows) {
      if (!row._eligibility.ok) {
        applyResult.skipped.push({ id: row.id, reasons: row._eligibility.reasons });
        continue;
      }
      const { patch, skipped } = row._patch;
      if (!patch || skipped.length) {
        applyResult.skipped.push({ id: row.id, reasons: skipped.length ? skipped : ["no_patch"] });
        continue;
      }
      try {
        await patchPartnerFact(row.id, patch);
        applyResult.patched += 1;
        applyResult.changes.push({ id: row.id, patch });
        console.log(`[quarantine-curio-pi] quarantined fact ${row.id} (${row.fieldName})`);
      } catch (err) {
        applyResult.skipped.push({ id: row.id, reasons: [err.message || String(err)] });
      }
      await new Promise((r) => setTimeout(r, 220));
    }
  }

  const neverUpdate = [
    "Source Library — Approved for Explorer Use",
    "Source Library — Source Quality / Status",
    "Brand Setup - Brand Basics governance fields",
    "Company Validated / Company Validation Date",
    "External Display Status / Show Trust Label",
    "Published Explorer Fields",
    "Non-Curio brand facts",
    "Facts outside scoped source IDs (unless --include-secondary)",
  ];

  const wouldQuarantine = quarantineRows.filter((r) => r.applyPlan.wouldApply).length;

  const report = {
    generatedAt: new Date().toISOString(),
    mode: DRY_RUN ? "dry-run" : "apply",
    brandId: CURIO_BRAND_ID,
    primarySourceId: PRIMARY_CONTAMINATED_SOURCE_ID,
    primarySourceTitle: primarySource?.sourceTitle || null,
    scopeSourceId: SOURCE_ID,
    includeSecondary: INCLUDE_SECONDARY,
    customReason: CUSTOM_REASON,
    proposedReviewStatus: proposedStatus,
    summary: {
      factsInScope: scopedFacts.length,
      factsFromPrimarySource: primaryFacts.length,
      contaminatedQuarantine: quarantineRows.length,
      contaminatedReportOnly: reportOnlyRows.length,
      identityFactsInScope: identityInScope,
      identityFactsContaminated: identityContaminated,
      wouldQuarantine,
    },
    quarantineRows: quarantineRows.map(({ _fact, _patch, _eligibility, ...rest }) => rest),
    reportOnlyRows,
    applyResult: APPLY ? applyResult : null,
    neverUpdate,
    applyCommandPreview: DRY_RUN
      ? `npm run quarantine-curio-pi-facts -- --apply --approve-curio-quarantine${INCLUDE_SECONDARY ? " --include-secondary" : ""}`
      : null,
  };

  mkdirSync(join(ROOT, "reports"), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, buildMarkdown(report), "utf8");
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${REPORT_MD}`);
  console.log(
    `[quarantine-curio-pi] scope=${scopedFacts.length} quarantine=${quarantineRows.length} wouldApply=${wouldQuarantine}`
  );
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
