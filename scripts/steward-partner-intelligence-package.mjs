#!/usr/bin/env node
/**
 * Generalized Partner Intelligence package stewardship assistant.
 * Dry-run by default; writes only with --apply --approve-stewardship and explicit ID lists.
 *
 * Usage:
 *   npm run steward-partner-intelligence -- --entity-type brand --target-rec-id rec... --dry-run
 *   npm run steward-partner-intelligence -- --entity-type operator --target-rec-id rec... --recompute
 *   npm run steward-partner-intelligence -- --apply --approve-stewardship --entity-type brand --target-rec-id rec... \
 *     --approve-source-ids "rec...,rec..." --approve-fact-ids "rec...,rec..."
 *
 * Does NOT write Setup profile governance, Company Validated, or publish profile governance.
 */
import "../load-env.js";
import { mkdirSync, readFileSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import {
  MAP_PARTNER_SOURCE,
  MAP_PARTNER_FACT,
  PARTNER_INTELLIGENCE_TABLES,
  PARTNER_INTELLIGENCE_LINKS,
} from "../api/lib/partner-intelligence-field-map.js";
import {
  getPartnerSourceById,
  patchPartnerSource,
  listPartnerSources,
} from "../lib/partner-intelligence/airtable-source.js";
import { listPartnerFacts, patchPartnerFact } from "../lib/partner-intelligence/airtable-facts.js";
import { assessPackageReadiness } from "../lib/partner-intelligence/profile-governance-publish-readiness.js";
import {
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  NEVER_UPDATE,
  parseIdList,
  sourceSnapshot,
  factSnapshot,
  sourceBlockers,
  recommendSourceUpdates,
  buildSafeSourcePatch,
  buildSafeFactPatch,
  simulateSourceAfterPatch,
  simulateFactsApproved,
  recommendGovernanceFacts,
  listAdditionalFacts,
  findPackageInReadinessReport,
  buildPackageFromRecords,
  collectPackageBlockerLabels,
  buildApplyCommandPreview,
  buildPublishDryRunPreview,
  factStatusCounts,
  RECOMMENDATION_EXCLUSION_NOTE,
} from "../lib/partner-intelligence/stewardship-package.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", REPORT_JSON_NAME);
const REPORT_MD = join(ROOT, "reports", REPORT_MD_NAME);
const READINESS_JSON = join(ROOT, "reports", "partner-intelligence-publish-readiness.json");

const APPLY = process.argv.includes("--apply");
const DRY_RUN = process.argv.includes("--dry-run") || !APPLY;
const APPROVE_STEWARDSHIP = process.argv.includes("--approve-stewardship");
const RECOMPUTE = process.argv.includes("--recompute");
const INCLUDE_DUPLICATES = process.argv.includes("--include-duplicates");

function argValue(flag) {
  const idx = process.argv.indexOf(flag);
  if (idx === -1) return "";
  return String(process.argv[idx + 1] || "").trim();
}

const ENTITY_TYPE = argValue("--entity-type");
const TARGET_REC_ID = argValue("--target-rec-id");
const FACT_LIMIT = Math.max(1, Number(argValue("--fact-limit") || "12") || 12);
const APPROVE_SOURCE_IDS = parseIdList(argValue("--approve-source-ids"));
const APPROVE_FACT_IDS = parseIdList(argValue("--approve-fact-ids"));

function validateCli() {
  if (!["brand", "operator"].includes(ENTITY_TYPE)) {
    console.error("--entity-type brand|operator is required.");
    process.exit(1);
  }
  if (!/^rec[a-zA-Z0-9]+$/.test(TARGET_REC_ID)) {
    console.error("--target-rec-id rec... is required.");
    process.exit(1);
  }
  if (APPLY && !APPROVE_STEWARDSHIP) {
    console.error("Apply requires both --apply and --approve-stewardship. No writes performed.");
    process.exit(1);
  }
}

async function fetchAllPartnerFacts(filter) {
  const all = [];
  let offset = null;
  do {
    const page = await listPartnerFacts({ ...filter, limit: 100, offset });
    all.push(...(page.facts || []));
    offset = page.offset;
  } while (offset);
  return all;
}

async function fetchAllPartnerSources(filter) {
  const all = [];
  let offset = null;
  do {
    const page = await listPartnerSources({ ...filter, limit: 100, offset });
    all.push(...(page.sources || []));
    offset = page.offset;
  } while (offset);
  return all;
}

async function fetchTargetProfile(entityType, targetRecId) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  const table =
    entityType === "brand"
      ? PARTNER_INTELLIGENCE_LINKS.brandBasics
      : process.env.AIRTABLE_OPERATOR_SETUP_MASTER_TABLE || PARTNER_INTELLIGENCE_LINKS.operatorMaster;
  const base = new Airtable({ apiKey }).base(baseId);
  try {
    const rec = await base(table).find(targetRecId);
    const fields = rec.fields || {};
    const name =
      entityType === "brand"
        ? String(fields["Brand Name"] || fields.brand_name || "").trim()
        : String(
            fields.company_name || fields["Company Name"] || fields["Operator Name"] || ""
          ).trim();
    return { id: rec.id, entityType, name: name || null, fields };
  } catch {
    return null;
  }
}

function loadReadinessReport() {
  try {
    return JSON.parse(readFileSync(READINESS_JSON, "utf8"));
  } catch {
    return null;
  }
}

function buildMarkdown(report) {
  const lines = [
    "# Partner Intelligence Stewardship Package",
    "",
    `Generated: ${report.generatedAt}`,
    `Mode: **${report.mode}**`,
    `Entity: ${report.entityType} — **${report.packageName || "(unknown)"}**`,
    `Target record: \`${report.targetRecId}\``,
    "",
    "## Summary",
    "",
    `- Sources in package: ${report.sources.length}`,
    `- Facts in package: ${report.factStatus.total}`,
    `  - Approved: ${report.factStatus.approved}`,
    `  - Pending candidates: ${report.factStatus.pendingCandidates}`,
    `  - Rejected / excluded: ${report.factStatus.excluded}`,
    `  - Recommended for review: ${report.recommendedFactCount}`,
    `- Current eligible: **${report.currentEligibility.eligible}**`,
    `- Projected eligible (explicit source IDs): **${report.projectedAfterSources.eligible}**`,
    `- Projected eligible (sources + recommended facts): **${report.projectedAfterSourcesAndFacts.eligible}**`,
    "",
  ];

  if (report.readinessReportUsed) {
    lines.push(`- Readiness report: \`${report.readinessReportUsed}\` (${report.readinessReportGeneratedAt || "unknown"})`);
    lines.push("");
  }

  if (report.factStatus.excluded > 0) {
    lines.push("## Fact Exclusion Note", "");
    lines.push(report.recommendationExclusionNote);
    lines.push("");
    lines.push(
      `${report.factStatus.excluded} fact(s) excluded from recommendations (rejected status or quarantine notes).`
    );
    if (report.factStatus.excludedFacts?.length) {
      lines.push("");
      for (const f of report.factStatus.excludedFacts.slice(0, 12)) {
        lines.push(
          `- \`${f.id}\` — ${f.fieldName || "field"} — ${f.humanReviewStatus || "—"} (${f.exclusionReason})`
        );
      }
      if (report.factStatus.excludedFacts.length > 12) {
        lines.push(`- … and ${report.factStatus.excludedFacts.length - 12} more (see JSON report)`);
      }
    }
    lines.push("");
  }

  if (report.blockerLabels.length) {
    lines.push("## Blockers", "");
    for (const b of report.blockerLabels) lines.push(`- ${b}`);
    lines.push("");
  }

  if (report.applyResult) {
    lines.push("## Apply Result", "");
    lines.push(`- Sources updated: ${report.applyResult.sourcesUpdated}`);
    lines.push(`- Facts updated: ${report.applyResult.factsUpdated}`);
    lines.push(`- Skipped: ${report.applyResult.skipped.length}`);
    lines.push("");
  }

  lines.push("## Sources", "");
  for (const s of report.sources) {
    lines.push(`### ${s.sourceTitle || s.id} (\`${s.id}\`)`, "");
    lines.push("| Field | Value |");
    lines.push("|-------|-------|");
    lines.push(`| Source Status | ${s.status || "—"} |`);
    lines.push(`| Approved for Explorer Use | ${s.approvedForExplorerUse || "—"} |`);
    lines.push(`| Source Quality | ${s.sourceQuality || "—"} |`);
    lines.push(`| Stale? | ${s.stale ? "Yes" : "No"} |`);
    lines.push(`| Source Type | ${s.sourceType || "—"} |`);
    lines.push(`| Region | ${s.region || "—"} |`);
    if (s.blockers.length) lines.push("", "**Blockers:** " + s.blockers.join("; "));
    if (s.recommendedUpdates.length) {
      lines.push("", "**Recommended updates:**");
      for (const u of s.recommendedUpdates) {
        lines.push(`- \`${u.field}\` → **${u.to}** — ${u.reason}`);
      }
    }
    if (s.applyPlan) {
      lines.push(
        "",
        `**Apply plan:** ${s.applyPlan.wouldApply ? "would patch" : "skip"} — ${(s.applyPlan.applied || []).join("; ") || (s.applyPlan.skipped || []).join("; ")}`
      );
    }
    lines.push("");
  }

  lines.push("## Recommended Governance Facts (3–8)", "");
  lines.push("_Approve only via `--approve-fact-ids` after evidence review._", "");
  for (const f of report.recommendedGovernanceFacts) {
    lines.push(`### \`${f.id}\` — ${f.fieldName || "field"}`, "");
    lines.push(`- Score: ${f.score}`);
    lines.push(`- Review status: ${f.humanReviewStatus || "—"}`);
    lines.push(`- Extraction: ${f.extractionType || "—"} · Confidence: ${f.confidenceLevel || "—"}`);
    if (f.recommendReasons?.length) lines.push(`- Why recommend: ${f.recommendReasons.join("; ")}`);
    if (f.avoidReasons?.length) lines.push(`- Caution: ${f.avoidReasons.join("; ")}`);
    if (f.extractedValuePreview) lines.push(`- Extracted preview: ${f.extractedValuePreview}`);
    lines.push("");
  }

  if (report.additionalFacts?.length) {
    lines.push("## Additional Pending Facts", "");
    for (const f of report.additionalFacts) {
      lines.push(`- \`${f.id}\` — ${f.fieldName || "field"} — score=${f.score}`);
    }
    lines.push("");
  }

  lines.push("## Eligibility Preview", "");
  lines.push("**Current blockers:** " + (report.currentEligibility.blockReasons.join("; ") || "none"));
  lines.push(
    "**After explicit source steward apply:** " + (report.projectedAfterSources.blockReasons.join("; ") || "none")
  );
  lines.push(
    "**After sources + recommended facts:** " +
      (report.projectedAfterSourcesAndFacts.blockReasons.join("; ") || "none")
  );
  lines.push("");

  lines.push("## Never Updated By This Script", "");
  for (const n of report.neverUpdate) lines.push(`- ${n}`);
  lines.push("");

  lines.push("## Suggested Apply Command", "");
  lines.push("```bash", report.suggestedApplyCommand, "```", "");
  lines.push("## Suggested Publish Dry-Run", "");
  lines.push("```bash", report.suggestedPublishDryRun, "```", "");

  if (report.skippedOrProtected?.length) {
    lines.push("## Skipped / Protected", "");
    for (const row of report.skippedOrProtected) {
      lines.push(`- \`${row.id}\` (${row.type}): ${(row.reasons || []).join("; ")}`);
    }
    lines.push("");
  }

  return lines.join("\n");
}

async function main() {
  validateCli();

  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  console.log(
    `[steward-pi-package] mode=${DRY_RUN ? "dry-run" : "apply"} ${ENTITY_TYPE}=${TARGET_REC_ID}`
  );

  const filter = ENTITY_TYPE === "brand" ? { brandId: TARGET_REC_ID } : { operatorId: TARGET_REC_ID };
  const [allSources, allFacts, targetProfile] = await Promise.all([
    fetchAllPartnerSources(filter),
    fetchAllPartnerFacts(filter),
    fetchTargetProfile(ENTITY_TYPE, TARGET_REC_ID),
  ]);

  const readinessReport = RECOMPUTE ? null : loadReadinessReport();
  const readinessEntry = findPackageInReadinessReport(readinessReport, ENTITY_TYPE, TARGET_REC_ID);

  const pkg = buildPackageFromRecords({
    sources: allSources,
    facts: allFacts,
    published: [],
    entityType: ENTITY_TYPE,
    targetRecId: TARGET_REC_ID,
  });

  const packageName =
    targetProfile?.name ||
    readinessEntry?.entityName ||
    pkg.entityKey ||
    TARGET_REC_ID;

  const approvedSourceIdSet = new Set(
    APPROVE_SOURCE_IDS.length
      ? APPROVE_SOURCE_IDS
      : (readinessEntry?.sourceIds || pkg.sources.map((s) => s.id))
  );
  const stewardSourceIds = [...approvedSourceIdSet];

  const sourcePatchOpts = {
    approvedSourceIds: new Set(APPROVE_SOURCE_IDS),
    allowWrites: APPLY && APPROVE_STEWARDSHIP,
    allowQualityBump: APPLY && APPROVE_STEWARDSHIP,
    allowStatusAdvance: APPLY && APPROVE_STEWARDSHIP,
  };

  const sourceReports = pkg.sources.map((source) => {
    const recommendedUpdates = recommendSourceUpdates(source);
    const patchResult = buildSafeSourcePatch(source, ENTITY_TYPE, TARGET_REC_ID, sourcePatchOpts);
    const previewPatch = buildSafeSourcePatch(source, ENTITY_TYPE, TARGET_REC_ID, {
      approvedSourceIds: new Set([source.id]),
      allowWrites: true,
      allowQualityBump: true,
      allowStatusAdvance: true,
    });
    return {
      ...sourceSnapshot(source),
      blockers: sourceBlockers(source),
      recommendedUpdates,
      applyPlan: {
        wouldApply: Boolean(previewPatch.patch),
        applied: previewPatch.applied,
        skipped: previewPatch.skipped,
        preview: previewPatch.patch,
        note: "Apply requires explicit --approve-source-ids",
      },
      _source: source,
      _patch: patchResult,
    };
  });

  const { labels: blockerLabels, blockReasons: currentBlockReasons, assessment: currentAssessment } =
    collectPackageBlockerLabels(pkg, targetProfile);

  const simulatedSourcesExplicit = pkg.sources.map((s) => {
    const shouldSimulate =
      APPROVE_SOURCE_IDS.length > 0
        ? APPROVE_SOURCE_IDS.includes(s.id)
        : true;
    if (!shouldSimulate) return s;
    const patchResult = buildSafeSourcePatch(s, ENTITY_TYPE, TARGET_REC_ID, {
      approvedSourceIds: new Set([s.id]),
      allowWrites: true,
      allowQualityBump: true,
      allowStatusAdvance: true,
    });
    return simulateSourceAfterPatch(s, patchResult.patch);
  });

  const recommendedGovernanceFacts = recommendGovernanceFacts(pkg.facts, ENTITY_TYPE, {
    stewardSourceIds,
    includeDuplicates: INCLUDE_DUPLICATES,
  });
  const recommendedIds = recommendedGovernanceFacts.map((f) => f.id);
  const additionalFacts = listAdditionalFacts(
    pkg.facts,
    recommendedIds,
    FACT_LIMIT,
    ENTITY_TYPE,
    stewardSourceIds
  );

  const projectedAfterSources = assessPackageReadiness(
    { ...pkg, sources: simulatedSourcesExplicit },
    targetProfile
  );

  const simulatedFacts = simulateFactsApproved(pkg.facts, recommendedIds);
  const projectedAfterSourcesAndFacts = assessPackageReadiness(
    { ...pkg, sources: simulatedSourcesExplicit, facts: simulatedFacts },
    targetProfile
  );

  const applyResult = { sourcesUpdated: 0, factsUpdated: 0, skipped: [], changes: [] };
  const skippedOrProtected = [];

  if (APPLY && APPROVE_STEWARDSHIP) {
    if (!APPROVE_SOURCE_IDS.length && !APPROVE_FACT_IDS.length) {
      console.error("Apply requires at least one of --approve-source-ids or --approve-fact-ids.");
      process.exit(1);
    }

    for (const sourceId of APPROVE_SOURCE_IDS) {
      const source = pkg.sources.find((s) => s.id === sourceId) || (await getPartnerSourceById(sourceId));
      if (!source) {
        applyResult.skipped.push({ id: sourceId, type: "source", reasons: ["source_not_found"] });
        continue;
      }
      const patchResult = buildSafeSourcePatch(source, ENTITY_TYPE, TARGET_REC_ID, {
        ...sourcePatchOpts,
        allowWrites: true,
      });
      if (!patchResult.patch) {
        applyResult.skipped.push({ id: sourceId, type: "source", reasons: patchResult.skipped });
        skippedOrProtected.push({ id: sourceId, type: "source", reasons: patchResult.skipped });
        continue;
      }
      try {
        await patchPartnerSource(sourceId, patchResult.patch);
        applyResult.sourcesUpdated += 1;
        applyResult.changes.push({ type: "source", id: sourceId, patch: patchResult.patch, applied: patchResult.applied });
        console.log(`[steward-pi-package] patched source ${sourceId}: ${patchResult.applied.join("; ")}`);
      } catch (err) {
        applyResult.skipped.push({ id: sourceId, type: "source", reasons: [err.message || String(err)] });
      }
      await new Promise((r) => setTimeout(r, 220));
    }

    for (const factId of APPROVE_FACT_IDS) {
      const fact = pkg.facts.find((f) => f.id === factId);
      const patchResult = buildSafeFactPatch(
        fact || { id: factId },
        ENTITY_TYPE,
        TARGET_REC_ID,
        {
          approvedFactIds: new Set(APPROVE_FACT_IDS),
          allowWrites: true,
        }
      );
      if (!fact) {
        applyResult.skipped.push({ id: factId, type: "fact", reasons: ["fact_not_found_in_package"] });
        skippedOrProtected.push({ id: factId, type: "fact", reasons: ["fact_not_found_in_package"] });
        continue;
      }
      if (!patchResult.patch) {
        applyResult.skipped.push({ id: factId, type: "fact", reasons: patchResult.skipped });
        skippedOrProtected.push({ id: factId, type: "fact", reasons: patchResult.skipped });
        continue;
      }
      try {
        await patchPartnerFact(factId, patchResult.patch);
        applyResult.factsUpdated += 1;
        applyResult.changes.push({ type: "fact", id: factId, fields: patchResult.patch });
        console.log(`[steward-pi-package] approved fact ${factId}`);
      } catch (err) {
        applyResult.skipped.push({ id: factId, type: "fact", reasons: [err.message || String(err)] });
      }
      await new Promise((r) => setTimeout(r, 220));
    }
  }

  const suggestedSourceIds =
    APPROVE_SOURCE_IDS.length > 0
      ? APPROVE_SOURCE_IDS
      : pkg.sources.filter((s) => s.approvedForExplorerUse !== "Yes").map((s) => s.id);
  const suggestedFactIds =
    APPROVE_FACT_IDS.length > 0 ? APPROVE_FACT_IDS : recommendedIds.slice(0, 8);

  const report = {
    generatedAt: new Date().toISOString(),
    mode: DRY_RUN ? "dry-run" : "apply",
    entityType: ENTITY_TYPE,
    targetRecId: TARGET_REC_ID,
    packageName,
    readinessReportUsed: readinessReport && !RECOMPUTE ? READINESS_JSON : null,
    readinessReportGeneratedAt: readinessReport?.generatedAt || null,
    recompute: RECOMPUTE,
    factStatus: factStatusCounts(pkg.facts),
    recommendedFactCount: recommendedGovernanceFacts.length,
    recommendationExclusionNote: RECOMMENDATION_EXCLUSION_NOTE,
    blockerLabels,
    sources: sourceReports.map(({ _source, _patch, ...rest }) => rest),
    recommendedGovernanceFacts,
    additionalFacts,
    currentEligibility: {
      eligible: currentAssessment.eligible,
      blockReasons: currentBlockReasons,
      warnings: currentAssessment.warnings,
      changeClass: currentAssessment.changeClass,
      expectedGovernance: currentAssessment.proposal?.expectedGovernance || null,
    },
    projectedAfterSources: {
      eligible: projectedAfterSources.eligible,
      blockReasons: projectedAfterSources.blockReasons,
      simulatedSourceIds: APPROVE_SOURCE_IDS.length ? APPROVE_SOURCE_IDS : suggestedSourceIds,
      note: "Preview assumes explicit source IDs patched; facts unchanged unless noted.",
    },
    projectedAfterSourcesAndFacts: {
      eligible: projectedAfterSourcesAndFacts.eligible,
      blockReasons: projectedAfterSourcesAndFacts.blockReasons,
      simulatedApprovedFactIds: recommendedIds,
      note: "Preview assumes source patches + top recommended facts approved.",
    },
    applyResult: APPLY ? applyResult : null,
    skippedOrProtected,
    neverUpdate: NEVER_UPDATE,
    suggestedApplyCommand: buildApplyCommandPreview({
      entityType: ENTITY_TYPE,
      targetRecId: TARGET_REC_ID,
      approveSourceIds: suggestedSourceIds,
      approveFactIds: suggestedFactIds,
    }),
    suggestedPublishDryRun: buildPublishDryRunPreview({
      entityType: ENTITY_TYPE,
      targetRecId: TARGET_REC_ID,
    }),
  };

  mkdirSync(join(ROOT, "reports"), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, buildMarkdown(report), "utf8");
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${REPORT_MD}`);

  if (APPLY && applyResult.sourcesUpdated + applyResult.factsUpdated > 0) {
    console.log("[steward-pi-package] re-running publish readiness audit…");
    try {
      const { execSync } = await import("child_process");
      execSync("npm run audit-partner-intelligence-publish-readiness", {
        cwd: ROOT,
        stdio: "inherit",
      });
    } catch (err) {
      console.warn("[steward-pi-package] post-apply audit exited non-zero:", err.message || err);
    }
  }
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
