#!/usr/bin/env node
/**
 * Read-only Partner Intelligence publish readiness audit.
 * Reports which PI packages are eligible/blocked for profile-governance publish.
 *
 * Usage:
 *   node scripts/audit-partner-intelligence-publish-readiness.mjs
 *   npm run audit-partner-intelligence-publish-readiness
 *
 * Requires: AIRTABLE_API_KEY, AIRTABLE_BASE_ID
 * Does NOT write records or modify schema.
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import {
  PARTNER_INTELLIGENCE_TABLES,
  PARTNER_INTELLIGENCE_LINKS,
  MAP_PARTNER_HELENA,
} from "../api/lib/partner-intelligence-field-map.js";
import {
  normalizePartnerSourceRecord,
} from "../lib/partner-intelligence/airtable-source.js";
import {
  normalizePartnerFactRecord,
  normalizePublishedFieldRecord,
} from "../lib/partner-intelligence/airtable-facts.js";
import { cellToString, extractLinkedRecordIds } from "../lib/airtable-utils.js";
import {
  buildPublishPackages,
  assessPackageReadiness,
} from "../lib/partner-intelligence/profile-governance-publish-readiness.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", "partner-intelligence-publish-readiness.json");
const REPORT_MD = join(ROOT, "reports", "partner-intelligence-publish-readiness.md");

const BRAND_TABLE = PARTNER_INTELLIGENCE_LINKS.brandBasics;
const OPERATOR_TABLE =
  process.env.AIRTABLE_OPERATOR_SETUP_MASTER_TABLE || PARTNER_INTELLIGENCE_LINKS.operatorMaster;
const BRAND_NAME_FIELD = "Brand Name";
const OPERATOR_NAME_FIELDS = [
  process.env.AIRTABLE_OPERATOR_COMPANY_NAME_FIELD || "company_name",
  "Company Name",
];

async function fetchAllRecords(base, tableName, { view, fields } = {}) {
  const records = [];
  await new Promise((resolve, reject) => {
    const selectOpts = { pageSize: 100 };
    if (view) selectOpts.view = view;
    if (fields?.length) selectOpts.fields = fields;
    base(tableName)
      .select(selectOpts)
      .eachPage(
        (page, next) => {
          records.push(...page);
          next();
        },
        (err) => (err ? reject(err) : resolve())
      );
  });
  return records;
}

function readName(fields, candidates) {
  for (const key of candidates) {
    const v = cellToString(fields[key]);
    if (v) return v;
  }
  return null;
}

function normalizeHelenaRecord(record) {
  const f = record.fields || {};
  return {
    id: record.id,
    profileType: cellToString(f[MAP_PARTNER_HELENA.profileType]),
    brandId: extractLinkedRecordIds(f[MAP_PARTNER_HELENA.brand])[0] || null,
    operatorId: extractLinkedRecordIds(f[MAP_PARTNER_HELENA.operator])[0] || null,
    extractionStatus: cellToString(f[MAP_PARTNER_HELENA.extractionStatus]),
    uploadedToSourceLibrary: cellToString(f[MAP_PARTNER_HELENA.uploadedToSourceLibrary]),
    linkedSourceRecordId: extractLinkedRecordIds(f[MAP_PARTNER_HELENA.linkedSourceRecord])[0] || null,
    notes: cellToString(f[MAP_PARTNER_HELENA.notes]),
  };
}

function buildMarkdown(report) {
  const s = report.summary;
  const lines = [
    "# Partner Intelligence Publish Readiness Audit",
    "",
    `Generated: ${report.generatedAt}`,
    `Mode: **read-only**`,
    `Base: \`${report.baseId}\``,
    "",
    "## Summary",
    "",
    "| Metric | Count |",
    "|--------|-------|",
    `| Source records reviewed | ${s.sourceRecords} |`,
    `| Fact records reviewed | ${s.factRecords} |`,
    `| Published field records reviewed | ${s.publishedRecords} |`,
    `| Helena intake records reviewed | ${s.helenaRecords} |`,
    `| Packages found | ${s.packagesFound} |`,
    `| Eligible packages | ${s.eligiblePackages} |`,
    `| Blocked packages | ${s.blockedPackages} |`,
    `| Missing-link packages | ${s.missingLinkPackages} |`,
    `| Target protected packages | ${s.targetProtectedPackages} |`,
    `| Needs manual review | ${s.needsManualReview} |`,
    "",
  ];

  if (report.tableErrors?.length) {
    lines.push("## Table Read Warnings", "");
    for (const e of report.tableErrors) lines.push(`- ${e}`);
    lines.push("");
  }

    if (report.eligiblePackages.length) {
    lines.push("## Eligible Packages", "");
    for (const p of report.eligiblePackages) {
      lines.push(`### ${p.entityName || p.entityKey} (${p.entityType})`, "");
      lines.push(`- Target record: \`${p.recordId}\``);
      lines.push(
        `- Publish scope: **${p.publishScopeSourceCount ?? "—"}** approved source(s); **${p.excludedSourceCount ?? 0}** excluded from publish scope`
      );
      lines.push(`- Approved publish-scope sources: ${(p.approvedSourceIds || p.sourceIds || []).join(", ") || "—"}`);
      lines.push(`- Approved facts in publish scope: ${p.publishScopeApprovedFactCount ?? p.approvedFactCount}`);
      if (p.excludedSourceCount > 0) {
        lines.push(`- Full linked package still has **${p.fullPackageSourceCount ?? "—"}** sources; excluded sources were not used for eligibility.`);
      }
      lines.push(`- Change class: **${p.changeClass}**`);
      if (p.proposed?.expectedGovernance) {
        lines.push(
          `- Expected chip: ${p.proposed.expectedGovernance.displayLabel ? `\`${p.proposed.expectedGovernance.displayLabel}\`` : "*(none)*"}`
        );
        if (p.proposed.expectedGovernance.displaySubtitle) {
          lines.push(`- Subtitle: \`${p.proposed.expectedGovernance.displaySubtitle}\``);
        }
      }
      if (p.proposed?.proposed) {
        lines.push("", "Proposed governance:", "```json");
        lines.push(JSON.stringify(p.proposed.proposed, null, 2));
        lines.push("```", "");
      }
    }
  } else {
    lines.push("## Eligible Packages", "", "_None._", "");
  }

  if (report.blockedPackages.length) {
    lines.push("## Blocked Packages", "");
    for (const p of report.blockedPackages.slice(0, 40)) {
      lines.push(`### ${p.entityName || p.entityKey}`, "");
      lines.push(`- Entity type: ${p.entityType || "unresolved"}`);
      lines.push(
        `- Publish scope: ${p.publishScopeSourceCount ?? "—"} approved source(s); ${p.excludedSourceCount ?? 0} excluded`
      );
      lines.push(`- **Publish-scope blockers:** ${(p.publishScopeBlockers || p.blockReasons || []).join("; ") || "—"}`);
      if (p.fullPackageWarnings?.length) {
        lines.push(`- Full-package diagnostics (non-blocking): ${p.fullPackageWarnings.slice(0, 5).join("; ")}${p.fullPackageWarnings.length > 5 ? "…" : ""}`);
      }
      if (p.recommendedFix?.length) {
        lines.push("- Recommended fix:");
        for (const fix of p.recommendedFix) lines.push(`  - ${fix}`);
      }
      lines.push("");
    }
    if (report.blockedPackages.length > 40) {
      lines.push(`_…and ${report.blockedPackages.length - 40} more (see JSON)._`, "");
    }
  }

  if (report.targetProtectionBlocks.length) {
    lines.push("## Target Protection Blocks", "");
    lines.push("| Target | Protection reason |", "|--------|-------------------|");
    for (const row of report.targetProtectionBlocks) {
      lines.push(`| ${row.entityName || row.recordId} | ${row.reasons.join("; ")} |`);
    }
    lines.push("");
  }

  if (report.missingLinks.length) {
    lines.push("## Missing Links", "");
    for (const row of report.missingLinks.slice(0, 30)) {
      lines.push(
        `- **${row.recordType}** \`${row.recordId}\` — ${row.candidateHint || "no Brand/Operator link"}`
      );
    }
    lines.push("");
  }

  if (report.conflictsStaleLow.length) {
    lines.push("## Conflicts / Stale / Low Confidence", "");
    for (const row of report.conflictsStaleLow.slice(0, 40)) {
      lines.push(`- ${row.entityKey}: ${row.issue}`);
    }
    lines.push("");
  }

  if (report.topBlockReasons?.length) {
    lines.push("## Top Block Reasons", "");
    for (const [reason, count] of report.topBlockReasons) {
      lines.push(`- \`${reason}\`: ${count}`);
    }
    lines.push("");
  }

  if (report.openQuestions?.length) {
    lines.push("## Open Questions / Needs Verification", "");
    for (const q of report.openQuestions) lines.push(`- ${q}`);
    lines.push("");
  }

  lines.push("## Recommended Next Step", "");
  lines.push(report.recommendedNextStep, "");

  return lines.join("\n");
}

function recommendedFixes(blockReasons) {
  const fixes = [];
  for (const r of blockReasons) {
    if (r === "missing_entity_link") fixes.push("Link Source/Fact to Brand or Operator Setup record.");
    if (r === "no_approved_facts") fixes.push("Approve or edit at least one Extracted Fact linked to an approved Explorer-use source.");
    if (r === "no_approved_explorer_sources") {
      fixes.push("Approve at least one Source Library row for Explorer Use (publish scope).");
    }
    if (r.startsWith("source:approved_for_explorer_use")) {
      fixes.push("Set Source Library Approved for Explorer Use = Yes.");
    }
    if (r.includes("source_quality_low")) fixes.push("Raise source quality to Medium/High or obtain reviewer override.");
    if (r.includes("source_stale")) fixes.push("Refresh or replace stale source.");
    if (r.startsWith("protected:")) fixes.push("Resolve profile protection or publish manually after founder review.");
    if (r.startsWith("conflict:")) fixes.push("Resolve conflicting sources before profile publish.");
  }
  return [...new Set(fixes)];
}

async function main() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const base = new Airtable({ apiKey }).base(baseId);
  const tableErrors = [];
  const openQuestions = [
    "Profile-governance rollup registry keys in Published Explorer Fields — Needs Verification.",
    "Whether Low source quality may be force-approved for profile publish — default blocked.",
    "Helena intake → Company Validated attestation path — manual only; Needs Verification.",
  ];

  async function safeFetch(tableName, label) {
    try {
      return await fetchAllRecords(base, tableName);
    } catch (err) {
      tableErrors.push(`${label} (${tableName}): ${err.message || err}`);
      return [];
    }
  }

  console.log("[audit-pi-publish-readiness] fetching Partner Intelligence tables…");
  const [sourceRaw, factRaw, publishedRaw, helenaRaw] = await Promise.all([
    safeFetch(
      process.env.PARTNER_INTELLIGENCE_SOURCE_TABLE_ID || PARTNER_INTELLIGENCE_TABLES.sourceLibrary,
      "Source Library"
    ),
    safeFetch(
      process.env.PARTNER_INTELLIGENCE_FACTS_TABLE_ID || PARTNER_INTELLIGENCE_TABLES.extractedFacts,
      "Extracted Facts"
    ),
    safeFetch(
      process.env.PARTNER_INTELLIGENCE_PUBLISHED_TABLE_ID ||
        PARTNER_INTELLIGENCE_TABLES.publishedFields,
      "Published Explorer Fields"
    ),
    safeFetch(
      process.env.PARTNER_INTELLIGENCE_HELENA_TABLE_ID || PARTNER_INTELLIGENCE_TABLES.helenaIntake,
      "Helena Outreach Intake"
    ),
  ]);

  const sources = sourceRaw.map(normalizePartnerSourceRecord);
  const facts = factRaw.map(normalizePartnerFactRecord);
  const published = publishedRaw.map(normalizePublishedFieldRecord);
  const helena = helenaRaw.map(normalizeHelenaRecord);

  console.log("[audit-pi-publish-readiness] fetching target profile tables…");
  const [brandRaw, operatorRaw] = await Promise.all([
    safeFetch(BRAND_TABLE, "Brand Basics"),
    safeFetch(OPERATOR_TABLE, "Operator Master"),
  ]);

  const brandById = new Map();
  for (const rec of brandRaw) {
    brandById.set(rec.id, {
      id: rec.id,
      entityType: "brand",
      name: readName(rec.fields, [BRAND_NAME_FIELD, "brand_name"]),
      fields: rec.fields || {},
    });
  }

  const operatorById = new Map();
  for (const rec of operatorRaw) {
    operatorById.set(rec.id, {
      id: rec.id,
      entityType: "operator",
      name: readName(rec.fields, OPERATOR_NAME_FIELDS),
      fields: rec.fields || {},
    });
  }

  const packages = buildPublishPackages({ sources, facts, published });

  const sourceById = new Map(sources.map((s) => [s.id, s]));
  for (const pkg of packages) {
    for (const fact of pkg.facts || []) {
      if (!fact.sourceRecordId) continue;
      const linked = sourceById.get(fact.sourceRecordId);
      if (linked && !(pkg.sources || []).some((s) => s.id === linked.id)) {
        pkg.sources = pkg.sources || [];
        pkg.sources.push(linked);
      }
    }
  }

  const eligiblePackages = [];
  const blockedPackages = [];
  const targetProtectionBlocks = [];
  const missingLinks = [];
  const conflictsStaleLow = [];
  const blockReasonCounts = new Map();

  for (const pkg of packages) {
    if (pkg.linkMethod === "missing_link" || !pkg.recordId) {
      missingLinks.push({
        entityKey: pkg.entityKey,
        recordType: "package",
        recordId: pkg.sources[0]?.id || pkg.facts[0]?.id || pkg.entityKey,
        candidateHint: pkg.sources[0]?.sourceTitle || pkg.facts[0]?.fieldName || null,
      });
    }

    const targetProfile =
      pkg.entityType === "brand"
        ? brandById.get(pkg.recordId)
        : pkg.entityType === "operator"
          ? operatorById.get(pkg.recordId)
          : null;

    const assessment = assessPackageReadiness(pkg, targetProfile);
    for (const r of assessment.publishScopeBlockers || assessment.blockReasons) {
      const key = r.split(":").slice(-1)[0].startsWith("rec") ? r : r.replace(/^[^:]+:/, "").split(":")[0] || r;
      blockReasonCounts.set(key, (blockReasonCounts.get(key) || 0) + 1);
    }

    if (assessment.protection?.blocked && targetProfile) {
      targetProtectionBlocks.push({
        recordId: targetProfile.id,
        entityName: targetProfile.name,
        entityType: pkg.entityType,
        reasons: assessment.protection.reasons,
        currentGovernance: assessment.protection.currentRaw,
      });
    }

    for (const s of pkg.sources || []) {
      if (s.status === "Stale") conflictsStaleLow.push({ entityKey: pkg.entityKey, issue: `stale source ${s.id}` });
      if (s.sourceQuality === "Low") {
        conflictsStaleLow.push({ entityKey: pkg.entityKey, issue: `low source quality ${s.id}` });
      }
    }
    for (const w of assessment.fullPackageWarnings || []) {
      if (w.includes("full_package_conflict")) {
        conflictsStaleLow.push({ entityKey: pkg.entityKey, issue: w });
      }
    }
    for (const r of assessment.publishScopeBlockers || assessment.blockReasons || []) {
      if (r.startsWith("conflict:")) {
        conflictsStaleLow.push({ entityKey: pkg.entityKey, issue: `publish_scope:${r}` });
      }
    }

    const entityName = targetProfile?.name || null;
    const approvedFactCount = (pkg.facts || []).filter((f) =>
      ["Approved", "Edited"].includes(String(f.humanReviewStatus || ""))
    ).length;
    const publishScopeApprovedFactCount = assessment.factsUsedForProposal?.length ?? 0;

    const entry = {
      entityKey: pkg.entityKey,
      entityType: pkg.entityType,
      recordId: pkg.recordId,
      entityName,
      linkMethod: pkg.linkMethod,
      sourceIds: (pkg.sources || []).map((s) => s.id),
      factIds: (pkg.facts || []).map((f) => f.id),
      publishedIds: (pkg.published || []).map((p) => p.id),
      approvedFactCount,
      publishScopeApprovedFactCount,
      fullPackageSourceCount: assessment.fullPackageSourceCount,
      publishScopeSourceCount: assessment.publishScopeSourceCount,
      excludedSourceCount: assessment.excludedSourceCount,
      approvedSourceIds: assessment.approvedSourceIds,
      excludedSourceIds: assessment.excludedSourceIds,
      approvedSources: assessment.approvedSources,
      excludedFromPublishScope: assessment.excludedFromPublishScope,
      nonApprovedSources: assessment.nonApprovedSources,
      factsUsedForProposal: assessment.factsUsedForProposal,
      rejectedFactCount: assessment.rejectedFactCount,
      blockReasons: assessment.blockReasons,
      publishScopeBlockers: assessment.publishScopeBlockers,
      fullPackageBlockers: assessment.fullPackageBlockers,
      fullPackageWarnings: assessment.fullPackageWarnings,
      warnings: assessment.warnings,
      needsManualReview: assessment.needsManualReview,
      changeClass: assessment.changeClass,
      proposed: assessment.proposal,
      scopes: assessment.scopes,
      recommendedFix: recommendedFixes(assessment.publishScopeBlockers || assessment.blockReasons),
    };

    if (assessment.eligible) {
      eligiblePackages.push(entry);
    } else {
      blockedPackages.push(entry);
    }
  }

  for (const source of sources) {
    if (!source.brandId && !source.operatorId) {
      missingLinks.push({
        recordType: "source",
        recordId: source.id,
        candidateHint: `${source.profileType || "?"} — ${source.sourceTitle || "untitled"}`,
      });
    }
  }

  const topBlockReasons = [...blockReasonCounts.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 15);

  const summary = {
    sourceRecords: sources.length,
    factRecords: facts.length,
    publishedRecords: published.length,
    helenaRecords: helena.length,
    packagesFound: packages.length,
    eligiblePackages: eligiblePackages.length,
    blockedPackages: blockedPackages.length,
    missingLinkPackages: packages.filter((p) => p.linkMethod === "missing_link").length,
    targetProtectedPackages: targetProtectionBlocks.length,
    needsManualReview: blockedPackages.filter((p) => p.needsManualReview?.length).length,
  };

  const report = {
    generatedAt: new Date().toISOString(),
    mode: "read-only",
    baseId,
    summary,
    tableErrors,
    eligiblePackages,
    blockedPackages,
    targetProtectionBlocks,
    missingLinks,
    conflictsStaleLow,
    topBlockReasons,
    openQuestions,
    helenaSummary: {
      total: helena.length,
      linkedToSource: helena.filter((h) => h.linkedSourceRecordId).length,
      note: "Helena intake is contextual only for this audit; does not gate eligibility directly.",
    },
    recommendedNextStep:
      "Review eligible packages and blocked reasons in this report. When ready, implement `scripts/publish-partner-intelligence-profile-governance.mjs` (dry-run default) using the same readiness rules — still no automatic Company Validated from public sources.",
  };

  mkdirSync(join(ROOT, "reports"), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, buildMarkdown(report), "utf8");

  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${REPORT_MD}`);
  console.log(
    `[audit-pi-publish-readiness] packages=${summary.packagesFound} eligible=${summary.eligiblePackages} blocked=${summary.blockedPackages}`
  );
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
