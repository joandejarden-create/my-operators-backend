#!/usr/bin/env node
/**
 * Publish eligible Partner Intelligence profile-governance proposals to Setup root tables.
 * Default dry-run; --apply required for writes. Never sets Company Validated.
 *
 * Usage:
 *   npm run publish-partner-intelligence-profile-governance -- --entity-type operator --target-rec-id recF5Z87OAqFgndoq --dry-run
 *   npm run publish-partner-intelligence-profile-governance -- --apply --entity-type operator --target-rec-id recF5Z87OAqFgndoq
 *
 * Requires: AIRTABLE_API_KEY, AIRTABLE_BASE_ID
 * Input: reports/partner-intelligence-publish-readiness.json (run audit first, or pass --recompute)
 */
import "../load-env.js";
import { mkdirSync, readFileSync, writeFileSync, existsSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import {
  PARTNER_INTELLIGENCE_TABLES,
  PARTNER_INTELLIGENCE_LINKS,
} from "../api/lib/partner-intelligence-field-map.js";
import { normalizePartnerSourceRecord } from "../lib/partner-intelligence/airtable-source.js";
import {
  normalizePartnerFactRecord,
  normalizePublishedFieldRecord,
} from "../lib/partner-intelligence/airtable-facts.js";
import { cellToString } from "../lib/airtable-utils.js";
import {
  buildPublishPackages,
  assessPackageReadiness,
} from "../lib/partner-intelligence/profile-governance-publish-readiness.js";
import {
  BRAND_TABLE,
  OPERATOR_TABLE,
  READINESS_REPORT_PATH,
  MAX_READINESS_REPORT_AGE_MS,
  NEVER_PUBLISH_API_KEYS,
  PUBLISH_GOVERNANCE_API_KEYS,
  filterReadinessPackages,
  readinessReportAgeMs,
  buildPublishPlanEntry,
} from "../lib/partner-intelligence/profile-governance-publish.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", "partner-intelligence-profile-governance-publish.json");
const REPORT_MD = join(ROOT, "reports", "partner-intelligence-profile-governance-publish.md");
const READINESS_JSON = join(ROOT, READINESS_REPORT_PATH);

const APPLY = process.argv.includes("--apply");
const DRY_RUN = process.argv.includes("--dry-run") || !APPLY;
const RECOMPUTE = process.argv.includes("--recompute");
const ONLY_ELIGIBLE = !process.argv.includes("--include-blocked");

const BRAND_NAME_FIELD = "Brand Name";
const OPERATOR_NAME_FIELDS = [
  process.env.AIRTABLE_OPERATOR_COMPANY_NAME_FIELD || "company_name",
  "Company Name",
];

function argValue(flag) {
  const idx = process.argv.indexOf(flag);
  if (idx === -1) return "";
  return String(process.argv[idx + 1] || "").trim();
}

function isRecordId(value) {
  return /^rec[a-zA-Z0-9]{10,}$/.test(String(value || "").trim());
}

async function fetchAllRecords(base, tableName) {
  const records = [];
  await new Promise((resolve, reject) => {
    base(tableName)
      .select({ pageSize: 100 })
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

function loadReadinessReport() {
  if (!existsSync(READINESS_JSON)) {
    throw new Error(
      `Missing ${READINESS_REPORT_PATH}. Run: npm run audit-partner-intelligence-publish-readiness`
    );
  }
  return JSON.parse(readFileSync(READINESS_JSON, "utf8"));
}

async function recomputeReadinessReport(base) {
  const [sourceRaw, factRaw, publishedRaw, brandRaw, operatorRaw] = await Promise.all([
    fetchAllRecords(
      base,
      process.env.PARTNER_INTELLIGENCE_SOURCE_TABLE_ID || PARTNER_INTELLIGENCE_TABLES.sourceLibrary
    ),
    fetchAllRecords(
      base,
      process.env.PARTNER_INTELLIGENCE_FACTS_TABLE_ID || PARTNER_INTELLIGENCE_TABLES.extractedFacts
    ),
    fetchAllRecords(
      base,
      process.env.PARTNER_INTELLIGENCE_PUBLISHED_TABLE_ID ||
        PARTNER_INTELLIGENCE_TABLES.publishedFields
    ),
    fetchAllRecords(base, BRAND_TABLE),
    fetchAllRecords(base, OPERATOR_TABLE),
  ]);

  const sources = sourceRaw.map(normalizePartnerSourceRecord);
  const facts = factRaw.map(normalizePartnerFactRecord);
  const published = publishedRaw.map(normalizePublishedFieldRecord);

  const brandById = new Map(
    brandRaw.map((rec) => [
      rec.id,
      {
        id: rec.id,
        entityType: "brand",
        name: readName(rec.fields, [BRAND_NAME_FIELD, "brand_name"]),
        fields: rec.fields || {},
      },
    ])
  );
  const operatorById = new Map(
    operatorRaw.map((rec) => [
      rec.id,
      {
        id: rec.id,
        entityType: "operator",
        name: readName(rec.fields, OPERATOR_NAME_FIELDS),
        fields: rec.fields || {},
      },
    ])
  );

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

  for (const pkg of packages) {
    const targetProfile =
      pkg.entityType === "brand"
        ? brandById.get(pkg.recordId)
        : pkg.entityType === "operator"
          ? operatorById.get(pkg.recordId)
          : null;

    const assessment = assessPackageReadiness(pkg, targetProfile);
    const approvedFactCount = (pkg.facts || []).filter((f) =>
      ["Approved", "Edited"].includes(String(f.humanReviewStatus || ""))
    ).length;

    const entry = {
      entityKey: pkg.entityKey,
      entityType: pkg.entityType,
      recordId: pkg.recordId,
      entityName: targetProfile?.name || null,
      linkMethod: pkg.linkMethod,
      sourceIds: (pkg.sources || []).map((s) => s.id),
      factIds: (pkg.facts || []).map((f) => f.id),
      publishedIds: (pkg.published || []).map((p) => p.id),
      approvedFactCount,
      blockReasons: assessment.blockReasons,
      warnings: assessment.warnings,
      needsManualReview: assessment.needsManualReview,
      changeClass: assessment.changeClass,
      proposed: assessment.proposal,
      recommendedFix: [],
    };

    if (assessment.eligible) eligiblePackages.push(entry);
    else blockedPackages.push(entry);
  }

  return {
    generatedAt: new Date().toISOString(),
    mode: "recomputed",
    baseId: process.env.AIRTABLE_BASE_ID,
    summary: {
      packagesFound: packages.length,
      eligiblePackages: eligiblePackages.length,
      blockedPackages: blockedPackages.length,
    },
    eligiblePackages,
    blockedPackages,
  };
}

function buildMarkdown(report) {
  const lines = [
    "# Partner Intelligence → Profile Governance Publish",
    "",
    `Generated: ${report.generatedAt}`,
    `Mode: **${report.mode}**`,
    `Base: \`${report.baseId}\``,
    `Readiness input: ${report.readinessInput.source} (${report.readinessInput.generatedAt || "n/a"})`,
    "",
    "## Summary",
    "",
    `| Metric | Count |`,
    `|--------|-------|`,
    `| Packages considered | ${report.summary.packagesConsidered} |`,
    `| Eligible in input | ${report.summary.eligibleInInput} |`,
    `| Would publish / published | ${report.summary.wouldPublish} |`,
    `| Skipped | ${report.summary.skipped} |`,
    `| Records modified | ${report.summary.recordsModified} |`,
    "",
    "## Publish mapping",
    "",
    "Writable API keys:",
    ...PUBLISH_GOVERNANCE_API_KEYS.map((k) => `- \`${k}\``),
    "",
    "Never written:",
    ...NEVER_PUBLISH_API_KEYS.map((k) => `- \`${k}\``),
    "",
    "Operator confidence column: **Data Confidence Level** (alias for Confidence Level).",
    "",
  ];

  for (const pkg of report.packages) {
    lines.push(`## ${pkg.entityName || pkg.entityKey} (${pkg.entityType})`, "");
    lines.push(`- Target: \`${pkg.recordId}\` on **${pkg.sourceTable}**`);
    lines.push(`- Write status: **${pkg.write?.status || "n/a"}**${pkg.write?.reason ? ` (${pkg.write.reason})` : ""}`);
    if (pkg.protection?.reasons?.length) {
      lines.push("- Protection checks:");
      for (const r of pkg.protection.reasons) lines.push(`  - ${r}`);
    }
    if (pkg.fieldDiff?.wouldUpdate?.length) {
      lines.push("", "### Field diff", "", "| Field | Live column | From | To |", "|-------|-------------|------|-----|");
      for (const row of pkg.fieldDiff.wouldUpdate) {
        const col =
          row.liveColumn && row.liveColumn !== row.field
            ? `\`${row.field}\` → \`${row.liveColumn}\``
            : `\`${row.field}\``;
        lines.push(
          `| ${col} | ${row.liveColumn || "—"} | ${row.from == null ? "—" : JSON.stringify(row.from)} | ${JSON.stringify(row.to)} |`
        );
      }
    }
    if (pkg.fieldDiff?.skipped?.length) {
      lines.push("", "### Skipped fields", "");
      for (const row of pkg.fieldDiff.skipped) {
        lines.push(`- \`${row.field}\`: ${row.reason}`);
      }
    }
    if (pkg.expectedGovernance) {
      lines.push("", "### Expected Explorer chip", "");
      lines.push(
        `- **displayLabel:** ${pkg.expectedGovernance.displayLabel == null ? "*(none)*" : `\`${pkg.expectedGovernance.displayLabel}\``}`
      );
      lines.push(
        `- **displaySubtitle:** ${pkg.expectedGovernance.displaySubtitle == null ? "*(none)*" : `\`${pkg.expectedGovernance.displaySubtitle}\``}`
      );
    }
    if (pkg.errors.length) {
      lines.push("", "### Errors / skips", "");
      for (const e of pkg.errors) lines.push(`- ${e}`);
    }
    lines.push("");
  }

  if (DRY_RUN) {
    lines.push(
      "## Apply",
      "",
      "Dry-run only. After founder approval:",
      "",
      "```bash",
      "npm run publish-partner-intelligence-profile-governance -- --apply --entity-type operator --target-rec-id recF5Z87OAqFgndoq",
      "```",
      ""
    );
  } else {
    lines.push("## Post-apply", "", "Re-verify Explorer trust chip and re-run readiness audit if needed.", "");
  }

  return lines.join("\n");
}

async function applyPatch(base, table, recordId, patchFields) {
  if (!patchFields || !Object.keys(patchFields).length) {
    return { applied: false, reason: "empty_patch" };
  }
  await base(table).update(recordId, patchFields, { typecast: true });
  return { applied: true, fieldCount: Object.keys(patchFields).length };
}

async function main() {
  const entityTypeFilter = argValue("--entity-type") || null;
  const targetRecId = argValue("--target-rec-id") || null;
  const packageKey = argValue("--package-key") || null;

  if (targetRecId && !isRecordId(targetRecId)) {
    console.error(`Invalid --target-rec-id: ${targetRecId}`);
    process.exit(1);
  }
  if (entityTypeFilter && !["brand", "operator"].includes(entityTypeFilter)) {
    console.error(`Invalid --entity-type: ${entityTypeFilter} (use brand|operator)`);
    process.exit(1);
  }

  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const base = new Airtable({ apiKey }).base(baseId);
  const generatedAt = new Date().toISOString();

  let readinessReport;
  let readinessInput = { source: "file", path: READINESS_REPORT_PATH };

  if (RECOMPUTE) {
    console.log("[pi-profile-governance-publish] recomputing readiness from live Airtable…");
    readinessReport = await recomputeReadinessReport(base);
    readinessInput = { source: "recomputed", generatedAt: readinessReport.generatedAt };
  } else {
    readinessReport = loadReadinessReport();
    readinessInput.generatedAt = readinessReport.generatedAt;
    const ageMs = readinessReportAgeMs(readinessReport);
    if (ageMs > MAX_READINESS_REPORT_AGE_MS) {
      console.warn(
        `[pi-profile-governance-publish] readiness report is ${Math.round(ageMs / 3600000)}h old — consider --recompute or re-run audit`
      );
    }
  }

  const packages = filterReadinessPackages(readinessReport, {
    entityType: entityTypeFilter,
    targetRecId,
    packageKey,
    onlyEligible: ONLY_ELIGIBLE,
  });

  if (!packages.length) {
    console.error("[pi-profile-governance-publish] no packages matched filters.");
    process.exit(1);
  }

  console.log(
    `[pi-profile-governance-publish] mode=${DRY_RUN ? "dry-run" : "apply"} packages=${packages.length}`
  );

  const profileCache = new Map();
  async function loadTargetProfile(recordId, entityType) {
    const key = `${entityType}:${recordId}`;
    if (profileCache.has(key)) return profileCache.get(key);
    const table = entityType === "brand" ? BRAND_TABLE : OPERATOR_TABLE;
    try {
      const rec = await base(table).find(recordId);
      const profile = {
        id: rec.id,
        entityType,
        name:
          entityType === "brand"
            ? readName(rec.fields, [BRAND_NAME_FIELD])
            : readName(rec.fields, OPERATOR_NAME_FIELDS),
        fields: rec.fields || {},
      };
      profileCache.set(key, profile);
      return profile;
    } catch (err) {
      profileCache.set(key, null);
      return null;
    }
  }

  const applyTimestamp = generatedAt.split("T")[0];
  const planEntries = [];

  for (const pkg of packages) {
    const targetProfile = await loadTargetProfile(pkg.recordId, pkg.entityType);
    const entry = buildPublishPlanEntry({
      packageEntry: pkg,
      targetProfile,
      mode: DRY_RUN ? "dry-run" : "apply",
      applyTimestamp: APPLY ? applyTimestamp : null,
    });
    planEntries.push(entry);
  }

  let recordsModified = 0;

  if (APPLY) {
    for (const entry of planEntries) {
      if (entry.write?.status !== "pending_apply" || !entry.recordId) continue;
      try {
        const result = await applyPatch(base, entry.sourceTable, entry.recordId, entry.write.patch);
        entry.write.status = "applied";
        entry.write.airtableResult = `updated ${result.fieldCount} field(s)`;
        recordsModified += 1;
        console.log(
          `[pi-profile-governance-publish] applied ${entry.entityType} ${entry.recordId} (${result.fieldCount} fields)`
        );
      } catch (err) {
        entry.write.status = "failed";
        entry.write.airtableResult = err.message || String(err);
        entry.errors.push(`Airtable update failed: ${entry.write.airtableResult}`);
        console.error(
          `[pi-profile-governance-publish] FAIL ${entry.entityType} ${entry.recordId}:`,
          entry.write.airtableResult
        );
      }
      await new Promise((r) => setTimeout(r, 250));
    }
  }

  const wouldPublish = planEntries.filter((p) =>
    ["dry_run", "pending_apply", "applied"].includes(p.write?.status)
  ).length;
  const skipped = planEntries.filter((p) => p.write?.status === "skipped").length;

  const report = {
    generatedAt,
    mode: DRY_RUN ? "dry-run" : "apply",
    baseId,
    filters: {
      entityType: entityTypeFilter,
      targetRecId,
      packageKey,
      onlyEligible: ONLY_ELIGIBLE,
      recompute: RECOMPUTE,
    },
    readinessInput,
    summary: {
      packagesConsidered: planEntries.length,
      eligibleInInput: packages.length,
      wouldPublish,
      skipped,
      recordsModified,
    },
    publishMapping: {
      writableKeys: PUBLISH_GOVERNANCE_API_KEYS,
      neverWriteKeys: NEVER_PUBLISH_API_KEYS,
      operatorConfidenceColumn: "Data Confidence Level",
    },
    protectionRules: [
      "Company Validated = true",
      "Validation Status = Company Validated",
      "Company Validation Date present",
      "Usage Permission = Do Not Use",
      "External Display Status = Do Not Display",
      "Target Last Reviewed Date newer than PI proposal",
      "Internal Notes HOLD / DO NOT USE / REVIEW markers",
      "Validation or confidence downgrade blocked",
    ],
    packages: planEntries,
  };

  mkdirSync(join(ROOT, "reports"), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, buildMarkdown(report), "utf8");

  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${REPORT_MD}`);

  const hasFailure = planEntries.some((p) => p.write?.status === "failed");
  if (hasFailure) process.exit(1);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
