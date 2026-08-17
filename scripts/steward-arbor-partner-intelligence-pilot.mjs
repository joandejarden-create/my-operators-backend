#!/usr/bin/env node
/**
 * Controlled Arbor Lodging (CALA) Partner Intelligence stewardship pilot.
 * **Pilot-specific example** — prefer `npm run steward-partner-intelligence` for new packages.
 * Prepares PI sources/facts for profile-governance publish readiness — does NOT write Setup governance.
 *
 * Usage:
 *   npm run steward-arbor-pi-pilot
 *   npm run steward-arbor-pi-pilot -- --dry-run
 *   npm run steward-arbor-pi-pilot -- --apply --approve-arbor-stewardship
 *   npm run steward-arbor-pi-pilot -- --apply --approve-arbor-stewardship --approve-fact-ids "rec...,rec..."
 *
 * Requires: AIRTABLE_API_KEY, AIRTABLE_BASE_ID
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import {
  MAP_PARTNER_SOURCE,
  MAP_PARTNER_FACT,
  MAP_PARTNER_PUBLISHED,
  VAL_PARTNER_SOURCE_SELECTS,
  PARTNER_INTELLIGENCE_TABLES,
} from "../api/lib/partner-intelligence-field-map.js";
import {
  getPartnerSourceById,
  patchPartnerSource,
  listPartnerSources,
} from "../lib/partner-intelligence/airtable-source.js";
import {
  listPartnerFacts,
  patchPartnerFact,
  normalizePublishedFieldRecord,
} from "../lib/partner-intelligence/airtable-facts.js";
import {
  assessPackageReadiness,
  assessSourceGate,
  buildPublishPackages,
} from "../lib/partner-intelligence/profile-governance-publish-readiness.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", "arbor-pi-stewardship-pilot.json");
const REPORT_MD = join(ROOT, "reports", "arbor-pi-stewardship-pilot.md");

const ARBOR_OPERATOR_ID = "recF5Z87OAqFgndoq";
const PUBLISHED_ROW_ID = "recyXqZU26sKrPVhZ";
const FOUND_STATUS_SOURCE_ID = "recyY5faXntjMFkZp";

const DEFAULT_STEWARD_SOURCE_IDS = [
  "rec83yK5rIkE7aTWx",
  "recg3p1cVgwVmZ9ot",
  "recgadiUD9cdGmaqY",
  "reckn9Hgz1StOc4t1",
  "recwa89aO43SS9uey",
  "recyY5faXntjMFkZp",
];

const APPLY = process.argv.includes("--apply");
const DRY_RUN = process.argv.includes("--dry-run") || !APPLY;
const APPROVE_STEWARDSHIP = process.argv.includes("--approve-arbor-stewardship");

function argValue(flag) {
  const idx = process.argv.indexOf(flag);
  if (idx === -1) return "";
  return String(process.argv[idx + 1] || "").trim();
}

function parseIdList(flag) {
  const raw = argValue(flag);
  if (!raw) return [];
  return raw
    .split(/[,\s]+/)
    .map((s) => s.trim())
    .filter((s) => /^rec[a-zA-Z0-9]+$/.test(s));
}

const FACT_LIMIT = Math.max(1, Number(argValue("--fact-limit") || "10") || 10);
const STEWARD_SOURCE_IDS =
  parseIdList("--source-ids").length > 0
    ? parseIdList("--source-ids")
    : DEFAULT_STEWARD_SOURCE_IDS;
const APPROVE_FACT_IDS = parseIdList("--approve-fact-ids");

async function fetchAllPartnerFacts(operatorId) {
  const all = [];
  let offset = null;
  do {
    const page = await listPartnerFacts({ operatorId, limit: 100, offset });
    all.push(...(page.facts || []));
    offset = page.offset;
  } while (offset);
  return all;
}

async function fetchPublishedById(recordId) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  const table =
    process.env.PARTNER_INTELLIGENCE_PUBLISHED_TABLE_ID ||
    PARTNER_INTELLIGENCE_TABLES.publishedFields;
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${encodeURIComponent(recordId)}`;
  const res = await fetch(url, {
    headers: { Authorization: `Bearer ${apiKey}` },
  });
  if (res.status === 404) return null;
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || "Failed to load published row");
  return normalizePublishedFieldRecord(json);
}

async function fetchOperatorProfile(operatorId) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  const table =
    process.env.AIRTABLE_OPERATOR_SETUP_MASTER_TABLE || "Operator Setup - Master";
  const base = new Airtable({ apiKey }).base(baseId);
  const rec = await base(table).find(operatorId);
  return { id: rec.id, fields: rec.fields || {} };
}

function sourceSnapshot(source) {
  return {
    id: source.id,
    sourceTitle: source.sourceTitle,
    operatorId: source.operatorId,
    status: source.status,
    approvedForExplorerUse: source.approvedForExplorerUse,
    sourceQuality: source.sourceQuality,
    sourceType: source.sourceType,
    sourceOrigin: source.sourceOrigin,
    region: source.region,
    verifiedSource: source.verifiedSource,
    lastReviewed: source.lastReviewed,
  };
}

function factSnapshot(fact) {
  return {
    id: fact.id,
    fieldName: fact.fieldName,
    humanReviewStatus: fact.humanReviewStatus,
    extractionType: fact.extractionType,
    confidenceLevel: fact.confidenceLevel,
    sourceRecordId: fact.sourceRecordId,
    hasEvidence: Boolean(fact.evidenceText),
    approvedValuePreview: (fact.approvedValue || fact.extractedValue || "").slice(0, 120),
  };
}

function publishedSnapshot(row) {
  if (!row) return null;
  return {
    id: row.id,
    fieldName: row.fieldName,
    publishStatus: row.publishStatus,
    stale: row.stale,
    overallSourceConfidence: row.overallSourceConfidence,
    operatorId: row.operatorId,
    approvedValuePreview: (row.approvedValue || "").slice(0, 120),
  };
}

function sourceBlockers(source) {
  const gate = assessSourceGate(source);
  return gate.failures;
}

function recommendSourceUpdates(source) {
  const rec = [];
  if (source.approvedForExplorerUse !== "Yes") {
    rec.push({ field: MAP_PARTNER_SOURCE.approvedForExplorerUse, to: "Yes", reason: "Required for publish readiness" });
  }
  if (
    source.id === FOUND_STATUS_SOURCE_ID &&
    (source.status === "Found" || source.status === "Captured")
  ) {
    rec.push({
      field: MAP_PARTNER_SOURCE.status,
      to: "Approved",
      reason: "Move from Found/Captured after steward review",
    });
  } else if (source.status === "Found" || source.status === "Captured") {
    rec.push({
      field: MAP_PARTNER_SOURCE.status,
      to: "Approved",
      reason: "Advance status after review (Needs Verification: Approved vs Extracted)",
    });
  }
  const q = source.sourceQuality || "";
  if (!q || q === "Low") {
    rec.push({
      field: MAP_PARTNER_SOURCE.sourceQuality,
      to: "Medium",
      reason: `Raise quality for eligibility (current: ${q || "blank"}) — founder must confirm source supports Medium`,
    });
  }
  return rec;
}

function buildSafeSourcePatch(source, { allowQualityBump }) {
  const patch = {};
  const skipped = [];
  const applied = [];

  if (source.operatorId !== ARBOR_OPERATOR_ID) {
    return { patch: null, skipped: ["not_linked_to_arbor"], applied };
  }
  if (source.status === "Stale") {
    return { patch: null, skipped: ["source_status_stale"], applied };
  }
  if (source.status === "Rejected") {
    return { patch: null, skipped: ["source_status_rejected"], applied };
  }

  if (source.approvedForExplorerUse !== "Yes") {
    patch[MAP_PARTNER_SOURCE.approvedForExplorerUse] = "Yes";
    applied.push("Approved for Explorer Use → Yes");
  }

  if (
    source.id === FOUND_STATUS_SOURCE_ID &&
    (source.status === "Found" || source.status === "Captured")
  ) {
    if (VAL_PARTNER_SOURCE_SELECTS.status.includes("Approved")) {
      patch[MAP_PARTNER_SOURCE.status] = "Approved";
      applied.push("Status → Approved");
    } else {
      skipped.push("unknown_status_option_Approved");
    }
  }

  const q = source.sourceQuality || "";
  if (allowQualityBump && (!q || q === "Low")) {
    if (VAL_PARTNER_SOURCE_SELECTS.sourceQuality.includes("Medium")) {
      patch[MAP_PARTNER_SOURCE.sourceQuality] = "Medium";
      applied.push(`Source Quality → Medium (was ${q || "blank"})`);
    }
  } else if (q === "Low") {
    skipped.push("source_quality_low_requires_explicit_review");
  }

  if (!Object.keys(patch).length) {
    return { patch: null, skipped: skipped.length ? skipped : ["no_changes_needed"], applied };
  }
  return { patch, skipped, applied };
}

function simulateSourceAfterPatch(source, patch) {
  if (!patch) return source;
  return {
    ...source,
    approvedForExplorerUse:
      patch[MAP_PARTNER_SOURCE.approvedForExplorerUse] ?? source.approvedForExplorerUse,
    status: patch[MAP_PARTNER_SOURCE.status] ?? source.status,
    sourceQuality: patch[MAP_PARTNER_SOURCE.sourceQuality] ?? source.sourceQuality,
  };
}

function recommendFactIds(facts, limit) {
  const pending = facts.filter((f) => {
    const st = String(f.humanReviewStatus || "");
    return st !== "Approved" && st !== "Edited";
  });
  const scored = pending.map((f) => {
    let score = 0;
    if (f.evidenceText) score += 2;
    if (f.extractionType === "Directly Stated") score += 2;
    if (f.confidenceLevel === "High") score += 1;
    if (f.sourceRecordId && STEWARD_SOURCE_IDS.includes(f.sourceRecordId)) score += 1;
    return { fact: f, score };
  });
  scored.sort((a, b) => b.score - a.score);
  return scored.slice(0, limit).map((s) => ({
    ...factSnapshot(s.fact),
    recommendForManualReview: true,
    score: s.score,
  }));
}

function buildMarkdown(report) {
  const lines = [
    "# Arbor PI Stewardship Pilot",
    "",
    `Generated: ${report.generatedAt}`,
    `Mode: **${report.mode}**`,
    `Operator: Arbor Lodging (CALA) — \`${report.operatorId}\``,
    "",
    "## Summary",
    "",
    `- Steward source IDs in scope: ${report.stewardSourceIds.length}`,
    `- All Arbor sources found: ${report.allArborSourceCount}`,
    `- Facts linked to Arbor: ${report.factCount}`,
    `- Approved facts (current): ${report.approvedFactCount}`,
    `- Published row: \`${report.publishedRowId}\` ${report.published ? "(found)" : "(not found)"}`,
    `- Current package eligible: **${report.currentEligibility.eligible}**`,
    `- Projected eligible after steward source updates: **${report.projectedEligibility.eligible}**`,
    "",
  ];

  if (report.applyResult) {
    lines.push("## Apply Result", "");
    lines.push(`- Sources updated: ${report.applyResult.sourcesUpdated}`);
    lines.push(`- Facts updated: ${report.applyResult.factsUpdated}`);
    lines.push(`- Skipped: ${report.applyResult.skipped.length}`);
    lines.push("");
  }

  lines.push("## Steward Sources (in scope)", "");
  for (const s of report.sources) {
    lines.push(`### ${s.sourceTitle || s.id} (\`${s.id}\`)`, "");
    lines.push("| Field | Value |");
    lines.push("|-------|-------|");
    lines.push(`| Status | ${s.status || "—"} |`);
    lines.push(`| Approved for Explorer Use | ${s.approvedForExplorerUse || "—"} |`);
    lines.push(`| Source Quality | ${s.sourceQuality || "—"} |`);
    lines.push(`| Source Type | ${s.sourceType || "—"} |`);
    lines.push(`| Region | ${s.region || "—"} |`);
    lines.push(`| Operator link | ${s.operatorId || "—"} |`);
    if (s.blockers.length) {
      lines.push("", "**Blockers:** " + s.blockers.join("; "));
    }
    if (s.recommendedUpdates.length) {
      lines.push("", "**Recommended updates:**");
      for (const u of s.recommendedUpdates) {
        lines.push(`- \`${u.field}\` → **${u.to}** — ${u.reason}`);
      }
    }
    if (s.applyPlan) {
      lines.push("", `**Apply plan:** ${s.applyPlan.wouldApply ? "would patch" : "skip"} — ${(s.applyPlan.applied || []).join("; ") || s.applyPlan.skipped?.join("; ")}`);
    }
    lines.push("");
  }

  if (report.otherArborSources?.length) {
    lines.push("## Other Arbor-Linked Sources (not in steward ID list)", "");
    for (const s of report.otherArborSources) {
      lines.push(`- \`${s.id}\` — ${s.sourceTitle || "untitled"} — status=${s.status || "—"}, explorer=${s.approvedForExplorerUse || "—"}`);
    }
    lines.push("");
  }

  lines.push("## Published Explorer Fields Row", "");
  if (report.published) {
    lines.push("```json");
    lines.push(JSON.stringify(report.published, null, 2));
    lines.push("```");
    lines.push("", report.publishedManualNote || "");
  } else {
    lines.push("_Row not found._");
  }
  lines.push("");

  lines.push("## Recommended Facts for Manual Review", "");
  lines.push(`Top ${report.recommendedFacts.length} pending facts (not auto-approved unless --approve-fact-ids):`, "");
  for (const f of report.recommendedFacts) {
    lines.push(
      `- \`${f.id}\` — ${f.fieldName || "field"} — review=${f.humanReviewStatus || "—"} — score=${f.score}`
    );
  }
  lines.push("");

  lines.push("## Eligibility Preview", "");
  lines.push("**Current blockers:** " + (report.currentEligibility.blockReasons.join("; ") || "none"));
  lines.push("**Projected blockers (after source steward apply):** " + (report.projectedEligibility.blockReasons.join("; ") || "none"));
  lines.push("");

  lines.push("## Never Updated By This Script", "");
  for (const n of report.neverUpdate) lines.push(`- ${n}`);
  lines.push("");

  lines.push("## Next Step", "");
  lines.push(report.nextStep, "");
  return lines.join("\n");
}

async function main() {
  if (APPLY && !APPROVE_STEWARDSHIP) {
    console.error(
      "Apply requires both --apply and --approve-arbor-stewardship. No writes performed."
    );
    process.exit(1);
  }

  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  console.log(
    `[steward-arbor-pi] mode=${DRY_RUN ? "dry-run" : "apply"} operator=${ARBOR_OPERATOR_ID}`
  );

  const stewardSources = [];
  const missingSources = [];
  for (const id of STEWARD_SOURCE_IDS) {
    const src = await getPartnerSourceById(id);
    if (!src) missingSources.push(id);
    else stewardSources.push(src);
  }

  let allArborSources = [];
  try {
    let offset = null;
    do {
      const page = await listPartnerSources({ operatorId: ARBOR_OPERATOR_ID, limit: 100, offset });
      allArborSources.push(...(page.sources || []));
      offset = page.offset;
    } while (offset);
  } catch (err) {
    console.warn("[steward-arbor-pi] listPartnerSources warning:", err.message || err);
  }

  const stewardIdSet = new Set(STEWARD_SOURCE_IDS);
  const otherArborSources = allArborSources
    .filter((s) => !stewardIdSet.has(s.id))
    .map(sourceSnapshot);

  const allFacts = await fetchAllPartnerFacts(ARBOR_OPERATOR_ID);
  const approvedFacts = allFacts.filter((f) =>
    ["Approved", "Edited"].includes(String(f.humanReviewStatus || ""))
  );

  const published = await fetchPublishedById(PUBLISHED_ROW_ID).catch((err) => {
    console.warn("[steward-arbor-pi] published row:", err.message || err);
    return null;
  });

  const operatorProfile = await fetchOperatorProfile(ARBOR_OPERATOR_ID).catch(() => null);

  const sourceReports = stewardSources.map((source) => {
    const recommendedUpdates = recommendSourceUpdates(source);
    const applyPlan = buildSafeSourcePatch(source, { allowQualityBump: APPLY && APPROVE_STEWARDSHIP });
    return {
      ...sourceSnapshot(source),
      blockers: sourceBlockers(source),
      recommendedUpdates,
      applyPlan: DRY_RUN
        ? {
            wouldApply: Boolean(applyPlan.patch),
            applied: applyPlan.applied,
            skipped: applyPlan.skipped,
            preview: applyPlan.patch,
          }
        : null,
      _source: source,
      _patch: applyPlan,
    };
  });

  const pkgSources = allArborSources.length ? allArborSources : stewardSources;
  const packages = buildPublishPackages({
    sources: pkgSources,
    facts: allFacts,
    published: published ? [published] : [],
  });
  const arborPackage =
    packages.find((p) => p.recordId === ARBOR_OPERATOR_ID) ||
    packages.find((p) => p.entityKey === `operator:${ARBOR_OPERATOR_ID}`) ||
    {
      entityKey: `operator:${ARBOR_OPERATOR_ID}`,
      entityType: "operator",
      recordId: ARBOR_OPERATOR_ID,
      sources: pkgSources,
      facts: allFacts,
      published: published ? [published] : [],
    };

  const currentEligibility = assessPackageReadiness(arborPackage, operatorProfile);

  const simulatedSources = pkgSources.map((s) => {
    const steward = sourceReports.find((r) => r.id === s.id);
    if (!steward?._patch?.patch) return s;
    return simulateSourceAfterPatch(s, steward._patch.patch);
  });
  const simulatedPackage = { ...arborPackage, sources: simulatedSources };
  const projectedEligibility = assessPackageReadiness(simulatedPackage, operatorProfile);

  const applyResult = {
    sourcesUpdated: 0,
    factsUpdated: 0,
    skipped: [],
    changes: [],
  };

  if (APPLY && APPROVE_STEWARDSHIP) {
    for (const row of sourceReports) {
      const { patch, skipped, applied } = row._patch;
      if (!patch) {
        applyResult.skipped.push({ id: row.id, reasons: skipped });
        continue;
      }
      try {
        await patchPartnerSource(row.id, patch);
        applyResult.sourcesUpdated += 1;
        applyResult.changes.push({ type: "source", id: row.id, patch, applied });
        console.log(`[steward-arbor-pi] patched source ${row.id}: ${applied.join("; ")}`);
      } catch (err) {
        applyResult.skipped.push({ id: row.id, reasons: [err.message || String(err)] });
      }
      await new Promise((r) => setTimeout(r, 220));
    }

    for (const factId of APPROVE_FACT_IDS) {
      const fact = allFacts.find((f) => f.id === factId);
      if (!fact) {
        applyResult.skipped.push({ id: factId, reasons: ["fact_not_found_or_not_arbor"] });
        continue;
      }
      if (fact.operatorId && fact.operatorId !== ARBOR_OPERATOR_ID) {
        applyResult.skipped.push({ id: factId, reasons: ["fact_not_linked_to_arbor"] });
        continue;
      }
      const st = String(fact.humanReviewStatus || "");
      if (st === "Approved" || st === "Edited") {
        applyResult.skipped.push({ id: factId, reasons: ["already_approved"] });
        continue;
      }
      if (!fact.evidenceText && !fact.approvedValue && !fact.extractedValue) {
        applyResult.skipped.push({ id: factId, reasons: ["missing_evidence_and_value"] });
        continue;
      }
      try {
        const fields = { [MAP_PARTNER_FACT.humanReviewStatus]: "Approved" };
        if (!fact.approvedValue && fact.extractedValue) {
          fields[MAP_PARTNER_FACT.approvedValue] = fact.extractedValue;
        }
        await patchPartnerFact(factId, fields);
        applyResult.factsUpdated += 1;
        applyResult.changes.push({ type: "fact", id: factId, fields });
        console.log(`[steward-arbor-pi] approved fact ${factId}`);
      } catch (err) {
        applyResult.skipped.push({ id: factId, reasons: [err.message || String(err)] });
      }
      await new Promise((r) => setTimeout(r, 220));
    }
  }

  const neverUpdate = [
    "Company Validated",
    "Company Validation Date",
    "Brand/Operator Setup profile governance fields",
    "Profile governance trust labels / External Display Status on Setup",
    "Scoring / snapshot fields",
    "Published Explorer Fields row (report only unless mapping verified)",
  ];

  const report = {
    generatedAt: new Date().toISOString(),
    mode: DRY_RUN ? "dry-run" : "apply",
    operatorId: ARBOR_OPERATOR_ID,
    stewardSourceIds: STEWARD_SOURCE_IDS,
    missingSources,
    allArborSourceCount: allArborSources.length,
    factCount: allFacts.length,
    approvedFactCount: approvedFacts.length,
    publishedRowId: PUBLISHED_ROW_ID,
    published: publishedSnapshot(published),
    publishedManualNote: published
      ? "Verify Publish Status and Stale? manually in Airtable — this script does not auto-patch published rows."
      : "Published row not loaded — verify recyXqZU26sKrPVhZ in Airtable.",
    sources: sourceReports.map(({ _source, _patch, ...rest }) => rest),
    otherArborSources,
    recommendedFacts: recommendFactIds(allFacts, FACT_LIMIT),
    currentEligibility: {
      eligible: currentEligibility.eligible,
      blockReasons: currentEligibility.blockReasons,
      warnings: currentEligibility.warnings,
    },
    projectedEligibility: {
      eligible: projectedEligibility.eligible,
      blockReasons: projectedEligibility.blockReasons,
      note: "Assumes steward source patches applied; facts unchanged unless --approve-fact-ids used.",
    },
    applyResult: APPLY ? applyResult : null,
    neverUpdate,
    nextStep: APPLY
      ? "Run: npm run audit-partner-intelligence-publish-readiness"
      : "Review this report, then apply with: npm run steward-arbor-pi-pilot -- --apply --approve-arbor-stewardship",
  };

  mkdirSync(join(ROOT, "reports"), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, buildMarkdown(report), "utf8");
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${REPORT_MD}`);

  if (missingSources.length) {
    console.warn("[steward-arbor-pi] missing sources:", missingSources.join(", "));
  }

  if (APPLY && applyResult.sourcesUpdated + applyResult.factsUpdated > 0) {
    console.log("[steward-arbor-pi] re-running publish readiness audit…");
    try {
      const { execSync } = await import("child_process");
      execSync("npm run audit-partner-intelligence-publish-readiness", {
        cwd: ROOT,
        stdio: "inherit",
      });
    } catch (err) {
      console.warn("[steward-arbor-pi] post-apply audit exited non-zero:", err.message || err);
    }
  }
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
