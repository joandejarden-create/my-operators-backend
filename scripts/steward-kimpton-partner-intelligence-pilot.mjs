#!/usr/bin/env node
/**
 * Controlled Kimpton Hotels Partner Intelligence stewardship pilot (brand-side).
 * **Pilot-specific example** — prefer `npm run steward-partner-intelligence` for new packages.
 * Prepares PI sources/facts for profile-governance publish readiness — does NOT write Brand Basics governance.
 *
 * Usage:
 *   npm run steward-kimpton-pi-pilot
 *   npm run steward-kimpton-pi-pilot -- --dry-run
 *   npm run steward-kimpton-pi-pilot -- --apply --approve-kimpton-stewardship
 *   npm run steward-kimpton-pi-pilot -- --apply --approve-kimpton-stewardship --approve-fact-ids "rec...,rec..."
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
  VAL_PARTNER_SOURCE_SELECTS,
} from "../api/lib/partner-intelligence-field-map.js";
import {
  getPartnerSourceById,
  patchPartnerSource,
  listPartnerSources,
} from "../lib/partner-intelligence/airtable-source.js";
import { listPartnerFacts, patchPartnerFact } from "../lib/partner-intelligence/airtable-facts.js";
import {
  assessPackageReadiness,
  assessSourceGate,
  buildPublishPackages,
} from "../lib/partner-intelligence/profile-governance-publish-readiness.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", "kimpton-pi-stewardship-pilot.json");
const REPORT_MD = join(ROOT, "reports", "kimpton-pi-stewardship-pilot.md");

const KIMPTON_BRAND_ID = "recCKuXCmGvxHPfb3";
const BRAND_TABLE = "Brand Setup - Brand Basics";

const DEFAULT_STEWARD_SOURCE_IDS = [
  "rec0wes92ieaTT5UU",
  "recEMuArkvbqpkrUU",
  "recjFAVo3M2CL9rBF",
  "recpXnHJUYhBIdCkt",
];

const GOVERNANCE_FIELD_BOOST = [
  { pattern: /companyname|brandname|company.?name|parentcompany|parent/i, boost: 4, reason: "brand/company identity" },
  { pattern: /positioning|overview|tagline|snapshot/i, boost: 3, reason: "brand positioning / overview" },
  { pattern: /region|geograph|market|global|footprint|cala/i, boost: 2, reason: "region/geography relevance" },
  { pattern: /standard|owner|development|conversion|website|portfolio/i, boost: 2, reason: "governance-relevant capability" },
];

const RECOMMEND_FACT_MIN = 3;
const RECOMMEND_FACT_MAX = 8;

const APPLY = process.argv.includes("--apply");
const DRY_RUN = process.argv.includes("--dry-run") || !APPLY;
const APPROVE_STEWARDSHIP = process.argv.includes("--approve-kimpton-stewardship");

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

const FACT_LIMIT = Math.max(1, Number(argValue("--fact-limit") || "12") || 12);
const STEWARD_SOURCE_IDS =
  parseIdList("--source-ids").length > 0
    ? parseIdList("--source-ids")
    : DEFAULT_STEWARD_SOURCE_IDS;
const APPROVE_FACT_IDS = parseIdList("--approve-fact-ids");

async function fetchAllPartnerFacts(brandId) {
  const all = [];
  let offset = null;
  do {
    const page = await listPartnerFacts({ brandId, limit: 100, offset });
    all.push(...(page.facts || []));
    offset = page.offset;
  } while (offset);
  return all;
}

async function fetchBrandProfile(brandId) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  const base = new Airtable({ apiKey }).base(baseId);
  const rec = await base(BRAND_TABLE).find(brandId);
  return { id: rec.id, fields: rec.fields || {} };
}

function sourceSnapshot(source) {
  return {
    id: source.id,
    sourceTitle: source.sourceTitle,
    brandId: source.brandId,
    status: source.status,
    approvedForExplorerUse: source.approvedForExplorerUse,
    sourceQuality: source.sourceQuality,
    sourceType: source.sourceType,
    sourceOrigin: source.sourceOrigin,
    region: source.region,
    verifiedSource: source.verifiedSource,
    lastReviewed: source.lastReviewed,
    staleStatus: source.status === "Stale",
  };
}

function factSnapshot(fact) {
  return {
    id: fact.id,
    fieldName: fact.fieldName,
    explorerSection: fact.explorerSection,
    humanReviewStatus: fact.humanReviewStatus,
    extractionType: fact.extractionType,
    confidenceLevel: fact.confidenceLevel,
    sourceRecordId: fact.sourceRecordId,
    hasEvidence: Boolean(fact.evidenceText),
    dataGap: fact.dataGap || null,
    extractedValuePreview: (fact.extractedValue || "").slice(0, 120),
    approvedValuePreview: (fact.approvedValue || "").slice(0, 120),
  };
}

function sourceBlockers(source) {
  return assessSourceGate(source).failures;
}

function recommendSourceUpdates(source) {
  const rec = [];
  if (source.approvedForExplorerUse !== "Yes") {
    rec.push({
      field: MAP_PARTNER_SOURCE.approvedForExplorerUse,
      to: "Yes",
      reason: "Required for publish readiness",
    });
  }
  if (source.status === "Found" || source.status === "Captured") {
    rec.push({
      field: MAP_PARTNER_SOURCE.status,
      to: "Approved",
      reason: "Advance status only after steward review (not auto-applied unless already reviewed)",
    });
  }
  const q = source.sourceQuality || "";
  if (!q || q === "Low") {
    rec.push({
      field: MAP_PARTNER_SOURCE.sourceQuality,
      to: "Medium",
      reason: `Raise quality if supported (current: ${q || "blank"}) — founder must confirm`,
    });
  }
  return rec;
}

function buildSafeSourcePatch(source, { allowQualityBump }) {
  const patch = {};
  const skipped = [];
  const applied = [];

  if (source.brandId !== KIMPTON_BRAND_ID) {
    return { patch: null, skipped: ["not_linked_to_kimpton"], applied };
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

function scoreGovernanceFact(fact, stewardSourceIds) {
  let score = 0;
  const reasons = [];
  const penalties = [];
  const fn = String(fact.fieldName || "");

  if (fact.evidenceText) {
    score += 2;
    reasons.push("has evidence text");
  } else {
    penalties.push("missing evidence");
    score -= 3;
  }
  if (fact.extractionType === "Directly Stated") {
    score += 2;
    reasons.push("directly stated");
  }
  if (fact.extractionType === "Inferred" || fact.extractionType === "Needs Confirmation") {
    score -= 2;
    penalties.push("inferred / needs confirmation");
  }
  if (fact.confidenceLevel === "High") score += 1;
  if (fact.confidenceLevel === "Low") {
    score -= 2;
    penalties.push("low confidence");
  }
  if (fact.dataGap) {
    score -= 1;
    penalties.push("data gap flagged");
  }
  if (fact.sourceRecordId && stewardSourceIds.includes(fact.sourceRecordId)) {
    score += 1;
    reasons.push("linked to steward source");
  }
  if (!fact.extractedValue && !fact.approvedValue) {
    score -= 4;
    penalties.push("no value");
  }

  for (const { pattern, boost, reason } of GOVERNANCE_FIELD_BOOST) {
    if (pattern.test(fn)) {
      score += boost;
      reasons.push(reason);
      break;
    }
  }

  return { score, reasons, penalties };
}

function recommendGovernanceFacts(facts, stewardSourceIds) {
  const pending = facts.filter((f) => {
    const st = String(f.humanReviewStatus || "");
    return st !== "Approved" && st !== "Edited";
  });

  const scored = pending.map((f) => {
    const { score, reasons, penalties } = scoreGovernanceFact(f, stewardSourceIds);
    return { fact: f, score, reasons, penalties };
  });

  scored.sort((a, b) => b.score - a.score);

  const positive = scored.filter((s) => s.score > 0);
  const pick = positive.slice(0, RECOMMEND_FACT_MAX);
  const governancePicks =
    pick.length >= RECOMMEND_FACT_MIN
      ? pick
      : scored.slice(0, Math.min(RECOMMEND_FACT_MAX, Math.max(RECOMMEND_FACT_MIN, scored.length)));

  return governancePicks.map((s) => ({
    ...factSnapshot(s.fact),
    recommendForManualReview: true,
    score: s.score,
    recommendReasons: s.reasons,
    avoidReasons: s.penalties,
  }));
}

function listAdditionalFacts(facts, recommendedIds, limit) {
  const recSet = new Set(recommendedIds);
  const pending = facts.filter((f) => {
    const st = String(f.humanReviewStatus || "");
    return st !== "Approved" && st !== "Edited" && !recSet.has(f.id);
  });
  return pending.slice(0, limit).map((f) => {
    const { score, reasons, penalties } = scoreGovernanceFact(f, STEWARD_SOURCE_IDS);
    return { ...factSnapshot(f), score, recommendReasons: reasons, avoidReasons: penalties };
  });
}

function simulateFactsApproved(facts, factIds) {
  const idSet = new Set(factIds);
  return facts.map((f) => {
    if (!idSet.has(f.id)) return f;
    return {
      ...f,
      humanReviewStatus: "Approved",
      approvedValue: f.approvedValue || f.extractedValue,
    };
  });
}

function buildMarkdown(report) {
  const lines = [
    "# Kimpton PI Stewardship Pilot",
    "",
    `Generated: ${report.generatedAt}`,
    `Mode: **${report.mode}**`,
    `Brand: Kimpton Hotels — \`${report.brandId}\``,
    "",
    "## Summary",
    "",
    `- Steward source IDs in scope: ${report.stewardSourceIds.length}`,
    `- All Kimpton-linked sources: ${report.allKimptonSourceCount}`,
    `- Facts linked to Kimpton: ${report.factCount}`,
    `- Approved facts (current): ${report.approvedFactCount}`,
    `- Published Explorer Fields rows: ${report.publishedCount}`,
    `- Current package eligible: **${report.currentEligibility.eligible}**`,
    `- Projected eligible (sources only): **${report.projectedAfterSources.eligible}**`,
    `- Projected eligible (sources + top ${report.recommendedGovernanceFacts.length} recommended facts): **${report.projectedAfterSourcesAndFacts.eligible}**`,
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
    lines.push(`| Brand link | ${s.brandId || "—"} |`);
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
        `**Apply plan:** ${s.applyPlan.wouldApply ? "would patch" : "skip"} — ${(s.applyPlan.applied || []).join("; ") || s.applyPlan.skipped?.join("; ")}`
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
    lines.push("## Additional Pending Facts (lower priority)", "");
    for (const f of report.additionalFacts) {
      lines.push(`- \`${f.id}\` — ${f.fieldName || "field"} — score=${f.score}`);
    }
    lines.push("");
  }

  lines.push("## Eligibility Preview", "");
  lines.push("**Current blockers:** " + (report.currentEligibility.blockReasons.join("; ") || "none"));
  lines.push(
    "**After source steward apply:** " + (report.projectedAfterSources.blockReasons.join("; ") || "none")
  );
  lines.push(
    "**After sources + recommended facts:** " +
      (report.projectedAfterSourcesAndFacts.blockReasons.join("; ") || "none")
  );
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
      "Apply requires both --apply and --approve-kimpton-stewardship. No writes performed."
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
    `[steward-kimpton-pi] mode=${DRY_RUN ? "dry-run" : "apply"} brand=${KIMPTON_BRAND_ID}`
  );

  const stewardSources = [];
  const missingSources = [];
  for (const id of STEWARD_SOURCE_IDS) {
    const src = await getPartnerSourceById(id);
    if (!src) missingSources.push(id);
    else stewardSources.push(src);
  }

  let allKimptonSources = [];
  try {
    let offset = null;
    do {
      const page = await listPartnerSources({ brandId: KIMPTON_BRAND_ID, limit: 100, offset });
      allKimptonSources.push(...(page.sources || []));
      offset = page.offset;
    } while (offset);
  } catch (err) {
    console.warn("[steward-kimpton-pi] listPartnerSources warning:", err.message || err);
  }

  const allFacts = await fetchAllPartnerFacts(KIMPTON_BRAND_ID);
  const approvedFacts = allFacts.filter((f) =>
    ["Approved", "Edited"].includes(String(f.humanReviewStatus || ""))
  );

  const brandProfile = await fetchBrandProfile(KIMPTON_BRAND_ID).catch(() => null);

  const sourceReports = stewardSources.map((source) => {
    const recommendedUpdates = recommendSourceUpdates(source);
    const applyPlan = buildSafeSourcePatch(source, {
      allowQualityBump: APPLY && APPROVE_STEWARDSHIP,
    });
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

  const pkgSources = allKimptonSources.length ? allKimptonSources : stewardSources;
  const kimptonPackage =
    buildPublishPackages({ sources: pkgSources, facts: allFacts, published: [] }).find(
      (p) => p.recordId === KIMPTON_BRAND_ID
    ) || {
      entityKey: `brand:${KIMPTON_BRAND_ID}`,
      entityType: "brand",
      recordId: KIMPTON_BRAND_ID,
      sources: pkgSources,
      facts: allFacts,
      published: [],
    };

  const currentEligibility = assessPackageReadiness(kimptonPackage, brandProfile);

  const simulatedSources = pkgSources.map((s) => {
    const steward = sourceReports.find((r) => r.id === s.id);
    if (!steward?._patch?.patch) return s;
    return simulateSourceAfterPatch(s, steward._patch.patch);
  });

  const recommendedGovernanceFacts = recommendGovernanceFacts(allFacts, STEWARD_SOURCE_IDS);
  const recommendedIds = recommendedGovernanceFacts.map((f) => f.id);
  const additionalFacts = listAdditionalFacts(allFacts, recommendedIds, FACT_LIMIT);

  const projectedAfterSources = assessPackageReadiness(
    { ...kimptonPackage, sources: simulatedSources },
    brandProfile
  );

  const simulatedFacts = simulateFactsApproved(allFacts, recommendedIds);
  const projectedAfterSourcesAndFacts = assessPackageReadiness(
    { ...kimptonPackage, sources: simulatedSources, facts: simulatedFacts },
    brandProfile
  );

  const applyResult = { sourcesUpdated: 0, factsUpdated: 0, skipped: [], changes: [] };

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
        console.log(`[steward-kimpton-pi] patched source ${row.id}: ${applied.join("; ")}`);
      } catch (err) {
        applyResult.skipped.push({ id: row.id, reasons: [err.message || String(err)] });
      }
      await new Promise((r) => setTimeout(r, 220));
    }

    for (const factId of APPROVE_FACT_IDS) {
      const fact = allFacts.find((f) => f.id === factId);
      if (!fact) {
        applyResult.skipped.push({ id: factId, reasons: ["fact_not_found_or_not_kimpton"] });
        continue;
      }
      if (fact.brandId && fact.brandId !== KIMPTON_BRAND_ID) {
        applyResult.skipped.push({ id: factId, reasons: ["fact_not_linked_to_kimpton"] });
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
        console.log(`[steward-kimpton-pi] approved fact ${factId}`);
      } catch (err) {
        applyResult.skipped.push({ id: factId, reasons: [err.message || String(err)] });
      }
      await new Promise((r) => setTimeout(r, 220));
    }
  }

  const neverUpdate = [
    "Company Validated",
    "Company Validation Date",
    "Brand Setup - Brand Basics profile governance fields",
    "External Display Status / Show Trust Label on Brand Basics",
    "Profile governance trust labels",
    "Published Explorer Fields",
    "Scoring / snapshot fields",
  ];

  const report = {
    generatedAt: new Date().toISOString(),
    mode: DRY_RUN ? "dry-run" : "apply",
    brandId: KIMPTON_BRAND_ID,
    stewardSourceIds: STEWARD_SOURCE_IDS,
    missingSources,
    allKimptonSourceCount: allKimptonSources.length,
    factCount: allFacts.length,
    approvedFactCount: approvedFacts.length,
    publishedCount: 0,
    sources: sourceReports.map(({ _source, _patch, ...rest }) => rest),
    recommendedGovernanceFacts,
    additionalFacts,
    currentEligibility: {
      eligible: currentEligibility.eligible,
      blockReasons: currentEligibility.blockReasons,
      warnings: currentEligibility.warnings,
    },
    projectedAfterSources: {
      eligible: projectedAfterSources.eligible,
      blockReasons: projectedAfterSources.blockReasons,
      note: "Assumes steward source patches applied; facts unchanged.",
    },
    projectedAfterSourcesAndFacts: {
      eligible: projectedAfterSourcesAndFacts.eligible,
      blockReasons: projectedAfterSourcesAndFacts.blockReasons,
      simulatedApprovedFactIds: recommendedIds,
      note: "Assumes steward source patches + top recommended facts approved (preview only).",
    },
    applyResult: APPLY ? applyResult : null,
    neverUpdate,
    nextStep: APPLY
      ? "Run: npm run audit-partner-intelligence-publish-readiness"
      : "Review this report, then apply sources with: npm run steward-kimpton-pi-pilot -- --apply --approve-kimpton-stewardship --approve-fact-ids \"rec...,rec...\"",
  };

  mkdirSync(join(ROOT, "reports"), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, buildMarkdown(report), "utf8");
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${REPORT_MD}`);

  if (missingSources.length) {
    console.warn("[steward-kimpton-pi] missing sources:", missingSources.join(", "));
  }

  if (APPLY && applyResult.sourcesUpdated + applyResult.factsUpdated > 0) {
    console.log("[steward-kimpton-pi] re-running publish readiness audit…");
    try {
      const { execSync } = await import("child_process");
      execSync("npm run audit-partner-intelligence-publish-readiness", {
        cwd: ROOT,
        stdio: "inherit",
      });
    } catch (err) {
      console.warn("[steward-kimpton-pi] post-apply audit exited non-zero:", err.message || err);
    }
  }
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
