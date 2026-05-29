#!/usr/bin/env node
/**
 * Phase 5F — Narrative diversity + neutral-language validation for OAS company cards.
 *   node scripts/validate-operator-alignment-narrative-diversity.mjs [dealId]
 */
import "dotenv/config";
import { fetchDealScoringContext } from "../api/my-deals.js";
import {
  loadActiveOperatorCandidatesForAlignment,
  buildCompanyAlignmentResult,
} from "../lib/operator-alignment-company-utils.js";
import { buildPrefillObjectFromNewBaseRows, loadBrandNameByIdMap } from "../api/lib/operator-setup-new-base-read.js";
import {
  BANNED_SUBSTRINGS,
  BANNED_SUPPORT_PHRASES,
} from "../lib/operator-alignment-company-narratives.js";
import { buildOperatorAlignmentExecutiveSummary } from "../lib/operator-alignment-executive-summary.js";
import { mergeDealFieldsForAlignment, buildDealContextFromMerged } from "../lib/operator-alignment-profile-utils.js";

const DEAL_ID = process.argv.find((a) => a.startsWith("rec")) || "recIeGRZP21udmTnt";

const INTERNAL_FACTOR_KEYS = [
  "geographyMarkets",
  "chainScale",
  "assetProjectStageFit",
  "dealStructureAssignment",
  "serviceOfferings",
  "systemsReporting",
  "feeCommercial",
  "brandPortfolioRelevance",
  "missingDataClass",
  "includedInDenominator",
  "fieldSource",
];

const FORBIDDEN_ANYWHERE = [
  "validate open alignment factors before external sharing",
  "current signals appear to be concentrated around market overlap, chain-scale overlap",
  "brand agreement is franchise; this is evaluated separately",
];

const FORBIDDEN_IN_SUPPORTS = [
  ...BANNED_SUPPORT_PHRASES,
  "fee / commercial assumptions may need validation",
  "brand or portfolio relevance may need confirmation",
];

function uniqueCount(arr) {
  return new Set((arr || []).map((x) => String(x || "").trim()).filter(Boolean)).size;
}

function firstSentence(text) {
  const s = String(text || "").trim();
  const m = s.match(/^[^.!?]+[.!?]?/);
  return (m ? m[0] : s).trim();
}

function collectText(company) {
  const parts = [
    company.ownerFacingRationale,
    company.keyConsideration,
    company.reviewStatusLabel,
    ...(company.whatSupportsReview || []),
    ...(company.whatNeedsValidation || []),
    ...(company.whatCouldWeakenAlignment || []),
    ...(company.ownerQuestions || []),
    ...(company.alignmentSignals || []),
    ...(company.reviewConsiderations || []),
  ];
  return parts.filter(Boolean).join("\n");
}

function checkBanned(text) {
  const hits = [];
  const lower = text.toLowerCase();
  for (const bad of BANNED_SUBSTRINGS) {
    if (lower.includes(bad)) hits.push(bad);
  }
  return hits;
}

function checkInternalKeys(text) {
  return INTERNAL_FACTOR_KEYS.filter((k) => text.includes(k));
}

async function main() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("Airtable not configured");

  const ctx = await fetchDealScoringContext(baseId, apiKey, DEAL_ID);
  if (!ctx) throw new Error("Deal not found: " + DEAL_ID);

  const { candidates } = await loadActiveOperatorCandidatesForAlignment();
  const brandNameById = await loadBrandNameByIdMap().catch(() => new Map());

  const companies = [];
  for (const c of candidates) {
    const row = buildCompanyAlignmentResult(
      c,
      ctx.dealFields,
      ctx.locationData,
      ctx.mpData,
      ctx.siData,
      brandNameById
    );
    if (row.alignmentBand === "Insufficient Data") continue;
    companies.push(row);
  }

  companies.sort((a, b) => (b._sortScore || 0) - (a._sortScore || 0));
  const top5 = companies.slice(0, 5);
  const top8 = companies.slice(0, 8);

  const errors = [];
  const warnings = [];

  const rationales = top5.map((c) => c.ownerFacingRationale).filter(Boolean);
  const firstSentences = top5.map((c) => firstSentence(c.ownerFacingRationale)).filter(Boolean);

  if (uniqueCount(rationales) < Math.min(5, rationales.length)) {
    errors.push("Top 5 owner-facing rationale paragraphs are not all unique.");
  }
  if (uniqueCount(firstSentences) < Math.min(5, firstSentences.length)) {
    errors.push("Top 5 rationale first sentences are not all distinct.");
  }

  const keyCons = top8.map((c) => c.keyConsideration).filter(Boolean);
  if (keyCons.length >= 4 && uniqueCount(keyCons) < 4) {
    errors.push("Key consideration values need >= 4 distinct values across top 8 (got " + uniqueCount(keyCons) + ").");
  }

  const supportsSets = top5.map((c) => (c.whatSupportsReview || []).join("|"));
  if (uniqueCount(supportsSets) < 3) {
    errors.push("WHAT SUPPORTS REVIEW sections do not differ across at least 3 of top 5 operators.");
  }

  const validationSets = top5.map((c) => (c.whatNeedsValidation || []).join("|"));
  if (uniqueCount(validationSets) < 3) {
    errors.push("WHAT NEEDS VALIDATION sections do not differ across at least 3 of top 5 operators.");
  }

  const supportBulletCounts = {};
  for (const c of top5) {
    for (const bullet of c.whatSupportsReview || []) {
      const key = String(bullet).trim().toLowerCase();
      if (!key) continue;
      supportBulletCounts[key] = (supportBulletCounts[key] || 0) + 1;
    }
  }
  for (const [bullet, count] of Object.entries(supportBulletCounts)) {
    if (count > top5.length) {
      errors.push(
        `Support bullet appears on ${count} of top 5 cards: "${bullet.slice(0, 72)}…"`
      );
    } else if (count === top5.length && count > 4) {
      warnings.push(
        `Universal support bullet on all top 5 (acceptable for shared deal geography): "${bullet.slice(0, 56)}…"`
      );
    } else if (count > 4) {
      errors.push(
        `Support bullet appears on ${count} of top 5 cards (max 4): "${bullet.slice(0, 72)}…"`
      );
    }
  }

  const ranked = companies.filter((c) => c.sourceStatus === "live" && c.alignmentBand !== "Insufficient Data");
  const exec = buildOperatorAlignmentExecutiveSummary({
    dealContext: buildDealContextFromMerged(
      mergeDealFieldsForAlignment(ctx.dealFields, ctx.locationData, ctx.siData, ctx.mpData)
    ),
    dealFields: ctx.dealFields,
    locationData: ctx.locationData,
    mpData: ctx.mpData,
    siData: ctx.siData,
    companiesAvailable: ranked.length >= 3,
    companiesForConsideration: ranked,
    tableShownLimit: 8,
    activeOperatorRecords: candidates.length,
  });
  const paras = exec.operatorAlignmentSummaryParagraphs || [];
  const summaryText = paras.join(" ").toLowerCase();

  if (ranked.length >= 3) {
    if (!/review set includes \d+ operating/i.test(paras[1] || "")) {
      errors.push("Executive summary missing review set count (paragraph 2).");
    }
    if (!/alignment pattern is|concentrated among|strong alignment signals/i.test(summaryText)) {
      errors.push("Executive summary missing tier distribution / alignment pattern.");
    }
    const topName = ranked[0]?.operatorName || "";
    if (topName && !summaryText.includes(topName.toLowerCase())) {
      errors.push("Executive summary missing a top company name.");
    }
  }
  if (!/validation before controlled operator outreach|require validation/i.test(summaryText)) {
    errors.push("Executive summary missing validation themes.");
  }
  if (!/internal screening|does not determine final operator selection/i.test(summaryText)) {
    errors.push("Executive summary missing internal-use disclaimer.");
  }
  if (exec.wordCount > 340) {
    warnings.push("Executive summary word count " + exec.wordCount + " exceeds ~325 target.");
  }
  const summaryBanned = [
    "dealality recommends",
    "recommended operator",
    "best operator",
    "preferred operator",
    "should select",
    "strongest path",
    "top operators",
  ];
  for (const bad of summaryBanned) {
    if (summaryText.includes(bad)) errors.push("Executive summary contains banned phrase: " + bad);
  }

  console.log("\nExecutive summary (" + paras.length + " paragraphs, ~" + exec.wordCount + " words):");
  paras.forEach((p, i) => console.log("  [" + (i + 1) + "] " + p.slice(0, 120) + (p.length > 120 ? "…" : "")));

  for (const c of top8) {
    const text = collectText(c);
    const lower = text.toLowerCase();

    for (const forbidden of FORBIDDEN_ANYWHERE) {
      if (lower.includes(forbidden)) {
        errors.push(`${c.operatorName}: forbidden phrase "${forbidden}"`);
      }
    }

    for (const bullet of c.whatSupportsReview || []) {
      const bl = String(bullet).toLowerCase();
      for (const forbidden of FORBIDDEN_IN_SUPPORTS) {
        if (bl.includes(forbidden)) {
          errors.push(`${c.operatorName}: forbidden support bullet (${forbidden})`);
        }
      }
      if ((c.whatSupportsReview || []).length > 4) {
        errors.push(`${c.operatorName}: more than 4 support bullets (${c.whatSupportsReview.length})`);
      }
    }

    if ((c.whatNeedsValidation || []).length > 4) {
      errors.push(`${c.operatorName}: more than 4 validation bullets`);
    }

    const banned = checkBanned(text);
    if (banned.length) errors.push(`${c.operatorName}: banned language (${banned.join(", ")})`);
    const internal = checkInternalKeys(text);
    if (internal.length) errors.push(`${c.operatorName}: raw internal scoring keys (${internal.join(", ")})`);
  }

  const scores = companies.map((c) => c.alignmentScoreOptional).filter((s) => s != null);
  const avg = scores.reduce((a, b) => a + b, 0) / Math.max(1, scores.length);

  console.log("Deal:", DEAL_ID);
  console.log("Scored companies:", companies.length);
  console.log("Avg score:", Math.round(avg * 10) / 10);
  console.log("Distinct rationales (top 5):", uniqueCount(rationales), "/", rationales.length);
  console.log("Distinct first sentences (top 5):", uniqueCount(firstSentences), "/", firstSentences.length);
  console.log("Distinct key considerations (top 8):", uniqueCount(keyCons), "/", keyCons.length);
  console.log("Distinct supports bundles (top 5):", uniqueCount(supportsSets));
  console.log("Distinct validation bundles (top 5):", uniqueCount(validationSets));

  console.log("\nTop 5 key considerations:");
  top5.forEach((c) => console.log(" -", c.operatorName, "→", c.keyConsideration));

  if (warnings.length) {
    console.log("\nWarnings:");
    warnings.forEach((w) => console.log(" -", w));
  }

  if (errors.length) {
    console.error("\nFAILED:");
    errors.forEach((e) => console.error(" -", e));
    process.exit(1);
  }

  console.log("\nPASS — narrative prioritization and diversity checks OK.");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
