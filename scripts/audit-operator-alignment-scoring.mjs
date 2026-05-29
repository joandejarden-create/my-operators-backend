#!/usr/bin/env node
/**
 * Operator Alignment scoring diagnostics (Phase 5E+).
 *   node scripts/audit-operator-alignment-scoring.mjs [dealId] [--out reports/foo.json]
 */
import "dotenv/config";
import { writeFileSync, readFileSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { scoreOperatorMatchForDeal, fetchDealScoringContext } from "../api/my-deals.js";
import { normalizeOperatorAlignmentDealInputs } from "../lib/operator-alignment-deal-normalize.js";
import {
  loadActiveOperatorCandidatesForAlignment,
  buildCompanyAlignmentResult,
} from "../lib/operator-alignment-company-utils.js";
import {
  buildPrefillObjectFromNewBaseRows,
  loadBrandNameByIdMap,
} from "../api/lib/operator-setup-new-base-read.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

const args = process.argv.slice(2);
const DEAL_ID = args.find((a) => a.startsWith("rec")) || "recIeGRZP21udmTnt";
const outFlag = args.indexOf("--out");
const outPath =
  outFlag >= 0 && args[outFlag + 1]
    ? join(root, args[outFlag + 1].replace(/^reports\//, "reports/"))
    : join(root, "reports", "operator-alignment-scoring-phase5e-" + DEAL_ID + ".json");

function factorBands(breakdown) {
  const strong = [];
  const conditional = [];
  const limited = [];
  const missing = [];
  for (const [k, f] of Object.entries(breakdown || {})) {
    const s = f.score;
    if (s === "—" || s == null) {
      missing.push(k);
      continue;
    }
    const n = Number(s);
    if (n >= 75) strong.push(k);
    else if (n >= 50) conditional.push(k);
    else limited.push(k);
  }
  return { strong, conditional, limited, missing };
}

function topSuppressors(breakdown) {
  const rows = [];
  for (const [k, f] of Object.entries(breakdown || {})) {
    if (f.score === "—" || f.score == null) {
      rows.push({ key: k, label: f.label, weight: f.weight, score: null, drag: f.weight, missingDataClass: f.missingDataClass });
    } else {
      const n = Number(f.score);
      rows.push({
        key: k,
        label: f.label,
        weight: f.weight,
        score: n,
        drag: f.weight * (100 - n),
        fieldSource: f.fieldSource,
        missingDataClass: f.missingDataClass,
        rationale: f.rationale,
      });
    }
  }
  return rows.sort((a, b) => b.drag - a.drag).slice(0, 6);
}

function scoreHistogram(rows) {
  const h = { "80+": 0, "65-79": 0, "50-64": 0, "35-49": 0, "<35": 0, null: 0 };
  for (const r of rows) {
    if (r.score == null) h.null += 1;
    else if (r.score >= 80) h["80+"] += 1;
    else if (r.score >= 65) h["65-79"] += 1;
    else if (r.score >= 50) h["50-64"] += 1;
    else if (r.score >= 35) h["35-49"] += 1;
    else h["<35"] += 1;
  }
  return h;
}

function factorFieldAudit(breakdown) {
  const out = {};
  for (const [k, f] of Object.entries(breakdown || {})) {
    out[k] = {
      score: f.score,
      fieldSource: f.fieldSource || null,
      missingDataClass: f.missingDataClass || null,
      includedInDenominator: f.includedInDenominator,
      rationale: f.rationale || null,
      dealValue: f.dealValue,
    };
  }
  return out;
}

async function main() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("Airtable not configured");

  const ctx = await fetchDealScoringContext(baseId, apiKey, DEAL_ID);
  if (!ctx) throw new Error("Deal not found: " + DEAL_ID);

  const normalized = normalizeOperatorAlignmentDealInputs(
    ctx.dealFields,
    ctx.locationData,
    ctx.mpData,
    ctx.siData
  );

  const { candidates } = await loadActiveOperatorCandidatesForAlignment();
  const brandNameById = await loadBrandNameByIdMap().catch(() => new Map());

  const beforePath = join(
    root,
    "reports/operator-alignment-scoring-after-backfill-" + DEAL_ID + ".json"
  );
  let beforeAvg = null;
  let beforeRange = null;
  if (existsSync(beforePath)) {
    try {
      const b = JSON.parse(readFileSync(beforePath, "utf8"));
      beforeAvg = b.avgScore;
      const top = b.top10 || [];
      if (top.length) {
        const scores = top.map((t) => t.score).filter((s) => s != null);
        beforeRange = { min: Math.min(...scores), max: Math.max(...scores) };
      }
    } catch {
      /* ignore */
    }
  }

  const rows = [];
  for (const c of candidates) {
    const result = buildCompanyAlignmentResult(
      c,
      ctx.dealFields,
      ctx.locationData,
      ctx.mpData,
      ctx.siData,
      brandNameById
    );
    const prefill = buildPrefillObjectFromNewBaseRows(
      c.master,
      c.profile,
      c.platform,
      c.commercial,
      c.governance
    );
    const { score, breakdownDetails } = scoreOperatorMatchForDeal(
      ctx.dealFields,
      ctx.locationData || {},
      ctx.mpData || {},
      ctx.siData || {},
      prefill,
      brandNameById
    );
    const bands = factorBands(breakdownDetails);
    rows.push({
      company: c.companyName,
      score,
      band: result.alignmentBand,
      structureScore: breakdownDetails.dealStructureAssignment?.score,
      structureSource: breakdownDetails.dealStructureAssignment?.fieldSource,
      structureRationale: breakdownDetails.dealStructureAssignment?.rationale,
      serviceScore: breakdownDetails.serviceOfferings?.score,
      serviceSource: breakdownDetails.serviceOfferings?.fieldSource,
      geoScore: breakdownDetails.geographyMarkets?.score,
      strong: bands.strong,
      conditional: bands.conditional,
      limited: bands.limited,
      missing: bands.missing,
      suppressors: topSuppressors(breakdownDetails),
      alignmentSignals: result.alignmentSignals,
      keyConsideration: result.keyConsideration,
      ownerFacingRationale: result.ownerFacingRationale,
      whatSupportsReview: result.whatSupportsReview,
      whatNeedsValidation: result.whatNeedsValidation,
      factorFieldAudit: factorFieldAudit(breakdownDetails),
    });
  }

  rows.sort((a, b) => (b.score || 0) - (a.score || 0));
  const scored = rows.filter((r) => r.score != null);
  const avgScore = scored.reduce((s, r) => s + r.score, 0) / Math.max(1, scored.length);
  const allScores = scored.map((r) => r.score);
  const afterRange =
    allScores.length > 0 ? { min: Math.min(...allScores), max: Math.max(...allScores) } : null;

  const phase =
    process.env.OAS_AUDIT_PHASE ||
    (outPath.includes("phase5f")
      ? "5F"
      : outPath.includes("phase5c")
        ? "5C"
        : "5E");
  const payload = {
    phase,
    dealId: DEAL_ID,
    generatedAt: new Date().toISOString(),
    dealNormalized: {
      brandAgreementStructure: normalized.brandAgreementStructure,
      operatingModel: normalized.operatingModel,
      preferredManagementStructure: normalized.preferredManagementStructure,
      mustHaveOperatorServices: normalized.mustHaveOperatorServices,
      marketPresenceRequirement: normalized.marketPresenceRequirement,
      fieldSources: normalized.fieldSources,
      legacyDealStructure: normalized.legacyDealStructure,
    },
    comparison: {
      beforeAvgScore: beforeAvg,
      afterAvgScore: Math.round(avgScore * 10) / 10,
      beforeRange,
      afterRange,
    },
    operatorCount: candidates.length,
    scoreDistribution: scoreHistogram(rows),
    top10: rows.slice(0, 10),
    avgScore: Math.round(avgScore * 10) / 10,
    sampleStructureFactor: rows[0]?.factorFieldAudit?.dealStructureAssignment,
    sampleServiceFactor: rows[0]?.factorFieldAudit?.serviceOfferings,
  };

  writeFileSync(outPath, JSON.stringify(payload, null, 2));

  console.log("Deal:", DEAL_ID);
  console.log("Structured:", normalized.brandAgreementStructure, "+", normalized.operatingModel);
  console.log("Before avg:", beforeAvg, "→ After avg:", payload.avgScore);
  console.log("Range:", afterRange);
  console.log("Histogram:", payload.scoreDistribution);
  console.log("\nTop 5:\n");
  for (const r of rows.slice(0, 5)) {
    console.log(
      `${String(r.score).padStart(5)}\t${r.company}\tstruct=${r.structureScore} svc=${r.serviceScore}`
    );
  }
  console.log("\nWrote", outPath);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
