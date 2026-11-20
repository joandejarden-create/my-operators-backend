#!/usr/bin/env node
/**
 * Portfolio Loyalty Elite Tier completeness audit + remediation ($0 paid).
 *
 * For every Factory frozen candidate:
 *  - Resolve official loyalty program from loyalty.program (or brand/parent)
 *  - Compare CURRENT-style named tiers vs PROPOSED (frozen) elite cards
 *  - Replace generic / collapsed / incomplete elite sets with canonical program tiers
 *  - Never invent tiers when program is unresolved
 *  - Rebuild approval hashes; sync depth candidates + stage.json + HE preview
 *
 * Distinct from Key Benefits exact-six (loyalty.proof).
 * No production Airtable Presentation writeback.
 *
 * Usage:
 *   node scripts/be-factory-loyalty-tier-completeness-audit-remediate-v1.mjs --dry-run
 *   node scripts/be-factory-loyalty-tier-completeness-audit-remediate-v1.mjs --apply
 */
import "../load-env.js";
import {
  existsSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  writeFileSync,
  copyFileSync,
  renameSync,
  unlinkSync,
} from "fs";
import { join } from "path";
import { hashPresentationBlocks } from "../lib/brand-explorer/be-founder-approval-publication-v1.js";
import {
  BE_FACTORY_PERMANENT_LOYALTY_TIER_COMPLETENESS_V1,
  LOYALTY_TIER_FAIL,
  buildLoyaltyEliteBlocksFromProgram,
  getCanonicalLoyaltyProgram,
  isGenericLoyaltyElitePlaceholder,
  resolveLoyaltyProgramId,
  validateLoyaltyEliteTier,
} from "../lib/brand-explorer/be-factory-permanent-loyalty-tier-completeness-v1.js";

const ROOT = process.cwd();
const PREVIEW = join(ROOT, "public/brand-explorer-phase2e-preview");
const STAGING = join(ROOT, "data/brand-explorer/factory-2-0/brands");
const OUT = join(ROOT, "reports/brand-explorer/portfolio-content-quality-audit-v1");
const REGISTRY_JS = join(ROOT, "public/js/brand-explorer-staged-candidate-registry.js");
const HE = "C:\\Dev\\dealality\\ws-hotel-explorer";
const apply = process.argv.includes("--apply");
const dryRun = !apply;

function nz(s) {
  return s == null ? "" : String(s).trim();
}

function writeJsonAtomic(abs, obj) {
  const tmp = `${abs}.tmp-${process.pid}`;
  const body = JSON.stringify(obj, null, 2) + "\n";
  writeFileSync(tmp, body);
  try {
    renameSync(tmp, abs);
  } catch {
    writeFileSync(abs, body);
    try {
      unlinkSync(tmp);
    } catch {
      /* ignore */
    }
  }
}

function eliteBlocks(blocks) {
  return (Array.isArray(blocks) ? blocks : []).filter(
    (b) => nz(b.slotKey || b.slot_key) === "loyalty.elite"
  );
}

function replaceEliteBlocks(blocks, newElite) {
  const others = (blocks || []).filter((b) => nz(b.slotKey || b.slot_key) !== "loyalty.elite");
  // Keep elite near other loyalty slots: insert after loyalty.redeem if present
  const redeemIdx = others.findIndex((b) => nz(b.slotKey) === "loyalty.redeem");
  if (redeemIdx >= 0) {
    return [
      ...others.slice(0, redeemIdx + 1),
      ...newElite,
      ...others.slice(redeemIdx + 1),
    ];
  }
  return [...others, ...newElite];
}

function patchRegistryHash(slug, newHash) {
  if (!existsSync(REGISTRY_JS) || !slug || !newHash) return false;
  let src = readFileSync(REGISTRY_JS, "utf8");
  const esc = slug.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const slugRe = new RegExp(
    `(slug:\\s*'${esc}'[\\s\\S]{0,600}?approvalContentHash:\\s*')([a-f0-9]+)(')`,
    "m"
  );
  if (!slugRe.test(src)) return false;
  src = src.replace(slugRe, `$1${newHash}$3`);
  writeFileSync(REGISTRY_JS, src);
  return true;
}

function classifyRow({ programId, prog, beforeVal, afterVal, beforeTitles, remediated }) {
  const flags = new Set([...(beforeVal.flags || [])]);
  let remediation = "NONE";
  let verificationStatus = "PASS";
  if (!programId) {
    verificationStatus = "NEEDS_RESEARCH";
    flags.add(LOYALTY_TIER_FAIL.PARTICIPATION_UNVERIFIED);
  } else if (prog?.nonTieredTreatment) {
    verificationStatus = afterVal.ok ? "PASS_NON_TIERED" : "FAIL_NON_TIERED";
  } else if (beforeVal.flags?.includes(LOYALTY_TIER_FAIL.COLLAPSED) || beforeTitles.length === 1 && /elite recognition/i.test(beforeTitles[0] || "")) {
    verificationStatus = remediated ? "REMEDIATED_FROM_COLLAPSED" : "FAIL_COLLAPSED";
    remediation = remediated ? "RESTORE_CANONICAL_TIERS" : "PENDING";
  } else if (!beforeVal.ok) {
    verificationStatus = remediated ? "REMEDIATED_INCOMPLETE" : "FAIL_INCOMPLETE";
    remediation = remediated ? "RESTORE_CANONICAL_TIERS" : "PENDING";
  } else if (remediated) {
    verificationStatus = "REMEDIATED";
    remediation = "REFRESH_CANONICAL_TIERS";
  }
  if (afterVal && !afterVal.ok && programId) {
    verificationStatus = "STILL_BLOCKED";
  }
  return { flags: [...flags], remediation, verificationStatus };
}

async function main() {
  mkdirSync(OUT, { recursive: true });
  const files = readdirSync(PREVIEW).filter((f) =>
    f.endsWith("-factory2-frozen-approval-payload-v1.json")
  );

  const rows = [];
  let patched = 0;
  let remediated = 0;
  let needsResearch = 0;
  let completeVerified = 0;
  let collapsed = 0;
  let incomplete = 0;
  let nonTiered = 0;
  let noProgram = 0;
  let withProgram = 0;
  let tiered = 0;

  for (const f of files) {
    const abs = join(PREVIEW, f);
    const j = JSON.parse(readFileSync(abs, "utf8"));
    const blocks = j.brandExplorer?.blocks || [];
    const progBlock = blocks.find((b) => nz(b.slotKey) === "loyalty.program");
    const programId = resolveLoyaltyProgramId({
      loyaltyProgram: progBlock?.title,
      brandName: j.brandName,
      parentCompany: j.parentCompany,
    });
    const prog = getCanonicalLoyaltyProgram(programId);
    const beforeElite = eliteBlocks(blocks);
    const beforeTitles = beforeElite.map((b) => nz(b.title));
    const beforeVal = validateLoyaltyEliteTier(blocks, {
      programId,
      participates: programId !== "none",
    });

    if (programId && programId !== "none") withProgram += 1;
    else noProgram += 1;
    if (prog?.nonTieredTreatment) nonTiered += 1;
    else if (prog?.tiers?.length) tiered += 1;
    if (beforeVal.flags?.includes(LOYALTY_TIER_FAIL.COLLAPSED) || (beforeElite.length === 1 && isGenericLoyaltyElitePlaceholder(beforeElite[0]))) {
      collapsed += 1;
    } else if (!beforeVal.ok && programId) {
      incomplete += 1;
    }

    let nextBlocks = blocks;
    let didRemediate = false;
    const canRemediate =
      programId &&
      prog &&
      (prog.tiers?.length > 0) &&
      (!beforeVal.ok ||
        beforeElite.some(isGenericLoyaltyElitePlaceholder) ||
        (beforeVal.expectedCount > 0 && beforeVal.namedCount < beforeVal.expectedCount));

    if (canRemediate) {
      const sortBase =
        beforeElite.reduce((m, b) => Math.max(m, Number(b.sort) || 0), 3400) || 3400;
      const source = prog.officialUrl || progBlock?.source || null;
      const newElite = buildLoyaltyEliteBlocksFromProgram(programId, {
        sortStart: Math.max(100, sortBase - prog.tiers.length),
        source,
      });
      nextBlocks = replaceEliteBlocks(blocks, newElite);
      didRemediate = true;
    }

    const afterVal = validateLoyaltyEliteTier(nextBlocks, {
      programId,
      participates: programId !== "none",
    });
    const afterTitles = eliteBlocks(nextBlocks).map((b) => nz(b.title));
    const assembledHash = hashPresentationBlocks(nextBlocks);
    const cls = classifyRow({
      programId,
      prog,
      beforeVal,
      afterVal,
      beforeTitles,
      remediated: didRemediate,
    });
    if (!programId) needsResearch += 1;
    if (afterVal.ok) completeVerified += 1;
    if (didRemediate) remediated += 1;

    const row = {
      brandId: j.brandId || null,
      brand: j.brandName,
      slug: j.slug,
      loyaltyProgram: prog?.programName || progBlock?.title || null,
      programId: programId || null,
      officialTierCount: prog?.tiers?.length || 0,
      officialTierNames: (prog?.tiers || []).map((t) => t.name),
      currentTierCount: beforeElite.length,
      currentTierNames: beforeTitles,
      proposedTierCount: afterTitles.length,
      proposedTierNames: afterTitles,
      missingTier: afterVal.missingTierNames || beforeVal.missingTierNames || [],
      beforeOk: beforeVal.ok,
      afterOk: afterVal.ok,
      flags: cls.flags,
      verificationStatus: cls.verificationStatus,
      remediation: cls.remediation,
      qualityGate: afterVal.ok ? "PASS" : "BLOCK",
      assembledHash,
      remediated: didRemediate,
      patched: false,
      sourceUrl: prog?.officialUrl || null,
      verifiedAt: prog?.verifiedAt || null,
    };

    if (!dryRun && didRemediate) {
      j.brandExplorer = j.brandExplorer || {};
      j.brandExplorer.blocks = nextBlocks;
      j.approvalContentHash = assembledHash;
      j.loyaltyTierCompletenessRemediation = {
        contract: BE_FACTORY_PERMANENT_LOYALTY_TIER_COMPLETENESS_V1,
        lesson: "BE-QL-LOYALTY-TIER-COMPLETENESS-001",
        remediatedAt: new Date().toISOString(),
        programId,
        researchSpendUsd: 0,
        beforeTierNames: beforeTitles,
        afterTierNames: afterTitles,
      };
      writeJsonAtomic(abs, j);

      const depthName = f.replace(
        "-factory2-frozen-approval-payload-v1.json",
        "-factory2-depth-candidate.json"
      );
      const depthAbs = join(PREVIEW, depthName);
      if (existsSync(depthAbs)) {
        try {
          const d = JSON.parse(readFileSync(depthAbs, "utf8"));
          if (d.brandExplorer) d.brandExplorer.blocks = nextBlocks;
          else d.blocks = nextBlocks;
          d.approvalContentHash = assembledHash;
          writeJsonAtomic(depthAbs, d);
        } catch (err) {
          console.error(`depth sync skip ${depthName}:`, err.message || err);
        }
      }

      const stagePath = join(STAGING, j.brandId || "", "stage.json");
      if (j.brandId && existsSync(stagePath)) {
        try {
          const stage = JSON.parse(readFileSync(stagePath, "utf8"));
          stage.assembledHash = assembledHash;
          stage.candidateHash = assembledHash;
          stage.loyaltyTierCompletenessAt = new Date().toISOString();
          stage.loyaltyProgramId = programId;
          if (Array.isArray(stage.gaps)) {
            stage.gaps = stage.gaps.filter((g) => !/loyalty_elite|loyalty_tier/i.test(String(g)));
          }
          writeJsonAtomic(stagePath, stage);
        } catch (err) {
          console.error(`stage sync skip ${j.brandId}:`, err.message || err);
        }
      }

      try {
        patchRegistryHash(j.slug, assembledHash);
      } catch (err) {
        console.error(`registry hash skip ${j.slug}:`, err.message || err);
      }

      if (existsSync(join(HE, "public/brand-explorer-phase2e-preview"))) {
        try {
          copyFileSync(abs, join(HE, "public/brand-explorer-phase2e-preview", f));
        } catch {
          /* ignore */
        }
      }

      row.patched = true;
      patched += 1;
    }

    rows.push(row);
    const mark = afterVal.ok ? "PASS" : programId ? "BLOCK" : "RESEARCH";
    console.error(
      `${mark} ${j.brandName} ${programId || "?"} ${beforeElite.length}→${afterTitles.length} [${beforeTitles.slice(0, 2).join("|")}…]`
    );
  }

  // Also count registry entries without frozen (for 233 coverage note)
  let registryCount = 0;
  if (existsSync(REGISTRY_JS)) {
    const src = readFileSync(REGISTRY_JS, "utf8");
    registryCount = [...src.matchAll(/^\s*id:\s*'/gm)].length;
  }

  const summary = {
    generatedAt: new Date().toISOString(),
    contract: BE_FACTORY_PERMANENT_LOYALTY_TIER_COMPLETENESS_V1,
    lesson: "BE-QL-LOYALTY-TIER-COMPLETENESS-001",
    mode: dryRun ? "dry-run" : "apply",
    researchSpendUsd: 0,
    totalRegisteredBrandsTarget: 233,
    registryCandidates: registryCount,
    frozenAudited: rows.length,
    brandsWithLoyaltyPrograms: withProgram,
    brandsWithTieredPrograms: tiered,
    brandsWithNonTieredRecognition: nonTiered,
    brandsWithoutLoyaltyParticipation: noProgram,
    brandsWithCompleteVerifiedTiers: completeVerified,
    brandsWithIncompleteTiers: incomplete,
    brandsWithCollapsedTiers: collapsed,
    brandsRemediated: remediated,
    brandsPatched: patched,
    brandsStillRequiringResearch: needsResearch,
    brandsStillBlocked: rows.filter((r) => r.qualityGate === "BLOCK").length,
    LOYALTY_TIER_AUDIT_COMPLETE: false,
    note:
      "LOYALTY_TIER_AUDIT_COMPLETE remains false until unresolved programs are researched and authenticated :8080 Comparison Studio certification passes.",
  };

  writeFileSync(join(OUT, "13-loyalty-tier-audit-summary.json"), JSON.stringify(summary, null, 2) + "\n");
  writeFileSync(
    join(OUT, "14-loyalty-tier-brand-matrix.json"),
    JSON.stringify({ generatedAt: summary.generatedAt, rows }, null, 2) + "\n"
  );

  const md = [
    `# Brand Explorer — Loyalty Elite Tier Completeness Audit`,
    ``,
    `Generated: ${summary.generatedAt}`,
    `Mode: \`${summary.mode}\``,
    `Contract: \`${summary.contract}\``,
    `Lesson: \`${summary.lesson}\``,
    `Research spend: **$0**`,
    ``,
    `## Portfolio summary`,
    ``,
    `| Metric | Count |`,
    `|---|---:|`,
    `| Registered brands (target) | 233 |`,
    `| Registry candidates | ${registryCount} |`,
    `| Frozen payloads audited | ${rows.length} |`,
    `| Brands with loyalty programs | ${withProgram} |`,
    `| Brands with tiered programs | ${tiered} |`,
    `| Brands with non-tiered recognition | ${nonTiered} |`,
    `| Brands without / unresolved program | ${noProgram} |`,
    `| Complete verified tiers (after) | ${completeVerified} |`,
    `| Collapsed (before) | ${collapsed} |`,
    `| Incomplete (before) | ${incomplete} |`,
    `| Remediated | ${remediated} |`,
    `| Still requiring research | ${needsResearch} |`,
    `| Still quality-gate BLOCK | ${summary.brandsStillBlocked} |`,
    ``,
    `## Brand-level matrix (truncated — full JSON in 14-loyalty-tier-brand-matrix.json)`,
    ``,
    `| Brand | Program | Official | Current | Proposed | Status | Missing | Gate |`,
    `|---|---|---:|---:|---:|---|---|---|`,
    ...rows.slice(0, 80).map(
      (r) =>
        `| ${r.brand} | ${r.loyaltyProgram || "—"} | ${r.officialTierCount} | ${r.currentTierCount} | ${r.proposedTierCount} | ${r.verificationStatus} | ${(r.missingTier || []).join("; ") || "—"} | ${r.qualityGate} |`
    ),
    rows.length > 80 ? `| … | ${rows.length - 80} more rows in JSON | | | | | | |` : ``,
    ``,
    `## Permanent rule`,
    ``,
    `- **Key Benefits & Program Strengths** (\`loyalty.proof\`) = exactly **six** cards.`,
    `- **Elite Tiers — Illustrative Benefits** (\`loyalty.elite\`) = **actual** official tier count (no artificial min/max; never collapse to one generic card).`,
    ``,
    `## LOYALTY_TIER_AUDIT_COMPLETE`,
    ``,
    `**NO** — ${needsResearch} brands still need program research and authenticated :8080 Comparison Studio certification remains outstanding.`,
    ``,
  ].join("\n");
  writeFileSync(join(OUT, "15-loyalty-tier-audit-dashboard.md"), md);

  console.log(JSON.stringify(summary, null, 2));
  if (dryRun) {
    console.error("Dry run only. Re-run with --apply to write candidates.");
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
