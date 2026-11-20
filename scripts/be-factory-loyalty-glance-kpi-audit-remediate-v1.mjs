#!/usr/bin/env node
/**
 * Portfolio Loyalty at a Glance KPI audit + remediation ($0 paid).
 *
 * Injects four meaningful loyalty.kpi.* cards from canonical program intelligence
 * + evidence-informed Estimated Loyalty Mix when actuals are unavailable.
 *
 * No production Airtable Presentation writeback.
 *
 * Usage:
 *   node scripts/be-factory-loyalty-glance-kpi-audit-remediate-v1.mjs --dry-run
 *   node scripts/be-factory-loyalty-glance-kpi-audit-remediate-v1.mjs --apply
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
  BE_FACTORY_PERMANENT_LOYALTY_GLANCE_KPI_V1,
  LOYALTY_KPI_SLOTS,
  buildLoyaltyGlanceKpiBlocks,
  resolveLoyaltyProgramId,
  validateLoyaltyGlanceKpis,
} from "../lib/brand-explorer/be-factory-permanent-loyalty-glance-kpi-v1.js";

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

function replaceKpiBlocks(blocks, newKpis) {
  const others = (blocks || []).filter(
    (b) => !LOYALTY_KPI_SLOTS.includes(nz(b.slotKey || b.slot_key))
  );
  const programIdx = others.findIndex((b) => nz(b.slotKey) === "loyalty.program");
  if (programIdx >= 0) {
    return [
      ...others.slice(0, programIdx + 1),
      ...newKpis,
      ...others.slice(programIdx + 1),
    ];
  }
  return [...newKpis, ...others];
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

function kpiSummary(blocks) {
  return LOYALTY_KPI_SLOTS.map((sk) => {
    const b = (blocks || []).find((x) => nz(x.slotKey) === sk);
    return b ? { title: nz(b.title), value: nz(b.body) } : null;
  });
}

async function remediateFile(abs, j, forceProgramId) {
  const blocks = j.brandExplorer?.blocks || j.blocks || [];
  const progBlock = blocks.find((b) => nz(b.slotKey) === "loyalty.program");
  const programId =
    forceProgramId ||
    resolveLoyaltyProgramId({
      loyaltyProgram: progBlock?.title,
      brandName: j.brandName,
      parentCompany: j.parentCompany,
    });
  const beforeVal = validateLoyaltyGlanceKpis(blocks, {
    participates: programId !== "none" && programId !== "non_tiered_network",
    notApplicable: programId === "non_tiered_network",
  });
  const built = buildLoyaltyGlanceKpiBlocks({
    programId,
    brandName: j.brandName,
    positioning: j.positioning,
    chainScale: j.chainScale,
    sortStart: 3200,
  });
  const nextBlocks = replaceKpiBlocks(blocks, built.blocks || []);
  const afterVal = validateLoyaltyGlanceKpis(nextBlocks, {
    participates: programId !== "none" && programId !== "non_tiered_network",
    notApplicable: built.notApplicable === true,
  });
  const assembledHash = hashPresentationBlocks(nextBlocks);
  const beforeCards = kpiSummary(blocks);
  const afterCards = kpiSummary(nextBlocks);
  const needRemediate = !beforeVal.ok || beforeVal.verifiedCount < 4;

  return {
    brandId: j.brandId || null,
    brand: j.brandName,
    slug: j.slug,
    programId: programId || null,
    loyaltyProgram: built.programName || progBlock?.title || null,
    before: beforeCards,
    after: afterCards,
    beforeVerified: beforeVal.verifiedCount,
    afterVerified: afterVal.verifiedCount,
    beforeClass: beforeVal.classification,
    afterClass: afterVal.classification,
    qualityGate: afterVal.ok || built.notApplicable ? "PASS" : "BLOCK",
    remediated: needRemediate && (built.ok || built.notApplicable),
    assembledHash,
    nextBlocks,
    needRemediate,
    mixKind: built.mix?.kind || null,
    mixValue: built.mix?.displayValue || built.mix?.value || null,
  };
}

async function main() {
  mkdirSync(OUT, { recursive: true });
  const files = [
    ...readdirSync(PREVIEW)
      .filter((f) => f.endsWith("-factory2-frozen-approval-payload-v1.json"))
      .map((f) => ({ file: f, forceProgramId: null })),
    {
      file: "curio-collection-phase2h-candidate.json",
      forceProgramId: "hilton_honors",
    },
    {
      file: "ascend-phase2h-candidate.json",
      forceProgramId: "choice_privileges",
    },
    {
      file: "autograph-collection-phase2h-candidate.json",
      forceProgramId: "marriott_bonvoy",
    },
  ];

  const rows = [];
  let patched = 0;
  let remediated = 0;
  const counts = { 0: 0, 1: 0, 2: 0, 3: 0, 4: 0 };

  for (const entry of files) {
    const abs = join(PREVIEW, entry.file);
    if (!existsSync(abs)) continue;
    const j = JSON.parse(readFileSync(abs, "utf8"));
    const row = await remediateFile(abs, j, entry.forceProgramId);
    counts[Math.min(4, row.beforeVerified)] =
      (counts[Math.min(4, row.beforeVerified)] || 0) + 1;

    if (!dryRun && row.needRemediate && row.remediated) {
      if (j.brandExplorer) j.brandExplorer.blocks = row.nextBlocks;
      else j.blocks = row.nextBlocks;
      j.approvalContentHash = row.assembledHash;
      j.loyaltyGlanceKpiRemediation = {
        contract: BE_FACTORY_PERMANENT_LOYALTY_GLANCE_KPI_V1,
        lesson: "BE-QL-LOYALTY-GLANCE-KPI-001",
        remediatedAt: new Date().toISOString(),
        programId: row.programId,
        researchSpendUsd: 0,
        mixKind: row.mixKind,
        beforeVerified: row.beforeVerified,
        afterVerified: row.afterVerified,
      };
      writeJsonAtomic(abs, j);

      if (entry.file.includes("-factory2-frozen-approval-payload-v1.json")) {
        const depthName = entry.file.replace(
          "-factory2-frozen-approval-payload-v1.json",
          "-factory2-depth-candidate.json"
        );
        const depthAbs = join(PREVIEW, depthName);
        if (existsSync(depthAbs)) {
          try {
            const d = JSON.parse(readFileSync(depthAbs, "utf8"));
            if (d.brandExplorer) d.brandExplorer.blocks = row.nextBlocks;
            else d.blocks = row.nextBlocks;
            d.approvalContentHash = row.assembledHash;
            writeJsonAtomic(depthAbs, d);
          } catch (err) {
            console.error(`depth sync skip ${depthName}:`, err.message || err);
          }
        }
      }

      if (j.brandId) {
        const stagePath = join(STAGING, j.brandId, "stage.json");
        if (existsSync(stagePath)) {
          try {
            const stage = JSON.parse(readFileSync(stagePath, "utf8"));
            stage.assembledHash = row.assembledHash;
            stage.candidateHash = row.assembledHash;
            stage.loyaltyGlanceKpiAt = new Date().toISOString();
            stage.loyaltyProgramId = row.programId;
            writeJsonAtomic(stagePath, stage);
          } catch (err) {
            console.error(`stage sync skip ${j.brandId}:`, err.message || err);
          }
        }
      }

      try {
        patchRegistryHash(j.slug, row.assembledHash);
      } catch (err) {
        console.error(`registry hash skip ${j.slug}:`, err.message || err);
      }

      if (existsSync(join(HE, "public/brand-explorer-phase2e-preview"))) {
        try {
          copyFileSync(abs, join(HE, "public/brand-explorer-phase2e-preview", entry.file));
        } catch {
          /* ignore */
        }
      }

      row.patched = true;
      patched += 1;
      remediated += 1;
    } else if (row.remediated) {
      remediated += 1;
    }

    rows.push({
      brandId: row.brandId,
      brand: row.brand,
      slug: row.slug,
      program: row.loyaltyProgram,
      programId: row.programId,
      kpi1: row.after[0],
      kpi2: row.after[1],
      kpi3: row.after[2],
      kpi4: row.after[3],
      beforeVerified: row.beforeVerified,
      afterVerified: row.afterVerified,
      beforeClass: row.beforeClass,
      afterClass: row.afterClass,
      mixKind: row.mixKind,
      mixValue: row.mixValue,
      remediation: row.needRemediate ? (row.remediated ? "RESTORED_PROGRAM_KPIS" : "PENDING") : "NONE",
      qualityGate: row.qualityGate,
      patched: !!row.patched,
    });

    console.error(
      `${row.qualityGate} ${row.brand} ${row.programId || "?"} ${row.beforeVerified}→${row.afterVerified} mix=${row.mixValue || "—"}`
    );
  }

  let registryCount = 0;
  if (existsSync(REGISTRY_JS)) {
    registryCount = [...readFileSync(REGISTRY_JS, "utf8").matchAll(/^\s*id:\s*'/gm)].length;
  }

  const summary = {
    generatedAt: new Date().toISOString(),
    contract: BE_FACTORY_PERMANENT_LOYALTY_GLANCE_KPI_V1,
    lesson: "BE-QL-LOYALTY-GLANCE-KPI-001",
    mode: dryRun ? "dry-run" : "apply",
    researchSpendUsd: 0,
    totalRegisteredBrandsTarget: 233,
    registryCandidates: registryCount,
    brandsEvaluated: rows.length,
    beforeVerifiedDistribution: counts,
    brandsWithFourVerifiedAfter: rows.filter((r) => r.afterVerified === 4).length,
    brandsWithThreeVerifiedAfter: rows.filter((r) => r.afterVerified === 3).length,
    brandsWithTwoVerifiedAfter: rows.filter((r) => r.afterVerified === 2).length,
    brandsWithOneVerifiedAfter: rows.filter((r) => r.afterVerified === 1).length,
    brandsWithZeroVerifiedAfter: rows.filter((r) => r.afterVerified === 0).length,
    brandsRemediated: remediated,
    brandsPatched: patched,
    brandsStillBlocked: rows.filter((r) => r.qualityGate === "BLOCK").length,
    estimatedMixCount: rows.filter((r) => r.mixKind === "estimated").length,
    verifiedProgramMixCount: rows.filter((r) => r.mixKind === "verified_program").length,
    LOYALTY_GLANCE_KPI_AUDIT_COMPLETE: false,
    note:
      "LOYALTY_GLANCE_KPI_AUDIT_COMPLETE remains false until authenticated :8080 Comparison Studio certification passes brand-by-brand.",
  };

  writeFileSync(
    join(OUT, "16-loyalty-glance-kpi-audit-summary.json"),
    JSON.stringify(summary, null, 2) + "\n"
  );
  writeFileSync(
    join(OUT, "17-loyalty-glance-kpi-brand-matrix.json"),
    JSON.stringify({ generatedAt: summary.generatedAt, rows }, null, 2) + "\n"
  );
  writeFileSync(
    join(OUT, "18-loyalty-glance-kpi-audit-dashboard.md"),
    [
      `# Brand Explorer — Loyalty at a Glance KPI Audit`,
      ``,
      `Generated: ${summary.generatedAt}`,
      `Mode: \`${summary.mode}\``,
      `Contract: \`${summary.contract}\``,
      `Lesson: \`${summary.lesson}\``,
      `Research spend: **$0**`,
      ``,
      `## Policy`,
      ``,
      `- Exactly **four** meaningful KPI cards for measurable loyalty programs.`,
      `- Four rendered N/A shells ≠ complete.`,
      `- **Typical Loyalty Mix** may be a clearly labeled **Estimated Loyalty Mix** range when actuals are unavailable.`,
      `- Program-level metrics reused across participating brands with explicit scope labels.`,
      ``,
      `## Summary`,
      ``,
      `| Metric | Count |`,
      `|---|---:|`,
      `| Registered brands (target) | 233 |`,
      `| Brands evaluated | ${rows.length} |`,
      `| Four verified KPIs (after) | ${summary.brandsWithFourVerifiedAfter} |`,
      `| Estimated Loyalty Mix cards | ${summary.estimatedMixCount} |`,
      `| Verified program mix cards | ${summary.verifiedProgramMixCount} |`,
      `| Remediated | ${remediated} |`,
      `| Still BLOCK | ${summary.brandsStillBlocked} |`,
      ``,
      `## Before → verified count distribution`,
      ``,
      `| Verified KPIs before | Brands |`,
      `|---:|---:|`,
      `| 0 | ${counts[0] || 0} |`,
      `| 1 | ${counts[1] || 0} |`,
      `| 2 | ${counts[2] || 0} |`,
      `| 3 | ${counts[3] || 0} |`,
      `| 4 | ${counts[4] || 0} |`,
      ``,
      `## Sample matrix (first 40)`,
      ``,
      `| Brand | Program | KPI1 | KPI2 | KPI3 | KPI4 | Gate |`,
      `|---|---|---|---|---|---|---|`,
      ...rows.slice(0, 40).map(
        (r) =>
          `| ${r.brand} | ${r.program || "—"} | ${r.kpi1?.value || "—"} | ${r.kpi2?.value || "—"} | ${r.kpi3?.value || "—"} | ${r.kpi4?.value || "—"} | ${r.qualityGate} |`
      ),
      ``,
      `## LOYALTY_GLANCE_KPI_AUDIT_COMPLETE`,
      ``,
      `**NO** — authenticated :8080 brand-by-brand Certification Studio verification outstanding.`,
      ``,
    ].join("\n")
  );

  // Update portfolio summary pointer
  const sumPath = join(OUT, "00-summary.json");
  if (existsSync(sumPath)) {
    try {
      const s = JSON.parse(readFileSync(sumPath, "utf8"));
      s.loyaltyGlanceKpiAudit = {
        contract: BE_FACTORY_PERMANENT_LOYALTY_GLANCE_KPI_V1,
        lesson: "BE-QL-LOYALTY-GLANCE-KPI-001",
        report: "18-loyalty-glance-kpi-audit-dashboard.md",
        matrix: "17-loyalty-glance-kpi-brand-matrix.json",
        brandsEvaluated: rows.length,
        brandsRemediated: remediated,
        fourVerifiedAfter: summary.brandsWithFourVerifiedAfter,
        LOYALTY_GLANCE_KPI_AUDIT_COMPLETE: false,
      };
      writeFileSync(sumPath, JSON.stringify(s, null, 2) + "\n");
    } catch {
      /* ignore */
    }
  }

  console.log(JSON.stringify(summary, null, 2));
  if (dryRun) console.error("Dry run only. Re-run with --apply to write candidates.");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
