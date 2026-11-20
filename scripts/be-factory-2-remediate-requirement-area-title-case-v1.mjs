#!/usr/bin/env node
/**
 * Remediate standards.requirement (Requirement Area) titles to Proper Case + &.
 * Permanent Factory rule (Curio/Ascend bar / BE_EDITORIAL_TITLE_CASE_V1).
 */
import "../load-env.js";
import {
  writeFileSync,
  mkdirSync,
  copyFileSync,
  existsSync,
  readdirSync,
  readFileSync,
  renameSync,
  unlinkSync,
} from "fs";
import { join } from "path";
import { setTimeout as sleep } from "timers/promises";
import {
  toEditorialTitleCase,
  isEditorialTitleCase,
  normalizeHeadingConjunctionAmpersand,
} from "../lib/brand-explorer/be-editorial-title-case-v1.js";

const ROOT = process.cwd();
const HE = "C:\\Dev\\dealality\\ws-hotel-explorer";
const PREVIEW = join(ROOT, "public/brand-explorer-phase2e-preview");

function nz(s) {
  return s == null ? "" : String(s).trim();
}

function fixRequirementTitle(title) {
  return toEditorialTitleCase(nz(title));
}

function titleOk(title) {
  const t = nz(title);
  if (!t) return true;
  if (/\sand\s/i.test(t)) return false;
  return isEditorialTitleCase(t);
}

async function writeJsonAtomic(abs, obj, attempts = 8) {
  const tmp = `${abs}.tmp-${process.pid}`;
  const body = JSON.stringify(obj, null, 2) + "\n";
  let lastErr;
  for (let i = 0; i < attempts; i++) {
    try {
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
      return;
    } catch (err) {
      lastErr = err;
      await sleep(150 + i * 200);
    }
  }
  throw lastErr;
}

function patchRequirementTitles(blocks) {
  let changed = 0;
  const next = (blocks || []).map((b) => {
    if (nz(b.slotKey) !== "standards.requirement") return b;
    const from = nz(b.title);
    if (!from) return b;
    const to = fixRequirementTitle(from);
    if (to === from) return b;
    changed += 1;
    return { ...b, title: to };
  });
  return { blocks: next, changed };
}

async function main() {
  const files = readdirSync(PREVIEW).filter((x) =>
    x.endsWith("-factory2-frozen-approval-payload-v1.json")
  );
  const rows = [];
  let patched = 0;
  let titleFixes = 0;
  const writeErrors = [];

  for (const f of files) {
    const abs = join(PREVIEW, f);
    const j = JSON.parse(readFileSync(abs, "utf8"));
    const slug = nz(j.slug) || f.replace(/-factory2-frozen-approval-payload-v1\.json$/i, "");
    const brand = nz(j.brandName) || slug;
    let blocks = j.brandExplorer?.blocks || [];
    const beforeBad = blocks
      .filter((b) => b.slotKey === "standards.requirement")
      .filter((b) => !titleOk(b.title));
    const result = patchRequirementTitles(blocks);
    blocks = result.blocks;

    if (result.changed > 0) {
      j.brandExplorer = j.brandExplorer || {};
      j.brandExplorer.blocks = blocks;
      j.requirementAreaTitleCaseRemediation = {
        contract: "BE_FACTORY_2_0_REQUIREMENT_AREA_TITLE_CASE_V1",
        remediatedAt: new Date().toISOString(),
        titlesFixed: result.changed,
      };
      try {
        await writeJsonAtomic(abs, j);
        patched += 1;
        titleFixes += result.changed;
        const depthName = f.replace(
          "-factory2-frozen-approval-payload-v1.json",
          "-factory2-depth-candidate.json"
        );
        const depthAbs = join(PREVIEW, depthName);
        if (existsSync(depthAbs)) {
          try {
            const d = JSON.parse(readFileSync(depthAbs, "utf8"));
            const dBlocks = d.brandExplorer?.blocks || d.blocks;
            if (Array.isArray(dBlocks)) {
              const dResult = patchRequirementTitles(dBlocks);
              if (d.brandExplorer) d.brandExplorer.blocks = dResult.blocks;
              else d.blocks = dResult.blocks;
              await writeJsonAtomic(depthAbs, d);
            }
          } catch (err) {
            console.error(`depth sync skip ${depthName}:`, err.message || err);
          }
        }
      } catch (err) {
        writeErrors.push({ file: f, error: String(err.message || err) });
      }
    }

    const afterBad = blocks
      .filter((b) => b.slotKey === "standards.requirement")
      .filter((b) => !titleOk(b.title));
    const reqCount = blocks.filter((b) => b.slotKey === "standards.requirement").length;
    rows.push({
      slug,
      brand,
      requirementRows: reqCount,
      beforeBad: beforeBad.length,
      afterBad: afterBad.length,
      fixed: result.changed,
      pass: afterBad.length === 0,
      sampleFixed: beforeBad.slice(0, 2).map((b) => ({
        from: b.title,
        to: fixRequirementTitle(b.title),
      })),
    });
    console.error(
      `${afterBad.length === 0 ? "PASS" : "FAIL"} ${brand} req=${reqCount} fixed=${result.changed} remainBad=${afterBad.length}`
    );
  }

  // Sync atelier + payloads to HE 8080
  if (existsSync(HE)) {
    for (const rel of ["public/js/brand-explorer-atelier-from-api.js", "public/js/brand-explorer-editorial-title-case.js"]) {
      const src = join(ROOT, rel);
      const dest = join(HE, rel);
      if (existsSync(src) && existsSync(join(HE, "public/js"))) copyFileSync(src, dest);
    }
    const previewDest = join(HE, "public/brand-explorer-phase2e-preview");
    if (existsSync(previewDest)) {
      for (const f of readdirSync(PREVIEW).filter(
        (x) =>
          x.endsWith("-factory2-frozen-approval-payload-v1.json") ||
          x.endsWith("-factory2-depth-candidate.json")
      )) {
        try {
          copyFileSync(join(PREVIEW, f), join(previewDest, f));
        } catch (err) {
          console.error(`HE sync skip ${f}:`, err.message || err);
        }
      }
    }
    console.error("Synced Requirement Area title-case to HE 8080");
  }

  const fail = rows.filter((r) => !r.pass);
  const dir = join(ROOT, "reports/brand-explorer/factory-2-0");
  mkdirSync(dir, { recursive: true });
  const report = {
    generatedAt: new Date().toISOString(),
    contract: "BE_FACTORY_2_0_REQUIREMENT_AREA_TITLE_CASE_V1",
    summary: {
      audited: rows.length,
      patched,
      titleFixes,
      pass: rows.filter((r) => r.pass).length,
      fail: fail.length,
      writeErrors: writeErrors.length,
    },
    writeErrors,
    fail: fail.map((r) => ({ slug: r.slug, brand: r.brand, afterBad: r.afterBad })),
  };
  writeFileSync(join(dir, "96-requirement-area-title-case-remediation.json"), JSON.stringify({ ...report, rows }, null, 2) + "\n");
  writeFileSync(
    join(dir, "96-requirement-area-title-case-remediation.md"),
    [
      `# Factory 2.0 — Requirement Area Title Case Remediation`,
      ``,
      `Generated: ${report.generatedAt}`,
      `Rule: Requirement Area titles (white) must be **Proper Case** and use **&** instead of "and" — all brands.`,
      ``,
      `| Metric | Value |`,
      `|---|---:|`,
      `| Audited | ${report.summary.audited} |`,
      `| Brands patched | ${report.summary.patched} |`,
      `| Titles fixed | ${report.summary.titleFixes} |`,
      `| PASS | ${report.summary.pass} |`,
      `| FAIL | ${report.summary.fail} |`,
      ``,
      `Examples:`,
      `- Design & guest promise review → Design & Guest Promise Review`,
      `- Public-space and amenity capital → Public-Space & Amenity Capital`,
      ``,
    ].join("\n")
  );
  console.log(JSON.stringify(report.summary, null, 2));
  if (fail.length || writeErrors.length) process.exit(2);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
