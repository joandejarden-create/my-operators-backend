/**
 * Deterministic staged renormalization for RECOMPUTABLE hotels.
 * Preserves historical research dates; never silently overwrites published customer reports.
 * Does not run paid research.
 */

import { mkdirSync, writeFileSync } from "fs";
import { join } from "path";
import {
  ADP_METHODOLOGY_CANONICAL_V1,
  ADP_METHODOLOGY_COMPATIBILITY,
} from "./adp-methodology-canonical-v1.js";
import { buildAdpMethodologyCompatibilityAudit } from "./adp-methodology-compatibility-audit-v1.js";
import {
  buildAdminMethodologyProvenanceBlock,
  resolveAdpMethodologyProvenance,
} from "./adp-methodology-provenance-v1.js";

/**
 * @param {{ cwd?: string, apply?: boolean }} opts
 */
export function planAdpMethodologyStagedNormalize(opts = {}) {
  const cwd = opts.cwd || process.cwd();
  const apply = opts.apply === true;
  const audit = buildAdpMethodologyCompatibilityAudit({ cwd });
  const recomputable = audit.rows.filter(
    (r) => r.compatibility === ADP_METHODOLOGY_COMPATIBILITY.RECOMPUTABLE
  );
  const staged = [];

  for (const row of recomputable) {
    const provenance = resolveAdpMethodologyProvenance(row.propertyId, { cwd });
    const plan = {
      propertyId: row.propertyId,
      hotel: row.hotel,
      researchPeriodId: provenance.researchPeriodId,
      researchPublishedAt: provenance.researchPublishedAt,
      recomputableFields: row.recomputableFields,
      mode: "DETERMINISTIC_RECALC_FROM_EXISTING_EVIDENCE",
      publication: "STAGED_ONLY_NO_CUSTOMER_OVERWRITE",
      distinction: "RECALCULATED_NOT_NEW_RESEARCH",
      provenance: buildAdminMethodologyProvenanceBlock({
        ...provenance,
        compatibility: row.compatibility,
      }),
    };
    staged.push(plan);
  }

  const result = {
    gate: ADP_METHODOLOGY_CANONICAL_V1,
    generatedAt: new Date().toISOString(),
    apply,
    recomputableCount: recomputable.length,
    comparableCount: audit.comparableCount,
    stagedPlans: staged,
    actionsTaken: [],
    notes: [
      "When recomputableCount is 0, durable CURRENT already matches canonical methodology — no staged metric rewrite required.",
      "Apply mode writes a staging manifest only; it does not republish or replace customer-approved shares/PDFs.",
    ],
  };

  if (apply) {
    const outDir = join(
      cwd,
      "reports/ai-demand-positioning/methodology/staged-normalize"
    );
    mkdirSync(outDir, { recursive: true });
    const outPath = join(
      outDir,
      `staged-normalize-plan-${Date.now()}.json`
    );
    writeFileSync(outPath, JSON.stringify(result, null, 2));
    result.actionsTaken.push({ type: "wrote_staging_manifest", path: outPath });
  }

  return result;
}
