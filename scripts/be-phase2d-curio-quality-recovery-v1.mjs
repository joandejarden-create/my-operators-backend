#!/usr/bin/env node
/**
 * Phase 2D Curio quality recovery — independent reconstruction via enhanced universal factory.
 * Never writes Airtable. Never uses production Presentation / fixtures as generator input.
 *
 *   node --env-file=.env scripts/be-phase2d-curio-quality-recovery-v1.mjs
 */
import { writeFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { runPhase2dCurioQualityRecovery } from "../lib/brand-explorer/be-phase2d-curio-quality-recovery-v1.js";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");

async function main() {
  const result = await runPhase2dCurioQualityRecovery({
    allowLiveHpc: true,
    allowOfficialFetch: true,
    reuseBaseline: true,
  });

  writeFileSync(
    join(result.dir, "12-atelier-mount-note.json"),
    JSON.stringify(
      {
        status: "READY_FOR_LIVE_ATELIER",
        overlayPath: "public/brand-explorer-phase2d-preview/curio-candidate.json",
        validationUrl:
          "/brand-explorer-combined.html?brand=curio-collection&beInternalPreview=1&beCandidateOverlay=1",
        note: "Mount uses real atelier (brand-explorer-atelier-from-api.js) with beCandidateOverlay=1. Standalone HTML is not a substitute.",
        renderedApplicationValidation: "PENDING_BROWSER",
      },
      null,
      2,
    ),
  );

  console.log(
    JSON.stringify(
      {
        ok: true,
        dir: result.dir,
        applicableCompleteness: result.quality.scores.applicableFieldCompleteness,
        actualCompleteness: result.quality.scores.actualFieldCompleteness,
        meetsTarget: result.quality.counts.meetsTarget,
        applicablePopulated: result.quality.counts.applicablePopulated,
        applicableSlots: result.quality.counts.applicableSlots,
        researchRequired: result.quality.counts.applicableResearchRequired,
        phase2cGapCategories: result.diagnosis.phase2cOriginal72.byCategory,
        phase2dRemainingCategories: result.diagnosis.phase2dRemaining.byCategory,
        vsPhase2c: result.vs2c.summary,
        atelierOverlay: "public/brand-explorer-phase2d-preview/curio-candidate.json",
        atelierUrl:
          "/brand-explorer-combined.html?brand=curio-collection&beInternalPreview=1&beCandidateOverlay=1",
      },
      null,
      2,
    ),
  );
  process.exit(0);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
