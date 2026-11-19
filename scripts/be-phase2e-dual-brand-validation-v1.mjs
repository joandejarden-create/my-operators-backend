#!/usr/bin/env node
/**
 * Phase 2E dual-brand validation — Curio browser re-check + Ascend independent reconstruction.
 * Never writes Airtable. Never uses Presentation / completed fixtures as generator input.
 *
 *   node --env-file=.env scripts/be-phase2e-dual-brand-validation-v1.mjs
 */
import { runPhase2eDualBrandValidation } from "../lib/brand-explorer/be-phase2e-universal-reconstruction-v1.js";

async function main() {
  const result = await runPhase2eDualBrandValidation({
    allowLiveHpc: true,
    allowOfficialFetch: true,
    captureBaseline: true,
  });

  console.log(
    JSON.stringify(
      {
        ok: true,
        reportDir: result.reportDir,
        recommendation: result.dual.readiness.recommendation,
        curio: {
          applicable: result.curio.quality.scores.applicableFieldCompleteness,
          atelier: result.curio.atelierVm.status,
          grounding: result.curio.attribution.financeOpsGroundingRate,
        },
        ascend: {
          applicable: result.ascend.quality.scores.applicableFieldCompleteness,
          atelier: result.ascend.atelierVm.status,
          grounding: result.ascend.attribution.financeOpsGroundingRate,
          loyaltyContamination: result.ascend.atelierVm.loyaltyContamination,
        },
      },
      null,
      2,
    ),
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
