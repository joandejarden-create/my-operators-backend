#!/usr/bin/env node
import { runPhase2eQualityCorrection } from "../lib/brand-explorer/be-phase2e-quality-reassessment-v1.js";

const result = await runPhase2eQualityCorrection({
  allowLiveHpc: true,
  allowOfficialFetch: true,
  maxRepairRounds: 3,
});

console.log(
  JSON.stringify(
    {
      ok: true,
      reportDir: result.reportDir,
      recommendation: result.dual.recommendation,
      bothMeetAllThree: result.dual.bothMeetAllThree,
      curio: result.dual.brands.curio.metrics,
      ascend: result.dual.brands.ascend.metrics,
      curioFailures: result.dual.brands.curio.failures,
      ascendFailures: result.dual.brands.ascend.failures,
    },
    null,
    2,
  ),
);
