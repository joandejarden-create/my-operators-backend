#!/usr/bin/env node
/**
 * No-spend dry-run: first live ownership experiment readiness manifest.
 */
import {
  runLiveExperimentReadinessValidation,
  writeLiveExperimentReadinessArtifacts,
} from "../lib/hotel-intelligence/research-engine-v1/benchmark-p3b-m1/index.js";

const report = await runLiveExperimentReadinessValidation({ memoryOnly: true });
const paths = writeLiveExperimentReadinessArtifacts(report);

console.log(
  JSON.stringify(
    {
      ok: report.ok,
      phase: report.phase,
      paid_execution_authorized: report.paid_execution_authorized,
      stop: report.stop,
      hotels: report.manifest?.cohort?.hotels,
      methods: Object.keys(report.manifest?.methods || {}),
      hardCapUsd: report.manifest?.financial_controls?.proposed_hard_cap_usd,
      expectedListPriceUsd:
        report.manifest?.financial_controls?.expected_list_price_provider_usd,
      expectedCostIsNotMaxGuarantee:
        report.manifest?.financial_controls?.expected_cost_is_not_max_guarantee,
      paidCommand:
        report.manifest?.exact_execution_command
          ?.phase2_paid_only_after_explicit_founder_approval,
      paths,
    },
    null,
    2
  )
);

process.exit(report.ok ? 0 : 1);
