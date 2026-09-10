#!/usr/bin/env node
/**
 * npm run test:adp-displacement-evidence-count-parity
 *
 * Enumerates ALL published displacement rows and asserts:
 *   displayedScenarioCount === canonicalSupportScenarioCount === drawerScenarioCount
 * Fail on count mismatch, empty drawer with nonzero count, wrong competitor/period,
 * missing raw response, or missing published support set.
 */

import assert from "assert";
import {
  listPublishedPropertyIds,
  loadPublishedManifest,
  loadPublishedReport,
  loadPublishedEvidenceIndex,
} from "../lib/ai-demand-positioning/published-snapshot.js";
import { loadPropertyProfile, loadPeriod } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { enrichObservationsWithRank } from "../lib/ai-demand-positioning/metrics/executive-metrics-foundation.js";
import {
  buildAllCanonicalDisplacementSupportSets,
  getPublishedDisplacementSupportSet,
  assertDisplacementCountEvidenceExactParity,
  ADP_DISPLACEMENT_COUNT_EVIDENCE_EXACT_PARITY,
  ADP_DISPLACEMENT_EVIDENCE_PUBLISHED_SNAPSHOT_SELF_CONTAINED,
} from "../lib/ai-demand-positioning/customer/adp-canonical-displacement-support-set-v1.js";
import { getPublishedEvidenceResponse } from "../lib/ai-demand-positioning/published-read-service.js";
import { buildAggregateClaimSupportParity } from "../lib/ai-demand-positioning/customer/adp-aggregate-claim-support-parity-v1.js";

const rows = [];
const fails = [];

function pushFail(row, reason) {
  fails.push({ ...row, reason });
}

async function auditProperty(propertyId) {
  const manifest = loadPublishedManifest(propertyId);
  const report = loadPublishedReport(propertyId);
  const evidenceIndex = loadPublishedEvidenceIndex(propertyId);
  assert.ok(manifest?.latestPeriodId, `${propertyId} manifest`);
  assert.ok(report, `${propertyId} report`);
  assert.ok(evidenceIndex?.displacementSupportSets, `${propertyId} must bake displacementSupportSets`);

  const periodId = manifest.latestPeriodId;
  const profile = loadPropertyProfile(propertyId);
  const period = loadPeriod(periodId);
  const scenarios = buildScenarioUniverse(profile);
  const observations = enrichObservationsWithRank(
    (period.observations || []).filter((o) => o.parsed),
    profile
  );

  // Reference from immutable observations — do NOT trust published aggregates alone
  const referenceBundle = buildAllCanonicalDisplacementSupportSets({
    propertyProfile: profile,
    observations,
    scenarios,
    periodMeta: { periodId, propertyId, executionDate: period.executionDate },
    scope: "overall",
  });

  const displayedRows = report.lostDemand?.displacement || [];
  for (const disp of displayedRows) {
    const entityId = disp.entityId;
    const displayed = Number(disp.displacementCount) || 0;
    const supportSet = getPublishedDisplacementSupportSet(evidenceIndex, entityId, "overall");
    const ref = referenceBundle.byCompetitor?.[entityId];
    const base = {
      propertyId,
      periodId,
      competitor: disp.name,
      entityId,
      displayedScenarioCount: displayed,
      canonicalSupportScenarioCount: supportSet?.displacementScenarioCount ?? null,
      referenceScenarioCount: ref?.displacementScenarioCount ?? null,
      drawerScenarioCount: supportSet
        ? new Set((supportSet.evidence || []).map((e) => e.scenarioId).filter(Boolean)).size
        : 0,
      observationCount: supportSet?.supportingObservationIds?.length ?? 0,
      entityParity: supportSet?.competitorEntityId === entityId,
      periodParity: !supportSet?.periodId || supportSet.periodId === periodId,
    };

    if (!supportSet) {
      pushFail(base, "missing_published_support_set");
      rows.push({ ...base, pass: false });
      continue;
    }

    const parity = assertDisplacementCountEvidenceExactParity({
      displayedScenarioCount: displayed,
      supportSet,
    });
    const claimParity = buildAggregateClaimSupportParity({
      claimId: supportSet.claim?.claimId,
      claimType: "competitive_displacement",
      aggregationGrain: "unique_scenario_id",
      supportIds: [...(supportSet.uniqueScenarioIds || [])],
      displayedCount: displayed,
      supportCount: supportSet.displacementScenarioCount,
    });

    // Drawer API must resolve from published support set (self-contained)
    const api = await getPublishedEvidenceResponse(propertyId, {
      type: "displacement",
      competitorId: entityId,
      scope: "overall",
    });
    const apiScenarios = new Set((api.evidence || []).map((e) => e.scenarioId).filter(Boolean));
    const apiCount = apiScenarios.size;
    const sourceOk = api.source === "published_displacement_support_set_v1";
    const rawOk = (api.evidence || []).every((e) => e.aiResponse || e.responseExcerpt || e.rawResponse);
    const competitorOk = api.competitorId === entityId;
    const periodOk = (api.evidence || []).every((e) => !e.periodId || e.periodId === periodId);

    const refMatch =
      ref == null
        ? displayed === 0
        : ref.displacementScenarioCount === displayed &&
          ref.displacementScenarioCount === supportSet.displacementScenarioCount;

    const pass =
      parity.pass &&
      claimParity.pass &&
      sourceOk &&
      api.ok !== false &&
      apiCount === displayed &&
      (displayed === 0 || (api.evidenceAvailable && apiCount > 0)) &&
      rawOk &&
      competitorOk &&
      periodOk &&
      base.entityParity &&
      base.periodParity &&
      refMatch;

    const out = {
      ...base,
      apiSource: api.source,
      apiScenarioCount: apiCount,
      pass,
      gate: ADP_DISPLACEMENT_COUNT_EVIDENCE_EXACT_PARITY,
      publishedSelfContained: ADP_DISPLACEMENT_EVIDENCE_PUBLISHED_SNAPSHOT_SELF_CONTAINED,
    };
    rows.push(out);
    if (!pass) {
      pushFail(out, [
        !parity.pass && "count_parity",
        !claimParity.pass && "claim_parity",
        !sourceOk && `wrong_api_source:${api.source}`,
        apiCount !== displayed && "api_count_mismatch",
        displayed > 0 && !api.evidenceAvailable && "empty_drawer",
        !rawOk && "missing_raw_response",
        !competitorOk && "wrong_competitor",
        !periodOk && "wrong_period",
        !refMatch && "reference_mismatch",
      ]
        .filter(Boolean)
        .join("|"));
    }
  }
}

async function main() {
  const ids = listPublishedPropertyIds().sort();
  for (const id of ids) {
    await auditProperty(id);
  }

  const pass = rows.filter((r) => r.pass).length;
  const fail = rows.filter((r) => !r.pass).length;
  const emptyDrawers = fails.filter((f) => String(f.reason).includes("empty_drawer")).length;
  const countMismatches = fails.filter((f) =>
    /count_parity|api_count_mismatch|reference_mismatch/.test(String(f.reason))
  ).length;

  console.log(
    JSON.stringify(
      {
        ok: fail === 0,
        gate: ADP_DISPLACEMENT_COUNT_EVIDENCE_EXACT_PARITY,
        totalDisplacementRows: rows.length,
        pass,
        fail,
        emptyDrawers,
        countMismatches,
        fails: fails.slice(0, 50),
      },
      null,
      2
    )
  );
  if (fail > 0) process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
