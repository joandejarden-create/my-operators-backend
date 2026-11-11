#!/usr/bin/env node
/**
 * ADP — Minimum Necessary Remeasurement Assessment (read-only).
 *
 * Classifies each of 38 certified hotels:
 *   A — FULLY VALID
 *   B — REPROCESS EXISTING EVIDENCE (no paid research)
 *   C — FRESH MEASUREMENT REQUIRED
 *
 * No paid research, Airtable writes, republish, or deploy.
 */
import { mkdirSync, writeFileSync, readFileSync, readdirSync, existsSync } from "fs";
import { join } from "path";
import { loadPeriod, loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import { detectPropertyMention } from "../lib/ai-demand-positioning/execution/response-parser.js";
import { isComparableObservation } from "../lib/ai-demand-positioning/metrics/grain-governance.js";
import { buildProviderPresenceRows } from "../lib/ai-demand-positioning/metrics/provider-presence-v1.js";
import {
  auditProviderPresenceGrain,
  repairProviderPresenceGrain,
} from "../lib/ai-demand-positioning/metrics/adp-provider-presence-grain-lock-v1.js";
import { roundAdpPercent } from "../lib/ai-demand-positioning/format-percent.js";
import { PROVIDER_CONFIGS } from "../lib/ai-demand-positioning/execution/multi-provider-runner.js";

const CORE_PROVIDERS = Object.freeze(["openai", "gemini", "perplexity", "claude"]);

function estimateCoreRemonitorCost(coreScenarios) {
  const providers = CORE_PROVIDERS;
  let coreTotal = 0;
  const byProvider = {};
  for (const p of providers) {
    const unit = PROVIDER_CONFIGS[p]?.costPerCall || 0.03;
    const calls = Math.max(0, Number(coreScenarios) || 0);
    byProvider[p] = {
      costPerCall: unit,
      coreCalls: calls,
      coreUsd: Number((calls * unit).toFixed(4)),
    };
    coreTotal += calls * unit;
  }
  return {
    providers,
    coreScenarios,
    loyaltyApplicable: false,
    expectedCoreResponses: coreScenarios * providers.length,
    byProvider,
    estimatedCoreUsd: Number(coreTotal.toFixed(2)),
    estimatedLoyaltyUsd: 0,
    estimatedTotalUsd: Number(coreTotal.toFixed(2)),
    hardMaxSuggestedUsd: Number((coreTotal + 2).toFixed(2)),
    scheduleNote: "Governed PROVIDER_CONFIGS costPerCall — invoice may differ",
  };
}

const OUT = "reports/dealality-adp-minimum-necessary-remeasurement-assessment-v1";
const CURRENT = "data/adp-durable-published/current";
const SNAP = "data/adp-durable-published/snapshots";

const CLASS = Object.freeze({
  A: "A_FULLY_VALID",
  B: "B_REPROCESS_EXISTING_EVIDENCE",
  C: "C_FRESH_MEASUREMENT_REQUIRED",
});

function rawText(obs) {
  return String(
    obs?.rawResponse ||
      obs?.responseText ||
      obs?.parsed?.rawText ||
      obs?.parsed?.text ||
      ""
  );
}

function computeFromFrozen(observations, frozenIds, mentionOf) {
  const idSet = new Set((frozenIds || []).map(String));
  const inEligible = (observations || []).filter((o) =>
    idSet.has(String(o.scenarioId || ""))
  );
  const comparable = inEligible.filter((o) => isComparableObservation(o));
  const present = comparable.filter((o) => mentionOf(o));
  const rate =
    comparable.length > 0
      ? roundAdpPercent((present.length / comparable.length) * 100)
      : null;

  const byScenario = new Map();
  for (const sid of idSet) byScenario.set(sid, false);
  for (const obs of comparable) {
    if (mentionOf(obs)) byScenario.set(String(obs.scenarioId), true);
  }
  let capturedScenarios = 0;
  for (const v of byScenario.values()) if (v) capturedScenarios += 1;
  const spRate =
    idSet.size > 0
      ? roundAdpPercent((capturedScenarios / idSet.size) * 100)
      : null;

  const projected = comparable.map((o) => ({
    ...o,
    mentioned: mentionOf(o),
  }));
  const { providers } = buildProviderPresenceRows(projected, {
    scenarioIds: idSet,
  });
  const mentionSum = providers.reduce((a, p) => a + Number(p.mentioned || 0), 0);
  const comparableSum = providers.reduce(
    (a, p) => a + Number(p.comparable || 0),
    0
  );

  return {
    frozenCount: idSet.size,
    eligibleObs: inEligible.length,
    comparableObservations: comparable.length,
    presentObservations: present.length,
    considerationRate: rate,
    scenarioPresence: {
      eligibleScenarios: idSet.size,
      capturedScenarios,
      rate: spRate,
    },
    providers,
    mentionSum,
    comparableSum,
    providerReconciles:
      mentionSum === present.length && comparableSum === comparable.length,
  };
}

function classifyHotel(row) {
  const reasons = [];
  if (!row.evidence.periodLoaded) {
    reasons.push("PERIOD_JSON_MISSING");
  }
  if (row.evidence.obsCount === 0) {
    reasons.push("ZERO_OBSERVATIONS");
  }
  if (row.evidence.rawTextCoveragePct < 95) {
    reasons.push("RAW_RESPONSE_COVERAGE_BELOW_95");
  }
  if (!row.frozen.eligibleScenarioIdsCount) {
    reasons.push("FROZEN_ELIGIBLE_IDS_MISSING");
  }
  if (!row.reproduction.stored.considerationMatchesCertified) {
    reasons.push("STORED_MENTION_CONSIDERATION_NOT_REPRODUCED");
  }
  if (!row.reproduction.readjudicated.considerationMatchesCertified) {
    reasons.push("READJUDICATED_CONSIDERATION_NOT_REPRODUCED");
  }
  if (row.reproduction.mentionDisagreementRatePct > 5) {
    reasons.push("MENTION_READJUDICATION_DISAGREEMENT_GT_5PCT");
  }

  const evidenceInsufficient =
    !row.evidence.periodLoaded ||
    row.evidence.obsCount === 0 ||
    row.evidence.rawTextCoveragePct < 50 ||
    !row.frozen.eligibleScenarioIdsCount ||
    (!row.reproduction.stored.considerationMatchesCertified &&
      !row.reproduction.readjudicated.considerationMatchesCertified);

  if (evidenceInsufficient) {
    return {
      class: CLASS.C,
      classLabel: "C — FRESH MEASUREMENT REQUIRED",
      reasons,
      paidResearchRequired: true,
    };
  }

  const packageClean =
    row.package.grainPass === true &&
    row.package.spMatchesFrozen === true &&
    row.reproduction.stored.providerReconciles === true &&
    row.reproduction.stored.spMatchesCertified !== false;

  const canRepair =
    row.repair.afterGrainPass === true &&
    row.repair.afterClientSafe === true;

  if (
    packageClean &&
    row.reproduction.stored.considerationMatchesCertified &&
    row.reproduction.mentionDisagreementRatePct <= 1 &&
    row.reproduction.readjudicated.considerationMatchesCertified
  ) {
    return {
      class: CLASS.A,
      classLabel: "A — FULLY VALID",
      reasons: ["PACKAGE_AND_EVIDENCE_RECONCILE"],
      paidResearchRequired: false,
    };
  }

  if (canRepair || row.reproduction.stored.considerationMatchesCertified) {
    const bReasons = [...reasons];
    if (!row.package.grainPass) bReasons.push("PACKAGE_PROVIDER_GRAIN_MISMATCH");
    if (!row.package.spMatchesFrozen) bReasons.push("SP_ELIGIBLE_NE_FROZEN_IDS");
    if (row.reproduction.mentionDisagreementRatePct > 1) {
      bReasons.push("MINOR_MENTION_DISAGREEMENT_REPROCESSABLE");
    }
    if (!bReasons.length) bReasons.push("DERIVED_METRICS_OR_PRESENTATION_CORRECTION");
    return {
      class: CLASS.B,
      classLabel: "B — REPROCESS EXISTING EVIDENCE",
      reasons: bReasons,
      paidResearchRequired: false,
    };
  }

  return {
    class: CLASS.C,
    classLabel: "C — FRESH MEASUREMENT REQUIRED",
    reasons: reasons.length
      ? reasons
      : ["CANNOT_TRUST_REPROCESS_WITHOUT_REMEASURE"],
    paidResearchRequired: true,
  };
}

mkdirSync(OUT, { recursive: true });
const hotels = readdirSync(CURRENT).filter((d) => d.startsWith("adp_"));
const rows = [];

for (const propertyId of hotels) {
  const man = JSON.parse(
    readFileSync(join(CURRENT, propertyId, "manifest.json"), "utf8")
  );
  const reportPath = join(SNAP, propertyId, man.reportFile);
  const wrap = JSON.parse(readFileSync(reportPath, "utf8"));
  const payload = wrap.payload || wrap;
  const cr = payload.executiveMetrics?.considerationRate || {};
  const sp = payload.executiveMetrics?.scenarioPresence || {};
  const frozenIds = Array.isArray(cr.eligibleScenarioIds)
    ? cr.eligibleScenarioIds.map(String)
    : [];

  let period = null;
  let periodErr = null;
  try {
    period = loadPeriod(man.latestPeriodId);
  } catch (e) {
    periodErr = String(e.message || e);
  }
  const profile = loadPropertyProfile(propertyId);
  const obs = period?.observations || [];

  let rawWithText = 0;
  let mentionDisagree = 0;
  let adjudicatedPresent = 0;
  let comparableInFrozen = 0;
  const idSet = new Set(frozenIds);

  for (const o of obs) {
    const text = rawText(o);
    if (text.trim().length > 0) rawWithText += 1;
    if (!idSet.has(String(o.scenarioId || ""))) continue;
    if (!isComparableObservation(o)) continue;
    comparableInFrozen += 1;
    const det = detectPropertyMention(text, profile || payload.property || {});
    const stored = Boolean(o.mentioned);
    const adj = Boolean(det.mentioned);
    if (adj) adjudicatedPresent += 1;
    if (stored !== adj) mentionDisagree += 1;
  }

  const mentionOfStored = (o) => Boolean(o.mentioned);
  const mentionOfAdj = (o) => {
    const det = detectPropertyMention(rawText(o), profile || payload.property || {});
    return Boolean(det.mentioned);
  };

  const storedRepro = computeFromFrozen(obs, frozenIds, mentionOfStored);
  const adjRepro = computeFromFrozen(obs, frozenIds, mentionOfAdj);

  const certifiedPresent = Number(cr.presentObservations);
  const certifiedComparable = Number(cr.comparableObservations);
  const certifiedRate = Number(cr.rate ?? cr.percentage);
  const certifiedSpEligible = Number(sp.eligibleScenarios);
  const certifiedSpCaptured = Number(sp.capturedScenarios);
  const certifiedSpRate = Number(sp.rate);

  const packageAudit = auditProviderPresenceGrain(payload);
  const repair = repairProviderPresenceGrain(propertyId, structuredClone(payload), {
    period,
    propertyProfile: profile,
  });

  const row = {
    propertyId,
    propertyName:
      payload.property?.name || man.propertyName || propertyId,
    periodId: man.latestPeriodId,
    publicationManifestSha256: wrap.publicationManifestSha256 || null,
    methodologyFingerprintSha256: wrap.methodologyFingerprintSha256 || null,
    pdfArtifactSha256: wrap.pdfArtifactSha256 || null,
    certified: {
      considerationRate: certifiedRate,
      presentObservations: certifiedPresent,
      comparableObservations: certifiedComparable,
      scenarioPresenceEligible: certifiedSpEligible,
      scenarioPresenceCaptured: certifiedSpCaptured,
      scenarioPresenceRate: certifiedSpRate,
      mentionPath: cr.mentionPath || null,
    },
    frozen: {
      eligibleScenarioIdsCount: frozenIds.length,
      eligibleScenarioIdsSha256Preview: frozenIds.length
        ? frozenIds.slice(0, 3)
        : [],
    },
    evidence: {
      periodLoaded: Boolean(period),
      periodError: periodErr,
      obsCount: obs.length,
      rawTextCount: rawWithText,
      rawTextCoveragePct:
        obs.length > 0
          ? roundAdpPercent((rawWithText / obs.length) * 100)
          : 0,
      comparableInFrozen,
    },
    reproduction: {
      mentionDisagreementCount: mentionDisagree,
      mentionDisagreementRatePct:
        comparableInFrozen > 0
          ? roundAdpPercent((mentionDisagree / comparableInFrozen) * 100)
          : 0,
      stored: {
        ...storedRepro,
        considerationMatchesCertified:
          storedRepro.presentObservations === certifiedPresent &&
          storedRepro.comparableObservations === certifiedComparable &&
          (certifiedRate == null ||
            Math.abs(Number(storedRepro.considerationRate) - certifiedRate) <=
              0.05),
        spMatchesCertified:
          certifiedSpCaptured == null ||
          storedRepro.scenarioPresence.capturedScenarios === certifiedSpCaptured,
      },
      readjudicated: {
        presentObservations: adjRepro.presentObservations,
        comparableObservations: adjRepro.comparableObservations,
        considerationRate: adjRepro.considerationRate,
        scenarioPresence: adjRepro.scenarioPresence,
        providerReconciles: adjRepro.providerReconciles,
        considerationMatchesCertified:
          adjRepro.presentObservations === certifiedPresent &&
          adjRepro.comparableObservations === certifiedComparable &&
          (certifiedRate == null ||
            Math.abs(Number(adjRepro.considerationRate) - certifiedRate) <=
              0.05),
      },
    },
    package: {
      grainPass: packageAudit.pass,
      mixedGrain: packageAudit.mixedGrain,
      mentionSum: packageAudit.mentionSum,
      comparableSum: packageAudit.comparableSum,
      spMatchesFrozen:
        frozenIds.length > 0 &&
        Number(certifiedSpEligible) === frozenIds.length,
    },
    repair: {
      repaired: repair.repaired === true,
      reason: repair.reason,
      afterGrainPass: repair.auditAfter?.pass === true,
      afterClientSafe: repair.auditAfter?.pass === true,
    },
  };

  // Cost estimate only meaningful for Group C — Core remonitor of scenario universe
  const distinctScenarios = new Set(obs.map((o) => o.scenarioId).filter(Boolean))
    .size;
  const coreScenarios = Math.max(distinctScenarios, frozenIds.length, 48);
  row.costIfRemeasureCoreUsd = estimateCoreRemonitorCost(coreScenarios);

  row.classification = classifyHotel(row);
  rows.push(row);
}

const byClass = {
  A: rows.filter((r) => r.classification.class === CLASS.A),
  B: rows.filter((r) => r.classification.class === CLASS.B),
  C: rows.filter((r) => r.classification.class === CLASS.C),
};

const groupCBudget = byClass.C.map((r) => ({
  propertyId: r.propertyId,
  propertyName: r.propertyName,
  reasons: r.classification.reasons,
  estimatedCoreUsd: r.costIfRemeasureCoreUsd.estimatedCoreUsd,
  hardMaxSuggestedUsd: r.costIfRemeasureCoreUsd.hardMaxSuggestedUsd,
  coreScenarios: r.costIfRemeasureCoreUsd.coreScenarios,
  expectedCoreResponses: r.costIfRemeasureCoreUsd.expectedCoreResponses,
  providerUnitCosts: Object.fromEntries(
    Object.entries(PROVIDER_CONFIGS).map(([k, v]) => [k, v.costPerCall])
  ),
}));

const minPaidBudget = {
  hotels: groupCBudget.length,
  estimatedCoreUsdTotal: Number(
    groupCBudget
      .reduce((s, r) => s + Number(r.estimatedCoreUsd || 0), 0)
      .toFixed(2)
  ),
  hardMaxSuggestedUsdTotal: Number(
    groupCBudget
      .reduce((s, r) => s + Number(r.hardMaxSuggestedUsd || 0), 0)
      .toFixed(2)
  ),
  note:
    "Core-only remonitor estimate from PROVIDER_CONFIGS. Excludes Loyalty/BPP peer research unless separately required. Invoice may differ.",
  pricingScheduleVersion: "PROVIDER_CONFIGS_v1",
};

const summary = {
  assessedAt: new Date().toISOString(),
  contract: "ADP_MINIMUM_NECESSARY_REMEASUREMENT_ASSESSMENT_V1",
  hotelCount: rows.length,
  counts: {
    A_FULLY_VALID: byClass.A.length,
    B_REPROCESS_EXISTING_EVIDENCE: byClass.B.length,
    C_FRESH_MEASUREMENT_REQUIRED: byClass.C.length,
  },
  groupA: byClass.A.map((r) => r.propertyId),
  groupB: byClass.B.map((r) => r.propertyId),
  groupC: byClass.C.map((r) => r.propertyId),
  evidenceAvailability: {
    periodsLoaded: rows.filter((r) => r.evidence.periodLoaded).length,
    rawTextGe95: rows.filter((r) => r.evidence.rawTextCoveragePct >= 95).length,
    frozenIdsPresent: rows.filter((r) => r.frozen.eligibleScenarioIdsCount > 0)
      .length,
    considerationReproducedStored: rows.filter(
      (r) => r.reproduction.stored.considerationMatchesCertified
    ).length,
    considerationReproducedReadjudicated: rows.filter(
      (r) => r.reproduction.readjudicated.considerationMatchesCertified
    ).length,
  },
  minimumPaidResearchBudget: minPaidBudget,
  preserve: {
    publishedReports: "PRESERVE_ALL_38",
    pdfs: "PRESERVE",
    historicalRecords: "PRESERVE",
    externalShareLinks: "PRESERVE_NO_TOKEN_ROTATION",
  },
  stopForFounderApproval: true,
  noPaidResearchExecuted: true,
  noAirtableMutations: true,
  noRepublish: true,
  noProductionDeploy: true,
};

writeFileSync(join(OUT, "01-hotel-by-hotel.json"), JSON.stringify(rows, null, 2));
writeFileSync(
  join(OUT, "02-summary.json"),
  JSON.stringify({ summary, groupCBudget }, null, 2)
);

const md = `# ADP — Minimum Necessary Remeasurement Assessment

**STOP for founder approval.** No paid research, Airtable mutations, republishing, or production deployment executed.

Assessed: **${rows.length}** certified hotels · ${summary.assessedAt}

## Classification counts

| Class | Count | Meaning |
|-------|------:|---------|
| A — FULLY VALID | ${byClass.A.length} | Measurement + report correct; no changes |
| B — REPROCESS EXISTING EVIDENCE | ${byClass.B.length} | Research valid; derived metrics/presentation need correction; **$0 paid** |
| C — FRESH MEASUREMENT REQUIRED | ${byClass.C.length} | Evidence missing/invalid/insufficient |

## Evidence availability (all 38)

- Period JSON loaded: **${summary.evidenceAvailability.periodsLoaded}/38**
- Raw response text ≥95%: **${summary.evidenceAvailability.rawTextGe95}/38**
- Frozen eligibleScenarioIds present: **${summary.evidenceAvailability.frozenIdsPresent}/38**
- Certified Consideration reproduced from stored mentions: **${summary.evidenceAvailability.considerationReproducedStored}/38**
- Certified Consideration reproduced after response-text readjudication: **${summary.evidenceAvailability.considerationReproducedReadjudicated}/38**

## Group A — FULLY VALID (${byClass.A.length})

${byClass.A.map((r) => `- \`${r.propertyId}\``).join("\n") || "_None_"}

## Group B — REPROCESS EXISTING EVIDENCE (${byClass.B.length})

Research is reusable. Corrected immutable packages should be built from existing responses (frozen eligible ids + Provider/Scenario Presence rebuilt to match Consideration). **No paid measurement.**

${byClass.B.map((r) => `- \`${r.propertyId}\` — ${r.classification.reasons.join(", ")}`).join("\n") || "_None_"}

### Group B publication plan (proposed — not executed)

1. For each B hotel: rebuild Provider Presence (+ align Scenario Presence eligible) from period observations filtered to frozen \`eligibleScenarioIds\`.
2. Stage corrected owner payload; run grain audit + challenger Consideration match + browser/PDF gates.
3. Publish as **new immutable version** under durable store; keep prior version as historical.
4. Point \`current\` to new version; **preserve existing share tokens** (\`reportScope=current_published\`).
5. Retain PDFs for prior versions; generate new PDF for corrected current.
6. Do **not** mutate Airtable until a separate history backfill authorization.

## Group C — FRESH MEASUREMENT REQUIRED (${byClass.C.length})

${byClass.C.length === 0 ? "_None — no hotel requires paid remonitor under this assessment._" : byClass.C.map((r) => `### ${r.propertyName} (\`${r.propertyId}\`)
- Why reuse fails: ${r.classification.reasons.join(", ")}
- Evidence: periodLoaded=${r.evidence.periodLoaded}, obs=${r.evidence.obsCount}, rawText=${r.evidence.rawTextCoveragePct}%
- Consideration reproduce (stored): ${r.reproduction.stored.considerationMatchesCertified}; (readjudicated): ${r.reproduction.readjudicated.considerationMatchesCertified}
- Estimated Core remonitor: **$${r.costIfRemeasureCoreUsd.estimatedCoreUsd}** (hard max ~$${r.costIfRemeasureCoreUsd.hardMaxSuggestedUsd}; ${r.costIfRemeasureCoreUsd.coreScenarios} scenarios × 4 providers)
`).join("\n")}

## Minimum paid research budget (Group C only)

| Metric | Value |
|--------|------:|
| Group C hotels | ${minPaidBudget.hotels} |
| Estimated Core USD total | $${minPaidBudget.estimatedCoreUsdTotal} |
| Hard max suggested USD total | $${minPaidBudget.hardMaxSuggestedUsdTotal} |

${minPaidBudget.note}

Provider unit costs: ${JSON.stringify(Object.fromEntries(Object.entries(PROVIDER_CONFIGS).map(([k,v])=>[k,v.costPerCall])))}

## Historical version plan (all classes)

- Preserve all 38 currently published reports, PDFs, historical records, and external links.
- Group A: no publication change.
- Group B: additive corrected current version; prior remains historical.
- Group C (if approved later): new measurement period → new certified package → new current; prior published packages remain historical; shares keep resolving to current.

## Founder decision needed

1. Authorize Group B reprocess + corrected publication (still no paid research)?
2. If any Group C: authorize paid Core remonitor budget up to hard max?
3. Confirm Loyalty/BPP peer research remains out of scope unless separately approved.

**STOPPED for founder approval.**
`;

writeFileSync(join(OUT, "FOUNDER_ASSESSMENT.md"), md);
writeFileSync(
  join(OUT, "03-hotel-classification-table.csv"),
  [
    "propertyId,class,consideration,frozenIds,rawTextPct,considerationReproStored,considerationReproAdj,packageGrainPass,spMatchesFrozen,repairPass,estCoreUsdIfC,reasons",
    ...rows.map((r) =>
      [
        r.propertyId,
        r.classification.class,
        r.certified.considerationRate,
        r.frozen.eligibleScenarioIdsCount,
        r.evidence.rawTextCoveragePct,
        r.reproduction.stored.considerationMatchesCertified,
        r.reproduction.readjudicated.considerationMatchesCertified,
        r.package.grainPass,
        r.package.spMatchesFrozen,
        r.repair.afterGrainPass,
        r.classification.class === CLASS.C
          ? r.costIfRemeasureCoreUsd.estimatedCoreUsd
          : 0,
        `"${r.classification.reasons.join("|")}"`,
      ].join(",")
    ),
  ].join("\n")
);

console.log(JSON.stringify(summary, null, 2));
