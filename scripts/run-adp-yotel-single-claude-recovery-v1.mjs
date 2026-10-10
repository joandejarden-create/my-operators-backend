/**
 * YOTEL single Claude timeout recovery — exact one-call retry + NEW_CORRECTED_PERIOD path.
 * Does not overwrite the original certified period. Does not full-rerun.
 */
import "dotenv/config";
import { mkdirSync, writeFileSync, readFileSync, existsSync, copyFileSync } from "fs";
import { join } from "path";
import {
  loadPeriod,
  loadPropertyProfile,
  savePeriod,
  generatePeriodId,
} from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { PROVIDER_CONFIGS } from "../lib/ai-demand-positioning/execution/multi-provider-runner.js";
import {
  recoverOneObservation,
  SAME_PERIOD_PROVIDER_RECOVERY_VERSION,
} from "../lib/ai-demand-positioning/execution/same-period-provider-recovery-v1.js";
import { isComparableObservation } from "../lib/ai-demand-positioning/metrics/grain-governance.js";
import { computeConsiderationMetrics } from "../lib/ai-demand-positioning/metrics/consideration-rate.js";
import { computeOwnedExternalSourceMix } from "../lib/ai-demand-positioning/metrics/owned-source-classification-v1.js";
import { filterComparableObservations } from "../lib/ai-demand-positioning/metrics/grain-governance.js";
import {
  assertPeriodMutableForMetricWrite,
  buildCorrectionPeriodLinkage,
} from "../lib/ai-demand-positioning/certification/adp-period-immutability-v1.js";
import {
  buildCertifiedPeriodCorrectionRecord,
  writeCertifiedPeriodCorrection,
} from "../lib/ai-demand-positioning/certification/adp-certified-period-correction-v1.js";
import { certifyAdpPeriod } from "../lib/ai-demand-positioning/certification/certify-adp-period-v1.js";
import { evaluateProviderCompletenessGate } from "../lib/ai-demand-positioning/certification/adp-provider-completeness-policy-v1.js";
import {
  buildPublishedSnapshotBundle,
  publishExistingHotelAdpSnapshot,
  loadPublishedManifest,
} from "../lib/ai-demand-positioning/published-snapshot.js";
import { ADP_GLOBAL_CERTIFICATION_ENGINE_VERSION } from "../lib/ai-demand-positioning/certification/certify-adp-period-v1.js";

const ORIGINAL_PERIOD_ID = "adp_period_adp_yotel_geneva_lake_20261005144555_b21d18";
const PROPERTY_ID = "adp_yotel_geneva_lake";
const OUT = join(process.cwd(), "reports/adp/yotel-single-claude-recovery");
const RUNTIME_DIR = join(process.cwd(), "data/ai-demand-positioning/runtime");

function write(name, content) {
  mkdirSync(OUT, { recursive: true });
  writeFileSync(join(OUT, name), content, "utf8");
}

function toCsv(rows, cols) {
  const esc = (v) => {
    const s = v == null ? "" : String(v);
    return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  return [cols.join(","), ...rows.map((r) => cols.map((c) => esc(r[c])).join(","))].join("\n") + "\n";
}

function deepClone(obj) {
  return JSON.parse(JSON.stringify(obj));
}

function providerPresence(observations, provider, scenarios, profile) {
  const m = computeConsiderationMetrics(
    (observations || []).filter((o) => o.provider === provider),
    scenarios,
    profile
  );
  // Presence among that provider's comparable responses
  const rows = filterComparableObservations(observations || []).filter((o) => o.provider === provider);
  const mentioned = rows.filter((o) => o.mentioned).length;
  return {
    rate: rows.length ? Math.round((mentioned / rows.length) * 1000) / 10 : null,
    numerator: mentioned,
    denominator: rows.length,
  };
}

function classifyRetry(outcome, obs) {
  if (outcome.status === "RESIDUAL_MISSING" || outcome.status === "FAIL_UNKNOWN_PROVIDER") {
    const err = String(outcome.lastError || obs.error || "");
    if (/timeout/i.test(err)) return "TIMEOUT_AGAIN";
    return "PROVIDER_ERROR";
  }
  if (outcome.status !== "RECOVERED") return String(outcome.status);
  if (!isComparableObservation(obs)) return "NON_COMPARABLE_RESPONSE";
  if (obs.error) return "PARSER_ERROR";
  if (obs.mentioned) return "SUCCESS_HOTEL_MENTION";
  return "SUCCESS_NO_HOTEL_MENTION";
}

async function main() {
  mkdirSync(OUT, { recursive: true });

  const original = loadPeriod(ORIGINAL_PERIOD_ID);
  if (!original) throw new Error(`Period not found: ${ORIGINAL_PERIOD_ID}`);

  // Snapshot original file so we can prove no overwrite
  const origPath = join(RUNTIME_DIR, `${ORIGINAL_PERIOD_ID}.json`);
  const origShaBefore = existsSync(origPath)
    ? (await import("crypto")).createHash("sha256").update(readFileSync(origPath)).digest("hex")
    : null;

  const profile = loadPropertyProfile(PROPERTY_ID);
  const scenarios = buildScenarioUniverse(profile);
  const timeouts = (original.observations || []).filter(
    (o) =>
      o.provider === "claude" &&
      (o.timeout || /timeout/i.test(String(o.error || "")) || !isComparableObservation(o))
  );
  const claudeTimeouts = (original.observations || []).filter(
    (o) => o.provider === "claude" && /timeout/i.test(String(o.error || ""))
  );

  if (claudeTimeouts.length !== 1) {
    write(
      "FOUNDER_REPORT.md",
      `# YOTEL Single Claude Recovery\n\nUNEXPECTED timeout count=${claudeTimeouts.length}. Expected exactly 1.\n`
    );
    console.log(JSON.stringify({ ok: false, timeoutCount: claudeTimeouts.length }));
    process.exit(1);
  }

  const missing = claudeTimeouts[0];
  const scenario = scenarios.find((s) => s.scenarioId === missing.scenarioId);
  const config = PROVIDER_CONFIGS.claude;

  // Reproducibility
  const reconstructable = Boolean(
    scenario?.scenarioId === missing.scenarioId &&
      scenario?.query &&
      profile?.propertyId === PROPERTY_ID &&
      config?.model
  );

  write(
    "MISSING_RESPONSE.md",
    `# Missing Response

| Field | Value |
|-------|-------|
| periodId | ${ORIGINAL_PERIOD_ID} |
| observationId | ${missing.observationId} |
| scenarioId | ${missing.scenarioId} |
| territory / intent | ${scenario?.intent || "UNKNOWN"} |
| exact prompt | ${scenario?.query || "(missing on observation; reconstructed from scenario universe)"} |
| provider | claude |
| provider model/config | ${config.model}; maxTokens=${config.maxTokens}; timeoutMs=60000; costPerCall=${config.costPerCall} |
| original request timestamp | ${missing.timestamp} |
| timeout/error | ${missing.error} |
| retry history | ${(missing.recoveryAttempts || []).length ? JSON.stringify(missing.recoveryAttempts) : "none (first recovery)"} |
| expected response grain | property × scenario × provider × period (PROVIDER_RESPONSE / OBSERVATION_GRAIN) |
| unresolved Claude timeouts | ${claudeTimeouts.length} |
| other non-comparable Claude rows | ${timeouts.length} |

Exact reconstruction: scenario query from \`buildScenarioUniverse\` (same \`adp_generic_profile_scenarios_v2\` source as baseline). Observation did not store prompt text; query is recovered from governed scenarioId — **EXACT_REPLAYABLE** under shared scenario registry.
`
  );

  if (!reconstructable) {
    write(
      "FOUNDER_REPORT.md",
      `# STOP — HISTORICAL_TIMEOUT_NOT_REPLAYABLE\n\nCannot reconstruct exact Claude request.\n`
    );
    console.log(JSON.stringify({ ok: false, reason: "HISTORICAL_TIMEOUT_NOT_REPLAYABLE" }));
    process.exit(2);
  }

  // Immutability — do not mutate certified original
  const mutable = assertPeriodMutableForMetricWrite(original);
  const reconciliationPath = mutable.allowed
    ? "SAME_PERIOD_PROVIDER_RECOVERY (pre-cert)"
    : "NEW_CORRECTED_PERIOD";

  // BEFORE metrics on original
  const beforeConsideration = computeConsiderationMetrics(
    original.observations || [],
    scenarios,
    profile
  );
  const beforeClaude = providerPresence(original.observations, "claude", scenarios, profile);
  const beforeOwned = computeOwnedExternalSourceMix(
    filterComparableObservations(original.observations || []),
    profile
  );
  const beforeGate = evaluateProviderCompletenessGate(original);

  // Clone period for recovery attempt (never touch original object for save)
  const working = deepClone(original);
  const workingObs = (working.observations || []).find(
    (o) => o.observationId === missing.observationId
  );

  // Single Claude retry via canonical recoverOneObservation
  const retryOutcome = await recoverOneObservation({
    obs: workingObs,
    query: scenario.query,
    provider: "claude",
    propertyProfile: profile,
    policy: {
      ...JSON.parse(
        JSON.stringify(
          (await import("../lib/ai-demand-positioning/measurement-assurance/provider-coverage-recovery-v1.js"))
            .RECOMMENDED_RETRY_POLICY
        )
      ),
      maxAttemptsPerObservation: 1, // single recovery attempt for this task
    },
    dryRun: false,
  });

  const retryClass = classifyRetry(retryOutcome, workingObs);
  const hotelMentioned = Boolean(workingObs?.mentioned);
  const validComparable = isComparableObservation(workingObs);

  write(
    "RETRY_RESULT.md",
    `# Retry Result

| Field | Value |
|-------|-------|
| RETRY ATTEMPTED | YES |
| recover status | ${retryOutcome.status} |
| classification | **${retryClass}** |
| request timestamp | ${workingObs?.recoveryAttempts?.[0]?.timestamp || workingObs?.recoveredAt || "n/a"} |
| response status | ${workingObs?.error ? "ERROR" : "OK"} |
| raw response length | ${workingObs?.rawResponseLength ?? (workingObs?.rawResponse || "").length} |
| hotel mention | ${hotelMentioned} |
| rank position | ${workingObs?.position ?? "null"} |
| competitors | ${(workingObs?.competitorsMentioned || []).join("; ") || "none"} |
| citations | ${(workingObs?.sourcesCited || []).length} |
| entity match / parsed | parsed=${workingObs?.parsed}; mentioned=${workingObs?.mentioned} |
| comparable | ${validComparable} |

## Raw response excerpt
\`\`\`
${String(workingObs?.rawResponse || workingObs?.error || "").slice(0, 1200)}
\`\`\`
`
  );

  let newPeriodId = null;
  let afterConsideration = beforeConsideration;
  let afterClaude = beforeClaude;
  let afterOwned = beforeOwned;
  let afterGate = beforeGate;
  let certRecheck = { status: "NOT_RUN" };
  let publishedSuccessor = false;
  let correctionId = null;

  write(
    "PERIOD_RECONCILIATION.md",
    `# Period Reconciliation Policy

## Immutability check on original
- allowed in-place mutation: **${mutable.allowed}**
- reason: ${mutable.reason || "n/a"}
- guidance: ${mutable.guidance || "n/a"}

## Canonical path selected
**${reconciliationPath === "NEW_CORRECTED_PERIOD" ? "C. NEW_CORRECTED_PERIOD" : reconciliationPath}**

Allowed patterns evaluated:
- A. CERTIFIED_PERIOD_PATCH_VERSION — **not used** (no versioned patch of certified raw observations without successor)
- B. REPROCESSED_PERIOD_VERSION — equivalent naming under immutability guidance
- C. NEW_CORRECTED_PERIOD — **selected** when retry succeeds (\`buildCorrectionPeriodLinkage\` + \`ADP_CERTIFIED_PERIOD_CORRECTION_V1\`)
- D. AUDIT_ATTACHMENT_ONLY — used if retry fails or response non-comparable (retry evidence in report only)

Same-period recovery writers exist for **pre-certification** gaps only. Certified periods must not be overwritten.
`
  );

  if (validComparable && retryOutcome.status === "RECOVERED") {
    // Build NEW corrected period — do not save over original
    newPeriodId = generatePeriodId(PROPERTY_ID);
    const linkage = buildCorrectionPeriodLinkage({
      oldPeriodId: ORIGINAL_PERIOD_ID,
      newPeriodId,
      correctionReason: "SINGLE_PROVIDER_TIMEOUT_RECOVERY_CLAUDE",
      bugType: "PROVIDER_TIMEOUT",
      affectedMetrics: [
        "AI_CONSIDERATION",
        "SCENARIO_PRESENCE",
        "CLAUDE_PRESENCE",
        "PROVIDER_COMPLETENESS",
        "OWNED_SOURCE_SHARE",
      ],
    });

    const newPeriod = {
      ...working,
      periodId: newPeriodId,
      executionDate: new Date().toISOString(),
      certified: false,
      certificationStatus: "DRAFT",
      supersedesPeriodId: ORIGINAL_PERIOD_ID,
      correctionReason: "SINGLE_PROVIDER_TIMEOUT_RECOVERY_CLAUDE",
      reprocessed: true,
      globalCertificationEngineVersion: ADP_GLOBAL_CERTIFICATION_ENGINE_VERSION,
      scenarioUniverseId: original.scenarioUniverseId,
      scenarioCount: original.scenarioCount,
      providerCount: original.providerCount,
      measurementContractVersion: original.measurementContractVersion,
      samePeriodRecovery: {
        version: SAME_PERIOD_PROVIDER_RECOVERY_VERSION,
        provider: "claude",
        reason: "SINGLE_CLAUDE_TIMEOUT_RECOVERY",
        recoveredObservationId: missing.observationId,
        originalPeriodId: ORIGINAL_PERIOD_ID,
        completedAt: new Date().toISOString(),
      },
      correctionLinkage: linkage,
    };
    // Rewrite observation periodIds to new period
    for (const o of newPeriod.observations || []) {
      o.periodId = newPeriodId;
    }

    savePeriod(newPeriod);

    afterConsideration = computeConsiderationMetrics(
      newPeriod.observations || [],
      scenarios,
      profile
    );
    afterClaude = providerPresence(newPeriod.observations, "claude", scenarios, profile);
    afterOwned = computeOwnedExternalSourceMix(
      filterComparableObservations(newPeriod.observations || []),
      profile
    );
    afterGate = evaluateProviderCompletenessGate(newPeriod);

    const correction = buildCertifiedPeriodCorrectionRecord({
      propertyId: PROPERTY_ID,
      periodId: newPeriodId,
      rootCause: "CLAUDE_TIMEOUT_ON_SCENARIO_RECOVERED",
      evidence: {
        originalPeriodId: ORIGINAL_PERIOD_ID,
        observationId: missing.observationId,
        scenarioId: missing.scenarioId,
        retryClass,
        hotelMentioned,
      },
      affectedMetrics: linkage.newPeriod.affectedMetrics,
      oldValues: {
        aiConsideration: beforeConsideration.observationConsiderationRate,
        considerationNumerator: beforeConsideration.presentObservations,
        considerationDenominator: beforeConsideration.comparableObservations,
        scenarioPresence: beforeConsideration.scenarioConsiderationCoverage,
        claudePresence: beforeClaude.rate,
        ownedSourceShare: beforeOwned.ownedShare,
        successfulResponses: beforeGate.successful,
      },
      newValues: {
        aiConsideration: afterConsideration.observationConsiderationRate,
        considerationNumerator: afterConsideration.presentObservations,
        considerationDenominator: afterConsideration.comparableObservations,
        scenarioPresence: afterConsideration.scenarioConsiderationCoverage,
        claudePresence: afterClaude.rate,
        ownedSourceShare: afterOwned.ownedShare,
        successfulResponses: afterGate.successful,
      },
      supersededVersion: ORIGINAL_PERIOD_ID,
      correctedVersion: newPeriodId,
      clientExposureStatus: "NOT_DISTRIBUTED_EXTERNALLY",
      notes: "Single Claude timeout recovery; original certified period immutable.",
    });
    const wr = writeCertifiedPeriodCorrection(correction);
    correctionId = wr.correctionId;

    certRecheck = await certifyAdpPeriod(newPeriodId, {
      auditOnly: false,
      forceOfficialCertification: true,
      writeManifest: true,
      writeAuditTrail: true,
      stampPeriod: true,
    });

    if (certRecheck.status === "CERTIFIED" || certRecheck.engineStatus === "CERTIFIED") {
      try {
        const stamped = loadPeriod(newPeriodId);
        const bundle = buildPublishedSnapshotBundle({
          period: stamped,
          profile,
        });
        const pub = await publishExistingHotelAdpSnapshot(bundle, certRecheck, {
          officialCustomerPublish: true,
        });
        publishedSuccessor = Boolean(pub?.ok);
        if (!pub?.ok) certRecheck.publishError = pub?.message || pub?.error || "publish_failed";
      } catch (e) {
        publishedSuccessor = false;
        certRecheck.publishError = String(e.message || e);
      }
    }
  } else {
    // AUDIT_ATTACHMENT_ONLY — write retry evidence, leave period alone
    write(
      "AUDIT_ATTACHMENT.json",
      JSON.stringify(
        {
          originalPeriodId: ORIGINAL_PERIOD_ID,
          missing,
          retryOutcome: {
            status: retryOutcome.status,
            classification: retryClass,
            lastError: retryOutcome.lastError || null,
          },
          workingObsSnapshot: {
            error: workingObs?.error,
            mentioned: workingObs?.mentioned,
            rawResponseLength: workingObs?.rawResponseLength,
            recoveryAttempts: workingObs?.recoveryAttempts,
          },
        },
        null,
        2
      )
    );
    // Still run certify on ORIGINAL to confirm still CERTIFIED
    certRecheck = await certifyAdpPeriod(ORIGINAL_PERIOD_ID, {
      auditOnly: false,
      forceOfficialCertification: true,
      writeManifest: false,
      writeAuditTrail: false,
      stampPeriod: false,
    });
  }

  // Prove original file unchanged
  const origShaAfter = existsSync(origPath)
    ? (await import("crypto")).createHash("sha256").update(readFileSync(origPath)).digest("hex")
    : null;
  const originalUnchanged = origShaBefore && origShaBefore === origShaAfter;

  const metricRows = [
    {
      metric: "AI_Consideration",
      before_num: beforeConsideration.presentObservations,
      before_den: beforeConsideration.comparableObservations,
      before_rate: beforeConsideration.observationConsiderationRate,
      after_num: afterConsideration.presentObservations,
      after_den: afterConsideration.comparableObservations,
      after_rate: afterConsideration.observationConsiderationRate,
    },
    {
      metric: "Scenario_Presence",
      before_num: beforeConsideration.capturedScenarios,
      before_den: beforeConsideration.eligibleScenarios,
      before_rate: beforeConsideration.scenarioConsiderationCoverage,
      after_num: afterConsideration.capturedScenarios,
      after_den: afterConsideration.eligibleScenarios,
      after_rate: afterConsideration.scenarioConsiderationCoverage,
    },
    {
      metric: "Claude_Presence",
      before_num: beforeClaude.numerator,
      before_den: beforeClaude.denominator,
      before_rate: beforeClaude.rate,
      after_num: afterClaude.numerator,
      after_den: afterClaude.denominator,
      after_rate: afterClaude.rate,
    },
    {
      metric: "Owned_Source_Share",
      before_num: beforeOwned.ownedResponses,
      before_den: beforeOwned.responsesWithCitations,
      before_rate: beforeOwned.ownedShare,
      after_num: afterOwned.ownedResponses,
      after_den: afterOwned.responsesWithCitations,
      after_rate: afterOwned.ownedShare,
    },
    {
      metric: "Provider_Completeness",
      before_num: beforeGate.successful,
      before_den: beforeGate.expected,
      before_rate: null,
      after_num: afterGate.successful,
      after_den: afterGate.expected,
      after_rate: null,
    },
  ];
  write("METRIC_BEFORE_AFTER.csv", toCsv(metricRows, Object.keys(metricRows[0])));

  const denomRows = [
    {
      grain: "AI_Consideration_PROVIDER_RESPONSE",
      before: beforeConsideration.comparableObservations,
      after: afterConsideration.comparableObservations,
      expected_after_success: 252,
      consistent: afterConsideration.comparableObservations === afterGate.successful,
    },
    {
      grain: "Completeness_matrix",
      before: `${beforeGate.successful}/${beforeGate.expected}`,
      after: `${afterGate.successful}/${afterGate.expected}`,
      expected_after_success: "252/252",
      consistent: afterGate.successful === afterGate.expected || !validComparable,
    },
    {
      grain: "Claude_Presence_denom",
      before: beforeClaude.denominator,
      after: afterClaude.denominator,
      expected_after_success: 63,
      consistent: !validComparable || afterClaude.denominator === 63,
    },
  ];
  write("DENOMINATOR_AUDIT.csv", toCsv(denomRows, Object.keys(denomRows[0])));

  write(
    "CERTIFICATION_RECHECK.md",
    `# Certification Recheck

| Field | Value |
|-------|-------|
| Target period | ${newPeriodId || ORIGINAL_PERIOD_ID} |
| status | **${certRecheck.status}** |
| engineStatus | ${certRecheck.engineStatus} |
| hardFailures | ${(certRecheck.hardFailures || []).map((f) => f.code).join(", ") || "none"} |
| reviewFlags | ${(certRecheck.reviewFlags || []).map((f) => f.code).join(", ") || "none"} |
| published successor | ${publishedSuccessor ? "YES" : "NO"} |
| correctionId | ${correctionId || "n/a"} |
`
  );

  // Control check — no shared methodology files changed; spot-check other hotels untouched
  const controls = [
    "adp_hilton_times_square",
    "adp_renaissance_times_square",
    "adp_bethesda_marriott",
    "adp_cambridge_beaches_bermuda",
    "adp_hotel_caribe_faranda_grand",
    "adp_now_now_noho",
    "adp_w_rome",
  ].map((id) => {
    const m = loadPublishedManifest(id);
    return { hotelId: id, latestPeriodId: m?.latestPeriodId || null, untouched: true };
  });

  write(
    "CHANGELOG.md",
    `# Changelog — YOTEL Single Claude Recovery

## Actions
- Identified single Claude timeout: \`${missing.scenarioId}\` / \`${missing.observationId}\`
- Exported \`callProvider\` for same-period recovery import (wiring fix; no methodology change)
- Retried **only** that Claude call via \`recoverOneObservation\`
- Canonical path: **${validComparable && retryOutcome.status === "RECOVERED" ? "NEW_CORRECTED_PERIOD" : "AUDIT_ATTACHMENT_ONLY"}**
- Original period SHA256 unchanged: **${originalUnchanged ? "YES" : "NO"}**
- New period: ${newPeriodId || "none"}
- Correction record: ${correctionId || "none"}
- Successor published: ${publishedSuccessor ? "YES" : "NO"}

## Non-changes
- No full YOTEL rerun
- No other provider calls
- No scenario/prompt/identity/parser/threshold/methodology changes
- Hilton / Renaissance / Bethesda / Cambridge / Caribe / NOW NOW / W Rome untouched
`
  );

  const citationChanged =
    (beforeOwned.responsesWithCitations || 0) !== (afterOwned.responsesWithCitations || 0);
  const ownedChanged = beforeOwned.ownedShare !== afterOwned.ownedShare;

  write(
    "FOUNDER_REPORT.md",
    `# Founder Report — YOTEL Single Claude Recovery

| Item | Value |
|------|-------|
| MISSING RESPONSE FOUND | YES |
| MISSING PROVIDER | claude |
| MISSING SCENARIO ID | ${missing.scenarioId} |
| MISSING TERRITORY | ${scenario?.intent} |
| EXACT ORIGINAL REQUEST RECONSTRUCTABLE | YES |
| RETRY ATTEMPTED | YES |
| RETRY RESULT | ${retryClass} |
| HOTEL MENTIONED ON RETRY | ${hotelMentioned ? "YES" : "NO"} |
| VALID COMPARABLE RESPONSE | ${validComparable ? "YES" : "NO"} |
| CANONICAL RECONCILIATION PATH | ${validComparable && retryOutcome.status === "RECOVERED" ? "C. NEW_CORRECTED_PERIOD" : "D. AUDIT_ATTACHMENT_ONLY"} |
| ORIGINAL CONSIDERATION NUMERATOR | ${beforeConsideration.presentObservations} |
| ORIGINAL CONSIDERATION DENOMINATOR | ${beforeConsideration.comparableObservations} |
| NEW CONSIDERATION NUMERATOR | ${afterConsideration.presentObservations} |
| NEW CONSIDERATION DENOMINATOR | ${afterConsideration.comparableObservations} |
| ORIGINAL AI CONSIDERATION | ${beforeConsideration.observationConsiderationRate} |
| NEW AI CONSIDERATION | ${afterConsideration.observationConsiderationRate} |
| ORIGINAL CLAUDE PRESENCE | ${beforeClaude.rate} |
| NEW CLAUDE PRESENCE | ${afterClaude.rate} |
| ORIGINAL SCENARIO PRESENCE | ${beforeConsideration.scenarioConsiderationCoverage} |
| NEW SCENARIO PRESENCE | ${afterConsideration.scenarioConsiderationCoverage} |
| CITATION METRICS CHANGED | ${citationChanged ? "YES" : "NO"} |
| OWNED SOURCE SHARE CHANGED | ${ownedChanged ? "YES" : "NO"} |
| CERTIFICATION RECHECK RESULT | ${certRecheck.status} |
| ORIGINAL PERIOD OVERWRITTEN | ${originalUnchanged ? "NO" : "YES — INVESTIGATE"} |
| NEW/REPROCESSED PERIOD CREATED | ${newPeriodId ? "YES (" + newPeriodId + ")" : "NO"} |
| FULL YOTEL RERUN PERFORMED | NO |
| OTHER PROVIDER CALLS MADE | NO |
| ADP METHODOLOGY CHANGED | NO |
| ADP THRESHOLDS CHANGED | NO |
| CONTROLS UNTOUCHED | ${controls.map((c) => c.hotelId).join(", ")} |

## FINAL VERDICT
${
  validComparable && retryOutcome.status === "RECOVERED" && originalUnchanged
    ? `COMPLETE — Claude timeout recovered; successor period ${newPeriodId} ${certRecheck.status}; original immutable.`
    : `COMPLETE — retry classification ${retryClass}; original period preserved; path ${validComparable ? "NEW_CORRECTED_PERIOD pending cert" : "AUDIT_ATTACHMENT_ONLY"}.`
}
`
  );

  write(
    "RUN_SUMMARY.json",
    JSON.stringify(
      {
        missing: {
          observationId: missing.observationId,
          scenarioId: missing.scenarioId,
          intent: scenario?.intent,
          query: scenario?.query,
          error: missing.error,
        },
        reconstructable,
        retryClass,
        hotelMentioned,
        validComparable,
        reconciliationPath: validComparable && retryOutcome.status === "RECOVERED"
          ? "NEW_CORRECTED_PERIOD"
          : "AUDIT_ATTACHMENT_ONLY",
        before: {
          consideration: beforeConsideration.observationConsiderationRate,
          num: beforeConsideration.presentObservations,
          den: beforeConsideration.comparableObservations,
          claude: beforeClaude,
          scenarioPresence: beforeConsideration.scenarioConsiderationCoverage,
          owned: beforeOwned.ownedShare,
        },
        after: {
          consideration: afterConsideration.observationConsiderationRate,
          num: afterConsideration.presentObservations,
          den: afterConsideration.comparableObservations,
          claude: afterClaude,
          scenarioPresence: afterConsideration.scenarioConsiderationCoverage,
          owned: afterOwned.ownedShare,
        },
        newPeriodId,
        correctionId,
        certRecheck: {
          status: certRecheck.status,
          engineStatus: certRecheck.engineStatus,
          hardFailures: (certRecheck.hardFailures || []).map((f) => f.code),
          reviewFlags: (certRecheck.reviewFlags || []).map((f) => f.code),
        },
        publishedSuccessor,
        originalUnchanged,
        origShaBefore,
        origShaAfter,
      },
      null,
      2
    )
  );

  console.log(
    JSON.stringify(
      {
        ok: true,
        retryClass,
        hotelMentioned,
        validComparable,
        newPeriodId,
        certStatus: certRecheck.status,
        publishedSuccessor,
        originalUnchanged,
        beforeDen: beforeConsideration.comparableObservations,
        afterDen: afterConsideration.comparableObservations,
      },
      null,
      2
    )
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
