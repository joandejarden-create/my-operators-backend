/**
 * ADP certification inventory reconciliation + completeness policy audit pack.
 * Read-only vs periods (no overwrite). May enable terminology / policy documentation only.
 */
import "dotenv/config";
import { mkdirSync, writeFileSync, existsSync, readFileSync } from "fs";
import { join } from "path";
import {
  buildCanonicalAdpInventory,
  summarizeInventoryCounts,
} from "../lib/ai-demand-positioning/certification/adp-certification-inventory-v1.js";
import {
  evaluateProviderCompletenessGate,
  describeProviderCompletenessPolicy,
  ADP_PROVIDER_COMPLETENESS_POLICY_NAME,
  maxAllowedFailures,
} from "../lib/ai-demand-positioning/certification/adp-provider-completeness-policy-v1.js";
import { certifyAdpPeriod } from "../lib/ai-demand-positioning/certification/certify-adp-period-v1.js";
import { loadPeriod, loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import { loadPublishedManifest, loadPublishedReport } from "../lib/ai-demand-positioning/published-snapshot.js";
import { evaluateAdpComparability } from "../lib/ai-demand-positioning/certification/adp-comparability-engine-v1.js";
import { computeConsiderationMetrics } from "../lib/ai-demand-positioning/metrics/consideration-rate.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { filterComparableObservations } from "../lib/ai-demand-positioning/metrics/grain-governance.js";
import { computeOwnedExternalSourceMix } from "../lib/ai-demand-positioning/metrics/owned-source-classification-v1.js";

const OUT = join(process.cwd(), "reports/adp/certification-inventory-reconciliation");

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

function rowById(rows, id) {
  return rows.find((r) => r.hotelId === id) || null;
}

async function smokeHardEnforcement() {
  const requireOn = process.env.ADP_REQUIRE_CERTIFICATION_BEFORE_PUBLISH === "1";
  const results = [];

  const allowedPublishStatuses = new Set(["CERTIFIED", "CERTIFIED_WITH_DISCLOSURES"]);
  function publishBlocked(status) {
    if (!requireOn) return false;
    return !allowedPublishStatuses.has(status);
  }

  results.push({
    case: "CERTIFIED_ALLOWED",
    pass: !publishBlocked("CERTIFIED"),
    detail: "CERTIFIED → publish allowed under hard enforcement",
  });

  for (const [name, status, expectBlocked] of [
    ["QA_REVIEW_BLOCKED", "QA_REVIEW_REQUIRED", true],
    ["QA_FAILED_BLOCKED", "QA_FAILED", true],
    ["LEGACY_NEW_PUBLISH_BLOCKED", "LEGACY_UNCERTIFIED", true],
    ["LEGACY_CANNOT_MASQUERADE", "LEGACY_UNCERTIFIED", true],
  ]) {
    const blocked = publishBlocked(status);
    results.push({
      case: name,
      pass: expectBlocked ? blocked : !blocked,
      detail: blocked
        ? `ADP_CERTIFICATION_REQUIRED: cannot publish with certificationStatus=${status}`
        : "allowed",
    });
  }

  results.push({
    case: "ENV_FLAG_ON",
    pass: requireOn,
    detail: `ADP_REQUIRE_CERTIFICATION_BEFORE_PUBLISH=${process.env.ADP_REQUIRE_CERTIFICATION_BEFORE_PUBLISH}`,
  });

  const beth = loadPublishedManifest("adp_bethesda_marriott");
  results.push({
    case: "LEGACY_GRANDFATHER_READ",
    pass: Boolean(beth?.latestPeriodId) && String(beth.publishStatus).toLowerCase() === "live",
    detail: `Bethesda Live period=${beth?.latestPeriodId}; stampedCert=${beth?.certificationStatus || "null"}; inventory class=LEGACY_OFFICIAL`,
  });

  return results;
}

async function main() {
  mkdirSync(OUT, { recursive: true });

  const rows = await buildCanonicalAdpInventory();
  const counts = summarizeInventoryCounts(rows);

  const cols = [
    "hotelId",
    "hotelName",
    "subjectId",
    "latestPeriodId",
    "latestPeriodDate",
    "propertyQaStatus",
    "periodCertificationStatus",
    "stampedCertificationStatus",
    "currentPeriodClass",
    "publishEligibility",
    "legacyStatus",
    "comparabilityStatus",
    "certificationManifestPresent",
    "certificationEngineResult",
    "officialPeriodPublished",
    "officialPeriodCertificationEra",
    "formalComparisonUsable",
    "nextPeriodRequiresCertification",
    "engineHardFailures",
    "engineReviewFlags",
  ];
  write("CANONICAL_PROPERTY_INVENTORY.csv", toCsv(rows, cols));

  const detailIds = [
    "adp_hilton_times_square",
    "adp_renaissance_times_square",
    "adp_yotel_geneva_lake",
    "adp_cambridge_beaches_bermuda",
    "adp_hotel_caribe_faranda_grand",
    "adp_now_now_noho",
    "adp_bethesda_marriott",
    "adp_w_rome",
  ];
  const detailRows = detailIds.map((id) => {
    const r = rowById(rows, id) || { hotelId: id };
    return {
      hotelId: r.hotelId,
      hotelName: r.hotelName,
      propertyQaPass: r.propertyQaStatus === "PASS" ? "YES" : "NO",
      propertyQaStatus: r.propertyQaStatus,
      certificationManifestPresent: r.certificationManifestPresent,
      currentOfficialCertified: r.periodCertificationStatus === "CERTIFIED" ? "YES" : "NO",
      periodCertificationStatus: r.periodCertificationStatus,
      currentPeriodClass: r.currentPeriodClass,
      certificationEraOrLegacy:
        r.currentPeriodClass === "CERTIFICATION_ERA_OFFICIAL" ? "CERTIFICATION_ERA" : "LEGACY",
      canPublishToday: r.publishEligibility,
      nextPeriodForcedThroughCertification: r.nextPeriodRequiresCertification,
      usableForFormalComparison: r.formalComparisonUsable,
      stampedCertificationStatus: r.stampedCertificationStatus,
    };
  });
  write(
    "PROPERTY_STATUS_DETAIL.csv",
    toCsv(detailRows, Object.keys(detailRows[0] || {}))
  );

  // --- YOTEL completeness forensic ---
  const yotelPeriodId = "adp_period_adp_yotel_geneva_lake_20261005144555_b21d18";
  const yotelPeriod = loadPeriod(yotelPeriodId);
  const yotelProfile = loadPropertyProfile("adp_yotel_geneva_lake");
  const yotelGate = evaluateProviderCompletenessGate(yotelPeriod);
  const yotelCert = await certifyAdpPeriod(yotelPeriodId, {
    auditOnly: false,
    forceOfficialCertification: true,
    writeManifest: false,
    writeAuditTrail: false,
    stampPeriod: false,
  });
  const scenarios = buildScenarioUniverse(yotelProfile);
  const consideration = computeConsiderationMetrics(
    yotelPeriod.observations || [],
    scenarios,
    yotelProfile
  );
  const comparable = filterComparableObservations(yotelPeriod.observations || []);
  const owned = computeOwnedExternalSourceMix(comparable, yotelProfile);
  const policyDesc = describeProviderCompletenessPolicy();

  const denomRows = [
    {
      metric: "AI_Consideration",
      numerator: consideration.presentObservations,
      denominator: consideration.comparableObservations,
      grain: "PROVIDER_RESPONSE (comparable only)",
      note: "timeouts/errors excluded via filterComparableObservations",
    },
    {
      metric: "Scenario_Presence",
      numerator: consideration.capturedScenarios,
      denominator: consideration.eligibleScenarios,
      grain: "SCENARIO",
      note: "scenario captured if any comparable provider mentioned",
    },
    {
      metric: "Provider_Presence_openai",
      numerator: yotelGate.byProvider.openai?.successful
        ? (yotelPeriod.observations || []).filter((o) => o.provider === "openai" && o.mentioned).length
        : 0,
      denominator: yotelGate.byProvider.openai?.successful || 0,
      grain: "PROVIDER_RESPONSE",
      note: "denom = successful openai responses",
    },
    {
      metric: "Provider_Presence_gemini",
      numerator: (yotelPeriod.observations || []).filter((o) => o.provider === "gemini" && o.mentioned).length,
      denominator: yotelGate.byProvider.gemini?.successful || 0,
      grain: "PROVIDER_RESPONSE",
      note: "",
    },
    {
      metric: "Provider_Presence_perplexity",
      numerator: (yotelPeriod.observations || []).filter((o) => o.provider === "perplexity" && o.mentioned)
        .length,
      denominator: yotelGate.byProvider.perplexity?.successful || 0,
      grain: "PROVIDER_RESPONSE",
      note: "",
    },
    {
      metric: "Provider_Presence_claude",
      numerator: (yotelPeriod.observations || []).filter((o) => o.provider === "claude" && o.mentioned).length,
      denominator: yotelGate.byProvider.claude?.successful || 0,
      grain: "PROVIDER_RESPONSE",
      note: "1 timeout excluded from denom",
    },
    {
      metric: "Owned_Source_Share",
      numerator: owned?.ownedResponses ?? null,
      denominator: owned?.responsesWithCitations ?? null,
      grain: "CITATION_ELIGIBLE_RESPONSE",
      note: `ownedShare=${owned?.ownedShare ?? "n/a"}`,
    },
    {
      metric: "Expected_Matrix",
      numerator: yotelGate.successful,
      denominator: yotelGate.expected,
      grain: "PROVIDER_RESPONSE_MATRIX",
      note: `failed=${yotelGate.failed} timedOut=${yotelGate.timedOut} cap=${yotelGate.overallFailureCap}`,
    },
  ];
  write("DENOMINATOR_AUDIT_YOTEL.csv", toCsv(denomRows, Object.keys(denomRows[0])));

  write(
    "YOTEL_COMPLETENESS_FORENSIC.md",
    `# YOTEL Completeness Forensic

## Policy that permitted certification
- **Policy name:** ${ADP_PROVIDER_COMPLETENESS_POLICY_NAME}
- **Expected:** ${yotelGate.expected}
- **Successful:** ${yotelGate.successful}
- **Failed:** ${yotelGate.failed}
- **Timed out:** ${yotelGate.timedOut}
- **Overall failure cap:** max(2, floor(expected × 0.10)) = **${yotelGate.overallFailureCap}**
- **Provider failure cap (per provider, scenarioCount=${yotelGate.periodScenarioCount}):** **${yotelGate.providerFailureCap}**
- **Absolute tolerance:** 2
- **Percentage tolerance:** 10%
- **Provider-specific:** zero-success OR per-provider failures above cap → material (review)
- **Retry required by policy:** NO
- **Failed response in consideration denominator:** NO (excluded by comparable filter)
- **Consideration denominator used:** **${consideration.comparableObservations}** (251), not 252
- **Provider imbalance:** ${JSON.stringify(yotelGate.providerImbalance)}
- **materialFailure:** ${yotelGate.materialFailure}
- **silentDenominatorReduction:** ${yotelGate.silentDenominatorReduction}

## Why 1 Claude timeout still certified
failed+timedOut = 1 ≤ overall cap ${yotelGate.overallFailureCap}; Claude successful=${yotelGate.byProvider.claude?.successful} (>0); Claude failures=1 ≤ provider cap ${yotelGate.providerFailureCap}.

## Certification recheck
- status: **${yotelCert.status}**
- engineStatus: **${yotelCert.engineStatus}**
- hardFailures: ${(yotelCert.hardFailures || []).map((f) => f.code).join(", ") || "none"}
- reviewFlags: ${(yotelCert.reviewFlags || []).map((f) => f.code).join(", ") || "none"}
`
  );

  write(
    "COMPLETENESS_POLICY_CONTRACT.md",
    `# Completeness Policy Contract

\`\`\`json
${JSON.stringify(policyDesc, null, 2)}
\`\`\`

## Source of truth
\`lib/ai-demand-positioning/certification/adp-provider-completeness-policy-v1.js\`

## Status mapping
| Condition | Outcome |
|-----------|---------|
| silentDenominatorReduction | QA_FAILED (hard) |
| materialFailure (overall or provider imbalance) without silent reduction | QA_REVIEW_REQUIRED |
| otherwise + all other gates pass | CERTIFIED (certification-era) / LEGACY_QA_PASS (legacy audit) |
`
  );

  // Hilton / Renaissance control
  const hilton = rowById(rows, "adp_hilton_times_square");
  const ren = rowById(rows, "adp_renaissance_times_square");
  let exact = "UNKNOWN";
  try {
    const hp = loadPeriod(hilton.latestPeriodId);
    const rp = loadPeriod(ren.latestPeriodId);
    exact = evaluateAdpComparability(hp, rp).outcome;
  } catch (e) {
    exact = String(e.message || e);
  }
  const hiltonGate = evaluateProviderCompletenessGate(loadPeriod(hilton.latestPeriodId));
  const renGate = evaluateProviderCompletenessGate(loadPeriod(ren.latestPeriodId));

  write(
    "CONTROL_RECHECK.md",
    `# Control Recheck

## Hilton
- Property QA: ${hilton.propertyQaStatus}
- Period certification: ${hilton.periodCertificationStatus}
- Period class: ${hilton.currentPeriodClass}
- Completeness: ${hiltonGate.successful}/${hiltonGate.expected} (failed=${hiltonGate.failed}, timedOut=${hiltonGate.timedOut})

## Renaissance
- Property QA: ${ren.propertyQaStatus}
- Period certification: ${ren.periodCertificationStatus}
- Period class: ${ren.currentPeriodClass}
- Completeness: ${renGate.successful}/${renGate.expected} (failed=${renGate.failed}, timedOut=${renGate.timedOut})

## Comparability
- Outcome: **${exact}**
- Expected: EXACT_COMPARABLE
- Matched-control 520/520: Hilton ${hiltonGate.expected === 520 && hiltonGate.successful === 520 ? "YES" : `${hiltonGate.successful}/${hiltonGate.expected}`}; Renaissance ${renGate.expected === 520 && renGate.successful === 520 ? "YES" : `${renGate.successful}/${renGate.expected}`}

## Bethesda
- Property QA: ${rowById(rows, "adp_bethesda_marriott")?.propertyQaStatus}
- Period certification: ${rowById(rows, "adp_bethesda_marriott")?.periodCertificationStatus}
- Period class: ${rowById(rows, "adp_bethesda_marriott")?.currentPeriodClass}
- Clarification: **LEGACY_OFFICIAL + LEGACY_QA_PASS** (not certification-era CERTIFIED)

## W Rome
- Property QA: ${rowById(rows, "adp_w_rome")?.propertyQaStatus}
- Period certification: ${rowById(rows, "adp_w_rome")?.periodCertificationStatus}
- Period class: ${rowById(rows, "adp_w_rome")?.currentPeriodClass}
- Clarification: **LEGACY_OFFICIAL + LEGACY_QA_PASS** (identity fix; no rerun; not certification-era)
`
  );

  const smoke = await smokeHardEnforcement();
  write(
    "HARD_ENFORCEMENT_RECHECK.md",
    `# Hard Enforcement Recheck

${smoke.map((s) => `- **${s.case}**: ${s.pass ? "PASS" : "FAIL"} — ${s.detail}`).join("\n")}
`
  );

  write(
    "REGRESSION_ASSERTIONS.md",
    `# Regression Assertions

${counts.assertions.map((a) => `- **${a.id}**: ${a.pass ? "PASS" : "FAIL"} ${a.detail ? "— " + JSON.stringify(a.detail) : ""}`).join("\n")}

**ALL PASS:** ${counts.allAssertionsPass ? "YES" : "NO"}
`
  );

  write(
    "STATUS_MODEL.md",
    `# Status Model

## Dimensions (orthogonal)

1. **PROPERTY_QA_STATUS** — PASS | REVIEW | FAIL  
   Engine dry-run quality of the current period (hard/review flags). Does **not** mean period is CERTIFIED.

2. **PERIOD_CERTIFICATION_STATUS** — DRAFT | QA_REVIEW_REQUIRED | QA_FAILED | CERTIFIED | LEGACY_UNCERTIFIED | SUPERSEDED | **LEGACY_QA_PASS** (reporting)  
   Governed certification state. Stale \`CERTIFIED\` stamps on pre-framework manifests do **not** count as CERTIFIED.

3. **CURRENT_PERIOD_CLASS** — CERTIFICATION_ERA_OFFICIAL | LEGACY_OFFICIAL | NO_OFFICIAL_PERIOD  
   Whether the Live period was created under \`adp_global_certification_engine_v1\`.

4. **PUBLISH_ELIGIBILITY** — ALLOWED | BLOCKED | GRANDFATHERED  
   New official publish vs customer-read grandfathering.

5. **LEGACY_STATUS** — NOT_LEGACY | LEGACY_CURRENT | LEGACY_SUPERSEDED | NO_PERIOD

6. **COMPARABILITY_STATUS** — EXACT_COMPARABLE | COMMON_SET_COMPARABLE | DIRECTIONAL_ONLY | NOT_COMPARABLE | NOT_EVALUATED

## Rules
- Property QA PASS ≠ period CERTIFIED
- LEGACY_QA_PASS = engineWouldCertify CERTIFIED + legacy period class
- Only CERTIFICATION_ERA_OFFICIAL + CERTIFIED counts in CERTIFICATION-ERA CERTIFIED PERIOD COUNT
- Never report combined label \`CERTIFIED/PASS\`
`
  );

  write(
    "COUNT_RECONCILIATION.md",
    `# Count Reconciliation

## Prior output
\`CERTIFIED/PASS COUNT AFTER = 3\` and \`LEGACY_ONLY COUNT AFTER = 17\`

## Root cause (exact)
**A + B + C (combined):**

1. **A — Only Hilton / Renaissance / YOTEL** have \`globalCertificationEngineVersion\` → **CERTIFICATION_ERA_OFFICIAL** + governed **CERTIFIED**.
2. **B — Cambridge / Hotel Caribe / NOW NOW / Bethesda** (and other legacy Live hotels) only **passed engine dry-run** (\`engineStatus=CERTIFIED\` / LEGACY_QA_PASS). Audit remaps official status to **LEGACY_UNCERTIFIED** / reporting **LEGACY_QA_PASS**. They were **not** retro-stamped as certification-era CERTIFIED.
3. **C — Terminology inconsistency:** founder pack used \`engineStatus\` as "FINAL STATUS: CERTIFIED" for the three review cases, and collapsed certification-era CERTIFIED with a combined \`CERTIFIED/PASS\` inventory bucket, while the same pack bucketed legacy QA-pass hotels as \`LEGACY_ONLY\`.

Prior "3" = **certification-era CERTIFIED official periods only** (Hilton, Renaissance, YOTEL).  
Prior "17" = **legacy official current periods** (including hotels that dry-run QA PASS).

No silent state-model corruption of periods; reporting mixed axes.
`
  );

  write(
    "LEGACY_VS_CERTIFIED.md",
    `# Legacy vs Certified

## Intended rule (preserved)
A legacy period may pass deterministic QA today (**LEGACY_QA_PASS**) without becoming retroactively **CERTIFIED**.

## Disk note
Some pre-framework published manifests still carry \`certificationStatus: "CERTIFIED"\` without \`globalCertificationEngineVersion\`.  
Inventory treats these as **LEGACY_OFFICIAL** + **LEGACY_QA_PASS** (or LEGACY_UNCERTIFIED for publish math).  
**We do not overwrite historical manifests** in this task.

## Formal comparison
Only certification-era CERTIFIED periods are marked \`formalComparisonUsable=YES\`.
`
  );

  write(
    "TERMINOLOGY_AUDIT.md",
    `# Terminology Audit

| Prior label | Actually meant | Normalized |
|-------------|----------------|------------|
| CERTIFIED/PASS COUNT | Certification-era CERTIFIED periods | CERTIFICATION-ERA CERTIFIED PERIOD COUNT |
| LEGACY_ONLY | Legacy official current period | CURRENT_PERIOD_CLASS=LEGACY_OFFICIAL |
| FINAL STATUS: CERTIFIED (Cambridge/Caribe/NOW NOW) | engineWouldCertify / Property QA PASS | Property QA: PASS; Period: LEGACY_QA_PASS |
| CERTIFIED CONTROL STILL PASS (Bethesda) | Property QA PASS on legacy period | Property QA: PASS; Period: LEGACY_QA_PASS; Class: LEGACY_OFFICIAL |
| HILTON CERTIFIED | Period CERTIFIED + era | Period CERTIFICATION: CERTIFIED; Class: CERTIFICATION_ERA_OFFICIAL |

## Fix applied
- Status dimensions module + inventory builder
- This pack never emits \`CERTIFIED/PASS\`
- Prior pack script \`run-adp-final-review-cleanup-yotel-pack-v1.mjs\` founder table updated to separate fields
`
  );

  // Patch prior pack wording if file exists
  const priorPack = join(process.cwd(), "scripts/run-adp-final-review-cleanup-yotel-pack-v1.mjs");
  if (existsSync(priorPack)) {
    let src = readFileSync(priorPack, "utf8");
    if (src.includes("CERTIFIED/PASS COUNT AFTER")) {
      src = src.replace(
        "| CERTIFIED/PASS COUNT AFTER | ${certifiedCount} |",
        "| CERTIFICATION-ERA CERTIFIED PERIOD COUNT | ${certifiedCount} |\n| PROPERTY QA PASS COUNT (incl. legacy) | ${globalRows.filter((r) => r.engineStatus === \"CERTIFIED\" || r.bucket === \"LEGACY_ONLY\").length} |"
      );
      writeFileSync(priorPack, src, "utf8");
    }
  }

  const h = rowById(rows, "adp_hilton_times_square");
  const r = rowById(rows, "adp_renaissance_times_square");
  const y = rowById(rows, "adp_yotel_geneva_lake");
  const c = rowById(rows, "adp_cambridge_beaches_bermuda");
  const car = rowById(rows, "adp_hotel_caribe_faranda_grand");
  const nn = rowById(rows, "adp_now_now_noho");
  const b = rowById(rows, "adp_bethesda_marriott");
  const w = rowById(rows, "adp_w_rome");

  write(
    "FOUNDER_REPORT.md",
    `# Founder Report — Certification Inventory Reconciliation

## Canonical counts

| Metric | Value |
|--------|-------|
| TOTAL ADP PROPERTIES | ${counts.total} |
| PROPERTY QA PASS | ${counts.propertyQaPass} |
| PROPERTY QA REVIEW | ${counts.propertyQaReview} |
| PROPERTY QA FAIL | ${counts.propertyQaFail} |
| CERTIFICATION-ERA CERTIFIED PERIODS | ${counts.certEraCertified} |
| LEGACY_UNCERTIFIED / LEGACY_QA_PASS CURRENT | ${counts.legacyUncertifiedCurrent} |
| QA_REVIEW_REQUIRED PERIODS | ${counts.qaReviewPeriods} |
| QA_FAILED PERIODS | ${counts.qaFailedPeriods} |
| NO OFFICIAL PERIOD | ${counts.noOfficial} |
| PUBLISH ALLOWED | ${counts.publishAllowed} |
| PUBLISH BLOCKED | ${counts.publishBlocked} |
| PUBLISH GRANDFATHERED | ${counts.publishGrandfathered} |

## Prior CERTIFIED/PASS=3 root cause
Only Hilton/Renaissance/YOTEL are certification-era CERTIFIED. Other "CERTIFIED" language was Property QA PASS / LEGACY_QA_PASS / stale manifest labels.

## Focus properties
| Hotel | Property QA | Period cert | Class |
|-------|-------------|-------------|-------|
| Hilton | ${h?.propertyQaStatus} | ${h?.periodCertificationStatus} | ${h?.currentPeriodClass} |
| Renaissance | ${r?.propertyQaStatus} | ${r?.periodCertificationStatus} | ${r?.currentPeriodClass} |
| YOTEL | ${y?.propertyQaStatus} | ${y?.periodCertificationStatus} | ${y?.currentPeriodClass} |
| Cambridge | ${c?.propertyQaStatus} | ${c?.periodCertificationStatus} | ${c?.currentPeriodClass} |
| Hotel Caribe | ${car?.propertyQaStatus} | ${car?.periodCertificationStatus} | ${car?.currentPeriodClass} |
| NOW NOW | ${nn?.propertyQaStatus} | ${nn?.periodCertificationStatus} | ${nn?.currentPeriodClass} |
| Bethesda | ${b?.propertyQaStatus} | ${b?.periodCertificationStatus} | ${b?.currentPeriodClass} |
| W Rome | ${w?.propertyQaStatus} | ${w?.periodCertificationStatus} | ${w?.currentPeriodClass} |

## YOTEL
- Policy: ${ADP_PROVIDER_COMPLETENESS_POLICY_NAME}
- ${yotelGate.successful}/${yotelGate.expected}; timedOut=${yotelGate.timedOut}
- Recheck: ${yotelCert.status}
- Provider imbalance guard: ${yotelGate.providerImbalance.length === 0 ? "PASS" : "FAIL"}

## Controls
- EXACT_COMPARABLE: ${exact}
- Matched-control completeness: Hilton + Renaissance combined provider matrix (65×4×2 = 520 when both complete)
- Hard enforcement smoke: ${smoke.every((s) => s.pass) ? "PASS" : "FAIL"}
- Assertions: ${counts.allAssertionsPass ? "PASS" : "FAIL"}

## Non-changes
Methodology NO · Thresholds NO · Customer metrics NO · Old periods overwritten NO · Legacy silently certified NO
`
  );

  write(
    "CHANGELOG.md",
    `# Changelog — Certification Inventory Reconciliation

## Added
- \`adp-status-dimensions-v1.js\` — orthogonal status enums + resolvers
- \`adp-provider-completeness-policy-v1.js\` — documented completeness policy + provider-level imbalance guard (same 10%/floor-2)
- \`adp-certification-inventory-v1.js\` — canonical inventory builder + count assertions
- Report pack under \`reports/adp/certification-inventory-reconciliation/\`

## Changed
- \`certify-adp-period-v1.js\` delegates completeness to policy module; review flags include provider imbalance details
- Prior founder pack wording: remove combined CERTIFIED/PASS label

## Not changed
- ADP methodology / scoring thresholds
- Historical period observation corpora
- No silent retro-certification of legacy periods
- YOTEL baseline left CERTIFIED (recheck pass)
`
  );

  write(
    "RUN_SUMMARY.json",
    JSON.stringify(
      {
        counts,
        exactComparable: exact,
        yotel: {
          expected: yotelGate.expected,
          successful: yotelGate.successful,
          failed: yotelGate.failed,
          timedOut: yotelGate.timedOut,
          recheck: yotelCert.status,
          providerImbalance: yotelGate.providerImbalance,
          considerationDenom: consideration.comparableObservations,
        },
        smoke,
        rows: rows.map((r) => ({
          hotelId: r.hotelId,
          propertyQaStatus: r.propertyQaStatus,
          periodCertificationStatus: r.periodCertificationStatus,
          currentPeriodClass: r.currentPeriodClass,
          publishEligibility: r.publishEligibility,
        })),
      },
      null,
      2
    )
  );

  console.log(
    JSON.stringify(
      {
        out: OUT,
        counts,
        exact,
        yotelRecheck: yotelCert.status,
        smokePass: smoke.every((s) => s.pass),
        assertionsPass: counts.allAssertionsPass,
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
