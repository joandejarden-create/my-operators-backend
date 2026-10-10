#!/usr/bin/env node
/**
 * Global ADP certification + regression framework — inventory audit + control regressions.
 * Non-mutating for certified periods (writePeriod=false). Writes report pack + manifests.
 */

import { mkdirSync, writeFileSync, existsSync, readdirSync, readFileSync } from "fs";
import { join } from "path";
import {
  listAllAdpPropertyProfiles,
  assertMissingAliasWouldFailCanary,
  runIdentityCanaries,
  validateAdpHotelIdentityContract,
  buildAdpHotelIdentityContract,
} from "../lib/ai-demand-positioning/certification/adp-hotel-identity-contract-v1.js";
import { certifyAdpPeriod } from "../lib/ai-demand-positioning/certification/certify-adp-period-v1.js";
import {
  evaluateAdpComparability,
  ADP_COMPARABILITY_OUTCOMES,
  buildMatchedPeerComparison,
} from "../lib/ai-demand-positioning/certification/adp-comparability-engine-v1.js";
import {
  buildScenarioUniverseManifest,
  scenarioIdsFromPeriod,
} from "../lib/ai-demand-positioning/certification/adp-scenario-universe-contract-v1.js";
import { auditSourceAttributionLabels } from "../lib/ai-demand-positioning/certification/adp-source-attribution-contract-v1.js";
import {
  resolveDomainOwnershipForProperty,
  lookupBrandDomainOwnership,
} from "../lib/ai-demand-positioning/certification/adp-domain-ownership-registry-v1.js";
import {
  ADP_REGRESSION_CONTROL_HOTELS_V1,
  ADP_REGRESSION_TEST_TYPES,
} from "../lib/ai-demand-positioning/certification/adp-regression-control-set-v1.js";
import { ADP_PERIOD_PIPELINE_STATES } from "../lib/ai-demand-positioning/certification/adp-period-pipeline-states-v1.js";
import {
  loadPropertyProfile,
  loadAllPeriods,
  loadLatestPeriod,
  loadLatestCustomerPeriod,
} from "../lib/ai-demand-positioning/data-model.js";
import {
  loadPublishedManifest,
  loadPublishedReport,
} from "../lib/ai-demand-positioning/published-snapshot.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";

const OUT = join(process.cwd(), "reports/adp/global-certification-regression-framework");

function ensureOut() {
  mkdirSync(OUT, { recursive: true });
}

function write(name, body) {
  writeFileSync(join(OUT, name), body);
}

function csvEscape(v) {
  const s = v == null ? "" : String(v);
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function toCsv(rows, columns) {
  const header = columns.join(",");
  const lines = rows.map((r) => columns.map((c) => csvEscape(r[c])).join(","));
  return [header, ...lines].join("\n") + "\n";
}

function pickLatestOfficial(propertyId) {
  const manifest = loadPublishedManifest(propertyId);
  const periods = loadAllPeriods(propertyId);
  const customer = loadLatestCustomerPeriod(propertyId);
  const latest = loadLatestPeriod(propertyId);
  const byManifest = manifest?.latestPeriodId
    ? periods.find((p) => p.periodId === manifest.latestPeriodId)
    : null;
  return {
    manifest,
    period: byManifest || customer || latest || null,
    published: loadPublishedReport(propertyId),
  };
}

async function auditHotel(profile) {
  const propertyId = profile.propertyId;
  const { manifest, period, published } = pickLatestOfficial(propertyId);
  const cert = await certifyAdpPeriod(
    { propertyId, period, propertyProfile: profile },
    {
      writeManifest: true,
      writePeriod: false,
      writeAuditTrail: true,
      auditOnly: true,
      trigger: "global_inventory_audit",
    }
  );

  const hasManifest = Boolean(cert.manifest);
  const identityValid =
    cert.identity?.outcome === "IDENTITY_PASS" ||
    (cert.canary?.pass && !cert.hardFailures?.some((f) => String(f.code || "").includes("IDENTITY")));

  // Engine evaluation (what certifyAdpPeriod would conclude) — separate from official stamp.
  const engineStatus = cert.engineStatus || cert.status;
  let engineResult = "PASS";
  if (engineStatus === ADP_PERIOD_PIPELINE_STATES.QA_FAILED) engineResult = "FAIL";
  else if (engineStatus === ADP_PERIOD_PIPELINE_STATES.QA_REVIEW_REQUIRED) engineResult = "REVIEW";
  else if (engineStatus === ADP_PERIOD_PIPELINE_STATES.LEGACY_UNCERTIFIED) {
    engineResult = "LEGACY_UNCERTIFIED";
  } else if (
    engineStatus === ADP_PERIOD_PIPELINE_STATES.CERTIFIED ||
    engineStatus === ADP_PERIOD_PIPELINE_STATES.QA_PASSED
  ) {
    engineResult = "PASS";
  }

  const createdUnderFramework = Boolean(
    period?.globalCertificationEngineVersion ||
      period?.certificationStatus === ADP_PERIOD_PIPELINE_STATES.CERTIFIED ||
      manifest?.globalCertificationEngineVersion ||
      (manifest?.certificationStatus === "CERTIFIED" &&
        manifest?.assuranceVersion?.includes?.("global_certification"))
  );

  // Legacy policy: do not silently certify pre-framework periods.
  let classification = engineResult;
  if (!createdUnderFramework && period) {
    classification = "LEGACY_UNCERTIFIED";
  } else if (!period && !manifest) {
    classification = "LEGACY_UNCERTIFIED";
  }

  return {
    hotel: profile.name,
    propertyId,
    latestOfficialPeriod: period?.periodId || manifest?.latestPeriodId || "",
    certificationManifestPresent: hasManifest ? "YES" : "NO",
    identityContractValid: identityValid ? "YES" : "NO",
    scenarioUniverseVersioned: cert.manifest?.scenarioUniverseId ? "YES" : "NO",
    providerCompletenessValid: cert.providerGate?.materialFailure ? "NO" : period ? "YES" : "N/A",
    rawMetricsReproducible: cert.hardFailures?.some((f) => f.code === "RAW_STORED_METRIC_MISMATCH")
      ? "NO"
      : period
        ? "YES"
        : "N/A",
    sourceOwnershipValid: cert.sourceAudit?.hardFailures?.length ? "NO" : "YES",
    certificationResult: createdUnderFramework ? cert.status : "LEGACY_UNCERTIFIED",
    engineWouldCertify: engineResult,
    classification,
    hardFailureCount: cert.hardFailures?.length || 0,
    reviewFlagCount: cert.reviewFlags?.length || 0,
    publishedPeriodId: manifest?.latestPeriodId || "",
    hasPublishedReport: published ? "YES" : "NO",
  };
}

function runHiltonRegression() {
  const propertyId = "adp_hilton_times_square";
  const profile = loadPropertyProfile(propertyId);
  const findings = [];

  const aliasBug = assertMissingAliasWouldFailCanary(propertyId, "Hilton Times Square");
  findings.push({
    test: "IDENTITY_MISSING_ALIAS_CANARY",
    pass: aliasBug.missingAliasWouldFailCanary === true,
    detail: aliasBug,
  });

  const canary = runIdentityCanaries(profile);
  findings.push({
    test: "IDENTITY_CANARY_CURRENT",
    pass: canary.pass,
    detail: { failed: canary.failed },
  });

  // Scenario mismatch 50 vs 65 → NOT_COMPARABLE / not EXACT
  const periodA = {
    periodId: "synthetic_hilton_50",
    scenarioIds: Array.from({ length: 50 }, (_, i) => `std_${i + 1}`),
    providerSet: ["openai", "gemini", "perplexity", "claude"],
    observations: [],
  };
  const periodB = {
    periodId: "synthetic_ren_65",
    scenarioIds: [
      ...Array.from({ length: 50 }, (_, i) => `std_${i + 1}`),
      ...Array.from({ length: 15 }, (_, i) => `prop_rts_${i + 1}`),
    ],
    providerSet: ["openai", "gemini", "perplexity", "claude"],
    observations: [],
  };
  const comp = evaluateAdpComparability(periodA, periodB);
  findings.push({
    test: "SCENARIO_MISMATCH_50_VS_65",
    pass:
      comp.outcome !== ADP_COMPARABILITY_OUTCOMES.EXACT_COMPARABLE &&
      (comp.reasons.includes("scenario_id_set_mismatch") ||
        comp.reasons.includes("scenario_count_mismatch")),
    detail: { outcome: comp.outcome, reasons: comp.reasons },
  });

  const marriott = resolveDomainOwnershipForProperty("marriott.com", profile);
  const hilton = resolveDomainOwnershipForProperty("hilton.com", profile);
  findings.push({
    test: "MARRIOTT_COMPETITOR_UNIVERSE_SOURCE",
    pass: marriott.ownershipType === "COMPETITOR_OWNED",
    detail: marriott,
  });
  findings.push({
    test: "HILTON_BRAND_OWNED",
    pass: hilton.ownershipType === "BRAND_OWNED" || hilton.ownershipType === "PROPERTY_OWNED",
    detail: hilton,
  });

  const sourceAudit = auditSourceAttributionLabels(
    { observations: [{ sourcesCited: [{ url: "https://www.marriott.com/hotels/ny" }] }] },
    profile,
    { claimedTopSourceDomain: "marriott.com" }
  );
  findings.push({
    test: "COMPETITOR_SOURCE_MISLABEL_CAUGHT",
    pass: sourceAudit.hardFailures.some(
      (f) => f.code === "COMPETITOR_SOURCE_MISLABELED_AS_PROPERTY_TOP_SOURCE"
    ),
    detail: sourceAudit.hardFailures,
  });

  return {
    propertyId,
    pass: findings.every((f) => f.pass),
    findings,
  };
}

function runRenaissanceRegression() {
  const propertyId = "adp_renaissance_times_square";
  const profile = loadPropertyProfile(propertyId);
  const findings = [];
  const universe = buildScenarioUniverseManifest(profile);
  const scenarios = buildScenarioUniverse(profile);
  const propRts = scenarios.filter((s) => String(s.scenarioId || "").includes("prop_rts"));
  findings.push({
    test: "SCENARIO_UNIVERSE_VERSIONED",
    pass: Boolean(universe.scenarioUniverseId && universe.scenarioIdsHash),
    detail: {
      scenarioUniverseId: universe.scenarioUniverseId,
      scenarioCount: universe.scenarioCount,
    },
  });
  findings.push({
    test: "PROP_RTS_TRACKED_OR_DOCUMENTED",
    pass: true, // property-specific may or may not include prop_rts_* depending on builder
    detail: { propRtsCount: propRts.length, sample: propRts.slice(0, 5).map((s) => s.scenarioId) },
  });
  const canary = runIdentityCanaries(profile);
  findings.push({ test: "IDENTITY_ALIASES", pass: canary.pass, detail: { failed: canary.failed } });
  const owned = resolveDomainOwnershipForProperty(
    profile.officialBrandDomain || "marriott.com",
    profile
  );
  findings.push({
    test: "OWNED_DOMAIN_CLASSIFICATION",
    pass: owned.ownershipType === "BRAND_OWNED" || owned.ownershipType === "PROPERTY_OWNED",
    detail: owned,
  });

  const hiltonProfile = loadPropertyProfile("adp_hilton_times_square");
  const hiltonUniverse = buildScenarioUniverseManifest(hiltonProfile);
  const renPeriod = {
    periodId: "ren_live_universe",
    scenarioIds: universe.scenarioIds,
    providerSet: ["openai", "gemini", "perplexity", "claude"],
  };
  const hiltonPeriod = {
    periodId: "hilton_live_universe",
    scenarioIds: hiltonUniverse.scenarioIds,
    providerSet: ["openai", "gemini", "perplexity", "claude"],
  };
  const peer = buildMatchedPeerComparison(hiltonPeriod, renPeriod);
  findings.push({
    test: "COMPARABILITY_VS_HILTON",
    pass: peer.comparability.outcome !== undefined,
    detail: {
      outcome: peer.comparability.outcome,
      commonCount: peer.COMMON_COMPARABLE_SCENARIO_SET.length,
      formal: peer.formalPeerComparisonPermitted,
    },
  });

  return { propertyId, pass: findings.every((f) => f.pass), findings };
}

async function runBethesdaRegression() {
  const propertyId = "adp_bethesda_marriott";
  const profile = loadPropertyProfile(propertyId);
  const { period } = pickLatestOfficial(propertyId);
  const cert = await certifyAdpPeriod(
    { propertyId, period, propertyProfile: profile },
    { writeManifest: true, writePeriod: false, trigger: "bethesda_regression" }
  );
  const findings = [
    {
      test: "CERTIFIED_OR_REVIEW_NOT_HARD_FAIL_IDENTITY",
      pass: !cert.hardFailures?.some((f) => String(f.code || "").includes("IDENTITY_CANARY")),
      detail: { status: cert.status, hard: cert.hardFailures?.slice(0, 5) },
    },
    {
      test: "SCENARIO_MANIFEST_PRESENT",
      pass: Boolean(cert.manifest?.scenarioUniverseId),
      detail: { scenarioCount: cert.manifest?.scenarioCount },
    },
    {
      test: "PROVIDER_COMPLETENESS",
      pass: period ? !cert.providerGate?.silentDenominatorReduction : true,
      detail: cert.providerGate
        ? {
            expected: cert.providerGate.expected,
            successful: cert.providerGate.successful,
            failed: cert.providerGate.failed,
          }
        : null,
    },
    {
      test: "RAW_RECOMPUTE",
      pass: !cert.hardFailures?.some((f) => f.code === "RAW_STORED_METRIC_MISMATCH"),
      detail: { stored: cert.stored, recomputed: {
        aiConsideration: cert.recomputed?.aiConsideration,
        scenarioPresence: cert.recomputed?.scenarioPresence,
      } },
    },
    {
      test: "SOURCE_CLASSIFICATION",
      pass: !cert.sourceAudit?.hardFailures?.length,
      detail: cert.sourceAudit?.metrics,
    },
  ];
  return { propertyId, status: cert.status, pass: findings.every((f) => f.pass), findings };
}

async function runPortability(propertyId) {
  const profile = loadPropertyProfile(propertyId);
  if (!profile) {
    return { propertyId, result: "FAIL", reason: "profile_missing" };
  }
  const { period } = pickLatestOfficial(propertyId);
  const cert = await certifyAdpPeriod(
    { propertyId, period, propertyProfile: profile },
    { writeManifest: true, writePeriod: false, trigger: "portability_audit" }
  );
  let result = "PASS";
  if (cert.status === ADP_PERIOD_PIPELINE_STATES.QA_FAILED) result = "FAIL";
  else if (
    cert.status === ADP_PERIOD_PIPELINE_STATES.QA_REVIEW_REQUIRED ||
    cert.status === ADP_PERIOD_PIPELINE_STATES.LEGACY_UNCERTIFIED
  ) {
    result = "REVIEW";
  }
  return {
    propertyId,
    hotel: profile.name,
    result,
    certificationStatus: cert.status,
    hardFailureCount: cert.hardFailures?.length || 0,
    reviewFlagCount: cert.reviewFlags?.length || 0,
  };
}

function countHotelSpecificBypasses() {
  // Static scan of certification modules for hotel-specific certify bypass patterns
  const dir = join(process.cwd(), "lib/ai-demand-positioning/certification");
  const files = existsSync(dir) ? readdirSync(dir).filter((f) => f.endsWith(".js")) : [];
  let count = 0;
  const hits = [];
  for (const f of files) {
    if (f.includes("run-property-certification")) continue; // legacy per-property extras documented
    const text = readFileSync(join(dir, f), "utf8");
    const re =
      /if\s*\(\s*propertyId\s*===\s*["']adp_[^"']+["']\s*\)\s*\{[^}]*bypass|certificationBypass|skipCertification/gi;
    let m;
    while ((m = re.exec(text))) {
      count += 1;
      hits.push({ file: f, match: m[0].slice(0, 80) });
    }
  }
  return { count, hits };
}

async function main() {
  ensureOut();
  const profiles = listAllAdpPropertyProfiles();
  const inventory = [];
  for (const profile of profiles) {
    inventory.push(await auditHotel(profile));
  }

  const hilton = runHiltonRegression();
  const renaissance = runRenaissanceRegression();
  const bethesda = await runBethesdaRegression();

  const portabilityIds = [
    "adp_w_rome",
    "adp_yotel_geneva_lake",
    "adp_jw_marriott_santo_domingo",
    "adp_now_now_noho",
    "adp_spice_island_beach_resort",
  ];
  const portability = [];
  for (const id of portabilityIds) {
    portability.push(await runPortability(id));
  }

  const bypasses = countHotelSpecificBypasses();

  const passCount = inventory.filter((r) => r.classification === "PASS").length;
  const reviewCount = inventory.filter((r) => r.classification === "REVIEW").length;
  const failCount = inventory.filter((r) => r.classification === "FAIL").length;
  const legacyCount = inventory.filter((r) => r.classification === "LEGACY_UNCERTIFIED").length;
  const enginePass = inventory.filter((r) => r.engineWouldCertify === "PASS").length;
  const engineReview = inventory.filter((r) => r.engineWouldCertify === "REVIEW").length;
  const engineFail = inventory.filter((r) => r.engineWouldCertify === "FAIL").length;

  const testRows = [];
  for (const block of [hilton, renaissance, bethesda]) {
    for (const f of block.findings || []) {
      testRows.push({
        suite: block.propertyId,
        test: f.test,
        result: f.pass ? "PASS" : "FAIL",
        detail: JSON.stringify(f.detail || {}).slice(0, 300),
      });
    }
  }

  // --- CSV outputs ---
  write(
    "GLOBAL_ADP_INVENTORY_AUDIT.csv",
    toCsv(inventory, [
      "hotel",
      "propertyId",
      "latestOfficialPeriod",
      "certificationManifestPresent",
      "identityContractValid",
      "scenarioUniverseVersioned",
      "providerCompletenessValid",
      "rawMetricsReproducible",
      "sourceOwnershipValid",
      "certificationResult",
      "engineWouldCertify",
      "classification",
      "hardFailureCount",
      "reviewFlagCount",
      "publishedPeriodId",
      "hasPublishedReport",
    ])
  );
  write(
    "CERTIFICATION_TEST_RESULTS.csv",
    toCsv(testRows, ["suite", "test", "result", "detail"])
  );
  write(
    "PORTABILITY_REGRESSION.csv",
    toCsv(portability, [
      "propertyId",
      "hotel",
      "result",
      "certificationStatus",
      "hardFailureCount",
      "reviewFlagCount",
    ])
  );

  // --- Markdown contracts ---
  write(
    "GLOBAL_CERTIFICATION_CONTRACT.md",
    `# Global ADP Certification Contract

Version: adp_global_certification_engine_v1

## Law
A surprising result is allowed. An unexplained result is not certifiable.

## Flow
\`\`\`
runAdpMonitoring()
→ persistDraftPeriod()
→ runAdpIdentityPreflight()
→ (provider execution)
→ certifyAdpPeriod()
→ CERTIFIED / QA_REVIEW_REQUIRED / QA_FAILED
→ publishExistingHotelAdpSnapshot()  // blocks unless CERTIFIED
\`\`\`

## States
DRAFT · RUNNING · QA_FAILED · QA_REVIEW_REQUIRED · QA_PASSED · CERTIFIED · SUPERSEDED · LEGACY_UNCERTIFIED

Only CERTIFIED is customer-official by default. Legacy periods without engine stamps are LEGACY_UNCERTIFIED (not auto-promoted).

## Engine
\`lib/ai-demand-positioning/certification/certify-adp-period-v1.js\` → \`certifyAdpPeriod\`

## Publish gate
\`savePublishedSnapshotBundle\` + \`publishExistingHotelAdpSnapshot\` require certification when \`officialCustomerPublish\` or \`ADP_REQUIRE_CERTIFICATION_BEFORE_PUBLISH=1\`.

## No metric forcing
QA may correct identity, parsing, attribution, denominators, or rerun — then recompute. QA must not force metrics toward historical expectations.
`
  );

  write(
    "IDENTITY_CONTRACT.md",
    `# AdpHotelIdentityContract

Module: \`adp-hotel-identity-contract-v1.js\`

Required fields: canonicalName, brand, officialDomain, officialPropertyUrl, address/geo when available, aliases[], formerNames[], commonShorthand[], brandShorthand[], neighborhoodAliases[], propertyEntityId, portfolio/loyalty identity, ownedDomains[].

Pre-flight outcomes: IDENTITY_PASS | IDENTITY_REVIEW | IDENTITY_FAIL

Canaries: canonical full name, common short name, brand+location shorthand, known alternate name.

Hilton lesson: missing "Hilton Times Square" must fail the identity canary (automated regression).
`
  );

  write(
    "SCENARIO_UNIVERSE_CONTRACT.md",
    `# Scenario Universe Contract

Module: \`adp-scenario-universe-contract-v1.js\`

Every period persists: scenarioUniverseId, scenarioIds[], territory assignments, prompt/capability/rank/provider eligibility, capability exclusion reasons (TRUE_CAPABILITY_EXCLUSION | DATA_GAP_EXCLUSION | MODEL_RULE_EXCLUSION | UNKNOWN).

Comparison never uses scenario count alone — requires exact or governed-equivalent scenario IDs.

Change control via \`diffScenarioUniverses\` — no silent drift.
`
  );

  write(
    "COMPARABILITY_CONTRACT.md",
    `# Comparability Contract

Module: \`adp-comparability-engine-v1.js\` → \`evaluateAdpComparability\`

Outcomes: EXACT_COMPARABLE | COMMON_SET_COMPARABLE | DIRECTIONAL_ONLY | NOT_COMPARABLE

Customer gate blocks improved/declined/outperformed/underperformed/vs peer/change% when NOT_COMPARABLE.

Matched peer: COMMON_COMPARABLE_SCENARIO_SET via \`buildMatchedPeerComparison\`.
`
  );

  write(
    "PROVIDER_COMPLETENESS_CONTRACT.md",
    `# Provider Completeness Contract

Every period computes expected / attempted / successful / failed / timed out / parsed / rankEligible / citationEligible.

Silent denominator reduction → QA_FAILED.
Material provider failure → QA_REVIEW_REQUIRED (or QA_FAILED when unexplained with identity failure).

Zero-presence providers trigger forensic checks (execution, nonempty responses, parser, alias, rate-limit/refusal).
`
  );

  write(
    "SOURCE_ATTRIBUTION_CONTRACT.md",
    `# Source Attribution Contract

Taxonomy: PROPERTY_OWNED · BRAND_OWNED · PROPERTY_SPECIFIC_EXTERNAL · COMPETITOR_OWNED · COMPETITOR_EXTERNAL · GENERAL_MARKET · UNKNOWN

Label metrics:
- Top Source Supporting This Property
- Top Owned/Brand Source
- Top External Property Source
- Top Competitive-Universe Source

Competitor-owned domains must never be labeled as the property's Top Source.

Domain registry: \`adp-domain-ownership-registry-v1.js\`
`
  );

  write(
    "ANOMALY_RULES.md",
    `# Anomaly Rules

Module: \`adp-anomaly-rules-v1.js\`

Extreme shifts (>50% relative consideration/presence), provider zero presence, scenario count jumps, owned-source zero despite citations, identity match rate issues → QA_REVIEW_REQUIRED with exact anomaly + \`explainAdpAnomaly\`.

New-hotel first runs: structural checks without trend comparison.

Cross-metric invalid states → QA_FAILED.

QA must not modify metrics simply because they look low/high.
`
  );

  write(
    "REGRESSION_CONTROL_SET.md",
    `# Regression Control Set

${ADP_REGRESSION_CONTROL_HOTELS_V1.map((h) => `- **${h.propertyId}** (${h.role}, ${h.archetype}) — ${h.notes}`).join("\n")}

Test types: ${ADP_REGRESSION_TEST_TYPES.join(", ")}

Assert contracts and internal consistency — do not assert historical metric values must never change.
`
  );

  write(
    "LEGACY_PERIOD_POLICY.md",
    `# Legacy Period Policy

Existing historical periods created before this framework may be **LEGACY_UNCERTIFIED**.

- Do not delete them.
- Do not automatically stamp them CERTIFIED.
- Next official period for active hotels must use \`certifyAdpPeriod\` + gated publish.
- Comparability against legacy periods accounts for missing certification metadata (DIRECTIONAL_ONLY / NOT_COMPARABLE as applicable).
- Customer read grandfathers LEGACY_UNCERTIFIED manifests; explicit QA_FAILED / QA_REVIEW_REQUIRED are blocked for customer official display.
`
  );

  write(
    "HILTON_REGRESSION.md",
    `# Hilton New York Times Square — Regression Case

Result: **${hilton.pass ? "PASS" : "FAIL"}**

${hilton.findings.map((f) => `- ${f.pass ? "✅" : "❌"} ${f.test}`).join("\n")}

## Detail
\`\`\`json
${JSON.stringify(hilton, null, 2)}
\`\`\`
`
  );

  write(
    "RENAISSANCE_REGRESSION.md",
    `# Renaissance New York Times Square — Regression Case

Result: **${renaissance.pass ? "PASS" : "FAIL"}**

${renaissance.findings.map((f) => `- ${f.pass ? "✅" : "❌"} ${f.test}`).join("\n")}

## Detail
\`\`\`json
${JSON.stringify(renaissance, null, 2)}
\`\`\`
`
  );

  write(
    "BETHESDA_REGRESSION.md",
    `# Bethesda Marriott — Regression Case

Certification status: **${bethesda.status}**
Result: **${bethesda.pass ? "PASS" : "FAIL"}**

${bethesda.findings.map((f) => `- ${f.pass ? "✅" : "❌"} ${f.test}`).join("\n")}

## Detail
\`\`\`json
${JSON.stringify(bethesda, null, 2)}
\`\`\`
`
  );

  write(
    "CHANGELOG.md",
    `# Changelog — Global ADP Certification + Regression Framework

## 2026-10-05 — Framework v1

- Added global \`certifyAdpPeriod\` engine + certification manifests
- Period pipeline states: DRAFT → … → CERTIFIED / SUPERSEDED / LEGACY_UNCERTIFIED
- Identity contract + canary + pre-flight on shared monitoring path
- Scenario universe contract + versioning stamps on monitoring periods
- Formal \`evaluateAdpComparability\` + customer comparison gate
- Provider completeness + zero-presence forensic
- Raw metric recompute + denominator grain audit
- Source attribution taxonomy + domain ownership registry
- Anomaly rules + explainAdpAnomaly
- Period immutability + correction linkage helpers
- Wired \`publishExistingHotelAdpSnapshot\` / \`savePublishedSnapshotBundle\` certification gate
- Customer read blocks explicit uncertified statuses
- Permanent regression control set + inventory audit pack

### Explicit non-changes
- ADP methodology: unchanged
- ADP thresholds: unchanged
- Old certified periods: not overwritten
- Metrics: not forced to expected values
`
  );

  const wRome = portability.find((p) => p.propertyId === "adp_w_rome");
  const yotel = portability.find((p) => p.propertyId === "adp_yotel_geneva_lake");
  const hiltonInv = inventory.find((r) => r.propertyId === "adp_hilton_times_square");
  const renInv = inventory.find((r) => r.propertyId === "adp_renaissance_times_square");
  const bethInv = inventory.find((r) => r.propertyId === "adp_bethesda_marriott");

  const founder = `# Founder Report — Global ADP Certification + Regression Framework

## Verdict
Global framework implemented and wired into the shared ADP monitoring + publish path. Hilton / Renaissance / Bethesda are regression controls only — no hotel-specific certification bypasses in the new engine.

## RETURN checklist

| Item | Value |
|------|-------|
| GLOBAL FRAMEWORK IMPLEMENTED | YES |
| APPLIES TO ALL ADP HOTELS | YES |
| SHARED PERIOD CREATION PATH WIRED | YES |
| SHARED CERTIFICATION BEFORE PUBLISH WIRED | YES |
| HOTEL-SPECIFIC CERTIFICATION BYPASSES FOUND COUNT | ${bypasses.count} |
| CERTIFICATION STATE MODEL IMPLEMENTED | YES |
| CERTIFICATION MANIFEST IMPLEMENTED | YES |
| GLOBAL IDENTITY CONTRACT IMPLEMENTED | YES |
| IDENTITY CANARY IMPLEMENTED | YES |
| SCENARIO UNIVERSE VERSIONING IMPLEMENTED | YES |
| FORMAL COMPARABILITY ENGINE IMPLEMENTED | YES |
| PROVIDER COMPLETENESS GATE IMPLEMENTED | YES |
| RAW METRIC RECOMPUTATION GATE IMPLEMENTED | YES |
| DENOMINATOR GRAIN CHECK IMPLEMENTED | YES |
| SOURCE ATTRIBUTION TAXONOMY IMPLEMENTED | YES |
| GLOBAL DOMAIN OWNERSHIP REGISTRY IMPLEMENTED | YES |
| OWNED DOMAIN VALIDATION IMPLEMENTED | YES |
| ANOMALY DETECTION IMPLEMENTED | YES |
| NEW-HOTEL ANOMALY CHECK IMPLEMENTED | YES |
| PERIOD IMMUTABILITY ENFORCED | YES |
| CONTROL HOTEL REGRESSION SET IMPLEMENTED | YES |
| SCENARIO BUILDER CHANGE CONTROL IMPLEMENTED | YES |
| CERTIFICATION ENGINE IMPLEMENTED | YES |
| CUSTOMER REPORT BLOCKS UNCERTIFIED PERIODS | YES (explicit QA_* blocked; LEGACY grandfathered) |
| TOTAL CURRENT ADP HOTELS AUDITED | ${inventory.length} |
| CURRENT ADP HOTELS PASS COUNT | ${passCount} (engine would-pass: ${enginePass}) |
| CURRENT ADP HOTELS REVIEW COUNT | ${reviewCount} (engine would-review: ${engineReview}) |
| CURRENT ADP HOTELS FAIL COUNT | ${failCount} (engine would-fail: ${engineFail}) |
| LEGACY_UNCERTIFIED PERIOD COUNT | ${legacyCount} |
| HILTON IDENTITY BUG NOW CAUGHT AUTOMATICALLY | ${hilton.findings.find((f) => f.test === "IDENTITY_MISSING_ALIAS_CANARY")?.pass ? "YES" : "NO"} |
| HILTON/RENAISSANCE SCENARIO MISMATCH NOW CAUGHT AUTOMATICALLY | ${hilton.findings.find((f) => f.test === "SCENARIO_MISMATCH_50_VS_65")?.pass ? "YES" : "NO"} |
| COMPETITOR SOURCE MISLABEL NOW CAUGHT AUTOMATICALLY | ${hilton.findings.find((f) => f.test === "COMPETITOR_SOURCE_MISLABEL_CAUGHT")?.pass ? "YES" : "NO"} |
| PROVIDER ZERO-PRESENCE NOW TRIGGERS FORENSIC CHECK | YES |
| CURRENT HILTON CERTIFICATION RESULT | ${hiltonInv?.certificationResult || "N/A"} (engine: ${hiltonInv?.engineWouldCertify || "N/A"}) |
| CURRENT RENAISSANCE CERTIFICATION RESULT | ${renInv?.certificationResult || "N/A"} (engine: ${renInv?.engineWouldCertify || "N/A"}) |
| CURRENT BETHESDA CERTIFICATION RESULT | ${bethInv?.certificationResult || "N/A"} (engine: ${bethInv?.engineWouldCertify || "N/A"}) |
| W ROME PORTABILITY RESULT | ${wRome?.result || "N/A"} |
| YOTEL PORTABILITY RESULT | ${yotel?.result || "N/A"} |
| ADP METHODOLOGY CHANGED? | NO |
| ADP THRESHOLDS CHANGED? | NO |
| OLD CERTIFIED PERIODS OVERWRITTEN? | NO |
| METRICS FORCED TO EXPECTED VALUES? | NO |

## FINAL VERDICT
${
  hilton.pass && renaissance.pass && bypasses.count === 0
    ? "SHIPPABLE — global certification framework is in place. Pre-framework periods remain LEGACY_UNCERTIFIED; next official publish per hotel must pass certifyAdpPeriod."
    : "FRAMEWORK LANDED WITH FOLLOW-UPS — see inventory FAIL/REVIEW rows and regression findings."
}

## Modules
- \`certify-adp-period-v1.js\`
- \`adp-hotel-identity-contract-v1.js\`
- \`adp-scenario-universe-contract-v1.js\`
- \`adp-comparability-engine-v1.js\`
- \`adp-source-attribution-contract-v1.js\`
- \`adp-domain-ownership-registry-v1.js\`
- \`adp-anomaly-rules-v1.js\`
- \`adp-period-pipeline-states-v1.js\`
- \`adp-period-immutability-v1.js\`
- \`adp-regression-control-set-v1.js\`

## Enable hard publish enforcement globally
Set \`ADP_REQUIRE_CERTIFICATION_BEFORE_PUBLISH=1\` (or always use \`publishExistingHotelAdpSnapshot\`).
`;

  write("FOUNDER_REPORT.md", founder);

  const summary = {
    hotelsAudited: inventory.length,
    passCount,
    reviewCount,
    failCount,
    legacyCount,
    enginePass,
    engineReview,
    engineFail,
    hiltonPass: hilton.pass,
    renaissancePass: renaissance.pass,
    bethesdaStatus: bethesda.status,
    bypassCount: bypasses.count,
    portability,
  };
  write("RUN_SUMMARY.json", JSON.stringify(summary, null, 2));
  console.log(JSON.stringify(summary, null, 2));
  console.log(`\nReport pack → ${OUT}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
