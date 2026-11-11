/**
 * Preparation + authorized live Principal–Savoy retrieval-recovery certification.
 * Live spend only when authorizeLive + AUTHORIZED manifest + isolated ledger hold.
 */

import fs from "node:fs";
import { createHash, randomBytes } from "node:crypto";
import {
  quotePlannerUsd,
  PLANNER_CEILING_USD,
  planDynamicResearch,
  RESEARCH_DISPOSITION,
} from "./dynamic-research-planner-v1.js";
import { quoteBenchmarkCall } from "./benchmark-provider-pricing-v1.js";
import {
  resolvePublishingOrganization,
  isIndependentPublisherUrl,
  RELATIONSHIP_VERIFICATION,
  classifyTechnicalSource,
  extractCandidateUrlsFromHtml,
  STOPPING_TRIGGER,
} from "./commercial-source-selection-v1.js";
import {
  proposeAdaptiveRecovery,
  runOfflineSavoyRetrievalRecoveryCertification,
  RETRIEVAL_OUTCOME,
  recordSourceAttempt,
  summarizeAttemptBudget,
  buildCrossOrgInvestigationReport,
} from "./cross-organization-retrieval-recovery-v1.js";
import {
  ORGANIZER_HOST,
  ORGANIZER_NAME,
  RELATED_ENTITY,
  SAVOY_CALL_CEILING_USD,
} from "./cross-organization-followup-savoy-v1.js";
import { writeMaturationCheckpointAtomic, maturationMemoryPath } from "./bethesda-maturation-checkpoint-v1.js";

export const LIVE_RECOVERY_CERT_VERSION = "gdi-cross-organization-live-recovery-certification-v1";
export const LIVE_RECOVERY_BUDGET_USD = 0.05;
export const LIVE_RECOVERY_MAX_PLANNER_CALLS = 3;
export const LIVE_RECOVERY_MAX_SUBSTANTIVE_RETRIEVALS = 3;
export const LIVE_RECOVERY_MAX_FAILED_TECHNICAL_ATTEMPTS = 6;
export const LIVE_RECOVERY_MAX_SUBSTANTIVE_QUESTIONS = 3;
export const LIVE_RECOVERY_LEDGER_RELATIVE = "reports/gdi-cross-organization-live-recovery-certification-v1/budget-ledger.json";
export const LIVE_RECOVERY_REPORTS_DIR = "reports/gdi-cross-organization-live-recovery-certification-v1";
export const PRIOR_SAVOY_CERT_RESULT = "reports/gdi-cross-organization-followup-savoy-v1/CERTIFICATION_RESULT.json";

/**
 * Ownership roles — domain diversity ≠ publisher independence ≠ relationship corroboration.
 */
export const SOURCE_OWNERSHIP_ROLE = Object.freeze({
  ORGANIZER_CLAIMED_PROVENANCE: "ORGANIZER_CLAIMED_PROVENANCE",
  /** Fairmont Savoy pages: official alternate retrieval surface for the property/brand. */
  OFFICIAL_PROPERTY_OR_BRAND_ALTERNATE: "OFFICIAL_PROPERTY_OR_BRAND_ALTERNATE",
  /** Third-party publisher not Principal and not the hotel brand family. */
  INDEPENDENT_THIRD_PARTY_PUBLISHER: "INDEPENDENT_THIRD_PARTY_PUBLISHER",
  /** Only after accepted evidence explicitly connects Principal to The Savoy / identified program. */
  INDEPENDENT_RELATIONSHIP_CORROBORATION: "INDEPENDENT_RELATIONSHIP_CORROBORATION",
});

const FAIRMONT_OR_SAVOY_BRAND_HOST = /^(?:thesavoy\.co\.uk|fairmont\.com)$/i;

export function classifySourceOwnershipRole({
  url = "",
  organizationHost = ORGANIZER_HOST,
  organizationName = ORGANIZER_NAME,
  relationshipExplicitlyCorroborated = false,
} = {}) {
  const meta = resolvePublishingOrganization({ url, organizationHost, organizationName });
  if (meta.samePublisherAsOrganizer || !meta.independentOfOrganizer) {
    return {
      ...meta,
      ownershipRole: SOURCE_OWNERSHIP_ROLE.ORGANIZER_CLAIMED_PROVENANCE,
      countsAsIndependentRelationshipCorroboration: false,
      note: "Organizer-owned page is provenance/claim only — not independent corroboration.",
    };
  }
  if (relationshipExplicitlyCorroborated && meta.independentOfOrganizer) {
    return {
      ...meta,
      ownershipRole: SOURCE_OWNERSHIP_ROLE.INDEPENDENT_RELATIONSHIP_CORROBORATION,
      countsAsIndependentRelationshipCorroboration: true,
      note: "Independent publisher evidence explicitly connects Principal to The Savoy or the identified program.",
    };
  }
  if (FAIRMONT_OR_SAVOY_BRAND_HOST.test(meta.sourceOwnerHost)) {
    return {
      ...meta,
      ownershipRole: SOURCE_OWNERSHIP_ROLE.OFFICIAL_PROPERTY_OR_BRAND_ALTERNATE,
      countsAsIndependentRelationshipCorroboration: false,
      note: "Fairmont/Savoy official pages are alternate retrieval sources for the property/brand — not automatic Principal–Savoy relationship confirmation.",
    };
  }
  return {
    ...meta,
    ownershipRole: SOURCE_OWNERSHIP_ROLE.INDEPENDENT_THIRD_PARTY_PUBLISHER,
    countsAsIndependentRelationshipCorroboration: false,
    note: "Third-party publisher may corroborate only if retrieved content explicitly names Principal with The Savoy or the identified program.",
  };
}

function firecrawlCeiling() {
  try {
    return quoteBenchmarkCall({
      provider: "firecrawl",
      operation: "scrape",
      params: { url: "https://www.fairmont.com/savoy-london/", formats: ["markdown"] },
    });
  } catch {
    return { bounded: false, maxUsd: 0 };
  }
}

function loadSavedSavoyFailures(rootRelativePath = PRIOR_SAVOY_CERT_RESULT) {
  if (!fs.existsSync(rootRelativePath)) {
    return { ok: false, reason: "saved_savoy_cert_missing", sourcesVisited: [], researchObjective: null };
  }
  const saved = JSON.parse(fs.readFileSync(rootRelativePath, "utf8"));
  return {
    ok: true,
    sourcesVisited: saved.sourcesVisited || [],
    researchObjective: saved.researchObjective || null,
    remainingAuthorizationUsd: saved.remainingAuthorizationUsd ?? null,
    verificationStatus: saved.verificationStatus || null,
    actualSpentUsd: saved.actualSpentUsd ?? null,
  };
}

/**
 * Build candidate source paths from the recovery planner starting from saved Savoy failures.
 * Paths are eligible directions — supervisor chooses by VoI; Fairmont acceptance is not predetermined.
 */
export function buildLiveRecoveryCandidateSourcePaths({
  savedFailures = null,
  organizationHost = ORGANIZER_HOST,
} = {}) {
  const saved = savedFailures || loadSavedSavoyFailures();
  const researchQuestion = saved.researchObjective
    || "Does independently published evidence substantiate a historical programme or commercial relationship between Principal. London DMC and The Savoy?";

  const attempts = (saved.sourcesVisited || []).map((visit, index) => ({
    url: visit.url,
    attemptIndex: index + 1,
    provider: visit.provider || "firecrawl",
    method: visit.provider || "firecrawl",
    researchQuestion: visit.question || researchQuestion,
    publishingOrganization: visit.publishingOrganization,
    sourceOwnerHost: visit.sourceOwnerHost,
    independentOfOrganizer: visit.independentOfOrganizer !== false,
    retrievalOutcome: visit.ok === false ? RETRIEVAL_OUTCOME.PROVIDER_ERROR : RETRIEVAL_OUTCOME.SUCCESS_WITHOUT_RELEVANT_CONTENT,
    isSuccessfulRead: visit.ok === true,
    isFailedTechnicalAttempt: visit.ok !== true,
    providerError: visit.ok === false ? "firecrawl_http_500" : null,
  }));

  const lastFailure = attempts.filter((item) => item.isFailedTechnicalAttempt).slice(-1)[0] || attempts.slice(-1)[0];
  const recovery = proposeAdaptiveRecovery({
    failedAttempt: lastFailure,
    attempts,
    candidateQueue: [
      "https://www.fairmont.com/savoy-london/",
      "https://www.fairmont.com/en/hotels/london/fairmont-savoy.html",
      "https://www.hospitalitynet.org/",
      "https://www.meetingpack.com/",
    ],
    researchQuestion,
    organizationHost,
  });

  const classifyPath = (item, family) => {
    const ownership = classifySourceOwnershipRole({ url: item.url, organizationHost });
    return {
      url: item.url,
      recoveryType: item.recoveryType || family,
      reason: item.reason,
      sourceFamily: family,
      ownershipRole: ownership.ownershipRole,
      publishingOrganization: ownership.publishingOrganization,
      sourceOwnerHost: ownership.sourceOwnerHost,
      independentOfOrganizer: ownership.independentOfOrganizer,
      countsAsIndependentRelationshipCorroborationByDefault: false,
      note: ownership.note,
    };
  };

  const officialFairmontAlternates = (recovery.nextCandidates || [])
    .filter((item) => /fairmont\.com/i.test(item.url) || item.recoveryType === "alternate_publisher")
    .map((item) => classifyPath(item, "official_property_or_brand_alternate"));

  const independentEventOrProgramDirections = (recovery.nextCandidates || [])
    .filter((item) => item.recoveryType === "independent_coverage_search_direction")
    .map((item) => classifyPath(item, "independent_third_party_or_trade_coverage"));

  const otherIndependentFamilies = (recovery.nextCandidates || [])
    .filter((item) => !officialFairmontAlternates.some((alt) => alt.url === item.url)
      && !independentEventOrProgramDirections.some((alt) => alt.url === item.url))
    .map((item) => classifyPath(item, "other_independent_publisher_direction"));

  const blockedFailedSavoyUrls = [...new Set(attempts.map((item) => item.url))];

  return {
    researchQuestion,
    startingFromSavedFailures: attempts.map((item) => ({
      url: item.url,
      sourceOwnerHost: item.sourceOwnerHost,
      retrievalOutcome: item.retrievalOutcome,
      mustNotRepeat: true,
    })),
    blockedFailedUrls: blockedFailedSavoyUrls,
    cooledHosts: recovery.cooledHosts || [],
    pathA_officialFairmontAlternate: {
      role: SOURCE_OWNERSHIP_ROLE.OFFICIAL_PROPERTY_OR_BRAND_ALTERNATE,
      predeterminedAcceptance: false,
      predeterminedVerification: false,
      candidates: officialFairmontAlternates,
      note: "Fairmont is an official alternative retrieval source after thesavoy.co.uk failures — not automatic independent confirmation of Principal–Savoy.",
    },
    pathB_independentEventOrProgramSource: {
      role: SOURCE_OWNERSHIP_ROLE.INDEPENDENT_THIRD_PARTY_PUBLISHER,
      predeterminedAcceptance: false,
      predeterminedVerification: false,
      candidates: independentEventOrProgramDirections,
      note: "Relevant independently published event/program coverage if available; supervisor selects by VoI.",
    },
    pathC_otherCorroboratingSourceFamily: {
      role: SOURCE_OWNERSHIP_ROLE.INDEPENDENT_THIRD_PARTY_PUBLISHER,
      predeterminedAcceptance: false,
      predeterminedVerification: false,
      candidates: otherIndependentFamilies,
      note: "Another source family capable of corroborating the relationship if VoI and availability justify it.",
    },
    recoveryActions: recovery.actions || [],
    supervisorChoosesNextBy: [
      "commercial_value_of_information",
      "prior_retrieval_failures",
      "source_availability",
      "ownership_role_accuracy",
    ],
    doNotPredetermine: [
      "Fairmont must be accepted",
      "Principal–Savoy relationship must be verified",
      "Any specific URL must win selection",
    ],
    proposedNextIndependentSource: recovery.proposedNextIndependentSource
      ? classifyPath(recovery.proposedNextIndependentSource, recovery.proposedNextIndependentSource.recoveryType || "alternate_publisher")
      : null,
  };
}

export function buildLiveRecoverySuccessCriteria() {
  return {
    mustDemonstrate: [
      {
        id: 1,
        criterion: "Previously failed Savoy URLs are not repeatedly retrieved",
        passIf: "No URL from the saved failed thesavoy.co.uk attempt set is re-fetched",
      },
      {
        id: 2,
        criterion: "Supervisor selects a defensible alternative",
        passIf: "Selected URL is outside cooled/failed Savoy set, non-Principal, and justified by recovery/VoI ranking",
      },
      {
        id: 3,
        criterion: "At least one new source is successfully read, or retrieval failure is correctly reported",
        passIf: "SUCCESS_* outcome with readable body OR PROVIDER_ERROR/THIN/ACCESS_RESTRICTED recorded without fake negative relationship claims",
      },
      {
        id: 4,
        criterion: "Source ownership is accurately classified",
        passIf: "Fairmont/Savoy brand pages labeled OFFICIAL_PROPERTY_OR_BRAND_ALTERNATE; third parties labeled INDEPENDENT_THIRD_PARTY_PUBLISHER; Principal never independent",
      },
      {
        id: 5,
        criterion: "Unsupported relationship claims remain unverified",
        passIf: "INDEPENDENTLY_VERIFIED only when accepted evidence explicitly connects Principal to The Savoy or identified program; else LEAD_UNVERIFIED or INDEPENDENT_CONFIRMATION_UNAVAILABLE",
      },
      {
        id: 6,
        criterion: "New evidence changes the research assessment when warranted",
        passIf: "Successful corroborating read upgrades verification; successful non-corroborating read updates assessment without inventing relationship; failures do not invent negation",
      },
      {
        id: 7,
        criterion: "Final stop accurately identifies cause",
        passIf: "Stop record separates research completeness, commercial next action, retrieval failures, budget/attempt limits, and independent verification status",
      },
      {
        id: 8,
        criterion: "Commercial classification and publication eligibility remain unchanged",
        passIf: "salesReadiness/commercialStage stay NEEDS_RESEARCH; publicationEligibilityChanged=false; no Airtable/deploy",
      },
    ],
    failIf: [
      "Fairmont page auto-accepted as Principal–Savoy corroboration without explicit Principal mention",
      "Repeated retrieval of saved failed Savoy URLs",
      "Principal-owned page counted as independent corroboration",
      "Rejected factual relationship claims created from empty/failed responses",
      "Reuse of prior longform/Savoy/benchmark ledger balances",
      "Independent-source investigation labeled complete with zero successful independent reads",
      "Live execution without AUTHORIZED manifest",
    ],
  };
}

export function buildLiveRecoveryAuthorizationManifest() {
  const planner = quotePlannerUsd();
  const firecrawl = firecrawlCeiling();
  const maxQuotedUsd = Number((
    LIVE_RECOVERY_MAX_PLANNER_CALLS * planner.estimatedUsd
    + LIVE_RECOVERY_MAX_SUBSTANTIVE_RETRIEVALS * (firecrawl.bounded ? Number(firecrawl.maxUsd) : 0)
  ).toFixed(6));

  return {
    version: LIVE_RECOVERY_CERT_VERSION,
    status: "AWAITING_AUTHORIZATION",
    objective: "Demonstrate live retrieval-recovery and independent-corroboration handling for Principal–Savoy after saved thesavoy.co.uk Firecrawl failures.",
    researchQuestion: "Does independently published evidence substantiate a historical programme or commercial relationship between Principal. London DMC and The Savoy?",
    orchestrator: "opportunity-research-supervisor-v1 (Node.js Research Supervisor sole orchestrator)",
    startingPoint: {
      priorCertification: PRIOR_SAVOY_CERT_RESULT,
      interpretation: "Prior Savoy run is a retrieval-recovery failure, not evidence that Principal–Savoy is false.",
      reusePriorSavoyLedgerBalance: false,
      reuseLongformOrBenchmarkBalances: false,
    },
    sourceOwnershipRules: {
      distinguish: [
        "URL/domain diversity",
        "Actual source/publisher ownership",
        "Independent corroboration of a specific commercial relationship",
      ],
      fairmontSavoyPage: SOURCE_OWNERSHIP_ROLE.OFFICIAL_PROPERTY_OR_BRAND_ALTERNATE,
      fairmontIsNotAutomaticIndependentConfirmation: true,
      independentVerificationRequires: "Retrieved evidence explicitly connects Principal to The Savoy or the identified program",
      organizerHostForbidden: ORGANIZER_HOST,
    },
    workflow: {
      pathPredetermined: false,
      fairmontAcceptancePredetermined: false,
      relationshipVerificationPredetermined: false,
      maxSubstantiveQuestions: LIVE_RECOVERY_MAX_SUBSTANTIVE_QUESTIONS,
      maxPlannerCalls: LIVE_RECOVERY_MAX_PLANNER_CALLS,
      maxSubstantiveRetrievals: LIVE_RECOVERY_MAX_SUBSTANTIVE_RETRIEVALS,
      maxFailedTechnicalAttempts: LIVE_RECOVERY_MAX_FAILED_TECHNICAL_ATTEMPTS,
      selectionDrivers: [
        "commercial_value_of_information",
        "prior_failures",
        "source_availability",
      ],
      capabilities: [
        "adaptive_source_recovery",
        "retrieval_outcome_vs_evidence_separation",
        "source_ownership_role_classification",
        "explicit_principal_savoy_corroboration_gate",
        "separated_stop_reporting",
      ],
    },
    operations: [
      {
        step: "planner_loop",
        operation: "planner_call",
        model: planner.model,
        maxCalls: LIVE_RECOVERY_MAX_PLANNER_CALLS,
        maxQuotedUsdEach: planner.estimatedUsd,
        hardCeilingUsdEach: PLANNER_CEILING_USD,
      },
      {
        step: "recovery_retrieval_loop",
        operation: "conditional_source_retrieval",
        provider: "direct_public_preferred_firecrawl_fallback_if_priced",
        maxSubstantiveRetrievals: LIVE_RECOVERY_MAX_SUBSTANTIVE_RETRIEVALS,
        maxFailedTechnicalAttempts: LIVE_RECOVERY_MAX_FAILED_TECHNICAL_ATTEMPTS,
        maxQuotedUsdEach: firecrawl.bounded ? Number(firecrawl.maxUsd) : 0,
        hardCeilingUsdEach: firecrawl.bounded ? Number(firecrawl.maxUsd) : 0,
        paidProvidersRefused: ["parallel", "exa"],
        unpricedProvidersRefused: true,
      },
    ],
    maxQuotedUsd,
    hardCeilingUsd: LIVE_RECOVERY_BUDGET_USD,
    perCallCeilingUsd: SAVOY_CALL_CEILING_USD,
    ledger: LIVE_RECOVERY_LEDGER_RELATIVE,
    usesPriorBenchmarkOrCertificationLedgers: false,
    usesPriorSavoyFollowupLedgerBalance: false,
    usesLongformLedgerRemainingBalance: false,
    productionAllowlistsUnchanged: true,
    productionFlagsUnchanged: true,
    publicationEligibilityChanged: false,
    airtableWrites: 0,
    deploy: false,
    liveExecutionAuthorized: false,
    certificationCriteria: buildLiveRecoverySuccessCriteria(),
    relatedEntity: RELATED_ENTITY,
    organizer: ORGANIZER_NAME,
  };
}

export function runLiveRecoveryOfflinePreparationChecks({
  savedPath = PRIOR_SAVOY_CERT_RESULT,
} = {}) {
  const checks = [];
  const push = (name, ok, detail = "") => {
    checks.push({ name, ok: Boolean(ok), detail });
  };

  const saved = loadSavedSavoyFailures(savedPath);
  push("saved_savoy_failures_present", saved.ok === true, saved.reason || "");
  push("three_saved_failures", (saved.sourcesVisited || []).length === 3);

  const offlineRecovery = runOfflineSavoyRetrievalRecoveryCertification({
    savedCertificationResult: saved.ok
      ? {
        researchObjective: saved.researchObjective,
        sourcesVisited: saved.sourcesVisited,
        remainingAuthorizationUsd: saved.remainingAuthorizationUsd,
      }
      : undefined,
  });
  push("offline_retrieval_recovery_suite_passes", offlineRecovery.certificationPassed === true);

  const paths = buildLiveRecoveryCandidateSourcePaths({ savedFailures: saved });
  push("failed_savoy_urls_blocked_from_repeat", (paths.blockedFailedUrls || []).length === 3);
  push("fairmont_official_alternate_present", (paths.pathA_officialFairmontAlternate.candidates || []).length > 0);
  push(
    "fairmont_not_predetermined_verification",
    paths.pathA_officialFairmontAlternate.predeterminedVerification === false
      && paths.doNotPredetermine.includes("Principal–Savoy relationship must be verified"),
  );
  push(
    "fairmont_ownership_is_brand_alternate_not_auto_corroboration",
    (paths.pathA_officialFairmontAlternate.candidates || []).every(
      (item) => item.ownershipRole === SOURCE_OWNERSHIP_ROLE.OFFICIAL_PROPERTY_OR_BRAND_ALTERNATE
        && item.countsAsIndependentRelationshipCorroborationByDefault === false,
    ),
  );
  push(
    "independent_event_or_coverage_path_present",
    (paths.pathB_independentEventOrProgramSource.candidates || []).length > 0,
  );
  push(
    "principal_news_not_in_independent_paths",
    ![...paths.pathA_officialFairmontAlternate.candidates,
      ...paths.pathB_independentEventOrProgramSource.candidates,
      ...paths.pathC_otherCorroboratingSourceFamily.candidates]
      .some((item) => /principallondon\.com/i.test(item.url)),
  );

  const fairmontClass = classifySourceOwnershipRole({
    url: "https://www.fairmont.com/savoy-london/",
  });
  push(
    "fairmont_classify_official_alternate",
    fairmontClass.ownershipRole === SOURCE_OWNERSHIP_ROLE.OFFICIAL_PROPERTY_OR_BRAND_ALTERNATE
      && fairmontClass.countsAsIndependentRelationshipCorroboration === false,
  );

  const principalUrl = "https://www.principallondon.com/2026/05/11/an-evening-at-the-savoy";
  const principalClass = classifySourceOwnershipRole({ url: principalUrl });
  push(
    "principal_classify_organizer_provenance",
    principalClass.ownershipRole === SOURCE_OWNERSHIP_ROLE.ORGANIZER_CLAIMED_PROVENANCE
      && isIndependentPublisherUrl(principalUrl, ORGANIZER_HOST) === false,
  );

  const thirdPartyClass = classifySourceOwnershipRole({
    url: "https://www.hospitalitynet.org/",
  });
  push(
    "trade_coverage_classify_third_party",
    thirdPartyClass.ownershipRole === SOURCE_OWNERSHIP_ROLE.INDEPENDENT_THIRD_PARTY_PUBLISHER,
  );

  const corroborated = classifySourceOwnershipRole({
    url: "https://www.hospitalitynet.org/example",
    relationshipExplicitlyCorroborated: true,
  });
  push(
    "explicit_corroboration_role_only_when_flagged",
    corroborated.ownershipRole === SOURCE_OWNERSHIP_ROLE.INDEPENDENT_RELATIONSHIP_CORROBORATION,
  );

  const manifest = buildLiveRecoveryAuthorizationManifest();
  push("manifest_awaiting_authorization", manifest.status === "AWAITING_AUTHORIZATION");
  push("manifest_live_not_authorized", manifest.liveExecutionAuthorized === false);
  push("isolated_new_ledger_path", manifest.ledger === LIVE_RECOVERY_LEDGER_RELATIVE);
  push("does_not_reuse_prior_savoy_balance", manifest.usesPriorSavoyFollowupLedgerBalance === false);
  push("does_not_reuse_longform_or_benchmark", manifest.usesPriorBenchmarkOrCertificationLedgers === false
    && manifest.usesLongformLedgerRemainingBalance === false);
  push("hard_ceiling_is_0_05", manifest.hardCeilingUsd === LIVE_RECOVERY_BUDGET_USD);
  push("publication_eligibility_unchanged", manifest.publicationEligibilityChanged === false);
  push("eight_success_criteria_present", (manifest.certificationCriteria?.mustDemonstrate || []).length === 8);

  return {
    version: LIVE_RECOVERY_CERT_VERSION,
    offline: true,
    liveCalls: 0,
    spent: false,
    checks,
    preparationPassed: checks.every((item) => item.ok),
    candidateSourcePaths: paths,
    manifest,
  };
}

export function prepareLiveRecoveryCertification() {
  const preparationChecks = runLiveRecoveryOfflinePreparationChecks();
  const manifest = preparationChecks.manifest;
  const candidateSourcePaths = preparationChecks.candidateSourcePaths;
  const successCriteria = buildLiveRecoverySuccessCriteria();
  return {
    prepared: true,
    executed: false,
    liveCalls: 0,
    spent: false,
    status: "AWAITING_AUTHORIZATION",
    version: LIVE_RECOVERY_CERT_VERSION,
    hardCeilingUsd: LIVE_RECOVERY_BUDGET_USD,
    maxQuotedUsd: manifest.maxQuotedUsd,
    ledger: LIVE_RECOVERY_LEDGER_RELATIVE,
    reportsDir: LIVE_RECOVERY_REPORTS_DIR,
    authorizationManifest: manifest,
    candidateSourcePaths,
    successCriteria,
    offlinePreparationChecks: preparationChecks.checks,
    preparationPassed: preparationChecks.preparationPassed,
    controls: {
      noLivePaidResearchWithoutExplicitAuthorization: true,
      noReuseExistingResearchBalances: true,
      noDeploy: true,
      noPublish: true,
      noAirtableWrites: true,
      noCustomerFacingClassificationChanges: true,
    },
    stopBeforeLiveExecution: true,
  };
}

const FORBIDDEN_LIVE_RECOVERY_LEDGER = /parallel-live-comparison|cohort-a-live|segment-e2e|paid-multisegment|parallel-budget|gdi-planner-evaluation|gdi-autonomous-research-certification|gdi-autonomous-deep-research|gdi-autonomous-longform-research-certification|gdi-cross-organization-followup-savoy-v1\/budget-ledger/i;

function normalizeUrl(value) {
  return String(value || "").trim().replace(/\/+$/, "").toLowerCase();
}

function pathDir(filePath) {
  const parts = String(filePath || "").replace(/\\/g, "/").split("/");
  parts.pop();
  return parts.join("/") || ".";
}

function writeJsonAtomic(filePath, data) {
  fs.mkdirSync(pathDir(filePath), { recursive: true });
  const tmp = `${filePath}.${process.pid}.tmp`;
  fs.writeFileSync(tmp, `${JSON.stringify(data, null, 2)}\n`);
  fs.rmSync(filePath, { force: true });
  fs.renameSync(tmp, filePath);
}

export function assertLiveRecoveryLedger(ledgerPath) {
  const normalized = String(ledgerPath || "").replace(/\\/g, "/");
  if (FORBIDDEN_LIVE_RECOVERY_LEDGER.test(normalized)) {
    const err = new Error("live_recovery_ledger_not_isolated");
    err.code = "LIVE_RECOVERY_LEDGER_NOT_ISOLATED";
    throw err;
  }
  if (!normalized.endsWith(LIVE_RECOVERY_LEDGER_RELATIVE)) {
    const err = new Error("live_recovery_ledger_path_refused");
    err.code = "LIVE_RECOVERY_LEDGER_PATH_REFUSED";
    throw err;
  }
}

function emptyLiveRecoveryLedger() {
  return {
    version: `${LIVE_RECOVERY_CERT_VERSION}-ledger`,
    currency: "USD",
    budgetUsd: LIVE_RECOVERY_BUDGET_USD,
    isolatedFromAllPriorResearchLedgers: true,
    usesPriorSavoyFollowupLedgerBalance: false,
    usesLongformLedgerRemainingBalance: false,
    reservedUsd: 0,
    estimatedSpentUsd: 0,
    plannerCalls: 0,
    retrievals: 0,
    substantiveQuestions: 0,
    entries: [],
    updatedAt: new Date().toISOString(),
  };
}

export function loadOrCreateLiveRecoveryLedger(ledgerPath) {
  assertLiveRecoveryLedger(ledgerPath);
  if (!fs.existsSync(ledgerPath)) {
    const ledger = emptyLiveRecoveryLedger();
    writeJsonAtomic(ledgerPath, ledger);
    return ledger;
  }
  const ledger = JSON.parse(fs.readFileSync(ledgerPath, "utf8"));
  if (Number(ledger.budgetUsd) !== LIVE_RECOVERY_BUDGET_USD) {
    const err = new Error("live_recovery_budget_refused");
    err.code = "LIVE_RECOVERY_BUDGET_REFUSED";
    throw err;
  }
  return ledger;
}

export function reserveLiveRecoveryCall({ ledgerPath, jobKey, quotedUsd, kind = "planner" } = {}) {
  const ledger = loadOrCreateLiveRecoveryLedger(ledgerPath);
  const quoted = Number(quotedUsd);
  if (!(quoted >= 0) || (quoted > 0 && quoted - SAVOY_CALL_CEILING_USD > 1e-9)) {
    const err = new Error("live_recovery_quote_above_call_ceiling");
    err.code = "LIVE_RECOVERY_QUOTE_ABOVE_CALL_CEILING";
    throw err;
  }
  const existing = (ledger.entries || []).find((entry) => entry.jobKey === jobKey);
  if (existing && (existing.status === "spent" || existing.status === "failed")) {
    return { ok: false, reason: "job_already_used", entry: existing };
  }
  if (existing && existing.status === "reserved") {
    return { ok: true, reservationId: existing.reservationId, alreadyReserved: true };
  }
  if (kind === "planner" && Number(ledger.plannerCalls || 0) >= LIVE_RECOVERY_MAX_PLANNER_CALLS) {
    const err = new Error("live_recovery_planner_cap");
    err.code = "LIVE_RECOVERY_PLANNER_CAP";
    throw err;
  }
  const committed = Number(ledger.estimatedSpentUsd || 0) + Number(ledger.reservedUsd || 0);
  if (quoted > 0 && committed + quoted - LIVE_RECOVERY_BUDGET_USD > 1e-9) {
    const err = new Error("live_recovery_budget_exhausted");
    err.code = "LIVE_RECOVERY_BUDGET_EXHAUSTED";
    throw err;
  }
  const reservationId = `rsv_lr_${Date.now().toString(36)}_${randomBytes(3).toString("hex")}`;
  if (quoted > 0) {
    ledger.reservedUsd = Number((Number(ledger.reservedUsd || 0) + quoted).toFixed(6));
    ledger.entries.push({
      reservationId,
      jobKey,
      kind,
      quotedUsd: quoted,
      actualUsd: null,
      status: "reserved",
      reservedAt: new Date().toISOString(),
    });
  } else {
    ledger.entries.push({
      reservationId,
      jobKey,
      kind,
      quotedUsd: 0,
      actualUsd: 0,
      status: "spent",
      spentAt: new Date().toISOString(),
    });
    if (kind === "retrieval") ledger.retrievals = Number(ledger.retrievals || 0) + 1;
    if (kind === "planner") ledger.plannerCalls = Number(ledger.plannerCalls || 0) + 1;
  }
  writeJsonAtomic(ledgerPath, ledger);
  return { ok: true, reservationId };
}

export function commitLiveRecoveryCall({ ledgerPath, reservationId, actualUsd, usage } = {}) {
  const ledger = loadOrCreateLiveRecoveryLedger(ledgerPath);
  const entry = (ledger.entries || []).find((item) => item.reservationId === reservationId);
  if (!entry) {
    const err = new Error("live_recovery_reservation_missing");
    err.code = "LIVE_RECOVERY_RESERVATION_MISSING";
    throw err;
  }
  if (entry.status === "spent") return { ok: true, idempotent: true };
  const actual = Number(actualUsd);
  const nextSpent = Number((Number(ledger.estimatedSpentUsd || 0) + actual).toFixed(6));
  if (!(actual >= 0) || actual - Number(entry.quotedUsd) > 1e-9 || nextSpent - LIVE_RECOVERY_BUDGET_USD > 1e-9) {
    const err = new Error("live_recovery_actual_above_reservation");
    err.code = "LIVE_RECOVERY_ACTUAL_ABOVE_RESERVATION";
    throw err;
  }
  entry.status = "spent";
  entry.actualUsd = actual;
  entry.usage = usage || null;
  entry.spentAt = new Date().toISOString();
  ledger.reservedUsd = Number(Math.max(0, Number(ledger.reservedUsd || 0) - Number(entry.quotedUsd)).toFixed(6));
  ledger.estimatedSpentUsd = nextSpent;
  if (entry.kind === "planner") ledger.plannerCalls = Number(ledger.plannerCalls || 0) + 1;
  if (entry.kind === "retrieval") ledger.retrievals = Number(ledger.retrievals || 0) + 1;
  writeJsonAtomic(ledgerPath, ledger);
  return { ok: true };
}

export function releaseLiveRecoveryCall({ ledgerPath, reservationId, reason } = {}) {
  const ledger = loadOrCreateLiveRecoveryLedger(ledgerPath);
  const entry = (ledger.entries || []).find((item) => item.reservationId === reservationId);
  if (!entry || entry.status === "spent") return { ok: true };
  entry.status = "released";
  entry.releasedAt = new Date().toISOString();
  entry.releaseReason = reason || "not_billed";
  ledger.reservedUsd = Number(Math.max(0, Number(ledger.reservedUsd || 0) - Number(entry.quotedUsd || 0)).toFixed(6));
  writeJsonAtomic(ledgerPath, ledger);
  return { ok: true };
}

export function failLiveRecoveryCall({ ledgerPath, reservationId, reason } = {}) {
  const ledger = loadOrCreateLiveRecoveryLedger(ledgerPath);
  const entry = (ledger.entries || []).find((item) => item.reservationId === reservationId);
  if (!entry || entry.status === "spent") return { ok: true };
  entry.status = "failed";
  entry.failedAt = new Date().toISOString();
  entry.failureReason = reason || "call_failed";
  ledger.reservedUsd = Number(Math.max(0, Number(ledger.reservedUsd || 0) - Number(entry.quotedUsd || 0)).toFixed(6));
  writeJsonAtomic(ledgerPath, ledger);
  return { ok: true };
}

function pageMentionsPrincipal(text = "") {
  return /\bprincipal(?:\.?\s*london|\s+dmc|\s+ge)?\b/i.test(String(text || ""));
}

function pageMentionsSavoy(text = "") {
  return /\b(?:the\s+)?savoy\b/i.test(String(text || ""));
}

function pageCorroboratesPrincipalSavoyRelationship(text = "") {
  const body = String(text || "");
  if (!pageMentionsPrincipal(body) || !pageMentionsSavoy(body)) return false;
  const window = body.replace(/\s+/g, " ");
  return /principal[^.]{0,240}savoy|savoy[^.]{0,240}principal/i.test(window)
    || /principal[^.]{0,120}(?:evening|event|incentive|programme|program|partner|hosted|reception)[^.]{0,120}savoy/i.test(window)
    || /savoy[^.]{0,120}(?:evening|event|incentive|programme|program|partner|hosted|reception)[^.]{0,120}principal/i.test(window);
}

function rankLiveRecoveryCandidate(url, question = "", ownershipRole = null) {
  const host = resolvePublishingOrganization({ url, organizationHost: ORGANIZER_HOST }).sourceOwnerHost;
  let score = 0;
  const q = String(question || "").toLowerCase();
  if (/fairmont\.com/i.test(host)) score += 18;
  if (/hospitalitynet|meetingpack|conference|incentive|event/i.test(host + url)) score += 14;
  if (ownershipRole === SOURCE_OWNERSHIP_ROLE.INDEPENDENT_THIRD_PARTY_PUBLISHER) score += 8;
  if (ownershipRole === SOURCE_OWNERSHIP_ROLE.OFFICIAL_PROPERTY_OR_BRAND_ALTERNATE) score += 6;
  if (/press|media|news|event|meeting|program/i.test(url)) score += 4;
  if (/savoy/.test(q) && /savoy|fairmont/i.test(url + host)) score += 3;
  if (/thesavoy\.co\.uk/i.test(host)) score -= 50;
  if (/principallondon\.com/i.test(host)) score -= 100;
  if (classifyTechnicalSource(url).rejected) score -= 100;
  if (!isIndependentPublisherUrl(url, ORGANIZER_HOST)) score -= 100;
  return score;
}

function selectLiveRecoverySource({ candidates, investigatedUrls, blockedUrls, cooledHosts, question }) {
  const held = new Set([...(investigatedUrls || []), ...(blockedUrls || [])].map(normalizeUrl));
  const cooled = new Set(cooledHosts || []);
  const ranked = [...new Set((candidates || []).map(String).filter(Boolean))]
    .filter((url) => !held.has(normalizeUrl(url)))
    .filter((url) => isIndependentPublisherUrl(url, ORGANIZER_HOST))
    .filter((url) => !classifyTechnicalSource(url).rejected)
    .filter((url) => {
      try {
        const host = new URL(url).hostname.replace(/^www\./i, "").toLowerCase();
        return !cooled.has(host);
      } catch {
        return false;
      }
    })
    .map((url) => {
      const ownership = classifySourceOwnershipRole({ url });
      return { url, score: rankLiveRecoveryCandidate(url, question, ownership.ownershipRole), ownership };
    })
    .filter((item) => item.score > 0)
    .sort((a, b) => b.score - a.score || a.url.localeCompare(b.url));
  return { selected: ranked[0] || null, ranking: ranked.slice(0, 8) };
}

async function billedLiveRecoveryPlanner({ ledgerPath, jobKey, planInput }) {
  const reservation = reserveLiveRecoveryCall({
    ledgerPath,
    jobKey,
    quotedUsd: SAVOY_CALL_CEILING_USD,
    kind: "planner",
  });
  if (!reservation.ok) return { skipped: true, reason: reservation.reason, entry: reservation.entry };
  let plan;
  try {
    plan = await planDynamicResearch({ ...planInput, authorizePlannerModel: true });
  } catch (err) {
    failLiveRecoveryCall({ ledgerPath, reservationId: reservation.reservationId, reason: err.code || "planner_threw" });
    throw err;
  }
  if (plan.plannerModelCalls === 1 && plan.plannerActualUsd != null) {
    commitLiveRecoveryCall({
      ledgerPath,
      reservationId: reservation.reservationId,
      actualUsd: plan.plannerActualUsd,
      usage: plan.plannerUsage,
    });
  } else if (plan.plannerModelCalls === 1) {
    failLiveRecoveryCall({ ledgerPath, reservationId: reservation.reservationId, reason: "usage_missing" });
  } else {
    releaseLiveRecoveryCall({ ledgerPath, reservationId: reservation.reservationId, reason: plan.fallbackReason || "model_not_called" });
  }
  return { plan };
}

async function fetchLiveRecoveryPage({ url, fetchPage, ledgerPath, jobKey }) {
  if (!isIndependentPublisherUrl(url, ORGANIZER_HOST)) {
    return {
      ok: false,
      refused: true,
      reason: "organizer_publisher_forbidden",
      text: "",
      html: "",
      actualUsd: 0,
      provider: null,
      providerError: "organizer_publisher_forbidden",
    };
  }
  const direct = await fetchPage(url);
  const body = String(direct?.text || "");
  if (direct?.ok && body.trim().length >= 200) {
    reserveLiveRecoveryCall({ ledgerPath, jobKey, quotedUsd: 0, kind: "retrieval" });
    return {
      ok: true,
      text: body.slice(0, 25000),
      html: String(direct.html || ""),
      provider: "direct_public",
      method: "direct_public",
      actualUsd: 0,
    };
  }
  const quote = quoteBenchmarkCall({
    provider: "firecrawl",
    operation: "scrape",
    params: { url, formats: ["markdown"] },
  });
  const key = process.env.FIRECRAWL_API_KEY || process.env.FIRECRAWL_KEY;
  if (!quote.bounded || !key) {
    return {
      ok: false,
      text: body.slice(0, 25000),
      html: String(direct?.html || ""),
      provider: "direct_public",
      method: "direct_public",
      actualUsd: 0,
      thin: true,
      providerError: !quote.bounded ? "firecrawl_unpriced" : (body.trim() ? "thin_or_unreadable_direct_public" : "empty_direct_public"),
    };
  }
  const reservation = reserveLiveRecoveryCall({
    ledgerPath,
    jobKey: `${jobKey}-firecrawl`,
    quotedUsd: Number(quote.maxUsd),
    kind: "retrieval",
  });
  if (!reservation.ok) {
    return {
      ok: false,
      text: body.slice(0, 25000),
      provider: "direct_public",
      method: "direct_public",
      actualUsd: 0,
      providerError: reservation.reason || "firecrawl_reservation_refused",
      thin: true,
    };
  }
  try {
    const res = await fetch("https://api.firecrawl.dev/v2/scrape", {
      method: "POST",
      headers: { Authorization: `Bearer ${key}`, "Content-Type": "application/json" },
      body: JSON.stringify({ url, formats: ["markdown"] }),
    });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) {
      failLiveRecoveryCall({ ledgerPath, reservationId: reservation.reservationId, reason: `firecrawl_http_${res.status}` });
      return {
        ok: false,
        text: "",
        provider: "firecrawl",
        method: "firecrawl",
        actualUsd: 0,
        httpStatus: res.status,
        providerError: `firecrawl_http_${res.status}`,
      };
    }
    const md = String(json?.data?.markdown || json?.markdown || "");
    if (!md.trim()) {
      failLiveRecoveryCall({ ledgerPath, reservationId: reservation.reservationId, reason: "firecrawl_empty_body" });
      return {
        ok: false,
        text: "",
        provider: "firecrawl",
        method: "firecrawl",
        actualUsd: 0,
        providerError: "firecrawl_empty_body",
        thin: true,
      };
    }
    commitLiveRecoveryCall({ ledgerPath, reservationId: reservation.reservationId, actualUsd: Number(quote.maxUsd) });
    return {
      ok: true,
      text: md.slice(0, 25000),
      html: "",
      provider: "firecrawl",
      method: "firecrawl",
      actualUsd: Number(quote.maxUsd),
    };
  } catch (err) {
    failLiveRecoveryCall({ ledgerPath, reservationId: reservation.reservationId, reason: err.message || "firecrawl_failed" });
    return {
      ok: false,
      text: "",
      provider: "firecrawl",
      method: "firecrawl",
      actualUsd: 0,
      providerError: err.message || "firecrawl_failed",
    };
  }
}

function evaluateLiveRecoveryEvidence({ url, text, html, ownership }) {
  const accepted = [];
  const rejected = [];
  const corroborates = pageCorroboratesPrincipalSavoyRelationship(text);
  const mentionsSavoyOnly = pageMentionsSavoy(text) && !pageMentionsPrincipal(text);
  const mentionsPrincipalOnly = pageMentionsPrincipal(text) && !pageMentionsSavoy(text);

  if (corroborates) {
    const match = String(text).replace(/\s+/g, " ").match(/.{0,80}(?:principal.{0,160}savoy|savoy.{0,160}principal).{0,80}/i);
    accepted.push({
      evidenceId: `ev_${createHash("sha256").update(`${url}:corroboration`).digest("hex").slice(0, 12)}`,
      url,
      statement: (match?.[0] || "Page co-names Principal and The Savoy in a programme/commercial context.").replace(/\s+/g, " ").trim().slice(0, 400),
      topic: "relationship_mention",
      accepted: true,
      publishingOrganization: ownership.publishingOrganization,
      sourceOwnerHost: ownership.sourceOwnerHost,
      ownershipRole: ownership.ownershipRole,
    });
  } else if (mentionsSavoyOnly) {
    accepted.push({
      evidenceId: `ev_${createHash("sha256").update(`${url}:savoy_only`).digest("hex").slice(0, 12)}`,
      url,
      statement: "Page describes The Savoy / property but does not name Principal; publisher ownership ≠ relationship corroboration.",
      topic: "property_description_without_principal",
      accepted: true,
      publishingOrganization: ownership.publishingOrganization,
      sourceOwnerHost: ownership.sourceOwnerHost,
      ownershipRole: ownership.ownershipRole,
    });
    // Missing Principal mention is not a contradiction.
  } else if (mentionsPrincipalOnly) {
    accepted.push({
      evidenceId: `ev_${createHash("sha256").update(`${url}:principal_only`).digest("hex").slice(0, 12)}`,
      url,
      statement: "Page names Principal without establishing a Savoy programme relationship in accepted excerpt.",
      topic: "principal_without_savoy_link",
      accepted: true,
      publishingOrganization: ownership.publishingOrganization,
      sourceOwnerHost: ownership.sourceOwnerHost,
      ownershipRole: ownership.ownershipRole,
    });
  }

  rejected.push({
    claim: "Page automatically independently verifies Principal–Savoy because Fairmont/Savoy brand published it",
    reason: "fairmont_brand_alternate_not_automatic_independent_corroboration",
    url,
  });
  rejected.push({
    claim: "Page establishes open Savoy accommodation demand for Nobu",
    reason: "out_of_scope_nobu_demand_not_asserted",
    url,
  });
  rejected.push({
    claim: "Page establishes a Nobu–Principal relationship",
    reason: "nobu_relationship_not_in_scope",
    url,
  });

  const links = extractCandidateUrlsFromHtml(html || text, url)
    .filter((link) => isIndependentPublisherUrl(link, ORGANIZER_HOST))
    .filter((link) => !classifyTechnicalSource(link).rejected);

  // Independent verification requires a separate (non-brand-family) publisher with explicit relationship text.
  const independentlyVerified = corroborates
    && ownership.ownershipRole === SOURCE_OWNERSHIP_ROLE.INDEPENDENT_THIRD_PARTY_PUBLISHER;

  return {
    corroborates,
    independentlyVerified,
    accepted,
    rejected,
    discoveredIndependentLinks: links,
  };
}

/**
 * Live retrieval-recovery certification under the isolated $0.05 ledger.
 */
export async function executeAuthorizedLiveRecoveryResearch(input = {}) {
  if (input.authorizeLive !== true || input.manifestAuthorized !== true || input.manifest?.status !== "AUTHORIZED") {
    return { executed: false, liveCalls: 0, reason: "awaiting_explicit_authorization", publicationEligibilityChanged: false };
  }
  if (Number(input.manifest?.hardCeilingUsd) !== LIVE_RECOVERY_BUDGET_USD) {
    return { executed: false, reason: "ceiling_mismatch", publicationEligibilityChanged: false, spent: false };
  }
  if (input.manifest?.usesPriorSavoyFollowupLedgerBalance === true
    || input.manifest?.usesPriorBenchmarkOrCertificationLedgers === true
    || input.manifest?.usesLongformLedgerRemainingBalance === true) {
    return { executed: false, reason: "prior_ledger_reuse_forbidden", publicationEligibilityChanged: false, spent: false };
  }
  if (!input.fetchPage || typeof input.fetchPage !== "function") {
    return { executed: false, reason: "fetch_page_required", publicationEligibilityChanged: false };
  }

  const ledgerPath = input.ledgerPath;
  const checkpointDir = input.checkpointDir;
  const id = "gdi_cross_org_live_recovery_principal_savoy";
  const paths = buildLiveRecoveryCandidateSourcePaths();
  const researchQuestion = paths.researchQuestion;
  const blockedFailedUrls = paths.blockedFailedUrls || [];
  const cooledHosts = [...(paths.cooledHosts || [])];

  const seedQueue = [
    ...(paths.pathA_officialFairmontAlternate.candidates || []).map((item) => item.url),
    ...(paths.pathB_independentEventOrProgramSource.candidates || []).map((item) => item.url),
    ...(paths.pathC_otherCorroboratingSourceFamily.candidates || []).map((item) => item.url),
  ].filter((url) => !blockedFailedUrls.map(normalizeUrl).includes(normalizeUrl(url)));

  let state = null;
  const checkpointFile = maturationMemoryPath(checkpointDir, id);
  if (fs.existsSync(checkpointFile)) {
    state = JSON.parse(fs.readFileSync(checkpointFile, "utf8"));
  }
  if (!state || state.status !== "IN_PROGRESS") {
    state = {
      id,
      status: "IN_PROGRESS",
      investigatedUrls: [...blockedFailedUrls],
      blockedFailedUrls,
      cooledHosts,
      candidateQueue: seedQueue,
      sourceAttempts: [],
      retrievals: [],
      substantiveQuestions: [],
      acceptedClaims: [],
      rejectedClaims: [],
      relatedEntityFollowUps: [],
      followUpQuestions: [],
      planLog: [
        {
          event: "resume_from_saved_failures",
          blockedFailedUrls,
          cooledHosts,
          note: "Prior Savoy failures are retrieval-recovery context; not negative relationship findings.",
        },
      ],
      plannerModelCalls: 0,
      disposition: null,
      stoppingTrigger: null,
      commercialNextAction: null,
      verificationStatus: RELATIONSHIP_VERIFICATION.LEAD_UNVERIFIED,
      independentlyVerified: false,
      publicationEligibilityChanged: false,
      salesReadiness: "NEEDS_RESEARCH",
      commercialStage: "NEEDS_RESEARCH",
    };
    writeMaturationCheckpointAtomic(checkpointDir, state);
  }

  let loops = 0;
  while (state.status === "IN_PROGRESS" && loops < 10) {
    loops += 1;
    const attemptBudget = summarizeAttemptBudget({
      attempts: state.sourceAttempts,
      substantiveRetrievalCap: LIVE_RECOVERY_MAX_SUBSTANTIVE_RETRIEVALS,
      failedTechnicalAttemptCap: LIVE_RECOVERY_MAX_FAILED_TECHNICAL_ATTEMPTS,
    });
    if (!attemptBudget.canAttemptMore) {
      state.stoppingTrigger = attemptBudget.failedTechnicalAttempts >= LIVE_RECOVERY_MAX_FAILED_TECHNICAL_ATTEMPTS
        ? "FAILED_TECHNICAL_ATTEMPT_CAP_REACHED"
        : STOPPING_TRIGGER.RETRIEVAL_CAP_REACHED;
      state.disposition = RESEARCH_DISPOSITION.BUYER_ENGAGEMENT_REQUIRED;
      state.commercialNextAction = "Buyer engagement remains valid for Principal→Nobu; Savoy independent corroboration incomplete";
      state.status = "STOPPED";
      state.planLog.push({ event: "attempt_budget_stop", attemptBudget });
      writeMaturationCheckpointAtomic(checkpointDir, state);
      break;
    }
    if (state.substantiveQuestions.length >= LIVE_RECOVERY_MAX_SUBSTANTIVE_QUESTIONS) {
      state.stoppingTrigger = "SUBSTANTIVE_QUESTION_CAP_REACHED";
      state.disposition = RESEARCH_DISPOSITION.BUYER_ENGAGEMENT_REQUIRED;
      state.status = "STOPPED";
      break;
    }

    const ledger = loadOrCreateLiveRecoveryLedger(ledgerPath);
    const remainingUsd = Number((LIVE_RECOVERY_BUDGET_USD - Number(ledger.estimatedSpentUsd || 0) - Number(ledger.reservedUsd || 0)).toFixed(6));
    if (!(remainingUsd > 0)) {
      state.disposition = RESEARCH_DISPOSITION.BUDGET_EXHAUSTED;
      state.stoppingTrigger = STOPPING_TRIGGER.BUDGET_EXHAUSTED;
      state.status = "STOPPED";
      state.planLog.push({ event: "budget_exhaustion", remainingUsd });
      writeMaturationCheckpointAtomic(checkpointDir, state);
      break;
    }

    const activeQuestion = state.followUpQuestions.slice(-1)[0] || researchQuestion;

    if (state.plannerModelCalls < LIVE_RECOVERY_MAX_PLANNER_CALLS) {
      const billed = await billedLiveRecoveryPlanner({
        ledgerPath,
        jobKey: `gdi-lr-planner-${state.plannerModelCalls + 1}`,
        planInput: {
          objective: {
            hotelId: "recC2kbCaS48jFjFF",
            hotelName: "Nobu Hotel London Portman Square",
            commercialQuestion: researchQuestion,
          },
          hotel: { name: "Nobu Hotel London Portman Square", market: "London" },
          opportunity: {
            organizationName: ORGANIZER_NAME,
            commercialStage: "NEEDS_RESEARCH",
            pathway: "ACCOUNT_LED",
          },
          commercial: { salesReadiness: "NEEDS_RESEARCH" },
          findings: state.acceptedClaims.map((item) => ({
            url: item.url,
            excerpt: item.statement,
            statement: item.statement,
          })),
          openQuestions: [
            activeQuestion,
            "Do not repeat failed thesavoy.co.uk URLs.",
            "Fairmont is an official alternate about The Savoy, not automatic Principal corroboration.",
            "Independent verification requires a separate publisher that explicitly connects Principal to The Savoy.",
            `Candidate pool: ${state.candidateQueue.slice(0, 4).join(", ") || "(empty)"}`,
          ],
          investigated: { urls: state.investigatedUrls },
          authorizePaidResearch: false,
          remainingUsd: 0,
          questionFingerprints: state.substantiveQuestions.map((item) => item.question),
        },
      });
      if (!billed.skipped) {
        state.plannerModelCalls += billed.plan.plannerModelCalls || 0;
        state.planLog.push({
          event: "planner_call",
          disposition: billed.plan.researchDisposition,
          proposals: (billed.plan.proposals || []).map((item) => ({ question: item.question, url: item.url })),
        });
        for (const proposal of billed.plan.proposals || []) {
          if (!proposal.url) continue;
          if (blockedFailedUrls.map(normalizeUrl).includes(normalizeUrl(proposal.url))) {
            state.planLog.push({ event: "blocked_failed_savoy_url_rejected", url: proposal.url });
            continue;
          }
          if (!isIndependentPublisherUrl(proposal.url, ORGANIZER_HOST)) {
            state.planLog.push({ event: "organizer_url_rejected", url: proposal.url });
            continue;
          }
          if (classifyTechnicalSource(proposal.url).rejected) continue;
          const n = normalizeUrl(proposal.url);
          if (!state.investigatedUrls.map(normalizeUrl).includes(n)
            && !state.candidateQueue.map(normalizeUrl).includes(n)) {
            state.candidateQueue.push(proposal.url);
            state.planLog.push({ event: "independent_lead_queued", url: proposal.url, from: "planner" });
          }
        }
        if (billed.plan.proposals?.[0]?.question) {
          state.followUpQuestions.push(billed.plan.proposals[0].question);
        }
      }
    }

    const selection = selectLiveRecoverySource({
      candidates: state.candidateQueue,
      investigatedUrls: state.investigatedUrls,
      blockedUrls: state.blockedFailedUrls,
      cooledHosts: state.cooledHosts,
      question: activeQuestion,
    });
    state.planLog.push({
      event: "source_selection",
      question: activeQuestion,
      selected: selection.selected?.url || null,
      score: selection.selected?.score || null,
      ownershipRole: selection.selected?.ownership?.ownershipRole || null,
      ranking: selection.ranking.map((item) => ({
        url: item.url,
        score: item.score,
        ownershipRole: item.ownership.ownershipRole,
      })),
      whySelected: selection.selected
        ? `Defensible recovery candidate score ${selection.selected.score}; ownership ${selection.selected.ownership.ownershipRole}`
        : "No eligible recovery candidate remaining",
    });

    if (!selection.selected) {
      state.verificationStatus = RELATIONSHIP_VERIFICATION.INDEPENDENT_CONFIRMATION_UNAVAILABLE;
      state.disposition = RESEARCH_DISPOSITION.BUYER_ENGAGEMENT_REQUIRED;
      state.stoppingTrigger = STOPPING_TRIGGER.NO_UNREAD_COMMERCIAL_SOURCE;
      state.commercialNextAction = "Buyer engagement remains valid; do not claim independently verified Principal–Savoy or Nobu demand";
      state.status = "STOPPED";
      state.planLog.push({ event: "no_eligible_recovery_candidate" });
      writeMaturationCheckpointAtomic(checkpointDir, state);
      break;
    }

    const url = selection.selected.url;
    if (blockedFailedUrls.map(normalizeUrl).includes(normalizeUrl(url))) {
      state.candidateQueue = state.candidateQueue.filter((item) => normalizeUrl(item) !== normalizeUrl(url));
      state.planLog.push({ event: "blocked_failed_savoy_url_suppressed", url });
      continue;
    }

    const attemptNumber = state.sourceAttempts.length + 1;
    const fetched = await fetchLiveRecoveryPage({
      url,
      fetchPage: input.fetchPage,
      ledgerPath,
      jobKey: `gdi-lr-attempt-${attemptNumber}`,
    });
    if (fetched.refused) {
      state.candidateQueue = state.candidateQueue.filter((item) => normalizeUrl(item) !== normalizeUrl(url));
      state.planLog.push({ event: "organizer_publisher_blocked", url });
      continue;
    }

    const attempt = recordSourceAttempt({
      url,
      organizationHost: ORGANIZER_HOST,
      organizationName: ORGANIZER_NAME,
      provider: fetched.provider,
      method: fetched.method || fetched.provider,
      researchQuestion: activeQuestion,
      ok: fetched.ok === true,
      text: fetched.text || "",
      html: fetched.html || "",
      providerError: fetched.providerError || null,
      httpStatus: fetched.httpStatus || null,
      actualUsd: fetched.actualUsd || 0,
      attemptIndex: attemptNumber,
      relevantContentPredicate: pageCorroboratesPrincipalSavoyRelationship,
    });
    const ownership = classifySourceOwnershipRole({
      url,
      relationshipExplicitlyCorroborated: false,
    });
    attempt.ownershipRole = ownership.ownershipRole;
    attempt.publishingOrganization = ownership.publishingOrganization;
    state.sourceAttempts.push(attempt);
    state.planLog.push({
      event: attempt.isFailedTechnicalAttempt ? "retrieval_failure_recorded" : "retrieval_success_recorded",
      url: attempt.url,
      retrievalOutcome: attempt.retrievalOutcome,
      ownershipRole: ownership.ownershipRole,
      publishingOrganization: ownership.publishingOrganization,
      evidenceEvaluationSkipped: attempt.evidenceEvaluationSkipped,
    });

    if (!attempt.mayEvaluateEvidence) {
      state.investigatedUrls.push(url);
      state.candidateQueue = state.candidateQueue.filter((item) => normalizeUrl(item) !== normalizeUrl(url));
      const recovery = proposeAdaptiveRecovery({
        failedAttempt: attempt,
        attempts: state.sourceAttempts,
        candidateQueue: state.candidateQueue,
        researchQuestion: activeQuestion,
        organizationHost: ORGANIZER_HOST,
      });
      state.cooledHosts = [...new Set([...(state.cooledHosts || []), ...(recovery.cooledHosts || [])])];
      for (const next of recovery.nextCandidates || []) {
        if (blockedFailedUrls.map(normalizeUrl).includes(normalizeUrl(next.url))) continue;
        const n = normalizeUrl(next.url);
        if (state.investigatedUrls.map(normalizeUrl).includes(n)) continue;
        if (state.candidateQueue.map(normalizeUrl).includes(n)) continue;
        if (!isIndependentPublisherUrl(next.url, ORGANIZER_HOST)) continue;
        state.candidateQueue.unshift(next.url);
        state.relatedEntityFollowUps.push({
          entityName: next.url,
          suggestedUrl: next.url,
          recoveryType: next.recoveryType,
        });
        state.planLog.push({ event: "independent_lead_queued", url: next.url, from: "adaptive_recovery" });
      }
      state.planLog.push({
        event: "adaptive_recovery_proposed",
        proposedNext: recovery.proposedNextIndependentSource,
        cooledHosts: recovery.cooledHosts,
      });
      state.followUpQuestions.push(activeQuestion);
      writeMaturationCheckpointAtomic(checkpointDir, state);
      continue;
    }

    const evaluated = evaluateLiveRecoveryEvidence({
      url,
      text: fetched.text,
      html: fetched.html,
      ownership,
    });
    // Reclassify ownership if third-party corroboration succeeded.
    const finalOwnership = classifySourceOwnershipRole({
      url,
      relationshipExplicitlyCorroborated: evaluated.independentlyVerified === true,
    });

    state.retrievals.push({
      url,
      ok: true,
      provider: fetched.provider,
      actualUsd: fetched.actualUsd || 0,
      publishingOrganization: finalOwnership.publishingOrganization,
      sourceOwnerHost: finalOwnership.sourceOwnerHost,
      ownershipRole: finalOwnership.ownershipRole,
      pageSubject: pageMentionsSavoy(fetched.text) ? RELATED_ENTITY : "unspecified",
      independentOfOrganizer: finalOwnership.independentOfOrganizer,
      question: activeQuestion,
      retrievalOutcome: attempt.retrievalOutcome,
      corroboratesRelationship: evaluated.corroborates,
      independentlyVerifiedFromThisSource: evaluated.independentlyVerified,
      whySelected: `Recovery candidate; ownership ${finalOwnership.ownershipRole}; publisher ${finalOwnership.publishingOrganization}`,
    });
    state.investigatedUrls.push(url);
    state.candidateQueue = state.candidateQueue.filter((item) => normalizeUrl(item) !== normalizeUrl(url));
    state.acceptedClaims.push(...evaluated.accepted);
    state.rejectedClaims.push(...evaluated.rejected);
    state.substantiveQuestions.push({
      question: activeQuestion,
      url,
      substantive: true,
      ownershipRole: finalOwnership.ownershipRole,
      corroborates: evaluated.corroborates,
      retrievalOutcome: attempt.retrievalOutcome,
    });

    for (const link of evaluated.discoveredIndependentLinks || []) {
      if (blockedFailedUrls.map(normalizeUrl).includes(normalizeUrl(link))) continue;
      const n = normalizeUrl(link);
      if (state.investigatedUrls.map(normalizeUrl).includes(n)) continue;
      if (state.candidateQueue.map(normalizeUrl).includes(n)) continue;
      if (!/savoy|fairmont|principal|incentive|meeting|event|press|hospitality/i.test(link)) continue;
      state.candidateQueue.push(link);
      state.relatedEntityFollowUps.push({ entityName: link, suggestedUrl: link });
      state.planLog.push({ event: "independent_lead_queued", url: link, from: "page_links" });
    }

    if (evaluated.independentlyVerified) {
      state.verificationStatus = RELATIONSHIP_VERIFICATION.INDEPENDENTLY_VERIFIED;
      state.independentlyVerified = true;
      state.confirmingUrl = url;
      state.confirmingPublisher = finalOwnership.publishingOrganization;
      state.disposition = RESEARCH_DISPOSITION.RESEARCH_SUFFICIENT;
      state.stoppingTrigger = STOPPING_TRIGGER.RESEARCH_SUFFICIENT;
      state.commercialNextAction = "Independent third-party corroboration of Principal–Savoy found; still no Nobu RFP/demand assertion";
      state.status = "STOPPED";
      state.planLog.push({
        event: "independent_verification_accepted",
        url,
        publishingOrganization: finalOwnership.publishingOrganization,
        ownershipRole: finalOwnership.ownershipRole,
      });
      writeMaturationCheckpointAtomic(checkpointDir, state);
      break;
    }

    // Fairmont/Savoy brand success without Principal = useful read, not verification.
    if (finalOwnership.ownershipRole === SOURCE_OWNERSHIP_ROLE.OFFICIAL_PROPERTY_OR_BRAND_ALTERNATE) {
      state.planLog.push({
        event: "brand_alternate_read_not_independent_verification",
        url,
        note: "Official Fairmont/Savoy alternate retrieved; Principal involvement remains unverified.",
      });
      state.followUpQuestions.push(
        "Is there an independently published third-party source that explicitly names Principal with The Savoy or the identified program?",
      );
    } else {
      state.followUpQuestions.push(
        `After reading ${finalOwnership.publishingOrganization}, does another independent publisher explicitly connect Principal to The Savoy?`,
      );
    }

    // VoI: after one successful brand alternate + optional third-party miss, buyer engagement may win.
    const successfulReads = state.sourceAttempts.filter((item) => item.isSuccessfulRead).length;
    if (successfulReads >= 2 && !state.independentlyVerified) {
      state.verificationStatus = RELATIONSHIP_VERIFICATION.INDEPENDENT_CONFIRMATION_UNAVAILABLE;
      state.disposition = RESEARCH_DISPOSITION.BUYER_ENGAGEMENT_REQUIRED;
      state.stoppingTrigger = STOPPING_TRIGGER.BUYER_ENGAGEMENT_MORE_USEFUL;
      state.commercialNextAction = "Buyer engagement remains valid; public independent corroboration of Principal–Savoy not established";
      state.status = "STOPPED";
      state.planLog.push({ event: "voi_stop_buyer_engagement", successfulReads });
      writeMaturationCheckpointAtomic(checkpointDir, state);
      break;
    }

    writeMaturationCheckpointAtomic(checkpointDir, state);
  }

  if (state.status === "IN_PROGRESS") {
    state.verificationStatus = state.independentlyVerified
      ? RELATIONSHIP_VERIFICATION.INDEPENDENTLY_VERIFIED
      : RELATIONSHIP_VERIFICATION.INDEPENDENT_CONFIRMATION_UNAVAILABLE;
    state.disposition = RESEARCH_DISPOSITION.BUYER_ENGAGEMENT_REQUIRED;
    state.stoppingTrigger = state.stoppingTrigger || "RETRIEVAL_RECOVERY_REQUIRED";
    state.commercialNextAction = state.commercialNextAction
      || "Buyer engagement remains valid; Savoy independent corroboration incomplete";
    state.status = "STOPPED";
    state.planLog.push({ event: "loop_complete_stop", stoppingTrigger: state.stoppingTrigger });
    writeMaturationCheckpointAtomic(checkpointDir, state);
  }

  const ledger = loadOrCreateLiveRecoveryLedger(ledgerPath);
  const remainingAuthorizationUsd = Number((LIVE_RECOVERY_BUDGET_USD - Number(ledger.estimatedSpentUsd || 0)).toFixed(6));
  const investigationReport = buildCrossOrgInvestigationReport({
    attempts: state.sourceAttempts,
    independentlyVerified: state.independentlyVerified === true,
    verificationStatus: state.verificationStatus,
    commercialNextAction: state.commercialNextAction,
    remainingAuthorizedBudgetUsd: remainingAuthorizationUsd,
    substantiveRetrievalCap: LIVE_RECOVERY_MAX_SUBSTANTIVE_RETRIEVALS,
    failedTechnicalAttemptCap: LIVE_RECOVERY_MAX_FAILED_TECHNICAL_ATTEMPTS,
    buyerEngagementStillValid: true,
  });

  const resumed = JSON.parse(fs.readFileSync(maturationMemoryPath(checkpointDir, id), "utf8"));
  const checkpointResumeOk = resumed.status === "STOPPED"
    && Array.isArray(resumed.blockedFailedUrls)
    && resumed.blockedFailedUrls.length === blockedFailedUrls.length
    && !(state.sourceAttempts || []).some((item) => blockedFailedUrls.map(normalizeUrl).includes(normalizeUrl(item.url)));

  const repeatedFailedSavoy = (state.sourceAttempts || []).some((item) => (
    blockedFailedUrls.map(normalizeUrl).includes(normalizeUrl(item.url))
  ));

  const substantiveSuccessful = state.sourceAttempts.filter((item) => item.countsTowardSubstantiveRetrievalCap).length;
  const failedTechnical = state.sourceAttempts.filter((item) => item.countsTowardFailedTechnicalAttemptCap).length;

  return {
    executed: true,
    certificationPassed: Number(ledger.estimatedSpentUsd || 0) - LIVE_RECOVERY_BUDGET_USD <= 1e-9
      && substantiveSuccessful <= LIVE_RECOVERY_MAX_SUBSTANTIVE_RETRIEVALS
      && failedTechnical <= LIVE_RECOVERY_MAX_FAILED_TECHNICAL_ATTEMPTS
      && state.plannerModelCalls <= LIVE_RECOVERY_MAX_PLANNER_CALLS
      && repeatedFailedSavoy === false
      && state.publicationEligibilityChanged !== true
      && state.salesReadiness === "NEEDS_RESEARCH"
      && state.commercialStage === "NEEDS_RESEARCH",
    version: LIVE_RECOVERY_CERT_VERSION,
    researchObjective: researchQuestion,
    sourceAttempts: state.sourceAttempts,
    sourcesVisited: state.retrievals,
    urlsAttempted: state.sourceAttempts.map((item) => ({
      url: item.url,
      retrievalOutcome: item.retrievalOutcome,
      publishingOrganization: item.publishingOrganization,
      ownershipRole: item.ownershipRole,
      provider: item.provider,
      providerError: item.providerError,
    })),
    substantiveQuestions: state.substantiveQuestions,
    acceptedClaims: state.acceptedClaims,
    rejectedClaims: state.rejectedClaims,
    unverifiedRelationship: state.independentlyVerified !== true,
    verificationStatus: state.verificationStatus,
    independentlyVerified: state.independentlyVerified === true,
    confirmingUrl: state.confirmingUrl || null,
    confirmingPublisher: state.confirmingPublisher || null,
    relatedEntityFollowUps: state.relatedEntityFollowUps,
    newResearchLeadsDiscovered: state.planLog.filter((item) => item.event === "independent_lead_queued").length,
    plannerModelCalls: state.plannerModelCalls,
    retrievalCount: substantiveSuccessful,
    failedTechnicalAttemptCount: failedTechnical,
    substantiveQuestionCount: state.substantiveQuestions.length,
    actualSpentUsd: ledger.estimatedSpentUsd,
    remainingAuthorizationUsd,
    authorizedUsd: LIVE_RECOVERY_BUDGET_USD,
    researchDisposition: state.disposition,
    stoppingTrigger: state.stoppingTrigger,
    commercialNextAction: state.commercialNextAction,
    researchCompleteness: investigationReport.researchCompleteness,
    investigationReport,
    stopRecord: investigationReport.stopRecord,
    repeatedFailedSavoyUrls: repeatedFailedSavoy,
    blockedFailedUrls,
    nobuCommercialImplications: {
      openSavoyDemandAsserted: false,
      nobuPrincipalRelationshipAsserted: false,
      principalAccountDevelopmentStillValid: true,
      savoyRelationshipIndependentlyVerified: state.independentlyVerified === true,
      fairmontReadIsNotAutomaticIndependentCorroboration: true,
      note: state.independentlyVerified
        ? "Independent third-party corroboration of Principal–Savoy exists. This does not create Nobu demand or an open RFP."
        : "Principal–Savoy remains unverified as an independent commercial relationship. Fairmont/Savoy official pages (if read) are brand alternates only. Principal remains an account-development prospect for Nobu; buyer engagement remains valid.",
    },
    checkpointStatus: state.status,
    checkpointResumeOk,
    planLog: state.planLog,
    publicationEligibilityChanged: false,
    salesReadiness: "NEEDS_RESEARCH",
    commercialStage: "NEEDS_RESEARCH",
  };
}
