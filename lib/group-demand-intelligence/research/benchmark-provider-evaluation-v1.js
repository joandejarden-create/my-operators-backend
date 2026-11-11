/**
 * One Cohort A benchmark attempt.
 * Reserves before the provider call, reconciles after, and stores evidence
 * outside Airtable and outside the opportunity file.
 * Publication and suitability functions are read. They are not modified.
 */

import { canonicalEntityKey } from "../workflow/gdi-global-weekly-feedback-v2.js";
import {
  classifyContactCompletenessGroup,
  evaluateProspectSuitability,
  hasUsableContactRoute,
} from "../prospect-publication-eligibility-v1.js";
import { cohortAHotel, isCohortBHotel } from "./benchmark-cohort-a-registry-v1.js";
import {
  assertBenchmarkEvaluationEnabled,
  credentialPresence,
  readBenchmarkFlags,
  readProviderCredential,
} from "./benchmark-provider-flags-v1.js";
import { quoteBenchmarkCall } from "./benchmark-provider-pricing-v1.js";
import {
  benchmarkSpendTotals,
  readBenchmarkBudgetLedger,
  reconcileBenchmarkReservation,
  releaseBenchmarkReservation,
  reserveBenchmarkBudget,
} from "./benchmark-cohort-a-budget-v1.js";
import {
  benchmarkRequestFingerprint,
  findBenchmarkAttempt,
  isTerminalBenchmarkAttempt,
  listOpenBenchmarkAttempts,
  normalizeEvidenceText,
  readBenchmarkEvidenceLedger,
  saveBenchmarkAttempt,
  sha256Text,
} from "./benchmark-evidence-ledger-v1.js";
import { executeExaBenchmarkCall } from "./exa-benchmark-adapter-v1.js";
import { executeFirecrawlBenchmarkCall } from "./firecrawl-benchmark-adapter-v1.js";
import { extractEvidenceContacts } from "../evidence-contact-extraction-v1.js";
import { planBenchmarkAcquisition } from "../workflow/gdi-evidence-acquisition-v2.js";

export const BENCHMARK_ARM = Object.freeze({
  EXA: "3_exa",
  FIRECRAWL: "4_firecrawl",
  COMPLEMENTARY: "5_complementary",
});

export function selectComplementaryProvider({
  priorExaTextLength = null,
  sourceKind = null,
} = {}) {
  const needsParse = sourceKind === "pdf" || sourceKind === "javascript";
  if (priorExaTextLength == null) {
    return { provider: "exa", reason: "EXA_AFTER_EXISTING_ARCHIVE" };
  }
  if (needsParse || priorExaTextLength < 200) {
    return {
      provider: "firecrawl",
      reason: needsParse ? "SOURCE_NEEDS_PARSE" : "EXA_TEXT_THIN",
    };
  }
  return { provider: "exa", reason: "EXA_TEXT_SUFFICIENT" };
}

function redact(value, secrets) {
  let out = String(value ?? "");
  for (const secret of secrets) {
    if (secret && secret.length > 6) out = out.split(secret).join("[redacted]");
  }
  return out;
}

function safeMessage(err, secrets) {
  return redact(err?.message || err?.code || "error", secrets).slice(0, 300);
}

export function assessBenchmarkQualification({
  blockerKey,
  priorOpportunity = {},
  extracted = {},
  priorEvidenceHash = null,
  excludedEntityKeys = [],
} = {}) {
  const quote = normalizeEvidenceText(extracted.evidenceQuote || "");
  const contentHash = sha256Text(quote);
  const unchanged =
    priorEvidenceHash != null && priorEvidenceHash === contentHash;
  const contactEvidence = extractEvidenceContacts({
    quote,
    sourceUrl: extracted.sourceUrl || priorOpportunity.sourceUrl || "",
    countryHint:
      extracted.countryHint ||
      priorOpportunity.countryHint ||
      priorOpportunity.country ||
      "",
  });
  const email = contactEvidence.email?.normalized || "";
  const phone = contactEvidence.phone?.normalized || "";
  const merged = {
    ...priorOpportunity,
    sourceUrl: extracted.sourceUrl || priorOpportunity.sourceUrl || "",
    sources: extracted.sourceUrl
      ? [...(priorOpportunity.sources || []), { url: extracted.sourceUrl }]
      : priorOpportunity.sources || [],
  };
  if (email) merged.primaryContactEmail = email;
  if (phone) merged.contactPhone = phone;

  const priorRoute = hasUsableContactRoute(priorOpportunity);
  const nextRoute = hasUsableContactRoute(merged);
  const priorGroup = classifyContactCompletenessGroup(priorOpportunity);
  const nextGroup = classifyContactCompletenessGroup(merged);
  const priorSuitability = evaluateProspectSuitability(priorOpportunity);
  const nextSuitability = evaluateProspectSuitability(merged);
  const entityKey = canonicalEntityKey(
    priorOpportunity.organizationName || priorOpportunity.name || ""
  );
  const rediscovery = excludedEntityKeys.includes(entityKey);
  const routeImproved = !priorRoute.ok && nextRoute.ok && Boolean(email || phone);
  const classificationChanged = priorGroup !== nextGroup && routeImproved;
  const suitabilityChanged =
    JSON.stringify(priorSuitability?.failed || priorSuitability) !==
    JSON.stringify(nextSuitability?.failed || nextSuitability);
  const blockerClosed =
    !unchanged &&
    blockerKey === "contact_route" &&
    routeImproved;

  return {
    contentHash,
    materialChange: !unchanged && quote.length > 0,
    unchanged,
    blockerClosed,
    classificationChanged: !unchanged && classificationChanged,
    suitabilityChanged: !unchanged && suitabilityChanged && routeImproved,
    routeReason: nextRoute.reason,
    priorGroup,
    nextGroup,
    rediscovery,
    newOpportunity: false,
    contactEvidence,
    reason: unchanged
      ? "UNCHANGED_EVIDENCE"
      : blockerClosed
        ? "BLOCKER_CLOSED"
        : quote
          ? "EVIDENCE_RETRIEVED_BLOCKER_OPEN"
          : "NO_EVIDENCE_QUOTE",
  };
}

function armFor(provider, requested) {
  if (
    requested === BENCHMARK_ARM.EXA ||
    requested === BENCHMARK_ARM.FIRECRAWL ||
    requested === BENCHMARK_ARM.COMPLEMENTARY
  ) {
    return requested;
  }
  return provider === "firecrawl" ? BENCHMARK_ARM.FIRECRAWL : BENCHMARK_ARM.EXA;
}

function resultFromAttempt(attempt, extra = {}) {
  const status = extra.status || attempt.status;
  const stopReason = extra.stopReason || attempt.stopReason;
  return {
    status,
    stopReason,
    attemptId: attempt.attemptId,
    requestFingerprint: attempt.requestFingerprint,
    estimatedUsd: attempt.estimatedUsd ?? 0,
    reservedUsd: attempt.reservedUsd ?? 0,
    actualUsd: attempt.actualUsd ?? 0,
    qualification: attempt.qualification || null,
    evidence: attempt.sourceUrl
      ? [{ url: attempt.sourceUrl, quote: attempt.evidenceQuote }]
      : [],
    sourceFamilies: [attempt.provider ? `benchmark_${attempt.provider}` : "benchmark"],
    notes: attempt.error || stopReason,
    providerCalled: false,
    ...extra,
    status,
    stopReason,
  };
}

export async function runBenchmarkProviderAttempt(input = {}) {
  const options = input.options || {};
  const env = input.env || options.env || process.env;
  const provider = input.provider || options.benchmarkProvider;
  const opportunity = input.opportunity || {};
  const blocker = input.blocker || {};
  const action = input.action || {};
  const hotelId = input.hotelId || options.hotelId || opportunity.hotelId;
  const ledgerPath = input.ledgerPath || options.benchmarkBudgetLedgerPath;
  const evidencePath = input.evidencePath || options.benchmarkEvidencePath;
  let operation = input.operation || options.benchmarkOperation || null;
  const explicitOperation = Boolean(operation);
  let params = { ...(input.operationParams || options.benchmarkOperationParams || {}) };
  let arm = armFor(provider, input.arm || options.benchmarkArm);
  const secrets = [
    readProviderCredential("exa", env),
    readProviderCredential("firecrawl", env),
  ].filter(Boolean);
  const language = input.language || options.language || null;
  const hotel = cohortAHotel(hotelId);

  const base = {
    hotelId: hotelId || null,
    canonicalIdentity: hotel
      ? {
          censusRecordId: hotel.censusRecordId,
          displayName: hotel.displayName,
          adpPropertyId: hotel.adpPropertyId,
          identityKey: hotel.identityKey,
          canonicalEntityKey: canonicalEntityKey(
            opportunity.organizationName || opportunity.name || ""
          ),
        }
      : null,
    provider,
    arm,
    strategy: action.strategy || null,
    blockerKey: blocker.key || null,
    language,
    estimatedUsd: 0,
    reservedUsd: 0,
    actualUsd: 0,
    retryCount: 0,
    qualification: null,
  };

  const refuse = (stopReason, error = null) => {
    const attempt = saveBenchmarkAttempt(
      {
        ...base,
        status: "refused",
        stopReason,
        error: error ? safeMessage(error, secrets) : null,
        requestFingerprint: null,
        providerCalled: false,
      },
      { ledgerPath: evidencePath, env }
    );
    return resultFromAttempt(attempt, { providerCalled: false });
  };

  try {
    assertBenchmarkEvaluationEnabled(env);
  } catch (err) {
    return refuse("BENCHMARK_EVAL_DISABLED", err);
  }
  if (isCohortBHotel(hotelId)) return refuse("COHORT_B_NOT_AUTHORIZED");
  if (!hotel) return refuse("HOTEL_NOT_IN_COHORT_A");
  if (provider !== "exa" && provider !== "firecrawl") {
    return refuse("BENCHMARK_PROVIDER_UNSUPPORTED");
  }
  if (!explicitOperation) {
    const plan = planBenchmarkAcquisition(opportunity, {
      benchmarkEvaluation: true,
      env,
      hotelId,
      query: params.query,
      localDocumentPath: params.filePath || options.localDocumentPath,
      maxPages: params.maxPages ?? options.maxPages,
      sourceKind: options.sourceKind,
      priorExaTextLength: options.priorExaTextLength,
      benchmarkProvider: provider,
      gap: options.gap,
    });
    if (!plan.benchmarkExecutable) {
      return refuse(plan.benchmarkRefusal || "BENCHMARK_PLAN_NOT_EXECUTABLE");
    }
    if (plan.benchmarkProvider !== provider) return refuse("BENCHMARK_PROVIDER_MISMATCH");
    operation = plan.benchmarkOperation;
    params = { ...plan.benchmarkParams, ...params };
    if (!input.arm && !options.benchmarkArm && plan.benchmarkArm) arm = plan.benchmarkArm;
    base.arm = arm;
  }

  const quote = quoteBenchmarkCall({ provider, operation, params });
  base.estimatedUsd = quote.bounded ? quote.maxUsd : null;
  if (!quote.bounded) return refuse(quote.reason || "UNPRICED_OPERATION");
  if (!credentialPresence(provider, env).configured) return refuse("CREDENTIAL_ABSENT");

  const flags = readBenchmarkFlags(env);
  const fetchImpl = input.fetchImpl || options.fetchImpl || null;
  if (!fetchImpl && !flags.liveEnabled) return refuse("LIVE_NETWORK_DISABLED");

  const requestFingerprint = benchmarkRequestFingerprint({
    hotelId: hotel.censusRecordId,
    provider,
    arm,
    strategy: base.strategy,
    blockerKey: base.blockerKey,
    operation,
    params,
  });
  base.requestFingerprint = requestFingerprint;

  const existingLedger = readBenchmarkEvidenceLedger({ ledgerPath: evidencePath });
  const prior = findBenchmarkAttempt(existingLedger, requestFingerprint);
  if (prior && (prior.status === "in_flight" || prior.status === "reserved")) {
    return resultFromAttempt(prior, {
      providerCalled: false,
      stopReason: "ATTEMPT_ALREADY_RUNNING",
      status: prior.status,
    });
  }
  if (prior && isTerminalBenchmarkAttempt(prior) && prior.status !== "refused") {
    if (!(prior.status === "failed" && options.allowRetry === true)) {
      return resultFromAttempt(
        { ...prior, stopReason: "DUPLICATE_ATTEMPT" },
        { providerCalled: false, idempotent: true }
      );
    }
    base.retryCount = Number(prior.retryCount || 0) + 1;
    base.priorAttemptId = prior.attemptId;
    if (base.retryCount > 2) return refuse("RETRY_LIMIT");
  }

  const budgetFingerprint =
    base.retryCount > 0 ? `${requestFingerprint}:${base.retryCount}` : requestFingerprint;
  let reservation;
  try {
    reservation = reserveBenchmarkBudget({
      hotelId: hotel.censusRecordId,
      provider,
      arm,
      operation,
      requestFingerprint: budgetFingerprint,
      maxUsd: quote.maxUsd,
      ledgerPath,
      env,
    });
  } catch (err) {
    return refuse(err.code || "BUDGET_REFUSED", err);
  }
  if (reservation.idempotent && reservation.status !== "reserved") {
    return resultFromAttempt(
      {
        ...base,
        status: "duplicate",
        stopReason: "DUPLICATE_ATTEMPT",
        reservedUsd: reservation.reservedUsd,
        attemptId: reservation.reservationId,
      },
      { providerCalled: false, idempotent: true }
    );
  }
  if (reservation.idempotent && reservation.status === "reserved") {
    return resultFromAttempt(
      {
        ...base,
        status: "reserved",
        stopReason: "ATTEMPT_ALREADY_RUNNING",
        reservationId: reservation.reservationId,
        reservedUsd: reservation.reservedUsd,
        attemptId: reservation.reservationId,
      },
      { providerCalled: false }
    );
  }

  const open = saveBenchmarkAttempt(
    {
      ...base,
      status: "in_flight",
      stopReason: null,
      reservationId: reservation.reservationId,
      reservedUsd: reservation.reservedUsd,
      estimatedUsd: quote.maxUsd,
    },
    { ledgerPath: evidencePath, env }
  );

  const call =
    provider === "exa" ? executeExaBenchmarkCall : executeFirecrawlBenchmarkCall;
  try {
    const response = await call({
      operation,
      params,
      reservation,
      ledgerPath,
      env,
      fetchImpl: fetchImpl || globalThis.fetch,
      timeoutMs: options.timeoutMs || input.timeoutMs,
    });
    const quoteText = redact(response.extracted?.evidenceQuote || "", secrets);
    const candidates = (response.extracted?.candidates || []).map((row) => ({
      url: row.url || null,
      title: row.title || null,
      excerpt: redact(row.excerpt || "", secrets).slice(0, 400),
    }));
    const qualification = assessBenchmarkQualification({
      blockerKey: base.blockerKey,
      priorOpportunity: opportunity,
      extracted: { ...response.extracted, evidenceQuote: quoteText },
      priorEvidenceHash: options.priorEvidenceHash || null,
      excludedEntityKeys: options.excludedEntityKeys || [],
    });
    const extractionOk = Boolean(response.extracted?.extractionOk);
    const actualUsd = Number(response.actualUsd);
    reconcileBenchmarkReservation(reservation.reservationId, {
      ledgerPath,
      env,
      actualUsd: Number.isFinite(actualUsd) ? actualUsd : reservation.reservedUsd,
    });
    const attempt = saveBenchmarkAttempt(
      {
        ...open,
        status: extractionOk ? "succeeded" : "failed",
        stopReason: extractionOk
          ? qualification.reason
          : "EXTRACTION_FAILED",
        sourceUrl: response.extracted?.sourceUrl || null,
        citation: response.extracted?.citation || response.extracted?.sourceUrl || null,
        language: response.extracted?.language || language,
        retrievedAt: new Date().toISOString(),
        evidenceQuote: quoteText.slice(0, 1200),
        sourceHash: sha256Text(response.extracted?.sourceUrl || ""),
        contentHash: qualification.contentHash,
        discoveredSources: candidates.map((row) => ({ url: row.url, title: row.title })),
        estimatedUsd: quote.maxUsd,
        reservedUsd: reservation.reservedUsd,
        actualUsd: Number.isFinite(actualUsd) ? actualUsd : reservation.reservedUsd,
        qualification,
        error: extractionOk ? null : "extraction_failed",
      },
      { ledgerPath: evidencePath, env }
    );
    return resultFromAttempt(attempt, { providerCalled: true, candidates });
  } catch (err) {
    const dispatched = Boolean(err?.dispatched);
    const actualUsd = dispatched
      ? Number(err.actualUsd ?? reservation.reservedUsd)
      : 0;
    if (dispatched) {
      reconcileBenchmarkReservation(reservation.reservationId, {
        ledgerPath,
        env,
        actualUsd,
      });
    } else {
      releaseBenchmarkReservation(reservation.reservationId, {
        ledgerPath,
        env,
        reason: err.code || "PROVIDER_NOT_DISPATCHED",
      });
    }
    const attempt = saveBenchmarkAttempt(
      {
        ...open,
        status: "failed",
        stopReason: err.code || "PROVIDER_REQUEST_FAILED",
        error: safeMessage(err, secrets),
        estimatedUsd: quote.maxUsd,
        reservedUsd: reservation.reservedUsd,
        actualUsd,
        qualification: {
          materialChange: false,
          blockerClosed: false,
          newOpportunity: false,
          reason: err.code || "PROVIDER_REQUEST_FAILED",
        },
      },
      { ledgerPath: evidencePath, env }
    );
    return resultFromAttempt(attempt, { providerCalled: dispatched });
  }
}

export async function recoverInterruptedBenchmarkAttempts({
  ledgerPath,
  evidencePath,
  env = process.env,
} = {}) {
  const open = listOpenBenchmarkAttempts({ ledgerPath: evidencePath, env });
  const budget = readBenchmarkBudgetLedger({ ledgerPath, createIfMissing: false });
  const reserved = (budget.entries || []).filter((entry) => entry.status === "reserved");
  const released = [];
  for (const entry of reserved) {
    const attempt = open.find(
      (row) =>
        row.reservationId === entry.reservationId ||
        row.requestFingerprint === entry.requestFingerprint
    );
    if (attempt && attempt.status === "in_flight") {
      releaseBenchmarkReservation(entry.reservationId, {
        ledgerPath,
        env,
        reason: "INTERRUPTED_EXECUTION",
      });
      saveBenchmarkAttempt(
        {
          ...attempt,
          status: "interrupted_released",
          stopReason: "INTERRUPTED_EXECUTION",
        },
        { ledgerPath: evidencePath, env }
      );
      released.push(entry.reservationId);
    } else if (!attempt) {
      releaseBenchmarkReservation(entry.reservationId, {
        ledgerPath,
        env,
        reason: "INTERRUPTED_EXECUTION",
      });
      released.push(entry.reservationId);
    }
  }
  return { released, open: open.length };
}

export function createExaBenchmarkExecutor() {
  return {
    provider: "exa",
    execute(args) {
      return runBenchmarkProviderAttempt({ ...args, provider: "exa" });
    },
  };
}

export function createFirecrawlBenchmarkExecutor() {
  return {
    provider: "firecrawl",
    execute(args) {
      return runBenchmarkProviderAttempt({ ...args, provider: "firecrawl" });
    },
  };
}

export function readBenchmarkBudgetSummary(opts = {}) {
  const ledger = readBenchmarkBudgetLedger({
    ledgerPath: opts.ledgerPath,
    createIfMissing: false,
  });
  return benchmarkSpendTotals(ledger);
}
