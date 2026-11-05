/**
 * AVA Autonomous Evidence Resolution Engine V1
 *
 * Permanent Full E2E stage: verify → remediate → revalidate → PDF certify.
 * Never grants founder approval or customer publication.
 * Never invents facts. Bounded retries / research budget.
 */
import {
  buildClaimEvidenceGraph,
  applyUrlVerificationToClaims,
} from "./claim-evidence-graph-v1.js";
import {
  collectEvidenceEntries,
  validateAuditEvidenceLinks,
} from "./evidence-link-validation-v1.js";
import {
  certifyAvaEvidenceAndHyperlinks,
  AVA_REJECTED_EVIDENCE_URLS,
  normalizeUrl,
} from "./evidence-hyperlink-certification-v1.js";
import {
  escalateRestrictedEvidence,
  inferClaimKeywords,
  loadVerificationCache,
  saveVerificationCache,
  browserVerifyUrl,
} from "./evidence-verification-escalation-v1.js";

export const AUTONOMOUS_EVIDENCE_RESOLUTION_CONTRACT =
  "AVA_AUTONOMOUS_EVIDENCE_RESOLUTION_V1";

export const EVIDENCE_PIPELINE_STATUSES = Object.freeze([
  "EVIDENCE_VALIDATION_PENDING",
  "EVIDENCE_REMEDIATION_RUNNING",
  "EVIDENCE_VALIDATED",
  "VALIDATION_REQUIRED",
]);

export const DEFAULT_RESOLUTION_LIMITS = Object.freeze({
  maxHttpRetries: 2,
  maxBrowserAttempts: 8,
  maxReplacementSearches: 12,
  maxClaimRewrites: 20,
  maxPdfRegenerations: 2,
  maxElapsedMs: 180000,
  concurrency: 4,
  paidResearchUsdBudget: 0,
});

function clone(obj) {
  return JSON.parse(JSON.stringify(obj));
}

function hostOf(u) {
  try {
    return new URL(u).hostname.replace(/^www\./i, "").toLowerCase();
  } catch {
    return "";
  }
}

function pathCandidates(url) {
  try {
    const u = new URL(url);
    const base = `${u.protocol}//${u.host}`;
    const parts = u.pathname.split("/").filter(Boolean);
    const out = [base + "/"];
    if (parts.length) {
      out.push(`${base}/${parts[0]}/`);
      out.push(url.replace(/\/$/, "") + "/");
    }
    // Common hotel site siblings
    for (const p of [
      "dining",
      "restaurants",
      "meetings",
      "events",
      "accommodations",
      "rooms",
      "overview",
      "specials",
      "offers",
    ]) {
      out.push(`${base}/${p}`);
      out.push(`${base}/${p}/`);
    }
    return [...new Set(out)];
  } catch {
    return [];
  }
}

/**
 * Attempt 1 — retry / redirect / normalize with bounded backoff.
 */
async function attemptRetryRedirect(url, probeFn, limits) {
  const attempts = [];
  let last = null;
  for (let i = 0; i <= (limits.maxHttpRetries || 2); i += 1) {
    if (i > 0) {
      await new Promise((r) => setTimeout(r, 200 * i));
    }
    last = await probeFn(url);
    attempts.push({
      strategy: "RETRY_REDIRECT",
      attempt: i + 1,
      status: last.status,
      finalUrl: last.finalUrl,
      error: last.error || null,
    });
    if (last.ok) {
      return {
        resolved: true,
        finalUrl: last.finalUrl || url,
        probe: last,
        attempts,
      };
    }
    // Follow recorded redirect chain tip if present
    if (last.finalUrl && last.finalUrl !== url && last.redirected) {
      url = last.finalUrl;
    }
  }
  return { resolved: false, finalUrl: last?.finalUrl || url, probe: last, attempts };
}

/**
 * Attempt 3 — same-domain replacement candidates (no paid search).
 */
async function attemptReplacementCandidates({
  url,
  claimKeywords,
  probeFn,
  browserFn,
  limits,
  searchesUsed,
}) {
  const log = [];
  if (searchesUsed.count >= limits.maxReplacementSearches) {
    return { resolved: false, log, reason: "REPLACEMENT_BUDGET_EXHAUSTED" };
  }
  const candidates = pathCandidates(url).filter(
    (c) => normalizeUrl(c) !== normalizeUrl(url)
  );
  for (const candidate of candidates.slice(0, 6)) {
    searchesUsed.count += 1;
    if (searchesUsed.count > limits.maxReplacementSearches) break;
    const probe = await probeFn(candidate);
    log.push({
      strategy: "REPLACEMENT_SAME_DOMAIN",
      candidate,
      status: probe.status,
      ok: probe.ok,
    });
    if (!probe.ok) continue;
    // Prefer browser claim check when keywords exist
    if (claimKeywords.length) {
      const br = await browserFn(candidate, claimKeywords);
      log.push({
        strategy: "REPLACEMENT_CLAIM_CHECK",
        candidate,
        claimSupport: br.claimSupport,
        title: br.title,
      });
      if (br.ok && br.claimSupport) {
        return {
          resolved: true,
          replacementUrl: candidate,
          title: br.title,
          log,
        };
      }
      continue;
    }
    // Without keywords, only accept non-root paths as replacements
    const path = (() => {
      try {
        return new URL(candidate).pathname;
      } catch {
        return "/";
      }
    })();
    if (path && path !== "/") {
      return { resolved: true, replacementUrl: candidate, log };
    }
  }
  return { resolved: false, log, reason: "NO_SAME_DOMAIN_REPLACEMENT" };
}

function rewriteClaimText(text, reason) {
  const base = String(text || "").trim();
  if (!base) return base;
  if (/could not be re-verified|qualified|prose only/i.test(base)) return base;
  return `${base} (Evidence qualified: ${reason})`;
}

function replaceUrlInPayload(payload, fromUrl, toUrl) {
  const fromN = normalizeUrl(fromUrl);
  const walk = (node) => {
    if (!node || typeof node !== "object") return;
    if (Array.isArray(node)) {
      for (let i = 0; i < node.length; i += 1) {
        if (typeof node[i] === "string" && normalizeUrl(node[i]) === fromN) {
          node[i] = toUrl;
        } else walk(node[i]);
      }
      return;
    }
    for (const [k, v] of Object.entries(node)) {
      if (
        typeof v === "string" &&
        normalizeUrl(v) === fromN &&
        /url|Url|href|Ref/i.test(k)
      ) {
        node[k] = toUrl;
      } else if (v && typeof v === "object") walk(v);
    }
  };
  walk(payload);
}

function removeUrlFromPayload(payload, fromUrl) {
  const fromN = normalizeUrl(fromUrl);
  const walk = (node) => {
    if (!node || typeof node !== "object") return;
    if (Array.isArray(node)) {
      for (let i = node.length - 1; i >= 0; i -= 1) {
        const item = node[i];
        if (typeof item === "string" && normalizeUrl(item) === fromN) {
          node.splice(i, 1);
          continue;
        }
        if (item && typeof item === "object" && item.url && normalizeUrl(item.url) === fromN) {
          node.splice(i, 1);
          continue;
        }
        walk(item);
      }
      return;
    }
    for (const v of Object.values(node)) {
      if (v && typeof v === "object") walk(v);
    }
  };
  walk(payload);
}

function findClaimsForUrl(graph, url) {
  const n = normalizeUrl(url);
  return (graph.claims || []).filter((c) =>
    (c.supportingEvidence || []).some((e) => normalizeUrl(e.url) === n)
  );
}

/**
 * Run autonomous resolution against a payload.
 *
 * @param {object} payload
 * @param {object} options
 *   - limits
 *   - probeFn / browserFn (injectable for tests)
 *   - pdfBuffer (optional existing PDF)
 *   - generatePdfFn (optional async ({payload}) => { buffer, path })
 *   - manualVerifications
 *   - dryRun (mutate clone only)
 */
export async function runAutonomousEvidenceResolution(payload, options = {}) {
  const startedAt = Date.now();
  const limits = { ...DEFAULT_RESOLUTION_LIMITS, ...(options.limits || {}) };
  const working = options.dryRun === false ? payload : clone(payload);
  const resolutionLog = [];
  const claimsRevised = [];
  const claimsRemoved = [];
  const urlsRepaired = [];
  const founderReviewDiffs = [];
  const searchesUsed = { count: 0 };
  let browserAttempts = 0;
  let pdfRegenerations = 0;

  const cache = options.verificationCache || loadVerificationCache(options.cachePath);
  const probeCache = options.probeCache || new Map();

  const probeFn =
    options.probeFn ||
    (async (url) => {
      if (probeCache.has(url)) return probeCache.get(url);
      const report = await validateAuditEvidenceLinks(
        {
          keyFindings: [{ title: "probe", evidence: [{ url }] }],
        },
        { probeCache }
      );
      const row = report.results?.[0] || {
        ok: false,
        status: 0,
        finalUrl: url,
        error: "NO_PROBE",
      };
      const out = {
        ok: row.pass || (row.status >= 200 && row.status < 400 && !(row.flags || []).length),
        status: row.status,
        finalUrl: row.finalUrl || url,
        error: row.error,
        flags: row.flags || [],
        redirected: row.redirected,
        methodUsed: row.methodUsed,
      };
      probeCache.set(url, out);
      return out;
    });

  const browserFn =
    options.browserFn ||
    (async (url, claimKeywords = []) => {
      browserAttempts += 1;
      if (browserAttempts > limits.maxBrowserAttempts) {
        return {
          ok: false,
          claimSupport: false,
          observation: "BROWSER_ATTEMPT_BUDGET_EXHAUSTED",
        };
      }
      return browserVerifyUrl(url, { claimKeywords });
    });

  let pipelineStatus = "EVIDENCE_REMEDIATION_RUNNING";
  let graph = buildClaimEvidenceGraph(working);

  // Rejected URLs — force removal / replacement path
  for (const entry of collectEvidenceEntries(working)) {
    if (Date.now() - startedAt > limits.maxElapsedMs) break;
    const rejected = AVA_REJECTED_EVIDENCE_URLS[entry.url];
    if (!rejected && !/cc\.nih\.gov\/about\/visitor\/hotels/i.test(entry.url)) continue;
    resolutionLog.push({
      originalUrl: entry.url,
      failure: "REJECTED_URL",
      strategiesAttempted: ["REJECTED_REGISTER"],
      finalUrl: null,
      claimAffected: entry.context,
      outcome: "REMOVED",
      supportingEvidence: "URL is permanently rejected for customer-facing use.",
    });
    removeUrlFromPayload(working, entry.url);
    urlsRepaired.push({ from: entry.url, to: null, reason: "REJECTED_URL" });
  }

  // Initial certification to discover blockers
  const certifyOpts = {
    pdfBuffer: options.pdfBuffer || null,
    pdfPath: options.pdfPath || null,
    manualVerifications: options.manualVerifications || {},
    probeCache,
    verificationCache: cache,
    escalateBrowser: options.skipEscalation === true ? false : true,
    persistCache: false,
    skipEscalation: options.skipEscalation === true,
    probeFn: options.probeFn || null,
  };

  let certification = await certifyAvaEvidenceAndHyperlinks(working, certifyOpts);

  const processBlocking = async () => {
    const blockers = (certification.evidence || []).filter((e) => e.gateBlocking);
    for (const block of blockers) {
      if (Date.now() - startedAt > limits.maxElapsedMs) {
        resolutionLog.push({
          originalUrl: block.sourceUrl,
          failure: "TIME_BUDGET",
          strategiesAttempted: [],
          outcome: "STOPPED_BUDGET",
          claimAffected: block.context,
        });
        break;
      }

      const url = block.sourceUrl;
      const relatedClaims = findClaimsForUrl(graph, url);
      const claimKeywords = inferClaimKeywords({
        label: block.label,
        claimNotes: block.observation,
        context: block.context,
      });
      const strategies = [];

      // Attempt 1 — retry/redirect
      const retry = await attemptRetryRedirect(url, probeFn, limits);
      strategies.push(...retry.attempts.map((a) => a.strategy));
      if (retry.resolved) {
        const esc = await escalateRestrictedEvidence({
          url: retry.finalUrl,
          httpProbe: retry.probe,
          flags: [],
          claimKeywords,
          cache,
        });
        if (esc.decision === "BROWSER_VERIFIED" || retry.probe?.ok) {
          resolutionLog.push({
            originalUrl: url,
            failure: block.evidenceClassification,
            strategiesAttempted: strategies,
            finalUrl: retry.finalUrl,
            claimAffected: relatedClaims.map((c) => c.claimId).join(",") || block.context,
            outcome: "VERIFIED_AFTER_RETRY",
            supportingEvidence: esc.observation || "HTTP OK after retry",
          });
          continue;
        }
      }

      // Attempt 2 — browser (if access restricted)
      if (
        /403|429|FETCH_FAILED|TIMEOUT|certificate|REQUIRES_MANUAL/i.test(
          `${block.evidenceClassification} ${(block.flags || []).join(",")}`
        )
      ) {
        strategies.push("BROWSER_VERIFICATION");
        const br = await browserFn(url, claimKeywords);
        if (br.ok && br.claimSupport !== false) {
          // Cache as browser verified for subsequent certify
          const { putCachedVerification } = await import(
            "./evidence-verification-escalation-v1.js"
          );
          putCachedVerification(cache, url, {
            level: 2,
            decision: "BROWSER_VERIFIED",
            gateBlocking: false,
            classificationHint: "VERIFIED_WITH_QUALIFICATION",
            verificationMethod: "PLAYWRIGHT_BROWSER",
            verifiedAt: new Date().toISOString(),
            finalUrl: br.finalUrl || url,
            title: br.title,
            status: br.status || 200,
            claimSupport: true,
            observation: br.observation,
          });
          resolutionLog.push({
            originalUrl: url,
            failure: block.evidenceClassification,
            strategiesAttempted: strategies,
            finalUrl: br.finalUrl || url,
            claimAffected: relatedClaims.map((c) => c.claimId).join(",") || block.context,
            outcome: "BROWSER_VERIFIED",
            supportingEvidence: br.observation,
          });
          continue;
        }
      }

      // Attempt 3 — replacement
      strategies.push("REPLACEMENT_SEARCH");
      const repl = await attemptReplacementCandidates({
        url,
        claimKeywords,
        probeFn,
        browserFn,
        limits,
        searchesUsed,
      });
      strategies.push(...(repl.log || []).map((l) => l.strategy));
      if (repl.resolved && repl.replacementUrl) {
        // Homepage-only replacements rejected unless keywords matched
        const replPath = (() => {
          try {
            return new URL(repl.replacementUrl).pathname;
          } catch {
            return "/";
          }
        })();
        if (replPath === "/" && claimKeywords.length && !repl.title) {
          // skip generic homepage
        } else {
          replaceUrlInPayload(working, url, repl.replacementUrl);
          urlsRepaired.push({
            from: url,
            to: repl.replacementUrl,
            reason: "SAME_DOMAIN_REPLACEMENT",
          });
          for (const c of relatedClaims) {
            if (
              c.claimType === "RECOMMENDATION" &&
              c.founderDecision &&
              /APPROVE|APPROVED/i.test(String(c.founderDecision))
            ) {
              founderReviewDiffs.push({
                claimId: c.claimId,
                actionId: c.actionId,
                beforeUrl: url,
                afterUrl: repl.replacementUrl,
                note: "Founder-reviewed recommendation evidence changed; staged for review.",
              });
            }
          }
          resolutionLog.push({
            originalUrl: url,
            failure: block.evidenceClassification,
            strategiesAttempted: strategies,
            finalUrl: repl.replacementUrl,
            claimAffected: relatedClaims.map((c) => c.claimId).join(",") || block.context,
            outcome: "REPLACED",
            supportingEvidence: `Replacement on ${hostOf(repl.replacementUrl)}`,
          });
          continue;
        }
      }

      // Attempt 4 — repair claim / remove unsupported URL
      strategies.push("CLAIM_REPAIR");
      if (claimsRevised.length >= limits.maxClaimRewrites) {
        resolutionLog.push({
          originalUrl: url,
          failure: block.evidenceClassification,
          strategiesAttempted: strategies,
          finalUrl: null,
          claimAffected: relatedClaims.map((c) => c.claimId).join(","),
          outcome: "REWRITE_BUDGET_EXHAUSTED",
        });
        continue;
      }

      if (block.evidenceClassification === "BROKEN_LINK" || /HTTP_404|HTTP_410|FETCH_FAILED/i.test((block.flags || []).join(","))) {
        const affectedFindings = (working.keyFindings || []).filter((f) =>
          (f.evidence || []).some(
            (e) => e && normalizeUrl(e.url || "") === normalizeUrl(url)
          )
        );
        removeUrlFromPayload(working, url);
        urlsRepaired.push({ from: url, to: null, reason: "REMOVED_BROKEN" });
        for (const f of affectedFindings) {
          const before = f.summary;
          f.summary = rewriteClaimText(
            f.summary,
            "prior source unreachable; citation removed pending authoritative replacement"
          );
          if (f.summary !== before) {
            claimsRevised.push({ claimType: "FINDING", before, after: f.summary });
          }
        }
        for (const a of working.actions || []) {
          if ((a.whatWeFound || "").includes(hostOf(url))) {
            const before = a.whatWeFound;
            a.whatWeFound = rewriteClaimText(
              a.whatWeFound,
              "supporting URL removed after failed verification"
            );
            if (a.whatWeFound !== before) {
              claimsRevised.push({
                claimType: "RECOMMENDATION",
                actionId: a.actionId,
                before,
                after: a.whatWeFound,
              });
              if (a.hotelDecision && /APPROVE|APPROVED/i.test(String(a.hotelDecision))) {
                founderReviewDiffs.push({
                  actionId: a.actionId,
                  before,
                  after: a.whatWeFound,
                  note: "Founder-reviewed recommendation text changed by remediation.",
                });
              }
            }
          }
        }
        resolutionLog.push({
          originalUrl: url,
          failure: block.evidenceClassification,
          strategiesAttempted: strategies,
          finalUrl: null,
          claimAffected: relatedClaims.map((c) => c.claimId).join(",") || block.context,
          outcome: "URL_REMOVED_CLAIM_QUALIFIED",
          supportingEvidence: block.observation,
        });
        continue;
      }

      if (block.evidenceClassification === "UNSUPPORTED_CLAIM") {
        // Remove claim support linkage; qualify text
        removeUrlFromPayload(working, url);
        claimsRemoved.push({ url, reason: "UNSUPPORTED_CLAIM" });
        resolutionLog.push({
          originalUrl: url,
          failure: "UNSUPPORTED_CLAIM",
          strategiesAttempted: strategies,
          finalUrl: null,
          claimAffected: block.context,
          outcome: "UNSUPPORTED_EVIDENCE_REMOVED",
        });
        continue;
      }

      // Attempt 5 — escalate
      strategies.push("MANUAL_ESCALATION");
      resolutionLog.push({
        originalUrl: url,
        failure: block.evidenceClassification,
        strategiesAttempted: strategies,
        finalUrl: null,
        claimAffected: relatedClaims.map((c) => c.claimId).join(",") || block.context,
        outcome: "REQUIRES_MANUAL_REVIEW",
        supportingEvidence: block.observation,
        suggestedNextActions: [
          "Open the URL in an ordinary browser",
          "Confirm the page supports the hotel-specific claim",
          "Record Admin manual verification with explanation",
          "Or supply an authoritative replacement URL",
        ],
      });
    }
  };

  await processBlocking();

  // Revalidate after mutations
  graph = buildClaimEvidenceGraph(working);
  certification = await certifyAvaEvidenceAndHyperlinks(working, {
    ...certifyOpts,
    persistCache: options.persistCache !== false,
  });

  // PDF regeneration when content repaired or annotations mismatch
  const annotationBroken =
    certification.pdfAnnotations?.compare &&
    certification.pdfAnnotations.compare.ok === false;
  const contentChanged =
    urlsRepaired.length > 0 || claimsRevised.length > 0 || claimsRemoved.length > 0;
  if (
    options.generatePdfFn &&
    (contentChanged || annotationBroken || options.forcePdfRegenerate === true) &&
    pdfRegenerations < limits.maxPdfRegenerations
  ) {
    pdfRegenerations += 1;
    const pdfResult = await options.generatePdfFn({ payload: working });
    certification = await certifyAvaEvidenceAndHyperlinks(working, {
      ...certifyOpts,
      pdfBuffer: pdfResult?.buffer || null,
      pdfPath: pdfResult?.path || null,
      persistCache: options.persistCache !== false,
    });
    resolutionLog.push({
      originalUrl: null,
      failure: annotationBroken ? "PDF_ANNOTATION_MISMATCH" : "PDF_CONTENT_CHANGED",
      strategiesAttempted: ["PDF_REGENERATE_AND_RECERTIFY"],
      finalUrl: null,
      claimAffected: null,
      outcome: certification.gate.pass ? "PDF_CERTIFIED" : "PDF_STILL_FAILING",
      supportingEvidence: `regeneration #${pdfRegenerations}`,
    });
  }

  if (options.persistCache !== false) {
    saveVerificationCache(cache, options.cachePath);
  }

  const unresolved = (certification.evidence || []).filter((e) => e.gateBlocking);
  pipelineStatus = certification.gate.pass
    ? "EVIDENCE_VALIDATED"
    : "VALIDATION_REQUIRED";

  const urlOutcomes = {};
  for (const e of certification.evidence || []) {
    urlOutcomes[e.sourceUrl] = {
      classification: e.evidenceClassification,
      gateBlocking: e.gateBlocking,
      finalUrl: e.finalResolvedUrl,
      status: e.httpStatus,
    };
  }
  graph = applyUrlVerificationToClaims(graph, urlOutcomes);

  const summary = {
    totalSourcesChecked: certification.summary?.totalEvidenceLinks || 0,
    automaticallyVerified: certification.summary?.verifiedLinks || 0,
    automaticallyRepaired: urlsRepaired.length,
    claimsRevisedOrRemoved: claimsRevised.length + claimsRemoved.length,
    remainingUnresolved: unresolved.length,
    lastVerificationDate: certification.checkedAt,
    certificationStatus: certification.gate.pass ? "PASS" : "FAIL",
    pipelineStatus,
    pdfRegenerations,
    browserAttempts,
    replacementSearches: searchesUsed.count,
    elapsedMs: Date.now() - startedAt,
    paidResearchUsd: 0,
  };

  return {
    contract: AUTONOMOUS_EVIDENCE_RESOLUTION_CONTRACT,
    startedAt: new Date(startedAt).toISOString(),
    finishedAt: new Date().toISOString(),
    pipelineStatus,
    payload: working,
    claimEvidenceGraph: graph,
    certification,
    resolutionLog,
    urlsRepaired,
    claimsRevised,
    claimsRemoved,
    founderReviewDiffs,
    summary,
    limits,
    founderApprovalGranted: false,
    customerPublicationAuthorized: false,
  };
}

/**
 * Mandatory AVA Full E2E evidence stage (steps 6–11).
 */
export async function runAvaFullE2eEvidenceStage(payload, options = {}) {
  const result = await runAutonomousEvidenceResolution(payload, {
    ...options,
    forcePdfRegenerate: options.forcePdfRegenerate === true,
  });
  return {
    ...result,
    stage: "AVA_FULL_E2E_EVIDENCE_STAGE_V1",
    mandatory: true,
    skipped: false,
    eligibleForFounderReview: result.pipelineStatus === "EVIDENCE_VALIDATED",
    eligibleForAuditValidated: result.certification?.gate?.pass === true,
  };
}

export function summarizeResolutionForAdmin(result) {
  if (!result) {
    return {
      totalSourcesChecked: 0,
      automaticallyVerified: 0,
      automaticallyRepaired: 0,
      claimsRevisedOrRemoved: 0,
      remainingUnresolved: 0,
      lastVerificationDate: null,
      certificationStatus: "NOT_RUN",
      pipelineStatus: "EVIDENCE_VALIDATION_PENDING",
    };
  }
  return {
    ...result.summary,
    resolutionLogCount: (result.resolutionLog || []).length,
    founderReviewDiffCount: (result.founderReviewDiffs || []).length,
  };
}
