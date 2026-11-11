/**
 * Composite maturation researcher:
 * 1) Cost-route provider for blocker
 * 2) If Jev_route → get strategy hint (no facts)
 * 3) Execute evidence acquisition via direct_public and/or Parallel adapters
 *
 * Jev never completes / publishes / invents evidence.
 */

import {
  selectProviderForBlocker,
  PROVIDER_KIND,
} from "./provider-cost-router-v1.js";
import {
  executeJevRouteForBlocker,
  assertJevNotCompletionAuthority,
  JEV_ACTION_TO_STRATEGY,
} from "./jev-maturation-adapter-v1.js";
import { isJevEnabled } from "../jev/jev-config.js";
import {
  HeuristicResearchPlanner,
  isParallelStrategy,
} from "./research-planner-v1.js";
import { runBenchmarkProviderAttempt } from "./benchmark-provider-evaluation-v1.js";

/**
 * @param {object} deps
 * @param {object} [deps.directPublic] { execute({ opportunity, blocker, action, options }) }
 * @param {object} [deps.parallel] same shape
 * @param {object} [deps.surfe] same shape — only when named person exists
 * @param {Function} [deps.jevDecideFn] test inject
 * @param {object} [deps.planner]
 */
export function createMaturationResearcherRouter(deps = {}) {
  const planner = deps.planner || new HeuristicResearchPlanner();

  return {
    async execute({ opportunity, blocker, action, options = {} }) {
      if (
        options.benchmarkProvider === PROVIDER_KIND.EXA ||
        options.benchmarkProvider === PROVIDER_KIND.FIRECRAWL
      ) {
        const benchmark = await runBenchmarkProviderAttempt({
          opportunity,
          blocker,
          action,
          options,
          env: options.env,
          fetchImpl: options.fetchImpl,
          provider: options.benchmarkProvider,
          arm: options.benchmarkArm,
          operation: options.benchmarkOperation,
          operationParams: options.benchmarkOperationParams,
          ledgerPath: options.benchmarkBudgetLedgerPath,
          evidencePath: options.benchmarkEvidencePath,
        });
        return {
          newMaterialEvidence: Boolean(benchmark.qualification?.blockerClosed),
          provider: options.benchmarkProvider,
          sourceFamilies: benchmark.sourceFamilies || ["benchmark"],
          costUsd: Number(benchmark.actualUsd || 0),
          disposition: benchmark.stopReason || benchmark.status,
          evidence: benchmark.evidence || [],
          isCompletionAuthority: false,
          notes: benchmark.notes || null,
          benchmarkAttempt: benchmark,
          providerSelection: {
            provider: options.benchmarkProvider,
            reason: "BENCHMARK_EVALUATION_ONLY",
          },
        };
      }
      // Do NOT permanently exclude providers after one attempt — strategy/fingerprint
      // dedupe lives in the supervisor. Skipping whole providers blocked Jev→public
      // escalation and multi-strategy public fetches.
      const jevEnabled =
        options.jevEnabled === false
          ? false
          : options.jevEnabled === true ||
            Boolean(options.forceJev || isJevEnabled());
      const selection = selectProviderForBlocker({
        blockerKey: blocker.key,
        opportunityExists: Boolean(
          opportunity.id || opportunity.organizationName || opportunity.name
        ),
        paidBlocked: Boolean(options.paidBlocked),
        jevEnabled,
        parallelEnabled:
          options.parallelEnabled !== false && Boolean(deps.parallel),
        attemptedProviders: [],
        remainingBudgetUsd: options.remainingBudgetUsd ?? options.maxCostUsd ?? Infinity,
      });

      // Prefer planner strategy; Jev may override strategy hint after routing call
      let effectiveAction = action || planner.plan({ opportunity, blocker, options });

      if (selection.provider === PROVIDER_KIND.NOOP) {
        return {
          newMaterialEvidence: false,
          provider: PROVIDER_KIND.NOOP,
          sourceFamilies: ["none"],
          costUsd: 0,
          disposition: "NO_ELIGIBLE_PROVIDER",
          notes: selection.reason,
          providerSelection: selection,
        };
      }

      // Explicit Parallel follow-up strategy (after public path) — do not re-route to public
      if (isParallelStrategy(effectiveAction?.strategy)) {
        if (options.paidBlocked) {
          return {
            newMaterialEvidence: false,
            provider: PROVIDER_KIND.PARALLEL,
            sourceFamilies: ["none"],
            costUsd: 0,
            disposition: "PAID_BLOCKED",
            stopHint: "PUBLIC_PATH_EXHAUSTED",
            notes:
              "Parallel follow-up justified but paidBlocked — classify PUBLIC_PATH_EXHAUSTED / PAID_FOLLOWUP_AVAILABLE, not RESEARCH_EXHAUSTED",
            providerSelection: {
              provider: PROVIDER_KIND.PARALLEL,
              reason: "PAID_BLOCKED",
            },
          };
        }
        if (!deps.parallel) {
          return {
            newMaterialEvidence: false,
            provider: PROVIDER_KIND.PARALLEL,
            sourceFamilies: ["none"],
            costUsd: 0,
            disposition: "EXECUTOR_NOT_INJECTED",
            stopHint: "PROVIDER_BLOCKED",
            notes: "Parallel strategy selected but executor not injected",
            providerSelection: selection,
          };
        }
        const parallelResult = await deps.parallel.execute({
          opportunity,
          blocker,
          action: effectiveAction,
          options,
        });
        return {
          ...parallelResult,
          provider: PROVIDER_KIND.PARALLEL,
          providerSelection: {
            provider: PROVIDER_KIND.PARALLEL,
            reason: "PLANNER_PARALLEL_FOLLOWUP",
          },
        };
      }

      // --- Jev routing pass (direction only) ---
      if (selection.provider === PROVIDER_KIND.JEV_ROUTE) {
        const jevResult = await executeJevRouteForBlocker({
          opportunity,
          blocker,
          options,
          decideFn: deps.jevDecideFn,
        });
        assertJevNotCompletionAuthority(jevResult);

        // If Jev suggests stop / wait — return routing record only (caller may exhaust)
        if (
          jevResult.disposition === "JEV_SUGGEST_STOP" ||
          !jevResult.strategyHint
        ) {
          return { ...jevResult, providerSelection: selection };
        }

        // Prefer Jev strategy for the *evidence* acquisition step
        const strategy =
          jevResult.strategyHint ||
          JEV_ACTION_TO_STRATEGY[jevResult.jevAction] ||
          effectiveAction.strategy;
        effectiveAction = {
          ...effectiveAction,
          strategy,
          jevAction: jevResult.jevAction,
          fingerprint: `${effectiveAction.fingerprint || ""}|jev:${jevResult.jevAction}`,
          provider: "router",
        };

        // Acquire evidence via cheapest executor capable of this strategy
        const execProvider =
          jevResult.nextRecommendedAction?.provider === PROVIDER_KIND.PARALLEL
            ? PROVIDER_KIND.PARALLEL
            : PROVIDER_KIND.DIRECT_PUBLIC;

        const executor =
          execProvider === PROVIDER_KIND.PARALLEL && deps.parallel
            ? deps.parallel
            : deps.directPublic;

        if (!executor) {
          // Return Jev routing alone — supervisor records attempt; next iteration may Parallel
          return {
            ...jevResult,
            notes: `${jevResult.notes}; no ${execProvider} executor injected — routing stored for resume`,
            providerSelection: selection,
            nextRecommendedAction: {
              provider: execProvider,
              strategy,
              jevAction: jevResult.jevAction,
            },
          };
        }

        const execResult = await executor.execute({
          opportunity,
          blocker,
          action: effectiveAction,
          options: { ...options, jevRoute: jevResult },
        });

        return {
          newMaterialEvidence: Boolean(execResult.newMaterialEvidence),
          provider: execResult.provider || execProvider,
          providerRunId: execResult.providerRunId || null,
          providerRequestId: execResult.providerRequestId || null,
          fingerprint: effectiveAction.fingerprint,
          sourceFamilies: [
            ...new Set([
              ...(jevResult.sourceFamilies || []),
              ...(execResult.sourceFamilies || []),
            ]),
          ],
          costUsd:
            Number(jevResult.costUsd || 0) + Number(execResult.costUsd || 0),
          disposition: execResult.disposition || "COMPLETED",
          evidence: [
            ...(jevResult.evidence || []),
            ...(execResult.evidence || []),
          ],
          evidenceIds: [
            ...(jevResult.evidenceIds || []),
            ...(execResult.evidenceIds || []),
          ],
          deepPatch: execResult.deepPatch,
          patch: execResult.patch,
          jevAction: jevResult.jevAction,
          strategyHint: strategy,
          isCompletionAuthority: false,
          notes: `Jev routed → ${execProvider} executed; Jev not completion authority`,
          providerSelection: selection,
          jevRouting: {
            decisionId: jevResult.providerRunId,
            providerRequestId: jevResult.providerRequestId,
            action: jevResult.jevAction,
            costUsd: jevResult.costUsd,
          },
        };
      }

      // --- Direct public ---
      if (selection.provider === PROVIDER_KIND.DIRECT_PUBLIC) {
        if (!deps.directPublic) {
          return {
            newMaterialEvidence: false,
            provider: PROVIDER_KIND.DIRECT_PUBLIC,
            sourceFamilies: ["none"],
            costUsd: 0,
            disposition: "EXECUTOR_NOT_INJECTED",
            notes: "direct_public preferred but executor not injected",
            providerSelection: selection,
          };
        }
        const r = await deps.directPublic.execute({
          opportunity,
          blocker,
          action: effectiveAction,
          options,
        });
        return { ...r, providerSelection: selection, isCompletionAuthority: false };
      }

      // --- Parallel ---
      if (selection.provider === PROVIDER_KIND.PARALLEL) {
        if (!deps.parallel) {
          return {
            newMaterialEvidence: false,
            provider: PROVIDER_KIND.PARALLEL,
            sourceFamilies: ["none"],
            costUsd: 0,
            disposition: "EXECUTOR_NOT_INJECTED",
            notes: "parallel preferred but executor not injected",
            providerSelection: selection,
          };
        }
        const r = await deps.parallel.execute({
          opportunity,
          blocker,
          action: effectiveAction,
          options,
        });
        return { ...r, providerSelection: selection, isCompletionAuthority: false };
      }

      return {
        newMaterialEvidence: false,
        provider: selection.provider,
        costUsd: 0,
        disposition: "UNSUPPORTED_PROVIDER",
        providerSelection: selection,
      };
    },
  };
}
