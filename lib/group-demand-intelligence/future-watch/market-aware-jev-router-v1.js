/**
 * Market-aware Jev next-action wrapper — research direction only.
 * Cannot promote, set WHO, mutate truth, or invent free-form actions.
 */

import { decideEvidenceGapNextAction } from "../evidence-gap/jev-evidence-gap-next-action.js";
import {
  defaultActionForBlocker,
  isAllowedJevAction,
  JEV_NEXT_ACTION,
} from "../evidence-gap/evidence-gap-model-v1.js";
import { archetypeBiasedDefaultAction } from "./market-archetype-v1.js";
import { suggestPriorAction } from "./jev-action-priors-v1.js";

/**
 * Build market-aware evidence packet + choose next action via Jev.
 * Deterministic rejectables must already be filtered out by caller.
 */
export async function decideMarketAwareNextAction({
  watch = {},
  packet = {},
  priorsLedger = null,
  remainingBudget = {},
  enableSafeApply = true,
} = {}) {
  const archetype = watch.marketArchetype || packet.marketArchetype || "OTHER";
  const primaryBlocker = watch.primaryBlocker || packet.primaryBlocker;
  const baseDefault = defaultActionForBlocker(primaryBlocker, watch.hotelShort || "");
  const biasedDefault = archetypeBiasedDefaultAction(
    primaryBlocker,
    archetype,
    baseDefault
  );

  const priorSug = suggestPriorAction({
    ledger: priorsLedger,
    archetype,
    blocker: primaryBlocker,
    sourceFamily: watch.sourceFamily || packet.sourceFamily,
    preferredActions: watch.preferredResearchActions || packet.preferredResearchActions || [],
    excludedActions: [JEV_NEXT_ACTION.VERIFY_WHO, JEV_NEXT_ACTION.VERIFY_WHO_ROLE],
    fallback: biasedDefault,
  });

  const existingDecision = priorSug.fromPrior ? priorSug.action : biasedDefault;

  const enriched = {
    ...packet,
    hotel: watch.hotel || packet.hotel,
    market: watch.market || packet.market,
    event: watch.event || packet.event,
    futureCycle: watch.cycleId || packet.futureCycle,
    marketArchetype: archetype,
    hotelType: packet.hotelType || watch.hotelShort || null,
    sourceFamily: watch.sourceFamily || packet.sourceFamily,
    primaryBlocker,
    secondaryBlocker: watch.secondaryBlocker || packet.secondaryBlocker,
    previousResearchActions: packet.previousResearchActions || [],
    previousFailedPaths: packet.previousFailedPaths || [],
    priorSuccessfulActionsForArchetype: (watch.preferredResearchActions || []).slice(0, 6),
    priorFailedActions: packet.previousFailedPaths || [],
    remainingBudget: {
      queries: remainingBudget.queries ?? 3,
      fetches: remainingBudget.fetches ?? 5,
      jevActions: remainingBudget.jevActions ?? 1,
    },
    resolvedDimensions: packet.resolvedDimensions || [],
    unresolvedDimensions: packet.unresolvedDimensions || [],
    sourceUrls: packet.sourceUrls || (watch.primarySource ? [watch.primarySource] : []),
    noPromote: true,
    noFinalWho: true,
    researchDirectionOnly: true,
  };

  const decision = await decideEvidenceGapNextAction({
    packet: enriched,
    primaryBlocker,
    hotelShort: watch.hotelShort || "",
    enableSafeApply,
  });

  // Re-bias applied action toward archetype when Jev falls back / agrees with weak default
  let applied = decision.appliedAction;
  if (!isAllowedJevAction(applied)) applied = existingDecision;
  // Never allow WHO on watch recheck unless lodging/commercial already plausible — deferred
  if (
    applied === JEV_NEXT_ACTION.VERIFY_WHO ||
    applied === JEV_NEXT_ACTION.VERIFY_WHO_ROLE
  ) {
    applied = biasedDefault;
  }

  return {
    ...decision,
    defaultAction: existingDecision,
    archetypeDefault: biasedDefault,
    priorSuggestion: priorSug,
    appliedAction: enableSafeApply ? applied : existingDecision,
    marketArchetype: archetype,
    canPromote: false,
    canSetWho: false,
    canMutateTruth: false,
  };
}
