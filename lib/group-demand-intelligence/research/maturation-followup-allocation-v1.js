/**
 * Generic hotel-scoped maturation follow-up allocation.
 *
 * Builds bounded Parallel tasks from existing candidate + blocker + strategy.
 * Reusable across hotels — hotel facts come from config, not code branches.
 * Stable taskId → stable Parallel job key → providerRunId reuse.
 */

import { createHash } from "node:crypto";
import { PARALLEL_PRICE_ESTIMATES_USD } from "../parallel/budget-ledger-v1.js";
import { canonicalizeSegmentId } from "./research-constants-v1.js";

export const MATURATION_FOLLOWUP_ALLOCATION_VERSION =
  "gdi-maturation-followup-allocation-v1";

function slug(s, max = 40) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "")
    .slice(0, max);
}

export function buildMaturationFollowUpTaskId({
  opportunityId,
  blockerKey,
  strategy,
  /** Bump when public strategies completed after a premature Parallel run */
  publicPathEpoch = "",
} = {}) {
  const base = `${opportunityId || "opp"}:${blockerKey || "b"}:${strategy || "s"}:${publicPathEpoch || ""}`;
  const hash = createHash("sha1").update(base).digest("hex").slice(0, 10);
  return `mat_fu_${hash}`;
}

/**
 * Build one follow-up Parallel task for a known opportunity gap.
 */
export function buildMaturationFollowUpTask({
  hotelId,
  hotelConfig = {},
  opportunity = {},
  blockerKey,
  strategy,
  queries = [],
  reserveUsd = PARALLEL_PRICE_ESTIMATES_USD.base,
  processor = "base",
  authorizationRef = null,
} = {}) {
  const publicAttempted = (opportunity.researchAttempts || [])
    .filter(
      (a) =>
        a &&
        a.blocker === blockerKey &&
        a.strategy &&
        a.strategy !== "parallel_targeted_followup" &&
        a.provider !== "parallel"
    )
    .map((a) => a.strategy);
  const publicPathEpoch = publicAttempted.length
    ? createHash("sha1")
        .update([...new Set(publicAttempted)].sort().join("|"))
        .digest("hex")
        .slice(0, 8)
    : "";
  const taskId = buildMaturationFollowUpTaskId({
    opportunityId: opportunity.id || opportunity.opportunityId,
    blockerKey,
    strategy,
    publicPathEpoch,
  });
  const org =
    opportunity.organizationName || opportunity.company || opportunity.name || "organization";
  const segment = canonicalizeSegmentId(
    opportunity.segment || opportunity.segmentId || "unknown"
  );
  const market =
    hotelConfig.demandTerritory?.label ||
    hotelConfig.market ||
    hotelConfig.city ||
    "hotel market";
  const hotelName =
    hotelConfig.displayName || hotelConfig.name || "the target hotel";

  const promptFocus = [
    `Targeted follow-up research for an EXISTING opportunity — do not invent a new unrelated account.`,
    `Organization: ${org}`,
    `Hotel: ${hotelName} (${hotelId}) in ${market}`,
    `Segment: ${segment}`,
    `Blocker to resolve with source evidence: ${blockerKey}`,
    `Research strategy: ${strategy}`,
    queries.length ? `Preferred queries: ${queries.slice(0, 4).join(" | ")}` : null,
    `Return only source-backed facts that address ${blockerKey}.`,
    `Do NOT invent people, emails, phone numbers, or future dates.`,
    `Do NOT return competing hotels as buyer prospects.`,
  ]
    .filter(Boolean)
    .join("\n");

  return {
    taskId,
    hotelId,
    segment,
    kind: "maturation_followup",
    allocationKind: MATURATION_FOLLOWUP_ALLOCATION_VERSION,
    blockerKey,
    strategy,
    opportunityId: opportunity.id || opportunity.opportunityId || null,
    organizationName: org,
    processor,
    reserveUsd: Number(reserveUsd),
    maxTasks: 1,
    stopCriteria:
      "Stop after 1 targeted follow-up task for this opportunity+blocker+strategy. Do not invent candidates. Source-backed evidence only.",
    promptFocus,
    queries: queries.slice(0, 6),
    authorizationRef,
    jobSegment: `maturation-fu-v1-${taskId}`,
  };
}

/**
 * Build a small allocation batch from prioritized maturation targets.
 * Generic — works for any hotelId + hotelConfig.
 */
export function buildMaturationFollowUpAllocation({
  hotelId,
  hotelConfig = {},
  targets = [],
  authorizationRef = null,
  authorizedUsd = null,
  maxTasks = 5,
  defaultReserveUsd = PARALLEL_PRICE_ESTIMATES_USD.base,
} = {}) {
  const tasks = [];
  let reserved = 0;
  const cap =
    authorizedUsd == null ? Infinity : Math.max(0, Number(authorizedUsd));

  for (const t of targets.slice(0, maxTasks * 2)) {
    if (tasks.length >= maxTasks) break;
    const reserveUsd = Number(t.reserveUsd ?? defaultReserveUsd);
    if (reserved + reserveUsd > cap) break;
    const task = buildMaturationFollowUpTask({
      hotelId,
      hotelConfig,
      opportunity: t.opportunity || t,
      blockerKey: t.blockerKey || t.blocker,
      strategy: t.strategy,
      queries: t.queries || [],
      reserveUsd,
      processor: t.processor || "base",
      authorizationRef,
    });
    tasks.push(task);
    reserved += reserveUsd;
  }

  return {
    version: MATURATION_FOLLOWUP_ALLOCATION_VERSION,
    hotelId,
    authorizationRef,
    authorizedUsd: authorizedUsd == null ? null : Number(authorizedUsd),
    reservedUsd: Number(reserved.toFixed(4)),
    taskCount: tasks.length,
    tasks,
    note: "Hotel-scoped maturation follow-ups — candidate+blocker+strategy stable IDs; not a full rediscovery wave",
  };
}

export function maturationJobKey(hotelId, task) {
  const segment = task.jobSegment || `maturation-fu-v1-${task.taskId || slug(task.strategy)}`;
  return `gdi-parallel-${String(hotelId).slice(0, 12)}-${segment}`;
}
