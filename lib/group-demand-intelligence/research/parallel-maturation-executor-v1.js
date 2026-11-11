/**
 * Parallel executor for candidate/blocker maturation follow-ups.
 * Uses generic maturation allocation + existing executePaidParallelTask resume.
 */

import { executePaidParallelTask } from "../workflow/paid-multisegment-research-v1.js";
import { buildMaturationFollowUpTask } from "./maturation-followup-allocation-v1.js";
import { PROVIDER_KIND } from "./provider-cost-router-v1.js";
import { PARALLEL_PRICE_ESTIMATES_USD } from "../parallel/budget-ledger-v1.js";

function mapCandidatesToEvidence(candidates = [], blockerKey) {
  const evidence = [];
  const deepPatch = {};
  let newMaterialEvidence = false;

  for (const c of candidates.slice(0, 8)) {
    const urls = []
      .concat(c.evidenceUrls || [])
      .concat(c.sourceUrl ? [c.sourceUrl] : [])
      .concat(c.officialUrl ? [c.officialUrl] : [])
      .filter((u) => /^https?:\/\//i.test(String(u)));
    for (const url of urls) {
      evidence.push({
        sourceFamily: "parallel",
        url,
        kind: "PARALLEL_FOLLOWUP_CANDIDATE",
        organizationName: c.organizationName || null,
        isMaterialDemandEvidence: true,
      });
    }

    if (blockerKey === "future_demand" && (c.eventStartDate || c.futureDate || c.dateWindow)) {
      deepPatch.futureDemand = {
        supported: true,
        dateWindow: String(c.eventStartDate || c.futureDate || c.dateWindow),
        sourceUrl: urls[0] || null,
      };
      newMaterialEvidence = true;
    }
    if (
      blockerKey === "demand_scale" &&
      (c.attendeeCount || c.peakRooms || c.roomBlock)
    ) {
      deepPatch.demandScale = {
        supported: true,
        hotelRelevant: true,
        attendeeCount: c.attendeeCount || null,
        estimatedPeakRooms: c.peakRooms || c.roomBlock || null,
        sourceUrl: urls[0] || null,
      };
      newMaterialEvidence = true;
    }
    if (blockerKey === "contact_route") {
      const contact = {};
      if (c.contactEmail || c.businessEmail) {
        contact.businessEmail = c.contactEmail || c.businessEmail;
      }
      if (c.contactPhone || c.businessPhone) {
        contact.businessPhone = c.contactPhone || c.businessPhone;
      }
      if (c.contactUrl || c.contactFormUrl) {
        contact.contactFormUrl = c.contactUrl || c.contactFormUrl;
      }
      if (Object.keys(contact).length) {
        deepPatch.contact = { ...(deepPatch.contact || {}), ...contact };
        newMaterialEvidence = true;
      }
    }
    if (
      blockerKey === "buyer_identity" &&
      c.contactName &&
      (c.contactRole || c.role || c.title)
    ) {
      deepPatch.contact = {
        ...(deepPatch.contact || {}),
        personName: c.contactName,
        role: c.contactRole || c.role || c.title,
      };
      newMaterialEvidence = true;
    }
  }

  if (evidence.length && !newMaterialEvidence) {
    // Corroboration-only: URLs without structured fields
    if (blockerKey === "corroboration") newMaterialEvidence = true;
  }

  return { evidence, deepPatch, newMaterialEvidence };
}

export function createParallelMaturationExecutor(defaults = {}) {
  return {
    async execute({ opportunity, blocker, action, options = {} } = {}) {
      const hotelId =
        options.hotelId ||
        opportunity.hotel?.hotelId ||
        opportunity.hotel?.canonicalHotelId ||
        opportunity.hotelId;
      const hotelConfig = options.hotelConfig || opportunity.hotel || {};
      const blockerKey = blocker?.key || blocker;

      if (options.paidBlocked) {
        return {
          newMaterialEvidence: false,
          provider: PROVIDER_KIND.PARALLEL,
          costUsd: 0,
          disposition: "PAID_BLOCKED",
          notes: "Parallel maturation follow-up blocked by paidBlocked",
        };
      }

      const task = buildMaturationFollowUpTask({
        hotelId,
        hotelConfig,
        opportunity,
        blockerKey,
        strategy: action?.strategy || "targeted_followup",
        queries: action?.queries || [],
        reserveUsd:
          options.reserveUsd ?? PARALLEL_PRICE_ESTIMATES_USD.base,
        processor: options.processor || "base",
        authorizationRef: options.authorizationRef || null,
      });

      const result = await executePaidParallelTask(task, {
        env: options.env || process.env,
        hotelConfig,
        ledgerPath: options.ledgerPath,
        maxUsd: options.maxUsd ?? options.remainingBudgetUsd,
        timeoutMs: options.timeoutMs,
        ...defaults,
      });

      if (
        result.status === "blocked" ||
        result.status === "incomplete" ||
        result.status === "failed"
      ) {
        return {
          newMaterialEvidence: false,
          provider: PROVIDER_KIND.PARALLEL,
          providerRunId: result.providerRunId || null,
          costUsd: Number(result.actualBilledUsd || 0),
          disposition: String(result.status || "FAILED").toUpperCase(),
          failureReason: result.reason || null,
          notes: result.note || result.reason || "parallel_followup_not_completed",
          taskId: task.taskId,
        };
      }

      const mapped = mapCandidatesToEvidence(result.candidates || [], blockerKey);
      return {
        newMaterialEvidence: mapped.newMaterialEvidence,
        provider: PROVIDER_KIND.PARALLEL,
        providerRunId: result.providerRunId || null,
        reusedProviderRun: Boolean(result.resumed),
        sourceFamilies: ["parallel"],
        costUsd: Number(result.actualBilledUsd || result.estimatedUsd || 0),
        disposition: result.resumed ? "REUSED_PROVIDER_RUN" : "COMPLETED",
        deepPatch: Object.keys(mapped.deepPatch).length
          ? mapped.deepPatch
          : undefined,
        evidence: mapped.evidence,
        evidenceIds: mapped.evidence.map((e) => e.url).filter(Boolean),
        taskId: task.taskId,
        jobSegment: task.jobSegment,
        notes: result.note || "maturation parallel follow-up",
        isCompletionAuthority: false,
      };
    },
  };
}
