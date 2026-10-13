/**
 * Single staging-only hotel -> owner -> person -> contact workflow.
 *
 * This is deliberately opt-in for paid enrichment. The research handoff remains
 * the source of truth for ownership evidence; this module only connects its
 * already-gated output to the existing contact provider adapter.
 */
import {
  researchHotelOwnershipContactPath,
  buildEnrichmentSubjectsFromResearch,
} from "./ownership-contact-research-handoff.js";
import { enrichOwnerPersonContactAfterGate } from "./post-gate-contact-enrichment.js";

export const HOTEL_OWNERSHIP_CONTACT_WORKFLOW_VERSION =
  "hotel-ownership-contact-workflow-v1";

function contactDisabled(reason = "PAID_ENRICHMENT_DISABLED") {
  return {
    status: "DISABLED",
    reason,
    attempted: 0,
    results: [],
    canonical_writes: false,
    customer_publication: "BLOCKED",
  };
}

/**
 * Run the complete staging chain. No provider call occurs unless
 * `enable_contact_enrichment` is true and a provider function is supplied (or
 * the provider's configured client is explicitly selected).
 *
 * `research` can be injected for offline tests; production callers use the
 * existing researchHotelOwnershipContactPath by default.
 */
export async function runHotelOwnershipContactWorkflow(
  caseInput = {},
  budgets = {},
  deps = {}
) {
  const researchFn = deps.research || researchHotelOwnershipContactPath;
  const researchDeps = {
    ...deps,
    onOperationCheckpoint: deps.onOperationCheckpoint,
    operation_journal: deps.operation_journal || caseInput.operation_journal,
    search: deps.search,
    scrape: deps.scrape,
    isConfigured: deps.isConfigured,
    serpGoogle: deps.serpGoogle,
  };
  const research = await researchFn(caseInput, budgets, researchDeps);
  if (!research?.ok) {
    return {
      version: HOTEL_OWNERSHIP_CONTACT_WORKFLOW_VERSION,
      ok: false,
      research,
      enrichment_subjects: [],
      rejected_pre_submission: [],
      contact_enrichment: contactDisabled("RESEARCH_FAILED"),
      write_guarantees: {
        canonical_writes: false,
        customer_publication: "BLOCKED",
      },
    };
  }

  const preview = buildEnrichmentSubjectsFromResearch([research], {
    maxPeople: Number(budgets.max_people || 10),
    maxPerOwner: Number(budgets.max_people_per_owner || 2),
  });
  // Trusted deny wins: deps.enableContactEnrichment === false always blocks
  const enable =
    deps.enableContactEnrichment !== false &&
    (deps.enableContactEnrichment === true ||
      budgets.enable_contact_enrichment === true ||
      caseInput.enable_contact_enrichment === true) &&
    Number(budgets.enrichment_max ?? 0) > 0;

  if (!enable) {
    return {
      version: HOTEL_OWNERSHIP_CONTACT_WORKFLOW_VERSION,
      ok: true,
      research,
      enrichment_subjects: preview.subjects,
      rejected_pre_submission: preview.rejected_pre_submission,
      contact_enrichment: contactDisabled(),
      write_guarantees: {
        canonical_writes: false,
        customer_publication: "BLOCKED",
      },
    };
  }

  const provider = deps.contact_provider || budgets.contact_provider || "surfe";
  const enrichmentMax = Number(budgets.enrichment_max);
  const maxAttempts =
    Number.isFinite(enrichmentMax) && enrichmentMax >= 0 ? Math.floor(enrichmentMax) : preview.subjects.length;
  if (maxAttempts <= 0) {
    return {
      version: HOTEL_OWNERSHIP_CONTACT_WORKFLOW_VERSION,
      ok: true,
      research,
      enrichment_subjects: preview.subjects,
      rejected_pre_submission: preview.rejected_pre_submission,
      contact_enrichment: contactDisabled("ENRICHMENT_MAX_ZERO"),
      write_guarantees: {
        canonical_writes: false,
        customer_publication: "BLOCKED",
      },
    };
  }

  const results = [];
  const subjects = preview.subjects.slice(0, maxAttempts);
  for (const subject of subjects) {
    if (typeof deps.onOperationCheckpoint === "function") {
      await deps.onOperationCheckpoint({
        stage: "CONTACT",
        event: "enrichment_reservation",
        subject_id: subject.subject_id,
        provider,
      });
    }
    const result = await enrichOwnerPersonContactAfterGate({
      candidate: subject,
      gate: subject.gate,
      subject,
      contact_provider: provider,
      require_gate: true,
      surfeEnrichFn: deps.surfeEnrichFn,
      pdlEnrichFn: deps.pdlEnrichFn,
    });
    results.push({ subject_id: subject.subject_id, result });
    if (typeof deps.onOperationCheckpoint === "function") {
      await deps.onOperationCheckpoint({
        stage: "CONTACT",
        event: "enrichment_settled",
        subject_id: subject.subject_id,
        status: result?.status || null,
      });
    }
  }

  return {
    version: HOTEL_OWNERSHIP_CONTACT_WORKFLOW_VERSION,
    ok: true,
    research,
    enrichment_subjects: preview.subjects,
    rejected_pre_submission: preview.rejected_pre_submission,
    contact_enrichment: {
      status: "COMPLETED",
      attempted: results.length,
      results,
      canonical_writes: false,
      customer_publication: "BLOCKED",
    },
    write_guarantees: {
      canonical_writes: false,
      customer_publication: "BLOCKED",
    },
  };
}
