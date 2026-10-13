/**
 * Owner-contact workflow benchmark helpers (offline + dry-plan).
 * Fail-closed budgets; no paid providers in this module.
 */
import {
  OWNER_DISJOINT_BENCHMARK_CAPS,
  buildHandoffHotel,
  buildHandoffCaseInput,
  assertHotelReadyForPaidResearch,
  normalizeBenchmarkBudgets,
  createRunBudgetLedger,
  worstCaseContextSpend,
  readContextSpendFromResearch,
  readModelSpendFromResearch,
  FORBIDDEN_BENCHMARK_INPUT_KEYS,
} from "./owner-disjoint-native-benchmark.js";
import { OWNERSHIP_RESEARCH_FAILURE_STAGE } from "./ownership-research-stage-trace.js";

export const OWNER_CONTACT_WORKFLOW_BENCHMARK_VERSION =
  "owner-contact-workflow-benchmark-v1";

/** Per-hotel Context cap (both arms combined). 5 × 8 = 40. */
export const OWNER_CONTACT_WORKFLOW_BENCHMARK_CAPS = Object.freeze({
  max_hotels: 5,
  context_dev_max_run: 40,
  context_dev_max_per_hotel: 8,
  /** Split across two arms so worst-case stays ≤ run cap. */
  context_dev_max_per_hotel_arm: 4,
  model_usd_max_run: 0.25,
  model_usd_max_per_hotel_arm: 0.05,
  serpapi_max: 0,
});

export const BENCHMARK_ARMS = Object.freeze({
  NATIVE_DISCOVERY_ONLY: "NATIVE_DISCOVERY_ONLY",
  E2E_WORKFLOW: "E2E_WORKFLOW",
});

export const PRESERVED_RUN_TAGS = Object.freeze(["benchmark-v1", "benchmark-v2"]);

export {
  buildHandoffHotel,
  buildHandoffCaseInput,
  assertHotelReadyForPaidResearch,
  normalizeBenchmarkBudgets,
  createRunBudgetLedger,
  worstCaseContextSpend,
  readContextSpendFromResearch,
  readModelSpendFromResearch,
  FORBIDDEN_BENCHMARK_INPUT_KEYS,
  OWNER_DISJOINT_BENCHMARK_CAPS,
};

/**
 * Classify earliest unresolved stage into product buckets.
 */
export function classifyFailureBucket(earliestFailureStage, extras = {}) {
  const stage = String(earliestFailureStage || "").toUpperCase();
  if (extras.provider_no_match) return "provider_no_match";
  if (extras.provider_returned_no_contact) return "provider_returned_no_contact";
  if (extras.contact_enrichment_failure) return "contact_enrichment_failure";
  if (
    stage === OWNERSHIP_RESEARCH_FAILURE_STAGE.PERSON_UNRESOLVED ||
    extras.person_qualification_failure
  ) {
    return "person_qualification_failure";
  }
  if (
    stage === OWNERSHIP_RESEARCH_FAILURE_STAGE.OWNER_DOMAIN_UNRESOLVED ||
    extras.organization_domain_failure
  ) {
    return "organization_domain_failure";
  }
  if (
    stage === OWNERSHIP_RESEARCH_FAILURE_STAGE.CONTACT_UNAVAILABLE ||
    extras.contact_unavailable
  ) {
    return "contact_enrichment_failure";
  }
  if (
    !stage ||
    stage === OWNERSHIP_RESEARCH_FAILURE_STAGE.NO_SEARCH_RESULT ||
    stage === OWNERSHIP_RESEARCH_FAILURE_STAGE.RESULT_FILTERED ||
    stage === OWNERSHIP_RESEARCH_FAILURE_STAGE.FETCH_FAILED ||
    stage === OWNERSHIP_RESEARCH_FAILURE_STAGE.NO_OWNERSHIP_PASSAGE ||
    stage === OWNERSHIP_RESEARCH_FAILURE_STAGE.RELATIONSHIP_AMBIGUOUS ||
    stage === OWNERSHIP_RESEARCH_FAILURE_STAGE.CURRENTNESS_UNRESOLVED
  ) {
    return "owner_discovery_failure";
  }
  return "owner_discovery_failure";
}

/**
 * Score a workflow or research result for the hotel→contact chain.
 * Does not invent owners/people — only reads returned fields.
 */
export function scoreOwnerContactChain(result = {}, { arm } = {}) {
  const research = result.research || result;
  const ownership = research.ownership || {};
  const claims = research.newly_researched?.ownership_claims || [];
  const people = (research.people || []).filter(
    (p) => p.display_name && !p.former_affiliation && !p.deceased
  );
  const domain =
    research.confirmed_company_domain_host ||
    research.confirmed_company_domain ||
    null;
  const ownerName = ownership.owner_display_name || null;
  const ownerClass = String(
    ownership.classification || ownership.owner_role || ""
  ).toUpperCase();

  const hasCurrentOwner = claims.some(
    (c) =>
      ["OWNS", "OWNED_BY", "SPONSORS"].includes(
        String(c.relationship || "").toUpperCase()
      ) &&
      String(c.currentness || "").toUpperCase() === "CURRENT_AS_OF_STATED_DATE" &&
      c.evidence_grounded
  );
  const evidencedOwnerSponsor = Boolean(
    ownerName &&
      ["PROPERTY_OWNER", "ECONOMIC_OWNER", "ECONOMIC_OWNER_OR_SPONSOR", "OWNER"].includes(
        ownerClass
      )
  );

  const subjects = result.enrichment_subjects || [];
  const gatedSubject = subjects.length > 0;
  const contact = result.contact_enrichment || {};
  const contactResults = contact.results || [];
  const providerReturned = contactResults.some((row) => {
    const r = row.result || row;
    const surfe = r.surfe || {};
    const pdl = r.pdl || {};
    return (
      surfe?.ok === true ||
      pdl?.score?.matched === true ||
      (surfe?.normalized && (surfe.normalized.matched || surfe.normalized.emails))
    );
  });
  const providerNoMatch = contactResults.some((row) => {
    const r = row.result || row;
    return (
      r.status === "OK" &&
      r.surfe?.normalized &&
      r.surfe.normalized.matched === false
    );
  });
  const providerNoContact = contactResults.some((row) => {
    const r = row.result || row;
    const emails = r.surfe?.normalized?.emails || r.pdl?.score?.emails || [];
    const phones = r.surfe?.normalized?.phones || r.pdl?.score?.phones || [];
    return r.status === "OK" && emails.length === 0 && phones.length === 0;
  });

  const attributableEmail = people.some((p) =>
    (p.channels || []).some((c) => /email/i.test(c.kind || "") && c.value)
  );
  const attributablePhone = people.some((p) =>
    (p.channels || []).some((c) => /phone|mobile/i.test(c.kind || "") && c.value)
  );

  const earliest =
    research.earliest_failure_stage ||
    research.iterative_ownership_loop?.earliest_failure_stage ||
    (evidencedOwnerSponsor
      ? domain
        ? people.length
          ? gatedSubject
            ? providerReturned
              ? OWNERSHIP_RESEARCH_FAILURE_STAGE.NONE
              : OWNERSHIP_RESEARCH_FAILURE_STAGE.CONTACT_UNAVAILABLE
            : OWNERSHIP_RESEARCH_FAILURE_STAGE.PERSON_UNRESOLVED
          : OWNERSHIP_RESEARCH_FAILURE_STAGE.PERSON_UNRESOLVED
        : OWNERSHIP_RESEARCH_FAILURE_STAGE.OWNER_DOMAIN_UNRESOLVED
      : OWNERSHIP_RESEARCH_FAILURE_STAGE.NO_OWNERSHIP_PASSAGE);

  const completeChain = Boolean(
    evidencedOwnerSponsor &&
      domain &&
      people.length &&
      gatedSubject &&
      (attributableEmail || providerReturned)
  );

  const failure_bucket = classifyFailureBucket(earliest, {
    person_qualification_failure:
      evidencedOwnerSponsor && domain && people.length === 0,
    organization_domain_failure: evidencedOwnerSponsor && !domain,
    contact_enrichment_failure:
      gatedSubject && contact.status === "DISABLED" && arm === BENCHMARK_ARMS.E2E_WORKFLOW,
    provider_no_match: providerNoMatch,
    provider_returned_no_contact: providerNoContact && !providerNoMatch,
  });

  return {
    arm: arm || null,
    evidenced_owner_sponsor: evidencedOwnerSponsor || hasCurrentOwner,
    current_owner_evidence: hasCurrentOwner,
    confirmed_organization_domain: Boolean(domain),
    relevant_person_independent_affiliation: people.some(
      (p) =>
        p.provenance?.independently_corroborated ||
        p.affiliation_status === "CORROBORATED"
    ),
    gated_enrichment_subject: gatedSubject,
    provider_contact_returned: Boolean(providerReturned),
    attributable_email: Boolean(attributableEmail),
    attributable_phone: Boolean(attributablePhone),
    complete_chain: completeChain,
    incorrect_owner_assignment: Boolean(
      ownerName && !evidencedOwnerSponsor && !hasCurrentOwner
    ),
    earliest_unresolved_stage: earliest,
    failure_bucket,
    write_guarantees: {
      canonical_writes:
        result.write_guarantees?.canonical_writes === true ||
        contact.canonical_writes === true
          ? true
          : false,
      customer_publication:
        result.write_guarantees?.customer_publication ||
        contact.customer_publication ||
        "BLOCKED",
    },
    cost: {
      context_dev: readContextSpendFromResearch(research),
      model_usd: readModelSpendFromResearch(research),
    },
  };
}

export function assertNotPreservedRunTag(runTag) {
  if (PRESERVED_RUN_TAGS.includes(String(runTag || ""))) {
    const err = new Error(
      `RUN_TAG_BLOCKED:${runTag} — never overwrite owner-disjoint benchmark-v1/v2; use a fresh namespace`
    );
    err.code = "RUN_TAG_BLOCKED";
    throw err;
  }
}

export function buildWorkflowPreflight(hotels, caps = OWNER_CONTACT_WORKFLOW_BENCHMARK_CAPS) {
  const worst = worstCaseContextSpend({
    hotelCount: hotels.length,
    arms: 2,
    perHotelArm: caps.context_dev_max_per_hotel_arm,
  });
  const errors = [];
  if (hotels.length > caps.max_hotels) {
    errors.push(`hotel_count_exceeds_max:${hotels.length}>${caps.max_hotels}`);
  }
  if (worst > caps.context_dev_max_run) {
    errors.push(`worst_case_context_exceeds_run_cap:${worst}>${caps.context_dev_max_run}`);
  }
  for (const h of hotels) {
    try {
      assertHotelReadyForPaidResearch(buildHandoffCaseInput(h, { iterative: true }));
    } catch (err) {
      errors.push(`${h.hotel_id}:${err.message}`);
    }
    const perHotelBothArms = caps.context_dev_max_per_hotel_arm * 2;
    if (perHotelBothArms > caps.context_dev_max_per_hotel) {
      errors.push(
        `per_hotel_arm_split_exceeds_hotel_cap:${perHotelBothArms}>${caps.context_dev_max_per_hotel}`
      );
    }
  }
  return {
    ok: errors.length === 0,
    errors,
    hotel_count: hotels.length,
    hotels: hotels.map((h) => {
      const caseInput = buildHandoffCaseInput(h, { iterative: false });
      const asserted = assertHotelReadyForPaidResearch(caseInput);
      return {
        hotel_id: h.hotel_id,
        hotel_name: h.hotel_name,
        city: h.city,
        country: h.country,
        first_query: asserted.first_query,
        query_count: asserted.queries.length,
        context_dev_max_per_hotel: caps.context_dev_max_per_hotel,
        context_dev_max_per_arm: caps.context_dev_max_per_hotel_arm,
        handoff_shape_ok: true,
      };
    }),
    per_hotel_context_budget: caps.context_dev_max_per_hotel,
    per_hotel_arm_context_budget: caps.context_dev_max_per_hotel_arm,
    combined_context_cap: caps.context_dev_max_run,
    worst_case_context_spend: worst,
    model_cap: caps.model_usd_max_run,
    providers: {
      context_dev: true,
      serpapi: false,
      parallel: false,
      webhound: false,
      surfe: false,
      fullenrich: false,
      pdl: false,
      apify: false,
      openai: false,
    },
    flags: {
      enrichment: false,
      outreach: false,
      customer_publication: false,
      airtable_census_writes: false,
      canonical_ownership_writes: false,
    },
    confirm_spend_required: true,
  };
}
