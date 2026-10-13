/**
 * Observable comparison schema for Webhound teacher vs native ownership path.
 * Missing Webhound fields must be recorded as UNAVAILABLE — never inferred.
 */
export const OBSERVABLE_FIELD_STATUS = Object.freeze({
  PRESENT: "PRESENT",
  UNAVAILABLE: "UNAVAILABLE",
  EMPTY: "EMPTY",
});

/**
 * Canonical observable artifact keys both arms normalize into.
 */
export const OBSERVABLE_ARTIFACT_KEYS = Object.freeze([
  "input_hotel_object",
  "generated_queries_or_research_questions",
  "search_results_and_urls",
  "pages_fetched_or_read",
  "passages_or_snippets_used",
  "follow_up_questions",
  "owner_sponsor_candidates",
  "relationship_type",
  "currentness_or_date",
  "organization_or_domain",
  "person_candidates",
  "email_fields",
  "phone_fields",
  "confidence_and_uncertainty",
  "stop_reasons",
  "timing",
  "provider_cost",
  "raw_response_hashes",
]);

export const SCORE_DIMENSIONS = Object.freeze([
  "hotel_identity",
  "hotel_specific_ownership_evidence",
  "current_owner_or_sponsor_evidence",
  "owner_operator_brand_separation",
  "confirmed_owner_organization_domain",
  "relevant_person",
  "independently_supported_affiliation",
  "attributable_email",
  "attributable_phone",
  "complete_hotel_to_contact_chain",
  "incorrect_owner_assignments",
  "earliest_failure_stage",
]);

export const BEHAVIOR_DIFF_DIMENSIONS = Object.freeze([
  "query_generation",
  "source_selection",
  "source_ranking",
  "page_fetching",
  "document_reading",
  "historical_currentness_follow_up",
  "entity_resolution",
  "person_discovery",
  "contact_enrichment",
]);

export function emptyObservableSlot(status = OBSERVABLE_FIELD_STATUS.UNAVAILABLE, note = null) {
  return {
    status,
    value: null,
    note: note || (status === OBSERVABLE_FIELD_STATUS.UNAVAILABLE ? "Not exposed by provider export" : null),
  };
}

/**
 * Build empty observable envelope for one arm × hotel.
 */
export function createEmptyObservableEnvelope({ arm, hotel_id, hotel_name } = {}) {
  const artifacts = {};
  for (const k of OBSERVABLE_ARTIFACT_KEYS) {
    artifacts[k] = emptyObservableSlot(OBSERVABLE_FIELD_STATUS.UNAVAILABLE);
  }
  return {
    version: "webhound-native-observable-envelope-v1",
    arm: arm || null,
    hotel_id: hotel_id || null,
    hotel_name: hotel_name || null,
    artifacts,
    write_guarantees: {
      canonical_writes: false,
      customer_publication: "BLOCKED",
      enrichment_executed: false,
    },
  };
}

/**
 * Normalize native researchHotelOwnershipContactPath result into observable envelope.
 */
export function normalizeNativeObservables(research = {}, hotel = {}) {
  const env = createEmptyObservableEnvelope({
    arm: "NATIVE",
    hotel_id: hotel.hotel_id || research.hotel_id,
    hotel_name: hotel.hotel_name || research.hotel_name,
  });
  const present = (v) =>
    v == null || (Array.isArray(v) && v.length === 0) || v === ""
      ? OBSERVABLE_FIELD_STATUS.EMPTY
      : OBSERVABLE_FIELD_STATUS.PRESENT;

  env.artifacts.input_hotel_object = {
    status: OBSERVABLE_FIELD_STATUS.PRESENT,
    value: {
      hotel_id: hotel.hotel_id,
      hotel_name: hotel.hotel_name,
      city: hotel.city || null,
      country: hotel.country || null,
      language: hotel.language || null,
    },
    note: null,
  };

  const queries =
    (research.iterative_ownership_loop?.queries_attempted ||
      research.stage_trace?.searches?.map((s) => s.query) ||
      research.calls?.filter((c) => c.query).map((c) => c.query) ||
      []) ?? [];
  env.artifacts.generated_queries_or_research_questions = {
    status: present(queries),
    value: queries,
    note: null,
  };

  const urls =
    research.stage_trace?.stages
      ?.filter((s) => s.candidate_urls_before_filter)
      .flatMap((s) => s.candidate_urls_before_filter) ||
    research.calls?.filter((c) => c.url).map((c) => c.url) ||
    [];
  env.artifacts.search_results_and_urls = {
    status: present(urls),
    value: [...new Set(urls)],
    note: null,
  };

  const fetches =
    research.stage_trace?.fetches ||
    research.calls?.filter((c) => c.kind === "scrape" || c.kind === "scrape_markdown") ||
    [];
  env.artifacts.pages_fetched_or_read = {
    status: present(fetches),
    value: fetches.map((f) => ({
      url: f.url || null,
      ok: f.ok,
      characters: f.characters ?? null,
      fetch_outcome: f.fetch_outcome || null,
    })),
    note: null,
  };

  const passages =
    research.sources?.flatMap((s) => s.passages || []) ||
    research.stage_trace?.stages?.flatMap((s) => s.extracted_passages || []) ||
    [];
  env.artifacts.passages_or_snippets_used = {
    status: present(passages),
    value: passages.slice(0, 40),
    note: null,
  };

  const followUps =
    research.iterative_ownership_loop?.follow_up_goals ||
    research.newly_researched?.notes?.filter((n) => /follow_up/i.test(String(n))) ||
    [];
  env.artifacts.follow_up_questions = {
    status: present(followUps),
    value: followUps,
    note: null,
  };

  const owner = research.ownership || {};
  env.artifacts.owner_sponsor_candidates = {
    status: owner.owner_display_name ? OBSERVABLE_FIELD_STATUS.PRESENT : OBSERVABLE_FIELD_STATUS.EMPTY,
    value: owner.owner_display_name
      ? [
          {
            name: owner.owner_display_name,
            classification: owner.classification || null,
            entity_id: owner.owner_entity_id || null,
          },
        ]
      : [],
    note: null,
  };
  env.artifacts.relationship_type = {
    status: owner.classification ? OBSERVABLE_FIELD_STATUS.PRESENT : OBSERVABLE_FIELD_STATUS.EMPTY,
    value: owner.classification || null,
    note: null,
  };

  const claims = research.newly_researched?.ownership_claims || [];
  env.artifacts.currentness_or_date = {
    status: present(claims),
    value: claims.map((c) => ({
      subject: c.subject || c.name || null,
      currentness: c.currentness || null,
      source_date: c.source_date || c.event_date || null,
      relationship: c.relationship || null,
    })),
    note: null,
  };

  env.artifacts.organization_or_domain = {
    status: research.confirmed_company_domain_host
      ? OBSERVABLE_FIELD_STATUS.PRESENT
      : OBSERVABLE_FIELD_STATUS.EMPTY,
    value: {
      domain_host: research.confirmed_company_domain_host || null,
      domain_url: research.confirmed_company_domain || null,
    },
    note: null,
  };

  const people = research.people || [];
  env.artifacts.person_candidates = {
    status: present(people),
    value: people.map((p) => ({
      display_name: p.display_name || null,
      title: p.title || null,
      affiliation_status: p.affiliation_status || null,
    })),
    note: null,
  };

  const emails = people.flatMap((p) =>
    (p.channels || []).filter((c) => /email/i.test(c.kind || "")).map((c) => c.value)
  );
  const phones = people.flatMap((p) =>
    (p.channels || []).filter((c) => /phone|mobile/i.test(c.kind || "")).map((c) => c.value)
  );
  env.artifacts.email_fields = {
    status: present(emails),
    value: emails,
    note: "Discovery staging only; enrichment disabled in this diagnostic",
  };
  env.artifacts.phone_fields = {
    status: present(phones),
    value: phones,
    note: "Discovery staging only; enrichment disabled in this diagnostic",
  };

  env.artifacts.confidence_and_uncertainty = {
    status: OBSERVABLE_FIELD_STATUS.PRESENT,
    value: {
      ownership_confidence: owner.confidence || null,
      unresolved_reasons: research.unresolved_reasons || [],
      earliest_failure_stage: research.earliest_failure_stage || null,
    },
    note: null,
  };
  env.artifacts.stop_reasons = {
    status: present(research.iterative_ownership_loop?.stop_reasons || research.unresolved_reasons),
    value: research.iterative_ownership_loop?.stop_reasons || research.unresolved_reasons || [],
    note: null,
  };
  env.artifacts.timing = {
    status: research.elapsed_ms != null ? OBSERVABLE_FIELD_STATUS.PRESENT : OBSERVABLE_FIELD_STATUS.EMPTY,
    value: { elapsed_ms: research.elapsed_ms ?? null },
    note: null,
  };
  env.artifacts.provider_cost = {
    status: OBSERVABLE_FIELD_STATUS.PRESENT,
    value: {
      context_dev_spent_credits: research.budgets?.context_dev?.spent_credits ?? null,
      model_usd_spent: research.budgets?.model_usd?.spent_usd ?? null,
    },
    note: null,
  };
  env.artifacts.raw_response_hashes = {
    status: OBSERVABLE_FIELD_STATUS.EMPTY,
    value: null,
    note: "Filled by runner when caching raw research JSON",
  };
  return env;
}

/**
 * Normalize Webhound evidence-pack / tool exports into observable envelope.
 * Unknown shapes → UNAVAILABLE (never invent steps).
 */
export function normalizeWebhoundObservables(pack = {}, hotel = {}, meta = {}) {
  const env = createEmptyObservableEnvelope({
    arm: "WEBHOUND",
    hotel_id: hotel.hotel_id,
    hotel_name: hotel.hotel_name,
  });

  env.artifacts.input_hotel_object = {
    status: OBSERVABLE_FIELD_STATUS.PRESENT,
    value: {
      hotel_id: hotel.hotel_id,
      hotel_name: hotel.hotel_name,
      city: hotel.city || null,
      country: hotel.country || null,
      language: hotel.language || null,
    },
    note: null,
  };

  const structured = pack.structured || pack.structuredContent || pack;
  const sources = structured.sources || pack.sources || null;
  const claims = structured.claims || pack.claims || null;
  const output = structured.output || structured.document || pack.output || null;
  const working = structured.working_documents || structured.working_docs || null;
  const session = structured.session || pack.session || null;
  const spend = structured.spend || structured.cost || session?.spend || pack.spend || null;

  const set = (key, value, note = null) => {
    if (value === undefined || value === null) {
      env.artifacts[key] = {
        status: OBSERVABLE_FIELD_STATUS.UNAVAILABLE,
        value: null,
        note: note || "Not exposed in Webhound export for this session",
      };
      return;
    }
    if ((Array.isArray(value) && value.length === 0) || value === "") {
      env.artifacts[key] = { status: OBSERVABLE_FIELD_STATUS.EMPTY, value: Array.isArray(value) ? [] : value, note };
      return;
    }
    env.artifacts[key] = { status: OBSERVABLE_FIELD_STATUS.PRESENT, value, note };
  };

  set(
    "generated_queries_or_research_questions",
    structured.queries || structured.research_questions || session?.queries || undefined
  );
  set(
    "search_results_and_urls",
    Array.isArray(sources)
      ? sources.map((s) => s.url || s.source_url || s).filter(Boolean)
      : sources == null
        ? undefined
        : []
  );
  set("pages_fetched_or_read", structured.pages_read || structured.fetched_urls || undefined);
  set(
    "passages_or_snippets_used",
    Array.isArray(claims)
      ? claims.map((c) => c.excerpt || c.passage || c.supporting_passage || c.text).filter(Boolean)
      : undefined
  );
  set("follow_up_questions", structured.follow_up_questions || working?.follow_ups || undefined);
  set("owner_sponsor_candidates", structured.owner_candidates || structured.ownership_claims || undefined);
  set("relationship_type", structured.relationship_types || undefined);
  set("currentness_or_date", structured.currentness || undefined);
  set("organization_or_domain", structured.organization || structured.domains || undefined);
  set("person_candidates", structured.people || structured.person_candidates || undefined);
  set("email_fields", structured.emails || undefined, "Enrichment disabled — only surface if Webhound returned without paid Surfe/PDL");
  set("phone_fields", structured.phones || undefined, "Enrichment disabled — only surface if Webhound returned without paid Surfe/PDL");
  set("confidence_and_uncertainty", structured.confidence || structured.uncertainty || undefined);
  set("stop_reasons", structured.stop_reasons || session?.status || undefined);
  set("timing", {
    started_at: meta.started_at || session?.started_at || null,
    completed_at: meta.completed_at || session?.completed_at || null,
    elapsed_ms: meta.elapsed_ms ?? null,
  });
  set("provider_cost", spend === undefined ? undefined : spend);
  set("raw_response_hashes", meta.raw_response_hashes || undefined);

  // Final output text is observable even when intermediate steps are UNAVAILABLE
  if (output != null) {
    env.artifacts.passages_or_snippets_used =
      env.artifacts.passages_or_snippets_used.status === OBSERVABLE_FIELD_STATUS.UNAVAILABLE
        ? {
            status: OBSERVABLE_FIELD_STATUS.PRESENT,
            value: { final_output_present: true, preview: String(output).slice(0, 500) },
            note: "Intermediate passages UNAVAILABLE; final output present",
          }
        : env.artifacts.passages_or_snippets_used;
  }

  return env;
}
