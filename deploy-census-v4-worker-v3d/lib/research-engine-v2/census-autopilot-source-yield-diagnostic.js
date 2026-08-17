/**
 * Census Autopilot source-yield diagnostics + apply recommendation.
 * Does not loosen confidence rules. Controlled mode never writes Airtable.
 */

export const YIELD_DIAGNOSTIC_VERSION = "census-autopilot-source-yield-diagnostic-v1";

export const NO_PROPOSAL_REASON_CODES = Object.freeze({
  A: "already_populated",
  B: "official_source_url_missing",
  C: "official_page_fetch_blocked",
  D: "page_fetched_data_not_present",
  E: "extractor_too_narrow",
  F: "confidence_rule_high_only",
  G: "field_already_populated",
  H: "schema_missing",
  I: "provider_decision_missing",
  J: "identity_name_issue",
  K: "brand_census_match_issue",
  L: "page_template_unsupported",
  M: "mixed_use_ambiguity",
  N: "low_value_field_only",
  O: "fetch_budget_deferred",
  P: "family_fetch_circuit_open",
});

/**
 * Map free-text / queue block reasons onto founder taxonomy A–P.
 * @param {string} reason
 */
export function classifyNoProposalReason(reason) {
  const r = String(reason || "").toLowerCase();
  if (!r) return { code: "E", label: NO_PROPOSAL_REASON_CODES.E };
  if (/already_enriched|already_populated|name_appears_valid/.test(r))
    return { code: "A", label: NO_PROPOSAL_REASON_CODES.A };
  if (/missing_source|generic_directory/.test(r))
    return { code: "B", label: NO_PROPOSAL_REASON_CODES.B };
  if (/official_page_blocked|family_fetch_circuit|403|access denied/.test(r))
    return { code: "C", label: NO_PROPOSAL_REASON_CODES.C };
  if (/source_quality|no_extractable|no_room_count|data_not_present/.test(r))
    return { code: "D", label: NO_PROPOSAL_REASON_CODES.D };
  if (/confidence|medium|low_confidence|not_high/.test(r))
    return { code: "F", label: NO_PROPOSAL_REASON_CODES.F };
  if (/schema|v1\.1\.4|v114/.test(r)) return { code: "H", label: NO_PROPOSAL_REASON_CODES.H };
  if (/provider|geocode|storage_terms/.test(r))
    return { code: "I", label: NO_PROPOSAL_REASON_CODES.I };
  if (/malformed|property_name|identity/.test(r))
    return { code: "J", label: NO_PROPOSAL_REASON_CODES.J };
  if (/not_in_active|brand_unconfirmed|uncertain_brand|match/.test(r))
    return { code: "K", label: NO_PROPOSAL_REASON_CODES.K };
  if (/mixed_use|units_ambiguity|residences/.test(r))
    return { code: "M", label: NO_PROPOSAL_REASON_CODES.M };
  if (/fetch_budget_deferred|candidate_needs_fetch/.test(r))
    return { code: "O", label: NO_PROPOSAL_REASON_CODES.O };
  if (/family_fetch_circuit/.test(r)) return { code: "P", label: NO_PROPOSAL_REASON_CODES.P };
  if (/human_review|held/.test(r)) return { code: "K", label: NO_PROPOSAL_REASON_CODES.K };
  return { code: "E", label: NO_PROPOSAL_REASON_CODES.E };
}

/**
 * Recommend whether an approval-bundle apply is worth running.
 * @param {{ high_proposals?: number, safety_ok?: boolean, soft_deferred?: string[] }} input
 */
export function recommendApplyFromYield(input = {}) {
  const high = Number(input.high_proposals || 0);
  const safetyOk = input.safety_ok !== false;
  if (!safetyOk) {
    return {
      recommend_apply: false,
      reason: "safety_checks_failed",
      message: "Do not apply — safety checks failed.",
      bundle_too_small: high < 10,
    };
  }
  if (high < 10) {
    return {
      recommend_apply: false,
      reason: "high_proposals_below_threshold",
      message:
        "High proposals < 10 — improve extractors / source access before apply unless founder wants a smoke-test write.",
      bundle_too_small: true,
      threshold: 10,
      high_proposals: high,
    };
  }
  return {
    recommend_apply: true,
    reason: "high_proposals_meet_threshold",
    message:
      "High proposals ≥ 10 and safety checks pass — recommend approval-bundle-bound apply after founder review.",
    bundle_too_small: false,
    threshold: 10,
    high_proposals: high,
  };
}

/**
 * Build a structured yield diagnostic from orchestration + queue reports.
 * @param {object} opts
 */
export function buildSourceYieldDiagnostic(opts = {}) {
  const orch = opts.orchestration || {};
  const queueResults = orch.queue_results || opts.queue_results || [];
  const blocked = orch.blocked || [];
  const proposals = opts.proposals || orch.proposals || [];

  const reasonCounts = {};
  for (const b of blocked) {
    const mapped = classifyNoProposalReason(b.block_reason || b.blocked_reason || b.action);
    const key = `${mapped.code}:${mapped.label}`;
    reasonCounts[key] = (reasonCounts[key] || 0) + 1;
  }

  const byQueue = {};
  for (const r of queueResults) {
    byQueue[r.queue_id] = {
      status: r.status,
      eligible_scanned: r.eligible_scanned,
      high_proposals: r.high_proposals,
      notes: r.notes || [],
      extractor:
        {
          description_extraction: "production-census-description-extractor",
          amenities_extraction: "description-extractor+lane-2",
          radar_public_readiness: "production-census-population-lane-2",
          address_confirmation: "production-census-address-geocode-resolver",
          property_name_cleanup: "production-census-property-name-cleanup-extractor",
          property_type_asset_context: "production-census-population-lane-2",
          rooms_keys: "production-census-rooms-keys-extractor",
          coordinate_resolution: "geocode-provider (soft-deferred)",
        }[r.queue_id] || "unknown",
    };
  }

  const highByQueue = {};
  for (const p of proposals) {
    const q = p.queue || "unknown";
    highByQueue[q] = (highByQueue[q] || 0) + 1;
  }

  const recommendation = recommendApplyFromYield({
    high_proposals: proposals.length,
    safety_ok: true,
    soft_deferred: orch.queues_soft_deferred || [],
  });

  const highestYieldNext = pickHighestYieldNextImprovement({
    queueResults,
    reasonCounts,
    familyGaps: opts.family_gaps || null,
  });

  return {
    version: YIELD_DIAGNOSTIC_VERSION,
    generated_at: new Date().toISOString(),
    run_id: opts.run_id || null,
    mode: opts.mode || "controlled",
    airtable_writes: false,
    total_high_proposals: proposals.length,
    queues: byQueue,
    high_proposals_by_queue: highByQueue,
    no_proposal_reason_counts: reasonCounts,
    queues_executed: orch.queues_executed || [],
    queues_exhausted: orch.queues_exhausted || [],
    queues_soft_deferred: orch.queues_soft_deferred || [],
    family_gaps: opts.family_gaps || null,
    blocked_source_families: opts.blocked_source_families || [
      "Hilton (hilton.com edge 403 — Page Reference Code)",
      "Marriott (marriott.com 403)",
      "Choice (choicehotels.com 403)",
    ],
    fetchable_families: ["IHG (ihg.com hoteldetail)"],
    highest_yield_next_improvement: highestYieldNext,
    apply_recommendation: recommendation,
    why_yield_was_low: [
      "Prior smoke used tiny description fetch budgets and burned attempts on Hilton 403s",
      "IHG descriptions/amenities already populated for active brands",
      "Hilton/Marriott/Choice corporate pages bot-blocked from Node fetch",
      "Address High VIC claims were dropped when geocode was deferred (fixed: address-only path)",
      "Rooms High exhausted for prior avid writes; many IHG pages have empty numberOfRooms",
    ],
  };
}

function pickHighestYieldNextImprovement({ queueResults, reasonCounts, familyGaps }) {
  const blockedHeavy = Object.entries(reasonCounts || {})
    .filter(([k]) => k.startsWith("C:"))
    .reduce((a, [, n]) => a + n, 0);
  if (blockedHeavy > 50) {
    return {
      id: "corporate_bot_block_bypass_learning",
      summary:
        "Hilton/Marriott/Choice official pages return 403 to Node fetch — need approved public-source path or Webhound learning for edge patterns (not production writes).",
      expected_fields: ["Hotel Description - Source Text", "Amenities - Source Text", "Rooms / Keys"],
    };
  }
  const addr = (queueResults || []).find((q) => q.queue_id === "address_confirmation");
  if (addr && (addr.high_proposals || 0) === 0) {
    return {
      id: "address_only_vic_and_ihg",
      summary:
        "Propose High Address-only from VIC claims + IHG official page JSON-LD without waiting on geocode provider.",
      expected_fields: ["Address", "Address Confidence", "Address Source URL"],
    };
  }
  if (familyGaps?.IHG?.miss_rooms > 20) {
    return {
      id: "ihg_rooms_prose_patterns",
      summary: "IHG hoteldetail often omits numberOfRooms — expand High-only prose patterns when explicit.",
      expected_fields: ["Rooms / Keys"],
    };
  }
  return {
    id: "continue_address_then_ihg_rooms",
    summary: "Continue address-only High writes, then IHG rooms where official counts appear.",
    expected_fields: ["Address", "Rooms / Keys"],
  };
}

/**
 * Build Webhound learning candidates (max 25) — never executed here.
 */
export function buildWebhoundLearningCandidates(opts = {}) {
  const max = Math.min(25, opts.max || 25);
  const fromAdapters = (opts.unresolved_patterns || []).map((p) => ({
    id: p.id,
    family: p.family,
    pattern: p.pattern,
    what_code_needs_to_learn: p.what_code_needs_to_learn,
    sample_urls: p.sample_urls || [],
    count: p.count,
    never_write_from_webhound: true,
    source: "family_directory_adapter_unresolved",
  }));

  const seeded = [
    {
      id: "hilton_edge_403_page_reference_code",
      family: "Hilton",
      pattern: "hilton.com/en/hotels/{ctyhocn}-…/ → HTTP 403 title 'Hilton Page Reference Code'",
      what_code_needs_to_learn:
        "Prefer Hilton locations directory adapters for address/amenities/coords; Webhound only if directory miss repeats.",
      sample_urls: opts.hilton_samples || [],
      never_write_from_webhound: true,
    },
    {
      id: "marriott_com_403",
      family: "Marriott",
      pattern: "marriott.com/.../overview/ → HTTP 403 short body",
      what_code_needs_to_learn:
        "Prefer Marriott HQV GraphQL for coordinates; descriptions need alternate public path.",
      never_write_from_webhound: true,
    },
    {
      id: "choicehotels_com_403",
      family: "Choice",
      pattern: "choicehotels.com/{state}/{city}/{brand}/{code} → HTTP 403",
      what_code_needs_to_learn:
        "Prefer Choice regional JSON-LD/hotel cards for address/amenities/coords; narrative description not on regional cards.",
      never_write_from_webhound: true,
    },
    {
      id: "ihg_empty_numberOfRooms",
      family: "IHG",
      pattern: 'hoteldetail JSON "numberOfRooms" : ""',
      what_code_needs_to_learn:
        "Where IHG publishes authoritative room counts when schema field is empty (without JS false positives).",
      never_write_from_webhound: true,
    },
  ];

  // Prefer repeated unresolved adapter patterns, then seeded learning IDs (dedupe by id)
  const byId = new Map();
  for (const c of [...fromAdapters, ...seeded]) {
    if (!byId.has(c.id)) byId.set(c.id, c);
  }
  const candidates = [...byId.values()].slice(0, max);

  return {
    version: "census-autopilot-webhound-learning-candidates-v1",
    capped_at: max,
    run_webhound: false,
    write_from_webhound: false,
    candidates,
    note: "Candidates only — do not run Webhound in this task; never write Airtable from Webhound. Emit only for repeated unresolved source patterns after directory/HQV adapters.",
  };
}

export function renderSourceYieldDiagnosticMarkdown(d) {
  const queueRows = Object.entries(d.queues || {})
    .map(
      ([id, q]) =>
        `| ${id} | ${q.status} | ${q.eligible_scanned ?? "—"} | ${q.high_proposals ?? 0} | ${q.extractor} |`
    )
    .join("\n");
  const reasonRows = Object.entries(d.no_proposal_reason_counts || {})
    .sort((a, b) => b[1] - a[1])
    .map(([k, n]) => `| ${k} | ${n} |`)
    .join("\n");
  return [
    `# Source Yield Diagnostic`,
    ``,
    `- **Run:** ${d.run_id || "(n/a)"}`,
    `- **High proposals:** ${d.total_high_proposals}`,
    `- **Airtable writes:** false`,
    `- **Apply recommendation:** ${d.apply_recommendation?.recommend_apply ? "YES" : "NO"} — ${d.apply_recommendation?.message || ""}`,
    ``,
    `## Why yield was low`,
    ``,
    ...(d.why_yield_was_low || []).map((x) => `- ${x}`),
    ``,
    `## Queues`,
    ``,
    `| Queue | Status | Eligible | High | Extractor |`,
    `| --- | --- | ---: | ---: | --- |`,
    queueRows || `| — | — | 0 | 0 | — |`,
    ``,
    `## No-proposal reasons (taxonomy)`,
    ``,
    `| Reason | Count |`,
    `| --- | ---: |`,
    reasonRows || `| — | 0 |`,
    ``,
    `## Highest-yield next improvement`,
    ``,
    `- **${d.highest_yield_next_improvement?.id}:** ${d.highest_yield_next_improvement?.summary}`,
    ``,
    `## Blocked source families`,
    ``,
    ...(d.blocked_source_families || []).map((x) => `- ${x}`),
    ``,
  ].join("\n");
}
