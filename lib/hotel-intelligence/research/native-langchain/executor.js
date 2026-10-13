/**
 * Packet 2.8C-2 — LangChain-backed native L3/L4 executor (hardened).
 * Gold/cohort answers are NEVER passed into prompts.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { ChatOpenAI } from "@langchain/openai";
import { HumanMessage, SystemMessage } from "@langchain/core/messages";
import { buildResearchInvestigationSpec } from "../investigation-spec.js";
import { getTemplate } from "../templates.js";
import {
  resolveJurisdictionProfile,
  buildJurisdictionAwareQueries,
} from "./jurisdiction-profiles.js";
import { createNativeCostLedger, recordModelCost, summarizeLedger } from "./cost-ledger.js";
import { createNativeResearchTools } from "./tools.js";
import { gateNativeResearchOutput } from "./quality-gate.js";
import { getOrganizationEvidenceCorpus } from "../../ownership/owner-control/organization-evidence-corpus.js";
import {
  createResearchCache,
  buildSourcePlanBeforeSearch,
  rankSourceCandidates,
  planEvidenceUpgrade,
  compileHotelAliases,
  exactPropertyMatch,
  bindClaimToEvidence,
  assessEvidenceSufficiency,
  prioritizeDocumentText,
  extractPortfolioTableRows,
  buildMxFilingDiscoveryQueries,
  buildPersonSearchQueries,
  classifyPersonProfileResult,
  resolveSearchBudget,
  classifyQueryOutcome,
  summarizeQueryEconomy,
  dedupeQueries,
  methodsSafeForLivePrompt,
} from "./hardening/index.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../../../");
const TRACE_DIR = path.join(ROOT, "data/hotel-intelligence/native-langchain/traces");

const DOMAIN_TO_TEMPLATE = Object.freeze({
  BRAND_HISTORY: "BRAND_HISTORY_GAP",
  BRAND: "BRAND_HISTORY_GAP",
  DEVELOPMENT: "DEVELOPMENT_GAP",
  TRANSACTIONS: "TRANSACTION_GAP",
  PEOPLE: "PERSON_PROFILE_GAP",
  OWNERSHIP: "OWNERSHIP_GAP",
  PROPCO: "OWNERSHIP_GAP",
  OPERATOR: "OPERATOR_GAP",
  PORTFOLIO: "PORTFOLIO_GAP",
  PROPERTY_FUNDAMENTALS: "PROPERTY_FUNDAMENTALS_GAP",
  MARKET: "PROPERTY_FUNDAMENTALS_GAP",
});

function ensureTraceDir() {
  fs.mkdirSync(TRACE_DIR, { recursive: true });
}

function safeJsonParse(text) {
  const raw = String(text || "").trim();
  const fence = raw.match(/```(?:json)?\s*([\s\S]*?)```/);
  const body = fence ? fence[1] : raw;
  try {
    return JSON.parse(body);
  } catch {
    const start = body.indexOf("{");
    const end = body.lastIndexOf("}");
    if (start >= 0 && end > start) {
      try {
        return JSON.parse(body.slice(start, end + 1));
      } catch {
        return null;
      }
    }
    return null;
  }
}

/**
 * Execute one domain L3/L4 native task (blind — no gold answers).
 */
export async function executeNativeLangchainTask(input = {}) {
  const started = Date.now();
  const ledger = createNativeCostLedger({ packet: input.packet || "2.8C-2" });
  const cache = input.cache || createResearchCache();
  const hotel_id = String(input.hotel_id || "").trim();
  const hotel_name = String(input.hotel_name || "").trim();
  const domain = String(input.domain || "OWNERSHIP").toUpperCase();
  const templateId = input.template_id || DOMAIN_TO_TEMPLATE[domain] || "OWNERSHIP_GAP";
  const template = getTemplate(templateId) || getTemplate("OWNERSHIP_GAP");
  const country = input.country || input.jurisdiction || "Unknown";
  const profile = resolveJurisdictionProfile(country);
  const level = input.level === 4 || input.level === "L4" ? 4 : 3;
  const budget = resolveSearchBudget({ level, domain, jurisdiction: profile.country_code });

  const known_facts = Array.isArray(input.known_facts) ? [...input.known_facts] : [];
  const unresolved_questions = Array.isArray(input.unresolved_questions)
    ? input.unresolved_questions
    : [`Resolve ${domain} for ${hotel_name}`];

  const aliases = compileHotelAliases({
    hotel_name,
    former_names: input.former_names || [],
    former_brands: input.former_brands || [],
    alt_names: input.alt_names || [],
    legal_entity_candidates: [input.known_propco, input.known_owner].filter(Boolean),
    city: input.city || null,
    address: input.address || null,
  });

  const sourcePlan = buildSourcePlanBeforeSearch({
    profile,
    domain,
    hotel_name,
    company: input.known_owner || hotel_name,
    aliases: aliases.all_aliases,
  });

  const methodHints = methodsSafeForLivePrompt(profile.country_code, domain);

  const spec = buildResearchInvestigationSpec({
    template,
    template_id: template.template_id,
    hotel_seed: {
      hotel_id,
      hotel_name,
      country: profile.country,
      current_brand: input.known_brand || null,
      operator: input.known_operator || null,
      parent_company: input.known_owner || null,
    },
    known_facts,
    unresolved_questions,
    negative_screens: template.negative_screens || [],
  });

  const task_id = `ntl_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
  const trace = {
    task_id,
    packet: "2.8C-2",
    hotel_id,
    hotel_name,
    domain,
    template_id: template.template_id,
    template_version: template.version,
    jurisdiction: profile.country_code,
    difficulty: level === 4 ? "L4" : "L3",
    source_plan: sourcePlan,
    method_hints_used: methodHints,
    gold_hidden: true,
    queries_attempted: [],
    query_economy: [],
    tools_invoked: [],
    urls_discovered: [],
    ranked_sources: [],
    documents_retrieved: [],
    documents_parsed: [],
    document_sections: [],
    table_extractions: [],
    sources_rejected: [],
    sources_accepted: [],
    claims_extracted: [],
    claim_bindings: [],
    entities_extracted: [],
    relationships_extracted: [],
    events_extracted: [],
    contradictions: [],
    upgrade_plans: [],
    person_verifications: [],
    stop_reason: null,
    quality_scores: {},
    latency_ms: 0,
    native_cost: null,
    org_corpus: null,
    cache_stats: null,
    search_budget: budget,
    improvement_classes: ["SOURCE_RANKING", "SEARCH_CACHE", "CLAIM_SOURCE_BINDING"],
    webhound_runs: 0,
  };

  const ownerId = input.owner_entity_id || null;
  if (ownerId) {
    const corpus = getOrganizationEvidenceCorpus(ownerId);
    trace.org_corpus = { status: corpus.status, version: corpus.version };
    if (corpus.status === "AVAILABLE") {
      trace.improvement_classes.push("ORGANIZATION_REUSE");
      known_facts.push(`OrganizationEvidenceCorpus available for ${ownerId}`);
    }
  }

  const { webSearch, fetchUrl } = createNativeResearchTools({
    ledger,
    maxResults: level === 4 ? 6 : 4,
  });

  let queries = buildJurisdictionAwareQueries({
    domain,
    hotel_name,
    company: input.known_owner || hotel_name,
    propco: input.known_propco || "",
    profile,
  });

  if (profile.country_code === "MX" && /OWNER|PROPCO|TRANSACT|PORTFOLIO/i.test(domain)) {
    const mx = buildMxFilingDiscoveryQueries({
      hotel_name,
      company: input.known_owner || hotel_name,
      propco: input.known_propco || "",
      aliases: aliases.all_aliases,
    });
    queries = [...mx.queries, ...queries];
    trace.improvement_classes.push("MX_FILING_DISCOVERY");
  }

  if (input.upgrade_from_claim) {
    const plan = planEvidenceUpgrade(input.upgrade_from_claim, {
      hotel_name,
      company: input.known_owner,
      propco: input.known_propco,
      brand: input.known_brand,
      former_brand: input.former_brands?.[0],
      operator: input.known_operator,
      domain,
    });
    trace.upgrade_plans.push(plan);
    queries = [...plan.focused_queries, ...queries];
    trace.improvement_classes.push("EVIDENCE_UPGRADE_PLANNER", "DECISIVE_SOURCE_FOLLOWUP");
  }

  if (domain === "PEOPLE") {
    const personQs = buildPersonSearchQueries({
      person: input.person_name || input.known_owner,
      organization: input.known_owner || input.known_operator,
      hotel_name,
    });
    queries = [...personQs, ...queries];
    trace.improvement_classes.push("PERSON_VERIFICATION");
  }

  let deduped = dedupeQueries(queries);
  queries = deduped.queries.slice(0, budget.max_queries);
  for (const q of deduped.duplicative) {
    trace.query_economy.push({ query: q, class: "DUPLICATIVE", reason: "near_duplicate" });
  }
  if (deduped.duplicative.length) trace.improvement_classes.push("QUERY_DUPLICATION_REDUCED");

  let model = null;
  if (process.env.OPENAI_API_KEY) {
    model = new ChatOpenAI({
      model: process.env.HI_NATIVE_LLM_MODEL || "gpt-4o-mini",
      temperature: 0,
    });
    if (!input.upgrade_from_claim) {
      try {
        const expand = await model.invoke([
          new SystemMessage(
            "You expand hotel research search queries. Return JSON {queries:[string]}. " +
              "Prefer decisive source families from the source plan. Use jurisdiction language when helpful. " +
              "Do NOT invent factual answers. Do NOT use private memory as evidence. Max 2 queries."
          ),
          new HumanMessage(
            JSON.stringify({
              hotel_name,
              domain,
              jurisdiction: profile.country,
              source_plan: sourcePlan,
              method_hints: methodHints,
              seed_queries: queries,
              unresolved_questions,
              known_facts,
            })
          ),
        ]);
        const usage = expand.usage_metadata || expand.response_metadata?.tokenUsage || {};
        recordModelCost(ledger, {
          input_tokens: usage.input_tokens || usage.promptTokens || 400,
          output_tokens: usage.output_tokens || usage.completionTokens || 120,
          model: process.env.HI_NATIVE_LLM_MODEL || "gpt-4o-mini",
          note: "query_expansion",
        });
        const parsed = safeJsonParse(expand.content);
        if (parsed?.queries?.length) {
          for (const q of parsed.queries.slice(0, 2)) {
            if (q && !queries.includes(q)) queries.push(String(q));
          }
          trace.improvement_classes.push("QUERY_STRATEGY");
        }
      } catch (err) {
        trace.tools_invoked.push({ tool: "llm_query_expand", error: String(err?.message || err) });
      }
    }
  }

  deduped = dedupeQueries(queries);
  queries = deduped.queries.slice(0, budget.max_queries);

  const snippets = [];
  const queryResults = [];
  for (const q of queries) {
    trace.queries_attempted.push(q);
    try {
      let parsed = cache.getQuery(q);
      const cachedHit = Boolean(parsed);
      if (!parsed) {
        const raw = await webSearch.invoke({ query: q, num: level === 4 ? 5 : 4 });
        parsed = typeof raw === "string" ? JSON.parse(raw) : raw;
        if (parsed?.ok) cache.setQuery(q, parsed);
      } else {
        trace.improvement_classes.push("SEARCH_CACHE_HIT");
      }
      trace.tools_invoked.push({ tool: "web_search", query: q, cached: cachedHit });
      queryResults.push({ query: q, results: parsed.results || [] });
      for (const r of parsed.results || []) {
        if (r.url) {
          trace.urls_discovered.push(r.url);
          snippets.push({ query: q, ...r });
        }
      }
      trace.improvement_classes.push("SOURCE_DISCOVERY");
    } catch (err) {
      trace.tools_invoked.push({ tool: "web_search", query: q, error: String(err?.message || err) });
      queryResults.push({ query: q, results: [] });
    }
  }

  const ranked = rankSourceCandidates(snippets, {
    domain,
    hotel_name,
    aliases: aliases.all_aliases,
  });
  trace.ranked_sources = ranked.slice(0, 15).map((r) => ({
    url: r.url,
    title: r.title,
    source_family: r.source_family,
    document_value: r.document_value,
    rank_score: r.rank_score,
  }));

  const highValue = ranked.filter((r) => r.document_value === "HIGH_VALUE_DOCUMENT");
  const supporting = ranked.filter((r) => r.document_value === "SUPPORTING_DOCUMENT");
  const rest = ranked.filter((r) => !["HIGH_VALUE_DOCUMENT", "SUPPORTING_DOCUMENT"].includes(r.document_value));
  const toFetch = [...highValue, ...supporting, ...rest]
    .map((r) => r.url)
    .filter(Boolean)
    .filter((u, i, arr) => arr.indexOf(u) === i)
    .slice(0, budget.max_fetches);
  if (highValue.length) trace.improvement_classes.push("HIGH_VALUE_DOCUMENT_PRIORITIZATION");

  const documents = [];
  for (const url of toFetch) {
    try {
      let parsed = cache.getUrl(url);
      const cachedHit = Boolean(parsed);
      if (!parsed) {
        const raw = await fetchUrl.invoke({ url, max_chars: level === 4 ? 14000 : 7000 });
        parsed = typeof raw === "string" ? JSON.parse(raw) : raw;
        if (parsed?.ok) cache.setUrl(url, parsed);
      } else {
        trace.improvement_classes.push("DOC_CACHE_HIT");
      }
      trace.tools_invoked.push({ tool: "fetch_url", url, cached: cachedHit });
      trace.documents_retrieved.push(url);
      if (parsed.ok && parsed.text) {
        const rankedMeta = ranked.find((r) => r.url === url) || {};
        const property_match = exactPropertyMatch({
          text: parsed.text,
          aliases: aliases.all_aliases,
          city: input.city,
          rooms: input.rooms,
        });
        trace.improvement_classes.push("EXACT_PROPERTY_MATCH");
        const prioritized = prioritizeDocumentText(parsed.text, {
          domain,
          maxChars: level === 4 ? 7000 : 4500,
        });
        if (prioritized.strategy === "section_targeting") {
          trace.improvement_classes.push("DOCUMENT_SECTION_TARGETING");
        }
        const tables = extractPortfolioTableRows(parsed.text, { hotel_name });
        if (tables.row_count) {
          trace.table_extractions.push({ url, row_count: tables.row_count });
          trace.improvement_classes.push("TABLE_EXTRACTION");
        }
        trace.document_sections.push({
          url,
          strategy: prioritized.strategy,
          preferred_sections: prioritized.preferred_sections || [],
        });
        documents.push({
          ...parsed,
          text: prioritized.text,
          source_family: rankedMeta.source_family,
          document_value: rankedMeta.document_value,
          property_match,
          table_rows: tables.rows.slice(0, 8),
        });
        trace.documents_parsed.push({
          url,
          is_pdf: Boolean(parsed.is_pdf),
          chars: prioritized.text.length,
          document_value: rankedMeta.document_value,
        });
        trace.improvement_classes.push("DOCUMENT_RETRIEVAL");
        if (parsed.is_pdf) trace.improvement_classes.push("DOCUMENT_EXTRACTION");
      } else {
        trace.sources_rejected.push({ url, reason: parsed.error || "empty_or_blocked" });
      }
    } catch (err) {
      trace.sources_rejected.push({ url, reason: String(err?.message || err) });
    }
  }

  if (domain === "PEOPLE") {
    for (const r of ranked.slice(0, 8)) {
      const v = classifyPersonProfileResult(r, {
        person: input.person_name || "",
        organization: input.known_owner || input.known_operator || "",
      });
      trace.person_verifications.push({ url: r.url, ...v });
    }
  }

  let extracted = { claims: [], leads: [], entities: [], relationships: [], events: [] };
  if (model && (documents.length || snippets.length)) {
    const evidencePack = {
      documents: documents.map((d) => ({
        url: d.url,
        is_pdf: d.is_pdf,
        source_family: d.source_family,
        document_value: d.document_value,
        property_match: d.property_match,
        table_rows: d.table_rows,
        text: String(d.text || "").slice(0, 5500),
      })),
      search_snippets: snippets.slice(0, 10),
      aliases: aliases.all_aliases.slice(0, 12),
    };
    try {
      const extractMsg = await model.invoke([
        new SystemMessage(
          [
            "You are Dealality's native hotel intelligence extractor (packet 2.8C-2).",
            "ONLY extract claims supported by the provided evidence pack.",
            "If evidence is insufficient, return leads not claims.",
            "Never use model world knowledge as a factual claim.",
            "Bind each claim to exact evidence: include evidence_fragment quoted from the pack.",
            "Reject adjacent/similar properties that fail alias/location match.",
            "Operator is not owner. Announced is not current. Historical is not current.",
            "Temporal status: CURRENT, HISTORICAL, FORMER, ANNOUNCED, PENDING, SUPERSEDED, UNKNOWN.",
            "Return JSON:",
            '{ "claims":[{ "claim_id":"", "claim_text":"", "relationship_type":"", "entities":[], "temporal_status":"", "source_urls":[], "evidence_fragment":"", "from_model_memory_only":false, "confidence":"PROBABLE" }], "leads":[{ "lead_text":"", "suggested_query":"", "suggested_decisive_source":"" }], "entities":[], "relationships":[], "events":[] }',
          ].join(" ")
        ),
        new HumanMessage(
          JSON.stringify({
            hotel_id,
            hotel_name,
            domain,
            unresolved_questions,
            known_facts,
            method_hints: methodHints,
            evidence_pack: evidencePack,
          })
        ),
      ]);
      const usage = extractMsg.usage_metadata || {};
      recordModelCost(ledger, {
        input_tokens: usage.input_tokens || 2200,
        output_tokens: usage.output_tokens || 600,
        model: process.env.HI_NATIVE_LLM_MODEL || "gpt-4o-mini",
        note: "grounded_extraction",
      });
      extracted = safeJsonParse(extractMsg.content) || extracted;
      trace.improvement_classes.push("DOCUMENT_EXTRACTION", "ALIAS_AWARE_DOCUMENT_SEARCH");
    } catch (err) {
      trace.tools_invoked.push({ tool: "llm_extract", error: String(err?.message || err) });
      trace.stop_reason = "extraction_error";
    }
  } else if (!model) {
    trace.stop_reason = "OPENAI_API_KEY_missing_search_only";
  } else {
    trace.stop_reason = "no_documents_or_snippets";
  }

  if (domain === "PEOPLE" && Array.isArray(extracted.claims)) {
    extracted.claims = extracted.claims.map((c) => {
      const url = (c.source_urls || [])[0];
      const v = trace.person_verifications.find((p) => p.url === url);
      if (v && v.status !== "VERIFIED" && /linkedin/i.test(url || "")) {
        return { ...c, person_verification: v.status, _force_lead: v.status === "REJECTED" };
      }
      return { ...c, person_verification: v?.status || "UNKNOWN" };
    });
    const forced = extracted.claims.filter((c) => c._force_lead);
    extracted.claims = extracted.claims.filter((c) => !c._force_lead);
    extracted.leads = [
      ...(extracted.leads || []),
      ...forced.map((c) => ({
        lead_text: c.claim_text,
        reason: "person_profile_not_verified",
      })),
    ];
  }

  const gated = gateNativeResearchOutput(extracted, { hotel_name });

  const bindings = [];
  for (const claim of gated.accepted_claims) {
    const doc =
      documents.find((d) => (claim.source_urls || []).includes(d.url)) || documents[0] || {};
    const binding = bindClaimToEvidence(
      { ...claim, domain },
      {
        url: (claim.source_urls || [])[0],
        text: doc.text,
        fragment: claim.evidence_fragment,
        source_family: doc.source_family,
        document_value: doc.document_value,
        property_match: doc.property_match,
      }
    );
    const sufficiency = assessEvidenceSufficiency(claim, binding, {
      source_family: doc.source_family,
    });
    bindings.push({ claim_id: claim.claim_id, binding, sufficiency });
    claim.evidence_binding = binding;
    claim.evidence_sufficiency = sufficiency;
    claim.exact_or_equivalent_capable = sufficiency.exact_or_equivalent_capable;
  }
  trace.claim_bindings = bindings;

  const acceptedUrls = [...new Set(gated.accepted_claims.flatMap((c) => c.source_urls || []))];
  for (const qr of queryResults) {
    trace.query_economy.push(
      classifyQueryOutcome({
        query: qr.query,
        results: qr.results,
        accepted_claim_urls: acceptedUrls,
        fetched_urls: trace.documents_retrieved,
      })
    );
  }
  trace.query_economy_summary = summarizeQueryEconomy(trace.query_economy);

  trace.claims_extracted = gated.accepted_claims;
  trace.contradictions = gated.contradictions;
  trace.sources_accepted = acceptedUrls;
  trace.entities_extracted = extracted.entities || [];
  trace.relationships_extracted = extracted.relationships || [];
  trace.events_extracted = extracted.events || [];
  trace.quality_scores = {
    accepted: gated.accepted_claims.length,
    rejected: gated.rejected_claims.length,
    leads: gated.search_leads.length,
    false_confident_critical: gated.false_confident_critical,
    exact_capable_claims: bindings.filter((b) => b.sufficiency.exact_or_equivalent_capable === "YES")
      .length,
  };
  if (!trace.stop_reason) {
    trace.stop_reason =
      gated.accepted_claims.length > 0
        ? "evidence_threshold_or_complete"
        : gated.search_leads.length
          ? "leads_only_insufficient_evidence"
          : "no_material_claims";
  }

  trace.latency_ms = Date.now() - started;
  trace.native_cost = summarizeLedger(ledger);
  trace.cache_stats = cache.summarize();
  trace.improvement_classes = [...new Set(trace.improvement_classes)];
  trace.spec_id = spec.investigation_id;
  trace.provider = "LANGCHAIN_NATIVE_L3L4";
  trace.legacy_cohort_blocked = true;

  ensureTraceDir();
  const tracePath = path.join(TRACE_DIR, `${task_id}.json`);
  fs.writeFileSync(tracePath, JSON.stringify(trace, null, 2));

  return {
    ok: true,
    task_id,
    trace_path: tracePath,
    trace,
    gated,
    ledger: summarizeLedger(ledger),
    cache: cache.summarize(),
    observations: gated.accepted_claims.map((c) => ({
      hotel_id,
      domain,
      ...c,
      provider: "LANGCHAIN_NATIVE",
      packet: "2.8C-2",
      task_id,
    })),
  };
}

export { DOMAIN_TO_TEMPLATE, TRACE_DIR };
