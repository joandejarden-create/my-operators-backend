/**
 * Consume bounded registry/historical follow-up queue through the existing
 * ownership evidence pipeline: search → rank → scrape → adjudicate.
 * Does not invent a new crawler. Never promotes REGISTERED_BUSINESS → property owner.
 */

import { CONTEXT_DEV_CREDIT_COSTS } from "../../context-dev/credit-ledger.js";
import {
  ownershipSearchResults,
  rankOwnershipSourcesDetailed,
  ownershipDocumentPassages,
} from "./ownership-research-planning.js";
import { adjudicateOwnershipDiscovery } from "./ownership-retrieval-adjudication.js";

export const REGISTRY_FOLLOW_UP_EVIDENCE_PIPELINE_VERSION =
  "registry-follow-up-evidence-pipeline-v1";

/** Party roles that must never collapse into each other. */
export const OWNERSHIP_PARTY_ROLE_DISTINCT = Object.freeze({
  REGISTERED_BUSINESS: "REGISTERED_BUSINESS",
  OPERATOR: "OPERATOR",
  SHAREHOLDER: "SHAREHOLDER",
  PROPERTY_OWNER: "PROPERTY_OWNER",
  ECONOMIC_OWNER_OR_SPONSOR: "ECONOMIC_OWNER_OR_SPONSOR",
  RELATIONSHIP_UNRESOLVED: "RELATIONSHIP_UNRESOLVED",
});

/**
 * Assert role vocabulary stays distinct (offline unit helper).
 */
export function assertOwnershipPartyRolesDistinct(claims = []) {
  const violations = [];
  for (const c of claims || []) {
    const rel = String(c.relationship || c.party_role || c.classification || "").toUpperCase();
    const auto = c.auto_ownership === true || c.promotes_to_current_ownership === true;
    if (rel === "REGISTERED_BUSINESS" && auto) {
      violations.push({
        kind: "REGISTERED_BUSINESS_AUTO_OWNERSHIP",
        subject: c.name || c.subject || null,
      });
    }
    if (rel === "OPERATOR" && (rel === "PROPERTY_OWNER" || c.party_role === "PROPERTY_OWNER")) {
      violations.push({ kind: "OPERATOR_COLLAPSED_TO_OWNER", subject: c.name || c.subject });
    }
    if (
      (rel === "SHAREHOLDER" || /QSA|SOCIO|SÓCIO|ADMINISTRADOR/i.test(String(c.person_role || ""))) &&
      c.relationship === "PROPERTY_OWNER" &&
      !c.evidence_grounded
    ) {
      violations.push({ kind: "SHAREHOLDER_AS_OWNER_WITHOUT_EVIDENCE", subject: c.name || c.subject });
    }
    // Dual-labeled as both registry and property owner without adjudication support
    if (
      c.classification === "REGISTERED_BUSINESS" &&
      (c.party_role === "PROPERTY_OWNER" || c.relationship === "OWNS")
    ) {
      violations.push({
        kind: "REGISTRY_AND_OWNER_COLLAPSED",
        subject: c.name || c.subject,
      });
    }
  }
  return { ok: violations.length === 0, violations };
}

/**
 * Map adjudicated claim → staging row without collapsing roles.
 */
export function stageAdjudicatedClaimForHandoff(claim = {}, { registryLead = null } = {}) {
  const rel = String(claim.relationship || "").toUpperCase();
  const role = String(claim.party_role || claim.party_kind || "").toUpperCase();
  let classification = "RELATIONSHIP_UNRESOLVED";
  if (rel === "OPERATES" || role === "OPERATOR") classification = "OPERATOR";
  else if (rel === "BRANDS" || role === "BRAND") classification = "BRAND";
  else if (
    ["OWNS", "OWNED_BY", "SPONSORS"].includes(rel) ||
    ["PROPERTY_OWNER", "ECONOMIC_OWNER", "ECONOMIC_OWNER_OR_SPONSOR"].includes(role)
  ) {
    classification = "PROPERTY_OWNER_CANDIDATE";
  } else if (rel === "ACQUIRED" || role === "HISTORICAL_ACQUIRER") {
    classification = "HISTORICAL_OWNER_CANDIDATE";
  } else if (role === "SHAREHOLDER" || /SHAREHOLDER|QSA/.test(rel)) {
    classification = "SHAREHOLDER";
  }

  return {
    name: claim.subject || claim.name || null,
    relationship: claim.relationship || classification,
    party_role: claim.party_role || claim.party_kind || classification,
    classification,
    currentness: claim.currentness || "UNRESOLVED",
    auto_ownership: false,
    promotes_to_current_ownership: false,
    evidence_level: "DOCUMENT_CANDIDATE_FOLLOW_UP",
    evidence_span: claim.evidence_span || claim.excerpt || null,
    source_url: claim.source_url || null,
    source_date: claim.source_date || claim.event_date || null,
    evidence_grounded: Boolean(claim.evidence_grounded),
    target_hotel_link_validated: Boolean(claim.target_hotel_link_validated),
    registry_lead_preserved: registryLead
      ? {
          relationship: "REGISTERED_BUSINESS",
          legal_name: registryLead.legal_name || registryLead.subject || null,
          cnpj: registryLead.cnpj || null,
          denue_clee: registryLead.denue_clee || null,
          note: "Registry identity remains REGISTERED_BUSINESS regardless of follow-up claims",
        }
      : null,
    uncertainty:
      classification === "PROPERTY_OWNER_CANDIDATE"
        ? "CANDIDATE_REQUIRES_CURRENTNESS_AND_HOTEL_LINK"
        : "NOT_AUTO_PROPERTY_OWNER",
  };
}

/**
 * Run bounded follow-up queue through search → select → fetch → adjudicate.
 */
export async function consumeBoundedFollowUpEvidencePipeline({
  hotel,
  queue = [],
  registryLead = null,
  searchFn,
  scrapeFn,
  budgetController,
  ledger,
  maxQueries = 4,
  maxDocsPerQuery = 2,
  calls = [],
  sources = [],
  visitedUrls = new Set(),
} = {}) {
  const status = {
    version: REGISTRY_FOLLOW_UP_EVIDENCE_PIPELINE_VERSION,
    queued: (queue || []).length,
    consumed: 0,
    searches_executed: 0,
    docs_fetched: 0,
    passages: [],
    claims_staged: [],
    executed_queries: [],
    earliest_unresolved_step: null,
    business_to_owner: {
      status: "UNRESOLVED",
      relationship: null,
      subject: null,
      note: "REGISTRY_BUSINESS_NOT_PROPERTY_OWNER_UNTIL_ADJUDICATED",
    },
    spend_credits_estimate: 0,
    note: null,
  };

  if (!queue?.length) {
    status.earliest_unresolved_step = "NO_FOLLOW_UP_QUEUE";
    return status;
  }
  if (typeof searchFn !== "function") {
    status.earliest_unresolved_step = "SEARCH_FN_MISSING";
    status.note = "Follow-up queue present but search not available";
    return status;
  }

  const docsForAdjudication = [];
  const maxQ = Math.min(maxQueries, queue.length);

  for (let i = 0; i < maxQ; i += 1) {
    const goal = queue[i];
    const searchCost = ledger?.estimateSearchCost?.(8) || CONTEXT_DEV_CREDIT_COSTS.search_per_10_results || 1;
    let reserve = null;
    if (budgetController) {
      reserve = budgetController.tryReserve(searchCost, {
        method: "bounded_follow_up_queue_search",
        query: goal.query,
      });
      if (!reserve.ok) {
        status.earliest_unresolved_step =
          status.earliest_unresolved_step || "FOLLOW_UP_SEARCH_BUDGET_DENIED";
        status.executed_queries.push({
          query: goal.query,
          researchGoal: goal.researchGoal,
          ok: false,
          blocked: "BUDGET",
        });
        break;
      }
    } else if (ledger && !ledger.canAfford(searchCost)) {
      status.earliest_unresolved_step =
        status.earliest_unresolved_step || "FOLLOW_UP_SEARCH_BUDGET_DENIED";
      break;
    }

    let search;
    try {
      search = await searchFn({ query: goal.query, numResults: 8 });
      if (reserve) budgetController.settle(reserve.token, "search", { method: "bounded_follow_up_queue" });
      else if (ledger) ledger.charge?.("search", searchCost, { query: goal.query });
    } catch (err) {
      if (reserve) budgetController.release(reserve.token);
      status.earliest_unresolved_step =
        status.earliest_unresolved_step || "FOLLOW_UP_SEARCH_TRANSPORT_ERROR";
      status.executed_queries.push({
        query: goal.query,
        ok: false,
        error: String(err?.message || err),
      });
      break;
    }

    status.consumed += 1;
    status.searches_executed += 1;
    status.spend_credits_estimate += searchCost;
    const mapped = search?.ok !== false ? ownershipSearchResults(search.data ?? search) : null;
    calls.push({
      provider: "context_dev",
      kind: "bounded_follow_up_queue_search",
      query: goal.query,
      ok: Boolean(search?.ok !== false && mapped),
      result_count: mapped?.length ?? 0,
    });

    if (!mapped) {
      status.earliest_unresolved_step =
        status.earliest_unresolved_step || "FOLLOW_UP_SEARCH_SHAPE_OR_EMPTY";
      status.executed_queries.push({
        query: goal.query,
        researchGoal: goal.researchGoal,
        ok: false,
        result_count: 0,
        step_reached: "SEARCH",
      });
      continue;
    }

    sources.push({
      kind: "registry_follow_up_search",
      query: goal.query,
      researchGoal: goal.researchGoal,
      results: mapped.slice(0, 8),
    });

    const rankDetail = rankOwnershipSourcesDetailed(mapped, hotel);
    const ranked = (rankDetail.ranked || []).filter((r) => r.url && !visitedUrls.has(r.url));
    const selected = ranked.slice(0, maxDocsPerQuery);

    const execRow = {
      query: goal.query,
      researchGoal: goal.researchGoal,
      ok: true,
      result_count: mapped.length,
      ranked_count: ranked.length,
      selected_urls: selected.map((s) => s.url),
      rejected_rank_count: (rankDetail.rejected || []).length,
      step_reached: selected.length ? "SOURCE_SELECTION" : "SEARCH_NO_RANKED_SOURCE",
      docs: [],
    };

    if (!selected.length) {
      status.earliest_unresolved_step =
        status.earliest_unresolved_step || "FOLLOW_UP_NO_RANKED_SOURCE";
      status.executed_queries.push(execRow);
      continue;
    }

    if (typeof scrapeFn !== "function") {
      status.earliest_unresolved_step =
        status.earliest_unresolved_step || "SCRAPE_FN_MISSING_AFTER_SEARCH";
      execRow.step_reached = "SEARCH_ONLY_SCRAPE_UNAVAILABLE";
      status.executed_queries.push(execRow);
      continue;
    }

    for (const u of selected) {
      const scrapeCost = CONTEXT_DEV_CREDIT_COSTS.scrape_markdown;
      let scrapeReserve = null;
      if (budgetController) {
        scrapeReserve = budgetController.tryReserve(scrapeCost, {
          method: "bounded_follow_up_queue_scrape",
          url: u.url,
        });
        if (!scrapeReserve.ok) {
          status.earliest_unresolved_step =
            status.earliest_unresolved_step || "FOLLOW_UP_SCRAPE_BUDGET_DENIED";
          break;
        }
      } else if (ledger && !ledger.canAfford(scrapeCost)) {
        status.earliest_unresolved_step =
          status.earliest_unresolved_step || "FOLLOW_UP_SCRAPE_BUDGET_DENIED";
        break;
      }

      visitedUrls.add(u.url);
      let scraped;
      try {
        scraped = await scrapeFn({ url: u.url });
        if (scrapeReserve) {
          budgetController.settle(scrapeReserve.token, "scrape_markdown", {
            method: "bounded_follow_up_queue",
          });
        } else if (ledger) {
          ledger.charge?.("scrape_markdown", scrapeCost, { url: u.url });
        }
      } catch (err) {
        if (scrapeReserve) budgetController.release(scrapeReserve.token);
        execRow.docs.push({ url: u.url, ok: false, error: String(err?.message || err) });
        status.earliest_unresolved_step =
          status.earliest_unresolved_step || "FOLLOW_UP_SCRAPE_TRANSPORT_ERROR";
        continue;
      }

      status.docs_fetched += 1;
      status.spend_credits_estimate += scrapeCost;
      const md =
        typeof scraped?.data === "string"
          ? scraped.data
          : scraped?.data?.markdown || scraped?.data?.content || scraped?.markdown || "";
      const ok = scraped?.ok !== false && Boolean(md);
      calls.push({
        provider: "context_dev",
        kind: "bounded_follow_up_queue_scrape",
        url: u.url,
        ok,
        characters: String(md || "").length,
      });
      execRow.docs.push({
        url: u.url,
        ok,
        characters: String(md || "").length,
        title: u.title || null,
      });
      execRow.step_reached = ok ? "DOCUMENT_FETCH" : "DOCUMENT_UNREADABLE";

      if (!ok) {
        status.earliest_unresolved_step =
          status.earliest_unresolved_step || "FOLLOW_UP_DOCUMENT_UNREADABLE";
        continue;
      }

      const pack = ownershipDocumentPassages(md, hotel);
      for (const p of pack.passages || []) {
        status.passages.push({
          url: u.url,
          excerpt: String(p.excerpt || "").slice(0, 400),
          query: goal.query,
        });
      }
      sources.push({
        kind: "registry_follow_up_scrape",
        url: u.url,
        query: goal.query,
        ...pack,
        observed_at: new Date().toISOString(),
      });
      docsForAdjudication.push({
        ok: true,
        url: u.url,
        markdown: md,
        characters: String(md).length,
        passage: (pack.passages || [])[0]?.excerpt || null,
      });
      execRow.step_reached = "DOCUMENT_READ";
    }

    status.executed_queries.push(execRow);
  }

  if (docsForAdjudication.length) {
    const adj = adjudicateOwnershipDiscovery(
      {
        provider: "context_dev_registry_follow_up",
        documents: docsForAdjudication,
        raw_result_count: status.executed_queries.reduce((a, q) => a + (q.result_count || 0), 0),
        ranked: docsForAdjudication.map((d) => ({ url: d.url })),
      },
      hotel
    );
    status.adjudication = {
      version: adj.version,
      claim_counts: adj.score?.claim_counts || null,
      supported_current_owner_evidence: adj.score?.supported_current_owner_evidence || false,
      historical_owner_evidence: adj.score?.historical_owner_evidence || false,
      ambiguous_relationship: adj.score?.ambiguous_relationship || false,
      earliest_failure_stage: adj.earliest_failure_stage || null,
      enrichment_authorized: false,
    };
    for (const c of adj.claims || []) {
      status.claims_staged.push(stageAdjudicatedClaimForHandoff(c, { registryLead }));
    }
    for (const p of adj.passages || []) {
      if (!status.passages.some((x) => x.url === p.url && x.excerpt === p.excerpt)) {
        status.passages.push(p);
      }
    }

    const supported = adj.score?.supported_current_owner_claims?.[0];
    if (supported) {
      status.business_to_owner = {
        status: "CANDIDATE_ESTABLISHED",
        relationship: supported.relationship,
        subject: supported.subject,
        evidence_span: supported.evidence_span,
        source_url: supported.source_url,
        note: "Candidate only — not canonical ownership; registry business remains distinct",
      };
    } else if (adj.score?.historical_owner_evidence) {
      status.business_to_owner = {
        status: "HISTORICAL_ONLY",
        relationship: adj.score.historical_claims?.[0]?.relationship || null,
        subject: adj.score.historical_claims?.[0]?.subject || null,
        note: "Historical claim — not current property owner",
      };
      status.earliest_unresolved_step =
        status.earliest_unresolved_step || adj.earliest_failure_stage || "CURRENTNESS_UNRESOLVED";
    } else {
      status.business_to_owner = {
        status: "UNRESOLVED",
        relationship: null,
        subject: null,
        note: "No supported current owner claim after follow-up adjudication",
      };
      status.earliest_unresolved_step =
        status.earliest_unresolved_step ||
        adj.earliest_failure_stage ||
        "OWNERSHIP_RELATIONSHIP_UNRESOLVED";
    }
  } else if (status.searches_executed && !status.docs_fetched) {
    status.earliest_unresolved_step =
      status.earliest_unresolved_step || "STOPPED_AT_SEARCH_OR_SOURCE_SELECTION";
  }

  const roleCheck = assertOwnershipPartyRolesDistinct([
    ...(registryLead
      ? [
          {
            name: registryLead.legal_name,
            relationship: "REGISTERED_BUSINESS",
            classification: "REGISTERED_BUSINESS",
            auto_ownership: false,
          },
        ]
      : []),
    ...status.claims_staged,
  ]);
  status.role_distinction = roleCheck;

  return status;
}
