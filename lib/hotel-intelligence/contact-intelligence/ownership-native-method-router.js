/**
 * Opt-in native method router for researchHotelOwnershipContactPath.
 * Default OFF (OWNERSHIP_NATIVE_METHOD_ROUTER / budgets.native_method_router).
 *
 * Wires existing reusable adapters only — Cadastur, DENUE, investor-doc,
 * follow-up planning, person-email discovery, owner-cache reuse.
 * No Parallel / Webhound / Surfe / FullEnrich / PDL enrichment execution.
 * No canonical ownership writes.
 */

import { matchHotelToCadastur } from "../../research-engine-v2/brazil-cadastur-match.js";
import { BUSINESS_RELATIONSHIP, matchHotelToDenue } from "../../inegi-denue/match.js";
import {
  buildInvestorSearchQueries,
  rankInvestorDocumentUrls,
  extractAttributedPersonEmailsFromText,
  INVESTOR_DOC_DISCOVERY_VERSION,
} from "./investor-document-discovery.js";
import {
  extractPeopleFromOrgPages,
  researchPersonBusinessEmail,
  PERSON_EMAIL_DISCOVERY_VERSION,
} from "./person-email-discovery.js";
import { buildQuestionDependentFollowUpPlan } from "./ownership-follow-up-research.js";
import { resolveOwnerDomain, isRejectedOwnerDomain } from "./owner-contact-resolution-v2/domain-resolver.js";
import { CONTEXT_DEV_CREDIT_COSTS } from "../../context-dev/credit-ledger.js";
import {
  createDefaultRegistryDatasetDeps,
  REGISTRY_DATASET_STATUS,
  OWNERSHIP_REGISTRY_DATASET_LOADER_VERSION,
} from "./ownership-registry-dataset-loaders.js";

export const OWNERSHIP_NATIVE_METHOD_ROUTER_VERSION = "ownership-native-method-router-v2";

export function isOwnershipNativeMethodRouterEnabled(caseInput = {}, budgets = {}, env = process.env) {
  if (budgets?.native_method_router === true || caseInput?.native_method_router === true) return true;
  if (budgets?.native_method_router === false || caseInput?.native_method_router === false) return false;
  const v = String(env.OWNERSHIP_NATIVE_METHOD_ROUTER || "").trim();
  return v === "1" || /^true$/i.test(v);
}

function countryOf(hotel = {}) {
  return String(hotel.country || hotel.Country || "")
    .trim()
    .toLowerCase();
}

function isBrazil(hotel) {
  const c = countryOf(hotel);
  return c === "brazil" || c === "brasil" || c === "br";
}

function isMexico(hotel) {
  const c = countryOf(hotel);
  return c === "mexico" || c === "méxico" || c === "mx";
}

function skip(method, reason, missing_prerequisite = null, further_research_could_resolve = false, extra = {}) {
  return {
    method,
    status: "SKIPPED",
    reason,
    missing_prerequisite,
    further_research_could_resolve,
    ...extra,
  };
}

function invoked(method, result = {}) {
  return {
    method,
    status: "INVOKED",
    ...result,
  };
}

function datasetUnavailable(method, loaded, extra = {}) {
  return {
    method,
    status: REGISTRY_DATASET_STATUS.DATASET_UNAVAILABLE,
    reason: loaded?.reason || "DATASET_UNAVAILABLE",
    missing_prerequisite: loaded?.missing_prerequisite || null,
    further_research_could_resolve: loaded?.further_research_could_resolve !== false,
    provenance: loaded?.provenance || null,
    ...extra,
  };
}

/**
 * Normalize loader result: array (test inject) or { ok, status, rows, provenance }.
 */
async function resolveRegistryRows(hotel, { injectedRows, fetchFn, defaultFetch, label }) {
  if (Array.isArray(injectedRows)) {
    return {
      ok: true,
      status: REGISTRY_DATASET_STATUS.LOADED,
      rows: injectedRows,
      provenance: { source_mode: "deps_injected_rows", label, local_only: true, network: false },
    };
  }
  const fn = typeof fetchFn === "function" ? fetchFn : defaultFetch;
  if (typeof fn !== "function") {
    return {
      ok: false,
      status: REGISTRY_DATASET_STATUS.DATASET_UNAVAILABLE,
      reason: `${label}_LOADER_MISSING`,
      rows: [],
      provenance: null,
    };
  }
  const loaded = await fn(hotel);
  if (Array.isArray(loaded)) {
    return {
      ok: true,
      status: REGISTRY_DATASET_STATUS.LOADED,
      rows: loaded,
      provenance: { source_mode: "fetch_returned_array", label, local_only: true, network: false },
    };
  }
  if (loaded && loaded.status === REGISTRY_DATASET_STATUS.DATASET_UNAVAILABLE) {
    return loaded;
  }
  if (loaded && loaded.ok === false) {
    return {
      ok: false,
      status: REGISTRY_DATASET_STATUS.DATASET_UNAVAILABLE,
      reason: loaded.reason || loaded.error || `${label}_LOAD_FAILED`,
      rows: [],
      provenance: loaded.provenance || null,
      missing_prerequisite: loaded.missing_prerequisite || null,
      further_research_could_resolve: loaded.further_research_could_resolve,
    };
  }
  if (loaded && Array.isArray(loaded.rows)) {
    return {
      ok: true,
      status: loaded.status || REGISTRY_DATASET_STATUS.LOADED,
      rows: loaded.rows,
      provenance: loaded.provenance || null,
    };
  }
  return {
    ok: false,
    status: REGISTRY_DATASET_STATUS.DATASET_UNAVAILABLE,
    reason: `${label}_EMPTY_OR_INVALID_LOADER_RESULT`,
    rows: [],
    provenance: loaded?.provenance || null,
  };
}

function buildRegistryBusinessFollowUpGoals(hotel, lead) {
  const name = hotel.hotel_name || hotel.property_name || "hotel";
  const biz = lead.legal_name || lead.commercial_name || "registered business";
  return {
    unresolved_question:
      "Is the registry-matched registered business the current economic/property owner, an operator/lessee, or an unrelated legal vehicle?",
    hotel_name: name,
    lead: {
      subject: biz,
      relationship: "REGISTERED_BUSINESS",
      auto_ownership: false,
      cnpj: lead.cnpj || null,
      denue_clee: lead.denue_clee || null,
      source: lead.source,
    },
    priority_query_types: ["owner_disclosure", "filing", "operator_vs_owner"],
    proposed_queries: [
      `"${name}" "${biz}" (owner OR ownership OR "propiedad de" OR proprietário OR "razón social")`,
      `"${biz}" (hotel OR hospedagem OR hospedaje) (owner OR operator OR operador OR asset)`,
    ],
    creates_current_ownership: false,
    enrichment_authorized: false,
    publication_authorized: false,
    classification_preserved: true,
    source: "registry_business_follow_up",
  };
}

/**
 * Flatten follow-up plans into a bounded query queue for the handoff/loop.
 * Previously plans were notes-only; this is the consumable queue shape.
 */
export function buildBoundedFollowUpQueryQueue(plans = [], { maxQueries = 6 } = {}) {
  const queue = [];
  const seen = new Set();
  for (const plan of plans || []) {
    for (const q of plan.proposed_queries || []) {
      const key = String(q || "")
        .trim()
        .toLowerCase();
      if (!key || seen.has(key)) continue;
      if ((plan.avoid_queries || []).some((a) => String(a).trim().toLowerCase() === key)) continue;
      seen.add(key);
      queue.push({
        query: q,
        researchGoal: plan.unresolved_question || plan.question || null,
        evidence_gap: plan.unresolved_question || "registry_or_historical_follow_up",
        lead: plan.lead || null,
        creates_current_ownership: plan.creates_current_ownership === true,
        source_plan: plan.kind || plan.source || "follow_up_plan",
      });
      if (queue.length >= maxQueries) return queue;
    }
  }
  return queue;
}

/**
 * Run registry / discovery methods. Uses parent budgetController for Context.dev costs.
 * Inject adapters via deps for offline tests (no network).
 */
export async function runOwnershipNativeMethodRouter({
  hotel,
  ownership = {},
  newly = {},
  claims = [],
  budgetController = null,
  ledger = null,
  deps = {},
  budgets = {},
} = {}) {
  const defaults = createDefaultRegistryDatasetDeps({
    root: deps.datasetRoot || process.cwd(),
  });
  const effectiveDeps = {
    ...defaults,
    ...deps,
    // Explicit undefined should not wipe defaults — only override when provided
    fetchCadasturRows:
      deps.fetchCadasturRows !== undefined ? deps.fetchCadasturRows : defaults.fetchCadasturRows,
    fetchDenueRows: deps.fetchDenueRows !== undefined ? deps.fetchDenueRows : defaults.fetchDenueRows,
  };

  const trace = [];
  const leads = {
    registry: [],
    historical_follow_ups: [],
    registry_follow_ups: [],
    investor_docs: [],
    person_emails: [],
    recovered_cache: null,
    domain_resolution: null,
    bounded_follow_up_queue: [],
    dataset_provenance: {},
  };

  // ——— Owner research cache (recovered vs new) ———
  if (typeof deps.ownerResearchCache?.takeReuse === "function" && ownership.owner_entity_id) {
    const reused = deps.ownerResearchCache.takeReuse(ownership.owner_entity_id);
    if (reused) {
      leads.recovered_cache = {
        owner_entity_id: ownership.owner_entity_id,
        recovered: true,
        label: "RECOVERED_FROM_OWNER_CACHE",
        cached_at: reused.cached_at || null,
        provenance: reused.provenance || reused.ownership_evidenced_when_cached || null,
        freshness: reused.cached_at || null,
      };
      trace.push(
        invoked("owner_research_cache", {
          outcome: "REUSED",
          label: "RECOVERED_FROM_OWNER_CACHE",
          cached_at: reused.cached_at || null,
        })
      );
    } else {
      trace.push(skip("owner_research_cache", "NO_CACHE_HIT", "owner_cache_entry", true));
    }
  } else {
    trace.push(
      skip(
        "owner_research_cache",
        ownership.owner_entity_id ? "CACHE_NOT_PROVIDED" : "NO_OWNER_ENTITY_ID",
        ownership.owner_entity_id ? "deps.ownerResearchCache" : "owner_entity_id",
        Boolean(ownership.owner_entity_id)
      )
    );
  }

  // ——— Brazil Cadastur ———
  if (isBrazil(hotel)) {
    const loaded = await resolveRegistryRows(hotel, {
      injectedRows: effectiveDeps.cadasturRows,
      fetchFn: effectiveDeps.fetchCadasturRows,
      defaultFetch: defaults.fetchCadasturRows,
      label: "CADASTUR",
    });
    leads.dataset_provenance.brazil_cadastur = loaded.provenance || null;
    if (!loaded.ok || loaded.status === REGISTRY_DATASET_STATUS.DATASET_UNAVAILABLE) {
      trace.push(datasetUnavailable("brazil_cadastur", loaded));
    } else {
      const matchFn = effectiveDeps.matchHotelToCadastur || matchHotelToCadastur;
      const hotelForMatch = {
        ...hotel,
        hotel_name: hotel.hotel_name || hotel.property_name,
        property_name: hotel.property_name || hotel.hotel_name,
      };
      const match = matchFn(hotelForMatch, loaded.rows);
      if (effectiveDeps.onCadasturInvoked) effectiveDeps.onCadasturInvoked(match);
      if (match.accepted && match.best) {
        const lead = {
          relationship_type: "REGISTERED_BUSINESS",
          auto_ownership: false,
          legal_name: match.best.legal_name,
          commercial_name: match.best.commercial_name,
          cnpj: match.best.cnpj,
          website: match.best.website,
          city: match.best.city,
          match_score: match.best.score,
          match_evidence: {
            score: match.best.score,
            candidates: match.candidates || [],
            uncertainty: match.accepted ? "REGISTRY_IDENTITY_ONLY" : null,
          },
          establishment_identifiers: {
            cnpj: match.best.cnpj || null,
          },
          source: "brazil_cadastur",
          dataset_provenance: loaded.provenance,
          note: "Registry identity ≠ property owner",
        };
        leads.registry.push(lead);
        leads.registry_follow_ups.push(buildRegistryBusinessFollowUpGoals(hotelForMatch, lead));
        trace.push(
          invoked("brazil_cadastur", {
            outcome: "REGISTERED_BUSINESS_MATCH",
            legal_name: match.best.legal_name,
            cnpj: match.best.cnpj,
            auto_ownership: false,
            match_score: match.best.score,
            dataset_status: REGISTRY_DATASET_STATUS.LOADED,
            provenance: loaded.provenance,
          })
        );
        if (match.best.cnpj && typeof effectiveDeps.fetchCnpjQsa === "function") {
          const qsa = await effectiveDeps.fetchCnpjQsa(match.best.cnpj);
          trace.push(
            invoked("brazil_cnpj_qsa", {
              outcome: qsa ? "QSA_FETCHED" : "QSA_EMPTY",
              note: "Administrators/shareholders are not automatic property owners",
              people_count: Array.isArray(qsa?.qsa) ? qsa.qsa.length : 0,
            })
          );
        } else {
          trace.push(
            skip(
              "brazil_cnpj_qsa",
              "CNPJ_QSA_HELPER_NOT_INJECTED",
              "deps.fetchCnpjQsa (lib helper not default — script-only historically)",
              true
            )
          );
        }
      } else {
        const top = (match.candidates || []).slice(0, 5).map((c) => ({
          legal_name: c.legal_name || null,
          commercial_name: c.commercial_name || null,
          cnpj: c.cnpj || null,
          score: c.score ?? null,
        }));
        trace.push(
          invoked("brazil_cadastur", {
            outcome: "NO_ACCEPTED_MATCH",
            candidates: (match.candidates || []).length,
            top_candidates: top,
            best_score: match.best?.score ?? null,
            dataset_status: REGISTRY_DATASET_STATUS.LOADED,
            provenance: loaded.provenance,
            auto_ownership: false,
          })
        );
      }
    }
  } else {
    trace.push(skip("brazil_cadastur", "COUNTRY_NOT_BRAZIL", "hotel.country=Brazil", false));
  }

  // ——— Mexico DENUE ———
  if (isMexico(hotel)) {
    const loaded = await resolveRegistryRows(hotel, {
      injectedRows: effectiveDeps.denueRows,
      fetchFn: effectiveDeps.fetchDenueRows,
      defaultFetch: defaults.fetchDenueRows,
      label: "DENUE",
    });
    leads.dataset_provenance.mexico_denue = loaded.provenance || null;
    if (!loaded.ok || loaded.status === REGISTRY_DATASET_STATUS.DATASET_UNAVAILABLE) {
      trace.push(datasetUnavailable("mexico_denue", loaded));
    } else {
      const matchFn = effectiveDeps.matchHotelToDenue || matchHotelToDenue;
      const hotelForMatch = {
        ...hotel,
        property_name: hotel.property_name || hotel.hotel_name,
        canonical_name: hotel.canonical_name || hotel.hotel_name,
      };
      const match = matchFn(hotelForMatch, loaded.rows);
      if (effectiveDeps.onDenueInvoked) effectiveDeps.onDenueInvoked(match);
      if (match.accepted && match.best) {
        const lead = {
          relationship_type: BUSINESS_RELATIONSHIP.REGISTERED_BUSINESS,
          auto_ownership: false,
          legal_name: match.best.razon_social,
          commercial_name: match.best.nombre,
          website: match.best.website,
          source: "mexico_denue",
          denue_clee: match.best.clee,
          establecimiento_id: match.best.establecimiento_id,
          match_evidence: {
            score: match.best.match?.score ?? null,
            band: match.best.match?.band ?? null,
            distance_m: match.best.match?.distance_m ?? null,
            reasons: match.best.match?.reasons || [],
            ambiguous: match.ambiguous,
            uncertainty: "REGISTRY_IDENTITY_ONLY",
          },
          establishment_identifiers: {
            clee: match.best.clee || null,
            establecimiento_id: match.best.establecimiento_id || null,
          },
          dataset_provenance: loaded.provenance,
          note: "DENUE establishment ≠ automatic economic owner",
        };
        leads.registry.push(lead);
        leads.registry_follow_ups.push(buildRegistryBusinessFollowUpGoals(hotelForMatch, lead));
        trace.push(
          invoked("mexico_denue", {
            outcome: "REGISTERED_BUSINESS_MATCH",
            razon_social: match.best.razon_social,
            nombre: match.best.nombre,
            clee: match.best.clee,
            auto_ownership: false,
            relationship: BUSINESS_RELATIONSHIP.REGISTERED_BUSINESS,
            match_band: match.best.match?.band || null,
            dataset_status: REGISTRY_DATASET_STATUS.LOADED,
            provenance: loaded.provenance,
          })
        );
      } else {
        const top = (match.candidates || []).slice(0, 5).map((c) => ({
          nombre: c.nombre || null,
          razon_social: c.razon_social || null,
          clee: c.clee || null,
          score: c.match?.score ?? null,
          band: c.match?.band ?? null,
          reasons: c.match?.reasons || [],
        }));
        trace.push(
          invoked("mexico_denue", {
            outcome: match.ambiguous ? "AMBIGUOUS" : "NO_ACCEPTED_MATCH",
            candidates: (match.candidates || []).length,
            top_candidates: top,
            best_score: match.best?.match?.score ?? null,
            dataset_status: REGISTRY_DATASET_STATUS.LOADED,
            provenance: loaded.provenance,
            auto_ownership: false,
          })
        );
      }
    }
  } else {
    trace.push(skip("mexico_denue", "COUNTRY_NOT_MEXICO", "hotel.country=Mexico", false));
  }

  // ——— Historical / acquisition follow-up (preserve classifications) ———
  const claimList = [
    ...(claims || []),
    ...(newly.ownership_claims || []),
  ];
  const historical = claimList.filter(
    (c) =>
      c &&
      (c.relationship === "ACQUIRED" ||
        c.relationship === "PARENT_ACQUIRED" ||
        c.party_role === "HISTORICAL_OWNER" ||
        c.historical_vs_current === "HISTORICAL_PURCHASE_CLAIM_NEEDS_CURRENCY_CHECK" ||
        c.currentness === "HISTORICAL")
  );
  if (historical.length) {
    const merged = { candidates: claimList };
    const plans = buildQuestionDependentFollowUpPlan(merged, hotel);
    leads.historical_follow_ups = (plans || []).map((p) => ({
      ...p,
      classification_preserved: true,
      creates_current_ownership: false,
      note: "Historical acquisition → follow-up work, not current ownership",
    }));
    trace.push(
      invoked("historical_follow_up_planning", {
        outcome: "FOLLOW_UP_PLANS",
        plan_count: leads.historical_follow_ups.length,
        creates_current_ownership: false,
      })
    );
  } else {
    trace.push(
      skip("historical_follow_up_planning", "NO_HISTORICAL_OR_ACQUISITION_CLAIMS", "ownership_claims", true)
    );
  }

  // ——— Investor document discovery (needs discovered org) ———
  const orgName =
    ownership.owner_display_name ||
    leads.registry.find((r) => r.legal_name)?.legal_name ||
    null;
  const orgDomain =
    newly.domain?.host ||
    newly.domain?.url ||
    ownership.owner_domain ||
    null;
  if (orgName || orgDomain) {
    const queries = buildInvestorSearchQueries({
      domain: orgDomain,
      orgName,
      language: hotel.language || "en",
      maxQueries: 4,
    });
    if (!queries.length) {
      trace.push(
        skip("investor_document_discovery", "NO_INVESTOR_QUERIES", "orgName|domain", true)
      );
    } else if (typeof deps.searchFn !== "function" && typeof deps.investorSearchResults !== "function") {
      // Still record that method was selected; queries ready for parent to run under budget
      leads.investor_docs.push({
        status: "QUERIES_READY",
        version: INVESTOR_DOC_DISCOVERY_VERSION,
        orgName,
        orgDomain,
        queries,
      });
      trace.push(
        invoked("investor_document_discovery", {
          outcome: "QUERIES_BUILT",
          query_count: queries.length,
          orgName,
          executed_search: false,
          note: "Search deferred to parent budgetController when searchFn provided",
        })
      );
    } else {
      // Execute under shared budget when search available
      const searchResults =
        typeof deps.investorSearchResults === "function"
          ? await deps.investorSearchResults({ queries, orgName, orgDomain })
          : [];
      if (typeof deps.searchFn === "function" && !searchResults.length) {
        for (const q of queries.slice(0, 2)) {
          const cost = ledger?.estimateSearchCost?.(8) || CONTEXT_DEV_CREDIT_COSTS.search_per_10 || 1;
          if (budgetController) {
            const reserve = budgetController.tryReserve(cost, { method: "investor_doc_search", query: q });
            if (!reserve.ok) {
              trace.push(
                skip("investor_document_discovery", "BUDGET_RESERVE_DENIED", "context_dev_credits", true, {
                  remaining: budgetController.remainingAvailable?.(),
                })
              );
              break;
            }
            try {
              const res = await deps.searchFn({ query: q, numResults: 8 });
              budgetController.settle(reserve.token, "search", { method: "investor_doc_search" });
              const organic = res?.data?.results || res?.organic || [];
              searchResults.push(...organic);
            } catch (err) {
              budgetController.release(reserve.token);
              trace.push(
                skip("investor_document_discovery", "SEARCH_TRANSPORT_ERROR", null, true, {
                  error: String(err?.message || err),
                })
              );
              break;
            }
          } else {
            const res = await deps.searchFn({ query: q, numResults: 8 });
            const organic = res?.data?.results || res?.organic || [];
            searchResults.push(...organic);
          }
        }
      }
      const ranked = rankInvestorDocumentUrls(searchResults, { limit: 5 });
      let attributed = [];
      if (ranked.length && typeof deps.scrapeFn === "function") {
        const top = ranked[0];
        const url = top.url || top.link;
        const cost = CONTEXT_DEV_CREDIT_COSTS.scrape_markdown || 1;
        let md = "";
        if (budgetController) {
          const reserve = budgetController.tryReserve(cost, { method: "investor_doc_scrape", url });
          if (reserve.ok) {
            try {
              const scraped = await deps.scrapeFn({ url });
              budgetController.settle(reserve.token, "scrape_markdown", { method: "investor_doc_scrape" });
              md = String(
                typeof scraped?.data === "string"
                  ? scraped.data
                  : scraped?.data?.markdown || scraped?.data?.content || ""
              );
            } catch {
              budgetController.release(reserve.token);
            }
          }
        } else {
          const scraped = await deps.scrapeFn({ url });
          md = String(
            typeof scraped?.data === "string"
              ? scraped.data
              : scraped?.data?.markdown || scraped?.data?.content || ""
          );
        }
        if (md) attributed = extractAttributedPersonEmailsFromText(md, { people: newly.people || [] });
      }
      leads.investor_docs.push({
        status: "RAN",
        version: INVESTOR_DOC_DISCOVERY_VERSION,
        orgName,
        orgDomain,
        queries,
        ranked_count: ranked.length,
        attributed_emails: attributed,
      });
      trace.push(
        invoked("investor_document_discovery", {
          outcome: "RAN",
          ranked_count: ranked.length,
          attributed_email_count: attributed.length,
        })
      );
    }
  } else {
    trace.push(
      skip(
        "investor_document_discovery",
        "NO_DISCOVERED_ORGANIZATION",
        "owner_display_name|registry_legal_name|domain",
        true
      )
    );
  }

  // ——— Shared domain resolver (not a brand blocklist alone) ———
  if (newly.domain?.url || newly.domain?.host || orgDomain) {
    const discovered = newly.domain?.host || newly.domain?.url || orgDomain;
    const brandIsOwner = Boolean(
      ownership.classification === "BRAND_AS_OWNER" || ownership.brand_is_owner === true
    );
    const resolved = resolveOwnerDomain({
      discovered_domain: discovered,
      evidence_domains: (newly.domain?.evidence && [newly.domain.url]) || [],
      brand_is_owner: brandIsOwner,
    });
    leads.domain_resolution = {
      ...resolved,
      rejected_by_resolver: isRejectedOwnerDomain(discovered),
      note: "Brand domain is not proof of ownership; not automatically invalid for evidenced owner-operator",
    };
    trace.push(
      invoked("domain_resolver", {
        outcome: resolved?.domain ? "RESOLVED" : "NO_ACCEPTABLE_DOMAIN",
        domain: resolved?.domain || null,
        brand_is_owner: brandIsOwner,
      })
    );
  } else {
    trace.push(skip("domain_resolver", "NO_DOMAIN_CANDIDATE", "newly.domain", true));
  }

  // ——— Public person / email discovery (no paid enrichment) ———
  const enrichmentBlocked =
    Number(budgets.enrichment_max || 0) > 0 ||
    budgets.allow_enrichment === true ||
    budgets.allow_surfe === true ||
    budgets.allow_fullenrich === true;
  if (enrichmentBlocked) {
    // Still never call paid enrichment from this router
    trace.push(
      invoked("paid_enrichment_guard", {
        outcome: "BLOCKED",
        note: "Router never calls Surfe/FullEnrich/PDL even if budget keys present",
      })
    );
  }

  const hasOwner = Boolean(ownership.owner_display_name || orgName);
  const hasDomain = Boolean(newly.domain?.url || newly.domain?.host || leads.domain_resolution?.domain);
  if (hasOwner && hasDomain && typeof deps.runPersonEmailDiscovery === "function") {
    const cost = CONTEXT_DEV_CREDIT_COSTS.scrape_markdown || 1;
    if (budgetController) {
      const reserve = budgetController.tryReserve(cost, { method: "person_email_discovery" });
      if (!reserve.ok) {
        trace.push(
          skip("person_email_discovery", "BUDGET_RESERVE_DENIED", "context_dev_credits", true)
        );
      } else {
        try {
          const pe = await deps.runPersonEmailDiscovery({
            ownership,
            domain: newly.domain,
            ledger,
            extractPeopleFromOrgPages,
            researchPersonBusinessEmail,
          });
          budgetController.settle(reserve.token, "scrape_markdown", { method: "person_email_discovery" });
          leads.person_emails = pe?.rows || pe || [];
          trace.push(
            invoked("person_email_discovery", {
              outcome: "RAN",
              version: PERSON_EMAIL_DISCOVERY_VERSION,
              row_count: Array.isArray(leads.person_emails) ? leads.person_emails.length : 0,
              paid_enrichment: false,
            })
          );
        } catch (err) {
          budgetController.release(reserve.token);
          trace.push(
            skip("person_email_discovery", "PERSON_EMAIL_ERROR", null, true, {
              error: String(err?.message || err),
            })
          );
        }
      }
    } else {
      const pe = await deps.runPersonEmailDiscovery({
        ownership,
        domain: newly.domain,
        ledger,
        extractPeopleFromOrgPages,
        researchPersonBusinessEmail,
      });
      leads.person_emails = pe?.rows || pe || [];
      trace.push(
        invoked("person_email_discovery", {
          outcome: "RAN",
          version: PERSON_EMAIL_DISCOVERY_VERSION,
          paid_enrichment: false,
        })
      );
    }
  } else if (!hasOwner || !hasDomain) {
    trace.push(
      skip(
        "person_email_discovery",
        "PREREQUISITES_MISSING",
        !hasOwner ? "owner_display_name" : "confirmed_domain",
        true
      )
    );
  } else {
    trace.push(
      skip(
        "person_email_discovery",
        "PERSON_EMAIL_RUNNER_NOT_INJECTED",
        "deps.runPersonEmailDiscovery",
        true,
        { note: "Prerequisites met; inject runner to execute under parent budget" }
      )
    );
  }

  // Explicit non-execution of Parallel / Webhound
  trace.push(
    skip("parallel_ownership_discovery", "EXPLICITLY_DISABLED_THIS_TASK", "allow_parallel+separate_fallback", false)
  );
  trace.push(
    skip("webhound_ownership_execute", "EXPLICITLY_DISABLED_THIS_TASK", "allow_webhound+separate_fallback", false)
  );

  // Bounded follow-up queue — consumable by handoff/loop (not notes-only)
  const allPlans = [
    ...(leads.historical_follow_ups || []),
    ...(leads.registry_follow_ups || []),
  ];
  leads.bounded_follow_up_queue = buildBoundedFollowUpQueryQueue(allPlans, {
    maxQueries: Number(budgets.follow_up_query_max ?? 6),
  });
  trace.push(
    invoked("bounded_follow_up_queue", {
      outcome: leads.bounded_follow_up_queue.length ? "QUEUED" : "EMPTY",
      queued: leads.bounded_follow_up_queue.length,
      historical_plans: (leads.historical_follow_ups || []).length,
      registry_plans: (leads.registry_follow_ups || []).length,
      note:
        "Queue is consumable by handoff Phase A+/iterative loop. Prior Wave A only noted plans without enqueue.",
    })
  );

  return {
    version: OWNERSHIP_NATIVE_METHOD_ROUTER_VERSION,
    enabled: true,
    loader_version: OWNERSHIP_REGISTRY_DATASET_LOADER_VERSION,
    trace,
    leads,
    canonical_writes: false,
    paid_enrichment: false,
    parallel_executed: false,
    webhound_executed: false,
  };
}
