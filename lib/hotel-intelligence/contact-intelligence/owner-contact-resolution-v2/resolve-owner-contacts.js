/**
 * resolve_owner_contacts — Contact Resolution V2 orchestrator.
 * Delegates person/org discovery to proven CI V1.4 modules; does not replace them with regex-only paths.
 * Staging writes only.
 */

import crypto from "node:crypto";
import { createContactStore } from "../store.js";
import { discoverOwnerPersonPath } from "../owner-person-discovery.js";
import { resolveOwnerDomain } from "./domain-resolver.js";
import { isPublishableEmailStatus } from "./email-resolution.js";
import { gateContactCandidate, retainRejected } from "./gates.js";
import { createResearchTrace, finalizeTrace } from "./research-trace.js";
import { planWebhoundEscalation } from "./webhound-escalation.js";
import { readCachedResolution, writeCachedResolution } from "./cache.js";
import {
  RESOLUTION_STATUS,
  CONTACT_STRENGTH,
  EMAIL_STATUS,
  PHONE_TYPE,
  CONTACT_RESOLUTION_V2,
} from "./vocabulary.js";
import { createContextDevCreditLedger, CONTEXT_DEV_CREDIT_COSTS } from "../../../context-dev/credit-ledger.js";
import { isContextDevConfigured } from "../../../context-dev/index.js";
import {
  getOwnerPortfolioProfile,
  resolveOwnerEntityId,
  ensureGoldenOwnerPortfoliosMaterialized,
} from "../../ownership/owner-control/index.js";
import {
  loadRecoveredBaselineForOwner,
  getKnownGoldenDomain,
} from "./baseline-evidence-recovery.js";
import {
  mapDiscoveryResultToV2,
  extractDomainFromPortfolio,
  mapCiPersonToV2Contact,
} from "./ci-orchestration-bridge.js";

export { CONTACT_RESOLUTION_V2 };

function labelContactStrength(c) {
  if (
    c.email_status === EMAIL_STATUS.EXPLICIT_VERIFIED &&
    c.phone &&
    [PHONE_TYPE.DIRECT_MOBILE, PHONE_TYPE.DIRECT_OFFICE].includes(c.phone_type)
  ) {
    return CONTACT_STRENGTH.VERIFIED_DIRECT_CONTACT;
  }
  if (c.email_status === EMAIL_STATUS.EXPLICIT_VERIFIED) return CONTACT_STRENGTH.VERIFIED_WORK_EMAIL;
  if (c.email_status === EMAIL_STATUS.INFERRED_VERIFIED) return CONTACT_STRENGTH.INFERRED_VERIFIED_EMAIL;
  if (c.phone_type === PHONE_TYPE.EXECUTIVE_OFFICE) return CONTACT_STRENGTH.EXECUTIVE_OFFICE;
  if (c.phone_type === PHONE_TYPE.CORPORATE_PHONE || c.email_status === EMAIL_STATUS.EXPLICIT_UNVERIFIED) {
    return CONTACT_STRENGTH.CORPORATE_CONTACT;
  }
  return CONTACT_STRENGTH.GENERAL_CONTACT;
}

function emptyResult(owner_entity_id, status, extra = {}) {
  return {
    owner_entity_id,
    owner_org_name: extra.owner_org_name || null,
    canonical_domain: null,
    contacts: [],
    organization_contacts: { general_email: null, corporate_phone: null, website: null },
    resolution_status: status,
    confidence: {
      OWNER_RELATIONSHIP_CONFIDENCE: extra.owner_conf || "UNKNOWN",
      PERSON_RELATIONSHIP_CONFIDENCE: "UNKNOWN",
      EMAIL_CONFIDENCE: "UNKNOWN",
      PHONE_CONFIDENCE: "UNKNOWN",
    },
    cost_usd: 0,
    elapsed_ms: 0,
    rejected_candidates: [],
    research_trace: extra.trace || null,
    webhound_plan: null,
    evaluation_staging: true,
    version: CONTACT_RESOLUTION_V2,
    orchestration: {
      path: "shared_ci_v1.4",
      baseline_recovery: false,
      live_discovery: false,
      zero_people_diagnosis: extra.zero_people_diagnosis || null,
    },
    ...extra,
  };
}

function mergeContacts(primary = [], secondary = []) {
  const byKey = new Map();
  for (const c of [...primary, ...secondary]) {
    const key = `${String(c.full_name || "").toLowerCase()}|${String(c.email || "").toLowerCase()}`;
    if (!byKey.has(key)) byKey.set(key, c);
  }
  return [...byKey.values()];
}

function mapRecoveredContact(rc) {
  const draft = {
    person_id: `ocr2_rec_${crypto.randomBytes(4).toString("hex")}`,
    full_name: rc.full_name,
    title: rc.title || null,
    relationship_role: rc.title || null,
    email: rc.email,
    email_status: rc.email_status,
    email_method: rc.email_method,
    email_publishable: rc.email_publishable !== false && isPublishableEmailStatus(rc.email_status),
    phone: rc.phone || null,
    phone_type: rc.phone_type || null,
    evidence: rc.evidence || [],
    research_method_ids: ["baseline_evidence_recovery"],
    provider: rc.provider || "recovered_baseline",
    last_verified_at: rc.last_verified_at,
    recovery_source: rc.recovery_source,
    provider_email_status: rc.provider_email_status || null,
    usage_rights: rc.usage_rights || null,
    do_not_advance_verification_date: true,
  };
  draft.contact_strength = labelContactStrength(draft);
  return draft;
}

/**
 * @param {string} owner_entity_id
 * @param {object} options
 */
export async function resolve_owner_contacts(owner_entity_id, options = {}) {
  const started = Date.now();
  ensureGoldenOwnerPortfoliosMaterialized();

  const ownerId = resolveOwnerEntityId(owner_entity_id) || String(owner_entity_id || "").trim();
  if (!ownerId) {
    return emptyResult(null, RESOLUTION_STATUS.OWNER_MISSING, { error: "owner_entity_id_required" });
  }

  const store = options.store || createContactStore();
  const force_refresh = Boolean(options.force_refresh);
  const max_cost_usd = Number(options.max_cost_usd ?? 2);
  const allow_webhound = options.allow_webhound !== false;
  const allow_paid_fallback = Boolean(options.allow_paid_fallback);
  const allow_live_discovery = options.allow_live_discovery !== false;
  const recover_baseline = options.recover_baseline !== false;
  const hotel_context_ids = [].concat(options.hotel_id || [], options.hotel_context_ids || []).filter(Boolean);
  const country = options.country || null;

  const cached = readCachedResolution(store, ownerId, { force_refresh });
  if (cached && !options.skip_cache) {
    return { ...cached, elapsed_ms: Date.now() - started, cost_usd: 0 };
  }

  const profile = getOwnerPortfolioProfile(ownerId);
  const owner_org_name =
    options.owner_org_name ||
    profile?.owner_display_name ||
    profile?.display_name ||
    profile?.legal_name ||
    profile?.owner_legal_name ||
    null;

  if (!owner_org_name || owner_org_name === ownerId) {
    return emptyResult(ownerId, RESOLUTION_STATUS.OWNER_MISSING, {
      owner_org_name: ownerId,
      error: "owner_display_name_unresolved",
      zero_people_diagnosis: {
        primary: "IDENTITY_MISMATCH",
        detail:
          "Owner entity ID passed without resolvable display/legal name; V1 harness used entity ID as org name so domain/person discovery could not anchor.",
      },
    });
  }

  const trace = createResearchTrace({
    owner_entity_id: ownerId,
    hotel_context_ids,
    country,
  });
  trace.methods_attempted.push("shared_ci_orchestration_v2");

  let cost_usd = 0;
  const addCost = (usd, key) => {
    cost_usd += usd;
    trace.cost_usd = cost_usd;
    trace.cost_breakdown[key] = (trace.cost_breakdown[key] || 0) + usd;
  };

  // ——— Domain from portfolio / golden hints ———
  const evidence_domains = [];
  const goldenDomain = getKnownGoldenDomain(ownerId) || extractDomainFromPortfolio(profile);
  if (goldenDomain) evidence_domains.push({ domain: goldenDomain, source: "golden_portfolio" });
  if (profile?.website) evidence_domains.push({ domain: profile.website, source: "portfolio_profile" });
  if (options.seed_domain) evidence_domains.push({ domain: options.seed_domain, source: "seed" });
  const stagedRoute = store.getOwnerRoute?.(ownerId);
  if (stagedRoute?.canonical_domain) {
    evidence_domains.push({ domain: stagedRoute.canonical_domain, source: "staged_owner_route" });
  }

  const domainRes = resolveOwnerDomain({
    stored_domain: options.stored_domain || stagedRoute?.verified_domain || goldenDomain || null,
    stored_confidence: profile?.domain_confidence || (goldenDomain ? "HIGH" : null),
    evidence_domains,
    discovered_domain: options.discovered_domain || null,
    brand_is_owner: Boolean(options.brand_is_owner),
  });
  trace.successful_path.push(domainRes.ok ? "domain_resolved" : "domain_unresolved");

  let contacts = [];
  let rejected_candidates = [];
  let organization_contacts = {
    general_email: stagedRoute?.organization_contacts?.general_email || null,
    corporate_phone: stagedRoute?.organization_contacts?.corporate_phone || null,
    website: domainRes.domain ? `https://${domainRes.domain}` : null,
  };
  let baselineRecovery = null;
  let discovery = null;
  const zero_people_diagnosis = { layers: [] };

  // ——— Recovered baseline (no paid calls) ———
  if (recover_baseline) {
    trace.methods_attempted.push("baseline_evidence_recovery");
    baselineRecovery = loadRecoveredBaselineForOwner(ownerId);
    if (baselineRecovery.contacts.length) {
      trace.successful_path.push("baseline_contacts_recovered");
      for (const rc of baselineRecovery.contacts) {
        const draft = mapRecoveredContact(rc);
        const gate = gateContactCandidate(draft, {
          owner_domain: domainRes.domain,
          brand_is_owner: Boolean(options.brand_is_owner),
        });
        if (!gate.ok) rejected_candidates.push(retainRejected(draft, gate.rejected));
        else contacts.push(draft);
      }
      organization_contacts = {
        ...organization_contacts,
        ...(baselineRecovery.organization_contacts || {}),
        website: organization_contacts.website || baselineRecovery.organization_contacts?.website,
      };
    } else {
      zero_people_diagnosis.layers.push({
        layer: "EVIDENCE_NOT_LOADED",
        detail: "No saved baseline package matched this owner_entity_id",
      });
    }
  }

  // ——— Live discovery via discoverOwnerPersonPath (proven CI path) ———
  if (allow_live_discovery && cost_usd < max_cost_usd) {
    trace.methods_attempted.push("discoverOwnerPersonPath_v1.4");
    const focusHotel = options.focus_hotel || {};
    const hotels = options.hotels?.length
      ? options.hotels
      : focusHotel.hotel_id
        ? [focusHotel]
        : hotel_context_ids.map((id) => ({ hotel_id: id, hotel_name: options.hotel_name || id }));

    const personHypotheses = (baselineRecovery?.contacts || []).map((c) => ({
      display_name: c.full_name,
      title: c.title,
      attributed_email_seed: c.email,
      source_page_url: c.evidence?.[0]?.source_url,
    }));

    try {
      discovery = await discoverOwnerPersonPath({
        owner_entity_id: ownerId,
        owner_display_name: owner_org_name,
        focus_hotel_id: focusHotel.hotel_id || hotel_context_ids[0] || null,
        focus_hotel_name: focusHotel.hotel_name || options.hotel_name || null,
        hotels,
        domain_hypotheses: domainRes.domain
          ? [{ url: `https://${domainRes.domain}`, confidence: "HIGH", from: "v2_domain_resolver" }]
          : evidence_domains.map((d) => ({ url: `https://${d.domain}`, from: d.source })),
        person_hypotheses: personHypotheses,
        forbidden_org_hosts: options.forbidden_org_hosts || [],
        max_serp_queries: options.max_serp_queries ?? 3,
      });
      addCost(discovery.cost?.serpapi_usd || 0, "serpapi");
      addCost((discovery.cost?.pages_fetched || 0) * 0.01, "context_or_fetch_est");

      if (discovery.sources?.length) trace.sources_opened.push(...discovery.sources.map((s) => s.url || s).filter(Boolean));
      if (discovery.people?.length) trace.successful_path.push("ci_people_found");
      else
        zero_people_diagnosis.layers.push({
          layer: "EXTRACTION_OR_DISCOVERY_GAP",
          detail: discovery.unresolved_reasons?.join("; ") || "discoverOwnerPersonPath returned zero people",
          blockers: discovery.blockers,
        });

      const mapped = mapDiscoveryResultToV2(discovery, {
        country,
        owner_domain: domainRes.domain,
        brand_is_owner: Boolean(options.brand_is_owner),
      });
      contacts = mergeContacts(contacts, mapped.contacts);
      rejected_candidates = rejected_candidates.concat(mapped.rejected_candidates);
      organization_contacts = {
        general_email: mapped.organization_contacts.general_email || organization_contacts.general_email,
        corporate_phone: mapped.organization_contacts.corporate_phone || organization_contacts.corporate_phone,
        website: mapped.organization_contacts.website || organization_contacts.website,
        contact_form_fallbacks: mapped.organization_contacts.contact_form_fallbacks,
      };
    } catch (err) {
      trace.methods_attempted.push(`discoverOwnerPersonPath_error:${err.message}`);
      zero_people_diagnosis.layers.push({ layer: "EXTRACTION_FAILURE", detail: err.message });
    }
  } else if (!allow_live_discovery) {
    zero_people_diagnosis.layers.push({
      layer: "LIVE_DISCOVERY_DISABLED",
      detail: "Baseline recovery only — no SerpAPI/Context.dev spend",
    });
  }

  // Publication / eligibility filtering already applied via gateContactCandidate
  if (!contacts.length && baselineRecovery?.contacts?.length) {
    zero_people_diagnosis.layers.push({
      layer: "PUBLICATION_FILTERING",
      detail: "Recovered contacts existed but were rejected by contact gates",
    });
  }

  let resolution_status = RESOLUTION_STATUS.UNRESOLVED;
  if (contacts.some((c) => c.email_publishable || (c.phone && c.phone_type !== PHONE_TYPE.HOTEL_PHONE))) {
    resolution_status = RESOLUTION_STATUS.RESOLVED_DIRECT;
  } else if (organization_contacts.general_email || organization_contacts.corporate_phone) {
    resolution_status = RESOLUTION_STATUS.RESOLVED_CORPORATE_ONLY;
  } else if (domainRes.ok || contacts.length) {
    resolution_status = RESOLUTION_STATUS.PARTIAL;
  }
  if (cost_usd >= max_cost_usd && resolution_status === RESOLUTION_STATUS.UNRESOLVED) {
    resolution_status = RESOLUTION_STATUS.BUDGET_EXHAUSTED;
  }

  const confidence = {
    OWNER_RELATIONSHIP_CONFIDENCE: profile ? "HIGH" : options.owner_org_name ? "MEDIUM" : "LOW",
    PERSON_RELATIONSHIP_CONFIDENCE: contacts.length ? "MEDIUM" : "LOW",
    EMAIL_CONFIDENCE: contacts.some((c) => c.email_publishable)
      ? contacts.some((c) => c.email_status === EMAIL_STATUS.EXPLICIT_VERIFIED)
        ? "HIGH"
        : "MEDIUM"
      : "LOW",
    PHONE_CONFIDENCE: organization_contacts.corporate_phone || contacts.some((c) => c.phone) ? "MEDIUM" : "LOW",
  };

  const result = {
    owner_entity_id: ownerId,
    owner_org_name,
    canonical_domain: domainRes.domain,
    domain_resolution: domainRes,
    contacts,
    organization_contacts,
    resolution_status,
    confidence,
    cost_usd,
    elapsed_ms: Date.now() - started,
    rejected_candidates,
    allow_paid_fallback,
    paid_fallback_used: false,
    evaluation_staging: true,
    version: CONTACT_RESOLUTION_V2,
    resolved_at: new Date().toISOString(),
    orchestration: {
      path: "shared_ci_v1.4",
      baseline_recovery: Boolean(baselineRecovery?.contacts?.length),
      baseline_sources: baselineRecovery?.sources_loaded || [],
      live_discovery: allow_live_discovery,
      discovery_version: discovery?.version || null,
      zero_people_diagnosis: contacts.length ? null : zero_people_diagnosis,
      note: "V2 orchestrates discoverOwnerPersonPath + baseline recovery; regex website-research is not primary.",
    },
  };

  const wh = planWebhoundEscalation(result, {
    allow_webhound,
    budget_allowed: Boolean(options.webhound_budget_allowed),
    country,
    hotel_context_ids,
  });
  result.webhound_plan = wh;

  finalizeTrace(trace, {
    result_status: resolution_status,
    final_contacts: contacts,
    elapsed_ms: result.elapsed_ms,
  });
  if (wh.should_escalate) trace.webhound = wh.request;
  result.research_trace = trace;

  if (!options.skip_cache_write) writeCachedResolution(store, ownerId, result);
  return result;
}

export default resolve_owner_contacts;
