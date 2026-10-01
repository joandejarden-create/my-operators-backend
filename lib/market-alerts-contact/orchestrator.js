/**
 * Market Alerts contact enrichment orchestrator (Phase A).
 *
 * Plan path: eligibility → roles → orgs → named person → would-search / would-enrich
 * Execute path: cache → (named direct enrich | search+enrich) with per-run caps
 *
 * Provider failures / caps never fail Market Alerts ingestion.
 */

import {
  assessContactEnrichmentEligibility,
  isContactEnrichmentEnabled,
  getContactEnrichmentLimits,
  hasAdequateNamedIdentityForDirectEnrich,
  CONTACT_BLOCK_REASONS,
} from "./eligibility.js";
import { stakeholderRoleLabels, isBlockedGenericRole } from "./stakeholder-roles.js";
import { classifyContactConfidence } from "./confidence.js";
import {
  readPersonContactCache,
  writePersonContactCache,
  writeAlertContactCache,
  writePendingEnrichment,
  readAlertContactCache,
} from "./cache.js";
import { assessContactPersistenceSafety } from "./persistence.js";
import {
  createProviderRunBudget,
  getActiveProviderRunBudget,
  beginProviderRunBudget,
  CONTACT_PROVIDER_CAP_REASON,
} from "./provider-budget.js";
import { createSurfeContactProvider } from "./surfe-adapter.js";
import { createNoopContactProvider } from "./provider-interface.js";
import { createContactRecord, toContactCards } from "./contact-card.js";
import { logContactOp } from "./safe-log.js";

/**
 * Build a dry-run / planning decision for one alert. Zero Surfe calls.
 */
export function planContactEnrichment(alert = {}) {
  const assessment = assessContactEnrichmentEligibility(alert);
  const limits = getContactEnrichmentLimits();
  const enrichmentEnabled = isContactEnrichmentEnabled();
  const persistence = assessContactPersistenceSafety();

  if (!assessment.eligible) {
    return {
      eligible: false,
      skipReason: assessment.reason,
      organizations: assessment.organizations || [],
      targetRoles: [],
      namedCandidate: assessment.namedStakeholder || null,
      wouldSearch: false,
      wouldEnrich: false,
      preferDirectEnrich: false,
      estimatedSurfeCalls: { search: 0, enrich: 0, total: 0 },
      cachedReuse: false,
      enrichmentEnabled,
      persistence,
    };
  }

  const targetCompany = assessment.organizations[0] || null;
  const named = assessment.namedStakeholder;
  const roleLabels = stakeholderRoleLabels(assessment.roles);
  const surfeTitles = assessment.roles.flatMap((r) => r.surfeTitles || []).slice(0, 8);

  let cachedReuse = false;
  if (named?.personName && targetCompany) {
    const hit = readPersonContactCache({ personName: named.personName, companyName: targetCompany });
    cachedReuse = Boolean(hit);
  }

  const preferDirectEnrich =
    !cachedReuse &&
    hasAdequateNamedIdentityForDirectEnrich({ namedStakeholder: named, companyName: targetCompany });

  const wouldSearch = !cachedReuse && Boolean(targetCompany) && !preferDirectEnrich;
  const wouldEnrich = !cachedReuse && Boolean(targetCompany);
  const estimatedSearch = wouldSearch ? 1 : 0;
  const estimatedEnrich = wouldEnrich ? 1 : 0;

  return {
    eligible: true,
    skipReason: null,
    providerBlockedReason: enrichmentEnabled ? null : CONTACT_BLOCK_REASONS.DISABLED,
    providerWouldRun: enrichmentEnabled,
    organizations: assessment.organizations,
    targetCompany,
    targetRoles: roleLabels,
    surfeTitles,
    namedCandidate: named,
    wouldSearch,
    wouldEnrich,
    preferDirectEnrich,
    estimatedSurfeCalls: {
      search: estimatedSearch,
      enrich: estimatedEnrich,
      total: estimatedSearch + estimatedEnrich,
    },
    cachedReuse,
    enrichmentEnabled,
    actionable: assessment.actionable,
    maxCandidates: limits.maxCandidatesPerAlert,
    persistence,
  };
}

/**
 * Plan many alerts; apply per-run search/enrich/total caps to estimates.
 * Honors caller-supplied `budget` (required for tests / dry-run caps).
 */
export function planContactEnrichmentBatch(alerts = [], { budget } = {}) {
  const runBudget = budget || createProviderRunBudget();
  return planContactEnrichmentBatchWithBudget(alerts, runBudget);
}

function planContactEnrichmentBatchWithBudget(alerts, runBudget) {
  const plans = [];
  let cached = 0;
  let eligible = 0;
  let skipped = 0;
  let namedDirect = 0;
  let searchTotal = 0;
  let enrichTotal = 0;

  for (const alert of alerts) {
    const plan = planContactEnrichment(alert);
    const base = {
      ...plan,
      alertId: alert.id || alert.alertId || null,
      title: alert.title || null,
    };

    if (!plan.eligible) {
      skipped += 1;
      plans.push(base);
      continue;
    }

    if (plan.cachedReuse) {
      eligible += 1;
      cached += 1;
      runBudget.recordCacheAvoided(2);
      plans.push({
        ...base,
        wouldSearch: false,
        wouldEnrich: false,
        estimatedSurfeCalls: { search: 0, enrich: 0, total: 0 },
      });
      continue;
    }

    eligible += 1;

    if (plan.preferDirectEnrich) {
      if (!runBudget.tryReserveEnrich()) {
        skipped += 1;
        eligible -= 1;
        plans.push({
          ...base,
          eligible: false,
          skipReason: CONTACT_PROVIDER_CAP_REASON,
          wouldSearch: false,
          wouldEnrich: false,
          estimatedSurfeCalls: { search: 0, enrich: 0, total: 0 },
        });
        continue;
      }
      runBudget.recordNamedDirectAvoidedSearch();
      namedDirect += 1;
      enrichTotal += 1;
      plans.push({
        ...base,
        wouldSearch: false,
        wouldEnrich: true,
        preferDirectEnrich: true,
        estimatedSurfeCalls: { search: 0, enrich: 1, total: 1 },
      });
      continue;
    }

    // Search + enrich path needs both reservations when enrich is planned.
    let wouldSearch = false;
    let wouldEnrich = false;
    if (plan.wouldSearch) {
      if (!runBudget.tryReserveSearch()) {
        skipped += 1;
        eligible -= 1;
        plans.push({
          ...base,
          eligible: false,
          skipReason: CONTACT_PROVIDER_CAP_REASON,
          wouldSearch: false,
          wouldEnrich: false,
          estimatedSurfeCalls: { search: 0, enrich: 0, total: 0 },
        });
        continue;
      }
      wouldSearch = true;
      searchTotal += 1;
    }
    if (plan.wouldEnrich) {
      if (!runBudget.tryReserveEnrich()) {
        // Search already reserved — keep search intent but mark enrich capped.
        plans.push({
          ...base,
          wouldSearch,
          wouldEnrich: false,
          skipReason: CONTACT_PROVIDER_CAP_REASON,
          estimatedSurfeCalls: {
            search: wouldSearch ? 1 : 0,
            enrich: 0,
            total: wouldSearch ? 1 : 0,
          },
        });
        continue;
      }
      wouldEnrich = true;
      enrichTotal += 1;
    }

    plans.push({
      ...base,
      wouldSearch,
      wouldEnrich,
      estimatedSurfeCalls: {
        search: wouldSearch ? 1 : 0,
        enrich: wouldEnrich ? 1 : 0,
        total: (wouldSearch ? 1 : 0) + (wouldEnrich ? 1 : 0),
      },
    });
  }

  const snap = runBudget.snapshot();
  return {
    enrichmentEnabled: isContactEnrichmentEnabled(),
    limits: getContactEnrichmentLimits(),
    persistence: assessContactPersistenceSafety(),
    providerBudget: snap,
    summary: {
      considered: alerts.length,
      eligible,
      skipped,
      cachedContactsReused: cached,
      namedDirectEnrichPath: namedDirect,
      estimatedSurfeCalls: {
        search: searchTotal,
        enrich: enrichTotal,
        total: searchTotal + enrichTotal,
      },
      searchesAvoidedNamedDirect: snap.searchesAvoidedNamedDirect,
      callsAvoidedCache: snap.callsAvoidedCache,
      schemaImpact:
        "NONE - Phase A CONTACT_PERSISTENCE_MODE=local_dev JSON only (not production-durable). No Airtable mutation. Production persistence TBD after canary.",
    },
    plans,
  };
}

/**
 * Execute enrichment for one alert. Safe when disabled / capped.
 * Never throws — returns { ok, error?, contacts? }.
 *
 * @param {object} alert
 * @param {{ provider?: object, budget?: object, pollOnEnrich?: boolean, dryRun?: boolean, forceProvider?: boolean }} [opts]
 */
export async function enrichAlertContacts(alert = {}, opts = {}) {
  const budget = opts.budget || getActiveProviderRunBudget() || beginProviderRunBudget();
  try {
    const plan = planContactEnrichment(alert);
    const limits = getContactEnrichmentLimits();
    const alertId = alert.id || alert.alertId || null;

    if (!plan.eligible) {
      return { ok: true, skipped: true, reason: plan.skipReason, plan, contacts: [], budget: budget.snapshot() };
    }

    // Cache-first
    if (plan.namedCandidate?.personName && plan.targetCompany) {
      const hit = readPersonContactCache({
        personName: plan.namedCandidate.personName,
        companyName: plan.targetCompany,
      });
      if (hit) {
        budget.recordCacheAvoided(2);
        const record = createContactRecord({
          ...hit,
          alertId,
          entityKey: alert.entityKey || null,
          stakeholderRole: plan.targetRoles[0] || null,
          reasonSelected: hit.reasonSelected || "Cached verified contact",
          sourceArticle: alert.sourceUrl || alert.title || null,
          enrichmentStatus: "cached",
        });
        if (alertId) writeAlertContactCache(alertId, { contacts: [record], status: "cached" });
        logContactOp("enrich_cache_hit", {
          alertId,
          ...budget.snapshot(),
        });
        return {
          ok: true,
          cached: true,
          plan,
          contacts: [record],
          cards: toContactCards([record]),
          budget: budget.snapshot(),
          providerCallsConsumed: 0,
        };
      }
    }

    if (!isContactEnrichmentEnabled()) {
      return {
        ok: true,
        skipped: true,
        reason: CONTACT_BLOCK_REASONS.DISABLED,
        plan,
        contacts: [],
        cards: [],
        budget: budget.snapshot(),
      };
    }

    if (!limits.dryRunProvider && opts.forceProvider !== true && opts.dryRun === true) {
      return {
        ok: true,
        skipped: true,
        reason: "DRY_RUN_NO_PROVIDER",
        plan,
        contacts: [],
        cards: [],
        budget: budget.snapshot(),
      };
    }

    const provider =
      opts.provider ||
      (getSurfeApiKeyFromEnvSafe()
        ? createSurfeContactProvider({ pollOnEnrich: opts.pollOnEnrich === true })
        : createNoopContactProvider());

    if (provider.id === "noop") {
      return {
        ok: true,
        skipped: true,
        reason: "MISSING_SURFE_API_KEY",
        plan,
        contacts: [],
        cards: [],
        budget: budget.snapshot(),
      };
    }

    let candidates = [];
    let searchRequired = true;
    let callsConsumed = 0;

    // Prefer direct enrichment when named person + company is adequate.
    if (plan.preferDirectEnrich && plan.namedCandidate) {
      if (!budget.tryReserveEnrich()) {
        return {
          ok: true,
          skipped: true,
          reason: CONTACT_PROVIDER_CAP_REASON,
          plan,
          contacts: [],
          cards: [],
          budget: budget.snapshot(),
          providerCallsConsumed: 0,
        };
      }
      budget.recordNamedDirectAvoidedSearch();
      callsConsumed += 1;
      searchRequired = false;

      const enrichRes = await provider.enrichPeople({
        people: [
          {
            personName: plan.namedCandidate.personName,
            jobTitle: plan.namedCandidate.jobTitle || null,
            companyName: plan.targetCompany,
          },
        ],
        includeEmail: true,
        includeMobile: limits.includeMobile,
        includeLinkedIn: true,
      });

      logContactOp("enrich_named_direct", {
        alertId,
        company: plan.targetCompany,
        ...budget.snapshot(),
      });

      if (!enrichRes.ok) {
        return {
          ok: true,
          providerError: enrichRes.error,
          plan,
          contacts: [],
          cards: [],
          budget: budget.snapshot(),
          providerCallsConsumed: callsConsumed,
          searchRequired: false,
        };
      }

      return finalizeEnrichmentResult({
        alert,
        alertId,
        plan,
        enrichRes,
        candidates: [
          {
            personName: plan.namedCandidate.personName,
            jobTitle: plan.namedCandidate.jobTitle || null,
            companyName: plan.targetCompany,
          },
        ],
        budget,
        callsConsumed,
        searchRequired: false,
        limits,
      });
    }

    // Company + role search path
    if (!budget.tryReserveSearch()) {
      return {
        ok: true,
        skipped: true,
        reason: CONTACT_PROVIDER_CAP_REASON,
        plan,
        contacts: [],
        cards: [],
        budget: budget.snapshot(),
        providerCallsConsumed: 0,
      };
    }
    callsConsumed += 1;

    const titles = (plan.surfeTitles || []).filter((t) => !isBlockedGenericRole(t));
    const searchRes = await provider.findPeople({
      companyName: plan.targetCompany,
      roles: titles,
      limit: limits.maxCandidatesPerAlert,
      namedPerson: plan.namedCandidate
        ? { fullName: plan.namedCandidate.personName }
        : undefined,
    });

    if (!searchRes.ok) {
      logContactOp("enrich_search_failed", { error: searchRes.error, alertId, ...budget.snapshot() });
      return {
        ok: true,
        providerError: searchRes.error,
        plan,
        contacts: [],
        cards: [],
        budget: budget.snapshot(),
        providerCallsConsumed: callsConsumed,
        searchRequired: true,
      };
    }

    candidates = (searchRes.people || []).slice(0, limits.maxCandidatesPerAlert);
    if (!candidates.length) {
      return {
        ok: true,
        empty: true,
        plan,
        contacts: [],
        cards: [],
        budget: budget.snapshot(),
        providerCallsConsumed: callsConsumed,
        searchRequired: true,
        candidatesReturned: 0,
      };
    }

    if (!budget.tryReserveEnrich()) {
      return {
        ok: true,
        skipped: true,
        reason: CONTACT_PROVIDER_CAP_REASON,
        plan,
        contacts: [],
        cards: [],
        budget: budget.snapshot(),
        providerCallsConsumed: callsConsumed,
        searchRequired: true,
        candidatesReturned: candidates.length,
        candidatesPreview: candidates.map((c) => ({
          personName: c.personName,
          jobTitle: c.jobTitle,
          companyName: c.companyName,
        })),
      };
    }
    callsConsumed += 1;

    const enrichRes = await provider.enrichPeople({
      people: candidates,
      includeEmail: true,
      includeMobile: limits.includeMobile,
      includeLinkedIn: true,
    });

    if (!enrichRes.ok) {
      logContactOp("enrich_failed", { error: enrichRes.error, alertId, ...budget.snapshot() });
      return {
        ok: true,
        providerError: enrichRes.error,
        plan,
        contacts: [],
        cards: [],
        budget: budget.snapshot(),
        providerCallsConsumed: callsConsumed,
        searchRequired: true,
      };
    }

    return finalizeEnrichmentResult({
      alert,
      alertId,
      plan,
      enrichRes,
      candidates,
      budget,
      callsConsumed,
      searchRequired: true,
      limits,
    });
  } catch (err) {
    logContactOp("enrich_throw_swallowed", { message: String(err?.message || err).slice(0, 120) });
    return {
      ok: true,
      error: "INTERNAL_SWALLOWED",
      contacts: [],
      cards: [],
      budget: budget?.snapshot?.() || null,
    };
  }
}

function finalizeEnrichmentResult({
  alert,
  alertId,
  plan,
  enrichRes,
  candidates,
  budget,
  callsConsumed,
  searchRequired,
  limits,
}) {
  if (enrichRes.pending && enrichRes.enrichmentId) {
    writePendingEnrichment(enrichRes.enrichmentId, {
      alertId,
      status: "PENDING",
      candidateCount: candidates.length,
      company: plan.targetCompany,
    });
    if (alertId) {
      writeAlertContactCache(alertId, {
        status: "pending",
        enrichmentId: enrichRes.enrichmentId,
        contacts: [],
      });
    }
    return {
      ok: true,
      pending: true,
      enrichmentId: enrichRes.enrichmentId,
      plan,
      contacts: [],
      cards: [],
      budget: budget.snapshot(),
      providerCallsConsumed: callsConsumed,
      searchRequired,
      candidatesReturned: candidates.length,
      phoneRequested: limits.includeMobile === true,
    };
  }

  const enriched = enrichRes.people?.length ? enrichRes.people : candidates;
  const contacts = enriched.map((p, idx) => {
    const namedInArticle =
      plan.namedCandidate &&
      p.personName &&
      String(p.personName).toLowerCase() === String(plan.namedCandidate.personName).toLowerCase();
    const hasVerifiedWorkEmail =
      Boolean(p.email) && /VALID|work|business|provider_returned/i.test(String(p.emailStatus || "VALID"));
    const confidence = classifyContactConfidence({
      namedInArticle: Boolean(namedInArticle),
      titleCompanyConfirmed: Boolean(p.jobTitle && p.companyName),
      hasVerifiedWorkEmail,
      hasBusinessContact: Boolean(p.email || p.phone || p.linkedinUrl),
      strongCompanyRoleMatch: Boolean(p.jobTitle),
      currentEmploymentConfirmed: Boolean(p.companyName),
      weakRoleMatch: false,
      ambiguousMatch: false,
    });
    const record = createContactRecord({
      ...p,
      alertId,
      entityKey: alert.entityKey || null,
      stakeholderRole:
        plan.targetRoles[Math.min(idx, plan.targetRoles.length - 1)] || plan.targetRoles[0] || null,
      matchConfidence: confidence,
      reasonSelected: buildReasonSelected(plan, p, namedInArticle),
      sourceArticle: alert.sourceUrl || alert.title || null,
      enrichmentStatus: "enriched",
      lastVerifiedAt: p.lastVerifiedAt || new Date().toISOString(),
    });
    writePersonContactCache(record);
    return record;
  });

  if (alertId) writeAlertContactCache(alertId, { status: "ready", contacts });

  logContactOp("enrich_complete", {
    alertId,
    contactCount: contacts.length,
    searchRequired,
    ...budget.snapshot(),
  });

  return {
    ok: true,
    plan,
    contacts,
    cards: toContactCards(contacts),
    budget: budget.snapshot(),
    providerCallsConsumed: callsConsumed,
    searchRequired,
    candidatesReturned: candidates.length,
    phoneRequested: limits.includeMobile === true,
  };
}

function buildReasonSelected(plan, person, namedInArticle) {
  if (namedInArticle) {
    return `Named stakeholder in article - ${person.jobTitle || plan.targetRoles[0] || "decision maker"}`;
  }
  const role = plan.targetRoles[0] || "Commercial decision maker";
  return `${role} for ${plan.targetCompany || "this project"}`;
}

function getSurfeApiKeyFromEnvSafe() {
  try {
    return Boolean(process.env.SURFE_API_KEY && String(process.env.SURFE_API_KEY).trim());
  } catch {
    return false;
  }
}

/**
 * Safe hook for RSS sync — never fails ingestion.
 */
export async function maybeEnrichAfterIngest(alert, opts = {}) {
  if (!isContactEnrichmentEnabled()) {
    return { skipped: true, reason: CONTACT_BLOCK_REASONS.DISABLED };
  }
  try {
    return await enrichAlertContacts(alert, opts);
  } catch (err) {
    logContactOp("ingest_hook_swallowed", { message: String(err?.message || err).slice(0, 80) });
    return { skipped: true, reason: "ERROR_SWALLOWED" };
  }
}

export function getCachedAlertContacts(alertId, { includeLow = false } = {}) {
  const cached = readAlertContactCache(alertId);
  if (!cached?.contacts?.length) {
    return {
      status: cached?.status || "empty",
      contacts: [],
      cards: [],
      enrichmentId: cached?.enrichmentId || null,
    };
  }
  return {
    status: cached.status || "ready",
    contacts: cached.contacts,
    cards: toContactCards(cached.contacts, { includeLow }),
    enrichmentId: cached.enrichmentId || null,
  };
}

export { beginProviderRunBudget, CONTACT_PROVIDER_CAP_REASON };
